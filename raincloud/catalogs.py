# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Explicit catalog lifecycle. Reads never refresh or write catalog state."""
from __future__ import annotations

import os
import shutil
import sys
import tempfile
import uuid
import warnings
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from importlib import resources
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit
from urllib.request import urlopen

from ._bundle import FILES, REVISION, Bundle, capabilities, digest, document, encode, make_bundle, read_bundle
from ._locking import atomic_write, creation_mode, is_held, locked
from ._transport import filesystem_url
from .config import REVISION_PREFIX, Config, get_config, use_config
from .exceptions import BuildToolingMissing, CatalogError, OfflineMiss

_BUNDLE_FILES = ("catalog.json", *FILES)
_PARSED: dict = {}
# Parsed catalogs kept per process. A process sees a handful (the selected one,
# a pinned revision, a build's override); the cap only bounds a long-lived
# process that walks many revisions.
_PARSED_MAX = 32


def _signature(paths) -> tuple:
    result = []
    for path in paths:
        try:
            st = os.stat(path)
        except OSError:
            result.append((str(path), None))
        else:
            result.append((str(path), st.st_mtime_ns, st.st_size, st.st_ino))
    return tuple(result)


def _parsed(key: tuple, paths, build):
    """`build()`, reused for as long as none of `paths` changes on disk.

    Reading and validating a multi-megabyte manifest costs ~70 ms, and every
    load()/describe() outside an operation resolves the catalog again. The files
    are replaced by rename (new inode) or edited (new mtime), either of which
    misses here, so an unchanged catalog is parsed once per process.
    """
    full = (key, _signature(paths))
    hit = _PARSED.get(full)
    if hit is None:
        hit = build()
        if len(_PARSED) >= _PARSED_MAX:
            _PARSED.clear()
        _PARSED[full] = hit
    return hit


def clear_parse_cache() -> None:
    """Forget every parsed catalog file (tests; a long-lived process never needs it)."""
    _PARSED.clear()


def _read_bundle(directory: Path, revision: str | None) -> Bundle:
    return _parsed(("bundle", str(directory), revision), [directory / n for n in _BUNDLE_FILES],
                   lambda: read_bundle(directory, revision))


def state(config: Config) -> dict:
    path = config.catalog_dir / "active.json"
    if not path.exists():
        return {"active": None, "pinned": False, "history": []}
    result = document(path.read_bytes(), "catalog state")
    if (not isinstance(result.get("active"), str) or not REVISION.fullmatch(result["active"])
            or type(result.get("pinned")) is not bool or not isinstance(result.get("history"), list)
            or not all(isinstance(r, str) and REVISION.fullmatch(r) for r in result["history"])):
        raise CatalogError(f"invalid catalog state at {path}")
    return result


def installed(config: Config, revision: str) -> Bundle:
    if not REVISION.fullmatch(revision):
        raise CatalogError("revision must be a complete SHA-256 catalog identifier")
    directory = config.catalog_dir / "revisions" / revision
    if not directory.is_dir():
        from .exceptions import MissingRevision
        raise MissingRevision(f"catalog revision is not installed: {revision}")
    return _read_bundle(directory, revision)


def _install(root: Path, bundle: Bundle) -> Path:
    bundle.validate()
    dest = root / bundle.revision
    if dest.exists():
        read_bundle(dest, bundle.revision)
        return dest
    root.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".incoming-", dir=root))
    staging.chmod(creation_mode(0o777))
    try:
        for name, raw in bundle.files().items():
            atomic_write(staging / name, raw)
        read_bundle(staging, bundle.revision)
        os.rename(staging, dest)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return dest


def store(config: Config, bundle: Bundle) -> Path:
    with locked(config.catalog_dir / ".lock"):
        return _install(config.catalog_dir / "revisions", bundle)


def release(bundle: Bundle, output: Path) -> str:
    """Install `bundle` into a pack directory and point latest.json at it."""
    with locked(output / ".lock"):
        _install(output, bundle)
        atomic_write(output / "latest.json", encode({"revision": bundle.revision}))
    return bundle.revision


def pack(manifest: Path, snapshot: Path, output: Path, catalog_id: str) -> str:
    """Create a static upstream directory locally; publishing is a separate step."""
    return release(make_bundle(manifest.read_bytes(), snapshot.read_bytes(), catalog_id), output)


def _fetch(source: str, name: str, offline: bool) -> bytes:
    parts = urlsplit(source)
    if parts.scheme in {"http", "https"}:
        if offline:
            raise OfflineMiss("catalog revision is not installed and offline mode is on")
        if parts.scheme == "http" and parts.hostname not in {"localhost", "127.0.0.1", "::1"}:
            raise CatalogError("catalog upstream must use HTTPS (HTTP is allowed only on loopback)")
        if parts.query or parts.fragment:
            raise CatalogError("catalog upstream must be a directory URL without query/fragment")
        try:
            with urlopen(source.rstrip("/") + "/" + name, timeout=30) as response:
                final = urlsplit(response.url)
                if final.scheme != "https" and not (final.scheme == "http" and final.hostname in {"localhost", "127.0.0.1", "::1"}):
                    raise CatalogError("catalog redirect must use HTTPS")
                raw = response.read(64 * 1024 * 1024 + 1)
        except HTTPError as exc:
            # Status and reason only: the URL can carry credentials.
            raise CatalogError(f"catalog download of {name} failed: HTTP {exc.code} {exc.reason}") from None
        except OSError as exc:
            raise CatalogError(f"catalog download of {name} failed ({type(exc).__name__}: "
                               f"{getattr(exc, 'reason', None) or exc.strerror or 'no detail'})") from None
        if len(raw) > 64 * 1024 * 1024:
            raise CatalogError("catalog file exceeds the 64 MiB limit")
        return raw
    if parts.scheme == "file":
        try:
            root = Path(filesystem_url(source))
        except ValueError as exc:
            raise CatalogError(f"catalog source: {exc}") from None
    elif not parts.scheme or (len(parts.scheme) == 1 and os.name == "nt"):
        root = Path(source).expanduser()
    else:
        raise CatalogError("catalog sources support HTTPS, file URLs, and local directories")
    try:
        return (root / name).read_bytes()
    except OSError as exc:
        raise CatalogError(f"cannot read catalog source file {name}: {exc}") from exc


def _activate(config: Config, old: dict, revision: str, pinned: bool) -> dict:
    history = list(old["history"])
    if old["active"] and old["active"] != revision:
        history.append(old["active"])
    new = {"active": revision, "pinned": pinned, "history": history}
    atomic_write(config.catalog_dir / "active.json", encode(new))
    return new


def update(config: Config, *, source: str | None = None, revision: str | None = None) -> dict:
    with locked(config.catalog_dir / ".lock"):
        old = state(config)
        if old["pinned"] and revision is None:
            # A pin is sticky, even for an explicit `update` with no revision.
            installed(config, old["active"])
            return old
        source = source or config.catalog_url
        if revision is None:
            if not source:
                raise CatalogError(
                    "catalog update needs a source: pass --source DIR|URL (a directory made by "
                    "`raincloud catalog pack`, or its HTTPS copy), or set catalog_url in the config; "
                    "there is no hosted catalog endpoint")
            revision = document(_fetch(source, "latest.json", config.offline), "latest.json").get("revision")
        if not isinstance(revision, str) or not REVISION.fullmatch(revision):
            raise CatalogError("upstream revision must be a complete SHA-256 identifier")
        dest = config.catalog_dir / "revisions" / revision
        if dest.exists():
            bundle = installed(config, revision)
        else:
            if not source:
                raise CatalogError("revision is not installed; configure catalog_url or pass --source")
            bundle = Bundle(*(_fetch(source, f"{revision}/{name}", config.offline)
                              for name in ("catalog.json", "sources.json", "snapshot.json"))).validate(revision)
            _install(config.catalog_dir / "revisions", bundle)
        return _activate(config, old, bundle.revision, old["pinned"])


def installed_revisions(config: Config) -> list[str]:
    revisions = config.catalog_dir / "revisions"
    return sorted(d.name for d in revisions.iterdir() if d.is_dir() and REVISION.fullmatch(d.name)) \
        if revisions.is_dir() else []


def expand_revision(config: Config, prefix: str, *, also: tuple[str, ...] = ()) -> str:
    """The one installed revision (or one of `also`) that `prefix` abbreviates.

    Overviews print 12 characters of a revision; typing those back must work.
    """
    if REVISION.fullmatch(prefix):
        return prefix
    if not REVISION_PREFIX.fullmatch(prefix):
        raise CatalogError(f"{prefix!r} is not a catalog revision; `raincloud catalog status` shows the active one")
    matches = sorted({r for r in (*installed_revisions(config), *also) if r.startswith(prefix)})
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise CatalogError(f"no installed catalog revision starts with {prefix}; "
                           "`raincloud catalog status` shows the active one")
    raise CatalogError(f"{prefix} is ambiguous: it abbreviates {', '.join(m[:16] for m in matches)}")


def pin(config: Config, revision: str) -> dict:
    with locked(config.catalog_dir / ".lock"):
        if not REVISION.fullmatch(revision):
            try:
                selected = resolve_context(config).bundle.revision
            except CatalogError:
                selected = None
            revision = expand_revision(config, revision, also=(selected,) if selected else ())
        if not (config.catalog_dir / "revisions" / revision).exists():
            selected = resolve_context(config)
            if selected.bundle.revision != revision:
                raise CatalogError("revision is not installed; fetch it explicitly with catalog update --revision")
            _install(config.catalog_dir / "revisions", selected.bundle)
        installed(config, revision)  # no download, including in offline mode
        return _activate(config, state(config), revision, True)


def unpin(config: Config) -> dict:
    with locked(config.catalog_dir / ".lock"):
        old = state(config)
        if old["active"] is None:
            raise CatalogError("no active catalog to unpin")
        return _activate(config, old, old["active"], False)


def rollback(config: Config) -> dict:
    with locked(config.catalog_dir / ".lock"):
        old = state(config)
        if not old["history"]:
            raise CatalogError("no previous catalog revision")
        revision = old["history"][-1]
        installed(config, revision)
        new = {"active": revision, "pinned": True, "history": old["history"][:-1]}
        atomic_write(config.catalog_dir / "active.json", encode(new))
        return new


DEFAULT_HISTORY_KEEP = 10


def _held_leases(config: Config, *, sweep: bool) -> set[str]:
    """The revisions a running build holds a lease on (see Context.pinned).

    A lease is a lock file its holder keeps locked, so one whose holder died is
    unheld. With `sweep` those are removed, whichever revision they name.
    """
    held = set()
    for lease in sorted((config.catalog_dir / "leases").glob("*.lock")):
        if is_held(lease):
            held.add(lease.name.split(".", 1)[0])
        elif sweep:
            lease.unlink(missing_ok=True)
    return held


def _revision_prefix(selector: str) -> bool:
    """Whether catalog selector `selector` abbreviates a revision id.

    Hex of 4-63 characters that is not an existing path (a bundle directory
    could be named that way).
    """
    return (not REVISION.fullmatch(selector) and REVISION_PREFIX.fullmatch(selector) is not None
            and not Path(selector).expanduser().exists())


def _drop_lease(lease: Path) -> None:
    try:
        lease.unlink(missing_ok=True)
    except PermissionError as exc:
        # Windows: a gc probe has it open. It is unheld now, so gc removes it.
        print(f"[catalog] could not remove the build lease {lease} ({exc}); "
              f"`raincloud catalog gc` removes it", file=sys.stderr)


def gc(config: Config, *, keep: int = DEFAULT_HISTORY_KEEP, dry_run: bool = False) -> dict:
    """Drop installed catalog revisions nothing can reach any more.

    Every activation installs a revision (a full copy of the manifest and
    snapshot) and appends the previous one to `history`; nothing ever removed
    either, so a machine that edits its manifest accumulates a copy per edit for
    the life of the install, and the rollback history grows without bound.

    Kept: the active revision; the `keep` most recent history entries, which is
    exactly what `rollback` can still reach; a revision the settings select by
    id or by prefix (`catalog = <revision>`; every revision an ambiguous prefix
    matches, since removing one would make it select another); and any
    revision a running build holds a lease on. Everything else is deleted. A
    PINNED catalog is left completely alone -- a pin says the revision matters,
    and deciding otherwise is not GC's call. A real run also removes the lease
    files of builds that died; a dry run removes nothing.

    Every call returns the same keys: `pinned`, `dry_run`, `removable` (what a
    real run deletes), `removed` (what this run deleted), `failed` ({revision:
    error} for deletions that did not complete), `kept` (installed revisions a
    real run leaves), `in_use` (leased), `history` (entries kept) and
    `trimmed_history` (entries dropped).
    """
    with locked(config.catalog_dir / ".lock"):
        old = state(config)
        history = list(old["history"])
        present = installed_revisions(config)
        result = {"pinned": old["pinned"], "dry_run": dry_run, "removable": [], "removed": [],
                  "failed": {}, "kept": present, "in_use": [], "history": len(history), "trimmed_history": 0}
        if old["pinned"]:
            return {**result, "note": "catalog is pinned; nothing removed"}
        trimmed = history[-keep:] if keep > 0 else []
        reachable = {r for r in [old["active"], *trimmed] if r}
        if REVISION.fullmatch(config.catalog):
            reachable.add(config.catalog)
        elif _revision_prefix(config.catalog):
            reachable.update(r for r in present if r.startswith(config.catalog))
        held = _held_leases(config, sweep=not dry_run)
        in_use = [r for r in present if r not in reachable and r in held]
        removable = [r for r in present if r not in reachable and r not in in_use]
        result.update(removable=removable, kept=sorted(set(present) - set(removable)), in_use=in_use,
                      history=len(trimmed), trimmed_history=len(history) - len(trimmed))
        if dry_run:
            return result

        revisions = config.catalog_dir / "revisions"
        for revision in removable:
            try:
                shutil.rmtree(revisions / revision)
            except OSError as exc:
                result["failed"][revision] = f"{type(exc).__name__}: {exc}"
            else:
                result["removed"].append(revision)
        if trimmed != history:
            atomic_write(config.catalog_dir / "active.json",
                         encode({"active": old["active"], "pinned": old["pinned"],
                                 "history": trimmed}))
        return result


@dataclass(frozen=True)
class Context:
    bundle: Bundle
    source: str
    # True when the catalog was assembled from loose checkout/bundled files
    # rather than read from a packed revision. It gates checkout-only behaviour:
    # scratch metadata beside the tracked snapshot, and profile promotion.
    legacy: bool = False
    manifest_path: Path | None = None
    snapshot_path: Path | None = None

    # Each access parses the bundle again (a few ms for the full manifest) and
    # returns a NEW mutable dict; callers may edit the result without affecting
    # this Context or anyone else's copy.
    @property
    def manifest(self):
        return document(self.bundle.manifest, "manifest")

    @property
    def snapshot(self):
        return document(self.bundle.snapshot, "snapshot")

    def build_check(self, spec: dict):
        from ._bundle import build_requirements
        required = set(build_requirements({"datasets": [spec]}))
        # Bundle-wide requirements can include future capabilities for other
        # datasets; only the requested recipe is gated here.
        metadata = document(self.bundle.metadata, "catalog.json")
        required |= set(metadata["builders"]) - set(build_requirements(self.manifest))
        missing = required - set(capabilities()["builders"])
        if missing:
            raise BuildToolingMissing(f"unsupported build capabilities: {sorted(missing)}; update Raincloud to build this recipe")

    @contextmanager
    def pinned(self, config: Config):
        """Yield `config` selecting this catalog by revision id, installed and
        leased for the duration.

        Installs this bundle into `catalog_dir` (a catalog-state write), because
        a revision id resolves only once installed. A build child resolves
        `catalog=<revision>` afresh on every read, for hours; `gc` in another
        process must not delete the revision under it. The lease is a lock file
        taken under the catalog lock (so gc, which holds that lock, sees it or
        the revision was never at risk). THIS process holds it: if this process
        dies, the lease goes with it, even while a child it started still runs.
        """
        revision = self.bundle.revision
        lease = config.catalog_dir / "leases" / f"{revision}.{os.getpid()}-{uuid.uuid4().hex[:8]}.lock"
        with ExitStack() as stack:
            # Registered first, so it runs last: the file is removed after its
            # lock is released (Windows refuses to delete a locked open file).
            stack.callback(_drop_lease, lease)
            with locked(config.catalog_dir / ".lock"):
                _install(config.catalog_dir / "revisions", self.bundle)
                stack.enter_context(locked(lease))
            yield replace(config, catalog=revision, manifest=None, snapshot=None)


_current: ContextVar[Context | None] = ContextVar("raincloud_catalog", default=None)


def current() -> Context | None:
    return _current.get()


def resolve_context(config: Config | None = None, *, repo_root: Path | None = None) -> Context:
    config = config or get_config()
    from ._catalog import _data_file, _repo_root
    root = repo_root if repo_root is not None else _repo_root()
    selector = config.catalog
    if config.manifest is not None:
        if selector not in {"auto", "local"}:
            raise CatalogError("select a catalog OR a manifest override, not both")
        manifest_path, snapshot_path = config.manifest, config.snapshot
        catalog_id = "local:" + digest(str(manifest_path.resolve()).encode())
        source, legacy = "local", False
    else:
        if selector == "local":
            raise CatalogError("catalog 'local' reads the manifest named by the `manifest` setting "
                               "(RAINCLOUD_MANIFEST), and none is set")
        if selector in {"auto", "active"}:
            active = state(config)["active"]
            if active:
                selector = active
            elif selector == "active":
                raise CatalogError("no active catalog; run catalog update or select bundled/checkout")
        if _revision_prefix(selector):
            # The 12-character revision an overview prints.
            selector = expand_revision(config, selector)
        if REVISION.fullmatch(selector):
            bundle = installed(config, selector)
            directory = config.catalog_dir / "revisions" / selector
            return Context(bundle, selector, False, directory / "sources.json", directory / "snapshot.json")
        if selector not in {"auto", "checkout", "bundled"}:
            directory = Path(selector).expanduser()
            revision = None
            if not (directory / "catalog.json").is_file() and (directory / "latest.json").is_file():
                # A `pack` directory: publishing a release rewrites latest.json,
                # so a machine config naming the directory follows it without
                # a config edit or a symlink swap.
                revision = document((directory / "latest.json").read_bytes(), "latest.json").get("revision")
                if not isinstance(revision, str) or not REVISION.fullmatch(revision):
                    raise CatalogError(f"{directory / 'latest.json'}: revision must be a complete SHA-256 identifier")
                directory = directory / revision
            bundle = _read_bundle(directory, revision)
            return Context(bundle, str(directory), False, directory / "sources.json", directory / "snapshot.json")
        if selector == "bundled" or (selector == "auto" and not (root / "sources.json").is_file()):
            directory = Path(str(resources.files("raincloud").joinpath("_data")))
            manifest_path, snapshot_path = directory / "sources.json", directory / "snapshot.json"
            if not manifest_path.is_file():
                raise CatalogError("there is no bundled catalog here: it ships only inside a built raincloud "
                                   "wheel; in a source checkout select `checkout`")
            source = "bundled"
        else:
            manifest_path = root / "sources.json"
            snapshot_path = _data_file("snapshot", config)
            source = "checkout"
        catalog_id, legacy = "raincloud", True
        if config.snapshot is not None:
            snapshot_path = config.snapshot

    def assemble():
        used = snapshot_path
        try:
            raw = manifest_path.read_bytes()
            manifest = document(raw, "manifest")
            blank = encode({"schema_version": manifest.get("schema_version"), "slugs": {}})
            if snapshot_path is None:
                return make_bundle(raw, blank, catalog_id), None
            snap = snapshot_path.read_bytes()
            snapshot = document(snap, "snapshot")
        except OSError as exc:
            raise CatalogError(f"cannot read {source} catalog: {exc}") from exc
        skew = (snapshot.get("schema_version"), manifest.get("schema_version"))
        if skew[0] != skew[1] and legacy and config.snapshot is None:
            if source == "bundled":
                # A wheel packages one snapshot; a schema_version bump that did not
                # reach pyproject.toml's force-include ships a mismatched pair.
                raise CatalogError(
                    f"this raincloud installation is broken: its packaged snapshot is schema_version "
                    f"{skew[0]} but its packaged manifest is {skew[1]}; reinstall a fixed release")
            # Dropping the snapshot is right -- it describes another layout's
            # artifacts -- but it also drops every recorded sha256 and size, so
            # mirror bytes stop being verified. That must not happen silently.
            # Once per change of these files per process, by design: _parsed
            # caches the result. stacklevel 4 skips assemble, _parsed and
            # resolve_context, naming resolve_context's caller.
            warnings.warn(
                f"{snapshot_path} is schema_version {skew[0]} but {manifest_path} is {skew[1]}; "
                f"ignoring the snapshot, so no recorded checksum or size applies and mirror "
                f"downloads are not verified (regenerate docs/v{skew[1]}/snapshot.json)",
                RuntimeWarning, stacklevel=4)
            snap, used = blank, None
        return make_bundle(raw, snap, catalog_id), used

    key = ("files", source, catalog_id, legacy, config.snapshot is None)
    bundle, snapshot_path = _parsed(key, [p for p in (manifest_path, snapshot_path) if p is not None], assemble)
    return Context(bundle, source, legacy, manifest_path, snapshot_path)


@contextmanager
def operation(config: Config, context: Context | None = None):
    context = context or resolve_context(config)
    with use_config(config):
        token = _current.set(context)
        try:
            yield context
        finally:
            _current.reset(token)


def status(config: Config) -> dict:
    context = resolve_context(config)
    return {**state(config), "selected": context.source, "catalog_id": context.bundle.catalog_id,
            "revision": context.bundle.revision, "catalog_dir": str(config.catalog_dir)}


def selected_context() -> Context | None:
    """The catalog pipeline views (list, status, browse, fetch) read, or None.

    None means "read the checkout directly", including scratch metadata beside
    the tracked snapshot. That is the answer only in a source checkout with
    nothing selected: an operation over the checkout's own files, or no
    operation, `catalog = auto`, no manifest/snapshot override and no active
    installed catalog. An explicit selection, an active catalog, an operation
    over anything else, or a wheel install (no checkout) returns that context.
    """
    context = current()
    if context is not None:
        return None if context.source == "checkout" and context.legacy else context
    config = get_config()
    from ._catalog import _repo_root
    if (config.catalog != "auto" or config.manifest is not None or config.snapshot is not None
            or state(config)["active"] or not (_repo_root() / "sources.json").is_file()):
        return resolve_context(config)
    return None
