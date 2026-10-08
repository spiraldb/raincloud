# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Resolution order: local data/cache -> mirror -> local build."""
from __future__ import annotations

import importlib
import os
import subprocess
import sys
import time
import uuid
import warnings
from contextlib import ExitStack
from pathlib import Path

from . import _builds, _cache, _transport
from ._catalog import load_catalog
from ._formats import WRITERS, select_format
from ._locking import locked
from .config import Config, get_config, redact_url
from .exceptions import (
    ArtifactNotFound,
    BuildFailed,
    BuildToolingMissing,
    FormatUnavailable,
    MirrorUnavailable,
    OfflineMiss,
    UnknownSlug,
)

# Stale .part files (crash / SIGKILL leftovers) older than this get swept on
# the next resolve() attempt for the same dest. Long enough that an actively
# running multi-hour download is safe; short enough that orphans don't pile up.
_STALE_PART_SECONDS = 6 * 3600


def _tmp_path(dest: Path) -> Path:
    """Per-process-unique .part path so concurrent loaders don't race."""
    return dest.parent / f".{dest.name}.{os.getpid()}-{uuid.uuid4().hex[:8]}.part"


def _sweep_stale_parts(dest: Path) -> None:
    """Best-effort cleanup of orphaned .part files in dest.parent.

    Matches `.<dest.name>.*.part` siblings whose mtime is older than the
    stale threshold. Silently ignores errors — sweep is hygiene, not
    correctness.
    """
    if not dest.parent.exists():
        return
    cutoff = time.time() - _STALE_PART_SECONDS
    prefix = f".{dest.name}."
    suffix = ".part"
    try:
        for p in dest.parent.iterdir():
            n = p.name
            if not (n.startswith(prefix) and n.endswith(suffix)):
                continue
            try:
                if p.stat().st_mtime < cutoff:
                    p.unlink()
            except OSError:
                pass
    except OSError:
        pass


def artifact_key(slug: str, fmt: str, version: int) -> str:
    """"v{version}/<slug>/<fmt>/<slug>.<ext>": an artifact's address in any store.

    `version` is the catalog's schema_version (Entry.version); it has no default
    because a forgotten one would quietly address the frozen v1 layout.
    """
    if fmt not in WRITERS:
        raise FormatUnavailable(f"unsupported artifact: {fmt!r}")
    return f"v{version}/{slug}/{fmt}/{slug}.{_cache.EXT[fmt]}"


def _stderr_fd() -> int:
    """A descriptor for a child's output that keeps it off our stdout.

    `raincloud load --build` prints a path for `$(...)`; a build log on stdout
    would be read as part of it.
    """
    try:
        return sys.stderr.fileno()
    except (AttributeError, OSError, ValueError):
        return 2


def _build_import_error() -> BaseException | None:
    """Return the exception that blocks importing the build pipeline, or None.

    `raincloud.pipeline.build` is packaged into the wheel even in a loader-only
    install, so `find_spec` is insufficient (it only checks the file exists).
    The module must be actually importable, which requires the `[build]` extra.

    We distinguish two failure classes so resolve() can give the right hint:
      - ImportError (incl. ModuleNotFoundError): the `[build]` extra isn't
        installed → "install raincloud[build]".
      - any other exception at module-init (a handler raising at top level, a
        malformed packaged manifest): the toolchain IS present but broken →
        surface the actual error rather than misdirecting to a pip install.
    Either way the build is unavailable; the caller decides the message.
    """
    try:
        importlib.import_module("raincloud.pipeline.build")
        return None
    except Exception as e:  # noqa: BLE001 — both classes mean "can't build"
        return e


def _earlier_build(config: Config, key: str, candidate: Path, recipe: str | None) -> bool:
    """Whether `candidate` is this install's build of `key` from a recipe other than `recipe`.

    Only the data dir holds builds; a file in the cache is a mirror download.
    """
    if recipe is None or candidate != config.data_dir / key:
        return False
    built = _builds.lookup(config.data_dir, key)
    return built is not None and built.get("recipe") != recipe


def _from_mirror(base: str, key: str, config: Config, info, slug: str, recipe: str | None) -> Path | None:
    """Fetch `key` from the mirror into the cache, or None when the mirror lacks it.

    The download runs under a per-artifact lock, so concurrent loaders of one
    artifact wait for the first and reuse its file; only the rename into the
    store takes the store lock. Locks are taken in one order, store then
    artifact, and the store lock is never WAITED for while holding the artifact
    lock: a build holds the store lock for hours and may itself load this
    artifact (a derived dataset loads its parent), so waiting there would
    deadlock the two processes. If a writer holds the store when the download
    finishes, the artifact lock is released first. With the default cache_dir
    (the data dir) that means adoption waits for a running build to finish; the
    wait is announced on stderr after ~10s.
    """
    dest = config.cache_dir / key
    store_lock = config.cache_dir / ".raincloud-write.lock"

    def present() -> bool:
        if not dest.is_file():
            return False
        if info.nbytes is not None:
            return dest.stat().st_size == info.nbytes
        # No recorded size: a file prepared() refused as an earlier recipe's
        # build is not the catalog's, so it is replaced, not returned.
        return not _earlier_build(config, key, dest, recipe)

    def adopt() -> Path:
        if present():
            return dest
        _cache.adopt(tmp, dest, info.sha256, expected_size=info.nbytes, slug=slug)
        if dest == config.data_dir / key:
            # The catalog's bytes replaced whatever this install built there.
            _builds.forget(config.data_dir, key)
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path(dest)
    try:
        with locked(dest.parent / f".{dest.name}.lock"):
            if present():
                return dest
            _sweep_stale_parts(dest)
            try:
                _transport.fetch(f"{base}/{key}", tmp)
            except ArtifactNotFound:
                return None
            with ExitStack() as held:
                try:
                    held.enter_context(locked(store_lock, timeout=0))
                except TimeoutError:
                    pass  # a writer has the store: wait for it below, without the artifact lock
                else:
                    return adopt()
        with locked(store_lock):
            if not tmp.is_file() and not present():
                # A wait of more than _STALE_PART_SECONDS let another loader sweep
                # the download. Fetch again, now holding the store (store, then artifact).
                return _from_mirror(base, key, config, info, slug, recipe)
            return adopt()
    finally:
        tmp.unlink(missing_ok=True)


def _local_mirror_problem(base: str) -> str | None:
    """Why the local mirror `base` cannot be read here, or None (always None for a remote one)."""
    if not _transport.is_local(base):
        return None
    try:
        root = Path(_transport.filesystem_url(base))
    except ValueError as exc:
        raise MirrorUnavailable(f"the mirror {redact_url(base)} is not a usable file URL: {exc}") from None
    return None if root.is_dir() else f"{redact_url(base)} is not a directory on this machine"


def _build_available() -> bool:
    # Boolean convenience wrapper. resolve() uses _build_import_error() directly
    # (it needs the exception to craft the right message); this stays as the
    # readable predicate exercised by the test suite.
    return _build_import_error() is None


def measured_unavailable(entry, fmt: str, config: Config) -> dict | None:
    """The build measurement saying `entry`'s `fmt` cannot be made at its
    recipe, or None: this install's build record when it has an entry for the
    current recipe, else the catalog's (`_builds.measured_unavailable`)."""
    info = entry.formats.get(fmt)
    return _builds.measured_unavailable(config.data_dir, artifact_key(entry.slug, fmt, entry.version),
                                        entry.recipe, info.unavailable if info is not None else None)


def unavailable_error(entry, fmt: str, measurement: dict) -> FormatUnavailable:
    """The typed error for a format measured unavailable: it quotes the
    measurement and names what the dataset does have, never a build."""
    from ._formats import describe_unavailable
    others = sorted(f for f in entry.formats if f != fmt)
    return FormatUnavailable(
        f"{entry.slug}/{fmt} is unavailable at this recipe: {describe_unavailable(measurement)}"
        + (f". Available formats: {', '.join(others)}" if others else ""),
        measurement=measurement)


def prepared(entry, fmt: str, config: Config) -> tuple[Path | None, str | None]:
    """The local file that is `entry`'s `fmt` artifact, else (None, why a file there is not).

    Looks in the shared data store, then this user's cache; reads only file
    sizes and the build record. A file of the catalog's size is the catalog's
    file; a different one is served only if this install built it, from the
    current recipe. For a format measured unavailable at the current recipe,
    only a file of the catalog's size is served (the catalog's file); with no
    catalog file, whatever is there is from an earlier build.
    """
    slug = entry.slug
    key = artifact_key(slug, fmt, entry.version)
    info = entry.formats[fmt]
    measurement = measured_unavailable(entry, fmt, config)
    if measurement is not None:
        found = [candidate for candidate in dict.fromkeys((config.data_dir / key, config.cache_dir / key))
                 if info.nbytes is not None and candidate.is_file() and candidate.stat().st_size == info.nbytes]
        return (found[0], None) if found else (None, str(unavailable_error(entry, fmt, measurement)))
    dest = config.cache_dir / key
    different = []
    earlier = None
    for candidate in dict.fromkeys((config.data_dir / key, dest)):
        if candidate.is_file():
            size = candidate.stat().st_size
            built_here = candidate == config.data_dir / key  # builds write the data dir, never the cache
            if info.nbytes is None:
                # No recorded size to recognise the catalog's file by. A build
                # record naming an earlier recipe still identifies a stale file.
                if not _earlier_build(config, key, candidate, entry.recipe):
                    return candidate, None
                earlier = candidate
                continue
            if size == info.nbytes:
                return candidate, None
            if built_here and _builds.serves(config.data_dir, key, size, entry.recipe):
                return candidate, None
            different.append(candidate)
    mismatch = None
    if earlier is not None:
        mismatch = (f"{earlier} was built here from an earlier recipe of {slug}, and the catalog records "
                    f"no size to recognise its own file by. Rebuild it with `raincloud build {slug}`, "
                    f"or select the catalog it came from")
    elif different:
        built = _builds.lookup(config.data_dir, key)
        why = ("it was exported here from an earlier build of the dataset, which a rebuild superseded"
               if built is not None and built.get("superseded") else
               "it was built here from an earlier recipe of the dataset"
               if built is not None and built.get("recipe") != entry.recipe else
               "it is not the catalog's file, and this install's build record does not name it")
        mismatch = (f"{different[0]} is {different[0].stat().st_size} bytes but the catalog's {slug}/{fmt} "
                    f"is {info.nbytes}; {why}. Rebuild it with `raincloud build {slug}`, "
                    f"or select the catalog it came from")
    return None, mismatch


def resolve(
    slug: str,
    fmt: str,
    *,
    mirror: str | None = None,
    offline: bool | None = None,
    allow_build: bool = False,
    entry=None,
    config: Config | None = None,
    context=None,
) -> Path:
    """Where `slug`'s `fmt` artifact is: the data store, the cache, a mirror, or a build.

    The catalog is the authority. A file at the artifact's key with the
    catalog's byte size is that artifact; bytes from a mirror are checked
    against the catalog's sha256 as they arrive. Reads never build unless
    `allow_build`.
    """
    # `entry` is passed by Dataset.path_for (already resolved); fall back to a
    # lookup for direct callers. load_catalog().entry(slug) raises UnknownSlug.
    config = config or get_config()
    if entry is None:
        catalog = load_catalog(config)
        entry = catalog.entry(slug)
        context = catalog.context
    fmt = select_format(entry.formats, fmt)
    key = artifact_key(slug, fmt, entry.version)
    info = entry.formats[fmt]

    # 0) measured unavailable at this recipe: nothing local is served, and a
    #    build would take the same measurement -- unless one was asked for,
    #    which may run a different toolchain. When only this install measured
    #    it and the catalog records a file, a mirror may still hold that file.
    measurement = measured_unavailable(entry, fmt, config)
    fetchable = measurement is not None and info.nbytes is not None
    if measurement is not None and not fetchable and not allow_build:
        raise unavailable_error(entry, fmt, measurement)

    # 1) prepared locally: the shared data store, then this user's cache.
    local, mismatch = prepared(entry, fmt, config)
    if local is not None:
        return local

    is_offline = config.offline if offline is None else offline
    if is_offline:
        refused = "; a build was not attempted because offline mode is on" if allow_build else ""
        raise OfflineMiss((mismatch or f"{slug}/{fmt} not cached and offline mode is on") + refused)

    # 2) mirror: bytes are checked against the catalog as they arrive.
    base = (mirror if mirror is not None else config.mirror)
    base = base.rstrip("/") if base else None
    where = "in a mirror (none is configured)"
    if base is not None and measurement is not None and not fetchable:
        # The catalog records no file to fetch: a build is the only way to one.
        base = None
    if base is not None:
        where = f"in the mirror {redact_url(base)}"
        try:
            problem = _local_mirror_problem(base)
            if problem is not None:
                # Said, because otherwise a mistyped mirror reads exactly like a real miss.
                where = f"in a mirror ({problem})"
                if allow_build:
                    warnings.warn(f"the mirror {problem}; building {slug} locally instead",
                                  RuntimeWarning, stacklevel=3)
            else:
                found = _from_mirror(base, key, config, info, slug, entry.recipe)
                if found is not None:
                    return found
        except MirrorUnavailable as exc:
            if not allow_build:
                raise
            warnings.warn(f"{exc}; building {slug} locally instead", RuntimeWarning, stacklevel=3)

    if not allow_build:
        if measurement is not None:
            raise unavailable_error(entry, fmt, measurement)
        raise ArtifactNotFound(f"{mismatch}; it is not {where} either" if mismatch else
                               f"{slug}/{fmt} is not prepared locally or {where}; "
                               f"build it with `raincloud build {slug}` (or load(..., build=True))")

    # 3) local build. Works from a wheel install too: raincloud.pipeline reads the
    #    selected manifest and writes to the same configured artifact directory.
    #    Requires the [build] extra (see _build_import_error).
    if context is not None:
        spec = next((s for s in context.manifest["datasets"] if s["slug"] == slug), None)
        if spec is None:
            raise UnknownSlug(f"{slug} is not in the {context.source} catalog", slug=slug)
        context.build_check(spec)
    build_err = _build_import_error()
    if build_err is None:
        with ExitStack() as stack:
            pinned = stack.enter_context(context.pinned(config)) if context else config
            try:
                # The build log goes to stderr: stdout is the caller's (a path, or JSON).
                # Only the format asked for: the install's other formats are
                # its own choice to build, not this load's. A v1 catalog predates
                # install formats and builds what its recipe lists, as in 0.3.0.
                command = [sys.executable, "-m", "raincloud.pipeline.build", slug]
                if entry.version >= 2:
                    command += ["--format", fmt]
                subprocess.run(command,
                               check=True,
                               env=pinned.subprocess_env(), stdout=_stderr_fd())
            except (subprocess.CalledProcessError, OSError) as e:
                # Honour the typed-error contract — callers catch RaincloudError,
                # not raw subprocess errors. CalledProcessError = non-zero exit;
                # OSError = couldn't even spawn (e.g. a bogus sys.executable).
                raise BuildFailed(f"build of {slug} failed: {e}") from e
        built = config.data_dir / key
        measurement = measured_unavailable(entry, fmt, config)
        if measurement is not None:
            raise unavailable_error(entry, fmt, measurement)
        if not built.is_file():
            raise ArtifactNotFound(f"build produced no {fmt} for {slug}")
        return built
    if not isinstance(build_err, ImportError):
        # The [build] subtree is present but failed to import for a
        # non-import reason (broken handler, malformed manifest). Surface
        # the real cause rather than telling the user to `pip install`
        # something they already have.
        raise BuildToolingMissing(
            f"{slug}/{fmt} not cached and not in mirror; the build pipeline is "
            f"installed but failed to import: {type(build_err).__name__}: {build_err}"
        )
    raise BuildToolingMissing(
        f"{slug}/{fmt} not cached and not in mirror; "
        f"install `raincloud[build]` or set RAINCLOUD_MIRROR"
    )
