# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Resolution order: local cache -> mirror -> local build."""
from __future__ import annotations

import importlib
import os
import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

from . import _cache, _transport
from ._catalog import load_catalog
from .exceptions import (
    ArtifactNotFound,
    BuildFailed,
    BuildToolingMissing,
    FormatUnavailable,
    OfflineMiss,
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


def artifact_key(slug: str, fmt: str) -> str:
    # v1 is hardcoded across the loader; revisit at a schema_version bump
    return f"v1/{slug}/{fmt}/{slug}.{_cache.EXT[fmt]}"


def _mirror_base(mirror: str | None) -> str | None:
    base = mirror if mirror is not None else os.environ.get("RAINCLOUD_MIRROR")
    return base.rstrip("/") if base else None


def _build_import_error() -> BaseException | None:
    """Return the exception that blocks importing the build pipeline, or None.

    `scripts.pipeline.build` is packaged into the wheel even in a loader-only
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
        importlib.import_module("scripts.pipeline.build")
        return None
    except Exception as e:  # noqa: BLE001 — both classes mean "can't build"
        return e


def _build_available() -> bool:
    # Boolean convenience wrapper. resolve() uses _build_import_error() directly
    # (it needs the exception to craft the right message); this stays as the
    # readable predicate exercised by the test suite.
    return _build_import_error() is None


def resolve(
    slug: str,
    fmt: str,
    *,
    mirror: str | None = None,
    offline: bool | None = None,
    allow_build: bool = True,
    entry=None,
) -> Path:
    # `entry` is passed by Dataset.path_for (already resolved); fall back to a
    # lookup for direct callers. load_catalog().entry(slug) raises UnknownSlug.
    if entry is None:
        entry = load_catalog().entry(slug)
    if fmt not in entry.formats:
        raise FormatUnavailable(
            f"{slug}: format {fmt!r} not available; have {sorted(entry.formats)}"
        )
    dest = _cache.cache_path(slug, fmt)
    expected = entry.formats[fmt].sha256
    expected_size = entry.formats[fmt].nbytes

    # 1) cache hit. Several short-circuits, cheapest first, so a full sha256
    #    over multi-GB artifacts never runs on the common load:
    #
    #    No pinned sha (the ~80% of the catalog with only a byte size): a pin
    #    matching the on-disk size serves immediately (covers deliberately
    #    adopted / locally-built bytes); else the snapshot byte size is used as
    #    a cheap corruption check — a size mismatch with no vouching pin warns
    #    and re-fetches rather than serving possibly-truncated bytes on trust.
    #
    #    With a pinned sha:
    #      a) size matches the pin   -> treat as the blessed artifact. (A
    #         same-size, different-content snapshot revision is the one case
    #         this can't tell apart — an accepted, pre-existing blind spot.)
    #      b) bytes are drift we already adopted against THIS snapshot pin
    #         (pin sidecar records snap sha + adopted size, and the file is
    #         still that size) -> serve it. Without this, knowingly-adopted
    #         drift matches neither (a) nor a sha match and would be
    #         re-fetched/rebuilt on *every* load.
    #      c) full sha matches the pin -> serve (legacy cache w/o a pin, or a
    #         post-revision re-download landing here).
    #    Otherwise warn and fall through to re-fetch (genuine snapshot revision
    #    or external corruption).
    #
    #    Strict mode (RAINCLOUD_STRICT_CHECKSUM) only changes behavior for a
    #    slug that HAS a pinned sha: it rehashes such a cache hit and serves
    #    only a sha match (so mirror drift adopted by a prior non-strict run
    #    can't slip through) — with ONE provenance exception: a locally-built
    #    artifact (origin=build pin) adopted against the current snapshot pin
    #    is served without rehashing, since there's no maintainer sha a
    #    non-reproducible build could match. Sha-less slugs have nothing to
    #    rehash against, so strict leaves their size/pin path unchanged.
    strict = _cache.strict_checksum()
    if dest.exists():
        cached_size = dest.stat().st_size
        if expected is None:
            # No pinned sha (true for ~80% of the catalog today). We can't
            # rehash, so the snapshot byte size is the cheap corruption check —
            # but mirror semantics mirror the strict branch: only a
            # *build-origin* pin overrides a size disagreement (a local build
            # legitimately differs from the maintainer's size, so its provenance
            # is the trust signal). A mirror/pin-less artifact must still match
            # the snapshot size, so a snapshot revision that ships a new
            # (still-sha-less) size is detected as stale and re-fetched instead
            # of serving the old cache forever.
            pin = _cache.read_pin(dest)
            if (pin is not None and pin.get("origin") == "build"
                    and pin.get("size") == cached_size):
                return dest
            if expected_size is None or cached_size == expected_size:
                return dest
            print(
                f"[raincloud] WARN: cached {slug}/{fmt} size {cached_size} != snapshot "
                f"{expected_size} and no local-build pin vouches for it; re-fetching.",
                file=sys.stderr,
            )
        elif strict:
            # Strict verifies UNTRUSTED bytes (mirror / pin-less) against the
            # snapshot sha on every load — this catches even same-size on-disk
            # tampering of a previously-verified file. But a locally-built
            # artifact has no maintainer sha to match (a non-reproducible build
            # legitimately differs), so trust its provenance pin instead of
            # rebuilding every load: an origin=build artifact adopted against
            # THIS snapshot pin (snap_sha == expected) and unchanged on disk
            # (size match) is served. A snapshot revision (expected changes)
            # makes the pin stale, so it falls through and the slug is rebuilt
            # — the cache is trusted until the source of truth moves.
            pin = _cache.read_pin(dest)
            if (pin is not None and pin.get("origin") == "build"
                    and pin.get("snap_sha") == expected
                    and pin.get("size") == cached_size):
                return dest
            if _cache.sha256_file(dest) == expected:
                return dest
            print(
                f"[raincloud] WARN: cached {slug}/{fmt} sha256 != snapshot (strict); "
                f"re-fetching from mirror/build.",
                file=sys.stderr,
            )
        else:
            if expected_size is not None and cached_size == expected_size:
                return dest
            pin = _cache.read_pin(dest)
            if pin is not None and pin.get("snap_sha") == expected and pin.get("size") == cached_size:
                return dest
            if _cache.sha256_file(dest) == expected:
                return dest
            print(
                f"[raincloud] WARN: cached {slug}/{fmt} sha256 drifted from snapshot; "
                f"re-fetching from mirror/build.",
                file=sys.stderr,
            )

    is_offline = _cache.is_offline() if offline is None else offline
    if is_offline:
        raise OfflineMiss(f"{slug}/{fmt} not cached and offline mode is on")

    # 2) mirror. Drifted bytes warn-and-adopt (upstream changes are not panic
    #    cases); a clean miss falls through to local build.
    base = _mirror_base(mirror)
    if base is not None:
        url = f"{base}/{artifact_key(slug, fmt)}"
        dest.parent.mkdir(parents=True, exist_ok=True)
        _sweep_stale_parts(dest)
        tmp = _tmp_path(dest)
        try:
            _transport.fetch(url, tmp)
            return _cache.adopt(tmp, dest, expected, strict=strict, slug=slug, origin="mirror")
        except ArtifactNotFound:
            pass  # fall through to build

    # 3) local build. Works from a wheel install too: scripts.pipeline reads the
    #    packaged manifest and writes under data_root() (~/.cache/raincloud) when
    #    there's no checkout. Requires the [build] extra (see _build_import_error).
    #    Only probe importability when we'd actually build — the import is wasted
    #    work when allow_build is False.
    if allow_build:
        build_err = _build_import_error()
        if build_err is None:
            try:
                subprocess.run(
                    [sys.executable, "-m", "scripts.pipeline.build", slug], check=True
                )
            except (subprocess.CalledProcessError, OSError) as e:
                # Honour the typed-error contract — callers catch RaincloudError,
                # not raw subprocess errors. CalledProcessError = non-zero exit;
                # OSError = couldn't even spawn (e.g. a bogus sys.executable).
                raise BuildFailed(f"build of {slug} failed: {e}") from e
            from scripts.pipeline.spec import output_format_dir  # type: ignore

            built = output_format_dir(slug, fmt) / f"{slug}.{_cache.EXT[fmt]}"
            if not built.exists():
                raise ArtifactNotFound(f"build produced no {fmt} for {slug}")
            dest.parent.mkdir(parents=True, exist_ok=True)
            _sweep_stale_parts(dest)
            tmp = _tmp_path(dest)
            try:
                shutil.copyfile(built, tmp)
                # adopt(strict=False, origin="build"): a client's local build
                # legitimately differs from the maintainer's snapshot bytes
                # (columnar output is rarely bit-reproducible), so we never
                # strict-gate it on the maintainer's sha. `expected` is still
                # passed so the pin records (snap_sha, size, origin=build) —
                # that provenance lets BOTH non-strict and strict later loads
                # serve these bytes straight from cache (no rebuild loop). The
                # slug is rebuilt only when the snapshot pin changes (the source
                # of truth moved) or the cached file is corrupted (size drift).
                return _cache.adopt(tmp, dest, expected, strict=False, slug=slug, origin="build")
            except Exception:
                if tmp.exists():
                    tmp.unlink()
                raise
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
