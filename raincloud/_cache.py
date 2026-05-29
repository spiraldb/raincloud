# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Local artifact cache: paths, sha256 verification, atomic adoption."""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path

from .exceptions import ChecksumMismatch

# Format -> on-disk file extension. Identity for today's two formats, but kept
# as a map so a future format whose extension differs from its name (e.g. a
# "parquet-hydrated" tier -> "parquet") slots in without touching call sites.
EXT = {"parquet": "parquet", "vortex": "vortex"}

_TRUTHY = {"1", "true", "yes", "on"}


def cache_root() -> Path:
    env = os.environ.get("RAINCLOUD_CACHE")
    if env:
        # .expanduser() for parity with _catalog._data_file and
        # scripts.pipeline.spec._env_path so `RAINCLOUD_CACHE=~/foo` resolves
        # to the home dir rather than a literal ./~/foo.
        return Path(env).expanduser()
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".cache"
    return base / "raincloud"


def cache_path(slug: str, fmt: str) -> Path:
    # schema_version is 1 today; hardcoded to match the loader's artifact_key + the pipeline's outputs/v1 layout
    return cache_root() / "v1" / slug / fmt / f"{slug}.{EXT[fmt]}"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):  # 1 MiB chunks
            h.update(chunk)
    return h.hexdigest()


def is_offline() -> bool:
    return os.environ.get("RAINCLOUD_OFFLINE", "").lower() in _TRUTHY


def strict_checksum() -> bool:
    """True if `RAINCLOUD_STRICT_CHECKSUM` opts into hard checksum failures.

    Default (off) is the 'drift is an alert' policy: a sha mismatch warns and
    adopts. Strict (on) is for security-sensitive deployments / CI:

      - A slug that HAS a pinned sha and comes from the mirror is rehashed on
        every cache hit and on download; a mismatch raises ChecksumMismatch
        (catches even same-size on-disk tampering of a previously-verified
        file). This is the integrity property strict buys, at the cost of a
        rehash per load.
      - A slug with NO pinned sha has nothing to rehash against, so strict
        changes nothing for it — the size/pin corruption check still applies.
      - A LOCALLY-BUILT artifact can't be verified against the maintainer's
        sha (a non-reproducible build legitimately differs), so it is trusted
        via its provenance pin (origin=build + the snapshot pin it was built
        against) rather than rebuilt every load. It is rebuilt only when that
        snapshot pin changes (the source of truth moved) — see _resolve.
        For full cryptographic integrity, point strict deployments at a mirror.
    """
    return os.environ.get("RAINCLOUD_STRICT_CHECKSUM", "").lower() in _TRUTHY


def pin_path(dest: Path) -> Path:
    """Sidecar recording which snapshot pin a cached file was reconciled against."""
    return dest.parent / f".{dest.name}.pin"


def read_pin(dest: Path) -> dict | None:
    """Return {"snap_sha": str|None, "size": int, "origin": str|None} for
    `dest`, or None.

    Returns None when the sidecar is absent, unreadable, or contains valid JSON
    that isn't an object (e.g. a torn/partial write leaving `42` or `[...]`) —
    callers do `pin.get(...)`, so a non-dict must not slip through and raise
    AttributeError inside resolve(). `origin` is absent on pins written before
    it was added; callers must treat a missing `origin` as untrusted (None).
    """
    try:
        pin = json.loads(pin_path(dest).read_text())
    except (OSError, ValueError):
        return None
    return pin if isinstance(pin, dict) else None


def _write_pin(dest: Path, snap_sha: str | None, size: int,
               origin: str | None = None) -> None:
    """Best-effort: record the snapshot sha (may be None), on-disk size, and
    origin (`build` / `mirror` / None) of the bytes we just adopted.

    Lets a later resolve() recognise bytes it deliberately adopted and serve
    them from cache instead of re-fetching/rebuilding every load. This covers
    cases the snapshot size alone can't bless: (a) already-adopted *drift*
    (bytes diverging from a pinned sha), (b) a slug with no pinned sha at all
    (`snap_sha=None`), and (c) a locally-built artifact whose bytes legitimately
    differ from the maintainer's snapshot. `origin` is what lets strict mode
    distinguish "my own local build against this snapshot pin" (trust the
    provenance) from "mirror bytes" (must rehash). A pin is hygiene, not
    correctness — a write failure just costs a rehash on the next load.
    """
    p = pin_path(dest)
    tmp = p.parent / f"{p.name}.{os.getpid()}.tmp"
    try:
        tmp.write_text(json.dumps({"snap_sha": snap_sha, "size": size, "origin": origin}))
        os.replace(tmp, p)  # atomic: a concurrent reader never sees a torn pin
    except OSError:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def adopt(
    tmp: Path,
    dest: Path,
    expected_sha256: str | None,
    *,
    strict: bool = False,
    slug: str | None = None,
    origin: str | None = None,
) -> Path:
    """Atomically move tmp -> dest, optionally checking against a known sha.

    Default semantics (`strict=False`): if `expected_sha256` is set and the
    bytes disagree, print a `[raincloud]` warning to stderr identifying the
    slug + origin, then adopt anyway. Upstream data drifts; we want the user
    informed, not blocked. The loader's mirror-fetch path passes
    `strict=strict` (i.e. `strict=True` under RAINCLOUD_STRICT_CHECKSUM) to
    turn a mismatch into a `ChecksumMismatch`; the local-build path always
    passes `strict=False` (a client build legitimately differs from the
    maintainer's bytes). NOTE: `scripts.pipeline.publish` does NOT go through
    adopt — it gates uploads with its own `PublishMismatch` in `plan_uploads`.

    When `expected_sha256` is None, verification is skipped — there's nothing
    pinned to alert against.

    `origin` (`build` / `mirror`) is recorded in the pin so a later strict
    resolve can trust a locally-built artifact by provenance instead of
    rebuilding it every load.

    On any failure, the tmp file is removed and dest is left untouched.
    """
    try:
        if expected_sha256 is not None:
            actual = sha256_file(tmp)
            if actual != expected_sha256:
                if strict:
                    raise ChecksumMismatch(
                        f"{tmp}: expected sha256 {expected_sha256}, got {actual}"
                    )
                label = slug or dest.name
                where = f" from {origin}" if origin else ""
                # A local build legitimately differs from the maintainer's
                # snapshot bytes (parquet/vortex output is rarely bit-stable
                # across library versions), so don't cry "upstream changed".
                why = ("locally built; differs from the maintainer's snapshot "
                       "(expected for non-reproducible formats)"
                       if origin == "build"
                       else "adopting anyway — upstream may have changed")
                print(
                    f"[raincloud] WARN: {label}{where} sha256 drifted "
                    f"(got {actual[:12]}…, snapshot expected {expected_sha256[:12]}…); "
                    f"{why}.",
                    file=sys.stderr,
                )
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, dest)  # atomic within a filesystem
        # Always record (snap_sha, size, origin) we just adopted so a later
        # resolve() can serve these exact bytes from cache without a rehash —
        # see _write_pin for the cases this covers (drift, sha-less slugs,
        # locally-built artifacts). origin lets strict mode trust a local
        # build by provenance instead of rebuilding it every load.
        _write_pin(dest, expected_sha256, dest.stat().st_size, origin)
        return dest
    finally:
        if tmp.exists():
            tmp.unlink()
