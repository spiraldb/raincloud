# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Local artifact cache: paths, sha256 verification, atomic adoption."""
from __future__ import annotations

import hashlib
import os
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
        return Path(env)
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


def verify(path: Path, expected_sha256: str) -> None:
    actual = sha256_file(path)
    if actual != expected_sha256:
        raise ChecksumMismatch(
            f"{path}: expected sha256 {expected_sha256}, got {actual}"
        )


def adopt(tmp: Path, dest: Path, expected_sha256: str | None) -> Path:
    """Verify (if a checksum is known) then atomically move tmp -> dest.

    On any failure, the tmp file is removed and dest is left untouched.
    """
    try:
        if expected_sha256 is not None:
            verify(tmp, expected_sha256)
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, dest)  # atomic within a filesystem
        return dest
    finally:
        if tmp.exists():
            tmp.unlink()
