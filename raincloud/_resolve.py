# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Resolution order: local cache -> mirror -> local build."""
from __future__ import annotations

import importlib
import os
import shutil
import subprocess
import sys
from pathlib import Path

from . import _cache, _transport
from ._catalog import load_catalog
from .exceptions import (
    ArtifactNotFound,
    BuildToolingMissing,
    FormatUnavailable,
    OfflineMiss,
)


def artifact_key(slug: str, fmt: str) -> str:
    # v1 is hardcoded across the loader; revisit at a schema_version bump
    return f"v1/{slug}/{fmt}/{slug}.{_cache.EXT[fmt]}"


def _mirror_base(mirror: str | None) -> str | None:
    base = mirror if mirror is not None else os.environ.get("RAINCLOUD_MIRROR")
    return base.rstrip("/") if base else None


def _build_available() -> bool:
    """True only if the build pipeline's heavy deps are importable.

    `scripts.pipeline.build` is packaged into the wheel even in a loader-only
    install, so `find_spec` is insufficient (it only checks the file exists).
    The module must be actually importable, which requires the `[build]` extra
    — a bare loader install fails the import (e.g. missing zstandard) and
    correctly falls through to the BuildToolingMissing message.
    """
    try:
        importlib.import_module("scripts.pipeline.build")
        return True
    except ImportError:
        return False


def resolve(
    slug: str,
    fmt: str,
    *,
    mirror: str | None = None,
    offline: bool | None = None,
    allow_build: bool = True,
) -> Path:
    cat = load_catalog()
    entry = cat.entry(slug)  # raises UnknownSlug
    if fmt not in entry.formats:
        raise FormatUnavailable(
            f"{slug}: format {fmt!r} not available; have {sorted(entry.formats)}"
        )
    dest = _cache.cache_path(slug, fmt)
    expected = entry.formats[fmt].sha256

    # 1) cache hit
    if dest.exists() and (expected is None or _cache.sha256_file(dest) == expected):
        return dest

    is_offline = _cache.is_offline() if offline is None else offline
    if is_offline:
        raise OfflineMiss(f"{slug}/{fmt} not cached and offline mode is on")

    # 2) mirror
    base = _mirror_base(mirror)
    if base is not None:
        url = f"{base}/{artifact_key(slug, fmt)}"
        tmp = dest.parent / f".{dest.name}.part"
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            _transport.fetch(url, tmp)
            return _cache.adopt(tmp, dest, expected)
        except ArtifactNotFound:
            pass  # fall through to build

    # 3) local build
    if allow_build and _build_available():
        subprocess.run(
            [sys.executable, "-m", "scripts.pipeline.build", slug], check=True
        )
        from scripts.pipeline.spec import output_format_dir  # type: ignore

        built = output_format_dir(slug, fmt) / f"{slug}.{_cache.EXT[fmt]}"
        if not built.exists():
            raise ArtifactNotFound(f"build produced no {fmt} for {slug}")
        tmp = dest.parent / f".{dest.name}.part"
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copyfile(built, tmp)
            return _cache.adopt(tmp, dest, None)  # locally built: trusted
        except Exception:
            if tmp.exists():
                tmp.unlink()
            raise

    raise BuildToolingMissing(
        f"{slug}/{fmt} not cached and not in mirror; "
        f"install `raincloud[build]` or set RAINCLOUD_MIRROR"
    )
