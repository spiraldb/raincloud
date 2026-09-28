# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Promote per-slug profiles within the selected catalog's observation namespace.

Built profiles live at outputs/v{n}/<slug>/profile.json. A checkout promotes
into docs/v{n}/profiles/<slug>.json for fresh clones. Custom, activated and
installed catalogs use data_dir/.raincloud/observations/<revision>/profiles/,
without writing into the software installation. Promotion is an atomic,
lock-serialized byte copy of each existing built profile, skipping a
destination that is already byte-identical. A built profile of another
profile schema_version than `profile._PROFILE_SCHEMA_VERSION` is skipped with a
note. Nothing checks that a profile still describes the current artifact (its
`parquet_sha256` is copied as-is).

Usage:
    python -m raincloud.pipeline.promote_profiles            # every built profile
    python -m raincloud.pipeline.promote_profiles slug-a slug-b
    python -m raincloud.pipeline.promote_profiles --check    # exit-code audit; no writes
"""
from __future__ import annotations

import argparse
import sys
from contextlib import nullcontext
from pathlib import Path

from raincloud._locking import atomic_write, locked
from raincloud.catalogs import current, operation, resolve_context
from raincloud.config import get_config

from .lifecycle import operation_lock
from .spec import REPO_ROOT, load_manifest, observations_dir, outputs_root, revision_observations_dir


def profile_observations_dir(manifest: dict | None = None, *, repo_root=None) -> Path:
    """Checkout snapshots remain tracked; all other observations live with data."""
    return observations_dir(manifest, repo_root=repo_root or REPO_ROOT) / "profiles"


def profile_search_paths(slug, manifest=None, *, repo_root=None, output_root=None) -> list[Path]:
    """Every path a profile for `slug` is looked for, freshest first, existing or not.

    The built profile, then the promoted observation. A checkout also reads
    what a pinned run of its catalog promoted into the data store's revision
    observations (overnight_profile runs pinned), then falls back to the frozen
    docs/v1/profiles (display only: it describes the v1 build, and goes when v2
    profiles are promoted for the catalog).
    """
    context = current() or resolve_context(get_config())
    m = manifest if manifest is not None else context.manifest
    root = repo_root or REPO_ROOT
    built = (output_root if output_root is not None else outputs_root(m)) / slug / "profile.json"
    promoted = profile_observations_dir(m, repo_root=root) / f"{slug}.json"
    paths = [built, promoted]
    # The checkout alone retains its historical v1 display fallback. Custom,
    # activated and installed bundles cannot borrow software-global profiles.
    if context.source == "checkout":
        paths.append(revision_observations_dir(get_config(), context) / "profiles" / f"{slug}.json")
        paths.append(root / "docs" / "v1" / "profiles" / f"{slug}.json")
    return list(dict.fromkeys(paths))


def profile_candidates(slug, manifest=None, *, repo_root=None, output_root=None):
    """The `profile_search_paths` that exist, freshest first. Chosen by path and
    existence alone: no check that a profile matches the current artifact."""
    for path in profile_search_paths(slug, manifest, repo_root=repo_root, output_root=output_root):
        if path.is_file():
            yield path


def _iter_built_profiles(manifest: dict) -> list[tuple[str, Path]]:
    """Return [(slug, source_path)] for every existing outputs/v{n}/<slug>/profile.json."""
    root = outputs_root(manifest)
    selected = {spec["slug"] for spec in manifest["datasets"]}
    return [
        (p.parent.name, p)
        for p in sorted(root.glob("*/profile.json"))
        if p.parent.name in selected
    ]


def _stale_schema(raw: bytes) -> str | None:
    """Why a built profile's bytes may not be promoted, or None when they may."""
    import json

    from .profile import _PROFILE_SCHEMA_VERSION
    try:
        version = json.loads(raw).get("schema_version")
    except (ValueError, AttributeError) as exc:
        return f"unreadable ({exc})"
    if version != _PROFILE_SCHEMA_VERSION:
        return f"profile schema_version {version!r}, not {_PROFILE_SCHEMA_VERSION}"
    return None


def promote(slugs: list[str] | None = None, *, check_only: bool = False) -> tuple[int, int, list[str]]:
    """Mirror built profiles into the selected observation directory.

    Returns (copied_count, skipped_count, missing_slugs).
    `slugs=None` promotes every built profile; otherwise just the named slugs.
    `check_only=True` reports what would change without writing.
    """
    # Audits freeze catalog/config selection without asking a read-only store
    # for writable lock files. Publications retain the ordered operation locks.
    config = get_config()
    scope = operation(config, current()) if check_only else operation_lock()
    with scope:
        return _promote(slugs, check_only=check_only)


def _promote(slugs, *, check_only):
    manifest = load_manifest()
    dest_dir = profile_observations_dir(manifest)

    available = dict(_iter_built_profiles(manifest))
    if slugs:
        targets = [(s, available[s]) for s in slugs if s in available]
        missing = sorted(set(slugs) - set(available))
    else:
        targets = list(available.items())
        missing = []

    # Different data roots may promote into the same checkout snapshot.
    guard = nullcontext() if check_only else locked(dest_dir.parent / ".profiles-write.lock")
    with guard:
        copied = 0
        skipped = 0
        for slug, src in targets:
            dst = dest_dir / f"{slug}.json"
            src_bytes = src.read_bytes()
            stale = _stale_schema(src_bytes)
            if stale is not None:
                print(f"[promote] {slug}: not promoting {src}: {stale}; re-profile it", file=sys.stderr)
                skipped += 1
                continue
            if dst.exists() and dst.read_bytes() == src_bytes:
                skipped += 1
                continue
            if check_only:
                copied += 1   # would-copy
                continue
            atomic_write(dst, src_bytes)
            copied += 1
    return copied, skipped, missing


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m raincloud.pipeline.promote_profiles",
        description=__doc__.split("\n\n", 1)[0],
    )
    p.add_argument("slugs", nargs="*", help="slugs to promote (default: every built profile)")
    p.add_argument("--check", action="store_true",
                   help="report would-be changes without writing; exit 1 if anything would change")
    args = p.parse_args(argv)

    copied, skipped, missing = promote(args.slugs or None, check_only=args.check)
    if missing:
        print(
            "no built profile for slug(s) (run `python -m raincloud.pipeline.profile`):\n  "
            + "\n  ".join(missing),
            file=sys.stderr,
        )
    if args.check:
        verb = "would update" if copied else "no changes"
        print(f"{verb}: {copied} profile(s); {skipped} already-tracked")
        return 1 if copied or missing else 0
    print(f"promoted {copied} profile(s); {skipped} unchanged")
    return 2 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
