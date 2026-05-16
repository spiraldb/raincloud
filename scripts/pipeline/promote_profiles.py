# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Mirror per-slug profile.json into the tracked snapshot directory.

Profiles live at `outputs/v1/<slug>/profile.json` (gitignored, build-time
artefact). Cloners who don't run `python -m scripts.pipeline.profile` need
those files too — the TUI's Columns sparklines depend on them. This script
copies built profiles into the tracked location:

    docs/v{n}/profiles/<slug>.json

mirroring how `docs/v{n}/snapshot.json` is the tracked counterpart to a
freshly-generated `docs/snapshot.json`. Idempotent: byte-identical
destinations are skipped so re-running doesn't churn mtimes or the diff.

Usage:
    python -m scripts.pipeline.promote_profiles            # every built profile
    python -m scripts.pipeline.promote_profiles slug-a slug-b
    python -m scripts.pipeline.promote_profiles --check    # exit-code audit; no writes
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .spec import REPO_ROOT, load_manifest, outputs_root


def _tracked_profiles_dir(manifest: dict | None = None) -> Path:
    m = manifest if manifest is not None else load_manifest()
    return REPO_ROOT / "docs" / f"v{m['schema_version']}" / "profiles"


def _iter_built_profiles(manifest: dict) -> list[tuple[str, Path]]:
    """Return [(slug, source_path)] for every existing outputs/v{n}/<slug>/profile.json."""
    root = outputs_root(manifest)
    return [
        (p.parent.name, p)
        for p in sorted(root.glob("*/profile.json"))
    ]


def promote(slugs: list[str] | None = None, *, check_only: bool = False) -> tuple[int, int, list[str]]:
    """Mirror built profiles into the tracked dir.

    Returns (copied_count, skipped_count, missing_slugs).
    `slugs=None` promotes every built profile; otherwise just the named slugs.
    `check_only=True` reports what would change without writing.
    """
    manifest = load_manifest()
    dest_dir = _tracked_profiles_dir(manifest)
    dest_dir.mkdir(parents=True, exist_ok=True)

    available = dict(_iter_built_profiles(manifest))
    if slugs:
        targets = [(s, available[s]) for s in slugs if s in available]
        missing = sorted(set(slugs) - set(available))
    else:
        targets = list(available.items())
        missing = []

    copied = 0
    skipped = 0
    for slug, src in targets:
        dst = dest_dir / f"{slug}.json"
        src_bytes = src.read_bytes()
        if dst.exists() and dst.read_bytes() == src_bytes:
            skipped += 1
            continue
        if check_only:
            copied += 1   # would-copy
            continue
        dst.write_bytes(src_bytes)
        copied += 1
    return copied, skipped, missing


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m scripts.pipeline.promote_profiles",
        description=__doc__.split("\n\n", 1)[0],
    )
    p.add_argument("slugs", nargs="*", help="slugs to promote (default: every built profile)")
    p.add_argument("--check", action="store_true",
                   help="report would-be changes without writing; exit 1 if anything would change")
    args = p.parse_args(argv)

    copied, skipped, missing = promote(args.slugs or None, check_only=args.check)
    if missing:
        print(
            "no built profile for slug(s) (run `python -m scripts.pipeline.profile`):\n  "
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
