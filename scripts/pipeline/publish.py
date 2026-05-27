# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Sync built artifacts to a mirror bucket, gated on snapshot sha256 match.

    python -m scripts.pipeline.publish <slug>... --mirror s3://bucket/prefix
    python -m scripts.pipeline.publish --all --mirror file:///tmp/mirror --dry-run
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import fsspec

from .spec import REPO_ROOT, load_manifest
from .spec import outputs_root as _outputs_root

EXT = {"parquet": "parquet", "vortex": "vortex"}


class PublishMismatch(Exception):
    """On-disk artifact sha256 disagrees with the snapshot."""


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def plan_uploads(slugs, snapshot, *, outputs_root: Path):
    """Return [(local_path, remote_key)] for present artifacts.

    `outputs_root` is the UNVERSIONED `outputs/` directory; the `v1/` segment
    (matching the loader's `_resolve.artifact_key` and the mirror layout) is
    added here. An artifact whose on-disk sha256 disagrees with the snapshot
    raises PublishMismatch; an artifact with no snapshot checksum yet (None)
    is included without a gate (e.g. a freshly built, not-yet-snapshotted slug).
    """
    slug_snaps = snapshot.get("slugs", {})
    plan: list[tuple[Path, str]] = []
    for slug in slugs:
        snap = slug_snaps.get(slug, {})
        for fmt in ("parquet", "vortex"):
            # v1 matches the loader's artifact_key; revisit at a schema_version bump
            local = outputs_root / "v1" / slug / fmt / f"{slug}.{EXT[fmt]}"
            if not local.exists():
                continue
            expected = snap.get(f"{fmt}_sha256")
            if expected is not None and _sha256(local) != expected:
                raise PublishMismatch(
                    f"{slug}/{fmt}: on-disk sha256 != snapshot; re-run docs snapshot"
                )
            plan.append((local, f"v1/{slug}/{fmt}/{slug}.{EXT[fmt]}"))
    return plan


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Publish built artifacts to a mirror.")
    ap.add_argument("slugs", nargs="*")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--mirror", required=True, help="fsspec base, e.g. s3://b/p")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    if args.all and args.slugs:
        ap.error("pass slugs or --all, not both")

    manifest = load_manifest()
    slugs = ([d["slug"] for d in manifest["datasets"]] if args.all else args.slugs)
    if not slugs:
        ap.error("pass slugs or --all")

    snapshot = json.loads((REPO_ROOT / "docs" / "v1" / "snapshot.json").read_text())
    # _outputs_root() returns the versioned dir (outputs/v1); plan_uploads
    # re-adds the v1/ segment, so pass the unversioned parent here.
    plan = plan_uploads(slugs, snapshot, outputs_root=_outputs_root(manifest).parent)
    base = args.mirror.rstrip("/")
    for local, key in plan:
        target = f"{base}/{key}"
        print(("DRY " if args.dry_run else "") + f"{local} -> {target}")
        if not args.dry_run:
            with open(local, "rb") as src, fsspec.open(target, "wb") as out:
                for chunk in iter(lambda: src.read(1024 * 1024), b""):
                    out.write(chunk)
    print(f"{'planned' if args.dry_run else 'published'} {len(plan)} artifact(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
