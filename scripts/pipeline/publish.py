# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Sync built artifacts to a mirror bucket, gated on snapshot sha256 match.

    python -m scripts.pipeline.publish <slug>... --mirror s3://bucket/prefix
    python -m scripts.pipeline.publish --all --mirror file:///tmp/mirror --dry-run
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from pathlib import Path

import fsspec

from raincloud._cache import sha256_file
from raincloud._resolve import artifact_key

from .spec import default_snapshot, load_manifest
from .spec import outputs_root as _outputs_root


class PublishMismatch(Exception):
    """On-disk artifact sha256 disagrees with the snapshot."""


def plan_uploads(slugs, snapshot, *, outputs_root: Path):
    """Return [(local_path, remote_key)] for present artifacts.

    `outputs_root` is the UNVERSIONED `outputs/` directory; the version segment
    is supplied by the loader's `artifact_key` (so local path, remote key, and
    the path the loader reads are derived from ONE source). An artifact whose
    on-disk sha256 disagrees with the snapshot raises PublishMismatch; an
    artifact with no snapshot checksum yet (None) is included without a gate
    (e.g. a freshly built, not-yet-snapshotted slug).
    """
    slug_snaps = snapshot.get("slugs", {})
    plan: list[tuple[Path, str]] = []
    for slug in slugs:
        snap = slug_snaps.get(slug, {})
        for fmt in ("parquet", "vortex"):
            key = artifact_key(slug, fmt)  # "v1/<slug>/<fmt>/<slug>.<ext>"
            local = outputs_root / key
            if not local.exists():
                continue
            expected = snap.get(f"{fmt}_sha256")
            if expected is not None and sha256_file(local) != expected:
                raise PublishMismatch(
                    f"{slug}/{fmt}: on-disk sha256 != snapshot. Regenerate the "
                    f"snapshot with `python -m scripts.pipeline.docs snapshot "
                    f"--rehash` (a plain regen reuses the stale sha on a size "
                    f"match) and re-promote docs/v1/snapshot.json before publishing."
                )
            plan.append((local, key))
    return plan


def _upload(local: Path, target: str) -> None:
    """Stream `local` to the fsspec URL `target` via a temp key + rename.

    Writing straight to the canonical key would leave a truncated object there
    on a mid-stream crash, which the loader's mirror path would then warn-and-
    adopt (non-strict) or adopt silently (no sha). Uploading to `<key>.<uuid>.part`
    and renaming means the final key only ever appears complete.
    """
    fs, path = fsspec.core.url_to_fs(target)
    parent = path.rsplit("/", 1)[0] if "/" in path else ""
    if parent:
        # url_to_fs's LocalFileSystem defaults to auto_mkdir=False; object
        # stores treat this as a no-op. Guard so a fresh mirror prefix works.
        try:
            fs.makedirs(parent, exist_ok=True)
        except (FileExistsError, NotImplementedError):
            pass
    tmp = f"{path}.{uuid.uuid4().hex}.part"
    try:
        with open(local, "rb") as src, fs.open(tmp, "wb") as out:
            for chunk in iter(lambda: src.read(1024 * 1024), b""):
                out.write(chunk)
        fs.mv(tmp, path)
    except Exception:
        try:
            if fs.exists(tmp):
                fs.rm(tmp)
        except Exception:
            pass
        raise


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

    # Resolve the snapshot the same way the loader does (RAINCLOUD_SNAPSHOT ->
    # checkout -> wheel-packaged), so publish gates against the file the loader
    # will trust rather than a hardcoded REPO_ROOT path.
    snapshot = json.loads(default_snapshot().read_text())
    # _outputs_root() returns the versioned dir (outputs/v1); artifact_key
    # re-adds the version segment, so pass the unversioned parent here.
    plan = plan_uploads(slugs, snapshot, outputs_root=_outputs_root(manifest).parent)
    base = args.mirror.rstrip("/")
    for local, key in plan:
        target = f"{base}/{key}"
        print(("DRY " if args.dry_run else "") + f"{local} -> {target}")
        if not args.dry_run:
            _upload(local, target)
    print(f"{'planned' if args.dry_run else 'published'} {len(plan)} artifact(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
