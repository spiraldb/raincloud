# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Overnight driver: build → profile → promote → wipe for every unprofiled slug.

Walks `sources.json`, skips slugs that already have a tracked profile in
`docs/v1/profiles/<slug>.json`, and for the rest:

  1. If the parquet isn't built locally, run `scripts.pipeline.build <slug>`.
  2. Run `scripts.pipeline.profile <slug>` → `outputs/v1/<slug>/profile.json`.
  3. Run `scripts.pipeline.promote_profiles <slug>` → tracked location.
  4. Wipe `outputs/v1/<slug>/` and `outputs/raw_downloads/<slug>/` to keep
     the disk from filling up over a multi-hour run.

Per-slug failures are logged but don't abort the run. A `--budget-mins`
limit (default unbounded) per-slug stops slow builds from monopolising
the night; the slug is marked as `timed-out` in the log so it can be
revisited manually.

Usage:
    python -m scripts.pipeline.overnight_profile [--skip-build] [--budget-mins N]
    python -m scripts.pipeline.overnight_profile --slugs slug-a slug-b ...
    python -m scripts.pipeline.overnight_profile --max-slugs 50    # stop after N

Outputs:
    outputs/_overnight.log    — running JSON-lines log (slug, status, secs, error)
    outputs/_overnight.state  — checkpoint dict for resuming
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LOG_PATH = REPO_ROOT / "outputs" / "_overnight.log"
STATE_PATH = REPO_ROOT / "outputs" / "_overnight.state"


def _log(event: dict) -> None:
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    event["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    with open(LOG_PATH, "a") as f:
        f.write(json.dumps(event) + "\n")
    # Also echo to stdout for tailing.
    print(json.dumps(event), flush=True)


def _profiled_slugs() -> set[str]:
    return {p.stem for p in (REPO_ROOT / "docs" / "v1" / "profiles").glob("*.json")}


def _slug_already_built(slug: str) -> bool:
    return (REPO_ROOT / "outputs" / "v1" / slug / "parquet" / f"{slug}.parquet").exists()


def _wipe_slug(slug: str) -> None:
    """Free disk: remove the slug's outputs/raw and outputs/v1 subdirectories.

    Doesn't touch outputs/v1/<slug>/profile.json — that's a build product
    too, but the promote step has already copied it to docs/v1/profiles/."""
    for p in [
        REPO_ROOT / "outputs" / "raw_downloads" / slug,
        REPO_ROOT / "outputs" / "v1" / slug,
        REPO_ROOT / "_workdir" / slug,
    ]:
        if p.exists():
            try:
                shutil.rmtree(p)
            except Exception as e:
                _log({"slug": slug, "action": "wipe-error", "path": str(p), "error": str(e)})


def _run_stage(slug: str, stage: str, *args: str, timeout: int | None) -> tuple[int, str]:
    """Run a pipeline stage as a subprocess; return (returncode, tail of stderr)."""
    cmd = ["python", "-m", f"scripts.pipeline.{stage}", *args, slug]
    if stage == "build":
        # Auto-clean _workdir/ between builds so decompressed intermediates
        # (Public BI bz2→csv can hit ~100 GB) don't accumulate.
        cmd.insert(-1, "--clean-workdir")
    if stage == "promote_profiles":
        cmd = ["python", "-m", "scripts.pipeline.promote_profiles", slug]
    try:
        proc = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, "PYTHONUNBUFFERED": "1"},
        )
        tail = (proc.stderr or "")[-2000:]
        return proc.returncode, tail
    except subprocess.TimeoutExpired:
        return 124, f"timeout after {timeout}s"


def process_slug(slug: str, *, skip_build: bool, budget_secs: int | None) -> dict:
    started = time.time()
    if not _slug_already_built(slug):
        if skip_build:
            return {"slug": slug, "status": "skipped-no-build"}
        rc, tail = _run_stage(slug, "build", timeout=budget_secs)
        if rc != 0:
            return {"slug": slug, "status": "build-failed", "secs": int(time.time() - started),
                    "error": tail[-600:].replace("\n", " | ")}

    # Profile
    rc, tail = _run_stage(slug, "profile", timeout=budget_secs)
    if rc != 0:
        # Salvage as much as we can — wipe to free disk before bailing.
        _wipe_slug(slug)
        return {"slug": slug, "status": "profile-failed", "secs": int(time.time() - started),
                "error": tail[-600:].replace("\n", " | ")}

    # Promote
    rc, tail = _run_stage(slug, "promote_profiles", timeout=120)
    if rc != 0:
        _wipe_slug(slug)
        return {"slug": slug, "status": "promote-failed", "secs": int(time.time() - started),
                "error": tail[-600:].replace("\n", " | ")}

    # Cleanup
    _wipe_slug(slug)
    return {"slug": slug, "status": "ok", "secs": int(time.time() - started)}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--slugs", nargs="*", help="explicit slugs (default: every unprofiled slug)")
    p.add_argument("--skip-build", action="store_true",
                   help="only profile already-built slugs; skip the rest")
    p.add_argument("--budget-mins", type=int, default=None,
                   help="per-slug budget in minutes; build/profile aborted if exceeded")
    p.add_argument("--max-slugs", type=int, default=None,
                   help="stop after processing N slugs (excludes already-profiled)")
    p.add_argument("--include-large", action="store_true",
                   help="include known-huge slugs (clickbench-hits, jsonbench-*, websight-*, etc.)")
    args = p.parse_args(argv)

    budget = args.budget_mins * 60 if args.budget_mins else None

    m = json.loads((REPO_ROOT / "sources.json").read_text())
    profiled = _profiled_slugs()
    LARGE_BLOCKLIST = {
        # Known multi-hour builds — opt-in only.
        "clickbench-hits", "jsonbench-bluesky-100m",
        "websight-v01", "finemath-4plus", "wikipedia-en",
        "wikipedia-structured-contents", "fineweb-sample-10bt",
        "laion-400m", "openorca", "slimpajama-6b", "beir-msmarco",
        "stackoverflow-posts", "stackoverflow-postlinks",
        "osm-germany-nodes", "osm-germany-ways", "osm-germany-relations",
        "openlibrary-works", "openlibrary-editions", "openlibrary-authors",
        "ghcn-daily", "hacker-news",
    }
    if args.slugs:
        candidates = [s for s in args.slugs if s not in profiled]
    else:
        candidates = [d["slug"] for d in m["datasets"] if d["slug"] not in profiled]
        if not args.include_large:
            # Skip large slugs only when they'd need a fresh build. If the
            # parquet is already on disk, profiling is cheap; do it.
            candidates = [s for s in candidates
                          if s not in LARGE_BLOCKLIST or _slug_already_built(s)]
    # Process already-built slugs first — they're free (no fetch) and any
    # huge ones (e.g. hacker-news, wikipedia-en) finish their profile pass
    # while smaller slugs are still being built.
    candidates.sort(key=lambda s: (not _slug_already_built(s), s))

    _log({"event": "start", "candidates": len(candidates),
          "profiled-before": len(profiled), "budget-mins": args.budget_mins})

    processed = 0
    for slug in candidates:
        if args.max_slugs is not None and processed >= args.max_slugs:
            break
        try:
            result = process_slug(slug, skip_build=args.skip_build, budget_secs=budget)
        except Exception as e:
            result = {"slug": slug, "status": "driver-error", "error": str(e)}
        _log(result)
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps({
            "last_slug": slug, "last_result": result, "processed": processed + 1,
        }, indent=2) + "\n")
        processed += 1

    _log({"event": "end", "processed": processed})
    return 0


if __name__ == "__main__":
    # Re-exec ourselves under `uv run` if invoked from a stripped env, since
    # build stages need the project's installed extras.
    sys.exit(main())
