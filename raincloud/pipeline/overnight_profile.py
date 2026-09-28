# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Overnight driver: build → profile → promote → wipe for every unprofiled slug.

The run pins the selected catalog by revision, in this process and in every
child stage, so all of them agree on one observation directory:
`<data_dir>/.raincloud/observations/<revision>/profiles/` — also for a
checkout, whose `--inspect` and browser read that directory beside docs/v{n}.
It never writes tracked docs. Slugs with a profile already promoted there are
skipped; for the rest:

  1. If the parquet isn't built locally, run `raincloud.pipeline.build <slug>`.
  2. Run `raincloud.pipeline.profile <slug> --no-promote` → `outputs/v{n}/<slug>/profile.json`.
  3. Run `raincloud.pipeline.promote_profiles <slug>` → the observation directory.
  4. Wipe `outputs/v{n}/<slug>/` and `outputs/raw_downloads/<slug>/` to keep
     the disk from filling up over a multi-hour run. The version-scoped wipe is
     REFUSED when `v{n}` is frozen by a newer sibling on disk (or the versions
     cannot be listed), so a v1-pinned manifest can't delete artifacts that v2
     has superseded.

Per-slug failures are logged but don't abort the run. A `--budget-mins` limit
(default unbounded) per slug stops slow stages from monopolising the night. A
slug's status is one of ok, skipped-no-build, build-failed, profile-timeout,
profile-failed, promote-timeout, promote-failed or driver-error. Artifacts are
wiped after a failure only when this run built them and the failure was not a
timeout; otherwise they are kept (`retained`) for a later pass.

Usage:
    python -m raincloud.pipeline.overnight_profile [--skip-build] [--budget-mins N]
    python -m raincloud.pipeline.overnight_profile --slugs slug-a slug-b ...
    python -m raincloud.pipeline.overnight_profile --max-slugs 50    # stop after N

Outputs:
    outputs/_overnight.log    — running JSON-lines log (slug, status, secs, error)
    outputs/_overnight.state  — the last slug and result (informational; nothing reads it)
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

from raincloud.catalogs import current, operation
from raincloud.config import get_config

from .lifecycle import entry_for_slug, maintenance
from .spec import (
    is_hydrated,
    load_manifest,
    outputs_base,
    outputs_root,
    prepared_parquet,
    raw_downloads_root,
    raw_slug_dir,
    recipe_workdir_root,
    workdir_root,
)

# Subprocess working directory; artifact and observation paths use the
# selected catalog and configuration.
REPO_ROOT = Path(__file__).resolve().parents[2]


def _log_path() -> Path:
    return outputs_base() / "_overnight.log"


def _state_path() -> Path:
    return outputs_base() / "_overnight.state"


def _log(event: dict) -> None:
    log_path = _log_path()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    event["ts"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    with open(log_path, "a") as f:
        f.write(json.dumps(event) + "\n")
    # Also echo to stdout for tailing.
    print(json.dumps(event), flush=True)


def _profiled_slugs() -> set[str]:
    """Slugs with a promoted profile in this run's observation directory.

    Only that directory counts: a built profile does not survive the wipe, and
    display fallbacks (a checkout's docs/v1/profiles) describe another build.
    """
    from .promote_profiles import profile_observations_dir
    manifest = load_manifest()
    promoted = profile_observations_dir(manifest)
    return {spec["slug"] for spec in manifest["datasets"]
            if (promoted / f"{spec['slug']}.json").is_file()}


def _slug_already_built(slug: str) -> bool:
    return prepared_parquet(slug).exists()


def frozen_version_reason() -> str | None:
    """Why the version-scoped outputs wipe must be refused, or None if it's safe.

    `outputs_root()` is scoped off the manifest's `schema_version`, so running
    this tool against a v1-pinned manifest would target `outputs/v1/<slug>/` —
    artifacts a later schema_version has already superseded and that nothing
    rebuilds. A version with a newer sibling on disk is FROZEN: deleting from it
    is never what an overnight disk-hygiene pass wants, so refuse rather than
    trust that `schema_version` happens to be the current one.

    Fails closed: when the version directories cannot be listed, that is a
    reason too, since a newer sibling may be hiding behind the error.
    """
    base = outputs_base()
    version_dir = outputs_root().name
    if not (version_dir.startswith("v") and version_dir[1:].isdigit()):
        return None
    try:
        # os.scandir raises on a permission error; pathlib's glob skips it.
        with os.scandir(base) as entries:
            present = [int(e.name[1:]) for e in entries
                       if e.name.startswith("v") and e.name[1:].isdigit() and e.is_dir()]
    except FileNotFoundError:
        return None  # no outputs at all: nothing versioned to protect
    except OSError as exc:
        return f"cannot enumerate {base}: {exc}"
    if not present:
        return None
    newest = max(present)
    if int(version_dir[1:]) < newest:
        return f"{version_dir} is frozen by newer v{newest} present in {base}"
    return None


@maintenance(resources=True)
def _wipe_slug(slug: str) -> None:
    """Free disk: remove the slug's raw_downloads, version-scoped outputs and workdir.

    Runs under the data, raw and scratch write locks (`maintenance(resources=True)`).
    The profile is removed with current-version outputs after promotion. Only
    the selected raw and recipe scratch generations are cleared; legacy checkout
    scratch is also eligible. Frozen output versions survive (see
    `frozen_version_reason`)."""
    entry = entry_for_slug(slug)
    spec = next(s for s in current().manifest["datasets"] if s["slug"] == slug)
    raw_root = raw_downloads_root() / slug
    selected_raw = raw_slug_dir(slug)
    # Raw generations are shared across data stores. Never recursively remove
    # the slug root: .recipes may contain another catalog's cache generations.
    if selected_raw != raw_root or raw_root.is_symlink():
        raw_targets = [selected_raw]
    elif raw_root.is_dir():
        raw_targets = [p for p in raw_root.iterdir() if p.name != ".recipes"]
    else:
        raw_targets = []
    targets = [*raw_targets, recipe_workdir_root(spec, current().manifest) / slug]
    # Unscoped scratch belongs to the legacy checkout only.
    if entry.legacy:
        targets.append(workdir_root() / slug)
    reason = frozen_version_reason()
    if reason:
        _log({"slug": slug, "action": "wipe-skipped-frozen", "reason": reason})
    else:
        targets.insert(1, outputs_root() / slug)
    for p in targets:
        if p.exists() or p.is_symlink():
            try:
                if p.is_dir() and not p.is_symlink():
                    shutil.rmtree(p)
                else:
                    p.unlink()
            except Exception as e:
                _log({"slug": slug, "action": "wipe-error", "path": str(p), "error": str(e)})
    if (selected_raw == raw_root and not raw_root.is_symlink()
            and raw_root.is_dir() and not any(raw_root.iterdir())):
        raw_root.rmdir()


def _run_stage(slug: str, stage: str, *args: str, timeout: int | None) -> tuple[int, str]:
    """Run a pipeline stage as a subprocess; return (returncode, tail of stderr)."""
    cmd = [sys.executable, "-m", f"raincloud.pipeline.{stage}", *args, slug]
    if stage == "build":
        # Auto-clean _workdir/ between builds so decompressed intermediates
        # (Public BI bz2→csv can hit ~100 GB) don't accumulate.
        cmd.insert(-1, "--clean-workdir")
    if stage == "profile":
        # Promotion is its own stage below, with its own status; the profile
        # child must not attempt it too.
        cmd.insert(-1, "--no-promote")
    if stage == "promote_profiles":
        cmd = [sys.executable, "-m", "raincloud.pipeline.promote_profiles", slug]
    try:
        proc = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            env={**os.environ, **get_config().subprocess_env(), "PYTHONUNBUFFERED": "1"},
        )
        tail = (proc.stderr or "")[-2000:]
        return proc.returncode, tail
    except subprocess.TimeoutExpired:
        return 124, f"timeout after {timeout}s"


def process_slug(slug: str, *, skip_build: bool, budget_secs: int | None) -> dict:
    started = time.time()
    built_here = False
    if not _slug_already_built(slug):
        if skip_build:
            return {"slug": slug, "status": "skipped-no-build"}
        rc, tail = _run_stage(slug, "build", timeout=budget_secs)
        if rc != 0:
            return {"slug": slug, "status": "build-failed", "secs": int(time.time() - started),
                    "error": tail[-600:].replace("\n", " | ")}
        built_here = True

    # Profile. A timeout says nothing about the artifacts, and a build this run
    # did not make may have taken hours: keep both. Only a build this run made,
    # whose profile then genuinely failed, is wiped to free disk.
    rc, tail = _run_stage(slug, "profile", timeout=budget_secs)
    if rc != 0:
        status = "profile-timeout" if rc == 124 else "profile-failed"
        result = {"slug": slug, "status": status, "secs": int(time.time() - started),
                  "error": tail[-600:].replace("\n", " | ")}
        if built_here and rc != 124:
            _wipe_slug(slug)
        else:
            result["retained"] = "artifacts kept for a later profile"
        return result

    # Promote. A failure keeps the artifacts and the profile: under the 120s
    # ceiling it is ordinarily a lost lock race, retryable by a later pass.
    rc, tail = _run_stage(slug, "promote_profiles", timeout=120)
    if rc != 0:
        status = "promote-timeout" if rc == 124 else "promote-failed"
        return {"slug": slug, "status": status, "secs": int(time.time() - started),
                "retained": "artifacts and profile kept for a later promote",
                "error": tail[-600:].replace("\n", " | ")}

    # Cleanup
    _wipe_slug(slug)
    return {"slug": slug, "status": "ok", "secs": int(time.time() - started)}


def _main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m raincloud.pipeline.overnight_profile",
                                description=__doc__.split("\n", 1)[0])
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

    m = load_manifest()
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
        # Hydrated datasets fetch from the open web; they are built only by name.
        candidates = [d["slug"] for d in m["datasets"] if d["slug"] not in profiled and not is_hydrated(d)]
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
        state_path = _state_path()
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps({
            "last_slug": slug, "last_result": result, "processed": processed + 1,
        }, indent=2) + "\n")
        processed += 1

    _log({"event": "end", "processed": processed})
    return 0


def main(argv=None):
    config = get_config()
    from raincloud.catalogs import resolve_context
    context = current() or resolve_context(config)
    # The children re-resolve the revision for hours; the lease keeps `gc`
    # from deleting it under them. Resolve the pinned selection in the parent
    # too: source="checkout" would choose tracked oracles while its pinned
    # children choose revision data.
    with context.pinned(config) as pinned, operation(pinned, resolve_context(pinned)):
        return _main(argv)


if __name__ == "__main__":
    sys.exit(main())
