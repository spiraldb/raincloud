# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""End-to-end orchestrator. Runs fetch → extract → parse → transform →
write → validate for one or more datasets selected from sources.json.

Examples:
    python -m scripts.pipeline.build clickbench-hits
    python -m scripts.pipeline.build --family uci
    python -m scripts.pipeline.build --all --loose
"""
from __future__ import annotations

import argparse
import shutil
import sys
import traceback

from .convert import convert
from .extract import extract
from .fetch import fetch
from .parse import parse
from .spec import REPO_ROOT, iter_datasets, load_manifest
from .transform import transform
from .validate import validate
from .write import write


def run_one(spec: dict, *, strict: bool, clean_workdir: bool = False) -> bool:
    print("\n" + "=" * 72)
    print(f"  {spec['slug']}  [{spec['family']}]")
    print("=" * 72)
    try:
        inputs = fetch(spec)
        extracted = extract(spec, inputs)
        parsed = list(parse(spec, extracted))
        tables = transform(spec, parsed)
        written = write(spec, tables)
        validate(spec, written, strict=strict)
        convert(spec)  # no-op unless spec sets convert.vortex = true
        if clean_workdir:
            wd = REPO_ROOT / "_workdir" / spec["slug"]
            if wd.exists():
                shutil.rmtree(wd, ignore_errors=True)
                print(f"  [clean] removed {wd.relative_to(REPO_ROOT)}")
        return True
    except NotImplementedError as e:
        print(f"  SKIP (not yet implemented): {e}")
        return False
    except Exception as e:
        print(f"  FAILED: {type(e).__name__}: {e}")
        traceback.print_exc()
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("slugs", nargs="*", help="specific slugs to build")
    ap.add_argument("--family", help="build all datasets in this family")
    ap.add_argument("--all", action="store_true", help="build every dataset")
    ap.add_argument("--loose", action="store_true", help="warn on validation failures instead of erroring")
    ap.add_argument("--clean-workdir", action="store_true",
                    help="after each successful build, remove _workdir/<slug>/ "
                         "so large decompressed intermediates (e.g. Public BI bz2→csv) "
                         "don't accumulate during batch runs")
    args = ap.parse_args()

    m = load_manifest()
    selected: list[dict] = []
    if args.slugs:
        for s in args.slugs:
            selected += list(iter_datasets(m, slug=s))
    if args.family:
        selected += list(iter_datasets(m, family=args.family))
    if args.all:
        selected = list(iter_datasets(m))

    if not selected:
        print("no datasets selected; pass slugs, --family, or --all", file=sys.stderr)
        return 2

    ok = failed = 0
    for spec in selected:
        if run_one(spec, strict=not args.loose, clean_workdir=args.clean_workdir):
            ok += 1
        else:
            failed += 1
    print(f"\nsummary: ok={ok}  failed/skipped={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
