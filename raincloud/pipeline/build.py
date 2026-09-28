# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""End-to-end orchestrator. For one or more datasets selected from sources.json:
fetch → extract → parse → transform, then the tables or batch streams are
written to the canonical Arrow spine (`write_canonical`), its schema and row
count validated, and each exported format derived from it (`run_exporters`),
by the first installed writer in the format's export priority. Streaming
handlers (which return `[]`) write the canonical themselves
(`open_canonical_writer`) and share the same validate → export tail. A derived
dataset (`derive`, e.g. `<parent>-hydrated`) takes its tables from
`hydrate.derive_tables` in place of fetch → transform.

Every file is recorded in this install's build record as soon as it is
committed, so a later writer's failure never leaves an accepted file that the
loader refuses.

A format whose writer cannot produce it for this dataset -- the writer raises,
dies, reports a failed round-trip, or exceeds `RAINCLOUD_EXPORT_TIMEOUT` -- does
not fail the build: the failure is recorded in the build record as that
format's "unavailable" measurement, the dataset is built with the formats that
worked, and an `[unavailable]` line, repeated in the summary, names it. Such a
build exits 0; the loader then reports the format as unavailable, quoting the
measurement, and the catalog shows it once a maintainer regenerates it.

A failure already measured is not repeated: when the measurement that applies
(this install's build record at the recipe, else the catalog's) records the
writer that would run now, with the same toolchain, reading the same canonical,
the format is skipped with a `[skip]` line, listed in the summary, and the
measurement is kept. That is not a new failure: the build exits 0. Pass
`--retry-errors` to attempt it anyway; a success replaces the measurement, a
failure records it again. A different writer or toolchain (an upgraded
library, say) or a new canonical is attempted without it, with a `[retry]`
line saying what changed.

By default the validate stage treats row/schema_hash drift as a warning
(`[WARN]` to stderr) and continues — users invoking a build already opted
into "download whatever's upstream now," so an upstream Arrow-conversion
bump shouldn't brick their build. Pass `--strict` to upgrade those
warnings to errors; that's the recommended setting for CI / pre-release
gates where drift should block.

Examples:
    python -m raincloud.pipeline.build clickbench-hits
    python -m raincloud.pipeline.build uci-iris uci-wine-quality
    python -m raincloud.pipeline.build --all --strict   # CI mode
"""
from __future__ import annotations

import argparse
import shutil
import sys
import traceback

from raincloud._cache import sha256_file
from raincloud.exceptions import BuildToolingMissing

from .canonical import write_canonical
from .export import plan, run_exporters, slug_from_canonical
from .extract import extract
from .fetch import fetch
from .lifecycle import build_outputs
from .parse import parse
from .records import record_build, recorders
from .selection import select_or_exit
from .spec import (
    check_env_knobs,
    display_path,
    load_manifest,
    prepared_arrow,
    recipe_scratch,
    workdir_root,
)
from .transform import transform
from .validate import validate


def _run_one(spec: dict, outputs, *, strict: bool, clean_workdir: bool = False,
             unavailable: list | None = None, skipped: list | None = None,
             unverified: list | None = None, retry_errors: bool = False) -> bool:
    print("\n" + "=" * 72)
    print(f"  {spec['slug']}")
    print("=" * 72)
    try:
        from raincloud.catalogs import current
        context = current()
        context.build_check(spec)
        outputs.preflight(spec["slug"])
        # Fail in a second, not after the transform, when a format this
        # dataset exports has no installed writer here.
        plan(spec)
        if spec.get("derive"):
            # A derived dataset's input is another dataset, not an upstream.
            # Its options arrive through a context variable: `hydrate.main`
            # wraps this call in `hydrate.using(config)`; `raincloud build`
            # takes the safe defaults.
            from .hydrate import derive_tables
            parsed, tables = [], derive_tables(spec)
        else:
            inputs = fetch(spec)
            extracted = extract(spec, inputs)
            # Batch-capable inputs contain lazy streams here, not decoded tables.
            parsed = list(parse(spec, extracted))
            tables = transform(spec, parsed)
        if tables:
            # Validate the whole table batch before publishing its first file.
            # No loop target may hold a table: `del tables` below must free them.
            for out_slug in [item[0] for item in tables]:
                outputs.preflight(out_slug)
            # Tables and planned batch streams → canonical Arrow spine
            # (<slug>.arrow.zstd); `validate` checks it and `run_exporters`
            # derives every exported format from it.
            canonicals = write_canonical(spec, tables)
        else:
            # Direct-writing (streaming) handlers published their canonical.
            canonicals = outputs.canonicals
            if not canonicals:
                raise RuntimeError(
                    f"{spec['slug']}: transform returned [] but wrote no "
                    f"canonical Arrow at {display_path(prepared_arrow(spec['slug']))} — "
                    "a streaming handler must use canonical.open_canonical_writer"
                )
        # Release the producers before validating/encoding the spine.
        del parsed, tables
        validate(spec, canonicals, strict=strict)
        outputs.accept_canonicals(canonicals)
        # Record each file as soon as it is committed: a later exporter's
        # failure must not leave an accepted file the loader refuses.
        for canonical in canonicals:
            record_build({slug_from_canonical(canonical): {
                "arrow": (sha256_file(canonical), canonical.stat().st_size)}})
        for canonical in canonicals:
            slug = slug_from_canonical(canonical)
            def note(failure, recorded, slug=slug):
                if recorded and unavailable is not None:
                    unavailable.append((slug, failure))

            def skip(known, slug=slug):
                if skipped is not None:
                    skipped.append((slug, known))
            def unchecked(result, why, slug=slug):
                if unverified is not None:
                    unverified.append((slug, result.format_id, why))
            # A planned writer's failure is recorded and the build goes on; one
            # already measured with this writer and toolchain is skipped.
            run_exporters(spec, canonical, retry_errors=retry_errors, on_skip=skip,
                          **recorders(slug, note, on_unverified=unchecked))
        if clean_workdir:
            wd = workdir_root() / spec["slug"]
            if wd.exists():
                try:
                    shutil.rmtree(wd)
                except OSError as e:
                    print(f"  [clean] could not remove {display_path(wd)}: {e}", file=sys.stderr)
                else:
                    print(f"  [clean] removed {display_path(wd)}")
        return True
    except NotImplementedError as e:
        print(f"  SKIP (not yet implemented): {e}")
        return False
    except BuildToolingMissing as e:
        # An install without an optional dependency: the message names the
        # extra, and a traceback would only bury it.
        print(f"  FAILED: {e}")
        return False
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as e:
        # Catch BaseException, not just Exception: native code (a handler, a
        # reader) can surface a Rust panic as pyo3_runtime.PanicException, which
        # subclasses BaseException. With a bare `except Exception`, one slug's
        # panic would escape run_one and abort the whole `--all` batch mid-run;
        # here it degrades to a per-slug FAILED. (A writer's panic never gets
        # here: it runs in a child process, and its failure is recorded.)
        # KeyboardInterrupt / SystemExit re-raise above so Ctrl-C still works.
        print(f"  FAILED: {type(e).__name__}: {e}")
        traceback.print_exc()
        return False


def _main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="raincloud build", allow_abbrev=False)
    ap.add_argument("slugs", nargs="*", help="specific slugs to build")
    ap.add_argument("--all", action="store_true",
                    help="build every dataset except hydrated ones, which are built by name")
    ap.add_argument("--strict", action="store_true",
                    help="upgrade validate-stage drift warnings to hard errors "
                         "(off by default; recommended for CI / pre-release gates)")
    ap.add_argument("--clean-workdir", action="store_true",
                    help="after each successful build, remove the selected recipe's scratch directory "
                         "so large decompressed intermediates (e.g. Public BI bz2→csv) "
                         "don't accumulate during batch runs")
    ap.add_argument("--retry-errors", action="store_true",
                    help="attempt a format even when its writer, with this toolchain, already failed to "
                         "write it at this recipe (skipped by default; see `[skip]` lines)")
    args = ap.parse_args(argv)
    # A malformed knob fails now, not at export time after an hours-long fetch.
    try:
        check_env_knobs()
    except ValueError as exc:
        ap.error(str(exc))
    # Every name is checked before any work: a typo is an error with a
    # did-you-mean, never a silently smaller build.
    selected = select_or_exit(ap, load_manifest(), args.slugs, all_=args.all, verb="build")

    ok = failed = 0
    unavailable: list = []
    skipped: list = []
    unverified: list = []
    for spec in selected:
        if run_one(spec, strict=args.strict, clean_workdir=args.clean_workdir, unavailable=unavailable,
                   skipped=skipped, unverified=unverified, retry_errors=args.retry_errors):
            ok += 1
        else:
            failed += 1
    print(f"\nsummary: ok={ok}  failed/skipped={failed}"
          + (f"  unavailable={len(unavailable)}" if unavailable else "")
          + (f"  known failures not retried={len(skipped)}" if skipped else "")
          + (f"  unverified={len(unverified)}" if unverified else ""))
    # Built without a format its writer measured it cannot have: recorded, not failed.
    for slug, failure in unavailable:
        print(f"  [unavailable] {slug}/{failure.format}: {failure.error}")
    # Built without a format whose failure is already measured for this writer
    # and toolchain: the measurement stands, and nothing new failed.
    for slug, known in skipped:
        print(f"  [skip] {slug}/{known.format}: {known.cell} failed at this recipe on "
              f"{known.measurement.get('measured_at') or 'an unrecorded date'}; --retry-errors attempts it")
    # Built with a file its writer published without verifying it reads back.
    for slug, cell, why in unverified:
        print(f"  [unverified] {slug}/{cell.partition('@')[0]}: {cell}: {why}")
    return 0 if failed == 0 else 1


def run_one(spec: dict, *, strict: bool, clean_workdir: bool = False, unavailable: list | None = None,
            skipped: list | None = None, unverified: list | None = None, retry_errors: bool = False) -> bool:
    """Build one dataset; True when it built. A format its planned writer could
    not produce is appended to `unavailable` as (slug, export.Unavailable); one
    skipped as an already-measured failure, to `skipped` as (slug,
    export.Skipped). `retry_errors` attempts those instead. A file promoted
    without its writer verifying it reads back (a sidecar's `roundtrip: null`)
    is appended to `unverified` as (slug, writer cell, its reason)."""
    from .lifecycle import operation_lock

    with operation_lock(resources=True):
        # Lock the shared scratch root before selecting a recipe generation.
        with recipe_scratch(spec):
            with build_outputs(spec) as outputs:
                return _run_one(spec, outputs, strict=strict, clean_workdir=clean_workdir,
                                unavailable=unavailable, skipped=skipped, unverified=unverified,
                                retry_errors=retry_errors)


def main(argv: list[str] | None = None):
    from raincloud.catalogs import operation
    from raincloud.config import get_config
    with operation(get_config()):
        return _main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
