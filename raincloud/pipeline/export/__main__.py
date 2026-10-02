# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage `run_exporters` on its own: derive output formats from canonicals already on disk.

`build` runs every stage, so re-deriving Parquet or Vortex from a canonical that
is already correct meant re-fetching and re-transforming — hours for a generated
slug, and it rewrites the canonical for no reason. Whenever a change touches only
the export stage (row-group sizing, a codec, adding a cell), this is the whole
job: the canonical Arrow spine is the input and it is untouched.

    python -m raincloud.pipeline.export <slug> [<slug> ...]
    python -m raincloud.pipeline.export --all
    python -m raincloud.pipeline.export --all --format parquet@rs

Refuses a slug with no canonical rather than building one — that is `build`'s job,
and quietly starting a multi-hour build from a command meant to re-export is
exactly the surprise this exists to avoid. Refuses, too, a canonical this
install built from an earlier recipe (fetch, parse or transform changed since),
whose exports would be recorded as the current recipe's, and one that is neither
this install's build nor the catalog's file, whose exports could not be recorded
at all (`records.check_canonical`). A cell named with `--format` whose writer is
not installed fails the slug.

Without `--format` it writes the formats a build would: the install's
`formats` setting (only Vortex by default), among those the dataset offers.

A planned writer (one of those formats, or a bare `--format vortex`) that cannot
produce its file -- it raises, dies, reports a failed round-trip or exceeds
`RAINCLOUD_EXPORT_TIMEOUT` -- is recorded as that format's "unavailable"
measurement (`records.record_unavailable`), as in a build. Without `--format`
the slug still counts as exported, with an `[unavailable]` line, and the run
exits 0. With `--format` the run asked for exactly those formats, so one that
was not produced fails the slug (exit 1); a writer named outright
(`--format vortex@rs`) is this run's choice, and its failure is not recorded.
Either way the previous file comes back, and when it is this install's export
of the same canonical it stays the dataset's file (nothing is recorded).

A failure already measured is skipped, not repeated: when the measurement that
applies (this install's build record at the recipe, else the catalog's) records
the writer that would run now -- planned, or named with `--format` -- with the
same toolchain, reading the same canonical, a `[skip]` line quotes it and
nothing new is recorded. Exit status follows the same rule as a failure:
without `--format` the slug counts as exported and the summary lists the
skipped formats (exit 0); with `--format` the requested file was not produced,
so the slug fails (exit 1). `--retry-errors` attempts it anyway: a success
replaces the measurement, a planned writer's failure records it again.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from raincloud._formats import build_formats
from raincloud.config import get_config

from ..lifecycle import maintenance
from ..selection import select_or_exit
from ..spec import check_env_knobs, display_path, load_manifest, prepared_arrow
from . import get_exporter, plan


@maintenance(resources=True)
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m raincloud.pipeline.export", allow_abbrev=False,
        description="Re-derive output formats from existing canonical Arrow artifacts.")
    ap.add_argument("slugs", nargs="*", help="specific slugs to export")
    ap.add_argument("--all", action="store_true",
                    help="export every dataset except hydrated ones, which are exported by name")
    ap.add_argument("--format", action="append", metavar="FORMAT", dest="formats",
                    help="export only this format (parquet), or this format by a named "
                         "writer (parquet@rs); repeatable. Overrides the install's `formats` setting. "
                         "Either way the file is <fmt>/<slug>.<ext>.")
    ap.add_argument("--dry-run", action="store_true",
                    help="list what would be exported and exit")
    ap.add_argument("--retry-errors", action="store_true",
                    help="attempt a format even when its writer, with this toolchain, already failed to "
                         "write it at this recipe (skipped by default; see `[skip]` lines)")
    args = ap.parse_args(argv)
    try:
        check_env_knobs()  # fail before touching anything
    except ValueError as exc:
        ap.error(str(exc))

    if args.formats:
        from raincloud._formats import EXPORTED_FORMATS
        for cell in args.formats:  # fail before touching anything
            if "@" not in cell:
                if cell not in EXPORTED_FORMATS:
                    ap.error(f"no exported format {cell!r}; formats: {', '.join(EXPORTED_FORMATS)}")
                continue
            try:
                get_exporter(cell)
            except KeyError:
                ap.error(f"no exporter registered for {cell!r}")
    selected = select_or_exit(ap, load_manifest(), args.slugs, all_=args.all, verb="export")

    from ..records import check_canonical, export_from_canonical
    n_exported = n_missing = n_failed = 0
    known_failures: list = []
    for spec in selected:
        slug = spec["slug"]
        canonical: Path = prepared_arrow(slug)
        if not canonical.exists():
            print(f"  [no canonical] {slug} — build it first", file=sys.stderr)
            n_missing += 1
            continue
        try:
            # Refuse a stale or unknown canonical before planning anything.
            status = check_canonical(canonical)
            formats = args.formats
            if formats is None:
                formats = build_formats(spec, load_manifest()["schema_version"], get_config())
            cells = plan(spec, formats)
            if args.dry_run:
                print(f"  would export {slug} from {display_path(canonical)}: {', '.join(cells)}")
                n_exported += 1
                continue
            # A --format request replaces the install's formats for this run only.
            failed, skipped = [], []
            results = export_from_canonical(spec, canonical, formats, status=status,
                                            on_unavailable=lambda failure, recorded: failed.append(failure),
                                            on_skip=skipped.append, retry_errors=args.retry_errors)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as e:  # noqa: BLE001 — a native panic is one slug's failure
            print(f"  [failed] {slug}: {type(e).__name__}: {e}", file=sys.stderr)
            n_failed += 1
            continue
        for r in results:
            print(f"    {r.format_id}: {r.nbytes:,} bytes")
        # With --format the run asked for exactly these files: one not produced,
        # failed or skipped as a known failure, fails the request.
        if (failed or skipped) and args.formats:
            print(f"  [failed] {slug}: {', '.join(f'{u.format} not exported' for u in [*failed, *skipped])}",
                  file=sys.stderr)
            n_failed += 1
            continue
        known_failures += [(slug, known) for known in skipped]
        # A cell the plan named that wrote nothing, and whose writer did not
        # fail, is a sidecar that is not installed: named outright, its absence
        # fails the request.
        done = {r.format_id for r in results} | {u.cell for u in [*failed, *skipped]}
        absent = [cell for cell in cells if cell not in done]
        if absent:
            print(f"  [failed] {slug}: {'nothing exported' if not results else 'only partly exported'}"
                  f" ({', '.join(absent)} not installed)", file=sys.stderr)
            n_failed += 1
            continue
        n_exported += 1

    print(f"\nexported {n_exported}"
          f"{f', {n_missing} without a canonical' if n_missing else ''}"
          f"{f', {n_failed} failed' if n_failed else ''}"
          f"{f', {len(known_failures)} known failure(s) not retried' if known_failures else ''}")
    for slug, known in known_failures:
        print(f"  [skip] {slug}/{known.format}: {known.cell} failed at this recipe on "
              f"{known.measurement.get('measured_at') or 'an unrecorded date'}; --retry-errors attempts it")
    # A slug named outright that has no canonical is a failed request; under
    # --all it is only unbuilt.
    return 1 if n_failed or (n_missing and not args.all) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
