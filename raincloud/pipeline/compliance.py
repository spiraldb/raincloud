# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""`raincloud compliance` — the maintainer-run step that measures the matrix.

Produces the `(slug × format-cell × reader)` read-conformance verdicts that are
the data behind the compliance matrix. For each requested slug it:

1. resolves the slug's canonical Arrow artifact (`prepared_arrow`) — which must
   already exist (this step runs after a build; it is not a builder);
2. runs the enabled write-cells over that canonical to (re)produce artifacts and
   collect each cell's write `Compliance` — the full requested set including the
   sidecar cells, each into its own scratch directory (`compliance_path`), never
   over the dataset's file. A sidecar whose binary is absent is `skip`, never a
   failure;
3. runs every applicable reader over each produced artifact -> `ReadResult`
   verdicts (a reader whose formats don't include the artifact's format -> `na`);
4. assembles + prints a human-readable matrix (rows = artifact cells, cols =
   readers) plus summary counts, and returns a structured `ComplianceReport`.

This is a separate maintainer step. It never gates the default `build`:
`build.run_one` neither imports nor invokes this module (asserted by
`tests/test_compliance.py`). It is tiered + safe — no network, no auto-install,
no toolchain assumptions: a pure-Python machine runs the in-process readers
(`parquet@py`, `vortex@py`) for a real partial matrix and skips the rest with a
note. It may exit non-zero when a reader returns a `fail` verdict or a write-cell
records a measured failure (`roundtrip=False`) — compat-gen style, for CI /
pre-release gates; sidecar/reader absence (`skip`) and `na` do not cause a
non-zero exit, and `spec_ambiguous` is a first-class result, not a failure.

Every write-cell runs under the export time limit (`RAINCLOUD_EXPORT_TIMEOUT`),
as a build's does, so a writer that loops is a measured failure. When a format
the selected catalog records as unavailable (`<fmt>_unavailable` in its
snapshot) round-trips through a write-cell in this run, a `[stale opt-out]`
line says so and the slug's ledger block carries it (`stale_opt_outs`); it is
informational and never gates.

The in-process write-cells are idempotent: an artifact this writer already made
is read from disk (not re-encoded) so measuring doesn't redo a multi-hour encode;
`--reencode` forces a fresh write. `--write-ledger [PATH]` serializes the verbose
matrix (see `ledger.py`). Without PATH, checkout catalogs use
`docs/v{n}/compliance.json`; installed, pinned and custom catalogs use
`<data_dir>/.raincloud/observations/<catalog-revision>/compliance.json`.
An explicit PATH overrides that default. `--check-oracle PATH` separately runs
the additive-only immutable-oracle gate (a cell may be added but never
removed/mutated).

    python -m raincloud.pipeline.compliance <slug> [<slug>...]
    python -m raincloud.pipeline.compliance --all
    python -m raincloud.pipeline.compliance <slug> --cells parquet@py,parquet@java
    python -m raincloud.pipeline.compliance <slug> --readers parquet@py,vortex@jni
    python -m raincloud.pipeline.compliance <slug> --reencode
    python -m raincloud.pipeline.compliance --all --write-ledger ./compliance.json
    python -m raincloud.pipeline.compliance --all --check-oracle docs/v2/compliance.json

`--all` leaves out hydrated datasets (their bytes come from the open web; they
are built only by name), exactly as `build --all` does.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa

from raincloud._cache import Publication, sha256_file
from raincloud.exceptions import BuildToolingMissing

from . import ledger
from .discovery import has_variant
from .export import (
    Compliance,
    ExportFailed,
    ExportResult,
    ReadResult,
    Verdict,
    all_exporters,
    all_readers,
    get_exporter,
    get_reader,
    run_bounded,
    run_reader,
    slug_from_canonical,
)
from .export.bounded import read_back_bounded
from .export.readers import pass_note
from .export.sidecar import SidecarExporter
from .lifecycle import maintenance, require_source
from .selection import SelectionError, select_specs
from .spec import (
    check_env_knobs,
    default_compliance_json,
    display_path,
    load_manifest,
    outputs_base,
    prepared_arrow,
    workdir_root,
)


def compliance_path(exporter, slug: str) -> Path:
    """Where compliance writes `exporter`'s output for `slug`.

    Scratch, one directory per writer cell: a dataset's store file is one per
    format, so side-by-side writers cannot all live there, and a measurement
    must not replace the file readers are served.
    """
    return workdir_root() / slug / "compliance" / exporter.cell_id.replace("@", "-") / exporter.out_path(slug).name


def _written_by(exporter, slug: str) -> Path | None:
    """The dataset's store file for `exporter`'s format, if `exporter` wrote it.

    Who wrote the file on disk is a fact about this install, so this install's
    build record decides when it names a file of that size (and not one a
    newer canonical superseded, see `records.record_build`). Only a file the
    build record does not describe falls back to the catalog, which describes
    its own file (same size); catalogs from before writers were recorded had
    only the Python writers. Anything else is not reused: the writer runs
    again, in scratch.
    """
    from raincloud import _builds
    from raincloud._resolve import artifact_key
    from raincloud.catalogs import current

    path = exporter.out_path(slug)
    context = current()
    if context is None or not path.is_file():
        return None
    size = path.stat().st_size
    fmt, writer = exporter.cell_id.split("@", 1)
    built = _builds.lookup(outputs_base(), artifact_key(slug, fmt, context.manifest["schema_version"]))
    if built is not None and built.get("bytes") == size:
        # A superseded entry names a file made from another canonical.
        return path if built.get("writer") == writer and not built.get("superseded") else None
    entry = context.snapshot.get("slugs", {}).get(slug, {})
    if entry.get(f"{fmt}_bytes") == size and (entry.get(f"{fmt}_writer") or "py") == writer:
        return path
    return None


# Compact, terminal-safe glyphs for the printed matrix (no emoji — width-stable).
GLYPH = {
    "pass": "PASS",
    "fail": "FAIL",
    "na": "-na-",
    "skip": "skip",
    "spec_ambiguous": "amb?",
}
_COUNT_ORDER = ["pass", "fail", "skip", "na", "spec_ambiguous"]


@dataclass
class SlugCompliance:
    """Per-slug compliance outcome — the structured result `ledger` persists."""

    slug: str
    canonical: Path
    write_results: list[ExportResult] = field(default_factory=list)
    # (cell_id, reason) for requested write-cells that produced no artifact
    # (sidecar binary absent, or an unregistered cell).
    skipped_cells: list[tuple[str, str]] = field(default_factory=list)
    read_results: list[ReadResult] = field(default_factory=list)
    # Formats the selected catalog records as unavailable that a write-cell
    # round-tripped in this run: {"format", "cells", "catalog_recorded"}.
    stale_opt_outs: list[dict] = field(default_factory=list)

    def artifact_cells(self) -> list[str]:
        """Write-cells that produced a readable artifact (matrix rows)."""
        return [r.format_id for r in self.write_results if r.out_path.exists()]

    def counts(self) -> dict[str, int]:
        """Read-verdict status -> count over this slug's read results."""
        out = {s: 0 for s in _COUNT_ORDER}
        for rr in self.read_results:
            out[rr.verdict.status] = out.get(rr.verdict.status, 0) + 1
        return out


@dataclass
class ComplianceReport:
    """Full structured result across all requested slugs."""

    slugs: list[SlugCompliance] = field(default_factory=list)
    # slugs requested whose canonical Arrow was missing (build-first errors).
    missing_canonical: list[str] = field(default_factory=list)
    # (slug, reason) for slugs deliberately not measured (`--skip-slug`) — e.g. a
    # known-brutal build (mmmu, code-contests) that OOMs the harness. Recorded in
    # the ledger as an explicit skip block rather than silently omitted, so the
    # oracle carries the whole catalog and a reader can see why a slug is absent.
    skipped_slugs: list[tuple[str, str]] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        total = {s: 0 for s in _COUNT_ORDER}
        for sc in self.slugs:
            for k, v in sc.counts().items():
                total[k] = total.get(k, 0) + v
        return total


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


def default_cells() -> list[str]:
    """Every registered write-cell id (incl. the opt-in sidecars)."""
    return [e.cell_id for e in all_exporters()]


def default_readers() -> list[str]:
    """Every registered reader id."""
    return [r.reader_id for r in all_readers()]


def _run_write_cells(
    spec: dict, canonical: Path, cells: list[str], *, reencode: bool = False
) -> tuple[list[ExportResult], list[tuple[str, str]]]:
    """Run the requested write-cells over the canonical (full set incl. sidecars).

    Idempotent by default, but only for the in-process cells
    (`parquet@py`/`vortex@py`), and only when the on-disk artifact is fresh
    (newer than the canonical) and this writer's: its scratch file, or the
    store file when `_written_by` attributes it to this writer. Their `variant_faithful` verdict is deterministic
    from the canonical schema, so we can synthesize it without re-running the
    (potentially multi-hour) in-process encode. `roundtrip` is measured the way
    the writer itself measures it after a write -- the same read-back, under
    the same ceilings (`bounded.read_back_bounded`) -- so a file is judged the
    same whether this run wrote it or found it. Sidecar cells are never skipped: their real
    verdict lives in the reference-writer's report, which only a fresh run
    produces — so synthesizing a verdict for a pre-existing sidecar artifact
    would fabricate a gate-bearing result we cannot know. Sidecar cells
    are opt-in + fast on the small compliance set, so always re-running them is
    the correct trade. `reencode=True` forces a fresh `export()` for every cell.

    Returns `(produced, skipped)`. A cell with no registered exporter, or a
    sidecar whose binary is absent (`export` -> None), is recorded in `skipped`
    with a reason; a cell that ran (even to a measured failure) or was read from
    a fresh in-process artifact is in `produced`.
    """
    # The canonical's own slug (from its path) is the authority for artifact
    # placement — key `out_path` the same way the exporters do.
    slug = slug_from_canonical(canonical)
    require_source(canonical)
    canonical_mtime = canonical.stat().st_mtime
    produced: list[ExportResult] = []
    skipped: list[tuple[str, str]] = []
    canonical_schema: pa.Schema | None = None  # read lazily — only if a cell is present
    for cell in cells:
        try:
            exporter = get_exporter(cell)
        except KeyError:
            skipped.append((cell, "no exporter registered"))
            continue
        dest = compliance_path(exporter, slug)
        # Idempotent read-if-present only for in-process cells with a fresh
        # artifact (mtime gate): their verdict is deterministic. Sidecar cells
        # + stale artifacts always re-run. The dataset's own file counts when
        # this writer made it, so a multi-hour Parquet encode is not repeated
        # just to be measured.
        in_process = not isinstance(exporter, SidecarExporter)
        present = next((path for path in (_written_by(exporter, slug), dest)
                        if path is not None and path.is_file()
                        and path.stat().st_mtime >= canonical_mtime), None)
        if not reencode and in_process and present is not None:
            dest = present
            if canonical_schema is None:
                with pa.ipc.open_file(str(canonical)) as reader:
                    canonical_schema = reader.schema
            try:
                roundtrip, why = read_back_bounded(exporter, dest, canonical)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BuildToolingMissing as e:
                skipped.append((cell, str(e)))
                continue
            except BaseException as e:  # a read-back that dies or runs out of time is measured
                roundtrip, why = False, str(e).splitlines()[0] if str(e) else type(e).__name__
            produced.append(
                ExportResult(
                    format_id=exporter.cell_id,
                    out_path=dest,
                    # A file that does not read back gets no read cells, as
                    # when this run's own write fails (see `_run_readers`).
                    nbytes=dest.stat().st_size if roundtrip else 0,
                    sha256=sha256_file(dest) if roundtrip else "",
                    compliance=Compliance(
                        roundtrip=roundtrip,
                        variant_faithful=not has_variant(canonical_schema),
                        note=("pre-existing (in-process, fresh); not re-encoded; "
                              + (why or "read back: matches the canonical")),
                    ),
                )
            )
            continue
        with Publication(dest) as publication:
            try:
                # Bounded like a build's export: a writer that loops is a
                # measured failure, not a hung campaign.
                result = run_bounded(exporter, spec, canonical, dest)
            except (KeyboardInterrupt, SystemExit):
                raise
            except BuildToolingMissing as e:
                # The writer's library is absent: like an absent sidecar, this
                # profile cannot measure it.
                skipped.append((cell, str(e)))
                continue
            except BaseException as e:
                # A write cell that raises, dies or runs out of time is a
                # measured write-failure, never a crashed compliance step: the
                # matrix wants to record "vortex@py fails on this slug", not die.
                # sha256="" marks it so `_run_readers` skips reads over any stale
                # artifact. (KeyboardInterrupt/SystemExit re-raise, as in
                # build._run_one.)
                msg = str(e).splitlines()[0] if str(e) else type(e).__name__
                # ExportFailed already names the cell and what happened to it.
                note = msg if isinstance(e, ExportFailed) else f"{exporter.cell_id}: write raised: {msg}"
                produced.append(
                    ExportResult(
                        format_id=exporter.cell_id,
                        out_path=dest,
                        nbytes=0,
                        sha256="",
                        compliance=Compliance(
                            roundtrip=False,
                            variant_faithful=False,
                            note=note,
                        ),
                    )
                )
                continue
            if result is None:
                skipped.append((cell, "reference-writer binary absent"))
                continue
            if result.sha256 and result.compliance.roundtrip is not False:
                publication.accept()
            else:
                # The old artifact is restored on exit. Keep the measured failure,
                # but never let readers mistake restored bytes for this attempt.
                result = replace(result, sha256="", nbytes=0)
            produced.append(result)
    return produced, skipped


def _run_readers(
    produced: list[ExportResult], canonical: Path, reader_ids: list[str]
) -> list[ReadResult]:
    """Run each requested reader over each produced (existing) artifact.

    An unregistered reader id is skipped with a stderr note (never a raw
    KeyError traceback) — mirroring `_run_write_cells`'s handling of an
    unregistered cell, so an operator typo in `--readers` degrades gracefully.

    The diagonal cell of an in-process writer -- its own reader over its own
    file -- is the read the writer already took (`exporters.read_back`: the
    same reader, the same comparison), so a file that read back is not read a
    second time; the cell records that pass.
    """
    readers = []
    for rid in reader_ids:
        try:
            readers.append(get_reader(rid))
        except KeyError:
            print(
                f"[compliance] no reader registered for {rid!r} — skipping",
                file=sys.stderr,
            )
    read_results: list[ReadResult] = []
    for res in produced:
        # Skip reads for a measured write failure. `_failure` sets sha256=""
        # (+ nbytes=0) and does not promote a fresh artifact — but a stale `dest`
        # from a prior good run may still be on disk. Reading that stale artifact
        # would show a green read row while the write row says roundtrip=False —
        # a different (stale) artifact than the verdict describes. The empty-sha
        # sentinel marks the failure so this run contributes no read cells for it.
        if res.sha256 == "" or not res.out_path.exists():
            continue
        read_back = (res.compliance.roundtrip is True
                     and not isinstance(get_exporter(res.format_id), SidecarExporter))
        for reader in readers:
            if read_back and reader.reader_id == res.format_id:
                with pa.ipc.open_file(str(canonical)) as source:
                    note = pass_note(reader.reader_id, source.schema)
                read_results.append(ReadResult(res.format_id, reader.reader_id, Verdict("pass", note=note)))
                continue
            read_results.append(
                run_reader(reader, res.format_id, res.out_path, canonical)
            )
    return read_results


@maintenance(resources=True)
def run_compliance(
    spec: dict,
    *,
    cells: list[str] | None = None,
    reader_ids: list[str] | None = None,
    reencode: bool = False,
) -> SlugCompliance | None:
    """Measure one slug's compliance. Returns `None` if its canonical is absent.

    `cells` / `reader_ids` default to every registered write-cell / reader.
    `reencode=True` forces every write-cell to re-encode even when its artifact
    is already on disk (default: read-if-present, see `_run_write_cells`).
    """
    slug = spec["slug"]
    canonical = prepared_arrow(slug)
    if not canonical.exists():
        return None
    cells = cells if cells is not None else default_cells()
    reader_ids = reader_ids if reader_ids is not None else default_readers()

    produced, skipped = _run_write_cells(spec, canonical, cells, reencode=reencode)
    read_results = _run_readers(produced, canonical, reader_ids)
    produced = _backfill_self_roundtrip(produced, read_results)
    return SlugCompliance(
        slug=slug,
        canonical=canonical,
        write_results=produced,
        skipped_cells=skipped,
        read_results=read_results,
        stale_opt_outs=_stale_opt_outs(slug, produced),
    )


def _stale_opt_outs(slug: str, produced: list[ExportResult]) -> list[dict]:
    """Formats the selected catalog records as unavailable for `slug` that a
    write-cell of this run round-tripped, each announced on stderr.

    The catalog's measurement is a fact about the toolchain that took it; when a
    writer now round-trips the format, the dataset should be re-exported and the
    catalog regenerated, and until then the catalog under-reports what it can
    serve. Informational, like `variant_faithful`: never a gate.
    """
    from raincloud._formats import EXPORTED_FORMATS, describe_unavailable
    from raincloud.catalogs import current

    context = current()
    entry = context.snapshot.get("slugs", {}).get(slug, {}) if context is not None else {}
    stale = []
    for fmt in EXPORTED_FORMATS:
        recorded = entry.get(f"{fmt}_unavailable")
        if not isinstance(recorded, dict):
            continue
        cells = sorted(r.format_id for r in produced
                       if r.format_id.partition("@")[0] == fmt and r.compliance.roundtrip is True)
        for cell in cells:
            print(f"[stale opt-out] {slug}/{fmt}: {cell} now round-trips "
                  f"(catalog recorded: {describe_unavailable(recorded)})", file=sys.stderr)
        if cells:
            stale.append({"format": fmt, "cells": cells, "catalog_recorded": recorded})
    return stale


def _backfill_self_roundtrip(
    produced: list[ExportResult], read_results: list[ReadResult]
) -> list[ExportResult]:
    """Fill a sidecar's unmeasured write `roundtrip` from the cell's own self-read verdict.

    Every writer reads back what it wrote. An in-process one always measures
    it (`exporters.read_back`, the same read this module's pre-existing path
    takes), but a sidecar may report `roundtrip: null`: its comparator could
    not decide, or it ran out of memory verifying. The read matrix already reads
    each artifact back with the reader of the same implementation — the
    diagonal `(artifact_cell == reader_id)` cell — so that verdict fills it.

    Only `None` is backfilled: a writer that measured its own verdict (true or
    false, or a failed write) is authoritative and never overwritten. A cell
    with no self-read cell (its reader wasn't requested, or its toolchain is
    absent) stays `None` — honestly unmeasured, and excluded from the oracle's
    write-cell comparison rather than counted as a pass.
    """
    self_verdict = {
        rr.artifact_cell: rr.verdict
        for rr in read_results
        if rr.artifact_cell == rr.reader_id
    }
    out: list[ExportResult] = []
    for r in produced:
        verdict = self_verdict.get(r.format_id)
        if r.compliance.roundtrip is not None or verdict is None:
            out.append(r)
            continue
        if verdict.status in ("pass", "fail"):
            measured = verdict.status == "pass"
            note = r.compliance.note
            suffix = f"self-read {verdict.status}"
            if verdict.note:
                suffix += f" ({verdict.note})"
            out.append(
                replace(
                    r,
                    compliance=Compliance(
                        roundtrip=measured,
                        variant_faithful=r.compliance.variant_faithful,
                        note="; ".join(n for n in (note, suffix) if n),
                    ),
                )
            )
        else:
            # skip / na / spec_ambiguous: the self-read produced no verdict about
            # faithfulness, so the write stays unmeasured.
            out.append(r)
    return out


# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------


def format_matrix(sc: SlugCompliance, reader_ids: list[str]) -> str:
    """Render the per-slug read-conformance matrix as a text block.

    Rows = artifact cells (produced write-cells); cols = readers; cell = the
    verdict glyph. Absent (never-run) pairs render blank.
    """
    lines: list[str] = []
    lines.append(f"compliance matrix: {sc.slug}")
    lines.append(f"  canonical: {display_path(sc.canonical)}")

    rows = sc.artifact_cells()
    verdict_by = {
        (rr.artifact_cell, rr.reader_id): rr.verdict for rr in sc.read_results
    }
    row_hdr_w = max([len("artifact \\ reader")] + [len(r) for r in rows] + [1])
    col_w = {rid: max(len(rid), max(len(g) for g in GLYPH.values())) for rid in reader_ids}

    header = "  " + "artifact \\ reader".ljust(row_hdr_w)
    for rid in reader_ids:
        header += "  " + rid.ljust(col_w[rid])
    lines.append(header)

    for cell in rows:
        line = "  " + cell.ljust(row_hdr_w)
        for rid in reader_ids:
            v = verdict_by.get((cell, rid))
            glyph = GLYPH.get(v.status, "?") if v is not None else ""
            line += "  " + glyph.ljust(col_w[rid])
        lines.append(line)

    if not rows:
        lines.append("  (no artifacts produced — every requested write-cell skipped)")

    # Skipped write-cells (sidecars absent / unregistered).
    for cell, reason in sc.skipped_cells:
        lines.append(f"  write-cell {cell} skipped: {reason}")

    # Write-cell compliance (the write-side Compliance, distinct from reads).
    for r in sc.write_results:
        c = r.compliance
        state = {True: "roundtrip", False: "NO-roundtrip"}.get(
            c.roundtrip, "roundtrip-UNMEASURED"
        )
        vf = "variant-faithful" if c.variant_faithful else "variant-lossy"
        extra = f" — {c.note}" if c.note else ""
        lines.append(f"  write {r.format_id}: {state}, {vf}{extra}")

    # Notable read verdicts (fail / spec_ambiguous) surfaced with their notes.
    for rr in sc.read_results:
        if rr.verdict.status in ("fail", "spec_ambiguous"):
            note = rr.verdict.note or rr.verdict.status
            lines.append(
                f"  read {rr.artifact_cell} <- {rr.reader_id}: "
                f"{rr.verdict.status.upper()} — {note}"
            )

    counts = sc.counts()
    summary = "  ".join(f"{s}={counts[s]}" for s in _COUNT_ORDER)
    lines.append(f"  reads: {summary}")
    return "\n".join(lines)


def _print_report(report: ComplianceReport, reader_ids: list[str]) -> None:
    for sc in report.slugs:
        print()
        print(format_matrix(sc, reader_ids))
    for slug in report.missing_canonical:
        print(
            f"\n[compliance] {slug}: no canonical Arrow at "
            f"{display_path(prepared_arrow(slug))} — build it first",
            file=sys.stderr,
        )
    total = report.counts()
    stale = [(sc.slug, s) for sc in report.slugs for s in sc.stale_opt_outs]
    if stale:
        print()
        for slug, s in stale:
            print(f"[stale opt-out] {slug}/{s['format']}: {', '.join(s['cells'])} round-trip now; "
                  f"re-export it (`python -m raincloud.pipeline.export {slug} --format {s['format']}`) "
                  f"and regenerate the catalog")
    print()
    print("=" * 60)
    print(
        "TOTAL reads: "
        + "  ".join(f"{s}={total[s]}" for s in _COUNT_ORDER)
        + f"   (slugs measured: {len(report.slugs)}"
        + (f", missing canonical: {len(report.missing_canonical)}" if report.missing_canonical else "")
        + ")"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_csv(value: str | None) -> list[str] | None:
    if value is None:
        return None
    return [c.strip() for c in value.split(",") if c.strip()]


def _main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="raincloud compliance",
        allow_abbrev=False,
        description="Measure the read-conformance matrix for built slugs "
        "(maintainer step; never gates the default build).",
    )
    ap.add_argument("slugs", nargs="*", help="specific slugs to measure")
    ap.add_argument("--all", action="store_true",
                    help="measure every dataset except hydrated ones, which are measured by name")
    ap.add_argument(
        "--cells",
        help="comma-separated WRITE-cell ids to run "
        "(default: every registered cell, incl. sidecars)",
    )
    ap.add_argument(
        "--readers",
        help="comma-separated reader ids to run "
        "(default: every registered reader)",
    )
    ap.add_argument(
        "--reencode",
        action="store_true",
        help="force every write-cell to re-encode even when its artifact is "
        "already on disk (default: read-if-present — the idempotent skip)",
    )
    ap.add_argument(
        "--write-ledger",
        nargs="?",
        const="",
        default=None,
        metavar="PATH",
        help="write the verbose compliance ledger JSON; bare flag uses the "
        "selected catalog observation directory, or pass an explicit PATH",
    )
    ap.add_argument(
        "--check-oracle",
        metavar="PATH",
        help="diff the measured matrix against an immutable-oracle "
        "compliance.json + gate: a cell may be ADDED but never removed/mutated",
    )
    ap.add_argument(
        "--skip-slug",
        action="append",
        default=[],
        metavar="SLUG[=REASON]",
        help="record SLUG as a deliberate SKIP (not measured) rather than "
        "omitting it — for known-brutal builds that OOM/crash the harness "
        "(e.g. mmmu, code-contests). Repeatable; optional '=REASON'.",
    )
    args = ap.parse_args(argv)
    try:
        check_env_knobs()  # a malformed knob fails before a multi-hour campaign
    except ValueError as exc:
        print(f"{ap.prog}: {exc}", file=sys.stderr)
        return 2

    m = load_manifest()
    try:
        selected = select_specs(m, args.slugs, all_=args.all, verb="measure")
    except SelectionError as exc:
        print(f"{ap.prog}: {exc}", file=sys.stderr)
        return 2

    # An explicitly requested cell/reader that isn't registered is an operator
    # error, not something to route around. The per-slug paths below skip an
    # unknown id with a note — which meant `--readers parquet@jav` (a typo)
    # measured nothing at all and still exited 0: a green run covering zero
    # cells. Defaults come from the registry, so only explicit ids need this.
    known_cells = set(default_cells())
    known_readers = set(default_readers())
    cells = _parse_csv(args.cells) or default_cells()
    reader_ids = _parse_csv(args.readers) or default_readers()
    for label, requested, known in (
        ("--cells", _parse_csv(args.cells), known_cells),
        ("--readers", _parse_csv(args.readers), known_readers),
    ):
        if requested == []:
            # `--cells ''` must not quietly mean "every cell".
            print(f"[compliance] {label}: empty list; name at least one id", file=sys.stderr)
            return 2
        unknown = [r for r in (requested or []) if r not in known]
        if unknown:
            print(
                f"[compliance] {label}: no such {'cell' if label == '--cells' else 'reader'}"
                f" registered: {', '.join(sorted(unknown))}\n"
                f"  registered: {', '.join(sorted(known))}",
                file=sys.stderr,
            )
            return 2

    # Resolve the ledger target and load the oracle up front, before any
    # measurement or write:
    #
    # 1. `--check-oracle` must read the ledger as it was before this run. When
    #    `--write-ledger` names the same file (both default to
    #    `docs/v{n}/compliance.json`), loading it after the write would compare
    #    the run against itself, and the additive-only gate could never see a
    #    regression.
    # 2. A malformed oracle fails before a multi-hour campaign, not after.
    ledger_path: Path | None = None
    if args.write_ledger is not None:
        ledger_path = (
            default_compliance_json()
            if args.write_ledger == ""
            else Path(args.write_ledger)
        )

    oracle = None
    if args.check_oracle:
        oracle_path = Path(args.check_oracle)
        if ledger_path is not None and _same_file(oracle_path, ledger_path):
            print(
                "[compliance] --check-oracle and --write-ledger name the same "
                f"file ({display_path(oracle_path)}); the gate would compare the "
                "run against itself. Diff against the committed oracle, then "
                "write the new ledger to an explicit PATH.",
                file=sys.stderr,
            )
            return 2
        try:
            oracle = ledger.load_oracle(oracle_path)
        except ledger.MalformedOracle as exc:
            print(f"[compliance] MALFORMED ORACLE: {exc}", file=sys.stderr)
            return 2

    # Deliberate skips: SLUG or SLUG=REASON. Excluded from measurement below, so
    # a `--skip-slug mmmu` alongside an explicit slug list (or --all) records mmmu
    # as skipped without attempting the OOM-prone build.
    skip_reasons: dict[str, str] = {}
    for raw in args.skip_slug:
        slug, _, reason = raw.partition("=")
        skip_reasons[slug.strip()] = reason.strip() or "deliberate skip (--skip-slug)"
    # A typo must not write a phantom "deliberate skip" into the ledger.
    try:
        if skip_reasons:
            select_specs(m, list(skip_reasons), quiet=True)
    except SelectionError as exc:
        print(f"{ap.prog}: --skip-slug: {exc}", file=sys.stderr)
        return 2
    chosen = {s["slug"] for s in selected}
    for slug in [s for s in skip_reasons if s not in chosen]:
        print(f"[compliance] --skip-slug {slug}: not selected; ignored", file=sys.stderr)
        del skip_reasons[slug]

    # Only an explicit --cells/--readers subset narrows the oracle gate. A full
    # run records None ("every registered id"), so an oracle cell of a writer or
    # reader that is no longer registered reads as removed, not out of scope.
    gate_scope = {"requested_cells": _parse_csv(args.cells), "requested_readers": _parse_csv(args.readers)}

    report = ComplianceReport()
    for spec in selected:
        if spec["slug"] in skip_reasons:
            continue  # deliberately not measured — recorded as a skip below
        sc = run_compliance(
            spec, cells=cells, reader_ids=reader_ids, reencode=args.reencode
        )
        if sc is None:
            report.missing_canonical.append(spec["slug"])
        else:
            report.slugs.append(sc)
    report.skipped_slugs = sorted(skip_reasons.items())

    _print_report(report, reader_ids)

    # Persist the verbose ledger when asked. The caller stamps the clock (the
    # serializer core takes a passed-in string). Bare `--write-ledger` targets
    # the selected catalog observation directory; an explicit PATH overrides it.
    if ledger_path is not None:
        generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Record what was asked for alongside what was measured, so a partial
        # ledger can be audited against its invocation instead of being taken on
        # faith (a 128-of-250 file is indistinguishable from a silent shrink
        # without it). `missing_canonical` is included because "selected but
        # never built" is exactly the gap the scope block has to make visible.
        scope = {
            "all": bool(args.all),
            "selected_slugs": sorted(s["slug"] for s in selected),
            **gate_scope,
            "deliberately_skipped_slugs": sorted(skip_reasons),
            "missing_canonical_slugs": sorted(report.missing_canonical),
        }
        ledger.write_compliance_json(
            report,
            ledger_path,
            generated_at=generated_at,
            versions=_runtime_versions(),
            scope=scope,
        )
        print(f"[compliance] wrote ledger: {display_path(ledger_path)}", file=sys.stderr)

    # Run the additive-only gate against the oracle loaded before the write
    # above (see the resolution block near the top of `main`). With no
    # `--check-oracle`, `oracle=None` still gates on read fail + write-cell fail
    # (the default exit) but has no removed/mutated cells to check (first run
    # seeds the oracle).
    gate_ok, reasons = ledger.oracle_gate(report, oracle, scope=gate_scope)
    for reason in reasons:
        print(f"[compliance] GATE: {reason}", file=sys.stderr)

    # Missing canonical (build-first misuse) is a non-zero exit independent of
    # the read/write/oracle gate.
    if report.missing_canonical or not gate_ok:
        return 1
    return 0


def _same_file(a: Path, b: Path) -> bool:
    """True when two paths name the same file on disk.

    `Path.samefile` needs both to exist — but the `--write-ledger` target is
    usually about to be created, so that alone would miss the case this guards.
    Fall back to comparing fully resolved paths, which also collapses `..`
    segments and symlinked parents.
    """
    try:
        if a.exists() and b.exists():
            return a.samefile(b)
    except OSError:  # pragma: no cover — stat failure, fall through to resolve()
        pass
    return a.resolve() == b.resolve()


def _runtime_versions() -> dict[str, str]:
    """Best-effort tool versions stamped into the verbose ledger envelope."""
    import platform

    versions = {"python": platform.python_version(), "pyarrow": pa.__version__}
    try:
        import vortex

        versions["vortex"] = getattr(vortex, "__version__", "unknown")
    except Exception:  # noqa: BLE001 — versions are informational, never fatal
        pass
    return versions


@maintenance(resources=True)
def main(argv: list[str] | None = None) -> int:
    return _main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
