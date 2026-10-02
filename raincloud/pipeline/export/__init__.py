# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Exporter registry — mirrors the handler registry idiom (`handlers/__init__.py`).

Exporters register themselves by their qualified `cell_id` (`<format>@<impl>`,
e.g. `parquet@py`), so several implementations of one format — `parquet@py`,
`parquet@rs`, `parquet@java` — are registered side by side. A build writes each
format once, to `<fmt>/`, with the writer `run_exporters` picks from the
priority; the build record says which. Importing this package registers the
pure-Python cells (parquet@py / vortex@py) plus the sidecar cells (parquet@rs /
parquet@java / parquet@hardwood / vortex@rs / vortex@jni), which run only where
their binary is installed.
"""
from __future__ import annotations

import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from raincloud._cache import Publication
from raincloud._registry import exporter_cells
from raincloud.exceptions import BuildToolingMissing

from .base import (
    VERDICT_STATUSES,
    Compliance,
    Exporter,
    ExportResult,
    ReadResult,
    Verdict,
    slug_from_canonical,
)
from .bounded import ExportFailed, export_timeout, run_bounded

_EXPORTERS: dict[str, Exporter] = {}


def register(exporter: Exporter) -> None:
    """Register `exporter` under its `cell_id`; raise on a duplicate or undeclared id.

    Cells are declared in `raincloud._registry`, which is what the loader reads
    to decide whether a catalog's recipes can be built here. Registering a cell
    that is not declared there would be invisible to that check, so it fails now
    rather than at catalog-load time on someone else's machine.
    """
    cell = exporter.cell_id
    if cell in _EXPORTERS:
        raise ValueError(f"exporter already registered for cell_id {cell!r}")
    if cell not in exporter_cells():
        raise ValueError(
            f"exporter cell_id {cell!r} is not declared in raincloud._registry; "
            f"add it to PY_EXPORTERS or SIDECAR_EXPORTERS"
        )
    _EXPORTERS[cell] = exporter


def get_exporter(cell_id: str) -> Exporter:
    """Return the exporter for `cell_id`, or raise KeyError if none is registered."""
    try:
        return _EXPORTERS[cell_id]
    except KeyError:
        raise KeyError(f"no exporter registered for cell_id {cell_id!r}") from None


def cell_available(cell_id: str) -> bool:
    """Can this cell actually run here?

    Registered is not the same as runnable: a sidecar cell is registered always
    and runnable only when its reference-writer binary is on PATH (or named by
    its env override); an in-process cell is runnable when its library imports.
    Each exporter answers through its `unavailable()`.
    """
    exporter = _EXPORTERS.get(cell_id)
    return exporter is not None and exporter.unavailable() is None


def all_exporters() -> list[Exporter]:
    """Every registered exporter, in registration order."""
    return list(_EXPORTERS.values())


def plan(spec: dict, formats: list[str] | None = None) -> list[str]:
    """The writer cell for each format to write: `formats`, else the install's
    (`build_formats`).

    A bare format takes the first INSTALLED writer in its priority. The
    priority is the most specific one that names the format -- spec, then
    catalog, then machine, then built in -- used on its own: a spec priority of
    `["rs"]` on a machine without rs fails rather than falling back to the
    catalog's order, so a spec lists its fallback writer (`["rs", "py"]`). A
    cell (`parquet@rs`) names its writer outright, for this run only.

    `build` calls this before fetching, so a machine that cannot write a
    format fails in a second, not after the transform.
    """
    return [cell for cell, _named in _plan(spec, formats)]


def _plan(spec: dict, formats: list[str] | None) -> list[tuple[str, bool]]:
    """`plan`, with whether each cell was named outright rather than planned."""
    from raincloud._formats import build_formats, export_priority, resolve_export_cell
    from raincloud.catalogs import current
    from raincloud.config import get_config

    context = current()
    manifest = context.manifest if context else None
    version = int(manifest["schema_version"]) if manifest else 2
    config = get_config()
    cells = []
    for fmt in (formats if formats is not None else build_formats(spec, version, config)):
        if "@" in fmt:
            cells.append((fmt, True))
            continue
        priority = export_priority(spec, manifest, config, fmt=fmt)
        cell = resolve_export_cell(fmt, priority, is_available=cell_available)
        if cell is None:
            missing = [f"{c}: {_EXPORTERS[c].unavailable()}" for c in
                       (f"{fmt}@{w}" for w in priority) if c in _EXPORTERS]
            raise BuildToolingMissing(
                f"{spec['slug']}: no installed writer for {fmt!r} (priority {', '.join(priority)})"
                + (f"; {'; '.join(missing)}" if missing else ""))
        cells.append((cell, False))
    return cells


@dataclass(frozen=True)
class Unavailable:
    """A planned writer's failure to write its format for one dataset.

    What the build record keeps (with the recipe and canonical it was measured
    against, `records.record_unavailable`) and the catalog then shows: the
    writer cell, its error, the toolchain that ran and when.
    """

    cell: str
    error: str
    toolchain: dict[str, str]
    measured_at: str

    @property
    def format(self) -> str:
        return self.cell.partition("@")[0]


@dataclass(frozen=True)
class Skipped:
    """A format not attempted this run: the measurement that applies (this
    install's build record at the recipe, else the catalog's) records the same
    writer cell failing with the same toolchain on the same canonical, so a new
    attempt would only repeat it. `--retry-errors` attempts it anyway."""

    cell: str
    measurement: dict

    @property
    def format(self) -> str:
        return self.cell.partition("@")[0]


def _versions(toolchain: dict) -> str:
    return ", ".join(f"{name} {version}" for name, version in toolchain.items())


def retry_reason(measurement: dict, cell: str, toolchain: dict[str, str],
                 canonical_sha256: str | None) -> str | None:
    """Why writing with `cell` now would not repeat `measurement`, or None when it would.

    A repeat is the same writer cell, the same toolchain (every version the
    measurement records, compared exactly: `writer_toolchain`) and, when the
    measurement names the canonical it read, the same canonical.
    """
    recorded = measurement.get("cell")
    if recorded != cell:
        return f"recorded for {recorded}, now {cell}"
    before = measurement.get("toolchain") or {}
    if before != toolchain:
        return "; ".join(f"recorded with {name} {before.get(name, 'none')}, now {toolchain.get(name, 'none')}"
                         for name in sorted({*before, *toolchain}) if before.get(name) != toolchain.get(name))
    measured_from = measurement.get("canonical_sha256")
    if measured_from and measured_from != canonical_sha256:
        return (f"recorded against canonical {measured_from[:12]}, now "
                f"{canonical_sha256[:12] if canonical_sha256 else 'one with no recorded checksum'}")
    return None


# Recorded error text is bounded: a Rust panic can carry a backtrace.
_ERROR_CHARS = 500


def _error_text(text: str) -> str:
    text = " ".join(text.split())
    return text if len(text) <= _ERROR_CHARS else text[:_ERROR_CHARS - 1] + "…"


# The Python distributions an in-process writer runs.
_DISTRIBUTIONS = {"parquet@py": ("pyarrow",), "vortex@py": ("vortex-data", "pyarrow"), "orc@py": ("pyarrow",)}


def writer_toolchain(exporter: Exporter) -> dict[str, str]:
    """The versions that decide what `exporter` can write: its libraries for an
    in-process writer, the binary for a sidecar (`SidecarExporter.toolchain`)."""
    import platform
    from importlib import metadata

    if hasattr(exporter, "toolchain"):
        return exporter.toolchain()
    versions = {"python": platform.python_version()}
    for distribution in _DISTRIBUTIONS.get(exporter.cell_id, ()):
        try:
            versions[distribution] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            versions[distribution] = "not installed"
    return versions


def run_exporters(spec: dict, canonical: Path, formats: list[str] | None = None, *,
                  on_accept: Callable[[ExportResult], None] | None = None,
                  on_unavailable: Callable[[Unavailable], None] | None = None,
                  measured: Callable[[str], tuple[dict, str | None] | None] | None = None,
                  retry_errors: bool = False,
                  on_skip: Callable[[Skipped], None] | None = None) -> list[ExportResult]:
    """Write each of `spec`'s formats from its canonical Arrow artifact.

    Every format is one file, `<fmt>/<slug>.<ext>`, whichever writer makes it
    (see `plan`). `formats` replaces the install's formats (`build_formats`)
    for this run; an entry may be a bare format or a writer cell (`parquet@rs`).

    Each file is published under a `Publication`, and each writer runs under
    the export time limit (`bounded.run_bounded`). Every writer reads back what
    it wrote: an in-process one always (`exporters.read_back`), a sidecar in
    its own process, which may report the read-back unmeasured
    (`roundtrip=None`) -- that file is promoted, with an `[unverified]` line,
    and `on_accept` sees the None, so the build record says so. When a writer
    raises, dies, runs out of time or reports a measured failure
    (`roundtrip=False`: its file did not read back to the canonical), the
    previous file comes back, and then:

    - a PLANNED writer (the one a bare format resolved to) has measured that
      this dataset cannot have the format here: `[export failed]` is printed,
      `on_unavailable` is called with the measurement (`records.recorders`
      records it), and the run continues with the next format. The dataset is
      built with the formats that worked.
    - a writer NAMED outright (`parquet@rs`) is this run's request, and its
      failure fails the run (RuntimeError); nothing is recorded for it.

    `on_accept` is called with each result as soon as its file is committed, so
    a later writer's failure cannot leave an accepted file unrecorded.

    A failure already measured is not repeated. `measured(fmt)` gives the
    measurement that applies to the format and the checksum of the canonical
    being exported (`records.recorded_failure`: this install's build record at
    the recipe, else the catalog's). When it records the writer that would run
    now, with the same toolchain, reading the same canonical (`retry_reason`),
    the format is skipped: `[skip]` is printed, `on_skip` is called, and nothing
    new is recorded. `retry_errors` (or the `retry_errors` setting) attempts it
    anyway; any other difference -- another writer, an upgraded library, a new
    canonical -- is attempted with a `[retry]` line saying what changed. This
    applies to a named writer too: naming the writer that failed does not
    re-run it without `retry_errors`. Compliance does not come through here: it
    measures every write cell.

    A named cell with no registered exporter fails. A sidecar whose binary is
    absent (`export` -> None) is skipped with a note: it can only be absent when
    named outright, since the priority skips writers that are not installed.
    """
    from datetime import datetime, timezone

    from raincloud.config import get_config

    from ..spec import scrub_published_text

    retry_errors = retry_errors or get_config().retry_errors
    results: list[ExportResult] = []
    slug = slug_from_canonical(canonical)
    for cell, named in _plan(spec, formats):
        try:
            exporter = get_exporter(cell)
        except KeyError:
            raise RuntimeError(
                f"{spec['slug']}: {cell!r} has no registered exporter (registered: "
                f"{', '.join(sorted(_EXPORTERS))})"
            ) from None
        fmt = cell.partition("@")[0]
        known = measured(fmt) if measured is not None else None
        if known is not None:
            measurement, canonical_sha = known
            # Compared as recorded: `records.record_unavailable` scrubs the values.
            toolchain = {name: scrub_published_text(value) for name, value in writer_toolchain(exporter).items()}
            reason = retry_reason(measurement, cell, toolchain, canonical_sha)
            if reason is None and not retry_errors:
                print(f"  [skip] {slug}/{fmt}: {cell} ({_versions(measurement.get('toolchain') or {})}) failed "
                      f"at this recipe on {measurement.get('measured_at') or 'an unrecorded date'}: "
                      f"{measurement.get('error') or 'no error recorded'}; pass --retry-errors to try again",
                      file=sys.stderr)
                if on_skip is not None:
                    on_skip(Skipped(cell=cell, measurement=measurement))
                continue
            print(f"  [retry] {slug}/{fmt}: {measurement.get('cell')} failed at this recipe on "
                  f"{measurement.get('measured_at') or 'an unrecorded date'}; "
                  f"{reason or 'trying again (--retry-errors)'}", file=sys.stderr)
        failure = None
        with Publication(exporter.out_path(slug)) as publication:
            try:
                result = run_bounded(exporter, spec, canonical, publication.dest)
                if result is None:
                    # A sidecar cell whose reference-writer binary is absent skips.
                    # Distinct from an unknown id (above) and from a writer that RAN
                    # and failed (below).
                    print(
                        f"[export] {spec['slug']}: {cell} skipped — "
                        f"reference-writer binary absent",
                        file=sys.stderr,
                    )
                    continue
                # A MEASURED write failure: the file did not read back to the
                # canonical, or could not be read at all.
                if result.compliance.roundtrip is False:
                    raise ExportFailed(f"{cell}: a measured write failure — "
                                       f"{result.compliance.note or 'roundtrip=False'}")
                if result.out_path != publication.dest:
                    raise RuntimeError(f"{cell}: exporter returned an unexpected artifact path")
                publication.accept()
                if result.compliance.roundtrip is None:
                    # Only a sidecar can leave its read-back unmeasured (its
                    # comparator could not decide, or it ran out of memory
                    # verifying): the file is served, and recorded unverified.
                    print(f"  [unverified] {slug}/{fmt}: {cell} published its file without verifying "
                          f"it reads back: {result.compliance.note or 'no reason given'}", file=sys.stderr)
            except ExportFailed as exc:
                if named:
                    raise RuntimeError(f"{spec['slug']}: {cell} export failed: {exc}") from None
                failure = Unavailable(
                    cell=cell, error=_error_text(str(exc)), toolchain=writer_toolchain(exporter),
                    measured_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        if failure is not None:
            # After the Publication: the previous file, if any, is back in place.
            print(f"  [export failed] {slug}/{failure.format}: {failure.error}", file=sys.stderr)
            if on_unavailable is not None:
                on_unavailable(failure)
            continue
        results.append(result)
        if on_accept is not None:
            on_accept(result)
    return results


__all__ = [
    "Compliance",
    "Exporter",
    "ExportResult",
    "ReadResult",
    "Verdict",
    "VERDICT_STATUSES",
    "slug_from_canonical",
    "register",
    "get_exporter",
    "all_exporters",
    "cell_available",
    "plan",
    "run_exporters",
    "run_bounded",
    "export_timeout",
    "ExportFailed",
    "Unavailable",
    "Skipped",
    "retry_reason",
    "writer_toolchain",
    "Reader",
    "register_reader",
    "get_reader",
    "all_readers",
    "run_reader",
]

# Register the built-in exporters. Imported at the bottom so `register` and the
# registry exist before these modules call back into this package. `exporters`
# registers the pure-Python default cells (parquet@py, vortex@py); `sidecar`
# registers the opt-in reference-writer cells (parquet@rs, parquet@java,
# parquet@hardwood, vortex@rs, vortex@jni).
from . import exporters as _exporters  # noqa: E402,F401
from . import sidecar as _sidecar  # noqa: E402,F401

# Read-conformance machinery — its own registry (keyed by reader_id). Importing
# `readers` registers the in-process readers (parquet@py, vortex@py) + the
# opt-in reference-reader sidecars.
from .readers import (  # noqa: E402
    Reader,
    all_readers,
    get_reader,
    register_reader,
    run_reader,
)
