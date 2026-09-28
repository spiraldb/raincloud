# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Exporter seam — the interface each output-format writer implements.

The canonical producer (`canonical.write_canonical`) emits one
`<slug>.arrow.zstd` per slug; every exporter reads that canonical and writes
one format. A dataset has ONE file per format, `<fmt>/<slug>.<ext>`, whichever
writer made it: the writer (`parquet@py`, `parquet@rs`) is provenance, kept in
this install's build record and, once a maintainer regenerates it, the catalog.
`run_exporters` picks the writer from the export priority.

`Compliance` is the tri-state verdict recorded per export: whether the artifact
round-trips back to Arrow, whether any VARIANT columns survived faithfully, and
a free-form note. This module is the seam only — no real exporters live here.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable


def slug_from_canonical(canonical: Path) -> str:
    """Inverse of `output_format_dir`: derive the slug from a canonical Arrow
    path `outputs/v{n}/<slug>/arrow/<slug>.arrow.zstd` -> `<slug>`.

    Exporters run per-canonical, so a canonical's slug (the transform's output
    slug, from `write_canonical`) is the authority for artifact placement — not
    `spec["slug"]`, which can differ under a multi-output transform.
    """
    return canonical.parent.parent.name


@dataclass(frozen=True)
class Compliance:
    """Export verdict recorded in the per-format ledger.

    `roundtrip` — did the exported artifact re-read back to the canonical Arrow?
    THREE states, and the distinction is load-bearing:

    - ``True``  — MEASURED faithful: something actually read the artifact back
                  and compared it. Only a real read may set this.
    - ``False`` — MEASURED mismatch (or a failed write).
    - ``None``  — WRITTEN, NOT MEASURED. Only a sidecar reports it: its
                  self-verify could not judge the file (a comparator gap, or
                  a resource limit). The build promotes that file and records
                  it unverified (`verified: false`, `verify_note`);
                  `compliance` backfills the real verdict from the read matrix
                  (see `compliance._backfill_self_roundtrip`). In-process
                  writers always read their output back, so they never
                  report it.

    `variant_faithful` — VARIANT columns (if any) survived byte-for-byte.
    `note` — free-form context (e.g. "skipped: binary absent").
    """

    roundtrip: bool | None
    variant_faithful: bool
    note: str = ""

    @property
    def roundtrip_measured(self) -> bool:
        """True when `roundtrip` is a real measurement rather than unmeasured."""
        return self.roundtrip is not None


@dataclass(frozen=True)
class ExportResult:
    """Outcome of a single exporter run over one canonical artifact.

    `format_id` here holds the writer CELL (`parquet@py`), not the bare format.
    """

    format_id: str
    out_path: Path
    nbytes: int
    sha256: str
    compliance: Compliance  # required — every export states a verdict (no silent default)


@runtime_checkable
class Exporter(Protocol):
    """A pluggable output-format writer over the canonical Arrow artifact.

    Implementations (in-process or sidecar-CLI) are registered in this
    package's `__init__` registry keyed by the qualified `cell_id`. `format_id`
    names the bare format (`parquet`); `cell_id` qualifies it with the
    implementation (`parquet@py`, `parquet@rs`) so several encoders of one
    format coexist.

    Every export runs under the export time limit (`bounded.run_bounded`): an
    in-process writer in a child process, which it needs no code for. A writer
    that is a subprocess already sets `bounds_itself = True` and applies
    `bounded.export_timeout()` itself, and may define `toolchain()` -> the
    versions that identify it, recorded when it cannot produce a format
    (`export.writer_toolchain` names an in-process writer's libraries).
    """

    format_id: str
    cell_id: str

    def unavailable(self) -> str | None:
        """Why this writer cannot run on this machine, or None when it can.

        A sidecar needs its binary; an in-process writer needs its library.
        `plan` skips an unavailable writer and names the reason when no writer
        for a format is left.
        """
        ...

    def out_path(self, slug: str) -> Path:
        """The dataset's store file for this format: `<fmt>/<slug>.<ext>`, the
        same for every writer of the format.

        The store destination only. Compliance never WRITES here: it measures
        writers side by side in scratch (`compliance.compliance_path`), though it
        may read a store file this writer made. Pure: computes a path, never
        touches the filesystem.
        """
        ...

    def export(self, spec: dict, canonical: Path, dest: Path | None = None) -> ExportResult | None:
        """Write the format artifact from `canonical` and report the outcome.

        `dest` defaults to `out_path(slug)`, the dataset's one file for this
        format, whichever writer makes it. Compliance passes its own, so every
        writer's output can be compared without touching the store.

        Returns `None` when the export was *skipped* (e.g. a sidecar cell whose
        reference-writer binary is absent) — a run/skip distinction the caller
        (`run_exporters`) filters out. In-process cells always return a result.
        """
        ...


# ---------------------------------------------------------------------------
# Read-conformance verdict vocab
#
# The WRITE side records `Compliance` (roundtrip / variant_faithful) per export.
# The READ side — does an implementation correctly *read* an artifact back to
# the canonical Arrow? — is a distinct dimension with its own richer vocab,
# modelled on `iceberg-vortex-cross-language-testing` (pass/fail/na/skip). We
# add one status raincloud found it needs in the wild: `spec_ambiguous`.
# This is additive; `Compliance` is NOT retrofitted onto this vocab.
# ---------------------------------------------------------------------------

VerdictStatus = Literal["pass", "fail", "na", "skip", "spec_ambiguous"]

#: The frozen read-conformance status vocabulary.
VERDICT_STATUSES: frozenset[str] = frozenset(
    {"pass", "fail", "na", "skip", "spec_ambiguous"}
)


@dataclass(frozen=True)
class Verdict:
    """One read-conformance result for a `(artifact-cell × reader)` pair.

    `status` is one of:

    - ``pass``           — the reader read the artifact *and* it round-tripped
                           back to the canonical Arrow (data-faithful).
    - ``fail``           — the reader read it but the data mismatched, OR the
                           read itself errored (a read error is a measured
                           ``fail``, never an escaping exception).
    - ``na``             — the reader cannot apply to this artifact's format
                           (e.g. a vortex reader over a parquet artifact).
    - ``skip``           — the read did not yield a verdict about the data.
                           TWO distinct flavours, separated by
                           `toolchain_absent`: the reader was never invoked
                           because its binary/library is absent
                           (`toolchain_absent=True`), or the reader RAN and
                           returned `skip` — an unsupported type, a comparator
                           gap (`toolchain_absent=False`). NOT a failure either
                           way, but only the first is benign across profiles.
    - ``spec_ambiguous`` — a read *disagreement* traced to an UNDERDEFINED
                           format spec (raincloud's first-class result — the
                           class of `dfa1/vortex-java#205`). Distinct from a
                           clean pass/fail; `note` should point at the ambiguity
                           so the matrix is actionable for spec work. A human /
                           reader marks this; raincloud records it.

    Kept small + additive. `note` is free-form context; `detail` optionally
    carries a longer pointer (error text, an issue URL, a spec section).

    `toolchain_absent` marks the one benign flavour of ``skip``: the reader was
    never invoked, because its binary/library isn't on this machine. It is not
    interchangeable with a ``skip`` a reader returned after running -- that is a
    *measured* verdict about the artifact and is compared like any other. The
    oracle gate depends on the distinction: if every ``skip`` were excused as an
    absent toolchain, a passing cell could regress to "unsupported" while the
    additive-only gate stayed green.
    """

    status: VerdictStatus
    note: str = ""
    detail: str = ""
    toolchain_absent: bool = False

    def __post_init__(self) -> None:
        if self.status not in VERDICT_STATUSES:
            raise ValueError(
                f"invalid verdict status {self.status!r} "
                f"(expected one of {sorted(VERDICT_STATUSES)})"
            )
        if self.toolchain_absent and self.status != "skip":
            raise ValueError(
                "toolchain_absent is only meaningful for a 'skip' verdict, got "
                f"status={self.status!r}"
            )


@dataclass(frozen=True)
class ReadResult:
    """Binds a produced artifact cell + a reader to its `Verdict`.

    `artifact_cell` is the WRITE cell that produced the artifact under test
    (e.g. ``parquet@py``); `reader_id` is the reader that read it (e.g.
    ``vortex@jni``). Together they name one cell of the read-conformance
    matrix; `ledger` persists them.
    """

    artifact_cell: str
    reader_id: str
    verdict: Verdict
