# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Tests for the compliance ledger — serializers, the additive-only oracle, and
`compliance`'s idempotent writes (present artifacts are read, not re-encoded).

Two flavours of hermeticity:

- The SERIALIZER + ORACLE tests build synthetic `ComplianceReport` /
  `SlugCompliance` dataclasses directly (no filesystem, no clock) — the
  serializer core takes a passed-in `generated_at` string, so the tests pin it.
- The idempotence tests point `RAINCLOUD_HOME` at a tmp dir, build the real
  canonical + parquet@py/vortex@py artifacts, then assert `compliance` READS
  them (does not re-encode) unless `--reencode` forces a fresh write. A spy
  wraps `Exporter.export` to observe whether it was invoked — no monkeypatched
  behaviour beyond call-recording.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa

from raincloud.pipeline import canonical, compliance, ledger
from raincloud.pipeline.compliance import ComplianceReport, SlugCompliance
from raincloud.pipeline.export.base import Compliance, ExportResult, ReadResult, Verdict
from raincloud.pipeline.export.exporters import ParquetExporter, VortexExporter
from raincloud.pipeline.spec import REPO_ROOT, display_path

# --------------------------------------------------------------------------- #
# Synthetic-report builders (no filesystem, no clock)
# --------------------------------------------------------------------------- #


def _write(cell, roundtrip=True, variant_faithful=True, note=""):
    return ExportResult(
        format_id=cell,
        out_path=Path(f"/x/{cell}.out"),
        nbytes=10,
        sha256="a" * 64,
        compliance=Compliance(roundtrip=roundtrip, variant_faithful=variant_faithful, note=note),
    )


def _read(cell, reader, status, note="", detail=""):
    return ReadResult(cell, reader, Verdict(status, note=note, detail=detail))


def _sc(slug="s1", *, canonical_path=None, writes=None, reads=None, skipped=None):
    return SlugCompliance(
        slug=slug,
        canonical=canonical_path or Path(f"/x/outputs/v2/{slug}/arrow/{slug}.arrow.zstd"),
        write_results=writes or [],
        skipped_cells=skipped or [],
        read_results=reads or [],
    )


def _report(*slug_compliances, missing=None):
    return ComplianceReport(slugs=list(slug_compliances), missing_canonical=missing or [])


# --------------------------------------------------------------------------- #
# 1. to_compliance_json — the VERBOSE matrix shape
# --------------------------------------------------------------------------- #


def test_to_compliance_json_shape_and_passthrough():
    canon = Path("/x/outputs/v2/s1/arrow/s1.arrow.zstd")
    sc = _sc(
        "s1",
        canonical_path=canon,
        writes=[_write("parquet@py", note="ok"), _write("vortex@py", variant_faithful=False, note="lossy")],
        reads=[
            _read("parquet@py", "parquet@py", "pass", note="round-trips"),
            _read("parquet@py", "vortex@py", "na", note="cross"),
            _read("parquet@py", "parquet@java", "skip", note="absent"),
            _read("vortex@py", "vortex@py", "pass"),
        ],
    )
    report = _report(sc)

    cj = ledger.to_compliance_json(
        report, generated_at="2026-07-08T12:00:00Z", versions={"pyarrow": "1.2.3"}
    )

    # Envelope: generated_at + versions are PASSED IN verbatim (no implicit clock).
    assert cj["generated_at"] == "2026-07-08T12:00:00Z"
    assert cj["versions"] == {"pyarrow": "1.2.3"}
    # Catalog rollup = report.counts() (all five statuses).
    assert cj["rollup"] == {"pass": 2, "fail": 0, "skip": 1, "na": 1, "spec_ambiguous": 0}

    block = cj["slugs"]["s1"]
    assert block["canonical"] == display_path(canon)
    # write: cell + roundtrip + variant_faithful + note, verbatim.
    assert block["write"] == [
        {"cell": "parquet@py", "roundtrip": True, "variant_faithful": True, "note": "ok"},
        {"cell": "vortex@py", "roundtrip": True, "variant_faithful": False, "note": "lossy"},
    ]
    # read: every ReadResult verbatim as a flat per-cell record.
    assert block["read"][0] == {
        "artifact_cell": "parquet@py",
        "reader_id": "parquet@py",
        "status": "pass",
        "note": "round-trips",
        "detail": "",
    }
    assert len(block["read"]) == 4
    assert block["rollup"] == {"pass": 2, "fail": 0, "skip": 1, "na": 1, "spec_ambiguous": 0}


def test_to_compliance_json_versions_none_is_empty_dict():
    cj = ledger.to_compliance_json(_report(_sc()), generated_at="t", versions=None)
    assert cj["versions"] == {}


# --------------------------------------------------------------------------- #
# 2. Additive-only immutable oracle — diff_against_oracle
# --------------------------------------------------------------------------- #


def _cj_from_reads(slug, reads):
    """Hand-author a minimal verbose compliance-json with just `read` cells.

    Each `reads` entry is `(cell, reader, status)`, or
    `(cell, reader, status, toolchain_absent)` to mark a `skip` as the benign
    never-invoked kind — the flag `diff_against_oracle` keys its benign-skip rule
    off. A bare
    3-tuple `skip` is therefore a MEASURED skip (the reader ran and returned it).
    """
    rows = []
    for entry in reads:
        cell, reader, status = entry[:3]
        row = {"artifact_cell": cell, "reader_id": reader, "status": status}
        if len(entry) > 3 and entry[3]:
            row["toolchain_absent"] = True
        rows.append(row)
    return {"slugs": {slug: {"read": rows}}}


def test_diff_against_oracle_flags_removed_and_mutated_passes_added():
    oracle = _cj_from_reads(
        "s1",
        [
            ("parquet@py", "parquet@py", "pass"),          # will mutate
            ("parquet@py", "parquet@java", "pass"),        # will be removed
        ],
    )
    new = _cj_from_reads(
        "s1",
        [
            ("parquet@py", "parquet@py", "fail"),          # mutated pass -> fail
            ("parquet@py", "parquet@hardwood", "pass"),    # added
        ],
    )
    diff = ledger.diff_against_oracle(new, oracle)

    assert diff.added == [("s1", "parquet@py", "parquet@hardwood")]
    assert diff.removed == [("s1", "parquet@py", "parquet@java")]
    assert diff.mutated == [("s1", "parquet@py", "parquet@py", "pass", "fail")]
    assert diff.unchanged == []
    assert diff.has_violations is True
    reasons = diff.violation_reasons()
    assert any("removed cell" in r for r in reasons)
    assert any("mutated cell" in r and "pass -> fail" in r for r in reasons)


def test_diff_against_oracle_additive_only_passes():
    oracle = _cj_from_reads("s1", [("parquet@py", "parquet@py", "pass")])
    new = _cj_from_reads(
        "s1",
        [
            ("parquet@py", "parquet@py", "pass"),        # unchanged
            ("parquet@py", "parquet@hardwood", "skip"),  # added — OK
        ],
    )
    diff = ledger.diff_against_oracle(new, oracle)
    assert diff.added == [("s1", "parquet@py", "parquet@hardwood")]
    assert diff.unchanged == [("s1", "parquet@py", "parquet@py")]
    assert diff.removed == [] and diff.mutated == []
    assert diff.has_violations is False


def test_diff_against_oracle_ignores_unmeasured_slugs():
    """An oracle cell for a slug NOT measured in `new` (a subset run) is NOT
    a `removed` violation — only a within-a-measured-slug deletion is flagged."""
    def _read(cell, reader, status):
        return {"artifact_cell": cell, "reader_id": reader, "status": status,
                "note": "", "detail": ""}

    new = {"slugs": {"s1": {"read": [_read("parquet@py", "parquet@py", "pass")]}}}
    # Oracle knows s1 (same) + s2 (this run didn't measure s2).
    oracle = {"slugs": {
        "s1": {"read": [_read("parquet@py", "parquet@py", "pass")]},
        "s2": {"read": [_read("parquet@py", "parquet@py", "pass")]},
    }}
    diff = ledger.diff_against_oracle(new, oracle)
    assert diff.removed == []             # s2 unmeasured -> NOT a removal
    assert diff.has_violations is False

    # But a cell deleted WITHIN the measured slug s1 IS a removal.
    oracle2 = {"slugs": {"s1": {"read": [
        _read("parquet@py", "parquet@py", "pass"),
        _read("parquet@py", "vortex@jni", "pass"),
    ]}}}
    diff2 = ledger.diff_against_oracle(new, oracle2)
    assert ("s1", "parquet@py", "vortex@jni") in diff2.removed
    assert diff2.has_violations is True


def test_diff_against_oracle_skip_in_new_is_not_a_mutation():
    """A cell the oracle measured (real verdict) but that `new` recorded as
    an ABSENT-TOOLCHAIN `skip` — the reader was never invoked on THIS profile —
    is `skipped`, NOT `mutated`. A genuine pass -> fail still is.

    Keeps one committed oracle valid across machines with different sidecar
    toolchains: `--check-oracle` on a Rust-less box mustn't red-gate the cells
    the oracle seeded on a Rust-equipped box."""
    oracle = _cj_from_reads(
        "s1",
        [
            ("vortex@rs", "vortex@rs", "pass"),     # oracle measured a real verdict
            ("parquet@py", "parquet@py", "pass"),   # will genuinely regress
        ],
    )
    new = _cj_from_reads(
        "s1",
        [
            # binary absent this run -> never invoked (the benign flavour)
            ("vortex@rs", "vortex@rs", "skip", True),
            ("parquet@py", "parquet@py", "fail"),    # genuine pass -> fail
        ],
    )
    diff = ledger.diff_against_oracle(new, oracle)

    # skip-in-new: benign, classified `skipped`, never `mutated`.
    assert ("s1", "vortex@rs", "vortex@rs") in diff.skipped
    assert all(m[:3] != ("s1", "vortex@rs", "vortex@rs") for m in diff.mutated)
    # genuine regression: still `mutated`.
    assert ("s1", "parquet@py", "parquet@py", "pass", "fail") in diff.mutated
    # Only the real regression counts as a violation.
    assert diff.has_violations is True
    reasons = diff.violation_reasons()
    assert all("vortex@rs" not in r for r in reasons)


def test_diff_against_oracle_all_skip_in_new_no_violation():
    """Corollary: when EVERY differing cell is an absent-toolchain skip (a
    fully toolchain-stripped run against a rich oracle), no violations."""
    oracle = _cj_from_reads("s1", [("vortex@rs", "vortex@rs", "pass")])
    new = _cj_from_reads("s1", [("vortex@rs", "vortex@rs", "skip", True)])
    diff = ledger.diff_against_oracle(new, oracle)
    assert diff.skipped == [("s1", "vortex@rs", "vortex@rs")]
    assert diff.mutated == [] and diff.removed == []
    assert diff.has_violations is False


def test_executed_skip_regression_is_a_mutation_not_benign():
    """The executed-skip twin: a reader that RAN and returned `skip` (unsupported
    type, comparator gap) has MEASURED the artifact, so pass -> skip is a real
    regression.

    Excusing every `skip` as an absent toolchain is what let comparator coverage
    rot away under a green gate: drop support for a type, get a `skip`, stay
    green. Only the never-invoked case (`toolchain_absent`) is benign.
    """
    oracle = _cj_from_reads("s1", [("parquet@java", "parquet@java", "pass")])
    # No `toolchain_absent` -> the sidecar ran and reported skip itself.
    new = _cj_from_reads("s1", [("parquet@java", "parquet@java", "skip")])
    diff = ledger.diff_against_oracle(new, oracle)

    assert diff.skipped == []
    assert diff.mutated == [
        ("s1", "parquet@java", "parquet@java", "pass", "skip")
    ]
    assert diff.has_violations is True


def _cj(slug, reads, *, skipped_cells=None):
    """A minimal verbose compliance-json with read cells + optional skipped
    write-cells (the shape `_slug_block` serializes)."""
    block = {
        "read": [{"artifact_cell": c, "reader_id": r, "status": s} for (c, r, s) in reads],
    }
    if skipped_cells is not None:
        block["skipped_cells"] = [{"cell": c, "reason": why} for (c, why) in skipped_cells]
    return {"slugs": {slug: block}}


def test_diff_against_oracle_absent_write_cell_is_skipped_not_removed():
    """The write-side twin of the benign reader skip: an oracle read-row over an artifact
    whose WRITE-cell this profile SKIPPED (sidecar binary absent → the artifact
    was never produced → its read rows are entirely ABSENT) is `skipped`, NOT
    `removed`. This keeps ONE committed oracle valid on a pure-Python box for the
    artifact-producer lane too (the reader lane is covered above)."""
    # Oracle (seeded WITH the Rust toolchain) records the rust-WRITTEN artifact.
    oracle = _cj(
        "s1",
        [
            ("parquet@py", "parquet@py", "pass"),   # in-process — this run measures it
            ("vortex@rs", "vortex@py", "pass"),      # reads over the rust-WRITTEN vortex artifact
            ("vortex@rs", "vortex@rs", "pass"),
        ],
    )
    # This run (pure-Python): the vortex@rs write-cell was skipped → its artifact
    # never produced → no read rows over it, declared in skipped_cells.
    new = _cj(
        "s1",
        [("parquet@py", "parquet@py", "pass")],
        skipped_cells=[("vortex@rs", "reference-writer binary absent")],
    )
    diff = ledger.diff_against_oracle(new, oracle)

    # Both vortex@rs read-rows are benign (write-cell skipped), not removed.
    assert ("s1", "vortex@rs", "vortex@py") in diff.skipped
    assert ("s1", "vortex@rs", "vortex@rs") in diff.skipped
    assert diff.removed == []
    assert diff.has_violations is False


def test_diff_against_oracle_absent_unexplained_is_still_removed():
    """Fail-closed guard: an oracle cell absent from a MEASURED slug that is
    NOT explained by a skipped write-cell is a genuine deletion → `removed`
    violation. The additive-only property survives the skipped-write-cell
    relaxation."""
    oracle = _cj(
        "s1",
        [
            ("parquet@py", "parquet@py", "pass"),
            ("parquet@py", "parquet@hardwood", "pass"),  # genuinely gone in new
            ("vortex@rs", "vortex@rs", "pass"),          # explained by a skip
        ],
    )
    new = _cj(
        "s1",
        [("parquet@py", "parquet@py", "pass")],
        skipped_cells=[("vortex@rs", "reference-writer binary absent")],
    )
    diff = ledger.diff_against_oracle(new, oracle)

    # vortex@rs absence is benign (skipped write-cell)...
    assert ("s1", "vortex@rs", "vortex@rs") in diff.skipped
    # ...but the parquet@py/parquet@hardwood absence is a real removal.
    assert ("s1", "parquet@py", "parquet@hardwood") in diff.removed
    assert diff.has_violations is True
    assert any("parquet@hardwood" in r for r in diff.violation_reasons())


def test_to_compliance_json_serializes_skipped_cells():
    """`skipped_cells` round-trips into the verbose ledger so a cross-profile
    `--check-oracle` can read them; empty list when nothing was skipped."""
    sc = _sc(
        "s1",
        reads=[_read("parquet@py", "parquet@py", "pass")],
        skipped=[
            ("vortex@rs", "reference-writer binary absent"),
            ("parquet@java", "reference-writer binary absent"),
        ],
    )
    block = ledger.to_compliance_json(_report(sc), generated_at="t", versions=None)["slugs"]["s1"]
    assert block["skipped_cells"] == [
        {"cell": "vortex@rs", "reason": "reference-writer binary absent"},
        {"cell": "parquet@java", "reason": "reference-writer binary absent"},
    ]
    # The no-skip case serializes an empty list (present, not missing).
    empty = ledger.to_compliance_json(_report(_sc("s2")), generated_at="t", versions=None)
    assert empty["slugs"]["s2"]["skipped_cells"] == []


def test_oracle_gate_cross_profile_skipped_write_cell_passes():
    """End-to-end: a report from a pure-Python profile (the rust write-cells
    skipped) checked against a Rust-seeded oracle PASSES — the absent rust-written
    artifacts' read rows are benign, not removed-cell violations. Mirrors the real
    `--check-oracle docs/v2/compliance.json` on a machine without the Rust binaries."""
    report = _report(
        _sc(
            "s1",
            writes=[_write("parquet@py"), _write("vortex@py")],
            reads=[
                _read("parquet@py", "parquet@py", "pass"),
                _read("vortex@py", "vortex@py", "pass"),
            ],
            skipped=[
                ("parquet@rs", "reference-writer binary absent"),
                ("vortex@rs", "reference-writer binary absent"),
            ],
        )
    )
    oracle = _cj(
        "s1",
        [
            ("parquet@py", "parquet@py", "pass"),
            ("vortex@py", "vortex@py", "pass"),
            ("vortex@rs", "vortex@rs", "pass"),      # rust-written, absent this profile
            ("parquet@rs", "parquet@py", "pass"),    # rust-written, absent this profile
        ],
    )
    ok, reasons = ledger.oracle_gate(report, oracle)
    assert ok is True, reasons


def test_diff_against_oracle_malformed_skipped_cells_degrades_and_does_not_mask():
    """`_skipped_cells` must degrade (never raise) on a
    malformed `skipped_cells` on the `new` side — the side actually consulted —
    AND a garbled skips list must NOT mask a genuine removal (fail-closed under
    junk input). Locks the isinstance guards under test."""
    oracle = _cj(
        "s1",
        [
            ("parquet@py", "parquet@py", "pass"),
            ("parquet@py", "parquet@hardwood", "pass"),  # genuinely absent in new
        ],
    )
    for bad in ("not-a-list", [123], [{"reason": "x"}], [{"cell": 123}], [None], []):
        new = {
            "slugs": {
                "s1": {
                    "read": [
                        {"artifact_cell": "parquet@py", "reader_id": "parquet@py",
                         "status": "pass"}
                    ],
                    "skipped_cells": bad,
                }
            }
        }
        diff = ledger.diff_against_oracle(new, oracle)  # must not raise
        # No parseable skip covers parquet@py, so the absent parquet@hardwood row
        # is a real removal — never silently reclassified to `skipped`.
        assert ("s1", "parquet@py", "parquet@hardwood") in diff.removed, bad
        assert diff.has_violations is True, bad


def test_diff_against_oracle_contradictory_produced_and_skipped_falls_to_removed():
    """Defense in depth: if a hand-edited `new` lists a
    cell in BOTH `skipped_cells` AND a real read row (a produced-AND-skipped
    contradiction the honest producer can't emit), a genuine per-reader removal
    over that cell must still be `removed`, not masked — the fail-closed property
    is intrinsic to the diff (`(slug, cell)` measured → not reclassified)."""
    oracle = _cj(
        "s1",
        [
            ("vortex@rs", "vortex@py", "pass"),   # this reader-row will be absent in new
            ("vortex@rs", "vortex@rs", "pass"),
        ],
    )
    # Contradiction: vortex@rs has a real read row AND appears in skipped_cells.
    new = _cj(
        "s1",
        [("vortex@rs", "vortex@rs", "pass")],
        skipped_cells=[("vortex@rs", "reference-writer binary absent")],
    )
    diff = ledger.diff_against_oracle(new, oracle)
    # vortex@rs IS measured (has a row) → the absent vortex@py row is a real
    # removal, NOT reclassified to skipped despite the skip note.
    assert ("s1", "vortex@rs", "vortex@py") in diff.removed
    assert ("s1", "vortex@rs", "vortex@py") not in diff.skipped
    assert diff.has_violations is True


# --------------------------------------------------------------------------- #
# 3. oracle_gate — exit semantics
# --------------------------------------------------------------------------- #


def test_oracle_gate_read_fail_is_fail():
    report = _report(_sc("s1", reads=[_read("parquet@py", "parquet@py", "fail", note="mismatch")]))
    ok, reasons = ledger.oracle_gate(report, None)
    assert ok is False
    assert any("read fail" in r for r in reasons)


def test_oracle_gate_write_fail_is_fail():
    report = _report(
        _sc(
            "s1",
            writes=[_write("parquet@java", roundtrip=False, variant_faithful=False, note="exit 1")],
            reads=[_read("parquet@py", "parquet@py", "pass")],
        )
    )
    ok, reasons = ledger.oracle_gate(report, None)
    assert ok is False
    assert any("write fail" in r for r in reasons)


def test_oracle_gate_skip_na_spec_ambiguous_pass():
    report = _report(
        _sc(
            "s1",
            writes=[_write("parquet@py", roundtrip=True)],
            reads=[
                _read("parquet@py", "parquet@java", "skip"),
                _read("parquet@py", "vortex@py", "na"),
                _read("parquet@py", "parquet@hardwood", "spec_ambiguous", note="underdefined"),
            ],
        )
    )
    ok, reasons = ledger.oracle_gate(report, None)
    assert ok is True
    assert reasons == []


def test_oracle_gate_known_fail_matching_oracle_passes():
    """5.4 semantics: WITH an oracle, a `fail` that MATCHES the oracle is a KNOWN
    recorded state (no deviation) → gate PASSES; a `pass`→`fail` vs the oracle is
    a regression → gate FAILS. (Raincloud's matrix legitimately carries known
    non-conformances, e.g. vortex@py can't encode the variant struct — the oracle
    records them; only DEVIATION gates.)"""
    report = _report(
        _sc("s1", reads=[_read("vortex@py", "vortex@py", "fail", note="variant panic")])
    )
    # Oracle recording that same cell as a KNOWN fail → no deviation → PASS.
    oracle_known = _cj_from_reads("s1", [("vortex@py", "vortex@py", "fail")])
    ok, reasons = ledger.oracle_gate(report, oracle_known)
    assert ok is True, reasons
    # Oracle recording it as PASS → now fail = mutation = regression → FAIL.
    oracle_was_pass = _cj_from_reads("s1", [("vortex@py", "vortex@py", "pass")])
    ok2, reasons2 = ledger.oracle_gate(report, oracle_was_pass)
    assert ok2 is False
    assert any("mutated" in r for r in reasons2)


def test_oracle_gate_no_oracle_clean_passes():
    report = _report(
        _sc("s1", writes=[_write("parquet@py")], reads=[_read("parquet@py", "parquet@py", "pass")])
    )
    ok, reasons = ledger.oracle_gate(report, None)
    assert ok is True and reasons == []


def test_oracle_gate_removed_cell_is_fail():
    # new matrix drops a cell the oracle had (a pass cell — removal is a
    # violation regardless of status).
    report = _report(
        _sc("s1", writes=[_write("parquet@py")], reads=[_read("parquet@py", "parquet@py", "pass")])
    )
    oracle = _cj_from_reads(
        "s1",
        [
            ("parquet@py", "parquet@py", "pass"),
            ("parquet@py", "parquet@hardwood", "pass"),  # removed in the new report
        ],
    )
    ok, reasons = ledger.oracle_gate(report, oracle)
    assert ok is False
    assert any("removed cell" in r for r in reasons)


# --------------------------------------------------------------------------- #
# 4. write_compliance_json — atomic write to a configurable path
# --------------------------------------------------------------------------- #


def test_write_compliance_json_atomic_to_explicit_path(tmp_path):
    report = _report(
        _sc("s1", writes=[_write("parquet@py")], reads=[_read("parquet@py", "parquet@py", "pass")])
    )
    out = tmp_path / "nested" / "compliance.json"
    ret = ledger.write_compliance_json(
        report, out, generated_at="2026-07-08T00:00:00Z", versions={"pyarrow": "x"}
    )
    assert ret == out and out.exists()
    data = json.loads(out.read_text())
    assert data["generated_at"] == "2026-07-08T00:00:00Z"
    assert data["versions"] == {"pyarrow": "x"}
    assert "s1" in data["slugs"] and "rollup" in data
    # No stray tmp left behind (atomic tmp -> replace).
    assert not (out.parent / "compliance.json.tmp").exists()


# --------------------------------------------------------------------------- #
# 5. Idempotence — present artifacts are READ, not re-encoded
# --------------------------------------------------------------------------- #


def _scratch(cell, slug):
    """Where compliance keeps `cell`'s output for `slug` (its per-writer scratch)."""
    from raincloud.pipeline.export import get_exporter

    return compliance.compliance_path(get_exporter(cell), slug)


def _build_artifacts(slug, table):
    """The canonical plus parquet@py/vortex@py outputs in compliance's scratch,
    as an earlier compliance run leaves them."""
    (canonical_path,) = canonical.write_canonical({"slug": slug}, [(slug, table)])
    for exporter in (ParquetExporter(), VortexExporter()):
        dest = _scratch(exporter.cell_id, slug)
        dest.parent.mkdir(parents=True, exist_ok=True)
        exporter.export({"slug": slug}, canonical_path, dest)
    return canonical_path


def _export_spy(monkeypatch):
    """Record (and still perform) each export compliance runs. The writer itself
    runs in a child process (`bounded.run_bounded`), so the spy wraps the call
    that starts it. Returns the shared call list."""
    calls: list[str] = []
    real = compliance.run_bounded

    def spy(exporter, spec, canon, dest):
        calls.append(exporter.cell_id)
        return real(exporter, spec, canon, dest)

    monkeypatch.setattr(compliance, "run_bounded", spy)
    return calls


def test_present_artifacts_not_reencoded(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "idempotent-present"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32()), "s": ["a", "b", "c"]})
    _build_artifacts(slug, table)

    p, v = _scratch("parquet@py", slug), _scratch("vortex@py", slug)
    p_mtime, v_mtime = p.stat().st_mtime_ns, v.stat().st_mtime_ns

    calls = _export_spy(monkeypatch)
    sc = compliance.run_compliance(
        {"slug": slug}, cells=["parquet@py", "vortex@py"], reader_ids=["parquet@py", "vortex@py"]
    )
    assert sc is not None
    # export() was NEVER invoked for the present in-process cells.
    assert calls == []
    # Both cells still appear in the matrix, synthesized from disk.
    assert set(sc.artifact_cells()) == {"parquet@py", "vortex@py"}
    assert {r.format_id: r.out_path for r in sc.write_results} == {"parquet@py": p, "vortex@py": v}
    notes = {r.format_id: r.compliance.note for r in sc.write_results}
    for cell in ("parquet@py", "vortex@py"):
        assert notes[cell].startswith("pre-existing (in-process, fresh); not re-encoded")
    # The write verdict is MEASURED — by the writer's own read-back of the file
    # on disk, not synthesized by the idempotent path (which cannot know it).
    rts = {r.format_id: r.compliance.roundtrip for r in sc.write_results}
    assert rts == {"parquet@py": True, "vortex@py": True}
    for cell in ("parquet@py", "vortex@py"):
        assert "read back: matches the canonical" in notes[cell]
    # Files untouched on disk (mtime unchanged).
    assert p.stat().st_mtime_ns == p_mtime
    assert v.stat().st_mtime_ns == v_mtime
    # Reads still ran over the on-disk artifacts (real verdicts).
    verdict_by = {(rr.artifact_cell, rr.reader_id): rr.verdict.status for rr in sc.read_results}
    assert verdict_by[("parquet@py", "parquet@py")] == "pass"
    assert verdict_by[("vortex@py", "vortex@py")] == "pass"


def test_reencode_forces_write(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "idempotent-reencode"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32())})
    _build_artifacts(slug, table)

    calls = _export_spy(monkeypatch)
    sc = compliance.run_compliance(
        {"slug": slug}, cells=["parquet@py"], reader_ids=["parquet@py"], reencode=True
    )
    assert sc is not None
    # --reencode forced a fresh export() of the present cell.
    assert calls == ["parquet@py"]
    assert set(sc.artifact_cells()) == {"parquet@py"}
    # A fresh export produces the real (non-"pre-existing") verdict note.
    notes = {r.format_id: r.compliance.note for r in sc.write_results}
    assert notes["parquet@py"] != "pre-existing; not re-encoded"


def test_absent_sidecar_still_skips_alongside_present_cell(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", raising=False)
    slug = "idempotent-sidecar-absent"
    table = pa.table({"n": pa.array([1, 2], type=pa.int32())})
    _build_artifacts(slug, table)

    sc = compliance.run_compliance(
        {"slug": slug},
        cells=["parquet@py", "parquet@hardwood"],
        reader_ids=["parquet@py"],
    )
    assert sc is not None
    # Present in-process cell is read from disk; absent sidecar still skips.
    assert "parquet@py" in set(sc.artifact_cells())
    skipped = {c for c, _ in sc.skipped_cells}
    assert "parquet@hardwood" in skipped


def test_stale_in_process_artifact_is_reencoded(tmp_path, monkeypatch):
    """An in-process artifact OLDER than the canonical is stale, so
    the idempotent skip must NOT apply — it re-encodes (mtime gate, à la convert.py).
    Guards against measuring a stale artifact as a fresh result."""
    import os as _os

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "idempotent-stale"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)

    # Make the parquet artifact STALE: mtime older than the canonical.
    p = _scratch("parquet@py", slug)
    old = canonical_path.stat().st_mtime - 100
    _os.utime(p, (old, old))

    calls = _export_spy(monkeypatch)
    compliance.run_compliance({"slug": slug}, cells=["parquet@py"], reader_ids=["parquet@py"])
    # Stale in-process artifact re-encoded (export() called), not skipped.
    assert "parquet@py" in calls


def test_malformed_oracle_degrades_not_raises():
    """`diff_against_oracle` / `_read_cells` must not raise on a
    partial/malformed oracle (hand-edited / truncated) — a bad entry is skipped,
    the diff treats it as fewer known cells (so `new` cells read as `added`)."""
    new = {
        "slugs": {
            "s": {"read": [{"artifact_cell": "parquet@py", "reader_id": "parquet@py",
                            "status": "pass", "note": "", "detail": ""}]}
        }
    }
    for bad_oracle in (
        {},                                          # empty
        {"slugs": "not-a-dict"},                     # slugs wrong type
        {"slugs": {"s": "not-a-dict"}},              # block wrong type
        {"slugs": {"s": {"read": "not-a-list"}}},    # read wrong type
        {"slugs": {"s": {"read": [{"reader_id": "x"}]}}},  # entry missing keys
        {"slugs": {"s": {"read": ["not-a-dict"]}}},  # entry wrong type
    ):
        diff = ledger.diff_against_oracle(new, bad_oracle)  # must not raise
        # The one real `new` cell is `added` (oracle had no usable cells); no crash.
        assert ("s", "parquet@py", "parquet@py") in diff.added
        assert not diff.has_violations


# --------------------------------------------------------------------------- #
# 6. No build -> ledger/compliance import edge
# --------------------------------------------------------------------------- #


def test_build_source_has_no_ledger_reference():
    import raincloud.pipeline.build as build

    src = Path(build.__file__).read_text()
    assert "ledger" not in src
    assert "compliance" not in src


def test_importing_build_does_not_import_ledger_or_compliance():
    code = (
        "import sys; import raincloud.pipeline.build; "
        "bad = {'raincloud.pipeline.ledger', 'raincloud.pipeline.compliance'} & set(sys.modules); "
        "sys.exit(1 if bad else 0)"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], cwd=str(REPO_ROOT), capture_output=True, text=True
    )
    assert proc.returncode == 0, (
        f"build pulled in ledger/compliance\nstdout={proc.stdout}\nstderr={proc.stderr}"
    )


# --------------------------------------------------------------------------- #
# 7. Path scrubbing + explicit slug skips
# --------------------------------------------------------------------------- #


def test_scrub_paths_relativizes_data_root_in_note_and_detail():
    """A reader/sidecar error quoting an absolute data-root path must be
    relativized on serialization — no home-dir leak in the committed oracle."""
    from raincloud.pipeline.spec import data_root

    root = str(data_root())
    abs_path = f"{root}/outputs/v2/foo/parquet-java/foo.parquet"
    sc = _sc(
        "foo",
        reads=[
            _read(
                "parquet@java",
                "parquet@py",
                "fail",
                note=f"open failed: {abs_path}",
                detail=f"Could not open '{abs_path}': Parquet file size is 0 bytes",
            )
        ],
    )
    cj = ledger.to_compliance_json(_report(sc), generated_at="", versions=None)
    block = cj["slugs"]["foo"]
    # No absolute data-root prefix survives anywhere in the serialized block.
    assert root not in json.dumps(block)
    # The path is relativized (not deleted): the relative form remains.
    assert "outputs/v2/foo/parquet-java/foo.parquet" in block["read"][0]["detail"]
    assert "outputs/v2/foo/parquet-java/foo.parquet" in block["read"][0]["note"]


def test_skipped_slugs_recorded_not_omitted():
    """`--skip-slug` slugs are serialized as explicit skip blocks (recorded, not
    silently dropped) and contribute nothing to the additive-only oracle gate."""
    report = ComplianceReport(
        slugs=[],
        skipped_slugs=[("mmmu", "OOM at scale"), ("code-contests", "OOM")],
    )
    cj = ledger.to_compliance_json(report, generated_at="", versions=None)
    mmmu = cj["slugs"]["mmmu"]
    assert mmmu["skipped"] is True
    assert mmmu["skip_reason"] == "OOM at scale"
    assert mmmu["read"] == [] and mmmu["write"] == []
    assert cj["slugs"]["code-contests"]["skipped"] is True
    # No read cells -> no removed/mutated cells -> no gate violation against itself.
    diff = ledger.diff_against_oracle(cj, cj)
    assert not diff.has_violations


def test_skip_slug_block_does_not_shadow_a_measured_slug():
    """If a slug is both measured and (spuriously) in skipped_slugs, the measured
    block wins (setdefault) — a skip never overwrites real data."""
    sc = _sc("s1", writes=[_write("parquet@py", note="ok")])
    report = ComplianceReport(slugs=[sc], skipped_slugs=[("s1", "should not win")])
    cj = ledger.to_compliance_json(report, generated_at="", versions=None)
    assert cj["slugs"]["s1"].get("skipped") is not True
    assert cj["slugs"]["s1"]["write"][0]["cell"] == "parquet@py"


# --------------------------------------------------------------------------- #
# 8. False-green regressions
#
# Each test here pins a path where the gate could report GREEN over a real
# regression, so they assert the FAILURE, not just the shape.
# --------------------------------------------------------------------------- #


def test_load_oracle_rejects_malformed_instead_of_failing_open():
    """A malformed oracle must RAISE, not degrade to "fewer known cells".

    Degrading is what made the gate fail OPEN: with an empty oracle every
    current cell looks merely `added`, so a corrupt baseline reported green over
    any regression at all.
    """
    import json as _json
    import tempfile
    from pathlib import Path as _Path

    import pytest

    with tempfile.TemporaryDirectory() as td:
        p = _Path(td) / "oracle.json"

        # `slugs` present but the wrong type — the old parser returned {} here.
        p.write_text(_json.dumps({"slugs": [1, 2, 3]}))
        with pytest.raises(ledger.MalformedOracle, match="must be an object"):
            ledger.load_oracle(p)

        # Unknown status string.
        p.write_text(_json.dumps({"slugs": {"s1": {"read": [
            {"artifact_cell": "parquet@py", "reader_id": "parquet@py",
             "status": "probably-fine"}], "write": [], "skipped_cells": []}}}))
        with pytest.raises(ledger.MalformedOracle, match="is not one of"):
            ledger.load_oracle(p)

        # Duplicate cell key — one verdict would shadow the other silently.
        row = {"artifact_cell": "parquet@py", "reader_id": "parquet@py",
               "status": "pass"}
        p.write_text(_json.dumps({"slugs": {"s1": {
            "read": [row, {**row, "status": "fail"}],
            "write": [], "skipped_cells": []}}}))
        with pytest.raises(ledger.MalformedOracle, match="duplicate cell"):
            ledger.load_oracle(p)

        # A well-formed ledger still loads.
        p.write_text(_json.dumps({"slugs": {"s1": {
            "read": [row], "write": [], "skipped_cells": []}}}))
        assert ledger.load_oracle(p)["slugs"]["s1"]["read"] == [row]


def test_write_roundtrip_regression_is_gated_with_an_oracle():
    """A writer regressing to `roundtrip=False` must fail the gate.

    Write rows were serialized but never COMPARED, and the absolute write check
    was skipped whenever an oracle was supplied — so a writer could start
    producing unfaithful artifacts and `--check-oracle` stayed green.
    """
    oracle = ledger.to_compliance_json(
        _report(_sc("s1", writes=[_write("parquet@py", roundtrip=True)])),
        generated_at="", versions=None,
    )
    regressed = _report(_sc("s1", writes=[_write("parquet@py", roundtrip=False)]))

    ok, reasons = ledger.oracle_gate(regressed, oracle)
    assert ok is False
    assert any("parquet@py" in r for r in reasons)


def test_total_writer_failure_cannot_hide_behind_zero_read_rows():
    """A slug whose every write-cell failed has no read rows — and must still be
    in scope for the `removed` check.

    Measurement scope used to be inferred from emitted READ rows, so a
    total writer-side collapse looked like "this slug was never measured" and
    every one of its oracle cells fell out of scope. Green on a full failure.
    """
    oracle = ledger.to_compliance_json(
        _report(_sc("s1",
                    writes=[_write("parquet@py", roundtrip=True)],
                    reads=[_read("parquet@py", "parquet@py", "pass")])),
        generated_at="", versions=None,
    )
    # Same slug measured, writer failed, so no artifact and no read rows at all.
    collapsed = _report(_sc("s1", writes=[_write("parquet@py", roundtrip=False)]))
    new = ledger.to_compliance_json(collapsed, generated_at="", versions=None)

    diff = ledger.diff_against_oracle(new, oracle)
    assert ("s1", "parquet@py", "parquet@py") in diff.removed
    assert diff.has_violations is True


def test_new_failing_cell_is_not_laundered_as_an_addition():
    """An `added` cell carrying a `fail` must fail the gate.

    "Additive-only" made every addition benign, so a newly measured slug or a
    newly enabled write-cell could arrive already red and pass.
    """
    oracle = ledger.to_compliance_json(
        _report(_sc("s1", writes=[_write("parquet@py")],
                    reads=[_read("parquet@py", "parquet@py", "pass")])),
        generated_at="", versions=None,
    )
    with_new_fail = _report(
        _sc("s1", writes=[_write("parquet@py")],
            reads=[_read("parquet@py", "parquet@py", "pass")]),
        _sc("s2", writes=[_write("parquet@py")],
            reads=[_read("parquet@py", "parquet@py", "fail")]),
    )
    ok, reasons = ledger.oracle_gate(with_new_fail, oracle)
    assert ok is False
    assert any("new read fail" in r and "s2" in r for r in reasons)
