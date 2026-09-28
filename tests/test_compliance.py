# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Tests for the read-conformance machinery and `raincloud compliance`.

Hermetic: `RAINCLOUD_HOME` points at a tmp dir so `prepared_*` / `output_format_dir`
resolve under tmp, never the real outputs/. The in-process readers (parquet@py,
vortex@py) are REAL — they read artifacts the built-in exporters produced from a
synthetic canonical and verdict the round-trip. The `SidecarReader` is exercised
via a MOCK reader binary (a python shim run under the current interpreter) driven
by a `RAINCLOUD_READER_*` env override — no real Rust/JVM binary, no PATH
mutation, no network. The end-to-end covers `run_compliance` producing a matrix
over the in-process cells+readers while skipping the absent sidecars, and asserts
the default build carries no import edge into compliance.
"""
from __future__ import annotations

import json
import stat
import subprocess
import sys
import textwrap
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from raincloud.pipeline import compliance
from raincloud.pipeline.export import run_reader
from raincloud.pipeline.export.exporters import ParquetExporter, VortexExporter
from raincloud.pipeline.export.readers import (
    PyarrowParquetReader,
    SidecarReader,
    VortexPyReader,
    _reader_env_var,
)
from raincloud.pipeline.spec import REPO_ROOT, prepared_parquet, prepared_vortex
from tests._helpers import write_canonical

# --- reader-sidecar CLI mocks (honor the --input/--canonical/--report contract) ---

_READER_PASS_MOCK = """
import argparse, json, os
ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--canonical", required=True)
ap.add_argument("--report", required=True)
a = ap.parse_args()
assert os.path.exists(a.input), "sidecar reader got no --input"
assert os.path.exists(a.canonical), "sidecar reader got no --canonical"
with open(a.report, "w") as f:
    json.dump({"status": "pass", "note": "mock reader"}, f)
"""

_READER_AMBIGUOUS_MOCK = """
import argparse, json
ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--canonical", required=True)
ap.add_argument("--report", required=True)
a = ap.parse_args()
with open(a.report, "w") as f:
    json.dump({"status": "spec_ambiguous",
               "note": "offset encoding underdefined (dfa1/vortex-java#205)"}, f)
"""

_READER_FAIL_EXIT_MOCK = """
import sys
sys.exit(3)
"""

_READER_BAD_REPORT_MOCK = """
import argparse
ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--canonical", required=True)
ap.add_argument("--report", required=True)
a = ap.parse_args()
open(a.report, "w").write("not json {")
"""


# A WRITE sidecar that produces a real artifact but reports roundtrip=False
# (a legitimate "measured mismatch, artifact promoted" state) — used to prove that
# a pre-existing sidecar artifact is NOT blessed to roundtrip=True on a
# compliance re-run.
_WRITE_MISMATCH_MOCK = """
import argparse, json
import pyarrow as pa
import pyarrow.parquet as pq
ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--output", required=True)
ap.add_argument("--report", required=True)
a = ap.parse_args()
with pa.ipc.open_file(a.input) as r:
    pq.write_table(r.read_all(), a.output)
with open(a.report, "w") as f:
    json.dump({"roundtrip": False, "variant_faithful": False,
               "note": "measured mismatch (mock)"}, f)
"""


def _write_mock(tmp_path: Path, name: str, body: str) -> Path:
    path = tmp_path / name
    path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body))
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IREAD)
    return path


def _build_artifacts(slug, table):
    """Produce the canonical + parquet@py + vortex@py artifacts for `slug`."""
    canonical_path = write_canonical(slug, table)
    ParquetExporter().export({"slug": slug}, canonical_path)
    VortexExporter().export({"slug": slug}, canonical_path)
    return canonical_path


# --------------------------------------------------------------------------- #
# 1. In-process read-conformance (REAL)
# --------------------------------------------------------------------------- #


def test_pyarrow_parquet_reader_passes(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "read-parquet-ok"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32()), "s": ["a", "b", "c"]})
    canonical_path = _build_artifacts(slug, table)

    verdict = PyarrowParquetReader().read_conformance(prepared_parquet(slug), canonical_path)
    assert verdict.status == "pass"
    assert "round-trips" in verdict.note


def test_vortex_reader_passes_despite_type_normalization(tmp_path, monkeypatch):
    """Vortex round-trips string -> string_view; the tolerant cast comparison
    still verdicts `pass` on the data round-trip."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "read-vortex-ok"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32()), "s": ["a", "b", "c"]})
    canonical_path = _build_artifacts(slug, table)

    verdict = VortexPyReader().read_conformance(prepared_vortex(slug), canonical_path)
    assert verdict.status == "pass"


def test_corrupted_artifact_is_measured_fail_not_raise(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "read-corrupt"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)

    # Corrupt the parquet artifact in place.
    prepared_parquet(slug).write_bytes(b"not a parquet file at all")

    verdict = PyarrowParquetReader().read_conformance(prepared_parquet(slug), canonical_path)
    assert verdict.status == "fail"  # measured, NOT an escaping exception
    assert "read error" in verdict.note


def test_row_mismatch_is_fail(tmp_path, monkeypatch):
    """A parquet with a different row count than the canonical -> fail."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "read-rowmismatch"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)

    # Overwrite the parquet with a valid-but-different table (1 row, not 3).
    pq.write_table(pa.table({"n": pa.array([1], type=pa.int32())}), prepared_parquet(slug))
    verdict = PyarrowParquetReader().read_conformance(prepared_parquet(slug), canonical_path)
    assert verdict.status == "fail"
    assert "row count" in verdict.note


def test_reader_on_wrong_format_is_na(tmp_path, monkeypatch):
    """run_reader dispatch: a vortex reader applied to a parquet artifact -> na
    (the read is never attempted)."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "read-wrongfmt"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)

    rr = run_reader(VortexPyReader(), "parquet@py", prepared_parquet(slug), canonical_path)
    assert rr.verdict.status == "na"
    assert rr.artifact_cell == "parquet@py"
    assert rr.reader_id == "vortex@py"

    # And the matching-format dispatch DOES run (pass).
    rr_ok = run_reader(PyarrowParquetReader(), "parquet@py", prepared_parquet(slug), canonical_path)
    assert rr_ok.verdict.status == "pass"


# --------------------------------------------------------------------------- #
# 2. SidecarReader (subprocess CLI)
# --------------------------------------------------------------------------- #


def _sidecar(reader_id="parquet@java", formats=None):
    return SidecarReader(reader_id, formats or {"parquet"}, "raincloud-read-parquet-java")


def test_sidecar_reader_pass_via_mock(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "sidecar-read-ok"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)

    mock = _write_mock(tmp_path, "reader_pass.py", _READER_PASS_MOCK)
    monkeypatch.setenv(_reader_env_var("parquet@java"), str(mock))

    verdict = _sidecar().read_conformance(prepared_parquet(slug), canonical_path)
    assert verdict.status == "pass"
    assert verdict.note == "mock reader"


def test_sidecar_reader_spec_ambiguous_via_mock(tmp_path, monkeypatch):
    """A reader can report the first-class `spec_ambiguous` verdict (the class of
    the underdefined-spec bug raincloud found in the wild)."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "sidecar-read-amb"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)

    mock = _write_mock(tmp_path, "reader_amb.py", _READER_AMBIGUOUS_MOCK)
    monkeypatch.setenv(_reader_env_var("parquet@java"), str(mock))

    verdict = _sidecar().read_conformance(prepared_parquet(slug), canonical_path)
    assert verdict.status == "spec_ambiguous"
    assert "underdefined" in verdict.note


def test_sidecar_reader_skip_when_absent(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    monkeypatch.delenv(_reader_env_var("parquet@java"), raising=False)
    slug = "sidecar-read-skip"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)

    verdict = _sidecar().read_conformance(prepared_parquet(slug), canonical_path)
    assert verdict.status == "skip"
    assert "absent" in verdict.note
    # ...and it is marked the NEVER-INVOKED skip. The oracle gate's benign-skip
    # rule keys off this flag: only a reader that never ran is benign across profiles.
    assert verdict.toolchain_absent is True


def test_sidecar_reported_skip_is_not_marked_toolchain_absent(tmp_path, monkeypatch):
    """A sidecar that RAN and reported `skip` must NOT be flagged absent.

    It measured the artifact (unsupported type / comparator gap), so a
    pass -> skip is a real regression. A sidecar must not be able to mark its own
    verdict un-comparable by putting anything in its report.
    """
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "sidecar-reported-skip"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)
    # A stub sidecar that exits 0 and reports `skip` — and even tries to claim
    # toolchain_absent in its report, which the parser must ignore.
    stub = tmp_path / "stub-reader"
    stub.write_text(
        "#!/bin/sh\n"
        'while [ $# -gt 0 ]; do case "$1" in --report) shift; OUT="$1";; esac; shift; done\n'
        'printf \'{"status":"skip","note":"unsupported type",'
        '"toolchain_absent":true}\' > "$OUT"\n'
    )
    stub.chmod(0o755)
    monkeypatch.setenv(_reader_env_var("parquet@java"), str(stub))

    verdict = _sidecar().read_conformance(prepared_parquet(slug), canonical_path)
    assert verdict.status == "skip"
    assert verdict.toolchain_absent is False


def test_sidecar_reader_bad_exit_is_measured_fail(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "sidecar-read-fail"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)

    mock = _write_mock(tmp_path, "reader_fail.py", _READER_FAIL_EXIT_MOCK)
    monkeypatch.setenv(_reader_env_var("parquet@java"), str(mock))

    verdict = _sidecar().read_conformance(prepared_parquet(slug), canonical_path)
    assert verdict.status == "fail"
    assert "exit 3" in verdict.note


def test_sidecar_reader_unlaunchable_is_measured_fail(tmp_path, monkeypatch):
    """A set-but-unlaunchable RAINCLOUD_READER_* override degrades to a MEASURED
    fail (never raises OSError) — mirrors the sidecar-exporter guard."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "sidecar-read-broken"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)

    monkeypatch.setenv(_reader_env_var("parquet@java"), str(tmp_path / "nope-binary"))
    verdict = _sidecar().read_conformance(prepared_parquet(slug), canonical_path)
    assert verdict.status == "fail"
    assert "failed to launch" in verdict.note


def test_sidecar_reader_bad_report_is_measured_fail(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "sidecar-read-badreport"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = _build_artifacts(slug, table)

    mock = _write_mock(tmp_path, "reader_badrep.py", _READER_BAD_REPORT_MOCK)
    monkeypatch.setenv(_reader_env_var("parquet@java"), str(mock))

    verdict = _sidecar().read_conformance(prepared_parquet(slug), canonical_path)
    assert verdict.status == "fail"
    assert "bad report" in verdict.note


# --------------------------------------------------------------------------- #
# 3. `raincloud compliance` end-to-end (hermetic, over a synthetic slug)
# --------------------------------------------------------------------------- #


def test_run_compliance_matrix_over_synthetic_slug(tmp_path, monkeypatch, capsys):
    from raincloud.pipeline.export.sidecar import SidecarExporter

    # This case measures the unavailable-toolchain path even on machines with
    # real sidecars configured for the independent fidelity tests.
    monkeypatch.setattr(SidecarExporter, "_discover", lambda self: None)
    monkeypatch.setattr(SidecarReader, "_discover", lambda self: None)
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "compliance-e2e"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32()), "s": ["a", "b", "c"]})
    write_canonical(slug, table)  # build only the canonical; compliance re-produces artifacts

    cells = compliance.default_cells()
    readers = compliance.default_readers()
    # Sanity: the reconciled write-cell set + the reader set are present. parquet@rs
    # + vortex@rs are the REAL Rust sidecar lanes (5.3); the Java lanes are
    # registered too, their binaries absent here.
    assert {"parquet@py", "vortex@py", "parquet@rs", "parquet@java", "parquet@hardwood",
            "vortex@rs", "vortex@jni"} <= set(cells)
    assert {"parquet@py", "vortex@py", "parquet@rs", "vortex@rs", "vortex@jni"} <= set(readers)

    sc = compliance.run_compliance({"slug": slug}, cells=cells, reader_ids=readers)
    assert sc is not None

    # In-process cells produced artifacts; the absent sidecars were skipped (no
    # RAINCLOUD_SIDECAR_* env override + the binary names are not on PATH here).
    assert set(sc.artifact_cells()) == {"parquet@py", "vortex@py"}
    skipped = {c for c, _ in sc.skipped_cells}
    assert skipped == {"parquet@rs", "parquet@java", "parquet@hardwood", "vortex@rs", "vortex@jni"}

    verdict_by = {(rr.artifact_cell, rr.reader_id): rr.verdict.status for rr in sc.read_results}
    # In-process readers over their own format -> pass.
    assert verdict_by[("parquet@py", "parquet@py")] == "pass"
    assert verdict_by[("vortex@py", "vortex@py")] == "pass"
    # Cross-format -> na.
    assert verdict_by[("parquet@py", "vortex@py")] == "na"
    assert verdict_by[("vortex@py", "parquet@py")] == "na"
    # Applicable-but-absent sidecar readers -> skip.
    assert verdict_by[("parquet@py", "parquet@java")] == "skip"
    assert verdict_by[("vortex@py", "vortex@jni")] == "skip"

    counts = sc.counts()
    assert counts["pass"] == 2
    assert counts["fail"] == 0
    assert counts["skip"] >= 2 and counts["na"] >= 2

    # The printed matrix names the slug, a PASS glyph, and a skip glyph.
    matrix = compliance.format_matrix(sc, readers)
    assert "compliance matrix: compliance-e2e" in matrix
    assert "PASS" in matrix and "skip" in matrix
    # write-side Compliance surfaced too.
    assert "write parquet@py" in matrix


def test_unknown_reader_id_skips_gracefully_not_raises(tmp_path, monkeypatch, capsys):
    """An unregistered `--readers` id degrades to a stderr skip note, NOT a raw
    KeyError traceback — mirroring the write side's handling of an
    unregistered cell. The known reader still runs."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "compliance-bad-reader"
    table = pa.table({"n": pa.array([1, 2], type=pa.int32())})
    write_canonical(slug, table)

    sc = compliance.run_compliance(
        {"slug": slug}, cells=["parquet@py"], reader_ids=["parquet@py", "bogus@nope"]
    )
    assert sc is not None  # did NOT raise
    # The known reader produced a verdict; the unknown one contributed none.
    rids = {rr.reader_id for rr in sc.read_results}
    assert "parquet@py" in rids and "bogus@nope" not in rids
    assert "no reader registered for 'bogus@nope'" in capsys.readouterr().err


def test_sidecar_write_fail_not_blessed_on_reencode_rerun(tmp_path, monkeypatch):
    """A SIDECAR write-cell that produced an artifact but measured
    `roundtrip=False` must NOT be synthesized to `roundtrip=True` on a compliance
    re-run (artifact present). Sidecar cells always re-run for their real verdict;
    only in-process cells are idempotently skipped. Guards the write-fail gate."""
    from raincloud.pipeline import ledger

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    mock = _write_mock(tmp_path, "mock_writer.py", _WRITE_MISMATCH_MOCK)
    monkeypatch.setenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", str(mock))

    slug = "sidecar-write-mismatch"
    table = pa.table({"n": pa.array([1, 2], type=pa.int32())})
    write_canonical(slug, table)

    def _run():
        return compliance.run_compliance(
            {"slug": slug}, cells=["parquet@hardwood"], reader_ids=["parquet@py"]
        )

    # Run 1 (fresh encode): the sidecar's real verdict is roundtrip=False.
    sc1 = _run()
    v1 = {r.format_id: r.compliance.roundtrip for r in sc1.write_results}
    assert v1["parquet@hardwood"] is False
    assert ledger.oracle_gate(compliance.ComplianceReport(slugs=[sc1]), None)[0] is False

    # Run 2 (artifact NOW on disk): must RE-RUN the sidecar, still roundtrip=False —
    # NOT a synthesized "pre-existing" roundtrip=True that would bless the bad artifact.
    sc2 = _run()
    w2 = {r.format_id: r.compliance for r in sc2.write_results}
    assert w2["parquet@hardwood"].roundtrip is False
    assert "pre-existing" not in w2["parquet@hardwood"].note
    assert ledger.oracle_gate(compliance.ComplianceReport(slugs=[sc2]), None)[0] is False


def test_measured_write_failure_skips_reads_over_stale_artifact(tmp_path, monkeypatch):
    """A measured write-FAILURE (sidecar exit!=0 → sha256='') must
    contribute NO read cells this run — even with a STALE prior artifact on disk —
    so the read matrix never reads a stale artifact as PASS while the write row
    says roundtrip=False."""
    import pyarrow.parquet as pq

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    fail_mock = _write_mock(tmp_path, "fail_writer.py", "import sys\nsys.exit(1)\n")
    monkeypatch.setenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", str(fail_mock))

    slug = "failed-write-stale-artifact"
    table = pa.table({"n": pa.array([1, 2], type=pa.int32())})
    write_canonical(slug, table)

    # Plant a STALE prior artifact where compliance keeps the sidecar's output
    # (a valid parquet the in-process reader WOULD read as pass if it were not
    # skipped).
    from raincloud.pipeline.export import get_exporter

    stale = compliance.compliance_path(get_exporter("parquet@hardwood"), slug)
    stale.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, stale)

    sc = compliance.run_compliance(
        {"slug": slug}, cells=["parquet@hardwood"], reader_ids=["parquet@py"]
    )
    # Write row records the measured failure ...
    w = {r.format_id: r.compliance.roundtrip for r in sc.write_results}
    assert w["parquet@hardwood"] is False
    # ... and NO read cell was produced for the failed write (stale artifact unread).
    assert not any(rr.artifact_cell == "parquet@hardwood" for rr in sc.read_results)


def test_write_cell_panic_is_measured_not_crash(tmp_path, monkeypatch):
    """A write cell that RAISES (e.g. Vortex 0.69 panics on a variant struct —
    PanicException is a BaseException) must be recorded as a measured write-failure,
    never crash the compliance step. Surfaced running real `compliance` on the
    VARIANT slug countries-of-the-world (compliance runs the FULL cell set incl.
    vortex@py, which the build had skipped)."""
    from raincloud.pipeline.export.exporters import VortexExporter

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "write-panic"
    table = pa.table({"n": pa.array([1, 2], type=pa.int32())})
    write_canonical(slug, table)  # canonical only; the vortex@py artifact is absent

    class _FakePanic(BaseException):
        """Stand-in for pyo3_runtime.PanicException (a BaseException)."""

    def _boom(self, spec, canonical, dest=None):
        raise _FakePanic("not implemented")

    monkeypatch.setattr(VortexExporter, "export", _boom)

    sc = compliance.run_compliance(
        {"slug": slug}, cells=["vortex@py"], reader_ids=["vortex@py"]
    )
    assert sc is not None  # did NOT crash
    w = {r.format_id: r.compliance for r in sc.write_results}
    assert w["vortex@py"].roundtrip is False
    assert "vortex@py: _FakePanic: not implemented" in w["vortex@py"].note
    # No read cell over the failed write (its sha256 is '', so reads skip it).
    assert not any(rr.artifact_cell == "vortex@py" for rr in sc.read_results)


def test_run_compliance_returns_none_when_canonical_absent(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    assert compliance.run_compliance({"slug": "never-built"}) is None


def test_cli_main_no_selection_returns_2(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    assert compliance.main([]) == 2
    assert "nothing selected; pass slugs or --all" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# 4. The default build carries NO import edge into compliance
# --------------------------------------------------------------------------- #


def test_build_source_has_no_compliance_reference():
    import raincloud.pipeline.build as build

    src = Path(build.__file__).read_text()
    assert "compliance" not in src


def test_importing_build_does_not_import_compliance():
    """Order-independent: importing build in a fresh interpreter must NOT pull in
    raincloud.pipeline.compliance (the client build stays pure + zero-setup)."""
    code = (
        "import sys; import raincloud.pipeline.build; "
        "sys.exit(1 if 'raincloud.pipeline.compliance' in sys.modules else 0)"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code], cwd=str(REPO_ROOT), capture_output=True, text=True
    )
    assert proc.returncode == 0, (
        f"build imported compliance transitively\nstdout={proc.stdout}\nstderr={proc.stderr}"
    )


# --------------------------------------------------------------------------- #
# CLI argument guards
# --------------------------------------------------------------------------- #


def test_cli_rejects_same_path_for_write_ledger_and_check_oracle(tmp_path, monkeypatch, capsys):
    """`--write-ledger P --check-oracle P` must refuse to run.

    The ledger was written BEFORE the oracle was read, so the natural invocation
    (both defaulting to docs/v{n}/compliance.json) made the gate compare the run
    against itself — a guaranteed green regardless of any regression.
    """
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    same = tmp_path / "compliance.json"
    rc = compliance.main(
        ["uci-iris", "--write-ledger", str(same), "--check-oracle", str(same)]
    )
    assert rc == 2
    err = capsys.readouterr().err
    assert "same file" in err and "against itself" in err
    # Refused BEFORE measuring anything — no ledger written.
    assert not same.exists()


def test_cli_rejects_unknown_requested_cell_or_reader(tmp_path, monkeypatch, capsys):
    """An explicitly requested unknown cell/reader is an error, not a skip.

    Skipping it with a note meant `--readers parquet@jav` measured ZERO cells and
    still exited 0: a green run that covered nothing.
    """
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    assert compliance.main(["uci-iris", "--readers", "parquet@jav"]) == 2
    assert "no such reader registered" in capsys.readouterr().err
    assert compliance.main(["uci-iris", "--cells", "parquet@nope"]) == 2
    assert "no such cell registered" in capsys.readouterr().err


def test_cli_malformed_oracle_fails_before_measuring(tmp_path, monkeypatch, capsys):
    """A malformed `--check-oracle` file fails fast, before the campaign runs."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    bad = tmp_path / "bad.json"
    bad.write_text('{"slugs": [1, 2, 3]}')
    assert compliance.main(["uci-iris", "--check-oracle", str(bad)]) == 2
    assert "MALFORMED ORACLE" in capsys.readouterr().err


def test_written_ledger_records_invocation_scope(tmp_path, monkeypatch):
    """The ledger records WHAT WAS REQUESTED, not just what was measured.

    Without a scope block a partial ledger is indistinguishable from a silent
    shrink — 128 of 250 slugs looks the same either way.
    """
    # A real manifest slug (the CLI selects from the manifest) with RAINCLOUD_HOME
    # in tmp, so nothing is built: the run records the slug as selected-but-
    # missing-canonical, which is exactly the gap the scope block must surface.
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    out = tmp_path / "ledger.json"
    compliance.main(
        ["uci-iris", "--cells", "parquet@py", "--readers", "parquet@py",
         "--write-ledger", str(out)]
    )
    scope = json.loads(out.read_text())["scope"]
    assert scope["selected_slugs"] == ["uci-iris"]
    assert scope["requested_cells"] == ["parquet@py"]
    assert scope["requested_readers"] == ["parquet@py"]
    assert scope["all"] is False
    # Selected but never measured — visible, not silently absent.
    assert scope["missing_canonical_slugs"] == ["uci-iris"]


def test_self_roundtrip_is_measured_not_asserted(tmp_path, monkeypatch):
    """The write `roundtrip` verdict must come from a real read of the artifact.

    The in-process exporters used to hardcode `roundtrip=True` right after
    writing, so the committed ledger recorded passes nothing had verified. Here
    the artifact is CORRUPTED after the write: an honest pipeline must report a
    measured `False`, while the old fabricated verdict would still say True.
    """
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "self-rt-measured"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32())})
    _build_artifacts(slug, table)

    # Plant a valid file holding DIFFERENT data where compliance keeps this
    # writer's scratch output, fresh (newer than the canonical) so the
    # idempotent path adopts it without re-encoding.
    from raincloud.pipeline.export import get_exporter

    dest = compliance.compliance_path(get_exporter("parquet@py"), slug)
    dest.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"n": pa.array([9], type=pa.int32())}), dest)

    sc = compliance.run_compliance(
        {"slug": slug}, cells=["parquet@py"], reader_ids=["parquet@py"]
    )
    assert sc is not None
    verdict = {r.format_id: r.compliance.roundtrip for r in sc.write_results}
    assert verdict["parquet@py"] is False, "a corrupted artifact must not report a round-trip"
    assert "read back: parquet@py: row count" in sc.write_results[0].compliance.note
    assert sc.write_results[0].out_path == dest, "the planted scratch file is what was measured"


def test_in_process_write_is_measured_without_a_self_reader(tmp_path, monkeypatch):
    """An in-process write cell reads its own file back, so its verdict is
    measured even when no self-reader is requested -- and is an oracle cell."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "self-rt-unrequested"
    table = pa.table({"n": pa.array([1, 2], type=pa.int32())})
    _build_artifacts(slug, table)

    # Request the parquet cell but only the VORTEX reader — no self-read cell.
    sc = compliance.run_compliance(
        {"slug": slug}, cells=["parquet@py"], reader_ids=["vortex@py"]
    )
    assert sc is not None
    assert sc.write_results[0].compliance.roundtrip is True

    from raincloud.pipeline import ledger

    cj = ledger.to_compliance_json(
        compliance.ComplianceReport(slugs=[sc]), generated_at="", versions=None
    )
    assert cj["slugs"][slug]["write"][0]["roundtrip"] is True
    assert ledger._write_cells(cj) == {(slug, "parquet@py", ledger.WRITE_CELL_READER): "pass"}


# --------------------------------------------------------------------------- #
# The float contract — Python must agree with the Rust/Java lanes (raw bits)
# --------------------------------------------------------------------------- #


def test_float_contract_nan_roundtrip_passes():
    """A byte-identical round-trip of a NaN column is a PASS.

    `Table.equals` uses IEEE semantics where NaN != NaN, so a faithful
    round-trip of any NaN-containing column was reported as a data mismatch —
    a false FAIL that Rust/Java (raw-bit comparison) did not produce.
    """
    from raincloud.pipeline.export.readers import _roundtrip_verdict

    nan = float("nan")
    t = pa.table({"f": pa.array([nan, 1.0, None], type=pa.float64())})
    assert _roundtrip_verdict("t", t, t).status == "pass"


def test_float_contract_signed_zero_is_a_mismatch():
    """`+0.0` vs `-0.0` is a MISMATCH: the sign bit did not survive.

    IEEE says they are equal, so a writer that flipped a sign bit was recorded
    as faithful — a false PASS. Rust/Java compare raw bits and would flag it.
    """
    from raincloud.pipeline.export.readers import _roundtrip_verdict

    pos = pa.table({"f": pa.array([0.0], type=pa.float64())})
    neg = pa.table({"f": pa.array([-0.0], type=pa.float64())})
    v = _roundtrip_verdict("t", pos, neg)
    assert v.status == "fail"
    assert "float bits differ" in v.detail


def test_float_contract_null_is_not_nan():
    """A null slot and a NaN value are different data, not interchangeable."""
    from raincloud.pipeline.export.readers import _roundtrip_verdict

    nulls = pa.table({"f": pa.array([None], type=pa.float64())})
    nans = pa.table({"f": pa.array([float("nan")], type=pa.float64())})
    assert _roundtrip_verdict("t", nulls, nans).status == "fail"


def test_float_contract_still_catches_ordinary_differences():
    """The bit comparison must not become permissive: 1.5 != 1.6.

    Guards the specific bug this replaced — comparing via `Array.cast(int64)`
    (a VALUE cast) truncated 1.5 and 1.6 both to 1 and passed everything.
    """
    from raincloud.pipeline.export.readers import _roundtrip_verdict

    a = pa.table({"f": pa.array([1.5], type=pa.float64())})
    b = pa.table({"f": pa.array([1.6], type=pa.float64())})
    assert _roundtrip_verdict("t", a, b).status == "fail"
    # ...and float32 works the same way.
    a32 = pa.table({"f": pa.array([1.5], type=pa.float32())})
    b32 = pa.table({"f": pa.array([1.6], type=pa.float32())})
    assert _roundtrip_verdict("t", a32, b32).status == "fail"


def test_float_contract_preserves_lossless_normalization():
    """Non-float tolerance is unchanged: string -> string_view still passes."""
    from raincloud.pipeline.export.readers import _roundtrip_verdict

    canonical = pa.table({"s": pa.array(["a", "b"], type=pa.string())})
    got = pa.table({"s": pa.array(["a", "b"], type=pa.string_view())})
    assert _roundtrip_verdict("t", got, canonical).status == "pass"
