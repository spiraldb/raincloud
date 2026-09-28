# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Tests for the `SidecarExporter` reference-writer CLI seam.

Hermetic: `RAINCLOUD_HOME` points at a tmp dir so `output_format_dir` resolves
under tmp, never the real outputs/. Discovery is driven via the
`RAINCLOUD_SIDECAR_PARQUET_HARDWOOD` env override pointing at a MOCK script (a tiny
python shim run under the current interpreter, so pyarrow is available) — no
real Rust/Java binary and no PATH mutation. Covers: success (mock writes a real
parquet + report), skip (binary absent -> None, filtered by run_exporters),
failure (mock exit 1 / bad report -> MEASURED failure, no raise), and
coexistence of the `parquet@py` and `parquet@hardwood` cells under the cell-keyed
registry.
"""
from __future__ import annotations

import stat
import sys
import textwrap
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.export import get_exporter, run_exporters
from raincloud.pipeline.export.sidecar import _env_var
from raincloud.pipeline.spec import output_format_dir
from tests._helpers import write_canonical

# A mock sidecar that honors the CLI contract: read --input (arrow-ipc), write a
# real parquet to --output, emit a passing --report.
_SUCCESS_MOCK = """
import argparse, json
import pyarrow as pa
import pyarrow.parquet as pq

ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--output", required=True)
ap.add_argument("--report", required=True)
a = ap.parse_args()

with pa.ipc.open_file(a.input) as r:
    table = r.read_all()
pq.write_table(table, a.output)
with open(a.report, "w") as f:
    json.dump({"roundtrip": True, "variant_faithful": True, "note": "mock"}, f)
"""

# A mock that fails to run (non-zero exit, no output, no report).
_FAIL_MOCK = """
import sys
sys.exit(1)
"""

# A mock that runs (exit 0) but emits a malformed report.
_BAD_REPORT_MOCK = """
import argparse
ap = argparse.ArgumentParser()
ap.add_argument("--input", required=True)
ap.add_argument("--output", required=True)
ap.add_argument("--report", required=True)
a = ap.parse_args()
open(a.output, "wb").write(b"stub")
open(a.report, "w").write("not json {")
"""

# A mock that hangs — used to prove RAINCLOUD_EXPORT_TIMEOUT → measured fail.
_SLEEP_MOCK = """
import time
time.sleep(30)
"""


def _write_mock(tmp_path: Path, body: str) -> Path:
    """Write an executable mock sidecar run by the current (venv) interpreter."""
    path = tmp_path / "mock_sidecar.py"
    path.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body))
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IREAD)
    return path


def test_sidecar_success_writes_the_parquet_file(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    mock = _write_mock(tmp_path, _SUCCESS_MOCK)
    monkeypatch.setenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", str(mock))

    slug = "sidecar-ok"
    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32()), "s": ["a", "b", "c"]})
    canonical_path = write_canonical(slug, table)

    result = get_exporter("parquet@hardwood").export({"slug": slug}, canonical_path)

    assert result is not None
    assert result.format_id == "parquet@hardwood"
    # One file per format: every Parquet writer writes parquet/.
    dest = output_format_dir(slug, "parquet") / f"{slug}.parquet"
    assert result.out_path == dest
    assert dest.exists()
    assert result.nbytes > 0 and len(result.sha256) == 64
    # Compliance verdict lifted from the report.
    assert result.compliance.roundtrip is True
    assert result.compliance.variant_faithful is True
    assert result.compliance.note == "mock"
    # The written parquet round-trips.
    assert pq.read_table(dest).equals(table)
    # No stray tmp left behind.
    assert not list(dest.parent.glob("*.tmp"))


def test_sidecar_skip_when_binary_absent(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    # No env override; the registered binary name is guaranteed absent from PATH.
    monkeypatch.delenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", raising=False)

    slug = "sidecar-skip"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = write_canonical(slug, table)

    # Direct export -> None (skip).
    assert get_exporter("parquet@hardwood").export({"slug": slug}, canonical_path) is None

    # run_exporters filters the None -> empty results + a stderr skip note.
    results = run_exporters(
        {"slug": slug}, canonical_path, ["parquet@hardwood"]
    )
    assert results == []
    assert "parquet@hardwood skipped" in capsys.readouterr().err
    assert not (output_format_dir(slug, "parquet") / f"{slug}.parquet").exists()


def test_sidecar_failure_is_measured_not_raised(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    mock = _write_mock(tmp_path, _FAIL_MOCK)
    monkeypatch.setenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", str(mock))

    slug = "sidecar-fail"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = write_canonical(slug, table)

    # Non-zero exit -> a MEASURED failure verdict, NOT an exception.
    result = get_exporter("parquet@hardwood").export({"slug": slug}, canonical_path)
    assert result is not None
    assert result.format_id == "parquet@hardwood"
    assert result.compliance.roundtrip is False
    assert result.compliance.variant_faithful is False
    assert "exit 1" in result.compliance.note
    # No artifact (and no tmp) promoted.
    dest = output_format_dir(slug, "parquet") / f"{slug}.parquet"
    assert not dest.exists()
    assert not list(dest.parent.glob("*.tmp"))


def test_sidecar_bad_report_is_measured_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    mock = _write_mock(tmp_path, _BAD_REPORT_MOCK)
    monkeypatch.setenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", str(mock))

    slug = "sidecar-bad-report"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = write_canonical(slug, table)

    # Exit 0 but malformed report -> MEASURED failure; the stub output is NOT promoted.
    result = get_exporter("parquet@hardwood").export({"slug": slug}, canonical_path)
    assert result is not None
    assert result.compliance.roundtrip is False
    assert "bad report" in result.compliance.note
    dest = output_format_dir(slug, "parquet") / f"{slug}.parquet"
    assert not dest.exists()
    assert not list(dest.parent.glob("*.tmp"))


def test_sidecar_timeout_is_measured_fail_not_hang(tmp_path, monkeypatch):
    """A hanging reference-writer must degrade to a measured failure under
    RAINCLOUD_EXPORT_TIMEOUT (the ceiling on every writer), never hang the step."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    mock = _write_mock(tmp_path, _SLEEP_MOCK)
    monkeypatch.setenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", str(mock))
    monkeypatch.setenv("RAINCLOUD_EXPORT_TIMEOUT", "0.3")

    slug = "sidecar-timeout"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = write_canonical(slug, table)

    result = get_exporter("parquet@hardwood").export({"slug": slug}, canonical_path)
    assert result is not None
    assert result.compliance.roundtrip is False
    assert "timed out" in result.compliance.note
    dest = output_format_dir(slug, "parquet") / f"{slug}.parquet"
    assert not dest.exists()
    assert not list(dest.parent.glob("*.tmp"))


def test_sidecar_broken_env_override_is_measured_not_raised(tmp_path, monkeypatch):
    """A set-but-unlaunchable RAINCLOUD_SIDECAR_* override (moved/renamed/mistyped)
    must degrade to a MEASURED failure, NOT raise FileNotFoundError out of export()
    — honoring the rule that a sidecar never hard-fails."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    monkeypatch.setenv(
        "RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", str(tmp_path / "does-not-exist-binary")
    )

    slug = "sidecar-broken-override"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = write_canonical(slug, table)

    result = get_exporter("parquet@hardwood").export({"slug": slug}, canonical_path)
    assert result is not None  # NOT a skip (the operator explicitly set it) ...
    assert result.compliance.roundtrip is False  # ... a measured failure
    assert "failed to launch" in result.compliance.note
    dest = output_format_dir(slug, "parquet") / f"{slug}.parquet"
    assert not dest.exists()
    assert not list(dest.parent.glob("*.tmp"))


def test_parquet_py_and_hardwood_cells_coexist():
    """The cell-keyed registry lets parquet@py and parquet@hardwood both resolve to
    distinct exporters of the same bare format."""
    py = get_exporter("parquet@py")
    hw = get_exporter("parquet@hardwood")
    assert py is not hw
    assert py.cell_id == "parquet@py"
    assert hw.cell_id == "parquet@hardwood"
    assert py.format_id == hw.format_id == "parquet"


def test_reconciled_write_cell_set_is_registered():
    """The 2026-07-08 target WRITE-cell set: parquet has distinct encoders —
    py (in-process pyarrow/cpp), rs (arrow-rs, the Rust sidecar in `sidecars/rust/`),
    java (Apache reference), hardwood (Hardwood, an independent pure-Java implementation); vortex is registrable via
    rs/jni sidecar bindings. `parquet@rs` is now a REAL arrow-rs sidecar lane (5.3)."""
    from raincloud.pipeline.export import get_exporter as _ge

    for cell in (
        "parquet@py",
        "parquet@rs",
        "parquet@java",
        "parquet@hardwood",
        "vortex@rs",
        "vortex@jni",
    ):
        assert _ge(cell).cell_id == cell


def test_env_var_helper():
    assert _env_var("parquet@hardwood") == "RAINCLOUD_SIDECAR_PARQUET_HARDWOOD"
    assert _env_var("parquet@java") == "RAINCLOUD_SIDECAR_PARQUET_JAVA"
    # `@` and `-` both map to `_` (a hyphenated impl segment collapses cleanly).
    assert _env_var("vortex@a-b") == "RAINCLOUD_SIDECAR_VORTEX_A_B"


@pytest.mark.parametrize("installed", [True, False])
def test_priority_picks_the_installed_writer_for_the_one_file(tmp_path, monkeypatch, installed):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    if installed:
        monkeypatch.setenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", str(_write_mock(tmp_path, _SUCCESS_MOCK)))
    else:
        monkeypatch.delenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", raising=False)
    slug = "sidecar-priority"
    table = pa.table({"n": pa.array([1, 2], type=pa.int32())})
    canonical_path = write_canonical(slug, table)
    spec = {"slug": slug, "export": {"formats": ["parquet"], "priority": ["hardwood", "py"]}}
    (result,) = run_exporters(spec, canonical_path)
    # The preferred writer when it is installed, the next one when it is not;
    # the file is the same path either way, and the cell says which ran.
    assert result.format_id == ("parquet@hardwood" if installed else "parquet@py")
    assert result.out_path == output_format_dir(slug, "parquet") / f"{slug}.parquet"
    assert pq.read_table(result.out_path).equals(table)
