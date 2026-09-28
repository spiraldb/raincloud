# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The parquet@hardwood (write + read) and vortex@jni (write) lanes, through the CLI
contract raincloud runs them by. Each test skips when its binary is not installed."""
from __future__ import annotations

import json
import subprocess
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.export.readers import _roundtrip_verdict
from raincloud.pipeline.export.sidecar import SidecarExporter
from tests._helpers import sidecar, write_ipc


def _canonical(tmp_path, table, name="source", chunk=None):
    return write_ipc(tmp_path / f"{name}.arrow.zstd", table, max_chunksize=chunk)


def _write(cell, tmp_path, table, ext, chunk=None):
    canonical = _canonical(tmp_path, table, chunk=chunk)
    artifact, report = tmp_path / f"data.{ext}", tmp_path / "write.json"
    proc = subprocess.run([sidecar(cell), "--input", str(canonical), "--output", str(artifact),
                           "--report", str(report)], capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr
    return canonical, artifact, json.loads(report.read_text())


def _read(tmp_path, artifact, canonical):
    report = tmp_path / "read.json"
    subprocess.run([sidecar("parquet@hardwood", "read"), "--input", str(artifact), "--canonical", str(canonical),
                    "--report", str(report)], check=True, capture_output=True, timeout=120)
    return json.loads(report.read_text())


def _mixed():
    # The shapes the catalog's canonicals hold (sidecars/README.md): narrow and unsigned
    # integers, floats, strings of every width, decimals, dates, times, timestamps, null
    # columns, lists and structs with null parents, and a dictionary column.
    n = 7
    return pa.table({
        "u8": pa.array([0, 255, None, 1, 2, 3, 4], pa.uint8()),
        "u32": pa.array([2**32 - 1, 0, None, 1, 2, 3, 4], pa.uint32()),
        "u64": pa.array([2**64 - 1, 0, None, 1, 2, 3, 4], pa.uint64()),
        "i16": pa.array([-(2**15), 2**15 - 1, None, 0, 1, 2, 3], pa.int16()),
        "f32": pa.array([float("nan"), -0.0, None, 1.5, 2.5, 3.5, 4.5], pa.float32()),
        "f64": pa.array([1.1, None, -0.0, float("inf"), 2.0, 3.0, 4.0]),
        "s": pa.array(["", "é", None, "a" * 300, "b", "c", "d"]),
        "ls": pa.array(["x", None, "y", "z", "w", "v", "u"], pa.large_string()),
        "sv": pa.array(["x", None, "y", "z", "w", "v", "u"], pa.string_view()),
        "dec": pa.array([1, None, -5, 7, 8, 9, 10], pa.decimal128(7, 2)),
        "dec15": pa.array([Decimal("1234567890123.45"), None, Decimal("-0.05"), 7, 8, 9, 10], pa.decimal128(15, 2)),
        "day": pa.array([0, None, 19000, -1, 1, 2, 3], pa.date32()),
        "us": pa.array([0, None, 86_399_999_999, 1, 2, 3, 4], pa.time64("us")),
        "ts": pa.array([-1, None, 1534377600, 0, 1, 2, 3], pa.timestamp("s")),
        "ts_us": pa.array([-1, None, 1534377600000000, 0, 1, 2, 3], pa.timestamp("us")),
        "flag": pa.array([True, False, None, True, False, True, False]),
        "nothing": pa.nulls(n),
        "list": pa.array([[1.5, None], None, [], [2.5], [3.5], [], None], pa.list_(pa.float64())),
        "fixed": pa.array([[1.0, 2.0]] * n, pa.list_(pa.float32(), 2)),
        "st": pa.array([{"a": 1, "t": ["x"]}, None, {"a": None, "t": None}, {"a": 4, "t": []},
                        {"a": 5, "t": ["y", "z"]}, None, {"a": 7, "t": ["w"]}],
                       pa.struct([("a", pa.int32()), ("t", pa.list_(pa.string()))])),
        "dict": pa.array(["p", "q", None, "p", "q", "p", "r"]).dictionary_encode(),
    })


def test_hardwood_writes_what_pyarrow_reads_back(tmp_path):
    expected = _mixed()
    canonical, artifact, verdict = _write("parquet@hardwood", tmp_path, expected, "parquet", chunk=3)
    assert verdict["roundtrip"] is True and verdict["variant_faithful"] is True, verdict
    assert _roundtrip_verdict("py", pq.read_table(artifact), expected).status == "pass"
    assert pq.ParquetFile(artifact).metadata.created_by.startswith("hardwood version ")


def test_hardwood_reads_what_pyarrow_writes(tmp_path):
    expected = _mixed()
    canonical = _canonical(tmp_path, expected)
    artifact = tmp_path / "py.parquet"
    pq.write_table(expected, artifact, row_group_size=3)
    verdict = _read(tmp_path, artifact, canonical)
    assert verdict["status"] == "pass", verdict


def test_hardwood_reads_an_int96_timestamp_as_a_comparator_gap(tmp_path):
    # INT96 is deprecated and has no Arrow type in this lane: unmeasured, never a verdict.
    expected = pa.table({"t": pa.array([0, None], pa.timestamp("ns"))})
    canonical = _canonical(tmp_path, expected)
    artifact = tmp_path / "int96.parquet"
    pq.write_table(expected, artifact, use_deprecated_int96_timestamps=True)
    verdict = _read(tmp_path, artifact, canonical)
    assert verdict["status"] == "skip" and "comparator gap" in verdict["note"], verdict
    assert "INT96" in verdict["detail"], verdict


def test_hardwood_page_header_peek_is_a_measured_read_fail(tmp_path):
    # Hardwood 1.1.0.Beta1 reads a page header from a 1 KiB peek and raises "Malformed
    # Parquet metadata" instead of growing it when page statistics are long (fixed after
    # the release by hardwood bdecd568, #1104; unreleased). pyarrow writes page statistics
    # up to 4 KiB untruncated, so a string column of 2 KB values is enough. The lane reports
    # a measured fail naming the cause, never a crash or a pass; when hardwoodVersion
    # moves past the fix this becomes a pass.
    expected = pa.table({"text": pa.array(["a" * 2000, "b" * 2000])})
    canonical = _canonical(tmp_path, expected)
    artifact = tmp_path / "long.parquet"
    pq.write_table(expected, artifact)
    verdict = _read(tmp_path, artifact, canonical)
    assert verdict["status"] == "fail" and "read error" in verdict["note"], verdict
    assert "Malformed Parquet metadata" in verdict["detail"], verdict


def test_hardwood_reports_an_unwritable_type_without_output(tmp_path):
    expected = pa.table({"d": pa.array([1, None], pa.duration("s"))})
    _, artifact, verdict = _write("parquet@hardwood", tmp_path, expected, "parquet")
    assert verdict["roundtrip"] is False and "unsupported type" in verdict["note"], verdict
    assert not artifact.exists()


@pytest.mark.parametrize("raw", ["abc", "-5", "0.5", "1_000", "\u00a05", "1\udcff"])
def test_hardwood_refuses_a_malformed_row_group_knob(tmp_path, monkeypatch, raw):
    # One knob grammar in every lane (sidecars/knob_cases.json).
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_MAX_ROWS", raw)
    _, artifact, verdict = _write("parquet@hardwood", tmp_path, pa.table({"x": [1, 2, 3]}), "parquet")
    assert verdict["roundtrip"] is False and "RAINCLOUD_ROW_GROUP_MAX_ROWS" in verdict["note"], verdict
    assert not artifact.exists()


@pytest.mark.parametrize("recipe_cap", [None, 600])
def test_hardwood_cuts_row_groups_at_the_planned_row(tmp_path, monkeypatch, recipe_cap):
    # The same plan parquet@rs gives: the environment's cap, or the recipe's
    # write.row_group_size_rows (which SidecarExporter passes as the environment's), cut
    # exactly, across the canonical's batch boundaries.
    monkeypatch.setenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", sidecar("parquet@hardwood"))
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_MAX_ROWS", "1e3")
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES", "0")
    spec = {"slug": "plan"} if recipe_cap is None else {"slug": "plan", "write": {"row_group_size_rows": recipe_cap}}
    cap = recipe_cap or 1000
    canonical = _canonical(tmp_path, pa.table({"x": pa.array(range(4500), pa.int64())}), name="plan", chunk=700)
    result = SidecarExporter("parquet@hardwood", "parquet", "raincloud-export-parquet-hardwood").export(
        spec, canonical, dest=tmp_path / "hardwood.parquet")
    assert result is not None and result.compliance.roundtrip is True, result
    meta = pq.ParquetFile(result.out_path).metadata
    groups = [meta.row_group(i).num_rows for i in range(meta.num_row_groups)]
    assert groups == [cap] * (4500 // cap) + ([4500 % cap] if 4500 % cap else []), groups


def test_jni_writer_round_trips_and_reads_back_in_python(tmp_path):
    vortex = pytest.importorskip("vortex")
    expected = _mixed()
    _, artifact, verdict = _write("vortex@jni", tmp_path, expected, "vortex", chunk=3)
    assert verdict["roundtrip"] is True, verdict
    assert _roundtrip_verdict("py", vortex.open(str(artifact)).to_arrow().read_all(), expected).status == "pass"


def test_jni_writer_stores_variant_as_the_python_writer_does(tmp_path):
    # Every vortex writer hands Vortex a VARIANT column as its storage struct, so one
    # format's writers agree on the dtype (see the vortex@rs twin in test_export_conformance).
    vortex = pytest.importorskip("vortex")
    from raincloud.pipeline.export.exporters import VortexExporter
    from raincloud.pipeline.variant import attach_variant_schema

    storage = pa.struct([pa.field("metadata", pa.binary(), nullable=False), ("value", pa.binary())])
    values = pa.array([{"metadata": b"\x01\x00\x00", "value": b"\x0c\x01"}, None], storage)
    table = pa.table({"v": values, "x": [1, 2]})
    table = table.cast(attach_variant_schema(table.schema, ["v"]))
    canonical, artifact, verdict = _write("vortex@jni", tmp_path, table, "vortex")
    assert verdict["roundtrip"] is True and verdict["variant_faithful"] is False, verdict
    py = VortexExporter().export({"slug": "variant"}, canonical, dest=tmp_path / "py.vortex")
    assert str(vortex.open(str(artifact)).dtype) == str(vortex.open(str(py.out_path)).dtype)


def test_jni_writer_keeps_boolean_offsets_across_batches(tmp_path):
    # Odd-sized batches leave Boolean buffers that are not byte-aligned at a batch start.
    vortex = pytest.importorskip("vortex")
    n = 100003
    expected = pa.table({"id": range(n), "flag": pa.array([None if i % 13 == 0 else i % 3 == 0 for i in range(n)])})
    _, artifact, verdict = _write("vortex@jni", tmp_path, expected, "vortex", chunk=1003)
    assert verdict["roundtrip"] is True, verdict
    assert vortex.open(str(artifact)).to_arrow().read_all().equals(expected)


def test_jni_writer_reports_a_type_vortex_refuses_with_its_reason(tmp_path):
    expected = pa.table({"f": pa.array([b"abc", None], pa.binary(3))})
    _, artifact, verdict = _write("vortex@jni", tmp_path, expected, "vortex")
    assert verdict["roundtrip"] is False and "FixedSizeBinary" in verdict["note"], verdict
    assert not artifact.exists()
