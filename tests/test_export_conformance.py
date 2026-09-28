# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Writer contracts checked through independent readers and physical schemas."""
import json
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.export.readers import _roundtrip_verdict
from tests._helpers import find_sidecar, sidecar, write_ipc


def _write(tmp_path, table, cell="rs"):
    canonical, artifact, report = (tmp_path / name for name in ("source.arrow", "data.parquet", "write.json"))
    write_ipc(canonical, table, max_chunksize=2)
    proc = subprocess.run([sidecar(f"parquet@{cell}"), "--input", str(canonical), "--output", str(artifact),
                           "--report", str(report)], capture_output=True, text=True, timeout=120)
    return proc, canonical, artifact, report


def _timestamp_table(shape, tz):
    dtype = pa.timestamp("s", tz)
    values = [-1, None, 0, 1534377600]
    if shape == "struct":
        values = [{"when": x} for x in values] + [None]
        dtype = pa.struct([("when", dtype)])
    elif shape == "list":
        values = [values, None, []]
        dtype = pa.list_(dtype)
    elif shape == "map":
        values = [[("when", x)] for x in values] + [None, []]
        dtype = pa.map_(pa.string(), dtype)
    elif shape == "empty":
        values = []
    elif shape == "dictionary":
        return pa.table({"event": pa.array(values, dtype).dictionary_encode()})
    return pa.table({"event": pa.array(values, dtype)})


@pytest.mark.parametrize("shape", ["scalar", "struct", "list", "empty", "map", "dictionary"])
@pytest.mark.parametrize("tz", [None, "UTC"])
def test_rust_timestamp_export_has_portable_parquet_type(tmp_path, shape, tz):
    expected = _timestamp_table(shape, tz)
    proc, _, artifact, report = _write(tmp_path, expected)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(report.read_text())["roundtrip"] is True
    # Inspect the standard Parquet schema, independently of Arrow's private
    # ARROW:schema footer that can disguise plain INT64 as a timestamp.
    parquet = pq.ParquetFile(artifact)
    logical = json.loads(parquet.schema.column(len(parquet.schema) - 1).logical_type.to_json())
    assert logical["Type"] == "Timestamp", logical
    assert logical["timeUnit"] == "milliseconds", logical
    assert logical["isAdjustedToUTC"] is (tz is not None)
    assert _roundtrip_verdict("py", parquet.read(), expected).status == "pass"


@pytest.mark.parametrize("shape", ["scalar", "struct", "list", "empty"])
def test_java_reads_rust_seconds_timestamp(tmp_path, shape):
    binary = sidecar("parquet@java", "read")
    expected = _timestamp_table(shape, None)
    proc, canonical, artifact, _ = _write(tmp_path, expected)
    assert proc.returncode == 0, proc.stderr
    report = tmp_path / "read.json"
    subprocess.run([binary, "--input", str(artifact), "--canonical", str(canonical),
                    "--report", str(report)], check=True, capture_output=True, timeout=60)
    verdict = json.loads(report.read_text())
    assert verdict["status"] == "pass", verdict


@pytest.mark.parametrize("value", [2**63 - 1, -(2**63)])
def test_rust_seconds_overflow_rejects_before_publication(tmp_path, value):
    # A failure is a report, never a bare exit: the ledger records its cause.
    proc, _, artifact, report = _write(tmp_path, pa.table({"event": pa.array([value], pa.timestamp("s"))}))
    assert proc.returncode == 0, proc.stderr
    assert "timestamp" in proc.stderr.lower(), proc.stderr
    verdict = json.loads(report.read_text())
    assert verdict["roundtrip"] is False and "timestamp" in verdict["note"].lower(), verdict
    assert not artifact.exists()


def test_java_reads_rust_seconds_time(tmp_path):
    # Parquet has no seconds time unit either: arrow-rs would write bare INT32.
    binary = find_sidecar("parquet@java", "read")
    expected = pa.table({"at": pa.array([0, None, 86_399], pa.time32("s"))})
    proc, canonical, artifact, report = _write(tmp_path, expected)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(report.read_text())["roundtrip"] is True
    parquet = pq.ParquetFile(artifact)
    logical = json.loads(parquet.schema.column(0).logical_type.to_json())
    assert logical["Type"] == "Time" and logical["timeUnit"] == "milliseconds", logical
    assert _roundtrip_verdict("py", parquet.read(), expected).status == "pass"
    if not binary:
        pytest.skip("parquet@java reader not installed")
    read = tmp_path / "read.json"
    subprocess.run([binary, "--input", str(artifact), "--canonical", str(canonical),
                    "--report", str(read)], check=True, capture_output=True, timeout=60)
    verdict = json.loads(read.read_text())
    # The JVM comparator does not judge time-unit changes (sidecars/README.md).
    assert verdict["status"] in {"pass", "skip"}, verdict


# "1\udcff" reaches the child as the bytes 1 0xFF: not UTF-8.
@pytest.mark.parametrize("raw", ["abc", "-5", "-0", "0.5", "1_000", "\uff11\uff12", "\u00a05", "1\udcff"])
@pytest.mark.parametrize("cell", ["rs", "java"])
def test_sidecar_writer_refuses_a_malformed_row_group_knob(tmp_path, monkeypatch, cell, raw):
    # Every lane reads the knob one way (sidecars/knob_cases.json): a malformed
    # value is a measured failure naming the variable, never quietly the default.
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_MAX_ROWS", raw)
    proc, _, artifact, report = _write(tmp_path, pa.table({"x": [1, 2, 3]}), cell)
    assert proc.returncode == 0, proc.stderr
    verdict = json.loads(report.read_text())
    assert verdict["roundtrip"] is False and "RAINCLOUD_ROW_GROUP_MAX_ROWS" in verdict["note"], verdict
    assert not artifact.exists()


@pytest.mark.parametrize("recipe_cap", [None, 600])
def test_one_recipe_and_environment_give_every_writer_one_row_group_plan(tmp_path, monkeypatch, recipe_cap):
    # The row cap binds (bytes are off): the environment's, or the recipe's
    # `write.row_group_size_rows`, which wins in every lane. Every writer slices
    # the canonical batch that crosses the cap and cuts exactly there, so the
    # 700-row batches give every lane the same layout.
    from raincloud.pipeline.export.exporters import ParquetExporter
    from raincloud.pipeline.export.sidecar import SidecarExporter

    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_MAX_ROWS", "1e3")
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES", "0")
    spec = {"slug": "plan"} if recipe_cap is None else {"slug": "plan", "write": {"row_group_size_rows": recipe_cap}}
    cap = recipe_cap or 1000
    table = pa.table({"x": pa.array(range(4500), pa.int64())})
    canonical = write_ipc(tmp_path / "plan.arrow.zstd", table, max_chunksize=700)
    plans = {}
    ParquetExporter().export(spec, canonical, dest=tmp_path / "py.parquet")
    plans["py"] = tmp_path / "py.parquet"
    for cell in ("rs", "java"):
        exporter = SidecarExporter(f"parquet@{cell}", "parquet", f"raincloud-export-parquet-{cell}")
        result = exporter.export(spec, canonical, dest=tmp_path / f"{cell}.parquet")
        if result is not None:
            assert result.compliance.roundtrip is True, result.compliance
            plans[cell] = result.out_path
    groups = {cell: [pq.ParquetFile(path).metadata.row_group(i).num_rows
                     for i in range(pq.ParquetFile(path).metadata.num_row_groups)]
              for cell, path in plans.items()}
    exact = [cap] * (4500 // cap) + ([4500 % cap] if 4500 % cap else [])
    assert groups["py"] == exact, groups
    if len(plans) < 2:
        pytest.skip("no sidecar Parquet writer installed to compare with parquet@py")
    assert all(plan == groups["py"] for plan in groups.values()), groups


def test_jni_preserves_boolean_offsets_across_batches(tmp_path):
    writer, reader = find_sidecar("vortex@rs"), find_sidecar("vortex@jni", "read")
    if not writer or not reader:
        pytest.skip("compiled Rust Vortex writer and JNI reader required")
    # An odd-sized multi-partition scan exposes non-byte-aligned Boolean buffers.
    # Vortex JNI 0.84 misreads the second partition; 0.86.1 rebases the offsets.
    n = 100003
    flags = [None if i % 13 == 0 else i % 3 == 0 for i in range(n)]
    expected = pa.table({"id": range(n), "flag": pa.array(flags, pa.bool_())})
    canonical, artifact = tmp_path / "source.arrow", tmp_path / "data.vortex"
    report = tmp_path / "write.json"
    write_ipc(canonical, expected, compression=None, max_chunksize=1003)
    subprocess.run([writer, "--input", str(canonical), "--output", str(artifact),
                    "--report", str(report)], check=True, capture_output=True, timeout=60)
    assert json.loads(report.read_text())["roundtrip"] is True
    for changed, status in [(False, "pass"), (True, "fail")]:
        if changed:
            flags[50003] = True  # was False: a negative control beyond the boundary
            expected = expected.set_column(1, "flag", pa.array(flags, pa.bool_()))
            write_ipc(canonical, expected, compression=None, max_chunksize=1003)
        report = tmp_path / f"read-{changed}.json"
        subprocess.run([reader, "--input", str(artifact), "--canonical", str(canonical),
                        "--report", str(report)], check=True, capture_output=True, timeout=60)
        verdict = json.loads(report.read_text())
        assert verdict["status"] == status, verdict
        if changed:
            assert 'column "flag" row 50003' in verdict["detail"], verdict


def test_rust_row_groups_are_sized_by_encoded_bytes(tmp_path, monkeypatch):
    # Encoded before compression, as the Python lane sizes them (parquet@java
    # measures closed pages compressed). arrow-rs's own limit measures after
    # compression, and applied only to rows it had buffered, so a whole-table
    # write met just the row cap.
    target = 1 << 20
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES", str(target))
    n = 400_000
    expected = pa.table({"id": pa.array(range(n), pa.int64()),
                         "tag": pa.array([f"tag-{i % 97}" for i in range(n)])})
    proc, _, artifact, report = _write(tmp_path, expected)
    assert proc.returncode == 0, proc.stderr
    assert json.loads(report.read_text())["roundtrip"] is True
    meta = pq.ParquetFile(artifact).metadata
    assert meta.num_row_groups > 2
    sizes = sorted(meta.row_group(i).total_byte_size for i in range(meta.num_row_groups - 1))
    assert abs(sizes[len(sizes) // 2] - target) <= target * 0.15, sizes
    assert pq.read_table(artifact).equals(expected)


def test_rust_and_python_vortex_writers_store_variant_alike(tmp_path):
    # vortex@py hands Vortex a VARIANT column as its storage struct; vortex@rs
    # must too, or Vortex reads the extension as native VARIANT and the two
    # writers of one format disagree on the dtype.
    vortex = pytest.importorskip("vortex")
    from raincloud.pipeline.export.exporters import VortexExporter
    from raincloud.pipeline.variant import attach_variant_schema

    binary = sidecar("vortex@rs")
    storage = pa.struct([pa.field("metadata", pa.binary(), nullable=False), ("value", pa.binary())])
    values = pa.array([{"metadata": b"\x01\x00\x00", "value": b"\x0c\x01"}, None], storage)
    table = pa.table({"v": values, "x": [1, 2]})
    table = table.cast(attach_variant_schema(table.schema, ["v"]))
    canonical = write_ipc(tmp_path / "variant.arrow.zstd", table)
    py = VortexExporter().export({"slug": "variant"}, canonical, dest=tmp_path / "py.vortex")
    rs, report = tmp_path / "rs.vortex", tmp_path / "rs.json"
    subprocess.run([binary, "--input", str(canonical), "--output", str(rs), "--report", str(report)],
                   check=True, timeout=120)
    verdict = json.loads(report.read_text())
    assert verdict["roundtrip"] is True and verdict["variant_faithful"] is False, verdict
    assert str(vortex.open(str(rs)).dtype) == str(vortex.open(str(py.out_path)).dtype)
