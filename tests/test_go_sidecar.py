# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Apache Arrow Go writes the canonical IPC, not a converted intermediate."""
import json
import struct
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.export import get_exporter, get_reader
from tests._helpers import sidecar, write_ipc


@pytest.mark.parametrize("rows", [0, 1, 31])
def test_go_streams_groups_across_canonical_batches(tmp_path, monkeypatch, rows):
    sidecar("parquet@go")
    table = pa.table({"n": pa.array(range(rows), pa.int64()),
                      "nested": pa.array([[float(i), None] if i % 3 else None for i in range(rows)],
                                         pa.list_(pa.float64()))})
    canonical = write_ipc(tmp_path / "sample.arrow.zstd", table, max_chunksize=3)
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_MAX_ROWS", "5")
    spec = {"slug": "sample", "write": {"row_group_size_rows": 7}}
    result = get_exporter("parquet@go").export(spec, canonical, tmp_path / "out.parquet")
    assert result.compliance.roundtrip is True, result.compliance
    assert result.compliance.variant_faithful is True
    parquet = pq.ParquetFile(result.out_path)
    assert parquet.read().equals(table)
    assert [parquet.metadata.row_group(i).num_rows for i in range(parquet.num_row_groups)] == (
        [7] * (rows // 7) + ([rows % 7] if rows % 7 else []))
    sidecar("parquet@go", "read")
    assert get_reader("parquet@go").read_conformance(result.out_path, canonical).status == "pass"


def test_go_refuses_page_row_limit_without_publishing(tmp_path, monkeypatch):
    sidecar("parquet@go")
    canonical = write_ipc(tmp_path / "x.arrow.zstd", pa.table({"x": range(10)}))
    dest = tmp_path / "x.parquet"
    dest.write_bytes(b"previous generation")
    monkeypatch.setenv("RAINCLOUD_PARQUET_PAGE_ROWS", "10")
    result = get_exporter("parquet@go").export({"slug": "x"}, canonical, dest)
    assert result.compliance.roundtrip is False
    assert "cannot honour RAINCLOUD_PARQUET_PAGE_ROWS=10" in result.compliance.note
    assert dest.read_bytes() == b"previous generation"


def test_go_corrupt_file_reports_failure(tmp_path):
    binary = sidecar("parquet@go", "read")
    canonical = write_ipc(tmp_path / "x.arrow.zstd", pa.table({"x": [1]}))
    artifact, report = tmp_path / "bad.parquet", tmp_path / "report.json"
    artifact.write_bytes(b"broken parquet")
    subprocess.run([binary, "--input", str(artifact), "--canonical", str(canonical),
                    "--report", str(report)], check=True, timeout=30)
    assert json.loads(report.read_text())["status"] == "fail"


def test_go_empty_file_still_checks_types(tmp_path):
    sidecar("parquet@go", "read")
    canonical = write_ipc(tmp_path / "empty.arrow.zstd", pa.table({"x": pa.array([], pa.int64())}))
    artifact = tmp_path / "empty.parquet"
    pq.write_table(pa.table({"x": pa.array([], pa.string())}), artifact)
    assert get_reader("parquet@go").read_conformance(artifact, canonical).status == "fail"


@pytest.mark.parametrize("value", ["7e0", "7.9"])
def test_go_row_cap_uses_shared_number_grammar(tmp_path, monkeypatch, value):
    sidecar("parquet@go")
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_MAX_ROWS", value)
    canonical = write_ipc(tmp_path / "tiny.arrow.zstd", pa.table({"x": range(20)}), max_chunksize=3)
    result = get_exporter("parquet@go").export({"slug": "tiny"}, canonical, tmp_path / "tiny.parquet")
    assert result.compliance.roundtrip is True, result.compliance
    metadata = pq.ParquetFile(result.out_path).metadata
    assert [metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)] == [7, 7, 6]


@pytest.mark.parametrize("null_row", [False, True])
def test_go_preserves_native_variant(tmp_path, null_row):
    from tests.test_parquet_variant_sidecars import _variant_table
    sidecar("parquet@go")
    canonical = write_ipc(tmp_path / "variant.arrow.zstd", _variant_table(null_row), max_chunksize=1)
    result = get_exporter("parquet@go").export({"slug": "variant"}, canonical, tmp_path / "variant.parquet")
    assert result.compliance.roundtrip is True, result.compliance
    assert result.compliance.variant_faithful is True
    assert "Variant(" in str(pq.ParquetFile(result.out_path).schema)


@pytest.mark.parametrize("got,expected,status", [
    (pa.array(["123"]), pa.array([123]), "fail"),
    (pa.array([1]), pa.array([True]), "fail"),
    (pa.array([None], pa.string()), pa.array([None], pa.int64()), "fail"),
    (pa.array([0], pa.timestamp("us")), pa.array([0], pa.timestamp("us", "UTC")), "fail"),
    (pa.array([[]], pa.list_(pa.struct([("a", pa.int32())]))),
     pa.array([[]], pa.list_(pa.struct([("b", pa.int32())]))), "fail"),
    (pa.array([], pa.int32()), pa.array([], pa.int64()), "pass"),
    (pa.array([], pa.binary(4)), pa.array([], pa.binary()), "pass"),
    (pa.array([], pa.list_(pa.int32(), 2)), pa.array([], pa.large_list(pa.int64())), "pass"),
    (pa.array([], pa.list_(pa.int32(), 2)), pa.array([], pa.list_(pa.int32(), 3)), "fail"),
    (pa.array([struct.unpack("d", struct.pack("Q", 0x7ff8000000000001))[0]]),
     pa.array([struct.unpack("d", struct.pack("Q", 0x7ff8000000000002))[0]]), "fail"),
])
def test_go_schema_and_float_fidelity(tmp_path, got, expected, status):
    sidecar("parquet@go", "read")
    canonical = write_ipc(tmp_path / "c.arrow.zstd", pa.table({"x": expected}))
    artifact = tmp_path / "a.parquet"
    pq.write_table(pa.table({"x": got}), artifact)
    result = get_reader("parquet@go").read_conformance(artifact, canonical)
    assert result.status == status, result


@pytest.mark.parametrize("dictionary", [False, True])
def test_go_decoded_guard_counts_skewed_slice_values(tmp_path, monkeypatch, dictionary):
    sidecar("parquet@go")
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_TARGET_BYTES", str(4 << 20))
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES", "0")
    monkeypatch.setenv("RAINCLOUD_PARQUET_DICTIONARY", "0")
    table = pa.table({"x": ["x" * 1024] * 8192 + [""] * 57344})
    if dictionary:
        table = pa.table({"x": table["x"].dictionary_encode()})
    canonical = write_ipc(tmp_path / "skewed.arrow.zstd", table)
    result = get_exporter("parquet@go").export({"slug": "skewed"}, canonical, tmp_path / "skewed.parquet")
    assert result.compliance.roundtrip is True, result.compliance
    file = pq.ParquetFile(result.out_path)
    assert file.read().equals(table)
    # The writer checks after each 1024-row slice, allowing at most one slice of overshoot.
    assert max(file.read_row_group(i).cast(pa.schema([("x", pa.string())])).nbytes
               for i in range(file.num_row_groups)) < (5 << 20)
