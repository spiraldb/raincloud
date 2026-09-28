# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The VARIANT stamp declares the storage nullability the specs require.

`arrow.parquet.variant` requires a non-nullable `metadata` child, and Parquet's
VariantEncoding makes `metadata` required and an unshredded `value` required
(a shredded `value` is optional). DuckDB exports every field nullable, so the
stamp declares them, checks every row against the declaration, and every
Parquet writer then emits `required binary metadata` inside the VARIANT group.
Sidecar tests skip when the binary is not installed.
"""
from __future__ import annotations

import json
import re
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud import duckdb_connect
from raincloud.pipeline import canonical, duckdb_variant
from raincloud.pipeline.export.exporters import ParquetExporter
from raincloud.pipeline.spec import output_format_dir
from raincloud.pipeline.variant import VARIANT_EXT, attach_variant, attach_variant_schema
from tests._helpers import sidecar, write_ipc

_LOOSE = pa.struct([("metadata", pa.binary()), ("value", pa.binary())])
# Variant int8 1 (metadata: version 1, empty dictionary).
_ROW = {"metadata": b"\x01\x00\x00", "value": b"\x0c\x01"}


def _children(field: pa.Field) -> dict[str, bool]:
    return {child.name: child.nullable for child in field.type}


def _required(artifact, column="v") -> set[str]:
    """Children of `column`'s group the Parquet schema declares `required`."""
    text = str(pq.ParquetFile(artifact).schema)
    group = re.search(rf"group [^\n]*\b{column} [^\n]*\{{(.*?)\}}", text, re.S)
    assert group, text
    return set(re.findall(r"required binary [^;\n]*?(\w+);", group.group(1)))


def test_stamp_declares_metadata_and_unshredded_value_non_nullable():
    schema = attach_variant_schema(pa.schema([("v", _LOOSE), ("x", pa.int64())]), ["v"])
    field = schema.field("v")
    assert _children(field) == {"metadata": False, "value": False}
    assert field.nullable  # a missing VARIANT is a null struct
    assert field.metadata == VARIANT_EXT
    assert schema.field("x") == pa.field("x", pa.int64())


def test_a_shredded_value_keeps_its_nullability():
    shredded = pa.struct([("metadata", pa.binary()), ("value", pa.binary()), ("typed_value", pa.int64())])
    field = attach_variant_schema(pa.schema([("v", shredded)]), ["v"]).field("v")
    assert _children(field) == {"metadata": False, "value": True, "typed_value": True}


@pytest.mark.parametrize("dtype, match", [
    (pa.string(), "expected a struct"),
    (pa.struct([("value", pa.binary())]), "holds `metadata`"),
    (pa.struct([("metadata", pa.binary()), ("value", pa.binary()), ("extra", pa.int8())]), "nothing else"),
    (pa.struct([("metadata", pa.string()), ("value", pa.binary())]), "expected binary"),
    (pa.struct([("metadata", pa.binary()), ("typed_value", pa.struct([("a", pa.int8())]))]), "not supported"),
])
def test_a_column_that_is_not_a_variant_storage_struct_is_refused(dtype, match):
    with pytest.raises(ValueError, match=match):
        attach_variant_schema(pa.schema([("v", dtype)]), ["v"])


def test_attach_variant_retypes_the_data_and_keeps_null_rows():
    # DuckDB nulls the children of a NULL struct: a don't-care slot, not malformed.
    loose = pa.StructArray.from_arrays(
        [pa.array([b"\x01\x00\x00", None]), pa.array([b"\x0c\x01", None])],
        names=["metadata", "value"], mask=pa.array([False, True]))
    table = attach_variant(pa.table({"v": loose}), ["v"])
    column = table.column("v")
    assert column.type == table.schema.field("v").type
    assert column.to_pylist() == [_ROW, None]
    assert all(chunk.field(0).null_count == 0 for chunk in column.chunks)
    table.validate(full=True)


@pytest.mark.parametrize("child", ["metadata", "value"])
def test_a_null_required_child_in_a_present_row_fails_naming_column_and_rows(child):
    values = {"metadata": [b"\x01\x00\x00"] * 6, "value": [b"\x0c\x01"] * 6}
    values[child][2] = values[child][3] = None  # both in the second 2-row batch
    loose = pa.StructArray.from_arrays([pa.array(values["metadata"]), pa.array(values["value"])],
                                       names=["metadata", "value"])
    table = pa.Table.from_batches(pa.table({"v": loose}).to_batches(max_chunksize=2))
    with pytest.raises(ValueError, match=rf"VARIANT column 'v': 2 non-null row\(s\) with a null "
                                         rf"`{child}`, rows 2..3\. .*malformed VARIANT data"):
        attach_variant(table, ["v"])


def test_streaming_bridge_declares_and_conforms_every_batch(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    con = duckdb_connect()
    try:
        sql = """SELECT i, CASE WHEN i % 3 = 0 THEN NULL ELSE variant_to_parquet_variant(
                     CAST(CAST('{"a": ' || i || '}' AS JSON) AS VARIANT)) END AS data FROM range(50) t(i)"""
        schema, batches = duckdb_variant.stream_canonical_arrow(con, sql, ["data"], batch_size=7)
        assert _children(schema.field("data")) == {"metadata": False, "value": False}
        with canonical.open_canonical_writer("variant-stream", schema) as writer:
            for batch in batches:
                assert batch.schema == schema
                writer.write_batch(batch)  # the IPC writer refuses a batch whose schema differs
    finally:
        con.close()
    got = pa.ipc.open_file(str(output_format_dir("variant-stream", "arrow") / "variant-stream.arrow.zstd")).read_all()
    assert got.schema == schema
    assert got.column("data").null_count == 17


def test_streaming_bridge_fails_a_null_metadata_row_numbered_across_batches():
    con = duckdb_connect()
    try:
        sql = """SELECT {'metadata': CASE WHEN i IN (7, 8) THEN NULL ELSE '\\x01\\x00\\x00'::BLOB END,
                         'value': '\\x0c\\x01'::BLOB} AS data FROM range(20) t(i)"""
        _, batches = duckdb_variant.stream_canonical_arrow(con, sql, ["data"], batch_size=5)
        with pytest.raises(ValueError, match=r"VARIANT column 'data': 2 non-null row\(s\) with a null "
                                             r"`metadata`, rows 7..8"):
            for _ in batches:
                pass
    finally:
        con.close()


def _canonical(tmp_path, null_row: bool):
    rows = [_ROW, None if null_row else _ROW, _ROW]
    table = attach_variant(pa.table({"v": pa.array(rows, _LOOSE), "x": [1, 2, 3]}), ["v"])
    return write_ipc(tmp_path / "variant.arrow.zstd", table)


def test_parquet_py_writes_required_metadata_and_value(tmp_path):
    canonical_path = _canonical(tmp_path, null_row=True)
    result = ParquetExporter().export({"slug": "variant-nullability"}, canonical_path, dest=tmp_path / "py.parquet")
    assert result.compliance.roundtrip is True, result
    assert _required(result.out_path) == {"metadata", "value"}
    assert pq.read_table(result.out_path).column("v").to_pylist() == [_ROW, None, _ROW]


@pytest.mark.parametrize("cell", ["parquet@rs", "parquet@java", "parquet@hardwood"])
def test_sidecar_parquet_writers_write_required_metadata(tmp_path, cell):
    binary = sidecar(cell)
    # Hardwood cannot write a null VARIANT row (test_parquet_variant_sidecars); the others get one.
    canonical_path = _canonical(tmp_path, null_row=cell != "parquet@hardwood")
    artifact, report = tmp_path / "out.parquet", tmp_path / "out.json"
    proc = subprocess.run([binary, "--input", str(canonical_path), "--output", str(artifact),
                           "--report", str(report)], capture_output=True, text=True, timeout=300)
    assert proc.returncode == 0, proc.stderr
    verdict = json.loads(report.read_text())
    assert verdict["roundtrip"] is True and verdict["variant_faithful"] is True, verdict
    assert _required(artifact) == {"metadata", "value"}
