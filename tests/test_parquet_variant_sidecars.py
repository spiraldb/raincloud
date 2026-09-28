# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""VARIANT in the three sidecar Parquet writers, measured rather than assumed.

Each writer's `variant_faithful` is true iff the file declares Parquet's VARIANT logical
type on the column AND the lane reads it back as the `arrow.parquet.variant` extension.
parquet@rs gets the logical type from arrow-rs's `variant_experimental` feature,
parquet@java from parquet-arrow-java, parquet@hardwood from Hardwood's schema elements;
Hardwood cannot write a null VARIANT row, and says so. Every Parquet reader is then run
over each writer's file. Each test skips when a binary it needs is not installed.
"""
from __future__ import annotations

import json
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.export.readers import PyarrowParquetReader
from raincloud.pipeline.variant import attach_variant_schema
from tests._helpers import sidecar, write_ipc

# The sidecar Parquet lanes: each has a writer and a reader.
_WRITERS = _READERS = ("parquet@rs", "parquet@java", "parquet@hardwood")


def _variant_table(null_row=False):
    storage = pa.struct([("metadata", pa.binary()), ("value", pa.binary())])
    # Variant int8 1 and 2 (metadata: version 1, empty dictionary).
    rows = [{"metadata": b"\x01\x00\x00", "value": b"\x0c\x01"},
            None if null_row else {"metadata": b"\x01\x00\x00", "value": b"\x0c\x02"}]
    table = pa.table({"v": pa.array(rows, storage), "x": pa.array([1, 2], pa.int64())})
    return table.cast(attach_variant_schema(table.schema, ["v"]))


def _canonical(tmp_path, table, name="canonical"):
    return write_ipc(tmp_path / f"{name}.arrow.zstd", table)


def _write(cell, tmp_path, canonical):
    artifact, report = tmp_path / f"{cell.split('@')[1]}.parquet", tmp_path / f"{cell.split('@')[1]}.json"
    proc = subprocess.run([sidecar(cell), "--input", str(canonical), "--output", str(artifact),
                           "--report", str(report)], capture_output=True, text=True, timeout=300)
    assert proc.returncode == 0, proc.stderr
    return artifact, json.loads(report.read_text())


def _read(cell, tmp_path, artifact, canonical):
    report = tmp_path / f"{artifact.stem}.read-{cell.split('@')[1]}.json"
    proc = subprocess.run([sidecar(cell, "read"), "--input", str(artifact), "--canonical", str(canonical),
                           "--report", str(report)], capture_output=True, text=True, timeout=300)
    assert proc.returncode == 0, proc.stderr
    return json.loads(report.read_text())


def _declared_variant(artifact):
    schema = str(pq.ParquetFile(artifact).schema)
    return [line.strip().split()[3] for line in schema.splitlines() if "(Variant(" in line]


@pytest.mark.parametrize("cell", list(_WRITERS))
def test_each_writer_keeps_a_variant_column_as_parquet_variant(tmp_path, cell):
    canonical = _canonical(tmp_path, _variant_table())
    artifact, verdict = _write(cell, tmp_path, canonical)
    assert verdict["roundtrip"] is True and verdict["variant_faithful"] is True, verdict
    assert "VARIANT kept (v)" in verdict["note"], verdict
    assert _declared_variant(artifact) == ["v"]
    assert PyarrowParquetReader().read_conformance(artifact, canonical).status == "pass"


@pytest.mark.parametrize("writer", list(_WRITERS))
def test_every_parquet_reader_reads_each_writers_variant_file(tmp_path, writer):
    canonical = _canonical(tmp_path, _variant_table())
    artifact, verdict = _write(writer, tmp_path, canonical)
    assert verdict["roundtrip"] is True, verdict
    for reader in _READERS:
        got = _read(reader, tmp_path, artifact, canonical)
        assert got["status"] == "pass", (writer, reader, got)


@pytest.mark.parametrize("cell", ["parquet@rs", "parquet@java"])
def test_a_null_variant_row_is_kept_where_the_library_can_write_it(tmp_path, cell):
    canonical = _canonical(tmp_path, _variant_table(null_row=True))
    artifact, verdict = _write(cell, tmp_path, canonical)
    assert verdict["roundtrip"] is True and verdict["variant_faithful"] is True, verdict
    assert pq.read_table(artifact).column("v").null_count == 1


def test_hardwood_refuses_a_null_variant_row_and_leaves_no_file(tmp_path):
    # Hardwood 1.1.0.Beta1's ColumnBatch.struct takes no validity for a VARIANT group: its
    # limit, recorded, never written as a plain struct instead.
    canonical = _canonical(tmp_path, _variant_table(null_row=True))
    artifact, verdict = _write("parquet@hardwood", tmp_path, canonical)
    assert verdict["roundtrip"] is False and verdict["variant_faithful"] is False, verdict
    assert "a null VARIANT row, which hardwood version " in verdict["note"], verdict
    assert not artifact.exists()


@pytest.mark.parametrize("cell", list(_WRITERS))
def test_the_marker_alone_is_measured_as_not_kept(tmp_path, cell):
    # raincloud's marker without the arrow.parquet.variant extension: no writer declares
    # VARIANT, and the report says what is missing rather than a fixed sentence.
    table = pa.table({"v": pa.array([1, 2], pa.int64())})
    table = table.cast(pa.schema([pa.field("v", pa.int64(), metadata={b"__variant_type": b"1"})]))
    canonical = _canonical(tmp_path, table)
    _, verdict = _write(cell, tmp_path, canonical)
    assert verdict["roundtrip"] is True and verdict["variant_faithful"] is False, verdict
    assert 'VARIANT not kept: column "v": the file declares no Parquet VARIANT logical type' in verdict["note"]
    assert "read back without the arrow.parquet.variant extension" in verdict["note"]


def test_a_canonical_without_variant_is_vacuously_faithful(tmp_path):
    canonical = _canonical(tmp_path, pa.table({"x": pa.array([1, 2], pa.int64())}))
    for cell in _WRITERS:
        _, verdict = _write(cell, tmp_path, canonical)
        assert verdict["roundtrip"] is True and verdict["variant_faithful"] is True, (cell, verdict)
        assert verdict["note"] == f"{cell}: round-trips to canonical", verdict
