# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the convert stage's Parquet -> Vortex writer.

Everything runs under tmp_path; nothing touches the configured outputs tree.
"""
from __future__ import annotations

import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from raincloud.pipeline.convert import _convert_one


def _write_tiny_parquet(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"id": pa.array([1, 2, 3], type=pa.int32())})
    pq.write_table(table, path, compression="zstd")


def test_convert_one_writes_vortex(tmp_path: Path):
    parquet, vortex = tmp_path / "t.parquet", tmp_path / "t.vortex"
    _write_tiny_parquet(parquet)
    assert _convert_one(parquet, vortex, "t") == vortex
    assert vortex.stat().st_size > 0


def test_convert_one_is_idempotent_when_vortex_newer(tmp_path: Path):
    """Re-running on an up-to-date pair is a cached no-op (same mtime)."""
    parquet, vortex = tmp_path / "t.parquet", tmp_path / "t.vortex"
    _write_tiny_parquet(parquet)
    _convert_one(parquet, vortex, "t")
    first_mtime = vortex.stat().st_mtime
    time.sleep(0.01)
    _convert_one(parquet, vortex, "t")
    assert vortex.stat().st_mtime == first_mtime


def test_convert_one_streams_multi_row_group_nested(tmp_path: Path):
    """A multi-row-group parquet with a nested column converts end-to-end.

    Regression for the `pf.read()` path that fails on nested columns whose
    Arrow representation would need to be chunked across the row groups —
    pyarrow raises `ArrowNotImplementedError: Nested data conversions not
    implemented for chunked array outputs` from `read_all`. Streaming
    `iter_batches` produces single-chunk RecordBatches and sidesteps it.
    """
    parquet = tmp_path / "nested.parquet"
    vortex_path = tmp_path / "nested.vortex"

    schema = pa.schema([
        ("msgs", pa.list_(pa.struct([("role", pa.string()), ("content", pa.string())]))),
    ])
    rows = [[{"role": "u", "content": "hi"}], [{"role": "a", "content": "hello"}]]
    batch = pa.record_batch([pa.array(rows, type=schema.field(0).type)], schema=schema)

    with pq.ParquetWriter(parquet, schema) as w:
        w.write_batch(batch)
        w.write_batch(batch)
        w.write_batch(batch)
    assert pq.ParquetFile(parquet).num_row_groups == 3

    out = _convert_one(parquet, vortex_path, "test-nested")
    assert out == vortex_path
    assert vortex_path.exists()
    assert vortex_path.stat().st_size > 0


def test_convert_one_renames_duplicate_columns(tmp_path: Path):
    """Top-level duplicate column names get suffixed before write.

    Vortex's StructLayout rejects duplicates; raincloud needs to rename
    them in-stream rather than via a `pa.Table.rename_columns` call on
    the materialised table.
    """
    import vortex as vx
    parquet = tmp_path / "dup.parquet"
    vortex_path = tmp_path / "dup.vortex"

    schema = pa.schema([("x", pa.int64()), ("x", pa.int64()), ("x", pa.int64())])
    batch = pa.record_batch([pa.array([1, 2]), pa.array([3, 4]), pa.array([5, 6])], schema=schema)
    with pq.ParquetWriter(parquet, schema) as w:
        w.write_batch(batch)

    _convert_one(parquet, vortex_path, "test-dup")
    assert vx.open(str(vortex_path)).dtype.names() == ["x", "x [1]", "x [2]"]


def test_convert_one_rebuilds_when_parquet_newer(tmp_path: Path):
    """A parquet newer than its vortex is picked up and rewritten."""
    import os
    parquet, vortex = tmp_path / "t.parquet", tmp_path / "t.vortex"
    _write_tiny_parquet(parquet)
    _convert_one(parquet, vortex, "t")
    first_mtime = vortex.stat().st_mtime
    future = time.time() + 10
    os.utime(parquet, (future, future))
    _convert_one(parquet, vortex, "t")
    assert vortex.stat().st_mtime != first_mtime
