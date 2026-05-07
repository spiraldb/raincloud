# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the convert stage — specifically the hydrated path.

The base-parquet conversion path is exercised end-to-end by every build
in the test suite; here we focus on the hydrated companion: opt-in
gating, no-op when no hydrated parquet exists, idempotent caching.

Each test writes a tiny test parquet to outputs/v1/<test-slug>/parquet-hydrated/,
runs convert_hydrated, then cleans up. No real Vortex import is required
to test the gating logic; the actual Vortex round-trip is exercised
elsewhere.
"""
from __future__ import annotations

import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.pipeline.convert import _convert_one, convert_hydrated
from scripts.pipeline.spec import (
    prepared_parquet_hydrated,
    prepared_vortex_hydrated,
)


def _make_spec(slug: str, *, vortex: bool = True, hydrate: bool = True) -> dict:
    return {
        "slug": slug,
        "convert": {
            "vortex": vortex,
            "vortex_skip_reason": None if vortex else "test fixture opt-out",
        },
        "hydrate": {
            "url_column": "url",
            "output_column": "content",
            "output_type": "binary",
            "advisory": "test fixture",
        } if hydrate else None,
    }


def _cleanup(slug: str):
    for p in (prepared_parquet_hydrated(slug), prepared_vortex_hydrated(slug)):
        if p.exists():
            p.unlink()
        if p.parent.exists() and not any(p.parent.iterdir()):
            p.parent.rmdir()
    # And the slug root if empty
    slug_root = prepared_parquet_hydrated(slug).parent.parent
    if slug_root.exists() and not any(slug_root.iterdir()):
        slug_root.rmdir()


def _write_tiny_parquet(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"id": pa.array([1, 2, 3], type=pa.int32())})
    pq.write_table(table, path, compression="zstd")


def test_convert_hydrated_returns_none_when_vortex_off():
    spec = _make_spec("test-conv-noop-vortex", vortex=False)
    assert convert_hydrated(spec) is None


def test_convert_hydrated_returns_none_when_no_hydrate_block():
    spec = _make_spec("test-conv-noop-hydrate", hydrate=False)
    assert convert_hydrated(spec) is None


def test_convert_hydrated_returns_none_when_hydrated_parquet_missing():
    """No file on disk → no-op (not an error). The hydrate stage simply
    hasn't run yet for this slug."""
    spec = _make_spec("test-conv-noop-missing")
    # Make sure no stale fixture exists
    _cleanup("test-conv-noop-missing")
    try:
        assert convert_hydrated(spec) is None
    finally:
        _cleanup("test-conv-noop-missing")


def test_convert_hydrated_writes_vortex_when_inputs_present():
    slug = "test-conv-writes"
    parquet = prepared_parquet_hydrated(slug)
    vortex = prepared_vortex_hydrated(slug)
    _cleanup(slug)
    try:
        _write_tiny_parquet(parquet)
        out = convert_hydrated(_make_spec(slug))
        assert out == vortex
        assert vortex.exists()
        assert vortex.stat().st_size > 0
    finally:
        _cleanup(slug)


def test_convert_hydrated_is_idempotent_when_vortex_newer():
    """Re-running on an up-to-date pair should be a cached no-op (no rewrite,
    same mtime)."""
    slug = "test-conv-idempotent"
    parquet = prepared_parquet_hydrated(slug)
    vortex = prepared_vortex_hydrated(slug)
    _cleanup(slug)
    try:
        _write_tiny_parquet(parquet)
        out1 = convert_hydrated(_make_spec(slug))
        assert out1 == vortex
        first_mtime = vortex.stat().st_mtime

        # Wait a beat, then re-run; vortex is newer than parquet so it must
        # be untouched.
        time.sleep(0.01)
        convert_hydrated(_make_spec(slug))
        assert vortex.stat().st_mtime == first_mtime
    finally:
        _cleanup(slug)


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


def test_convert_hydrated_rebuilds_when_parquet_newer():
    """If the hydrated parquet has been re-built (mtime newer than the
    vortex), the convert path picks it up and rewrites."""
    slug = "test-conv-rebuild"
    parquet = prepared_parquet_hydrated(slug)
    vortex = prepared_vortex_hydrated(slug)
    _cleanup(slug)
    try:
        _write_tiny_parquet(parquet)
        convert_hydrated(_make_spec(slug))
        first_mtime = vortex.stat().st_mtime

        # Re-write the parquet with a fresher mtime, then re-convert.
        time.sleep(0.05)
        _write_tiny_parquet(parquet)
        # Force the parquet to be newer than the vortex.
        import os
        future = vortex.stat().st_mtime + 5
        os.utime(parquet, (future, future))

        convert_hydrated(_make_spec(slug))
        assert vortex.stat().st_mtime != first_mtime
    finally:
        _cleanup(slug)
