# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit test for the DuckDB -> canonical-Arrow VARIANT bridge
(`duckdb_variant.variant_columns` / `to_canonical_arrow`).

Builds a factbook-shaped `t(id INTEGER, data VARIANT)` table in DuckDB (opened
through `raincloud.duckdb_connect`), bridges it to Arrow, and asserts the `data`
column becomes a `struct<metadata, value, ...>` carrying `VARIANT_EXT` so
`discovery._is_variant_field` recognizes it. A hermetic `RAINCLOUD_HOME`
monkeypatch (per test_canonical.py) then proves the marker survives
`canonical.write_canonical`'s `.arrow.zstd` IPC round-trip.
"""
from __future__ import annotations

import pyarrow as pa

from raincloud import duckdb_connect
from raincloud.pipeline import canonical, discovery, duckdb_variant
from raincloud.pipeline.spec import output_format_dir
from raincloud.pipeline.variant import VARIANT_EXT


def _make_table(con):
    con.execute("CREATE TABLE t (id INTEGER, data VARIANT)")
    con.executemany(
        "INSERT INTO t VALUES (?, CAST(CAST(? AS JSON) AS VARIANT))",
        [(1, '{"a": 1, "b": "x"}'), (2, "[1, 2, 3]")],
    )


def test_variant_columns_detects_only_variant():
    con = duckdb_connect()
    try:
        _make_table(con)
        assert duckdb_variant.variant_columns(con, "t") == ["data"]
    finally:
        con.close()


def test_to_canonical_arrow_bridges_and_marks_variant():
    con = duckdb_connect()
    try:
        _make_table(con)
        tbl = duckdb_variant.to_canonical_arrow(con, "t")
    finally:
        con.close()

    # Column order preserved.
    assert tbl.column_names == ["id", "data"]

    # `data` is bridged to the shredded Parquet-variant struct.
    data_field = tbl.schema.field("data")
    assert pa.types.is_struct(data_field.type)
    child_names = {data_field.type.field(i).name for i in range(data_field.type.num_fields)}
    assert {"metadata", "value"} <= child_names

    # VARIANT_EXT stamped on `data` only; discovery recognizes it there.
    assert data_field.metadata == VARIANT_EXT
    assert (tbl.schema.field("id").metadata or {}) == {}
    assert discovery._is_variant_field(data_field) is True
    assert discovery._is_variant_field(tbl.schema.field("id")) is False

    # Scalar `id` values survive the bridge.
    assert tbl.column("id").to_pylist() == [1, 2]


def test_order_and_multi_variant_preserved():
    con = duckdb_connect()
    try:
        con.execute("CREATE TABLE m (a INTEGER, v1 VARIANT, b VARCHAR, v2 VARIANT)")
        con.execute(
            "INSERT INTO m VALUES (1, CAST(CAST('{\"x\": 1}' AS JSON) AS VARIANT), 'hi', CAST(CAST('[1, 2]' AS JSON) AS VARIANT))"
        )
        assert duckdb_variant.variant_columns(con, "m") == ["v1", "v2"]
        tbl = duckdb_variant.to_canonical_arrow(con, "m")
    finally:
        con.close()

    # Column order preserved with a VARIANT column NOT last and multiple VARIANTs.
    assert tbl.column_names == ["a", "v1", "b", "v2"]
    # Both VARIANT columns bridged to struct + stamped.
    for vc in ("v1", "v2"):
        f = tbl.schema.field(vc)
        assert pa.types.is_struct(f.type)
        assert f.metadata == VARIANT_EXT
        assert discovery._is_variant_field(f) is True
    # Non-VARIANT columns untouched.
    assert (tbl.schema.field("a").metadata or {}) == {}
    assert (tbl.schema.field("b").metadata or {}) == {}


def test_canonical_roundtrip_preserves_variant_marker(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    con = duckdb_connect()
    try:
        _make_table(con)
        tbl = duckdb_variant.to_canonical_arrow(con, "t")
    finally:
        con.close()

    slug = "test-duckdb-variant"
    out_paths = canonical.write_canonical({"slug": slug}, [(slug, tbl)])

    dest = output_format_dir(slug, "arrow") / f"{slug}.arrow.zstd"
    assert out_paths == [dest]
    assert dest.exists() and dest.stat().st_size > 0

    with pa.ipc.open_file(str(dest)) as reader:
        got = reader.read_all()

    got_data = got.schema.field("data")
    assert got_data.metadata == VARIANT_EXT
    assert discovery._is_variant_field(got_data) is True
    assert got.column("id").to_pylist() == [1, 2]


# --------------------------------------------------------------------------
# stream_canonical_arrow — the memory-bounded streaming half of the bridge.
# Its VARIANT columns are ALREADY projected through variant_to_parquet_variant
# in the SQL; the helper stamps VARIANT_EXT on the reader schema and re-emits
# every batch under it. These tests drive a real DuckDB relation through the
# streaming bridge into `open_canonical_writer` with batch_size < row count, so
# it yields multiple batches, then reopen the canonical Arrow to assert the
# markers + all rows survive.
# --------------------------------------------------------------------------


def _read_canonical(slug: str):
    dest = output_format_dir(slug, "arrow") / f"{slug}.arrow.zstd"
    assert dest.exists() and dest.stat().st_size > 0
    with pa.ipc.open_file(str(dest)) as reader:
        return reader.read_all()


def test_stream_canonical_arrow_single_variant(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    con = duckdb_connect()
    try:
        con.execute("CREATE TABLE t (id INTEGER, data VARIANT)")
        con.executemany(
            "INSERT INTO t VALUES (?, CAST(CAST(? AS JSON) AS VARIANT))",
            [(i, '{"a": %d}' % i) for i in range(500)],
        )
        sql = "SELECT id, variant_to_parquet_variant(data) AS data FROM t"
        schema, batches = duckdb_variant.stream_canonical_arrow(
            con, sql, ["data"], batch_size=100
        )
        # The stamp is applied to the returned schema before any batch is read.
        assert schema.field("data").metadata == VARIANT_EXT
        slug = "test-stream-1v"
        n_batches = 0
        with canonical.open_canonical_writer(slug, schema) as w:
            for b in batches:
                w.write_batch(b)
                n_batches += 1
    finally:
        con.close()

    # batch_size (100) < row count (500) → the bridge streamed multiple batches.
    assert n_batches >= 2

    got = _read_canonical(slug)
    assert got.column_names == ["id", "data"]
    assert got.num_rows == 500
    data_field = got.schema.field("data")
    assert pa.types.is_struct(data_field.type)
    assert data_field.metadata == VARIANT_EXT
    assert discovery._is_variant_field(data_field) is True
    # non-variant column values intact through the stream + IPC round-trip
    assert got.column("id").to_pylist() == list(range(500))
    assert (got.schema.field("id").metadata or {}) == {}


def test_stream_canonical_arrow_two_variants(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    con = duckdb_connect()
    try:
        con.execute("CREATE TABLE m (a INTEGER, v1 VARIANT, b VARCHAR, v2 VARIANT)")
        con.executemany(
            "INSERT INTO m VALUES (?, CAST(CAST(? AS JSON) AS VARIANT), ?, CAST(CAST(? AS JSON) AS VARIANT))",
            [(i, '{"x": %d}' % i, "row%d" % i, "[%d]" % i) for i in range(300)],
        )
        sql = (
            "SELECT a, variant_to_parquet_variant(v1) AS v1, b, "
            "variant_to_parquet_variant(v2) AS v2 FROM m"
        )
        schema, batches = duckdb_variant.stream_canonical_arrow(
            con, sql, ["v1", "v2"], batch_size=100
        )
        slug = "test-stream-2v"
        n_batches = 0
        with canonical.open_canonical_writer(slug, schema) as w:
            for b in batches:
                w.write_batch(b)
                n_batches += 1
    finally:
        con.close()

    assert n_batches >= 2

    got = _read_canonical(slug)
    # Column order preserved with VARIANTs NOT last and interleaved.
    assert got.column_names == ["a", "v1", "b", "v2"]
    assert got.num_rows == 300
    for vc in ("v1", "v2"):
        f = got.schema.field(vc)
        assert pa.types.is_struct(f.type)
        assert f.metadata == VARIANT_EXT
        assert discovery._is_variant_field(f) is True
    # Non-variant columns untouched (metadata) and value-preserving.
    assert (got.schema.field("a").metadata or {}) == {}
    assert (got.schema.field("b").metadata or {}) == {}
    assert got.column("a").to_pylist() == list(range(300))
    assert got.column("b").to_pylist() == ["row%d" % i for i in range(300)]


def test_stream_canonical_arrow_rejects_non_struct_variant_col():
    """Defensive guard: naming a non-struct column as a variant col (i.e. NOT
    projected through variant_to_parquet_variant) raises ValueError rather than
    silently mis-stamping a scalar."""
    import pytest

    con = duckdb_connect()
    try:
        con.execute("CREATE TABLE t (id INTEGER, data VARIANT)")
        con.execute("INSERT INTO t VALUES (1, CAST(CAST('{\"a\": 1}' AS JSON) AS VARIANT))")
        # `id` is an int32, not a shredded struct.
        sql = "SELECT id, variant_to_parquet_variant(data) AS data FROM t"
        with pytest.raises(ValueError):
            duckdb_variant.stream_canonical_arrow(con, sql, ["id"], batch_size=100)
    finally:
        con.close()
