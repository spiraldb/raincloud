# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the new --columns / --coverage flags on list_datasets.

The pure helpers (_canonicalize_type, _filter_columns, _coverage_summary)
don't touch the filesystem and are tested directly. The _iter_columns path
touches the filesystem; we exercise it against a fixture parquet written
to outputs/v1/<test-slug>/parquet/ and clean up.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.pipeline.list_datasets import (
    _canonicalize_type,
    _coverage_summary,
    _filter_columns,
    _iter_columns,
)
from scripts.pipeline.spec import prepared_parquet


def test_canonicalize_type_preserves_scalar_params():
    assert _canonicalize_type("DECIMAL(10, 2)") == "DECIMAL(10, 2)"
    assert _canonicalize_type("TIMESTAMP WITH TIME ZONE") == "TIMESTAMP WITH TIME ZONE"
    assert _canonicalize_type("VARCHAR") == "VARCHAR"


def test_canonicalize_type_collapses_struct_bodies():
    assert _canonicalize_type("STRUCT(a INTEGER, b VARCHAR)") == "STRUCT(...)"
    # Nested structs collapse
    assert _canonicalize_type(
        "STRUCT(a STRUCT(x INTEGER), b VARCHAR)"
    ) == "STRUCT(...)"
    # Lists of structs collapse the struct interior, keep the list wrapper
    assert _canonicalize_type(
        "LIST(STRUCT(a INTEGER, b VARCHAR))"
    ) == "LIST(STRUCT(...))"


def test_filter_columns_grep():
    rows = [
        {"slug": "a", "column": "url", "type": "string"},
        {"slug": "b", "column": "image_url", "type": "string"},
        {"slug": "c", "column": "id", "type": "int64"},
    ]
    out = _filter_columns(rows, "url")
    assert {r["column"] for r in out} == {"url", "image_url"}
    out = _filter_columns(rows, "^url$")
    assert {r["column"] for r in out} == {"url"}
    # No filter is a passthrough
    assert _filter_columns(rows, None) == rows


def test_coverage_summary_aggregates_by_canonical_type():
    rows = [
        {"slug": "a", "column": "x", "type": "string"},
        {"slug": "a", "column": "y", "type": "string"},
        {"slug": "b", "column": "z", "type": "string"},
        {"slug": "b", "column": "n", "type": "int64"},
    ]
    cov = _coverage_summary(rows)
    by_type = {r["type"]: r for r in cov}
    assert by_type["string"]["columns"] == 3
    assert by_type["string"]["datasets"] == 2
    assert by_type["int64"]["columns"] == 1
    assert by_type["int64"]["datasets"] == 1
    # Examples are slug.column triples
    assert "a.x" in by_type["string"]["examples"]


def test_coverage_summary_collapses_struct_shapes():
    rows = [
        {"slug": "a", "column": "x", "type": "STRUCT(a INTEGER)"},
        {"slug": "a", "column": "y", "type": "STRUCT(a VARCHAR, b BIGINT)"},
    ]
    cov = _coverage_summary(rows)
    # Different struct bodies, same canonical bucket
    assert len(cov) == 1
    assert cov[0]["type"] == "STRUCT(...)"
    assert cov[0]["columns"] == 2


def test_iter_columns_reads_built_parquet():
    """End-to-end: write a tiny parquet under the canonical layout, run
    _iter_columns, confirm it surfaces."""
    slug = "test-list-datasets-cols"
    parquet = prepared_parquet(slug)
    parquet.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int32()),
        "label": pa.array(["a", "b", "c"], type=pa.string()),
    })
    pq.write_table(table, parquet, compression="zstd")
    try:
        out = _iter_columns([{"slug": slug}], source="parquet")
        names = [r["column"] for r in out]
        assert names == ["id", "label"]
        assert all(r["source"] == "parquet" for r in out)
        assert all(r["slug"] == slug for r in out)
    finally:
        parquet.unlink()
        if parquet.parent.exists() and not any(parquet.parent.iterdir()):
            parquet.parent.rmdir()
        if parquet.parent.parent.exists() and not any(parquet.parent.parent.iterdir()):
            parquet.parent.parent.rmdir()


def test_iter_columns_skips_unbuilt_slugs():
    """Slugs without a built parquet are silently skipped — column inspection
    is only meaningful for built outputs."""
    out = _iter_columns([{"slug": "no-such-slug-anywhere-abc123"}], source="parquet")
    assert out == []
