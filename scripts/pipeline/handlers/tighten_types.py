# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Apply the standard type-tightening pass:

    - integer width narrowing from min/max (int64 → tightest signed/unsigned)
    - binary → string re-annotation when the column's bytes are valid UTF-8

Binary-column re-annotation handles upstream parquets that were written as
`BYTE_ARRAY` with no logical-type annotation (DuckDB and some ClickHouse
exports do this for VARCHAR). pyarrow loads those as `binary`; if the bytes
are valid UTF-8 we cast back to `string` so downstream tools render the
column correctly and stats land as min/max strings rather than byte ranges.

This is a reasonable default for simple tabular datasets. For datasets with
known manual overrides (GloVe split, OSM GeoParquet, VARIANT columns, etc.),
use a dedicated handler instead.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

_UTF8_SAMPLE_SIZE = 4096


def tighten_types(spec: dict, parsed: list[tuple[Path, pa.Table | None]]) -> list[tuple[str, pa.Table]]:
    if len(parsed) != 1:
        # Concatenate tables from multiple files. Resolve column-type drift
        # (e.g. one file has int64, another has double for the same column —
        # common in Kaggle bundles split per city / per month) by unifying
        # schemas with permissive promotion and casting each table to the
        # unified schema before concat.
        tables = [t for _, t in parsed if t is not None]
        if not tables:
            raise ValueError("tighten_types: no parsed tables")
        unified = pa.unify_schemas([t.schema for t in tables], promote_options="permissive")
        unified_names = [f.name for f in unified]
        # select() reorders columns by name to match the unified schema (cast()
        # matches by index, so without this step parquets with the same columns
        # in different orders — common across HF dataset splits — would fail).
        tables = [t.select(unified_names).cast(unified) for t in tables]
        table = pa.concat_tables(tables)
    else:
        _, table = parsed[0]
        if table is None:
            raise ValueError("tighten_types requires an already-parsed Table")

    new_cols = []
    new_fields = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        ty = field.type

        if pa.types.is_integer(ty) and col.null_count < len(col):
            try:
                mn = pc.min(col).as_py()
                mx = pc.max(col).as_py()
                if mn is not None and mx is not None:
                    narrow = _pick_integer_type(mn, mx)
                    if narrow is not None and narrow != ty:
                        col = col.cast(narrow)
            except pa.ArrowInvalid:
                pass

        elif pa.types.is_binary(ty) or pa.types.is_large_binary(ty):
            if _is_utf8_binary(col):
                target = pa.large_string() if pa.types.is_large_binary(ty) else pa.string()
                col = col.cast(target)

        new_cols.append(col)
        new_fields.append(pa.field(field.name, col.type, nullable=field.nullable))

    table = pa.Table.from_arrays(new_cols, schema=pa.schema(new_fields))
    return [(spec["slug"], table)]


def _pick_integer_type(mn: int, mx: int) -> pa.DataType | None:
    if mn >= 0:
        if mx <= 0xFF: return pa.uint8()
        if mx <= 0xFFFF: return pa.uint16()
        if mx <= 0xFFFFFFFF: return pa.uint32()
        return pa.uint64()
    if -128 <= mn and mx <= 127: return pa.int8()
    if -32768 <= mn and mx <= 32767: return pa.int16()
    if -2**31 <= mn and mx <= 2**31 - 1: return pa.int32()
    return pa.int64()


def _is_utf8_binary(col: pa.ChunkedArray) -> bool:
    """Return True iff a sample of non-null values from `col` all decode as UTF-8."""
    seen = 0
    for chunk in col.chunks:
        if chunk.null_count == len(chunk):
            continue
        for i in range(len(chunk)):
            if seen >= _UTF8_SAMPLE_SIZE:
                return True
            v = chunk[i]
            if not v.is_valid:
                continue
            try:
                v.as_py().decode("utf-8")
            except UnicodeDecodeError:
                return False
            seen += 1
    return seen > 0
