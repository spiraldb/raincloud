# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""DuckDB -> canonical-Arrow bridge for VARIANT columns.

DuckDB (1.5.x) cannot export a raw `VARIANT` column to Arrow -- `.arrow()` /
`to_arrow_table()` raise "Unsupported Arrow type VARIANT". Its
`variant_to_parquet_variant(<col>)` scalar function instead yields the
Parquet-variant shredded `struct<metadata: binary, value: binary>` (plus a
`typed_value` child when shredded), which Arrow does export. This module wraps
each VARIANT column in that call, materializes the relation as an Arrow Table,
and stamps `arrow.parquet.variant` on the former VARIANT columns via
`variant.attach_variant` so `discovery._is_variant_field` recognizes them and
they survive `canonical.write_canonical`'s IPC round-trip. The stamp also
declares the struct's `metadata` (and unshredded `value`) non-nullable, as the
VARIANT specs require, and checks every row against that (see `variant`):
DuckDB exports every field nullable.

Use `to_canonical_arrow` for a relation small enough to materialize. Large
handlers use `stream_canonical_arrow` to emit bounded batches through the same
VARIANT representation and metadata contract.
"""
from __future__ import annotations

from typing import Iterator

import pyarrow as pa

from .variant import attach_variant, attach_variant_schema, conform_variant_batch


def _quote(ident: str) -> str:
    """Quote a DuckDB identifier (double-quote, doubling embedded quotes)."""
    return '"' + ident.replace('"', '""') + '"'


def _describe(con, relation: str) -> list[tuple[str, str]]:
    """Return `(column_name, type_string)` pairs for `relation`, in order."""
    rows = con.execute(f"DESCRIBE {relation}").fetchall()
    # DESCRIBE columns: (column_name, column_type, null, key, default, extra).
    return [(r[0], r[1]) for r in rows]


def variant_columns(con, relation: str) -> list[str]:
    """Names of VARIANT-typed columns of `relation`, in declaration order.

    Matches only top-level scalar VARIANT columns; a nested VARIANT (`VARIANT[]`,
    `STRUCT(... VARIANT)`) is not detected here and would fail *loudly* at Arrow
    export (`Unsupported Arrow type VARIANT`) rather than silently mis-handle —
    acceptable for the top-level-VARIANT handlers this bridge serves.
    """
    return _variant_names(_describe(con, relation))


def _variant_names(described: list[tuple[str, str]]) -> list[str]:
    return [name for name, typ in described if typ.upper() == "VARIANT"]


def to_canonical_arrow(con, relation: str) -> pa.Table:
    """Materialize `relation` as an Arrow Table with VARIANT columns bridged.

    Each VARIANT column is projected through `variant_to_parquet_variant(...)`
    (so Arrow can export it as `struct<metadata, value[, typed_value]>`);
    non-VARIANT columns pass through unchanged and column order is preserved.
    The former VARIANT columns are then stamped with `VARIANT_EXT` field
    metadata and their spec storage nullability (`variant.attach_variant`).
    Raises `ValueError` if a bridged column does not come back as a VARIANT
    storage struct (defensive -- guards against a DuckDB behaviour change), or
    if a non-null VARIANT row has a null `metadata` / unshredded `value`.

    `relation` is interpolated raw into the query (so a qualified `schema.table`
    or a subquery works, unlike the `_quote`d column identifiers); it must be a
    trusted, caller-controlled identifier -- never external/untrusted input.
    """
    described = _describe(con, relation)
    variant_cols = _variant_names(described)

    projections = [
        f"variant_to_parquet_variant({_quote(name)}) AS {_quote(name)}"
        if name in variant_cols else _quote(name)
        for name, _ in described
    ]
    sql = f"SELECT {', '.join(projections)} FROM {relation}"
    table = con.execute(sql).to_arrow_table()

    for name in variant_cols:
        field = table.schema.field(name)
        if not pa.types.is_struct(field.type):
            raise ValueError(
                f"bridged VARIANT column {name!r} came back as {field.type}, "
                "expected a struct<metadata, value, ...>"
            )
    return attach_variant(table, variant_cols)


def stream_canonical_arrow(
    con, sql: str, variant_cols: list[str], batch_size: int = 100_000
) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
    """Stream `sql` as canonical-Arrow batches with VARIANT columns bridged.

    Unlike `to_canonical_arrow` (which materializes the whole relation), this is
    the memory-bounded streaming half of the bridge: `sql` is executed and read
    through DuckDB's `to_arrow_reader(batch_size)`, so the working set is capped
    at one batch. `sql`'s `variant_cols` are expected to be ALREADY projected
    through `variant_to_parquet_variant(...)` by the caller (each yielding the
    shredded `struct<metadata: binary, value: binary[, typed_value]>` Arrow can
    export); like `to_canonical_arrow`, a nested VARIANT that DuckDB refuses to
    export would fail *loudly* at `to_arrow_reader` rather than silently
    mis-handle — acceptable for the top-level-VARIANT handlers this serves.

    Returns `(target_schema, batch_iter)` where `target_schema` stamps
    `VARIANT_EXT` (via `attach_variant_schema`) on each of `variant_cols`, with
    the storage struct's `metadata` (and unshredded `value`) declared
    non-nullable as the VARIANT specs require, and each yielded `RecordBatch`
    is checked and re-emitted under it (`variant.conform_variant_batch`): the
    same buffers, so only the declared schema differs from what DuckDB streamed.
    A non-null VARIANT row with a null required child raises `ValueError`
    naming the column and rows (numbered from the stream's first row) when its
    batch is reached.

    `sql` is interpolated raw into `con.execute` — it must be a trusted,
    caller-built query, never external/untrusted input.
    """
    reader = con.execute(sql).to_arrow_reader(batch_size)
    for name in variant_cols:
        field = reader.schema.field(name)
        if not pa.types.is_struct(field.type):
            raise ValueError(
                f"bridged VARIANT column {name!r} came back as {field.type}, "
                "expected a struct<metadata, value, ...>"
            )
    target = attach_variant_schema(reader.schema, variant_cols)

    def gen() -> Iterator[pa.RecordBatch]:
        row = 0
        for b in reader:
            yield conform_variant_batch(b, target, variant_cols, row)
            row += b.num_rows

    return target, gen()
