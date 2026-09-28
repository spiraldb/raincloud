# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Mark Arrow columns as the canonical Parquet VARIANT extension type.

VARIANT identity is carried as field-level `custom_metadata` rather than a
`pa.ExtensionType` subclass (pyarrow 24 ships no native `arrow.parquet.variant`
type). Metadata rides in the uncompressed schema FlatBuffer of an Arrow IPC
message, so it survives zstd-compressed IPC round-trips byte-for-byte, and the
`__variant_type` key it stamps is exactly what `discovery._is_variant_field`
already recognizes.

Stamping the marker is a claim that the column is a spec VARIANT, so the stamp
also declares the storage struct's nullability the specs require. DuckDB's
Arrow export declares every field nullable (its types carry no nullability),
which is loose, not wrong; the claim is ours:

- Arrow canonical extension `arrow.parquet.variant` (CanonicalExtensions.rst,
  "Parquet Variant"): the storage has "a *non-nullable* field named
  ``metadata``".
- Parquet VariantEncoding.md, "Variant in Parquet" (LogicalTypes.md, VARIANT,
  says the same): "The `metadata` field is `required`"; "The `value` field must
  be annotated as `required` for unshredded Variant values, or `optional` if
  parts of the value are shredded as typed Parquet columns."

So `metadata` is non-nullable, `value` is non-nullable when there is no
`typed_value` and keeps its nullability when there is; a missing VARIANT is a
null struct. Parquet writers map these to `required binary` children of the
VARIANT group. Data is checked, never repaired: a present VARIANT with a null
`metadata` (or unshredded `value`) fails, naming the column and rows.
"""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from .discovery import VARIANT_EXT  # the marker is declared beside its reader


def _is_binary(dtype: pa.DataType) -> bool:
    return (pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype)
            or pa.types.is_binary_view(dtype))


def _is_nested(dtype: pa.DataType) -> bool:
    return (pa.types.is_struct(dtype) or pa.types.is_list(dtype) or pa.types.is_large_list(dtype)
            or pa.types.is_list_view(dtype) or pa.types.is_large_list_view(dtype))


def variant_storage_type(name: str, dtype: pa.DataType) -> pa.StructType:
    """`dtype` with the nullability the VARIANT specs require (module docstring).

    Raises `ValueError` for a type that is not a VARIANT storage struct, and for
    a shredded `typed_value` that is itself a struct or list: the Arrow extension
    then requires its fields / elements non-nullable too, which raincloud does
    not yet declare or check (no handler produces one), so it fails loudly
    rather than stamping an off-spec column.
    """
    if not pa.types.is_struct(dtype):
        raise ValueError(f"VARIANT column {name!r} is {dtype}, expected a struct<metadata, value, ...>")
    names = [f.name for f in dtype]
    if ("metadata" not in names or not {"value", "typed_value"} & set(names)
            or not set(names) <= {"metadata", "value", "typed_value"} or len(set(names)) != len(names)):
        raise ValueError(f"VARIANT column {name!r} is {dtype}: the storage struct holds `metadata` "
                         "and one or both of `value` / `typed_value`, and nothing else")
    shredded = "typed_value" in names
    fields = []
    for field in dtype:
        if field.name in ("metadata", "value") and not _is_binary(field.type):
            raise ValueError(f"VARIANT column {name!r}: `{field.name}` is {field.type}, expected binary")
        if field.name == "typed_value" and _is_nested(field.type):
            raise ValueError(f"VARIANT column {name!r}: a shredded `typed_value` of {field.type} is not "
                             "supported (its fields / elements would have to be declared non-nullable)")
        if field.name == "metadata" or (field.name == "value" and not shredded):
            field = field.with_nullable(False)
        fields.append(field)
    return pa.struct(fields)


def attach_variant_schema(schema: pa.Schema, cols: list[str]) -> pa.Schema:
    """Return a copy of `schema` with `VARIANT_EXT` merged into each named field
    and its storage struct declared as the specs require (`variant_storage_type`).

    Existing field metadata is preserved (VARIANT_EXT keys win on conflict);
    fields not named in `cols` are left untouched. Raises `KeyError` if a name
    is not a field of `schema`, or names more than one, and `ValueError` if the
    field is not a VARIANT storage struct.

    This is the schema-level half of `attach_variant`; the streaming VARIANT
    bridge (`duckdb_variant.stream_canonical_arrow`) stamps the marker once on a
    reader's schema and re-emits every batch under it (`conform_variant_batch`),
    without ever holding a whole Table.
    """
    for name in cols:
        found = schema.get_all_field_indices(name)
        if not found:
            raise KeyError(f"column {name!r} not found in table schema")
        if len(found) > 1:
            raise KeyError(f"column {name!r} is ambiguous: {len(found)} columns share the name")
        idx = found[0]
        field = schema.field(idx)
        merged = dict(field.metadata or {})
        merged.update(VARIANT_EXT)
        field = field.with_type(variant_storage_type(name, field.type))
        schema = schema.set(idx, field.with_metadata(merged))
    return schema


def _conform_variant_array(name: str, array: pa.StructArray, dtype: pa.StructType,
                           first_row: int) -> pa.StructArray:
    """`array` re-typed as `dtype`, after checking that every child `dtype`
    declares non-nullable is present on every non-null row.

    A null under a null VARIANT row is a don't-care slot (DuckDB nulls the
    children of a NULL struct); its validity is dropped, zero-copy, so the child
    holds no nulls at all. `first_row` numbers `array`'s first row in the error.
    """
    valid = array.is_valid()
    children = []
    for i, field in enumerate(dtype):
        child = array.field(i)
        if not field.nullable and child.null_count:
            bad = pc.and_(child.is_null(), valid)
            count = pc.sum(bad).as_py() or 0
            if count:
                rows = pc.indices_nonzero(bad)
                lo, hi = first_row + rows[0].as_py(), first_row + rows[-1].as_py()
                raise ValueError(
                    f"VARIANT column {name!r}: {count:,} non-null row(s) with a null "
                    f"`{field.name}`, rows {lo:,}..{hi:,}. A present Parquet VARIANT requires "
                    f"`{field.name}`; this is malformed VARIANT data, not a null row")
            child = pa.Array.from_buffers(child.type, len(child), [None, *child.buffers()[1:]],
                                          null_count=0, offset=child.offset)
        children.append(child)
    mask = pc.invert(valid) if array.null_count else None
    return pa.StructArray.from_arrays(children, fields=list(dtype), mask=mask)


def conform_variant_batch(batch: pa.RecordBatch, schema: pa.Schema, cols: list[str],
                          first_row: int = 0) -> pa.RecordBatch:
    """`batch` re-emitted under `schema` (from `attach_variant_schema(batch.schema,
    cols)`), each of `cols` checked and re-typed to its spec storage struct.

    Raises `ValueError` naming the column and rows (numbered from `first_row`)
    when a non-null VARIANT row lacks a required child. Other columns pass
    through unchanged; the check is per-batch pyarrow compute, so it streams.
    """
    columns = list(batch.columns)
    for name in cols:
        idx = schema.get_field_index(name)
        columns[idx] = _conform_variant_array(name, columns[idx], schema.field(idx).type, first_row)
    return pa.RecordBatch.from_arrays(columns, schema=schema)


def attach_variant(table: pa.Table, cols: list[str]) -> pa.Table:
    """Return a copy of `table` with `VARIANT_EXT` merged into each named column
    and its storage declared and checked as the specs require.

    Existing field metadata is preserved (VARIANT_EXT keys win on conflict);
    columns not named in `cols` are left untouched. Raises `KeyError` if a name
    is not a column of `table`, and `ValueError` as `conform_variant_batch` does.

    Wraps `attach_variant_schema` + `conform_variant_batch` batch by batch, so
    the existing column buffers are re-attached, not copied.
    """
    schema = attach_variant_schema(table.schema, cols)
    batches, row = [], 0
    for batch in table.to_batches():
        batches.append(conform_variant_batch(batch, schema, cols, row))
        row += batch.num_rows
    return pa.Table.from_batches(batches, schema=schema)
