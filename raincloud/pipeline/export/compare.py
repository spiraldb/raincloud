# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Logical Arrow equality with lossless normalization and recursive float fidelity.

Where equality turns on representation rather than data, the rule is shared with
the Rust and JVM comparators through `sidecars/compare_cases` (pairs of files and
the verdict each must get): a union of exactly `null` and `T` is a nullable `T`;
zoned timestamps compare by instant, whatever zone labels them; an integer and
a scale-0 decimal holding the same values are equal.
"""
from __future__ import annotations

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc


def _is_list(dtype: pa.DataType) -> bool:
    return (pa.types.is_list(dtype) or pa.types.is_large_list(dtype)
            or pa.types.is_fixed_size_list(dtype) or pa.types.is_list_view(dtype)
            or pa.types.is_large_list_view(dtype))


def _nullable_member(dtype: pa.DataType) -> int | None:
    """The index of `T` in a union of exactly `null` and `T`, else None: such a
    union is how some readers spell a nullable `T` (Avro's ["null", T])."""
    if pa.types.is_union(dtype) and dtype.num_fields == 2:
        nulls = [pa.types.is_null(field.type) for field in dtype]
        if nulls.count(True) == 1:
            return nulls.index(False)
    return None


def _logical_type(dtype: pa.DataType) -> pa.DataType:
    # Extension annotations, dictionary indices and a null|T union are physical
    # representations.
    while True:
        if isinstance(dtype, pa.BaseExtensionType):
            dtype = dtype.storage_type
        elif pa.types.is_dictionary(dtype):
            dtype = dtype.value_type
        elif (member := _nullable_member(dtype)) is not None:
            dtype = dtype.field(member).type
        else:
            return dtype


def _has_nullable_union(dtype: pa.DataType) -> bool:
    if _nullable_member(dtype) is not None:
        return True
    return any(_has_nullable_union(dtype.field(i).type) for i in range(dtype.num_fields))


def _without_nullable_unions(array: pa.Array) -> pa.Array:
    """`array` with every union of `null` and `T` replaced by the nullable `T`
    it spells, at any depth of struct, list and map."""
    dtype = array.type
    if not _has_nullable_union(dtype):
        return array
    member = _nullable_member(dtype)
    if member is not None:
        selected = pc.equal(array.type_codes, pa.scalar(dtype.type_codes[member], pa.int8()))
        positions = (array.offsets if dtype.mode == "dense"
                     else pa.array(np.arange(len(array), dtype=np.int32)))
        # A sparse member comes sliced with its union; a dense one is indexed by the offsets.
        picked = array.field(member).take(pc.if_else(selected, positions, pa.scalar(None, positions.type)))
        return _without_nullable_unions(picked)
    validity = array.is_null() if array.null_count else None
    if pa.types.is_struct(dtype):
        children = [_without_nullable_unions(child) for child in array.flatten()]
        return pa.StructArray.from_arrays(children, fields=[f.with_type(c.type) for f, c in zip(dtype, children)],
                                          mask=validity)
    if pa.types.is_map(dtype):
        return pa.MapArray.from_arrays(array.offsets, _without_nullable_unions(array.keys),
                                       _without_nullable_unions(array.items), mask=validity)
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        factory = pa.LargeListArray if pa.types.is_large_list(dtype) else pa.ListArray
        return factory.from_arrays(array.offsets, _without_nullable_unions(array.values), mask=validity)
    if pa.types.is_fixed_size_list(dtype):
        values = array.values.slice(array.offset * dtype.list_size, len(array) * dtype.list_size)
        return pa.FixedSizeListArray.from_arrays(_without_nullable_unions(values), dtype.list_size, mask=validity)
    return array


def _compatible(got: pa.DataType, expected: pa.DataType) -> bool:
    """Check logical schema shape independently of the arrays' values.

    Same-family widths/units still need the reversible value check below. Names
    belong to struct fields; list element names and field metadata do not carry
    logical data identity. In particular, empty/null values cannot justify a
    cross-family coercion or hide missing, added or renamed struct children.
    """
    got, expected = _logical_type(got), _logical_type(expected)
    if pa.types.is_struct(got) and pa.types.is_struct(expected):
        return (got.num_fields == expected.num_fields
                and all(g.name == e.name and _compatible(g.type, e.type)
                        for g, e in zip(got, expected)))
    if _is_list(got) and _is_list(expected):
        if (pa.types.is_fixed_size_list(got) and pa.types.is_fixed_size_list(expected)
                and got.list_size != expected.list_size):
            return False
        return _compatible(got.value_type, expected.value_type)
    if pa.types.is_map(got) and pa.types.is_map(expected):
        return all(g.name == e.name and _compatible(g.type, e.type)
                   for g, e in [(got.key_field, expected.key_field),
                                (got.item_field, expected.item_field)])
    if pa.types.is_union(got) and pa.types.is_union(expected):
        return (got.mode == expected.mode and got.type_codes == expected.type_codes
                and got.num_fields == expected.num_fields
                and all(g.name == e.name and _compatible(g.type, e.type)
                        for g, e in zip(got, expected)))
    if pa.types.is_run_end_encoded(got) and pa.types.is_run_end_encoded(expected):
        return _compatible(got.value_type, expected.value_type)
    if pa.types.is_timestamp(got) and pa.types.is_timestamp(expected):
        # A zone labels instants; it is not data. Naive and zoned differ in kind.
        return (got.tz is None) == (expected.tz is None)
    if ((pa.types.is_integer(got) and pa.types.is_decimal(expected) and expected.scale == 0)
            or (pa.types.is_decimal(got) and got.scale == 0 and pa.types.is_integer(expected))):
        return True
    if (pa.types.is_fixed_size_binary(got) and pa.types.is_fixed_size_binary(expected)
            and got.byte_width != expected.byte_width):
        return False
    for family in (
        pa.types.is_integer, pa.types.is_floating, pa.types.is_decimal,
        pa.types.is_date, pa.types.is_time, pa.types.is_duration,
        lambda t: pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t),
        lambda t: (pa.types.is_binary(t) or pa.types.is_large_binary(t)
                   or pa.types.is_binary_view(t) or pa.types.is_fixed_size_binary(t)),
    ):
        if family(got) and family(expected):
            return True
    return got == expected


def _offset_type(dtype: pa.DataType) -> pa.DataType:
    """Materialize view leaves before Arrow filter/take kernels touch them.

    Null structs/lists cause filter to take their children. PyArrow 24/25 lack
    take kernels for string/binary views, including dictionary values. Large
    offsets preserve the view buffer's full range without an i32 size limit.
    """
    if pa.types.is_string_view(dtype):
        return pa.large_string()
    if pa.types.is_binary_view(dtype):
        return pa.large_binary()
    if pa.types.is_struct(dtype):
        return pa.struct([f.with_type(_offset_type(f.type)) for f in dtype])
    if _is_list(dtype):
        field = dtype.value_field.with_type(_offset_type(dtype.value_type))
        if pa.types.is_fixed_size_list(dtype):
            return pa.list_(field, dtype.list_size)
        factory = (pa.large_list if pa.types.is_large_list(dtype) else
                   pa.list_view if pa.types.is_list_view(dtype) else
                   pa.large_list_view if pa.types.is_large_list_view(dtype) else pa.list_)
        return factory(field)
    if pa.types.is_map(dtype):
        return pa.map_(dtype.key_field.with_type(_offset_type(dtype.key_type)),
                       dtype.item_field.with_type(_offset_type(dtype.item_type)),
                       keys_sorted=dtype.keys_sorted)
    if pa.types.is_dictionary(dtype):
        return pa.dictionary(dtype.index_type, _offset_type(dtype.value_type),
                             ordered=dtype.ordered)
    return dtype


def _materialize_views(array: pa.Array) -> pa.Array:
    dtype = _offset_type(array.type)
    return array if array.type == dtype else array.cast(dtype)


def _exact_equal(got: pa.Array, expected: pa.Array) -> bool:
    """Compare equal logical types, ignoring payloads hidden by null parents.

    Arrow's ordinary equality treats NaNs as unequal and signed zeros as equal.
    Neither is appropriate for fidelity measurements, including inside containers.
    Dictionary encoding is a representation detail, not part of logical equality.
    """
    got, expected = _materialize_views(got), _materialize_views(expected)
    if pa.types.is_dictionary(got.type):
        got = got.dictionary_decode()
    if pa.types.is_dictionary(expected.type):
        expected = expected.dictionary_decode()
    if got.type != expected.type or len(got) != len(expected):
        return False
    if not got.is_null().equals(expected.is_null()):
        return False
    if got.null_count:
        valid = got.is_valid()
        got, expected = got.filter(valid), expected.filter(valid)
    dtype = expected.type
    if pa.types.is_floating(dtype):
        # Both arrays have the same width, and null payloads were removed above.
        bits = f"u{dtype.bit_width // 8}"
        return bool(np.array_equal(got.to_numpy().view(bits), expected.to_numpy().view(bits)))
    if pa.types.is_struct(dtype):
        return all(_exact_equal(got.field(i), expected.field(i)) for i in range(dtype.num_fields))
    if _is_list(dtype):
        return (pc.list_value_length(got).equals(pc.list_value_length(expected))
                and _exact_equal(got.flatten(), expected.flatten()))
    if pa.types.is_map(dtype):
        def lengths(a):
            return pc.subtract(a.offsets.slice(1), a.offsets.slice(0, len(a)))
        # Slice child storage to the logical parent span; offsets may be nonzero.
        def child(a, values):
            start, end = a.offsets[0].as_py(), a.offsets[-1].as_py()
            return values.slice(start, end - start)
        return (lengths(got).equals(lengths(expected))
                and _exact_equal(child(got, got.keys), child(expected, expected.keys))
                and _exact_equal(child(got, got.items), child(expected, expected.items)))
    if pa.types.is_union(dtype):
        if not got.type_codes.equals(expected.type_codes):
            return False
        for i, code in enumerate(dtype.type_codes):
            selected = pc.equal(got.type_codes, code)
            if dtype.mode == "dense":
                g = got.field(i).take(got.offsets.filter(selected))
                e = expected.field(i).take(expected.offsets.filter(selected))
            else:
                g, e = got.field(i).filter(selected), expected.field(i).filter(selected)
            if not _exact_equal(g, e):
                return False
        return True
    if pa.types.is_run_end_encoded(dtype):
        return _exact_equal(pc.run_end_decode(got), pc.run_end_decode(expected))
    if isinstance(dtype, pa.BaseExtensionType):
        return _exact_equal(got.storage, expected.storage)
    return got.equals(expected)


def _windows(got: pa.ChunkedArray, expected: pa.ChunkedArray):
    """Yield aligned (got, expected) array slices, each within one chunk of both.

    Never combines a whole column: a string, binary or list column past 2 GiB
    of data overflows int32 offsets when its chunks are concatenated. Walks both
    chunk lists with one cursor each and slices single chunks, so a column costs
    O(chunks), not O(windows x chunks) as slicing the ChunkedArray would.
    """
    got_chunks = (chunk for chunk in got.chunks if len(chunk))
    expected_chunks = (chunk for chunk in expected.chunks if len(chunk))
    g, e = next(got_chunks, None), next(expected_chunks, None)
    while g is not None and e is not None:
        n = min(len(g), len(e))
        yield g.slice(0, n), e.slice(0, n)
        g = g.slice(n) if n < len(g) else next(got_chunks, None)
        e = e.slice(n) if n < len(e) else next(expected_chunks, None)


def values_equal(got: pa.Table, expected: pa.Table) -> tuple[bool, str]:
    """Allow same-family normalization only when casting back recovers values.

    This matches the Rust comparator's schema and losslessness guards. Safe Arrow
    casts alone cannot enforce logical families, and float narrowing can lose
    information without raising. Reversibility uses the same recursive bit-exact
    equality as the final comparison, including nested NaNs and signed zeros.
    Columns are compared window by window (`_windows`), never combined whole.
    """
    if got.num_rows != expected.num_rows or got.column_names != expected.column_names:
        return False, "row count or column names differ"
    for name in expected.column_names:
        if not _compatible(got.column(name).type, expected.column(name).type):
            return False, f"column {name!r}: incompatible logical types {got.column(name).type} vs {expected.column(name).type}"
        try:
            for g, e in _windows(got.column(name), expected.column(name)):
                g, e = _without_nullable_unions(g), _without_nullable_unions(e)
                g, e = _materialize_views(g), _materialize_views(e)
                if pa.types.is_dictionary(g.type):
                    g = g.dictionary_decode()
                if pa.types.is_dictionary(e.type):
                    e = e.dictionary_decode()
                if g.type != e.type:
                    normalized = g.cast(e.type)
                    if not _exact_equal(normalized.cast(g.type), g):
                        return False, f"column {name!r}: lossy cast {g.type} -> {e.type}"
                    g = normalized
                if not _exact_equal(g, e):
                    return False, f"column {name!r}: values or float bits differ"
        except (pa.ArrowException, ValueError, TypeError) as exc:
            return False, f"column {name!r}: normalization failed: {exc}"
    return True, ""


class _Rows:
    """One side of a streamed comparison: a batch stream read as a row sequence."""

    def __init__(self, batches):
        self._batches = iter(batches)
        self._current: pa.RecordBatch | None = None
        self._offset = 0

    def available(self) -> int:
        """Rows left in the current batch, advancing past empty ones; 0 at the end."""
        while self._current is None or self._offset >= self._current.num_rows:
            # Drop the spent batch before pulling the next, so one side holds
            # at most one batch.
            self._current = None
            self._current = next(self._batches, None)
            self._offset = 0
            if self._current is None:
                return 0
        return self._current.num_rows - self._offset

    def take(self, n: int) -> pa.RecordBatch:
        piece = self._current.slice(self._offset, n)
        self._offset += n
        return piece

    def count_rest(self) -> int:
        n = 0
        while (available := self.available()):
            self.take(available)
            n += available
        return n


def stream_equal(got_schema: pa.Schema, got, expected_schema: pa.Schema, expected) -> tuple[bool, str]:
    """`values_equal` over two record-batch streams, one window of each at a time.

    Batch boundaries need not agree -- a reader's batches are compared against
    the canonical's ingestion batches window by window -- and neither side is
    ever read whole: memory is one batch of each stream, like the Rust
    sidecars' `logical_eq_stream`. Names and types are checked from the
    schemas first, so an empty file is still compared. A failure names the
    rows of the window it was found in.
    """
    if got_schema.names != expected_schema.names:
        return False, f"column names differ: got {got_schema.names} != canonical {expected_schema.names}"
    equal, detail = values_equal(got_schema.empty_table(), expected_schema.empty_table())
    if not equal:
        return False, detail
    got_rows, expected_rows = _Rows(got), _Rows(expected)
    row = 0
    while True:
        g, e = got_rows.available(), expected_rows.available()
        if not g or not e:
            if not g and not e:
                return True, ""
            return False, (f"row count {row + got_rows.count_rest():,} != canonical "
                           f"{row + expected_rows.count_rest():,}")
        n = min(g, e)
        equal, detail = values_equal(pa.Table.from_batches([got_rows.take(n)]),
                                     pa.Table.from_batches([expected_rows.take(n)]))
        if not equal:
            return False, f"rows {row:,}..{row + n:,}: {detail}"
        row += n
