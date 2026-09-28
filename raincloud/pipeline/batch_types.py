# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Dataset-wide type decisions from a bounded, projected statistics pass."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc

from .batches import BatchStream, SourceBatch

# Non-null values sampled to decide that a binary column is UTF-8 text.
UTF8_SAMPLE_SIZE = 4096


def pick_integer_type(mn: int, mx: int) -> pa.DataType:
    """The narrowest integer type holding [mn, mx] (unsigned when mn >= 0)."""
    if mn >= 0:
        if mx <= 0xFF:
            return pa.uint8()
        if mx <= 0xFFFF:
            return pa.uint16()
        if mx <= 0xFFFFFFFF:
            return pa.uint32()
        return pa.uint64()
    if -128 <= mn and mx <= 127:
        return pa.int8()
    if -32768 <= mn and mx <= 32767:
        return pa.int16()
    if -2**31 <= mn and mx <= 2**31 - 1:
        return pa.int32()
    return pa.int64()


def _sample_utf8(stream: BatchStream, names: list[str]) -> dict[str, bool]:
    """Per binary column: do its first UTF8_SAMPLE_SIZE non-null values decode?

    A pass of its own that stops once every column is decided, so payload
    columns are not decoded through the whole dataset during planning.
    """
    state = {name: [0, True] for name in names}  # [seen, valid]
    with stream.select(names).open() as batches:
        for item in batches:
            for name, entry in state.items():
                if entry[0] >= UTF8_SAMPLE_SIZE or not entry[1]:
                    continue
                for value in item.batch.column(name):
                    if entry[0] >= UTF8_SAMPLE_SIZE:
                        break
                    if not value.is_valid:
                        continue
                    try:
                        value.as_py().decode("utf-8")
                    except UnicodeDecodeError:
                        entry[1] = False
                        break
                    entry[0] += 1
            if all(seen >= UTF8_SAMPLE_SIZE or not valid for seen, valid in state.values()):
                break
    return {name: bool(seen) and valid for name, (seen, valid) in state.items()}


def tighten_stream(stream: BatchStream, fixed_lists: list[str]) -> BatchStream:
    """Plan dataset-wide types in bounded passes, then replay with the plan.

    Planning reads `stream` (which must be replayable, see `BatchStream`) for
    integer bounds and fixed list lengths over a projection of just those
    columns, and samples binary columns for UTF-8 separately. The returned
    stream replays `stream` and casts each batch; a cast that no longer fits
    (a late non-UTF-8 value) fails before the canonical is published.

    Field and schema metadata are dropped, as the table path of
    `handlers.tighten_types` drops them, so a dataset's canonical does not
    depend on whether its parquet was read as a table or as batches.
    """
    integers = {f.name: [None, None] for f in stream.schema if pa.types.is_integer(f.type)}
    binary = [f.name for f in stream.schema
              if pa.types.is_binary(f.type) or pa.types.is_large_binary(f.type)]
    lengths = {f.name: [None, None] for f in stream.schema if f.name in fixed_lists
               and (pa.types.is_list(f.type) or pa.types.is_large_list(f.type))}
    names = [f.name for f in stream.schema if f.name in integers or f.name in lengths]

    def accumulate(bounds, values):
        mm = pc.min_max(values).as_py()
        if mm['min'] is not None:
            bounds[0] = mm['min'] if bounds[0] is None else min(bounds[0], mm['min'])
            bounds[1] = mm['max'] if bounds[1] is None else max(bounds[1], mm['max'])

    if names or binary:
        print(f"  planning global types ({len(names) + len(binary)} projected columns)", flush=True)
    if names:
        with stream.select(names).open() as batches:
            for item in batches:
                for name, bounds in integers.items():
                    accumulate(bounds, item.batch.column(name))
                for name, bounds in lengths.items():
                    accumulate(bounds, pc.list_value_length(item.batch.column(name)))
    text = _sample_utf8(stream, binary) if binary else {}

    fields = []
    for field in stream.schema:
        dtype = field.type
        if field.name in integers:
            low, high = integers[field.name]
            if low is not None:
                dtype = pick_integer_type(low, high)
        elif text.get(field.name):
            dtype = pa.large_string() if pa.types.is_large_binary(dtype) else pa.string()
        if field.name in lengths:
            low, high = lengths[field.name]
            if low is not None and low == high and low > 0:
                target = pa.list_(dtype.value_type, low)
                try:
                    pa.array([], type=dtype).cast(target)
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    # Probe only: a raising cast means this dtype pair is not castable,
                    # which is the answer being sought. Nothing is lost by discarding it.
                    pass
                else:
                    dtype = target
        fields.append(pa.field(field.name, dtype, nullable=field.nullable))
    schema = pa.schema(fields)

    def batches():
        with stream.open() as reader:
            for item in reader:
                # Safe casts detect invalid UTF-8 beyond the global sample and
                # values that no longer fit a plan, before canonical publication.
                arrays = [item.batch.column(f.name).cast(f.type) for f in schema]
                yield SourceBatch(item.source, item.row_offset,
                                  pa.RecordBatch.from_arrays(arrays, schema=schema))

    return BatchStream(schema, batches)
