# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Serial shard alignment with projected reads and fixed output schemas."""
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import pyarrow as pa

from .batches import BatchLimits, BatchStream, SourceBatch, split_batch


def as_stream(path: Path, value: pa.Table | BatchStream) -> BatchStream:
    """Accept existing table parsers without duplicating their transformations."""
    if isinstance(value, BatchStream):
        return value
    if not isinstance(value, pa.Table):
        raise ValueError(f"expected a parsed table or batch stream for {path}")
    limits = BatchLimits.from_env()

    def make(columns):
        table = value if columns is None else value.select(columns)

        def batches():
            offset = 0
            with table.to_reader(max_chunksize=limits.rows) as reader:
                for decoded in reader:
                    for batch in split_batch(decoded, limits):
                        yield SourceBatch(path, offset, batch)
                        offset += batch.num_rows

        return BatchStream(table.schema, batches, make)

    return make(None)


def merge_streams(
    streams: list[BatchStream], *, promotion: str,
    renames: Mapping[str, str] | None = None,
    constants: list[Mapping[str, pa.Scalar]] | None = None,
) -> BatchStream:
    """Align shards by name, padding missing fields with nulls, in input order.

    Renames apply to source fields before per-shard constants are appended. The
    projection maps back to physical fields, so planning never decodes unrelated
    payload columns. The schema is resolved entirely before the first row read.
    """
    if not streams:
        raise ValueError("no parsed inputs")
    renames = renames or {}
    constants = constants if constants is not None else [{} for _ in streams]
    if len(constants) != len(streams):
        raise ValueError("one constant mapping is required per input")
    schemas, mappings = [], []
    for stream, added in zip(streams, constants):
        names = [renames.get(f.name, f.name) for f in stream.schema]
        if len(set(names + list(added))) != len(names) + len(added):
            raise ValueError("renamed source fields or synthetic fields collide")
        mappings.append(dict(zip(names, stream.schema.names)))
        fields = [f.with_name(name) for f, name in zip(stream.schema, names)]
        fields.extend(pa.field(name, value.type) for name, value in added.items())
        schemas.append(pa.schema(fields, metadata=stream.schema.metadata))
    unified = pa.unify_schemas(schemas, promote_options=promotion)
    # A field absent from a shard is nullable, even if its declaring shard
    # marks it required. Do not publish nulls under a non-nullable declaration.
    unified = pa.schema([
        f.with_nullable(True) if any(f.name not in s.names for s in schemas) else f
        for f in unified
    ], metadata=unified.metadata)

    def make(columns):
        schema = unified if columns is None else pa.schema(
            [unified.field(name) for name in columns], metadata=unified.metadata)

        def batches():
            for stream, mapping, added in zip(streams, mappings, constants):
                needed = [mapping[f.name] for f in schema if f.name in mapping]
                with stream.select(needed).open() as reader:
                    for item in reader:
                        arrays = []
                        for field in schema:
                            if field.name in added:
                                array = pa.repeat(added[field.name], item.batch.num_rows)
                            elif field.name in mapping:
                                array = item.batch.column(mapping[field.name])
                            else:
                                array = pa.nulls(item.batch.num_rows, type=field.type)
                            arrays.append(array.cast(field.type))
                        yield SourceBatch(item.source, item.row_offset,
                                          pa.RecordBatch.from_arrays(arrays, schema=schema))

        return BatchStream(schema, batches, make)

    return make(None)
