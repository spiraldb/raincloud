# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Serial, pull-based Arrow batches with a schema fixed before consumption.

Limits target decoded batch payload, not process RSS: source decoders may retain
pages/row groups, slices retain their parent buffers, and one oversized record
is indivisible. No task queue, prefetch, or whole-source collection lives here.
"""
from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa


@dataclass(frozen=True)
class BatchLimits:
    rows: int = 4096
    target_bytes: int = 16 * 1024 * 1024

    def __post_init__(self):
        for name in ("rows", "target_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"batch {name} must be a positive integer")

    @classmethod
    def from_env(cls) -> BatchLimits:
        """Limits from `RAINCLOUD_BATCH_ROWS` / `RAINCLOUD_BATCH_BYTES`; unset or
        empty means the default. A malformed value names its variable."""
        return cls(rows=_env_int("RAINCLOUD_BATCH_ROWS", cls.rows),
                   target_bytes=_env_int("RAINCLOUD_BATCH_BYTES", cls.target_bytes))


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}") from None
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}")
    return value


@dataclass(frozen=True)
class SourceBatch:
    source: Path
    row_offset: int
    batch: pa.RecordBatch


@dataclass(frozen=True)
class BatchStream:
    """A lazily opened, fixed-schema sequence of record batches.

    `factory` must be REPLAYABLE: each call starts a fresh pass from the first
    row, because consumers read a stream more than once (`tighten_stream` plans
    types in one pass and emits in another). `projection`, when given, returns
    the same rows restricted to the named columns, in that order, reading only
    them — so a planning pass never decodes unrelated payload columns. Without
    one, `select` projects each batch after it is read.
    """

    schema: pa.Schema
    factory: Callable[[], Iterator[SourceBatch]]
    projection: Callable[[list[str]], BatchStream] | None = None

    def select(self, columns: list[str]) -> BatchStream:
        if self.projection is not None:
            return self.projection(columns)
        schema = pa.schema([self.schema.field(name) for name in columns], metadata=self.schema.metadata)

        def batches():
            with self.open() as reader:
                for item in reader:
                    yield SourceBatch(item.source, item.row_offset, item.batch.select(columns))

        return BatchStream(schema, batches)

    @contextmanager
    def open(self) -> Iterator[Iterator[SourceBatch]]:
        """Close the producer on exhaustion, consumer failure, or early exit."""
        iterator = iter(self.factory())
        try:
            yield iterator
        finally:
            close = getattr(iterator, "close", None)
            if close is not None:
                close()


def batch_input(*readers: str):
    """Declare which input readers a handler accepts without materialization.

    For `"parquet"`, parse hands the handler a `BatchStream` in place of a
    decoded table. `"custom"` changes nothing in parse (a custom reader always
    passes each path with None); it records, for handlers.md, that the handler
    builds its own BatchStream from the path rather than decoding a table.
    """
    def decorate(fn):
        fn.batch_readers = frozenset(readers)
        return fn
    return decorate


def _dictionary_bytes(array: pa.Array) -> int:
    """Bytes of every dictionary in `array`, nested ones included (a struct or
    list child): each slice carries the whole dictionary."""
    t = array.type
    if pa.types.is_dictionary(t):
        return array.dictionary.nbytes
    if pa.types.is_struct(t):
        return sum(_dictionary_bytes(array.field(i)) for i in range(t.num_fields))
    if pa.types.is_list(t) or pa.types.is_large_list(t) or pa.types.is_fixed_size_list(t) or pa.types.is_map(t):
        return _dictionary_bytes(array.values)
    return 0


def _payload_bytes(batch: pa.RecordBatch) -> int:
    """`nbytes` without dictionaries: every slice of a dictionary column (or of
    one nested in a struct or list) carries the whole dictionary, so it cannot
    shrink by splitting."""
    return batch.nbytes - sum(_dictionary_bytes(column) for column in batch.columns)


def split_batch(batch: pa.RecordBatch, limits: BatchLimits) -> Iterator[pa.RecordBatch]:
    """Split by rows and observed bytes; a single large row passes alone.

    These are zero-copy views. Producers must bound their own allocations as
    well; splitting an already materialized dataset is not bounded ingestion.
    """
    for start in range(0, batch.num_rows, limits.rows):
        part = batch.slice(start, limits.rows)
        if _payload_bytes(part) <= limits.target_bytes or part.num_rows == 1:
            yield part
        else:
            # Depth is logarithmic; only views into this one producer batch live.
            middle = part.num_rows // 2
            yield from split_batch(part.slice(0, middle), limits)
            yield from split_batch(part.slice(middle), limits)
