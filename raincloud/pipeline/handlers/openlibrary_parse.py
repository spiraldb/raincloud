# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse OpenLibrary tab-separated dumps into bounded canonical Arrow batches.

Each dump line is `type <TAB> key <TAB> revision <TAB> last_modified <TAB> JSON`,
emitted as columns `key: string`, `revision: int64`, `last_modified:
timestamp[us]`, `type: string` and `record: string` (the JSON, uninterpreted).
A line with fewer than five fields, a revision that is not an integer, bytes
that are not UTF-8, or a `last_modified` that is neither empty (null) nor a
timestamp fails the build, naming the file and line: no record is skipped or
rewritten. A fixed schema needs no inference pass.

Params:
    record_type : the dump's record type ("work", "edition", "author"). Lines of
                  any other `/type/...` are kept but counted and reported.
"""
from __future__ import annotations

import gzip
import sys
from pathlib import Path

import pyarrow as pa

from ..batches import BatchLimits, BatchStream, SourceBatch, batch_input, split_batch

_SCHEMA = pa.schema([
    ("key", pa.string()), ("revision", pa.int64()),
    ("last_modified", pa.timestamp("us")), ("type", pa.string()), ("record", pa.string()),
])


def _record_batch(columns):
    keys, revisions, timestamps, types, records = columns
    return pa.RecordBatch.from_arrays([
        pa.array(keys, type=pa.string()), pa.array(revisions, type=pa.int64()),
        pa.array([t or None for t in timestamps], type=pa.string()).cast(pa.timestamp("us"), safe=False),
        pa.array(types, type=pa.string()), pa.array(records, type=pa.string()),
    ], schema=_SCHEMA)


@batch_input("custom")
def openlibrary_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                      record_type: str = "work") -> list[tuple[str, BatchStream]]:
    if len(parsed) != 1:
        raise ValueError(f"openlibrary_parse expects exactly 1 input file, got {len(parsed)}")
    path, _ = parsed[0]
    limits = BatchLimits.from_env()
    expected_kind = f"/type/{record_type}"
    totals_reported = False  # the stream may be replayed; report its totals once

    def batches():
        nonlocal totals_reported
        opener = gzip.open if path.suffix == ".gz" else open
        columns = [[], [], [], [], []]
        retained_bytes = offset = other_kind = number = 0
        # newline="\n": a line ends only at \n, as the dump writes them, so a
        # stray \r inside a record cannot split it.
        with opener(path, "rt", encoding="utf-8", newline="\n") as source:
            lines = iter(source)
            while True:
                try:
                    line = next(lines)
                except StopIteration:
                    break
                except UnicodeDecodeError as exc:
                    raise ValueError(f"openlibrary_parse: {path.name} after line {number:,} "
                                     f"is not UTF-8: {exc}") from None
                number += 1
                parts = line.rstrip("\n").split("\t", 4)
                if len(parts) < 5:
                    raise ValueError(f"openlibrary_parse: {path.name} line {number:,} has "
                                     f"{len(parts)} tab-separated fields, not 5: {line[:120]!r}")
                kind, key, revision, timestamp, record = parts
                if kind != expected_kind:
                    other_kind += 1
                try:
                    revision = int(revision)
                except ValueError:
                    raise ValueError(f"openlibrary_parse: {path.name} line {number:,} has "
                                     f"revision {revision[:40]!r}, not an integer") from None
                # Bound the Python accumulation as well as the emitted Arrow
                # batches. Include per-record/list-slot overhead conservatively;
                # one unusually large input line may exceed the target by itself.
                cost = 4 * len(line) + 512
                if columns[0] and retained_bytes + cost > limits.target_bytes:
                    for batch in split_batch(_record_batch(columns), limits):
                        yield SourceBatch(path, offset, batch)
                        offset += batch.num_rows
                    columns = [[], [], [], [], []]
                    retained_bytes = 0
                for column, value in zip(columns, (key, revision, timestamp, kind, record)):
                    column.append(value)
                retained_bytes += cost
                if len(columns[0]) >= limits.rows:
                    for batch in split_batch(_record_batch(columns), limits):
                        yield SourceBatch(path, offset, batch)
                        offset += batch.num_rows
                    columns = [[], [], [], [], []]
                    retained_bytes = 0
            if columns[0]:
                for batch in split_batch(_record_batch(columns), limits):
                    yield SourceBatch(path, offset, batch)
                    offset += batch.num_rows
        if not totals_reported:
            totals_reported = True
            print(f"    total: {offset:,} records")
            if other_kind:
                print(f"  [warn] {other_kind:,} record(s) in {path.name} are not {expected_kind}",
                      file=sys.stderr)

    return [(spec["slug"], BatchStream(_SCHEMA, batches))]
