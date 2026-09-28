# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse ClickHouse JSONBench's Bluesky JSONL.gz dumps into the canonical Arrow
spine with a single VARIANT column.

Source: https://github.com/ClickHouse/JSONBench
Data: https://clickhouse-public-datasets.s3.amazonaws.com/bluesky/file_NNNN.json.gz

Each line is one Bluesky firehose event — different event types carry
different fields, which is exactly what VARIANT is for.

Implementation: Python reads each .json.gz and splits it into lines on b"\\n"
only, so it owns every line's number and bytes. A batch of lines goes to DuckDB
(1.5.x) as an Arrow table, which casts each line through JSON to VARIANT
(`CAST(TRY_CAST(line AS JSON) AS VARIANT)`: casting the text straight to VARIANT
stores a VARCHAR variant, one string per event, not the parsed object) and
projects it through `variant_to_parquet_variant`, the shredded
`struct<metadata, value, ...>` Arrow can export (DuckDB cannot export a raw
VARIANT). `duckdb_variant.stream_canonical_arrow` stamps `VARIANT_EXT` on it so
`discovery._is_variant_field` recognizes it, declares its `metadata` and `value`
non-nullable as the VARIANT specs require (checking every row), and the batch
goes to the canonical Arrow writer (`<slug>.arrow.zstd`). One batch is in memory
at a time.

The upstream dumps wrap a record longer than 65,535 bytes: its first 65,535
bytes, a newline, then the rest (16 records in the 100M dump, in files 5-7, 68,
82, 88, 94, 96 and 97). The newline is not the record's: every one of those cuts
falls inside a JSON string, where a raw newline is invalid, and the pieces
joined without it are exactly one JSON object. So a line of exactly
WRAP_BYTES bytes that is not JSON on its own is joined with the lines after it,
as long as each piece before is WRAP_BYTES long, until the join is one JSON
object. A line that is not a JSON object and does not rejoin into one fails the
build, naming the file, line and byte length. Nothing is dropped:
`expect.rows` counts upstream records.

Output schema: single column `data: VARIANT`. Returns `[]` (streaming handler —
the canonical Arrow is fully written by return time, so `write_canonical` is
skipped and the exporters derive parquet/vortex from the canonical).

Params:
    files_limit : int | None  — process first N files only (testing).
    batch_size  : int         — lines per DuckDB query and written batch. Default 100_000.
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Iterator

import pyarrow as pa

from raincloud import duckdb_connect

from ..canonical import open_canonical_writer
from ..duckdb_variant import stream_canonical_arrow

# The width at which the upstream dumps wrap a long record (see the module docstring).
WRAP_BYTES = 65_535
# Decompressed bytes read per call; a partial last line carries to the next read.
_READ_BYTES = 1 << 26

_SQL = """
    SELECT variant_to_parquet_variant(CAST(j AS VARIANT)) AS data,
           coalesce(json_type(j) = 'OBJECT', false) AS is_object
    FROM (SELECT TRY_CAST(line AS JSON) AS j FROM jsonbench_lines)
"""


def _line_batches(path: Path, batch_size: int) -> Iterator[tuple[int, list[bytes]]]:
    """Yield `(first_line_number, lines)` over `path`'s lines, split on b"\\n" only.

    A batch never ends on a line of exactly WRAP_BYTES bytes unless the file
    does, so the pieces of a wrapped record always arrive in one batch.
    """
    pending: list[bytes] = []
    tail = b""
    lineno = 1
    with gzip.open(path, "rb") as f:
        while True:
            block = f.read(_READ_BYTES)
            if block:
                lines = (tail + block).split(b"\n")
                tail = lines.pop()
                pending.extend(lines)
            elif tail:
                pending.append(tail)  # a last line with no newline
                tail = b""
            while pending and (len(pending) >= batch_size or not block):
                cut = min(batch_size, len(pending))
                while cut < len(pending) and len(pending[cut - 1]) == WRAP_BYTES:
                    cut += 1
                if block and len(pending[cut - 1]) == WRAP_BYTES:
                    break  # the rest of a wrapped record is not read yet
                yield lineno, pending[:cut]
                lineno += cut
                del pending[:cut]
            if not block:
                return


def _is_object(text: bytes) -> bool:
    try:
        return isinstance(json.loads(text), dict)
    except ValueError:  # includes UnicodeDecodeError
        return False


def _rejoin(name: str, first: int, lines: list[bytes]) -> tuple[list[bytes], list[int] | None]:
    """Join each wrapped record's lines back into one; return the records and,
    when any were joined, each record's first line number."""
    heads = [i for i, line in enumerate(lines) if len(line) == WRAP_BYTES]
    if not heads:
        return lines, None
    records: list[bytes] = []
    numbers: list[int] = []
    done = 0
    for head in heads:
        if head < done:
            continue  # a piece of the record before
        records.extend(lines[done:head])
        numbers.extend(range(first + done, first + head))
        end = head + 1
        while not _is_object(b"".join(lines[head:end])):
            if len(lines[end - 1]) != WRAP_BYTES or end == len(lines):
                pieces = ", ".join(f"line {first + i} ({len(lines[i]):,} bytes)" for i in range(head, end))
                raise ValueError(
                    f"{name}: {pieces} do not rejoin into one JSON object. A line of "
                    f"exactly {WRAP_BYTES:,} bytes that is not JSON is a record the dump "
                    "wrapped, and its pieces must join into one.")
            end += 1
        records.append(b"".join(lines[head:end]))
        numbers.append(first + head)
        done = end
    records.extend(lines[done:])
    numbers.extend(range(first + done, first + len(lines)))
    return records, numbers


def _line_table(name: str, first: int, records: list[bytes], numbers: list[int] | None,
                chunks: int) -> pa.Table:
    """The records as a `line: string` table in `chunks` pieces, so DuckDB scans it in parallel."""
    try:
        lines = pa.array(records, pa.binary()).cast(pa.string())
    except pa.ArrowInvalid:
        for i, record in enumerate(records):
            try:
                record.decode("utf-8")
            except UnicodeDecodeError as exc:
                line = numbers[i] if numbers else first + i
                raise ValueError(f"{name}: line {line} ({len(record):,} bytes) is not UTF-8: {exc}") from None
        raise
    step = max(1, -(-len(lines) // chunks))
    return pa.Table.from_batches(
        [pa.record_batch([lines.slice(o, step)], names=["line"]) for o in range(0, len(lines), step)],
        schema=pa.schema([("line", pa.string())]))


def jsonbench_variant_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                              files_limit: int | None = None,
                              batch_size: int = 100_000
                              ) -> list[tuple[str, pa.Table]]:
    gz_files = sorted(p for p, _ in parsed if p.name.endswith(".json.gz"))
    if not gz_files:
        raise ValueError("jsonbench_variant_parse: no .json.gz files in extracted output")
    if files_limit:
        gz_files = gz_files[:files_limit]
    print(f"  processing {len(gz_files)} .json.gz files")

    # In memory: each query reads one registered batch, so nothing grows or spills.
    con = duckdb_connect()
    try:
        chunks = int(con.execute("SELECT current_setting('threads')").fetchone()[0])
        con.register("jsonbench_lines", pa.table({"line": pa.array([], pa.string())}))
        streamed, _ = stream_canonical_arrow(con, _SQL, ["data"], batch_size)
        schema = pa.schema([streamed.field("data")])
        total = reported = rejoined = 0
        with open_canonical_writer(spec["slug"], schema) as w:
            for path in gz_files:
                for first, lines in _line_batches(path, batch_size):
                    records, numbers = _rejoin(path.name, first, lines)
                    rejoined += len(lines) - len(records)
                    con.register("jsonbench_lines", _line_table(path.name, first, records, numbers, chunks))
                    _, batches = stream_canonical_arrow(con, _SQL, ["data"], batch_size)
                    row = 0
                    for b in batches:
                        is_object = b.column("is_object")
                        if is_object.false_count:
                            i = row + is_object.to_pylist().index(False)
                            line = numbers[i] if numbers else first + i
                            raise ValueError(
                                f"{path.name}: line {line} ({len(records[i]):,} bytes) is not a JSON "
                                f"object, and is not a record the dump wrapped at {WRAP_BYTES:,} bytes")
                        data = b.column("data")
                        if data.type != schema.field("data").type:
                            raise ValueError(f"{path.name}: from line {first}, DuckDB's shredded VARIANT "
                                             f"is {data.type}, not {schema.field('data').type}")
                        w.write_batch(pa.record_batch([data], schema=schema))
                        row += b.num_rows
                    if row != len(records):
                        raise ValueError(f"{path.name}: DuckDB returned {row:,} rows for "
                                         f"{len(records):,} records from line {first}")
                    total += row
                    if total - reported >= 10_000_000:
                        print(f"    streamed {total:,} rows", flush=True)
                        reported = total
    finally:
        con.close()

    print(f"  wrote {spec['slug']} canonical Arrow ({total:,} rows, "
          f"{rejoined} continuation line(s) of records wrapped at {WRAP_BYTES:,} bytes rejoined)")
    return []
