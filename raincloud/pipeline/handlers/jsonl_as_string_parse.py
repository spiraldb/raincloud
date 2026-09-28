# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stream a JSONL[.gz] file into a parquet with a single `raw_json: string`
column — one row per line, preserving the JSON text byte-for-byte.

Useful for upstreams whose JSON has cross-record type drift (the same field
is `number` in one record and `string` in another), which trips pyarrow's
strict-typed JSON reader. Storing the canonical JSON as text sidesteps the
type problem entirely and defers VARIANT conversion to a later in-place
string→VARIANT cast.

Streams through the canonical Arrow writer so multi-GB inputs don't OOM.

Params:
    batch_size : int  — rows per Arrow batch. Default 100_000.
    encoding   : str  — input encoding. Default "utf-8".
"""
from __future__ import annotations

import gzip
from pathlib import Path

import pyarrow as pa

from ..canonical import open_canonical_writer

SCHEMA = pa.schema([("raw_json", pa.string())])


def jsonl_as_string_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                          batch_size: int = 100_000,
                          encoding: str = "utf-8",
                          ) -> list[tuple[str, pa.Table]]:
    paths = sorted(p for p, _ in parsed)
    if not paths:
        raise ValueError("jsonl_as_string_parse: no input files")

    total = 0
    with open_canonical_writer(spec["slug"], SCHEMA) as writer:
        for path in paths:
            opener = gzip.open if path.name.endswith(".gz") else open
            buffer: list[str] = []
            # Binary lines end only at \n, so a stray \r inside a record cannot
            # split it; decoding is strict, so bytes are never replaced.
            with opener(path, "rb") as f:
                for number, raw in enumerate(f, 1):
                    raw = raw.rstrip(b"\n").rstrip(b"\r")
                    if not raw:
                        continue
                    try:
                        line = raw.decode(encoding)
                    except UnicodeDecodeError as exc:
                        raise ValueError(f"jsonl_as_string_parse: {path.name} line {number:,} "
                                         f"is not {encoding}: {exc}") from None
                    buffer.append(line)
                    if len(buffer) >= batch_size:
                        writer.write_table(pa.Table.from_pydict({"raw_json": buffer}, schema=SCHEMA))
                        total += len(buffer)
                        buffer = []
                if buffer:
                    writer.write_table(pa.Table.from_pydict({"raw_json": buffer}, schema=SCHEMA))
                    total += len(buffer)
            print(f"    {path.name}: cumulative {total:,} rows")

    print(f"  wrote {spec['slug']}  rows={total:,}")
    return []
