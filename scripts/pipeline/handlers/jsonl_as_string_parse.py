# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stream a JSONL[.gz] file into a parquet with a single `raw_json: string`
column — one row per line, preserving the JSON text byte-for-byte.

Useful for upstreams whose JSON has cross-record type drift (the same field
is `number` in one record and `string` in another), which trips pyarrow's
strict-typed JSON reader. Storing the canonical JSON as text sidesteps the
type problem entirely and defers VARIANT conversion to a later in-place
string→VARIANT cast.

Streams through pq.ParquetWriter so multi-GB inputs don't OOM.

Params:
    batch_size : int  — rows per parquet batch. Default 100_000.
    encoding   : str  — input encoding. Default "utf-8".
"""
from __future__ import annotations

import gzip
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA = pa.schema([("raw_json", pa.string())])


def jsonl_as_string_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                          batch_size: int = 100_000,
                          encoding: str = "utf-8",
                          ) -> list[tuple[str, pa.Table]]:
    paths = sorted(p for p, _ in parsed)
    if not paths:
        raise ValueError("jsonl_as_string_parse: no input files")

    from ..spec import display_path, output_format_dir, spec_field
    out_path = output_format_dir(spec["slug"], "parquet") / spec_field(
        spec, "write.output", f"{spec['slug']}.parquet"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    compression = spec_field(spec, "write.compression", "zstd")

    total = 0
    writer = pq.ParquetWriter(out_path, SCHEMA, compression=compression)
    try:
        for path in paths:
            opener = gzip.open if path.name.endswith(".gz") else open
            buffer: list[str] = []
            with opener(path, "rt", encoding=encoding, errors="replace") as f:
                for line in f:
                    line = line.rstrip("\n").rstrip("\r")
                    if not line:
                        continue
                    buffer.append(line)
                    if len(buffer) >= batch_size:
                        writer.write_table(pa.Table.from_pydict({"raw_json": buffer}, schema=SCHEMA))
                        total += len(buffer)
                        buffer = []
                if buffer:
                    writer.write_table(pa.Table.from_pydict({"raw_json": buffer}, schema=SCHEMA))
                    total += len(buffer)
            print(f"    {path.name}: cumulative {total:,} rows")
    finally:
        writer.close()

    print(f"  wrote {display_path(out_path)}  rows={total:,}")
    return []
