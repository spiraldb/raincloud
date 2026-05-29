# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse ClickHouse JSONBench's Bluesky JSONL.gz dumps into a single parquet
with a VARIANT column.

Source: https://github.com/ClickHouse/JSONBench
Data: https://clickhouse-public-datasets.s3.amazonaws.com/bluesky/file_NNNN.json.gz

Each line is one Bluesky firehose event — different event types carry
different fields, which is exactly what VARIANT is for.

Implementation: VARIANT is a DuckDB/Parquet logical type with no direct Arrow
equivalent (DuckDB's .arrow() export on a VARIANT column raises
`Unsupported Arrow type VARIANT`). So we stay entirely in DuckDB:

  1. Create a disk-backed DuckDB DB in /tmp (so 100 M rows don't OOM the
     in-memory DB).
  2. Stream each .json.gz, batch-insert lines via executemany with
     `CAST(? AS VARIANT)`.
  3. At the end, `COPY ... TO 'out.parquet' (FORMAT PARQUET, COMPRESSION
     'zstd')` — this path DOES write VARIANT correctly.
  4. Clean up the tmp DuckDB file.

Output schema: single column `data: VARIANT`.

Params:
    files_limit : int | None  — process first N files only (testing).
    batch_size  : int         — rows per executemany. Default 50_000.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa

from ..spec import duckdb_connect


def jsonbench_variant_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                              files_limit: int | None = None,
                              batch_size: int = 50_000
                              ) -> list[tuple[str, pa.Table]]:
    gz_files = sorted(p for p, _ in parsed if p.name.endswith(".json.gz"))
    if not gz_files:
        raise ValueError("jsonbench_variant_parse: no .json.gz files in extracted output")
    if files_limit:
        gz_files = gz_files[:files_limit]
    print(f"  processing {len(gz_files)} .json.gz files")

    from ..spec import display_path, output_format_dir, spec_field
    out_path = output_format_dir(spec["slug"], "parquet") / spec_field(spec, "write.output", f"{spec['slug']}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    compression = spec_field(spec, "write.compression", "zstd")

    # Disk-backed temp DB so the build doesn't OOM if DuckDB spills.
    # VARIANT requires DuckDB storage version ≥ v1.5.0; the helper applies it.
    tmpdir = Path(tempfile.mkdtemp(prefix="jsonbench_"))
    db_path = tmpdir / "build.duckdb"
    con = duckdb_connect(db_path)
    try:
        # Process the entire .json.gz set in one server-side query: read_csv
        # with disabled delim/quote/escape treats each line as one VARCHAR row,
        # then CAST to VARIANT parses the JSON in C++. ~5-10× faster than the
        # prior per-row Python executemany loop.
        # Process per-file so progress is visible in the log.
        total = 0
        for i, gz_path in enumerate(gz_files, 1):
            n = con.execute(f"""
                SELECT COUNT(*) FROM read_csv(
                    '{gz_path}',
                    delim => chr(1),
                    quote => chr(2),
                    escape => chr(3),
                    header = false,
                    columns = {{'line': 'VARCHAR'}},
                    compression = 'gzip',
                    max_line_size = 100000000,
                    strict_mode = false
                )
                WHERE length(line) > 0
            """).fetchone()[0]
            # We staged a count; now do the actual insert into a VARIANT column
            # via a parquet-direct COPY at end of loop (single query).
            total += n
            print(f"    {i}/{len(gz_files)} {gz_path.name}: {n:,} rows  (running total: {total:,})")

        glob_pattern = str(gz_files[0].parent / "*.json.gz")
        print(f"  writing {total:,} rows to {display_path(out_path)}")
        con.execute(f"""
            COPY (
                SELECT CAST(line AS VARIANT) AS data
                FROM read_csv(
                    '{glob_pattern}',
                    delim => chr(1),
                    quote => chr(2),
                    escape => chr(3),
                    header = false,
                    columns = {{'line': 'VARCHAR'}},
                    compression = 'gzip',
                    max_line_size = 100000000,
                    strict_mode = false
                )
                WHERE length(line) > 0
            ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION '{compression}')
        """)
    finally:
        con.close()
        # Tidy up the temp DB
        for p in tmpdir.iterdir():
            p.unlink()
        tmpdir.rmdir()

    print(f"  wrote {display_path(out_path)}")
    return []
