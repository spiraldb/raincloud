# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Merge Public BI Benchmark .csv.bz2 partitions using the sibling .sql schema.

Each workload at https://event.cwi.nl/da/PublicBIbenchmark/<Name>/ ships:
    - <Name>.schema.sql   : CREATE TABLE ... (col1 TYPE, col2 TYPE, ...)
    - <Name>_1.csv.bz2, <Name>_2.csv.bz2, ...   : pipe-delimited partitions

The handler streams each CSV through `pyarrow.csv.open_csv` one batch at a
time straight into a `ParquetWriter`, rather than materialising every
partition into memory and then concatenating — that approach OOMs on large
workloads (CommonGovernment's 13 partitions decompress to ~103 GB of CSV,
which can't fit in RAM simultaneously).

Writes the output parquet directly and returns `[]`, so the normal write
stage becomes a no-op (same contract as `lichess_pgn_parse`,
`stack_exchange_split`, and the variant handlers).
"""
from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pac
import pyarrow.parquet as pq

SQL_TO_ARROW = {
    "SMALLINT": pa.int16(), "INTEGER": pa.int32(), "INT": pa.int32(), "BIGINT": pa.int64(),
    "DECIMAL": pa.float64(), "REAL": pa.float32(), "DOUBLE": pa.float64(),
    "FLOAT": pa.float64(),
    "VARCHAR": pa.string(), "CHAR": pa.string(), "TEXT": pa.string(),
    "DATE": pa.date32(), "TIMESTAMP": pa.timestamp("us"), "TIME": pa.time32("s"),
    "BOOLEAN": pa.bool_(),
}


def _parse_schema_sql(sql_text: str) -> list[tuple[str, pa.DataType]]:
    """Return `[(col_name, arrow_type), ...]` parsed from a CWI-style
    `CREATE TABLE "Workload_N"(...)` statement.

    Handles:
      - quoted-or-bare table names and column names
      - parenthesised type widths (`varchar(50)`, `decimal(8, 4)`) — uses a
        balanced-paren scan instead of a naive `)` terminator so nested
        parens don't close the body early
      - trailing `NOT NULL` on column defs
    """
    m = re.search(r'CREATE\s+TABLE\s+(?:"[^"]+"|\w+)\s*\(', sql_text, re.IGNORECASE)
    if not m:
        raise ValueError("could not locate CREATE TABLE statement in schema SQL")
    start = m.end()
    depth = 1
    i = start
    while i < len(sql_text) and depth:
        c = sql_text[i]
        if c == "(": depth += 1
        elif c == ")": depth -= 1
        i += 1
    body = sql_text[start:i - 1]  # drop the closing ')'

    # Split on top-level commas — commas inside parens are part of a type
    # spec (e.g. `decimal(8, 4)`), and commas inside a quoted column name
    # are literal (Rentabilidad has `"VENTA: Coord, OL, Merc. 14"`).
    parts: list[str] = []
    depth = 0
    in_quote = False
    buf: list[str] = []
    for c in body:
        if c == '"':
            in_quote = not in_quote
        elif not in_quote:
            if c == "(": depth += 1
            elif c == ")": depth -= 1
        if c == "," and depth == 0 and not in_quote:
            parts.append("".join(buf)); buf = []
        else:
            buf.append(c)
    parts.append("".join(buf))

    columns: list[tuple[str, pa.DataType]] = []
    for raw in parts:
        raw = raw.strip()
        if not raw or raw.upper().startswith(("PRIMARY", "CONSTRAINT", "UNIQUE", "FOREIGN")):
            continue
        m2 = re.match(r'^(?:"([^"]+)"|(\w+))\s+(.+)$', raw)
        if not m2: continue
        name = m2.group(1) or m2.group(2)
        rest = m2.group(3).strip().upper()
        base = re.match(r"([A-Z]+)", rest)
        base = base.group(1) if base else "VARCHAR"
        columns.append((name, SQL_TO_ARROW.get(base, pa.string())))
    return columns


_PART_RE = re.compile(r"^(.+)_(\d+)\.csv$")


def _partition_index(p: Path) -> int:
    m = _PART_RE.match(p.name)
    return int(m.group(2)) if m else 0


def public_bi_merge(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                    workload: str, all_varchar: bool = False
                    ) -> list[tuple[str, pa.Table]]:
    """If `all_varchar=True`, every column is read as `pa.string()` regardless
    of the declared SQL type. Set this on workloads where the upstream
    `<W>_N.table.sql` disagrees with its CSV (Public BI Benchmark has a few:
    MLB declares `int16` for a column that holds floats; TableroSistemaPenal
    declares numeric for a column containing Spanish words; Rentabilidad has
    a partition whose declared column count is off by one from its row
    delimiter count). Downstream consumers can TRY_CAST back in SQL."""
    # The extract stage delivers:
    #   - `<W>_N.csv` partitions (decompressed from bz2)
    #   - `<W>_N.table.sql` per-partition schemas (from the custom fetcher)
    schema_paths: dict[int, Path] = {}
    partitions: list[Path] = []
    for path, _ in parsed:
        if path.name.endswith(".table.sql"):
            m = re.match(rf"{re.escape(workload)}_(\d+)\.table\.sql$", path.name)
            if m: schema_paths[int(m.group(1))] = path
        elif path.name.endswith(".csv"):
            partitions.append(path)
    if not schema_paths:
        raise FileNotFoundError(f"no per-partition schema for {workload}")
    if not partitions:
        raise FileNotFoundError(f"no CSV partitions found for {workload}")

    sorted_parts = sorted(partitions, key=_partition_index)

    # Parse every partition's schema and build a unified column order:
    # keep the first occurrence's type for a given column name; subsequent
    # partitions that add new names append to the tail. This behaves like
    # `union_by_name` across partitions.
    per_partition_schema: dict[int, list[tuple[str, pa.DataType]]] = {}
    unified_order: list[str] = []
    unified_types: dict[str, pa.DataType] = {}
    for idx in sorted(schema_paths):
        cols = _parse_schema_sql(schema_paths[idx].read_text())
        if all_varchar:
            cols = [(n, pa.string()) for n, _ in cols]
        per_partition_schema[idx] = cols
        for name, ty in cols:
            if name not in unified_types:
                unified_types[name] = ty
                unified_order.append(name)

    arrow_schema = pa.schema([pa.field(n, unified_types[n]) for n in unified_order])
    print(f"  unified schema: {len(unified_order)} cols "
          f"(partition widths: {sorted(len(v) for v in per_partition_schema.values())})")

    from ..spec import REPO_ROOT, output_format_dir, spec_field
    out_path = output_format_dir(spec["slug"], "parquet") / spec_field(
        spec, "write.output", f"{spec['slug']}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    compression = spec_field(spec, "write.compression", "zstd")

    # `invalid_row_handler` skips rows with the wrong column count (rather
    # than aborting the whole read). Public BI Benchmark has stray unescaped
    # pipes in a few workloads (Rentabilidad) where one of ~10M rows holds an
    # extra delimiter; we'd rather drop it than fail the build.
    parse_opts = pac.ParseOptions(
        delimiter="|", quote_char=False,
        invalid_row_handler=lambda row: "skip",
    )
    block_size = 16 * 1024 * 1024

    total_rows = 0
    with pq.ParquetWriter(out_path, arrow_schema, compression=compression) as writer:
        for i, p in enumerate(sorted_parts, 1):
            idx = _partition_index(p)
            part_cols = per_partition_schema.get(idx)
            if part_cols is None:
                print(f"    [{i}/{len(sorted_parts)}] {p.name}: no schema — skipping")
                continue
            part_names = [n for n, _ in part_cols]
            part_types = {n: t for n, t in part_cols}
            read_opts = pac.ReadOptions(column_names=part_names, block_size=block_size)
            convert_opts = pac.ConvertOptions(column_types=part_types,
                                              strings_can_be_null=True)

            reader = pac.open_csv(p, read_options=read_opts,
                                  parse_options=parse_opts,
                                  convert_options=convert_opts)
            part_rows = 0
            try:
                while True:
                    try:
                        batch = reader.read_next_batch()
                    except StopIteration:
                        break
                    # Extend batch to the unified schema: add NULL arrays for
                    # any column the partition is missing, then reorder.
                    arrays = []
                    for name in unified_order:
                        ty = unified_types[name]
                        if name in part_types:
                            arrays.append(batch.column(part_names.index(name)))
                        else:
                            arrays.append(pa.nulls(batch.num_rows, type=ty))
                    extended = pa.record_batch(arrays, schema=arrow_schema)
                    writer.write_batch(extended)
                    part_rows += batch.num_rows
            finally:
                reader.close()
            total_rows += part_rows
            print(f"    [{i}/{len(sorted_parts)}] {p.name} ({len(part_cols)} cols): "
                  f"{part_rows:,} rows (total {total_rows:,})", flush=True)

    print(f"  wrote {out_path.relative_to(REPO_ROOT)}  "
          f"({total_rows:,} rows across {len(sorted_parts)} partitions)")
    return []
