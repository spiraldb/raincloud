# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stream Wikipedia's Parquet shards into canonical Arrow with two VARIANTs.

The upstream archive contains enwiki/data/ and frwiki/data/ Parquet partitions.
Typed columns are reconciled by name across all selected shards. `sections`
and `infoboxes` contain JSON text: cast through JSON before VARIANT so arrays,
objects and JSON null remain values rather than becoming quoted text strings.
A NULL upstream cell is a null VARIANT struct; a present one always carries its
`metadata` and `value`, which the stamp declares non-nullable (`variant`).
Other columns retain their upstream meaning, including JSON text in `tables`.

Returns [] after writing canonical Arrow; the common build tail validates and
exports it. DuckDB and the canonical writer use bounded batches, with scratch
owned by this invocation and removed on success or failure.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pyarrow as pa

from raincloud import duckdb_connect

from ..batches import BatchLimits, split_batch
from ..canonical import open_canonical_writer
from ..duckdb_variant import stream_canonical_arrow
from ..spec import workdir_root


def wikipedia_variant_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]]
                            ) -> list[tuple[str, pa.Table]]:
    files = sorted(p for p, _ in parsed if p.suffix == ".parquet" and p.is_file())
    if not files:
        raise ValueError("wikipedia_variant_parse: no .parquet files in extracted output")
    limits = BatchLimits.from_env()
    workdir = workdir_root() / spec["slug"]
    workdir.mkdir(parents=True, exist_ok=True)
    print(f"  reading {len(files)} selected Parquet shards", flush=True)
    with tempfile.TemporaryDirectory(prefix="wikipedia-variant-", dir=workdir) as tmp:
        con = duckdb_connect(Path(tmp) / "variant.db")
        try:
            # Pass exact filenames through DuckDB's relation API. No glob can
            # pull in unrelated shards, and quotes in paths stay literal.
            con.read_parquet([str(p) for p in files], union_by_name=True,
                             hive_partitioning=False).create_view("wiki_input")
            sql = """
                SELECT * EXCLUDE (sections, infoboxes),
                    CASE WHEN sections IS NULL THEN NULL ELSE
                        variant_to_parquet_variant(CAST(CAST(sections AS JSON) AS VARIANT)) END AS sections,
                    CASE WHEN infoboxes IS NULL THEN NULL ELSE
                        variant_to_parquet_variant(CAST(CAST(infoboxes AS JSON) AS VARIANT)) END AS infoboxes
                FROM wiki_input
            """
            schema, batches = stream_canonical_arrow(
                con, sql, ["sections", "infoboxes"], batch_size=limits.rows
            )
            total = 0
            with open_canonical_writer(spec["slug"], schema) as writer:
                for decoded in batches:
                    for batch in split_batch(decoded, limits):
                        writer.write_batch(batch)
                        total += batch.num_rows
            print(f"  wrote {spec['slug']} canonical Arrow ({total:,} rows)", flush=True)
        finally:
            con.close()
    return []
