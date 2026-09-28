# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Walk the CIA World Factbook JSON dump (factbook/factbook.json on GitHub),
one JSON file per country, and emit a single table with schema:

    region        : VARCHAR  (e.g. 'AFRICA', 'EUROPE', 'MIDDLE-EAST')
    country_code  : VARCHAR  (e.g. 'au', 'fr')
    data          : VARIANT  (the per-country JSON, parsed: an OBJECT variant)

VARIANT is the motivation — country schemas vary wildly (some have an
"Economy" section, some don't, some have country-specific fields), which
is the textbook use case. The VARIANT column is built in DuckDB (the one
engine that holds a true VARIANT), then bridged to canonical Arrow with
`duckdb_variant.to_canonical_arrow`: each VARIANT is projected through
`variant_to_parquet_variant(...)` to the shredded
`struct<metadata, value, ...>` Arrow can carry, and stamped with
`arrow.parquet.variant` (its `metadata` / `value` declared non-nullable, as the
VARIANT specs require) so it survives the canonical `.arrow.zstd` round-trip.

Returns the single materialized table (~260 rows) so the slug flows the normal
TABLE export path (arrow → parquet + vortex). The Parquet export loses the
VARIANT *logical* type (pyarrow can't emit it — the exporter records that as
`variant_faithful=False`); the shredded struct itself is preserved.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from raincloud import duckdb_connect

from ..duckdb_variant import to_canonical_arrow


def factbook_variant_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]]
                            ) -> list[tuple[str, pa.Table]]:
    json_files = [p for p, _ in parsed if str(p).endswith(".json") and p.is_file()]
    if not json_files:
        raise ValueError("factbook_variant_parse: no .json files in extracted output")

    records = []
    for path in sorted(json_files):
        # Path is something like _workdir/<slug>/factbook.json-master/AFRICA/au.json
        region = path.parent.name
        if region in ("factbook.json-master", ""): continue  # skip top-level README etc.
        country_code = path.stem
        with open(path, "r", encoding="utf-8") as f:
            json_text = f.read()
        records.append((region, country_code, json_text))
    print(f"  {len(records)} country records across "
          f"{len({r for r, _, _ in records})} regions")

    con = duckdb_connect()
    try:
        con.execute("""
            CREATE TABLE facts (
                region VARCHAR,
                country_code VARCHAR,
                data VARIANT
            )
        """)
        # Through JSON, so each VARIANT holds the parsed country object; the
        # text cast straight to VARIANT would store it as one string.
        con.executemany(
            "INSERT INTO facts VALUES (?, ?, CAST(CAST(? AS JSON) AS VARIANT))",
            records,
        )
        table = to_canonical_arrow(con, "facts")
    finally:
        con.close()
    return [(spec["slug"], table)]
