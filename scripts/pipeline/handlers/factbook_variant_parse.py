# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Walk the CIA World Factbook JSON dump (factbook/factbook.json on GitHub),
one JSON file per country, and emit a single parquet with schema:

    region        : VARCHAR  (e.g. 'AFRICA', 'EUROPE', 'MIDDLE-EAST')
    country_code  : VARCHAR  (e.g. 'au', 'fr')
    data          : VARIANT  (the per-country JSON as a Parquet VARIANT column)

VARIANT is the motivation — country schemas vary wildly (some have an
"Economy" section, some don't, some have country-specific fields), which
is the textbook use case. Written via DuckDB's VARIANT cast + COPY TO
PARQUET since pyarrow 23.x doesn't expose VARIANT natively; the on-disk
encoding is the shredded `struct<metadata binary, value binary, typed_value ...>`
format Parquet 2.10+ specifies.

Streams directly to `outputs/prepared/<slug>.parquet` and returns [] so the
normal write stage is a no-op.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from ..spec import duckdb_connect


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

    from ..spec import display_path, output_format_dir, spec_field
    out_path = output_format_dir(spec["slug"], "parquet") / spec_field(spec, "write.output", f"{spec['slug']}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    compression = spec_field(spec, "write.compression", "zstd")

    con = duckdb_connect()
    con.execute("""
        CREATE TABLE facts (
            region VARCHAR,
            country_code VARCHAR,
            data VARIANT
        )
    """)
    con.executemany(
        "INSERT INTO facts VALUES (?, ?, CAST(? AS VARIANT))",
        records,
    )
    con.execute(
        f"COPY facts TO '{out_path}' (FORMAT PARQUET, COMPRESSION '{compression}')"
    )
    con.close()
    print(f"  wrote {display_path(out_path)}")
    return []
