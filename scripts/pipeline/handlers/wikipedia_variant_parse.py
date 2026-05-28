# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse the Wikimedia Foundation *Wikipedia Structured Contents* dump into
a parquet with two VARIANT columns.

Source (Kaggle): `wikimedia-foundation/wikipedia-structured-contents`. The zip
unpacks into two JSONL directories — `enwiki_namespace_0/` and
`frwiki_namespace_0/` — each holding ~54 partitioned `*.jsonl` files, one
JSON object per line per article (~106 GB combined, ~10.1 M articles).

Output schema (top-level, DuckDB view):

    name                : VARCHAR
    url                 : VARCHAR
    identifier          : BIGINT
    abstract            : VARCHAR
    description         : VARCHAR
    date_created        : TIMESTAMP
    date_modified       : TIMESTAMP
    event               : STRUCT(identifier UUID, ...)
    version             : STRUCT(...)
    in_language         : STRUCT(identifier VARCHAR)
    is_part_of          : STRUCT(identifier VARCHAR, url VARCHAR)
    image               : STRUCT(content_url VARCHAR, width BIGINT, height BIGINT)
    main_entity         : STRUCT(identifier VARCHAR, url VARCHAR)
    license             : STRUCT(...)[]
    additional_entities : STRUCT(...)[]
    sections            : VARIANT   <-- heterogeneous recursive tree
    infoboxes           : VARIANT   <-- heterogeneous recursive tree

`sections` and `infoboxes` are modelled as VARIANT because their sub-schemas
vary wildly per article (nesting depth, which keys exist, list-vs-scalar at
each level). The typed-column fields above are inferred by DuckDB's
`read_json` from a file sample and have consistent shapes across articles.

Written via DuckDB's `CAST(to_json(...) AS VARIANT)` + `COPY TO PARQUET`
path — same approach as `factbook_variant_parse` and
`jsonbench_variant_parse`. VARIANT requires a persistent DuckDB database
at `storage_compatibility_version = v1.5.0`, so we stage it under
`_workdir/<slug>/variant.db` and clean up on success.

Returns `[]` because the parquet is fully written by the time the handler
returns; the normal `write` stage is a no-op.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa

from ..spec import duckdb_connect


def wikipedia_variant_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]]
                            ) -> list[tuple[str, pa.Table]]:
    jsonl_files = sorted(p for p, _ in parsed if str(p).endswith(".jsonl") and p.is_file())
    if not jsonl_files:
        raise ValueError("wikipedia_variant_parse: no .jsonl files in extracted output")

    # Build a glob pattern rooted at the common parent so DuckDB reads all
    # partitions in one plan. Both en/fr sit under raw_downloads/<slug>/.
    common_root = jsonl_files[0].parents[1]
    glob_pattern = str(common_root / "*" / "*.jsonl")
    print(f"  reading {len(jsonl_files)} JSONL files under {common_root.name}/ "
          f"({sum(p.stat().st_size for p in jsonl_files) / 1e9:.1f} GB raw)")

    from ..spec import display_path, output_format_dir, spec_field, workdir_root
    out_path = output_format_dir(spec["slug"], "parquet") / spec_field(spec, "write.output", f"{spec['slug']}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    compression = spec_field(spec, "write.compression", "zstd")

    workdir = workdir_root() / spec["slug"]
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "variant.db"
    if db_path.exists(): db_path.unlink()

    con = duckdb_connect(db_path)
    try:
        # `sample_size=-1` forces DuckDB to scan every row when inferring the
        # schema — necessary because rare top-level fields (e.g. `event.fail_count`,
        # `version.tags`) appear only in a small fraction of records but we want
        # them typed in the output. `union_by_name=true` is required across the
        # en/fr partitions, which differ in which top-level fields are ever set.
        # `sections` and `infoboxes` are downgraded to JSON strings *before*
        # DuckDB introspects them — without this, schema inference on their
        # heterogeneous recursive structures fails with "unknown key" errors
        # on articles whose trees include fields the sample didn't contain.
        con.execute(f"""
            COPY (
                SELECT
                    * EXCLUDE (sections, infoboxes),
                    CAST(to_json(sections)  AS VARIANT) AS sections,
                    CAST(to_json(infoboxes) AS VARIANT) AS infoboxes
                FROM read_json(
                    '{glob_pattern}',
                    format = 'newline_delimited',
                    maximum_object_size = 100000000,
                    sample_size = -1,
                    union_by_name = true
                )
            ) TO '{out_path}' (FORMAT PARQUET, COMPRESSION '{compression}')
        """)
    finally:
        con.close()
        # Tear down the persistent DB — it can grow to tens of GB of spill.
        if db_path.exists(): db_path.unlink()
        wal = db_path.with_suffix(db_path.suffix + ".wal")
        if wal.exists(): wal.unlink()
        # DuckDB also writes temp/ directories alongside the DB
        for d in workdir.iterdir():
            if d.is_dir() and d.name.endswith(".tmp"):
                shutil.rmtree(d, ignore_errors=True)

    print(f"  wrote {display_path(out_path)}")
    return []
