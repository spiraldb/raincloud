---
name: raincloud-promote-variant
description: Pick the right path (in-place tightening vs new-build streaming handler) for promoting a JSON column to VARIANT. Use when the user wants to upgrade a JSON-annotated string column to VARIANT and isn't sure whether to rewrite an existing parquet or change the build path.
argument-hint: [<slug>...]
---

Pick the right VARIANT-promotion path. Reference: [SKILLS.md "Promoting a JSON column to VARIANT"](../../context/SKILLS.md#promoting-a-json-column-to-variant).

**If the parquet already exists** (in-place tightening): use `/raincloud-tighten-variant`.
- Idempotent — parquets already lacking JSON-annotated columns are skipped.
- For heavily-nested payloads (e.g. Open Food Facts), set `RAINCLOUD_DUCKDB_MEMORY_LIMIT=96GB` (tested ceiling).
- Add `--chunked` if a single-shot cast spills/OOMs.

**If you're writing a new dataset and want VARIANT from the start**: use `/raincloud-add-handler` to create a streaming handler that uses DuckDB's `CAST(to_json(col) AS VARIANT)` inside the `COPY ... TO PARQUET` statement.
- Simplest 1-column example: `factbook_variant_parse`.
- Multi-column with typed siblings: `wikipedia_variant_parse`.
- VARIANT requires a persistent DuckDB DB opened at `storage_compatibility_version=v1.5.0` — `duckdb_connect(db_path)` applies this automatically. **Never** call `duckdb.connect(...)` directly.

After promoting, regenerate docs via `/raincloud-docs columns_parquet coverage_parquet`.

Caveat for vortex sibling files: VARIANT columns surface as their shredded struct on the round-trip (the VARIANT logical annotation isn't preserved) — this can blow up the `.vortex` size relative to the parquet on heavily-nested datasets.
