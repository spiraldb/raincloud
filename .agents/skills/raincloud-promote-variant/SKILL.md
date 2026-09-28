---
name: raincloud-promote-variant
description: Promote a JSON column to VARIANT by changing the transform recipe and rebuilding. Use when the user wants to upgrade a JSON-annotated string column to VARIANT.
argument-hint: [<slug>...]
---

Promote a JSON column to VARIANT. Reference: [SKILLS.md "Promoting a JSON column to VARIANT"](../../context/SKILLS.md#promoting-a-json-column-to-variant).

Update the transform recipe and rebuild. Use `/raincloud-add-handler`: build VARIANT in DuckDB, bridge through `duckdb_variant.to_canonical_arrow` or `stream_canonical_arrow`, then write canonical Arrow. Exporters derive sibling formats from that spine; there is no in-place pass over existing files.
- Cast parsed JSON, `CAST(CAST(col AS JSON) AS VARIANT)`; a bare text-to-VARIANT cast stores the JSON as one string.
- `to_canonical_arrow` projects each VARIANT column through `variant_to_parquet_variant(...)` itself; with `stream_canonical_arrow`, the SQL must do that projection (see `jsonbench_variant_parse`).
- Both bridges stamp through `variant.attach_variant[_schema]`, which declares the storage struct's `metadata` (and an unshredded `value`) non-nullable, as the Parquet VARIANT spec requires, and fails on a present VARIANT missing either. Never set the `VARIANT_EXT` marker by hand.
- Simplest 1-column example: `factbook_variant_parse`.
- Multi-column with typed siblings: `wikipedia_variant_parse`.
- VARIANT requires a persistent DuckDB DB opened at `storage_compatibility_version=v1.5.0` — `raincloud.duckdb_connect(db_path)` applies this automatically. **Never** call `duckdb.connect(...)` directly.

After promoting, regenerate docs via `python -m raincloud.pipeline.docs`.

Caveat for vortex sibling files: VARIANT columns surface as their shredded struct on the round-trip (the VARIANT logical annotation isn't preserved) — this can blow up the `.vortex` size relative to the parquet on heavily-nested datasets.
