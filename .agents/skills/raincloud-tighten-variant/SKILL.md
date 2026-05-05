---
name: raincloud-tighten-variant
description: In-place promote JSON-annotated string columns to VARIANT in built parquets. Use when a parquet already exists and you want to upgrade its JSON columns without rerunning the full pipeline.
argument-hint: [<slug>...] [--chunked] [--dry-run]
disable-model-invocation: true
allowed-tools: Bash(python -m scripts.pipeline.tighten_variant *)
---

Run the JSON → VARIANT in-place tightening pass:

```bash
python -m scripts.pipeline.tighten_variant $ARGUMENTS
```

Selection:
- *(no slugs)* — every parquet under `outputs/v*/` is considered.
- `<slug>...` — only those specific slugs.

Modifiers:
- `--dry-run` — report which parquets *would* be rewritten, without touching them.
- `--chunked` — force row-group-at-a-time mode. Needed for wide / deeply-nested records where single-shot VARIANT cast spills blow up DuckDB memory (e.g. Open Food Facts).

Behavior:
- Idempotent: parquets already lacking JSON-annotated columns are skipped (reported as "already").
- Uses `CAST(col AS VARIANT)` + atomic tmp-rename via `duckdb_connect` (which applies `storage_compatibility_version=v1.5.0`).
- Memory-hungry on heavily-nested payloads. Set `RAINCLOUD_DUCKDB_MEMORY_LIMIT=96GB` for known-heavy cases (Open Food Facts is the tested ceiling).

When to use:
- After a build that produced a JSON-annotated string column you want to promote without rerunning the full pipeline.
- For new builds that should emit VARIANT from the start, write a streaming handler instead — see `/raincloud-add-handler` and [SKILLS.md](../../context/SKILLS.md#promoting-a-json-column-to-variant).

After this runs, suggest `/raincloud-docs columns_parquet coverage_parquet` to refresh the parquet-side derived docs.
