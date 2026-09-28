---
name: raincloud-add-handler
description: Walk through writing a new transform handler under raincloud/pipeline/handlers/. Use when the default tighten_types/identity paths can't produce the right shape — row-level JSON parsing, streaming to avoid OOM, multi-output splitting, or VARIANT-from-the-start.
argument-hint: [<handler-name>]
---

Guide the user through adding a transform handler. Reference: [SKILLS.md "Adding a new transform handler"](../../context/SKILLS.md#adding-a-new-transform-handler) and ["Writing a streaming handler"](../../context/SKILLS.md#writing-a-streaming-handler).

When this is the right pattern: the default `tighten_types` / `identity` paths can't produce the right shape — e.g. the source needs row-level JSON parsing, streaming to avoid OOM, multi-output splitting (one upstream → many slugs), or VARIANT columns emitted from the start.

Steps:

1. **Create the handler** at `raincloud/pipeline/handlers/<name>.py`. Signature:

   ```python
   def <name>(spec: dict, parsed: list[tuple[Path, pa.Table | BatchStream | None]], **params
              ) -> list[tuple[str, pa.Table | BatchStream]]:
       ...
   ```

   - `parsed` contains one `(path, table)` tuple per parsed file; `table` is a `BatchStream` for a reader the handler declares with `batches.batch_input`, and `None` when `parse.reader = "custom"`.
   - Return `[(output_slug, table), ...]` — one tuple per canonical output, a `Table` or a fixed-schema `BatchStream`. Multi-output handlers emit several slugs from one source (see `glove_split`, `osm_pbf_split`, `stack_exchange_split`).
   - **Streaming handlers** write canonical Arrow IPC incrementally with `canonical.open_canonical_writer`, then return `[]`. The builder validates the canonical and derives exports from it. Copy [`templates/streaming_handler.py.tmpl`](../../../templates/streaming_handler.py.tmpl); it demonstrates configured scratch paths, `duckdb_connect`, Arrow batches and cleanup. Study `lichess_pgn_parse`, `jsonbench_variant_parse`, and `wikipedia_variant_parse` for other upstream shapes. Run it only through the builder: `build.run_one` scopes `workdir_root()` to the recipe, registers the canonical the writer produces so it is published atomically, holds the operation lock and writes the build record.

2. **Declare** it in `raincloud/_registry.py`:

   ```python
   # raincloud/_registry.py
   HANDLERS: dict[str, str] = {
       ...
       "<name>": "<name>:<name>",   # "<module>:<attr>" under handlers/
   }
   ```

   That is the only place to add it. The registry imports the module on demand
   and the catalog capability list is derived from the same declaration, so
   there is no second file to keep in step.

3. **Wire it into the manifest** — set `"transform": { "handler": "<name>", "params": { ... } }` on the relevant spec.

4. **Validate** — `python -m raincloud.pipeline.validate_manifest` checks that every handler name in the manifest is declared in `HANDLERS` and warns on orphans (declared but unreferenced); `pytest tests/test_manifest.py` checks that every `HANDLERS` entry imports (`test_handlers_all_import`; the `_REGISTRY` in `handlers/__init__.py` is derived from `HANDLERS`). Seconds; catches typos before paying for a fetch.

5. **Build the dataset** — invoke `/raincloud-build <slug>` to validate end-to-end.

Style: handlers stay short (most under 150 lines). Reuse `open_canonical_writer` from `raincloud.pipeline.canonical`, `duckdb_connect` from `raincloud`, and `workdir_root`, `spec_field` from `raincloud.pipeline.spec`. Don't shoehorn a new shape into `tighten_types` or `identity` — write a dedicated handler.

For VARIANT-from-the-start handlers, parse the JSON before casting — `CAST(CAST(col AS JSON) AS VARIANT)` for a JSON text column, `CAST(to_json(col) AS VARIANT)` for a DuckDB struct — in the ingestion query, then `duckdb_variant.to_canonical_arrow` (small tables) or `stream_canonical_arrow` (batches) to preserve the shredded representation and field marker. `to_canonical_arrow` projects each VARIANT column through `variant_to_parquet_variant(...)` for you; for `stream_canonical_arrow`, project each one in the SQL yourself (see `wikipedia_variant_parse`). `duckdb_connect(db_path)` applies `storage_compatibility_version=v1.5.0` automatically. See `factbook_variant_parse` and `wikipedia_variant_parse`.
