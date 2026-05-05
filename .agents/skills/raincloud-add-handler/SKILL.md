---
name: raincloud-add-handler
description: Walk through writing a new transform handler under scripts/pipeline/handlers/. Use when the default tighten_types/identity paths can't produce the right shape — row-level JSON parsing, streaming to avoid OOM, multi-output splitting, or VARIANT-from-the-start.
argument-hint: [<handler-name>]
---

Guide the user through adding a transform handler. Reference: [SKILLS.md "Adding a new transform handler"](../../context/SKILLS.md#adding-a-new-transform-handler) and ["Writing a streaming handler"](../../context/SKILLS.md#writing-a-streaming-handler).

When this is the right pattern: the default `tighten_types` / `identity` paths can't produce the right shape — e.g. the source needs row-level JSON parsing, streaming to avoid OOM, multi-output splitting (one upstream → many slugs), or VARIANT columns emitted from the start.

Steps:

1. **Create the handler** at `scripts/pipeline/handlers/<name>.py`. Signature:

   ```python
   def <name>(spec: dict, parsed: list[tuple[Path, pa.Table | None]], **params
              ) -> list[tuple[str, pa.Table]]:
       ...
   ```

   - `parsed` contains one `(path, table)` tuple per parsed file; `table` is `None` when `parse.reader = "custom"`.
   - Return `[(output_slug, table), ...]` — one tuple per output parquet. Multi-output handlers emit several slugs from one source (see `glove_split`, `osm_pbf_split`, `stack_exchange_split`).
   - **Streaming handlers** (write direct to parquet, bypass the write stage) return `[]`. Copy [`examples/streaming_handler.py.tmpl`](../../../examples/streaming_handler.py.tmpl) as the starting point — it has the `outputs_root()` / `duckdb_connect()` / cleanup wiring already shaped — and study `factbook_variant_parse`, `wikipedia_variant_parse`, `lichess_pgn_parse` for upstream-shape variations.

2. **Register** in `scripts/pipeline/handlers/__init__.py`:

   ```python
   from .<name> import <name>
   _REGISTRY = {
       ...
       "<name>": <name>,
   }
   ```

3. **Wire it into the manifest** — set `"transform": { "handler": "<name>", "params": { ... } }` on the relevant spec.

4. **Run the test suite** — `pytest` checks that every handler in `_REGISTRY` imports and that there are no orphans (registered but unreferenced) or unregistered handler names in the manifest. Sub-second; catches typos before paying for a fetch.

5. **Build the dataset** — invoke `/raincloud-build <slug> --loose` to validate end-to-end.

Style: handlers stay short (most under 150 lines). Reuse `duckdb_connect`, `outputs_root`, `spec_field` from `scripts.pipeline.spec` rather than reimplementing. Don't shoehorn a new shape into `tighten_types` or `identity` — write a dedicated handler.

For VARIANT-from-the-start handlers, use DuckDB's `CAST(to_json(col) AS VARIANT)` inside `COPY ... TO PARQUET`. `duckdb_connect(db_path)` applies `storage_compatibility_version=v1.5.0` automatically. See `factbook_variant_parse` (1-column) and `wikipedia_variant_parse` (multi-column with typed siblings).
