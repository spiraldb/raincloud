---
name: raincloud-debug-build
description: Diagnostic checklist for a failing build — isolate which stage broke. Use when a build errors out, when validation fails, when fetch returns 403, or any time the user needs to triage a pipeline failure.
argument-hint: <slug>
---

Triage a failing `/raincloud-build <slug>`. Reference: [SKILLS.md "Debugging a failing build"](../../context/SKILLS.md#debugging-a-failing-build).

Walk these in order — stop as soon as the cause is found:

1. **Is it a row-count mismatch in validate?** If running with `--strict`, omit it to use the default warning behavior:

   ```bash
   python -m raincloud.pipeline.build <slug>
   ```

   If the build now succeeds, update `expect.rows` in `sources.json` to the actual count.

2. **Isolate the stage** — invoke each independently to see exactly where it breaks:

   ```bash
   python -m raincloud.pipeline.fetch <slug>
   python -m raincloud.pipeline.extract <slug>
   ```

   Pass these stage commands slugs only (or `--all`); build flags such as `--strict` are not theirs. Once extract runs cleanly, use `/raincloud-build` for parse/transform/canonical validation. `python -m raincloud.pipeline.export <slug>` separately re-derives Parquet/Vortex from the canonical Arrow already on disk.

3. **Check the selected raw cache.** `raincloud config show` identifies configured roots. `raincloud.pipeline.spec.raw_slug_dir(slug)` resolves the selected catalog/fetch generation. Fetch skips matching cached payloads. If invalidating a cache, remove only that generation's upstream payloads; preserve sibling `.recipes/` generations and Raincloud metadata.

4. **Check the configured scratch directory** — build scratch is under `.recipes/<recipe-hash>/<slug>/`. If a handler complains "no .xxx files", look here first to see what was actually unpacked.

5. **DuckDB OOM / swap thrash** — cap memory and redirect spill:

   ```bash
   RAINCLOUD_DUCKDB_MEMORY_LIMIT=8GB \
   RAINCLOUD_DUCKDB_TEMP_DIRECTORY=/mnt/scratch/duckdb-tmp \
     python -m raincloud.pipeline.build <slug>
   ```

   Default DuckDB memory_limit (~80% of system RAM) can swap-thrash on heavily-nested VARIANT shredding. See [AGENTS.md "Data locations"](../../context/AGENTS.md#data-locations).

6. **Kaggle 403** — see `/raincloud-add-kaggle-tos`. The fetch error message points at the URL to click through.

7. **Vortex failure on export** — the build does not fail: it records the failure as Vortex's "unavailable" measurement and prints `[unavailable] <slug>/vortex`. `raincloud describe <slug>` quotes the recorded error and toolchain; compare with current conformance results for the installed writer, since type support depends on that version. A writer that loops is stopped by `RAINCLOUD_EXPORT_TIMEOUT`. After an upgrade, re-run just that step with `/raincloud-export <slug> --format vortex`; a success replaces the measurement. See [SKILLS.md](../../context/SKILLS.md#emitting-a-vortex-file-alongside-the-parquet).

When in doubt, prefer Read → Grep over guessing. The pipeline has hidden contracts (streaming handlers returning `[]`, raw_downloads being unversioned, VARIANT requiring v1.5.0) that aren't obvious from any single file — see [AGENTS.md](../../context/AGENTS.md#when-youre-unsure).
