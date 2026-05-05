---
name: raincloud-debug-build
description: Diagnostic checklist for a failing build — isolate which stage broke. Use when a build errors out, when validation fails, when fetch returns 403, or any time the user needs to triage a pipeline failure.
argument-hint: <slug>
---

Triage a failing `/raincloud-build $ARGUMENTS`. Reference: [SKILLS.md "Debugging a failing build"](../../context/SKILLS.md#debugging-a-failing-build).

Walk these in order — stop as soon as the cause is found:

1. **Is it a row-count mismatch in validate?** Re-run with `--loose` to convert the error to a warning:

   ```bash
   python -m scripts.pipeline.build $ARGUMENTS --loose
   ```

   If the build now succeeds, update `expect.rows` in `sources.json` to the actual count.

2. **Isolate the stage** — invoke each independently to see exactly where it breaks:

   ```bash
   python -m scripts.pipeline.fetch $ARGUMENTS
   python -m scripts.pipeline.extract $ARGUMENTS
   ```

   (`parse`, `transform`, `write`, `validate`, `convert` are not standalone CLIs — once `extract` runs cleanly, fall through to `/raincloud-build`.)

3. **Check the raw cache** at `outputs/raw_downloads/<slug>/`. Fetch is idempotent and skips when `expected_bytes` / `expected_sha256` already match. If you suspect a stale or corrupt download, `rm -rf outputs/raw_downloads/<slug>/` and re-fetch.

4. **Check `_workdir/<slug>/`** — extract stage output. If a handler complains "no .xxx files", look here first to see what was actually unpacked.

5. **DuckDB OOM / swap thrash** — cap memory and redirect spill:

   ```bash
   RAINCLOUD_DUCKDB_MEMORY_LIMIT=8GB \
   RAINCLOUD_DUCKDB_TEMP_DIRECTORY=/mnt/scratch/duckdb-tmp \
     python -m scripts.pipeline.build $ARGUMENTS --loose
   ```

   Default DuckDB memory_limit (~80% of system RAM) can swap-thrash on heavily-nested VARIANT shredding. See [README.md "DuckDB resource limits"](../../context/README.md#duckdb-resource-limits).

6. **Kaggle 403** — see `/raincloud-add-kaggle-tos`. The fetch error message points at the URL to click through.

7. **Vortex panic on convert** — known type-support gaps in vortex 0.69 (FixedSizeBinary(16), VARIANT round-trip blow-up). See `/raincloud-convert` and [SKILLS.md](../../context/SKILLS.md#emitting-a-vortex-file-alongside-the-parquet).

When in doubt, prefer Read → Grep over guessing. The pipeline has hidden contracts (streaming handlers returning `[]`, raw_downloads being unversioned, VARIANT requiring v1.5.0) that aren't obvious from any single file — see [AGENTS.md](../../context/AGENTS.md#when-youre-unsure).
