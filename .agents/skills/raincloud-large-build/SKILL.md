---
name: raincloud-large-build
description: Run a memory- or runtime-heavy build safely with memory caps, scratch redirection, nohup, and progress logging. Use for multi-hour or multi-GB builds (JSONBench 100M, Wikipedia Structured Contents, OSM Germany, Public BI batches).
argument-hint: <slug>... [--all] [--strict] [--clean-workdir] [--retry-errors]
disable-model-invocation: true
---

Run a memory- or runtime-heavy build with the safety knobs enabled. Reference: [SKILLS.md "Running a large build safely"](../../context/SKILLS.md#running-a-large-build-safely).

**Confirm with the user before triggering.** Observed timings: JSONBench 100M ≈ 6 h, Wikipedia Structured Contents → ~70 GB parquet (multi-hour), OSM Germany ≈ 45 min per kind. Rebuilding wipes and redoes the existing version-scoped canonical Arrow and configured export artifacts on disk.

Pattern (adjust the slug, memory cap, tempdir and log directory to the user's machine). The log goes to a durable directory, never `/tmp`: on many machines `/tmp` is tmpfs, and a reboot during a multi-hour run would delete the only record of a failure. `raincloud config show` reports the data directory; a `logs/` directory beside it is a good default. Not beside the scratch root: outside a checkout that is the user cache, which is disposable.

```bash
LOG_DIR=/path/to/durable/logs
mkdir -p "$LOG_DIR"
RAINCLOUD_DUCKDB_MEMORY_LIMIT=32GB \
RAINCLOUD_DUCKDB_TEMP_DIRECTORY=/mnt/scratch/duckdb-tmp \
PYTHONUNBUFFERED=1 \
  nohup python -m raincloud.pipeline.build $ARGUMENTS \
    > "$LOG_DIR/build-$(date +%s).log" 2>&1 &
```

Flag rationale:
- `--strict` — make validation drift an error. Without it, row-count mismatches are warnings, suitable for first builds with estimated counts.
- `--clean-workdir` — clear the selected `.recipes/<recipe-hash>/<slug>/` scratch directory after each successful build. Essential for large batch runs (Public BI decompressed CSVs can hit ~100 GB).
- `--retry-errors` — attempt a format whose writer, with this toolchain, already failed at this recipe; skipped (`[skip]`) otherwise. Only when the user asks: a writer that hung last time costs `RAINCLOUD_EXPORT_TIMEOUT` again.
- `RAINCLOUD_DUCKDB_MEMORY_LIMIT` — caps DuckDB's working set; default (~80% of system RAM) can swap-thrash on heavily-nested VARIANT shredding. 96 GB is the tested ceiling for Open Food Facts.
- `RAINCLOUD_DUCKDB_TEMP_DIRECTORY` — point at a large volume; the system tempdir often runs out on big builds.
- `PYTHONUNBUFFERED=1` — log file flushes line-by-line so progress is inspectable mid-run.
- `nohup … &` — survives terminal disconnect.

Monitor:

```bash
LOG_DIR=/path/to/durable/logs      # the same directory; a new shell does not inherit it
tail -f "$LOG_DIR"/build-*.log
du -sh <scratch_dir>/.recipes/<recipe-hash>/<slug>/   # selected build scratch
df -h .                   # disk headroom
```

Do **not** invoke this without confirming the slug and timing with the user. Suggest `/raincloud-docs` after success.
