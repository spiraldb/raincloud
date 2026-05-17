---
name: raincloud-large-build
description: Run a memory- or runtime-heavy build safely with memory caps, scratch redirection, nohup, and progress logging. Use for multi-hour or multi-GB builds (JSONBench 100M, Wikipedia Structured Contents, OSM Germany, Public BI batches).
argument-hint: <slug>... [--all] [--loose] [--clean-workdir]
disable-model-invocation: true
---

Run a memory- or runtime-heavy build with the safety knobs enabled. Reference: [SKILLS.md "Running a large build safely"](../../context/SKILLS.md#running-a-large-build-safely).

**Confirm with the user before triggering.** Observed timings: JSONBench 100M ≈ 6 h, Wikipedia Structured Contents → 34 GB parquet (multi-hour), OSM Germany ≈ 45 min per kind. Rebuilding wipes and redoes the existing `outputs/v1/<slug>/<slug>.parquet`+`.vortex` pair on disk.

Pattern (adjust the slug, memory cap, and tempdir to the user's machine):

```bash
RAINCLOUD_DUCKDB_MEMORY_LIMIT=32GB \
RAINCLOUD_DUCKDB_TEMP_DIRECTORY=/mnt/scratch/duckdb-tmp \
PYTHONUNBUFFERED=1 \
  nohup python -m scripts.pipeline.build $ARGUMENTS \
    > /tmp/raincloud-build.log 2>&1 &
```

Flag rationale:
- `--loose` — first build of a new slug, before `expect.rows` is known. Downgrades row-count mismatches from errors to warnings.
- `--clean-workdir` — wipe `_workdir/<slug>/` after each successful build. Essential for large batch runs (Public BI decompressed CSVs can hit ~100 GB).
- `RAINCLOUD_DUCKDB_MEMORY_LIMIT` — caps DuckDB's working set; default (~80% of system RAM) can swap-thrash on heavily-nested VARIANT shredding. 96 GB is the tested ceiling for Open Food Facts.
- `RAINCLOUD_DUCKDB_TEMP_DIRECTORY` — point at a large volume; the system tempdir often runs out on big builds.
- `PYTHONUNBUFFERED=1` — log file flushes line-by-line so progress is inspectable mid-run.
- `nohup … &` — survives terminal disconnect.

Monitor:

```bash
tail -f /tmp/raincloud-build.log
du -sh _workdir/<slug>/   # scratch growth
df -h .                   # disk headroom
```

Do **not** invoke this without confirming the slug and timing with the user. Suggest `/raincloud-docs` after success.
