---
name: raincloud-build
description: Run the full Raincloud pipeline (fetch → extract → parse → transform → write → validate → convert) for one or more dataset slugs. Use when the user asks to build a dataset, rebuild a slug, or process a family.
argument-hint: <slug>... | --family <name> | --all  [--loose] [--clean-workdir]
disable-model-invocation: true
allowed-tools: Bash(python -m scripts.pipeline.build *)
---

Run the Raincloud build orchestrator with the user's args:

```bash
python -m scripts.pipeline.build $ARGUMENTS
```

Selection (at least one required):
- `<slug>...` — positional dataset slugs (any number)
- `--family <name>` — every dataset in a family (`direct`, `kaggle-upstream`, `nyc-tlc`, `public-bi`, `uci`)
- `--all` — every dataset in `sources.json`

Modifiers:
- `--loose` — downgrade `expect.rows` mismatches from errors to warnings. Use on the first build of a new slug before you know the exact row count.
- `--clean-workdir` — wipe `_workdir/<slug>/` after each successful build. Essential for whole-family runs (Public BI decompressed CSVs can hit ~100 GB).

Before running:
- **Confirm with the user** before triggering anything non-trivial. JSONBench 100M ≈ 6 h, Wikipedia Structured Contents → 34 GB parquet, OSM Germany ~45 min per kind. Small (<100 MB) parquets are fine without asking. (See [AGENTS.md "Rebuilding is expensive"](../../context/AGENTS.md).)
- For large builds, set `RAINCLOUD_DUCKDB_MEMORY_LIMIT` and `RAINCLOUD_DUCKDB_TEMP_DIRECTORY` — see `/raincloud-large-build` for the full pattern.
- For Kaggle/HF datasets, ensure `uv sync --extra kaggle` (or `--extra huggingface`) was run.

After a successful build, suggest running `/raincloud-docs` to regenerate derived docs.

Context: [SKILLS.md](../../context/SKILLS.md), [AGENTS.md](../../context/AGENTS.md).
