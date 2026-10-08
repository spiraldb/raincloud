---
name: raincloud-build
description: Run the full Raincloud pipeline (fetch → extract → parse → transform → canonical Arrow → validate → exports) for one or more dataset slugs. Use when the user asks to build a dataset, rebuild a slug, or process a batch.
argument-hint: <slug>... | --all  [--format FORMAT] [--only] [--strict] [--clean-workdir] [--retry-errors]
disable-model-invocation: true
allowed-tools: Bash(python -m raincloud.pipeline.build *)
---

Run the Raincloud build orchestrator with the user's args:

```bash
python -m raincloud.pipeline.build $ARGUMENTS
```

Selection (at least one required):
- `<slug>...` — positional dataset slugs (any number)
- `--all` — every dataset in `sources.json` except hydrated ones (name those explicitly)

Modifiers:
- `--strict` — make validation drift an error. By default, row-count mismatches are warnings; use that default for first builds with estimated counts.
- `--clean-workdir` — clear the selected `.recipes/<recipe-hash>/<slug>/` scratch directory after each successful build. Essential for large batch runs (Public BI decompressed CSVs can hit ~100 GB).
- `--retry-errors` — attempt a format even when its writer, with this toolchain, already failed to write it at this recipe (see below). Without it that format is skipped.
- `--format FORMAT` (repeatable or comma-separated) — write these formats instead of the install's `formats` setting (only `vortex` by default; `parquet`, `orc`, `avro`, `nimble`, or `arrow` to keep the canonical). Only the format is taken: a writer suffix (`parquet@rs`) is dropped, and the writer comes from `export.priority`.
- `--only` — for a generated table, build its whole group but keep only the tables named.

Encoder settings — a Parquet page index, statistics for the first N columns, a compression level, dictionaries, page checksums, ORC/Avro codecs, Vortex compact encodings — are environment settings every writer of the format reads (`RAINCLOUD_PARQUET_*`, `RAINCLOUD_ORC_*`, `RAINCLOUD_AVRO_*`, `RAINCLOUD_VORTEX_*`); unset is each library's default. Pass them in the build's environment; see `/raincloud-write-settings` for what each does, which writer refuses which, and how to check the result.

Before running:
- **Confirm with the user** before triggering anything non-trivial. JSONBench 100M ≈ 6 h, Wikipedia Structured Contents → ~70 GB parquet, OSM Germany ~45 min per kind. Small (<100 MB) parquets are fine without asking. (See [AGENTS.md "Confirm before rebuilding"](../../context/AGENTS.md#confirm-before-rebuilding).)
- For large builds, set `RAINCLOUD_DUCKDB_MEMORY_LIMIT` and `RAINCLOUD_DUCKDB_TEMP_DIRECTORY` — see `/raincloud-large-build` for the full pattern.
- A dataset whose format or source needs an extra names it when missing: `osm`, `sas`, `excel`, `archives`, `generated` (TPC-H/TPC-DS), `kaggle`, `huggingface`. Install with `uv sync --extra <name> --inexact`. The `--inexact` flag is important: without it, syncing one extra removes the others.
- An unknown slug exits 2 with a did-you-mean before anything runs. A build missing a format's writer (e.g. no `raincloud[vortex]`) fails in about a second, naming the extra, before it fetches anything.
- A writer that runs but cannot produce its format for the dataset (raises, dies, reports a failed round-trip, or exceeds `RAINCLOUD_EXPORT_TIMEOUT`, default 6 h) does not fail the build: it prints `[unavailable] <slug>/<fmt>`, records the measurement in the build record, and the build exits 0 with the formats that worked. Report those lines to the user; `raincloud describe <slug>` quotes the recorded reason.
- A failure already recorded is not repeated. When the measurement that applies (this install's build record at the recipe, else the catalog's) names the same writer cell, the same toolchain versions and the same canonical, the build prints `[skip] <slug>/<fmt>: <cell> (<toolchain>) failed at this recipe on <date>: <error>; pass --retry-errors to try again`, keeps the measurement, lists it in the summary, and still exits 0. A changed toolchain (an upgraded `vortex-data`, a new sidecar binary) or a rebuilt canonical is attempted on its own, with a `[retry]` line saying what changed. Report skips to the user; only pass `--retry-errors` when they ask to try again with nothing changed (it can cost the full export time limit).

After a successful build, suggest running `/raincloud-docs` to regenerate derived docs.

Context: [SKILLS.md](../../context/SKILLS.md), [AGENTS.md](../../context/AGENTS.md).

## Per-column profiles

Per-column profiles are a separate opt-in stage:
`python -m raincloud.pipeline.profile <slug>` after a build. Not part of the
default build pipeline. See `raincloud-profile` skill.
