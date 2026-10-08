---
name: raincloud-export
description: Re-derive a dataset's Parquet, Vortex, ORC, Avro or Nimble files from the canonical Arrow file already on disk, without refetching or re-transforming. Use when a change touches only the export stage (row-group sizing, a codec, an encoder setting such as a Parquet page index, a writer upgrade) or to refresh one format.
argument-hint: <slug>... | --all  [--format parquet|vortex|orc|avro|nimble|parquet@rs|...] [--dry-run] [--retry-errors]
disable-model-invocation: true
allowed-tools: Bash(python -m raincloud.pipeline.export *)
---

Run the export-only entrypoint:

```bash
python -m raincloud.pipeline.export $ARGUMENTS
```

Selection (one required):
- `<slug>...` — positional dataset slugs.
- `--all` — every dataset. Hours of work on a full store; confirm first.

Modifiers:
- `--format FORMAT` (repeatable) — export only this format, replacing the install's formats for this run: `parquet`, `vortex`, `orc`, `avro` or `nimble`. The writer is chosen by `export.priority` as in a build. It overrides a dataset whose policy leaves the format out, so check `python -m raincloud.pipeline.list_datasets --no-vortex --json` before forcing Vortex.
- `--format parquet@rs` (or another `<format>@<writer>`) — use that writer for this run. The file is still `parquet/<slug>.parquet`, but its bytes and sha256 change, so it no longer matches the catalog until a maintainer regenerates it. Confirm before doing this to datasets others read.
- `--dry-run` — list what would be exported and exit.
- Encoder settings come from the environment (`RAINCLOUD_PARQUET_PAGE_INDEX=1`, `RAINCLOUD_PARQUET_STATISTICS_COLUMNS=100`, `RAINCLOUD_PARQUET_COMPRESSION_LEVEL=9`, ...): this is the cheapest way to rewrite a file with them, since the canonical is the input. A writer that cannot honour a set one refuses (`[unavailable]`, `<writer> cannot honour <VARIABLE>=...`); name another with `--format <fmt>@<writer>`. See `/raincloud-write-settings`.
- `--retry-errors` — attempt a format even when its writer, with this toolchain, already failed to write it at this recipe (see below).

Behavior:
- Reads `outputs/v{n}/<slug>/arrow/<slug>.arrow.zstd` and never rewrites it. A slug with no canonical is reported (`[no canonical]`) and skipped, never built; use `/raincloud-build` for it. So is a canonical this install built from an earlier recipe (fetch, parse or transform changed since), and one that is neither this install's build nor the catalog's file, whose exports could not be recorded: rebuild either (the slug is reported `[failed]`). A change that touches only the export stage needs no rebuild.
- An unknown slug exits 2 with a did-you-mean before any work; `--all` skips hydrated datasets.
- Records each file it writes in the build record (`<data_dir>/builds.json`), as a build does, so the loader serves the new file.
- Every export is bounded by `RAINCLOUD_EXPORT_TIMEOUT` (default 6 h; `0` disables it). When the planned writer raises, dies, reports a failed round-trip or runs out of time, the previous file comes back and the failure is recorded as the format's "unavailable" measurement (`[unavailable] <slug>/<fmt>`), unless the previous file is this install's export of the same canonical, which stays. Without `--format` the run goes on and exits 0; with `--format` the request failed and it exits 1. A writer named outright (`--format vortex@rs`) that fails exits 1 and records nothing. A later successful export replaces the measurement.
- A failure already recorded is not repeated. When the measurement that applies (this install's build record at the recipe, else the catalog's) names the writer that would run -- planned, or named with `--format` -- with the same toolchain versions and the same canonical, the export prints `[skip] <slug>/<fmt>: <cell> (<toolchain>) failed at this recipe on <date>: <error>; pass --retry-errors to try again` and records nothing. Without `--format` the slug still counts as exported and the summary lists the skip (exit 0); with `--format` the requested file was not produced (exit 1). A changed toolchain or another writer is attempted, with a `[retry]` line saying what changed. `--retry-errors` attempts it anyway: success replaces the measurement, a planned writer's failure records it again.

After exporting, regenerate derived docs with `/raincloud-docs` if the catalog should record the new files.

Context: [AGENTS.md "How a build works"](../../context/AGENTS.md#how-a-build-works).
