---
name: raincloud-list-datasets
description: Filter and list datasets from sources.json without grepping the manifest JSON. Use when the user asks "which slugs use handler X", "show me all UCI datasets", "what's gated behind Kaggle ToS", or any other catalog-shape question that's faster than reading docs/v2/datasets.md end to end.
argument-hint: [--handler <h>] [--license <spdx>] [--fetch-type <t>] [--reader <r>] [--vortex|--no-vortex] [--kaggle-tos] [--grep <pattern>] [--long|--json|--count]
allowed-tools: Bash(python -m raincloud.pipeline.list_datasets *)
---

Run the catalog-query CLI:

```bash
python -m raincloud.pipeline.list_datasets $ARGUMENTS
```

Read-only — never writes anything. Fast on the full manifest. `--columns` / `--coverage` read locally built files; everything else answers from the catalog.

Filters (compose with AND):

| Flag | Filter |
|---|---|
| `--handler <h>` | `transform.handler` exact match (e.g. `tighten_types`, `glove_split`, `uci_default`) |
| `--license <spdx>` | `license.spdx` exact match (e.g. `CC0-1.0`, `Apache-2.0`) |
| `--fetch-type <t>` | `fetch.type` ∈ `generated`, `http`, `huggingface`, `custom`, `kaggle`, or `derived` for a dataset built from a parent (a hydrated dataset) |
| `--reader <r>` | `parse.reader` ∈ `csv`, `parquet`, `jsonl`, `xml`, `pbf`, `custom` |
| `--vortex` / `--no-vortex` | The catalog has Vortex for the dataset / it has none: a build measured the writer unable to produce it (`--json`: `vortex_unavailable`), or `export.formats` leaves it out (mutually exclusive) |
| `--kaggle-tos` | only specs gated behind a one-time click-through (`fetch.requires_interactive_accept`, Kaggle or Hugging Face) |
| `--local` | only datasets with a file prepared on this machine's disk |
| `--grep <pattern>` | regex over slug + short_name + full_name + description (case-insensitive) |

Output modes (default = one bare slug per line; on a terminal, hydrated datasets are marked `[hydrated]`):

- `--long` — wide table with slug, handler, fetch type, reader, license, row count, vortex, scrape and hydrated flags, `recorded` (what the tracked catalog records) and `local` (formats prepared on this install's disk).
- `--json` — one JSON object per matching dataset (pipe into `jq` for further filtering).
- `--count` — just the count of matches.

Common shapes:

```bash
# every dataset gated behind a one-time ToS click-through
python -m raincloud.pipeline.list_datasets --kaggle-tos

# every spec using the streaming wikipedia handler
python -m raincloud.pipeline.list_datasets --handler wikipedia_variant_parse --long

# count of CSV-reader specs that opt into Vortex
python -m raincloud.pipeline.list_datasets --reader csv --vortex --count

# slugs whose description mentions geometry or geo
python -m raincloud.pipeline.list_datasets --grep '\bgeo' --long
```

Pair with `/raincloud-status <slug>` to check filesystem state of any returned slug, and `/raincloud-validate-manifest` after editing the manifest based on findings.

Context: [SKILLS.md](../../context/SKILLS.md), [sources.schema.md](../../context/sources.schema.md), [`docs/v2/datasets.md`](../../../docs/v2/datasets.md) for full-row metadata.

## Discovery axes

`--showcase`, `--tag`, `--size`, `--trait`, `--view`, `--inspect`, `--tags-help`, `--showcase-help`. See the `raincloud-discover` skill for the full vocabulary and patterns.
