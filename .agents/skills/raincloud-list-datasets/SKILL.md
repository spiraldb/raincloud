---
name: raincloud-list-datasets
description: Filter and list datasets from sources.json without grepping a 313 KB JSON file. Use when the user asks "which slugs use handler X", "show me all UCI datasets", "what's gated behind Kaggle ToS", or any other catalog-shape question that's faster than reading docs/v1/datasets.md (55 KB) end to end.
argument-hint: [--family <f>] [--handler <h>] [--license <spdx>] [--fetch-type <t>] [--reader <r>] [--vortex|--no-vortex] [--kaggle-tos] [--grep <pattern>] [--long|--json|--count]
allowed-tools: Bash(python -m scripts.pipeline.list_datasets *)
---

Run the catalog-query CLI:

```bash
python -m scripts.pipeline.list_datasets $ARGUMENTS
```

Read-only — never touches `outputs/` or `_workdir/`. Sub-second on the full manifest.

Filters (compose with AND):

| Flag | Filter |
|---|---|
| `--family <f>` | `family` ∈ `direct`, `kaggle-upstream`, `nyc-tlc`, `public-bi`, `uci` |
| `--handler <h>` | `transform.handler` exact match (e.g. `tighten_types`, `glove_split`) |
| `--license <spdx>` | `license.spdx` exact match (e.g. `CC0-1.0`, `Apache-2.0`) |
| `--fetch-type <t>` | `fetch.type` ∈ `http`, `kaggle`, `huggingface`, `custom` |
| `--reader <r>` | `parse.reader` ∈ `csv`, `parquet`, `jsonl`, `xml`, `pbf`, `custom` |
| `--vortex` / `--no-vortex` | `convert.vortex` is true / false (mutually exclusive) |
| `--kaggle-tos` | only Kaggle specs gated behind a one-time click-through |
| `--grep <pattern>` | regex over slug + short_name + full_name + description (case-insensitive) |

Output modes (default = one slug per line):

- `--long` — wide table with slug, family, handler, fetch type, reader, license, row count, vortex flag.
- `--json` — one JSON object per matching dataset (pipe into `jq` for further filtering).
- `--count` — just the count of matches.

Common shapes:

```bash
# every Kaggle dataset gated behind ToS acceptance
python -m scripts.pipeline.list_datasets --fetch-type kaggle --kaggle-tos

# every spec using the streaming wikipedia handler
python -m scripts.pipeline.list_datasets --handler wikipedia_variant_parse --long

# count of CSV-reader specs that opt into Vortex
python -m scripts.pipeline.list_datasets --reader csv --vortex --count

# slugs whose description mentions geometry or geo
python -m scripts.pipeline.list_datasets --grep '\bgeo' --long
```

Pair with `/raincloud-status <slug>` to check filesystem state of any returned slug, and `/raincloud-validate-manifest` after editing the manifest based on findings.

Context: [SKILLS.md](../../context/SKILLS.md), [sources.schema.md](../../context/sources.schema.md), [`docs/v1/datasets.md`](../../../docs/v1/datasets.md) for full-row metadata.
