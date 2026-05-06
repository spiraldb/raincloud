# 🌧️ Raincloud

[![CI](https://github.com/spiraldb/raincloud/actions/workflows/ci.yml/badge.svg)](https://github.com/spiraldb/raincloud/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/spiraldb/raincloud)](https://github.com/spiraldb/raincloud/releases)
[![License](https://img.shields.io/github/license/spiraldb/raincloud)](LICENSE)
[![Cite](https://img.shields.io/badge/cite-CITATION.cff-blue)](CITATION.cff)

A reproducible pipeline for building a curated catalog of public datasets as analytics-ready Parquet and Vortex files.

Raincloud is a reproducible baseline of public datasets in modern columnar formats, curated from research papers and existing community efforts. The project's motivation comes from file-format research, where consistent test corpora are needed to compare encoding, compression, and layout choices on real-world inputs. Beyond file-format research, we see broader value in providing a community-curated set of real-world data, as we expect it to be useful for other tasks such as analytical benchmarking and model evaluation.

> ⚠ **Third-party data.** Raincloud fetches data from URLs declared in `sources.json`. Those bytes come from upstream sources, not from us — we don't audit, host, or redistribute them. See [`DISCLAIMER.md`](DISCLAIMER.md) for the AS IS posture, content / license / supply-chain disclaimers, and the dataset-removal channel.

The repo is driven by a single manifest — `sources.json` — which declares:

1. **Where to fetch data from.** Each entry names an upstream source (HTTP, Kaggle, Hugging Face) and the URLs to pull.
2. **How to transform it.** Each entry names a parser (`csv`, `parquet`, `jsonl`, `xml`, `pbf`, `custom`) and a transform handler that converts the raw bytes into one or more typed Arrow tables.
3. **Where it lands.** Transformed tables are written to `outputs/v{schema_version}/<slug>/parquet/<slug>.parquet`, with the optional Vortex sibling at `outputs/v{schema_version}/<slug>/vortex/<slug>.vortex`. The per-format subdirectory leaves room for additional artifact tiers (e.g. `parquet-hydrated/`) without filename collisions.

Nothing downstream of `sources.json` is hand-maintained; `docs/datasets.md` and `docs/handlers.md` are derived artefacts regenerated after each build. Column-level / type-coverage / vortex-skip / hydration-candidate views are queryable via `list_datasets` flags and the TUI rather than markdown.

## Getting started

**Browse the catalog at a glance** — sortable columns, parquet/vortex presence per slug, no builds required:

```bash
uv sync --extra tui
python -m scripts.pipeline.browse
```

A read-only Textual TUI over `sources.json`. Click any column header to sort; right pane shows description, license, fetch URL, and on-disk state for the highlighted slug. Press `q` to quit.

**Tell Raincloud which dataset you want; get back a Parquet + Vortex file on disk.**

```bash
uv sync
python -m scripts.pipeline.status --fast --missing-only   # read-only env check
python -m scripts.pipeline.build countries-of-the-world
```

```
outputs/v1/countries-of-the-world/parquet/countries-of-the-world.parquet
outputs/v1/countries-of-the-world/vortex/countries-of-the-world.vortex
```

The command runs every pipeline stage — fetch, extract, parse, transform, write, validate, convert — and leaves both a Parquet file and its converted Vortex sibling under per-format subdirectories of `outputs/v1/<slug>/`.

**Pick any other dataset from [`docs/v1/datasets.md`](docs/v1/datasets.md)** (249 curated) and pass its slug the same way. Examples spanning the size range:

```bash
python -m scripts.pipeline.build uci-seeds                  # 210 rows, ~200 ms
python -m scripts.pipeline.build clickbench-hits            # 100 M rows, ~10 GB parquet
python -m scripts.pipeline.build --family public-bi         # all 46 Public BI workloads
```

### Upstream-specific extras

157 of 249 manifest entries fetch from direct HTTPS endpoints and need no additional setup. The rest:

```bash
# Kaggle-hosted (33 slugs). One-time credential setup:
uv sync --extra kaggle
mkdir -p ~/.kaggle && mv /path/to/kaggle.json ~/.kaggle/ && chmod 600 ~/.kaggle/kaggle.json

# Hugging Face-hosted (59 slugs):
uv sync --extra huggingface

# Everything:
uv sync --extra all
```

## For AI coding agents

If you're an AI coding agent landing in this repo:

1. Read [`AGENTS.md`](AGENTS.md) (auto-loaded from `CLAUDE.md → AGENTS.md`) for the invariants and architecture.
2. Run `python -m scripts.pipeline.status --fast --missing-only` to verify the env, then `python -m scripts.pipeline.validate_manifest` to confirm `sources.json` is well-formed. Both are sub-second and side-effect-free.
3. Run `pytest` (after `uv sync --extra dev`) for a regression net before any non-trivial change to the manifest, schema, or handler registry.
4. For catalog questions ("which slugs use handler X", "what's CC0-licensed"), use `python -m scripts.pipeline.list_datasets` rather than greping `sources.json` or scrolling [`docs/v1/datasets.md`](docs/v1/datasets.md).
5. Copy-pasteable templates for new manifest entries and streaming handlers live in [`examples/`](examples/).
6. Harnesses that follow the [Agent Skills](https://agentskills.io) standard get 16 invokable skills under [`.agents/skills/`](.agents/skills/) (the `.claude → .agents` symlink means Claude Code sees the same files). Tracked safe-default permissions in [`.agents/settings.json`](.agents/settings.json) — see [`.agents/README.md`](.agents/README.md) for the full layout.

## Repository layout

```
sources.json                    # the manifest — one DatasetSpec per dataset
sources.schema.md               # human-friendly schema reference
sources.schema.json             # machine-readable JSON Schema (Draft 2020-12)
AGENTS.md                       # invariants + first-contact guide for AI coding agents (CLAUDE.md → AGENTS.md)
SKILLS.md                       # narrative playbooks
HYDRATING.md                    # hand-maintained hydration policy / philosophy
DISCLAIMER.md                   # AS IS posture, content/license disclaimers, dataset-removal reporting
scripts/
  pipeline/
    build.py                    # orchestrator — ties the 7 stages together
    fetch.py                    # stage 1: download raw bytes
    custom_fetch.py             # named custom-fetch helpers (fetch.type = "custom")
    extract.py                  # stage 2: unpack archives into _workdir/
    parse.py                    # stage 3: read raw files into Arrow tables
    transform.py                # stage 4: dispatch to named handler
    write.py                    # stage 5: emit parquet
    validate.py                 # stage 6: assert rows / schema_hash
    convert.py                  # stage 7 (optional): emit sibling .vortex per spec's convert.vortex flag
    hydrate.py                  # stage 8 (optional, opt-in): dereference URL columns into parquet-hydrated/
    docs.py                     # regenerate docs/datasets.md + handlers.md (other catalog views live in list_datasets / TUI)
    tighten_variant.py          # in-place JSON → VARIANT pass
    validate_manifest.py        # static checks on sources.json (schema + cross-checks)
    list_datasets.py            # filter/list slugs by family / handler / license / etc.
    status.py                   # per-slug filesystem state report
    browse.py                   # interactive Textual TUI over sources.json (requires --extra tui)
    spec.py                     # manifest loader, path helpers, duckdb_connect
    handlers/                   # named transform handlers
tests/                          # pytest smoke suite (manifest, schema, handler registry, examples)
examples/                       # copy-pasteable templates (minimal_spec.json, streaming_handler.py.tmpl)
.agents/                        # tracked agent allow-list (settings.json) + 16 invokable skills (.claude → .agents)
outputs/
  raw_downloads/<slug>/         # stage 1 output — unversioned, cached
  v{schema_version}/<slug>/     # stage 5 output — version-scoped
docs/
  datasets.md                   # auto-generated index (one row per dataset)
  handlers.md                   # auto-generated registry view (purpose, streaming, extra deps, usage)
  v{schema_version}/            # tracked canonical snapshot of datasets.md + handlers.md
_workdir/<slug>/                # stage 2 scratch space (gitignored)
```

## Running the pipeline

```bash
# Build a single dataset
python -m scripts.pipeline.build <slug>

# Build everything in a family
python -m scripts.pipeline.build --family uci
python -m scripts.pipeline.build --family public-bi

# Build every dataset in the manifest
python -m scripts.pipeline.build --all

# Loosen validation (warn instead of error on row-count drift)
python -m scripts.pipeline.build <slug> --loose

# Regenerate the derived docs after a build
python -m scripts.pipeline.docs            # both files (datasets.md + handlers.md)
python -m scripts.pipeline.docs datasets   # just datasets.md
python -m scripts.pipeline.docs handlers   # just handlers.md

# Catalog views that used to be markdown live as list_datasets flags now
python -m scripts.pipeline.list_datasets --columns [<slug>...] [--column-grep PATTERN]
python -m scripts.pipeline.list_datasets --coverage [--source parquet|vortex]
python -m scripts.pipeline.list_datasets --no-vortex --json    # vortex-skip slugs + reasons
python -m scripts.pipeline.list_datasets --hydrate --long      # hydration candidates

# Post-process: promote JSON-annotated string columns to VARIANT in-place
python -m scripts.pipeline.tighten_variant            # every built parquet
python -m scripts.pipeline.tighten_variant <slug>...  # specific slugs

# Emit a sibling .vortex for every spec that opts in via convert.vortex: true
python -m scripts.pipeline.convert                    # respects per-spec flag
python -m scripts.pipeline.convert <slug>...

# Optional stage 8: dereference URL columns into parquet-hydrated/<slug>.parquet.
# Off by default — only for slugs that opted in via the `hydrate` block.
# Safety filter ON by default; bypass requires --unsafe-allow-all-domains
# AND --i-accept-the-risk. See HYDRATING.md for the full discussion.
python -m scripts.pipeline.hydrate <slug>             # one slug
python -m scripts.pipeline.hydrate <slug> --limit 100 # first N rows (recommended for first run)
python -m scripts.pipeline.hydrate --all              # every spec with hydrate

# Read-only inspection / triage
python -m scripts.pipeline.status --fast --missing-only       # filesystem state across the manifest
python -m scripts.pipeline.validate_manifest                  # static checks on sources.json (schema + cross-checks)
python -m scripts.pipeline.list_datasets --family uci         # filter the catalog without grepping JSON
python -m scripts.pipeline.list_datasets --handler tighten_types --long
python -m scripts.pipeline.list_datasets --grep '\bgeo' --long
python -m scripts.pipeline.browse                             # interactive TUI (requires --extra tui)

# Run the test suite (sub-second, no fetch / no build)
uv sync --extra dev && pytest
```

Each stage is independently invokable — e.g. `python -m scripts.pipeline.fetch <slug>` to download raw bytes without running the rest. Stages are idempotent: fetch skips when `expected_bytes`/`expected_sha256` already matches on disk, write skips when the output parquet is already current.

### DuckDB resource limits

Every DuckDB connection in the pipeline goes through `scripts.pipeline.spec.duckdb_connect`, which reads these optional env vars before opening:

| Env var | Effect |
|---|---|
| `RAINCLOUD_DUCKDB_MEMORY_LIMIT` | Caps DuckDB's working set (`memory_limit` setting). E.g. `8GB`, `512MB`. DuckDB spills to disk once the limit is hit. Unset = DuckDB's default (~80% of system RAM), which can be a problem on shared hosts or CI runners. |
| `RAINCLOUD_DUCKDB_THREADS` | Caps the thread pool. Integer. |
| `RAINCLOUD_DUCKDB_TEMP_DIRECTORY` | Where DuckDB spills intermediate batches. Default = system tempdir. Point at a larger volume if the system tempdir runs out of room on a big build. |

Example:

```bash
RAINCLOUD_DUCKDB_MEMORY_LIMIT=8GB \
RAINCLOUD_DUCKDB_TEMP_DIRECTORY=/mnt/scratch/duckdb-tmp \
  python -m scripts.pipeline.build jsonbench-bluesky-100m
```

Persistent DuckDB databases are opened with `storage_compatibility_version=v1.5.0` automatically (required for VARIANT columns).

## The manifest (`sources.json`)

See [`sources.schema.md`](sources.schema.md) for the human-friendly reference and [`sources.schema.json`](sources.schema.json) for the machine-readable JSON Schema (Draft 2020-12). After editing the manifest, run

```bash
python -m scripts.pipeline.validate_manifest
```

to catch typo'd handler names, slug collisions, and shape errors in under a second before paying for a fetch.

A minimal entry looks like:

```jsonc
{
  "slug": "clickbench-hits",
  "short_name": "ClickBench Hits",
  "family": "direct",
  "license": { "spdx": "Apache-2.0", "source_url": "..." },
  "fetch":     { "type": "http", "urls": ["https://datasets.clickhouse.com/hits_compatible/hits.parquet"] },
  "extract":   { "type": "passthrough" },
  "parse":     { "reader": "parquet" },
  "transform": { "handler": "identity" },
  "write":     { "output": "clickbench-hits.parquet", "compression": "zstd" },
  "expect":    { "rows": 99997497 }
}
```

Current counts:

- **249 datasets** across five families: `direct`, `kaggle-upstream`, `nyc-tlc`, `public-bi`, `uci`.
- **Fetch types in use:** `http`, `kaggle`, `huggingface`.
- **Parse readers in use:** `csv`, `parquet`, `jsonl`, `xml`, `pbf`, `custom`.
- **Schema version:** 1 — outputs land in `outputs/v1/`.

## Output layout

Each dataset produces exactly one parquet per output slug (some handlers split one source into multiple outputs — `glove_split` → 3, `osm_pbf_split` → 3, `stack_exchange_split` → N). Within each slug directory, artefacts live in per-format subdirectories so additional tiers (Vortex sibling, future hydrated copies, partitioned variants) can coexist without filename collisions:

```
outputs/v1/clickbench-hits/parquet/clickbench-hits.parquet
outputs/v1/clickbench-hits/vortex/clickbench-hits.vortex
outputs/v1/glove-6b-50d/parquet/glove-6b-50d.parquet
outputs/v1/glove-6b-50d/vortex/glove-6b-50d.vortex
outputs/v1/osm-germany-nodes/parquet/osm-germany-nodes.parquet
...
```

Raw downloads are cached separately and *not* version-scoped, since the same upstream bytes can feed any schema_version:

```
outputs/raw_downloads/clickbench-hits/hits.parquet
outputs/raw_downloads/glove-6b-50d/glove.6B.zip   # hardlinked into sibling slugs
outputs/raw_downloads/osm-germany-nodes/germany-latest.osm.pbf
```

Sibling slugs sharing the same upstream URL (GloVe 50d/100d/200d; OSM Germany nodes/ways/relations) are deduped via hardlink during fetch.

## Parquet type coverage

The manifest is curated to exercise a broad range of Parquet logical and nested types, including:

- `VARIANT` — `countries-of-the-world` (227 country JSON blobs), `jsonbench-bluesky-100m` (100M Bluesky firehose records).
- GeoParquet 1.1 with WKB geometry — `osm-germany-{nodes,ways,relations}`.
- `fixed_size_list<float32, N>` — GloVe embeddings, dbpedia 1536-dim OpenAI embeddings.
- `list<...>` with tightened element types — e.g. `list<uint32>` for Hacker News kids/parts.
- Nested `struct` / `list<struct>` / `map<string, int64>` — Wikipedia Structured Contents.
- Timestamp precision narrowing (`ns → ms` where every value is a whole second).
- UUID and JSON logical-type annotations on string columns.
- `DECIMAL(P, S)` where every double value round-trips losslessly through the chosen precision.

Type-tightening is idempotent — `tighten_types` can be re-run against any parquet in `outputs/v*/` without regressing the widths.

## Derived docs

- [`docs/datasets.md`](docs/datasets.md) — one row per dataset with short/full name, description, source URL, data kind, license, row count, row group count, and file size. Regenerate: `python -m scripts.pipeline.docs datasets`.
- [`docs/handlers.md`](docs/handlers.md) — one row per registered transform handler with its one-line purpose, streaming flag, **format-specific deps it imports** (e.g. `pandas`, `openpyxl`, `pyreadstat`, `osmium`, `zstandard`, `unlzw3` — pyarrow / numpy / duckdb suppressed as core), manifest-spec usage count, and example slugs. Useful when picking a handler for a new dataset, finding precedent for a given upstream shape, or knowing which extras a new manifest entry will pull in. Regenerate: `python -m scripts.pipeline.docs handlers`.

Both are machine-generated; do not hand-edit. `python -m scripts.pipeline.docs` with no args refreshes both.

[`HYDRATING.md`](HYDRATING.md) is **hand-maintained** policy / philosophy for the optional hydrate stage — preamble only, no auto-generated per-slug list.

Other catalog views (column index, type coverage, vortex-skip list, hydration candidates) used to be auto-generated markdown. They moved out of the markdown layer because the multi-megabyte indexes were unscannable as a reading experience and duplicated state already queryable. They're now flags on `list_datasets`:

```bash
python -m scripts.pipeline.list_datasets --columns [<slug>...] [--column-grep PATTERN]
python -m scripts.pipeline.list_datasets --coverage [--source parquet|vortex]
python -m scripts.pipeline.list_datasets --no-vortex --json    # vortex-skip slugs + reasons
python -m scripts.pipeline.list_datasets --hydrate --long      # hydration candidates
```

Or use `python -m scripts.pipeline.browse` for an interactive view.

## Contributing

Bug reports, feature requests, and PRs are welcome. See
[`CONTRIBUTING.md`](CONTRIBUTING.md) for dev-environment setup, the pre-PR
check sequence, and pointers into [`SKILLS.md`](SKILLS.md) for the most common
change types (new dataset, new handler). Notable changes land in
[`CHANGELOG.md`](CHANGELOG.md).

## Security

Please report vulnerabilities privately rather than via a public issue — see
[`SECURITY.md`](SECURITY.md) for the disclosure channel and timelines.

## Disclaimers

[`DISCLAIMER.md`](DISCLAIMER.md) covers Raincloud's posture on third-party
datasets: AS IS warranty disclaimer, content and association disclaimer
(any fetched file may contain questionable or offensive material — we
don't audit upstream content), license diligence, supply-chain risk, and
the process for requesting that a dataset be removed from `sources.json`.

## License

Raincloud is licensed under the [Apache License 2.0](LICENSE). Each dataset
declared in `sources.json` carries its own upstream license under the
`license.spdx` field; those licenses govern redistribution of any Parquet /
Vortex artefact built against that upstream and are independent of the
license covering the pipeline code itself.
