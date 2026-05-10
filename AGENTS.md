# AGENTS.md

Guidance for AI coding agents (Claude Code, Cursor, etc.) working in this repo. Read this before making non-trivial changes — the repo's layout is intentional and the core invariants below are easy to violate accidentally.

For the user-facing overview, see [`README.md`](README.md). For the manifest schema, see [`sources.schema.md`](sources.schema.md). The repo root also has a `CLAUDE.md → AGENTS.md` symlink so Claude Code auto-loads this file; both names point at the same content.

## First contact

On a fresh clone `outputs/` is empty — that's expected. The `outputs/v1/<slug>/` directories are gitignored and only populated by builds. Verify your environment with the read-only smoke test before doing anything heavy:

```bash
python -m scripts.pipeline.status --fast --missing-only
```

It loads `sources.json`, walks the manifest, and prints per-slug filesystem state in seconds with no side effects. If it errors, fix the env (`uv sync --inexact`) before running any build. Always pass `--inexact` to `uv sync`: without it, syncing one extra (e.g. `--extra dev`) silently uninstalls the others (kaggle, huggingface, tui), so a subsequent build of an HF/Kaggle slug will fail.

For a manifest sanity check that doesn't touch the filesystem at all:

```bash
python -m scripts.pipeline.validate_manifest
```

Validates `sources.json` against [`sources.schema.json`](sources.schema.json) (Draft 2020-12) plus cross-checks that the schema can't express — handler-name resolution against the live registry, slug uniqueness, `fetch.type`/`fetch.auth` consistency. Sub-second; safe to invoke after any manifest edit.

For catalog queries that would otherwise require greping the ~545 KB `sources.json` (or scrolling ~158 KB of [`docs/v1/datasets.md`](docs/v1/datasets.md)):

```bash
python -m scripts.pipeline.list_datasets --family uci --count
python -m scripts.pipeline.list_datasets --handler tighten_types --long
python -m scripts.pipeline.list_datasets --fetch-type kaggle --kaggle-tos
python -m scripts.pipeline.list_datasets --grep '\bgeo' --long
```

Filters compose with AND across `--family`, `--handler`, `--license`, `--fetch-type`, `--reader`, `--vortex` / `--no-vortex`, `--kaggle-tos`, `--grep`. Output modes: default (one slug per line), `--long` (wide table), `--json` (jq-friendly), `--count`.

If the user wants to *browse* interactively rather than query, point them at `python -m scripts.pipeline.browse` (read-only Textual TUI over the same data; requires `uv sync --extra tui --inexact`). It's a human-facing tool — don't try to run it from an agent context, since it won't render and will hang waiting for keystrokes.

For a slightly broader regression net, the `tests/` directory carries a sub-second pytest smoke suite (manifest shape, schema self-consistency, handler registry, example template). Run it after any change to the manifest, the schema, or the handler registry:

```bash
uv sync --extra dev --inexact   # one-time — installs pytest, preserves other extras
pytest
```

Copy-pasteable templates for the two most common edits live under [`examples/`](examples/) — `minimal_spec.json` for new manifest entries, `streaming_handler.py.tmpl` for memory-constrained transform handlers.

If your agent harness supports the [Agent Skills](https://agentskills.io) standard (Claude Code, Codex, etc.), the `.agents/skills/` directory carries 16 invokable skills wrapping every pipeline entry point and procedural playbook — see [`.agents/skills/README.md`](.agents/skills/README.md). `.claude → .agents` is a symlink so both naming conventions resolve. The `.agents/settings.json` at the same level is a tracked allow-list of safe, read-only commands so a fresh-clone agent doesn't burn turns on permission prompts; per-machine overrides go in the gitignored `.agents/settings.local.json`.

## Don't read the giant derived docs cover-to-cover

`docs/v1/datasets.md` (~158 KB) is large; prefer targeted reads via `offset`/`limit`. Column-level / coverage / vortex-skip / hydrate-candidate views are NOT markdown anymore — they used to be (huge `columns_*.md` and `coverage_*.md` files, plus per-slug `vortex_skip.md` and `hydrated.md` listings) but those were unscannable as a reading experience and duplicated state already queryable. They're now flags on `list_datasets`:

```bash
python -m scripts.pipeline.list_datasets --columns [<slug>...] [--column-grep PATTERN]
python -m scripts.pipeline.list_datasets --coverage [--source parquet|vortex]
python -m scripts.pipeline.list_datasets --no-vortex --long      # vortex-opted-out slugs + reasons (via --json)
python -m scripts.pipeline.list_datasets --hydrate --long        # hydration candidates
```

Hydration policy / philosophy lives in the hand-maintained [`HYDRATING.md`](HYDRATING.md) (preamble only, no auto-generated per-slug list). For catalog-shape questions ("which slugs use handler X", "what's CC0-licensed") prefer `list_datasets`. The top-level `docs/*.md` mirrors are gitignored scratch and behave identically.

[`docs/v1/handlers.md`](docs/v1/handlers.md) (small — ~3 KB) is fine to read in full and carries one row per registered handler with purpose, streaming flag, **the format-specific deps it imports** (`pandas`, `openpyxl`, `pyreadstat`, `osmium`, `zstandard`, `unlzw3` — pyarrow / numpy / duckdb suppressed as core), manifest spec count, and example slugs. Read it before adding a new handler so you can pick precedent and know which extras the manifest entry will need.

## What this repo does

Raincloud is a **client-reproducible pipeline** for building a curated catalog of public datasets as Parquet + optional Vortex files. The single source of truth is `sources.json`. Everything under `outputs/`, the two derived docs (`docs/datasets.md`, `docs/handlers.md`), and the JSON catalog snapshot (`docs/snapshot.json` — read by the TUI as a fallback for unbuilt-locally slugs, AND used by `docs.py` itself as the row-count / file-size fallback when regenerating `datasets.md` on a partial build) is **derived** — regenerate, never hand-edit. Column-level / coverage / vortex-skip / hydrate-candidate views are queryable via `list_datasets` flags rather than markdown.

The pipeline flow is: **fetch → extract → parse → transform → write → validate → convert** (stage 7 opt-in per-spec), orchestrated by `scripts.pipeline.build`.

## Invariants (don't break these)

1. **`sources.json` is authoritative.** Every row of every derived artefact maps back to a `DatasetSpec` here. If you're tempted to hand-edit `docs/*.md` or drop a parquet into `outputs/v1/<slug>/` by hand — stop, fix the manifest, re-run the build, re-run `docs.py`.
2. **`outputs/raw_downloads/<slug>/` is unversioned; `outputs/v{schema_version}/<slug>/<format>/<filename>` is version-scoped.** Raw upstream bytes are the same regardless of output schema_version, so they're cached outside the version prefix. Within a version, artefacts live under per-format subdirectories: today `parquet/<slug>.parquet` and `vortex/<slug>.vortex`, with room for `parquet-hydrated/`, partitioned variants, etc. without filename collisions. Path helpers in `scripts/pipeline/spec.py`: `output_format_dir(slug, fmt)`, `prepared_parquet(slug)`, `prepared_vortex(slug)`. A manifest bump to v2 would populate `outputs/v2/` alongside `outputs/v1/`, both sharing `raw_downloads/`.
3. **`_workdir/<slug>/` is scratch.** Gitignored and safe to wipe. Handlers should clean up what they put there; `build.py --clean-workdir` also wipes after a successful build.
4. **`.archive/` is gitignored and local-only.** Holds Kaggle-era triage/attribution docs kept on the maintainer's tree for reference. A fresh-clone agent won't have this directory — when other docs reference it as a "fallback" alongside git history, treat git history as the only fallback you can rely on.
5. **Always go through `spec.duckdb_connect`** when opening a DuckDB connection, not `duckdb.connect(...)` directly. The helper applies env-var-driven resource limits and the `storage_compatibility_version=v1.5.0` setting required for persistent VARIANT writes. See [`SKILLS.md`](SKILLS.md#opening-a-duckdb-connection) for detail.
6. **`docs/` layout is split.** Top-level `docs/*.md` is gitignored scratch — regenerable against a subset of parquets for local type-coverage experiments. `docs/v{schema_version}/*.md` is the tracked canonical snapshot matching `outputs/v{n}/`. Regenerating docs via `scripts.pipeline.docs` writes to the top-level path; promotion to `docs/v{n}/` is a manual copy.

## How the build pipeline is structured

The seven stages are in `scripts/pipeline/` and are each independently invokable:

| Stage | Module | Reads | Writes |
|---|---|---|---|
| fetch | `fetch.py` | `fetch.*` | `outputs/raw_downloads/<slug>/` |
| extract | `extract.py` | `extract.*` | `_workdir/<slug>/` |
| parse | `parse.py` | `parse.*` | in-memory `(Path, Table)` tuples |
| transform | `transform.py` | `transform.*` | in-memory `(slug, Table)` tuples *or* direct-to-parquet (streaming handlers) |
| write | `write.py` | `write.*` | `outputs/v{n}/<slug>/parquet/<slug>.parquet` |
| validate | `validate.py` | `expect.*` | raises on mismatch unless `--loose` |
| convert | `convert.py` | `convert.*` | `outputs/v{n}/<slug>/vortex/<slug>.vortex` (when `convert.vortex = true`); ALSO `outputs/v{n}/<slug>/vortex-hydrated/<slug>.vortex` when a hydrated parquet exists. Same flag governs both pairs. |
| hydrate (opt-in, off the default build path) | `hydrate.py` | `hydrate.*` | `outputs/v{n}/<slug>/parquet-hydrated/<slug>.parquet` (only when `hydrate` is set; safety-filter-gated; outbound HTTP). Auto-runs convert at the end when `convert.vortex = true`. |

Streaming handlers (`factbook_variant_parse`, `jsonbench_variant_parse`, `wikipedia_variant_parse`, `lichess_pgn_parse`, `stack_exchange_split`, `osm_pbf_split`, `public_bi_merge`) write the parquet themselves and return `[]` — the write stage becomes a no-op.

Some `fetch.type: "kaggle"` entries carry `fetch.requires_interactive_accept: true` — those datasets are gated behind a one-time click-through ToS acceptance on the Kaggle web UI and can't be built on a fresh Kaggle account without that manual step. See [`SKILLS.md`](SKILLS.md#adding-a-kaggle-dataset-gated-behind-tos-acceptance) for the pattern.

## Safe ways to edit `sources.json`

The manifest is a large hand-authored JSON file (~545 KB, 249 dataset entries) with a specific top-level key order (`schema_version`, `generated_at`, `audit_cutoff`, `notes`, `datasets`). Stick with small Python scripts for edits:

```python
import json
from pathlib import Path
SRC = Path("sources.json")  # run from the repo root, or use an absolute path
m = json.loads(SRC.read_text())
for d in m["datasets"]:
    if d["slug"] == "target-slug":
        d["transform"]["handler"] = "new_handler"
        break
SRC.write_text(json.dumps(m, indent=2) + "\n")
```

Don't use `sed` or text-based edits — JSON-safe structural edits are cheap and avoid accidental quoting breakage.

## Rebuilding is expensive — confirm before triggering

Building a single large dataset can take hours (observed: JSONBench 100M ≈ 6 h; OSM Germany extract ≈ 45 min per element kind; Wikipedia Structured Contents → 34 GB parquet, multi-hour). The `outputs/v1/<slug>/parquet/<slug>.parquet` + `vortex/<slug>.vortex` pair on disk already reflects a full catalog build — rebuilding wipes and redoes that work. Before running `python -m scripts.pipeline.build <slug>` on anything non-trivial, confirm with the user.

Small (<100 MB) parquets are fine to rebuild without asking.

## Regenerate derived docs after any pipeline change

```bash
python -m scripts.pipeline.docs    # datasets.md + handlers.md + snapshot.json
```

All three derived artefacts regenerate in one pass by default. Run this after any build, convert run, in-place parquet mutation, or when a handler is added/removed/renamed (handlers.md regenerates from the registry + manifest).

**Keep `docs/snapshot.json` fresh — it's load-bearing.** `datasets.md` regen reads from disk for slugs you've built locally and falls back to `docs/snapshot.json` (or `docs/v{schema_version}/snapshot.json` on a fresh clone) for everything else. Without that fallback, regenerating on a partial build would dash-out 200+ rows and silently destroy ground truth in the tracked snapshot. The default no-args invocation regens snapshot + datasets in lockstep, so it's only at risk if you do partial regens — `docs.py datasets` alone won't refresh the snapshot. After a build, prefer the no-args form.

## Style and scope

- **No Kaggle-era narrative.** The legacy triage / binary-blob-integration / Kaggle-filter history lives in `.archive/`. New README/AGENTS/SKILLS content should reflect only the current three-point intent: fetch → transform → outputs.
- **One handler per upstream shape.** Don't shoehorn a new shape into `tighten_types` or `identity`; write a dedicated handler under `scripts/pipeline/handlers/` and register it in `handlers/__init__.py`.
- **Handlers are short.** Most are under 150 lines. If a new handler balloons past that, look for reuse opportunities with existing helpers (`duckdb_connect`, `outputs_root`, `spec_field`).
- **No backwards-compat stubs.** When removing a handler or slug, remove it fully — git history (and the maintainer's local `.archive/`) is the fallback, not half-wired shims.

## When you're unsure

Prefer *Read* → *Grep* → ask, over guessing. The pipeline has hidden contracts (streaming handlers returning `[]`, raw_downloads being unversioned, VARIANT requiring storage_compatibility_version) that aren't obvious from any single file.
