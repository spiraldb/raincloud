# SKILLS.md

Playbooks for common operations in this repo. Each section is a self-contained recipe — copy and adapt.

Prereqs: Python 3.11+ and [uv](https://docs.astral.sh/uv/). Run `uv sync --inexact` in the repo root to install the pinned core deps (`pyarrow`, `duckdb`, `vortex-data`, `zstandard`, `py7zr`, `unlzw3`, `pandas`, `openpyxl`, `pyreadstat`, `osmium`, `jsonschema`). Add `--extra kaggle` for Kaggle-hosted datasets, `--extra huggingface` for Hugging Face ones, or `--extra dev` for `pytest` — see [`README.md`](README.md#upstream-specific-extras). Always pass `--inexact` so subsequent extras accumulate instead of overwriting prior ones (uv's default is "exact" sync, which removes anything not requested by the current invocation). Invoke Python as `.venv/bin/python` (or activate the venv).

## Index

- [Opening a DuckDB connection](#opening-a-duckdb-connection)
- [Running the test suite](#running-the-test-suite) — pytest smoke regression net
- [Querying the catalog](#querying-the-catalog) — `list_datasets` filters
- [Validating `sources.json`](#validating-sourcesjson) — schema + cross-checks
- [Adding a new dataset](#adding-a-new-dataset)
- [Emitting a Vortex file alongside the Parquet](#emitting-a-vortex-file-alongside-the-parquet)
- [Adding a Kaggle dataset gated behind ToS acceptance](#adding-a-kaggle-dataset-gated-behind-tos-acceptance)
- [Adding a new transform handler](#adding-a-new-transform-handler)
- [Writing a streaming handler](#writing-a-streaming-handler)
- [Promoting a JSON column to VARIANT](#promoting-a-json-column-to-variant)
- [Tightening existing integer / binary-string columns](#tightening-existing-integer--binary-string-columns)
- [Hydrating a URL column](#hydrating-a-url-column) — optional stage 8
- [Debugging a failing build](#debugging-a-failing-build)
- [Running a large build safely](#running-a-large-build-safely)
- [Regenerating specific docs](#regenerating-specific-docs)
- [Removing a dataset](#removing-a-dataset)

Templates referenced by the playbooks: [`examples/minimal_spec.json`](examples/minimal_spec.json) (new manifest entry), [`examples/streaming_handler.py.tmpl`](examples/streaming_handler.py.tmpl) (memory-constrained handler).

## Opening a DuckDB connection

Always go through `scripts.pipeline.spec.duckdb_connect` instead of `duckdb.connect(...)`. It applies Raincloud's env-var-driven resource limits and the `storage_compatibility_version=v1.5.0` setting required for persistent VARIANT writes.

```python
from scripts.pipeline.spec import duckdb_connect

# In-memory, default config
con = duckdb_connect()

# Persistent DB (automatically gets storage_compatibility_version=v1.5.0)
con = duckdb_connect("/path/to/build.duckdb")

# Override or extend
con = duckdb_connect(extra_config={"preserve_insertion_order": False})
```

Env vars the helper honours (see [`README.md`](README.md#duckdb-resource-limits)):

- `RAINCLOUD_DUCKDB_MEMORY_LIMIT` — e.g. `8GB`, `96GB`. **Set this for large builds.** Default (unset) lets DuckDB grab ~80% of system RAM, which can swap-thrash on heavily-nested VARIANT shredding.
- `RAINCLOUD_DUCKDB_THREADS` — int.
- `RAINCLOUD_DUCKDB_TEMP_DIRECTORY` — path for spill files.

## Running the test suite

```bash
uv sync --extra dev --inexact   # one-time — installs pytest, preserves other extras
pytest                          # ~0.5 s on the full suite
```

`tests/` carries a sub-second smoke suite for the manifest, the schema, the handler registry, and the example templates. No fetch, no build, no filesystem writes. Run after any change to `sources.json`, `sources.schema.json`, `scripts/pipeline/handlers/__init__.py`, or `examples/`. Tests exercise the same `validate_manifest` codepath the `/raincloud-validate-manifest` skill runs.

## Querying the catalog

Filter the manifest from the command line instead of grepping the ~545 KB JSON or scrolling [`docs/v1/datasets.md`](docs/v1/datasets.md) (~158 KB):

```bash
python -m scripts.pipeline.list_datasets --family uci                  # one slug per line
python -m scripts.pipeline.list_datasets --handler tighten_types --long
python -m scripts.pipeline.list_datasets --fetch-type kaggle --kaggle-tos
python -m scripts.pipeline.list_datasets --reader csv --vortex --count
python -m scripts.pipeline.list_datasets --grep '\bgeo' --long
python -m scripts.pipeline.list_datasets --license CC0-1.0 --json | jq -s 'length'
```

Filters compose with AND. `--long` emits a wide table (slug + family + handler + fetch type + reader + license + rows + vortex flag), `--json` emits one row per object for piping into `jq`, `--count` emits just the match count. Read-only — sub-second on the full manifest. Always pair with `/raincloud-status` if you need filesystem state for any returned slug.

## Validating `sources.json`

Sub-second static check; safe to run after any manifest edit and before triggering a build:

```bash
python -m scripts.pipeline.validate_manifest          # human-readable
python -m scripts.pipeline.validate_manifest --json   # machine-readable
python -m scripts.pipeline.validate_manifest --strict # warnings → errors
```

Two layers:

1. **JSON Schema** ([`sources.schema.json`](sources.schema.json), Draft 2020-12) — shape, enums, regexes, required fields. Uses the `jsonschema` package (pinned in `pyproject.toml`); skipped with a hint if missing.
2. **Cross-checks** the schema can't express:
   - slug uniqueness across `datasets[]`
   - every `transform.handler` resolves in the live registry (`scripts/pipeline/handlers/__init__.py:_REGISTRY`)
   - registered handlers referenced by 0 specs (orphans → warning)
   - `fetch.urls` non-empty unless `fetch.type == "custom"`
   - `fetch.auth` matches `fetch.type` for `kaggle` / `huggingface`
   - `fetch.requires_interactive_accept` only on `fetch.type == "kaggle"`
   - `write.output` filename agrees with slug (single-output handlers; multi-output are flagged as warnings — usually safe)

Exit code: `0` on success (warnings allowed), `1` on errors.

The companion [`sources.schema.md`](sources.schema.md) is the human-friendly reference; `sources.schema.json` is the machine version. Keep them in lockstep when adding fields.

## Adding a new dataset

1. **Identify the upstream.** Get a stable public URL (prefer the publisher's canonical endpoint over a mirror) and check the license permits redistribution-of-derivatives.
2. **Append a `DatasetSpec` to `sources.json`.** See [`sources.schema.md`](sources.schema.md) for the full shape. Minimal direct-HTTP example:

    ```jsonc
    {
      "slug": "my-dataset",
      "short_name": "My Dataset",
      "full_name": "My Dataset (publisher attribution)",
      "description": "One-line summary.",
      "family": "direct",
      "license": { "spdx": "CC0-1.0", "source_url": "...", "redistribution_permitted": true, "attribution_required": false },
      "fetch":     { "type": "http", "urls": ["https://..."], "auth": null },
      "extract":   { "type": "passthrough" },
      "parse":     { "reader": "csv", "options": { "delimiter": "," } },
      "transform": { "handler": "tighten_types", "params": {} },
      "write":     { "output": "my-dataset.parquet", "compression": "zstd", "row_group_size_rows": 1048576, "statistics": true },
      "expect":    { "rows": 123456 }
    }
    ```

    Use the Python-load-edit-dump pattern from [`AGENTS.md`](AGENTS.md#safe-ways-to-edit-sourcesjson), not `sed`.

3. **Validate the manifest** before paying for a fetch:

    ```bash
    python -m scripts.pipeline.validate_manifest
    ```

    Catches typo'd handler names, missing required fields, and family-enum violations in under a second. See [Validating `sources.json`](#validating-sourcesjson) above.

4. **Run the build** for just this slug:

    ```bash
    python -m scripts.pipeline.build my-dataset
    ```

    The first run will fetch + extract + parse + transform + write + validate. If `expect.rows` was a guess and differs from the actual, re-run with `--loose` to get a warning instead of an error, then update the manifest.

5. **Regenerate derived docs:**

    ```bash
    python -m scripts.pipeline.docs
    ```

## Emitting a Vortex file alongside the Parquet

Vortex (https://github.com/spiraldb/vortex) is an optional stage-7 output — a sibling `<slug>.vortex` next to the prepared parquet.

1. Opt the spec in by adding a `convert` block:

    ```jsonc
    "convert": { "vortex": true }
    ```

2. Build (or just run the convert stage in isolation):

    ```bash
    python -m scripts.pipeline.build <slug>        # convert runs as stage 7
    python -m scripts.pipeline.convert <slug>      # just the convert stage
    python -m scripts.pipeline.convert --all       # every opted-in spec
    ```

3. The stage is idempotent: skips when `<slug>.vortex` is newer than `<slug>.parquet`. `vortex-data` is pulled in automatically by `uv sync` (pinned at `==0.69.0`); the old `vortex-array` package name is yanked.

4. **Hydrated companion is converted automatically.** When a slug has `convert.vortex: true` AND a hydrated parquet exists at `outputs/v{n}/<slug>/parquet-hydrated/<slug>.parquet`, the same convert run produces `outputs/v{n}/<slug>/vortex-hydrated/<slug>.vortex`. Same opt-in flag governs both pairs — there's no separate `convert.vortex_hydrated`. The `python -m scripts.pipeline.hydrate <slug>` stage also auto-runs this convert at the end of its work, so the hydrated parquet and its vortex sibling land together.

Known type-support gaps and the per-slug opt-outs they drive are queryable via `python -m scripts.pipeline.list_datasets --no-vortex --json` (the `vortex_skip_reason` field is in each spec's JSON). Today: `wikipedia-structured-contents` (FSB(16) UUIDs) and `ultrachat-200k` (nested list<struct> on chunked-array writes). Add a new opt-out by flipping `convert.vortex` to `false` and writing the reason; `validate_manifest` enforces that the boolean and the reason stay in lockstep.

Other format-level caveats:
  - VARIANT columns surface as their shredded struct on the round-trip (the VARIANT logical annotation isn't preserved) — see `_workdir/vortex_size_report.md` (gitignored, regenerate locally) for the resulting size blow-up on OpenLibrary.
  - Expect per-file overhead to dominate on very small parquets — the size ratio can exceed 1.0 for datasets under a few MB.
  - `Decimal128` works via the current `pf.read() → vxio.write(table)` path, despite older failures on `read_table()` routing. No special handling needed.

## Adding a Kaggle dataset gated behind ToS acceptance

Some Kaggle datasets (many academic re-uploads, certain restricted-licence mirrors) require a one-time click-through acceptance of the dataset's distribution terms on the Kaggle web UI before the API will serve downloads. Attempting to fetch one of these returns HTTP 403. (Note: 403 can also indicate the slug itself is wrong — double-check the Kaggle URL before reaching for this pattern.)

Use this pattern to document them in the manifest without breaking a clean-clone build:

1. Add the entry as a normal `fetch.type: "kaggle"` spec, but set `fetch.requires_interactive_accept: true` and leave `expect.rows: null` (we don't know the count yet):

    ```jsonc
    "fetch": {
      "type": "kaggle",
      "urls": ["https://www.kaggle.com/datasets/<owner>/<dataset>"],
      "auth": "kaggle",
      "requires_interactive_accept": true,
      "notes": "Kaggle gates this dataset behind a one-time click-through ToS acceptance."
    },
    "expect": { "rows": null, "notes": "Row count populated after the first successful build." }
    ```

2. The pre-flight print from `fetch_kaggle` will announce `kaggle (ToS-gated): ...` instead of the plain `kaggle: ...` line, and any 403 response will be caught and re-raised with a multi-line message pointing the user at the exact URL and telling them to click Download once.

3. Once the user clicks through in a browser (signed into Kaggle), the next build succeeds. Update the manifest's `expect.rows` with the actual count.

The 403 handling is generic — it triggers whether or not `requires_interactive_accept` is set — so forgetting the flag still yields a useful error; the flag only improves the up-front announcement.

## Adding a new transform handler

Use a dedicated handler when the default `tighten_types` / `identity` path can't produce the right shape — e.g. the source needs row-level JSON parsing, streaming to avoid OOM, multi-output splitting, or VARIANT columns.

1. **Create the handler** at `scripts/pipeline/handlers/<name>.py`. Signature:

    ```python
    def <name>(spec: dict, parsed: list[tuple[Path, pa.Table | None]], **params
               ) -> list[tuple[str, pa.Table]]:
        ...
    ```

    - `parsed` contains one `(path, table)` tuple per parsed file; `table` is `None` when `parse.reader = "custom"`.
    - Return `[(output_slug, table), ...]` — one tuple per output parquet. Multi-output handlers emit several slugs from one source (see `glove_split`, `osm_pbf_split`, `stack_exchange_split`).
    - **Streaming handlers** (write direct to parquet, bypass the write stage) return `[]`. See [below](#writing-a-streaming-handler).

2. **Register** in `scripts/pipeline/handlers/__init__.py`:

    ```python
    from .<name> import <name>
    _REGISTRY = {
        ...
        "<name>": <name>,
    }
    ```

3. **Wire in the manifest:**

    ```jsonc
    "transform": { "handler": "<name>", "params": { ... } }
    ```

4. **Smoke-test:** `.venv/bin/python -c "from scripts.pipeline.handlers import _REGISTRY; print('<name>' in _REGISTRY)"`.

## Writing a streaming handler

Use this pattern when the full dataset can't fit in memory and you need to spill to disk during ingestion.

Template (based on `jsonbench_variant_parse`, `wikipedia_variant_parse`, `lichess_pgn_parse`):

```python
from ..spec import REPO_ROOT, duckdb_connect, spec_field, outputs_root

def my_streaming_handler(spec, parsed):
    # 1. Resolve output path
    out_path = outputs_root() / spec["slug"] / spec_field(
        spec, "write.output", f"{spec['slug']}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    compression = spec_field(spec, "write.compression", "zstd")

    # 2. Set up scratch DB under _workdir/<slug>/ — helper applies v1.5.0
    workdir = REPO_ROOT / "_workdir" / spec["slug"]
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "build.duckdb"
    if db_path.exists(): db_path.unlink()
    con = duckdb_connect(db_path)

    try:
        # 3. Stream input into a DuckDB table, then COPY TO PARQUET
        ...
    finally:
        con.close()
        if db_path.exists(): db_path.unlink()

    print(f"  wrote {out_path.relative_to(REPO_ROOT)}")
    return []  # <-- streaming contract: write stage is a no-op
```

`spec.parse.reader = "custom"` is how the manifest tells the pipeline to skip the normal parser and hand raw file paths to the handler.

## Promoting a JSON column to VARIANT

Two paths depending on whether the parquet already exists:

**Existing parquet (in-place tightening):**

```bash
python -m scripts.pipeline.tighten_variant           # all built parquets
python -m scripts.pipeline.tighten_variant <slug>... # specific slugs
python -m scripts.pipeline.tighten_variant --dry-run
```

The pass is idempotent: parquets already lacking JSON-annotated columns are skipped. Uses `CAST(col AS VARIANT)` + atomic tmp-rename. Heavily-nested payloads (e.g. Open Food Facts) need a generous `RAINCLOUD_DUCKDB_MEMORY_LIMIT`; 96 GB is the tested ceiling.

**New-build handler (emit VARIANT from the start):**

Write a streaming handler that uses DuckDB's `CAST(to_json(col) AS VARIANT)` inside the `COPY ... TO PARQUET` statement. See `factbook_variant_parse` for the simplest 1-column example and `wikipedia_variant_parse` for the multi-column-with-typed-siblings example.

VARIANT requires a persistent DuckDB DB opened at `storage_compatibility_version=v1.5.0` — `duckdb_connect(db_path)` applies that automatically.

## Tightening existing integer / binary-string columns

`tighten_types` is the default handler for simple CSV-sourced datasets. Running it on a parsed pyarrow Table:

- Narrows integer columns by min/max (`int64 → uint8/uint16/int32/...`).
- Re-annotates `binary` columns as `string` when their bytes are valid UTF-8 (fixes the common DuckDB/ClickHouse export pattern where VARCHAR ships as unannotated BYTE_ARRAY).

To apply it to a new dataset, set `"transform": {"handler": "tighten_types"}` in the manifest. Runs automatically for the 62 slugs already wired.

There's no standalone in-place `tighten_types` script (unlike `tighten_variant.py`) because the integer width / string annotation pass is cheap enough to do during build rather than as a post-pass.

## Hydrating a URL column

Optional stage 8 — for slugs that opted in via a `hydrate` block in the manifest, dereference each row's URL into a sibling parquet at `outputs/v{n}/<slug>/parquet-hydrated/<slug>.parquet`. Off the default build path; invoke separately:

```bash
python -m scripts.pipeline.hydrate <slug>             # one slug
python -m scripts.pipeline.hydrate <slug> --limit 100 # first 100 rows (recommended for first run)
python -m scripts.pipeline.hydrate --all              # every spec with hydrate
```

The hydrated parquet is a **separate, deliberately sketchy artefact tier**: no file-size guarantees, no reproducibility (URLs die, content drifts), no completeness (rows that fail filter / fetch keep null `<output_column>` cells, with a per-row `_hydrate_provenance` struct recording why).

### Safety filter

Always on by default. Layered, all opt-out:

| Layer | How to extend | How to disable |
|---|---|---|
| Scheme allowlist (`http`/`https` only) | — | bypass-only (see below) |
| Per-slug `hydrate.blocked_hosts_extra` | edit the manifest | bypass-only |
| Per-run blocklist | `--block FILE` (repeatable; one host per line; `/etc/hosts`-style accepted; `#`-comments stripped) | omit the flag |
| URLhaus | `--urlhaus` (off by default; cached 24h under `_workdir/.urlhaus.hostfile`) | omit the flag |

Raincloud ships the **mechanism**, not the **policy** — no static "unsafe" list is bundled. Plug in upstream filter sources you trust ([StevenBlack/hosts](https://github.com/StevenBlack/hosts), URLhaus, IWF feeds for members, your corporate DNS list) or run hydration behind a DNS-filtered network (CleanBrowsing, Quad9, Cloudflare 1.1.1.2). See [`HYDRATING.md`](HYDRATING.md) for the full discussion.

### Bypass

Both flags are required — single-flag accident is impossible:

```bash
python -m scripts.pipeline.hydrate <slug> --unsafe-allow-all-domains --i-accept-the-risk
```

The bypass is for narrow research use against URL columns you've separately verified. **Do not** suggest it as a default. A multi-line warning prints regardless.

### Tuning

- `--concurrency N` (default 8) — many origins rate-limit aggressively; raise carefully.
- `--timeout SEC` (default 30).
- `--max-bytes N` (default 10 MB) — per-row payload cap; truncated rows record `error="truncated"` in provenance.

### Vortex companion

When the slug has `convert.vortex: true`, the hydrate stage **auto-runs** vortex conversion at the end of its work, writing `outputs/v{n}/<slug>/vortex-hydrated/<slug>.vortex` alongside the hydrated parquet. Same opt-in flag as the base — no separate manifest field. Standalone `python -m scripts.pipeline.convert <slug>` also handles both pairs in one invocation.

After a hydrate run, rerun `python -m scripts.pipeline.docs hydrated` if you've edited any `hydrate` blocks (the doc generates from the manifest, not from runs).

## Debugging a failing build

1. Run with `--loose` to downgrade row-count mismatches from errors to warnings: `python -m scripts.pipeline.build <slug> --loose`.
2. Invoke individual stages to isolate: `python -m scripts.pipeline.fetch <slug>`, `python -m scripts.pipeline.extract <slug>`.
3. Check `outputs/raw_downloads/<slug>/` — fetch skips when `expected_bytes` / `expected_sha256` match. Delete the dir to force a re-fetch.
4. Check `_workdir/<slug>/` — contains extract stage output. If the handler complains "no .xxx files", look here first.
5. For DuckDB OOM / swap issues: cap memory via `RAINCLOUD_DUCKDB_MEMORY_LIMIT` and redirect spill via `RAINCLOUD_DUCKDB_TEMP_DIRECTORY`.

## Running a large build safely

```bash
RAINCLOUD_DUCKDB_MEMORY_LIMIT=32GB \
RAINCLOUD_DUCKDB_TEMP_DIRECTORY=/mnt/scratch/duckdb-tmp \
PYTHONUNBUFFERED=1 \
  nohup python -m scripts.pipeline.build wikipedia-structured-contents --loose --clean-workdir \
    > /tmp/wiki-build.log 2>&1 &
```

Flags that matter for batch runs:
  - `--loose` — first build of a new slug; you don't yet know the exact row count, so downgrade `expect.rows` mismatches to warnings.
  - `--clean-workdir` — wipe `_workdir/<slug>/` after each successful build. Essential when running a whole family at once; otherwise decompressed CSVs (Public BI can hit ~100 GB for one workload) accumulate.
  - `PYTHONUNBUFFERED=1` — makes the tee'd log file update line-by-line instead of flushing only on buffer fill, so progress is inspectable mid-run.

Monitor:

```bash
tail -f /tmp/wiki-build.log
du -sh _workdir/wikipedia-structured-contents/    # scratch growth
df -h .                                           # disk headroom
```

## Regenerating specific docs

```bash
python -m scripts.pipeline.docs            # all three (datasets.md + handlers.md + snapshot.json)
python -m scripts.pipeline.docs datasets   # just datasets.md
python -m scripts.pipeline.docs handlers   # just handlers.md (registry + manifest usage)
python -m scripts.pipeline.docs snapshot   # just snapshot.json (per-slug schema + sizes)
```

Writes land in `docs/{datasets.md, handlers.md, snapshot.json}` (gitignored scratch). To promote, copy to the tracked `docs/v{schema_version}/`.

Regenerate **after** any of: build, convert run, in-place tightening, manifest edit that changes short_name / license / description / family / expect.rows. Skip if the change doesn't affect the catalog or the handler registry.

**`snapshot.json` is the load-bearing fallback** — `datasets.md` regen reads it for any slug whose parquet isn't on disk locally (otherwise the row would dash out the row count, sizes, and column-derived "Data Kind" tag). The no-args form keeps snapshot + datasets in lockstep; if you do a partial regen with `docs.py datasets`, run `docs.py snapshot` first (or just use the no-args form) so the table doesn't drift.

The other catalog views (columns, coverage, vortex-skip, hydration candidates) are no longer markdown — query them via `python -m scripts.pipeline.list_datasets --columns / --coverage / --no-vortex / --hydrate` or interactively in the TUI (`python -m scripts.pipeline.browse`).

## Removing a dataset

1. Remove the `DatasetSpec` entry from `sources.json` (Python load-edit-dump).
2. Delete the output parquet: `rm -rf outputs/v1/<slug>/`.
3. Optionally delete the raw cache: `rm -rf outputs/raw_downloads/<slug>/` (only if you're certain you won't re-add it).
4. Regenerate docs: `python -m scripts.pipeline.docs`.

Don't bother with backwards-compat shims — removed means removed. Git history is the fallback (`.archive/` is gitignored and only present on the maintainer's tree).
