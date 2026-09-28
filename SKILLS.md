# SKILLS.md

Playbooks for common operations in this repo. Each section is a self-contained recipe — copy and adapt.

Prereqs: Python 3.11+ and [uv](https://docs.astral.sh/uv/). A bare `uv sync --inexact` installs only the lightweight loader (`pyarrow`, `numpy`, `fsspec`, `platformdirs`); **running the build pipeline needs the `build` extra**, the pipeline core (`uv sync --extra build --inexact`). A dataset whose format or source needs more names its extra when it is missing; add it as another `--extra`: `osm`, `sas`, `excel` and `archives` for handler formats, `generated`, `kaggle` and `huggingface` for acquisition, `dev` for `pytest` and `ruff`, or `all` for everything. What each extra installs is declared once, in `[project.optional-dependencies]` in [`pyproject.toml`](pyproject.toml). Always pass `--inexact` so subsequent extras accumulate instead of overwriting prior ones (uv's default is "exact" sync, which removes anything not requested by the current invocation). Invoke Python as `.venv/bin/python` (or activate the venv).

## Index

- [Loading prepared datasets](#loading-prepared-datasets) — `raincloud.load` from cache / mirror
- [Releasing to this machine's store](#releasing-to-this-machines-store) — `publish --store`
- [Publishing artefacts to a mirror](#publishing-artefacts-to-a-mirror) — `publish --mirror`
- [Opening a DuckDB connection](#opening-a-duckdb-connection)
- [Running the test suite](#running-the-test-suite) — pytest regression net
- [Querying the catalog](#querying-the-catalog) — `list_datasets` filters
- [Validating `sources.json`](#validating-sourcesjson) — schema + cross-checks
- [Adding a new dataset](#adding-a-new-dataset)
- [Emitting a Vortex file alongside the Parquet](#emitting-a-vortex-file-alongside-the-parquet)
- [Adding a Kaggle dataset gated behind ToS acceptance](#adding-a-kaggle-dataset-gated-behind-tos-acceptance)
- [Adding a new transform handler](#adding-a-new-transform-handler)
- [Writing a streaming handler](#writing-a-streaming-handler)
- [Promoting a JSON column to VARIANT](#promoting-a-json-column-to-variant)
- [Tightening existing integer / binary-string columns](#tightening-existing-integer--binary-string-columns)
- [Hydrating a URL column](#hydrating-a-url-column) — a `<parent>-hydrated` dataset
- [Debugging a failing build](#debugging-a-failing-build)
- [Running a large build safely](#running-a-large-build-safely)
- [Regenerating specific docs](#regenerating-specific-docs)
- [Removing a dataset](#removing-a-dataset)

Templates referenced by the playbooks: [`templates/minimal_spec.json`](templates/minimal_spec.json) (new manifest entry), [`templates/streaming_handler.py.tmpl`](templates/streaming_handler.py.tmpl) (memory-constrained handler).

## Loading prepared datasets

Use the importable `raincloud` loader to pull an *already-prepared* artefact instead of rebuilding it. The base install (a `pip install` from the GitHub repo, or a bare `uv sync --inexact`) is the lightweight loader only — add `[s3]` / `[http]` for a remote mirror, `[pandas]` for `.to_pandas()`; `.dataset()` needs only pyarrow (plus `[vortex]` for Vortex files).

```python
import raincloud
ds = raincloud.load("countries-of-the-world")  # lazy handle, auto selects an installed reader
table = ds.to_arrow()                           # materialize when you want it
path  = ds.path()                               # or just the cached file path
```

Resolution is **local data/cache → mirror**. Missing prepared files raise an error. Build explicitly with `raincloud build SLUG`, or opt in with `load(SLUG, build=True)` and the `[build]` extra. `RAINCLOUD_MIRROR` selects a private/internal artifact store (`s3://bucket/prefix`, `file:///path`). The catalog is the authority on each artifact's sha256 and byte size: a local file at its key with the catalog's size is the artifact, and bytes from a mirror must match the catalog's sha256 (or its size, when it records none) or they are refused with `ChecksumMismatch`. No build changes the catalog. A build records what it produced in this install's build record (`<data_dir>/builds.json`), and the loader serves a local file that differs from the catalog's (upstream drift) when the build record names it at its size, built from the dataset's current recipe.

```bash
export RAINCLOUD_MIRROR=s3://your-bucket/raincloud   # point CI at a prepared-artefact bucket
export RAINCLOUD_CACHE=/var/cache/raincloud          # optional cache-dir override
export RAINCLOUD_OFFLINE=1                            # local only: never contacts the mirror; misses raise OfflineMiss
```

Use `raincloud init --data-dir /mnt/data/raincloud --scratch-dir /mnt/fast/raincloud` for optional local settings. `raincloud config show` reports effective paths. Add `[vortex]` for Vortex reads; `raincloud capabilities` reports installed readers. Catalog update/pin/rollback is explicit and independent of package installation; see [README](README.md#pinning-a-catalog).

## Releasing to this machine's store

A machine that serves prepared data to its users has one shared content store (an operator-owned `data_dir`) and a catalog pack directory that users' system config names as their `catalog`. A maintainer releases into both from a reviewed commit:

```bash
python -m raincloud.pipeline.publish <slug>... --store /path/to/store --catalogs /path/to/catalogs --dry-run   # preview
python -m raincloud.pipeline.publish <slug>... --store /path/to/store --catalogs /path/to/catalogs
python -m raincloud.pipeline.publish --all --store /path/to/store --catalogs /path/to/catalogs
```

- Each artifact's sha256 is checked against the selected catalog's snapshot (`docs/v{n}/snapshot.json` in a checkout) before it enters the store, then placed as a hard link (a copy across filesystems) and renamed into place. An artifact whose snapshot entry records no sha256 is placed ungated, and one already in the store as the same file is not re-hashed (so when `data_dir` is the store, nothing sha-checked a build written straight into it).
- A hard link or copy keeps the builder's owner and mode. Build under a umask (or group) that lets the store's other users read the files.
- `--catalogs DIR` releases the catalog: it writes the selected catalog into `DIR` as a revision and rewrites `DIR/latest.json` to point at it. Before placing anything it refuses a catalog that names an artifact the store would not hold at the recorded size. Without `--catalogs`, only artifacts are placed and the released catalog is unchanged.
- Users whose `catalog` setting names the pack directory (normally in the machine's system config) follow the release on their next read, with no config edit. A dataset handle already open keeps the catalog metadata it opened with, but store keys are not content-addressed: a file under an unchanged key is replaced in place, so a handle that reads after a release can get the new bytes (or a size mismatch against its older metadata).
- Rolling back restores catalog metadata only. Releasing the previous commit's catalog works while the store still holds that catalog's bytes; once a release replaced them, rebuild the previous commit's artifacts and place them with `--store` before releasing its catalog, or `--catalogs` refuses (sizes differ) or names bytes the store no longer holds (sizes match).
- The license gates do not apply: the store serves this machine's users, which is not redistribution. `--allow-*` flags have no effect with `--store`.

## Publishing artefacts to a mirror

Maintainers upload built artefacts to an off-machine mirror with `--mirror`. Each artefact's on-disk sha256 must match the selected catalog's snapshot (`docs/v{n}/snapshot.json`, today `docs/v2/`), otherwise the upload is refused with a `PublishMismatch`. An artefact whose snapshot entry records no sha256 is uploaded ungated.

```bash
python -m raincloud.pipeline.publish countries-of-the-world --mirror s3://your-bucket/raincloud
python -m raincloud.pipeline.publish --all --mirror file:///path/to/mirror --dry-run   # preview the plan
```

After a rebuild, the snapshot must record the new bytes before they can be published. A plain regeneration takes each file's sha256 and writer from this install's build record, so run it after a local build or export, promote all three scratch copies, and commit them:

```bash
python -m raincloud.pipeline.docs
cp docs/snapshot.json docs/datasets.md docs/handlers.md docs/v2/
git diff docs/v2/
```

Keep `docs snapshot --rehash` for files the build record does not describe (built elsewhere, or by an older raincloud). It bypasses the record and re-hashes every file, so a rehashed file whose sha changed loses its recorded writer: Parquet falls back to the writer its `created_by` names, and Vortex to none.

Mirror uploads are redistribution, so two license gates apply: a spec whose license carries a `scrape_advisory`, or sets `redistribution_permitted: false`, is refused unless you pass `--allow-scrape-advisory` or `--allow-no-redistribution` respectively. The mirror is a private/internal store you control — there is no public Raincloud-hosted endpoint, and publishing here does not change the no-redistribution posture in [`DISCLAIMER.md`](DISCLAIMER.md). The `--mirror` base is a writable `fsspec` URL: `s3://` needs `[s3]`, `file://` needs nothing. An `https://` mirror can serve readers (with `[http]`) but is not writable, so `publish` refuses it (exit 2) before planning anything.

## Opening a DuckDB connection

Raincloud code, tests and examples always go through `raincloud.duckdb_connect` instead of `duckdb.connect(...)`. It applies Raincloud's env-var-driven resource limits and the `storage_compatibility_version=v1.5.0` setting required for persistent VARIANT writes.

```python
from raincloud import duckdb_connect

# In-memory, default config
con = duckdb_connect()

# Persistent DB (automatically gets storage_compatibility_version=v1.5.0)
con = duckdb_connect("/path/to/build.duckdb")

# Override or extend
con = duckdb_connect(extra_config={"preserve_insertion_order": False})
```

Env vars the helper honours (see [`AGENTS.md`](AGENTS.md#data-locations)):

- `RAINCLOUD_DUCKDB_MEMORY_LIMIT` — e.g. `8GB`, `96GB`. **Set this for large builds.** Default (unset) lets DuckDB grab ~80% of system RAM, which can swap-thrash on heavily-nested VARIANT shredding.
- `RAINCLOUD_DUCKDB_THREADS` — int.
- `RAINCLOUD_DUCKDB_TEMP_DIRECTORY` — path for spill files.

## Running the test suite

```bash
uv sync --extra dev --extra all --inexact   # one-time — pytest plus every optional part
pytest                                      # the full hermetic suite (minutes)
python -m raincloud.pipeline.validate_manifest && pytest tests/test_manifest.py   # the fast gate after a manifest edit
```

The suite is hermetic by default: it writes only to temporary directories, never to your data directory or the tracked docs, and it runs real small builds (`build.run_one`, build subprocesses) rather than mocking them. Tests that need live upstreams or a built wheel are opt-in, with `--run-network` and `--run-wheel`. Run it after any change to `sources.json`, `sources.schema.json`, `raincloud/_registry.py`, `templates/` or `examples/`; the manifest tests exercise the same `validate_manifest` codepath the `/raincloud-validate-manifest` skill runs.

## Querying the catalog

Filter the manifest from the command line instead of grepping the JSON or scrolling [`docs/v2/datasets.md`](docs/v2/datasets.md):

```bash
python -m raincloud.pipeline.list_datasets --handler uci_default         # one slug per line
python -m raincloud.pipeline.list_datasets --handler tighten_types --long
python -m raincloud.pipeline.list_datasets --kaggle-tos
python -m raincloud.pipeline.list_datasets --reader csv --vortex --count
python -m raincloud.pipeline.list_datasets --grep '\bgeo' --long
python -m raincloud.pipeline.list_datasets --license CC0-1.0 --json | jq -s 'length'
```

Filters compose with AND. `--kaggle-tos` selects datasets gated behind a one-time click-through, Kaggle or Hugging Face. `--vortex` / `--no-vortex` say whether the catalog has Vortex for a dataset: `--no-vortex` lists one whose Vortex writer a build measured unable to produce the file (`--json` carries the measurement as `vortex_unavailable`) or whose export policy leaves Vortex out. The default output is one bare slug per line; on a terminal hydrated datasets are marked `[hydrated]`. `--long` emits a wide table (slug, handler, fetch type, reader, license, rows, vortex, scrape, hydrated, `recorded` — what the tracked catalog records — and `local` — the formats prepared on this install's disk), `--json` emits one row per object for piping into `jq`, `--count` emits just the match count. `--local` keeps only datasets with a file on this machine. Read-only and fast on the full manifest. Pair with `/raincloud-status` when you need filesystem state for a returned slug.

## Validating `sources.json`

A static check that takes seconds; safe to run after any manifest edit and before triggering a build:

```bash
python -m raincloud.pipeline.validate_manifest                      # the selected catalog's manifest
python -m raincloud.pipeline.validate_manifest path/to/sources.json # a named file
python -m raincloud.pipeline.validate_manifest --json               # machine-readable
python -m raincloud.pipeline.validate_manifest --strict             # warnings → errors
```

It prints which manifest it validated: `RAINCLOUD_MANIFEST`, the checkout's `sources.json`, or the copy installed with raincloud.

Two layers:

1. **JSON Schema** ([`sources.schema.json`](sources.schema.json), Draft 2020-12) — shape, enums, regexes, required fields. Uses the `jsonschema` package (declared in the `[build]` extra); skipped with a hint if missing.
2. **Cross-checks** the schema can't express:
   - slug uniqueness across `datasets[]`
   - every `transform.handler` is declared in `HANDLERS` in `raincloud/_registry.py` (the handler registry is derived from it)
   - declared handlers referenced by 0 specs (orphans → warning)
   - `derive.from` names an ordinary (non-derived) dataset, and a hydrated dataset is named `<parent>-hydrated`
   - `fetch.urls` non-empty unless `fetch.type` is `custom` or `generated`
   - a `generated` fetch names a registered generator, valid parameters and one of its outputs
   - `fetch.auth` matches `fetch.type` for `kaggle` / `huggingface`
   - `fetch.requires_interactive_accept` only on `fetch.type` `kaggle` or `huggingface`
   - which formats a dataset exports: in v2, `export.formats` names the formats it wants (`parquet`/`vortex`) and `convert.*` is rejected (v1 pairs `convert.vortex` with `convert.vortex_skip_reason`). A writer's limitation is never declared: the build measures it
   - `export.priority` (and the catalog's `export_priority`), a list or a per-format map, names writers that have an export cell, and a list names a writer for every format the dataset exports: a list serves every format and does not fall through, so `["hardwood"]` (a Parquet-only writer) on a dataset that exports Vortex is an error

Exit code: `0` on success (warnings allowed), `1` on errors.

The companion [`sources.schema.md`](sources.schema.md) is the human-friendly reference; `sources.schema.json` is the machine version. Keep them in lockstep when adding fields.

## Adding a new dataset

1. **Identify the upstream.** Get a stable public URL (prefer the publisher's canonical endpoint over a mirror) and record its license accurately: SPDX id, `source_url`, `redistribution_permitted`, and `scrape_advisory` for broad-web crawls. A license that forbids redistribution does not keep a dataset out of the catalog; it keeps it off mirrors (`publish --mirror` refuses it).
2. **Append a `DatasetSpec` to `sources.json`.** Copy [`templates/minimal_spec.json`](templates/minimal_spec.json), which validates as-is, and edit its placeholders; [`sources.schema.md`](sources.schema.md) documents every field. Use the Python-load-edit-dump pattern from [`AGENTS.md`](AGENTS.md#editing-sourcesjson), not `sed`.

3. **Validate the manifest** before paying for a fetch:

    ```bash
    python -m raincloud.pipeline.validate_manifest
    ```

    Catches typo'd handler names, missing required fields, and closed-vocab violations (tags / showcase / SPDX) in seconds. See [Validating `sources.json`](#validating-sourcesjson) above.

4. **Run the build** for just this slug:

    ```bash
    python -m raincloud.pipeline.build my-dataset
    ```

    The first run will fetch + extract + parse + transform + write the canonical + validate + export. If `expect.rows` was a guess and differs from the actual, the validate stage emits a `[WARN]` (row-count drift does not fail a build unless you pass `--strict`) — read the actual count off that warning and update the manifest.

5. **Regenerate derived docs:**

    ```bash
    python -m raincloud.pipeline.docs
    ```

## Emitting a Vortex file alongside the Parquet

Vortex (https://github.com/spiraldb/vortex) is one of the default exports: under `schema_version` 2 a dataset exports `vortex/<slug>.vortex` from its canonical Arrow file, beside `parquet/<slug>.parquet`.

1. `export.formats` is the only declaration of which formats a dataset wants. Without it, the defaults export Parquet and Vortex. A deliberate policy may leave Vortex out (`"export": {"formats": ["parquet"]}`, with `notes` if the reason is worth keeping). Do **not** leave it out because the Vortex writer fails on the data: keep it listed and let the build measure that. (`convert.vortex` is v1-only; v2 validation rejects it.)

2. Build all requested exports, or refresh only the Vortex file from the canonical already on disk:

    ```bash
    python -m raincloud.pipeline.build <slug>
    python -m raincloud.pipeline.export <slug> --format vortex
    ```

    `export` records what it wrote in the build record, as a build does. It refuses a canonical that is neither this install's build nor the catalog's file, since nothing exported from it could be recorded; rebuild that one. `--format` overrides the dataset's `export.formats` for the run, so `--format vortex` forces a Vortex export even where the dataset's policy leaves Vortex out. `vortex-data` is included in `[vortex]` and `[build]`, pinned to `0.86.1` alongside the Rust and JNI lanes.

3. Every writer reads back what it wrote, compared with the canonical, before the file is promoted. When the Vortex writer cannot produce the file for this dataset (it raises, dies, writes a file that does not read back to the canonical or cannot be read at all, or runs past `RAINCLOUD_EXPORT_TIMEOUT`, default 6 h), the build does not fail. The previous file, if any, comes back; the failure is recorded in the build record as the format's *unavailable* measurement (writer cell, error, toolchain versions, recipe, time); the dataset is built with the formats that worked; and the build prints `[unavailable] <slug>/vortex` and repeats it in its summary, exiting 0. `raincloud describe <slug>` quotes the measurement, `load(..., format="vortex")` raises `FormatUnavailable` quoting it, and automatic format selection skips Vortex. Regenerating docs carries the measurement into the catalog snapshot (`vortex_unavailable`). A later successful export replaces it: after a toolchain upgrade, run `python -m raincloud.pipeline.export <slug> --format vortex` (exit 1 if Vortex is still unavailable) and regenerate. A recorded failure is not repeated: while the measurement that applies (this install's build record at the recipe, else the catalog's) names the same writer cell, the same toolchain versions and the same canonical, `build`, `export` and `convert` skip the format with `[skip] <slug>/vortex: ...; pass --retry-errors to try again` and keep the measurement (a `build`, or an `export` without `--format`, still exits 0 and lists it in the summary; `export --format vortex` and `convert` exit 1). A changed toolchain is attempted on its own, with a `[retry]` line saying what changed; `--retry-errors` attempts it with nothing changed. `compliance` prints `[stale opt-out]` when a writer round-trips a format the catalog records as unavailable, and docs regeneration warns when the compliance ledger says so. A sidecar writer that could not verify its own file (`"roundtrip": null`: a comparator gap, or out of memory while verifying) still publishes it; the build prints `[unverified] <slug>/<fmt>` with the writer's reason and lists it in the summary, the build record and, after regeneration, the snapshot mark it (`verified: false` with `verify_note`; `<fmt>_verified` / `<fmt>_verify_note`), and `raincloud describe` shows the format as UNVERIFIED with the reason.

Datasets without a Vortex file, with the measured reason (or the policy note) for each, are listed by `python -m raincloud.pipeline.list_datasets --no-vortex --json`.

Other format-level caveats:
  - VARIANT columns surface as their shredded struct in Vortex (the VARIANT logical annotation isn't preserved), which can make a `.vortex` file much larger than the Parquet on heavily nested data such as Open Library.
  - Expect per-file overhead to dominate on very small datasets — the Vortex/Parquet size ratio can exceed 1.0 below a few MB.

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

1. **Create the handler** at `raincloud/pipeline/handlers/<name>.py`. Signature:

    ```python
    def <name>(spec: dict, parsed: list[tuple[Path, pa.Table | BatchStream | None]], **params
               ) -> list[tuple[str, pa.Table | BatchStream]]:
        ...
    ```

    - `parsed` contains one `(path, table)` tuple per parsed file; `table` is a `BatchStream` for a reader the handler declares with `batches.batch_input`, and `None` when `parse.reader = "custom"`.
    - Return `[(output_slug, table), ...]` — one tuple per canonical output, a `Table` or a fixed-schema `BatchStream`. Multi-output handlers emit several slugs from one source (see `glove_split`, `osm_pbf_split`, `stack_exchange_split`).
    - **Streaming handlers** write the canonical Arrow spine themselves (incrementally, via `canonical.open_canonical_writer`), bypassing the `write_canonical` stage, and return `[]`; the exporters then derive parquet/vortex from that canonical like any other slug. See [below](#writing-a-streaming-handler).

2. **Declare** it in `HANDLERS` in `raincloud/_registry.py`:

    ```python
    # raincloud/_registry.py
    HANDLERS: dict[str, str] = {
        ...
        "<name>": "<name>:<name>",   # "<module>:<attr>" under handlers/
    }
    ```

That is the only place to add it. The registry imports the module on demand and
the catalog capability list is derived from the same declaration, so there is no
second file to keep in step.

3. **Wire in the manifest:**

    ```jsonc
    "transform": { "handler": "<name>", "params": { ... } }
    ```

4. **Smoke-test:** `.venv/bin/python -c "from raincloud.pipeline.handlers import _REGISTRY; print('<name>' in _REGISTRY)"`.

## Writing a streaming handler

Use this pattern when the full dataset can't fit in memory and you need to spill to disk during ingestion.

A streaming handler opens the canonical Arrow spine with `canonical.open_canonical_writer(spec["slug"], schema)` and writes `RecordBatch`es (or tables) into it incrementally, then `return []`. The build's `write_canonical` stage becomes a no-op for it, and `validate → run_exporters` (parquet@py + vortex@py) run over the canonical the handler wrote — exactly as for a table-path slug. Look at `lichess_pgn_parse` / `osm_pbf_split` / `stack_exchange_split` for real, current examples.

Use [`templates/streaming_handler.py.tmpl`](templates/streaming_handler.py.tmpl) for an executable NDJSON example: it ingests into a scratch DuckDB table, streams Arrow batches into `open_canonical_writer`, and removes its own scratch database.

A handler depends on two things `build.run_one` sets up around it, so run it only through the builder. `workdir_root()` is the recipe's own scratch directory only inside `spec.recipe_scratch()`; called elsewhere it is the unscoped scratch root. `open_canonical_writer` registers the canonical it writes with the build's outputs (`lifecycle.build_outputs`), which is what publishes it atomically when the build succeeds. Around those, `run_one` holds the operation lock on the configured roots and writes the build record.

```python
from raincloud import duckdb_connect

from ..canonical import open_canonical_writer

def my_streaming_handler(spec, parsed):
    with duckdb_connect() as con:
        reader = con.execute(
            "SELECT * FROM read_json_auto(?, format='newline_delimited')",
            [[str(path) for path, _ in parsed]],
        ).to_arrow_reader(65536)
        with reader, open_canonical_writer(spec["slug"], reader.schema) as writer:
            for batch in reader:
                writer.write_batch(batch)
    return []
```

For DuckDB VARIANT, use `duckdb_variant.stream_canonical_arrow` to bridge the query result to canonical Arrow with the required shredded representation and field marker. A plain DuckDB Arrow fetch cannot preserve that logical type. `stream_canonical_arrow` expects the SQL to project each VARIANT column through `variant_to_parquet_variant(...)` itself; `to_canonical_arrow` does that projection for you. See `jsonbench_variant_parse` and `wikipedia_variant_parse` for streaming examples.

`spec.parse.reader = "custom"` is how the manifest tells the pipeline to skip the normal parser and hand raw file paths to the handler.

## Promoting a JSON column to VARIANT

Promote JSON in the transform recipe and rebuild the canonical Arrow artifact and exports; exports are never rewritten in place, since they would diverge from the canonical spine. Heavily-nested payloads (e.g. Open Food Facts) need a generous `RAINCLOUD_DUCKDB_MEMORY_LIMIT`; 96 GB is the tested ceiling.

Build the VARIANT column in DuckDB from parsed JSON, `CAST(CAST(col AS JSON) AS VARIANT)` (a bare text-to-VARIANT cast would store the JSON as one string), then project it through `variant_to_parquet_variant(...)` — the scalar that shreds a VARIANT to the `struct<metadata, value, ...>` Arrow can carry — and write the **canonical Arrow spine**: for a small input, materialize the table with `duckdb_variant.to_canonical_arrow` and return `[(slug, table)]` (see `factbook_variant_parse`, ~1-column); for a large one, stream batch-by-batch through `duckdb_variant.stream_canonical_arrow` + `open_canonical_writer` and return `[]` (see `jsonbench_variant_parse`, single-column, and `wikipedia_variant_parse`, multi-column-with-typed-siblings). Either way the exporters derive Parquet/Vortex from the spine — pyarrow can't emit the VARIANT *logical* type, so the Parquet exporter records `variant_faithful=False` while preserving the shredded struct. The bridge's stamp (`variant.attach_variant` / `attach_variant_schema`) declares the struct's `metadata`, and an unshredded `value`, non-nullable, as the Parquet VARIANT spec and `arrow.parquet.variant` require (DuckDB exports every field nullable), so Parquet writers emit `required binary metadata`; a non-null VARIANT row missing either fails the build naming the column and rows. A NULL cell stays a null struct. A shredded `typed_value` that is itself a struct or list is refused, since its fields would need declaring too.

VARIANT requires a persistent DuckDB DB opened at `storage_compatibility_version=v1.5.0` — `duckdb_connect(db_path)` applies that automatically.

## Tightening existing integer / binary-string columns

`tighten_types` is the default handler for simple CSV-sourced datasets. Running it on a parsed pyarrow Table:

- Narrows integer columns by min/max (`int64 → uint8/uint16/int32/...`).
- Re-annotates `binary` columns as `string` when their bytes are valid UTF-8 (fixes the common DuckDB/ClickHouse export pattern where VARCHAR ships as unannotated BYTE_ARRAY).

To apply it to a new dataset, set `"transform": {"handler": "tighten_types"}` in the manifest. Runs automatically for the slugs already wired.

There's no standalone in-place `tighten_types` script because the integer width / string annotation pass is cheap enough to do during build rather than as a post-pass.

## Hydrating a URL column

A hydrated dataset is its own manifest entry, `<parent>-hydrated`, that derives from an ordinary dataset instead of fetching an upstream:

```json
{"slug": "laion-400m-hydrated", "short_name": "...", "full_name": "...", "description": "...", "license": {...},
 "derive": {"from": "laion-400m", "hydrate": {"columns": {"url": {"into": "content", "type": "binary"}}}},
 "advisory": "why a reader probably wants laion-400m instead"}
```

It has no fetch/extract/parse/transform stages: the build reads the parent's canonical Arrow, fetches each listed column's URLs into `<into>`, appends a `_<into>_provenance` struct per column, and exports as usual. Never built unless named (`build --all` skips it):

```bash
raincloud build laion-400m-hydrated                                   # safe defaults
python -m raincloud.pipeline.hydrate laion-400m-hydrated --limit 100  # a sample of the first 100 rows, in scratch
python -m raincloud.pipeline.hydrate --all                            # every hydrated dataset
```

`hydrate` also accepts the bare parent (`hydrate laion-400m`) for its `-hydrated` entry. Options that change what is fetched (`--limit`, `--block`, `--urlhaus`, a non-default `--max-bytes` or `--timeout`, the bypass) make the run a **sample** in scratch, never published or served; [`HYDRATING.md`](HYDRATING.md) has the full rule.

It is **deliberately sketchy**: no file-size guarantees, no reproducibility (URLs die, content drifts), no completeness (rows that fail filter / fetch keep null cells, with the provenance struct recording why). Loading one emits `HydratedDatasetWarning`.

### Safety filter

On by default. The scheme allowlist and `blocked_hosts_extra` apply unless the two-flag bypass is active; `--block` and `--urlhaus` are opt-in, and a run that uses them is a sample:

| Layer | Applies to | How to extend | How to disable |
|---|---|---|---|
| Scheme allowlist (`http`/`https` only) | the dataset and samples | — | bypass-only (see below) |
| Per-dataset `derive.hydrate.blocked_hosts_extra` | the dataset and samples | edit the manifest | bypass-only |
| Per-run blocklist | samples only | `--block FILE` (repeatable; one host per line; `/etc/hosts`-style accepted; `#`-comments stripped) | omit the flag |
| URLhaus | samples only | `--urlhaus` (off by default; cached 24h under `<scratch_dir>/.urlhaus.hostfile`) | omit the flag |

Raincloud ships the **mechanism**, not the **policy** — no static "unsafe" list is bundled. A blocklist that must govern the published dataset goes in `derive.hydrate.blocked_hosts_extra`, or run hydration behind a DNS-filtered network (CleanBrowsing, Quad9, Cloudflare 1.1.1.2); `--block` and `--urlhaus` preview what such a list would remove ([StevenBlack/hosts](https://github.com/StevenBlack/hosts), URLhaus, IWF feeds for members, your corporate DNS list). See [`HYDRATING.md`](HYDRATING.md) for the full discussion.

### Bypass

Both flags are required — single-flag accident is impossible:

```bash
python -m raincloud.pipeline.hydrate <parent>-hydrated --unsafe-allow-all-domains --i-accept-the-risk
```

The bypass turns off **every** layer above, including the scheme allowlist and the dataset's own `blocked_hosts_extra`: each URL is fetched whatever its scheme or host, and the run is a sample. Its fetched rows record `filter_decision = "allowed"`. It is for narrow research use against URL columns you've separately verified. **Do not** suggest it as a default. A multi-line warning prints regardless.

### Tuning

- `--concurrency N` (default 8) — many origins rate-limit aggressively; raise carefully. It never makes a sample.
- `--timeout SEC` (default 30) — a non-default value makes the run a sample.
- `--max-bytes N` (default 10 MB) — per-row payload cap; truncated rows record `error="truncated"` in provenance. A non-default value makes the run a sample.

After a hydrated build, regenerate with `python -m raincloud.pipeline.docs`. List hydrated datasets with `python -m raincloud.pipeline.list_datasets --hydrate --long`.

## Debugging a failing build

1. Row-count mismatches are `[WARN]` by default — a build only hard-fails on them under `--strict`. If a build IS failing on drift, drop `--strict` to see the rest of the pipeline run.
2. Invoke individual stages to isolate: `python -m raincloud.pipeline.fetch <slug>`, then `python -m raincloud.pipeline.extract <slug>`. These stage commands take slugs or `--all` (and `--help`), not the build's flags; an unknown name exits 2 with a did-you-mean. `fetch --verify` re-hashes receipt-backed HTTP (and UCI) downloads against their recorded sha256 instead of trusting their size.
3. Check the selected raw cache using `raincloud.pipeline.spec.raw_slug_dir(slug)` and the configured roots shown by `raincloud config show`. Invalidate only that generation's upstream payloads; preserve sibling `.recipes/` generations and internal metadata.
4. Check the configured scratch root's `.recipes/<recipe-hash>/<slug>/` directory — contains build extract output. If the handler complains "no .xxx files", look here first.
5. For DuckDB OOM / swap issues: cap memory via `RAINCLOUD_DUCKDB_MEMORY_LIMIT` and redirect spill via `RAINCLOUD_DUCKDB_TEMP_DIRECTORY`.

## Running a large build safely

Logs go to a persistent directory. Never `/tmp`: on many machines it is tmpfs, and a reboot mid-build would delete the only record of what failed. Pick a directory on durable disk, for example beside the data directory `raincloud config show` reports. Not beside the scratch root: outside a checkout that is the user cache, which is disposable:

```bash
LOG_DIR=/path/to/durable/logs      # e.g. beside the data dir, never under a cache or /tmp
mkdir -p "$LOG_DIR"
RAINCLOUD_DUCKDB_MEMORY_LIMIT=32GB \
RAINCLOUD_DUCKDB_TEMP_DIRECTORY=/mnt/scratch/duckdb-tmp \
PYTHONUNBUFFERED=1 \
  nohup python -m raincloud.pipeline.build wikipedia-structured-contents --clean-workdir \
    > "$LOG_DIR/build-wikipedia-structured-contents-$(date +%s).log" 2>&1 &
```

Flags that matter for batch runs:
  - (no `--strict`) — the default for a first build of a new slug: you don't yet know the exact row count, and `expect.rows` drift stays a warning. Add `--strict` only for CI / pre-release gates.
  - `--clean-workdir` — wipe only `<scratch_dir>/.recipes/<recipe-hash>/<slug>/` after each successful build. Standalone extraction uses the same directory. Essential when running large batches at once; otherwise decompressed CSVs (Public BI can hit ~100 GB for one workload) accumulate.
  - `PYTHONUNBUFFERED=1` — makes the log file update line-by-line instead of flushing only on buffer fill, so progress is inspectable mid-run.

Monitor:

```bash
LOG_DIR=/path/to/durable/logs      # the same directory; a new shell does not inherit it
tail -f "$LOG_DIR"/build-wikipedia-structured-contents-*.log
du -sh <scratch_dir>/.recipes/*/wikipedia-structured-contents/   # scratch growth
df -h <data_dir> <scratch_dir>                                    # disk headroom
```

## Regenerating specific docs

```bash
python -m raincloud.pipeline.docs            # all three (datasets.md + handlers.md + snapshot.json)
python -m raincloud.pipeline.docs datasets   # just datasets.md
python -m raincloud.pipeline.docs handlers   # just handlers.md (registry + manifest usage)
python -m raincloud.pipeline.docs snapshot   # just snapshot.json (per-slug schema + sizes)
```

For the checkout catalog, writes land in `docs/{datasets.md, handlers.md, snapshot.json}` (gitignored scratch); inspect before promoting to current `docs/v2/`. Frozen `docs/v1/` remains unchanged. Installed, pinned and custom catalogs write under `<data_dir>/.raincloud/observations/<catalog-revision>/`, outside both the installation and immutable bundle.

Regenerate **after** any of: build, export run, manifest edit that changes short_name / license / description / expect.rows. Skip if the change doesn't affect the catalog or the handler registry.

**`snapshot.json` is the load-bearing fallback** — `datasets.md` regen reads it for any slug whose parquet isn't on disk locally (otherwise the row would dash out the row count, sizes, and column-derived "Data Kind" tag). The no-args form keeps snapshot + datasets in lockstep; if you do a partial regen with `docs.py datasets`, run `docs.py snapshot` first (or just use the no-args form) so the table doesn't drift.

The other catalog views (columns, coverage, datasets without Vortex, hydrated datasets) are queryable rather than markdown — use `python -m raincloud.pipeline.list_datasets --columns / --coverage / --no-vortex / --hydrate` or interactively in the TUI (`python -m raincloud.pipeline.browse`).

## Removing a dataset

The procedure is the `/raincloud-remove-dataset` skill. In short:

1. **Check for dependents and resolve the paths** before editing the manifest. A `<slug>-hydrated` child (`list_datasets --hydrate --long`) goes with its parent, or is re-parented. `operation_lock` takes the store, raw and scratch locks while the paths are resolved, and releases them when the block ends:

    ```python
    from raincloud.pipeline.lifecycle import operation_lock
    from raincloud.pipeline.spec import outputs_root, raw_slug_dir, recipe_workdir_root

    slug = "my-dataset"
    with operation_lock(resources=True) as ctx:
        manifest = ctx.manifest
        spec = next(s for s in manifest["datasets"] if s["slug"] == slug)
        print(outputs_root() / slug)                       # built files, current schema_version
        print(raw_slug_dir(slug))                          # raw payloads, shared across versions
        print(recipe_workdir_root(spec, manifest) / slug)  # this recipe's scratch
    ```

2. **Delete what the user asked for, while no build is running.** The output directory and the recipe scratch, and the raw payloads only if requested. When the raw path printed is `<raw_dir>/<slug>` itself, delete its payload files and keep its `.recipes/` subdirectory, which holds other catalogs' generations. Never delete an older `outputs/v{n}/` (a frozen version; nothing regenerates it), a `.recipes/` generation other than the one printed, or through a symlink (unlink the link itself).
3. **Remove the `DatasetSpec`** from the checkout's `sources.json` (Python load-edit-dump), or package a new bundle for an installed catalog; never edit an installed revision in place.
4. **Regenerate and promote the docs:** `python -m raincloud.pipeline.docs`, then copy `docs/{snapshot.json,datasets.md,handlers.md}` into `docs/v2/` and review the diff. If a handler is now unused, delete its module and its `HANDLERS` entry in `raincloud/_registry.py`.

Don't bother with backwards-compat shims — removed means removed. Git history is the fallback (`.archive/` is gitignored and only present on the maintainer's tree).
