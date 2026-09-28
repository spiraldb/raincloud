# `sources.json` schema (v1/v2)

> **Machine-readable companion:** [`sources.schema.json`](sources.schema.json) is the JSON Schema (Draft 2020-12) version of this document. Run `python -m raincloud.pipeline.validate_manifest` to validate `sources.json` against it plus cross-checks the schema can't express (handler registry, slug uniqueness, etc.). Keep both files in lockstep when adding fields.

This document defines the shape of `sources.json`, the manifest that drives the Raincloud pipeline. The file declares, per dataset, **how to fetch**, **how to extract**, **how to parse**, **how to transform**, and **how to validate** canonical Arrow under `outputs/v{schema_version}/<slug>/arrow/`, from which configured exporters derive sibling formats (v1 validates Parquet). Stages are processed by separate scripts under `raincloud/pipeline/`; each stage reads only the fields it needs so scripts can be developed, tested, and re-run independently.

## Top-level shape

```jsonc
{
  "schema_version": 2,                    // Current catalog is v2; legacy v1 is also accepted.
                                          // v2 adds canonical Arrow (arrow/<slug>.arrow.zstd)
                                          // and the optional per-slug `export` block.
  "datasets": [ /* DatasetSpec, one per dataset */ ]
}
```

An optional top-level `export_priority` sets a catalog-wide writer order, in the same list-or-map shape as `export.priority` (see [`export`](#export-object-optional-v2-only)); omit the key rather than set it to `null`.

## `DatasetSpec`

```jsonc
{
  /* Identity (used by every stage) */
  "slug": "clickbench-hits",            // kebab-case; matches outputs/v{n}/<slug>/parquet/<slug>.parquet
  "short_name": "ClickBench Hits",      // table-friendly label
  "full_name": "ClickBench Hits (Yandex Metrica log)",
  "description": "100M-row web-analytics event log used by the ClickBench OLAP benchmark.",

  /* License (driven by the license-audit pass — machine-readable) */
  "license": {
    "spdx": "Apache-2.0",               // SPDX id OR free-form token if no SPDX. Describes the aggregator's declared license; see `scrape_advisory` for the gap between that and any uncleared underlying content.
    "source_url": "https://github.com/ClickHouse/ClickBench/blob/main/LICENSE",
    "redistribution_permitted": true,   // what the audit confirmed
    "attribution_required": true,
    "notes": null,
    "scrape_advisory": null             // null for cleared datasets. When non-null, holds a heavy-asterisk warning rendered prominently in datasets.md / list_datasets --long / the TUI for datasets that aggregate or reference content whose underlying licenses have not been individually cleared (public-web scrapes, Common Crawl derivatives, image/code corpora). Free-form per-source string — write a one-line summary of the gap and what a downstream user should do (e.g. "contact original authors before redistributing").
  },

  /* Stage 1 — fetch (raincloud/pipeline/fetch.py) */
  "fetch": {
    "type": "http",                     // "http" | "kaggle" | "uci" | "huggingface" | "custom" | "generated".
                                        // For "custom", `notes` names the fetcher: a CUSTOM_FETCHERS entry
                                        // in raincloud/_registry.py (the slug when notes is null).
    "urls": [                           // list; multiple URLs are fetched in order and concatenated/merged per `extract`. May be empty only for "custom" and "generated".
      "https://datasets.clickhouse.com/hits_compatible/hits.parquet"
    ],
    "auth": null,                       // null | "kaggle" | "huggingface"; kaggle and huggingface fetches require their own
    "requires_interactive_accept": false, // kaggle | huggingface only: marks datasets gated behind a one-time click-through on the provider's web UI before API access. The fetcher surfaces a clear "visit URL, accept, re-run" error on 403 regardless of this flag, but setting it lets the orchestrator announce the requirement up front.
    "hf_allow_patterns": null,          // huggingface-only: glob patterns forwarded to snapshot_download(allow_patterns=...). Use to fetch a subset of a giant repo (e.g. ["data/sample-10BT/*.parquet"] for fineweb).
    "hf_revision": null,                // huggingface-only: git revision (branch/tag/commit SHA) forwarded to snapshot_download(revision=...).
    "expected_bytes": 14779976446,      // optional; used only to warn on drift
    "expected_sha256": null,            // optional; prefer when upstream publishes it
  },

  /* Stage 2 — extract (raincloud/pipeline/extract.py) */
  "extract": {
    "type": "passthrough",              // "passthrough" | "zip" | "tar" (also .tar.gz/.tgz) | "bz2" | "gzip" | "7z"
    "include": ["hits.parquet"],        // glob list; applied after decompression
    "exclude": [],                      // optional; wins over include
    "post": null                        // optional custom post-extract step name
  },

  /* Stage 3 — parse (raincloud/pipeline/parse.py) */
  "parse": {
    "reader": "parquet",                // "csv" | "parquet" | "jsonl" | "xml" | "pbf" | "custom"
    "options": {                        // reader-specific
      /* for csv: { "delimiter": ",", "has_header": true, "encoding": "utf-8", "quoting": "minimal" } */
      /* for parquet: {} */
      /* for jsonl: { "record_path": null } */
    }
  },

  /* Stage 4 — transform (raincloud/pipeline/transform.py) */
  "transform": {
    "handler": "identity",              // a name declared in HANDLERS in raincloud/_registry.py; "identity" means no-op
    "params": {}                        // handler-specific kwargs
  },

  /* Parquet writer settings, read when a writer derives <slug>.parquet from the
     canonical Arrow spine (raincloud/pipeline/canonical.py). parquet@py
     (raincloud/pipeline/export/) honours all three fields. The sidecar writers
     (parquet@rs, parquet@java, parquet@hardwood) receive row_group_size_rows as
     RAINCLOUD_ROW_GROUP_MAX_ROWS in their environment, so the recipe's cap wins
     in every lane; they choose their own compression and statistics. v1
     manifests also carried `output` and `page_index`; no writer read either,
     and v2 rejects them. */
  "write": {
    "compression": "zstd",
    "row_group_size_rows": 10000000,    // a backstop row cap, not a target: groups are sized by encoded
                                        // bytes; set it low and it binds first (the manifest uses 10000000)
    "statistics": true
  },

  /* Stage 6 — validate (raincloud/pipeline/validate.py) */
  "expect": {
    "rows": 99997497,                   // exact; mismatch emits [WARN], does not fail unless --strict
    "schema_hash": null,                // optional; SHA-256 of canonicalised Arrow schema.
                                        // May be the full 64-char hex or a leading prefix
                                        // (manifest convention is 12 chars, matching the
                                        // schema_hash= line printed by the validate stage).
                                        // Mismatch emits [WARN] only; pass --strict to fail.
    "notes": null
  },

  /* Optional curatorial domain tags — closed vocab from
     raincloud/pipeline/discovery.py:TAG_VOCAB. At most 3 per spec; unique. */
  "tags": ["prose", "nested-json"],

  /* Optional editorial showcase tiers — closed vocab from
     raincloud/pipeline/discovery.py:SHOWCASE_TIERS. Multi-tier membership allowed. */
  "showcase": ["encoding"],

  /* Optional canonical references beyond license.source_url. kind ∈
     {paper, blog, homepage, github, dataset_card}. */
  "references": [
    {"kind": "paper",  "url": "https://arxiv.org/abs/2310.01377"},
    {"kind": "github", "url": "https://github.com/foo/bar"}
  ],

  /* Optional export policy (v2 only) — which formats to materialise from the
     canonical Arrow spine, and which writer makes them. Each format is one
     file, <format>/<slug>.<ext>; `arrow` is never listed (it's the spine). */
  "export": {
    "formats": ["parquet", "vortex"],  // formats; never "arrow", never writer-qualified
    "priority": {"parquet": ["rs", "py"]}, // writer preference: a list for every format, or per format
    "notes": null                      // free-form; never a writer's limitation (the build measures those)
  }
}
```

### `tags` *(array of string, optional, default `[]`)*

Closed-vocab tags drawn from the discovery module's `TAG_VOCAB`. At most 3 per spec; values must be unique. Used by the TUI's facet groups and `list_datasets --tag`. The vocab describes a dataset's **content shape** — what its columns hold — not its subject domain. Current vocab (`raincloud/pipeline/discovery.py` is authoritative; `list_datasets --tags-help` prints it):

- *String content* — `urls`, `prose`, `enums`, `identifiers`, `code-strings`
- *Numeric content* — `timestamps`, `embeddings`, `counts`, `monetary`, `measurements`
- *Payload / structure* — `coordinates`, `binary-payload`, `nested-json`

### `showcase` *(array of string, optional, default `[]`)*

Editorial showcase tiers from `SHOWCASE_TIERS`: `encoding`, `stress`. Multi-tier membership allowed. Drives the TUI view presets, the `--view` CLI flag, and the curated-picks block in `docs/v2/datasets.md`.

### `convert` *(object; schema_version 1 only)*

The v1 Vortex opt-in: `{"vortex": true, "vortex_skip_reason": null}` emits a sibling `<slug>.vortex` beside the parquet, and `"vortex": false` requires a non-null `vortex_skip_reason` saying why. A v2 spec declares its formats in `export.formats` instead; see below.

### `export` *(object, optional; v2 only)*

Says which formats to materialise from the canonical Arrow spine (`outputs/v{n}/<slug>/arrow/<slug>.arrow.zstd`), which is always produced under v2 and is *not* itself an export target, and which writer makes them. Each format is **one file**, `<format>/<slug>.<ext>`, whichever writer made it; the catalog records the writer (`parquet_writer`, `vortex_writer`) beside the file's sha256. The writer is provenance, not part of the file's address.

- `formats` *(array of `"parquet"` / `"vortex"`, optional)* — in v2 the **only** declaration of the formats a dataset *wants*; omitted, it is `["parquet", "vortex"]`. `[]` produces only canonical Arrow. A format the planned writer cannot produce for the dataset (it raises, dies, reports a failed round-trip, or exceeds `RAINCLOUD_EXPORT_TIMEOUT`) stays listed: the build records the failure (writer cell, error, toolchain versions, recipe, time) in the install's build record and carries on with the formats that worked, and regenerating the catalog carries that measurement into the snapshot as `<fmt>_unavailable`, where `raincloud describe` and the loader report it. A later build or export does not repeat it: while the measurement names the same writer cell, toolchain versions and canonical as the writer that would run, the format is skipped (`[skip]`) unless `--retry-errors` is passed. A later successful export replaces it, and `compliance` announces a `[stale opt-out]` when a writer round-trips a format the catalog records as unavailable. The schema and `validate_manifest` reject a `convert` block in a v2 manifest being authored. v2 catalogs released before that rule may still carry `convert.vortex: false`, and they keep reading: such a spec with no `export.formats` exports Parquet only.
- `priority` *(optional)* — this dataset's writer preference, either a list of writer names that serves every format (`["rs", "py"]`) or a map from format to such a list (`{"parquet": ["rs", "py"]}`) that serves only the formats it names. For each format, the first writer in the list that exists for it and is installed writes it, so a machine without a preferred sidecar falls through to the next writer. A list does not fall through to the next level, so it must name a writer for every format the dataset exports: `["hardwood"]` (a Parquet-only writer) on a dataset that also exports Vortex leaves Vortex with no writer, and its export fails. `validate_manifest` rejects a list that leaves an exported format with no writer; use a map to prefer a writer for one format only.
- `notes` *(string | null, optional)* — free-form annotation, such as why a deliberate policy leaves a format out. Never a writer's technical limitation: that is measured, never hand-written, so it cannot go stale. `list_datasets --no-vortex` and the TUI show it for a spec whose `formats` leaves out `vortex`.

**Writer precedence, per format** (stated here once): the spec's `export.priority`, then the catalog's top-level `export_priority` (same list-or-map shape), then the machine's `RAINCLOUD_EXPORT_PRIORITY` (a comma-separated list), then the built-in `py, rs, java, canonical`. A level that is a map without the format falls through to the next. `canonical` is the Arrow spine's writer; it has no Parquet or Vortex cell, so for exported formats the built-in order is effectively `py, rs, java`.

Unknown writer names are treated differently by audience: `validate_manifest` rejects a manifest name (spec or catalog) that is no export writer for its format, because in a manifest a typo would silently fall through to the next writer; at run time an unknown name is skipped, so a machine's `RAINCLOUD_EXPORT_PRIORITY` may name a writer this release does not ship.

Run `python -m raincloud.pipeline.compliance` to measure every writer against every reader; it writes each writer's output to scratch, never over the dataset's file.

## Handlers

`transform.handler` names a Python callable declared in `HANDLERS` in `raincloud/_registry.py`, the only registration (`raincloud/pipeline/handlers/__init__.py` just resolves those names, importing each module on demand). Each handler takes

```python
(spec: dict, parsed: list[(Path, pa.Table | BatchStream | None)], **params)
    -> list[(output_slug, pa.Table | BatchStream)]
```

`parsed` carries a `BatchStream` for a reader the handler declares with `batches.batch_input`, and `None` for inputs parse leaves to the handler. A single source can produce multiple outputs (GloVe → 3 datasets, OSM Germany → 3, Stack Exchange dump → 5).

The full list is `HANDLERS` in `raincloud/_registry.py` (and `docs/v2/handlers.md`); highlights:

- `identity` — passthrough.
- `tighten_types` — standard retype / list-element tightening / UUID/JSON annotation pass.
- `tlc_merge_months` — concatenate 12 TLC monthly parquets into an annual file.
- `public_bi_merge` — concatenate `.csv.bz2` partitions using the companion `.sql` schema.
- `glove_split` — read GloVe `.txt`, split into 3 per-dimension `fixed_size_list<float, N>` parquets.
- `osm_pbf_split` — read `.osm.pbf` and emit 3 GeoParquet files (nodes/ways/relations) with WKB geometry.
- `stack_exchange_split` — read Stack Exchange XML dump and emit one parquet per table.
- `openlibrary_parse` — read `ol_dump_*.txt.gz` and split by record type.
- `uci_default` — UCI `data.csv` with standard type-tightening + column-name normalisation.
- `factbook_variant_parse` / `jsonbench_variant_parse` — DuckDB `CAST(CAST(... AS JSON) AS VARIANT)` + `variant_to_parquet_variant` shredding into the canonical Arrow spine's `struct<metadata, value, ...>` VARIANT column (factbook materializes a small table; jsonbench streams, memory-bounded).
- `lichess_pgn_parse` — stream a Lichess `.pgn.zst` monthly dump.

## Stage contracts

1. **fetch** reads `fetch.*` and writes bytes under the configured raw root (`RAINCLOUD_RAW_DOWNLOADS`; `raincloud config show` prints it), in `<slug>/`, or in a `<slug>/.recipes/<fetch-key>/` generation when the fetch recipe differs from the one those bytes came from (`raincloud.pipeline.spec.raw_slug_dir`). Idempotent (skip if `expected_bytes`/`expected_sha256` already matches). Raw downloads are *not* version-scoped — the same upstream bytes are reused across schema_versions.
2. **extract** reads `extract.*` and expands downloaded files into the configured scratch root (`RAINCLOUD_WORKDIR`), under `.recipes/<recipe-hash>/<slug>/` (`raincloud.pipeline.spec.recipe_workdir_root`). Outputs a list of `(relative_path, type)` tuples.
3. **parse** reads `parse.*` and each extracted file, produces one `pyarrow.Table` per source file. Reader options mirror the underlying library.
4. **transform** dispatches to `handler` with the parsed tables as input. Output is `(output_slug, arrow_table)` tuples. Streaming handlers write the canonical Arrow spine directly (via `open_canonical_writer`) and return `[]`.
5. **write_canonical** ([`raincloud/pipeline/canonical.py`](raincloud/pipeline/canonical.py)) writes the canonical Arrow IPC spine — zstd-compressed — to `outputs/v{schema_version}/<output_slug>/arrow/<output_slug>.arrow.zstd`. This is the source of truth every format derives from; the former `write.py` parquet stage was removed. (For a streaming handler the spine was already written in transform.)
6. **validate** reads the canonical spine and compares row count + schema hash to `expect.*`. Warnings by default; `--strict` promotes mismatches to errors.
7. **export** ([`raincloud/pipeline/export/`](raincloud/pipeline/export/)) re-encodes canonical Arrow through the configured exporter cells: the formats in `export.formats` (default Parquet and Vortex), each written once, to `parquet/` or `vortex/`, by the first installed writer in that format's priority (see [`export`](#export-object-optional-v2-only) for the precedence). Use `python -m raincloud.pipeline.compliance` to measure the writer/reader matrix.

## Multi-output datasets

When `transform.handler` returns multiple tables, each handler-emitted `output_slug` becomes a dataset of its own, `<format>/<output_slug>.<ext>`. Example: a single source (`glove.6B.zip`) produces 3 parquets (`glove-6b-50d.parquet`, etc.).

Client-side the 3 outputs appear as 3 distinct `DatasetSpec` entries in `sources.json`, each referencing the same `fetch` config but with `transform.handler = "glove_split"` and a `params.dimension` discriminator. The pipeline dedupes the actual download.

## Generated acquisition (`fetch.type = "generated"`)

```json
{
  "type": "generated",
  "generator": "duckdb-tpch",
  "version": "1.5.5",
  "parameters": {"sf": 1},
  "output": "lineitem",
  "urls": [],
  "auth": null,
  "expected_bytes": null,
  "expected_sha256": null
}
```

`generator`, `version`, and `parameters` identify a complete invocation; `output`
selects one named file from it. All four fields participate in the dataset's recipe
hash. The shared input-cache key excludes output selection and catalog
labels, allowing entries to reuse co-generated files. A group receipt records
individual sizes and SHA-256 values; ordinary prepared-artifact checksums remain
in the catalog snapshot. The existing single-URL expected fields are null here.

The registered adapters are `duckdb-tpch` (version is the DuckDB version) and
`tpcgen-rs-tpch` (version is the `tpchgen-cli` version). Both accept exactly one
parameter, a finite positive `sf`, and produce `region`, `nation`, `supplier`,
`customer`, `part`, `partsupp`, `orders`, and `lineitem`. They acquire Parquet
inputs: use passthrough extraction, the Parquet parser and identity transform
to retain each generator's types. Generator capabilities are checked by the
builder; reading prepared outputs needs no generator capability.

### TPC-DS adapters

`duckdb-tpcds` accepts `{ "sf": 1 }` and pins the DuckDB/core extension version,
currently `1.5.5`. `tpcgen-rs-tpcds` accepts `{ "sf": 1, "compat": "c" }` or
`{ "sf": 1, "compat": "trino" }`; `compat` is required so upstream default
changes cannot silently alter a recipe. These initial adapters accept finite
scale factors of at least 1.

The unified Rust CLI is pinned as
`0.1.0+git.3fc6faaa7dd28e4330b24d4240efe2852e79d00d`. Source setup records the
commit and installed executable checksum; acquisition verifies that receipt.
Both generators select the 24 TPC-DS data tables. The Rust CLI additionally
writes a one-row `dbgen_version` (generator version, run date and time, and the
full command line); it is not a selected output, because it describes the
invocation rather than the data -- two logically identical builds differ there,
and it tells a consumer nothing about the dataset they are using. Generator
identity is already recorded in the fetch recipe. Output names are those returned by
`python -m raincloud.pipeline.list_datasets --fetch-type generated --json`.

## Derived datasets (`<parent>-hydrated`)

A hydrated dataset derives from another entry instead of an upstream, so it carries
`derive` and `advisory` and none of fetch/extract/parse/transform/write:

```jsonc
{
  "slug": "laion-400m-hydrated",           // must be <derive.from>-hydrated
  "short_name": "...", "full_name": "...", "description": "...", "license": { ... },
  "derive": {
    "from": "laion-400m",                  // an ordinary (non-derived) dataset
    "hydrate": {
      "columns": {                         // parent URL column -> new column
        "url": { "into": "content", "type": "binary" }   // "binary" | "string"
      },
      "blocked_hosts_extra": ["..."]       // optional: always-blocked hosts
    }
  },
  "advisory": "Many LAION URLs return 404 (10-30% takedown rate); ..."  // shown by `raincloud describe`, which every load's warning points to
}
```

Its recipe folds in the parent's, so rebuilding the parent makes it stale. See
[`HYDRATING.md`](HYDRATING.md).
