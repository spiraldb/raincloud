# `sources.json` schema (v1)

> **Machine-readable companion:** [`sources.schema.json`](sources.schema.json) is the JSON Schema (Draft 2020-12) version of this document. Run `python -m scripts.pipeline.validate_manifest` to validate `sources.json` against it plus cross-checks the schema can't express (handler registry, slug uniqueness, etc.). Keep both files in lockstep when adding fields.

This document defines the shape of `sources.json`, the manifest that drives the Raincloud pipeline. The file declares, per dataset, **how to fetch**, **how to extract**, **how to parse**, **how to transform**, and **how to validate** the resulting parquet in `outputs/v{schema_version}/<slug>/parquet/<slug>.parquet`. Stages are processed by separate scripts under `scripts/pipeline/`; each stage reads only the fields it needs so scripts can be developed, tested, and re-run independently.

## Top-level shape

```jsonc
{
  "schema_version": 1,
  "datasets": [ /* DatasetSpec, one per dataset */ ]
}
```

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

  /* Stage 1 — fetch (scripts/pipeline/fetch.py) */
  "fetch": {
    "type": "http",                     // "http" | "kaggle" | "uci" | "huggingface" | "custom"
    "urls": [                           // list; multiple URLs are fetched in order and concatenated/merged per `extract`
      "https://datasets.clickhouse.com/hits_compatible/hits.parquet"
    ],
    "auth": null,                       // null | "kaggle" | "huggingface" | "<custom-token-key>"
    "requires_interactive_accept": false, // kaggle-only: marks datasets that require a one-time ToS click-through on the Kaggle web UI before API access. fetch_kaggle surfaces a clear "visit URL, click Download, re-run" error on 403 regardless of this flag, but setting it lets the orchestrator announce the requirement up front.
    "hf_allow_patterns": null,          // huggingface-only: glob patterns forwarded to snapshot_download(allow_patterns=...). Use to fetch a subset of a giant repo (e.g. ["data/sample-10BT/*.parquet"] for fineweb).
    "hf_revision": null,                // huggingface-only: git revision (branch/tag/commit SHA) forwarded to snapshot_download(revision=...).
    "expected_bytes": 14779976446,      // optional; used only to warn on drift
    "expected_sha256": null,            // optional; prefer when upstream publishes it
    "verify_tls": true                  // optional; default true. Set to false only as a documented escape hatch for upstreams whose certs have rotted but whose payload integrity we cover via expected_sha256.
  },

  /* Stage 2 — extract (scripts/extract.py) */
  "extract": {
    "type": "passthrough",              // "passthrough" | "zip" | "tar" | "bz2" | "gzip" | "7z" | "custom"
    "include": ["hits.parquet"],        // glob list; applied after decompression
    "exclude": [],                      // optional; wins over include
    "post": null                        // optional custom post-extract step name
  },

  /* Stage 3 — parse (scripts/pipeline/parse.py) */
  "parse": {
    "reader": "parquet",                // "csv" | "parquet" | "jsonl" | "xml" | "pbf" | "custom"
    "options": {                        // reader-specific
      /* for csv: { "delimiter": ",", "has_header": true, "encoding": "utf-8", "quoting": "minimal" } */
      /* for parquet: {} */
      /* for jsonl: { "record_path": null } */
    }
  },

  /* Stage 4 — transform (scripts/pipeline/transform.py) */
  "transform": {
    "handler": "identity",              // named Python callable in scripts/pipeline/handlers/; "identity" means no-op
    "params": {}                        // handler-specific kwargs
  },

  /* Stage 5 — write (scripts/pipeline/write.py) */
  "write": {
    "output": "clickbench-hits.parquet",
    "compression": "zstd",
    "row_group_size_rows": 1048576,
    "statistics": true,
    "page_index": false
  },

  /* Stage 6 — validate (scripts/pipeline/validate.py) */
  "expect": {
    "rows": 99997497,                   // exact; mismatch emits [WARN], does not fail unless --strict
    "schema_hash": null,                // optional; SHA-256 of canonicalised Arrow schema.
                                        // May be the full 64-char hex or a leading prefix
                                        // (manifest convention is 12 chars, matching the
                                        // schema_hash= line printed by the validate stage).
                                        // Mismatch emits [WARN] only; pass --strict to fail.
    "notes": null
  },

  /* Stage 7 — convert (scripts/pipeline/convert.py, optional) */
  "convert": {
    "vortex": true,                     // opt-in; when true, emit a sibling <slug>.vortex alongside the parquet.
    "vortex_skip_reason": null          // null when vortex=true. When vortex=false, holds a non-null free-form string explaining why (typically a known type-support gap in the current Vortex release). The pair is rendered into docs/v{n}/vortex_skip.md so the catalog tracks *why* slugs lack a `.vortex` rather than leaving it ambiguous.
  },

  /* Hydration mark (optional, opt-in) — for slugs with a URL column whose
     contents could be dereferenced into a sibling parquet under the
     `parquet-hydrated/` format dir. Today this is a manifest-level mark only;
     the hydrate stage isn't implemented yet. Surface in TUI / list_datasets
     --hydrate / --json. */
  "hydrate": {
    "url_column": "url",                 // existing string column to dereference
    "output_column": "content",          // new column on the hydrated copy
    "output_type": "binary",             // "binary" (image/pdf bytes) | "string" (HTML/text)
    "advisory": "Many LAION URLs return 404 (10-30% takedown rate); ..."  // per-slug pitfalls
  },

  /* Optional curatorial domain tags — closed vocab from
     scripts/pipeline/discovery.py:TAG_VOCAB. At most 3 per spec; unique. */
  "tags": ["nlp-text", "benchmark"],

  /* Optional editorial showcase tiers — closed vocab from
     scripts/pipeline/discovery.py:SHOWCASE_TIERS. Multi-tier membership allowed. */
  "showcase": ["encoding"],

  /* Optional canonical references beyond license.source_url. kind ∈
     {paper, blog, homepage, github, dataset_card}. */
  "references": [
    {"kind": "paper",  "url": "https://arxiv.org/abs/2310.01377"},
    {"kind": "github", "url": "https://github.com/foo/bar"}
  ]
}
```

### `tags` *(array of string, optional, default `[]`)*

Closed-vocab domain tags drawn from the discovery module's `TAG_VOCAB`. At most 3 per spec; values must be unique. Used by the TUI's "Domain" facet group and `list_datasets --tag`. Current vocab:

`geospatial`, `nlp-text`, `web-analytics`, `e-commerce`, `finance`, `social`, `scientific`, `healthcare`, `sports`, `transportation`, `government`, `benchmark`.

### `showcase` *(array of string, optional, default `[]`)*

Editorial showcase tiers from `SHOWCASE_TIERS`: `encoding`, `stress`. Multi-tier membership allowed. Drives the TUI view presets, the `--view` CLI flag, and the curated-picks block in `docs/v1/datasets.md`.

## Handlers

`transform.handler` names a Python callable registered in `scripts/pipeline/handlers/__init__.py`. Each handler takes

```python
(spec: dict, parsed: list[(Path, pa.Table | None)], **params) -> list[(output_slug, pa.Table)]
```

so a single source can produce multiple parquets (GloVe → 3 files, OSM Germany → 3 files, Stack Exchange dump → 5 files).

The full registry lives in `scripts/pipeline/handlers/__init__.py`; highlights:

- `identity` — passthrough.
- `tighten_types` — standard retype / list-element tightening / UUID/JSON annotation pass.
- `tlc_merge_months` — concatenate 12 TLC monthly parquets into an annual file.
- `public_bi_merge` — concatenate `.csv.bz2` partitions using the companion `.sql` schema.
- `glove_split` — read GloVe `.txt`, split into 3 per-dimension `fixed_size_list<float, N>` parquets.
- `osm_pbf_split` — read `.osm.pbf` and emit 3 GeoParquet files (nodes/ways/relations) with WKB geometry.
- `stack_exchange_split` — read Stack Exchange XML dump and emit one parquet per table.
- `openlibrary_parse` — read `ol_dump_*.txt.gz` and split by record type.
- `uci_default` — UCI `data.csv` with standard type-tightening + column-name normalisation.
- `factbook_variant_parse` / `jsonbench_variant_parse` — stream JSON-per-row into a Parquet `VARIANT` column via DuckDB's `CAST(... AS VARIANT)` + `COPY TO PARQUET`.
- `lichess_pgn_parse` — stream a Lichess `.pgn.zst` monthly dump.

## Stage contracts

1. **fetch** reads `fetch.*` and writes bytes to `outputs/raw_downloads/<slug>/<basename>`. Idempotent (skip if `expected_bytes`/`expected_sha256` already matches). Raw downloads are *not* version-scoped — the same upstream bytes are reused across schema_versions.
2. **extract** reads `extract.*` and expands downloaded files into `_workdir/<slug>/`. Outputs a list of `(relative_path, type)` tuples.
3. **parse** reads `parse.*` and each extracted file, produces one `pyarrow.Table` per source file. Reader options mirror the underlying library.
4. **transform** dispatches to `handler` with the parsed tables as input. Output is `(output_slug, arrow_table)` tuples. Streaming handlers may write parquet directly and return `[]`.
5. **write** writes parquet to `outputs/v{schema_version}/<output_slug>/<output_slug>.parquet` per `write.*` settings.
6. **validate** reads written parquet and compares row count + schema hash to `expect.*`. Errors are loud by default; `--loose` downgrades to warnings.
7. **convert** (optional) — when `convert.vortex = true`, emit `outputs/v{n}/<slug>/vortex/<slug>.vortex` (a sibling format directory next to `parquet/`). No-op otherwise. See [`scripts/pipeline/convert.py`](scripts/pipeline/convert.py) for type-support caveats; current known gaps in vortex 0.69 are listed in `SKILLS.md`.

## Multi-output datasets

When `transform.handler` returns multiple tables, the `write.output` field is ignored and each handler-emitted `output_slug` becomes the output filename. Example: a single source (`glove.6B.zip`) produces 3 parquets (`glove-6b-50d.parquet`, etc.).

Client-side the 3 outputs appear as 3 distinct `DatasetSpec` entries in `sources.json`, each referencing the same `fetch` config but with `transform.handler = "glove_split"` and a `params.dimension` discriminator. The pipeline dedupes the actual download.
