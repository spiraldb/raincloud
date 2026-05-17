# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.5] - 2026-05-17

### Fixed

- **TUI Columns modal crash on slugs with duplicate column names.**
  Eleven slugs ship parquet schemas with legitimately repeated
  top-level column names — the `osmi-mental-health-in-tech-*` survey
  series (2016 through 2023) repeats "Why or why not?" follow-ups
  under each yes/no item, and `uci-spambase`, `uci-parkinsons`, and
  `uk-price-paid` each have one or more repeated headers. The new
  Columns modal used the bare column name as the Textual DataTable
  row key, so the second occurrence crashed with `DuplicateKey`.
  Repeated names are now suffixed with ` (2)`, ` (3)`, etc. for
  display + lookup; the by-name stats dict no longer silently
  collapses entries either. The underlying parquet's column names
  are unchanged.

## [0.1.4] - 2026-05-17

### Added

- **Catalog discoverability** — new ways to navigate the 249-spec
  catalog without scrolling the full `docs/v1/datasets.md`.
- **TUI faceted side panel** (`browse.py`) — filter groups for showcase,
  domain tags, size, shape traits, license, fetch type. View-preset bar
  (`encoding`, `stress`) on top, selectable from the `View` row. Counts
  header shows `N of 249`.
- **TUI search** — `/` focuses a search input above the table. Bare
  tokens match any field (substring, case-insensitive); qualified
  clauses (`slug:foo desc:bar tag:enums col:lat lic:cc0 handler:…
  reader:… fetch:…`) scope to one field. Clauses AND together and AND
  with the facet selection. Aliases: `name` / `desc[ription]` /
  `tag[s]` / `col[umn][s]` / `lic[ense]`.
- **TUI Columns-modal rendering refresh** — pessimistic per-codepoint
  cell-width accounting fixes Sinhala / CJK / Arabic content overflow.
  Block-glyph histograms scale with pane width; new x-axis tick labels
  (`lo / mid / hi`) under each numeric histogram and horizontal bars
  for top-K string distributions. Modal widened (90% → 95%); the
  unreliable yellow border replaced by `$surface` background contrast.
- **Per-column profiles** — new opt-in stage
  `python -m scripts.pipeline.profile [<slug>]` produces
  `outputs/v1/<slug>/profile.json` with per-dtype stats (numeric
  histograms, string NDV + top-K, bool T/F/null, date/timestamp ranges,
  list/map length stats). Surfaced in the TUI's detail pane and via
  `list_datasets --inspect <slug>`. Auto-promotes the result into
  `docs/v1/profiles/<slug>.json` so fresh clones can render sparklines
  without rebuilding; `--no-promote` opts out.
- **`promote_profiles` tooling** — new
  `python -m scripts.pipeline.promote_profiles` mirrors built
  profiles into the tracked `docs/v1/profiles/` directory. Idempotent
  (byte-identical destinations are skipped); `--check` for CI audits.
- **List-element dtypes in profiles.** `profile.py` now renders list,
  large_list, and fixed_size_list element types recursively in the
  dtype label (`list<float>`, `fixed_size_list<float>[100]`,
  `list<struct>`). Downstream consumers (e.g. `autotag`) can
  distinguish embedding-shaped columns from lists-of-structs without
  re-opening the parquet.
- **Editorial metadata** in `sources.json` — optional `tags` (closed
  vocab, 13 data-kind entries grouped by content axis:
  string — urls / prose / enums / identifiers / code-strings;
  numeric — timestamps / embeddings / counts / monetary / measurements;
  payload — coordinates / binary-payload / nested-json) and
  `showcase` (closed vocab, 2 tiers: encoding / stress) per
  `DatasetSpec`. `scripts.pipeline.autotag` proposes tags from each
  slug's profile + handler/slug-name fallbacks; hand-edit in
  `sources.json` after that like any other manifest field.
- **Public BI workload descriptions.** All 46 `bi-*` slugs in the
  Public BI Benchmark now carry per-workload descriptions grounded in
  actual column names rather than the workbook label, with a
  data-shape lead (`N rows × M cols`, dtype-family mix, notable
  columns) and a `Background:` note. Many workbook names mislead about
  contents — e.g. `bi-romance` is Instagram social posts;
  `bi-physicians` is CMS Medicare payment records; `bi-iglocations1`
  is US Census geographic codes; `bi-eixo` and `bi-uberlandia` share a
  schema with `bi-mulheresmil` (a Brazilian education program). Two
  slugs (`bi-arade`, `bi-wins`) retain a generic description because
  their columns are anonymised beyond recognition.
- **Derived signals** in `docs/snapshot.json` — per-slug `shape_traits`
  (has_nested, has_timestamp, has_variant, string_heavy, wide_row,
  high_cardinality_present) and `size_bucket` (xs/s/m/l/xl), derived by
  `docs.py` from on-disk parquets.
- **CLI parity** — `list_datasets` gains `--tag`, `--showcase`, `--size`,
  `--trait` (with `!` negation), `--view`, `--inspect`, `--tags-help`,
  `--showcase-help`. `--inspect` falls back from the built-parquet
  profile to the tracked `docs/v1/profiles/<slug>.json` mirror, so a
  fresh clone can inspect any slug in the catalog without rebuilding.
- **Curated-picks header** in `docs/v1/datasets.md` — one block per
  showcase tier, regenerated from `sources.json`.
- **README "Discover" subsection** — directs newcomers at the TUI first.
- **Skills**: new `raincloud-profile`, new `raincloud-discover`; updated
  `raincloud-list-datasets`, `raincloud-build`.
- **Tracked profiles for all 249 specs.** `docs/v1/profiles/` ships a
  per-slug profile for every entry in the manifest, including the
  multi-hour heavyweights (`clickbench-hits`, `fineweb-sample-10bt`,
  `wikipedia-structured-contents`, `jsonbench-bluesky-100m`,
  `osm-germany-nodes`, the OpenLibrary dumps, etc.). A fresh clone can
  render the TUI Columns pane and use `list_datasets --inspect <slug>`
  on any slug without building anything locally.

### Changed

- **`autotag` enums classifier tightened.** A string column counts as
  enum-shaped only when `ndv ≤ 32 AND mean_len ≤ 24`, or when
  `ndv ≤ 256 AND ndv/rows ≤ 0.001 AND mean_len ≤ 24` for very wide
  datasets. The slug-level `enums` tag additionally requires ≥2
  qualifying columns, so a single class-label column no longer
  promotes the whole dataset to enum-shaped.
- **`autotag` embeddings detection** now reads the list-element dtype
  written by `profile.py` and recognises `list<float>` / `list<double>`
  / `fixed_size_list<float>` columns as embeddings without relying on
  slug-name heuristics. The remaining slug-name fallback uses
  word-boundary matching (`\b(embeddings?|word vectors?|dense vector|
  glove|word2vec|fasttext|encoder output)\b`) so unrelated copy like
  "sensors embedded in …" no longer matches.

### Removed

- **`DatasetSpec.family` field and `--family` CLI flag.** The field was
  used to invoke batched builds (`python -m scripts.pipeline.build
  --family uci`); each slug is now invoked by name, and `--all` remains
  available for whole-catalog passes. Pass multiple slugs space-separated
  to `build` / `convert` for ad-hoc batches.
- **Subject-matter `TAG_VOCAB`** (12 entries: geospatial / nlp-text /
  web-analytics / e-commerce / finance / social / scientific /
  healthcare / sports / transportation / government / benchmark)
  replaced by the 13 data-kind vocab above.
- **`curation.json` + `scripts/pipeline/curate.py` + `tests/test_curate.py`**
  removed. Tags now sit inline in `sources.json` alongside
  `description` / `license` / `showcase`. The `curate apply` bridge is
  gone.

### Fixed

- **`profile.py` DECIMAL overflow in histogram-bucket SQL.** DuckDB was
  inferring DECIMAL types from inlined `lo_f` / `hi_f` Python repr
  (e.g. `0.26851799179226266` → DECIMAL(18,17)); `(value - lo) * 10`
  then overflowed. All histogram-bucket literals are now `::DOUBLE`-cast.
- **`profile.py` zero-length identifier on empty column names.** Some
  upstream CSVs ship an unnamed pandas-index column whose Arrow field
  has `name == ""`; DuckDB rejects empty delimited identifiers. Skip
  with a placeholder `__unnamed_column__` entry.
- **`profile.py` TIME-of-day column cast.** DuckDB doesn't implement
  `CAST(time AS TIMESTAMP)`; standalone TIME columns now route through
  the string profile (null_count + NDV + top-K of rendered HH:MM:SS).
- **`profile.py` `fixed_size_list` columns** were silently profiled as
  `null` because the dispatcher only checked `is_list` / `is_large_list`.
  They now route through the list profile and pick up the new
  element-type rendering, so e.g. `glove-6b-100d`'s
  `vector: fixed_size_list<float>[100]` is fully described.
- **WDI re-enabled.** The upstream redirect target
  `databankfiles.worldbank.org` serves an expired TLS cert, so Python's
  default `urllib` refused the connection. The new `fetch.verify_tls`
  field (boolean, default `true`) lets a slug bypass verification when
  its `expected_sha256` provides independent integrity. WDI ships at
  395,276 rows × 70 columns (70 MB parquet).

### Schema

- `sources.schema.json` adds three optional fields, all additive
  (existing manifests are accepted unchanged):
  - `DatasetSpec.tags` (array of TAG_VOCAB strings, default `[]`).
  - `DatasetSpec.showcase` (array of SHOWCASE_TIERS strings, default `[]`).
  - `DatasetSpec.fetch.verify_tls` (boolean, default `true`) — escape
    hatch for upstreams whose TLS certs have rotted but whose payload
    integrity is gated by `expected_sha256`.
- New `profile.schema.json` (Draft 2020-12) for the per-slug profile
  output format.

## [0.1.3] - 2026-05-10

### Changed

- **Validate stage no longer hard-fails on row/schema_hash drift by
  default.** A mismatch now emits a `[WARN]` line to stderr and the build
  continues. Users invoking `python -m scripts.pipeline.build <slug>` have
  already opted into "fetch whatever is upstream now"; an upstream Arrow-
  conversion bump or a slightly-grown row count shouldn't turn that into a
  failed build. Pass `--strict` (new flag on `scripts.pipeline.build`) to
  upgrade warnings to errors — recommended for CI / pre-release gates.
- The previous `--loose` flag has been removed; its behaviour (warn, don't
  raise) is now the default. Migrate `--loose` invocations to dropping the
  flag entirely; replace any "default-strict" CI invocations with
  `--strict`.

### Fixed

- **`validate.py` now compares `expect.schema_hash` as a prefix when the
  manifest value is shorter than the full 64-char SHA-256.** All 37 slugs
  with `schema_hash` set in `sources.json` use a 12-char short hash
  (matching the `[validate] schema_hash=` print convention, akin to git
  short SHAs); the previous full-string equality made every one of them
  fail validation on rebuild. Equal-length values still use strict
  equality, so full hashes remain enforceable for callers that prefer
  them.
- `sources.schema.md` updated to document the prefix-match rule and the
  new warn-vs-`--strict` semantics for the `expect` block.

## [0.1.2] - 2026-05-10

### Fixed

- All `uv sync` instructions across the docs (README, AGENTS, CONTRIBUTING,
  SKILLS, in-code install hints, and skill files) now pass `--inexact` so
  installing one extra no longer uninstalls the others. Without this, the
  documented sequential setup (`uv sync --extra tui` → bare `uv sync` →
  `uv sync --extra huggingface`) silently left the user with only the last
  extra installed, and subsequent builds of HF/Kaggle slugs failed with
  `ImportError`. uv has no project-level toggle for this — `--inexact` is
  per-command — so the fix is documentation-wide.

### Changed

- TUI build action (`python -m scripts.pipeline.browse`, then `b` on a row)
  now runs `uv sync --extra <kaggle|huggingface> --inexact` automatically
  before the build subprocess when the dataset's `fetch.type` requires an
  upstream-fetch backend. Sync output streams into the same RichLog as the
  build; sync failure aborts the build with a visible exit code. Pure-HTTP
  and custom-fetch slugs see the same flow as before (no extra sync).
  `BuildConfirmModal` surfaces the sync command line above the build command
  line so the user sees both before confirming.

## [0.1.1] - 2026-05-07

### Added

- README badges (CI status, latest release, license, citation).

### Changed

- **Convert stage now streams parquet batches** via `pf.iter_batches() →
  RecordBatchReader → vxio.write` instead of materialising whole tables.
  Resolves `ArrowNotImplementedError: Nested data conversions not implemented
  for chunked array outputs` from pyarrow on slugs whose nested columns
  (`list<struct>`, `struct<bytes,…>`) would need to be chunked across multiple
  Arrow arrays. Re-enables Vortex output for `osm-germany-ways`,
  `ultrachat-200k`, `mmmu`, `websight-v01`, `peoples-speech-clean-validation`.
- `code-contests` Vortex skip re-diagnosed: not the chunked-array path; a
  separate upstream FSST i32-offset overflow on `list<string>` >2 GB.
- `open-food-facts` description aligned with shipped output (currently a
  single `raw_json: string` column via `jsonl_as_string_parse`; VARIANT
  promotion deferred).
- PR template: dropped the "Test plan" checklist (CI runs the same gates on
  every PR; CONTRIBUTING.md documents them once).
- Agent-tooling docs (AGENTS.md, SKILLS.md, `raincloud-docs` skill) now flag
  `docs/snapshot.json` as load-bearing — TUI fallback _and_ the
  row-count / file-size fallback for `datasets.md` regen. Stale "six derived
  docs" reference in AGENTS.md cleaned up to three.

### Fixed

- `docs/datasets.md` regeneration now falls back to `docs/snapshot.json`
  (top-level scratch, then `docs/v{schema_version}/snapshot.json` on a fresh
  clone) for slugs whose parquet isn't built locally. Previously,
  partial-build regen would silently dash-out row counts and file sizes for
  any slug not on disk, destroying ground truth in the v1 snapshot. Snapshot
  regen now also captures `last_built_row_groups`. Five regression tests
  added in `tests/test_docs.py`.

## [0.1.0] - 2026-05-06

Initial public release.

Raincloud is a client-reproducible pipeline for building a curated catalog
of public datasets as analytics-ready Parquet + Vortex files. See
[`README.md`](README.md) for the user-facing overview,
[`AGENTS.md`](AGENTS.md) for the architecture, and
[`SKILLS.md`](SKILLS.md) for procedural playbooks.

This release bundles:

- The 7-stage build pipeline (fetch → extract → parse → transform → write
  → validate → convert) plus the optional opt-in hydrate stage.
- 249 dataset specs across 5 families (`direct`, `kaggle-upstream`,
  `nyc-tlc`, `public-bi`, `uci`).
- 24 named transform handlers covering CSV / Parquet / JSONL / XML / PBF /
  custom-format upstreams plus streaming variants for memory-constrained
  shapes.
- A read-only Textual TUI for browsing the catalog
  (`python -m scripts.pipeline.browse`, requires `--extra tui`).
- Per-dataset Vortex conversion via the `convert.vortex` flag.
- Apache License 2.0, with SPDX file headers on all Python sources.
- Governance: `SECURITY.md`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`
  (Contributor Covenant 2.1), `DISCLAIMER.md` (AS IS posture, content
  and license disclaimers, dataset-removal reporting), and
  `HYDRATING.md` (policy for the optional hydrate stage).
- Tooling: `ruff` lint (rules `E`, `F`, `W`, `I`) + GitHub Actions CI
  (`.github/workflows/ci.yml`) running lint, manifest validation, and
  `pytest` on every push and PR to `develop`.
- Dataset-removal issue template
  (`.github/ISSUE_TEMPLATE/dataset-removal.yml`) — structured form for
  the channel `DISCLAIMER.md` points readers at.
- Pull-request template (`.github/pull_request_template.md`) prompting
  for summary, test-plan checkbox list against the standard pre-PR gate,
  and change-type tags.
- `CITATION.cff` — GitHub-native citation metadata; surfaces the "Cite
  this repository" button in the repo sidebar with BibTeX / APA / Chicago
  exports.

[0.1.4]: https://github.com/spiraldb/raincloud/releases/tag/v0.1.4
[0.1.3]: https://github.com/spiraldb/raincloud/releases/tag/v0.1.3
[0.1.2]: https://github.com/spiraldb/raincloud/releases/tag/v0.1.2
[0.1.1]: https://github.com/spiraldb/raincloud/releases/tag/v0.1.1
[0.1.0]: https://github.com/spiraldb/raincloud/releases/tag/v0.1.0
