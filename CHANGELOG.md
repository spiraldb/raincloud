# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.3]: https://github.com/spiraldb/raincloud/releases/tag/v0.1.3
[0.1.2]: https://github.com/spiraldb/raincloud/releases/tag/v0.1.2
[0.1.1]: https://github.com/spiraldb/raincloud/releases/tag/v0.1.1
[0.1.0]: https://github.com/spiraldb/raincloud/releases/tag/v0.1.0
