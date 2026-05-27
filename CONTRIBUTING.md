# Contributing to Raincloud

Thanks for your interest in Raincloud. This guide covers how to set up a dev
environment, run the test suite, and submit changes. For deeper dives into the
pipeline itself, see [`README.md`](README.md), [`AGENTS.md`](AGENTS.md), and
[`SKILLS.md`](SKILLS.md).

## Setting up

```bash
git clone git@github.com:spiraldb/raincloud.git
cd raincloud
uv sync --extra dev --inexact
```

`--extra dev` pulls in `pytest`. The base install is the lightweight loader
(`pyarrow`, `numpy`, `vortex-data`, `fsspec`); the heavy build toolchain
(duckdb, pandas, osmium, pyreadstat, openpyxl, …) now lives behind the `build`
extra, so **add `--extra build` when working on the pipeline or handlers** —
otherwise the build stages fail on a missing import. Add `--extra kaggle` or
`--extra huggingface` if your work touches those upstream types, or `--extra
all` for everything. Always pass `--inexact` — without it, each `uv sync
--extra X` removes the extras from the previous one (e.g. syncing `--extra dev`
after `--extra huggingface` uninstalls `huggingface_hub`).

## Before you open a PR

Three sub-second checks are the minimum gate (CI runs all three):

```bash
ruff check                                     # lint (pyflakes + pycodestyle + isort)
python -m scripts.pipeline.validate_manifest   # JSON Schema + cross-checks on sources.json
pytest                                         # smoke regression net (manifest, schema, registry, examples, loader)
```

`pytest` now also covers the `raincloud` loader package (catalog resolution,
cache/mirror/build dispatch, sha256 integrity) alongside the manifest checks.

If you touched the build pipeline, install the build extra and run a small
end-to-end build to make sure it still produces the expected output:

```bash
uv sync --extra build --inexact
python -m scripts.pipeline.build countries-of-the-world   # ~200 ms, 227 rows
```

For larger builds, see [`SKILLS.md`](SKILLS.md#running-a-large-build-safely).

## What to send a PR for

- **New datasets** — see [`SKILLS.md`](SKILLS.md#adding-a-new-dataset). Most
  entries copy [`examples/minimal_spec.json`](examples/minimal_spec.json) and
  pick an existing handler from [`docs/v1/handlers.md`](docs/v1/handlers.md).
- **New transform handlers** — see
  [`SKILLS.md`](SKILLS.md#adding-a-new-transform-handler). One handler per
  upstream shape; register in `scripts/pipeline/handlers/__init__.py`.
- **Bug fixes** — start with a failing test where practical.
- **Documentation** — README/AGENTS/SKILLS edits welcome. The two derived docs
  (`docs/datasets.md`, `docs/handlers.md`) are machine-generated; don't
  hand-edit them — fix the manifest or the registry and regenerate via
  `python -m scripts.pipeline.docs`.

## Tests for new functionality

Add a test alongside any new behaviour:

- **New transform handler** — a fixture-based test demonstrating the
  expected output shape (small in-memory `pa.Table`; see existing handler
  tests in `tests/test_manifest.py` for the pattern).
- **New manifest field or schema rule** — extend `test_manifest.py` to
  assert it validates as expected.
- **New CLI flag** — extend the relevant `test_*.py` (e.g.
  `test_list_datasets.py` for catalog-filter flags).
- **Bug fix** — a failing test that the fix turns green.

`pytest` is the minimum pre-PR gate (see [Before you open a PR](#before-you-open-a-pr));
CI re-runs it on every PR via [`.github/workflows/ci.yml`](.github/workflows/ci.yml).

## Branching and commits

- Branch off `develop`. Branch names follow `<initials>/<topic>`
  (e.g. `mp/add-fastlanes`).
- Open PRs against `develop`.
- Commit messages: short imperative subject ("add X", "fix Y", "swap Z to W"),
  optional body explaining *why* the change is needed.

## Reporting bugs

Open an issue on
[GitHub Issues](https://github.com/spiraldb/raincloud/issues). Include the
slug you were building, the command you ran, and any traceback.

For security-related issues, do **not** open a public issue — see
[`SECURITY.md`](SECURITY.md) for the private channel.

## Coding style

- Python ≥ 3.11. Match the style of nearby code; the repo prefers terse,
  comment-light Python with explicit names over abstractions.
- No backwards-compat stubs or shims when removing handlers/slugs — git
  history is the fallback.
- Always go through `scripts.pipeline.spec.duckdb_connect` for DuckDB
  connections so resource limits and `storage_compatibility_version=v1.5.0`
  apply (see [`AGENTS.md`](AGENTS.md#invariants-dont-break-these)).

## License

By submitting a PR, you agree that your contribution will be licensed under
the [Apache License 2.0](LICENSE), the same license that covers the rest of
the project.
