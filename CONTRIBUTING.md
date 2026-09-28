# Contributing to Raincloud

Thanks for your interest in Raincloud. This guide covers how to set up a dev
environment, run the test suite, and submit changes. For deeper dives into the
pipeline itself, see [`README.md`](README.md), [`AGENTS.md`](AGENTS.md), and
[`SKILLS.md`](SKILLS.md).

## Setting up

```bash
git clone --recurse-submodules https://github.com/spiraldb/raincloud.git
cd raincloud
uv sync --extra dev --extra all --inexact
```

The submodule (`sidecars/java/parquet-arrow-java`) is needed only for JVM
sidecar work; in a clone made without it, run `git submodule update --init
--recursive`.

`--extra dev` pulls in `pytest`; `--extra all` installs every optional part. The
base install is the lightweight loader (`pyarrow`, `numpy`, `fsspec`,
`platformdirs`). The pipeline core (duckdb, zstandard, jsonschema, Vortex) is the
`build` extra, and each handler's format-specific dependency is its own extra,
imported only when that handler runs: `osm` (osmium), `sas` (pyreadstat), `excel`
(openpyxl, pandas), `archives` (py7zr, unlzw3). A build that needs one you lack
says which to install. `generated` covers the TPC-H/TPC-DS generators
(tpchgen-cli and a pinned DuckDB), and `kaggle` and `huggingface` cover those
upstreams. Always pass `--inexact` — without it, each `uv sync --extra X` removes
the extras from the previous one (e.g. syncing `--extra dev` after
`--extra huggingface` uninstalls `huggingface_hub`).

## Before you open a PR

Three checks are the minimum gate (CI runs all three):

```bash
ruff check                                       # lint (pyflakes + pycodestyle + isort); seconds
python -m raincloud.pipeline.validate_manifest   # JSON Schema + cross-checks on sources.json; seconds
pytest                                           # the hermetic suite; minutes, needs --extra dev --extra all
```

`pytest` covers the manifest, schema and registry, the `raincloud` loader
(catalog resolution, cache/mirror dispatch, sha256 integrity), the examples, and
real small builds run in temporary directories. CI's hermetic lane syncs
`--extra dev --extra tui --extra build --extra pandas --extra osm --extra sas
--extra excel --extra archives`; `--extra all` adds the acquisition extras and pins DuckDB to the
`generated` extra's exact version, so a local run with it can differ from that lane. After a manifest-only edit,
`pytest tests/test_manifest.py` is the fast subset.

If you touched the build pipeline, install the build extra and run a small
end-to-end build to make sure it still produces the expected output:

```bash
uv sync --extra build --inexact
python -m raincloud.pipeline.build countries-of-the-world   # ~200 ms, 262 rows
```

For larger builds, see [`SKILLS.md`](SKILLS.md#running-a-large-build-safely).

## What to send a PR for

- **New datasets** — see [`SKILLS.md`](SKILLS.md#adding-a-new-dataset). Most
  entries copy [`templates/minimal_spec.json`](templates/minimal_spec.json) and
  pick an existing handler from [`docs/v2/handlers.md`](docs/v2/handlers.md).
- **New transform handlers** — see
  [`SKILLS.md`](SKILLS.md#adding-a-new-transform-handler). One handler per
  upstream shape; declare it in `HANDLERS` in `raincloud/_registry.py`, the
  only registration.
- **Bug fixes** — start with a failing test where practical.
- **Documentation** — README/AGENTS/SKILLS edits welcome. The derived docs
  (`docs/v2/datasets.md`, `docs/v2/handlers.md`, `docs/v2/snapshot.json`) are
  machine-generated; don't hand-edit them — fix the manifest or the registry,
  regenerate via `python -m raincloud.pipeline.docs`, and promote the result
  (see [`AGENTS.md`](AGENTS.md#regenerating-derived-docs)).

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
- Raincloud code, tests and examples always go through `raincloud.duckdb_connect`
  for DuckDB connections so resource limits and
  `storage_compatibility_version=v1.5.0` apply (see
  [`AGENTS.md`](AGENTS.md#invariants)).

## License

By submitting a PR, you agree that your contribution will be licensed under
the [Apache License 2.0](LICENSE), the same license that covers the rest of
the project.
