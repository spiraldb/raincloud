# 🌧️ Raincloud

[![CI](https://github.com/spiraldb/raincloud/actions/workflows/ci.yml/badge.svg)](https://github.com/spiraldb/raincloud/actions/workflows/ci.yml)
[![Latest release](https://img.shields.io/github/v/release/spiraldb/raincloud)](https://github.com/spiraldb/raincloud/releases)
[![License](https://img.shields.io/github/license/spiraldb/raincloud)](LICENSE)
[![Cite](https://img.shields.io/badge/cite-CITATION.cff-blue)](CITATION.cff)

**Raincloud is a harness for data management research.** It curates the datasets the
field actually works with — standard benchmarks, real-world workloads at real scale,
and data chosen to stress storage formats — and gives you one way to fetch, build and
read all of them.

Raincloud distributes the harness, not the data. Each dataset is a reproducible
recipe: where the bytes come from, how to turn them into Arrow, Parquet and Vortex,
and how to check you got what was expected. The catalog records the sha256 and size
of every file a recipe produces. Bytes coming from a mirror, or entering a shared
store, are checked against it; a build you run yourself is recorded in your install's
build record and served from there, and may differ when an upstream has drifted.
You either read prepared files, from a store an operator set up on your machine or a
mirror your team runs, or you build a dataset yourself from its upstream source.

> ⚠ **Third-party data.** Raincloud fetches from URLs declared in `sources.json`.
> Those bytes come from upstream sources, not from us — we don't audit, host or
> redistribute them. See [`DISCLAIMER.md`](DISCLAIMER.md).

---

## What's in it

**Over 250 real-world datasets, plus tables from standard data generators**
(`raincloud list --count` counts every entry, generated tables included). Selection is
by relevance to data management research — some of it canonical, much of it niche, chosen
for what it exercises rather than how well known it is. The catalog grows; what follows
is what it is for, not an inventory.

**Standard benchmarks.** TPC-H and TPC-DS, wired to more than one generator so you can
compare implementations rather than trusting a single one, at SF1, SF10 and SF100
(another scale factor is one manifest entry away). Alongside them the Public BI
benchmark, the Join Order Benchmark over the IMDb tables, ClickBench, and the Appian
benchmark set.

**Real-world data at awkward sizes.** Weather observations in the billions of rows
(`ghcn-daily`), Wikipedia's structured contents in the tens of gigabytes, the full
Stack Overflow dump split into posts, users, tags, badges and post links, OpenStreetMap
Germany as nodes, ways and relations, NYC taxi trips, the 2011 Google cluster trace,
Open Library and Hacker News. Sizes span from empty tables to billions of rows, because a
format that behaves well at one scale often does not at another.

**Encoding and type stress.** The catalog is curated so a format or encoding change has
something real to run against: deeply nested structs and lists, dense float vectors
both fixed-width and variable (embeddings from GloVe, DBpedia and Cohere), JSON
promoted to VARIANT, geospatial geometry as WKB, and decimals carried through the
benchmark tables.

**Classic tabular.** The UCI staples, for when you want something small, familiar and
quick to reason about.

For the actual contents, query the catalog rather than reading a list here. These
commands answer from catalog metadata, so they work before anything is built:

```bash
raincloud list --long            # every dataset: handler, source, license, rows, what is on disk here
raincloud list tpch lineitem     # only the entries matching every word
raincloud describe uci-iris      # one dataset: columns and types, rows, formats and sizes, license
```

## Getting data

Install the reader. It is deliberately light: Arrow, NumPy, fsspec and platformdirs.
Requires Python 3.11+.

```bash
pip install "raincloud @ git+https://github.com/spiraldb/raincloud"
```

Raincloud reads prepared files, and they come from one of three places: a store an
operator has set up on this machine (a system config file names it), a mirror, or a
build you run yourself. A mirror is a private artifact store your team fills with
`python -m raincloud.pipeline.publish --mirror` and points readers at with
`RAINCLOUD_MIRROR` (or `--mirror`): a directory (`file:///path`), `s3://bucket/prefix`
with the `[s3]` extra, or `https://…` with `[http]`. There is no public mirror. On a fresh install nothing is
prepared, so build the dataset first. Building needs the `[build]` extra (see
[Building locally](#building-locally)):

```bash
pip install "raincloud[build] @ git+https://github.com/spiraldb/raincloud"
raincloud build uci-iris
```

Then read it by name. Nothing is read until you ask for data:

```python
import raincloud

ds = raincloud.load("uci-iris")
ds.num_rows            # catalog metadata, no bytes read
ds.column_names

table = ds.to_arrow()                      # materialize the whole table

with ds.batches(columns=["sepal_length"]) as batches:
    for batch in batches:                  # or stream PyArrow RecordBatches
        print(batch.num_rows)

d = ds.dataset()                           # lazy pyarrow Dataset of the loaded format;
con = raincloud.duckdb_connect()           # DuckDB, Polars and pyarrow scan it with pushdown
con.sql("select count(*) from d").show()   # DuckDB needs `raincloud[duckdb]`
ds.to_pandas()                             # pandas DataFrame  [pandas]
```

`raincloud load uci-iris` prints the path of the file instead, and
`raincloud describe uci-iris` shows which formats are prepared on this machine.

A format the dataset's writer could not produce is reported, not offered as a
build: when a build measured that (the writer raised, crashed, or ran past
`RAINCLOUD_EXPORT_TIMEOUT`), `raincloud describe` shows the format as unavailable
with the writer, its version and its error, asking for it raises
`FormatUnavailable` quoting them, and `format="auto"` skips it.

Reads never build. A dataset that is not prepared raises `ArtifactNotFound`, and
the message names the command that prepares it. To let a read build, ask for it
explicitly (this needs `[build]` too):

```python
raincloud.load("uci-iris", build=True)
```
```bash
raincloud load uci-iris --build
```

**Which format you get.** A dataset can have Arrow IPC, Parquet and Vortex files.
`format="auto"`, the default, picks Vortex, then Parquet, then Arrow, among the
formats you have a reader for. The base install reads Arrow and Parquet; `[vortex]`
adds Vortex, and `[build]` includes it. Installing either one therefore changes what
`auto` returns, down to the Arrow types: Vortex returns some string columns as
`string_view`. Name the format when that matters:
`raincloud.load("uci-iris", format="parquet")`.

Other extras: `[s3]` and `[http]` add mirror transports, `[pandas]` backs `.to_pandas()`.

### From other languages

Prepared data reads from Rust, C, C++ and Java, returning native Arrow record
batches. The native clients only read. Choosing, downloading and verifying the file
is done by the Python `raincloud` command, which they run, so install the Python
package as well. They find it through their `cli` option, then `RAINCLOUD_CLI`, then
`PATH`.

```rust
use raincloud_reader::{Dataset, RecordBatchReader};
use serde_json::json;

fn main() -> raincloud_reader::Result<()> {
    // raincloud_reader::load("uci-iris")? uses default settings and picks the format.
    let ds = Dataset::load(&json!({"config": "/path/to/config.toml"}), "uci-iris", "parquet")?;
    let batches = ds.batches(65536)?;
    println!("{:?}", batches.schema());
    for batch in batches {
        println!("{} rows", batch?.num_rows());
    }
    Ok(())
}
```

The clients are built from source; no native binary packages are published yet.
The C and C++ build installs a relocatable CMake package, and the Java build a
runtime distribution. See [`clients/README.md`](clients/README.md). These source
builds are verified on Linux x86-64 only.

### Pinning a catalog

The catalog is versioned independently of the software and of your data. Updates
are explicit — nothing refreshes underneath a running job.

```bash
raincloud catalog status                     # the catalog in use, and the installed revisions
raincloud catalog update --source LOCATION   # install the latest revision from a catalog source
raincloud catalog pin <revision>             # a revision, or the 12-character prefix `raincloud` prints
raincloud catalog rollback
```

There is no hosted catalog. `update` needs the location your operator or team
publishes catalogs to: a local directory or an HTTPS URL, given as `--source` or as
`catalog_url` in the config file.

## Adding your own

A dataset is one entry in the manifest, [`sources.json`](sources.json). The shape,
abridged to the parts you will actually think about — where the bytes come from, how
to parse them, and what you expect to get:

```json
{
  "slug": "my-dataset",
  "short_name": "My Dataset",
  "description": "One-line summary of what this contains.",
  "license":   { "spdx": "CC0-1.0", "redistribution_permitted": true },
  "fetch":     { "type": "http", "urls": ["https://example.org/data.csv"] },
  "parse":     { "reader": "csv", "options": { "has_header": true } },
  "transform": { "handler": "tighten_types" },
  "expect":    { "rows": 1000 }
}
```

That block is illustrative and will not validate on its own; copy
[`templates/minimal_spec.json`](templates/minimal_spec.json), which does. Many
datasets need nothing more than a stock handler (`tighten_types`, `uci_default`,
`identity`). When upstream data needs real work, write a handler under
`raincloud/pipeline/handlers/` and name it here; see [`templates/`](templates/) for
starting points and [`sources.schema.md`](sources.schema.md) for the full schema.

In a source checkout, edit `sources.json` itself. A pip install reads the copy
inside the package, which `raincloud config show` names under `manifest`. Copy that
file, add your entry, and point Raincloud at the copy:

```bash
export RAINCLOUD_MANIFEST=/path/to/my-sources.json   # or `manifest = "..."` in the config file
```

On a machine whose config file selects a catalog (an operator-configured store
does), a manifest override is refused until you also set `RAINCLOUD_CATALOG=local`,
which says to read the manifest you named. Every command then reads your manifest. Validate the entry, then build it:

```bash
python -m raincloud.pipeline.validate_manifest
raincloud build my-dataset
```

## Building locally

Builds need the pipeline toolchain, which is not installed by default:

```bash
pip install "raincloud[build] @ git+https://github.com/spiraldb/raincloud"
```

Some datasets need one more extra, for their file format or their source:

| extra | for |
|---|---|
| `osm` | OpenStreetMap PBF extracts |
| `sas` | SAS transport (`.xpt`) files |
| `excel` | Excel workbooks |
| `archives` | 7-Zip and Unix `.Z` archives |
| `generated` | TPC-H and TPC-DS tables from the DuckDB and tpcgen-rs generators |
| `kaggle` | Kaggle-hosted datasets; downloading needs a Kaggle API token, in `KAGGLE_API_TOKEN` or `~/.kaggle/access_token` (the legacy `~/.kaggle/kaggle.json` still works). A download already on disk needs no token. |
| `huggingface` | Hugging Face datasets; gated ones need a token (`hf auth login`, or `HF_TOKEN`) |

Combine them as `raincloud[build,osm,kaggle]`; `raincloud[all]` installs everything.
Generators never download anything while they run, so two one-time steps come
first. The DuckDB-generated tables need DuckDB's `tpch` / `tpcds` extension:
`python -c "import duckdb; duckdb.execute('INSTALL tpch')"` (and the same with
`tpcds`); a build that lacks it prints this command. The TPC-DS tables from
tpcgen-rs need that generator's CLI, built from a clean tpcgen-rs checkout at the
commit the recipe pins (the `+git.<commit>` in its `fetch.version`, which the build
error also names):
`python -m raincloud.pipeline.generators.install_tpcgen --source <checkout>`.

Built files land under `<data_dir>/v2/<slug>/{arrow,parquet,vortex}/`. The data
directory is `outputs/` in a source checkout and the platform's user data directory
otherwise (`~/.local/share/raincloud` on Linux). Raw downloads are cached under
`<data_dir>/raw_downloads/`, outside the version prefix, and are shared across
schema versions. Every location is configurable: `raincloud init --data-dir DIR`
writes the setting, and `raincloud config show` prints what is in effect.

Some datasets are large: single datasets can run for hours, and a full catalog build
is measured in days. Build what you need.

A build does not fail because one format's writer cannot handle a dataset. Every
export is bounded by `RAINCLOUD_EXPORT_TIMEOUT` (default 6 h; `0` disables it); a
writer that raises, crashes, reports a failed round-trip or runs out of time leaves
the previous file in place, the failure is recorded in the build record
(`<data_dir>/builds.json`), and the build prints `[unavailable] <slug>/<format>`,
repeats it in its summary, and succeeds with the formats that worked. A later
successful export (`python -m raincloud.pipeline.export <slug> --format <format>`)
replaces the record.

A recorded failure is not repeated: the next build skips that format with a
`[skip]` line when the same writer, with the same toolchain, already failed on the
same inputs, and still exits 0. An upgraded writer library is attempted again on
its own. To try again with nothing changed, pass `--retry-errors`
(`raincloud build <slug> --retry-errors`, `raincloud load <slug> --retry-errors`,
or `load(..., retry_errors=True)`).

## Licensing and redistribution

Each entry carries an SPDX identifier, a source URL, and an explicit
`redistribution_permitted` flag. **A large share of the catalog may not be
redistributed** — which is why raincloud ships recipes rather than bytes, and why the
build fetches from upstream.

If you mirror artifacts for your own team, `python -m raincloud.pipeline.publish
--mirror URL` refuses to upload anything whose license forbids it. Your obligations
are the upstream licenses; the catalog records them but does not grant them.

## More

- [`AGENTS.md`](AGENTS.md) — working in this repo; also the entry point for coding agents
- [`SKILLS.md`](SKILLS.md) — task-by-task procedures
- [`CONTRIBUTING.md`](CONTRIBUTING.md) · [`SECURITY.md`](SECURITY.md) · [`DISCLAIMER.md`](DISCLAIMER.md)
- [`CITATION.cff`](CITATION.cff) — how to cite this work
