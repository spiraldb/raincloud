---
name: raincloud-load
description: Read prepared Raincloud artifacts, inspect catalog metadata, or load batches from local storage or a configured mirror.
argument-hint: <slug> [--format auto|arrow|parquet|vortex|orc|avro|nimble]
disable-model-invocation: true
allowed-tools: Bash(python -m raincloud describe *), Bash(python -m raincloud list *), Bash(python -m raincloud load *), Bash(python -m raincloud config show), Bash(python -m raincloud capabilities), Bash(python examples/use_loader.py *)
---

Use the lightweight Python loader for prepared data. Resolution is local data/cache
then mirror. Reads never build unless explicitly requested with `build=True` or
`raincloud load <slug> --build`. Never add `--build` (or `build=True`) yourself: a
build can run for hours, so it needs the user's go-ahead as described in AGENTS.md,
even though this skill's allowed commands would match it.

For the dataset the user named (`$ARGUMENTS`), start with the commands:

```bash
python -m raincloud describe $ARGUMENTS   # catalog metadata: rows, columns, formats, license
python -m raincloud load $ARGUMENTS       # resolve the file and print its path
```

From Python:

```python
import raincloud

ds = raincloud.load("SLUG")  # lazy; auto selects an available reader
print(ds.num_rows, ds.column_names, ds.catalog_revision, ds.artifacts)
with ds.batches(batch_size=65536) as batches:
    for batch in batches:
        print(batch.num_rows)   # each is a PyArrow RecordBatch
```

Metadata does not fetch artifact bytes. `.path()`, `.schema`, and `.batches()`
resolve the artifact as needed. `.to_arrow()` materializes the whole table.
`.to_pandas()` requires `[pandas]`. `.dataset()` is a lazy pyarrow Dataset of the
selected format for any engine (DuckDB, Polars, pyarrow). `.to_vortex()` requires
`[vortex]` and a Vortex artifact. `.path()`, `.batches()`, `.to_arrow()` and
`.dataset()` retain the selected artifact; `.to_vortex()` resolves the dataset's
Vortex file. Each format is one file; `describe` shows which writer made it. ORC reads
through pyarrow; Avro and Nimble are served by `.path()` only.

A load serves the file it finds. Encoder settings (a Parquet page index, compression
level, ...) apply only when this install writes a file, so a file already on disk, in
the store or on a mirror is served as it was written, and `--build` builds only a file
that is missing. To get one written with settings, see `/raincloud-write-settings`.

Use `raincloud capabilities` for installed reader modules and `raincloud config
show` for effective paths. Optional TOML settings and environment overrides share
one resolution policy across the CLI, Python API, and native clients:

| Override | Meaning |
|---|---|
| `RAINCLOUD_OUTPUTS` | Durable artifact root; may be an existing outputs directory on another disk |
| `RAINCLOUD_CACHE` | Optional extra download cache; defaults to the durable root |
| `RAINCLOUD_MIRROR` | Private artifact source: local/file, HTTP(S), or optional fsspec transport |
| `RAINCLOUD_OFFLINE=1` | Read only local data/cache; missing files raise `OfflineMiss` |
| `RAINCLOUD_CATALOG_DIR` | Installed immutable catalog revisions |
| `RAINCLOUD_MANIFEST` / `RAINCLOUD_SNAPSHOT` | Explicit matched local catalog files |

`raincloud catalog update`, `pin`, and `rollback` are explicit operations; ordinary
reads never refresh the catalog. Prefer a matched bundle over unrelated loose
manifest/snapshot files. The catalog is the authority: mirror bytes that do not
match its sha256 are refused. A local file of another size is served only when
this install's build record (`<data_dir>/builds.json`) names it at that size under
the dataset's current recipe; the loader says so when the record came from an
earlier recipe.

Install from the Git repository (pin a reviewed revision with `@<commit>`) or a
local wheel:

```bash
pip install "raincloud @ git+https://github.com/spiraldb/raincloud"
# Optional extras on that same source: [vortex], [pandas], [s3], [http].
# [build] includes the builder and Vortex; it does not enable automatic builds.
```

The base install reads IPC and Parquet. Vortex 0.86.1 publishes Linux, macOS, and
Windows wheels; local Vortex execution and the native clients' source builds are
verified on Linux x86-64 only.

Reads raise subclasses of `raincloud.RaincloudError` (all in `raincloud.exceptions`):
`UnknownSlug` (with `suggestions`), `UnknownColumn`, `FormatUnavailable` (its
`measurement` is set when a build measured the format's writer unable to produce it at
this recipe; the message quotes it, and `auto` skips that format; a requested build skips
a failure recorded for the same writer and toolchain, and raises it again, unless
`retry_errors=True` / `--retry-errors`, which implies a build),
`ArtifactNotFound` (not prepared locally or in the mirror), `OfflineMiss`,
`MirrorUnavailable` (the mirror could not be read at all), `ChecksumMismatch`,
`CorruptArtifact` (the file is present but cannot be decoded; re-fetch or rebuild),
`UnsupportedType` (the reader cannot represent a type; ask for another format),
`MissingDependency`, `CatalogError` / `MissingRevision`, and with a build requested
`BuildToolingMissing` / `BuildFailed`. Preserve these distinctions when reporting a
failure.

See [AGENTS.md](../../../AGENTS.md), [configuration and catalogs](../../../README.md),
and [native clients](../../../clients/README.md). The
[loader example](../../../examples/use_loader.py) defaults to metadata only;
`--materialize` resolves files and may contact the configured mirror.
