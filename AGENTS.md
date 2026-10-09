# AGENTS.md

Guidance for AI coding agents working in this repo. The layout is deliberate and the
invariants below are easy to break by accident — read them before non-trivial changes.

For what raincloud is and how to use it, see [`README.md`](README.md). For the manifest
schema, [`sources.schema.md`](sources.schema.md). For step-by-step procedures,
[`SKILLS.md`](SKILLS.md). `CLAUDE.md` is a symlink to this file.

## Start here

On a fresh clone `outputs/` is empty. That's expected — artifacts are built, not shipped.

```bash
python -m raincloud.pipeline.status --fast --missing-only   # read-only; seconds
python -m raincloud.pipeline.validate_manifest              # schema + registry cross-checks
pytest                                                    # needs --extra dev --extra all
```

Installs are layered. A bare `uv sync --inexact` gets only the loader; builds need
`uv sync --extra build --inexact`, plus the extra a dataset needs for its format or
source: `osm`, `sas`, `excel`, `archives`, `generated` (the TPC-H/TPC-DS generators),
`kaggle`, `huggingface`. `--extra all` installs everything. **Always pass
`--inexact`**: without it, syncing one extra silently uninstalls the others, and a
later Kaggle/HF build fails.

Query the catalog rather than grepping `sources.json` or scrolling
`docs/v2/datasets.md`:

```bash
python -m raincloud.pipeline.list_datasets --handler uci_default --count
python -m raincloud.pipeline.list_datasets --kaggle-tos         # gated behind a one-time click-through (Kaggle or Hugging Face)
python -m raincloud.pipeline.list_datasets --grep '\bgeo' --long
raincloud describe <slug>                                      # one dataset's columns and types, from the catalog
python -m raincloud.pipeline.list_datasets --columns --column-grep PATTERN   # columns across locally built files
python -m raincloud.pipeline.list_datasets --coverage --source parquet       # type coverage of locally built files
python -m raincloud.pipeline.list_datasets --stale-version      # slugs BUILT under an OLDER schema_version (never-built excluded)
```

`--grep` is a regex over `slug short_name full_name description` joined by spaces,
so anchor one slug as `'^<slug> '`; `'^<slug>\b'` also matches `<slug>-hydrated`
and other hyphenated siblings.

Filters AND together across `--handler`, `--license`, `--fetch-type`, `--reader`,
`--vortex`/`--no-vortex`, `--kaggle-tos`, `--stale-version`, `--local`, `--grep`. Output:
default one bare slug per line (a terminal also marks hydrated ones `[hydrated]`),
`--long`, `--json`, `--count`. In `--long`, `recorded` is what the tracked catalog
records (in `--json`, `built_version` / `stale_version`) and `local` is the formats
prepared on this install's disk.

`raincloud.pipeline.browse` is a human-facing TUI — it will hang waiting for keystrokes.
Don't run it from an agent context; point the user at it instead.

## Invariants

1. **`sources.json` is authoritative.** Every row of every derived artifact maps back to
   a spec here. Never hand-edit `docs/*.md` or drop a file into `outputs/` — fix the
   manifest, rebuild, regenerate docs.
2. **`outputs/raw_downloads/<slug>/` is unversioned; `outputs/v{n}/<slug>/<format>/` is
   version-scoped.** Raw upstream bytes don't depend on schema version, so they're cached
   outside the version prefix and shared across versions. Path helpers live in
   `raincloud/pipeline/spec.py` — use them rather than composing paths by hand.
3. **`<scratch_dir>/.recipes/<recipe-hash>/<slug>/` is scratch.** Handlers clean up after
   themselves; `build.py --clean-workdir` removes only the selected generation. Clearing
   it forces re-extraction without disturbing another recipe.
4. **Raincloud code, tests and examples open DuckDB through `raincloud.duckdb_connect`**,
   never `duckdb.connect` directly. It applies the `RAINCLOUD_DUCKDB_*` resource limits
   and `storage_compatibility_version=v1.5.0`, which persistent VARIANT writes require.
5. **`docs/` is split.** Top-level `docs/*.md` is gitignored scratch. `docs/v{n}/*` is the
   tracked canonical set. `raincloud.pipeline.docs` writes to the top level; promoting to
   `docs/v{n}/` is a deliberate manual copy.
6. **A superseded version is frozen.** Artifacts under an older `outputs/v{n}/` are not
   rebuilt and must not be wiped — nothing regenerates them.
7. **`.archive/` is local-only and gitignored.** A fresh clone won't have it. Where other
   docs name it as a fallback, git history is the only one you can rely on.

## How a build works

Orchestrated by `raincloud.pipeline.build`. The pipeline is **canonical-Arrow-spined**:
transform produces Arrow, `write_canonical` persists the one canonical artifact, and
every output format is derived from it by an exporter.

| stage | module | reads | writes |
|---|---|---|---|
| fetch | `fetch.py` | `fetch.*` | `outputs/raw_downloads/<slug>/` |
| extract | `extract.py` | `extract.*` | `<scratch_dir>/.recipes/<hash>/<slug>/` |
| parse | `parse.py` | `parse.*` | in-memory `(Path, Table)` |
| transform | `transform.py` | `transform.*` | in-memory `(slug, Table)` |
| write_canonical | `canonical.py` | transform output | `outputs/v{n}/<slug>/arrow/<slug>.arrow.zstd` |
| validate | `validate.py` | `expect.*` | hashes canonical schema, checks rows; `[WARN]` unless `--strict` |
| run_exporters | `export/` | the install's `formats`, `export.priority` | `<fmt>/` under `outputs/v{n}/<slug>/`; the build record |
| hydrate *(named builds only)* | `hydrate.py` | `derive.hydrate` | a `<parent>-hydrated` dataset — outbound HTTP, safety-filter gated |

**Formats are opt-in, per install.** A dataset offers every exported format (a recipe's
`export.formats` can only narrow that); a build writes the install's `formats` setting —
only Vortex by default — or what `--format` names, and then removes the raw download and
the canonical unless `keep_raw` / `keep_canonical` are set (a canonical from which no
format was written stays: it is the dataset's file). Maintaining the catalog wants
everything, so a maintainer's config (or environment) sets `formats = "all"`,
`keep_raw = true` and `keep_canonical = true`; without them a checkout build deletes the
raw bytes a re-run would reuse.

`run_exporters` is also invokable on its own, which is the whole job whenever a
change touches only the export stage (row-group sizing, a codec, a new cell) —
the canonical is the input and is left alone:

```bash
python -m raincloud.pipeline.export <slug>...                   # re-derive from existing canonicals
python -m raincloud.pipeline.export <slug> --format vortex      # refresh only the Vortex file
python -m raincloud.pipeline.export <slug> --format parquet@rs  # this writer, this run
```

It refuses a slug with no canonical rather than silently starting a build. A
`--format parquet@rs` override replaces that dataset's file with one from another
writer, so its sha256 no longer matches the catalog; `--all` with it rewrites every
Parquet file in the store, which takes hours. Confirm before running either. The
file is `parquet/<slug>.parquet` whichever writer made it, and the build record
(`<data_dir>/builds.json`) records the writer; the catalog learns it only when a
maintainer regenerates it (see [Regenerating derived docs](#regenerating-derived-docs)).

Field-level `custom_metadata` (the `VARIANT_EXT` marker, GeoParquet `geo` metadata) rides
through the canonical IPC losslessly. Stamp VARIANT only through `variant.attach_variant` /
`attach_variant_schema` (the DuckDB bridge does): the stamp also declares the storage struct's
`metadata`, and an unshredded `value`, non-nullable, as the Parquet VARIANT spec and
`arrow.parquet.variant` require, and checks every row against that. DuckDB's Arrow export
declares every field nullable, which a hand-set marker would carry into the Parquet schema.

**Streaming handlers** write the canonical spine themselves via
`canonical.open_canonical_writer` and `return []`, so `write_canonical` is a no-op for
them; they share the same `validate → run_exporters` tail. To find which handlers do
this, check the `streaming` column in `docs/v2/handlers.md` — don't rely on a list here.

In `schema_version` 2, `export.formats` is the only declaration of which formats a
dataset exports. `convert.vortex` is v1-only: the schema and `validate_manifest`
reject it in a v2 manifest, while a released v2 catalog that still carries
`convert.vortex: false` (and no `export.formats`) keeps reading as Parquet-only. A
format is one file whichever writer makes it. The writer is the first *installed* one
in `export.priority`, looked up in the spec, then the catalog's `export_priority`, then
`RAINCLOUD_EXPORT_PRIORITY`, then the built-in `py, rs, java, cpp`. The spec and catalog
levels take a list, which applies to every format and so must name a writer for each
one the dataset exports, or a map from format to list (`{"parquet": ["rs", "py"]}`);
a format the map leaves out falls through to the next level. The machine level is a
list. The build record, and after regeneration the catalog, records the writer as
`<fmt>_writer`. Sidecar cells (`parquet@rs`, `parquet@java`, `parquet@hardwood`,
`vortex@rs`, `vortex@jni`) run only where their binary is installed. Compliance
measures every writer in scratch, never over the dataset's file.

Every Parquet writer is given one set of options (`spec.parquet_options`): the recipe's
`write.compression`, `write.statistics` and row cap, and the install's
`RAINCLOUD_PARQUET_*` settings in the table below (compression level, statistics and the
page index for all or the first N columns, page size and rows, dictionaries, page
checksums). Page indexes and checksums default on wherever supported: pyarrow and
parquet-java write both, arrow-rs cannot write checksums, and Hardwood cannot write
indexes. Page statistics require the recipe's statistics to be enabled. Other unset
settings use each library's defaults. A set one reaches every writer and
becomes part of its toolchain, and a writer whose library cannot do what it asks fails
that export as a measurement rather than writing something else; `sidecars/README.md`
tabulates which writer honours what, and the `/raincloud-write-settings` skill is the
procedure (a setting changes only files this install writes, so an existing file must be
re-exported or rebuilt to gain it). ORC, Avro and Vortex have the same kind of settings
(`spec.FORMAT_SETTINGS`, `RAINCLOUD_ORC_*`, `RAINCLOUD_AVRO_*`, `RAINCLOUD_VORTEX_*`), read
and refused the same way; their codec, unset, is the zstd raincloud has always written.

`export.formats` lists the formats a dataset wants. When the planned writer cannot
produce one for the dataset -- it raises, dies, reports a failed round-trip, or exceeds
`RAINCLOUD_EXPORT_TIMEOUT` -- the previous file comes back and the build records the
failure in the build record as that format's `unavailable` measurement (writer cell,
error, toolchain versions, recipe, canonical sha, time), then carries on: the dataset is
built with the formats that worked, `[unavailable] <slug>/<fmt>` is printed and repeated
in the summary, and the build exits 0. Never write a writer's limitation into
`export.notes`: docs regen carries the measurement into the snapshot
(`<fmt>_unavailable`), the loader reports it (`describe`; `FormatUnavailable` quoting
it; `auto` skips it), a later successful export replaces it, and `compliance` prints
`[stale opt-out]` once a writer round-trips it. In-process writers run in a forked
child so the limits can stop them: `RAINCLOUD_EXPORT_TIMEOUT` and
`RAINCLOUD_EXPORT_MEMORY` (resident memory, default half of RAM), and the child raises
its own `oom_score_adj` so a machine that runs short loses the writer, not the build.
Run an unattended build as a systemd unit with `OOMPolicy=continue`: the default
`stop` ends the whole unit when the kernel kills one process in it. `export` without `--format` behaves like the build;
with a bare `--format vortex` it records the failure and exits 1, and a named cell's
failure (`--format vortex@rs`) exits 1 and records nothing.

Every writer reads back what it writes before its file is promoted. An in-process
writer reads its file with the same format's in-process reader and compares it to the
canonical (`exporters.read_back`), streamed window by window (`compare.stream_equal`:
one batch of each side in memory, Parquet read batches sized by bytes), inside the
bounded child, so the time and memory limits cover the read too. A mismatch or a read
error (Vortex 0.86.1 writes a multi-batch VARIANT column it cannot read back) is a
failed round-trip, recorded as above; an in-process writer never reports
`roundtrip=None`, and a read-back that decides neither pass nor fail raises as a bug. A
sidecar verifies in its own process and may report `roundtrip: null` (a comparator gap,
or out of memory while verifying): that file is promoted, `[unverified] <slug>/<fmt>`
is printed and repeated in the summary, the build record keeps `verified: false` and the
writer's note as `verify_note` (every other export records `verified: true`), docs
regen carries them into the snapshot (`<fmt>_verified`, `<fmt>_verify_note`), and
`describe` shows the file as UNVERIFIED with the reason. The read-back is one more full
read of every file a build writes: on stackoverflow-badges (51M rows, 479 MB canonical)
it adds 6.7 s to an 8.1 s Parquet write and 2.0 s to a 2.7 s Vortex write; a streamed
compare peaks at 2.9 GiB resident on code-contests' 4.3 GB Parquet (18.5 GiB decoded)
and 1.1 GiB on jsonbench's 22.5 GB Vortex file.

A recorded failure is not repeated. Before a writer runs, `run_exporters` looks up the
measurement that applies (this install's build record at the recipe, else the catalog's
snapshot; `records.recorded_failure`). If it names the same writer cell with the same
toolchain (`writer_toolchain`, compared exactly: Python and library versions in
process, binary name and sha prefix for a sidecar) and, when it records one, the same
canonical sha, the format is skipped: `[skip] <slug>/<fmt>: ...; pass --retry-errors to
try again`, nothing new recorded, the measurement kept. Anything different (an upgraded
`vortex-data`, another writer, a rebuilt canonical) is attempted with a `[retry]` line
naming the difference. A skip is not a new failure: `build` and `export` without
`--format` exit 0 and list it in the summary; `export --format <fmt|cell>` and `convert`
asked for that file, so they exit 1. `--retry-errors` on `build`, `export` and `convert`
(`raincloud load --retry-errors`, `load(..., retry_errors=True)`, or the `retry_errors`
setting / `RAINCLOUD_RETRY_ERRORS`, which carries it to a child build) attempts it
anyway: a success replaces the measurement, a planned writer's failure records it again.
Compliance calls `run_bounded` directly and measures every write cell regardless, which
is how a stale opt-out is found. A write cell's `roundtrip` is the writer's own
read-back, including for an in-process file compliance finds on disk and does not
re-encode (`bounded.read_back_bounded`), and that read-back is also the in-process
writer's diagonal read cell (its own reader over its own file), not a second read. Only
a sidecar's `null` is filled from the read matrix's diagonal cell.

## Editing `sources.json`

Large hand-authored JSON with a fixed top-level key order (`schema_version`,
`generated_at`, `audit_cutoff`, `notes`, `datasets`). Edit structurally, never with `sed`:

```python
import json
from pathlib import Path
SRC = Path("sources.json")
m = json.loads(SRC.read_text())
for d in m["datasets"]:
    if d["slug"] == "target-slug":
        d["transform"]["handler"] = "new_handler"
        break
SRC.write_text(json.dumps(m, indent=2) + "\n")
```

Run `validate_manifest` afterwards. Templates for common edits are in
[`templates/`](templates/).

## Data locations

The build and loader resolve their roots in this order: explicit argument, environment,
user config file, system config file, default. `raincloud config show` prints what is
in effect and where each value came from. A source checkout skips the system config
files (the user config and `RAINCLOUD_CONFIG` still apply), so a checkout builds into
its own `outputs/` rather than a machine's shared store.

| env var | controls | default |
|---|---|---|
| `RAINCLOUD_HOME` | when set, forces `<home>/outputs` and `<home>/_workdir` | checkout root, else the user data dir |
| `RAINCLOUD_OUTPUTS` | built-artifact root (`v{n}/` and `raw_downloads/` live directly under it) | `<checkout>/outputs`; outside a checkout, the user data dir itself |
| `RAINCLOUD_RAW_DOWNLOADS` | cached raw upstream bytes | `$RAINCLOUD_OUTPUTS/raw_downloads` |
| `RAINCLOUD_WORKDIR` | scratch root holding `.recipes/` | `<checkout>/_workdir`; outside a checkout, `<user cache>/workdir` |
| `RAINCLOUD_MANIFEST` / `RAINCLOUD_SNAPSHOT` | a matched local catalog: `sources.json` and its snapshot | checkout copy, else the packaged copy |
| `RAINCLOUD_CATALOG` | which catalog: `auto`, `active`, `checkout`, `bundled`, `local` (the manifest `RAINCLOUD_MANIFEST` names), a revision or unique prefix, or a bundle/pack directory | `auto` |
| `RAINCLOUD_CATALOG_DIR` | installed catalog revisions | `<user data>/catalogs` |
| `RAINCLOUD_CATALOG_URL` | where `raincloud catalog update` looks when no `--source` is given | unset |
| `RAINCLOUD_CACHE` | optional separate artifact cache for the loader | same as the data root |
| `RAINCLOUD_MIRROR` | a private artifact store readers fall back to (`s3://` needs `[s3]`, `https://` needs `[http]`) | unset |
| `RAINCLOUD_OFFLINE` | `1`: read only local files; never contact the mirror | unset |
| `RAINCLOUD_RETRY_ERRORS` | `1`: a build attempts a format whose writer, with this toolchain, already failed at the recipe (as `--retry-errors`) | unset |
| `RAINCLOUD_FORMATS` | formats a build writes, e.g. `vortex,parquet`, or `all` (as `--format` for one build) | `vortex` |
| `RAINCLOUD_KEEP_RAW` | `1`: a successful build keeps the raw download (generated datasets always keep their generator output) | unset (removed) |
| `RAINCLOUD_KEEP_CANONICAL` | `1`: a successful build keeps the canonical Arrow (kept anyway when no other format was written) | unset (removed) |
| `RAINCLOUD_CONFIG` / `RAINCLOUD_NO_CONFIG` | select or disable the config file | unset |
| `RAINCLOUD_SETTINGS` | settings JSON the CLI reads with `--settings-env`; how native readers pass options | unset |
| `RAINCLOUD_DUCKDB_MEMORY_LIMIT` | DuckDB memory ceiling, applied by `raincloud.duckdb_connect` | DuckDB default (~80% RAM) |
| `RAINCLOUD_DUCKDB_THREADS` | DuckDB thread count | DuckDB default |
| `RAINCLOUD_DUCKDB_TEMP_DIRECTORY` | DuckDB spill directory | DuckDB default |
| `RAINCLOUD_FETCH_DEADLINE` | wall-clock ceiling on one download | 6 h (`0` disables) |
| `RAINCLOUD_GENERATOR_TIMEOUT` | ceiling on a generator subprocess | 6 h (`0` disables) |
| `RAINCLOUD_MAX_TABLE_CELLS` | rows x columns a whole-table handler may materialize | 50,000,000 (`0` disables) |
| `RAINCLOUD_MAX_DECOMPRESSED_BYTES` | one in-memory decompression | 4 GiB (`0` disables) |
| `RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES` | Parquet row-group size, in encoded bytes (parquet@java counts compressed pages, so its groups come out larger) | 128 MiB |
| `RAINCLOUD_ROW_GROUP_MAX_ROWS` | row cap per group, used only when a spec omits `write.row_group_size_rows`; a spec's cap wins in every writer, sidecars included | 10,000,000 |
| `RAINCLOUD_ROW_GROUP_TARGET_BYTES` | memory guard: decoded Arrow bytes buffered for one row group | 512 MiB |
| `RAINCLOUD_ROW_GROUP_PROBE_ROWS` | rows the Python Parquet writer samples to size its groups (must be > 0) | 262,144 |
| `RAINCLOUD_PARQUET_COMPRESSION_LEVEL` | the recipe's codec at this level in every Parquet writer (zstd 1-22, gzip 0-9, brotli 0-11) | unset (each writer's own) |
| `RAINCLOUD_PARQUET_STATISTICS_COLUMNS` | statistics (chunk and page) only for the first N leaf columns (`0`: every column) | unset (each writer's own: every column) |
| `RAINCLOUD_PARQUET_PAGE_INDEX` | `1`: every Parquet writer writes a page index (ColumnIndex + OffsetIndex); `0`: none | on where supported: pyarrow, arrow-rs and parquet-java; Hardwood cannot write one; page statistics require statistics on |
| `RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS` | page statistics only for the first N leaf columns, chunk statistics for every column (`0`: every column) | unset (each writer's own) |
| `RAINCLOUD_PARQUET_PAGE_BYTES` | data page size target in every Parquet writer, each measuring a page its own way (`0`: no limit) | unset (each writer's own) |
| `RAINCLOUD_PARQUET_PAGE_ROWS` | data page row limit in every Parquet writer (`0`: no limit) | unset (each writer's own) |
| `RAINCLOUD_PARQUET_DICTIONARY` | `0`: no dictionary encoding (PLAIN); `1`: dictionaries | unset (each writer's own: on) |
| `RAINCLOUD_PARQUET_DICTIONARY_PAGE_BYTES` | dictionary page size limit, past which a column falls back to PLAIN (`0`: no limit) | unset (each writer's own) |
| `RAINCLOUD_PARQUET_PAGE_CHECKSUMS` | `1`: a CRC in every page header; `0`: none | on where supported: pyarrow, parquet-java and Hardwood; arrow-rs cannot write them |
| `RAINCLOUD_ORC_COMPRESSION` | ORC codec in every ORC writer: `zstd`, `snappy`, `zlib`, `lz4`, `none` | unset (zstd, as always) |
| `RAINCLOUD_ORC_COMPRESSION_STRATEGY` | `speed` or `compression` (ORC C++ only; orc-rust refuses) | unset (each writer's own) |
| `RAINCLOUD_ORC_STRIPE_BYTES` | ORC stripe size target, measured encoded and compressed | unset (each writer's own) |
| `RAINCLOUD_ORC_COMPRESSION_BLOCK_BYTES` | ORC compression block size (ORC C++ takes only multiples of 64 KiB) | unset (each writer's own) |
| `RAINCLOUD_AVRO_COMPRESSION` | Avro codec in every Avro writer: `zstd`, `deflate`, `snappy`, `bzip2`, `xz`, `none` | unset (zstd, as always) |
| `RAINCLOUD_AVRO_COMPRESSION_LEVEL` | level for zstd (1-22), deflate or xz (0-9); Avro Java only, arrow-avro refuses | unset (each writer's own) |
| `RAINCLOUD_AVRO_BLOCK_BYTES` | Avro block (sync interval) size; Avro Java only, arrow-avro refuses | unset (each writer's own) |
| `RAINCLOUD_VORTEX_COMPACT` | `1`: BtrBlocks' compact encodings (vortex@py and vortex@rs; vortex@jni refuses) | unset (each writer's own: default) |
| `RAINCLOUD_VORTEX_ROW_BLOCK_ROWS` | Vortex row block size (vortex@rs only) | unset (each writer's own) |
| `RAINCLOUD_VORTEX_DATA_BLOCK_BYTES` | Vortex data block size target (vortex@rs only) | unset (each writer's own) |
| `RAINCLOUD_BATCH_ROWS` / `RAINCLOUD_BATCH_BYTES` | batch bounds in the streaming ingestion paths (memory only, NOT the row-group size) | 4096 rows / 16 MiB |
| `RAINCLOUD_EXPORT_PRIORITY` | machine writer preference, e.g. `rs,py` | unset (`py, rs, java, cpp`) |
| `RAINCLOUD_EXPORT_TIMEOUT` | ceiling on one export: an in-process writer (run in a child process) or a sidecar writer; hitting it records the format unavailable | 6 h (`0` disables) |
| `RAINCLOUD_EXPORT_MEMORY` | ceiling on one in-process export's resident memory (bytes); the parent stops a writer over it and records the format unavailable | half of physical memory (`0` disables) |
| `RAINCLOUD_SIDECAR_TIMEOUT` | ceiling on one sidecar reader call (sidecar writers use `RAINCLOUD_EXPORT_TIMEOUT`) | 30 min (`0` disables) |
| `RAINCLOUD_TPCGEN_CLI` | path to the `tpcgen-cli` executable the tpcgen-rs TPC-DS generator runs | beside the Python executable, else `PATH` |

Individual vars and explicit values win over `RAINCLOUD_HOME`. A malformed numeric value
is an error naming the variable, never a silent default. Loader handles and build
entry points freeze these for the duration of an operation. The seven `0 disables`
ceilings exist because builds are often left to run unattended: each bounds work that
is otherwise decided by an upstream file or an external process (a download, a
generator, a sidecar), and each can be lifted with `0` for a run that genuinely needs
it.

Two directories are named `.recipes/`. `<scratch_dir>/.recipes/<recipe-hash>/<slug>/`
is one recipe's extract scratch, so a changed recipe never reuses another's
intermediates. `<raw_dir>/<slug>/.recipes/<fetch-key>/` holds raw bytes for a catalog
or fetch recipe other than the one that owns `<raw_dir>/<slug>/` itself. Skills and
playbooks that say `<recipe-hash>` mean the first.

## Public loader API

`load` / `load_dataset`, `slugs()`, `describe(slug)`, `reader_capabilities()`,
`Config` / `resolve_config`, and the exception hierarchy. Anything under a leading
underscore is not it — an example that reaches into `raincloud._catalog` teaches
that import to everyone who copies it, and usage is what makes a name public.

## Confirm before rebuilding

Large datasets take hours, and a rebuild wipes and redoes existing work. Ask the user
before running `raincloud.pipeline.build` on anything non-trivial. Parquets under ~100 MB
are fine to rebuild unprompted.

## Regenerating derived docs

```bash
python -m raincloud.pipeline.docs    # datasets.md + handlers.md + snapshot.json
```

This is the only way the catalog changes: builds write the install's build record
(`<data_dir>/builds.json`), never the tracked snapshot, and regenerating takes each built
file's sha and writer from that record. In a checkout it writes the gitignored scratch
copies under `docs/`; promote them, then review and commit:

```bash
python -m raincloud.pipeline.docs
cp docs/snapshot.json docs/datasets.md docs/handlers.md docs/v2/
git diff docs/v2/
```

A machine's shared store is released from a commit with
`python -m raincloud.pipeline.publish <slugs|--all> --store DIR --catalogs DIR`.

**The snapshot is load-bearing.** `datasets.md` regen reads from disk for locally built
slugs and falls back to the snapshot for everything else. A partial regen without that
fallback would dash out most rows and destroy ground truth. The no-args form regenerates
snapshot and datasets in lockstep — prefer it; `docs.py datasets` alone will not refresh
the snapshot.

## Conventions

- **One handler per upstream shape.** Don't stretch `tighten_types` or `identity` — add a
  handler under `raincloud/pipeline/handlers/` and declare it in `HANDLERS` in
  `raincloud/_registry.py` (see below). Read `docs/v2/handlers.md` first to pick
  precedent and see which extras you'll need.
- **Handlers stay short.** Most are under 150 lines; reuse `open_canonical_writer`,
  `duckdb_connect`, `workdir_root`, `spec_field` before growing one.
- **No backwards-compat stubs.** Remove a handler or slug fully; git history is the
  fallback.
- **Handlers, exporters and generators are declared in `raincloud/_registry.py`**,
  and nowhere else. The registries build from it (`handlers/__init__.py` only
  resolves names lazily), and the capability list a catalog bundle records derives
  from it, so adding one is a single edit. That module
  imports nothing, which is what lets the loader answer "can this be built here?"
  without pulling in the build toolchain.
- **Version numbers have one home each.** The release version is
  `raincloud/__init__.py:__version__`. `pyproject.toml`, the Java client and the
  C/C++ CMake build read it. Two files cannot and carry a literal:
  `clients/rust/Cargo.toml` (Cargo requires one) and `CITATION.cff`. Bump them with
  it; `tests/test_loader_package.py::test_version_mirrors_agree` fails on drift. The set of
  artifact layouts is the `schema_version` enum in `sources.schema.json`, read by
  Python at runtime; the native clients hold no copy, because the CLI resolves
  layouts for them. Don't add a second copy, and don't give `schema_version` a
  default — a wrong one silently selects another layout.
- **Upstream bytes are untrusted input.** Archive members get `_safe_target` +
  `_claim` (no escape, no two members on one path); anything written under a final
  name goes through an atomic temp-then-rename, so `[cached]` can mean "complete";
  and a row the pipeline drops gets counted and printed. Silence is the bug — a
  short table looks exactly like a correct one.
- **Test the observable result**, not the mock. Use real small Arrow/Parquet files and
  real build subprocesses; don't mock the resolver, serializer, checksum or builder that
  the test exists to verify. Wheel and live-upstream tests are opt-in
  (`--run-wheel`, `--run-network`).

## Where things are

- `raincloud/pipeline/` — the build stages and CLI entry points
- `raincloud/` — the importable loader; `clients/` — Rust, C/C++, Java readers
  (`clients/README.md`). The native readers hold no catalog; they ask the `raincloud`
  CLI, so there is nothing of the catalog to keep in step there.
- `sidecars/` — reference writers/readers behind the sidecar cells, a PATH-discovered CLI
  contract. The JVM lanes are a Gradle composite build over a git submodule; a fresh
  clone needs `git submodule update --init`.
- `raincloud.pipeline.compliance` — maintainer-run measurement of the
  `(slug × format × impl)` matrix. Never gates a build; absent toolchains `skip`.
  `--check-oracle` runs the additive-only gate: a cell may be added, never removed or
  mutated. Re-measure the committed oracle whenever the toolchain pins move; until
  then the gate reports every cell the new toolchain changed as a mutation. See
  `sidecars/README.md`.
- `.agents/skills/` — invokable skills wrapping the pipeline entry points.
  `.claude → .agents` is a symlink. `.agents/settings.json` is a tracked read-only
  command allow-list; machine overrides go in the gitignored `settings.local.json`.

## When you're unsure

Read, then grep, then ask — don't guess. The pipeline has contracts that aren't visible
from any single file: streaming handlers returning `[]`, `raw_downloads` being
unversioned, VARIANT requiring the DuckDB compatibility setting.
