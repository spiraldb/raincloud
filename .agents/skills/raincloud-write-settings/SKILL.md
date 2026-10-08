---
name: raincloud-write-settings
description: Write a dataset's files with chosen encoder settings — a Parquet page index (for every column or the first N), Parquet compression level, statistics, page sizes, dictionaries or page checksums; the ORC or Avro codec and level; Vortex compact encodings or block sizes. Use when the user wants page statistics / a page index / column indexes in Parquet, a different codec or compression level, or to compare encoder settings across writers.
argument-hint: <slug> [setting=value ...]
---

Encoder settings are **install settings in the environment**, read by every writer of the
format alike (`raincloud/pipeline/spec.py`: `_PARQUET_SETTINGS`, `FORMAT_SETTINGS`). They are
not recipe fields and not load options. The full list, with defaults, is the
`RAINCLOUD_PARQUET_*`, `RAINCLOUD_ORC_*`, `RAINCLOUD_AVRO_*` and `RAINCLOUD_VORTEX_*` rows of
the table in [AGENTS.md "Data locations"](../../context/AGENTS.md#data-locations); which
writer honours which is tabulated in [`sidecars/README.md`](../../../sidecars/README.md).

## What to know before changing a setting

- **Unset means each writer library's own default**, and is what every published file was
  written with. pyarrow (the default Parquet writer, `parquet@py`) writes **no page
  index** unless asked; arrow-rs and parquet-java write one.
- **A setting applies to files this install writes.** It does not change a file already on
  disk, in this machine's store, or on a mirror, and `raincloud load` serves whichever of
  those it finds first. To get a file with the setting, write it again (below).
- **A file written with a setting differs from the catalog's** (another sha256). The build
  record (`<data_dir>/builds.json`) records it, and the loader serves this install's file.
  Do not do this to a shared store others read without asking.
- **A writer that cannot honour a set value refuses**, rather than writing something else:
  the format is recorded unavailable with the reason (`[unavailable] <slug>/parquet:
  parquet@py cannot honour RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS=100: ...`). When the default
  writer refuses, pick another: `export --format parquet@rs` names it for one export; a
  build takes only the format, so set the machine's writer preference instead
  (`RAINCLOUD_EXPORT_PRIORITY=rs,py`; a recipe's or the catalog's `export.priority` outranks
  it). arrow-rs (`rs`) is the writer for a page index on the first N columns only.
- **Contradictions are refused before any writer runs**: a page index while the recipe turns
  statistics off, `RAINCLOUD_PARQUET_PAGE_INDEX=0` with `_PAGE_INDEX_COLUMNS`, a level for a
  codec without levels, a level out of range. A malformed value names the variable.
- **A set value is part of the writer's toolchain**, so a failure recorded without it is
  attempted again, and one recorded with it is skipped until something changes.

## Parquet page index

```bash
# every column; the default writer (pyarrow) does this
export RAINCLOUD_PARQUET_PAGE_INDEX=1
# or: page statistics for the first 100 leaf columns only, chunk statistics for all
# (arrow-rs only: pyarrow writes a page index for all columns or none)
export RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS=100
# or: all statistics, chunk and page, for the first 100 leaf columns only
# (pyarrow, arrow-rs and parquet-java; Hardwood refuses)
export RAINCLOUD_PARQUET_STATISTICS_COLUMNS=100
```

"Leaf columns" are the Parquet column chunks, in schema order: a struct or list contributes
one per leaf field.

## Writing the file again

```bash
# The canonical Arrow is on disk (keep_canonical, or a maintainer's store): re-derive only
python -m raincloud.pipeline.export <slug> --format parquet
# Otherwise rebuild (refetches unless the raw download was kept)
python -m raincloud.pipeline.build <slug> --format parquet
```

`raincloud load <slug> --format parquet --build` builds only when no file is found, so it
does not rewrite an existing one. Confirm with the user before rebuilding anything large
([AGENTS.md "Confirm before rebuilding"](../../context/AGENTS.md#confirm-before-rebuilding)).

## Checking the result

```python
import pyarrow.parquet as pq, raincloud
meta = pq.ParquetFile(raincloud.load("<slug>", format="parquet").path()).metadata
chunks = [meta.row_group(g).column(c) for g in range(meta.num_row_groups) for c in range(meta.num_columns)]
print(sum(c.has_column_index for c in chunks), "of", len(chunks), "column chunks have a page index")
```

`raincloud describe <slug>` shows the writer that made the file; an `[unavailable]` line or a
`FormatUnavailable` from `load` quotes a writer's refusal.

## Other settings

| want | set |
|---|---|
| Parquet zstd/gzip/brotli level | `RAINCLOUD_PARQUET_COMPRESSION_LEVEL` (zstd 1-22, gzip 0-9, brotli 0-11; Hardwood refuses) |
| Parquet page size / rows per page | `RAINCLOUD_PARQUET_PAGE_BYTES`, `RAINCLOUD_PARQUET_PAGE_ROWS` |
| Parquet without dictionaries | `RAINCLOUD_PARQUET_DICTIONARY=0` |
| Parquet page checksums | `RAINCLOUD_PARQUET_PAGE_CHECKSUMS=1` (arrow-rs refuses) or `=0` (Hardwood refuses) |
| ORC codec / stripes | `RAINCLOUD_ORC_COMPRESSION`, `RAINCLOUD_ORC_STRIPE_BYTES`, `RAINCLOUD_ORC_COMPRESSION_BLOCK_BYTES` |
| Avro codec / level / blocks | `RAINCLOUD_AVRO_COMPRESSION`, `RAINCLOUD_AVRO_COMPRESSION_LEVEL`, `RAINCLOUD_AVRO_BLOCK_BYTES` (the last two Avro Java only) |
| Vortex compact encodings | `RAINCLOUD_VORTEX_COMPACT=1` (vortex@jni refuses) |

The Parquet codec itself is the recipe's `write.compression`, which wins over the
environment; changing it is a recipe edit, which changes the recipe hash.

Context: [AGENTS.md "How a build works"](../../context/AGENTS.md#how-a-build-works),
[SKILLS.md](../../context/SKILLS.md#writing-files-with-chosen-encoder-settings).
