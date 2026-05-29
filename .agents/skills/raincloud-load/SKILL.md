---
name: raincloud-load
description: Load a Raincloud dataset via the lightweight Python loader API (`raincloud.load(slug)`). Use when the user wants to read a prepared parquet/vortex artifact, inspect a slug's metadata, or pull a dataset for downstream analysis from cache → mirror → local build.
argument-hint: <slug> [--format vortex|parquet] [--materialize to_arrow|scan|to_pandas]
disable-model-invocation: true
allowed-tools: Bash(python -c *), Bash(python -m raincloud *), Bash(python examples/use_loader.py *)
---

The Raincloud loader is a separate, lightweight Python package (`raincloud`) for reading **already-prepared** artifacts. Resolution order is `local cache → mirror → local build`; nothing is fetched until you call an accessor.

## Most common shape

```bash
python -c "
import raincloud
ds = raincloud.load('$ARGUMENTS')   # default format='vortex' with parquet fallback
print('rows   :', ds.num_rows)
print('cols   :', ds.column_names[:5])
print('source :', ds.info.get('source_url'))
print('path   :', ds.path())        # triggers cache/mirror/build resolution
"
```

## Materialization

| Accessor | Returns | Notes |
|---|---|---|
| `ds.path()` | `pathlib.Path` to the on-disk artifact | First call resolves; subsequent calls are cache hits. |
| `ds.to_arrow()` | `pyarrow.Table` | Materializes the whole table; expensive on multi-GB slugs. |
| `ds.to_vortex()` | `vortex.VortexFile` (lazy) | Vortex-native handle. |
| `ds.scan()` | `duckdb.DuckDBPyRelation` | Requires `raincloud[duckdb]`. Always reads the parquet sibling — if the slug was loaded as vortex, you'll see a `[raincloud]` stderr note before the parquet is resolved. |
| `ds.to_pandas()` | `pandas.DataFrame` | Requires `raincloud[pandas]`. |
| `ds.schema` | `pyarrow.Schema` | Footer-only read for parquet; opens the file for vortex. |

## Config via env vars

| Env var | Effect |
|---|---|
| `RAINCLOUD_CACHE` | Local artifact cache dir (default `~/.cache/raincloud`). |
| `RAINCLOUD_MIRROR` | Mirror URL (`s3://…`, `https://…`, `file://…`). Tried before the local build. |
| `RAINCLOUD_OFFLINE=1` | Block mirror + build; raise `OfflineMiss` on cache miss. |
| `RAINCLOUD_SNAPSHOT` | Override `docs/v1/snapshot.json` (catalog). |
| `RAINCLOUD_MANIFEST` | Override `sources.json`. |

## Drift semantics

When a slug has a `sha256` pinned in the snapshot and the mirror or local build produces bytes that disagree, the loader prints a `[raincloud] WARN: <slug> from <mirror|build> sha256 drifted ...` to stderr and adopts the new bytes anyway. Upstream content drifts; that's not a panic case. Catch + escalate via `raincloud.ChecksumMismatch` only if you want a hard gate (e.g. `_cache.adopt(..., strict=True)` — used by `python -m scripts.pipeline.publish`).

## Errors (all subclass `raincloud.RaincloudError`)

`UnknownSlug`, `FormatUnavailable`, `ArtifactNotFound`, `OfflineMiss`, `BuildToolingMissing`, `MissingDependency`, `ChecksumMismatch`.

## Worked example

```bash
python examples/use_loader.py --slug $ARGUMENTS              # metadata only
python examples/use_loader.py --slug $ARGUMENTS --materialize  # full path
```

[`examples/use_loader.py`](../../../examples/use_loader.py) walks the full API end-to-end against the packaged catalog with no network.

## Install tiers

```bash
pip install raincloud                # lightweight loader
pip install 'raincloud[duckdb]'      # adds .scan()
pip install 'raincloud[pandas]'      # adds .to_pandas()
pip install 'raincloud[s3]'          # s3:// mirror transport
pip install 'raincloud[http]'        # https:// mirror transport
pip install 'raincloud[build]'       # adds local-build fallback (heavyweight)
```

Context: [AGENTS.md "The loader package"](../../../AGENTS.md), [`examples/use_loader.py`](../../../examples/use_loader.py).
