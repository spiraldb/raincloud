# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Load and validate sources.json entries.

Each DatasetSpec is a plain dict — we don't wrap it in a class, but we
provide helpers for the common field accesses so scripts at each stage
fail early on missing/typo'd keys.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST = REPO_ROOT / "sources.json"


def load_manifest(path: Path | None = None) -> dict:
    p = Path(path) if path else DEFAULT_MANIFEST
    with open(p) as f:
        m = json.load(f)
    if m.get("schema_version") != 1:
        raise ValueError(f"unsupported schema_version in {p}: {m.get('schema_version')!r}")
    return m


def outputs_root(manifest: dict | None = None) -> Path:
    """Version-scoped output root: outputs/v{schema_version}/.

    All pipeline stages (fetch, write, docs) read the `schema_version` field
    of `sources.json` to resolve the layout. A manifest bump to v2 would
    land new builds under `outputs/v2/` while v1 artefacts remain intact.
    """
    m = manifest if manifest is not None else load_manifest()
    return REPO_ROOT / "outputs" / f"v{m['schema_version']}"


def output_format_dir(slug: str, fmt: str = "parquet",
                      manifest: dict | None = None) -> Path:
    """outputs/v{n}/<slug>/<fmt>/ — the format-scoped output directory.

    `fmt` is a free-form identifier describing the file layout: today
    "parquet" and "vortex"; future variants include "parquet-hydrated",
    "vortex-hydrated", or partitioned layouts (e.g. "parquet-by-date").
    The format dir lets one slug carry multiple representations of the
    same logical dataset without filename collisions.
    """
    return outputs_root(manifest) / slug / fmt


def prepared_parquet(slug: str, manifest: dict | None = None) -> Path:
    """outputs/v{n}/<slug>/parquet/<slug>.parquet — the canonical prepared parquet."""
    return output_format_dir(slug, "parquet", manifest) / f"{slug}.parquet"


def prepared_vortex(slug: str, manifest: dict | None = None) -> Path:
    """outputs/v{n}/<slug>/vortex/<slug>.vortex — the canonical converted vortex."""
    return output_format_dir(slug, "vortex", manifest) / f"{slug}.vortex"


def prepared_parquet_hydrated(slug: str, manifest: dict | None = None) -> Path:
    """outputs/v{n}/<slug>/parquet-hydrated/<slug>.parquet — URL-dereferenced
    companion parquet (only present for slugs with hydrate config + a hydrate
    run)."""
    return output_format_dir(slug, "parquet-hydrated", manifest) / f"{slug}.parquet"


def prepared_vortex_hydrated(slug: str, manifest: dict | None = None) -> Path:
    """outputs/v{n}/<slug>/vortex-hydrated/<slug>.vortex — Vortex companion
    of the hydrated parquet, produced by `python -m scripts.pipeline.convert`
    (or autorun by `python -m scripts.pipeline.hydrate`) when the slug has
    `convert.vortex = true` and a hydrated parquet on disk."""
    return output_format_dir(slug, "vortex-hydrated", manifest) / f"{slug}.vortex"


def iter_datasets(manifest: dict, *, family: str | None = None, slug: str | None = None) -> Iterator[dict]:
    for d in manifest["datasets"]:
        if family and d.get("family") != family: continue
        if slug and d.get("slug") != slug: continue
        yield d


def spec_field(spec: dict, dotted: str, default: Any = None) -> Any:
    """Safe nested getter — spec_field(spec, "fetch.urls", [])."""
    cur = spec
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def duckdb_connect(database: str | Path | None = None, *,
                   extra_config: dict | None = None):
    """Open a DuckDB connection with raincloud's environment-driven defaults.

    Env vars (all optional):
      RAINCLOUD_DUCKDB_MEMORY_LIMIT — e.g. "8GB", "512MB". Forwarded to
          DuckDB's `memory_limit` setting; DuckDB spills to disk once the
          working set exceeds this. If unset, DuckDB uses its default
          (~80% of system RAM), which can be a problem on shared hosts or
          CI runners.
      RAINCLOUD_DUCKDB_THREADS — int. Caps DuckDB's thread pool.
      RAINCLOUD_DUCKDB_TEMP_DIRECTORY — path. Where DuckDB writes spill
          files. Default is the system tempdir; override when the system
          tempdir doesn't have room for the working set (e.g. point at a
          larger volume).

    When `database` is a path, the connection is opened in persistent mode
    with `storage_compatibility_version=v1.5.0` (required for VARIANT
    columns in persistent DuckDB databases).

    `extra_config` merges on top of the env-derived config; explicit keys win.
    """
    import duckdb
    cfg: dict = {}
    if database is not None:
        cfg["storage_compatibility_version"] = "v1.5.0"
    mem = os.environ.get("RAINCLOUD_DUCKDB_MEMORY_LIMIT")
    if mem: cfg["memory_limit"] = mem
    thr = os.environ.get("RAINCLOUD_DUCKDB_THREADS")
    if thr: cfg["threads"] = thr
    tmp = os.environ.get("RAINCLOUD_DUCKDB_TEMP_DIRECTORY")
    if tmp: cfg["temp_directory"] = tmp
    if extra_config: cfg.update(extra_config)
    if database is None:
        return duckdb.connect(config=cfg) if cfg else duckdb.connect()
    return duckdb.connect(str(database), config=cfg)


def _json_safe(v: Any) -> Any:
    """Coerce a parquet stat value (min/max) to a JSON-encodable form.
    bytes → utf-8 string when valid; date/datetime → ISO format; bool/int/float/str
    pass through; anything else stringified."""
    import datetime as _dt
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return v.hex()
    if isinstance(v, (_dt.date, _dt.datetime, _dt.time)):
        return v.isoformat()
    return str(v)


def read_column_stats(parquet: Path) -> list[dict] | None:
    """Per-column metadata aggregated across row-groups. Returns one dict per
    top-level column with: name, type, length (compressed bytes), null_count,
    min, max. min/max are JSON-safe via `_json_safe`. None when the file is
    missing or unreadable.

    Aggregation: for multi-leaf columns (struct / list-of-struct), `length`
    is summed across leaves; null_count and min/max are dropped (stats live
    on leaves and aren't meaningfully aggregable for nested types).

    Used by both `browse.py` (live parquet stats) and `docs.py snapshot`
    (capture into the snapshot for TUI fallback when the parquet isn't local).
    """
    if not parquet.exists():
        return None
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(parquet)
        meta = pf.metadata
        arrow_schema = pf.schema_arrow
        phys = meta.schema
    except Exception:
        return None

    by_top: dict[str, list[int]] = {}
    for ci in range(len(phys.names)):
        top = phys.column(ci).path.split(".", 1)[0]
        by_top.setdefault(top, []).append(ci)

    out: list[dict] = []
    for field in arrow_schema:
        indices = by_top.get(field.name, [])
        length = 0
        for rg_i in range(meta.num_row_groups):
            for ci in indices:
                length += meta.row_group(rg_i).column(ci).total_compressed_size
        mn: Any = None
        mx: Any = None
        nulls: int | None = 0
        if len(indices) == 1:
            ci = indices[0]
            any_stats = False
            for rg_i in range(meta.num_row_groups):
                s = meta.row_group(rg_i).column(ci).statistics
                if s is None:
                    continue
                any_stats = True
                if s.has_min_max:
                    try:
                        if mn is None or s.min < mn:
                            mn = s.min
                    except (OverflowError, ValueError, TypeError):
                        pass
                    try:
                        if mx is None or s.max > mx:
                            mx = s.max
                    except (OverflowError, ValueError, TypeError):
                        pass
                if s.null_count is not None:
                    nulls = (nulls or 0) + s.null_count
            if not any_stats:
                nulls = None
        else:
            nulls = None  # nested types: stats live on leaves, not aggregable
        out.append({
            "name": field.name,
            "type": str(field.type),
            "length": length,
            "null_count": nulls,
            "min": _json_safe(mn),
            "max": _json_safe(mx),
        })
    return out
