# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Worked example: the Raincloud loader API.

Demonstrates the `raincloud.load(slug)` flow end-to-end:

  * cheap, network-free metadata access (rows, columns, license, source URL)
  * format selection: vortex (default) vs parquet
  * materialization: .to_arrow(), .to_vortex(), .scan() (DuckDB),
    .to_pandas()
  * configuration via env vars: RAINCLOUD_CACHE, RAINCLOUD_MIRROR,
    RAINCLOUD_OFFLINE, RAINCLOUD_STRICT_CHECKSUM (hard integrity gate)
  * the typed exception hierarchy (UnknownSlug, FormatUnavailable,
    OfflineMiss, BuildToolingMissing, ChecksumMismatch, MissingDependency)

Run it:

    # Metadata-only demo against the packaged catalog (no network):
    python examples/use_loader.py

    # Pick a specific slug (any slug name in the catalog):
    python examples/use_loader.py --slug clickbench-hits

    # Full materialization demo (needs RAINCLOUD_MIRROR or local cache hit):
    RAINCLOUD_MIRROR=file:///path/to/mirror \\
        python examples/use_loader.py --materialize

Install (raincloud is not on PyPI — install from GitHub):

    pip install "raincloud @ git+https://github.com/spiraldb/raincloud"           # loader only
    pip install "raincloud[duckdb] @ git+https://github.com/spiraldb/raincloud"   # Dataset.scan()
    pip install "raincloud[pandas] @ git+https://github.com/spiraldb/raincloud"   # Dataset.to_pandas()
    pip install "raincloud[build] @ git+https://github.com/spiraldb/raincloud"    # local-build fallback
"""
from __future__ import annotations

import argparse
import os
import sys

import raincloud


def show_metadata(slug: str) -> None:
    """Pure-metadata path: walks the packaged catalog, no I/O beyond it."""
    print(f"\n== metadata for {slug!r} ==")
    ds = raincloud.load(slug)  # default format='vortex' with parquet fallback
    print(f"  repr            : {ds!r}")
    print(f"  format          : {ds.format}")
    print(f"  num_rows        : {ds.num_rows}")
    print(f"  column count    : {len(ds.column_names)}")
    print(f"  first 5 columns : {ds.column_names[:5]}")
    info = ds.info
    print(f"  short_name      : {info.get('short_name')}")
    print(f"  license (SPDX)  : {(info.get('license') or {}).get('spdx')}")
    print(f"  source_url      : {info.get('source_url')}")


def show_materialization(slug: str) -> None:
    """The four materialization shapes. Resolves the artifact on first call."""
    print(f"\n== materialize {slug!r} ==")
    ds = raincloud.load(slug)
    print("  .path() ->", ds.path(), "(cache or mirror or local build)")
    if ds.format == "parquet":
        schema = ds.schema  # parquet footer read — cheap
        print(f"  .schema  : {len(schema)} fields")
    table = ds.to_arrow()
    print(f"  .to_arrow().num_rows : {table.num_rows}")

    # .to_vortex() opens the vortex artifact (vortex-data is a base dep).
    # Guarded for parquet-only slugs that have no vortex sibling.
    try:
        vf = ds.to_vortex()
        print(f"  .to_vortex()     : {type(vf).__name__}")
    except raincloud.FormatUnavailable as e:
        print(f"  .to_vortex()     : skipped ({e})")

    # Optional backends — guarded so the demo runs without [duckdb]/[pandas].
    try:
        rel = ds.scan()
        # `.scan()` always resolves the parquet sibling; if you loaded as
        # vortex, you'll see a [raincloud] note on stderr before resolution.
        head = rel.limit(3).fetchall()
        print(f"  .scan() head     : {head}")
    except raincloud.MissingDependency as e:
        print(f"  .scan()          : skipped ({e})")
    try:
        df = ds.to_pandas()
        print(f"  .to_pandas() rows: {len(df)}")
    except raincloud.MissingDependency as e:
        print(f"  .to_pandas()     : skipped ({e})")


def show_format_override(slug: str) -> None:
    """Pick a format explicitly. Falls back from vortex -> parquet automatically."""
    print(f"\n== format override for {slug!r} ==")
    for fmt in ("vortex", "parquet"):
        try:
            ds = raincloud.load(slug, format=fmt)
            print(f"  format={fmt!r:>8} -> resolved as {ds.format!r}")
        except raincloud.FormatUnavailable as e:
            print(f"  format={fmt!r:>8} -> unavailable ({e})")


def show_error_handling() -> None:
    """Every loader error is a RaincloudError subclass."""
    print("\n== error handling ==")
    try:
        raincloud.load("definitely-not-a-real-slug")
    except raincloud.UnknownSlug as e:
        print(f"  UnknownSlug caught: {e}")

    # Offline mode blocks mirror+build fallback.
    os.environ["RAINCLOUD_OFFLINE"] = "1"
    try:
        # Pick a slug that's unlikely to be cached locally.
        from raincloud._catalog import load_catalog
        load_catalog.cache_clear()
        ds = raincloud.load(next(iter(load_catalog().slugs())))
        ds.path()
    except raincloud.OfflineMiss as e:
        print(f"  OfflineMiss caught: {e}")
    except raincloud.RaincloudError as e:
        # Any other RaincloudError is fine for the demo — it's the catch-all.
        print(f"  {type(e).__name__} caught: {e}")
    finally:
        del os.environ["RAINCLOUD_OFFLINE"]
        from raincloud._catalog import load_catalog
        load_catalog.cache_clear()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--slug", help="specific slug to load (default: catalog's first)")
    p.add_argument("--materialize", action="store_true",
                   help="exercise .to_arrow/.scan/.to_pandas (needs cache or mirror)")
    args = p.parse_args(argv)

    from raincloud._catalog import load_catalog
    slugs = load_catalog().slugs()
    slug = args.slug or slugs[0]
    print(f"raincloud {raincloud.__version__} — {len(slugs)} slugs in catalog")
    print(f"using slug: {slug}")

    show_metadata(slug)
    show_format_override(slug)
    if args.materialize:
        try:
            show_materialization(slug)
        except (raincloud.OfflineMiss, raincloud.BuildToolingMissing) as e:
            print(f"\n[materialize] skipped: {type(e).__name__}: {e}")
            print('  hint: set RAINCLOUD_MIRROR=<url>, or pip install '
                  '"raincloud[build] @ git+https://github.com/spiraldb/raincloud"')
    show_error_handling()
    return 0


if __name__ == "__main__":
    sys.exit(main())
