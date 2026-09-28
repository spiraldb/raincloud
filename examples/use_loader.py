# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Worked example: the Raincloud loader API.

Demonstrates the `raincloud.load(slug)` flow end-to-end:

  * cheap, network-free metadata access (rows, columns, license, source URL)
  * format selection: automatic or an explicit format
  * materialization: .to_arrow(), .to_vortex(), .dataset() (any engine),
    .to_pandas()
  * configuration via env vars: RAINCLOUD_CACHE, RAINCLOUD_MIRROR,
    RAINCLOUD_OFFLINE
  * the typed exception hierarchy, all subclasses of RaincloudError and
    listed in raincloud/exceptions.py (UnknownSlug, UnknownColumn,
    FormatUnavailable, ArtifactNotFound, MirrorUnavailable, OfflineMiss,
    ChecksumMismatch, CorruptArtifact, UnsupportedType, MissingDependency,
    BuildToolingMissing, BuildFailed, CatalogError)

Run it:

    # Metadata-only demo against the packaged catalog (no network):
    python examples/use_loader.py

    # Pick a specific slug (any slug name in the catalog):
    python examples/use_loader.py --slug clickbench-hits

    # Full materialization demo. Reads never build, so the slug must be
    # prepared (`raincloud build SLUG`) or in a mirror:
    raincloud build uci-iris
    python examples/use_loader.py --slug uci-iris --materialize
    RAINCLOUD_MIRROR=file:///path/to/mirror \\
        python examples/use_loader.py --materialize

Install (raincloud is not on PyPI — install from GitHub):

    pip install "raincloud @ git+https://github.com/spiraldb/raincloud"           # loader only
    pip install "raincloud[pandas] @ git+https://github.com/spiraldb/raincloud"   # Dataset.to_pandas()
    pip install "raincloud[build] @ git+https://github.com/spiraldb/raincloud"    # `raincloud build SLUG`
"""
from __future__ import annotations

import argparse
import os
import sys

import raincloud


def show_metadata(slug: str) -> None:
    """Pure-metadata path: walks the packaged catalog, no I/O beyond it."""
    print(f"\n== metadata for {slug!r} ==")
    ds = raincloud.load(slug)  # chooses an available reader
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
    print("  .path() ->", ds.path(), "(data/cache or mirror)")
    if ds.format == "parquet":
        schema = ds.schema  # parquet footer read — cheap
        print(f"  .schema  : {len(schema)} fields")
    table = ds.to_arrow()
    print(f"  .to_arrow().num_rows : {table.num_rows}")

    # .to_vortex() opens the vortex artifact (requires the optional [vortex] extra).
    # Guarded for slugs with no Vortex file, or none prepared here.
    try:
        vf = ds.to_vortex()
        print(f"  .to_vortex()     : {type(vf).__name__}")
    except (raincloud.FormatUnavailable, raincloud.MissingDependency, raincloud.ArtifactNotFound) as e:
        print(f"  .to_vortex()     : skipped ({e})")

    # .dataset() is a lazy pyarrow Dataset of the loaded format; DuckDB,
    # Polars and pyarrow scan it with pushdown. Optional backends are guarded.
    try:
        d = ds.dataset()
        print(f"  .dataset() head  : {d.head(3).to_pylist()}")
    except raincloud.MissingDependency as e:
        print(f"  .dataset()       : skipped ({e})")
    try:
        df = ds.to_pandas()
        print(f"  .to_pandas() rows: {len(df)}")
    except raincloud.MissingDependency as e:
        print(f"  .to_pandas()     : skipped ({e})")


def show_format_override(slug: str) -> None:
    """Pick a format explicitly. Explicit requests never substitute another format."""
    print(f"\n== format override for {slug!r} ==")
    for fmt in ("vortex", "parquet"):
        try:
            ds = raincloud.load(slug, format=fmt)
            print(f"  format={fmt!r:>8} -> resolved as {ds.format!r}")
        except (raincloud.FormatUnavailable, raincloud.MissingDependency) as e:
            print(f"  format={fmt!r:>8} -> unavailable ({e})")


def show_error_handling() -> None:
    """Every loader error is a RaincloudError subclass."""
    print("\n== error handling ==")
    try:
        raincloud.load("definitely-not-a-real-slug")
    except raincloud.UnknownSlug as e:
        print(f"  UnknownSlug caught: {e}")

    # Offline mode never contacts the mirror.
    os.environ["RAINCLOUD_OFFLINE"] = "1"
    try:
        # Pick a slug that's unlikely to be cached locally.
        ds = raincloud.load(raincloud.slugs()[0])
        ds.path()
    except raincloud.OfflineMiss as e:
        print(f"  OfflineMiss caught: {e}")
    except raincloud.RaincloudError as e:
        # Any other RaincloudError is fine for the demo — it's the catch-all.
        print(f"  {type(e).__name__} caught: {e}")
    finally:
        del os.environ["RAINCLOUD_OFFLINE"]


def materialize_hint(error: raincloud.RaincloudError, slug: str) -> str:
    """What to do about a failed read; each RaincloudError subclass has its own remedy."""
    if isinstance(error, (raincloud.ArtifactNotFound, raincloud.OfflineMiss)):
        return (f"prepare it with `raincloud build {slug}` (needs raincloud[build]), "
                "or set RAINCLOUD_MIRROR to a mirror that has it")
    if isinstance(error, (raincloud.MirrorUnavailable, raincloud.ChecksumMismatch)):
        return "check RAINCLOUD_MIRROR: the mirror could not be read, or served other bytes than the catalog names"
    if isinstance(error, raincloud.CorruptArtifact):
        return f"the prepared file is damaged; rebuild it with `raincloud build {slug}`"
    if isinstance(error, (raincloud.UnsupportedType, raincloud.MissingDependency)):
        return "load another format (format='parquet' or 'arrow'), or install the missing reader"
    return "see the message above"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    p.add_argument("--slug", help="specific slug to load (default: catalog's first)")
    p.add_argument("--materialize", action="store_true",
                   help="exercise .to_arrow/.dataset/.to_pandas (needs a prepared slug or a mirror)")
    args = p.parse_args(argv)

    slugs = raincloud.slugs()
    slug = args.slug or slugs[0]
    print(f"raincloud {raincloud.__version__} — {len(slugs)} slugs in catalog")
    print(f"using slug: {slug}")

    show_metadata(slug)
    show_format_override(slug)
    if args.materialize:
        try:
            show_materialization(slug)
        except raincloud.RaincloudError as e:
            print(f"\n[materialize] skipped: {type(e).__name__}: {e}", file=sys.stderr)
            print(f"  hint: {materialize_hint(e, slug)}", file=sys.stderr)
    show_error_handling()
    return 0


if __name__ == "__main__":
    sys.exit(main())
