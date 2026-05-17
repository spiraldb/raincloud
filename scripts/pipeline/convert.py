# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 7 (optional) — convert prepared parquets to Vortex format.

For any DatasetSpec with `convert.vortex = true`, read the prepared parquet
at `outputs/v{n}/<slug>/parquet/<slug>.parquet` and write the converted
sibling at `outputs/v{n}/<slug>/vortex/<slug>.vortex`. The stage is a
no-op for specs without that flag.

When a slug has a hydrated companion at
`outputs/v{n}/<slug>/parquet-hydrated/<slug>.parquet` (produced by the
hydrate stage) AND `convert.vortex = true`, this stage ALSO writes
`outputs/v{n}/<slug>/vortex-hydrated/<slug>.vortex`. The two pairs are
governed by the same `convert.vortex` flag — converting the base implies
converting the hydrated companion.

Idempotent: skips when the `.vortex` is newer than the `.parquet` (i.e. the
source hasn't changed since the last convert).

Vortex (https://github.com/spiraldb/vortex) is a columnar file format built
around Apache Arrow's type system, with richer compression and pushdown than
Parquet. Conversion streams `pf.iter_batches() → pa.RecordBatchReader →
vxio.write`, so Arrow-supported types round-trip cleanly without ever
materialising the full table. Types the current Vortex release doesn't
accept (e.g. any `FixedSizeBinary` in 0.69) raise at `vortex.Array.from_arrow`;
this module reports the failure and continues with the next slug when run
in `--all` mode.

Caveats:
  - Parquet VARIANT columns surface in Vortex as their shredded struct
    (`struct<metadata: binary, value: binary, typed_value: ...>`) — the
    VARIANT logical annotation isn't currently preserved on the round-trip.
  - Vortex file sizes are not always smaller than zstd-compressed Parquet,
    especially on tiny datasets where per-file metadata dominates.
  - Per-slug opt-outs (slugs that hit a known type-support gap and ship
    with `convert.vortex = false`) are catalogued in
    `docs/v1/vortex_skip.md`, generated from `convert.vortex_skip_reason`
    in the manifest.

Usage:
    python -m scripts.pipeline.convert                # every slug with convert.vortex
    python -m scripts.pipeline.convert <slug>...
    python -m scripts.pipeline.convert --all
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .spec import (
    REPO_ROOT,
    iter_datasets,
    load_manifest,
    prepared_parquet,
    prepared_parquet_hydrated,
    prepared_vortex,
    prepared_vortex_hydrated,
    spec_field,
)


def _uniquify_names(names: list[str]) -> list[str] | None:
    """Disambiguate duplicate top-level column names by suffixing ` [N]`.

    Vortex's StructLayout rejects duplicates (e.g. the OSMI survey parquets
    have repeated `Anxiety Disorder` headers from matrix questions).

    Returns the rewritten list, or None if `names` is already unique.
    """
    if len(set(names)) == len(names):
        return None
    counts: dict[str, int] = {}
    out: list[str] = []
    for n in names:
        if n in counts:
            counts[n] += 1
            out.append(f"{n} [{counts[n]}]")
        else:
            counts[n] = 0
            out.append(n)
    return out


def _convert_one(parquet: Path, vortex_path: Path, label: str) -> Path:
    """Read `parquet`, write `vortex_path`. Idempotent: returns immediately
    when the vortex file is newer than the parquet. Used for both the base
    parquet (vortex/) and the hydrated companion (vortex-hydrated/).

    Streams the parquet via `iter_batches` rather than `read()` so we never
    ask pyarrow to materialise a single Arrow array large enough to need
    chunked output for a nested column — that path is unimplemented in
    pyarrow and raises `ArrowNotImplementedError: Nested data conversions
    not implemented for chunked array outputs` for parquets with sizeable
    nested fields (list/struct).
    """
    vortex_path.parent.mkdir(parents=True, exist_ok=True)
    if vortex_path.exists() and vortex_path.stat().st_mtime >= parquet.stat().st_mtime:
        print(f"[convert] {label}  [cached] {vortex_path.name}")
        return vortex_path

    print(f"[convert] {label}  parquet -> vortex")
    import vortex.io as vxio

    tmp = vortex_path.with_suffix(".vortex.tmp")
    if tmp.exists():
        tmp.unlink()

    t0 = time.monotonic()
    pf = pq.ParquetFile(str(parquet))
    schema = pf.schema_arrow
    new_names = _uniquify_names(schema.names)
    if new_names is not None:
        schema = pa.schema(
            [f.with_name(n) for f, n in zip(schema, new_names)],
            metadata=schema.metadata,
        )

    # Smaller than pyarrow's 65536 default: with large nested cells (audio
    # bytes, list<struct<string,string>>), 65536 rows can build a per-column
    # buffer past i32-offset limits and trigger pyarrow's chunked-output
    # NotImplementedError in the C-stream export. 1024 keeps batches under
    # that ceiling for every slug we currently ship; the per-batch overhead
    # is negligible vs. the parquet-decode and vortex-encode costs.
    BATCH_SIZE = 1024

    def batches():
        for b in pf.iter_batches(batch_size=BATCH_SIZE):
            yield b.rename_columns(new_names) if new_names is not None else b

    reader = pa.RecordBatchReader.from_batches(schema, batches())
    vxio.write(reader, str(tmp))
    tmp.replace(vortex_path)
    elapsed = time.monotonic() - t0

    sz_p = parquet.stat().st_size
    sz_v = vortex_path.stat().st_size
    try:
        log_path = vortex_path.relative_to(REPO_ROOT)
    except ValueError:
        log_path = vortex_path
    print(
        f"  wrote {log_path}  "
        f"{sz_v / 1e6:.1f} MB (ratio {sz_v / sz_p:.3f}) in {elapsed:.1f}s"
    )
    return vortex_path


def convert(spec: dict) -> Path | None:
    """Convert `spec`'s prepared parquet into a sibling `.vortex` file.

    Returns the output path when a conversion (or cache hit) occurred,
    `None` when the spec doesn't opt in via `convert.vortex = true`.
    """
    if not spec_field(spec, "convert.vortex", False):
        return None

    out_slug = spec["slug"]
    parquet = prepared_parquet(out_slug)
    if not parquet.exists():
        raise FileNotFoundError(f"no parquet at {parquet.relative_to(REPO_ROOT)}")

    return _convert_one(parquet, prepared_vortex(out_slug), out_slug)


def convert_hydrated(spec: dict) -> Path | None:
    """Convert `spec`'s hydrated companion parquet (if one exists on disk)
    into a sibling `.vortex` file under `vortex-hydrated/`.

    Returns the output path on conversion / cache hit, `None` when the spec
    doesn't opt in via `convert.vortex = true`, has no `hydrate` block, or
    has no hydrated parquet built yet.
    """
    if not spec_field(spec, "convert.vortex", False):
        return None
    if not spec.get("hydrate"):
        return None
    parquet = prepared_parquet_hydrated(spec["slug"])
    if not parquet.exists():
        # Hydrated parquet hasn't been produced yet — that's a separate
        # `python -m scripts.pipeline.hydrate <slug>` run, not an error.
        return None
    label = f"{spec['slug']} (hydrated)"
    return _convert_one(parquet, prepared_vortex_hydrated(spec["slug"]), label)


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("slugs", nargs="*", help="specific slugs to convert")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args(argv)

    m = load_manifest()
    selected: list[dict] = []
    if args.slugs:
        for s in args.slugs:
            selected += list(iter_datasets(m, slug=s))
    if args.all:
        selected = list(iter_datasets(m))
    if not selected:
        print("nothing selected; pass slugs or --all", file=sys.stderr)
        return 2

    n_converted = n_no_opt_in = n_no_parquet = n_failed = 0
    n_hydrated_converted = n_hydrated_skipped = n_hydrated_failed = 0
    for spec in selected:
        if not spec_field(spec, "convert.vortex", False):
            n_no_opt_in += 1
            continue
        parquet = prepared_parquet(spec["slug"])
        if not parquet.exists():
            n_no_parquet += 1
            continue
        try:
            convert(spec)
            n_converted += 1
        except BaseException as e:
            # Vortex raises `pyo3_runtime.PanicException` (a BaseException
            # subclass) on unsupported Arrow types — a plain `except Exception`
            # misses it. Still honour KeyboardInterrupt / SystemExit.
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            msg = str(e).splitlines()[0] if str(e) else ""
            print(
                f"  [fail] {spec['slug']}: {type(e).__name__}: {msg}", file=sys.stderr
            )
            n_failed += 1

        # Convert hydrated companion if it exists.
        if not spec.get("hydrate"):
            continue
        if not prepared_parquet_hydrated(spec["slug"]).exists():
            n_hydrated_skipped += 1
            continue
        try:
            convert_hydrated(spec)
            n_hydrated_converted += 1
        except BaseException as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            msg = str(e).splitlines()[0] if str(e) else ""
            print(
                f"  [fail] {spec['slug']} (hydrated): {type(e).__name__}: {msg}",
                file=sys.stderr,
            )
            n_hydrated_failed += 1

    print(
        f"\nconverted: {n_converted}   skipped-no-opt-in: {n_no_opt_in}   "
        f"skipped-no-parquet: {n_no_parquet}   failed: {n_failed}"
    )
    if n_hydrated_converted or n_hydrated_failed or n_hydrated_skipped:
        print(
            f"hydrated: converted={n_hydrated_converted}  "
            f"skipped-no-hydrated-parquet={n_hydrated_skipped}  "
            f"failed={n_hydrated_failed}"
        )
    return 0 if (n_failed == 0 and n_hydrated_failed == 0) else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
