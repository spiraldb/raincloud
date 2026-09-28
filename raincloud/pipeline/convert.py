# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Write a dataset's Vortex file with the Python writer (`vortex@py`).

Schema version 2: runs only where `vortex@py` is the writer a build would pick
for the dataset's Vortex file (`export.formats`, then the export priority with
this machine's configuration), and is the export stage for that one format --
`python -m raincloud.pipeline.export <slug> --format vortex` with a reuse
check, through the same `records.export_from_canonical`, so a failure is
recorded as the format's "unavailable" measurement and fails the slug, and a
failure already measured for this toolchain is skipped and fails the slug too
(`--retry-errors` attempts it anyway). It refuses a canonical
the export stage refuses, reuses a Vortex file the build record says `vortex@py`
made from the current recipe after the canonical was written, and records what
it writes. Schema version 1 converts prepared Parquet when `convert.vortex` is
true, reusing a Vortex file newer than the Parquet.

The module exists for the v1 layout; drop it when v1 catalogs are no longer
read, and use `export --format vortex@py` for v2.

    python -m raincloud.pipeline.convert <slug>...
    python -m raincloud.pipeline.convert --all
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from raincloud._cache import Publication

from .canonical import uniquify_names
from .lifecycle import entry_for, maintenance, require_source
from .selection import select_or_exit
from .spec import (
    display_path,
    load_manifest,
    prepared_arrow,
    prepared_parquet,
    prepared_vortex,
    spec_field,
)


def vortex_enabled(spec: dict) -> bool:
    """Whether this stage writes `spec`'s Vortex file (see the module docstring)."""
    from raincloud.catalogs import current
    manifest = current().manifest
    version = int(manifest["schema_version"])
    if version >= 2:
        from raincloud._formats import export_formats, export_priority, resolve_export_cell
        from raincloud.config import get_config

        from .export import cell_available
        if "vortex" not in export_formats(spec, version):
            return False
        # This stage is the Python Vortex writer: it runs only where that is the
        # writer a build here would pick (`export.plan`), or it would replace
        # another writer's file.
        priority = export_priority(spec, manifest, get_config(), fmt="vortex")
        return resolve_export_cell("vortex", priority, is_available=cell_available) == "vortex@py"
    # v1 catalogs only; remove when v1 bundles are no longer read.
    return bool(spec_field(spec, "convert.vortex", False))


def _convert_one(parquet: Path, vortex_path: Path, label: str) -> Path:
    """Read `parquet`, write `vortex_path`. Idempotent: returns immediately
    when the vortex file is newer than the parquet.

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
    new_names = uniquify_names(schema.names)
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
    try:
        vxio.write(reader, str(tmp))
        tmp.replace(vortex_path)
    finally:
        tmp.unlink(missing_ok=True)
        reader.close()
        pf.close()
    elapsed = time.monotonic() - t0

    sz_p = parquet.stat().st_size
    sz_v = vortex_path.stat().st_size
    log_path = display_path(vortex_path)
    print(
        f"  wrote {log_path}  "
        f"{sz_v / 1e6:.1f} MB (ratio {sz_v / sz_p:.3f}) in {elapsed:.1f}s"
    )
    return vortex_path


@maintenance()
def convert(spec: dict, *, retry_errors: bool = False) -> Path | None:
    """Write `spec`'s Vortex file (see the module docstring).

    Returns the output path when a conversion (or reuse) occurred, `None` when
    the version-specific export policy disables the Python cell. Raises when
    `records.check_canonical` refuses the canonical, the write fails, or (v2)
    the write is skipped as a failure already measured with this toolchain
    unless `retry_errors`; the previous file is then left in place.
    """
    if not vortex_enabled(spec):
        return None

    entry = entry_for(spec)
    out_slug = spec["slug"]
    dest = prepared_vortex(out_slug)
    if entry.version >= 2:
        from .records import check_canonical, export_from_canonical
        canonical = prepared_arrow(out_slug)
        require_source(canonical)
        status = check_canonical(canonical)
        if _reusable(dest, canonical, entry):
            print(f"[convert] {out_slug}  [cached] {dest.name}")
            return dest
        # The bare format: `vortex_enabled` has established that it resolves to
        # vortex@py, and a planned writer's failure is recorded like a build's.
        failed, skipped = [], []
        results = export_from_canonical(spec, canonical, ["vortex"], status=status,
                                        on_unavailable=lambda failure, recorded: failed.append(failure),
                                        on_skip=skipped.append, retry_errors=retry_errors)
        if failed:
            raise RuntimeError(f"vortex export failed: {failed[0].error}")
        if skipped:
            raise RuntimeError(f"vortex not exported: {skipped[0].cell} already failed with this toolchain "
                               f"(pass --retry-errors to try again)")
        return results[0].out_path
    parquet = prepared_parquet(out_slug)
    if not parquet.exists():
        raise FileNotFoundError(f"no parquet at {display_path(parquet)}")
    with Publication(dest) as publication:
        result = _convert_one(parquet, dest, out_slug)
        publication.accept()
    return result


def _reusable(dest: Path, canonical: Path, entry) -> bool:
    """A v2 Vortex file this install's build record says `vortex@py` wrote from
    the current recipe, after the canonical it reads was written.

    Stricter than `compliance._written_by`, which also adopts a catalog file of
    the catalog's size: compliance only reads that file, while reuse here stands
    in for the write, so it needs this install's record of the recipe it was made
    from.
    """
    from raincloud import _builds
    from raincloud._resolve import artifact_key

    from .spec import outputs_base
    if not dest.is_file() or dest.stat().st_mtime_ns < canonical.stat().st_mtime_ns:
        return False
    built = _builds.serves(outputs_base(), artifact_key(entry.slug, "vortex", entry.version),
                           dest.stat().st_size, entry.recipe)
    return built is not None and built.get("writer") == "py"


@maintenance()
def main(argv):
    ap = argparse.ArgumentParser(prog="python -m raincloud.pipeline.convert", allow_abbrev=False)
    ap.add_argument("slugs", nargs="*", help="specific slugs to convert")
    ap.add_argument("--all", action="store_true",
                    help="convert every dataset except hydrated ones, which are converted by name")
    ap.add_argument("--retry-errors", action="store_true",
                    help="(schema version 2) attempt the Vortex file even when vortex@py, with this "
                         "toolchain, already failed to write it at this recipe")
    args = ap.parse_args(argv)

    m = load_manifest()
    selected = select_or_exit(ap, m, args.slugs, all_=args.all, verb="convert")
    v2 = int(m["schema_version"]) >= 2
    source = "canonical" if v2 else "parquet"

    n_converted = n_no_opt_in = n_no_source = n_failed = 0
    named = not args.all
    for spec in selected:
        try:
            if not vortex_enabled(spec):
                n_no_opt_in += 1
                continue
            if not (prepared_arrow(spec["slug"]) if v2 else prepared_parquet(spec["slug"])).exists():
                n_no_source += 1
                if named:
                    print(f"  [no {source}] {spec['slug']} — build it first", file=sys.stderr)
                continue
            convert(spec, retry_errors=args.retry_errors)
            n_converted += 1
        except BaseException as e:
            # The v1 path runs Vortex in this process, and Vortex raises
            # `pyo3_runtime.PanicException` (a BaseException subclass) — a plain
            # `except Exception` misses it. Still honour KeyboardInterrupt / SystemExit.
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            msg = str(e).splitlines()[0] if str(e) else ""
            print(
                f"  [fail] {spec['slug']}: {type(e).__name__}: {msg}", file=sys.stderr
            )
            n_failed += 1

    print(
        f"\nconverted: {n_converted}   skipped-no-opt-in: {n_no_opt_in}   "
        f"skipped-no-{source}: {n_no_source}   failed: {n_failed}"
    )
    # A slug named outright that is not converted is a failed request, as in
    # `export`; under --all it is only not opted in, or not built.
    return 1 if n_failed or (named and (n_no_opt_in or n_no_source)) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
