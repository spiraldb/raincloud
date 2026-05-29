# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""In-place pass: promote Parquet JSON-annotated string columns to VARIANT.

For each parquet under `outputs/v{schema_version}/<slug>/parquet/<slug>.parquet`:
    - Inspect the logical types of every leaf column.
    - If any column carries the JSON logical-type annotation, rewrite the
      parquet with those columns `CAST(... AS VARIANT)`.
    - Atomic replace via a sibling `.variant-tmp.parquet`.

VARIANT is a strict upgrade over JSON for whole-record string JSON payloads:
the shredded physical layout (`metadata`, `value`, `typed_value`) lets query
engines extract typed sub-fields without re-parsing the string for every row.

Idempotent: parquets whose JSON columns have already been promoted simply
skip the rewrite on re-run.

Two rewrite modes:
    single-shot (default) — `COPY (SELECT ... CAST(col AS VARIANT) ... FROM
        read_parquet) TO parquet` in one query. Fastest for flat records.
    chunked (`--chunked`)  — INSERT one source row group at a time into a
        persistent DuckDB table, then COPY to parquet. Bounded intermediate
        state per row group; required for pathologically wide / deeply-nested
        records (e.g. Open Food Facts) where the single-shot plan's spill
        blows past 1 TiB.

Usage:
    python -m scripts.pipeline.tighten_variant               # every built parquet
    python -m scripts.pipeline.tighten_variant <slug>...     # specific slugs
    python -m scripts.pipeline.tighten_variant --chunked     # force chunked mode
    python -m scripts.pipeline.tighten_variant --dry-run
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import pyarrow.parquet as pq

from .spec import display_path, duckdb_connect, outputs_root, prepared_parquet, workdir_root


def _json_columns(parquet: Path) -> list[str]:
    """Return distinct top-level column names whose physical leaves carry the
    JSON logical-type annotation."""
    pf = pq.ParquetFile(parquet)
    md = pf.metadata.schema
    out = set()
    for i in range(len(md.names)):
        col = md.column(i)
        lt = col.logical_type
        if lt is not None and "JSON" in str(lt):
            out.add(col.path.split(".", 1)[0])
    return sorted(out)


def _tighten_single_shot(parquet: Path, tmp: Path, replace_clause: str) -> None:
    """Single-query CAST + COPY. Works for flat-ish JSON records."""
    con = duckdb_connect(extra_config={
        "storage_compatibility_version": "v1.5.0",
        "preserve_insertion_order": "false",
    })
    try:
        con.execute(f"""
            COPY (
                SELECT * REPLACE ({replace_clause})
                FROM read_parquet('{parquet}')
            ) TO '{tmp}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """)
    finally:
        con.close()


def _tighten_chunked(parquet: Path, tmp: Path, replace_clause: str,
                     *, output_row_group_size: int = 50_000) -> None:
    """Row-group-at-a-time INSERT into a persistent DuckDB table, then COPY.

    Bounded intermediate state per chunk — scales to records too wide/nested
    for the single-shot plan to handle without massive spill. The persistent
    DB lives under `_workdir/<slug>/tighten_variant.db`.

    The final `COPY ... TO PARQUET` carries `ROW_GROUP_SIZE output_row_group_size`
    so DuckDB's parallel writer emits row groups incrementally instead of
    buffering one massive group per thread — this is what was OOMing /
    over-spilling on pathologically nested records (Open Food Facts) in
    earlier iterations.

    Intermediate files under `_workdir/<slug>/` are NOT auto-deleted; if an
    existing DB is found the function errors out so the caller can decide
    whether to preserve or remove it. Clean up manually after a successful
    build: `rm -rf _workdir/<slug>/`.
    """
    pf = pq.ParquetFile(parquet)
    n_rg = pf.metadata.num_row_groups

    workdir = workdir_root() / parquet.parent.name
    workdir.mkdir(parents=True, exist_ok=True)
    db_path = workdir / "tighten_variant.db"
    if db_path.exists():
        raise RuntimeError(
            f"{display_path(db_path)} already exists from a prior run. "
            f"Move or remove it manually before retrying — this function does "
            f"not auto-delete intermediates."
        )

    con = duckdb_connect(db_path, extra_config={"preserve_insertion_order": "false"})
    try:
        # Materialise the target schema once by running the CAST against an
        # empty slice. This gets us a zero-row table with VARIANT in the
        # right spots; we INSERT into it below.
        con.execute(f"""
            CREATE TABLE tightened AS
                SELECT * REPLACE ({replace_clause})
                FROM read_parquet('{parquet}')
                LIMIT 0
        """)

        for rg_i in range(n_rg):
            batch = pf.read_row_group(rg_i)
            con.register("rg_arrow", batch)
            con.execute(f"""
                INSERT INTO tightened
                    SELECT * REPLACE ({replace_clause}) FROM rg_arrow
            """)
            con.unregister("rg_arrow")
            print(f"      row group {rg_i+1}/{n_rg} inserted "
                  f"(+{batch.num_rows:,} rows)", flush=True)

        print(f"      final COPY (row_group_size={output_row_group_size:,})", flush=True)
        con.execute(f"""
            COPY tightened TO '{tmp}' (
                FORMAT PARQUET,
                COMPRESSION 'zstd',
                ROW_GROUP_SIZE {output_row_group_size}
            )
        """)
    finally:
        con.close()


def tighten_one(parquet: Path, *, dry_run: bool = False, chunked: bool = False) -> bool:
    """Returns True iff the parquet was (or would be) rewritten."""
    json_cols = _json_columns(parquet)
    if not json_cols:
        return False
    rel = display_path(parquet)
    mode = "chunked" if chunked else "single-shot"
    print(f"  {rel}: promote {json_cols} -> VARIANT ({mode})", flush=True)
    if dry_run:
        return True

    tmp = parquet.with_name(parquet.stem + ".variant-tmp.parquet")
    if tmp.exists(): tmp.unlink()

    replace_clause = ", ".join(f"CAST({c} AS VARIANT) AS {c}" for c in json_cols)
    t0 = time.monotonic()
    if chunked:
        _tighten_chunked(parquet, tmp, replace_clause)
    else:
        _tighten_single_shot(parquet, tmp, replace_clause)

    # Verify the new file round-trips and has the expected VARIANT columns.
    check = duckdb_connect()
    try:
        new_types = {r[0]: r[1] for r in check.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{tmp}')").fetchall()}
    finally:
        check.close()
    for c in json_cols:
        if new_types.get(c) != "VARIANT":
            tmp.unlink()
            raise RuntimeError(f"post-tighten check failed: {c} is {new_types.get(c)!r}, expected VARIANT")

    tmp.replace(parquet)
    elapsed = time.monotonic() - t0
    print(f"    -> {parquet.stat().st_size / 1e9:.2f} GB, {elapsed:.1f}s", flush=True)
    return True


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("slugs", nargs="*", help="Specific slugs to tighten")
    ap.add_argument("--chunked", action="store_true",
                    help="Force chunked mode (row-group at a time). "
                         "Needed for wide/deeply-nested records where single-shot spill blows up.")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    root = outputs_root()
    if args.slugs:
        candidates = []
        for s in args.slugs:
            p = prepared_parquet(s)
            if not p.exists():
                print(f"  skip {s}: no parquet at {display_path(p)}", file=sys.stderr)
                continue
            candidates.append(p)
    else:
        # Match parquets under the new <slug>/parquet/ format dir; rglob skips
        # any sibling format dirs (vortex/, parquet-hydrated/, ...).
        candidates = sorted(root.glob("*/parquet/*.parquet"))

    n_rewritten = n_already = 0
    for p in candidates:
        if tighten_one(p, dry_run=args.dry_run, chunked=args.chunked):
            n_rewritten += 1
        else:
            n_already += 1
    print(f"\nrewritten: {n_rewritten}   already-variant or no-json: {n_already}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
