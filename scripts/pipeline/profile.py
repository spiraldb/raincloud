# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Per-column profile stage.

Off the default build path. Run explicitly:

    python -m scripts.pipeline.profile <slug>...
    python -m scripts.pipeline.profile --all
    python -m scripts.pipeline.profile --sample-rows 1000000 <slug>
    python -m scripts.pipeline.profile <slug> --no-promote

Reads outputs/v1/<slug>/parquet/<slug>.parquet via spec.duckdb_connect,
writes outputs/v1/<slug>/profile.json. Idempotent against the parquet's
SHA-256: a matching profile.json is reused without recomputation.

After a successful per-slug loop, the stage auto-runs
`scripts.pipeline.promote_profiles.promote(...)` so the tracked mirror at
`docs/v{n}/profiles/<slug>.json` stays in sync without a manual second step.
Pass `--no-promote` to suppress that step while iterating.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from .discovery import _is_variant_field
from .spec import (
    duckdb_connect,
    iter_datasets,
    load_manifest,
    outputs_root,
    prepared_parquet,
)


_PROFILE_SCHEMA_VERSION = 1
_HISTOGRAM_BUCKETS = 10
_TOPK = 5
_TOPK_NDV_CEILING = 256


def _quote_ident(name: str) -> str:
    """Quote a SQL identifier, escaping any embedded double-quotes."""
    return '"' + name.replace('"', '""') + '"'


def _file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _arrow_dtype_label(arrow_type) -> str:
    """Human-readable dtype label persisted into profile.json."""
    import pyarrow as pa
    if pa.types.is_signed_integer(arrow_type) or pa.types.is_unsigned_integer(arrow_type):
        return str(arrow_type)
    if pa.types.is_floating(arrow_type):
        # pyarrow repr is "half"/"float"/"double" — normalise to bit-width tag
        # so downstream consumers (and the profile.schema.json dtype regex
        # ^(int|uint|float|decimal)) recognise the column as numeric.
        bits = arrow_type.bit_width
        return f"float{bits}"
    if pa.types.is_decimal(arrow_type):
        return str(arrow_type)
    if pa.types.is_boolean(arrow_type):
        return "bool"
    if pa.types.is_string(arrow_type):
        return "string"
    if pa.types.is_large_string(arrow_type):
        return "large_string"
    if pa.types.is_binary(arrow_type):
        return "binary"
    if pa.types.is_large_binary(arrow_type):
        return "large_binary"
    if pa.types.is_date(arrow_type):
        return f"date{32 if pa.types.is_date32(arrow_type) else 64}"
    if pa.types.is_timestamp(arrow_type):
        return f"timestamp[{arrow_type.unit}]"
    if pa.types.is_time(arrow_type):
        return "time"
    if pa.types.is_list(arrow_type):
        return f"list<{_arrow_dtype_label(arrow_type.value_type)}>"
    if pa.types.is_large_list(arrow_type):
        return f"large_list<{_arrow_dtype_label(arrow_type.value_type)}>"
    if pa.types.is_fixed_size_list(arrow_type):
        return f"fixed_size_list<{_arrow_dtype_label(arrow_type.value_type)}>[{arrow_type.list_size}]"
    if pa.types.is_map(arrow_type):
        return "map"
    if pa.types.is_struct(arrow_type):
        return "struct"
    return str(arrow_type)


def _numeric_profile(con, table_name: str, col: str, row_count: int) -> dict | None:
    """Numeric column profile: min/max/mean/null/NDV + equi-width histogram.

    Returns None for all-null columns (lo and hi both null) — the dispatcher
    propagates that as `null` at the column-map level, matching the existing
    struct/variant convention and keeping profile.schema.json happy
    (NumericProfile.mean requires a number).
    """
    quoted = _quote_ident(col)
    stats = con.execute(f"""
        SELECT
            min({quoted})                 AS lo,
            max({quoted})                 AS hi,
            avg(CAST({quoted} AS DOUBLE)) AS mean,
            count(*) - count({quoted})    AS nulls,
            approx_count_distinct({quoted}) AS ndv
        FROM {table_name}
    """).fetchone()
    lo, hi, mean, nulls, ndv = stats
    if lo is None and hi is None:
        # All-null column — emit null at the column-map level for schema validity.
        return None
    if lo == hi:
        # Degenerate: single distinct value. Skip histogram.
        return {
            "dtype": None,   # filled in by caller
            "null_count": int(nulls),
            "min": lo,
            "max": hi,
            "mean": float(mean) if mean is not None else None,
            "ndv_approx": int(ndv),
            "histogram": {"buckets": [lo, lo], "counts": [row_count - int(nulls)]},
        }

    # Equi-width histogram. DuckDB lacks `width_bucket`, so emulate via floor:
    #   bucket = CASE x = hi THEN N ELSE floor((x - lo) / (hi - lo) * N) + 1 END
    # The CASE pins the right-edge (x == hi) into the last bucket; floor()
    # alone would push it into bucket N+1.
    #
    # Every numeric literal is `::DOUBLE`-cast so DuckDB doesn't infer
    # DECIMAL types from the inline Python repr — a 14-digit `lo_f` like
    # `0.26851799179226266` gets bound as DECIMAL(18,17) by default, and
    # then `(value - lo) * 10` overflows DECIMAL(18,17)'s 1-digit integer
    # part for any large-magnitude column.
    lo_f = float(lo)
    hi_f = float(hi)
    bounds = con.execute(f"""
        WITH bucketed AS (
            SELECT CASE
                WHEN CAST({quoted} AS DOUBLE) = {hi_f}::DOUBLE THEN {_HISTOGRAM_BUCKETS}
                ELSE CAST(floor((CAST({quoted} AS DOUBLE) - {lo_f}::DOUBLE) /
                                ({hi_f}::DOUBLE - {lo_f}::DOUBLE) * {_HISTOGRAM_BUCKETS}::DOUBLE) AS INTEGER) + 1
            END AS b
            FROM {table_name}
            WHERE {quoted} IS NOT NULL
        )
        SELECT b, count(*) FROM bucketed GROUP BY b ORDER BY b
    """).fetchall()
    counts = [0] * _HISTOGRAM_BUCKETS
    for b, c in bounds:
        if b is None:
            continue
        idx = min(max(int(b) - 1, 0), _HISTOGRAM_BUCKETS - 1)
        counts[idx] += int(c)

    step = (float(hi) - float(lo)) / _HISTOGRAM_BUCKETS
    buckets = [float(lo) + step * i for i in range(_HISTOGRAM_BUCKETS + 1)]
    return {
        "null_count": int(nulls),
        "min": lo,
        "max": hi,
        "mean": float(mean) if mean is not None else None,
        "ndv_approx": int(ndv),
        "histogram": {"buckets": buckets, "counts": counts},
    }


def _bool_profile(con, table_name: str, col: str) -> dict:
    quoted = _quote_ident(col)
    t, f, n = con.execute(f"""
        SELECT
            sum(CASE WHEN {quoted} THEN 1 ELSE 0 END),
            sum(CASE WHEN {quoted} = false THEN 1 ELSE 0 END),
            sum(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END)
        FROM {table_name}
    """).fetchone()
    return {
        "dtype": "bool",
        "true_count": int(t or 0),
        "false_count": int(f or 0),
        "null_count": int(n or 0),
    }


def _temporal_profile(con, table_name: str, col: str, row_count: int, dtype: str) -> dict | None:
    """Temporal column profile (date / time / timestamp).

    Returns None for all-null columns (TemporalProfile.min/max in
    profile.schema.json require strings — emitting null at the column-map
    level is the schema-conformant signal for "not profiled / no data").

    All temporal strings (min, max, histogram bucket edges) use the same
    `datetime.fromtimestamp(ms/1000, tz=utc).isoformat()` form for a
    uniform `YYYY-MM-DDTHH:MM:SS+00:00` representation.
    """
    quoted = _quote_ident(col)
    lo_ms, hi_ms, nulls = con.execute(f"""
        SELECT epoch_ms(CAST(min({quoted}) AS TIMESTAMP)),
               epoch_ms(CAST(max({quoted}) AS TIMESTAMP)),
               count(*) - count({quoted})
        FROM {table_name}
    """).fetchone()
    if lo_ms is None and hi_ms is None:
        # All-null column — emit null at the column-map level for schema validity.
        return None

    def _iso(ms: int) -> str:
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()

    if lo_ms == hi_ms:
        # Single distinct timestamp value: skip histogram bucketing.
        edge = _iso(lo_ms)
        return {
            "dtype": dtype,
            "null_count": int(nulls),
            "min": edge,
            "max": edge,
            "histogram": {"buckets": [edge, edge], "counts": [row_count - int(nulls)]},
        }
    # Bucket the rows by floor-arithmetic (DuckDB lacks `width_bucket`).
    rows = con.execute(f"""
        WITH ts AS (
            SELECT epoch_ms(CAST({quoted} AS TIMESTAMP)) AS ms
            FROM {table_name}
            WHERE {quoted} IS NOT NULL
        )
        SELECT CASE
            WHEN ms = {hi_ms} THEN {_HISTOGRAM_BUCKETS}
            ELSE CAST(floor((ms - {lo_ms}) * 1.0 /
                            ({hi_ms} - {lo_ms}) * {_HISTOGRAM_BUCKETS}) AS INTEGER) + 1
        END AS b, count(*)
        FROM ts GROUP BY b ORDER BY b
    """).fetchall()
    counts = [0] * _HISTOGRAM_BUCKETS
    for b, c in rows:
        if b is None:
            continue
        idx = min(max(int(b) - 1, 0), _HISTOGRAM_BUCKETS - 1)
        counts[idx] += int(c)
    # Bucket edges as ISO strings (uniform with min/max above).
    edges_ms = con.execute(f"""
        SELECT {lo_ms} + ({hi_ms} - {lo_ms}) * gen / {_HISTOGRAM_BUCKETS}
        FROM range(0, {_HISTOGRAM_BUCKETS + 1}) AS t(gen)
    """).fetchall()
    buckets = [_iso(ms) for (ms,) in edges_ms]
    return {
        "dtype": dtype,
        "null_count": int(nulls),
        "min": _iso(lo_ms),
        "max": _iso(hi_ms),
        "histogram": {"buckets": buckets, "counts": counts},
    }


def _string_profile(con, table_name: str, col: str, include_topk: bool = True,
                    length_expr: str = "length({col})") -> dict | None:
    """String/binary column profile.

    `length_expr` is a SQL expression template that returns an integer length
    for one row of column `{col}`. Callers pick the right form for the column
    type, because DuckDB ≥ 1.5 has non-overlapping signatures:

      - VARCHAR / LARGE_STRING → `length({col})` (character count)
      - BLOB / LARGE_BINARY    → `octet_length({col})` (byte count)
      - GEOMETRY (GeoParquet)  → `octet_length(ST_AsWKB({col}))` (byte count)

    `include_topk=False` suppresses the top-K query — used for binary columns
    where bytes literals don't render as useful "top values" text regardless
    of NDV.
    """
    quoted = _quote_ident(col)
    length_sql = length_expr.format(col=quoted)
    n_total, n_null, ndv, mean_len = con.execute(f"""
        SELECT
            count(*),
            count(*) - count({quoted}),
            approx_count_distinct({quoted}),
            avg({length_sql})
        FROM {table_name}
    """).fetchone()
    # All-null → return None at column-map level (matches numeric/temporal convention).
    if n_total > 0 and n_null == n_total:
        return None
    out = {
        "null_count": int(n_null),
        "ndv_approx": int(ndv),
        "mean_length": float(mean_len) if mean_len is not None else None,
        "top_values": None,
    }
    if include_topk and 0 < int(ndv) <= _TOPK_NDV_CEILING:
        rows = con.execute(f"""
            SELECT {quoted} AS v, count(*) AS c
            FROM {table_name}
            WHERE {quoted} IS NOT NULL
            GROUP BY v
            ORDER BY c DESC
            LIMIT {_TOPK}
        """).fetchall()
        out["top_values"] = [
            {"value": (v if isinstance(v, (str, type(None))) else str(v)),
             "count": int(c)} for v, c in rows
        ]
    return out


def _list_profile(con, table_name: str, col: str,
                  length_fn: str = "len") -> dict | None:
    """List/map column profile.

    `length_fn` selects the DuckDB length function appropriate to the column's
    type: `len` (alias `length` / `array_length`) for LIST, `cardinality` for
    MAP. In DuckDB ≥ 1.5 these signatures are non-overlapping — `len(MAP)`
    raises BinderException and `cardinality(LIST)` raises "Cardinality can
    only operate on MAPs" — so the caller must pass the right one.
    """
    quoted = _quote_ident(col)
    n_total, nulls, lmin, lmax, lmean = con.execute(f"""
        SELECT
            count(*),
            count(*) - count({quoted}),
            min({length_fn}({quoted})),
            max({length_fn}({quoted})),
            avg({length_fn}({quoted}))
        FROM {table_name}
    """).fetchone()
    if n_total > 0 and nulls == n_total:
        return None
    return {
        "null_count": int(nulls),
        "length_min": int(lmin if lmin is not None else 0),
        "length_max": int(lmax if lmax is not None else 0),
        "length_mean": float(lmean) if lmean is not None else 0.0,
    }


def _column_profile(con, table_name: str, field, row_count: int,
                    duckdb_type: str | None = None) -> dict | None:
    """Dispatch on pyarrow dtype. Returns None for struct/variant (caller writes null).

    `duckdb_type` is DuckDB's reported SQL type for this column. Used to detect
    cases where DuckDB has reinterpreted a parquet binary column as an extension
    type (today: GEOMETRY, from GeoParquet metadata + auto-loaded `spatial`),
    and route the length expression accordingly.
    """
    import pyarrow as pa
    t = field.type
    dtype_label = _arrow_dtype_label(t)

    if pa.types.is_signed_integer(t) or pa.types.is_unsigned_integer(t) \
            or pa.types.is_floating(t) or pa.types.is_decimal(t):
        p = _numeric_profile(con, table_name, field.name, row_count)
        if p is None:
            return None
        p["dtype"] = dtype_label
        return p
    if pa.types.is_boolean(t):
        return _bool_profile(con, table_name, field.name)
    if pa.types.is_date(t) or pa.types.is_timestamp(t):
        return _temporal_profile(con, table_name, field.name, row_count, dtype_label)
    if pa.types.is_time(t):
        # TIME-of-day can't be epoch-cast in DuckDB (no `CAST(time AS TIMESTAMP)`
        # implementation). Fall through to the string profile for null_count
        # + NDV + top-K of the rendered HH:MM:SS form.
        p = _string_profile(con, table_name, field.name,
                            include_topk=True, length_expr="length(CAST({col} AS VARCHAR))")
        if p is not None:
            p["dtype"] = dtype_label
        return p
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        p = _string_profile(con, table_name, field.name,
                            include_topk=True, length_expr="length({col})")
        if p is None:
            return None
        p["dtype"] = dtype_label
        return p
    if pa.types.is_binary(t) or pa.types.is_large_binary(t):
        # BLOB columns: `length()` is undefined on BLOB in DuckDB ≥ 1.5, so use
        # `octet_length` for byte length. Suppress top_values because bytes
        # literals don't render as useful text strings.
        #
        # Special case: GeoParquet binary columns get reinterpreted as GEOMETRY
        # by the auto-loaded `spatial` extension, and `octet_length(GEOMETRY)`
        # doesn't bind. Wrap with ST_AsWKB to round-trip to BLOB.
        if duckdb_type and duckdb_type.upper() == "GEOMETRY":
            length_expr = "octet_length(ST_AsWKB({col}))"
        else:
            length_expr = "octet_length({col})"
        p = _string_profile(con, table_name, field.name,
                            include_topk=False, length_expr=length_expr)
        if p is None:
            return None
        p["dtype"] = dtype_label
        return p
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        p = _list_profile(con, table_name, field.name, length_fn="len")
        if p is None:
            return None
        p["dtype"] = dtype_label
        return p
    if pa.types.is_fixed_size_list(t):
        # Fixed-size lists have a constant element count = list_size. DuckDB's
        # `len()` still works on them, but recording the constant directly
        # avoids a SQL pass and is correct by construction.
        quoted = _quote_ident(field.name)
        n_total, nulls = con.execute(
            f"SELECT count(*), count(*) - count({quoted}) FROM {table_name}"
        ).fetchone()
        if n_total > 0 and nulls == n_total:
            return None
        size = int(t.list_size)
        return {
            "dtype": dtype_label,
            "null_count": int(nulls),
            "length_min": size,
            "length_max": size,
            "length_mean": float(size),
        }
    if pa.types.is_map(t):
        # MAP columns: `len()` is undefined on MAP in DuckDB ≥ 1.5, so use
        # `cardinality` (entry count).
        p = _list_profile(con, table_name, field.name, length_fn="cardinality")
        if p is None:
            return None
        p["dtype"] = "map"
        return p
    if pa.types.is_struct(t) or _is_variant_field(field):
        return None
    # Fallback for unknown types: column-map None (not profiled).
    return None


def profile_slug(*, slug: str, parquet_path: Path, sample_rows: int | None = None) -> dict:
    """Compute a full profile for one slug. Pure: no filesystem writes."""
    sha = _file_sha256(parquet_path)

    schema = pq.read_schema(parquet_path)
    row_count = pq.ParquetFile(parquet_path).metadata.num_rows

    src = str(parquet_path).replace("'", "''")
    if sample_rows and row_count > sample_rows:
        # Reservoir sampling via DuckDB.
        view_sql = f"SELECT * FROM read_parquet('{src}') USING SAMPLE {sample_rows} ROWS"
    else:
        view_sql = f"SELECT * FROM read_parquet('{src}')"

    columns: dict[str, dict | None] = {}
    with duckdb_connect() as con:
        con.execute(f"CREATE OR REPLACE TEMP VIEW _profile_src AS {view_sql}")
        eff_rows = con.execute("SELECT count(*) FROM _profile_src").fetchone()[0]
        # DuckDB may re-interpret some columns via auto-loaded extensions
        # (e.g. GeoParquet binary → GEOMETRY under `spatial`). Capture the
        # SQL-level type so `_column_profile` can pick the right expression.
        # PRAGMA table_info returns (cid, name, type, notnull, dflt, pk).
        duckdb_types = {
            row[1]: row[2]
            for row in con.execute("PRAGMA table_info(_profile_src)").fetchall()
        }
        for field in schema:
            # Skip empty-named columns — some upstream CSVs ship an unnamed
            # pandas-index column whose Arrow field has `name == ""`, and
            # DuckDB rejects `""` as a zero-length delimited identifier.
            if not field.name:
                columns["__unnamed_column__"] = {"dtype": str(field.type),
                                                  "skipped": "empty column name"}
                continue
            columns[field.name] = _column_profile(
                con, "_profile_src", field, eff_rows,
                duckdb_type=duckdb_types.get(field.name),
            )

    return {
        "schema_version": _PROFILE_SCHEMA_VERSION,
        "slug": slug,
        "row_count": int(row_count),
        "parquet_sha256": sha,
        "computed_at": _now_iso(),
        "sample_rows": sample_rows,
        "columns": columns,
    }


def _profile_path(slug: str) -> Path:
    return outputs_root() / slug / "profile.json"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="profile",
                                 description=__doc__.splitlines()[0])
    ap.add_argument("slugs", nargs="*", help="slugs to profile (default: empty → see --all)")
    ap.add_argument("--all", action="store_true",
                    help="profile every slug whose parquet exists locally")
    ap.add_argument("--sample-rows", type=int, default=None,
                    help="cap row count via reservoir sampling (default: full pass)")
    ap.add_argument("--no-promote", action="store_true",
                    help="skip the auto-promote step that mirrors built profiles "
                         "into docs/v{n}/profiles/ (default: promote)")
    ap.add_argument("--force", action="store_true",
                    help="bypass the parquet-sha256 cache and re-profile even when "
                         "the existing profile.json reports an identical hash")
    args = ap.parse_args(argv)

    manifest = load_manifest()
    targets: list[str]
    if args.all:
        targets = [s["slug"] for s in iter_datasets(manifest)
                   if prepared_parquet(s["slug"], manifest).exists()]
    else:
        if not args.slugs:
            ap.error("pass --all or one or more slugs")
        targets = list(args.slugs)

    failures = 0
    for slug in targets:
        parquet = prepared_parquet(slug, manifest)
        if not parquet.exists():
            print(f"skip: {slug} — no parquet at {parquet}", file=sys.stderr)
            continue
        out = _profile_path(slug)
        if not args.force and out.exists():
            try:
                prior = json.loads(out.read_text())
                if prior.get("parquet_sha256") == _file_sha256(parquet) and \
                        prior.get("sample_rows") == args.sample_rows:
                    print(f"cached: {slug}")
                    continue
            except Exception:
                pass

        try:
            result = profile_slug(slug=slug, parquet_path=parquet,
                                  sample_rows=args.sample_rows)
        except Exception as e:   # noqa: BLE001 — surface per-slug errors
            print(f"error: {slug}: {e}", file=sys.stderr)
            failures += 1
            continue

        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(result, indent=2, default=str) + "\n")
        print(f"wrote: {out}")

    if not args.no_promote:
        # Auto-mirror built profiles into docs/v{n}/profiles/ so the tracked
        # snapshot stays in sync without humans remembering a second command.
        # Pass None for --all (idempotent: byte-identical destinations are
        # skipped); otherwise restrict to the slugs we just processed.
        promote_slugs: list[str] | None = None if args.all else list(targets)
        try:
            from . import promote_profiles
            copied, skipped, _missing = promote_profiles.promote(slugs=promote_slugs)
            print(f"mirrored {copied} profile(s) to docs/v1/profiles/ ({skipped} unchanged)")
        except Exception as e:   # noqa: BLE001 — don't undo profile success on a mirror glitch
            print(f"mirror skipped: {e}", file=sys.stderr)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
