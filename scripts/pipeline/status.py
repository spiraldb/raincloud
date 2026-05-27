# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Report per-dataset state across the manifest.

For each DatasetSpec in sources.json, walk the filesystem and report:
    raw      — outputs/raw_downloads/<slug>/ present (and matches expected_bytes
               if the manifest declared one for a single-URL fetch)
    work     — _workdir/<slug>/ present (extract scratch — wiped by --clean-workdir)
    parquet  — outputs/v{n}/<slug>/parquet/<slug>.parquet present; row count vs expect.rows
    vortex   — outputs/v{n}/<slug>/vortex/<slug>.vortex present (only meaningful when convert.vortex=true)
    variant  — count of residual JSON-annotated columns (= /tighten-variant pending);
               skipped under --fast (column-level scan opens every parquet footer)

Usage:
    python -m scripts.pipeline.status                  # all slugs, full scan
    python -m scripts.pipeline.status <slug>...
    python -m scripts.pipeline.status --fast           # skip parquet footer reads
    python -m scripts.pipeline.status --missing-only   # only incomplete slugs
    python -m scripts.pipeline.status --json           # machine-readable
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from .spec import (
    iter_datasets,
    load_manifest,
    prepared_parquet,
    prepared_vortex,
    raw_downloads_root,
    spec_field,
    workdir_root,
)
from .tighten_variant import _json_columns


def _raw_status(spec: dict) -> dict:
    d = raw_downloads_root() / spec["slug"]
    if not d.exists() or not any(d.iterdir()):
        return {"present": False}
    files = [p for p in d.rglob("*") if p.is_file()]
    total_bytes = sum(p.stat().st_size for p in files)
    info: dict[str, Any] = {"present": True, "files": len(files), "bytes": total_bytes}
    # fetch.expected_bytes is only honoured by fetch.py for single-URL specs,
    # so we only flag a mismatch under that same condition.
    urls = spec_field(spec, "fetch.urls", []) or []
    ex_bytes = spec_field(spec, "fetch.expected_bytes")
    if ex_bytes is not None and len(urls) == 1 and total_bytes != ex_bytes:
        info["bytes_expected"] = ex_bytes
    return info


def _workdir_status(slug: str) -> dict:
    d = workdir_root() / slug
    if not d.exists() or not any(d.iterdir()):
        return {"present": False}
    return {"present": True}


def _parquet_path(spec: dict, m: dict) -> Path:
    return prepared_parquet(spec["slug"], m)


def _parquet_status(spec: dict, m: dict, *, fast: bool) -> dict:
    p = _parquet_path(spec, m)
    if not p.exists():
        return {"present": False}
    info: dict[str, Any] = {"present": True, "bytes": p.stat().st_size}
    if fast:
        return info
    try:
        pf = pq.ParquetFile(p)
        info["rows"] = pf.metadata.num_rows
        info["json_cols"] = _json_columns(p)
    except Exception as e:
        info["error"] = f"{type(e).__name__}: {e}"
        return info
    expected_rows = spec_field(spec, "expect.rows")
    if expected_rows is not None and info["rows"] != expected_rows:
        info["rows_expected"] = expected_rows
    return info


def _vortex_status(spec: dict, m: dict) -> dict:
    if not spec_field(spec, "convert.vortex", False):
        return {"opted_in": False}
    parquet = _parquet_path(spec, m)
    vortex = prepared_vortex(spec["slug"], m)
    if not vortex.exists():
        return {"opted_in": True, "present": False}
    info: dict[str, Any] = {"opted_in": True, "present": True, "bytes": vortex.stat().st_size}
    if parquet.exists() and parquet.stat().st_mtime > vortex.stat().st_mtime:
        info["stale"] = True
    return info


def gather(spec: dict, m: dict, *, fast: bool) -> dict:
    return {
        "slug": spec["slug"],
        "raw":     _raw_status(spec),
        "work":    _workdir_status(spec["slug"]),
        "parquet": _parquet_status(spec, m, fast=fast),
        "vortex":  _vortex_status(spec, m),
    }


def _is_incomplete(row: dict) -> bool:
    parq = row["parquet"]
    vrtx = row["vortex"]
    return bool(
        not row["raw"].get("present")
        or row["raw"].get("bytes_expected") is not None
        or not parq.get("present")
        or parq.get("rows_expected") is not None
        or parq.get("json_cols")
        or parq.get("error")
        or (vrtx.get("opted_in") and (not vrtx.get("present") or vrtx.get("stale")))
    )


# ---------- rendering ----------

def _fmt_row(row: dict) -> tuple[str, ...]:
    raw = row["raw"]
    raw_cell = "≠" if raw.get("bytes_expected") is not None else ("✓" if raw.get("present") else "·")

    work_cell = "✓" if row["work"].get("present") else "·"

    parq = row["parquet"]
    if parq.get("error"):
        parq_cell = "err"
    elif not parq.get("present"):
        parq_cell = "·"
    elif parq.get("rows_expected") is not None:
        parq_cell = f"≠{parq['rows']:,}"
    elif "rows" in parq:
        parq_cell = f"✓{parq['rows']:,}"
    else:
        parq_cell = "✓"

    v = row["vortex"]
    if not v.get("opted_in"):
        vrtx_cell = "n/a"
    elif not v.get("present"):
        vrtx_cell = "·"
    elif v.get("stale"):
        vrtx_cell = "stale"
    else:
        vrtx_cell = "✓"

    if not parq.get("present"):
        var_cell = "·"
    elif "json_cols" not in parq:
        var_cell = "?"  # --fast skipped the column-level scan
    elif parq["json_cols"]:
        var_cell = f"≠{len(parq['json_cols'])}"
    else:
        var_cell = "✓"

    return row["slug"], raw_cell, work_cell, parq_cell, vrtx_cell, var_cell


def render_table(rows: list[dict]) -> str:
    headers = ("slug", "raw", "work", "parquet", "vortex", "variant")
    cells = [headers] + [_fmt_row(r) for r in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(headers))]
    out = []
    for i, r in enumerate(cells):
        out.append("  ".join(c.ljust(widths[j]) for j, c in enumerate(r)))
        if i == 0:
            out.append("  ".join("-" * w for w in widths))
    return "\n".join(out)


def render_summary(rows: list[dict]) -> str:
    n = len(rows)
    raw_ok    = sum(1 for r in rows if r["raw"].get("present") and r["raw"].get("bytes_expected") is None)
    parq_ok   = sum(1 for r in rows if r["parquet"].get("present") and not r["parquet"].get("error"))
    rows_ok   = sum(1 for r in rows
                    if r["parquet"].get("present")
                    and not r["parquet"].get("error")
                    and r["parquet"].get("rows_expected") is None
                    and "rows" in r["parquet"])
    vrtx_opt  = [r for r in rows if r["vortex"].get("opted_in")]
    vrtx_ok   = sum(1 for r in vrtx_opt if r["vortex"].get("present") and not r["vortex"].get("stale"))
    var_pend  = sum(1 for r in rows if r["parquet"].get("json_cols"))
    return (f"\n{n} slugs  ·  raw {raw_ok}/{n}  ·  parquet {parq_ok}/{n}"
            f"  ·  rows-match {rows_ok}/{n}"
            f"  ·  vortex {vrtx_ok}/{len(vrtx_opt)}"
            f"  ·  tighten-variant pending {var_pend}")


# ---------- CLI ----------

def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("slugs", nargs="*", help="specific slugs (default: all)")
    ap.add_argument("--all", action="store_true", help="all slugs (the default)")
    ap.add_argument("--fast", action="store_true",
                    help="skip parquet footer reads (no row count, no JSON-column check)")
    ap.add_argument("--missing-only", action="store_true",
                    help="only show slugs with at least one incomplete stage")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = ap.parse_args(argv)

    m = load_manifest()
    if args.slugs:
        selected = [d for s in args.slugs for d in iter_datasets(m, slug=s)]
    else:
        selected = list(iter_datasets(m))

    rows = [gather(spec, m, fast=args.fast) for spec in selected]
    if args.missing_only:
        rows = [r for r in rows if _is_incomplete(r)]

    if args.json:
        json.dump(rows, sys.stdout, default=str, indent=2)
        sys.stdout.write("\n")
        return 0

    if not rows:
        print("(all slugs complete)" if args.missing_only else "(no slugs matched)")
        return 0

    print(render_table(rows))
    print(render_summary(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
