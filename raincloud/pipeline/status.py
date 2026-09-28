# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Report per-dataset state across the manifest.

For each DatasetSpec in sources.json, walk the filesystem and report:
    raw      — the selected recipe generation's raw bytes (under the configured
               raw root, `<slug>/` or `<slug>/.recipes/<key>/`) present, and
               matching expected_bytes if the manifest declared one for a
               single-URL fetch; `err` when a generated dataset's receipt
               cannot be read
    work     — the recipe's scratch directory under the configured scratch root
               present (extract scratch — wiped by --clean-workdir)
    arrow    — (schema_version 2+) the canonical outputs/v{n}/<slug>/arrow/<slug>.arrow.zstd present
    parquet  — outputs/v{n}/<slug>/parquet/<slug>.parquet present when the export
               policy includes Parquet; row count vs expect.rows; stale (v2) when
               older than the canonical Arrow
    vortex   — the Vortex export present when the export policy includes Vortex;
               stale when older than its source (the canonical Arrow in v2)

A format a build measured unavailable at the current recipe (this install's
build record, else the selected catalog's snapshot) shows `unavail` and counts
as complete: the build did what its writer could, and `raincloud describe`
quotes the measurement.

"stale" here is an mtime display hint, never a trust decision: which file the
loader serves, the catalog and the build record decide.

`--missing-only` leaves out a hydrated dataset that was never built, unless it
is named: those are built only on request.

Usage:
    python -m raincloud.pipeline.status                  # all slugs, full scan
    python -m raincloud.pipeline.status <slug>...
    python -m raincloud.pipeline.status --fast           # skip parquet footer reads
    python -m raincloud.pipeline.status --missing-only   # only incomplete slugs
    python -m raincloud.pipeline.status --json           # machine-readable
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from raincloud._formats import buildable_formats, vortex_cells

from .selection import SelectionError, select_specs
from .spec import (
    is_hydrated,
    load_manifest,
    prepared_arrow,
    prepared_parquet,
    prepared_vortex,
    raw_slug_dir,
    recipe_workdir_root,
    spec_field,
    workdir_root,
)


def _raw_status(spec: dict) -> dict:
    if spec.get("fetch", {}).get("type") == "generated":
        from raincloud._generated import generation_key

        from .generate import cached_outputs
        try:
            paths = cached_outputs(spec["fetch"], verify=False)
            path = paths.get(spec["fetch"]["output"])
            return {"present": path is not None, "files": int(path is not None),
                    "bytes": path.stat().st_size if path else 0,
                    "generation_group": generation_key(spec["fetch"]), "group_files": len(paths)}
        except (ValueError, OSError) as exc:
            return {"present": False, "error": str(exc)}
    d = raw_slug_dir(spec["slug"])
    if not d.exists() or not any(d.iterdir()):
        return {"present": False}
    files = [p for p in d.rglob("*") if p.is_file() and not any(part.startswith(".") for part in p.relative_to(d).parts)]
    if not files:
        return {"present": False}
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
    from raincloud.catalogs import current, selected_context
    context = current() or selected_context()
    if context is not None:
        recipe = next((s for s in context.manifest["datasets"] if s["slug"] == slug), None)
        if recipe is not None:
            candidate = recipe_workdir_root(recipe, context.manifest) / slug
            if candidate.exists():
                d = candidate
    if not d.exists() or not any(d.iterdir()):
        return {"present": False}
    return {"present": True}


def _parquet_path(spec: dict, m: dict) -> Path:
    return prepared_parquet(spec["slug"], m)


def _arrow_status(spec: dict, m: dict) -> dict:
    if m["schema_version"] < 2:  # v1 catalogs only; remove when v1 bundles are no longer read.
        return {"expected": False}
    p = prepared_arrow(spec["slug"], m)
    if not p.is_file():
        return {"expected": True, "present": False}
    return {"expected": True, "present": True, "bytes": p.stat().st_size}


def _measured(spec: dict, fmt: str, m: dict) -> dict | None:
    """The measurement saying `fmt` cannot be made for `spec` here (see the
    module docstring), or None; the selected catalog supplies the snapshot."""
    from raincloud.catalogs import current

    from .records import measured_unavailable
    context = current()
    if context is None or context.manifest["schema_version"] != m["schema_version"]:
        return None
    return measured_unavailable(spec, fmt, context.snapshot.get("slugs", {}).get(spec["slug"]),
                                context.manifest, builds=_record())


_RECORD: dict = {}


def _record() -> dict:
    """This install's build record, re-read only when the file changes: a
    status walk asks for it once per dataset and format."""
    from raincloud import _builds

    from .spec import outputs_base
    path = _builds.record_path(outputs_base())
    try:
        stat = path.stat()
        key = (str(path), stat.st_mtime_ns, stat.st_size)
    except FileNotFoundError:
        key = (str(path), None, None)
    if _RECORD.get("key") != key:
        _RECORD.update(key=key, artifacts=_builds.read(outputs_base()))
    return _RECORD["artifacts"]


def _parquet_status(spec: dict, m: dict, *, fast: bool) -> dict:
    if "parquet" not in buildable_formats(spec, m["schema_version"]):
        return {"expected": False}
    measured = _measured(spec, "parquet", m)
    if measured is not None:
        return {"expected": True, "present": False, "unavailable": measured}
    p = _parquet_path(spec, m)
    if not p.exists():
        return {"expected": True, "present": False}
    info: dict[str, Any] = {"expected": True, "present": True, "bytes": p.stat().st_size}
    canonical = prepared_arrow(spec["slug"], m) if m["schema_version"] >= 2 else None
    if canonical is not None and canonical.exists() and canonical.stat().st_mtime > p.stat().st_mtime:
        info["stale"] = True  # exported from an older canonical: a display hint, as for vortex
    if fast:
        return info
    try:
        pf = pq.ParquetFile(p)
        info["rows"] = pf.metadata.num_rows
    except Exception as e:
        info["error"] = f"{type(e).__name__}: {e}"
        return info
    expected_rows = spec_field(spec, "expect.rows")
    if expected_rows is not None and info["rows"] != expected_rows:
        info["rows_expected"] = expected_rows
    return info


def vortex_status(spec: dict, m: dict, *, source: Path | None = None,
                  vortex: Path | None = None) -> dict:
    """The Vortex export's state: `opted_in` (the export policy includes Vortex),
    `present`, `stale` when older than `source`, and `unavailable` (the
    measurement) when a build measured its writer unable to produce it.

    `vortex` and `source` default to the prepared paths; `source` is the
    canonical Arrow in schema_version 2 and the Parquet in v1.
    """
    if not vortex_cells(spec, m["schema_version"], m):
        return {"opted_in": False}
    measured = _measured(spec, "vortex", m)
    if measured is not None:
        return {"opted_in": True, "present": False, "unavailable": measured}
    vortex = vortex if vortex is not None else prepared_vortex(spec["slug"], m)
    if not vortex.is_file():
        return {"opted_in": True, "present": False}
    if source is None:
        # The Parquet source is for v1 catalogs only; remove when v1 bundles are no longer read.
        source = (prepared_arrow(spec["slug"], m) if m["schema_version"] >= 2
                  else _parquet_path(spec, m))
    info: dict[str, Any] = {"opted_in": True, "present": True,
                            "bytes": vortex.stat().st_size}
    if source.exists() and source.stat().st_mtime > vortex.stat().st_mtime:
        info["stale"] = True
    return info


def gather(spec: dict, m: dict, *, fast: bool) -> dict:
    return {
        "slug": spec["slug"],
        "raw":     _raw_status(spec),
        "work":    _workdir_status(spec["slug"]),
        "arrow":   _arrow_status(spec, m),
        "parquet": _parquet_status(spec, m, fast=fast),
        "vortex":  vortex_status(spec, m),
    }


def _is_incomplete(row: dict) -> bool:
    arrow = row["arrow"]
    parq = row["parquet"]
    vrtx = row["vortex"]
    return bool(
        not row["raw"].get("present")
        or row["raw"].get("bytes_expected") is not None
        or (arrow.get("expected") and not arrow.get("present"))
        or (parq.get("expected") and not parq.get("unavailable")
            and (not parq.get("present") or parq.get("stale")))
        or parq.get("rows_expected") is not None
        or parq.get("error")
        or (vrtx.get("opted_in") and not vrtx.get("unavailable")
            and (not vrtx.get("present") or vrtx.get("stale")))
    )


# ---------- rendering ----------

def _fmt_row(row: dict) -> tuple[str, ...]:
    raw = row["raw"]
    if raw.get("error"):
        raw_cell = "err"
    elif raw.get("bytes_expected") is not None:
        raw_cell = "≠"
    else:
        raw_cell = "✓" if raw.get("present") else "·"

    work_cell = "✓" if row["work"].get("present") else "·"

    arrow = row["arrow"]
    arrow_cell = "n/a" if not arrow.get("expected") else ("✓" if arrow.get("present") else "·")

    parq = row["parquet"]
    if not parq.get("expected"):
        parq_cell = "n/a"
    elif parq.get("unavailable"):
        parq_cell = "unavail"
    elif parq.get("error"):
        parq_cell = "err"
    elif not parq.get("present"):
        parq_cell = "·"
    elif parq.get("stale"):
        parq_cell = "stale"
    elif parq.get("rows_expected") is not None:
        parq_cell = f"≠{parq['rows']:,}"
    elif "rows" in parq:
        parq_cell = f"✓{parq['rows']:,}"
    else:
        parq_cell = "✓"

    v = row["vortex"]
    if not v.get("opted_in"):
        vrtx_cell = "n/a"
    elif v.get("unavailable"):
        vrtx_cell = "unavail"
    elif not v.get("present"):
        vrtx_cell = "·"
    elif v.get("stale"):
        vrtx_cell = "stale"
    else:
        vrtx_cell = "✓"

    return row["slug"], raw_cell, work_cell, arrow_cell, parq_cell, vrtx_cell


def render_table(rows: list[dict]) -> str:
    headers = ("slug", "raw", "work", "arrow", "parquet", "vortex")
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
    arrow_exp = [r for r in rows if r["arrow"].get("expected")]
    arrow_ok  = sum(1 for r in arrow_exp if r["arrow"].get("present"))
    parq_exp  = [r for r in rows if r["parquet"].get("expected")]
    parq_ok   = sum(1 for r in parq_exp if r["parquet"].get("present") and not r["parquet"].get("error")
                    and not r["parquet"].get("stale"))
    rows_ok   = sum(1 for r in parq_exp
                    if r["parquet"].get("present")
                    and not r["parquet"].get("error")
                    and r["parquet"].get("rows_expected") is None
                    and "rows" in r["parquet"])
    vrtx_opt  = [r for r in rows if r["vortex"].get("opted_in")]
    vrtx_ok   = sum(1 for r in vrtx_opt if r["vortex"].get("present") and not r["vortex"].get("stale"))
    arrow = f"  ·  arrow {arrow_ok}/{len(arrow_exp)}" if arrow_exp else ""
    unavailable = sum(1 for r in rows for fmt in ("parquet", "vortex") if r[fmt].get("unavailable"))
    return (f"\n{n} slugs  ·  raw {raw_ok}/{n}{arrow}  ·  parquet {parq_ok}/{len(parq_exp)}"
            f"  ·  rows-match {rows_ok}/{len(parq_exp)}"
            f"  ·  vortex {vrtx_ok}/{len(vrtx_opt)}"
            + (f"  ·  {unavailable} measured unavailable" if unavailable else ""))


# ---------- CLI ----------

def _main(argv):
    ap = argparse.ArgumentParser(prog="python -m raincloud.pipeline.status",
                                 description=__doc__.split("\n", 1)[0])
    ap.add_argument("slugs", nargs="*", help="specific slugs (default: all)")
    ap.add_argument("--all", action="store_true", help="all slugs (the default)")
    ap.add_argument("--fast", action="store_true",
                    help="skip parquet footer reads (no row count)")
    ap.add_argument("--missing-only", action="store_true",
                    help="only show slugs with at least one incomplete stage")
    ap.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = ap.parse_args(argv)

    m = load_manifest()
    try:
        selected = select_specs(m, args.slugs, all_=args.all or not args.slugs,
                                include_hydrated=True, quiet=True)
    except SelectionError as exc:
        print(f"{ap.prog}: {exc}", file=sys.stderr)
        return 2

    rows = [gather(spec, m, fast=args.fast) for spec in selected]
    if args.missing_only:
        hydrated = {spec["slug"] for spec in selected if is_hydrated(spec)} - set(args.slugs)
        rows = [r for r in rows if _is_incomplete(r)
                and not (r["slug"] in hydrated and not r["arrow"].get("present"))]

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


def main(argv=None):
    from raincloud.catalogs import operation
    from raincloud.config import get_config
    with operation(get_config()):
        return _main(argv)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
