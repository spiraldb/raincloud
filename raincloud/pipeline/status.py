# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Report per-dataset state across the manifest.

For each DatasetSpec in sources.json, walk the filesystem and report what this
install builds and keeps (its `formats`, `keep_raw` and `keep_canonical`
settings):
    raw      — the selected recipe generation's raw bytes (under the configured
               raw root, `<slug>/` or `<slug>/.recipes/<key>/`) present, and
               matching expected_bytes if the manifest declared one for a
               single-URL fetch; `err` when a generated dataset's receipt
               cannot be read
    work     — the recipe's scratch directory under the configured scratch root
               present (extract scratch — wiped by --clean-workdir)
    arrow    — (schema_version 2+) the canonical outputs/v{n}/<slug>/arrow/<slug>.arrow.zstd present
    parquet  — outputs/v{n}/<slug>/parquet/<slug>.parquet present when this install
               builds Parquet; row count vs expect.rows; stale (v2) when older
               than the canonical Arrow
    vortex   — the Vortex export present when this install builds Vortex; stale
               when older than its source (the canonical Arrow in v2)
    <format> — likewise for each other format this install builds, as a column
               when any dataset has it

A raw download or canonical Arrow the install does not keep is reported, but
its absence leaves a dataset complete.

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

from raincloud._formats import EXPORTED_FORMATS, buildable_formats, vortex_cells, wanted_formats
from raincloud.config import get_config

from .selection import SelectionError, select_specs
from .spec import (
    is_hydrated,
    load_manifest,
    prepared_arrow,
    prepared_artifact,
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
    """The canonical Arrow: `expected` when this install keeps it."""
    if m["schema_version"] < 2:  # v1 catalogs only; remove when v1 bundles are no longer read.
        return {"expected": False}
    expected = get_config().keep_canonical
    p = prepared_arrow(spec["slug"], m)
    if not p.is_file():
        return {"expected": expected, "present": False}
    return {"expected": expected, "present": True, "bytes": p.stat().st_size}


def _builds_here(fmt: str, m: dict) -> bool:
    """Whether this install builds `fmt` (its `formats` setting); a v1 catalog
    predates the setting and builds what its recipes say."""
    return m["schema_version"] < 2 or fmt in wanted_formats(get_config())


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


def _format_status(spec: dict, m: dict, fmt: str) -> dict:
    """An exported format's state: `expected` (the dataset offers it and this
    install builds it), `present` with its `bytes`, `stale` when older than the
    canonical Arrow it was exported from, and `unavailable` (the measurement)
    when a build measured its writer unable to produce it."""
    if fmt not in buildable_formats(spec, m["schema_version"]) or not _builds_here(fmt, m):
        return {"expected": False}
    measured = _measured(spec, fmt, m)
    if measured is not None:
        return {"expected": True, "present": False, "unavailable": measured}
    p = prepared_artifact(spec["slug"], fmt, m)
    if not p.exists():
        return {"expected": True, "present": False}
    info: dict[str, Any] = {"expected": True, "present": True, "bytes": p.stat().st_size}
    canonical = prepared_arrow(spec["slug"], m) if m["schema_version"] >= 2 else None
    if canonical is not None and canonical.exists() and canonical.stat().st_mtime > p.stat().st_mtime:
        info["stale"] = True  # exported from an older canonical: a display hint, as for vortex
    return info


def _parquet_status(spec: dict, m: dict, *, fast: bool) -> dict:
    info = _format_status(spec, m, "parquet")
    if fast or not info.get("present"):
        return info
    try:
        pf = pq.ParquetFile(_parquet_path(spec, m))
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
    if not vortex_cells(spec, m["schema_version"], m) or not _builds_here("vortex", m):
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


# Formats with a status of their own: Parquet also reports its footer's row
# count, Vortex keeps the `opted_in` shape the browser reads. Every other
# exported format is reported by `_format_status`.
_OTHER_FORMATS = tuple(fmt for fmt in EXPORTED_FORMATS if fmt not in ("parquet", "vortex"))


def gather(spec: dict, m: dict, *, fast: bool) -> dict:
    return {
        "slug": spec["slug"],
        "raw":     _raw_status(spec),
        "work":    _workdir_status(spec["slug"]),
        "arrow":   _arrow_status(spec, m),
        "parquet": _parquet_status(spec, m, fast=fast),
        "vortex":  vortex_status(spec, m),
        **{fmt: _format_status(spec, m, fmt) for fmt in _OTHER_FORMATS},
    }


def _wanted(state: dict) -> bool:
    """A format the export policy includes, not measured unavailable."""
    return bool((state.get("expected") or state.get("opted_in")) and not state.get("unavailable"))


def _is_incomplete(row: dict) -> bool:
    arrow = row["arrow"]
    parq = row["parquet"]
    return bool(
        (not row["raw"].get("present") and get_config().keep_raw)
        or row["raw"].get("bytes_expected") is not None
        or (arrow.get("expected") and not arrow.get("present"))
        or parq.get("rows_expected") is not None
        or parq.get("error")
        or any(_wanted(row[fmt]) and (not row[fmt].get("present") or row[fmt].get("stale"))
               for fmt in EXPORTED_FORMATS)
    )


# ---------- rendering ----------

def _fmt_row(row: dict, rows: list[dict] | None = None) -> tuple[str, ...]:
    raw = row["raw"]
    if raw.get("error"):
        raw_cell = "err"
    elif raw.get("bytes_expected") is not None:
        raw_cell = "≠"
    else:
        raw_cell = "✓" if raw.get("present") else "·"

    work_cell = "✓" if row["work"].get("present") else "·"

    arrow = row["arrow"]
    # A canonical the install does not keep can still be present: kept as the
    # dataset's only file, or left by an earlier build.
    arrow_cell = "✓" if arrow.get("present") else ("·" if arrow.get("expected") else "n/a")

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

    others = tuple(_presence_cell(row[fmt]) for fmt in ("vortex", *_shown(rows or [row])))
    return (row["slug"], raw_cell, work_cell, arrow_cell, parq_cell, *others)


def _presence_cell(state: dict) -> str:
    if not (state.get("expected") or state.get("opted_in")):
        return "n/a"
    if state.get("unavailable"):
        return "unavail"
    if not state.get("present"):
        return "·"
    return "stale" if state.get("stale") else "✓"


def _shown(rows: list[dict]) -> tuple[str, ...]:
    """The formats beyond parquet and vortex that some row's policy includes:
    a column no dataset exports would be all n/a."""
    return tuple(fmt for fmt in _OTHER_FORMATS if any(r.get(fmt, {}).get("expected") for r in rows))


def render_table(rows: list[dict]) -> str:
    headers = ("slug", "raw", "work", "arrow", "parquet", "vortex", *_shown(rows))
    cells = [headers] + [_fmt_row(r, rows) for r in rows]
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
    unavailable = sum(1 for r in rows for fmt in EXPORTED_FORMATS if r[fmt].get("unavailable"))
    others = ""
    for fmt in _shown(rows):
        expected = [r for r in rows if r[fmt].get("expected")]
        ok = sum(1 for r in expected if r[fmt].get("present") and not r[fmt].get("stale"))
        others += f"  ·  {fmt} {ok}/{len(expected)}"
    return (f"\n{n} slugs  ·  raw {raw_ok}/{n}{arrow}  ·  parquet {parq_ok}/{len(parq_exp)}"
            f"  ·  rows-match {rows_ok}/{len(parq_exp)}"
            f"  ·  vortex {vrtx_ok}/{len(vrtx_opt)}{others}"
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
