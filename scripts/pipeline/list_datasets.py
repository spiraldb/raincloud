# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Filter and list datasets from sources.json without grepping the manifest by hand.

The manifest is a multi-hundred-KB JSON file with hundreds of entries. Greppable but
awkward when you want "every public-bi slug" or "every spec using handler X".
This is the read-only query layer over it.

Usage:
    python -m scripts.pipeline.list_datasets                        # every slug
    python -m scripts.pipeline.list_datasets --family uci           # filter
    python -m scripts.pipeline.list_datasets --handler tighten_types
    python -m scripts.pipeline.list_datasets --license CC0-1.0
    python -m scripts.pipeline.list_datasets --fetch-type kaggle
    python -m scripts.pipeline.list_datasets --reader csv
    python -m scripts.pipeline.list_datasets --vortex              # convert.vortex == true
    python -m scripts.pipeline.list_datasets --no-vortex           # convert.vortex == false / missing
    python -m scripts.pipeline.list_datasets --kaggle-tos          # requires_interactive_accept
    python -m scripts.pipeline.list_datasets --scrape              # license.scrape_advisory non-null
    python -m scripts.pipeline.list_datasets --hydrate             # hydrate config non-null
    python -m scripts.pipeline.list_datasets --showcase start-here # editorial tier (repeatable)
    python -m scripts.pipeline.list_datasets --tag geospatial      # domain tag (repeatable)
    python -m scripts.pipeline.list_datasets --size s --size m     # size bucket (repeatable)
    python -m scripts.pipeline.list_datasets --trait has_nested    # shape trait; ! to negate
    python -m scripts.pipeline.list_datasets --view start-here     # named preset (clears other axes)
    python -m scripts.pipeline.list_datasets --long                # slug + key fields
    python -m scripts.pipeline.list_datasets --json                # one JSON object per row
    python -m scripts.pipeline.list_datasets --count               # just the count

Inspection modes — these read built parquet (or vortex) files instead of
just the manifest, so they only show slugs that have actually been built:

    python -m scripts.pipeline.list_datasets --columns                    # every (slug, column, type) row
    python -m scripts.pipeline.list_datasets --columns --column-grep emb  # only columns matching regex
    python -m scripts.pipeline.list_datasets --columns --source vortex    # vortex schema instead of parquet
    python -m scripts.pipeline.list_datasets --coverage                   # per-distinct-type counts + examples

Filters compose with AND. Pass --grep PATTERN for a regex match against
slug + short_name + full_name + description.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from typing import Any

from .discovery import (
    FilterState,
    SHOWCASE_TIERS,
    SIZE_BUCKETS,
    TAG_VOCAB,
    TRAIT_FLAGS,
    VIEW_PRESETS,
    apply_preset,
    format_column_line,
)
from .spec import (
    REPO_ROOT,
    iter_datasets,
    load_manifest,
    outputs_root,
    prepared_parquet,
    prepared_vortex,
    spec_field,
)


def _load_snapshot() -> dict:
    """Load docs/snapshot.json (or v1 fallback) keyed by slug.

    Returns {} when neither file exists or both are malformed.
    """
    import json as _json
    for p in (REPO_ROOT / "docs" / "snapshot.json",
              REPO_ROOT / "docs" / "v1" / "snapshot.json"):
        if p.exists():
            try:
                blob = _json.loads(p.read_text())
            except Exception:
                continue
            if isinstance(blob, dict) and "slugs" in blob:
                return blob["slugs"]
            if isinstance(blob, dict) and "datasets" in blob:
                return {d["slug"]: d for d in blob["datasets"]}
            if isinstance(blob, dict):
                return blob
    return {}


def _filter_state_from_args(args) -> FilterState:
    """Build FilterState from the parsed argparse Namespace.

    --view replaces ALL other facet selections (preset is the complete spec).
    Mixing a preset with individual facet flags is incoherent UX, so when
    --view is set we short-circuit and return the preset state unmodified.
    Other inline filters (handler, reader, kaggle_tos, scrape, hydrate, grep)
    still apply since they're outside FilterState's domain.
    """
    if getattr(args, "view", None):
        return apply_preset(args.view)
    state = FilterState()
    # Additive: each repeated flag adds to the corresponding axis.
    for axis_name, src in (
        ("showcase", args.showcase),
        ("tag", args.tag),
        ("size", args.size),
    ):
        if src:
            getattr(state, axis_name).update(src)
    # The existing --license, --family, --fetch-type flags are single-value.
    if getattr(args, "license", None):
        state.license.add(args.license)
    if getattr(args, "family", None):
        state.family.add(args.family)
    if getattr(args, "fetch_type", None):
        state.fetch_type.add(args.fetch_type)
    # Trait flags: prefix '!' negates.
    for flag in args.trait or []:
        if flag.startswith("!"):
            state.trait_negated.add(flag[1:])
        else:
            state.trait.add(flag)
    if args.vortex:
        state.vortex = True
    elif args.no_vortex:
        state.vortex = False
    return state


def _matches(spec: dict, args, state: FilterState, snapshot: dict) -> bool:
    """Apply the inline filters not covered by FilterState, then defer the
    closed-vocab axes (family / license / fetch_type / vortex / showcase /
    tag / size / trait) to FilterState.matches().
    """
    if args.handler and spec_field(spec, "transform.handler") != args.handler: return False
    if args.reader and spec_field(spec, "parse.reader") != args.reader: return False
    if args.kaggle_tos and not spec_field(spec, "fetch.requires_interactive_accept", False): return False
    if args.scrape and not spec_field(spec, "license.scrape_advisory"): return False
    if args.hydrate and not spec.get("hydrate"): return False
    if not state.matches(spec=spec, snapshot=snapshot.get(spec["slug"], {})):
        return False
    if args.grep:
        haystack = " ".join((
            spec.get("slug", ""),
            spec.get("short_name", ""),
            spec.get("full_name", ""),
            spec.get("description", ""),
        ))
        if not re.search(args.grep, haystack, flags=re.IGNORECASE): return False
    return True


def _long_row(spec: dict) -> dict[str, Any]:
    return {
        "slug":                spec["slug"],
        "family":              spec.get("family"),
        "handler":             spec_field(spec, "transform.handler"),
        "fetch_type":          spec_field(spec, "fetch.type"),
        "reader":              spec_field(spec, "parse.reader"),
        "license":             spec_field(spec, "license.spdx"),
        "rows":                spec_field(spec, "expect.rows"),
        "vortex":              bool(spec_field(spec, "convert.vortex", False)),
        "vortex_skip_reason":  spec_field(spec, "convert.vortex_skip_reason"),
        "scrape_advisory":     spec_field(spec, "license.scrape_advisory"),
        "hydrate":             spec.get("hydrate"),
        "row_stability":       (spec.get("expect") or {}).get("row_stability"),
        "references":          spec.get("references") or [],
        "short_name":          spec.get("short_name"),
    }


def _render_long_table(rows: list[dict]) -> str:
    if not rows: return ""
    headers = ("slug", "family", "handler", "fetch", "reader", "license", "rows", "vortex", "scrape", "hydrate")
    cells = [headers]
    for r in rows:
        cells.append((
            r["slug"],
            r["family"] or "",
            r["handler"] or "",
            r["fetch_type"] or "",
            r["reader"] or "",
            r["license"] or "",
            f"{r['rows']:,}" if isinstance(r["rows"], int) else "—",
            "✓" if r["vortex"] else "·",
            "⚠" if r["scrape_advisory"] else "·",
            "✓" if r["hydrate"] else "·",
        ))
    widths = [max(len(c) for c in (row[i] for row in cells)) for i in range(len(headers))]
    out = []
    for i, row in enumerate(cells):
        out.append("  ".join(c.ljust(widths[j]) for j, c in enumerate(row)))
        if i == 0: out.append("  ".join("-" * w for w in widths))
    return "\n".join(out)


# ---------- column / coverage inspection ----------

def _collapse_bracketed(ty: str, prefix: str, open_b: str, close_b: str) -> str:
    """Replace `prefix<body>` (or `prefix(body)`) with `prefix...<close>`.
    Handles nested brackets via depth counter."""
    out = []
    i = 0
    while True:
        j = ty.find(prefix, i)
        if j < 0:
            out.append(ty[i:])
            return "".join(out)
        out.append(ty[i:j + len(prefix)])
        depth = 1
        k = j + len(prefix)
        while k < len(ty) and depth > 0:
            if ty[k] == open_b:
                depth += 1
            elif ty[k] == close_b:
                depth -= 1
            k += 1
        out.append("...")
        out.append(close_b)
        i = k


def _canonicalize_type(ty: str) -> str:
    """Collapse STRUCT bodies so distinct struct *shapes* don't each become
    their own coverage row. Handles both DuckDB-style `STRUCT(...)` (used
    by --coverage's DuckDB DESCRIBE path) and pyarrow-style `struct<...>`
    (used by the TUI's per-slug coverage modal). Scalars keep their
    parameters — `DECIMAL(10, 2)`, `TIMESTAMP WITH TIME ZONE` are unchanged."""
    ty = _collapse_bracketed(ty, "STRUCT(", "(", ")")
    ty = _collapse_bracketed(ty, "struct<", "<", ">")
    return ty


def _iter_columns(matched: list[dict], source: str) -> list[dict]:
    """Walk built parquet (or vortex) for each matched slug and yield one
    dict per top-level column. Slugs with no built file are skipped silently
    — column inspection is only meaningful for built outputs.
    """
    rows: list[dict] = []
    if source == "vortex":
        try:
            import vortex as vx
        except ImportError:
            print("--source vortex requires `vortex-data` (uv sync pulls it in)",
                  file=sys.stderr)
            return rows
    else:
        import pyarrow.parquet as pq

    for spec in matched:
        slug = spec["slug"]
        if source == "vortex":
            path = prepared_vortex(slug)
            if not path.exists(): continue
            try:
                vf = vx.open(str(path))
                schema = vf.dtype.to_arrow_schema()
            except BaseException as e:
                print(f"  [skip] {slug}: {type(e).__name__}: {str(e).splitlines()[0][:80]}",
                      file=sys.stderr)
                continue
            for f in schema:
                rows.append({"slug": slug, "column": f.name, "type": str(f.type),
                             "source": "vortex"})
        else:
            path = prepared_parquet(slug)
            if not path.exists(): continue
            try:
                pf = pq.ParquetFile(path)
            except Exception as e:
                print(f"  [skip] {slug}: {type(e).__name__}: {str(e).splitlines()[0][:80]}",
                      file=sys.stderr)
                continue
            for f in pf.schema_arrow:
                rows.append({"slug": slug, "column": f.name, "type": str(f.type),
                             "source": "parquet"})
    return rows


def _filter_columns(col_rows: list[dict], pattern: str | None) -> list[dict]:
    if not pattern: return col_rows
    rx = re.compile(pattern, re.IGNORECASE)
    return [r for r in col_rows if rx.search(r["column"])]


def _render_columns_table(col_rows: list[dict]) -> str:
    if not col_rows: return ""
    headers = ("slug", "column", "type")
    cells = [headers]
    for r in col_rows:
        cells.append((r["slug"], r["column"], r["type"]))
    widths = [max(len(c) for c in (row[i] for row in cells)) for i in range(len(headers))]
    out = []
    for i, row in enumerate(cells):
        out.append("  ".join(c.ljust(widths[j]) for j, c in enumerate(row)))
        if i == 0: out.append("  ".join("-" * w for w in widths))
    return "\n".join(out)


def _coverage_summary(col_rows: list[dict]) -> list[dict]:
    """Aggregate column rows into one row per canonical type."""
    by_type: dict[str, list[tuple[str, str]]] = {}
    for r in col_rows:
        canon = _canonicalize_type(r["type"])
        by_type.setdefault(canon, []).append((r["slug"], r["column"]))
    out = []
    for ty in sorted(by_type):
        entries = by_type[ty]
        out.append({
            "type": ty,
            "columns": len(entries),
            "datasets": len({slug for slug, _ in entries}),
            "examples": [f"{slug}.{col}" for slug, col in entries[:3]],
        })
    return out


def _render_coverage_table(rows: list[dict]) -> str:
    if not rows: return ""
    headers = ("type", "columns", "datasets", "examples")
    cells = [headers]
    for r in rows:
        ex = ", ".join(r["examples"])
        if r["columns"] > len(r["examples"]):
            ex += f" (+{r['columns'] - len(r['examples'])})"
        cells.append((r["type"], str(r["columns"]), str(r["datasets"]), ex))
    widths = [max(len(c) for c in (row[i] for row in cells)) for i in range(len(headers))]
    out = []
    for i, row in enumerate(cells):
        out.append("  ".join(c.ljust(widths[j]) for j, c in enumerate(row)))
        if i == 0: out.append("  ".join("-" * w for w in widths))
    return "\n".join(out)


def _inspect(slug: str) -> int:
    """Render the TUI detail-pane equivalent as plain text."""
    import json as _json
    manifest = load_manifest()
    spec = next((s for s in manifest["datasets"] if s["slug"] == slug), None)
    if spec is None:
        print(f"unknown slug: {slug}", file=sys.stderr)
        return 2

    print(f"# {slug} — {spec.get('short_name', '')}")
    if spec.get("showcase"):
        print(f"showcase: {', '.join(spec['showcase'])}")
    if spec.get("tags"):
        print(f"tags:     {', '.join(spec['tags'])}")
    lic = (spec.get("license") or {}).get("spdx")
    if lic:
        print(f"license:  {lic}")
    print(f"family:   {spec.get('family')}")
    print()
    desc = (spec.get("description") or "").strip()
    if desc:
        print(desc)
        print()

    profile_path = outputs_root() / slug / "profile.json"
    if not profile_path.exists():
        print(f"no profile yet — run `python -m scripts.pipeline.profile {slug}`")
        return 0

    try:
        profile = _json.loads(profile_path.read_text())
    except Exception as e:
        print(f"profile.json malformed: {e}", file=sys.stderr)
        return 2
    print(f"rows: {profile['row_count']}   sample_rows: {profile.get('sample_rows')}")
    print(f"columns ({len(profile['columns'])}):")
    for name, col in profile["columns"].items():
        print(format_column_line(name, col))
    return 0


def _print_vocab_help(manifest: dict, *, vocab_name: str) -> int:
    """Print closed vocab + per-value count from the live manifest."""
    if vocab_name == "tags":
        vocab = TAG_VOCAB
        field = "tags"
    else:
        vocab = SHOWCASE_TIERS
        field = "showcase"
    counts = {v: 0 for v in vocab}
    for spec in manifest["datasets"]:
        for v in spec.get(field) or []:
            if v in counts:
                counts[v] += 1
    for v in vocab:
        print(f"  {v:<20} ({counts[v]})")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--family", help="filter by family (direct, kaggle-upstream, nyc-tlc, public-bi, uci)")
    ap.add_argument("--handler", help="filter by transform.handler name")
    ap.add_argument("--license", help="filter by license.spdx")
    ap.add_argument("--fetch-type", help="filter by fetch.type (http, kaggle, huggingface, custom)")
    ap.add_argument("--reader", help="filter by parse.reader (csv, parquet, jsonl, xml, pbf, custom)")
    ap.add_argument("--vortex", action="store_true", help="only specs with convert.vortex == true")
    ap.add_argument("--no-vortex", action="store_true", help="only specs with convert.vortex != true")
    ap.add_argument("--kaggle-tos", action="store_true",
                    help="only kaggle specs gated behind a one-time ToS click-through")
    ap.add_argument("--scrape", action="store_true",
                    help="only specs with a non-null license.scrape_advisory "
                         "(scrape corpora whose underlying licenses aren't cleared)")
    ap.add_argument("--hydrate", action="store_true",
                    help="only specs with a non-null hydrate block "
                         "(URL columns marked as candidates for the hydrate stage)")
    ap.add_argument("--showcase", action="append", choices=list(SHOWCASE_TIERS),
                    help="filter by editorial showcase tier; repeatable")
    ap.add_argument("--tag", action="append", choices=list(TAG_VOCAB),
                    help="filter by domain tag; repeatable")
    ap.add_argument("--size", action="append", choices=list(SIZE_BUCKETS),
                    help="filter by size bucket; repeatable")
    ap.add_argument("--trait", action="append",
                    help="filter by shape trait (e.g. has_nested); prefix with ! to negate; repeatable")
    ap.add_argument("--view", choices=list(VIEW_PRESETS),
                    help="apply a named view preset (replaces other selections)")
    ap.add_argument("--grep", help="regex over slug + short_name + full_name + description (case-insensitive)")
    ap.add_argument("--long", action="store_true", help="emit a wide table with key fields")
    ap.add_argument("--json", action="store_true", help="emit one JSON object per matching dataset")
    ap.add_argument("--count", action="store_true", help="emit just the count of matches")

    # Inspection modes — read built parquet/vortex instead of just the manifest.
    ap.add_argument("--columns", action="store_true",
                    help="emit one row per (slug, column, type) across built outputs")
    ap.add_argument("--coverage", action="store_true",
                    help="emit one row per distinct type with column / dataset counts and examples")
    ap.add_argument("--column-grep", metavar="PATTERN",
                    help="when used with --columns, only emit columns whose name matches this regex (case-insensitive)")
    ap.add_argument("--source", choices=("parquet", "vortex"), default="parquet",
                    help="when used with --columns / --coverage, read from parquet (default) or vortex outputs")
    ap.add_argument("--inspect", metavar="SLUG",
                    help="render the detail view for one slug (description + per-column profile)")
    ap.add_argument("--tags-help", action="store_true",
                    help="list the closed tag vocab + per-tag counts in the manifest")
    ap.add_argument("--showcase-help", action="store_true",
                    help="list the showcase tiers + per-tier counts in the manifest")
    args = ap.parse_args(argv)

    if args.vortex and args.no_vortex:
        print("--vortex and --no-vortex are mutually exclusive", file=sys.stderr)
        return 2
    if args.columns and args.coverage:
        print("--columns and --coverage are mutually exclusive", file=sys.stderr)
        return 2

    if args.inspect:
        return _inspect(args.inspect)
    manifest = load_manifest()
    if args.tags_help:
        return _print_vocab_help(manifest, vocab_name="tags")
    if args.showcase_help:
        return _print_vocab_help(manifest, vocab_name="showcase")

    m = load_manifest()
    state = _filter_state_from_args(args)
    snapshot = _load_snapshot()
    matched = [s for s in iter_datasets(m) if _matches(s, args, state, snapshot)]

    if args.columns or args.coverage:
        col_rows = _iter_columns(matched, args.source)
        col_rows = _filter_columns(col_rows, args.column_grep)
        if args.coverage:
            cov = _coverage_summary(col_rows)
            if args.json:
                for r in cov:
                    json.dump(r, sys.stdout); sys.stdout.write("\n")
                return 0
            if args.count:
                print(len(cov)); return 0
            print(_render_coverage_table(cov))
            return 0
        # --columns
        if args.json:
            for r in col_rows:
                json.dump(r, sys.stdout); sys.stdout.write("\n")
            return 0
        if args.count:
            print(len(col_rows)); return 0
        print(_render_columns_table(col_rows))
        return 0

    if args.count:
        print(len(matched))
        return 0
    if args.json:
        for s in matched:
            json.dump(_long_row(s), sys.stdout)
            sys.stdout.write("\n")
        return 0
    if args.long:
        if matched:
            print(_render_long_table([_long_row(s) for s in matched]))
        return 0
    for s in matched:
        print(s["slug"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
