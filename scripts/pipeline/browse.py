# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Interactive TUI for browsing sources.json + triggering builds.

Sortable columns plus a detail pane on the right. The build-trigger flow
spawns `python -m scripts.pipeline.build <slug>` as a subprocess and
streams output into a modal log; cancellation kills the subprocess.

Run: `python -m scripts.pipeline.browse`
Install: `uv sync --extra tui --inexact`

Keybindings:
    q       — quit (orphans any in-flight build subprocesses)
    c       — open Columns modal for the highlighted slug (full per-column
              metadata: name, type, length, null_count, min, max from
              parquet row-group stats; placeholder + build hint when not
              built)
    t       — open Types modal for the highlighted slug (per-type
              aggregation across that slug's columns)
    b       — open Build-confirm modal for the highlighted slug (shows
              license / scrape advisory / time estimate / command line);
              press Enter or `b` again to actually launch the build
    esc / q — close any open modal (cancels an in-flight build)
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any

try:
    from textual.app import App, ComposeResult
    from textual.binding import Binding
    from textual.containers import Container, Horizontal, Vertical, VerticalScroll
    from textual.screen import ModalScreen
    from textual.widgets import (
        Collapsible,
        DataTable,
        Footer,
        Header,
        Input,
        Label,
        RadioButton,
        RadioSet,
        RichLog,
        Static,
    )
except ImportError:
    print(
        "textual is not installed. Install with: uv sync --extra tui --inexact",
        file=sys.stderr,
    )
    raise SystemExit(2)

from .discovery import (
    SHOWCASE_TIERS,
    SIZE_BUCKETS,
    TAG_VOCAB,
    TRAIT_FLAGS,
    FilterState,
)
from .list_datasets import _canonicalize_type
from .spec import (
    REPO_ROOT,
    iter_datasets,
    load_manifest,
    outputs_base,
    outputs_root,
    prepared_parquet,
    prepared_parquet_hydrated,
    prepared_vortex,
    spec_field,
)


def _filter_state_from_selections(selections: dict) -> FilterState:
    """Build a FilterState from raw {axis_name: set-of-values} selections.

    Each axis name corresponds to a facet RadioSet in the side-bar panel.
    Sets are 0- or 1-element (radios are exclusive); the vortex axis is a
    single Optional[bool] rather than a set.
    """
    state = FilterState()
    for axis in ("showcase", "tag", "size", "license", "fetch_type"):
        getattr(state, axis).update(selections.get(axis) or set())
    v = selections.get("vortex")
    if v is True or v is False:
        state.vortex = v
    return state


# ---------- free-text search over the slug table ----------

# Field qualifiers recognised in the search input. Each maps to the spec
# (or snapshot) attribute(s) the token's value is substring-matched against,
# case-insensitively. A bare token (no `field:` prefix) matches across the
# union of all of these.
_QUERY_FIELDS: tuple[str, ...] = (
    "slug", "name", "desc", "tag", "col", "lic", "handler", "reader", "fetch",
)
# Alias map → canonical field. Lets the user type plural/full forms.
_QUERY_ALIASES: dict[str, str] = {
    "tags": "tag", "column": "col", "columns": "col",
    "license": "lic", "description": "desc",
}


def _query_field_values(field: str, spec: dict, snap_slug: dict) -> list[str]:
    """The strings a `field:value` token matches against. Empty list means
    the field doesn't apply to this spec (auto-fail for that token).

    Every returned element is guaranteed to be `str`. We can't lean on
    `dict.get(k, "")` defaults — those only apply when the key is missing.
    Specs frequently carry explicit `"license": {"notes": null}` etc.,
    which would yield None and crash a downstream `" ".join(...)`."""
    def _s(v) -> str:
        return "" if v is None else str(v)

    if field == "slug":     return [_s(spec.get("slug"))]
    if field == "name":     return [_s(spec.get("short_name")), _s(spec.get("full_name"))]
    if field == "desc":     return [_s(spec.get("description"))]
    if field == "tag":      return [_s(t) for t in (spec.get("tags") or [])]
    if field == "col":      return [_s(c.get("name")) for c in (snap_slug.get("columns") or [])]
    if field == "lic":
        lic = spec.get("license") or {}
        return [_s(lic.get("spdx")), _s(lic.get("notes"))]
    if field == "handler":  return [_s((spec.get("transform") or {}).get("handler"))]
    if field == "reader":   return [_s((spec.get("parse") or {}).get("reader"))]
    if field == "fetch":    return [_s((spec.get("fetch") or {}).get("type"))]
    return []


def _parse_query(query: str) -> list[tuple[str | None, str]]:
    """Parse `slug:foo bar tag:enums` → [(field, value), ...] (lowercased).

    A token with `field:value` is a qualified clause; `field` must be one
    of `_QUERY_FIELDS` (after alias resolution) or the whole token is
    treated as bare. A bare token matches across every field."""
    out: list[tuple[str | None, str]] = []
    for tok in query.split():
        if ":" in tok:
            f, v = tok.split(":", 1)
            f = _QUERY_ALIASES.get(f.lower(), f.lower())
            if f in _QUERY_FIELDS and v:
                out.append((f, v.lower()))
                continue
        out.append((None, tok.lower()))
    return out


def _query_matches(spec: dict, snap_slug: dict, query: str) -> bool:
    """Whether `spec` satisfies every clause in `query` (AND across tokens)."""
    tokens = _parse_query(query)
    if not tokens:
        return True
    for field, val in tokens:
        if field:
            hay = " ".join(_query_field_values(field, spec, snap_slug)).lower()
        else:
            # Bare token: pool every field's strings.
            chunks: list[str] = []
            for f in _QUERY_FIELDS:
                chunks.extend(_query_field_values(f, spec, snap_slug))
            hay = " ".join(chunks).lower()
        if val not in hay:
            return False
    return True


def _trait_state_to_filter(trait_states: dict[str, str]) -> FilterState:
    """Convert {flag: "yes"|"no"|"unknown"} → FilterState fragment.

    Used by the Shape-traits facet group. "unknown" leaves the flag out of
    both `trait` and `trait_negated`, i.e. don't filter on this trait.
    """
    state = FilterState()
    for flag, val in trait_states.items():
        if val == "yes":
            state.trait.add(flag)
        elif val == "no":
            state.trait_negated.add(flag)
    return state


def _combine_filters(a: FilterState, b: FilterState) -> FilterState:
    """Union of two FilterStates across every axis. b takes precedence for vortex."""
    out = FilterState()
    out.showcase = a.showcase | b.showcase
    out.tag = a.tag | b.tag
    out.size = a.size | b.size
    out.trait = a.trait | b.trait
    out.trait_negated = a.trait_negated | b.trait_negated
    out.license = a.license | b.license
    out.fetch_type = a.fetch_type | b.fetch_type
    out.vortex = b.vortex if b.vortex is not None else a.vortex
    return out


def _live_vocabs(manifest: dict) -> dict[str, list[str]]:
    """Discover the values present in the live manifest for facets whose
    vocab isn't closed (license, fetch_type)."""
    licenses: set[str] = set()
    fetch_types: set[str] = set()
    for spec in manifest.get("datasets", []):
        lic = (spec.get("license") or {}).get("spdx")
        if lic:
            licenses.add(lic)
        ft = (spec.get("fetch") or {}).get("type")
        if ft:
            fetch_types.add(ft)
    return {
        "license": sorted(licenses),
        "fetch_type": sorted(fetch_types),
    }


def _radio_facet(axis: str, options: list[str]) -> RadioSet:
    """Build an exclusive (radio) facet for the side-bar panel.

    Renders an "(any)" option (default selected, no filter) plus one
    RadioButton per value. Button IDs use the option's position rather
    than its raw value, so option strings with dots or other glyphs that
    aren't valid Textual widget IDs (e.g. SPDX licenses like "CC-BY-SA-3.0")
    still work. State readers look up the pressed button's label, not its id.
    """
    buttons = [RadioButton("(any)", id=f"facet-{axis}-radio-any", value=True)]
    for i, v in enumerate(options):
        buttons.append(RadioButton(v, id=f"facet-{axis}-radio-{i}"))
    return RadioSet(*buttons, id=f"facet-{axis}", classes="facet-radio")

COLUMNS: tuple[tuple[str, str], ...] = (
    ("slug", "slug"),
    ("handler", "handler"),
    ("license", "license"),
    ("parquet", "parquet"),
    ("vortex", "vortex"),
    ("scrape", "scrape"),
    ("hydrate", "hydrate"),
    ("Tags", "tags"),
    ("Showcase", "showcase"),
    ("Size", "size_bucket"),
)


def _output_paths(spec: dict, manifest: dict) -> tuple[Path, Path]:
    """Resolve the on-disk locations the build pipeline writes to.

    Mirrors status.py: parquet under outputs/v{n}/<slug>/parquet/,
    vortex under outputs/v{n}/<slug>/vortex/.
    """
    return prepared_parquet(spec["slug"], manifest), prepared_vortex(spec["slug"], manifest)


def _hydrated_parquet(spec: dict, manifest: dict) -> Path:
    """Where a hydrated companion parquet would live (parquet-hydrated/ tier)."""
    return prepared_parquet_hydrated(spec["slug"], manifest)


def _vortex_cell(spec: dict, parquet: Path, vortex: Path) -> str:
    """Four states: not opted in (—), opted in & missing (·),
    opted in & stale vs parquet (⚠), opted in & fresh (✓)."""
    if not spec_field(spec, "convert.vortex", False):
        return "—"
    if not vortex.exists():
        return "·"
    if parquet.exists() and parquet.stat().st_mtime > vortex.stat().st_mtime:
        return "⚠"
    return "✓"


def _hydrate_cell(spec: dict, hydrated: Path) -> str:
    """Three states: not configured (—), configured & missing (·),
    configured & present (✓). The hydrate stage isn't implemented yet, so
    today the cell is — or · for every slug; ✓ becomes reachable once the
    hydrate stage starts populating outputs/v{n}/<slug>/parquet-hydrated/."""
    if not spec.get("hydrate"):
        return "—"
    return "✓" if hydrated.exists() else "·"


def _parquet_cell(parquet: Path) -> str:
    return "✓" if parquet.exists() else "·"


def _read_columns(parquet: Path) -> list[tuple[str, str]] | None:
    """Open the parquet's schema (footer-only — no data scan) and return
    `[(name, type), ...]`. None when the file isn't built or unreadable —
    caller renders a placeholder.
    """
    if not parquet.exists():
        return None
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(parquet)
        return [(f.name, str(f.type)) for f in pf.schema_arrow]
    except Exception:
        return None


def _load_snapshot() -> dict | None:
    """Read snapshot data, merging two sources per-slug:
      - docs/v1/snapshot.json — manually-promoted canonical (preferred)
      - docs/snapshot.json    — auto-gen working copy (fallback)
    For each slug, prefers whichever entry has data (parquet_bytes>0). If both
    have data the canonical version wins; if neither has data, the canonical
    placeholder wins. Returns None only when neither file exists or parses."""
    import json

    def _try_load(p):
        if not p.exists():
            return None
        try:
            return json.loads(p.read_text())
        except Exception:
            return None

    canonical = _try_load(REPO_ROOT / "docs" / "v1" / "snapshot.json")
    fresh = _try_load(REPO_ROOT / "docs" / "snapshot.json")
    if canonical is None and fresh is None:
        return None
    if canonical is None:
        return fresh
    if fresh is None:
        return canonical
    def _entry_richness(e: dict | None) -> tuple[int, int, int]:
        """Sort key for picking the richer of two snapshot entries.
        Prefer (in order): has parquet_bytes, has last_built_rows, has
        per-column min/max stats. Higher tuple wins."""
        if not e:
            return (0, 0, 0)
        has_parquet = 1 if e.get("parquet_bytes") is not None else 0
        has_last = 1 if e.get("last_built_rows") is not None else 0
        cols = e.get("columns") or []
        has_stats = 1 if any(c.get("min") is not None for c in cols) else 0
        return (has_parquet, has_last, has_stats)

    merged = dict(canonical)
    merged_slugs = dict(canonical.get("slugs") or {})
    for slug, entry in (fresh.get("slugs") or {}).items():
        existing = merged_slugs.get(slug)
        if _entry_richness(entry) > _entry_richness(existing):
            merged_slugs[slug] = entry
    merged["slugs"] = merged_slugs
    return merged


def _resolve_columns(slug: str, manifest: dict, snapshot: dict | None
                     ) -> tuple[list[tuple[str, str]] | None, str | None]:
    """Resolve a slug's columns from local parquet first, then snapshot.
    Returns (columns, source) where source ∈ {"parquet", "snapshot", None}.
    """
    cols = _read_columns(prepared_parquet(slug, manifest))
    if cols is not None:
        return cols, "parquet"
    if snapshot:
        entry = (snapshot.get("slugs") or {}).get(slug)
        if entry and entry.get("columns"):
            return [(c["name"], c["type"]) for c in entry["columns"]], "snapshot"
    return None, None


def _resolve_stats(slug: str, manifest: dict, snapshot: dict | None
                   ) -> tuple[list[dict] | None, str | None]:
    """Like _resolve_columns but returns the richer stats dict shape used
    by the Columns modal. Snapshot fallback returns whatever per-column
    metadata the snapshot captured at build time — full {length, null_count,
    min, max} for entries written by current docs.py / build_loop, or
    schema-only for older entries (those degrade to None for the stat
    fields)."""
    stats = _read_column_stats(prepared_parquet(slug, manifest))
    if stats is not None:
        return stats, "parquet"
    if snapshot:
        entry = (snapshot.get("slugs") or {}).get(slug)
        if entry and entry.get("columns"):
            out: list[dict] = []
            for c in entry["columns"]:
                out.append({
                    "name": c.get("name"),
                    "type": c.get("type"),
                    "length": c.get("length"),
                    "null_count": c.get("null_count"),
                    "min": c.get("min"),
                    "max": c.get("max"),
                })
            return out, "snapshot"
    return None, None


def _read_column_stats(parquet: Path) -> list[dict] | None:
    """Thin wrapper around `spec.read_column_stats` for in-module reuse."""
    from .spec import read_column_stats
    return read_column_stats(parquet)


def _required_extras(spec: dict) -> list[str]:
    """Optional pyproject extras the build needs based on fetch.type.

    Handler-specific format deps (pandas, openpyxl, pyreadstat, osmium,
    zstandard, py7zr, unlzw3) all live in core deps, so the only extras
    we ever need to pull in on demand are the upstream-fetch backends:
    `kaggle` for fetch.type=kaggle, `huggingface` for fetch.type=huggingface.
    Returns [] for http / custom — no sync needed before build.
    """
    ftype = (spec.get("fetch") or {}).get("type")
    if ftype == "kaggle":      return ["kaggle"]
    if ftype == "huggingface": return ["huggingface"]
    return []


def _uv_sync_command(extras: list[str]) -> list[str]:
    """Argv for `uv sync --extra X [--extra Y...] --inexact`."""
    cmd = ["uv", "sync"]
    for e in extras:
        cmd += ["--extra", e]
    cmd.append("--inexact")
    return cmd


def _build_time_estimate(spec: dict, snapshot: dict | None = None) -> str:
    """Heuristic build-time bracket. Prefers fetch.expected_bytes when set
    (most accurate, but rare in the manifest); falls back to expect.rows,
    then to the snapshot's last_built_rows; last resort is 'unknown'."""
    bytes_hint = (spec.get("fetch") or {}).get("expected_bytes")
    if bytes_hint:
        if bytes_hint < 100_000_000:    return "seconds"
        if bytes_hint < 1_000_000_000:  return "minutes"
        if bytes_hint < 10_000_000_000: return "tens of minutes"
        if bytes_hint < 100_000_000_000: return "hours"
        return "many hours / overnight"
    rows = (spec.get("expect") or {}).get("rows")
    if rows is None and snapshot:
        entry = (snapshot.get("slugs") or {}).get(spec["slug"]) or {}
        rows = entry.get("last_built_rows")
    if rows is None:
        return "unknown (no expect.rows, last_built_rows, or fetch.expected_bytes)"
    if rows < 100_000:    return "seconds"
    if rows < 10_000_000: return "minutes"
    if rows < 100_000_000: return "tens of minutes"
    return "hours"


def _format_bytes(n: int | None) -> str:
    if n is None:
        return "—"
    if n < 1024:        return f"{n} B"
    if n < 1024**2:     return f"{n/1024:.1f} KB"
    if n < 1024**3:     return f"{n/1024**2:.1f} MB"
    return f"{n/1024**3:.2f} GB"


def _cell_weight(ch: str) -> int:
    """*Pessimistic* per-codepoint rendered-cell weight.

    The simple `max(1, cell_len(ch))` rule held for CJK (fullwidth=2) and
    Latin / combining marks (1 each), but **not** for Indic scripts in
    real terminals: Sinhala / Devanagari letters report `cell_len=1`, yet
    terminal font fallback for those scripts routinely paints each codepoint
    at ~1.2-1.5 cells (the glyphs from the fallback font aren't strictly
    monospace). A 57-codepoint Sinhala value with pessimistic-count 57
    paints at ~71 cells — overflowing a 79-cell pane by 1-2 cells visually,
    which is exactly the bug the user reported.

    Rule:
      - ASCII (cp ≤ 127): `max(1, cell_len(ch))` — never widened.
      - non-ASCII letter / mark (Unicode L* or M* category):
          `max(2, cell_len(ch))` — counted at ≥2 cells so font-fallback /
          shaping widening can't break the budget. Covers Indic / Arabic
          / Hebrew / Latin extensions / CJK (already 2).
      - everything else non-ASCII (block glyphs, arrows, em dashes, …):
          `max(1, cell_len(ch))` — these are typically standard halfwidth
          and reliably monospace.
    """
    import unicodedata

    from rich.cells import cell_len
    cl = cell_len(ch)
    if ord(ch) > 127 and unicodedata.category(ch)[0] in ("L", "M"):
        return max(2, cl)
    return max(1, cl)


def _render_len(s: str) -> int:
    """Pessimistic rendered-cell sum across `s`; see `_cell_weight`."""
    return sum(_cell_weight(ch) for ch in s)


def _truncate_render_cells(s: str, max_cells: int) -> str:
    """Truncate by pessimistic rendered-cell budget. Mate of `_render_len`."""
    out: list[str] = []
    cells = 0
    for ch in s:
        ch_w = _cell_weight(ch)
        if cells + ch_w > max_cells:
            break
        out.append(ch)
        cells += ch_w
    return "".join(out)


_ELLIPSIS_CELLS = 2   # `…` (U+2026) is East-Asian Width Ambiguous; some
                      # terminals + CJK locales paint it at 2 cells.


def _format_stat(v: Any, *, max_cells: int = 18) -> str:
    """Render a min/max value safely; truncate by *pessimistic* rendered-cell
    width so wide-glyph / CJK / RTL / combining-mark values can't paint past
    the column-detail pane's right edge. Replaces control chars so DataTable
    rows don't blow up.

    Reserves `_ELLIPSIS_CELLS` for the truncation marker `…` (which is
    ambiguous-width and can paint at 2 cells in CJK locales). The default
    18 cells is sized so `label(≤6) + pad(≥4) + value(≤18) = 28` fits
    inside the right pane of the Columns modal even on an 80-cell terminal
    (modal box ≈ 76 cells, pane ≈ 46 cells, content area ≈ 42)."""
    if v is None:
        return "—"
    s = str(v).replace("\n", " ").replace("\r", " ").replace("\t", " ")
    if _render_len(s) <= max_cells:
        return s
    return _truncate_render_cells(s, max_cells - _ELLIPSIS_CELLS) + "…"


def _per_slug_type_coverage(stats: list[dict]) -> list[dict]:
    """Aggregate per-column stats into per-canonical-type rows for the
    Types modal. Mirrors `list_datasets --coverage` but scoped to one slug.
    """
    by_type: dict[str, list[str]] = {}
    for s in stats:
        canon = _canonicalize_type(s["type"])
        by_type.setdefault(canon, []).append(s["name"])
    out = []
    for ty in sorted(by_type):
        cols = by_type[ty]
        out.append({"type": ty, "count": len(cols), "examples": cols[:5]})
    return out


def _resolve_rows(spec: dict, snapshot: dict | None) -> tuple[str, str]:
    """Return (display_string, source) for the rows count. Source is one of
    'expect' / 'last-seen' / '—'. Falls back to snapshot's last_built_rows
    when expect.rows is null but a prior build captured an actual count."""
    rows = (spec.get("expect") or {}).get("rows")
    if isinstance(rows, int):
        return f"{rows:,}", "expect"
    if snapshot:
        entry = (snapshot.get("slugs") or {}).get(spec["slug"]) or {}
        last = entry.get("last_built_rows")
        if isinstance(last, int):
            return f"{last:,} [dim](last seen)[/dim]", "last-seen"
    return "—", "—"


_STABILITY_HINT = {
    "static":    "[dim](static)[/dim]",
    "grow_only": "[dim](grow-only)[/dim]",
    "mutable":   "[dim](mutable)[/dim]",
}


def _references_block(spec: dict) -> str:
    """Render `references` as one URL per line, kind-prefixed. Empty when
    none are set."""
    refs = spec.get("references") or []
    if not refs:
        return ""
    lines = "\n".join(f"  [dim]{r['kind']}:[/dim] {r['url']}" for r in refs)
    return f"[b]references[/b]\n{lines}\n"


_HIST_LADDER = " ▁▂▃▄▅▆▇█"


def _render_block_histogram(counts: list[int], *, rows: int, bar_cells: int) -> list[str]:
    """Multi-row block-glyph bar chart for a list of non-negative counts.

    `rows`-tall, with each bar `bar_cells` wide (1-cell gap between bars).
    The block-glyph ladder is 8 levels per row, so total resolution is
    `8 * rows` quantization steps. Empty counts → empty list."""
    if not counts:
        return []
    hi = max(counts)
    if hi <= 0:
        return []
    units_per_row = len(_HIST_LADDER) - 1   # 8 sub-levels per row
    total_units = rows * units_per_row
    fills = [int(round((c / hi) * total_units)) for c in counts]
    out: list[str] = []
    for r in range(rows - 1, -1, -1):
        row_chars: list[str] = []
        for f in fills:
            row_fill = max(0, min(units_per_row, f - r * units_per_row))
            row_chars.append(_HIST_LADDER[row_fill])
        # bar_cells = wide bar; one trailing space separates bars.
        out.append("".join(c * bar_cells + " " for c in row_chars).rstrip())
    return out


def _format_axis_value(v: Any) -> str:
    """Format a histogram bucket edge for an x-axis tick label. Numbers in
    `[1, 100_000)` get comma-separated standard form with up to 3 sig figs
    after the leading digit (`1.23`, `12.3`, `123`, `1,234`, `12,345`);
    everything else falls through to `:.3g` so very small or very large
    floats compress to scientific. ISO timestamps truncate to their date
    portion."""
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, int):
        return f"{v:,}"
    if isinstance(v, float):
        import math
        av = abs(v)
        if 1 <= av < 100_000 and math.isfinite(v):
            decimals = max(0, 2 - int(math.floor(math.log10(av))))
            return f"{v:,.{decimals}f}"
        return f"{v:.3g}"
    s = str(v)
    if len(s) >= 10 and s[4:5] == "-" and s[7:8] == "-":
        return s[:10]
    return s


def _render_x_axis_ticks(buckets: list, *, chart_cells: int) -> str:
    """Render `lo … mid … hi` axis ticks across `chart_cells` cells.

    `buckets` is the n+1 bin-edge list from the profile (so a 10-bin
    histogram has 11 edges); the labels come from edges 0, len/2, len-1.
    Falls back to `lo  →  hi` when there isn't room for all three."""
    if len(buckets) < 2 or chart_cells <= 0:
        return ""
    lo = _format_axis_value(buckets[0])
    mid = _format_axis_value(buckets[len(buckets) // 2])
    hi = _format_axis_value(buckets[-1])
    needed = len(lo) + len(mid) + len(hi) + 2   # ≥1 space between each
    if needed > chart_cells:
        # Drop the mid tick if there's no room.
        if len(lo) + len(hi) + 5 <= chart_cells:
            pad = " " * (chart_cells - len(lo) - len(hi) - 4)
            return f"{lo}  →{pad}{hi}"
        return f"{lo}  →  {hi}"
    # Distribute remaining cells: half before mid, half after.
    gap = chart_cells - len(lo) - len(mid) - len(hi)
    left = gap // 2
    right = gap - left
    return f"{lo}{' ' * left}{mid}{' ' * right}{hi}"


def _ljust_cells(s: str, target_cells: int) -> str:
    """Left-justify `s` to `target_cells` *pessimistic* render cells with
    trailing ASCII spaces. If `_render_len(s)` already exceeds the target,
    returns `s` unchanged (caller is expected to have pre-truncated)."""
    cells = _render_len(s)
    if cells >= target_cells:
        return s
    return s + " " * (target_cells - cells)


def _render_top_value_bars(
    top: list[dict], *, value_cells: int, bar_cells: int, count_cells: int,
) -> list[str]:
    """Horizontal-bar chart of `top` value/count pairs.

    Each row: `<value padded to value_cells+2>  <bar padded to bar_cells>  <count right-justified to count_cells>`.
    Bars are proportional to count / max(counts). The +2 on the value column is
    for the surrounding `'…'` repr quotes."""
    if not top:
        return []
    max_count = max(t["count"] for t in top)
    out: list[str] = []
    for t in top:
        v = _format_stat(t["value"], max_cells=value_cells)
        v_repr = repr(v)
        v_col = _ljust_cells(v_repr, value_cells + 2)
        fill = int(round((t["count"] / max_count) * bar_cells)) if max_count else 0
        bar = ("█" * fill).ljust(bar_cells)
        c_str = f"{t['count']:,}".rjust(count_cells)
        out.append(f"{v_col}  {bar}  {c_str}")
    return out


def _render_column_detail(name: str, spec_stat: dict | None,
                          profile_col: dict | None,
                          *, pane_cells: int = 36,
                          profile_loaded: bool = False) -> str:
    """Multi-line markup for the right pane of the Columns modal.

    Combines parquet schema stats (nulls + row-group min/max from `spec_stat`)
    with profile.json data (histogram / NDV / top-K / bool counts / length
    stats). Tailors output to dtype so each shape gets a useful detail view.

    `pane_cells` is the current cell width of the right pane; min/max
    rendering, top-values, and the histogram bar chart all scale to it.
    """
    if spec_stat is None and profile_col is None:
        return "[dim](no data)[/dim]"

    # Slack cells reserved off every line budget. `_render_len` is a
    # *pessimistic* cell estimate per codepoint, but several effects can
    # still push painted width beyond the prediction:
    #   - Mid-grapheme-cluster truncation (a base char without its mark).
    #   - East-Asian ambiguous-width chars painted as 2 cells in CJK locales.
    #   - Sinhala / Devanagari grapheme clusters whose reshaping renders
    #     wider than the codepoint-sum estimate.
    #   - Static widget's internal cell-len measurement disagreeing with
    #     the terminal's actual paint width (so Textual decides "no wrap"
    #     and we leak past the pane edge).
    # The slack has crept upward each time a new script broke the prior
    # estimate; 11 cells is a safety budget chosen after Sinhala still
    # overflowed at 6.
    slack = 11
    budget = max(14, pane_cells - slack)

    # Derived cell budgets. label+pad is 10 cells; value content gets the
    # rest, capped at 60 (beyond which longer min/max stops being useful).
    value_cells = max(14, min(budget - 10, 60))
    # Histogram bar geometry — derived from `budget` assuming 10 bins per
    # profile with one cell of inter-bar gap (total = 10 * (bar+1) - 1).
    # Clamp to [1, 6] cells per bar; beyond 6 looks chunky.
    n_bins = 10
    bar_cells = max(1, min(6, (budget - (n_bins - 1)) // n_bins))

    dtype = (spec_stat or {}).get("type") or (profile_col or {}).get("dtype") or "?"
    lines: list[str] = [f"[b]{name}[/b]  [dim]{dtype}[/dim]", ""]

    def _kv(label: str, value: str) -> list[str]:
        """Render `label  value` on a single line. Callers are responsible
        for cell-bounding `value` (typically via `_format_stat`) — the
        previous multi-line chunk-wrap form used codepoint slicing, which
        mis-measured wide-glyph content and let lines paint past the
        modal's right edge."""
        pad = " " * max(1, 10 - len(label))
        return [f"{label}{pad}{value}"]

    if spec_stat:
        nulls = spec_stat.get("null_count")
        if nulls is not None:
            lines += _kv("nulls:", f"{nulls:,}")
        if spec_stat.get("min") is not None:
            lines += _kv("min:", _format_stat(spec_stat["min"], max_cells=value_cells))
        if spec_stat.get("max") is not None:
            lines += _kv("max:", _format_stat(spec_stat["max"], max_cells=value_cells))

    if profile_col is None:
        if profile_loaded:
            # The slug's profile.json was loaded but this column's entry is
            # `null`. `profile.py` intentionally returns null at the
            # column-map level for shapes it can't usefully aggregate per
            # element: struct, variant, list-of-struct, and all-null
            # columns. Don't ask the user to re-run profile — they'd get
            # the same answer.
            lines += [
                "",
                "[dim]No per-element distribution: `profile.py` skips this "
                "column's shape (typically struct / variant / all-null).[/dim]",
            ]
        else:
            lines += [
                "",
                "[dim]No profile yet — run "
                "[b]python -m scripts.pipeline.profile <slug>[/b] "
                "to populate per-column distribution stats.[/dim]",
            ]
        return "\n".join(lines)

    if "histogram" in profile_col:
        ndv = profile_col.get("ndv_approx")
        mean = profile_col.get("mean")
        if ndv is not None:
            lines.append(f"NDV≈:     {ndv:,}")
        if mean is not None:
            lines.append(f"mean:     {mean:.4g}")
        counts = profile_col["histogram"].get("counts") or []
        bars = _render_block_histogram(counts, rows=5, bar_cells=bar_cells)
        lines += ["", "[b]distribution[/b]", *bars]
        # 3-tick x-axis (lo / mid / hi) aligned with the chart's total
        # width. Falls back to `lo → hi` (or just `lo  →  hi`) on narrow
        # panes where mid won't fit.
        buckets = profile_col["histogram"].get("buckets") or []
        chart_cells = max(0, n_bins * (bar_cells + 1) - 1)
        if buckets and chart_cells > 0:
            ticks = _render_x_axis_ticks(buckets, chart_cells=chart_cells)
            if ticks:
                lines.append(f"[dim]{ticks}[/dim]")
    elif "ndv_approx" in profile_col:
        ndv = profile_col["ndv_approx"]
        lines.append(f"NDV≈:     {ndv:,}")
        mean_len = profile_col.get("mean_length")
        if mean_len is not None:
            lines.append(f"avg_len:  {mean_len:.1f}")
        top = profile_col.get("top_values") or []
        if top:
            # Horizontal bar chart per top value. Allocate cell columns
            # within `budget`: value (with quotes) + 2 sep + bar + 2 sep +
            # right-justified count.
            count_strs = [f"{t['count']:,}" for t in top[:5]]
            count_cells = max(len(s) for s in count_strs)
            # Value column ≈ a third of the budget, capped at 20 cells.
            # The +2 in `value_w + 2` accounts for repr quotes.
            value_w = max(6, min(20, budget // 3))
            # Cap the bar column at 36 cells — wider bars become more
            # visual clutter than communication. Past ~36 cells the user
            # already sees the proportional shape (e.g. 6:1 train/test).
            bar_w = max(3, min(36, budget - (value_w + 2) - 2 - 2 - count_cells))
            lines += [
                "",
                "[b]top values[/b]",
                *_render_top_value_bars(
                    top[:5],
                    value_cells=value_w,
                    bar_cells=bar_w,
                    count_cells=count_cells,
                ),
            ]
    elif "true_count" in profile_col:
        t = profile_col["true_count"]
        f = profile_col["false_count"]
        n = profile_col.get("null_count", 0)
        total = t + f + n
        def _pct(c: int) -> str:
            return f"  ({100 * c / total:.1f}%)" if total else ""
        lines += [
            "",
            f"true:    {t:>10,}{_pct(t)}",
            f"false:   {f:>10,}{_pct(f)}",
            f"null:    {n:>10,}{_pct(n)}",
        ]
    elif "length_min" in profile_col:
        lines += [
            "",
            "[b]length[/b]",
            f"min:      {profile_col['length_min']:,}",
            f"max:      {profile_col['length_max']:,}",
            f"mean:     {profile_col['length_mean']:.2f}",
        ]

    return "\n".join(lines)


def _columns_block(spec: dict, columns: list[tuple[str, str]] | None) -> str:
    """Render the `[b]columns[/b]` section for the detail pane.

    Three states:
      - None             — parquet not built; render an empty placeholder
      - empty list       — parquet built but no top-level columns (shouldn't
                           happen in practice; rendered as "—")
      - non-empty list   — render `name: type` per column
    """
    if columns is None:
        return "[b]columns[/b]   [dim](parquet not built — press [b]c[/b] for build hint)[/dim]\n"
    if not columns:
        return "[b]columns[/b]   —\n"
    body = "\n".join(f"  {name}: [dim]{ty}[/dim]" for name, ty in columns)
    return f"[b]columns[/b]   ({len(columns)})\n{body}\n"


def _row(spec: dict, parquet_cell: str, vortex_cell: str,
         hydrate_cell: str, *, snapshot: dict | None = None) -> tuple[str, ...]:
    """Build one DataTable row from a spec + its snapshot record.

    Returns cell values matching the order of COLUMNS. The `snapshot` kwarg
    carries per-slug snapshot data (e.g. `size_bucket`) for fields that aren't
    on the spec itself; callers outside `_rebuild_table` can omit it.
    """
    snapshot = snapshot or {}
    scrape_cell = "⚠" if spec_field(spec, "license.scrape_advisory") else "·"
    tags_cell = ", ".join(spec.get("tags") or []) or "·"
    showcase_cell = ", ".join(spec.get("showcase") or []) or "·"
    size_bucket_cell = snapshot.get("size_bucket") or "·"
    return (
        spec["slug"],
        spec_field(spec, "transform.handler") or "",
        spec_field(spec, "license.spdx") or "",
        parquet_cell,
        vortex_cell,
        scrape_cell,
        hydrate_cell,
        tags_cell,
        showcase_cell,
        size_bucket_cell,
    )


def _detail(spec: dict, parquet_cell: str, vortex_cell: str,
            hydrate_cell: str,
            columns: list[tuple[str, str]] | None = None,
            snapshot: dict | None = None) -> str:
    lic = spec.get("license") or {}
    fetch = spec.get("fetch") or {}
    parse = spec.get("parse") or {}
    transform = spec.get("transform") or {}
    expect = spec.get("expect") or {}
    convert = spec.get("convert") or {}
    hydrate = spec.get("hydrate")

    rows_str, _ = _resolve_rows(spec, snapshot)
    stability = expect.get("row_stability")
    stability_hint = f"  {_STABILITY_HINT.get(stability, '')}" if stability else ""

    full_name = spec.get("full_name") or spec.get("short_name") or spec["slug"]
    description = spec.get("description") or "—"
    scrape_advisory = lic.get("scrape_advisory")
    vortex_skip_reason = convert.get("vortex_skip_reason")

    urls = fetch.get("urls") or []
    url_block = "\n".join(f"    {u}" for u in urls) if urls else "    —"

    parquet_state = {"✓": "present", "·": "missing"}[parquet_cell]
    vortex_state = {
        "✓": "present",
        "·": "missing",
        "⚠": "present (stale vs parquet)",
        "—": "not opted in",
    }[vortex_cell]
    vortex_opt = "on" if convert.get("vortex") else "off"
    hydrate_state = {
        "✓": "present",
        "·": "configured (file missing)",
        "—": "not configured",
    }[hydrate_cell]

    advisory_block = (
        f"\n[red]⚠ scrape advisory[/red]\n[red]{scrape_advisory}[/red]\n"
        if scrape_advisory else ""
    )
    skip_block = (
        f"[dim]vortex skipped: {vortex_skip_reason}[/dim]\n"
        if vortex_skip_reason else ""
    )
    hydrate_block = (
        f"[b]hydrate[/b]   {hydrate_state}  "
        f"[dim]({hydrate['url_column']} → {hydrate['output_column']}: "
        f"{hydrate['output_type']})[/dim]\n"
        f"[dim red]{hydrate['advisory']}[/dim red]\n"
        if hydrate else ""
    )
    columns_section = _columns_block(spec, columns)
    refs_section = _references_block(spec)
    return (
        f"[b]{spec['slug']}[/b]\n"
        f"[dim]{full_name}[/dim]\n"
        f"\n"
        f"{description}\n"
        f"\n"
        f"[b]license[/b]   {lic.get('spdx') or '—'}\n"
        f"[b]rows[/b]      {rows_str}{stability_hint}\n"
        f"{advisory_block}"
        f"\n"
        f"[b]parquet[/b]   {parquet_state}\n"
        f"[b]vortex[/b]    {vortex_state}  [dim](opt-in: {vortex_opt})[/dim]\n"
        f"{skip_block}"
        f"{hydrate_block}"
        f"\n"
        f"{columns_section}"
        f"\n"
        f"[b]fetch[/b]     {fetch.get('type') or '—'}\n"
        f"{url_block}\n"
        f"\n"
        f"[b]parse[/b]     reader = {parse.get('reader') or '—'}\n"
        f"[b]transform[/b] handler = {transform.get('handler') or '—'}\n"
        f"{refs_section}"
    )


# ---------- Modal screens ----------

class _DatasetModal(ModalScreen):
    """Shared base for ColumnsModal and TypesModal: same overlay framing,
    same close-on-esc/q binding. Subclasses fill in the body."""

    DEFAULT_CSS = """
    _DatasetModal {
        align: center middle;
    }
    _DatasetModal > #modal-box {
        width: 95%;
        height: 92%;
        background: $surface;
        /* No outline / border — wide-glyph content (CJK / RTL / combining
           marks) reliably found ways to paint past the modal's frame on
           several terminals, leaving a half-broken yellow box that looked
           worse than no frame at all. The `$surface` background contrast
           against the dimmed app body is enough modal affordance. */
        padding: 1 2;
    }
    _DatasetModal #modal-title {
        height: auto;
        margin-bottom: 1;
    }
    _DatasetModal #modal-footer {
        dock: bottom;
        height: auto;
        margin-top: 1;
    }
    _DatasetModal DataTable {
        height: 1fr;
        width: 1fr;
    }
    ColumnsModal #col-list {
        width: 24;
        height: 1fr;
        margin-right: 2;
    }
    ColumnsModal #col-detail-wrap {
        width: 1fr;
        height: 1fr;
    }
    ColumnsModal #col-detail {
        height: auto;
        width: 1fr;
        overflow-x: hidden;
    }
    ColumnsModal #col-pane {
        width: 1fr;
        height: 1fr;
        overflow: hidden;
    }
    """

    BINDINGS = [
        Binding("escape", "dismiss", "close"),
        Binding("q", "dismiss", "close"),
    ]


def _dedupe_stat_names(stats: list[dict]) -> list[dict]:
    """Suffix repeated `name` values with ` (2)`, ` (3)`, etc. so each entry is uniquely keyable."""
    seen: dict[str, int] = {}
    out: list[dict] = []
    for s in stats:
        name = s.get("name")
        count = seen.get(name, 0) + 1
        seen[name] = count
        if count == 1:
            out.append(s)
        else:
            new = dict(s)
            new["name"] = f"{name} ({count})"
            out.append(new)
    return out


class ColumnsModal(_DatasetModal):
    """Full per-column metadata for one slug. When the local parquet isn't
    built, falls back to docs/v1/snapshot.json so the modal can still show
    *expected* columns (without per-row-group stats — those need the actual
    file). When neither is available, renders an instructional placeholder
    with the build command + a heuristic time estimate."""

    def __init__(self, slug: str, spec: dict,
                 stats: list[dict] | None,
                 source: str | None = None,
                 profile: dict | None = None) -> None:
        super().__init__()
        self.slug = slug
        self.spec = spec
        # Some slugs (osmi-* surveys, uci-spambase, uk-price-paid) carry
        # legitimately duplicated top-level column names. Suffix repeats so
        # the DataTable row key is unique and stats_by_name doesn't collapse.
        self.stats = _dedupe_stat_names(stats) if stats is not None else None
        self.source = source  # "parquet" | "snapshot" | None
        # profile.json keyed-by-column-name. Empty when the profile stage
        # hasn't been run; the right detail pane then renders "no profile".
        self.profile_columns = (profile or {}).get("columns") or {}
        # parquet schema stats keyed by name for O(1) lookup from the detail pane.
        self.stats_by_name = {s["name"]: s for s in (self.stats or [])}

    def compose(self) -> ComposeResult:
        suffix = " [dim](from snapshot)[/dim]" if self.source == "snapshot" else ""
        with Container(id="modal-box"):
            yield Static(f"[b]Columns: {self.slug}[/b]{suffix}", id="modal-title")
            if self.stats is None:
                est = _build_time_estimate(self.spec)
                yield Static(
                    f"[dim]The parquet for [b]{self.slug}[/b] isn't built locally, "
                    f"and no snapshot entry was found.[/dim]\n\n"
                    f"Estimated build time: [b]{est}[/b]\n\n"
                    f"To build it, run from the repo root:\n\n"
                    f"  [reverse] python -m scripts.pipeline.build {self.slug} [/reverse]\n\n"
                    f"[dim](See SKILLS.md \"Running a large build safely\" for memory + nohup guidance on bigger slugs.)[/dim]"
                )
            else:
                with Horizontal(id="col-pane"):
                    table = DataTable(id="col-list", cursor_type="row", zebra_stripes=True)
                    table.add_columns("column", "type")
                    for s in self.stats:
                        table.add_row(s["name"], s["type"], key=s["name"])
                    yield table
                    with VerticalScroll(id="col-detail-wrap"):
                        yield Static(id="col-detail")
            yield Static("[dim]esc / q to close[/dim]", id="modal-footer")

    def on_mount(self) -> None:
        # Defer the first render to *after* the next layout pass — at
        # on_mount, the col-detail Static may not have its size computed
        # yet (size.width can read as 0 or the full screen width), which
        # would either under- or over-estimate the pane budget. By the
        # time `call_after_refresh` fires, layout has settled.
        if self.stats:
            self.call_after_refresh(self._render_detail, self.stats[0]["name"])

    def on_data_table_row_highlighted(self, event) -> None:
        key = event.row_key.value if event.row_key is not None else None
        if key:
            self._render_detail(key)

    def on_resize(self, event) -> None:
        # Re-render the active column whenever the modal resizes so the
        # histogram and min/max caps reflect the new pane width.
        try:
            table = self.query_one("#col-list", DataTable)
        except Exception:
            return
        if table.cursor_row is not None and table.cursor_row < len(self.stats or []):
            self._render_detail(self.stats[table.cursor_row]["name"])

    def _render_detail(self, name: str) -> None:
        spec_stat = self.stats_by_name.get(name)
        prof = self.profile_columns.get(name)
        try:
            target = self.query_one("#col-detail", Static)
        except Exception:
            return
        # Compute the pane width from the screen width and the known CSS
        # geometry instead of reading `target.size.width`. The latter is
        # unreliable for `width: 1fr` widgets — Textual sometimes reports
        # the natural content width (which can be the longest line of the
        # current markup, ~3x the actual flex-allocated width). That made
        # us render values much wider than the pane could hold, leaking
        # past the right edge.
        try:
            screen_w = self.app.size.width
        except Exception:
            screen_w = 80
        # CSS path: _DatasetModal #modal-box is width 95% with padding 1 2
        # (4 cells horizontal); ColumnsModal #col-list is width 24 with
        # margin-right 2; col-detail-wrap (and #col-detail inside it) take
        # the remaining 1fr.
        pane_cells = max(24, int(screen_w * 0.95) - 4 - 24 - 2)
        # Pass the markup string directly — Static's own renderer wraps at the
        # widget width, which is what we actually want. `Text.from_markup` +
        # `overflow="fold"` was wrapping at Console width (full terminal) and
        # letting wide lines bleed past the right border of the pane.
        # `bool(self.profile_columns)` is the "did profile.py run" signal —
        # an empty dict means no slug-level profile was loaded, so a null
        # `prof` for this column means "run profile.py"; a non-empty dict
        # means the slug WAS profiled and this column was intentionally
        # skipped.
        target.update(_render_column_detail(
            name, spec_stat, prof,
            pane_cells=pane_cells,
            profile_loaded=bool(self.profile_columns),
        ))


class BuildConfirmModal(_DatasetModal):
    """Read-only confirmation overlay before launching a build subprocess.

    Renders the slug's license + scrape advisory (in red, when set) +
    description + estimated build time + the exact command line. A second
    keypress (Enter or `b`) confirms; `esc` / `q` cancels.

    On confirm, dismisses with `True`. The caller observes the dismiss value
    and pushes a BuildLogModal to actually run the build.
    """

    BINDINGS = [
        Binding("escape", "dismiss", "cancel"),
        Binding("q", "dismiss", "cancel"),
        Binding("enter", "confirm", "build"),
        Binding("b", "confirm", "build"),
    ]

    def __init__(self, slug: str, spec: dict, snapshot: dict | None = None) -> None:
        super().__init__()
        self.slug = slug
        self.spec = spec
        self.snapshot = snapshot

    def compose(self) -> ComposeResult:
        spec = self.spec
        lic = spec.get("license") or {}
        advisory = lic.get("scrape_advisory")
        full_name = spec.get("full_name") or spec.get("short_name") or self.slug
        description = spec.get("description") or "—"
        spdx = lic.get("spdx") or "—"
        license_notes = lic.get("notes")
        rows_str, _src = _resolve_rows(spec, self.snapshot)
        if rows_str == "—":
            rows_str = "unknown"
        est = _build_time_estimate(spec, self.snapshot)

        license_block = (
            f"[b]license[/b]    {spdx}\n"
            + (f"[dim]  {license_notes}[/dim]\n" if license_notes else "")
        )
        advisory_block = (
            f"\n[red]⚠ scrape advisory[/red]\n[red]{advisory}[/red]\n"
            if advisory else ""
        )

        extras = _required_extras(spec)
        sync_line = (
            f"  [reverse] {' '.join(_uv_sync_command(extras))} [/reverse]\n"
            if extras else ""
        )
        sync_note = (
            f"[dim]First syncs the {'/'.join(extras)} extra (preserving any "
            f"others installed) so the {(spec.get('fetch') or {}).get('type')} "
            f"backend is available; then runs the build.[/dim]\n\n"
            if extras else ""
        )

        body = (
            f"[dim]{full_name}[/dim]\n\n"
            f"{description}\n\n"
            f"{license_block}"
            f"[b]rows[/b]       {rows_str}\n"
            f"[b]est. time[/b]  {est}\n"
            f"{advisory_block}\n"
            f"Will run from the repo root:\n"
            f"{sync_line}"
            f"  [reverse] python -m scripts.pipeline.build {self.slug} [/reverse]\n\n"
            f"{sync_note}"
            f"[dim]The TUI will stream the subprocess output. Cancelling the "
            f"build modal terminates the subprocess. Quitting the TUI "
            f"orphans any in-flight builds — use the CLI for hours-long "
            f"runs you want to keep going in the background.[/dim]"
        )

        with Container(id="modal-box"):
            yield Static(f"[b]Build: {self.slug}[/b]", id="modal-title")
            with VerticalScroll():
                yield Static(body, id="modal-body")
            yield Static(
                "[b]Enter[/b] / [b]b[/b] to launch build  ·  "
                "[b]esc[/b] / [b]q[/b] to cancel",
                id="modal-footer",
            )

    def action_confirm(self) -> None:
        self.dismiss(True)


class BuildLogModal(ModalScreen):
    """Spawn `python -m scripts.pipeline.build <slug>` and stream output.

    Cancellation: dismissing the modal (esc/q) cancels the worker task,
    which SIGTERMs the subprocess (then SIGKILLs after a 5s grace).
    Quitting the App while a build is running orphans the subprocess —
    intentional, so users who close the TUI can still let the CLI build
    finish, but not generally what you want for hours-long jobs.
    """

    DEFAULT_CSS = """
    BuildLogModal {
        align: center middle;
    }
    BuildLogModal > #modal-box {
        width: 95%;
        height: 90%;
        background: $surface;
        padding: 1 2;
    }
    BuildLogModal #modal-title {
        height: auto;
        margin-bottom: 1;
    }
    BuildLogModal #status {
        height: auto;
        margin-top: 1;
    }
    BuildLogModal #modal-footer {
        dock: bottom;
        height: auto;
        margin-top: 1;
    }
    BuildLogModal RichLog {
        height: 1fr;
    }
    """

    BINDINGS = [
        Binding("escape", "request_close", "cancel + close"),
        Binding("q", "request_close", "cancel + close"),
    ]

    def __init__(self, slug: str, spec: dict | None = None) -> None:
        super().__init__()
        self.slug = slug
        self.spec = spec or {}
        self._process: asyncio.subprocess.Process | None = None
        self._task: asyncio.Task | None = None

    def compose(self) -> ComposeResult:
        with Container(id="modal-box"):
            yield Static(f"[b]Building {self.slug}[/b]", id="modal-title")
            yield RichLog(id="build-log", wrap=False, markup=False, highlight=False)
            yield Static("[yellow]running…[/yellow]", id="status")
            yield Static(
                "[dim]esc / q to cancel build + close[/dim]",
                id="modal-footer",
            )

    def on_mount(self) -> None:
        self._task = asyncio.create_task(self._run_build())

    async def _stream_subprocess(self, argv: list[str], log: "RichLog") -> int:
        """Spawn argv, stream stdout/stderr line-by-line into `log`, return exit code.
        Stores the process on self so cancellation can SIGTERM/SIGKILL it."""
        self._process = await asyncio.create_subprocess_exec(
            *argv,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            cwd=str(REPO_ROOT),
        )
        assert self._process.stdout is not None
        while True:
            line = await self._process.stdout.readline()
            if not line:
                break
            log.write(line.decode("utf-8", errors="replace").rstrip("\n"))
        return await self._process.wait()

    async def _run_build(self) -> None:
        log = self.query_one("#build-log", RichLog)
        status = self.query_one("#status", Static)
        try:
            extras = _required_extras(self.spec)
            if extras:
                sync_cmd = _uv_sync_command(extras)
                status.update(f"[yellow]syncing {'/'.join(extras)}…[/yellow]")
                log.write(f"$ {' '.join(sync_cmd)}")
                rc = await self._stream_subprocess(sync_cmd, log)
                if rc != 0:
                    status.update(f"[red]✗ uv sync failed (exit {rc}) — build skipped[/red]")
                    return
                log.write("")  # blank line between sync and build output
            status.update("[yellow]running…[/yellow]")
            build_cmd = [sys.executable, "-u", "-m", "scripts.pipeline.build", self.slug]
            log.write(f"$ {' '.join(build_cmd)}")
            rc = await self._stream_subprocess(build_cmd, log)
            if rc == 0:
                status.update("[green]✓ build succeeded[/green]")
            else:
                status.update(f"[red]✗ build failed (exit {rc})[/red]")
        except asyncio.CancelledError:
            if self._process is not None and self._process.returncode is None:
                self._process.terminate()
                try:
                    await asyncio.wait_for(self._process.wait(), 5)
                except asyncio.TimeoutError:
                    self._process.kill()
                    await self._process.wait()
            status.update("[yellow]cancelled[/yellow]")
            raise

    async def action_request_close(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self.dismiss()


class TypesModal(_DatasetModal):
    """Per-canonical-type aggregation for one slug — mirrors
    `list_datasets --coverage` but scoped to a single slug. Falls back to
    snapshot data when the local parquet isn't built (with a
    "(from snapshot)" indicator)."""

    def __init__(self, slug: str, spec: dict,
                 stats: list[dict] | None,
                 source: str | None = None) -> None:
        super().__init__()
        self.slug = slug
        self.spec = spec
        self.stats = stats
        self.source = source

    def compose(self) -> ComposeResult:
        suffix = " [dim](from snapshot)[/dim]" if self.source == "snapshot" else ""
        with Container(id="modal-box"):
            yield Static(f"[b]Types: {self.slug}[/b]{suffix}", id="modal-title")
            if self.stats is None:
                est = _build_time_estimate(self.spec)
                yield Static(
                    f"[dim]The parquet for [b]{self.slug}[/b] isn't built locally, "
                    f"and no snapshot entry was found.[/dim]\n\n"
                    f"Estimated build time: [b]{est}[/b]\n\n"
                    f"Build with [reverse] python -m scripts.pipeline.build {self.slug} [/reverse] then re-open this modal."
                )
            else:
                cov = _per_slug_type_coverage(self.stats)
                table = DataTable(zebra_stripes=True)
                table.add_columns("type", "columns", "examples")
                for r in cov:
                    examples = ", ".join(r["examples"])
                    if r["count"] > len(r["examples"]):
                        examples += f" (+{r['count'] - len(r['examples'])})"
                    table.add_row(r["type"], str(r["count"]), examples)
                yield table
            yield Static("[dim]esc / q to close[/dim]", id="modal-footer")


class DatasetBrowser(App):
    """Read-only viewer for sources.json."""

    TITLE = "raincloud · datasets"

    CSS = """
    Screen { layout: vertical; }
    #root-row { height: 1fr; }
    #facets {
        width: 28;
        height: 1fr;
        border-right: solid $accent;
        padding: 0 1;
    }
    #facets .facets-title {
        text-style: bold;
        padding: 0 0 1 0;
    }
    #counts-label {
        padding: 0 0 1 0;
        color: $text-muted;
    }
    #main-col { width: 1fr; height: 1fr; }
    #search-input {
        height: auto;
        margin: 0 1;
    }
    #body { height: 1fr; }
    #table { width: 60%; height: 1fr; }
    #detail {
        width: 40%;
        height: 1fr;
        padding: 1 2;
        border-left: solid $accent;
    }
    .facet-radio { height: auto; max-height: 12; }
    .facet-hint {
        color: $text-muted;
        padding: 0 0 1 0;
        height: auto;
        width: 1fr;
    }
    .trait-block {
        height: auto;
        padding: 0 0 1 0;
    }
    .trait-name {
        height: 1;
        padding: 0;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "quit"),
        Binding("c", "show_columns", "Columns"),
        Binding("t", "show_types", "Types"),
        Binding("b", "show_build_confirm", "Build"),
        Binding("f", "clear_facets", "Clear filters"),
        # `/` opens the search input. Textual accepts both "slash" and "/"
        # as key names; some older versions parse only the literal char.
        Binding("/", "focus_search", "Search"),
    ]

    def __init__(
        self,
        specs: list[dict] | None = None,
        manifest: dict | None = None,
    ) -> None:
        super().__init__()
        if manifest is None:
            manifest = load_manifest()
        if specs is None:
            specs = list(iter_datasets(manifest))
        self._specs = specs
        self._by_slug = {s["slug"]: s for s in specs}
        # Stat once at startup; presence is unlikely to change while browsing.
        self._presence: dict[str, tuple[str, str, str]] = {}
        for spec in specs:
            parquet, vortex = _output_paths(spec, manifest)
            hydrated = _hydrated_parquet(spec, manifest)
            self._presence[spec["slug"]] = (
                _parquet_cell(parquet),
                _vortex_cell(spec, parquet, vortex),
                _hydrate_cell(spec, hydrated),
            )
        # Per-column sort direction; reset whenever a different column is clicked.
        self._sort_key: str | None = None
        self._sort_reverse: bool = False
        # Lazy caches — populated on first row-highlight / first modal open
        # per slug. Each entry is (data, source) where source is "parquet" /
        # "snapshot" / None.
        self._columns_cache: dict[str, tuple[list[tuple[str, str]] | None, str | None]] = {}
        self._stats_cache: dict[str, tuple[list[dict] | None, str | None]] = {}
        self._manifest = manifest
        self._snapshot = _load_snapshot()
        self._highlighted_slug: str | None = None

    def compose(self) -> ComposeResult:
        yield Header()
        live_vocabs = _live_vocabs(self._manifest)
        with Horizontal(id="root-row"):
            with VerticalScroll(id="facets"):
                yield Label("Filters", classes="facets-title")
                yield Label(
                    f"{len(self._specs)} of {len(self._specs)}",
                    id="counts-label",
                )
                with Collapsible(title="Showcase", id="facet-group-showcase"):
                    yield Label("Editorial tiers — curated entry points.", classes="facet-hint")
                    yield _radio_facet("showcase", list(SHOWCASE_TIERS))
                with Collapsible(title="Domain", id="facet-group-tag"):
                    yield Label("Subject tags — narrow by topic area.", classes="facet-hint")
                    yield _radio_facet("tag", list(TAG_VOCAB))
                with Collapsible(title="Size", id="facet-group-size"):
                    yield Label("Parquet size bucket — pick a budget.", classes="facet-hint")
                    yield _radio_facet("size", list(SIZE_BUCKETS))
                with Collapsible(title="Shape traits", id="facet-group-traits"):
                    yield Label("Schema-derived flags (nested, variant, etc).", classes="facet-hint")
                    for flag in TRAIT_FLAGS:
                        with Vertical(classes="trait-block"):
                            yield Label(flag, classes="trait-name")
                            yield RadioSet(
                                RadioButton("Any", id=f"trait-{flag}-any", value=True),
                                RadioButton("Yes", id=f"trait-{flag}-yes"),
                                RadioButton("No",  id=f"trait-{flag}-no"),
                                id=f"trait-radioset-{flag}",
                            )
                with Collapsible(title="License", id="facet-group-license"):
                    yield Label("Upstream SPDX — check redistribution rights.", classes="facet-hint")
                    yield _radio_facet("license", live_vocabs["license"])
                with Collapsible(title="Fetch type", id="facet-group-fetch_type"):
                    yield Label("Download mechanism (http, huggingface, …).", classes="facet-hint")
                    yield _radio_facet("fetch_type", live_vocabs["fetch_type"])
            with Vertical(id="main-col"):
                yield Input(
                    placeholder="/ search — bare words match anywhere; "
                                "slug: name: desc: tag: col: lic: handler: reader: fetch: scope it",
                    id="search-input",
                )
                with Horizontal(id="body"):
                    yield DataTable(id="table", cursor_type="row", zebra_stripes=True)
                    with VerticalScroll(id="detail"):
                        yield Static("", id="detail-content")
        yield Footer()

    # ----- search input plumbing -----

    def action_focus_search(self) -> None:
        try:
            self.query_one("#search-input", Input).focus()
        except Exception:
            pass

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "search-input":
            return
        # Defensive: any exception in the refilter pipeline should surface as
        # a TUI notification rather than tearing the app down. Without this,
        # an unhandled error in `_refresh_filter` would crash the process the
        # moment a user types — and the underlying terminal restoration runs
        # before the traceback is visible.
        try:
            self._refresh_filter()
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            try:
                err_log = outputs_base() / "_browse_search_error.log"
                err_log.parent.mkdir(parents=True, exist_ok=True)
                err_log.write_text(tb)
            except Exception:
                pass
            try:
                self.notify(f"search filter error: {e}", severity="error", timeout=8)
            except Exception:
                pass

    def on_input_submitted(self, event: Input.Submitted) -> None:
        """Enter in the search box: leave the query in place, move focus
        to the slug table so cursor keys work immediately."""
        if event.input.id == "search-input":
            try:
                self.query_one("#table", DataTable).focus()
            except Exception:
                pass

    def _current_query(self) -> str:
        try:
            return (self.query_one("#search-input", Input).value or "").strip()
        except Exception:
            return ""

    def on_mount(self) -> None:
        table = self.query_one("#table", DataTable)
        for label, key in COLUMNS:
            table.add_column(label, key=key)
        self._rebuild_table(self._specs)
        if self._specs:
            self._update_detail(self._specs[0]["slug"])

    def _rebuild_table(self, specs: list[dict]) -> None:
        """Repopulate the slug table with the given (already-filtered) specs."""
        table = self.query_one("#table", DataTable)
        table.clear()
        snapshot_slugs: dict = {}
        if isinstance(self._snapshot, dict):
            snapshot_slugs = self._snapshot.get("slugs") or {}
        for spec in specs:
            cells = self._presence.get(spec["slug"])
            if cells is None:
                # Defensive — for fixtures or specs missing from _presence
                # (shouldn't happen in normal use, but the test path can
                # construct specs the app didn't stat).
                parquet, vortex = _output_paths(spec, self._manifest)
                hydrated = _hydrated_parquet(spec, self._manifest)
                cells = (
                    _parquet_cell(parquet),
                    _vortex_cell(spec, parquet, vortex),
                    _hydrate_cell(spec, hydrated),
                )
                self._presence[spec["slug"]] = cells
            parquet_cell, vortex_cell, hydrate_cell = cells
            snap = snapshot_slugs.get(spec["slug"]) or {}
            table.add_row(
                *_row(spec, parquet_cell, vortex_cell, hydrate_cell, snapshot=snap),
                key=spec["slug"],
            )

    def _current_selections(self) -> dict:
        """Read exclusive (radio) facet selections into a {axis: set|bool|None} dict.

        Each axis is now single-value: the pressed button's label is the chosen
        value, or "(any)" / unpressed means no filter on that axis.
        """
        def _pressed_label(axis: str) -> str | None:
            try:
                rs = self.query_one(f"#facet-{axis}", RadioSet)
                pressed = rs.pressed_button
                if pressed is None:
                    return None
                label = str(pressed.label).strip()
                return None if label == "(any)" else label
            except Exception:
                return None

        sel: dict = {}
        for axis in ("showcase", "tag", "size", "license", "fetch_type"):
            v = _pressed_label(axis)
            sel[axis] = {v} if v is not None else set()
        # vortex axis intentionally has no facet group — model still supports it
        # (CLI --vortex / --no-vortex), but it's not a useful TUI side-bar filter.
        sel["vortex"] = None

        # Tri-state shape-trait radios. Each trait's RadioSet has three
        # buttons (Any / Yes / No); "Any" maps to "unknown" (no filter).
        trait_states: dict[str, str] = {}
        for flag in TRAIT_FLAGS:
            try:
                rs = self.query_one(f"#trait-radioset-{flag}", RadioSet)
                pressed = rs.pressed_button
                if pressed is None:
                    trait_states[flag] = "unknown"
                elif pressed.id and pressed.id.endswith("-yes"):
                    trait_states[flag] = "yes"
                elif pressed.id and pressed.id.endswith("-no"):
                    trait_states[flag] = "no"
                else:
                    trait_states[flag] = "unknown"
            except Exception:
                trait_states[flag] = "unknown"
        sel["_traits"] = trait_states
        return sel

    def _refresh_filter(self) -> None:
        """Apply current FilterState + search query to the slug table; update counts label."""
        selections = self._current_selections()
        trait_states = selections.pop("_traits", {})
        primary = _filter_state_from_selections(selections)
        traits_fs = _trait_state_to_filter(trait_states)
        state = _combine_filters(primary, traits_fs)
        query = self._current_query()
        snapshot_slugs: dict = {}
        if isinstance(self._snapshot, dict):
            snapshot_slugs = self._snapshot.get("slugs") or {}
        visible: list[dict] = []
        for spec in self._specs:
            snap = snapshot_slugs.get(spec["slug"]) or {}
            if not state.matches(spec=spec, snapshot=snap):
                continue
            if query and not _query_matches(spec, snap, query):
                continue
            visible.append(spec)
        self._rebuild_table(visible)
        try:
            label = self.query_one("#counts-label", Label)
            label.update(f"{len(visible)} of {len(self._specs)}")
        except Exception:
            pass

    def on_radio_set_changed(self, event: RadioSet.Changed) -> None:
        """Any facet or shape-trait radio toggled — re-filter the slug table."""
        self._refresh_filter()

    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        if event.row_key is not None and event.row_key.value:
            self._highlighted_slug = event.row_key.value
            self._update_detail(event.row_key.value)

    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        key = event.column_key.value
        if key is None:
            return
        if self._sort_key == key:
            self._sort_reverse = not self._sort_reverse
        else:
            self._sort_key = key
            self._sort_reverse = False
        self.query_one("#table", DataTable).sort(key, reverse=self._sort_reverse)

    def _profile_for(self, slug: str) -> dict | None:
        """Load and cache the per-slug profile.json (None when missing/malformed).

        Prefers the locally-built outputs/v{n}/<slug>/profile.json (fresher).
        Falls back to the tracked docs/v{n}/profiles/<slug>.json so a fresh
        clone can show sparklines without first running `profile`.
        """
        if not hasattr(self, "_profile_cache"):
            self._profile_cache: dict[str, dict | None] = {}
        if slug in self._profile_cache:
            return self._profile_cache[slug]
        v = self._manifest.get("schema_version", 1)
        candidates = (
            outputs_root(self._manifest) / slug / "profile.json",
            REPO_ROOT / "docs" / f"v{v}" / "profiles" / f"{slug}.json",
        )
        for path in candidates:
            if not path.exists():
                continue
            try:
                import json as _json
                self._profile_cache[slug] = _json.loads(path.read_text())
                return self._profile_cache[slug]
            except Exception:
                continue
        self._profile_cache[slug] = None
        return None

    def _update_detail(self, slug: str) -> None:
        spec = self._by_slug.get(slug)
        if spec is None:
            return
        parquet_cell, vortex_cell, hydrate_cell = self._presence[slug]
        if slug not in self._columns_cache:
            self._columns_cache[slug] = _resolve_columns(
                slug, self._manifest, self._snapshot
            )
        columns, _src = self._columns_cache[slug]
        body = _detail(spec, parquet_cell, vortex_cell, hydrate_cell, columns,
                       snapshot=self._snapshot)
        self.query_one("#detail-content", Static).update(body)

    def _ensure_stats(self, slug: str) -> tuple[list[dict] | None, str | None]:
        """Lazy-load + cache the full per-column stats for `slug`. Returns
        (stats, source) where source is "parquet" | "snapshot" | None."""
        if slug not in self._stats_cache:
            self._stats_cache[slug] = _resolve_stats(
                slug, self._manifest, self._snapshot
            )
        return self._stats_cache[slug]

    def action_show_columns(self) -> None:
        slug = self._highlighted_slug
        if slug is None:
            return
        spec = self._by_slug.get(slug)
        if spec is None:
            return
        stats, source = self._ensure_stats(slug)
        self.push_screen(ColumnsModal(slug, spec, stats, source, profile=self._profile_for(slug)))

    def action_show_types(self) -> None:
        slug = self._highlighted_slug
        if slug is None:
            return
        spec = self._by_slug.get(slug)
        if spec is None:
            return
        stats, source = self._ensure_stats(slug)
        self.push_screen(TypesModal(slug, spec, stats, source))

    def _apply_state_to_facets(self, state: FilterState) -> None:
        """Update facet RadioSets so their selection matches `state`.

        With exclusive radios each axis carries at most one value. If `state`
        names multiple (legacy multi-select callers, or migration paths),
        an arbitrary single value is honoured and the rest are dropped.
        """
        def _press(axis: str, target: str | None) -> None:
            try:
                rs = self.query_one(f"#facet-{axis}", RadioSet)
                btns = list(rs.query(RadioButton))
                match = None
                if target is not None:
                    for b in btns:
                        if str(b.label).strip() == target:
                            match = b
                            break
                if match is None:
                    match = rs.query_one(f"#facet-{axis}-radio-any", RadioButton)
                for b in btns:
                    b.value = (b is match)
            except Exception:
                pass

        axis_to_values = {
            "showcase": state.showcase,
            "tag": state.tag,
            "size": state.size,
            "license": state.license,
            "fetch_type": state.fetch_type,
        }
        for axis, values in axis_to_values.items():
            _press(axis, next(iter(values), None))

        # Trait RadioSets: reset all to "Any", then set Yes/No for active traits.
        for flag in TRAIT_FLAGS:
            try:
                rs = self.query_one(f"#trait-radioset-{flag}", RadioSet)
                rs.query_one(f"#trait-{flag}-any", RadioButton).value = True
                if flag in state.trait:
                    rs.query_one(f"#trait-{flag}-yes", RadioButton).value = True
                elif flag in state.trait_negated:
                    rs.query_one(f"#trait-{flag}-no", RadioButton).value = True
            except Exception:
                pass

    def action_clear_facets(self) -> None:
        self._apply_state_to_facets(FilterState())
        self._refresh_filter()

    def action_show_build_confirm(self) -> None:
        slug = self._highlighted_slug
        if slug is None:
            return
        spec = self._by_slug.get(slug)
        if spec is None:
            return

        def _on_confirm(confirmed: bool | None) -> None:
            if not confirmed:
                return
            # Invalidate the per-slug schema / stats caches so the next
            # detail-pane render reads the freshly-built parquet.
            self._columns_cache.pop(slug, None)
            self._stats_cache.pop(slug, None)
            # Also re-stat presence so the parquet/vortex/hydrate cells in
            # the table refresh after the build completes (or rather, on
            # next row-highlight after; the cells stay stale until then).
            parquet, vortex = _output_paths(spec, self._manifest)
            hydrated = _hydrated_parquet(spec, self._manifest)
            self._presence[slug] = (
                _parquet_cell(parquet),
                _vortex_cell(spec, parquet, vortex),
                _hydrate_cell(spec, hydrated),
            )
            self.push_screen(BuildLogModal(slug, spec))

        self.push_screen(BuildConfirmModal(slug, spec, snapshot=self._snapshot), _on_confirm)


def main(argv: list[str] | None = None) -> int:
    DatasetBrowser().run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
