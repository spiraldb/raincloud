# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Interactive TUI for browsing sources.json + triggering builds.

Sortable columns plus a detail pane on the right. The build-trigger flow
spawns `python -m scripts.pipeline.build <slug>` as a subprocess and
streams output into a modal log; cancellation kills the subprocess.

Run: `python -m scripts.pipeline.browse`
Install: `uv sync --extra tui`

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
    from textual.containers import Container, Horizontal, VerticalScroll
    from textual.screen import ModalScreen
    from textual.widgets import DataTable, Footer, Header, RichLog, Static
except ImportError:
    print(
        "textual is not installed. Install with: uv sync --extra tui",
        file=sys.stderr,
    )
    raise SystemExit(2)

from .list_datasets import _canonicalize_type
from .spec import (
    REPO_ROOT,
    iter_datasets,
    load_manifest,
    prepared_parquet,
    prepared_parquet_hydrated,
    prepared_vortex,
    spec_field,
)

COLUMNS: tuple[tuple[str, str], ...] = (
    ("slug", "slug"),
    ("family", "family"),
    ("handler", "handler"),
    ("license", "license"),
    ("parquet", "parquet"),
    ("vortex", "vortex"),
    ("scrape", "scrape"),
    ("hydrate", "hydrate"),
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


def _format_stat(v: Any, *, max_len: int = 50) -> str:
    """Render a min/max value safely; truncate long strings; replace control
    chars so DataTable rows don't blow up."""
    if v is None:
        return "—"
    s = str(v).replace("\n", " ").replace("\r", " ").replace("\t", " ")
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return s


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
         hydrate_cell: str) -> tuple[str, ...]:
    scrape_cell = "⚠" if spec_field(spec, "license.scrape_advisory") else "·"
    return (
        spec["slug"],
        spec.get("family") or "",
        spec_field(spec, "transform.handler") or "",
        spec_field(spec, "license.spdx") or "",
        parquet_cell,
        vortex_cell,
        scrape_cell,
        hydrate_cell,
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
        f"[b]family[/b]    {spec.get('family') or '—'}\n"
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
        width: 90%;
        height: 90%;
        background: $surface;
        border: thick $accent;
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
    }
    """

    BINDINGS = [
        Binding("escape", "dismiss", "close"),
        Binding("q", "dismiss", "close"),
    ]


class ColumnsModal(_DatasetModal):
    """Full per-column metadata for one slug. When the local parquet isn't
    built, falls back to docs/v1/snapshot.json so the modal can still show
    *expected* columns (without per-row-group stats — those need the actual
    file). When neither is available, renders an instructional placeholder
    with the build command + a heuristic time estimate."""

    def __init__(self, slug: str, spec: dict,
                 stats: list[dict] | None,
                 source: str | None = None) -> None:
        super().__init__()
        self.slug = slug
        self.spec = spec
        self.stats = stats
        self.source = source  # "parquet" | "snapshot" | None

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
                table = DataTable(zebra_stripes=True)
                table.add_columns("column", "type", "length", "nulls", "min", "max")
                for s in self.stats:
                    table.add_row(
                        s["name"],
                        s["type"],
                        _format_bytes(s["length"]),
                        f"{s['null_count']:,}" if s["null_count"] is not None else "—",
                        _format_stat(s["min"]),
                        _format_stat(s["max"]),
                    )
                yield table
            yield Static("[dim]esc / q to close[/dim]", id="modal-footer")


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

        body = (
            f"[dim]{full_name}[/dim]\n\n"
            f"{description}\n\n"
            f"{license_block}"
            f"[b]rows[/b]       {rows_str}\n"
            f"[b]est. time[/b]  {est}\n"
            f"{advisory_block}\n"
            f"Will run from the repo root:\n"
            f"  [reverse] python -m scripts.pipeline.build {self.slug} [/reverse]\n\n"
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
        border: thick $accent;
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

    def __init__(self, slug: str) -> None:
        super().__init__()
        self.slug = slug
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

    async def _run_build(self) -> None:
        log = self.query_one("#build-log", RichLog)
        status = self.query_one("#status", Static)
        try:
            self._process = await asyncio.create_subprocess_exec(
                sys.executable, "-u", "-m", "scripts.pipeline.build", self.slug,
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
            rc = await self._process.wait()
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
    #body { height: 1fr; }
    DataTable { width: 60%; height: 1fr; }
    #detail {
        width: 40%;
        height: 1fr;
        padding: 1 2;
        border-left: solid $accent;
    }
    """

    BINDINGS = [
        Binding("q", "quit", "quit"),
        Binding("c", "show_columns", "Columns"),
        Binding("t", "show_types", "Types"),
        Binding("b", "show_build_confirm", "Build"),
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
        with Horizontal(id="body"):
            yield DataTable(id="table", cursor_type="row", zebra_stripes=True)
            with VerticalScroll(id="detail"):
                yield Static("", id="detail-content")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#table", DataTable)
        for label, key in COLUMNS:
            table.add_column(label, key=key)
        for spec in self._specs:
            parquet_cell, vortex_cell, hydrate_cell = self._presence[spec["slug"]]
            table.add_row(
                *_row(spec, parquet_cell, vortex_cell, hydrate_cell),
                key=spec["slug"],
            )
        if self._specs:
            self._update_detail(self._specs[0]["slug"])

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
        self.query_one("#detail-content", Static).update(
            _detail(spec, parquet_cell, vortex_cell, hydrate_cell, columns,
                    snapshot=self._snapshot)
        )

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
        self.push_screen(ColumnsModal(slug, spec, stats, source))

    def action_show_types(self) -> None:
        slug = self._highlighted_slug
        if slug is None:
            return
        spec = self._by_slug.get(slug)
        if spec is None:
            return
        stats, source = self._ensure_stats(slug)
        self.push_screen(TypesModal(slug, spec, stats, source))

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
            self.push_screen(BuildLogModal(slug))

        self.push_screen(BuildConfirmModal(slug, spec, snapshot=self._snapshot), _on_confirm)


def main(argv: list[str] | None = None) -> int:
    DatasetBrowser().run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
