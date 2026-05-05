# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Headless smoke test for the TUI browser.

Skipped when the optional `textual` dep isn't installed (the [tui] extra).
"""
from __future__ import annotations

import asyncio
import sys

import pytest


def test_row_helper_handles_missing_fields():
    """_row should never raise on a sparse spec — empty strings are fine."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _row

    minimal = {"slug": "x"}
    cells = _row(minimal, "·", "—", "—")
    assert cells[0] == "x"
    assert cells[1] == ""
    assert cells[4] == "·"   # parquet
    assert cells[5] == "—"   # vortex
    assert cells[6] == "·"   # scrape (no advisory on minimal spec)
    assert cells[7] == "—"   # hydrate (passed through as cell arg)


def test_row_renders_scrape_advisory_marker():
    """A non-null license.scrape_advisory yields ⚠ in the scrape column."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _row

    spec = {"slug": "x", "license": {"scrape_advisory": "do not redistribute"}}
    cells = _row(spec, "·", "—", "—")
    assert cells[6] == "⚠"


def test_hydrate_cell_states(tmp_path):
    """Three-state hydrate cell: not configured / configured & missing / present."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _hydrate_cell

    hydrated = tmp_path / "x.parquet"
    # Not configured
    assert _hydrate_cell({"slug": "x"}, hydrated) == "—"
    # Configured, file missing
    spec = {"slug": "x", "hydrate": {"url_column": "url", "output_column": "content",
                                     "output_type": "binary", "advisory": "..."}}
    assert _hydrate_cell(spec, hydrated) == "·"
    # Configured, file present
    hydrated.write_bytes(b"")
    assert _hydrate_cell(spec, hydrated) == "✓"


def test_vortex_cell_states(tmp_path):
    """Four-state cell logic: opt-in × file presence × staleness."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _vortex_cell

    parquet = tmp_path / "x.parquet"
    vortex = tmp_path / "x.vortex"

    # Not opted in.
    assert _vortex_cell({"convert": {"vortex": False}}, parquet, vortex) == "—"
    # Opted in, vortex missing.
    assert _vortex_cell({"convert": {"vortex": True}}, parquet, vortex) == "·"
    # Opted in, vortex present, no parquet (treated as fresh).
    vortex.write_bytes(b"")
    assert _vortex_cell({"convert": {"vortex": True}}, parquet, vortex) == "✓"
    # Opted in, parquet newer than vortex → stale.
    parquet.write_bytes(b"")
    import os
    import time
    os.utime(parquet, (time.time() + 10, time.time() + 10))
    assert _vortex_cell({"convert": {"vortex": True}}, parquet, vortex) == "⚠"


def test_read_columns_returns_none_for_missing_file(tmp_path):
    """Unbuilt parquet path → None, never raises."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _read_columns

    assert _read_columns(tmp_path / "missing.parquet") is None


def test_read_columns_extracts_schema_from_real_parquet(tmp_path):
    """Footer-only schema read against a tiny fixture parquet."""
    pytest.importorskip("textual")
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.pipeline.browse import _read_columns

    p = tmp_path / "x.parquet"
    pq.write_table(
        pa.table({"id": pa.array([1, 2], type=pa.int32()),
                  "label": pa.array(["a", "b"], type=pa.string())}),
        p,
    )
    cols = _read_columns(p)
    assert cols == [("id", "int32"), ("label", "string")]


def test_resolve_rows_prefers_expect():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _resolve_rows
    spec = {"slug": "x", "expect": {"rows": 12345}}
    out, src = _resolve_rows(spec, snapshot=None)
    assert out == "12,345"
    assert src == "expect"


def test_resolve_rows_falls_back_to_last_built():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _resolve_rows
    spec = {"slug": "x", "expect": {"rows": None}}
    snap = {"slugs": {"x": {"last_built_rows": 4567}}}
    out, src = _resolve_rows(spec, snap)
    assert "4,567" in out
    assert "last seen" in out
    assert src == "last-seen"


def test_resolve_rows_em_dash_when_neither():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _resolve_rows
    out, src = _resolve_rows({"slug": "x", "expect": {"rows": None}}, snapshot=None)
    assert out == "—"
    assert src == "—"


def test_references_block_empty_when_no_refs():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _references_block
    assert _references_block({"slug": "x"}) == ""
    assert _references_block({"slug": "x", "references": []}) == ""


def test_references_block_renders_kind_url_pairs():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _references_block
    refs = [{"kind": "paper", "url": "https://arxiv.org/abs/1234.5678"},
            {"kind": "github", "url": "https://github.com/foo/bar"}]
    out = _references_block({"slug": "x", "references": refs})
    assert "paper" in out and "https://arxiv.org/abs/1234.5678" in out
    assert "github" in out and "https://github.com/foo/bar" in out


def test_columns_block_renders_states():
    """Three rendering paths: None (not built), [] (empty schema), populated."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _columns_block

    spec = {"slug": "x"}
    not_built = _columns_block(spec, None)
    assert "parquet not built" in not_built

    empty = _columns_block(spec, [])
    assert "—" in empty

    populated = _columns_block(spec, [("id", "int32"), ("label", "string")])
    assert "id" in populated and "int32" in populated
    assert "label" in populated and "string" in populated
    assert "(2)" in populated  # column count


def test_read_column_stats_extracts_full_metadata(tmp_path):
    """Footer + row-group stats; nullable null_count, min/max for scalars."""
    pytest.importorskip("textual")
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.pipeline.browse import _read_column_stats

    p = tmp_path / "x.parquet"
    pq.write_table(
        pa.table({
            "id": pa.array([1, 2, 3, None], type=pa.int32()),
            "label": pa.array(["a", "b", "c", "d"], type=pa.string()),
        }),
        p, write_statistics=True,
    )
    stats = _read_column_stats(p)
    assert stats is not None
    by_name = {s["name"]: s for s in stats}
    assert by_name["id"]["type"] == "int32"
    assert by_name["id"]["null_count"] == 1
    assert by_name["id"]["min"] == 1
    assert by_name["id"]["max"] == 3
    assert by_name["id"]["length"] > 0
    assert by_name["label"]["null_count"] == 0


def test_read_column_stats_returns_none_for_missing(tmp_path):
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _read_column_stats
    assert _read_column_stats(tmp_path / "missing.parquet") is None


def test_build_time_estimate_brackets():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _build_time_estimate
    # row-based fallback
    assert "seconds" in _build_time_estimate({"expect": {"rows": 5_000}})
    assert "minutes" in _build_time_estimate({"expect": {"rows": 5_000_000}})
    assert "tens of minutes" in _build_time_estimate({"expect": {"rows": 50_000_000}})
    assert "hours" in _build_time_estimate({"expect": {"rows": 500_000_000}})
    # bytes-based path takes precedence
    assert "seconds" in _build_time_estimate({"fetch": {"expected_bytes": 50_000_000}})
    assert "many hours" in _build_time_estimate({"fetch": {"expected_bytes": 500_000_000_000}})
    # unknown
    assert "unknown" in _build_time_estimate({})


def test_per_slug_type_coverage_aggregates_by_canonical_type():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _per_slug_type_coverage

    stats = [
        {"name": "a", "type": "string"},
        {"name": "b", "type": "string"},
        {"name": "c", "type": "int64"},
        {"name": "d", "type": "struct<x int32, y string>"},
        {"name": "e", "type": "struct<a string, b string>"},
    ]
    cov = _per_slug_type_coverage(stats)
    by_type = {r["type"]: r for r in cov}
    assert by_type["string"]["count"] == 2
    assert by_type["int64"]["count"] == 1
    # Different struct shapes collapse via _canonicalize_type. pyarrow's
    # lowercase `struct<...>` form is what the TUI sees (the DuckDB path
    # used by --coverage produces uppercase STRUCT(...)).
    assert by_type["struct<...>"]["count"] == 2


def test_resolve_columns_prefers_parquet_over_snapshot(tmp_path, monkeypatch):
    """When both a built parquet and a snapshot entry exist for a slug, the
    parquet wins. Snapshot is only a fallback for unbuilt slugs."""
    pytest.importorskip("textual")
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.pipeline import browse

    slug = "test-resolve-prefers"
    parquet = browse.prepared_parquet(slug)
    parquet.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"id": pa.array([1, 2, 3], type=pa.int32())}), parquet)
    try:
        snapshot = {"slugs": {slug: {"columns": [
            {"name": "STALE_FROM_SNAPSHOT", "type": "string"},
        ]}}}
        cols, src = browse._resolve_columns(slug, manifest={"schema_version": 1}, snapshot=snapshot)
        assert src == "parquet"
        assert cols == [("id", "int32")]
    finally:
        parquet.unlink()
        if parquet.parent.exists() and not any(parquet.parent.iterdir()):
            parquet.parent.rmdir()
        if parquet.parent.parent.exists() and not any(parquet.parent.parent.iterdir()):
            parquet.parent.parent.rmdir()


def test_resolve_columns_falls_back_to_snapshot():
    pytest.importorskip("textual")
    from scripts.pipeline import browse

    slug = "no-such-slug-anywhere"
    snapshot = {"slugs": {slug: {"columns": [
        {"name": "id", "type": "int32"},
        {"name": "label", "type": "string"},
    ]}}}
    cols, src = browse._resolve_columns(slug, manifest={"schema_version": 1}, snapshot=snapshot)
    assert src == "snapshot"
    assert cols == [("id", "int32"), ("label", "string")]


def test_resolve_columns_returns_none_when_no_data():
    pytest.importorskip("textual")
    from scripts.pipeline import browse
    cols, src = browse._resolve_columns("no-such-slug", manifest={"schema_version": 1}, snapshot=None)
    assert cols is None and src is None


def test_resolve_stats_snapshot_fills_unknowns():
    """Snapshot fallback for the stats path: name+type from snapshot,
    None for the per-row-group fields (length, null_count, min, max)."""
    pytest.importorskip("textual")
    from scripts.pipeline import browse

    slug = "no-such-slug-stats"
    snapshot = {"slugs": {slug: {"columns": [
        {"name": "id", "type": "int32"},
    ]}}}
    stats, src = browse._resolve_stats(slug, manifest={"schema_version": 1}, snapshot=snapshot)
    assert src == "snapshot"
    assert stats == [{"name": "id", "type": "int32", "length": None,
                      "null_count": None, "min": None, "max": None}]


def test_load_snapshot_returns_none_when_absent(monkeypatch, tmp_path):
    """If docs/v1/snapshot.json doesn't exist, _load_snapshot returns None
    cleanly (no exception)."""
    pytest.importorskip("textual")
    from scripts.pipeline import browse

    monkeypatch.setattr(browse, "REPO_ROOT", tmp_path)
    assert browse._load_snapshot() is None


def test_columns_modal_renders_unbuilt_state():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import ColumnsModal, _build_time_estimate

    spec = {"slug": "x", "expect": {"rows": 1_000_000}}
    m = ColumnsModal("x", spec, None)
    # The body's text is built lazily in compose(); assert the inputs are
    # plumbed in correctly + the build estimate is non-empty.
    assert m.slug == "x"
    assert m.stats is None
    assert _build_time_estimate(spec)


def test_columns_modal_renders_built_state():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import ColumnsModal

    stats = [{"name": "id", "type": "int32", "length": 100,
              "null_count": 0, "min": 1, "max": 99}]
    m = ColumnsModal("x", {"slug": "x"}, stats)
    assert m.stats == stats


def test_build_confirm_modal_plumbs_inputs():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import BuildConfirmModal

    spec = {
        "slug": "x", "short_name": "X", "full_name": "X (full)",
        "description": "tiny test fixture",
        "license": {"spdx": "MIT"},
        "expect": {"rows": 1_000_000},
    }
    m = BuildConfirmModal("x", spec)
    assert m.slug == "x"
    assert m.spec is spec


def test_build_log_modal_constructor():
    """Sanity-check that the BuildLogModal class instantiates without
    actually starting a subprocess (subprocess only spawns on mount)."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import BuildLogModal

    m = BuildLogModal("x")
    assert m.slug == "x"
    assert m._process is None
    assert m._task is None


def test_build_confirm_dismiss_returns_true_on_confirm():
    """End-to-end: mount the BuildConfirmModal, press Enter, confirm the
    callback fires with True."""
    pytest.importorskip("textual")
    from textual.app import App, ComposeResult
    from textual.widgets import Static

    from scripts.pipeline.browse import BuildConfirmModal

    spec = {
        "slug": "x", "short_name": "X", "full_name": "X",
        "description": "fixture", "license": {"spdx": "MIT"},
        "expect": {"rows": 1000},
    }
    result = {"value": None}

    class _Host(App):
        def compose(self) -> ComposeResult:
            yield Static("host")

        def on_mount(self) -> None:
            def _capture(v):
                result["value"] = v
            self.push_screen(BuildConfirmModal("x", spec), _capture)

    async def _run():
        async with _Host().run_test() as pilot:
            await pilot.pause()
            await pilot.press("enter")
            await pilot.pause()

    asyncio.run(_run())
    assert result["value"] is True


def test_build_log_modal_runs_subprocess_and_exposes_returncode():
    """Spawn a tiny synthetic subprocess instead of a real Raincloud build,
    via a subclass that overrides _run_build to use a no-op python -c.
    Verifies the modal's task lifecycle, log-streaming, and exit-code path
    without invoking the actual pipeline."""
    pytest.importorskip("textual")
    from textual.app import App, ComposeResult
    from textual.widgets import Static

    from scripts.pipeline.browse import BuildLogModal

    captured: dict = {}

    class _FakeBuildLogModal(BuildLogModal):
        async def _run_build(self) -> None:
            from textual.widgets import RichLog
            log = self.query_one("#build-log", RichLog)
            self._process = await asyncio.create_subprocess_exec(
                sys.executable, "-c",
                "print('hi'); print('there')",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            assert self._process.stdout is not None
            lines = []
            while True:
                ln = await self._process.stdout.readline()
                if not ln:
                    break
                txt = ln.decode().rstrip("\n")
                log.write(txt)
                lines.append(txt)
            captured["rc"] = await self._process.wait()
            captured["lines"] = lines

    class _Host(App):
        def compose(self) -> ComposeResult:
            yield Static("host")

        async def on_mount(self) -> None:
            await self.push_screen(_FakeBuildLogModal("x"))

    async def _run():
        async with _Host().run_test() as pilot:
            # Pump the loop until the build task finishes (or 5s timeout).
            for _ in range(100):
                await pilot.pause(0.05)
                if "rc" in captured:
                    return

    asyncio.run(_run())
    assert captured.get("rc") == 0
    assert captured.get("lines") == ["hi", "there"]


def test_browse_app_mounts_and_renders():
    """The TUI composes, mounts, and updates the detail pane without errors."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import DatasetBrowser

    fixture = [
        {
            "slug": "test-alpha",
            "short_name": "Test Alpha",
            "full_name": "Test Alpha (fixture)",
            "description": "First fixture row.",
            "family": "test",
            "license": {"spdx": "MIT"},
            "fetch": {"type": "http", "urls": ["https://example.com/a"]},
            "parse": {"reader": "csv"},
            "transform": {"handler": "identity"},
            "expect": {"rows": 100},
            "convert": {"vortex": True},
        },
        {
            "slug": "test-beta",
            "family": "test",
            "license": {"spdx": "Apache-2.0"},
            "fetch": {"type": "http", "urls": []},
            "parse": {"reader": "parquet"},
            "transform": {"handler": "tighten_types"},
            "expect": {"rows": None},
            "convert": {"vortex": False},
        },
    ]

    fake_manifest = {"schema_version": 1, "datasets": fixture}

    async def _run() -> None:
        app = DatasetBrowser(specs=fixture, manifest=fake_manifest)
        async with app.run_test() as pilot:
            await pilot.pause()
            from textual.widgets import DataTable

            table = app.query_one("#table", DataTable)
            assert table.row_count == 2
            assert len(table.columns) == 8

    asyncio.run(_run())
