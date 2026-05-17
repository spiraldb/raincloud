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
    assert cells[1] == ""    # handler (empty on minimal spec)
    assert cells[3] == "·"   # parquet
    assert cells[4] == "—"   # vortex
    assert cells[5] == "·"   # scrape (no advisory on minimal spec)
    assert cells[6] == "—"   # hydrate (passed through as cell arg)


def test_row_renders_scrape_advisory_marker():
    """A non-null license.scrape_advisory yields ⚠ in the scrape column."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _row

    spec = {"slug": "x", "license": {"scrape_advisory": "do not redistribute"}}
    cells = _row(spec, "·", "—", "—")
    assert cells[5] == "⚠"


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
    # No profile passed → distribution lookup returns the empty default.
    assert m.profile_columns == {}


def test_render_column_detail_dtype_shapes():
    """`_render_column_detail` produces shape-appropriate multi-line markup."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _render_column_detail

    # Numeric column with histogram → spark + range labels.
    out = _render_column_detail(
        "x",
        {"type": "int32", "null_count": 0, "min": 1, "max": 99},
        {"dtype": "int32", "ndv_approx": 42, "mean": 50.5, "min": 1, "max": 99,
         "histogram": {"counts": [1, 5, 9, 5, 1]}},
    )
    assert "[b]x[/b]" in out and "distribution" in out and "NDV≈" in out

    # String column with top values → top list, not just NDV. Schema-stat
    # min/max are also rendered for text columns (regression guard: they
    # were briefly suppressed during the master/detail rewrite).
    out = _render_column_detail(
        "tag", {"type": "string", "null_count": 0, "min": "a", "max": "z"},
        {"dtype": "string", "ndv_approx": 4, "mean_length": 5.0,
         "top_values": [{"value": "alpha", "count": 7}, {"value": "beta", "count": 3}]},
    )
    assert "top values" in out and "alpha" in out and "beta" in out
    assert "min:" in out and "max:" in out

    # Boolean column → T/F/null counts with percentages.
    out = _render_column_detail(
        "flag", {"type": "bool", "null_count": 2, "min": None, "max": None},
        {"dtype": "bool", "true_count": 5, "false_count": 7, "null_count": 2},
    )
    assert "true:" in out and "false:" in out and "null:" in out
    # Percentages are computed against (true + false + null) = 14.
    assert "%" in out

    # No profile (slug-level) → render the "no profile yet" hint with the
    # build command, but still show schema stats.
    out = _render_column_detail(
        "y", {"type": "int32", "null_count": 5, "min": 0, "max": 9}, None,
    )
    assert "nulls:" in out and "No profile yet" in out
    assert "scripts.pipeline.profile" in out

    # Profile WAS loaded but this column's entry is null — e.g. a struct
    # field. The user shouldn't be told to re-run profile (they'd get the
    # same answer); show a "skipped by design" hint instead.
    out = _render_column_detail(
        "msg", {"type": "struct", "null_count": 0, "min": None, "max": None},
        None, profile_loaded=True,
    )
    assert "No profile yet" not in out
    assert "scripts.pipeline.profile" not in out
    assert "skips" in out and "struct" in out

    # No data at all → defensive "(no data)" placeholder.
    out = _render_column_detail("z", None, None)
    assert "no data" in out


def test_format_stat_truncates_by_pessimistic_render_width():
    """`_format_stat` clamps by *pessimistic* render-cell width so wide-glyph
    *and* combining-mark scripts both stay inside their budget.

    `rich.cells.cell_len` reports spec-correct 0 cells for Sinhala vowel
    signs / Arabic diacritics — but terminals paint them at 1 cell anyway.
    The pessimistic measure (`max(1, cell_len(ch))` per codepoint) bounds
    what the terminal will actually paint."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _format_stat, _render_len

    # CJK: worst-case fullwidth — 2 cells per codepoint.
    cjk = "你好世界" * 20
    out = _format_stat(cjk, max_cells=18)
    assert _render_len(out) <= 18, f"got {_render_len(out)} pessimistic cells"
    assert out.endswith("…")

    # Sinhala: heavy combining marks — cell_len reports < codepoints but
    # terminals paint at codepoint count.
    sinhala = "ඇපල් සහ පෙයාර්ස් පලතුරු වන අතර පොත් පලතුරු නොවේ" * 2
    out = _format_stat(sinhala, max_cells=18)
    assert _render_len(out) <= 18, f"got {_render_len(out)} pessimistic cells"
    assert out.endswith("…")

    # Arabic: similar — diacritics report 0 cells but render as 1.
    arabic = "السؤال: حل العدد ديال المناطق الزمنيه اللي كاينة فالعالم" * 2
    out = _format_stat(arabic, max_cells=30)
    assert _render_len(out) <= 30, f"got {_render_len(out)} pessimistic cells"

    # Short ASCII passes through unchanged.
    assert _format_stat("alpha", max_cells=30) == "alpha"
    # None → em dash.
    assert _format_stat(None) == "—"


@pytest.mark.parametrize("pane_cells", [30, 50, 80])
def test_render_column_detail_fits_pane_width(pane_cells):
    """Regression: Arabic / CJK / Sinhala top-values + min/max + numeric
    histograms must fit the pane at any width. Every visible line's
    *pessimistic* render length must come in at or under `pane_cells`."""
    pytest.importorskip("textual")
    import re

    from scripts.pipeline.browse import _render_column_detail, _render_len

    strip_markup = re.compile(r"\[/?[^\]]+\]")

    def _assert_fits(out: str, label: str) -> None:
        for line in out.splitlines():
            visible = strip_markup.sub("", line)
            assert _render_len(visible) <= pane_cells, (
                f"[{label}] line too wide at pane_cells={pane_cells} "
                f"({_render_len(visible)} cells): {visible!r}"
            )

    long_arabic = "السؤال: حل العدد ديال المناطق الزمنيه اللي كاينة فالعالم"
    long_cjk = "你好世界" * 30
    long_sinhala = "ඇපල් සහ පෙයාර්ස් පලතුරු වන අතර පොත් පලතුරු නොවේ" * 3

    # String column with mixed-script min/max + top value.
    out_str = _render_column_detail(
        "q",
        {"type": "string", "null_count": 0, "min": long_sinhala, "max": long_cjk},
        {"dtype": "string", "ndv_approx": 4, "mean_length": 10.0,
         "top_values": [{"value": long_arabic, "count": 7},
                        {"value": long_sinhala, "count": 5}]},
        pane_cells=pane_cells,
    )
    _assert_fits(out_str, "string col")
    assert "min:" in out_str and "max:" in out_str
    assert "…" in out_str

    # Numeric column with histogram — the bar chart must also fit.
    out_num = _render_column_detail(
        "Age",
        {"type": "int32", "null_count": 1234, "min": 10, "max": 980},
        {"dtype": "int32", "ndv_approx": 89, "mean": 32.7, "min": 10, "max": 980,
         "histogram": {"counts": [3, 18, 42, 67, 91, 78, 55, 31, 14, 5]}},
        pane_cells=pane_cells,
    )
    _assert_fits(out_num, "numeric col")
    assert "distribution" in out_num


def test_render_block_histogram_scales_with_bar_cells():
    """Bar chart: rows tall, bars `bar_cells` wide with a 1-cell gap; the
    tallest count touches the top row."""
    pytest.importorskip("textual")
    from rich.cells import cell_len

    from scripts.pipeline.browse import _render_block_histogram

    counts = [1, 3, 5, 7, 9, 7, 5, 3, 1, 0]
    bars = _render_block_histogram(counts, rows=5, bar_cells=3)
    assert len(bars) == 5
    # Widest line: 10 bins * (3 cells + 1 space) - 1 trailing strip = 39.
    for line in bars:
        assert cell_len(line) <= 10 * 4
    # Top row carries at least one filled glyph (the peak bin at count=9).
    assert any(ch != " " for ch in bars[0])
    # Empty counts → empty list (no spurious bar).
    assert _render_block_histogram([], rows=5, bar_cells=3) == []
    assert _render_block_histogram([0, 0, 0], rows=5, bar_cells=3) == []


def test_render_x_axis_ticks_spaces_lo_mid_hi():
    """3-tick axis: lo left, hi right, mid centered, ASCII spaces between."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _render_len, _render_x_axis_ticks

    # 11 bin edges → mid = buckets[5].
    edges = [0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]
    out = _render_x_axis_ticks(edges, chart_cells=40)
    assert _render_len(out) == 40
    assert out.startswith("0")
    assert out.endswith("1,000")
    assert "500" in out
    # When the chart is too narrow for all 3, fall back to lo → hi.
    narrow = _render_x_axis_ticks(edges, chart_cells=10)
    assert "→" in narrow
    # ISO timestamps get truncated to date prefix.
    iso_edges = ["2020-01-01T00:00:00", "2022-06-15T12:00:00", "2024-12-31T23:59:59"]
    out = _render_x_axis_ticks(iso_edges, chart_cells=40)
    assert "2020-01-01" in out
    assert "2024-12-31" in out
    # Floats get 3-sig-fig form.
    float_edges = [0.0001, 0.005, 0.01]
    out = _render_x_axis_ticks(float_edges, chart_cells=40)
    assert "0.0001" in out and "0.01" in out


def test_format_axis_value_uses_standard_notation_between_1_and_100k():
    """Floats in [1, 100k) render with commas in standard form so a
    histogram of counts/measurements doesn't paint `1e+03` / `1.23e+04`
    when the reader would naturally read `1,000` / `12,300`. Outside that
    range and below 1, fall through to `:.3g`."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _format_axis_value

    # Standard-notation band — was the bug zone.
    assert _format_axis_value(1.0) == "1.00"
    assert _format_axis_value(1.234) == "1.23"
    assert _format_axis_value(12.34) == "12.3"
    assert _format_axis_value(123.4) == "123"
    assert _format_axis_value(1234.6) == "1,235"
    assert _format_axis_value(12345.6) == "12,346"
    assert _format_axis_value(99999.4) == "99,999"
    # Negative numbers — same rules on the magnitude.
    assert _format_axis_value(-1234.6) == "-1,235"
    # Boundary: ≥ 100k flips back to scientific.
    assert "e+05" in _format_axis_value(100_000.0)
    assert "e+06" in _format_axis_value(1_234_567.0)
    # Below 1: existing `:.3g` behaviour.
    assert _format_axis_value(0.5) == "0.5"
    assert _format_axis_value(0.001) == "0.001"
    assert "e-05" in _format_axis_value(0.0000123)
    # Integers always use comma form (no scientific).
    assert _format_axis_value(0) == "0"
    assert _format_axis_value(1234) == "1,234"
    assert _format_axis_value(1_000_000) == "1,000,000"
    # Bools stay as their str repr (e.g. `True` for a boolean histogram).
    assert _format_axis_value(True) == "True"


def test_render_top_value_bars_proportional_widths():
    """Top-value bars scale to count / max(counts); each row reports the
    raw count right-justified."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _render_top_value_bars

    top = [
        {"value": "alpha", "count": 1000},
        {"value": "beta",  "count": 500},
        {"value": "gamma", "count": 100},
    ]
    rows = _render_top_value_bars(top, value_cells=8, bar_cells=10, count_cells=5)
    assert len(rows) == 3
    # Alpha (max) gets a full 10-cell bar; beta half; gamma 1.
    assert rows[0].count("█") == 10
    assert rows[1].count("█") == 5
    assert rows[2].count("█") == 1
    # Counts appear at the right.
    assert rows[0].rstrip().endswith("1,000")
    assert rows[2].rstrip().endswith("100")
    # Empty → empty.
    assert _render_top_value_bars([], value_cells=8, bar_cells=10, count_cells=5) == []


def test_search_query_parser_and_matcher():
    """Free-text search supports bare tokens (match anywhere) + qualified
    `field:value` clauses, ANDed across tokens."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _parse_query, _query_matches

    spec_iris = {
        "slug": "uci-iris", "short_name": "UCI Iris", "full_name": "UCI Iris dataset",
        "description": "Famous flower dataset", "tags": ["measurements", "enums"],
        "transform": {"handler": "uci_default"}, "parse": {"reader": "csv"},
        "fetch": {"type": "http"}, "license": {"spdx": "CC-BY-4.0"},
    }
    spec_wiki = {
        "slug": "wikipedia-en", "short_name": "Wikipedia English", "full_name": "wikipedia-en",
        "description": "English Wikipedia article corpus", "tags": ["prose", "urls"],
        "transform": {"handler": "tighten_types"}, "parse": {"reader": "parquet"},
        "fetch": {"type": "huggingface"}, "license": {"spdx": "CC-BY-SA-4.0"},
    }
    snap_iris = {"columns": [{"name": "sepal_length"}, {"name": "class"}]}
    snap_wiki = {"columns": [{"name": "title"}, {"name": "url"}, {"name": "text"}]}

    # Parser: qualified vs bare; alias resolution.
    assert _parse_query("iris") == [(None, "iris")]
    assert _parse_query("tag:enums foo") == [("tag", "enums"), (None, "foo")]
    assert _parse_query("tags:prose columns:url") == [("tag", "prose"), ("col", "url")]
    # Unknown qualifier falls through as a bare token.
    assert _parse_query("nope:bar") == [(None, "nope:bar")]
    # Empty query → no clauses.
    assert _parse_query("") == []

    # Bare token: match anywhere across all fields.
    assert _query_matches(spec_iris, snap_iris, "iris")
    assert not _query_matches(spec_wiki, snap_wiki, "iris")
    # Qualified clauses.
    assert _query_matches(spec_wiki, snap_wiki, "tag:prose")
    assert _query_matches(spec_iris, snap_iris, "col:sepal")
    assert _query_matches(spec_wiki, snap_wiki, "handler:tighten_types")
    assert _query_matches(spec_wiki, snap_wiki, "lic:CC-BY-SA")
    assert _query_matches(spec_wiki, snap_wiki, "fetch:huggingface")
    # AND across multiple clauses.
    assert _query_matches(spec_iris, snap_iris, "tag:enums col:class")
    assert not _query_matches(spec_iris, snap_iris, "tag:enums col:url")
    # Empty / whitespace query matches everything.
    assert _query_matches(spec_iris, snap_iris, "")
    assert _query_matches(spec_wiki, snap_wiki, "   ")

    # Regression: specs with explicit-null fields must not crash the matcher.
    # `dict.get(k, "")` only substitutes when the key is missing; a stored
    # None used to propagate into `" ".join(...)` and blow up.
    spec_nullish = {
        "slug": "null-edges",
        "short_name": None, "full_name": None, "description": None,
        "tags": ["enums", None],
        "transform": {"handler": None}, "parse": {"reader": None},
        "fetch": {"type": None},
        "license": {"spdx": None, "notes": None},
    }
    snap_nullish = {"columns": [{"name": None}, {"name": "ok"}]}
    # No qualifier, bare-token search across all fields — must not crash.
    assert _query_matches(spec_nullish, snap_nullish, "null") is True   # matches slug
    assert _query_matches(spec_nullish, snap_nullish, "nope") is False
    # Qualified search through fields that contain None.
    assert _query_matches(spec_nullish, snap_nullish, "col:ok") is True
    assert _query_matches(spec_nullish, snap_nullish, "desc:anything") is False


def test_columns_modal_profile_passthrough():
    """`profile=...` is unpacked into `profile_columns` keyed by column name."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import ColumnsModal

    profile = {"columns": {
        "id":  {"histogram": {"counts": [1, 2, 3]}},
        "tag": {"ndv_approx": 4, "top_values": []},
    }}
    m = ColumnsModal("x", {"slug": "x"}, stats=[], profile=profile)
    assert "id" in m.profile_columns
    assert "tag" in m.profile_columns


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
            "license": {"spdx": "MIT"},
            "fetch": {"type": "http", "urls": ["https://example.com/a"]},
            "parse": {"reader": "csv"},
            "transform": {"handler": "identity"},
            "expect": {"rows": 100},
            "convert": {"vortex": True},
        },
        {
            "slug": "test-beta",
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
            assert len(table.columns) == 10

    asyncio.run(_run())


def test_shape_trait_radioset_is_visible_in_facet_panel():
    """Regression: each trait RadioSet must render inside the 28-cell facets panel.

    Earlier the trait RadioSet was packed into a Horizontal `.trait-row` beside a
    22-cell label, which pushed it to region=(28,21,4,2) — off the right edge of
    the 28-cell panel and so visually invisible/unclickable. The vertical-stack
    `.trait-block` layout gives the RadioSet its own row inside the panel."""
    pytest.importorskip("textual")
    from textual.widgets import Collapsible, RadioSet

    from scripts.pipeline.browse import DatasetBrowser

    specs = [{"slug": "x", "license": {"spdx": "MIT"},
              "fetch": {"type": "http", "urls": []}, "parse": {"reader": "csv"},
              "transform": {"handler": "identity"}, "expect": {"rows": 1},
              "convert": {"vortex": True}}]
    PANEL_WIDTH = 28

    async def _run():
        app = DatasetBrowser(specs=specs, manifest={"schema_version": 1, "datasets": specs})
        app._snapshot = {"slugs": {}}
        async with app.run_test(size=(120, 60)) as pilot:
            await pilot.pause()
            app.query_one("#facet-group-traits", Collapsible).collapsed = False
            for _ in range(3):
                await pilot.pause()
            return app.query_one("#trait-radioset-has_nested", RadioSet).region

    region = asyncio.run(_run())
    # RadioSet origin must sit inside the panel and have room for at least one
    # column of `( ) Any` (5+ cells). Pre-fix region was (28, *, 4, 2).
    assert region.x < PANEL_WIDTH, f"radio pushed off panel: x={region.x}"
    assert region.width >= 8, f"radio width too small: {region.width}"
    assert region.height >= 3, f"radio height too small to hold 3 buttons: {region.height}"


def test_shape_trait_yes_propagates_to_filter():
    """Flipping a trait RadioSet's Yes button must filter the table.

    `pilot.click` on RadioButtons is unreliable in the test framework, so we
    drive the press through `.value = True` (same Changed event Textual fires
    for a real mouse click) and assert the resulting row count."""
    pytest.importorskip("textual")
    from textual.widgets import DataTable, RadioButton, RadioSet

    from scripts.pipeline.browse import DatasetBrowser

    specs = [
        {"slug": "nested-alpha", "license": {"spdx": "MIT"},
         "fetch": {"type": "http", "urls": []}, "parse": {"reader": "csv"},
         "transform": {"handler": "identity"}, "expect": {"rows": 1},
         "convert": {"vortex": True}},
        {"slug": "plain-beta", "license": {"spdx": "MIT"},
         "fetch": {"type": "http", "urls": []}, "parse": {"reader": "csv"},
         "transform": {"handler": "identity"}, "expect": {"rows": 1},
         "convert": {"vortex": True}},
    ]

    async def _run() -> int:
        app = DatasetBrowser(specs=specs, manifest={"schema_version": 1, "datasets": specs})
        app._snapshot = {"slugs": {
            "nested-alpha": {"shape_traits": {"has_nested": True}},
            "plain-beta":   {"shape_traits": {"has_nested": False}},
        }}
        async with app.run_test() as pilot:
            await pilot.pause()
            rs = app.query_one("#trait-radioset-has_nested", RadioSet)
            rs.query_one("#trait-has_nested-yes", RadioButton).value = True
            await pilot.pause()
            return app.query_one("#table", DataTable).row_count

    assert asyncio.run(_run()) == 1


def test_collect_filter_state_from_facet_selections():
    """_filter_state_from_selections collects checkbox selections from each
    group into a FilterState (multi-select within axis, AND across axes)."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _filter_state_from_selections

    selections = {
        "showcase": {"encoding"},
        "tag": {"geospatial", "scientific"},
        "size": {"l", "xl"},
        "license": set(),
        "fetch_type": {"http"},
        "vortex": True,
    }
    state = _filter_state_from_selections(selections)
    assert state.showcase == {"encoding"}
    assert state.tag == {"geospatial", "scientific"}
    assert state.size == {"l", "xl"}
    assert state.fetch_type == {"http"}
    assert state.vortex is True
    # Empty axis stays empty.
    assert state.license == set()


def test_filter_state_from_selections_handles_vortex_none():
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _filter_state_from_selections
    state = _filter_state_from_selections({"vortex": None})
    assert state.vortex is None


def test_trait_tri_state_to_filter_state():
    """A tri-state widget maps {yes, no, unknown} → {trait, trait_negated, ignore}."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _trait_state_to_filter

    state = _trait_state_to_filter({
        "has_nested": "yes",
        "has_timestamp": "no",
        "string_heavy": "unknown",
    })
    assert state.trait == {"has_nested"}
    assert state.trait_negated == {"has_timestamp"}
    # "unknown" doesn't filter on either side
    assert "string_heavy" not in state.trait
    assert "string_heavy" not in state.trait_negated


def test_combine_filters_merges_axes():
    """The combine helper preserves set fields from both sources."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _combine_filters, _filter_state_from_selections
    from scripts.pipeline.discovery import FilterState

    a = _filter_state_from_selections({"showcase": {"encoding"}})
    b = FilterState(trait={"has_nested"})
    out = _combine_filters(a, b)
    assert out.showcase == {"encoding"}
    assert out.trait == {"has_nested"}


def test_apply_view_preset_matches_filter_state():
    """Applying a preset programmatically yields the expected FilterState shape."""
    pytest.importorskip("textual")
    from scripts.pipeline.discovery import apply_preset

    state = apply_preset("stress")
    assert state.showcase == {"stress"}
    assert state.size == set()
    # And presets that aren't stress still produce clean states.
    state2 = apply_preset("encoding")
    assert state2.showcase == {"encoding"}
    assert state2.size == set()


def test_row_helper_renders_tags_and_size_cells():
    """_row now emits cells for the new sortable columns (tags, showcase, size_bucket)."""
    pytest.importorskip("textual")
    from scripts.pipeline.browse import _row

    spec = {"slug": "x", "tags": ["geospatial"], "showcase": ["encoding"],
            "convert": {"vortex": True},
            "license": {"spdx": "MIT"}, "short_name": "X"}
    snapshot = {"size_bucket": "m"}
    cells = _row(spec, parquet_cell="·", vortex_cell="·", hydrate_cell="—",
                 snapshot=snapshot)
    text = " ".join(str(c) for c in cells)
    # New cells should appear in the row output.
    assert "geospatial" in text
    assert "encoding" in text
    assert "m" in cells   # size bucket as a literal cell


