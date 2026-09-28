# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the new --columns / --coverage flags on list_datasets.

The pure helpers (_canonicalize_type, _filter_columns, _coverage_summary)
don't touch the filesystem and are tested directly. The _iter_columns path
touches the filesystem; we exercise it against a fixture parquet written
to outputs/v1/<test-slug>/parquet/ and clean up.
"""
from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.list_datasets import (
    _canonicalize_type,
    _coverage_summary,
    _filter_columns,
    _iter_columns,
)
from raincloud.pipeline.spec import prepared_parquet


def test_canonicalize_type_preserves_scalar_params():
    assert _canonicalize_type("DECIMAL(10, 2)") == "DECIMAL(10, 2)"
    assert _canonicalize_type("TIMESTAMP WITH TIME ZONE") == "TIMESTAMP WITH TIME ZONE"
    assert _canonicalize_type("VARCHAR") == "VARCHAR"


def test_canonicalize_type_collapses_struct_bodies():
    assert _canonicalize_type("STRUCT(a INTEGER, b VARCHAR)") == "STRUCT(...)"
    # Nested structs collapse
    assert _canonicalize_type(
        "STRUCT(a STRUCT(x INTEGER), b VARCHAR)"
    ) == "STRUCT(...)"
    # Lists of structs collapse the struct interior, keep the list wrapper
    assert _canonicalize_type(
        "LIST(STRUCT(a INTEGER, b VARCHAR))"
    ) == "LIST(STRUCT(...))"


def test_filter_columns_grep():
    rows = [
        {"slug": "a", "column": "url", "type": "string"},
        {"slug": "b", "column": "image_url", "type": "string"},
        {"slug": "c", "column": "id", "type": "int64"},
    ]
    out = _filter_columns(rows, "url")
    assert {r["column"] for r in out} == {"url", "image_url"}
    out = _filter_columns(rows, "^url$")
    assert {r["column"] for r in out} == {"url"}
    # No filter is a passthrough
    assert _filter_columns(rows, None) == rows


def test_coverage_summary_aggregates_by_canonical_type():
    rows = [
        {"slug": "a", "column": "x", "type": "string"},
        {"slug": "a", "column": "y", "type": "string"},
        {"slug": "b", "column": "z", "type": "string"},
        {"slug": "b", "column": "n", "type": "int64"},
    ]
    cov = _coverage_summary(rows)
    by_type = {r["type"]: r for r in cov}
    assert by_type["string"]["columns"] == 3
    assert by_type["string"]["datasets"] == 2
    assert by_type["int64"]["columns"] == 1
    assert by_type["int64"]["datasets"] == 1
    # Examples are slug.column triples
    assert "a.x" in by_type["string"]["examples"]


def test_coverage_summary_collapses_struct_shapes():
    rows = [
        {"slug": "a", "column": "x", "type": "STRUCT(a INTEGER)"},
        {"slug": "a", "column": "y", "type": "STRUCT(a VARCHAR, b BIGINT)"},
    ]
    cov = _coverage_summary(rows)
    # Different struct bodies, same canonical bucket
    assert len(cov) == 1
    assert cov[0]["type"] == "STRUCT(...)"
    assert cov[0]["columns"] == 2


def test_iter_columns_reads_built_parquet():
    """End-to-end: write a tiny parquet under the canonical layout, run
    _iter_columns, confirm it surfaces."""
    slug = "test-list-datasets-cols"
    parquet = prepared_parquet(slug)
    parquet.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int32()),
        "label": pa.array(["a", "b", "c"], type=pa.string()),
    })
    pq.write_table(table, parquet, compression="zstd")
    try:
        out = _iter_columns([{"slug": slug}], source="parquet")
        names = [r["column"] for r in out]
        assert names == ["id", "label"]
        assert all(r["source"] == "parquet" for r in out)
        assert all(r["slug"] == slug for r in out)
    finally:
        parquet.unlink()
        if parquet.parent.exists() and not any(parquet.parent.iterdir()):
            parquet.parent.rmdir()
        if parquet.parent.parent.exists() and not any(parquet.parent.parent.iterdir()):
            parquet.parent.parent.rmdir()


def test_iter_columns_skips_unbuilt_slugs():
    """Slugs without a built parquet are silently skipped — column inspection
    is only meaningful for built outputs."""
    out = _iter_columns([{"slug": "no-such-slug-anywhere-abc123"}], source="parquet")
    assert out == []


@pytest.fixture
def tmp_manifest_with_showcase():
    """Two-spec synthetic manifest for filter testing."""
    return {
        "schema_version": 1,
        "datasets": [
            {"slug": "demo-start", "short_name": "Demo", "full_name": "Demo",
             "description": "",
             "license": {"spdx": "MIT"},
             "fetch": {"type": "http", "urls": ["https://x"], "auth": None,
                       "expected_bytes": None, "expected_sha256": None, "notes": None},
             "extract": {"type": "passthrough", "include": [], "exclude": [], "post": None},
             "parse": {"reader": "csv", "options": {}},
             "transform": {"handler": "identity", "params": {}},
             "write": {"output": "demo-start.parquet", "compression": "zstd",
                       "row_group_size_rows": 1024, "statistics": True, "page_index": False},
             "expect": {"rows": 10, "schema_hash": None, "notes": None, "row_stability": "static"},
             "convert": {"vortex": True, "vortex_skip_reason": None},
             "tags": ["coordinates"], "showcase": ["encoding"]},
            {"slug": "demo-other", "short_name": "Demo2", "full_name": "Demo2",
             "description": "",
             "license": {"spdx": "MIT"},
             "fetch": {"type": "http", "urls": ["https://x"], "auth": None,
                       "expected_bytes": None, "expected_sha256": None, "notes": None},
             "extract": {"type": "passthrough", "include": [], "exclude": [], "post": None},
             "parse": {"reader": "csv", "options": {}},
             "transform": {"handler": "identity", "params": {}},
             "write": {"output": "demo-other.parquet", "compression": "zstd",
                       "row_group_size_rows": 1024, "statistics": True, "page_index": False},
             "expect": {"rows": 10, "schema_hash": None, "notes": None, "row_stability": "static"},
             "convert": {"vortex": True, "vortex_skip_reason": None},
             "tags": [], "showcase": []},
        ],
    }


@pytest.fixture
def snapshot_with_traits():
    return {
        "demo-start": {"shape_traits": {"has_nested": True}, "size_bucket": "s"},
        "demo-other": {"shape_traits": {"has_nested": False}, "size_bucket": "s"},
    }


def test_list_datasets_filter_by_showcase(monkeypatch, capsys, tmp_manifest_with_showcase):
    """Filtering by showcase tier restricts the slug list."""
    import raincloud.pipeline.list_datasets as ld_mod
    monkeypatch.setattr(ld_mod, "load_manifest", lambda: tmp_manifest_with_showcase)
    monkeypatch.setattr(ld_mod, "_load_snapshot", lambda *a: {})
    rc = ld_mod.main(["--showcase", "encoding"])
    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert "demo-start" in out
    assert "demo-other" not in out


def test_list_datasets_filter_by_tag(monkeypatch, capsys, tmp_manifest_with_showcase):
    import raincloud.pipeline.list_datasets as ld_mod
    monkeypatch.setattr(ld_mod, "load_manifest", lambda: tmp_manifest_with_showcase)
    monkeypatch.setattr(ld_mod, "_load_snapshot", lambda *a: {})
    rc = ld_mod.main(["--tag", "coordinates"])
    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert "demo-start" in out
    assert "demo-other" not in out


def test_list_datasets_view_preset(monkeypatch, capsys, tmp_manifest_with_showcase, snapshot_with_traits):
    """--view encoding equivalent to --showcase encoding."""
    import raincloud.pipeline.list_datasets as ld_mod
    monkeypatch.setattr(ld_mod, "load_manifest", lambda: tmp_manifest_with_showcase)
    monkeypatch.setattr(ld_mod, "_load_snapshot", lambda *a: snapshot_with_traits)
    rc = ld_mod.main(["--view", "encoding"])
    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert "demo-start" in out
    assert "demo-other" not in out


def test_list_datasets_trait_negation(monkeypatch, capsys, tmp_manifest_with_showcase, snapshot_with_traits):
    """--trait '!has_nested' excludes slugs whose snapshot has has_nested=True."""
    import raincloud.pipeline.list_datasets as ld_mod
    monkeypatch.setattr(ld_mod, "load_manifest", lambda: tmp_manifest_with_showcase)
    monkeypatch.setattr(ld_mod, "_load_snapshot", lambda *a: snapshot_with_traits)
    rc = ld_mod.main(["--trait", "!has_nested"])
    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert "demo-start" not in out      # has_nested=True excluded
    assert "demo-other" in out


def test_list_datasets_size_filter(monkeypatch, capsys, tmp_manifest_with_showcase, snapshot_with_traits):
    """--size s matches both demo slugs (both have size_bucket=s)."""
    import raincloud.pipeline.list_datasets as ld_mod
    monkeypatch.setattr(ld_mod, "load_manifest", lambda: tmp_manifest_with_showcase)
    monkeypatch.setattr(ld_mod, "_load_snapshot", lambda *a: snapshot_with_traits)
    rc = ld_mod.main(["--size", "s"])
    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert "demo-start" in out
    assert "demo-other" in out
    ld_mod.main(["--size", "l"])
    out2 = capsys.readouterr().out.splitlines()
    assert "demo-start" not in out2


def test_list_datasets_showcase_and_tag_and_combine(monkeypatch, capsys, tmp_manifest_with_showcase):
    """--showcase AND --tag compose (AND across axes)."""
    import raincloud.pipeline.list_datasets as ld_mod
    monkeypatch.setattr(ld_mod, "load_manifest", lambda: tmp_manifest_with_showcase)
    monkeypatch.setattr(ld_mod, "_load_snapshot", lambda *a: {})
    rc = ld_mod.main(["--showcase", "encoding", "--tag", "coordinates"])
    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert "demo-start" in out
    assert "demo-other" not in out


def test_list_datasets_view_overrides_other_facets(monkeypatch, capsys, tmp_manifest_with_showcase):
    """--view replaces other facet selections (doesn't union with them).

    demo-other has showcase=[], so adding --showcase doesn't add it back;
    --view encoding alone yields demo-start, and so should --view encoding
    --showcase stress (the showcase flag is ignored when --view is set).
    """
    import raincloud.pipeline.list_datasets as ld_mod
    monkeypatch.setattr(ld_mod, "load_manifest", lambda: tmp_manifest_with_showcase)
    monkeypatch.setattr(ld_mod, "_load_snapshot", lambda *a: {})
    rc = ld_mod.main(["--view", "encoding", "--showcase", "stress"])
    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert "demo-start" in out      # preset still matches; --showcase ignored
    assert "demo-other" not in out


def test_inspect_renders_profile_when_present(monkeypatch, capsys, tmp_path, tmp_manifest_with_showcase):
    """--inspect <slug> reads profile.json and renders one line per column."""
    monkeypatch.setattr("raincloud.pipeline.list_datasets.outputs_root",
                        lambda manifest=None: tmp_path / "outputs" / "v1")
    monkeypatch.setattr("raincloud.pipeline.list_datasets.load_manifest",
                        lambda: {"schema_version": 1, "datasets": [
                            {"slug": "uci-seeds", "short_name": "UCI Seeds",
                             "full_name": "UCI Seeds", "description": "small",
                             "license": {"spdx": "MIT"},
                             "tags": [], "showcase": []},
                        ]})

    profile_dir = tmp_path / "outputs" / "v1" / "uci-seeds"
    profile_dir.mkdir(parents=True)
    (profile_dir / "profile.json").write_text(json.dumps({
        "schema_version": 1, "slug": "uci-seeds", "row_count": 210,
        "parquet_sha256": "0" * 64, "computed_at": "2026-05-12T00:00:00Z",
        "sample_rows": None,
        "columns": {
            "area":  {"dtype": "float64", "null_count": 0, "min": 10.5,
                      "max": 21.2, "mean": 14.8, "ndv_approx": 200,
                      "histogram": {"buckets": [10.5, 21.2], "counts": [210]}},
            "label": {"dtype": "int32", "null_count": 0, "min": 1, "max": 3,
                      "mean": 2.0, "ndv_approx": 3,
                      "histogram": {"buckets": [1, 3], "counts": [210]}},
        },
    }))

    from raincloud.pipeline.list_datasets import main as ld_main
    rc = ld_main(["--inspect", "uci-seeds"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "uci-seeds" in out
    assert "area" in out and "label" in out
    assert "210" in out   # row count surfaces
    assert "no profile" not in out.lower()


def test_inspect_warns_when_profile_missing(monkeypatch, capsys, tmp_path):
    # Point both candidate paths (built + tracked) at an empty tmp tree so neither resolves.
    monkeypatch.setattr("raincloud.pipeline.list_datasets.outputs_root",
                        lambda manifest=None: tmp_path / "outputs" / "v1")
    monkeypatch.setattr("raincloud.pipeline.list_datasets.REPO_ROOT", tmp_path)
    monkeypatch.setattr("raincloud.pipeline.list_datasets.load_manifest",
                        lambda: {"schema_version": 1, "datasets": [
                            {"slug": "uci-seeds", "short_name": "UCI Seeds",
                             "full_name": "UCI Seeds", "description": "",
                             "license": {"spdx": "MIT"},
                             "tags": [], "showcase": []},
                        ]})

    from raincloud.pipeline.list_datasets import main as ld_main
    rc = ld_main(["--inspect", "uci-seeds"])
    assert rc == 0
    assert "no profile" in capsys.readouterr().out.lower()


def test_inspect_falls_back_to_tracked_profile_when_built_missing(
    monkeypatch, capsys, tmp_path
):
    """On a fresh clone the built profile doesn't exist, but the tracked mirror
    at docs/v{n}/profiles/<slug>.json does — --inspect should render it."""
    monkeypatch.setattr("raincloud.pipeline.list_datasets.outputs_root",
                        lambda manifest=None: tmp_path / "outputs" / "v1")
    monkeypatch.setattr("raincloud.pipeline.list_datasets.REPO_ROOT", tmp_path)
    monkeypatch.setattr("raincloud.pipeline.list_datasets.load_manifest",
                        lambda: {"schema_version": 1, "datasets": [
                            {"slug": "uci-seeds", "short_name": "UCI Seeds",
                             "full_name": "UCI Seeds", "description": "",
                             "license": {"spdx": "MIT"},
                             "tags": [], "showcase": []},
                        ]})

    # Only the tracked mirror exists; no built outputs/v1/<slug>/profile.json.
    tracked_dir = tmp_path / "docs" / "v1" / "profiles"
    tracked_dir.mkdir(parents=True)
    (tracked_dir / "uci-seeds.json").write_text(json.dumps({
        "schema_version": 1, "slug": "uci-seeds", "row_count": 7777,
        "parquet_sha256": "0" * 64, "computed_at": "2026-05-12T00:00:00Z",
        "sample_rows": None,
        "columns": {
            "tracked_only_marker": {"dtype": "float64", "null_count": 0,
                                    "min": 0.0, "max": 1.0, "mean": 0.5,
                                    "ndv_approx": 2,
                                    "histogram": {"buckets": [0.0, 1.0],
                                                  "counts": [7777]}},
        },
    }))

    from raincloud.pipeline.list_datasets import main as ld_main
    rc = ld_main(["--inspect", "uci-seeds"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "tracked_only_marker" in out      # column from the tracked profile rendered
    assert "7777" in out                      # row count from the tracked profile rendered
    assert "no profile" not in out.lower()


def test_inspect_prefers_built_profile_when_both_exist(
    monkeypatch, capsys, tmp_path
):
    """When both built and tracked profiles exist, the built one wins (it's fresher)."""
    monkeypatch.setattr("raincloud.pipeline.list_datasets.outputs_root",
                        lambda manifest=None: tmp_path / "outputs" / "v1")
    monkeypatch.setattr("raincloud.pipeline.list_datasets.REPO_ROOT", tmp_path)
    monkeypatch.setattr("raincloud.pipeline.list_datasets.load_manifest",
                        lambda: {"schema_version": 1, "datasets": [
                            {"slug": "uci-seeds", "short_name": "UCI Seeds",
                             "full_name": "UCI Seeds", "description": "",
                             "license": {"spdx": "MIT"},
                             "tags": [], "showcase": []},
                        ]})

    built_dir = tmp_path / "outputs" / "v1" / "uci-seeds"
    built_dir.mkdir(parents=True)
    (built_dir / "profile.json").write_text(json.dumps({
        "schema_version": 1, "slug": "uci-seeds", "row_count": 111,
        "parquet_sha256": "0" * 64, "computed_at": "2026-05-12T00:00:00Z",
        "sample_rows": None,
        "columns": {
            "built_marker": {"dtype": "float64", "null_count": 0,
                             "min": 0.0, "max": 1.0, "mean": 0.5,
                             "ndv_approx": 2,
                             "histogram": {"buckets": [0.0, 1.0],
                                           "counts": [111]}},
        },
    }))

    tracked_dir = tmp_path / "docs" / "v1" / "profiles"
    tracked_dir.mkdir(parents=True)
    (tracked_dir / "uci-seeds.json").write_text(json.dumps({
        "schema_version": 1, "slug": "uci-seeds", "row_count": 999,
        "parquet_sha256": "0" * 64, "computed_at": "2020-01-01T00:00:00Z",
        "sample_rows": None,
        "columns": {
            "tracked_marker": {"dtype": "float64", "null_count": 0,
                               "min": 0.0, "max": 1.0, "mean": 0.5,
                               "ndv_approx": 2,
                               "histogram": {"buckets": [0.0, 1.0],
                                             "counts": [999]}},
        },
    }))

    from raincloud.pipeline.list_datasets import main as ld_main
    rc = ld_main(["--inspect", "uci-seeds"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "built_marker" in out          # built profile won
    assert "111" in out                    # built row count
    assert "tracked_marker" not in out    # tracked NOT rendered
    assert "999" not in out


def test_inspect_v2_falls_back_to_v1_profile(monkeypatch, capsys, tmp_path):
    """schema_version=2 with no docs/v2/profiles (not promoted) but the frozen
    docs/v1/profiles present → --inspect renders the v1 profile rather than
    reporting 'no profile'. Gap C: a bare docs/v{n} lookup misses the ~250 v1
    profiles the moment schema_version flips."""
    import json
    monkeypatch.setattr("raincloud.pipeline.list_datasets.outputs_root",
                        lambda manifest=None: tmp_path / "outputs" / "v2")
    monkeypatch.setattr("raincloud.pipeline.list_datasets.REPO_ROOT", tmp_path)
    monkeypatch.setattr("raincloud.pipeline.list_datasets.load_manifest",
                        lambda: {"schema_version": 2, "datasets": [
                            {"slug": "uci-seeds", "short_name": "UCI Seeds",
                             "full_name": "UCI Seeds", "description": "",
                             "license": {"spdx": "MIT"},
                             "tags": [], "showcase": []},
                        ]})

    # No docs/v2/profiles; only the frozen docs/v1 mirror exists.
    assert not (tmp_path / "docs" / "v2" / "profiles").exists()
    tracked_dir = tmp_path / "docs" / "v1" / "profiles"
    tracked_dir.mkdir(parents=True)
    (tracked_dir / "uci-seeds.json").write_text(json.dumps({
        "schema_version": 1, "slug": "uci-seeds", "row_count": 7777,
        "parquet_sha256": "0" * 64, "computed_at": "2026-05-12T00:00:00Z",
        "sample_rows": None,
        "columns": {
            "v1_fallback_marker": {"dtype": "float64", "null_count": 0,
                                   "min": 0.0, "max": 1.0, "mean": 0.5,
                                   "ndv_approx": 2,
                                   "histogram": {"buckets": [0.0, 1.0],
                                                 "counts": [7777]}},
        },
    }))

    from raincloud.pipeline.list_datasets import main as ld_main
    rc = ld_main(["--inspect", "uci-seeds"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "v1_fallback_marker" in out
    assert "7777" in out
    assert "no profile" not in out.lower()


def test_inspect_unknown_slug(monkeypatch, capsys):
    monkeypatch.setattr("raincloud.pipeline.list_datasets.load_manifest",
                        lambda: {"schema_version": 1, "datasets": []})
    from raincloud.pipeline.list_datasets import main as ld_main
    rc = ld_main(["--inspect", "no-such-slug"])
    assert rc == 2
    assert "no-such-slug" in capsys.readouterr().err


def test_tags_help_lists_vocab(monkeypatch, capsys, tmp_manifest_with_showcase):
    import raincloud.pipeline.list_datasets as ld_mod
    from raincloud.pipeline.discovery import TAG_VOCAB
    monkeypatch.setattr(ld_mod, "load_manifest", lambda: tmp_manifest_with_showcase)
    ld_mod.main(["--tags-help"])
    out = capsys.readouterr().out
    for tag in TAG_VOCAB:
        assert tag in out


def test_showcase_help_lists_tiers(monkeypatch, capsys, tmp_manifest_with_showcase):
    import raincloud.pipeline.list_datasets as ld_mod
    from raincloud.pipeline.discovery import SHOWCASE_TIERS
    monkeypatch.setattr(ld_mod, "load_manifest", lambda: tmp_manifest_with_showcase)
    ld_mod.main(["--showcase-help"])
    out = capsys.readouterr().out
    for tier in SHOWCASE_TIERS:
        assert tier in out


# ---------- --stale-version: artifacts that predate the current schema_version ----------

def _stub_snapshots(monkeypatch, current: int, per_version: dict[int, dict]):
    """Replace the per-version snapshot cache and pin the current schema_version."""
    from raincloud.pipeline import list_datasets as ld
    monkeypatch.setattr(ld, "_VERSION_SNAPSHOTS", dict(per_version))
    monkeypatch.setattr(ld, "_current_schema_version", lambda: current)
    return ld


def test_current_version_build_is_not_stale(monkeypatch):
    ld = _stub_snapshots(monkeypatch, 2, {
        1: {"demo": {"parquet_bytes": 10}},
        2: {"demo": {"parquet_bytes": 10, "arrow_sha256": "abc"}},
    })
    assert ld.built_version("demo") == 2
    assert ld.is_stale_version("demo") is False


def test_v1_era_record_is_stale(monkeypatch):
    ld = _stub_snapshots(monkeypatch, 2, {
        1: {"demo": {"parquet_bytes": 10}},
        2: {"demo": {"parquet_bytes": 10}},          # inherited, no v2 fingerprint
    })
    assert ld.built_version("demo") == 1
    assert ld.is_stale_version("demo") is True


def test_parquet_bytes_alone_does_not_prove_a_current_build(monkeypatch):
    """Regression: `parquet_bytes` / `last_built_rows` are carried forward by the
    snapshot-preservation fallback and are set for every slug in every version, so
    using them as the built-ness signal reported 0 stale slugs for a catalog with
    122. Only a version-introduced field (v2: arrow_sha256) proves the build."""
    ld = _stub_snapshots(monkeypatch, 2, {
        1: {"demo": {"parquet_bytes": 10, "last_built_rows": 5}},
        2: {"demo": {"parquet_bytes": 10, "last_built_rows": 5}},
    })
    assert ld.is_stale_version("demo") is True


def test_unknown_slug_is_neither_built_nor_stale(monkeypatch):
    ld = _stub_snapshots(monkeypatch, 2, {1: {}, 2: {}})
    assert ld.built_version("nope") is None
    assert ld.is_stale_version("nope") is False


def test_version_without_a_known_fingerprint_never_flags_staleness(monkeypatch):
    """A future schema_version with no entry in _VERSION_BUILD_MARKER must not
    declare the whole catalog stale — absence of a fingerprint is not evidence."""
    ld = _stub_snapshots(monkeypatch, 3, {
        2: {"demo": {"arrow_sha256": "abc"}},
        3: {"demo": {"parquet_bytes": 10}},
    })
    assert ld.built_version("demo") == 3
    assert ld.is_stale_version("demo") is False


def test_long_table_marks_stale_and_current(monkeypatch):
    from raincloud.pipeline.list_datasets import _render_long_table
    rows = [
        {"slug": "old", "handler": "h", "fetch_type": "http", "reader": "csv",
         "license": "MIT", "rows": 1, "vortex": True, "scrape_advisory": None,
         "hydrated": False, "built_version": 1, "stale_version": True},
        {"slug": "new", "handler": "h", "fetch_type": "http", "reader": "csv",
         "license": "MIT", "rows": 1, "vortex": True, "scrape_advisory": None,
         "hydrated": False, "built_version": 2, "stale_version": False},
        {"slug": "none", "handler": "h", "fetch_type": "http", "reader": "csv",
         "license": "MIT", "rows": 1, "vortex": True, "scrape_advisory": None,
         "hydrated": False, "built_version": None, "stale_version": False},
    ]
    rows[1]["local"] = ["arrow", "parquet"]
    table = _render_long_table(rows)
    # `recorded` is the version the tracked snapshot records as built; `local`
    # is what this machine has on disk.
    assert table.splitlines()[0].split()[-2:] == ["recorded", "local"]
    body = {ln.split()[0]: ln.split()[-2:] for ln in table.splitlines()[2:]}
    assert body["old"] == ["v1!", "·"]
    assert body["new"] == ["v2", "arrow,parquet"]
    assert body["none"] == ["·", "·"]
