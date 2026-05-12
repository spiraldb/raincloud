# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Tests for the derived-doc regenerator.

Specifically the snapshot-as-fallback behaviour: a maintainer who hasn't
built every parquet locally must still get an accurate `datasets.md`,
because regen would otherwise dash-out row counts / sizes for the slugs
they don't have on disk and silently destroy ground truth in the v1
snapshot.
"""
from __future__ import annotations

import json

import pytest

_FAKE_SPEC = {
    "slug": "fake-slug",
    "short_name": "Fake",
    "full_name": "Fake Dataset",
    "description": "A test dataset for snapshot-fallback verification.",
    "family": "direct",
    "license": {"spdx": "MIT", "source_url": "https://example.com/data"},
    "fetch": {"type": "http", "urls": ["https://example.com/data.parquet"]},
    "parse": {"reader": "parquet", "options": {}},
    "transform": {"handler": "identity", "params": {}},
    "expect": {"rows": 100},
}


@pytest.fixture
def patched_docs(tmp_path, monkeypatch):
    """Redirect docs.py I/O to tmp_path and stub the manifest + path helpers."""
    from scripts.pipeline import docs

    monkeypatch.setattr(docs, "DATASETS_MD", tmp_path / "datasets.md")
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", tmp_path / "snapshot.json")
    monkeypatch.setattr(
        docs, "load_manifest",
        lambda: {"schema_version": 1, "datasets": [dict(_FAKE_SPEC)]},
    )
    # prepared_parquet / prepared_vortex point at paths that never exist
    # → forces the code through the missing-on-disk branch.
    monkeypatch.setattr(docs, "prepared_parquet", lambda slug: tmp_path / "missing.parquet")
    monkeypatch.setattr(docs, "prepared_vortex", lambda slug: tmp_path / "missing.vortex")
    return docs, tmp_path


def _fake_slug_row(body: str) -> str:
    rows = [ln for ln in body.splitlines() if ln.startswith("|") and "Fake" in ln]
    assert len(rows) == 1, f"expected 1 Fake row, got {len(rows)}: {rows!r}"
    return rows[0]


def test_datasets_md_falls_back_to_snapshot_when_parquet_missing(patched_docs):
    """No parquet on disk + snapshot has the slug → row count + sizes come
    from snapshot.json instead of being dashed out."""
    docs, tmp_path = patched_docs
    (tmp_path / "snapshot.json").write_text(json.dumps({
        "schema_version": 1,
        "slugs": {
            "fake-slug": {
                "expected_rows": 100,
                "last_built_rows": 100,
                "last_built_row_groups": 2,
                "parquet_bytes": 5 * 1024 * 1024,   # → "5.0 MB"
                "vortex_bytes": 6 * 1024 * 1024,    # → "6.0 MB"
                "columns": [{"name": "id", "type": "int64"}],
            }
        }
    }))

    docs.generate_datasets_md()
    row = _fake_slug_row((tmp_path / "datasets.md").read_text())

    assert "100" in row, f"row count from snapshot missing: {row!r}"
    assert "2 " in row or "| 2 |" in row, f"row group count from snapshot missing: {row!r}"
    assert "5.0 MB" in row, f"parquet size from snapshot missing: {row!r}"
    assert "6.0 MB" in row, f"vortex size from snapshot missing: {row!r}"


def test_datasets_md_dashes_when_neither_disk_nor_snapshot(patched_docs):
    """No parquet on disk + slug not in snapshot → dashes (the safe fallback)."""
    docs, tmp_path = patched_docs
    (tmp_path / "snapshot.json").write_text(json.dumps({
        "schema_version": 1,
        "slugs": {},   # fake-slug deliberately absent
    }))

    docs.generate_datasets_md()
    row = _fake_slug_row((tmp_path / "datasets.md").read_text())

    # Four data-state cells: row count, row groups, parquet size, vortex size.
    # All four should render as "—" when there's nothing to fall back to.
    assert row.count("—") >= 4, (
        f"expected ≥4 dash placeholders for missing data, got: {row!r}"
    )


def test_datasets_md_dashes_when_snapshot_file_absent(patched_docs):
    """No parquet on disk + no snapshot.json at all → dashes (don't crash)."""
    docs, tmp_path = patched_docs
    # don't create snapshot.json at all
    assert not (tmp_path / "snapshot.json").exists()

    docs.generate_datasets_md()
    row = _fake_slug_row((tmp_path / "datasets.md").read_text())

    assert row.count("—") >= 4, (
        f"expected ≥4 dash placeholders when snapshot.json absent, got: {row!r}"
    )


def test_datasets_md_falls_back_to_tracked_v1_snapshot(tmp_path, monkeypatch):
    """No top-level scratch snapshot + tracked v{n} snapshot present →
    pull data from the tracked path. This is the fresh-clone case: the
    maintainer hasn't regenerated locally yet, but `docs/v1/snapshot.json`
    is in git.
    """
    from scripts.pipeline import docs

    v1_snap = tmp_path / "docs" / "v1" / "snapshot.json"
    v1_snap.parent.mkdir(parents=True)
    v1_snap.write_text(json.dumps({
        "schema_version": 1,
        "slugs": {
            "fake-slug": {
                "last_built_rows": 42,
                "last_built_row_groups": 1,
                "parquet_bytes": 7 * 1024 * 1024,
                "vortex_bytes": 8 * 1024 * 1024,
                "columns": [{"name": "id", "type": "int64"}],
            }
        }
    }))

    monkeypatch.setattr(docs, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(docs, "DATASETS_MD", tmp_path / "datasets.md")
    # Top-level scratch path deliberately doesn't exist.
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", tmp_path / "docs" / "snapshot.json")
    monkeypatch.setattr(
        docs, "load_manifest",
        lambda: {"schema_version": 1, "datasets": [dict(_FAKE_SPEC)]},
    )
    monkeypatch.setattr(docs, "prepared_parquet", lambda slug: tmp_path / "missing.parquet")
    monkeypatch.setattr(docs, "prepared_vortex", lambda slug: tmp_path / "missing.vortex")

    docs.generate_datasets_md()
    row = _fake_slug_row((tmp_path / "datasets.md").read_text())

    assert "42" in row, f"row count from tracked v1 snapshot missing: {row!r}"
    assert "7.0 MB" in row, f"parquet size from tracked v1 snapshot missing: {row!r}"
    assert "8.0 MB" in row, f"vortex size from tracked v1 snapshot missing: {row!r}"


def test_snapshot_captures_row_groups_for_built_slugs(tmp_path, monkeypatch):
    """The snapshot regen path must record `last_built_row_groups` so that
    the datasets.md fallback has it available."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.pipeline import docs

    # Build a real tiny parquet so generate_snapshot's metadata read succeeds.
    fake_pq_dir = tmp_path / "outputs" / "v1" / "fake-slug" / "parquet"
    fake_pq_dir.mkdir(parents=True)
    fake_pq = fake_pq_dir / "fake-slug.parquet"
    table = pa.table({"id": pa.array([1, 2, 3], type=pa.int64())})
    # Force 2 row groups so the captured value isn't trivially 1.
    pq.write_table(table, fake_pq, row_group_size=2)
    assert pq.ParquetFile(fake_pq).metadata.num_row_groups == 2

    snapshot_path = tmp_path / "snapshot.json"
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", snapshot_path)
    monkeypatch.setattr(
        docs, "load_manifest",
        lambda: {"schema_version": 1, "datasets": [dict(_FAKE_SPEC)]},
    )
    monkeypatch.setattr(docs, "prepared_parquet", lambda slug: fake_pq)
    monkeypatch.setattr(docs, "prepared_vortex", lambda slug: tmp_path / "missing.vortex")

    docs.generate_snapshot(overwrite_missing=True)

    snap = json.loads(snapshot_path.read_text())
    entry = snap["slugs"]["fake-slug"]
    assert entry["last_built_rows"] == 3
    assert entry["last_built_row_groups"] == 2


def test_snapshot_has_size_bucket_per_slug(tmp_path):
    """_snapshot_for_slug emits size_bucket from on-disk parquet bytes."""
    from scripts.pipeline import docs as docs_mod
    from scripts.pipeline.discovery import SIZE_BUCKETS

    parquet = tmp_path / "outputs" / "v1" / "fake" / "parquet" / "fake.parquet"
    parquet.parent.mkdir(parents=True)
    parquet.write_bytes(b"x" * (50 * 1024 * 1024))   # 50 MB → "s"

    snapshot = docs_mod._snapshot_for_slug(
        slug="fake",
        parquet_path=parquet,
        prior_snapshot=None,
    )
    assert snapshot["size_bucket"] == "s"
    assert snapshot["size_bucket"] in SIZE_BUCKETS


def test_snapshot_size_bucket_falls_back_to_prior(tmp_path):
    """When the parquet is missing, the prior snapshot's value is preserved."""
    from scripts.pipeline import docs as docs_mod

    prior = {"size_bucket": "xl"}
    snapshot = docs_mod._snapshot_for_slug(
        slug="fake",
        parquet_path=tmp_path / "absent.parquet",
        prior_snapshot=prior,
    )
    assert snapshot["size_bucket"] == "xl"


def test_snapshot_size_bucket_unknown_when_no_data(tmp_path):
    """No parquet, no prior — bucket is null."""
    from scripts.pipeline import docs as docs_mod

    snapshot = docs_mod._snapshot_for_slug(
        slug="fake",
        parquet_path=tmp_path / "absent.parquet",
        prior_snapshot=None,
    )
    assert snapshot.get("size_bucket") is None


def test_shape_traits_from_schema_flat_string_only():
    import pyarrow as pa
    from scripts.pipeline.docs import _shape_traits_from_schema

    schema = pa.schema([("a", pa.string()), ("b", pa.string())])
    traits = _shape_traits_from_schema(schema)
    assert traits["has_nested"] is False
    assert traits["has_timestamp"] is False
    assert traits["has_variant"] is False
    assert traits["string_heavy"] is True
    assert traits["wide_row"] is False
    assert traits["high_cardinality_present"] is None


def test_shape_traits_from_schema_nested_timestamp_wide():
    import pyarrow as pa
    from scripts.pipeline.docs import _shape_traits_from_schema

    fields = [(f"col{i}", pa.int32()) for i in range(60)] + [
        ("nested", pa.list_(pa.int32())),
        ("ts", pa.timestamp("us")),
    ]
    schema = pa.schema(fields)
    traits = _shape_traits_from_schema(schema)
    assert traits["has_nested"] is True
    assert traits["has_timestamp"] is True
    assert traits["wide_row"] is True
    assert traits["string_heavy"] is False
    assert traits["high_cardinality_present"] is None


def test_shape_traits_detects_variant_via_metadata():
    """VARIANT in raincloud is stored as a struct with a `__variant_type` marker
    in pyarrow field metadata."""
    import pyarrow as pa
    from scripts.pipeline.docs import _shape_traits_from_schema

    inner = pa.struct([("v", pa.binary())])
    meta_field = pa.field("data", inner, metadata={b"__variant_type": b"1"})
    schema = pa.schema([meta_field])
    traits = _shape_traits_from_schema(schema)
    assert traits["has_variant"] is True
    assert traits["has_nested"] is True   # struct is nested


def test_high_cardinality_present_from_profile(tmp_path):
    """When profile.json exists, docs.py sets the trait flag from string NDVs."""
    from scripts.pipeline import docs as docs_mod

    profile = tmp_path / "profile.json"
    profile.write_text(json.dumps({
        "schema_version": 1, "slug": "fake", "row_count": 1_000_000,
        "parquet_sha256": "0" * 64, "computed_at": "2026-05-12T00:00:00Z",
        "sample_rows": None,
        "columns": {
            "id":   {"dtype": "string", "null_count": 0, "ndv_approx": 950_000,
                     "mean_length": 12.0, "top_values": None},
            "kind": {"dtype": "string", "null_count": 0, "ndv_approx": 5,
                     "mean_length": 4.0,
                     "top_values": [{"value": "a", "count": 200_000}]},
        },
    }) + "\n")

    flag = docs_mod._high_cardinality_from_profile(profile)
    assert flag is True


def test_high_cardinality_present_false_when_all_low(tmp_path):
    from scripts.pipeline import docs as docs_mod

    profile = tmp_path / "profile.json"
    profile.write_text(json.dumps({
        "schema_version": 1, "slug": "fake", "row_count": 1_000_000,
        "parquet_sha256": "0" * 64, "computed_at": "2026-05-12T00:00:00Z",
        "sample_rows": None,
        "columns": {
            "kind": {"dtype": "string", "null_count": 0, "ndv_approx": 5,
                     "mean_length": 4.0,
                     "top_values": [{"value": "a", "count": 200_000}]},
        },
    }) + "\n")
    assert docs_mod._high_cardinality_from_profile(profile) is False


def test_high_cardinality_present_null_when_no_profile(tmp_path):
    from scripts.pipeline import docs as docs_mod
    assert docs_mod._high_cardinality_from_profile(tmp_path / "missing.json") is None


def test_datasets_md_carries_curated_picks_header():
    """docs.py emits a curated-picks block keyed by SHOWCASE_TIERS before the table."""
    from scripts.pipeline import docs as docs_mod
    from scripts.pipeline.discovery import SHOWCASE_TIERS

    manifest = {"schema_version": 1, "datasets": [
        {"slug": "s1", "short_name": "S1", "full_name": "S1",
         "description": "lorem", "family": "uci",
         "license": {"spdx": "MIT"},
         "fetch": {"type": "http", "urls": []}, "extract": {}, "parse": {},
         "transform": {}, "write": {}, "expect": {},
         "tags": [], "showcase": ["start-here"]},
        {"slug": "s2", "short_name": "S2", "full_name": "S2",
         "description": "ipsum", "family": "uci",
         "license": {"spdx": "MIT"},
         "fetch": {"type": "http", "urls": []}, "extract": {}, "parse": {},
         "transform": {}, "write": {}, "expect": {},
         "tags": [], "showcase": ["encoding-research"]},
    ]}
    md = docs_mod._render_curated_picks(manifest)
    # All 4 tiers appear (either by slug or by titled form).
    for tier in SHOWCASE_TIERS:
        assert tier in md or tier.replace("-", " ").title() in md
    assert "s1" in md
    assert "s2" in md


def test_curated_picks_empty_tier_placeholder():
    """Tiers with no members render with a placeholder, not an empty block."""
    from scripts.pipeline import docs as docs_mod

    manifest = {"schema_version": 1, "datasets": []}
    md = docs_mod._render_curated_picks(manifest)
    # Each of the 4 tiers should have the "no picks yet" placeholder text.
    assert md.count("No picks yet") >= 4
