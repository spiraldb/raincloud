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
    "license": {"spdx": "MIT", "source_url": "https://example.com/data"},
    "fetch": {"type": "http", "urls": ["https://example.com/data.parquet"]},
    "parse": {"reader": "parquet", "options": {}},
    "transform": {"handler": "identity", "params": {}},
    "expect": {"rows": 100},
}



def _artifacts(tmp_path, paths):
    """A `prepared_artifact` stand-in: `paths[fmt]`, else a file that never exists."""
    return lambda slug, fmt, manifest=None: paths.get(fmt, tmp_path / f"missing.{fmt}")

@pytest.fixture
def patched_docs(tmp_path, monkeypatch):
    """Redirect docs.py I/O to tmp_path and stub the manifest + path helpers."""
    from raincloud.pipeline import docs

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
    monkeypatch.setattr(docs, "prepared_artifact", _artifacts(tmp_path, {"parquet": tmp_path / "missing.parquet", "vortex": tmp_path / "missing.vortex", "arrow": tmp_path / "missing.arrow.zstd"}))
    return docs, tmp_path


@pytest.mark.parametrize("present", ["parquet", "arrow"])
@pytest.mark.parametrize("overwrite", [False, True])
def test_snapshot_preserves_absent_formats_individually(patched_docs, monkeypatch, present, overwrite):
    import pyarrow as pa
    import pyarrow.parquet as pq

    docs, root = patched_docs
    prior = {"last_built_rows": 99, "columns": [{"name": "old", "type": "string"}],
             "parquet_bytes": 101, "parquet_sha256": "p", "vortex_bytes": 102,
             "vortex_sha256": "v", "arrow_bytes": 103, "arrow_sha256": "a"}
    docs.SNAPSHOT_JSON.write_text(json.dumps({"schema_version": 1, "slugs": {"fake-slug": prior}}))
    table = pa.table({"new": [1, 2]})
    path = root / f"local.{present}"
    monkeypatch.setattr(docs, "prepared_artifact", _artifacts(root, {present: path}))
    if present == "parquet":
        pq.write_table(table, path)
    else:
        with pa.OSFile(str(path), "wb") as sink:
            with pa.ipc.new_file(sink, table.schema) as writer:
                writer.write_table(table)
    docs.generate_snapshot(overwrite_missing=overwrite)
    record = json.loads(docs.SNAPSHOT_JSON.read_text())["slugs"]["fake-slug"]
    assert record["last_built_rows"] == 2
    assert record["columns"][0]["name"] == "new"
    for fmt in ("parquet", "vortex", "arrow"):
        if fmt == present:
            assert record[f"{fmt}_bytes"] == path.stat().st_size
            assert len(record[f"{fmt}_sha256"]) == 64
        elif overwrite:
            assert record.get(f"{fmt}_bytes") is None
            assert record.get(f"{fmt}_sha256") is None
        else:
            assert record[f"{fmt}_bytes"] == prior[f"{fmt}_bytes"]
            assert record[f"{fmt}_sha256"] == prior[f"{fmt}_sha256"]


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
    from raincloud.pipeline import docs

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
    monkeypatch.setattr(docs, "prepared_artifact", _artifacts(tmp_path, {"parquet": tmp_path / "missing.parquet", "vortex": tmp_path / "missing.vortex"}))

    docs.generate_datasets_md()
    row = _fake_slug_row((tmp_path / "datasets.md").read_text())

    assert "42" in row, f"row count from tracked v1 snapshot missing: {row!r}"
    assert "7.0 MB" in row, f"parquet size from tracked v1 snapshot missing: {row!r}"
    assert "8.0 MB" in row, f"vortex size from tracked v1 snapshot missing: {row!r}"


def test_load_snapshot_slugs_does_not_borrow_v1_at_v2(tmp_path, monkeypatch, capsys):
    """schema_version=2 with no docs/v2 snapshot → `{}`: the frozen docs/v1
    snapshot describes other artifacts and is never borrowed. A v1 snapshot in
    the scratch slot is skipped with a warning naming it, not read."""
    from raincloud.pipeline import docs

    v1_snap = tmp_path / "docs" / "v1" / "snapshot.json"
    v1_snap.parent.mkdir(parents=True)
    v1_snap.write_text(json.dumps({"schema_version": 1, "slugs": {
        "fake-slug": {"last_built_rows": 5}}}))
    monkeypatch.setattr(docs, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", tmp_path / "docs" / "snapshot.json")
    assert not (tmp_path / "docs" / "v2" / "snapshot.json").exists()

    assert docs._load_snapshot_slugs(2) == {}

    scratch = tmp_path / "docs" / "snapshot.json"
    scratch.write_text(v1_snap.read_text())
    assert docs._load_snapshot_slugs(2) == {}
    err = capsys.readouterr().err
    assert f"ignoring {scratch}: schema_version 1, the manifest is 2" in err


def test_datasets_md_v2_does_not_use_v1_snapshot(tmp_path, monkeypatch):
    """schema_version=2 with only docs/v1/snapshot.json present (no docs/v2, no
    scratch) → an unbuilt slug's row count / sizes are dashed, not copied from
    the v1 snapshot, whose numbers describe v1 artifacts."""
    from raincloud.pipeline import docs

    v1_snap = tmp_path / "docs" / "v1" / "snapshot.json"
    v1_snap.parent.mkdir(parents=True)
    v1_snap.write_text(json.dumps({"schema_version": 1, "slugs": {
        "fake-slug": {
            "last_built_rows": 42, "last_built_row_groups": 1,
            "parquet_bytes": 7 * 1024 * 1024, "vortex_bytes": 8 * 1024 * 1024,
            "columns": [{"name": "id", "type": "int64"}],
        }}}))
    monkeypatch.setattr(docs, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(docs, "DATASETS_MD", tmp_path / "datasets.md")
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", tmp_path / "docs" / "snapshot.json")
    monkeypatch.setattr(docs, "load_manifest",
                        lambda: {"schema_version": 2, "datasets": [dict(_FAKE_SPEC)]})
    monkeypatch.setattr(docs, "prepared_parquet", lambda slug: tmp_path / "missing.parquet")
    monkeypatch.setattr(docs, "prepared_vortex", lambda slug: tmp_path / "missing.vortex")
    monkeypatch.setattr(docs, "prepared_artifact", _artifacts(tmp_path, {"parquet": tmp_path / "missing.parquet", "vortex": tmp_path / "missing.vortex"}))

    docs.generate_datasets_md()
    row = _fake_slug_row((tmp_path / "datasets.md").read_text())
    assert "42" not in row, f"v1 row count leaked into the v2 table: {row!r}"
    assert "7.0 MB" not in row and "8.0 MB" not in row, f"v1 sizes leaked into v2: {row!r}"
    assert row.count("—") >= 3, f"unbuilt v2 slug should be dashed: {row!r}"


def test_snapshot_captures_row_groups_for_built_slugs(tmp_path, monkeypatch):
    """The snapshot regen path must record `last_built_row_groups` so that
    the datasets.md fallback has it available."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    from raincloud.pipeline import docs

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
    monkeypatch.setattr(docs, "prepared_artifact", _artifacts(tmp_path, {"parquet": fake_pq, "vortex": tmp_path / "missing.vortex"}))

    docs.generate_snapshot(overwrite_missing=True)

    snap = json.loads(snapshot_path.read_text())
    entry = snap["slugs"]["fake-slug"]
    assert entry["last_built_rows"] == 3
    assert entry["last_built_row_groups"] == 2


def test_snapshot_captures_arrow_when_present(tmp_path, monkeypatch):
    """An arrow artifact on disk → the snapshot record carries arrow_bytes +
    arrow_sha256 (the keys the loader's _catalog already reads), mirroring the
    parquet/vortex capture."""
    import hashlib

    from raincloud.pipeline import docs

    arrow_dir = tmp_path / "outputs" / "v1" / "fake-slug" / "arrow"
    arrow_dir.mkdir(parents=True)
    arrow = arrow_dir / "fake-slug.arrow.zstd"
    payload = b"ARROW-IPC-ZSTD-BYTES"
    arrow.write_bytes(payload)

    snapshot_path = tmp_path / "snapshot.json"
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", snapshot_path)
    monkeypatch.setattr(
        docs, "load_manifest",
        lambda: {"schema_version": 1, "datasets": [dict(_FAKE_SPEC)]},
    )
    monkeypatch.setattr(docs, "prepared_parquet", lambda slug: tmp_path / "missing.parquet")
    monkeypatch.setattr(docs, "prepared_vortex", lambda slug: tmp_path / "missing.vortex")
    monkeypatch.setattr(docs, "prepared_artifact", _artifacts(tmp_path, {"parquet": tmp_path / "missing.parquet", "vortex": tmp_path / "missing.vortex", "arrow": arrow}))

    docs.generate_snapshot(overwrite_missing=True)

    entry = json.loads(snapshot_path.read_text())["slugs"]["fake-slug"]
    assert entry["arrow_bytes"] == len(payload)
    assert entry["arrow_sha256"] == hashlib.sha256(payload).hexdigest()


def test_snapshot_omits_arrow_keys_when_absent(tmp_path, monkeypatch):
    """No arrow artifact on disk → the snapshot record carries NO arrow keys, so
    the current catalog's snapshot stays unchanged (additive readiness only)."""
    from raincloud.pipeline import docs

    snapshot_path = tmp_path / "snapshot.json"
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", snapshot_path)
    monkeypatch.setattr(
        docs, "load_manifest",
        lambda: {"schema_version": 1, "datasets": [dict(_FAKE_SPEC)]},
    )
    monkeypatch.setattr(docs, "prepared_parquet", lambda slug: tmp_path / "missing.parquet")
    monkeypatch.setattr(docs, "prepared_vortex", lambda slug: tmp_path / "missing.vortex")
    monkeypatch.setattr(docs, "prepared_artifact", _artifacts(tmp_path, {"parquet": tmp_path / "missing.parquet", "vortex": tmp_path / "missing.vortex", "arrow": tmp_path / "missing.arrow.zstd"}))

    docs.generate_snapshot(overwrite_missing=True)

    entry = json.loads(snapshot_path.read_text())["slugs"]["fake-slug"]
    assert "arrow_bytes" not in entry
    assert "arrow_sha256" not in entry


def test_snapshot_arrow_only_slug_written_on_default_regen(tmp_path, monkeypatch):
    """The DEFAULT regen (overwrite_missing=False, what `python -m raincloud.pipeline.docs`
    runs) must still capture an arrow-only slug: with a prior entry present and
    parquet/vortex absent, retention comes solely from the `fresh_has_data` arrow
    term, so the fresh arrow-bearing record replaces the prior entry."""
    import hashlib

    from raincloud.pipeline import docs

    arrow_dir = tmp_path / "outputs" / "v1" / "fake-slug" / "arrow"
    arrow_dir.mkdir(parents=True)
    arrow = arrow_dir / "fake-slug.arrow.zstd"
    payload = b"ARROW-ONLY"
    arrow.write_bytes(payload)

    snapshot_path = tmp_path / "snapshot.json"
    # Prior entry present → `slug not in existing_slugs` is False, so retention
    # must be driven by fresh_has_data (the arrow term), not the fresh-slug path.
    snapshot_path.write_text(json.dumps({"slugs": {"fake-slug": {"expected_rows": 3}}}))
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", snapshot_path)
    monkeypatch.setattr(
        docs, "load_manifest",
        lambda: {"schema_version": 1, "datasets": [dict(_FAKE_SPEC)]},
    )
    monkeypatch.setattr(docs, "prepared_parquet", lambda slug: tmp_path / "missing.parquet")
    monkeypatch.setattr(docs, "prepared_vortex", lambda slug: tmp_path / "missing.vortex")
    monkeypatch.setattr(docs, "prepared_artifact", _artifacts(tmp_path, {"parquet": tmp_path / "missing.parquet", "vortex": tmp_path / "missing.vortex", "arrow": arrow}))

    docs.generate_snapshot(overwrite_missing=False)

    entry = json.loads(snapshot_path.read_text())["slugs"]["fake-slug"]
    assert entry["arrow_bytes"] == len(payload)
    assert entry["arrow_sha256"] == hashlib.sha256(payload).hexdigest()


def test_snapshot_has_size_bucket_per_slug(tmp_path):
    """_snapshot_for_slug emits size_bucket from on-disk parquet bytes."""
    from raincloud.pipeline import docs as docs_mod
    from raincloud.pipeline.discovery import SIZE_BUCKETS

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
    from raincloud.pipeline import docs as docs_mod

    prior = {"size_bucket": "xl"}
    snapshot = docs_mod._snapshot_for_slug(
        slug="fake",
        parquet_path=tmp_path / "absent.parquet",
        prior_snapshot=prior,
    )
    assert snapshot["size_bucket"] == "xl"


def test_snapshot_size_bucket_unknown_when_no_data(tmp_path):
    """No parquet, no prior — bucket is null."""
    from raincloud.pipeline import docs as docs_mod

    snapshot = docs_mod._snapshot_for_slug(
        slug="fake",
        parquet_path=tmp_path / "absent.parquet",
        prior_snapshot=None,
    )
    assert snapshot.get("size_bucket") is None


def test_shape_traits_from_schema_flat_string_only():
    import pyarrow as pa

    from raincloud.pipeline.docs import _shape_traits_from_schema

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

    from raincloud.pipeline.docs import _shape_traits_from_schema

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

    from raincloud.pipeline.docs import _shape_traits_from_schema

    inner = pa.struct([("v", pa.binary())])
    meta_field = pa.field("data", inner, metadata={b"__variant_type": b"1"})
    schema = pa.schema([meta_field])
    traits = _shape_traits_from_schema(schema)
    assert traits["has_variant"] is True
    assert traits["has_nested"] is True   # struct is nested


def test_high_cardinality_present_from_profile(tmp_path):
    """When profile.json exists, docs.py sets the trait flag from string NDVs."""
    from raincloud.pipeline import docs as docs_mod

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
    from raincloud.pipeline import docs as docs_mod

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
    from raincloud.pipeline import docs as docs_mod
    assert docs_mod._high_cardinality_from_profile(tmp_path / "missing.json") is None


def test_datasets_md_carries_curated_picks_header():
    """docs.py emits a curated-picks block keyed by SHOWCASE_TIERS before the table."""
    from raincloud.pipeline import docs as docs_mod
    from raincloud.pipeline.discovery import SHOWCASE_TIERS

    manifest = {"schema_version": 1, "datasets": [
        {"slug": "s1", "short_name": "S1", "full_name": "S1",
         "description": "lorem",
         "license": {"spdx": "MIT"},
         "fetch": {"type": "http", "urls": []}, "extract": {}, "parse": {},
         "transform": {}, "write": {}, "expect": {},
         "tags": [], "showcase": ["encoding"]},
        {"slug": "s2", "short_name": "S2", "full_name": "S2",
         "description": "ipsum",
         "license": {"spdx": "MIT"},
         "fetch": {"type": "http", "urls": []}, "extract": {}, "parse": {},
         "transform": {}, "write": {}, "expect": {},
         "tags": [], "showcase": ["stress"]},
    ]}
    md = docs_mod._render_curated_picks(manifest)
    # All 4 tiers appear (either by slug or by titled form).
    for tier in SHOWCASE_TIERS:
        assert tier in md or tier.replace("-", " ").title() in md
    assert "s1" in md
    assert "s2" in md


def test_curated_picks_empty_tier_placeholder():
    """Tiers with no members render with a placeholder, not an empty block."""
    from raincloud.pipeline import docs as docs_mod

    manifest = {"schema_version": 1, "datasets": []}
    md = docs_mod._render_curated_picks(manifest)
    # Each tier should have the "no picks yet" placeholder text.
    assert md.count("No picks yet") >= 2


def test_snapshot_regen_preserves_from_tracked_snapshot_without_scratch_copy(tmp_path, monkeypatch):
    """A regen with NO scratch `docs/snapshot.json` must preserve unbuilt slugs
    from the tracked `docs/v{n}/snapshot.json`.

    Preservation used to read the gitignored top-level scratch file ONLY, so the
    regen's safety silently depended on an undocumented
    `cp docs/v{n}/snapshot.json docs/snapshot.json` first. On any tree without
    that copy — a fresh clone, or one where it was cleaned — preservation found
    nothing and every not-built-locally slug had its `last_built_rows` /
    `parquet_bytes` / `vortex_bytes` / `columns` nulled. Promoting the result
    destroyed ground truth the committed snapshot was the only record of
    (observed: 617 fields across 122 slugs).
    """
    import json

    from raincloud.pipeline import docs

    tracked = tmp_path / "docs" / "v2" / "snapshot.json"
    tracked.parent.mkdir(parents=True)
    tracked.write_text(json.dumps({
        "schema_version": 2,
        "slugs": {"never-built-here": {
            "last_built_rows": 12345,
            "parquet_bytes": 999,
            "parquet_sha256": "a" * 64,
        }},
    }))
    scratch = tmp_path / "docs" / "snapshot.json"
    assert not scratch.exists()  # the condition that used to null everything

    monkeypatch.setattr(docs, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", scratch)

    # The tracked copy is found via the manifest's schema_version.
    found = docs._tracked_snapshot_json({"schema_version": 2})
    assert found == tracked and found.exists()
    preserved = json.loads(found.read_text())["slugs"]["never-built-here"]
    assert preserved["last_built_rows"] == 12345
    assert preserved["parquet_sha256"] == "a" * 64


def test_tracked_snapshot_path_is_version_scoped():
    """`_tracked_snapshot_json` follows the manifest's schema_version, and
    returns None rather than guessing when it is absent."""
    from raincloud.pipeline import docs

    assert docs._tracked_snapshot_json({"schema_version": 1}).parent.name == "v1"
    assert docs._tracked_snapshot_json({"schema_version": 2}).parent.name == "v2"
    assert docs._tracked_snapshot_json({}) is None


def test_docs_cli_rejects_unknown_flags_and_targets():
    """`docs` REGENERATES derived artifacts, so an unrecognized token must be an error.

    The hand-rolled parser this replaced treated any unknown token as "no targets
    given" and fell through to regenerating all three artifacts -- so `--help`, or any
    typo, silently rewrote datasets.md, handlers.md and snapshot.json.
    """
    import pytest

    from raincloud.pipeline.docs import TARGETS, _parse_args

    assert _parse_args([]).targets == list(TARGETS)
    assert _parse_args(["snapshot"]).targets == ["snapshot"]
    assert _parse_args(["snapshot", "--rehash"]).rehash is True

    # A typo must be an error, never a silent full regeneration.
    for bad in (["--dry-run"], ["datsets"], ["snapshot", "handlrs"]):
        with pytest.raises(SystemExit) as exc:
            _parse_args(bad)
        assert exc.value.code != 0, f"{bad} must not fall through to a full regeneration"

    # --help exits 0 (help is a successful action) but must still not regenerate.
    with pytest.raises(SystemExit) as exc:
        _parse_args(["--help"])
    assert exc.value.code == 0


def test_docs_main_does_no_work_before_parsing():
    """A rejected invocation must not acquire the store lock or touch the filesystem.

    The earlier regression test asserted only an exit code, which a version that
    locked-then-parsed would still pass. `main()` parses first, so a typo costs
    nothing: no exclusive lock on the data root, no stat of every artifact path.
    """
    import pytest

    from raincloud.pipeline import docs, lifecycle

    called = []
    original = lifecycle.operation_lock
    lifecycle.operation_lock = lambda *a, **k: called.append(1)
    try:
        for bad in (["datsets"], ["--dry-run"]):
            with pytest.raises(SystemExit):
                docs.main(bad)
        assert not called, "main() took the store lock before rejecting the arguments"
    finally:
        lifecycle.operation_lock = original
