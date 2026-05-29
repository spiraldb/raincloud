# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import json

import pytest


class _FakePackaged:
    """Stand-in for an importlib.resources traversable pointing at a wheel
    `raincloud/_data/<name>` copy, so the packaged-fallback branch can be
    exercised without building a wheel."""
    def __init__(self, path):
        self._path = path

    def is_file(self):
        return True

    def __str__(self):
        return str(self._path)


@pytest.mark.parametrize("kind,repo_rel,packaged_name", [
    ("snapshot", "docs/v1/snapshot.json", "snapshot.json"),
    ("manifest", "sources.json", "sources.json"),
])
def test_data_file_prefers_repo_over_packaged(tmp_path, monkeypatch, kind, repo_rel, packaged_name):
    """Finding 5: precedence must be env -> repo checkout -> wheel-packaged,
    matching scripts.pipeline.spec._default_manifest and the documented intent.
    A checkout copy must win over a packaged copy when both exist."""
    from importlib import resources

    from raincloud import _catalog

    env = {"snapshot": "RAINCLOUD_SNAPSHOT", "manifest": "RAINCLOUD_MANIFEST"}[kind]
    monkeypatch.delenv(env, raising=False)

    repo = tmp_path / "repo"
    repo_file = repo / repo_rel
    repo_file.parent.mkdir(parents=True, exist_ok=True)
    repo_file.write_text("{}")
    packaged_file = tmp_path / "wheel_data" / packaged_name
    packaged_file.parent.mkdir(parents=True, exist_ok=True)
    packaged_file.write_text("{}")

    monkeypatch.setattr(_catalog, "_repo_root", lambda: repo)
    monkeypatch.setattr(resources, "files", lambda pkg: _Joiner(packaged_file))

    # Both present -> repo wins.
    assert _catalog._data_file(kind) == repo_file
    # Repo absent -> packaged wins (the wheel-install path).
    repo_file.unlink()
    assert str(_catalog._data_file(kind)) == str(packaged_file)


class _Joiner:
    def __init__(self, path):
        self._path = path

    def joinpath(self, *parts):
        return _FakePackaged(self._path)


@pytest.fixture
def fake_catalog(tmp_path, monkeypatch):
    snapshot = {
        "schema_version": 1,
        "slugs": {
            "tiny": {
                "expected_rows": 3, "last_built_rows": 3,
                "parquet_bytes": 100, "vortex_bytes": 120,
                "parquet_sha256": "aa", "vortex_sha256": "bb",
                "columns": [{"name": "x", "type": "int64"}],
            },
            "pq_only": {
                "expected_rows": 5, "last_built_rows": 5,
                "parquet_bytes": 50, "vortex_bytes": None,
                "parquet_sha256": "cc", "vortex_sha256": None,
                "columns": [{"name": "y", "type": "string"}],
            },
        },
    }
    manifest = {"schema_version": 1, "datasets": [
        {"slug": "tiny", "short_name": "Tiny", "full_name": "Tiny set",
         "description": "a tiny set",
         "license": {"spdx": "CC0-1.0", "source_url": "http://x",
                     "redistribution_permitted": True, "attribution_required": False},
         "fetch": {"urls": ["http://src/tiny.csv"]}},
        {"slug": "pq_only", "short_name": "PQ", "full_name": "PQ only",
         "description": "p", "license": {"spdx": "MIT", "source_url": "http://y"},
         "fetch": {"urls": ["http://src/pq.csv"]}},
    ]}
    sp = tmp_path / "snapshot.json"; sp.write_text(json.dumps(snapshot))
    mp = tmp_path / "sources.json"; mp.write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(sp))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(mp))
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    yield _catalog.load_catalog()
    _catalog.load_catalog.cache_clear()


def test_entry_formats_and_checksums(fake_catalog):
    e = fake_catalog.entry("tiny")
    assert e.rows == 3
    assert set(e.formats) == {"parquet", "vortex"}
    assert e.formats["vortex"].sha256 == "bb"
    assert e.formats["parquet"].nbytes == 100


def test_format_availability_excludes_missing(fake_catalog):
    e = fake_catalog.entry("pq_only")
    assert set(e.formats) == {"parquet"}  # vortex_bytes was None


def test_info_fields(fake_catalog):
    e = fake_catalog.entry("tiny")
    assert e.info["short_name"] == "Tiny"
    assert e.info["license"]["spdx"] == "CC0-1.0"
    assert e.info["source_url"] == "http://src/tiny.csv"
    assert e.column_names == ["x"]


def test_unknown_slug(fake_catalog):
    from raincloud.exceptions import UnknownSlug
    assert "nope" not in fake_catalog
    with pytest.raises(UnknownSlug):
        fake_catalog.entry("nope")


def test_entry_formats_from_manifest_only(tmp_path, monkeypatch):
    """A manifest slug never written to the snapshot should still be loadable.

    Manifest declares the slug + convert.vortex=true; snapshot has no bytes/sha
    for it. Both parquet and vortex must be exposed with None checksums.
    """
    import json
    snapshot = {"schema_version": 1, "slugs": {"new_slug": {"expected_rows": 100}}}
    manifest = {"schema_version": 1, "datasets": [{
        "slug": "new_slug", "short_name": "New", "license": {},
        "fetch": {"urls": []}, "convert": {"vortex": True},
    }]}
    sp = tmp_path / "snapshot.json"; sp.write_text(json.dumps(snapshot))
    mp = tmp_path / "sources.json"; mp.write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(sp))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(mp))
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    try:
        e = _catalog.load_catalog().entry("new_slug")
        assert set(e.formats) == {"parquet", "vortex"}
        assert e.formats["parquet"].sha256 is None
        assert e.formats["parquet"].nbytes is None
        assert e.formats["vortex"].sha256 is None
        assert e.formats["vortex"].nbytes is None
        # rows still resolves from expected_rows in snapshot
        assert e.rows == 100
    finally:
        _catalog.load_catalog.cache_clear()


def test_entry_parquet_visible_for_snapshot_only_slug(tmp_path, monkeypatch):
    """A slug present in the snapshot but dropped from the manifest (legacy/
    deprecated entries still on a mirror) must still expose parquet — the
    snapshot's recorded parquet_bytes is the loader's signal that bytes exist.
    Mirrors the vortex_bytes fallback that already covers the same case.
    """
    snapshot = {"schema_version": 1, "slugs": {"legacy": {
        "expected_rows": 7, "last_built_rows": 7,
        "parquet_bytes": 999, "parquet_sha256": "ff" * 32,
    }}}
    manifest = {"schema_version": 1, "datasets": []}
    sp = tmp_path / "snapshot.json"; sp.write_text(json.dumps(snapshot))
    mp = tmp_path / "sources.json"; mp.write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(sp))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(mp))
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    try:
        e = _catalog.load_catalog().entry("legacy")
        assert "parquet" in e.formats
        assert e.formats["parquet"].sha256 == "ff" * 32
        assert e.formats["parquet"].nbytes == 999
    finally:
        _catalog.load_catalog.cache_clear()


def test_entry_no_vortex_when_convert_vortex_false(tmp_path, monkeypatch):
    """A manifest slug with convert.vortex=false (or absent) exposes parquet only."""
    import json
    snapshot = {"schema_version": 1, "slugs": {"pq_slug": {"expected_rows": 50}}}
    manifest = {"schema_version": 1, "datasets": [{
        "slug": "pq_slug", "short_name": "PQ", "license": {},
        "fetch": {"urls": []}, "convert": {"vortex": False},
    }]}
    sp = tmp_path / "snapshot.json"; sp.write_text(json.dumps(snapshot))
    mp = tmp_path / "sources.json"; mp.write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(sp))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(mp))
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    try:
        e = _catalog.load_catalog().entry("pq_slug")
        assert set(e.formats) == {"parquet"}
    finally:
        _catalog.load_catalog.cache_clear()
