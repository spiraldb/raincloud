import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _sha(p) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


@pytest.fixture
def loaded(tmp_path, monkeypatch):
    table = pa.table({"x": [1, 2, 3]})
    mirror = tmp_path / "mirror"
    pqkey = mirror / "v1" / "tiny" / "parquet" / "tiny.parquet"
    pqkey.parent.mkdir(parents=True); pq.write_table(table, pqkey)
    snapshot = {"schema_version": 1, "slugs": {"tiny": {
        "expected_rows": 3, "last_built_rows": 3,
        "parquet_bytes": pqkey.stat().st_size, "vortex_bytes": None,
        "parquet_sha256": _sha(pqkey), "vortex_sha256": None,
        "columns": [{"name": "x", "type": "int64"}]}}}
    manifest = {"schema_version": 1, "datasets": [{"slug": "tiny",
        "short_name": "Tiny", "full_name": "Tiny", "description": "d",
        "license": {"spdx": "CC0-1.0"}, "fetch": {"urls": ["http://s"]}}]}
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "snapshot.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{mirror}")
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    yield
    _catalog.load_catalog.cache_clear()


def test_load_metadata_is_cheap(loaded):
    import raincloud
    ds = raincloud.load("tiny", format="parquet")
    assert ds.num_rows == 3
    assert ds.column_names == ["x"]
    assert ds.info["license"]["spdx"] == "CC0-1.0"


def test_load_to_arrow(loaded):
    import raincloud
    ds = raincloud.load("tiny", format="parquet")
    tbl = ds.to_arrow()
    assert tbl.num_rows == 3 and tbl.column_names == ["x"]
    assert ds.path().exists()


def test_default_format_falls_back_to_parquet(loaded):
    import raincloud
    ds = raincloud.load("tiny")  # default vortex, but only parquet exists
    assert ds.format == "parquet"


def test_unknown_slug_raises(loaded):
    import raincloud
    from raincloud.exceptions import UnknownSlug
    with pytest.raises(UnknownSlug):
        raincloud.load("nope")


def test_load_dataset_alias(loaded):
    import raincloud
    assert raincloud.load_dataset is raincloud.load


def test_format_unavailable_raises(tmp_path, monkeypatch):
    snapshot = {"schema_version": 1, "slugs": {"vx": {
        "expected_rows": 1, "last_built_rows": 1,
        "parquet_bytes": None, "vortex_bytes": 10,
        "parquet_sha256": None, "vortex_sha256": "aa",
        "columns": [{"name": "x", "type": "int64"}]}}}
    manifest = {"schema_version": 1, "datasets": [{"slug": "vx",
        "short_name": "VX", "license": {}, "fetch": {"urls": []}}]}
    (tmp_path / "s.json").write_text(json.dumps(snapshot))
    (tmp_path / "m.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "s.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "m.json"))
    import raincloud
    from raincloud import _catalog
    from raincloud.exceptions import FormatUnavailable
    _catalog.load_catalog.cache_clear()
    try:
        # only vortex exists; requesting parquet has no fallback
        with pytest.raises(FormatUnavailable):
            raincloud.load("vx", format="parquet")
    finally:
        _catalog.load_catalog.cache_clear()
