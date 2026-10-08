# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Loader coverage for the canonical `arrow` format + schema-version threading.

Additive to the v1 loader tests: an `arrow` artifact (a zstd Arrow IPC file, the
same shape `raincloud.pipeline.canonical.write_canonical` produces) resolves via
the version-threaded cache/mirror path and materializes through `to_arrow()` /
`.schema`; `artifact_key`/`cache_path` compose `v{version}/...`; and the default
`load()` fallback cascades vortex -> parquet -> arrow. Hermetic — a file://
mirror, no network, no wheel build (per the other test_loader_* fixtures).
"""
import hashlib
import json

import pyarrow as pa
import pytest

from raincloud._formats import ALL_FORMATS


@pytest.mark.parametrize("extra,formats", [
    ({"export": {"formats": []}}, {"arrow"}),
    ({"export": {"formats": ["parquet"]}}, {"arrow", "parquet"}),
    ({"export": {"formats": ["vortex"]}}, {"arrow", "vortex"}),
    ({"export": {"formats": ["parquet"], "priority": ["rs"]}}, {"arrow", "parquet"}),
    ({"export": {"formats": ["parquet"], "notes": "x"}}, {"arrow", "parquet"}),
    ({}, set(ALL_FORMATS)),
])
def test_unbuilt_v2_catalog_uses_export_policy(extra, formats):
    from raincloud._catalog import Catalog

    manifest = {"schema_version": 2, "datasets": [{"slug": "new", **extra}]}
    entry = Catalog({"schema_version": 2, "slugs": {}}, manifest).entry("new")
    assert set(entry.formats) == formats


def _sha(p) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _write_arrow_ipc(table: pa.Table, path) -> None:
    """Write `table` as a zstd Arrow IPC file, matching canonical.write_canonical."""
    opts = pa.ipc.IpcWriteOptions(compression="zstd")
    with pa.OSFile(str(path), "wb") as sink:
        with pa.ipc.new_file(sink, table.schema, options=opts) as writer:
            writer.write_table(table)


# --- path helpers: version threading, default stays v1 -----------------------


def test_ext_has_arrow():
    from raincloud import _cache
    assert _cache.EXT["arrow"] == "arrow.zstd"


def test_artifact_key_requires_a_version():
    from raincloud import _resolve
    # No default: a forgotten version would quietly address the frozen v1 layout.
    with pytest.raises(TypeError):
        _resolve.artifact_key("tiny", "parquet")
    assert _resolve.artifact_key("tiny", "parquet", 1) == "v1/tiny/parquet/tiny.parquet"
    assert _resolve.artifact_key("tiny", "arrow", 1) == "v1/tiny/arrow/tiny.arrow.zstd"


def test_artifact_key_composes_v2():
    from raincloud import _resolve
    assert _resolve.artifact_key("tiny", "arrow", 2) == "v2/tiny/arrow/tiny.arrow.zstd"
    assert _resolve.artifact_key("tiny", "parquet", version=2) == "v2/tiny/parquet/tiny.parquet"


def test_cache_path_default_version_is_v1(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path))
    from raincloud import _cache
    assert _cache.cache_path("foo", "arrow") == tmp_path / "v1" / "foo" / "arrow" / "foo.arrow.zstd"


def test_cache_path_composes_v2(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path))
    from raincloud import _cache
    assert _cache.cache_path("foo", "arrow", 2) == tmp_path / "v2" / "foo" / "arrow" / "foo.arrow.zstd"


# --- catalog: arrow recognition + version on the Entry ------------------------


def _write_catalog(tmp_path, monkeypatch, snapshot, manifest):
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "snapshot.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    return _catalog


def test_catalog_recognizes_arrow_and_carries_version(tmp_path, monkeypatch):
    snapshot = {"schema_version": 2, "slugs": {"tiny": {
        "expected_rows": 3, "last_built_rows": 3,
        "parquet_bytes": 100, "vortex_bytes": 120, "arrow_bytes": 90,
        "parquet_sha256": "aa" * 32, "vortex_sha256": "bb" * 32, "arrow_sha256": "cc" * 32,
        "columns": [{"name": "x", "type": "int64"}]}}}
    manifest = {"schema_version": 2, "datasets": [{
        "slug": "tiny", "short_name": "T", "license": {},
        "fetch": {"urls": []}}]}
    _catalog = _write_catalog(tmp_path, monkeypatch, snapshot, manifest)
    try:
        e = _catalog.load_catalog().entry("tiny")
        assert set(e.formats) == set(ALL_FORMATS)
        assert e.formats["arrow"].sha256 == "cc" * 32
        assert e.formats["arrow"].nbytes == 90
        assert e.version == 2
    finally:
        _catalog.load_catalog.cache_clear()


def test_catalog_no_arrow_when_absent_and_defaults_v1(tmp_path, monkeypatch):
    """A v1 snapshot with no arrow_bytes exposes no arrow format and version 1."""
    snapshot = {"schema_version": 1, "slugs": {"tiny": {
        "expected_rows": 3, "parquet_bytes": 100, "parquet_sha256": "aa" * 32,
        "columns": []}}}
    manifest = {"schema_version": 1, "datasets": [{
        "slug": "tiny", "short_name": "T", "license": {}, "fetch": {"urls": []}}]}
    _catalog = _write_catalog(tmp_path, monkeypatch, snapshot, manifest)
    try:
        e = _catalog.load_catalog().entry("tiny")
        assert "arrow" not in e.formats
        assert e.version == 1
    finally:
        _catalog.load_catalog.cache_clear()


# --- end-to-end: v2 arrow-only slug resolves + round-trips --------------------


@pytest.fixture
def arrow_only(tmp_path, monkeypatch):
    """A v2 file:// mirror carrying ONLY an arrow artifact for a snapshot-only
    slug (not in the manifest, so parquet is not auto-offered). Proves the
    fallback chain reaches arrow and that resolution uses the v2 path prefix."""
    table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    mirror = tmp_path / "mirror"
    key = mirror / "v2" / "arrow_only" / "arrow" / "arrow_only.arrow.zstd"
    key.parent.mkdir(parents=True)
    _write_arrow_ipc(table, key)
    snapshot = {"schema_version": 2, "slugs": {"arrow_only": {
        "expected_rows": 3, "last_built_rows": 3,
        "arrow_bytes": key.stat().st_size, "arrow_sha256": _sha(key),
        "columns": [{"name": "x", "type": "int64"}, {"name": "y", "type": "string"}]}}}
    manifest = {"schema_version": 2, "datasets": [{"slug": "arrow_only", "export": {"formats": []}}]}
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "snapshot.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{mirror}")
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    yield {"mirror": mirror}
    _catalog.load_catalog.cache_clear()


def test_default_format_falls_back_to_arrow(arrow_only):
    import raincloud
    ds = raincloud.load("arrow_only")  # default vortex; only arrow exists
    assert ds.format == "arrow"


def test_resolve_uses_v2_path(arrow_only):
    from raincloud import _cache, _resolve
    p = _resolve.resolve("arrow_only", "arrow")
    assert p == _cache.cache_path("arrow_only", "arrow", 2)
    assert "v2" in p.parts and p.name == "arrow_only.arrow.zstd"


def test_arrow_to_arrow_roundtrips(arrow_only):
    import raincloud
    tbl = raincloud.load("arrow_only").to_arrow()
    assert tbl.num_rows == 3
    assert tbl.column_names == ["x", "y"]
    assert tbl.column("x").to_pylist() == [1, 2, 3]


def test_arrow_schema_is_readable(arrow_only):
    import raincloud
    schema = raincloud.load("arrow_only").schema
    assert schema.names == ["x", "y"]


def test_arrow_explicit_format_load(arrow_only):
    import raincloud
    ds = raincloud.load("arrow_only", format="arrow")
    assert ds.format == "arrow"
    assert ds.to_arrow().num_rows == 3


def test_explicit_parquet_absent_does_not_cascade_to_arrow(arrow_only):
    """An explicit non-vortex request that's absent RAISES — it must not silently
    cascade to arrow (the refactor's central invariant)."""
    import raincloud
    from raincloud.exceptions import FormatUnavailable
    with pytest.raises(FormatUnavailable):
        raincloud.load("arrow_only", format="parquet")


@pytest.fixture
def parquet_and_arrow(tmp_path, monkeypatch):
    """A v2 file:// mirror with parquet + arrow but NO vortex, for a snapshot-only
    slug — proves the vortex-default cascade prefers parquet over arrow."""
    import pyarrow.parquet as pq
    table = pa.table({"x": [1, 2, 3]})
    mirror = tmp_path / "mirror"
    pkey = mirror / "v2" / "pq_arrow" / "parquet" / "pq_arrow.parquet"
    akey = mirror / "v2" / "pq_arrow" / "arrow" / "pq_arrow.arrow.zstd"
    pkey.parent.mkdir(parents=True)
    akey.parent.mkdir(parents=True)
    pq.write_table(table, str(pkey))
    _write_arrow_ipc(table, akey)
    snapshot = {"schema_version": 2, "slugs": {"pq_arrow": {
        "expected_rows": 3, "last_built_rows": 3,
        "parquet_bytes": pkey.stat().st_size, "parquet_sha256": _sha(pkey),
        "arrow_bytes": akey.stat().st_size, "arrow_sha256": _sha(akey),
        "columns": [{"name": "x", "type": "int64"}]}}}
    manifest = {"schema_version": 2, "datasets": [{"slug": "pq_arrow", "export": {"formats": ["parquet"]}}]}
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "snapshot.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{mirror}")
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    yield
    _catalog.load_catalog.cache_clear()


def test_cascade_prefers_parquet_over_arrow(parquet_and_arrow):
    """vortex absent; the default cascade must pick parquet before arrow."""
    import raincloud
    assert raincloud.load("pq_arrow").format == "parquet"
