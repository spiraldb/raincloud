import hashlib
import json

import pytest


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A file:// mirror with one artifact + a catalog describing it."""
    payload = b"PARQUETBYTES"
    mirror = tmp_path / "mirror"
    key = mirror / "v1" / "tiny" / "parquet" / "tiny.parquet"
    key.parent.mkdir(parents=True)
    key.write_bytes(payload)
    snapshot = {
        "schema_version": 1,
        "slugs": {
            "tiny": {
                "expected_rows": 3,
                "last_built_rows": 3,
                "parquet_bytes": len(payload),
                "vortex_bytes": None,
                "parquet_sha256": _sha(payload),
                "vortex_sha256": None,
                "columns": [{"name": "x", "type": "int64"}],
            }
        },
    }
    manifest = {
        "schema_version": 1,
        "datasets": [
            {"slug": "tiny", "short_name": "T", "license": {}, "fetch": {"urls": []}}
        ],
    }
    sp = tmp_path / "snapshot.json"
    sp.write_text(json.dumps(snapshot))
    mp = tmp_path / "sources.json"
    mp.write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(sp))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(mp))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{mirror}")
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)
    from raincloud import _catalog

    _catalog.load_catalog.cache_clear()
    yield {"payload": payload, "mirror": mirror, "tmp": tmp_path}
    _catalog.load_catalog.cache_clear()


def test_artifact_key():
    from raincloud import _resolve

    assert _resolve.artifact_key("tiny", "parquet") == "v1/tiny/parquet/tiny.parquet"


def test_resolve_from_mirror_then_cache(env):
    from raincloud import _cache, _resolve

    p = _resolve.resolve("tiny", "parquet")
    assert p == _cache.cache_path("tiny", "parquet")
    assert p.read_bytes() == env["payload"]
    # second call is a pure cache hit (corrupt the mirror to prove no refetch)
    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"X")
    assert _resolve.resolve("tiny", "parquet").read_bytes() == env["payload"]


def test_resolve_checksum_mismatch(env):
    from raincloud import _resolve
    from raincloud.exceptions import ChecksumMismatch

    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"corrupt")
    with pytest.raises(ChecksumMismatch):
        _resolve.resolve("tiny", "parquet")


def test_resolve_offline_miss(env, monkeypatch):
    from raincloud import _resolve
    from raincloud.exceptions import OfflineMiss

    monkeypatch.setenv("RAINCLOUD_OFFLINE", "1")
    with pytest.raises(OfflineMiss):
        _resolve.resolve("tiny", "parquet")


def test_resolve_mirror_miss_no_build(env, monkeypatch):
    from raincloud import _resolve
    from raincloud.exceptions import BuildToolingMissing

    # point mirror somewhere empty; disable build
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{env['tmp']}/empty")
    monkeypatch.setattr(_resolve, "_build_available", lambda: False)
    with pytest.raises(BuildToolingMissing):
        _resolve.resolve("tiny", "parquet", allow_build=True)


def test_resolve_propagates_non_notfound_transport_error(env, monkeypatch):
    from raincloud import _resolve, _transport

    def boom(url, dest):
        raise PermissionError("403 denied")

    monkeypatch.setattr(_transport, "fetch", boom)
    # A transport error that is NOT a clean miss must propagate, not silently
    # fall through to a (potentially multi-hour) local build.
    with pytest.raises(PermissionError):
        _resolve.resolve("tiny", "parquet")
