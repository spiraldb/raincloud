import hashlib

import pytest


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def test_cache_path_honors_env(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path))
    from raincloud import _cache
    p = _cache.cache_path("foo", "vortex")
    assert p == tmp_path / "v1" / "foo" / "vortex" / "foo.vortex"


def test_sha256_file(tmp_path):
    from raincloud import _cache
    f = tmp_path / "a.bin"; f.write_bytes(b"hello")
    assert _cache.sha256_file(f) == _sha(b"hello")


def test_adopt_verifies_and_renames(tmp_path):
    from raincloud import _cache
    src = tmp_path / "t.part"; src.write_bytes(b"data")
    dest = tmp_path / "v1" / "s" / "parquet" / "s.parquet"
    out = _cache.adopt(src, dest, _sha(b"data"))
    assert out == dest and dest.read_bytes() == b"data"
    assert not src.exists()


def test_adopt_mismatch_raises_and_cleans(tmp_path):
    from raincloud import _cache
    from raincloud.exceptions import ChecksumMismatch
    src = tmp_path / "t.part"; src.write_bytes(b"data")
    dest = tmp_path / "out.parquet"
    with pytest.raises(ChecksumMismatch):
        _cache.adopt(src, dest, "deadbeef")
    assert not src.exists() and not dest.exists()


def test_is_offline(monkeypatch):
    from raincloud import _cache
    monkeypatch.setenv("RAINCLOUD_OFFLINE", "1")
    assert _cache.is_offline() is True
    monkeypatch.setenv("RAINCLOUD_OFFLINE", "0")
    assert _cache.is_offline() is False
    monkeypatch.setenv("RAINCLOUD_OFFLINE", "TRUE")
    assert _cache.is_offline() is True
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)
    assert _cache.is_offline() is False


def test_adopt_none_checksum_skips_verification(tmp_path):
    from raincloud import _cache
    src = tmp_path / "t.part"; src.write_bytes(b"locally-built")
    dest = tmp_path / "v1" / "s" / "vortex" / "s.vortex"
    # None checksum => trusted local build, no verification, must still adopt.
    out = _cache.adopt(src, dest, None)
    assert out == dest and dest.read_bytes() == b"locally-built"
    assert not src.exists()
