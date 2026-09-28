# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import hashlib

import pytest


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def test_cache_path_honors_env(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path))
    from raincloud import _cache
    p = _cache.cache_path("foo", "vortex")
    assert p == tmp_path / "v1" / "foo" / "vortex" / "foo.vortex"


def test_cache_root_default_branch(tmp_path, monkeypatch):
    from raincloud import _cache
    from raincloud.config import get_config
    monkeypatch.delenv("RAINCLOUD_CACHE", raising=False)
    assert _cache.cache_root() == get_config().data_dir
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(tmp_path / "hdd"))
    assert _cache.cache_root() == tmp_path / "hdd"


def test_cache_root_expands_tilde(monkeypatch):
    """RAINCLOUD_CACHE=~/... expands, matching _data_file / spec env handling."""
    from pathlib import Path

    from raincloud import _cache
    monkeypatch.setenv("RAINCLOUD_CACHE", "~/rc-cache-tilde-test")
    assert _cache.cache_root() == Path.home() / "rc-cache-tilde-test"


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


def test_adopt_none_checksum_skips_verification(tmp_path):
    from raincloud import _cache
    src = tmp_path / "t.part"; src.write_bytes(b"locally-built")
    dest = tmp_path / "v1" / "s" / "vortex" / "s.vortex"
    # None checksum => trusted local build, no verification, must still adopt.
    out = _cache.adopt(src, dest, None)
    assert out == dest and dest.read_bytes() == b"locally-built"
    assert not src.exists()


def test_adopt_without_sha_checks_the_catalog_size(tmp_path):
    from raincloud import _cache
    from raincloud.exceptions import ChecksumMismatch
    dest = tmp_path / "v1" / "s" / "parquet" / "s.parquet"
    src = tmp_path / "t.part"; src.write_bytes(b"sizeable")
    assert _cache.adopt(src, dest, None, expected_size=len(b"sizeable")) == dest
    assert list(dest.parent.iterdir()) == [dest]  # no sidecar
    src.write_bytes(b"short")
    with pytest.raises(ChecksumMismatch):
        _cache.adopt(src, dest, None, expected_size=len(b"sizeable"))
    assert dest.read_bytes() == b"sizeable" and not src.exists()
