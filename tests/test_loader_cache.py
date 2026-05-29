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
    """With RAINCLOUD_CACHE unset, cache_root honors XDG_CACHE_HOME, else
    ~/.cache/raincloud. (The autouse isolation fixture sets RAINCLOUD_CACHE, so
    delete it here to actually exercise the default branch.)"""
    from pathlib import Path

    from raincloud import _cache
    monkeypatch.delenv("RAINCLOUD_CACHE", raising=False)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    assert _cache.cache_root() == tmp_path / "xdg" / "raincloud"
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    assert _cache.cache_root() == Path.home() / ".cache" / "raincloud"


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


def test_adopt_strict_mismatch_raises_and_cleans(tmp_path):
    from raincloud import _cache
    from raincloud.exceptions import ChecksumMismatch
    src = tmp_path / "t.part"; src.write_bytes(b"data")
    dest = tmp_path / "out.parquet"
    with pytest.raises(ChecksumMismatch):
        _cache.adopt(src, dest, "deadbeef", strict=True)
    assert not src.exists() and not dest.exists()


def test_adopt_default_mismatch_warns_but_adopts(tmp_path, capsys):
    """Upstream content drifts; the contract is alert-the-user, don't block."""
    from raincloud import _cache
    src = tmp_path / "t.part"; src.write_bytes(b"new-upstream-bytes")
    dest = tmp_path / "v1" / "demo" / "parquet" / "demo.parquet"
    out = _cache.adopt(src, dest, "deadbeef" * 8, slug="demo", origin="mirror")
    # The drifted bytes still landed at dest — the build did not panic.
    assert out == dest
    assert dest.read_bytes() == b"new-upstream-bytes"
    assert not src.exists()
    err = capsys.readouterr().err
    assert "[raincloud] WARN" in err
    assert "demo" in err and "mirror" in err and "drifted" in err


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


def test_adopt_writes_pin_even_without_sha(tmp_path):
    """A pin is written on every adopt — including sha-less artifacts — so a
    later resolve() can serve them from cache via the size pin."""
    from raincloud import _cache
    src = tmp_path / "t.part"; src.write_bytes(b"sizeable")
    dest = tmp_path / "v1" / "s" / "parquet" / "s.parquet"
    _cache.adopt(src, dest, None)
    pin = _cache.read_pin(dest)
    assert pin == {"snap_sha": None, "size": len(b"sizeable"), "origin": None}


def test_read_pin_rejects_non_dict_json(tmp_path):
    """A torn/partial or tampered .pin holding valid-but-non-object JSON must
    return None, not a list/int that later .get() calls would choke on."""
    from raincloud import _cache
    dest = tmp_path / "v1" / "s" / "parquet" / "s.parquet"
    dest.parent.mkdir(parents=True)
    pin = _cache.pin_path(dest)
    for junk in ("[1, 2, 3]", "42", '"a string"', "not json at all"):
        pin.write_text(junk)
        assert _cache.read_pin(dest) is None
