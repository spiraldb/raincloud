# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Real network build tests (opt-in via --run-network, non-blocking in CI).

Each parametrized case calls raincloud.load('<slug>'), which exercises the
loader's real build-fallback against a live upstream: load → cache miss →
mirror absent → subprocess build (fetch→…→convert) → adopt → materialize.
Three tiny slugs across two hosts and three handlers for coverage.
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.network


@pytest.mark.parametrize("slug,expected_rows", [
    ("uci-iris", 150),
    ("uci-seeds", 210),
    ("countries-of-the-world", 262),
])
def test_real_build_via_load(tmp_path, monkeypatch, slug, expected_rows):
    monkeypatch.setenv("RAINCLOUD_HOME",  str(tmp_path / "home"))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.delenv("RAINCLOUD_MIRROR",  raising=False)
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)

    import raincloud
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    try:
        ds = raincloud.load(slug)
        tbl = ds.to_arrow()
        assert tbl.num_rows == expected_rows, (
            f"{slug}: expected {expected_rows} rows, got {tbl.num_rows}"
        )
    finally:
        _catalog.load_catalog.cache_clear()
