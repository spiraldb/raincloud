# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Real network build tests (opt-in via --run-network, non-blocking in CI).

Each parametrized case calls raincloud.load('<slug>', build=True), which exercises the
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
def test_real_build_via_load(tmp_path, slug, expected_rows):
    import raincloud

    cfg = raincloud.resolve_config(no_config=True, catalog="checkout",
        data_dir=tmp_path / "data", cache_dir=tmp_path / "cache",
        raw_dir=tmp_path / "raw", scratch_dir=tmp_path / "scratch",
        catalog_dir=tmp_path / "catalogs", mirror="", offline=False)
    ds = raincloud.load(slug, config=cfg, build=True)
    tbl = ds.to_arrow()
    assert tbl.num_rows == expected_rows
    # Assert the actual public read resolves the artifact produced by the child
    # builder inside this store; ambient machine configuration cannot redirect it.
    assert ds.path().is_relative_to(cfg.data_dir)
    # The build wrote the format the load asked for: auto, on an install that builds
    # only Vortex (the default), with every extra installed.
    assert ds.format == "vortex" and ds.path().is_file()
    assert not cfg.cache_dir.exists()
