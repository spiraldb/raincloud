# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Pytest configuration: opt-in flags for slow/network tests.

The default `pytest` invocation skips:
  - @pytest.mark.wheel    — builds the wheel + spins throwaway venvs (slow)
  - @pytest.mark.network  — fetches real upstream data (flaky)

Enable each via `--run-wheel` / `--run-network` on the command line.
The flags are independent: passing one does not enable the other.
"""
from __future__ import annotations

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-wheel", action="store_true", default=False,
        help="Run @pytest.mark.wheel tests (builds the wheel + spins venvs).",
    )
    parser.addoption(
        "--run-network", action="store_true", default=False,
        help="Run @pytest.mark.network tests (hits real upstream sources).",
    )


def pytest_collection_modifyitems(config, items):
    skip_wheel = pytest.mark.skip(reason="needs --run-wheel")
    skip_network = pytest.mark.skip(reason="needs --run-network")
    run_wheel = config.getoption("--run-wheel")
    run_network = config.getoption("--run-network")
    for item in items:
        if "wheel" in item.keywords and not run_wheel:
            item.add_marker(skip_wheel)
        if "network" in item.keywords and not run_network:
            item.add_marker(skip_network)


@pytest.fixture(autouse=True)
def _isolate_loader_cache(tmp_path, monkeypatch):
    """Belt-and-suspenders hermeticity for every test.

    Points the loader cache at a per-test tmp dir so no test can read or write
    the developer's real ~/.cache/raincloud (the loader's cache_root() default),
    and clears the catalog lru_cache around each test so a snapshot/manifest set
    by one test never leaks into the next. Tests that need a specific cache
    location just set RAINCLOUD_CACHE again — a later monkeypatch.setenv wins.
    """
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "_loader_cache"))

    def _clear():
        try:
            from raincloud._catalog import load_catalog
            load_catalog.cache_clear()
        except Exception:
            pass

    _clear()
    yield
    _clear()
