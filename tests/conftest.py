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

import os
import tempfile

import pytest


def pytest_configure(config):
    # Module- and session-scoped fixtures run before the per-test isolation
    # below, so without this they read the machine's /etc/xdg config and
    # whatever catalog it names. Tests that want config files opt back in.
    os.environ["RAINCLOUD_NO_CONFIG"] = "1"
    os.environ["RAINCLOUD_CATALOG_DIR"] = tempfile.mkdtemp(prefix="raincloud-test-catalogs-")
    # The tracked catalog changes only when a maintainer commits it; no test
    # may leave docs/v{n}/snapshot.json changed (checked at session end).
    config._tracked_snapshots = _tracked_snapshots()


def _tracked_snapshots():
    import hashlib
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent
    return {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root / "docs").glob("v*/snapshot.json"))}


def pytest_sessionfinish(session, exitstatus):
    before = getattr(session.config, "_tracked_snapshots", {})
    changed = [str(p) for p, digest in _tracked_snapshots().items() if before.get(p) != digest]
    if changed:
        session.exitstatus = 1
        print(f"\nERROR: the test run modified tracked snapshot(s): {changed}; restore with git checkout")


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
    pins the build settings most tests assume (below),
    and clears the catalog lru_cache around each test so a snapshot/manifest set
    by one test never leaks into the next. Tests that need a specific cache
    location just set RAINCLOUD_CACHE again — a later monkeypatch.setenv wins.
    """
    monkeypatch.setenv("RAINCLOUD_CATALOG_DIR", str(tmp_path / "_catalogs"))
    monkeypatch.setenv("RAINCLOUD_NO_CONFIG", "1")
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "_loader_cache"))
    # Most tests exercise the pipeline over a dataset's Parquet and Vortex with
    # everything kept; the opt-in defaults (Vortex only, nothing kept) have
    # tests of their own, which clear these.
    monkeypatch.setenv("RAINCLOUD_FORMATS", "parquet,vortex")
    monkeypatch.setenv("RAINCLOUD_KEEP_RAW", "1")
    monkeypatch.setenv("RAINCLOUD_KEEP_CANONICAL", "1")

    def _clear():
        try:
            from raincloud._catalog import load_catalog
            load_catalog.cache_clear()
        except Exception:
            pass

    _clear()
    yield
    _clear()
