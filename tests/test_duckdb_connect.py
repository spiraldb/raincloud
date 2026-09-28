# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""`raincloud.duckdb_connect` is the one way to open DuckDB: imported lazily, with a
clear error when DuckDB is missing, and applying the environment's settings."""
import subprocess
import sys

import pytest

import raincloud


def test_importing_raincloud_does_not_import_duckdb():
    code = "import sys, raincloud; assert 'duckdb' not in sys.modules; print(raincloud.duckdb_connect.__module__)"
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert proc.stdout.strip() == "raincloud._duckdb"


def test_a_missing_duckdb_says_how_to_install_it(monkeypatch):
    monkeypatch.setitem(sys.modules, "duckdb", None)
    with pytest.raises(raincloud.MissingDependency, match=r"needs duckdb; install `duckdb`"):
        raincloud.duckdb_connect()


def test_duckdb_connect_applies_the_environment_and_persistent_settings(tmp_path, monkeypatch):
    pytest.importorskip("duckdb")
    monkeypatch.setenv("RAINCLOUD_DUCKDB_THREADS", "2")
    with raincloud.duckdb_connect() as con:
        assert con.execute("select current_setting('threads')").fetchone()[0] == 2
    with raincloud.duckdb_connect(tmp_path / "x.duckdb") as con:
        con.execute("create table t as select 1::VARIANT v")
        assert con.execute("select count(*) from t").fetchone()[0] == 1


def test_duckdb_connect_has_one_home():
    from raincloud.pipeline import spec
    assert not hasattr(spec, "duckdb_connect")
