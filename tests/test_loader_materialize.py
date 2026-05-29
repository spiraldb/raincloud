# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Default-lane coverage for the Dataset materialization API.

`.scan()`, `.to_pandas()`, `.to_vortex()`, and `.schema` were previously
exercised ONLY in the `--run-wheel`-gated subprocess tests, so a regression in
any of them sailed through the blocking `pytest` lane. duckdb + pandas are
installed via `--extra build` in that lane, so these run hermetically against a
file:// mirror with no network and no wheel build.
"""
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import vortex


def sha256_path(p) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


@pytest.fixture
def both_formats(tmp_path, monkeypatch):
    """A file:// mirror carrying BOTH parquet and vortex for slug 'tiny'."""
    table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    mirror = tmp_path / "mirror"
    pqkey = mirror / "v1" / "tiny" / "parquet" / "tiny.parquet"
    vxkey = mirror / "v1" / "tiny" / "vortex" / "tiny.vortex"
    pqkey.parent.mkdir(parents=True)
    vxkey.parent.mkdir(parents=True)
    pq.write_table(table, pqkey)
    vortex.io.write(table, str(vxkey))
    snapshot = {"schema_version": 1, "slugs": {"tiny": {
        "expected_rows": 3, "last_built_rows": 3,
        "parquet_bytes": pqkey.stat().st_size, "vortex_bytes": vxkey.stat().st_size,
        "parquet_sha256": sha256_path(pqkey), "vortex_sha256": sha256_path(vxkey),
        "columns": [{"name": "x", "type": "int64"}, {"name": "y", "type": "string"}]}}}
    manifest = {"schema_version": 1, "datasets": [{
        "slug": "tiny", "short_name": "Tiny", "license": {"spdx": "CC0-1.0"},
        "fetch": {"urls": []}, "convert": {"vortex": True}}]}
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


def test_scan_returns_queryable_relation(both_formats):
    import raincloud
    ds = raincloud.load("tiny", format="parquet")
    rel = ds.scan()
    assert rel.aggregate("count(*)").fetchone()[0] == 3
    assert rel.filter("x >= 2").aggregate("count(*)").fetchone()[0] == 2


def test_scan_from_vortex_resolves_parquet_sibling(both_formats, capsys):
    """A vortex-loaded handle has no native DuckDB reader, so scan() resolves
    the parquet sibling and warns first so the implicit fetch isn't a surprise."""
    import raincloud
    ds = raincloud.load("tiny")  # default vortex (available here)
    assert ds.format == "vortex"
    rel = ds.scan()
    assert rel.aggregate("count(*)").fetchone()[0] == 3
    err = capsys.readouterr().err
    assert "scan() needs parquet" in err and "tiny" in err


def test_to_pandas_returns_dataframe(both_formats):
    import raincloud
    df = raincloud.load("tiny", format="parquet").to_pandas()
    assert list(df.columns) == ["x", "y"]
    assert len(df) == 3
    assert df["x"].tolist() == [1, 2, 3]


def test_to_vortex_returns_openable(both_formats):
    import raincloud
    vf = raincloud.load("tiny").to_vortex()
    assert vf.to_arrow().read_all().num_rows == 3


def test_schema_parquet_and_vortex_agree(both_formats):
    import raincloud
    pq_schema = raincloud.load("tiny", format="parquet").schema
    vx_schema = raincloud.load("tiny").schema
    assert pq_schema.names == ["x", "y"]
    assert vx_schema.names == ["x", "y"]
