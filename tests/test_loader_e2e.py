# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import hashlib
import json

import pyarrow as pa
import vortex


def _sha(p) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def test_e2e_vortex_through_file_mirror(tmp_path, monkeypatch):
    table = pa.table({"x": [10, 20, 30], "y": ["a", "b", "c"]})
    mirror = tmp_path / "mirror"
    vkey = mirror / "v1" / "tiny" / "vortex" / "tiny.vortex"
    vkey.parent.mkdir(parents=True)
    vortex.io.write(table, str(vkey))  # the public write API we depend on
    snapshot = {
        "schema_version": 1,
        "slugs": {
            "tiny": {
                "expected_rows": 3,
                "last_built_rows": 3,
                "parquet_bytes": None,
                "vortex_bytes": vkey.stat().st_size,
                "parquet_sha256": None,
                "vortex_sha256": _sha(vkey),
                "columns": [
                    {"name": "x", "type": "int64"},
                    {"name": "y", "type": "string"},
                ],
            }
        },
    }
    manifest = {
        "schema_version": 1,
        "datasets": [
            {
                "slug": "tiny",
                "short_name": "Tiny",
                "license": {"spdx": "CC0-1.0"},
                "fetch": {"urls": []},
            }
        ],
    }
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "snapshot.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{mirror}")

    import raincloud
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    try:
        ds = raincloud.load("tiny")              # default vortex, available
        assert ds.format == "vortex"
        tbl = ds.to_arrow()
        assert tbl.num_rows == 3
        assert tbl.column_names == ["x", "y"]
        assert tbl["x"].to_pylist() == [10, 20, 30]   # roundtrip proof, not just shape
        assert tbl["y"].to_pylist() == ["a", "b", "c"]
        assert ds.path().exists()                 # cached after first resolve
    finally:
        _catalog.load_catalog.cache_clear()
