# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import json

import pytest


@pytest.fixture
def fake_catalog(tmp_path, monkeypatch):
    snapshot = {
        "schema_version": 1,
        "slugs": {
            "tiny": {
                "expected_rows": 3, "last_built_rows": 3,
                "parquet_bytes": 100, "vortex_bytes": 120,
                "parquet_sha256": "aa", "vortex_sha256": "bb",
                "columns": [{"name": "x", "type": "int64"}],
            },
            "pq_only": {
                "expected_rows": 5, "last_built_rows": 5,
                "parquet_bytes": 50, "vortex_bytes": None,
                "parquet_sha256": "cc", "vortex_sha256": None,
                "columns": [{"name": "y", "type": "string"}],
            },
        },
    }
    manifest = {"schema_version": 1, "datasets": [
        {"slug": "tiny", "short_name": "Tiny", "full_name": "Tiny set",
         "description": "a tiny set",
         "license": {"spdx": "CC0-1.0", "source_url": "http://x",
                     "redistribution_permitted": True, "attribution_required": False},
         "fetch": {"urls": ["http://src/tiny.csv"]}},
        {"slug": "pq_only", "short_name": "PQ", "full_name": "PQ only",
         "description": "p", "license": {"spdx": "MIT", "source_url": "http://y"},
         "fetch": {"urls": ["http://src/pq.csv"]}},
    ]}
    sp = tmp_path / "snapshot.json"; sp.write_text(json.dumps(snapshot))
    mp = tmp_path / "sources.json"; mp.write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(sp))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(mp))
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    yield _catalog.load_catalog()
    _catalog.load_catalog.cache_clear()


def test_entry_formats_and_checksums(fake_catalog):
    e = fake_catalog.entry("tiny")
    assert e.rows == 3
    assert set(e.formats) == {"parquet", "vortex"}
    assert e.formats["vortex"].sha256 == "bb"
    assert e.formats["parquet"].nbytes == 100


def test_format_availability_excludes_missing(fake_catalog):
    e = fake_catalog.entry("pq_only")
    assert set(e.formats) == {"parquet"}  # vortex_bytes was None


def test_info_fields(fake_catalog):
    e = fake_catalog.entry("tiny")
    assert e.info["short_name"] == "Tiny"
    assert e.info["license"]["spdx"] == "CC0-1.0"
    assert e.info["source_url"] == "http://src/tiny.csv"
    assert e.column_names == ["x"]


def test_unknown_slug(fake_catalog):
    from raincloud.exceptions import UnknownSlug
    assert "nope" not in fake_catalog
    with pytest.raises(UnknownSlug):
        fake_catalog.entry("nope")
