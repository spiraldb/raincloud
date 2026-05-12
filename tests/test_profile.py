# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Tests for the per-column profile stage.

Tasks 7-9 land the schema (this file), the numeric/bool/temporal stats, then
string/list/struct. The golden fixture for uci-seeds gets committed in Task 9
once the full stat menu is implemented; Task 7 only validates schema shape.
"""
from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import jsonschema
import pytest

from scripts.pipeline.spec import REPO_ROOT


@pytest.fixture(scope="module")
def profile_schema() -> dict:
    return json.loads((REPO_ROOT / "profile.schema.json").read_text())


def test_profile_schema_loads(profile_schema):
    """Draft 2020-12 schema parses and declares the expected required fields."""
    assert profile_schema["$schema"].endswith("/2020-12/schema")
    assert "schema_version" in profile_schema["required"]
    assert "columns" in profile_schema["required"]


def test_minimal_profile_validates(profile_schema):
    sample = {
        "schema_version": 1,
        "slug": "fake",
        "row_count": 100,
        "parquet_sha256": "0" * 64,
        "computed_at": "2026-05-12T10:30:00Z",
        "sample_rows": None,
        "columns": {
            "col": {"dtype": "int32", "null_count": 0, "min": 1, "max": 100,
                    "mean": 50.5, "ndv_approx": 100,
                    "histogram": {"buckets": [1, 11, 21], "counts": [10, 10, 80]}},
        },
    }
    jsonschema.Draft202012Validator(profile_schema).validate(sample)


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


@pytest.fixture(scope="module")
def uci_seeds_parquet() -> Path:
    p = REPO_ROOT / "outputs" / "v1" / "uci-seeds" / "parquet" / "uci-seeds.parquet"
    if not p.exists():
        pytest.skip("uci-seeds parquet not built locally")
    return p


def test_profile_skeleton_writes_top_level_fields(tmp_path, uci_seeds_parquet, profile_schema):
    """profile_slug() returns a dict with the required envelope keys."""
    from scripts.pipeline.profile import profile_slug

    result = profile_slug(
        slug="uci-seeds",
        parquet_path=uci_seeds_parquet,
        sample_rows=None,
    )
    jsonschema.Draft202012Validator(profile_schema).validate(result)
    assert result["slug"] == "uci-seeds"
    assert result["row_count"] == 210
    assert result["parquet_sha256"] == _sha256(uci_seeds_parquet)
    assert result["schema_version"] == 1
    datetime.fromisoformat(result["computed_at"].replace("Z", "+00:00"))


def test_profile_numeric_columns_have_histogram(uci_seeds_parquet):
    from scripts.pipeline.profile import profile_slug

    result = profile_slug(slug="uci-seeds", parquet_path=uci_seeds_parquet)
    numeric_cols = [name for name, col in result["columns"].items()
                    if col and col.get("dtype", "").startswith(("int", "float", "decimal"))]
    assert len(numeric_cols) >= 7
    sample = result["columns"][numeric_cols[0]]
    assert isinstance(sample["min"], (int, float))
    assert isinstance(sample["max"], (int, float))
    assert sample["max"] >= sample["min"]
    assert len(sample["histogram"]["buckets"]) >= 2
    assert sum(sample["histogram"]["counts"]) <= result["row_count"]


def test_profile_idempotent_for_same_sha(uci_seeds_parquet):
    """Two invocations against the same parquet produce identical columns/stats."""
    from scripts.pipeline.profile import profile_slug

    a = profile_slug(slug="uci-seeds", parquet_path=uci_seeds_parquet)
    b = profile_slug(slug="uci-seeds", parquet_path=uci_seeds_parquet)
    a.pop("computed_at"); b.pop("computed_at")
    assert a == b


def test_profile_all_null_column_returns_null(tmp_path):
    """Columns whose values are entirely null serialize as null at column-map
    level (schema-conformant; matches struct/variant convention)."""
    import pyarrow as pa
    import pyarrow.parquet as papq
    from scripts.pipeline.profile import profile_slug

    table = pa.table({
        "name": pa.array([None, None, None], type=pa.string()),
        "n":    pa.array([None, None, None], type=pa.int64()),
        "ts":   pa.array([None, None, None], type=pa.timestamp("us")),
    })
    parquet = tmp_path / "allnull.parquet"
    papq.write_table(table, parquet)

    result = profile_slug(slug="tmp", parquet_path=parquet)
    # All three columns reported as null (no profile body).
    assert result["columns"]["n"] is None
    assert result["columns"]["ts"] is None
    # The string column also returns None today because Task 9 hasn't landed
    # yet — placeholder; this assertion stays true once Task 9 wires strings
    # since an all-null string also produces "all-null at column-map level."
    assert result["columns"]["name"] is None


def test_profile_string_column_records_ndv(tmp_path):
    """A tiny synthetic parquet exercises the string path."""
    import pyarrow as pa, pyarrow.parquet as papq
    from scripts.pipeline.profile import profile_slug

    table = pa.table({"label": ["a", "b", "a", "c", None, "a", "b"]})
    parquet = tmp_path / "x.parquet"
    papq.write_table(table, parquet)

    result = profile_slug(slug="tmp", parquet_path=parquet)
    label = result["columns"]["label"]
    assert label["dtype"] == "string"
    assert label["null_count"] == 1
    assert label["ndv_approx"] >= 3
    # NDV is small → top_values populated.
    assert label["top_values"] is not None
    top = {entry["value"]: entry["count"] for entry in label["top_values"]}
    assert top["a"] == 3


def test_profile_string_column_skips_topk_when_ndv_large(tmp_path):
    import pyarrow as pa, pyarrow.parquet as papq
    from scripts.pipeline.profile import profile_slug

    table = pa.table({"id": [f"u{i}" for i in range(1024)]})
    parquet = tmp_path / "ids.parquet"
    papq.write_table(table, parquet)

    result = profile_slug(slug="tmp", parquet_path=parquet)
    assert result["columns"]["id"]["top_values"] is None


def test_profile_list_column_length_stats(tmp_path):
    import pyarrow as pa, pyarrow.parquet as papq
    from scripts.pipeline.profile import profile_slug

    table = pa.table({"xs": [[1, 2], [3], [], [4, 5, 6]]})
    parquet = tmp_path / "lists.parquet"
    papq.write_table(table, parquet)

    result = profile_slug(slug="tmp", parquet_path=parquet)
    col = result["columns"]["xs"]
    assert col["dtype"] == "list"
    assert col["length_min"] == 0
    assert col["length_max"] == 3
    assert 0 < col["length_mean"] < 3


def test_profile_struct_column_emits_null_entry(tmp_path):
    import pyarrow as pa, pyarrow.parquet as papq
    from scripts.pipeline.profile import profile_slug

    inner = pa.struct([("v", pa.int32())])
    table = pa.table({"nested": pa.array([{"v": 1}, {"v": 2}], type=inner)})
    parquet = tmp_path / "structs.parquet"
    papq.write_table(table, parquet)

    result = profile_slug(slug="tmp", parquet_path=parquet)
    assert result["columns"]["nested"] is None


def test_profile_binary_column_uses_octet_length(tmp_path):
    """Binary columns produce mean_length in bytes; top_values stays null."""
    import pyarrow as pa, pyarrow.parquet as papq
    from scripts.pipeline.profile import profile_slug

    table = pa.table({"blob": pa.array([b"abc", b"defgh", b""], type=pa.binary())})
    parquet = tmp_path / "blobs.parquet"
    papq.write_table(table, parquet)

    result = profile_slug(slug="tmp", parquet_path=parquet)
    col = result["columns"]["blob"]
    assert col["dtype"] == "binary"
    assert col["null_count"] == 0
    assert col["ndv_approx"] >= 3
    # mean of byte-lengths: (3 + 5 + 0) / 3 ≈ 2.67
    assert col["mean_length"] is not None
    assert 2 < col["mean_length"] < 3
    # top_values intentionally null for binary
    assert col["top_values"] is None


def test_profile_map_column_uses_cardinality(tmp_path):
    """Map columns produce length_min/max/mean from cardinality."""
    import pyarrow as pa, pyarrow.parquet as papq
    from scripts.pipeline.profile import profile_slug

    map_type = pa.map_(pa.string(), pa.int32())
    table = pa.table({"m": pa.array([
        [("a", 1), ("b", 2)],
        [("a", 3)],
        [],
        [("a", 1), ("b", 2), ("c", 3)],
    ], type=map_type)})
    parquet = tmp_path / "maps.parquet"
    papq.write_table(table, parquet)

    result = profile_slug(slug="tmp", parquet_path=parquet)
    col = result["columns"]["m"]
    assert col["dtype"] == "map"
    assert col["null_count"] == 0
    assert col["length_min"] == 0
    assert col["length_max"] == 3
    assert 0 < col["length_mean"] < 3


def test_profile_large_string_column(tmp_path):
    """large_string dispatches through the same path as string."""
    import pyarrow as pa, pyarrow.parquet as papq
    from scripts.pipeline.profile import profile_slug

    table = pa.table({"s": pa.array(["alpha", "beta", "alpha"], type=pa.large_string())})
    parquet = tmp_path / "ls.parquet"
    papq.write_table(table, parquet)

    result = profile_slug(slug="tmp", parquet_path=parquet)
    col = result["columns"]["s"]
    assert col["dtype"] == "large_string"
    assert col["ndv_approx"] >= 2
    assert col["top_values"] is not None


def test_profile_handles_column_name_with_quotes(tmp_path):
    """A parquet column whose name contains a `"` must not break the SQL."""
    import pyarrow as pa, pyarrow.parquet as papq
    from scripts.pipeline.profile import profile_slug

    weird = 'foo"bar'
    table = pa.table({weird: [1, 2, 3]})
    parquet = tmp_path / "weird.parquet"
    papq.write_table(table, parquet)

    result = profile_slug(slug="tmp", parquet_path=parquet)
    col = result["columns"][weird]
    assert col is not None
    assert col["dtype"].startswith("int")
    assert col["min"] == 1
    assert col["max"] == 3


def test_uci_seeds_profile_matches_golden(uci_seeds_parquet, profile_schema):
    """Hermetic golden check — the committed fixture round-trips against a fresh profile."""
    from scripts.pipeline.profile import profile_slug
    fresh = profile_slug(slug="uci-seeds", parquet_path=uci_seeds_parquet)
    jsonschema.Draft202012Validator(profile_schema).validate(fresh)
    golden_path = REPO_ROOT / "tests" / "fixtures" / "profile_uci_seeds.json"
    golden = json.loads(golden_path.read_text())
    fresh.pop("computed_at"); golden.pop("computed_at")
    # parquet_sha256 should match because uci-seeds is row_stability: static
    assert fresh == golden, "uci-seeds profile drifted — see test docstring"
