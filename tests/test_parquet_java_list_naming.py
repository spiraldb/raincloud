# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""parquet@java reads Parquet lists and maps whatever their level names.

A reader that accepts a 3-level LIST only when its element is named "element" or
"item" fails (`UnsupportedNestedEncodingException: CHILD_NAME`) on a list whose
element is named "l", as wikipedia-structured-contents' is, and as arrow-rs writes.
parquet-format's LogicalTypes.md says those names "should not be enforced as errors
when reading". parquet@java writes the spec's names ("element", "key"/"value") and
restores the Arrow names from ARROW:schema. Each test skips when a binary it needs
is not installed.
"""
from __future__ import annotations

import json
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.export.readers import PyarrowParquetReader
from tests._helpers import sidecar, write_ipc


def _table():
    # Element, key and value names that are not the spec's, nested one level down too.
    inner = pa.list_(pa.field("item", pa.int32()))
    entry = pa.map_(pa.field("keys", pa.string(), nullable=False), pa.field("values", inner))
    return pa.table({
        "tags": pa.array([["a", None], None, []], pa.list_(pa.field("l", pa.string()))),
        "grid": pa.array([[[1, 2], None], [[3]], None], pa.list_(pa.field("l", inner))),
        "attrs": pa.array([[("k", [7, None])], None, []], entry),
    })


def _canonical(tmp_path, table):
    return write_ipc(tmp_path / "canonical.arrow.zstd", table)


def _write(cell, tmp_path, canonical):
    artifact, report = tmp_path / f"{cell.split('@')[1]}.parquet", tmp_path / f"{cell.split('@')[1]}.json"
    proc = subprocess.run([sidecar(cell), "--input", str(canonical), "--output", str(artifact),
                           "--report", str(report)], capture_output=True, text=True, timeout=300)
    assert proc.returncode == 0, proc.stderr
    return artifact, json.loads(report.read_text())


def _read_java(tmp_path, artifact, canonical):
    report = tmp_path / f"{artifact.stem}.read-java.json"
    proc = subprocess.run([sidecar("parquet@java", "read"), "--input", str(artifact), "--canonical", str(canonical),
                           "--report", str(report)], capture_output=True, text=True, timeout=300)
    assert proc.returncode == 0, proc.stderr
    return json.loads(report.read_text())


def _level_names(artifact):
    """The repeated-group and leaf names under each list/map, as the file's schema spells them."""
    schema = pq.ParquetFile(artifact).schema
    return sorted({schema.column(i).path for i in range(len(schema))})


def test_parquet_java_writes_spec_names_and_reads_its_own_file(tmp_path):
    canonical = _canonical(tmp_path, _table())
    artifact, verdict = _write("parquet@java", tmp_path, canonical)
    assert verdict["roundtrip"] is True, verdict
    assert _level_names(artifact) == [
        "attrs.key_value.key", "attrs.key_value.value.list.element",
        "grid.list.element.list.element", "tags.list.element",
    ]
    assert _read_java(tmp_path, artifact, canonical)["status"] == "pass"
    assert PyarrowParquetReader().read_conformance(artifact, canonical).status == "pass"


@pytest.mark.parametrize("compliant", [True, False])
def test_parquet_java_reads_pyarrow_lists_under_either_naming(tmp_path, compliant):
    # use_compliant_nested_type=False keeps the Arrow names ("l", "item") as the element.
    canonical = _canonical(tmp_path, _table())
    artifact = tmp_path / f"py-{compliant}.parquet"
    pq.write_table(_table(), artifact, use_compliant_nested_type=compliant)
    assert ("tags.list.l" in _level_names(artifact)) is not compliant
    got = _read_java(tmp_path, artifact, canonical)
    assert got["status"] == "pass", got


def test_parquet_java_reads_arrow_rs_lists_named_l(tmp_path):
    canonical = _canonical(tmp_path, _table())
    artifact, verdict = _write("parquet@rs", tmp_path, canonical)
    assert verdict["roundtrip"] is True, verdict
    assert "tags.list.l" in _level_names(artifact)
    got = _read_java(tmp_path, artifact, canonical)
    assert got["status"] == "pass", got
