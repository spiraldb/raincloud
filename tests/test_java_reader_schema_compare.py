# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Java readers must retain and compare schemas even without populated cells."""
from __future__ import annotations

import json
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests._helpers import sidecar, write_ipc


def _cases():
    integer, string = pa.int64(), pa.string()
    struct = pa.struct([("a", integer)])
    renamed = pa.struct([("b", integer)])
    extra = pa.struct([("a", integer), ("b", integer)])
    for name, expected, got, values, status in [
        ("empty-match", integer, integer, [], "pass"),
        ("empty-cross-family", integer, string, [], "fail"),
        ("null-cross-family", integer, string, [None], "fail"),
        ("null-struct-names", struct, renamed, [None], "fail"),
        ("null-struct-arity", struct, extra, [None], "fail"),
        ("empty-list-child-names", pa.list_(struct), pa.list_(renamed), [[]], "fail"),
        ("null-integer-width", integer, pa.int32(), [None], "pass"),
        ("empty-string-width", string, pa.large_string(), [], "pass"),
        ("empty-list-width", pa.list_(integer), pa.large_list(pa.int32()), [[]], "pass"),
        # A dropped timezone is a mismatch in every lane (Python, Rust, JVM), not a gap.
        ("empty-timestamp-timezone", pa.timestamp("ms", "UTC"), pa.timestamp("ms"), [], "fail"),
    ]:
        yield pytest.param(pa.table({"x": pa.array(values, expected)}),
                           pa.table({"x": pa.array(values, got)}), status, id=name)
    # Fixed binary dimensions survive Parquet. Check recursively
    # without relying on populated values, and retain fixed/variable normalizations.
    for shape, wrap, values in [
        ("empty", lambda t: t, []),
        ("null", lambda t: t, [None]),
        ("null-struct", lambda t: pa.struct([("a", t)]), [None]),
        ("empty-list", pa.list_, [[]]),
    ]:
        for name, expected, got, status in [
            ("binary-dimension", pa.binary(1), pa.binary(2), "fail"),
            ("binary-fixed-match", pa.binary(1), pa.binary(1), "pass"),
            ("binary-fixed-variable", pa.binary(1), pa.binary(), "pass"),
            ("list-fixed-variable", pa.list_(integer, 1), pa.list_(integer), "pass"),
        ]:
            if shape == "null-struct" and name.endswith("variable"):
                continue  # parquet-arrow-java cannot decode these null-parent shapes
            yield pytest.param(pa.table({"x": pa.array(values, wrap(expected))}),
                               pa.table({"x": pa.array(values, wrap(got))}), status,
                               id=f"{shape}-{name}")
    for name, expected, got in [
        ("populated-binary-fixed-variable", pa.array([b"a", None], pa.binary(1)),
         pa.array([b"a", None], pa.binary())),
        ("populated-list-fixed-variable", pa.array([[1], None], pa.list_(integer, 1)),
         pa.array([[1], None], pa.list_(integer))),
    ]:
        yield pytest.param(pa.table({"x": expected}), pa.table({"x": got}), "pass", id=name)
    for name, expected_type, got_type, expected_value, got_value in [
        ("binary", pa.binary(1), pa.binary(), b"a", b"a"),
        ("list", pa.list_(integer, 1), pa.list_(integer), [1], [1]),
    ]:
        expected = pa.table({"x": pa.array([[expected_value], None, []], pa.list_(expected_type))})
        got = pa.table({"x": pa.array([[got_value], None, []], pa.list_(got_type))})
        yield pytest.param(expected, got, "pass", id=f"nested-populated-{name}-fixed-variable")
        changed = b"ab" if name == "binary" else [1, 2]
        got = pa.table({"x": pa.array([[changed], None, []], pa.list_(got_type))})
        yield pytest.param(expected, got, "fail", id=f"nested-populated-{name}-lossy")
    yield pytest.param(pa.table({"expected": pa.array([], integer)}),
                       pa.table({"WRONG": pa.array([], integer)}), "fail", id="empty-column-name")
    yield pytest.param(pa.table({"x": pa.array([], integer)}),
                       pa.table({"x": pa.array([], integer), "extra": pa.array([], integer)}),
                       "fail", id="empty-column-count")


def _fixed_list_cases():
    for shape, wrap, values in [
        ("empty", lambda t: t, []),
        ("null", lambda t: t, [None]),
        ("nested-empty-list", pa.list_, [[]]),
    ]:
        expected, got = (wrap(pa.list_(pa.int64(), n)) for n in (1, 2))
        yield pytest.param(pa.table({"x": pa.array(values, expected)}),
                           pa.table({"x": pa.array(values, got)}), "fail",
                           id=f"{shape}-list-dimension")


def _contains_fixed_binary(type):
    if pa.types.is_fixed_size_binary(type):
        return True
    if pa.types.is_struct(type):
        return any(_contains_fixed_binary(field.type) for field in type)
    if pa.types.is_list(type) or pa.types.is_large_list(type) or pa.types.is_fixed_size_list(type):
        return _contains_fixed_binary(type.value_type)
    return False


@pytest.mark.parametrize("expected,got,status", list(_cases()))
@pytest.mark.parametrize("format,impl", [("parquet", "java"), ("parquet", "hardwood"), ("vortex", "jni")])
def test_java_schema_without_values(tmp_path, expected, got, status, format, impl):
    verdict = _read_java(tmp_path, expected, got, format, impl)
    assert verdict["status"] == status, verdict
    assert "read error" not in verdict["note"], verdict


@pytest.mark.parametrize("expected,got,status", list(_fixed_list_cases()))
@pytest.mark.parametrize("format,impl", [("parquet", "java"), ("parquet", "hardwood"), ("vortex", "jni")])
def test_java_fixed_list_dimensions(tmp_path, expected, got, status, format, impl):
    verdict = _read_java(tmp_path, expected, got, format, impl)
    # parquet-arrow-java and Hardwood reconstruct Parquet LIST as variable-size Arrow
    # lists: no fixed dimension survives decoding. Vortex retains the actual dimension.
    assert verdict["status"] == ("pass" if format == "parquet" else status), verdict
    assert "read error" not in verdict["note"], verdict


def _read_java(tmp_path, expected, got, format, impl):
    binary = sidecar(f"{format}@{impl}", "read")
    canonical = tmp_path / "canonical.arrow.zstd"
    artifact = tmp_path / f"artifact.{format}"
    report = tmp_path / "report.json"
    write_ipc(canonical, expected)  # zero rows deliberately produce no IPC record batches
    if format == "parquet":
        pq.write_table(got, artifact)
    else:
        vortex = pytest.importorskip("vortex")
        if any(_contains_fixed_binary(field.type) for field in got.schema):
            pytest.skip("Vortex writer does not support fixed-size binary arrays")
        vortex.io.write(vortex.array(got), str(artifact))
    subprocess.run([binary, "--input", str(artifact), "--canonical", str(canonical),
                    "--report", str(report)], check=True, capture_output=True, timeout=60)
    verdict = json.loads(report.read_text())
    return verdict
