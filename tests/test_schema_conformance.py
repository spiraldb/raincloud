# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Schema compatibility precedes values in Python and compiled Rust readers."""
from __future__ import annotations

import json
import subprocess
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.export.readers import _roundtrip_verdict
from tests._helpers import sidecar, write_ipc
from tests.test_java_reader_schema_compare import _cases as java_cases
from tests.test_java_reader_schema_compare import _contains_fixed_binary, _fixed_list_cases


def _cases():
    yield from _fixed_list_cases()
    for case in java_cases():
        expected, got, status = case.values
        # Java cannot compare changed timezone identity and records a gap. These
        # comparators know it is not a physical normalization, even without rows.
        yield pytest.param(expected, got, "fail" if status == "skip" else status, id=case.id)
    for name, expected, got, status in [
        ("populated-integer-float", pa.array([1]), pa.array([1.0]), "fail"),
        ("extension-json-storage", pa.array(["{}", None], pa.json_()), pa.array(["{}", None]), "pass"),
        ("extension-uuid-storage", pa.array([b"0" * 16, None], pa.uuid()),
         pa.array([b"0" * 16, None], pa.binary()), "pass"),
        ("string-view", pa.array(["a", None], pa.string_view()), pa.array(["a", None]), "pass"),
        ("binary-view", pa.array([b"a", None], pa.binary_view()), pa.array([b"a", None]), "pass"),
        ("list-fixed", pa.array([[1], None], pa.list_(pa.int64(), 1)),
         pa.array([[1], None], pa.list_(pa.int64())), "pass"),
        ("date-width", pa.array([86400000, None], pa.date64()), pa.array([1, None], pa.date32()), "pass"),
        ("time-units", pa.array([1000000, None], pa.time64("us")), pa.array([1, None], pa.time32("s")), "pass"),
        ("duration-units", pa.array([1000, None], pa.duration("ms")), pa.array([1, None], pa.duration("s")), "pass"),
        ("decimal-width", pa.array([Decimal("1.5"), None], pa.decimal256(40, 1)),
         pa.array([Decimal("1.5"), None], pa.decimal128(8, 1)), "pass"),
        ("populated-cross-family", pa.array([1]), pa.array(["1"]), "fail"),
        ("null-integer-float", pa.array([None], pa.int64()), pa.array([None], pa.float64()), "fail"),
        ("null-binary-string", pa.array([None], pa.binary()), pa.array([None], pa.string()), "fail"),
        ("null-date-integer", pa.array([None], pa.date32()), pa.array([None], pa.int32()), "fail"),
        ("null-list-cross-family", pa.array([None], pa.list_(pa.string())),
         pa.array([None], pa.list_(pa.int64())), "fail"),
        ("populated-integer-width", pa.array([1, None], pa.int64()), pa.array([1, None], pa.int32()), "pass"),
        ("float-width", pa.array([1.5, None], pa.float64()), pa.array([1.5, None], pa.float32()), "pass"),
        ("string-width", pa.array(["a", None], pa.large_string()), pa.array(["a", None]), "pass"),
        ("binary-width", pa.array([b"a", None], pa.large_binary()), pa.array([b"a", None]), "pass"),
        ("binary-fixed", pa.array([b"a", None], pa.binary(1)), pa.array([b"a", None]), "pass"),
        ("dictionary", pa.array(["a", None]), pa.array(["a", None]).dictionary_encode(), "pass"),
        ("list-width-and-element-name", pa.array([[1], None], pa.large_list(pa.field("original", pa.int64()))),
         pa.array([[1], None], pa.list_(pa.field("element", pa.int32()))), "pass"),
        ("struct-child-width", pa.array([{"a": 1}, None], pa.struct([("a", pa.int64())])),
         pa.array([{"a": 1}, None], pa.struct([("a", pa.int32())])), "pass"),
        ("timestamp-units", pa.array([1000, None], pa.timestamp("ms", "UTC")),
         pa.array([1, None], pa.timestamp("s", "UTC")), "pass"),
        ("timestamp-lossy", pa.array([1], pa.timestamp("s")), pa.array([1001], pa.timestamp("ms")), "fail"),
        ("decimal-scale", pa.array([Decimal("1.50"), None], pa.decimal128(12, 2)),
         pa.array([Decimal("1.5"), None], pa.decimal128(8, 1)), "pass"),
        ("decimal-lossy", pa.array([Decimal("1.5")], pa.decimal128(8, 1)),
         pa.array([Decimal("1.51")], pa.decimal128(12, 2)), "fail"),
    ]:
        yield pytest.param(pa.table({"x": expected}), pa.table({"x": got}), status, id=name)


@pytest.mark.parametrize("expected,got,status", list(_cases()))
@pytest.mark.parametrize("reader", ["python", "parquet-rust", "vortex-rust"])
def test_schema_conformance(tmp_path, expected, got, status, reader):
    if reader == "python":
        verdict = _roundtrip_verdict("test", got, expected)
        assert verdict.status == status, verdict
        return
    format = reader.split("-")[0]
    binary = sidecar(f"{format}@rs", "read")
    canonical, artifact, report = (tmp_path / name for name in ("canonical.arrow", f"artifact.{format}", "report.json"))
    write_ipc(canonical, expected, compression=None)
    if format == "parquet":
        pq.write_table(got, artifact)
    else:
        vortex = pytest.importorskip("vortex")
        if any(_contains_fixed_binary(field.type) for field in got.schema):
            pytest.skip("Vortex writer does not support fixed-size binary arrays")
        if pa.types.is_duration(got.schema[0].type):
            pytest.skip("Vortex writer does not support Arrow duration arrays")
        vortex.io.write(vortex.array(got), str(artifact))
    subprocess.run([binary, "--input", str(artifact), "--canonical", str(canonical),
                    "--report", str(report)], check=True, capture_output=True, timeout=60)
    verdict = json.loads(report.read_text())
    assert verdict["status"] == status, verdict
