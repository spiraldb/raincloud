# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The same adversarial artifacts must receive consistent native-reader verdicts."""
from __future__ import annotations

import json
import subprocess

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.export.readers import PyarrowParquetReader, _roundtrip_verdict
from tests._helpers import sidecar, write_ipc


def _cases():
    for dtype, wrap in [
        (pa.list_(pa.float64()), lambda x: [x]),
        (pa.struct([("f", pa.float64())]), lambda x: {"f": x}),
        (pa.list_(pa.struct([("f", pa.float64())])), lambda x: [{"f": x}]),
    ]:
        for label, got, expected, verdict in [
            ("zero", 0.0, -0.0, "fail"),
            ("nan", float("nan"), float("nan"), "pass"),
        ]:
            yield pytest.param(pa.array([wrap(got), None], dtype),
                               pa.array([wrap(expected), None], dtype), verdict,
                               id=f"{dtype}-{label}")
    yield pytest.param(pa.array([2], pa.int64()), pa.array([True]), "fail", id="lossy-bool")
    yield pytest.param(pa.array([[-1]], pa.list_(pa.int64())),
                       pa.array([[2**64 - 1]], pa.list_(pa.uint64())), "fail", id="nested-unsigned")
    for got_type, expected_type in [(pa.float32(), pa.float64()), (pa.float64(), pa.float32())]:
        yield pytest.param(pa.array([1.5, -0.0, None], got_type),
                           pa.array([1.5, -0.0, None], expected_type), "pass",
                           id=f"float-width-{got_type}")
    yield pytest.param(pa.array([1.1], pa.float64()), pa.array([1.1], pa.float32()),
                       "fail", id="lossy-float-width")


@pytest.mark.parametrize("got,expected,status", list(_cases()))
@pytest.mark.parametrize("reader", ["python", "rust", "java"])
def test_reader_fidelity(tmp_path, got, expected, status, reader):
    canonical = pa.table({"x": expected})
    artifact = pa.table({"x": got})
    if reader != "python":
        binary = sidecar({"rust": "parquet@rs", "java": "parquet@java"}[reader], "read")
    ipc, parquet, report = (tmp_path / name for name in ("c.arrow.zstd", "a.parquet", "r.json"))
    write_ipc(ipc, canonical)
    pq.write_table(artifact, parquet)
    if reader == "python":
        verdict = PyarrowParquetReader().read_conformance(parquet, ipc)
        assert verdict.status == status, verdict
        return
    subprocess.run([binary, "--input", str(parquet), "--canonical", str(ipc),
                    "--report", str(report)], check=True, capture_output=True, timeout=60)
    verdict = json.loads(report.read_text())
    assert verdict["status"] == status, verdict


def test_null_struct_parent_hides_child_payload():
    # These children differ only under a null parent; neither is a logical value.
    mask = pa.array([True, False])
    got = pa.StructArray.from_arrays([pa.array([0.0, -0.0])], names=["f"], mask=mask)
    expected = pa.StructArray.from_arrays([pa.array([-0.0, -0.0])], names=["f"], mask=mask)
    assert _roundtrip_verdict("test", pa.table({"x": got}), pa.table({"x": expected})).status == "pass"


@pytest.mark.parametrize("dtype", [pa.large_list(pa.float64()), pa.list_(pa.float64(), 1),
                                   pa.map_(pa.string(), pa.float64())])
def test_additional_nested_float_shapes(dtype):
    wrap = (lambda x: [("k", x)]) if pa.types.is_map(dtype) else (lambda x: [x])
    for got_value, expected_value, status in [(0.0, -0.0, "fail"),
                                            (float("nan"), float("nan"), "pass")]:
        got = pa.table({"x": pa.array([None, wrap(got_value)], dtype).slice(1)})
        expected = pa.table({"x": pa.array([None, wrap(expected_value)], dtype).slice(1)})
        assert _roundtrip_verdict("test", got, expected).status == status
