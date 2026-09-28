# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parity tests for the REAL Rust sidecar binaries vs the Python in-process reader.

The rest of the compliance suite drives the sidecar plumbing through MOCK shell
scripts, so it never exercises the compiled Rust `logical_eq`. This module runs
the real `parquet-read` binary against crafted inputs and asserts its verdict
AGREES with the Python `_roundtrip_verdict` on the same (artifact, canonical)
pair: a reference reader must match the in-process reader's LOGICAL-equality
semantics (pyarrow's raise-on-lossy).

Runs when RAINCLOUD_READER_PARQUET_RS names the binary, as the native-reader
runner (`scripts/test_native_readers.py`) and a machine config set it; skips
otherwise, so a pure-Python checkout's `pytest` is unaffected.
"""
from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.export.readers import _roundtrip_verdict
from tests._helpers import write_ipc

_PARQUET_READ = os.environ.get("RAINCLOUD_READER_PARQUET_RS")

pytestmark = pytest.mark.skipif(
    not _PARQUET_READ,
    reason="set RAINCLOUD_READER_PARQUET_RS to the Rust parquet-read sidecar",
)


def _rust_verdict(tmp_path: Path, canonical_tbl: pa.Table, artifact_tbl: pa.Table) -> str:
    canon = tmp_path / "c.arrow.zstd"
    artifact = tmp_path / "a.parquet"
    report = tmp_path / "r.json"
    write_ipc(canon, canonical_tbl)
    pq.write_table(artifact_tbl, artifact)
    subprocess.run(
        [str(_PARQUET_READ), "--input", str(artifact),
         "--canonical", str(canon), "--report", str(report)],
        check=False,
    )
    return json.loads(report.read_text())["status"]


def _python_verdict(canonical_tbl: pa.Table, artifact_tbl: pa.Table, tmp_path: Path) -> str:
    # Mirror what the Rust reader does: read the parquet artifact, compare to the
    # canonical via the in-process _roundtrip_verdict.
    artifact = tmp_path / "py.parquet"
    pq.write_table(artifact_tbl, artifact)
    got = pq.read_table(artifact)
    return _roundtrip_verdict("parquet@py", got, canonical_tbl).status


# (name, canonical, artifact, expected_status)
_CASES = [
    (
        "identity",
        pa.table({"n": pa.array([1, 2, 3], pa.int32()), "s": ["a", "b", "c"]}),
        pa.table({"n": pa.array([1, 2, 3], pa.int32()), "s": ["a", "b", "c"]}),
        "pass",
    ),
    (
        "benign-widening",  # int64 values that fit int32 -> lossless -> pass
        pa.table({"n": pa.array([1, 2, 100], pa.int32())}),
        pa.table({"n": pa.array([1, 2, 100], pa.int64())}),
        "pass",
    ),
    (
        "int-overflow",  # int64 value out of int32 range -> lossy -> fail
        pa.table({"n": pa.array([1, 2, 100], pa.int32())}),
        pa.table({"n": pa.array([1, 2, 9999999999], pa.int64())}),
        "fail",
    ),
    (
        "ts-truncation",  # ts[ms] with sub-second precision vs ts[s] -> lossy -> fail
        pa.table({"t": pa.array([100], pa.timestamp("s"))}),
        pa.table({"t": pa.array([100500], pa.timestamp("ms"))}),
        "fail",
    ),
    (
        "wrong-value",  # same type, different data -> fail
        pa.table({"n": pa.array([1, 2, 3], pa.int32())}),
        pa.table({"n": pa.array([1, 2, 4], pa.int32())}),
        "fail",
    ),
    (
        "row-mismatch",  # fewer rows -> fail
        pa.table({"n": pa.array([1, 2, 3], pa.int32())}),
        pa.table({"n": pa.array([1, 2], pa.int32())}),
        "fail",
    ),
]


@pytest.mark.parametrize("name,canonical,artifact,expected", _CASES,
                         ids=[c[0] for c in _CASES])
def test_rust_parquet_read_matches_python_verdict(tmp_path, name, canonical, artifact, expected):
    """The real Rust parquet-read verdict == the Python in-process verdict == the
    expected verdict (parity with pyarrow's raise-on-lossy semantics)."""
    rust = _rust_verdict(tmp_path, canonical, artifact)
    py = _python_verdict(canonical, artifact, tmp_path)
    assert rust == expected, f"{name}: rust={rust} expected={expected}"
    assert rust == py, f"{name}: rust={rust} disagrees with python={py}"
