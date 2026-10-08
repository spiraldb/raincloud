# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The Python comparator gives every shared comparison case its verdict
(sidecars/compare_cases: the Rust and JVM comparators read the same files)."""
from __future__ import annotations

import json

import pyarrow as pa
import pytest

from raincloud.pipeline.export.compare import values_equal
from raincloud.pipeline.spec import REPO_ROOT

CASES = REPO_ROOT / "sidecars" / "compare_cases"


def _read(path):
    with pa.memory_map(str(path), "r") as source:
        return pa.ipc.open_file(source).read_all()


@pytest.mark.parametrize("case", json.loads((CASES / "cases.json").read_text())["cases"], ids=lambda c: c["name"])
def test_shared_comparison_case(case):
    got = _read(CASES / f"{case['name']}.got.arrow")
    expected = _read(CASES / f"{case['name']}.expected.arrow")
    equal, detail = values_equal(got, expected)
    assert equal == (case["verdict"] == "equal"), detail
