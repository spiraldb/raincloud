# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Cross-language regressions for exact recipes and original Vortex schemas."""
import json
import os
import subprocess
from pathlib import Path

import pyarrow as pa
import pytest

import raincloud
from raincloud._bundle import encode, make_bundle
from tests.reader_fixture import create
from tests.test_native_clients import native as native  # shared optional native fixture


@pytest.mark.parametrize("token", [
    str(2**64), str(2**64 + 1), str(-2**63 - 1), str(-2**63 - 2),
    "1" + "0" * 400, "-0", "-0.0", "1.00000000000000001",
    "1e-5", "1E+20", "1e16", "4.9406564584124654e-324",
    "1e-400", "-1e-400", "1.7976931348623157e308",
])
def test_exact_recipe_matches_python(native, tmp_path, token):
    _, options = create(tmp_path)
    catalog = Path(options["catalog"])
    manifest = json.loads((catalog / "sources.json").read_bytes())
    manifest["datasets"][0]["expect"]["numeric_control"] = "TOKEN"
    # Preserve the original JSON token, including -0 and alternate float spellings.
    raw = encode(manifest).replace(b'"TOKEN"', token.encode())
    bundle = make_bundle(raw, (catalog / "snapshot.json").read_bytes(), "reader-fixture")
    for name, data in bundle.files().items():
        (catalog / name).write_bytes(data)
    ds = raincloud.load("tiny", format="arrow", config=raincloud.resolve_config(**options))
    path = ds.path()
    handle = native.open(options, "arrow")
    try:
        assert native.metadata(handle)["recipe"] == ds.recipe_fingerprint
        assert Path(native.string("raincloud_path", handle)) == path
    finally:
        native.close(handle)


@pytest.mark.parametrize("got,expected,status", [
    (pa.table({"other": [1]}), pa.table({"x": [1]}), "fail"),
    (pa.table({"other": pa.array([], pa.int64())}),
     pa.table({"x": pa.array([], pa.int64())}), "fail"),
    (pa.table({"x": [1.1]}), pa.table({"x": pa.array([1.1], pa.float32())}), "fail"),
    (pa.table({"x": [1.5]}), pa.table({"x": pa.array([1.5], pa.float32())}), "pass"),
    (pa.table({"x": ["a", None]}),
     pa.table({"x": pa.array(["a", None], pa.large_string())}), "pass"),
])
def test_vortex_original_schema(tmp_path, got, expected, status):
    binary = os.environ.get("RAINCLOUD_READER_VORTEX_RS")
    if not binary:
        pytest.skip("set RAINCLOUD_READER_VORTEX_RS to the Rust vortex-read sidecar")
    import vortex
    artifact, canonical, report = (tmp_path / name for name in ("data.vortex", "data.arrow", "report.json"))
    vortex.io.write(got, str(artifact))
    with pa.ipc.new_file(canonical, expected.schema) as writer:
        writer.write_table(expected)
    subprocess.run([binary, "--input", str(artifact), "--canonical", str(canonical),
                    "--report", str(report)], check=True, capture_output=True, text=True)
    assert json.loads(report.read_text())["status"] == status


def test_adjacent_wide_integers_give_different_recipes(native, tmp_path):
    # Seeds one apart past 2**64 must not collapse to one fingerprint, in either reader.
    _, options = create(tmp_path)
    catalog = Path(options["catalog"])
    manifest = json.loads((catalog / "sources.json").read_bytes())
    snapshot = (catalog / "snapshot.json").read_bytes()
    recipes = []
    for seed in (2**64, 2**64 + 1):
        manifest["datasets"][0]["expect"]["seed"] = seed
        bundle = make_bundle(encode(manifest), snapshot, "reader-fixture")
        for name, data in bundle.files().items():
            (catalog / name).write_bytes(data)
        handle = native.open(options, "arrow")
        try:
            recipes.append(native.metadata(handle)["recipe"])
        finally:
            native.close(handle)
        assert recipes[-1] == raincloud.load("tiny", format="arrow",
                                             config=raincloud.resolve_config(**options)).recipe_fingerprint
    assert recipes[0] != recipes[1]
