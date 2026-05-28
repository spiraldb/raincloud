# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Source-tree synthetic build + handler-function tests.

Always-on hermetic guard for the pipeline stage chain: drives the real
`build.run_one(spec)` against a synthetic file:// source with all output
dirs under `tmp_path`. No marker — runs in the default `pytest`.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def _synth_spec(csv: Path) -> dict:
    return {
        "slug": "synth-e2e",
        "short_name": "Synth E2E",
        "full_name": "Synth E2E",
        "description": "synthetic file:// CSV through the full stage chain",
        "license": {"spdx": "CC0-1.0"},
        "fetch": {"type": "http", "urls": [csv.as_uri()]},
        "extract": {"type": "passthrough"},
        "parse": {"reader": "csv"},
        "transform": {"handler": "tighten_types"},
        "write": {"output": "synth-e2e.parquet", "compression": "zstd"},
        "expect": {"rows": 3},
        "convert": {"vortex": True},
    }


def test_pipeline_e2e_synthetic_build(tmp_path, monkeypatch):
    """fetch → extract → parse → transform → write → validate → convert end-to-end
    on a synthetic file:// CSV, all writes under tmp."""
    csv = tmp_path / "tiny.csv"
    csv.write_text("n,s\n1,a\n2,b\n3,c\n")
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    # No mirror, no offline; data_root() resolves outputs under RAINCLOUD_HOME.
    monkeypatch.delenv("RAINCLOUD_MIRROR", raising=False)
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)

    from scripts.pipeline.build import run_one

    spec = _synth_spec(csv)
    ok = run_one(spec, strict=True)
    assert ok, "run_one returned False — see captured stderr for stage failure"

    outputs = tmp_path / "home" / "outputs" / "v1" / "synth-e2e"
    parquet = outputs / "parquet" / "synth-e2e.parquet"
    vortex_path = outputs / "vortex" / "synth-e2e.vortex"
    assert parquet.exists(), parquet
    assert vortex_path.exists(), vortex_path

    # Validate the parquet content.
    tbl = pq.read_table(parquet)
    assert tbl.num_rows == 3
    assert tbl.column_names == ["n", "s"]
    # tighten_types should narrow the small-int 'n' column away from int64.
    n_type = str(tbl.schema.field("n").type)
    assert n_type != "int64", f"tighten_types did not narrow 'n' (got {n_type})"

    # Validate the vortex output round-trips.
    import vortex

    vt = vortex.open(str(vortex_path)).to_arrow().read_all()
    assert vt.num_rows == 3 and vt.column_names == ["n", "s"]


def test_handlers_identity_and_tighten_types_function():
    """Direct invocation: handlers *function* on synthetic Arrow tables."""
    from scripts.pipeline.handlers import identity, tighten_types

    src = pa.table(
        {"x": pa.array([1, 2, 3], type=pa.int64()), "s": ["a", "b", "c"]}
    )
    # identity: returns the single input table as the output.
    # Signature: (spec, [(Path, table)]) -> [(slug_str, table)]
    out = identity({"slug": "x"}, [(Path("ignore.csv"), src)])
    assert isinstance(out, list) and out and out[0][1].num_rows == 3

    # tighten_types: narrows int64 with tiny values to a smaller int dtype.
    out2 = tighten_types({"slug": "x"}, [(Path("ignore.csv"), src)])
    assert isinstance(out2, list) and out2
    narrowed = out2[0][1]
    assert str(narrowed.schema.field("x").type) != "int64"
