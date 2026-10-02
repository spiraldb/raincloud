# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""ORC: written by `orc@py` (pyarrow, the Apache ORC C++ library) on request,
read back by the loader, and a type the library does not write is measured,
never converted for it."""
from __future__ import annotations

import pyarrow as pa
import pytest

import raincloud
from raincloud import _builds
from raincloud._resolve import artifact_key
from raincloud.catalogs import operation
from raincloud.pipeline import build
from raincloud.pipeline.spec import prepared_artifact
from tests.test_pipeline_contracts import TABLE, _catalog
from tests.test_pipeline_contracts import stages as stages  # noqa: F401 (fixture)

pytest.importorskip("pyarrow._orc")
SPEC = {"slug": "tiny"}


def test_an_orc_file_is_written_on_request_and_loads(tmp_path, stages):
    cfg = _catalog(tmp_path, "orc", [SPEC])
    with operation(cfg):
        assert build.run_one(SPEC, strict=False, formats=["orc"])
        assert prepared_artifact("tiny", "orc").is_file()
    ds = raincloud.load("tiny", format="orc", config=cfg, offline=True)
    assert ds.path().name == "tiny.orc"
    assert ds.to_arrow().equals(TABLE)
    with ds.batches(batch_size=1, columns=["x"]) as batches:
        assert [b.to_pydict() for b in batches] == [{"x": [1]}, {"x": [2]}]
    assert ds.dataset().count_rows() == 2
    # ORC is opt-in and never what `auto` picks.
    assert raincloud.load("tiny", config=cfg, offline=True).format != "orc"


def test_unsigned_and_view_columns_are_widened_and_read_back_equal(tmp_path, stages, monkeypatch):
    table = pa.table({"u8": pa.array([1, None], pa.uint8()), "u64": pa.array([2**64 - 1, 0], pa.uint64()),
                      "v": pa.array(["a", None], pa.string_view()),
                      "nested": pa.array([{"u": 1}, None], pa.struct([("u", pa.uint32())]))})
    monkeypatch.setattr(build, "transform", lambda spec, tables: [(spec["slug"], table)])
    cfg = _catalog(tmp_path, "orc-widen", [SPEC])
    with operation(cfg):
        assert build.run_one(SPEC, strict=False, formats=["orc@py"])
    stored = raincloud.load("tiny", format="orc", config=cfg, offline=True).schema
    assert [stored.field(n).type for n in ("u8", "u64", "v")] == [pa.int16(), pa.decimal128(20, 0), pa.string()]
    assert stored.field("nested").type == pa.struct([("u", pa.int64())])


def test_a_type_orc_cannot_write_is_measured_unavailable(tmp_path, stages, monkeypatch):
    table = pa.table({"t": pa.array([1, 2], pa.time64("us"))})
    monkeypatch.setattr(build, "transform", lambda spec, tables: [(spec["slug"], table)])
    cfg = _catalog(tmp_path, "orc-uint", [SPEC])
    with operation(cfg):
        assert build.run_one(SPEC, strict=False, formats=["orc"])
        assert not prepared_artifact("tiny", "orc").exists()
    measured = _builds.read(cfg.data_dir)[artifact_key("tiny", "orc", 2)]["unavailable"]
    assert measured["cell"] == "orc@py" and "time64" in measured["error"]
