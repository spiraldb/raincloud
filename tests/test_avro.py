# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Avro: written by the sidecar lanes on request, and served by path -- pyarrow
reads no Avro, so the loader has no in-process reader for it."""
from __future__ import annotations

import os

import pytest

import raincloud
from raincloud._readers import reader_capabilities
from raincloud.catalogs import operation
from raincloud.exceptions import MissingDependency
from raincloud.pipeline import build
from raincloud.pipeline.spec import prepared_artifact
from tests.test_pipeline_contracts import _catalog
from tests.test_pipeline_contracts import stages as stages  # noqa: F401 (fixture)

SPEC = {"slug": "tiny"}


def test_avro_has_no_in_process_reader():
    assert reader_capabilities()["avro"] == {"available": False, "implementation": None}


@pytest.mark.skipif(not os.environ.get("RAINCLOUD_SIDECAR_AVRO_RS"),
                    reason="needs the avro@rs sidecar (RAINCLOUD_SIDECAR_AVRO_RS)")
def test_an_avro_file_is_written_on_request_and_served_by_path(tmp_path, stages):
    cfg = _catalog(tmp_path, "avro", [SPEC])
    with operation(cfg):
        assert build.run_one(SPEC, strict=False, formats=["avro"])
        written = prepared_artifact("tiny", "avro")
        assert written.read_bytes()[:4] == b"Obj\x01"
        assert written.read_bytes()[-16:] == b"raincloud-avro01"
    ds = raincloud.load("tiny", format="avro", config=cfg, offline=True)
    assert ds.path() == written
    with pytest.raises(MissingDependency, match=r"Dataset.path\(\)"):
        ds.to_arrow()
