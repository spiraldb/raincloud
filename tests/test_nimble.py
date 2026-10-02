# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Nimble: written by `nimble@cpp` (upstream Nimble's C++, built by
sidecars/nimble/build.sh) on request, served by path, absent where it is not built."""
from __future__ import annotations

import os

import pytest

import raincloud
from raincloud.catalogs import operation
from raincloud.pipeline import build
from raincloud.pipeline.export import cell_available
from raincloud.pipeline.spec import prepared_artifact
from tests.test_pipeline_contracts import _catalog
from tests.test_pipeline_contracts import stages as stages  # noqa: F401 (fixture)

SPEC = {"slug": "tiny"}
BUILT = bool(os.environ.get("RAINCLOUD_SIDECAR_NIMBLE_CPP"))


def test_the_lane_is_absent_without_its_binary(monkeypatch, tmp_path):
    monkeypatch.delenv("RAINCLOUD_SIDECAR_NIMBLE_CPP", raising=False)
    monkeypatch.setenv("PATH", str(tmp_path))
    assert not cell_available("nimble@cpp")


@pytest.mark.skipif(not BUILT, reason="needs the nimble@cpp binaries (sidecars/nimble/build.sh)")
def test_a_nimble_file_is_written_on_request_and_served_by_path(tmp_path, stages):
    cfg = _catalog(tmp_path, "nimble", [SPEC])
    with operation(cfg):
        assert build.run_one(SPEC, strict=False, formats=["nimble"])
        written = prepared_artifact("tiny", "nimble")
        assert written.is_file()
    assert raincloud.load("tiny", format="nimble", config=cfg, offline=True).path() == written
