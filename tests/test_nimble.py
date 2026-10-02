# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Nimble: written by `nimble@cpp` (upstream Nimble's C++, through raincloud-nimble)
on request, served by path, and absent wherever the tool is not built."""
from __future__ import annotations

import os

import pytest

import raincloud
from raincloud.catalogs import operation
from raincloud.pipeline import build
from raincloud.pipeline.export import cell_available, get_exporter
from raincloud.pipeline.spec import prepared_artifact
from tests.test_pipeline_contracts import _catalog
from tests.test_pipeline_contracts import stages as stages  # noqa: F401 (fixture)

SPEC = {"slug": "tiny"}
BUILT = bool(os.environ.get("RAINCLOUD_NIMBLE_TOOL") and os.environ.get("RAINCLOUD_SIDECAR_NIMBLE_CPP"))


def test_the_lane_needs_the_tool_as_well_as_the_sidecar(monkeypatch, tmp_path):
    sidecar = tmp_path / "nimble-write"
    sidecar.write_text("#!/bin/sh\n")
    sidecar.chmod(0o755)
    monkeypatch.setenv("RAINCLOUD_SIDECAR_NIMBLE_CPP", str(sidecar))
    monkeypatch.setenv("RAINCLOUD_NIMBLE_TOOL", str(tmp_path / "missing"))
    monkeypatch.setenv("PATH", str(tmp_path))
    assert not cell_available("nimble@cpp")
    assert "raincloud-nimble" in get_exporter("nimble@cpp").unavailable()
    tool = tmp_path / "raincloud-nimble"
    tool.write_text("#!/bin/sh\n")
    monkeypatch.setenv("RAINCLOUD_NIMBLE_TOOL", str(tool))
    assert cell_available("nimble@cpp")
    toolchain = get_exporter("nimble@cpp").toolchain()
    assert toolchain["helper"] == "raincloud-nimble" and len(toolchain["helper_sha256"]) == 16


@pytest.mark.skipif(not BUILT, reason="needs raincloud-nimble (RAINCLOUD_NIMBLE_TOOL) and the nimble@cpp sidecar")
def test_a_nimble_file_is_written_on_request_and_served_by_path(tmp_path, stages):
    cfg = _catalog(tmp_path, "nimble", [SPEC])
    with operation(cfg):
        assert build.run_one(SPEC, strict=False, formats=["nimble"])
        written = prepared_artifact("tiny", "nimble")
        assert written.is_file()
    assert raincloud.load("tiny", format="nimble", config=cfg, offline=True).path() == written
