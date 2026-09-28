# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Conformance test for the exporter seam (protocol + result types + registry).

Verifies the frozen dataclass shapes, `runtime_checkable` structural
conformance, and the registry's register/get/dup semantics. No real exporter is
exercised — a minimal in-file `_FakeExporter` stands in for the parquet/vortex
implementations.
"""
from __future__ import annotations

import dataclasses
from pathlib import Path

import pytest

from raincloud.pipeline import export
from raincloud.pipeline.export import base
from raincloud.pipeline.export.base import (
    Compliance,
    Exporter,
    ExportResult,
)


class _FakeExporter:
    """Duck-typed stand-in for a real exporter."""

    format_id = "fake"
    cell_id = "fake@x"

    def unavailable(self) -> str | None:
        return None

    def out_path(self, slug: str) -> Path:
        return Path(f"/tmp/{slug}.out")

    def export(self, spec: dict, canonical: Path, dest: Path | None = None) -> ExportResult:
        return ExportResult(
            format_id=self.cell_id,
            out_path=Path("/tmp/fake.out"),
            nbytes=0,
            sha256="0" * 64,
            compliance=Compliance(roundtrip=True, variant_faithful=True, note="ok"),
        )


@pytest.fixture
def clean_registry():
    """Snapshot and restore the module-level registry so tests don't leak state.

    Also declares the fake cell: `register` refuses a cell_id that is not in
    `raincloud._registry`, because an undeclared cell is invisible to the
    capability check the loader runs. Declaring it here keeps that guard intact
    rather than carving out an exception to it.
    """
    from raincloud import _registry

    saved = dict(export._EXPORTERS)
    saved_cells = dict(_registry.PY_EXPORTERS)
    export._EXPORTERS.clear()
    _registry.PY_EXPORTERS["fake@x"] = "tests:_FakeExporter"
    try:
        yield
    finally:
        export._EXPORTERS.clear()
        export._EXPORTERS.update(saved)
        _registry.PY_EXPORTERS.clear()
        _registry.PY_EXPORTERS.update(saved_cells)


def test_fake_is_structural_exporter():
    assert isinstance(_FakeExporter(), Exporter)  # presence-only: runtime_checkable ignores signatures


def test_dataclass_shapes_are_frozen():
    comp = Compliance(roundtrip=True, variant_faithful=False, note="n")
    assert (comp.roundtrip, comp.variant_faithful, comp.note) == (True, False, "n")
    assert dataclasses.is_dataclass(comp)
    with pytest.raises(dataclasses.FrozenInstanceError):
        comp.roundtrip = False  # type: ignore[misc]

    res = ExportResult(
        format_id="fake",
        out_path=Path("/tmp/x"),
        nbytes=7,
        sha256="a" * 64,
        compliance=comp,
    )
    assert res.format_id == "fake"
    assert res.nbytes == 7
    assert res.compliance is comp
    with pytest.raises(dataclasses.FrozenInstanceError):
        res.nbytes = 8  # type: ignore[misc]


def test_compliance_note_defaults_empty():
    comp = Compliance(roundtrip=False, variant_faithful=False)
    assert comp.note == ""


def test_registry_register_get_and_list(clean_registry):
    fake = _FakeExporter()
    assert export.all_exporters() == []
    export.register(fake)
    assert export.get_exporter("fake@x") is fake
    assert export.all_exporters() == [fake]


def test_registry_rejects_duplicate(clean_registry):
    export.register(_FakeExporter())
    with pytest.raises(ValueError):
        export.register(_FakeExporter())


def test_registry_missing_raises_keyerror(clean_registry):
    with pytest.raises(KeyError):
        export.get_exporter("nope")


def test_base_module_reexports_match():
    # __init__ re-exports the same objects defined in base.
    assert export.Exporter is base.Exporter
    assert export.ExportResult is base.ExportResult
    assert export.Compliance is base.Compliance
