# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Single-file Lance artifacts: streaming, settings, loader, and measured failures."""
from __future__ import annotations

import pyarrow as pa
import pytest

import raincloud
from raincloud import _builds
from raincloud._resolve import artifact_key
from raincloud.catalogs import operation
from raincloud.pipeline import build
from raincloud.pipeline.export import get_exporter, get_reader, run_bounded, writer_toolchain
from raincloud.pipeline.export.bounded import ExportFailed, read_back_bounded
from raincloud.pipeline.export.compare import stream_equal
from raincloud.pipeline.spec import FORMAT_SETTINGS, prepared_artifact, write_settings
from tests._helpers import write_ipc
from tests.test_pipeline_contracts import TABLE, _catalog
from tests.test_pipeline_contracts import stages as stages  # noqa: F401

pytest.importorskip("lance")
from lance.file import LanceFileReader, LanceFileWriter  # noqa: E402


@pytest.fixture(autouse=True)
def settings(monkeypatch):
    for _, var, _ in FORMAT_SETTINGS["lance"]:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("RAINCLOUD_EXPORT_TIMEOUT", "15")


@pytest.mark.parametrize("empty", [False, True])
def test_lance_streams_nested_data_and_empty_schema(tmp_path, empty):
    table = pa.table({"x": pa.array([0., -0., float("nan"), None]),
                      "s": ["a", None, "", "é"],
                      "nested": pa.array([[1, None], None, [], [3]], pa.list_(pa.int32()))})
    if empty:
        table = table.slice(0, 0)
    canonical = write_ipc(tmp_path / "tiny.arrow.zstd", table, max_chunksize=1)
    result = get_exporter("lance@py").export({"slug": "tiny"}, canonical, tmp_path / "tiny.lance")
    assert result.compliance.roundtrip is True, result.compliance
    assert result.out_path.is_file()
    reader = LanceFileReader(str(result.out_path))
    meta = reader.metadata()
    assert (meta.major_version, meta.minor_version) == (2, 2)
    batches = reader.read_all(batch_size=2, batch_readahead=1).to_batches()
    assert stream_equal(batches.schema, batches, table.schema, table.to_batches())[0]
    assert get_reader("lance@py").read_conformance(result.out_path, canonical).status == "pass"


def test_lance_export_runs_after_parent_has_used_lance(tmp_path):
    # Native runtime threads initialized above must not deadlock a later bounded export.
    table = pa.table({"x": [1, 2]})
    with LanceFileWriter(str(tmp_path / "parent.lance")) as writer:
        writer.write_batch(table)
    canonical = write_ipc(tmp_path / "tiny.arrow.zstd", table)
    result = run_bounded(get_exporter("lance@py"), {"slug": "tiny"}, canonical, tmp_path / "bounded.lance")
    assert result.compliance.roundtrip is True
    assert read_back_bounded(get_exporter("lance@py"), result.out_path, canonical)[0]


def test_lance_build_load_and_record(tmp_path, stages):
    spec = {"slug": "tiny"}
    cfg = _catalog(tmp_path, "lance", [spec])
    with operation(cfg):
        assert build.run_one(spec, strict=False, formats=["lance"])
        assert prepared_artifact("tiny", "lance").is_file()
    ds = raincloud.load("tiny", format="lance", config=cfg, offline=True)
    assert ds.path().name == "tiny.lance"
    assert ds.to_arrow().equals(TABLE)
    assert ds.schema.equals(TABLE.schema)
    with ds.batches(batch_size=1, columns=["url", "x"]) as batches:
        assert pa.Table.from_batches(list(batches)).equals(TABLE.select(["url", "x"]))
    with ds.batches(batch_size=1) as batches:
        assert next(batches).num_rows == 1  # closes an early-terminated reader
    with pytest.raises(raincloud.UnknownColumn):
        with ds.batches(columns=["absent"]):
            pass
    with pytest.raises(raincloud.UnsupportedType, match="batches"):
        ds.dataset()
    entry = _builds.read(cfg.data_dir)[artifact_key("tiny", "lance", 2)]
    assert entry["verified"] is True and entry["writer"] == "py"
    assert "pylance" in writer_toolchain(get_exporter("lance@py"))


def test_lance_reader_keeps_open_generation(tmp_path, stages):
    cfg = _catalog(tmp_path, "generation", [{"slug": "tiny"}])
    with operation(cfg):
        assert build.run_one({"slug": "tiny"}, strict=False, formats=["lance"])
    ds = raincloud.load("tiny", format="lance", config=cfg, offline=True)
    other = tmp_path / "replacement.lance"
    with LanceFileWriter(str(other)) as writer:
        writer.write_batch(pa.table({"x": [9, 10], "url": ["changed", "changed"]}))
    with ds.batches(batch_size=1) as batches:
        first = next(batches)
        other.replace(ds.path())
        assert pa.Table.from_batches([first, *batches]).equals(TABLE)


@pytest.mark.parametrize("var,value", [("RAINCLOUD_LANCE_PAGE_BYTES", "bad"),
                                        ("RAINCLOUD_LANCE_DATA_CACHE_BYTES", "-1"),
                                        ("RAINCLOUD_LANCE_FILE_VERSION", "9.0")])
def test_lance_invalid_setting_is_named(monkeypatch, var, value):
    monkeypatch.setenv(var, value)
    with pytest.raises(ValueError, match=var):
        write_settings("lance")


@pytest.mark.parametrize("setting,value", [("DATA_CACHE_BYTES", "8192")])
def test_lance_page_settings_change_real_file_layout(tmp_path, monkeypatch, setting, value):
    # Fixed width, incompressible values, so reducing the page budget produces more pages.
    import numpy as np
    table = pa.table({"x": np.random.default_rng(0).random(30_000)})
    canonical = write_ipc(tmp_path / "tiny.arrow.zstd", table, max_chunksize=1000)
    exporter = get_exporter("lance@py")
    default = exporter.export({"slug": "tiny"}, canonical, tmp_path / "default.lance")
    monkeypatch.setenv(f"RAINCLOUD_LANCE_{setting}", value)
    small = exporter.export({"slug": "tiny"}, canonical, tmp_path / "small.lance")
    assert small.compliance.roundtrip is True
    before = LanceFileReader(str(default.out_path)).metadata()
    after = LanceFileReader(str(small.out_path)).metadata()
    assert (after.major_version, after.minor_version) == (2, 2)
    assert len(after.columns[0].pages) > len(before.columns[0].pages)
    toolchain = writer_toolchain(exporter)
    assert toolchain[f"lance_{setting.lower()}"] == value


def test_lance_reader_rejects_wrong_values(tmp_path):
    canonical = write_ipc(tmp_path / "tiny.arrow.zstd", pa.table({"x": [1]}))
    artifact = tmp_path / "wrong.lance"
    with LanceFileWriter(str(artifact)) as writer:
        writer.write_batch(pa.table({"x": [2]}))
    assert get_reader("lance@py").read_conformance(artifact, canonical).status == "fail"


def test_lance_explicit_file_version(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_LANCE_FILE_VERSION", "2.1")
    canonical = write_ipc(tmp_path / "tiny.arrow.zstd", pa.table({"x": [1]}))
    result = get_exporter("lance@py").export({"slug": "tiny"}, canonical, tmp_path / "v21.lance")
    metadata = LanceFileReader(str(result.out_path)).metadata()
    assert (metadata.major_version, metadata.minor_version) == (2, 1)
    assert result.compliance.roundtrip is True


def test_lance_unsupported_type_preserves_previous_file(tmp_path):
    arr = pa.UnionArray.from_dense(pa.array([0, 0], pa.int8()), pa.array([0, 1], pa.int32()),
                                  [pa.array([1, 2])], field_names=["i"])
    canonical = write_ipc(tmp_path / "tiny.arrow.zstd", pa.table({"u": arr}))
    previous = tmp_path / "tiny.lance"
    with LanceFileWriter(str(previous)) as writer:
        writer.write_batch(pa.table({"x": [1, 2]}))
    saved = previous.read_bytes()
    with pytest.raises(ExportFailed):
        run_bounded(get_exporter("lance@py"), {"slug": "tiny"}, canonical, previous)
    assert previous.read_bytes() == saved
    assert not list(tmp_path.glob(".*.export"))


def test_lance_refuses_ineffective_page_limit(tmp_path, monkeypatch):
    from raincloud.pipeline.export.exporters import UnsupportedOption
    monkeypatch.setenv("RAINCLOUD_LANCE_PAGE_BYTES", "4096")
    canonical = write_ipc(tmp_path / "tiny.arrow.zstd", pa.table({"x": [1]}))
    with pytest.raises(UnsupportedOption, match="cannot honour RAINCLOUD_LANCE_PAGE_BYTES=4096"):
        get_exporter("lance@py").export({"slug": "tiny"}, canonical, tmp_path / "out.lance")
    assert not (tmp_path / "out.lance").exists()


def test_lance_preserves_variant_storage_without_claiming_parquet_annotation(tmp_path):
    from tests.test_parquet_variant_sidecars import _variant_table
    table = _variant_table(null_row=True)
    canonical = write_ipc(tmp_path / "variant.arrow.zstd", table, max_chunksize=1)
    result = get_exporter("lance@py").export({"slug": "variant"}, canonical, tmp_path / "variant.lance")
    assert result.compliance.roundtrip is True, result.compliance
    assert result.compliance.variant_faithful is False
    assert "shredded struct" in result.compliance.note
