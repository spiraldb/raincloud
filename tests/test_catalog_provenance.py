# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Catalog provenance and encoding: the writer is provenance, not an address; non-finite
numbers are refused; and the streaming-handler template builds and exports."""
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import raincloud
from raincloud import _bundle
from raincloud._resolve import artifact_key
from raincloud.exceptions import CatalogError


@pytest.fixture(autouse=True)
def isolated(monkeypatch, tmp_path):
    import os
    for name in tuple(os.environ):
        if name.startswith("RAINCLOUD_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("RAINCLOUD_NO_CONFIG", "1")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "old-cache"))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "user-data"))


@pytest.mark.parametrize("fmt", ["parquet", "vortex"])
def test_writer_is_provenance_not_address(tmp_path, fmt):
    # One file per format, whichever writer made it; the catalog says which.
    import vortex
    table = pa.table({"x": [2]})
    path = tmp_path / "data" / artifact_key("tiny", fmt, 2)
    assert path.parent.name == fmt
    path.parent.mkdir(parents=True)
    if fmt == "parquet":
        pq.write_table(table, path)
    else:
        vortex.io.write(table, str(path))
    entry = {f"{fmt}_bytes": path.stat().st_size, f"{fmt}_writer": "rs",
             f"{fmt}_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    snapshot = {"schema_version": 2, "slugs": {"tiny": entry}}
    manifest = {"schema_version": 2, "datasets": [{"slug": "tiny", "export": {"formats": [fmt], "priority": ["rs", "py"]}}]}
    bundle = _bundle.make_bundle(_bundle.encode(manifest), _bundle.encode(snapshot), "writer-provenance")
    directory = tmp_path / "bundle"
    directory.mkdir()
    for name, raw in bundle.files().items():
        (directory / name).write_bytes(raw)
    cfg = raincloud.resolve_config(catalog=str(directory), data_dir=tmp_path / "data", offline=True)
    ds = raincloud.load("tiny", format=fmt, config=cfg)
    assert ds.to_arrow().column("x").to_pylist() == [2]
    assert raincloud.describe("tiny", config=cfg)["formats"][fmt]["writer"] == "rs"
    assert [a["writer"] for a in ds.artifacts if a["format"] == fmt] == ["rs"]
    with pytest.raises(raincloud.FormatUnavailable, match=f"ask for '{fmt}'"):
        raincloud.load("tiny", format=f"{fmt}@rs", config=cfg)


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity", "1e400", "-1e400"])
def test_catalog_rejects_nonfinite_json_numbers(token):
    with pytest.raises(CatalogError):
        _bundle.document(('{"expect":{"x":' + token + '}}').encode(), "sources.json")


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_catalog_encoding_rejects_nonfinite(value):
    with pytest.raises(CatalogError):
        _bundle.encode({"expect": {"x": value}})


def test_streaming_template_builds_canonical_and_exports(tmp_path, monkeypatch):
    from raincloud import catalogs
    from raincloud.catalogs import operation
    from raincloud.pipeline import handlers
    from raincloud.pipeline.build import run_one
    src = tmp_path / "rows.jsonl"
    src.write_text('{"x": 1, "s": "a"}\n{"x": 2, "s": "b"}\n')
    template = Path(raincloud.__file__).resolve().parents[1] / "templates/streaming_handler.py.tmpl"
    namespace = {"__name__": "raincloud.pipeline.handlers.template_probe", "__package__": "raincloud.pipeline.handlers"}
    exec(compile(template.read_text(), str(template), "exec"), namespace)
    monkeypatch.setitem(handlers._REGISTRY, "template_probe", namespace["my_streaming_handler"])
    caps = _bundle.capabilities()
    caps["builders"].append("handler:template_probe")
    monkeypatch.setattr(catalogs, "capabilities", lambda: caps)
    spec = {"slug": "template-probe", "fetch": {"type": "http", "urls": [src.as_uri()]},
            "extract": {"type": "passthrough"}, "parse": {"reader": "custom"},
            "transform": {"handler": "template_probe"}, "expect": {"rows": 2},
            "export": {"formats": ["parquet"]}}
    manifest = tmp_path / "sources.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": [spec]}))
    cfg = raincloud.resolve_config(manifest=manifest, data_dir=tmp_path / "hdd", scratch_dir=tmp_path / "scratch")
    with operation(cfg):
        assert run_one(spec, strict=True)
    ds = raincloud.load("template-probe", format="arrow", config=cfg, offline=True)
    assert ds.to_arrow().to_pydict() == {"x": [1, 2], "s": ["a", "b"]}
    assert raincloud.load("template-probe", format="parquet", config=cfg, offline=True).to_arrow().equals(ds.to_arrow())
    assert not list(cfg.scratch_dir.rglob("*.duckdb"))
