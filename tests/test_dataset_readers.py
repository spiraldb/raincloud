# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import json
from dataclasses import replace

import pyarrow as pa
import pytest

import raincloud
from raincloud.exceptions import ArtifactNotFound, FormatUnavailable
from tests.reader_fixture import create


@pytest.fixture
def prepared(tmp_path):
    table, options = create(tmp_path)
    config = raincloud.resolve_config(**options)
    return table, config


@pytest.mark.parametrize("fmt", ["arrow", "parquet", "vortex"])
def test_streaming_projection_and_early_close(prepared, fmt):
    table, config = prepared
    ds = raincloud.load("tiny", format=fmt, config=config)
    assert ds.catalog_id == "reader-fixture"
    with ds.batches(batch_size=2, columns=["id", "value"]) as batches:
        values = list(batches)
    assert all(b.num_rows <= 2 for b in values)
    assert pa.Table.from_batches(values).equals(table.select(["id", "value"]))
    with ds.batches(batch_size=1) as batches:
        assert next(batches).num_rows == 1
    assert ds.schema.names == table.schema.names
    assert len(ds.artifacts) == 3


def test_explicit_format_never_falls_back(prepared):
    _, config = prepared
    ds = raincloud.load("tiny", format="parquet", config=config)
    assert ds.path().parent.name == "parquet"
    for fmt in ("parquet@rs", "nonsense"):
        with pytest.raises(FormatUnavailable):
            raincloud.load("tiny", format=fmt, config=config)
    with pytest.raises(ValueError):
        with ds.batches(batch_size=0):
            pass


def test_read_miss_never_imports_build_toolchain(prepared, tmp_path, monkeypatch):
    _, config = prepared
    config = replace(config, data_dir=tmp_path / "absent", offline=False)
    monkeypatch.setattr(raincloud._resolve, "_build_import_error", lambda: pytest.fail("read imported builder"))
    ds = raincloud.load("tiny", config=config)
    assert not config.cache_dir.exists()
    assert json.dumps(ds.artifacts)
    with pytest.raises(ArtifactNotFound, match="raincloud build tiny"):
        ds.path()


def test_snapshot_records_writer_and_publish_plan(prepared, tmp_path, monkeypatch):
    from raincloud import catalogs
    from raincloud.pipeline import docs, publish
    _, config = prepared
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", tmp_path / "observed.json")
    with catalogs.operation(config) as context:
        docs.generate_snapshot(rehash=True)
        observed = json.loads((tmp_path / "observed.json").read_text())
        info = observed["slugs"]["tiny"]
        assert info["parquet_sha256"] == context.snapshot["slugs"]["tiny"]["parquet_sha256"]
        assert (info["parquet_writer"], info["arrow_writer"]) == ("py", "canonical")
        assert "artifacts" not in info
        plan = publish.plan_uploads(["tiny"], observed, outputs_root=config.data_dir, version=2)
        assert sorted(key.split("/")[2] for _, key in plan) == ["arrow", "parquet", "vortex"]
        observed["slugs"]["tiny"]["parquet_sha256"] = "0"*64
        with pytest.raises(publish.PublishMismatch):
            publish.plan_uploads(["tiny"], observed, outputs_root=config.data_dir, version=2)


def test_unsupported_reader_type_is_typed(prepared, monkeypatch):
    import pyarrow.parquet as pq
    _, config = prepared
    def unsupported(*args, **kwargs):
        raise pa.ArrowNotImplementedError("unsupported type fixture")
    monkeypatch.setattr(pq, "ParquetFile", unsupported)
    ds = raincloud.load("tiny", format="parquet", config=config)
    with pytest.raises(raincloud.UnsupportedType):
        with ds.batches():
            pass
