# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Readers retain the generation acquired, even across atomic publication."""
import ctypes as c
import os
import threading

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import raincloud
from raincloud import _resolve
from raincloud._bundle import encode, make_bundle
from raincloud._locking import locked
from tests.test_native_clients import Stream
from tests.test_native_clients import native as native


@pytest.fixture
def generations(tmp_path):
    data = tmp_path / "data"
    handles = []
    for generation in (1, 2):
        spec = {"slug": "tiny", "transform": {"params": {"generation": generation}}}
        bundle = make_bundle(encode({"schema_version": 2, "datasets": [spec]}),
                             encode({"schema_version": 2, "slugs": {}}), "reader-publication")
        directory = tmp_path / str(generation)
        directory.mkdir()
        for name, raw in bundle.files().items():
            (directory / name).write_bytes(raw)
        config = raincloud.resolve_config(no_config=True, catalog=str(directory),
                    catalog_dir=tmp_path / "catalogs", data_dir=data, cache_dir=data, offline=True)
        handles.append(raincloud.load("tiny", format="arrow", config=config))
    def publish(generation, fmt="arrow", *, table=None, locking=True, root=None):
        path = (root or data) / _resolve.artifact_key("tiny", fmt, 2)
        path.parent.mkdir(parents=True, exist_ok=True)
        table = table if table is not None else pa.table({"generation": [generation] * 4})
        def write():
            temp = path.with_suffix(".new")
            if fmt == "arrow":
                with pa.ipc.new_file(temp, table.schema) as writer:
                    writer.write_table(table, max_chunksize=1)
            elif fmt == "parquet":
                pq.write_table(table, temp, row_group_size=1)
            else:
                vortex = pytest.importorskip("vortex")
                vortex.io.write(table, str(temp))
            os.replace(temp, path)
        if locking:
            with locked((root or data) / ".raincloud-write.lock"):
                write()
        else:
            write()
        return path
    return handles, publish


@pytest.mark.parametrize("fmt", ["arrow", "parquet", "vortex"])
def test_open_batches_survive_publication_without_blocking_writer(generations, fmt):
    handles, publish = generations
    publish(1, fmt)
    handle = raincloud.load("tiny", format=fmt, config=handles[0].config)
    with handle.batches(batch_size=1) as batches:
        first = next(batches)
        thread = threading.Thread(target=publish, args=(2, fmt))
        thread.start()
        thread.join(5)
        assert not thread.is_alive(), "reader retained the publication lock"
        assert pa.Table.from_batches([first, *batches])["generation"].to_pylist() == [1] * 4


def test_empty_materialization_does_not_reacquire_schema(generations, monkeypatch):
    handles, publish = generations
    publish(1, table=pa.table({"generation": pa.array([], type=pa.int64())}))
    original = raincloud.Dataset._acquire
    def interleave(self, fmt, opener):
        reader = original(self, fmt, opener)  # the file is open; now replace it
        publish(2, table=pa.table({"different": ["new"]}))
        return reader
    monkeypatch.setattr(raincloud.Dataset, "_acquire", interleave)
    result = handles[0].to_arrow()
    assert result.num_rows == 0 and result.schema.names == ["generation"]


@pytest.mark.parametrize("api,fmt", [("dataset", "parquet"), ("dataset", "vortex"), ("dataset", "arrow"),
                                     ("to_vortex", "vortex")])
def test_lazy_reusable_readers_keep_open_generation(generations, api, fmt):
    handles, publish = generations
    publish(1, fmt)
    handle = raincloud.load("tiny", format=fmt, config=handles[0].config)
    if api == "dataset":
        reader = handle.dataset()
        publish(2, fmt)
        for _ in range(2):
            assert reader.to_table(columns=["generation"])["generation"].to_pylist() == [1] * 4
    else:
        reader = handle.to_vortex()
        publish(2, fmt)
        for _ in range(2):
            with reader.to_arrow() as stream:
                assert stream.read_all()["generation"].to_pylist() == [1] * 4


@pytest.mark.parametrize("fmt", ["arrow", "parquet", "vortex"])
def test_native_open_stream_keeps_generation_after_handle_close(native, generations, fmt):
    handles, publish = generations
    publish(1, fmt)
    config = handles[0].config
    options = {"no_config": True, "catalog": config.catalog, "data_dir": str(config.data_dir),
               "cache_dir": str(config.cache_dir), "catalog_dir": str(config.catalog_dir), "offline": True}
    handle = native.open(options, fmt)
    stream = Stream()
    native.call("raincloud_batches", handle, 1, c.byref(stream))
    native.close(handle)
    publish(2, fmt)
    with pa.RecordBatchReader._import_from_c(c.addressof(stream)) as reader:
        assert reader.read_all()["generation"].to_pylist() == [1] * 4


@pytest.mark.parametrize("empty", [False, True])
def test_nested_parquet_projection_matches_native_reader(generations, empty):
    handles, publish = generations
    table = pa.table({"nested": [{"x": 1, "y": "a"}], "other": [7]})
    if empty:
        table = table.slice(0, 0)
    path = publish(1, "parquet", table=table)
    handle = raincloud.load("tiny", format="parquet", config=handles[0].config)
    with pq.ParquetFile(path) as reader:
        expected = reader.read(columns=["nested.x"])
    with handle.batches(columns=["nested.x"]) as batches:
        got = pa.Table.from_batches(list(batches), schema=expected.schema)
    assert got.equals(expected)
    # Empty iteration still has the native projected schema for materializers.
    with handle._batches(columns=["nested.x"]) as reader:
        assert reader.schema == expected.schema
        assert reader.read_all().equals(expected)


def test_variant_parquet_dataset_keeps_generation_and_native_decode_stays_available(generations):
    pytest.importorskip("duckdb")
    from raincloud import duckdb_connect

    handles, publish = generations
    path = publish(1, "parquet")
    temporary = path.with_suffix(".variant")
    with duckdb_connect() as connection:
        connection.execute("COPY (SELECT v FROM (VALUES (42::VARIANT), "
                           "({'a': [1, 2]}::VARIANT), ('text'::VARIANT), "
                           "(NULL::VARIANT)) t(v)) TO ? (FORMAT PARQUET)", [str(temporary)])
    with locked(handles[0].config.data_dir / ".raincloud-write.lock"):
        os.replace(temporary, path)
    handle = raincloud.load("tiny", format="parquet", config=handles[0].config)
    d = handle.dataset()
    # pyarrow carries VARIANT as its shredded struct; DuckDB decodes the logical
    # type natively only from the file itself (documented on Dataset.dataset).
    assert {"metadata", "value"} <= {f.name for f in d.schema.field("v").type}
    native = duckdb_connect().read_parquet(str(handle.path()))
    assert [str(dtype) for dtype in native.types] == ["VARIANT"]
    publish(2, "parquet")
    for _ in range(2):
        assert d.to_table().num_rows == 4 and d.schema.names == ["v"]


def test_dataset_releases_its_open_file_with_the_last_reference(generations):
    import gc
    import os
    handles, publish = generations
    path = publish(1, "parquet")

    def open_on_path():
        fds = os.listdir("/proc/self/fd")
        return sum(1 for fd in fds if os.path.realpath(f"/proc/self/fd/{fd}") == str(path.resolve()))
    before = open_on_path()
    d = raincloud.load("tiny", format="parquet", config=handles[0].config).dataset()
    assert open_on_path() == before + 1
    assert d.to_table().num_rows == 4
    del d
    gc.collect()
    assert open_on_path() == before
