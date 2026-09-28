# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The native C ABI dispatches to the raincloud CLI and reads what it returns.

Set RAINCLOUD_NATIVE_LIBRARY to the built cdylib to run this optional lane.
Resolution policy is Python's and tested there; these tests pin the native
contract on top of it — stream ownership, handle metadata, and that every
Python failure surfaces as the same stable error code.
"""
import ctypes as c
import json
import os
import sys
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest

import raincloud
from raincloud import catalogs
from raincloud._bundle import encode, make_bundle
from tests.reader_fixture import create


class Failure(c.Structure):
    _fields_ = [("code", c.c_int32), ("message", c.c_void_p)]


class Stream(c.Structure):
    _fields_ = [(name, c.c_void_p) for name in ("get_schema", "get_next", "get_last_error", "release", "private_data")]


class NativeError(Exception):
    def __init__(self, code, message):
        self.code = code
        super().__init__(message)


class Native:
    def __init__(self, path):
        self.lib = lib = c.CDLL(path)
        lib.raincloud_open.argtypes = [c.c_char_p, c.c_char_p, c.c_char_p, c.POINTER(c.c_void_p), c.POINTER(Failure)]
        lib.raincloud_metadata.argtypes = [c.c_void_p, c.POINTER(c.c_void_p), c.POINTER(Failure)]
        lib.raincloud_path.argtypes = lib.raincloud_metadata.argtypes
        lib.raincloud_batches.argtypes = [c.c_void_p, c.c_size_t, c.POINTER(Stream), c.POINTER(Failure)]
        lib.raincloud_close.argtypes = [c.c_void_p]
        lib.raincloud_string_free.argtypes = [c.c_void_p]
        lib.raincloud_error_free.argtypes = [c.POINTER(Failure)]

    def call(self, name, *args):
        failure = Failure()
        code = getattr(self.lib, name)(*args, c.byref(failure))
        try:
            if code:
                raise NativeError(code, c.string_at(failure.message).decode())
        finally:
            self.lib.raincloud_error_free(c.byref(failure))

    def open(self, options, fmt="auto", slug="tiny"):
        handle = c.c_void_p()
        self.call("raincloud_open", json.dumps(options).encode(), slug.encode(), fmt.encode(), c.byref(handle))
        return handle

    def string(self, name, handle):
        out = c.c_void_p()
        self.call(name, handle, c.byref(out))
        try:
            return c.string_at(out).decode()
        finally:
            self.lib.raincloud_string_free(out)

    def metadata(self, handle):
        return json.loads(self.string("raincloud_metadata", handle))

    def close(self, handle):
        self.lib.raincloud_close(handle)


@pytest.fixture
def native(monkeypatch):
    path = os.environ.get("RAINCLOUD_NATIVE_LIBRARY")
    if not path:
        pytest.skip("set RAINCLOUD_NATIVE_LIBRARY for the native client contract lane")
    # The library runs the CLI of the Python under test, not whatever is on PATH.
    monkeypatch.setenv("RAINCLOUD_CLI", str(Path(sys.executable).with_name("raincloud")))
    return Native(path)


@pytest.fixture
def prepared(tmp_path):
    return create(tmp_path)


@pytest.mark.parametrize("fmt", ["arrow", "parquet", "vortex"])
def test_native_stream_owns_reader_after_dataset_close(native, prepared, fmt):
    table, options = prepared
    handle = native.open(options, fmt)
    stream = Stream()
    native.call("raincloud_batches", handle, 2, c.byref(stream))
    native.close(handle)
    with pa.RecordBatchReader._import_from_c(c.addressof(stream)) as reader:
        batches = list(reader)
        assert all(batch.num_rows <= 2 for batch in batches)
        got = pa.Table.from_batches(batches).cast(table.schema)
        assert got.equals(table)
        # Equality treats signed zero as equal: verify its physical bits too.
        assert got.column("value").combine_chunks().buffers()[1].to_pybytes()[:16] == table.column("value").combine_chunks().buffers()[1].to_pybytes()[:16]
    assert not stream.release


@pytest.mark.parametrize("fmt", ["arrow", "parquet", "vortex"])
def test_native_early_close(native, prepared, fmt):
    _, options = prepared
    handle = native.open(options, fmt)
    stream = Stream()
    native.call("raincloud_batches", handle, 1, c.byref(stream))
    native.close(handle)
    with pa.RecordBatchReader._import_from_c(c.addressof(stream)) as reader:
        assert reader.read_next_batch().num_rows == 1


def test_native_metadata_matches_python(native, prepared):
    _, options = prepared
    handle = native.open(options, "parquet")
    try:
        metadata = native.metadata(handle)
        ds = raincloud.load("tiny", format="parquet", config=raincloud.resolve_config(**options))
        assert metadata["catalog_revision"] == ds.catalog_revision
        assert metadata["recipe"] == ds.recipe_fingerprint
        assert metadata["catalog_id"] == ds.catalog_id
        assert sorted(metadata["artifacts"], key=lambda a: a["key"]) == sorted(ds.artifacts, key=lambda a: a["key"])
        assert native.string("raincloud_path", handle) == str(ds.path())
    finally:
        native.close(handle)


def test_native_offline_laziness_and_explicit_selection(native, prepared, tmp_path):
    _, options = prepared
    options["data_dir"] = str(tmp_path / "absent")
    with pytest.raises(NativeError) as error:
        native.open(options, "parquet@java")
    assert error.value.code == 5
    handle = native.open(options, "arrow")
    try:
        assert native.metadata(handle)["rows"] == 8
        assert not Path(options["cache_dir"]).exists()
        with pytest.raises(NativeError) as error:
            native.string("raincloud_path", handle)
        assert error.value.code == 6
    finally:
        native.close(handle)


def test_native_uses_active_pin_and_holds_metadata(native, prepared):
    _, options = prepared
    cfg = raincloud.resolve_config(**options)
    bundle = catalogs.resolve_context(cfg).bundle
    catalogs.store(cfg, bundle)
    catalogs.pin(replace(cfg, catalog="auto"), bundle.revision)
    options["catalog"] = "auto"
    handle = native.open(options, "arrow")
    try:
        assert native.metadata(handle)["catalog_revision"] == bundle.revision
        (cfg.catalog_dir / "active.json").write_bytes(encode({"active": "f"*64, "pinned": True, "history": []}))
        assert native.metadata(handle)["catalog_revision"] == bundle.revision
        assert Path(native.string("raincloud_path", handle)).is_file()
        with pytest.raises(NativeError) as error:
            native.open(options)
        assert error.value.code == 3
    finally:
        native.close(handle)


def test_native_mirror_adoption_is_python_readable(native, prepared, tmp_path):
    table, options = prepared
    options.update(mirror=Path(options["data_dir"]).as_uri(), data_dir=str(tmp_path / "empty"), offline=False)
    handle = native.open(options, "parquet")
    try:
        path = native.string("raincloud_path", handle)
        cfg = raincloud.resolve_config(**{**options, "offline": True})
        ds = raincloud.load("tiny", format="parquet", config=cfg)
        assert str(ds.path()) == path
        assert ds.to_arrow().equals(table)
    finally:
        native.close(handle)


def test_native_strict_mirror_mismatch_does_not_adopt(native, prepared, tmp_path):
    _, options = prepared
    root = Path(options["data_dir"])
    path = root / "v2/tiny/arrow/tiny.arrow.zstd"
    path.write_bytes(b"bad artifact")
    options.update(mirror=root.as_uri(), data_dir=str(tmp_path / "empty"), offline=False)
    handle = native.open(options, "arrow")
    try:
        with pytest.raises(NativeError) as error:
            native.string("raincloud_path", handle)
        assert error.value.code == 8
        assert not (Path(options["cache_dir"]) / "v2/tiny/arrow/tiny.arrow.zstd").exists()
    finally:
        native.close(handle)


@pytest.mark.parametrize("value", [1e-5, 1e-7, 1e20, 1e16, -0.0, 1.2345678901234567])
def test_native_numeric_recipe_fingerprints(native, tmp_path, value):
    manifest = {"schema_version": 2, "datasets": [{"slug": "tiny", "expect": {"tolerance": value}}]}
    bundle = make_bundle(encode(manifest), encode({"schema_version": 2, "slugs": {}}), "numbers")
    for name, raw in bundle.files().items():
        (tmp_path / name).write_bytes(raw)
    options = {"no_config": True, "catalog": str(tmp_path)}
    handle = native.open(options)
    try:
        ds = raincloud.load("tiny", config=raincloud.resolve_config(**options))
        assert native.metadata(handle)["recipe"] == ds.recipe_fingerprint
    finally:
        native.close(handle)


def test_native_toml_relative_paths_and_environment_precedence(native, prepared, tmp_path, monkeypatch):
    _, options = prepared
    config = tmp_path / "config.toml"
    config.write_text('[raincloud]\ndata_dir="data"\ncache_dir="cache"\ncatalog="catalog"\noffline=true\n')
    handle = native.open({"config": str(config)}, "arrow")
    try:
        assert Path(native.string("raincloud_path", handle)).parent.parent.parent.parent == tmp_path / "data"
    finally:
        native.close(handle)
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(tmp_path / "absent"))
    handle = native.open({"config": str(config)}, "arrow")
    try:
        with pytest.raises(NativeError) as error:
            native.string("raincloud_path", handle)
        assert error.value.code == 6
    finally:
        native.close(handle)
    handle = native.open({"config": str(config), "data_dir": options["data_dir"]}, "arrow")
    try:
        assert Path(native.string("raincloud_path", handle)).is_file()
    finally:
        native.close(handle)


def test_native_http_mirror_and_cross_process_lock(native, prepared, tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from functools import partial
    from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    from raincloud._locking import locked

    _, options = prepared
    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=options["data_dir"]))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    options.update(mirror=f"http://127.0.0.1:{server.server_port}", data_dir=str(tmp_path / "absent"), offline=False)
    handle = native.open(options, "parquet")
    try:
        with ThreadPoolExecutor(1) as pool:
            with locked(Path(options["cache_dir"]) / ".raincloud-write.lock"):
                future = pool.submit(native.string, "raincloud_path", handle)
                from concurrent.futures import TimeoutError
                with pytest.raises(TimeoutError):
                    future.result(timeout=0.15)
            path = future.result(timeout=10)
            assert Path(path).is_file()
        ds = raincloud.load("tiny", format="parquet", config=raincloud.resolve_config(**{**options, "offline": True}))
        assert str(ds.path()) == path
    finally:
        native.close(handle)
        server.shutdown()
        server.server_close()
        thread.join()


def test_native_readonly_hit_writes_nothing(native, prepared):
    _, options = prepared
    root = Path(options["data_dir"])
    files = [p for p in root.rglob("*") if p.is_file()]
    before = [(p, p.stat().st_mtime_ns) for p in files]
    for path in files:
        path.chmod(0o444)
    handle = native.open(options, "arrow")
    try:
        assert Path(native.string("raincloud_path", handle)).is_file()
        assert before == [(p, p.stat().st_mtime_ns) for p in files]
        assert not list(root.rglob("*.pin"))
        assert not Path(options["cache_dir"]).exists()
    finally:
        native.close(handle)


@pytest.mark.parametrize("change", ["hash", "future_format", "reader", "shape"])
def test_native_rejects_incompatible_or_damaged_bundle(native, prepared, change):
    _, options = prepared
    path = Path(options["catalog"]) / "catalog.json"
    meta = json.loads(path.read_bytes())
    if change == "hash":
        meta["files"]["sources.json"] = "0"*64
    elif change == "future_format":
        # catalog_format 1 carried an engine version window; 2 dropped it, and
        # an unreadable catalog now announces itself by its format number alone.
        meta["catalog_format"] = meta["catalog_format"] + 1
    elif change == "reader":
        meta["readers"].append("future-reader")
    else:
        meta["catalog_format"] = True
    path.write_bytes(encode(meta))
    with pytest.raises(NativeError) as error:
        native.open(options)
    assert error.value.code == 2
    with pytest.raises(raincloud.CatalogError):
        raincloud.load("tiny", config=raincloud.resolve_config(**options))
