# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Identical public reads through Python and the compiled C ABI.

Use real catalog bundles, IPC bytes and filesystem failures. No resolver,
hashing, path normalization or native operation is mocked.
"""
import ctypes as c
import hashlib
import json
import os
from pathlib import Path

import pyarrow as pa
import pytest

import raincloud
from raincloud._bundle import encode, make_bundle
from tests.reader_fixture import create
from tests.test_native_clients import NativeError, Stream
from tests.test_native_clients import native as native


def tree(root):
    return {str(p.relative_to(root)): (p.stat().st_mtime_ns, hashlib.sha256(p.read_bytes()).hexdigest())
            for p in root.rglob("*") if p.is_file()}


def native_table(native, handle):
    stream = Stream()
    native.call("raincloud_batches", handle, 3, c.byref(stream))
    with pa.RecordBatchReader._import_from_c(c.addressof(stream)) as reader:
        return reader.read_all()


@pytest.mark.parametrize("origin,spelling", [
    ("api", "absolute"), ("api", "relative"), ("api", "home"),
    ("env", "relative"), ("env", "home"),
    ("toml", "relative"), ("toml", "home"),
])
def test_catalog_selection_reads_same_generation(native, tmp_path, monkeypatch, origin, spelling):
    table, options = create(tmp_path / "fixture with spaces")
    catalog = Path(options.pop("catalog"))
    cwd = tmp_path / "different working directory"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    config = tmp_path / "settings" / "config.toml"
    config.parent.mkdir()
    base = config.parent if origin == "toml" else cwd
    selector = (str(catalog) if spelling == "absolute" else
                "~/" + os.path.relpath(catalog, Path.home()) if spelling == "home" else
                os.path.relpath(catalog, base))
    # A wrong precedence decision must fail, not silently open the same bundle.
    config.write_text("[raincloud]\ncatalog=" + json.dumps(selector if origin == "toml" else "missing") + "\n")
    monkeypatch.delenv("RAINCLOUD_NO_CONFIG", raising=False)
    options.pop("no_config")
    options["config"] = str(config)
    if origin == "api":
        options["catalog"] = selector
        monkeypatch.setenv("RAINCLOUD_CATALOG", str(tmp_path / "missing env catalog"))
    elif origin == "env":
        monkeypatch.setenv("RAINCLOUD_CATALOG", selector)
    else:
        monkeypatch.delenv("RAINCLOUD_CATALOG", raising=False)
    before = tree(tmp_path)
    python = raincloud.load("tiny", format="arrow", config=raincloud.resolve_config(**options))
    handle = native.open(options, "arrow")
    try:
        assert native.metadata(handle)["catalog_revision"] == python.catalog_revision
        assert native.metadata(handle)["recipe"] == python.recipe_fingerprint
        assert Path(native.string("raincloud_path", handle)).resolve() == python.path().resolve()
        assert python.to_arrow().equals(table)
        assert native_table(native, handle).equals(table)
    finally:
        native.close(handle)
    assert tree(tmp_path) == before
    assert not Path(options["cache_dir"]).exists()


@pytest.mark.parametrize("fmt", ["arrow", "parquet", "vortex"])
def test_unreadable_artifact_is_an_io_failure(native, tmp_path, fmt):
    # An access failure, in every reader: never a corrupt artifact.
    _, options = create(tmp_path)
    path = next((Path(options["data_dir"]) / f"v2/tiny/{fmt}").iterdir())
    mode = path.stat().st_mode
    path.chmod(0)
    handle = native.open(options, fmt)
    try:
        if os.access(path, os.R_OK):
            pytest.skip("this platform/user bypasses file read permissions")
        python = raincloud.load("tiny", format=fmt, config=raincloud.resolve_config(**options))
        with pytest.raises(OSError):
            python.to_arrow()
        with pytest.raises(NativeError) as error:
            native_table(native, handle)
        assert error.value.code == 12
    finally:
        path.chmod(mode)
        native.close(handle)


def test_a_file_the_catalog_does_not_describe_is_refused(native, tmp_path):
    # The catalog is the authority: a file at the key with another size is not
    # its artifact, and the refusal says what to do. Same-size edits are
    # outside the model -- bytes are checked when they enter a store.
    _, options = create(tmp_path)
    path = Path(options["data_dir"]) / "v2/tiny/arrow/tiny.arrow.zstd"
    path.write_bytes(path.read_bytes() + b"x")
    python = raincloud.load("tiny", format="arrow", config=raincloud.resolve_config(**options))
    with pytest.raises(raincloud.OfflineMiss, match="not the catalog's file") as error:
        python.path()
    assert "raincloud build tiny" in str(error.value)
    handle = native.open(options, "arrow")
    try:
        with pytest.raises(NativeError) as error:
            native.string("raincloud_path", handle)
        assert error.value.code == 6
    finally:
        native.close(handle)


def test_generated_recipe_has_same_identity_in_native_reader(native, tmp_path):
    table, options = create(tmp_path)
    catalog = Path(options['catalog'])
    manifest = json.loads((catalog / 'sources.json').read_text())
    manifest['datasets'][0]['fetch'] = {
        'type': 'generated', 'generator': 'duckdb-tpch', 'version': '1.5.5',
        'parameters': {'sf': 1}, 'output': 'region',
    }
    snapshot = (catalog / 'snapshot.json').read_bytes()
    bundle = make_bundle(encode(manifest), snapshot, 'reader-fixture')
    for name, content in bundle.files().items():
        (catalog / name).write_bytes(content)
    python = raincloud.load('tiny', format='arrow', config=raincloud.resolve_config(**options))
    handle = native.open(options, 'arrow')
    try:
        assert native.metadata(handle)['recipe'] == python.recipe_fingerprint
        assert native.metadata(handle)['catalog_revision'] == python.catalog_revision
        assert native_table(native, handle).equals(table)
        assert python.to_arrow().equals(table)
    finally:
        native.close(handle)
