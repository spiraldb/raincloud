# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import hashlib
import json
import os

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud import load, resolve_config
from tests.test_native_clients import native as native


def setup_layers(tmp_path, monkeypatch):
    for key in tuple(os.environ):
        # RAINCLOUD_CLI only tells native readers which CLI to run; it is not a setting.
        if key.startswith("RAINCLOUD_") and key != "RAINCLOUD_CLI":
            monkeypatch.delenv(key)
    # These tests describe an installed machine; a checkout skips system TOML.
    import raincloud.config
    monkeypatch.setattr(raincloud.config, "_is_checkout", lambda root: False)
    roots = [tmp_path / "preferred", tmp_path / "fallback"]
    user = tmp_path / "user"
    monkeypatch.setenv("XDG_CONFIG_DIRS", os.pathsep.join(map(str, roots)))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(user))
    files = [root / "raincloud/config.toml" for root in [*roots, user]]
    for p in files:
        p.parent.mkdir(parents=True)
    return files


@pytest.mark.skipif(os.name != "posix" or __import__('sys').platform == 'darwin', reason="XDG config discovery")
def test_shared_dataset_read_and_user_override(tmp_path, monkeypatch):
    preferred, fallback, user = setup_layers(tmp_path, monkeypatch)
    data = tmp_path / "shared"
    artifact = data / "v2/tiny/parquet/tiny.parquet"
    artifact.parent.mkdir(parents=True)
    pq.write_table(pa.table({"value": [10, 20]}), artifact)
    manifest = tmp_path / "sources.json"
    snapshot = tmp_path / "snapshot.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": [{"slug": "tiny"}]}))
    snapshot.write_text(json.dumps({"schema_version": 2, "slugs": {"tiny": {
        "parquet_bytes": artifact.stat().st_size,
        "parquet_sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
        "last_built_rows": 2}}}))
    fallback.write_text('[raincloud]\nmirror="file:///unused"\noffline=false\n')
    preferred.write_text('[raincloud]\ndata_dir="../../shared"\n'
                         f'manifest="{manifest}"\nsnapshot="{snapshot}"\noffline=true\n')
    user.write_text('[raincloud]\ncache_dir="my-cache"\nmirror=""\n')
    cfg = resolve_config()
    assert cfg.data_dir == data
    assert cfg.cache_dir == user.parent / "my-cache"
    assert cfg.offline and cfg.mirror == ""
    assert dict(cfg.origins)["data_dir"] == str(preferred)
    before = (artifact.stat().st_ino, artifact.stat().st_mtime_ns)
    ds = load("tiny", format="parquet")
    assert ds.path() == artifact
    with ds.batches(batch_size=1) as batches:
        assert [b.column(0)[0].as_py() for b in batches] == [10, 20]
    assert before == (artifact.stat().st_ino, artifact.stat().st_mtime_ns)
    assert not cfg.cache_dir.exists()
    user.write_text(user.read_text() + 'data_dir="private"\n')
    assert resolve_config().data_dir == user.parent / "private"
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(tmp_path / "environment"))
    assert resolve_config().data_dir == tmp_path / "environment"
    assert resolve_config(data_dir=tmp_path / "explicit").data_dir == tmp_path / "explicit"


@pytest.mark.skipif(os.name != "posix" or __import__('sys').platform == 'darwin', reason="XDG config discovery")
def test_explicit_and_disabled_config_are_isolated(tmp_path, monkeypatch):
    preferred, fallback, user = setup_layers(tmp_path, monkeypatch)
    preferred.write_text('[raincloud]\nunknown="bad"\n')
    with pytest.raises(ValueError, match="unknown settings"):
        resolve_config()
    explicit = tmp_path / "explicit.toml"
    explicit.write_text('[raincloud]\ndata_dir="private"\n')
    assert resolve_config(config=explicit).data_dir == tmp_path / "private"
    cfg = resolve_config(no_config=True, repo_root=tmp_path / "no-checkout")
    assert cfg.file is None
    assert str(preferred) not in dict(cfg.origins).values()


@pytest.mark.skipif(os.name != "posix" or __import__('sys').platform == 'darwin', reason="XDG config discovery")
def test_native_and_python_discover_same_shared_defaults(native, tmp_path, monkeypatch):
    from tests.reader_fixture import create
    from tests.test_client_boundaries import native_table
    table, options = create(tmp_path / "fixture")
    preferred, fallback, user = setup_layers(tmp_path, monkeypatch)
    # The native reader runs the CLI in a subprocess; make that one "installed" too.
    import sys
    installed = tmp_path / "installed-raincloud"
    installed.write_text(f"#!{sys.executable}\nimport raincloud.config as c\n"
                         "c._is_checkout = lambda root: False\n"
                         "from raincloud.cli import main\nraise SystemExit(main())\n")
    installed.chmod(0o755)
    monkeypatch.setenv("RAINCLOUD_CLI", str(installed))
    preferred.write_text('[raincloud]\n' + '\n'.join(
        f'{key}={json.dumps(options[key])}' for key in ['data_dir', 'catalog']) + '\n')
    user.write_text('[raincloud]\ncache_dir="private-cache"\noffline=true\n')
    for fmt in ['arrow', 'parquet', 'vortex']:
        ds = load('tiny', format=fmt)
        handle = native.open({}, fmt)
        try:
            assert native.metadata(handle)['catalog_revision'] == ds.catalog_revision
            assert native.string('raincloud_path', handle) == str(ds.path())
            assert ds.to_arrow().cast(table.schema).equals(table)
            assert native_table(native, handle).cast(table.schema).equals(table)
        finally:
            native.close(handle)
    assert not (user.parent / 'private-cache').exists()
