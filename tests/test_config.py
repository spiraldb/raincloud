# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import hashlib
import json
from pathlib import Path

import pytest

from raincloud import load, resolve_config
from raincloud._catalog import Entry, FormatInfo
from raincloud._resolve import resolve
from raincloud.cli import main
from raincloud.config import use_config
from raincloud.pipeline import spec


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    import os
    for name in tuple(os.environ):
        if name.startswith("RAINCLOUD_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("RAINCLOUD_NO_CONFIG", "1")


def settings(tmp_path, monkeypatch, text):
    path = tmp_path / "settings" / "config.toml"
    path.parent.mkdir()
    path.write_text("[raincloud]\n" + text)
    monkeypatch.delenv("RAINCLOUD_NO_CONFIG")
    return path


def test_precedence_and_relative_paths(tmp_path, monkeypatch):
    path = settings(tmp_path, monkeypatch, 'data_dir = "../hdd"\nscratch_dir = "scratch"\n')
    cfg = resolve_config(config=path)
    assert cfg.data_dir == tmp_path / "hdd"
    assert cfg.scratch_dir == path.parent / "scratch"
    assert cfg.cache_dir == cfg.data_dir
    assert cfg.raw_dir == cfg.data_dir / "raw_downloads"
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "legacy"))
    assert resolve_config(config=path).data_dir == tmp_path / "legacy/outputs"
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(tmp_path / "env"))
    assert resolve_config(config=path).data_dir == tmp_path / "env"
    cfg = resolve_config(config=path, data_dir="explicit")
    assert cfg.data_dir == Path.cwd() / "explicit"
    assert dict(cfg.origins)["data_dir"] == "explicit"


def test_file_selection_and_ignore(tmp_path, monkeypatch):
    path = settings(tmp_path, monkeypatch, 'data_dir = "disk"\n')
    monkeypatch.setenv("RAINCLOUD_CONFIG", str(path))
    assert resolve_config().data_dir == path.parent / "disk"
    assert resolve_config(no_config=True).file is None
    path.unlink()
    with pytest.raises(ValueError, match="cannot read"):
        resolve_config()
    monkeypatch.setenv("RAINCLOUD_NO_CONFIG", "1")
    assert resolve_config().file is None


@pytest.mark.parametrize("text", ['data_dir = 2', 'offline = "false"', 'typo = "oops"', 'data_dir = ""', 'mirror = [1]', 'broken = ['])
def test_invalid_config_fails(tmp_path, monkeypatch, text):
    path = settings(tmp_path, monkeypatch, text)
    with pytest.raises(ValueError):
        resolve_config(config=path)


def test_defaults_never_create_directories(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    cfg = resolve_config(repo_root=tmp_path / "not-a-checkout")
    assert cfg.data_dir == tmp_path / "data/raincloud"
    assert cfg.scratch_dir == tmp_path / "cache/raincloud/workdir"
    assert list(tmp_path.iterdir()) == []


def test_shared_pipeline_paths_and_frozen_environment(tmp_path, monkeypatch):
    cfg = resolve_config(data_dir=tmp_path / "hdd", scratch_dir=tmp_path / "ssd")
    with use_config(cfg):
        monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(tmp_path / "changed"))
        assert spec.outputs_base() == cfg.data_dir
        assert spec.workdir_root() == cfg.scratch_dir
        assert spec.raw_downloads_root() == cfg.raw_dir
    child = cfg.subprocess_env()
    assert child["RAINCLOUD_OUTPUTS"] == str(cfg.data_dir)
    assert child["RAINCLOUD_NO_CONFIG"] == "1"
    assert child["RAINCLOUD_MIRROR"] == ""


def test_read_only_local_store_needs_no_writes(tmp_path, monkeypatch):
    cfg = resolve_config(data_dir=tmp_path / "disk", cache_dir=tmp_path / "cache", offline=True)
    artifact = cfg.data_dir / "v2/tiny/parquet/tiny.parquet"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"payload")
    entry = Entry("tiny", 1, formats={"parquet": FormatInfo(hashlib.sha256(b"payload").hexdigest(), 7)}, version=2)
    artifact.chmod(0o444)
    artifact.parent.chmod(0o555)
    try:
        assert resolve("tiny", "parquet", config=cfg, entry=entry) == artifact
        assert not cfg.cache_dir.exists()
        assert list(artifact.parent.iterdir()) == [artifact]
    finally:
        artifact.parent.chmod(0o755)


def test_lazy_handle_keeps_config_and_catalog(tmp_path, monkeypatch):
    manifest, snapshot = tmp_path / "sources.json", tmp_path / "snapshot.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": [{"slug": "tiny"}]}))
    snapshot.write_text(json.dumps({"schema_version": 2, "slugs": {"tiny": {"parquet_bytes": 2, "parquet_sha256": hashlib.sha256(b"ok").hexdigest()}}}))
    cfg = resolve_config(data_dir=tmp_path / "hdd", manifest=manifest, snapshot=snapshot, offline=True)
    handle = load("tiny", format="parquet", config=cfg)
    artifact = cfg.data_dir / "v2/tiny/parquet/tiny.parquet"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"ok")
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(tmp_path / "elsewhere"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "missing"))
    assert handle.path() == artifact


def test_init_idempotence_and_show_redaction(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("RAINCLOUD_NO_CONFIG")
    config = tmp_path / "config.toml"
    cmd = ["--config", str(config), "init", "--data-dir", str(tmp_path / "hdd"),
           "--mirror", "https://user:secret@example.com/path?token=private#fragment"]
    assert main(cmd) == 0
    first = config.stat().st_mtime_ns
    assert main(cmd) == 0
    assert config.stat().st_mtime_ns == first
    assert main(["--config", str(config), "init", "--data-dir", str(tmp_path / "new")]) == 1
    assert config.stat().st_mtime_ns == first
    capsys.readouterr()
    assert main(["--config", str(config), "config", "show"]) == 0
    output = capsys.readouterr().out
    assert "secret" not in output and "private" not in output and "fragment" not in output
    assert "https://example.com/path" in output
    assert not (tmp_path / "hdd").exists()
    assert main(["--config", str(config), "init", "--force", "--data-dir", str(tmp_path / "new")]) == 0
    assert resolve_config(config=config).data_dir == tmp_path / "new"


def test_json_protocol_for_native_readers(tmp_path, capsys):
    # Native readers pass their options verbatim as --settings and read stdout.
    pytest.importorskip("vortex")
    from tests.reader_fixture import create
    _, options = create(tmp_path)
    settings = ["--json", "--settings", json.dumps(options)]

    def run(*args):
        code = main([*settings, *args])
        return code, json.loads(capsys.readouterr().out)

    code, about = run("describe", "tiny", "--format", "parquet")
    assert code == 0
    assert (about["format"], about["rows"], about["catalog_id"]) == ("parquet", 8, "reader-fixture")
    assert about["formats"]["parquet"]["writer"] == "py"
    assert about["catalog_source"] == options["catalog"]
    assert not (tmp_path / "cache").exists()  # describe never touches artifacts
    code, loaded = run("load", "tiny", "--format", "arrow")
    assert code == 0 and Path(loaded["path"]).is_file() and loaded["format"] == "arrow"
    for args, kind in [(("describe", "nope"), "UnknownSlug"),
                       (("describe", "tiny", "--format", "parquet@rs"), "FormatUnavailable")]:
        code, reply = run(*args)
        assert code == 1 and reply["error"]["type"] == kind
    code = main(["--json", "--settings", json.dumps({**options, "data_dir": str(tmp_path / "gone")}),
                 "load", "tiny", "--format", "arrow"])
    assert code == 1 and json.loads(capsys.readouterr().out)["error"]["type"] == "OfflineMiss"
    code = main(["--json", "--settings", json.dumps({"no_config": True, "bogus": 1}), "describe", "tiny"])
    assert code == 1 and json.loads(capsys.readouterr().out)["error"]["type"] == "ValueError"


def test_every_everyday_command_does_something_useful(tmp_path, capsys):
    # Project rule: things a person would type do not error. A
    # dataset or command that does not exist fails, but with a suggestion.
    pytest.importorskip("vortex")
    from tests.reader_fixture import create
    _, options = create(tmp_path)
    settings = ["--settings", json.dumps(options)]
    ok = [[], ["help"], ["help", "list"], ["help", "describe"], ["help", "ls"], ["version"],
          ["list"], ["ls"], ["search", "tiny"], ["list", "nothing-like-this"], ["list", "--long"],
          ["describe", "tiny"], ["info", "tiny"], ["show", "tiny"],
          ["load", "tiny", "--format", "arrow"], ["path", "tiny", "--format", "arrow"],
          ["config"], ["config", "show"], ["catalog"], ["catalog", "status"], ["capabilities"]]
    for args in ok:
        try:
            code = main([*settings, *args])
        except SystemExit as exc:  # argparse --help paths exit 0
            code = exc.code
        out = capsys.readouterr()
        assert code == 0, (args, out.err)
        assert "usage:" not in out.err, (args, out.err)
    assert "Native reader contract fixture" in (main([*settings, "describe", "tiny"]), capsys.readouterr().out)[1]
    assert main([*settings, "describe", "tinny"]) == 1
    assert "Did you mean tiny?" in capsys.readouterr().err
    for typo, want in [("lst", "list"), ("desribe", "describe")]:
        with pytest.raises(SystemExit) as exc:
            main([*settings, typo])
        err = capsys.readouterr().err
        assert exc.value.code == 2 and f"Did you mean {want}" in err and "usage:" not in err, err


def test_a_checkout_ignores_the_machine_config(tmp_path, monkeypatch):
    # On a machine whose config names the shared catalog and store, a checkout
    # still builds against its own manifest into its own data directory.
    machine = tmp_path / "xdg" / "raincloud" / "config.toml"
    machine.parent.mkdir(parents=True)
    machine.write_text(f'[raincloud]\ndata_dir = "{tmp_path / "shared"}"\ncatalog = "{tmp_path / "shared-catalogs"}"\n')
    monkeypatch.delenv("RAINCLOUD_NO_CONFIG")
    monkeypatch.setenv("XDG_CONFIG_DIRS", str(tmp_path / "xdg"))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "user"))
    checkout = resolve_config()
    assert checkout.data_dir != tmp_path / "shared" and checkout.catalog == "auto"
    import raincloud.config
    monkeypatch.setattr(raincloud.config, "_is_checkout", lambda root: False)
    installed = resolve_config()
    assert installed.data_dir == tmp_path / "shared"
    assert installed.catalog == str(tmp_path / "shared-catalogs")
