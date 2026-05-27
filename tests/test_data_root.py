# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import json
from pathlib import Path

from scripts.pipeline import spec


def test_data_root_honors_raincloud_home(monkeypatch, tmp_path):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    assert spec.data_root() == tmp_path / "home"


def test_data_root_detects_checkout(monkeypatch, tmp_path):
    monkeypatch.delenv("RAINCLOUD_HOME", raising=False)
    (tmp_path / "sources.json").write_text("{}")
    monkeypatch.setattr(spec, "REPO_ROOT", tmp_path)
    assert spec.data_root() == tmp_path


def test_data_root_falls_back_to_xdg_cache(monkeypatch, tmp_path):
    monkeypatch.delenv("RAINCLOUD_HOME", raising=False)
    monkeypatch.setattr(spec, "REPO_ROOT", tmp_path / "no-manifest-here")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    assert spec.data_root() == tmp_path / "xdg" / "raincloud"


def test_path_helpers_honor_env(monkeypatch, tmp_path):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "h"))
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(tmp_path / "out"))
    monkeypatch.setenv("RAINCLOUD_RAW_DOWNLOADS", str(tmp_path / "raw"))
    monkeypatch.setenv("RAINCLOUD_WORKDIR", str(tmp_path / "wd"))
    assert spec.outputs_base() == tmp_path / "out"
    assert spec.outputs_root({"schema_version": 1}) == tmp_path / "out" / "v1"
    assert spec.raw_downloads_root() == tmp_path / "raw"
    assert spec.workdir_root() == tmp_path / "wd"


def test_path_helpers_default_under_data_root(monkeypatch, tmp_path):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path))
    for v in ("RAINCLOUD_OUTPUTS", "RAINCLOUD_RAW_DOWNLOADS", "RAINCLOUD_WORKDIR"):
        monkeypatch.delenv(v, raising=False)
    assert spec.outputs_base() == tmp_path / "outputs"
    assert spec.raw_downloads_root() == tmp_path / "outputs" / "raw_downloads"
    assert spec.workdir_root() == tmp_path / "_workdir"


def test_display_path_relative_under_root_else_absolute(monkeypatch, tmp_path):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path))
    inside = tmp_path / "outputs" / "v1" / "x.parquet"
    assert spec.display_path(inside) == "outputs/v1/x.parquet"
    outside = Path("/var/somewhere/else.bin")
    assert spec.display_path(outside) == "/var/somewhere/else.bin"


def test_load_manifest_packaged_fallback(monkeypatch, tmp_path):
    # No env override and no checkout manifest -> _default_manifest() consults
    # the packaged copy via _packaged_data(). Patch it to a tmp fixture so the
    # fallback logic is tested without needing a built wheel.
    fake = tmp_path / "packaged_sources.json"
    fake.write_text(json.dumps({"schema_version": 1, "datasets": [{"slug": "x"}]}))
    monkeypatch.delenv("RAINCLOUD_MANIFEST", raising=False)
    monkeypatch.setattr(spec, "REPO_ROOT", tmp_path / "no-checkout")
    monkeypatch.setattr(spec, "_packaged_data",
                        lambda name: fake if name == "sources.json" else None)
    m = spec.load_manifest()
    assert m["schema_version"] == 1
    assert m["datasets"][0]["slug"] == "x"
