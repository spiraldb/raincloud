# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Multi-output builds, and revision-scoped profile observations."""
import json
from dataclasses import replace

import pyarrow as pa
import pytest

import raincloud
from raincloud._bundle import encode, make_bundle
from raincloud.catalogs import current, operation
from raincloud.pipeline import build, overnight_profile, profile, promote_profiles
from raincloud.pipeline.canonical import open_canonical_writer
from raincloud.pipeline.spec import prepared_arrow, prepared_parquet


@pytest.fixture
def isolated(tmp_path, monkeypatch):
    spec = {"slug": "source", "export": {"formats": ["parquet"]}}
    child = {**spec, "slug": "child"}
    bundle = make_bundle(encode({"schema_version": 2, "datasets": [spec, child]}),
                         encode({"schema_version": 2, "slugs": {}}), "identity-test")
    directory = tmp_path / "catalog"
    directory.mkdir()
    for name, raw in bundle.files().items():
        (directory / name).write_bytes(raw)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(directory),
        data_dir=tmp_path / "data", cache_dir=tmp_path / "data", raw_dir=tmp_path / "raw",
        scratch_dir=tmp_path / "scratch", catalog_dir=tmp_path / "catalogs", offline=True)
    monkeypatch.setattr(build, "fetch", lambda spec: [])
    monkeypatch.setattr(build, "extract", lambda spec, paths: [])
    monkeypatch.setattr(build, "parse", lambda spec, paths: [])
    monkeypatch.setattr(build, "transform", lambda spec, tables: [(spec["slug"], pa.table({"x": [1]}))])
    monkeypatch.setattr(promote_profiles, "REPO_ROOT", tmp_path / "install")
    with operation(cfg):
        yield cfg, spec


@pytest.mark.parametrize("streaming", [False, True])
def test_child_output_is_built_and_loads(isolated, monkeypatch, streaming):
    cfg, spec = isolated
    def produce(*args):
        table = pa.table({"x": [2]})
        if not streaming:
            return [("child", table)]
        with open_canonical_writer("child", table.schema) as writer:
            writer.write_table(table)
        return []
    monkeypatch.setattr(build, "transform", produce)
    assert build.run_one(spec, strict=False)
    assert prepared_arrow("child").exists()
    assert raincloud.load("child", format="parquet", config=cfg).to_arrow()["x"].to_pylist() == [2]


def test_producer_cannot_claim_different_manifest_child_recipe(isolated, monkeypatch):
    _, spec = isolated
    spec = {**spec, "expect": {"rows": 1}}
    monkeypatch.setattr(build, "transform", lambda *args: [("child", pa.table({"x": [2]}))])
    assert not build.run_one(spec, strict=False)
    assert not prepared_arrow("child").exists()


@pytest.mark.parametrize("source", ["custom", "bundled"])
def test_profile_uses_revision_observations_and_reads_them(isolated, source):
    cfg, spec = isolated
    context = current()
    if source == "bundled":
        context = replace(context, source="bundled")
    global_profile = promote_profiles.REPO_ROOT / "docs/v2/profiles/source.json"
    global_profile.parent.mkdir(parents=True)
    global_profile.write_bytes(b"foreign global profile")
    with operation(cfg, context):
        assert build.run_one(spec, strict=False)
        assert profile.main(["source"]) == 0
        directory = cfg.data_dir / ".raincloud/observations" / context.bundle.revision / "profiles"
        promoted = directory / "source.json"
        assert json.loads(promoted.read_text())["row_count"] == 1
        profile._profile_path("source").unlink()
        assert list(promote_profiles.profile_candidates("source")) == [promoted]
        assert overnight_profile._profiled_slugs() == {"source"}
    assert global_profile.read_bytes() == b"foreign global profile"


def test_custom_profile_never_reads_global_profiles(isolated):
    _, _spec = isolated
    for version in (1, 2):
        path = promote_profiles.REPO_ROOT / f"docs/v{version}/profiles/source.json"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"global")
    assert list(promote_profiles.profile_candidates("source")) == []
    assert overnight_profile._profiled_slugs() == set()


def test_check_promotion_creates_no_observation_directory(isolated):
    _, spec = isolated
    assert build.run_one(spec, strict=False)
    assert profile.main(["source", "--no-promote"]) == 0
    assert promote_profiles.promote(["source"], check_only=True) == (1, 0, [])
    assert not promote_profiles.profile_observations_dir().exists()


def test_unlisted_child_output_preserves_multi_output_contract(isolated, monkeypatch):
    _, spec = isolated
    monkeypatch.setattr(build, "transform", lambda *args: [("extra", pa.table({"x": [3]}))])
    assert build.run_one(spec, strict=False)
    assert prepared_parquet("extra").exists()


def test_profiles_do_not_cross_catalog_revision(isolated):
    cfg, spec = isolated
    assert build.run_one(spec, strict=False)
    assert profile.main(["source"]) == 0
    profile._profile_path("source").unlink()
    old = current()
    manifest = old.manifest
    manifest["datasets"][0]["description"] = "new revision, identical data recipe"
    revised = replace(old, bundle=make_bundle(encode(manifest), old.bundle.snapshot, old.bundle.catalog_id))
    with operation(cfg, revised):
        assert list(promote_profiles.profile_candidates("source")) == []
        assert overnight_profile._profiled_slugs() == set()


def test_promote_holds_data_and_destination_locks(isolated, monkeypatch):
    fcntl = pytest.importorskip("fcntl")
    cfg, spec = isolated
    assert build.run_one(spec, strict=False)
    assert profile.main(["source", "--no-promote"]) == 0
    original = promote_profiles.atomic_write
    paths = [cfg.data_dir / ".raincloud-write.lock",
             promote_profiles.profile_observations_dir().parent / ".profiles-write.lock"]
    def guarded_write(path, content):
        for lockpath in paths:
            with lockpath.open("a+b") as stream:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        original(path, content)
    monkeypatch.setattr(promote_profiles, "atomic_write", guarded_write)
    assert promote_profiles.promote(["source"]) == (1, 0, [])


def test_list_and_browser_use_selected_profile_observations(isolated, monkeypatch, capsys):
    from types import SimpleNamespace

    from raincloud.pipeline import list_datasets
    cfg, spec = isolated
    assert build.run_one(spec, strict=False)
    assert profile.main(["source"]) == 0
    profile._profile_path("source").unlink()
    assert list_datasets.main(["--inspect", "source"]) == 0
    assert "rows: 1" in capsys.readouterr().out
    pytest.importorskip("textual")
    from raincloud.pipeline.browse import DatasetBrowser
    browser = SimpleNamespace(_manifest=current().manifest)
    assert DatasetBrowser._profile_for(browser, "source")["row_count"] == 1


def test_check_promotion_leaves_missing_data_root_absent(isolated):
    cfg, _ = isolated
    assert not cfg.data_dir.exists()
    assert promote_profiles.promote(check_only=True) == (0, 0, [])
    assert not cfg.data_dir.exists()


def test_check_promotion_audits_store_without_any_writes(isolated, monkeypatch):
    from pathlib import Path
    cfg, spec = isolated
    assert build.run_one(spec, strict=False)
    assert profile.main(["source", "--no-promote"]) == 0
    before = {p: p.read_bytes() for p in cfg.data_dir.rglob("*") if p.is_file()}
    original_open = Path.open
    def read_only_open(path, mode="r", *args, **kwargs):
        if path.is_relative_to(cfg.data_dir) and any(flag in mode for flag in "wax+"):
            raise PermissionError("read-only data store")
        return original_open(path, mode, *args, **kwargs)
    monkeypatch.setattr(Path, "open", read_only_open)
    assert promote_profiles.promote(["source"], check_only=True) == (1, 0, [])
    assert {p: p.read_bytes() for p in cfg.data_dir.rglob("*") if p.is_file()} == before


def test_profile_named_slug_counts_as_promoted(isolated):
    cfg, _ = isolated
    old = current()
    spec = {"slug": "profile", "export": {"formats": ["parquet"]}}
    bundle = make_bundle(encode({"schema_version": 2, "datasets": [spec]}),
                         encode({"schema_version": 2, "slugs": {}}), old.bundle.catalog_id)
    with operation(cfg, replace(old, bundle=bundle)):
        assert build.run_one(spec, strict=False)
        assert profile.main(["profile"]) == 0
        profile._profile_path("profile").unlink()
        assert overnight_profile._profiled_slugs() == {"profile"}


def test_local_profile_alone_does_not_count_as_promoted(isolated):
    _, spec = isolated
    assert build.run_one(spec, strict=False)
    assert profile.main(["source", "--no-promote"]) == 0
    assert overnight_profile._profiled_slugs() == set()
