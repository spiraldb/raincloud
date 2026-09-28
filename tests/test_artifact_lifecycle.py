# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Tiny isolated ownership, publication and maintenance regressions."""
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import raincloud
from raincloud._bundle import encode, make_bundle
from raincloud.catalogs import operation
from raincloud.pipeline import build, compliance, convert, hydrate, overnight_profile, publish
from raincloud.pipeline.lifecycle import entry_for, operation_lock
from raincloud.pipeline.spec import prepared_parquet, prepared_vortex


@pytest.fixture
def artifacts(tmp_path, monkeypatch):
    spec = {"slug": "tiny", "export": {"formats": ["parquet", "vortex"]}}
    hydrated = {"slug": "tiny-hydrated", "advisory": "a test fixture's pages",
                "derive": {"from": "tiny", "hydrate": {"columns": {"url": {"into": "content", "type": "binary"}}}}}
    bundle = make_bundle(encode({"schema_version": 2, "datasets": [spec, hydrated]}),
                         encode({"schema_version": 2, "slugs": {"tiny": {}}}), "lifecycle-test")
    directory = tmp_path / "catalog"
    directory.mkdir()
    for name, raw in bundle.files().items():
        (directory / name).write_bytes(raw)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(directory),
            data_dir=tmp_path / "data", cache_dir=tmp_path / "data",
            raw_dir=tmp_path / "raw", scratch_dir=tmp_path / "scratch",
            catalog_dir=tmp_path / "catalogs", offline=True)
    table = pa.table({"x": [1, 2], "url": ["https://example.test/a", None]})
    monkeypatch.setattr(build, "fetch", lambda spec: [])
    monkeypatch.setattr(build, "extract", lambda spec, paths: [])
    monkeypatch.setattr(build, "parse", lambda spec, paths: [])
    monkeypatch.setattr(build, "transform", lambda spec, tables: [(spec["slug"], table)])
    with operation(cfg):
        assert build.run_one(spec, strict=False)
        yield cfg, spec


def test_compliance_reencode_leaves_the_dataset_file_alone(artifacts):
    # Compliance measures writers in scratch; the file readers are served is
    # the dataset's one Parquet file and stays as the build left it.
    cfg, spec = artifacts
    before = prepared_parquet("tiny").stat().st_mtime_ns
    report = compliance.run_compliance(spec, cells=["parquet@py"], reader_ids=["parquet@py"], reencode=True)
    assert report.read_results[0].verdict.status == "pass"
    assert report.write_results[0].out_path != prepared_parquet("tiny")
    assert prepared_parquet("tiny").stat().st_mtime_ns == before
    assert raincloud.load("tiny", format="parquet", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def test_publish_checks_actual_stream_before_promoting(artifacts, tmp_path, monkeypatch):
    _, spec = artifacts
    original = publish._upload
    mirror = tmp_path / "mirror"
    def replace_before_open(local, target, **kwargs):
        if local.suffix == ".parquet":
            replacement = local.with_suffix(".replacement")
            pq.write_table(pa.table({"x": [77]}), replacement)
            replacement.replace(local)
        return original(local, target, **kwargs)
    monkeypatch.setattr(publish, "_upload", replace_before_open)
    with pytest.raises(publish.PublishMismatch, match="changed during publication"):
        publish.main([spec["slug"], "--mirror", mirror.as_uri()])
    assert not (mirror / "v2/tiny/parquet/tiny.parquet").exists()
    assert not list(mirror.rglob("*.part"))


def test_v2_convert_reads_canonical(artifacts):
    cfg, spec = artifacts
    # Even a divergent parquet must not become the source of a v2 export.
    pq.write_table(pa.table({"x": [77]}), prepared_parquet("tiny"))
    prepared_vortex("tiny").unlink()
    convert.convert(spec)
    assert raincloud.load("tiny", format="vortex", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def _hydrated(cfg):
    from raincloud._catalog import load_catalog
    return next(d for d in load_catalog(cfg).context.manifest["datasets"] if d["slug"] == "tiny-hydrated")


def test_hydrated_dataset_builds_from_its_parent_and_warns_on_load(artifacts, monkeypatch):
    cfg, _ = artifacts
    fetched = []

    def fake_fetch(url, config):
        fetched.append(url)
        return b"page", {"http_status": 200, "content_type": "text/html", "fetched_at": None,
                         "sha256": b"", "bytes_total": 4, "filter_decision": "allowed", "error": None}
    monkeypatch.setattr(hydrate, "http_fetch", fake_fetch)
    assert build.run_one(_hydrated(cfg), strict=False)
    assert fetched == ["https://example.test/a"]  # the null URL is never fetched
    for fmt in ("arrow", "parquet", "vortex"):
        with pytest.warns(raincloud.HydratedDatasetWarning, match="You probably want 'tiny'"):
            table = raincloud.load("tiny-hydrated", format=fmt, config=cfg).to_arrow()
        assert table["x"].to_pylist() == [1, 2]
        assert table["content"].to_pylist() == [b"page", None]
        assert table["_content_provenance"].to_pylist()[1]["filter_decision"] != "allowed"
    about = raincloud.describe("tiny-hydrated", config=cfg)
    assert about["derived_from"] == "tiny" and about["advisory"] == "a test fixture's pages"


def test_build_all_skips_hydrated_datasets(artifacts, monkeypatch, capsys):
    cfg, _ = artifacts
    built = []
    monkeypatch.setattr(build, "run_one", lambda spec, **kw: built.append(spec["slug"]) or True)
    monkeypatch.setattr("sys.argv", ["build", "--all"])
    assert build._main() == 0
    assert built == ["tiny"]
    assert "build them by name" in capsys.readouterr().err


def test_cleanup_removes_only_selected_recipe_scratch(artifacts):
    cfg, spec = artifacts
    from raincloud._bundle import recipe_hash
    fingerprint = recipe_hash(spec, 2, specs=None)
    chosen = cfg.scratch_dir / ".recipes" / fingerprint / "tiny"
    other = cfg.scratch_dir / ".recipes" / "different" / "tiny"
    chosen.mkdir(parents=True)
    other.mkdir(parents=True)
    overnight_profile._wipe_slug("tiny")
    assert not chosen.exists()
    assert other.exists()


def test_nested_maintenance_reuses_lock_and_frozen_context(artifacts, monkeypatch):
    cfg, spec = artifacts
    with operation_lock(resources=True) as original:
        monkeypatch.setenv("RAINCLOUD_CATALOG", "nonexistent")
        with operation_lock() as nested:
            assert nested is original
            assert entry_for(spec).catalog_id == "lifecycle-test"

@pytest.mark.parametrize("exports, expected", [
    (None, True), ({"formats": []}, False),
    ({"formats": ["vortex"], "priority": ["rs"]}, False),
    ({"formats": ["vortex"]}, True),
    ({"formats": ["parquet"], "notes": "opted out"}, False),
])
def test_v2_convert_export_policy(artifacts, exports, expected, monkeypatch):
    # In schema_version 2, export.formats (with its writer priority) is the
    # only declaration of whether this stage's writer, vortex@py, runs.
    _, original = artifacts
    spec = {"slug": original["slug"]}
    if exports is not None:
        spec["export"] = exports
    assert convert.vortex_enabled(spec) is expected
    if not expected:
        monkeypatch.setattr(convert, "require_source", lambda *a: pytest.fail("disabled export read source"))
        assert convert.convert(spec) is None


def test_publish_holds_store_lock_while_streaming(artifacts, tmp_path, monkeypatch):
    fcntl = pytest.importorskip("fcntl")
    cfg, spec = artifacts
    original = publish._upload
    calls = []
    def checked_upload(local, target, **kwargs):
        with (cfg.data_dir / ".raincloud-write.lock").open("a+b") as stream:
            with pytest.raises(BlockingIOError):
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        calls.append(local)
        return original(local, target, **kwargs)
    monkeypatch.setattr(publish, "_upload", checked_upload)
    assert publish.main([spec["slug"], "--mirror", (tmp_path / "mirror").as_uri()]) == 0
    assert calls


def test_cleanup_holds_all_resource_locks(artifacts, monkeypatch):
    fcntl = pytest.importorskip("fcntl")
    cfg, _ = artifacts
    original = overnight_profile.shutil.rmtree
    def checked_remove(path, *args, **kwargs):
        for root in (cfg.data_dir, cfg.raw_dir, cfg.scratch_dir):
            with (root / ".raincloud-write.lock").open("a+b") as stream:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return original(path, *args, **kwargs)
    monkeypatch.setattr(overnight_profile.shutil, "rmtree", checked_remove)
    overnight_profile._wipe_slug("tiny")
    assert not prepared_parquet("tiny").exists()


@pytest.mark.parametrize("legacy_first", [False, True])
def test_cleanup_preserves_other_catalog_raw_generations(tmp_path, legacy_first):
    from dataclasses import replace

    from raincloud._bundle import digest
    from raincloud.catalogs import resolve_context
    from raincloud.pipeline.spec import raw_slug_dir

    spec = {"slug": "tiny", "fetch": {"type": "http", "urls": ["https://example.test/data"]}}
    configs = []
    for name in ("owner-a", "owner-b"):
        bundle = make_bundle(encode({"schema_version": 2, "datasets": [spec]}),
                             encode({"schema_version": 2, "slugs": {"tiny": {}}}), name)
        directory = tmp_path / name / "catalog"
        directory.mkdir(parents=True)
        for filename, raw in bundle.files().items():
            (directory / filename).write_bytes(raw)
        configs.append(raincloud.resolve_config(no_config=True, catalog=str(directory),
            data_dir=tmp_path / name / "data", raw_dir=tmp_path / "shared-raw",
            scratch_dir=tmp_path / name / "scratch", catalog_dir=tmp_path / name / "catalogs"))
    first, second = configs
    context = resolve_context(first)
    if legacy_first:
        context = replace(context, legacy=True)
    with operation(first, context):
        own = raw_slug_dir("tiny")
        own.mkdir(parents=True)
        (own / "payload").write_bytes(b"owner-a")
        # Fetch stamps the legacy root once it has been used. Its marker must
        # be reclaimed along with direct files, without deleting .recipes.
        if legacy_first:
            key = digest(encode({"catalog_id": "owner-a", "fetch": spec["fetch"]}))
            (own / ".fetch-recipe.json").write_bytes(encode({"fetch_recipe": key}))
    with operation(second):
        other = raw_slug_dir("tiny")
        other.mkdir(parents=True)
        (other / "payload").write_bytes(b"owner-b")
    assert own != other
    with operation(first, context):
        overnight_profile._wipe_slug("tiny")
    assert not (own / "payload").exists()
    assert not (own / ".fetch-recipe.json").exists()
    assert (other / "payload").read_bytes() == b"owner-b"
    with operation(second):
        assert raw_slug_dir("tiny") == other


def test_cleanup_unlinks_legacy_raw_symlink_without_visiting_target(artifacts, tmp_path):
    from dataclasses import replace

    from raincloud.catalogs import current

    cfg, _ = artifacts
    target = tmp_path / "elsewhere"
    target.mkdir()
    (target / "payload").write_bytes(b"keep")
    (target / ".recipes").mkdir()
    (target / ".recipes/another-owner").write_bytes(b"also keep")
    link = cfg.raw_dir / "tiny"
    link.symlink_to(target, target_is_directory=True)
    with operation(cfg, replace(current(), legacy=True)):
        overnight_profile._wipe_slug("tiny")
    assert not link.is_symlink()
    assert (target / "payload").read_bytes() == b"keep"
    assert (target / ".recipes/another-owner").read_bytes() == b"also keep"
