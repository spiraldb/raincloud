# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Recipe-scoped extraction through real CLIs, tiny local archives only."""
import bz2
import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pytest

import raincloud
from raincloud._bundle import recipe_hash
from raincloud.catalogs import current, operation
from raincloud.pipeline import build, extract, fetch
from raincloud.pipeline.spec import prepared_arrow, raw_slug_dir, recipe_workdir_root, workdir_root

ROOT = Path(__file__).resolve().parents[1]


def recipe(tmp_path, generation, slug="tiny"):
    source = tmp_path / f"source{generation}" / "payload.csv.bz2"
    source.parent.mkdir(exist_ok=True)
    source.write_bytes(bz2.compress(f"generation\n{generation}\n".encode()))
    return {"slug": slug, "fetch": {"type": "http", "urls": [source.as_uri()]},
            "extract": {"type": "bz2"}, "parse": {"reader": "csv"},
            "transform": {"handler": "identity"}, "expect": {"rows": 1},
            "export": {"formats": []}}


def manifest(tmp_path, recipes, name="catalog"):
    path = tmp_path / f"{name}.json"
    path.write_text(json.dumps({"schema_version": 2, "datasets": recipes}))
    return path


def config_for(tmp_path, path):
    return raincloud.resolve_config(no_config=True, manifest=path,
        data_dir=tmp_path / "data", raw_dir=tmp_path / "raw",
        scratch_dir=tmp_path / "scratch", catalog_dir=tmp_path / "catalogs")


def test_extract_cli_catalog_collision_repeat_and_build_cleanup(tmp_path):
    first, second = recipe(tmp_path, 1), recipe(tmp_path, 2)
    manifests = [manifest(tmp_path, [item], f"catalog{i}")
                 for i, item in enumerate((first, second))]
    config = tmp_path / "config.toml"
    # Raw bytes are kept: the test counts them across recipes.
    config.write_text('[raincloud]\ndata_dir = "data"\nscratch_dir = "scratch"\n'
                      'catalog_dir = "catalogs"\nkeep_raw = true\n')
    env = {k: v for k, v in os.environ.items() if not k.startswith("RAINCLOUD_")}
    env["RAINCLOUD_CONFIG"] = str(config)

    def run(stage, selected, *args):
        env["RAINCLOUD_MANIFEST"] = str(selected)
        return subprocess.run([sys.executable, "-m", f"raincloud.pipeline.{stage}", "tiny", *args],
                              cwd=ROOT, env=env, capture_output=True, text=True, check=True)

    paths = [tmp_path / "scratch/.recipes" / recipe_hash(item, 2, specs=None) / "tiny/payload.csv"
             for item in (first, second)]
    for selected, path, value in zip(manifests, paths, (1, 2)):
        result = run("extract", selected)
        assert str(path.parent) in result.stdout
        assert path.read_text() == f"generation\n{value}\n"
    assert not (tmp_path / "scratch/tiny").exists()
    assert len(list((tmp_path / "data/raw_downloads").rglob("payload.csv.bz2"))) == 2
    before = paths[1].stat().st_mtime_ns
    assert "[cached] payload.csv" in run("extract", manifests[1]).stdout
    assert paths[1].stat().st_mtime_ns == before

    # A build must reuse the CLI's extraction, without adding another scope.
    assert "[cached] payload.csv" in run("build", manifests[1], "--strict").stdout
    assert paths[1].stat().st_mtime_ns == before
    with pa.ipc.open_file(str(tmp_path / "data/v2/tiny/arrow/tiny.arrow.zstd")) as reader:
        assert reader.read_all().to_pydict() == {"generation": [2]}
    assert len(list((tmp_path / "scratch").rglob("payload.csv"))) == 2
    legacy = tmp_path / "scratch/tiny/keep"
    legacy.parent.mkdir()
    legacy.write_text("legacy")
    run("build", manifests[1], "--strict", "--clean-workdir")
    assert not paths[1].parent.exists()
    assert paths[0].read_text() == "generation\n1\n"
    assert legacy.read_text() == "legacy"
    assert len(list((tmp_path / "data/raw_downloads").rglob("payload.csv.bz2"))) == 2
    run("extract", manifests[1])
    assert paths[1].read_text() == "generation\n2\n"


def test_build_override_uses_effective_extraction_recipe(tmp_path):
    original, override = recipe(tmp_path, 1), recipe(tmp_path, 2)
    cfg = config_for(tmp_path, manifest(tmp_path, [original]))
    with operation(cfg):
        # Path derivation has no initialization side effects.
        original_root = recipe_workdir_root(original)
        override_root = recipe_workdir_root(override)
        assert not cfg.scratch_dir.exists()
        assert build.run_one(original, strict=True)
        old_raw = raw_slug_dir("tiny", original)
        assert build.run_one(override, strict=True, clean_workdir=True)
        assert (original_root / "tiny/payload.csv").read_text() == "generation\n1\n"
        assert not (override_root / "tiny").exists()
        assert (old_raw / "payload.csv.bz2").exists()
        assert raw_slug_dir("tiny", override) != old_raw
        assert workdir_root() == cfg.scratch_dir
        with pa.ipc.open_file(str(prepared_arrow("tiny"))) as reader:
            assert reader.read_all().to_pydict() == {"generation": [2]}


def test_extract_cli_freezes_config_catalog_and_locks_base_roots(tmp_path, monkeypatch):
    fcntl = pytest.importorskip("fcntl")
    recipes = [recipe(tmp_path, 1, "first"), recipe(tmp_path, 2, "second")]
    cfg = config_for(tmp_path, manifest(tmp_path, recipes))
    original_fetch = fetch.fetch
    seen = []
    with operation(cfg) as context:
        def guarded_fetch(spec):
            assert current() is context
            assert workdir_root() == cfg.scratch_dir / ".recipes" / recipe_hash(spec, 2, specs=None)
            for root in (cfg.raw_dir, cfg.scratch_dir):
                with (root / ".raincloud-write.lock").open("a+b") as stream:
                    with pytest.raises(BlockingIOError):
                        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            seen.append(spec["slug"])
            # Neither environment nor loose manifest changes may alter the
            # selected context midway through this multi-slug operation.
            monkeypatch.setenv("RAINCLOUD_WORKDIR", str(tmp_path / "wrong-scratch"))
            monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "missing.json"))
            cfg.manifest.write_text('{"schema_version": 2, "datasets": []}')
            return original_fetch(spec)
        monkeypatch.setattr(fetch, "fetch", guarded_fetch)
        assert extract.main(["first", "second"]) == 0
        assert workdir_root() == cfg.scratch_dir
        assert current() is context
    assert seen == ["first", "second"]
    assert not (tmp_path / "wrong-scratch").exists()
    for spec, value in zip(recipes, (1, 2)):
        path = cfg.scratch_dir / ".recipes" / recipe_hash(spec, 2, specs=None) / spec["slug"] / "payload.csv"
        assert path.read_text() == f"generation\n{value}\n"
