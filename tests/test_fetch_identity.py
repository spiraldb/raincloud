# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Raw recipe isolation and payload filtering, using fake APIs and tiny files."""
from __future__ import annotations

import copy
import io
import json
import sys
import types
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest

import raincloud
from raincloud._bundle import digest, encode
from raincloud.catalogs import operation, resolve_context
from raincloud.config import use_config
from raincloud.pipeline import build, fetch
from raincloud.pipeline.spec import prepared_arrow, raw_slug_dir


def config_for(tmp_path, recipes, name="catalog"):
    manifest = tmp_path / f"{name}.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": recipes}))
    return raincloud.resolve_config(no_config=True, manifest=manifest,
        data_dir=tmp_path / "data", raw_dir=tmp_path / "raw",
        scratch_dir=tmp_path / "scratch", catalog_dir=tmp_path / "catalogs")


def recipe(kind="http", url="https://example.test/rows.csv", slug="tiny"):
    return {"slug": slug, "fetch": {"type": kind, "urls": [url]},
        "extract": {"type": "passthrough"}, "parse": {"reader": "csv"},
        "transform": {"handler": "identity"}, "expect": {"rows": 1},
        "export": {"formats": []}}


def test_kaggle_metadata_is_not_a_completed_download(tmp_path, monkeypatch):
    spec = recipe("kaggle", "https://www.kaggle.com/datasets/owner/tiny")
    cfg = config_for(tmp_path, [spec])
    calls = []
    class Api:
        def authenticate(self):
            pass
        def dataset_download_files(self, ref, *, path, **kwargs):
            calls.append(ref)
            (Path(path) / "tiny.zip").write_bytes(b"payload")
    monkeypatch.setitem(sys.modules, "kaggle", types.SimpleNamespace(KaggleApi=Api))
    with operation(cfg):
        target = fetch.slug_dir("tiny")
        (target / ".recipes" / "other").mkdir(parents=True)
        (target / ".recipes" / "other" / "foreign.zip").write_bytes(b"foreign")
        paths = fetch.fetch(spec)
        assert calls == ["owner/tiny"]
        assert paths == [target / "tiny.zip"]
        assert fetch.fetch(spec) == paths
        assert calls == ["owner/tiny"]


def test_a_cached_kaggle_payload_needs_no_credentials(tmp_path, monkeypatch):
    spec = recipe("kaggle", "https://www.kaggle.com/datasets/owner/tiny")
    cfg = config_for(tmp_path, [spec])
    # The real package authenticates on import, so a cached fetch must not import it.
    class Refuse(types.ModuleType):
        def __getattr__(self, name):
            raise AssertionError("imported kaggle for a payload already on disk")
    monkeypatch.setitem(sys.modules, "kaggle", Refuse("kaggle"))
    with operation(cfg):
        target = fetch.slug_dir("tiny")
        target.mkdir(parents=True, exist_ok=True)
        (target / "tiny.zip").write_bytes(b"payload")
        assert fetch.fetch(spec) == [target / "tiny.zip"]


def test_huggingface_returns_only_payload_files(tmp_path, monkeypatch):
    spec = recipe("huggingface", "hf://owner/tiny")
    cfg = config_for(tmp_path, [spec])
    def download(repo_id, *, local_dir, **kwargs):
        target = Path(local_dir)
        for name in ("data/rows.csv", ".cache/huggingface/download/rows.metadata",
                     ".recipes/other/foreign.csv"):
            path = target / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"x\n1\n")
    monkeypatch.setitem(sys.modules, "huggingface_hub", types.SimpleNamespace(snapshot_download=download))
    monkeypatch.setitem(sys.modules, "huggingface_hub.errors", types.SimpleNamespace(GatedRepoError=type("GatedRepoError", (Exception,), {})))
    with operation(cfg):
        assert fetch.fetch(spec) == [raw_slug_dir("tiny") / "data/rows.csv"]


@pytest.mark.parametrize("legacy", [False, True])
def test_build_override_fetches_actual_recipe(tmp_path, legacy):
    for directory, value in (("first", 1), ("second", 2)):
        (tmp_path / directory).mkdir()
        (tmp_path / directory / "rows.csv").write_text(f"x\n{value}\n")
    original = recipe(url=(tmp_path / "first/rows.csv").as_uri())
    changed = copy.deepcopy(original)
    changed["fetch"]["urls"] = [(tmp_path / "second/rows.csv").as_uri()]
    cfg = config_for(tmp_path, [original])
    context = replace(resolve_context(cfg), legacy=legacy)
    with operation(cfg, context):
        assert build.run_one(original, strict=True)
        original_dir = raw_slug_dir("tiny")
        assert build.run_one(changed, strict=True)
        with pa.ipc.open_file(str(prepared_arrow("tiny"))) as reader:
            assert reader.read_all().to_pydict() == {"x": [2]}
        assert (original_dir / "rows.csv").read_text() == "x\n1\n"
        changed_dir = raw_slug_dir("tiny", changed)
        assert json.loads((changed_dir / ".fetch-recipe.json").read_text()) == {
            "fetch_recipe": digest(encode({"catalog_id": context.bundle.catalog_id, "fetch": changed["fetch"]}))}


def test_unmarked_legacy_cache_cannot_satisfy_override(tmp_path, monkeypatch):
    original = recipe()
    changed = recipe(url="https://different.test/rows.csv")
    cfg = config_for(tmp_path, [original])
    context = replace(resolve_context(cfg), legacy=True)
    with operation(cfg, context):
        old_dir = raw_slug_dir("tiny")
        old_dir.mkdir(parents=True)
        (old_dir / "rows.csv").write_bytes(b"old")
        monkeypatch.setattr(fetch.urllib.request, "urlopen", lambda *a, **k: io.BytesIO(b"new"))
        paths = fetch.fetch(changed)
        assert paths[0].read_bytes() == b"new"
        assert (old_dir / "rows.csv").read_bytes() == b"old"
        assert not (old_dir / ".fetch-recipe.json").exists()


def test_custom_fetch_uses_override_recipe(tmp_path, monkeypatch):
    original = recipe("custom")
    original["fetch"]["notes"] = "public_bi_fetch"
    original["transform"] = {"params": {"workload": "Tiny"}}
    changed = copy.deepcopy(original)
    changed["fetch"]["urls"] = ["https://different.test/rows.csv"]
    cfg = config_for(tmp_path, [original])
    # public_bi_fetch downloads through fetch.fetch_url: the partition list
    # names one partition, and every other URL answers b"new".
    def urlopen(req, **kwargs):
        url = getattr(req, "full_url", req)
        return io.BytesIO(b"https://example.test/Tiny_1.csv.bz2\n"
                          if url.endswith("/data-urls.txt") else b"new")
    monkeypatch.setattr(fetch.urllib.request, "urlopen", urlopen)
    with operation(cfg):
        old_dir = fetch.slug_dir("tiny")
        (old_dir / "Tiny_1.csv.bz2").write_bytes(b"old")
        paths = fetch.fetch(changed)
        assert paths[0].read_bytes() == b"new"
        assert all(p.parent != old_dir for p in paths)
        assert (old_dir / "Tiny_1.csv.bz2").read_bytes() == b"old"


def test_sibling_lookup_follows_catalog_switch_without_operation(tmp_path, monkeypatch):
    donor_a = recipe(url="https://a.test/rows.csv", slug="donor")
    donor_b = recipe(url="https://b.test/rows.csv", slug="donor")
    first = config_for(tmp_path, [donor_a], "first")
    second = config_for(tmp_path, [donor_b], "second")
    for cfg, contents in ((first, b"A"), (second, b"B")):
        with operation(cfg):
            directory = fetch.slug_dir("donor")
            (directory / "rows.csv").write_bytes(contents)
    # Exercise the public standalone fetch path, outside a frozen operation.
    with use_config(first):
        assert fetch._find_sibling_cache(tmp_path / "target", donor_a["fetch"]["urls"][0], "rows.csv", None, None).read_bytes() == b"A"
    with use_config(second):
        assert fetch._find_sibling_cache(tmp_path / "target", donor_a["fetch"]["urls"][0], "rows.csv", None, None) is None
        monkeypatch.setattr(fetch.urllib.request, "urlopen", lambda *a, **k: io.BytesIO(b"A fresh"))
        target = recipe(url=donor_a["fetch"]["urls"][0], slug="target")
        assert fetch.fetch(target)[0].read_bytes() == b"A fresh"


def test_exact_url_sibling_reuse_remains_available(tmp_path, monkeypatch):
    donor = recipe(slug="donor")
    target = recipe(slug="target")
    cfg = config_for(tmp_path, [donor, target])
    with operation(cfg):
        source = fetch.slug_dir("donor") / "rows.csv"
        source.write_bytes(b"x\n1\n")
        def unexpected_download(*args, **kwargs):
            pytest.fail("matching sibling should avoid downloading")
        monkeypatch.setattr(fetch.urllib.request, "urlopen", unexpected_download)
        paths = fetch.fetch(target)
        assert paths == [raw_slug_dir("target") / "rows.csv"]
        assert paths[0].read_bytes() == source.read_bytes()
