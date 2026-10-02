# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""A generated table is one of a group its generator writes at once: building
one builds the group, its generator output goes once the group has built
(unless keep_raw keeps it), and `--only` builds just the tables named."""
from __future__ import annotations

from dataclasses import replace

import pytest

import raincloud
from raincloud import _bundle, catalogs
from raincloud.catalogs import operation
from raincloud.pipeline import build, generate
from raincloud.pipeline.spec import prepared_artifact
from tests.test_generated import FixtureGenerator


def _spec(name):
    return {"slug": f"fixture-{name}", "fetch": {"type": "generated", "generator": "fixture", "version": "1",
            "parameters": {"size": 2}, "output": name},
            "extract": {"type": "passthrough"}, "parse": {"reader": "parquet"},
            "transform": {"handler": "identity"}}


@pytest.fixture
def group(tmp_path, monkeypatch):
    monkeypatch.delenv("RAINCLOUD_KEEP_RAW")  # conftest pins it; these tests need the default
    producer = FixtureGenerator()
    monkeypatch.setitem(generate.REGISTRY, "fixture", producer)
    caps = _bundle.capabilities()
    caps["builders"].append("generator:fixture")
    monkeypatch.setattr(catalogs, "capabilities", lambda: caps)
    specs = [_spec("left"), _spec("right")]
    bundle = _bundle.make_bundle(_bundle.encode({"schema_version": 2, "datasets": specs}),
                                 _bundle.encode({"schema_version": 2, "slugs": {}}), "groups")
    directory = tmp_path / "catalog"
    directory.mkdir()
    for filename, raw in bundle.files().items():
        (directory / filename).write_bytes(raw)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(directory), data_dir=tmp_path / "data",
                                   raw_dir=tmp_path / "raw", scratch_dir=tmp_path / "scratch",
                                   catalog_dir=tmp_path / "catalogs", formats="parquet")
    return cfg, producer, specs


def _built(cfg, name):
    with operation(cfg):
        return prepared_artifact(f"fixture-{name}", "parquet").is_file()


def test_building_one_table_builds_its_group_and_then_cleans_the_generator_output(group):
    cfg, producer, specs = group
    with operation(cfg):
        assert build._main(["fixture-left"]) == 0
        root = generate.group_root(specs[0]["fetch"])
    assert _built(cfg, "left") and _built(cfg, "right")
    assert producer.calls == 1 and not root.exists()


def test_keep_raw_keeps_the_generator_output(group):
    cfg, _, specs = group
    cfg = replace(cfg, keep_raw=True)
    with operation(cfg):
        assert build._main(["fixture-left"]) == 0
        assert generate.group_root(specs[0]["fetch"]).exists()


def test_only_builds_just_the_named_table(group):
    cfg, _, specs = group
    with operation(cfg):
        assert build._main(["fixture-left", "--only"]) == 0
        assert not generate.group_root(specs[0]["fetch"]).exists()
    assert _built(cfg, "left") and not _built(cfg, "right")
