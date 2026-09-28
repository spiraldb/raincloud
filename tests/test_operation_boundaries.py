# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Build/read/observe operations keep their selected catalog and store.

Only scheduling is controlled: builds run in real child processes and docs
generation uses its real serializers, catalog context and filesystem paths.
"""
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

import raincloud
from raincloud._bundle import encode, make_bundle
from raincloud.catalogs import current, operation
from raincloud.config import use_config
from raincloud.exceptions import ArtifactNotFound
from raincloud.pipeline import docs


def store(root, name, rows):
    root.mkdir()
    csv = root / "input.csv"
    csv.write_text("n,label\n" + "".join(f"{i},{name}\n" for i in range(rows)))
    recipe = {"slug": "shared-name", "short_name": name, "full_name": name,
              "fetch": {"type": "http", "urls": [csv.as_uri()]},
              "extract": {"type": "passthrough"}, "parse": {"reader": "csv"},
              "transform": {"handler": "identity"}, "expect": {"rows": rows},
              "export": {"formats": ["parquet"]}}
    bundle = make_bundle(encode({"schema_version": 2, "datasets": [recipe]}),
                         encode({"schema_version": 2, "slugs": {}}), name)
    catalog = root / "catalog"
    catalog.mkdir()
    for filename, data in bundle.files().items():
        (catalog / filename).write_bytes(data)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(catalog), data_dir=root / "data",
        cache_dir=root / "unused cache", scratch_dir=root / "scratch", raw_dir=root / "raw",
        catalog_dir=root / "catalog revisions", offline=False)
    # Loading metadata is lazy; ordinary reads never start the builder.
    ds = raincloud.load("shared-name", format="parquet", config=cfg)
    assert not cfg.data_dir.exists()
    with pytest.raises(ArtifactNotFound):
        ds.to_arrow()
    assert not cfg.raw_dir.exists()
    built = raincloud.load("shared-name", format="parquet", config=cfg, build=True)
    assert built.to_arrow().column("label").to_pylist() == [name] * rows
    assert not cfg.cache_dir.exists()  # shared data is consumed in place
    return cfg, bundle


@pytest.mark.parametrize("fail_first", [False, True])
def test_concurrent_documentation_is_operation_local(tmp_path, monkeypatch, fail_first):
    a, bundle_a = store(tmp_path / "a", "catalog-a", 2)
    b, bundle_b = store(tmp_path / "b", "catalog-b", 3)
    destinations = {name: cfg.data_dir / ".raincloud/observations" / bundle.revision
                    for name, cfg, bundle in (("a", a, bundle_a), ("b", b, bundle_b))}
    # Sentinel stand-in for the installation: any escaped destination is detected.
    installation = tmp_path / "installation"
    installation.mkdir()
    for key, filename in (("DATASETS_MD", "datasets.md"), ("HANDLERS_MD", "handlers.md"),
                          ("SNAPSHOT_JSON", "snapshot.json")):
        p = installation / filename
        p.write_text("installation sentinel")
        monkeypatch.setattr(docs, key, p)
    if fail_first:
        (destinations["a"] / "snapshot.json").mkdir(parents=True)
    entered_a, entered_b, finished_a = Event(), Event(), Event()
    generate = docs._main

    def scheduled(*args, **kwargs):
        if current().bundle.catalog_id == "catalog-a":
            entered_a.set()
            assert entered_b.wait(10), "second operation did not enter"
        else:
            entered_b.set()
            assert finished_a.wait(10), "first operation did not finish"
        return generate(*args, **kwargs)

    monkeypatch.setattr(docs, "_main", scheduled)

    def run(cfg, first):
        try:
            with use_config(cfg):
                return docs.main([])
        finally:
            if first:
                finished_a.set()

    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(run, a, True)
        assert entered_a.wait(10)
        second = pool.submit(run, b, False)
        if fail_first:
            with pytest.raises(OSError):
                first.result(timeout=20)
        else:
            assert first.result(timeout=20) == 0
        assert second.result(timeout=20) == 0
    assert all(p.read_text() == "installation sentinel" for p in installation.iterdir())
    for name, cfg, bundle, rows in (("a", a, bundle_a, 2), ("b", b, bundle_b, 3)):
        dest = destinations[name]
        if name == "a" and fail_first:
            (dest / "snapshot.json").rmdir()
        # Retry and preserve observations after a reader round trip.
        with operation(cfg):
            assert raincloud.load("shared-name", config=cfg, offline=True).to_arrow().num_rows == rows
            if name == "a" and fail_first:
                assert docs.main([]) == 0
        snapshot = json.loads((dest / "snapshot.json").read_text())
        assert snapshot["catalog_id"] == bundle.catalog_id
        assert snapshot["catalog_revision"] == bundle.revision
        assert snapshot["slugs"]["shared-name"]["last_built_rows"] == rows
        assert bundle.catalog_id in (dest / "datasets.md").read_text()
        assert "shared-name" in (dest / "handlers.md").read_text()
    assert current() is None
