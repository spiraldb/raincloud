# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import hashlib
import json
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import raincloud
from raincloud import catalogs
from raincloud._bundle import Bundle, encode, make_bundle, read_bundle
from raincloud.config import resolve_config
from raincloud.exceptions import BuildToolingMissing, CatalogError, OfflineMiss


def bundle(*, catalog_id="example", revision=1, metadata="", handler="identity", payload=b"data"):
    manifest = {"schema_version": 2, "datasets": [{"slug": "tiny", "description": metadata,
                "fetch": {"type": "http", "urls": [f"https://example.com/{revision}.csv"]},
                "transform": {"handler": handler}, "export": {"formats": ["parquet"]}}]}
    snapshot = {"schema_version": 2, "slugs": {"tiny": {
        "parquet_bytes": len(payload), "parquet_sha256": hashlib.sha256(payload).hexdigest()}}}
    return make_bundle(encode(manifest), encode(snapshot), catalog_id)


def upstream(root, value):
    path = root / value.revision
    path.mkdir(parents=True, exist_ok=True)
    for name, raw in value.files().items():
        (path / name).write_bytes(raw)
    (root / "latest.json").write_bytes(encode({"revision": value.revision}))
    return path


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    monkeypatch.delenv("RAINCLOUD_CACHE", raising=False)
    return resolve_config(no_config=True, data_dir=tmp_path / "data", scratch_dir=tmp_path / "scratch",
                          catalog_dir=tmp_path / "catalogs", catalog="auto")


def test_pack_update_pin_rollback_and_sticky_pin(tmp_path, cfg):
    source = tmp_path / "upstream"
    a, b = bundle(), bundle(revision=2)
    upstream(source, a)
    assert catalogs.update(cfg, source=str(source))["active"] == a.revision
    assert catalogs.resolve_context(cfg).bundle == a
    catalogs.pin(cfg, a.revision)
    upstream(source, b)
    assert catalogs.update(cfg, source=str(source))["active"] == a.revision
    assert not (cfg.catalog_dir / "revisions" / b.revision).exists()
    assert catalogs.update(cfg, source=str(source), revision=b.revision)["active"] == b.revision
    assert catalogs.rollback(cfg)["active"] == a.revision
    assert catalogs.state(cfg)["pinned"] is True
    assert catalogs.pin(replace(cfg, offline=True), b.revision)["active"] == b.revision
    catalogs.unpin(cfg)
    assert catalogs.state(cfg)["pinned"] is False
    assert read_bundle(cfg.catalog_dir / "revisions" / a.revision) == a


@pytest.mark.parametrize("failure", ["file_hash", "format", "stray_field", "reader", "shape", "snapshot_version"])
def test_bad_update_preserves_active_revision(tmp_path, cfg, failure):
    source = tmp_path / "upstream"
    a = bundle()
    upstream(source, a)
    catalogs.update(cfg, source=str(source))
    b = bundle(revision=2)
    meta = json.loads(b.metadata)
    manifest, snapshot = b.manifest, b.snapshot
    if failure == "file_hash":
        manifest += b" "
    elif failure == "format":
        meta["catalog_format"] = 100
    elif failure == "stray_field":
        # catalog_format 2 dropped the `engine` version window; an unknown field must
        # still be refused rather than silently ignored, which is what makes the format
        # bump safe to rely on.
        meta["engine"] = {"min": "9.0.0", "max_exclusive": "9.9.9"}
    elif failure == "reader":
        meta["readers"].append("future-array-layout")
    elif failure == "shape":
        doc = json.loads(manifest)
        doc["datasets"][0]["slug"] = "../escape"
        manifest = encode(doc)
        meta["files"]["sources.json"] = hashlib.sha256(manifest).hexdigest()
    else:
        snapshot = encode({"schema_version": 1, "slugs": {}})
        meta["files"]["snapshot.json"] = hashlib.sha256(snapshot).hexdigest()
    invalid = Bundle(encode(meta), manifest, snapshot)
    upstream(source, invalid)
    with pytest.raises(CatalogError):
        catalogs.update(cfg, source=str(source))
    assert catalogs.state(cfg)["active"] == a.revision
    assert not (cfg.catalog_dir / "revisions" / invalid.revision).exists()


def test_interrupted_activation_preserves_old_pointer(tmp_path, cfg, monkeypatch):
    source = tmp_path / "upstream"
    a, b = bundle(), bundle(revision=2)
    upstream(source, a)
    catalogs.update(cfg, source=str(source))
    old = (cfg.catalog_dir / "active.json").read_bytes()
    upstream(source, b)
    real = catalogs.atomic_write

    def interrupted(path, content):
        if path.name == "active.json":
            raise OSError("injected interruption before atomic replace")
        return real(path, content)

    monkeypatch.setattr(catalogs, "atomic_write", interrupted)
    with pytest.raises(OSError):
        catalogs.update(cfg, source=str(source))
    assert (cfg.catalog_dir / "active.json").read_bytes() == old
    assert catalogs.installed(cfg, a.revision) == a
    assert not list((cfg.catalog_dir / "revisions").glob(".incoming-*"))


def test_manifest_only_has_no_packaged_metadata(tmp_path, cfg):
    path = tmp_path / "sources.json"
    path.write_bytes(bundle().manifest)
    local = replace(cfg, manifest=path)
    context = catalogs.resolve_context(local)
    assert context.snapshot == {"schema_version": 2, "slugs": {}}
    assert raincloud.load("tiny", config=local).num_rows is None
    path.write_bytes(bundle(revision=2).manifest)
    assert catalogs.resolve_context(local).bundle.revision != context.bundle.revision
    assert json.loads(context.bundle.manifest)["datasets"][0]["fetch"]["urls"] == ["https://example.com/1.csv"]


def test_revisions_naming_the_same_bytes_reuse_the_file(tmp_path, cfg):
    source, mirror = tmp_path / "upstream", tmp_path / "mirror"
    table = pa.table({"value": [1, 2], "label": ["first", None]})
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    payload = sink.getvalue().to_pybytes()
    a = bundle(payload=payload)
    upstream(source, a)
    catalogs.update(cfg, source=str(source))
    artifact = mirror / "v2/tiny/parquet/tiny.parquet"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(payload)
    config = replace(cfg, mirror=mirror.as_uri())
    first = raincloud.load("tiny", format="parquet", config=config).path()
    assert raincloud.load("tiny", format="parquet", config=config).to_arrow().equals(table)
    before = first.stat().st_mtime_ns, first.read_bytes()
    meta = bundle(metadata="updated description", payload=payload)
    upstream(source, meta)
    catalogs.update(cfg, source=str(source))
    assert raincloud.load("tiny", format="parquet", config=replace(config, offline=True)).path() == first
    assert raincloud.load("tiny", format="parquet", config=replace(config, offline=True)).to_arrow().equals(table)
    assert (first.stat().st_mtime_ns, first.read_bytes()) == before
    # A recipe change whose catalog names the same bytes still names this file.
    changed = bundle(revision=2, payload=payload)
    upstream(source, changed)
    catalogs.update(cfg, source=str(source))
    assert raincloud.load("tiny", format="parquet", config=replace(config, offline=True)).path() == first


def test_catalog_packed_by_an_older_release_stays_readable(tmp_path, cfg):
    # An older packer derived fewer builder names than this release does. That
    # must not make its catalog unreadable; only a build rechecks requirements.
    fresh = bundle()
    meta = json.loads(fresh.metadata)
    meta["builders"] = []
    old = Bundle(encode(meta), fresh.manifest, fresh.snapshot)
    directory = tmp_path / "old-catalog"
    directory.mkdir()
    for name, raw in old.files().items():
        (directory / name).write_bytes(raw)
    context = catalogs.resolve_context(replace(cfg, catalog=str(directory)))
    assert context.bundle == old
    assert raincloud.describe("tiny", config=replace(cfg, catalog=str(directory)))["rows"] is None
    context.build_check(context.manifest["datasets"][0])  # identity is buildable here


def test_unsupported_builder_does_not_block_reads(tmp_path, cfg):
    source, mirror = tmp_path / "upstream", tmp_path / "mirror"
    upstream(source, bundle(handler="future-handler"))
    catalogs.update(cfg, source=str(source))
    context = catalogs.resolve_context(cfg)
    with pytest.raises(BuildToolingMissing, match="future-handler"):
        context.build_check(context.manifest["datasets"][0])
    artifact = mirror / "v2/tiny/parquet/tiny.parquet"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"data")
    assert raincloud.load("tiny", format="parquet", config=replace(cfg, mirror=mirror.as_uri())).path().read_bytes() == b"data"


def test_offline_missing_revision_fails_without_network(cfg, monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("offline update accessed network")
    monkeypatch.setattr(catalogs, "urlopen", unexpected)
    with pytest.raises(OfflineMiss):
        catalogs.update(replace(cfg, offline=True), source="https://example.com/catalog", revision="a" * 64)


def test_pack_directory_follows_latest_without_installing(tmp_path, cfg):
    # A machine config names the shared pack directory; publishing a new
    # revision rewrites latest.json and readers follow it with no per-user
    # `catalog update` and nothing written under catalog_dir.
    source = tmp_path / "upstream"
    a, b = bundle(), bundle(revision=2)
    upstream(source, a)
    shared = replace(cfg, catalog=str(source))
    assert catalogs.resolve_context(shared).bundle == a
    upstream(source, b)
    assert catalogs.resolve_context(shared).bundle == b
    assert not cfg.catalog_dir.exists()
    (source / "latest.json").write_bytes(encode({"revision": "not-a-sha"}))
    with pytest.raises(CatalogError, match="latest.json"):
        catalogs.resolve_context(shared)


def test_released_catalog_is_readable_by_other_users(tmp_path):
    # A release on a shared machine is read by every account, so its files
    # follow the umask like any created file, not mkstemp's private 0600.
    import os
    import stat
    old = os.umask(0o022)
    try:
        catalogs.release(bundle(), tmp_path / "pack")
    finally:
        os.umask(old)
    for path in [tmp_path / "pack" / "latest.json", *(tmp_path / "pack").glob("*/*")]:
        assert stat.S_IMODE(path.stat().st_mode) == 0o644, path
    for path in (tmp_path / "pack").iterdir():
        if path.is_dir():
            assert stat.S_IMODE(path.stat().st_mode) == 0o755, path


def test_recipes_of_older_catalogs_do_not_move():
    # Pins record a recipe; if a release stops hashing a key an older catalog
    # still carries, every artifact built against that catalog stops matching.
    from raincloud._bundle import digest, recipe_hash
    legacy = {"slug": "hn", "fetch": {"type": "http", "urls": ["https://example.com"]},
              "hydrate": {"url_column": "url", "output_column": "content", "output_type": "string"}}
    as_packed = digest(encode({"schema_version": 2, "recipe": legacy}))
    assert recipe_hash(legacy, 2, specs=None) == as_packed


def test_checkout_and_explicit_revision_ignore_active(tmp_path, cfg):
    source = tmp_path / "upstream"
    a, b = bundle(), bundle(revision=2)
    upstream(source, a)
    catalogs.update(cfg, source=str(source))
    upstream(source, b)
    catalogs.update(cfg, source=str(source))
    assert catalogs.resolve_context(replace(cfg, catalog=a.revision)).bundle == a
    assert catalogs.resolve_context(replace(cfg, catalog="checkout")).source == "checkout"


def test_declared_capabilities_are_all_real():
    """Every name the loader advertises must resolve to something that exists.

    `capabilities()` is now derived from `raincloud._registry`, so comparing the
    two would prove nothing. The invariant that still has teeth is the other
    direction: the declaration is what the loader promises a catalog can be
    built with, and each entry must correspond to a handler that imports, a
    generator that instantiates, and an exporter that actually registered.
    """
    from importlib import import_module

    from raincloud._bundle import capabilities
    from raincloud._registry import CUSTOM_FETCHERS, exporter_cells
    from raincloud.pipeline.export import all_exporters
    from raincloud.pipeline.generators import REGISTRY as GENERATORS
    from raincloud.pipeline.handlers import get, names

    declared = set(capabilities()["builders"])

    for name in names():
        assert callable(get(name)), f"declared handler {name!r} does not resolve"
    for name, target in CUSTOM_FETCHERS.items():
        module_name, _, attr = target.partition(":")
        module = import_module(f"raincloud.pipeline.{module_name}")
        assert callable(getattr(module, attr, None)), (
            f"declared custom fetcher {name!r} does not resolve to {target!r}")
    for name in GENERATORS:
        assert GENERATORS[name] is not None, f"declared generator {name!r} does not resolve"
    registered = {e.cell_id for e in all_exporters()}
    assert registered == set(exporter_cells()), (
        "declared exporter cells and registered ones differ: "
        f"declared-only={sorted(set(exporter_cells()) - registered)} "
        f"registered-only={sorted(registered - set(exporter_cells()))}"
    )

    # And the token list the bundle records covers exactly those three sources.
    assert declared == (
        {"fetcher:" + n for n in CUSTOM_FETCHERS}
        | {"handler:" + n for n in names()}
        | {"generator:" + n for n in GENERATORS}
        | {"exporter:" + c for c in exporter_cells()}
    )


def test_build_uses_handle_revision_after_activation(tmp_path, cfg):
    source = tmp_path / "upstream"
    first, second = tmp_path / "first.csv", tmp_path / "second.csv"
    first.write_text("a\n1\n2\n")
    second.write_text("a\n7\n8\n")

    def with_source(path):
        manifest = {"schema_version": 2, "datasets": [{"slug": "tiny", "fetch": {"type": "http", "urls": [path.as_uri()]},
                    "extract": {"type": "passthrough"}, "parse": {"reader": "csv"},
                    "transform": {"handler": "identity"}, "export": {"formats": []}}]}
        return make_bundle(encode(manifest), encode({"schema_version": 2, "slugs": {}}), "example")

    a, b = with_source(first), with_source(second)
    upstream(source, a)
    catalogs.update(cfg, source=str(source))
    old = raincloud.load("tiny", format="arrow", config=cfg, build=True)
    upstream(source, b)
    catalogs.update(cfg, source=str(source))
    assert old.to_arrow().column("a").to_pylist() == [1, 2]
    # These catalogs record no sizes, so the new recipe is rebuilt explicitly.
    old.path().unlink()
    assert raincloud.load("tiny", format="arrow", config=cfg, build=True).to_arrow().column("a").to_pylist() == [7, 8]
    assert catalogs.state(cfg)["active"] == b.revision
    # Raw sources for both fetch recipes survive in separate unversioned caches.
    assert len(list(cfg.raw_dir.glob("tiny/.recipes/*/*.csv"))) == 2
    from raincloud.pipeline.status import _raw_status
    with catalogs.operation(cfg) as context:
        raw = _raw_status(context.manifest["datasets"][0])
    assert raw["files"] == 1
    assert raw["bytes"] == second.stat().st_size


def test_refresh_over_local_http(tmp_path, cfg):
    from functools import partial
    from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    value = bundle()
    source = tmp_path / "upstream"
    upstream(source, value)
    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(SimpleHTTPRequestHandler, directory=str(source)))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{server.server_port}"
        assert catalogs.update(cfg, source=url)["active"] == value.revision
        assert raincloud.load("tiny", config=cfg)._entry.revision == value.revision
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_concurrent_activations_keep_complete_history(tmp_path, cfg):
    from concurrent.futures import ThreadPoolExecutor
    source = tmp_path / "upstream"
    versions = [bundle(revision=n) for n in (1, 2, 3)]
    for value in versions:
        upstream(source, value)
    catalogs.update(cfg, source=str(source), revision=versions[0].revision)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [pool.submit(catalogs.update, cfg, source=str(source), revision=v.revision) for v in versions[1:]]
        for result in results:
            result.result(timeout=10)
    selected = catalogs.state(cfg)
    assert selected["history"][0] == versions[0].revision
    assert len(selected["history"]) == 2
    assert set(selected["history"] + [selected["active"]]) == {v.revision for v in versions}


def test_process_lock_serializes_writers(tmp_path):
    import subprocess
    import sys

    from raincloud._locking import locked

    lock = tmp_path / "writer.lock"
    code = f'''
from pathlib import Path
from raincloud._locking import locked
print("ready", flush=True)
with locked(Path({str(lock)!r})):
    print("acquired", flush=True)
'''
    process = None
    try:
        with locked(lock):
            process = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
            assert process.stdout.readline().strip() == "ready"
            with pytest.raises(subprocess.TimeoutExpired):
                process.wait(timeout=0.1)
        out, _ = process.communicate(timeout=10)
        assert process.returncode == 0 and out.strip() == "acquired"
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()


def test_local_observations_do_not_modify_bundle(tmp_path, cfg):
    from raincloud.config import use_config
    from raincloud.pipeline import docs
    source = tmp_path / "upstream"
    value = bundle()
    upstream(source, value)
    catalogs.update(cfg, source=str(source))
    with use_config(cfg):
        assert docs.main(["snapshot"]) == 0
    assert catalogs.installed(cfg, value.revision) == value
    observation = cfg.data_dir / ".raincloud/observations" / value.revision / "snapshot.json"
    result = json.loads(observation.read_text())
    assert result["catalog_revision"] == value.revision
    assert set(result["slugs"]) == {"tiny"}
    assert result["slugs"]["tiny"]["parquet_sha256"] == hashlib.sha256(b"data").hexdigest()


def test_global_builder_requirement_is_checked_only_for_build(tmp_path, cfg):
    value = bundle()
    metadata = json.loads(value.metadata)
    metadata["builders"].append("pipeline:future")
    value = Bundle(encode(metadata), value.manifest, value.snapshot)
    source = tmp_path / "upstream"
    upstream(source, value)
    catalogs.update(cfg, source=str(source))
    context = catalogs.resolve_context(cfg)
    with pytest.raises(BuildToolingMissing, match="pipeline:future"):
        context.build_check(context.manifest["datasets"][0])


def test_offline_pin_current_starter_without_refresh(cfg):
    selected = catalogs.resolve_context(cfg)
    assert not cfg.catalog_dir.exists()
    result = catalogs.pin(replace(cfg, offline=True), selected.bundle.revision)
    assert result["active"] == selected.bundle.revision
    assert result["pinned"] is True
    assert catalogs.installed(cfg, selected.bundle.revision) == selected.bundle


def test_explicit_api_catalog_overrides_operation_context(tmp_path, cfg):
    source = tmp_path / "upstream"
    a, b = bundle(), bundle(revision=2)
    for value in (a, b):
        upstream(source, value)
        catalogs.update(cfg, source=str(source))
    with catalogs.operation(replace(cfg, catalog=a.revision)):
        old = raincloud.load("tiny")
        explicit = raincloud.load("tiny", config=replace(cfg, catalog=b.revision))
    assert old._entry.revision == a.revision
    assert explicit._entry.revision == b.revision


def test_pinning_starter_retains_legacy_data_adoption(tmp_path, cfg):
    selected = raincloud.load("uci-seeds", format="parquet", config=cfg)
    size = selected._entry.formats["parquet"].nbytes
    assert size and size < 100_000
    path = cfg.data_dir / "v2/uci-seeds/parquet/uci-seeds.parquet"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"0" * size)
    assert selected.path() == path
    catalogs.pin(cfg, selected.catalog_revision)
    assert raincloud.load("uci-seeds", format="parquet", config=replace(cfg, offline=True)).path() == path
    assert not list(path.parent.glob("*.pin"))


def test_format_1_bundle_with_engine_window_still_loads():
    """catalog_format 1 carried an `engine` version range that 2 removed.

    A bundle packed before the change must keep working: the field is accepted and
    ignored, not rejected. Without this, dropping the window would have bricked every
    catalog anyone had already packed -- which was half the reason to drop it.
    """
    b = bundle()
    meta = json.loads(b.metadata)
    meta["catalog_format"] = 1
    meta["engine"] = {"min": "0.3.0", "max_exclusive": "0.4.0"}
    Bundle(encode(meta), b.manifest, b.snapshot).validate()


def test_gc_keeps_what_rollback_can_reach(tmp_path, monkeypatch):
    """Revisions accumulate forever otherwise: one per manifest edit, ~2.8 MB each.

    Kept: the active revision plus the `keep` most recent history entries, which
    is exactly the set `rollback` can still reach.
    """
    import json

    from raincloud import catalogs
    from raincloud.config import resolve_config

    monkeypatch.setenv("RAINCLOUD_CATALOG_DIR", str(tmp_path / "catalogs"))
    cfg = resolve_config()
    revisions = cfg.catalog_dir / "revisions"
    revisions.mkdir(parents=True)
    revs = [f"{i:064x}" for i in range(1, 9)]
    for r in revs:
        (revisions / r).mkdir()
    (cfg.catalog_dir / "active.json").write_text(
        json.dumps({"active": revs[-1], "pinned": False, "history": revs[:-1]}))

    preview = catalogs.gc(cfg, keep=2, dry_run=True)
    assert len(preview["removable"]) == 5
    assert preview["removed"] == [] and preview["failed"] == {}
    assert len(list(revisions.iterdir())) == 8, "dry run must not delete"

    result = catalogs.gc(cfg, keep=2)
    assert sorted(result["kept"]) == sorted(revs[-3:])
    assert {d.name for d in revisions.iterdir()} == set(revs[-3:])
    assert json.loads((cfg.catalog_dir / "active.json").read_text())["history"] == revs[-3:-1]


def test_gc_leaves_a_pinned_catalog_alone(tmp_path, monkeypatch):
    """A pin says the revision matters; deciding otherwise is not GC's call."""
    import json

    from raincloud import catalogs
    from raincloud.config import resolve_config

    monkeypatch.setenv("RAINCLOUD_CATALOG_DIR", str(tmp_path / "catalogs"))
    cfg = resolve_config()
    revisions = cfg.catalog_dir / "revisions"
    revisions.mkdir(parents=True)
    revs = [f"{i:064x}" for i in range(1, 5)]
    for r in revs:
        (revisions / r).mkdir()
    (cfg.catalog_dir / "active.json").write_text(
        json.dumps({"active": revs[-1], "pinned": True, "history": revs[:-1]}))

    result = catalogs.gc(cfg, keep=0)
    assert result["pinned"] is True and result["removed"] == []
    assert len(list(revisions.iterdir())) == 4
