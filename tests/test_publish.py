# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import hashlib
import json

import pytest


@pytest.fixture(autouse=True)
def _store_root(tmp_path, monkeypatch):
    """Publish takes the data store's write lock; keep that store under tmp_path."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))


def test_plan_uploads_matching_slug(tmp_path):
    from raincloud.pipeline import publish
    art = tmp_path / "outputs" / "v1" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    sha = hashlib.sha256(b"data").hexdigest()
    snapshot = {"slugs": {"tiny": {"parquet_bytes": 4, "parquet_sha256": sha,
                                   "vortex_bytes": None, "vortex_sha256": None}}}
    plan = publish.plan_uploads(["tiny"], snapshot,
                                outputs_root=tmp_path / "outputs")
    assert plan == [(art, "v1/tiny/parquet/tiny.parquet")]


def test_plan_refuses_on_mismatch(tmp_path):
    from raincloud.pipeline import publish
    art = tmp_path / "outputs" / "v1" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    snapshot = {"slugs": {"tiny": {"parquet_bytes": 4, "parquet_sha256": "WRONG",
                                   "vortex_bytes": None, "vortex_sha256": None}}}
    with pytest.raises(publish.PublishMismatch) as ei:
        publish.plan_uploads(["tiny"], snapshot, outputs_root=tmp_path / "outputs")
    # The remediation must point at --rehash, since a plain
    # `docs snapshot` regen reuses the stale sha on a size match and re-fails.
    assert "--rehash" in str(ei.value)


def test_plan_includes_vortex_and_skips_absent(tmp_path):
    """Both formats are planned when present; an absent format is skipped, not
    errored. A None snapshot sha is published ungated (freshly built slug)."""
    from raincloud.pipeline import publish
    root = tmp_path / "outputs"
    pqf = root / "v1" / "tiny" / "parquet" / "tiny.parquet"
    vxf = root / "v1" / "tiny" / "vortex" / "tiny.vortex"
    pqf.parent.mkdir(parents=True); pqf.write_bytes(b"P")
    vxf.parent.mkdir(parents=True); vxf.write_bytes(b"V")
    snapshot = {"slugs": {"tiny": {
        "parquet_bytes": 1, "parquet_sha256": hashlib.sha256(b"P").hexdigest(),
        "vortex_bytes": None, "vortex_sha256": None}}}  # vortex sha unknown -> ungated
    plan = publish.plan_uploads(["tiny"], snapshot, outputs_root=root)
    keys = {k for _, k in plan}
    assert keys == {"v1/tiny/parquet/tiny.parquet", "v1/tiny/vortex/tiny.vortex"}


def test_plan_includes_arrow_when_present(tmp_path):
    """An arrow artifact on disk with a matching snapshot sha is planned for
    upload, keyed with the arrow extension (.arrow.zstd) via artifact_key."""
    from raincloud.pipeline import publish
    root = tmp_path / "outputs"
    arrow = root / "v1" / "tiny" / "arrow" / "tiny.arrow.zstd"
    arrow.parent.mkdir(parents=True); arrow.write_bytes(b"A")
    snapshot = {"slugs": {"tiny": {
        "arrow_bytes": 1, "arrow_sha256": hashlib.sha256(b"A").hexdigest()}}}
    plan = publish.plan_uploads(["tiny"], snapshot, outputs_root=root)
    assert plan == [(arrow, "v1/tiny/arrow/tiny.arrow.zstd")]


def test_plan_refuses_on_arrow_mismatch(tmp_path):
    """An arrow artifact whose on-disk sha disagrees with the snapshot is refused,
    the same integrity gate as parquet/vortex."""
    from raincloud.pipeline import publish
    root = tmp_path / "outputs"
    arrow = root / "v1" / "tiny" / "arrow" / "tiny.arrow.zstd"
    arrow.parent.mkdir(parents=True); arrow.write_bytes(b"A")
    snapshot = {"slugs": {"tiny": {"arrow_bytes": 1, "arrow_sha256": "WRONG"}}}
    with pytest.raises(publish.PublishMismatch) as ei:
        publish.plan_uploads(["tiny"], snapshot, outputs_root=root)
    assert "--rehash" in str(ei.value)


def test_plan_includes_all_three_formats(tmp_path):
    """arrow + parquet + vortex all present → all three planned, arrow leading
    per _PUBLISH_FORMATS."""
    from raincloud.pipeline import publish
    root = tmp_path / "outputs"
    files = {
        "arrow": root / "v1" / "tiny" / "arrow" / "tiny.arrow.zstd",
        "parquet": root / "v1" / "tiny" / "parquet" / "tiny.parquet",
        "vortex": root / "v1" / "tiny" / "vortex" / "tiny.vortex",
    }
    for f in files.values():
        f.parent.mkdir(parents=True); f.write_bytes(b"X")
    sha = hashlib.sha256(b"X").hexdigest()
    snapshot = {"slugs": {"tiny": {
        "arrow_bytes": 1, "arrow_sha256": sha,
        "parquet_bytes": 1, "parquet_sha256": sha,
        "vortex_bytes": 1, "vortex_sha256": sha}}}
    keys = [k for _, k in publish.plan_uploads(["tiny"], snapshot, outputs_root=root)]
    assert keys == ["v1/tiny/arrow/tiny.arrow.zstd",
                    "v1/tiny/parquet/tiny.parquet",
                    "v1/tiny/vortex/tiny.vortex"]


def test_plan_uploads_targets_v2_keys(tmp_path):
    """schema_version=2 → plan_uploads keys the artifact under v2/... (not v1),
    reading it from outputs/v2/. The sha-gate is preserved (matching sha passes)."""
    from raincloud.pipeline import publish
    art = tmp_path / "outputs" / "v2" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    sha = hashlib.sha256(b"data").hexdigest()
    snapshot = {"schema_version": 2, "slugs": {"tiny": {
        "parquet_bytes": 4, "parquet_sha256": sha,
        "vortex_bytes": None, "vortex_sha256": None}}}
    plan = publish.plan_uploads(["tiny"], snapshot,
                                outputs_root=tmp_path / "outputs", version=2)
    assert plan == [(art, "v2/tiny/parquet/tiny.parquet")]


def test_plan_uploads_v2_sha_gate_still_refuses(tmp_path):
    """The version bump doesn't weaken the integrity gate: a v2 artifact whose
    on-disk sha disagrees with the snapshot still raises PublishMismatch."""
    from raincloud.pipeline import publish
    art = tmp_path / "outputs" / "v2" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    snapshot = {"schema_version": 2, "slugs": {"tiny": {
        "parquet_bytes": 4, "parquet_sha256": "WRONG",
        "vortex_bytes": None, "vortex_sha256": None}}}
    with pytest.raises(publish.PublishMismatch):
        publish.plan_uploads(["tiny"], snapshot,
                             outputs_root=tmp_path / "outputs", version=2)


def test_main_targets_v2_when_schema_version_2(tmp_path, monkeypatch, capsys):
    """A v2 manifest + v2 snapshot → main() threads schema_version=2 into the
    upload plan, so the (dry-run) target key is v2/... . No bytes are published."""
    from raincloud.pipeline import publish
    repo = tmp_path
    art = repo / "outputs" / "v2" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    sha = hashlib.sha256(b"data").hexdigest()
    snap = repo / "snapshot.json"
    snap.write_text(json.dumps({"schema_version": 2, "slugs": {"tiny": {
        "parquet_bytes": 4, "parquet_sha256": sha,
        "vortex_bytes": None, "vortex_sha256": None}}}))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(snap))
    monkeypatch.setattr(publish, "load_manifest",
                        lambda: {"schema_version": 2, "datasets": [{"slug": "tiny"}]})
    monkeypatch.setattr(publish, "_outputs_root", lambda m=None: repo / "outputs" / "v2")
    _matched_catalog(monkeypatch, tmp_path, publish.load_manifest(), snap)
    rc = publish.main(["tiny", "--mirror", f"file://{tmp_path}/mirror", "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "v2/tiny/parquet/tiny.parquet" in out
    assert "v1/tiny" not in out
    assert "planned 1 artifact" in out


def test_main_uploads_to_file_mirror(tmp_path, monkeypatch):
    """Non-dry-run path actually writes bytes to the (file://) mirror, via a
    temp key + rename that leaves no `.part` debris at the canonical key."""
    from raincloud.pipeline import publish
    repo = tmp_path
    art = repo / "outputs" / "v1" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"REALBYTES")
    sha = hashlib.sha256(b"REALBYTES").hexdigest()
    snap = repo / "snapshot.json"
    snap.write_text(json.dumps(
        {"slugs": {"tiny": {"parquet_bytes": 9, "parquet_sha256": sha,
                            "vortex_bytes": None, "vortex_sha256": None}}}))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(snap))
    monkeypatch.setattr(publish, "load_manifest",
                        lambda: {"schema_version": 1, "datasets": [{"slug": "tiny"}]})
    monkeypatch.setattr(publish, "_outputs_root", lambda m=None: repo / "outputs" / "v1")
    mirror = tmp_path / "mirror"
    _matched_catalog(monkeypatch, tmp_path, publish.load_manifest(), snap)
    rc = publish.main(["tiny", "--mirror", f"file://{mirror}"])
    assert rc == 0
    uploaded = mirror / "v1" / "tiny" / "parquet" / "tiny.parquet"
    assert uploaded.read_bytes() == b"REALBYTES"
    # No leftover temp-key debris from the atomic upload.
    assert list((mirror / "v1" / "tiny" / "parquet").glob("*.part")) == []


def test_upload_cleans_up_temp_key_on_crash(tmp_path, monkeypatch):
    """A failure during the rename must not leave a `.part` object on the
    mirror, and must not create the canonical key."""
    import fsspec

    from raincloud.pipeline import publish
    local = tmp_path / "src.parquet"; local.write_bytes(b"PAYLOAD")
    mirror = tmp_path / "mirror"; mirror.mkdir()
    target = f"file://{mirror}/v1/tiny/parquet/tiny.parquet"

    real_url_to_fs = fsspec.core.url_to_fs

    def fake_url_to_fs(t):
        fs, path = real_url_to_fs(t)

        def boom(*a, **k):
            raise OSError("simulated mirror rename failure")

        monkeypatch.setattr(fs, "mv", boom)
        return fs, path

    monkeypatch.setattr(fsspec.core, "url_to_fs", fake_url_to_fs)
    with pytest.raises(OSError):
        publish._upload(local, target)
    # The temp .part was cleaned up and the canonical key never appeared.
    assert list((mirror / "v1" / "tiny" / "parquet").glob("*")) == []


def test_main_rejects_all_with_slugs(tmp_path, monkeypatch):
    from raincloud.pipeline import publish
    monkeypatch.setattr(publish, "load_manifest",
                        lambda: {"schema_version": 1, "datasets": []})
    with pytest.raises(SystemExit):
        publish.main(["tiny", "--all", "--mirror", f"file://{tmp_path}/m"])


def test_main_rejects_no_targets(tmp_path, monkeypatch):
    from raincloud.pipeline import publish
    monkeypatch.setattr(publish, "load_manifest",
                        lambda: {"schema_version": 1, "datasets": []})
    with pytest.raises(SystemExit):
        publish.main(["--mirror", f"file://{tmp_path}/m"])


def _scrape_manifest(*slugs_with_advisory, extra=()):
    """Manifest where the named slugs carry a license.scrape_advisory and any
    `extra` slugs are clean (license present, advisory None)."""
    datasets = [{"slug": s, "license": {"scrape_advisory": "scraped; do not mirror"}}
                for s in slugs_with_advisory]
    datasets += [{"slug": s, "license": {"scrape_advisory": None}} for s in extra]
    return {"schema_version": 1, "datasets": datasets}


def test_scrape_advisory_slugs_selects_flagged():
    from raincloud.pipeline import publish
    m = _scrape_manifest("scraped-a", extra=["clean-b"])
    # A slug with no license block at all must not trip the filter.
    m["datasets"].append({"slug": "bare"})
    assert publish.scrape_advisory_slugs(m, ["scraped-a", "clean-b", "bare"]) == \
        ["scraped-a"]


def test_main_refuses_explicit_scrape_slug(tmp_path, monkeypatch, capsys):
    """An explicit `publish <scrape-slug>` is blocked and fails loudly (rc=1),
    never touching the mirror."""
    from raincloud.pipeline import publish
    monkeypatch.setattr(publish, "load_manifest", lambda: _scrape_manifest("scraped-a"))
    rc = publish.main(["scraped-a", "--mirror", f"file://{tmp_path}/m"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "scrape_advisory" in err and "--allow-scrape-advisory" in err


def _plant(root, slug):
    """A small built artifact for `slug` under the test's v1 outputs root."""
    art = root / "outputs" / "v1" / slug / "parquet" / f"{slug}.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    return art


def test_main_allow_flag_overrides_block(tmp_path, monkeypatch, capsys):
    """--allow-scrape-advisory opts back in: the slug passes the gate and its
    built artifact is published (rc=0)."""
    from raincloud.pipeline import publish
    snap = tmp_path / "snapshot.json"
    snap.write_text(json.dumps({"slugs": {}}))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(snap))
    monkeypatch.setattr(publish, "load_manifest", lambda: _scrape_manifest("scraped-a"))
    monkeypatch.setattr(publish, "_outputs_root", lambda m=None: tmp_path / "outputs" / "v1")
    _matched_catalog(monkeypatch, tmp_path, publish.load_manifest(), snap)
    args = ["scraped-a", "--allow-scrape-advisory", "--mirror", f"file://{tmp_path}/m"]
    # Past the gate, a named slug with nothing built here is refused, not "published 0".
    assert publish.main(args) == 1
    assert "nothing built for scraped-a" in capsys.readouterr().err
    _plant(tmp_path, "scraped-a")
    assert publish.main(args) == 0
    assert (tmp_path / "m" / "v1/scraped-a/parquet/scraped-a.parquet").read_bytes() == b"data"


def test_main_all_skips_scrape_slug_publishes_rest(tmp_path, monkeypatch, capsys):
    """--all drops the scrape-advisory slug with a notice and still publishes the
    clean one."""
    from raincloud.pipeline import publish
    repo = tmp_path
    art = repo / "outputs" / "v1" / "clean-b" / "parquet" / "clean-b.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    sha = hashlib.sha256(b"data").hexdigest()
    snap = repo / "snapshot.json"
    snap.write_text(json.dumps({"slugs": {"clean-b": {
        "parquet_bytes": 4, "parquet_sha256": sha,
        "vortex_bytes": None, "vortex_sha256": None}}}))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(snap))
    monkeypatch.setattr(publish, "load_manifest",
                        lambda: _scrape_manifest("scraped-a", extra=["clean-b"]))
    monkeypatch.setattr(publish, "_outputs_root", lambda m=None: repo / "outputs" / "v1")
    _matched_catalog(monkeypatch, tmp_path, publish.load_manifest(), snap)
    rc = publish.main(["--all", "--mirror", f"file://{tmp_path}/mirror", "--dry-run"])
    assert rc == 0
    cap = capsys.readouterr()
    assert "refusing scraped-a" in cap.err
    assert "v1/clean-b/parquet/clean-b.parquet" in cap.out


def test_no_redistribution_slugs_selects_flagged():
    from raincloud.pipeline import publish
    m = {"schema_version": 1, "datasets": [
        {"slug": "blocked", "license": {"redistribution_permitted": False}},
        {"slug": "ok", "license": {"redistribution_permitted": True}},
        {"slug": "bare"},  # no license block must not trip the filter
    ]}
    assert publish.no_redistribution_slugs(m, ["blocked", "ok", "bare"]) == ["blocked"]


def test_main_refuses_no_redistribution_slug(tmp_path, monkeypatch, capsys):
    from raincloud.pipeline import publish
    m = {"schema_version": 1, "datasets": [
        {"slug": "restricted", "license": {"redistribution_permitted": False,
                                           "scrape_advisory": None}}]}
    monkeypatch.setattr(publish, "load_manifest", lambda: m)
    rc = publish.main(["restricted", "--mirror", f"file://{tmp_path}/m"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "redistribution_permitted=false" in err
    assert "--allow-no-redistribution" in err


def test_main_both_gates_need_both_flags(tmp_path, monkeypatch, capsys):
    """A slug that trips BOTH gates (scrape_advisory + no-redistribution, like the
    Amazon corpus) stays refused until BOTH overrides are passed."""
    from raincloud.pipeline import publish
    m = {"schema_version": 1, "datasets": [
        {"slug": "amazon", "license": {"redistribution_permitted": False,
                                       "scrape_advisory": "scraped; do not mirror"}}]}
    monkeypatch.setattr(publish, "load_manifest", lambda: m)
    snap = tmp_path / "snapshot.json"; snap.write_text(json.dumps({"slugs": {}}))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(snap))
    monkeypatch.setattr(publish, "_outputs_root", lambda mm=None: tmp_path / "outputs" / "v1")
    _matched_catalog(monkeypatch, tmp_path, publish.load_manifest(), snap)
    base = ["amazon", "--mirror", f"file://{tmp_path}/m"]
    _plant(tmp_path, "amazon")  # built, so only the gates can refuse it
    # Only the scrape override -> still blocked by the redistribution gate.
    assert publish.main(base + ["--allow-scrape-advisory"]) == 1
    assert "redistribution_permitted=false" in capsys.readouterr().err
    # Only the redistribution override -> still blocked by the advisory gate.
    assert publish.main(base + ["--allow-no-redistribution"]) == 1
    assert "scrape_advisory" in capsys.readouterr().err
    assert not (tmp_path / "m").exists()
    # Both overrides -> the built artifact is published.
    assert publish.main(base + ["--allow-scrape-advisory",
                                "--allow-no-redistribution"]) == 0
    assert (tmp_path / "m" / "v1/amazon/parquet/amazon.parquet").read_bytes() == b"data"


def test_main_dry_run_finds_artifact(tmp_path, monkeypatch, capsys):
    from raincloud.pipeline import publish
    repo = tmp_path
    art = repo / "outputs" / "v1" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    sha = hashlib.sha256(b"data").hexdigest()
    snap = repo / "snapshot.json"
    snap.write_text(
        json.dumps({"slugs": {"tiny": {"parquet_bytes": 4, "parquet_sha256": sha,
                                       "vortex_bytes": None, "vortex_sha256": None}}}))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(snap))
    monkeypatch.setattr(publish, "load_manifest",
                        lambda: {"schema_version": 1, "datasets": [{"slug": "tiny"}]})
    monkeypatch.setattr(publish, "_outputs_root", lambda m=None: repo / "outputs" / "v1")
    _matched_catalog(monkeypatch, tmp_path, publish.load_manifest(), snap)
    rc = publish.main(["tiny", "--mirror", f"file://{tmp_path}/mirror", "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "v1/tiny/parquet/tiny.parquet" in out
    assert "planned 1 artifact" in out


def _matched_catalog(monkeypatch, root, manifest, snapshot):
    path = root / "sources.json"
    path.write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(path))
    doc = json.loads(snapshot.read_text())
    doc.setdefault("schema_version", manifest["schema_version"])
    snapshot.write_text(json.dumps(doc))


def test_file_uri_publish_roundtrip(tmp_path):
    from raincloud._transport import fetch
    from raincloud.pipeline.publish import _upload

    source = tmp_path / "source"
    source.write_bytes(b"prepared bytes")
    target = tmp_path / "mirror é 100%" / "nested" / "artifact#1"
    _upload(source, target.as_uri())
    assert target.read_bytes() == source.read_bytes()
    downloaded = tmp_path / "downloaded"
    fetch(target.as_uri(), downloaded)
    assert downloaded.read_bytes() == source.read_bytes()


def _store_fixture(tmp_path, monkeypatch, *, license=None, extra_snapshot=None):
    from raincloud.pipeline import publish
    art = tmp_path / "outputs" / "v2" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"REALBYTES")
    snap = tmp_path / "snapshot.json"
    slugs = {"tiny": {"parquet_bytes": 9, "parquet_sha256": hashlib.sha256(b"REALBYTES").hexdigest()}}
    slugs.update(extra_snapshot or {})
    snap.write_text(json.dumps({"schema_version": 2, "slugs": slugs}))
    spec = {"slug": "tiny", **({"license": license} if license else {})}
    datasets = [spec, *({"slug": s} for s in (extra_snapshot or {}))]
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(snap))
    monkeypatch.setattr(publish, "load_manifest", lambda: {"schema_version": 2, "datasets": datasets})
    monkeypatch.setattr(publish, "_outputs_root", lambda m=None: tmp_path / "outputs" / "v2")
    _matched_catalog(monkeypatch, tmp_path, publish.load_manifest(), snap)
    return publish, art


def test_store_places_one_linked_copy(tmp_path, monkeypatch):
    publish, art = _store_fixture(tmp_path, monkeypatch)
    store = tmp_path / "store"
    assert publish.main(["tiny", "--store", str(store)]) == 0
    placed = store / "v2" / "tiny" / "parquet" / "tiny.parquet"
    assert placed.read_bytes() == b"REALBYTES"
    assert placed.stat().st_ino == art.stat().st_ino  # same filesystem: linked, not copied
    assert list(placed.parent.iterdir()) == [placed]
    # A re-run finds the artifact already published and changes nothing.
    before = placed.stat()
    assert publish.main(["tiny", "--store", str(store)]) == 0
    assert (placed.stat().st_ino, placed.stat().st_mtime_ns) == (before.st_ino, before.st_mtime_ns)


def test_store_is_not_gated_by_redistribution_license(tmp_path, monkeypatch):
    # The gates guard off-machine redistribution; the machine's own store is not that.
    publish, _ = _store_fixture(tmp_path, monkeypatch, license={"redistribution_permitted": False})
    assert publish.main(["tiny", "--mirror", f"file://{tmp_path}/mirror"]) == 1
    assert publish.main(["tiny", "--store", str(tmp_path / "store")]) == 0
    assert (tmp_path / "store" / "v2" / "tiny" / "parquet" / "tiny.parquet").is_file()


def test_catalogs_requires_store(tmp_path, monkeypatch):
    publish, _ = _store_fixture(tmp_path, monkeypatch)
    with pytest.raises(SystemExit):
        publish.main(["tiny", "--mirror", f"file://{tmp_path}/m", "--catalogs", str(tmp_path / "c")])


def test_release_refuses_catalog_naming_absent_artifacts(tmp_path, monkeypatch, capsys):
    publish, _ = _store_fixture(tmp_path, monkeypatch,
                                extra_snapshot={"unbuilt": {"parquet_bytes": 5, "parquet_sha256": "0" * 64}})
    catalogs_dir = tmp_path / "catalogs"
    rc = publish.main(["tiny", "--store", str(tmp_path / "store"), "--catalogs", str(catalogs_dir)])
    assert rc == 1
    assert "v2/unbuilt/parquet/unbuilt.parquet" in capsys.readouterr().err
    assert not (catalogs_dir / "latest.json").exists()


def test_released_store_serves_readers_without_rehash(tmp_path, monkeypatch):
    import raincloud._cache as cache
    from raincloud._resolve import resolve
    from raincloud.config import resolve_config
    publish, _ = _store_fixture(tmp_path, monkeypatch)
    store, catalogs_dir = tmp_path / "store", tmp_path / "catalogs"
    assert publish.main(["tiny", "--store", str(store), "--catalogs", str(catalogs_dir)]) == 0
    assert (catalogs_dir / "latest.json").is_file()
    for var in ("RAINCLOUD_MANIFEST", "RAINCLOUD_SNAPSHOT"):
        monkeypatch.delenv(var)
    reader = resolve_config(no_config=True, data_dir=store, cache_dir=tmp_path / "cache",
                            catalog_dir=tmp_path / "user-catalogs", catalog=str(catalogs_dir))
    monkeypatch.setattr(cache, "sha256_file", lambda p: pytest.fail(f"rehashed {p}"))
    assert resolve("tiny", "parquet", config=reader) == store / "v2" / "tiny" / "parquet" / "tiny.parquet"


def test_store_does_not_rehash_what_it_already_holds(tmp_path, monkeypatch):
    publish, art = _store_fixture(tmp_path, monkeypatch)
    store = tmp_path / "store"
    assert publish.main(["tiny", "--store", str(store)]) == 0
    monkeypatch.setattr(publish, "sha256_file", lambda p: pytest.fail(f"rehashed {p}"))
    assert publish.main(["tiny", "--store", str(store)]) == 0  # same inode: nothing enters
