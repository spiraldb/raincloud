# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import hashlib
import json

import pytest


def test_plan_uploads_matching_slug(tmp_path):
    from scripts.pipeline import publish
    art = tmp_path / "outputs" / "v1" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    sha = hashlib.sha256(b"data").hexdigest()
    snapshot = {"slugs": {"tiny": {"parquet_bytes": 4, "parquet_sha256": sha,
                                   "vortex_bytes": None, "vortex_sha256": None}}}
    plan = publish.plan_uploads(["tiny"], snapshot,
                                outputs_root=tmp_path / "outputs")
    assert plan == [(art, "v1/tiny/parquet/tiny.parquet")]


def test_plan_refuses_on_mismatch(tmp_path):
    from scripts.pipeline import publish
    art = tmp_path / "outputs" / "v1" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    snapshot = {"slugs": {"tiny": {"parquet_bytes": 4, "parquet_sha256": "WRONG",
                                   "vortex_bytes": None, "vortex_sha256": None}}}
    with pytest.raises(publish.PublishMismatch) as ei:
        publish.plan_uploads(["tiny"], snapshot, outputs_root=tmp_path / "outputs")
    # Finding 2: the remediation must point at --rehash, since a plain
    # `docs snapshot` regen reuses the stale sha on a size match and re-fails.
    assert "--rehash" in str(ei.value)


def test_plan_includes_vortex_and_skips_absent(tmp_path):
    """Both formats are planned when present; an absent format is skipped, not
    errored. A None snapshot sha is published ungated (freshly built slug)."""
    from scripts.pipeline import publish
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


def test_main_uploads_to_file_mirror(tmp_path, monkeypatch):
    """Non-dry-run path actually writes bytes to the (file://) mirror, via a
    temp key + rename that leaves no `.part` debris at the canonical key."""
    from scripts.pipeline import publish
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

    from scripts.pipeline import publish
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
    from scripts.pipeline import publish
    monkeypatch.setattr(publish, "load_manifest",
                        lambda: {"schema_version": 1, "datasets": []})
    with pytest.raises(SystemExit):
        publish.main(["tiny", "--all", "--mirror", f"file://{tmp_path}/m"])


def test_main_rejects_no_targets(tmp_path, monkeypatch):
    from scripts.pipeline import publish
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
    from scripts.pipeline import publish
    m = _scrape_manifest("scraped-a", extra=["clean-b"])
    # A slug with no license block at all must not trip the filter.
    m["datasets"].append({"slug": "bare"})
    assert publish.scrape_advisory_slugs(m, ["scraped-a", "clean-b", "bare"]) == \
        ["scraped-a"]


def test_main_refuses_explicit_scrape_slug(tmp_path, monkeypatch, capsys):
    """An explicit `publish <scrape-slug>` is blocked and fails loudly (rc=1),
    never touching the mirror."""
    from scripts.pipeline import publish
    monkeypatch.setattr(publish, "load_manifest", lambda: _scrape_manifest("scraped-a"))
    rc = publish.main(["scraped-a", "--mirror", f"file://{tmp_path}/m"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "scrape_advisory" in err and "--allow-scrape-advisory" in err


def test_main_allow_flag_overrides_block(tmp_path, monkeypatch):
    """--allow-scrape-advisory opts back in: the slug reaches plan_uploads (and,
    absent any artifact on disk, publishes 0 with rc=0 rather than being refused)."""
    from scripts.pipeline import publish
    snap = tmp_path / "snapshot.json"
    snap.write_text(json.dumps({"slugs": {}}))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(snap))
    monkeypatch.setattr(publish, "load_manifest", lambda: _scrape_manifest("scraped-a"))
    monkeypatch.setattr(publish, "_outputs_root", lambda m=None: tmp_path / "outputs" / "v1")
    rc = publish.main(["scraped-a", "--allow-scrape-advisory",
                       "--mirror", f"file://{tmp_path}/m"])
    assert rc == 0


def test_main_all_skips_scrape_slug_publishes_rest(tmp_path, monkeypatch, capsys):
    """--all drops the scrape-advisory slug with a notice and still publishes the
    clean one."""
    from scripts.pipeline import publish
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
    rc = publish.main(["--all", "--mirror", f"file://{tmp_path}/mirror", "--dry-run"])
    assert rc == 0
    cap = capsys.readouterr()
    assert "refusing scraped-a" in cap.err
    assert "v1/clean-b/parquet/clean-b.parquet" in cap.out


def test_no_redistribution_slugs_selects_flagged():
    from scripts.pipeline import publish
    m = {"schema_version": 1, "datasets": [
        {"slug": "blocked", "license": {"redistribution_permitted": False}},
        {"slug": "ok", "license": {"redistribution_permitted": True}},
        {"slug": "bare"},  # no license block must not trip the filter
    ]}
    assert publish.no_redistribution_slugs(m, ["blocked", "ok", "bare"]) == ["blocked"]


def test_main_refuses_no_redistribution_slug(tmp_path, monkeypatch, capsys):
    from scripts.pipeline import publish
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
    from scripts.pipeline import publish
    m = {"schema_version": 1, "datasets": [
        {"slug": "amazon", "license": {"redistribution_permitted": False,
                                       "scrape_advisory": "scraped; do not mirror"}}]}
    monkeypatch.setattr(publish, "load_manifest", lambda: m)
    snap = tmp_path / "snapshot.json"; snap.write_text(json.dumps({"slugs": {}}))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(snap))
    monkeypatch.setattr(publish, "_outputs_root", lambda mm=None: tmp_path / "outputs" / "v1")
    base = ["amazon", "--mirror", f"file://{tmp_path}/m"]
    # Only the scrape override -> still blocked by the redistribution gate.
    assert publish.main(base + ["--allow-scrape-advisory"]) == 1
    # Only the redistribution override -> still blocked by the advisory gate.
    assert publish.main(base + ["--allow-no-redistribution"]) == 1
    # Both overrides -> reaches plan_uploads (no artifact on disk -> publishes 0).
    assert publish.main(base + ["--allow-scrape-advisory",
                                "--allow-no-redistribution"]) == 0


def test_main_dry_run_finds_artifact(tmp_path, monkeypatch, capsys):
    from scripts.pipeline import publish
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
    rc = publish.main(["tiny", "--mirror", f"file://{tmp_path}/mirror", "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "v1/tiny/parquet/tiny.parquet" in out
    assert "planned 1 artifact" in out
