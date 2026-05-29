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
