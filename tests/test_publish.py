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
    with pytest.raises(publish.PublishMismatch):
        publish.plan_uploads(["tiny"], snapshot, outputs_root=tmp_path / "outputs")


def test_main_dry_run_finds_artifact(tmp_path, monkeypatch, capsys):
    from scripts.pipeline import publish
    repo = tmp_path
    (repo / "docs" / "v1").mkdir(parents=True)
    art = repo / "outputs" / "v1" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True); art.write_bytes(b"data")
    sha = hashlib.sha256(b"data").hexdigest()
    (repo / "docs" / "v1" / "snapshot.json").write_text(
        json.dumps({"slugs": {"tiny": {"parquet_bytes": 4, "parquet_sha256": sha,
                                       "vortex_bytes": None, "vortex_sha256": None}}}))
    monkeypatch.setattr(publish, "REPO_ROOT", repo)
    monkeypatch.setattr(publish, "load_manifest",
                        lambda: {"schema_version": 1, "datasets": [{"slug": "tiny"}]})
    monkeypatch.setattr(publish, "_outputs_root", lambda m=None: repo / "outputs" / "v1")
    rc = publish.main(["tiny", "--mirror", f"file://{tmp_path}/mirror", "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "v1/tiny/parquet/tiny.parquet" in out
    assert "planned 1 artifact" in out
