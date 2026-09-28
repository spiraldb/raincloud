# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Observations follow selected catalog identity, including real subprocesses."""
import json
import os
import sys
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import raincloud
from raincloud._bundle import encode, make_bundle
from raincloud.catalogs import current, operation
from raincloud.pipeline import browse, compliance, docs, overnight_profile, promote_profiles, spec


@pytest.fixture
def selected(tmp_path, monkeypatch):
    recipe = {"slug": "tiny", "short_name": "Tiny", "full_name": "Tiny", "export": {"formats": []}}
    bundle = make_bundle(encode({"schema_version": 2, "datasets": [recipe]}),
                         encode({"schema_version": 2, "slugs": {}}), "observations-test")
    directory = tmp_path / "catalog"
    directory.mkdir()
    for name, raw in bundle.files().items():
        (directory / name).write_bytes(raw)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(directory),
        data_dir=tmp_path / "data", cache_dir=tmp_path / "data", raw_dir=tmp_path / "raw",
        scratch_dir=tmp_path / "scratch", catalog_dir=tmp_path / "catalogs", offline=True)
    install = tmp_path / "install"
    for module in (spec, docs, browse, promote_profiles):
        monkeypatch.setattr(module, "REPO_ROOT", install)
    oracle = install / "docs/v2/compliance.json"
    oracle.parent.mkdir(parents=True)
    oracle.write_text(json.dumps({"slugs": {"tiny": {"write": [{"cell": "parquet@py", "roundtrip": True}]}}}))
    with operation(cfg):
        yield cfg, install, oracle


@pytest.mark.parametrize("source", ["custom", "bundled", "pinned"])
def test_compliance_default_and_browser_are_revision_local(selected, source):
    cfg, install, oracle = selected
    original = oracle.read_bytes()
    context = replace(current(), source=source)
    with operation(cfg, context):
        assert browse._load_compliance(2) == {}
        assert compliance.main(["tiny", "--skip-slug", "tiny", "--write-ledger"]) == 0
        expected = cfg.data_dir / ".raincloud/observations" / context.bundle.revision / "compliance.json"
        assert spec.default_compliance_json() == expected
        assert expected.is_file()
        assert browse._load_compliance(2) == {"tiny": {}}
    assert oracle.read_bytes() == original


def test_compliance_checkout_and_explicit_paths_remain_authoritative(selected, tmp_path):
    cfg, install, oracle = selected
    with operation(cfg, replace(current(), source="checkout", legacy=True)):
        assert spec.default_compliance_json() == oracle
        assert browse._load_compliance(2) == {"tiny": {"parquet@py": True}}
    explicit = tmp_path / "report.json"
    assert compliance.main(["tiny", "--skip-slug", "tiny", "--write-ledger", str(explicit)]) == 0
    assert explicit.is_file()


def test_checkout_overnight_real_children_cleanup_and_second_run(selected, monkeypatch):
    cfg, _, _ = selected
    monkeypatch.setenv("PATH", str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"])
    # Start from a checkout context, exactly the case whose parent previously
    # searched tracked docs while pinned children wrote revision observations.
    context = replace(current(), source="checkout", legacy=True)
    path = spec.prepared_parquet("tiny")
    path.parent.mkdir(parents=True)
    pq.write_table(pa.table({"x": [1, 2, 3]}), path)
    with operation(cfg, context):
        assert overnight_profile.main(["--slugs", "tiny", "--skip-build"]) == 0
        assert not path.exists()
        promoted = cfg.data_dir / ".raincloud/observations" / context.bundle.revision / "profiles/tiny.json"
        assert json.loads(promoted.read_text())["row_count"] == 3
        def unexpected(*args, **kwargs):
            pytest.fail("completed slug must not run another child stage")
        monkeypatch.setattr(overnight_profile, "_run_stage", unexpected)
        assert overnight_profile.main(["--slugs", "tiny", "--skip-build"]) == 0
        events = [json.loads(line) for line in overnight_profile._log_path().read_text().splitlines()]
        assert events[-2]["candidates"] == 0
        assert events[-1]["processed"] == 0


def test_docs_records_which_writer_made_each_file(selected):
    parquet = spec.prepared_parquet("tiny")
    parquet.parent.mkdir(parents=True)
    pq.write_table(pa.table({"x": [1, 2]}), parquet)
    assert docs.main(["snapshot"]) == 0
    observed = spec.observations_dir() / "snapshot.json"
    result = json.loads(observed.read_text())["slugs"]["tiny"]
    assert result["last_built_rows"] == 2
    assert result["parquet_writer"] == "py"  # from the footer's created_by
    assert "artifacts" not in result
    # What a build recorded stands while the file is the one it recorded...
    document = json.loads(observed.read_text())
    document["slugs"]["tiny"]["parquet_writer"] = "rs"
    observed.write_text(json.dumps(document))
    assert docs.main(["snapshot"]) == 0
    assert json.loads(observed.read_text())["slugs"]["tiny"]["parquet_writer"] == "rs"
    # ...and a different file is described by itself.
    pq.write_table(pa.table({"x": [1, 2, 3]}), parquet)
    assert docs.main(["snapshot"]) == 0
    updated = json.loads(observed.read_text())["slugs"]["tiny"]
    assert updated["parquet_sha256"] != result["parquet_sha256"]
    assert updated["parquet_writer"] == "py"


def test_docs_takes_a_built_file_from_the_build_record(selected):
    # Regenerating is how a maintainer turns this install's builds into the
    # catalog: a file the build record names at its size is described by it.
    from raincloud import _builds
    from raincloud._resolve import artifact_key
    cfg, _, _ = selected
    parquet = spec.prepared_parquet("tiny")
    parquet.parent.mkdir(parents=True)
    pq.write_table(pa.table({"x": [1, 2]}), parquet)
    _builds.record(cfg.data_dir, {artifact_key("tiny", "parquet", 2): {
        "sha256": "b" * 64, "bytes": parquet.stat().st_size, "writer": "rs", "recipe": "r"}})
    assert docs.main(["snapshot"]) == 0
    result = json.loads((spec.observations_dir() / "snapshot.json").read_text())["slugs"]["tiny"]
    assert (result["parquet_sha256"], result["parquet_writer"]) == ("b" * 64, "rs")
    assert docs.main(["snapshot", "--rehash"]) == 0
    rehashed = json.loads((spec.observations_dir() / "snapshot.json").read_text())["slugs"]["tiny"]
    assert rehashed["parquet_sha256"] != "b" * 64


def test_installed_legacy_catalog_does_not_borrow_installation_observations(selected, monkeypatch):
    cfg, install, _ = selected
    foreign_snapshot = install / "docs/snapshot.json"
    foreign_snapshot.write_text(json.dumps({"schema_version": 2, "slugs": {"tiny": {"last_built_rows": 999}}}))
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", foreign_snapshot)
    with operation(cfg, replace(current(), source="bundled", legacy=True)):
        assert docs._load_snapshot_slugs(2) == {}


def test_compliance_measures_real_artifact_with_readonly_install(selected):
    cfg, install, oracle = selected
    canonical = spec.prepared_arrow("tiny")
    canonical.parent.mkdir(parents=True)
    table = pa.table({"x": [1, 2]})
    with pa.ipc.new_file(canonical, table.schema) as writer:
        writer.write_table(table)
    original = oracle.read_bytes()
    # Exercise the full measurement and real ledger serializer while the
    # installation's oracle directory is unwritable. Only temp data is used.
    oracle.parent.chmod(0o555)
    try:
        assert compliance.main(["tiny", "--cells", "parquet@py", "--readers", "parquet@py",
                                "--reencode", "--write-ledger"]) == 0
    finally:
        oracle.parent.chmod(0o755)
    ledger = json.loads(spec.default_compliance_json().read_text())
    assert ledger["slugs"]["tiny"]["write"][0]["roundtrip"] is True
    assert browse._load_compliance(2) == {"tiny": {"parquet@py": True}}
    assert oracle.read_bytes() == original
