# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Build / export / compliance / convert / hydrate core: selection, publication,
provenance and the canonical spine's invariants."""
from __future__ import annotations

import json
import os
import sys
import textwrap

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import raincloud
from raincloud import _builds
from raincloud._bundle import encode, make_bundle
from raincloud._resolve import artifact_key
from raincloud.catalogs import operation
from raincloud.exceptions import ArtifactNotFound, CatalogConflict, OfflineMiss
from raincloud.pipeline import build, canonical, compliance, convert, docs, hydrate
from raincloud.pipeline.export import ReadResult, Verdict, compare, get_exporter
from raincloud.pipeline.export import exporters as exporters_mod
from raincloud.pipeline.export.__main__ import main as export_main
from raincloud.pipeline.lifecycle import BuildOutputs, build_outputs, operation_lock
from raincloud.pipeline.spec import ParquetOptions, prepared_arrow, prepared_parquet, prepared_vortex
from raincloud.pipeline.validate import validate

TINY = {"slug": "tiny", "export": {"formats": ["parquet", "vortex"]}}
HYDRATED = {"slug": "tiny-hydrated", "advisory": "a test fixture's pages",
            "derive": {"from": "tiny", "hydrate": {"columns": {"url": {"into": "content", "type": "binary"}}}}}
TABLE = pa.table({"x": [1, 2], "url": ["https://example.test/a", None]})


def _artifacts(tmp_path, paths):
    """A `prepared_artifact` stand-in: `paths[fmt]`, else a file that never exists."""
    return lambda slug, fmt, manifest=None: paths.get(fmt, tmp_path / f"missing.{fmt}")


def _catalog(tmp_path, name, datasets, slugs=None):
    """A config selecting a catalog of `datasets`, over one shared store."""
    bundle = make_bundle(encode({"schema_version": 2, "datasets": datasets}),
                         encode({"schema_version": 2, "slugs": slugs or {}}), name)
    directory = tmp_path / name
    directory.mkdir()
    for filename, raw in bundle.files().items():
        (directory / filename).write_bytes(raw)
    return raincloud.resolve_config(
        no_config=True, catalog=str(directory), data_dir=tmp_path / "data",
        cache_dir=tmp_path / "data", raw_dir=tmp_path / "raw", scratch_dir=tmp_path / "scratch",
        catalog_dir=tmp_path / "catalogs", offline=True)


@pytest.fixture
def stages(monkeypatch):
    """Replace fetch → transform with a fixed table; returns the call log."""
    calls = []
    monkeypatch.setattr(build, "fetch", lambda spec: calls.append(spec["slug"]) or [])
    monkeypatch.setattr(build, "extract", lambda spec, paths: [])
    monkeypatch.setattr(build, "parse", lambda spec, paths: [])
    monkeypatch.setattr(build, "transform", lambda spec, tables: [(spec["slug"], TABLE)])
    return calls


@pytest.fixture
def store(tmp_path, stages):
    # `other` has a recipe of its own (offered formats are not part of one).
    other = {"slug": "other", "export": {"formats": ["parquet"]}, "expect": {"rows": 7}}
    cfg = _catalog(tmp_path, "contracts", [TINY, HYDRATED, other])
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        yield cfg


def _record(cfg):
    return _builds.read(cfg.data_dir)


# ---------- canonical names ----------

@pytest.mark.parametrize("names", [["a", "a", "a [1]"], ["a", "a [1]", "a"], ["a", "a", "a [1]", "a [1]"]])
def test_uniquify_never_collides_with_a_real_name(names):
    out = canonical.uniquify_names(names)
    assert len(set(out)) == len(out) == len(names)
    assert out[0] == "a" and "a [1]" in out  # first occurrences keep their names


def test_duplicate_headers_reach_every_format_unique(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    table = pa.Table.from_arrays([pa.array([1]), pa.array([2]), pa.array([3])], names=["a", "a", "a [1]"])
    (path,) = canonical.write_canonical({"slug": "dupes"}, [("dupes", table)])
    assert path == prepared_arrow("dupes")
    for cell in ("parquet@py", "vortex@py"):
        get_exporter(cell).export({"slug": "dupes"}, path)
    assert pq.read_table(prepared_parquet("dupes")).column_names == ["a", "a [2]", "a [1]"]
    assert prepared_vortex("dupes").stat().st_size > 0


def test_open_canonical_writer_refuses_duplicate_names(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    schema = pa.schema([("a", pa.int64()), ("a", pa.int64())])
    with pytest.raises(ValueError, match="repeats column name"):
        with canonical.open_canonical_writer("dupes", schema):
            pass
    assert not prepared_arrow("dupes").exists()


# ---------- comparator windows ----------

def test_values_equal_compares_misaligned_chunks_window_by_window(monkeypatch):
    got = pa.table({"s": pa.chunked_array([["a", "b", "c"], ["d"]])})
    same = pa.table({"s": pa.chunked_array([["a"], ["b", "c", "d"]])})
    differs = pa.table({"s": pa.chunked_array([["a"], ["b", "c", "X"]])})
    lengths = []
    real = compare._exact_equal
    monkeypatch.setattr(compare, "_exact_equal", lambda g, e: lengths.append(len(e)) or real(g, e))
    assert compare.values_equal(got, same) == (True, "")
    # Never a whole-column combine: every compared window lies inside one chunk of both.
    assert lengths and max(lengths) <= 2
    ok, why = compare.values_equal(got, differs)
    assert not ok and "values or float bits differ" in why


# ---------- selection ----------

@pytest.mark.parametrize("run", [
    lambda argv: build._main(argv),
    lambda argv: export_main(argv),
    lambda argv: convert.main(argv),
    lambda argv: compliance.main(argv),
], ids=["build", "export", "convert", "compliance"])
def test_a_typo_among_valid_slugs_exits_2_with_a_suggestion(store, stages, run, capsys):
    stages.clear()
    try:
        code = run(["tiny", "tinny"])
    except SystemExit as exc:
        code = exc.code
    assert code == 2
    err = capsys.readouterr().err
    assert "tinny" in err and "tiny" in err.split("tinny", 1)[1]
    assert stages == []  # nothing ran for the valid name either


def test_compliance_all_skips_hydrated_datasets_and_passes(store, capsys):
    code = compliance.main(["--all", "--cells", "parquet@py", "--readers", "parquet@py",
                            "--skip-slug", "other"])
    assert code == 0
    assert "measure them by name" in capsys.readouterr().err


def test_compliance_rejects_a_skip_slug_typo_and_an_empty_cell_list(store, capsys):
    assert compliance.main(["tiny", "--skip-slug", "tinny"]) == 2
    assert "tiny" in capsys.readouterr().err
    assert compliance.main(["tiny", "--cells", ""]) == 2


def test_export_all_skips_hydrated_and_a_named_missing_canonical_fails(store, capsys):
    assert export_main(["--all", "--dry-run"]) == 0
    captured = capsys.readouterr()
    assert "tiny-hydrated" not in captured.out + captured.err
    assert export_main(["other"]) == 1
    assert "[no canonical] other" in capsys.readouterr().err


# ---------- export publication and provenance ----------

def test_export_cli_restores_the_prior_file_when_a_writer_fails(store, monkeypatch):
    parquet = prepared_parquet("tiny")
    before = parquet.read_bytes()
    cls = type(get_exporter("parquet@py"))
    real = cls.export

    def broken(self, spec, canonical, dest=None):
        real(self, spec, canonical, dest)
        raise RuntimeError("writer failed after writing")
    monkeypatch.setattr(cls, "export", broken)
    assert export_main(["tiny", "--format", "parquet"]) == 1
    assert parquet.read_bytes() == before
    assert not list(parquet.parent.glob(".*"))


_MISMATCH = """
import argparse, json
import pyarrow as pa, pyarrow.parquet as pq
ap = argparse.ArgumentParser()
for flag in ("--input", "--output", "--report"):
    ap.add_argument(flag)
a = ap.parse_args()
with pa.ipc.open_file(a.input) as r:
    pq.write_table(r.read_all().slice(0, 1), a.output)
json.dump({"roundtrip": False, "variant_faithful": True, "note": "mock mismatch"}, open(a.report, "w"))
"""


def test_a_sidecar_reporting_a_mismatch_never_replaces_the_file(store, tmp_path, monkeypatch):
    mock = tmp_path / "mock_writer.py"
    mock.write_text(f"#!{sys.executable}\n" + textwrap.dedent(_MISMATCH))
    mock.chmod(0o755)
    monkeypatch.setenv("RAINCLOUD_SIDECAR_PARQUET_RS", str(mock))
    parquet = prepared_parquet("tiny")
    before = parquet.read_bytes()
    assert export_main(["tiny", "--format", "parquet@rs"]) == 1
    assert parquet.read_bytes() == before
    result = get_exporter("parquet@rs").export(TINY, prepared_arrow("tiny"))
    assert result.compliance.roundtrip is False and "mock mismatch" in result.compliance.note
    assert parquet.read_bytes() == before


def test_export_refuses_a_canonical_from_an_earlier_recipe(store, tmp_path):
    changed = {**TINY, "expect": {"rows": 2}}  # a canonical-determining change
    parquet = prepared_parquet("tiny")
    before = parquet.read_bytes()
    with operation(_catalog(tmp_path, "contracts-new", [changed])):
        assert export_main(["tiny"]) == 1
    assert parquet.read_bytes() == before


def test_export_after_an_export_stage_change_serves_canonical_and_exports(store, tmp_path):
    changed = {**TINY, "write": {"compression": "snappy"}}
    cfg = _catalog(tmp_path, "contracts-write", [changed])
    with operation(cfg):
        assert export_main(["tiny"]) == 0
        for fmt in ("arrow", "parquet", "vortex"):
            assert raincloud.load("tiny", format=fmt, config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def test_accepted_files_are_recorded_before_a_later_writer_fails(tmp_path, stages, monkeypatch):
    cfg = _catalog(tmp_path, "contracts-partial", [TINY])
    cls = type(get_exporter("vortex@py"))
    monkeypatch.setattr(cls, "export", lambda self, spec, canonical, dest=None: 1 / 0)
    with operation(cfg):
        # A planned writer's failure is recorded, and the build succeeds without it.
        assert build.run_one(TINY, strict=False)
        recorded = _record(cfg)
        assert artifact_key("tiny", "arrow", 2) in recorded
        assert artifact_key("tiny", "parquet", 2) in recorded
        assert "ZeroDivisionError" in recorded[artifact_key("tiny", "vortex", 2)]["unavailable"]["error"]
        for fmt in ("arrow", "parquet"):
            assert raincloud.load("tiny", format=fmt, config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def test_export_all_survives_one_slugs_native_panic(tmp_path, stages, monkeypatch):
    two = {"slug": "two", "export": {"formats": ["parquet"]}}
    cfg = _catalog(tmp_path, "contracts-panic", [TINY, two])
    with operation(cfg):
        assert build.run_one(TINY, strict=False) and build.run_one(two, strict=False)

        class FakePanic(BaseException):
            pass
        cls = type(get_exporter("vortex@py"))

        def panic(self, spec, canonical, dest=None):
            raise FakePanic("native panic")
        monkeypatch.setattr(cls, "export", panic)
        before = prepared_parquet("two").stat().st_mtime_ns
        # The panic is tiny's Vortex writer's, and the run goes on. The file
        # its Publication put back was made from this canonical: it stays.
        vortex = _record(cfg)[artifact_key("tiny", "vortex", 2)]
        assert export_main(["--all"]) == 0
        assert prepared_parquet("two").stat().st_mtime_ns != before
        assert _record(cfg)[artifact_key("tiny", "vortex", 2)]["sha256"] == vortex["sha256"]


# ---------- build ----------

def test_a_missing_writer_fails_before_fetching_with_the_extra_named(tmp_path, stages, monkeypatch, capsys):
    cfg = _catalog(tmp_path, "contracts-noreader", [TINY])
    from raincloud.pipeline.export import _EXPORTERS
    # Every Vortex writer, so an installed vortex@rs or vortex@jni sidecar
    # cannot stand in for the missing library.
    for cell, exporter in _EXPORTERS.items():
        if cell.startswith("vortex@"):
            monkeypatch.setattr(exporter, "unavailable",
                                lambda: "needs vortex-data; install `raincloud[vortex]`")
    with operation(cfg):
        assert not build.run_one(TINY, strict=False)
    assert stages == []
    out = capsys.readouterr()
    assert "no installed writer for 'vortex'" in out.out and "raincloud[vortex]" in out.out
    assert "Traceback" not in out.out + out.err


def test_clean_workdir_failure_is_reported_not_claimed(tmp_path, stages, monkeypatch, capsys):
    cfg = _catalog(tmp_path, "contracts-clean", [TINY])

    def refuse(path, *a, **kw):
        raise PermissionError(13, "denied", str(path))
    monkeypatch.setattr(build.shutil, "rmtree", refuse)
    monkeypatch.setattr(build, "workdir_root", lambda: tmp_path / "work")
    (tmp_path / "work" / "tiny").mkdir(parents=True)
    with operation(cfg):
        assert build.run_one(TINY, strict=False, clean_workdir=True)
    out = capsys.readouterr()
    assert "could not remove" in out.err and "[clean] removed" not in out.out


# ---------- compliance ----------

def _plant(path, rows):
    pq.write_table(pa.table({"x": list(range(rows)), "url": [None] * rows}), path)
    os.utime(path, None)


def test_compliance_trusts_the_build_record_over_the_catalog_writer(tmp_path, stages):
    # The catalog says py wrote the store file; this install's record says rs.
    cfg = _catalog(tmp_path, "contracts-writer", [TINY])
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        parquet = prepared_parquet("tiny")
        key = artifact_key("tiny", "parquet", 2)
        _builds.record(cfg.data_dir, {key: {**_record(cfg)[key], "writer": "rs"}})
        py = get_exporter("parquet@py")
        assert compliance._written_by(py, "tiny") is None
        report = compliance.run_compliance(TINY, cells=["parquet@py"], reader_ids=["parquet@py"])
        assert report.write_results[0].out_path == compliance.compliance_path(py, "tiny")
        # The inverse: the record says py, so the store file is reused as py's.
        _builds.record(cfg.data_dir, {key: {**_record(cfg)[key], "writer": "py"}})
        assert compliance._written_by(py, "tiny") == parquet
        # A file the record does not describe (another size) is never adopted.
        _plant(parquet, 5)
        assert compliance._written_by(py, "tiny") is None


def test_compliance_writes_scratch_under_every_resource_lock(store, monkeypatch):
    fcntl = pytest.importorskip("fcntl")
    cfg = store
    cls = type(get_exporter("parquet@py"))
    real = cls.export

    def checked(self, spec, canonical, dest=None):
        for root in (cfg.data_dir, cfg.raw_dir, cfg.scratch_dir):
            with (root / ".raincloud-write.lock").open("a+b") as stream:
                with pytest.raises(BlockingIOError):
                    fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return real(self, spec, canonical, dest)
    monkeypatch.setattr(cls, "export", checked)
    compliance.run_compliance(TINY, cells=["parquet@py"], reader_ids=["parquet@py"], reencode=True)


def test_write_panic_with_the_real_signature_is_a_measured_failure(store, monkeypatch):
    class FakePanic(BaseException):
        pass

    def boom(self, spec, canonical, dest=None):
        raise FakePanic("vortex panicked")
    monkeypatch.setattr(type(get_exporter("vortex@py")), "export", boom)
    report = compliance.run_compliance(TINY, cells=["vortex@py"], reader_ids=["vortex@py"], reencode=True)
    (result,) = report.write_results
    assert result.compliance.roundtrip is False and "vortex panicked" in result.compliance.note


def test_a_failed_sidecar_write_leaves_its_stale_scratch_file_unread(store, tmp_path, monkeypatch):
    fail = tmp_path / "fail_writer.py"
    fail.write_text(f"#!{sys.executable}\nimport sys\nsys.exit(1)\n")
    fail.chmod(0o755)
    monkeypatch.setenv("RAINCLOUD_SIDECAR_PARQUET_HARDWOOD", str(fail))
    stale = compliance.compliance_path(get_exporter("parquet@hardwood"), "tiny")
    stale.parent.mkdir(parents=True, exist_ok=True)
    _plant(stale, 2)
    assert stale.exists()
    report = compliance.run_compliance(TINY, cells=["parquet@hardwood"], reader_ids=["parquet@py"])
    assert report.write_results[0].compliance.roundtrip is False
    assert report.read_results == []


# ---------- convert ----------

def test_convert_records_what_it_writes_so_the_loader_serves_it(store, monkeypatch):
    cfg = store
    vortex = prepared_vortex("tiny")
    vortex.write_bytes(b"not the file the build recorded")  # e.g. an older encoder's output
    os.utime(vortex, ns=(1, 1))
    assert convert.convert(TINY) == vortex
    entry = _record(cfg)[artifact_key("tiny", "vortex", 2)]
    assert entry["bytes"] == vortex.stat().st_size and entry["writer"] == "py"
    assert raincloud.load("tiny", format="vortex", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]
    mtime = vortex.stat().st_mtime_ns
    assert convert.convert(TINY) == vortex  # recorded, fresh: reused
    assert vortex.stat().st_mtime_ns == mtime


# ---------- hydrate ----------

def _fake_fetch(monkeypatch):
    fetched = []

    def fetch(url, config):
        fetched.append(url)
        return b"page", {"http_status": 200, "content_type": "text/html", "fetched_at": None,
                         "sha256": b"", "bytes_total": 4, "filter_decision": "allowed", "error": None}
    monkeypatch.setattr(hydrate, "http_fetch", fetch)
    return fetched


def test_hydrate_limit_writes_a_sample_and_never_the_dataset(store, monkeypatch):
    cfg = store
    _fake_fetch(monkeypatch)
    recorded = _record(cfg)
    assert hydrate.main(["tiny", "--limit", "1"]) == 0  # the bare parent names tiny-hydrated
    sample = cfg.scratch_dir / "tiny-hydrated" / "sample" / "tiny-hydrated.arrow.zstd"
    with pa.ipc.open_file(sample) as reader:
        assert reader.read_all().num_rows == 1  # the CLI's config reached derive_tables
    assert not prepared_arrow("tiny-hydrated").exists()
    assert _record(cfg) == recorded
    with pytest.raises((ArtifactNotFound, OfflineMiss)), pytest.warns(raincloud.HydratedDatasetWarning):
        raincloud.load("tiny-hydrated", format="arrow", config=cfg).path()


def test_hydrate_default_run_builds_the_dataset(store, monkeypatch):
    cfg = store
    assert _fake_fetch(monkeypatch) == []
    assert hydrate.main(["tiny-hydrated"]) == 0
    with pytest.warns(raincloud.HydratedDatasetWarning):
        table = raincloud.load("tiny-hydrated", format="arrow", config=cfg).to_arrow()
    assert table["content"].to_pylist() == [b"page", None]


def test_hydrate_rejects_plain_datasets_abbreviations_and_lone_bypass(store):
    with pytest.raises(SystemExit) as exc:
        hydrate.main(["other"])
    assert exc.value.code == 2
    with pytest.raises(SystemExit):
        hydrate.main(["tiny", "--unsafe", "--i"])
    with pytest.raises(ValueError, match="risk_accepted"):
        hydrate.HydrateConfig(bypass_safety=True)


# ---------- lifecycle guards ----------

def test_build_outputs_guards(store, tmp_path):
    outputs = BuildOutputs(TINY)
    with pytest.raises(ValueError, match="unsafe output slug"):
        outputs.preflight("../x")
    # A multi-output producer may not publish a declared slug under its recipe.
    with pytest.raises(CatalogConflict):
        outputs.preflight("other")
    path = tmp_path / "canonical.arrow.zstd"
    path.write_bytes(b"old")
    with build_outputs(TINY) as state:
        state.prepare_canonical(path)
        with pytest.raises(RuntimeError, match="published twice"):
            state.prepare_canonical(path)
        replacement = tmp_path / "replacement"
        replacement.write_bytes(b"new, never accepted")
        replacement.replace(path)  # writers replace by rename
    assert path.read_bytes() == b"old"  # rolled back on exit


def test_resource_locks_cannot_be_taken_inside_an_operation(store):
    with operation_lock():
        with pytest.raises(RuntimeError, match="outer operation boundary"):
            with operation_lock(resources=True):
                pass


# ---------- validate ----------

def test_validate_reads_only_canonical_arrow(store, tmp_path, capsys):
    (result,) = validate({"expect": {"rows": 2}}, [prepared_arrow("tiny")])
    assert result["rows_actual"] == 2 and result["rows_ok"]
    with pytest.raises(ValueError, match="canonical Arrow"):
        validate({}, [prepared_parquet("tiny")])


# ---------- docs snapshot preservation ----------

def _docs_env(tmp_path, monkeypatch, version):
    monkeypatch.setattr(docs, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(docs, "SNAPSHOT_JSON", tmp_path / "docs" / "snapshot.json")
    monkeypatch.setattr(docs, "load_manifest", lambda: {"schema_version": version, "datasets": [{"slug": "kept"}]})
    monkeypatch.setattr(docs, "prepared_parquet", lambda slug: tmp_path / "missing.parquet")
    monkeypatch.setattr(docs, "prepared_vortex", lambda slug: tmp_path / "missing.vortex")
    monkeypatch.setattr(docs, "prepared_artifact", _artifacts(tmp_path, {"parquet": tmp_path / "missing.parquet", "vortex": tmp_path / "missing.vortex", "arrow": tmp_path / "missing.arrow.zstd"}))
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    (tmp_path / "docs" / f"v{version}").mkdir(parents=True)


def test_snapshot_regen_refuses_when_no_candidate_parses(tmp_path, monkeypatch, capsys):
    _docs_env(tmp_path, monkeypatch, 2)
    (tmp_path / "docs" / "snapshot.json").write_text("{not json")
    (tmp_path / "docs" / "v2" / "snapshot.json").write_text("[truncated")
    dest = tmp_path / "out.json"
    with pytest.raises(RuntimeError, match="no readable snapshot"):
        docs.generate_snapshot(destination=dest)
    assert not dest.exists()
    assert "unreadable snapshot" in capsys.readouterr().err


def test_snapshot_regen_skips_a_scratch_copy_of_another_version(tmp_path, monkeypatch, capsys):
    _docs_env(tmp_path, monkeypatch, 2)
    scratch = tmp_path / "docs" / "snapshot.json"
    scratch.write_text(json.dumps({"schema_version": 1, "slugs": {"kept": {"parquet_bytes": 1}}}))
    (tmp_path / "docs" / "v2" / "snapshot.json").write_text(
        json.dumps({"schema_version": 2, "slugs": {"kept": {"parquet_bytes": 2}}}))
    docs.generate_snapshot(destination=scratch)
    assert json.loads(scratch.read_text())["slugs"]["kept"]["parquet_bytes"] == 2
    assert "schema_version 1" in capsys.readouterr().err


# ---------- Parquet row-group sizing ----------

def _canonical_file(tmp_path, table, batch_rows):
    path = tmp_path / "c" / "arrow" / "c.arrow.zstd"
    path.parent.mkdir(parents=True)
    with pa.ipc.new_file(path, table.schema) as writer:
        for batch in table.to_batches(batch_rows):
            writer.write_batch(batch)
    return path


def test_row_group_second_pass_rules(tmp_path):
    table = pa.table({"n": pa.array(range(4000), type=pa.int64())})
    canonical_path = _canonical_file(tmp_path, table, 100)
    out = tmp_path / "out.parquet"
    # Rows closed every group: not byte-bound.
    assert exporters_mod._write_parquet(canonical_path, out, 1000, 1 << 40, options=ParquetOptions()) is False
    assert pq.ParquetFile(out).metadata.num_row_groups == 4
    # The decoded-byte ceiling closed the groups first.
    assert exporters_mod._write_parquet(canonical_path, out, 4000, 1000, options=ParquetOptions()) is True
    one = tmp_path / "one.parquet"
    pq.write_table(table, one)
    assert exporters_mod._corrected_rows(one, 4000, 10, 1 << 30) is None  # < 2 groups
    exporters_mod._write_parquet(canonical_path, out, 1000, 1 << 40, options=ParquetOptions())
    median = sorted(pq.ParquetFile(out).metadata.row_group(i).total_byte_size for i in range(4))[2]
    assert exporters_mod._corrected_rows(out, 1000, median, 1 << 30) is None  # on target
    assert exporters_mod._corrected_rows(out, 1000, median * 2, 1 << 30) == pytest.approx(2000, rel=0.02)
    assert exporters_mod._corrected_rows(out, 1000, median * 2, 1000) is None  # capped: unchanged


def test_probe_converges_on_a_uniform_table(tmp_path, monkeypatch):
    table = pa.table({"n": pa.array(range(200_000), type=pa.int64())})
    canonical_path = _canonical_file(tmp_path, table, 10_000)
    probe = tmp_path / "probe.parquet"
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_PROBE_ROWS", "10000")
    rows, encoded = exporters_mod._probe_encoded(canonical_path, 200_000, probe, options=ParquetOptions())
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES", str(encoded // 4))
    want = exporters_mod._rows_for_encoded_target(canonical_path, probe, options=ParquetOptions(),
                                                  row_cap=1 << 30)
    assert 0.5 * rows / 4 < want < 2 * rows / 4
    assert not probe.exists()


# ---------- batch planning ----------

def test_planning_decodes_binary_only_until_its_sample_is_decided(tmp_path):
    from raincloud.pipeline.batch_types import UTF8_SAMPLE_SIZE, tighten_stream
    from raincloud.pipeline.batches import BatchStream, SourceBatch

    table = pa.table({"n": pa.array(range(10_000), type=pa.int64()),
                      "b": pa.array([b"text"] * 10_000, type=pa.binary())})
    reads = []

    def make(columns):
        view = table if columns is None else table.select(columns)

        def batches():
            for batch in view.to_batches(1000):
                reads.append(tuple(view.column_names))
                yield SourceBatch(tmp_path, 0, batch)
        return BatchStream(view.schema, batches, make)

    planned = tighten_stream(make(None), [])
    assert planned.schema.field("n").type == pa.uint16()
    assert planned.schema.field("b").type == pa.string()
    assert reads.count(("n",)) == 10  # the statistics pass reads no payload
    assert reads.count(("b",)) == -(-UTF8_SAMPLE_SIZE // 1000)  # and the sample stops early


def test_batch_limits_name_a_malformed_variable(monkeypatch):
    from raincloud.pipeline.batches import BatchLimits
    monkeypatch.setenv("RAINCLOUD_BATCH_BYTES", "16MiB")
    with pytest.raises(ValueError, match="RAINCLOUD_BATCH_BYTES"):
        BatchLimits.from_env()
    monkeypatch.setenv("RAINCLOUD_BATCH_BYTES", "")
    assert BatchLimits.from_env() == BatchLimits()


def test_a_dictionary_larger_than_the_target_does_not_split_to_single_rows():
    from raincloud.pipeline.batches import BatchLimits, split_batch
    dictionary = pa.array([f"value-{i:06d}" * 10 for i in range(20_000)])
    column = pa.DictionaryArray.from_arrays(pa.array([i % 20_000 for i in range(1000)], type=pa.int32()), dictionary)
    batch = pa.record_batch([column], names=["d"])
    parts = list(split_batch(batch, BatchLimits(rows=4096, target_bytes=64 * 1024)))
    assert sum(p.num_rows for p in parts) == 1000
    assert len(parts) < 10


# ---------- build record: canonical and exports agree ----------

def test_a_rebuilt_canonical_supersedes_an_export_its_writer_failed_to_redo(store, stages, monkeypatch):
    cfg = store
    assert raincloud.load("tiny", format="vortex", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]
    # The same recipe over a refetched rolling upstream: new rows, and a Vortex
    # writer that fails, so its Publication puts the old file back.
    monkeypatch.setattr(build, "transform", lambda spec, tables: [(spec["slug"], pa.table(
        {"x": [7, 8, 9], "url": [None, None, None]}))])
    monkeypatch.setattr(type(get_exporter("vortex@py")), "export", lambda self, spec, canonical, dest=None: 1 / 0)
    assert build.run_one(TINY, strict=False)
    recorded = _record(cfg)
    # The restored file was made from the old canonical, so it is not kept: the
    # entry is this attempt's measurement, against the new canonical.
    vortex_entry = recorded[artifact_key("tiny", "vortex", 2)]
    assert "ZeroDivisionError" in vortex_entry["unavailable"]["error"]
    assert vortex_entry["canonical_sha256"] == recorded[artifact_key("tiny", "arrow", 2)]["sha256"]
    assert recorded[artifact_key("tiny", "parquet", 2)]["canonical_sha256"] == \
        recorded[artifact_key("tiny", "arrow", 2)]["sha256"]
    for fmt in ("arrow", "parquet"):
        assert raincloud.load("tiny", format=fmt, config=cfg).to_arrow()["x"].to_pylist() == [7, 8, 9]
    with pytest.raises(raincloud.FormatUnavailable, match="ZeroDivisionError"):
        raincloud.load("tiny", format="vortex", config=cfg).to_arrow()
    # Compliance does not adopt the superseded file as vortex@py's output.
    assert compliance._written_by(get_exporter("vortex@py"), "tiny") is None


# ---------- sidecar reports ----------

_NULL_REPORT = """
import argparse, json, os
import pyarrow as pa, pyarrow.parquet as pq
ap = argparse.ArgumentParser()
for flag in ("--input", "--output", "--report"):
    ap.add_argument(flag)
a = ap.parse_args()
with pa.ipc.open_file(a.input) as r:
    pq.write_table(r.read_all(), a.output)
json.dump({"roundtrip": None, "variant_faithful": True,
           "note": os.environ.get("RAINCLOUD_ROW_GROUP_MAX_ROWS", "unset")}, open(a.report, "w"))
"""


def _mock_sidecar(tmp_path, monkeypatch, body, var="RAINCLOUD_SIDECAR_PARQUET_RS"):
    mock = tmp_path / "mock_writer.py"
    mock.write_text(f"#!{sys.executable}\n" + textwrap.dedent(body))
    mock.chmod(0o755)
    monkeypatch.setenv(var, str(mock))
    return mock


def test_an_unmeasured_sidecar_report_is_promoted_and_recorded(store, tmp_path, monkeypatch):
    from raincloud.pipeline import export as export_pkg
    cfg = store
    _mock_sidecar(tmp_path, monkeypatch, _NULL_REPORT)
    result = get_exporter("parquet@rs").export(TINY, prepared_arrow("tiny"), dest=tmp_path / "direct.parquet")
    assert result.compliance.roundtrip is None and (tmp_path / "direct.parquet").is_file()
    accepted = []
    (result,) = export_pkg.run_exporters(TINY, prepared_arrow("tiny"), ["parquet@rs"], on_accept=accepted.append)
    assert accepted == [result] and result.compliance.roundtrip is None
    assert pq.read_table(prepared_parquet("tiny"))["x"].to_pylist() == [1, 2]
    assert export_main(["tiny", "--format", "parquet@rs"]) == 0
    assert _record(cfg)[artifact_key("tiny", "parquet", 2)]["writer"] == "rs"
    # Compliance measures what the sidecar left unmeasured, from the self-read.
    self_read = ReadResult("parquet@rs", "parquet@rs", Verdict("pass"))
    (filled,) = compliance._backfill_self_roundtrip([result], [self_read])
    assert filled.compliance.roundtrip is True


def test_a_sidecar_report_without_roundtrip_is_a_bad_report(tmp_path):
    from raincloud.pipeline.export.sidecar import _read_report
    report = tmp_path / "r.json"
    report.write_text(json.dumps({"variant_faithful": True, "note": ""}))
    assert _read_report(report) is None
    report.write_text(json.dumps({"roundtrip": None, "variant_faithful": True, "note": ""}))
    assert _read_report(report) == (None, True, "")


def test_the_recipe_row_cap_reaches_the_sidecar_and_parquet_py(store, tmp_path, monkeypatch):
    capped = {**TINY, "write": {"row_group_size_rows": 1}}
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_MAX_ROWS", "1e3")
    _mock_sidecar(tmp_path, monkeypatch, _NULL_REPORT)
    result = get_exporter("parquet@rs").export(capped, prepared_arrow("tiny"), dest=tmp_path / "rs.parquet")
    assert result.compliance.note == "1"
    uncapped = get_exporter("parquet@rs").export(TINY, prepared_arrow("tiny"), dest=tmp_path / "rs2.parquet")
    assert uncapped.compliance.note == "1e3"  # the environment's own value, untouched
    get_exporter("parquet@py").export(capped, prepared_arrow("tiny"), dest=tmp_path / "py.parquet")
    meta = pq.ParquetFile(tmp_path / "py.parquet").metadata
    assert max(meta.row_group(i).num_rows for i in range(meta.num_row_groups)) == 1


# ---------- export / convert over each canonical status ----------

def test_export_of_a_named_cell_that_is_not_installed_fails(store, monkeypatch, capsys):
    monkeypatch.delenv("RAINCLOUD_SIDECAR_PARQUET_RS", raising=False)
    monkeypatch.setenv("PATH", "")
    assert export_main(["tiny", "--format", "parquet@rs"]) == 1
    assert "[failed] tiny: nothing exported" in capsys.readouterr().err
    # One named cell missing beside one that ran still fails the request.
    assert export_main(["tiny", "--format", "parquet@rs", "--format", "vortex"]) == 1
    assert "only partly exported" in capsys.readouterr().err


def test_convert_refuses_a_canonical_from_an_earlier_transform(store, tmp_path):
    vortex = prepared_vortex("tiny")
    before = vortex.read_bytes(), vortex.stat().st_mtime_ns
    changed = {**TINY, "transform": {"handler": "identity"}}
    with operation(_catalog(tmp_path, "contracts-transform", [changed])):
        with pytest.raises(RuntimeError, match="earlier recipe"):
            convert.convert(changed)
        assert convert.main(["tiny"]) == 1
    assert (vortex.read_bytes(), vortex.stat().st_mtime_ns) == before


def test_convert_after_a_write_only_change_records_vortex_under_the_current_recipe(store, tmp_path):
    from raincloud._bundle import recipe_hash
    changed = {**TINY, "write": {"compression": "snappy"}}
    cfg = _catalog(tmp_path, "contracts-convert-write", [changed])
    with operation(cfg):
        os.utime(prepared_vortex("tiny"), ns=(1, 1))  # older than the canonical: not reusable
        assert convert.convert(changed) == prepared_vortex("tiny")
        recorded = _record(cfg)
        for fmt in ("arrow", "vortex"):
            assert recorded[artifact_key("tiny", fmt, 2)]["recipe"] == recipe_hash(changed, 2, specs={"tiny": changed})
        assert raincloud.load("tiny", format="vortex", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def _unrecorded_canonical(tmp_path, name, slugs=None):
    cfg = _catalog(tmp_path, name, [TINY], slugs=slugs)
    with operation(cfg):
        path = prepared_arrow("tiny")
        path.parent.mkdir(parents=True)
        with pa.ipc.new_file(path, TABLE.schema) as writer:
            writer.write_table(TABLE)
    return cfg, path


def test_export_and_convert_refuse_an_unknown_canonical_and_record_nothing(tmp_path, capsys):
    cfg, _ = _unrecorded_canonical(tmp_path, "contracts-unknown")
    with operation(cfg):
        assert export_main(["tiny"]) == 1
        assert "neither this install's build nor the catalog's file" in capsys.readouterr().err
        with pytest.raises(RuntimeError, match="neither this install's build"):
            convert.convert(TINY)
        assert not prepared_parquet("tiny").exists() and not prepared_vortex("tiny").exists()
    assert _record(cfg) == {}


def test_export_of_the_catalogs_canonical_records_its_exports(tmp_path):
    from raincloud.pipeline import records
    sink = pa.BufferOutputStream()
    with pa.ipc.new_file(sink, TABLE.schema) as writer:
        writer.write_table(TABLE)
    size = sink.getvalue().size
    cfg, path = _unrecorded_canonical(tmp_path, "contracts-catalog", slugs={"tiny": {"arrow_bytes": size}})
    with operation(cfg):
        assert records.canonical_status(path) == "catalog"
        assert export_main(["tiny"]) == 0
        recorded = _record(cfg)
        assert artifact_key("tiny", "arrow", 2) not in recorded
        assert recorded[artifact_key("tiny", "parquet", 2)]["writer"] == "py"
        assert raincloud.load("tiny", format="parquet", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


# ---------- the oracle gate's scope ----------

def _seeded_oracle(tmp_path, extra_row):
    seed = tmp_path / "seed.json"
    assert compliance.main(["tiny", "--cells", "parquet@py", "--readers", "parquet@py",
                            "--write-ledger", str(seed)]) == 0
    oracle = json.loads(seed.read_text())
    oracle["slugs"]["tiny"]["read"].append(extra_row)
    path = tmp_path / "oracle.json"
    path.write_text(json.dumps(oracle))
    return path


def test_a_full_run_reads_an_unregistered_reader_as_removed(store, tmp_path, capsys):
    oracle = _seeded_oracle(tmp_path, {"artifact_cell": "parquet@py", "reader_id": "gone@x", "status": "pass"})
    assert compliance.main(["tiny", "--check-oracle", str(oracle)]) == 1
    assert "gone@x" in capsys.readouterr().err
    # An explicit subset still narrows the gate.
    assert compliance.main(["tiny", "--cells", "parquet@py", "--readers", "parquet@py",
                            "--check-oracle", str(oracle)]) == 0


def test_a_full_run_ledger_records_no_scope_narrowing(store, tmp_path):
    from raincloud.pipeline import ledger
    out = tmp_path / "full.json"
    compliance.main(["tiny", "--write-ledger", str(out)])
    written = json.loads(out.read_text())
    assert written["scope"]["requested_cells"] is None and written["scope"]["requested_readers"] is None
    oracle = json.loads(out.read_text())
    oracle["slugs"]["tiny"]["read"].append({"artifact_cell": "parquet@py", "reader_id": "gone@x", "status": "pass"})
    assert ledger.diff_against_oracle(written, oracle).removed == [("tiny", "parquet@py", "gone@x")]


def test_hydrate_timeout_writes_a_sample_and_never_the_dataset(store, monkeypatch):
    cfg = store
    _fake_fetch(monkeypatch)
    recorded = _record(cfg)
    assert hydrate.main(["tiny-hydrated", "--timeout", "5"]) == 0
    assert (cfg.scratch_dir / "tiny-hydrated" / "sample" / "tiny-hydrated.arrow.zstd").is_file()
    assert not prepared_arrow("tiny-hydrated").exists()
    assert _record(cfg) == recorded


# ---------- Parquet row groups: one heavy region ----------

def test_a_byte_heavy_region_does_not_cancel_the_second_pass(tmp_path):
    light = pa.table({"s": pa.array(["x"] * 3000)})
    heavy = pa.table({"s": pa.array(["y" * 4000] * 100)})
    table = pa.concat_tables([light, heavy, light])
    canonical_path = _canonical_file(tmp_path, table, 100)
    out = tmp_path / "out.parquet"
    # The ceiling closes the heavy region's groups; the light ones close on rows.
    assert exporters_mod._write_parquet(canonical_path, out, 1000, 100_000, options=ParquetOptions()) is False
    assert exporters_mod._write_parquet(canonical_path, out, 1000, 1000, options=ParquetOptions()) is True


def test_a_nested_dictionary_larger_than_the_target_does_not_split_to_single_rows():
    from raincloud.pipeline.batches import BatchLimits, split_batch
    dictionary = pa.array([f"value-{i:06d}" * 10 for i in range(20_000)])
    column = pa.DictionaryArray.from_arrays(pa.array([i % 20_000 for i in range(1000)], type=pa.int32()), dictionary)
    struct = pa.StructArray.from_arrays([column, pa.array(range(1000))], names=["d", "n"])
    batch = pa.record_batch([struct], names=["s"])
    parts = list(split_batch(batch, BatchLimits(rows=4096, target_bytes=64 * 1024)))
    assert sum(p.num_rows for p in parts) == 1000
    assert len(parts) < 10


def test_comparator_windows_walk_differing_layouts_and_empty_chunks():
    got = pa.chunked_array([[], ["a", "b"], [], ["c", "d", "e"], []], type=pa.string())
    expected = pa.chunked_array([["a"], [], ["b", "c", "d"], ["e"]], type=pa.string())
    windows = list(compare._windows(got, expected))
    assert [len(g) for g, _ in windows] == [1, 1, 2, 1]
    assert all(isinstance(g, pa.Array) and isinstance(e, pa.Array) and g.equals(e) for g, e in windows)
    assert list(compare._windows(pa.chunked_array([], pa.int64()), pa.chunked_array([[]], pa.int64()))) == []
    assert compare.values_equal(pa.table({"s": got}), pa.table({"s": expected})) == (True, "")


def test_snapshot_regen_describes_a_multi_batch_ipc_canonical(tmp_path, monkeypatch):
    _docs_env(tmp_path, monkeypatch, 2)
    arrow = tmp_path / "kept.arrow.zstd"
    table = pa.table({"x": list(range(10))})
    with pa.ipc.new_file(arrow, table.schema, options=pa.ipc.IpcWriteOptions(compression="zstd")) as writer:
        for batch in table.to_batches(3):
            writer.write_batch(batch)
    monkeypatch.setattr(docs, "prepared_artifact", _artifacts(tmp_path, {"arrow": arrow}))
    dest = tmp_path / "out.json"
    docs.generate_snapshot(destination=dest)
    entry = json.loads(dest.read_text())["slugs"]["kept"]
    assert entry["last_built_rows"] == 10 and entry["last_built_row_groups"] is None
    assert entry["columns"] == [{"name": "x", "type": "int64"}] and "columns_error" not in entry


def test_snapshot_regen_refuses_a_preserved_nan_before_writing(tmp_path, monkeypatch):
    _docs_env(tmp_path, monkeypatch, 2)
    (tmp_path / "docs" / "v2" / "snapshot.json").write_text(
        '{"schema_version": 2, "slugs": {"kept": {"parquet_bytes": NaN}}}')
    dest = tmp_path / "out.json"
    with pytest.raises(ValueError, match="JSON compliant"):
        docs.generate_snapshot(destination=dest)
    assert not dest.exists()


def test_snapshot_regen_refuses_an_unreadable_candidate_beside_a_skipped_one(tmp_path, monkeypatch):
    _docs_env(tmp_path, monkeypatch, 2)
    (tmp_path / "docs" / "snapshot.json").write_text(json.dumps({"schema_version": 1, "slugs": {"kept": {}}}))
    (tmp_path / "docs" / "v2" / "snapshot.json").write_text("<<<<<<< HEAD")
    dest = tmp_path / "docs" / "snapshot.json"
    with pytest.raises(RuntimeError, match="no readable snapshot"):
        docs.generate_snapshot(destination=dest)
