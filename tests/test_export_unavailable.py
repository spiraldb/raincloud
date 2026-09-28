# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Measured opt-outs: a writer that cannot produce a format is recorded,
bounded in time, carried into the catalog, reported by the loader, and
announced as stale once a writer round-trips it again."""
from __future__ import annotations

import json
import os
import signal
import time

import pytest

import raincloud
from raincloud import _builds, cli
from raincloud._bundle import recipe_hash
from raincloud._resolve import artifact_key
from raincloud.catalogs import operation
from raincloud.config import use_config
from raincloud.exceptions import FormatUnavailable
from raincloud.pipeline import build, compliance, docs, ledger, list_datasets, status
from raincloud.pipeline.export import ExportFailed, export_timeout, get_exporter, run_bounded, run_exporters
from raincloud.pipeline.export.__main__ import main as export_main
from raincloud.pipeline.spec import prepared_arrow, prepared_parquet, prepared_vortex
from tests.test_pipeline_contracts import TINY, _catalog
from tests.test_pipeline_contracts import stages as stages  # noqa: F401 (fixture)

pytest.importorskip("vortex")
VORTEX = type(get_exporter("vortex@py"))


def _record(cfg):
    return _builds.read(cfg.data_dir)


def _vortex_entry(cfg):
    return _record(cfg)[artifact_key("tiny", "vortex", 2)]


def _broken_vortex(monkeypatch, message="not implemented"):
    class FakePanic(BaseException):
        """Stand-in for pyo3_runtime.PanicException."""

    def panic(self, spec, canonical, dest=None):
        raise FakePanic(message)
    monkeypatch.setattr(VORTEX, "export", panic)


def _measurement(error="FakePanic: not implemented"):
    return {"cell": "vortex@py", "error": error,
            "toolchain": {"python": "3.11.0", "vortex-data": "0.69.0", "pyarrow": "18.1.0"},
            "recipe": recipe_hash(TINY, 2, specs={"tiny": TINY}), "canonical_sha256": "0" * 64,
            "measured_at": "2026-01-01T00:00:00Z"}


# ---------- build: record and continue ----------

def test_a_failing_writer_is_recorded_and_the_build_succeeds(tmp_path, stages, monkeypatch, capsys):
    cfg = _catalog(tmp_path, "unavailable-fail", [TINY])
    _broken_vortex(monkeypatch)
    with operation(cfg):
        assert build._main(["tiny"]) == 0
    out = capsys.readouterr()
    assert "unavailable=1" in out.out and "[unavailable] tiny/vortex: vortex@py: FakePanic: not implemented" in out.out
    entry = _vortex_entry(cfg)
    measured = entry["unavailable"]
    assert entry["recipe"] == measured["recipe"] == recipe_hash(TINY, 2, specs={"tiny": TINY})
    assert measured["cell"] == "vortex@py" and "FakePanic: not implemented" in measured["error"]
    assert {"vortex-data", "pyarrow"} <= set(measured["toolchain"])
    assert measured["canonical_sha256"] == _record(cfg)[artifact_key("tiny", "arrow", 2)]["sha256"]
    assert measured["measured_at"].endswith("Z") and "sha256" not in entry
    with operation(cfg):
        assert prepared_parquet("tiny").is_file() and not prepared_vortex("tiny").exists()
        assert not list(prepared_vortex("tiny").parent.glob(".*"))


def test_recorded_error_text_is_bounded_and_scrubbed(tmp_path, stages, monkeypatch):
    cfg = _catalog(tmp_path, "unavailable-long", [TINY])
    _broken_vortex(monkeypatch, f"failed at {tmp_path}/data/v2/tiny/x " + "backtrace line\n" * 500)
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
    error = _vortex_entry(cfg)["unavailable"]["error"]
    assert len(error) <= 500 and "\n" not in error and str(tmp_path) not in error


def test_a_hanging_writer_is_killed_by_the_time_limit(tmp_path, stages, monkeypatch):
    cfg = _catalog(tmp_path, "unavailable-hang", [TINY])
    monkeypatch.setenv("RAINCLOUD_EXPORT_TIMEOUT", "2")
    pids = tmp_path / "pids"

    def hang(self, spec, canonical, dest=None):
        pids.write_text(str(os.getpid()))
        time.sleep(600)
    monkeypatch.setattr(VORTEX, "export", hang)
    started = time.monotonic()
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        assert not list(prepared_vortex("tiny").parent.glob(".*"))
    assert time.monotonic() - started < 60
    assert "timed out after 2s (RAINCLOUD_EXPORT_TIMEOUT)" in _vortex_entry(cfg)["unavailable"]["error"]
    with pytest.raises(ProcessLookupError):
        os.kill(int(pids.read_text()), 0)  # the writer is gone, not orphaned


def test_a_writer_that_dies_is_a_measured_failure(tmp_path, stages, monkeypatch):
    cfg = _catalog(tmp_path, "unavailable-die", [TINY])
    monkeypatch.setattr(VORTEX, "export", lambda self, spec, canonical, dest=None: os.kill(os.getpid(), signal.SIGKILL))
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
    assert "was killed by SIGKILL (the kernel's out-of-memory killer, most likely) without a result" \
        in _vortex_entry(cfg)["unavailable"]["error"]


@pytest.mark.skipif(not os.path.exists("/proc/self/status"), reason="resident memory is read from /proc")
def test_a_writer_over_the_memory_ceiling_is_stopped_and_recorded(tmp_path, stages, monkeypatch):
    cfg = _catalog(tmp_path, "unavailable-memory", [TINY])
    monkeypatch.setenv("RAINCLOUD_EXPORT_MEMORY", str(400 * 2**20))
    pids = tmp_path / "pids"

    def balloon(self, spec, canonical, dest=None):
        pids.write_text(str(os.getpid()))
        held = []
        while True:  # touch every page, so it is resident
            held.append(bytearray(b"x" * (64 * 2**20)))
            time.sleep(0.05)
    monkeypatch.setattr(VORTEX, "export", balloon)
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        assert not list(prepared_vortex("tiny").parent.glob(".*"))
    error = _vortex_entry(cfg)["unavailable"]["error"]
    assert "GiB ceiling (RAINCLOUD_EXPORT_MEMORY)" in error
    with pytest.raises(ProcessLookupError):
        os.kill(int(pids.read_text()), 0)


@pytest.mark.skipif(not os.path.exists("/proc/self/oom_score_adj"), reason="Linux only")
def test_the_writer_child_is_the_first_oom_victim(tmp_path, stages, monkeypatch):
    cfg = _catalog(tmp_path, "unavailable-oom-score", [TINY])
    seen = tmp_path / "score"
    real = VORTEX.export

    def record_score(self, spec, canonical, dest=None):
        seen.write_text(open("/proc/self/oom_score_adj").read().strip())
        return real(self, spec, canonical, dest)
    monkeypatch.setattr(VORTEX, "export", record_score)
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
    assert seen.read_text() == "1000"
    assert open("/proc/self/oom_score_adj").read().strip() != "1000"  # the parent is not


@pytest.mark.parametrize("raw, value", [("0", None), ("1e9", 10**9)])
def test_the_export_memory_knob(monkeypatch, raw, value):
    from raincloud.pipeline.spec import export_memory
    monkeypatch.setenv("RAINCLOUD_EXPORT_MEMORY", raw)
    assert export_memory() == value


def test_the_export_memory_default_is_half_of_physical_memory(monkeypatch):
    from raincloud.pipeline.spec import export_memory
    monkeypatch.delenv("RAINCLOUD_EXPORT_MEMORY", raising=False)
    total = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    assert export_memory() == total // 2


def test_a_later_success_replaces_the_measurement(tmp_path, stages, monkeypatch):
    cfg = _catalog(tmp_path, "unavailable-heal", [TINY])
    with operation(cfg):
        with monkeypatch.context() as fault:
            _broken_vortex(fault)
            assert build.run_one(TINY, strict=False)
        assert "unavailable" in _vortex_entry(cfg)
        # The same writer and toolchain: only --retry-errors attempts it again.
        assert export_main(["tiny", "--retry-errors"]) == 0
        entry = _vortex_entry(cfg)
        assert "unavailable" not in entry and entry["sha256"] and entry["writer"] == "py"
        assert raincloud.load("tiny", format="vortex", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def test_export_cli_exit_status(tmp_path, stages, monkeypatch, capsys):
    cfg = _catalog(tmp_path, "unavailable-cli", [TINY])
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        prepared_vortex("tiny").unlink()
        _builds.forget(cfg.data_dir, artifact_key("tiny", "vortex", 2))
        _broken_vortex(monkeypatch)
        # The spec's own formats: recorded, and the slug still counts as exported.
        assert export_main(["tiny"]) == 0
        assert "unavailable" in _vortex_entry(cfg)
        # A bare --format asked for exactly that format: recorded, and the request failed.
        assert export_main(["tiny", "--format", "vortex"]) == 1
        assert "[failed] tiny: vortex not exported" in capsys.readouterr().err
        # A writer named outright is this run's choice: it fails, and records nothing.
        before = _vortex_entry(cfg)
        assert export_main(["tiny", "--format", "vortex@py"]) == 1
        assert _vortex_entry(cfg) == before


def test_a_named_writer_failure_raises_and_records_nothing(tmp_path, stages, monkeypatch):
    cfg = _catalog(tmp_path, "unavailable-named", [TINY])
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        good = _vortex_entry(cfg)
        _broken_vortex(monkeypatch)
        noted = []
        with pytest.raises(RuntimeError, match="vortex@py export failed"):
            run_exporters(TINY, prepared_arrow("tiny"), ["vortex@py"], on_unavailable=noted.append)
        assert noted == [] and _vortex_entry(cfg) == good


def test_a_restored_export_of_the_same_canonical_stays(tmp_path, stages, monkeypatch, capsys):
    cfg = _catalog(tmp_path, "unavailable-keep", [TINY])
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        good = _vortex_entry(cfg)
        _broken_vortex(monkeypatch)
        assert export_main(["tiny"]) == 0
        assert _vortex_entry(cfg)["sha256"] == good["sha256"]
        assert "this attempt is not recorded" in capsys.readouterr().out
        assert raincloud.load("tiny", format="vortex", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def test_run_bounded_reports_what_the_child_raised(tmp_path, monkeypatch):
    monkeypatch.setattr(VORTEX, "export", lambda self, spec, canonical, dest=None: 1 / 0)
    with pytest.raises(ExportFailed, match="vortex@py: ZeroDivisionError: division by zero"):
        run_bounded(get_exporter("vortex@py"), TINY, tmp_path / "missing.arrow.zstd", tmp_path / "out.vortex")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("raw, value", [(None, 21600.0), ("0", None), ("90", 90.0)])
def test_the_export_timeout_knob(monkeypatch, raw, value):
    if raw is None:
        monkeypatch.delenv("RAINCLOUD_EXPORT_TIMEOUT", raising=False)
    else:
        monkeypatch.setenv("RAINCLOUD_EXPORT_TIMEOUT", raw)
    assert export_timeout() == value


@pytest.mark.parametrize("bad", ["6h", "-1", "inf", "1_000", "0x10"])
def test_a_malformed_export_timeout_names_the_knob(monkeypatch, bad):
    monkeypatch.setenv("RAINCLOUD_EXPORT_TIMEOUT", bad)
    with pytest.raises(ValueError, match="RAINCLOUD_EXPORT_TIMEOUT"):
        export_timeout()


def test_a_malformed_export_timeout_fails_the_build_up_front(monkeypatch, capsys):
    monkeypatch.setenv("RAINCLOUD_EXPORT_TIMEOUT", "6h")
    with pytest.raises(SystemExit):
        build._main(["tiny"])
    assert "RAINCLOUD_EXPORT_TIMEOUT" in capsys.readouterr().err


# ---------- catalog: docs regen carries the measurement ----------

def test_snapshot_carries_the_measurement_and_drops_it_after_a_success(tmp_path, stages, monkeypatch, capsys):
    # datasets.md needs the display fields; they are not part of the recipe.
    named = {**TINY, "short_name": "Tiny", "full_name": "A tiny table", "description": "Two rows."}
    cfg = _catalog(tmp_path, "unavailable-docs", [named])
    destination = tmp_path / "snapshot.json"
    with operation(cfg):
        with monkeypatch.context() as fault:
            _broken_vortex(fault)
            assert build.run_one(TINY, strict=False)
        docs.generate_snapshot(destination=destination)
        entry = json.loads(destination.read_text())["slugs"]["tiny"]
        assert entry["vortex_bytes"] is None and entry["vortex_sha256"] is None and entry["vortex_writer"] is None
        assert entry["vortex_unavailable"] == _vortex_entry(cfg)["unavailable"]
        assert entry["parquet_sha256"] and "parquet_unavailable" not in entry
        assert "1 format(s) measured unavailable" in capsys.readouterr().out
        markdown = tmp_path / "datasets.md"
        docs.generate_datasets_md(destination=markdown, snapshot_path=destination)
        assert "| unavailable |" in markdown.read_text()

        assert export_main(["tiny", "--retry-errors"]) == 0
        docs.generate_snapshot(destination=destination)
        entry = json.loads(destination.read_text())["slugs"]["tiny"]
        assert entry["vortex_sha256"] and "vortex_unavailable" not in entry


def test_a_measurement_from_another_recipe_is_dropped(tmp_path):
    stale = {**_measurement(), "recipe": "f" * 64}
    kept = docs._without_stale_measurements({"vortex_unavailable": stale, "parquet_bytes": 3}, "e" * 64)
    assert kept == {"parquet_bytes": 3}


def test_docs_warns_when_the_ledger_round_trips_a_recorded_opt_out(tmp_path, capsys):
    cfg = _catalog(tmp_path, "unavailable-warn", [TINY])
    with operation(cfg):
        from raincloud.pipeline.spec import default_compliance_json
        path = default_compliance_json()
        path.parent.mkdir(parents=True)
        path.write_text(json.dumps({"generated_at": "2026-09-01T00:00:00Z", "slugs": {"tiny": {
            "write": [{"cell": "vortex@py", "roundtrip": True}], "read": [], "skipped_cells": []}}}))
        docs._warn_stale_opt_outs({"tiny": {"vortex_unavailable": _measurement()}},
                                  {"schema_version": 2, "datasets": [TINY]})
    err = capsys.readouterr().err
    assert "[stale opt-out] tiny/vortex: vortex@py round-trips" in err and "vortex-data 0.69.0" in err


# ---------- loader: describe, load and auto ----------

def _measured_catalog(tmp_path, name="unavailable-loader"):
    return _catalog(tmp_path, name, [TINY], {"tiny": {"vortex_unavailable": _measurement()}})


def test_describe_and_load_quote_the_catalogs_measurement(tmp_path):
    cfg = _measured_catalog(tmp_path)
    about = raincloud.describe("tiny", config=cfg)
    assert about["formats"]["vortex"]["unavailable"] == _measurement()
    assert "unavailable" not in about["formats"]["parquet"]
    with pytest.raises(FormatUnavailable) as error:
        raincloud.load("tiny", format="vortex", config=cfg)
    message = str(error.value)
    assert error.value.measurement == _measurement()
    assert "vortex@py" in message and "vortex-data 0.69.0" in message and "FakePanic: not implemented" in message
    assert "build" not in message.lower()
    assert raincloud.load("tiny", config=cfg).format == "parquet"  # auto skips it


def test_a_measurement_from_an_older_recipe_is_not_honoured(tmp_path):
    cfg = _catalog(tmp_path, "unavailable-old", [TINY], {"tiny": {"vortex_unavailable": {**_measurement(), "recipe": "a" * 64}}})
    assert "unavailable" not in raincloud.describe("tiny", config=cfg)["formats"]["vortex"]
    assert raincloud.load("tiny", config=cfg).format == "vortex"


def test_this_installs_measurement_is_reported_too(tmp_path, stages, monkeypatch):
    cfg = _catalog(tmp_path, "unavailable-local", [TINY])
    with operation(cfg):
        _broken_vortex(monkeypatch)
        assert build.run_one(TINY, strict=False)
    assert "FakePanic" in raincloud.describe("tiny", config=cfg)["formats"]["vortex"]["unavailable"]["error"]
    with pytest.raises(FormatUnavailable, match="FakePanic: not implemented"):
        raincloud.load("tiny", format="vortex", config=cfg)
    assert raincloud.load("tiny", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def test_a_catalog_measurement_is_overridden_by_this_installs_build(tmp_path, stages):
    cfg = _measured_catalog(tmp_path, "unavailable-override")
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
    assert "unavailable" not in raincloud.describe("tiny", config=cfg)["formats"]["vortex"]
    assert raincloud.load("tiny", format="vortex", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def test_a_malformed_measurement_is_a_catalog_error(tmp_path):
    cfg = _catalog(tmp_path, "unavailable-bad", [TINY], {"tiny": {"vortex_unavailable": "vortex is broken"}})
    with pytest.raises(raincloud.CatalogError, match="vortex_unavailable must be a measurement object"):
        raincloud.describe("tiny", config=cfg)


def test_cli_describe_and_load_report_it(tmp_path, capsys):
    cfg = _measured_catalog(tmp_path, "unavailable-cli-describe")
    settings = json.dumps({"no_config": True, "catalog": str(tmp_path / "unavailable-cli-describe"),
                           "data_dir": str(cfg.data_dir), "cache_dir": str(cfg.cache_dir),
                           "catalog_dir": str(tmp_path / "catalogs"), "offline": True})
    assert cli.main(["--json", "--settings", settings, "describe", "tiny"]) == 0
    about = json.loads(capsys.readouterr().out)
    assert about["formats"]["vortex"]["unavailable"]["cell"] == "vortex@py" and about["format"] == "parquet"
    assert cli.main(["--settings", settings, "describe", "tiny"]) == 0
    text = " ".join(capsys.readouterr().out.split())  # wrapped to the terminal
    assert "vortex unavailable" in text and "vortex: vortex@py could not write it (" in text
    assert "vortex-data 0.69.0" in text and "FakePanic: not implemented" in text
    assert cli.main(["--json", "--settings", settings, "load", "tiny", "--format", "vortex"]) == 1
    error = json.loads(capsys.readouterr().out)["error"]
    # Native clients classify by the MRO: FormatUnavailable is FORMAT_UNAVAILABLE.
    assert error["type"] == "FormatUnavailable" and "FormatUnavailable" in error["mro"]
    assert "FakePanic: not implemented" in error["message"]


# ---------- compliance: a stale opt-out is announced ----------

def test_compliance_announces_a_stale_opt_out(tmp_path, stages, capsys):
    cfg = _measured_catalog(tmp_path, "unavailable-stale")
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        sc = compliance.run_compliance(TINY, cells=["vortex@py"], reader_ids=["vortex@py"], reencode=True)
    assert sc.stale_opt_outs == [{"format": "vortex", "cells": ["vortex@py"], "catalog_recorded": _measurement()}]
    err = capsys.readouterr().err
    assert "[stale opt-out] tiny/vortex: vortex@py now round-trips (catalog recorded: vortex@py could not write it" in err

    report = compliance.ComplianceReport(slugs=[sc])
    path = tmp_path / "ledger.json"
    with operation(cfg):
        ledger.write_compliance_json(report, path, generated_at="2026-09-24T00:00:00Z")
    block = json.loads(path.read_text())["slugs"]["tiny"]
    assert block["stale_opt_outs"][0]["cells"] == ["vortex@py"]
    oracle = ledger.load_oracle(path)  # the gate reads it as an ordinary ledger
    with operation(cfg):
        assert ledger.oracle_gate(report, oracle) == (True, [])


def test_a_malformed_stale_opt_out_is_a_malformed_oracle(tmp_path):
    path = tmp_path / "oracle.json"
    path.write_text(json.dumps({"slugs": {"tiny": {"read": [], "write": [], "skipped_cells": [],
                                                   "stale_opt_outs": [{"format": "vortex"}]}}}))
    with pytest.raises(ledger.MalformedOracle, match="stale_opt_outs"):
        ledger.load_oracle(path)


def test_no_stale_opt_out_without_a_catalog_measurement(tmp_path, stages):
    cfg = _catalog(tmp_path, "unavailable-fresh", [TINY])
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        sc = compliance.run_compliance(TINY, cells=["vortex@py"], reader_ids=["vortex@py"])
    assert sc.stale_opt_outs == []


# ---------- listings ----------

def test_list_datasets_and_status_show_measured_unavailability(tmp_path, capsys):
    cfg = _measured_catalog(tmp_path, "unavailable-list")
    with use_config(cfg):
        assert list_datasets.main(["--no-vortex", "--json"]) == 0
    row = json.loads(capsys.readouterr().out)
    assert row["slug"] == "tiny" and row["vortex"] is False
    assert row["vortex_unavailable"] == _measurement()
    assert row["vortex_skip_reason"].startswith("vortex@py could not write it")
    with use_config(cfg):
        assert list_datasets.main(["--vortex", "--count"]) == 0
    assert capsys.readouterr().out.strip() == "0"
    with operation(cfg):
        row = status.gather(TINY, {"schema_version": 2, "datasets": [TINY]}, fast=True)
    assert row["vortex"]["unavailable"] == _measurement() and not status._is_incomplete(
        {**row, "raw": {"present": True}, "arrow": {"expected": True, "present": True},
         "parquet": {"expected": True, "present": True}})
    assert status._fmt_row(row)[-1] == "unavail"
