# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""A failure already measured is skipped, not repeated: a writer that failed to
write a format at this recipe, with this toolchain, from this canonical, is not
run again unless `--retry-errors` asks. Compliance still measures every cell."""
from __future__ import annotations

import json
from dataclasses import replace

import pytest

import raincloud
from raincloud import _builds
from raincloud._resolve import artifact_key
from raincloud.catalogs import operation
from raincloud.pipeline import build, compliance, convert
from raincloud.pipeline import export as export_pkg
from raincloud.pipeline.export import get_exporter, retry_reason, writer_toolchain
from raincloud.pipeline.export.__main__ import main as export_main
from raincloud.pipeline.spec import prepared_parquet, prepared_vortex
from tests.test_export_unavailable import _broken_vortex, _measurement, _vortex_entry
from tests.test_pipeline_contracts import TINY, _catalog
from tests.test_pipeline_contracts import stages as stages  # noqa: F401 (fixture)

pytest.importorskip("vortex")


def _no_vortex_writer(monkeypatch):
    """Fail the test if the Vortex writer is started; other writers run.
    Returns the cells started."""
    started = []
    real = export_pkg.run_bounded

    def guarded(exporter, spec, canonical, dest):
        started.append(exporter.cell_id)
        if exporter.cell_id.startswith("vortex@"):
            pytest.fail(f"{exporter.cell_id} was started for a failure already measured")
        return real(exporter, spec, canonical, dest)
    monkeypatch.setattr(export_pkg, "run_bounded", guarded)
    return started


def _measured_here(tmp_path, stages, monkeypatch, name):
    """A catalog whose tiny/vortex this install measured unavailable."""
    cfg = _catalog(tmp_path, name, [TINY])
    with operation(cfg), monkeypatch.context() as fault:
        _broken_vortex(fault)
        assert build.run_one(TINY, strict=False)
    assert "unavailable" in _vortex_entry(cfg)
    return cfg


def _rewrite_measurement(cfg, **changes):
    path = _builds.record_path(cfg.data_dir)
    document = json.loads(path.read_text())
    document["artifacts"][artifact_key("tiny", "vortex", 2)]["unavailable"].update(changes)
    path.write_text(json.dumps(document))


# ---------- the same writer, toolchain and canonical: skipped ----------

def test_a_build_skips_a_measured_failure_and_exits_0(tmp_path, stages, monkeypatch, capsys):
    cfg = _measured_here(tmp_path, stages, monkeypatch, "skip-build")
    before = _vortex_entry(cfg)
    capsys.readouterr()
    started = _no_vortex_writer(monkeypatch)
    with operation(cfg):
        assert build._main(["tiny"]) == 0
        assert prepared_parquet("tiny").is_file()
    assert started == ["parquet@py"]
    out = capsys.readouterr()
    measured = before["unavailable"]
    assert (f"[skip] tiny/vortex: vortex@py (python {measured['toolchain']['python']}" in out.err
            and f"failed at this recipe on {measured['measured_at']}: {measured['error']}; "
                "pass --retry-errors to try again" in out.err)
    assert "known failures not retried=1" in out.out and "[skip] tiny/vortex: vortex@py failed" in out.out
    after = _vortex_entry(cfg)
    assert after["unavailable"] == measured  # kept, not re-measured
    assert "unavailable=" not in out.out.split("summary:")[1].split("known")[0]


def test_a_catalog_measurement_alone_skips(tmp_path, stages, monkeypatch, capsys):
    current = writer_toolchain(get_exporter("vortex@py"))
    measured = {**_measurement(), "toolchain": current, "canonical_sha256": None}
    cfg = _catalog(tmp_path, "skip-catalog", [TINY], {"tiny": {"vortex_unavailable": measured}})
    _no_vortex_writer(monkeypatch)
    with operation(cfg):
        assert build._main(["tiny"]) == 0
    assert "[skip] tiny/vortex: vortex@py" in capsys.readouterr().err
    assert artifact_key("tiny", "vortex", 2) not in _builds.read(cfg.data_dir)  # nothing new recorded


def test_export_exit_status_for_a_skipped_format(tmp_path, stages, monkeypatch, capsys):
    cfg = _measured_here(tmp_path, stages, monkeypatch, "skip-export")
    before = _vortex_entry(cfg)
    _no_vortex_writer(monkeypatch)
    capsys.readouterr()
    with operation(cfg):
        # The spec's own formats: the known failure is listed, not a failure.
        assert export_main(["tiny"]) == 0
        out = capsys.readouterr()
        assert "[skip] tiny/vortex" in out.err and "1 known failure(s) not retried" in out.out
        # A format asked for outright was not produced: the request failed.
        assert export_main(["tiny", "--format", "vortex"]) == 1
        err = capsys.readouterr().err
        assert "pass --retry-errors to try again" in err and "[failed] tiny: vortex not exported" in err
        # Naming the writer that failed does not re-run it either.
        assert export_main(["tiny", "--format", "vortex@py"]) == 1
        assert "[skip] tiny/vortex: vortex@py" in capsys.readouterr().err
    assert _vortex_entry(cfg) == before


def test_convert_fails_a_skipped_vortex_file(tmp_path, stages, monkeypatch, capsys):
    cfg = _measured_here(tmp_path, stages, monkeypatch, "skip-convert")
    with operation(cfg):
        with monkeypatch.context() as guard:
            _no_vortex_writer(guard)
            assert convert.main(["tiny"]) == 1
        assert "--retry-errors" in capsys.readouterr().err
        assert convert.main(["tiny", "--retry-errors"]) == 0
        assert prepared_vortex("tiny").is_file() and "unavailable" not in _vortex_entry(cfg)


# ---------- anything different: attempted, saying why ----------

def test_an_upgraded_toolchain_is_attempted(tmp_path, stages, monkeypatch, capsys):
    cfg = _measured_here(tmp_path, stages, monkeypatch, "retry-toolchain")
    recorded = _vortex_entry(cfg)["unavailable"]["toolchain"]["vortex-data"]
    real = export_pkg.writer_toolchain
    monkeypatch.setattr(export_pkg, "writer_toolchain",
                        lambda exporter: {**real(exporter), "vortex-data": "99.0.0"}
                        if exporter.cell_id == "vortex@py" else real(exporter))
    capsys.readouterr()
    with operation(cfg):
        assert export_main(["tiny"]) == 0
    assert "[retry] tiny/vortex: vortex@py failed at this recipe on" in (err := capsys.readouterr().err)
    assert f"recorded with vortex-data {recorded}, now 99.0.0" in err
    entry = _vortex_entry(cfg)
    assert "unavailable" not in entry and entry["sha256"] and entry["writer"] == "py"


def test_another_canonical_is_attempted(tmp_path, stages, monkeypatch, capsys):
    cfg = _measured_here(tmp_path, stages, monkeypatch, "retry-canonical")
    _rewrite_measurement(cfg, canonical_sha256="f" * 64)
    capsys.readouterr()
    with operation(cfg):
        assert export_main(["tiny"]) == 0
    assert "recorded against canonical ffffffffffff, now " in capsys.readouterr().err
    assert "unavailable" not in _vortex_entry(cfg)


def test_the_retry_reason():
    measured = _measurement()
    toolchain = dict(measured["toolchain"])
    assert retry_reason(measured, "vortex@py", toolchain, "0" * 64) is None
    assert retry_reason(measured, "vortex@rs", toolchain, "0" * 64) == "recorded for vortex@py, now vortex@rs"
    assert retry_reason(measured, "vortex@py", {**toolchain, "pyarrow": "21.0.0"}, "0" * 64) \
        == "recorded with pyarrow 18.1.0, now 21.0.0"
    assert retry_reason(measured, "vortex@py", {"sidecar": "x"}, "0" * 64).count("recorded with") == 4
    # A measurement that names no canonical is not compared on it.
    assert retry_reason({**measured, "canonical_sha256": None}, "vortex@py", toolchain, "1" * 64) is None


# ---------- --retry-errors ----------

def test_retry_errors_attempts_and_a_success_replaces_the_measurement(tmp_path, stages, monkeypatch):
    cfg = _measured_here(tmp_path, stages, monkeypatch, "retry-heal")
    with operation(cfg):
        assert export_main(["tiny", "--retry-errors"]) == 0
    entry = _vortex_entry(cfg)
    assert "unavailable" not in entry and entry["sha256"] and entry["writer"] == "py"
    assert raincloud.load("tiny", format="vortex", config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def test_retry_errors_records_a_repeated_failure_again(tmp_path, stages, monkeypatch, capsys):
    cfg = _measured_here(tmp_path, stages, monkeypatch, "retry-again")
    _rewrite_measurement(cfg, measured_at="2026-01-01T00:00:00Z")
    _broken_vortex(monkeypatch, "still broken")
    capsys.readouterr()
    with operation(cfg):
        assert build._main(["tiny", "--retry-errors"]) == 0
    assert "trying again (--retry-errors)" in capsys.readouterr().err
    measured = _vortex_entry(cfg)["unavailable"]
    assert measured["measured_at"] != "2026-01-01T00:00:00Z" and "still broken" in measured["error"]


def test_the_retry_errors_setting_reaches_a_child_build(tmp_path, stages, monkeypatch):
    cfg = _measured_here(tmp_path, stages, monkeypatch, "retry-setting")
    handle = raincloud.load("tiny", format="parquet", config=cfg, retry_errors=True)
    assert handle._build and handle.config.retry_errors
    assert handle.config.subprocess_env()["RAINCLOUD_RETRY_ERRORS"] == "1"
    assert cfg.subprocess_env()["RAINCLOUD_RETRY_ERRORS"] == "0"
    # A child build reads it from its settings, as `--retry-errors`.
    with operation(replace(cfg, retry_errors=True)):
        assert build._main(["tiny"]) == 0
    assert "unavailable" not in _vortex_entry(cfg)


# ---------- compliance measures regardless ----------

def test_compliance_still_runs_a_skipped_cell(tmp_path, stages, monkeypatch):
    cfg = _measured_here(tmp_path, stages, monkeypatch, "skip-compliance")
    calls = []
    real = compliance.run_bounded

    def spy(exporter, spec, canonical, dest):
        calls.append(exporter.cell_id)
        return real(exporter, spec, canonical, dest)
    monkeypatch.setattr(compliance, "run_bounded", spy)
    with operation(cfg):
        sc = compliance.run_compliance(TINY, cells=["vortex@py"], reader_ids=["vortex@py"], reencode=True)
    assert calls == ["vortex@py"]
    (written,) = [r for r in sc.write_results if r.format_id == "vortex@py"]
    assert written.sha256 and written.compliance.roundtrip is True
