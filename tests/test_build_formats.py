# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Opt-in formats: a build writes the install's formats (only Vortex by
default) or the ones asked for, and removes what it was made from unless the
install keeps it -- except a canonical that is the dataset's only file."""
from __future__ import annotations

from dataclasses import replace

import pytest

import raincloud
from raincloud.catalogs import operation
from raincloud.pipeline import build, status
from raincloud.pipeline.spec import prepared_arrow, prepared_parquet, prepared_vortex, raw_slug_dir
from tests.test_export_unavailable import _broken_vortex
from tests.test_pipeline_contracts import _catalog
from tests.test_pipeline_contracts import stages as stages  # noqa: F401 (fixture)

pytest.importorskip("vortex")
# No `export.formats`: the dataset offers every format.
SPEC = {"slug": "tiny"}


@pytest.fixture
def defaults(monkeypatch):
    """The settings an install has when it sets none (conftest pins others)."""
    for name in ("RAINCLOUD_FORMATS", "RAINCLOUD_KEEP_RAW", "RAINCLOUD_KEEP_CANONICAL"):
        monkeypatch.delenv(name, raising=False)


def _store(tmp_path, datasets=(SPEC,), **settings):
    cfg = replace(_catalog(tmp_path, "formats", list(datasets)), **settings)
    with operation(cfg):
        raw = raw_slug_dir("tiny")
    raw.mkdir(parents=True)
    (raw / "upstream.csv").write_text("x\n1\n")
    return cfg, raw


def test_a_default_build_writes_only_vortex_and_keeps_nothing(tmp_path, stages, defaults):
    cfg, raw = _store(tmp_path)
    assert cfg.formats == ("vortex",) and not cfg.keep_raw and not cfg.keep_canonical
    with operation(cfg):
        assert build.run_one(SPEC, strict=False)
        assert prepared_vortex("tiny").is_file()
        assert not prepared_parquet("tiny").exists()
        assert not prepared_arrow("tiny").exists()
    assert not raw.exists()
    assert raincloud.load("tiny", config=cfg, offline=True).format == "vortex"


def test_the_canonical_stays_when_no_format_was_written(tmp_path, stages, defaults, monkeypatch):
    cfg, _ = _store(tmp_path)
    _broken_vortex(monkeypatch)
    with operation(cfg):
        assert build.run_one(SPEC, strict=False)
        assert prepared_arrow("tiny").is_file() and not prepared_vortex("tiny").exists()
    # Vortex is measured unavailable, so the canonical is what `auto` serves.
    assert raincloud.load("tiny", config=cfg, offline=True).format == "arrow"


def test_keep_settings_keep_the_raw_download_and_the_canonical(tmp_path, stages, defaults):
    cfg, raw = _store(tmp_path, keep_raw=True, keep_canonical=True)
    with operation(cfg):
        assert build.run_one(SPEC, strict=False)
        assert prepared_arrow("tiny").is_file() and prepared_vortex("tiny").is_file()
    assert (raw / "upstream.csv").is_file()


def test_format_flag_writes_only_what_is_asked(tmp_path, stages, defaults):
    cfg, _ = _store(tmp_path)
    with operation(cfg):
        assert build._main(["tiny", "--format", "parquet"]) == 0
        assert prepared_parquet("tiny").is_file()
        assert not prepared_vortex("tiny").exists() and not prepared_arrow("tiny").exists()
        # Asking for arrow keeps the canonical; nothing else is written.
        assert build._main(["tiny", "--format", "arrow"]) == 0
        assert prepared_arrow("tiny").is_file() and not prepared_vortex("tiny").exists()


def test_formats_all_writes_every_offered_format(tmp_path, stages, defaults):
    cfg, _ = _store(tmp_path, formats=("all",))
    with operation(cfg):
        assert build.run_one(SPEC, strict=False)
        assert prepared_parquet("tiny").is_file() and prepared_vortex("tiny").is_file()


def test_formats_all_leaves_out_a_format_with_no_installed_writer(tmp_path, stages, defaults, monkeypatch,
                                                                  capsys):
    from raincloud._formats import WRITERS
    from raincloud.pipeline.export import get_exporter
    for writer in WRITERS["orc"]:
        monkeypatch.setattr(get_exporter(f"orc@{writer}"), "unavailable", lambda: "not on this machine")
    cfg, _ = _store(tmp_path, formats=("all",))
    with operation(cfg):
        assert build.run_one(SPEC, strict=False)
        assert prepared_vortex("tiny").is_file()
        # Named outright, it fails the build instead.
        assert not build.run_one(SPEC, strict=False, formats=["orc"])
    assert "[skip] orc: tiny: no installed writer for 'orc'" in capsys.readouterr().out


def test_a_format_the_dataset_does_not_offer_fails_the_build(tmp_path, stages, defaults, capsys):
    narrow = {"slug": "tiny", "export": {"formats": ["parquet"]}}
    cfg, _ = _store(tmp_path, datasets=(narrow,))
    with operation(cfg):
        assert not build.run_one(narrow, strict=False, formats=["vortex"])
    assert "tiny does not offer vortex (it offers parquet)" in capsys.readouterr().out


def test_unknown_format_names_are_refused_with_a_suggestion(tmp_path, stages, defaults, capsys):
    with pytest.raises(ValueError, match="parquet"):
        raincloud.resolve_config(no_config=True, formats="parqet")
    with pytest.raises(ValueError, match="keep_canonical"):
        raincloud.resolve_config(no_config=True, formats="arrow")
    cfg, _ = _store(tmp_path)
    with operation(cfg), pytest.raises(SystemExit):
        build._main(["tiny", "--format", "vortx"])
    assert "vortex" in capsys.readouterr().err


def test_status_counts_a_dataset_complete_without_what_it_does_not_keep(tmp_path, stages, defaults):
    cfg, _ = _store(tmp_path)
    with operation(cfg):
        assert build.run_one(SPEC, strict=False)
        manifest = {"schema_version": 2, "datasets": [SPEC]}
        row = status.gather(SPEC, manifest, fast=True)
        assert not row["raw"].get("present") and not row["arrow"].get("present")
        assert row["parquet"] == {"expected": False}
        assert not status._is_incomplete(row)


def test_a_v1_catalog_loads_and_builds_as_before(tmp_path, defaults):
    """Install formats arrived in 0.3.1; a v1 catalog keeps 0.3.0's behaviour so
    its users do not break: `auto` tries vortex, parquet, arrow, and a build
    writes what the recipe lists."""
    from raincloud._catalog import Entry, FormatInfo
    from raincloud._formats import build_formats
    cfg = raincloud.resolve_config(no_config=True)
    parquet_only = Entry("old", 1, formats={"parquet": FormatInfo(None, 10)}, version=1)
    assert raincloud._choose_format(parquet_only, "auto", False, config=cfg) == "parquet"
    v1 = {"slug": "old", "convert": {"vortex": True}}
    assert build_formats(v1, 1, cfg) == ["parquet", "vortex"]
