# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Pipeline infrastructure contracts: published-value redaction, the oracle gate,
publish ordering, loud error paths, and the listing/status CLI views."""
from __future__ import annotations

import builtins
import hashlib
import importlib
import json
import math
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline import ledger, spec
from raincloud.pipeline.compliance import ComplianceReport, SlugCompliance
from raincloud.pipeline.export.base import ReadResult, Verdict

# --------------------------------------------------------------------------- #
# Redaction of published values (snapshot stats, profile top_values)
# --------------------------------------------------------------------------- #

CRED = "<redacted-credential>"
PATH = "<redacted-path>"


@pytest.mark.parametrize("value, expected", [
    ("https://u:p@host/x", f"https://{CRED}@host/x"),
    ("ftp://anonymous:guest@ftp.example.org/pub", f"ftp://{CRED}@ftp.example.org/pub"),
    ("https://u:pa/ss@host/x", f"https://{CRED}@host/x"),          # '/' inside a password
    ("https://TOKEN@host/x", f"https://{CRED}@host/x"),            # token-only userinfo
    ("https://h/x?token=abc&b=1", f"https://h/x?token={CRED}&b=1"),
    ("https://h/x?X-Amz-Signature=beef&X-Amz-Credential=AK/2020",
     f"https://h/x?X-Amz-Signature={CRED}&X-Amz-Credential={CRED}"),
    ("https://h/x?sig=abc", f"https://h/x?sig={CRED}"),
    ("/home/someone/f", PATH),
    ("/Users/someone/f", PATH),
    ("/srv/data/x", PATH),
    ("/tmp/abc", PATH),
    ("/var/folders/xy/z", PATH),
    ("/mnt/disk/x", PATH),
    ("/opt/tool/bin", PATH),
    ("/private/var/x", PATH),
    ("file:///home/x/y", f"file://{PATH}"),
    ("PATH=/usr/bin:/home/x/bin", f"PATH=/usr/bin:{PATH}"),
    ("C:\\Users\\bob\\x", PATH),
    # Must survive: a URL whose path merely contains such a segment, an
    # ordinary absolute path outside every host root, and a lookalike prefix.
    ("https://www.nyc.gov/home/index.page", "https://www.nyc.gov/home/index.page"),
    ("/usr/lib/x", "/usr/lib/x"),
    ("/homer/x", "/homer/x"),
    ("https://h/x?design=1", "https://h/x?design=1"),
    # A port then a later '@' is not userinfo.
    ("https://example.com:443/@user/post", "https://example.com:443/@user/post"),
    ("http://localhost:8080/@vite/client", "http://localhost:8080/@vite/client"),
    ("https://example.com:443/p?email=a@b.com", "https://example.com:443/p?email=a@b.com"),
    ("https://u:p@example.com:443/x", f"https://{CRED}@example.com:443/x"),
    # Bare host-path prefixes over-redact a genuine leading URL path: intended.
    ("/media/img.jpg", PATH),
])
def test_redact_published_value(value, expected):
    assert spec.redact_published_value(value) == expected


def test_redact_covers_configured_roots_outside_known_prefixes(monkeypatch, tmp_path):
    odd = Path("/data-store/raincloud")
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(odd))
    assert spec.redact_published_value(f"argv {odd}/v2/x.dat") == f"argv {PATH}"


def test_json_safe_decodes_bytes_and_maps_non_finite_floats():
    assert spec._json_safe(b"/home/a/b") == PATH
    assert spec._json_safe(b"\xff\xfe") == "fffe"
    assert spec._json_safe(1.5) == 1.5
    for bad in (math.inf, -math.inf, math.nan):
        assert spec._json_safe(bad) is None


def test_double_column_with_inf_yields_strict_json_stats(tmp_path):
    from raincloud._bundle import document
    path = tmp_path / "x.parquet"
    pq.write_table(pa.table({"d": [1.0, math.inf, -math.inf]}), path)
    stats = spec.read_column_stats(path)
    raw = json.dumps({"slugs": {"x": {"columns": stats}}}, allow_nan=False).encode()
    assert document(raw, "snapshot")["slugs"]["x"]["columns"][0]["max"] is None


def test_null_count_is_unknown_when_a_row_group_lacks_statistics(tmp_path, monkeypatch):
    path = tmp_path / "x.parquet"
    pq.write_table(pa.table({"a": [1, None, 3, None]}), path, row_group_size=2)
    assert spec.read_column_stats(path)[0]["null_count"] == 2
    real = pq.ParquetFile

    class _NoStatsSecondGroup:
        def __init__(self, p):
            self._f = real(p)
            self.schema_arrow = self._f.schema_arrow
            meta = self._f.metadata

            class _Meta:
                num_row_groups = meta.num_row_groups
                schema = meta.schema

                def row_group(self, i):
                    rg = meta.row_group(i)
                    if i == 0:
                        return rg

                    class _RG:
                        def column(self, ci):
                            col = rg.column(ci)

                            class _Col:
                                total_compressed_size = col.total_compressed_size
                                statistics = None
                            return _Col()
                    return _RG()
            self.metadata = _Meta()

    monkeypatch.setattr(pq, "ParquetFile", _NoStatsSecondGroup)
    (stats,) = spec.read_column_stats(path)
    # Bounds from the first group alone are not the column's bounds either.
    assert stats["null_count"] is None and stats["min"] is None and stats["max"] is None


def test_an_all_null_group_leaves_the_bounds_known(tmp_path):
    path = tmp_path / "x.parquet"
    pq.write_table(pa.table({"a": pa.array([None, None, 3, 5], pa.int64())}), path, row_group_size=2)
    (stats,) = spec.read_column_stats(path)
    assert (stats["min"], stats["max"], stats["null_count"]) == (3, 5, 2)


def test_profile_top_values_are_redacted(tmp_path):
    pytest.importorskip("duckdb")
    from raincloud.pipeline.profile import profile_slug
    path = tmp_path / "x.parquet"
    values = ["https://u:p@host/x", "/home/someone/f", "https://www.nyc.gov/home/a"] * 3
    pq.write_table(pa.table({"s": values}), path)
    top = profile_slug(slug="x", parquet_path=path)["columns"]["s"]["top_values"]
    got = {t["value"] for t in top}
    assert f"https://{CRED}@host/x" in got
    assert PATH in got
    assert "https://www.nyc.gov/home/a" in got
    assert not any("u:p@" in v or v.startswith("/home/") for v in got)


# --------------------------------------------------------------------------- #
# Ledger: every configured root is scrubbed from a tracked ledger
# --------------------------------------------------------------------------- #


def _report(*reads, slug="s1", canonical=None, skipped=None):
    return ComplianceReport(slugs=[SlugCompliance(
        slug=slug,
        canonical=canonical or Path(f"/x/outputs/v2/{slug}/arrow/{slug}.arrow.zstd"),
        read_results=[ReadResult(c, r, v) for c, r, v in reads],
        skipped_cells=skipped or [],
    )])


def test_ledger_scrubs_outputs_configured_outside_the_checkout(monkeypatch, tmp_path):
    home = tmp_path / "checkout"
    store = tmp_path / "elsewhere" / "store"
    monkeypatch.setenv("RAINCLOUD_HOME", str(home))
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(store))
    art = store / "v2" / "foo" / "parquet" / "foo.parquet"
    report = _report(("parquet@py", "parquet@java", Verdict(
        "fail", note=f"open failed: {art}", detail=f"Could not open '{art}': 0 bytes")),
        slug="foo", canonical=store / "v2" / "foo" / "arrow" / "foo.arrow.zstd")
    block = ledger.to_compliance_json(report, generated_at="", versions=None)["slugs"]["foo"]
    text = json.dumps(block)
    assert str(store) not in text and str(tmp_path) not in text
    assert block["canonical"] == "<data>/v2/foo/arrow/foo.arrow.zstd"
    assert "<data>/v2/foo/parquet/foo.parquet" in block["read"][0]["detail"]


def test_scrub_respects_path_boundaries(monkeypatch):
    home = Path("/work/raincloud")
    monkeypatch.setenv("RAINCLOUD_HOME", str(home))
    assert spec.scrub_published_text(f"at {home}/outputs/x") == "at outputs/x"
    assert spec.scrub_published_text(f"'{home}'") == "'.'"
    # A sibling that merely shares the prefix is not the root.
    assert spec.scrub_published_text(f"at {home}@other/x") == f"at {home}@other/x"


# --------------------------------------------------------------------------- #
# Oracle gate
# --------------------------------------------------------------------------- #


def _cj(slug, reads, **block):
    rows = []
    for cell, reader, status, *absent in reads:
        row = {"artifact_cell": cell, "reader_id": reader, "status": status}
        if absent and absent[0]:
            row["toolchain_absent"] = True
        rows.append(row)
    return {"slugs": {slug: {"read": rows, **block}}}


def test_oracle_toolchain_absent_cell_measured_now_is_added_not_mutated():
    # A poor-toolchain oracle checked on a rich-toolchain run.
    oracle = _cj("s1", [("parquet@py", "parquet@hardwood", "skip", True)])
    new = _cj("s1", [("parquet@py", "parquet@hardwood", "pass")])
    diff = ledger.diff_against_oracle(new, oracle)
    assert diff.added == [("s1", "parquet@py", "parquet@hardwood")]
    assert diff.mutated == [] and not diff.has_violations


def test_oracle_toolchain_absent_cell_that_now_fails_still_gates():
    oracle = _cj("s1", [("parquet@py", "parquet@hardwood", "skip", True)])
    ok, reasons = ledger.oracle_gate(
        _report(("parquet@py", "parquet@hardwood", Verdict("fail", note="bad"))), oracle)
    assert not ok
    assert any("new read fail s1/parquet@py/parquet@hardwood" in r for r in reasons)


def test_oracle_executed_skip_is_still_a_mutation():
    oracle = _cj("s1", [("parquet@py", "parquet@java", "skip")])     # the reader RAN
    new = _cj("s1", [("parquet@py", "parquet@java", "pass")])
    assert ledger.diff_against_oracle(new, oracle).mutated


def test_subset_run_does_not_read_as_removals():
    oracle = _cj("s1", [("parquet@py", "parquet@py", "pass"),
                        ("parquet@py", "vortex@jni", "pass"),
                        ("vortex@py", "vortex@py", "pass")])
    scope = {"requested_cells": ["parquet@py"], "requested_readers": ["parquet@py"]}
    new = _cj("s1", [("parquet@py", "parquet@py", "pass")])
    diff = ledger.diff_against_oracle(new, oracle, scope=scope)
    assert diff.removed == []
    assert set(diff.skipped) == {("s1", "parquet@py", "vortex@jni"), ("s1", "vortex@py", "vortex@py")}
    # Without a scope the same absences are removals.
    assert len(ledger.diff_against_oracle(new, oracle).removed) == 2
    # oracle_gate threads the scope through.
    ok, _ = ledger.oracle_gate(_report(("parquet@py", "parquet@py", Verdict("pass"))), oracle, scope=scope)
    assert ok


def test_oracle_write_row_must_carry_roundtrip(tmp_path):
    path = tmp_path / "oracle.json"
    path.write_text(json.dumps({"slugs": {"s": {"read": [], "skipped_cells": [],
                                                "write": [{"cell": "parquet@py"}]}}}))
    with pytest.raises(ledger.MalformedOracle, match="roundtrip"):
        ledger.load_oracle(path)
    path.write_text(json.dumps({"slugs": {"s": {"read": [], "skipped_cells": [],
                                                "write": [{"cell": "parquet@py", "roundtrip": None}]}}}))
    ledger.load_oracle(path)


def test_oracle_rejects_the_reserved_write_reader(tmp_path):
    path = tmp_path / "oracle.json"
    path.write_text(json.dumps({"slugs": {"s": {"write": [], "skipped_cells": [], "read": [
        {"artifact_cell": "parquet@py", "reader_id": ledger.WRITE_CELL_READER, "status": "pass"}]}}}))
    with pytest.raises(ledger.MalformedOracle, match="reserved"):
        ledger.load_oracle(path)


def test_ledger_write_is_atomic_and_leaves_no_tmp(tmp_path):
    out = tmp_path / "d" / "compliance.json"
    ledger.write_compliance_json(_report(), out, generated_at="t")
    assert json.loads(out.read_text())["generated_at"] == "t"
    assert [p.name for p in out.parent.iterdir()] == ["compliance.json"]


# --------------------------------------------------------------------------- #
# Environment knobs fail loudly
# --------------------------------------------------------------------------- #


KNOBS = [
    ("RAINCLOUD_MAX_DECOMPRESSED_BYTES", spec.max_decompressed_bytes),
    ("RAINCLOUD_MAX_TABLE_CELLS", spec.max_table_cells),
    ("RAINCLOUD_ROW_GROUP_TARGET_BYTES", spec.row_group_target_bytes),
    ("RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES", spec.row_group_target_encoded_bytes),
    ("RAINCLOUD_ROW_GROUP_MAX_ROWS", spec.row_group_max_rows),
    ("RAINCLOUD_FETCH_DEADLINE", spec.fetch_deadline),
    ("RAINCLOUD_GENERATOR_TIMEOUT", spec.generator_timeout),
    ("RAINCLOUD_SIDECAR_TIMEOUT", spec.sidecar_timeout),
    ("RAINCLOUD_EXPORT_TIMEOUT", spec.export_timeout),
    ("RAINCLOUD_EXPORT_MEMORY", spec.export_memory),
]


@pytest.mark.parametrize("var, knob", KNOBS)
# One grammar in every lane (spec._env_number; the Rust and Java sidecars' knob()
# mirror this table): ASCII digits, an optional fraction and exponent.
@pytest.mark.parametrize("bad", ["8GiB", "6h", "10,000", "-1", "inf", "nan", "1_000", "\u0661\u0660",
                                 "+5", "-0", ".5", "5.", "0x10", "1e999", "\udcff"])
def test_malformed_or_negative_knob_raises_naming_it(monkeypatch, var, knob, bad):
    monkeypatch.setenv(var, bad)
    with pytest.raises(ValueError, match=var):
        knob()


@pytest.mark.parametrize("var, knob", KNOBS)
@pytest.mark.parametrize("good, expected", [(" 1e3\t", 1000), ("12.9", 12)])
def test_the_knob_grammar_accepts_exponents_and_fractions(monkeypatch, var, knob, good, expected):
    monkeypatch.setenv(var, good)
    got = knob()
    assert got == expected or (var.endswith(("DEADLINE", "TIMEOUT")) and got == float(good))


COUNT_KNOBS = [(var, knob) for var, knob in KNOBS if not var.endswith(("DEADLINE", "TIMEOUT"))]


@pytest.mark.parametrize("var, knob", COUNT_KNOBS + [("RAINCLOUD_ROW_GROUP_PROBE_ROWS", spec.row_group_probe_rows)])
def test_a_fraction_rounding_to_zero_is_refused_for_counts(monkeypatch, var, knob):
    monkeypatch.setenv(var, "0.5")
    with pytest.raises(ValueError, match="rounds down to 0"):
        knob()


@pytest.mark.parametrize("var, knob", [(v, k) for v, k in KNOBS if v.endswith(("DEADLINE", "TIMEOUT"))])
def test_a_fraction_of_a_second_is_a_timeout(monkeypatch, var, knob):
    monkeypatch.setenv(var, "0.5")
    assert knob() == 0.5


def test_a_huge_count_saturates_at_the_disabled_figure(monkeypatch):
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_MAX_ROWS", "1e300")
    assert spec.row_group_max_rows() == (1 << 31) - 1
    monkeypatch.setenv("RAINCLOUD_MAX_TABLE_CELLS", "1e300")
    assert spec.max_table_cells() == 1 << 62


def test_check_env_knobs_names_the_bad_one(monkeypatch):
    spec.check_env_knobs()
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES", "128MiB")
    with pytest.raises(ValueError, match="RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES"):
        spec.check_env_knobs()


@pytest.mark.parametrize("var, knob", KNOBS)
def test_empty_and_zero_disable_a_ceiling(monkeypatch, var, knob):
    monkeypatch.delenv(var, raising=False)
    default = knob()
    assert default is not None
    for off in ("", "0", " 0 ", "0.0"):
        monkeypatch.setenv(var, off)
        value = knob()
        # Ceilings that return None when disabled; row-group limits map "off" to huge.
        assert value is None or value > (default or 0)
    monkeypatch.setenv(var, "123")
    assert knob() == 123


def test_probe_rows_cannot_be_disabled(monkeypatch):
    for off in ("", "0"):
        monkeypatch.setenv("RAINCLOUD_ROW_GROUP_PROBE_ROWS", off)
        with pytest.raises(ValueError, match="PROBE_ROWS"):
            spec.row_group_probe_rows()
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_PROBE_ROWS", "1000")
    assert spec.row_group_probe_rows() == 1000


# --------------------------------------------------------------------------- #
# raw_slug_dir: an unreadable marker is announced, not swallowed
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("damage", ["{not json", "[]", '"x"'])
def test_corrupt_fetch_marker_warns(tmp_path, capsys, damage):
    import raincloud
    from raincloud._bundle import encode, make_bundle
    from raincloud.catalogs import operation

    recipe = {"slug": "tiny", "short_name": "T", "full_name": "T", "export": {"formats": []},
              "fetch": {"type": "http", "urls": ["https://example.test/x"]}}
    bundle = make_bundle(encode({"schema_version": 2, "datasets": [recipe]}),
                         encode({"schema_version": 2, "slugs": {}}), "fetch-marker")
    directory = tmp_path / "catalog"
    directory.mkdir()
    for name, raw in bundle.files().items():
        (directory / name).write_bytes(raw)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(directory), data_dir=tmp_path / "data",
                                   cache_dir=tmp_path / "data", raw_dir=tmp_path / "raw",
                                   scratch_dir=tmp_path / "scratch", catalog_dir=tmp_path / "catalogs")
    marker = tmp_path / "raw" / "tiny" / ".fetch-recipe.json"
    marker.parent.mkdir(parents=True)
    marker.write_text(damage)
    with operation(cfg):
        got = spec.raw_slug_dir("tiny")
        err = capsys.readouterr().err
        assert got.parent.name == ".recipes"
        assert str(marker) in err and "another recipe's" in err and str(got) in err
        assert "fetching" not in err  # a read-only caller (status) fetches nothing
        assert spec.raw_slug_dir("tiny") == got
        assert capsys.readouterr().err == ""  # said once per marker


# --------------------------------------------------------------------------- #
# publish --store: refuse before placing; dry run agrees with the real run
# --------------------------------------------------------------------------- #


def _store_fixture(tmp_path, monkeypatch, *, extra_snapshot=None, sha=None):
    from raincloud.pipeline import publish
    art = tmp_path / "outputs" / "v2" / "tiny" / "parquet" / "tiny.parquet"
    art.parent.mkdir(parents=True)
    art.write_bytes(b"REALBYTES")
    snap = tmp_path / "snapshot.json"
    slugs = {"tiny": {"parquet_bytes": 9,
                      "parquet_sha256": sha or hashlib.sha256(b"REALBYTES").hexdigest()}}
    slugs.update(extra_snapshot or {})
    snap.write_text(json.dumps({"schema_version": 2, "slugs": slugs}))
    datasets = [{"slug": "tiny"}, *({"slug": s} for s in (extra_snapshot or {}))]
    manifest = {"schema_version": 2, "datasets": datasets}
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(snap))
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))  # the store publish locks
    monkeypatch.setattr(publish, "load_manifest", lambda: manifest)
    monkeypatch.setattr(publish, "_outputs_root", lambda m=None: tmp_path / "outputs" / "v2")
    return publish, snap


def test_refused_release_leaves_the_store_untouched(tmp_path, monkeypatch, capsys):
    publish, _ = _store_fixture(tmp_path, monkeypatch,
                                extra_snapshot={"unbuilt": {"parquet_bytes": 5, "parquet_sha256": "0" * 64}})
    store, catalogs_dir = tmp_path / "store", tmp_path / "catalogs"
    assert publish.main(["tiny", "--store", str(store), "--catalogs", str(catalogs_dir)]) == 1
    assert "v2/unbuilt/parquet/unbuilt.parquet" in capsys.readouterr().err
    assert not (store / "v2").exists()
    assert not (catalogs_dir / "latest.json").exists()


def test_dry_run_release_on_an_empty_store_matches_the_real_run(tmp_path, monkeypatch, capsys):
    publish, _ = _store_fixture(tmp_path, monkeypatch)
    store, catalogs_dir = tmp_path / "store", tmp_path / "catalogs"
    assert publish.main(["tiny", "--store", str(store), "--catalogs", str(catalogs_dir), "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "DRY release catalog" in out
    assert not store.exists() and not catalogs_dir.exists()
    assert publish.main(["tiny", "--store", str(store), "--catalogs", str(catalogs_dir)]) == 0
    assert (catalogs_dir / "latest.json").is_file()


def test_publish_mismatch_is_a_clean_refusal_naming_the_snapshot(tmp_path, monkeypatch, capsys):
    publish, snap = _store_fixture(tmp_path, monkeypatch, sha="1" * 64)
    assert publish.main(["tiny", "--mirror", f"file://{tmp_path}/mirror"]) == 1
    err = capsys.readouterr().err
    assert "refusing to publish" in err and str(snap) in err


def test_publish_help_needs_no_catalog(monkeypatch, capsys):
    from raincloud.pipeline import publish
    monkeypatch.setattr(publish, "operation_lock", lambda: pytest.fail("locked for --help"))
    with pytest.raises(SystemExit) as exc:
        publish.main(["--help"])
    assert exc.value.code == 0


def test_publish_unknown_slug_suggests(tmp_path, monkeypatch, capsys):
    publish, _ = _store_fixture(tmp_path, monkeypatch)
    with pytest.raises(SystemExit) as exc:
        publish.main(["tiyn", "--store", str(tmp_path / "store")])
    assert exc.value.code == 2  # as every stage CLI
    assert "Did you mean tiny" in capsys.readouterr().err
    assert not (tmp_path / "store").exists()


def test_refused_release_places_nothing_even_for_a_planned_slug(tmp_path, monkeypatch, capsys):
    publish, snap = _store_fixture(tmp_path, monkeypatch,
                                   extra_snapshot={"other": {"parquet_bytes": 5, "parquet_sha256": "0" * 64}})
    store, catalogs_dir = tmp_path / "store", tmp_path / "catalogs"
    # `other` is planned too, but its local file is not the snapshot's size.
    other = tmp_path / "outputs" / "v2" / "other" / "parquet" / "other.parquet"
    other.parent.mkdir(parents=True)
    other.write_bytes(b"123")
    doc = json.loads(snap.read_text())
    doc["slugs"]["other"]["parquet_sha256"] = hashlib.sha256(b"123").hexdigest()
    snap.write_text(json.dumps(doc))
    assert publish.main(["tiny", "other", "--store", str(store), "--catalogs", str(catalogs_dir)]) == 1
    assert "v2/other/parquet/other.parquet" in capsys.readouterr().err
    assert not (store / "v2" / "tiny" / "parquet" / "tiny.parquet").exists()
    assert not (catalogs_dir / "latest.json").exists()


def test_publish_refuses_a_catalog_with_no_snapshot_for_its_version(tmp_path, monkeypatch, capsys):
    publish, _ = _store_fixture(tmp_path, monkeypatch)
    monkeypatch.delenv("RAINCLOUD_SNAPSHOT")  # a manifest override with no snapshot beside it
    mirror = tmp_path / "mirror"
    assert publish.main(["tiny", "--mirror", mirror.as_uri()]) == 1
    assert "has no snapshot" in capsys.readouterr().err
    assert not mirror.exists()


# --------------------------------------------------------------------------- #
# Browser: loud ledger errors; importable without textual
# --------------------------------------------------------------------------- #


def test_malformed_compliance_ledger_raises_naming_it(monkeypatch, tmp_path):
    from raincloud.exceptions import CatalogError
    from raincloud.pipeline import browse
    bad = tmp_path / "compliance.json"
    monkeypatch.setattr(spec, "default_compliance_json", lambda *a, **k: bad)
    assert browse._load_compliance(2) == {}          # absent: nothing measured
    bad.write_text("{truncated")
    with pytest.raises(CatalogError, match=str(bad)):
        browse._load_compliance(2)
    bad.write_text(json.dumps({"slugs": []}))
    with pytest.raises(CatalogError, match=str(bad)):
        browse._load_compliance(2)


def test_browse_imports_without_textual_and_main_says_how_to_install(monkeypatch, capsys):
    real_import = builtins.__import__

    def blocked(name, *args, **kwargs):
        if name == "textual" or name.startswith("textual."):
            raise ImportError(f"No module named {name!r}")
        return real_import(name, *args, **kwargs)

    for name in [n for n in sys.modules if n == "textual" or n.startswith("textual.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.delitem(sys.modules, "raincloud.pipeline.browse", raising=False)
    monkeypatch.setattr(builtins, "__import__", blocked)
    browse = importlib.import_module("raincloud.pipeline.browse")
    try:
        assert browse.TEXTUAL_IMPORT_ERROR is not None
        assert browse._hydrate_cell({"derive": {"hydrate": {"columns": ["url"]}}}) == "⚠"
        assert browse.main([]) == 2
        err = capsys.readouterr().err
        assert "raincloud[tui]" in err
    finally:
        monkeypatch.undo()
        sys.modules.pop("raincloud.pipeline.browse", None)
        importlib.import_module("raincloud.pipeline.browse")


# --------------------------------------------------------------------------- #
# list_datasets
# --------------------------------------------------------------------------- #


def _manifest(*specs):
    return {"schema_version": 2, "datasets": [
        {"slug": s, "short_name": s, "full_name": s, "description": d, **extra}
        for s, d, extra in specs]}


@pytest.fixture
def listing(monkeypatch):
    from raincloud.pipeline import list_datasets as ld
    m = _manifest(("bi-rentabilidad", "Devol-uci-ones", {"transform": {"handler": "public_bi_merge"}}),
                  ("uci-iris", "Iris", {"transform": {"handler": "uci_default"}}),
                  ("hn-hydrated", "", {"derive": {"from": "hn", "hydrate": {"columns": ["url"]}},
                                       "transform": {"handler": "uci_default"}}))
    monkeypatch.setattr(ld, "load_manifest", lambda: m)
    monkeypatch.setattr(ld, "_load_snapshot", lambda *a: {})
    monkeypatch.setattr(ld, "_VERSION_SNAPSHOTS", {1: {}, 2: {}})
    return ld


def test_default_output_is_bare_slugs_when_piped(listing, capsys, monkeypatch):
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
    assert listing.main([]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines == ["bi-rentabilidad", "uci-iris", "hn-hydrated"]


def test_default_output_marks_hydrated_on_a_terminal(listing, capsys, monkeypatch):
    monkeypatch.setattr(sys.stdout, "isatty", lambda: True)
    assert listing.main(["--hydrate"]) == 0
    assert capsys.readouterr().out.splitlines() == ["hn-hydrated  [hydrated]"]


def test_word_terms_rank_slug_matches_first(listing, capsys, monkeypatch):
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
    assert listing.main(["uci"]) == 0
    assert capsys.readouterr().out.splitlines() == ["uci-iris", "bi-rentabilidad"]


def test_unknown_filter_value_is_an_error_with_a_suggestion(listing, capsys):
    assert listing.main(["--handler", "uci_defualt"]) == 2
    assert "Did you mean uci_default" in capsys.readouterr().err
    assert listing.main(["--handler", "nonexistent"]) == 2


def test_derived_fetch_type_filters_like_it_lists(listing, capsys, monkeypatch):
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
    assert listing.main(["--fetch-type", "derived"]) == 0
    assert capsys.readouterr().out.splitlines() == ["hn-hydrated"]


def test_malformed_tracked_snapshot_raises_and_is_not_cached(monkeypatch, tmp_path, capsys):
    from raincloud.pipeline import list_datasets as ld
    monkeypatch.setattr(ld, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(ld, "_VERSION_SNAPSHOTS", {})
    monkeypatch.setattr("raincloud.catalogs.selected_context", lambda: None)
    assert ld._version_snapshot(2) == {}                  # absent -> no records
    ld._VERSION_SNAPSHOTS.clear()
    path = tmp_path / "docs" / "v2" / "snapshot.json"
    path.parent.mkdir(parents=True)
    path.write_text("{broken")
    from raincloud.exceptions import CatalogError
    with pytest.raises(CatalogError, match="snapshot"):
        ld._version_snapshot(2)
    assert 2 not in ld._VERSION_SNAPSHOTS
    path.write_text(json.dumps({"slugs": {"demo": {"arrow_sha256": "x"}}}))
    assert ld.is_stale_version("demo", 2) is False


def test_inspect_missing_profile_lists_every_searched_path(monkeypatch, capsys, tmp_path):
    from raincloud.pipeline import list_datasets as ld
    monkeypatch.setattr(ld, "outputs_root", lambda manifest=None: tmp_path / "outputs" / "v2")
    monkeypatch.setattr(ld, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(ld, "load_manifest", lambda: _manifest(("demo", "", {})))
    assert ld.main(["--inspect", "demo"]) == 0
    out = capsys.readouterr().out
    assert "checked ;" not in out
    assert str(tmp_path / "outputs" / "v2" / "demo" / "profile.json") in out


def test_inspect_unknown_slug_suggests(listing, capsys):
    assert listing.main(["--inspect", "uci-irsi"]) == 2
    assert "Did you mean uci-iris" in capsys.readouterr().err


def test_checkout_readers_see_revision_observations(monkeypatch, tmp_path):
    from dataclasses import replace

    from raincloud.catalogs import current, operation, resolve_context
    from raincloud.config import get_config
    from raincloud.pipeline import promote_profiles
    config = get_config()
    context = replace(current() or resolve_context(config), source="checkout")
    with operation(config, context):
        paths = promote_profiles.profile_search_paths("demo", repo_root=tmp_path)
    expected = spec.revision_observations_dir(config, context) / "profiles" / "demo.json"
    assert expected in paths


# --------------------------------------------------------------------------- #
# status
# --------------------------------------------------------------------------- #


def test_status_unknown_slug_is_an_error(monkeypatch, capsys):
    from raincloud.pipeline import status
    monkeypatch.setattr(status, "load_manifest", lambda: _manifest(("uci-iris", "", {})))
    assert status.main(["uci-irsi"]) == 2
    assert "Did you mean uci-iris" in capsys.readouterr().err


def test_status_gates_parquet_on_the_export_policy(monkeypatch, tmp_path):
    from raincloud.pipeline import status
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(tmp_path / "out"))
    m = _manifest(("vx-only", "", {"export": {"formats": ["vortex"]}}))
    row = {"raw": {"present": True}, "work": {},
           "arrow": status._arrow_status(m["datasets"][0], m),
           "parquet": status._parquet_status(m["datasets"][0], m, fast=True),
           "vortex": {"opted_in": True, "present": True}}
    assert row["parquet"] == {"expected": False}
    assert status._fmt_row({"slug": "vx-only", **row})[4] == "n/a"
    assert status._is_incomplete(row)                     # no canonical Arrow yet
    arrow = spec.prepared_arrow("vx-only", m)
    arrow.parent.mkdir(parents=True)
    arrow.write_bytes(b"x")
    row["arrow"] = status._arrow_status(m["datasets"][0], m)
    assert not status._is_incomplete(row)


# --------------------------------------------------------------------------- #
# overnight_profile
# --------------------------------------------------------------------------- #


@pytest.fixture
def overnight(monkeypatch):
    from raincloud.pipeline import overnight_profile as op
    wiped = []
    monkeypatch.setattr(op, "_wipe_slug", wiped.append)
    monkeypatch.setattr(op, "_slug_already_built", lambda slug: True)
    return op, wiped


@pytest.mark.parametrize("rc, status", [(124, "promote-timeout"), (1, "promote-failed")])
def test_promote_failure_retains_artifacts(overnight, monkeypatch, rc, status):
    op, wiped = overnight
    rcs = {"build": 0, "profile": 0, "promote_profiles": rc}
    monkeypatch.setattr(op, "_run_stage", lambda slug, stage, *a, timeout: (rcs[stage], "tail"))
    result = op.process_slug("tiny", skip_build=False, budget_secs=None)
    assert result["status"] == status
    assert "retained" in result
    assert wiped == []


@pytest.mark.parametrize("built_here, rc, status, wipe", [
    (False, 124, "profile-timeout", False),
    (False, 1, "profile-failed", False),
    (True, 124, "profile-timeout", False),
    (True, 1, "profile-failed", True),
])
def test_profile_failure_retains_what_this_run_did_not_build(overnight, monkeypatch, built_here, rc,
                                                              status, wipe):
    op, wiped = overnight
    monkeypatch.setattr(op, "_slug_already_built", lambda slug: not built_here)
    rcs = {"build": 0, "profile": rc}
    monkeypatch.setattr(op, "_run_stage", lambda slug, stage, *a, timeout: (rcs[stage], "tail"))
    result = op.process_slug("tiny", skip_build=False, budget_secs=None)
    assert result["status"] == status
    assert (wiped == ["tiny"]) is wipe
    assert ("retained" in result) is (not wipe)


def test_frozen_guard_fails_closed_when_versions_cannot_be_listed(monkeypatch, tmp_path):
    from raincloud.pipeline import overnight_profile as op
    base = tmp_path / "outputs"
    (base / "v2").mkdir(parents=True)
    monkeypatch.setattr(op, "outputs_base", lambda: base)
    monkeypatch.setattr(op, "outputs_root", lambda: base / "v2")

    def denied(path):
        raise PermissionError(13, "Permission denied", str(path))
    monkeypatch.setattr(op.os, "scandir", denied)
    reason = op.frozen_version_reason()
    assert reason is not None and "cannot enumerate" in reason


def test_profile_child_does_not_promote(monkeypatch):
    from raincloud.pipeline import overnight_profile as op
    seen = {}

    def fake_run(cmd, **kwargs):
        seen["cmd"] = cmd

        class P:
            returncode, stderr = 0, ""
        return P()
    monkeypatch.setattr(op.subprocess, "run", fake_run)
    op._run_stage("tiny", "profile", timeout=None)
    assert "--no-promote" in seen["cmd"] and seen["cmd"][-1] == "tiny"


# --------------------------------------------------------------------------- #
# profile: never promotes a stale profile; discards one of replaced bytes
# --------------------------------------------------------------------------- #


def _tmp_catalog(tmp_path, datasets, slugs=None, name="tools-test"):
    import raincloud
    from raincloud._bundle import encode, make_bundle
    bundle = make_bundle(encode({"schema_version": 2, "datasets": datasets}),
                         encode({"schema_version": 2, "slugs": slugs or {}}), name)
    directory = tmp_path / name
    directory.mkdir()
    for filename, raw in bundle.files().items():
        (directory / filename).write_bytes(raw)
    return raincloud.resolve_config(no_config=True, catalog=str(directory), data_dir=tmp_path / "data",
                                    cache_dir=tmp_path / "data", raw_dir=tmp_path / "raw",
                                    scratch_dir=tmp_path / "scratch", catalog_dir=tmp_path / "catalogs",
                                    offline=True)


@pytest.fixture
def profiled(tmp_path, monkeypatch):
    pytest.importorskip("duckdb")
    from raincloud.catalogs import operation
    from raincloud.pipeline import profile, promote_profiles
    monkeypatch.setattr(promote_profiles, "REPO_ROOT", tmp_path / "install")
    cfg = _tmp_catalog(tmp_path, [{"slug": "tiny", "export": {"formats": ["parquet"]}},
                                  {"slug": "unbuilt", "export": {"formats": ["parquet"]}}])
    with operation(cfg):
        parquet = spec.prepared_parquet("tiny")
        parquet.parent.mkdir(parents=True)
        pq.write_table(pa.table({"x": [1, 2, 3]}), parquet)
        yield profile, promote_profiles, parquet


def test_a_failed_reprofile_never_promotes_the_older_profile(profiled, monkeypatch, capsys):
    profile, promote_profiles, _ = profiled
    built = profile._profile_path("tiny")
    built.write_text(json.dumps({"schema_version": 1, "slug": "tiny", "columns": {}}))

    def boom(**kwargs):
        raise RuntimeError("profiling failed")
    monkeypatch.setattr(profile, "profile_slug", boom)
    assert profile.main(["tiny"]) == 1
    promoted = promote_profiles.profile_observations_dir() / "tiny.json"
    assert not promoted.exists()
    # Promotion itself refuses a built profile of another schema_version.
    assert promote_profiles.promote(["tiny"]) == (0, 1, [])
    assert not promoted.exists()
    assert "schema_version 1" in capsys.readouterr().err


def test_a_parquet_replaced_while_profiling_writes_no_profile(profiled, monkeypatch, capsys):
    profile, promote_profiles, parquet = profiled
    real = profile.profile_slug

    def swapped(**kwargs):
        result = real(**kwargs)
        replacement = parquet.with_name("replacement.parquet")
        pq.write_table(pa.table({"x": [9]}), replacement)
        replacement.replace(parquet)  # a concurrent build renames its file in
        return result
    monkeypatch.setattr(profile, "profile_slug", swapped)
    assert profile.main(["tiny"]) == 1
    assert not profile._profile_path("tiny").exists()
    assert not (promote_profiles.profile_observations_dir() / "tiny.json").exists()
    assert "changed while it was being profiled" in capsys.readouterr().err


def test_profile_names_are_checked(profiled, capsys):
    profile, _, _ = profiled
    with pytest.raises(SystemExit) as exc:
        profile.main(["tinny"])
    assert exc.value.code == 2 and "Did you mean tiny" in capsys.readouterr().err
    assert profile.main(["unbuilt", "--no-promote"]) == 1
    assert "no parquet" in capsys.readouterr().err
    assert profile.main(["--all", "--no-promote"]) == 0  # --all profiles what is built
    assert profile._profile_path("tiny").exists()


# --------------------------------------------------------------------------- #
# validate_manifest: every malformed field an error, never a traceback
# --------------------------------------------------------------------------- #


_OK_SPEC = {"slug": "ok", "transform": {"handler": "identity"}, "fetch": {"type": "http", "urls": ["https://x/y"]},
            "export": {"formats": ["parquet", "vortex"]}}


@pytest.mark.parametrize("spec_patch, message", [
    ({"export": {"formats": ["parquet"], "notes": "n", "priority": [["rs"]]}}, "list of writer names"),
    ({"export": {"formats": ["parquet"], "notes": "n", "priority": [{}]}}, "list of writer names"),
    ({"export": {"formats": ["parquet"], "notes": "n", "priority": [1]}}, "list of writer names"),
    ({"export": {"formats": ["parquet"], "notes": "n", "priority": {"parquet": [["rs"]]}}}, "list of writer names"),
    ({"export": {"formats": ["parquet"], "notes": "n", "priority": {"parquet": ["gone"]}}}, "'gone' is not a writer"),
    ({"export": {"formats": 5}}, "export.formats must be a list"),
    ({"slug": ["ok"]}, "slug must be a string"),
    ({"transform": {"handler": ["identity"]}}, "transform.handler must be a string"),
    ({"tags": 5}, "tags must be a list of strings"),
    ({"showcase": 5}, "showcase must be a list of strings"),
    ({"tags": [1]}, "tags must be a list of strings"),
    ({"derive": {"from": ["ok"], "hydrate": {"columns": {}}}}, "derive.from must be a string"),
    ({"license": "CC0"}, "license must be an object"),
    ({"hydrate": []}, "hydrate must be an object"),
])
def test_malformed_fields_are_errors_not_tracebacks(spec_patch, message):
    from raincloud.pipeline import validate_manifest as vm
    errors, _ = vm._cross_checks({"schema_version": 2, "datasets": [{**_OK_SPEC, **spec_patch}]})
    assert any(message in e for e in errors), errors


@pytest.mark.parametrize("priority", [[["rs"]], [{}], [1]])
def test_a_malformed_catalog_export_priority_is_an_error(priority):
    from raincloud.pipeline import validate_manifest as vm
    errors, _ = vm._cross_checks({"schema_version": 2, "export_priority": priority, "datasets": [_OK_SPEC]})
    assert any(e.startswith("export_priority must be") for e in errors), errors


def test_an_unusable_schema_version_adds_no_version_rules():
    from raincloud.pipeline import validate_manifest as vm
    errors, _ = vm._cross_checks({"schema_version": 2.0, "datasets": [{**_OK_SPEC, "export": {"formats": ["parquet"]}}]})
    assert not any("vortex" in e for e in errors), errors


@pytest.mark.parametrize("content, message", [
    (None, "No such file"),
    ("{not json", "Expecting property name"),
    ("[]", "must be a JSON object"),
    (json.dumps({"schema_version": "2", "datasets": []}), "unsupported schema_version"),
])
def test_validate_manifest_reports_an_unreadable_manifest(tmp_path, capsys, content, message):
    from raincloud.pipeline import validate_manifest as vm
    path = tmp_path / "sources.json"
    if content is not None:
        path.write_text(content)
    assert vm.main([str(path)]) == 1
    err = capsys.readouterr().err
    assert str(path) in err and message in err
    assert vm.main([str(path), "--json"]) == 1
    report = json.loads(capsys.readouterr().out)
    assert report["ok"] is False and message in report["errors"][0]


# --------------------------------------------------------------------------- #
# status: the rendered v2 table and summary
# --------------------------------------------------------------------------- #


def test_status_renders_a_v2_table_and_its_denominators():
    from raincloud.pipeline import status
    rows = [
        {"slug": "arrow-only", "raw": {"present": True}, "work": {},
         "arrow": {"expected": True, "present": True}, "parquet": {"expected": False},
         "vortex": {"opted_in": False}},
        {"slug": "parquet-only", "raw": {"present": True}, "work": {"present": True},
         "arrow": {"expected": True, "present": False},
         "parquet": {"expected": True, "present": True, "rows": 1200}, "vortex": {"opted_in": True, "present": False}},
        {"slug": "raw-error", "raw": {"present": False, "error": "unreadable receipt"}, "work": {},
         "arrow": {"expected": True, "present": True},
         "parquet": {"expected": True, "present": True, "stale": True, "rows": 3},
         "vortex": {"opted_in": True, "present": True, "stale": True}},
    ]
    lines = status.render_table(rows).splitlines()
    assert lines[0].split() == ["slug", "raw", "work", "arrow", "parquet", "vortex"]
    assert lines[2].split() == ["arrow-only", "✓", "·", "✓", "n/a", "n/a"]
    assert lines[3].split() == ["parquet-only", "✓", "✓", "·", "✓1,200", "·"]
    assert lines[4].split() == ["raw-error", "err", "·", "✓", "stale", "stale"]
    summary = status.render_summary(rows)
    assert "3 slugs" in summary and "raw 2/3" in summary and "arrow 2/3" in summary
    assert "parquet 1/2" in summary and "rows-match 2/2" in summary and "vortex 0/2" in summary
    assert all(status._is_incomplete(r) for r in rows[1:]) and not status._is_incomplete(rows[0])


def test_status_marks_a_parquet_older_than_its_canonical_stale(monkeypatch, tmp_path):
    import os

    from raincloud.pipeline import status
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(tmp_path / "out"))
    m = _manifest(("both", "", {"export": {"formats": ["parquet"], "notes": "n"}}))
    arrow, parquet = spec.prepared_arrow("both", m), spec.prepared_parquet("both", m)
    for path in (arrow, parquet):
        path.parent.mkdir(parents=True)
    pq.write_table(pa.table({"x": [1]}), parquet)
    arrow.write_bytes(b"x")
    os.utime(parquet, ns=(1, 1))
    got = status._parquet_status(m["datasets"][0], m, fast=True)
    assert got["stale"] and status._fmt_row({"slug": "both", "raw": {}, "work": {}, "arrow": {},
                                             "parquet": got, "vortex": {}})[4] == "stale"


def test_status_selection_follows_the_stage_rules(monkeypatch, capsys):
    from raincloud.pipeline import status
    m = _manifest(("uci-iris", "", {}), ("uci-iris-hydrated", "", {"derive": {"from": "uci-iris",
                                                                               "hydrate": {"columns": {}}}}))
    monkeypatch.setattr(status, "load_manifest", lambda: m)
    monkeypatch.setattr(status, "gather", lambda spec, m, fast: {
        "slug": spec["slug"], "raw": {}, "work": {}, "arrow": {"expected": True, "present": False},
        "parquet": {"expected": False}, "vortex": {"opted_in": False}})
    assert status.main(["uci-iris", "--all"]) == 2
    assert "not both" in capsys.readouterr().err
    assert status.main(["--missing-only", "--json"]) == 0
    assert [r["slug"] for r in json.loads(capsys.readouterr().out)] == ["uci-iris"]
    assert status.main(["uci-iris-hydrated", "uci-iris-hydrated", "--missing-only", "--json"]) == 0
    assert [r["slug"] for r in json.loads(capsys.readouterr().out)] == ["uci-iris-hydrated"]


# --------------------------------------------------------------------------- #
# autotag: names checked; the built profile wins; checkout catalog only
# --------------------------------------------------------------------------- #


@pytest.fixture
def autotag_sources(tmp_path, monkeypatch):
    from raincloud.pipeline import autotag
    sources = tmp_path / "sources.json"
    sources.write_text(json.dumps({"schema_version": 2, "datasets": [{"slug": "tiny"}]}, indent=2) + "\n")
    monkeypatch.setattr(autotag, "SOURCES", sources)
    return autotag, sources


def test_autotag_unknown_slug_exits_2_and_leaves_sources_alone(autotag_sources, capsys):
    autotag, sources = autotag_sources
    before = sources.read_bytes()
    assert autotag.main(["--slug", "tinny"]) == 2
    assert "Did you mean tiny" in capsys.readouterr().err
    assert sources.read_bytes() == before


def test_autotag_refuses_a_catalog_other_than_the_checkout(autotag_sources, capsys):
    autotag, sources = autotag_sources
    before = sources.read_bytes()
    assert autotag.main(["--slug", "tiny"]) == 2  # the real checkout is selected, not this file
    assert "runs only with that checkout catalog" in capsys.readouterr().err
    assert sources.read_bytes() == before


def test_autotag_prefers_the_built_profile_over_the_v1_fallback(autotag_sources, tmp_path, monkeypatch):
    from argparse import Namespace
    from dataclasses import replace

    from raincloud.catalogs import operation, resolve_context
    from raincloud.pipeline import promote_profiles
    autotag, sources = autotag_sources
    cfg = _tmp_catalog(tmp_path, [{"slug": "tiny"}])
    context = replace(resolve_context(cfg), source="checkout", manifest_path=sources)
    monkeypatch.setattr(promote_profiles, "REPO_ROOT", tmp_path)
    fallback = tmp_path / "docs" / "v1" / "profiles" / "tiny.json"
    fallback.parent.mkdir(parents=True)
    fallback.write_text(json.dumps({"which": "v1"}))
    seen = []
    monkeypatch.setattr(autotag, "infer_for_slug", lambda spec, profile: seen.append(profile) or [])
    args = Namespace(slug=["tiny"], preserve=False, dry=True)
    with operation(cfg, context):
        assert autotag._main(args) == 0
        built = spec.outputs_root() / "tiny" / "profile.json"
        built.parent.mkdir(parents=True)
        built.write_text(json.dumps({"which": "built"}))
        assert autotag._main(args) == 0
    assert [p["which"] for p in seen] == ["v1", "built"]


def test_publish_refuses_an_http_mirror_before_planning(monkeypatch, capsys):
    from raincloud.pipeline import publish
    monkeypatch.setattr(publish, "operation_lock", lambda: pytest.fail("locked for a refused mirror"))
    with pytest.raises(SystemExit) as exc:
        publish.main(["tiny", "--mirror", "https://example.test/mirror"])
    assert exc.value.code == 2 and "read-only" in capsys.readouterr().err


def test_a_list_priority_must_name_a_writer_for_every_format_it_serves():
    from raincloud.pipeline import validate_manifest as vm
    both = {**_OK_SPEC, "export": {"formats": ["parquet", "vortex"], "priority": ["hardwood"]}}
    errors, _ = vm._cross_checks({"schema_version": 2, "datasets": [both]})
    assert any("export.priority ['hardwood'] names no vortex writer" in e for e in errors), errors
    assert not any("names no parquet writer" in e for e in errors)
    # The catalog's list serves every format no spec priority names.
    errors, _ = vm._cross_checks({"schema_version": 2, "export_priority": ["hardwood"], "datasets": [_OK_SPEC]})
    assert any(e.startswith("export_priority ['hardwood'] names no vortex writer") for e in errors), errors
    covered = {**_OK_SPEC, "export": {**_OK_SPEC["export"], "priority": {"vortex": ["py"]}}}
    errors, _ = vm._cross_checks({"schema_version": 2, "export_priority": ["hardwood"], "datasets": [covered]})
    assert not any("names no" in e for e in errors), errors


def test_malformed_browse_snapshot_raises_naming_it(monkeypatch, tmp_path, capsys):
    from raincloud.exceptions import CatalogError
    from raincloud.pipeline import browse
    monkeypatch.setattr(browse, "REPO_ROOT", tmp_path)
    monkeypatch.setattr("raincloud.catalogs.selected_context", lambda: None)
    bad = tmp_path / "docs" / "v2" / "snapshot.json"
    bad.parent.mkdir(parents=True)
    bad.write_text('{"schema_version": 2, "slugs": {"a"')
    with pytest.raises(CatalogError, match=str(bad)):
        browse._load_snapshot(2)
    # A stale scratch copy of another schema_version never describes v2 slugs.
    bad.write_text(json.dumps({"schema_version": 2, "slugs": {"a": {"parquet_bytes": None}}}))
    (tmp_path / "docs" / "snapshot.json").write_text(
        json.dumps({"schema_version": 1, "slugs": {"a": {"parquet_bytes": 5}}}))
    assert browse._load_snapshot(2)["slugs"]["a"]["parquet_bytes"] is None
    assert "schema_version 1" in capsys.readouterr().err
    if browse.TEXTUAL_IMPORT_ERROR is not None:
        pytest.skip("browse.main needs the [tui] extra")
    bad.write_text("[truncated")
    monkeypatch.setattr(browse, "_main", lambda argv: browse._load_snapshot(2) and 0)
    assert browse.main([]) == 1
    assert "browse:" in capsys.readouterr().err


def test_browse_fetch_type_facet_offers_derived():
    from raincloud.pipeline import browse
    vocab = browse._live_vocabs({"datasets": [
        {"slug": "a", "fetch": {"type": "http"}},
        {"slug": "a-hydrated", "derive": {"from": "a", "hydrate": {"columns": {}}}}]})
    assert vocab["fetch_type"] == ["derived", "http"]


def test_list_local_filters_and_reports_the_formats_on_disk(listing, capsys, monkeypatch, tmp_path):
    monkeypatch.setattr(sys.stdout, "isatty", lambda: False)
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(tmp_path / "out"))
    arrow = tmp_path / "out" / "v2" / "uci-iris" / "arrow" / "uci-iris.arrow.zstd"
    arrow.parent.mkdir(parents=True)
    arrow.write_bytes(b"x")
    assert listing.main(["--local"]) == 0
    assert capsys.readouterr().out.splitlines() == ["uci-iris"]
    assert listing.main(["--local", "--json"]) == 0
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines() if line.strip()]
    assert [(r["slug"], r["local"]) for r in rows] == [("uci-iris", ["arrow"])]
    assert listing.main(["--local", "--long"]) == 0
    line = next(ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("uci-iris"))
    assert "arrow" in line.split()
