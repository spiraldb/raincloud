# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Source-tree synthetic build + handler-function tests.

Always-on hermetic guard for the pipeline stage chain: drives the real
`build.run_one(spec)` against a synthetic file:// source with all output
dirs under `tmp_path`. No marker — runs in the default `pytest`.
"""
from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.spec import prepared_arrow, prepared_parquet, prepared_vortex


def test_loader_builds_canonical_only_without_snapshot_regeneration(tmp_path, monkeypatch):
    import raincloud
    from raincloud import _catalog

    csv = tmp_path / "tiny.csv"
    csv.write_text("n,s\n1,a\n2,b\n3,c\n")
    spec = _synth_spec(csv)
    spec["export"] = {"formats": []}
    manifest, snapshot = tmp_path / "sources.json", tmp_path / "snapshot.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": [spec]}))
    snapshot.write_text(json.dumps({"schema_version": 2, "slugs": {}}))
    for key, path in {"MANIFEST": manifest, "SNAPSHOT": snapshot,
                      "HOME": tmp_path / "home", "CACHE": tmp_path / "cache"}.items():
        monkeypatch.setenv(f"RAINCLOUD_{key}", str(path))
    for key in ("MIRROR", "OFFLINE", "OUTPUTS"):
        monkeypatch.delenv(f"RAINCLOUD_{key}", raising=False)
    _catalog.load_catalog.cache_clear()
    try:
        dataset = raincloud.load(spec["slug"], format="arrow", build=True)
        assert dataset.to_arrow().num_rows == 3
        assert raincloud.load(spec["slug"]).format == "arrow"
        assert raincloud.load(spec["slug"], offline=True).path() == dataset.path()
        assert not prepared_parquet(spec["slug"]).exists()
        assert not prepared_vortex(spec["slug"]).exists()
    finally:
        _catalog.load_catalog.cache_clear()


def _synth_spec(csv: Path) -> dict:
    return {
        "slug": "synth-e2e",
        "short_name": "Synth E2E",
        "full_name": "Synth E2E",
        "description": "synthetic file:// CSV through the full stage chain",
        "license": {"spdx": "CC0-1.0"},
        "fetch": {"type": "http", "urls": [csv.as_uri()]},
        "extract": {"type": "passthrough"},
        "parse": {"reader": "csv"},
        "transform": {"handler": "tighten_types"},
        "write": {"compression": "zstd"},
        "expect": {"rows": 3},
    }


def test_pipeline_e2e_synthetic_build(tmp_path, monkeypatch):
    """fetch → extract → parse → transform → write_canonical → validate →
    run_exporters end-to-end on a synthetic file:// CSV, all writes under tmp.
    The TABLE path lands the canonical Arrow spine plus its parquet@py +
    vortex@py exports, and all three round-trip."""
    csv = tmp_path / "tiny.csv"
    csv.write_text("n,s\n1,a\n2,b\n3,c\n")
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    # No mirror, no offline; data_root() resolves outputs under RAINCLOUD_HOME.
    monkeypatch.delenv("RAINCLOUD_MIRROR", raising=False)
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)

    from raincloud.pipeline.build import run_one

    spec = _synth_spec(csv)
    ok = run_one(spec, strict=True)
    assert ok, "run_one returned False — see captured stderr for stage failure"

    arrow_path = prepared_arrow("synth-e2e")
    parquet = prepared_parquet("synth-e2e")
    vortex_path = prepared_vortex("synth-e2e")
    assert arrow_path.exists(), arrow_path
    assert parquet.exists(), parquet
    assert vortex_path.exists(), vortex_path

    # The canonical Arrow spine round-trips.
    with pa.ipc.open_file(str(arrow_path)) as reader:
        at = reader.read_all()
    assert at.num_rows == 3 and at.column_names == ["n", "s"]

    # Validate the parquet content.
    tbl = pq.read_table(parquet)
    assert tbl.num_rows == 3
    assert tbl.column_names == ["n", "s"]
    # tighten_types should narrow the small-int 'n' column away from int64.
    n_type = str(tbl.schema.field("n").type)
    assert n_type != "int64", f"tighten_types did not narrow 'n' (got {n_type})"

    # Validate the vortex output round-trips.
    import vortex

    vt = vortex.open(str(vortex_path)).to_arrow().read_all()
    assert vt.num_rows == 3 and vt.column_names == ["n", "s"]


def test_run_one_survives_baseexception_from_exporter(tmp_path, monkeypatch):
    """A native exporter surfacing a Rust panic (pyo3_runtime.PanicException
    subclasses BaseException, not Exception — e.g. Vortex 0.69 on a shredded
    variant struct) must degrade to a per-slug FAILED, not escape run_one and
    abort a --all batch. Simulate it with a BaseException-raising run_exporters."""
    csv = tmp_path / "tiny.csv"
    csv.write_text("n,s\n1,a\n2,b\n3,c\n")
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("RAINCLOUD_MIRROR", raising=False)
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)

    import raincloud.pipeline.build as build

    class _FakePanic(BaseException):
        """Stand-in for pyo3_runtime.PanicException (a BaseException)."""

    def _boom(spec, canonical):
        raise _FakePanic("not implemented")

    monkeypatch.setattr(build, "run_exporters", _boom)

    # Must return False (handled), NOT propagate the BaseException.
    ok = build.run_one(_synth_spec(csv), strict=True)
    assert ok is False

    # The earlier stages still ran — the canonical Arrow spine landed before the
    # (simulated) exporter panic.
    arrow_path = prepared_arrow("synth-e2e")
    assert arrow_path.exists()


# --------------------------------------------------------------------------
# Synthetic streaming-handler e2e.
#
# The migrated ParquetWriter streaming handlers now write the canonical Arrow
# spine themselves (`open_canonical_writer`) and return []; run_one's
# migrated-streaming branch then flows validate → run_exporters (the same tail
# as the table path). These drive real (tiny) inputs through the whole chain to
# prove arrow/ + parquet/ + vortex/ all land and round-trip — coverage the
# streaming handlers previously had zero of in the default suite.
# --------------------------------------------------------------------------


def _streaming_spec(slug: str, src: Path, handler: str, reader: str,
                    params: dict, rows: int) -> dict:
    return {
        "slug": slug,
        "short_name": slug,
        "full_name": slug,
        "description": "synthetic streaming build through the full stage chain",
        "license": {"spdx": "CC0-1.0"},
        "fetch": {"type": "http", "urls": [src.as_uri()]},
        "extract": {"type": "passthrough"},
        "parse": {"reader": reader},
        "transform": {"handler": handler, "params": params},
        "expect": {"rows": rows},
    }


def _dly_line(station: str, year: int, month: int, element: str,
              day_values: dict[int, int]) -> str:
    """Build one fixed-width GHCN-Daily `.dly` record line.

    Header: station(A11) year(I4) month(I2) element(A4); then 31 day fields of
    value(I5) mflag(A1) qflag(A1) sflag(A1). -9999 marks a missing day.
    """
    header = f"{station:<11}{year:04d}{month:02d}{element:<4}"
    body = "".join(f"{day_values.get(d, -9999):5d}   " for d in range(1, 32))
    return header + body


def _build_jsonl(tmp_path: Path):
    src = tmp_path / "tiny.jsonl"
    src.write_text('{"a": 1}\n{"b": [1, 2]}\n{"c": "x"}\n')
    spec = _streaming_spec("synth-jsonl", src, "jsonl_as_string_parse",
                           "custom", {}, 3)
    return src, spec, 3, ["raw_json"]


def _build_stackexchange(tmp_path: Path):
    src = tmp_path / "tags.xml"
    src.write_text(
        '<?xml version="1.0" encoding="utf-8"?>\n'
        "<tags>\n"
        '  <row Id="1" TagName="javascript" Count="100" ExcerptPostId="2" '
        'WikiPostId="3" IsModeratorOnly="False" IsRequired="False"/>\n'
        '  <row Id="2" TagName="python" Count="200" ExcerptPostId="4" '
        'WikiPostId="5" IsModeratorOnly="True" IsRequired="False"/>\n'
        "</tags>\n"
    )
    spec = _streaming_spec("synth-stackexchange", src, "stack_exchange_split",
                           "xml", {"table": "tags"}, 2)
    cols = ["Id", "TagName", "Count", "ExcerptPostId", "WikiPostId",
            "IsModeratorOnly", "IsRequired"]
    return src, spec, 2, cols


def _build_ghcn(tmp_path: Path):
    src = tmp_path / "USW00094728.dly"
    lines = [
        _dly_line("USW00094728", 2020, 1, "TMAX", {1: 100, 2: 150, 3: 200}),
        _dly_line("USW00094728", 2020, 1, "TMIN", {1: -50, 2: -30}),
    ]
    src.write_text("\n".join(lines) + "\n")
    spec = _streaming_spec("synth-ghcn", src, "ghcn_daily_parse", "custom", {}, 5)
    cols = ["station_id", "date", "element", "value", "mflag", "qflag", "sflag"]
    return src, spec, 5, cols


@pytest.mark.parametrize(
    "builder",
    [_build_jsonl, _build_stackexchange, _build_ghcn],
    ids=["jsonl", "stackexchange", "ghcn"],
)
def test_pipeline_e2e_streaming_build(tmp_path, monkeypatch, builder):
    """fetch → extract → parse → transform (streaming handler writes canonical
    Arrow directly) → validate → run_exporters, all under tmp. Each migrated
    streaming slug lands arrow/ + parquet/ + vortex/, and all three round-trip
    (rows + columns). Exercises the migrated-streaming build branch incl.
    run_exporters and the vortex re-batch on real tiny outputs."""
    src, spec, rows, columns = builder(tmp_path)

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("RAINCLOUD_MIRROR", raising=False)
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)

    from raincloud.pipeline.build import run_one

    ok = run_one(spec, strict=True)
    assert ok, "run_one returned False — see captured stderr for stage failure"

    slug = spec["slug"]
    arrow_path = prepared_arrow(slug)
    parquet = prepared_parquet(slug)
    vortex_path = prepared_vortex(slug)
    assert arrow_path.exists(), arrow_path
    assert parquet.exists(), parquet
    assert vortex_path.exists(), vortex_path

    # The canonical Arrow spine round-trips.
    with pa.ipc.open_file(str(arrow_path)) as reader:
        at = reader.read_all()
    assert at.num_rows == rows
    assert at.column_names == columns

    # The parquet export round-trips.
    tbl = pq.read_table(parquet)
    assert tbl.num_rows == rows
    assert tbl.column_names == columns

    # The vortex export round-trips.
    import vortex

    vt = vortex.open(str(vortex_path)).to_arrow().read_all()
    assert vt.num_rows == rows
    assert vt.column_names == columns


# --------------------------------------------------------------------------
# Synthetic DuckDB-VARIANT streaming e2e.
#
# jsonbench_variant_parse + wikipedia_variant_parse now stream through the
# DuckDB→canonical-Arrow bridge (variant_to_parquet_variant projection →
# to_arrow_reader(batch_size) → VARIANT_EXT stamp → open_canonical_writer) and
# return [], sharing run_one's unified validate → run_exporters tail. These
# drive tiny real inputs end-to-end and assert the canonical carries the
# VARIANT marker (_is_variant_field) and every row survives.
#
# JSONBench retains its manifest export selection. Wikipedia's current Parquet
# source has string event identifiers and the test exercises Vortex 0.86.1 as
# well as Parquet, comparing the exported VARIANT storage to canonical Arrow.
# --------------------------------------------------------------------------


def _variant_spec(slug: str, src: Path, handler: str, extract: dict,
                  params: dict, rows: int) -> dict:
    return {
        "slug": slug,
        "short_name": slug,
        "full_name": slug,
        "description": "synthetic DuckDB-VARIANT streaming build",
        "license": {"spdx": "CC0-1.0"},
        "fetch": {"type": "http", "urls": [src.as_uri()]},
        "extract": extract,
        "parse": {"reader": "custom"},
        "transform": {"handler": handler, "params": params},
        "expect": {"rows": rows},
        "export": {"formats": ["parquet"],
                   "notes": "Vortex cannot encode the shredded VARIANT struct"},
    }


def test_pipeline_e2e_jsonbench_variant_build(tmp_path, monkeypatch):
    """fetch → extract → parse → transform (jsonbench_variant_parse streams the
    DuckDB VARIANT bridge into the canonical Arrow writer) → validate →
    run_exporters, strict, all under tmp. Asserts arrow/ + parquet/ land (vortex
    is skipped — see module note), the `data` column is VARIANT in the canonical
    (_is_variant_field), and the row count matches."""
    import gzip
    import json

    from raincloud.pipeline.discovery import _is_variant_field

    src = tmp_path / "bluesky.json.gz"
    events = [
        {"did": "did:plc:aaa", "kind": "commit",
         "commit": {"operation": "create", "collection": "app.bsky.feed.post"}},
        {"did": "did:plc:bbb", "kind": "identity", "seq": 42},
        {"did": "did:plc:ccc", "kind": "account", "active": True},
    ]
    with gzip.open(src, "wt", encoding="utf-8") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("RAINCLOUD_MIRROR", raising=False)
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)

    from raincloud.pipeline.build import run_one

    spec = _variant_spec("synth-jsonbench", src, "jsonbench_variant_parse",
                         {"type": "passthrough"}, {}, 3)
    ok = run_one(spec, strict=True)
    assert ok, "run_one returned False — see captured stderr for stage failure"

    arrow_path = prepared_arrow("synth-jsonbench")
    parquet = prepared_parquet("synth-jsonbench")
    vortex_path = prepared_vortex("synth-jsonbench")
    assert arrow_path.exists(), arrow_path
    assert parquet.exists(), parquet
    # Vortex 0.69 cannot encode the shredded VARIANT struct — legitimately skipped.
    assert not vortex_path.exists(), "vortex should be skipped for a VARIANT slug"

    with pa.ipc.open_file(str(arrow_path)) as reader:
        at = reader.read_all()
    assert at.num_rows == 3
    assert at.column_names == ["data"]
    data_field = at.schema.field("data")
    assert pa.types.is_struct(data_field.type)
    assert _is_variant_field(data_field) is True

    tbl = pq.read_table(parquet)
    assert tbl.num_rows == 3
    assert tbl.column_names == ["data"]


@pytest.mark.parametrize("batch_rows", [1, 4096])
def test_pipeline_e2e_wikipedia_variant_build(tmp_path, monkeypatch, batch_rows):
    """Real Parquet ZIP → decoded VARIANT values → canonical + exported files."""
    import json
    import zipfile

    from raincloud.pipeline.discovery import _is_variant_field

    en = [
        {"name": "Alpha", "url": "http://en/a", "identifier": 1,
         "sections": [{"type": "section", "name": "Intro", "content": "hi"}],
         "infoboxes": [{"type": "infobox", "name": "IB", "value": "x"}]},
        {"name": "Beta", "url": "http://en/b", "identifier": 2,
         "sections": [{"type": "section", "name": "Body"}],
         "infoboxes": []},
    ]
    fr = [
        {"name": "Gamma", "url": "http://fr/g", "identifier": 3,
         "sections": [{"type": "section", "name": "Intro",
                       "children": [{"type": "paragraph", "value": "z"}]}],
         "infoboxes": [{"type": "infobox", "name": "Boite"}]},
    ]
    src = tmp_path / "wiki.zip"
    with zipfile.ZipFile(src, "w") as z:
        for language, articles in [("en", en), ("fr", fr)]:
            table = pa.Table.from_pylist([{**article,
                "sections": json.dumps(article["sections"]),
                "infoboxes": json.dumps(article["infoboxes"])} for article in articles])
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink)
            z.writestr(f"{language}wiki/data/part_0.parquet", sink.getvalue().to_pybytes())
        z.writestr("enwiki/schema.json", "{}")

    monkeypatch.setenv("RAINCLOUD_BATCH_ROWS", str(batch_rows))
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("RAINCLOUD_MIRROR", raising=False)
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)

    from raincloud.pipeline.build import run_one

    spec = _variant_spec("synth-wikipedia", src, "wikipedia_variant_parse",
                         {"type": "zip", "include": ["*wiki/data/*.parquet"]}, {}, 3)
    del spec["export"]  # the default formats: Vortex as well as Parquet
    pytest.importorskip("vortex")
    ok = run_one(spec, strict=True)
    assert ok, "run_one returned False — see captured stderr for stage failure"

    arrow_path = prepared_arrow("synth-wikipedia")
    parquet = prepared_parquet("synth-wikipedia")
    vortex_path = prepared_vortex("synth-wikipedia")
    assert arrow_path.exists(), arrow_path
    assert parquet.exists(), parquet
    assert vortex_path.exists()

    with pa.ipc.open_file(str(arrow_path)) as reader:
        at = reader.read_all()
    assert at.num_rows == 3
    for vc in ("sections", "infoboxes"):
        f = at.schema.field(vc)
        assert pa.types.is_struct(f.type), f"{vc} not a shredded struct"
        assert _is_variant_field(f) is True, f"{vc} missing VARIANT marker"
    # Independent typed-value expressions detect JSON being encoded as a string.
    from raincloud import duckdb_connect
    expressions = [
        "[{'type':'section','name':'Intro','content':'hi'}]",
        "[{'type':'section','name':'Body'}]",
        "[{'type':'section','name':'Intro','children':[{'type':'paragraph','value':'z'}]}]",
    ]
    with duckdb_connect() as con:
        expected = [con.execute(f"SELECT variant_to_parquet_variant(CAST({expr} AS VARIANT))").fetchone()[0]
                    for expr in expressions]
    assert at['sections'].to_pylist() == expected
    with pa.ipc.open_file(str(arrow_path)) as reader:
        assert all(reader.get_batch(i).num_rows <= batch_rows for i in range(reader.num_record_batches))
    # Typed columns survive to the canonical.
    assert set(at.column_names) >= {"name", "url", "identifier", "sections", "infoboxes"}

    tbl = pq.read_table(parquet)
    assert tbl.num_rows == 3
    import vortex

    from raincloud.pipeline.export.compare import values_equal
    assert values_equal(tbl, at) == (True, '')
    with vortex.open(str(vortex_path)).to_arrow(batch_size=2) as reader:
        assert values_equal(reader.read_all(), at) == (True, '')


def test_pipeline_e2e_public_bi_merge_build(tmp_path, monkeypatch):
    """Deferred 3.2b coverage: public_bi_merge 2-partition synthetic e2e. Two
    `Foo_N.csv` partitions of DIFFERING width (partition 2 adds column `c`) plus
    matching `Foo_N.table.sql` schemas exercise the union-by-name merge. Drives
    the full chain (strict); asserts arrow/ + parquet/ + vortex/ land, the
    unified column order is [a, b, c], the row count is the partition sum, and
    partition 1's missing `c` is NULL-filled."""
    (tmp_path / "Foo_1.table.sql").write_text('CREATE TABLE "Foo_1"(a INTEGER, b VARCHAR);\n')
    (tmp_path / "Foo_2.table.sql").write_text('CREATE TABLE "Foo_2"(a INTEGER, b VARCHAR, c INTEGER);\n')
    (tmp_path / "Foo_1.csv").write_text("1|x\n2|y\n")
    (tmp_path / "Foo_2.csv").write_text("3|z|30\n4|w|40\n")
    names = ["Foo_1.table.sql", "Foo_2.table.sql", "Foo_1.csv", "Foo_2.csv"]
    urls = [(tmp_path / n).as_uri() for n in names]

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("RAINCLOUD_MIRROR", raising=False)
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)

    from raincloud.pipeline.build import run_one

    spec = {
        "slug": "synth-public-bi",
        "short_name": "synth-public-bi",
        "full_name": "synth-public-bi",
        "description": "synthetic Public BI 2-partition union-by-name build",
        "license": {"spdx": "CC0-1.0"},
        "fetch": {"type": "http", "urls": urls},
        "extract": {"type": "passthrough"},
        "parse": {"reader": "custom"},
        "transform": {"handler": "public_bi_merge", "params": {"workload": "Foo"}},
        "expect": {"rows": 4},
    }
    ok = run_one(spec, strict=True)
    assert ok, "run_one returned False — see captured stderr for stage failure"

    arrow_path = prepared_arrow("synth-public-bi")
    parquet = prepared_parquet("synth-public-bi")
    vortex_path = prepared_vortex("synth-public-bi")
    assert arrow_path.exists(), arrow_path
    assert parquet.exists(), parquet
    assert vortex_path.exists(), vortex_path

    with pa.ipc.open_file(str(arrow_path)) as reader:
        at = reader.read_all()
    assert at.column_names == ["a", "b", "c"]
    assert at.num_rows == 4
    # union-by-name: partition 1 (a=1,2) has no `c` → NULL-filled; partition 2 has it.
    assert at.column("a").to_pylist() == [1, 2, 3, 4]
    assert at.column("c").to_pylist() == [None, None, 30, 40]

    import vortex
    vt = vortex.open(str(vortex_path)).to_arrow().read_all()
    assert vt.num_rows == 4 and vt.column_names == ["a", "b", "c"]


def test_pipeline_e2e_osm_pbf_split_build(tmp_path, monkeypatch):
    """Deferred 3.2b coverage: osm_pbf_split e2e on a REAL tiny `.pbf` (written
    cheaply via osmium.SimpleWriter — 2 nodes). Drives the full chain (strict)
    with batch_size=1 so the streaming handler flushes multiple batches; asserts
    arrow/ + parquet/ + vortex/ land, the nested `tags` (list<struct<key,value>>)
    and WKB `geometry` (binary) survive, and the GeoParquet `geo` schema metadata
    rides canonical → parquet."""
    osmium = pytest.importorskip("osmium")
    import osmium.osm.mutable as mut

    src = tmp_path / "tiny.osm.pbf"
    w = osmium.SimpleWriter(str(src))
    w.add_node(mut.Node(id=1, location=(13.4, 52.5), tags={"amenity": "cafe"}, version=1))
    w.add_node(mut.Node(id=2, location=(13.5, 52.6), tags={"name": "Park"}, version=1))
    w.close()

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    monkeypatch.delenv("RAINCLOUD_MIRROR", raising=False)
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)

    from raincloud.pipeline.build import run_one

    spec = {
        "slug": "synth-osm",
        "short_name": "synth-osm",
        "full_name": "synth-osm",
        "description": "synthetic OSM PBF nodes build",
        "license": {"spdx": "CC0-1.0"},
        "fetch": {"type": "http", "urls": [src.as_uri()]},
        "extract": {"type": "passthrough"},
        "parse": {"reader": "pbf"},
        "transform": {"handler": "osm_pbf_split",
                      "params": {"element_kind": "nodes", "batch_size": 1}},
        "expect": {"rows": 2},
    }
    ok = run_one(spec, strict=True)
    assert ok, "run_one returned False — see captured stderr for stage failure"

    arrow_path = prepared_arrow("synth-osm")
    parquet = prepared_parquet("synth-osm")
    vortex_path = prepared_vortex("synth-osm")
    assert arrow_path.exists(), arrow_path
    assert parquet.exists(), parquet
    assert vortex_path.exists(), vortex_path

    with pa.ipc.open_file(str(arrow_path)) as reader:
        at = reader.read_all()
    assert at.num_rows == 2
    assert at.column_names == ["id", "version", "timestamp", "lon", "lat", "tags", "geometry"]
    assert pa.types.is_list(at.schema.field("tags").type)
    assert pa.types.is_binary(at.schema.field("geometry").type)
    assert at.column("tags").to_pylist()[0] == [{"key": "amenity", "value": "cafe"}]

    # GeoParquet `geo` schema metadata survives canonical → parquet.
    tbl = pq.read_table(parquet)
    assert tbl.num_rows == 2
    assert b"geo" in (tbl.schema.metadata or {}), "geo metadata lost in parquet"

    import vortex
    vt = vortex.open(str(vortex_path)).to_arrow().read_all()
    assert vt.num_rows == 2


def test_handlers_identity_and_tighten_types_function():
    """Direct invocation: handlers *function* on synthetic Arrow tables."""
    # Handlers resolve through `get`: the registry holds import paths so that
    # listing names costs no imports, which means the package no longer re-exports
    # each handler function as an attribute.
    from raincloud.pipeline.handlers import get
    identity, tighten_types = get("identity"), get("tighten_types")

    src = pa.table(
        {"x": pa.array([1, 2, 3], type=pa.int64()), "s": ["a", "b", "c"]}
    )
    # identity: returns the single input table as the output.
    # Signature: (spec, [(Path, table)]) -> [(slug_str, table)]
    out = identity({"slug": "x"}, [(Path("ignore.csv"), src)])
    assert isinstance(out, list) and out and out[0][1].num_rows == 3

    # tighten_types: narrows int64 with tiny values to a smaller int dtype.
    out2 = tighten_types({"slug": "x"}, [(Path("ignore.csv"), src)])
    assert isinstance(out2, list) and out2
    narrowed = out2[0][1]
    assert str(narrowed.schema.field("x").type) != "int64"


def test_direct_build_is_recorded_and_reused_offline(tmp_path, monkeypatch):
    # The catalog names a stale artifact; the build records what it produced
    # in the build record, so the next read takes the new file offline.
    import raincloud
    from raincloud.pipeline import build

    csv = tmp_path / "tiny.csv"
    csv.write_text("n,s\n1,a\n2,b\n3,c\n")
    recipe = _synth_spec(csv)
    recipe["export"] = {"formats": []}
    manifest, snapshot = tmp_path / "sources.json", tmp_path / "snapshot.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": [recipe]}))
    snapshot.write_text(json.dumps({"schema_version": 2, "slugs": {
        recipe["slug"]: {"arrow_sha256": "0" * 64, "arrow_bytes": 1},
    }}))
    for key, path in {"MANIFEST": manifest, "SNAPSHOT": snapshot,
                      "HOME": tmp_path / "home", "CACHE": tmp_path / "cache"}.items():
        monkeypatch.setenv(f"RAINCLOUD_{key}", str(path))
    assert build.run_one(recipe, strict=True)
    path = prepared_arrow(recipe["slug"])
    from raincloud import _builds
    recorded = _builds.read(raincloud.resolve_config().data_dir)
    (entry,) = recorded.values()
    assert entry["bytes"] == path.stat().st_size and entry["writer"] == "canonical"
    assert json.loads(snapshot.read_text())["slugs"][recipe["slug"]]["arrow_bytes"] == 1
    assert raincloud.load(recipe["slug"], format="arrow", offline=True).path() == path
    assert not (tmp_path / "cache").exists()


def test_builds_leave_the_catalog_alone_and_serve_from_the_build_record(tmp_path, monkeypatch):
    # The catalog is shared and changes only when a maintainer commits it. What
    # a build made -- which drift can make differ from the catalog's file -- is
    # this install's build record, which the loader honours for this install
    # while the recipe is unchanged.
    import raincloud
    from raincloud import _builds
    from raincloud._resolve import artifact_key
    from raincloud.exceptions import ArtifactNotFound
    from raincloud.pipeline import build
    from raincloud.pipeline.export.__main__ import main as export_main
    csv = tmp_path / "tiny.csv"
    csv.write_text("n,s\n1,a\n2,b\n3,c\n")
    recipe = _synth_spec(csv)
    recipe["export"] = {"formats": ["parquet"]}
    manifest, snapshot = tmp_path / "sources.json", tmp_path / "snapshot.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": [recipe]}))
    # The catalog names a file this build will not reproduce: an upstream drifted.
    snapshot.write_text(json.dumps({"schema_version": 2, "slugs": {recipe["slug"]: {
        "parquet_sha256": "0" * 64, "parquet_bytes": 1, "parquet_writer": "py"}}}))
    catalog = snapshot.read_bytes()
    for key, path in {"MANIFEST": manifest, "SNAPSHOT": snapshot,
                      "HOME": tmp_path / "home", "CACHE": tmp_path / "cache"}.items():
        monkeypatch.setenv(f"RAINCLOUD_{key}", str(path))
    assert build.run_one(recipe, strict=True)
    assert export_main([recipe["slug"]]) == 0
    assert snapshot.read_bytes() == catalog

    config = raincloud.resolve_config()
    key = artifact_key(recipe["slug"], "parquet", 2)
    built = _builds.lookup(config.data_dir, key)
    local = config.data_dir / key
    assert built["bytes"] == local.stat().st_size and built["writer"] == "py"
    assert raincloud.load(recipe["slug"], format="parquet", offline=True).path() == local

    # A new recipe is not served the old recipe's build.
    recipe["expect"] = {"rows": 3, "notes": "changed"}
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": [recipe]}))
    raincloud._catalog.load_catalog.cache_clear()
    with pytest.raises(ArtifactNotFound, match="earlier recipe"):
        raincloud.load(recipe["slug"], format="parquet", offline=False).path()
