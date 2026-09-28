# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Ingest-stage gate: VARIANT parsing, fetch integrity, extraction and stage CLIs.

Everything here runs on tiny local inputs. The HTTP cases use a server bound to
127.0.0.1 inside the test; nothing reaches the network.
"""
from __future__ import annotations

import bz2
import copy
import gzip
import io
import json
import os
import sys
import threading
import time
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pyarrow as pa
import pytest

import raincloud
from raincloud.catalogs import operation
from raincloud.pipeline import custom_fetch, extract, fetch, generate
from raincloud.pipeline.spec import prepared_arrow

# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------

def config_for(tmp_path, recipes):
    manifest = tmp_path / "catalog.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": recipes}))
    return raincloud.resolve_config(no_config=True, manifest=manifest,
        data_dir=tmp_path / "data", raw_dir=tmp_path / "raw",
        scratch_dir=tmp_path / "scratch", catalog_dir=tmp_path / "catalogs")


def http_recipe(url, slug="tiny"):
    return {"slug": slug, "fetch": {"type": "http", "urls": [url]},
            "extract": {"type": "passthrough"}, "parse": {"reader": "csv"},
            "transform": {"handler": "identity"}, "expect": {"rows": 1},
            "export": {"formats": []}}


def variant_kinds(column: pa.ChunkedArray) -> list[int]:
    """Parquet-variant basic type of each value: 0 primitive, 1 short string,
    2 object, 3 array (the low two bits of the value's header byte)."""
    values = column.combine_chunks().field("value").to_pylist()
    return [v[0] & 0x03 for v in values]


class _Server:
    """A one-route HTTP server on 127.0.0.1 with a pluggable handler body."""

    def __init__(self, respond):
        server = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                server.requests += 1
                try:
                    respond(self)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def log_message(self, *args):
                pass

        self.requests = 0
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.httpd.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}/data.bin"
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()
        self.httpd.server_close()


@pytest.fixture
def raw(tmp_path, monkeypatch):
    """Standalone fetch_http: isolated raw dir, no sibling lookup, no proxies."""
    directory = tmp_path / "raw"
    directory.mkdir()
    monkeypatch.setenv("RAINCLOUD_RAW_DOWNLOADS", str(directory))
    monkeypatch.setenv("no_proxy", "*")
    monkeypatch.setattr(fetch, "_find_sibling_cache", lambda *a, **kw: None)
    return directory


# --------------------------------------------------------------------------
# VARIANT handlers parse JSON text into structure
# --------------------------------------------------------------------------

def test_jsonbench_variant_is_an_object_and_files_limit_holds(tmp_path):
    from raincloud.pipeline.handlers.jsonbench_variant_parse import jsonbench_variant_parse

    files = []
    for n, events in enumerate([[{"kind": "commit", "seq": 1}, {"kind": "identity"}],
                                [{"kind": "account", "active": True}]]):
        path = tmp_path / f"file_{n:04d}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
        files.append(path)
    spec = http_recipe("https://example.test/unused", slug="jb")
    with operation(config_for(tmp_path, [spec])):
        assert jsonbench_variant_parse(spec, [(p, None) for p in files], files_limit=1) == []
        with pa.ipc.open_file(str(prepared_arrow("jb"))) as reader:
            table = reader.read_all()
    assert table.num_rows == 2, "files_limit=1 must read only the first file"
    assert variant_kinds(table.column("data")) == [2, 2]


def test_jsonbench_rejoins_wrapped_records_and_fails_on_other_splits(tmp_path, monkeypatch):
    # Upstream wraps a long record onto a second line; neither half is JSON.
    from raincloud.pipeline.handlers import jsonbench_variant_parse as jb

    path = tmp_path / "file_0000.json.gz"
    head = '{"kind": "commit", "text": "split in'
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"kind": "commit"}) + "\n")
        f.write(head + "\n")
        f.write(' two"}\n')
        f.write(json.dumps({"kind": "identity"}) + "\n")
    spec = http_recipe("https://example.test/unused", slug="jb")
    with operation(config_for(tmp_path, [spec])):
        with pytest.raises(ValueError, match=r"line 2 \(36 bytes\) is not a JSON object"):
            jb.jsonbench_variant_parse(spec, [(path, None)])
        monkeypatch.setattr(jb, "WRAP_BYTES", len(head))
        assert jb.jsonbench_variant_parse(spec, [(path, None)]) == []
        with pa.ipc.open_file(str(prepared_arrow("jb"))) as reader:
            table = reader.read_all()
        assert table.num_rows == 3
        assert variant_kinds(table.column("data")) == [2, 2, 2]


def test_factbook_variant_is_an_object(tmp_path):
    from raincloud.pipeline.handlers.factbook_variant_parse import factbook_variant_parse

    country = tmp_path / "factbook.json-master" / "europe" / "fr.json"
    country.parent.mkdir(parents=True)
    country.write_text(json.dumps({"Government": {"Capital": "Paris"}, "Codes": [1, 2]}))
    [(slug, table)] = factbook_variant_parse({"slug": "fb"}, [(country, None)])
    assert slug == "fb" and table.num_rows == 1
    assert table.column("region").to_pylist() == ["europe"]
    assert variant_kinds(table.column("data")) == [2]


def test_duckdb_text_cast_would_have_been_a_string():
    """Why the VARIANT parse goes through JSON: a text cast yields a VARCHAR variant,
    not an object. Pinned so a DuckDB change in either cast shows up here."""
    from raincloud import duckdb_connect

    con = duckdb_connect()
    try:
        text, parsed = con.execute(
            "SELECT variant_typeof(CAST(? AS VARIANT)), variant_typeof(CAST(CAST(? AS JSON) AS VARIANT))",
            ['{"a": 1}', '{"a": 1}']).fetchone()
    finally:
        con.close()
    assert text == "VARCHAR"
    assert parsed.startswith("OBJECT")


# --------------------------------------------------------------------------
# fetch: truncation, deadline, receipts
# --------------------------------------------------------------------------

def test_truncated_body_is_never_committed_or_pinned(raw):
    def respond(handler):
        handler.send_response(200)
        handler.send_header("Content-Length", "100")
        handler.end_headers()
        handler.wfile.write(b"x" * 10)

    with _Server(respond) as server:
        spec = http_recipe(server.url)
        with pytest.raises(ConnectionError, match="10 of the 100 bytes"):
            fetch.fetch_http(spec)
        assert server.requests == 3, "a short body is transient: it is retried"
    target = raw / "tiny"
    assert not (target / "data.bin").exists()
    assert not (target / fetch.RECEIPTS).exists()
    assert [p.name for p in target.iterdir()] == []


def test_complete_body_from_a_real_socket_is_committed(raw):
    def respond(handler):
        handler.send_response(200)
        handler.send_header("Content-Length", "5")
        handler.end_headers()
        handler.wfile.write(b"a\n1\n\n")

    with _Server(respond) as server:
        [path] = fetch.fetch_http(http_recipe(server.url))
    assert path.read_bytes() == b"a\n1\n\n"
    assert fetch._read_receipts(path.parent)["data.bin"]["bytes"] == 5


def test_dripping_server_trips_the_deadline_once_and_leaves_nothing(raw, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_FETCH_DEADLINE", "0.5")
    stop = threading.Event()

    def respond(handler):
        handler.send_response(200)
        handler.send_header("Content-Length", "1000")
        handler.end_headers()
        for _ in range(1000):
            if stop.is_set():
                return
            handler.wfile.write(b"x")
            handler.wfile.flush()
            time.sleep(0.02)

    with _Server(respond) as server:
        started = time.monotonic()
        try:
            with pytest.raises(fetch.FetchDeadlineExceeded):
                fetch.fetch_http(http_recipe(server.url))
        finally:
            stop.set()
        assert time.monotonic() - started < 10
        assert server.requests == 1, "the deadline is not retried"
    assert not (raw / "tiny" / "data.bin").exists()


class _Bytes(io.BytesIO):
    """A urlopen stand-in; each open serves `payload[0]`."""


@pytest.fixture
def served(raw, monkeypatch):
    state = {"payload": b"a\n1\n", "calls": 0}

    def urlopen(req, **kwargs):
        state["calls"] += 1
        return _Bytes(state["payload"])

    monkeypatch.setattr(fetch.urllib.request, "urlopen", urlopen)
    return state


@pytest.mark.parametrize("damage", [b"{not json", b"[]", b'{"d.csv": "text"}'],
                         ids=["undecodable", "not-an-object", "entry-not-an-object"])
def test_damaged_receipts_fail_closed(served, raw, damage):
    spec = http_recipe("https://example.test/d.csv")
    [path] = fetch.fetch_http(spec)
    (path.parent / fetch.RECEIPTS).write_bytes(damage)
    with pytest.raises(ValueError, match="fetch receipts"):
        fetch.fetch_http(spec)
    assert served["calls"] == 1


def test_receipt_size_is_trusted_and_verify_rehashes(served):
    spec = http_recipe("https://example.test/d.csv")
    [path] = fetch.fetch_http(spec)
    path.write_bytes(b"a\n9\n")  # same size, different bytes
    fetch.fetch_http(spec)
    assert served["calls"] == 1
    fetch.fetch_http(spec, verify=True)
    assert served["calls"] == 2
    assert path.read_bytes() == b"a\n1\n"


def test_declared_pin_mismatch_is_refused(served, raw):
    spec = http_recipe("https://example.test/d.csv")
    spec["fetch"]["expected_bytes"] = 99
    with pytest.raises(ValueError, match="pins expected_bytes=99"):
        fetch.fetch_http(spec)
    assert not (raw / "tiny" / "d.csv").exists()


def test_urls_sharing_a_basename_are_refused(served):
    spec = http_recipe("https://example.test/2019/data.csv")
    spec["fetch"]["urls"].append("https://example.test/2020/data.csv")
    with pytest.raises(ValueError, match="both download"):
        fetch.fetch_http(spec)
    assert served["calls"] == 0


def test_interrupted_kaggle_download_leaves_no_trusted_payload(tmp_path, monkeypatch):
    spec = http_recipe("https://www.kaggle.com/datasets/owner/tiny")
    spec["fetch"]["type"] = "kaggle"
    state = {"fail": True}

    class Api:
        def authenticate(self):
            pass

        def dataset_download_files(self, ref, *, path, **kwargs):
            (Path(path) / "tiny.zip").write_bytes(b"partial" if state["fail"] else b"complete")
            if state["fail"]:
                raise OSError("connection reset")

    monkeypatch.setitem(sys.modules, "kaggle", type(sys)("kaggle"))
    sys.modules["kaggle"].KaggleApi = Api
    with operation(config_for(tmp_path, [spec])):
        with pytest.raises(OSError, match="connection reset"):
            fetch.fetch(spec)
        target = fetch.slug_dir("tiny")
        assert fetch._payload_files(target, recursive=True) == []
        state["fail"] = False
        assert [p.read_bytes() for p in fetch.fetch(spec)] == [b"complete"]


@pytest.mark.parametrize("module,kind,extra", [("kaggle", "kaggle", "kaggle"),
                                                ("huggingface_hub", "huggingface", "huggingface")])
def test_missing_fetch_client_names_its_extra(tmp_path, monkeypatch, module, kind, extra):
    from raincloud.exceptions import BuildToolingMissing

    spec = http_recipe("hf://owner/tiny" if kind == "huggingface"
                       else "https://www.kaggle.com/datasets/owner/tiny")
    spec["fetch"]["type"] = kind
    monkeypatch.setitem(sys.modules, module, None)
    with operation(config_for(tmp_path, [spec])):
        with pytest.raises(BuildToolingMissing, match=rf"raincloud\[{extra}\]"):
            fetch.fetch(spec)


def test_public_bi_fetch_caches_everything_behind_receipts(tmp_path, monkeypatch):
    spec = http_recipe("https://example.test/unused", slug="bi-tiny")
    spec["fetch"] = {"type": "custom", "notes": "public_bi_fetch", "urls": []}
    spec["transform"] = {"handler": "public_bi_merge", "params": {"workload": "Tiny"}}
    bodies = {"data-urls.txt": b"http://event.test/Tiny_1.csv.bz2\n",
              "Tiny_1.csv.bz2": b"partition", "Tiny_1.table.sql": b'CREATE TABLE "Tiny_1"(a int);'}
    calls = []

    def urlopen(req, **kwargs):
        calls.append(req.full_url)
        return io.BytesIO(bodies[req.full_url.rsplit("/", 1)[-1]])

    monkeypatch.setattr(fetch.urllib.request, "urlopen", urlopen)
    with operation(config_for(tmp_path, [spec])):
        first = custom_fetch.public_bi_fetch(spec)
        assert [p.name for p in first] == ["Tiny_1.csv.bz2", "Tiny_1.table.sql"]
        assert calls[1] == "https://event.test/Tiny_1.csv.bz2"
        receipts = fetch._read_receipts(first[0].parent)
        assert set(receipts) == {"data-urls.txt", "Tiny_1.csv.bz2", "Tiny_1.table.sql"}
        assert custom_fetch.public_bi_fetch(spec) == first
    assert len(calls) == 3, "a fully cached workload needs no network"


def _public_bi(tmp_path, monkeypatch, bodies, fail=None):
    spec = http_recipe("https://example.test/unused", slug="bi-tiny")
    spec["fetch"] = {"type": "custom", "notes": "public_bi_fetch", "urls": []}
    spec["transform"] = {"handler": "public_bi_merge", "params": {"workload": "Tiny"}}
    calls = []

    def urlopen(req, **kwargs):
        calls.append(req.full_url)
        name = req.full_url.rsplit("/", 1)[-1]
        if fail and name in fail:
            raise fail[name]
        return io.BytesIO(bodies[name])
    monkeypatch.setattr(fetch.urllib.request, "urlopen", urlopen)
    return spec, calls


def test_public_bi_skips_only_a_schema_the_upstream_does_not_have(tmp_path, monkeypatch, capsys):
    import urllib.error
    bodies = {"data-urls.txt": b"http://event.test/Tiny_1.csv.bz2\nhttp://event.test/Tiny_2.csv.bz2\n",
              "Tiny_1.csv.bz2": b"one", "Tiny_2.csv.bz2": b"two", "Tiny_1.table.sql": b"CREATE TABLE t(a int);"}
    missing = urllib.error.HTTPError("https://x/Tiny_2.table.sql", 404, "Not Found", {}, None)
    spec, _ = _public_bi(tmp_path, monkeypatch, bodies, fail={"Tiny_2.table.sql": missing})
    with operation(config_for(tmp_path, [spec])):
        got = custom_fetch.public_bi_fetch(spec)
    assert [p.name for p in got] == ["Tiny_1.csv.bz2", "Tiny_2.csv.bz2", "Tiny_1.table.sql"]
    assert "no schema for partition 2" in capsys.readouterr().err
    # Anything but a definitive 404 -- here, a server error after the retries -- raises.
    broken = urllib.error.HTTPError("https://x/Tiny_2.table.sql", 503, "Unavailable", {}, None)
    spec, _ = _public_bi(tmp_path / "again", monkeypatch, bodies, fail={"Tiny_2.table.sql": broken})
    (tmp_path / "again").mkdir()
    with operation(config_for(tmp_path / "again", [spec])):
        with pytest.raises(urllib.error.HTTPError, match="Unavailable"):
            custom_fetch.public_bi_fetch(spec)


def test_fetch_verify_refetches_a_corrupted_custom_download(tmp_path, monkeypatch):
    bodies = {"data-urls.txt": b"http://event.test/Tiny_1.csv.bz2\n",
              "Tiny_1.csv.bz2": b"partition", "Tiny_1.table.sql": b"CREATE TABLE t(a int);"}
    spec, calls = _public_bi(tmp_path, monkeypatch, bodies)
    with operation(config_for(tmp_path, [spec])):
        (partition, _) = fetch.fetch(spec)
        partition.write_bytes(b"PARTITION")  # same size, other bytes
        assert len(calls) == 3
        fetch.fetch(spec)
        assert len(calls) == 3  # trusted by size
        fetch.fetch(spec, verify=True)
        assert calls[-1].endswith("Tiny_1.csv.bz2") and partition.read_bytes() == b"partition"


def test_raw_input_stages_refuse_a_derived_dataset(tmp_path, capsys):
    recipes = [http_recipe("https://example.test/rows.csv", slug="uci-iris"),
               {"slug": "uci-iris-hydrated", "derive": {"from": "uci-iris", "hydrate": {"columns": {}}}}]
    with operation(config_for(tmp_path, recipes)):
        for main in (fetch.main, extract.main, generate.main):
            with pytest.raises(SystemExit) as exit_:
                main(["uci-iris-hydrated"])
            assert exit_.value.code == 2
            assert "is derived from 'uci-iris'" in capsys.readouterr().err


# --------------------------------------------------------------------------
# refusal paths
# --------------------------------------------------------------------------

def test_whitespace_file_over_the_cell_ceiling_and_ragged_lines(tmp_path, monkeypatch):
    from raincloud.pipeline.handlers.text_whitespace_parse import text_whitespace_parse

    path = tmp_path / "seeds.txt"
    path.write_text((" ".join(["9"] * 10) + "\n") * 3)
    monkeypatch.setenv("RAINCLOUD_MAX_TABLE_CELLS", "20")
    with pytest.raises(ValueError, match="30 cells"):
        text_whitespace_parse({"slug": "s"}, [(path, None)])
    monkeypatch.setenv("RAINCLOUD_MAX_TABLE_CELLS", "0")
    [(_, table)] = text_whitespace_parse({"slug": "s"}, [(path, None)])
    assert table.shape == (3, 10)
    path.write_text("1 2\n3 4\n" + " ".join(["9"] * 10) + "\n")
    with pytest.raises(ValueError, match="seeds.txt line 3 has 10 fields, the first line 2"):
        text_whitespace_parse({"slug": "s"}, [(path, None)])


def _diabetes_zip(path: Path, payload: bytes, *, understate_to: int | None = None) -> Path:
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_STORED) as z:
        z.writestr("diabetes-data.tar.Z", payload)
    if understate_to is not None:
        data = bytearray(path.read_bytes())
        real = len(payload).to_bytes(4, "little")
        fake = understate_to.to_bytes(4, "little")
        # Uncompressed size: offset 22 in the local header, 24 in the central entry.
        assert data[22:26] == real
        data[22:26] = fake
        central = data.index(b"PK\x01\x02")
        assert data[central + 24:central + 28] == real
        data[central + 24:central + 28] = fake
        path.write_bytes(bytes(data))
    return path


def test_diabetes_member_over_its_ceiling_is_refused(tmp_path, monkeypatch):
    from raincloud.pipeline.handlers import uci_diabetes_parse as mod

    monkeypatch.setattr(mod, "_MAX_TARZ_BYTES", 16)
    declared = _diabetes_zip(tmp_path / "declared.zip", b"z" * 100)
    with pytest.raises(ValueError, match="declares 100 bytes"):
        mod.uci_diabetes_parse({"slug": "d"}, [(declared, None)])
    # An understated size cannot smuggle more bytes past the ceiling: zipfile
    # stops at the declared size and the CRC check then refuses the member.
    understated = _diabetes_zip(tmp_path / "understated.zip", b"z" * 100, understate_to=8)
    with pytest.raises((ValueError, zipfile.BadZipFile)):
        mod.uci_diabetes_parse({"slug": "d"}, [(understated, None)])


def test_diabetes_decompressed_output_over_the_limit_is_refused(tmp_path, monkeypatch):
    from raincloud.pipeline.handlers import uci_diabetes_parse as mod

    fake = type(sys)("unlzw3")
    fake.unlzw = lambda data: b"t" * 1000
    monkeypatch.setitem(sys.modules, "unlzw3", fake)
    monkeypatch.setenv("RAINCLOUD_MAX_DECOMPRESSED_BYTES", "100")
    source = _diabetes_zip(tmp_path / "d.zip", b"z" * 10)
    with pytest.raises(ValueError, match="decompressed tar is 1,000 bytes"):
        mod.uci_diabetes_parse({"slug": "d"}, [(source, None)])


def test_public_bi_rejected_rows_and_schemaless_partitions_fail(tmp_path):
    from raincloud.pipeline.handlers.public_bi_merge import public_bi_merge

    schema = tmp_path / "W_1.table.sql"
    schema.write_text('CREATE TABLE "W_1"("a" integer, "b" varchar(4));')
    rejected = tmp_path / "W_1.csv"
    rejected.write_text("1|x|extra\n2|y|extra\n")
    orphan = tmp_path / "W_2.csv"
    orphan.write_text("3|z\n")
    spec = http_recipe("https://example.test/unused", slug="bi-w")
    with operation(config_for(tmp_path, [spec])):
        with pytest.raises(FileNotFoundError, match="no declared schema .* for partition\\(s\\) W_2.csv"):
            public_bi_merge(spec, [(schema, None), (rejected, None), (orphan, None)], workload="W")
        with pytest.raises(ValueError, match=r"W_1\.csv: .*Expected 2 columns, got 3"):
            public_bi_merge(spec, [(schema, None), (rejected, None)], workload="W")


# --------------------------------------------------------------------------
# extract
# --------------------------------------------------------------------------

@pytest.fixture
def workdir(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_WORKDIR", str(tmp_path / "wd"))
    return tmp_path / "wd"


def _seven_zip(tmp_path, members):
    py7zr = pytest.importorskip("py7zr")
    source = tmp_path / "src"
    source.mkdir()
    (source / "a.txt").write_text("a\n1\n")
    (source / "d").mkdir()
    (source / "link").symlink_to("a.txt")
    archive = tmp_path / "x.7z"
    with py7zr.SevenZipFile(archive, "w") as z:
        for local, arcname in members:
            z.write(source / local, arcname)
    return archive


def test_7z_member_escaping_the_workdir_is_refused(tmp_path, workdir):
    archive = _seven_zip(tmp_path, [("a.txt", "../evil.txt")])
    with pytest.raises(ValueError, match="escapes the scratch directory"):
        extract.extract_7z({"slug": "z"}, [archive])
    assert not (workdir / "evil.txt").exists() and not (tmp_path / "evil.txt").exists()


def test_7z_lists_only_regular_files_and_never_creates_symlinks(tmp_path, workdir, capsys):
    archive = _seven_zip(tmp_path, [("d", "d"), ("link", "link"), ("a.txt", "d/b.txt")])
    out = extract.extract_7z({"slug": "z"}, [archive])
    assert [p.relative_to((workdir / "z").resolve()).as_posix() for p in out] == ["d/b.txt"]
    assert not os.path.lexists(workdir / "z" / "link")
    assert "[skip] link" in capsys.readouterr().err


def test_same_member_in_two_inputs_is_refused(tmp_path, workdir):
    archives = []
    for n in (1, 2):
        archive = tmp_path / f"part{n}.zip"
        with zipfile.ZipFile(archive, "w") as z:
            z.writestr("t.csv", f"a\n{n}\n")
        archives.append(archive)
    with pytest.raises(ValueError, match="both extract to"):
        extract.extract({"slug": "two", "extract": {"type": "zip"}}, archives)


def test_bz2_cache_follows_a_refetched_source(tmp_path, workdir):
    source = tmp_path / "raw" / "t.csv.bz2"
    source.parent.mkdir()
    source.write_bytes(bz2.compress(b"a\n1\n"))
    spec = {"slug": "bz", "extract": {"type": "bz2"}}
    assert extract.extract(spec, [source])[0].read_bytes() == b"a\n1\n"
    replacement = source.with_name(".t.csv.bz2.part")
    replacement.write_bytes(bz2.compress(b"a\n2\n"))
    os.replace(replacement, source)  # how a refetch lands
    assert extract.extract(spec, [source])[0].read_bytes() == b"a\n2\n"


@pytest.mark.parametrize("kind", ["custom", "tgz"])
def test_undeclared_extract_types_are_unknown(kind):
    with pytest.raises(ValueError, match="unknown extract.type"):
        extract.extract({"slug": "x", "extract": {"type": kind}}, [])


# --------------------------------------------------------------------------
# stage CLIs select datasets like build does
# --------------------------------------------------------------------------

def _cli_config(tmp_path):
    recipes = [http_recipe("https://example.test/rows.csv", slug="uci-iris"),
               {"slug": "gen-left", "fetch": {"type": "generated", "generator": "fixture",
                "version": "1", "parameters": {"size": 2}, "output": "left"}}]
    return config_for(tmp_path, recipes)


@pytest.mark.parametrize("main", [fetch.main, extract.main, generate.main],
                         ids=["fetch", "extract", "generate"])
def test_stage_cli_unknown_slug_exits_2_with_a_suggestion(tmp_path, capsys, main):
    with operation(_cli_config(tmp_path)):
        with pytest.raises(SystemExit) as exit_:
            main(["uci-irs"])
    assert exit_.value.code == 2
    assert "Did you mean uci-iris?" in capsys.readouterr().err


@pytest.mark.parametrize("main", [fetch.main, extract.main, generate.main],
                         ids=["fetch", "extract", "generate"])
def test_stage_cli_with_no_selection_does_nothing(tmp_path, capsys, main, monkeypatch):
    monkeypatch.setattr(fetch.urllib.request, "urlopen",
                        lambda *a, **k: pytest.fail("a bare stage CLI must not download"))
    with operation(_cli_config(tmp_path)):
        with pytest.raises(SystemExit) as exit_:
            main([])
    assert exit_.value.code == 2
    assert "nothing selected" in capsys.readouterr().err


@pytest.mark.parametrize("main", [fetch.main, extract.main, generate.main],
                         ids=["fetch", "extract", "generate"])
def test_stage_cli_help_prints_usage_without_fetching(capsys, main, monkeypatch):
    monkeypatch.setattr(fetch.urllib.request, "urlopen",
                        lambda *a, **k: pytest.fail("--help must not download"))
    with pytest.raises(SystemExit) as exit_:
        main(["uci-iris", "--help"])
    assert exit_.value.code == 0
    assert "usage:" in capsys.readouterr().out


def test_generate_cli_refuses_a_dataset_that_is_not_generated(tmp_path, capsys):
    with operation(_cli_config(tmp_path)):
        with pytest.raises(SystemExit) as exit_:
            generate.main(["uci-iris"])
    assert exit_.value.code == 2
    assert "not generated datasets: uci-iris" in capsys.readouterr().err


# --------------------------------------------------------------------------
# generate: cache hits, superset receipts, refresh after damage
# --------------------------------------------------------------------------

class _Generator:
    outputs = {"left": "left.bin", "right": "right.bin"}

    def __init__(self):
        self.calls = 0

    def validate(self, parameters):
        pass

    def generate(self, recipe, destination, scratch):
        self.calls += 1
        for name, filename in self.outputs.items():
            (destination / filename).write_bytes(f"{name}-{self.calls}".encode())
        return {}


@pytest.fixture
def group(tmp_path, monkeypatch):
    for key in ("RAINCLOUD_HOME", "RAINCLOUD_MANIFEST", "RAINCLOUD_SNAPSHOT"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RAINCLOUD_NO_CONFIG", "1")
    monkeypatch.setenv("RAINCLOUD_RAW_DOWNLOADS", str(tmp_path / "raw"))
    monkeypatch.setenv("RAINCLOUD_WORKDIR", str(tmp_path / "scratch"))
    producer = _Generator()
    monkeypatch.setitem(generate.REGISTRY, "fixture", producer)
    spec = {"slug": "left", "fetch": {"type": "generated", "generator": "fixture", "version": "1",
            "parameters": {"size": 2}, "output": "left"}}
    return spec, producer


def test_generation_announces_the_whole_group(group, capsys):
    spec, _ = group
    generate.fetch_generated(spec)
    assert "whole fixture" in capsys.readouterr().err


def test_a_hit_hashes_only_the_requested_member(group):
    spec, producer = group
    left = generate.fetch_generated(spec)[0]
    right = left.with_name("right.bin")
    right.write_bytes(b"RIGHT-1")  # same size, different bytes
    assert generate.fetch_generated(spec) == [left]
    with pytest.raises(ValueError, match="checksum mismatch: right"):
        generate.cached_outputs(spec["fetch"], verify=True)
    sibling = copy.deepcopy(spec)
    sibling["fetch"]["output"] = "right"
    with pytest.raises(ValueError, match="checksum mismatch: right.*--refresh left"):
        generate.fetch_generated(sibling)
    assert producer.calls == 1


def test_a_receipt_with_extra_outputs_is_still_a_hit(group, monkeypatch):
    spec, producer = group
    left = generate.fetch_generated(spec)[0]
    monkeypatch.setattr(producer, "outputs", {"left": "left.bin"})
    assert generate.fetch_generated(spec) == [left]
    assert producer.calls == 1


def test_refresh_recovers_a_damaged_receipt(group):
    spec, producer = group
    generate.fetch_generated(spec)
    pointer = generate.group_root(spec["fetch"]) / "current.json"
    pointer.write_text("{damaged")
    with pytest.raises(ValueError, match="--refresh left"):
        generate.fetch_generated(spec)
    with pytest.warns(UserWarning, match="previous generated receipt is invalid"):
        [path] = generate.fetch_generated(spec, refresh=True)
    assert path.read_bytes() == b"left-2"
    assert "previous_receipt_invalid" in json.loads(pointer.read_text())
    assert generate.fetch_generated(spec) == [path]


# --------------------------------------------------------------------------
# handler and helper edges
# --------------------------------------------------------------------------

def test_tighten_types_pads_a_column_one_csv_lacks():
    from raincloud.pipeline.handlers.tighten_types import tighten_types

    first = pa.table({"a": [1, 2], "b": ["x", "y"]})
    second = pa.table({"a": [3]})
    [(_, table)] = tighten_types({"slug": "t"}, [(Path("1.csv"), first), (Path("2.csv"), second)])
    assert table.column("a").to_pylist() == [1, 2, 3]
    assert table.column("b").to_pylist() == ["x", "y", None]


def test_openlibrary_empty_timestamp_is_null_and_totals_report_once(tmp_path, capsys):
    from raincloud.pipeline.handlers.openlibrary_parse import openlibrary_parse

    source = tmp_path / "dump.txt"
    source.write_text("/type/work\t/works/A\t1\t\t{}\n/type/redirect\t/works/B\t1\t\t{}\n")
    [(_, stream)] = openlibrary_parse({"slug": "ol"}, [(source, None)], record_type="work")
    for _ in range(2):
        with stream.open() as batches:
            rows = [r for item in batches for r in item.batch.to_pylist()]
    assert [r["last_modified"] for r in rows] == [None, None]
    captured = capsys.readouterr()
    assert captured.out.count("total: 2 records") == 1
    assert "1 record(s) in dump.txt are not /type/work" in captured.err


def test_attach_variant_refuses_an_ambiguous_column_name():
    from raincloud.pipeline.variant import attach_variant_schema

    schema = pa.schema([("v", pa.binary()), ("v", pa.binary())])
    with pytest.raises(KeyError, match="ambiguous"):
        attach_variant_schema(schema, ["v"])
    with pytest.raises(KeyError, match="not found"):
        attach_variant_schema(schema, ["w"])


def test_a_sibling_holding_another_urls_bytes_is_not_a_donor(tmp_path, monkeypatch):
    url = "https://example.test/data.csv"
    donor = http_recipe(url, slug="donor")
    target = http_recipe(url, slug="target")
    with operation(config_for(tmp_path, [donor, target])):
        directory = fetch.slug_dir("donor")
        stale = directory / "data.csv"
        stale.write_bytes(b"old upstream")
        # The donor was fetched from another URL before its recipe was re-pointed.
        fetch._write_receipt(directory, "data.csv", "https://old.test/data.csv", stale)
        assert fetch._find_sibling_cache(fetch.slug_dir("target"), url, "data.csv", None, None) is None
        monkeypatch.setattr(fetch.urllib.request, "urlopen", lambda *a, **k: io.BytesIO(b"fresh"))
        assert fetch.fetch(target)[0].read_bytes() == b"fresh"
        # A donor whose receipt names this URL is reused.
        fetch._write_receipt(directory, "data.csv", url, stale)
        assert fetch._find_sibling_cache(fetch.slug_dir("target"), url, "data.csv", None, None) == stale


def test_a_killed_kaggle_download_is_swept_on_the_next_attempt(tmp_path, monkeypatch):
    spec = http_recipe("https://www.kaggle.com/datasets/owner/tiny")
    spec["fetch"]["type"] = "kaggle"

    class Api:
        def authenticate(self):
            pass

        def dataset_download_files(self, ref, *, path, **kwargs):
            (Path(path) / "tiny.zip").write_bytes(b"complete")

    monkeypatch.setitem(sys.modules, "kaggle", type(sys)("kaggle"))
    sys.modules["kaggle"].KaggleApi = Api
    with operation(config_for(tmp_path, [spec])):
        target = fetch.slug_dir("tiny")
        leftover = target / ".kaggle-dead.part"
        leftover.mkdir(parents=True)
        (leftover / "tiny.zip").write_bytes(b"partial")  # a SIGKILLed attempt
        assert [p.read_bytes() for p in fetch.fetch(spec)] == [b"complete"]
        assert not list(target.glob(".kaggle-*.part"))


def test_bz2_cache_stamp_ignores_the_device_and_sees_a_rewrite(tmp_path):
    src, dest = tmp_path / "x.csv.bz2", tmp_path / "x.csv"
    src.write_bytes(bz2.compress(b"a,b\n1,2\n"))
    extract._copy_from(src, dest, bz2.open)
    assert extract._is_fresh(dest, src)
    assert "dev" not in json.loads(extract._stamp_path(dest).read_text())
    os.utime(src, ns=(1, 1))  # same inode and size, other bytes' timestamp
    assert not extract._is_fresh(dest, src)
