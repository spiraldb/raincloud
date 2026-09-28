# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Ingest never drops upstream records: it recovers them faithfully or fails the build.

JSONBench's dumps wrap a record longer than 65,535 bytes onto a second line; the
handler rejoins it. These tests shrink the wrap width so a synthetic record can
be wrapped the same way.
"""
from __future__ import annotations

import gzip
import json

import pyarrow as pa
import pytest

from raincloud.catalogs import operation
from raincloud.pipeline.handlers import jsonbench_variant_parse as jb
from raincloud.pipeline.spec import prepared_arrow
from tests.test_ingest_contracts import config_for, http_recipe, variant_kinds

WRAP = 40


def _events(n: int) -> list[bytes]:
    return [json.dumps({"kind": "commit", "seq": i}).encode() for i in range(n)]


def _long(text: str) -> bytes:
    return json.dumps({"kind": "commit", "text": text}).encode()


def _wrap(record: bytes, width: int = WRAP) -> list[bytes]:
    """Wrap `record` the way the upstream dumps do: `width` bytes, a newline, the rest."""
    return [record[i:i + width] for i in range(0, len(record), width)]


def _gz(path, lines: list[bytes], trailing_newline: bool = True):
    with gzip.open(path, "wb") as f:
        f.write(b"\n".join(lines) + (b"\n" if trailing_newline else b""))
    return path


def _build(tmp_path, files, **params) -> pa.ChunkedArray:
    tmp_path.mkdir(exist_ok=True)
    spec = http_recipe("https://example.test/unused", slug="jb")
    with operation(config_for(tmp_path, [spec])):
        assert jb.jsonbench_variant_parse(spec, [(p, None) for p in files], **params) == []
        with pa.ipc.open_file(str(prepared_arrow("jb"))) as reader:
            return reader.read_all().column("data")


@pytest.fixture
def wrap(monkeypatch):
    monkeypatch.setattr(jb, "WRAP_BYTES", WRAP)


def _same(tmp_path, wrapped_lines, faithful_lines, **params):
    """The build over the wrapped file equals the build over the unwrapped one."""
    got = _build(tmp_path / "w", [_gz(tmp_path / "file_0001.json.gz", wrapped_lines)], **params)
    want = _build(tmp_path / "f", [_gz(tmp_path / "file_0002.json.gz", faithful_lines)], **params)
    assert got.combine_chunks().to_pylist() == want.combine_chunks().to_pylist()
    assert len(got) == len(faithful_lines)
    return got


def test_wrapped_record_is_rejoined_without_the_newline(tmp_path, wrap):
    record = _long("split inside a JSON string")
    pieces = _wrap(record)
    assert len(pieces) == 2 and len(pieces[0]) == WRAP
    before, after = _events(3), _events(2)
    data = _same(tmp_path, before + pieces + after, before + [record] + after)
    assert variant_kinds(data) == [2] * 6


def test_record_wrapped_into_three_pieces(tmp_path, wrap):
    record = _long("x" * (2 * WRAP + 7))
    pieces = _wrap(record)
    assert len(pieces) == 3
    _same(tmp_path, _events(1) + pieces + _events(1), _events(1) + [record] + _events(1))


def test_record_wrapped_at_an_exact_multiple_of_the_width(tmp_path, wrap):
    # The last piece is itself WRAP bytes long; the join stops once it parses.
    text = "y" * (2 * WRAP - len(_long("")))
    record = _long(text)
    assert len(record) == 2 * WRAP
    _same(tmp_path, _events(1) + _wrap(record) + _events(2), _events(1) + [record] + _events(2))


def test_wrap_that_cuts_a_utf8_character(tmp_path, wrap):
    prefix = _long("")[:-2]  # '{"kind": "commit", "text": "'
    record = (prefix.decode() + "a" * (WRAP - len(prefix) - 1) + "é tail" + '"}').encode()
    pieces = _wrap(record)
    with pytest.raises(UnicodeDecodeError):
        pieces[0].decode()
    _same(tmp_path, pieces, [record])


def test_record_of_exactly_the_width_is_not_joined(tmp_path, wrap):
    record = _long("z" * (WRAP - len(_long(""))))
    assert len(record) == WRAP
    _same(tmp_path, [record] + _events(2), [record] + _events(2))


def test_wrap_across_batch_and_read_boundaries(tmp_path, wrap, monkeypatch):
    # The head lands on the last line of a batch and of a decompressed read.
    record = _long("crosses a boundary " * 3)
    before = _events(4)
    monkeypatch.setattr(jb, "_READ_BYTES", len(b"\n".join(before + _wrap(record)[:1])) + 1)
    lines = before + _wrap(record) + _events(5)
    _same(tmp_path, lines, before + [record] + _events(5), batch_size=5)


def test_order_holds_across_many_batches(tmp_path, wrap):
    events = _events(1000)
    data = _same(tmp_path, events, events, batch_size=37)
    assert len(data) == 1000


def test_last_line_without_a_newline_is_a_record(tmp_path):
    path = _gz(tmp_path / "file_0001.json.gz", _events(3), trailing_newline=False)
    assert len(_build(tmp_path, [path])) == 3


def test_line_that_is_not_json_fails_naming_file_line_and_bytes(tmp_path):
    bad = b'{"kind": "commit", "text": "never closed'
    path = _gz(tmp_path / "file_0007.json.gz", _events(2) + [bad] + _events(1))
    with pytest.raises(ValueError, match=rf"file_0007\.json\.gz: line 3 \({len(bad)} bytes\) is not a JSON object"):
        _build(tmp_path, [path])


def test_json_that_is_not_an_object_fails(tmp_path):
    path = _gz(tmp_path / "file_0001.json.gz", _events(1) + [b"[1, 2]"])
    with pytest.raises(ValueError, match=r"line 2 \(6 bytes\) is not a JSON object"):
        _build(tmp_path, [path])


def test_empty_line_fails(tmp_path):
    path = _gz(tmp_path / "file_0001.json.gz", _events(1) + [b""] + _events(1))
    with pytest.raises(ValueError, match=r"line 2 \(0 bytes\)"):
        _build(tmp_path, [path])


def test_wrapped_head_whose_pieces_do_not_join_fails(tmp_path, wrap):
    record = _long("a sentence long enough to wrap, split inside a JSON string")
    head = _wrap(record)[0]
    path = _gz(tmp_path / "file_0003.json.gz", _events(1) + [head] + _events(1))
    with pytest.raises(ValueError, match=rf"file_0003\.json\.gz: line 2 \({WRAP} bytes\), "
                                         r"line 3 \(\d+ bytes\) do not rejoin"):
        _build(tmp_path, [path])


def test_wrapped_head_at_end_of_file_fails(tmp_path, wrap):
    head = _wrap(_long("a sentence long enough to wrap, split inside a JSON string"))[0]
    path = _gz(tmp_path / "file_0001.json.gz", _events(1) + [head])
    with pytest.raises(ValueError, match=rf"line 2 \({WRAP} bytes\) do not rejoin"):
        _build(tmp_path, [path])


def test_line_that_is_not_utf8_fails(tmp_path):
    path = _gz(tmp_path / "file_0001.json.gz", _events(1) + [b'{"kind": "\xff"}'])
    with pytest.raises(ValueError, match=r"file_0001\.json\.gz: line 2 \(13 bytes\) is not UTF-8"):
        _build(tmp_path, [path])


def test_no_malformed_threshold_remains():
    assert not hasattr(jb, "MAX_MALFORMED")


# --------------------------------------------------------------------------
# generic CSV: a row with the wrong field count fails, naming file and row
# --------------------------------------------------------------------------

def test_csv_row_with_wrong_field_count_fails(tmp_path):
    from raincloud.pipeline.parse import parse_csv

    csv = tmp_path / "t.csv"
    csv.write_text('a,b,c\n1,2,3\n"multi\nline",5,6\n7,8\n')
    with pytest.raises(ValueError, match=r"t\.csv: row 4 has 2 fields, not 3: '7,8'"):
        parse_csv({"parse": {"options": {}}}, csv)


def test_csv_multiline_cells_and_blank_lines_are_not_rows(tmp_path):
    from raincloud.pipeline.parse import parse_csv

    csv = tmp_path / "t.csv"
    csv.write_text('a,b\n"x\ny",1\n\n2,3\n')
    assert parse_csv({"parse": {"options": {}}}, csv).num_rows == 2


def test_csv_strict_option_is_gone():
    import inspect

    from raincloud.pipeline import parse
    assert '"strict"' not in inspect.getsource(parse.parse_csv)


# --------------------------------------------------------------------------
# small text handlers: malformed data fails instead of being skipped
# --------------------------------------------------------------------------

_CADATA_ROW = "  4.5e+005  8.3e+000  4.1e+001  8.8e+002  1.29e+002  3.22e+002  1.26e+002  3.788e+001 -1.2223e+002"


def test_cal_housing_keeps_every_data_row_and_fails_on_a_bad_one(tmp_path):
    from raincloud.pipeline.handlers.cal_housing_parse import cal_housing_parse

    path = tmp_path / "cadata.txt"
    path.write_text("S&P Letters Data\nINTERCEPT 11.49 275.75\n\n" + "\n".join([_CADATA_ROW] * 3) + "\n\n")
    [(_, table)] = cal_housing_parse({"slug": "c"}, [(path, None)])
    assert table.num_rows == 3
    path.write_text("preamble\n" + _CADATA_ROW + "\n" + _CADATA_ROW.rsplit(" ", 1)[0] + "\n")
    with pytest.raises(ValueError, match=r"cadata\.txt line 3 is not nine numbers"):
        cal_housing_parse({"slug": "c"}, [(path, None)])


def test_glove_keeps_a_token_containing_spaces(tmp_path):
    from raincloud.pipeline.handlers.glove_split import glove_split

    path = tmp_path / "g.txt"
    path.write_text("the 0.1 0.2 0.3\nnew york 1.0 2.0 3.0\n. . . 4 5 6\n")
    [(_, table)] = glove_split({"slug": "g"}, [(path, None)], dimension=3)
    assert table.column("word").to_pylist() == ["the", "new york", ". . ."]
    assert table.column("vector")[1].as_py() == [1.0, 2.0, 3.0]


@pytest.mark.parametrize("bad", ["short 0.1 0.2", "tok 0.1 x 0.3", ""])
def test_glove_line_that_is_not_token_and_vector_fails(tmp_path, bad):
    from raincloud.pipeline.handlers.glove_split import glove_split

    path = tmp_path / "g.txt"
    path.write_text(f"the 0.1 0.2 0.3\n{bad}\nlast 1 2 3\n")
    with pytest.raises(ValueError, match=r"g\.txt line 2 is not a token and 3 numbers"):
        glove_split({"slug": "g"}, [(path, None)], dimension=3)


def test_uci_default_merges_every_parsed_file(tmp_path):
    from raincloud.pipeline.handlers.uci_default import uci_default
    from raincloud.pipeline.parse import parse_csv

    spec = {"slug": "u", "parse": {"reader": "csv", "options": {"has_header": False}}}
    train, test = tmp_path / "allbp.data", tmp_path / "allbp.test"
    train.write_text("35,F,1.5\n?,M,2.5\n")
    test.write_text("63,M,3.5\n")  # f0 is an integer here, text in the other file
    parsed = [(p, parse_csv(spec, p)) for p in (train, test)]
    [(_, table)] = uci_default(spec, parsed)
    assert table.num_rows == 3
    assert table.column("f0").to_pylist() == ["35", "?", "63"]
    assert table.column("f2").to_pylist() == [1.5, 2.5, 3.5]


def test_openlibrary_malformed_line_and_revision_fail(tmp_path):
    from raincloud.pipeline.handlers.openlibrary_parse import openlibrary_parse

    good = "/type/work\t/works/A\t1\t2020-01-02T03:04:05\t{}\n"
    for body, message in [(good + "short line\n", r"dump\.txt line 2 has 1 tab-separated fields"),
                          (good + "/type/work\t/works/B\tbad\t\t{}\n", r"line 2 has revision 'bad'"),
                          (good + "/type/work\t/works/B\t1\t\t{\"t\": \"a\rb\"}\n", None)]:
        source = tmp_path / "dump.txt"
        source.write_bytes(body.encode())
        [(_, stream)] = openlibrary_parse({"slug": "ol"}, [(source, None)], record_type="work")
        if message is None:  # a stray \r inside a record does not split it
            with stream.open() as batches:
                rows = [r for item in batches for r in item.batch.to_pylist()]
            assert [r["record"] for r in rows] == ["{}", '{"t": "a\rb"}']
            continue
        with pytest.raises(ValueError, match=message), stream.open() as batches:
            list(batches)


def test_openlibrary_refuses_a_second_input(tmp_path):
    from raincloud.pipeline.handlers.openlibrary_parse import openlibrary_parse

    with pytest.raises(ValueError, match="exactly 1 input file, got 2"):
        openlibrary_parse({"slug": "ol"}, [(tmp_path / "a", None), (tmp_path / "b", None)])


def _dly(values: dict[int, str]) -> str:
    return "USW00094728202002TMAX" + "".join(f"{values.get(d, '-9999'):>5}   " for d in range(1, 32))


def test_ghcn_sentinel_is_no_observation_and_malformed_input_fails(tmp_path):
    from raincloud.pipeline.handlers.ghcn_daily_parse import _parse_dly_file

    path = tmp_path / "USW00094728.dly"
    path.write_text(_dly({1: "12", 29: "-3"}) + "\n")
    assert [(t[1].day, t[3]) for t in _parse_dly_file(path)] == [(1, 12), (29, -3)]
    for line, message in [(_dly({30: "5"}), r"value 5 on 2020-02-30, a date that does not exist"),
                          (_dly({2: "  x  "}), r"line 2 day 2 has value '  x  '"),
                          (_dly({2: "     "}), r"line 2 day 2 has value"),
                          (_dly({})[:-8], r"line 2 is 261 characters, not 269")]:
        path.write_text(_dly({1: "12"}) + "\n" + line + "\n")
        with pytest.raises(ValueError, match=message):
            list(_parse_dly_file(path))


def test_ghcn_non_ascii_fails(tmp_path):
    from raincloud.pipeline.handlers.ghcn_daily_parse import _parse_dly_file

    path = tmp_path / "X.dly"
    path.write_bytes((_dly({1: "12"}) + "\n").replace("USW", "US\xe9").encode("latin-1"))
    with pytest.raises(ValueError, match=r"X\.dly .*not ASCII"):
        list(_parse_dly_file(path))


# --------------------------------------------------------------------------
# extract: a fetched file the extractor would pass over fails
# --------------------------------------------------------------------------

@pytest.mark.parametrize("kind", ["zip", "tar", "gzip", "7z"])
def test_extractor_refuses_a_fetched_file_it_would_ignore(tmp_path, monkeypatch, kind):
    from raincloud.pipeline import extract

    monkeypatch.setenv("RAINCLOUD_WORKDIR", str(tmp_path / "wd"))
    stray = tmp_path / "README.txt"
    stray.write_text("not an archive")
    spec = {"slug": "x", "extract": {"type": kind, "include": [], "exclude": []}}
    if kind == "7z":
        pytest.importorskip("py7zr")
    with pytest.raises(ValueError, match=rf"extract.type '{kind}' got README\.txt"):
        extract.extract(spec, [stray])


# --------------------------------------------------------------------------
# custom handlers: every record kept, malformed input fails
# --------------------------------------------------------------------------

def _diabetes(tmp_path, monkeypatch, text: str):
    import io
    import sys
    import tarfile
    import zipfile

    tar = io.BytesIO()
    with tarfile.open(fileobj=tar, mode="w") as t:
        data = text.encode()
        info = tarfile.TarInfo("Diabetes-Data/data-27")
        info.size = len(data)
        t.addfile(info, io.BytesIO(data))
    fake = type(sys)("unlzw3")
    fake.unlzw = lambda _: tar.getvalue()
    monkeypatch.setitem(sys.modules, "unlzw3", fake)
    path = tmp_path / "diabetes.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("diabetes-data.tar.Z", b"z")
    return path


def test_diabetes_record_with_an_empty_edge_field_is_kept(tmp_path, monkeypatch):
    from raincloud.pipeline.handlers.uci_diabetes_parse import uci_diabetes_parse

    path = _diabetes(tmp_path, monkeypatch,
                     "04-21-1991\t9:09\t58\t100\n10-12-1989\t7:00\t0\t\n\t138\t33\t3\n\n")
    [(_, table)] = uci_diabetes_parse({"slug": "d"}, [(path, None)])
    assert table.num_rows == 3
    assert table.column("date").to_pylist() == ["04-21-1991", "10-12-1989", ""]


def test_diabetes_line_with_wrong_field_count_fails(tmp_path, monkeypatch):
    from raincloud.pipeline.handlers.uci_diabetes_parse import uci_diabetes_parse

    path = _diabetes(tmp_path, monkeypatch, "04-21-1991\t9:09\t58\t100\n04-21-1991\t9:09\t58\n")
    with pytest.raises(ValueError, match=r"data-27 line 2 has 3 tab-separated fields, not 4"):
        uci_diabetes_parse({"slug": "d"}, [(path, None)])


def _xml(tmp_path, rows: str):
    path = tmp_path / "Tags.xml"
    path.write_text(f'<?xml version="1.0" encoding="utf-8"?>\n<tags>\n{rows}</tags>\n')
    return path


@pytest.mark.parametrize("row,message", [
    ('<row Id="1" TagName="a" Count="x"/>', r"row Id='1' Count='x' is not int64"),
    ('<row Id="1" TagName="a" Brand="new"/>', r"attribute\(s\) \['Brand'\] outside the tags schema"),
    ('<row Id="1" TagName="a" IsRequired="maybe"/>', r"IsRequired='maybe' is not bool"),
])
def test_stack_exchange_value_or_attribute_that_does_not_fit_fails(tmp_path, row, message):
    from raincloud.pipeline.handlers.stack_exchange_split import stack_exchange_split

    spec = http_recipe("https://example.test/unused", slug="se")
    with operation(config_for(tmp_path, [spec])), pytest.raises(ValueError, match=message):
        stack_exchange_split(spec, [(_xml(tmp_path, row + "\n"), None)], table="tags")


def test_jsonl_as_string_keeps_bytes_exact_and_refuses_bad_utf8(tmp_path):
    from raincloud.pipeline.handlers.jsonl_as_string_parse import jsonl_as_string_parse

    path = tmp_path / "x.jsonl"
    path.write_bytes(b'{"a": "x\ry"}\n\n{"b": 1}\r\n')
    spec = http_recipe("https://example.test/unused", slug="js")
    with operation(config_for(tmp_path, [spec])):
        assert jsonl_as_string_parse(spec, [(path, None)]) == []
        with pa.ipc.open_file(str(prepared_arrow("js"))) as reader:
            assert reader.read_all().column("raw_json").to_pylist() == ['{"a": "x\ry"}', '{"b": 1}']
        path.write_bytes(b'{"a": 1}\n{"b": "\xff"}\n')
        with pytest.raises(ValueError, match=r"x\.jsonl line 2 is not utf-8"):
            jsonl_as_string_parse(spec, [(path, None)])


_GAME = '[Event "Rated game"]\n[White "a"]\n[WhiteElo "1500"]\n[BlackElo "?"]\n\n1. e4 e5 1-0\n\n'


def _pgn(tmp_path, text: str):
    import zstandard

    path = tmp_path / "db.pgn.zst"
    path.write_bytes(zstandard.ZstdCompressor().compress(text.encode()))
    return path


def test_lichess_keeps_titles_and_unknown_elo_is_null(tmp_path):
    from raincloud.pipeline.handlers.lichess_pgn_parse import lichess_pgn_parse

    spec = http_recipe("https://example.test/unused", slug="pgn")
    path = _pgn(tmp_path, _GAME + _GAME.replace('[White "a"]', '[White "a"]\n[WhiteTitle "GM"]'))
    with operation(config_for(tmp_path, [spec])):
        assert lichess_pgn_parse(spec, [(path, None)]) == []
        with pa.ipc.open_file(str(prepared_arrow("pgn"))) as reader:
            table = reader.read_all()
    assert table.column("white_title").to_pylist() == [None, "GM"]
    assert table.column("white_elo").to_pylist() == [1500, 1500]
    assert table.column("black_elo").to_pylist() == [None, None]


@pytest.mark.parametrize("text,message", [
    (_GAME + _GAME.replace("[White", "White"), r"line 9 is in a header block but is not a tag pair"),
    (_GAME.replace("\n\n1. e4", "\n\n1. e4") .replace("1-0\n\n", "1-0\n") + _GAME,
     r"line 7 is a tag pair inside move text"),
    (_GAME.replace('"1500"', '"15x0"'), r"line 1: white_elo '15x0' is not an integer"),
    (_GAME.replace("[White ", "[Mystery "), r"tag\(s\) \['Mystery'\] have no column"),
])
def test_lichess_malformed_pgn_fails(tmp_path, text, message):
    from raincloud.pipeline.handlers.lichess_pgn_parse import lichess_pgn_parse

    spec = http_recipe("https://example.test/unused", slug="pgn")
    with operation(config_for(tmp_path, [spec])), pytest.raises(ValueError, match=message):
        lichess_pgn_parse(spec, [(_pgn(tmp_path, text), None)])


def test_public_bi_row_with_an_extra_field_fails(tmp_path):
    from raincloud.pipeline.handlers.public_bi_merge import public_bi_merge

    schema = tmp_path / "W_1.table.sql"
    schema.write_text('CREATE TABLE "W_1"("a" integer, "b" varchar(4));')
    part = tmp_path / "W_1.csv"
    part.write_text("1|x\n2|y|stray\n")
    spec = http_recipe("https://example.test/unused", slug="bi-w")
    with operation(config_for(tmp_path, [spec])), pytest.raises(ValueError, match=r"W_1\.csv: .*got 3"):
        public_bi_merge(spec, [(schema, None), (part, None)], workload="W")
