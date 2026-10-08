# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The Parquet write options: one set (`spec.ParquetOptions`) given the same way
to every Parquet writer, each page knob leaving the writer's own default when
unset, and a writer that cannot do what a set knob asks failing rather than
writing something else."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud.pipeline.export import writer_toolchain
from raincloud.pipeline.export.exporters import ParquetExporter, UnsupportedOption
from raincloud.pipeline.export.sidecar import SidecarExporter
from raincloud.pipeline.spec import PARQUET_SETTING_VARS, ParquetOptions, parquet_options
from tests._helpers import find_sidecar, write_ipc

KNOBS = PARQUET_SETTING_VARS
SPEC = {"slug": "pages", "write": {"compression": "zstd", "statistics": True}}
TABLE = pa.table({"x": pa.array(range(50_000), pa.int64()), "s": [f"v{i % 97}" for i in range(50_000)]})


@pytest.fixture(autouse=True)
def _unset(monkeypatch):
    for var in KNOBS:
        monkeypatch.delenv(var, raising=False)


def _data_pages(path, column: int = 0) -> list[dict]:
    """The data page headers of one column chunk of the first row group, read
    from the file: {thrift field id: value}, as parquet.thrift numbers them."""
    meta = pq.ParquetFile(path).metadata
    chunk = meta.row_group(0).column(column)
    data = path.read_bytes()
    start = chunk.dictionary_page_offset if chunk.has_dictionary_page else chunk.data_page_offset
    pos, end, pages = start, start + chunk.total_compressed_size, []
    while pos < end:
        header, pos = _thrift_struct(data, pos)
        pos += header[3]  # compressed_page_size
        if header[1] in (0, 3):  # DATA_PAGE, DATA_PAGE_V2
            pages.append(header)
    return pages


def _thrift_struct(data: bytes, pos: int) -> tuple[dict, int]:
    """A thrift compact-protocol struct at `pos`: ({field id: value}, end)."""
    def varint():
        nonlocal pos
        shift = value = 0
        while True:
            byte = data[pos]
            pos += 1
            value |= (byte & 0x7F) << shift
            shift += 7
            if not byte & 0x80:
                return value

    def zigzag():
        value = varint()
        return (value >> 1) ^ -(value & 1)

    def read(kind, element=False):
        nonlocal pos
        if kind in (1, 2):
            if element:
                pos += 1
                return data[pos - 1] == 1
            return kind == 1
        if kind == 3:
            pos += 1
            return data[pos - 1]
        if kind in (4, 5, 6):
            return zigzag()
        if kind == 7:
            pos += 8
            return None
        if kind == 8:
            size = varint()
            pos += size
            return data[pos - size:pos]
        if kind in (9, 10):
            head = data[pos]
            pos += 1
            count = head >> 4 if head >> 4 != 15 else varint()
            return [read(head & 0xF, True) for _ in range(count)]
        if kind == 12:
            value, pos = _thrift_struct(data, pos)
            return value
        raise ValueError(f"thrift type {kind} at byte {pos}")

    fields, last = {}, 0
    while data[pos]:
        head = data[pos]
        pos += 1
        last = last + (head >> 4) if head >> 4 else zigzag()
        fields[last] = read(head & 0xF)
    return fields, pos + 1


def _layout(path) -> tuple[bool, int]:
    """(every column chunk has a page index, data pages in the first chunk)."""
    meta = pq.ParquetFile(path).metadata
    chunks = [meta.row_group(g).column(c) for g in range(meta.num_row_groups) for c in range(meta.num_columns)]
    indexed = {chunk.has_column_index and chunk.has_offset_index for chunk in chunks}
    assert len(indexed) == 1, "some column chunks have a page index and some do not"
    return indexed.pop(), len(_data_pages(path))


# ---- the options -----------------------------------------------------------------------


def test_unset_page_knobs_leave_each_writers_default(monkeypatch):
    assert parquet_options(SPEC) == ParquetOptions()
    assert parquet_options(SPEC).chosen() == {}
    assert parquet_options(SPEC).env() == {"RAINCLOUD_PARQUET_COMPRESSION": "zstd",
                                           "RAINCLOUD_PARQUET_STATISTICS": "1"}
    monkeypatch.setenv("RAINCLOUD_PARQUET_PAGE_INDEX", " ")
    assert parquet_options(SPEC).page_index is None


def test_set_knobs_are_read_once_and_passed_in_one_form(monkeypatch):
    monkeypatch.setenv("RAINCLOUD_PARQUET_PAGE_INDEX", " Yes ")
    monkeypatch.setenv("RAINCLOUD_PARQUET_PAGE_BYTES", "64e3")
    monkeypatch.setenv("RAINCLOUD_PARQUET_PAGE_ROWS", "0")
    options = parquet_options({"write": {"compression": "gzip", "statistics": True}})
    assert options == ParquetOptions("gzip", True, page_index=True, page_bytes=64_000, page_rows=(1 << 31) - 1)
    assert options.env() == {"RAINCLOUD_PARQUET_COMPRESSION": "gzip", "RAINCLOUD_PARQUET_STATISTICS": "1",
                             "RAINCLOUD_PARQUET_PAGE_INDEX": "1", "RAINCLOUD_PARQUET_PAGE_BYTES": "64000",
                             "RAINCLOUD_PARQUET_PAGE_ROWS": str((1 << 31) - 1)}
    assert options.chosen() == {"parquet_page_index": "1", "parquet_page_bytes": "64000",
                                "parquet_page_rows": str((1 << 31) - 1)}


@pytest.mark.parametrize("env, spec, error", [
    ({"RAINCLOUD_PARQUET_PAGE_INDEX": "maybe"}, SPEC, "is not a switch"),
    ({"RAINCLOUD_PARQUET_PAGE_BYTES": "1MiB"}, SPEC, "is not a number"),
    ({"RAINCLOUD_PARQUET_PAGE_ROWS": "-1"}, SPEC, "is not a number"),
    ({"RAINCLOUD_PARQUET_PAGE_INDEX": "1"}, {"write": {"statistics": False}}, "asks for statistics"),
    ({"RAINCLOUD_PARQUET_STATISTICS_COLUMNS": "5"}, {"write": {"statistics": False}}, "asks for statistics"),
    ({"RAINCLOUD_PARQUET_PAGE_INDEX": "0", "RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS": "5"}, SPEC,
     "asks for a page index"),
    ({"RAINCLOUD_PARQUET_COMPRESSION_LEVEL": "-1"}, SPEC, "is not a compression level"),
    ({"RAINCLOUD_PARQUET_COMPRESSION_LEVEL": "23"}, SPEC, "outside zstd's levels 1..22"),
    ({"RAINCLOUD_PARQUET_COMPRESSION_LEVEL": "1"}, {"write": {"compression": "snappy"}},
     "snappy takes no compression level"),
    ({}, {"write": {"compression": "lzo"}}, "is not one of"),
])
def test_a_malformed_option_is_refused_naming_it(monkeypatch, env, spec, error):
    for var, value in env.items():
        monkeypatch.setenv(var, value)
    with pytest.raises(ValueError, match=error):
        parquet_options(spec)


def test_set_page_knobs_are_part_of_every_parquet_writers_toolchain(monkeypatch):
    rs = SidecarExporter("parquet@rs", "parquet", "raincloud-export-parquet-rs")
    vortex = SidecarExporter("vortex@rs", "vortex", "raincloud-export-vortex-rs")
    before = writer_toolchain(ParquetExporter()), writer_toolchain(rs), writer_toolchain(vortex)
    monkeypatch.setenv("RAINCLOUD_PARQUET_PAGE_INDEX", "1")
    assert writer_toolchain(ParquetExporter()) == {**before[0], "parquet_page_index": "1"}
    assert writer_toolchain(rs) == {**before[1], "parquet_page_index": "1"}
    assert writer_toolchain(vortex) == before[2]


def test_a_sidecar_gets_the_options_in_place_of_the_raw_environment(monkeypatch):
    monkeypatch.setenv("RAINCLOUD_PARQUET_PAGE_INDEX", " on ")
    monkeypatch.setenv("RAINCLOUD_PARQUET_COMPRESSION", "snappy")  # the recipe's wins
    env = SidecarExporter("parquet@java", "parquet", "raincloud-export-parquet-java")._child_env(SPEC)
    assert (env["RAINCLOUD_PARQUET_PAGE_INDEX"], env["RAINCLOUD_PARQUET_COMPRESSION"]) == ("1", "zstd")
    assert "RAINCLOUD_PARQUET_PAGE_ROWS" not in env
    other = SidecarExporter("orc@rs", "orc", "raincloud-export-orc-rs")._child_env(SPEC)
    assert other["RAINCLOUD_PARQUET_PAGE_INDEX"] == " on "  # passed through untouched, unread


# ---- every writer --------------------------------------------------------------------------


def _write(tmp_path, cell: str, spec: dict = SPEC):
    """Write TABLE with `cell`: (round-trips, note, the file), or None when not installed."""
    canonical = tmp_path / "pages.arrow.zstd"
    if not canonical.exists():
        write_ipc(canonical, TABLE, max_chunksize=8192)
    dest = tmp_path / f"{cell.replace('@', '-')}.parquet"
    if cell == "parquet@py":
        try:
            result = ParquetExporter().export(spec, canonical, dest=dest)
        except UnsupportedOption as refused:  # recorded unavailable when a build runs it
            return False, str(refused), dest
    else:
        impl = cell.partition("@")[2]
        if not find_sidecar(cell):
            return None
        result = SidecarExporter(cell, "parquet", f"raincloud-export-parquet-{impl}").export(spec, canonical, dest=dest)
    return result.compliance.roundtrip, result.compliance.note, dest


CELLS = ("parquet@py", "parquet@rs", "parquet@java", "parquet@hardwood")


@pytest.mark.parametrize("cell", CELLS)
def test_a_page_index_is_written_on_request_or_refused(tmp_path, monkeypatch, cell):
    monkeypatch.setenv("RAINCLOUD_PARQUET_PAGE_INDEX", "1")
    written = _write(tmp_path, cell)
    if written is None:
        pytest.skip(f"{cell} not installed")
    roundtrip, note, dest = written
    if cell == "parquet@hardwood":
        assert roundtrip is False and "cannot honour RAINCLOUD_PARQUET_PAGE_INDEX=1" in note, note
        assert not dest.exists()
        return
    assert roundtrip is True, note
    assert _layout(dest)[0] is True


@pytest.mark.parametrize("cell", CELLS)
def test_no_page_index_is_written_on_request_or_refused(tmp_path, monkeypatch, cell):
    monkeypatch.setenv("RAINCLOUD_PARQUET_PAGE_INDEX", "0")
    written = _write(tmp_path, cell)
    if written is None:
        pytest.skip(f"{cell} not installed")
    roundtrip, note, dest = written
    if cell == "parquet@java":
        assert roundtrip is False and "cannot honour RAINCLOUD_PARQUET_PAGE_INDEX=0" in note, note
        return
    assert roundtrip is True, note
    assert _layout(dest)[0] is False


@pytest.mark.parametrize("cell", CELLS)
def test_the_page_size_knob_reaches_every_writer(tmp_path, monkeypatch, cell):
    unset = _write(tmp_path, cell)
    if unset is None:
        pytest.skip(f"{cell} not installed")
    monkeypatch.setenv("RAINCLOUD_PARQUET_PAGE_BYTES", "4096")
    (tmp_path / "small").mkdir()
    roundtrip, note, small = _write(tmp_path / "small", cell)
    assert roundtrip is True, note
    # More pages, not a figure: each library measures a page its own way (arrow-rs
    # sizes a dictionary-encoded page by its estimate, checked every 1,024 values).
    assert _layout(small)[1] > _layout(unset[2])[1], (cell, _layout(small), _layout(unset[2]))


def _statistics(path) -> list[tuple[bool, bool]]:
    """(chunk statistics, page index) per column of the first row group."""
    group = pq.ParquetFile(path).metadata.row_group(0)
    return [(group.column(c).is_stats_set, group.column(c).has_column_index) for c in range(group.num_columns)]


def _codec(path) -> str:
    return pq.ParquetFile(path).metadata.row_group(0).column(0).compression


REFUSED = "refused"
# Each setting, and what every writer does with it: REFUSED, or a check of the file.
SETTINGS = {
    "statistics for the first column": (
        {"RAINCLOUD_PARQUET_STATISTICS_COLUMNS": "1"},
        {"parquet@py": lambda f: [s for s, _ in _statistics(f)] == [True, False],
         "parquet@rs": lambda f: _statistics(f) == [(True, True), (False, False)],
         "parquet@java": lambda f: _statistics(f) == [(True, True), (False, False)],
         "parquet@hardwood": REFUSED}),
    "page index for the first column": (
        {"RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS": "1"},
        {"parquet@rs": lambda f: _statistics(f) == [(True, True), (True, False)],
         "parquet@py": REFUSED, "parquet@java": REFUSED, "parquet@hardwood": REFUSED}),
    "no dictionaries": (
        {"RAINCLOUD_PARQUET_DICTIONARY": "0"},
        dict.fromkeys(CELLS, lambda f: not any(pq.ParquetFile(f).metadata.row_group(0).column(c).has_dictionary_page
                                               for c in range(2)))),
    "a small dictionary page": (
        # 50,000 distinct int64s do not fit 1 KiB: the column falls back to PLAIN pages.
        {"RAINCLOUD_PARQUET_DICTIONARY_PAGE_BYTES": "1024"},
        {**dict.fromkeys(("parquet@py", "parquet@rs", "parquet@java"),
                         lambda f: any(page[5][2] == 0 for page in _data_pages(f) if 5 in page)),
         "parquet@hardwood": REFUSED}),
    "a zstd level": (
        {"RAINCLOUD_PARQUET_COMPRESSION_LEVEL": "19"},
        {**dict.fromkeys(("parquet@py", "parquet@rs", "parquet@java"), lambda f: _codec(f) == "ZSTD"),
         "parquet@hardwood": REFUSED}),

    "page checksums": (
        {"RAINCLOUD_PARQUET_PAGE_CHECKSUMS": "1"},
        {**dict.fromkeys(("parquet@py", "parquet@java", "parquet@hardwood"),
                         lambda f: all(4 in page for page in _data_pages(f))),
         "parquet@rs": REFUSED}),
    "no page checksums": (
        {"RAINCLOUD_PARQUET_PAGE_CHECKSUMS": "0"},
        {**dict.fromkeys(("parquet@py", "parquet@rs", "parquet@java"),
                         lambda f: not any(4 in page for page in _data_pages(f))),
         "parquet@hardwood": REFUSED}),
}


@pytest.mark.parametrize("cell", CELLS)
@pytest.mark.parametrize("setting", SETTINGS)
def test_each_setting_is_honoured_or_refused_by_every_writer(tmp_path, monkeypatch, setting, cell):
    env, expected = SETTINGS[setting]
    for var, value in env.items():
        monkeypatch.setenv(var, value)
    written = _write(tmp_path, cell)
    if written is None:
        pytest.skip(f"{cell} not installed")
    roundtrip, note, dest = written
    if expected[cell] is REFUSED:
        assert roundtrip is False and f"{cell} cannot honour {next(iter(env))}=" in note, note
        assert not dest.exists()
        return
    assert roundtrip is True, note
    assert expected[cell](dest), (setting, cell)


@pytest.mark.parametrize("cell", CELLS)
# pyarrow names the LZ4_RAW codec "LZ4" (it reads no Hadoop-framed LZ4 either).
@pytest.mark.parametrize("codec, written", [("lz4", "LZ4"), ("gzip", "GZIP"), ("snappy", "SNAPPY"),
                                            ("none", "UNCOMPRESSED"), ("brotli", "BROTLI")])
def test_the_recipes_codec_reaches_every_writer(tmp_path, cell, codec, written):
    roundtrip_note_dest = _write(tmp_path, cell, {"slug": "pages", "write": {"compression": codec, "statistics": True}})
    if roundtrip_note_dest is None:
        pytest.skip(f"{cell} not installed")
    roundtrip, note, dest = roundtrip_note_dest
    if (cell, codec) == ("parquet@java", "brotli"):
        assert roundtrip is False and "parquet@java cannot honour RAINCLOUD_PARQUET_COMPRESSION=brotli" in note, note
        return
    assert roundtrip is True, note
    assert _codec(dest) == written
    # Another library decodes it: pyarrow reads every writer's file in every codec.
    assert pq.read_table(dest).select(["x", "s"]).to_pydict() == TABLE.to_pydict()
