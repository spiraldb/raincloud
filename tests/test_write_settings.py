# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The ORC, Avro and Vortex write settings (`spec.FORMAT_SETTINGS`): given the
same way to every writer of the format, unset leaving what raincloud has always
written, and a writer whose library cannot honour a set one refusing it."""
from __future__ import annotations

import numpy as np
import pyarrow as pa
import pytest

from raincloud.pipeline.export import writer_toolchain
from raincloud.pipeline.export.exporters import OrcExporter, UnsupportedOption, VortexExporter
from raincloud.pipeline.export.sidecar import SidecarExporter
from raincloud.pipeline.spec import FORMAT_SETTINGS, chosen_settings, sidecar_settings, write_settings
from tests._helpers import find_sidecar, write_ipc

# `f` does not compress: ORC C++ measures a stripe once encoded and compressed.
TABLE = pa.table({"x": pa.array(range(40_000), pa.int64()), "s": [f"v{i % 97}" for i in range(40_000)],
                  "f": np.random.default_rng(0).random(40_000)})
SPEC = {"slug": "settings"}


@pytest.fixture(autouse=True)
def _unset(monkeypatch):
    for settings in FORMAT_SETTINGS.values():
        for _, var, _ in settings:
            monkeypatch.delenv(var, raising=False)


# ---- the settings ------------------------------------------------------------------------


def test_unset_is_what_raincloud_has_always_written():
    for fmt in FORMAT_SETTINGS:
        assert set(write_settings(fmt).values()) == {None}
        assert chosen_settings(fmt) == {} and sidecar_settings(fmt, SPEC) == {}


def test_set_settings_are_passed_in_one_form(monkeypatch):
    monkeypatch.setenv("RAINCLOUD_AVRO_COMPRESSION", " Deflate ")
    monkeypatch.setenv("RAINCLOUD_AVRO_COMPRESSION_LEVEL", "9")
    monkeypatch.setenv("RAINCLOUD_VORTEX_COMPACT", "yes")
    monkeypatch.setenv("RAINCLOUD_ORC_STRIPE_BYTES", "64e3")
    assert sidecar_settings("avro", SPEC) == {"RAINCLOUD_AVRO_COMPRESSION": "deflate",
                                              "RAINCLOUD_AVRO_COMPRESSION_LEVEL": "9"}
    assert sidecar_settings("vortex", SPEC) == {"RAINCLOUD_VORTEX_COMPACT": "1"}
    assert chosen_settings("orc") == {"orc_stripe_bytes": "64000"}
    rs = SidecarExporter("avro@rs", "avro", "raincloud-export-avro-rs")
    assert writer_toolchain(rs)["avro_compression"] == "deflate"
    assert writer_toolchain(VortexExporter())["vortex_compact"] == "1"
    assert rs._child_env(SPEC)["RAINCLOUD_AVRO_COMPRESSION"] == "deflate"


@pytest.mark.parametrize("var, value, error", [
    ("RAINCLOUD_ORC_COMPRESSION", "brotli", "is not one of zstd, snappy, zlib, lz4, none"),
    ("RAINCLOUD_ORC_COMPRESSION_STRATEGY", "fast", "is not one of speed, compression"),
    ("RAINCLOUD_AVRO_COMPRESSION_LEVEL", "23", "outside zstd's levels 1..22"),
    ("RAINCLOUD_VORTEX_COMPACT", "maybe", "is not a switch"),
    ("RAINCLOUD_VORTEX_ROW_BLOCK_ROWS", "8k", "is not a number"),
])
def test_a_malformed_setting_is_refused_naming_it(monkeypatch, var, value, error):
    monkeypatch.setenv(var, value)
    with pytest.raises(ValueError, match=error):
        write_settings(var.split("_")[1].lower())


def test_a_level_for_a_codec_without_one_is_refused(monkeypatch):
    monkeypatch.setenv("RAINCLOUD_AVRO_COMPRESSION", "snappy")
    monkeypatch.setenv("RAINCLOUD_AVRO_COMPRESSION_LEVEL", "3")
    with pytest.raises(ValueError, match="snappy takes no compression level"):
        write_settings("avro")


# ---- every writer --------------------------------------------------------------------------

IN_PROCESS = {"orc@py": OrcExporter, "vortex@py": VortexExporter}


def _write(tmp_path, cell: str):
    """Write TABLE with `cell`: (round-trips, note, the file), or None when not installed."""
    fmt, _, impl = cell.partition("@")
    canonical = tmp_path / "settings.arrow.zstd"
    if not canonical.exists():
        write_ipc(canonical, TABLE, max_chunksize=8192)
    dest = tmp_path / f"{fmt}-{impl}.{fmt}"
    if cell in IN_PROCESS:
        try:
            result = IN_PROCESS[cell]().export(SPEC, canonical, dest=dest)
        except UnsupportedOption as refused:  # recorded unavailable when a build runs it
            return False, str(refused), dest
    else:
        if not find_sidecar(cell):
            return None
        result = SidecarExporter(cell, fmt, f"raincloud-export-{fmt}-{impl}").export(SPEC, canonical, dest=dest)
    return result.compliance.roundtrip, result.compliance.note, dest


def _orc(path):
    import pyarrow.orc as orc
    return orc.ORCFile(str(path))


def _avro_codec(path) -> str:
    """The `avro.codec` an Avro object container file's header names."""
    data = path.read_bytes()
    at = data.index(b"avro.codec") + len(b"avro.codec")
    size = data[at] >> 1  # a short zigzag varint length
    return data[at + 1:at + 1 + size].decode()


def _avro_blocks(path) -> int:
    """Data blocks in an Avro file: each ends with the sync marker both lanes
    write, which the header carries once too."""
    return path.read_bytes().count(b"raincloud-avro01") - 1


REFUSED = "refused"
SETTINGS = {
    "an ORC codec": (
        {"RAINCLOUD_ORC_COMPRESSION": "snappy"},
        dict.fromkeys(("orc@py", "orc@rs"), lambda f: _orc(f).compression == "SNAPPY")),
    "no ORC compression": (
        {"RAINCLOUD_ORC_COMPRESSION": "none"},
        dict.fromkeys(("orc@py", "orc@rs"), lambda f: _orc(f).compression == "UNCOMPRESSED")),
    "an ORC compression strategy": (
        {"RAINCLOUD_ORC_COMPRESSION_STRATEGY": "compression"},
        {"orc@py": lambda f: _orc(f).compression == "ZSTD", "orc@rs": REFUSED}),
    "small ORC stripes": (
        {"RAINCLOUD_ORC_STRIPE_BYTES": "65536"},
        dict.fromkeys(("orc@py", "orc@rs"), lambda f: _orc(f).nstripes > 1)),
    "an ORC compression block size": (
        # ORC C++ takes only a multiple of its 64 KiB memory block; another size fails there.
        {"RAINCLOUD_ORC_COMPRESSION_BLOCK_BYTES": "131072"},
        dict.fromkeys(("orc@py", "orc@rs"), lambda f: _orc(f).compression_size == 131072)),
    **{f"Avro {codec}": (
        {"RAINCLOUD_AVRO_COMPRESSION": codec},
        dict.fromkeys(("avro@rs", "avro@java"),
                      lambda f, name={"zstd": "zstandard", "none": "null"}.get(codec, codec): _avro_codec(f) == name))
       for codec in ("zstd", "deflate", "snappy", "bzip2", "xz", "none")},
    "an Avro level": (
        {"RAINCLOUD_AVRO_COMPRESSION": "deflate", "RAINCLOUD_AVRO_COMPRESSION_LEVEL": "9"},
        {"avro@java": lambda f: _avro_codec(f) == "deflate", "avro@rs": REFUSED}),
    "small Avro blocks": (
        {"RAINCLOUD_AVRO_BLOCK_BYTES": "4096"},
        {"avro@java": lambda f: _avro_blocks(f) > 10, "avro@rs": REFUSED}),
    "compact Vortex": (
        {"RAINCLOUD_VORTEX_COMPACT": "1"},
        {"vortex@py": lambda f: True, "vortex@rs": lambda f: True, "vortex@jni": REFUSED}),
    "a Vortex row block size": (
        {"RAINCLOUD_VORTEX_ROW_BLOCK_ROWS": "1024"},
        {"vortex@rs": lambda f: True, "vortex@py": REFUSED, "vortex@jni": REFUSED}),
    "a Vortex data block size": (
        {"RAINCLOUD_VORTEX_DATA_BLOCK_BYTES": "65536"},
        {"vortex@rs": lambda f: True, "vortex@py": REFUSED, "vortex@jni": REFUSED}),
}
CASES = [(setting, cell) for setting, (_, cells) in SETTINGS.items() for cell in cells]


@pytest.mark.parametrize("setting, cell", CASES)
def test_each_setting_is_honoured_or_refused_by_every_writer(tmp_path, monkeypatch, setting, cell):
    env, expected = SETTINGS[setting]
    for var, value in env.items():
        monkeypatch.setenv(var, value)
    written = _write(tmp_path, cell)
    if written is None:
        pytest.skip(f"{cell} not installed")
    roundtrip, note, dest = written
    if expected[cell] is REFUSED:
        assert roundtrip is False and f"{cell} cannot honour RAINCLOUD_" in note, note
        assert not dest.exists()
        return
    assert roundtrip is True, note
    assert expected[cell](dest), (setting, cell)
