# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit test for the canonical-Arrow producer (`canonical.write_canonical`).

Hermetic: points `RAINCLOUD_HOME` at a tmp dir (per test_pipeline_e2e.py) so
`output_format_dir` resolves the artifact under tmp, never the real outputs/.
Verifies the zstd Arrow IPC round-trip preserves data AND that a
`variant.attach_variant`-marked column keeps its `VARIANT_EXT` field metadata
byte-for-byte, so `discovery._is_variant_field` still recognizes it.
"""
from __future__ import annotations

import pyarrow as pa
import pytest

from raincloud.pipeline import canonical, discovery
from raincloud.pipeline.spec import output_format_dir
from raincloud.pipeline.variant import VARIANT_EXT, attach_variant


def test_write_canonical_roundtrips_variant_metadata(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    slug = "test-canonical"
    table = attach_variant(
        pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int32()),
                # A VARIANT storage struct: attach_variant refuses anything else.
                "v": pa.array([{"metadata": b"\x01\x00\x00", "value": bytes([12, i])} for i in (1, 2, 3)],
                              pa.struct([("metadata", pa.binary()), ("value", pa.binary())])),
            }
        ),
        ["v"],
    )

    out_paths = canonical.write_canonical({"slug": slug}, [(slug, table)])

    dest = output_format_dir(slug, "arrow") / f"{slug}.arrow.zstd"
    assert out_paths == [dest]
    assert dest.exists() and dest.stat().st_size > 0

    with pa.ipc.open_file(str(dest)) as reader:
        got = reader.read_all()

    # Data survives the round-trip.
    assert got.equals(table)
    assert got.column_names == ["id", "v"]

    # The VARIANT_EXT field metadata survives byte-for-byte on 'v' only.
    v_field = got.schema.field("v")
    assert v_field.metadata == VARIANT_EXT
    assert (got.schema.field("id").metadata or {}) == {}

    # discovery recognizes the marker on 'v' but not on 'id'.
    assert discovery._is_variant_field(v_field) is True
    assert discovery._is_variant_field(got.schema.field("id")) is False


def test_write_canonical_multi_slug_and_tmp_cleanup(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    t1 = pa.table({"a": pa.array([1], type=pa.int32())})
    t2 = pa.table({"b": pa.array(["x"])})

    # A different writer may own the legacy temporary filename. Leave it alone.
    d1 = output_format_dir("one", "arrow")
    d1.mkdir(parents=True, exist_ok=True)
    stale = d1 / "one.arrow.zstd.tmp"
    stale.write_bytes(b"stale")

    out = canonical.write_canonical({}, [("one", t1), ("two", t2)])

    dest1 = output_format_dir("one", "arrow") / "one.arrow.zstd"
    dest2 = output_format_dir("two", "arrow") / "two.arrow.zstd"
    # Per-slug dirs resolved independently; paths returned in input order.
    assert out == [dest1, dest2]
    assert dest1.exists() and dest2.exists()
    # This writer never removes another writer's temporary file.
    assert stale.read_bytes() == b"stale"
    assert list(d1.glob(".*.tmp")) == []


def test_open_canonical_writer_streams_batches_and_preserves_metadata(
    tmp_path, monkeypatch
):
    """The streaming writer (used by the migrated ParquetWriter handlers) lands a
    zstd IPC file with multiple record batches and preserves BOTH schema-level
    metadata (GeoParquet `geo`) and field-level VARIANT_EXT byte-for-byte, and
    leaves no `.tmp` behind on a clean write."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    slug = "stream-canonical"
    # Schema-level metadata (osm's GeoParquet `geo`) + a VARIANT_EXT-marked field.
    geo = b'{"version": "1.1.0", "primary_column": "geometry"}'
    base = pa.schema(
        [pa.field("id", pa.int32()), pa.field("v", pa.string())]
    )
    v_field = base.field("v").with_metadata(dict(VARIANT_EXT))
    schema = base.set(1, v_field).with_metadata({b"geo": geo})

    b1 = pa.record_batch(
        [pa.array([1, 2], type=pa.int32()), pa.array(['{"a":1}', '{"b":2}'])],
        schema=schema,
    )
    b2 = pa.record_batch(
        [pa.array([3], type=pa.int32()), pa.array(['{"c":3}'])], schema=schema
    )

    dest = output_format_dir(slug, "arrow") / f"{slug}.arrow.zstd"
    tmp = dest.parent / f"{dest.name}.tmp"

    with canonical.open_canonical_writer(slug, schema) as writer:
        writer.write_batch(b1)
        writer.write_batch(b2)

    assert dest.exists() and dest.stat().st_size > 0
    assert not tmp.exists()  # no leftover tmp after a clean write

    with pa.ipc.open_file(str(dest)) as reader:
        assert reader.num_record_batches == 2
        got = reader.read_all()
        got_schema = reader.schema

    assert got.num_rows == 3
    assert got.column_names == ["id", "v"]
    # Schema-level GeoParquet metadata survives.
    assert (got_schema.metadata or {}).get(b"geo") == geo
    # Field-level VARIANT_EXT survives on 'v' only, and discovery recognizes it.
    assert got_schema.field("v").metadata == VARIANT_EXT
    assert discovery._is_variant_field(got_schema.field("v")) is True
    assert discovery._is_variant_field(got_schema.field("id")) is False


def test_open_canonical_writer_removes_tmp_on_exception(tmp_path, monkeypatch):
    """If the streaming block raises, the tmp is reaped and no artifact lands —
    the atomic-replace contract holds under failure."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    slug = "stream-boom"
    schema = pa.schema([pa.field("id", pa.int32())])
    dest = output_format_dir(slug, "arrow") / f"{slug}.arrow.zstd"
    tmp = dest.parent / f"{dest.name}.tmp"

    class _Boom(RuntimeError):
        pass

    with pytest.raises(_Boom):
        with canonical.open_canonical_writer(slug, schema) as writer:
            writer.write_batch(
                pa.record_batch([pa.array([1], type=pa.int32())], schema=schema)
            )
            raise _Boom("mid-stream failure")

    assert not tmp.exists()
    assert not dest.exists()


@pytest.mark.parametrize("fail_second", [False, True])
def test_overlapping_writers_publish_only_their_own_complete_file(tmp_path, monkeypatch, fail_second):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path))
    first = pa.table({"x": [1]})
    second = pa.table({"x": [2]})
    slug = "overlap"
    dest = output_format_dir(slug, "arrow") / f"{slug}.arrow.zstd"
    with canonical.open_canonical_writer(slug, second.schema) as writer:
        writer.write_table(second)
        # Table and streaming paths overlap deterministically. The inner
        # publication must be readable while the outer writer remains open.
        canonical.write_canonical({}, [(slug, first)])
        with pa.ipc.open_file(str(dest)) as reader:
            assert reader.read_all().equals(first)
        if fail_second:
            with pytest.raises(RuntimeError):
                with canonical.open_canonical_writer(slug, first.schema) as failing:
                    failing.write_table(first)
                    raise RuntimeError("aborted writer")
            with pa.ipc.open_file(str(dest)) as reader:
                assert reader.read_all().equals(first)
    with pa.ipc.open_file(str(dest)) as reader:
        assert reader.read_all().equals(second)
    assert list(dest.parent.glob(".*.tmp")) == []
