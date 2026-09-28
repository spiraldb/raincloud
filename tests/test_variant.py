# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the canonical VARIANT extension marker module.

Verifies that `attach_variant` stamps `VARIANT_EXT` onto the named columns,
that the marker survives a zstd-compressed Arrow IPC round-trip byte-for-byte,
that `discovery._is_variant_field` recognizes it, and that untouched columns
keep their original (empty) field metadata.
"""
from __future__ import annotations

import io

import pyarrow as pa

from raincloud.pipeline.discovery import _is_variant_field
from raincloud.pipeline.variant import (
    VARIANT_EXT,
    attach_variant,
    attach_variant_schema,
)


def _shredded_table() -> pa.Table:
    """A table with a shredded-VARIANT struct column `v` and a plain `id`."""
    variant_type = pa.struct([
        ("metadata", pa.binary()),
        ("value", pa.binary()),
        ("typed_value", pa.string()),
    ])
    v = pa.array([{"metadata": b"\x01", "value": b"\x02", "typed_value": "x"}], type=variant_type)
    ids = pa.array([1], type=pa.int64())
    return pa.table({"v": v, "id": ids})


def _roundtrip_zstd(table: pa.Table) -> pa.Table:
    """Write `table` to a zstd-compressed Arrow IPC stream and read it back."""
    opts = pa.ipc.IpcWriteOptions(compression="zstd")
    sink = io.BytesIO()
    with pa.ipc.new_stream(sink, table.schema, options=opts) as writer:
        writer.write_table(table)
    return pa.ipc.open_stream(io.BytesIO(sink.getvalue())).read_all()


def test_attach_variant_stamps_named_column():
    marked = attach_variant(_shredded_table(), ["v"])
    md = marked.schema.field("v").metadata
    for key, value in VARIANT_EXT.items():
        assert md[key] == value


def test_variant_ext_survives_zstd_ipc_roundtrip():
    marked = attach_variant(_shredded_table(), ["v"])
    back = _roundtrip_zstd(marked)
    md = back.schema.field("v").metadata
    # byte-identical after the round-trip
    for key, value in VARIANT_EXT.items():
        assert md[key] == value


def test_discovery_recognizes_roundtripped_field():
    back = _roundtrip_zstd(attach_variant(_shredded_table(), ["v"]))
    assert _is_variant_field(back.schema.field("v")) is True
    assert _is_variant_field(back.schema.field("id")) is False


def test_plain_column_metadata_unchanged():
    original = _shredded_table()
    marked = attach_variant(original, ["v"])
    # `id` was never touched — no VARIANT_EXT leakage.
    assert marked.schema.field("id").metadata == original.schema.field("id").metadata
    assert not _is_variant_field(marked.schema.field("id"))


def test_merge_preserves_and_overrides_existing_metadata():
    # Regression guard: seed `v` with pre-existing
    # metadata incl. a conflicting `__variant_type`, then assert attach_variant
    # PRESERVES the unrelated key and lets VARIANT_EXT win on conflict. A
    # clobbering regression (with_metadata(VARIANT_EXT)) would pass every other
    # test in this file, since their fixture columns start with empty metadata.
    t = _shredded_table()
    idx = t.schema.get_field_index("v")
    seeded = t.schema.field(idx).with_metadata({b"foo": b"bar", b"__variant_type": b"old"})
    t = pa.Table.from_arrays(t.columns, schema=t.schema.set(idx, seeded))
    md = attach_variant(t, ["v"]).schema.field("v").metadata
    assert md[b"foo"] == b"bar"  # pre-existing, unrelated key preserved
    assert md[b"__variant_type"] == b"1"  # VARIANT_EXT wins on conflict
    assert md[b"ARROW:extension:name"] == b"arrow.parquet.variant"


def test_unknown_column_raises():
    import pytest

    with pytest.raises(KeyError):
        attach_variant(_shredded_table(), ["nope"])


# --------------------------------------------------------------------------
# attach_variant_schema — the schema-level half `attach_variant` wraps, and the
# stamp the streaming VARIANT bridge (stream_canonical_arrow) applies per reader.
# --------------------------------------------------------------------------


def test_attach_variant_schema_stamps_named_field():
    stamped = attach_variant_schema(_shredded_table().schema, ["v"])
    md = stamped.field("v").metadata
    for key, value in VARIANT_EXT.items():
        assert md[key] == value
    # untouched columns keep their original (empty) metadata
    assert (stamped.field("id").metadata or {}) == {}
    assert _is_variant_field(stamped.field("v")) is True
    assert _is_variant_field(stamped.field("id")) is False


def test_attach_variant_schema_parity_with_attach_variant():
    # attach_variant is a thin wrapper: the schema it produces must be exactly
    # what the schema-level helper produces on the same columns.
    t = _shredded_table()
    assert attach_variant_schema(t.schema, ["v"]) == attach_variant(t, ["v"]).schema


def test_attach_variant_schema_unknown_column_raises():
    import pytest

    with pytest.raises(KeyError):
        attach_variant_schema(_shredded_table().schema, ["nope"])
