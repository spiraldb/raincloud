# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Canonical-Arrow producer — write transformed Tables to
outputs/v{schema_version}/<slug>/arrow/<slug>.arrow.zstd.

The Arrow-IPC analogue of the parquet `write` stage: takes the transform
stage's `list[(slug, Table | BatchStream)]` and emits one zstd-compressed
Arrow IPC *file* per slug. Field-level `custom_metadata` rides in the
uncompressed schema FlatBuffer of the IPC message, so a `VARIANT_EXT`-marked
column (from `variant.attach_variant`) survives the round-trip byte-for-byte
and is recognized by `discovery._is_variant_field`.

Writes are crash-safe: each artifact lands at a writer-owned temporary path then is moved into
place with an atomic `os.replace`. There is no mtime idempotence — this is a
producer from in-memory tables, not a re-encode from a source file.
"""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import pyarrow as pa

from .batches import BatchStream
from .spec import display_path, output_format_dir


def uniquify_names(names: list[str]) -> list[str] | None:
    """Disambiguate duplicate names by suffixing ` [N]`, or None if all unique.

    The first occurrence keeps its name. A repeat takes the lowest ` [N]` that
    is neither another column's real name nor already handed out, so
    `['a', 'a', 'a [1]']` becomes `['a', 'a [2]', 'a [1]']`.
    """
    if len(set(names)) == len(names):
        return None
    taken = set(names)
    seen: set[str] = set()
    out: list[str] = []
    for name in names:
        if name not in seen:
            seen.add(name)
            out.append(name)
            continue
        n = 1
        while f"{name} [{n}]" in taken:
            n += 1
        out.append(f"{name} [{n}]")
        taken.add(out[-1])
    return out


def dedupe_column_names(table: pa.Table, label: str) -> pa.Table:
    """Ensure unique top-level column names on the canonical, renaming if needed.

    Duplicate top-level names must not reach the canonical spine. Arrow permits
    them, but the formats derived FROM it do not, so a canonical carrying them
    yields artifacts nothing can read back:

    - **Parquet**: the artifact writes fine, then `pq.read_table` raises
      `ArrowInvalid: Can't unify schema with duplicate field names` — the
      published file is unreadable. The fabricated `roundtrip=True` verdict hid
      this until the write verdict became a real measurement.
    - **Vortex**: `StructLayout` rejects duplicates outright.

    Doing it here, and refusing duplicates in `open_canonical_writer`, makes one
    set of names true for every format derived from the spine.

    Upstream sources that hit this are survey exports whose matrix questions
    repeat a header (`osmi-mental-health-in-tech-2023`, `uci-spambase`,
    `uci-parkinsons`). Renaming rather than failing keeps them in the catalog;
    the rename is deterministic (a ` [N]` suffix) and logged, never silent.
    """
    new_names = uniquify_names(list(table.schema.names))
    if new_names is None:
        return table
    names = list(table.schema.names)
    dupes = len(names) - len(set(names))
    print(
        f"[canonical] {label}: disambiguated {dupes} duplicate column name(s) "
        f"with a ' [N]' suffix — duplicates are unreadable in Parquet and "
        f"rejected by Vortex"
    )
    return table.rename_columns(new_names).replace_schema_metadata(table.schema.metadata)


def write_canonical(spec: dict, tables: list[tuple[str, pa.Table | BatchStream]]) -> list[Path]:
    """Write each table or fixed-schema batch stream to canonical Arrow IPC.

    `spec` is unused -- the canonical format is a fixed zstd Arrow IPC file --
    and kept because every stage takes `(spec, inputs)`, as `build` calls them.

    Duplicate top-level column names are disambiguated before the write — see
    `dedupe_column_names` for why the canonical must not carry them.
    """
    out_paths: list[Path] = []
    for out_slug, table in tables:
        # Resolve deterministic names once, including for an empty stream.
        schema = dedupe_column_names(pa.Table.from_batches([], schema=table.schema), out_slug).schema
        if not isinstance(table, BatchStream):
            table = pa.Table.from_arrays(table.columns, schema=schema)
        with open_canonical_writer(out_slug, schema) as writer:
            if isinstance(table, BatchStream):
                with table.open() as batches:
                    for item in batches:
                        if not item.batch.schema.equals(table.schema, check_metadata=True):
                            raise ValueError(f"batch schema changed at {item.source}:{item.row_offset}")
                        writer.write_batch(pa.RecordBatch.from_arrays(item.batch.columns, schema=schema))
                        del item
            else:
                writer.write_table(table)
        out_paths.append(writer.dest)
    return out_paths


@contextmanager
def open_canonical_writer(
    slug: str, schema: pa.Schema
) -> Iterator[pa.ipc.RecordBatchFileWriter]:
    """Streaming analogue of `write_canonical`: incrementally write RecordBatches
    to `outputs/v{n}/<slug>/arrow/<slug>.arrow.zstd` (zstd IPC file) without ever
    materializing the whole table.

    Atomic — each writer owns a unique temporary file in the destination directory
    and replaces the destination on clean exit. Failure cleans up only its own
    temporary file. Concurrent writers can each publish a complete artifact.
    `schema` may carry field- or schema-level `custom_metadata` (e.g. the
    `VARIANT_EXT` marker, GeoParquet `geo` schema metadata) — IPC preserves it
    losslessly, so downstream exporters see the same schema the streaming handler
    declared. Callers write via `writer.write_batch(b)` and/or
    `writer.write_table(t)` (a `RecordBatchFileWriter` supports both); the
    destination is `writer.dest`, an attribute raincloud sets on pyarrow's
    writer, not one pyarrow defines.

    Raises ValueError on duplicate top-level column names: no format derived
    from the spine can hold them (see `dedupe_column_names`).
    """
    from .lifecycle import canonical_completed, canonical_destination

    duplicates = sorted({n for n in schema.names if schema.names.count(n) > 1})
    if duplicates:
        raise ValueError(
            f"{slug}: canonical schema repeats column name(s) {duplicates}; "
            "Parquet cannot read them back and Vortex rejects them — rename them "
            "first (canonical.dedupe_column_names)")
    format_dir = output_format_dir(slug, "arrow")
    format_dir.mkdir(parents=True, exist_ok=True)
    dest = format_dir / f"{slug}.arrow.zstd"
    canonical_destination(slug, dest)
    tmp = dest.parent / f".{dest.name}.{uuid4().hex}.tmp"
    opts = pa.ipc.IpcWriteOptions(compression="zstd")
    try:
        with pa.OSFile(str(tmp), "wb") as sink:
            with pa.ipc.new_file(sink, schema, options=opts) as writer:
                writer.dest = dest
                yield writer
        tmp.replace(dest)
        canonical_completed(dest)
    finally:
        tmp.unlink(missing_ok=True)
    print(f"[canonical] {display_path(dest)}")
