# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Built-in exporters — read the canonical Arrow IPC artifact and write one
output format each. Registered in the package registry at import (mirroring
`handlers/__init__.py`), so importing `raincloud.pipeline.export` makes them
resolvable via `get_exporter`.

`cell_id` is the QUALIFIED registry key + `ExportResult.format_id`
(`parquet@py`, `vortex@py`) — parquet@py is pyarrow; vortex@py is the Vortex
Rust core reached through its Python binding. `format_id` is the BARE format.
Every writer of a format, these and the sidecars alike, publishes the same
`<fmt>/<slug>.<ext>`; which writer made it is provenance, not part of the path.

Every file these writers make is read back before it is reported (`read_back`):
with the same format's in-process reader, compared to the canonical window by
window, inside the export's child process, so the export's time and memory
ceilings (`bounded`) bound the read too. A file that does not read back to the
canonical -- a mismatch or a read error -- is `roundtrip=False`, which
`run_exporters` records as the format unavailable, never promoted. So an
in-process writer's `roundtrip` is always measured, never None.

Neither writer can carry a Parquet VARIANT *logical type* through: pyarrow
can't emit one at all, and the Vortex round-trip drops the VARIANT annotation
(the column survives as its shredded `struct<metadata, value, ...>`). So both
report `variant_faithful=False` when a VARIANT column is present — an honest
ledger row, not a failure.
"""
from __future__ import annotations

import importlib.util
import time
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from raincloud._cache import sha256_file

from ..discovery import VARIANT_EXT, has_variant
from ..spec import (
    PARQUET_PAGE_INDEX_COLUMNS,
    VORTEX_DATA_BLOCK_BYTES,
    VORTEX_ROW_BLOCK_ROWS,
    ParquetOptions,
    display_path,
    parquet_options,
    prepared_artifact,
    prepared_parquet,
    prepared_vortex,
    row_group_cap,
    row_group_probe_rows,
    row_group_target_bytes,
    row_group_target_encoded_bytes,
    write_settings,
)
from . import register
from .base import Compliance, ExportResult, slug_from_canonical


def tmp_path(dest: Path) -> Path:
    """Writer-owned sibling for atomic publication without touching other writes."""
    return dest.parent / f".{dest.name}.{uuid4().hex}.tmp"


def read_back(cell: str, written: Path, canonical: Path) -> tuple[bool, str]:
    """Read `written` back with `cell`'s own in-process reader and compare it to
    the canonical: (round-trips, why not).

    Streamed (`readers.stream_verdict`): one batch of each side at a time, so
    reading back a file larger than memory costs a batch, not the file. A read
    error is a mismatch like any other. The reader yields pass or fail; anything
    else would be the comparator declining to decide on our own file, which is
    a bug here to fix, not a verdict, so it raises.
    """
    from . import get_reader

    started = time.monotonic()
    verdict = get_reader(cell).read_conformance(written, canonical)
    if verdict.status not in ("pass", "fail"):
        raise RuntimeError(f"{cell}: reading back its own file gave {verdict.status!r} "
                           f"({verdict.note}); an in-process read-back decides pass or fail")
    if verdict.status == "fail":
        return False, f"read back: {verdict.note}" + (f" — {verdict.detail}" if verdict.detail else "")
    print(f"  [read back] {cell}: {display_path(written)} matches the canonical "
          f"({time.monotonic() - started:.1f}s)")
    return True, ""


def _groups(reader, row_group: int, byte_target: int):
    """Cut the canonical's batches into row groups: yield (batches, by_bytes).

    A group closes at exactly `row_group` rows, slicing the batch that crosses
    it and carrying the rest into the next group, as arrow-rs does -- so every
    Parquet writer gives one recipe the same layout. The `byte_target` DECODED
    byte ceiling can close a group earlier, at a batch or slice boundary.
    `by_bytes` says the ceiling closed it before its row count did; the tail's
    is None. An empty canonical still yields its one empty group.
    """
    pending: list[pa.RecordBatch] = []
    pending_rows = pending_bytes = 0
    yielded = False
    for i in range(reader.num_record_batches):
        b = reader.get_batch(i)
        if not b.num_rows:
            pending.append(b)
            continue
        start = 0
        while start < b.num_rows:
            piece = b.slice(start, min(row_group - pending_rows, b.num_rows - start))
            if pending_rows and pending_bytes + piece.nbytes > byte_target:
                yield pending, True
                pending, pending_rows, pending_bytes, yielded = [], 0, 0, True
                continue
            pending.append(piece)
            pending_rows += piece.num_rows
            pending_bytes += piece.nbytes
            start += piece.num_rows
            if pending_rows >= row_group or pending_bytes >= byte_target:
                yield pending, pending_rows < row_group
                pending, pending_rows, pending_bytes, yielded = [], 0, 0, True
    if pending_rows or not yielded:
        yield pending, None


class UnsupportedOption(ValueError):
    """A write option this writer's library cannot honour: the export fails,
    and the failure is recorded, rather than the file being written another way."""


def _leaf_paths(schema: pa.Schema) -> list[str]:
    """The Parquet leaf column paths pyarrow writes `schema` as, in order."""
    sink = pa.BufferOutputStream()
    pq.write_table(schema.empty_table(), sink)
    written = pq.ParquetFile(pa.BufferReader(sink.getvalue())).schema
    return [written.column(i).path for i in range(len(written))]


def _writer_options(options: ParquetOptions, schema: pa.Schema) -> dict:
    """pyarrow's `ParquetWriter` arguments for `options`. An unset setting is
    left out, so pyarrow's own default applies, except two: unset, a page index
    (when statistics are on) and page checksums are written, as parquet-java
    writes them, where pyarrow writes neither.

    pyarrow writes a page index for every column with statistics or for none,
    so `page_index_columns` (page statistics for some columns, chunk statistics
    for all) is refused.
    """
    if options.page_index_columns is not None:
        raise UnsupportedOption(
            f"parquet@py cannot honour {PARQUET_PAGE_INDEX_COLUMNS}={options.page_index_columns}: pyarrow "
            "writes a page index for every column that has statistics, or for none")
    statistics: bool | list[str] = options.statistics
    if options.statistics and options.statistics_columns is not None:
        statistics = _leaf_paths(schema)[:options.statistics_columns]
    page_index = options.statistics if options.page_index is None else options.page_index
    page_checksums = True if options.page_checksums is None else options.page_checksums
    kwargs = {"compression": options.compression, "write_statistics": statistics,
              "write_page_index": page_index, "write_page_checksum": page_checksums}
    for name, value in (("compression_level", options.compression_level),
                        ("data_page_size", options.page_bytes),
                        ("max_rows_per_page", options.page_rows), ("use_dictionary", options.dictionary),
                        ("dictionary_pagesize_limit", options.dictionary_page_bytes)):
        if value is not None:
            kwargs[name] = value
    return kwargs


def _write_parquet(canonical: Path, dest: Path, row_group: int, byte_target: int,
                   *, options: ParquetOptions) -> bool:
    """Stream the canonical into `dest` in the groups `_groups` cuts.

    Returns whether the byte ceiling closed EVERY group but the tail before its
    row count did; only then can asking for more rows per group not change the
    file. One heavy region among light batches caps only its own groups.
    """
    byte_closed: list[bool] = []  # one flag per group, the tail left out
    with pa.ipc.open_file(str(canonical)) as reader:
        schema = reader.schema
        with pq.ParquetWriter(dest, schema, **_writer_options(options, schema)) as writer:
            for group, by_bytes in _groups(reader, row_group, byte_target):
                if by_bytes is not None:
                    byte_closed.append(by_bytes)
                if group:
                    writer.write_table(pa.Table.from_batches(group, schema=schema),
                                       row_group_size=row_group)
    return bool(byte_closed) and all(byte_closed)


def _probe_encoded(canonical: Path, want_rows: int, probe: Path, *, options: ParquetOptions) -> tuple[int, int]:
    """Encode the real write's FIRST group at `want_rows` rows/group as ONE row
    group at `probe`; return (rows, encoded). The decoded-byte ceiling or the
    end of the file can make it shorter, as they would the real group."""
    with pa.ipc.open_file(str(canonical)) as reader:
        schema = reader.schema
        taken, _ = next(_groups(reader, want_rows, row_group_target_bytes()))
    if not sum(b.num_rows for b in taken):
        return 0, 0
    try:
        table = pa.Table.from_batches(taken, schema=schema)
        with pq.ParquetWriter(probe, schema, **_writer_options(options, schema)) as writer:
            # One group of exactly this size. Left to its own default pyarrow
            # splits at ~1Mi rows, and the measurement would then describe a
            # group of THAT size -- useless, since dictionary and page overhead
            # amortize differently as a group grows.
            writer.write_table(table, row_group_size=max(1, table.num_rows))
        meta = pq.ParquetFile(probe).metadata
        return meta.num_rows, sum(meta.row_group(i).total_byte_size
                                  for i in range(meta.num_row_groups))
    finally:
        probe.unlink(missing_ok=True)


def _rows_for_encoded_target(canonical: Path, probe: Path, *, options: ParquetOptions, row_cap: int) -> int:
    """Rows per group that land near the encoded target — arrow-rs, approximated.

    Iterated, because bytes-per-row is not constant in the group size: a small
    probe amortizes dictionaries worse than a full group and so overestimates.
    The probe file is written at `probe`, beside the destination.
    """
    target = row_group_target_encoded_bytes()
    rows, encoded = _probe_encoded(canonical, row_group_probe_rows(), probe, options=options)
    if not rows or not encoded:
        return row_cap
    want = min(max(1, int(target * rows / encoded)), row_cap)
    for _ in range(2):
        if want <= rows:
            break
        rows2, encoded2 = _probe_encoded(canonical, want, probe, options=options)
        if not rows2 or not encoded2:
            break
        if rows2 < want:
            # The whole file, or the decoded-byte ceiling, ended the probe:
            # either way the real write stops there too.
            return min(want, row_cap)
        refined = min(max(1, int(target * rows2 / encoded2)), row_cap)
        if abs(refined - want) <= want * 0.05:
            return refined
        rows, encoded, want = rows2, encoded2, refined
    return want


def _corrected_rows(written: Path, used_rows: int, target: int, row_cap: int) -> int | None:
    """Rows/group for a second pass, or None if the first was close enough.

    The probe samples the HEAD; a file whose head does not encode like its body
    defeats that. Judged on the MEDIAN group — one short tail group is normal.
    """
    meta = pq.ParquetFile(written).metadata
    if meta.num_row_groups < 2:
        return None
    sizes = sorted(meta.row_group(i).total_byte_size for i in range(meta.num_row_groups))
    median = sizes[len(sizes) // 2]
    if not median or abs(median - target) <= target * 0.10:
        return None
    corrected = min(max(1, int(used_rows * target / median)), row_cap)
    return corrected if corrected != used_rows else None


class ParquetExporter:
    """pyarrow Parquet writer — the `parquet@py` cell."""

    format_id = "parquet"
    cell_id = "parquet@py"

    def unavailable(self) -> str | None:
        return None  # pyarrow is a base dependency

    def out_path(self, slug: str) -> Path:
        return prepared_parquet(slug)

    def export(self, spec: dict, canonical: Path, dest: Path | None = None) -> ExportResult:
        # The canonical's own slug (from its path) is the authority for
        # artifact placement — not spec["slug"], which can differ under a
        # multi-output transform. `spec` still supplies write.* opts + logging.
        slug = slug_from_canonical(canonical)

        # The recipe's compression and statistics, and the page knobs: the same
        # options every Parquet lane is given (`spec.parquet_options`).
        options = parquet_options(spec)
        # A CAP, not a target. arrow-rs's `max_row_group_row_count` and
        # parquet-java's `parquet.block.row.count.limit` are both effectively off
        # by default so that BYTES decide the group; absent here means uncapped.
        row_cap = row_group_cap(spec)

        dest = dest or self.out_path(slug)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = tmp_path(dest)
        print(f"[export:parquet@py] {display_path(dest)}")
        # Stream batch-by-batch — NOT reader.read_all(). The streaming handlers
        # (osm/ghcn/stack_exchange/…) write canonicals far larger than RAM;
        # materializing the whole Table here would OOM the very slugs the
        # streaming path exists to bound.
        #
        # `write_batch` opens a NEW row group per call, so writing one canonical
        # batch per call would make the row-group size `BatchLimits.rows`, the
        # 4096-row INGESTION memory knob. Batches are accumulated instead.
        #
        # Groups are sized by ENCODED bytes, following arrow-rs: a fixed row
        # count cannot hold a byte size, and across TPC-H SF10 the same 1,048,576
        # rows gave encoded groups from 63 MiB (lineitem) to 164 MiB (customer).
        # arrow-rs measures the live group and slices the batch at the row that
        # fits; pyarrow exposes no in-progress size, so the size is estimated
        # from a probe and checked against what was actually written.
        byte_target = row_group_target_bytes()
        target_encoded = row_group_target_encoded_bytes()
        with pa.ipc.open_file(str(canonical)) as reader:
            schema = reader.schema
        try:
            row_group = _rows_for_encoded_target(
                canonical, tmp.with_suffix(".probe"), options=options, row_cap=row_cap,
            )
            bytes_bound = _write_parquet(canonical, tmp, row_group, byte_target, options=options)
            corrected = _corrected_rows(tmp, row_group, target_encoded, row_cap)
            # When the decoded-byte ceiling closed every group, more rows per
            # group are capped the same way and the second pass would rewrite
            # an identical file.
            if corrected is not None and not (bytes_bound and corrected > row_group):
                print(f"  [row-groups] re-sizing {row_group:,} -> {corrected:,} rows/group")
                _write_parquet(canonical, tmp, corrected, byte_target, options=options)
            tmp.replace(dest)
        finally:
            tmp.unlink(missing_ok=True)

        variant = has_variant(schema)
        note = (
            "VARIANT column downgraded to its shredded struct — pyarrow cannot "
            "emit a Parquet VARIANT logical type"
        ) if variant else ""
        roundtrip, why = read_back(self.cell_id, dest, canonical)
        return ExportResult(
            format_id=self.cell_id,
            out_path=dest,
            nbytes=dest.stat().st_size,
            sha256=sha256_file(dest),
            compliance=Compliance(
                roundtrip=roundtrip,
                variant_faithful=not variant,
                note="; ".join(n for n in (why, note) if n),
            ),
        )


_EXTENSION_NAME = b"ARROW:extension:name"


class VortexExporter:
    """Vortex writer — the `vortex@py` cell.

    Streams the canonical Arrow batches through a `RecordBatchReader` into
    `vortex.io.write`, feeding the canonical's OWN stored batches, one
    non-chunked RecordBatch at a time.

    The batching does not bound what Vortex encodes: its default write strategy
    regroups every column into 8,192-row blocks and compresses each block
    whole, and `vortex.io` exposes no block-size option. A column whose 8,192
    rows hold more than 4 GiB (code-contests' `incorrect_solutions`: ~7 GiB)
    fails inside Vortex, which keeps some of a block's bytes in one buffer
    addressed by u32. vortex-data 0.86.1 reports that as `struct column writer
    finished before all chunks were sent`, hiding the column's own error.
    Nothing fed from here avoids it; it fails loudly, never silently corrupts.

    Column names are the canonical's: a canonical never carries duplicate
    top-level names (`canonical.open_canonical_writer` refuses them), which is
    what Vortex's StructLayout requires.
    """

    format_id = "vortex"
    cell_id = "vortex@py"

    def unavailable(self) -> str | None:
        if importlib.util.find_spec("vortex") is not None:
            return None
        from raincloud._extras import extra_for
        extra = extra_for("vortex-data")
        return f"needs vortex-data; install `raincloud[{extra}]`" if extra else "needs vortex-data"

    def out_path(self, slug: str) -> Path:
        return prepared_vortex(slug)

    def export(self, spec: dict, canonical: Path, dest: Path | None = None) -> ExportResult:
        slug = slug_from_canonical(canonical)
        dest = dest or self.out_path(slug)
        print(f"[export:vortex@py] {display_path(dest)}")
        missing = self.unavailable()
        if missing is not None:
            from raincloud.exceptions import BuildToolingMissing
            raise BuildToolingMissing(f"{self.cell_id} {missing}")
        import vortex.io as vxio

        settings = write_settings("vortex")
        for field, var in (("row_block_rows", VORTEX_ROW_BLOCK_ROWS), ("data_block_bytes", VORTEX_DATA_BLOCK_BYTES)):
            if settings[field] is not None:
                raise UnsupportedOption(f"vortex@py cannot honour {var}={settings[field]}: vortex-data's "
                                        "Python writer has no block size setting")
        # Unset is `vxio.write`, the default options; compact is BtrBlocks' compact encodings.
        write = (vxio.VortexWriteOptions.compact().write if settings["compact"]
                 else vxio.VortexWriteOptions.default().write if settings["compact"] is False else vxio.write)

        # `with` closes the reader (RecordBatchFileReader is a context manager,
        # not a .close()-able) — vxio.write consumes the batch generator
        # synchronously inside the block, so the reader is done before exit.
        with pa.ipc.open_file(str(canonical)) as reader:
            schema = reader.schema
            variant = has_variant(schema)

            # This lane exports VARIANT as its storage struct and reports
            # annotation loss. Use the same storage schema on the stream and
            # batches so Vortex cannot implicitly interpret one as native VARIANT.
            # Retain unrelated metadata and leave the canonical file unchanged.
            fields = []
            for field in schema:
                metadata = dict(field.metadata or {})
                if metadata.get(_EXTENSION_NAME) == VARIANT_EXT[_EXTENSION_NAME]:
                    for key in (*VARIANT_EXT, b"ARROW:extension:metadata"):
                        metadata.pop(key, None)
                    field = field.with_metadata(metadata or None)
                fields.append(field)
            schema = pa.schema(fields, metadata=schema.metadata)

            def batches():
                for i in range(reader.num_record_batches):
                    b = reader.get_batch(i)
                    yield pa.RecordBatch.from_arrays(b.columns, schema=schema)

            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = tmp_path(dest)
            rbr = pa.RecordBatchReader.from_batches(schema, batches())
            try:
                write(rbr, str(tmp))
                tmp.replace(dest)
            finally:
                tmp.unlink(missing_ok=True)

        note = ("VARIANT annotation not preserved on the Vortex round-trip — "
                "column survives as its shredded struct") if variant else ""
        roundtrip, why = read_back(self.cell_id, dest, canonical)
        return ExportResult(
            format_id=self.cell_id,
            out_path=dest,
            nbytes=dest.stat().st_size,
            sha256=sha256_file(dest),
            compliance=Compliance(
                roundtrip=roundtrip,
                variant_faithful=not variant,
                note="; ".join(n for n in (why, note) if n),
            ),
        )


def orc_storage_type(dtype: pa.DataType) -> pa.DataType:
    """`dtype` as both ORC lanes store it: ORC has no unsigned integers and no
    view types, so they widen to the next type that holds every value --
    uint8 -> int16, uint16 -> int32, uint32 -> int64, uint64 -> decimal(20, 0),
    string/binary views -> their plain types -- at any depth. Always, not by the
    data's range: a dataset's ORC schema must not change with its values. The
    comparators read each back as the canonical's type (`sidecars/compare_cases`).
    The Rust lane's `orc_storage_type` is the same rule."""
    widened = {pa.uint8(): pa.int16(), pa.uint16(): pa.int32(), pa.uint32(): pa.int64(),
               pa.uint64(): pa.decimal128(20, 0), pa.string_view(): pa.string(),
               pa.binary_view(): pa.binary()}
    if dtype in widened:
        return widened[dtype]
    if pa.types.is_struct(dtype):
        return pa.struct([f.with_type(orc_storage_type(f.type)) for f in dtype])
    if pa.types.is_map(dtype):
        return pa.map_(dtype.key_field.with_type(orc_storage_type(dtype.key_type)),
                       dtype.item_field.with_type(orc_storage_type(dtype.item_type)),
                       keys_sorted=dtype.keys_sorted)
    if pa.types.is_fixed_size_list(dtype):
        return pa.list_(dtype.value_field.with_type(orc_storage_type(dtype.value_type)), dtype.list_size)
    if pa.types.is_large_list(dtype):
        return pa.large_list(dtype.value_field.with_type(orc_storage_type(dtype.value_type)))
    if pa.types.is_list(dtype):
        return pa.list_(dtype.value_field.with_type(orc_storage_type(dtype.value_type)))
    return dtype


def _orc_writer_options(settings: dict) -> dict:
    """pyarrow's `ORCWriter` arguments for the ORC write settings; an unset one
    is left out, so pyarrow's default applies (zstd for the codec)."""
    codec = settings["compression"] or "zstd"
    kwargs = {"compression": "uncompressed" if codec == "none" else codec}
    for name, value in (("compression_strategy", settings["compression_strategy"]),
                        ("stripe_size", settings["stripe_bytes"]),
                        ("compression_block_size", settings["compression_block_bytes"])):
        if value is not None:
            kwargs[name] = value
    return kwargs


class OrcExporter:
    """pyarrow ORC writer, the Apache ORC C++ library -- the `orc@py` cell.

    Streams the canonical's stored batches into one `ORCWriter`, with the ORC
    write settings (`spec.FORMAT_SETTINGS["orc"]`): unset, zstd, since the API
    makes the caller pick a codec (its default is none), and the library's own
    stripe and compression block sizes and strategy. Unsigned integers and view types are widened
    first (`orc_storage_type`); any other type the library does not write
    (dictionaries, time, durations, ...) raises from it, and the build records
    ORC unavailable for that dataset with its error.
    """

    format_id = "orc"
    cell_id = "orc@py"

    def unavailable(self) -> str | None:
        if importlib.util.find_spec("pyarrow._orc") is not None:
            return None
        return "needs a pyarrow built with ORC support (this platform's wheel has none)"

    def out_path(self, slug: str) -> Path:
        return prepared_artifact(slug, "orc")

    def export(self, spec: dict, canonical: Path, dest: Path | None = None) -> ExportResult:
        import pyarrow.orc as orc

        slug = slug_from_canonical(canonical)
        dest = dest or self.out_path(slug)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = tmp_path(dest)
        print(f"[export:orc@py] {display_path(dest)}")
        with pa.ipc.open_file(str(canonical)) as reader:
            schema = reader.schema
            stored = pa.schema([f.with_type(orc_storage_type(f.type)) for f in schema], metadata=schema.metadata)
            try:
                writer = orc.ORCWriter(str(tmp), **_orc_writer_options(write_settings("orc")))
                try:
                    for i in range(reader.num_record_batches):
                        batch = pa.Table.from_batches([reader.get_batch(i)], schema=schema)
                        writer.write(batch if stored == schema else batch.cast(stored))
                    if not reader.num_record_batches:
                        writer.write(stored.empty_table())
                finally:
                    writer.close()
                tmp.replace(dest)
            finally:
                tmp.unlink(missing_ok=True)

        variant = has_variant(schema)
        note = ("VARIANT column written as its shredded struct — ORC has no VARIANT type"
                if variant else "")
        roundtrip, why = read_back(self.cell_id, dest, canonical)
        return ExportResult(
            format_id=self.cell_id,
            out_path=dest,
            nbytes=dest.stat().st_size,
            sha256=sha256_file(dest),
            compliance=Compliance(
                roundtrip=roundtrip,
                variant_faithful=not variant,
                note="; ".join(n for n in (why, note) if n),
            ),
        )


register(ParquetExporter())
register(VortexExporter())
register(OrcExporter())
