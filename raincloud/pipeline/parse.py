# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 3 — parse extracted files into pyarrow Tables or BatchStreams.

Reads the `parse` block, plus `transform.handler` to learn which readers the
handler takes as batches (its `batch_readers`, declared with
`batches.batch_input`). Yields one `(path, value)` per input file, where value
is a Table, a BatchStream, or None for a reader the handler parses itself.
Multi-file merging happens in the transform stage.

Readers:
    csv      : pyarrow.csv.read_csv -> Table
    parquet  : a lazy BatchStream for a handler declaring "parquet"; otherwise a Table
    jsonl    : line-delimited JSON via pyarrow.json.read_json -> Table
    json / xml / pbf / sqlite / custom : None -- the transform handler reads the file
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.csv as pac
import pyarrow.json as paj
import pyarrow.parquet as pq

from .batches import BatchLimits, BatchStream, SourceBatch, split_batch
from .spec import spec_field


def parse_csv(spec: dict, path: Path, column_types: dict[str, pa.DataType] | None = None) -> pa.Table:
    """Read one CSV per the recipe's `parse.options`. `column_types` pins the
    named columns' types instead of inferring them (a handler merging several
    files uses it so every file reads a column the same way)."""
    opts = spec_field(spec, "parse.options", {}) or {}
    read_opts = pac.ReadOptions(
        encoding=opts.get("encoding", "utf-8"),
        block_size=opts.get("block_size") or 1 << 20,  # default 1MB; bump for sources with very long cells
    )

    # `quoting: "none"` disables quoting; otherwise `quote_char` is used,
    # defaulting to '"'.
    if opts.get("quoting") == "none":
        quote_char = False
    else:
        quote_char = opts.get("quote_char") or '"'

    # Strict: a row whose field count differs from the header's fails the
    # build, naming the file and row. Skipping it would drop an upstream record;
    # a source that needs different quoting or delimiters says so in its recipe.
    # `newlines_in_values=True` reads multi-line quoted cells.
    invalid: list = []

    def _invalid_row(row):
        invalid.append(row)
        return "error"

    parse_opts = pac.ParseOptions(
        delimiter=opts.get("delimiter", ","),
        quote_char=quote_char,
        newlines_in_values=bool(opts.get("newlines_in_values", True)),
        invalid_row_handler=_invalid_row,
    )
    convert_opts = pac.ConvertOptions(strings_can_be_null=True, column_types=column_types)
    if opts.get("has_header") is False:
        read_opts.autogenerate_column_names = True
    if opts.get("skip_rows"):
        read_opts.skip_rows = int(opts["skip_rows"])
    try:
        return pac.read_csv(path, read_options=read_opts, parse_options=parse_opts,
                            convert_options=convert_opts)
    except pa.ArrowInvalid:
        if not invalid:
            raise
    # pyarrow numbers the row only when parsing on one thread; re-read that way
    # to name it.
    row = invalid[0]
    if row.number is None:
        invalid.clear()
        read_opts.use_threads = False
        try:
            pac.read_csv(path, read_options=read_opts, parse_options=parse_opts,
                         convert_options=convert_opts)
        except pa.ArrowInvalid:
            pass
        row = invalid[0] if invalid else row
    # The number counts rows, header included; a quoted cell spanning lines counts once.
    where = f"row {row.number:,}" if row.number is not None else "a row"
    raise ValueError(
        f"{path}: {where} has {row.actual_columns} fields, not {row.expected_columns}: "
        f"{row.text[:200]!r}. Fix the recipe's parse.options (delimiter, quote_char, "
        "quoting, skip_rows) so every row parses; rows are never skipped.")


def parse_parquet(spec: dict, path: Path) -> pa.Table:
    return pq.read_table(path)


def parquet_batches(path: Path) -> BatchStream:
    """Plan from the footer; retain one source generation across projected passes.

    A consumer may open the stream several times (a statistics pass, then
    emission). The stat tuple and schema recheck only guarantee those passes
    read the same file: they detect a source replaced between them, and are not
    a check on whether the download itself can be trusted.
    """
    limits = BatchLimits.from_env()

    def identity():
        stat = path.stat()
        return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns

    planned = identity()
    with pq.ParquetFile(path) as reader:
        full_schema = reader.schema_arrow

    def check_source():
        if identity() != planned:
            raise ValueError(f"Parquet source changed after planning: {path}")

    check_source()

    def make(columns):
        schema = full_schema if columns is None else pa.schema(
            [full_schema.field(name) for name in columns], metadata=full_schema.metadata)

        def batches():
            check_source()
            with pq.ParquetFile(path) as reader:
                if not reader.schema_arrow.equals(full_schema, check_metadata=True):
                    raise ValueError(f"Parquet schema changed after planning: {path}")
                offset = 0
                for decoded in reader.iter_batches(batch_size=limits.rows, columns=columns, use_threads=False):
                    for batch in split_batch(decoded, limits):
                        yield SourceBatch(path, offset, batch)
                        offset += batch.num_rows
                check_source()

        return BatchStream(schema, batches, make)

    return make(None)


def parse_jsonl(spec: dict, path: Path) -> pa.Table:
    return paj.read_json(path)


def parse(spec: dict, inputs: list[Path]) -> Iterator[tuple[Path, pa.Table | BatchStream | None]]:
    reader = spec_field(spec, "parse.reader", "csv")
    print(f"[parse] {spec['slug']} ({reader}, {len(inputs)} file(s))")
    from .handlers import get

    handler = get(spec_field(spec, "transform.handler", "identity"))
    # Only the parquet reader streams here. A handler declaring "custom" builds
    # its own BatchStream from the path, which it receives with None.
    batched = reader in getattr(handler, "batch_readers", ())
    for path in inputs:
        if reader == "csv":         yield path, parse_csv(spec, path)
        elif reader == "parquet":   yield path, parquet_batches(path) if batched else parse_parquet(spec, path)
        elif reader == "jsonl":     yield path, parse_jsonl(spec, path)
        elif reader in ("json", "xml", "pbf", "sqlite", "custom"):
            # Defer to transform handler; yield path with a sentinel table
            yield path, None  # type: ignore[misc]
        else:
            raise ValueError(f"unknown parse.reader: {reader}")
