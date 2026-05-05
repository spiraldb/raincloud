# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 3 — parse extracted files into pyarrow Tables.

Reads only the `parse` block. Returns one Table per input file. Multi-file
merging happens in the transform stage.

Readers:
    csv      : pyarrow.csv.read_csv
    parquet  : pyarrow.parquet.read_table
    jsonl    : line-delimited JSON via pyarrow.json.read_json
    json     : whole-file JSON via Python + Arrow
    sqlite   : via duckdb
    xml      : delegated to transform stage (handler-specific)
    pbf      : delegated to transform stage (osm_pbf_split)
    custom   : delegated to scripts/pipeline/custom_parse.py
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pyarrow as pa
import pyarrow.csv as pac
import pyarrow.json as paj
import pyarrow.parquet as pq

from .spec import spec_field


def parse_csv(spec: dict, path: Path) -> pa.Table:
    opts = spec_field(spec, "parse.options", {}) or {}
    read_opts = pac.ReadOptions(
        encoding=opts.get("encoding", "utf-8"),
        block_size=opts.get("block_size") or 1 << 20,  # default 1MB; bump for sources with very long cells
    )

    # Lenient-by-default posture for CSV: live upstreams routinely drift in
    # ways that aren't worth fighting per-slug. `newlines_in_values=True`
    # tolerates multi-line quoted cells; `invalid_row_handler` skips rows
    # whose column count doesn't match the inferred schema (with a printed
    # count so silent data loss is visible). Opt back into strict parsing
    # via `parse.options.strict: true` for sources where the shape is
    # contractual (clickbench-hits, etc.).
    strict = bool(opts.get("strict", False))
    skipped_counter = [0]

    def _skip_handler(_row):
        skipped_counter[0] += 1
        return "skip"

    parse_opts = pac.ParseOptions(
        delimiter=opts.get("delimiter", ","),
        quote_char=opts.get("quote_char") or False if opts.get("quoting") == "none" else '"',
        newlines_in_values=bool(opts.get("newlines_in_values", True)),
        invalid_row_handler=None if strict else _skip_handler,
    )
    convert_opts = pac.ConvertOptions(strings_can_be_null=True)
    if opts.get("has_header") is False:
        read_opts.autogenerate_column_names = True
    if opts.get("skip_rows"):
        read_opts.skip_rows = int(opts["skip_rows"])
    table = pac.read_csv(path, read_options=read_opts, parse_options=parse_opts,
                         convert_options=convert_opts)
    if skipped_counter[0]:
        print(f"  skipped {skipped_counter[0]:,} malformed row(s) in {path.name}")
    return table


def parse_parquet(spec: dict, path: Path) -> pa.Table:
    return pq.read_table(path)


def parse_jsonl(spec: dict, path: Path) -> pa.Table:
    return paj.read_json(path)


def parse(spec: dict, inputs: list[Path]) -> Iterator[tuple[Path, pa.Table]]:
    reader = spec_field(spec, "parse.reader", "csv")
    print(f"[parse] {spec['slug']} ({reader}, {len(inputs)} file(s))")
    for path in inputs:
        if reader == "csv":         yield path, parse_csv(spec, path)
        elif reader == "parquet":   yield path, parse_parquet(spec, path)
        elif reader == "jsonl":     yield path, parse_jsonl(spec, path)
        elif reader in ("json", "xml", "pbf", "sqlite", "custom"):
            # Defer to transform handler; yield path with a sentinel table
            yield path, None  # type: ignore[misc]
        else:
            raise ValueError(f"unknown parse.reader: {reader}")
