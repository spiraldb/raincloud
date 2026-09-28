# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse a Stack Exchange Data Dump XML file into a streamed parquet.

Format:

    <?xml version="1.0" encoding="utf-8"?>
    <tags>
      <row Id="1" TagName="javascript" Count="..." .../>
      ...
    </tags>

Memory strategy: same as `osm_pbf_split`. We iterparse the XML, accumulate a
fixed `batch_size` of rows, flush to the canonical Arrow writer, and repeat.
This keeps peak Python memory bounded regardless of input size (Posts.xml is
130 GB uncompressed).

Schema is fixed per table — taken from the `_SCHEMAS` map below. The
attributes that would otherwise be strings get cast to int/bool/timestamp per
the hints. A row carrying an attribute outside the schema, or a value that
does not parse as its column's type, fails the build naming the row: nothing
is dropped or nulled.

Returns `[]` from the handler so the `write_canonical` stage is skipped; the
canonical Arrow spine is fully written by the time the handler returns.
"""
from __future__ import annotations

import datetime as _dt
import xml.etree.ElementTree as ET
from pathlib import Path

import pyarrow as pa

from ..canonical import open_canonical_writer

# Fixed per-table schemas: every attribute the dump's rows carry.
_SCHEMAS: dict[str, pa.Schema] = {
    "posts": pa.schema([
        ("Id", pa.int64()), ("PostTypeId", pa.int8()),
        ("AcceptedAnswerId", pa.int64()), ("ParentId", pa.int64()),
        ("CreationDate", pa.timestamp("us")),
        ("Score", pa.int32()), ("ViewCount", pa.int64()),
        ("Body", pa.string()),
        ("OwnerUserId", pa.int64()), ("OwnerDisplayName", pa.string()),
        ("LastEditorUserId", pa.int64()), ("LastEditorDisplayName", pa.string()),
        ("LastEditDate", pa.timestamp("us")),
        ("LastActivityDate", pa.timestamp("us")),
        ("Title", pa.string()), ("Tags", pa.string()),
        ("AnswerCount", pa.int32()), ("CommentCount", pa.int32()),
        ("FavoriteCount", pa.int32()),
        ("CommunityOwnedDate", pa.timestamp("us")),
        ("ClosedDate", pa.timestamp("us")),
        ("ContentLicense", pa.string()),
    ]),
    "users": pa.schema([
        ("Id", pa.int64()), ("Reputation", pa.int64()),
        ("CreationDate", pa.timestamp("us")),
        ("DisplayName", pa.string()),
        ("LastAccessDate", pa.timestamp("us")),
        ("WebsiteUrl", pa.string()), ("Location", pa.string()),
        ("AboutMe", pa.string()),
        ("Views", pa.int64()), ("UpVotes", pa.int64()), ("DownVotes", pa.int64()),
        ("ProfileImageUrl", pa.string()), ("AccountId", pa.int64()),
    ]),
    "tags": pa.schema([
        ("Id", pa.int64()), ("TagName", pa.string()), ("Count", pa.int64()),
        ("ExcerptPostId", pa.int64()), ("WikiPostId", pa.int64()),
        ("IsModeratorOnly", pa.bool_()), ("IsRequired", pa.bool_()),
    ]),
    "badges": pa.schema([
        ("Id", pa.int64()), ("UserId", pa.int64()), ("Name", pa.string()),
        ("Date", pa.timestamp("us")),
        ("Class", pa.int8()), ("TagBased", pa.bool_()),
    ]),
    "postlinks": pa.schema([
        ("Id", pa.int64()), ("CreationDate", pa.timestamp("us")),
        ("PostId", pa.int64()), ("RelatedPostId", pa.int64()),
        ("LinkTypeId", pa.int8()),
    ]),
}


_BOOLEANS = {"true": True, "false": False}


def _coerce(val: str | None, ty: pa.DataType):
    """`val` as `ty`; an empty or absent attribute is null. Raises ValueError
    on a value that is not of the type."""
    if val is None or val == "":
        return None
    if pa.types.is_integer(ty):
        return int(val)
    if pa.types.is_boolean(ty):
        if val.lower() not in _BOOLEANS:
            raise ValueError(f"{val!r} is not a boolean")
        return _BOOLEANS[val.lower()]
    if pa.types.is_timestamp(ty):
        return _dt.datetime.fromisoformat(val).replace(tzinfo=None)
    return val


def stack_exchange_split(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                          table: str, batch_size: int = 100_000
                          ) -> list[tuple[str, pa.Table]]:
    if table not in _SCHEMAS:
        raise ValueError(f"unknown Stack Exchange table {table!r}; "
                         f"expected one of {sorted(_SCHEMAS)}")
    schema = _SCHEMAS[table]
    column_types = {f.name: f.type for f in schema}

    xml_files = [p for p, _ in parsed if str(p).lower().endswith(".xml")]
    if not xml_files:
        raise ValueError("stack_exchange_split: no .xml files in extracted output")
    if len(xml_files) > 1:
        raise ValueError(f"stack_exchange_split: multiple XML files: "
                         f"{[p.name for p in xml_files]}")
    path = xml_files[0]
    print(f"  streaming {path.name} ({path.stat().st_size / 1e9:.2f} GB on disk, table={table})")

    # Per-column accumulators — one list per expected column
    cols: dict[str, list] = {name: [] for name in column_types}
    count = 0

    def flush(writer):
        nonlocal count, cols
        if not cols[next(iter(cols))]:  # empty batch
            return
        arrays = []
        for name in column_types:
            try:
                arrays.append(pa.array(cols[name], type=column_types[name]))
            except (pa.ArrowInvalid, pa.ArrowTypeError, OverflowError) as exc:
                raise ValueError(f"stack_exchange_split: {path.name} column {name} "
                                 f"does not fit {column_types[name]}: {exc}") from None
        batch = pa.record_batch(arrays, schema=schema)
        writer.write_batch(batch)
        count += len(cols[next(iter(cols))])
        cols = {name: [] for name in column_types}

    with open_canonical_writer(spec["slug"], schema) as writer:
        # `events=("start", "end")` so the first yield hands us the root. That
        # matters: `elem.clear()` empties a row but leaves it attached to the
        # root, whose child list then grows for the life of the parse -- measured
        # at ~57 B/row against a ~60M-row Posts.xml, which is the opposite of the
        # "bounded memory regardless of input size" this handler advertises.
        # Detaching the consumed rows is what actually bounds it.
        context = ET.iterparse(path, events=("start", "end"))
        _, root = next(context)
        for event, elem in context:
            if event != "end" or elem.tag != "row":
                continue
            attrib = elem.attrib
            unknown = attrib.keys() - column_types.keys()
            if unknown:
                raise ValueError(f"stack_exchange_split: {path.name} row Id={attrib.get('Id')!r} "
                                 f"has attribute(s) {sorted(unknown)} outside the {table} schema")
            for name, ty in column_types.items():
                try:
                    cols[name].append(_coerce(attrib.get(name), ty))
                except ValueError as exc:
                    raise ValueError(f"stack_exchange_split: {path.name} row Id={attrib.get('Id')!r} "
                                     f"{name}={attrib.get(name)!r} is not {ty}: {exc}") from None
            elem.clear()
            del root[:]
            if len(cols[next(iter(cols))]) >= batch_size:
                flush(writer)
                if count % (batch_size * 10) == 0:
                    print(f"    {count:,} rows flushed")
        flush(writer)
    print(f"    total: {count:,} rows written")
    return []
