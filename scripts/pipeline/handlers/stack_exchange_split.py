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
fixed `batch_size` of rows, flush to a ParquetWriter, and repeat. This keeps
peak Python memory bounded regardless of input size (Posts.xml is 130 GB
uncompressed).

Schema is fixed per table — taken from the `_SCHEMAS` map below. Any
attributes the XML carries beyond the expected set are dropped. The
attributes we know about that would otherwise be strings get cast to
int/bool/timestamp per the hints.

Returns `[]` from the handler so the normal write stage is a no-op; the
parquet is fully written by the time the handler returns.
"""
from __future__ import annotations

import datetime as _dt
import xml.etree.ElementTree as ET
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Fixed per-table schemas. These are the Stack Exchange attributes we care
# about; everything else in the XML is dropped.
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


def _coerce(val: str | None, ty: pa.DataType):
    if val is None or val == "":
        return None
    if pa.types.is_integer(ty):
        try: return int(val)
        except ValueError: return None
    if pa.types.is_boolean(ty):
        return val.lower() in ("true", "1", "yes")
    if pa.types.is_timestamp(ty):
        try: return _dt.datetime.fromisoformat(val).replace(tzinfo=None)
        except ValueError: return None
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

    from ..spec import REPO_ROOT, output_format_dir, spec_field
    out_path = output_format_dir(spec["slug"], "parquet") / spec_field(spec, "write.output", f"{spec['slug']}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    compression = spec_field(spec, "write.compression", "zstd")

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
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                # Fall back to string with best-effort str() on values
                fallback = [None if v is None else str(v) for v in cols[name]]
                arrays.append(pa.array(fallback, type=pa.string()))
        batch = pa.record_batch(arrays, schema=schema)
        writer.write_batch(batch)
        count += len(cols[next(iter(cols))])
        cols = {name: [] for name in column_types}

    with pq.ParquetWriter(out_path, schema, compression=compression) as writer:
        context = ET.iterparse(path, events=("end",))
        for _, elem in context:
            if elem.tag != "row":
                continue
            attrib = elem.attrib
            for name, ty in column_types.items():
                cols[name].append(_coerce(attrib.get(name), ty))
            elem.clear()
            if len(cols[next(iter(cols))]) >= batch_size:
                flush(writer)
                if count % (batch_size * 10) == 0:
                    print(f"    {count:,} rows flushed")
        flush(writer)
    print(f"    total: {count:,} rows written to {out_path.relative_to(REPO_ROOT)}")
    return []
