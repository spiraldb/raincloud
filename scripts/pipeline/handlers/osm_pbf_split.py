# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse an OSM PBF file and emit one of the three element kinds as Arrow.

Dispatched by `element_kind` param:
    "nodes"     → (id, version, timestamp, lon, lat, tags, geometry=POINT WKB)
    "ways"      → (id, version, timestamp, refs, tags, geometry=LINESTRING WKB)
    "relations" → (id, version, timestamp, members, tags)   (no geometry)

The handler is memory-bounded: it opens a ParquetWriter directly and streams
RecordBatches of `batch_size` rows each. This is required for country-scale
PBFs — Germany has 432 M nodes, which would need tens of GB of Python memory
if we collected them into lists before converting to Arrow.

Because streaming requires direct file output, this handler is an exception
to the usual handler contract: it returns an empty list `[]` instead of
`[(slug, Table)]`, and the normal write stage becomes a no-op. The parquet
is fully written by the time the handler returns.

GeoParquet 1.1 metadata is attached to the output file so downstream
consumers (DuckDB spatial, GeoPandas, ogr2ogr) recognise the `geometry`
column as WGS84-CRS WKB.
"""
from __future__ import annotations

import json
from pathlib import Path

import osmium
import pyarrow as pa
import pyarrow.parquet as pq


def _wgs84_geo_metadata(geometry_types: list[str]) -> dict[bytes, bytes]:
    meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {
            "encoding": "WKB",
            "geometry_types": geometry_types,
            "crs": None,  # null = WGS84 longitude/latitude per GeoParquet 1.1
        }},
    }
    return {b"geo": json.dumps(meta).encode()}


_TAGS_TYPE = pa.list_(pa.struct([("key", pa.string()), ("value", pa.string())]))
_MEMBERS_TYPE = pa.list_(pa.struct([
    ("ref", pa.int64()), ("role", pa.string()), ("type", pa.string()),
]))

_NODE_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("version", pa.int32()),
    ("timestamp", pa.string()),
    ("lon", pa.float64()),
    ("lat", pa.float64()),
    ("tags", _TAGS_TYPE),
    ("geometry", pa.binary()),
])
_WAY_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("version", pa.int32()),
    ("timestamp", pa.string()),
    ("refs", pa.list_(pa.int64())),
    ("tags", _TAGS_TYPE),
    ("geometry", pa.binary()),
])
_RELATION_SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("version", pa.int32()),
    ("timestamp", pa.string()),
    ("members", _MEMBERS_TYPE),
    ("tags", _TAGS_TYPE),
])


class _StreamingNodeHandler(osmium.SimpleHandler):
    def __init__(self, writer: pq.ParquetWriter, wkb_factory, batch_size: int):
        super().__init__()
        self.writer = writer
        self.wkb = wkb_factory
        self.batch_size = batch_size
        self._clear()
        self.total = 0

    def _clear(self):
        self.ids: list[int] = []
        self.versions: list[int] = []
        self.timestamps: list[str] = []
        self.lons: list[float | None] = []
        self.lats: list[float | None] = []
        self.tags: list[list] = []
        self.geoms: list[bytes | None] = []

    def node(self, n):
        self.ids.append(n.id)
        self.versions.append(n.version)
        self.timestamps.append(n.timestamp.isoformat() if n.timestamp else None)
        self.tags.append([{"key": k, "value": v} for k, v in n.tags])
        if n.location.valid():
            self.lons.append(n.location.lon)
            self.lats.append(n.location.lat)
            try:
                self.geoms.append(bytes.fromhex(self.wkb.create_point(n)))
            except (osmium.InvalidLocationError, RuntimeError):
                self.geoms.append(None)
        else:
            self.lons.append(None)
            self.lats.append(None)
            self.geoms.append(None)
        if len(self.ids) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.ids: return
        batch = pa.record_batch([
            pa.array(self.ids, type=pa.int64()),
            pa.array(self.versions, type=pa.int32()),
            pa.array(self.timestamps, type=pa.string()),
            pa.array(self.lons, type=pa.float64()),
            pa.array(self.lats, type=pa.float64()),
            pa.array(self.tags, type=_TAGS_TYPE),
            pa.array(self.geoms, type=pa.binary()),
        ], schema=_NODE_SCHEMA)
        self.writer.write_batch(batch)
        self.total += len(self.ids)
        self._clear()
        if self.total % (self.batch_size * 10) == 0:
            print(f"    {self.total:,} nodes flushed")


class _StreamingWayHandler(osmium.SimpleHandler):
    def __init__(self, writer: pq.ParquetWriter, wkb_factory, batch_size: int):
        super().__init__()
        self.writer = writer
        self.wkb = wkb_factory
        self.batch_size = batch_size
        self._clear()
        self.total = 0
        self.resolved = 0

    def _clear(self):
        self.ids: list[int] = []
        self.versions: list[int] = []
        self.timestamps: list[str] = []
        self.refs: list[list[int]] = []
        self.tags: list[list] = []
        self.geoms: list[bytes | None] = []

    def way(self, w):
        self.ids.append(w.id)
        self.versions.append(w.version)
        self.timestamps.append(w.timestamp.isoformat() if w.timestamp else None)
        self.refs.append([n.ref for n in w.nodes])
        self.tags.append([{"key": k, "value": v} for k, v in w.tags])
        try:
            self.geoms.append(bytes.fromhex(self.wkb.create_linestring(w)))
            self.resolved += 1
        except (osmium.InvalidLocationError, RuntimeError):
            self.geoms.append(None)
        if len(self.ids) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.ids: return
        batch = pa.record_batch([
            pa.array(self.ids, type=pa.int64()),
            pa.array(self.versions, type=pa.int32()),
            pa.array(self.timestamps, type=pa.string()),
            pa.array(self.refs, type=pa.list_(pa.int64())),
            pa.array(self.tags, type=_TAGS_TYPE),
            pa.array(self.geoms, type=pa.binary()),
        ], schema=_WAY_SCHEMA)
        self.writer.write_batch(batch)
        self.total += len(self.ids)
        self._clear()
        if self.total % (self.batch_size * 10) == 0:
            print(f"    {self.total:,} ways flushed ({self.resolved:,} resolved)")


class _StreamingRelationHandler(osmium.SimpleHandler):
    def __init__(self, writer: pq.ParquetWriter, batch_size: int):
        super().__init__()
        self.writer = writer
        self.batch_size = batch_size
        self._clear()
        self.total = 0

    def _clear(self):
        self.ids: list[int] = []
        self.versions: list[int] = []
        self.timestamps: list[str] = []
        self.members: list[list] = []
        self.tags: list[list] = []

    def relation(self, r):
        self.ids.append(r.id)
        self.versions.append(r.version)
        self.timestamps.append(r.timestamp.isoformat() if r.timestamp else None)
        self.members.append([
            {"ref": m.ref, "role": m.role, "type": m.type} for m in r.members
        ])
        self.tags.append([{"key": k, "value": v} for k, v in r.tags])
        if len(self.ids) >= self.batch_size:
            self.flush()

    def flush(self):
        if not self.ids: return
        batch = pa.record_batch([
            pa.array(self.ids, type=pa.int64()),
            pa.array(self.versions, type=pa.int32()),
            pa.array(self.timestamps, type=pa.string()),
            pa.array(self.members, type=_MEMBERS_TYPE),
            pa.array(self.tags, type=_TAGS_TYPE),
        ], schema=_RELATION_SCHEMA)
        self.writer.write_batch(batch)
        self.total += len(self.ids)
        self._clear()


def osm_pbf_split(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *,
                   element_kind: str, batch_size: int = 100_000
                   ) -> list[tuple[str, pa.Table]]:
    """Streaming handler — writes directly to outputs/prepared/<slug>.parquet
    and returns an empty list so the normal write stage is a no-op."""
    if element_kind not in ("nodes", "ways", "relations"):
        raise ValueError(f"element_kind must be one of nodes/ways/relations, got {element_kind!r}")
    pbf_files = [p for p, _ in parsed
                 if str(p).lower().endswith(".pbf") or str(p).lower().endswith(".osm.pbf")]
    if not pbf_files:
        raise ValueError("osm_pbf_split: no .pbf files in extracted output")
    path = pbf_files[0]
    print(f"  parsing {path.name} ({path.stat().st_size / 1e9:.2f} GB)  kind={element_kind}")

    # Resolve output path by reading the manifest's write.output
    from ..spec import output_format_dir, spec_field
    out_path = output_format_dir(spec["slug"], "parquet") / spec_field(spec, "write.output", f"{spec['slug']}.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    compression = spec_field(spec, "write.compression", "zstd")

    if element_kind == "nodes":
        geo_meta = _wgs84_geo_metadata(["Point"])
        schema = _NODE_SCHEMA.with_metadata(geo_meta)
        wkb = osmium.geom.WKBFactory()
        with pq.ParquetWriter(out_path, schema, compression=compression) as writer:
            h = _StreamingNodeHandler(writer, wkb, batch_size)
            h.apply_file(str(path))
            h.flush()
            print(f"    total: {h.total:,} nodes")
    elif element_kind == "ways":
        geo_meta = _wgs84_geo_metadata(["LineString"])
        schema = _WAY_SCHEMA.with_metadata(geo_meta)
        wkb = osmium.geom.WKBFactory()
        with pq.ParquetWriter(out_path, schema, compression=compression) as writer:
            h = _StreamingWayHandler(writer, wkb, batch_size)
            # locations=True enables osmium's node-location index for way geometries
            h.apply_file(str(path), locations=True)
            h.flush()
            print(f"    total: {h.total:,} ways ({h.resolved:,} with resolved geometry)")
    else:  # relations
        schema = _RELATION_SCHEMA
        with pq.ParquetWriter(out_path, schema, compression=compression) as writer:
            h = _StreamingRelationHandler(writer, batch_size)
            h.apply_file(str(path))
            h.flush()
            print(f"    total: {h.total:,} relations")

    # Streaming handlers write the parquet themselves — tell the write stage
    # to skip.
    return []
