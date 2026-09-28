# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Merge a year of monthly NYC TLC parquets into one canonical-Arrow BatchStream.

`kind` is the service ("yellow", "green", "fhv", "fhvhv"). `year` is not read:
the months merged are the recipe's `fetch.urls`. It stays a required param
because it is part of the recipe fingerprint, and it names the year for a reader
of sources.json. Yellow/Green taxis also get a
synthesised `trip_duration_us: BIGINT` column (microseconds between pickup and
dropoff). See README "NYC TLC license note" for why INTERVAL isn't a good fit —
negative durations appear due to clock drift, so we use a signed integer.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from ..batch_merge import as_stream, merge_streams
from ..batches import BatchStream, SourceBatch, batch_input


@batch_input("parquet")
def tlc_merge_months(spec: dict, parsed: list[tuple[Path, pa.Table | BatchStream | None]], *,
                     kind: str, year: int) -> list[tuple[str, BatchStream]]:
    streams = [as_stream(path, table) for path, table in parsed if table is not None]
    merged = merge_streams(streams, promotion="default")
    pickup = next((n for n in ("tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime")
                   if n in merged.schema.names), None)
    dropoff = next((n for n in ("tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime",
                                "dropOff_datetime") if n in merged.schema.names), None)
    if kind not in ("yellow", "green") or pickup is None or dropoff is None:
        return [(spec["slug"], merged)]
    schema = merged.schema.append(pa.field("trip_duration_us", pa.int64()))

    def batches():
        with merged.open() as reader:
            for item in reader:
                duration = pc.subtract(item.batch.column(dropoff), item.batch.column(pickup))
                duration = duration.cast(pa.duration("us")).cast(pa.int64())
                yield SourceBatch(item.source, item.row_offset,
                                  pa.RecordBatch.from_arrays([*item.batch.columns, duration], schema=schema))

    return [(spec["slug"], BatchStream(schema, batches))]
