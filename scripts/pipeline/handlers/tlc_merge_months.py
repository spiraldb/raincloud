# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Merge 12 monthly NYC TLC parquets into a single annual parquet.

Yellow/Green taxis also get a synthesised `trip_duration_us: BIGINT` column
(microseconds between pickup and dropoff). See README "NYC TLC license note"
for why INTERVAL isn't a good fit — negative durations appear due to
clock drift, so we use a signed integer.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc


def tlc_merge_months(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *, kind: str, year: int
                     ) -> list[tuple[str, pa.Table]]:
    tables = [t for _, t in parsed if t is not None]
    if not tables:
        raise ValueError("tlc_merge_months: no parsed tables")
    # Concatenate (TLC monthly files sometimes add/remove columns across months)
    merged = pa.concat_tables(tables, promote_options="default")

    if kind in ("yellow", "green"):
        pu = _pickup_col(merged)
        do = _dropoff_col(merged)
        if pu is not None and do is not None:
            dur_us = pc.cast(
                pc.subtract(merged.column(do), merged.column(pu)),
                pa.duration("us"),
            ).cast(pa.int64())
            merged = merged.append_column("trip_duration_us", dur_us)
    return [(spec["slug"], merged)]


def _pickup_col(t: pa.Table) -> str | None:
    for n in ("tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"):
        if n in t.schema.names: return n
    return None


def _dropoff_col(t: pa.Table) -> str | None:
    for n in ("tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime", "dropOff_datetime"):
        if n in t.schema.names: return n
    return None
