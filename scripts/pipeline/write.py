# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 5 — write transformed Tables to outputs/v{schema_version}/<slug>/parquet/<slug>.parquet."""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .spec import display_path, output_format_dir, spec_field


def write(spec: dict, tables: list[tuple[str, pa.Table]]) -> list[Path]:
    compression = spec_field(spec, "write.compression", "zstd")
    row_group = spec_field(spec, "write.row_group_size_rows", 1 << 20)
    stats = spec_field(spec, "write.statistics", True)

    out_paths = []
    for out_slug, table in tables:
        format_dir = output_format_dir(out_slug, "parquet")
        format_dir.mkdir(parents=True, exist_ok=True)
        dest = format_dir / f"{out_slug}.parquet"
        print(f"[write] {display_path(dest)}  rows={table.num_rows:,}")
        pq.write_table(
            table, dest,
            compression=compression,
            row_group_size=row_group,
            write_statistics=stats,
        )
        out_paths.append(dest)
    return out_paths
