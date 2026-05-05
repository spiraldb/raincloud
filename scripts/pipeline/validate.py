# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 6 — compare actual parquet to the `expect` block."""
from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow.parquet as pq

from .spec import spec_field


def validate(spec: dict, written: list[Path], *, strict: bool = True) -> list[dict]:
    results = []
    for p in written:
        md = pq.ParquetFile(p).metadata
        expected_rows = spec_field(spec, "expect.rows")
        actual_rows = md.num_rows
        ok_rows = expected_rows is None or actual_rows == expected_rows
        schema_hash = _schema_hash(pq.ParquetFile(p).schema_arrow)
        expected_hash = spec_field(spec, "expect.schema_hash")
        ok_schema = expected_hash is None or schema_hash == expected_hash
        result = {
            "path": str(p),
            "rows_ok": ok_rows,
            "rows_expected": expected_rows, "rows_actual": actual_rows,
            "schema_ok": ok_schema,
            "schema_hash": schema_hash,
        }
        results.append(result)
        if strict and not (ok_rows and ok_schema):
            raise AssertionError(f"validation failed for {p}: {result}")
        print(f"[validate] {p.name}  rows={actual_rows:,} "
              f"(expected={expected_rows}) schema_hash={schema_hash[:12]}")
    return results


def _schema_hash(schema) -> str:
    # Canonical form: sorted field_name:arrow_type
    items = sorted(f"{f.name}:{f.type}" for f in schema)
    return hashlib.sha256("|".join(items).encode()).hexdigest()
