# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 6 — compare actual parquet to the `expect` block.

Drift is treated as a signal, not an error: a row count or schema_hash
mismatch emits a `[WARN]` line but does NOT abort the build. The intent
is that users who ran `build <slug>` already opted into "download
whatever's currently upstream"; brittle equality checks against a
manifest captured weeks ago shouldn't turn an HF Arrow-conversion bump
into a failed build. Pass `strict=True` (CLI `--strict`) to opt back
into hard failures — useful for CI / pre-release gates.

Schema-hash comparison is prefix-aware: `expect.schema_hash` may be the
full 64-char SHA-256 or a short prefix (the manifest convention is 12
chars, matching the `schema_hash=` line printed by this stage). Equal-
length values use strict equality; a shorter expected acts as a prefix
match on the computed hash.
"""
from __future__ import annotations

import hashlib
import sys
from pathlib import Path

import pyarrow.parquet as pq

from .spec import spec_field


def _schema_hash_matches(actual: str, expected: str | None) -> bool:
    """Strict equality when lengths match; prefix match when expected is
    shorter. Manifest entries are typically the 12-char short form."""
    if expected is None:
        return True
    if len(expected) == len(actual):
        return actual == expected
    if len(expected) < len(actual):
        return actual.startswith(expected)
    return False


def validate(spec: dict, written: list[Path], *, strict: bool = False) -> list[dict]:
    results = []
    for p in written:
        md = pq.ParquetFile(p).metadata
        expected_rows = spec_field(spec, "expect.rows")
        actual_rows = md.num_rows
        ok_rows = expected_rows is None or actual_rows == expected_rows
        schema_hash = _schema_hash(pq.ParquetFile(p).schema_arrow)
        expected_hash = spec_field(spec, "expect.schema_hash")
        ok_schema = _schema_hash_matches(schema_hash, expected_hash)
        result = {
            "path": str(p),
            "rows_ok": ok_rows,
            "rows_expected": expected_rows, "rows_actual": actual_rows,
            "schema_ok": ok_schema,
            "schema_hash": schema_hash,
        }
        results.append(result)
        if not (ok_rows and ok_schema):
            if not ok_rows:
                print(f"[WARN] {p.name}: rows drift "
                      f"(expected={expected_rows:,} actual={actual_rows:,})",
                      file=sys.stderr)
            if not ok_schema:
                exp_disp = expected_hash if expected_hash else "—"
                print(f"[WARN] {p.name}: schema_hash drift "
                      f"(expected={exp_disp} actual={schema_hash[:12]})",
                      file=sys.stderr)
            if strict:
                raise AssertionError(f"validation failed for {p}: {result}")
        print(f"[validate] {p.name}  rows={actual_rows:,} "
              f"(expected={expected_rows}) schema_hash={schema_hash[:12]}")
    return results


def _schema_hash(schema) -> str:
    # Canonical form: sorted field_name:arrow_type
    items = sorted(f"{f.name}:{f.type}" for f in schema)
    return hashlib.sha256("|".join(items).encode()).hexdigest()
