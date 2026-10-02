# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Write the comparison cases every lane's comparator is held to.

The comparators (Python `raincloud/pipeline/export/compare.py`, Rust
`sidecars/rust/src/lib.rs`, JVM `LogicalCompare`) decide when a file read back
holds the canonical's data. Where that decision turns on representation rather
than data -- a zone label, a union spelling of nullability, an integer held as a
scale-0 decimal -- the rule is declared here once, as pairs of Arrow IPC files
and the verdict each pair must get, and every lane's tests read the same files.

`verdict` is `equal` or `differ`. `gap: true` lets a comparator that cannot
represent the type report a gap instead (the JVM comparator has no decimals);
no comparator may ever give the opposite verdict.

Run from the repository root to regenerate `cases.json` and `*.arrow`:
    python sidecars/compare_cases/generate.py
"""
from __future__ import annotations

import datetime
import decimal
import json
from pathlib import Path

import pyarrow as pa

HERE = Path(__file__).resolve().parent
UTC = datetime.timezone.utc


def _union(values, member: pa.DataType, *, mode: str = "sparse", null_first: bool = True) -> pa.Array:
    """A union of exactly `null` and `member`: a None selects the null member."""
    null_code, member_code = (0, 1) if null_first else (1, 0)
    codes = pa.array([null_code if v is None else member_code for v in values], pa.int8())
    member_values = pa.array(values, member)
    if mode == "sparse":
        children = [pa.nulls(len(values)), member_values]
        if not null_first:
            children.reverse()
        return pa.UnionArray.from_sparse(codes, children, ["null", "value"] if null_first else ["value", "null"],
                                         [0, 1])
    present = [v for v in values if v is not None]
    offsets, n_null, n_member = [], 0, 0
    for v in values:
        if v is None:
            offsets.append(n_null)
            n_null += 1
        else:
            offsets.append(n_member)
            n_member += 1
    children = [pa.nulls(n_null), pa.array(present, member)]
    if not null_first:
        children.reverse()
    return pa.UnionArray.from_dense(codes, pa.array(offsets, pa.int32()), children,
                                    ["null", "value"] if null_first else ["value", "null"], [0, 1])


def _ts(values, tz):
    return pa.array([None if v is None else datetime.datetime(2024, 1, 1, v, tzinfo=UTC) for v in values],
                    pa.timestamp("us", tz))


def cases():
    ints = [1, None, 3]
    yield "union-null-int-sparse", "equal", _union(ints, pa.int32()), pa.array(ints, pa.int32())
    yield "union-null-int-dense", "equal", _union(ints, pa.int32(), mode="dense"), pa.array(ints, pa.int32())
    yield "union-int-null-order", "equal", _union(ints, pa.int32(), null_first=False), pa.array(ints, pa.int32())
    yield "union-null-string", "equal", _union(["a", None, "c"], pa.string()), pa.array(["a", None, "c"])
    yield "union-null-int-values-differ", "differ", _union([1, None, 4], pa.int32()), pa.array(ints, pa.int32())
    yield "union-null-int-nulls-differ", "differ", _union([1, 2, 3], pa.int32()), pa.array(ints, pa.int32())
    nested = _union(ints, pa.int64())
    yield ("union-inside-struct", "equal",
           pa.StructArray.from_arrays([nested], ["x"]), pa.StructArray.from_arrays([pa.array(ints, pa.int64())], ["x"]))
    two = pa.UnionArray.from_sparse(pa.array([0, 1, 0], pa.int8()),
                                    [pa.array([1, 2, 3], pa.int32()), pa.array(["a", "b", "c"])], ["i", "s"], [0, 1])
    yield "union-two-members-is-not-nullable", "differ", two, pa.array([1, 2, 3], pa.int32())

    hours = [1, None, 3]
    yield "tz-utc-vs-offset", "equal", _ts(hours, "+00:00"), _ts(hours, "UTC")
    yield "tz-other-zone-same-instants", "equal", _ts(hours, "America/New_York"), _ts(hours, "UTC")
    yield "tz-etc-utc", "equal", _ts(hours, "Etc/UTC"), _ts(hours, "UTC")
    yield "tz-same-zone-values-differ", "differ", _ts([1, None, 4], "+00:00"), _ts(hours, "UTC")
    yield "tz-naive-vs-zoned", "differ", _ts(hours, None), _ts(hours, "UTC")

    big = [0, None, 2**64 - 1]
    as_decimal = pa.array([None if v is None else decimal.Decimal(v) for v in big], pa.decimal128(20, 0))
    yield "decimal-scale0-holds-uint64", "equal", as_decimal, pa.array(big, pa.uint64()), True
    yield ("decimal-scale0-values-differ", "differ",
           pa.array([decimal.Decimal(1), None, decimal.Decimal(2)], pa.decimal128(20, 0)),
           pa.array([0, None, 2], pa.uint64()), True)
    yield ("decimal-scale2-is-not-an-integer", "differ",
           pa.array([decimal.Decimal("0.00"), None, decimal.Decimal("2.00")], pa.decimal128(20, 2)),
           pa.array([0, None, 2], pa.int64()), True)

    yield "int-width", "equal", pa.array(ints, pa.int64()), pa.array(ints, pa.int8())
    yield "string-view", "equal", pa.array(["a", None], pa.string_view()), pa.array(["a", None])


def _write(path: Path, array: pa.Array) -> None:
    table = pa.table({"c": array})
    with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
        writer.write_table(table)


def main():
    listed = []
    for name, verdict, got, expected, *gap in cases():
        _write(HERE / f"{name}.got.arrow", got)
        _write(HERE / f"{name}.expected.arrow", expected)
        listed.append({"name": name, "verdict": verdict, **({"gap": True} if gap and gap[0] else {})})
    (HERE / "cases.json").write_text(json.dumps({
        "_about": "Comparison rules every lane shares; see generate.py. Each case is <name>.got.arrow "
                  "(column `c`, what a reader returned) and <name>.expected.arrow (the canonical).",
        "cases": listed}, indent=1) + "\n")


if __name__ == "__main__":
    main()
