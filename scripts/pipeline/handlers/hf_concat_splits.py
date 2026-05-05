# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Concat HF parquet shards into one parquet, optionally injecting a
synthetic `split: string` column derived from each shard's filename.

HF datasets ship as multi-shard parquets named
`<split>-NNNNN-of-NNNNN[-<hash>].parquet` under a config-name subdir.
Concatenating the shards naively collapses train/val/test into one table
with no way to recover which row came from which split — so the default
behaviour here is to infer the split from each filename and append it as a
top-level `split` column before delegating to `tighten_types` for the
standard integer-narrowing / binary-→-string pass.

Params (all optional):
    add_split_column : bool, default True
        Inject a synthetic `split` column. Set to False for sharded
        single-corpus datasets where the shard index isn't a meaningful
        partition (e.g. Cohere wikipedia embeddings — every shard is part
        of the same logical corpus).
    cast_to_fixed_size_list : list[str], default []
        Column names to cast from `list<T>` to `fixed_size_list<T, N>` if
        every non-null row has the same list length N. Useful for
        embedding columns that ship as variable-length lists upstream
        but are uniformly sized in practice (e.g. Cohere's `emb`).

If the upstream parquet already has a `split` column (hellaswag does — it's
a fold identifier from the source paper), the existing column is renamed to
`source_split` so neither value is lost.

Falls back to the file stem when a filename doesn't match the canonical
shard pattern.
"""
from __future__ import annotations

import re
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from .tighten_types import tighten_types

_SHARD_RE = re.compile(
    r"^([A-Za-z][A-Za-z0-9_]*?)-\d+-of-\d+(?:-[0-9a-f]+)?\.parquet$"
)


def _split_name(path: Path) -> str:
    m = _SHARD_RE.match(path.name)
    if m:
        return m.group(1)
    return path.stem


def _maybe_cast_fsl(table: pa.Table, names: list[str]) -> pa.Table:
    for name in names:
        if name not in table.column_names:
            continue
        col = table.column(name)
        ty = col.type
        if not (pa.types.is_list(ty) or pa.types.is_large_list(ty)):
            continue
        lengths = pc.list_value_length(col)
        non_null = pc.drop_null(lengths)
        if len(non_null) == 0:
            continue
        sizes = pc.unique(non_null).to_pylist()
        if len(sizes) != 1 or sizes[0] is None or sizes[0] <= 0:
            continue
        n = int(sizes[0])
        target = pa.list_(ty.value_type, n)
        try:
            new_col = col.cast(target)
        except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
            continue
        i = table.column_names.index(name)
        table = table.set_column(
            i, pa.field(name, target, nullable=table.field(i).nullable), new_col,
        )
    return table


def hf_concat_splits(
    spec: dict,
    parsed: list[tuple[Path, pa.Table | None]],
    *,
    add_split_column: bool = True,
    cast_to_fixed_size_list: list[str] | None = None,
) -> list[tuple[str, pa.Table]]:
    tagged: list[tuple[Path, pa.Table]] = []
    for path, table in parsed:
        if table is None:
            raise ValueError(
                "hf_concat_splits requires already-parsed tables "
                f"(got None for {path})"
            )
        if add_split_column:
            if "split" in table.column_names:
                i = table.column_names.index("split")
                table = table.set_column(
                    i,
                    pa.field("source_split", table.field(i).type, nullable=table.field(i).nullable),
                    table.column(i),
                )
            split = _split_name(path)
            col = pa.array([split] * len(table), type=pa.string())
            table = table.append_column("split", col)
        tagged.append((path, table))
    out = tighten_types(spec, tagged)
    if cast_to_fixed_size_list:
        slug, table = out[0]
        out = [(slug, _maybe_cast_fsl(table, list(cast_to_fixed_size_list)))]
    return out
