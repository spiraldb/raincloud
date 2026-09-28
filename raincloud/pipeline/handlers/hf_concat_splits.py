# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Concatenate Hugging Face Parquet shards into one canonical-Arrow BatchStream.

Optionally injects a synthetic `split: string` column derived from each shard's
filename.

HF datasets ship as multi-shard parquets named
`<split>-NNNNN-of-NNNNN[-<hash>].parquet` under a config-name subdir.
Concatenating the shards naively collapses train/val/test into one table
with no way to recover which row came from which split — so the default
behaviour here is to infer the split from each filename and append it as a
top-level `split` column. A projected statistics pass plans dataset-wide
integer narrowing, binary-to-string conversion and fixed list lengths before
serial batch emission. Parquet payload columns are read only during emission.

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

from ..batch_merge import as_stream, merge_streams
from ..batch_types import tighten_stream
from ..batches import BatchStream, batch_input

_SHARD_RE = re.compile(
    r"^([A-Za-z][A-Za-z0-9_]*?)-\d+-of-\d+(?:-[0-9a-f]+)?\.parquet$"
)


def _split_name(path: Path) -> str:
    m = _SHARD_RE.match(path.name)
    if m:
        return m.group(1)
    return path.stem


@batch_input("parquet")
def hf_concat_splits(
    spec: dict,
    parsed: list[tuple[Path, pa.Table | BatchStream | None]],
    *,
    add_split_column: bool = True,
    cast_to_fixed_size_list: list[str] | None = None,
) -> list[tuple[str, BatchStream]]:
    streams = [as_stream(path, table) for path, table in parsed]
    constants = ([{"split": pa.scalar(_split_name(path), type=pa.string())} for path, _ in parsed]
                 if add_split_column else None)
    merged = merge_streams(streams, promotion="permissive",
                           renames={"split": "source_split"} if add_split_column else None,
                           constants=constants)
    return [(spec["slug"], tighten_stream(merged, cast_to_fixed_size_list or []))]
