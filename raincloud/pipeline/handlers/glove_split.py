# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Read a single-dimension GloVe text file and emit a
`(word: string, vector: fixed_size_list<float32, dim>)` parquet.

Input file format (space-separated, no header):
    <word> <f1> <f2> ... <f_dim>
400,000 lines for the 6B variants.

The Stanford zip (glove.6B.zip) contains four files at 50/100/200/300 dims;
this handler reads exactly one, discriminated by the `dimension` param on
the DatasetSpec. Stanford is the canonical upstream — the Kaggle mirror was
dropped in an earlier license pass because Kaggle tagged it "other" even
though Stanford publishes under PDDL + Apache-2.0.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa


def glove_split(spec: dict, parsed: list[tuple[Path, pa.Table | None]], *, dimension: int
                ) -> list[tuple[str, pa.Table]]:
    if len(parsed) != 1:
        raise ValueError(f"glove_split expects exactly 1 input file, got {len(parsed)}")
    path, _ = parsed[0]
    print(f"  reading {path.name} ({dimension}-dim)")

    words: list[str] = []
    # Pre-allocate a flat float32 numpy buffer of shape (400k * dim,)
    # We don't know exact row count up-front, so grow by chunks.
    chunk_size = 50_000
    buf = np.empty(chunk_size * dimension, dtype=np.float32)
    row = 0

    # The vector is the last `dimension` fields and the token is everything
    # before them, so a token containing spaces (the larger GloVe variants have
    # some) stays whole. A line that does not end in `dimension` numbers fails
    # the build, naming the line.
    with open(path, "r", encoding="utf-8", newline="\n") as f:
        for number, line in enumerate(f, 1):
            line = line.rstrip("\n")
            parts = line.rsplit(" ", dimension)
            try:
                if len(parts) != dimension + 1 or not parts[0]:
                    raise ValueError(f"{len(parts) - 1} fields after the token")
                vector = [float(v) for v in parts[1:]]
            except ValueError as exc:
                raise ValueError(f"glove_split: {path.name} line {number} is not a token and "
                                 f"{dimension} numbers ({exc}): {line[:120]!r}") from None
            if row * dimension + dimension > buf.size:
                new_size = (buf.size + chunk_size * dimension) * 2
                new_buf = np.empty(new_size, dtype=np.float32)
                new_buf[:buf.size] = buf
                buf = new_buf
            words.append(parts[0])
            buf[row * dimension:(row + 1) * dimension] = vector
            row += 1

    print(f"  parsed {row:,} embeddings at {dimension} dims")
    values = pa.array(buf[:row * dimension], type=pa.float32())
    vector_array = pa.FixedSizeListArray.from_arrays(values, dimension)
    table = pa.table({"word": pa.array(words, type=pa.string()),
                      "vector": vector_array})
    return [(spec["slug"], table)]
