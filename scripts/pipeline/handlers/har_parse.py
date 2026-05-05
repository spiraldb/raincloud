# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Parse UCI Human Activity Recognition Using Smartphones (dataset 240).

Upstream shape: outer zip contains `UCI HAR Dataset.zip` (inner zip) plus
a `UCI HAR Dataset.names` metadata file. The inner zip carries the actual
dataset:

    UCI HAR Dataset/
        activity_labels.txt    -- id  name (1..6)
        features.txt           -- 561 feature names
        train/
            X_train.txt        -- 7352 rows × 561 floats (whitespace-sep)
            y_train.txt        -- 7352 activity ids
            subject_train.txt  -- 7352 subject ids (1..30)
        test/
            X_test.txt, y_test.txt, subject_test.txt  (2947 rows)

This handler unpacks the inner zip into a scratch dir, reads each file via
whitespace-regex split, and emits one merged parquet:

    split: string ("train"|"test")
    subject_id: uint8
    activity_id: uint8
    activity_name: string
    feature_001..feature_561: float32 (with the original feature names from
        features.txt stored as Arrow field metadata so they're not lost
        — the 561 distinct names contain repeated / nested labels that
        wouldn't make unique top-level parquet columns).

Total rows: 10,299.
"""
from __future__ import annotations

import re
import zipfile
from pathlib import Path

import pyarrow as pa

from ..spec import REPO_ROOT


def _read_whitespace(text: str) -> list[list[str]]:
    rows = []
    for line in text.splitlines():
        line = line.strip()
        if not line: continue
        rows.append(re.split(r"\s+", line))
    return rows


def har_parse(spec: dict, parsed: list[tuple[Path, pa.Table | None]],
              **kwargs
              ) -> list[tuple[str, pa.Table]]:
    outer_inputs = [p for p, _ in parsed if p.suffix == ".zip"]
    if not outer_inputs:
        raise ValueError("har_parse: expected a nested `UCI HAR Dataset.zip` in input")
    inner_zip_path = outer_inputs[0]

    # Unpack the inner zip to a scratch dir for file-by-file reads.
    work = REPO_ROOT / "_workdir" / spec["slug"] / "inner"
    work.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(inner_zip_path) as z:
        z.extractall(work)

    base = work / "UCI HAR Dataset"
    if not base.exists():
        # zip may have slightly different casing; look for any dir
        dirs = [d for d in work.iterdir() if d.is_dir()]
        if not dirs: raise FileNotFoundError(f"nothing extracted under {work}")
        base = dirs[0]

    # activity_labels.txt:  "1 WALKING\n2 WALKING_UPSTAIRS\n..."
    act_map: dict[int, str] = {}
    for row in _read_whitespace((base / "activity_labels.txt").read_text()):
        act_map[int(row[0])] = row[1]

    # features.txt:  "1 tBodyAcc-mean()-X\n2 tBodyAcc-mean()-Y\n..."
    feature_names: list[str] = []
    for row in _read_whitespace((base / "features.txt").read_text()):
        feature_names.append(row[1] if len(row) > 1 else f"feat_{row[0]}")

    def read_split(split: str) -> pa.Table:
        d = base / split
        subjects = [int(r[0]) for r in _read_whitespace((d / f"subject_{split}.txt").read_text())]
        activities = [int(r[0]) for r in _read_whitespace((d / f"y_{split}.txt").read_text())]
        X_rows = _read_whitespace((d / f"X_{split}.txt").read_text())
        n = len(X_rows)
        if n != len(subjects) or n != len(activities):
            raise ValueError(f"{split}: row count mismatch X={n} y={len(activities)} subj={len(subjects)}")
        n_feat = len(X_rows[0])

        # Pivot X_rows into per-feature columns as float32.
        feat_cols = [[0.0] * n for _ in range(n_feat)]
        for i, row in enumerate(X_rows):
            for j, v in enumerate(row):
                feat_cols[j][i] = float(v) if v not in ("", "NA") else 0.0

        fields = [
            pa.field("split", pa.string()),
            pa.field("subject_id", pa.uint8()),
            pa.field("activity_id", pa.uint8()),
            pa.field("activity_name", pa.string()),
        ] + [pa.field(f"feature_{j+1:03d}", pa.float32(),
                      metadata={"original_name": feature_names[j]})
             for j in range(n_feat)]
        arrays = [
            pa.array([split] * n, type=pa.string()),
            pa.array(subjects, type=pa.uint8()),
            pa.array(activities, type=pa.uint8()),
            pa.array([act_map.get(a, str(a)) for a in activities], type=pa.string()),
        ] + [pa.array(feat_cols[j], type=pa.float32()) for j in range(n_feat)]
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    train = read_split("train")
    test = read_split("test")
    merged = pa.concat_tables([train, test], promote_options="default")
    print(f"  HAR: train={train.num_rows:,}  test={test.num_rows:,}  "
          f"merged={merged.num_rows:,} × {merged.num_columns} cols")
    return [(spec["slug"], merged)]
