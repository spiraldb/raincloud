# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Helpers shared by test modules: sidecar binary lookup and canonical Arrow IPC files."""
from __future__ import annotations

import os
import shutil

import pyarrow as pa
import pytest

_ROLES = {"write": ("SIDECAR", "export"), "read": ("READER", "read")}


def find_sidecar(cell: str, role: str = "write") -> str | None:
    """The installed sidecar binary for `cell` (e.g. "parquet@rs"), or None.

    A writer is `$RAINCLOUD_SIDECAR_<FORMAT>_<IMPL>` or `raincloud-export-<format>-<impl>`
    on PATH; a reader (`role="read"`) is `$RAINCLOUD_READER_<FORMAT>_<IMPL>` or
    `raincloud-read-<format>-<impl>`.
    """
    fmt, impl = cell.split("@")
    env, verb = _ROLES[role]
    return os.environ.get(f"RAINCLOUD_{env}_{fmt.upper()}_{impl.upper()}") \
        or shutil.which(f"raincloud-{verb}-{fmt}-{impl}")


def sidecar(cell: str, role: str = "write") -> str:
    """`find_sidecar(cell, role)`, skipping the test when it is not installed."""
    binary = find_sidecar(cell, role)
    if not binary:
        pytest.skip(f"{cell} {'writer' if role == 'write' else 'reader'} not installed")
    return binary


def write_ipc(path, data, *, compression="zstd", max_chunksize=None):
    """Write `data` as an Arrow IPC file at `path` and return the path.

    `data` is a Table (written in chunks of at most `max_chunksize` rows) or a list
    of RecordBatches, written one by one exactly as given, empty ones included.
    """
    schema = data.schema if isinstance(data, pa.Table) else data[0].schema
    with pa.ipc.new_file(path, schema, options=pa.ipc.IpcWriteOptions(compression=compression)) as writer:
        if isinstance(data, pa.Table):
            writer.write_table(data, max_chunksize=max_chunksize)
        else:
            for batch in data:
                writer.write_batch(batch)
    return path


def write_canonical(slug, table):
    """`table` as `slug`'s canonical Arrow file under the configured store; returns its path."""
    from raincloud.pipeline import canonical

    (path,) = canonical.write_canonical({"slug": slug}, [(slug, table)])
    return path
