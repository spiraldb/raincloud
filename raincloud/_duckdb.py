# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The one way raincloud code, tests and examples open DuckDB.

DuckDB is optional for the loader: importing raincloud never imports it, and
`duckdb_connect` says how to install it when it is missing.
"""
from __future__ import annotations

import os
from pathlib import Path

from .exceptions import MissingDependency


def duckdb_connect(database: str | Path | None = None, *,
                   extra_config: dict | None = None):
    """Open a DuckDB connection with raincloud's environment-driven defaults.

    Env vars (all optional):
      RAINCLOUD_DUCKDB_MEMORY_LIMIT — e.g. "8GB", "512MB". Forwarded to
          DuckDB's `memory_limit` setting; DuckDB spills to disk once the
          working set exceeds this. If unset, DuckDB uses its default
          (~80% of system RAM), which can be a problem on shared hosts or
          CI runners.
      RAINCLOUD_DUCKDB_THREADS — int. Caps DuckDB's thread pool.
      RAINCLOUD_DUCKDB_TEMP_DIRECTORY — path. Where DuckDB writes spill
          files. Default is the system tempdir; override when the system
          tempdir doesn't have room for the working set (e.g. point at a
          larger volume).

    When `database` is a path, the connection is opened in persistent mode
    with `storage_compatibility_version=v1.5.0` (required for VARIANT
    columns in persistent DuckDB databases).

    `extra_config` merges on top of the env-derived config; explicit keys win.
    A missing duckdb raises MissingDependency saying how to install it.
    """
    try:
        import duckdb
    except ModuleNotFoundError as exc:
        if exc.name != "duckdb":
            raise
        from ._extras import extra_for
        extra = extra_for("duckdb")
        within = f" (`raincloud[{extra}]` includes it)" if extra else ""
        raise MissingDependency(f"duckdb_connect() needs duckdb; install `duckdb`{within}") from exc
    cfg: dict = {}
    if database is not None:
        cfg["storage_compatibility_version"] = "v1.5.0"
    mem = os.environ.get("RAINCLOUD_DUCKDB_MEMORY_LIMIT")
    if mem: cfg["memory_limit"] = mem
    thr = os.environ.get("RAINCLOUD_DUCKDB_THREADS")
    if thr: cfg["threads"] = thr
    tmp = os.environ.get("RAINCLOUD_DUCKDB_TEMP_DIRECTORY")
    if tmp: cfg["temp_directory"] = tmp
    if extra_config: cfg.update(extra_config)
    if database is None:
        return duckdb.connect(config=cfg) if cfg else duckdb.connect()
    return duckdb.connect(str(database), config=cfg)
