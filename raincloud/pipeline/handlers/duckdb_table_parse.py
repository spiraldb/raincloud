# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stream one physical table from a downloaded DuckDB database, read-only.

Needs `parse.reader: "custom"`, so parse hands over the database path unread
(one input, with no table). Params:
    table  : the table to stream (must be a BASE TABLE, not a view)
    schema : its DuckDB schema, default "main"
"""
from __future__ import annotations

from pathlib import Path

from raincloud import duckdb_connect

from ..batches import BatchLimits, BatchStream, SourceBatch, batch_input, split_batch


def _quote(name: str) -> str:
    if not isinstance(name, str) or not name or '\x00' in name:
        raise ValueError('DuckDB table/schema names must be nonempty strings')
    return '"' + name.replace('"', '""') + '"'


@batch_input("custom")
def duckdb_table_parse(spec, parsed, *, table: str, schema: str = 'main'):
    if len(parsed) != 1 or parsed[0][1] is not None:
        raise ValueError('duckdb_table_parse expects one deferred database input')
    path = Path(parsed[0][0])
    if not path.is_file():
        raise ValueError(f'DuckDB input is not a file: {path}')
    relation = _quote(schema) + '.' + _quote(table)
    limits = BatchLimits.from_env()

    # Not a trust check on the download: the stat tuple only detects the file
    # being replaced between planning (schema read below) and emission, so a
    # multi-pass consumer never mixes two databases in one output.
    def identity():
        s = path.stat()
        return s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns

    planned = identity()

    def check_source():
        if identity() != planned:
            raise ValueError(f'DuckDB source changed after planning: {path}')

    def connect():
        # One scan thread preserves source scan order. External views/functions
        # are outside this reader's contract; only physical tables are accepted.
        con = duckdb_connect(path, extra_config={'access_mode': 'READ_ONLY', 'threads': 1})
        try:
            # Apply after resource configuration: DuckDB rejects configuring a
            # temp directory once external access has been disabled.
            con.execute('SET enable_external_access = false')
            return con
        except BaseException:
            con.close()
            raise

    with connect() as con:
        found = con.execute('SELECT table_type FROM information_schema.tables '
                            'WHERE table_schema = ? AND table_name = ?', [schema, table]).fetchone()
        if found != ('BASE TABLE',):
            raise ValueError(f'Expected a physical DuckDB table: {relation}')
        with con.execute(f'SELECT * FROM {relation} LIMIT 0').to_arrow_reader() as reader:
            arrow_schema = reader.schema
    check_source()

    def batches():
        check_source()
        with connect() as con:
            with con.execute(f'SELECT * FROM {relation}').to_arrow_reader(limits.rows) as reader:
                if not reader.schema.equals(arrow_schema, check_metadata=True):
                    raise ValueError(f'DuckDB schema changed after planning: {path}')
                offset = 0
                for decoded in reader:
                    for batch in split_batch(decoded, limits):
                        yield SourceBatch(path, offset, batch)
                        offset += batch.num_rows
                check_source()

    return [(spec['slug'], BatchStream(arrow_schema, batches))]
