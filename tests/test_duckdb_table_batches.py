# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Exercise downloaded DuckDB inputs across source, batch, and reader boundaries."""
import hashlib

import pyarrow as pa
import pytest

import raincloud
from raincloud import duckdb_connect
from raincloud._bundle import encode, make_bundle
from raincloud.catalogs import operation
from raincloud.config import use_config
from raincloud.pipeline import build
from raincloud.pipeline.export.compare import values_equal
from raincloud.pipeline.handlers.duckdb_table_parse import duckdb_table_parse


def database(tmp_path):
    path = tmp_path / 'source.duckdb'
    with duckdb_connect(path) as con:
        con.execute('CREATE TABLE "table.with.dot" AS SELECT i::BIGINT id, '
                    "CASE WHEN i=2 THEN NULL ELSE 'é-' || i END AS label, "
                    '(i * 1.25)::DECIMAL(12,2) price, '
                    "DATE '2020-01-01' + i::INTEGER AS day FROM range(7) t(i)")
        con.execute('CREATE TABLE unrelated AS SELECT 999 id')
        con.execute('CREATE VIEW untrusted AS SELECT * FROM unrelated')
    return path


@pytest.mark.parametrize('batch_rows', [1, 3])
def test_stream_replays_and_closes_without_modifying_source(tmp_path, monkeypatch, batch_rows):
    path = database(tmp_path)
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', str(batch_rows))
    stream = duckdb_table_parse({'slug': 'selected'}, [(path, None)], table='table.with.dot')[0][1]
    with stream.open() as reader:
        assert next(reader).batch.num_rows == batch_rows
    # Early exit must release the connection; a writable opener can now connect.
    with duckdb_connect(path):
        pass
    for _ in range(2):
        with stream.open() as reader:
            items = list(reader)
        assert [x.row_offset for x in items] == list(range(0, 7, batch_rows))
        actual = pa.Table.from_batches([x.batch for x in items])
        assert actual['id'].to_pylist() == list(range(7))
        assert actual['label'].to_pylist() == ['é-0', 'é-1', None, 'é-3', 'é-4', 'é-5', 'é-6']
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before


def test_rejects_views_missing_tables_and_changed_source(tmp_path):
    path = database(tmp_path)
    for table in ['untrusted', 'missing', 'unrelated"; DROP TABLE unrelated; --']:
        with pytest.raises(ValueError, match='physical DuckDB table'):
            duckdb_table_parse({'slug': 'x'}, [(path, None)], table=table)
    stream = duckdb_table_parse({'slug': 'x'}, [(path, None)], table='unrelated')[0][1]
    with duckdb_connect(path) as con:
        con.execute('INSERT INTO unrelated VALUES (1000)')
    with pytest.raises(ValueError, match='changed after planning'), stream.open() as reader:
        next(reader)
    missing = tmp_path / 'missing.duckdb'
    with pytest.raises(ValueError, match='not a file'):
        duckdb_table_parse({'slug': 'x'}, [(missing, None)], table='anything')
    assert not missing.exists()


def test_real_build_and_dataset_interfaces(tmp_path, monkeypatch):
    pytest.importorskip('vortex')
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', '2')
    source = database(tmp_path)
    recipe = {'slug': 'selected', 'short_name': 'Selected', 'full_name': 'Selected table',
              'fetch': {'type': 'http', 'urls': [source.as_uri()]},
              'extract': {'type': 'passthrough'}, 'parse': {'reader': 'custom'},
              'transform': {'handler': 'duckdb_table_parse', 'params': {'table': 'table.with.dot'}},
              'expect': {'rows': 7}, 'export': {'formats': ['parquet', 'vortex']}}
    bundle = make_bundle(encode({'schema_version': 2, 'datasets': [recipe]}),
                         encode({'schema_version': 2, 'slugs': {}}), 'duckdb-table-test')
    catalog = tmp_path / 'catalog'
    catalog.mkdir()
    for name, data in bundle.files().items():
        (catalog / name).write_bytes(data)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(catalog),
        data_dir=tmp_path / 'data', raw_dir=tmp_path / 'raw', scratch_dir=tmp_path / 'scratch',
        cache_dir=tmp_path / 'cache', catalog_dir=tmp_path / 'revisions', offline=False)
    with duckdb_connect(source, extra_config={'access_mode': 'READ_ONLY'}) as con:
        expected = con.execute('SELECT * FROM "table.with.dot"').to_arrow_table()
    with use_config(cfg), operation(cfg):
        assert build.run_one(recipe, strict=True, clean_workdir=True)
    for fmt in ('arrow', 'parquet', 'vortex'):
        actual = raincloud.load('selected', format=fmt, config=cfg).to_arrow()
        assert values_equal(actual, expected) == (True, ''), fmt
