# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Wikipedia source semantics, exact selection and failure publication boundaries."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud import duckdb_connect
from raincloud.pipeline.canonical import write_canonical
from raincloud.pipeline.handlers.wikipedia_variant_parse import wikipedia_variant_parse
from raincloud.pipeline.spec import prepared_arrow, workdir_root


def read(slug):
    with pa.ipc.open_file(str(prepared_arrow(slug))) as reader:
        return reader.read_all()


@pytest.mark.parametrize('rows', [1, 4096])
def test_json_values_not_strings_and_sql_null_is_distinct(tmp_path, monkeypatch, rows):
    monkeypatch.setenv('RAINCLOUD_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', str(rows))
    path = tmp_path / "wiki's.parquet"
    values = ['[{"n":9007199254740993,"label":"café"}]', '{"nested":[1,2]}',
              'null', None, '[]', '"a string"', 'true', '12.5']
    pq.write_table(pa.table({'id': range(len(values)), 'sections': values, 'infoboxes': values}), path)
    wikipedia_variant_parse({'slug': 'wiki'}, [(path, None)])
    actual = read('wiki')
    expressions = [
        "[{'n':9007199254740993::BIGINT,'label':'café'}]",
        "{'nested':[1::BIGINT,2::BIGINT]}",
        "[]::BIGINT[]", "'a string'", 'true', '12.5::DOUBLE',
    ]
    with duckdb_connect() as con:
        expected = [con.execute(f'SELECT variant_to_parquet_variant(CAST({e} AS VARIANT))').fetchone()[0]
                    for e in expressions]
    for name in ['sections', 'infoboxes']:
        values = actual[name].to_pylist()
        assert [*values[:2], *values[4:]] == expected
        assert values[2] is not None and values[2]['value'] == b'\x00'  # JSON null encoded as a value.
        assert values[3] is None  # SQL null is a missing value.
        assert actual.schema.field(name).metadata[b'ARROW:extension:name'] == b'arrow.parquet.variant'
    assert not list((workdir_root() / 'wiki').glob('wikipedia-variant-*'))


def test_exact_selected_files_and_cross_shard_schema_union(tmp_path, monkeypatch):
    monkeypatch.setenv('RAINCLOUD_HOME', str(tmp_path / 'home'))
    folder = tmp_path / "lang=wiki's"
    folder.mkdir()
    first, second, unwanted = [folder / n for n in ['a.parquet', 'b.parquet', 'unselected.parquet']]
    pq.write_table(pa.table({'id': [2], 'meta': [{'first': 'a'}], 'sections': ['[]'], 'infoboxes': ['[]']}), first)
    pq.write_table(pa.table({'id': [1], 'meta': [{'late': 17}], 'sections': ['[]'], 'infoboxes': ['[]'], 'extra': ['x']}), second)
    unwanted.write_bytes(b'not parquet; must never be selected by a glob')
    wikipedia_variant_parse({'slug': 'wiki'}, [(second, None), (first, None)])
    actual = read('wiki')
    assert actual['id'].to_pylist() == [2, 1]  # Deterministic sorted shard order.
    assert actual['extra'].to_pylist() == [None, 'x']
    assert actual['meta'].to_pylist() == [{'first': 'a', 'late': None}, {'first': None, 'late': 17}]
    assert 'lang' not in actual.column_names  # Paths do not imply Hive partition columns.


def test_malformed_json_preserves_previous_canonical_and_cleans_scratch(tmp_path, monkeypatch):
    monkeypatch.setenv('RAINCLOUD_HOME', str(tmp_path / 'home'))
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', '1')
    old = write_canonical({}, [('wiki', pa.table({'old': [7]}))])[0]
    before = old.read_bytes()
    path = tmp_path / 'broken.parquet'
    pq.write_table(pa.table({'sections': ['[]'] * 8192 + ['{broken'], 'infoboxes': ['[]'] * 8193}), path)
    with pytest.raises(Exception, match='Malformed JSON|Invalid Input|Conversion Error'):
        wikipedia_variant_parse({'slug': 'wiki'}, [(path, None)])
    assert old.read_bytes() == before
    assert not list(old.parent.glob('*.tmp'))
    assert not list((workdir_root() / 'wiki').glob('wikipedia-variant-*'))
