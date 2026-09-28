# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Shard-wide type planning and serial ingestion through real prepared readers."""
from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import raincloud
from raincloud._bundle import encode, make_bundle
from raincloud.catalogs import operation
from raincloud.config import use_config
from raincloud.pipeline import build, canonical, parse
from raincloud.pipeline.batches import BatchStream
from raincloud.pipeline.export.compare import values_equal
from raincloud.pipeline.handlers.hf_concat_splits import hf_concat_splits
from raincloud.pipeline.handlers.tlc_merge_months import tlc_merge_months


def configuration(tmp_path, sources, handler, params, rows, reader='parquet'):
    recipe = {'slug': 'shards', 'short_name': 'Shards', 'full_name': 'Shards',
              'fetch': {'type': 'http', 'urls': [p.as_uri() for p in sources]},
              'extract': {'type': 'passthrough'}, 'parse': {'reader': reader},
              'transform': {'handler': handler, 'params': params}, 'expect': {'rows': rows},
              'export': {'formats': ['parquet', 'vortex']}}
    bundle = make_bundle(encode({'schema_version': 2, 'datasets': [recipe]}),
                         encode({'schema_version': 2, 'slugs': {}}), 'shard-test')
    catalog = tmp_path / 'catalog'
    catalog.mkdir()
    for name, data in bundle.files().items():
        (catalog / name).write_bytes(data)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(catalog),
        data_dir=tmp_path / 'data', raw_dir=tmp_path / 'raw', scratch_dir=tmp_path / 'scratch',
        cache_dir=tmp_path / 'unused-cache', catalog_dir=tmp_path / 'catalog-revisions', offline=False)
    return cfg, recipe


def files(tmp_path, tables):
    paths = [tmp_path / 'train-00000-of-00001.parquet', tmp_path / 'validation-00000-of-00001.parquet']
    for path, table in zip(paths, tables):
        pq.write_table(table, path, row_group_size=1)
    return paths[:len(tables)]


def assert_readers(cfg, expected):
    for fmt in ('arrow', 'parquet', 'vortex'):
        actual = raincloud.load('shards', format=fmt, config=cfg).to_arrow()
        assert values_equal(actual, expected) == (True, ''), fmt
    assert not cfg.cache_dir.exists()


def materialize(stream):
    with stream.open() as reader:
        return pa.Table.from_batches([item.batch for item in reader], schema=stream.schema)


@pytest.mark.parametrize('batch_rows', [1, 3, 4096])
@pytest.mark.parametrize('add_split', [False, True])
def test_hf_global_plan_and_order(tmp_path, monkeypatch, batch_rows, add_split):
    pytest.importorskip('vortex')
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', str(batch_rows))
    first = pa.table({'n': pa.array([1, 2], pa.int64()), 'mixed': pa.array([2, 3], pa.int32()),
        'blob': pa.array([b'hello', None], pa.binary()),
        'emb': pa.array([[1.0, 2.0], [7.0, 8.0]], pa.list_(pa.float32())), 'split': [0, 1],
        'payload': ['first', 'second']})
    second = pa.table({'payload': ['third', 'fourth'], 'split': [2, 3],
        'emb': pa.array([[3.0, 4.0], [5.0, 6.0]], pa.list_(pa.float32())),
        'blob': pa.array([b'world', b'last'], pa.binary()), 'mixed': [1.5, None],
        'n': pa.array([-1, 70000], pa.int32()), 'later': pa.array([5, 6], pa.int32())})
    sources = files(tmp_path, [first, second])
    cfg, recipe = configuration(tmp_path, sources, 'hf_concat_splits',
        {'add_split_column': add_split, 'cast_to_fixed_size_list': ['emb']}, 4)
    with use_config(cfg), operation(cfg):
        assert build.run_one(recipe, strict=True)
    expected = pa.table({'n': pa.array([1, 2, -1, 70000], pa.int32()), 'mixed': [2.0, 3.0, 1.5, None],
        'blob': ['hello', None, 'world', 'last'],
        'emb': pa.array([[1.0, 2.0], [7.0, 8.0], [3.0, 4.0], [5.0, 6.0]], pa.list_(pa.float32(), 2)),
        'source_split' if add_split else 'split': pa.array([0, 1, 2, 3], pa.uint8()),
        'payload': ['first', 'second', 'third', 'fourth']})
    if add_split:
        expected = expected.append_column('split', pa.array(['train', 'train', 'validation', 'validation']))
    expected = expected.append_column('later', pa.array([None, None, 5, 6], pa.uint8()))
    assert_readers(cfg, expected)
    schema = raincloud.load('shards', format='arrow', config=cfg).schema
    assert schema.field('n').type == pa.int32()  # Last shard controls the global width.
    assert schema.field('source_split' if add_split else 'split').type == pa.uint8()
    assert schema.field('emb').type == pa.list_(pa.float32(), 2)
    assert schema.field('later').nullable


@pytest.mark.parametrize('batch_rows', [1, 4096])
@pytest.mark.parametrize('kind,prefix', [('yellow', 'tpep'), ('green', 'lpep'), ('fhvhv', 'plain')])
def test_tlc_month_union_and_signed_durations(tmp_path, monkeypatch, batch_rows, kind, prefix):
    pytest.importorskip('vortex')
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', str(batch_rows))
    pu = 'pickup_datetime' if prefix == 'plain' else f'{prefix}_pickup_datetime'
    do = 'dropOff_datetime' if prefix == 'plain' else f'{prefix}_dropoff_datetime'
    first = pa.table({pu: pa.array([1000, 5000], pa.timestamp('us')),
                      do: pa.array([4000, 2000], pa.timestamp('us')), 'id': [7, 1]})
    second = pa.table({'id': [3, 2], do: pa.array([3000, None], pa.timestamp('us')),
                      pu: pa.array([None, 1000], pa.timestamp('us')),
                      'fee': pa.array([2, 3], pa.int32())})
    # Missing fee rows must be nullable even when the declaring month says required.
    second = second.set_column(3, pa.field('fee', pa.int32(), nullable=False), second['fee'])
    sources = files(tmp_path, [first, second])
    cfg, recipe = configuration(tmp_path, sources, 'tlc_merge_months', {'kind': kind, 'year': 2025}, 4)
    with use_config(cfg), operation(cfg):
        assert build.run_one(recipe, strict=True)
    expected = pa.table({pu: pa.array([1000, 5000, None, 1000], pa.timestamp('us')),
        do: pa.array([4000, 2000, 3000, None], pa.timestamp('us')), 'id': [7, 1, 3, 2],
        'fee': pa.array([None, None, 2, 3], pa.int32())})
    if kind in ('yellow', 'green'):
        expected = expected.append_column('trip_duration_us', pa.array([3000, -3000, None, None], pa.int64()))
    assert_readers(cfg, expected)


def test_plan_reads_only_required_columns_and_tlc_needs_no_row_scan(tmp_path, monkeypatch):
    source = files(tmp_path, [pa.table({'id': [1, 70000], 'text': ['large text', 'more text']})])[0]
    original = pq.ParquetFile
    projections = []

    class Observed:
        def __init__(self, *args, **kwargs): self.reader = original(*args, **kwargs)
        def __enter__(self): return self
        def __exit__(self, *args): self.reader.close()
        @property
        def schema_arrow(self): return self.reader.schema_arrow
        def iter_batches(self, **kwargs):
            projections.append(kwargs['columns'])
            yield from self.reader.iter_batches(**kwargs)

    monkeypatch.setattr(pq, 'ParquetFile', Observed)
    parsed = [(source, parse.parquet_batches(source))]
    hf = hf_concat_splits({'slug': 'x'}, parsed)[0][1]
    assert projections == [['id']]
    assert materialize(hf)['id'].to_pylist() == [1, 70000]
    assert projections[-1] == ['id', 'text']
    projections.clear()
    tlc = tlc_merge_months({'slug': 'x'}, parsed, kind='fhvhv', year=2025)[0][1]
    assert projections == []
    assert materialize(tlc)['id'].to_pylist() == [1, 70000]


def test_late_list_length_prevents_fixed_size_plan(tmp_path, monkeypatch):
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', '1')
    tables = [pa.table({'emb': [[1, 2], None]}), pa.table({'emb': [[3, 4], [5, 6, 7]]})]
    paths = files(tmp_path, tables)
    stream = hf_concat_splits({'slug': 'x'}, [(p, parse.parquet_batches(p)) for p in paths],
        add_split_column=False, cast_to_fixed_size_list=['emb'])[0][1]
    assert pa.types.is_list(stream.schema.field('emb').type)
    assert materialize(stream)['emb'].to_pylist() == [[1, 2], None, [3, 4], [5, 6, 7]]


@pytest.mark.parametrize('batch_rows', [1, 4096])
@pytest.mark.parametrize('handler', ['hf_concat_splits', 'tighten_types'])
def test_invalid_utf8_after_global_sample_preserves_previous_canonical(tmp_path, monkeypatch, batch_rows, handler):
    monkeypatch.setenv('RAINCLOUD_HOME', str(tmp_path / 'data'))
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', str(batch_rows))
    source = tmp_path / 'input.parquet'
    pq.write_table(pa.table({'blob': pa.array([b'ok'] * 4096 + [b'\xff'], pa.binary())}), source)
    from raincloud.pipeline.handlers import get
    stream = get(handler)({'slug': 'x'}, [(source, parse.parquet_batches(source))])[0][1]
    assert stream.schema.field('blob').type == pa.string()
    path = canonical.write_canonical({}, [('x', pa.table({'blob': ['prior']}))])[0]
    before = path.read_bytes()
    with pytest.raises(pa.ArrowInvalid):
        canonical.write_canonical({}, [('x', stream)])
    assert path.read_bytes() == before
    assert not list(path.parent.glob('*.tmp'))


def test_changed_parquet_generation_rejected_between_passes(tmp_path, monkeypatch):
    monkeypatch.setenv('RAINCLOUD_HOME', str(tmp_path / 'data'))
    source = files(tmp_path, [pa.table({'id': [1, 2]})])[0]
    stream = hf_concat_splits({'slug': 'x'}, [(source, parse.parquet_batches(source))],
                             add_split_column=False)[0][1]
    pq.write_table(pa.table({'id': [3, 4]}), source)
    with pytest.raises(ValueError, match='source changed after planning'):
        canonical.write_canonical({}, [('x', stream)])
    assert not list((tmp_path / 'data').rglob('*.arrow.zstd'))


def test_legacy_jsonl_parser_uses_same_global_handler_plan(tmp_path):
    pytest.importorskip('vortex')
    source = tmp_path / 'train.jsonl'
    source.write_text('\n'.join(json.dumps({'n': n, 'text': str(n)}) for n in [1, 70000]) + '\n')
    cfg, recipe = configuration(tmp_path, [source], 'hf_concat_splits', {}, 2, reader='jsonl')
    with use_config(cfg), operation(cfg):
        assert build.run_one(recipe, strict=True)
    assert_readers(cfg, pa.table({'n': pa.array([1, 70000], pa.uint32()),
                                  'text': ['1', '70000'], 'split': ['train', 'train']}))


def test_split_collision_is_rejected_before_data_read(tmp_path):
    path = files(tmp_path, [pa.table({'split': [1], 'source_split': [2]})])[0]
    with pytest.raises(ValueError, match='collide'):
        hf_concat_splits({'slug': 'x'}, [(path, parse.parquet_batches(path))])


def test_empty_and_null_only_shards_keep_types(tmp_path):
    table = pa.table({'id': pa.array([None], pa.int64()), 'blob': pa.array([None], pa.binary()),
                      'emb': pa.array([None], pa.list_(pa.int32()))})
    paths = files(tmp_path, [table.slice(0, 0), table])
    stream = hf_concat_splits({'slug': 'x'}, [(p, parse.parquet_batches(p)) for p in paths],
                             add_split_column=False, cast_to_fixed_size_list=['emb'])[0][1]
    assert isinstance(stream, BatchStream)
    actual = materialize(stream)
    assert actual.schema.equals(table.schema)
    assert actual.to_pylist() == table.to_pylist()


def test_nullable_fixed_list_plan_preserves_arrow_values(tmp_path, monkeypatch):
    # Parquet's PyArrow reader cannot restore nullable fixed-size lists (also
    # reproducible with plain pq.write_table/read_table in Arrow 24 and 25).
    # Preserve the existing HF type decision and test it at the canonical boundary.
    monkeypatch.setenv('RAINCLOUD_HOME', str(tmp_path / 'data'))
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', '1')
    path = files(tmp_path, [pa.table({'emb': [[1, 2], None, [3, 4]]})])[0]
    stream = hf_concat_splits({'slug': 'x'}, [(path, parse.parquet_batches(path))],
        add_split_column=False, cast_to_fixed_size_list=['emb'])[0][1]
    out = canonical.write_canonical({}, [('x', stream)])[0]
    with pa.memory_map(str(out), 'r') as source:
        actual = pa.ipc.open_file(source).read_all()
    assert actual.schema.field('emb').type == pa.list_(pa.int64(), 2)
    assert actual['emb'].to_pylist() == [[1, 2], None, [3, 4]]


def test_projected_plan_missing_column_preserves_shard_rows(tmp_path):
    paths = files(tmp_path, [pa.table({'text': ['a', 'b']}),
                            pa.table({'text': ['c'], 'id': [70000]})])
    stream = hf_concat_splits({'slug': 'x'}, [(p, parse.parquet_batches(p)) for p in paths],
                             add_split_column=False)[0][1]
    actual = materialize(stream)
    assert actual.to_pydict() == {'text': ['a', 'b', 'c'], 'id': [None, None, 70000]}
    assert actual.schema.field('id').type == pa.uint32()


@pytest.mark.parametrize('batch_rows', [1, 3, 4096])
def test_tighten_parquet_matches_table_contract_without_whole_input_reads(tmp_path, monkeypatch, batch_rows):
    from raincloud.pipeline.handlers.tighten_types import tighten_types

    pytest.importorskip('vortex')
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', str(batch_rows))
    tables = [pa.table({'id': pa.array([1, 2], pa.int64()),
                       'score': pa.array([0, None], pa.int64()),
                       'text': pa.array([b'first', None], pa.binary()),
                       'body': ['long first body', 'second body']}),
              pa.table({'body': ['third body', 'fourth body'],
                       'text': pa.array([b'third', b'fourth'], pa.binary()),
                       'score': pa.array([-129, 32768], pa.int64()),
                       'id': pa.array([70000, 2**40], pa.int64())})]
    paths = files(tmp_path, tables)
    expected = tighten_types({'slug': 'shards'}, list(zip(paths, tables)))[0][1]
    assert isinstance(expected, pa.Table)  # Existing table callers keep their contract.
    assert expected.schema.field('id').type == pa.uint64()
    assert expected.schema.field('score').type == pa.int32()
    cfg, recipe = configuration(tmp_path, paths, 'tighten_types', {}, 4)
    with use_config(cfg), operation(cfg):
        def forbidden(*args, **kwargs):
            raise AssertionError('whole-input Parquet read bypassed batching')
        with monkeypatch.context() as patch:
            patch.setattr(pq, 'read_table', forbidden)
            assert build.run_one(recipe, strict=True)
    assert_readers(cfg, expected)
    with pa.ipc.open_file(str(raincloud.load('shards', format='arrow', config=cfg).path())) as reader:
        assert reader.schema.equals(expected.schema, check_metadata=True)
        assert all(reader.get_batch(i).num_rows <= batch_rows for i in range(reader.num_record_batches))


def test_tighten_plan_projects_statistics_without_body(tmp_path, monkeypatch):
    from raincloud.pipeline.handlers.tighten_types import tighten_types

    path = files(tmp_path, [pa.table({'id': [1, 70000], 'body': ['body one', 'body two']})])[0]
    original = pq.ParquetFile
    projections = []

    class Observed:
        def __init__(self, *args, **kwargs): self.reader = original(*args, **kwargs)
        def __enter__(self): return self
        def __exit__(self, *args): self.reader.close()
        @property
        def schema_arrow(self): return self.reader.schema_arrow
        def iter_batches(self, **kwargs):
            projections.append(kwargs['columns'])
            yield from self.reader.iter_batches(**kwargs)

    monkeypatch.setattr(pq, 'ParquetFile', Observed)
    recipe = {'slug': 'x', 'parse': {'reader': 'parquet'}, 'transform': {'handler': 'tighten_types'}}
    parsed = list(parse.parse(recipe, [path]))
    assert isinstance(parsed[0][1], BatchStream)
    assert projections == []
    stream = tighten_types(recipe, parsed)[0][1]
    assert projections == [['id']]
    assert materialize(stream).to_pydict() == {'id': [1, 70000], 'body': ['body one', 'body two']}
    assert projections[-1] == ['id', 'body']
