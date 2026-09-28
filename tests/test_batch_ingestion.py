# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Real build/read boundaries for serial fixed-schema batch ingestion."""
from __future__ import annotations

import gzip
import weakref

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import raincloud
from raincloud._bundle import encode, make_bundle
from raincloud.catalogs import operation
from raincloud.config import use_config
from raincloud.pipeline import build, canonical, docs, parse
from raincloud.pipeline.batches import BatchLimits, BatchStream
from raincloud.pipeline.export.compare import values_equal
from raincloud.pipeline.handlers.openlibrary_parse import openlibrary_parse


def configuration(tmp_path, source, reader, handler, rows):
    recipe = {'slug': 'batch-test', 'short_name': 'Batch test', 'full_name': 'Batch test', 'fetch': {'type': 'http', 'urls': [source.as_uri()]},
              'extract': {'type': 'passthrough'}, 'parse': {'reader': reader},
              'transform': {'handler': handler}, 'expect': {'rows': rows},
              'export': {'formats': ['parquet', 'vortex']}}
    bundle = make_bundle(encode({'schema_version': 2, 'datasets': [recipe]}),
                         encode({'schema_version': 2, 'slugs': {}}), 'batch-test')
    catalog = tmp_path / 'catalog'
    catalog.mkdir()
    for name, data in bundle.files().items():
        (catalog / name).write_bytes(data)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(catalog),
        data_dir=tmp_path / 'data', raw_dir=tmp_path / 'raw', scratch_dir=tmp_path / 'scratch',
        cache_dir=tmp_path / 'unused-cache', catalog_dir=tmp_path / 'catalog-revisions', offline=False)
    return cfg, recipe


def assert_readers(cfg, expected):
    for fmt in ('arrow', 'parquet', 'vortex'):
        ds = raincloud.load('batch-test', format=fmt, config=cfg)
        with ds.batches(batch_size=2) as batches:
            parts = list(batches)
            got = pa.Table.from_batches(parts) if parts else ds.to_arrow()
        assert values_equal(got, expected) == (True, ''), fmt
    assert not cfg.cache_dir.exists()


@pytest.mark.parametrize('rows,byte_target', [(1, 4096), (3, 32), (4096, 16 * 1024 * 1024)])
def test_parquet_identity_partition_independence(tmp_path, monkeypatch, rows, byte_target):
    pytest.importorskip('vortex')
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', str(rows))
    monkeypatch.setenv('RAINCLOUD_BATCH_BYTES', str(byte_target))
    schema = pa.schema([pa.field('key', pa.int32(), metadata={b'role': b'identity'}),
                        ('text', pa.string()), ('nested', pa.list_(pa.int64())), ('float', pa.float64())],
                       metadata={b'owner': b'test'})
    expected = pa.Table.from_arrays([
        pa.array([7, 1, 4, 2, 9, 6, 8], pa.int32()),
        pa.array(['short', None, 'x' * 300, '', 'last', 'again', 'end']),
        pa.array([[1], None, [], [2, 3], [4], [], [5]], pa.list_(pa.int64())),
        pa.array([0.0, -0.0, 1.5, float('nan'), 2.5, 3.5, 4.5]),
    ], schema=schema)
    source = tmp_path / 'input.parquet'
    pq.write_table(expected, source, row_group_size=2)
    cfg, recipe = configuration(tmp_path, source, 'parquet', 'identity', 7)
    with use_config(cfg), operation(cfg):
        assert build.run_one(recipe, strict=True)
        # Regenerate all derived observations using the actual bounded outputs.
        assert docs.main([]) == 0
    assert_readers(cfg, expected)
    with pa.ipc.open_file(str(cfg.data_dir / 'v2/batch-test/arrow/batch-test.arrow.zstd')) as reader:
        assert reader.schema.equals(pq.read_schema(source), check_metadata=True)
        got = reader.read_all()
        np.testing.assert_array_equal(got['float'].to_numpy().view('uint64'),
                                      expected['float'].to_numpy().view('uint64'))
        for i in range(reader.num_record_batches):
            batch = reader.get_batch(i)
            assert batch.num_rows <= rows
            assert batch.nbytes <= byte_target or batch.num_rows == 1
    observations = list((cfg.data_dir / '.raincloud/observations').glob('*/handlers.md'))
    assert len(observations) == 1
    assert 'batches (parquet)' in observations[0].read_text()


@pytest.mark.parametrize('rows,byte_target', [(1, 4096), (100, 700), (4096, 16 * 1024 * 1024)])
@pytest.mark.parametrize('compressed', [False, True])
def test_openlibrary_partition_independence(tmp_path, monkeypatch, rows, byte_target, compressed):
    pytest.importorskip('vortex')
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', str(rows))
    monkeypatch.setenv('RAINCLOUD_BATCH_BYTES', str(byte_target))
    source = tmp_path / ('dump.txt.gz' if compressed else 'dump.txt')
    content = ('/type/work\t/works/A\t2\t2020-01-02T03:04:05.123456\t{"title":"héllo"}\n'
               '/type/work\t/works/B\t5\t2021-02-03T04:05:06\t{"raw":"' + 'x' * 800 + '"}\n'
               '/type/redirect\t/works/C\t7\t2022-03-04T05:06:07\t{"to":"A"}\n')
    source.write_bytes(gzip.compress(content.encode()) if compressed else content.encode())
    expected = pa.table({'key': ['/works/A', '/works/B', '/works/C'],
        'revision': pa.array([2, 5, 7], pa.int64()),
        'last_modified': pa.array(['2020-01-02T03:04:05.123456', '2021-02-03T04:05:06',
                                   '2022-03-04T05:06:07']).cast(pa.timestamp('us')),
        'type': ['/type/work', '/type/work', '/type/redirect'],
        'record': ['{"title":"héllo"}', '{"raw":"' + 'x' * 800 + '"}', '{"to":"A"}']})
    cfg, recipe = configuration(tmp_path, source, 'custom', 'openlibrary_parse', 3)
    with use_config(cfg), operation(cfg):
        assert build.run_one(recipe, strict=True)
    assert_readers(cfg, expected)
    stream = openlibrary_parse(recipe, [(source, None)])[0][1]
    offset = 0
    with stream.open() as batches:
        for item in batches:
            assert item.source == source and item.row_offset == offset
            assert item.batch.schema.equals(expected.schema, check_metadata=True)
            assert item.batch.num_rows <= rows
            assert item.batch.nbytes <= byte_target or item.batch.num_rows == 1
            offset += item.batch.num_rows
    assert offset == 3


@pytest.mark.parametrize('reader,handler', [('parquet', 'identity'), ('custom', 'openlibrary_parse')])
def test_empty_input_keeps_declared_schema(tmp_path, monkeypatch, reader, handler):
    pytest.importorskip('vortex')
    source = tmp_path / ('empty.parquet' if reader == 'parquet' else 'empty.txt')
    if reader == 'parquet':
        expected = pa.table({'x': pa.array([], pa.int32())})
        pq.write_table(expected, source)
    else:
        source.write_text('')
        stream = openlibrary_parse({'slug': 'batch-test'}, [(source, None)])[0][1]
        expected = pa.Table.from_batches([], schema=stream.schema)
    cfg, recipe = configuration(tmp_path, source, reader, handler, 0)
    with use_config(cfg), operation(cfg):
        assert build.run_one(recipe, strict=True)
    assert_readers(cfg, expected)


def test_late_source_failure_preserves_published_generation(tmp_path, monkeypatch):
    pytest.importorskip('vortex')
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', '1')
    source = tmp_path / 'input.parquet'
    expected = pa.table({'x': [1, 2, 3, 4]})
    pq.write_table(expected, source, row_group_size=1)
    cfg, recipe = configuration(tmp_path, source, 'parquet', 'identity', 4)
    with use_config(cfg), operation(cfg):
        assert build.run_one(recipe, strict=True)
        directory = cfg.data_dir / 'v2/batch-test'
        before = {p.relative_to(directory): p.read_bytes() for p in directory.rglob('*') if p.is_file()}
        real_reader = pq.ParquetFile
        reads = []

        class FailingFile:
            def __init__(self, *args, **kwargs):
                self.reader = real_reader(*args, **kwargs)
            def __enter__(self):
                return self
            def __exit__(self, *args):
                self.reader.close()
            @property
            def schema_arrow(self):
                return self.reader.schema_arrow
            def iter_batches(self, **kwargs):
                for i, batch in enumerate(self.reader.iter_batches(**kwargs)):
                    if i == 2:
                        raise OSError('injected late source read failure')
                    reads.append(i)
                    yield batch

        with monkeypatch.context() as patch:
            patch.setattr(pq, 'ParquetFile', FailingFile)
            assert not build.run_one(recipe, strict=True)
        assert reads == [0, 1]
        assert before == {p.relative_to(directory): p.read_bytes() for p in directory.rglob('*') if p.is_file()}
        assert build.run_one(recipe, strict=True)
    assert_readers(cfg, expected)


def test_pull_order_and_early_close_at_real_file_boundary(tmp_path, monkeypatch):
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', '2')
    source = tmp_path / 'input.parquet'
    pq.write_table(pa.table({'x': list(range(20))}), source, row_group_size=2)
    real_reader = pq.ParquetFile
    events = []

    class ObservedFile:
        def __init__(self, *args, **kwargs):
            self.reader = real_reader(*args, **kwargs)
        def __enter__(self):
            events.append('open')
            return self
        def __exit__(self, *args):
            self.reader.close()
            events.append('close')
        @property
        def schema_arrow(self):
            return self.reader.schema_arrow
        def iter_batches(self, **kwargs):
            for batch in self.reader.iter_batches(**kwargs):
                events.append('decode')
                yield batch

    monkeypatch.setattr(pq, 'ParquetFile', ObservedFile)
    stream = parse.parquet_batches(source)
    assert events == ['open', 'close']  # Footer only; no rows decoded during planning.
    with stream.open() as iterator:
        assert events == ['open', 'close']
        first = next(iterator)
        assert first.row_offset == 0 and first.batch.column(0).to_pylist() == [0, 1]
        assert events == ['open', 'close', 'open', 'decode']
        second = next(iterator)
        assert second.row_offset == 2 and second.batch.column(0).to_pylist() == [2, 3]
        assert events.count('decode') == 2
    assert events[-1] == 'close' and events.count('decode') == 2


def test_table_producers_released_before_export(tmp_path, monkeypatch):
    pytest.importorskip('vortex')
    source = tmp_path / 'input.csv'
    source.write_text('x\n1\n2\n3\n')
    cfg, recipe = configuration(tmp_path, source, 'csv', 'identity', 3)
    real_parse, real_export = build.parse, build.run_exporters
    references = []

    def observed_parse(*args, **kwargs):
        for path, table in real_parse(*args, **kwargs):
            references.append(weakref.ref(table))
            yield path, table

    def observed_export(*args, **kwargs):
        assert references and all(reference() is None for reference in references)
        return real_export(*args, **kwargs)

    monkeypatch.setattr(build, 'parse', observed_parse)
    monkeypatch.setattr(build, 'run_exporters', observed_export)
    with use_config(cfg), operation(cfg):
        assert build.run_one(recipe, strict=True)
    assert_readers(cfg, pa.table({'x': [1, 2, 3]}))


@pytest.mark.parametrize('value', [0, -1, True, 1.5])
def test_invalid_batch_limits(value):
    with pytest.raises(ValueError):
        BatchLimits(rows=value)
    with pytest.raises(ValueError):
        BatchLimits(target_bytes=value)


def test_late_batch_schema_change_closes_stream_and_preserves_file(tmp_path, monkeypatch):
    monkeypatch.setenv('RAINCLOUD_HOME', str(tmp_path))
    schema = pa.schema([('x', pa.int32())])
    prior = pa.table({'x': pa.array([9], pa.int32())})
    path = canonical.write_canonical({}, [('schema-test', prior)])[0]
    before = path.read_bytes()
    closed = []

    def batches():
        from raincloud.pipeline.batches import SourceBatch
        try:
            yield SourceBatch(path, 0, pa.record_batch([pa.array([1], pa.int32())], schema=schema))
            yield SourceBatch(path, 1, pa.record_batch([pa.array([2], pa.int64())], names=['x']))
        finally:
            closed.append(True)

    stream = BatchStream(schema, batches)
    with pytest.raises(ValueError, match='batch schema changed'):
        canonical.write_canonical({}, [('schema-test', stream)])
    assert closed == [True]
    assert path.read_bytes() == before
    assert not list(path.parent.glob('*.tmp'))


def test_openlibrary_decoded_memory_does_not_grow_with_source(tmp_path, monkeypatch):
    target = 256 * 1024
    monkeypatch.setenv('RAINCLOUD_BATCH_ROWS', '4096')
    monkeypatch.setenv('RAINCLOUD_BATCH_BYTES', str(target))
    source = tmp_path / 'dump.txt'
    records = 16_000
    line = '/type/work\t/works/A\t1\t2020-01-01T00:00:00\t' + 'x' * 2048 + '\n'
    with source.open('w') as output:
        for _ in range(records):
            output.write(line)
    baseline = pa.total_allocated_bytes()
    stream = openlibrary_parse({'slug': 'memory-test'}, [(source, None)])[0][1]
    peak = total = 0
    with stream.open() as batches:
        for item in batches:
            total += item.batch.num_rows
            peak = max(peak, pa.total_allocated_bytes() - baseline)
            del item
    assert total == records
    # Source is >32 MiB decoded; consume all of it without retaining its arrays.
    assert peak < 8 * target


@pytest.mark.parametrize('streaming', [False, True])
@pytest.mark.parametrize('duplicate', [False, True])
def test_canonical_names_and_metadata_match_for_tables_and_batches(tmp_path, monkeypatch, streaming, duplicate):
    from raincloud.pipeline.batches import SourceBatch

    monkeypatch.setenv('RAINCLOUD_HOME', str(tmp_path))
    names = ['x', 'x'] if duplicate else ['x', 'y']
    schema = pa.schema([pa.field(name, pa.int32(), metadata={b'ordinal': str(i).encode()})
                        for i, name in enumerate(names)], metadata={b'owner': b'preserve'})
    table = pa.Table.from_arrays([pa.array([1, 2], pa.int32()), pa.array([3, 4], pa.int32())], schema=schema)

    def batches():
        yield SourceBatch(tmp_path / 'input', 0, table.to_batches()[0])

    value = BatchStream(schema, batches) if streaming else table
    path = canonical.write_canonical({}, [('metadata-test', value)])[0]
    with pa.ipc.open_file(str(path)) as reader:
        result = reader.read_all()
    assert result.schema.metadata == schema.metadata
    for i in range(2):
        assert result.field(i).metadata == schema.field(i).metadata
        assert result.column(i).equals(table.column(i))
    assert len(set(result.column_names)) == 2
