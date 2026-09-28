# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""A maintenance publisher that fails leaves the previous artifact in place."""
import json
import os
from dataclasses import replace
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import raincloud
from raincloud._bundle import encode, make_bundle
from raincloud._cache import sha256_file
from raincloud._locking import atomic_write
from raincloud.catalogs import operation
from raincloud.pipeline import compliance, convert, profile, promote_profiles, records
from raincloud.pipeline.export import get_exporter
from raincloud.pipeline.lifecycle import entry_for, operation_lock
from raincloud.pipeline.spec import (
    prepared_arrow,
    prepared_parquet,
    prepared_vortex,
)

KINDS = ['convert-v2', 'convert-v1', 'compliance', 'profile', 'promote']


@pytest.fixture(params=KINDS)
def publisher(request, tmp_path, monkeypatch):
    kind = request.param
    version = 1 if kind == 'convert-v1' else 2
    # schema_version 2 declares formats in export.formats only; v1 reads convert.vortex.
    spec = ({'slug': 'tiny', 'convert': {'vortex': True}} if version == 1
            else {'slug': 'tiny', 'export': {'formats': ['parquet', 'vortex']}})
    bundle = make_bundle(encode({'schema_version': version, 'datasets': [spec]}),
                         encode({'schema_version': version, 'slugs': {}}), 'maintenance-recovery')
    directory = tmp_path / 'catalog'
    directory.mkdir()
    for name, raw in bundle.files().items():
        (directory / name).write_bytes(raw)
    config = raincloud.resolve_config(no_config=True, catalog=str(directory),
        data_dir=tmp_path / 'data', raw_dir=tmp_path / 'raw', scratch_dir=tmp_path / 'scratch',
        catalog_dir=tmp_path / 'catalogs', offline=True)
    with operation(config), operation_lock(resources=True):
        entry = entry_for(spec)
        table = pa.table({'x': [1], 'url': [None]})
        canonical = prepared_arrow('tiny')
        canonical.parent.mkdir(parents=True)
        with pa.ipc.new_file(canonical, table.schema) as writer:
            writer.write_table(table)
        parquet = prepared_parquet('tiny')
        parquet.parent.mkdir(parents=True)
        pq.write_table(table, parquet)
        built_profile = profile._profile_path('tiny')
        atomic_write(built_profile, b'{"generation": 1}')
        if kind == 'convert-v2':
            # v2 convert exports only a canonical this install recorded building.
            records.record_build({'tiny': {'arrow': (sha256_file(canonical), canonical.stat().st_size)}})
        if kind.startswith('convert'):
            dest = prepared_vortex('tiny')
            def call():
                # The tests retry identically after a failure, which only
                # --retry-errors re-attempts once it is recorded.
                return convert.convert(spec, retry_errors=True)
            writer, attr = ((type(get_exporter('vortex@py')), 'export') if kind == 'convert-v2'
                            else (convert, '_convert_one'))
        elif kind == 'compliance':
            dest = compliance.compliance_path(get_exporter('parquet@py'), 'tiny')
            def call():
                return compliance._run_write_cells(spec, canonical, ['parquet@py'], reencode=True)
            writer, attr = type(get_exporter('parquet@py')), 'export'
        elif kind == 'profile':
            dest = built_profile
            monkeypatch.setattr(profile, 'profile_slug', lambda **kw: {'generation': 2})
            def call():
                return profile.main(['tiny', '--force', '--no-promote'])
            writer, attr = profile, 'atomic_write'
        else:  # promote
            dest = promote_profiles.profile_observations_dir() / 'tiny.json'
            def call():
                return promote_profiles.promote(['tiny'])
            writer, attr = promote_profiles, 'atomic_write'
        yield SimpleNamespace(kind=kind, dest=dest, call=call, writer=writer, attr=attr,
                              entry=entry, canonical=canonical)


def prepare(publisher, prior):
    dest = publisher.dest
    if not prior:
        dest.unlink(missing_ok=True)
        return None
    # Exercise rollback of a real successful artifact.
    publisher.call()
    if publisher.kind == 'promote':
        atomic_write(profile._profile_path('tiny'), b'{"generation": 2}')
    os.utime(dest, ns=(1, 1))  # Force mtime-based converters to regenerate.
    return dest.read_bytes(), dest.stat().st_mtime_ns


def assert_restored(dest, before):
    if before is None:
        assert not dest.exists()
    else:
        assert (dest.read_bytes(), dest.stat().st_mtime_ns) == before
    assert not list(dest.parent.glob('*.rollback'))
    assert not list(dest.parent.glob('*.vortex.tmp'))


@pytest.mark.parametrize('prior', [False, True])
def test_partial_writer_rolls_back_and_retries(publisher, monkeypatch, prior):
    if publisher.kind == 'promote':
        pytest.skip('promotion is one atomic copy; a completed copy needs no rollback')
    if publisher.kind == 'profile':
        pytest.skip('profile.json is one atomic write; see test_profile_write_is_never_partial')
    before = prepare(publisher, prior)
    original = getattr(publisher.writer, publisher.attr)
    def broken(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError('writer failed after writing')
    with monkeypatch.context() as fault:
        fault.setattr(publisher.writer, publisher.attr, broken)
        if publisher.kind == 'compliance':
            produced, _ = publisher.call()
            assert produced[0].sha256 == ''
            assert produced[0].compliance.roundtrip is False
        else:
            with pytest.raises(RuntimeError, match='writer failed'):
                publisher.call()
        assert_restored(publisher.dest, before)
    publisher.call()
    assert publisher.dest.is_file()


@pytest.mark.parametrize('prior', [False, True])
def test_profile_write_is_never_partial(publisher, monkeypatch, prior):
    """profile.json is written with one atomic_write, not a Publication: a
    failure raised after the write leaves the COMPLETE new profile (nothing to
    roll back), and one raised inside it, before the rename, leaves the
    previous file. Neither leaves a partial profile or a temp file behind."""
    if publisher.kind != 'profile':
        pytest.skip('profile only')
    dest = publisher.dest
    leftovers = lambda: list(dest.parent.glob(f'.{dest.name}.*'))  # noqa: E731
    original = getattr(publisher.writer, publisher.attr)

    prepare(publisher, prior)
    def fails_after(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError('writer failed after writing')
    with monkeypatch.context() as fault:
        fault.setattr(publisher.writer, publisher.attr, fails_after)
        with pytest.raises(RuntimeError, match='writer failed'):
            publisher.call()
    assert json.loads(dest.read_text()) == {'generation': 2}
    assert not leftovers()

    before = prepare(publisher, prior)
    def fails_inside(fd):
        raise OSError('disk failed mid-write')
    with monkeypatch.context() as fault:
        fault.setattr(os, 'fsync', fails_inside)
        with pytest.raises(OSError, match='mid-write'):
            publisher.call()
    if before is None:
        assert not dest.exists()
    else:
        assert (dest.read_bytes(), dest.stat().st_mtime_ns) == before
    assert not leftovers()

    publisher.call()
    assert json.loads(dest.read_text()) == {'generation': 2}


@pytest.mark.parametrize('prior', [False, True])
def test_measured_failure_preserved_without_publishing(publisher, monkeypatch, prior):
    if publisher.kind not in {'compliance', 'convert-v2'}:
        pytest.skip('these publishers have no exporter measurement')
    before = prepare(publisher, prior)
    original = getattr(publisher.writer, publisher.attr)
    def measured_failure(*args, **kwargs):
        result = original(*args, **kwargs)
        return replace(result, compliance=replace(result.compliance, roundtrip=False, note='measured mismatch'))
    with monkeypatch.context() as fault:
        fault.setattr(publisher.writer, publisher.attr, measured_failure)
        if publisher.kind == 'compliance':
            produced, _ = publisher.call()
            assert produced[0].compliance.note == 'measured mismatch'
            assert produced[0].compliance.roundtrip is False
            assert compliance._run_readers(produced, publisher.canonical, ['parquet@py']) == []
        else:
            with pytest.raises(RuntimeError, match='export failed'):
                publisher.call()
        assert_restored(publisher.dest, before)
    publisher.call()
    assert publisher.dest.is_file()
