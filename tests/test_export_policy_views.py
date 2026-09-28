# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Catalog diagnostics agree with selected schema-version export policy."""
import json
import os

import pytest

from raincloud.catalogs import operation
from raincloud.config import get_config
from raincloud.pipeline import browse, list_datasets, status


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    for key in list(os.environ):
        if key.startswith('RAINCLOUD_'):
            monkeypatch.delenv(key)
    monkeypatch.setenv('RAINCLOUD_NO_CONFIG', '1')
    monkeypatch.setenv('RAINCLOUD_OUTPUTS', str(tmp_path / 'data'))
    monkeypatch.setenv('RAINCLOUD_RAW_DOWNLOADS', str(tmp_path / 'raw'))
    monkeypatch.setenv('RAINCLOUD_WORKDIR', str(tmp_path / 'scratch'))
    manifest = tmp_path / 'sources.json'
    monkeypatch.setenv('RAINCLOUD_MANIFEST', str(manifest))

    def select(version, changes):
        spec = {'slug': 'tiny', 'fetch': {'type': 'http', 'urls': []}, **changes}
        m = {'schema_version': version, 'datasets': [spec]}
        manifest.write_text(json.dumps(m))
        return spec, m
    return select


@pytest.mark.parametrize('version,changes,enabled', [
    (2, {}, True),
    (2, {'export': {'formats': []}}, False),
    (2, {'export': {'formats': ['vortex'], 'priority': ['rs']}}, True),
    (2, {'export': {'formats': ['parquet'], 'priority': ['java']}}, False),
    (2, {'export': {'formats': ['parquet'], 'notes': 'x'}}, False),
    (1, {}, False),
    (1, {'convert': {'vortex': True}}, True),
    (1, {'convert': {'vortex': False}, 'export': {'formats': ['vortex']}}, False),
])
def test_selected_policy_reaches_cli_status_and_browser(catalog, capsys, version, changes, enabled):
    spec, manifest = catalog(version, changes)
    with operation(get_config()):
        assert list_datasets.main(['--vortex', '--json']) == 0
        rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        assert bool(rows) is enabled
        if rows:
            assert rows[0]['vortex'] is True
        assert list_datasets.main(['--no-vortex', '--json']) == 0
        rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
        assert bool(rows) is not enabled
        if rows:
            assert rows[0]['vortex'] is False
        assert status.vortex_status(spec, manifest)['opted_in'] is enabled
        app = browse.DatasetBrowser([spec], manifest)
        assert app._presence['tiny'][1] == ('·' if enabled else '—')


def test_declared_writer_presence_and_canonical_staleness(catalog):
    # One Vortex file whichever writer makes it; conformance is read for the
    # writer the spec declares.
    spec, manifest = catalog(2, {'export': {'formats': ['vortex'], 'priority': ['rs']}})
    with operation(get_config()):
        from raincloud.pipeline.spec import output_format_dir
        path = output_format_dir('tiny', 'vortex') / 'tiny.vortex'
        source = output_format_dir('tiny', 'arrow') / 'tiny.arrow.zstd'
        source.parent.mkdir(parents=True)
        source.write_bytes(b'canonical')
        os.utime(source, (10, 10))
        assert status.vortex_status(spec, manifest)['present'] is False
        path.parent.mkdir(parents=True)
        path.write_bytes(b'prepared')
        os.utime(path, (20, 20))
        assert status.vortex_status(spec, manifest)['present'] is True
        app = browse.DatasetBrowser([spec], manifest)
        assert app._presence['tiny'][1] == '✓'
        os.utime(source, (30, 30))
        assert status.vortex_status(spec, manifest)['stale'] is True
        assert browse.DatasetBrowser([spec], manifest)._presence['tiny'][1] == '⚠'
        assert browse._conformance_cell(spec, {'parquet@java': True, 'vortex@py': True}, 2) == 'P✓ V·'
        assert browse._conformance_cell(spec, {'parquet@java': True, 'vortex@rs': True}, 2) == 'P✓ V✓'
        assert browse._conformance_cell(spec, {'parquet@java': True, 'vortex@py': True, 'vortex@rs': False}, 2) == 'P✓ V✗'


def test_raw_metadata_is_not_downloaded_payload(catalog):
    spec, _ = catalog(2, {})
    with operation(get_config()):
        from raincloud.pipeline.spec import raw_slug_dir
        raw = raw_slug_dir('tiny')
        raw.mkdir(parents=True)
        (raw / '.fetch-recipe.json').write_text('{}')
        assert status._raw_status(spec) == {'present': False}
        (raw / 'rows.csv').write_text('x\n1\n')
        assert status._raw_status(spec)['present'] is True
