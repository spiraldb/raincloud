# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS generator isolation, pinned executable provenance and actual readers."""
import hashlib
import json
import os
import sys

import pytest

from raincloud._bundle import encode, make_bundle
from raincloud._generated import generation_key
from raincloud.pipeline.generators.tpcds import source_cli

REVISION = '0.1.0+git.3fc6faaa7dd28e4330b24d4240efe2852e79d00d'


@pytest.mark.skipif(os.name == 'nt', reason='executable script fixture requires POSIX')
@pytest.mark.parametrize('damage', ['none', 'bytes', 'revision', 'missing', 'nonobject'])
def test_source_cli_checks_provenance_before_execution(tmp_path, monkeypatch, damage):
    binary = tmp_path / 'tpcgen-cli'
    invoked = tmp_path / 'invoked'
    binary.write_text(f'#!{sys.executable}\nfrom pathlib import Path\nPath({str(invoked)!r}).touch()\nprint("tpcgen-cli 0.1.0")\n')
    binary.chmod(0o755)
    receipt = {'version': REVISION, 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest()}
    if damage == 'bytes':
        binary.write_text(binary.read_text() + '# changed executable\n')
    elif damage == 'revision':
        receipt['version'] = '0.1.0+git.' + '0' * 40
    elif damage == 'nonobject':
        receipt = []
    if damage != 'missing':
        binary.with_name(binary.name + '.raincloud.json').write_text(json.dumps(receipt))
    monkeypatch.setenv('RAINCLOUD_TPCGEN_CLI', str(binary))
    if damage == 'none':
        assert source_cli(REVISION) == binary
        assert invoked.exists()
    else:
        with pytest.raises(RuntimeError):
            source_cli(REVISION)
        assert not invoked.exists()


@pytest.mark.parametrize('generator,compat,reason_rows', [
    ('duckdb-tpcds', None, 35), ('tpcgen-rs-tpcds', 'c', 75), ('tpcgen-rs-tpcds', 'trino', 35),
])
def test_real_tpcds_generation_and_reader(tmp_path, generator, compat, reason_rows):
    """Full co-generation at SF1; requires installed tools, never installs them."""
    import raincloud
    from raincloud import duckdb_connect
    version = REVISION if compat else '1.5.5'
    if compat:
        try:
            source_cli(version)
        except RuntimeError:
            pytest.skip('pinned unified source CLI not installed')
    else:
        duckdb = pytest.importorskip('duckdb')
        if duckdb.__version__ != version:
            pytest.skip('pinned DuckDB version unavailable')
        with duckdb_connect() as con:
            if not con.execute("SELECT installed FROM duckdb_extensions() WHERE extension_name='tpcds'").fetchone()[0]:
                pytest.skip('TPC-DS extension not installed')
    params = {'sf': 1, 'compat': compat} if compat else {'sf': 1}
    recipes = [{'slug': name, 'fetch': {'type': 'generated', 'generator': generator, 'version': version,
                'parameters': params, 'output': name}, 'extract': {'type': 'passthrough'},
                'parse': {'reader': 'parquet'}, 'transform': {'handler': 'identity'},
                'export': {'formats': ['parquet']}, 'expect': {'rows': rows}}
               for name, rows in [('reason', reason_rows), ('ship_mode', 20)]]
    manifest = {'schema_version': 2, 'datasets': recipes}
    bundle = make_bundle(encode(manifest), encode({'schema_version': 2, 'slugs': {}}), 'tpcds-test')
    catalog = tmp_path/'catalog'; catalog.mkdir()
    for name, raw in bundle.files().items():
        (catalog/name).write_bytes(raw)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(catalog), data_dir=tmp_path/'data',
          raw_dir=tmp_path/'raw', scratch_dir=tmp_path/'work', cache_dir=tmp_path/'cache', catalog_dir=tmp_path/'catalogs')
    for name, count in [('reason', reason_rows), ('ship_mode', 20)]:
        table = raincloud.load(name, config=cfg, format='parquet', build=True).to_arrow()
        assert table.num_rows == count
        key = 'r_reason_sk' if name == 'reason' else 'sm_ship_mode_sk'
        assert sorted(table[key].to_pylist()) == list(range(1, count + 1))
    receipts = list((tmp_path/'raw').glob('.generated/*/*/receipt.json'))
    assert len(receipts) == 1
    receipt = json.loads(receipts[0].read_text())
    # Both generators now select exactly the 24 TPC-DS data tables. The Rust CLI also
    # emits a one-row `dbgen_version` (generator version, run timestamp, command line),
    # which is metadata about the invocation rather than about the data -- it would make
    # two logically identical builds compare unequal, so it is not a selected output.
    assert len(receipt['outputs']) == 24
    assert 'dbgen_version' not in receipt['outputs']
    assert receipt['recipe']['parameters'] == params
    if compat:
        other = dict(recipes[0]['fetch'], parameters={'sf': 1, 'compat': 'trino' if compat == 'c' else 'c'})
        assert generation_key(other) != generation_key(recipes[0]['fetch'])
