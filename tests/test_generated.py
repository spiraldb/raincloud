# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Generated inputs: group reuse, transactional publication, identity and readers."""
import copy
import json
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from raincloud._bundle import build_requirements, encode, make_bundle, recipe_hash
from raincloud._generated import generation_key
from raincloud.pipeline import generate


class FixtureGenerator:
    """A real small file producer; cache, hashing and publication stay production code."""
    outputs = {"left": "left.parquet", "right": "right.parquet"}

    def __init__(self):
        self.calls = 0
        self.fail = False
        self.value = 1
        self.entered = None
        self.release = None

    def validate(self, params):
        assert params == {"size": 2}

    def generate(self, recipe, destination, scratch):
        self.calls += 1
        if self.entered:
            self.entered.set()
            assert self.release.wait(10)
        for name in self.outputs:
            pq.write_table(pa.table({"value": [self.value, self.value + 1]}), destination / f"{name}.parquet")
            if self.fail:
                raise RuntimeError("producer interrupted after first output")
        return {"fixture": recipe["version"]}


@pytest.fixture
def generated(tmp_path, monkeypatch):
    for key in ("RAINCLOUD_HOME", "RAINCLOUD_MANIFEST", "RAINCLOUD_SNAPSHOT"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("RAINCLOUD_NO_CONFIG", "1")
    monkeypatch.setenv("RAINCLOUD_RAW_DOWNLOADS", str(tmp_path / "raw"))
    monkeypatch.setenv("RAINCLOUD_WORKDIR", str(tmp_path / "scratch"))
    producer = FixtureGenerator()
    monkeypatch.setitem(generate.REGISTRY, "fixture", producer)
    spec = {"slug": "left", "fetch": {"type": "generated", "generator": "fixture", "version": "1",
            "parameters": {"size": 2}, "output": "left"}}
    return spec, producer


def test_siblings_reuse_complete_group_and_detect_same_size_corruption(generated):
    spec, producer = generated
    left = generate.fetch_generated(spec)[0]
    sibling = copy.deepcopy(spec)
    sibling.update(slug="unrelated-catalog-label")
    sibling["fetch"]["output"] = "right"
    right = generate.fetch_generated(sibling)[0]
    assert producer.calls == 1 and left.parent == right.parent
    assert pq.read_table(right).to_pydict() == {"value": [1, 2]}
    data = bytearray(left.read_bytes()); data[10] ^= 1; left.write_bytes(data)
    # A cache hit re-hashes only the requested member: the sibling asking for
    # `right` is still served, while `left` (same size, flipped bit) is caught
    # both by its own fetch and by a full verification of the group.
    assert generate.fetch_generated(sibling) == [right]
    assert pq.read_table(right).to_pydict() == {"value": [1, 2]}
    with pytest.raises(ValueError, match="checksum mismatch: left"):
        generate.fetch_generated(spec)
    with pytest.raises(ValueError, match="checksum mismatch: left"):
        generate.cached_outputs(spec["fetch"], verify=True)
    assert producer.calls == 1


def test_failure_preserves_group_and_refresh_records_drift(generated):
    spec, producer = generated
    original = generate.fetch_generated(spec)[0]
    receipt = generate.group_root(spec["fetch"]) / "current.json"
    committed = receipt.read_bytes()
    producer.fail = True
    with pytest.raises(RuntimeError, match="interrupted"):
        generate.fetch_generated(spec, refresh=True)
    assert receipt.read_bytes() == committed
    assert pq.read_table(original).to_pydict() == {"value": [1, 2]}
    assert len(list(receipt.parent.glob('*/receipt.json'))) == 1
    producer.fail = False
    producer.value = 42
    with pytest.warns(UserWarning, match="generated output drift"):
        replacement = generate.fetch_generated(spec, refresh=True)[0]
    assert replacement != original
    assert pq.read_table(replacement).to_pydict() == {"value": [42, 43]}
    assert pq.read_table(original).to_pydict() == {"value": [1, 2]}
    assert set(json.loads(receipt.read_text())["drift"]) == {"left", "right"}


def test_failed_commit_keeps_previous_pointer(generated, monkeypatch):
    spec, producer = generated
    original = generate.fetch_generated(spec)[0]
    pointer = generate.group_root(spec["fetch"]) / "current.json"
    old = pointer.read_bytes()
    atomic_write = generate.atomic_write

    def fail_pointer(path, data):
        if path == pointer:
            raise OSError("injected pointer I/O failure")
        atomic_write(path, data)

    monkeypatch.setattr(generate, "atomic_write", fail_pointer)
    with pytest.raises(OSError, match="pointer I/O"):
        generate.fetch_generated(spec, refresh=True)
    assert pointer.read_bytes() == old
    assert generate.fetch_generated(spec) == [original]


def test_concurrent_siblings_generate_once(generated):
    spec, producer = generated
    producer.entered, producer.release = Event(), Event()
    sibling = copy.deepcopy(spec); sibling["fetch"]["output"] = "right"
    with ThreadPoolExecutor(max_workers=2) as pool:
        left = pool.submit(generate.fetch_generated, spec)
        assert producer.entered.wait(10)
        right = pool.submit(generate.fetch_generated, sibling)
        producer.release.set()
        a, b = left.result(timeout=10)[0], right.result(timeout=10)[0]
    assert producer.calls == 1 and a.parent == b.parent
    assert pq.read_table(a).equals(pq.read_table(b))


def test_generated_identity_includes_parameters_version_and_selected_output(generated):
    spec, _ = generated
    base = generation_key(spec["fetch"])
    for key, value in (("version", "2"), ("parameters", {"size": 3}), ("generator", "another")):
        changed = copy.deepcopy(spec); changed["fetch"][key] = value
        assert generation_key(changed["fetch"]) != base
        assert recipe_hash(changed, 2, specs=None) != recipe_hash(spec, 2, specs=None)
    sibling = copy.deepcopy(spec); sibling["fetch"]["output"] = "right"
    assert generation_key(sibling["fetch"]) == base
    assert recipe_hash(sibling, 2, specs=None) != recipe_hash(spec, 2, specs=None)
    assert "generator:fixture" in build_requirements({"datasets": [spec]})


@pytest.mark.parametrize("damage", ["missing", "unsafe", "incomplete", "nonobject"])
def test_partial_or_unsafe_receipt_is_not_a_hit(generated, damage):
    spec, _ = generated
    generate.fetch_generated(spec)
    root = generate.group_root(spec["fetch"])
    receipt = json.loads((root / "current.json").read_text())
    if damage == "missing":
        (root / receipt['generation'] / 'right.parquet').unlink()
    elif damage == "unsafe":
        receipt['outputs']['right']['file'] = '../elsewhere.parquet'
    elif damage == "incomplete":
        del receipt['outputs']['right']
    else:
        receipt = []
    (root / "current.json").write_bytes(encode(receipt))
    with pytest.raises(ValueError):
        generate.fetch_generated(spec)


@pytest.mark.parametrize("generator,version", [("duckdb-tpch", "1.5.5"), ("tpcgen-rs-tpch", "3.0.0")])
def test_real_generator_build_and_prepared_reader(tmp_path, generator, version):
    """Optional installed toolchains; no downloads or installation in tests."""
    import shutil
    import sys
    from pathlib import Path

    import raincloud
    from raincloud import duckdb_connect
    if generator == "duckdb-tpch":
        import duckdb
        if duckdb.__version__ != version:
            pytest.skip("pinned DuckDB version unavailable")
        with duckdb_connect() as c:
            if not c.execute("select installed from duckdb_extensions() where extension_name='tpch'").fetchone()[0]:
                pytest.skip("tpch extension not installed")
    elif not (Path(sys.executable).parent / 'tpchgen-cli').is_file() and not shutil.which('tpchgen-cli'):
        pytest.skip("tpchgen-cli unavailable")
    specs = [{"slug": f"generated-{name}", "fetch": {"type": "generated", "generator": generator,
              "version": version, "parameters": {"sf": 0.01}, "output": name},
              "extract": {"type": "passthrough"}, "parse": {"reader": "parquet"},
              "transform": {"handler": "identity"}, "export": {"formats": ["parquet"]},
              "expect": {"rows": rows}}
             for name, rows in (("region", 5), ("nation", 25))]
    bundle = make_bundle(encode({"schema_version": 2, "datasets": specs}),
                         encode({"schema_version": 2, "slugs": {}}), "generated-test")
    catalog = tmp_path / 'catalog'; catalog.mkdir()
    for filename, content in bundle.files().items():
        (catalog / filename).write_bytes(content)
    cfg = raincloud.resolve_config(no_config=True, catalog=str(catalog), data_dir=tmp_path/'data',
          raw_dir=tmp_path/'raw', scratch_dir=tmp_path/'scratch', cache_dir=tmp_path/'cache', catalog_dir=tmp_path/'catalogs')
    for name, rows in (("region", 5), ("nation", 25)):
        ds = raincloud.load(f"generated-{name}", config=cfg, format="parquet", build=True)
        table = ds.to_arrow()
        assert table.num_rows == rows
        key = 'r_regionkey' if name == 'region' else 'n_nationkey'
        assert sorted(table[key].to_pylist()) == list(range(rows))
    assert len(list((tmp_path/'raw').glob('.generated/*/*/receipt.json'))) == 1
    assert not (tmp_path/'cache').exists()
