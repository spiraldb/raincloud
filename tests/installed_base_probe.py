# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Runs in an isolated base wheel installation; no source-tree imports."""
import hashlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from platformdirs import user_cache_path, user_data_path

import raincloud
from raincloud.cli import main
from raincloud.config import resolve_config

root = Path.cwd() / "portable files é"
root.mkdir()
assert importlib.util.find_spec("vortex") is None
assert importlib.util.find_spec("duckdb") is None
assert main(["capabilities"]) == 0
assert not raincloud.reader_capabilities()["vortex"]["available"]
settings = resolve_config(no_config=True)
assert settings.data_dir == user_data_path("raincloud", appauthor=False)
assert settings.scratch_dir == user_cache_path("raincloud", appauthor=False) / "workdir"
package = Path(raincloud.__file__).parent
before = {p.relative_to(package): p.stat().st_mtime_ns for p in package.rglob("*") if p.is_file()}
mirror = root / "mirror"
table = pa.table({"x": [1, 2, 3], "label": ["é", None, "雪"]})
row = {"expected_rows": 3}
for fmt, name in (("arrow", "tiny.arrow.zstd"), ("parquet", "tiny.parquet")):
    path = mirror / "v2" / "tiny" / fmt / name
    path.parent.mkdir(parents=True)
    if fmt == "arrow":
        with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_file(sink, table.schema, options=pa.ipc.IpcWriteOptions(compression="zstd")) as writer:
            writer.write_table(table)
    else:
        pq.write_table(table, path)
    row[f"{fmt}_bytes"] = path.stat().st_size
    row[f"{fmt}_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
manifest = {"schema_version": 2, "datasets": [{"slug": "tiny", "fetch": {"urls": ["https://example.invalid/tiny"]}}]}
(root / "sources.json").write_text(json.dumps(manifest), encoding="utf-8")
(root / "snapshot.json").write_text(json.dumps({"schema_version": 2, "slugs": {"tiny": row}}), encoding="utf-8")
config = root / "config.toml"
config.write_text('[raincloud]\ndata_dir = "hdd"\nscratch_dir = "scratch"\ncatalog_dir = "catalogs"\n'
                  + 'mirror = ' + json.dumps(mirror.as_uri()) + '\n', encoding="utf-8")
assert main(["--config", str(config), "catalog", "pack", "--manifest", str(root / "sources.json"), "--snapshot", str(root / "snapshot.json"), "--id", "portable", "--output", str(root / "upstream")]) == 0
assert main(["--config", str(config), "catalog", "update", "--source", str(root / "upstream")]) == 0
settings = resolve_config(config=config)
assert settings.data_dir == root / "hdd"
assert settings.scratch_dir == root / "scratch"
# Metadata stays lazy even though the catalog advertises an unavailable reader.
handle = raincloud.load("tiny", config=config)
# auto picks among the formats the install builds (only Vortex by default), then the
# canonical Arrow: a base install has no Vortex reader, so it is the Arrow file.
assert handle.format == "arrow", handle.format
assert not (root / "hdd").exists()
try:
    raincloud.load("tiny", format="vortex", config=config)
except raincloud.MissingDependency:
    pass
else:
    raise AssertionError("explicit Vortex must report its missing extra")
# Simultaneous processes must adopt one complete artifact and compatible pin.
code = 'import raincloud; ds=raincloud.load("tiny", config=' + repr(str(config)) + '); assert ds.to_arrow().num_rows == 3'
children = [subprocess.Popen([sys.executable, "-I", "-B", "-c", code]) for _ in range(3)]
assert all(child.wait(timeout=60) == 0 for child in children)
for fmt in ("parquet", "arrow"):
    ds = raincloud.load("tiny", format=fmt, config=config)
    with ds.batches(batch_size=2) as batches:
        materialized = pa.Table.from_batches(list(batches))
    assert materialized.equals(table)
    assert raincloud.load("tiny", format=fmt, config=config, offline=True).path() == ds.path()
assert not (root / "scratch").exists()
assert "vortex" not in sys.modules
assert "raincloud.pipeline.build" not in sys.modules
assert before == {p.relative_to(package): p.stat().st_mtime_ns for p in package.rglob("*") if p.is_file()}
print("base install: config, catalogs, concurrent mirror, offline IPC/Parquet, optional-reader errors passed on", sys.platform)
