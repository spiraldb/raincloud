# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Shared prepared-data fixture for Python, Rust, C, C++, and Java tests."""
from pathlib import Path


def create(root: Path):
    import hashlib
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq
    import vortex

    from raincloud._bundle import encode, make_bundle
    from raincloud._resolve import artifact_key

    table = pa.table({
        "id": pa.array([0, 1, 2, 3, 4, 5, 6, 2**64-1], type=pa.uint64()),
        "text": ["a", None, "é", "", "five", "six", "seven", "last"],
        "value": pa.array([0., -0., None, 1.5, 2.5, -3.5, 4., 5.]),
        "nested": pa.array([[1, None], None, [], [3], [4], [5], [6], [7]], type=pa.list_(pa.int32())),
    })
    data = root / "data"
    snapshot = {"schema_version": 2, "slugs": {"tiny": {"last_built_rows": 8, "columns": [{"name": f.name, "type": str(f.type)} for f in table.schema]}}}
    spec = {"slug": "tiny", "description": "Native reader contract fixture", "expect": {"rows": 8}}
    for fmt in ("arrow", "parquet", "vortex"):
        path = data / artifact_key("tiny", fmt, 2)
        path.parent.mkdir(parents=True, exist_ok=True)
        if fmt == "arrow":
            with pa.ipc.new_file(path, table.schema, options=pa.ipc.IpcWriteOptions(compression="zstd")) as writer:
                writer.write_table(table, max_chunksize=3)
        elif fmt == "vortex":
            vortex.io.write(table, str(path))
        else:
            pq.write_table(table, path, row_group_size=3)
        size, sha = path.stat().st_size, hashlib.sha256(path.read_bytes()).hexdigest()
        entry = snapshot["slugs"]["tiny"]
        entry[f"{fmt}_bytes"], entry[f"{fmt}_sha256"] = size, sha
        entry[f"{fmt}_writer"] = "canonical" if fmt == "arrow" else "py"
    manifest = {"schema_version": 2, "datasets": [spec]}
    bundle = make_bundle(encode(manifest), encode(snapshot), "reader-fixture")
    catalog = root / "catalog"
    catalog.mkdir(parents=True, exist_ok=True)
    for name, raw in bundle.files().items():
        (catalog / name).write_bytes(raw)
    options = {"no_config": True, "catalog": str(catalog), "data_dir": str(data),
               "cache_dir": str(root / "cache"), "catalog_dir": str(root / "catalogs"), "offline": True}
    (root / "options.json").write_text(json.dumps(options))
    return table, options


if __name__ == "__main__":
    import sys
    create(Path(sys.argv[1]).resolve())
