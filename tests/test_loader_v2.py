# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""v2-visibility: the loader derives its checkout snapshot path from the
manifest schema_version (docs/v{n}/snapshot.json) with a docs/v1/ fallback,
threads the version into artifact_key / cache_path, and the promoted
docs/v2/snapshot.json carries the full 250-slug catalog forward."""
from __future__ import annotations

import hashlib
import json

import pyarrow as pa
import vortex


def _sha(p) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _write_repo(root, *, schema_version, with_versioned_snapshot=True,
                with_v1_snapshot=True):
    """Materialize a synthetic checkout: sources.json (schema_version) plus the
    tracked docs/v{n}/snapshot.json and/or docs/v1/snapshot.json."""
    root.mkdir(parents=True, exist_ok=True)
    (root / "sources.json").write_text(json.dumps({"schema_version": schema_version}))
    if with_versioned_snapshot:
        p = root / "docs" / f"v{schema_version}" / "snapshot.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps({"schema_version": schema_version, "slugs": {}}))
    if with_v1_snapshot:
        p = root / "docs" / "v1" / "snapshot.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        if not p.exists():
            p.write_text(json.dumps({"schema_version": 1, "slugs": {}}))
    return root


# ---------- Gap A: _data_file snapshot path derives from schema_version ----------

def test_data_file_snapshot_resolves_versioned_docs(tmp_path, monkeypatch):
    """schema_version=2 + docs/v2/snapshot.json present → loader reads docs/v2."""
    from raincloud import _catalog
    monkeypatch.delenv("RAINCLOUD_SNAPSHOT", raising=False)
    repo = _write_repo(tmp_path / "repo", schema_version=2)
    monkeypatch.setattr(_catalog, "_repo_root", lambda: repo)
    assert _catalog._data_file("snapshot") == repo / "docs" / "v2" / "snapshot.json"


def test_data_file_snapshot_falls_back_to_v1_when_versioned_absent(tmp_path, monkeypatch):
    """schema_version=2 but docs/v2/snapshot.json NOT promoted yet → scaffold-safe
    fallback to docs/v1 (the loader never points at a missing file)."""
    from raincloud import _catalog
    monkeypatch.delenv("RAINCLOUD_SNAPSHOT", raising=False)
    repo = _write_repo(tmp_path / "repo", schema_version=2,
                       with_versioned_snapshot=False)
    monkeypatch.setattr(_catalog, "_repo_root", lambda: repo)
    assert _catalog._data_file("snapshot") == repo / "docs" / "v1" / "snapshot.json"


def test_data_file_snapshot_v1_manifest_reads_v1(tmp_path, monkeypatch):
    """schema_version=1 → docs/v1 (unchanged pre-migration behaviour)."""
    from raincloud import _catalog
    monkeypatch.delenv("RAINCLOUD_SNAPSHOT", raising=False)
    repo = _write_repo(tmp_path / "repo", schema_version=1)
    monkeypatch.setattr(_catalog, "_repo_root", lambda: repo)
    assert _catalog._data_file("snapshot") == repo / "docs" / "v1" / "snapshot.json"


def test_data_file_snapshot_no_manifest_defaults_to_v1(tmp_path, monkeypatch):
    """No checkout sources.json (wheel-ish) → version defaults to 1 → docs/v1."""
    from raincloud import _catalog
    monkeypatch.delenv("RAINCLOUD_SNAPSHOT", raising=False)
    repo = tmp_path / "repo"
    (repo / "docs" / "v1").mkdir(parents=True)
    (repo / "docs" / "v1" / "snapshot.json").write_text(
        json.dumps({"schema_version": 1, "slugs": {}}))
    monkeypatch.setattr(_catalog, "_repo_root", lambda: repo)
    assert _catalog._data_file("snapshot") == repo / "docs" / "v1" / "snapshot.json"


def test_data_file_env_override_still_wins(tmp_path, monkeypatch):
    """RAINCLOUD_SNAPSHOT beats the version-derived path (precedence unchanged)."""
    from raincloud import _catalog
    override = tmp_path / "custom.json"
    override.write_text("{}")
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(override))
    repo = _write_repo(tmp_path / "repo", schema_version=2)
    monkeypatch.setattr(_catalog, "_repo_root", lambda: repo)
    assert _catalog._data_file("snapshot") == override


def test_catalog_version_follows_v2_snapshot(tmp_path, monkeypatch):
    """A v2 snapshot → every Entry.version is 2, so artifact_key/cache_path
    compose the v2 prefix."""
    from raincloud import _catalog
    snapshot = {"schema_version": 2, "slugs": {"tiny": {
        "expected_rows": 3, "parquet_bytes": 10, "vortex_bytes": 20}}}
    manifest = {"schema_version": 2, "datasets": [
        {"slug": "tiny", "short_name": "T", "license": {}, "fetch": {"urls": []}}]}
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "snapshot.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    _catalog.load_catalog.cache_clear()
    try:
        assert _catalog.load_catalog().entry("tiny").version == 2
    finally:
        _catalog.load_catalog.cache_clear()


# ---------- promoted docs/v2/snapshot.json covers the whole catalog ----------

def test_promoted_v2_snapshot_covers_whole_catalog():
    """The tracked docs/v2/snapshot.json must be schema_version 2, carry an entry
    for every slug in sources.json (no catalog dash — a partial regen must never
    silently drop rows), and advertise arrow_bytes on the built-to-v2 subset.

    This deliberately does NOT compare against docs/v1. v2 began as a promotion of
    v1, but the catalog grows and slugs are retired on their own schedule, so
    pinning v2's membership to v1's froze the catalog rather than protecting it.
    sources.json is the authority; the snapshot is checked against that.
    """
    from raincloud.pipeline.spec import REPO_ROOT
    v2 = json.loads((REPO_ROOT / "docs" / "v2" / "snapshot.json").read_text())
    manifest = json.loads((REPO_ROOT / "sources.json").read_text())
    # Hydrated datasets are built only by name, so they have no bytes to record
    # until someone asks for one.
    catalog = {d["slug"] for d in manifest["datasets"] if not d.get("derive")}

    assert v2["schema_version"] == 2
    missing = catalog - set(v2["slugs"])
    assert not missing, f"snapshot dashed {len(missing)} slug(s): {sorted(missing)[:5]}"
    extra = set(v2["slugs"]) - {d["slug"] for d in manifest["datasets"]}
    assert not extra, f"snapshot carries {len(extra)} slug(s) absent from the manifest: {sorted(extra)[:5]}"

    arrow = {s for s, e in v2["slugs"].items() if e.get("arrow_bytes") is not None}
    assert arrow <= catalog
    assert arrow, "expected at least one arrow-bearing (built) slug"


# ---------- end-to-end: load resolves a v2 artifact through a file mirror ----------

def _v2_manifest_and_snapshot(vkey, extra_slugs=None):
    snapshot = {
        "schema_version": 2,
        "slugs": {
            "tiny": {
                "expected_rows": 3, "last_built_rows": 3,
                "parquet_bytes": None, "vortex_bytes": vkey.stat().st_size,
                "parquet_sha256": None, "vortex_sha256": _sha(vkey),
                "columns": [{"name": "x", "type": "int64"},
                            {"name": "y", "type": "string"}],
            },
        },
    }
    datasets = [{"slug": "tiny", "short_name": "Tiny",
                 "license": {"spdx": "CC0-1.0"}, "fetch": {"urls": []}}]
    for s, snap in (extra_slugs or {}).items():
        snapshot["slugs"][s] = snap
        datasets.append({"slug": s, "short_name": s,
                         "license": {"spdx": "CC0-1.0"}, "fetch": {"urls": []}})
    manifest = {"schema_version": 2, "datasets": datasets}
    return manifest, snapshot


def test_e2e_load_resolves_v2_key(tmp_path, monkeypatch):
    """A v2 catalog resolves the mirror's `v2/<slug>/<fmt>/...` key (not v1) and
    caches under cache_root()/v2/, proving the version threads end-to-end."""
    table = pa.table({"x": [10, 20, 30], "y": ["a", "b", "c"]})
    mirror = tmp_path / "mirror"
    vkey = mirror / "v2" / "tiny" / "vortex" / "tiny.vortex"
    vkey.parent.mkdir(parents=True)
    vortex.io.write(table, str(vkey))
    manifest, snapshot = _v2_manifest_and_snapshot(vkey)
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "snapshot.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{mirror}")

    import raincloud
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    try:
        ds = raincloud.load("tiny")
        assert ds.format == "vortex"
        tbl = ds.to_arrow()
        assert tbl.num_rows == 3
        assert tbl["x"].to_pylist() == [10, 20, 30]
        p = ds.path()
        assert p.exists()
        # Cached under the v2 prefix (not v1) — the version threaded through.
        assert p.parts[-4] == "v2", p
    finally:
        _catalog.load_catalog.cache_clear()


def test_e2e_v1_only_slug_still_resolves_under_v2(tmp_path, monkeypatch):
    """A slug carried forward from v1 (no arrow_bytes) still resolves in a v2
    catalog: parquet/vortex formats are exposed and the artifact loads from the
    v2-keyed mirror — the 247 carried slugs stay served."""
    table = pa.table({"n": [1, 2]})
    mirror = tmp_path / "mirror"
    vkey = mirror / "v2" / "legacy" / "vortex" / "legacy.vortex"
    vkey.parent.mkdir(parents=True)
    vortex.io.write(table, str(vkey))
    legacy_snap = {
        "expected_rows": 2, "last_built_rows": 2,
        "parquet_bytes": None, "vortex_bytes": vkey.stat().st_size,
        "parquet_sha256": None, "vortex_sha256": _sha(vkey),
        "columns": [{"name": "n", "type": "int64"}],
        # deliberately NO arrow_bytes — this is a v1-carried entry.
    }
    snapshot = {"schema_version": 2, "slugs": {"legacy": legacy_snap}}
    manifest = {"schema_version": 2, "datasets": [
        {"slug": "legacy", "short_name": "Legacy",
         "license": {"spdx": "CC0-1.0"}, "fetch": {"urls": []}}]}
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "snapshot.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{mirror}")

    import raincloud
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    try:
        entry = _catalog.load_catalog().entry("legacy")
        # The v2 recipe can build canonical Arrow even without a recorded artifact.
        assert entry.formats["arrow"].sha256 is None
        assert entry.formats["arrow"].nbytes is None
        assert {"vortex"} <= set(entry.formats)
        ds = raincloud.load("legacy")
        assert ds.to_arrow().num_rows == 2
        assert ds.path().parts[-4] == "v2"
    finally:
        _catalog.load_catalog.cache_clear()


def test_manifest_override_derives_its_own_snapshot_version(tmp_path, monkeypatch):
    """`RAINCLOUD_MANIFEST` must move the snapshot version WITH it.

    The version probe used to read the checkout `sources.json` unconditionally
    while the manifest read honored the override, so pointing at a v1 catalog
    from a v2 checkout paired that manifest with the checkout's v2 snapshot —
    two different catalogs fused, with mismatched cache keys and mirror metadata.
    """
    import json

    from raincloud import _catalog

    other = tmp_path / "other-sources.json"
    other.write_text(json.dumps({"schema_version": 1, "datasets": []}))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(other))
    monkeypatch.delenv("RAINCLOUD_SNAPSHOT", raising=False)

    assert _catalog._manifest_path() == other
    assert _catalog._manifest_schema_version() == 1
    # The snapshot now resolves under the OVERRIDDEN manifest's version, not the
    # checkout's (which is 2 on this branch).
    assert _catalog._snapshot_repo_path().parent.name == "v1"
    assert _catalog._data_file("manifest") == other
