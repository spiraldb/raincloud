# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import hashlib
import json

import pytest


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


@pytest.fixture
def env(tmp_path, monkeypatch):
    """A file:// mirror with one artifact + a catalog describing it."""
    payload = b"PARQUETBYTES"
    mirror = tmp_path / "mirror"
    key = mirror / "v1" / "tiny" / "parquet" / "tiny.parquet"
    key.parent.mkdir(parents=True)
    key.write_bytes(payload)
    snapshot = {
        "schema_version": 1,
        "slugs": {
            "tiny": {
                "expected_rows": 3,
                "last_built_rows": 3,
                "parquet_bytes": len(payload),
                "vortex_bytes": None,
                "parquet_sha256": _sha(payload),
                "vortex_sha256": None,
                "columns": [{"name": "x", "type": "int64"}],
            }
        },
    }
    manifest = {
        "schema_version": 1,
        "datasets": [
            {"slug": "tiny", "short_name": "T", "license": {}, "fetch": {"urls": []}}
        ],
    }
    sp = tmp_path / "snapshot.json"
    sp.write_text(json.dumps(snapshot))
    mp = tmp_path / "sources.json"
    mp.write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(sp))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(mp))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{mirror}")
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)
    from raincloud import _catalog

    _catalog.load_catalog.cache_clear()
    yield {"payload": payload, "mirror": mirror, "tmp": tmp_path}
    _catalog.load_catalog.cache_clear()


def test_artifact_key():
    from raincloud import _resolve

    assert _resolve.artifact_key("tiny", "parquet", 1) == "v1/tiny/parquet/tiny.parquet"


def test_resolve_from_mirror_then_cache(env):
    from raincloud import _cache, _resolve

    p = _resolve.resolve("tiny", "parquet")
    assert p == _cache.cache_path("tiny", "parquet")
    assert p.read_bytes() == env["payload"]
    # second call is a pure cache hit (corrupt the mirror to prove no refetch)
    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"X")
    assert _resolve.resolve("tiny", "parquet").read_bytes() == env["payload"]


def test_cache_hit_size_match_skips_full_rehash(env, monkeypatch):
    """Repeat loads of a cached file must NOT re-stream the full sha256.

    Defeats the cache on multi-GB artifacts (the original bug). Size match
    with a pinned snapshot size is the fast path; full rehash only runs when
    size disagrees.
    """
    from raincloud import _cache, _resolve

    # Prime the cache via a mirror fetch.
    _resolve.resolve("tiny", "parquet")
    rehash_calls = []
    real_sha = _cache.sha256_file
    monkeypatch.setattr(
        _cache, "sha256_file",
        lambda p: (rehash_calls.append(p), real_sha(p))[1],
    )
    # Cache hit with matching size — must NOT call sha256_file.
    _resolve.resolve("tiny", "parquet")
    assert rehash_calls == [], (
        f"cache hit re-streamed sha256 ({len(rehash_calls)} calls); "
        f"size short-circuit failed"
    )


def test_resolve_snapshot_revision_refetches_over_stale_cache(env, monkeypatch):
    """A genuine snapshot revision must still pull the new artifact.

    Distinct from adopted-drift: here the *snapshot* pins new bytes while the
    cache holds the old ones. The loader must re-fetch, not serve stale cache.
    """
    from raincloud import _catalog, _resolve

    # Prime cache with the original blessed artifact.
    assert _resolve.resolve("tiny", "parquet").read_bytes() == env["payload"]

    # Publish new bytes to the mirror AND revise the snapshot pin to match.
    new = b"REVISED-CONTENT-LONGER"
    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(new)
    snap_path = env["tmp"] / "snapshot.json"
    snap = json.loads(snap_path.read_text())
    snap["slugs"]["tiny"]["parquet_bytes"] = len(new)
    snap["slugs"]["tiny"]["parquet_sha256"] = _sha(new)
    snap_path.write_text(json.dumps(snap))
    _catalog.load_catalog.cache_clear()

    assert _resolve.resolve("tiny", "parquet").read_bytes() == new


def test_resolve_refuses_mirror_bytes_the_catalog_does_not_name(env):
    """The catalog is the authority: drifted mirror bytes are not its artifact."""
    from raincloud import _cache, _resolve
    from raincloud.exceptions import ChecksumMismatch

    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"drifted")
    with pytest.raises(ChecksumMismatch):
        _resolve.resolve("tiny", "parquet")
    assert not _cache.cache_path("tiny", "parquet").exists()


def test_resolve_serves_matching_cache_without_refetch(env):
    from raincloud import _resolve

    _resolve.resolve("tiny", "parquet")  # prime cache with blessed bytes
    # corrupt the mirror to prove the cache served, not a re-fetch
    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"X")
    assert _resolve.resolve("tiny", "parquet").read_bytes() == env["payload"]


def test_tmp_path_is_unique_and_sweepable(tmp_path):
    """Per-process-unique .part names so concurrent loaders don't clobber, and
    they match the prefix/suffix _sweep_stale_parts looks for."""
    from raincloud import _resolve
    dest = tmp_path / "tiny.parquet"
    a, b = _resolve._tmp_path(dest), _resolve._tmp_path(dest)
    assert a != b
    for p in (a, b):
        assert p.name.startswith(".tiny.parquet.") and p.name.endswith(".part")


def test_sweep_stale_parts_removes_only_old(tmp_path):
    """Orphaned .part files older than the threshold are swept; an actively
    written one (fresh mtime) and unrelated files are left alone."""
    import os

    from raincloud import _resolve
    dest = tmp_path / "tiny.parquet"
    stale = _resolve._tmp_path(dest); stale.write_bytes(b"x")
    fresh = _resolve._tmp_path(dest); fresh.write_bytes(b"y")
    unrelated = tmp_path / "keepme.parquet"; unrelated.write_bytes(b"z")
    old = _resolve.time.time() - _resolve._STALE_PART_SECONDS - 60
    os.utime(stale, (old, old))

    _resolve._sweep_stale_parts(dest)
    assert not stale.exists(), "stale .part should have been swept"
    assert fresh.exists(), "fresh in-flight .part must be preserved"
    assert unrelated.exists(), "non-.part siblings must be untouched"


def test_resolve_build_fallback_serves_data_dir_build(env, monkeypatch):
    """Cache miss + mirror miss + build available: resolve() shells out to the
    build and serves the artifact it wrote under the data dir, in place -- it
    is not copied into the cache. The build log goes to stderr, never the
    caller's stdout. Exercised hermetically (the real path is otherwise only
    under --run-network).
    """
    from raincloud import _cache, _resolve

    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{env['tmp']}/empty")  # mirror miss
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(env["tmp"] / "out"))      # build writes here
    monkeypatch.setattr(_resolve, "_build_import_error", lambda: None)    # build available

    built_calls = []

    def fake_build(cmd, check, **kwargs):
        built_calls.append((cmd, kwargs))
        from raincloud.pipeline.spec import output_format_dir
        d = output_format_dir("tiny", "parquet")
        d.mkdir(parents=True, exist_ok=True)
        (d / "tiny.parquet").write_bytes(env["payload"])

    monkeypatch.setattr(_resolve.subprocess, "run", fake_build)

    p = _resolve.resolve("tiny", "parquet", allow_build=True)
    assert built_calls, "build subprocess was never invoked"
    assert built_calls[0][1].get("stdout") is not None, "build log must not reach stdout"
    assert p == env["tmp"] / "out/v1/tiny/parquet/tiny.parquet"
    assert not _cache.cache_path("tiny", "parquet").exists()
    assert p.read_bytes() == env["payload"]


def test_resolve_build_produces_nothing_raises(env, monkeypatch):
    """If the build runs but emits no artifact, resolve() raises ArtifactNotFound
    rather than returning a phantom path."""
    from raincloud import _resolve
    from raincloud.exceptions import ArtifactNotFound

    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{env['tmp']}/empty")
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(env["tmp"] / "out"))
    monkeypatch.setattr(_resolve, "_build_import_error", lambda: None)  # build available
    monkeypatch.setattr(_resolve.subprocess, "run", lambda cmd, check, **kwargs: None)  # writes nothing

    with pytest.raises(ArtifactNotFound):
        _resolve.resolve("tiny", "parquet", allow_build=True)


def test_resolve_offline_miss(env, monkeypatch):
    from raincloud import _resolve
    from raincloud.exceptions import OfflineMiss

    monkeypatch.setenv("RAINCLOUD_OFFLINE", "1")
    with pytest.raises(OfflineMiss):
        _resolve.resolve("tiny", "parquet")


def test_resolve_mirror_miss_no_build(env, monkeypatch):
    from raincloud import _resolve
    from raincloud.exceptions import BuildToolingMissing

    # point mirror somewhere empty; disable build (missing [build] extra)
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{env['tmp']}/empty")
    monkeypatch.setattr(_resolve, "_build_import_error",
                        lambda: ImportError("No module named 'zstandard'"))
    with pytest.raises(BuildToolingMissing) as ei:
        _resolve.resolve("tiny", "parquet", allow_build=True)
    # ImportError -> the actionable "install the extra" hint.
    assert "raincloud[build]" in str(ei.value)


def test_build_available_false_when_build_import_fails(monkeypatch):
    import importlib

    from raincloud import _resolve
    real_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name == "raincloud.pipeline.build":
            raise ModuleNotFoundError("No module named 'zstandard'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    assert _resolve._build_available() is False


def test_build_available_false_when_handler_module_init_raises(monkeypatch):
    """A non-ImportError raised at module-init in the build subtree must

    flip _build_available to False, not propagate up through resolve().
    A regression in a handler's top-level code shouldn't break the loader's
    BuildToolingMissing fallback message.
    """
    import importlib

    from raincloud import _resolve
    real_import = importlib.import_module

    def fake_import(name, *args, **kwargs):
        if name == "raincloud.pipeline.build":
            raise RuntimeError("top-of-module assertion in some handler")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    assert _resolve._build_available() is False


def test_resolve_propagates_non_notfound_transport_error(env, monkeypatch):
    from raincloud import _resolve, _transport

    def boom(url, dest):
        raise PermissionError("403 denied")

    monkeypatch.setattr(_transport, "fetch", boom)
    # A transport error that is NOT a clean miss must propagate, not silently
    # fall through to a (potentially multi-hour) local build.
    with pytest.raises(PermissionError):
        _resolve.resolve("tiny", "parquet")


def test_resolve_build_failure_raises_buildfailed(env, monkeypatch):
    """A non-zero build subprocess must surface as a typed BuildFailed, not a
    raw subprocess.CalledProcessError that escapes the RaincloudError hierarchy."""
    import subprocess as sp

    from raincloud import _resolve
    from raincloud.exceptions import BuildFailed, RaincloudError

    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{env['tmp']}/empty")  # mirror miss
    monkeypatch.setattr(_resolve, "_build_import_error", lambda: None)    # build available

    def boom(cmd, check, **kwargs):
        raise sp.CalledProcessError(2, cmd)

    monkeypatch.setattr(_resolve.subprocess, "run", boom)
    with pytest.raises(BuildFailed) as ei:
        _resolve.resolve("tiny", "parquet", allow_build=True)
    assert isinstance(ei.value, RaincloudError)  # typed, catchable as the base


def test_resolve_build_import_broken_says_so(env, monkeypatch):
    """When the [build] subtree is present but fails to import for a NON-import
    reason, the message must surface that — not misdirect to `pip install`."""
    from raincloud import _resolve
    from raincloud.exceptions import BuildToolingMissing

    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{env['tmp']}/empty")  # mirror miss
    monkeypatch.setattr(_resolve, "_build_import_error",
                        lambda: RuntimeError("a handler exploded at import"))
    with pytest.raises(BuildToolingMissing) as ei:
        _resolve.resolve("tiny", "parquet", allow_build=True)
    msg = str(ei.value)
    assert "failed to import" in msg and "RuntimeError" in msg
    assert "pip install" not in msg  # not the wrong-remediation message


# ---- sha-less slugs: the snapshot byte size is the integrity check ----

@pytest.fixture
def shaless_env(tmp_path, monkeypatch):
    """Like `env`, but the snapshot records only a byte size (no sha256) — the
    common case for ~80% of the catalog."""
    payload = b"NO-SHA-BYTES"
    mirror = tmp_path / "mirror"
    key = mirror / "v1" / "ns" / "parquet" / "ns.parquet"
    key.parent.mkdir(parents=True)
    key.write_bytes(payload)
    snapshot = {
        "schema_version": 1,
        "slugs": {"ns": {"expected_rows": 1, "parquet_bytes": len(payload),
                         "vortex_bytes": None, "parquet_sha256": None,
                         "vortex_sha256": None, "columns": []}},
    }
    manifest = {"schema_version": 1, "datasets": [
        {"slug": "ns", "short_name": "N", "license": {}, "fetch": {"urls": []}}]}
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "snapshot.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{mirror}")
    monkeypatch.delenv("RAINCLOUD_OFFLINE", raising=False)
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    yield {"payload": payload, "mirror": mirror, "tmp": tmp_path}
    _catalog.load_catalog.cache_clear()


def test_shaless_cache_served_by_size_no_refetch(shaless_env, monkeypatch):
    """A sha-less slug, once adopted, serves from cache via its size pin without
    re-fetching — even though there's no sha to match."""
    from raincloud import _resolve, _transport

    _resolve.resolve("ns", "parquet")  # prime cache (adopt writes a {None,size} pin)
    calls = []
    real = _transport.fetch
    monkeypatch.setattr(_transport, "fetch",
                        lambda u, d: (calls.append(u), real(u, d))[1])
    # corrupt the mirror to prove the second load doesn't re-fetch
    (shaless_env["mirror"] / "v1" / "ns" / "parquet" / "ns.parquet").write_bytes(b"X")
    assert _resolve.resolve("ns", "parquet").read_bytes() == shaless_env["payload"]
    assert calls == []


def test_shaless_size_mismatch_refetches(shaless_env):
    """A sha-less cache file whose size != the snapshot byte size and has NO pin
    vouching for it is treated as corruption: warn + re-fetch, rather than
    serving possibly-truncated bytes on mere existence (the old behavior)."""
    from dataclasses import replace

    from raincloud import _cache, _resolve
    from raincloud._catalog import load_catalog
    legacy_entry = replace(load_catalog().entry("ns"), legacy=True)
    dest = _cache.cache_path("ns", "parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"TRUNC")  # 5 bytes != 12; no pin written
    p = _resolve.resolve("ns", "parquet", entry=legacy_entry)
    assert p.read_bytes() == shaless_env["payload"]  # re-fetched the good bytes


def test_shaless_size_match_served(shaless_env, monkeypatch):
    """A legacy sha-less cache file (no pin) whose size matches the snapshot byte
    size is trusted and served without a re-fetch."""
    from dataclasses import replace

    from raincloud import _cache, _resolve, _transport
    from raincloud._catalog import load_catalog
    legacy_entry = replace(load_catalog().entry("ns"), legacy=True)
    dest = _cache.cache_path("ns", "parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(shaless_env["payload"])  # right size, no pin
    calls = []
    real = _transport.fetch
    monkeypatch.setattr(_transport, "fetch",
                        lambda u, d: (calls.append(u), real(u, d))[1])
    assert _resolve.resolve("ns", "parquet", entry=legacy_entry).read_bytes() == shaless_env["payload"]
    assert calls == []


def test_shaless_mirror_revision_refetches(shaless_env):
    """A sha-less slug whose mirror bytes are revised to a DIFFERENT size (and
    the snapshot byte size updated to match) must re-fetch — the old cached
    bytes are stale against the new source of truth, not served forever.
    Regression: the sha-less cache-hit used to serve on ANY size-matching pin,
    shadowing the snapshot-size check, so a revision was never seen."""
    from raincloud import _catalog, _resolve

    # Prime the cache from the mirror (mirror-origin pin, old size).
    assert _resolve.resolve("ns", "parquet").read_bytes() == shaless_env["payload"]
    # Maintainer republishes new, larger bytes and updates the snapshot size
    # (still no sha — this slug stays sha-less).
    new = b"REVISED-NS-BYTES-LONGER"
    (shaless_env["mirror"] / "v1" / "ns" / "parquet" / "ns.parquet").write_bytes(new)
    snap_path = shaless_env["tmp"] / "snapshot.json"
    snap = json.loads(snap_path.read_text())
    snap["slugs"]["ns"]["parquet_bytes"] = len(new)
    snap_path.write_text(json.dumps(snap))
    _catalog.load_catalog.cache_clear()

    assert _resolve.resolve("ns", "parquet").read_bytes() == new


def test_shaless_sizeless_serves_on_existence(tmp_path, monkeypatch):
    """A slug with NEITHER a pinned sha NOR a byte size has nothing to check,
    so a pre-existing cache file is served on existence alone. (Offline is on,
    so any fall-through to fetch/build would raise instead of serving.)"""
    from raincloud import _cache, _catalog, _resolve
    snapshot = {"schema_version": 1, "slugs": {"nn": {
        "expected_rows": 1, "parquet_bytes": None, "vortex_bytes": None,
        "parquet_sha256": None, "vortex_sha256": None, "columns": []}}}
    manifest = {"schema_version": 1, "datasets": [
        {"slug": "nn", "short_name": "N", "license": {}, "fetch": {"urls": []}}]}
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))
    monkeypatch.setenv("RAINCLOUD_SNAPSHOT", str(tmp_path / "snapshot.json"))
    monkeypatch.setenv("RAINCLOUD_MANIFEST", str(tmp_path / "sources.json"))
    monkeypatch.setenv("RAINCLOUD_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("RAINCLOUD_OFFLINE", "1")
    _catalog.load_catalog.cache_clear()
    from dataclasses import replace

    from raincloud._catalog import load_catalog
    legacy_entry = replace(load_catalog().entry("nn"), legacy=True)
    dest = _cache.cache_path("nn", "parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"whatever-bytes")
    assert _resolve.resolve("nn", "parquet", entry=legacy_entry).read_bytes() == b"whatever-bytes"
    _catalog.load_catalog.cache_clear()
