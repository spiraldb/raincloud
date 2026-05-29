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

    assert _resolve.artifact_key("tiny", "parquet") == "v1/tiny/parquet/tiny.parquet"


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


def test_resolve_mirror_drift_warns_and_returns_artifact(env, capsys):
    """Mirror bytes that diverge from the snapshot sha are not a panic case.

    The user gets a warning naming the slug + origin; the artifact is still
    adopted into the cache so downstream work proceeds. Refusing to load
    would defeat the build over what is usually a benign upstream content
    refresh — exactly the kind of drift the loader is supposed to absorb.
    """
    from raincloud import _cache, _resolve

    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"drifted")
    p = _resolve.resolve("tiny", "parquet")
    assert p == _cache.cache_path("tiny", "parquet")
    assert p.read_bytes() == b"drifted"
    err = capsys.readouterr().err
    assert "[raincloud] WARN" in err
    assert "tiny" in err and "mirror" in err and "drifted" in err


def test_resolve_adopted_drift_served_from_cache_no_refetch(env, monkeypatch):
    """Once drifted bytes are adopted, later loads must serve them from cache.

    Regression: the cache-hit check only accepted a cached file whose size or
    sha matched the (stale) snapshot pin. Adopted drift matches neither, so
    every subsequent resolve re-fetched the mirror (or, with no mirror, re-ran
    the multi-hour build) — forever. A different-size drift (7 != 12 bytes) is
    the worst case because it can't ride the size fast-path.
    """
    from raincloud import _resolve, _transport

    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"drifted")

    calls = []
    real_fetch = _transport.fetch
    monkeypatch.setattr(
        _transport, "fetch",
        lambda url, dest: (calls.append(url), real_fetch(url, dest))[1],
    )
    for _ in range(3):
        p = _resolve.resolve("tiny", "parquet")
    assert p.read_bytes() == b"drifted"
    assert len(calls) == 1, (
        f"adopted drift re-fetched {len(calls)}x across 3 resolves "
        f"(expected 1 — the drift loop is back)"
    )


def test_resolve_adopted_drift_refetches_if_cache_truncated(env):
    """The pin records the adopted size, so post-adoption corruption (a torn
    write / truncation that changes the size) is NOT served blindly — it falls
    through to a fresh fetch instead of trusting the pin."""
    from raincloud import _cache, _resolve

    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"drifted")
    cached = _resolve.resolve("tiny", "parquet")
    assert cached.read_bytes() == b"drifted"

    # Corrupt the cached file (different size than the pin recorded), and have
    # the mirror now serve good drifted bytes again.
    cached.write_bytes(b"trunc")
    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"drifted")
    assert _resolve.resolve("tiny", "parquet").read_bytes() == b"drifted"
    # And the freshly re-adopted file is once again a clean cache hit.
    assert _cache.read_pin(cached)["size"] == len(b"drifted")


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


def test_resolve_strict_checksum_hard_fails_on_mirror_drift(env, monkeypatch):
    """RAINCLOUD_STRICT_CHECKSUM flips drift from warn-and-adopt to a hard
    ChecksumMismatch — the opt-in integrity gate for security-sensitive use."""
    from raincloud import _resolve
    from raincloud.exceptions import ChecksumMismatch

    monkeypatch.setenv("RAINCLOUD_STRICT_CHECKSUM", "1")
    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"drifted")
    with pytest.raises(ChecksumMismatch):
        _resolve.resolve("tiny", "parquet")


def test_resolve_strict_ignores_prior_drift_pin(env, monkeypatch):
    """Drift adopted by an earlier non-strict run must NOT be served once strict
    mode is on: the pin sidecar vouches for it, but strict rehashes and refuses."""
    from raincloud import _cache, _resolve
    from raincloud.exceptions import ChecksumMismatch

    # Non-strict: adopt drifted bytes (writes a drift pin that vouches for them).
    (env["mirror"] / "v1" / "tiny" / "parquet" / "tiny.parquet").write_bytes(b"drifted")
    assert _resolve.resolve("tiny", "parquet").read_bytes() == b"drifted"
    assert _cache.read_pin(_cache.cache_path("tiny", "parquet")) is not None

    # Strict: the pin is ignored, the cached bytes are rehashed, and since the
    # mirror still serves the same drift, the strict re-fetch hard-fails.
    monkeypatch.setenv("RAINCLOUD_STRICT_CHECKSUM", "1")
    with pytest.raises(ChecksumMismatch):
        _resolve.resolve("tiny", "parquet")


def test_resolve_strict_serves_matching_cache(env, monkeypatch):
    """Strict mode still serves a cache file whose sha matches the snapshot."""
    from raincloud import _resolve

    _resolve.resolve("tiny", "parquet")  # prime cache with blessed bytes
    monkeypatch.setenv("RAINCLOUD_STRICT_CHECKSUM", "1")
    # corrupt the mirror to prove strict served from cache, not a re-fetch
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


def test_resolve_build_fallback_adopts_built_artifact(env, monkeypatch):
    """Cache miss + mirror miss + build available: resolve() shells out to the
    build, then adopts the produced artifact into the cache. Exercised
    hermetically (the real path is otherwise only under --run-network).
    """
    from raincloud import _cache, _resolve

    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{env['tmp']}/empty")  # mirror miss
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(env["tmp"] / "out"))      # build writes here
    monkeypatch.setattr(_resolve, "_build_import_error", lambda: None)    # build available

    built_calls = []

    def fake_build(cmd, check):
        built_calls.append(cmd)
        from scripts.pipeline.spec import output_format_dir
        d = output_format_dir("tiny", "parquet")
        d.mkdir(parents=True, exist_ok=True)
        (d / "tiny.parquet").write_bytes(env["payload"])  # bytes match snapshot sha

    monkeypatch.setattr(_resolve.subprocess, "run", fake_build)

    p = _resolve.resolve("tiny", "parquet")
    assert built_calls, "build subprocess was never invoked"
    assert p == _cache.cache_path("tiny", "parquet")
    assert p.read_bytes() == env["payload"]


def test_resolve_build_produces_nothing_raises(env, monkeypatch):
    """If the build runs but emits no artifact, resolve() raises ArtifactNotFound
    rather than returning a phantom path."""
    from raincloud import _resolve
    from raincloud.exceptions import ArtifactNotFound

    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{env['tmp']}/empty")
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(env["tmp"] / "out"))
    monkeypatch.setattr(_resolve, "_build_import_error", lambda: None)  # build available
    monkeypatch.setattr(_resolve.subprocess, "run", lambda cmd, check: None)  # writes nothing

    with pytest.raises(ArtifactNotFound):
        _resolve.resolve("tiny", "parquet")


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
        if name == "scripts.pipeline.build":
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
        if name == "scripts.pipeline.build":
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

    def boom(cmd, check):
        raise sp.CalledProcessError(2, cmd)

    monkeypatch.setattr(_resolve.subprocess, "run", boom)
    with pytest.raises(BuildFailed) as ei:
        _resolve.resolve("tiny", "parquet")
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
        _resolve.resolve("tiny", "parquet")
    msg = str(ei.value)
    assert "failed to import" in msg and "RuntimeError" in msg
    assert "pip install" not in msg  # not the wrong-remediation message


def _strict_build_env(env, monkeypatch):
    """Wire strict mode + a mirror miss + a fake build that writes bytes which
    deliberately differ from the snapshot sha; return the build-invocation log."""
    from raincloud import _resolve

    monkeypatch.setenv("RAINCLOUD_STRICT_CHECKSUM", "1")
    monkeypatch.setenv("RAINCLOUD_MIRROR", f"file://{env['tmp']}/empty")  # mirror miss
    monkeypatch.setenv("RAINCLOUD_OUTPUTS", str(env["tmp"] / "out"))
    monkeypatch.setattr(_resolve, "_build_import_error", lambda: None)
    builds = []

    def fake_build(cmd, check):
        builds.append(cmd)
        from scripts.pipeline.spec import output_format_dir
        d = output_format_dir("tiny", "parquet")
        d.mkdir(parents=True, exist_ok=True)
        (d / "tiny.parquet").write_bytes(b"locally-built-different-bytes")

    monkeypatch.setattr(_resolve.subprocess, "run", fake_build)
    return builds


def test_resolve_strict_build_serves_from_cache_not_rebuild(env, monkeypatch):
    """A client's local build legitimately differs from the maintainer's
    snapshot bytes. Under strict mode the build path must NOT hard-fail with
    ChecksumMismatch, AND a SECOND load must serve the built artifact from
    cache via its origin=build provenance pin — not rebuild every load. (The
    earlier single-resolve version of this test hid a rebuild-every-load cliff.)
    """
    from raincloud import _cache, _resolve

    builds = _strict_build_env(env, monkeypatch)
    p1 = _resolve.resolve("tiny", "parquet")
    assert p1 == _cache.cache_path("tiny", "parquet")
    assert p1.read_bytes() == b"locally-built-different-bytes"
    # Second strict load: served from cache via the origin=build pin, no rebuild.
    p2 = _resolve.resolve("tiny", "parquet")
    assert p2.read_bytes() == b"locally-built-different-bytes"
    assert len(builds) == 1, (
        f"strict mode rebuilt instead of serving the cached local build "
        f"({len(builds)} builds across 2 loads — the rebuild-every-load cliff)"
    )


def test_resolve_strict_build_rebuilds_when_snapshot_pin_changes(env, monkeypatch):
    """The origin=build pin is honored under strict only while the snapshot pin
    is unchanged. A genuine snapshot revision (the maintainer republished new
    bytes) makes the cached local build stale, so it is rebuilt against the new
    source of truth rather than served forever."""
    from raincloud import _catalog, _resolve

    builds = _strict_build_env(env, monkeypatch)
    _resolve.resolve("tiny", "parquet")
    assert len(builds) == 1

    # Maintainer revises the snapshot pin (new published sha) -> the cached
    # local build is now stale against the source of truth.
    snap_path = env["tmp"] / "snapshot.json"
    snap = json.loads(snap_path.read_text())
    snap["slugs"]["tiny"]["parquet_sha256"] = _sha(b"REVISED-PUBLISHED-BYTES")
    snap_path.write_text(json.dumps(snap))
    _catalog.load_catalog.cache_clear()

    _resolve.resolve("tiny", "parquet")
    assert len(builds) == 2, "stale local build (snapshot pin changed) not rebuilt under strict"


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


def test_shaless_cache_served_via_pin_no_refetch(shaless_env, monkeypatch):
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


def test_shaless_size_mismatch_without_pin_refetches(shaless_env, capsys):
    """A sha-less cache file whose size != the snapshot byte size and has NO pin
    vouching for it is treated as corruption: warn + re-fetch, rather than
    serving possibly-truncated bytes on mere existence (the old behavior)."""
    from raincloud import _cache, _resolve

    dest = _cache.cache_path("ns", "parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"TRUNC")  # 5 bytes != 12; no pin written
    p = _resolve.resolve("ns", "parquet")
    assert p.read_bytes() == shaless_env["payload"]  # re-fetched the good bytes
    assert "size" in capsys.readouterr().err


def test_shaless_size_match_without_pin_served(shaless_env, monkeypatch):
    """A legacy sha-less cache file (no pin) whose size matches the snapshot byte
    size is trusted and served without a re-fetch."""
    from raincloud import _cache, _resolve, _transport

    dest = _cache.cache_path("ns", "parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(shaless_env["payload"])  # right size, no pin
    calls = []
    real = _transport.fetch
    monkeypatch.setattr(_transport, "fetch",
                        lambda u, d: (calls.append(u), real(u, d))[1])
    assert _resolve.resolve("ns", "parquet").read_bytes() == shaless_env["payload"]
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
    dest = _cache.cache_path("nn", "parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(b"whatever-bytes")
    assert _resolve.resolve("nn", "parquet").read_bytes() == b"whatever-bytes"
    _catalog.load_catalog.cache_clear()
