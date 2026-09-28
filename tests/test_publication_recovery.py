# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Failed publications keep the previous bytes and permit the identical retry."""
import json
from pathlib import Path

import pyarrow as pa
import pytest

import raincloud
from raincloud._bundle import encode, make_bundle
from raincloud.pipeline import build
from raincloud.pipeline.canonical import open_canonical_writer
from raincloud.pipeline.export import get_exporter
from tests.reader_fixture import create
from tests.test_build_profile_identity import isolated as isolated
from tests.test_native_clients import NativeError
from tests.test_native_clients import native as native


@pytest.mark.parametrize("streaming", [False, True])
@pytest.mark.parametrize("failure", ["validation", "exporter", "measurement"])
@pytest.mark.parametrize("prior", [False, True])
def test_build_failure_retry(isolated, monkeypatch, streaming, failure, prior):
    cfg, spec = isolated
    from raincloud.pipeline.spec import prepared_arrow, prepared_parquet
    canonical, parquet = prepared_arrow("source"), prepared_parquet("source")
    if prior:
        assert build.run_one(spec, strict=False)
    before = {p: p.read_bytes() for p in (canonical, parquet) if p.exists()}
    table = pa.table({"x": [2]})
    def produce(*args):
        if not streaming:
            return [("source", table)]
        with open_canonical_writer("source", table.schema) as writer:
            writer.write_table(table)
        return []
    monkeypatch.setattr(build, "transform", produce)
    with monkeypatch.context() as fault:
        if failure == "validation":
            fault.setattr(build, "validate", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("validation")))
        elif failure in {"exporter", "measurement"}:
            from dataclasses import replace
            exporter = type(get_exporter("parquet@py"))
            original = exporter.export
            def broken(*args):
                result = original(*args)
                if failure == "exporter":
                    raise RuntimeError("after replacement")
                return replace(result, compliance=replace(result.compliance, roundtrip=False))
            fault.setattr(exporter, "export", broken)
        # A writer's failure is recorded as the format's measurement and the
        # build succeeds without it; a failed validation fails the build.
        assert build.run_one(spec, strict=False) is (failure != "validation")
    # A validated canonical may commit before a later export fails. Failed
    # validation and failed exports may never replace an earlier good file.
    rollback = (canonical, parquet) if failure == "validation" else (parquet,)
    for path in rollback:
        if path in before:
            assert path.read_bytes() == before[path]
        else:
            assert not path.exists()
    # The identical retry: the failure it recorded is skipped unless retried.
    assert build.run_one(spec, strict=False, retry_errors=True)
    assert raincloud.load("source", format="parquet", config=cfg).to_arrow()["x"].to_pylist() == [2]


def mirror_fixture(tmp_path, checksum):
    _, options = create(tmp_path)
    mirror = Path(options["data_dir"])
    options.update(data_dir=str(tmp_path / "local"), mirror=str(mirror), offline=False)
    catalog = Path(options["catalog"])
    snapshot = json.loads((catalog / "snapshot.json").read_bytes())
    if checksum != "matching":
        snapshot["slugs"]["tiny"]["arrow_sha256"] = "0" * 64 if checksum == "wrong" else None
    bundle = make_bundle((catalog / "sources.json").read_bytes(), encode(snapshot), "reader-fixture")
    for name, raw in bundle.files().items():
        (catalog / name).write_bytes(raw)
    return options, Path(options["cache_dir"]) / "v2/tiny/arrow/tiny.arrow.zstd"


@pytest.mark.parametrize("checksum", ["matching", "missing", "wrong"])
def test_mirror_bytes_must_be_the_catalog_artifact(tmp_path, checksum):
    # The catalog is the authority: mirror bytes are checked as they arrive,
    # against its sha256, or its byte size when it records no sha.
    options, dest = mirror_fixture(tmp_path, checksum)
    cfg = raincloud.resolve_config(**options)
    if checksum == "wrong":
        with pytest.raises(raincloud.ChecksumMismatch, match="not the catalog's"):
            raincloud.load("tiny", format="arrow", config=cfg).path()
        assert not dest.exists() and not list(dest.parent.glob("*.part"))
        return
    assert raincloud.load("tiny", format="arrow", config=cfg).path() == dest
    offline = raincloud.resolve_config(**{**options, "offline": True})
    assert raincloud.load("tiny", format="arrow", config=offline).path() == dest


@pytest.mark.parametrize("checksum", ["matching", "wrong"])
def test_native_mirror_bytes_must_be_the_catalog_artifact(native, tmp_path, checksum):
    options, dest = mirror_fixture(tmp_path, checksum)
    handle = native.open(options, "arrow")
    try:
        if checksum == "wrong":
            with pytest.raises(NativeError) as error:
                native.string("raincloud_path", handle)
            assert error.value.code == 8  # checksum mismatch
            assert not dest.exists()
        else:
            assert Path(native.string("raincloud_path", handle)) == dest
    finally:
        native.close(handle)


@pytest.mark.parametrize("streaming", [False, True])
def test_partial_exports_keep_completed_formats_and_retry(isolated, monkeypatch, streaming):
    cfg, base_spec = isolated
    from dataclasses import replace

    from raincloud.catalogs import current, operation
    from raincloud.pipeline.spec import prepared_arrow, prepared_parquet, prepared_vortex
    spec = {**base_spec, "export": {"formats": ["parquet", "vortex"]}}
    old = current()
    manifest = old.manifest
    manifest["datasets"][0] = spec
    context = replace(old, bundle=make_bundle(encode(manifest), old.bundle.snapshot, old.bundle.catalog_id))
    if streaming:
        def produce(*args):
            table = pa.table({"x": [1]})
            with open_canonical_writer("source", table.schema) as writer:
                writer.write_table(table)
            return []
        monkeypatch.setattr(build, "transform", produce)
    with operation(cfg, context):
        with monkeypatch.context() as fault:
            exporter = type(get_exporter("vortex@py"))
            original = exporter.export
            def broken(*args):
                original(*args)
                raise RuntimeError("late exporter failure")
            fault.setattr(exporter, "export", broken)
            # Built with the formats that worked; Vortex is recorded unavailable.
            assert build.run_one(spec, strict=False)
        for path in (prepared_arrow("source"), prepared_parquet("source")):
            assert path.exists()
        assert not prepared_vortex("source").exists()
        assert build.run_one(spec, strict=False, retry_errors=True)
        assert raincloud.load("source", format="vortex", config=cfg).to_arrow()["x"].to_pylist() == [1]


def test_streaming_failure_after_canonical_write_rolls_back(isolated, monkeypatch):
    _, spec = isolated
    from raincloud.pipeline.spec import prepared_arrow
    original = build.transform
    def broken(*args):
        table = pa.table({"x": [1]})
        with open_canonical_writer("source", table.schema) as writer:
            writer.write_table(table)
        raise RuntimeError("handler failed after closing its writer")
    monkeypatch.setattr(build, "transform", broken)
    assert not build.run_one(spec, strict=False)
    assert not prepared_arrow("source").exists()
    monkeypatch.setattr(build, "transform", original)
    assert build.run_one(spec, strict=False)


def test_publication_copy_fallback_keeps_prior_bytes(tmp_path, monkeypatch):
    import os

    from raincloud._cache import Publication
    dest = tmp_path / "data"
    dest.write_bytes(b"original")
    before = dest.read_bytes(), dest.stat().st_mtime_ns
    monkeypatch.setattr(os, "link", lambda *a: (_ for _ in ()).throw(OSError("no hard links")))
    with pytest.raises(ValueError):
        with Publication(dest):
            tmp = tmp_path / "new"
            tmp.write_bytes(b"replacement")
            tmp.replace(dest)
            raise ValueError("reject")
    assert (dest.read_bytes(), dest.stat().st_mtime_ns) == before
    assert not list(tmp_path.glob("*.rollback"))


def test_refused_download_leaves_the_prior_artifact(tmp_path):
    from raincloud import _cache
    dest = tmp_path / "data"
    dest.write_bytes(b"old bytes")
    for expected_sha, expected_size in [("0" * 64, None), (None, 3)]:  # wrong sha; wrong size, no sha
        downloaded = tmp_path / "download"
        downloaded.write_bytes(b"new bytes")
        with pytest.raises(raincloud.ChecksumMismatch):
            _cache.adopt(downloaded, dest, expected_sha, expected_size=expected_size)
        assert dest.read_bytes() == b"old bytes" and not downloaded.exists()


def test_rollback_before_replacement_leaves_no_backup(tmp_path):
    # If the write fails before the new file replaces the old one, the backup
    # and the destination are one inode; rename() between them does nothing.
    from raincloud._cache import Publication
    dest = tmp_path / "data"
    dest.write_bytes(b"original")
    with pytest.raises(ValueError):
        with Publication(dest):
            raise ValueError("failed before replacing anything")
    assert dest.read_bytes() == b"original"
    assert [p.name for p in tmp_path.iterdir()] == ["data"]
