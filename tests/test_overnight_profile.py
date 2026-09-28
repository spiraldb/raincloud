# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The overnight driver's wipe must never delete a frozen version's artifacts.

`outputs_root()` is scoped off the manifest's `schema_version`, so a v1-pinned
manifest would aim the disk-hygiene wipe at `outputs/v1/<slug>/` — artifacts a
later schema_version has superseded and that nothing rebuilds. Safety here used
to be incidental (it held only because `schema_version` happened to be current);
these tests make it a contract.
"""
from __future__ import annotations

from raincloud.pipeline import overnight_profile as op


def _wire(monkeypatch, tmp_path, current: str):
    """Point the module's roots at tmp_path with `current` as the active version."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path))
    monkeypatch.setenv("RAINCLOUD_CATALOG", "checkout")
    base = tmp_path / "outputs"
    monkeypatch.setattr(op, "outputs_base", lambda: base)
    monkeypatch.setattr(op, "outputs_root", lambda: base / current)
    monkeypatch.setattr(op, "raw_downloads_root", lambda: base / "raw_downloads")
    monkeypatch.setattr(op, "workdir_root", lambda: tmp_path / "_workdir")
    monkeypatch.setattr(op, "_log", lambda event: None)
    return base


def test_older_version_is_frozen_by_newer_sibling(monkeypatch, tmp_path):
    base = _wire(monkeypatch, tmp_path, "v1")
    (base / "v1").mkdir(parents=True)
    (base / "v2").mkdir(parents=True)
    reason = op.frozen_version_reason()
    assert reason is not None
    assert "v1 is frozen" in reason and "v2" in reason


def test_newest_version_is_not_frozen(monkeypatch, tmp_path):
    base = _wire(monkeypatch, tmp_path, "v2")
    (base / "v1").mkdir(parents=True)
    (base / "v2").mkdir(parents=True)
    assert op.frozen_version_reason() is None


def test_single_version_is_not_frozen(monkeypatch, tmp_path):
    base = _wire(monkeypatch, tmp_path, "v2")
    (base / "v2").mkdir(parents=True)
    assert op.frozen_version_reason() is None


def test_wipe_spares_frozen_outputs_but_still_clears_scratch(monkeypatch, tmp_path):
    base = _wire(monkeypatch, tmp_path, "v1")
    frozen = base / "v1" / "uci-iris"
    frozen.mkdir(parents=True)
    (frozen / "parquet").mkdir()
    (frozen / "parquet" / "uci-iris.parquet").write_bytes(b"v1 artifact")
    (base / "v2").mkdir(parents=True)
    raw = base / "raw_downloads" / "uci-iris"
    raw.mkdir(parents=True)
    (raw / "upstream.csv").write_bytes(b"raw")
    wd = tmp_path / "_workdir" / "uci-iris"
    wd.mkdir(parents=True)
    (wd / "scratch.tmp").write_bytes(b"scratch")

    op._wipe_slug("uci-iris")

    # the frozen version's artifacts survive ...
    assert (frozen / "parquet" / "uci-iris.parquet").read_bytes() == b"v1 artifact"
    # ... while unversioned scratch is still reclaimed
    assert not raw.exists()
    assert not wd.exists()


def test_wipe_removes_current_version_outputs(monkeypatch, tmp_path):
    base = _wire(monkeypatch, tmp_path, "v2")
    cur = base / "v2" / "uci-iris"
    cur.mkdir(parents=True)
    (cur / "uci-iris.parquet").write_bytes(b"current")
    (base / "v1").mkdir(parents=True)

    op._wipe_slug("uci-iris")

    assert not cur.exists()
    # the older version was never a target of this wipe
    assert (base / "v1").exists()
