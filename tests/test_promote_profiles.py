# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Mirror semantics for scripts.pipeline.promote_profiles.

Verifies the outputs/v{n}/<slug>/profile.json → docs/v{n}/profiles/<slug>.json
sync used to ship profile data to fresh clones. Runs entirely in tmp_path so
the real docs/v1/profiles/ tree is never touched.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def fake_repo(tmp_path, monkeypatch):
    """Redirect REPO_ROOT to tmp_path in both modules that read it."""
    from scripts.pipeline import promote_profiles, spec
    monkeypatch.setattr(spec, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(spec, "DEFAULT_MANIFEST", tmp_path / "sources.json")
    monkeypatch.setattr(promote_profiles, "REPO_ROOT", tmp_path)
    (tmp_path / "sources.json").write_text(
        json.dumps({"schema_version": 1, "datasets": [{"slug": "alpha"}, {"slug": "beta"}]})
    )
    return tmp_path


def _put_built_profile(repo: Path, slug: str, body: str) -> None:
    p = repo / "outputs" / "v1" / slug / "profile.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(body)


def test_promote_copies_built_profile_to_tracked_dir(fake_repo):
    from scripts.pipeline.promote_profiles import promote

    _put_built_profile(fake_repo, "alpha", '{"version": 1}')
    copied, skipped, missing = promote()
    assert copied == 1 and skipped == 0 and missing == []
    dst = fake_repo / "docs" / "v1" / "profiles" / "alpha.json"
    assert dst.read_text() == '{"version": 1}'


def test_promote_is_idempotent(fake_repo):
    from scripts.pipeline.promote_profiles import promote

    _put_built_profile(fake_repo, "alpha", '{"version": 1}')
    promote()
    copied, skipped, _ = promote()
    assert copied == 0 and skipped == 1


def test_promote_rewrites_when_source_changes(fake_repo):
    from scripts.pipeline.promote_profiles import promote

    _put_built_profile(fake_repo, "alpha", '{"version": 1}')
    promote()
    _put_built_profile(fake_repo, "alpha", '{"version": 2}')
    copied, skipped, _ = promote()
    assert copied == 1 and skipped == 0
    dst = fake_repo / "docs" / "v1" / "profiles" / "alpha.json"
    assert dst.read_text() == '{"version": 2}'


def test_promote_reports_named_slug_without_built_profile(fake_repo):
    from scripts.pipeline.promote_profiles import promote

    _put_built_profile(fake_repo, "alpha", '{"v": 1}')
    copied, skipped, missing = promote(slugs=["alpha", "beta"])
    assert copied == 1
    assert missing == ["beta"]


def test_promote_named_subset_skips_others(fake_repo):
    from scripts.pipeline.promote_profiles import promote

    _put_built_profile(fake_repo, "alpha", '{"v": 1}')
    _put_built_profile(fake_repo, "beta", '{"v": 1}')
    copied, _, _ = promote(slugs=["alpha"])
    assert copied == 1
    assert not (fake_repo / "docs" / "v1" / "profiles" / "beta.json").exists()


def test_promote_check_mode_does_not_write(fake_repo):
    from scripts.pipeline.promote_profiles import promote

    _put_built_profile(fake_repo, "alpha", '{"v": 1}')
    copied, _, _ = promote(check_only=True)
    assert copied == 1
    assert not (fake_repo / "docs" / "v1" / "profiles" / "alpha.json").exists()
