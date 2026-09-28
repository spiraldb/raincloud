# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for raincloud/pipeline/fetch.py.

Side-effect-free — no network. The HTTP client is monkeypatched so we can
inspect the SSL context that fetch.fetch_http would have passed to urllib.
"""
from __future__ import annotations

import io
from pathlib import Path

import pytest

from raincloud.pipeline import fetch as fetch_mod


class _FakeResponse:
    """Minimal context-manager stand-in for urllib's HTTPResponse."""

    def __init__(self, payload: bytes) -> None:
        self._buf = io.BytesIO(payload)

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, *exc) -> None:
        self._buf.close()

    def read(self, n: int = -1) -> bytes:
        return self._buf.read(n)


def _patch_urlopen(monkeypatch, payload: bytes, captured: dict) -> None:
    """Replace urllib.request.urlopen so we can inspect kwargs without a network call."""

    def fake_urlopen(req, **kwargs):
        captured["kwargs"] = kwargs
        captured["url"] = req.full_url if hasattr(req, "full_url") else str(req)
        return _FakeResponse(payload)

    monkeypatch.setattr(fetch_mod.urllib.request, "urlopen", fake_urlopen)


def _patch_originals_dir(monkeypatch, tmp_path: Path) -> None:
    scratch = tmp_path / "raw_downloads"
    scratch.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("RAINCLOUD_RAW_DOWNLOADS", str(scratch))
    # Disable the sibling-cache lookup so the test stays hermetic.
    monkeypatch.setattr(fetch_mod, "_find_sibling_cache", lambda *a, **kw: None)


def _base_spec(url: str) -> dict:
    return {
        "slug": "verify-tls-fixture",
        "fetch": {
            "type": "http",
            "urls": [url],
            "auth": None,
            "expected_bytes": None,
            "expected_sha256": None,
        },
    }


def test_fetch_http_always_uses_the_verifying_context(monkeypatch, tmp_path):
    """urlopen never receives a `context` kwarg, so urllib verifies certificates.

    There is no longer a way for a recipe to ask for anything else.
    """
    _patch_originals_dir(monkeypatch, tmp_path)
    captured: dict = {}
    _patch_urlopen(monkeypatch, b"payload-bytes", captured)

    out = fetch_mod.fetch_http(_base_spec("https://example.com/file.bin"))
    assert len(out) == 1 and out[0].read_bytes() == b"payload-bytes"
    assert "context" not in captured["kwargs"]


def test_fetch_http_refuses_a_recipe_asking_to_skip_verification(monkeypatch, tmp_path):
    """`fetch.verify_tls` is refused rather than ignored.

    Honouring it would give the recipe less protection than TLS promises;
    ignoring it silently would give more than it asked for without saying so.
    A catalog is shareable, so the recipe carrying it may not be the operator's.
    """
    _patch_originals_dir(monkeypatch, tmp_path)
    _patch_urlopen(monkeypatch, b"insecure-payload", {})

    spec = _base_spec("https://expired.example.com/file.bin")
    spec["fetch"]["verify_tls"] = False

    with pytest.raises(ValueError, match="no longer supported"):
        fetch_mod.fetch_http(spec)


def test_fetch_http_refuses_verify_tls_true_as_well(monkeypatch, tmp_path):
    """Even the harmless spelling is refused — the field is gone, not defaulted."""
    _patch_originals_dir(monkeypatch, tmp_path)
    _patch_urlopen(monkeypatch, b"verified", {})

    spec = _base_spec("https://example.com/file2.bin")
    spec["fetch"]["verify_tls"] = True

    with pytest.raises(ValueError, match="no longer supported"):
        fetch_mod.fetch_http(spec)
