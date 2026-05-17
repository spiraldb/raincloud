# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for scripts/pipeline/fetch.py.

Side-effect-free — no network. The HTTP client is monkeypatched so we can
inspect the SSL context that fetch.fetch_http would have passed to urllib.
"""
from __future__ import annotations

import io
import ssl
from pathlib import Path

import pytest

from scripts.pipeline import fetch as fetch_mod
from scripts.pipeline.spec import REPO_ROOT


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
    # fetch.py prints dest.relative_to(REPO_ROOT), so the scratch dir must live
    # under REPO_ROOT. Use a unique pytest-tmp subdir inside the repo's gitignored
    # _workdir/ area so we stay hermetic without breaking the relative_to call.
    # Fresh dir per test invocation — fetch.py treats a present file as cached.
    import shutil
    scratch = REPO_ROOT / "_workdir" / "test_fetch_scratch" / tmp_path.name
    if scratch.exists():
        shutil.rmtree(scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(fetch_mod, "ORIGINALS_DIR", scratch)
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


def test_fetch_http_default_uses_verified_context(monkeypatch, tmp_path):
    """When verify_tls is unset, urlopen receives no context kwarg (default verification)."""
    _patch_originals_dir(monkeypatch, tmp_path)
    captured: dict = {}
    _patch_urlopen(monkeypatch, b"payload-bytes", captured)

    spec = _base_spec("https://example.com/file.bin")

    out = fetch_mod.fetch_http(spec)
    assert len(out) == 1 and out[0].read_bytes() == b"payload-bytes"

    # No `context=` kwarg means urllib uses its default verifying SSL context.
    assert "context" not in captured["kwargs"]


def test_fetch_http_verify_tls_false_passes_unverified_context(monkeypatch, tmp_path):
    """When verify_tls=False, urlopen receives an SSL context with verification disabled."""
    _patch_originals_dir(monkeypatch, tmp_path)
    captured: dict = {}
    _patch_urlopen(monkeypatch, b"insecure-payload", captured)

    spec = _base_spec("https://expired.example.com/file.bin")
    spec["fetch"]["verify_tls"] = False

    out = fetch_mod.fetch_http(spec)
    assert len(out) == 1 and out[0].read_bytes() == b"insecure-payload"

    ctx = captured["kwargs"].get("context")
    assert isinstance(ctx, ssl.SSLContext), f"expected an SSLContext, got {type(ctx).__name__}"
    assert ctx.check_hostname is False
    assert ctx.verify_mode == ssl.CERT_NONE


def test_fetch_http_verify_tls_true_explicit_uses_verified_context(monkeypatch, tmp_path):
    """Explicitly setting verify_tls=True must match the default path (no context kwarg)."""
    _patch_originals_dir(monkeypatch, tmp_path)
    captured: dict = {}
    _patch_urlopen(monkeypatch, b"verified", captured)

    spec = _base_spec("https://example.com/file2.bin")
    spec["fetch"]["verify_tls"] = True

    fetch_mod.fetch_http(spec)
    assert "context" not in captured["kwargs"]
