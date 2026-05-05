# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the hydrate stage.

Covers the pieces that don't require real HTTP: the URL filter, the
two-flag bypass guard, the blocklist loader, and the end-to-end stage
with a dependency-injected fetcher.

The fetcher is mocked — these tests never make outbound network calls.
"""
from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.pipeline.hydrate import (
    PROVENANCE_TYPE,
    FilterDecision,
    HydrateConfig,
    _empty_provenance,
    confirm_bypass,
    filter_url,
    hydrate,
    load_blocklist,
)
from scripts.pipeline.spec import prepared_parquet, prepared_parquet_hydrated

# ---------- Filter ----------

def test_filter_blocks_non_http_schemes():
    cfg = HydrateConfig()
    assert filter_url("file:///etc/passwd", cfg) == (False, FilterDecision.BLOCKED_SCHEME)
    assert filter_url("data:text/plain,foo", cfg) == (False, FilterDecision.BLOCKED_SCHEME)
    assert filter_url("javascript:alert(1)", cfg) == (False, FilterDecision.BLOCKED_SCHEME)
    assert filter_url("ftp://example.com/x", cfg) == (False, FilterDecision.BLOCKED_SCHEME)


def test_filter_blocks_onion_hosts():
    cfg = HydrateConfig()
    assert filter_url("http://3g2upl4pq6kufc4m.onion/", cfg)[1] == FilterDecision.BLOCKED_SCHEME


def test_filter_allows_http_https():
    cfg = HydrateConfig()
    assert filter_url("http://example.com/x", cfg) == (True, FilterDecision.ALLOWED)
    assert filter_url("https://example.com/x?q=1", cfg) == (True, FilterDecision.ALLOWED)


def test_filter_blocks_listed_host():
    cfg = HydrateConfig(blocked_hosts=frozenset({"evil.example.com"}))
    assert filter_url("http://evil.example.com/x", cfg) == (False, FilterDecision.BLOCKED_BY_HOST)
    # Subdomain is NOT auto-blocked — caller must list each variant.
    assert filter_url("http://sub.evil.example.com/x", cfg)[0] is True


def test_filter_normalizes_host_case_and_port():
    cfg = HydrateConfig(blocked_hosts=frozenset({"evil.example.com"}))
    assert filter_url("http://EVIL.example.com:8080/x", cfg)[0] is False


def test_filter_handles_garbage_inputs():
    cfg = HydrateConfig()
    assert filter_url(None, cfg)[0] is False
    assert filter_url("", cfg)[0] is False
    assert filter_url("not a url", cfg)[0] is False


def test_filter_bypass_lets_anything_through():
    cfg = HydrateConfig(bypass_safety=True)
    assert filter_url("javascript:alert(1)", cfg) == (True, FilterDecision.ALLOWED_BYPASS)
    assert filter_url("http://evil.example.com/x", cfg) == (True, FilterDecision.ALLOWED_BYPASS)


# ---------- Blocklist loader ----------

def test_load_blocklist_handles_hosts_file_format(tmp_path):
    f = tmp_path / "block.txt"
    f.write_text(
        "# comment\n"
        "0.0.0.0 evil.example.com\n"
        "0.0.0.0 ads.example.com  # trailing comment\n"
        "127.0.0.1 localhost     # not a real host but still parsed\n"
        "\n"
        "anothereviladnetwork.com\n"
        "                  \n"  # whitespace-only
    )
    out = load_blocklist([f])
    assert "evil.example.com" in out
    assert "ads.example.com" in out
    assert "anothereviladnetwork.com" in out
    # `localhost` is filtered out by the loader's "host must contain a dot"
    # rule, which avoids accidentally banning bare-name typos.
    assert "localhost" not in out


# ---------- Bypass guard ----------

def test_bypass_requires_both_flags(capsys):
    args = argparse.Namespace(unsafe_allow_all_domains=True, i_accept_the_risk=False)
    assert confirm_bypass(args) is False
    err = capsys.readouterr().err
    assert "Refusing to bypass" in err

    args = argparse.Namespace(unsafe_allow_all_domains=True, i_accept_the_risk=True)
    assert confirm_bypass(args) is True

    args = argparse.Namespace(unsafe_allow_all_domains=False, i_accept_the_risk=True)
    assert confirm_bypass(args) is False


# ---------- End-to-end stage with mocked fetcher ----------

def _fake_fetch(success_urls: dict[str, bytes]):
    """Return a fetcher that returns canned bytes for known URLs and an error
    for everything else."""
    def fetcher(url, config):
        if url in success_urls:
            body = success_urls[url]
            prov = {
                "http_status": 200,
                "content_type": "application/octet-stream",
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
                "sha256": hashlib.sha256(body).digest(),
                "bytes_total": len(body),
                "filter_decision": FilterDecision.ALLOWED,
                "error": None,
            }
            return body, prov
        prov = _empty_provenance(FilterDecision.FETCH_ERROR, error="fake 500")
        prov["http_status"] = 500
        return None, prov
    return fetcher


def test_hydrate_writes_parquet_with_provenance(tmp_path, monkeypatch):
    """End-to-end stage with a fixture parquet + injected fetcher.

    Builds outputs in the real outputs/ tree under a test-only slug,
    then cleans up.
    """
    slug = "test-hydrate-pytest"
    base = prepared_parquet(slug)
    hydrated = prepared_parquet_hydrated(slug)
    base.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({
        "id": pa.array([1, 2, 3, 4], type=pa.int32()),
        "url": pa.array([
            "https://ok.example.com/a",
            "javascript:bad",                     # blocked_scheme
            "http://blocked.example.com/x",       # per-slug blocked
            "https://errors.example.com/y",       # fake fetch error
        ], type=pa.string()),
    })
    pq.write_table(table, base, compression="zstd")
    spec = {
        "slug": slug,
        "hydrate": {
            "url_column": "url",
            "output_column": "content",
            "output_type": "binary",
            "advisory": "test fixture",
            "blocked_hosts_extra": ["blocked.example.com"],
        },
    }
    fetcher = _fake_fetch({"https://ok.example.com/a": b"hello"})
    try:
        out = hydrate(spec, HydrateConfig(concurrency=2), fetcher=fetcher)
        assert out == hydrated
        result = pq.read_table(out)
        assert result.column_names == ["id", "url", "content", "_hydrate_provenance"]

        contents = result["content"].to_pylist()
        provs = result["_hydrate_provenance"].to_pylist()
        assert contents[0] == b"hello"
        assert contents[1] is None and provs[1]["filter_decision"] == FilterDecision.BLOCKED_SCHEME
        assert contents[2] is None and provs[2]["filter_decision"] == FilterDecision.BLOCKED_BY_HOST
        assert contents[3] is None and provs[3]["filter_decision"] == FilterDecision.FETCH_ERROR
        assert provs[3]["http_status"] == 500
    finally:
        if hydrated.exists():
            hydrated.unlink()
        if hydrated.parent.exists() and not any(hydrated.parent.iterdir()):
            hydrated.parent.rmdir()
        if base.exists():
            base.unlink()
        if base.parent.exists() and not any(base.parent.iterdir()):
            base.parent.rmdir()
        if base.parent.parent.exists() and not any(base.parent.parent.iterdir()):
            base.parent.parent.rmdir()


def test_hydrate_returns_none_when_no_hydrate_config():
    assert hydrate({"slug": "x"}) is None


def test_hydrate_raises_on_missing_url_column(tmp_path):
    """If hydrate.url_column references a column that doesn't exist in the
    base parquet, raise — don't silently produce an all-null hydrated copy."""
    slug = "test-hydrate-bad-col"
    base = prepared_parquet(slug)
    base.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table({"id": pa.array([1, 2], type=pa.int32())})
    pq.write_table(table, base)
    spec = {
        "slug": slug,
        "hydrate": {
            "url_column": "nonexistent",
            "output_column": "content",
            "output_type": "binary",
            "advisory": "test",
        },
    }
    try:
        with pytest.raises(ValueError, match="not in parquet"):
            hydrate(spec)
    finally:
        if base.exists():
            base.unlink()
        if base.parent.exists() and not any(base.parent.iterdir()):
            base.parent.rmdir()
        if base.parent.parent.exists() and not any(base.parent.parent.iterdir()):
            base.parent.parent.rmdir()


def test_provenance_struct_shape():
    """The PROVENANCE_TYPE matches the public docstring claim."""
    expected = [
        ("http_status", pa.int16()),
        ("content_type", pa.string()),
        ("fetched_at", pa.timestamp("s")),
        # binary, not fixed_size_binary(32), because vortex 0.69 doesn't
        # accept FixedSizeBinary types yet — see docs/v1/vortex_skip.md.
        ("sha256", pa.binary()),
        ("bytes_total", pa.int32()),
        ("filter_decision", pa.string()),
        ("error", pa.string()),
    ]
    actual = [(f.name, f.type) for f in PROVENANCE_TYPE]
    assert actual == expected
