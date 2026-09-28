# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the hydrate stage.

Covers the pieces that don't require real HTTP: the URL filter, the
two-flag bypass guard, the blocklist loader, and deriving a hydrated
dataset's table with a dependency-injected fetcher. The full build of a
hydrated dataset from its parent is in test_artifact_lifecycle.py.

The fetcher is mocked — these tests never make outbound network calls.
"""
from __future__ import annotations

import argparse
import hashlib
from datetime import datetime, timezone

import pyarrow as pa
import pytest

from raincloud.pipeline.hydrate import (
    PROVENANCE_TYPE,
    FilterDecision,
    HydrateConfig,
    _empty_provenance,
    confirm_bypass,
    derive_tables,
    filter_url,
    load_blocklist,
)

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
    cfg = HydrateConfig(bypass_safety=True, risk_accepted=True)
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
    assert {"evil.example.com", "ads.example.com", "anothereviladnetwork.com"} <= out
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


def _hydrated_spec(columns, **hydrate):
    return {"slug": "tiny-hydrated", "advisory": "test fixture",
            "derive": {"from": "tiny", "hydrate": {"columns": columns, **hydrate}}}


def test_derive_appends_fetched_column_and_provenance(monkeypatch):
    """Parent rows kept; each hydrated column plus its provenance appended."""
    from raincloud.pipeline import hydrate
    table = pa.table({
        "id": pa.array([1, 2, 3, 4], type=pa.int32()),
        "url": pa.array([
            "https://ok.example.com/a",
            "javascript:bad",                     # blocked_scheme
            "http://blocked.example.com/x",       # per-dataset blocked
            "https://errors.example.com/y",       # fake fetch error
        ], type=pa.string()),
    })
    monkeypatch.setattr(hydrate, "_parent_table", lambda parent: table)
    spec = _hydrated_spec({"url": {"into": "content", "type": "binary"}},
                          blocked_hosts_extra=["blocked.example.com"])
    with hydrate.using(HydrateConfig(concurrency=2)):
        [(slug, result)] = derive_tables(spec, fetcher=_fake_fetch({"https://ok.example.com/a": b"hello"}))
    assert slug == "tiny-hydrated"
    assert result.column_names == ["id", "url", "content", "_content_provenance"]
    contents = result["content"].to_pylist()
    provs = result["_content_provenance"].to_pylist()
    assert contents[0] == b"hello"
    assert contents[1] is None and provs[1]["filter_decision"] == FilterDecision.BLOCKED_SCHEME
    assert contents[2] is None and provs[2]["filter_decision"] == FilterDecision.BLOCKED_BY_HOST
    assert contents[3] is None and provs[3]["filter_decision"] == FilterDecision.FETCH_ERROR
    assert provs[3]["http_status"] == 500


def test_derive_hydrates_several_columns_as_text(monkeypatch):
    from raincloud.pipeline import hydrate
    table = pa.table({"page": ["https://a.example.com/"], "cover": ["https://b.example.com/"]})
    monkeypatch.setattr(hydrate, "_parent_table", lambda parent: table)
    spec = _hydrated_spec({"page": {"into": "html", "type": "string"},
                           "cover": {"into": "image", "type": "binary"}})
    [(_, result)] = derive_tables(spec, fetcher=_fake_fetch({"https://a.example.com/": b"<p>",
                                                           "https://b.example.com/": b"\x89PNG"}))
    assert result["html"].to_pylist() == ["<p>"] and result.schema.field("html").type == pa.string()
    assert result["image"].to_pylist() == [b"\x89PNG"]
    assert {"_html_provenance", "_image_provenance"} <= set(result.column_names)


def test_derive_rejects_a_column_the_parent_does_not_have(monkeypatch):
    """Raise rather than silently produce an all-null hydrated copy."""
    from raincloud.pipeline import hydrate
    monkeypatch.setattr(hydrate, "_parent_table", lambda parent: pa.table({"id": [1, 2]}))
    with pytest.raises(ValueError, match="not a column of tiny"):
        derive_tables(_hydrated_spec({"nonexistent": {"into": "content", "type": "binary"}}),
                      fetcher=lambda *a: pytest.fail("no fetch for a bad recipe"))


def test_derive_refuses_a_dataset_that_is_not_hydrated():
    with pytest.raises(NotImplementedError):
        derive_tables({"slug": "x"})


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
