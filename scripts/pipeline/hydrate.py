# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 8 (optional, opt-in) — hydrate URL columns into bytes / strings.

For specs with `hydrate` set in the manifest, this stage walks the base
parquet's URL column, dereferences each URL over HTTP, and writes a
sibling parquet at:

    outputs/v{n}/<slug>/parquet-hydrated/<slug>.parquet

The hydrated copy is a SECOND, deliberately sketchy artefact tier:

  * No file-size guarantees. The base slug stays a metadata index;
    hydrated copies can be 10×–1000× larger.
  * No reproducibility guarantees. Dereferencing arbitrary URLs is
    time-dependent (link rot, takedowns, content drift). Two runs days
    apart can produce different content.
  * No completeness guarantees. The dataset's URL column may already
    have stale entries, and this stage only widens the gap.
  * The `<output_column>` is whatever bytes / text the URL returned.
    raincloud makes no claim about its safety, legality, or
    appropriateness. You consented to download it.

Each row carries a `_hydrate_provenance` struct recording what happened:

    struct<
      http_status:     int16        # 0 if not attempted, else HTTP code
      content_type:    string
      fetched_at:      timestamp[s]
      sha256:          binary    # 32 bytes by construction; FSB(32) blocked vortex 0.69
      bytes_total:     int32
      filter_decision: string       # "allowed" | "blocked_*" | "fetch_error"
      error:           string       # null on success
    >

so absent `<output_column>` values are never silent — every null cell has
a provenance entry that explains why.

============================================================================
SAFETY FILTER
============================================================================
By default the stage rejects URLs that are obviously inappropriate to
fetch:

  1. Scheme allowlist (always on): only http / https. file://, data:,
     javascript:, ftp:, etc. are blocked.
  2. Per-slug `hydrate.blocked_hosts_extra`: hostnames the dataset
     author has pre-banned (manifest-level).
  3. Per-run `--block FILE`: additional hosts you supply (e.g. piped
     from StevenBlack/hosts, your corp DNS list, an IWF feed if you have
     access).
  4. `--urlhaus` (opt-in): fetch abuse.ch URLhaus's hostfile at run
     start, cache for 24h. Covers active malware. Off by default
     because it adds a network dependency at hydrate-start.

raincloud SHIPS THE MECHANISM, NOT THE POLICY. We don't bundle a static
"unsafe" list — they go stale, can't cover every category (e.g. CSAM
lists like IWF aren't publicly distributable), and our editorial
choices won't match yours. Plug in the upstream filter sources you
trust, or run hydration behind a DNS-filtered network (CleanBrowsing,
Quad9, Cloudflare 1.1.1.2).

To bypass the filter entirely, both flags are required:

    --unsafe-allow-all-domains --i-accept-the-risk

This is intentional — a single-flag accident is impossible. A multi-line
warning prints regardless.

============================================================================
USAGE
============================================================================
  python -m scripts.pipeline.hydrate <slug>             # one slug
  python -m scripts.pipeline.hydrate <slug> --limit 100 # first N rows (testing)
  python -m scripts.pipeline.hydrate --all              # every spec with hydrate

  Filter:
    --block FILE                 add hosts from FILE (one per line; #-comments ok)
    --urlhaus                    pull abuse.ch URLhaus hostfile (default off)
    --unsafe-allow-all-domains --i-accept-the-risk
                                 disable the filter entirely (strong warning)

  Fetcher:
    --concurrency N              parallel workers (default 8)
    --timeout SEC                per-request timeout (default 30)
    --max-bytes N                cap per-row payload (default 10 MB)

============================================================================
"""
from __future__ import annotations

import argparse
import dataclasses
import hashlib
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from .spec import (
    REPO_ROOT,
    iter_datasets,
    load_manifest,
    prepared_parquet,
    prepared_parquet_hydrated,
    spec_field,
)

# ---------- Provenance schema ----------

PROVENANCE_TYPE = pa.struct([
    pa.field("http_status", pa.int16()),
    pa.field("content_type", pa.string()),
    pa.field("fetched_at", pa.timestamp("s")),
    # `sha256` is always 32 bytes by construction, but stored as plain
    # `binary` rather than `fixed_size_binary[32]`: vortex 0.69 does not
    # accept any FixedSizeBinary type yet (see docs/v1/vortex_skip.md), so
    # using FSB here would block vortex conversion of every hydrated
    # parquet. The contract is preserved by the writer; only the type
    # annotation is loosened.
    pa.field("sha256", pa.binary()),
    pa.field("bytes_total", pa.int32()),
    pa.field("filter_decision", pa.string()),
    pa.field("error", pa.string()),
])


class FilterDecision:
    ALLOWED = "allowed"
    ALLOWED_BYPASS = "allowed_bypass"
    BLOCKED_SCHEME = "blocked_scheme"
    BLOCKED_BY_HOST = "blocked_by_host"
    BLOCKED_BY_URLHAUS = "blocked_by_urlhaus"
    FETCH_ERROR = "fetch_error"


@dataclasses.dataclass
class HydrateConfig:
    """Per-run options for the hydrate stage."""
    concurrency: int = 8
    timeout_s: float = 30.0
    max_bytes_per_row: int = 10 * 1024 * 1024  # 10 MB
    user_agent: str = "raincloud-hydrate/0.1"
    blocked_hosts: frozenset[str] = frozenset()
    bypass_safety: bool = False
    limit: int | None = None  # cap rows hydrated (testing / sampling)


# ---------- Filter ----------

_ALLOWED_SCHEMES = {"http", "https"}


def _normalize_host(host: str) -> str:
    """Lowercase, strip port + brackets. Refuses .onion (no DNS in stdlib)."""
    h = host.strip().lower()
    if h.startswith("[") and "]" in h:  # IPv6
        h = h[1:h.index("]")]
    if ":" in h and not h.startswith("["):
        h = h.split(":", 1)[0]
    return h


def filter_url(url: str | None, config: HydrateConfig) -> tuple[bool, str]:
    """Return (allowed: bool, decision: str). The decision string is one of
    the FilterDecision constants — recorded into per-row provenance."""
    if config.bypass_safety:
        return True, FilterDecision.ALLOWED_BYPASS
    if url is None or not isinstance(url, str) or not url.strip():
        return False, FilterDecision.BLOCKED_SCHEME
    try:
        parsed = urllib.parse.urlparse(url)
    except ValueError:
        return False, FilterDecision.BLOCKED_SCHEME
    if parsed.scheme.lower() not in _ALLOWED_SCHEMES:
        return False, FilterDecision.BLOCKED_SCHEME
    host = _normalize_host(parsed.netloc)
    if not host or host.endswith(".onion"):
        return False, FilterDecision.BLOCKED_SCHEME
    if host in config.blocked_hosts:
        return False, FilterDecision.BLOCKED_BY_HOST
    return True, FilterDecision.ALLOWED


def load_blocklist(paths: list[Path]) -> set[str]:
    """Load hostnames from one or more text files (one per line, # comments).
    Lines that look like /etc/hosts entries (`0.0.0.0 evil.example`) have
    their leading IP stripped."""
    out: set[str] = set()
    for path in paths:
        for raw in Path(path).read_text(errors="replace").splitlines():
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            tokens = line.split()
            host = tokens[-1] if len(tokens) > 1 else tokens[0]
            host = _normalize_host(host)
            if host and "." in host:
                out.add(host)
    return out


_URLHAUS_URL = "https://urlhaus.abuse.ch/downloads/hostfile/"
_URLHAUS_CACHE = REPO_ROOT / "_workdir" / ".urlhaus.hostfile"
_URLHAUS_TTL_S = 24 * 3600


def fetch_urlhaus_hostlist(*, force: bool = False) -> set[str]:
    """Download (or cache) the abuse.ch URLhaus hostfile. 24h TTL."""
    cache = _URLHAUS_CACHE
    cache.parent.mkdir(parents=True, exist_ok=True)
    if cache.exists() and not force:
        age = time.time() - cache.stat().st_mtime
        if age < _URLHAUS_TTL_S:
            return load_blocklist([cache])
    try:
        req = urllib.request.Request(
            _URLHAUS_URL, headers={"User-Agent": "raincloud-hydrate/0.1"}
        )
        with urllib.request.urlopen(req, timeout=30) as r:
            cache.write_bytes(r.read())
    except Exception as e:
        print(f"  [urlhaus] fetch failed ({type(e).__name__}: {e}); using cache if any",
              file=sys.stderr)
        if not cache.exists():
            return set()
    return load_blocklist([cache])


# ---------- Fetcher ----------

def _empty_provenance(decision: str, *, error: str | None = None) -> dict:
    """Provenance row for a non-attempted (filtered) URL."""
    return {
        "http_status": 0,
        "content_type": "",
        "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "sha256": b"\x00" * 32,
        "bytes_total": 0,
        "filter_decision": decision,
        "error": error,
    }


def http_fetch(url: str, config: HydrateConfig) -> tuple[bytes | None, dict]:
    """Fetch URL with retries; return (content_bytes_or_None, provenance_dict).
    Caps body length at config.max_bytes_per_row (returns truncated bytes +
    error="truncated" in provenance)."""
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": config.user_agent})
            with urllib.request.urlopen(req, timeout=config.timeout_s) as r:
                content_type = r.headers.get("Content-Type", "") or ""
                # Read in chunks; bail past max
                buf = bytearray()
                truncated = False
                while True:
                    chunk = r.read(1 << 16)
                    if not chunk:
                        break
                    if len(buf) + len(chunk) > config.max_bytes_per_row:
                        buf.extend(chunk[: config.max_bytes_per_row - len(buf)])
                        truncated = True
                        break
                    buf.extend(chunk)
                content = bytes(buf)
            sha = hashlib.sha256(content).digest()
            prov = {
                "http_status": int(r.status if hasattr(r, "status") else 200),
                "content_type": content_type,
                "fetched_at": datetime.now(timezone.utc).replace(tzinfo=None),
                "sha256": sha,
                "bytes_total": len(content),
                "filter_decision": FilterDecision.ALLOWED,
                "error": "truncated" if truncated else None,
            }
            return content, prov
        except urllib.error.HTTPError as e:
            # Non-retryable on most HTTP error codes; record and bail
            prov = _empty_provenance(FilterDecision.FETCH_ERROR,
                                      error=f"HTTP {e.code}")
            prov["http_status"] = int(e.code)
            return None, prov
        except Exception as e:
            last_exc = e
            if attempt < 2:
                time.sleep(0.5 * (2 ** attempt))
            continue
    prov = _empty_provenance(
        FilterDecision.FETCH_ERROR,
        error=f"{type(last_exc).__name__}: {str(last_exc)[:200]}" if last_exc else "unknown",
    )
    return None, prov


# ---------- Bypass guard ----------

_BYPASS_BANNER = """\

  ⚠⚠⚠  --unsafe-allow-all-domains is set  ⚠⚠⚠

  The hydrate stage's safety filter is DISABLED for this run. Every URL
  in the configured column will be dereferenced regardless of:
    * scheme (http, https — and now also file:, data:, ftp:, ...)
    * hostname (your blocklists are ignored; URLhaus is ignored)
    * the dataset author's per-slug `blocked_hosts_extra`

  This is intended for narrow research use against URL columns you have
  separately verified. It is NOT appropriate for general-purpose
  hydration of public-web scrape datasets.

  raincloud makes no claim about the safety, legality, or
  appropriateness of any bytes you fetch under this flag. You are
  consenting to download whatever the URL returns.

"""


def confirm_bypass(args: argparse.Namespace) -> bool:
    """Returns True only if both --unsafe-allow-all-domains and
    --i-accept-the-risk are set. Prints the warning either way."""
    if not args.unsafe_allow_all_domains:
        return False
    print(_BYPASS_BANNER, file=sys.stderr)
    if not args.i_accept_the_risk:
        print(
            "  Refusing to bypass without --i-accept-the-risk. Aborting.\n",
            file=sys.stderr,
        )
        return False
    print("  --i-accept-the-risk acknowledged. Proceeding without filter.\n",
          file=sys.stderr)
    return True


# ---------- Stage ----------

def hydrate(
    spec: dict,
    config: HydrateConfig | None = None,
    *,
    fetcher: Callable[[str, HydrateConfig], tuple[bytes | None, dict]] | None = None,
) -> Path | None:
    """Build the hydrated parquet for `spec`. Returns the output path on
    success, None when the spec doesn't opt in via `hydrate`.

    `fetcher` is dependency-injected for tests — defaults to `http_fetch`.
    """
    if not spec.get("hydrate"):
        return None
    config = config or HydrateConfig()
    fetcher = fetcher or http_fetch

    h = spec["hydrate"]
    url_col = h["url_column"]
    out_col = h["output_column"]
    out_type = h["output_type"]
    extra_blocked = set(h.get("blocked_hosts_extra") or [])
    full_blocked = frozenset(config.blocked_hosts | extra_blocked)
    cfg = dataclasses.replace(config, blocked_hosts=full_blocked)

    base = prepared_parquet(spec["slug"])
    if not base.exists():
        raise FileNotFoundError(f"base parquet missing: {base.relative_to(REPO_ROOT)}")

    table = pq.read_table(base)
    if url_col not in table.column_names:
        raise ValueError(
            f"{spec['slug']}: hydrate.url_column={url_col!r} not in parquet "
            f"(columns: {table.column_names[:8]}...)"
        )
    urls: list[str | None] = table[url_col].to_pylist()
    n_total = len(urls)
    if cfg.limit is not None and cfg.limit < n_total:
        urls = urls[: cfg.limit]
        n_total = cfg.limit
        # We'll write a row-bounded hydrated parquet; trim base to match.
        table = table.slice(0, n_total)

    print(f"[hydrate] {spec['slug']}  {n_total:,} URLs to consider")
    contents: list[bytes | str | None] = [None] * n_total
    provenances: list[dict] = [_empty_provenance(FilterDecision.BLOCKED_SCHEME)] * n_total

    def _process(i: int) -> tuple[int, bytes | str | None, dict]:
        url = urls[i]
        ok, decision = filter_url(url, cfg)
        if not ok:
            return i, None, _empty_provenance(decision)
        content, prov = fetcher(url, cfg)
        # If output_type is string, decode bytes as utf-8 (replace errors)
        if content is not None and out_type == "string":
            try:
                content = content.decode("utf-8", errors="replace")
            except Exception as e:
                prov["error"] = f"decode: {type(e).__name__}: {str(e)[:120]}"
                content = None
        return i, content, prov

    # Process in parallel with a bounded pool. Order is preserved by i.
    n_done = 0
    counts = {
        FilterDecision.ALLOWED: 0,
        FilterDecision.ALLOWED_BYPASS: 0,
        FilterDecision.BLOCKED_SCHEME: 0,
        FilterDecision.BLOCKED_BY_HOST: 0,
        FilterDecision.BLOCKED_BY_URLHAUS: 0,
        FilterDecision.FETCH_ERROR: 0,
    }
    with ThreadPoolExecutor(max_workers=cfg.concurrency) as ex:
        futures = [ex.submit(_process, i) for i in range(n_total)]
        for f in as_completed(futures):
            i, content, prov = f.result()
            contents[i] = content
            provenances[i] = prov
            counts[prov["filter_decision"]] = counts.get(prov["filter_decision"], 0) + 1
            n_done += 1
            if n_done % max(1, n_total // 20) == 0:
                pct = n_done / n_total * 100
                print(f"  [{n_done:,}/{n_total:,}] {pct:.0f}%  "
                      f"allowed={counts[FilterDecision.ALLOWED]}  "
                      f"blocked={counts[FilterDecision.BLOCKED_BY_HOST] + counts[FilterDecision.BLOCKED_BY_URLHAUS] + counts[FilterDecision.BLOCKED_SCHEME]}  "
                      f"err={counts[FilterDecision.FETCH_ERROR]}")

    # Build hydrated table.
    if out_type == "binary":
        content_arr = pa.array(contents, type=pa.binary())
    else:
        content_arr = pa.array(contents, type=pa.string())
    prov_arr = pa.array(provenances, type=PROVENANCE_TYPE)
    hydrated = table.append_column(out_col, content_arr).append_column(
        "_hydrate_provenance", prov_arr
    )

    out = prepared_parquet_hydrated(spec["slug"])
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(hydrated, out, compression="zstd")

    n_present = sum(1 for c in contents if c is not None)
    n_blocked = (counts[FilterDecision.BLOCKED_SCHEME]
                 + counts[FilterDecision.BLOCKED_BY_HOST]
                 + counts[FilterDecision.BLOCKED_BY_URLHAUS])
    print(f"  wrote {out.relative_to(REPO_ROOT)}  "
          f"({n_present:,} hydrated / {n_blocked:,} blocked / "
          f"{counts[FilterDecision.FETCH_ERROR]:,} errored / {n_total:,} total)")

    # Auto-convert to vortex-hydrated/ if the spec opts in via convert.vortex.
    # Mirrors how `build` runs `convert` after `write`. Failures here don't
    # invalidate the hydrated parquet — log and continue.
    if spec_field(spec, "convert.vortex", False):
        from .convert import convert_hydrated
        try:
            convert_hydrated(spec)
        except BaseException as e:
            if isinstance(e, (KeyboardInterrupt, SystemExit)):
                raise
            msg = str(e).splitlines()[0] if str(e) else ""
            print(f"  [convert-hydrated fail] {type(e).__name__}: {msg}",
                  file=sys.stderr)

    return out


# ---------- CLI ----------

def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__.strip().splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("slugs", nargs="*", help="specific slugs to hydrate")
    ap.add_argument("--all", action="store_true",
                    help="hydrate every spec with `hydrate` set")
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--max-bytes", type=int, default=10 * 1024 * 1024,
                    help="per-row payload cap in bytes (default 10 MB)")
    ap.add_argument("--limit", type=int, default=None,
                    help="cap rows hydrated (testing / sampling)")
    ap.add_argument("--block", action="append", default=[],
                    help="path to a hostname blocklist (one host per line; "
                         "/etc/hosts-style 'IP host' lines accepted; "
                         "#-comments stripped). Repeatable.")
    ap.add_argument("--urlhaus", action="store_true",
                    help="extend the blocklist with abuse.ch URLhaus hostfile "
                         "(fetched once, cached 24h under _workdir/.urlhaus.hostfile)")
    ap.add_argument("--unsafe-allow-all-domains", action="store_true",
                    help="DISABLE the safety filter for this run. Requires "
                         "--i-accept-the-risk to actually take effect.")
    ap.add_argument("--i-accept-the-risk", action="store_true",
                    help="companion flag for --unsafe-allow-all-domains.")
    args = ap.parse_args(argv)

    bypass = confirm_bypass(args)
    if args.unsafe_allow_all_domains and not args.i_accept_the_risk:
        return 2

    block_paths = [Path(p) for p in args.block]
    blocked = load_blocklist(block_paths) if block_paths else set()
    if args.urlhaus:
        print("[urlhaus] loading abuse.ch hostlist", file=sys.stderr)
        blocked |= fetch_urlhaus_hostlist()
    print(f"[filter] {len(blocked):,} hostnames in blocklist "
          f"(scheme allowlist={'OFF' if bypass else 'on'})", file=sys.stderr)

    config = HydrateConfig(
        concurrency=args.concurrency,
        timeout_s=args.timeout,
        max_bytes_per_row=args.max_bytes,
        blocked_hosts=frozenset(blocked),
        bypass_safety=bypass,
        limit=args.limit,
    )

    m = load_manifest()
    selected: list[dict] = []
    if args.slugs:
        for s in args.slugs:
            selected += list(iter_datasets(m, slug=s))
    if args.all:
        selected = [d for d in iter_datasets(m) if d.get("hydrate")]
    if not selected:
        print("no slugs selected; pass <slug>... or --all", file=sys.stderr)
        return 2

    n_ok = n_skipped = n_failed = 0
    for spec in selected:
        if not spec.get("hydrate"):
            print(f"  [skip] {spec['slug']}: no hydrate config")
            n_skipped += 1
            continue
        try:
            hydrate(spec, config)
            n_ok += 1
        except Exception as e:
            print(f"  [fail] {spec['slug']}: {type(e).__name__}: {e}", file=sys.stderr)
            n_failed += 1

    print(f"\nsummary: ok={n_ok}  skipped={n_skipped}  failed={n_failed}")
    return 0 if n_failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
