# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Hydrated datasets: a parent dataset with some URL columns fetched into bytes.

A hydrated dataset is its own catalog entry, `<parent>-hydrated`, declared as

    "derive": {"from": "<parent>",
               "hydrate": {"columns": {"<url column>": {"into": "<new column>",
                                                         "type": "binary" | "string"}}}},
    "advisory": "why you probably want the parent instead"

Building it reads the parent's canonical Arrow, dereferences every URL in each
listed column, and appends the fetched payload plus a provenance column; the
ordinary pipeline then writes its canonical Arrow and Parquet/Vortex exports,
so it loads like any dataset -- with a warning every time.

It is deliberately sketchy, and never built unless named:

  * No file-size guarantees. The parent stays a metadata index;
    hydrated copies can be 10x-1000x larger.
  * No reproducibility guarantees. Dereferencing arbitrary URLs is
    time-dependent (link rot, takedowns, content drift). Two runs days
    apart can produce different content.
  * No completeness guarantees. The dataset's URL column may already
    have stale entries, and this stage only widens the gap.
  * The fetched column is whatever bytes / text the URL returned.
    raincloud makes no claim about its safety, legality, or
    appropriateness. You consented to download it.

Each hydrated column `<into>` gets a `_<into>_provenance` struct recording what
happened:

    struct<
      http_status:     int16        # 0 if not attempted, else HTTP code
      content_type:    string
      fetched_at:      timestamp[s]
      sha256:          binary    # 32 bytes by construction; FSB(32) blocked vortex 0.69
      bytes_total:     int32
      filter_decision: string       # "allowed" | "blocked_*" | "fetch_error"
      error:           string       # null on success
    >

so absent fetched values are never silent — every null cell has
a provenance entry that explains why.

============================================================================
SAFETY FILTER
============================================================================
By default the stage rejects URLs that are obviously inappropriate to
fetch:

  1. Scheme allowlist (always on): only http / https. file://, data:,
     javascript:, ftp:, etc. are blocked.
  2. Per-dataset `derive.hydrate.blocked_hosts_extra`: hostnames the
     dataset author has pre-banned (manifest-level).
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
  raincloud build <parent>-hydrated                        # safe defaults
  python -m raincloud.pipeline.hydrate <parent>-hydrated   # same; <parent> works too
  python -m raincloud.pipeline.hydrate <slug> --limit 100  # first N rows (a sample)
  python -m raincloud.pipeline.hydrate --all               # every hydrated dataset

  An option that changes WHAT is fetched (--limit, --block, --urlhaus,
  --max-bytes or --timeout other than the default, or the unsafe bypass) makes
  the run a SAMPLE: its table is written to scratch (<scratch>/<slug>/sample/) and never
  published as the dataset, so `raincloud load` does not serve it.

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
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import pyarrow as pa

from .spec import display_path, is_hydrated, load_manifest, workdir_root

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
    BLOCKED_BY_HOST = "blocked_by_host"  # blocked_hosts, including the URLhaus list
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
    # Disabling the filter takes two deliberate acts, and the pairing is enforced
    # HERE rather than only in the CLI. The CLI's two flags never protected a
    # library caller: `HydrateConfig(bypass_safety=True)` turned off scheme
    # checks, host blocklists and the URLhaus list in one keyword. Hydrate
    # dereferences URLs out of a dataset, so "off" means fetching whatever an
    # upstream row happens to contain.
    risk_accepted: bool = False
    limit: int | None = None  # cap rows hydrated (testing / sampling)

    def __post_init__(self):
        if self.bypass_safety and not self.risk_accepted:
            raise ValueError(
                "bypass_safety requires risk_accepted=True: disabling the hydrate "
                "URL filter means fetching whatever the dataset's rows contain"
            )


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
_URLHAUS_TTL_S = 24 * 3600


def fetch_urlhaus_hostlist(*, force: bool = False) -> set[str]:
    """Download (or cache) the abuse.ch URLhaus hostfile. 24h TTL."""
    # Resolved lazily so $RAINCLOUD_WORKDIR / $RAINCLOUD_HOME are honored and
    # a wheel install doesn't try to write under site-packages at import time.
    cache = workdir_root() / ".urlhaus.hostfile"
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
    * the dataset author's `blocked_hosts_extra`

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

_ACTIVE: ContextVar[HydrateConfig | None] = ContextVar("raincloud_hydrate_config", default=None)


@contextmanager
def using(config: HydrateConfig):
    """Run the builds inside this block with `config` instead of the safe default."""
    token = _ACTIVE.set(config)
    try:
        yield
    finally:
        _ACTIVE.reset(token)


def _parent_table(parent: str) -> pa.Table:
    from raincloud import load
    from raincloud.config import get_config
    from raincloud.exceptions import ArtifactNotFound, OfflineMiss
    try:
        path = load(parent, format="arrow", config=get_config()).path()
    except (ArtifactNotFound, OfflineMiss) as exc:
        raise ArtifactNotFound(f"hydrating needs {parent} prepared first: raincloud build {parent}") from exc
    with pa.ipc.open_file(path) as reader:
        return reader.read_all()


def hydrate_column(urls: list, out_type: str, config: HydrateConfig, *, label: str,
                   fetcher: Callable[[str, HydrateConfig], tuple[bytes | None, dict]] | None = None,
                   ) -> tuple[pa.Array, pa.Array]:
    """Fetch every URL through the safety filter: (payload, provenance) arrays."""
    fetcher = fetcher or http_fetch
    n_total = len(urls)
    print(f"[hydrate] {label}  {n_total:,} URLs to consider")
    contents: list[bytes | str | None] = [None] * n_total
    provenances: list[dict] = [_empty_provenance(FilterDecision.BLOCKED_SCHEME)] * n_total

    def _process(i: int) -> tuple[int, bytes | str | None, dict]:
        url = urls[i]
        ok, decision = filter_url(url, config)
        if not ok:
            return i, None, _empty_provenance(decision)
        content, prov = fetcher(url, config)
        # If out_type is string, decode bytes as utf-8 (replace errors)
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
        FilterDecision.FETCH_ERROR: 0,
    }
    with ThreadPoolExecutor(max_workers=config.concurrency) as ex:
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
                      f"blocked={counts[FilterDecision.BLOCKED_BY_HOST] + counts[FilterDecision.BLOCKED_SCHEME]}  "
                      f"err={counts[FilterDecision.FETCH_ERROR]}")

    n_present = sum(1 for c in contents if c is not None)
    n_blocked = counts[FilterDecision.BLOCKED_SCHEME] + counts[FilterDecision.BLOCKED_BY_HOST]
    print(f"  {n_present:,} hydrated / {n_blocked:,} blocked / "
          f"{counts[FilterDecision.FETCH_ERROR]:,} errored / {n_total:,} total")
    content_arr = pa.array(contents, type=pa.binary() if out_type == "binary" else pa.string())
    return content_arr, pa.array(provenances, type=PROVENANCE_TYPE)


def derive_tables(
    spec: dict,
    *,
    fetcher: Callable[[str, HydrateConfig], tuple[bytes | None, dict]] | None = None,
) -> list[tuple[str, pa.Table]]:
    """The hydrated dataset's table: the parent's rows plus each fetched column.

    Called by the build in place of fetch/extract/parse/transform; the build
    then writes canonical Arrow and exports it like any other dataset.
    `fetcher` is dependency-injected for tests -- defaults to `http_fetch`.
    """
    if not is_hydrated(spec):
        raise NotImplementedError(f"{spec['slug']}: only hydrated datasets are derived")
    derive = spec["derive"]
    h = derive["hydrate"]
    config = _ACTIVE.get() or HydrateConfig()
    extra_blocked = set(h.get("blocked_hosts_extra") or [])
    config = dataclasses.replace(config, blocked_hosts=frozenset(config.blocked_hosts | extra_blocked))
    table = _parent_table(derive["from"])
    if config.limit is not None and config.limit < table.num_rows:
        table = table.slice(0, config.limit)
    for url_col, target in h["columns"].items():
        if url_col not in table.column_names:
            raise ValueError(
                f"{spec['slug']}: {url_col!r} is not a column of {derive['from']} "
                f"(columns: {table.column_names[:8]}...)"
            )
        content, provenance = hydrate_column(table[url_col].to_pylist(), target["type"], config,
                                             label=f"{spec['slug']}.{url_col}", fetcher=fetcher)
        table = table.append_column(target["into"], content).append_column(
            f"_{target['into']}_provenance", provenance)
    return [(spec["slug"], table)]


# ---------- CLI ----------

_DEFAULT_MAX_BYTES = HydrateConfig.max_bytes_per_row


def _sample_reasons(args: argparse.Namespace, bypass: bool) -> list[str]:
    """The options that make this run's table differ from the dataset's.

    `--timeout` counts: it decides which rows fetch successfully, so it changes
    the payload and provenance columns. `--concurrency` changes only how fast
    the same requests are made.
    """
    return [flag for flag, on in (
        ("--limit", args.limit is not None),
        ("--block", bool(args.block)),
        ("--urlhaus", args.urlhaus),
        ("--max-bytes", args.max_bytes != _DEFAULT_MAX_BYTES),
        ("--timeout", args.timeout != HydrateConfig.timeout_s),
        ("--unsafe-allow-all-domains", bypass),
    ) if on]


def _write_sample(spec: dict, config: HydrateConfig) -> bool:
    """Hydrate `spec` with `config` into scratch, never the dataset's store."""
    import uuid

    from .lifecycle import operation_lock
    try:
        with operation_lock(resources=True):
            with using(config):
                ((slug, table),) = derive_tables(spec)
            dest = workdir_root() / slug / "sample" / f"{slug}.arrow.zstd"
            dest.parent.mkdir(parents=True, exist_ok=True)
            tmp = dest.with_name(f".{dest.name}.{uuid.uuid4().hex}.tmp")
            try:
                with pa.OSFile(str(tmp), "wb") as sink, pa.ipc.new_file(
                        sink, table.schema, options=pa.ipc.IpcWriteOptions(compression="zstd")) as writer:
                    writer.write_table(table)
                tmp.replace(dest)
            finally:
                tmp.unlink(missing_ok=True)
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as e:  # noqa: BLE001 — one dataset's failure, as in build._run_one
        print(f"  FAILED: {spec['slug']}: {type(e).__name__}: {e}", file=sys.stderr)
        return False
    print(f"[hydrate] sample: {display_path(dest)} ({table.num_rows:,} rows) — not the "
          f"dataset; `raincloud load {slug}` does not serve it")
    return True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m raincloud.pipeline.hydrate",
        allow_abbrev=False,
        description=__doc__.strip().splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("slugs", nargs="*",
                    help="hydrated datasets to build (<parent>-hydrated, or just <parent>)")
    ap.add_argument("--all", action="store_true",
                    help="build every hydrated dataset")
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--timeout", type=float, default=HydrateConfig.timeout_s)
    ap.add_argument("--max-bytes", type=int, default=_DEFAULT_MAX_BYTES,
                    help="per-row payload cap in bytes (default 10 MB); another value "
                         "makes the run a sample")
    ap.add_argument("--limit", type=int, default=None,
                    help="hydrate only the first N rows, as a sample in scratch")
    ap.add_argument("--block", action="append", default=[],
                    help="path to a hostname blocklist (one host per line; "
                         "/etc/hosts-style 'IP host' lines accepted; "
                         "#-comments stripped). Repeatable. Makes the run a sample.")
    ap.add_argument("--urlhaus", action="store_true",
                    help="extend the blocklist with abuse.ch URLhaus hostfile "
                         "(fetched once, cached 24h under <scratch_dir>/.urlhaus.hostfile). "
                         "Makes the run a sample.")
    ap.add_argument("--unsafe-allow-all-domains", action="store_true",
                    help="DISABLE the safety filter for this run. Requires "
                         "--i-accept-the-risk to actually take effect. Makes the run a sample.")
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

    # HydrateConfig itself refuses bypass_safety without risk_accepted; the
    # acknowledgement comes from its own flag, not from the bypass decision.
    config = HydrateConfig(
        concurrency=args.concurrency,
        timeout_s=args.timeout,
        max_bytes_per_row=args.max_bytes,
        blocked_hosts=frozenset(blocked),
        bypass_safety=bypass, risk_accepted=args.i_accept_the_risk,
        limit=args.limit,
    )

    from raincloud.catalogs import operation
    from raincloud.config import get_config

    from .build import run_one
    from .selection import select_or_exit
    with operation(get_config()):
        m = load_manifest()
        by_slug = {d["slug"]: d for d in m["datasets"]}

        def alias(name: str) -> str:
            # The bare parent names its hydrated dataset.
            hydrated = f"{name}-hydrated"
            if not is_hydrated(by_slug.get(name, {})) and is_hydrated(by_slug.get(hydrated, {})):
                return hydrated
            return name

        selected = select_or_exit(ap, m, [alias(s) for s in args.slugs], all_=args.all,
                                  include_hydrated=True, quiet=True)
        if args.all:
            selected = [d for d in selected if is_hydrated(d)]
        plain = [d["slug"] for d in selected if not is_hydrated(d)]
        if plain:
            ap.exit(2, f"{ap.prog}: not a hydrated dataset: {', '.join(plain)} "
                       "(build it with `raincloud build`)\n")
        if not selected:
            ap.exit(2, f"{ap.prog}: the catalog has no hydrated datasets\n")
        sample = _sample_reasons(args, bypass)
        if sample:
            print(f"[hydrate] {', '.join(sample)} change what is fetched: writing a sample "
                  "to scratch; the dataset is left as it is", file=sys.stderr)
            results = [_write_sample(spec, config) for spec in selected]
        else:
            with using(config):
                results = [run_one(spec, strict=False) for spec in selected]
    print(f"\nsummary: ok={sum(results)}  failed={len(results) - sum(results)}")
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
