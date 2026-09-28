# Hydration policy

A **hydrated dataset** is a parent dataset with some of its URL columns fetched from the open web. It is its own catalog entry, named `<parent>-hydrated`, declared with a `derive` block (`from` the parent, which columns to fetch `into` what) and an `advisory`. Building it reads the parent's canonical Arrow, fetches every URL through the filter below, and writes the result like any other dataset — canonical Arrow plus Parquet/Vortex exports — so `raincloud.load("laion-400m-hydrated")` works. It warns every time, because you probably want the parent.

Hydrated datasets are never built unless named: `raincloud build <parent>-hydrated` uses the safe defaults, and `python -m raincloud.pipeline.hydrate <parent>-hydrated` takes the options below (`hydrate <parent>` names the same dataset). Options that change what is fetched — `--limit`, `--block`, `--urlhaus`, a non-default `--max-bytes` or `--timeout`, or the bypass — make the run a **sample**: its table is written to `<scratch_dir>/<slug>/sample/<slug>.arrow.zstd` and is never published, recorded in the build record, or served by `raincloud.load()`. Only a run with the safe defaults (`raincloud build <parent>-hydrated`, or `hydrate` without those options) produces the dataset. Spell the flags out in full; abbreviations are refused. In a terminal, `raincloud list` marks them `[hydrated]` (piped output is bare slugs); `raincloud list --hydrate` lists only them.

This document is hand-maintained.

## What we provide vs. what you provide

Raincloud ships the **mechanism**, not the **policy**. Unless the bypass below is active, the hydrate stage filters URLs through:

1. **A scheme allowlist** — only `http` and `https`. `file://`, `data:`, `javascript:`, `.onion`, etc. are blocked.
2. **The dataset's own `derive.hydrate.blocked_hosts_extra`** — the manifest author's pre-banned hostnames, applied whatever `--block`/`--urlhaus` flags a run passes.
3. **Per-run `--block FILE`** — additional hostnames you supply, e.g. [StevenBlack/hosts](https://github.com/StevenBlack/hosts), your corporate DNS list, an IWF feed if you have access.
4. **`--urlhaus`** (opt-in) — fetches the [abuse.ch URLhaus](https://urlhaus.abuse.ch/) hostfile at run start, caches it for 24h. Covers active malware. Off by default because it adds a network dependency at hydrate-start.

Layers 3 and 4 make the run a sample, so they preview what a filtered dataset would hold; they never shape the published one. A blocklist that must govern the published dataset goes in the manifest, as `derive.hydrate.blocked_hosts_extra`, or in the network: run hydration behind a DNS-filtered resolver (CleanBrowsing, Quad9, Cloudflare 1.1.1.2).

We do **not** bundle a static "unsafe" list. They go stale; we can't cover every category (e.g. CSAM lists like IWF aren't publicly distributable); and our editorial choices wouldn't match yours. Bring the filter sources you trust.

**Bypass** requires *two* flags to make a single-flag accident impossible:

```bash
python -m raincloud.pipeline.hydrate <parent>-hydrated --unsafe-allow-all-domains --i-accept-the-risk
```

The bypass disables **every** layer, the scheme allowlist and the dataset's own `blocked_hosts_extra` included: each URL is fetched whatever its scheme or host. A bypass run is always a sample.

Each fetched column `<into>` is followed by a `_<into>_provenance` struct recording the filter decision per row, so a downstream consumer always knows *why* a value is null. `filter_decision` is one of `allowed` (fetched, including under the bypass), `blocked_scheme`, `blocked_by_host` (any blocklist: `blocked_hosts_extra`, `--block` or `--urlhaus`) or `fetch_error`; `error` and `http_status` say more about a fetch that failed.

## What you're consenting to

Hydration dereferences arbitrary URLs from the open web. Raincloud makes no claim about the safety, legality, or appropriateness of any bytes you receive — you are consenting to download whatever the URL returns. A hydrated dataset is **deliberately sketchy**: no file-size guarantees, no reproducibility guarantees (URLs die, content drifts), no completeness guarantees. Treat it as research convenience, not a redistributable corpus.
