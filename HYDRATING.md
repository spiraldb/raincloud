# Hydration policy

The optional **hydrate stage** (`python -m scripts.pipeline.hydrate <slug>`) dereferences URL columns from slugs marked with a `hydrate` block in `sources.json`, writing a sibling parquet at `outputs/v{n}/<slug>/parquet-hydrated/<slug>.parquet`. When the slug also has `convert.vortex: true`, the same run produces `outputs/v{n}/<slug>/vortex-hydrated/<slug>.vortex`.

This document is hand-maintained. The list of currently-marked candidates is queryable via the TUI (`python -m scripts.pipeline.browse` — sort by the `hydrate` column) or `python -m scripts.pipeline.list_datasets --hydrate --long`.

## What we provide vs. what you provide

Raincloud ships the **mechanism**, not the **policy**. The hydrate stage filters URLs through:

1. **A scheme allowlist** (always on) — only `http` and `https`. `file://`, `data:`, `javascript:`, `.onion`, etc. are blocked.
2. **The dataset's own `hydrate.blocked_hosts_extra`** — the manifest author's pre-banned hostnames for that slug, applied regardless of run-time flags.
3. **Per-run `--block FILE`** — additional hostnames you supply. Plug in [StevenBlack/hosts](https://github.com/StevenBlack/hosts), your corporate DNS list, an IWF feed if you have access, etc.
4. **`--urlhaus`** (opt-in) — fetches the [abuse.ch URLhaus](https://urlhaus.abuse.ch/) hostfile at run start, caches it for 24h. Covers active malware. Off by default because it adds a network dependency at hydrate-start.

We do **not** bundle a static "unsafe" list. They go stale; we can't cover every category (e.g. CSAM lists like IWF aren't publicly distributable); and our editorial choices wouldn't match yours. Plug in the upstream filter sources you trust, or run hydration behind a DNS-filtered network (CleanBrowsing, Quad9, Cloudflare 1.1.1.2).

**Bypass** requires *two* flags to make a single-flag accident impossible:

```bash
python -m scripts.pipeline.hydrate <slug> --unsafe-allow-all-domains --i-accept-the-risk
```

The hydrated parquet records the filter decision per row (`_hydrate_provenance.filter_decision`), so a downstream consumer always knows *why* a row's `<output_column>` is null — fetched-and-empty, blocked-by-host, blocked-by-scheme, or fetched-and-errored.

## What you're consenting to

Hydration dereferences arbitrary URLs from the open web. Raincloud makes no claim about the safety, legality, or appropriateness of any bytes you receive — you are consenting to download whatever the URL returns. The hydrated parquet is a **separate, deliberately sketchy artefact tier**: no file-size guarantees, no reproducibility guarantees (URLs die, content drifts), no completeness guarantees. Treat it as research convenience, not a redistributable corpus.
