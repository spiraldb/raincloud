---
name: raincloud-hydrate
description: Run the optional hydrate stage to dereference a slug's URL column into a sibling parquet under outputs/v{n}/<slug>/parquet-hydrated/. Use ONLY for slugs that opted in via the manifest's `hydrate` block (see /raincloud-list-datasets --hydrate). Strong safety filter is on by default; bypass requires two flags. Makes outbound HTTP requests to arbitrary URLs from the open web.
argument-hint: [<slug>...] [--all] [--limit N] [--block FILE] [--urlhaus] [--concurrency N] [--timeout SEC] [--max-bytes N]
disable-model-invocation: true
allowed-tools: Bash(python -m scripts.pipeline.hydrate *)
---

Run the URL hydration stage:

```bash
python -m scripts.pipeline.hydrate $ARGUMENTS
```

## Selection

- `<slug>...` — specific slugs (must each have `hydrate` set in the manifest).
- `--all` — every slug with `hydrate` set.

Currently marked candidates: `laion-400m`, `goodbooks-10k`, `hacker-news`. Use `python -m scripts.pipeline.list_datasets --hydrate --long` to see the live list.

## Safety filter

Always on by default. Layered, all opt-out:

1. **Scheme allowlist** (always on, even with `--unsafe-allow-all-domains`-without-bypass). `http` / `https` only. Refuses `file://`, `data:`, `javascript:`, `.onion`, etc.
2. **Per-slug `hydrate.blocked_hosts_extra`** — the manifest author's pre-banned hosts for the slug. Always applied.
3. **Per-run `--block FILE`** — additional hostnames you supply (one per line; `/etc/hosts`-style `IP host` lines accepted; `#`-comments stripped). Repeatable.
4. **`--urlhaus`** — fetch [abuse.ch URLhaus](https://urlhaus.abuse.ch/) hostfile at run start, cache 24h.

Raincloud ships the **mechanism**, not the **policy** — no static "unsafe" list is bundled. Suggested upstream filter sources to plug into `--block`: [StevenBlack/hosts](https://github.com/StevenBlack/hosts), your corporate DNS list, IWF feeds (members only). Or run hydration behind a DNS-filtered network (CleanBrowsing, Quad9, Cloudflare 1.1.1.2).

## Bypass

Requires both flags — single-flag accident is impossible:

```bash
python -m scripts.pipeline.hydrate <slug> --unsafe-allow-all-domains --i-accept-the-risk
```

The bypass is intended for narrow research use against URL columns you have separately verified. **Do not** suggest it as a default. A multi-line warning prints regardless of acceptance.

## Tuning

- `--concurrency N` — parallel HTTP workers (default 8). Raise carefully on bulk crawls; many origins rate-limit aggressively.
- `--timeout SEC` — per-request timeout (default 30).
- `--max-bytes N` — per-row payload cap (default 10 MB). Truncated rows record `error="truncated"` in provenance.
- `--limit N` — only hydrate the first N rows. Recommended for first-time runs against a slug to characterize the failure modes before going wide.

## Output

Sibling parquet at `outputs/v{n}/<slug>/parquet-hydrated/<slug>.parquet`. Adds two columns to whatever was in the base:

- `<output_column>` — dereferenced bytes (`output_type: binary`) or string (`output_type: string`); null when the URL was filtered out or fetch failed.
- `_hydrate_provenance: struct<http_status: int16, content_type: string, fetched_at: timestamp[s], sha256: binary, bytes_total: int32, filter_decision: string, error: string>` — every row carries an honest record of why bytes are or aren't there. (`sha256` is always 32 bytes by construction but stored as `binary` rather than `fixed_size_binary[32]` because vortex 0.69 doesn't accept FSB types yet.)

When the slug has `convert.vortex: true`, the stage **auto-runs** vortex conversion on the hydrated parquet, writing `outputs/v{n}/<slug>/vortex-hydrated/<slug>.vortex`. Same opt-in flag as the base parquet — no separate manifest field. Failures during convert are logged but don't roll back the hydrated parquet.

The hydrated parquet is a **separate, deliberately sketchy artefact tier**: no file-size guarantees, no reproducibility guarantees (URLs die, content drifts), no completeness guarantees. Treat it as research convenience, not a redistributable corpus.

## When to suggest this

- The user explicitly asks to hydrate a marked slug.
- The user asks "what's actually in the LAION images" / "what does this URL column look like dereferenced" against a marked slug.

## When NOT to suggest this

- The slug has no `hydrate` block (will raise / be skipped).
- The user is just trying to read the base parquet (nothing to hydrate).
- The user is on a build machine without network egress to arbitrary internet hosts.

After this runs, also suggest `/raincloud-docs hydrated` to refresh the centralized list (purely cosmetic — the doc generates from the manifest, not from runs).

See [`HYDRATING.md`](../../../HYDRATING.md) for the policy / philosophy and per-slug advisories.
