---
name: raincloud-hydrate
description: Build a hydrated dataset (<parent>-hydrated) — the parent's rows with URL columns fetched from the open web — with the safe defaults, or write a scratch sample with non-default options (--limit/--block/--urlhaus/--max-bytes/--timeout/bypass) that is never published or served. Use ONLY for datasets the manifest declares as hydrated (see /raincloud-list-datasets --hydrate). Strong safety filter is on by default; bypass requires two flags. Makes outbound HTTP requests to arbitrary URLs from the open web.
argument-hint: [<parent>-hydrated | <parent> ...] [--all] [--limit N] [--block FILE] [--urlhaus] [--concurrency N] [--timeout SEC] [--max-bytes N]
disable-model-invocation: true
allowed-tools: Bash(python -m raincloud.pipeline.hydrate *)
---

Build hydrated datasets (`raincloud build <parent>-hydrated` does the same with the safe defaults):

```bash
python -m raincloud.pipeline.hydrate $ARGUMENTS
```

## Selection

- `<parent>-hydrated...` — specific hydrated datasets. A bare `<parent>` is accepted too and selects its `<parent>-hydrated` entry, when the parent is not itself hydrated and that entry exists. Any other dataset that is not hydrated is refused (exit 2, `not a hydrated dataset`); an unknown name exits 2 with a did-you-mean.
- `--all` — every hydrated dataset.

Currently declared: `laion-400m-hydrated`, `goodbooks-10k-hydrated`, `hacker-news-hydrated`. Use `python -m raincloud.pipeline.list_datasets --hydrate --long` to see the live list.

Options that change what is fetched make the run a **sample**: `--limit`, `--block`, `--urlhaus`, a non-default `--max-bytes` or `--timeout`, or the bypass. [`HYDRATING.md`](../../../HYDRATING.md) states the rule in full: a sample's table goes to `<scratch_dir>/<slug>/sample/<slug>.arrow.zstd` and is never published, recorded or served. Only a run with the safe defaults (`raincloud build <parent>-hydrated`, or `hydrate` without those options) produces the dataset. Spell the flags out in full; abbreviations are refused.

## Safety filter

On by default. Layers 1 and 2 are applied unless the two-flag bypass below is active; 3 and 4 are opt-in, and a run that uses them is a sample:

1. **Scheme allowlist** — `http` / `https` only. Refuses `file://`, `data:`, `javascript:`, `.onion`, etc.
2. **Per-dataset `derive.hydrate.blocked_hosts_extra`** — the manifest author's pre-banned hosts.
3. **Per-run `--block FILE`** — additional hostnames you supply (one per line; `/etc/hosts`-style `IP host` lines accepted; `#`-comments stripped). Repeatable.
4. **`--urlhaus`** — fetch [abuse.ch URLhaus](https://urlhaus.abuse.ch/) hostfile at run start, cache 24h.

Raincloud ships the **mechanism**, not the **policy** — no static "unsafe" list is bundled. Because `--block` and `--urlhaus` make a sample, they only preview a filtered run. Hosts that must be kept out of the published dataset go in the manifest's `derive.hydrate.blocked_hosts_extra`, or run hydration behind a DNS-filtered network (CleanBrowsing, Quad9, Cloudflare 1.1.1.2). Sources worth previewing with `--block`: [StevenBlack/hosts](https://github.com/StevenBlack/hosts), your corporate DNS list, IWF feeds (members only).

## Bypass

Requires both flags — single-flag accident is impossible:

```bash
python -m raincloud.pipeline.hydrate <parent>-hydrated --unsafe-allow-all-domains --i-accept-the-risk
```

The bypass disables **every** layer, the scheme allowlist and `blocked_hosts_extra` included, and makes the run a sample. Its fetched rows record `filter_decision = "allowed"`, like any fetch. It is intended for narrow research use against URL columns you have separately verified. **Do not** suggest it as a default. A multi-line warning prints regardless of acceptance.

## Tuning

- `--concurrency N` — parallel HTTP workers (default 8). Raise carefully on bulk crawls; many origins rate-limit aggressively. It changes only how fast the same requests are made, so it never makes a sample.
- `--timeout SEC` — per-request timeout (default 30). A non-default value makes the run a sample, since it changes which fetches fail.
- `--max-bytes N` — per-row payload cap (default 10 MB). Truncated rows record `error="truncated"` in provenance. A non-default value makes the run a sample.
- `--limit N` — only hydrate the first N rows; always a sample. Recommended for first-time runs to characterize the failure modes before going wide.

## Output

For a run with the safe defaults, an ordinary dataset — canonical Arrow plus the default Parquet/Vortex exports under `outputs/v{n}/<parent>-hydrated/` — with the parent's columns plus, for each hydrated column:

- `<into>` — fetched bytes (`type: binary`) or text (`type: string`); null when the URL was filtered out or the fetch failed.
- `_<into>_provenance: struct<http_status: int16, content_type: string, fetched_at: timestamp[s], sha256: binary, bytes_total: int32, filter_decision: string, error: string>` — an honest record of why bytes are or aren't there. `filter_decision` is `allowed`, `blocked_scheme`, `blocked_by_host` (the dataset's `blocked_hosts_extra`, `--block` and `--urlhaus` hosts alike) or `fetch_error`.

The parent must be prepared first. Loading the result emits `HydratedDatasetWarning`: no file-size, reproducibility or completeness guarantees. Treat it as research convenience, not a redistributable corpus.

## When to suggest this

- The user explicitly asks to hydrate a marked slug.
- The user asks "what's actually in the LAION images" / "what does this URL column look like dereferenced" against a marked slug.

## When NOT to suggest this

- The dataset isn't declared hydrated (it is refused, exit 2).
- The user just wants the parent dataset (nothing to hydrate).
- The user is on a build machine without network egress to arbitrary internet hosts.

After a build, regenerate observations with `/raincloud-docs`. List hydrated datasets with `/raincloud-list-datasets --hydrate --long`.

See [`HYDRATING.md`](../../../HYDRATING.md) for the policy / philosophy.
