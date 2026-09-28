---
name: raincloud-status
description: Report per-dataset filesystem state (raw / work / arrow / parquet / vortex) across the manifest. Use when the user asks what's downloaded, what's built, what's missing, or to triage which slugs still need work.
argument-hint: [<slug>...] [--fast] [--missing-only] [--json]
allowed-tools: Bash(python -m raincloud.pipeline.status *)
---

Run the status reporter:

```bash
python -m raincloud.pipeline.status $ARGUMENTS
```

Walks the manifest and reports per-slug filesystem state:

| Column | What it shows |
|---|---|
| `raw` | Selected raw generation contains payload files; recipe metadata alone does not count. `≠` if `fetch.expected_bytes` is declared and the on-disk total disagrees (only checked for single-URL specs — that's the same condition `fetch.py` uses). |
| `work` | Selected `.recipes/<recipe-hash>/<slug>/` scratch (or legacy unscoped scratch) is non-empty (extract scratch — wiped by `--clean-workdir`). |
| `arrow` | (`schema_version` 2) the canonical `<data_dir>/v{n}/<slug>/arrow/<slug>.arrow.zstd` is present. |
| `parquet` | `<data_dir>/v{n}/<slug>/parquet/<slug>.parquet` present when the export policy includes Parquet. Shows row count; `≠` prefix if it disagrees with `expect.rows`; `unavail` as for `vortex`. |
| `vortex` | `<data_dir>/v{n}/<slug>/vortex/<slug>.vortex` present when the export policy includes Vortex (one file, whichever writer made it). In v2 the policy is `export.formats`, or the default formats when it is absent; in v1, `convert.vortex`. `n/a` when Vortex is not exported; `stale` if its source (canonical Arrow in v2, Parquet in v1) is newer than the file; `unavail` when a build measured the writer unable to produce it at the current recipe (this install's build record, else the catalog's), which counts as complete. |

Selection (default: every slug in the manifest):
- `<slug>...` — positional slugs
- `--all` — explicit "every dataset" (the default, kept for parity with `/raincloud-build`)

Modifiers:
- `--fast` — skip the parquet footer scan, which drops the row count. Useful on slow storage or for a quick pass over the full catalog.
- `--missing-only` — narrow output to slugs with at least one incomplete stage. Combines well with `--fast` for a quick "what still needs work" view.
- `--json` — emit a JSON array instead of the table. Use when piping into another tool.

The summary line at the bottom shows totals (the last part only when a format was measured unavailable), e.g.:

```
N slugs  ·  raw M/N  ·  arrow M/N  ·  parquet M/N  ·  rows-match M/N  ·  vortex M/N  ·  K measured unavailable
```

This is read-only — it never writes anything. Safe to invoke whenever you want a snapshot. Suggest `/raincloud-build` for a missing canonical or Parquet file, and `/raincloud-export <slug> --format vortex` for a missing or stale Vortex file when the canonical is present.

Context: [SKILLS.md](../../context/SKILLS.md), [AGENTS.md "How a build works"](../../context/AGENTS.md#how-a-build-works).
