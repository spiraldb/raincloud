---
name: raincloud-status
description: Report per-dataset state (raw / workdir / parquet / vortex / variant-pending) across the manifest. Use when the user asks what's downloaded, what's built, what's missing, what needs re-tightening, or to triage which slugs still need work.
argument-hint: [<slug>...] [--family <name>] [--fast] [--missing-only] [--json]
allowed-tools: Bash(python -m scripts.pipeline.status *)
---

Run the status reporter:

```bash
python -m scripts.pipeline.status $ARGUMENTS
```

Walks the manifest and reports per-slug filesystem state in five columns:

| Column | What it shows |
|---|---|
| `raw` | `outputs/raw_downloads/<slug>/` present. `≠` if `fetch.expected_bytes` is declared and the on-disk total disagrees (only checked for single-URL specs — that's the same condition `fetch.py` uses). |
| `work` | `_workdir/<slug>/` non-empty (extract scratch — wiped by `--clean-workdir`). |
| `parquet` | `outputs/v{n}/<slug>/<slug>.parquet` present. Shows row count; `≠` prefix if it disagrees with `expect.rows`. |
| `vortex` | Sibling `.vortex` for slugs with `convert.vortex: true`. `n/a` for non-opted-in specs, `stale` if the parquet is newer. |
| `variant` | Count of residual JSON-annotated columns (= `/raincloud-tighten-variant` pending). `?` when `--fast` skipped the parquet footer scan. |

Selection (default: every slug in the manifest):
- `<slug>...` — positional slugs
- `--family <name>` — every dataset in a family
- `--all` — explicit "every dataset" (the default, kept for parity with `/raincloud-build`)

Modifiers:
- `--fast` — skip the parquet footer scan. Drops the row count and JSON-column check; keeps everything else. Useful on slow storage or for a quick pass over the full catalog.
- `--missing-only` — narrow output to slugs with at least one incomplete stage. Combines well with `--fast` for a quick "what still needs work" view.
- `--json` — emit a JSON array instead of the table. Use when piping into another tool.

The summary line at the bottom shows totals, e.g.:

```
N slugs  ·  raw M/N  ·  parquet M/N  ·  rows-match M/N  ·  vortex M/N  ·  tighten-variant pending K
```

This is read-only — it never touches `outputs/` or `_workdir/`. Safe to invoke whenever you want a snapshot. Suggest `/raincloud-build` for missing parquets, `/raincloud-convert` for missing vortex siblings, `/raincloud-tighten-variant` for variant-pending slugs.

Context: [SKILLS.md](../../context/SKILLS.md), [AGENTS.md "How the build pipeline is structured"](../../context/AGENTS.md#how-the-build-pipeline-is-structured).
