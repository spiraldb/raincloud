---
name: raincloud-profile
description: Use when the user asks to compute or refresh per-column statistics for a raincloud dataset — produces `outputs/v1/<slug>/profile.json` for the TUI's detail pane and `list_datasets --inspect`.
---

# raincloud-profile

Wraps `python -m scripts.pipeline.profile`. Opt-in stage; off the default
build path. Idempotent against parquet sha256.

## When to invoke

- "Generate profiles for X / for everything that's built"
- "Refresh the profile after I rebuilt slug X"
- "Show me per-column statistics" → run this first, then `list_datasets --inspect <slug>`

## Patterns

```bash
python -m scripts.pipeline.profile <slug>                          # one
python -m scripts.pipeline.profile --all                           # every built parquet
python -m scripts.pipeline.profile --sample-rows 1000000 <slug>    # cap for huge slugs
```

Per-dtype stats:
- **Numeric**: histogram + NDV + min/max/mean
- **String / binary**: NDV; top-5 when NDV ≤ 256 (binary skips top-5 — bytes don't render usefully)
- **Bool**: T/F/null counts
- **Date/Timestamp**: range + 10-bucket histogram
- **List/Map**: length min/max/mean
- **Struct / variant**: skipped (emits null at column-map level)

Profiles live at `outputs/v1/<slug>/profile.json` and are read by:
- The TUI's right-pane Columns section (`python -m scripts.pipeline.browse`)
- The CLI's `--inspect <slug>` rendering
- `docs.py` for backfilling `shape_traits.high_cardinality_present` into `snapshot.json`

The tracked mirror at `docs/v1/profiles/<slug>.json` is the fallback path that
fresh clones ship — both `list_datasets --inspect` and the TUI's Columns pane
read it when no built `outputs/v1/<slug>/profile.json` exists locally. The
`profile` stage auto-runs `python -m scripts.pipeline.promote_profiles` at the
end of a successful run (copies built profile.json into the tracked mirror,
byte-identical mirror skipped), so the tracked snapshot stays in sync without
a manual step. Pass `--no-promote` to suppress that auto-step while iterating;
invoke `promote_profiles` explicitly (or with `--check`) to re-sync or audit
the mirror after manual edits.
