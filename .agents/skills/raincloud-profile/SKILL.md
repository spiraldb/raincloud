---
name: raincloud-profile
description: Use when the user asks to compute or refresh per-column statistics for a raincloud dataset — produces `outputs/v{n}/<slug>/profile.json` for the TUI's detail pane and `list_datasets --inspect`.
---

# raincloud-profile

Wraps `python -m raincloud.pipeline.profile`. Opt-in stage; off the default
build path. Idempotent against parquet sha256.

## When to invoke

- "Generate profiles for X / for everything that's built"
- "Refresh the profile after I rebuilt slug X"
- "Show me per-column statistics" → run this first, then `list_datasets --inspect <slug>`

## Patterns

```bash
python -m raincloud.pipeline.profile <slug>                          # one
python -m raincloud.pipeline.profile --all                           # every built parquet
python -m raincloud.pipeline.profile --sample-rows 1000000 <slug>    # cap for huge slugs
```

Per-dtype stats:
- **Numeric**: histogram + NDV + min/max/mean
- **String / binary**: NDV; top-5 when NDV ≤ 256 (binary skips top-5 — bytes don't render usefully)
- **Bool**: T/F/null counts
- **Date/Timestamp**: range + 10-bucket histogram
- **List/Map**: length min/max/mean
- **Struct / variant**: skipped (emits null at column-map level)

Profiles live at `outputs/v{n}/<slug>/profile.json` and are read by:
- The TUI's right-pane Columns section (`python -m raincloud.pipeline.browse`)
- The CLI's `--inspect <slug>` rendering
- `docs.py` for backfilling `shape_traits.high_cardinality_present` into `snapshot.json`

For the checkout catalog, promotion mirrors profiles into `docs/v{n}/profiles/`; the current version is v2. Installed, pinned and custom catalogs use `<data_dir>/.raincloud/observations/<catalog-revision>/profiles/` instead. Profiles from unrelated catalogs must not be used as fallbacks. A checkout falls back to the frozen `docs/v1/profiles/` for a slug with no v2 profile. That fallback is removed once `docs/v2/profiles/` covers the catalog: `tests/test_docs_contracts.py::test_v1_profile_fallback_is_still_needed` fails then, naming the code to delete. The overnight runner pins its catalog for the entire run, so its parent and subprocesses use revision-local observations even when launched from a checkout.

The profile command promotes after a successful run. Pass `--no-promote` to suppress that step; use `python -m raincloud.pipeline.promote_profiles --check` to audit the selected profile destination.
