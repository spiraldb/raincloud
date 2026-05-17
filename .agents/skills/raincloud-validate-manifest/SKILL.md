---
name: raincloud-validate-manifest
description: Static checks for sources.json — JSON Schema shape + cross-checks (handler registry, slug uniqueness, fetch.type/auth consistency). Use after any manifest edit, before triggering a build, or whenever an agent wants a sub-second sanity check that the manifest is well-formed.
argument-hint: [--json] [--strict]
allowed-tools: Bash(python -m scripts.pipeline.validate_manifest *)
---

Run the manifest validator:

```bash
python -m scripts.pipeline.validate_manifest $ARGUMENTS
```

What it checks:

1. **JSON Schema** — shape and enums declared in [`sources.schema.json`](../../../sources.schema.json) (Draft 2020-12). Requires the `jsonschema` package; pinned in `pyproject.toml`. Skipped with a hint if missing.
2. **Cross-checks** the schema can't express:
   - Slug uniqueness across `datasets[]`.
   - Every `transform.handler` resolves in `scripts/pipeline/handlers/__init__.py:_REGISTRY`.
   - Every registered handler is referenced by ≥1 spec (orphans → warning).
   - `fetch.urls` non-empty unless `fetch.type == "custom"`.
   - `fetch.auth` matches `fetch.type` for `kaggle` / `huggingface`.
   - `fetch.requires_interactive_accept` only on `fetch.type == "kaggle"`.
   - `write.output` filename agrees with slug for single-output handlers (warning otherwise — multi-output handlers legitimately diverge).

Modifiers:
- `--json` — machine-readable report (`{ok, n_datasets, errors, warnings, schema_skipped}`).
- `--strict` — treat warnings as errors. Useful in CI-style gating.

Exit code: `0` on success (warnings allowed), `1` on errors.

When to invoke:
- After editing `sources.json` (especially handler renames, license changes, slug additions).
- Before `/raincloud-build` on a fresh slug — catches typo'd handler names without paying for a fetch.
- As the read-only counterpart to `/raincloud-status`: `/raincloud-status` reports filesystem state, `/raincloud-validate-manifest` reports manifest correctness.

Context: [AGENTS.md "Safe ways to edit sources.json"](../../context/AGENTS.md#safe-ways-to-edit-sourcesjson), [sources.schema.md](../../context/sources.schema.md), [`sources.schema.json`](../../../sources.schema.json).
