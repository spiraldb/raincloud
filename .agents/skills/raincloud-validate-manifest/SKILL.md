---
name: raincloud-validate-manifest
description: Static checks for sources.json — JSON Schema shape + cross-checks (handler registry, slug uniqueness, fetch.type/auth consistency). Use after any manifest edit, before triggering a build, or whenever an agent wants a fast sanity check that the manifest is well-formed.
argument-hint: [path/to/sources.json] [--json] [--strict]
allowed-tools: Bash(python -m raincloud.pipeline.validate_manifest *)
---

Run the manifest validator:

```bash
python -m raincloud.pipeline.validate_manifest $ARGUMENTS
```

What it checks:

1. **JSON Schema** — shape and enums declared in [`sources.schema.json`](../../../sources.schema.json) (Draft 2020-12). Requires the `jsonschema` package, declared in the `[build]` extra. Skipped with a hint if missing.
2. **Cross-checks** the schema can't express:
   - Slug uniqueness across `datasets[]`.
   - Every `transform.handler` is declared in `HANDLERS` in `raincloud/_registry.py` (`handlers/__init__.py:_REGISTRY` is derived from it).
   - Every declared handler is referenced by ≥1 spec (orphans → warning).
   - `derive.from` names an ordinary dataset, and a hydrated dataset is named `<parent>-hydrated`.
   - `fetch.urls` non-empty unless `fetch.type` is `custom` or `generated`; a `generated` fetch names a registered generator, valid parameters and one of its outputs.
   - `fetch.auth` matches `fetch.type` for `kaggle` / `huggingface`.
   - `fetch.requires_interactive_accept` only on `kaggle` / `huggingface` fetches.
   - Exports: in v2, `export.formats` names the formats a dataset wants (`parquet`/`vortex`; leaving one out needs no reason, and a writer's limitation is measured by the build, never declared), and `convert.*` is rejected; `export.priority` (a list or a per-format map) names real writers. A list must name a writer for every exported format (`["hardwood"]` on a dataset that exports Vortex is an error).

It prints which manifest it validated. With no argument that is the selected catalog's (`RAINCLOUD_MANIFEST`, the checkout's `sources.json`, or the copy installed with raincloud); a path argument validates that file instead.

Modifiers:
- `--json` — machine-readable report (`{ok, manifest, n_datasets, schema_skipped, errors, warnings}`; `manifest` names where the validated manifest came from).
- `--strict` — treat warnings as errors. Useful in CI-style gating.

Exit code: `0` on success (warnings allowed), `1` on errors.

When to invoke:
- After editing `sources.json` (especially handler renames, license changes, slug additions).
- Before `/raincloud-build` on a fresh slug — catches typo'd handler names without paying for a fetch.
- As the read-only counterpart to `/raincloud-status`: `/raincloud-status` reports filesystem state, `/raincloud-validate-manifest` reports manifest correctness.

Context: [AGENTS.md "Editing sources.json"](../../context/AGENTS.md#editing-sourcesjson), [sources.schema.md](../../context/sources.schema.md), [`sources.schema.json`](../../../sources.schema.json).
