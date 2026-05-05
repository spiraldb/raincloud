---
name: raincloud-remove-dataset
description: Remove a dataset from sources.json and clean up its outputs. Use when the user wants to delete a slug from the manifest. Destructive — always confirm before acting.
argument-hint: <slug>
disable-model-invocation: true
---

Walk through removing dataset `$ARGUMENTS`. Reference: [SKILLS.md "Removing a dataset"](../../context/SKILLS.md#removing-a-dataset).

**This is destructive. Confirm with the user before doing any of these steps.** No backwards-compat shims are kept — git history is the fallback (`.archive/` is gitignored and only present on the maintainer's tree).

Steps:

1. **Remove the `DatasetSpec` entry from `sources.json`** using the [Python load-edit-dump pattern](../../context/AGENTS.md#safe-ways-to-edit-sourcesjson) — never `sed`. Look up by `slug` and remove from `m["datasets"]`.

2. **Delete the output parquet** (and sibling `.vortex` if present):

   ```bash
   rm -rf outputs/v1/<slug>/
   ```

3. **Optionally delete the raw cache:**

   ```bash
   rm -rf outputs/raw_downloads/<slug>/
   ```

   Only do this if you're confident the dataset won't be re-added. The raw cache is unversioned and could feed a future schema_version bump.

   **Watch for sibling slugs sharing the same upstream URL** (GloVe sizes, OSM Germany kinds) — those are deduped via hardlink. Removing `outputs/raw_downloads/<slug>/` is fine because each slug gets its own subdir, but verify there isn't a multi-output handler producing the parquet from a shared raw dir before deleting either.

4. **Regenerate docs:**

   ```bash
   python -m scripts.pipeline.docs
   ```

   (Or invoke `/raincloud-docs`.)

5. **If a handler became unused** (only this slug referenced it), remove the handler file from `scripts/pipeline/handlers/` and unregister it from `__init__.py`. No stub or deprecation shim — fully delete.

Removed means removed.
