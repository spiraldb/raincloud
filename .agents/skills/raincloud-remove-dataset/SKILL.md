---
name: raincloud-remove-dataset
description: Remove a dataset from sources.json and clean up its outputs. Use when the user wants to delete a slug from the manifest. Destructive — always confirm before acting.
argument-hint: <slug>
disable-model-invocation: true
---

Walk through removing dataset `$ARGUMENTS`. Reference: [SKILLS.md "Removing a dataset"](../../context/SKILLS.md#removing-a-dataset).

**This is destructive. Confirm with the user before doing any of these steps.** No backwards-compat shims are kept — git history is the fallback (`.archive/` is gitignored and only present on the maintainer's tree).

Steps:

1. **Check for dependents.** Run `python -m raincloud.pipeline.list_datasets --hydrate --long`. A `<slug>-hydrated` entry whose `derive.from` names this dataset must be removed with it, or re-parented; otherwise `validate_manifest` fails on its dangling `derive.from`.

2. **Resolve the paths before changing the manifest.** Run this from the checkout, with the slug filled in. `operation_lock(resources=True)` takes the store, raw and scratch locks, so a build or export cannot move anything while the paths are resolved; the locks are released when the block ends:

   ```python
   from raincloud.pipeline.lifecycle import operation_lock
   from raincloud.pipeline.spec import outputs_root, raw_slug_dir, recipe_workdir_root

   slug = "SLUG"
   with operation_lock(resources=True) as ctx:
       manifest = ctx.manifest
       spec = next(s for s in manifest["datasets"] if s["slug"] == slug)
       print(outputs_root() / slug)                       # built files, current schema_version
       print(raw_slug_dir(slug))                          # raw payloads, shared across versions
       print(recipe_workdir_root(spec, manifest) / slug)  # this recipe's scratch
   ```

   Show the user the three paths and what each holds.

3. **Delete only what the user approved, while no build or export is running** (the lookup's locks are no longer held). The output directory; the recipe scratch; the raw payloads only if asked. Keep:
   - every older `outputs/v{n}/<slug>/` — a frozen version, which nothing regenerates;
   - any `.recipes/` generation other than the ones printed, which belong to other catalogs or recipes (when the raw path printed is `<raw_dir>/<slug>` itself, delete its payload files and leave its `.recipes/` subdirectory);
   - symlink targets: remove a symlink itself, never recurse through it.

4. **Remove the `DatasetSpec` entry from `sources.json`** using the [Python load-edit-dump pattern](../../context/AGENTS.md#editing-sourcesjson). Immutable installed bundles require a newly packaged catalog; never edit a cached revision in place.

5. **If a handler became unused** (only this slug referenced it), delete the handler file from `raincloud/pipeline/handlers/` and its entry from `HANDLERS` in `raincloud/_registry.py`. That entry is the only registration, and the catalog capability list derives from it. No stub or deprecation shim — fully delete.

6. **Regenerate and promote the docs**, or invoke `/raincloud-docs`. The regeneration writes gitignored scratch; the tracked `docs/v2/` keeps the removed slug until it is promoted:

   ```bash
   python -m raincloud.pipeline.docs
   cp docs/snapshot.json docs/datasets.md docs/handlers.md docs/v2/
   git diff docs/v2/
   ```

Entries for the slug in this install's build record (`<data_dir>/builds.json`) are left alone: the loader consults them only for a dataset the catalog names, and only for a file present at the recorded size, so they are inert once the files are gone.

Removed means removed.
