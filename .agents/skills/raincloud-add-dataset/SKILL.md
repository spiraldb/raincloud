---
name: raincloud-add-dataset
description: Walk through adding a new dataset to sources.json and producing its first build. Use when the user wants to onboard a new upstream source, add a slug to the manifest, or extend the catalog.
argument-hint: [<proposed-slug>] [<upstream-url>]
---

Guide the user through adding a new dataset entry. Reference: [SKILLS.md "Adding a new dataset"](../../context/SKILLS.md#adding-a-new-dataset), [sources.schema.md](../../context/sources.schema.md).

Steps:

1. **Identify the upstream.** Confirm with the user:
   - A stable public URL (prefer the publisher's canonical endpoint over a mirror).
   - The license — must permit redistribution-of-derivatives. Check SPDX ID and `source_url`.
   - Approximate row count (used for `expect.rows`; can be `null` on first build).

2. **Append a `DatasetSpec` to `sources.json`** using the Python load-edit-dump pattern from [AGENTS.md](../../context/AGENTS.md#safe-ways-to-edit-sourcesjson) — never `sed`. Start from [`templates/minimal_spec.json`](../../../templates/minimal_spec.json) (every field present with placeholder values) rather than typing one from scratch. Minimal direct-HTTP shape:

   ```jsonc
   {
     "slug": "my-dataset",
     "short_name": "My Dataset",
     "full_name": "My Dataset (publisher attribution)",
     "description": "One-line summary.",
     "license": { "spdx": "CC0-1.0", "source_url": "...", "redistribution_permitted": true, "attribution_required": false },
     "fetch":     { "type": "http", "urls": ["https://..."], "auth": null },
     "extract":   { "type": "passthrough" },
     "parse":     { "reader": "csv", "options": { "delimiter": "," } },
     "transform": { "handler": "tighten_types", "params": {} },
     "write":     { "output": "my-dataset.parquet", "compression": "zstd", "row_group_size_rows": 1048576, "statistics": true },
     "expect":    { "rows": 123456 }
   }
   ```

3. **Validate the manifest.** Invoke `/raincloud-validate-manifest` — sub-second check that the new entry has the right shape, the handler resolves, the slug is unique, and `fetch.type`/`fetch.auth` agree. Catches typos before paying for a fetch.

4. **Run the first build.** Invoke `/raincloud-build <slug> --loose` (the `--loose` is essential when the row count is a guess). If `expect.rows` was wrong, update the manifest with the actual count once the build succeeds.

5. **Regenerate docs.** Invoke `/raincloud-docs` (or just `/raincloud-docs datasets columns_parquet coverage_parquet`).

6. **Optionally opt into Vortex.** If the dataset's types are vortex-compatible, add `"convert": { "vortex": true }` to the spec and run `/raincloud-convert <slug>`.

If the upstream needs unpacking, set `extract.type` accordingly and pick a parser — see existing specs in `sources.json` for shapes. If the source has nested JSON or row-level processing, you'll likely need a custom handler — see `/raincloud-add-handler`. If it's a Kaggle dataset behind ToS acceptance, see `/raincloud-add-kaggle-tos`.
