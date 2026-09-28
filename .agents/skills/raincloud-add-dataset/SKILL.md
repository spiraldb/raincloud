---
name: raincloud-add-dataset
description: Walk through adding a new dataset to sources.json and producing its first build. Use when the user wants to onboard a new upstream source, add a slug to the manifest, or extend the catalog.
argument-hint: [<proposed-slug>] [<upstream-url>]
---

Guide the user through adding a new dataset entry. Reference: [SKILLS.md "Adding a new dataset"](../../context/SKILLS.md#adding-a-new-dataset), [sources.schema.md](../../context/sources.schema.md).

Steps:

1. **Identify the upstream.** Confirm with the user:
   - A stable public URL (prefer the publisher's canonical endpoint over a mirror).
   - The license, recorded accurately: SPDX ID, `source_url`, `redistribution_permitted`, and `scrape_advisory` for broad-web crawls. A license that forbids redistribution does not keep a dataset out of the catalog; the redistribution gates apply only to `publish --mirror`.
   - Approximate row count (used for `expect.rows`; can be `null` on first build).

2. **Append a `DatasetSpec` to `sources.json`** using the Python load-edit-dump pattern from [AGENTS.md](../../context/AGENTS.md#editing-sourcesjson) — never `sed`. Copy [`templates/minimal_spec.json`](../../../templates/minimal_spec.json), which has every required field and validates as-is, and edit its placeholders rather than typing a spec from scratch. Keep its `write.*` values: they match the rest of the catalog, and `write.row_group_size_rows` is a row cap on top of the byte-sized row groups, and it wins over `RAINCLOUD_ROW_GROUP_MAX_ROWS` in every Parquet writer (the sidecars receive it through that variable), so a smaller value makes every Parquet export's groups smaller. [sources.schema.md](../../context/sources.schema.md) documents each field.

3. **Validate the manifest.** Invoke `/raincloud-validate-manifest` — sub-second check that the new entry has the right shape, the handler resolves, the slug is unique, and `fetch.type`/`fetch.auth` agree. Catches typos before paying for a fetch.

4. **Run the first build.** Invoke `/raincloud-build <slug>` (validation drift is a warning by default; add `--strict` for a hard gate). If `expect.rows` was wrong, update the manifest with the actual count once the build succeeds.

5. **Regenerate docs.** Invoke `/raincloud-docs` (or just `python -m raincloud.pipeline.docs`).

6. **Choose exports.** Without an `export` block a dataset exports Parquet and Vortex. `export.formats` is the only declaration of which formats it wants: `["parquet"]` leaves Vortex out, `[]` keeps only the canonical Arrow file. Keep a format listed even if its writer fails on the data: the build records the failure as the format's measured unavailability and carries on. `export.priority` prefers a writer, either for every format (`["rs", "py"]`, which must then name a writer for each exported format) or per format (`{"parquet": ["rs", "py"]}`); each format is one file whichever writer makes it. `convert.vortex` is v1-only and rejected in v2. Use `/raincloud-export <slug> --format vortex` to refresh one format without refetching.

If the upstream needs unpacking, set `extract.type` accordingly and pick a parser — see existing specs in `sources.json` for shapes. If the source has nested JSON or row-level processing, you'll likely need a custom handler — see `/raincloud-add-handler`. If it's a Kaggle dataset behind ToS acceptance, see `/raincloud-add-kaggle-tos`.
