---
name: raincloud-docs
description: Regenerate the derived docs (datasets.md, handlers.md, snapshot.json). Use after a build, a manifest edit, or a handler add/remove/rename. snapshot.json is load-bearing — it's the fallback both for the TUI's columns / types modals AND for `datasets.md` regen on slugs not built locally, so partial-build maintainers don't dash-out the table. Other catalog views (columns, coverage, vortex-skip, hydrate candidates) are queryable via `/raincloud-list-datasets` and the TUI.
argument-hint: [datasets | handlers | snapshot]...
allowed-tools: Bash(python -m scripts.pipeline.docs *)
---

Run the docs regenerator:

```bash
python -m scripts.pipeline.docs $ARGUMENTS
```

Targets:
- *(no args)* — regenerates all three: `datasets.md`, `handlers.md`, `snapshot.json`.
- `datasets` — just `docs/datasets.md` (one row per dataset).
- `handlers` — just `docs/handlers.md` (one row per registered transform handler).
- `snapshot` — just `docs/snapshot.json` (per-slug schema + file sizes; read by the TUI for unbuilt-locally slugs AND by `datasets.md` regen as the row-count / size fallback). Run alongside `datasets` if you ever invoke `datasets` on its own — otherwise the markdown table will drift from the manifest.

Output lands in **`docs/*.md` / `docs/*.json`** (gitignored scratch). Promote to the tracked canonical path with a manual copy:

```bash
python -m scripts.pipeline.docs && cp docs/datasets.md docs/handlers.md docs/snapshot.json docs/v1/
```

When to run:
- After a build (row counts, file sizes, and `snapshot.json` schema for that slug change).
- After manifest edits that affect `short_name` / `license` / `description` / `expect.rows` / `convert.vortex`.
- After adding, removing, or renaming a handler (`handlers.md` regenerates from the registry + manifest usage).
- After any schema-affecting change to a slug's transform handler — re-run `snapshot` so the TUI's fallback view picks up the new shape.

Other catalog views (no longer auto-generated as markdown):

| What you want | How to get it |
|---|---|
| Columns of one slug | `python -m scripts.pipeline.list_datasets <slug> --columns` |
| Cross-catalog column index | `python -m scripts.pipeline.list_datasets --columns [--column-grep PATTERN]` |
| Type coverage summary | `python -m scripts.pipeline.list_datasets --coverage` |
| Vortex-opted-out slugs + reasons | `python -m scripts.pipeline.list_datasets --no-vortex --json` |
| Hydration candidates | `python -m scripts.pipeline.list_datasets --hydrate --long` |
| Interactive browse | `python -m scripts.pipeline.browse` |

Hydration policy / philosophy is hand-maintained in `HYDRATING.md`.
