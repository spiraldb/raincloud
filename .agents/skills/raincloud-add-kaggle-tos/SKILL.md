---
name: raincloud-add-kaggle-tos
description: Add a Kaggle dataset gated behind a one-time ToS click-through. Use when a Kaggle URL returns 403 from the API and the user needs the manifest+workflow pattern that documents the click-through gate cleanly.
argument-hint: [<proposed-slug>] [<kaggle-url>]
---

Guide the user through adding a Kaggle dataset that requires a one-time click-through ToS acceptance on the Kaggle web UI before the API will serve downloads. Reference: [SKILLS.md "Adding a Kaggle dataset gated behind ToS acceptance"](../../context/SKILLS.md#adding-a-kaggle-dataset-gated-behind-tos-acceptance).

**Diagnostic note:** an HTTP 403 from a Kaggle fetch can also indicate a wrong slug. Double-check the Kaggle URL before reaching for this pattern.

Steps:

1. **Add the spec** as a normal `fetch.type: "kaggle"` entry but mark it as gated and leave `expect.rows: null`:

   ```jsonc
   "fetch": {
     "type": "kaggle",
     "urls": ["https://www.kaggle.com/datasets/<owner>/<dataset>"],
     "auth": "kaggle",
     "requires_interactive_accept": true,
     "notes": "Kaggle gates this dataset behind a one-time click-through ToS acceptance."
   },
   "expect": { "rows": null, "notes": "Row count populated after the first successful build." }
   ```

   Use the [Python load-edit-dump pattern](../../context/AGENTS.md#safe-ways-to-edit-sourcesjson) — never `sed`.

2. **Confirm Kaggle creds are set up:** `~/.kaggle/kaggle.json` with `chmod 600`, and the project synced via `uv sync --extra kaggle --inexact`.

3. **Try the first build via `/raincloud-build <slug> --loose`.** Pre-flight will print `kaggle (ToS-gated): ...`. Expect a 403 on the first try. The error message will point at the exact Kaggle URL the user must visit.

4. **Tell the user to click Download once** in a browser signed into Kaggle. The 403 handler is generic — it triggers whether or not `requires_interactive_accept` is set, so forgetting the flag still yields a useful error; the flag only improves the up-front announcement.

5. **Re-run `/raincloud-build <slug> --loose`.** It should succeed.

6. **Update `expect.rows`** in the manifest with the actual count and **drop `expect.notes`** (now that the row count is known).

7. **Regenerate docs** via `/raincloud-docs`.
