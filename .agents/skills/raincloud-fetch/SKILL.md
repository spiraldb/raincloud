---
name: raincloud-fetch
description: Run only the fetch stage (download raw bytes) for the given slugs. Use when the user wants to prime the cache, debug fetch logic, or download upstream bytes without running the full pipeline.
argument-hint: <slug>...
disable-model-invocation: true
allowed-tools: Bash(python -m scripts.pipeline.fetch *)
---

Run the fetch-only entrypoint:

```bash
python -m scripts.pipeline.fetch $ARGUMENTS
```

Behavior:
- Writes to `outputs/raw_downloads/<slug>/` (unversioned cache — same upstream bytes serve any `schema_version`).
- Idempotent: skips when local file matches `expected_bytes` / `expected_sha256` from the spec. Delete `outputs/raw_downloads/<slug>/` to force re-fetch.
- Sibling slugs sharing a URL (GloVe sizes, OSM Germany kinds) are deduped via hardlink.
- With no arg, fetches every dataset in the manifest — almost certainly not what the user wants. Confirm before invoking that way.

For Kaggle entries with `requires_interactive_accept: true`, a 403 surfaces as an error pointing at the URL the user must click through in a browser (signed into Kaggle) before retrying. See [SKILLS.md "Adding a Kaggle dataset gated behind ToS acceptance"](../../context/SKILLS.md#adding-a-kaggle-dataset-gated-behind-tos-acceptance) and `/raincloud-add-kaggle-tos`.

Use `/raincloud-build` instead when you want the full pipeline, not just the download step. Useful standalone for cache priming or debugging fetch logic.
