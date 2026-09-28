---
name: raincloud-fetch
description: Run only the fetch stage (download raw bytes) for the given slugs. Use when the user wants to prime the cache, debug fetch logic, or download upstream bytes without running the full pipeline.
argument-hint: <slug>... | --all  [--verify]
disable-model-invocation: true
allowed-tools: Bash(python -m raincloud.pipeline.fetch *)
---

Run the fetch-only entrypoint:

```bash
python -m raincloud.pipeline.fetch $ARGUMENTS
```

Behavior:
- Uses the configured unversioned raw cache. `raincloud.pipeline.spec.raw_slug_dir(slug)` selects the catalog/fetch generation under `<raw_dir>/<slug>/`; the same upstream recipe can serve multiple output schema versions.
- Idempotent: skips when the local file matches `expected_bytes` / `expected_sha256` from the spec, or the size recorded when it was fetched; `--verify` re-hashes cached files instead of trusting that size. Invalidate only upstream payloads in the selected generation to force a refetch; preserve Raincloud metadata and sibling `.recipes/` generations.
- Sibling slugs sharing a URL (GloVe sizes, OSM Germany kinds) are deduped via hardlink.
- Name the slugs. `--all` fetches every dataset in the manifest except hydrated ones, the whole catalog's upstream bytes, which is almost certainly not what the user wants, so confirm before invoking that way. An unknown slug exits 2 with a did-you-mean.

For Kaggle entries with `requires_interactive_accept: true`, a 403 surfaces as an error pointing at the URL the user must click through in a browser (signed into Kaggle) before retrying. See [SKILLS.md "Adding a Kaggle dataset gated behind ToS acceptance"](../../context/SKILLS.md#adding-a-kaggle-dataset-gated-behind-tos-acceptance) and `/raincloud-add-kaggle-tos`.

Use `/raincloud-build` instead when you want the full pipeline, not just the download step. Useful standalone for cache priming or debugging fetch logic.
