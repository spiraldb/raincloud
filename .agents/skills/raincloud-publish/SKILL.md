---
name: raincloud-publish
description: Push locally-built Raincloud artifacts to a mirror (`scripts.pipeline.publish`). Use when the maintainer wants to upload one or more slugs' parquet/vortex bytes to a configured mirror after a successful build, gated on the snapshot's recorded sha256.
argument-hint: <slug>... | --all  --mirror <s3://… | https://… | file://…>  [--dry-run]
disable-model-invocation: true
allowed-tools: Bash(python -m scripts.pipeline.publish *)
---

Sync locally-built artifacts to a mirror so downstream `raincloud.load()` users can fetch them. The CLI verifies each artifact's sha256 against `docs/v1/snapshot.json` before upload — mismatches block the push (publish is the one place an integrity gate is correct: corrupted bytes should never reach a shared mirror).

## Most common shape

```bash
python -m scripts.pipeline.publish $ARGUMENTS
```

Selection (one required):
- `<slug>...` — positional dataset slugs (any number).
- `--all` — every slug whose parquet OR vortex artifact exists locally under `outputs/v{n}/<slug>/`.

Required:
- `--mirror <url>` — destination root. Examples:
  - `s3://my-bucket/raincloud` (needs `pip install raincloud[s3]`)
  - `https://artifacts.example.com/raincloud` (needs `raincloud[http]`)
  - `file:///mnt/shared/raincloud-mirror` (built-in)
- `RAINCLOUD_MIRROR=<url>` env var works as an alternative to the flag.

Modifiers:
- `--dry-run` — print the upload plan (paths + keys) without writing. Always preview large publishes this way first.

## Before publishing

1. **Build the slug locally first** (`/raincloud-build <slug>` or `python -m scripts.pipeline.build <slug>`). Publish does not build; it only pushes what's already in `outputs/v{n}/<slug>/`.
2. **Regenerate the snapshot** (`/raincloud-docs` or `python -m scripts.pipeline.docs`). The publish gate compares local bytes against `parquet_sha256` / `vortex_sha256` in `docs/v1/snapshot.json` — a stale snapshot causes false `PublishMismatch` failures.
3. **Verify the mirror URL** with a dry-run first.

## What gets uploaded

For each slug × format pair (`parquet`, `vortex`) where a local artifact exists:
- Key: `v1/<slug>/<fmt>/<slug>.<ext>`
- Body: the raw artifact bytes
- Gate: `sha256(local) == snapshot[<fmt>_sha256]` (raises `PublishMismatch` on disagree; an unpinned slug — `sha256` is `null` — uploads without verification, so prefer to regen the snapshot first).

## Failure modes

| Error | Meaning | Fix |
|---|---|---|
| `PublishMismatch: ...` | Local bytes diverge from the snapshot's recorded sha. | Re-run `/raincloud-docs` to refresh the snapshot, OR confirm the local artifact is correct and commit the new snapshot. |
| `FileNotFoundError: outputs/v1/<slug>/...` | Slug isn't built locally. | Run `/raincloud-build <slug>` first. |
| `ImportError: Install s3fs ...` | `s3://` mirror without `[s3]` extra. | `pip install 'raincloud[s3]'`. |

## After publishing

Downstream users can now `raincloud.load(<slug>)` and the loader will pull from the configured mirror (cache → mirror → build).

Context: [AGENTS.md "The loader package"](../../../AGENTS.md), [`scripts/pipeline/publish.py`](../../../scripts/pipeline/publish.py).
