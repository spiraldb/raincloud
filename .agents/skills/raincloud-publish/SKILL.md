---
name: raincloud-publish
description: Release locally built Raincloud artifacts (`raincloud.pipeline.publish`) — into this machine's shared store with a catalog release (`--store DIR --catalogs DIR`), or to an off-machine mirror (`--mirror URL`). Use when the maintainer wants built parquet/vortex bytes served to other users, gated on the snapshot's recorded sha256.
argument-hint: <slug>... | --all  (--store DIR [--catalogs DIR] | --mirror <s3://… | file://…>)  [--dry-run] [--allow-scrape-advisory] [--allow-no-redistribution]
disable-model-invocation: true
allowed-tools: Bash(python -m raincloud.pipeline.publish *)
---

Place or upload locally built artifacts so other users' `raincloud.load()` finds them. The CLI verifies each artifact's sha256 against the selected catalog's snapshot first; a mismatch blocks it (bytes the catalog does not record must never reach a shared store or mirror). An artifact whose snapshot entry records no sha256 is published ungated.

```bash
python -m raincloud.pipeline.publish $ARGUMENTS
```

Selection (one required):
- `<slug>...` — positional dataset slugs (any number). An unknown slug exits 2 with a did-you-mean before any work; a named slug with nothing built here is refused (exit 1).
- `--all` — every slug in the manifest; slugs with nothing built locally are skipped.

Target (exactly one required):
- `--store DIR` — this machine's shared content store, e.g. the `data_dir` an operator configured for all users. Each verified artifact is hard-linked (copied across filesystems) and renamed into place. The license gates do not apply: the store serves this machine's users, which is not redistribution.
  - `--catalogs DIR` (with `--store` only) — also release the catalog: write the selected catalog into the pack directory `DIR` and point `DIR/latest.json` at it. Readers whose `catalog` setting names `DIR` follow the release with no config edit. Before placing anything it refuses a catalog that names an artifact the store would not hold at the recorded size.
- `--mirror URL` — an off-machine mirror, a writable fsspec URL: `s3://my-bucket/raincloud` (needs `pip install raincloud[s3]`) or `file:///mnt/shared/raincloud-mirror`. An `https://` mirror can serve readers but is not writable, so `publish` refuses it (exit 2) before planning anything. `RAINCLOUD_MIRROR` configures readers only; publish always needs the flag.

Modifiers:
- `--dry-run` — print the plan (paths + keys, and the catalog revision a release would point at) without writing. Always preview large publishes this way first.
- `--allow-scrape-advisory`, `--allow-no-redistribution` — `--mirror` only. A slug whose license carries a `scrape_advisory`, or sets `redistribution_permitted: false`, is refused unless you pass the matching flag. Each flag clears only its own gate.

## Before publishing

1. **Build the slug locally first** (`/raincloud-build <slug>`). Publish does not build; it only places what is already under `outputs/v{n}/<slug>/`.
2. **Make the snapshot record the bytes.** In a checkout, after a local build or export: the plain `python -m raincloud.pipeline.docs`, which takes each file's sha256 and writer from this install's build record (`<data_dir>/builds.json`). Keep `docs snapshot --rehash` for files the build record does not describe (built elsewhere, or by an older raincloud): it bypasses the record, so a rehashed Vortex file loses its recorded writer. Then promote all three, `cp docs/snapshot.json docs/datasets.md docs/handlers.md docs/v2/`, review `git diff docs/v2/`, and commit. A custom bundle is immutable: package and select a new matched bundle if its pinned checksums need changing.
3. **Preview** with `--dry-run`.

## What gets placed

For each present canonical Arrow, Parquet or Vortex file (one per format, whichever writer made it):
- Key: `v{n}/<slug>/<format>/<filename>`, under the store or mirror root.
- Gate: the snapshot checksum, when it records one, must match, checked under the store lock. A file already in the store as the same file (a hard link from an earlier publish, or a build whose `data_dir` is the store) is not re-hashed. A mirror upload goes to a temporary key and is promoted only after the uploaded stream matches the validated local digest.
- Store files keep the builder's owner and mode (a hard link or `copy2`), so build under a umask or group that lets the store's other users read them.

## Failure modes

| Output | Meaning | Fix |
|---|---|---|
| `refusing to publish: …sha256… != …` (`PublishMismatch`) | Local bytes differ from the snapshot's recorded sha. | Regenerate and promote the snapshot as above, or create/select a new matched bundle. |
| `refusing <slug>: license …` | A license gate blocked a `--mirror` upload. | Leave it unpublished, or pass the named `--allow-*` flag if redistribution really is permitted. |
| `refusing to release the catalog: it names N artifact(s) …` | `--catalogs` would release a catalog naming files the store lacks. | Publish those slugs to the store first. |
| `refusing to publish: nothing built for <slug> under <root>` (exit 1) | A slug named on the command line has no built file here. | Run `/raincloud-build <slug>` first. |
| `published 0 artifact(s)` / `placed 0 artifact(s)` (exit 0) | `--all` found nothing built locally; unbuilt slugs are skipped under `--all`. | Build what should be published, and check the count after every `--all` publish. |
| `unknown dataset '<slug>'. Did you mean …?` (exit 2) | The slug is not in the selected catalog. | Use the suggested name, or `raincloud list`. |
| `ImportError: Install s3fs …` | `s3://` mirror without the `[s3]` extra. | `pip install 'raincloud[s3]'`. |

## After publishing

Readers of the store (via the released catalog) or of the mirror (`RAINCLOUD_MIRROR`) can now `raincloud.load(<slug>)` it. Local/cache is checked first, then the mirror; building still requires explicit opt-in.

Context: [SKILLS.md "Releasing to this machine's store"](../../context/SKILLS.md#releasing-to-this-machines-store), [AGENTS.md "Public loader API"](../../context/AGENTS.md#public-loader-api), [`raincloud/pipeline/publish.py`](../../../raincloud/pipeline/publish.py).
