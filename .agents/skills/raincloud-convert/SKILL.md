---
name: raincloud-convert
description: Re-encode prepared data to Vortex with the Python writer (v1 catalogs convert from Parquet). For a v2 catalog prefer /raincloud-export --format vortex, the refresh path that follows the dataset's export policy.
argument-hint: <slug>... | --all  [--retry-errors]
disable-model-invocation: true
allowed-tools: Bash(python -m raincloud.pipeline.convert *)
---

For a `schema_version` 2 catalog, refresh a Vortex file with `/raincloud-export <slug> --format vortex` instead. It picks the writer from `export.priority` exactly as a build does, and records the file in the build record. This skill runs the older convert-only entrypoint:

```bash
python -m raincloud.pipeline.convert $ARGUMENTS
```

Selection (at least one required):
- `<slug>...` — positional slugs
- `--all` — every dataset in the manifest

Behavior:
- It uses the Python Vortex writer (`vortex@py`) only. In v2 it is, in effect, `python -m raincloud.pipeline.export <slug> --format vortex@py` with a reuse check, and runs only where `vortex@py` is the declared Vortex writer (`export.formats` / `export.priority`); a dataset whose priority puts another Vortex writer first is skipped, so use `/raincloud-export` for it. In v1 it converts prepared Parquet when `convert.vortex=true`.
- V2 reads canonical Arrow. It reuses an existing Vortex file only when the build record says `vortex@py` wrote it from the current recipe after the canonical; it refuses a canonical this install built from an earlier recipe, and one that is neither this install's build nor the catalog's file (rebuild either with `raincloud build <slug>`); and it records what it writes in the build record. A slug named outright that is not converted (not opted in, or not built) makes the command exit 1. A failure `vortex@py` already recorded at this recipe with this toolchain, from this canonical, is not repeated: the slug is reported `[fail]` after a `[skip]` line (exit 1), unless `--retry-errors` is passed. V1 reuses a Vortex file newer than its Parquet. Writes hold the store lock and replace the file by rename.
- An unknown slug exits 2 with a did-you-mean before any work; `--all` skips hydrated datasets.
- Vortex support comes from `[vortex]` or `[build]`. Unsupported types may fail; consult current conformance results rather than historical version-specific gaps.

After conversion, regenerate derived docs with `python -m raincloud.pipeline.docs`.
