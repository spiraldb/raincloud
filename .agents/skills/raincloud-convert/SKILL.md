---
name: raincloud-convert
description: Run only stage 7 — emit a sibling .vortex next to each opted-in parquet. Use when the user asks to (re)convert parquet files to Vortex format without rebuilding from raw bytes.
argument-hint: <slug>... | --all
disable-model-invocation: true
allowed-tools: Bash(python -m scripts.pipeline.convert *)
---

Run the convert-only entrypoint:

```bash
python -m scripts.pipeline.convert $ARGUMENTS
```

Selection (at least one required):
- `<slug>...` — positional slugs
- `--all` — every dataset in the manifest

Behavior:
- Only acts on specs that have `convert.vortex: true` in `sources.json` — others are reported as "no opt-in" and skipped.
- Idempotent: skips when `<slug>.vortex` is newer than `<slug>.parquet`.
- Writes the base vortex at `outputs/v{n}/<slug>/vortex/<slug>.vortex`.
- **Hydrated companion:** when a slug has both `convert.vortex: true` AND a hydrated parquet at `outputs/v{n}/<slug>/parquet-hydrated/<slug>.parquet` (produced by `/raincloud-hydrate`), this stage *also* writes `outputs/v{n}/<slug>/vortex-hydrated/<slug>.vortex` in the same run. Same opt-in flag governs both pairs.

Known type-support gaps (vortex 0.69 — see [SKILLS.md](../../context/SKILLS.md#emitting-a-vortex-file-alongside-the-parquet) and [docs/v1/vortex_skip.md](../../docs/v1/vortex_skip.md)):
- `FixedSizeBinary(N)` not yet supported (blocks `wikipedia-structured-contents`'s UUIDs; this is also why hydrate's `_hydrate_provenance.sha256` is `binary` not `fixed_size_binary[32]`).
- VARIANT columns surface as their shredded struct on round-trip — size blow-up on heavily-nested payloads.
- Per-file overhead dominates on very small parquets — size ratio can exceed 1.0 under a few MB.
- `Decimal128` works via the current `pf.read() → vxio.write(table)` path.

After a convert run, suggest `/raincloud-docs coverage_vortex columns_vortex` to refresh the vortex-side derived docs.
