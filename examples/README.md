# examples

Copy-pasteable templates for the two most common tasks against this repo.

| File | Purpose |
|---|---|
| [`minimal_spec.json`](minimal_spec.json) | Full `DatasetSpec` with every field present and placeholder values. Validates against [`../sources.schema.json`](../sources.schema.json). Use as the starting point for a new entry in `sources.json`. |
| [`streaming_handler.py.tmpl`](streaming_handler.py.tmpl) | Template for a streaming transform handler — the pattern for datasets that don't fit in RAM. Copy into `scripts/pipeline/handlers/`, fill in the ingest loop, register in `scripts/pipeline/handlers/__init__.py`. |

These mirror the inline templates in [`../SKILLS.md`](../SKILLS.md) but live as real files so `git grep`, IDE schema validation, and copy-paste-without-markdown-quirks all work.

After dropping a new entry into `sources.json`, run [`/raincloud-validate-manifest`](../.agents/skills/raincloud-validate-manifest/SKILL.md) to confirm the shape is correct before paying for a fetch.
