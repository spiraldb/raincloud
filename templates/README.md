# templates

Copy-pasteable starting points for the two most common ways to extend the
pipeline with a new upstream source. These are authoring references, not
runnable scripts — for runnable demos of the loader API see [`../examples/`](../examples/).

| File | Purpose |
|---|---|
| [`minimal_spec.json`](minimal_spec.json) | Full `DatasetSpec` with every field present and placeholder values. Validates against [`../sources.schema.json`](../sources.schema.json). Use as the starting point for a new entry in `sources.json`. |
| [`streaming_handler.py.tmpl`](streaming_handler.py.tmpl) | Template for a streaming transform handler — the pattern for datasets that don't fit in RAM. Copy into `scripts/pipeline/handlers/`, fill in the ingest loop, register in `scripts/pipeline/handlers/__init__.py`. |

The playbooks in [`../SKILLS.md`](../SKILLS.md) carry shorter inline snippets; these standalone files are the full versions, so `git grep`, IDE schema validation, and copy-paste-without-markdown-quirks all work.

After dropping a new entry into `sources.json`, run [`/raincloud-validate-manifest`](../.agents/skills/raincloud-validate-manifest/SKILL.md) to confirm the shape is correct before paying for a fetch.
