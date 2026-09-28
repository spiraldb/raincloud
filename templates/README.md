# templates

Copy-pasteable starting points for the two most common ways to extend the
pipeline with a new upstream source. These are authoring references, not
standalone scripts: copy one into place to run it. For runnable demos of the loader API see [`../examples/`](../examples/).

| File | Purpose |
|---|---|
| [`minimal_spec.json`](minimal_spec.json) | A `DatasetSpec` with every required field present and placeholder values. Validates against [`../sources.schema.json`](../sources.schema.json). Use as the starting point for a new entry in `sources.json`. Optional blocks it leaves out are documented in [`../sources.schema.md`](../sources.schema.md): `export` chooses formats (`export.formats`) and writers (`export.priority`), and `tags` / `showcase` / `references` feed discovery. |
| [`streaming_handler.py.tmpl`](streaming_handler.py.tmpl) | Template for a streaming transform handler — the pattern for datasets that don't fit in RAM. Copy into `raincloud/pipeline/handlers/`, fill in the ingest loop, and declare it in `HANDLERS` in `raincloud/_registry.py` (`"<name>": "<module>:<function>"`). |

The playbooks in [`../SKILLS.md`](../SKILLS.md) carry shorter inline snippets; these standalone files are the full versions, so `git grep`, IDE schema validation, and copy-paste-without-markdown-quirks all work.

After dropping a new entry into `sources.json`, run [`/raincloud-validate-manifest`](../.agents/skills/raincloud-validate-manifest/SKILL.md) to confirm the shape is correct before paying for a fetch.

## `minimal_spec.json`

Copy the whole object into the `datasets` array in `sources.json` and edit the
placeholders. It validates against `sources.schema.json` as-is — keep it that way:
the spec schema sets `additionalProperties: false`, so an explanatory key added to
this file would make every copy of it fail `validate_manifest`. Insert it with the
Python load-edit-dump pattern in AGENTS.md, never with `sed`.
