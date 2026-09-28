# skills

Project-local skills for AI coding agents (Claude Code, plus other tools that follow the [Agent Skills](https://agentskills.io) open standard, e.g. Codex). Each `<skill-name>/SKILL.md` is invokable as `/<skill-name>` and Claude can also load it automatically when its `description` matches the user's intent (unless `disable-model-invocation: true` is set).

`.agents/` is the canonical directory; `.claude → .agents` is a symlink at the repo root, so Claude Code (which reads `.claude/skills/`) and tooling that reads `.agents/skills/` see the same files.

## Script wrappers

Wrappers around `python -m raincloud.pipeline.<module>`. Side-effecting ones set `disable-model-invocation: true` so Claude won't auto-trigger destructive work; read-only ones are model-invocable.

| Skill | Wraps | Purpose |
|---|---|---|
| `/raincloud-build` | `raincloud.pipeline.build` | Full pipeline (fetch → … → write_canonical → validate → run_exporters) for one or more slugs. |
| `/raincloud-fetch` | `raincloud.pipeline.fetch` | Download raw bytes only. |
| `/raincloud-extract` | `raincloud.pipeline.extract` | Unpack archives into the recipe's scratch directory. |
| `/raincloud-export` | `raincloud.pipeline.export` | Re-derive Parquet/Vortex from the canonical Arrow already on disk, without refetching; the refresh path for one format. |
| `/raincloud-convert` | `raincloud.pipeline.convert` | Re-encode Vortex with the Python writer (v1 catalogs: from Parquet). For v2, prefer `/raincloud-export --format vortex`. |
| `/raincloud-hydrate` | `raincloud.pipeline.hydrate` | Build a `<parent>-hydrated` dataset (URL columns fetched from the open web) with the safe defaults, or write a scratch sample with non-default options (`--limit`/`--block`/`--urlhaus`/`--max-bytes`/`--timeout`/bypass), never published or served. Side-effecting (outbound HTTP); safety-filter-gated; `disable-model-invocation: true`. |
| `/raincloud-docs` | `raincloud.pipeline.docs` | Regenerate derived docs. *(model-invocable — regen is mostly idempotent.)* |
| `/raincloud-status` | `raincloud.pipeline.status` | Per-slug filesystem state (raw / work / arrow / parquet / vortex). *(read-only, model-invocable.)* |
| `/raincloud-validate-manifest` | `raincloud.pipeline.validate_manifest` | Static checks for `sources.json` — JSON Schema + handler-registry / slug-uniqueness / fetch-auth cross-checks. *(read-only, model-invocable.)* |
| `/raincloud-list-datasets` | `raincloud.pipeline.list_datasets` | Filter/list slugs by handler / license / fetch-type / reader / vortex / tag / showcase / size / regex. *(read-only, model-invocable.)* |
| `/raincloud-discover` | `raincloud.pipeline.list_datasets` | Find "interesting" datasets via the discoverability flags — tag / showcase / size / trait / view. *(read-only, model-invocable.)* |
| `/raincloud-profile` | `raincloud.pipeline.profile` | Compute per-column statistics → `outputs/v{n}/<slug>/profile.json` (opt-in; feeds the TUI detail pane + `list_datasets --inspect`). *(writes `profile.json`; model-invocable.)* |
| `/raincloud-load` | `raincloud.load` (loader API) | Load a prepared dataset (cache → mirror; never builds implicitly) as a lazy `Dataset`; inspect metadata or materialize. *(`disable-model-invocation: true`.)* |
| `/raincloud-publish` | `raincloud.pipeline.publish` | Place built artefacts in this machine's store and release the catalog (`--store`), or upload them to a mirror (`--mirror`), gated on the snapshot sha256. *(side-effecting — `disable-model-invocation: true`.)* |

## Procedural playbooks (model-invocable)

These guide multi-step procedures from [`SKILLS.md`](../context/SKILLS.md). Default frontmatter — Claude can pull them up automatically when the user's request matches.

| Skill | When to use |
|---|---|
| `/raincloud-add-dataset` | Adding a new dataset to `sources.json` and producing its first build. |
| `/raincloud-add-handler` | Writing a new transform handler under `raincloud/pipeline/handlers/`. |
| `/raincloud-add-kaggle-tos` | Adding a Kaggle dataset gated behind a one-time ToS click-through. |
| `/raincloud-promote-variant` | JSON → VARIANT via the transform recipe and a rebuild. |
| `/raincloud-debug-build` | Diagnostic checklist for a failing build — isolate which stage broke. |
| `/raincloud-large-build` | Run a memory- or runtime-heavy build safely (caps, nohup, logging). *(side-effecting — `disable-model-invocation: true`.)* |
| `/raincloud-remove-dataset` | Remove a dataset from the manifest and clean up its outputs. *(destructive — `disable-model-invocation: true`.)* |

## Supporting context

`../context/` holds symlinks back to the repo-root canonical docs:

- [`AGENTS.md`](../context/AGENTS.md) — invariants and architecture for AI agents.
- [`SKILLS.md`](../context/SKILLS.md) — playbooks (the source for the procedural skills above).
- [`README.md`](../context/README.md) — user-facing project overview.
- [`sources.schema.md`](../context/sources.schema.md) — `sources.json` schema reference.

Each `SKILL.md` references these via relative paths (`../../context/X.md`) so the agent pulls authoritative guidance without copying.

## Adding or editing a skill

```text
my-skill/
├── SKILL.md           # required — frontmatter + instructions
├── reference.md       # optional — detailed reference loaded only when needed
└── scripts/           # optional — bundled scripts the skill can execute
    └── helper.py
```

Reference: <https://code.claude.com/docs/en/skills>. Frontmatter fields used here:

- `name` — slug; matches the directory name.
- `description` — front-load the key use case (truncated at 1,536 chars in the listing).
- `argument-hint` — autocomplete hint for `/<skill> <args>`.
- `disable-model-invocation` — `true` for side-effecting skills so Claude won't auto-trigger them.
- `allowed-tools` — pre-approve specific tool patterns when the skill is active (e.g. `Bash(python -m raincloud.pipeline.build *)`).
