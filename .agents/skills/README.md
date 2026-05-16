# skills

Project-local skills for AI coding agents (Claude Code, plus other tools that follow the [Agent Skills](https://agentskills.io) open standard, e.g. Codex). Each `<skill-name>/SKILL.md` is invokable as `/<skill-name>` and Claude can also load it automatically when its `description` matches the user's intent (unless `disable-model-invocation: true` is set).

`.agents/` is the canonical directory; `.claude → .agents` is a symlink at the repo root, so Claude Code (which reads `.claude/skills/`) and tooling that reads `.agents/skills/` see the same files.

## Script wrappers

Wrappers around `python -m scripts.pipeline.<module>`. Side-effecting ones set `disable-model-invocation: true` so Claude won't auto-trigger destructive work; read-only ones are model-invocable.

| Skill | Wraps | Purpose |
|---|---|---|
| `/raincloud-build` | `scripts.pipeline.build` | Full pipeline (fetch → … → convert) for one or more slugs. |
| `/raincloud-fetch` | `scripts.pipeline.fetch` | Download raw bytes only. |
| `/raincloud-extract` | `scripts.pipeline.extract` | Unpack archives into `_workdir/`. |
| `/raincloud-convert` | `scripts.pipeline.convert` | Stage 7 — emit sibling `.vortex` per spec opt-in. |
| `/raincloud-hydrate` | `scripts.pipeline.hydrate` | Stage 8 (optional, opt-in) — dereference a slug's URL column into a sibling parquet under `parquet-hydrated/`. Side-effecting (outbound HTTP); safety-filter-gated; `disable-model-invocation: true`. |
| `/raincloud-docs` | `scripts.pipeline.docs` | Regenerate derived docs. *(model-invocable — regen is mostly idempotent.)* |
| `/raincloud-tighten-variant` | `scripts.pipeline.tighten_variant` | In-place JSON → VARIANT promotion. |
| `/raincloud-status` | `scripts.pipeline.status` | Per-slug filesystem state (raw / workdir / parquet / vortex / variant-pending). *(read-only, model-invocable.)* |
| `/raincloud-validate-manifest` | `scripts.pipeline.validate_manifest` | Static checks for `sources.json` — JSON Schema + handler-registry / slug-uniqueness / fetch-auth cross-checks. *(read-only, model-invocable.)* |
| `/raincloud-list-datasets` | `scripts.pipeline.list_datasets` | Filter/list slugs by handler / license / fetch-type / reader / vortex / tag / showcase / size / regex. *(read-only, model-invocable.)* |

## Procedural playbooks (model-invocable)

These guide multi-step procedures from [`SKILLS.md`](../context/SKILLS.md). Default frontmatter — Claude can pull them up automatically when the user's request matches.

| Skill | When to use |
|---|---|
| `/raincloud-add-dataset` | Adding a new dataset to `sources.json` and producing its first build. |
| `/raincloud-add-handler` | Writing a new transform handler under `scripts/pipeline/handlers/`. |
| `/raincloud-add-kaggle-tos` | Adding a Kaggle dataset gated behind a one-time ToS click-through. |
| `/raincloud-promote-variant` | Picks the right path (in-place vs new-build) for JSON → VARIANT. |
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
- `allowed-tools` — pre-approve specific tool patterns when the skill is active (e.g. `Bash(python -m scripts.pipeline.build *)`).
