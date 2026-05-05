# .agents

Everything an AI coding agent needs to operate inside this repo. The `.claude → .agents` symlink at the repo root means tooling that follows either naming convention (Claude Code, Codex, Cursor, …) sees the same files.

## What's here

| Path | Role |
|---|---|
| [`settings.json`](settings.json) | **Tracked** allow-list of safe, read-only Bash / git / WebFetch / pipeline commands. A fresh-clone agent gets these pre-approved so it doesn't burn turns on permission prompts. Side-effecting stages (build / fetch / extract / convert / tighten_variant) are intentionally *not* pre-approved here. |
| `settings.local.json` | **Gitignored** per-machine override — additional permissions specific to the local agent's session. Don't commit. |
| [`skills/`](skills/) | 16 invokable skills following the [Agent Skills](https://agentskills.io) standard — wrappers around pipeline entrypoints (`/raincloud-build`, `/raincloud-fetch`, `/raincloud-status`, `/raincloud-validate-manifest`, `/raincloud-list-datasets`, …) and procedural playbooks (`/raincloud-add-dataset`, `/raincloud-add-handler`, `/raincloud-debug-build`, …). See [`skills/README.md`](skills/README.md). |
| [`context/`](context/) | Symlinks back to the repo-root canonical docs (`AGENTS.md`, `SKILLS.md`, `README.md`, `sources.schema.md`) so each `SKILL.md` can pull authoritative guidance via a stable relative path without copying. |
| `scheduled_tasks.lock` | Gitignored — agent-runtime state. |

## Where to start (fresh-clone agent)

1. Read [`../AGENTS.md`](../AGENTS.md) (auto-loaded via `../CLAUDE.md → AGENTS.md` symlink) for the invariants.
2. Run `python -m scripts.pipeline.status --fast --missing-only` to verify the env.
3. Run `python -m scripts.pipeline.validate_manifest` to confirm the manifest is well-formed.
4. Browse [`skills/README.md`](skills/README.md) for the catalog of invokable commands.

## Adding or editing a skill

See [`skills/README.md`](skills/README.md#adding-or-editing-a-skill) — frontmatter schema, the `disable-model-invocation` flag for destructive skills, and the `allowed-tools` pre-approval pattern.
