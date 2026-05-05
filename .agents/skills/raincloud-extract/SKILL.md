---
name: raincloud-extract
description: Run only the extract stage (unpack archives into _workdir/) for the given slugs. Use when the user wants to inspect what the unpack stage produces, or debug a handler complaint about missing files.
argument-hint: <slug>...
disable-model-invocation: true
allowed-tools: Bash(python -m scripts.pipeline.extract *)
---

Run the extract-only entrypoint:

```bash
python -m scripts.pipeline.extract $ARGUMENTS
```

Behavior:
- Implicitly invokes `fetch` first to make sure raw bytes exist.
- Unpacks archives (zip, gzip, 7z, .lzw, etc.) under `_workdir/<slug>/` — gitignored scratch.
- Useful when a downstream stage complains "no .xxx files" and you want to inspect what was unpacked. Look in `_workdir/<slug>/` after this runs.

Wipe `_workdir/<slug>/` to force re-extraction. `--clean-workdir` on `/raincloud-build` does this automatically after each successful build.

Use `/raincloud-build` instead for the full pipeline.
