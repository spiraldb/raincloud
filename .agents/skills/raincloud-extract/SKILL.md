---
name: raincloud-extract
description: Run only the extract stage (unpack archives into the recipe's scratch directory) for the given slugs. Use when the user wants to inspect what the unpack stage produces, or debug a handler complaint about missing files.
argument-hint: <slug>... | --all
disable-model-invocation: true
allowed-tools: Bash(python -m raincloud.pipeline.extract *)
---

Run the extract-only entrypoint:

```bash
python -m raincloud.pipeline.extract $ARGUMENTS
```

Selection: positional slugs, or `--all`. An unknown slug exits 2 with a did-you-mean.

Behavior:
- Implicitly invokes `fetch` first to make sure raw bytes exist.
- Pins one catalog and configuration for the operation, then unpacks archives under `<configured scratch>/.recipes/<recipe-hash>/<slug>/`, the same namespace used by full builds. Changed recipes get separate intermediates; repeating the same recipe reuses its extraction cache.
- Useful when a downstream stage complains "no .xxx files" and you want to inspect what was unpacked. Inspect the path printed by extraction after this runs.

Clear only the selected scratch directory to force re-extraction. `--clean-workdir` on `/raincloud-build` clears that build's recipe generation after success.

Use `/raincloud-build` instead for the full pipeline.
