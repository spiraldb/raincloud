---
name: raincloud-discover
description: Use when the user wants to find "interesting" datasets — exposes the new tag / showcase / size / trait / view filters on list_datasets.
---

# raincloud-discover

Wraps `python -m scripts.pipeline.list_datasets` with the discoverability flags. The TUI (`python -m scripts.pipeline.browse`) is the interactive sibling.

## Closed vocab

- **Showcase tiers** (2): `encoding`, `stress`. Run `--showcase-help` for live counts.
- **Domain tags** (12): `geospatial`, `nlp-text`, `web-analytics`, `e-commerce`, `finance`, `social`, `scientific`, `healthcare`, `sports`, `transportation`, `government`, `benchmark`. Run `--tags-help` for live counts.
- **Size buckets** (5): `xs / s / m / l / xl` (file-size on disk).
- **Trait flags** (6): `has_nested`, `has_timestamp`, `has_variant`, `string_heavy`, `wide_row`, `high_cardinality_present`. Prefix with `!` to negate.

## Patterns

```bash
python -m scripts.pipeline.list_datasets --view encoding --long
python -m scripts.pipeline.list_datasets --tag geospatial --tag scientific
python -m scripts.pipeline.list_datasets --trait has_nested --size m
python -m scripts.pipeline.list_datasets --trait '!has_nested' --vortex
python -m scripts.pipeline.list_datasets --inspect clickbench-hits
python -m scripts.pipeline.list_datasets --tags-help
python -m scripts.pipeline.list_datasets --showcase-help
```

Filters AND across axes, OR within an axis. `--view` replaces other facet flags (preset is the entire facet spec).

## When to invoke

- "Find me datasets with X property" → use traits + tags + size filters
- "What's good for testing nested-type encoding?" → `--trait has_nested --vortex`
- "Show me a small dataset to iterate on" → `--size xs --size s`
- "Inspect this slug" → `--inspect <slug>`
- "What tags / tiers exist?" → `--tags-help` / `--showcase-help`
