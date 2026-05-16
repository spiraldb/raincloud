# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Propose `tags` for every slug by classifying its profile.json columns.

Tags live inline on each `DatasetSpec` in `sources.json` (same layer as
`description`, `license`, `showcase`). This module reads each slug's
profile in `docs/v1/profiles/<slug>.json`, classifies its columns
against the 13 closed `TAG_VOCAB` data-kinds in `discovery.py`, and
writes the top-3 per slug **directly into sources.json**. Slugs without
a built profile fall through to a handler/slug-name fallback table.

Tags are authored manifest data, not derived snapshot data — re-run
this when profiles change or when the heuristics improve, then commit
the resulting sources.json diff like any other manifest edit.

Usage:
    python -m scripts.pipeline.autotag             # update sources.json in place
    python -m scripts.pipeline.autotag --dry       # print proposal + distribution only
    python -m scripts.pipeline.autotag --slug foo  # one slug
    python -m scripts.pipeline.autotag --preserve  # only set tags on slugs that
                                                   #   have none today (no overwrite)
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCES = REPO_ROOT / "sources.json"
PROFILE_DIR = REPO_ROOT / "docs" / "v1" / "profiles"
MAX_TAGS_PER_SLUG = 3


def _classify_column(name: str, col: dict, row_count: int) -> str | None:
    """Map a profile column entry to a single TAG_VOCAB kind, or None to skip.

    Order matters: dtype is the strongest signal (timestamps, binary, nested
    types short-circuit), then column-name hints for context overrides
    (urls, monetary, coordinates), then content shape (mean_length, NDV)."""
    dtype = (col.get("dtype") or "").lower()
    name_l = name.lower()
    ndv = col.get("ndv_approx")
    mean_len = col.get("mean_length")
    top = col.get("top_values") or []
    top_sample = " ".join(str(t.get("value", "")) for t in (top[:5] if isinstance(top, list) else []))

    # ---- type-driven (strongest signal) ----
    if "timestamp" in dtype or dtype.startswith("date") or dtype == "time":
        return "timestamps"
    if "binary" in dtype:
        return "binary-payload"
    if "list" in dtype:
        # list<float|double> = embeddings; otherwise it's a list of structs/
        # strings which we classify as nested-json. (profile.py drops the
        # element type today so we get "list" with no hint — relying on
        # name and the slug-name fallback path to surface embeddings.)
        if "float" in dtype or "double" in dtype:
            return "embeddings"
        return "nested-json"
    if any(k in dtype for k in ("struct", "map", "variant")):
        return "nested-json"

    # ---- string columns ----
    if "string" in dtype:
        if any(k in name_l for k in ("url", "href", "link")):
            return "urls"
        if any(k in name_l for k in ("html", "markdown", "_md", "code", "source", "body", "xml")):
            return "code-strings"
        if any(k in name_l for k in ("geometry", "wkb", "wkt", "geom")):
            return "coordinates"
        if top_sample:
            sl = top_sample.lower()
            if sl.count("http://") + sl.count("https://") >= 1:
                return "urls"
            if any(m in top_sample for m in ("<html", "<div", "<p>", "<a ", "<b>", "<table", "<script", "<!--",
                                              "```", "function ", "def ", "import ", "{\"", "}\""))or top_sample.lstrip().startswith("{"):
                return "code-strings"
        if mean_len is not None:
            if mean_len > 80:
                return "prose"
            if ndv is not None and row_count > 0:
                ratio = ndv / row_count
                if (ndv <= 32 and mean_len <= 24) or (ndv <= 256 and ratio <= 0.001 and mean_len <= 24):
                    return "enums"
                if ratio >= 0.5 and 4 <= mean_len <= 40:
                    return "identifiers"
                if mean_len >= 40:
                    return "prose"
            if ndv is None:
                return "prose" if mean_len >= 40 else None
            return "prose" if mean_len >= 40 else "identifiers" if ndv > 256 else "enums"
        return None

    # ---- numeric columns ----
    if any(k in dtype for k in ("int", "double", "float", "decimal", "number")):
        if any(k in name_l for k in ("lat", "lon", "latitude", "longitude")):
            return "coordinates"
        if any(k in name_l for k in ("price", "amount", "cost", "fee", "revenue", "salary",
                                      "income", "usd", "eur", "gbp", "dollar", "wage", "cents",
                                      "rent", "fare", "tip")):
            return "monetary"
        mn, mx = col.get("min"), col.get("max")
        try:
            if mn is not None and mx is not None and "int" in dtype:
                if float(mn) >= 0 and float(mx) <= 1e6:
                    return "counts"
        except (TypeError, ValueError):
            pass
        return "measurements"

    return None


# Fallback per-handler tags for slugs without a built profile. Empty
# value means "no auto-guess — leave untagged".
_HANDLER_FALLBACK: dict[str, list[str]] = {
    "osm_pbf_split":            ["coordinates", "nested-json"],
    "wikipedia_variant_parse":  ["prose", "nested-json", "urls"],
    "factbook_variant_parse":   ["nested-json", "prose"],
    "jsonbench_variant_parse":  ["nested-json", "timestamps"],
    "stack_exchange_split":     ["code-strings", "prose", "timestamps"],
    "lichess_pgn_parse":        ["code-strings", "timestamps"],
    "public_bi_merge":          ["enums", "measurements"],
}

# Slug-specific fallback when handler + profile are both unavailable.
_SLUG_FALLBACK: dict[str, list[str]] = {
    "clickbench-hits":           ["urls", "timestamps", "counts"],
    "wdi":                       ["measurements", "timestamps", "identifiers"],
    "ghcn-daily":                ["measurements", "timestamps", "identifiers"],
    "openlibrary-works":         ["prose", "identifiers", "nested-json"],
    "openlibrary-editions":      ["prose", "identifiers", "nested-json"],
    "openlibrary-authors":       ["prose", "identifiers", "nested-json"],
    "nypd-complaints":           ["timestamps", "coordinates", "enums"],
    "stackoverflow-posts":       ["prose", "code-strings", "timestamps"],
    "stackoverflow-postlinks":   ["timestamps", "counts", "identifiers"],
    "openorca":                  ["prose"],
    "beir-msmarco":              ["prose", "identifiers"],
    "slimpajama-6b":             ["prose"],
    "fineweb-sample-10bt":       ["prose", "urls"],
    "laion-400m":                ["urls", "binary-payload", "embeddings"],
    "jsonbench-bluesky-100m":    ["nested-json", "timestamps"],
    "wikipedia-structured-contents": ["prose", "nested-json", "urls"],
    "osm-germany-nodes":         ["coordinates", "nested-json"],
    "osm-germany-relations":     ["coordinates", "nested-json"],
}


def _slug_name_fallback(spec: dict) -> list[str]:
    """Slug-name / description hints for kinds profile.json can't surface
    (today: embeddings hidden behind `dtype: "list"`, image / audio BLOBs
    that aren't profiled at all)."""
    slug = spec["slug"].lower()
    short = (spec.get("short_name") or "").lower()
    desc = (spec.get("description") or "").lower()
    handler = ((spec.get("transform") or {}).get("handler") or "").lower()
    text = " ".join([slug, short, desc, handler])
    tags: list[str] = []
    if any(k in text for k in (" embed", "embedding", "glove", "vector", "word2vec",
                                "fasttext", "encoder output", "dense vector")):
        tags.append("embeddings")
    if any(k in text for k in ("image", " photo", "laion", "websight", " audio",
                                "blob", "pdf binary", " weights")):
        tags.append("binary-payload")
    return tags


def infer_for_slug(spec: dict, profile: dict | None) -> list[str]:
    """Top-3 tags for a slug, ranked by column-count of each matching kind.
    Profile absent → handler / slug-specific / slug-name fallbacks."""
    from .discovery import TAG_VOCAB
    order = {t: i for i, t in enumerate(TAG_VOCAB)}

    if profile is None:
        slug = spec["slug"]
        handler = ((spec.get("transform") or {}).get("handler") or "")
        seed = list(_SLUG_FALLBACK.get(slug, []))
        seed += _HANDLER_FALLBACK.get(handler, [])
        seed += _slug_name_fallback(spec)
        return sorted(set(seed), key=lambda t: order.get(t, 99))[:MAX_TAGS_PER_SLUG]

    row_count = profile.get("row_count") or 1
    kinds: Counter[str] = Counter()
    for name, col in (profile.get("columns") or {}).items():
        if not isinstance(col, dict) or col.get("skipped"):
            continue
        k = _classify_column(name, col, row_count)
        if k:
            kinds[k] += 1
    # Score slug-name fallbacks at +1 so they participate in top-3 without
    # drowning out genuinely column-heavy kinds.
    for t in _slug_name_fallback(spec):
        kinds[t] += 1
    ranked = sorted(kinds.items(), key=lambda x: (-x[1], order.get(x[0], 99)))
    return [t for t, _ in ranked[:MAX_TAGS_PER_SLUG]]


def _print_distribution(specs: list[dict]) -> None:
    """Show the post-apply tag distribution grouped by content axis."""
    tag_counts: Counter[str] = Counter()
    untagged: list[str] = []
    for spec in specs:
        tags = spec.get("tags") or []
        if not tags:
            untagged.append(spec["slug"])
        for t in tags:
            tag_counts[t] += 1
    sections = [
        ("STRING CONTENT", ["urls", "prose", "enums", "identifiers", "code-strings"]),
        ("NUMERIC CONTENT", ["timestamps", "embeddings", "counts", "monetary", "measurements"]),
        ("PAYLOAD / STRUCTURE", ["coordinates", "binary-payload", "nested-json"]),
    ]
    print("tag distribution (slug count per tag):")
    for section, tags in sections:
        print(f"\n  {section}")
        for tag in tags:
            n = tag_counts[tag]
            bar = "█" * min(40, n)
            print(f"    {tag:<16} {n:>3}  {bar}")
    if untagged:
        print(f"\nuntagged ({len(untagged)}):")
        for s in untagged:
            print(f"  {s}")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    p.add_argument("--dry", action="store_true",
                   help="print proposed tags + distribution; don't write")
    p.add_argument("--slug", action="append", default=None,
                   help="process just this slug (repeatable); default: all")
    p.add_argument("--preserve", action="store_true",
                   help="only assign tags to slugs that have none today")
    args = p.parse_args(argv)

    m = json.loads(SOURCES.read_text())
    targets = set(args.slug) if args.slug else None

    changed = 0
    proposed_by_slug: dict[str, list[str]] = {}
    for spec in m["datasets"]:
        slug = spec["slug"]
        if targets is not None and slug not in targets:
            continue
        if args.preserve and spec.get("tags"):
            continue
        prof_path = PROFILE_DIR / f"{slug}.json"
        profile = json.loads(prof_path.read_text()) if prof_path.exists() else None
        new_tags = sorted(set(infer_for_slug(spec, profile)))
        old_tags = list(spec.get("tags") or [])
        proposed_by_slug[slug] = new_tags
        if new_tags != old_tags:
            if new_tags:
                spec["tags"] = new_tags
            else:
                spec.pop("tags", None)
            changed += 1

    print(f"slugs processed: {len(proposed_by_slug)}")
    print(f"slugs changed:   {changed}")
    print()
    _print_distribution(m["datasets"])

    if args.dry:
        print("\n(dry — sources.json not written)")
        return 0
    if changed == 0:
        print("\nno changes — sources.json already matches inferred tags")
        return 0
    SOURCES.write_text(json.dumps(m, indent=2) + "\n")
    print(f"\nwrote {changed} updated spec(s) to {SOURCES}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
