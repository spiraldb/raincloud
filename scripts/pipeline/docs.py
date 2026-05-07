# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Generate the derived Markdown docs + JSON snapshot from sources.json + outputs/v*/.

Three files are produced, all derived artefacts — never hand-edit:

    docs/datasets.md  — one row per dataset (row count, size, kind, license)
    docs/handlers.md  — one row per registered transform handler (purpose, streaming, usage)
    docs/snapshot.json — per-slug schema + file-size record for the canonical
                          built state. Read by the TUI as a fallback when a
                          local parquet isn't built, so the columns / types
                          modals can still show *expected* contents.
                          ALSO read by `generate_datasets_md` below as the
                          fallback for row count / row-group count / file
                          sizes when a slug's parquet isn't present locally
                          — without it, regen by a maintainer who hasn't
                          built every slug would dash-out the whole table.
                          Keep snapshot.json regenerated whenever a new
                          slug lands or a build's row count / size changes.

Per-column / per-coverage / vortex-skip / hydrated detail used to live as
markdown too, but the rendering was unscannable and duplicated state
already queryable via the TUI and `list_datasets`. Those views moved to:

    python -m scripts.pipeline.list_datasets --columns [<slug>...]    # column listing
    python -m scripts.pipeline.list_datasets --coverage               # type coverage
    python -m scripts.pipeline.list_datasets --no-vortex --long       # vortex-opted-out slugs
    python -m scripts.pipeline.list_datasets --hydrate --long         # hydrate candidates
    python -m scripts.pipeline.browse                                 # interactive

Hydration policy / philosophy lives in the hand-maintained
`HYDRATING.md` (preamble only, no auto-generated per-slug list).

Usage:

    python -m scripts.pipeline.docs            # all three (datasets + handlers + snapshot)
    python -m scripts.pipeline.docs datasets   # just datasets.md
    python -m scripts.pipeline.docs handlers   # just handlers.md
    python -m scripts.pipeline.docs snapshot   # just snapshot.json
"""
from __future__ import annotations

import re
import sys
from datetime import datetime, timezone

import pyarrow.parquet as pq

from .spec import (
    REPO_ROOT,
    load_manifest,
    prepared_parquet,
    prepared_vortex,
    spec_field,
)


def _generation_header(kind: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return (f"<!-- AUTO-GENERATED {kind} by scripts/pipeline/docs.py at {ts}. "
            f"Regenerate with: python -m scripts.pipeline.docs {kind}. DO NOT EDIT. -->")


DATASETS_MD = REPO_ROOT / "docs" / "datasets.md"
HANDLERS_MD = REPO_ROOT / "docs" / "handlers.md"
SNAPSHOT_JSON = REPO_ROOT / "docs" / "snapshot.json"


# ---------- shared helpers ----------

_KIND_BY_FAMILY = {
    "nyc-tlc": "Tabular (Parquet)",
    "public-bi": "Tabular (CSV)",
    "uci": "Tabular (CSV)",
}


def _data_kind(spec: dict, column_names: set[str] | None = None) -> str:
    """Best-effort inference of the 'Data Kind' label.

    `column_names` may come from a live parquet schema OR from the snapshot
    fallback — both cases need to recognise the `content` blob convention.
    """
    family = spec.get("family", "")
    if family in _KIND_BY_FAMILY:
        return _KIND_BY_FAMILY[family]
    reader = spec_field(spec, "parse.reader", "csv")
    handler = spec_field(spec, "transform.handler", "")
    if reader == "parquet":
        base = "Tabular (Parquet)"
    elif reader in ("json", "jsonl"):
        base = "Structured (JSON)"
    elif reader == "xml":
        base = "Structured (XML)"
    elif reader == "pbf":
        base = "Geo (OSM PBF)"
    elif reader == "sqlite":
        base = "Tabular (SQLite)"
    elif reader == "custom":
        if handler == "glove_split":
            return "Structured (Embeddings)"
        if handler in ("osm_pbf_split",):
            return "Geo (GeoParquet)"
        base = "Custom"
    else:
        base = "Tabular (CSV)"
    if column_names and "content" in column_names:
        base = f"{base.split(' (')[0]} + Blobs"
    return base


def _load_snapshot_slugs(schema_version: int | None = None) -> dict[str, dict]:
    """Return the `slugs` mapping from the on-disk snapshot, or `{}`.

    Used by `generate_datasets_md` to fall back to the last-known row count
    / sizes when a slug's parquet isn't present locally. Tries:

        1. `docs/snapshot.json`                 (gitignored scratch — wins
           if a maintainer regenerated locally)
        2. `docs/v{schema_version}/snapshot.json`  (tracked canonical — what
           a fresh clone has)

    Returns `{}` on a missing or malformed snapshot — callers degrade to
    the dash placeholder.
    """
    import json
    candidates = [SNAPSHOT_JSON]
    if schema_version is not None:
        candidates.append(REPO_ROOT / "docs" / f"v{schema_version}" / "snapshot.json")
    for path in candidates:
        if not path.exists():
            continue
        try:
            return json.loads(path.read_text()).get("slugs", {})
        except (json.JSONDecodeError, OSError):
            continue
    return {}


def _size_label(bytes_: int | None) -> str:
    if bytes_ is None:
        return "—"
    mb = bytes_ / (1024 * 1024)
    return f"{mb:,.1f} MB"


# ---------- datasets.md ----------

def generate_datasets_md():
    manifest = load_manifest()
    snapshot_slugs = _load_snapshot_slugs(manifest.get("schema_version"))
    rows = []
    advisories: list[tuple[str, str, str]] = []  # (slug, short_name, advisory text)
    for spec in manifest["datasets"]:
        slug = spec["slug"]
        parquet = prepared_parquet(slug)
        snap = snapshot_slugs.get(slug, {})
        if parquet.exists():
            meta = pq.ParquetFile(parquet).metadata
            schema = pq.ParquetFile(parquet).schema_arrow
            row_count = f"{meta.num_rows:,}"
            row_groups = f"{meta.num_row_groups:,}"
            parquet_size = _size_label(parquet.stat().st_size)
            kind = _data_kind(spec, column_names={f.name for f in schema})
        else:
            # Fall back to the last-known snapshot entry so partial-build
            # maintainers don't dash-out everything they haven't built locally.
            r = snap.get("last_built_rows")
            rg = snap.get("last_built_row_groups")
            row_count = f"{r:,}" if isinstance(r, int) else "—"
            row_groups = f"{rg:,}" if isinstance(rg, int) else "—"
            parquet_size = _size_label(snap.get("parquet_bytes"))
            cols = snap.get("columns") or []
            names = {c["name"] for c in cols if isinstance(c, dict) and "name" in c}
            kind = _data_kind(spec, column_names=names or None)

        vortex = prepared_vortex(slug)
        if vortex.exists():
            vortex_size = _size_label(vortex.stat().st_size)
        else:
            vortex_size = _size_label(snap.get("vortex_bytes"))

        short = spec["short_name"]
        advisory = spec_field(spec, "license.scrape_advisory")
        # Flag the row by linking the short_name to a footnote anchor below.
        # Plain ⚠ glyph prefix so the row stays scannable even when the link
        # isn't followed.
        if advisory:
            anchor = f"scrape-advisory-{slug}"
            short = f"[⚠ {spec['short_name']}](#{anchor})"
            advisories.append((slug, spec["short_name"], advisory))
        full = spec["full_name"]
        # Collapse all line-ending sequences (LF / CR / CRLF) to a single space
        # so descriptions stay on one row even when an upstream description was
        # copied with embedded line breaks. Then escape pipes for the GFM table.
        desc = re.sub(r"[\r\n]+", " ", spec.get("description", "")).replace("|", "\\|").strip()
        url = spec_field(spec, "license.source_url") or (spec_field(spec, "fetch.urls", [""])[0] or "")
        lic = spec_field(spec, "license.spdx", "—")
        rows.append((short, full, desc, url, kind, lic, row_count, row_groups,
                     parquet_size, vortex_size))

    # Stable sort by short name
    rows.sort(key=lambda r: r[0].lower())

    header = ("| Dataset Short Name | Dataset Full Name | Dataset Description "
              "| Dataset Source (URL) | Data Kind | License | Row Count "
              "| Row Groups - Parquet | File Size - Parquet | File Size - Vortex |")
    sep = ("|--------------------|-------------------|---------------------"
           "|----------------------|-----------|---------|-----------"
           "|----------------------|---------------------|--------------------|")
    out = [_generation_header("datasets"), header, sep]
    for r in rows:
        out.append("| " + " | ".join(r) + " |")

    if advisories:
        out.append("")
        out.append("## ⚠ Scrape advisories")
        out.append("")
        out.append(
            "These datasets aggregate or reference content whose underlying "
            "licenses have not been individually cleared. The aggregator's "
            "declared license (the License column above) governs only the "
            "metadata it ships, not the content it points at. Read each "
            "advisory before redistributing or building on top of one of "
            "these slugs."
        )
        out.append("")
        for slug, short_name, advisory in sorted(advisories, key=lambda t: t[1].lower()):
            anchor = f"scrape-advisory-{slug}"
            esc = advisory.replace("\n", " ").strip()
            out.append(f'<a id="{anchor}"></a>')
            out.append(f"**{short_name}** (`{slug}`) — {esc}")
            out.append("")

    DATASETS_MD.write_text("\n".join(out) + "\n")
    note = f", {len(advisories)} scrape-flagged" if advisories else ""
    print(f"wrote {DATASETS_MD}  ({len(rows)} data rows{note})")


# ---------- handlers.md ----------

_STREAMING_RETURN_RE = re.compile(r"\breturn\s*\[\s*\]")

# Handler-import → pyproject.toml dep mapping. Only "format-specific" deps are
# surfaced — pyarrow/numpy/duckdb are always available so showing them is noise.
# Optional extras (kaggle, huggingface) live at the fetch stage, not in handlers.
_DEP_MAP = {
    "pandas":     "pandas",
    "openpyxl":   "openpyxl",
    "pyreadstat": "pyreadstat",
    "osmium":     "osmium",
    "zstandard":  "zstandard",
    "py7zr":      "py7zr",
    "unlzw3":     "unlzw3",
}


def _handler_extra_deps(src: str) -> list[str]:
    """Return sorted, deduped pyproject deps imported (directly) by a handler.

    Suppresses core deps (pyarrow / numpy / duckdb) and stdlib. xlsx_parse imports
    `pandas` only — pandas needs openpyxl as its xlsx engine, so we surface that
    transitive runtime requirement explicitly.
    """
    import ast
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []
    found: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                top = n.name.split(".", 1)[0]
                if top in _DEP_MAP:
                    found.add(_DEP_MAP[top])
        elif isinstance(node, ast.ImportFrom) and node.module:
            top = node.module.split(".", 1)[0]
            if top in _DEP_MAP:
                found.add(_DEP_MAP[top])
    if "pandas" in found:
        # pandas reads .xlsx via openpyxl; surface the implicit runtime dep.
        found.add("openpyxl")
    return sorted(found)


def generate_handlers_md():
    """One row per registered transform handler.

    Sources the registry from `scripts.pipeline.handlers._REGISTRY`, the
    handler's purpose from the first line of the module docstring, and
    streaming-vs-not from a regex scan of the module source for the
    `return []` contract (streaming handlers write parquet themselves and
    return an empty list so the write stage becomes a no-op). Manifest spec
    counts come from `transform.handler` usage in `sources.json`.
    """
    import inspect

    from . import handlers as h_mod

    manifest = load_manifest()
    usage: dict[str, list[str]] = {}
    for spec in manifest["datasets"]:
        h = spec_field(spec, "transform.handler", "")
        if not h:
            continue
        usage.setdefault(h, []).append(spec["slug"])

    header = ("| Handler | Purpose | Streaming | Extra Deps | # Manifest Specs | Example Slugs |")
    sep    = ("|---------|---------|-----------|------------|------------------|---------------|")
    lines = [_generation_header("handlers"), header, sep]

    for name in sorted(h_mod._REGISTRY):
        fn = h_mod._REGISTRY[name]
        mod = inspect.getmodule(fn)
        first_doc_line = ((mod.__doc__ or "").strip().split("\n", 1)[0]).strip() or "—"
        try:
            fn_src = inspect.getsource(fn)
        except (OSError, TypeError):
            fn_src = ""
        try:
            mod_src = inspect.getsource(mod) if mod else ""
        except (OSError, TypeError):
            mod_src = ""
        streaming = "yes" if _STREAMING_RETURN_RE.search(fn_src) else "no"
        deps = _handler_extra_deps(mod_src)
        deps_cell = ", ".join(f"`{d}`" for d in deps) if deps else "—"
        slugs = sorted(usage.get(name, []))
        n_specs = len(slugs)
        ex = ", ".join(f"`{s}`" for s in slugs[:2])
        if n_specs > 2:
            ex += f" (+{n_specs - 2:,} more)"
        if not ex:
            ex = "—"
        purpose = first_doc_line.replace("|", "\\|")
        lines.append(
            f"| `{name}` | {purpose} | {streaming} | {deps_cell} | {n_specs:,} | {ex} |"
        )

    HANDLERS_MD.write_text("\n".join(lines) + "\n")
    n_handlers = len(h_mod._REGISTRY)
    n_used = sum(1 for n in h_mod._REGISTRY if n in usage)
    print(f"wrote {HANDLERS_MD}  ({n_handlers} handlers, {n_used} used by ≥1 manifest spec)")


# ---------- snapshot.json ----------

def generate_snapshot(*, overwrite_missing: bool = False):
    """Per-slug record of canonical-build state for the TUI / agents.

    Walks the manifest; for each slug, captures:
      - parquet schema (top-level columns: name + type) when built
      - file sizes (parquet + vortex bytes) when present
      - row count from spec.expect.rows

    Default semantics: "update if present". If a slug has fresh build data
    on disk (parquet/vortex), the snapshot entry is rewritten from disk. If
    not, the prior snapshot entry is preserved (so iterative workflows that
    delete `outputs/v1/<slug>/` between builds don't clobber the snapshot
    each time docs is regenerated). expected_rows is always re-read from
    the manifest since that's the authoritative source.

    Pass overwrite_missing=True to emit null entries for slugs without
    fresh build data, regardless of any prior snapshot — use this for a
    full from-scratch regeneration.
    """
    import json
    manifest = load_manifest()
    existing_slugs: dict = {}
    if SNAPSHOT_JSON.exists() and not overwrite_missing:
        try:
            existing_slugs = json.loads(SNAPSHOT_JSON.read_text()).get("slugs", {})
        except Exception:
            pass
    out: dict = {
        "schema_version": manifest["schema_version"],
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "note": (
            "Auto-generated by scripts/pipeline/docs.py. Read by the TUI as a "
            "fallback when a local parquet isn't built. Regenerate after any "
            "build / schema change with `python -m scripts.pipeline.docs snapshot`."
        ),
        "slugs": {},
    }
    n_with_schema = 0
    n_preserved = 0
    for spec in manifest["datasets"]:
        slug = spec["slug"]
        parquet = prepared_parquet(slug)
        vortex = prepared_vortex(slug)
        expected_rows = spec_field(spec, "expect.rows")
        fresh: dict = {
            "expected_rows": expected_rows,
            "last_built_rows": None,        # populated below from parquet metadata
            "last_built_row_groups": None,  # populated below from parquet metadata
            "parquet_bytes": parquet.stat().st_size if parquet.exists() else None,
            "vortex_bytes": vortex.stat().st_size if vortex.exists() else None,
            "columns": None,  # populated below when schema is readable
        }
        if parquet.exists():
            try:
                from .spec import read_column_stats
                pf = pq.ParquetFile(parquet)
                # Full per-column metadata: name, type, length, null_count, min, max.
                # Falls back to schema-only on read failure.
                cols = read_column_stats(parquet)
                fresh["columns"] = cols if cols is not None else [
                    {"name": f.name, "type": str(f.type)} for f in pf.schema_arrow
                ]
                fresh["last_built_rows"] = int(pf.metadata.num_rows)
                fresh["last_built_row_groups"] = int(pf.metadata.num_row_groups)
                n_with_schema += 1
            except Exception as e:
                fresh["columns_error"] = f"{type(e).__name__}: {str(e)[:120]}"
        fresh_has_data = (fresh["parquet_bytes"] is not None
                          or fresh["vortex_bytes"] is not None
                          or fresh["columns"] is not None)
        if fresh_has_data or overwrite_missing or slug not in existing_slugs:
            out["slugs"][slug] = fresh
        else:
            preserved = dict(existing_slugs[slug])
            preserved["expected_rows"] = expected_rows
            out["slugs"][slug] = preserved
            n_preserved += 1
    SNAPSHOT_JSON.write_text(json.dumps(out, indent=2) + "\n")
    print(f"wrote {SNAPSHOT_JSON}  "
          f"({len(out['slugs'])} slugs, {n_with_schema} schemas captured this run, "
          f"{n_preserved} preserved from prior snapshot)")


# ---------- CLI ----------

def main(argv):
    overwrite_missing = "--overwrite-missing" in argv
    targets = [a for a in argv if not a.startswith("-")] or ["datasets", "handlers", "snapshot"]
    if "datasets" in targets:
        generate_datasets_md()
    if "handlers" in targets:
        generate_handlers_md()
    if "snapshot" in targets:
        generate_snapshot(overwrite_missing=overwrite_missing)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
