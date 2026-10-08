# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Filter and list datasets from sources.json without grepping the manifest by hand.

The manifest is a multi-hundred-KB JSON file with hundreds of entries. Greppable but
awkward when you want "every public-bi slug" or "every spec using handler X".
This is the read-only query layer over it.

Usage:
    python -m raincloud.pipeline.list_datasets                        # every slug
    python -m raincloud.pipeline.list_datasets tpch lineitem          # slug/name/description contains every WORD
    python -m raincloud.pipeline.list_datasets --handler tighten_types
    python -m raincloud.pipeline.list_datasets --license CC0-1.0
    python -m raincloud.pipeline.list_datasets --fetch-type kaggle
    python -m raincloud.pipeline.list_datasets --reader csv
    python -m raincloud.pipeline.list_datasets --vortex              # the catalog has Vortex for it
    python -m raincloud.pipeline.list_datasets --no-vortex           # no Vortex: measured unavailable, or left out
    python -m raincloud.pipeline.list_datasets --kaggle-tos          # requires_interactive_accept
    python -m raincloud.pipeline.list_datasets --scrape              # license.scrape_advisory non-null
    python -m raincloud.pipeline.list_datasets --hydrate             # hydrated datasets only
    python -m raincloud.pipeline.list_datasets --stale-version       # recorded built only under an older schema_version
    python -m raincloud.pipeline.list_datasets --local               # an artifact is on this machine's disk
    python -m raincloud.pipeline.list_datasets --showcase encoding   # editorial tier (repeatable)
    python -m raincloud.pipeline.list_datasets --tag geospatial      # domain tag (repeatable)
    python -m raincloud.pipeline.list_datasets --size s --size m     # size bucket (repeatable)
    python -m raincloud.pipeline.list_datasets --trait has_nested    # shape trait; ! to negate
    python -m raincloud.pipeline.list_datasets --view encoding       # named preset (clears other axes)
    python -m raincloud.pipeline.list_datasets --long                # slug + key fields
    python -m raincloud.pipeline.list_datasets --json                # one JSON object per row
    python -m raincloud.pipeline.list_datasets --count               # just the count

The default output is one slug per line, safe to pipe (`| xargs raincloud build`).
On a terminal, hydrated datasets are marked `[hydrated]`; --long and --json
always carry a `hydrated` field.

Inspection modes — these read built parquet (or vortex) files instead of
just the manifest, so they only show slugs that have actually been built:

    python -m raincloud.pipeline.list_datasets --columns                    # every (slug, column, type) row
    python -m raincloud.pipeline.list_datasets --columns --column-grep emb  # only columns matching regex
    python -m raincloud.pipeline.list_datasets --columns --source vortex    # vortex schema instead of parquet
    python -m raincloud.pipeline.list_datasets --coverage                   # per-distinct-type counts + examples

Filters compose with AND. Pass --grep PATTERN for a regex match against
slug + short_name + full_name + description. WORD terms rank the matches: a
term that is a whole word of the slug first, then the slug, the names, and
the description last.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from typing import Any

from raincloud._catalog import recorded_unavailable
from raincloud._formats import EXPORTED_FORMATS, vortex_cells, vortex_skip_reason
from raincloud._suggest import hint
from raincloud.exceptions import CatalogError

from .discovery import (
    SHOWCASE_TIERS,
    SIZE_BUCKETS,
    TAG_VOCAB,
    VIEW_PRESETS,
    FilterState,
    apply_preset,
    effective_fetch_type,
    format_column_line,
)
from .spec import (
    REPO_ROOT,
    is_hydrated,
    iter_datasets,
    load_manifest,
    outputs_root,
    prepared_artifact,
    prepared_parquet,
    prepared_vortex,
    spec_field,
)


def _load_snapshot(schema_version: int | None = None) -> dict:
    """Load the on-disk snapshot keyed by slug, returning its `slugs` mapping.

    Candidate chain (first hit wins), the same two candidates as
    `docs._load_snapshot_slugs`:

        1. `docs/snapshot.json`                     (gitignored scratch — a
           local regen wins)
        2. `docs/v{schema_version}/snapshot.json`   (tracked canonical for the
           live manifest version — carries the v{n} enrichment for built slugs)

    The versioned candidate is only inserted when `schema_version` is given
    (the caller passes the live manifest's `schema_version`). As in docs, a
    candidate declaring another schema_version is skipped with a note (a stale
    v1 scratch copy must not describe v2 slugs), and there is no docs/v1
    fallback.

    Returns {} when no candidate exists. Unlike docs, which must not preserve
    from a guess, a malformed candidate here is named on stderr and skipped:
    this is a listing, and the tracked copy behind it is still a fact.
    """
    import json as _json

    from raincloud.catalogs import selected_context
    context = selected_context()
    if context is not None:
        return context.snapshot.get("slugs", {})
    candidates = [REPO_ROOT / "docs" / "snapshot.json"]
    if schema_version is not None:
        candidates.append(REPO_ROOT / "docs" / f"v{schema_version}" / "snapshot.json")
    for p in candidates:
        if p.exists():
            try:
                blob = _json.loads(p.read_text())
            except (OSError, ValueError) as exc:
                print(f"[warn] skipping unreadable snapshot {p}: {exc}", file=sys.stderr)
                continue
            declared = blob.get("schema_version") if isinstance(blob, dict) else None
            if schema_version is not None and declared is not None and declared != schema_version:
                print(f"[warn] skipping snapshot {p}: schema_version {declared}, "
                      f"the manifest is {schema_version}", file=sys.stderr)
                continue
            if isinstance(blob, dict) and "slugs" in blob:
                return blob["slugs"]
            if isinstance(blob, dict) and "datasets" in blob:
                return {d["slug"]: d for d in blob["datasets"]}
            if isinstance(blob, dict):
                return blob
    return {}


_VERSION_SNAPSHOTS: dict[int, dict] = {}


def _version_snapshot(schema_version: int) -> dict:
    """`slugs` mapping from exactly `docs/v{n}/snapshot.json` — no scratch, no fallback.

    Deliberately NOT `_load_snapshot`: that chain prefers the gitignored scratch
    regen and falls back across versions, which is right for enrichment but wrong
    here. Deciding whether a slug was built under v1 but not v{n} needs each
    version's tracked snapshot as its own independent fact.

    Only an absent file means "no records" ({}). A present snapshot that cannot
    be read or is not a JSON object raises CatalogError: reading it as empty
    would flag every slug stale. A failed load is never cached.
    """
    from raincloud.catalogs import selected_context
    context = selected_context()
    if context is not None:
        return context.snapshot.get("slugs", {}) if context.manifest["schema_version"] == schema_version else {}
    if schema_version in _VERSION_SNAPSHOTS:
        return _VERSION_SNAPSHOTS[schema_version]
    import json as _json
    p = REPO_ROOT / "docs" / f"v{schema_version}" / "snapshot.json"
    try:
        blob = _json.loads(p.read_text())
    except FileNotFoundError:
        blob = {}
    except (OSError, ValueError) as exc:
        raise CatalogError(f"cannot read tracked snapshot {p}: {exc}") from exc
    slugs = blob.get("slugs", {}) if isinstance(blob, dict) else None
    if not isinstance(slugs, dict):
        raise CatalogError(f"tracked snapshot {p} is not a {{'slugs': {{...}}}} object")
    _VERSION_SNAPSHOTS[schema_version] = slugs
    return slugs


def _current_schema_version() -> int:
    # load_manifest() already rejects a manifest whose schema_version is not one
    # this build knows, so there is nothing left to default to here.
    return int(load_manifest()["schema_version"])


# Fingerprint proving a record was produced by a given schema_version's pipeline.
# It must be a field that version INTRODUCED: v2 added the canonical Arrow spine,
# so `arrow_sha256` appears only on records a v2 build wrote. Size/row fields are
# useless here — `parquet_bytes` and `last_built_rows` are carried forward by the
# snapshot-preservation fallback into every version's records, so they say
# nothing about WHICH version built a slug. Every schema_version bump needs an
# entry here, or staleness detection for that version switches off.
#
# All of this reads the maintainer's TRACKED snapshots: "recorded as built", not
# "present on this machine" (that is `--local`, or `raincloud status`).
_VERSION_BUILD_MARKER = {2: "arrow_sha256"}


def built_version(slug: str, current: int | None = None) -> int | None:
    """Newest schema_version whose tracked snapshot records `slug` as built under it.

    Returns `current` when the current version's fingerprint is present; otherwise
    the newest older version whose snapshot carries the slug at all (its record was
    inherited from there); `None` when no version's snapshot knows the slug.
    """
    if current is None:
        current = _current_schema_version()
    marker = _VERSION_BUILD_MARKER.get(current)
    rec = _version_snapshot(current).get(slug)
    if rec is not None and (marker is None or rec.get(marker) is not None):
        # No known fingerprint for this version => can't claim staleness, so a
        # present record counts as current rather than silently flagging the catalog.
        return current
    for v in range(current - 1, 0, -1):
        if _version_snapshot(v).get(slug) is not None:
            return v
    return None


def is_stale_version(slug: str, current: int | None = None, *, built: int | None = None) -> bool:
    """True when `slug`'s recorded artifacts predate the current schema_version.

    The "might need updating" signal: the catalog knows this slug, but nothing was
    recorded as built for it under the current version, so its artifacts are
    superseded. Pass `built` (a `built_version` result) to skip recomputing it.
    """
    if current is None:
        current = _current_schema_version()
    v = built if built is not None else built_version(slug, current)
    return v is not None and v < current


def local_formats(spec: dict, manifest: dict) -> list[str]:
    """Formats of `spec` whose artifact is on this machine's disk, for the
    current schema_version. Presence only: `raincloud status` checks staleness."""
    slug = spec["slug"]
    return [fmt for fmt in ("arrow", *EXPORTED_FORMATS) if prepared_artifact(slug, fmt, manifest).is_file()]


def _filter_state_from_args(args) -> FilterState:
    """Build FilterState from the parsed argparse Namespace.

    --view replaces ALL other facet selections (preset is the complete spec).
    Mixing a preset with individual facet flags is incoherent UX, so when
    --view is set we short-circuit and return the preset state unmodified.
    Other inline filters (handler, reader, kaggle_tos, scrape, hydrate,
    stale_version, local, grep and the positional WORD terms) still apply since
    they're outside FilterState's domain.
    """
    if getattr(args, "view", None):
        return apply_preset(args.view)
    state = FilterState()
    # Additive: each repeated flag adds to the corresponding axis.
    for axis_name, src in (
        ("showcase", args.showcase),
        ("tag", args.tag),
        ("size", args.size),
    ):
        if src:
            getattr(state, axis_name).update(src)
    # The existing --license, --fetch-type flags are single-value.
    if getattr(args, "license", None):
        state.license.add(args.license)
    if getattr(args, "fetch_type", None):
        state.fetch_type.add(args.fetch_type)
    # Trait flags: prefix '!' negates.
    for flag in args.trait or []:
        if flag.startswith("!"):
            state.trait_negated.add(flag[1:])
        else:
            state.trait.add(flag)
    # --vortex / --no-vortex are applied in `_matches`, which also knows the
    # catalog's measurements; FilterState.vortex reads the policy alone.
    return state


def _haystack(spec: dict) -> str:
    return " ".join((
        spec.get("slug", ""),
        spec.get("short_name", ""),
        spec.get("full_name", ""),
        spec.get("description", ""),
    ))


def _term_rank(spec: dict, terms) -> int:
    """How strongly `spec` matches every WORD term: 0 each is a whole word of
    the slug, 1 each is in the slug, 2 each is in the slug or names, 3 else
    (some term only in the description)."""
    terms = [t.lower() for t in terms]
    slug = spec.get("slug", "").lower()
    if all(t in re.split(r"[-_]+", slug) for t in terms):
        return 0
    if all(t in slug for t in terms):
        return 1
    names = " ".join((slug, spec.get("short_name", ""), spec.get("full_name", ""))).lower()
    return 2 if all(t in names for t in terms) else 3


def _matches(spec: dict, args, state: FilterState, snapshot: dict, schema_version: int,
             manifest: dict) -> bool:
    """Apply the inline filters not covered by FilterState, then defer the
    closed-vocab axes (license / fetch_type / vortex / showcase / tag / size /
    trait) to FilterState.matches().
    """
    if args.handler and spec_field(spec, "transform.handler") != args.handler: return False
    if args.reader and spec_field(spec, "parse.reader") != args.reader: return False
    if args.kaggle_tos and not spec_field(spec, "fetch.requires_interactive_accept", False): return False
    if args.scrape and not spec_field(spec, "license.scrape_advisory"): return False
    if args.hydrate and not is_hydrated(spec): return False
    if args.stale_version and not is_stale_version(spec["slug"], schema_version): return False
    if args.local and not local_formats(spec, manifest): return False
    if (args.vortex or args.no_vortex) and has_vortex(spec, snapshot.get(spec["slug"], {}), manifest) != args.vortex:
        return False
    if not state.matches(spec=spec, snapshot=snapshot.get(spec["slug"], {}), schema_version=schema_version):
        return False
    haystack = _haystack(spec)
    if args.grep and not re.search(args.grep, haystack, flags=re.IGNORECASE): return False
    if any(term.lower() not in haystack.lower() for term in getattr(args, "terms", ())): return False
    return True


def vortex_unavailable(spec: dict, snapshot_entry: dict, manifest: dict) -> dict | None:
    """The catalog's measurement that `spec`'s Vortex file cannot be made at its
    recipe (`<fmt>_unavailable` in the snapshot), or None."""
    specs = {d["slug"]: d for d in manifest["datasets"]}
    return recorded_unavailable(spec, snapshot_entry, "vortex", manifest["schema_version"], specs)


def has_vortex(spec: dict, snapshot_entry: dict, manifest: dict) -> bool:
    """Whether the catalog has Vortex for `spec`: its policy exports Vortex and
    no build measured the writer unable to produce it."""
    return (bool(vortex_cells(spec, manifest["schema_version"], manifest))
            and vortex_unavailable(spec, snapshot_entry, manifest) is None)


def _long_row(spec: dict, schema_version: int, manifest: dict, snapshot_entry: dict | None = None) -> dict[str, Any]:
    snapshot_entry = snapshot_entry or {}
    vortex = has_vortex(spec, snapshot_entry, manifest)
    built = built_version(spec["slug"], schema_version)
    return {
        "slug":                spec["slug"],
        "handler":             spec_field(spec, "transform.handler"),
        "fetch_type":          effective_fetch_type(spec),
        "reader":              spec_field(spec, "parse.reader"),
        "license":             spec_field(spec, "license.spdx"),
        "rows":                spec_field(spec, "expect.rows"),
        "vortex":              vortex,
        # Why there is no Vortex file: the catalog's measurement, else the policy.
        "vortex_skip_reason":  vortex_skip_reason(spec, schema_version, snapshot_entry),
        "vortex_unavailable":  vortex_unavailable(spec, snapshot_entry, manifest),
        "scrape_advisory":     spec_field(spec, "license.scrape_advisory"),
        "hydrated":            is_hydrated(spec),
        "derived_from":        (spec.get("derive") or {}).get("from"),
        "row_stability":       (spec.get("expect") or {}).get("row_stability"),
        "references":          spec.get("references") or [],
        "short_name":          spec.get("short_name"),
        # Recorded in the tracked snapshots — says nothing about this machine.
        "built_version":       built,
        "stale_version":       is_stale_version(spec["slug"], schema_version, built=built),
        # On this machine's disk, for the current schema_version.
        "local":               local_formats(spec, manifest),
    }


def _render_long_table(rows: list[dict]) -> str:
    if not rows: return ""
    headers = ("slug", "handler", "fetch", "reader", "license", "rows", "vortex", "scrape", "hydrated",
               "recorded", "local")
    cells = [headers]
    for r in rows:
        # `recorded` names the newest version whose TRACKED snapshot records the
        # slug as built (the maintainer's catalog, not this machine): the CURRENT
        # version reads as v{n}, an older one is flagged `v1!` (superseded — needs
        # a rebuild), and `·` means never recorded. `local` lists the formats on
        # this machine's disk.
        bv = r.get("built_version")
        built = "·" if bv is None else (f"v{bv}!" if r.get("stale_version") else f"v{bv}")
        cells.append((
            r["slug"],
            r["handler"] or "",
            r["fetch_type"] or "",
            r["reader"] or "",
            r["license"] or "",
            f"{r['rows']:,}" if isinstance(r["rows"], int) else "—",
            "✓" if r["vortex"] else "·",
            "⚠" if r["scrape_advisory"] else "·",
            "✓" if r["hydrated"] else "·",
            built,
            ",".join(r.get("local") or []) or "·",
        ))
    widths = [max(len(c) for c in (row[i] for row in cells)) for i in range(len(headers))]
    out = []
    for i, row in enumerate(cells):
        out.append("  ".join(c.ljust(widths[j]) for j, c in enumerate(row)))
        if i == 0: out.append("  ".join("-" * w for w in widths))
    return "\n".join(out)


# ---------- column / coverage inspection ----------

def _collapse_bracketed(ty: str, prefix: str, open_b: str, close_b: str) -> str:
    """Replace `prefix<body>` (or `prefix(body)`) with `prefix...<close>`.
    Handles nested brackets via depth counter."""
    out = []
    i = 0
    while True:
        j = ty.find(prefix, i)
        if j < 0:
            out.append(ty[i:])
            return "".join(out)
        out.append(ty[i:j + len(prefix)])
        depth = 1
        k = j + len(prefix)
        while k < len(ty) and depth > 0:
            if ty[k] == open_b:
                depth += 1
            elif ty[k] == close_b:
                depth -= 1
            k += 1
        out.append("...")
        out.append(close_b)
        i = k


def _canonicalize_type(ty: str) -> str:
    """Collapse STRUCT bodies so distinct struct *shapes* don't each become
    their own coverage row. Handles both DuckDB-style `STRUCT(...)` (used
    by --coverage's DuckDB DESCRIBE path) and pyarrow-style `struct<...>`
    (used by the TUI's per-slug coverage modal). Scalars keep their
    parameters — `DECIMAL(10, 2)`, `TIMESTAMP WITH TIME ZONE` are unchanged."""
    ty = _collapse_bracketed(ty, "STRUCT(", "(", ")")
    ty = _collapse_bracketed(ty, "struct<", "<", ">")
    return ty


def _iter_columns(matched: list[dict], source: str) -> list[dict]:
    """Walk built parquet (or vortex) for each matched slug and yield one
    dict per top-level column. Slugs with no built file are skipped silently
    — column inspection is only meaningful for built outputs.
    """
    rows: list[dict] = []
    if source == "vortex":
        try:
            import vortex as vx
        except ImportError:
            print("--source vortex requires `vortex-data` (uv sync pulls it in)",
                  file=sys.stderr)
            return rows
    else:
        import pyarrow.parquet as pq

    for spec in matched:
        slug = spec["slug"]
        if source == "vortex":
            path = prepared_vortex(slug)
            if not path.exists(): continue
            try:
                vf = vx.open(str(path))
                schema = vf.dtype.to_arrow_schema()
            except BaseException as e:
                print(f"  [skip] {slug}: {type(e).__name__}: {str(e).splitlines()[0][:80]}",
                      file=sys.stderr)
                continue
            for f in schema:
                rows.append({"slug": slug, "column": f.name, "type": str(f.type),
                             "source": "vortex"})
        else:
            path = prepared_parquet(slug)
            if not path.exists(): continue
            try:
                pf = pq.ParquetFile(path)
            except Exception as e:
                print(f"  [skip] {slug}: {type(e).__name__}: {str(e).splitlines()[0][:80]}",
                      file=sys.stderr)
                continue
            for f in pf.schema_arrow:
                rows.append({"slug": slug, "column": f.name, "type": str(f.type),
                             "source": "parquet"})
    return rows


def _filter_columns(col_rows: list[dict], pattern: str | None) -> list[dict]:
    if not pattern: return col_rows
    rx = re.compile(pattern, re.IGNORECASE)
    return [r for r in col_rows if rx.search(r["column"])]


def _render_columns_table(col_rows: list[dict]) -> str:
    if not col_rows: return ""
    headers = ("slug", "column", "type")
    cells = [headers]
    for r in col_rows:
        cells.append((r["slug"], r["column"], r["type"]))
    widths = [max(len(c) for c in (row[i] for row in cells)) for i in range(len(headers))]
    out = []
    for i, row in enumerate(cells):
        out.append("  ".join(c.ljust(widths[j]) for j, c in enumerate(row)))
        if i == 0: out.append("  ".join("-" * w for w in widths))
    return "\n".join(out)


def _coverage_summary(col_rows: list[dict]) -> list[dict]:
    """Aggregate column rows into one row per canonical type."""
    by_type: dict[str, list[tuple[str, str]]] = {}
    for r in col_rows:
        canon = _canonicalize_type(r["type"])
        by_type.setdefault(canon, []).append((r["slug"], r["column"]))
    out = []
    for ty in sorted(by_type):
        entries = by_type[ty]
        out.append({
            "type": ty,
            "columns": len(entries),
            "datasets": len({slug for slug, _ in entries}),
            "examples": [f"{slug}.{col}" for slug, col in entries[:3]],
        })
    return out


def _render_coverage_table(rows: list[dict]) -> str:
    if not rows: return ""
    headers = ("type", "columns", "datasets", "examples")
    cells = [headers]
    for r in rows:
        ex = ", ".join(r["examples"])
        if r["columns"] > len(r["examples"]):
            ex += f" (+{r['columns'] - len(r['examples'])})"
        cells.append((r["type"], str(r["columns"]), str(r["datasets"]), ex))
    widths = [max(len(c) for c in (row[i] for row in cells)) for i in range(len(headers))]
    out = []
    for i, row in enumerate(cells):
        out.append("  ".join(c.ljust(widths[j]) for j, c in enumerate(row)))
        if i == 0: out.append("  ".join("-" * w for w in widths))
    return "\n".join(out)


def _inspect(slug: str) -> int:
    """Render the TUI detail-pane equivalent as plain text."""
    import json as _json
    manifest = load_manifest()
    spec = next((s for s in manifest["datasets"] if s["slug"] == slug), None)
    if spec is None:
        print(hint(slug, [s["slug"] for s in manifest["datasets"]],
                   everything="List them with `raincloud list`."), file=sys.stderr)
        return 2

    print(f"# {slug} — {spec.get('short_name', '')}")
    if spec.get("showcase"):
        print(f"showcase: {', '.join(spec['showcase'])}")
    if spec.get("tags"):
        print(f"tags:     {', '.join(spec['tags'])}")
    lic = (spec.get("license") or {}).get("spdx")
    if lic:
        print(f"license:  {lic}")
    print()
    desc = (spec.get("description") or "").strip()
    if desc:
        print(desc)
        print()

    from .promote_profiles import profile_candidates, profile_search_paths
    where = dict(repo_root=REPO_ROOT, output_root=outputs_root(manifest))
    profile: dict | None = None
    last_error: Exception | None = None
    last_error_path = None
    chosen = None
    for path in profile_candidates(slug, manifest, **where):
        try:
            profile = _json.loads(path.read_text())
            chosen = path
            break
        except (OSError, ValueError) as e:
            last_error = e
            last_error_path = path
            continue
    if profile is None:
        if last_error is not None:
            print(f"profile.json malformed at {last_error_path}: {last_error}", file=sys.stderr)
            return 2
        searched = profile_search_paths(slug, manifest, **where)
        checked = " and ".join(str(p) for p in dict.fromkeys(searched))
        print(
            f"no profile yet for {slug} — checked {checked}; "
            f"run `python -m raincloud.pipeline.profile {slug}`"
        )
        return 0
    if manifest["schema_version"] != 1 and chosen is not None and chosen.parent == REPO_ROOT / "docs" / "v1" / "profiles":
        print(f"(v1 profile: describes the v1 build, not the current v{manifest['schema_version']} artifact)")
    print(f"rows: {profile['row_count']}   sample_rows: {profile.get('sample_rows')}")
    print(f"columns ({len(profile['columns'])}):")
    for name, col in profile["columns"].items():
        print(format_column_line(name, col))
    return 0


def _print_vocab_help(manifest: dict, *, vocab_name: str) -> int:
    """Print closed vocab + per-value count from the live manifest."""
    if vocab_name == "tags":
        vocab = TAG_VOCAB
        field = "tags"
    else:
        vocab = SHOWCASE_TIERS
        field = "showcase"
    counts = {v: 0 for v in vocab}
    for spec in manifest["datasets"]:
        for v in spec.get(field) or []:
            if v in counts:
                counts[v] += 1
    for v in vocab:
        print(f"  {v:<20} ({counts[v]})")
    return 0


def _main(argv: list[str] | None = None, prog: str | None = None) -> int:
    ap = argparse.ArgumentParser(prog=prog, description=__doc__.split("\n", 1)[0])
    ap.add_argument("terms", nargs="*", metavar="WORD",
                    help="only datasets whose slug, name or description contains every word")
    ap.add_argument("--handler", help="filter by transform.handler name")
    ap.add_argument("--license", help="filter by license.spdx")
    ap.add_argument("--fetch-type", help="filter by fetch.type (http, kaggle, huggingface, custom, "
                                          "generated), or derived for a dataset built from a parent")
    ap.add_argument("--reader", help="filter by parse.reader (csv, parquet, jsonl, xml, pbf, custom)")
    ap.add_argument("--vortex", action="store_true",
                    help="only specs the catalog has Vortex for (exported, and not measured unavailable)")
    ap.add_argument("--no-vortex", action="store_true",
                    help="only specs without Vortex: a build measured the writer unable to produce it "
                         "(--json: vortex_unavailable), or the export policy leaves it out")
    ap.add_argument("--kaggle-tos", action="store_true",
                    help="only kaggle specs gated behind a one-time ToS click-through")
    ap.add_argument("--scrape", action="store_true",
                    help="only specs with a non-null license.scrape_advisory "
                         "(scrape corpora whose underlying licenses aren't cleared)")
    ap.add_argument("--stale-version", action="store_true",
                    help="only slugs the tracked snapshots record as built under an older "
                         "schema_version and not the current one (superseded — candidates for a "
                         "rebuild). This is the maintainer's record, not this machine's disk; see "
                         "--local. Never-recorded slugs are excluded: those are unbuilt, not stale.")
    ap.add_argument("--local", action="store_true",
                    help="only slugs with an artifact on this machine's disk for the current "
                         "schema_version (`raincloud status` also checks staleness)")
    ap.add_argument("--hydrate", action="store_true",
                    help="only hydrated datasets (<parent>-hydrated: URL columns fetched from the open web)")
    ap.add_argument("--showcase", action="append", choices=list(SHOWCASE_TIERS),
                    help="filter by editorial showcase tier; repeatable")
    ap.add_argument("--tag", action="append", choices=list(TAG_VOCAB),
                    help="filter by domain tag; repeatable")
    ap.add_argument("--size", action="append", choices=list(SIZE_BUCKETS),
                    help="filter by size bucket; repeatable")
    ap.add_argument("--trait", action="append",
                    help="filter by shape trait (e.g. has_nested); prefix with ! to negate; repeatable")
    ap.add_argument("--view", choices=list(VIEW_PRESETS),
                    help="apply a named view preset (replaces other selections)")
    ap.add_argument("--grep", help="regex over slug + short_name + full_name + description (case-insensitive)")
    ap.add_argument("--long", action="store_true", help="emit a wide table with key fields")
    ap.add_argument("--json", action="store_true", help="emit one JSON object per matching dataset")
    ap.add_argument("--count", action="store_true", help="emit just the count of matches")

    # Inspection modes — read built parquet/vortex instead of just the manifest.
    ap.add_argument("--columns", action="store_true",
                    help="emit one row per (slug, column, type) across built outputs")
    ap.add_argument("--coverage", action="store_true",
                    help="emit one row per distinct type with column / dataset counts and examples")
    ap.add_argument("--column-grep", metavar="PATTERN",
                    help="when used with --columns, only emit columns whose name matches this regex (case-insensitive)")
    ap.add_argument("--source", choices=("parquet", "vortex"), default="parquet",
                    help="when used with --columns / --coverage, read from parquet (default) or vortex outputs")
    ap.add_argument("--inspect", metavar="SLUG",
                    help="render the detail view for one slug (description + per-column profile)")
    ap.add_argument("--tags-help", action="store_true",
                    help="list the closed tag vocab + per-tag counts in the manifest")
    ap.add_argument("--showcase-help", action="store_true",
                    help="list the showcase tiers + per-tier counts in the manifest")
    # Intermixed, so WORD terms may follow options (`tpch --count lineitem`).
    args = ap.parse_intermixed_args(argv)

    if args.vortex and args.no_vortex:
        print("--vortex and --no-vortex are mutually exclusive", file=sys.stderr)
        return 2
    if args.columns and args.coverage:
        print("--columns and --coverage are mutually exclusive", file=sys.stderr)
        return 2

    if args.inspect:
        return _inspect(args.inspect)
    manifest = load_manifest()
    if args.tags_help:
        return _print_vocab_help(manifest, vocab_name="tags")
    if args.showcase_help:
        return _print_vocab_help(manifest, vocab_name="showcase")

    m = manifest
    # A value no dataset carries is almost always a typo: say so rather than
    # printing nothing with exit 0.
    for flag, value, field in (("--handler", args.handler, "transform.handler"),
                               ("--reader", args.reader, "parse.reader"),
                               ("--license", args.license, "license.spdx")):
        known = {spec_field(s, field) for s in m["datasets"]} - {None}
        if value is not None and value not in known:
            print(hint(value, known, noun=f"{flag} value"), file=sys.stderr)
            return 2
    known = {effective_fetch_type(s) for s in m["datasets"]} - {None}
    if args.fetch_type is not None and args.fetch_type not in known:
        print(hint(args.fetch_type, known, noun="--fetch-type value"), file=sys.stderr)
        return 2
    state = _filter_state_from_args(args)
    snapshot = _load_snapshot(m.get("schema_version"))
    matched = [s for s in iter_datasets(m) if _matches(s, args, state, snapshot, m["schema_version"], m)]
    if args.terms:
        matched.sort(key=lambda s: _term_rank(s, args.terms))

    if args.columns or args.coverage:
        col_rows = _iter_columns(matched, args.source)
        col_rows = _filter_columns(col_rows, args.column_grep)
        if args.coverage:
            cov = _coverage_summary(col_rows)
            if args.json:
                for r in cov:
                    json.dump(r, sys.stdout); sys.stdout.write("\n")
                return 0
            if args.count:
                print(len(cov)); return 0
            print(_render_coverage_table(cov))
            return 0
        # --columns
        if args.json:
            for r in col_rows:
                json.dump(r, sys.stdout); sys.stdout.write("\n")
            return 0
        if args.count:
            print(len(col_rows)); return 0
        print(_render_columns_table(col_rows))
        return 0

    if args.count:
        print(len(matched))
        return 0
    if args.json:
        for s in matched:
            json.dump(_long_row(s, m["schema_version"], m, snapshot.get(s["slug"])), sys.stdout)
            sys.stdout.write("\n")
        return 0
    if args.long:
        if matched:
            print(_render_long_table([_long_row(s, m["schema_version"], m, snapshot.get(s["slug"]))
                                      for s in matched]))
        return 0
    # Hydrated datasets are marked for a person reading a terminal (`raincloud
    # describe` says why); piped output stays one bare slug per line.
    mark = sys.stdout.isatty()
    for s in matched:
        print(f"{s['slug']}  [hydrated]" if mark and is_hydrated(s) else s["slug"])
    if not matched and args.terms:
        print(f"no datasets match {' '.join(args.terms)!r}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None, prog: str | None = None) -> int:
    from raincloud.catalogs import operation
    from raincloud.config import get_config
    try:
        with operation(get_config()):
            code = _main(argv, prog=prog)
        # Inside the try: the last block-buffered chunk would otherwise be
        # flushed at interpreter exit, where a reader that left raises
        # BrokenPipeError past this handler and the exit code is 120.
        sys.stdout.flush()
        return code
    except BrokenPipeError:
        # `raincloud list --long | head`: the reader left, which is not an error.
        # Point stdout at devnull so the interpreter's final flush stays quiet.
        try:
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except (OSError, ValueError):
            pass  # stdout has no real descriptor (captured): nothing to quiet
        return 0
    except CatalogError as exc:
        print(f"list: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
