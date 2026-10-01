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
                          A format a build measured unavailable (the build
                          record's "unavailable" entry at the current
                          recipe) is carried as `<fmt>_unavailable`, with
                          null sha/bytes/writer; datasets.md shows it as
                          `unavailable`, and a compliance ledger in which a
                          writer round-trips it draws a `[stale opt-out]`
                          warning.

Per-column / per-coverage / vortex-skip / hydrated detail used to live as
markdown too, but the rendering was unscannable and duplicated state
already queryable via the TUI and `list_datasets`. Those views moved to:

    python -m raincloud.pipeline.list_datasets --grep '^<slug>\b' --columns    # column listing
    python -m raincloud.pipeline.list_datasets --coverage               # type coverage
    python -m raincloud.pipeline.list_datasets --no-vortex --long       # slugs without Vortex, and why
    python -m raincloud.pipeline.list_datasets --hydrate --long         # hydrated datasets
    python -m raincloud.pipeline.browse                                 # interactive

Hydration policy / philosophy lives in the hand-maintained
`HYDRATING.md` (preamble only, no auto-generated per-slug list).

Usage:

    python -m raincloud.pipeline.docs            # all three (datasets + handlers + snapshot)
    python -m raincloud.pipeline.docs datasets   # just datasets.md
    python -m raincloud.pipeline.docs handlers   # just handlers.md
    python -m raincloud.pipeline.docs snapshot   # just snapshot.json
"""
from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from raincloud._formats import ALL_FORMATS

from .discovery import SHOWCASE_TIERS, _is_variant_field, bucket_for_size
from .spec import (
    REPO_ROOT,
    load_manifest,
    prepared_artifact,
    prepared_parquet,
    prepared_vortex,
    spec_field,
)


def _sha256_or_reuse(
    path,
    current_size: int | None,
    prior_size: int | None,
    prior_sha: str | None,
    *,
    force: bool = False,
) -> str | None:
    """Reuse `prior_sha` when size is unchanged + prior sha is known; else hash.

    Bytes-on-disk are content-addressed in the snapshot, so a matching size
    is a near-perfect indicator the content is unchanged. Avoids re-streaming
    multi-GB artifacts on every snapshot regen.

    The size-only reuse has one blind spot: a rebuild that produces
    *different content at the same byte length* keeps the stale sha, which then
    permanently fails `publish`'s integrity gate (re-running plain `docs
    snapshot` reuses the same stale sha). `force=True` (the `--rehash` flag)
    recomputes every present file's sha to break out of that, while still
    preserving the prior sha for files that are missing this run.
    """
    if current_size is None:
        # File missing; preserve the prior sha so partial regens don't dash
        # out tracked ground truth (existing fallback semantics).
        return prior_sha
    if not force and prior_sha is not None and prior_size == current_size:
        return prior_sha
    return _sha256_for_path(path)


def _sha256_for_path(path) -> str | None:
    """Stream a file's sha256, or None if it doesn't exist.

    Delegates to the loader's single sha256 implementation so the pipeline and
    the loader can't drift on chunk size / semantics.
    """
    from raincloud._cache import sha256_file
    p = Path(path)
    if not p.exists():
        return None
    return sha256_file(p)


def _generation_header(kind: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return (f"<!-- AUTO-GENERATED {kind} by raincloud/pipeline/docs.py at {ts}. "
            f"Regenerate with: python -m raincloud.pipeline.docs {kind}. DO NOT EDIT. -->")


DATASETS_MD = REPO_ROOT / "docs" / "datasets.md"
HANDLERS_MD = REPO_ROOT / "docs" / "handlers.md"
SNAPSHOT_JSON = REPO_ROOT / "docs" / "snapshot.json"


def _tracked_snapshot_json(manifest: dict) -> Path | None:
    """The TRACKED, version-scoped snapshot (`docs/v{n}/snapshot.json`).

    The canonical committed copy, as opposed to `SNAPSHOT_JSON` (the gitignored
    top-level scratch regen target). Used as the preservation fallback when no
    scratch copy exists, so a regen on a clean tree can't null out the data for
    slugs that aren't built locally.
    """
    version = manifest.get("schema_version")
    if not version:
        return None
    return REPO_ROOT / "docs" / f"v{version}" / "snapshot.json"


# ---------- shared helpers ----------

def _data_kind(spec: dict, column_names: set[str] | None = None) -> str:
    """Best-effort inference of the 'Data Kind' label.

    `column_names` may come from a live parquet schema OR from the snapshot
    fallback — both cases need to recognise the `content` blob convention.
    Inferred from `parse.reader` + `transform.handler`; falls through to
    "Tabular (CSV)" for the default case.
    """
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


def _preserved_slugs(candidates: list[Path | None], schema_version: int | None) -> dict[str, dict]:
    """The `slugs` of the first candidate snapshot that reads, is non-empty and
    is of `schema_version`.

    A candidate that does not parse, or describes another schema_version, is
    skipped with a warning naming it. When no candidate was usable (read, and
    of this schema_version) and at least one did not parse, this raises rather
    than returning `{}`: callers would regenerate every unbuilt slug's fields
    as nulls, destroying what the unreadable snapshot recorded.
    """
    import json

    usable = unreadable = 0
    for path in candidates:
        if path is None or not path.exists():
            continue
        try:
            document = json.loads(path.read_text())
            slugs = document.get("slugs", {})
        except (OSError, ValueError, AttributeError) as exc:
            print(f"[docs] WARNING: unreadable snapshot {path}: {exc}", file=sys.stderr)
            unreadable += 1
            continue
        version = document.get("schema_version")
        if schema_version is not None and version is not None and version != schema_version:
            print(f"[docs] WARNING: ignoring {path}: schema_version {version}, "
                  f"the manifest is {schema_version}", file=sys.stderr)
            continue
        usable += 1
        if slugs:
            return slugs
    if unreadable and not usable:
        raise RuntimeError(
            "no readable snapshot to preserve unbuilt slugs from (see the warnings "
            "above); fix or remove the unreadable file rather than regenerating nulls")
    return {}


def _load_snapshot_slugs(schema_version: int | None = None, *, snapshot_path: Path | None = None) -> dict[str, dict]:
    """Return the `slugs` mapping from the on-disk snapshot, or `{}` when none
    exists; raises RuntimeError (see `_preserved_slugs`) when a checkout's
    candidates exist but none is usable and one does not parse.

    Used by `generate_datasets_md` to fall back to the last-known row count
    / sizes when a slug's parquet isn't present locally. Tries:

        1. `docs/snapshot.json`                 (gitignored scratch — wins
           if a maintainer regenerated locally)
        2. `docs/v{schema_version}/snapshot.json`  (tracked canonical — what
           a fresh clone has)

    through `_preserved_slugs`: a candidate of another schema_version is
    skipped, and one that does not parse is reported, never silently dropped.
    """
    import json

    snapshot_path = snapshot_path if snapshot_path is not None else SNAPSHOT_JSON

    from raincloud.catalogs import current
    context = current()
    if context is not None and context.source != "checkout":
        if snapshot_path.is_file():
            try:
                observed = json.loads(snapshot_path.read_text())
            except (OSError, ValueError) as exc:
                print(f"[docs] WARNING: unreadable snapshot {snapshot_path}: {exc}; "
                      "using the catalog's", file=sys.stderr)
                observed = {}
            if observed.get("catalog_revision") == context.bundle.revision:
                return observed.get("slugs", {})
        return context.snapshot.get("slugs", {})
    candidates = [snapshot_path]
    if schema_version is not None:
        candidates.append(REPO_ROOT / "docs" / f"v{schema_version}" / "snapshot.json")
    return _preserved_slugs(candidates, schema_version)


def _size_label(bytes_: int | None) -> str:
    if bytes_ is None:
        return "—"
    mb = bytes_ / (1024 * 1024)
    return f"{mb:,.1f} MB"


# ---------- datasets.md ----------

_TIER_TITLES = {
    "encoding": "Encoding",
    "stress":   "Stress",
}


def _render_curated_picks(manifest: dict) -> str:
    """Render a curated-picks Markdown block keyed by SHOWCASE_TIERS.

    Member slugs come from sources.json `showcase` arrays; up to 8 per tier
    (deterministic by manifest order). Empty tiers get a one-line
    placeholder rather than disappearing.
    """
    lines: list[str] = ["## Curated picks", ""]
    for tier in SHOWCASE_TIERS:
        members = [s for s in manifest.get("datasets", [])
                   if tier in (s.get("showcase") or [])]
        title = _TIER_TITLES.get(tier, tier)
        lines.append(f"### {title}")
        if not members:
            lines.append("_No picks yet — curation pass pending._")
            lines.append("")
            continue
        for spec in members[:8]:
            desc = (spec.get("description") or "").splitlines()[0][:140]
            # Plain code span, NOT a link: the dataset table below is rows, not
            # headings, so `#<slug>` anchors do not exist and every such link dangles.
            lines.append(f"- **`{spec['slug']}`** — {desc}")
        lines.append("")
    lines.append("---")
    lines.append("")
    return "\n".join(lines)


def generate_datasets_md(*, destination: Path | None = None, snapshot_path: Path | None = None):
    destination = destination if destination is not None else DATASETS_MD
    manifest = load_manifest()
    snapshot_slugs = _load_snapshot_slugs(manifest.get("schema_version"), snapshot_path=snapshot_path)
    from raincloud import _builds

    from .records import measured_unavailable
    from .spec import outputs_base
    builds = _builds.read(outputs_base())
    rows = []
    advisories: list[tuple[str, str, str]] = []  # (slug, short_name, advisory text)
    for spec in manifest["datasets"]:
        slug = spec["slug"]
        parquet = prepared_parquet(slug)
        snap = snapshot_slugs.get(slug, {})
        meta = None
        if parquet.exists():
            try:
                pf = pq.ParquetFile(parquet)
                meta = pf.metadata
                schema = pf.schema_arrow
            except Exception:
                meta = None
        if meta is not None:
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
        # A format a build measured unavailable has no file to size; any on
        # disk is from an earlier build. `raincloud describe` quotes the reason.
        if measured_unavailable(spec, "vortex", snap, manifest, builds=builds):
            vortex_size = "unavailable"
        if measured_unavailable(spec, "parquet", snap, manifest, builds=builds):
            parquet_size = row_groups = "unavailable"

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
    out = [_generation_header("datasets"), "", _render_curated_picks(manifest), header, sep]
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

    destination.write_text("\n".join(out) + "\n")
    note = f", {len(advisories)} scrape-flagged" if advisories else ""
    print(f"wrote {destination}  ({len(rows)} data rows{note})")


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

    Suppresses core deps (pyarrow / numpy / duckdb) and stdlib.
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
    return sorted(found)


def generate_handlers_md(*, destination: Path | None = None):
    """One row per registered transform handler.

    Sources the registry from `raincloud.pipeline.handlers`, the
    handler's purpose from the first line of the module docstring, and
    batch input support from the handler declaration, or direct streaming
    from the `return []` contract (handlers write canonical Arrow themselves).
    Manifest spec
    counts come from `transform.handler` usage in `sources.json`.
    """
    import inspect

    from . import handlers as h_mod

    destination = destination if destination is not None else HANDLERS_MD
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

    for name in sorted(h_mod.names()):
        # `get` imports the handler's module; docs generation runs in a [build]
        # environment, so resolving every one here is fine.
        fn = h_mod.get(name)
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
        readers = getattr(fn, "batch_readers", ())
        streaming = ("batches (" + ", ".join(sorted(readers)) + ")" if readers else
                     "yes" if _STREAMING_RETURN_RE.search(fn_src) else "no")
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

    destination.write_text("\n".join(lines) + "\n")
    n_handlers = len(h_mod.names())
    n_used = sum(1 for n in h_mod.names() if n in usage)
    print(f"wrote {destination}  ({n_handlers} handlers, {n_used} used by ≥1 manifest spec)")


# ---------- snapshot.json ----------

def _is_nested_arrow_type(arrow_type) -> bool:
    import pyarrow as pa
    return (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_struct(arrow_type)
        or pa.types.is_map(arrow_type)
    )


_HIGH_CARDINALITY_RATIO = 0.5   # NDV / row_count threshold to flag a string column


def _high_cardinality_from_profile(profile_path: "Path") -> bool | None:
    """True if any string-typed column has ndv_approx / row_count >= ratio.

    Returns False when no string column meets the bar, None when no profile
    exists or the profile is malformed. Reads profile.json only — does not
    open the parquet.
    """
    import json as _json
    if not profile_path.exists():
        return None
    try:
        profile = _json.loads(profile_path.read_text())
    except Exception:
        return None
    rows = profile.get("row_count") or 0
    if rows <= 0:
        return None
    for col in (profile.get("columns") or {}).values():
        if not col:
            continue
        if col.get("dtype") not in {"string", "binary", "large_string", "large_binary"}:
            continue
        ndv = col.get("ndv_approx") or 0
        if ndv / rows >= _HIGH_CARDINALITY_RATIO:
            return True
    return False


def _shape_traits_from_schema(schema) -> dict[str, bool | None]:
    """Compute the schema-derivable subset of TRAIT_FLAGS.

    `high_cardinality_present` is left null here — Task 10 populates it
    from profile.json when available.
    """
    import pyarrow as pa
    n_cols = len(schema)
    n_string = 0
    has_nested = False
    has_timestamp = False
    has_variant = False
    for field in schema:
        t = field.type
        if pa.types.is_string(t) or pa.types.is_large_string(t):
            n_string += 1
        if _is_nested_arrow_type(t):
            has_nested = True
        if pa.types.is_timestamp(t) or pa.types.is_date(t):
            has_timestamp = True
        if _is_variant_field(field):
            has_variant = True
            # variant is a struct payload, so also nested
            has_nested = True

    return {
        "has_nested": has_nested,
        "has_timestamp": has_timestamp,
        "has_variant": has_variant,
        "string_heavy": (n_cols > 0) and (n_string / n_cols > 0.5),
        "wide_row": n_cols > 50,
        "high_cardinality_present": None,
    }


def _snapshot_for_slug(*, slug: str, parquet_path: Path, prior_snapshot: dict | None) -> dict:
    """Build the discovery-specific subset of a per-slug snapshot record.

    Owned fields:
      - `size_bucket`   — from parquet bytes via discovery.bucket_for_size.
      - `shape_traits`  — schema-derived; Task 10 backfills high_cardinality_present.

    Tasks 10 will further refine `high_cardinality_present` from profile.json.

    Falls back to prior snapshot values per the "load-bearing snapshot"
    invariant: partial regens must not dash-out tracked ground truth.

    The `slug` param is currently informational only.
    """
    _OWNED_KEYS = ("size_bucket", "shape_traits")
    record: dict = {}

    if parquet_path.exists():
        record["size_bucket"] = bucket_for_size(parquet_path.stat().st_size)
        # Read schema metadata only — no full file scan. Fall back to prior
        # (or null) on a malformed/empty parquet so this stays robust.
        try:
            schema = pq.read_schema(parquet_path)
            record["shape_traits"] = _shape_traits_from_schema(schema)
            # If a per-slug profile exists, backfill high_cardinality_present.
            profile_path = parquet_path.parent.parent / "profile.json"
            record["shape_traits"]["high_cardinality_present"] = (
                _high_cardinality_from_profile(profile_path)
            )
        except Exception:
            if prior_snapshot is not None and "shape_traits" in prior_snapshot:
                record["shape_traits"] = prior_snapshot["shape_traits"]
            else:
                record["shape_traits"] = None
    elif prior_snapshot is not None:
        for k in _OWNED_KEYS:
            if k in prior_snapshot:
                record[k] = prior_snapshot[k]
    else:
        record["size_bucket"] = None
        record["shape_traits"] = None

    return record


# `created_by` prefixes of the Parquet writers raincloud runs.
_PARQUET_CREATORS = (("parquet-rs", "rs"), ("parquet-cpp-arrow", "py"), ("parquet-mr", "java"))


def _writer_of(fmt: str, path: Path, known: str | None) -> str | None:
    """Which writer made `path`: what a build recorded while the file is
    unchanged, else what the file itself says, else unknown."""
    if fmt == "arrow":
        return "canonical"
    if known is not None:
        return known
    if fmt == "parquet":
        try:
            created_by = pq.ParquetFile(path).metadata.created_by or ""
        except Exception:  # noqa: BLE001 — provenance is informational
            return None
        return next((writer for prefix, writer in _PARQUET_CREATORS if created_by.startswith(prefix)), None)
    return None


def generate_snapshot(*, overwrite_missing: bool = False, rehash: bool = False,
                      destination: Path | None = None):
    """Per-slug record of canonical-build state for the TUI / agents.

    Walks the manifest; for each slug, captures:
      - parquet schema (top-level columns: name + type) when built
      - file sizes (parquet + vortex bytes) when present
      - row count from spec.expect.rows

    Default semantics: "update if present". If a slug has fresh build data
    on disk, metadata for those formats is refreshed. Absent formats retain
    their prior size/hash pairs; a wholly absent slug retains its prior entry (so iterative workflows that
    delete `outputs/v1/<slug>/` between builds don't clobber the snapshot
    each time docs is regenerated). expected_rows is always re-read from
    the manifest since that's the authoritative source.

    Pass overwrite_missing=True to emit null entries for slugs without
    fresh build data, regardless of any prior snapshot — use this for a
    full from-scratch regeneration.

    Pass rehash=True to recompute every present file's sha256 instead of
    reusing the prior sha on a size match. Use this (the `--rehash` flag) when
    a rebuild changed an artifact's content without changing its byte length —
    the only case the size-based reuse misses, which otherwise wedges
    `publish`'s checksum gate. Unlike overwrite_missing, it preserves prior
    data for slugs not built this run, so it's safe on a partial checkout.
    """
    import json
    destination = destination if destination is not None else SNAPSHOT_JSON
    manifest = load_manifest()
    existing_slugs: dict = {}
    from raincloud.catalogs import current
    context = current()
    if context is not None and context.source != "checkout" and not overwrite_missing:
        # A non-checkout catalog preserves from its own observation or bundle,
        # never from a checkout's docs.
        existing_slugs = _load_snapshot_slugs(manifest["schema_version"], snapshot_path=destination)
    elif not overwrite_missing:
        # Preserve from the scratch copy if present, else from the tracked
        # `docs/v{n}/snapshot.json`. The tracked fallback is required: on a tree
        # without the scratch copy (a fresh clone, or a cleaned tree) there would
        # be nothing to preserve from, and every slug not built locally would
        # lose its `last_built_rows` / `parquet_bytes` / `vortex_bytes` /
        # `columns` -- ground truth the committed snapshot is the only record of.
        existing_slugs = _preserved_slugs([destination, _tracked_snapshot_json(manifest)],
                                          manifest.get("schema_version"))
    out: dict = {
        **({"catalog_id": context.bundle.catalog_id, "catalog_revision": context.bundle.revision} if context else {}),
        "schema_version": manifest["schema_version"],
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "note": (
            "Auto-generated by raincloud/pipeline/docs.py. Read by the TUI as a "
            "fallback when a local parquet isn't built. Regenerate after any "
            "build / schema change with `python -m raincloud.pipeline.docs snapshot`."
        ),
        "slugs": {},
    }
    n_with_schema = 0
    n_preserved = 0
    from raincloud import _builds
    from raincloud._bundle import recipe_hash
    from raincloud._resolve import artifact_key

    from .spec import outputs_base
    builds = _builds.read(outputs_base())
    specs = {spec["slug"]: spec for spec in manifest["datasets"]}
    for spec in manifest["datasets"]:
        slug = spec["slug"]
        recipe = recipe_hash(spec, manifest["schema_version"], specs=specs)
        expected_rows = spec_field(spec, "expect.rows")
        prior_for_slug = existing_slugs.get(slug)
        prior = prior_for_slug or {}
        paths = {fmt: prepared_artifact(slug, fmt) for fmt in ALL_FORMATS}
        sizes = {fmt: path.stat().st_size if path.exists() else None for fmt, path in paths.items()}
        parquet, arrow = paths["parquet"], paths["arrow"]
        # Preserve each absent format independently. A local Parquet file does
        # not prove that an Arrow/Vortex artifact recorded elsewhere disappeared.
        fresh: dict = {
            "last_built_rows": None,
            "last_built_row_groups": None,
            "parquet_bytes": None,
            "vortex_bytes": None,
            "parquet_sha256": None,
            "vortex_sha256": None,
            "columns": None,
            **prior,
            "expected_rows": expected_rows,
        }
        n_unavailable = 0
        for fmt, path in paths.items():
            size = sizes[fmt]
            built = builds.get(artifact_key(slug, fmt, manifest["schema_version"])) or {}
            if isinstance(built.get("unavailable"), dict) and built.get("recipe") == recipe:
                # This install's build measured that its writer cannot make the
                # file at this recipe: the catalog records that, and no file (a
                # file still on disk is from an earlier build).
                fresh.update({f"{fmt}_bytes": None, f"{fmt}_sha256": None, f"{fmt}_writer": None,
                              f"{fmt}_unavailable": built["unavailable"]})
                fresh.pop(f"{fmt}_verified", None)
                fresh.pop(f"{fmt}_verify_note", None)
                n_unavailable += 1
                continue
            if size is not None:
                # A file this install built is described by its build record;
                # regenerating is how a maintainer turns builds into catalog.
                fresh.pop(f"{fmt}_unavailable", None)
                if not rehash and built.get("bytes") == size and built.get("sha256"):
                    sha, writer = built["sha256"], built.get("writer")
                    verified, why = built.get("verified"), built.get("verify_note")
                else:
                    sha = _sha256_or_reuse(
                        path, size, prior.get(f"{fmt}_bytes"), prior.get(f"{fmt}_sha256"),
                        force=rehash,
                    )
                    same = sha == prior.get(f"{fmt}_sha256")
                    writer = _writer_of(fmt, path, prior.get(f"{fmt}_writer") if same else None)
                    verified, why = ((prior.get(f"{fmt}_verified"), prior.get(f"{fmt}_verify_note"))
                                     if same else (None, None))
                fresh[f"{fmt}_bytes"] = size
                fresh[f"{fmt}_sha256"] = sha
                fresh[f"{fmt}_writer"] = writer
                # Whether the writer read the file back: recorded when the
                # build record says (a sidecar may promote a file it could not
                # verify, with its reason); absent when nothing does.
                for key, value in ((f"{fmt}_verified", verified),
                                   (f"{fmt}_verify_note", why if verified is False else None)):
                    if value is None:
                        fresh.pop(key, None)
                    else:
                        fresh[key] = value
                if verified is False:
                    print(f"[docs] {slug}/{fmt}: {writer} did not verify that its file reads back: {why}",
                          file=sys.stderr)
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
                fresh.pop("columns_error", None)
                n_with_schema += 1
            except Exception as e:
                fresh["columns_error"] = f"{type(e).__name__}: {str(e)[:120]}"
        elif arrow.exists():
            try:
                import pyarrow.dataset as ds
                with pa.ipc.open_file(str(arrow)) as reader:
                    fresh["columns"] = [{"name": f.name, "type": str(f.type)} for f in reader.schema]
                # Counted from record-batch metadata; no batch is decompressed.
                fresh["last_built_rows"] = ds.dataset(str(arrow), format="ipc").count_rows()
                fresh["last_built_row_groups"] = None  # a Parquet property; there is none
                fresh.pop("columns_error", None)
                n_with_schema += 1
            except Exception as e:
                fresh["columns_error"] = f"{type(e).__name__}: {str(e)[:120]}"
        # Discovery-axis fields: size_bucket + shape_traits. Adds to `fresh`
        # without disturbing existing fields. When the parquet is missing this
        # run, the helper preserves the prior snapshot's values so partial
        # regens don't dash out tracked ground truth. The merge tuple below
        # mirrors `_OWNED_KEYS` inside the helper.
        snap_fragment = _snapshot_for_slug(
            slug=slug,
            parquet_path=parquet,
            prior_snapshot=prior_for_slug,
        )
        for k in ("size_bucket", "shape_traits"):
            if k in snap_fragment:
                fresh[k] = snap_fragment[k]
        fresh_has_data = n_unavailable or any(size is not None for size in sizes.values())
        if fresh_has_data or overwrite_missing or slug not in existing_slugs:
            out["slugs"][slug] = _without_stale_measurements(fresh, recipe)
        else:
            preserved = dict(existing_slugs[slug])
            preserved["expected_rows"] = expected_rows
            out["slugs"][slug] = _without_stale_measurements(preserved, recipe)
            n_preserved += 1
    # A non-finite stat fails here, at regen time, not in a consumer's JSON parser.
    destination.write_text(json.dumps(out, indent=2, allow_nan=False) + "\n")
    unavailable = sorted(f"{slug}/{key[:-len('_unavailable')]}" for slug, entry in out["slugs"].items()
                         for key, value in entry.items() if key.endswith("_unavailable") and value)
    print(f"wrote {destination}  "
          f"({len(out['slugs'])} slugs, {n_with_schema} schemas captured this run, "
          f"{n_preserved} preserved from prior snapshot"
          f"{f', {len(unavailable)} format(s) measured unavailable' if unavailable else ''})")
    _warn_stale_opt_outs(out["slugs"], manifest)


def _without_stale_measurements(entry: dict, recipe: str) -> dict:
    """`entry` without "unavailable" measurements taken at another recipe: the
    recipe changed since, so they no longer say anything about the dataset."""
    return {key: value for key, value in entry.items()
            if not (key.endswith("_unavailable") and isinstance(value, dict) and value.get("recipe") != recipe)}


def _warn_stale_opt_outs(slugs: dict, manifest: dict) -> None:
    """Warn where the compliance ledger contradicts a format the snapshot records
    as unavailable: a writer of that format round-tripped the dataset there.

    Either the measurement is stale (a newer toolchain can write the format:
    re-export and regenerate) or the ledger predates it (a regression). Both
    name the dataset; neither changes what is written.
    """
    import json

    from raincloud._formats import describe_unavailable

    from .spec import default_compliance_json
    path = default_compliance_json(manifest)
    if not path.is_file():
        return
    try:
        document = json.loads(path.read_text())
        blocks = document.get("slugs") or {}
    except (OSError, ValueError, AttributeError) as exc:
        print(f"[docs] WARNING: could not read the compliance ledger {path}: {exc}", file=sys.stderr)
        return
    measured = document.get("generated_at") or ""
    for slug, entry in sorted(slugs.items()):
        block = blocks.get(slug)
        if not isinstance(block, dict):
            continue
        for key, recorded in sorted(entry.items()):
            if not (key.endswith("_unavailable") and isinstance(recorded, dict)):
                continue
            fmt = key[:-len("_unavailable")]
            cells = sorted(w.get("cell") for w in block.get("write") or []
                           if isinstance(w, dict) and str(w.get("cell", "")).partition("@")[0] == fmt
                           and w.get("roundtrip") is True)
            if not cells:
                continue
            if measured >= str(recorded.get("measured_at") or ""):
                print(f"[docs] WARNING: [stale opt-out] {slug}/{fmt}: {', '.join(cells)} round-trips in "
                      f"{path} (measured {measured}), but the snapshot records it unavailable "
                      f"({describe_unavailable(recorded)}); re-export it with "
                      f"`python -m raincloud.pipeline.export {slug} --format {fmt}` and regenerate",
                      file=sys.stderr)
            else:
                print(f"[docs] WARNING: {slug}/{fmt}: recorded unavailable "
                      f"({describe_unavailable(recorded)}), but {', '.join(cells)} round-tripped in the "
                      f"earlier ledger {path} (measured {measured}): a regression?", file=sys.stderr)


# ---------- CLI ----------

TARGETS = ("datasets", "handlers", "snapshot")


def _parse_args(argv):
    """Parse the docs CLI.

    Uses argparse deliberately: this command REGENERATES derived artifacts, and the
    previous hand-rolled parsing treated any unrecognized token as "no targets given"
    and fell through to regenerating all three. A typo -- `--dry-run`, or even
    `--help` -- silently rewrote datasets.md, handlers.md and snapshot.json.
    """
    import argparse
    parser = argparse.ArgumentParser(
        prog="python -m raincloud.pipeline.docs",
        # No abbreviations: this command regenerates derived artifacts, so `--reh`
        # silently meaning `--rehash` is the same class of surprise as a typo running.
        allow_abbrev=False,
        description="Regenerate derived docs. With no targets, regenerates all three.")
    parser.add_argument("targets", nargs="*", metavar="TARGET", default=[],
                        help=f"which artifacts to regenerate: {', '.join(TARGETS)} (default: all)")
    parser.add_argument("--overwrite-missing", action="store_true",
                        help="replace snapshot entries whose artifacts are missing on disk")
    parser.add_argument("--rehash", action="store_true",
                        help="recompute artifact checksums rather than preserving recorded ones")
    args = parser.parse_args(list(argv))
    unknown = [t for t in args.targets if t not in TARGETS]
    if unknown:
        parser.error(f"unknown target(s) {unknown}; choose from {', '.join(TARGETS)}")
    args.targets = list(args.targets) or list(TARGETS)
    return args


def _main(args, *, directory: Path):
    overwrite_missing, rehash, targets = args.overwrite_missing, args.rehash, args.targets
    if "datasets" in targets:
        generate_datasets_md(destination=directory / "datasets.md",
                             snapshot_path=directory / "snapshot.json")
    if "handlers" in targets:
        generate_handlers_md(destination=directory / "handlers.md")
    if "snapshot" in targets:
        generate_snapshot(overwrite_missing=overwrite_missing, rehash=rehash,
                          destination=directory / "snapshot.json")
    return 0


def main(argv):
    from .lifecycle import operation_lock
    from .spec import observations_dir

    # Parse BEFORE the store lock and before touching the filesystem. `--help` and a
    # mistyped target must do NOTHING: acquiring an exclusive lock on the data root
    # and stat-ing every (slug x format x writer) path is real work, and doing it
    # ahead of validation is what made a typo consequential in the first place.
    args = _parse_args(argv)

    # The store lock coordinates artifacts, not other stores in this process.
    # Carry destinations explicitly so overlapping operations cannot redirect
    # either writes or snapshot-preservation reads into another catalog.
    with operation_lock() as context:
        directory = observations_dir(context.manifest, repo_root=REPO_ROOT, scratch=True)
        directory.mkdir(parents=True, exist_ok=True)
        return _main(args, directory=directory)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
