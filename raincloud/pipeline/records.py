# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""What the pipeline stages record in this install's build record, and what
they read back from it before exporting from a canonical already on disk.

`build`, `export` and `convert` all record through `record_build` (a file) and
`record_unavailable` (a planned writer's failure), read a failure already
measured back through `recorded_failure` (so it is skipped rather than
repeated), and the two stages that
export an existing canonical share `export_from_canonical`, so the rules for
which canonical may be exported, and what is then recorded, are written once. The record itself (`<data_dir>/builds.json`) is
`raincloud._builds`; the catalog is never touched.
"""
from __future__ import annotations

from pathlib import Path

from .export import run_exporters, slug_from_canonical
from .spec import display_path, outputs_base

# The export stage's keys: changing them re-exports, but leaves the canonical.
_EXPORT_STAGE = ("write", "convert", "export")


def canonical_recipe(spec: dict, version: int, specs) -> str:
    """The recipe without its export stage: what determines the canonical's bytes.

    A derived dataset folds in its parent's canonical recipe, not its full one:
    re-exporting the parent does not change the derived canonical. `expect`
    stays in: only a build validates a canonical against it, so an edited
    expectation needs one.
    """
    from raincloud._bundle import recipe_hash

    def strip(s: dict) -> dict:
        return {k: v for k, v in s.items() if k not in _EXPORT_STAGE}
    return recipe_hash(strip(spec), version, specs={slug: strip(s) for slug, s in (specs or {}).items()})


def record_build(produced: dict[str, dict[str, tuple[str, int]]], *,
                 unverified: dict[str, dict[str, str]] | None = None) -> None:
    """Record what a build produced in this install's build record.

    `produced` maps slug -> {cell: (sha256, bytes)}; a cell (`parquet@rs`)
    names its format's file and the writer that made it. Every export records
    whether its writer read the file back and found the canonical (`verified`);
    `unverified` maps slug -> {cell: why not} for the files promoted without
    that (a sidecar reporting its read-back unmeasured), and the reason is kept
    as `verify_note`. The catalog is not
    touched: it is shared, and changes only when a maintainer regenerates and
    commits it. The loader serves these files for this install when they
    differ from the catalog's, as long as the recipe is unchanged.

    A canonical also records `canonical_recipe`, so an export-stage change can
    re-export it without a rebuild (see `canonical_status`). Every export
    records the sha256 of the canonical it was made from, and recording a new
    canonical supersedes the entries of exports made from another one: a
    writer that fails after a rebuild puts its previous file back, and that
    file must not be served beside a canonical it no longer matches.
    """
    from raincloud import _builds
    from raincloud._bundle import recipe_hash
    from raincloud._formats import WRITERS
    from raincloud._resolve import artifact_key
    from raincloud.catalogs import current

    from .spec import scrub_published_text

    context = current()
    if context is None:
        print("  [build record] not recorded: no catalog is selected for this operation")
        return
    manifest = context.manifest
    version = manifest["schema_version"]
    specs = {spec["slug"]: spec for spec in manifest["datasets"]}
    recorded = _builds.read(outputs_base())
    entries = {}
    # A producer's undeclared extra output has no recipe; it stays a local file.
    for slug, cells in produced.items():
        if slug not in specs:
            continue
        recipe = recipe_hash(specs[slug], version, specs=specs)
        if "arrow" in cells:
            canonical_sha = cells["arrow"][0]
            for fmt in WRITERS:
                key = artifact_key(slug, fmt, version)
                old = recorded.get(key)
                if (fmt != "arrow" and isinstance(old, dict) and old.get("recipe") is not None
                        and old.get("canonical_sha256") != canonical_sha):
                    entries[key] = {**old, "recipe": None,
                                    "superseded": f"made from another canonical than {canonical_sha}"}
        else:
            built = recorded.get(artifact_key(slug, "arrow", version)) or {}
            canonical_sha = built.get("sha256") or context.snapshot.get("slugs", {}).get(slug, {}).get(
                "arrow_sha256")
        for cell, (sha256, nbytes) in cells.items():
            base, _, writer = cell.partition("@")
            entry = {"sha256": sha256, "bytes": nbytes, "writer": writer or "canonical", "recipe": recipe}
            if base == "arrow":
                entry["canonical_recipe"] = canonical_recipe(specs[slug], version, specs)
            else:
                entry["canonical_sha256"] = canonical_sha
                why = (unverified or {}).get(slug, {}).get(cell)
                entry["verified"] = why is None
                if why is not None:
                    entry["verify_note"] = scrub_published_text(why)
            entries[artifact_key(slug, base, version)] = entry
    if entries:
        path = _builds.record(outputs_base(), entries)
        print(f"  [build record] {len(entries)} file(s) in {display_path(path)}")


def record_unavailable(slug: str, failure) -> bool:
    """Record that the planned writer could not make `slug`'s format here.

    `failure` is `export.Unavailable`. The build record's entry for the
    format's file becomes an "unavailable" measurement -- the writer cell, its
    error, the toolchain, the recipe and the canonical it was measured against,
    and when -- which the loader reports instead of offering a build, and which
    regenerating the catalog carries into the snapshot. A later successful
    export replaces it.

    Except when the file `run_exporters` put back is still this install's
    export of the same canonical at the current recipe: it stays the dataset's
    file, and this attempt's failure is only reported. Returns whether a
    measurement was recorded.
    """
    from raincloud import _builds
    from raincloud._bundle import recipe_hash
    from raincloud._resolve import artifact_key
    from raincloud.catalogs import current

    from .export import get_exporter
    from .spec import scrub_published_text

    context = current()
    if context is None:
        print("  [build record] not recorded: no catalog is selected for this operation")
        return False
    manifest = context.manifest
    version = manifest["schema_version"]
    specs = {spec["slug"]: spec for spec in manifest["datasets"]}
    if slug not in specs:
        return False  # an undeclared extra output has no recipe
    recipe = recipe_hash(specs[slug], version, specs=specs)
    recorded = _builds.read(outputs_base())
    canonical_sha = _canonical_sha(slug, recorded, context)
    key = artifact_key(slug, failure.format, version)
    old = recorded.get(key) or {}
    kept = get_exporter(failure.cell).out_path(slug)
    if (old.get("sha256") and old.get("recipe") == recipe and not old.get("superseded")
            and old.get("canonical_sha256") == canonical_sha
            and kept.is_file() and kept.stat().st_size == old.get("bytes")):
        print(f"  [build record] kept {display_path(kept)}: {old.get('writer')} made it from this canonical; "
              f"this attempt is not recorded")
        return False
    measurement = {
        "cell": failure.cell,
        "error": scrub_published_text(failure.error),
        "toolchain": {name: scrub_published_text(value) for name, value in failure.toolchain.items()},
        "recipe": recipe,
        "canonical_sha256": canonical_sha,
        "measured_at": failure.measured_at,
    }
    path = _builds.record(outputs_base(), {key: {
        "writer": failure.cell.partition("@")[2], "recipe": recipe, "canonical_sha256": canonical_sha,
        "unavailable": measurement}})
    print(f"  [unavailable] {slug}/{failure.format}: recorded in {display_path(path)}")
    return True


def _canonical_sha(slug: str, recorded: dict, context) -> str | None:
    """The checksum of `slug`'s canonical as a measurement records it: this
    install's build of it, else the catalog's."""
    from raincloud._resolve import artifact_key

    version = context.manifest["schema_version"]
    return ((recorded.get(artifact_key(slug, "arrow", version)) or {}).get("sha256")
            or context.snapshot.get("slugs", {}).get(slug, {}).get("arrow_sha256"))


def recorded_failure(slug: str):
    """`run_exporters`'s `measured` callback for `slug`: for a format, the
    measurement that applies at the current recipe (`measured_unavailable`:
    this install's build record, else the catalog's) and the checksum of the
    canonical being exported, taken as `record_unavailable` takes it; or None."""
    from raincloud import _builds
    from raincloud.catalogs import current

    def measured(fmt: str) -> tuple[dict, str | None] | None:
        context = current()
        if context is None:
            return None
        manifest = context.manifest
        spec = next((d for d in manifest["datasets"] if d["slug"] == slug), None)
        if spec is None:
            return None  # an undeclared extra output has no recipe
        recorded = _builds.read(outputs_base())
        measurement = measured_unavailable(spec, fmt, context.snapshot.get("slugs", {}).get(slug), manifest,
                                           builds=recorded)
        return None if measurement is None else (measurement, _canonical_sha(slug, recorded, context))
    return measured


def measured_unavailable(spec: dict, fmt: str, snapshot_entry: dict | None, manifest: dict, *,
                         builds: dict | None = None) -> dict | None:
    """The measurement saying `spec`'s `fmt` cannot be made at its current
    recipe, or None: this install's build record when it has an entry for the
    recipe, else the catalog's (`snapshot_entry`, when taken at the recipe).

    What the pipeline's own views (docs, status, browse) show, and the same
    rule the loader applies (`raincloud._resolve.measured_unavailable`).
    `builds` is the build record already read, for a caller walking the
    whole catalog.
    """
    from raincloud import _builds
    from raincloud._bundle import recipe_hash
    from raincloud._catalog import recorded_unavailable
    from raincloud._resolve import artifact_key

    version = manifest["schema_version"]
    if version < 2:
        return None
    specs = {d["slug"]: d for d in manifest["datasets"]}
    recipe = recipe_hash(spec, version, specs=specs)
    catalog = recorded_unavailable(spec, snapshot_entry or {}, fmt, version, specs)
    entry = (builds if builds is not None else _builds.read(outputs_base())).get(
        artifact_key(spec["slug"], fmt, version))
    return _builds.unavailable_at(entry, recipe, catalog)


def canonical_status(canonical: Path) -> str:
    """Whether the canonical on disk is the current recipe's, before exporting it.

    "current": this install built it from the current recipe. "export-changed":
    built here from a recipe that differs only in the export stage, so its
    exports can be redone without a rebuild (`adopt_canonical` then moves its
    record to the current recipe). "catalog": it is the catalog's file (the
    catalog's size, as the loader judges). "stale": built here from an earlier
    recipe. "unknown": neither the build record nor the catalog names it.
    """
    from raincloud._bundle import recipe_hash
    from raincloud._catalog import Catalog
    from raincloud.catalogs import current

    context = current()
    manifest = context.manifest
    version = manifest["schema_version"]
    specs = {spec["slug"]: spec for spec in manifest["datasets"]}
    slug = slug_from_canonical(canonical)
    size = canonical.stat().st_size
    built = _canonical_record(canonical, version)
    if built is not None and built.get("bytes") == size and slug in specs:
        if built.get("recipe") == recipe_hash(specs[slug], version, specs=specs):
            return "current"
        if built.get("canonical_recipe") == canonical_recipe(specs[slug], version, specs):
            return "export-changed"
        return "stale"
    catalog = Catalog(context.snapshot, manifest, context)
    arrow = catalog.entry(slug).formats.get("arrow") if slug in catalog else None
    if arrow is not None and arrow.nbytes == size:
        return "catalog"
    return "unknown"


def adopt_canonical(canonical: Path) -> None:
    """Record an "export-changed" canonical under the current recipe, so the
    loader serves it beside the exports made from it."""
    from raincloud.catalogs import current

    built = _canonical_record(canonical, current().manifest["schema_version"])
    record_build({slug_from_canonical(canonical): {"arrow": (built["sha256"], built["bytes"])}})


def _canonical_record(canonical: Path, version: int) -> dict | None:
    from raincloud import _builds
    from raincloud._resolve import artifact_key
    return _builds.lookup(outputs_base(), artifact_key(slug_from_canonical(canonical), "arrow", version))


def check_canonical(canonical: Path) -> str:
    """`canonical_status`, refusing a canonical nothing may be exported from.

    A "stale" canonical's exports would be recorded as the current recipe's;
    an "unknown" one's could not be recorded at all, so they would replace the
    store's files with ones the loader refuses. Both raise RuntimeError naming
    the rebuild; every other status is returned.
    """
    status = canonical_status(canonical)
    slug = slug_from_canonical(canonical)
    if status == "stale":
        raise RuntimeError(f"canonical {display_path(canonical)} is from an earlier recipe; "
                           f"rebuild it with `raincloud build {slug}`")
    if status == "unknown":
        raise RuntimeError(f"canonical {display_path(canonical)} is neither this install's build nor "
                           f"the catalog's file, so nothing exported from it could be recorded; "
                           f"rebuild it with `raincloud build {slug}`")
    return status


def export_from_canonical(spec: dict, canonical: Path, formats: list[str] | None = None, *,
                          status: str | None = None, on_unavailable=None, on_skip=None,
                          retry_errors: bool = False) -> list:
    """Export `spec`'s formats from a canonical already on disk and record them.

    `status` is the canonical's `check_canonical` verdict when the caller has
    already taken it; otherwise it is taken here, and raises for a stale or
    unknown canonical before anything is written. An "export-changed" canonical
    is first recorded under the current recipe. Returns `run_exporters`'s
    results; `formats`, `on_skip` and `retry_errors` are passed to it as given.
    A planned writer that fails is recorded (`record_unavailable`) and then
    passed to `on_unavailable` with whether it was recorded.
    """
    status = status or check_canonical(canonical)
    if status == "export-changed":
        adopt_canonical(canonical)
    return run_exporters(spec, canonical, formats, on_skip=on_skip, retry_errors=retry_errors,
                         **recorders(slug_from_canonical(canonical), on_unavailable))


def recorders(slug: str, on_unavailable=None, on_unverified=None) -> dict:
    """`run_exporters`'s callbacks that read and record each outcome for
    `slug`: the failure already measured for a format (`recorded_failure`), an
    accepted file (`record_build`: unverified when its writer reported the
    read-back unmeasured, with the writer's note as the reason, then
    `on_unverified(result, reason)`) and a planned
    writer's failure (`record_unavailable`, then `on_unavailable(failure,
    recorded)`; not recorded means the previous file stays the dataset's)."""
    def unavailable(failure):
        recorded = record_unavailable(slug, failure)
        if on_unavailable is not None:
            on_unavailable(failure, recorded)

    def accept(result):
        why = None
        if result.compliance.roundtrip is None:
            why = result.compliance.note or f"{result.format_id} did not say why it could not verify its file"
        record_build({slug: {result.format_id: (result.sha256, result.nbytes)}},
                     unverified={slug: {result.format_id: why}} if why is not None else None)
        if why is not None and on_unverified is not None:
            on_unverified(result, why)
    return {"on_accept": accept, "on_unavailable": unavailable, "measured": recorded_failure(slug)}
