# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Static checks for sources.json — runs in well under a second.

Two layers of validation:

  1. JSON Schema (sources.schema.json, Draft 2020-12) — shape, enums,
     required fields, regexes. Requires the optional `jsonschema` package
     (part of the [build] extra); if it's not installed, this layer is skipped
     with a hint and only the cross-checks below run.
  2. Cross-checks the schema can't express:
       - slug uniqueness
       - every transform.handler resolves in the live registry
         (raincloud/_registry.py HANDLERS)
       - every registered handler is referenced by ≥1 spec (orphans → warning)
       - derive.from names an ordinary (non-derived) dataset, and a hydrated
         dataset is named <parent>-hydrated
       - fetch.urls non-empty unless fetch.type is "custom" or "generated"
       - a generated fetch names a registered generator, valid parameters and
         one of its outputs
       - fetch.auth matches fetch.type for kaggle / huggingface
       - fetch.requires_interactive_accept only on kaggle / huggingface fetches
       - which formats a dataset exports: v2 reads export.formats and rejects
         convert.*; v1 pairs convert.vortex with convert.vortex_skip_reason. A
         v2 format a writer cannot produce is not declared here at all: the
         build measures it and records it (see `raincloud.pipeline.build`)
       - export.formats names parquet/vortex; export.priority (and the
         catalog's export_priority) names writers with an export cell

It prints which manifest it validated: the selected catalog's by default
(RAINCLOUD_MANIFEST, the checkout's sources.json, or the copy installed with
raincloud), or the one named on the command line.

Exit codes:
  0  manifest is valid (warnings allowed)
  1  one or more errors

Usage:
  python -m raincloud.pipeline.validate_manifest
  python -m raincloud.pipeline.validate_manifest path/to/sources.json
  python -m raincloud.pipeline.validate_manifest --json
  python -m raincloud.pipeline.validate_manifest --strict   # warnings → errors
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

from raincloud._formats import EXPORTED_FORMATS, WRITERS, export_formats, priority_shape_error
from raincloud.exceptions import CatalogError

from .discovery import SHOWCASE_TIERS, TAG_VOCAB
from .spec import REPO_ROOT, _packaged_data, select_manifest

# Writers a manifest may name, per exported format. Stricter than run time,
# which skips an unknown name: in a manifest it is a typo that would fall
# through to the next writer unnoticed. `canonical` writes only the Arrow
# spine, so it is no export writer.
EXPORT_WRITERS = {fmt: WRITERS[fmt] for fmt in EXPORTED_FORMATS}


def _schema_path():
    repo = REPO_ROOT / "sources.schema.json"
    if repo.exists():
        return repo
    packaged = _packaged_data("sources.schema.json")
    return packaged if packaged is not None else repo


def _schema_errors(manifest: dict) -> tuple[list[str], str | None]:
    """Run JSON Schema validation if jsonschema is importable.

    Returns (errors, skip_reason). When jsonschema is missing, errors is []
    and skip_reason explains why.
    """
    try:
        import jsonschema
    except ImportError:
        return [], ("jsonschema not installed; install the [build] extra "
                    "(uv sync --extra build --inexact) for full schema checks")
    schema_path = _schema_path()
    if not schema_path.exists():
        return [f"sources.schema.json missing at {schema_path}"], None
    schema = json.loads(schema_path.read_text())
    v = jsonschema.Draft202012Validator(schema)
    errs = []
    for e in v.iter_errors(manifest):
        path = ".".join(str(p) for p in e.absolute_path) or "<root>"
        errs.append(f"{path}: {e.message}")
    return errs, None


def _registry_handlers() -> set[str]:
    from .handlers import names
    return set(names())


def _check_discovery_vocab(manifest: dict) -> tuple[list[str], list[str]]:
    """Per-spec validation of `tags` + `showcase`, plus an empty-tier warning.

    Schema-level enums already cover unknown values, but this cross-check pass
    produces clearer error formatting AND adds the empty-tier warning, which
    can't be expressed in JSON schema.

    Returns (errors, warnings).
    """
    errors: list[str] = []
    warnings: list[str] = []
    tier_members: dict[str, list[str]] = {t: [] for t in SHOWCASE_TIERS}

    for spec in manifest.get("datasets", []):
        slug = spec.get("slug", "<unknown>")

        for tag in spec.get("tags") or []:
            if tag not in TAG_VOCAB:
                errors.append(
                    f"{slug}: tags entry {tag!r} is not in TAG_VOCAB "
                    f"(see raincloud/pipeline/discovery.py)"
                )

        for tier in spec.get("showcase") or []:
            if tier not in SHOWCASE_TIERS:
                errors.append(
                    f"{slug}: showcase entry {tier!r} is not in SHOWCASE_TIERS"
                )
            else:
                tier_members[tier].append(slug)

    # Only flag empty tiers once *any* tier has at least one member.
    # All-empty = "not curated yet" (scaffolding state); no warnings then.
    any_populated = any(members for members in tier_members.values())
    if any_populated:
        for tier, members in tier_members.items():
            if not members:
                warnings.append(
                    f"showcase tier {tier!r} has zero members across the manifest"
                )

    return errors, warnings


# The blocks `_bundle.validate_documents` requires to be objects, so a manifest
# this passes without jsonschema is not one the loader then rejects.
_OBJECT_BLOCKS = ("license", "fetch", "extract", "parse", "transform", "write", "expect", "convert",
                  "export", "hydrate", "derive")


def _field_errors(d: dict) -> list[str]:
    """Shapes the cross-checks below read, beyond whole blocks: each bad field
    is reported, and the spec is then left out of the checks that read it."""
    slug = d.get("slug")
    label = slug if isinstance(slug, str) else "?"
    errors = [f"{label}: {key} must be an object" for key in _OBJECT_BLOCKS
              if key in d and not isinstance(d[key], dict)]
    if not isinstance(slug, str):
        errors.append(f"{label}: slug must be a string (got {slug!r})")
    for block, key in (("transform", "handler"), ("derive", "from")):
        value = (d.get(block) or {}).get(key) if isinstance(d.get(block), dict) else None
        if value is not None and not isinstance(value, str):
            errors.append(f"{label}: {block}.{key} must be a string (got {value!r})")
    for key in ("tags", "showcase"):
        value = d.get(key)
        if value is not None and not (isinstance(value, list) and all(isinstance(v, str) for v in value)):
            errors.append(f"{label}: {key} must be a list of strings (got {value!r})")
    export = d.get("export")
    if isinstance(export, dict) and "formats" in export and not isinstance(export["formats"], list):
        errors.append(f"{label}: export.formats must be a list of formats (parquet, vortex)")
    return errors


def _priority_errors(value, where: str) -> list[str]:
    """A writer order: a list (every format) or a {format: [writers]} map.

    The shape is `_formats.priority_shape_error`, the one rule the loader
    applies too; this adds only what a manifest being authored must get right
    and run time forgives: every name is a writer of its format.
    """
    shape = priority_shape_error(value, where)
    if shape is not None:
        return [shape]
    if value is None:
        return []
    orders = value.items() if isinstance(value, dict) else [(None, value)]
    errors = []
    for fmt, order in orders:
        known = EXPORT_WRITERS[fmt] if fmt else {w for writers in EXPORT_WRITERS.values() for w in writers}
        at = f"{where}.{fmt}" if fmt else where
        errors += [f"{at} entry {w!r} is not a writer ({', '.join(sorted(known))})" for w in order if w not in known]
    return errors


def _uncovered_formats(order, formats, where: str) -> list[str]:
    """A LIST priority serves every format, so it must name a writer of each
    format it serves; one that names none fails every build of that format."""
    if not isinstance(order, list) or priority_shape_error(order, where):
        return []
    return [f"{where} {order!r} names no {fmt} writer ({', '.join(sorted(EXPORT_WRITERS[fmt]))}); "
            f"add one, or use a {{format: [writers]}} map"
            for fmt in formats if fmt in EXPORT_WRITERS and not set(order) & set(EXPORT_WRITERS[fmt])]


def _declaration_errors(d: dict, slug: str, version) -> list[str]:
    """Fields whose meaning depends on schema_version. Which formats a dataset
    exports is declared once: export.formats in v2 (the formats it WANTS; one a
    writer cannot produce is measured by the build, never explained here),
    convert.vortex in v1, where leaving Vortex out needs a reason. v2 also drops
    write.output / write.page_index, which no writer read. With no usable
    schema_version (`version` None) nothing here applies: the schema layer
    reports the version itself, and guessing one would pile on bogus errors."""
    errors = []
    if version is None:
        return errors
    convert = d.get("convert") or {}
    if version is not None and version >= 2:
        if "convert" in d:
            errors.append(
                f"{slug}: convert.* is schema_version 1 only; in v2, export.formats says which "
                f"formats a dataset has (e.g. [\"parquet\"])"
            )
        dead = [key for key in ("output", "page_index") if key in (d.get("write") or {})]
        if dead:
            errors.append(f"{slug}: write.{' and write.'.join(dead)} are schema_version 1 only; no writer "
                          f"reads them (every file is <format>/<slug>.<ext>)")
        return errors
    # v1 catalogs only; remove when v1 bundles are no longer read.
    vortex_on = convert.get("vortex", False)
    skip_reason = convert.get("vortex_skip_reason")
    if vortex_on and skip_reason is not None:
        errors.append(
            f"{slug}: convert.vortex=true but convert.vortex_skip_reason "
            f"is set — clear the reason or flip vortex to false"
        )
    if not vortex_on and not skip_reason:
        errors.append(
            f"{slug}: convert.vortex=false requires a non-null "
            f"convert.vortex_skip_reason explaining the opt-out"
        )
    if "export" in d:
        errors.append(f"{slug}: export.* is schema_version 2 only; v1 declares Vortex with convert.vortex")
    return errors


def _cross_checks(manifest: dict) -> tuple[list[str], list[str]]:
    """Returns (errors, warnings).

    Runs whether or not the schema check passed, so it must not assume shapes
    the schema enforces: a malformed block becomes an error entry, never a
    traceback that hides the schema report.
    """
    errors: list[str] = []
    warnings: list[str] = []

    if not isinstance(manifest.get("datasets"), list):
        return ["datasets must be a list of dataset specs"], []
    # A spec with a malformed block is reported once, here, and left out of
    # the checks below, which read those blocks as objects.
    datasets = []
    for i, d in enumerate(manifest["datasets"]):
        if not isinstance(d, dict):
            errors.append(f"datasets[{i}]: must be an object")
            continue
        bad = _field_errors(d)
        errors += bad
        if not bad:
            datasets.append(d)
    version = manifest.get("schema_version")
    version = version if type(version) is int else None

    # Slug uniqueness.
    slugs = [d.get("slug") for d in datasets]
    dups = [s for s, c in Counter(slugs).items() if c > 1]
    for s in dups:
        errors.append(f"duplicate slug: {s!r}")

    # Handler resolution + orphan detection.
    registry = _registry_handlers()
    used_by: dict[str, list[str]] = defaultdict(list)
    for d in datasets:
        if d.get("derive"):
            continue  # built from another dataset; no handler of its own
        h = (d.get("transform") or {}).get("handler")
        slug = d.get("slug", "?")
        if h is None:
            errors.append(f"{slug}: transform.handler is missing")
            continue
        if h not in registry:
            errors.append(
                f"{slug}: transform.handler={h!r} is not in the registry "
                f"(raincloud/_registry.py HANDLERS)"
            )
        used_by[h].append(slug)
    orphans = sorted(registry - set(used_by))
    for h in orphans:
        warnings.append(f"handler {h!r} is registered but referenced by 0 specs")

    # Derived datasets: the parent must exist and be an ordinary dataset, and
    # the name says what it is.
    by_slug = {d.get("slug"): d for d in datasets}
    for d in datasets:
        derive = d.get("derive")
        if not derive:
            continue
        slug, parent = d.get("slug", "?"), derive.get("from")
        if parent not in by_slug:
            errors.append(f"{slug}: derive.from={parent!r} is not a dataset in the manifest")
        elif by_slug[parent].get("derive"):
            errors.append(f"{slug}: derive.from={parent!r} is itself derived; derive from an upstream dataset")
        if derive.get("hydrate") and slug != f"{parent}-hydrated":
            errors.append(f"{slug}: a hydrated dataset must be named {parent}-hydrated")

    errors += _priority_errors(manifest.get("export_priority"), "export_priority")
    catalog_served: set[str] = set()

    # Per-spec sanity checks the schema can't express.
    for d in datasets:
        slug = d.get("slug", "?")
        errors += _declaration_errors(d, slug, version)
        spec_priority = (d.get("export") or {}).get("priority")
        errors += _priority_errors(spec_priority, f"{slug}: export.priority")
        exported = export_formats(d, version) if version is not None else []
        errors += _uncovered_formats(spec_priority, exported, f"{slug}: export.priority")
        # The formats the catalog's own priority serves: those no spec priority names.
        for fmt in exported:
            if not (isinstance(spec_priority, list)
                    or (isinstance(spec_priority, dict) and fmt in spec_priority)):
                catalog_served.add(fmt)
        # export.formats names formats, each written once to <fmt>/; `arrow` is
        # the canonical every exporter reads, not an export target. The writer
        # comes from export.priority.
        formats = (d.get("export") or {}).get("formats") or []  # a list: see _field_errors
        for fmt in formats:
            if fmt not in EXPORTED_FORMATS:
                hint = " (name the format; export.priority picks the writer)" if isinstance(fmt, str) and "@" in fmt else ""
                errors.append(f"{slug}: export.formats entry {fmt!r} is not an exported format "
                              f"({', '.join(EXPORTED_FORMATS)}){hint}")
        if d.get("derive"):
            continue
        fetch = d.get("fetch") or {}
        ftype = fetch.get("type")
        urls = fetch.get("urls") or []
        auth = fetch.get("auth")

        if not urls and ftype not in ("custom", "generated"):
            errors.append(f"{slug}: fetch.urls is empty but fetch.type={ftype!r} (only 'custom' and 'generated' may be empty)")

        if ftype == "generated":
            from raincloud._generated import generation_recipe

            from .generators import REGISTRY
            try:
                recipe = generation_recipe(fetch)
                generator = REGISTRY.get(recipe["generator"])
                if generator is None:
                    raise ValueError(f"unknown generator {recipe['generator']!r} "
                                     f"(registered: {', '.join(sorted(REGISTRY))})")
                generator.validate(recipe["parameters"])
                if fetch.get("output") not in generator.outputs:
                    raise ValueError(f"unknown generated output {fetch.get('output')!r} "
                                     f"(outputs: {', '.join(generator.outputs)})")
            except (ValueError, KeyError, TypeError, CatalogError) as exc:
                errors.append(f"{slug}: invalid generated fetch: {exc}")

        if ftype == "kaggle" and auth != "kaggle":
            errors.append(f"{slug}: fetch.type=kaggle requires fetch.auth=\"kaggle\" (got {auth!r})")
        if ftype == "huggingface" and auth != "huggingface":
            errors.append(f"{slug}: fetch.type=huggingface requires fetch.auth=\"huggingface\" (got {auth!r})")

        if fetch.get("requires_interactive_accept") and ftype not in ("kaggle", "huggingface"):
            errors.append(
                f"{slug}: fetch.requires_interactive_accept=true is only valid "
                f"for kaggle and huggingface fetches (fetch.type={ftype!r})"
            )

        if fetch.get("hf_allow_patterns") is not None and ftype != "huggingface":
            errors.append(
                f"{slug}: fetch.hf_allow_patterns is huggingface-only "
                f"(fetch.type={ftype!r})"
            )
        if fetch.get("hf_revision") is not None and ftype != "huggingface":
            errors.append(
                f"{slug}: fetch.hf_revision is huggingface-only "
                f"(fetch.type={ftype!r})"
            )

    errors += _uncovered_formats(manifest.get("export_priority"), sorted(catalog_served), "export_priority")

    discovery_errors, discovery_warnings = _check_discovery_vocab({"datasets": datasets})
    errors.extend(discovery_errors)
    warnings.extend(discovery_warnings)

    return errors, warnings


def _selected_manifest(path: Path | None) -> tuple[dict, str]:
    """(manifest, where it came from): `spec.select_manifest`, the same choice
    `load_manifest()` makes, with a hint when it is the installed copy."""
    manifest, where = select_manifest(path)
    packaged = _packaged_data("sources.json")
    if path is None and packaged is not None and where == str(packaged):
        where += (" -- the copy installed with raincloud; pass your sources.json's path, "
                  "or set RAINCLOUD_MANIFEST, to validate your own")
    return manifest, where


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m raincloud.pipeline.validate_manifest",
                                 description=__doc__.split("\n", 1)[0])
    ap.add_argument("manifest", nargs="?", type=Path,
                    help="manifest to validate (default: the selected catalog's sources.json)")
    ap.add_argument("--json", action="store_true", help="emit a JSON report")
    ap.add_argument("--strict", action="store_true",
                    help="treat warnings as errors")
    args = ap.parse_args(argv)

    try:
        manifest, where = _selected_manifest(args.manifest)
    except (OSError, ValueError, CatalogError) as exc:
        # The input this exists to diagnose: an error, never a traceback.
        where = str(args.manifest) if args.manifest is not None else "the selected manifest"
        if args.json:
            json.dump({"ok": False, "manifest": where, "errors": [str(exc)], "warnings": []},
                      sys.stdout, indent=2)
            sys.stdout.write("\n")
        else:
            print(f"{ap.prog}: {where}: {exc}", file=sys.stderr)
        return 1
    schema_errors, schema_skip = _schema_errors(manifest)
    cross_errors, warnings = _cross_checks(manifest)
    errors = schema_errors + cross_errors
    if args.strict:
        errors += warnings
        warnings = []

    if args.json:
        report = {
            "ok": not errors,
            "manifest": where,
            "n_datasets": len(manifest.get("datasets", [])),
            "schema_skipped": schema_skip,
            "errors": errors,
            "warnings": warnings,
        }
        json.dump(report, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0 if not errors else 1

    n = len(manifest.get("datasets", []))
    print(f"validating {where} ({n} datasets)")
    if schema_skip:
        print(f"  note: {schema_skip}")
    elif schema_errors:
        print(f"  schema check: {len(schema_errors)} error(s)")
    else:
        print("  schema check: ok")

    if errors:
        print(f"\nERRORS ({len(errors)}):")
        for e in errors:
            print(f"  - {e}")
    if warnings:
        print(f"\nWARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"  - {w}")
    if not errors and not warnings:
        print("  cross-checks: ok")
        print("\nmanifest is valid.")
    elif not errors:
        print("\nmanifest is valid (with warnings).")
    else:
        print("\nmanifest has errors.")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
