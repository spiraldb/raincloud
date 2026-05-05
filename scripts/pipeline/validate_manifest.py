# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Static checks for sources.json — runs in well under a second.

Two layers of validation:

  1. JSON Schema (sources.schema.json, Draft 2020-12) — shape, enums,
     required fields, regexes. Requires the optional `jsonschema` package;
     if it's not installed, this layer is skipped with a hint and only the
     cross-checks below run.
  2. Cross-checks the schema can't express:
       - slug uniqueness
       - every transform.handler resolves in the live registry
         (scripts/pipeline/handlers/__init__.py)
       - every registered handler is referenced by ≥1 spec (orphans → warning)
       - fetch.urls non-empty unless fetch.type == "custom"
       - fetch.auth matches fetch.type for kaggle / huggingface
       - fetch.requires_interactive_accept only on fetch.type == "kaggle"
       - convert.vortex consistency

Exit codes:
  0  manifest is valid (warnings allowed)
  1  one or more errors

Usage:
  python -m scripts.pipeline.validate_manifest
  python -m scripts.pipeline.validate_manifest --json
  python -m scripts.pipeline.validate_manifest --strict   # warnings → errors
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict

from .spec import REPO_ROOT, load_manifest

SCHEMA_PATH = REPO_ROOT / "sources.schema.json"


def _schema_errors(manifest: dict) -> tuple[list[str], str | None]:
    """Run JSON Schema validation if jsonschema is importable.

    Returns (errors, skip_reason). When jsonschema is missing, errors is []
    and skip_reason explains why.
    """
    try:
        import jsonschema
    except ImportError:
        return [], "jsonschema not installed (uv pip install jsonschema for full schema checks)"
    if not SCHEMA_PATH.exists():
        return [f"sources.schema.json missing at {SCHEMA_PATH}"], None
    schema = json.loads(SCHEMA_PATH.read_text())
    v = jsonschema.Draft202012Validator(schema)
    errs = []
    for e in v.iter_errors(manifest):
        path = ".".join(str(p) for p in e.absolute_path) or "<root>"
        errs.append(f"{path}: {e.message}")
    return errs, None


def _registry_handlers() -> set[str]:
    from .handlers import _REGISTRY
    return set(_REGISTRY)


def _cross_checks(manifest: dict) -> tuple[list[str], list[str]]:
    """Returns (errors, warnings)."""
    errors: list[str] = []
    warnings: list[str] = []

    datasets = manifest.get("datasets", [])

    # Slug uniqueness.
    slugs = [d.get("slug") for d in datasets]
    dups = [s for s, c in Counter(slugs).items() if c > 1]
    for s in dups:
        errors.append(f"duplicate slug: {s!r}")

    # Handler resolution + orphan detection.
    registry = _registry_handlers()
    used_by: dict[str, list[str]] = defaultdict(list)
    for d in datasets:
        h = (d.get("transform") or {}).get("handler")
        slug = d.get("slug", "?")
        if h is None:
            errors.append(f"{slug}: transform.handler is missing")
            continue
        if h not in registry:
            errors.append(
                f"{slug}: transform.handler={h!r} is not in the registry "
                f"(scripts/pipeline/handlers/__init__.py)"
            )
        used_by[h].append(slug)
    orphans = sorted(registry - set(used_by))
    for h in orphans:
        warnings.append(f"handler {h!r} is registered but referenced by 0 specs")

    # Per-spec sanity checks the schema can't express.
    for d in datasets:
        slug = d.get("slug", "?")
        fetch = d.get("fetch") or {}
        ftype = fetch.get("type")
        urls = fetch.get("urls") or []
        auth = fetch.get("auth")

        if not urls and ftype != "custom":
            errors.append(f"{slug}: fetch.urls is empty but fetch.type={ftype!r} (only 'custom' may be empty)")

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

        # write.output should agree with slug for single-output handlers; we can't
        # know which handlers are multi-output without running them, so just
        # flag a hint when neither matches.
        write_out = (d.get("write") or {}).get("output", "")
        if write_out and not write_out.startswith(slug) and not write_out.startswith(slug.replace("_", "-")):
            warnings.append(
                f"{slug}: write.output={write_out!r} doesn't start with slug — "
                f"OK for multi-output handlers, otherwise consider renaming"
            )

        # Vortex skip reason must be paired with vortex=false (and only with).
        convert = d.get("convert") or {}
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

    return errors, warnings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--json", action="store_true", help="emit a JSON report")
    ap.add_argument("--strict", action="store_true",
                    help="treat warnings as errors")
    args = ap.parse_args(argv)

    manifest = load_manifest()
    schema_errors, schema_skip = _schema_errors(manifest)
    cross_errors, warnings = _cross_checks(manifest)
    errors = schema_errors + cross_errors
    if args.strict:
        errors += warnings
        warnings = []

    if args.json:
        report = {
            "ok": not errors,
            "n_datasets": len(manifest.get("datasets", [])),
            "schema_skipped": schema_skip,
            "errors": errors,
            "warnings": warnings,
        }
        json.dump(report, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0 if not errors else 1

    n = len(manifest.get("datasets", []))
    print(f"validating sources.json ({n} datasets)")
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
