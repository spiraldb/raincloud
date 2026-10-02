# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Language-neutral catalog bundle format, integrity and capability checks."""
from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from pathlib import Path

from ._catalog import known_schema_versions
from ._formats import ALL_FORMATS, EXPORTED_FORMATS, export_cells, priority_shape_error
from ._registry import BUNDLE_READER_TOKENS, builder_capabilities
from .exceptions import CatalogError

REVISION = re.compile(r"[0-9a-f]{64}\Z")
SLUG = re.compile(r"[a-zA-Z0-9][a-zA-Z0-9_.-]*\Z")
FILES = ("sources.json", "snapshot.json")


def encode(value) -> bytes:
    try:
        return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                           allow_nan=False) + "\n").encode()
    except ValueError as exc:
        raise CatalogError(f"invalid catalog JSON: {exc}") from exc


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def document(raw: bytes, label: str) -> dict:
    def finite_float(token: str) -> float:
        value = float(token)
        if not math.isfinite(value):
            raise ValueError("catalog floating-point numbers must be finite")
        return value

    def invalid_constant(token: str):
        raise ValueError(f"invalid JSON number: {token}")

    try:
        value = json.loads(raw, parse_float=finite_float, parse_constant=invalid_constant)
    except (ValueError, UnicodeError) as exc:
        raise CatalogError(f"invalid {label}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalogError(f"{label} must be a JSON object")
    return value


_RECIPE_KEYS = ("slug", "fetch", "extract", "parse", "transform", "write", "expect", "convert", "export",
                "hydrate", "derive")


def recipe_hash(spec: dict, version: int, *, specs) -> str:
    """Fingerprint of what determines a dataset's bytes.

    `specs` maps slug -> spec for the whole catalog (or None when the caller has
    no catalog). A derived dataset's bytes depend on its parent's, so its recipe
    folds in the parent's recipe: rebuilding the parent makes it stale, while
    editing the derivation never touches the parent.
    """
    # Display metadata, descriptions, licenses, tags and advisories do not change bytes.
    # This key list only grows: dropping a key re-fingerprints every catalog that
    # still carries it, so artifacts built against it stop matching their pins.
    # (`hydrate` is the pre-0.3.0 spelling of a hydration block on the parent.)
    recipe = {key: spec[key] for key in _RECIPE_KEYS if key in spec}
    # Which formats a dataset offers decides no file's bytes (since 0.3.1 an install
    # chooses what it builds), so `export.formats` stays out, and an `export` left
    # empty without it fingerprints like none at all.
    if isinstance(recipe.get("export"), dict) and "formats" in recipe["export"]:
        export = {k: v for k, v in recipe["export"].items() if k != "formats"}
        if export:
            recipe["export"] = export
        else:
            del recipe["export"]
    parent = (spec.get("derive") or {}).get("from")
    if parent and specs and parent in specs and parent != spec.get("slug"):
        recipe["from_recipe"] = recipe_hash(specs[parent], version, specs=specs)
    return digest(encode({"schema_version": version, "recipe": recipe}))


def capabilities() -> dict:
    """The bundle `readers` and `builders` tokens this installation supports,
    from `raincloud._registry`, their one declaration."""
    return {"readers": list(BUNDLE_READER_TOKENS), "builders": builder_capabilities()}


def build_requirements(manifest: dict) -> list[str]:
    required = set()
    for spec in manifest.get("datasets", []):
        handler = spec.get("transform", {}).get("handler")
        if handler:
            required.add(f"handler:{handler}")
        fetch = spec.get("fetch", {})
        if fetch.get("type") == "generated":
            required.add(f"generator:{fetch['generator']}")
        if fetch.get("type") == "custom":
            # Named like any other seam, so a catalog needing a fetcher this
            # build does not have says so up front instead of failing partway
            # through a fetch.
            required.add(f"fetcher:{fetch.get('notes') or spec['slug']}")
        required.update(f"exporter:{cell}" for cell in export_cells(spec, manifest))
    return sorted(required)


def _check_priority(value, where: str):
    """A writer order, by `_formats.priority_shape_error`'s rule."""
    error = priority_shape_error(value, where)
    if error:
        raise CatalogError(error)


def validate_documents(manifest: dict, snapshot: dict):
    known = known_schema_versions()
    version = manifest.get("schema_version")
    if type(version) is not int or version not in known:
        expected = " or ".join(str(v) for v in known)
        raise CatalogError(f"unsupported manifest schema_version; expected {expected}")
    if snapshot.get("schema_version") != version:
        raise CatalogError("manifest and snapshot schema_version must match")
    if not isinstance(manifest.get("datasets"), list) or not isinstance(snapshot.get("slugs"), dict):
        raise CatalogError("catalog needs a datasets list and snapshot slugs object")
    _check_priority(manifest.get("export_priority"), "export_priority")
    slugs = set()
    for spec in manifest["datasets"]:
        if not isinstance(spec, dict) or not isinstance(spec.get("slug"), str) or not SLUG.fullmatch(spec["slug"]):
            raise CatalogError("unsafe or missing dataset slug")
        if spec["slug"] in slugs:
            raise CatalogError(f"duplicate dataset slug: {spec['slug']}")
        slugs.add(spec["slug"])
        for key in ("license", "fetch", "extract", "parse", "transform", "write", "expect", "convert", "export",
                    "hydrate", "derive"):
            if key in spec and not isinstance(spec[key], dict):
                raise CatalogError(f"{spec['slug']}: {key} must be an object")
        # A v2 `convert` block is refused when a manifest is authored (schema and
        # validate_manifest), not here: v2 catalogs released before that rule
        # carry one, and released catalogs keep reading (_formats.export_formats).
        if spec.get("fetch", {}).get("type") == "generated":
            from ._generated import generation_recipe
            try:
                generation_recipe(spec["fetch"])
            except ValueError as exc:
                raise CatalogError(f"{spec['slug']}: {exc}") from exc
        export = spec.get("export", {})
        formats = export.get("formats")
        if formats is not None and (not isinstance(formats, list) or not all(f in EXPORTED_FORMATS for f in formats)):
            raise CatalogError(f"{spec['slug']}: export.formats must list formats ({', '.join(EXPORTED_FORMATS)})")
        _check_priority(export.get("priority"), f"{spec['slug']}: export.priority")
    # A derived dataset's parent is an ordinary dataset in the same catalog. That
    # also rules out derive cycles, which recipe_hash would otherwise recurse on.
    derived = {spec["slug"]: spec["derive"] for spec in manifest["datasets"] if "derive" in spec}
    for slug, derive in derived.items():
        parent = derive.get("from")
        if not isinstance(parent, str) or parent not in slugs or parent in derived:
            raise CatalogError(f"{slug}: derive.from must name a dataset in this catalog that is not itself derived")
        if not isinstance(derive.get("hydrate"), dict):
            raise CatalogError(f"{slug}: derive.hydrate must be an object")
    for slug, entry in snapshot["slugs"].items():
        if not SLUG.fullmatch(slug) or not isinstance(entry, dict):
            raise CatalogError("unsafe snapshot slug or invalid entry")
        if slug not in slugs:
            raise CatalogError(f"snapshot slug {slug!r} has no manifest recipe")
        for fmt in ALL_FORMATS:
            sha, size = entry.get(f"{fmt}_sha256"), entry.get(f"{fmt}_bytes")
            if sha is not None and (not isinstance(sha, str) or not REVISION.fullmatch(sha)):
                raise CatalogError(f"{slug}: invalid {fmt} checksum")
            if size is not None and (type(size) is not int or size < 0):
                raise CatalogError(f"{slug}: invalid {fmt} byte size")
            # Provenance only: a writer this version does not know is still a
            # file it can read.
            if not isinstance(entry.get(f"{fmt}_writer", ""), (str, type(None))):
                raise CatalogError(f"{slug}: invalid {fmt} writer")
        columns = entry.get("columns")
        if columns is not None and not (isinstance(columns, list) and all(
                isinstance(c, dict) and isinstance(c.get("name"), str) for c in columns)):
            raise CatalogError(f"{slug}: columns must be a list of objects with a name")


# The catalog bundle envelope. Unlike `schema_version`, no data file declares
# this. Python is its only implementation: native clients read catalogs through
# the `raincloud` CLI rather than parsing bundles themselves. Format 1 bundles
# exist only in pre-0.3.0 box-local stores; reading them can go once no
# supported store still holds one.
CATALOG_FORMAT = 2
READABLE_CATALOG_FORMATS = (1, 2)


@dataclass(frozen=True)
class Bundle:
    metadata: bytes
    manifest: bytes
    snapshot: bytes

    @property
    def revision(self) -> str:
        return digest(self.metadata)

    @property
    def catalog_id(self) -> str:
        return document(self.metadata, "catalog.json")["catalog_id"]

    def files(self) -> dict[str, bytes]:
        return {"catalog.json": self.metadata, "sources.json": self.manifest, "snapshot.json": self.snapshot}

    def validate(self, expected: str | None = None):
        meta = document(self.metadata, "catalog.json")
        if expected is not None and (not REVISION.fullmatch(expected) or self.revision != expected):
            raise CatalogError("catalog revision hash mismatch")
        # catalog_format 1 carried an `engine` version window. It was removed in 2: a
        # version RANGE cannot express forward compatibility (a 0.4.0 release would have
        # refused every catalog packed under 0.3.x, and widening the range rewrites every
        # revision hash and invalidates every pin), while the `readers` / `builders`
        # capability lists below say the same thing precisely and degrade gracefully.
        # Format 1 bundles still load; their `engine` field is accepted and ignored.
        fmt = meta.get("catalog_format")
        if type(fmt) is not int or fmt not in READABLE_CATALOG_FORMATS:
            raise CatalogError("unsupported catalog_format")
        expected_fields = {"catalog_format", "catalog_id", "files", "readers", "builders"}
        if fmt == 1:
            expected_fields = expected_fields | {"engine"}
        if set(meta) != expected_fields:
            raise CatalogError("unknown or missing catalog metadata fields")
        if not isinstance(meta["catalog_id"], str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}", meta["catalog_id"]):
            raise CatalogError("invalid catalog_id")
        if not isinstance(meta["files"], dict) or set(meta["files"]) != set(FILES):
            raise CatalogError("bundle must hash sources.json and snapshot.json")
        for name, raw in (("sources.json", self.manifest), ("snapshot.json", self.snapshot)):
            if meta["files"][name] != digest(raw):
                raise CatalogError(f"{name} checksum mismatch")
        if fmt == 1 and not isinstance(meta["engine"], dict):
            raise CatalogError("invalid engine compatibility bounds")
        for key in ("readers", "builders"):
            if not isinstance(meta[key], list) or not all(isinstance(v, str) for v in meta[key]):
                raise CatalogError(f"{key} capabilities must be a list of names")
        missing = set(meta["readers"]) - set(capabilities()["readers"])
        if missing:
            raise CatalogError(f"unsupported reader capabilities: {sorted(missing)}")
        manifest, snapshot = document(self.manifest, "manifest"), document(self.snapshot, "snapshot")
        validate_documents(manifest, snapshot)
        # `builders` is not re-derived here. The files are already pinned by the
        # revision hash, and deriving with *this* code's naming made every
        # catalog packed by an older release unreadable, even for a describe.
        # Builds recompute requirements for the recipe they run (build_check).
        return self


def make_bundle(manifest: bytes, snapshot: bytes, catalog_id: str, *, readers=None) -> Bundle:
    m, s = document(manifest, "manifest"), document(snapshot, "snapshot")
    validate_documents(m, s)
    meta = {"catalog_format": CATALOG_FORMAT, "catalog_id": catalog_id,
            "files": {"sources.json": digest(manifest), "snapshot.json": digest(snapshot)},
            "readers": readers if readers is not None else ["artifact-layout-v1"],
            "builders": build_requirements(m)}
    return Bundle(encode(meta), manifest, snapshot).validate()


def read_bundle(path: Path, revision: str | None = None) -> Bundle:
    try:
        return Bundle(*(path.joinpath(name).read_bytes() for name in ("catalog.json", *FILES))).validate(revision)
    except OSError as exc:
        raise CatalogError(f"cannot read catalog bundle {path}: {exc}") from exc
