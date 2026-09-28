# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Catalog: per-slug metadata + checksums from the shipped snapshot + manifest."""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from functools import lru_cache
from importlib import resources
from pathlib import Path

from ._formats import ALL_FORMATS, buildable_formats
from .config import Config, get_config
from .exceptions import CatalogError, UnknownSlug


def _repo_root() -> Path:
    # raincloud/_catalog.py -> repo root is two parents up in a source checkout.
    return Path(__file__).resolve().parent.parent


def _manifest_path(config: Config | None = None) -> Path:
    """The manifest the loader will actually read — `RAINCLOUD_MANIFEST` first.

    Shared by `_data_file` and `_manifest_schema_version` so the version probe
    and the read always name the same manifest; otherwise an override would be
    paired with the checkout's snapshot for an unrelated schema_version.
    """
    override = (config or get_config()).manifest
    if override:
        return Path(override).expanduser()
    return _repo_root() / "sources.json"


def _manifest_schema_version(config: Config | None = None) -> int | None:
    """Read the RESOLVED manifest's schema_version, or None if there isn't one.

    Used only to pick the checkout snapshot directory (`docs/v{n}/`). Tolerant
    of a missing/malformed manifest — a wheel install has no checkout
    `sources.json`, and the packaged-snapshot fallback covers it regardless.
    "Unknown" is reported as None rather than as a guess: a wrong version here
    would silently point the loader at another layout's snapshot.
    """
    manifest = _manifest_path(config)
    if not manifest.is_file():
        return None

    def probe():
        try:
            version = json.loads(manifest.read_text()).get("schema_version")
        except (json.JSONDecodeError, OSError, ValueError, TypeError):
            return None
        return version if type(version) is int else None

    from .catalogs import _parsed  # parsed once per manifest version, like the catalog
    return _parsed(("schema_version", str(manifest)), [manifest], probe)


def _snapshot_repo_path(config: Config | None = None) -> Path:
    """Checkout snapshot path, derived from the manifest schema_version.

    The build pipeline writes the tracked snapshot to `docs/v{n}/snapshot.json`
    (version-scoped, matching `outputs/v{n}/`), so the loader must read the
    same version the manifest declares. **Scaffold-safe fallback:** if
    `docs/v{n}/snapshot.json` doesn't exist yet (e.g. v{n} promoted for the
    manifest but the snapshot not yet generated), fall back to the oldest known
    layout so the loader never points at a missing file. That path is returned
    when neither exists too — an existence check upstream then falls through to
    the wheel-packaged copy, and it is a sensible error target.
    """
    root = _repo_root()
    version = _manifest_schema_version(config)
    if version is not None:
        versioned = root / "docs" / f"v{version}" / "snapshot.json"
        if versioned.is_file():
            return versioned
    return root / "docs" / f"v{known_schema_versions()[0]}" / "snapshot.json"


@lru_cache(maxsize=1)
def known_schema_versions() -> tuple[int, ...]:
    """The artifact layouts this build understands, read from the JSON Schema.

    `sources.schema.json` already declares the legal set as an enum on
    `schema_version` -- it is the one place a v3 has to be written down for the
    manifest to validate at all. Reading it back here means Python has no second
    opinion to keep in sync; Python is its only reader, since native clients
    resolve catalogs through the `raincloud` CLI.

    Raises CatalogError when neither the checkout nor the packaged schema can be
    read: that is a broken install, and guessing a version would pick a layout.
    """
    for candidate in (_repo_root() / "sources.schema.json", _packaged_schema()):
        if candidate is None or not candidate.is_file():
            continue
        try:
            enum = json.loads(candidate.read_text())["properties"]["schema_version"]["enum"]
        except (json.JSONDecodeError, OSError, KeyError, TypeError):
            continue
        versions = tuple(sorted(v for v in enum if type(v) is int))
        if versions:
            return versions
    raise CatalogError(
        "cannot read the schema_version enum from sources.schema.json; "
        "the installation is incomplete"
    )


def _packaged_schema() -> Path | None:
    try:
        p = resources.files("raincloud").joinpath("_data", "sources.schema.json")
        return Path(str(p)) if p.is_file() else None
    except FileNotFoundError:
        return None


def _data_file(kind: str, config: Config | None = None) -> Path:
    """Locate a data file. kind in {"snapshot", "manifest"}.

    Precedence: env override -> repo source copy -> wheel-packaged copy. This
    matches `raincloud.pipeline.spec._default_manifest` and the documented
    intent ("checkout copy, else the wheel-packaged copy") so the loader and
    the build pipeline never read different copies in an editable install.

    The snapshot's checkout path is version-scoped (`docs/v{n}/snapshot.json`,
    derived from the RESOLVED manifest's schema_version — so overriding the
    manifest moves the snapshot with it) with a `docs/v1/` fallback; the manifest
    is `RAINCLOUD_MANIFEST` or the checkout `sources.json`.
    """
    config = config or get_config()
    if kind == "manifest":
        # `_manifest_path` owns the override so the version probe and the read
        # can never resolve different manifests.
        repo = _manifest_path(config)
        if repo.is_file() or config.manifest:
            return repo
    else:
        override = config.snapshot
        if override:
            return Path(override).expanduser()
        repo = _snapshot_repo_path(config)
    if repo.is_file():
        return repo
    packaged_name = {"snapshot": "snapshot.json", "manifest": "sources.json"}[kind]
    try:
        p = resources.files("raincloud").joinpath("_data", packaged_name)
        if p.is_file():
            return Path(str(p))
    except FileNotFoundError:
        pass
    return repo  # let open() error point at the expected checkout path


def recorded_unavailable(spec: dict, snapshot_entry: dict, fmt: str, version: int, specs: dict) -> dict | None:
    """The catalog's measurement that `spec`'s `fmt` cannot be made, or None.

    `snapshot_entry` is the spec's snapshot record, `specs` the catalog's specs
    by slug. Only a measurement taken at the spec's current recipe counts: once
    the recipe changes, it no longer says anything about the dataset. Raises
    CatalogError for a record that is not a measurement object.
    """
    measured = (snapshot_entry or {}).get(f"{fmt}_unavailable")
    if measured is None:
        return None
    if not isinstance(measured, dict):
        raise CatalogError(f"{spec['slug']}: {fmt}_unavailable must be a measurement object")
    if fmt not in buildable_formats(spec, version):
        return None
    from ._bundle import recipe_hash
    return measured if measured.get("recipe") == recipe_hash(spec, version, specs=specs) else None


@dataclass(frozen=True)
class FormatInfo:
    sha256: str | None
    nbytes: int | None
    # Which writer made the file (`py`, `rs`, `canonical`, ...): provenance, not
    # part of its address. None in catalogs recorded before it was tracked.
    writer: str | None = None
    # A build's measurement that the planned writer cannot produce this format
    # at this recipe (`<fmt>_unavailable` in the snapshot: cell, error,
    # toolchain, recipe, measured_at). None: available, or never measured.
    unavailable: dict | None = None
    # Whether the writer read the file back and found the canonical
    # (`<fmt>_verified`); None in catalogs recorded before it was, and for
    # files the build record did not describe. False only for a sidecar that
    # reported its read-back unmeasured, with its reason (`<fmt>_verify_note`).
    verified: bool | None = None
    verify_note: str | None = None


@dataclass(frozen=True)
class Entry:
    slug: str
    rows: int | None
    columns: list[dict] = field(default_factory=list)
    formats: dict[str, FormatInfo] = field(default_factory=dict)
    info: dict = field(default_factory=dict)
    # schema_version this slug's artifacts live under (outputs/v{n}, cache v{n}).
    # Threaded onto every Entry so resolve()/artifact_key() compose the right
    # version prefix. Required: a forgotten one must not quietly mean v1.
    version: int = field(kw_only=True)
    catalog_id: str | None = None
    revision: str | None = None
    recipe: str | None = None
    # Context.legacy of the catalog this came from (loose checkout/bundled files).
    legacy: bool = False

    @property
    def column_names(self) -> list[str]:
        return [c["name"] for c in self.columns]


def unverified(entry: Entry, fmt: str, data_dir: Path, local: Path | None) -> str | None:
    """Why `entry`'s `fmt` file was published without its writer verifying
    that it reads back to the canonical, or None.

    `local` is the file prepared on this machine, if any. When it is this
    install's build (in the data dir, the size its build record names, at the
    current recipe) the build record answers; otherwise the catalog does.
    """
    from . import _builds
    from ._resolve import artifact_key

    info = entry.formats.get(fmt)
    if info is None:
        return None
    key = artifact_key(entry.slug, fmt, entry.version)
    size = local.stat().st_size if local is not None and local == Path(data_dir) / key and local.is_file() else None
    catalog = (info.verify_note or "its writer did not verify it") if info.verified is False else None
    return _builds.unverified_at(_builds.lookup(Path(data_dir), key), entry.recipe, size, catalog)


class Catalog:
    def __init__(self, snapshot: dict, manifest: dict, context=None):
        self.context = context
        self._slugs = snapshot.get("slugs", {})
        self._specs = {d["slug"]: d for d in manifest.get("datasets", [])}
        # Recipes determine the build namespace. A fallback snapshot from a
        # different schema version must never move a build into its namespace
        # or apply that older artifact's checksum to newly built bytes.
        version = manifest.get("schema_version") or snapshot.get("schema_version")
        if type(version) is not int:
            raise CatalogError(
                "catalog has no schema_version; neither the manifest nor the "
                "snapshot declares one, and guessing would pick a build namespace"
            )
        self._version = version
        # resolve_context owns the skew rule (it warns and drops a skewed loose
        # snapshot; bundles refuse one), so a skew here is a caller bug.
        if snapshot.get("schema_version", version) != version:
            raise CatalogError(
                f"catalog snapshot is schema_version {snapshot.get('schema_version')} but the "
                f"manifest is {version}; resolve the catalog through resolve_context"
            )

    def __contains__(self, slug: str) -> bool:
        # Every snapshot slug has a recipe (validate_documents), so the recipes
        # are the whole set of names.
        return slug in self._specs

    def slugs(self) -> list[str]:
        return sorted(self._specs)

    def entry(self, slug: str) -> Entry:
        if slug not in self:
            from ._suggest import hint, suggest
            names = self.slugs()
            raise UnknownSlug(hint(slug, names, everything="`raincloud list` shows every dataset.",
                                   narrow="`raincloud list {query}`"),
                              slug=slug, suggestions=suggest(slug, names)[0])
        snap = self._slugs.get(slug, {})
        spec = self._specs[slug]
        version = self._version
        formats: dict[str, FormatInfo] = {}
        buildable = buildable_formats(spec, version)
        # The recipe says what can be built; the snapshot independently records
        # artifacts that exist. Neither needs a local docs regeneration to make
        # canonical Arrow loadable.
        for fmt in ALL_FORMATS:
            unavailable = recorded_unavailable(spec, snap, fmt, version, self._specs)
            if fmt in buildable or snap.get(f"{fmt}_bytes") is not None:
                formats[fmt] = FormatInfo(
                    sha256=snap.get(f"{fmt}_sha256"),
                    nbytes=snap.get(f"{fmt}_bytes"),
                    writer=snap.get(f"{fmt}_writer"),
                    unavailable=unavailable,
                    verified=snap.get(f"{fmt}_verified"),
                    verify_note=snap.get(f"{fmt}_verify_note"),
                )
        lic = spec.get("license", {}) or {}
        urls = (spec.get("fetch", {}) or {}).get("urls") or []
        info = {
            "short_name": spec.get("short_name"),
            "full_name": spec.get("full_name"),
            "description": spec.get("description"),
            "license": lic,
            "source_url": urls[0] if urls else lic.get("source_url"),
        }
        derive = spec.get("derive") or {}
        if derive.get("hydrate"):
            info.update(derived_from=derive.get("from"), advisory=spec.get("advisory"),
                        hydrated_columns=dict(derive["hydrate"].get("columns") or {}))
        from ._bundle import recipe_hash

        return Entry(
            slug=slug,
            # last_built_rows when recorded (0 included), else expected_rows.
            rows=snap.get("last_built_rows") if snap.get("last_built_rows") is not None else snap.get("expected_rows"),
            columns=snap.get("columns") or [],
            formats=formats,
            info=info,
            version=version,
            catalog_id=self.context.bundle.catalog_id if self.context else None,
            revision=self.context.bundle.revision if self.context else None,
            recipe=recipe_hash(spec, version, specs=self._specs) if self.context else None,
            legacy=self.context.legacy if self.context else False,
        )


def load_catalog(config: Config | None = None) -> Catalog:
    from .catalogs import current, resolve_context
    context = current() if config is None or config == get_config() else None
    context = context or resolve_context(config or get_config())
    return _cached_catalog(context)


@lru_cache(maxsize=8)
def _cached_catalog(context):
    return Catalog(context.snapshot, context.manifest, context)


def _cache_clear() -> None:
    """Forget every parsed catalog (tests; a long-lived process never needs it)."""
    from .catalogs import clear_parse_cache
    clear_parse_cache()
    _cached_catalog.cache_clear()


load_catalog.cache_clear = _cache_clear
