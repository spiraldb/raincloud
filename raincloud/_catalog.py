# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Catalog: per-slug metadata + checksums from the shipped snapshot + manifest."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from functools import lru_cache
from importlib import resources
from pathlib import Path

from .exceptions import UnknownSlug


def _repo_root() -> Path:
    # raincloud/_catalog.py -> repo root is two parents up in a source checkout.
    return Path(__file__).resolve().parent.parent


def _data_file(kind: str) -> Path:
    """Locate a data file. kind in {"snapshot", "manifest"}.

    Precedence: env override -> wheel-packaged copy -> repo source fallback.
    """
    env = {"snapshot": "RAINCLOUD_SNAPSHOT", "manifest": "RAINCLOUD_MANIFEST"}[kind]
    override = os.environ.get(env)
    if override:
        return Path(override)
    packaged_name = {"snapshot": "snapshot.json", "manifest": "sources.json"}[kind]
    try:
        p = resources.files("raincloud").joinpath("_data", packaged_name)
        if p.is_file():
            return Path(str(p))
    except FileNotFoundError:
        pass
    repo_name = {"snapshot": "docs/v1/snapshot.json", "manifest": "sources.json"}[kind]
    return _repo_root() / repo_name


@dataclass(frozen=True)
class FormatInfo:
    sha256: str | None
    nbytes: int | None


@dataclass(frozen=True)
class Entry:
    slug: str
    rows: int | None
    columns: list[dict] = field(default_factory=list)
    formats: dict[str, FormatInfo] = field(default_factory=dict)
    info: dict = field(default_factory=dict)

    @property
    def column_names(self) -> list[str]:
        return [c["name"] for c in self.columns]


class Catalog:
    def __init__(self, snapshot: dict, manifest: dict):
        self._slugs = snapshot.get("slugs", {})
        self._specs = {d["slug"]: d for d in manifest.get("datasets", [])}

    def __contains__(self, slug: str) -> bool:
        return slug in self._slugs or slug in self._specs

    def slugs(self) -> list[str]:
        return sorted(set(self._slugs) | set(self._specs))

    def entry(self, slug: str) -> Entry:
        if slug not in self:
            raise UnknownSlug(slug)
        snap = self._slugs.get(slug, {})
        spec = self._specs.get(slug, {})
        formats: dict[str, FormatInfo] = {}
        for fmt in ("parquet", "vortex"):
            nbytes = snap.get(f"{fmt}_bytes")
            if nbytes is None:
                continue
            formats[fmt] = FormatInfo(sha256=snap.get(f"{fmt}_sha256"), nbytes=nbytes)
        lic = spec.get("license", {}) or {}
        urls = (spec.get("fetch", {}) or {}).get("urls") or []
        info = {
            "short_name": spec.get("short_name"),
            "full_name": spec.get("full_name"),
            "description": spec.get("description"),
            "license": lic,
            "source_url": urls[0] if urls else lic.get("source_url"),
        }
        return Entry(
            slug=slug,
            # Prefer last_built_rows, fall back to expected_rows. Truthiness
            # means rows==0 falls through to expected_rows — benign: no slug
            # has last_built_rows==0.
            rows=snap.get("last_built_rows") or snap.get("expected_rows"),
            columns=snap.get("columns") or [],
            formats=formats,
            info=info,
        )


@lru_cache(maxsize=1)
def load_catalog() -> Catalog:
    snapshot = json.loads(_data_file("snapshot").read_text())
    manifest = json.loads(_data_file("manifest").read_text())
    return Catalog(snapshot, manifest)
