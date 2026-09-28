# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Typed error hierarchy for the raincloud loader."""
from __future__ import annotations


class RaincloudError(Exception):
    """Base class for all loader errors."""


class UnknownSlug(RaincloudError):
    """Requested slug is not in the catalog. `suggestions` holds near matches."""

    def __init__(self, message: str, *, slug: str | None = None, suggestions: list[str] | None = None):
        super().__init__(message)
        self.slug = slug if slug is not None else message
        self.suggestions = suggestions or []


class FormatUnavailable(RaincloudError):
    """Requested format is not available for this slug.

    `measurement` is set when a build MEASURED that its writer cannot produce
    the format at this recipe (the writer cell, `error`, `toolchain`,
    `measured_at`, `recipe`); the message quotes it. It is None when the
    format is simply not one the dataset has.
    """

    def __init__(self, message: str, *, measurement: dict | None = None):
        super().__init__(message)
        self.measurement = measurement


class ArtifactNotFound(RaincloudError):
    """Artifact key was not present at the transport source (clean miss)."""


class MirrorUnavailable(RaincloudError):
    """The mirror could not be read: unreachable, refused, or not a store at all.

    Distinct from ArtifactNotFound, which is a mirror that answered "no such
    file". The message names the mirror without its credentials or query.
    """


class UnknownColumn(RaincloudError):
    """A requested column is not in the artifact's schema."""


class ChecksumMismatch(RaincloudError):
    """Downloaded artifact's sha256 did not match the catalog."""


class BuildToolingMissing(RaincloudError):
    """Local build was needed but `raincloud[build]` is not installed (or the
    build subtree failed to import for another reason — the message says which)."""


class BuildFailed(RaincloudError):
    """The local build subprocess ran but exited non-zero."""


class OfflineMiss(RaincloudError):
    """Offline mode is on and the artifact is not in the local cache."""


class MissingDependency(RaincloudError):
    """An optional dependency (e.g. pandas, a Vortex reader) is not installed."""


class CatalogError(RaincloudError):
    """Catalog syntax, integrity, selection or compatibility is invalid."""


class CatalogConflict(RaincloudError):
    """A producer (build, publish) would write a dataset declared by another recipe.

    Producer-side only: readers never raise it, and there is no provenance or
    ownership check on the files a reader is served."""


class MissingRevision(CatalogError):
    """The selected catalog revision is not installed locally."""


class UnsupportedType(RaincloudError):
    """The selected reader cannot represent the artifact's data type.

    The bytes are fine; this build cannot express them. Ask for another format.
    Contrast `CorruptArtifact`, where the bytes themselves are the problem.
    """


class CorruptArtifact(RaincloudError):
    """The artifact was found but could not be decoded.

    Truncated, damaged, or not the format its path claims. The remedy is to
    re-fetch or rebuild, not to change format. Mirrors
    `RAINCLOUD_CORRUPT_ARTIFACT` in the C API, so a Python caller and a C or
    Java caller classify the same failure the same way: Dataset readers raise
    it when the decoder reports bad bytes in a file that is present
    (`raincloud._decoding` lists the exact set). An access failure -- permission
    denied, a vanished file -- raises its OSError instead.
    """


class HydratedDatasetWarning(UserWarning):
    """Loaded a hydrated dataset: columns fetched from the open web, not an upstream file."""
