# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Typed error hierarchy for the raincloud loader."""
from __future__ import annotations


class RaincloudError(Exception):
    """Base class for all loader errors."""


class UnknownSlug(RaincloudError):
    """Requested slug is not in the catalog."""


class FormatUnavailable(RaincloudError):
    """Requested format is not available for this slug."""


class ArtifactNotFound(RaincloudError):
    """Artifact key was not present at the transport source (clean miss)."""


class ChecksumMismatch(RaincloudError):
    """Downloaded artifact's sha256 did not match the catalog."""


class BuildToolingMissing(RaincloudError):
    """Local build was needed but `raincloud[build]` is not installed."""


class OfflineMiss(RaincloudError):
    """Offline mode is on and the artifact is not in the local cache."""


class MissingDependency(RaincloudError):
    """An optional convenience dependency (duckdb/pandas) is not installed."""
