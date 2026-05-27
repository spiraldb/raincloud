"""Raincloud loader: datasets-style access to prepared Vortex/Parquet files."""
from __future__ import annotations

from .exceptions import (  # noqa: F401
    ArtifactNotFound,
    BuildToolingMissing,
    ChecksumMismatch,
    FormatUnavailable,
    MissingDependency,
    OfflineMiss,
    RaincloudError,
    UnknownSlug,
)

__version__ = "0.2.0"
