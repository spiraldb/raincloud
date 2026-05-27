"""Raincloud loader: datasets-style access to prepared Vortex/Parquet files."""
from __future__ import annotations

from pathlib import Path

from . import _resolve
from ._catalog import load_catalog
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

_DEFAULT_FORMAT = "vortex"


class Dataset:
    """Lazy handle to a prepared artifact. Nothing is fetched until you ask."""

    def __init__(self, slug: str, fmt: str, *, mirror: str | None,
                 offline: bool | None):
        self.slug = slug
        self.format = fmt
        self._mirror = mirror
        self._offline = offline
        self._entry = load_catalog().entry(slug)

    def __repr__(self) -> str:
        return f"Dataset(slug={self.slug!r}, format={self.format!r})"

    # --- cheap metadata (no I/O beyond the in-memory catalog) ---
    @property
    def num_rows(self) -> int | None:
        return self._entry.rows

    @property
    def column_names(self) -> list[str]:
        return self._entry.column_names

    @property
    def info(self) -> dict:
        return self._entry.info

    # --- resolution ---
    def path(self) -> Path:
        return self.path_for(self.format)

    def path_for(self, fmt: str) -> Path:
        return _resolve.resolve(self.slug, fmt, mirror=self._mirror,
                                offline=self._offline)

    # --- materialization ---
    def to_arrow(self):
        import pyarrow.parquet as pq
        if self.format == "parquet":
            return pq.read_table(self.path())
        import vortex
        return vortex.open(str(self.path())).to_arrow().read_all()

    def to_vortex(self):
        import vortex
        return vortex.open(str(self.path_for("vortex")))

    @property
    def schema(self):
        import pyarrow.parquet as pq
        if self.format == "parquet":
            return pq.read_schema(self.path())  # footer-only, cheap
        import vortex
        # vortex path must open the file (heavier than the parquet footer read)
        return vortex.open(str(self.path())).to_arrow().schema

    def scan(self):
        """Return a DuckDB relation over the dataset.

        Always reads the parquet artifact (DuckDB has no native Vortex
        reader), resolving the parquet sibling even when this handle's
        format is vortex.
        """
        try:
            import duckdb
        except ImportError as e:
            raise MissingDependency(
                "scan() needs DuckDB — install `raincloud[duckdb]`"
            ) from e
        pq_path = self.path_for("parquet")
        return duckdb.connect().read_parquet(str(pq_path))

    def to_pandas(self):
        try:
            import pandas  # noqa: F401
        except ImportError as e:
            raise MissingDependency(
                "to_pandas() needs pandas — install `raincloud[pandas]`"
            ) from e
        return self.to_arrow().to_pandas()


def load(slug: str, *, format: str = _DEFAULT_FORMAT,
         offline: bool | None = None, mirror: str | None = None) -> Dataset:
    """Return a lazy Dataset handle for `slug`.

    `format` defaults to "vortex" and falls back to "parquet" when the slug
    has no vortex artifact. Raises UnknownSlug for an unknown slug and
    FormatUnavailable when neither the requested nor a fallback format exists.
    """
    entry = load_catalog().entry(slug)  # raises UnknownSlug
    fmt = format
    if fmt not in entry.formats:
        if fmt == "vortex" and "parquet" in entry.formats:
            fmt = "parquet"
        else:
            raise FormatUnavailable(
                f"{slug}: format {format!r} unavailable; have {sorted(entry.formats)}"
            )
    return Dataset(slug, fmt, mirror=mirror, offline=offline)


load_dataset = load  # datasets-muscle-memory alias

__all__ = [
    "load", "load_dataset", "Dataset", "__version__",
    "RaincloudError", "UnknownSlug", "FormatUnavailable", "ArtifactNotFound",
    "ChecksumMismatch", "BuildToolingMissing", "OfflineMiss", "MissingDependency",
]
