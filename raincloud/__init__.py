# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Raincloud loader: datasets-style access to prepared Vortex/Parquet files."""
from __future__ import annotations

import re
from contextlib import ExitStack, contextmanager
from pathlib import Path

from . import _resolve
from ._cache import EXT
from ._catalog import load_catalog
from ._catalog import unverified as _catalog_unverified
from ._duckdb import duckdb_connect
from ._formats import auto_formats, select_format
from ._readers import open_batches, open_dataset, reader_capabilities, require_reader
from ._registry import FORMATS
from .config import Config, get_config, resolve_config
from .exceptions import (  # noqa: F401
    ArtifactNotFound,
    BuildFailed,
    BuildToolingMissing,
    CatalogConflict,
    CatalogError,
    ChecksumMismatch,
    CorruptArtifact,
    FormatUnavailable,
    HydratedDatasetWarning,
    MirrorUnavailable,
    MissingDependency,
    MissingRevision,
    OfflineMiss,
    RaincloudError,
    UnknownColumn,
    UnknownSlug,
    UnsupportedType,
)

# THE release version. `pyproject.toml` reads this file via `[tool.hatch.version]`
# and `clients/java/build.gradle.kts` reads it directly. `clients/rust/Cargo.toml`
# and `CITATION.cff` are hand-bumped copies: bump them with this literal, and
# tests/test_loader_package.py::test_version_mirrors_agree fails if they disagree.
__version__ = "0.3.1"

_DEFAULT_FORMAT = "auto"
# Spellings people type for a format, suggested (never silently substituted).
_FORMAT_ALIASES = {"pq": "parquet", "ipc": "arrow", "feather": "arrow", "vx": "vortex"}


def _settings(config: Config | str | Path | None) -> Config:
    """A Config from what the public entry points accept: a Config, a TOML path, or None."""
    if isinstance(config, Config):
        return config
    return resolve_config(config=config) if config else get_config()


# How vortex-data reports an operating-system failure: its Rust io::Error,
# surfaced as a RuntimeError ("Io: Permission denied (os error 13)").
_VORTEX_OS_ERROR = re.compile(r"Io: (.*) \(os error (\d+)\)")


@contextmanager
def _decoding(path, fmt: str):
    """Classify a reader failure on a file that is present.

    - NotImplementedError (incl. ArrowNotImplementedError): a type this reader
      cannot represent -> UnsupportedType.
    - an OSError carrying an errno (permission denied, a vanished file) is about
      access, not the bytes, and propagates as itself; a decoder's own OSError
      has none -> CorruptArtifact.
    - pyarrow's ArrowInvalid -> CorruptArtifact.
    - vortex: vortex-data raises every Rust-side error as a plain RuntimeError.
      One naming an OS error number is an access failure and is re-raised as
      that OSError (PermissionError, FileNotFoundError, ...); any other means
      the bytes are bad -> CorruptArtifact.

    Anything else propagates unclassified.
    """
    import pyarrow as pa

    def corrupt(exc):
        return CorruptArtifact(f"{path} could not be read as {fmt} ({type(exc).__name__}: {exc}); "
                               f"re-fetch or rebuild it")
    try:
        yield
    except RaincloudError:
        raise
    except NotImplementedError as exc:
        raise UnsupportedType(str(exc)) from exc
    except OSError as exc:
        if exc.errno is not None:
            raise
        raise corrupt(exc) from exc
    except pa.ArrowInvalid as exc:
        raise corrupt(exc) from exc
    except RuntimeError as exc:
        if fmt != "vortex":
            raise
        system = _VORTEX_OS_ERROR.fullmatch(str(exc))
        if system is not None:
            raise OSError(int(system.group(2)), system.group(1), str(path)) from exc
        raise corrupt(exc) from exc


def _check_columns(schema, columns, fmt: str, slug: str) -> None:
    """Refuse a column the file does not have, the same way for every format.

    Parquet also takes nested struct paths ("nested.x"), which pyarrow reads
    natively; pyarrow silently drops an unknown one, so they are checked here.
    """
    for column in columns:
        if column in schema.names:
            continue
        parts = column.split(".")
        if fmt == "parquet" and len(parts) > 1 and parts[0] in schema.names:
            kind = schema.field(parts[0]).type
            for part in parts[1:]:
                if not (hasattr(kind, "get_field_index") and kind.get_field_index(part) >= 0):
                    break
                kind = kind.field(part).type
            else:
                continue
        nested = ("; nested paths like 'a.b' are read from parquet only"
                  if len(parts) > 1 and fmt != "parquet" else "")
        shown = ", ".join(schema.names[:12]) + (", ..." if len(schema.names) > 12 else "")
        raise UnknownColumn(f"{slug} ({fmt}) has no column {column!r}{nested}; it has: {shown}")


class Dataset:
    """Lazy handle to a prepared artifact. Nothing is fetched until you ask."""

    def __init__(self, slug: str, fmt: str, *, mirror: str | None,
                 offline: bool | None, entry=None, config: Config | None = None, build: bool = False,
                 context=None):
        self.config = config or get_config()
        self.slug = slug
        self.format = fmt
        self._build = build
        self._mirror = mirror
        self._offline = offline
        # load() passes the Entry and the catalog Context it resolved together;
        # direct construction looks both up, so a build still pins the catalog.
        if entry is None:
            catalog = load_catalog(self.config)
            entry, context = catalog.entry(slug), catalog.context
        self._entry = entry
        self._context = context

    def __repr__(self) -> str:
        return f"Dataset(slug={self.slug!r}, format={self.format!r})"

    @property
    def catalog_source(self) -> str | None:
        """Where the catalog came from: a revision id, a bundle directory,
        `checkout`, `bundled` or `local`. Select it (`catalog=`) to reopen the
        same catalog; compare catalog_revision to detect that it changed."""
        return self._context.source if self._context is not None else None

    @property
    def catalog_id(self) -> str | None:
        return self._entry.catalog_id

    @property
    def catalog_revision(self) -> str | None:
        return self._entry.revision

    @property
    def recipe_fingerprint(self) -> str | None:
        return self._entry.recipe

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
        """Return a location, not a read lease; publications can replace it."""
        return self.path_for(self.format)

    def path_for(self, fmt: str) -> Path:
        """Resolve a representation location; it may be replaced after return."""
        # Reuse the already-resolved Entry (covers every format of this slug)
        # so resolve() doesn't rebuild it a third time.
        return _resolve.resolve(self.slug, fmt, mirror=self._mirror,
                                offline=self._offline, allow_build=self._build, entry=self._entry, config=self.config, context=self._context)

    def local_paths(self) -> dict[str, Path]:
        """The formats already prepared on this machine, and where; fetches nothing."""
        found = {fmt: _resolve.prepared(self._entry, fmt, self.config)[0] for fmt in self._entry.formats}
        return {fmt: path for fmt, path in sorted(found.items()) if path is not None}

    # --- materialization ---
    @property
    def artifacts(self) -> list[dict]:
        """Catalog-declared files; listing does not probe or fetch artifacts."""
        return [{"format": fmt, "writer": info.writer,
                 "key": _resolve.artifact_key(self.slug, fmt, self._entry.version),
                 "sha256": info.sha256, "bytes": info.nbytes,
                 "schema_version": self._entry.version}
                for fmt, info in sorted(self._entry.formats.items())]

    @contextmanager
    def _batches(self, *, batch_size: int = 65536, columns: list[str] | None = None):
        """Yield a native batch iterator; use ``with ds.batches() as batches``.

        Closing the context releases readers even after early termination. IPC
        decompression still operates on the file's original record-batch size;
        batch_size bounds returned batches, not a producer's encoded batch.
        """
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")
        fmt = self.format
        require_reader(fmt)
        import pyarrow as pa
        with ExitStack() as stack:
            def open_reader(path):
                schema, native = open_batches(
                    fmt, path, stack, columns=columns, batch_size=batch_size,
                    check_columns=lambda found: _check_columns(found, columns, fmt, self.slug))

                def chunks():
                    with _decoding(path, fmt):
                        for batch in native:
                            for start in range(0, batch.num_rows, batch_size):
                                yield batch.slice(start, batch_size)
                return schema, chunks()
            schema, iterator = self._acquire(self.format, open_reader)
            stack.callback(iterator.close)
            with pa.RecordBatchReader.from_batches(schema, iterator) as reader:
                yield reader

    def _acquire(self, fmt, opener):
        # Openers keep the opened file, not its path: publication replaces a
        # file by rename, so a reader keeps the whole generation it opened.
        fmt = select_format(self._entry.formats, fmt)
        path = self.path_for(fmt)
        with _decoding(path, fmt):
            return opener(path)

    @contextmanager
    def batches(self, *, batch_size: int = 65536, columns: list[str] | None = None):
        """Read native PyArrow batches within an explicit close scope.

        Use ``with ds.batches() as batches``. batch_size bounds returned rows;
        decompression can still require a larger producer-encoded IPC/Vortex chunk.
        A column the file lacks raises UnknownColumn; undecodable bytes raise
        CorruptArtifact; a type this reader cannot represent raises UnsupportedType.
        """
        def checked(iterator):
            while True:
                try:
                    batch = next(iterator)
                except StopIteration:
                    return
                except NotImplementedError as exc:
                    raise UnsupportedType(str(exc)) from exc
                yield batch

        with ExitStack() as stack:
            try:
                iterator = iter(stack.enter_context(self._batches(batch_size=batch_size, columns=columns)))
            except NotImplementedError as exc:
                raise UnsupportedType(str(exc)) from exc
            batches = checked(iterator)
            try:
                yield batches
            finally:
                batches.close()
                if hasattr(iterator, "close"):
                    iterator.close()

    def to_arrow(self):
        """Explicitly materialize the entire dataset as a PyArrow Table."""
        if self.format == "parquet":
            # One native read: faster than batches + read_all, and fewer chunks.
            import pyarrow as pa
            import pyarrow.parquet as pq

            def read(path):
                with pa.OSFile(str(path), "r") as source, pq.ParquetFile(source) as reader:
                    return reader.read()
            return self._acquire("parquet", read)
        try:
            with self._batches() as reader:
                return reader.read_all()
        except NotImplementedError as exc:
            raise UnsupportedType(str(exc)) from exc

    def to_vortex(self):
        require_reader("vortex")
        import vortex
        return self._acquire("vortex", lambda path: vortex.open(str(path)))

    @property
    def schema(self):
        with self._batches() as reader:
            return reader.schema

    def dataset(self):
        """A lazy pyarrow Dataset over the loaded format, for whichever engine you use.

        DuckDB (``raincloud.duckdb_connect().sql("select ... from d")``), Polars
        (``polars.scan_pyarrow_dataset(d)``) and pyarrow (``d.to_table(filter=...)``)
        scan it with projection and filter pushdown. It reads the file generation
        that was open when it was created, even if a publish later replaces it.

        Parquet VARIANT columns arrive as their shredded struct here; DuckDB
        decodes them natively when given the file:
        ``raincloud.duckdb_connect().read_parquet(str(ds.path()))``.
        """
        fmt = self.format
        require_reader(fmt)
        return self._acquire(fmt, lambda path: open_dataset(fmt, path))

    def to_pandas(self):
        try:
            import pandas  # noqa: F401
        except ImportError as e:
            raise MissingDependency(
                "to_pandas() needs pandas — install `raincloud[pandas]`"
            ) from e
        return self.to_arrow().to_pandas()


def _choose_format(entry, requested: str, readable: bool, readers: set[str] | None = None, *,
                   config: Config | None = None, build: bool = False) -> str:
    """The format `requested` resolves to for `entry`.

    With `readable`, "auto" skips formats this install cannot read and an
    explicit format must be readable here; without it (the CLI, which hands a
    path to native readers that bring their own) every recorded format counts.
    `readers`, when given, is the set "auto" chooses among instead.

    "auto" tries the install's `auto_formats`: the formats it builds (only
    Vortex by default), in vortex, parquet order, then the canonical Arrow.
    "auto" also skips a format a build measured unavailable at this recipe
    (`_resolve.measured_unavailable`); asking for one outright raises
    FormatUnavailable quoting that measurement, unless `build` allows a new
    attempt.
    """
    from ._suggest import hint

    fmt = requested.lower() if isinstance(requested, str) else requested
    known = {"auto", *EXT}
    if isinstance(fmt, str) and "@" not in fmt and fmt not in known:
        raise FormatUnavailable(hint(requested, [*known, *_FORMAT_ALIASES], noun="format",
                                     canonical=_FORMAT_ALIASES,
                                     everything=f"Formats are {', '.join(sorted(known))}."))
    formats = entry.formats
    config = config or get_config()
    measured = {key: m for key in formats
                if (m := _resolve.measured_unavailable(entry, key, config)) is not None}
    # The catalog records no file for it, so nothing could be served or fetched.
    # (One it does record may still be on a mirror; resolution decides then.)
    if fmt in measured and not build and formats[fmt].nbytes is None:
        raise _resolve.unavailable_error(entry, fmt, measured[fmt])
    if fmt == "auto":
        formats = {key: value for key, value in formats.items() if key not in measured}
    if fmt == "auto" and readers is not None:
        formats = {key: value for key, value in formats.items() if key in readers}
    if fmt == "auto" and readable:
        available = reader_capabilities()
        usable = {key: value for key, value in formats.items() if available[key]["available"]}
        if formats and not usable:
            try:
                for other in sorted(formats):
                    require_reader(other, import_native=False)
            except MissingDependency as exc:
                raise MissingDependency(f"{entry.slug} is prepared only as {', '.join(sorted(formats))}: {exc}") from None
        formats = usable
    try:
        fmt = select_format(formats, fmt, auto_formats(config, entry.version))
    except FormatUnavailable as exc:
        raise FormatUnavailable(f"{entry.slug}: {exc}") from None
    # A format raincloud reads in-process must be readable here; one it only
    # serves by path (`_registry.FORMATS` declares no reader) loads for its
    # `path()`, and its readers raise MissingDependency saying so.
    if readable and FORMATS[fmt]["reader"] is not None:
        require_reader(fmt, import_native=False)
    return fmt


def _open(slug: str, *, format: str, offline: bool | None, mirror: str | None,
          config: Config | str | Path | None, build: bool, readable: bool, stacklevel: int,
          readers: set[str] | None = None, retry_errors: bool = False) -> Dataset:
    settings = _settings(config)
    if retry_errors:
        # A build setting: the child build reads it from its pinned settings.
        from dataclasses import replace
        settings, build = replace(settings, retry_errors=True), True
    catalog = load_catalog(settings)
    entry = catalog.entry(slug)  # raises UnknownSlug
    fmt = _choose_format(entry, format, readable, readers, config=settings, build=build)
    if entry.info.get("derived_from") and entry.info.get("hydrated_columns") is not None:
        import warnings
        fetched = ", ".join(target.get("into") for target in entry.info["hydrated_columns"].values())
        warnings.warn(HydratedDatasetWarning(
            f"{slug} is a hydrated dataset ({fetched} fetched from the open web: large, time-dependent, "
            f"not an upstream file; `raincloud describe {slug}` explains). "
            f"You probably want {entry.info['derived_from']!r}."), stacklevel=stacklevel + 1)
    return Dataset(slug, fmt, mirror=mirror, offline=offline, entry=entry, config=settings, build=build,
                   context=catalog.context)


def load(slug: str, *, format: str = _DEFAULT_FORMAT,
         offline: bool | None = None, mirror: str | None = None,
         config: Config | str | Path | None = None, build: bool = False,
         retry_errors: bool = False) -> Dataset:
    """Return a lazy Dataset handle for `slug`.

    Automatic selection prefers Vortex, Parquet, then canonical IPC, among the
    formats this install can read. Explicit format requests never substitute
    another representation. Reads never build unless build=True; catalog
    metadata access never fetches artifact bytes.

    A build does not repeat a failure already measured: a format whose writer,
    with this toolchain, failed to write it at the recipe is skipped, and
    asking for it raises FormatUnavailable quoting the measurement.
    `retry_errors=True` allows a build (it implies `build=True`) that attempts
    it anyway.
    """
    return _open(slug, format=format, offline=offline, mirror=mirror, config=config, build=build,
                 readable=True, stacklevel=2, retry_errors=retry_errors)


def slugs(*, config: Config | str | Path | None = None) -> list[str]:
    """Every dataset name in the selected catalog, sorted.

    The supported way to find out what is loadable. `examples/use_loader.py`
    reached into `raincloud._catalog.load_catalog` to do this, which meant the
    documented front door taught a private import — and anything users copy from
    an example becomes public by usage whatever the underscore says.
    """
    return load_catalog(_settings(config)).slugs()


def _unverified(entry, fmt: str, data_dir: Path) -> str | None:
    return _catalog_unverified(entry, fmt, data_dir, Path(data_dir) / _resolve.artifact_key(entry.slug, fmt, entry.version))


def describe(slug: str, *, config: Config | str | Path | None = None) -> dict:
    """What the catalog records about `slug`, without reading any artifact.

    Rows, column metadata, the formats available with their recorded size and
    checksum, and the catalog revision the answer came from. A format a build
    measured unavailable at this recipe carries that measurement as
    `unavailable` (writer cell, error, toolchain, measured_at): this install's
    build record when it has one for the recipe, else the catalog's. Raises
    `UnknownSlug` if the catalog does not have it.
    """
    settings = _settings(config)
    entry = load_catalog(settings).entry(slug)
    info = entry.info or {}
    return {
        "slug": entry.slug,
        "name": info.get("full_name") or info.get("short_name"),
        "description": info.get("description"),
        "license": (info.get("license") or {}).get("spdx"),
        "source_url": info.get("source_url"),
        "derived_from": info.get("derived_from"),
        "hydrated_columns": info.get("hydrated_columns"),
        "advisory": info.get("advisory"),
        "rows": entry.rows,
        "columns": list(entry.columns),
        "formats": {
            fmt: {"sha256": info.sha256, "bytes": info.nbytes, "writer": info.writer,
                  **({"unavailable": measured}
                     if (measured := _resolve.measured_unavailable(entry, fmt, settings)) else {}),
                  # A file published without its writer verifying it reads back.
                  **({"unverified": why} if (why := _unverified(entry, fmt, settings.data_dir)) else {})}
            for fmt, info in entry.formats.items()
        },
        "schema_version": entry.version,
        "catalog_id": entry.catalog_id,
        "catalog_revision": entry.revision,
    }


load_dataset = load  # datasets-muscle-memory alias

__all__ = [
    "load", "load_dataset", "slugs", "describe", "duckdb_connect",
    "reader_capabilities", "Dataset", "Config", "resolve_config", "__version__",
    "RaincloudError", "UnknownSlug", "FormatUnavailable", "ArtifactNotFound",
    "ChecksumMismatch", "BuildToolingMissing", "BuildFailed", "OfflineMiss",
    "MissingDependency", "CatalogError", "CatalogConflict", "MissingRevision",
    "UnsupportedType", "CorruptArtifact", "MirrorUnavailable", "UnknownColumn",
    "HydratedDatasetWarning",
]


def __dir__():
    # Tab completion and dir() show the API, not this module's own imports.
    return sorted(__all__)
