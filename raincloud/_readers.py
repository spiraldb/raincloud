# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""In-process format readers, and their availability without importing optional native extensions.

Which formats have a reader is declared in `_registry.FORMATS`; how each one
opens a file is here. A format with no in-process reader is still served by
path (`Dataset.path()`), for a reader the caller brings.
"""
from importlib.util import find_spec

from ._registry import FORMATS
from .exceptions import MissingDependency


def reader_capabilities() -> dict:
    """Which format readers this install has, per format; imports and fetches nothing."""
    capabilities = {}
    for fmt, info in FORMATS.items():
        module = info["reader"]
        entry = {"available": module is not None and find_spec(module) is not None,
                 "implementation": info.get("implementation")}
        if info.get("extra"):
            entry["extra"] = info["extra"]
        capabilities[fmt] = entry
    return capabilities


def _missing(fmt: str) -> str:
    info = FORMATS[fmt]
    if info["reader"] is None:
        return (f"raincloud has no in-process {fmt} reader; `Dataset.path()` gives the file "
                f"for a reader of your own")
    if fmt == "vortex":
        return "Vortex reads require raincloud[vortex] and a supported native wheel for this platform"
    extra = f"raincloud[{info['extra']}]" if info.get("extra") else info["reader"]
    return f"{fmt} reads require {extra}"


def require_reader(fmt: str, *, import_native: bool = True):
    if not reader_capabilities()[fmt]["available"]:
        raise MissingDependency(_missing(fmt))
    if not import_native:
        return
    try:
        __import__(FORMATS[fmt]["reader"])
    except (ImportError, OSError) as exc:
        raise MissingDependency(_missing(fmt)) from exc


def open_batches(fmt: str, path, stack, *, columns, batch_size: int, check_columns):
    """(schema, iterator of record batches) over `path`, closed by `stack`.

    `check_columns(schema)` refuses a requested column the file lacks before
    any batch is read; the iterator yields only `columns` when they are given.
    """
    opener = _BATCHES.get(fmt)
    if opener is None:
        raise MissingDependency(_missing(fmt))
    return opener(path, stack, columns=columns, batch_size=batch_size, check_columns=check_columns)


def open_dataset(fmt: str, path):
    """A lazy pyarrow Dataset over the open file at `path`."""
    import pyarrow as pa
    import pyarrow.dataset as pads
    if fmt == "vortex":
        import vortex
        return vortex.open(str(path)).to_dataset()
    if fmt == "parquet":
        file_format, source = pads.ParquetFileFormat(), pa.OSFile(str(path), "r")
    elif fmt == "arrow":
        file_format, source = pads.IpcFileFormat(), pa.memory_map(str(path), "r")
    else:
        raise MissingDependency(_missing(fmt))
    # A fragment over the open file, not the path: the dataset keeps reading
    # this generation for as long as it lives.
    fragment = file_format.make_fragment(source)
    return pads.FileSystemDataset([fragment], fragment.physical_schema, file_format)


def _parquet_batches(path, stack, *, columns, batch_size, check_columns):
    import pyarrow as pa
    import pyarrow.parquet as pq
    source = stack.enter_context(pa.OSFile(str(path), "r"))
    reader = stack.enter_context(pq.ParquetFile(source))
    schema = reader.schema_arrow
    if columns is not None:
        check_columns(schema)
        schema = reader.read_row_groups([], columns=columns).schema
    return schema, reader.iter_batches(batch_size=batch_size, columns=columns)


def _arrow_batches(path, stack, *, columns, batch_size, check_columns):
    import pyarrow as pa
    source = stack.enter_context(pa.memory_map(str(path), "r"))
    reader = pa.ipc.open_file(source)
    schema = reader.schema
    if columns is not None:
        check_columns(schema)
        schema = pa.schema([schema.field(c) for c in columns], metadata=schema.metadata)
    return schema, (reader.get_batch(i) if columns is None else reader.get_batch(i).select(columns)
                    for i in range(reader.num_record_batches))


def _vortex_batches(path, stack, *, columns, batch_size, check_columns):
    import vortex
    file = vortex.open(str(path))
    if columns is not None:
        check_columns(file.dtype.to_arrow_schema())
    # Projection is pushed into the scan: unrequested columns are never read.
    reader = stack.enter_context(file.to_arrow(projection=columns, batch_size=batch_size))
    return reader.schema, reader


_BATCHES = {"parquet": _parquet_batches, "arrow": _arrow_batches, "vortex": _vortex_batches}
