# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Read-conformance machinery — does an implementation correctly READ an
artifact back to the canonical Arrow?

This is the READ twin of the exporter seam. Where an `Exporter` *writes* a
format from the canonical, a `Reader` *reads* a produced artifact and states a
`Verdict` (pass/fail/na/skip/spec_ambiguous) on whether it round-trips to the
canonical Arrow. Two flavours, mirroring the exporters:

- **In-process readers (REAL):** `PyarrowParquetReader` (`parquet@py`) reads a
  parquet with pyarrow; `VortexPyReader` (`vortex@py`) reads a vortex file with
  the Vortex Python binding. Both compare the result to the canonical Arrow and
  return a genuine verdict — so a pure-Python machine gets a real (partial)
  matrix. A read error is caught and returned as a measured ``fail``; it NEVER
  escapes. Both stream (`stream_verdict`): a file and its canonical are
  compared window by window, never read whole, which is what lets the
  in-process WRITERS read back every file they write (`exporters`).
- **`SidecarReader` (subprocess):** delegates the read to an external
  reference-reader binary (Rust / JVM, source under `sidecars/`, built and
  installed separately). raincloud owns the *interface* — it PATH-discovers the
  binary, runs it if present, and skips-with-note (``skip`` verdict) if absent.
  It never auto-installs, and a reader that fails to run is a measured ``fail``.

The reader-sidecar CLI contract (the stable interface a real reference-reader
must implement)::

    <binary> --input <artifact> --canonical <canonical> --report <report.json>

- ``--input``     : path to the format artifact to read (a parquet / vortex /
                    ... file raincloud produced).
- ``--canonical`` : path to the canonical artifact — a standard **Apache Arrow
                    IPC *file*** (``ARROW1`` magic; ``<slug>.arrow.zstd``). The
                    ``zstd`` is IPC-INTERNAL record-batch-body compression per the
                    IPC spec — NOT an outer zstd wrapper (despite the ``.zstd``
                    extension), so read it with a compression-enabled IPC *file*
                    reader (e.g. arrow-rs ``arrow_ipc::reader::FileReader`` with
                    the ``zstd`` feature), do NOT ``zstd``-decompress the whole
                    file first. This is the source of truth the read is compared to.
- ``--report``    : path the reader MUST write a JSON verdict to::

                        {"status": "pass"|"fail"|"na"|"skip"|"spec_ambiguous",
                         "note": str, "detail": str}

                    ``note`` / ``detail`` are optional (default ``""``).

- Comparison      : to match the in-process readers' verdicts (`stream_verdict`
                    below), a reference-reader MUST compare with **LOGICAL** equality,
                    not strict byte/type equality: (1) compare after a LOSSLESS cast
                    to the canonical schema — ``string`` / ``string_view`` /
                    ``large_string`` (and dictionary-encoded equivalents) are the
                    SAME logical value → ``pass``; (2) IGNORE schema/field metadata
                    — a VARIANT column arrives as its shredded ``struct<metadata,
                    value>`` with the ``arrow.parquet.variant`` marker possibly
                    dropped; the DATA round-trip is still ``pass`` (annotation loss
                    is a write-side `variant_faithful=False`, not a read fail).
                    Reserve ``spec_ambiguous`` for a disagreement that traces to an
                    UNDERDEFINED format-spec point (not a plain mismatch).
- Exit code       : ``0`` means the reader RAN (its verdict is in the report —
                    the verdict itself may be a negative result); any non-zero
                    exit means it FAILED to run -> a measured ``fail``.
                    The invocation is subject to ``RAINCLOUD_SIDECAR_TIMEOUT``
                    (a timeout → a measured ``fail``, never a hang).

Discovery precedence: ``$RAINCLOUD_READER_<CELL>`` (reader_id upper-cased with
``@`` / ``-`` mapped to ``_`` — e.g. ``parquet@hardwood`` ->
``RAINCLOUD_READER_PARQUET_HARDWOOD``) overrides ``shutil.which(<binary>)``. If
neither resolves, the reader is skipped (``skip`` verdict).

The reader registry mirrors the exporter one (`export/__init__`): a dict keyed
by `reader_id`, `register_reader` / `get_reader` / `all_readers`, dup-raises.
`run_reader` is the dispatch — it runs a reader only when its `formats` include
the artifact's bare format, else returns ``na``.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Protocol, runtime_checkable

import pyarrow as pa
import pyarrow.parquet as pq

from ..discovery import has_variant
from ..spec import sidecar_timeout
from .base import VERDICT_STATUSES, ReadResult, Verdict
from .compare import stream_equal

# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


# Decoded bytes one Parquet read batch is planned to hold, and a cap on its
# rows, as the Rust sidecars' `READ_BATCH_BYTES` / `READ_BATCH_ROWS`: a batch
# sized in rows alone can hold gigabytes when the rows are large (code-contests:
# ~1.4 MiB a row). The plan sees only each row group's average row.
_READ_BATCH_BYTES = 256 << 20
_READ_BATCH_ROWS = 65_536


def _read_plan(metadata) -> list[tuple[list[int], int]]:
    """(row groups, batch rows) segments that read a Parquet file within
    `_READ_BATCH_BYTES` decoded bytes a batch -- `read_plan` in the Rust
    sidecar. Consecutive small groups share a batch size; a group over the
    budget is read alone, in batches of as many of its average rows as fit."""
    plan: list[tuple[list[int], int]] = []
    packed: list[int] = []
    packed_rows = packed_bytes = 0
    for index in range(metadata.num_row_groups):
        group = metadata.row_group(index)
        rows, nbytes = group.num_rows, max(group.total_byte_size, 0)
        if not rows:
            continue
        if nbytes <= _READ_BATCH_BYTES and rows <= _READ_BATCH_ROWS:
            if packed and (packed_bytes + nbytes > _READ_BATCH_BYTES or packed_rows + rows > _READ_BATCH_ROWS):
                plan.append((packed, packed_rows))
                packed, packed_rows, packed_bytes = [], 0, 0
            packed.append(index)
            packed_rows += rows
            packed_bytes += nbytes
            continue
        if packed:
            plan.append((packed, packed_rows))
            packed, packed_rows, packed_bytes = [], 0, 0
        fit = _READ_BATCH_BYTES * rows // max(nbytes, 1)
        plan.append(([index], max(1, min(fit, rows, _READ_BATCH_ROWS))))
    if packed:
        plan.append((packed, packed_rows))
    return plan


def parquet_batches(artifact: Path):
    """(schema, rows, batches) of a Parquet file read with pyarrow, the batches
    sized by `_read_plan` so none holds more than about one budget of rows."""
    parquet = pq.ParquetFile(artifact)

    def batches():
        for groups, batch_rows in _read_plan(parquet.metadata):
            yield from parquet.iter_batches(batch_size=batch_rows, row_groups=groups)
    return parquet.schema_arrow, parquet.metadata.num_rows, batches()


def vortex_batches(artifact: Path):
    """(schema, rows, batches) of a Vortex file read with the Vortex Python
    binding, in the batches its scan yields (one per split of its layout),
    without the segment cache: each segment is read once."""
    import vortex

    file = vortex.open(str(artifact), without_segment_cache=True)
    reader = file.to_arrow()
    return reader.schema, len(file), iter(reader)


def orc_batches(artifact: Path):
    """(schema, rows, batches) of an ORC file read with pyarrow (the Apache ORC
    C++ library), one stripe at a time."""
    import pyarrow.orc as orc

    file = orc.ORCFile(str(artifact))
    return file.schema, file.nrows, (file.read_stripe(i) for i in range(file.nstripes))


@contextmanager
def canonical_batches(canonical: Path):
    """(schema, rows, batches) of the canonical Arrow IPC file, one stored batch
    at a time. The row count comes from the batches' metadata; none is
    decompressed to count it."""
    import pyarrow.dataset as ds

    rows = ds.dataset(str(canonical), format="ipc").count_rows()
    with pa.ipc.open_file(str(canonical)) as reader:
        yield reader.schema, rows, (reader.get_batch(i) for i in range(reader.num_record_batches))


def pass_note(reader_id: str, canonical_schema: pa.Schema) -> str:
    """The note of a `pass`: the reader, and whether a VARIANT column was
    compared as its shredded struct."""
    return f"{reader_id}: round-trips to canonical" + (
        " (VARIANT column present — annotation not preserved on read; "
        "data compared as its shredded struct)" if has_variant(canonical_schema) else "")


def stream_verdict(reader_id: str, got, canonical: Path) -> Verdict:
    """Compare a reader's `got` = (schema, rows or None, batches) to the
    canonical, streamed (`compare.stream_equal`): neither side is read whole.

    Comparison is deliberately tolerant of lossless type normalization: a reader
    may return `string`/`string_view`/`large_string` or a differently-encoded
    equivalent of the canonical data (e.g. Vortex round-trips `string` ->
    `string_view`). Row count (when the reader knows it up front) and column
    names are checked first, then values window by window, with reversible
    normalization to the canonical type and bit-exact floats at every nesting
    level. Field metadata is not compared, so the known VARIANT marker drop does
    not fail a round-trip; VARIANT columns survive as their shredded struct,
    which is a `pass` on the DATA round-trip (the write side records
    `variant_faithful=False`).
    """
    got_schema, got_rows, got_batches = got
    with canonical_batches(canonical) as (schema, rows, batches):
        if got_rows is not None and got_rows != rows:
            return Verdict("fail", note=f"{reader_id}: row count {got_rows:,} != canonical {rows:,}")
        equal, detail = stream_equal(got_schema, got_batches, schema, batches)
    if equal:
        return Verdict("pass", note=pass_note(reader_id, schema))
    if detail.startswith("row count"):
        return Verdict("fail", note=f"{reader_id}: {detail}")
    if detail.startswith("column names differ"):
        return Verdict("fail", note=f"{reader_id}: column names differ", detail=detail)
    return Verdict("fail", note=f"{reader_id}: data mismatch vs canonical", detail=detail)


def _roundtrip_verdict(reader_id: str, got: pa.Table, expected: pa.Table) -> Verdict:
    """`stream_verdict`'s comparison over two tables already in memory."""
    if got.num_rows != expected.num_rows:
        return Verdict("fail", note=f"{reader_id}: row count {got.num_rows:,} != canonical {expected.num_rows:,}")
    equal, detail = stream_equal(got.schema, got.to_batches(), expected.schema, expected.to_batches())
    if equal:
        return Verdict("pass", note=pass_note(reader_id, expected.schema))
    if detail.startswith("column names differ"):
        return Verdict("fail", note=f"{reader_id}: column names differ", detail=detail)
    return Verdict("fail", note=f"{reader_id}: data mismatch vs canonical", detail=detail)


def _guarded(reader_id: str, fn):
    """Run `fn` (a read+compare), turning ANY error into a measured `fail`.

    Catches `BaseException` (not just `Exception`) so a native reader's Rust
    panic — `pyo3_runtime.PanicException` subclasses `BaseException` — degrades
    to a `fail` verdict instead of escaping. `KeyboardInterrupt` / `SystemExit`
    re-raise so Ctrl-C still works. This is the read-side analogue of
    `build._run_one`'s BaseException guard.
    """
    try:
        return fn()
    except (KeyboardInterrupt, SystemExit):
        raise
    except BaseException as e:  # noqa: BLE001
        return Verdict(
            "fail",
            note=f"{reader_id}: read error",
            detail=f"{type(e).__name__}: {e}",
        )


# ---------------------------------------------------------------------------
# Reader protocol + in-process readers
# ---------------------------------------------------------------------------


@runtime_checkable
class Reader(Protocol):
    """Reads a produced artifact and verdicts its round-trip to the canonical.

    `reader_id` names the reading implementation (e.g. `parquet@py`,
    `vortex@jni`); `formats` is the set of BARE formats it can read
    (`{"parquet"}`, `{"vortex"}`). `read_conformance` must never raise — a read
    error is returned as a `fail` verdict (in-process) or a measured `fail`
    (sidecar), and an absent sidecar binary as `skip`.
    """

    reader_id: str
    formats: set[str]

    def read_conformance(self, artifact: Path, canonical: Path) -> Verdict:
        ...


class PyarrowParquetReader:
    """In-process `parquet@py` reader — pyarrow's batches vs the canonical's, streamed."""

    reader_id = "parquet@py"
    formats = {"parquet"}

    def read_conformance(self, artifact: Path, canonical: Path) -> Verdict:
        def _run() -> Verdict:
            return stream_verdict(self.reader_id, parquet_batches(artifact), canonical)

        return _guarded(self.reader_id, _run)


class VortexPyReader:
    """In-process `vortex@py` reader — `vortex.open(...).to_arrow()` batches vs the canonical's, streamed."""

    reader_id = "vortex@py"
    formats = {"vortex"}

    def read_conformance(self, artifact: Path, canonical: Path) -> Verdict:
        def _run() -> Verdict:
            return stream_verdict(self.reader_id, vortex_batches(artifact), canonical)

        return _guarded(self.reader_id, _run)


class PyarrowOrcReader:
    """In-process `orc@py` reader — pyarrow's ORC stripes vs the canonical's batches, streamed."""

    reader_id = "orc@py"
    formats = {"orc"}

    def read_conformance(self, artifact: Path, canonical: Path) -> Verdict:
        def _run() -> Verdict:
            return stream_verdict(self.reader_id, orc_batches(artifact), canonical)

        return _guarded(self.reader_id, _run)


# ---------------------------------------------------------------------------
# Sidecar reader (subprocess CLI)
# ---------------------------------------------------------------------------


def _reader_env_var(reader_id: str) -> str:
    """Env-var override name: `parquet@hardwood` -> `RAINCLOUD_READER_PARQUET_HARDWOOD`.

    Both `@` and `-` map to `_` (env-var names can't contain either), matching
    the sidecar EXPORTER convention (`sidecar._env_var`). A hypothetical
    `vortex@a_b` would collide with `vortex@a-b`; no registered reader has
    that clash, and the impl vocab is controlled.
    """
    suffix = reader_id.upper().replace("@", "_").replace("-", "_")
    return f"RAINCLOUD_READER_{suffix}"


def _read_verdict_report(path: Path) -> Verdict | None:
    """Parse a reader report JSON into a `Verdict`, or `None` if malformed.

    NB: `toolchain_absent` is deliberately NOT readable from the report. By the
    time we parse one, the sidecar has RUN — so its `skip` is a measured verdict
    (unsupported type, comparator gap), never the benign never-invoked case. A
    sidecar must not be able to mark its own verdict un-comparable.
    """
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    status = data.get("status")
    note = data.get("note", "")
    detail = data.get("detail", "")
    if status not in VERDICT_STATUSES:
        return None
    if not isinstance(note, str) or not isinstance(detail, str):
        return None
    return Verdict(status, note=note, detail=detail)


class SidecarReader:
    """A `Reader` that delegates the read to an external reference-reader binary.

    See the module docstring for the reader-sidecar CLI contract. PATH-discovers
    its binary; ``skip`` verdict when absent; a MEASURED ``fail`` (never a raise)
    on an unlaunchable path / non-zero exit / missing or malformed report. Reuses
    the sidecar-EXPORTER OSError-guard idiom.
    """

    def __init__(self, reader_id: str, formats: set[str], binary: str) -> None:
        from raincloud._registry import SIDECAR_HELPERS
        self.reader_id = reader_id
        self.formats = set(formats)
        self.binary = binary
        self.helper = SIDECAR_HELPERS.get(reader_id)

    def _discover(self) -> str | None:
        return os.environ.get(_reader_env_var(self.reader_id)) or shutil.which(self.binary)

    def read_conformance(self, artifact: Path, canonical: Path) -> Verdict:
        from .sidecar import helper_path
        exe = self._discover()
        if exe is None or (self.helper is not None and helper_path(self.helper) is None):
            # The ONLY benign skip: the reader was never invoked, so it says
            # nothing about the artifact. Everything past this point RAN, so any
            # `skip` it yields is a measured verdict — `_read_verdict_report`
            # deliberately refuses to let a report set `toolchain_absent`.
            return Verdict(
                "skip",
                note=f"{self.reader_id}: reference-reader binary absent",
                toolchain_absent=True,
            )
        with tempfile.TemporaryDirectory() as td:
            report = Path(td) / "report.json"
            try:
                proc = subprocess.run(
                    [
                        exe,
                        "--input", str(artifact),
                        "--canonical", str(canonical),
                        "--report", str(report),
                    ],
                    check=False,
                    timeout=sidecar_timeout(),
                )
            except subprocess.TimeoutExpired:
                # A deadlocked / blocked reference-reader degrades to a
                # measured failure, never hangs the step.
                return Verdict("fail", note=f"{self.reader_id}: timed out")
            except OSError as e:
                # Discovery yielded a path but it's not launchable (moved /
                # renamed / mistyped / not executable). A set-but-broken reader
                # is a MEASURED failure, NOT a silent skip and NEVER a hard-fail.
                return Verdict(
                    "fail", note=f"{self.reader_id}: failed to launch: {e}"
                )
            if proc.returncode != 0:
                return Verdict(
                    "fail", note=f"{self.reader_id}: exit {proc.returncode}"
                )
            verdict = _read_verdict_report(report)
            if verdict is None:
                return Verdict("fail", note=f"{self.reader_id}: bad report")
            return verdict


# ---------------------------------------------------------------------------
# Reader registry (mirrors the exporter registry in export/__init__)
# ---------------------------------------------------------------------------

_READERS: dict[str, Reader] = {}


def register_reader(reader: Reader) -> None:
    """Register `reader` under its `reader_id`; raise on a duplicate id."""
    rid = reader.reader_id
    if rid in _READERS:
        raise ValueError(f"reader already registered for reader_id {rid!r}")
    _READERS[rid] = reader


def get_reader(reader_id: str) -> Reader:
    """Return the reader for `reader_id`, or raise KeyError if none is registered."""
    try:
        return _READERS[reader_id]
    except KeyError:
        raise KeyError(f"no reader registered for reader_id {reader_id!r}") from None


def all_readers() -> list[Reader]:
    """Every registered reader, in registration order."""
    return list(_READERS.values())


def run_reader(
    reader: Reader, artifact_cell: str, artifact: Path, canonical: Path
) -> ReadResult:
    """Dispatch one reader over one artifact -> a `ReadResult`.

    A reader whose `formats` do NOT include the artifact's bare format (the part
    before ``@`` in `artifact_cell`) is not applicable -> ``na``. Otherwise the
    reader's `read_conformance` runs; it never raises (in-process readers guard;
    the sidecar guards + measures), but `run_reader` still wraps it defensively.
    """
    bare_format = artifact_cell.split("@", 1)[0]
    if bare_format not in reader.formats:
        return ReadResult(
            artifact_cell,
            reader.reader_id,
            Verdict("na", note=f"{reader.reader_id} does not read {bare_format!r}"),
        )
    verdict = _guarded(reader.reader_id, lambda: reader.read_conformance(artifact, canonical))
    return ReadResult(artifact_cell, reader.reader_id, verdict)


# Register the built-in readers. In-process (REAL, always available) first, then
# the opt-in reference-reader sidecars — absent binaries simply skip-with-note.
# The Rust reference readers (parquet@rs via arrow-rs, vortex@rs via the Vortex
# core) live in `sidecars/rust/`. The JVM lanes ship under `sidecars/java/`:
# `parquet@java` (via parquet-arrow-java + parquet-java), `parquet@hardwood`
# (via Hardwood) and `vortex@jni` (via vortex-jni).
register_reader(PyarrowParquetReader())
register_reader(VortexPyReader())
register_reader(PyarrowOrcReader())
register_reader(SidecarReader("parquet@rs", {"parquet"}, "raincloud-read-parquet-rs"))
register_reader(SidecarReader("vortex@rs", {"vortex"}, "raincloud-read-vortex-rs"))
register_reader(SidecarReader("parquet@java", {"parquet"}, "raincloud-read-parquet-java"))
register_reader(SidecarReader("parquet@hardwood", {"parquet"}, "raincloud-read-parquet-hardwood"))
register_reader(SidecarReader("vortex@jni", {"vortex"}, "raincloud-read-vortex-jni"))
register_reader(SidecarReader("orc@rs", {"orc"}, "raincloud-read-orc-rs"))
register_reader(SidecarReader("avro@rs", {"avro"}, "raincloud-read-avro-rs"))
register_reader(SidecarReader("avro@java", {"avro"}, "raincloud-read-avro-java"))
register_reader(SidecarReader("nimble@cpp", {"nimble"}, "raincloud-read-nimble-cpp"))
