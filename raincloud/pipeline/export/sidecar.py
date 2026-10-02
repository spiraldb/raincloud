# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Sidecar exporter — the Python side of a stable reference-writer subprocess CLI.

A *sidecar* cell (`parquet@rs`, `parquet@java`, `parquet@hardwood`, `vortex@rs`,
`vortex@jni`) delegates the actual write to an external reference-writer binary
— a Rust or Java encoder whose source lives under `sidecars/` and which is built
and installed separately. raincloud owns the *interface*: it PATH-discovers the
binary, runs it if present, and records the verdict. It never auto-installs.

A missing binary is not a failure: the export priority skips a writer that is
not installed, and a cell named outright is skipped with a note. A sidecar that
RAN and failed (non-zero exit, bad report, `roundtrip=false`, a timeout) is a
measured failure: its output is never promoted, and when the priority chose it
for a build, the build records the format unavailable (`run_exporters`).

The sidecar CLI contract (the stable interface a real Rust/Java writer must
implement)::

    <binary> --input <canonical> --output <dest> --report <report.json>

- ``--input``  : path to the canonical artifact — a standard **Apache Arrow IPC
                 *file*** (``ARROW1`` magic; ``<slug>.arrow.zstd``). The ``zstd``
                 is IPC-INTERNAL record-batch-body compression per the IPC spec —
                 NOT an outer zstd wrapper (despite the ``.zstd`` extension), so
                 read it with a compression-enabled IPC *file* reader (e.g.
                 arrow-rs ``arrow_ipc::reader::FileReader`` with the ``zstd``
                 feature); do NOT ``zstd``-decompress the whole file first. This
                 is the sidecar's source of truth.
- ``--output`` : path the sidecar MUST write the format artifact to (raincloud
                 hands it a temp path and atomically promotes it unless the
                 report says ``roundtrip`` is false).
- ``--report`` : path the sidecar MUST write a JSON verdict to::

                     {"roundtrip": true | false | null, "variant_faithful": bool, "note": str}

                 ``roundtrip``        — whether the artifact re-reads back to the
                                        canonical. ``false`` is a measured
                                        failure: the output is never promoted.
                                        ``null`` means UNMEASURED (the sidecar's
                                        comparator could not decide, e.g. a type
                                        it cannot compare): the output IS
                                        promoted, recorded with
                                        ``Compliance(roundtrip=None)``, and
                                        compliance backfills the verdict from the
                                        cell's own self-read. The key is
                                        required; a report without it is a bad
                                        report.
                 ``variant_faithful`` — VARIANT columns survived faithfully.
                 ``note``             — free-form context (may be "").

- Exit code    : ``0`` means the sidecar RAN (its verdict is in the report — the
                 verdict itself may still be a negative result); any non-zero
                 exit means it FAILED to run. The invocation is subject to
                 ``RAINCLOUD_EXPORT_TIMEOUT``, the ceiling on every export (a
                 timeout → a measured ``fail``, never a hang).

raincloud supplies ``nbytes`` + ``sha256`` from the written ``--output`` itself,
so the sidecar need not report them.

Environment: the sidecar inherits raincloud's, and reads the row-group knobs
(``RAINCLOUD_ROW_GROUP_*``) from it. A recipe's ``write.row_group_size_rows``
wins over ``RAINCLOUD_ROW_GROUP_MAX_ROWS`` in every lane, so when the recipe
declares one it is passed as that variable in the child's environment
(``spec.row_group_cap``).

Discovery precedence: ``$RAINCLOUD_SIDECAR_<CELL>`` (cell_id upper-cased with
``@`` / ``-`` mapped to ``_`` — e.g. ``parquet@java`` -> ``RAINCLOUD_SIDECAR_PARQUET_JAVA``)
overrides ``shutil.which(<binary>)``. If neither resolves, the cell is skipped
(``export`` returns ``None``).

On-disk layout: like every writer of its format, a sidecar publishes the
dataset's one ``<fmt>/<slug>.<ext>`` file; which writer made it is recorded in the
build record, not in the path. Compliance runs writers side by side in scratch
(``compliance.compliance_path``), never over that file.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from raincloud._cache import EXT, sha256_file
from raincloud._registry import SIDECAR_EXPORTERS

from ..spec import display_path, output_format_dir, row_group_cap, spec_field
from . import register
from .base import Compliance, ExportResult, slug_from_canonical
from .bounded import export_timeout
from .exporters import tmp_path


def _env_var(cell_id: str) -> str:
    """Env-var override name for a cell: `parquet@rs` -> `RAINCLOUD_SIDECAR_PARQUET_RS`.

    NB: both `@` and `-` map to `_` (env-var names can't contain `-`), so a sidecar
    impl segment SHOULD be `[a-z0-9]+` only — otherwise `parquet@a-b` and a
    hypothetical `parquet@a_b` would collide on the same var. No registered
    cell today has a separator in its impl; enforce/uniquify if the impl vocab grows.
    """
    suffix = cell_id.upper().replace("@", "_").replace("-", "_")
    return f"RAINCLOUD_SIDECAR_{suffix}"


def _read_report(path: Path) -> tuple[bool | None, bool, str] | None:
    """Parse a sidecar report JSON. Return `(roundtrip, variant_faithful, note)`
    or `None` if the file is missing / not JSON / malformed. `roundtrip` is None
    when the sidecar's comparator could not decide (unmeasured, not a failure);
    a report that leaves the key out is malformed, not unmeasured."""
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or "roundtrip" not in data:
        return None
    roundtrip = data.get("roundtrip")
    variant_faithful = data.get("variant_faithful")
    note = data.get("note", "")
    if not (roundtrip is None or isinstance(roundtrip, bool)) or not isinstance(variant_faithful, bool):
        return None
    if not isinstance(note, str):
        return None
    return roundtrip, variant_faithful, note


class SidecarExporter:
    """An `Exporter` that delegates the write to an external reference-writer.

    See the module docstring for the sidecar CLI contract. This cell PATH-
    discovers its binary, runs it over the canonical, and records the verdict;
    it skips (`export` -> `None`) when the binary is absent and returns a
    MEASURED failure (no raise, nothing promoted) on a non-zero exit, a bad
    report, a missing output or a reported `roundtrip=false`. A reported
    `roundtrip=null` is promoted and returned as unmeasured.
    """

    # A subprocess already: it applies the export time limit itself, rather
    # than running in the child process `bounded.run_bounded` gives an
    # in-process writer.
    bounds_itself = True

    def __init__(
        self, cell_id: str, format_id: str, binary: str, ext: str | None = None
    ) -> None:
        self.cell_id = cell_id
        self.format_id = format_id
        self.binary = binary
        self.ext = ext or EXT[format_id]

    def unavailable(self) -> str | None:
        if self._discover() is not None:
            return None
        return f"needs `{self.binary}` on PATH (or ${_env_var(self.cell_id)})"

    def out_path(self, slug: str) -> Path:
        return output_format_dir(slug, self.format_id) / f"{slug}.{self.ext}"

    def toolchain(self) -> dict[str, str]:
        """What identifies the writer that ran: the binary, by name and content
        hash (a sidecar reports no version of its own)."""
        exe = self._discover()
        found = shutil.which(exe) if exe else None
        return {"sidecar": self.binary,
                **({"sidecar_sha256": sha256_file(Path(found))[:16]} if found else {})}

    def _discover(self) -> str | None:
        """Resolve the reference-writer executable, or `None` if absent."""
        return os.environ.get(_env_var(self.cell_id)) or shutil.which(self.binary)

    @staticmethod
    def _child_env(spec: dict) -> dict[str, str]:
        """raincloud's environment, with the recipe's row cap when it declares one."""
        env = dict(os.environ)
        if spec_field(spec, "write.row_group_size_rows"):
            env["RAINCLOUD_ROW_GROUP_MAX_ROWS"] = str(row_group_cap(spec))
        return env

    def _failure(self, dest: Path, reason: str, *, variant_faithful: bool = False) -> ExportResult:
        """A MEASURED failure verdict — roundtrip=False, no artifact promoted."""
        return ExportResult(
            format_id=self.cell_id,
            out_path=dest,
            nbytes=0,
            sha256="",
            compliance=Compliance(
                roundtrip=False,
                variant_faithful=variant_faithful,
                note=f"sidecar {self.cell_id}: {reason}",
            ),
        )

    def export(self, spec: dict, canonical: Path, dest: Path | None = None) -> ExportResult | None:
        exe = self._discover()
        if exe is None:
            # Skip: binary absent. The caller (run_exporters) logs the skip note.
            return None

        # The canonical's own slug (from its path) is the authority for
        # artifact placement — not spec["slug"] (see ParquetExporter).
        slug = slug_from_canonical(canonical)
        dest = dest or self.out_path(slug)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = tmp_path(dest)

        print(f"[export:{self.cell_id}] {display_path(dest)}")
        limit = export_timeout()
        try:
            with tempfile.TemporaryDirectory() as td:
                report = Path(td) / "report.json"
                try:
                    proc = subprocess.run(
                        [
                            exe,
                            "--input", str(canonical),
                            "--output", str(tmp),
                            "--report", str(report),
                        ],
                        check=False,
                        timeout=limit,
                        env=self._child_env(spec),
                    )
                except subprocess.TimeoutExpired:
                    # A deadlocked / blocked reference-writer must degrade to a
                    # measured failure, never hang the step.
                    return self._failure(dest, f"timed out after {limit:g}s (RAINCLOUD_EXPORT_TIMEOUT)")
                except OSError as e:
                    # Discovery yielded a path (env override or `which`) but it's not
                    # launchable — moved / renamed / mistyped / not executable. A
                    # set-but-broken sidecar is a MEASURED failure (the operator
                    # explicitly pointed at it), NOT a silent skip.
                    return self._failure(dest, f"failed to launch: {e}")

                if proc.returncode != 0:
                    return self._failure(dest, f"exit {proc.returncode}")

                parsed = _read_report(report)
                if parsed is None:
                    return self._failure(dest, "bad report")

                if not tmp.exists():
                    return self._failure(dest, f"no output: {parsed[2]}" if parsed[2] else "no output")

                roundtrip, variant_faithful, note = parsed
                if roundtrip is False:
                    # Its own verdict says the file is wrong: keep the old one.
                    return self._failure(dest, note or "reported roundtrip=false",
                                         variant_faithful=variant_faithful)
                tmp.replace(dest)
        finally:
            tmp.unlink(missing_ok=True)

        return ExportResult(
            format_id=self.cell_id,
            out_path=dest,
            nbytes=dest.stat().st_size,
            sha256=sha256_file(dest),
            compliance=Compliance(
                roundtrip=roundtrip, variant_faithful=variant_faithful, note=note
            ),
        )


# Register the reference-writer cells. A build runs one only when it is the first
# installed writer in the spec's priority, or named by `--format`; named on a
# machine without the binary, it skips-with-note. Parquet has DISTINCT encoders — parquet@py
# (in-process, pyarrow/cpp), parquet@rs (arrow-rs, the Rust sidecar in
# `sidecars/rust/`), parquet@java (Apache reference), parquet@hardwood
# (Hardwood, an independent pure-Java Parquet implementation) — while vortex is ONE encoder (the Rust core) reached through
# byte-identical bindings, registered here as vortex@rs / vortex@jni (their
# matrix value is read-conformance, but they are registrable write cells).
# One class serves every sidecar cell, so the cells are pure data. They are
# declared in `raincloud._registry.SIDECAR_EXPORTERS` — the same place the loader
# reads them from to answer "can this catalog be built here?" — and instantiated
# from it here, rather than written out again as five near-identical calls.
for _cell, (_format_id, _binary) in SIDECAR_EXPORTERS.items():
    register(SidecarExporter(_cell, _format_id, _binary))
