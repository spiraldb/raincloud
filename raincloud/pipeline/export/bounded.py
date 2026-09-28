# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Every export ends: run one writer under `RAINCLOUD_EXPORT_TIMEOUT` and
`RAINCLOUD_EXPORT_MEMORY`.

A writer can fail to finish -- Vortex's dictionary layout loops without
progress on a binary value past its 1 MiB limit -- and builds run unattended,
so a writer that does not finish in time is a measured failure, not a hang.
Nor may one take the machine's memory: the box is shared, and the kernel's
out-of-memory killer ends whatever it picks -- another account's work, or the
whole build. So the parent watches an in-process writer's resident memory and
stops it at the ceiling, and the child volunteers as the kernel's first victim
(`oom_score_adj`), so a machine that runs short loses the writer, which the
parent records, rather than the build.

A sidecar writer is already a subprocess and applies the ceiling to itself
(`sidecar.SidecarExporter`). An in-process writer (`parquet@py`, `vortex@py`)
runs native code that a Python thread cannot interrupt, so it runs in a child
process instead: forked on Linux, so the child shares the parent's imports,
configuration and catalog selection and costs no interpreter start; spawned
elsewhere (macOS cannot fork a threaded process safely). The child writes into
a private directory beside the destination, and the parent moves the file into
place only after the child reports success: a child that is killed, crashes
or raises leaves nothing at the destination, and the caller's `Publication`
puts the previous file back. The writer reads its file back in the child too
(`exporters.read_back`), so the ceilings bound that read as well as the
write; compliance reads back an in-process writer's file already on disk the
same way (`read_back_bounded`). The parent keeps the store's write lock
throughout. A child also dies with its parent, so an interrupted build leaves
no writer holding the lock.
"""
from __future__ import annotations

import multiprocessing
import os
import shutil
import signal
import sys
import time
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

from raincloud.exceptions import BuildToolingMissing

from ..spec import display_path, export_memory, export_timeout
from .base import ExportResult

# Linux forks (see the module docstring); every other platform spawns.
_START_METHOD = "fork" if sys.platform.startswith("linux") else "spawn"
# How long a child that has reported its result may take to exit.
_EXIT_GRACE_SECONDS = 60.0
# How often the parent checks the child's deadline and memory.
_WATCH_SECONDS = 1.0


class ExportFailed(RuntimeError):
    """A writer ran and did not produce its file: it raised, crashed, reported a
    measured failure, or exceeded the export time limit. The message says which."""


def run_bounded(exporter, spec: dict, canonical: Path, dest: Path) -> ExportResult | None:
    """`exporter.export(spec, canonical, dest)`, ended by `export_timeout()`.

    Returns the result (None: a sidecar whose binary is absent). Raises
    ExportFailed when an in-process writer raises, dies or runs out of time --
    a sidecar reports those as a `roundtrip=False` result instead -- and
    BuildToolingMissing when an in-process writer's library is not installed.
    An in-process writer reads its file back inside the child, under the same
    ceilings; one that reports its read-back unmeasured is a bug in raincloud,
    not a verdict, and raises RuntimeError.
    """
    if getattr(exporter, "bounds_itself", False):
        return exporter.export(spec, canonical, dest)
    missing = exporter.unavailable()
    if missing is not None:
        raise BuildToolingMissing(f"{exporter.cell_id} {missing}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    work = dest.parent / f".{dest.name}.{uuid4().hex}.export"
    work.mkdir()
    written = work / dest.name
    try:
        result = _bounded_call(f"export {exporter.cell_id}", exporter.cell_id, exporter.export,
                               (spec, canonical, written))
        if result.out_path != written or not written.is_file():
            raise ExportFailed(f"{exporter.cell_id}: the writer reported {result.out_path}, "
                               f"not the file it was given")
        if result.compliance.roundtrip is None:
            raise RuntimeError(f"{exporter.cell_id}: reported its read-back unmeasured; an "
                               f"in-process writer always reads back what it wrote")
        os.replace(written, dest)
        return replace(result, out_path=dest)
    finally:
        try:
            shutil.rmtree(work)
        except FileNotFoundError:
            pass
        except OSError as exc:
            print(f"[export] could not remove {display_path(work)}: {exc}", file=sys.stderr)


def read_back_bounded(exporter, artifact: Path, canonical: Path) -> tuple[bool, str]:
    """`exporters.read_back` of a file an in-process writer made earlier, under
    the same ceilings as an export: compliance measures a file already on disk
    this way rather than encoding it again."""
    from .exporters import read_back

    missing = exporter.unavailable()
    if missing is not None:
        raise BuildToolingMissing(f"{exporter.cell_id} {missing}")
    return _bounded_call(f"read back {exporter.cell_id}", exporter.cell_id, read_back,
                         (exporter.cell_id, artifact, canonical))


def _bounded_call(name: str, cell: str, fn, args: tuple):
    """`fn(*args)` in a child process under the time and memory ceilings: its
    return value, or ExportFailed naming what ended it."""
    limit = export_timeout()
    memory = export_memory()
    context = multiprocessing.get_context(_START_METHOD)
    receiver, sender = context.Pipe(duplex=False)
    child = context.Process(target=_call_in_child, name=name, args=(sender, fn, args, os.getpid()))
    started = reported = False
    try:
        # A forked child inherits unflushed output and would print it again.
        for stream in (sys.stdout, sys.stderr):
            stream.flush()
        child.start()
        started = True
        sender.close()  # the child holds the only writer: its death reads as EOF
        _watch(cell, child, receiver, limit, memory)
        reported = True
        try:
            outcome, payload = receiver.recv()
        except EOFError:
            child.join()
            status = _exit_status(child.exitcode)
            if child.exitcode == -signal.SIGKILL:
                status += " (the kernel's out-of-memory killer, most likely)"
            raise ExportFailed(f"{cell}: the writer process {status} "
                               f"without a result") from None
        if outcome == "error":
            raise ExportFailed(f"{cell}: {payload}")
        return payload
    finally:
        if started:
            if reported:  # it is on its way out; a timed-out or interrupted one is not
                child.join(_EXIT_GRACE_SECONDS)
            if child.is_alive():
                child.kill()
            child.join()
        receiver.close()
        sender.close()


def _watch(cell: str, child, receiver, limit: float | None, memory: int | None) -> None:
    """Wait for the child's result; raise ExportFailed at the time or memory ceiling."""
    deadline = None if limit is None else time.monotonic() + limit
    while not receiver.poll(_WATCH_SECONDS):
        if deadline is not None and time.monotonic() >= deadline:
            raise ExportFailed(f"{cell}: timed out after {limit:g}s (RAINCLOUD_EXPORT_TIMEOUT)")
        resident = _resident_bytes(child.pid)
        if memory is not None and resident is not None and resident > memory:
            raise ExportFailed(f"{cell}: stopped at {resident / 2**30:.1f} GiB resident, over the "
                               f"{memory / 2**30:.1f} GiB ceiling (RAINCLOUD_EXPORT_MEMORY)")
        if not child.is_alive():
            return  # its result (or EOF) is ready to read


def _resident_bytes(pid: int) -> int | None:
    """The process's resident set size, from /proc (Linux); None elsewhere or once it is gone."""
    try:
        with open(f"/proc/{pid}/status", "rb") as status:
            for line in status:
                if line.startswith(b"VmRSS:"):
                    return int(line.split()[1]) * 1024
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        return None
    return None


def _exit_status(code: int | None) -> str:
    if code is not None and code < 0:
        try:
            return f"was killed by {signal.Signals(-code).name}"
        except ValueError:
            return f"was killed by signal {-code}"
    return f"exited with status {code}"


def _call_in_child(sender, fn, args: tuple, parent: int) -> None:
    _die_with_parent(parent)
    _volunteer_for_oom()
    try:
        result = fn(*args)
    except BaseException as exc:  # noqa: BLE001 — a native panic is this writer's failure
        text = str(exc)
        sender.send(("error", f"{type(exc).__name__}: {text}" if text else type(exc).__name__))
    else:
        sender.send(("ok", result))
    finally:
        sender.close()


def _volunteer_for_oom() -> None:
    """Linux: be the first process the kernel kills when the machine runs out
    of memory. Raising one's own score needs no privilege."""
    if not sys.platform.startswith("linux"):
        return
    try:
        with open("/proc/self/oom_score_adj", "w") as score:
            score.write("1000")
    except OSError as exc:
        print(f"[export] could not raise this writer's oom_score_adj: {exc}", file=sys.stderr)


def _die_with_parent(parent: int) -> None:
    """Linux: have the kernel kill this child if the build process dies first,
    so an orphaned writer cannot run on holding the store lock."""
    if not sys.platform.startswith("linux"):
        return
    import ctypes
    PR_SET_PDEATHSIG = 1
    ctypes.CDLL(None, use_errno=True).prctl(PR_SET_PDEATHSIG, signal.SIGKILL)
    if os.getppid() != parent:  # the parent died before the request took effect
        os._exit(1)
