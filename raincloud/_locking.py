# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Process locks released by the OS on exit, and same-filesystem atomic writes."""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path

# Seconds to wait quietly before saying what we are blocked on. A build holds
# the store lock for hours, so waiting is normal -- waiting SILENTLY is what
# makes it indistinguishable from a deadlock.
_ANNOUNCE_AFTER = 10.0


@dataclass
class _Entry:
    """In-process ownership of one lock file.

    `flock` is per open file description, so a second `open()` of the same path
    in this process is a DIFFERENT description and blocks against our own first
    one -- a self-deadlock with no timeout and no message. The re-entrant lock
    below makes the in-process case explicit: one thread may re-enter, another
    waits here rather than at `flock`, and the file lock is taken once by the
    outermost holder and released by it.
    """
    rlock: threading.RLock = field(default_factory=threading.RLock)
    depth: int = 0
    stream: object = None
    # Threads inside `locked()` for this path, holding or waiting. The entry is
    # dropped when the last leaves, so uniquely named locks (build leases, one
    # per build) do not accumulate in a long-lived process.
    users: int = 0


_registry_guard = threading.Lock()
_registry: dict[str, _Entry] = {}


def _reset_after_fork() -> None:
    """Drop inherited ownership in a forked child.

    `fork` copies this registry, so without this a child sees `depth > 0` and
    concludes it already holds a lock its PARENT holds -- it then skips `flock`
    entirely and runs concurrently with the holder. The inherited descriptors go
    with the discarded entries; closing them in the child does not release the
    parent's lock, which is held by the parent's own descriptor.
    """
    _registry.clear()


if hasattr(os, "register_at_fork"):  # POSIX only
    os.register_at_fork(after_in_child=_reset_after_fork)


def _acquire_file_lock(stream, path: Path, timeout: float | None) -> None:
    deadline = None if timeout is None else time.monotonic() + timeout
    announced = False
    started = time.monotonic()
    if os.name == "nt":
        import msvcrt
        # msvcrt locks a byte range, and byte 0 must exist to be locked. Only an
        # EMPTY file gets one: a holder always wrote it, and writing into a range
        # another process has locked fails (is_held probes held files).
        if os.fstat(stream.fileno()).st_size == 0:
            stream.write(b"\0")
            stream.flush()
        stream.seek(0)
        blocked_error: type[BaseException] = PermissionError
        def attempt():
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        blocked_error = BlockingIOError
        def attempt():
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    while True:
        try:
            attempt()
            if announced:
                print(f"[lock] acquired {path} after {time.monotonic() - started:.0f}s",
                      file=sys.stderr)
            return
        except blocked_error:
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                raise TimeoutError(
                    f"could not acquire {path} within {timeout:.0f}s; another "
                    f"process holds it (a build holds the store lock for the whole build)"
                ) from None
            if not announced and now - started >= _ANNOUNCE_AFTER:
                announced = True
                print(f"[lock] waiting for {path} (held by another process)", file=sys.stderr)
            time.sleep(0.1)


def _release_file_lock(stream) -> None:
    if os.name == "nt":
        import msvcrt
        stream.seek(0)
        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
    else:
        import fcntl
        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


@contextmanager
def locked(path: Path, *, timeout: float | None = None):
    """Exclusive lock on `path`, re-entrant within this process.

    `timeout` (seconds) turns an unbounded wait into a `TimeoutError`, whether
    the holder is another process or another thread of this one; `timeout=0`
    only tries. The default keeps the blocking behaviour, since a legitimate
    wait can be hours. Either way the wait is announced after ~10s.

    Lock files are permanent: unlinking one while others may open it would
    split concurrent lockers across two files. The exception is a lock file
    only its holder ever takes, under a name nobody else creates -- a build
    lease (`catalogs.Context.pinned`): its holder removes it after releasing
    it, and `catalogs.gc`, which serialises lease creation under the catalog
    lock, removes one whose holder died.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    # Keyed on the resolved path: `data/.lock` and `outputs-symlink/.lock` are
    # one file, and a second open() of it here would flock against ourselves.
    key = os.path.realpath(path)
    with _registry_guard:
        entry = _registry.setdefault(key, _Entry())
        entry.users += 1
    try:
        started = time.monotonic()
        if not entry.rlock.acquire(timeout=-1 if timeout is None else timeout):
            raise TimeoutError(f"could not acquire {path} within {timeout:.0f}s; "
                               f"another thread of this process holds it")
        try:
            if entry.depth == 0:
                stream = path.open("a+b")
                try:
                    remaining = None if timeout is None else max(0.0, timeout - (time.monotonic() - started))
                    _acquire_file_lock(stream, path, remaining)
                except BaseException:
                    stream.close()
                    raise
                entry.stream = stream
            entry.depth += 1
            try:
                yield
            finally:
                entry.depth -= 1
                if entry.depth == 0:
                    stream = entry.stream
                    entry.stream = None
                    try:
                        _release_file_lock(stream)
                    finally:
                        stream.close()
        finally:
            entry.rlock.release()
    finally:
        with _registry_guard:
            entry.users -= 1
            # Identity, not key: after a fork the registry was cleared and a new
            # entry may hold this key.
            if entry.users == 0 and _registry.get(key) is entry:
                del _registry[key]


def is_held(path: Path) -> bool:
    """Whether some holder -- this process or another -- has `path` locked now.

    A probe, not an acquisition: an unheld lock is released again at once. An
    absent file is unheld.
    """
    with _registry_guard:
        entry = _registry.get(os.path.realpath(path))
    if entry is not None and entry.depth > 0:
        return True
    try:
        # Read-only: a probe never writes, and another account's lock file is
        # usually not ours to write. One we cannot open at all may be held.
        stream = path.open("rb")
    except FileNotFoundError:
        return False
    except PermissionError:
        return True
    with stream:
        if os.name == "nt" and os.fstat(stream.fileno()).st_size == 0:
            return False  # a holder writes the byte it locks
        try:
            _acquire_file_lock(stream, path, 0)
        except (TimeoutError, PermissionError):
            return True
        _release_file_lock(stream)
        return False


def _umask_by_toggle() -> int:
    mask = os.umask(0)
    os.umask(mask)
    return mask


# Where /proc is unavailable (macOS, Windows) the umask can only be read by
# setting it, a process-global toggle that another thread creating a file at
# that moment would see as 0. Read it once here, at import, instead.
_IMPORT_UMASK = None if sys.platform.startswith("linux") else _umask_by_toggle()


def creation_mode(base: int = 0o666) -> int:
    """`base` narrowed by the process umask, as an ordinary open() or mkdir() would be.

    mkstemp/mkdtemp create 0600/0700 whatever the umask says, so anything
    published through them -- a catalog revision, its latest.json pointer --
    became unreadable to every other user of a shared machine. Linux reports
    the live umask in /proc without changing it; elsewhere the umask read at
    import applies.
    """
    try:
        with open("/proc/self/status") as status:
            for line in status:
                if line.startswith("Umask:"):
                    return base & ~int(line.split()[1], 8)
    except (OSError, ValueError):
        pass
    return base & ~(_umask_by_toggle() if _IMPORT_UMASK is None else _IMPORT_UMASK)


def atomic_write(path: Path, content: bytes, *, mode: int | None = None):
    """Replace `path` with `content` by rename; `mode` defaults to creation_mode()."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        # Windows has os.fchmod only from Python 3.13, and there mode bits other
        # than read-only mean nothing; mkstemp's file is already writable.
        if hasattr(os, "fchmod"):
            os.fchmod(fd, creation_mode() if mode is None else mode)
        with os.fdopen(fd, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)

