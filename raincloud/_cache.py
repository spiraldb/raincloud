# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Local artifact files: paths, sha256, and safe replacement.

The catalog is the authority for what each artifact is: its sha256 and byte
size. Bytes are checked once, when they enter a store (a mirror download here,
a publish into a shared store); after that a file at its key with the
catalog's size is the artifact.
"""
from __future__ import annotations

import hashlib
import os
import re
import shutil
import sys
import time
import uuid
from pathlib import Path

from ._registry import FORMATS
from .exceptions import ChecksumMismatch

# Format -> on-disk file extension, from `_registry.FORMATS`. `arrow` ->
# `arrow.zstd` is an Arrow IPC file whose buffers are zstd-compressed inside the
# IPC format (there is no outer zstd frame). The suffix is part of the
# native-client path contract.
EXT = {fmt: info["ext"] for fmt, info in FORMATS.items()}

# A rollback copy another process left this long ago is an orphan of a crash
# (SIGKILL, OOM) mid-publish; nothing else would ever remove it.
_STALE_BACKUP_SECONDS = 6 * 3600


def cache_root() -> Path:
    from .config import get_config
    return get_config().cache_dir


def cache_path(slug: str, fmt: str, version: int = 1) -> Path:
    # Only tests call this. The loader composes cache_dir / artifact_key(slug,
    # fmt, Entry.version) itself; artifact_key has no default version.
    from ._resolve import artifact_key
    return cache_root() / artifact_key(slug, fmt, version)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):  # 1 MiB chunks
            h.update(chunk)
    return h.hexdigest()


class Publication:
    """Keep the previous `dest` restorable until the caller accepts its replacement.

    Writers replace files by rename, never in place, so readers see the old
    file or the new one, never a torn one. This adds rollback for a failed
    write: a hard link (a copy where links are unavailable) holds the old bytes
    until `accept()`.
    """

    def __init__(self, dest: Path):
        self.dest = dest
        self.backup = None
        self.existed = False
        self.accepted = False

    def __enter__(self):
        _sweep_stale_backups(self.dest)
        if self.dest.is_file():
            self.existed = True
            self.backup = self.dest.parent / f".{self.dest.name}.{os.getpid()}-{uuid.uuid4().hex}.rollback"
            try:
                os.link(self.dest, self.backup)
            except OSError:
                try:
                    shutil.copy2(self.dest, self.backup)
                except BaseException:
                    # __exit__ never runs when __enter__ raises; a partial copy
                    # of a large file (ENOSPC) must not stay behind.
                    self.backup.unlink(missing_ok=True)
                    raise
        return self

    def accept(self):
        self.accepted = True

    def __exit__(self, *exc):
        try:
            if not self.accepted:
                if self.existed:
                    os.replace(self.backup, self.dest)
                else:
                    self.dest.unlink(missing_ok=True)
        finally:
            # Always drop the backup name. When the old file was never replaced,
            # backup and dest are links to one inode and rename() is a no-op.
            if self.backup is not None:
                self.backup.unlink(missing_ok=True)


def _sweep_stale_backups(dest: Path) -> None:
    """Remove rollback copies of `dest` that a crashed publisher left behind.

    Only another process's, and only once stale: this process may hold a live
    one (a pending publication). Age is the link's ctime -- a hard link shares
    the old file's mtime, which says nothing about when the link was made.
    """
    pattern = re.compile(rf"\.{re.escape(dest.name)}\.(?:(\d+)-)?[0-9a-f]{{32}}\.rollback")
    cutoff = time.time() - _STALE_BACKUP_SECONDS
    try:
        siblings = list(dest.parent.iterdir())
    except FileNotFoundError:
        return
    for path in siblings:
        found = pattern.fullmatch(path.name)
        if not found or found.group(1) == str(os.getpid()):
            continue
        try:
            if path.lstat().st_ctime < cutoff:
                path.unlink()
                print(f"[publish] removed a stale rollback copy left by a crashed publisher: {path}",
                      file=sys.stderr)
        except FileNotFoundError:
            pass
        except OSError as exc:
            # Housekeeping: another account's orphan in a sticky-bit directory
            # is not ours to remove, and must not fail this publication.
            print(f"[publish] could not remove a stale rollback copy {path}: {exc}", file=sys.stderr)


def adopt(tmp: Path, dest: Path, expected_sha256: str | None, *,
          expected_size: int | None = None, slug: str | None = None) -> Path:
    """Move downloaded `tmp` to `dest` if it is the artifact the catalog names.

    The catalog's sha256 decides; with no sha recorded, its byte size does.
    Anything else is refused with ChecksumMismatch and `dest` is left as it was.
    """
    try:
        label = slug or dest.name
        if expected_sha256 is not None:
            actual = sha256_file(tmp)
            if actual != expected_sha256:
                raise ChecksumMismatch(f"{label}: downloaded sha256 {actual} is not the catalog's {expected_sha256}")
        elif expected_size is not None and tmp.stat().st_size != expected_size:
            raise ChecksumMismatch(f"{label}: downloaded {tmp.stat().st_size} bytes, the catalog says {expected_size}")
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp, dest)  # atomic within a filesystem
        return dest
    finally:
        if tmp.exists():
            tmp.unlink()
