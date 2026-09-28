# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The build record: what this install built, beside what the catalog says.

The catalog is shared -- tracked in git, released as fixed bundles -- and names
the file each dataset should be. An upstream can drift, so a local build may
make different bytes. That is a fact about this install, not about the catalog,
so it is recorded here, in `<data_dir>/builds.json`, and the loader consults it
for this install only. Nothing a build does changes the catalog; a maintainer
changes that on purpose, by regenerating the snapshot and committing it.

Each entry is keyed by artifact key (`v2/<slug>/<fmt>/<slug>.<ext>`) and names
the recipe it was built from, so a build of an older recipe is not served as
the current one. An entry is a file (`sha256`, `bytes`, `writer`; an export
also says whether its writer read it back and found the canonical,
`verified`, and when not, why, `verify_note`), or, when the planned writer
could not make the file, an `unavailable` measurement (the writer cell, its
error, the toolchain, the canonical it read and when) in place of the file's
checksum and size.
"""
from __future__ import annotations

import json
import warnings
from datetime import datetime, timezone
from pathlib import Path

FILENAME = "builds.json"


def record_path(data_dir: Path) -> Path:
    return Path(data_dir) / FILENAME


def read(data_dir: Path) -> dict[str, dict]:
    """Every recorded build under `data_dir`, keyed by artifact key."""
    path = record_path(data_dir)
    try:
        document = json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as exc:
        warnings.warn(f"ignoring unreadable build record {path}: {exc}", RuntimeWarning, stacklevel=2)
        return {}
    artifacts = document.get("artifacts") if isinstance(document, dict) else None
    return artifacts if isinstance(artifacts, dict) else {}


def lookup(data_dir: Path, key: str) -> dict | None:
    entry = read(data_dir).get(key)
    return entry if isinstance(entry, dict) else None


def serves(data_dir: Path, key: str, size: int, recipe: str | None) -> dict | None:
    """The build record entry that makes a local file at `key` this install's
    artifact: same size as recorded, built from the dataset's current recipe."""
    entry = lookup(data_dir, key)
    if entry is None or recipe is None or entry.get("bytes") != size or entry.get("recipe") != recipe:
        return None
    return entry


def measured_unavailable(data_dir: Path, key: str, recipe: str | None,
                         catalog_measurement: dict | None) -> dict | None:
    """The measurement saying the file at `key` cannot be made at `recipe`, or None.

    This install's record decides when it has an entry for the current recipe:
    its own failed attempt (an "unavailable" entry), or a successful build,
    which makes the format available here whatever the catalog measured.
    Otherwise the catalog's measurement stands.
    """
    return unavailable_at(lookup(data_dir, key), recipe, catalog_measurement)


def unavailable_at(entry: dict | None, recipe: str | None, catalog_measurement: dict | None) -> dict | None:
    """`measured_unavailable`'s rule, given the build record `entry` already read."""
    if isinstance(entry, dict) and recipe is not None and entry.get("recipe") == recipe:
        measured = entry.get("unavailable")
        return measured if isinstance(measured, dict) else None
    return catalog_measurement


def unverified_at(entry: dict | None, recipe: str | None, size: int | None,
                  catalog_note: str | None) -> str | None:
    """Why the file at a key was published without its writer verifying that
    it reads back, or None: this install's build record when it describes the
    file there (its size, the current recipe), else the catalog's note."""
    if (isinstance(entry, dict) and recipe is not None and entry.get("recipe") == recipe
            and entry.get("sha256") and size is not None and entry.get("bytes") == size):
        if entry.get("verified") is False:
            return entry.get("verify_note") or "its writer did not verify it"
        return None
    return catalog_note


def _write(path: Path, artifacts: dict) -> None:
    from ._locking import atomic_write
    atomic_write(path, (json.dumps({"format_version": 1, "artifacts": dict(sorted(artifacts.items()))},
                                   indent=2) + "\n").encode())


def forget(data_dir: Path, key: str) -> None:
    """Drop `key`'s entry: the file there is no longer this install's build
    (a mirror download of the catalog's bytes replaced it)."""
    from ._locking import locked

    path = record_path(data_dir)
    with locked(path.with_name(f".{FILENAME}.lock")):
        artifacts = read(data_dir)
        if artifacts.pop(key, None) is None:
            return
        _write(path, artifacts)


def record(data_dir: Path, entries: dict[str, dict]) -> Path:
    """Add or replace `entries` ({key: {sha256, bytes, writer, recipe}}, or
    {writer, recipe, canonical_sha256, unavailable} for a measured failure)."""
    from ._locking import locked

    path = record_path(data_dir)
    built_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    # Read-modify-write: two builds recording at once would each drop the
    # other's entries without the lock.
    with locked(path.with_name(f".{FILENAME}.lock")):
        artifacts = read(data_dir)
        for key, entry in entries.items():
            artifacts[key] = {**entry, "built_at": built_at}
        _write(path, artifacts)
    return path
