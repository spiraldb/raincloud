# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""fsspec transport-only: copy a remote URL to a local temp file."""
from __future__ import annotations

from pathlib import Path

import fsspec

from .exceptions import ArtifactNotFound


def fetch(url: str, dest: Path) -> None:
    """Stream the object at `url` into `dest`. Raise ArtifactNotFound on a miss.

    `url` is any fsspec-understood URL (file://, s3://, https://, ...). The
    matching backend extra (raincloud[s3]/[http]) must be installed for
    non-local schemes; fsspec raises ImportError otherwise, which we let
    propagate as an actionable message.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with fsspec.open(url, "rb") as src, open(dest, "wb") as out:
            for chunk in iter(lambda: src.read(1024 * 1024), b""):
                out.write(chunk)
    except FileNotFoundError as e:
        if dest.exists():
            dest.unlink()
        raise ArtifactNotFound(url) from e
    except Exception:
        if dest.exists():
            dest.unlink()
        raise
