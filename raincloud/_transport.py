# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""fsspec transport-only: copy a remote URL to a local temp file."""
from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import urlsplit
from urllib.request import url2pathname

from .config import redact_url
from .exceptions import ArtifactNotFound, MirrorUnavailable, MissingDependency

# URL scheme -> the raincloud extra that installs its fsspec backend.
_BACKEND_EXTRAS = {"s3": "s3", "s3a": "s3", "http": "http", "https": "http"}


def filesystem_url(url: str) -> str:
    """Decode file URIs once; fsspec expects a native path for local files."""
    if url.startswith("file:"):
        parts = urlsplit(url)
        if parts.netloc not in {"", "localhost"} or parts.query or parts.fragment:
            raise ValueError("file URLs must name a local path without host, query or fragment")
        return url2pathname(parts.path)
    return url


def is_local(url: str) -> bool:
    """Whether `url` names this machine's filesystem (a file: URL or a plain path)."""
    return url.startswith("file:") or "://" not in url


# Anything URL-shaped in an error message. A backend may requote the URL it was
# given, so its query need not match ours byte for byte.
_URL = re.compile(r"[A-Za-z][A-Za-z0-9+.-]*://[^\s'\"<>]+")


class _LocalWriteError(Exception):
    """Writing the local copy failed: this machine's problem, not the mirror's."""

    def __init__(self, error: OSError):
        super().__init__(str(error))
        self.error = error


def _scrub(text: str, url: str) -> str:
    """`text` with `url`'s credentials, query and fragment removed wherever they appear."""
    text = _URL.sub(lambda found: redact_url(found.group()), text.replace(url, redact_url(url)))
    parts = urlsplit(url)
    secrets = [parts.query, parts.fragment]
    if "@" in parts.netloc:
        secrets.append(parts.netloc.rsplit("@", 1)[0])
    for secret in filter(None, secrets):
        text = text.replace(secret, "<redacted>")
    return text


def fetch(url: str, dest: Path) -> None:
    """Stream the object at `url` into `dest`. Raise ArtifactNotFound on a miss.

    `url` is any fsspec-understood URL (file://, s3://, https://, ...). A
    missing backend raises MissingDependency naming the extra that installs it;
    any other failure to reach or read the source raises MirrorUnavailable.
    Messages never carry the URL's credentials or query string, and the raw
    error is not chained, because its traceback would print them.

    A failure to write `dest` (an unwritable cache, a full disk) is this
    machine's, not the mirror's: it raises the OSError itself. Reported as an
    unreachable mirror, a build=True load would answer it by building on the
    same full disk.
    """
    import fsspec

    dest.parent.mkdir(parents=True, exist_ok=True)

    def local(operation, *args):
        try:
            return operation(*args)
        except OSError as exc:
            raise _LocalWriteError(exc) from None

    try:
        out = local(open, dest, "wb")
        try:
            with fsspec.open(filesystem_url(url), "rb") as src:
                for chunk in iter(lambda: src.read(1024 * 1024), b""):
                    local(out.write, chunk)
        finally:
            local(out.close)
    except _LocalWriteError as exc:
        dest.unlink(missing_ok=True)
        error = exc.error
        if error.filename is None and error.errno is not None:
            # A failed write names no file; say which one.
            raise OSError(error.errno, error.strerror, str(dest)) from None
        raise error from None
    except FileNotFoundError as exc:
        dest.unlink(missing_ok=True)
        # fsspec's HTTP backend reports every failed probe -- connection refused,
        # a 500 -- as FileNotFoundError, with the real failure as its cause. Only
        # a genuine miss is a miss; anything else means the mirror is unreachable.
        cause = exc.__cause__
        if cause is not None and not isinstance(cause, FileNotFoundError) and getattr(cause, "status", None) != 404:
            raise MirrorUnavailable(
                f"cannot fetch {redact_url(url)}: {type(cause).__name__}: {_scrub(str(cause), url)}") from None
        raise ArtifactNotFound(redact_url(url)) from None
    except ImportError:
        dest.unlink(missing_ok=True)
        scheme = urlsplit(url).scheme
        extra = _BACKEND_EXTRAS.get(scheme)
        how = f"install `raincloud[{extra}]`" if extra else f"install an fsspec backend for {scheme}://"
        raise MissingDependency(f"the mirror {redact_url(url)} needs a {scheme}:// transport; {how}") from None
    except Exception as exc:
        dest.unlink(missing_ok=True)
        raise MirrorUnavailable(
            f"cannot fetch {redact_url(url)}: {type(exc).__name__}: {_scrub(str(exc), url)}") from None
