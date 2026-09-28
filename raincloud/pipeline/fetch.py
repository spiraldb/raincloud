# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 1 — fetch upstream sources into the recipe's raw directory.

Downloads land in `spec.raw_slug_dir(slug, recipe)`: `<raw root>/<slug>/`, or a
`.recipes/<key>/` generation under it when the fetch recipe differs from the one
that directory was first fetched for. Reads only the `fetch` block of a
DatasetSpec. Dispatches by `fetch.type`:
    - http         : urllib download(s)
    - uci          : http download using UCI's canonical data_url
    - kaggle       : kaggle.KaggleApi.dataset_download_files
    - huggingface  : huggingface_hub.snapshot_download
    - generated    : a generator group cache (`generate.fetch_generated`)
    - custom       : a helper declared in `raincloud._registry.CUSTOM_FETCHERS`

Idempotent: an http download is skipped when the cached file matches the
declared `fetch.expected_bytes` / `expected_sha256` or, with nothing declared,
the size in the receipt written when it was fetched (`--verify` also re-hashes
it against the receipt's sha256). See `_already_ok`.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import urllib.request
import uuid
from contextlib import contextmanager
from pathlib import Path

from . import spec
from .spec import display_path, fetch_deadline, load_manifest, spec_field

# Per-slug record of what a completed download actually produced. Hidden, so
# `_payload_files` keeps it out of the extract inputs.
RECEIPTS = ".fetch-receipts.json"


def user_agent() -> str:
    """The User-Agent every raincloud download sends."""
    from raincloud import __version__
    return f"raincloud/{__version__} (+https://github.com/spiraldb/raincloud)"


class FetchDeadlineExceeded(TimeoutError):
    """A download outlived RAINCLOUD_FETCH_DEADLINE. Never retried: the deadline
    spans every attempt, so a retry would start already out of time."""


def slug_dir(slug: str, recipe: dict | None = None) -> Path:
    """Create and return the raw directory for `recipe` (see `spec.raw_slug_dir`).

    Inside a catalog context, the first fetch into a directory stamps it with
    `.fetch-recipe.json`, the key `raw_slug_dir` compares against to decide
    whether cached bytes belong to this recipe. Written once only: rewriting it
    for a different recipe would claim the old bytes for the new one.
    """
    d = spec.raw_slug_dir(slug, recipe)
    d.mkdir(parents=True, exist_ok=True)
    from raincloud._bundle import digest, encode
    from raincloud._locking import atomic_write
    from raincloud.catalogs import current, selected_context
    context = current() or selected_context()
    if context is not None:
        if recipe is None:
            recipe = next((s for s in context.manifest["datasets"] if s["slug"] == slug), None)
        if recipe is not None and not (d / ".fetch-recipe.json").exists():
            key = digest(encode({"catalog_id": context.bundle.catalog_id, "fetch": recipe.get("fetch", {})}))
            atomic_write(d / ".fetch-recipe.json", encode({"fetch_recipe": key}))
    return d


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_receipts(directory: Path) -> dict[str, dict]:
    """The receipts recorded in `directory`; `{}` only when none were written.

    A receipts file that exists but cannot be read is an error, not "no
    receipts": `_already_ok` accepts a file with no receipt as one that predates
    receipts, so reading a damaged file as empty would pass every cached file
    in the directory unchecked.
    """
    path = directory / RECEIPTS
    try:
        data = json.loads(path.read_text())
    except FileNotFoundError:
        return {}
    except (OSError, ValueError) as e:
        raise ValueError(f"cannot read fetch receipts {display_path(path)}: {e}; "
                         f"delete {display_path(directory)} to refetch its downloads") from e
    if not isinstance(data, dict) or not all(isinstance(v, dict) for v in data.values()):
        raise ValueError(f"fetch receipts {display_path(path)} must map file names to objects; "
                         f"delete {display_path(directory)} to refetch its downloads")
    return data


def _write_receipt(directory: Path, name: str, url: str, path: Path,
                   sha256: str | None = None) -> None:
    """Record what this download produced, so a later run can check it.

    Most of the catalog cannot pin its inputs: it declares neither
    `expected_bytes` nor `expected_sha256`, because upstreams roll (Geofabrik
    keeps 7 days) or are unversioned. Recording what we GOT is the part that is
    always available, and it turns the cache check from "a file exists" into "a
    file matches the bytes this recipe last fetched".

    `sha256` is supplied by the caller when it was computed while streaming.
    Hashing here instead would re-read the whole file -- several of these run to
    tens of gigabytes, so that is a second full pass for nothing.
    """
    from raincloud._locking import atomic_write
    receipts = _read_receipts(directory)
    receipts[name] = {"url": url, "bytes": path.stat().st_size,
                      "sha256": sha256 if sha256 is not None else _sha256(path),
                      "fetched_at": _utcnow()}
    atomic_write(directory / RECEIPTS, (json.dumps(receipts, indent=2, sort_keys=True) + "\n").encode())


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@contextmanager
def _atomic_download(dest: Path):
    """Write `dest` via a sibling temp file, renamed only on success.

    Writing straight to the final name means an OOM-kill or power loss leaves a
    truncated file that every later run reports `[cached]`. The `except` branch
    below cannot help: the process is gone before it runs. Same pattern as
    `canonical.open_canonical_writer` and `extract._atomic_output`.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.parent / f".{dest.name}.{uuid.uuid4().hex}.part"
    try:
        with open(tmp, "wb") as f:
            yield f
        os.replace(tmp, dest)
    finally:
        tmp.unlink(missing_ok=True)


def _already_ok(path: Path, expected_bytes: int | None, expected_sha256: str | None,
                *, receipt: dict | None = None, verify: bool = False) -> bool:
    """Is the cached file at `path` the one this recipe wants?

    Declared expectations win. With none declared — the common case — fall back
    to the receipt written when the file was last fetched: its size must match,
    and with `verify` its sha256 too. Size alone is the default because hashing
    re-reads every multi-gigabyte download on every build, and a short download
    can no longer reach the final name when the response declares its length
    (`_stream` checks Content-Length and
    `_atomic_download` renames only on success). A file with no receipt and
    nothing declared is accepted: it predates receipts, and refusing it would
    re-download most of the catalog.
    """
    if not path.exists():
        return False
    if expected_bytes is not None and path.stat().st_size != expected_bytes:
        return False
    if expected_sha256:
        return _sha256(path) == expected_sha256
    if expected_bytes is not None:
        return True
    if receipt:
        if receipt.get("bytes") != path.stat().st_size:
            return False
        recorded = receipt.get("sha256")
        return not (verify and recorded) or _sha256(path) == recorded
    return True


def _payload_files(directory: Path, *, recursive: bool = False) -> list[Path]:
    """Files handed to extraction, excluding internal hidden files/directories.

    Recipe generations and Hugging Face's .cache are bookkeeping, even when
    they contain files with a dataset extension.
    """
    paths = directory.rglob("*") if recursive else directory.iterdir()
    return sorted(path for path in paths if path.is_file()
                  and not any(part.startswith(".") for part in path.relative_to(directory).parts))


def _find_sibling_cache(target_dir: Path, url: str, name: str,
                        ex_bytes: int | None, ex_sha: str | None) -> Path | None:
    """Find a sibling slug whose spec declares this exact URL and whose
    cached file is already on disk.

    Hardlinking across slugs is only safe when they share an upstream URL
    (e.g. GloVe sizes, OSM Germany kinds). Matching by basename alone would
    hand every slug whose URL ends in `data.csv` the first one's bytes, so the
    URL is matched via the manifest, and (when declared) size + sha are still
    verified as defense in depth.

    The donor's own receipt is held to what `_cached` holds a file to: one
    naming another URL means the donor's recipe was re-pointed and its bytes
    are the old URL's, and one whose size differs describes other bytes. A
    donor with a damaged receipt is skipped; one with none predates receipts
    and is accepted, as `_already_ok` accepts it.
    """
    raw_root = spec.raw_downloads_root()
    if not raw_root.exists():
        return None
    # load_manifest honors the frozen operation, or the current selection for
    # standalone calls. A process-global cache can outlive a catalog switch.
    for ds in load_manifest()["datasets"]:
        if url not in spec_field(ds, "fetch.urls", []):
            continue
        candidate_dir = spec.raw_slug_dir(ds["slug"], ds)
        if candidate_dir == target_dir:
            continue
        candidate = candidate_dir / name
        if not candidate.exists():
            continue
        try:
            receipt = _read_receipts(candidate_dir).get(name)
        except ValueError as exc:
            print(f"  [skip] sibling {display_path(candidate)}: {exc}", file=sys.stderr)
            continue
        if receipt is not None and (receipt.get("url") not in (None, url)
                                    or receipt.get("bytes") != candidate.stat().st_size):
            continue
        if ex_bytes is not None and candidate.stat().st_size != ex_bytes:
            continue
        if ex_sha and _sha256(candidate) != ex_sha:
            continue
        return candidate
    return None


def _content_length(response) -> int | None:
    """The body length the response declares, if it declares one."""
    headers = getattr(response, "headers", None)
    value = headers.get("Content-Length") if headers is not None else None
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


def _stream(req, dest: Path, *, timeout: float, deadline: float | None, started: float) -> str:
    """One download attempt of `req` into `dest`; returns the body's sha256.

    Reads with `read1`, which returns after a single socket read. `read(n)`
    blocks until n bytes arrive, so a server dripping bytes just inside the
    socket timeout would keep it from ever returning to the deadline check.
    A body shorter than its Content-Length raises: `http.client` reports that
    as a plain EOF, and committing it would pin the truncated bytes in the
    receipt. Either failure leaves nothing under `dest` (see `_atomic_download`).
    """
    digest = hashlib.sha256()
    written = 0
    with urllib.request.urlopen(req, timeout=timeout) as r, _atomic_download(dest) as w:
        # urlopen's HTTP and file:// responses both offer read1 and headers; a
        # bare file-like response is read with read() and declares no length.
        read = getattr(r, "read1", r.read)
        declared = _content_length(r)
        while chunk := read(1 << 20):
            w.write(chunk)
            digest.update(chunk)
            written += len(chunk)
            if deadline is not None and time.monotonic() - started > deadline:
                raise FetchDeadlineExceeded(
                    f"download exceeded {deadline:.0f}s (RAINCLOUD_FETCH_DEADLINE); "
                    f"set it to 0 to disable")
        if declared is not None and written != declared:
            raise ConnectionError(f"truncated download: got {written:,} of the "
                                  f"{declared:,} bytes its Content-Length declares")
    return digest.hexdigest()


def _download(url: str, dest: Path, *, expected_bytes: int | None = None,
              expected_sha256: str | None = None) -> None:
    """Download `url` to `dest` (3 attempts, one deadline) and write its receipt.

    A body that disagrees with a declared size or sha256 is removed and raises:
    the pin is the recipe's statement of what it wants, and keeping other bytes
    would only refetch them on every run.
    """
    deadline = fetch_deadline()
    started = time.monotonic()
    req = urllib.request.Request(url, headers={"User-Agent": user_agent()})
    print(f"  fetching {url} -> {display_path(dest)}")
    # Per-URL retry (transient network failures common on S3 with 100-file fetches)
    for attempt in range(3):
        # No context kwarg: urllib uses its default verifying SSL context, and
        # there is no way for a recipe to opt out of it.
        timeout = 300.0
        if deadline is not None:
            remaining = deadline - (time.monotonic() - started)
            if remaining <= 0:
                raise FetchDeadlineExceeded(
                    f"download of {url} exceeded {deadline:.0f}s across attempts "
                    f"(RAINCLOUD_FETCH_DEADLINE); set it to 0 to disable")
            # A socket timeout past the deadline would let one stalled read outlive it.
            timeout = min(timeout, remaining)
        try:
            digest = _stream(req, dest, timeout=timeout, deadline=deadline, started=started)
            break
        except Exception as e:
            # `_atomic_download` already guarantees no partial file is left
            # under the final name. This additionally drops a COMPLETE file
            # from an earlier run when a re-fetch fails, so the next attempt
            # starts clean rather than reusing bytes we just tried to replace.
            if dest.exists(): dest.unlink()
            if isinstance(e, FetchDeadlineExceeded) or attempt == 2:
                raise
            print(f"    retry {attempt + 1}/3 after {type(e).__name__}: {e}", file=sys.stderr)
    size = dest.stat().st_size
    if ((expected_bytes is not None and size != expected_bytes)
            or (expected_sha256 and digest != expected_sha256)):
        dest.unlink()
        raise ValueError(
            f"{url}: downloaded {size:,} bytes with sha256 {digest}, but the recipe "
            f"pins expected_bytes={expected_bytes} expected_sha256={expected_sha256}; "
            f"the upstream changed or the pin is stale")
    _write_receipt(dest.parent, dest.name, url, dest, digest)


def _cached(url: str, dest: Path, expected_bytes: int | None, expected_sha256: str | None,
            *, verify: bool = False) -> bool:
    """Report and return whether `dest` already holds this recipe's bytes for `url`."""
    receipt = _read_receipts(dest.parent).get(dest.name)
    # A receipt naming a DIFFERENT url means the recipe was re-pointed and the
    # cached bytes belong to the old one. That is distinct from having no
    # receipt at all (a file predating receipts), which stays acceptable -- so
    # the check lives here rather than inside `_already_ok`, where "absent" and
    # "superseded" would collapse into the same None.
    if receipt is not None and receipt.get("url") not in (None, url):
        print(f"  [refetch] {display_path(dest)} (recipe url changed)")
        return False
    if _already_ok(dest, expected_bytes, expected_sha256, receipt=receipt, verify=verify):
        print(f"  [cached] {display_path(dest)}")
        return True
    return False


def fetch_url(url: str, dest: Path, *, expected_bytes: int | None = None,
              expected_sha256: str | None = None, verify: bool = False) -> Path:
    """Fetch one URL to `dest` with the stage's guarantees: receipt-checked
    cache, atomic write, deadline, retries and truncation check. The receipt
    lives in `dest.parent`, so `dest` should be inside a raw slug directory."""
    if not _cached(url, dest, expected_bytes, expected_sha256, verify=verify):
        _download(url, dest, expected_bytes=expected_bytes, expected_sha256=expected_sha256)
    return dest


def url_filename(url: str) -> str:
    """The file name a download of `url` lands under."""
    return url.rsplit("/", 1)[-1].split("?", 1)[0] or "download.bin"


def fetch_http(spec: dict, *, verify: bool = False) -> list[Path]:
    out = []
    urls = spec_field(spec, "fetch.urls", [])
    target_dir = slug_dir(spec["slug"], spec)
    ex_bytes = spec_field(spec, "fetch.expected_bytes")
    ex_sha = spec_field(spec, "fetch.expected_sha256")
    if spec_field(spec, "fetch.verify_tls") is not None:
        # Refused, not ignored, for catalogs that skipped validate_manifest:
        # TLS verification is always on, and a shared recipe must not weaken it.
        raise ValueError(
            f"{spec['slug']}: fetch.verify_tls is no longer supported — TLS "
            f"verification is always on. Pin the payload with "
            f"fetch.expected_sha256 instead."
        )
    # Two URLs sharing a basename would write one file: the list would name it
    # twice, parse would read the last download twice, and the receipt would
    # flip between the URLs so every run refetched.
    names: dict[str, str] = {}
    for url in urls:
        prior = names.setdefault(url_filename(url), url)
        if prior != url:
            raise ValueError(f"{spec['slug']}: fetch.urls {prior} and {url} both download "
                             f"to {url_filename(url)!r}; refusing to overwrite one with the other")
    for url in urls:
        dest = target_dir / url_filename(url)
        size_hint = ex_bytes if len(urls) == 1 else None
        sha_hint = ex_sha if len(urls) == 1 else None
        if _cached(url, dest, size_hint, sha_hint, verify=verify):
            out.append(dest)
            continue
        sibling = _find_sibling_cache(target_dir, url, dest.name, size_hint, sha_hint)
        if sibling is not None:
            print(f"  [reuse] hardlink from {display_path(sibling)} -> {display_path(dest)}")
            dest.unlink(missing_ok=True)
            try:
                os.link(sibling, dest)
            except OSError:
                import shutil
                with open(sibling, "rb") as r, _atomic_download(dest) as w:
                    shutil.copyfileobj(r, w)
            _write_receipt(target_dir, dest.name, url, dest)
            out.append(dest)
            continue
        _download(url, dest, expected_bytes=size_hint, expected_sha256=sha_hint)
        out.append(dest)
    return out


def _kaggle_api():
    """An authenticated Kaggle API. The `kaggle` package authenticates when it
    is imported, so this is imported only to download."""
    try:
        import kaggle
    except ModuleNotFoundError as e:
        from raincloud._extras import missing
        raise missing(e, "fetching a Kaggle dataset", needs=("kaggle",)) from e
    api = kaggle.KaggleApi(); api.authenticate()
    return api


def fetch_kaggle(spec: dict) -> list[Path]:
    import re
    import shutil
    # Credentials only to download: a payload already on disk needs none, as
    # with every other fetch type.
    api = None
    urls = spec_field(spec, "fetch.urls", [])
    target_dir = slug_dir(spec["slug"], spec)
    needs_accept = spec_field(spec, "fetch.requires_interactive_accept", False)
    out = []
    for url in urls:
        # Expect e.g. https://www.kaggle.com/datasets/<owner>/<dataset>
        m = re.match(r"^https://www\.kaggle\.com/datasets/([^/]+)/([^/? ]+)", url)
        if not m: raise ValueError(f"not a Kaggle dataset URL: {url}")
        ref = f"{m.group(1)}/{m.group(2)}"
        # Internal recipe markers and generations do not complete a download.
        if _payload_files(target_dir):
            print(f"  [cached] {display_path(target_dir)} (payload present)")
        else:
            if needs_accept:
                print(f"  kaggle (ToS-gated): {ref} -> {display_path(target_dir)}")
            else:
                print(f"  kaggle: {ref} -> {display_path(target_dir)}")
            # The Kaggle API writes into the directory it is given with no temp
            # file, so give it a hidden sibling and move the payload in only
            # once the call returns: an interrupted download then leaves no
            # payload for the check above to trust.
            # A killed earlier attempt leaves its staging directory (possibly
            # gigabytes); nothing trusts it, and the stage holds the raw-root
            # lock, so no other download is using one.
            for stale in target_dir.glob(".kaggle-*.part"):
                print(f"  [clean] removing interrupted download {display_path(stale)}", file=sys.stderr)
                shutil.rmtree(stale)
            if api is None:
                api = _kaggle_api()
            staging = target_dir / f".kaggle-{uuid.uuid4().hex}.part"
            staging.mkdir()
            try:
                try:
                    api.dataset_download_files(ref, path=str(staging), quiet=False, unzip=False)
                except Exception as e:
                    # Kaggle returns 403 on datasets that require a one-time
                    # click-through-ToS acceptance on the web UI before API access.
                    if "403" in str(e) or "Forbidden" in str(e):
                        raise RuntimeError(_kaggle_accept_message(url, ref)) from e
                    raise
                for path in _payload_files(staging, recursive=True):
                    final = target_dir / path.relative_to(staging)
                    final.parent.mkdir(parents=True, exist_ok=True)
                    os.replace(path, final)
            finally:
                try:
                    shutil.rmtree(staging)
                except OSError as exc:
                    print(f"  [clean] could not remove {display_path(staging)}: {exc}", file=sys.stderr)
        out.extend(_payload_files(target_dir))
    return out


def _kaggle_accept_message(url: str, ref: str) -> str:
    return (
        f"Kaggle returned 403 Forbidden for dataset '{ref}'.\n"
        f"  This dataset requires a one-time click-through to accept its\n"
        f"  distribution terms before the Kaggle API will serve downloads.\n"
        f"  Resolve by visiting the dataset page in a browser (while\n"
        f"  signed in to your Kaggle account):\n\n"
        f"      {url}\n\n"
        f"  Click the 'Download' button; Kaggle records the acceptance\n"
        f"  against your account and subsequent API calls will succeed.\n"
        f"  Re-run the pipeline after accepting.\n"
        f"  (Mark the manifest entry with fetch.requires_interactive_accept\n"
        f"  = true to surface this message deterministically next time.)"
    )


def fetch_huggingface(spec: dict) -> list[Path]:
    try:
        from huggingface_hub import snapshot_download
        from huggingface_hub.errors import GatedRepoError
    except ModuleNotFoundError as e:
        from raincloud._extras import missing
        raise missing(e, "fetching a Hugging Face dataset", needs=("huggingface_hub",)) from e
    target_dir = slug_dir(spec["slug"], spec)
    allow_patterns = spec_field(spec, "fetch.hf_allow_patterns")
    revision = spec_field(spec, "fetch.hf_revision")
    needs_accept = spec_field(spec, "fetch.requires_interactive_accept", False)
    out = []
    for url in spec_field(spec, "fetch.urls", []):
        assert url.startswith("hf://"), url
        repo_id = url[len("hf://"):]
        scope = ""
        if allow_patterns:
            scope += f" allow_patterns={allow_patterns}"
        if revision:
            scope += f" revision={revision}"
        gate = " (gated)" if needs_accept else ""
        print(f"  huggingface{gate}: {repo_id} -> {display_path(target_dir)}{scope}")
        try:
            snapshot_download(
                repo_id,
                repo_type="dataset",
                local_dir=str(target_dir),
                allow_patterns=allow_patterns,
                revision=revision,
            )
        except GatedRepoError as e:
            raise RuntimeError(_hf_gate_message(repo_id, url)) from e
        except Exception as e:
            # Some HF gate denials surface as plain HTTPError 401 rather than
            # GatedRepoError. Detect and route to the same message so the
            # remediation hint is consistent.
            msg = str(e)
            if "401" in msg or "GatedRepoError" in type(e).__name__:
                raise RuntimeError(_hf_gate_message(repo_id, url)) from e
            raise
        out.extend(_payload_files(target_dir, recursive=True))
    return out


def _hf_gate_message(repo_id: str, url: str) -> str:
    return (
        f"Hugging Face returned 401/Gated for dataset '{repo_id}'.\n"
        f"  This dataset requires accepting its terms of use on the Hugging\n"
        f"  Face web UI (and being signed in) before the API will serve\n"
        f"  downloads. Resolve by visiting the dataset page:\n\n"
        f"      https://huggingface.co/datasets/{repo_id}\n\n"
        f"  Click 'Agree and access repository' (or equivalent), make sure\n"
        f"  your local `hf auth login` token is set (or HF_TOKEN), and re-run.\n"
        f"  (Mark the manifest entry with fetch.requires_interactive_accept\n"
        f"  = true to surface this message deterministically next time.)"
    )


def fetch(spec: dict, *, verify: bool = False) -> list[Path]:
    """Fetch `spec`'s inputs. `verify` re-hashes receipt-backed downloads (http,
    uci and custom fetchers); the other kinds keep no receipts, and say so."""
    kind = spec_field(spec, "fetch.type", "http")
    print(f"[fetch] {spec['slug']} ({kind})")
    if verify and kind in ("generated", "kaggle", "huggingface"):
        print(f"  [verify] {spec['slug']}: --verify does not apply to a {kind} fetch; "
              "its cache is checked as usual", file=sys.stderr)
    if kind == "generated":
        from .generate import fetch_generated
        return fetch_generated(spec)
    if kind == "http":        return fetch_http(spec, verify=verify)
    if kind == "kaggle":      return fetch_kaggle(spec)
    if kind == "huggingface": return fetch_huggingface(spec)
    if kind == "uci":         return fetch_http(spec, verify=verify)  # UCI uses plain http under the hood
    if kind == "custom":
        # Resolved against the declared set, not `getattr` on a module, so a
        # manifest string cannot name an arbitrary attribute. Each declared name
        # carries a `fetcher:` capability token, so a catalog needing one this
        # build lacks says so before fetching.
        from importlib import import_module

        from raincloud._registry import CUSTOM_FETCHERS
        name = spec_field(spec, "fetch.notes") or spec["slug"]
        target = CUSTOM_FETCHERS.get(name)
        if target is None:
            raise ValueError(
                f"no custom fetch handler {name!r}; declared: "
                f"{', '.join(sorted(CUSTOM_FETCHERS)) or '(none)'}"
            )
        module_name, _, attr = target.partition(":")
        return getattr(import_module(f".{module_name}", __package__), attr)(spec, verify=verify)
    raise ValueError(f"unknown fetch.type: {kind}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m raincloud.pipeline.fetch", allow_abbrev=False,
                                     description="Download the raw inputs of the named datasets.")
    parser.add_argument("slugs", nargs="*", help="datasets to fetch")
    parser.add_argument("--all", action="store_true",
                        help="fetch every dataset except hydrated ones (name those explicitly)")
    parser.add_argument("--verify", action="store_true",
                        help="re-hash cached receipt-backed downloads (http, uci and custom fetches) "
                             "against their receipts instead of trusting size")
    args = parser.parse_args(argv)
    # Same root set and bookkeeping as every other entry point; see
    # `extract.main`.
    from .lifecycle import operation_lock
    from .selection import select_or_exit
    with operation_lock(resources=True):
        for ds in select_or_exit(parser, load_manifest(), args.slugs, all_=args.all,
                                 verb="fetch", derived="reject"):
            fetch(ds, verify=args.verify)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
