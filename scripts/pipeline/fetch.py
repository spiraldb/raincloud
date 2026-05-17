# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 1 — fetch upstream sources to outputs/raw_downloads/<slug>/.

Reads only the `fetch` block of a DatasetSpec. Dispatches by `fetch.type`:
    - http         : urllib download(s)
    - kaggle       : kaggle.KaggleApi.dataset_download_files
    - uci          : http download using UCI's canonical data_url
    - huggingface  : huggingface_hub.snapshot_download
    - custom       : named helper in scripts/pipeline/custom_fetch.py

Idempotent: if `fetch.expected_bytes` matches the on-disk size (and
`expected_sha256` if provided), the URL is skipped.
"""
from __future__ import annotations

import hashlib
import os
import ssl
import sys
import urllib.request
import warnings
from pathlib import Path

from .spec import REPO_ROOT, load_manifest, spec_field

# Raw downloads are NOT version-scoped — the same upstream bytes are fetched
# regardless of pipeline schema_version. Only the `prepared/` outputs are
# versioned, because their layout / column conventions can change.
ORIGINALS_DIR = REPO_ROOT / "outputs" / "raw_downloads"


def slug_dir(slug: str) -> Path:
    d = ORIGINALS_DIR / slug
    d.mkdir(parents=True, exist_ok=True)
    return d


def _already_ok(path: Path, expected_bytes: int | None, expected_sha256: str | None) -> bool:
    if not path.exists():
        return False
    if expected_bytes is not None and path.stat().st_size != expected_bytes:
        return False
    if expected_sha256:
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                h.update(chunk)
        if h.hexdigest() != expected_sha256:
            return False
    return True


_MANIFEST_CACHE: dict | None = None


def _cached_manifest() -> dict:
    """Process-local manifest cache for sibling-cache URL lookup."""
    global _MANIFEST_CACHE
    if _MANIFEST_CACHE is None:
        _MANIFEST_CACHE = load_manifest()
    return _MANIFEST_CACHE


def _find_sibling_cache(target_dir: Path, url: str, name: str,
                        ex_bytes: int | None, ex_sha: str | None) -> Path | None:
    """Find a sibling slug whose spec declares this exact URL and whose
    cached file is already on disk.

    Hardlinking across slugs is only safe when they share an upstream URL
    (e.g. GloVe sizes, OSM Germany kinds). Earlier the match was by basename
    alone, which silently propagated a single bad download across 43 UCI
    slugs whose URLs all ended in `data.csv` — every late slug hardlinked to
    the first one's content rather than fetching its own. Match the URL via
    the manifest, and (when declared) still verify size + sha as defense in
    depth.
    """
    if not ORIGINALS_DIR.exists():
        return None
    target_slug = target_dir.name
    for ds in _cached_manifest()["datasets"]:
        if ds["slug"] == target_slug:
            continue
        if url not in spec_field(ds, "fetch.urls", []):
            continue
        candidate = ORIGINALS_DIR / ds["slug"] / name
        if not candidate.exists():
            continue
        if ex_bytes is not None and candidate.stat().st_size != ex_bytes:
            continue
        if ex_sha:
            h = hashlib.sha256()
            with open(candidate, "rb") as f:
                for chunk in iter(lambda: f.read(1 << 20), b""):
                    h.update(chunk)
            if h.hexdigest() != ex_sha:
                continue
        return candidate
    return None


def _unverified_ssl_context() -> ssl.SSLContext:
    """Build an SSL context that skips cert verification.

    Per-slug escape hatch used only when fetch.verify_tls is False — upstream
    cert has rotted but payload integrity is gated by expected_sha256.
    """
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def fetch_http(spec: dict) -> list[Path]:
    out = []
    urls = spec_field(spec, "fetch.urls", [])
    target_dir = slug_dir(spec["slug"])
    ex_bytes = spec_field(spec, "fetch.expected_bytes")
    ex_sha = spec_field(spec, "fetch.expected_sha256")
    verify_tls = spec_field(spec, "fetch.verify_tls", True)
    for url in urls:
        name = url.rsplit("/", 1)[-1].split("?", 1)[0] or "download.bin"
        dest = target_dir / name
        size_hint = ex_bytes if len(urls) == 1 else None
        sha_hint = ex_sha if len(urls) == 1 else None
        if _already_ok(dest, size_hint, sha_hint):
            print(f"  [cached] {dest.relative_to(REPO_ROOT)}")
            out.append(dest)
            continue
        sibling = _find_sibling_cache(target_dir, url, name, size_hint, sha_hint)
        if sibling is not None:
            print(f"  [reuse] hardlink from {sibling.relative_to(REPO_ROOT)} -> {dest.relative_to(REPO_ROOT)}")
            try:
                os.link(sibling, dest)
            except OSError:
                import shutil
                shutil.copyfile(sibling, dest)
            out.append(dest)
            continue
        # Per-slug escape hatch: when fetch.verify_tls is False, skip cert
        # verification for this URL. Used for upstreams whose cert has expired
        # but whose payload integrity is gated by expected_sha256.
        urlopen_kwargs: dict = {"timeout": 300}
        if not verify_tls:
            print(f"  [warn] verify_tls=false — TLS verification disabled (integrity gated by expected_sha256)")
            urlopen_kwargs["context"] = _unverified_ssl_context()
        print(f"  fetching {url} -> {dest.relative_to(REPO_ROOT)}")
        req = urllib.request.Request(url, headers={"User-Agent": "raincloud-pipeline/0.1"})
        # Per-URL retry (transient network failures common on S3 with 100-file fetches)
        for attempt in range(3):
            try:
                with warnings.catch_warnings():
                    if not verify_tls:
                        # urllib3 surfaces InsecureRequestWarning when verification is
                        # disabled; silence it for this one fetch so output stays clean.
                        try:
                            from urllib3.exceptions import InsecureRequestWarning
                            warnings.simplefilter("ignore", InsecureRequestWarning)
                        except ImportError:
                            pass
                    with urllib.request.urlopen(req, **urlopen_kwargs) as r, open(dest, "wb") as w:
                        while True:
                            chunk = r.read(1 << 20)
                            if not chunk: break
                            w.write(chunk)
                break
            except Exception as e:
                # Drop any partial file so a future run restarts cleanly rather
                # than tripping on cached-but-corrupt bytes. Done on every
                # failure, including the final attempt.
                if dest.exists(): dest.unlink()
                if attempt < 2:
                    print(f"    retry {attempt + 1}/3 after {type(e).__name__}: {e}")
                else:
                    raise
        out.append(dest)
    return out


def fetch_kaggle(spec: dict) -> list[Path]:
    import kaggle
    api = kaggle.KaggleApi(); api.authenticate()
    urls = spec_field(spec, "fetch.urls", [])
    target_dir = slug_dir(spec["slug"])
    needs_accept = spec_field(spec, "fetch.requires_interactive_accept", False)
    out = []
    for url in urls:
        # Expect e.g. https://www.kaggle.com/datasets/<owner>/<dataset>
        import re
        m = re.match(r"^https://www\.kaggle\.com/datasets/([^/]+)/([^/? ]+)", url)
        if not m: raise ValueError(f"not a Kaggle dataset URL: {url}")
        ref = f"{m.group(1)}/{m.group(2)}"
        # Skip if anything already present for this slug
        if any(target_dir.iterdir()):
            print(f"  [cached] {target_dir.relative_to(REPO_ROOT)} (non-empty)")
        else:
            if needs_accept:
                print(f"  kaggle (ToS-gated): {ref} -> {target_dir.relative_to(REPO_ROOT)}")
            else:
                print(f"  kaggle: {ref} -> {target_dir.relative_to(REPO_ROOT)}")
            try:
                api.dataset_download_files(ref, path=str(target_dir), quiet=False, unzip=False)
            except Exception as e:
                # Kaggle returns 403 on datasets that require a one-time
                # click-through-ToS acceptance on the web UI before API access.
                if "403" in str(e) or "Forbidden" in str(e):
                    raise RuntimeError(_kaggle_accept_message(url, ref)) from e
                raise
        out.extend(sorted(target_dir.iterdir()))
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
    from huggingface_hub import snapshot_download
    from huggingface_hub.errors import GatedRepoError
    target_dir = slug_dir(spec["slug"])
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
        print(f"  huggingface{gate}: {repo_id} -> {target_dir.relative_to(REPO_ROOT)}{scope}")
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
        out.extend(sorted(target_dir.rglob("*")))
    return out


def _hf_gate_message(repo_id: str, url: str) -> str:
    return (
        f"Hugging Face returned 401/Gated for dataset '{repo_id}'.\n"
        f"  This dataset requires accepting its terms of use on the Hugging\n"
        f"  Face web UI (and being signed in) before the API will serve\n"
        f"  downloads. Resolve by visiting the dataset page:\n\n"
        f"      https://huggingface.co/datasets/{repo_id}\n\n"
        f"  Click 'Agree and access repository' (or equivalent), make sure\n"
        f"  your local `huggingface-cli login` token is set, and re-run.\n"
        f"  (Mark the manifest entry with fetch.requires_interactive_accept\n"
        f"  = true to surface this message deterministically next time.)"
    )


def fetch(spec: dict) -> list[Path]:
    kind = spec_field(spec, "fetch.type", "http")
    print(f"[fetch] {spec['slug']} ({kind})")
    if kind == "http":        return fetch_http(spec)
    if kind == "kaggle":      return fetch_kaggle(spec)
    if kind == "huggingface": return fetch_huggingface(spec)
    if kind == "uci":         return fetch_http(spec)  # UCI uses plain http under the hood
    if kind == "custom":
        from . import custom_fetch
        handler = spec_field(spec, "fetch.notes") or spec["slug"]
        fn = getattr(custom_fetch, handler, None)
        if not fn: raise ValueError(f"no custom fetch handler: {handler}")
        return fn(spec)
    raise ValueError(f"unknown fetch.type: {kind}")


if __name__ == "__main__":
    # CLI: python -m scripts.pipeline.fetch <slug> [<slug> ...]
    from .spec import iter_datasets, load_manifest
    m = load_manifest()
    slugs = sys.argv[1:] or [d["slug"] for d in m["datasets"]]
    for slug in slugs:
        ds = list(iter_datasets(m, slug=slug))
        if not ds:
            print(f"  no such slug: {slug}", file=sys.stderr); continue
        fetch(ds[0])
