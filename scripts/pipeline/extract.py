# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 2 — extract downloaded sources into _workdir/<slug>/.

Reads only the `extract` block. Dispatches by `extract.type`:
    passthrough : no-op, downloaded files flow straight through
    zip / tar / 7z / bz2 / gzip : decompress
    custom      : delegate to `scripts/pipeline/custom_extract.py:<spec.slug>`
"""
from __future__ import annotations

import bz2
import fnmatch
import gzip
import shutil
import sys
import tarfile
import zipfile
from pathlib import Path

from .spec import REPO_ROOT, spec_field

WORKDIR = REPO_ROOT / "_workdir"


def slug_workdir(slug: str) -> Path:
    d = WORKDIR / slug
    d.mkdir(parents=True, exist_ok=True)
    return d


def _apply_include_exclude(candidates: list[Path], include: list[str], exclude: list[str]) -> list[Path]:
    out = []
    for p in candidates:
        rel = str(p.name)
        if include and not any(fnmatch.fnmatch(rel, pat) for pat in include):
            continue
        if any(fnmatch.fnmatch(rel, pat) for pat in exclude or []):
            continue
        out.append(p)
    return out


def extract_passthrough(spec: dict, inputs: list[Path]) -> list[Path]:
    include = spec_field(spec, "extract.include", [])
    exclude = spec_field(spec, "extract.exclude", [])
    return _apply_include_exclude(inputs, include, exclude)


def extract_zip(spec: dict, inputs: list[Path]) -> list[Path]:
    wd = slug_workdir(spec["slug"])
    include = spec_field(spec, "extract.include", [])
    exclude = spec_field(spec, "extract.exclude", [])
    out = []
    for src in inputs:
        if not src.name.endswith(".zip"): continue
        print(f"  unzip {src.name} -> {wd.relative_to(REPO_ROOT)}")
        with zipfile.ZipFile(src) as z:
            for name in z.namelist():
                # Strip trailing whitespace for glob matching / disk write
                # (SAS Transport zip files from CDC come with trailing spaces)
                canonical = name.rstrip()
                if include and not any(fnmatch.fnmatch(canonical, pat) for pat in include):
                    continue
                if any(fnmatch.fnmatch(canonical, pat) for pat in exclude or []):
                    continue
                # Extract to the canonical (stripped) name
                target = wd / canonical
                target.parent.mkdir(parents=True, exist_ok=True)
                with z.open(name) as src_f, open(target, "wb") as dst_f:
                    while True:
                        chunk = src_f.read(1 << 20)
                        if not chunk: break
                        dst_f.write(chunk)
                out.append(target)
    return out


def extract_tar(spec: dict, inputs: list[Path]) -> list[Path]:
    wd = slug_workdir(spec["slug"])
    include = spec_field(spec, "extract.include", [])
    exclude = spec_field(spec, "extract.exclude", [])
    out = []
    for src in inputs:
        if not (src.name.endswith(".tar") or src.name.endswith(".tar.gz") or src.name.endswith(".tgz")):
            continue
        print(f"  untar {src.name} -> {wd.relative_to(REPO_ROOT)}")
        with tarfile.open(src) as t:
            for member in t.getmembers():
                if include and not any(fnmatch.fnmatch(member.name, pat) for pat in include): continue
                if any(fnmatch.fnmatch(member.name, pat) for pat in exclude or []): continue
                t.extract(member, wd)
                out.append(wd / member.name)
    return out


def extract_gzip(spec: dict, inputs: list[Path]) -> list[Path]:
    wd = slug_workdir(spec["slug"])
    out = []
    for src in inputs:
        if not src.name.endswith(".gz"): continue
        dest = wd / src.name[:-3]
        print(f"  gunzip {src.name} -> {dest.name}")
        with gzip.open(src, "rb") as g, open(dest, "wb") as w:
            shutil.copyfileobj(g, w)
        out.append(dest)
    return out


def extract_7z(spec: dict, inputs: list[Path]) -> list[Path]:
    import py7zr
    wd = slug_workdir(spec["slug"])
    include = spec_field(spec, "extract.include", [])
    exclude = spec_field(spec, "extract.exclude", [])
    out = []
    for src in inputs:
        if not src.name.endswith(".7z"): continue
        print(f"  un-7z {src.name} -> {wd.relative_to(REPO_ROOT)}")
        with py7zr.SevenZipFile(src, mode="r") as z:
            names = z.getnames()
            keep = []
            for name in names:
                canonical = name.rstrip()
                if include and not any(fnmatch.fnmatch(canonical, pat) for pat in include):
                    continue
                if any(fnmatch.fnmatch(canonical, pat) for pat in exclude or []):
                    continue
                keep.append(name)
            if keep:
                z.extract(path=str(wd), targets=keep)
                for name in keep:
                    out.append(wd / name)
    return out


def extract_bz2(spec: dict, inputs: list[Path]) -> list[Path]:
    wd = slug_workdir(spec["slug"])
    out = []
    for src in inputs:
        if src.name.endswith(".bz2"):
            dest = wd / src.name[:-4]
            if dest.exists() and dest.stat().st_size > 0:
                print(f"  [cached] {dest.name}")
            else:
                print(f"  bunzip2 {src.name} -> {dest.name}")
                with bz2.open(src, "rb") as g, open(dest, "wb") as w:
                    shutil.copyfileobj(g, w)
            out.append(dest)
        else:
            # Pass through non-bz2 inputs (sibling schema files, READMEs, etc.)
            # so the parse/transform stages see them without a separate extract step.
            dest = wd / src.name
            if not (dest.exists() and dest.stat().st_size == src.stat().st_size):
                shutil.copyfile(src, dest)
            out.append(dest)
    return out


def extract(spec: dict, inputs: list[Path]) -> list[Path]:
    kind = spec_field(spec, "extract.type", "passthrough")
    print(f"[extract] {spec['slug']} ({kind})")
    if kind == "passthrough": return extract_passthrough(spec, inputs)
    if kind == "zip":         return extract_zip(spec, inputs)
    if kind in ("tar","tar.gz","tgz"): return extract_tar(spec, inputs)
    if kind == "gzip":        return extract_gzip(spec, inputs)
    if kind == "bz2":         return extract_bz2(spec, inputs)
    if kind == "7z":
        return extract_7z(spec, inputs)
    if kind == "custom":
        from . import custom_extract
        fn = getattr(custom_extract, spec["slug"].replace("-", "_"), None)
        if not fn: raise ValueError(f"no custom extract handler for {spec['slug']}")
        return fn(spec, inputs)
    raise ValueError(f"unknown extract.type: {kind}")


if __name__ == "__main__":
    from .fetch import fetch
    from .spec import iter_datasets, load_manifest
    m = load_manifest()
    for slug in sys.argv[1:]:
        ds = list(iter_datasets(m, slug=slug))
        if not ds: continue
        inputs = fetch(ds[0])
        extract(ds[0], inputs)
