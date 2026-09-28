# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Stage 2 — extract downloaded sources into the operation's scratch directory.

Build and standalone CLI operations select the same recipe generation under
the configured scratch root: .recipes/<recipe-hash>/<slug>/.

Reads only the `extract` block. Dispatches by `extract.type`:
    passthrough : no-op, downloaded files flow straight through
    zip / tar / 7z / bz2 / gzip : decompress (`tar` also reads .tar.gz / .tgz)

Every output path is claimed once per extract call, across all inputs: two
members (or two inputs) that land on one file are refused rather than
overwriting each other and being listed twice.
"""
from __future__ import annotations

import argparse
import bz2
import fnmatch
import gzip
import json
import os
import shutil
import sys
import tarfile
import uuid
import zipfile
from contextlib import contextmanager
from pathlib import Path

from . import spec
from .spec import display_path, spec_field


def _safe_target(wd: Path, name: str) -> Path:
    """Resolve an archive member name to a path inside `wd`, or raise.

    Covers more than the classic `..` traversal: an absolute member name makes
    `wd / name` discard `wd` entirely, and a member written THROUGH a symlink
    planted by an earlier member of the same archive resolves outside without
    any `..` appearing anywhere. `.resolve()` follows both, so comparing the
    resolved path against the resolved scratch root catches all three.
    """
    root = wd.resolve()
    target = (wd / name).resolve()
    if target != root and root not in target.parents:
        raise ValueError(f"archive member escapes the scratch directory: {name!r}")
    return target


def _claim(seen: dict[Path, str], target: Path, name: str) -> None:
    """Refuse a second member landing on a path another member already owns.

    Archive formats permit duplicate member names, canonicalization can map
    distinct names onto one path ("t.csv" and "t.csv " both strip to "t.csv"),
    and two inputs can hold a member of the same name. Overwriting silently is
    the worst outcome available: the extract list still grows by one entry per
    member, so parse reads the winner N times and reports N files' worth of rows
    drawn from one. Fail instead of guessing.
    """
    target = target.resolve()
    prior = seen.get(target)
    if prior is not None:
        raise ValueError(
            f"archive members {prior!r} and {name!r} both extract to "
            f"{target.name!r}; refusing to overwrite (the extracted file list "
            f"would name it twice and parse would read it twice)"
        )
    seen[target] = name


@contextmanager
def _atomic_output(target: Path):
    """Write `target` via a temp file in the same directory, renamed on success.

    A decompression killed partway (OOM, power loss, Ctrl-C) must not leave a
    truncated file under the final name, because every later run then reports it
    `[cached]`. Same pattern as `canonical.open_canonical_writer`.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.parent / f".{target.name}.{uuid.uuid4().hex}.part"
    try:
        with open(tmp, "wb") as f:
            yield f
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)


def _source_stamp(src: Path) -> str:
    """Identity of the raw file a cached decompression was made from.

    Inode, size and mtime_ns. A refetch lands through `os.replace` as a new
    file, written now, while a sibling hardlink to the same bytes shares the
    inode and mtime. No device number: dm and nvme device numbers change across
    reboots, which would redo every decompression. The mtime covers what the
    inode alone cannot: an inode reused for a refetched file of the same size.
    This is scratch-cache freshness, not trust in the catalog's bytes.
    """
    st = src.stat()
    return json.dumps({"ino": st.st_ino, "bytes": st.st_size, "mtime_ns": st.st_mtime_ns})


def _stamp_path(dest: Path) -> Path:
    return dest.parent / f".{dest.name}.source"


def _is_fresh(dest: Path, src: Path) -> bool:
    """Is `dest` the decompression of the raw file now at `src`?"""
    stamp = _stamp_path(dest)
    if not (dest.exists() and stamp.is_file()):
        return False
    return stamp.read_text() == _source_stamp(src)


def _copy_from(src: Path, dest: Path, opener) -> None:
    """Write `opener(src)` to `dest` atomically, then record which source it was."""
    with opener(src) as r, _atomic_output(dest) as w:
        shutil.copyfileobj(r, w)
    _stamp_path(dest).write_text(_source_stamp(src))


def slug_workdir(slug: str) -> Path:
    d = spec.workdir_root() / slug
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


def _require_archive(src: Path, kind: str, suffixes: tuple[str, ...]) -> None:
    """Refuse a fetched file this extractor would otherwise pass over: skipping
    it would drop it from the dataset without a word."""
    if not src.name.endswith(suffixes):
        raise ValueError(f"extract.type {kind!r} got {src.name}, which is not a "
                         f"{' / '.join(suffixes)} archive; every fetched file must be extracted")


def extract_passthrough(spec: dict, inputs: list[Path], *, seen: dict | None = None) -> list[Path]:
    include = spec_field(spec, "extract.include", [])
    exclude = spec_field(spec, "extract.exclude", [])
    return _apply_include_exclude(inputs, include, exclude)


def extract_zip(spec: dict, inputs: list[Path], *, seen: dict | None = None) -> list[Path]:
    wd = slug_workdir(spec["slug"])
    include = spec_field(spec, "extract.include", [])
    exclude = spec_field(spec, "extract.exclude", [])
    seen = {} if seen is None else seen
    out = []
    for src in inputs:
        _require_archive(src, "zip", (".zip",))
        print(f"  unzip {src.name} -> {display_path(wd)}")
        with zipfile.ZipFile(src) as z:
            for info in z.infolist():
                name = info.filename
                # A directory entry is identified by the archive metadata, not by
                # its name: `rstrip()` removes whitespace but not the trailing
                # slash, so "data/" is not caught by the name.
                if info.is_dir():
                    continue
                # Strip trailing whitespace for glob matching / disk write
                # (SAS Transport zip files from CDC come with trailing spaces)
                canonical = name.rstrip()
                if include and not any(fnmatch.fnmatch(canonical, pat) for pat in include):
                    continue
                if any(fnmatch.fnmatch(canonical, pat) for pat in exclude or []):
                    continue
                # Extract to the canonical (stripped) name
                target = _safe_target(wd, canonical)
                _claim(seen, target, name)
                with z.open(info) as src_f, _atomic_output(target) as dst_f:
                    while True:
                        chunk = src_f.read(1 << 20)
                        if not chunk: break
                        dst_f.write(chunk)
                out.append(target)
    return out


def extract_tar(spec: dict, inputs: list[Path], *, seen: dict | None = None) -> list[Path]:
    wd = slug_workdir(spec["slug"])
    include = spec_field(spec, "extract.include", [])
    exclude = spec_field(spec, "extract.exclude", [])
    seen = {} if seen is None else seen
    out = []
    for src in inputs:
        _require_archive(src, "tar", (".tar", ".tar.gz", ".tgz"))
        print(f"  untar {src.name} -> {display_path(wd)}")
        with tarfile.open(src) as t:
            for member in t.getmembers():
                if include and not any(fnmatch.fnmatch(member.name, pat) for pat in include): continue
                if any(fnmatch.fnmatch(member.name, pat) for pat in exclude or []): continue
                if member.isdir():
                    continue
                # Only regular files are payload. Symlinks, hardlinks, devices and
                # fifos are never the data we came for, and a symlink member is how
                # an archive redirects a LATER member outside the scratch tree
                # without using `..` anywhere. Skip them loudly rather than
                # silently, so a surprising archive is visible in the log.
                if not member.isfile():
                    print(f"  [skip] {member.name} (not a regular file: {member.type!r})", file=sys.stderr)
                    continue
                target = _safe_target(wd, member.name)
                _claim(seen, target, member.name)
                # `filter=` landed in 3.11.4; on 3.11.0-.3 the checks above still
                # stand on their own. Where it exists it is defence in depth: it
                # also normalizes modes and rejects absolute paths itself.
                if hasattr(tarfile, "data_filter"):
                    t.extract(member, wd, filter="data")
                else:  # pragma: no cover - only on 3.11.0-3.11.3
                    t.extract(member, wd)
                out.append(target)
    return out


def extract_gzip(spec: dict, inputs: list[Path], *, seen: dict | None = None) -> list[Path]:
    wd = slug_workdir(spec["slug"])
    seen = {} if seen is None else seen
    out = []
    for src in inputs:
        _require_archive(src, "gzip", (".gz",))
        dest = wd / src.name[:-3]
        _claim(seen, dest, src.name)
        print(f"  gunzip {src.name} -> {dest.name}")
        with gzip.open(src, "rb") as g, _atomic_output(dest) as w:
            shutil.copyfileobj(g, w)
        out.append(dest)
    return out


def extract_7z(spec: dict, inputs: list[Path], *, seen: dict | None = None) -> list[Path]:
    try:
        import py7zr
    except ModuleNotFoundError as error:
        from raincloud._extras import missing
        raise missing(error, "extracting a .7z archive", needs=("py7zr",)) from error
    wd = slug_workdir(spec["slug"])
    include = spec_field(spec, "extract.include", [])
    exclude = spec_field(spec, "extract.exclude", [])
    seen = {} if seen is None else seen
    out = []
    for src in inputs:
        _require_archive(src, "7z", (".7z",))
        print(f"  un-7z {src.name} -> {display_path(wd)}")
        with py7zr.SevenZipFile(src, mode="r") as z:
            keep = []
            for entry in z.list():
                name = entry.filename
                if entry.is_directory:
                    continue
                # Same rule as tar: only regular files are payload, and leaving a
                # symlink out of the targets means py7zr never creates it, so no
                # later member can be written through it.
                if not entry.is_file:
                    print(f"  [skip] {name} (not a regular file)", file=sys.stderr)
                    continue
                canonical = name.rstrip()
                if include and not any(fnmatch.fnmatch(canonical, pat) for pat in include):
                    continue
                if any(fnmatch.fnmatch(canonical, pat) for pat in exclude or []):
                    continue
                # py7zr does its own writing, so validate the names BEFORE it runs
                # rather than trusting where it puts them.
                target = _safe_target(wd, name)
                _claim(seen, target, name)
                keep.append((name, target))
            if keep:
                z.extract(path=str(wd), targets=[name for name, _ in keep])
                out.extend(target for _, target in keep)
    return out


def extract_bz2(spec: dict, inputs: list[Path], *, seen: dict | None = None) -> list[Path]:
    wd = slug_workdir(spec["slug"])
    seen = {} if seen is None else seen
    out = []
    for src in inputs:
        if src.name.endswith(".bz2"):
            dest = wd / src.name[:-4]
            _claim(seen, dest, src.name)
            # Cached only when it was decompressed from the raw file that is
            # there now: a refetched source must not be served the old output.
            if _is_fresh(dest, src):
                print(f"  [cached] {dest.name}")
            else:
                print(f"  bunzip2 {src.name} -> {dest.name}")
                _copy_from(src, dest, lambda p: bz2.open(p, "rb"))
            out.append(dest)
        else:
            # Pass through non-bz2 inputs (sibling schema files, READMEs, etc.)
            # so the parse/transform stages see them without a separate extract step.
            dest = wd / src.name
            _claim(seen, dest, src.name)
            if not _is_fresh(dest, src):
                _copy_from(src, dest, lambda p: open(p, "rb"))
            out.append(dest)
    return out


_EXTRACTORS = {
    "passthrough": extract_passthrough,
    "zip": extract_zip,
    "tar": extract_tar,
    "gzip": extract_gzip,
    "bz2": extract_bz2,
    "7z": extract_7z,
}


def extract(spec: dict, inputs: list[Path]) -> list[Path]:
    kind = spec_field(spec, "extract.type", "passthrough")
    print(f"[extract] {spec['slug']} ({kind})")
    extractor = _EXTRACTORS.get(kind)
    if extractor is None:
        raise ValueError(f"unknown extract.type: {kind}; known: {', '.join(sorted(_EXTRACTORS))}")
    return extractor(spec, inputs, seen={})


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m raincloud.pipeline.extract", allow_abbrev=False,
                                     description="Fetch and extract the named datasets into scratch.")
    parser.add_argument("slugs", nargs="*", help="datasets to extract")
    parser.add_argument("--all", action="store_true",
                        help="extract every dataset except hydrated ones (name those explicitly)")
    args = parser.parse_args(argv)
    # One helper, `operation_lock`, decides the root set (data, raw and scratch
    # directories) and the nesting bookkeeping for every entry point.
    from .fetch import fetch
    from .lifecycle import operation_lock
    from .selection import select_or_exit
    from .spec import load_manifest, recipe_scratch
    with operation_lock(resources=True):
        for ds in select_or_exit(parser, load_manifest(), args.slugs, all_=args.all,
                                 verb="extract", derived="reject"):
            with recipe_scratch(ds) as scratch:
                print(f"[scratch] {display_path(scratch / ds['slug'])}")
                extract(ds, fetch(ds))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
