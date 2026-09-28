# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Acquire a generated input from an atomically committed group cache.

A generator produces a whole group at once (every TPC-H table of one scale
factor, say), so asking for one member generates all of them; later siblings
reuse the group. Each invocation writes a fresh directory. A receipt commits
every output at once, under a group lock independent of the requesting
slug/catalog. Old committed directories stay readable when a group is
refreshed; nothing removes them yet.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tempfile
import warnings
from pathlib import Path

from raincloud._bundle import encode
from raincloud._generated import generation_key, generation_recipe, new_generation, receipt_paths
from raincloud._locking import atomic_write, locked

from .generators import REGISTRY
from .spec import load_manifest, raw_downloads_root, spec_field, workdir_root


def group_root(fetch: dict) -> Path:
    return raw_downloads_root() / ".generated" / generation_key(fetch)


def _checksum(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def cached_outputs(fetch: dict, *, verify: bool | str = True) -> dict[str, Path]:
    """The committed group's outputs, by name; `{}` when nothing is committed.

    Every member is size-checked. `verify` picks which are also re-hashed:
    True for all of them, a member name for that one only (what a cache hit
    needs -- re-hashing a 100 GB group to serve its smallest table is the
    cost this avoids), False for none. Damage raises ValueError.
    """
    root = group_root(fetch)
    try:
        receipt = json.loads((root / "current.json").read_text())
    except FileNotFoundError:
        return {}
    paths = receipt_paths(root, receipt, fetch)
    for name, path in paths.items():
        entry = receipt["outputs"][name]
        if not path.is_file() or path.stat().st_size != entry["bytes"]:
            raise ValueError(f"generated cache member missing or resized: {name}")
        if (verify is True or verify == name) and _checksum(path) != entry["sha256"]:
            raise ValueError(f"generated cache checksum mismatch: {name}")
    return paths


def _previous_receipt(root: Path, fetch: dict) -> tuple[dict | None, str | None]:
    """The committed receipt a refresh compares against, or why it cannot.

    A refresh is the recovery path for a damaged group, so an unreadable or
    invalid receipt is reported and treated as absent rather than raised.
    """
    pointer = root / "current.json"
    try:
        previous = json.loads(pointer.read_text())
        receipt_paths(root, previous, fetch)  # validation only; the paths are unused
    except FileNotFoundError:
        return None, None
    except (OSError, ValueError) as exc:
        return None, f"{pointer}: {exc}"
    return previous, None


def fetch_generated(spec: dict, *, refresh: bool = False) -> list[Path]:
    fetch = spec["fetch"]
    recipe = generation_recipe(fetch)
    try:
        generator = REGISTRY[recipe["generator"]]
    except KeyError:
        raise ValueError(f"unknown generator: {recipe['generator']}") from None
    generator.validate(recipe["parameters"])
    output = fetch["output"]
    if output not in generator.outputs:
        raise ValueError(f"unknown generated output {output!r}")
    root = group_root(fetch)
    remedy = f"regenerate the group with `python -m raincloud.pipeline.generate --refresh {spec['slug']}`"
    with locked(root / ".lock"):
        if not refresh:
            # Corruption fails explicitly. Refresh regenerates the complete
            # group and records drift rather than blessing damaged bytes.
            try:
                paths = cached_outputs(fetch, verify=output)
            except ValueError as exc:
                raise ValueError(f"{exc}; {remedy}") from exc
            if paths:
                # A receipt may carry more outputs than the generator now
                # selects; only a missing member makes the group incomplete.
                missing = sorted(set(generator.outputs) - set(paths))
                if missing:
                    raise ValueError(f"generated cache lacks {', '.join(missing)}; {remedy}")
                print(f"  generated cache hit: {output} ({generation_key(fetch)[:12]})")
                return [paths[output]]
        previous, invalid = _previous_receipt(root, fetch)
        if invalid:
            warnings.warn(f"previous generated receipt is invalid, regenerating without a "
                          f"drift comparison: {invalid}", stacklevel=2)
        print(f"  [note] generating the whole {recipe['generator']} {recipe['parameters']} "
              f"group ({len(generator.outputs)} outputs) to serve {output}; its siblings "
              f"will reuse it", file=sys.stderr, flush=True)
        generation = new_generation()
        destination = root / generation
        destination.mkdir(parents=True)
        scratch_root = workdir_root()
        scratch_root.mkdir(parents=True, exist_ok=True)
        try:
            with tempfile.TemporaryDirectory(prefix="generate-", dir=scratch_root) as scratch:
                runtime = generator.generate(recipe, destination, Path(scratch))
            outputs = {}
            for name, filename in generator.outputs.items():
                path = destination / filename
                outputs[name] = {"file": path.name, "bytes": path.stat().st_size, "sha256": _checksum(path)}
            drift = {name: {"previous": previous["outputs"].get(name), "current": entry}
                     for name, entry in outputs.items()
                     if previous and previous["outputs"].get(name) != entry}
            receipt = {"format_version": 1, "recipe": recipe, "generation": generation,
                       "outputs": outputs, "runtime": runtime, "drift": drift}
            if invalid:
                receipt["previous_receipt_invalid"] = invalid
            # Keep both the invocation receipt and the committed pointer. A
            # failed producer or failed pointer write leaves the prior set intact.
            atomic_write(destination / "receipt.json", encode(receipt))
            atomic_write(root / "current.json", encode(receipt))
        except BaseException:
            shutil.rmtree(destination)
            raise
        if drift:
            warnings.warn(f"generated output drift: {', '.join(sorted(drift))}", stacklevel=2)
        return [destination / outputs[output]["file"]]


def main(argv=None):
    parser = argparse.ArgumentParser(prog="python -m raincloud.pipeline.generate", allow_abbrev=False,
                                     description=__doc__)
    parser.add_argument("slugs", nargs="*", help="generated datasets whose groups to generate")
    parser.add_argument("--all", action="store_true", help="every generated dataset in the catalog")
    parser.add_argument("--refresh", action="store_true", help="regenerate each selected group once, recording checksum drift")
    args = parser.parse_args(argv)
    from .lifecycle import operation_lock
    from .selection import select_or_exit
    # Generation writes under both the raw and the scratch roots.
    with operation_lock(resources=True):
        selected = select_or_exit(parser, load_manifest(), args.slugs, all_=args.all,
                                  verb="generate", derived="reject")
        if args.all:
            selected = [s for s in selected if spec_field(s, "fetch.type") == "generated"]
            if not selected:
                print(f"{parser.prog}: the catalog has no generated datasets", file=sys.stderr)
        others = [s["slug"] for s in selected if spec_field(s, "fetch.type") != "generated"]
        if others:
            parser.exit(2, f"{parser.prog}: not generated datasets: {', '.join(others)}; "
                           f"fetch them with `python -m raincloud.pipeline.fetch`\n")
        seen = set()
        for spec in selected:
            key = generation_key(spec["fetch"])
            if key not in seen:
                print(fetch_generated(spec, refresh=args.refresh)[0])
                seen.add(key)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
