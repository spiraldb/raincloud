# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Portable identity for generated inputs; no generator dependencies imported."""
from __future__ import annotations

import secrets

from ._bundle import REVISION, digest, encode

# A generation id names one invocation's output directory. It is 64 lowercase
# hex characters -- the shape of a catalog revision -- so `_bundle.REVISION`
# validates it; nothing else ties the two together.
GENERATION = REVISION


def new_generation() -> str:
    """A fresh generation id: 256 random bits, as `GENERATION` expects."""
    return secrets.token_hex(32)


def generation_recipe(fetch: dict) -> dict:
    if fetch.get("type") != "generated":
        raise ValueError("expected fetch.type=generated")
    for name in ("generator", "version", "output"):
        if not isinstance(fetch.get(name), str) or not fetch[name]:
            raise ValueError(f"generated fetch needs a nonempty {name}")
    if not isinstance(fetch.get("parameters"), dict):
        raise ValueError("generated fetch needs a parameters object")
    # Output selection, catalog labels and downstream transforms do not split a
    # co-generated input group. The complete generator invocation does.
    recipe = {k: fetch[k] for k in ("generator", "version", "parameters")}
    encode(recipe)  # reject non-finite JSON before constructing a cache path
    return recipe


def generation_key(fetch: dict) -> str:
    return digest(encode(generation_recipe(fetch)))


def receipt_paths(root, receipt: dict, fetch: dict) -> dict:
    """Validate untrusted receipt paths before a reader touches any files."""
    if not isinstance(receipt, dict):
        raise ValueError("generated cache receipt must be an object")
    if receipt.get("recipe") != generation_recipe(fetch) or receipt.get("format_version") != 1:
        raise ValueError("generated cache receipt identity mismatch")
    generation = receipt.get("generation", "")
    if not isinstance(generation, str) or not GENERATION.fullmatch(generation):
        raise ValueError("invalid generated cache generation")
    entries = receipt.get("outputs")
    if not isinstance(entries, dict) or not entries:
        raise ValueError("empty generated cache receipt")
    paths = {}
    for name, entry in entries.items():
        if not isinstance(entry, dict):
            raise ValueError("invalid generated output receipt")
        filename = entry.get("file")
        if (not isinstance(filename, str) or filename in ("", ".", "..")
                or "/" in filename or "\\" in filename or ":" in filename):
            raise ValueError("unsafe generated output filename")
        if not isinstance(entry.get("sha256"), str) or not REVISION.fullmatch(entry["sha256"]):
            raise ValueError("invalid generated output checksum")
        if type(entry.get("bytes")) is not int or entry["bytes"] < 0:
            raise ValueError("invalid generated output size")
        paths[name] = root / generation / filename
    return paths
