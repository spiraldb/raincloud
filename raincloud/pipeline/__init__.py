# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Raincloud pipeline — client-side reconstruction of a catalog's datasets from its manifest.

Stages (each a separate module):
    fetch     — download upstream sources into the recipe's raw directory
                (`spec.raw_slug_dir`), or take them from a generator group
    extract   — decompress/unpack into a working dir
    parse     — turn raw files into pyarrow Tables or BatchStreams
    transform — apply the named handler to produce the final table(s)
    canonical — write the canonical Arrow spine (outputs/v{n}/<slug>/arrow/<slug>.arrow.zstd)
    validate  — compare actual output to the `expect` block in the manifest
    export    — derive each output format (outputs/v{n}/<slug>/<format>/) from canonical

The orchestrator is `build.py`. Each stage reads ONLY the fields of the
DatasetSpec it needs, so stages can be invoked individually for debugging.
"""
