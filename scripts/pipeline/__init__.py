# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Raincloud pipeline — client-side reconstruction of outputs/prepared/ from sources.json.

Stages (each a separate module):
    fetch     — download upstream sources to outputs/downloads/
    extract   — decompress/unpack into a working dir
    parse     — turn raw files into pyarrow.Tables
    transform — apply the named handler to produce the final table(s)
    write     — emit parquet
    validate  — compare actual output to the `expect` block in the manifest

The orchestrator is `build.py`. Each stage reads ONLY the fields of the
DatasetSpec it needs, so stages can be invoked individually for debugging.
"""
