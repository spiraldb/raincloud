# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Serial TPC-H adapters producing one complete set of intermediate files."""
from __future__ import annotations

import math
import shutil
import subprocess
import sys
from pathlib import Path

from ..spec import generator_timeout
from .duckdb import DuckDBGenerator

TABLES = ("region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem")


def validate(parameters):
    if set(parameters) != {"sf"}:
        raise ValueError("TPC-H parameters must contain exactly sf")
    sf = parameters["sf"]
    if type(sf) not in (int, float) or not math.isfinite(sf) or sf <= 0:
        raise ValueError("TPC-H sf must be a finite positive number")


class DuckDBTPCH(DuckDBGenerator):
    extension = "tpch"
    function = "dbgen"
    outputs = {table: f"{table}.parquet" for table in TABLES}
    validate = staticmethod(validate)


class RustTPCH:
    outputs = {table: f"{table}.parquet" for table in TABLES}
    validate = staticmethod(validate)

    def generate(self, recipe: dict, destination: Path, scratch: Path) -> dict:
        # Prefer a CLI installed alongside the running Python, then PATH; its
        # reported version is checked, so a stray system binary cannot drift.
        name = "tpchgen-cli.exe" if sys.platform == "win32" else "tpchgen-cli"
        adjacent = Path(sys.executable).parent / name
        binary = str(adjacent) if adjacent.is_file() else shutil.which(name)
        if binary is None:
            from raincloud._extras import extra_for
            from raincloud.exceptions import BuildToolingMissing
            extra = extra_for("tpchgen-cli")
            how = f"install `raincloud[{extra}]`" if extra else "install it"
            raise BuildToolingMissing(
                f"generating this recipe needs tpchgen-cli=={recipe['version']} on PATH or "
                f"beside {sys.executable}; {how}")
        actual = subprocess.check_output([binary, "--version"], text=True, timeout=60).strip()
        if actual != "tpchgen " + recipe["version"]:
            raise RuntimeError(f"tpchgen-cli {recipe['version']} required; found {actual}")
        subprocess.run([binary, "parquet", "--scale-factor", str(recipe["parameters"]["sf"]),
                        "--output-dir", str(destination), "--num-threads", "1", "--quiet",
                        "--compression", "ZSTD(1)", "--row-group-bytes", "8388608"], check=True,
                       timeout=generator_timeout())
        return {"tpchgen-cli": recipe["version"]}
