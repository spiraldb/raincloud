# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""TPC-DS inputs from DuckDB and the pinned unified tpcgen-rs CLI."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

from ..spec import generator_timeout
from .duckdb import DuckDBGenerator
from .tpch import validate as validate_scale

TABLES = (
    "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
    "customer_address", "customer_demographics", "date_dim", "household_demographics",
    "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
    "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
    "web_sales", "web_site",
)


def validate(parameters):
    validate_scale(parameters)
    if parameters["sf"] < 1:
        raise ValueError("TPC-DS sf must be at least 1")


def validate_rust(parameters):
    if set(parameters) != {"sf", "compat"} or parameters["compat"] not in ("c", "trino"):
        raise ValueError("TPC-DS parameters require sf and compat ('c' or 'trino')")
    validate({"sf": parameters["sf"]})


def source_cli(version: str) -> Path:
    """Authenticate local build provenance, including the exact source revision.

    CLI semver alone is insufficient for an unreleased source build. `version`
    is `<semver>+git.<commit>`: the tpcgen-rs commit to build, from a checkout
    the operator supplies, with `python -m raincloud.pipeline.generators.install_tpcgen
    --source <checkout>`, which writes the receipt checked here beside the
    executable. Cache hits never need the tool.
    """
    name = "tpcgen-cli.exe" if sys.platform == "win32" else "tpcgen-cli"
    adjacent = Path(sys.executable).parent / name
    configured = os.environ.get("RAINCLOUD_TPCGEN_CLI")
    binary = Path(configured) if configured else adjacent if adjacent.is_file() else Path(shutil.which(name) or name)
    binary = binary.resolve()
    receipt_path = binary.with_name(binary.name + ".raincloud.json")
    try:
        receipt = json.loads(receipt_path.read_text())
        with binary.open("rb") as stream:
            checksum = hashlib.file_digest(stream, "sha256").hexdigest()
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"tpcgen-cli {version} is not installed with a receipt at {binary} ({exc}); build it "
            f"from a tpcgen-rs checkout at that commit with python -m "
            f"raincloud.pipeline.generators.install_tpcgen --source CHECKOUT") from exc
    if not isinstance(receipt, dict) or receipt.get("version") != version or receipt.get("sha256") != checksum:
        found = receipt.get("version") if isinstance(receipt, dict) else receipt
        recorded = receipt.get("sha256") if isinstance(receipt, dict) else None
        raise RuntimeError(
            f"{binary}: the recipe wants tpcgen-cli {version}; its receipt records version "
            f"{found} with sha256 {recorded}, and the executable's sha256 is {checksum}")
    actual = subprocess.check_output([str(binary), "--version"], text=True, timeout=60).strip()
    expected = "tpcgen-cli " + version.split("+git.", 1)[0]
    if actual != expected:
        raise RuntimeError(f"{binary} reports {actual!r}; the recipe wants {expected!r}")
    return binary.resolve()


class RustTPCDS:
    # The CLI also emits a one-row `dbgen_version` table: generator version, the
    # generation date and time, and the full command line. That is metadata about
    # OUR invocation, not about the data -- two logically identical builds differ
    # there, so carrying it would make identical datasets compare unequal, and it
    # tells a third party nothing about the dataset they are using. Generator
    # identity belongs in the fetch recipe, which already records it. Not selected.
    outputs = {table: f"{table}.parquet" for table in TABLES}
    validate = staticmethod(validate_rust)

    def generate(self, recipe: dict, destination: Path, scratch: Path) -> dict:
        binary = source_cli(recipe["version"])
        subprocess.run([str(binary), "tpcds", "parquet", "--scale-factor", str(recipe["parameters"]["sf"]),
                        "--compat", recipe["parameters"]["compat"], "--output-dir", str(destination),
                        "--num-threads", "1", "--quiet", "--compression", "ZSTD(1)",
                        "--row-group-bytes", "8388608"], check=True,
                       timeout=generator_timeout())
        return {"tpcgen-cli": recipe["version"], "compat": recipe["parameters"]["compat"]}


class DuckDBTPCDS(DuckDBGenerator):
    extension = "tpcds"
    function = "dsdgen"
    outputs = {table: f"{table}.parquet" for table in TABLES}
    validate = staticmethod(validate)
