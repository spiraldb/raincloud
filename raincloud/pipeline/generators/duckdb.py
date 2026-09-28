# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Serial DuckDB generators, with pinned engine and core extension versions."""
import sys
from pathlib import Path

from raincloud import duckdb_connect
from raincloud.exceptions import BuildToolingMissing


class DuckDBGenerator:
    extension: str
    function: str
    outputs: dict[str, str]

    def generate(self, recipe: dict, destination: Path, scratch: Path) -> dict:
        import duckdb
        if duckdb.__version__ != recipe["version"]:
            # The `generated` extra pins the engine each recipe was recorded with.
            raise BuildToolingMissing(f"generating this recipe needs DuckDB {recipe['version']}; installed "
                                      f"{duckdb.__version__}; install `raincloud[generated]`")
        # Persistent tables can spill; COPY emits one file at a time. SQL names
        # come from registered adapter constants, never catalog input.
        with duckdb_connect(scratch / f"{self.extension}.duckdb", extra_config={"threads": 1}) as con:
            try:
                con.execute(f"LOAD {self.extension}")
            except duckdb.Error as exc:
                # Generation never downloads, so installing the extension is a
                # one-time step of its own.
                raise BuildToolingMissing(
                    f"DuckDB {duckdb.__version__} has no {self.extension} extension installed; "
                    f"install it once with: {sys.executable} -c "
                    f"\"import duckdb; duckdb.execute('INSTALL {self.extension}')\"") from exc
            version = con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name=?",
                                  [self.extension]).fetchone()[0]
            if version != "v" + recipe["version"]:
                raise BuildToolingMissing(
                    f"the {self.extension} extension installed is {version}, not v{recipe['version']}; "
                    f"reinstall it for DuckDB {recipe['version']} with: {sys.executable} -c "
                    f"\"import duckdb; duckdb.execute('FORCE INSTALL {self.extension}')\"")
            con.execute(f"CALL {self.function}(sf = ?)", [recipe["parameters"]["sf"]])
            for table, filename in self.outputs.items():
                con.execute(f'COPY "{table}" TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 65536)',
                            [str(destination / filename)])
        return {"duckdb": duckdb.__version__, self.extension + "_extension": version}
