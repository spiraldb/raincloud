# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""What this distribution can build with — declared once, in one place.

The loader has to answer "can this catalog's recipes be built here?" without
importing twenty-five handler modules that pull pandas, openpyxl, osmium and
duckdb. The names are declared HERE, as import paths resolved on demand, so the
loader gets the answer for free — this module imports nothing — and the
pipeline registries are built from this declaration rather than restated
alongside it.

Adding a handler, exporter or generator is one edit: add it here, and the
registry picks it up. `export/__init__.py` fails at import if a registration and
this declaration disagree, so the two cannot drift apart silently.
"""
from __future__ import annotations

# Transform handlers, as "<module>:<attr>" under `raincloud.pipeline.handlers`.
# By convention the module and the function share the dataset-shape name; the
# mapping stays explicit so one that does not cannot drift unnoticed.
HANDLERS: dict[str, str] = {
    "beijing_pm25_parse": "beijing_pm25_parse:beijing_pm25_parse",
    "cal_housing_parse": "cal_housing_parse:cal_housing_parse",
    "duckdb_table_parse": "duckdb_table_parse:duckdb_table_parse",
    "factbook_variant_parse": "factbook_variant_parse:factbook_variant_parse",
    "ghcn_daily_parse": "ghcn_daily_parse:ghcn_daily_parse",
    "glove_split": "glove_split:glove_split",
    "har_parse": "har_parse:har_parse",
    "hf_concat_splits": "hf_concat_splits:hf_concat_splits",
    "identity": "identity:identity",
    "jsonbench_variant_parse": "jsonbench_variant_parse:jsonbench_variant_parse",
    "jsonl_as_string_parse": "jsonl_as_string_parse:jsonl_as_string_parse",
    "lichess_pgn_parse": "lichess_pgn_parse:lichess_pgn_parse",
    "nyc_rolling_sales": "nyc_rolling_sales:nyc_rolling_sales",
    "openlibrary_parse": "openlibrary_parse:openlibrary_parse",
    "osm_pbf_split": "osm_pbf_split:osm_pbf_split",
    "public_bi_merge": "public_bi_merge:public_bi_merge",
    "sas_xpt_parse": "sas_xpt_parse:sas_xpt_parse",
    "stack_exchange_split": "stack_exchange_split:stack_exchange_split",
    "text_whitespace_parse": "text_whitespace_parse:text_whitespace_parse",
    "tighten_types": "tighten_types:tighten_types",
    "tlc_merge_months": "tlc_merge_months:tlc_merge_months",
    "uci_default": "uci_default:uci_default",
    "uci_diabetes_parse": "uci_diabetes_parse:uci_diabetes_parse",
    "wikipedia_variant_parse": "wikipedia_variant_parse:wikipedia_variant_parse",
    "xlsx_parse": "xlsx_parse:xlsx_parse",
}

# Dataset generators, as "<module>:<class>" under `raincloud.pipeline.generators`.
GENERATORS: dict[str, str] = {
    "duckdb-tpcds": "tpcds:DuckDBTPCDS",
    "duckdb-tpch": "tpch:DuckDBTPCH",
    "tpcgen-rs-tpcds": "tpcds:RustTPCDS",
    "tpcgen-rs-tpch": "tpch:RustTPCH",
}

# Artifact formats. Each is one file per dataset, `<fmt>/<slug>.<ext>`,
# whichever writer made it; `arrow` is the canonical every exporter reads.
#   ext             the file extension; part of the native-client path contract.
#   auto            whether `load(slug)` with no format may pick it. Those that
#                   may are tried in declaration order; any other format opens
#                   only when asked for by name.
#   reader          the module whose presence means this install reads the
#                   format in-process, or None when raincloud only serves the
#                   file's path to a reader the caller brings.
#   implementation  that reader, as `raincloud describe --readers` names it.
#   extra           the raincloud extra that installs the reader, if any.
# Every exporter cell's format must be declared here; `_formats` checks at import.
FORMATS: dict[str, dict] = {
    "vortex": {"ext": "vortex", "auto": True, "reader": "vortex",
               "implementation": "vortex-python", "extra": "vortex"},
    "parquet": {"ext": "parquet", "auto": True, "reader": "pyarrow", "implementation": "pyarrow"},
    "arrow": {"ext": "arrow.zstd", "auto": True, "reader": "pyarrow", "implementation": "pyarrow"},
    # pyarrow's ORC support is a compiled extension some builds leave out.
    "orc": {"ext": "orc", "auto": False, "reader": "pyarrow._orc", "implementation": "pyarrow"},
}

# In-process exporter cells, as "<module>:<class>" under `raincloud.pipeline.export`.
# The built-in priority's first choice; the class carries its own `cell_id`, and
# the key here must match it.
PY_EXPORTERS: dict[str, str] = {
    "parquet@py": "exporters:ParquetExporter",
    "vortex@py": "exporters:VortexExporter",
    "orc@py": "exporters:OrcExporter",
}

# Opt-in exporter cells that shell out to a PATH-discovered reference writer,
# as cell_id -> (format_id, binary). They run where the binary is installed and
# `export.priority` or `--format` asks for them. One SidecarExporter
# class serves all of them, so the whole cell is data — which is why these are
# declared rather than written out as five near-identical registrations.
SIDECAR_EXPORTERS: dict[str, tuple[str, str]] = {
    "parquet@rs": ("parquet", "raincloud-export-parquet-rs"),
    "parquet@java": ("parquet", "raincloud-export-parquet-java"),
    "parquet@hardwood": ("parquet", "raincloud-export-parquet-hardwood"),
    "vortex@rs": ("vortex", "raincloud-export-vortex-rs"),
    "vortex@jni": ("vortex", "raincloud-export-vortex-jni"),
    "orc@rs": ("orc", "raincloud-export-orc-rs"),
}

# Custom fetchers, as "<module>:<attr>" under `raincloud.pipeline`. A recipe
# names its fetcher in `fetch.notes`, a free-text field that also serves as
# human notes: only a name allow-listed here runs, and it becomes the recipe's
# `fetcher:<name>` capability token, which the loader's build check sees. So an
# edit to fetch.notes changes which capability the recipe requires.
CUSTOM_FETCHERS: dict[str, str] = {
    "public_bi_fetch": "custom_fetch:public_bi_fetch",
}

# Bundle reader tokens: the artifact layout features a reader must understand
# to open this distribution's artifacts. Recorded in every bundle's `readers`
# list and checked on load, so a catalog packed by a newer raincloud refuses to
# open here rather than half-working.
BUNDLE_READER_TOKENS: tuple[str, ...] = (
    "artifact-layout-v1",
    "arrow-ipc-zstd",
    "parquet",
    "vortex",
)


def exporter_cells() -> dict[str, str]:
    """Every exporter cell id, in-process and sidecar, mapped to its kind."""
    return {**{cell: "python" for cell in PY_EXPORTERS},
            **{cell: "sidecar" for cell in SIDECAR_EXPORTERS}}


def builder_capabilities() -> list[str]:
    """The `builders` token list recorded in a catalog bundle."""
    return sorted(
        [f"fetcher:{name}" for name in CUSTOM_FETCHERS]
        + [f"handler:{name}" for name in HANDLERS]
        + [f"generator:{name}" for name in GENERATORS]
        + [f"exporter:{cell}" for cell in exporter_cells()]
    )
