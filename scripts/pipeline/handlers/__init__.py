# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Named transform handlers. Each handler has signature:
    (spec: dict, parsed: list[(Path, pa.Table|None)], **params) -> list[(output_slug, pa.Table)]
"""
from __future__ import annotations

from .beijing_pm25_parse import beijing_pm25_parse
from .cal_housing_parse import cal_housing_parse
from .factbook_variant_parse import factbook_variant_parse
from .ghcn_daily_parse import ghcn_daily_parse
from .glove_split import glove_split
from .har_parse import har_parse
from .hf_concat_splits import hf_concat_splits
from .identity import identity
from .jsonbench_variant_parse import jsonbench_variant_parse
from .jsonl_as_string_parse import jsonl_as_string_parse
from .lichess_pgn_parse import lichess_pgn_parse
from .nyc_rolling_sales import nyc_rolling_sales
from .openlibrary_parse import openlibrary_parse
from .osm_pbf_split import osm_pbf_split
from .public_bi_merge import public_bi_merge
from .sas_xpt_parse import sas_xpt_parse
from .stack_exchange_split import stack_exchange_split
from .text_whitespace_parse import text_whitespace_parse
from .tighten_types import tighten_types
from .tlc_merge_months import tlc_merge_months
from .uci_default import uci_default
from .uci_diabetes_parse import uci_diabetes_parse
from .wikipedia_variant_parse import wikipedia_variant_parse
from .xlsx_parse import xlsx_parse

_REGISTRY = {
    "identity": identity,
    "tighten_types": tighten_types,
    "tlc_merge_months": tlc_merge_months,
    "public_bi_merge": public_bi_merge,
    "uci_default": uci_default,
    "glove_split": glove_split,
    "osm_pbf_split": osm_pbf_split,
    "stack_exchange_split": stack_exchange_split,
    "openlibrary_parse": openlibrary_parse,
    "ghcn_daily_parse": ghcn_daily_parse,
    "sas_xpt_parse": sas_xpt_parse,
    "cal_housing_parse": cal_housing_parse,
    "nyc_rolling_sales": nyc_rolling_sales,
    "factbook_variant_parse": factbook_variant_parse,
    "lichess_pgn_parse": lichess_pgn_parse,
    "jsonbench_variant_parse": jsonbench_variant_parse,
    "wikipedia_variant_parse": wikipedia_variant_parse,
    "text_whitespace_parse": text_whitespace_parse,
    "xlsx_parse": xlsx_parse,
    "har_parse": har_parse,
    "beijing_pm25_parse": beijing_pm25_parse,
    "uci_diabetes_parse": uci_diabetes_parse,
    "hf_concat_splits": hf_concat_splits,
    "jsonl_as_string_parse": jsonl_as_string_parse,
}


def get(name: str):
    return _REGISTRY.get(name)
