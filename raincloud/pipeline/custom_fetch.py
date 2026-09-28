# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Named custom fetch helpers, dispatched by `fetch.notes` when
`fetch.type = "custom"` (the names are declared in
`raincloud._registry.CUSTOM_FETCHERS`). Each function returns a list of `Path`
objects that the subsequent extract / parse / transform stages will consume as
inputs — same contract as the stock `fetch_http` / `fetch_kaggle` handlers.

Download through `fetch.fetch_url` into `fetch.slug_dir(...)`, so a custom
fetcher gets the stage's guarantees: receipt-checked caching, atomic writes,
the fetch deadline and the truncation check.
"""
from __future__ import annotations

import sys
import urllib.error
from pathlib import Path

from .fetch import fetch_url, slug_dir, url_filename
from .spec import display_path, spec_field


def public_bi_fetch(spec: dict, *, verify: bool = False) -> list[Path]:
    """Fetch a Public BI Benchmark workload.

    `verify` re-hashes cached files against their receipts (`fetch --verify`).

    Sources:
      - Partition list:  GitHub `cwida/public_bi_benchmark/benchmark/<W>/data-urls.txt`
                         (cached beside the partitions, so a fully cached
                          build needs no network)
      - Partition data:  `http://event.cwi.nl/da/PublicBIbenchmark/<W>/<W>_N.csv.bz2`
                         (upgraded to https, since CWI serves both)
      - Schema:          GitHub `benchmark/<W>/tables/<W>_N.table.sql`, one per
                         partition, read by the `public_bi_merge` handler
    """
    workload = spec_field(spec, "transform.params.workload")
    if not workload:
        raise ValueError("public_bi_fetch: spec missing transform.params.workload")
    target_dir = slug_dir(spec["slug"], spec)
    out: list[Path] = []
    base = f"https://raw.githubusercontent.com/cwida/public_bi_benchmark/master/benchmark/{workload}"

    # 1. The partition list.
    urls_txt = fetch_url(f"{base}/data-urls.txt", target_dir / "data-urls.txt", verify=verify)
    url_lines = [ln.strip() for ln in urls_txt.read_text().splitlines() if ln.strip()]

    # 2. Each partition bz2.
    for raw_url in url_lines:
        url = raw_url.replace("http://", "https://")
        dest = fetch_url(url, target_dir / url_filename(url), verify=verify)
        print(f"      -> {display_path(dest)} ({dest.stat().st_size:,} B)")
        out.append(dest)

    # 3. Each partition's `<W>_N.table.sql`. The handler needs them all because
    # some workloads have schema drift across partitions (MLB, SalariesFrance,
    # TrainsUK1, Wins, Rentabilidad, TableroSistemaPenal).
    for n in range(1, len(url_lines) + 1):
        schema_dest = target_dir / f"{workload}_{n}.table.sql"
        try:
            fetch_url(f"{base}/tables/{workload}_{n}.table.sql", schema_dest, verify=verify)
        except urllib.error.HTTPError as e:
            # Only a definitive "upstream has no such schema" is a missing
            # schema: `public_bi_merge` drops that partition and says so. Any
            # other failure would silently shrink the build, so it raises.
            if e.code != 404:
                raise
            print(f"  [warn] no schema for partition {n} of {workload}: {e}", file=sys.stderr)
            continue
        out.append(schema_dest)

    return out
