# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Named custom fetch helpers, dispatched by `fetch.notes` when
`fetch.type = "custom"`. Each function returns a list of `Path` objects that
the subsequent extract / parse / transform stages will consume as inputs —
same contract as the stock `fetch_http` / `fetch_kaggle` handlers.

Keep custom fetchers idempotent: skip downloads for files already present
on disk. Files should land under `outputs/raw_downloads/<slug>/` so the
standard caching guarantees apply.
"""
from __future__ import annotations

import urllib.request
from pathlib import Path

from .fetch import slug_dir
from .spec import REPO_ROOT, spec_field


def _http_download(url: str, dest: Path, timeout: int = 300) -> None:
    req = urllib.request.Request(url, headers={"User-Agent": "raincloud-pipeline/0.1"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r, open(dest, "wb") as w:
            while True:
                chunk = r.read(1 << 20)
                if not chunk: break
                w.write(chunk)
    except Exception:
        if dest.exists(): dest.unlink()
        raise


def public_bi_fetch(spec: dict) -> list[Path]:
    """Fetch a Public BI Benchmark workload.

    Sources:
      - Partition list:  GitHub `cwida/public_bi_benchmark/benchmark/<W>/data-urls.txt`
      - Partition data:  `http://event.cwi.nl/da/PublicBIbenchmark/<W>/<W>_N.csv.bz2`
                         (upgraded to https, since CWI serves both)
      - Schema:          GitHub `benchmark/<W>/tables/<W>_1.table.sql`
                         (renamed to `<W>.schema.sql` on landing so the
                          existing `public_bi_merge` handler picks it up;
                          partitions share a compatible schema modulo the
                          varchar widths pyarrow ignores anyway)
    """
    workload = spec_field(spec, "transform.params.workload")
    if not workload:
        raise ValueError("public_bi_fetch: spec missing transform.params.workload")
    target_dir = slug_dir(spec["slug"])
    out: list[Path] = []

    # 1. Fetch data-urls.txt from GitHub
    urls_txt_url = (
        "https://raw.githubusercontent.com/cwida/public_bi_benchmark/"
        f"master/benchmark/{workload}/data-urls.txt"
    )
    print(f"  fetching data-urls for {workload}")
    try:
        with urllib.request.urlopen(urls_txt_url, timeout=60) as r:
            url_lines = [ln.strip() for ln in r.read().decode().splitlines() if ln.strip()]
    except Exception as e:
        raise RuntimeError(f"public_bi_fetch: failed to read data-urls.txt for {workload}: {e}") from e

    # 2. Download each partition bz2
    for raw_url in url_lines:
        url = raw_url.replace("http://", "https://")
        name = url.rsplit("/", 1)[-1]
        dest = target_dir / name
        if dest.exists():
            print(f"    [cached] {dest.relative_to(REPO_ROOT)} ({dest.stat().st_size:,} B)")
        else:
            print(f"    fetching {url}")
            _http_download(url, dest)
            print(f"      -> {dest.relative_to(REPO_ROOT)} ({dest.stat().st_size:,} B)")
        out.append(dest)

    # 3. Download each partition's `<W>_N.table.sql`. The handler needs them
    # all because some workloads have schema drift across partitions (MLB,
    # SalariesFrance, TrainsUK1, Wins, Rentabilidad, TableroSistemaPenal).
    n_partitions = len(url_lines)
    for n in range(1, n_partitions + 1):
        schema_url = (
            "https://raw.githubusercontent.com/cwida/public_bi_benchmark/"
            f"master/benchmark/{workload}/tables/{workload}_{n}.table.sql"
        )
        schema_dest = target_dir / f"{workload}_{n}.table.sql"
        if schema_dest.exists():
            print(f"    [cached] {schema_dest.relative_to(REPO_ROOT)}")
        else:
            print(f"    fetching {schema_url}")
            try:
                _http_download(schema_url, schema_dest, timeout=60)
            except Exception as e:
                print(f"      skipped partition schema {n}: {e}")
                continue
        out.append(schema_dest)

    return out
