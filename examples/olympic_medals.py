# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Who wins the most Olympic medals, and how has the haul grown over time?

Loads 120 years of Olympic athlete results
(`120-years-of-olympic-history-athletes-and-results`, ~271k athlete-event rows)
and runs two DuckDB group-bys over `Dataset.dataset()`: the top medal-winning
national committees (NOC), and medals awarded per decade. Each row is one
athlete in one event, so a medal row is a medal won (team events count once
per athlete — a known quirk of this dataset).

Run it:

    python examples/olympic_medals.py [--build]

Install (raincloud is not on PyPI — install from GitHub):

    pip install "raincloud[build,pandas] @ git+https://github.com/spiraldb/raincloud"   # build: prepare the data, and DuckDB; pandas: DuckDB's .df()

The dataset must be prepared first; there is no public mirror. Either build it
once (needs the [build] extra; fetches ~5 MB from upstream):

    raincloud build 120-years-of-olympic-history-athletes-and-results

or pass --build to let this script build it on a miss, or set RAINCLOUD_MIRROR
to a mirror your team runs. Later runs read the prepared file directly.
"""
from __future__ import annotations

import argparse
import sys

import raincloud

SLUG = "120-years-of-olympic-history-athletes-and-results"

TOP_NOCS = """
    SELECT NOC, count(*) AS medals
    FROM games
    WHERE Medal IS NOT NULL
    GROUP BY NOC
    ORDER BY medals DESC
    LIMIT 10
"""

PER_DECADE = """
    SELECT (Year // 10) * 10 AS decade, count(*) AS medals
    FROM games
    WHERE Medal IS NOT NULL
    GROUP BY decade
    ORDER BY decade
"""


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--build", action="store_true",
                    help="build the dataset locally if it is not prepared (needs raincloud[build])")
    args = ap.parse_args(argv)
    try:
        connection = raincloud.duckdb_connect()
    except raincloud.MissingDependency as e:
        print(f"this example needs DuckDB: {e}", file=sys.stderr)
        return 1
    try:
        games = raincloud.load(SLUG, build=args.build).dataset()
    except raincloud.RaincloudError as e:
        print(f"could not load {SLUG}: {type(e).__name__}: {e}", file=sys.stderr)
        if isinstance(e, raincloud.BuildToolingMissing):
            hint = 'install the builder: pip install "raincloud[build] @ git+https://github.com/spiraldb/raincloud"'
        elif isinstance(e, raincloud.BuildFailed):
            hint = "the build failed; its output above says why"
        else:
            hint = (f"prepare it with `raincloud build {SLUG}` (needs raincloud[build]; fetches ~5 MB)"
                    + ("" if args.build else ", rerun with --build,") + " or set RAINCLOUD_MIRROR")
        print(f"  hint: {hint}", file=sys.stderr)
        return 1

    connection.register("games", games)
    top = connection.sql(TOP_NOCS).df()
    decades = connection.sql(PER_DECADE).df()

    print("top 10 medal-winning national committees (1896-2016):")
    for r in top.itertuples():
        print(f"  {r.NOC:<5}{r.medals:>8,}")

    print("\nmedals awarded per decade:")
    peak = decades["medals"].max()
    for r in decades.itertuples():
        bar = "#" * round(40 * r.medals / peak)
        print(f"  {int(r.decade)}s  {r.medals:>6,}  {bar}")
    return 0


if __name__ == "__main__":
    sys.exit(main())


# --- Sample output (captured 2026-05-29 against live upstream; exact numbers
#     drift as the dataset is revised) ---
#
#   top 10 medal-winning national committees (1896-2016):
#     USA     5,637
#     URS     2,503
#     GER     2,165
#     GBR     2,068
#     FRA     1,777
#     ITA     1,637
#     SWE     1,536
#     CAN     1,352
#     AUS     1,320
#     RUS     1,165
#
#   medals awarded per decade:
#     1890s     143  #
#     1900s   2,379  #############
#     1910s     941  #####
#     1920s   3,093  ##################
#     1930s   1,764  ##########
#     1940s     987  ######
#     1950s   2,076  ############
#     1960s   3,529  ####################
#     1970s   2,945  #################
#     1980s   5,145  #############################
#     1990s   4,643  ##########################
#     2000s   7,057  ########################################
#     2010s   5,081  #############################
