# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""How many NYC yellow-cab riders left no tip in 2025?

Loads the full 2025 yellow-taxi trip record (`yellow_tripdata_2025`,
~48.7M rows) and asks: of the *probably-valid* trips — those whose metered
fare cleared the city's minimum (the flag-drop initial charge) — what share
left no recorded tip? The answer is then broken down by `payment_type`, which
reveals the catch: the TLC only records tips paid by **card**, so cash trips
always show a $0 tip. The headline "no tip" rate is really a "paid cash" rate.

This is the OLAP-on-a-big-dataset showcase: the query runs in DuckDB directly
over `Dataset.dataset()`, so nothing materializes 48M rows into
memory — only the small grouped result comes back as a DataFrame.

Run it:

    python examples/nyc_taxi_tip_rate.py [--build]

Install (raincloud is not on PyPI — install from GitHub):

    pip install "raincloud[build,pandas] @ git+https://github.com/spiraldb/raincloud"   # build: prepare the data, and DuckDB; pandas: DuckDB's .df()

The dataset must be prepared first; there is no public mirror. Either build it
once (needs the [build] extra; fetches ~900 MB, 12 monthly parquet files merged from upstream):

    raincloud build yellow_tripdata_2025

or pass --build to let this script build it on a miss, or set RAINCLOUD_MIRROR
to a mirror your team runs. Later runs read the prepared file directly.
"""
from __future__ import annotations

import argparse
import sys

import raincloud

SLUG = "yellow_tripdata_2025"

# NYC yellow-cab initial charge ("flag drop") for 2025 — the smallest metered
# fare a real trip can record. Filtering above it drops $0 / negative / voided
# rows. See https://www.nyc.gov/site/tlc/passengers/taxi-fare.page
BASE_FARE_2025 = 3.00

# TLC trip-record `payment_type` codes (data dictionary).
PAYMENT_TYPES = {1: "Credit card", 2: "Cash", 3: "No charge",
                 4: "Dispute", 5: "Unknown", 6: "Voided trip"}

QUERY = f"""
    SELECT
        payment_type,
        count(*)                                AS trips,
        count(*) FILTER (WHERE tip_amount <= 0) AS no_tip
    FROM taxi
    WHERE fare_amount > {BASE_FARE_2025}
    GROUP BY payment_type
    ORDER BY trips DESC
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
        taxi = raincloud.load(SLUG, build=args.build).dataset()
    except raincloud.RaincloudError as e:
        print(f"could not load {SLUG}: {type(e).__name__}: {e}", file=sys.stderr)
        if isinstance(e, raincloud.BuildToolingMissing):
            hint = 'install the builder: pip install "raincloud[build] @ git+https://github.com/spiraldb/raincloud"'
        elif isinstance(e, raincloud.BuildFailed):
            hint = "the build failed; its output above says why"
        else:
            hint = (f"prepare it with `raincloud build {SLUG}` (needs raincloud[build]; fetches ~900 MB)"
                    + ("" if args.build else ", rerun with --build,") + " or set RAINCLOUD_MIRROR")
        print(f"  hint: {hint}", file=sys.stderr)
        return 1

    connection.register("taxi", taxi)
    df = connection.sql(QUERY).df()
    total = int(df["trips"].sum())
    no_tip = int(df["no_tip"].sum())

    print(f"NYC yellow cab 2025 — valid trips (fare > ${BASE_FARE_2025:.2f}): {total:,}")
    print(f"  left no recorded tip: {no_tip:,}  ({100 * no_tip / total:.1f}%)\n")
    print(f"  {'payment type':<14}{'trips':>14}{'no tip':>14}{'% no tip':>10}")
    for r in df.itertuples():
        label = PAYMENT_TYPES.get(int(r.payment_type), f"code {r.payment_type}")
        pct = 100 * r.no_tip / r.trips if r.trips else 0.0
        print(f"  {label:<14}{r.trips:>14,}{r.no_tip:>14,}{pct:>9.1f}%")

    print("\nThe TLC records tips only for card payments, so cash trips always show")
    print("a $0 tip — most of the headline 'no tip' rate is really 'paid cash'.")
    return 0


if __name__ == "__main__":
    sys.exit(main())


# --- Sample output (captured 2026-05-29 against live upstream; exact numbers
#     drift as the TLC revises the year's data) ---
#
#   NYC yellow cab 2025 — valid trips (fare > $3.00): 45,387,796
#     left no recorded tip: 15,836,419  (34.9%)
#
#     payment type           trips        no tip  % no tip
#     Credit card       31,025,313     2,282,009      7.4%
#     code 0             9,280,676     8,473,528     91.3%   # an unmapped code that
#     Cash               4,380,579     4,380,337    100.0%   # appeared in 2025 data;
#     Dispute              534,317       533,846     99.9%   # the fallback labels it
#     No charge            166,910       166,698     99.9%   # gracefully rather than
#     Unknown                    1             1    100.0%   # guessing its meaning
#
#   The TLC records tips only for card payments, so cash trips always show
#   a $0 tip — most of the headline 'no tip' rate is really 'paid cash'.
