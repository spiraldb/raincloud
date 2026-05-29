# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""How many Kepler candidates panned out — and what's the smallest one?

Loads the Kepler Objects of Interest table (`kepler-exoplanet-search-results`,
~9.5k rows) into a pandas DataFrame and does two quick passes: the
CONFIRMED / CANDIDATE / FALSE POSITIVE disposition breakdown, and the
smallest-radius CONFIRMED planet. Small enough that pandas is the right tool —
no SQL engine needed.

Run it:

    python examples/kepler_exoplanets.py

Install (raincloud is not on PyPI — install from GitHub):

    pip install "raincloud[build,pandas] @ git+https://github.com/spiraldb/raincloud"   # build: first-run fetch; pandas: .to_pandas()

First run fetches ~3 MB from upstream (or a configured RAINCLOUD_MIRROR) and is
cached; it runs in seconds thereafter.
"""
from __future__ import annotations

import sys

import raincloud

SLUG = "kepler-exoplanet-search-results"


def main(argv: list[str] | None = None) -> int:
    try:
        df = raincloud.load(SLUG).to_pandas()
    except raincloud.MissingDependency as e:
        print(f"this example needs pandas: {e}\n"
              '  pip install "raincloud[pandas] @ git+https://github.com/spiraldb/raincloud"')
        return 1
    except raincloud.RaincloudError as e:
        print(f"could not load {SLUG}: {type(e).__name__}: {e}")
        print('  hint: pip install "raincloud[build] @ git+https://github.com/spiraldb/raincloud" '
              "(first run fetches ~3 MB) or set RAINCLOUD_MIRROR=<url>")
        return 1

    import pandas as pd

    print(f"Kepler Objects of Interest: {len(df):,}\n")
    print("disposition breakdown:")
    for disp, n in df["koi_disposition"].value_counts().items():
        print(f"  {disp:<15}{n:>7,}  ({100 * n / len(df):.1f}%)")

    confirmed = df[df["koi_disposition"] == "CONFIRMED"].dropna(subset=["koi_prad"])
    smallest = confirmed.nsmallest(1, "koi_prad").iloc[0]
    name = smallest["kepler_name"]
    if pd.isna(name):
        name = smallest["kepoi_name"]
    print("\nsmallest CONFIRMED planet by radius:")
    print(f"  {name}: {smallest['koi_prad']:.2f} Earth radii, "
          f"{smallest['koi_period']:.2f}-day orbit")
    return 0


if __name__ == "__main__":
    sys.exit(main())


# --- Sample output (captured 2026-05-29 against live upstream; exact numbers
#     drift as the archive is revised) ---
#
#   Kepler Objects of Interest: 9,564
#
#   disposition breakdown:
#     FALSE POSITIVE   4,839  (50.6%)
#     CONFIRMED        2,747  (28.7%)
#     CANDIDATE        1,978  (20.7%)
#
#   smallest CONFIRMED planet by radius:
#     Kepler-37 b: 0.27 Earth radii, 13.37-day orbit
