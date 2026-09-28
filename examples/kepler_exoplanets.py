# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""How many Kepler candidates panned out — and what's the smallest one?

Loads the Kepler Objects of Interest table (`kepler-exoplanet-search-results`,
~9.5k rows) into a pandas DataFrame and does two quick passes: the
CONFIRMED / CANDIDATE / FALSE POSITIVE disposition breakdown, and the
smallest-radius CONFIRMED planet. Small enough that pandas is the right tool —
no SQL engine needed.

Run it:

    python examples/kepler_exoplanets.py [--build]

Install (raincloud is not on PyPI — install from GitHub):

    pip install "raincloud[build,pandas] @ git+https://github.com/spiraldb/raincloud"   # build: prepare the data; pandas: .to_pandas()

The dataset must be prepared first; there is no public mirror. Either build it
once (needs the [build] extra; fetches ~3 MB from upstream):

    raincloud build kepler-exoplanet-search-results

or pass --build to let this script build it on a miss, or set RAINCLOUD_MIRROR
to a mirror your team runs. Later runs read the prepared file directly.
"""
from __future__ import annotations

import argparse
import sys

import raincloud

SLUG = "kepler-exoplanet-search-results"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--build", action="store_true",
                    help="build the dataset locally if it is not prepared (needs raincloud[build])")
    args = ap.parse_args(argv)
    try:
        df = raincloud.load(SLUG, build=args.build).to_pandas()
    except raincloud.MissingDependency as e:
        print(f"this example needs pandas: {e}\n"
              '  pip install "raincloud[pandas] @ git+https://github.com/spiraldb/raincloud"', file=sys.stderr)
        return 1
    except raincloud.RaincloudError as e:
        print(f"could not load {SLUG}: {type(e).__name__}: {e}", file=sys.stderr)
        if isinstance(e, raincloud.BuildToolingMissing):
            hint = 'install the builder: pip install "raincloud[build] @ git+https://github.com/spiraldb/raincloud"'
        elif isinstance(e, raincloud.BuildFailed):
            hint = "the build failed; its output above says why"
        else:
            hint = (f"prepare it with `raincloud build {SLUG}` (needs raincloud[build]; fetches ~3 MB)"
                    + ("" if args.build else ", rerun with --build,") + " or set RAINCLOUD_MIRROR")
        print(f"  hint: {hint}", file=sys.stderr)
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
