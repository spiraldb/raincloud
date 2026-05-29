# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Which measurable properties of a wine track its quality score?

Loads the UCI wine-quality table (`uci-wine-quality`, 6,497 red + white wines
with 11 physicochemical measurements plus a 0-10 sensory `quality` score) into
pandas and correlates each numeric feature against `quality`, sorted. A tiny,
instant example of the load -> DataFrame -> explore loop.

Run it:

    python examples/wine_quality_correlations.py

Install (raincloud is not on PyPI — install from GitHub):

    pip install "raincloud[build,pandas] @ git+https://github.com/spiraldb/raincloud"   # build: first-run fetch; pandas: .to_pandas()

First run fetches ~80 KB from upstream (or a configured RAINCLOUD_MIRROR), then
it's cached.
"""
from __future__ import annotations

import sys

import raincloud

SLUG = "uci-wine-quality"


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
              "(first run fetches ~80 KB) or set RAINCLOUD_MIRROR=<url>")
        return 1

    # numeric_only drops the non-numeric `color` (red/white) column.
    corr = df.corr(numeric_only=True)["quality"].drop("quality").sort_values()

    print(f"{SLUG}: {len(df):,} wines (red + white)\n")
    print("correlation of each feature with the quality score:")
    for feat, c in corr.items():
        print(f"  {feat:<22}{c:+.3f}")
    print(f"\nstrongest positive: {corr.index[-1]} ({corr.iloc[-1]:+.3f})")
    print(f"strongest negative: {corr.index[0]} ({corr.iloc[0]:+.3f})")
    return 0


if __name__ == "__main__":
    sys.exit(main())


# --- Sample output (captured 2026-05-29 against live upstream; exact numbers
#     drift as the dataset is revised) ---
#
#   uci-wine-quality: 6,497 wines (red + white)
#
#   correlation of each feature with the quality score:
#     density               -0.306
#     volatile_acidity      -0.266
#     chlorides             -0.201
#     fixed_acidity         -0.077
#     total_sulfur_dioxide  -0.041
#     residual_sugar        -0.037
#     ph                    +0.020
#     sulphates             +0.038
#     free_sulfur_dioxide   +0.055
#     citric_acid           +0.086
#     alcohol               +0.444
#
#   strongest positive: alcohol (+0.444)
#   strongest negative: density (-0.306)
