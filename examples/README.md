# examples

Runnable, single-file demos of the `raincloud.load(slug)` datasets-like API —
load a real catalog dataset and run an actual query. (For *authoring* templates —
new manifest entries and streaming handlers — see [`../templates/`](../templates/).)

| File | What it does | Engine | What a build fetches |
|---|---|---|---|
| [`use_loader.py`](use_loader.py) | API basics: metadata, format override, `.to_arrow` / `.dataset` / `.to_pandas`, env vars, the typed exception hierarchy. | — | nothing (metadata is network-free; `--materialize` reads one prepared artifact) |
| [`nyc_taxi_tip_rate.py`](nyc_taxi_tip_rate.py) | Of "probably-valid" 2025 yellow-cab trips, what % left no recorded tip? Broken down by `payment_type` to expose that the TLC only records *card* tips. | DuckDB (`raincloud.duckdb_connect()`) over `.dataset()` | ~900 MB (48.7M rows, 12 monthly parquets) |
| [`kepler_exoplanets.py`](kepler_exoplanets.py) | How many Kepler candidates are CONFIRMED vs FALSE POSITIVE, and what's the smallest confirmed planet? | pandas | ~3 MB, seconds |
| [`wine_quality_correlations.py`](wine_quality_correlations.py) | Which physicochemical features correlate with a wine's quality score? | pandas `.corr()` | ~80 KB, instant |
| [`olympic_medals.py`](olympic_medals.py) | Top medal-winning nations and medals per decade across 120 years of the Games. | DuckDB (`raincloud.duckdb_connect()`) over `.dataset()` | ~5 MB, seconds |

## Running them

```bash
pip install "raincloud[build,pandas] @ git+https://github.com/spiraldb/raincloud"
raincloud build kepler-exoplanet-search-results
python examples/kepler_exoplanets.py
```

Reads never build, and there is **no public Raincloud mirror**, so each dataset
must be prepared before its example can read it. Build it once with
`raincloud build <slug>` (the slug is in each script's docstring and `SLUG`),
or run one of the four dataset examples with `--build` to let it build on a miss;
both need the `[build]` extra. Later runs read the prepared file directly. If your
team runs a private mirror, set `RAINCLOUD_MIRROR=s3://bucket/prefix` (or
`file:///path`) and the examples read from it instead. On a miss a dataset example
prints the command that prepares the data and exits 1. `use_loader.py` is the
exception: it has no `--build` and no fixed slug (`--slug` picks one), and a
`--materialize` miss prints a hint for the error it hit and exits 0, since the
rest of its demo still runs. The DuckDB examples open DuckDB through
`raincloud.duckdb_connect()`, which needs the `[duckdb]` extra (`[build]`
includes it). `[pandas]` backs `.to_pandas()`
and DuckDB's `.df()`.
