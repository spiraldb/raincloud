# examples

Runnable, single-file demos of the `raincloud.load(slug)` datasets-like API —
load a real catalog dataset and run an actual query. (For *authoring* templates —
new manifest entries and streaming handlers — see [`../templates/`](../templates/).)

| File | What it does | Engine | Cost on first run |
|---|---|---|---|
| [`use_loader.py`](use_loader.py) | API basics: metadata, format override, `.to_arrow` / `.scan` / `.to_pandas`, env vars, the typed exception hierarchy. | — | none (metadata is network-free; `--materialize` resolves one artifact) |
| [`nyc_taxi_tip_rate.py`](nyc_taxi_tip_rate.py) | Of "probably-valid" 2025 yellow-cab trips, what % left no recorded tip? Broken down by `payment_type` to expose that the TLC only records *card* tips. | DuckDB over `.scan()` | ~900 MB (48.7M rows, 12 monthly parquets) |
| [`kepler_exoplanets.py`](kepler_exoplanets.py) | How many Kepler candidates are CONFIRMED vs FALSE POSITIVE, and what's the smallest confirmed planet? | pandas | ~3 MB, seconds |
| [`wine_quality_correlations.py`](wine_quality_correlations.py) | Which physicochemical features correlate with a wine's quality score? | pandas `.corr()` | ~80 KB, instant |
| [`olympic_medals.py`](olympic_medals.py) | Top medal-winning nations and medals per decade across 120 years of the Games. | DuckDB over `.scan()` | ~5 MB, seconds |

## Running them

```bash
pip install "raincloud[build,duckdb,pandas] @ git+https://github.com/spiraldb/raincloud"
python examples/kepler_exoplanets.py
```

There is **no public Raincloud mirror**, so the first run of an example
fetches the upstream data and builds the artifact locally (this is what the
`[build]` extra is for); subsequent runs hit the local cache and are fast. If
your team runs a private mirror, set `RAINCLOUD_MIRROR=s3://bucket/prefix` (or
`file:///path`) and the examples pull from it instead of building. `[duckdb]`
backs `.scan()`, `[pandas]` backs `.to_pandas()`.
