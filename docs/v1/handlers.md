<!-- AUTO-GENERATED handlers by scripts/pipeline/docs.py at 2026-05-07T19:55:30Z. Regenerate with: python -m scripts.pipeline.docs handlers. DO NOT EDIT. -->
| Handler | Purpose | Streaming | Extra Deps | # Manifest Specs | Example Slugs |
|---------|---------|-----------|------------|------------------|---------------|
| `beijing_pm25_parse` | Parse UCI Beijing Multi-Site Air Quality (dataset 501). | no | — | 1 | `uci-beijing-multi-site-air-quality` |
| `cal_housing_parse` | Parse StatLib's `cadata.txt` — California Housing dataset from | no | — | 1 | `california-housing-prices` |
| `factbook_variant_parse` | Walk the CIA World Factbook JSON dump (factbook/factbook.json on GitHub), | yes | — | 1 | `countries-of-the-world` |
| `ghcn_daily_parse` | Parse NOAA GHCN-Daily .dly fixed-width records into a long-format parquet. | yes | — | 1 | `ghcn-daily` |
| `glove_split` | Read a single-dimension GloVe text file and emit a | no | — | 3 | `glove-6b-100d`, `glove-6b-200d` (+1 more) |
| `har_parse` | Parse UCI Human Activity Recognition Using Smartphones (dataset 240). | no | — | 1 | `uci-human-activity-recognition-using-smartphones` |
| `hf_concat_splits` | Concat HF parquet shards into one parquet, optionally injecting a | no | — | 52 | `ai2-arc`, `anthropic-hh-rlhf-helpful-base` (+50 more) |
| `identity` | Passthrough handler — single parsed table becomes the output. | no | — | 3 | `clickbench-hits`, `emotions-dataset-for-nlp` (+1 more) |
| `jsonbench_variant_parse` | Parse ClickHouse JSONBench's Bluesky JSONL.gz dumps into a single parquet | yes | — | 1 | `jsonbench-bluesky-100m` |
| `jsonl_as_string_parse` | Stream a JSONL[.gz] file into a parquet with a single `raw_json: string` | yes | — | 1 | `open-food-facts` |
| `lichess_pgn_parse` | Parse a Lichess monthly PGN.zst dump into a streamed parquet. | yes | `zstandard` | 1 | `chess` |
| `nyc_rolling_sales` | Parse NYC Finance rolling-sales XLSX files (one per borough) into a | no | `openpyxl` | 1 | `nyc-property-sales` |
| `openlibrary_parse` | Parse Internet Archive OpenLibrary dump files (`ol_dump_*.txt.gz`). | no | — | 3 | `openlibrary-authors`, `openlibrary-editions` (+1 more) |
| `osm_pbf_split` | Parse an OSM PBF file and emit one of the three element kinds as Arrow. | yes | `osmium` | 3 | `osm-germany-nodes`, `osm-germany-relations` (+1 more) |
| `public_bi_merge` | Merge Public BI Benchmark .csv.bz2 partitions using the sibling .sql schema. | yes | — | 46 | `bi-arade`, `bi-bimbo` (+44 more) |
| `sas_xpt_parse` | Read SAS V5 Transport format (.XPT) files via pyreadstat → Arrow. | no | `pyreadstat` | 5 | `behavioral-risk-factor-surveillance-system`, `cardiovascular-diseases-risk-prediction-dataset` (+3 more) |
| `stack_exchange_split` | Parse a Stack Exchange Data Dump XML file into a streamed parquet. | yes | — | 4 | `stackoverflow-badges`, `stackoverflow-postlinks` (+2 more) |
| `text_whitespace_parse` | Parse a text file with irregular whitespace separators into a flat table. | no | — | 1 | `uci-seeds` |
| `tighten_types` | Apply the standard type-tightening pass: | no | — | 65 | `120-years-of-olympic-history-athletes-and-results`, `airbnb-prices-in-european-cities` (+63 more) |
| `tlc_merge_months` | Merge 12 monthly NYC TLC parquets into a single annual parquet. | no | — | 4 | `fhv_tripdata_2025`, `fhvhv_tripdata_2025` (+2 more) |
| `uci_default` | UCI ML Repository default handler. | no | — | 48 | `glass`, `lung-cancer` (+46 more) |
| `uci_diabetes_parse` | Parse UCI dataset 34 (Diabetes, AIM '94) into a single flat parquet. | no | `unlzw3` | 1 | `uci-diabetes` |
| `wikipedia_variant_parse` | Parse the Wikimedia Foundation *Wikipedia Structured Contents* dump into | yes | — | 1 | `wikipedia-structured-contents` |
| `xlsx_parse` | Parse an .xlsx file into a single parquet, concatenating all sheets. | no | `openpyxl`, `pandas` | 1 | `uci-online-retail-ii` |
