# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""One-shot enricher: replace the templated Public BI descriptions with
workload-specific ones built from `docs/v1/profiles/<slug>.json`.

All 46 bi-* slugs currently share the same boilerplate — a search for
"Public BI Benchmark" floods every workload with no way to differentiate
them. This script reads each workload's profile and synthesises a lead
sentence around row count, column count, dtype mix, and a handful of
the workload's most distinctive column names (those that aren't the
upstream's anonymised `F1`, `F2`, … placeholders).

Usage:
    python -m scripts.pipeline._enrich_public_bi --dry
    python -m scripts.pipeline._enrich_public_bi          # writes sources.json
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCES = REPO_ROOT / "sources.json"
PROFILE_DIR = REPO_ROOT / "docs" / "v1" / "profiles"

_TRAILING_CONTEXT = (
    "One of CWI's 46 real-world BI-dashboard workloads in the Public BI "
    "Benchmark — pipe-delimited CSV partitions used to stress columnar "
    "engines on production data quirks: inconsistent encodings, mixed "
    "quoting, sparse columns, real-world cardinalities."
)

# Hand-curated provenance notes. The upstream CWI repo ships no per-workload
# descriptions and Tableau Public is gated behind WAF + captcha, so direct
# scraping isn't tractable. Each note here is grounded in inspecting the
# *actual columns* of the slug's profile — the workload names are unreliable
# (`bi-food` is network telemetry, `bi-romance` is Instagram posts,
# `bi-taxpayer` carries the Medicare provider schema, `bi-uberlandia` is
# the same Brazilian-education dump as `bi-mulheresmil`).
_DOMAIN_NOTES: dict[str, str] = {
    # Brazilian federal education / training programs (Mulheres Mil
    # family) — same schema, different workbook framings.
    "bi-mulheresmil": "Background: Brazilian federal education / "
                "professional-training program data (e.g. \"Mulheres Mil\"); "
                "rows carry diploma codes, demographic fields, and "
                "pre-enrolment / certificate dates.",
    "bi-eixo": "Background: Brazilian federal education / training program "
                "data (same schema family as `mulheresmil` — diploma codes, "
                "skin-colour demographic, certificate dates). The workbook "
                "name (\"Eixo\" = axis) doesn't reflect the contents.",
    "bi-uberlandia": "Background: Brazilian federal education / training "
                "program data, same schema family as `mulheresmil`/`eixo` "
                "(the workbook name implies the city of Uberlândia but the "
                "columns are program-wide).",

    # CMS Medicare provider payment / utilization (one canonical CMS feed
    # cropped a half-dozen ways by different workbook authors).
    "bi-cmsprovider": "Background: US Centers for Medicare & Medicaid "
                "Services — physician/provider utilization and Medicare "
                "payment data; columns include AVERAGE_MEDICARE_ALLOWED_AMT, "
                "BENE_UNIQUE_CNT, HCPCS_CODE.",
    "bi-medicare1": "Background: CMS Medicare provider / beneficiary data "
                "(BENE_COUNT, BENE_COUNT_GE65) with Tableau calculation "
                "columns layered on.",
    "bi-medicare2": "Background: CMS Medicare provider payment data — "
                "same `average_Medicare_*` / `bene_*` schema as "
                "`cmsprovider`, different workbook framing.",
    "bi-medicare3": "Background: CMS Medicare provider payment data — "
                "same `AVERAGE_MEDICARE_*` schema as `cmsprovider`, "
                "different workbook framing.",
    "bi-medpayment1": "Background: CMS Medicare provider payment data — "
                "same `average_Medicare_*` schema as `cmsprovider`.",
    "bi-medpayment2": "Background: CMS Medicare provider payment data — "
                "same `average_Medicare_*` schema as `cmsprovider`.",
    "bi-physicians": "Background: CMS Medicare provider payment data — "
                "same `average_Medicare_*` schema as `cmsprovider`; the "
                "workbook name (\"Physicians\") implies a demographic feed, "
                "but the columns are payment / utilisation.",
    "bi-pancreactomy1": "Background: CMS Medicare provider payment data "
                "(`AVERAGE_MEDICARE_*` / `HCPCS_CODE` schema) — the "
                "workbook frames it around pancreatectomy procedures but "
                "the schema is the generic CMS feed.",
    "bi-pancreactomy2": "Background: CMS Medicare provider payment data, "
                "same schema as `pancreactomy1`.",
    "bi-taxpayer": "Background: CMS Medicare provider payment data — "
                "same `average_Medicare_*` schema as `cmsprovider`, "
                "despite the workbook name implying taxpayer / income data.",
    "bi-provider": "Background: CMS Medicare provider payment data — "
                "same `average_Medicare_*` schema as `cmsprovider`.",

    # Twitter-style social-media analytics (one columnar shape repeats).
    "bi-hashtags": "Background: social-media (Twitter-style) analytics "
                "with `interaction#author#*` and `demographic#*` columns.",
    "bi-hatred": "Background: social-media (Twitter) analytics — "
                "`Created Date`, `FF Ratio` (follower / following), "
                "`Favorites`, City; likely sentiment- or hate-speech "
                "tracking via tweets.",
    "bi-citymaxcapita": "Background: Twitter analytics — `FF Ratio` "
                "(follower / following), `Favorites`, `First Link in Tweet`. "
                "The workbook name is not domain-revealing.",
    "bi-euro2016": "Background: Twitter sentiment data *about* UEFA Euro "
                "2016 — `lang`, `latitude` / `longitude`, `polarity`. "
                "Not match / player statistics despite the workbook name.",
    "bi-romance": "Background: Instagram-style social-post data "
                "(`caption`, `created_time`, `lat` / `lng`, `link`) — "
                "workbook name doesn't reflect contents.",
    "bi-iglocations2": "Background: Instagram-style geo-tagged social "
                "posts (`caption`, `created_time`, `country`, `city`).",
    # iglocations1 is NOT Instagram despite the name — it's US Census
    # geographic codes.
    "bi-iglocations1": "Background: US Census geographic data — `CENSUS"
                "2010POP`, `CONCIT`, `COUSUB`, `County Code`, `ESTIMATES"
                "BASE2010`. Workbook name (\"iglocations\") is misleading.",

    # Spanish-language commercial data.
    "bi-rentabilidad": "Background: Spanish-language consumer-goods "
                "distribution profitability data — `AUTOPREVENTA` "
                "(pre-sales) and `AUTOVENTA` (direct-sales) cost/time "
                "columns per channel.",
    "bi-motos": "Background: Spanish-language print/media advertising data "
                "— `Anunciante` (advertiser), `Aviso` (ad), `Año`, "
                "`Cadena` (chain), `Circulacion`. Not motorcycle data "
                "despite the workbook name.",
    "bi-generico": "Background: Spanish-language print/media advertising "
                "data — same `Anunciante` / `Aviso` / `Circulacion` schema "
                "as `motos`, different workbook framing.",

    # Single-purpose workloads.
    "bi-bimbo": "Background: Grupo Bimbo (Mexican multinational bakery) "
                "sales / inventory demand data — `Agencia_ID`, `Cliente_ID`, "
                "`Demanda_uni_equil`; widely circulated via the 2016 Bimbo "
                "Inventory Demand Kaggle competition.",
    "bi-commongovernment": "Background: US federal contract / grant awards "
                "data of the kind published on USAspending.gov.",
    "bi-iublibrary": "Background: Indiana University Bloomington library "
                "catalog data — `Author`, `CallNumber`, `CatalogKey`, "
                "`CopyNumber`.",
    "bi-yalelanguages": "Background: Yale University library catalog / "
                "circulation data — `BIB_ID`, `BIB_FORMAT`, `CHARGE_DATE`, "
                "`CALL_NO_TYPE`. The workbook name suggests language-program "
                "coverage but the schema is generic library bibliographic.",
    "bi-mlb": "Background: Major League Baseball season statistics — `AB` "
                "(at-bats), `AVG` (batting average), `BABIP`, `BB` (walks). "
                "Standard sabermetrics column shape.",
    "bi-nyc": "Background: New York City 311 service requests — `Agency "
                "Name`, `Borough`, `Bridge Highway *` columns; non-emergency "
                "complaint feed.",
    "bi-realestate1": "Background: UK Land Registry property-sale records "
                "— `Address 1` / `Address 2`, `County`, `Date of Transfer`, "
                "`Duration` (lease / freehold).",
    "bi-realestate2": "Background: UK Land Registry property-sale records, "
                "same schema as `realestate1`.",
    "bi-redfin1": "Background: Redfin US housing-market data — homes sold, "
                "average sale-to-list ratio (`avg_sale_to_list_mom`/`yoy`), "
                "by city.",
    "bi-redfin2": "Background: Redfin US housing-market data, same schema "
                "as `redfin1`.",
    "bi-redfin3": "Background: Redfin US housing-market data, same schema "
                "as `redfin1`.",
    "bi-redfin4": "Background: Redfin US housing-market data, same schema "
                "as `redfin1`.",
    "bi-salariesfrance": "Background: French public-sector salary data — "
                "categorical fields keyed by `A129` / `A88` classification "
                "codes (likely the INSEE nomenclatures).",
    "bi-tablerosistemapenal": "Background: Latin-American criminal-justice "
                "dashboard data — `COD DELITO` (offence code), `COMUNA`, "
                "`COD. REGIÓN`. Likely Chilean (\"Tablero del Sistema "
                "Penal\").",
    "bi-telco": "Background: Telecommunications customer-lifecycle data — "
                "`ACTIVATION_DATE`, `ARPU_P1`…`ARPU_P6` (Average Revenue "
                "Per User over six periods); typical churn-analysis shape.",
    "bi-trainsuk1": "Background: UK National Rail operations / lateness "
                "data — `Average Lateness` plus Tableau calculation "
                "columns.",
    "bi-trainsuk2": "Background: UK National Rail operations / lateness "
                "data — `Actual Total Distance Miles`, `Operator`, "
                "`Planned Dest Actual Datetime`, `Financial Year & Period`.",
    "bi-uscensus": "Background: US Census Bureau ACS PUMS variables — "
                "`AGEP` (age), `ACCESS`, `ADJINC` (income adjustment) "
                "and similar three- to five-letter PUMS field codes.",
    "bi-food": "Background: Network / app-usage telemetry data — "
                "`application`, `device`, `subscribers`, `volume_total"
                "_bytes`. Workbook name (\"Food\") doesn't reflect contents.",
    "bi-corporations": "Background: Startup / company database — fields "
                "include `angelco_account` (AngelList) and `crunchbase"
                "_account` identifiers, plus `business_model`, `city`, "
                "`continent`, `country`.",
    # No confident identification (opaque F1...Fn or anonymised
    # Calculation_* columns) for: bi-arade, bi-wins, bi-generico,
    # bi-provider — leave them with the data-shape lead only.
}


def _normalise_dtype_family(dt: str) -> str:
    """Coarse-grain DuckDB / pyarrow dtype names into family buckets."""
    dt = (dt or "").lower()
    if dt.startswith("int") or dt.startswith("uint") or "int8" in dt or "int16" in dt \
            or "int32" in dt or "int64" in dt:
        return "integer"
    if "decimal" in dt or "double" in dt or "float" in dt or dt == "real":
        return "decimal/float"
    if "timestamp" in dt or dt.startswith("date") or dt == "time":
        return "temporal"
    if "string" in dt or "varchar" in dt or "text" in dt:
        return "string"
    if "bool" in dt:
        return "bool"
    if "binary" in dt:
        return "binary"
    if "list" in dt or "struct" in dt or "map" in dt or "variant" in dt:
        return "nested"
    return dt or "other"


_ANON_F = re.compile(r"^F\d+$")


def _is_anonymous(name: str) -> bool:
    return bool(_ANON_F.match(name))


def _pretty_count(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}k"
    return str(n)


def _build_description(slug: str, profile: dict, spec: dict) -> str | None:
    # Preserve the original CamelCase workload name (slug is lowercased).
    workload = (spec.get("short_name") or "").removeprefix("BI-") \
        or slug.removeprefix("bi-")
    rows = profile.get("row_count") or 0
    columns: dict = profile.get("columns") or {}
    if not columns:
        return None

    families: Counter[str] = Counter()
    for n, c in columns.items():
        if not isinstance(c, dict):
            continue
        families[_normalise_dtype_family(c.get("dtype"))] += 1

    n_cols = len(columns)
    # Build the dtype-mix string, dominant family first.
    mix = ", ".join(f"{n} {fam}" for fam, n in families.most_common())

    # Pick up to 4 distinctive (non-anonymous, non-bookkeeping) columns
    # to give the workload a recognisable fingerprint.
    bookkeeping = {"Number of Records"}
    candidates = [
        n for n in columns.keys()
        if not _is_anonymous(n) and n not in bookkeeping
    ]
    candidates = candidates[:4]
    notable = ""
    if candidates:
        cols_str = ", ".join(f"`{c}`" for c in candidates)
        notable = f" Notable columns: {cols_str}."

    rows_str = _pretty_count(int(rows))
    domain = _DOMAIN_NOTES.get(slug, "")
    domain_part = f" {domain}" if domain else ""
    return (
        f"Public BI workload `{workload}` — {rows_str} rows × {n_cols} columns "
        f"({mix}).{notable}{domain_part} {_TRAILING_CONTEXT}"
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    p.add_argument("--dry", action="store_true", help="don't write, just print")
    args = p.parse_args(argv)

    m = json.loads(SOURCES.read_text())
    changed: list[tuple[str, str, str]] = []   # (slug, old_desc, new_desc)
    skipped: list[str] = []
    for spec in m["datasets"]:
        slug = spec["slug"]
        if not slug.startswith("bi-"):
            continue
        prof_path = PROFILE_DIR / f"{slug}.json"
        if not prof_path.exists():
            skipped.append(slug)
            continue
        profile = json.loads(prof_path.read_text())
        new_desc = _build_description(slug, profile, spec)
        if not new_desc:
            skipped.append(slug)
            continue
        old_desc = spec.get("description") or ""
        if old_desc != new_desc:
            changed.append((slug, old_desc, new_desc))
            spec["description"] = new_desc

    print(f"bi-* slugs found:  {sum(1 for d in m['datasets'] if d['slug'].startswith('bi-'))}")
    print(f"updated:           {len(changed)}")
    print(f"skipped (no prof): {len(skipped)}")
    if skipped:
        for s in skipped:
            print(f"  {s}")
    print()
    print("=== sample diffs (first 4) ===\n")
    for slug, old, new in changed[:4]:
        print(f"--- {slug} ---")
        print(f"  OLD: {old[:200]}{'...' if len(old) > 200 else ''}")
        print(f"  NEW: {new}")
        print()

    if args.dry:
        print("(dry — sources.json not written)")
        return 0
    if not changed:
        print("no changes — sources.json already matches")
        return 0
    SOURCES.write_text(json.dumps(m, indent=2) + "\n")
    print(f"wrote {len(changed)} description(s) to {SOURCES}")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
