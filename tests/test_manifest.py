# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Static checks on sources.json + the surrounding wiring.

These tests are deliberately fast and side-effect-free — no fetch, no
build, no filesystem writes. They're the regression net for changes to
the manifest, the schema, or the handler registry.

Run: `uv sync --extra dev --inexact && pytest`.
"""
from __future__ import annotations

import json
import subprocess
import sys

import jsonschema
import pytest

from scripts.pipeline.handlers import _REGISTRY
from scripts.pipeline.spec import REPO_ROOT, load_manifest, spec_field
from scripts.pipeline.validate_manifest import _cross_checks, _schema_errors


@pytest.fixture(scope="module")
def manifest() -> dict:
    return load_manifest()


@pytest.fixture(scope="module")
def schema() -> dict:
    return json.loads((REPO_ROOT / "sources.schema.json").read_text())


# ---------- manifest shape ----------

def test_manifest_loads(manifest):
    assert manifest["schema_version"] == 1
    assert isinstance(manifest["datasets"], list)
    assert len(manifest["datasets"]) > 0


def test_manifest_matches_schema(manifest, schema):
    """Authoritative shape check — driven by sources.schema.json."""
    v = jsonschema.Draft202012Validator(schema)
    errs = list(v.iter_errors(manifest))
    assert not errs, "schema errors:\n" + "\n".join(
        f"  {'.'.join(str(p) for p in e.absolute_path)}: {e.message}"
        for e in errs[:10]
    )


def test_schema_is_valid_draft_2020_12(schema):
    """sources.schema.json itself is a well-formed Draft 2020-12 schema."""
    jsonschema.Draft202012Validator.check_schema(schema)


def test_validate_manifest_passes(manifest):
    """The full validator (schema + cross-checks) reports no errors."""
    schema_errs, _ = _schema_errors(manifest)
    cross_errs, _warnings = _cross_checks(manifest)
    assert not schema_errs and not cross_errs, {
        "schema_errors": schema_errs[:5],
        "cross_errors": cross_errs[:5],
    }


# ---------- handler registry ----------

def test_handlers_all_import():
    """Every name in _REGISTRY resolves to a callable."""
    for name, fn in _REGISTRY.items():
        assert callable(fn), f"handler {name!r} is not callable: {fn!r}"


def test_every_spec_handler_is_registered(manifest):
    used = {(d["transform"]["handler"], d["slug"]) for d in manifest["datasets"]}
    missing = [(h, s) for h, s in used if h not in _REGISTRY]
    assert not missing, f"specs reference unregistered handlers: {missing[:5]}"


def test_license_scrape_advisory_present_and_nullable(manifest, schema):
    """`license.scrape_advisory` is required and accepts null (current state) or string.

    Live manifest: every license block has the field set to null today (no
    scrape corpora yet). The schema must accept both the null sentinel and a
    non-null string so the first scrape-flagged slug we add validates.
    """
    for d in manifest["datasets"]:
        assert "scrape_advisory" in d["license"], f"{d['slug']}: license.scrape_advisory missing"
        v = d["license"]["scrape_advisory"]
        assert v is None or isinstance(v, str), f"{d['slug']}: scrape_advisory must be null or string, got {type(v).__name__}"

    # Stamp a non-null string onto the first spec and re-validate to confirm
    # the schema accepts the warning shape we'll actually use.
    sample = json.loads(json.dumps(manifest))  # deep copy
    sample["datasets"][0]["license"]["scrape_advisory"] = (
        "Public-web scrape: per-item upstream licenses are not cleared."
    )
    v = jsonschema.Draft202012Validator(schema)
    errs = list(v.iter_errors(sample))
    assert not errs, [
        f"{'.'.join(str(p) for p in e.absolute_path)}: {e.message}" for e in errs[:3]
    ]


def test_vortex_skip_reason_paired_with_vortex_false(manifest):
    """`convert.vortex` and `convert.vortex_skip_reason` stay in lockstep:
    non-null reason iff `vortex == false`. Cross-check enforces this so the
    derived `docs/v1/vortex_skip.md` is never silent or stale.
    """
    for d in manifest["datasets"]:
        v = d["convert"]["vortex"]
        r = d["convert"]["vortex_skip_reason"]
        if v:
            assert r is None, f"{d['slug']}: vortex=true but skip reason set: {r!r}"
        else:
            assert isinstance(r, str) and r.strip(), (
                f"{d['slug']}: vortex=false requires a non-null reason"
            )


def test_vortex_skip_cross_check_rejects_inconsistent(manifest):
    """The cross-check fires when vortex/skip_reason are mismatched."""
    bad = json.loads(json.dumps(manifest))
    spec = next(d for d in bad["datasets"] if d["convert"]["vortex"])
    spec["convert"]["vortex_skip_reason"] = "stale"
    errs, _ = _cross_checks(bad)
    assert any("vortex=true" in e and "vortex_skip_reason" in e for e in errs), errs[:5]

    bad2 = json.loads(json.dumps(manifest))
    spec2 = next(d for d in bad2["datasets"] if d["convert"]["vortex"])
    spec2["convert"]["vortex"] = False
    spec2["convert"]["vortex_skip_reason"] = None
    errs, _ = _cross_checks(bad2)
    assert any("vortex=false requires" in e for e in errs), errs[:5]


def test_requires_interactive_accept_allowed_on_huggingface(manifest, schema):
    """`fetch.requires_interactive_accept` may be set on huggingface (gated
    repos like LAION) as well as kaggle, but rejects on http/uci/custom."""
    sample = json.loads(json.dumps(manifest))
    hf_spec = next(d for d in sample["datasets"] if d["fetch"]["type"] == "huggingface")
    hf_spec["fetch"]["requires_interactive_accept"] = True
    cross_errs, _ = _cross_checks(sample)
    assert not any("requires_interactive_accept" in e for e in cross_errs), cross_errs[:5]

    bad = json.loads(json.dumps(manifest))
    http_spec = next(d for d in bad["datasets"] if d["fetch"]["type"] == "http")
    http_spec["fetch"]["requires_interactive_accept"] = True
    cross_errs, _ = _cross_checks(bad)
    assert any("requires_interactive_accept" in e for e in cross_errs), cross_errs[:5]


def test_hf_concat_splits_optional_split_and_fsl_cast():
    """`hf_concat_splits` honours add_split_column=False and casts uniform
    list columns to fixed_size_list when named in cast_to_fixed_size_list."""
    from pathlib import Path

    import pyarrow as pa

    from scripts.pipeline.handlers.hf_concat_splits import hf_concat_splits

    spec = {"slug": "fixture"}
    table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "emb": pa.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]],
                         type=pa.list_(pa.float32())),
    })
    parsed = [(Path("fixture/0000.parquet"), table)]

    # Default behaviour: split column added, no FSL cast
    [(_, t1)] = hf_concat_splits(spec, parsed)
    assert "split" in t1.column_names
    assert pa.types.is_list(t1.column("emb").type)

    # add_split_column=False, cast to FSL
    [(_, t2)] = hf_concat_splits(
        spec, parsed,
        add_split_column=False,
        cast_to_fixed_size_list=["emb"],
    )
    assert "split" not in t2.column_names
    assert pa.types.is_fixed_size_list(t2.column("emb").type)
    assert t2.column("emb").type.list_size == 3


def test_hf_allow_patterns_and_revision_validate(manifest, schema):
    """`fetch.hf_allow_patterns` accepts list[str] | null; `hf_revision` accepts str | null.

    Cross-check: both fields are huggingface-only — non-null values on a non-HF spec
    are flagged by validate_manifest.
    """
    sample = json.loads(json.dumps(manifest))
    hf_spec = next((d for d in sample["datasets"] if d["fetch"]["type"] == "huggingface"), None)
    assert hf_spec is not None, "manifest has no huggingface specs to exercise this on"

    hf_spec["fetch"]["hf_allow_patterns"] = ["data/*.parquet", "*.json"]
    hf_spec["fetch"]["hf_revision"] = "main"
    v = jsonschema.Draft202012Validator(schema)
    errs = list(v.iter_errors(sample))
    assert not errs, [
        f"{'.'.join(str(p) for p in e.absolute_path)}: {e.message}" for e in errs[:3]
    ]

    # Cross-check rejects setting these on a non-HF spec
    bad = json.loads(json.dumps(manifest))
    http_spec = next(d for d in bad["datasets"] if d["fetch"]["type"] == "http")
    http_spec["fetch"]["hf_allow_patterns"] = ["foo"]
    cross_errs, _ = _cross_checks(bad)
    assert any("hf_allow_patterns is huggingface-only" in e for e in cross_errs), cross_errs[:5]


def test_hydrate_field_optional_and_validates(manifest, schema):
    """`hydrate` is opt-in: omitted on most specs, present on URL-bearing scrape
    candidates. Schema must accept omission, accept a populated block, and
    reject a malformed shape (missing required key)."""
    n_with = sum(1 for d in manifest["datasets"] if d.get("hydrate"))
    assert n_with > 0, "manifest has no hydrate-marked specs (expected at least laion-400m)"
    for d in manifest["datasets"]:
        if not d.get("hydrate"):
            continue
        h = d["hydrate"]
        for k in ("url_column", "output_column", "output_type", "advisory"):
            assert k in h, f"{d['slug']}: hydrate.{k} missing"
        assert h["output_type"] in ("binary", "string"), d["slug"]
        assert isinstance(h["advisory"], str) and h["advisory"].strip(), d["slug"]

    # Schema rejects a hydrate block missing a required key.
    bad = json.loads(json.dumps(manifest))
    target = next(d for d in bad["datasets"] if d.get("hydrate"))
    target["hydrate"] = {"url_column": "url"}  # missing required keys
    v = jsonschema.Draft202012Validator(schema)
    errs = list(v.iter_errors(bad))
    assert errs, "schema should reject incomplete hydrate block"


def test_references_optional_and_validate(manifest, schema):
    """`references` is opt-in: omitted on most legacy specs, populated on
    the AI/ML cohort. Schema must accept omission, accept populated blocks
    of valid shape, and reject malformed entries."""
    # References are {kind, url} dicts with kind in the enum
    allowed_kinds = {"paper", "blog", "homepage", "github", "dataset_card"}
    for d in manifest["datasets"]:
        for r in d.get("references") or []:
            assert set(r.keys()) == {"kind", "url"}, f"{d['slug']}: ref {r}"
            assert r["kind"] in allowed_kinds, f"{d['slug']}: unknown ref kind {r['kind']!r}"
            assert r["url"].startswith(("http://", "https://")), f"{d['slug']}: ref url {r['url']!r}"

    # Schema rejects an unknown reference kind.
    bad = json.loads(json.dumps(manifest))
    target = next(d for d in bad["datasets"] if d.get("references"))
    target["references"] = [{"kind": "podcast", "url": "https://example.com/x"}]
    v = jsonschema.Draft202012Validator(schema)
    errs = list(v.iter_errors(bad))
    assert errs, "schema should reject unknown reference kind"


def test_no_orphan_handlers(manifest):
    """Every registered handler is referenced by ≥1 spec.

    Catches handlers left behind after a slug removal — they're either
    dead code (delete) or about to be wired up (add a spec).
    """
    used = {d["transform"]["handler"] for d in manifest["datasets"]}
    orphans = sorted(set(_REGISTRY) - used)
    assert not orphans, f"unreferenced handlers: {orphans}"


# ---------- spec helpers ----------

def test_spec_field_walks_dotted_path(manifest):
    spec = manifest["datasets"][0]
    assert spec_field(spec, "fetch.type") == spec["fetch"]["type"]
    assert spec_field(spec, "no.such.path", "default") == "default"
    assert spec_field(spec, "expect.rows") == spec["expect"]["rows"]


# ---------- examples ----------

def test_minimal_spec_example_validates(schema):
    """examples/minimal_spec.json is a valid DatasetSpec.

    Catches drift between the example template and the schema.
    """
    raw = json.loads((REPO_ROOT / "examples" / "minimal_spec.json").read_text())
    raw.pop("_comment", None)
    fake_manifest = {"schema_version": 1, "datasets": [raw]}
    v = jsonschema.Draft202012Validator(schema)
    errs = list(v.iter_errors(fake_manifest))
    assert not errs, [
        f"{'.'.join(str(p) for p in e.absolute_path)}: {e.message}"
        for e in errs[:5]
    ]


# ---------- CLI smoke test ----------

def test_list_datasets_cli_default_lists_all(manifest):
    """Default invocation prints one slug per dataset."""
    proc = subprocess.run(
        [sys.executable, "-m", "scripts.pipeline.list_datasets"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    slugs = [line for line in proc.stdout.splitlines() if line]
    assert len(slugs) == len(manifest["datasets"])


def test_list_datasets_filter_by_family(manifest):
    proc = subprocess.run(
        [sys.executable, "-m", "scripts.pipeline.list_datasets",
         "--family", "uci", "--count"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    expected = sum(1 for d in manifest["datasets"] if d.get("family") == "uci")
    assert int(proc.stdout.strip()) == expected


def test_validate_manifest_cli_exits_zero():
    """End-to-end: the CLI returns 0 on the live manifest."""
    proc = subprocess.run(
        [sys.executable, "-m", "scripts.pipeline.validate_manifest"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stdout + proc.stderr
    assert "manifest is valid" in proc.stdout


def test_schema_has_tags_and_showcase(schema):
    """DatasetSpec advertises the new editorial discovery fields."""
    props = schema["$defs"]["DatasetSpec"]["properties"]
    assert "tags" in props and props["tags"]["type"] == "array"
    assert "showcase" in props and props["showcase"]["type"] == "array"


def test_schema_tags_use_closed_vocab(schema):
    from scripts.pipeline.discovery import TAG_VOCAB
    props = schema["$defs"]["DatasetSpec"]["properties"]
    assert set(props["tags"]["items"]["enum"]) == set(TAG_VOCAB)
    assert props["tags"]["uniqueItems"] is True
    assert props["tags"]["maxItems"] == 3


def test_schema_showcase_use_closed_vocab(schema):
    from scripts.pipeline.discovery import SHOWCASE_TIERS
    props = schema["$defs"]["DatasetSpec"]["properties"]
    assert set(props["showcase"]["items"]["enum"]) == set(SHOWCASE_TIERS)
    assert props["showcase"]["uniqueItems"] is True


def test_validate_manifest_rejects_unknown_tag(manifest):
    """Cross-check surfaces an out-of-vocab tag clearly."""
    bad = json.loads(json.dumps(manifest))   # deep copy
    bad["datasets"][0]["tags"] = ["not-a-real-tag"]
    errors, _warnings = _cross_checks(bad)
    assert any("tags entry" in e and "not-a-real-tag" in e for e in errors)


def test_validate_manifest_rejects_unknown_showcase(manifest):
    bad = json.loads(json.dumps(manifest))
    bad["datasets"][0]["showcase"] = ["nonexistent-tier"]
    errors, _warnings = _cross_checks(bad)
    assert any("showcase entry" in e and "nonexistent-tier" in e for e in errors)


def test_validate_manifest_no_empty_tier_warnings_when_uncurated(manifest):
    """All tiers empty = scaffolding state; no empty-tier warnings."""
    from scripts.pipeline.discovery import SHOWCASE_TIERS

    empty = json.loads(json.dumps(manifest))
    for d in empty["datasets"]:
        d["showcase"] = []
    errors, warnings = _cross_checks(empty)
    for tier in SHOWCASE_TIERS:
        assert not any(tier in w for w in warnings), (
            f"unexpected empty-tier warning for {tier} when all tiers are empty"
        )


def test_validate_manifest_warns_on_partially_curated_empty_tiers(manifest):
    """If at least one tier has members, every other empty tier warns."""
    from scripts.pipeline.discovery import SHOWCASE_TIERS

    partial = json.loads(json.dumps(manifest))
    for d in partial["datasets"]:
        d["showcase"] = []
    partial["datasets"][0]["showcase"] = ["start-here"]   # populate exactly one tier

    errors, warnings = _cross_checks(partial)
    # start-here is populated → no warning for it
    assert not any("start-here" in w for w in warnings)
    # the other 3 tiers are empty → one warning each
    for tier in SHOWCASE_TIERS:
        if tier == "start-here":
            continue
        assert any(tier in w for w in warnings), f"missing warning for empty tier {tier}"
    # And these are warnings, not errors.
    assert not any("encoding-research" in e for e in errors)


def test_validate_manifest_strict_passes_on_live_manifest():
    """`--strict` must exit 0 against the uncurated live manifest."""
    rc = subprocess.run(
        [sys.executable, "-m", "scripts.pipeline.validate_manifest", "--strict"],
        cwd=REPO_ROOT,
        capture_output=True,
    )
    assert rc.returncode == 0, (
        f"validate_manifest --strict failed:\n"
        f"stdout:\n{rc.stdout.decode()}\nstderr:\n{rc.stderr.decode()}"
    )
