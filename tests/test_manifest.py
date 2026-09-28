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

from raincloud.pipeline.handlers import _REGISTRY
from raincloud.pipeline.spec import REPO_ROOT, load_manifest, spec_field
from raincloud.pipeline.validate_manifest import _cross_checks, _schema_errors


@pytest.fixture(scope="module")
def manifest() -> dict:
    return load_manifest()


@pytest.fixture(scope="module")
def schema() -> dict:
    return json.loads((REPO_ROOT / "sources.schema.json").read_text())


# ---------- manifest shape ----------

def test_manifest_loads(manifest):
    assert manifest["schema_version"] == 2
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
    """Every declared handler name resolves to a callable.

    The registry holds import paths until something asks for a handler, so this
    is the test that actually proves each path is correct — a typo in
    `raincloud._registry.HANDLERS` would otherwise surface only when a build
    reached that handler.
    """
    from raincloud.pipeline.handlers import get, names

    for name in names():
        fn = get(name)
        assert callable(fn), f"handler {name!r} is not callable: {fn!r}"


def test_every_spec_handler_is_registered(manifest):
    used = {(d["transform"]["handler"], d["slug"]) for d in manifest["datasets"] if not d.get("derive")}
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


def test_vortex_opt_out_is_declared_in_export_formats(manifest):
    """In schema_version 2, export.formats is the only declaration of which
    formats a dataset wants: no spec carries the v1 `convert` block, and none
    leaves Vortex out for a writer's limitation -- the build measures those.
    """
    from raincloud._formats import export_formats

    assert manifest["schema_version"] == 2
    carrying = [d["slug"] for d in manifest["datasets"] if "convert" in d]
    assert not carrying, f"v2 specs still carry convert.*: {carrying}"
    opted_out = [d["slug"] for d in manifest["datasets"] if "vortex" not in export_formats(d, 2)]
    assert not opted_out, f"specs leave Vortex out: {opted_out}"


def test_vortex_opt_out_cross_check_rejects_inconsistent(manifest):
    """The v2 cross-check rejects a `convert` block. Leaving Vortex out needs
    no reason: a deliberate policy may give one, and a writer's limitation is
    measured, not declared."""
    from raincloud._formats import export_formats

    bad = json.loads(json.dumps(manifest))
    spec = next(d for d in bad["datasets"] if not d.get("derive"))
    spec["convert"] = {"vortex": True, "vortex_skip_reason": None}
    errs, _ = _cross_checks(bad)
    assert any(e.startswith(f"{spec['slug']}: convert.* is schema_version 1 only") for e in errs), errs[:5]

    bad2 = json.loads(json.dumps(manifest))
    spec2 = next(d for d in bad2["datasets"]
                 if not d.get("derive") and "vortex" in export_formats(d, 2))
    spec2["export"] = {"formats": ["parquet"]}
    errs, _ = _cross_checks(bad2)
    assert not any(e.startswith(f"{spec2['slug']}:") for e in errs), errs[:5]

    spec2["export"]["notes"] = "a reason"
    errs, _ = _cross_checks(bad2)
    assert not any(e.startswith(f"{spec2['slug']}:") for e in errs), errs[:5]


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

    from raincloud.pipeline.handlers.hf_concat_splits import hf_concat_splits

    spec = {"slug": "fixture"}
    table = pa.table({
        "id": pa.array([1, 2, 3], type=pa.int64()),
        "emb": pa.array([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]],
                         type=pa.list_(pa.float32())),
    })
    parsed = [(Path("fixture/0000.parquet"), table)]

    # Default behaviour: split column added, no FSL cast
    [(_, t1)] = hf_concat_splits(spec, parsed)
    assert "split" in t1.schema.names
    assert pa.types.is_list(t1.schema.field("emb").type)

    # add_split_column=False, cast to FSL
    [(_, t2)] = hf_concat_splits(
        spec, parsed,
        add_split_column=False,
        cast_to_fixed_size_list=["emb"],
    )
    assert "split" not in t2.schema.names
    assert pa.types.is_fixed_size_list(t2.schema.field("emb").type)
    assert t2.schema.field("emb").type.list_size == 3
    with t2.open() as batches:
        actual = pa.Table.from_batches([item.batch for item in batches], schema=t2.schema)
    assert actual["emb"].to_pylist() == table["emb"].to_pylist()


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


def test_hydrated_datasets_are_derived_entries(manifest, schema):
    """A hydrated dataset is its own entry, `<parent>-hydrated`, deriving from an
    ordinary dataset; the schema requires its advisory and forbids upstream stages."""
    by_slug = {d["slug"]: d for d in manifest["datasets"]}
    hydrated = [d for d in manifest["datasets"] if (d.get("derive") or {}).get("hydrate")]
    assert hydrated, "manifest has no hydrated datasets (expected at least laion-400m-hydrated)"
    for d in hydrated:
        parent = d["derive"]["from"]
        assert d["slug"] == f"{parent}-hydrated" and not by_slug[parent].get("derive"), d["slug"]
        assert isinstance(d["advisory"], str) and d["advisory"].strip(), d["slug"]
        for target in d["derive"]["hydrate"]["columns"].values():
            assert target["type"] in ("binary", "string"), d["slug"]
    assert not any("hydrate" in d for d in manifest["datasets"]), "hydrate lives under derive now"

    v = jsonschema.Draft202012Validator(schema)
    for change in (lambda t: t.pop("advisory"),                             # advisory required
                   lambda t: t.update(fetch={"type": "http", "urls": []}),  # no upstream stages
                   lambda t: t["derive"]["hydrate"].update(columns={})):    # at least one column
        bad = json.loads(json.dumps(manifest))
        change(next(d for d in bad["datasets"] if d.get("derive")))
        assert list(v.iter_errors(bad)), "schema should reject a malformed hydrated dataset"
    bad = json.loads(json.dumps(manifest))
    next(d for d in bad["datasets"] if not d.get("derive"))["advisory"] = "only derived datasets carry one"
    assert list(v.iter_errors(bad))


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


# ---------- v2 schema + export block ----------

def test_schema_version_2_accepted(manifest, schema, tmp_path):
    """The schema now accepts `schema_version: 2`, and `load_manifest` loads a
    v2 manifest from disk. sources.json is now v2; this test still deep-copies
    the live manifest and sets 2 to exercise the accept path in isolation."""
    v2 = json.loads(json.dumps(manifest))  # deep copy of the live manifest
    v2["schema_version"] = 2

    v = jsonschema.Draft202012Validator(schema)
    errs = list(v.iter_errors(v2))
    assert not errs, [
        f"{'.'.join(str(p) for p in e.absolute_path)}: {e.message}" for e in errs[:5]
    ]

    p = tmp_path / "sources_v2.json"
    p.write_text(json.dumps(v2) + "\n")
    loaded = load_manifest(p)
    assert loaded["schema_version"] == 2


def test_load_manifest_rejects_unknown_schema_version(manifest, tmp_path):
    """`load_manifest` hard-raises for anything outside {1, 2}: bad ints, a
    stringified version, and a missing field."""
    for i, bad_version in enumerate([3, 0, "1"]):
        bad = json.loads(json.dumps(manifest))
        bad["schema_version"] = bad_version
        p = tmp_path / f"sources_bad_{i}.json"
        p.write_text(json.dumps(bad) + "\n")
        with pytest.raises(ValueError, match="schema_version"):
            load_manifest(p)

    missing = json.loads(json.dumps(manifest))
    del missing["schema_version"]
    p = tmp_path / "sources_missing.json"
    p.write_text(json.dumps(missing) + "\n")
    with pytest.raises(ValueError, match="schema_version"):
        load_manifest(p)


def test_export_block_optional_and_validates(manifest, schema):
    """`export` is opt-in (v2): the schema accepts omission, accepts formats and
    a writer priority, and rejects writer-qualified formats and unknown keys."""
    v = jsonschema.Draft202012Validator(schema)

    good = json.loads(json.dumps(manifest))
    good["datasets"][0]["export"] = {"formats": ["parquet", "vortex"], "priority": ["rs", "py"], "notes": None}
    errs = list(v.iter_errors(good))
    assert not errs, [
        f"{'.'.join(str(p) for p in e.absolute_path)}: {e.message}" for e in errs[:5]
    ]
    priority_only = json.loads(json.dumps(manifest))
    priority_only["datasets"][0]["export"] = {"priority": ["rs"]}
    assert not list(v.iter_errors(priority_only))

    for bad in ({"formats": ["parquet@py"]}, {"formats": ["arrow"]}, {"formats": ["parquet"], "bogus": 1}):
        doc = json.loads(json.dumps(manifest))
        doc["datasets"][0]["export"] = bad
        assert list(v.iter_errors(doc)), bad


def test_export_cross_check_names_formats_and_writers(manifest):
    """The cross-check wants formats in export.formats (a writer-qualified name
    points at export.priority), and writers raincloud has in export.priority."""
    ok = json.loads(json.dumps(manifest))
    ok["datasets"][0]["export"] = {"formats": ["parquet", "vortex"], "priority": ["rs", "java", "py"]}
    errs, _ = _cross_checks(ok)
    assert not any("export." in e for e in errs), errs[:5]

    for formats, needle in ((["parquet@rs"], "export.priority picks the writer"), (["arrow"], "not an exported format")):
        doc = json.loads(json.dumps(manifest))
        doc["datasets"][0]["export"] = {"formats": formats}
        errs, _ = _cross_checks(doc)
        assert any("export.formats" in e and needle in e for e in errs), (formats, errs[:5])

    typo = json.loads(json.dumps(manifest))
    typo["datasets"][0]["export"] = {"priority": ["rust"]}
    errs, _ = _cross_checks(typo)
    assert any("export.priority" in e and "'rust'" in e for e in errs), errs[:5]


def test_live_manifest_export_policy_is_writer_priority_only(manifest, schema):
    """The live sources.json is v2. Its export blocks either list both formats
    (`formats: ["parquet", "vortex"]`, the specs whose Vortex writer once failed:
    the build now measures that) or, for the SF100 TPC specs, prefer arrow-rs for
    Parquet only (a per-format priority map); schema and cross-checks pass."""
    assert manifest["schema_version"] == 2
    exports = {d["slug"]: d["export"] for d in manifest["datasets"] if "export" in d}
    assert all(set(e) <= {"formats", "priority", "notes"} for e in exports.values()), exports
    formats = {slug: e["formats"] for slug, e in exports.items() if "formats" in e}
    assert formats and all(f == ["parquet", "vortex"] for f in formats.values()), formats
    priorities = {slug: e["priority"] for slug, e in exports.items() if "priority" in e}
    assert all(p == {"parquet": ["rs", "py"]} for p in priorities.values()), priorities
    sf100 = {d["slug"] for d in manifest["datasets"] if "-sf100-" in d["slug"]}
    assert set(priorities) == sf100 and len(sf100) == 32, sorted(set(priorities) ^ sf100)

    v = jsonschema.Draft202012Validator(schema)
    assert not list(v.iter_errors(manifest))

    schema_errs, _ = _schema_errors(manifest)
    cross_errs, _ = _cross_checks(manifest)
    assert not schema_errs and not cross_errs, {
        "schema_errors": schema_errs[:5],
        "cross_errors": cross_errs[:5],
    }


def test_no_orphan_handlers(manifest):
    """Every registered handler is referenced by ≥1 spec.

    Catches handlers left behind after a slug removal — they're either
    dead code (delete) or about to be wired up (add a spec).
    """
    used = {d["transform"]["handler"] for d in manifest["datasets"] if not d.get("derive")}
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
    """templates/minimal_spec.json is a valid DatasetSpec.

    Catches drift between the example template and the schema.
    """
    raw = json.loads((REPO_ROOT / "templates" / "minimal_spec.json").read_text())
    # The template must validate AS SHIPPED -- the schema is additionalProperties:
    # false, so any explanatory key here breaks every copy-paste of it.
    assert "_comment" not in raw, "minimal_spec.json must not carry non-schema keys"
    fake_manifest = {"schema_version": 2, "datasets": [raw]}
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
        [sys.executable, "-m", "raincloud.pipeline.list_datasets"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    slugs = [line for line in proc.stdout.splitlines() if line]
    assert len(slugs) == len(manifest["datasets"])


def test_list_datasets_filter_by_handler(manifest):
    proc = subprocess.run(
        [sys.executable, "-m", "raincloud.pipeline.list_datasets",
         "--handler", "uci_default", "--count"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    expected = sum(
        1 for d in manifest["datasets"]
        if (d.get("transform") or {}).get("handler") == "uci_default"
    )
    assert int(proc.stdout.strip()) == expected


def test_validate_manifest_cli_exits_zero():
    """End-to-end: the CLI returns 0 on the live manifest."""
    proc = subprocess.run(
        [sys.executable, "-m", "raincloud.pipeline.validate_manifest"],
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
    from raincloud.pipeline.discovery import TAG_VOCAB
    props = schema["$defs"]["DatasetSpec"]["properties"]
    assert set(props["tags"]["items"]["enum"]) == set(TAG_VOCAB)
    assert props["tags"]["uniqueItems"] is True
    assert props["tags"]["maxItems"] == 3


def test_schema_showcase_use_closed_vocab(schema):
    from raincloud.pipeline.discovery import SHOWCASE_TIERS
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
    from raincloud.pipeline.discovery import SHOWCASE_TIERS

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
    from raincloud.pipeline.discovery import SHOWCASE_TIERS

    partial = json.loads(json.dumps(manifest))
    for d in partial["datasets"]:
        d["showcase"] = []
    partial["datasets"][0]["showcase"] = ["encoding"]   # populate exactly one tier

    errors, warnings = _cross_checks(partial)
    # encoding is populated → no warning for it
    assert not any("encoding" in w for w in warnings)
    # every other tier is empty → one warning each
    for tier in SHOWCASE_TIERS:
        if tier == "encoding":
            continue
        assert any(tier in w for w in warnings), f"missing warning for empty tier {tier}"
    # And these are warnings, not errors.
    assert not any("stress" in e for e in errors)


def test_validate_manifest_strict_passes_on_live_manifest():
    """`--strict` must exit 0 against the uncurated live manifest."""
    rc = subprocess.run(
        [sys.executable, "-m", "raincloud.pipeline.validate_manifest", "--strict"],
        cwd=REPO_ROOT,
        capture_output=True,
    )
    assert rc.returncode == 0, (
        f"validate_manifest --strict failed:\n"
        f"stdout:\n{rc.stdout.decode()}\nstderr:\n{rc.stderr.decode()}"
    )
