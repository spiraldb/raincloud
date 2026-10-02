# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Export policy (per-format writer priority, export.formats as the one v2
declaration), manifest cross-check error paths, and catalog-bundle shape checks."""
from __future__ import annotations

import copy
import json

import jsonschema
import pytest

from raincloud import _formats
from raincloud._bundle import build_requirements, validate_documents
from raincloud._formats import (
    DEFAULT_EXPORT_PRIORITY,
    EXPORTED_FORMATS,
    WRITERS,
    buildable_formats,
    export_cells,
    export_formats,
    export_priority,
    resolve_export_cell,
    vortex_cells,
    vortex_skip_reason,
)
from raincloud._registry import exporter_cells
from raincloud.exceptions import CatalogError
from raincloud.pipeline import validate_manifest as vm
from raincloud.pipeline.spec import REPO_ROOT, load_manifest

TEMPLATE = json.loads((REPO_ROOT / "templates" / "minimal_spec.json").read_text())
SCHEMA = json.loads((REPO_ROOT / "sources.schema.json").read_text())


def spec(slug="my-dataset", **blocks):
    out = copy.deepcopy(TEMPLATE)
    out["slug"] = slug
    out.update(blocks)
    return out


def manifest(*specs, version=2, **top):
    return {"schema_version": version, "datasets": list(specs), **top}


def cross_errors(m):
    return vm._cross_checks(m)[0]


def schema_errors(m):
    return [e.message for e in jsonschema.Draft202012Validator(SCHEMA).iter_errors(m)]


def one(errors, *needles):
    assert any(all(n in e for n in needles) for e in errors), errors


# ---------- per-format writer priority ----------

class Cfg:
    export_priority = ("java",)


def test_priority_list_serves_every_format():
    s = {"export": {"priority": ["rs", "py"]}}
    assert export_priority(s, fmt="parquet") == export_priority(s, fmt="vortex") == ("rs", "py")
    assert export_priority(s) == ("rs", "py")


def test_priority_map_serves_named_formats_and_falls_through():
    s = {"export": {"priority": {"parquet": ["rs", "py"]}}}
    assert export_priority(s, fmt="parquet") == ("rs", "py")
    assert export_priority(s, fmt="vortex") == DEFAULT_EXPORT_PRIORITY
    # No format: a map says nothing, so the next level answers.
    assert export_priority(s) == DEFAULT_EXPORT_PRIORITY
    assert export_priority(s, {"export_priority": {"vortex": ["rs"]}}, fmt="vortex") == ("rs",)
    assert export_priority(s, {"export_priority": {"vortex": ["rs"]}}, Cfg(), fmt="parquet") == ("rs", "py")
    assert export_priority(s, {"export_priority": {"vortex": ["rs"]}}, Cfg()) == ("java",)
    assert export_priority({}, {"export_priority": ["rs"]}, Cfg(), fmt="vortex") == ("rs",)


def test_priority_rejects_a_bad_shape():
    with pytest.raises(ValueError, match="export.priority"):
        export_priority({"slug": "x", "export": {"priority": "rs"}}, fmt="parquet")


def test_export_cells_per_format():
    s = {"export": {"priority": {"parquet": ["rs", "py"]}}}
    assert export_cells(s) == ["parquet@rs", "vortex@py", "orc@py", "avro@rs", "nimble@cpp"]
    assert export_cells({"export": {"priority": ["java", "py"]}}) == ["parquet@java", "vortex@py", "orc@py", "avro@java", "nimble@cpp"]
    assert export_cells({}, {"schema_version": 2, "export_priority": {"vortex": ["rs"]}}) == \
        ["parquet@py", "vortex@rs", "orc@py", "avro@rs", "nimble@cpp"]


def test_resolve_export_cell_skips_uninstalled_and_unknown():
    installed = {"parquet@py", "vortex@py"}.__contains__
    assert resolve_export_cell("parquet", ("nope", "rs", "py"), is_available=installed) == "parquet@py"
    assert resolve_export_cell("vortex", ("java",), is_available=installed) is None


def test_writers_derive_from_the_registry():
    cells = {f"{fmt}@{w}" for fmt, writers in WRITERS.items() if fmt != "arrow" for w in writers}
    assert cells == set(exporter_cells())
    assert WRITERS["arrow"] == ("canonical",)
    assert "rs" in WRITERS["vortex"]


def test_sf100_specs_prefer_rs_for_parquet_only():
    m = load_manifest()
    sf100 = [d for d in m["datasets"] if "-sf100-" in d["slug"]]
    assert len(sf100) == 32
    for d in sf100:
        assert d["export"]["priority"] == {"parquet": ["rs", "py"]}, d["slug"]
        assert export_cells(d, m) == ["parquet@rs", "vortex@py", "orc@py", "avro@rs", "nimble@cpp"]


# ---------- export.formats is the one v2 declaration ----------

def test_v2_reads_export_formats_only():
    assert export_formats({}) == list(EXPORTED_FORMATS)
    assert export_formats({"export": {"formats": ["parquet"]}}) == ["parquet"]
    assert vortex_cells({"export": {"formats": ["parquet"]}}, 2) == []
    assert buildable_formats({"export": {"formats": []}}, 2) == {"arrow"}


def test_v1_reads_convert_vortex():
    assert export_formats({"convert": {"vortex": True}}, 1) == ["parquet", "vortex"]
    assert export_formats({}, 1) == ["parquet"]
    assert vortex_cells({"convert": {"vortex": True}}, 1) == ["vortex@py"]
    assert buildable_formats({"convert": {"vortex": False}}, 1) == {"parquet"}
    assert export_cells({"convert": {"vortex": True}}, {"schema_version": 1}) == ["parquet@py", "vortex@py"]


def test_vortex_skip_reason_by_version():
    assert vortex_skip_reason({"export": {"formats": ["parquet"], "notes": "why"}}, 2) == "why"
    assert vortex_skip_reason({"export": {"notes": "about rs"}}, 2) is None
    assert vortex_skip_reason({"convert": {"vortex": False, "vortex_skip_reason": "old"}}, 1) == "old"
    assert vortex_skip_reason({"convert": {"vortex": True, "vortex_skip_reason": None}}, 1) is None


def test_live_manifest_has_no_convert_blocks():
    m = load_manifest()
    assert not [d["slug"] for d in m["datasets"] if "convert" in d]
    # A writer's limitation is measured by the build, never declared as an opt-out.
    assert not [d["slug"] for d in m["datasets"] if not vortex_cells(d, 2)]


def test_v2_parquet_only_with_reason_passes():
    """Reproduced: a parquet-only v2 spec with no convert block was rejected."""
    m = manifest(spec(export={"formats": ["parquet"], "notes": "vortex bug"}))
    assert cross_errors(m) == []
    assert schema_errors(m) == []


def test_v2_parquet_only_needs_no_reason():
    """A deliberate policy omission needs no notes: a writer's technical
    limitation is measured by the build, so no prose is required to go stale."""
    m = manifest(spec(export={"formats": ["parquet"]}))
    assert cross_errors(m) == []
    assert schema_errors(m) == []


def test_v2_rejects_convert_both_ways():
    """Reproduced: convert.vortex=true beside a parquet-only export passed."""
    for convert in ({"vortex": True, "vortex_skip_reason": None},
                    {"vortex": False, "vortex_skip_reason": "x"}):
        m = manifest(spec(convert=convert, export={"formats": ["parquet"], "notes": "n"}))
        one(cross_errors(m), "convert.* is schema_version 1 only")
        assert schema_errors(m), "the schema must reject convert in v2"


def test_v2_rejects_dead_write_fields():
    s = spec()
    s["write"]["output"] = "my-dataset.parquet"
    s["write"]["page_index"] = False
    m = manifest(s)
    one(cross_errors(m), "write.output and write.page_index", "schema_version 1 only")
    assert schema_errors(m)


def test_v1_keeps_convert_pairing():
    s = spec(convert={"vortex": False, "vortex_skip_reason": None})
    s["write"]["output"] = "my-dataset.parquet"
    s["write"]["page_index"] = False
    one(cross_errors(manifest(s, version=1)), "convert.vortex=false requires")
    s["convert"] = {"vortex": True, "vortex_skip_reason": None}
    assert cross_errors(manifest(s, version=1)) == []
    assert schema_errors(manifest(s, version=1)) == []
    one(cross_errors(manifest(spec(export={"formats": ["parquet"], "notes": "n"}), version=1)),
        "export.* is schema_version 2 only")


# ---------- export.priority validation (strict in a manifest) ----------

@pytest.mark.parametrize("priority, needle", [
    (["rs", "pyy"], "'pyy' is not a writer"),
    (["canonical"], "'canonical' is not a writer"),
    ({"vortex": ["java"]}, "export.priority.vortex entry 'java'"),
    ({"csv": ["py"]}, "map must name exported formats"),
    ([], "non-empty list"),
])
def test_priority_names_are_checked(priority, needle):
    one(cross_errors(manifest(spec(export={"priority": priority}))), needle)


def test_catalog_priority_is_checked():
    one(cross_errors(manifest(spec(), export_priority=["rs", "rust"])), "export_priority entry 'rust'")
    assert cross_errors(manifest(spec(), export_priority={"parquet": ["rs", "py"]})) == []
    assert schema_errors(manifest(spec(), export_priority={"parquet": ["rs", "py"]})) == []
    assert schema_errors(manifest(spec(), export_priority="rs"))


def test_priority_map_matches_schema():
    assert schema_errors(manifest(spec(export={"priority": {"parquet": ["rs", "py"]}}))) == []
    assert schema_errors(manifest(spec(export={"priority": {"csv": ["py"]}})))


# ---------- cross-check error paths ----------

def hydrated(slug, parent):
    return {"slug": slug, "short_name": "H", "full_name": "H", "description": "", "license": TEMPLATE["license"],
            "derive": {"from": parent, "hydrate": {"columns": {"url": {"into": "content", "type": "binary"}}}},
            "advisory": "fetched from the web"}


def test_derive_from_unknown_slug():
    one(cross_errors(manifest(spec(), hydrated("ghost-hydrated", "ghost"))),
        "derive.from='ghost' is not a dataset")


def test_derive_from_a_derived_dataset():
    m = manifest(spec(), hydrated("my-dataset-hydrated", "my-dataset"),
                 hydrated("my-dataset-hydrated-hydrated", "my-dataset-hydrated"))
    one(cross_errors(m), "is itself derived")


def test_hydrated_naming_rule():
    one(cross_errors(manifest(spec(), hydrated("fetched", "my-dataset"))), "must be named my-dataset-hydrated")


def test_malformed_blocks_are_errors_not_tracebacks():
    errors = cross_errors(manifest(spec(), {**hydrated("x-hydrated", "my-dataset"), "derive": "x"},
                                   spec("broken", fetch="http"), "not-a-spec"))
    one(errors, "x-hydrated: derive must be an object")
    one(errors, "broken: fetch must be an object")
    one(errors, "datasets[3]: must be an object")


def generated(**fetch):
    base = {"type": "generated", "generator": "duckdb-tpch", "version": "1.5.5", "parameters": {"sf": 1},
            "output": "nation", "urls": [], "auth": None, "expected_bytes": None, "expected_sha256": None}
    return spec("gen", fetch={**base, **fetch}, parse={"reader": "parquet", "options": {}},
                transform={"handler": "identity", "params": {}})


@pytest.mark.parametrize("fetch, needle", [
    ({"version": ""}, "nonempty version"),
    ({"generator": "nope"}, "unknown generator 'nope'"),
    ({"output": "planets"}, "unknown generated output 'planets'"),
    ({"parameters": {"sf": -1}}, "finite positive"),
    ({"parameters": {"sf": float("nan")}}, "invalid generated fetch"),
])
def test_generated_fetch_errors(fetch, needle):
    one(cross_errors(manifest(generated(**fetch))), "gen: invalid generated fetch", needle)


def test_generator_type_error_is_reported(monkeypatch):
    from raincloud.pipeline.generators import REGISTRY

    class Picky:
        outputs = {"nation": "nation.parquet"}

        @staticmethod
        def validate(parameters):
            raise TypeError("sf must be a number")

    monkeypatch.setitem(REGISTRY, "picky", Picky())
    one(cross_errors(manifest(generated(generator="picky"))), "invalid generated fetch", "sf must be a number")


# ---------- the template matches the manifest convention ----------

def test_template_write_matches_manifest_convention():
    m = load_manifest()
    conventions = {json.dumps(d["write"], sort_keys=True) for d in m["datasets"] if "write" in d}
    assert conventions == {json.dumps(TEMPLATE["write"], sort_keys=True)}
    assert TEMPLATE["write"]["row_group_size_rows"] == 10_000_000


def test_template_is_a_valid_v2_spec():
    m = manifest(copy.deepcopy(TEMPLATE))
    assert schema_errors(m) == []
    assert cross_errors(m) == []


# ---------- catalog bundles reject malformed shapes ----------

SNAPSHOT = {"schema_version": 2, "slugs": {}}


@pytest.mark.parametrize("specs, needle", [
    ([{"slug": "a", "derive": "x"}], "derive must be an object"),
    ([{"slug": "a", "license": "MIT"}], "license must be an object"),
    ([{"slug": "a", "derive": {"from": "b", "hydrate": {}}},
      {"slug": "b", "derive": {"from": "a", "hydrate": {}}}], "not itself derived"),
    ([{"slug": "a", "derive": {"from": "a", "hydrate": {}}}], "not itself derived"),
    ([{"slug": "a", "derive": {"from": "ghost", "hydrate": {}}}], "derive.from"),
    ([{"slug": "p"}, {"slug": "a", "derive": {"from": "p", "hydrate": "x"}}], "derive.hydrate must be an object"),
    ([{"slug": "a", "export": {"priority": {"csv": ["py"]}}}], "export.priority map"),
    ([{"slug": "a", "export": {"priority": {"parquet": "rs"}}}], "export.priority must be"),
])
def test_bundle_rejects(specs, needle):
    with pytest.raises(CatalogError, match=needle):
        validate_documents(manifest(*specs), SNAPSHOT)


def test_bundle_rejects_bad_catalog_priority_and_columns():
    with pytest.raises(CatalogError, match="export_priority"):
        validate_documents(manifest({"slug": "a"}, export_priority=[1]), SNAPSHOT)
    with pytest.raises(CatalogError, match="columns"):
        validate_documents(manifest({"slug": "a"}), {"schema_version": 2, "slugs": {"a": {"columns": ["x"]}}})


def test_bundle_accepts_v1_convert_and_priority_maps():
    validate_documents(manifest({"slug": "a", "convert": {"vortex": True}}, version=1),
                       {"schema_version": 1, "slugs": {}})
    m = manifest({"slug": "a", "export": {"priority": {"parquet": ["rs", "py"]}}},
                 export_priority={"vortex": ["rs"]})
    validate_documents(m, SNAPSHOT)
    assert "exporter:parquet@rs" in build_requirements(m)
    assert "exporter:vortex@rs" in build_requirements(m)


# ---------- say which manifest was validated ----------

def test_main_names_the_manifest(tmp_path, capsys):
    path = tmp_path / "mine.json"
    path.write_text(json.dumps(manifest(spec())))
    assert vm.main([str(path)]) == 0
    out = capsys.readouterr().out
    assert out.splitlines()[0] == f"validating {path} (1 datasets)"
    assert vm.main([str(path), "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["manifest"] == str(path)


def test_main_names_the_default_manifest(capsys):
    assert vm.main([]) == 0
    assert str(REPO_ROOT / "sources.json") in capsys.readouterr().out.splitlines()[0]


def test_formats_module_stays_lightweight():
    # resolve_export_cell no longer imports the pipeline for a default checker.
    import inspect
    source = inspect.getsource(_formats)
    assert "from .pipeline" not in source and "from raincloud.pipeline" not in source


def test_released_v2_catalog_with_convert_still_reads():
    # A v2 catalog released before convert.* became v1-only keeps reading, and
    # its convert.vortex=false opt-out still means no Vortex.
    from raincloud._bundle import encode, make_bundle
    manifest = {"schema_version": 2, "datasets": [
        {"slug": "old-optout", "convert": {"vortex": False, "vortex_skip_reason": "too wide"}},
        {"slug": "old-default", "convert": {"vortex": True}}]}
    make_bundle(encode(manifest), encode({"schema_version": 2, "slugs": {}}), "released")
    assert export_formats(manifest["datasets"][0], 2) == ["parquet"]
    assert export_formats(manifest["datasets"][1], 2) == list(EXPORTED_FORMATS)


def test_recipe_keys_only_grow():
    # Dropping a key re-fingerprints every released catalog that carries it.
    from raincloud._bundle import _RECIPE_KEYS
    assert {"slug", "fetch", "extract", "parse", "transform", "write", "expect", "convert",
            "export", "hydrate", "derive"} <= set(_RECIPE_KEYS)


# ---------- one priority shape rule, and the catalog's priority reaches every view ----------

def test_vortex_cells_honours_the_catalog_priority():
    s = {"slug": "a"}
    catalog = {"schema_version": 2, "export_priority": {"vortex": ["rs"]}}
    assert vortex_cells(s, 2, catalog) == ["vortex@rs"] == [c for c in export_cells(s, catalog) if c.startswith("vortex@")]
    assert vortex_cells(s, 2) == ["vortex@py"]
    assert vortex_cells({"export": {"formats": ["parquet"]}}, 2, catalog) == []


def test_empty_priority_is_refused_everywhere():
    from raincloud._formats import priority_shape_error
    for value in ([], {}, {"parquet": []}):
        assert priority_shape_error(value, "x") is not None
        with pytest.raises(ValueError, match="export.priority"):
            export_priority({"slug": "a", "export": {"priority": value}}, fmt="parquet")
        with pytest.raises(CatalogError, match="export.priority"):
            validate_documents(manifest({"slug": "a", "export": {"priority": value}}), SNAPSHOT)
    # The machine's parsed setting is a tuple; an empty one is unset, not an error.
    class Unset:
        export_priority = ()
    assert export_priority({}, None, Unset(), fmt="parquet") == DEFAULT_EXPORT_PRIORITY


def test_a_priority_naming_no_writer_for_a_format_falls_back_to_the_default():
    assert export_cells({"export": {"priority": ["java"]}}) == ["parquet@java", "vortex@py", "orc@py", "avro@java", "nimble@cpp"]


def test_format_sets_derive_from_the_writers():
    from raincloud._formats import ALL_FORMATS, EXPORTED_FORMATS
    assert set(EXPORTED_FORMATS) == {base for base in WRITERS if base != "arrow"} == {"parquet", "vortex", "orc", "avro", "nimble"}
    assert set(ALL_FORMATS) == set(WRITERS)


def test_the_built_in_order_names_a_writer_for_every_format():
    """A dataset with no priority still exports every format it offers."""
    from raincloud._formats import EXPORTED_FORMATS
    for fmt in EXPORTED_FORMATS:
        assert set(WRITERS[fmt]) & set(DEFAULT_EXPORT_PRIORITY), fmt


def test_every_artifact_format_is_declared_once():
    """A format is declared in `_registry.FORMATS`; the extension map, reader
    capabilities and `auto` order are derived from it, never restated."""
    from raincloud._cache import EXT
    from raincloud._formats import ALL_FORMATS, AUTO_FORMATS
    from raincloud._readers import reader_capabilities
    from raincloud._registry import FORMATS
    assert set(ALL_FORMATS) <= set(FORMATS)
    assert set(EXT) == set(reader_capabilities()) == set(FORMATS)
    assert AUTO_FORMATS == ("vortex", "parquet", "arrow")


def test_the_schema_names_exactly_the_exported_formats():
    """sources.schema.json cannot import the registry, so this is its gate: a
    format added to (or removed from) the registry must be added to the schema's
    `export.formats` and `export.priority` enums too."""
    from raincloud._formats import EXPORTED_FORMATS
    defs = SCHEMA["$defs"]
    assert defs["Export"]["properties"]["formats"]["items"]["enum"] == list(EXPORTED_FORMATS)
    by_format = next(option for option in defs["WriterPriority"]["oneOf"] if option.get("type") == "object")
    assert by_format["propertyNames"]["enum"] == list(EXPORTED_FORMATS)
