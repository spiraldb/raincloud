# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""The docs and examples do what they say.

- Each example runs hermetically against a tiny catalog of synthetic file://
  sources: a miss prints the `raincloud build` hint and exits 1 with no
  traceback, `--build` builds and answers, and a second run reads the prepared
  file.
- The README's Python quick start runs as written against a built fixture.
- The README's Rust snippet names only what `clients/rust/src/lib.rs` exports.
- Every `python -m raincloud.pipeline.X --flag` and `raincloud CMD --flag` in the
  prose names a real module, command and flag.
- The C/C++ package version agrees with `raincloud.__version__`.
- sources.schema.md's reference DatasetSpec validates against sources.schema.json,
  and the schema's own descriptions carry none of the removed spellings.
- A few stated behaviours hold: publish's exit codes, hydrate's bare-parent
  alias and sample options, the remove-dataset lookup's lock, and a manifest
  override beside a config file that selects a catalog.
"""
from __future__ import annotations

import importlib.util
import json
import re
import textwrap
from pathlib import Path

import pytest

import raincloud

ROOT = Path(__file__).resolve().parents[1]
EXAMPLES = ROOT / "examples"

# Prose whose commands must stay runnable. CHANGELOG is history, so it is not here.
DOCS = [ROOT / name for name in ("README.md", "AGENTS.md", "SKILLS.md", "HYDRATING.md",
                                 "DISCLAIMER.md", "CONTRIBUTING.md", "sources.schema.md")]
DOCS += [ROOT / "templates/README.md", EXAMPLES / "README.md",
         *sorted((ROOT / ".agents/skills").glob("*/SKILL.md")), ROOT / ".agents/skills/README.md"]


# --------------------------------------------------------------------------- fixture catalog

def _spec(slug: str, csv: Path, rows: int) -> dict:
    return {
        "slug": slug, "short_name": slug, "full_name": slug,
        "description": "synthetic stand-in for a documented example",
        "license": {"spdx": "CC0-1.0"},
        "fetch": {"type": "http", "urls": [csv.as_uri()]},
        "extract": {"type": "passthrough"},
        "parse": {"reader": "csv"},
        "transform": {"handler": "tighten_types"},
        "expect": {"rows": rows},
        "export": {"formats": ["parquet"]},
    }


# slug -> (example module, CSV body, text the answer must contain)
EXAMPLE_DATA = {
    "kepler-exoplanet-search-results": (
        "kepler_exoplanets.py",
        "kepoi_name,kepler_name,koi_disposition,koi_prad,koi_period\n"
        "K1,Kepler-1 b,CONFIRMED,2.5,10.0\nK2,,CONFIRMED,0.5,3.0\nK3,,FALSE POSITIVE,9.0,1.0\n",
        "smallest CONFIRMED planet by radius"),
    "uci-wine-quality": (
        "wine_quality_correlations.py",
        "fixed_acidity,alcohol,quality,color\n7.4,9.4,5,red\n7.8,9.8,5,red\n6.3,12.8,8,white\n6.9,11.0,6,white\n",
        "strongest positive: alcohol"),
    "yellow_tripdata_2025": (
        "nyc_taxi_tip_rate.py",
        "payment_type,fare_amount,tip_amount\n1,12.5,2.0\n2,8.0,0.0\n1,20.0,0.0\n2,1.0,0.0\n",
        "left no recorded tip"),
    "120-years-of-olympic-history-athletes-and-results": (
        "olympic_medals.py",
        "NOC,Year,Medal\nUSA,1996,Gold\nUSA,2000,Silver\nGBR,1908,\nFRA,1924,Bronze\n",
        "medals awarded per decade"),
}


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    """A manifest holding every example's slug, nothing built, no mirror."""
    specs = []
    for slug, (_, body, _) in EXAMPLE_DATA.items():
        csv = tmp_path / "upstream" / f"{slug}.csv"
        csv.parent.mkdir(parents=True, exist_ok=True)
        csv.write_text(body)
        specs.append(_spec(slug, csv, body.count("\n") - 1))
    iris = tmp_path / "upstream" / "iris.csv"
    iris.write_text("sepal_length,sepal_width,petal_length,petal_width,class\n"
                    "5.1,3.5,1.4,0.2,setosa\n7.0,3.2,4.7,1.4,versicolor\n6.3,3.3,6.0,2.5,virginica\n")
    specs.append(_spec("uci-iris", iris, 3))
    manifest, snapshot = tmp_path / "sources.json", tmp_path / "snapshot.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": specs}))
    snapshot.write_text(json.dumps({"schema_version": 2, "slugs": {}}))
    for key, path in {"MANIFEST": manifest, "SNAPSHOT": snapshot,
                      "HOME": tmp_path / "home", "CACHE": tmp_path / "cache"}.items():
        monkeypatch.setenv(f"RAINCLOUD_{key}", str(path))
    for key in ("MIRROR", "OFFLINE", "OUTPUTS", "WORKDIR", "RAW_DOWNLOADS", "CATALOG"):
        monkeypatch.delenv(f"RAINCLOUD_{key}", raising=False)
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    yield tmp_path
    _catalog.load_catalog.cache_clear()


def _example(name: str):
    spec = importlib.util.spec_from_file_location(f"example_{Path(name).stem}", EXAMPLES / name)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------- examples

@pytest.mark.parametrize("slug", list(EXAMPLE_DATA))
def test_example_miss_prints_build_hint(slug, catalog, capsys):
    pytest.importorskip("pandas")
    pytest.importorskip("duckdb")
    module_name, _, _ = EXAMPLE_DATA[slug]
    assert _example(module_name).main([]) == 1
    err = capsys.readouterr().err
    assert "ArtifactNotFound" in err, err
    assert f"raincloud build {slug}" in err, err
    assert "Traceback" not in err


@pytest.mark.parametrize("slug", list(EXAMPLE_DATA))
def test_example_builds_on_request_then_reads_prepared(slug, catalog, capsys):
    pytest.importorskip("pandas")
    pytest.importorskip("duckdb")
    module_name, _, expected = EXAMPLE_DATA[slug]
    module = _example(module_name)
    assert module.main(["--build"]) == 0, capsys.readouterr()
    assert expected in capsys.readouterr().out
    # The second run is the documented steady state: a plain read, no build.
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    assert module.main([]) == 0, capsys.readouterr()
    assert expected in capsys.readouterr().out


def test_use_loader_materialize_miss_is_a_hint_not_a_traceback(catalog, capsys):
    module = _example("use_loader.py")
    assert module.main(["--slug", "uci-iris", "--materialize"]) == 0
    captured = capsys.readouterr()
    assert "[materialize] skipped: ArtifactNotFound" in captured.err, captured.err
    assert "raincloud build uci-iris" in captured.err
    assert "UnknownSlug caught" in captured.out


def _fenced_blocks(text: str, language: str) -> list[str]:
    return re.findall(rf"```{language}\n(.*?)```", text, flags=re.S)


def test_readme_python_quick_start_runs(catalog, capsys):
    pytest.importorskip("pandas")
    pytest.importorskip("duckdb")
    raincloud.load("uci-iris", format="parquet", build=True).path()
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()
    readme = (ROOT / "README.md").read_text()
    getting_data = readme[readme.index("## Getting data"):readme.index("### From other languages")]
    quick_start = _fenced_blocks(getting_data, "python")[0]
    exec(compile(quick_start, "README.md#getting-data", "exec"), {})
    assert "3" in capsys.readouterr().out


# --------------------------------------------------------------------------- Rust snippet

def test_readme_rust_snippet_uses_the_real_api():
    lib = (ROOT / "clients/rust/src/lib.rs").read_text()
    exported = set(re.findall(r"^pub (?:struct|enum|fn) (\w+)", lib, flags=re.M))
    for target in re.findall(r"^pub use ([^;]+);", lib, flags=re.M):
        if "{" in target:
            exported |= {name.strip() for name in target.split("{", 1)[1].rstrip("}").split(",")}
        else:
            exported.add(target.rsplit("::", 1)[-1])
    methods = set(re.findall(r"pub fn (\w+)", lib)) | set(
        re.findall(r"pub fn (\w+)", (ROOT / "clients/rust/src/reader.rs").read_text()))
    for doc in (ROOT / "README.md", ROOT / "clients/README.md"):
        for block in _fenced_blocks(doc.read_text(), r"rust[^\n]*"):
            for group in re.findall(r"use raincloud_reader::\{([^}]*)\}", block):
                for name in (n.strip() for n in group.split(",")):
                    assert name in exported, f"{doc.name}: raincloud_reader exports no {name}"
            for name in re.findall(r"raincloud_reader::(\w+)", block):
                assert name in exported, f"{doc.name}: raincloud_reader exports no {name}"
            for method in re.findall(r"Dataset::(\w+)", block):
                assert method in methods, f"{doc.name}: Dataset has no {method}"
            # Dataset::load takes the options by reference: (&Value, slug, format).
            for args in re.findall(r"Dataset::load\(([^;]*)\)\?", block):
                assert args.startswith("&"), f"{doc.name}: Dataset::load takes &Value first: {args}"


# --------------------------------------------------------------------------- commands in prose

def _code_lines(text: str) -> list[str]:
    lines = []
    for block in re.findall(r"```[^\n]*\n(.*?)```", text, flags=re.S):
        lines += block.splitlines()
    outside = re.sub(r"```[^\n]*\n.*?```", "", text, flags=re.S)
    lines += re.findall(r"`([^`\n]+)`", outside)
    return lines


def _command_tail(rest: str) -> str:
    return re.split(r"\s(?:#|&&|\||;|>)", rest, maxsplit=1)[0]


def _flags(rest: str) -> list[str]:
    return re.findall(r"(?<![\w\-])(--[a-z][a-z0-9\-]*)", _command_tail(rest))


def _module_source(module: str) -> str | None:
    base = ROOT / "raincloud" / "pipeline" / Path(*module.split("."))
    for candidate in (base.with_suffix(".py"), base / "__main__.py"):
        if candidate.is_file():
            return candidate.read_text()
    return None


CLI = (ROOT / "raincloud/cli.py").read_text()
# Subcommands the CLI hands to a pipeline parser; their flags live there.
_PASSTHROUGH = {"list": "list_datasets", "build": "build", "browse": "browse"}


def _commands():
    for doc in DOCS:
        for line in _code_lines(doc.read_text()):
            for module, rest in re.findall(r"python -m raincloud\.pipeline\.([\w.]+)(.*)", line):
                yield doc, ("module", module, rest)
            # `from raincloud import ...` is Python, not a command.
            for command, rest in re.findall(r"(?:^|[\s(])(?<!from )raincloud ([a-z][\w-]*)(.*)", line):
                yield doc, ("cli", command, rest)


def test_documented_commands_exist():
    commands = set(re.findall(r'command\("([a-z]+)"', CLI))
    aliases = set(re.findall(r'"(\w+)": "\w+"', CLI.split("_ALIASES = ", 1)[1].split("\n", 1)[0]))
    problems = []
    for doc, (kind, name, rest) in _commands():
        if kind == "module":
            source = _module_source(name)
            if source is None:
                problems.append(f"{doc.relative_to(ROOT)}: no module raincloud.pipeline.{name}")
                continue
        else:
            if name not in commands | aliases:
                problems.append(f"{doc.relative_to(ROOT)}: no command `raincloud {name}`")
                continue
            source = _module_source(_PASSTHROUGH[name]) if name in _PASSTHROUGH else CLI
            if name == "build":
                source = (source or "") + CLI
        for flag in _flags(rest):
            # A flag is spelled out, or generated from its bare name (`f"--{name}"`).
            if not any(f"{q}{spelling}{q}" in source for q in "\"'" for spelling in (flag, flag[2:])):
                problems.append(f"{doc.relative_to(ROOT)}: `{kind} {name}` has no flag {flag}")
    assert not problems, "\n".join(problems)


BANNED = {
    r"scripts[./]pipeline": "the pipeline moved to raincloud.pipeline",
    r"(?:register(?:ed)?(?: it)?|registry(?: lives)?) in `?(?:raincloud/pipeline/)?handlers/__init__\.py":
        "handlers are declared in HANDLERS in raincloud/_registry.py",
    r"> /tmp/": "logs of long runs must not live on tmpfs",
    r"\.scan\(\)|`\.scan`": "Dataset.scan() was replaced by Dataset.dataset()",
    r"raincloud_reader::Config|Config::from_options": "the Rust client has no Config type",
    r"huggingface-cli": "huggingface-hub 1.x removed huggingface-cli; it is `hf auth login` (or HF_TOKEN)",
    r"<recipe-fingerprint>|<fetch-fingerprint>": "the scratch keys are <recipe-hash> and <fetch-key>",
    r"allowed_bypass|blocked_by_urlhaus": "hydrate provenance records only allowed, blocked_scheme, "
                                          "blocked_by_host and fetch_error",
}


def _schema_descriptions(node) -> list[str]:
    if isinstance(node, dict):
        return [text for key, value in node.items()
                for text in ([value] if key == "description" and isinstance(value, str)
                             else _schema_descriptions(value))]
    if isinstance(node, list):
        return [text for item in node for text in _schema_descriptions(item)]
    return []


def test_prose_carries_no_removed_spellings():
    schema = json.loads((ROOT / "sources.schema.json").read_text())
    texts = [(doc.relative_to(ROOT), doc.read_text()) for doc in DOCS]
    texts += [("sources.schema.json descriptions", "\n".join(_schema_descriptions(schema)))]
    problems = [f"{where}: {why} ({pattern})"
                for where, text in texts for pattern, why in BANNED.items()
                if re.search(pattern, text)]
    assert not problems, "\n".join(problems)


# --------------------------------------------------------------------------- sources.schema.md

def _strip_jsonc(text: str) -> str:
    """JSON from a commented example: drop // and /* */ comments outside strings,
    then trailing commas."""
    out, i, in_string = [], 0, False
    while i < len(text):
        c = text[i]
        if in_string:
            out.append(c)
            if c == "\\":
                out.append(text[i + 1])
                i += 1
            elif c == '"':
                in_string = False
        elif c == '"':
            in_string = True
            out.append(c)
        elif text.startswith("//", i):
            i = text.find("\n", i) - 1 if "\n" in text[i:] else len(text)
        elif text.startswith("/*", i):
            i = text.index("*/", i) + 1
        else:
            out.append(c)
        i += 1
    return re.sub(r",(\s*[}\]])", r"\1", "".join(out))


def test_schema_md_reference_spec_validates():
    """The DatasetSpec reference example, copied as documented, is a valid v2 spec."""
    jsonschema = pytest.importorskip("jsonschema")
    from raincloud.pipeline.validate_manifest import _cross_checks
    text = (ROOT / "sources.schema.md").read_text()
    section = text[text.index("## `DatasetSpec`"):]
    spec = json.loads(_strip_jsonc(_fenced_blocks(section, "jsonc")[0]))
    top = json.loads(_strip_jsonc(_fenced_blocks(text, "jsonc")[0]))
    manifest = {**top, "datasets": [spec]}
    schema = json.loads((ROOT / "sources.schema.json").read_text())
    errors = [e.message for e in jsonschema.Draft202012Validator(schema).iter_errors(manifest)]
    assert not errors, errors
    errors, _ = _cross_checks(manifest)
    assert not errors, errors


# --------------------------------------------------------------------------- version literals

def test_cmake_package_version_matches_release():
    cmake = (ROOT / "clients/c/CMakeLists.txt").read_text()
    literal = re.search(r"project\(\s*raincloud\s+VERSION\s+([0-9][0-9.]*)", cmake)
    if literal is None:
        pytest.skip("CMakeLists.txt derives its version rather than carrying a literal")
    assert literal.group(1) == raincloud.__version__


# --------------------------------------------------------------------------- stated contracts

def test_agents_row_group_defaults_match_the_code(monkeypatch):
    from raincloud.pipeline import spec
    table = (ROOT / "AGENTS.md").read_text()
    for var, read, default, spelled in (
        ("RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES", spec.row_group_target_encoded_bytes, 128 << 20, "128 MiB"),
        ("RAINCLOUD_ROW_GROUP_TARGET_BYTES", spec.row_group_target_bytes, 512 << 20, "512 MiB"),
        ("RAINCLOUD_ROW_GROUP_MAX_ROWS", spec.row_group_max_rows, 10_000_000, "10,000,000"),
        ("RAINCLOUD_ROW_GROUP_PROBE_ROWS", spec.row_group_probe_rows, 262_144, "262,144"),
    ):
        monkeypatch.delenv(var, raising=False)
        assert read() == default, var
        row = next(line for line in table.splitlines() if line.startswith(f"| `{var}`"))
        assert row.rstrip(" |").endswith(spelled), row


def test_hydrate_bypass_disables_every_layer_as_documented():
    """HYDRATING.md: the scheme allowlist and blocked_hosts_extra apply unless
    the two-flag bypass is active, and the bypass disables every layer."""
    from raincloud.pipeline.hydrate import FilterDecision, HydrateConfig, filter_url
    guarded = HydrateConfig(blocked_hosts=frozenset({"banned.example"}))
    assert filter_url("file:///etc/passwd", guarded) == (False, FilterDecision.BLOCKED_SCHEME)
    assert filter_url("https://banned.example/x", guarded) == (False, FilterDecision.BLOCKED_BY_HOST)
    assert filter_url("https://ok.example/x", guarded) == (True, FilterDecision.ALLOWED)
    bypass = HydrateConfig(blocked_hosts=frozenset({"banned.example"}), bypass_safety=True, risk_accepted=True)
    for url in ("file:///etc/passwd", "https://banned.example/x"):
        assert filter_url(url, bypass) == (True, FilterDecision.ALLOWED_BYPASS)


# --------------------------------------------------------------------------- stated behaviours

def _exit_code(main, argv) -> int:
    try:
        return main(argv)
    except SystemExit as exc:
        return exc.code


def test_publish_exit_codes_match_the_skill(catalog, tmp_path, capsys):
    """raincloud-publish's failure table: a named slug with nothing built is
    refused (exit 1); an unknown slug exits 2 with a did-you-mean."""
    from raincloud.pipeline import publish
    skill = (ROOT / ".agents/skills/raincloud-publish/SKILL.md").read_text()
    assert "`refusing to publish: nothing built for <slug> under <root>` (exit 1)" in skill
    mirror = ["--mirror", (tmp_path / "mirror").as_uri(), "--dry-run"]
    assert _exit_code(publish.main, ["uci-iris", *mirror]) == 1
    assert "refusing to publish: nothing built for uci-iris" in capsys.readouterr().err
    assert _exit_code(publish.main, ["uci-irs", *mirror]) == 2
    assert "Did you mean uci-iris?" in capsys.readouterr().err


def _add_hydrated(catalog, parent: str) -> None:
    manifest_path = catalog / "sources.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["datasets"].append({
        "slug": f"{parent}-hydrated", "short_name": "h", "full_name": "h",
        "description": "a hydrated fixture", "license": {"spdx": "CC0-1.0"},
        "derive": {"from": parent, "hydrate": {"columns": {"class": {"into": "content", "type": "string"}}}},
        "advisory": "a fixture",
    })
    manifest_path.write_text(json.dumps(manifest))
    from raincloud import _catalog
    _catalog.load_catalog.cache_clear()


def test_hydrate_accepts_the_bare_parent_and_refuses_plain_datasets(catalog, capsys):
    """The hydrate skill and HYDRATING.md: `hydrate <parent>` names
    `<parent>-hydrated`; a dataset with no hydrated entry is refused (exit 2)."""
    from raincloud.pipeline import hydrate
    assert "A bare `<parent>` is accepted too" in (ROOT / ".agents/skills/raincloud-hydrate/SKILL.md").read_text()
    assert "`hydrate <parent>` names the same dataset" in (ROOT / "HYDRATING.md").read_text()
    _add_hydrated(catalog, "uci-iris")
    # The parent is not prepared, so the sample fails, but only after the alias selected it.
    assert _exit_code(hydrate.main, ["uci-iris", "--limit", "1"]) == 1
    assert "FAILED: uci-iris-hydrated" in capsys.readouterr().err
    assert _exit_code(hydrate.main, ["uci-wine-quality"]) == 2
    assert "not a hydrated dataset: uci-wine-quality" in capsys.readouterr().err


@pytest.mark.parametrize("option", [["--limit", "1"], ["--max-bytes", "1"], ["--timeout", "5"],
                                    ["--urlhaus"]])
def test_hydrate_options_that_change_the_rows_make_a_sample(option, catalog, monkeypatch, capsys):
    """HYDRATING.md lists the options that make a run a sample; each one does."""
    from raincloud.pipeline import hydrate
    flag = option[0]
    rule = (ROOT / "HYDRATING.md").read_text().split("make the run a **sample**", 1)[0]
    assert f"`{flag}`" in rule, flag
    monkeypatch.setattr(hydrate, "fetch_urlhaus_hostlist", lambda **_: set())  # no network
    _add_hydrated(catalog, "uci-iris")
    _exit_code(hydrate.main, ["uci-iris-hydrated", *option])
    assert "writing a sample" in capsys.readouterr().err, flag


def test_remove_dataset_lookup_holds_the_operation_lock(catalog):
    """The remove-dataset snippet, as documented, runs and resolves the paths
    while the store, raw and scratch locks are held."""
    from raincloud.config import get_config
    from raincloud.pipeline import lifecycle
    for doc, slug in ((ROOT / ".agents/skills/raincloud-remove-dataset/SKILL.md", "SLUG"),
                      (ROOT / "SKILLS.md", "my-dataset")):
        text = doc.read_text()
        snippet = next(b for b in _fenced_blocks(text, "python") if "operation_lock" in b)
        held, printed = [], []

        def record(value, _held=held, _printed=printed):
            _held.append(lifecycle._held.get())
            _printed.append(value)

        source = textwrap.dedent(snippet)  # the fence is indented under a list item
        exec(compile(source.replace(f'"{slug}"', '"uci-iris"'), str(doc), "exec"), {"print": record})
        config = get_config()
        roots = {config.data_dir.resolve(), config.raw_dir.resolve(), config.scratch_dir.resolve()}
        assert len(printed) == 3 and all(roots <= h for h in held), (doc, held)


def test_manifest_override_beside_a_config_catalog_needs_catalog_local(catalog, tmp_path, monkeypatch):
    """README "Adding your own": when a config file selects a catalog, a
    manifest override also needs RAINCLOUD_CATALOG=local."""
    from raincloud import config as config_module
    from raincloud.catalogs import resolve_context
    from raincloud.exceptions import CatalogError
    machine = tmp_path / "etc" / "config.toml"
    machine.parent.mkdir()
    machine.write_text(f'[raincloud]\ncatalog = "{tmp_path / "packs"}"\n')
    monkeypatch.setattr(config_module, "system_config_paths", lambda: [machine])
    monkeypatch.setattr(config_module, "config_path", lambda: tmp_path / "no-user-config.toml")
    monkeypatch.delenv("RAINCLOUD_NO_CONFIG", raising=False)
    monkeypatch.delenv("RAINCLOUD_CONFIG", raising=False)
    install = tmp_path / "install"  # not a checkout, so the machine config applies
    install.mkdir()
    assert "RAINCLOUD_CATALOG=local" in (ROOT / "README.md").read_text()
    with pytest.raises(CatalogError, match="OR a manifest override"):
        resolve_context(config_module.resolve_config(repo_root=install), repo_root=install)
    monkeypatch.setenv("RAINCLOUD_CATALOG", "local")
    context = resolve_context(config_module.resolve_config(repo_root=install), repo_root=install)
    assert context.source == "local"
    assert [d["slug"] for d in context.manifest["datasets"]][-1] == "uci-iris"


def test_v1_profile_fallback_is_still_needed():
    """Tripwire named by the raincloud-profile skill: a checkout reads the frozen
    docs/v1/profiles/ only for slugs with no v2 profile. Once docs/v2/profiles/
    covers the catalog, delete that fallback (promote_profiles.profile_search_paths,
    and the browse docstring describing it) and this test."""
    manifest = json.loads((ROOT / "sources.json").read_text())
    profiles = ROOT / "docs" / f"v{manifest['schema_version']}" / "profiles"
    missing = [d["slug"] for d in manifest["datasets"] if not (profiles / f"{d['slug']}.json").is_file()]
    assert missing, f"{profiles} covers the catalog: remove the docs/v1/profiles fallback"


def test_use_loader_unreachable_mirror_is_a_hint_not_a_traceback(catalog, monkeypatch, capsys):
    pytest.importorskip("aiohttp")
    monkeypatch.setenv("RAINCLOUD_MIRROR", "http://127.0.0.1:9/raincloud")  # nothing listens there
    module = _example("use_loader.py")
    assert module.main(["--slug", "uci-iris", "--materialize"]) == 0
    err = capsys.readouterr().err
    assert "[materialize] skipped: MirrorUnavailable" in err, err
    assert "check RAINCLOUD_MIRROR" in err
