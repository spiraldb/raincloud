# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Loader, config, catalog lifecycle and CLI contracts."""
import json
import os
import shutil
import threading
from dataclasses import replace
from pathlib import Path

import pytest

import raincloud
from raincloud import _cache, _catalog, _extras, _resolve, catalogs
from raincloud._bundle import encode, make_bundle
from raincloud._catalog import Entry, FormatInfo
from raincloud._locking import atomic_write, is_held, locked
from raincloud.cli import main
from raincloud.config import _ENV, resolve_config, use_config
from raincloud.exceptions import (
    ArtifactNotFound,
    CatalogError,
    CorruptArtifact,
    MirrorUnavailable,
    MissingDependency,
    OfflineMiss,
    UnknownColumn,
)


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    for name in tuple(os.environ):
        if name.startswith("RAINCLOUD_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("RAINCLOUD_NO_CONFIG", "1")


@pytest.fixture
def fixture(tmp_path):
    pytest.importorskip("vortex")
    from tests.reader_fixture import create
    _, options = create(tmp_path)
    cfg = resolve_config(no_config=True, **{k: v for k, v in options.items() if k != "no_config"})
    return options, cfg


def catalog_dir(root: Path, slug="tiny", catalog_id="loader") -> Path:
    manifest = {"schema_version": 2, "datasets": [{"slug": slug, "export": {"formats": ["parquet"]}}]}
    bundle = make_bundle(encode(manifest), encode({"schema_version": 2, "slugs": {}}), catalog_id)
    root.mkdir(parents=True, exist_ok=True)
    for name, raw in bundle.files().items():
        (root / name).write_bytes(raw)
    return root


def cli(capsys, options, *args):
    code = main(["--settings", json.dumps(options), *args])
    out = capsys.readouterr()
    return code, out.out, out.err


# ---------- a child build receives settings it can parse back ----------

@pytest.mark.parametrize("key", sorted(_ENV))
@pytest.mark.parametrize("empty", [False, True])
def test_subprocess_env_round_trips_every_setting(tmp_path, monkeypatch, key, empty):
    values = {"export_priority": "rs,py", "offline": True, "retry_errors": True, "mirror": "s3://bucket/prefix",
              "catalog": "checkout", "catalog_url": "https://example.com/catalogs", "formats": "parquet,vortex",
              "keep_raw": True, "keep_canonical": True}
    value = values.get(key, str(tmp_path / key))
    parent = resolve_config(no_config=True, **({} if empty else {key: value}))
    for name, setting in parent.subprocess_env().items():
        monkeypatch.setenv(name, setting)
    child = resolve_config()
    # An empty mirror is passed deliberately (it overrides a config file) and means none.
    same = (lambda v: v or None) if key == "mirror" else (lambda v: v)
    assert same(getattr(child, key)) == same(getattr(parent, key))
    if key == "export_priority" and not empty:
        assert child.export_priority == ("rs", "py")


def test_explicit_writer_list_is_a_tuple():
    assert resolve_config(no_config=True, export_priority=["rs", "py"]).export_priority == ("rs", "py")
    with pytest.raises(ValueError, match="writer names"):
        resolve_config(no_config=True, export_priority=[1])


# ---------- a skewed loose snapshot is announced, never silently dropped ----------

def _skewed(root: Path):
    _catalog.known_schema_versions()  # read before the checkout root moves
    (root / "docs" / "v1").mkdir(parents=True)
    (root / "sources.json").write_text(json.dumps({"schema_version": 2, "datasets": [{"slug": "tiny"}]}))
    (root / "docs" / "v1" / "snapshot.json").write_text(json.dumps(
        {"schema_version": 1, "slugs": {"tiny": {"parquet_bytes": 5}}}))


def test_checkout_snapshot_skew_warns(tmp_path, monkeypatch):
    _skewed(tmp_path)
    monkeypatch.setattr(_catalog, "_repo_root", lambda: tmp_path)
    cfg = resolve_config(no_config=True, catalog="checkout", catalog_dir=tmp_path / "catalogs")
    with pytest.warns(RuntimeWarning, match="schema_version 1 but .* is 2.*not verified"):
        context = catalogs.resolve_context(cfg, repo_root=tmp_path)
    assert context.snapshot_path is None
    assert context.snapshot["slugs"] == {}


def test_bundled_snapshot_skew_is_a_broken_install(tmp_path, monkeypatch):
    data = tmp_path / "_data"
    data.mkdir()
    (data / "sources.json").write_text(json.dumps({"schema_version": 2, "datasets": []}))
    (data / "snapshot.json").write_text(json.dumps({"schema_version": 1, "slugs": {}}))
    monkeypatch.setattr(catalogs.resources, "files", lambda package: tmp_path)
    cfg = resolve_config(no_config=True, catalog="bundled", catalog_dir=tmp_path / "catalogs")
    with pytest.raises(CatalogError, match="installation is broken"):
        catalogs.resolve_context(cfg)


def test_bundled_catalog_missing_in_a_checkout_says_so(tmp_path, monkeypatch):
    monkeypatch.setattr(catalogs.resources, "files", lambda package: tmp_path)
    cfg = resolve_config(no_config=True, catalog="bundled", catalog_dir=tmp_path / "catalogs")
    with pytest.raises(CatalogError, match="only inside a built raincloud wheel"):
        catalogs.resolve_context(cfg)


def test_catalog_local_without_a_manifest_is_refused(tmp_path):
    cfg = resolve_config(no_config=True, catalog="local", catalog_dir=tmp_path / "catalogs")
    with pytest.raises(CatalogError, match="manifest"):
        catalogs.resolve_context(cfg)


# ---------- locks and atomic writes ----------

def test_lock_is_reentrant_through_a_symlinked_directory(tmp_path):
    real = tmp_path / "real"
    real.mkdir()
    (tmp_path / "link").symlink_to(real)
    with locked(real / ".lock"):
        with locked(tmp_path / "link" / ".lock", timeout=2):
            assert is_held(real / ".lock")
    assert not is_held(real / ".lock")


def test_atomic_write_without_fchmod(tmp_path, monkeypatch):
    monkeypatch.delattr(os, "fchmod")
    atomic_write(tmp_path / "file", b"payload")
    assert (tmp_path / "file").read_bytes() == b"payload"


# ---------- gc ----------

GC_KEYS = {"pinned", "dry_run", "removable", "removed", "failed", "kept", "in_use", "history", "trimmed_history"}


def _revisions(cfg, count):
    revisions = cfg.catalog_dir / "revisions"
    revisions.mkdir(parents=True)
    revs = [f"{i:064x}" for i in range(1, count + 1)]
    for r in revs:
        (revisions / r).mkdir()
    return revisions, revs


def test_gc_reports_failures_separately(tmp_path, monkeypatch):
    cfg = resolve_config(no_config=True, catalog_dir=tmp_path / "catalogs")
    revisions, revs = _revisions(cfg, 3)
    (cfg.catalog_dir / "active.json").write_text(json.dumps({"active": revs[-1], "pinned": False, "history": []}))
    real = shutil.rmtree

    def rmtree(path, *a, **k):
        if Path(path).name == revs[0]:
            raise PermissionError(13, "Permission denied", str(path))
        real(path, *a, **k)
    monkeypatch.setattr(catalogs.shutil, "rmtree", rmtree)
    result = catalogs.gc(cfg, keep=0)
    assert result["removed"] == [revs[1]]
    assert list(result["failed"]) == [revs[0]] and "Permission denied" in result["failed"][revs[0]]
    assert (revisions / revs[0]).exists()


def test_gc_returns_one_schema(tmp_path):
    cfg = resolve_config(no_config=True, catalog_dir=tmp_path / "catalogs")
    _, revs = _revisions(cfg, 3)
    state = cfg.catalog_dir / "active.json"
    state.write_text(json.dumps({"active": revs[-1], "pinned": False, "history": revs[:-1]}))
    dry = catalogs.gc(cfg, keep=1, dry_run=True)
    assert GC_KEYS <= set(dry) and dry["removable"] == [revs[0]] and dry["removed"] == []
    real = catalogs.gc(cfg, keep=1)
    assert set(real) == set(dry) and real["removed"] == [revs[0]]
    state.write_text(json.dumps({"active": revs[-1], "pinned": True, "history": []}))
    assert GC_KEYS <= set(catalogs.gc(cfg))


def test_gc_keeps_a_revision_the_settings_select(tmp_path):
    cfg = resolve_config(no_config=True, catalog_dir=tmp_path / "catalogs")
    revisions, revs = _revisions(cfg, 2)
    (cfg.catalog_dir / "active.json").write_text(json.dumps({"active": revs[1], "pinned": False, "history": []}))
    result = catalogs.gc(replace(cfg, catalog=revs[0]), keep=0)
    assert result["removed"] == [] and (revisions / revs[0]).exists()


def test_gc_spares_a_revision_a_running_build_holds(tmp_path):
    cfg = resolve_config(no_config=True, catalog=str(catalog_dir(tmp_path / "bundle")),
                         catalog_dir=tmp_path / "catalogs")
    context = catalogs.resolve_context(cfg)
    revision = context.bundle.revision
    with context.pinned(cfg) as pinned:
        assert pinned.catalog == revision
        result = catalogs.gc(cfg, keep=0)
        assert result["in_use"] == [revision] and result["removed"] == []
        # The child, resolving afresh after gc, still finds its catalog.
        assert catalogs.resolve_context(pinned).bundle.revision == revision
    assert catalogs.gc(cfg, keep=0)["removed"] == [revision]
    assert not list((cfg.catalog_dir / "leases").iterdir())


def test_gc_drops_a_dead_holders_lease(tmp_path):
    cfg = resolve_config(no_config=True, catalog=str(catalog_dir(tmp_path / "bundle")),
                         catalog_dir=tmp_path / "catalogs")
    context = catalogs.resolve_context(cfg)
    catalogs.store(cfg, context.bundle)
    lease = cfg.catalog_dir / "leases" / f"{context.bundle.revision}.999999-deadbeef.lock"
    lease.parent.mkdir(parents=True)
    lease.touch()  # nobody holds it: its process is gone
    assert catalogs.gc(cfg, keep=0)["removed"] == [context.bundle.revision]
    assert not lease.exists()


# ---------- an unchanged catalog is parsed once ----------

def test_unchanged_catalog_is_not_parsed_again(tmp_path):
    manifest = tmp_path / "sources.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": [{"slug": "one"}]}))
    cfg = resolve_config(no_config=True, manifest=manifest, catalog_dir=tmp_path / "catalogs")
    first, second = catalogs.resolve_context(cfg), catalogs.resolve_context(cfg)
    assert first.bundle is second.bundle
    atomic_write(manifest, encode({"schema_version": 2, "datasets": [{"slug": "one"}, {"slug": "two"}]}))
    assert "two" in raincloud.slugs(config=cfg)


# ---------- format choice ----------

@pytest.fixture
def no_vortex(monkeypatch):
    import raincloud._readers as readers
    real = readers.find_spec
    monkeypatch.setattr(readers, "find_spec", lambda name: None if name == "vortex" else real(name))


def test_cli_resolution_does_not_need_a_python_reader(fixture, no_vortex, capsys):
    options, cfg = fixture
    code, out, err = cli(capsys, options, "--json", "load", "tiny", "--format", "vortex")
    assert code == 0, out + err
    reply = json.loads(out)
    assert reply["path"].endswith("tiny.vortex") and reply["catalog_revision"]
    code, out, _ = cli(capsys, options, "--json", "describe", "tiny")
    assert json.loads(out)["format"] == "vortex"
    # Parquet is opt-in: an install that does not build it gets the canonical
    # Arrow when it cannot read Vortex, even with a Parquet file present...
    code, out, _ = cli(capsys, options, "--json", "describe", "tiny", "--readers", "arrow,parquet")
    assert json.loads(out)["format"] == "arrow"
    # ...and Parquet once it opts in.
    code, out, _ = cli(capsys, {**options, "formats": "vortex,parquet"}, "--json", "describe", "tiny",
                       "--readers", "arrow,parquet")
    assert json.loads(out)["format"] == "parquet"
    # Python reads still need their reader.
    assert raincloud.load("tiny", config=cfg).format == "arrow"
    assert raincloud.load("tiny", config=replace(cfg, formats=("vortex", "parquet"))).format == "parquet"
    with pytest.raises(MissingDependency):
        raincloud.load("tiny", format="vortex", config=cfg)


def test_auto_with_only_an_unreadable_format_names_the_extra(no_vortex):
    entry = Entry("only-vortex", 1, formats={"vortex": FormatInfo(None, 1)}, version=2)
    with pytest.raises(MissingDependency, match="only-vortex is prepared only as vortex.*raincloud\\[vortex\\]"):
        raincloud._choose_format(entry, "auto", True)


def test_format_names_fold_case_and_suggest(fixture, capsys):
    options, cfg = fixture
    assert raincloud.load("tiny", format="Parquet", config=cfg).format == "parquet"
    code, _, err = cli(capsys, options, "load", "tiny", "-f", "pq")
    assert code == 1 and "Did you mean parquet?" in err


# ---------- typed read errors ----------

@pytest.mark.parametrize("fmt", ["arrow", "parquet", "vortex"])
def test_damaged_file_is_corrupt_artifact(fixture, fmt):
    options, cfg = fixture
    path = Path(options["data_dir"]) / _resolve.artifact_key("tiny", fmt, 2)
    raw = bytearray(path.read_bytes())
    raw[-16:] = b"\0" * 16  # same size: the catalog still recognises the file
    path.write_bytes(bytes(raw))
    with pytest.raises(CorruptArtifact, match="re-fetch or rebuild"):
        raincloud.load("tiny", format=fmt, config=cfg).to_arrow()


@pytest.mark.parametrize("fmt", ["arrow", "parquet", "vortex"])
def test_unknown_columns_are_refused_alike(fixture, fmt):
    _, cfg = fixture
    handle = raincloud.load("tiny", format=fmt, config=cfg)
    with handle.batches(columns=["id"]) as batches:
        assert [b.schema.names for b in batches][0] == ["id"]
    for columns in (["nope"], ["nested.x"]):
        with pytest.raises(UnknownColumn):
            with handle.batches(columns=columns) as batches:
                list(batches)


# ---------- a mirror download does not wait for the store lock ----------

def test_mirror_download_runs_outside_the_store_lock(fixture, monkeypatch):
    options, cfg = fixture
    cfg = replace(cfg, data_dir=Path(options["cache_dir"]) / "empty", offline=False,
                  mirror=Path(options["data_dir"]).as_uri())
    fetched = threading.Event()
    real = _resolve._transport.fetch

    def fetch(url, dest):
        real(url, dest)
        fetched.set()
    monkeypatch.setattr(_resolve._transport, "fetch", fetch)
    store = cfg.cache_dir / ".raincloud-write.lock"
    result = {}
    with locked(store):
        worker = threading.Thread(target=lambda: result.setdefault(
            "path", raincloud.load("tiny", format="parquet", config=cfg).path()))
        worker.start()
        assert fetched.wait(10), "the download waited for the store lock"
        assert "path" not in result  # adoption does wait for it
    worker.join(10)
    assert result["path"] == cfg.cache_dir / _resolve.artifact_key("tiny", "parquet", 2)


# ---------- mirror failures are named, never with credentials ----------

def test_transport_errors_are_typed_and_redacted(tmp_path, monkeypatch):
    import fsspec

    url = "https://reader:s3cr3t@mirror.example/v2/x.parquet?sig=t0ken"
    monkeypatch.setattr(fsspec, "open", lambda *a, **k: (_ for _ in ()).throw(ImportError("no aiohttp")))
    with pytest.raises(MissingDependency, match=r"raincloud\[http\]") as exc:
        _resolve._transport.fetch(url, tmp_path / "part")
    assert "s3cr3t" not in str(exc.value) and "t0ken" not in str(exc.value)

    def refuse(*a, **k):
        raise ConnectionError(f"cannot connect to {url} (sig=t0ken)")
    monkeypatch.setattr(fsspec, "open", refuse)
    with pytest.raises(MirrorUnavailable) as exc:
        _resolve._transport.fetch(url, tmp_path / "part")
    assert "s3cr3t" not in str(exc.value) and "t0ken" not in str(exc.value)
    assert exc.value.__cause__ is None and exc.value.__suppress_context__


def test_a_missing_local_mirror_is_named(fixture):
    options, cfg = fixture
    cfg = replace(cfg, data_dir=Path(options["cache_dir"]) / "empty", offline=False,
                  mirror=(Path(options["cache_dir"]) / "nowhere").as_uri())
    with pytest.raises(ArtifactNotFound, match="is not a directory on this machine"):
        raincloud.load("tiny", format="parquet", config=cfg).path()


# ---------- no recorded size still refuses an earlier recipe's build ----------

def test_unsized_artifact_from_an_earlier_recipe_is_refused(tmp_path):
    from raincloud import _builds

    manifest = tmp_path / "sources.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": [{"slug": "tiny"}]}))
    cfg = resolve_config(no_config=True, manifest=manifest, data_dir=tmp_path / "data",
                         catalog_dir=tmp_path / "catalogs", offline=True)
    key = _resolve.artifact_key("tiny", "parquet", 2)
    (cfg.data_dir / key).parent.mkdir(parents=True)
    (cfg.data_dir / key).write_bytes(b"old bytes")
    assert raincloud.load("tiny", format="parquet", config=cfg).path() == cfg.data_dir / key
    _builds.record(cfg.data_dir, {key: {"bytes": 9, "recipe": "an-earlier-recipe"}})
    with pytest.raises(OfflineMiss, match="earlier recipe"):
        raincloud.load("tiny", format="parquet", config=cfg).path()


# ---------- builds from load ----------

def test_a_build_from_load_logs_to_stderr(fixture, monkeypatch):
    options, cfg = fixture
    cfg = replace(cfg, data_dir=Path(options["cache_dir"]) / "built", offline=False)
    seen = {}

    def run(argv, **kwargs):
        seen.update(kwargs)
        path = cfg.data_dir / _resolve.artifact_key("tiny", "parquet", 2)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
    monkeypatch.setattr(_resolve, "_build_import_error", lambda: None)
    monkeypatch.setattr(_resolve.subprocess, "run", run)
    raincloud.load("tiny", format="parquet", config=cfg, build=True).path()
    assert seen["stdout"] not in (None, 1)


def test_offline_says_the_build_was_not_attempted(fixture):
    options, cfg = fixture
    cfg = replace(cfg, data_dir=Path(options["cache_dir"]) / "none")
    with pytest.raises(OfflineMiss, match="build was not attempted"):
        raincloud.load("tiny", format="parquet", config=cfg, build=True).path()


# ---------- entry points ----------

def test_every_entry_point_takes_a_toml_path(fixture, tmp_path):
    options, _ = fixture
    toml = tmp_path / "settings.toml"
    toml.write_text("[raincloud]\n" + "".join(f"{k} = {json.dumps(v)}\n" for k, v in options.items()
                                              if k != "no_config"))
    assert raincloud.slugs(config=toml) == ["tiny"]
    assert raincloud.describe("tiny", config=str(toml))["rows"] == 8
    assert raincloud.load("tiny", config=toml).catalog_source == options["catalog"]


def test_empty_config_variable_means_unset(tmp_path, monkeypatch):
    monkeypatch.delenv("RAINCLOUD_NO_CONFIG")
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "none"))
    monkeypatch.setenv("RAINCLOUD_CONFIG", "")
    assert resolve_config().file == tmp_path / "none" / "raincloud" / "config.toml"
    assert resolve_config(config="").file == tmp_path / "none" / "raincloud" / "config.toml"


def test_direct_dataset_keeps_its_catalog(fixture):
    options, cfg = fixture
    handle = raincloud.Dataset("tiny", "parquet", mirror=None, offline=True, config=cfg)
    assert handle.catalog_source == options["catalog"]


def test_artifact_key_has_no_default_version():
    with pytest.raises(TypeError):
        _resolve.artifact_key("tiny", "parquet")


# ---------- rollback copies ----------

def test_publication_sweeps_orphaned_rollback_copies(tmp_path, monkeypatch):
    dest = tmp_path / "tiny.parquet"
    dest.write_bytes(b"old")
    orphan = tmp_path / f".tiny.parquet.12345-{'a' * 32}.rollback"
    legacy = tmp_path / f".tiny.parquet.{'b' * 32}.rollback"
    mine = tmp_path / f".tiny.parquet.{os.getpid()}-{'c' * 32}.rollback"
    other = tmp_path / f".other.parquet.12345-{'d' * 32}.rollback"
    for path in (orphan, legacy, mine, other):
        path.write_bytes(b"x")
    monkeypatch.setattr(_cache, "_STALE_BACKUP_SECONDS", -60)
    with _cache.Publication(dest) as publication:
        dest.write_bytes(b"new")
        publication.accept()
    assert not orphan.exists() and not legacy.exists()
    assert mine.exists() and other.exists()


def test_failed_rollback_copy_leaves_nothing(tmp_path, monkeypatch):
    dest = tmp_path / "tiny.parquet"
    dest.write_bytes(b"old")
    monkeypatch.setattr(_cache.os, "link", lambda *a: (_ for _ in ()).throw(OSError("no links")))

    def full(src, dst):
        Path(dst).write_bytes(b"part")
        raise OSError(28, "No space left on device")
    monkeypatch.setattr(_cache.shutil, "copy2", full)
    with pytest.raises(OSError, match="No space"):
        _cache.Publication(dest).__enter__()
    assert [p.name for p in tmp_path.iterdir()] == ["tiny.parquet"]


# ---------- the extra a missing module names ----------

@pytest.mark.parametrize("requirements", [
    # Self-references kept, as some installers write them.
    ["vortex-data==0.86.1; extra == 'vortex'", "raincloud[vortex]; extra == 'build'",
     "duckdb>=1.5.0; extra == 'build'", "zstandard; extra == 'build'", "jsonschema; extra == 'build'",
     "raincloud[build]; extra == 'generated'", "duckdb==1.5.5; extra == 'generated'",
     "tpchgen-cli==3.0.0; extra == 'generated'", "pandas; extra == 'pandas'",
     "openpyxl; extra == 'excel'", "pandas; extra == 'excel'", "osmium; extra == 'osm'",
     "raincloud[build,osm,excel,generated]; extra == 'all'"],
    # Flattened, as hatchling writes them.
    ["vortex-data==0.86.1; extra == 'vortex'", "vortex-data==0.86.1; extra == 'build'",
     "duckdb>=1.5.0; extra == 'build'", "zstandard; extra == 'build'", "jsonschema; extra == 'build'",
     "vortex-data==0.86.1; extra == 'generated'", "duckdb>=1.5.0; extra == 'generated'",
     "zstandard; extra == 'generated'", "jsonschema; extra == 'generated'",
     "duckdb==1.5.5; extra == 'generated'", "tpchgen-cli==3.0.0; extra == 'generated'",
     "pandas; extra == 'pandas'", "openpyxl; extra == 'excel'", "pandas; extra == 'excel'",
     "osmium; extra == 'osm'",
     *(f"{dist}; extra == 'all'" for dist in ("vortex-data", "duckdb", "zstandard", "jsonschema",
                                              "tpchgen-cli", "pandas", "openpyxl", "osmium"))],
])
def test_extra_for_names_the_smallest_extra_that_covers(monkeypatch, requirements):
    monkeypatch.setattr(_extras.metadata, "requires", lambda name: requirements)
    assert _extras.extra_for("duckdb") == "build"
    assert _extras.extra_for("vortex") == "vortex"
    assert _extras.extra_for("osmium") == "osm"
    assert _extras.extra_for("pandas") == "pandas"
    assert _extras.extra_for("pandas", "openpyxl") == "excel"
    error = ModuleNotFoundError("No module named 'pandas'", name="pandas")
    assert "raincloud[excel]" in str(_extras.missing(error, "the xlsx handler", needs=("openpyxl", "pandas")))


# ---------- which catalog pipeline views read ----------

def test_selected_context_decision_table(tmp_path):
    base = resolve_config(no_config=True, catalog_dir=tmp_path / "catalogs")
    directory = catalog_dir(tmp_path / "bundle")
    with use_config(base):
        assert catalogs.selected_context() is None  # a checkout, nothing selected
    chosen = replace(base, catalog=str(directory))
    with use_config(chosen):
        assert catalogs.selected_context().source == str(directory)
    context = catalogs.resolve_context(chosen)
    with catalogs.operation(base, replace(context, source="checkout", legacy=True)):
        assert catalogs.selected_context() is None
    with catalogs.operation(base, context):
        assert catalogs.selected_context() is context
    catalogs.store(base, context.bundle)
    catalogs.pin(base, context.bundle.revision)
    with use_config(base):
        assert catalogs.selected_context().bundle.revision == context.bundle.revision


# ---------- CLI ----------

def test_cli_commands_people_type(fixture, capsys):
    options, _ = fixture
    for args in (["describe"], ["load"], ["catalog", "pin"], ["help", "build"], ["help", "catalog"]):
        code, out, err = cli(capsys, options, *args)
        assert code == 0 and "usage: raincloud:" not in err, (args, err)
    assert "raincloud build SLUG" in cli(capsys, options)[1]
    for args, want in ((["catalog", "statsu"], "Did you mean status?"), (["config", "shwo"], "Did you mean show?")):
        with pytest.raises(SystemExit) as exc:
            main(["--settings", json.dumps(options), *args])
        err = capsys.readouterr().err
        assert exc.value.code == 2 and want in err and "usage:" not in err
    code, out, _ = cli(capsys, options, "load", "tiny", "--format", "arrow", "--json")
    assert code == 0 and json.loads(out)["format"] == "arrow"
    code, out, _ = cli(capsys, options, "config")
    assert code == 0 and out.startswith("config file") and not out.lstrip().startswith("{")
    assert json.loads(cli(capsys, options, "config", "--json")[1])["data_dir"]["value"] == options["data_dir"]
    assert "available" in cli(capsys, options, "capabilities")[1]
    code, out, _ = cli(capsys, options, "help", "load")
    out = " ".join(out.split())
    assert "never fetch or build" in out and "auto (the default)" in out


def test_catalog_pin_accepts_the_printed_prefix(fixture, tmp_path, capsys):
    options, cfg = fixture
    revision = catalogs.resolve_context(cfg).bundle.revision
    code, _, err = cli(capsys, options, "catalog", "pin", revision[:12])
    assert code == 0, err
    assert catalogs.state(cfg) == {"active": revision, "pinned": True, "history": []}
    code, _, err = cli(capsys, options, "catalog", "pin", "0000")
    assert code == 1 and "no installed catalog revision starts with 0000" in err


def test_catalog_update_without_a_source_says_what_to_pass(fixture, capsys):
    options, _ = fixture
    code, _, err = cli(capsys, options, "catalog", "update")
    assert code == 1 and "--source" in err and "raincloud catalog pack" in err


def test_settings_travel_in_the_environment(fixture, monkeypatch, capsys):
    options, _ = fixture
    monkeypatch.setenv("RAINCLOUD_SETTINGS", json.dumps(options))
    assert main(["--json", "--settings-env", "describe", "--", "tiny"]) == 0
    assert json.loads(capsys.readouterr().out)["slug"] == "tiny"
    # Consumed on read, so a `raincloud build` child does not inherit it.
    assert "RAINCLOUD_SETTINGS" not in os.environ
    monkeypatch.setenv("RAINCLOUD_SETTINGS", json.dumps(options))
    assert main(["--json", "--settings-env", "describe", "nope"]) == 1
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["type"] == "UnknownSlug" and error["mro"][:2] == ["UnknownSlug", "RaincloudError"]


def test_init_writes_a_template_and_force_matches_permissions(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("RAINCLOUD_NO_CONFIG")
    plain, forced = tmp_path / "plain.toml", tmp_path / "forced.toml"
    assert main(["--config", str(plain), "init"]) == 0
    text = plain.read_text()
    assert text.startswith("[raincloud]\n") and "# data_dir = " in text
    assert resolve_config(config=plain).data_dir  # the template parses
    forced.write_text("[raincloud]\n")
    assert main(["--config", str(forced), "init", "--force", "--data-dir", str(tmp_path / "d")]) == 0
    assert plain.stat().st_mode & 0o777 == forced.stat().st_mode & 0o777


def test_a_closed_pipe_is_a_clean_exit(fixture, monkeypatch, capsys):
    options, _ = fixture
    import raincloud.cli as cli_module

    def closed(*args):
        raise BrokenPipeError(32, "Broken pipe")
    monkeypatch.setattr(cli_module, "_describe_text", closed)
    code, _, err = cli(capsys, options, "describe", "tiny")
    assert code == 0 and "Broken pipe" not in err


# ---------- lock order between a build and a mirror download ----------

_LOCK_ORDER_CHILD = r'''
import json, sys, time
from pathlib import Path
import raincloud
from raincloud import _resolve
from raincloud._locking import locked
from raincloud.config import resolve_config

role, options, flags = sys.argv[1], json.loads(sys.argv[2]), Path(sys.argv[3])
cfg = resolve_config(no_config=True, **options)
if role == "reader":
    real = _resolve._transport.fetch

    def fetch(url, dest):
        real(url, dest)
        (flags / "fetched").touch()  # the artifact lock is held from here ...
        time.sleep(1)                # ... long enough for the build to queue on it
    _resolve._transport.fetch = fetch
    print(raincloud.load("tiny", format="parquet", config=cfg).path(), flush=True)
else:
    # A build: it holds the store lock, then loads the artifact the reader is fetching.
    with locked(cfg.cache_dir / ".raincloud-write.lock"):
        (flags / "store-held").touch()
        deadline = time.monotonic() + 30
        while not (flags / "fetched").exists():
            if time.monotonic() > deadline:
                sys.exit("the reader never fetched")
            time.sleep(0.05)
        print(raincloud.load("tiny", format="parquet", config=cfg).path(), flush=True)
'''


def _child(script: Path, *args):
    import subprocess
    import sys
    root = Path(raincloud.__file__).resolve().parent.parent
    env = {**os.environ, "PYTHONPATH": str(root)}
    return subprocess.Popen([sys.executable, str(script), *args], cwd=root, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _wait_for(path: Path, seconds=30):
    import time
    deadline = time.monotonic() + seconds
    while not path.exists():
        assert time.monotonic() < deadline, f"{path} never appeared"
        time.sleep(0.05)


def test_a_build_holding_the_store_and_a_mirror_reader_do_not_deadlock(fixture, tmp_path):
    import subprocess
    options, _ = fixture
    store = tmp_path / "store"
    child_options = {**{k: v for k, v in options.items() if k != "no_config"}, "offline": False,
                     "data_dir": str(store), "cache_dir": str(store),
                     "mirror": Path(options["data_dir"]).as_uri()}
    script, flags = tmp_path / "child.py", tmp_path / "flags"
    script.write_text(_LOCK_ORDER_CHILD)
    flags.mkdir()
    builder = _child(script, "builder", json.dumps(child_options), str(flags))
    reader = None
    try:
        _wait_for(flags / "store-held")
        reader = _child(script, "reader", json.dumps(child_options), str(flags))
        try:
            built, err = builder.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            pytest.fail("deadlock: the build waited on the artifact lock while the reader waited on the store")
        assert builder.returncode == 0, err
        read, err = reader.communicate(timeout=60)
        assert reader.returncode == 0, err
    finally:
        for process in (builder, reader):
            if process is not None and process.poll() is None:
                process.kill()
                process.communicate()
    dest = store / _resolve.artifact_key("tiny", "parquet", 2)
    assert built.strip() == read.strip() == str(dest)
    assert dest.read_bytes() == (Path(options["data_dir"]) / _resolve.artifact_key("tiny", "parquet", 2)).read_bytes()


def test_locked_timeout_covers_another_thread(tmp_path):
    path = tmp_path / ".lock"
    held, release = threading.Event(), threading.Event()

    def holder():
        with locked(path):
            held.set()
            release.wait(10)
    worker = threading.Thread(target=holder)
    worker.start()
    try:
        assert held.wait(10)
        with pytest.raises(TimeoutError, match="another thread"):
            with locked(path, timeout=0):
                pass
    finally:
        release.set()
        worker.join(10)
    with locked(path, timeout=0):
        pass


def test_lock_registry_does_not_grow_with_unique_locks(tmp_path):
    from raincloud import _locking
    before = len(_locking._registry)
    for i in range(50):
        with locked(tmp_path / f"lease-{i}.lock"):
            pass
    assert len(_locking._registry) == before


# ---------- no recorded size, a mirror, and an earlier recipe's build ----------

def test_unsized_artifact_from_an_earlier_recipe_is_replaced_from_the_mirror(tmp_path):
    from raincloud import _builds

    manifest = tmp_path / "sources.json"
    manifest.write_text(json.dumps({"schema_version": 2, "datasets": [{"slug": "tiny"}]}))
    cfg = resolve_config(no_config=True, manifest=manifest, data_dir=tmp_path / "data",
                         catalog_dir=tmp_path / "catalogs", mirror=(tmp_path / "mirror").as_uri())
    assert cfg.cache_dir == cfg.data_dir  # the default: the refused file IS the mirror's destination
    key = _resolve.artifact_key("tiny", "parquet", 2)
    for root, payload in ((cfg.data_dir, b"old bytes"), (tmp_path / "mirror", b"the catalog's bytes")):
        (root / key).parent.mkdir(parents=True)
        (root / key).write_bytes(payload)
    _builds.record(cfg.data_dir, {key: {"bytes": 9, "recipe": "an-earlier-recipe"}})
    path = raincloud.load("tiny", format="parquet", config=cfg).path()
    assert path == cfg.data_dir / key and path.read_bytes() == b"the catalog's bytes"
    # The build record no longer claims the file, so the next load serves it without a download.
    assert _builds.lookup(cfg.data_dir, key) is None
    assert raincloud.load("tiny", format="parquet", config=replace(cfg, offline=True)).path() == path


# ---------- mirror problems are said, and never mistaken ----------

def _building(monkeypatch, cfg):
    def run(argv, **kwargs):
        path = cfg.data_dir / _resolve.artifact_key("tiny", "parquet", 2)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"x")
    monkeypatch.setattr(_resolve, "_build_import_error", lambda: None)
    monkeypatch.setattr(_resolve.subprocess, "run", run)


def test_a_missing_local_mirror_warns_before_building(fixture, monkeypatch):
    options, cfg = fixture
    cfg = replace(cfg, data_dir=Path(options["cache_dir"]) / "built", offline=False,
                  mirror=(Path(options["cache_dir"]) / "nowhere").as_uri())
    _building(monkeypatch, cfg)
    with pytest.warns(RuntimeWarning, match="is not a directory on this machine; building tiny locally"):
        raincloud.load("tiny", format="parquet", config=cfg, build=True).path()


def test_a_refused_local_file_still_says_where_the_mirror_looked(fixture):
    options, cfg = fixture
    cfg = replace(cfg, data_dir=Path(options["cache_dir"]) / "wrong", offline=False,
                  mirror=(Path(options["cache_dir"]) / "nowhere").as_uri())
    stale = cfg.data_dir / _resolve.artifact_key("tiny", "parquet", 2)
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"x")
    with pytest.raises(ArtifactNotFound, match="bytes but the catalog's.*is not a directory on this machine"):
        raincloud.load("tiny", format="parquet", config=cfg).path()


def test_a_file_url_with_a_host_is_an_unusable_mirror(fixture, monkeypatch):
    options, cfg = fixture
    cfg = replace(cfg, data_dir=Path(options["cache_dir"]) / "built", offline=False,
                  mirror="file://elsewhere/srv/mirror")
    with pytest.raises(MirrorUnavailable, match="not a usable file URL"):
        raincloud.load("tiny", format="parquet", config=cfg).path()
    _building(monkeypatch, cfg)
    with pytest.warns(RuntimeWarning, match="building tiny locally"):
        raincloud.load("tiny", format="parquet", config=cfg, build=True).path()


def test_a_local_write_failure_is_not_an_unreachable_mirror(tmp_path, monkeypatch):
    from raincloud import _transport

    source = tmp_path / "source.bin"
    source.write_bytes(b"payload")

    class Full:
        def write(self, chunk):
            raise OSError(28, "No space left on device")

        def close(self):
            pass
    monkeypatch.setattr(_transport, "open", lambda *a, **k: Full(), raising=False)
    dest = tmp_path / "cache" / "part"
    with pytest.raises(OSError, match="No space left") as exc:
        _transport.fetch(source.as_uri(), dest)
    assert not isinstance(exc.value, MirrorUnavailable) and exc.value.filename == str(dest)
    monkeypatch.undo()
    if os.geteuid() != 0:
        readonly = tmp_path / "readonly"
        readonly.mkdir()
        readonly.chmod(0o500)
        try:
            with pytest.raises(PermissionError):
                _transport.fetch(source.as_uri(), readonly / "part")
        finally:
            readonly.chmod(0o700)


def test_scrub_redacts_a_requoted_url():
    from raincloud._transport import _scrub
    url = "https://mirror.example/v2/x.parquet?sig=t0ken"
    text = _scrub("GET https://mirror.example/v2/x.parquet?sig=t0k%65n failed", url)
    assert "t0k" not in text and "https://mirror.example/v2/x.parquet" in text


# ---------- an OS error from vortex is about access, not the bytes ----------

def test_vortex_os_errors_are_access_errors(tmp_path):
    with pytest.raises(PermissionError) as exc:
        with raincloud._decoding(tmp_path / "x.vortex", "vortex"):
            raise RuntimeError("Io: Permission denied (os error 13)")
    assert exc.value.filename == str(tmp_path / "x.vortex")
    with pytest.raises(FileNotFoundError):
        with raincloud._decoding(tmp_path / "x.vortex", "vortex"):
            raise RuntimeError("Io: No such file or directory (os error 2)")
    with pytest.raises(CorruptArtifact):
        with raincloud._decoding(tmp_path / "x.vortex", "vortex"):
            raise RuntimeError("Other error: Malformed file, invalid magic bytes, got [0, 0, 0, 0]")
    with pytest.raises(RuntimeError, match="unrelated"):  # other formats: unclassified
        with raincloud._decoding(tmp_path / "x.parquet", "parquet"):
            raise RuntimeError("unrelated")


@pytest.mark.skipif(os.name != "posix" or os.geteuid() == 0, reason="needs file permissions that bind")
def test_an_unreadable_vortex_file_is_not_corrupt(fixture):
    options, cfg = fixture
    path = Path(options["data_dir"]) / _resolve.artifact_key("tiny", "vortex", 2)
    path.chmod(0)
    try:
        with pytest.raises(PermissionError):
            raincloud.load("tiny", format="vortex", config=cfg).to_arrow()
    finally:
        path.chmod(0o644)


# ---------- gc and revision prefixes ----------

def test_gc_keeps_a_revision_the_settings_select_by_prefix(tmp_path):
    cfg = resolve_config(no_config=True, catalog_dir=tmp_path / "catalogs")
    revisions = cfg.catalog_dir / "revisions"
    revs = ["a" * 64, "b" * 64, "c" * 64]
    for r in revs:
        (revisions / r).mkdir(parents=True)
    (cfg.catalog_dir / "active.json").write_text(json.dumps({"active": revs[2], "pinned": False, "history": []}))
    result = catalogs.gc(replace(cfg, catalog=revs[0][:12]), keep=0)
    assert result["removed"] == [revs[1]] and (revisions / revs[0]).exists()
    assert result["kept"] == [revs[0], revs[2]]


def test_gc_dry_run_leaves_dead_leases_and_a_real_run_sweeps_them_all(tmp_path):
    cfg = resolve_config(no_config=True, catalog=str(catalog_dir(tmp_path / "bundle")),
                         catalog_dir=tmp_path / "catalogs")
    context = catalogs.resolve_context(cfg)
    catalogs.store(cfg, context.bundle)
    catalogs.pin(cfg, context.bundle.revision)
    catalogs.unpin(cfg)  # active, so reachable: gc never probes its leases per revision
    leases = cfg.catalog_dir / "leases"
    leases.mkdir()
    dead = [leases / f"{context.bundle.revision}.999999-deadbeef.lock", leases / f"{'f' * 64}.999999-deadbeef.lock"]
    for lease in dead:
        lease.touch()
    catalogs.gc(cfg, keep=0, dry_run=True)
    assert all(lease.exists() for lease in dead)
    catalogs.gc(cfg, keep=0)
    assert not any(lease.exists() for lease in dead)


_LEASE_HOLDER = r'''
import json, sys
from raincloud import catalogs
from raincloud.config import resolve_config
cfg = resolve_config(no_config=True, **json.loads(sys.argv[1]))
with catalogs.resolve_context(cfg).pinned(cfg):
    print("held", flush=True)
    sys.stdin.read()
'''


def test_gc_in_another_process_sees_a_live_lease_and_drops_a_dead_one(tmp_path):
    import subprocess
    import sys
    options = {"catalog": str(catalog_dir(tmp_path / "bundle")), "catalog_dir": str(tmp_path / "catalogs")}
    cfg = resolve_config(no_config=True, **options)
    revision = catalogs.resolve_context(cfg).bundle.revision
    script = tmp_path / "holder.py"
    script.write_text(_LEASE_HOLDER)
    root = Path(raincloud.__file__).resolve().parent.parent
    holder = subprocess.Popen([sys.executable, str(script), json.dumps(options)], cwd=root,
                              env={**os.environ, "PYTHONPATH": str(root)}, text=True,
                              stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        assert holder.stdout.readline().strip() == "held", holder.stderr.read()
        result = catalogs.gc(cfg, keep=0)
        assert result["in_use"] == [revision] and result["removed"] == []
        holder.kill()  # SIGKILL: no cleanup runs, the lease file stays behind
        holder.communicate()
        assert list((cfg.catalog_dir / "leases").iterdir())
        assert catalogs.gc(cfg, keep=0)["removed"] == [revision]
        assert not list((cfg.catalog_dir / "leases").iterdir())
    finally:
        if holder.poll() is None:
            holder.kill()
            holder.communicate()


def test_a_revision_prefix_in_toml_selects_the_revision(tmp_path):
    cfg = resolve_config(no_config=True, catalog=str(catalog_dir(tmp_path / "bundle")),
                         catalog_dir=tmp_path / "catalogs")
    bundle = catalogs.resolve_context(cfg).bundle
    catalogs.store(cfg, bundle)
    toml = tmp_path / "settings.toml"
    toml.write_text(f'[raincloud]\ncatalog = "{bundle.revision[:12]}"\ncatalog_dir = "catalogs"\n')
    settings = resolve_config(config=toml)
    assert settings.catalog == bundle.revision[:12]
    assert catalogs.resolve_context(settings).bundle.revision == bundle.revision
    # A directory of that name beside the file is still the directory.
    (tmp_path / bundle.revision[:12]).mkdir()
    assert resolve_config(config=toml).catalog == str(tmp_path / bundle.revision[:12])


# ---------- describe says what is here and how to load it ----------

def test_describe_shows_what_is_prepared_here(fixture, capsys):
    import re
    options, cfg = fixture
    (Path(options["data_dir"]) / _resolve.artifact_key("tiny", "arrow", 2)).unlink()
    handle = raincloud.load("tiny", config=cfg)
    assert set(handle.local_paths()) == {"parquet", "vortex"}
    code, out, _ = cli(capsys, options, "--json", "describe", "tiny")
    assert code == 0 and set(json.loads(out)["prepared"]) == {"parquet", "vortex"}
    code, out, _ = cli(capsys, options, "describe", "tiny")
    formats = next(line for line in out.splitlines() if line.startswith("formats"))
    assert re.search(r"vortex [^,]*\(default, here\)", formats), formats
    assert re.search(r"parquet [^,]*\(here\)", formats) and not re.search(r"arrow [^,]*\(", formats), formats
    assert "load      raincloud load tiny" in out
    code, out, _ = cli(capsys, options, "--mirror", "s3://bucket/prefix", "describe", "tiny", "--format", "arrow")
    assert re.search(r"arrow [^,]*\(default\)", out) and "fetches it from the mirror" in out
    code, out, _ = cli(capsys, options, "describe", "tiny", "--format", "arrow")
    assert "no mirror is configured" in out and "raincloud build tiny" in out


# ---------- --json callers get JSON ----------

def test_json_callers_get_json_from_bare_commands(fixture, capsys):
    options, _ = fixture
    for command in ("describe", "load"):
        code, out, _ = cli(capsys, options, "--json", command)
        error = json.loads(out)["error"]
        assert code == 2 and error["type"] == "ValueError" and "raincloud list" in error["message"]
    code, out, _ = cli(capsys, options, "--json", "catalog", "pin")
    assert code == 0 and "selected" in json.loads(out)


def test_init_keeps_a_credentialed_mirror_private(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("RAINCLOUD_NO_CONFIG")
    secret, forced = tmp_path / "secret.toml", tmp_path / "forced.toml"
    mirror = "https://reader:s3cr3t@mirror.example/prefix"
    assert main(["--config", str(secret), "init", "--mirror", mirror]) == 0
    forced.write_text("[raincloud]\n")
    assert main(["--config", str(forced), "init", "--force", "--mirror", mirror]) == 0
    assert secret.stat().st_mode & 0o777 == forced.stat().st_mode & 0o777 == 0o600
    assert resolve_config(config=secret).mirror == mirror
