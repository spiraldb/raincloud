# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Wheel build + install-tier + loader-API + build-proof tests.

All tests in this module are `@pytest.mark.wheel` — gated by --run-wheel
(see tests/conftest.py). A session fixture builds the wheel once via
`uv build --wheel`; per-test helpers create throwaway venvs with `uv venv`
and install the wheel (± extras) via `uv pip install`.
"""
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.wheel


def _clean_env(**overrides: str) -> dict:
    """A subprocess env with the developer's ambient RAINCLOUD_* scrubbed.

    `os.environ.copy()` would otherwise leak the maintainer's exported
    RAINCLOUD_OUTPUTS / RAINCLOUD_WORKDIR / etc. into the venv subprocess,
    breaking hermeticity (artifacts written outside tmp_path). We start
    from a copy, drop every RAINCLOUD_* key, then layer on only the overrides
    the test sets explicitly.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("RAINCLOUD_")}
    env.pop("PYTHONPATH", None)
    env["RAINCLOUD_NO_CONFIG"] = "1"
    env.update(overrides)
    return env


@pytest.fixture(scope="session")
def built_wheel():
    """Build the raincloud wheel once per pytest session; return its Path."""
    # `uv build --wheel` writes to <repo>/dist/. Pick the newest matching
    # wheel after — multiple runs may leave older wheels lying around.
    subprocess.run(["uv", "build", "--wheel"], cwd=REPO_ROOT, check=True)
    wheels = sorted(
        (REPO_ROOT / "dist").glob("raincloud-*.whl"),
        key=lambda p: p.stat().st_mtime,
    )
    assert wheels, "uv build did not produce a wheel under dist/"
    return wheels[-1]


def _make_venv(tmp_path: Path, wheel: Path, extras: str = "") -> Path:
    """Create a throwaway venv and install raincloud[extras] from the wheel.

    `extras` is e.g. "[s3]" or "[build,pandas]" (PEP 508 form), or "" for base.
    Returns the venv root path.
    """
    venv = tmp_path / "venv"
    subprocess.run(["uv", "venv", "--python", sys.executable, str(venv)], check=True)
    pkg = f"raincloud{extras}" if extras else "raincloud"
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")),
            f"{pkg} @ {wheel.as_uri()}",
        ],
        check=True,
    )
    return venv


def _run_py(
    venv: Path, code: str, env: dict | None = None
) -> subprocess.CompletedProcess:
    """Run a Python snippet in the venv via subprocess; capture stdout/stderr."""
    env = dict(env if env is not None else _clean_env())
    env.setdefault("RAINCLOUD_CATALOG_DIR", str(venv.parent / "catalogs"))
    return subprocess.run(
        [str(venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")), "-c", code],
        capture_output=True,
        text=True,
        env=env if env is not None else _clean_env(),
        cwd=venv.parent,
    )


def test_wheel_builds_and_base_install_imports(built_wheel, tmp_path):
    """Smoke: wheel builds, base venv installs it, `import raincloud` succeeds,
    and the packaged catalog resolves with no env overrides (> 200 slugs)."""
    venv = _make_venv(tmp_path, built_wheel)
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "import importlib.metadata as _md\n"
            "from raincloud._catalog import load_catalog\n"
            "print(raincloud.__version__, _md.version('raincloud'),"
            "      len(load_catalog().slugs()))\n"
        ),
    )
    assert cp.returncode == 0, cp.stderr
    out = cp.stdout.strip().split()
    # Version is non-empty, PEP 440-shaped (starts with a digit), and
    # __version__ matches the installed dist's metadata (the sync invariant
    # pyproject already enforces). Avoids hardcoding the literal version.
    assert out[0] and out[0][0].isdigit(), f"version string looks wrong: {out[0]!r}"
    assert out[0] == out[1], f"__version__ {out[0]!r} != dist metadata {out[1]!r}"
    assert int(out[2]) > 200


def test_base_install_excludes_heavy_deps(built_wheel, tmp_path):
    """Base install MUST NOT pull osmium/pyreadstat/zstandard (those moved to [build])."""
    venv = _make_venv(tmp_path, built_wheel)
    cp = _run_py(
        venv,
        (
            "import importlib.util\n"
            "names = ['vortex', 'osmium', 'pyreadstat', 'zstandard', 'py7zr', 'unlzw3', 'openpyxl']\n"
            "present = [n for n in names if importlib.util.find_spec(n) is not None]\n"
            "print(','.join(present))\n"
        ),
    )
    assert cp.returncode == 0, cp.stderr
    assert cp.stdout.strip() == "", (
        f"base install unexpectedly pulled heavy deps: {cp.stdout.strip()!r}"
    )


def test_base_install_to_pandas_raises_missing_dependency(built_wheel, tmp_path):
    """`.to_pandas()` in a base install raises MissingDependency (pandas
    absent), without needing any I/O — the lazy guard fires before resolution."""
    venv = _make_venv(tmp_path, built_wheel)
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "from raincloud._catalog import load_catalog\n"
            "# pick any slug whose entry exists in the packaged catalog\n"
            "slug = next(iter(load_catalog().slugs()))\n"
            "ds = raincloud.load(slug)\n"
            "errors = []\n"
            "try:\n"
            "    ds.to_pandas()\n"
            "except raincloud.MissingDependency as e:\n"
            "    errors.append(('to_pandas', 'pandas' in str(e).lower()))\n"
            "print(errors)\n"
        ),
    )
    assert cp.returncode == 0, cp.stderr
    assert "('to_pandas', True)" in cp.stdout


def test_build_extra_installs_the_core_and_build_is_available(built_wheel, tmp_path):
    """`[build]` pulls the pipeline core AND _build_available() is True; a
    format-specific dependency is its own extra, absent until asked for."""
    venv = _make_venv(tmp_path, built_wheel, extras="[build]")
    cp = _run_py(
        venv,
        (
            "import duckdb, zstandard, jsonschema, importlib.util\n"
            "from raincloud import _resolve\n"
            "print(_resolve._build_available(), importlib.util.find_spec('osmium') is None)\n"
        ),
    )
    assert cp.returncode == 0, cp.stderr
    assert cp.stdout.strip() == "True True"


@pytest.mark.parametrize(
    "extra,backend",
    [
        ("[s3]", "s3fs"),
        ("[http]", "aiohttp"),
        ("[pandas]", "pandas"),
        ("[osm]", "osmium"),
        ("[sas]", "pyreadstat"),
    ],
)
def test_extra_installs_its_backend(built_wheel, tmp_path, extra, backend):
    """Each per-scheme/convenience extra wires its named backend module."""
    venv = _make_venv(tmp_path, built_wheel, extras=extra)
    cp = _run_py(venv, f"import {backend}; print('ok')")
    assert cp.returncode == 0, cp.stderr
    assert cp.stdout.strip() == "ok"


def _write_loader_fixture(tmp_path: Path):
    """Build a tmp file:// mirror with a real parquet + vortex artifact for slug 'tiny'.

    Creates the artifacts in the outer test env (requires the vortex extra),
    writes a fixture snapshot + manifest
    pinning their real sha256, and returns (env_dict, mirror_dir) where env_dict
    is ready to pass as `env=` to subprocess. The venv then reads the fixture
    via RAINCLOUD_* env overrides — hermetic.
    """
    import hashlib
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq
    import vortex

    table = pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
    mirror = tmp_path / "mirror"
    pq_key = mirror / "v1" / "tiny" / "parquet" / "tiny.parquet"
    vx_key = mirror / "v1" / "tiny" / "vortex" / "tiny.vortex"
    pq_key.parent.mkdir(parents=True)
    vx_key.parent.mkdir(parents=True)
    pq.write_table(table, pq_key)
    vortex.io.write(table, str(vx_key))

    def sha(p):
        return hashlib.sha256(p.read_bytes()).hexdigest()

    snapshot = {
        "schema_version": 1,
        "slugs": {
            "tiny": {
                "expected_rows": 3,
                "last_built_rows": 3,
                "parquet_bytes": pq_key.stat().st_size,
                "vortex_bytes": vx_key.stat().st_size,
                "parquet_sha256": sha(pq_key),
                "vortex_sha256": sha(vx_key),
                "columns": [
                    {"name": "x", "type": "int64"},
                    {"name": "y", "type": "string"},
                ],
            }
        },
    }
    manifest = {
        "schema_version": 1,
        "datasets": [
            {
                "slug": "tiny",
                "short_name": "Tiny",
                "full_name": "Tiny",
                "description": "d",
                "license": {"spdx": "CC0-1.0"},
                "fetch": {"urls": ["http://s"]},
                "convert": {"vortex": True},
            }
        ],
    }
    (tmp_path / "snapshot.json").write_text(json.dumps(snapshot))
    (tmp_path / "sources.json").write_text(json.dumps(manifest))

    env = _clean_env(
        RAINCLOUD_SNAPSHOT=str(tmp_path / "snapshot.json"),
        RAINCLOUD_MANIFEST=str(tmp_path / "sources.json"),
        RAINCLOUD_CACHE=str(tmp_path / "cache"),
        RAINCLOUD_MIRROR=f"file://{mirror}",
    )
    return env, mirror


def test_loader_happy_paths_against_wheel(built_wheel, tmp_path):
    """`load(); to_arrow/to_vortex/schema/path/num_rows/column_names/info`
    + vortex->parquet format fallback, against the installed wheel."""
    env, _mirror = _write_loader_fixture(tmp_path)
    venv = _make_venv(tmp_path, built_wheel, extras="[vortex]")
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "ds = raincloud.load('tiny')\n"
            "assert ds.format == 'vortex'\n"
            "assert ds.num_rows == 3\n"
            "assert ds.column_names == ['x', 'y']\n"
            "assert ds.info['license']['spdx'] == 'CC0-1.0'\n"
            "tbl = ds.to_arrow()\n"
            "assert tbl.num_rows == 3 and tbl.column_names == ['x', 'y']\n"
            "assert tbl['x'].to_pylist() == [1, 2, 3]\n"
            "vf = ds.to_vortex()\n"
            "assert vf is not None\n"
            "schema = ds.schema\n"
            "assert [f.name for f in schema] == ['x', 'y']\n"
            "p = ds.path()\n"
            "assert p.exists()\n"
            "ds_pq = raincloud.load('tiny', format='parquet')\n"
            "assert ds_pq.to_arrow().num_rows == 3\n"
            "print('ok')\n"
        ),
        env=env,
    )
    assert cp.returncode == 0, cp.stderr
    assert "ok" in cp.stdout


def test_loader_error_paths_against_wheel(built_wheel, tmp_path):
    """OfflineMiss, and refused mirror drift, against the installed wheel."""
    env, mirror = _write_loader_fixture(tmp_path)
    venv = _make_venv(tmp_path, built_wheel, extras="[vortex]")

    # 1) OfflineMiss: cache empty, RAINCLOUD_OFFLINE=1 -> can't fetch.
    # Use a dedicated cache subdir so this call cannot prime the cache for later calls.
    env_off = {**env, "RAINCLOUD_OFFLINE": "1", "RAINCLOUD_CACHE": str(tmp_path / "cache_offline")}
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "try:\n"
            "    raincloud.load('tiny').path()\n"
            "except raincloud.OfflineMiss:\n"
            "    print('offline_ok')\n"
        ),
        env=env_off,
    )
    assert cp.returncode == 0, cp.stderr
    assert "offline_ok" in cp.stdout, (
        f"OfflineMiss not raised; stdout={cp.stdout!r} stderr={cp.stderr!r}"
    )

    # 2) Drift: the mirror serves bytes whose sha is not the catalog's. The
    #    catalog is the authority, so the download is refused and nothing is
    #    cached (a fresh cache subdir so no prior clean copy short-circuits).
    (mirror / "v1" / "tiny" / "vortex" / "tiny.vortex").write_bytes(b"DRIFTED-UPSTREAM")
    env_drift = {**env, "RAINCLOUD_CACHE": str(tmp_path / "cache_drift")}
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "try:\n"
            "    raincloud.load('tiny').path()\n"
            "except raincloud.ChecksumMismatch:\n"
            "    print('drift_refused_ok')\n"
        ),
        env=env_drift,
    )
    assert cp.returncode == 0, cp.stderr
    assert "drift_refused_ok" in cp.stdout, (
        f"drift not refused; stdout={cp.stdout!r} stderr={cp.stderr!r}"
    )
    assert not list((tmp_path / "cache_drift").rglob("tiny.vortex"))

    # 3) UnknownSlug: a slug nowhere in the catalog.
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "try:\n"
            "    raincloud.load('does-not-exist')\n"
            "except raincloud.UnknownSlug:\n"
            "    print('unknown_ok')\n"
        ),
        env=env,
    )
    assert cp.returncode == 0, cp.stderr
    assert "unknown_ok" in cp.stdout, (
        f"UnknownSlug not raised; stdout={cp.stdout!r} stderr={cp.stderr!r}"
    )


def test_dataset_works_in_a_base_install(built_wheel, tmp_path):
    """Base venv: `.dataset()` scans a file:// parquet with nothing but pyarrow."""
    env, _mirror = _write_loader_fixture(tmp_path)
    venv = _make_venv(tmp_path, built_wheel)
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "d = raincloud.load('tiny', format='parquet').dataset()\n"
            "assert d.count_rows() == 3, d.count_rows()\n"
            "print('ok')\n"
        ),
        env=env,
    )
    assert cp.returncode == 0, cp.stderr
    assert "ok" in cp.stdout


def test_to_pandas_works_with_pandas_extra(built_wheel, tmp_path):
    """`[pandas]` venv: `.to_pandas()` returns a DataFrame with the expected rows/cols."""
    env, _mirror = _write_loader_fixture(tmp_path)
    venv = _make_venv(tmp_path, built_wheel, extras="[pandas]")
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "df = raincloud.load('tiny').to_pandas()\n"
            "assert list(df.columns) == ['x', 'y']\n"
            "assert len(df) == 3 and df['x'].tolist() == [1, 2, 3]\n"
            "print('ok')\n"
        ),
        env=env,
    )
    assert cp.returncode == 0, cp.stderr
    assert "ok" in cp.stdout


def _write_synth_manifest(tmp_path: Path) -> tuple[Path, Path, int]:
    """Write a tmp CSV + a synthetic sources.json describing one slug fetched
    via file://. Returns (manifest_path, csv_path, expected_rows).
    """
    import json

    csv = tmp_path / "tiny.csv"
    csv.write_text("a,b\n1,x\n2,y\n3,z\n")
    expected_rows = 3
    manifest = {
        "schema_version": 1,
        "datasets": [
            {
                "slug": "synth",
                "short_name": "Synth",
                "full_name": "Synthetic build proof",
                "description": "tmp CSV via file://",
                "license": {"spdx": "CC0-1.0"},
                "fetch": {"type": "http", "urls": [csv.as_uri()]},
                "extract": {"type": "passthrough"},
                "parse": {"reader": "csv"},
                "transform": {"handler": "tighten_types"},
                "write": {"output": "synth.parquet", "compression": "zstd"},
                "expect": {"rows": expected_rows},
                "convert": {"vortex": True},
            }
        ],
    }
    mp = tmp_path / "sources.json"
    mp.write_text(json.dumps(manifest))
    return mp, csv, expected_rows


def test_wheel_build_proof_via_load(built_wheel, tmp_path):
    """The capstone: `raincloud.load('synth', build=True)` in a [build] venv drives load →
    cache miss → mirror absent → build-fallback subprocess (file:// fetch +
    fetch→…→convert) → adopt → vortex materialization. End-to-end, hermetic."""
    manifest, _csv, expected_rows = _write_synth_manifest(tmp_path)
    venv = _make_venv(tmp_path, built_wheel, extras="[build]")
    env = _clean_env(
        RAINCLOUD_MANIFEST=str(manifest),
        RAINCLOUD_HOME=str(tmp_path / "home"),
        RAINCLOUD_CACHE=str(tmp_path / "cache"),
    )
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "ds = raincloud.load('synth', build=True)        # default vortex; convert.vortex=True\n"
            "tbl = ds.to_arrow()\n"
            f"assert tbl.num_rows == {expected_rows}, tbl.num_rows\n"
            "assert tbl.column_names == ['a', 'b']\n"
            "print('ok')\n"
        ),
        env=env,
    )
    assert cp.returncode == 0, f"stdout:\n{cp.stdout}\nstderr:\n{cp.stderr}"
    assert "ok" in cp.stdout
    # The build wrote real artifacts under $RAINCLOUD_HOME/outputs/v1/synth/
    outputs = tmp_path / "home" / "outputs" / "v1" / "synth"
    assert (outputs / "parquet" / "synth.parquet").exists()
    assert (outputs / "vortex" / "synth.vortex").exists()


def test_installed_cli_config_and_shared_storage(built_wheel, tmp_path):
    """Installed CLI + API operate outside a checkout with separate data/scratch."""
    import json

    venv = _make_venv(tmp_path, built_wheel, extras="[build]")
    manifest, _, rows = _write_synth_manifest(tmp_path)
    # Exercise the Arrow spine and use a matching independent snapshot.
    doc = json.loads(manifest.read_text())
    doc["schema_version"] = 2
    for spec in doc["datasets"]:  # v1-only fields; v2 rejects them
        spec.pop("convert", None)
        spec["write"].pop("output", None)
    manifest.write_text(json.dumps(doc))
    snapshot = tmp_path / "snapshot.json"
    snapshot.write_text(json.dumps({"schema_version": 2, "slugs": {}}))
    config = tmp_path / "machine.toml"
    env = _clean_env(RAINCLOUD_NO_CONFIG="0", RAINCLOUD_CATALOG_DIR=str(tmp_path / "catalogs"))
    cli = str(venv / "bin/raincloud")
    cp = subprocess.run([cli, "--config", str(config), "init", "--data-dir", str(tmp_path / "hdd"),
                         "--scratch-dir", str(tmp_path / "ssd"), "--cache-dir", str(tmp_path / "cache")],
                        cwd=tmp_path, env=env, capture_output=True, text=True)
    assert cp.returncode == 0, cp.stderr
    with config.open("a") as stream:
        stream.write(f'manifest = {json.dumps(str(manifest))}\nsnapshot = {json.dumps(str(snapshot))}\n')
    cp = subprocess.run([cli, "--config", str(config), "config", "show", "--json"], cwd=tmp_path,
                        env=env, capture_output=True, text=True)
    assert cp.returncode == 0, cp.stderr
    assert json.loads(cp.stdout)["data_dir"]["value"] == str(tmp_path / "hdd")
    assert not (tmp_path / "hdd").exists()
    cp = _run_py(venv, f'''
import sys
from pathlib import Path
import raincloud
assert Path(raincloud.__file__).is_relative_to({str(venv)!r})
assert "raincloud.pipeline.build" not in sys.modules
handle = raincloud.load("synth", format="arrow", config={str(config)!r}, build=True)
assert handle.to_arrow().num_rows == {rows}
assert handle.path() == Path({str(tmp_path / "hdd/v2/synth/arrow/synth.arrow.zstd")!r})
assert not Path({str(tmp_path / "cache")!r}).exists()
''', env=env)
    assert cp.returncode == 0, cp.stdout + cp.stderr
    # Offline CLI hits the same artifact; no build or second copy is needed.
    cp = subprocess.run([cli, "--config", str(config), "load", "synth", "--format", "arrow", "--offline"],
                        cwd=tmp_path, env=env, capture_output=True, text=True)
    assert cp.returncode == 0, cp.stderr
    assert Path(cp.stdout.strip()) == tmp_path / "hdd/v2/synth/arrow/synth.arrow.zstd"
    assert not (tmp_path / "cache").exists()


def test_base_wheel_catalog_cli_and_offline_read(built_wheel, tmp_path):
    """A base install packs/activates/pins catalogs and reads without builders or Git."""
    env, mirror = _write_loader_fixture(tmp_path)
    venv = _make_venv(tmp_path, built_wheel)
    # Only selection state and transport config: no loose manifest/snapshot override.
    env = _clean_env(RAINCLOUD_CATALOG_DIR=str(tmp_path / "catalogs"),
                     RAINCLOUD_OUTPUTS=str(tmp_path / "data"), RAINCLOUD_MIRROR=mirror.as_uri())
    code = f'''
import importlib.util
import json
import sys
from pathlib import Path
import raincloud
from raincloud.cli import main
from raincloud.catalogs import state
from raincloud.config import get_config
root = Path({str(tmp_path)!r})
assert Path(raincloud.__file__).is_relative_to({str(venv)!r})
assert importlib.util.find_spec("duckdb") is None
assert main(["catalog", "pack", "--manifest", str(root / "sources.json"), "--snapshot", str(root / "snapshot.json"), "--id", "wheel-fixture", "--output", str(root / "upstream")]) == 0
assert main(["catalog", "update", "--source", str(root / "upstream")]) == 0
revision = state(get_config())["active"]
assert main(["catalog", "pin", revision]) == 0
assert main(["catalog", "status"]) == 0
assert "raincloud.pipeline.handlers" not in sys.modules
assert "raincloud.pipeline.build" not in sys.modules
handle = raincloud.load("tiny", format="parquet")
assert handle.to_arrow().column("x").to_pylist() == [1, 2, 3]
assert raincloud.load("tiny", format="parquet", offline=True).path() == handle.path()
assert handle._entry.revision == revision
'''
    cp = _run_py(venv, code, env=env)
    assert cp.returncode == 0, cp.stdout + cp.stderr


def test_portable_base_install(built_wheel, tmp_path):
    """Run unchanged on Linux/macOS/Windows, including the OS file-lock branch."""
    venv = _make_venv(tmp_path, built_wheel)
    env = _clean_env()
    # Do not mask native directory defaults; explicit config confines all writes.
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    probe = (REPO_ROOT / "tests" / "installed_base_probe.py").read_text(encoding="utf-8")
    cp = subprocess.run(
        [str(venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")), "-I", "-B", "-X", "utf8", "-c", probe],
        cwd=tmp_path, env=env, capture_output=True, text=True, encoding="utf-8",
    )
    assert cp.returncode == 0, cp.stdout + cp.stderr


def test_source_distribution_rebuild(tmp_path):
    """A Python source archive excludes local state and rebuilds a complete wheel."""
    import tarfile
    import zipfile

    out = tmp_path / "source"
    subprocess.run(["uv", "build", "--sdist", "--out-dir", str(out)], cwd=REPO_ROOT, check=True)
    archive = next(out.glob("raincloud-*.tar.gz"))
    forbidden = {".big-plans", ".dispatch", ".tmp", ".git", ".venv", "outputs", "_workdir", "target", "build"}
    with tarfile.open(archive) as tar:
        names = tar.getnames()
        assert not any(forbidden.intersection(Path(name).parts) for name in names)
        assert any(name.endswith("/docs/v2/snapshot.json") for name in names)
    rebuilt = tmp_path / "rebuilt"
    subprocess.run(["uv", "build", str(archive), "--wheel", "--out-dir", str(rebuilt)], cwd=tmp_path, check=True)
    with zipfile.ZipFile(next(rebuilt.glob("*.whl"))) as wheel:
        assert "raincloud/_data/sources.json" in wheel.namelist()
        assert "raincloud/_data/snapshot.json" in wheel.namelist()
        assert "raincloud/_readers.py" in wheel.namelist()
        assert not any(forbidden.intersection(Path(name).parts) for name in wheel.namelist())

@pytest.mark.wheel
def test_packaged_data_carries_no_credentials_or_host_paths(tmp_path):
    """Path-name exclusion is not enough: the leak that mattered was INSIDE a file.

    `docs/v2/snapshot.json` is force-included into the wheel and sdist and embedded
    into the native reader, and its column statistics are verbatim upstream values.
    Two shapes escaped that way: a plaintext FTP credential carried in an upstream
    dataset, and the maintainer's own build paths, because TPC-DS's dbgen_version
    column records the generator's full command line. Assert on content, not names.
    """
    # The same patterns the snapshot writer redacts with, so this check and the
    # redaction cannot drift apart. Only the snapshot carries verbatim upstream
    # values; sources.json and datasets.md legitimately name upstream URLs.
    from raincloud.pipeline.spec import _REDACT_PATTERNS

    path = REPO_ROOT / "docs" / "v2" / "snapshot.json"
    if not path.is_file():
        pytest.skip("no docs/v2/snapshot.json")
    text = path.read_text(encoding="utf-8", errors="replace")
    for pattern, replacement in _REDACT_PATTERNS:
        # Already-redacted text (`ftp://<redacted-credential>@`) matches its own
        # pattern; a leak is a match that redaction would still change.
        leaks = [m.group(0) for m in pattern.finditer(text) if m.expand(replacement) != m.group(0)]
        assert not leaks, f"{path.name} carries unredacted text ({replacement!r}): {leaks[0][:60]!r}"
