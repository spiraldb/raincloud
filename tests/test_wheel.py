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
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.wheel


def _clean_env(**overrides: str) -> dict:
    """A subprocess env with the developer's ambient RAINCLOUD_* scrubbed.

    `os.environ.copy()` would otherwise leak the maintainer's exported
    RAINCLOUD_OUTPUTS / RAINCLOUD_WORKDIR / RAINCLOUD_STRICT_CHECKSUM / etc. into
    the venv subprocess, breaking hermeticity (artifacts written outside
    tmp_path; strict mode flipping warn-and-adopt into a hard failure). We start
    from a copy, drop every RAINCLOUD_* key, then layer on only the overrides
    the test sets explicitly.
    """
    env = {k: v for k, v in os.environ.items() if not k.startswith("RAINCLOUD_")}
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

    `extras` is e.g. "[s3]" or "[build,duckdb]" (PEP 508 form), or "" for base.
    Returns the venv root path.
    """
    venv = tmp_path / "venv"
    subprocess.run(["uv", "venv", str(venv)], check=True)
    pkg = f"raincloud{extras}" if extras else "raincloud"
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(venv / "bin" / "python"),
            f"{pkg} @ {wheel.as_uri()}",
        ],
        check=True,
    )
    return venv


def _run_py(
    venv: Path, code: str, env: dict | None = None
) -> subprocess.CompletedProcess:
    """Run a Python snippet in the venv via subprocess; capture stdout/stderr."""
    return subprocess.run(
        [str(venv / "bin" / "python"), "-c", code],
        capture_output=True,
        text=True,
        env=env,
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
            "names = ['osmium', 'pyreadstat', 'zstandard', 'py7zr', 'unlzw3', 'openpyxl']\n"
            "present = [n for n in names if importlib.util.find_spec(n) is not None]\n"
            "print(','.join(present))\n"
        ),
    )
    assert cp.returncode == 0, cp.stderr
    assert cp.stdout.strip() == "", (
        f"base install unexpectedly pulled heavy deps: {cp.stdout.strip()!r}"
    )


def test_base_install_scan_and_to_pandas_raise_missing_dependency(built_wheel, tmp_path):
    """`.scan()` and `.to_pandas()` in a base install raise MissingDependency
    (duckdb/pandas absent), without needing any I/O — the lazy guard fires
    before resolution."""
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
            "    ds.scan()\n"
            "except raincloud.MissingDependency as e:\n"
            "    errors.append(('scan', 'duckdb' in str(e).lower()))\n"
            "try:\n"
            "    ds.to_pandas()\n"
            "except raincloud.MissingDependency as e:\n"
            "    errors.append(('to_pandas', 'pandas' in str(e).lower()))\n"
            "print(errors)\n"
        ),
    )
    assert cp.returncode == 0, cp.stderr
    assert "('scan', True)" in cp.stdout and "('to_pandas', True)" in cp.stdout


def test_build_extra_installs_heavy_deps_and_build_is_available(built_wheel, tmp_path):
    """`[build]` install pulls the heavy toolchain AND _build_available() is True
    (the real import-probe succeeds when [build] is installed)."""
    venv = _make_venv(tmp_path, built_wheel, extras="[build]")
    cp = _run_py(
        venv,
        (
            "import osmium, pyreadstat, zstandard\n"
            "from raincloud import _resolve\n"
            "print(_resolve._build_available())\n"
        ),
    )
    assert cp.returncode == 0, cp.stderr
    assert cp.stdout.strip() == "True"


@pytest.mark.parametrize(
    "extra,backend",
    [
        ("[s3]", "s3fs"),
        ("[http]", "aiohttp"),
        ("[duckdb]", "duckdb"),
        ("[pandas]", "pandas"),
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

    Creates the artifacts in the outer test env (vortex-data is a base dep,
    so vortex.io.write is available here), writes a fixture snapshot + manifest
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
    venv = _make_venv(tmp_path, built_wheel)
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
    """OfflineMiss + drift warn-and-adopt against the installed wheel."""
    env, mirror = _write_loader_fixture(tmp_path)
    venv = _make_venv(tmp_path, built_wheel)

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

    # 2) Drift warn-and-adopt: the mirror serves bytes whose sha disagrees with
    #    the snapshot pin. Policy is alert-not-block — the loader warns on
    #    stderr and adopts anyway (a fresh cache subdir so no prior clean copy
    #    short-circuits). A second load must then be a pure cache hit (the pin
    #    sidecar vouches for the adopted bytes), not a re-fetch.
    (mirror / "v1" / "tiny" / "vortex" / "tiny.vortex").write_bytes(b"DRIFTED-UPSTREAM")
    env_drift = {**env, "RAINCLOUD_CACHE": str(tmp_path / "cache_drift")}
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "p1 = raincloud.load('tiny').path()\n"
            "assert p1.read_bytes() == b'DRIFTED-UPSTREAM', p1.read_bytes()\n"
            "# second load: served from cache, bytes unchanged\n"
            "p2 = raincloud.load('tiny').path()\n"
            "assert p2.read_bytes() == b'DRIFTED-UPSTREAM'\n"
            "print('drift_adopt_ok')\n"
        ),
        env=env_drift,
    )
    assert cp.returncode == 0, cp.stderr
    assert "drift_adopt_ok" in cp.stdout, (
        f"drift not adopted; stdout={cp.stdout!r} stderr={cp.stderr!r}"
    )
    assert "WARN" in cp.stderr and "drifted" in cp.stderr, (
        f"expected a drift warning on stderr; stderr={cp.stderr!r}"
    )

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


def test_scan_works_with_duckdb_extra(built_wheel, tmp_path):
    """`[duckdb]` venv: `.scan()` returns a queryable relation against a file:// parquet."""
    env, _mirror = _write_loader_fixture(tmp_path)
    venv = _make_venv(tmp_path, built_wheel, extras="[duckdb]")
    cp = _run_py(
        venv,
        (
            "import raincloud\n"
            "ds = raincloud.load('tiny', format='parquet')\n"
            "rel = ds.scan()\n"
            "rows = rel.fetchall()\n"
            "assert len(rows) == 3, rows\n"
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
    """The capstone: `raincloud.load('synth')` in a [build] venv drives load →
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
            "ds = raincloud.load('synth')        # default vortex; convert.vortex=True\n"
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
