# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Wheel build + install-tier + loader-API + build-proof tests.

All tests in this module are `@pytest.mark.wheel` — gated by --run-wheel
(see tests/conftest.py). A session fixture builds the wheel once via
`uv build --wheel`; per-test helpers create throwaway venvs with `uv venv`
and install the wheel (± extras) via `uv pip install`.
"""
from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

pytestmark = pytest.mark.wheel


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
