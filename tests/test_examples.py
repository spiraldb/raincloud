# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Guards for the runnable examples/ demos.

- compile check: every examples/*.py must byte-compile (catches syntax/indent
  rot on every default `pytest` run, no network).
- end-to-end: one small example (kepler) actually runs against real upstream
  data — load -> build -> query -> print — gated behind --run-network so the
  default suite stays hermetic. Exercises the real code path at least once.
"""
from __future__ import annotations

import os
import py_compile
import subprocess
import sys
from pathlib import Path

import pytest

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"


def _example_files() -> list[Path]:
    return sorted(EXAMPLES_DIR.glob("*.py"))


def test_examples_dir_has_scripts():
    assert _example_files(), "no example scripts found under examples/"


@pytest.mark.parametrize("path", _example_files(), ids=lambda p: p.name)
def test_example_compiles(path):
    """Each example byte-compiles (syntax/indentation/obvious-name guard)."""
    py_compile.compile(str(path), doraise=True)


@pytest.mark.network
def test_kepler_example_runs_end_to_end(tmp_path):
    """Run the kepler example for real: small upstream fetch + build + query.

    Non-blocking in CI (the realbuild job is continue-on-error). Proves the
    examples' load->materialize->query path works against live data, not just
    that the file parses.
    """
    script = EXAMPLES_DIR / "kepler_exoplanets.py"
    # Scrub the developer's ambient RAINCLOUD_* (OUTPUTS / WORKDIR /
    # STRICT_CHECKSUM / MIRROR / OFFLINE) so the run is hermetic, then set only
    # HOME + CACHE under tmp_path. Mirrors test_wheel._clean_env.
    env = {k: v for k, v in os.environ.items() if not k.startswith("RAINCLOUD_")}
    env["RAINCLOUD_HOME"] = str(tmp_path / "home")
    env["RAINCLOUD_CACHE"] = str(tmp_path / "cache")
    cp = subprocess.run(
        [sys.executable, str(script)],
        capture_output=True, text=True, env=env, timeout=600,
    )
    assert cp.returncode == 0, f"stdout={cp.stdout!r}\nstderr={cp.stderr!r}"
    assert "CONFIRMED" in cp.stdout, f"unexpected output: {cp.stdout!r}"
    assert "Earth radii" in cp.stdout, f"missing smallest-planet line: {cp.stdout!r}"
