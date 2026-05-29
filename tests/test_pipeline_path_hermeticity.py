# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Regression guard against REPO_ROOT path fragility in scripts/pipeline/.

Long-running build / hydrate / tighten stages must not crash on log or
cleanup operations when their outputs live outside the checkout (wheel
install, $RAINCLOUD_HOME or $RAINCLOUD_OUTPUTS redirect). The contract:

  * Format paths for display via `display_path()`, not `.relative_to(REPO_ROOT)`
    — the latter raises ValueError on any path outside the repo and has
    bitten us when invoked at the tail of a multi-hour run.
  * Route artifact/scratch paths through `outputs_root()`,
    `raw_downloads_root()`, `workdir_root()`, `prepared_parquet()`, etc. —
    hardcoded `REPO_ROOT / "outputs" / ...` and `REPO_ROOT / "_workdir" / ...`
    fragments wipe the wrong tree (or write to a read-only one).

This test greps the pipeline package and fails on any new occurrence. To
opt out in a tracked-data, checkout-only context, append a trailing
`# allow-repo-root-path` comment on the offending line and document why.
"""
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = REPO_ROOT / "scripts" / "pipeline"

# spec.py defines REPO_ROOT and is the one place these fragments are allowed.
ALLOWED_FILES = {"spec.py"}

OPT_OUT_TAG = "# allow-repo-root-path"

PATTERNS = {
    "relative_to_repo_root": re.compile(r"\.relative_to\(\s*REPO_ROOT\s*\)"),
    "outputs_under_repo_root": re.compile(r'REPO_ROOT\s*/\s*"outputs"'),
    "workdir_under_repo_root": re.compile(r'REPO_ROOT\s*/\s*"_workdir"'),
}


def test_pipeline_files_do_not_hardcode_repo_root_paths():
    offenders: list[str] = []
    for py in sorted(PIPELINE_DIR.rglob("*.py")):
        if py.name in ALLOWED_FILES:
            continue
        for lineno, line in enumerate(py.read_text().splitlines(), start=1):
            if OPT_OUT_TAG in line:
                continue
            for name, pat in PATTERNS.items():
                if pat.search(line):
                    rel = py.relative_to(REPO_ROOT)
                    offenders.append(f"{rel}:{lineno} [{name}]  {line.strip()}")
    assert not offenders, (
        "Found REPO_ROOT path fragility that breaks wheel installs and "
        "redirected $RAINCLOUD_HOME/$RAINCLOUD_OUTPUTS runs. Use "
        "display_path() / outputs_root() / workdir_root() instead, or add "
        f"`{OPT_OUT_TAG}` with a justification:\n  "
        + "\n  ".join(offenders)
    )
