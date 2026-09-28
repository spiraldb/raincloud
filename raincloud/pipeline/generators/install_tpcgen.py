# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Explicit local setup for the unpublished unified tpcgen-rs CLI.

Build a clean checkout at the caller's selected commit, then install beside
this Python (or --bin-dir). Generation never downloads or installs toolchains.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path

from raincloud._locking import atomic_write

from ..spec import generator_timeout


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--bin-dir", type=Path, default=Path(sys.executable).parent)
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args(argv)
    source = args.source.resolve()

    def git(*command):
        return subprocess.check_output(["git", "-C", str(source), *command], text=True).strip()

    revision = git("rev-parse", "HEAD")
    if git("status", "--porcelain", "--untracked-files=normal"):
        parser.error("source checkout must be clean before building")
    # Override inherited target locations so the installed binary is exactly
    # the one built by this invocation. cwd selects upstream's pinned toolchain.
    env = {**os.environ, "CARGO_TARGET_DIR": str(source / "target")}
    subprocess.run(["cargo", "build", "--locked", "--release", "-p", "tpcgen-cli", "-j", str(args.jobs)],
                   cwd=source, env=env, check=True, timeout=generator_timeout())
    if git("rev-parse", "HEAD") != revision or git("status", "--porcelain", "--untracked-files=normal"):
        raise RuntimeError("source checkout changed during compilation")
    name = "tpcgen-cli.exe" if sys.platform == "win32" else "tpcgen-cli"
    built = source / "target/release" / name
    reported = subprocess.check_output([str(built), "--version"], text=True, timeout=60).strip()
    if not reported.startswith("tpcgen-cli "):
        raise RuntimeError(f"unexpected CLI version: {reported}")
    version = reported.split()[1] + "+git." + revision
    data = built.read_bytes()
    destination = args.bin_dir.resolve() / name
    atomic_write(destination, data)
    destination.chmod(0o755)
    receipt = {"version": version, "sha256": hashlib.sha256(data).hexdigest()}
    atomic_write(destination.with_name(name + ".raincloud.json"), (json.dumps(receipt, indent=2) + "\n").encode())
    print(f"Installed {destination}: {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
