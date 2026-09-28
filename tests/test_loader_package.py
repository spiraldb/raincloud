# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
def test_import_and_version():
    import raincloud
    assert isinstance(raincloud.__version__, str)
    assert raincloud.__version__  # non-empty
    # top-level re-exports are present and wired to the hierarchy
    from raincloud import RaincloudError, UnknownSlug
    assert issubclass(UnknownSlug, RaincloudError)


def test_version_mirrors_agree():
    """Every copy of the release version must equal `raincloud.__version__`.

    `raincloud/__init__.py` is the sole authority. `pyproject.toml` reads it via
    `[tool.hatch.version]` and the Java client reads it in `build.gradle.kts`, so
    neither can drift. Two copies cannot derive it:

      - `clients/rust/Cargo.toml` -- cargo requires a literal, so this test is
        what catches drift.
      - `CITATION.cff` -- a citation record, with nothing to derive from.
    """
    import re
    import tomllib
    from pathlib import Path

    import pytest

    import raincloud

    root = Path(__file__).resolve().parent.parent
    if not (root / "pyproject.toml").exists():  # wheel install, no checkout
        pytest.skip("not a checkout")

    cargo = root / "clients/rust/Cargo.toml"
    if cargo.exists():
        declared = tomllib.loads(cargo.read_text())["package"]["version"]
        assert declared == raincloud.__version__, (
            f"clients/rust/Cargo.toml version {declared!r} != "
            f"raincloud.__version__ {raincloud.__version__!r}"
        )

    citation = root / "CITATION.cff"
    if citation.exists():
        m = re.search(r"^version:\s*(\S+)\s*$", citation.read_text(), re.M)
        assert m, "CITATION.cff has no top-level `version:` line"
        assert m.group(1) == raincloud.__version__, (
            f"CITATION.cff version {m.group(1)!r} != "
            f"raincloud.__version__ {raincloud.__version__!r}"
        )


def test_exceptions_hierarchy():
    from raincloud.exceptions import (
        ArtifactNotFound,
        BuildToolingMissing,
        ChecksumMismatch,
        FormatUnavailable,
        MissingDependency,
        OfflineMiss,
        RaincloudError,
        UnknownSlug,
    )
    for exc in (UnknownSlug, FormatUnavailable, ArtifactNotFound, ChecksumMismatch,
                BuildToolingMissing, OfflineMiss, MissingDependency):
        assert issubclass(exc, RaincloudError)


def test_profile_schema_version_matches_its_schema():
    """`profile.py`'s payload version and `profile.schema.json` must agree.

    The schema enumerates every version a profile on disk may carry (older ones
    still validate); the code writes the newest. A bump in one place and not the
    other produces profiles that fail their own schema.
    """
    import json
    from pathlib import Path

    import pytest

    root = Path(__file__).resolve().parent.parent
    schema = root / "profile.schema.json"
    if not schema.exists():  # not a checkout
        pytest.skip("no profile.schema.json")
    pytest.importorskip("pyarrow")
    from raincloud.pipeline.profile import _PROFILE_SCHEMA_VERSION

    declared = json.loads(schema.read_text())["properties"]["schema_version"]["enum"]
    assert _PROFILE_SCHEMA_VERSION in declared and _PROFILE_SCHEMA_VERSION == max(declared), (
        f"_PROFILE_SCHEMA_VERSION {_PROFILE_SCHEMA_VERSION} is not the newest "
        f"version in profile.schema.json's enum {declared}"
    )


def test_packaged_snapshot_tracks_the_manifest_schema_version():
    """`pyproject.toml` must package the snapshot for the CURRENT layout.

    A wheel carries exactly one snapshot. Bump `schema_version` in sources.json
    without moving the force-include and the installed snapshot describes the
    previous layout, at which point the loader drops every recorded checksum as
    inapplicable and stops verifying mirror downloads. The path cannot be
    derived — hatch needs a literal — so it is checked here instead.
    """
    import json
    import re
    import tomllib
    from pathlib import Path

    import pytest

    root = Path(__file__).resolve().parent.parent
    pyproject, manifest = root / "pyproject.toml", root / "sources.json"
    if not pyproject.exists() or not manifest.exists():
        pytest.skip("not a checkout")

    version = json.loads(manifest.read_text())["schema_version"]
    config = tomllib.loads(pyproject.read_text())
    force = config["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]
    packaged = [src for src, dest in force.items() if dest.endswith("_data/snapshot.json")]
    assert packaged == [f"docs/v{version}/snapshot.json"], (
        f"wheel packages {packaged} but sources.json declares schema_version "
        f"{version}; the installed snapshot would describe another layout"
    )

    sdist = config["tool"]["hatch"]["build"]["targets"]["sdist"]["include"]
    snapshots = [e for e in sdist if re.fullmatch(r"/docs/v\d+/snapshot\.json", e)]
    assert snapshots == [f"/docs/v{version}/snapshot.json"], (
        f"sdist includes {snapshots}, expected the v{version} snapshot"
    )


def test_top_level_namespace_is_the_public_api():
    import raincloud
    assert dir(raincloud) == sorted(raincloud.__all__)
    for name in raincloud.__all__:
        assert hasattr(raincloud, name), name


def test_missing_optional_dependency_names_its_extra(monkeypatch):
    import importlib

    import pytest

    import raincloud._extras as extras
    from raincloud import BuildToolingMissing
    from raincloud.pipeline import handlers
    lines = ["osmium>=4.3; extra == 'osm'", "osmium>=4.3; extra == 'all'", "duckdb>=1.5.0; extra == 'all'",
             "duckdb>=1.5.0; extra == 'build'", "duckdb>=1.5.0; extra == 'duckdb'",
             "pyarrow>=23.0"]
    monkeypatch.setattr(extras.metadata, "requires", lambda name: lines)
    assert extras.extra_for("osmium") == "osm"      # the smallest extra wins over [all]
    assert extras.extra_for("py_arrow") is None     # a base dependency needs no extra
    real = importlib.import_module

    def fake(name, package=None):
        if name.endswith("osm_pbf_split"):
            raise ModuleNotFoundError("No module named 'osmium'", name="osmium")
        return real(name, package)
    monkeypatch.setattr(handlers, "import_module", fake)
    handlers._REGISTRY.pop("osm_pbf_split", None)
    handlers._REGISTRY["osm_pbf_split"] = "osm_pbf_split:osm_pbf_split"
    with pytest.raises(BuildToolingMissing, match=r"osm_pbf_split handler needs osmium; install `raincloud\[osm\]`"):
        handlers.get("osm_pbf_split")
