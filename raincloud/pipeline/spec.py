# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Shared pipeline plumbing: manifest loading, data roots and path helpers,
environment-driven ceilings, row-group sizing, DuckDB connections, and the
redaction + column statistics that go into published documents.

Each DatasetSpec is a plain dict — we don't wrap it in a class, but we
provide helpers for the common field accesses so scripts at each stage
fail early on missing/typo'd keys.
"""
from __future__ import annotations

import functools
import json
import math
import os
import re
import sys
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterator

from raincloud._catalog import known_schema_versions
from raincloud.config import get_config

REPO_ROOT = Path(__file__).resolve().parents[2]


def data_root() -> Path:
    """Legacy build home used for display paths; artifacts use outputs_base().

    RAINCLOUD_HOME retains its parent-of-outputs meaning. Otherwise this is
    the checkout root or the native user data directory. No directories are created.
    """
    return get_config(repo_root=REPO_ROOT).home_dir


def _packaged_data(name: str) -> Path | None:
    """Path to a file shipped in the wheel under raincloud/_data/, or None."""
    try:
        from importlib import resources
        p = resources.files("raincloud").joinpath("_data", name)
        return Path(str(p)) if p.is_file() else None
    except (ModuleNotFoundError, FileNotFoundError):
        return None


def _default_manifest() -> Path:
    override = get_config(repo_root=REPO_ROOT).manifest
    if override:
        return override
    repo = REPO_ROOT / "sources.json"
    if repo.exists():
        return repo
    packaged = _packaged_data("sources.json")
    if packaged is not None:
        return packaged
    return repo  # let open() error point at the expected checkout path


def observations_dir(manifest: dict | None = None, *, repo_root=None, scratch=False) -> Path:
    """Selected catalog's mutable observations, separate from its bundle.

    Checkout oracles remain versioned under docs; derived checkout documents
    use its scratch docs directory. Pinned, custom and installed catalogs use
    a revision namespace in the data store. Resolve this from the operation's
    context, so subprocess parents must enter the same pinned selection.
    """
    from raincloud.catalogs import current, resolve_context
    config = get_config()
    context = current() or resolve_context(config)
    if context.source != "checkout":
        return revision_observations_dir(config, context)
    root = repo_root if repo_root is not None else REPO_ROOT
    directory = root / "docs"
    m = manifest if manifest is not None else context.manifest
    return directory if scratch else directory / f"v{m['schema_version']}"


def revision_observations_dir(config, context) -> Path:
    """Observations for `context`'s catalog revision in the configured data store.

    Where every non-checkout catalog keeps them, and where a pinned run from a
    checkout (overnight_profile) promotes, so checkout readers consult it too.
    """
    return config.data_dir / ".raincloud" / "observations" / context.bundle.revision


def default_compliance_json(manifest: dict | None = None, *, repo_root=None) -> Path:
    """Default ledger observation; explicit CLI PATH always overrides this."""
    return observations_dir(manifest, repo_root=repo_root) / "compliance.json"


# The knob grammar every lane reads identically (Python here, the Rust and Java
# sidecars): ASCII digits, an optional fraction and an optional exponent. No
# sign, no underscores or separators, no inf/nan, no units.
_KNOB_NUMBER = re.compile(r"[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?")
_ASCII_SPACE = " \t\n\r\x0b\x0c"


def _env_number(var: str, default: float) -> float | None:
    """A non-negative number from `var`: `default` when unset, None when disabled.

    Shared by every numeric knob here (seconds, bytes, rows, cells). After
    trimming ASCII whitespace, an empty value or zero means "no ceiling", for
    the rare legitimate run that outlasts any figure we could pick. Anything
    else must match `_KNOB_NUMBER`: `8GiB`, `6h`, `10,000`, `1_000`, `-1` or a
    non-ASCII digit raise rather than quietly meaning the default, since these
    guard memory and time on unattended runs. So does a value that is not
    valid UTF-8, naming the variable, and one too large to be finite.
    """
    raw = os.environ.get(var)
    if raw is None:
        return default
    try:
        raw.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError(f"{var} is not valid UTF-8; give a plain number, or 0 to disable") from None
    value = raw.strip(_ASCII_SPACE)
    if not value:
        return None
    if not _KNOB_NUMBER.fullmatch(value):
        raise ValueError(
            f"{var}={raw!r} is not a number; give plain ASCII digits with an optional "
            "fraction and exponent (bytes, rows, cells or seconds), or 0 to disable"
        )
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{var}={raw!r} is too large; give a finite number, or 0 to disable")
    return number or None


def _env_count(var: str, default: float, *, limit: int = 1 << 62, zero_disables: bool = True) -> int | None:
    """`_env_number` for a whole-number knob (bytes, rows, cells).

    A fraction below one would truncate to 0, which here means a zero ceiling
    rather than "disabled"; the Rust and Java sidecars refuse it, and so does
    this. A value above `limit` (the knob's disabled figure) is `limit`, as the
    other lanes saturate rather than overflow.
    """
    value = _env_number(var, default)
    if value is not None and value < 1:
        remedy = "or 0 to disable" if zero_disables else "at least 1"
        raise ValueError(f"{var}={os.environ.get(var)!r} rounds down to 0; give a whole number, {remedy}")
    return None if value is None else min(int(value), limit)


def row_group_target_bytes() -> int:
    """Decoded-bytes ceiling on one accumulated Parquet row group, from
    `RAINCLOUD_ROW_GROUP_TARGET_BYTES`. Default 512 MiB.

    A MEMORY GUARD, not a sizing control. Three row-group limits apply, and the
    exporter flushes on whichever it reaches first:

      - `row_group_target_encoded_bytes` (default 128 MiB encoded) sizes groups;
      - this ceiling (512 MiB DECODED Arrow bytes) stops a very wide table
        (nips-papers at ~13 KB/row) from buffering gigabytes;
      - `row_group_max_rows` / `write.row_group_size_rows` caps the row count.

    It must sit well above the decoded size of a target-sized group, or it
    silently becomes the control: at 256 MiB it capped TPC-H lineitem at
    1,589,248 rows (168.8 decoded B/row) and held its groups 26% under target.
    Decoded runs 1.2-2.7x encoded across the catalog's shapes, so 512 MiB clears
    a 128 MiB encoded target on every one of them.

    Decoded, because it is the one size that is deterministic for a given
    canonical: independent of codec and codec version. Artifacts here are gated
    on sha256, so a threshold that moved with a zstd upgrade would change
    row-group boundaries and therefore the file's hash.
    """
    value = _env_count("RAINCLOUD_ROW_GROUP_TARGET_BYTES", float(512 << 20))
    return value if value is not None else (1 << 62)


def row_group_target_encoded_bytes() -> int:
    """Target ENCODED size of one Parquet row group, from
    `RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES`. Default 128 MiB.

    Every Parquet lane sizes groups against this figure, measured differently.
    arrow-rs's own `max_row_group_bytes` counts closed pages after compression,
    so the Rust lane plans groups on an uncompressed first pass, where its
    measure is the encoded size. parquet-arrow-java counts closed pages at their
    compressed size, so parquet@java groups come out larger. pyarrow will not
    report an in-progress size (`writer.metadata` is None until close), so the
    Python lane estimates from a probe and checks the median group it wrote.

    Encoded, not compressed: it is codec-independent, so row-group boundaries do
    not move when zstd changes, and these artifacts are gated on sha256.
    """
    value = _env_count("RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES", float(128 << 20))
    return value if value is not None else (1 << 62)


def row_group_max_rows() -> int:
    """Backstop cap on rows in one Parquet row group, from
    `RAINCLOUD_ROW_GROUP_MAX_ROWS`. Default 10,000,000.

    Byte size decides a row group; this only catches the shape bytes cannot. A
    table of one narrow integer column encodes to roughly a byte a row, so a
    128 MiB target alone would put ~134M rows in a single group — fine on disk,
    awful for read parallelism and for anything that materializes a group.

    A dataset's cap is `row_group_cap(spec)`; this is its default.
    """
    value = _env_count("RAINCLOUD_ROW_GROUP_MAX_ROWS", 10_000_000.0, limit=(1 << 31) - 1)
    return value if value is not None else (1 << 31) - 1


def row_group_cap(spec: dict) -> int:
    """The row cap one dataset's Parquet row groups are written with.

    A recipe's `write.row_group_size_rows` wins over `RAINCLOUD_ROW_GROUP_MAX_ROWS`
    in every writer lane: parquet@py reads it here, and the sidecar exporter
    hands it to the Rust and Java writers as that variable in their environment,
    so one recipe and one environment give every lane one row-group plan.
    """
    return spec_field(spec, "write.row_group_size_rows") or row_group_max_rows()


def row_group_probe_rows() -> int:
    """Rows encoded to estimate bytes-per-row before the real export.

    The Rust lane measures an uncompressed planning pass and needs no probe;
    pyarrow cannot, so encode a sample and scale. Costs about one row group,
    not the second full pass a measure-then-rewrite would. The probe cannot be
    disabled, so `0` or an empty value is rejected.
    """
    value = _env_count("RAINCLOUD_ROW_GROUP_PROBE_ROWS", 262144.0, limit=(1 << 31) - 1, zero_disables=False)
    if value is None:
        raise ValueError("RAINCLOUD_ROW_GROUP_PROBE_ROWS must be a positive row count; give a whole number >= 1")
    return value


# Parquet write options every Parquet writer receives the same way. The page
# knobs differ from the row-group knobs in one respect: UNSET means each
# writer's own default, not a raincloud figure, because the four libraries'
# defaults differ (arrow-rs and parquet-java write a page index, pyarrow and
# Hardwood do not) and no one figure leaves every lane's files as they are.
# Set, a knob reaches every lane, and a writer whose library cannot do what it
# asks fails that export rather than writing something else.
PARQUET_COMPRESSION = "RAINCLOUD_PARQUET_COMPRESSION"
PARQUET_STATISTICS = "RAINCLOUD_PARQUET_STATISTICS"
PARQUET_PAGE_INDEX = "RAINCLOUD_PARQUET_PAGE_INDEX"
PARQUET_PAGE_BYTES = "RAINCLOUD_PARQUET_PAGE_BYTES"
PARQUET_PAGE_ROWS = "RAINCLOUD_PARQUET_PAGE_ROWS"
PARQUET_CODECS = ("zstd", "snappy", "gzip", "lz4", "brotli", "none")
_PAGE_LIMIT = (1 << 31) - 1  # parquet-java and Hardwood take an int
_TRUE, _FALSE = {"1", "true", "yes", "on"}, {"0", "false", "no", "off"}


def _env_switch(var: str) -> bool | None:
    """An on/off knob: None when unset or empty, else one of 1/true/yes/on or
    0/false/no/off (any case); anything else raises, naming the variable."""
    raw = os.environ.get(var)
    value = (raw or "").strip(_ASCII_SPACE).lower()
    if not value:
        return None
    if value in _TRUE | _FALSE:
        return value in _TRUE
    raise ValueError(f"{var}={raw!r} is not a switch; give 1 or 0 (true/false, yes/no, on/off), "
                     "or leave it unset for each writer's own default")


def _env_page_count(var: str) -> int | None:
    """A page-size knob: None when unset (the writer's default); the count
    grammar otherwise, where 0 or empty means no limit."""
    if os.environ.get(var) is None:
        return None
    value = _env_count(var, 0.0, limit=_PAGE_LIMIT)
    return _PAGE_LIMIT if value is None else value


@dataclass(frozen=True)
class ParquetOptions:
    """What one dataset's Parquet file is written with, in every writer lane.

    `compression` and `statistics` are the recipe's `write.compression` and
    `write.statistics`. `page_index` (a ColumnIndex and OffsetIndex for every
    column chunk), `page_bytes` (the data page size target) and `page_rows`
    (the data page row limit) come from `RAINCLOUD_PARQUET_PAGE_INDEX`,
    `_PAGE_BYTES` and `_PAGE_ROWS`; None leaves the writer's own default.
    """
    compression: str = "zstd"
    statistics: bool = True
    page_index: bool | None = None
    page_bytes: int | None = None
    page_rows: int | None = None

    def chosen(self) -> dict[str, str]:
        """The page knobs that are set, as a writer's toolchain records them: a
        recorded failure is repeated only under the same options."""
        return {name: str(value) for name, value in (
            ("parquet_page_index", None if self.page_index is None else int(self.page_index)),
            ("parquet_page_bytes", self.page_bytes), ("parquet_page_rows", self.page_rows))
            if value is not None}

    def env(self) -> dict[str, str]:
        """The options as a sidecar writer reads them from its environment."""
        env = {PARQUET_COMPRESSION: self.compression, PARQUET_STATISTICS: str(int(self.statistics))}
        for var, value in ((PARQUET_PAGE_INDEX, self.page_index), (PARQUET_PAGE_BYTES, self.page_bytes),
                           (PARQUET_PAGE_ROWS, self.page_rows)):
            if value is not None:
                env[var] = str(int(value))
        return env


def parquet_page_options() -> ParquetOptions:
    """The page knobs from the environment, with the recipe fields at their defaults."""
    return ParquetOptions(page_index=_env_switch(PARQUET_PAGE_INDEX),
                          page_bytes=_env_page_count(PARQUET_PAGE_BYTES),
                          page_rows=_env_page_count(PARQUET_PAGE_ROWS))


def parquet_options(spec: dict) -> ParquetOptions:
    """The Parquet write options for `spec`: its recipe's `write.compression`
    and `write.statistics`, and the page knobs from the environment."""
    compression = spec_field(spec, "write.compression", "zstd")
    if compression not in PARQUET_CODECS:
        raise ValueError(f"write.compression={compression!r} is not one of {', '.join(PARQUET_CODECS)}")
    statistics = spec_field(spec, "write.statistics", True)
    page = parquet_page_options()
    if page.page_index and not statistics:
        raise ValueError(f"{PARQUET_PAGE_INDEX}=1 asks for page statistics, but the recipe sets "
                         "write.statistics to false")
    return replace(page, compression=compression, statistics=bool(statistics))


def max_decompressed_bytes() -> int | None:
    """Ceiling on a single in-memory decompression, from
    `RAINCLOUD_MAX_DECOMPRESSED_BYTES`. `0` (or empty) disables it. Default 4 GiB.
    """
    return _env_count("RAINCLOUD_MAX_DECOMPRESSED_BYTES", float(4 << 30))


def max_table_cells() -> int | None:
    """Ceiling on rows x columns for handlers that materialize a whole table,
    from `RAINCLOUD_MAX_TABLE_CELLS`. `0` (or empty) disables it.

    Guards the handlers whose output width is derived from the input rather than
    declared, where one ragged line silently multiplies the table. Default 50M
    cells -- four orders of magnitude above anything such a handler serves today.
    """
    return _env_count("RAINCLOUD_MAX_TABLE_CELLS", 50_000_000.0)


def check_env_knobs() -> None:
    """Read every numeric knob now, so a malformed one fails in the first second
    of an unattended run instead of after an hours-long fetch. Raises the
    knob's ValueError."""
    for knob in (row_group_target_bytes, row_group_target_encoded_bytes, row_group_max_rows,
                 row_group_probe_rows, max_decompressed_bytes, max_table_cells, fetch_deadline,
                 generator_timeout, sidecar_timeout, export_timeout, export_memory):
        knob()


def fetch_deadline() -> float | None:
    """Wall-clock ceiling for a single download, from `RAINCLOUD_FETCH_DEADLINE`.

    `urlopen(timeout=...)` bounds each socket operation, not the transfer: a
    server dripping one byte just inside the socket timeout holds the connection
    indefinitely, and the 3x retry multiplies that. Builds often run unattended, so
    a stuck fetch has to end by itself. Default 6h — far beyond any single file
    in the catalog, yet finite.
    """
    return _env_number("RAINCLOUD_FETCH_DEADLINE", 21600.0)


def generator_timeout() -> float | None:
    """Subprocess ceiling for dataset generators, from `RAINCLOUD_GENERATOR_TIMEOUT`.

    Generators run long and unattended, so they get a finite ceiling like the
    conformance sidecars. Default 6h covers TPC-DS at the scale factors in the
    catalog.
    """
    return _env_number("RAINCLOUD_GENERATOR_TIMEOUT", 21600.0)


def sidecar_timeout() -> float | None:
    """Per-invocation subprocess timeout (seconds) for the reference-writer /
    reference-reader sidecars, from `RAINCLOUD_SIDECAR_TIMEOUT`.

    Default 1800s (30 min) — generous for the small compliance set yet a hard
    ceiling so a deadlocked/blocked real Rust/JVM binary degrades to a measured
    `fail` ("timed out") instead of hanging the step. Set the env var to `0` (or
    empty) to disable the timeout entirely for a genuinely long-running
    reference impl on a large slug."""
    return _env_number("RAINCLOUD_SIDECAR_TIMEOUT", 1800.0)


def export_timeout() -> float | None:
    """Ceiling on one export (seconds), from `RAINCLOUD_EXPORT_TIMEOUT`.

    Covers every writer: an in-process one in its child process, and a sidecar
    writer's subprocess. Default 6 h, like the fetch and generator ceilings --
    far beyond any export in the catalog, yet finite. `0` (or empty) disables it.
    """
    return _env_number("RAINCLOUD_EXPORT_TIMEOUT", 21600.0)


def export_memory() -> int | None:
    """Ceiling on one in-process export's resident memory (bytes), from
    `RAINCLOUD_EXPORT_MEMORY`.

    The box is shared, so a writer that balloons (Vortex's dictionary layout on
    a huge binary value grew past 48 GB before the kernel's out-of-memory
    killer ended the whole build) is stopped and recorded instead. Default:
    half of physical memory where it can be read (Linux, macOS), else no
    ceiling. `0` (or empty) disables it.
    """
    half = _half_physical_memory()
    value = _env_count("RAINCLOUD_EXPORT_MEMORY", float(half) if half else float(1 << 62))
    return None if value is None or value >= 1 << 62 else value


def _half_physical_memory() -> int | None:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") // 2
    except (AttributeError, OSError, ValueError):
        return None


def load_manifest(path: Path | None = None) -> dict:
    """The manifest this call should read (see `select_manifest`)."""
    return select_manifest(path)[0]


def select_manifest(path: Path | None = None) -> tuple[dict, str]:
    """(manifest, where it came from): the one declaration of which manifest a
    call reads, in precedence order:

    1. an explicit `path` — read from disk;
    2. the operation's active context (`catalogs.current()`);
    3. a selected catalog: `catalog` is set to something other than "auto", or
       no manifest file is configured and a catalog has been activated
       (`raincloud catalog use`) — resolved through `resolve_context`;
    4. otherwise the configured manifest file, the checkout's sources.json, or
       the copy packaged in the wheel.

    File reads check the top level and `schema_version` here, raising
    ValueError naming the file (OSError when it cannot be read). Context
    manifests come from a catalog bundle, which `_bundle` has already
    validated on load. `where` is the file's path, or the catalog's manifest
    path (or source) and which kind of catalog it is.
    """
    if path is None:
        from raincloud.catalogs import current, resolve_context, state
        context = current()
        config = get_config(repo_root=REPO_ROOT)
        if context is None and (config.catalog != "auto" or (config.manifest is None and state(config)["active"])):
            context = resolve_context(config, repo_root=REPO_ROOT)
        if context is not None:
            return context.manifest, f"{context.manifest_path or context.source} (catalog {context.source})"
    p = Path(path) if path else _default_manifest()
    with open(p) as f:
        m = json.load(f)
    if not isinstance(m, dict):
        raise ValueError(f"{p}: the manifest must be a JSON object, not {type(m).__name__}")
    known = known_schema_versions()
    version = m.get("schema_version")
    if type(version) is not int or version not in known:
        raise ValueError(
            f"unsupported schema_version in {p}: {version!r} "
            f"(expected {' or '.join(str(v) for v in known)})"
        )
    return m, str(p)


def outputs_base() -> Path:
    return get_config(repo_root=REPO_ROOT).data_dir


def outputs_root(manifest: dict | None = None) -> Path:
    """Version-scoped output root: <outputs_base>/v{schema_version}/.

    `outputs_base()` is the shared configured artifact directory. A checkout
    defaults to `<repo>/outputs/v{n}/`; installed use defaults to native user data.

    The selected catalog determines the version; v1 and v2 artifacts remain
    in separate directories and are never migrated implicitly.
    """
    m = manifest if manifest is not None else load_manifest()
    return outputs_base() / f"v{m['schema_version']}"


def raw_downloads_root() -> Path:
    """Unversioned raw-download cache: $RAINCLOUD_RAW_DOWNLOADS or
    <outputs_base>/raw_downloads."""
    return get_config(repo_root=REPO_ROOT).raw_dir


_WARNED_MARKERS: set[tuple[Path, str]] = set()


def raw_slug_dir(slug: str, recipe: dict | None = None) -> Path:
    """Resolve raw bytes for the effective recipe, including caller overrides."""
    from raincloud._bundle import digest, encode
    from raincloud.catalogs import current, selected_context
    root = raw_downloads_root() / slug
    context = current() or selected_context()
    if context is None:
        return root
    selected = next((s for s in context.manifest["datasets"] if s["slug"] == slug), None)
    recipe = recipe if recipe is not None else selected
    if recipe is None:
        return root
    if recipe["slug"] != slug:
        raise ValueError("raw cache slug does not match recipe")
    # Same catalog + fetch recipe reuses raw bytes across schema versions and
    # metadata/transform updates. Custom catalogs cannot consume each other's raw cache.
    key = digest(encode({"catalog_id": context.bundle.catalog_id, "fetch": recipe.get("fetch", {})}))
    marker = root / ".fetch-recipe.json"
    if marker.is_file():
        try:
            data = json.loads(marker.read_text())
            if not isinstance(data, dict):
                raise ValueError(f"not a JSON object ({type(data).__name__})")
            if data.get("fetch_recipe") == key:
                return root
        except (OSError, ValueError) as exc:
            # The bytes under `root` cannot be proven to belong to this recipe,
            # so they are treated as another recipe's, and this recipe's raw
            # bytes live in their own generation -- which a fetch may then take
            # hours to fill. Said once per marker, not on every lookup.
            if (marker, key) not in _WARNED_MARKERS:
                _WARNED_MARKERS.add((marker, key))
                print(f"[warn] cannot read raw-cache marker {marker}: {exc}; its bytes are treated as "
                      f"another recipe's, and raw bytes for this recipe resolve to {root / '.recipes' / key}",
                      file=sys.stderr)
    elif (context.legacy and selected is not None
          and recipe.get("fetch", {}) == selected.get("fetch", {})):
        # Unmarked legacy bytes belong only to the selected recipe. An
        # override must fetch into its own generation before recording a pin.
        return root
    return root / ".recipes" / key


def workdir_root() -> Path:
    """Extract/scratch root: the configured `scratch_dir` (see config.py for the
    checkout and installed defaults)."""
    return get_config(repo_root=REPO_ROOT).scratch_dir


def recipe_workdir_root(recipe: dict, manifest: dict | None = None) -> Path:
    """Derive a recipe's scratch root from the unscoped configured root.

    This is read-only, and includes effective caller overrides in the recipe.
    Stage functions use workdir_root() inside recipe_scratch() instead.
    """
    from raincloud._bundle import recipe_hash
    manifest = manifest if manifest is not None else load_manifest()
    specs = {d["slug"]: d for d in manifest.get("datasets", [])}
    return workdir_root() / ".recipes" / recipe_hash(recipe, manifest["schema_version"], specs=specs)


@contextmanager
def recipe_scratch(recipe: dict):
    """Scope all stages/handlers to one recipe, once at an operation boundary.

    The caller must acquire resource locks on the configured roots first.
    Keep the selected catalog frozen while overriding only scratch_dir; raw
    generations and durable destinations retain their original configuration.
    """
    from dataclasses import replace

    from raincloud.catalogs import current, operation, resolve_context
    config = get_config()
    context = current() or resolve_context(config)
    scoped = replace(config, scratch_dir=recipe_workdir_root(recipe, context.manifest))
    with operation(scoped, context):
        yield scoped.scratch_dir


def display_path(p) -> str:
    """Render `p` relative to data_root() for LOGS; absolute if outside it.

    Not for anything written into a published or tracked file: use
    `published_path` / `scrub_published_text`, which cover every configured root.
    """
    p = Path(p)
    try:
        return str(p.relative_to(data_root()))
    except ValueError:
        return str(p)


def output_format_dir(slug: str, fmt: str = "parquet",
                      manifest: dict | None = None) -> Path:
    """outputs/v{n}/<slug>/<fmt>/ — the format-scoped output directory.

    `fmt` is a free-form identifier describing the file layout: today
    "arrow", "parquet", "vortex" (one file each, whichever writer made it), or
    a future partitioned layout (e.g. "parquet-by-date").
    The format dir lets one slug carry multiple representations of the
    same logical dataset without filename collisions.
    """
    return outputs_root(manifest) / slug / fmt


def prepared_artifact(slug: str, fmt: str, manifest: dict | None = None) -> Path:
    """outputs/v{n}/<slug>/<fmt>/<slug>.<ext> — the dataset's one file of an
    artifact format (`_registry.FORMATS`), whichever writer made it."""
    from raincloud._cache import EXT
    return output_format_dir(slug, fmt, manifest) / f"{slug}.{EXT[fmt]}"


def prepared_parquet(slug: str, manifest: dict | None = None) -> Path:
    """outputs/v{n}/<slug>/parquet/<slug>.parquet — the canonical prepared parquet."""
    return output_format_dir(slug, "parquet", manifest) / f"{slug}.parquet"


def prepared_vortex(slug: str, manifest: dict | None = None) -> Path:
    """outputs/v{n}/<slug>/vortex/<slug>.vortex — the canonical converted vortex."""
    return output_format_dir(slug, "vortex", manifest) / f"{slug}.vortex"


def prepared_arrow(slug: str, manifest: dict | None = None) -> Path:
    """outputs/v{n}/<slug>/arrow/<slug>.arrow.zstd — the canonical Arrow IPC
    (zstd-compressed) artifact. Always produced under the v2 layout; it's the
    spine every exporter reads from, not an `export.formats` target."""
    return output_format_dir(slug, "arrow", manifest) / f"{slug}.arrow.zstd"


def iter_datasets(manifest: dict, *, slug: str | None = None) -> Iterator[dict]:
    for d in manifest["datasets"]:
        if slug and d.get("slug") != slug: continue
        yield d


def spec_field(spec: dict, dotted: str, default: Any = None) -> Any:
    """Safe nested getter — spec_field(spec, "fetch.urls", [])."""
    cur = spec
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


# Column statistics are PUBLISHED: they land in docs/v{n}/snapshot.json, which is
# tracked, force-included into the wheel and sdist, and embedded into the native
# reader binary. Upstream data is not controlled, and a statistic is a verbatim value
# from it -- so two shapes must never survive into a published artifact:
#   - a credential carried in a URL, which some upstream datasets genuinely contain:
#     userinfo (`user:pass@`, or a bare token `TOKEN@`) and signed-URL / token
#     query parameters;
#   - a host-absolute path, which leaks the machine that ran the build (TPC-DS's
#     dbgen_version records the generator's full command line, argv paths included).
#     Covered: the host-path prefixes below and every configured root (data, raw,
#     scratch, cache, catalogs, home, the temp dir), resolved and as configured.
#     A path under some other root (/usr, /nix, ...) is not recognised. So the
#     redacted output depends on the regenerating machine's configured roots
#     and TMPDIR. A bare prefix also redacts a genuine leading URL-path value
#     (`/media/img.jpg`): over-redaction is the intended side of that trade.
#
# A filesystem path starts at a boundary: start of text, whitespace, a quote, one
# of `=,;:[(<` (so `PATH=/usr:/home/x` is caught), or `file://`. Requiring one avoids
# mangling a URL whose path merely contains such a segment -- https://www.nyc.gov/home/...
# is a real source_url in this catalog and must survive untouched.
_PATH_BOUNDARY = r"(?:(?<=^)|(?<=[\s\"'`=,;:\[(<])|(?<=file://))"
_HOST_PATH_PREFIXES = ("home", "Users", "root", "srv", "tmp", "var/tmp", "var/folders",
                       "private", "mnt", "media", "opt", "scratch")
_CREDENTIAL_QUERY_KEY = (
    r"(?:[A-Za-z0-9_.-]*(?:token|secret|passw(?:or)?d|api[_-]?key|credential|signature)"
    r"[A-Za-z0-9_.-]*|key|sig|auth|pwd|x-amz-[A-Za-z-]+|x-goog-[A-Za-z-]+)"
)
_REDACT_PATTERNS = (
    # userinfo with or without a password, inside the authority. A password
    # may contain '/', but never '?' or '#' (those end the authority), and one
    # that starts with digits then '/' is a port followed by a path, so
    # `https://h:443/@user` and `https://h:443/p?e=a@b.com` survive.
    (re.compile(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*://)[^/\s:@?#]+(?::(?![0-9]*/)[^\s@?#]*)?@"),
     r"\g<scheme><redacted-credential>@"),
    (re.compile(rf"(?i)(?<=[?&;])(?P<key>{_CREDENTIAL_QUERY_KEY})=[^&#\s\"']+"),
     r"\g<key>=<redacted-credential>"),
    (re.compile(rf"{_PATH_BOUNDARY}/(?:{'|'.join(_HOST_PATH_PREFIXES)})(?=/|$|[\s\"'])[^\s\"']*"),
     "<redacted-path>"),
    (re.compile(rf"{_PATH_BOUNDARY}[A-Za-z]:\\Users\\[^\s\"']*"), "<redacted-path>"),
)
_ROOT_FOLLOW = r"(/|(?=$|[\s\"'`,;:)\]>]))"


def _configured_roots() -> dict[str, str]:
    """Every configured root, as configured and resolved, -> its published token.

    The home root renders as a relative path (""), a root under it as its path
    relative to home (so a symlinked `outputs` that resolves onto another disk
    still reads `outputs/...`), and any other root as `<data>`, `<raw>`,
    `<scratch>`, `<cache>`, `<catalogs>` or `<tmp>`. The filesystem root is
    never a root here. Resolved once per configuration and temp dir: it runs
    for every published string.
    """
    return dict(_roots_for(get_config(repo_root=REPO_ROOT), tempfile.gettempdir()))


@functools.lru_cache(maxsize=32)
def _roots_for(config, tmp: str) -> tuple[tuple[str, str], ...]:
    home = Path(config.home_dir)
    named = (("", home), ("<data>", config.data_dir), ("<raw>", config.raw_dir),
             ("<scratch>", config.scratch_dir), ("<cache>", config.cache_dir),
             ("<catalogs>", config.catalog_dir), ("<tmp>", Path(tmp)))
    out: dict[str, str] = {}
    for token, root in named:
        root = Path(root)
        if token:
            try:
                token = str(root.relative_to(home))
            except ValueError:
                pass
        for form in (root, root.resolve()):
            text = str(form).rstrip("/\\")
            if len(text) > 1:
                out.setdefault(text, token)
    return tuple(out.items())


_ROOT_PATTERNS: dict[tuple, re.Pattern] = {}


def _root_pattern(roots: dict[str, str], *, whole: bool = False) -> re.Pattern:
    """Match any of `roots` at a path boundary; `whole` also takes the rest of the path.

    Longest first, so a root nested in another wins the alternation. `roots`
    always holds the home root, so the alternation is never empty.
    """
    key = (tuple(sorted(roots)), whole)
    if key not in _ROOT_PATTERNS:
        alternation = "|".join(re.escape(r) for r in sorted(roots, key=len, reverse=True))
        tail = r"[^\s\"']*" if whole else ""
        _ROOT_PATTERNS[key] = re.compile(rf"{_PATH_BOUNDARY}({alternation}){_ROOT_FOLLOW}{tail}")
    return _ROOT_PATTERNS[key]


def redact_published_value(v: Any) -> Any:
    """Strip credentials and host-absolute paths from a value bound for a published file.

    Applied to statistics, never to dataset content -- raincloud republishes metadata
    about datasets, not the datasets themselves, so this only ever touches min/max and
    top-value samples. See `_REDACT_PATTERNS` for exactly what is covered.
    """
    if not isinstance(v, str):
        return v
    for pattern, replacement in _REDACT_PATTERNS:
        v = pattern.sub(replacement, v)
    if "/" in v or "\\" in v:
        v = _root_pattern(_configured_roots(), whole=True).sub("<redacted-path>", v)
    return v


def scrub_published_text(text: str) -> str:
    """Make free text (a reader error, a note) safe for a tracked document.

    Every configured root is rewritten to its stable token (see
    `_configured_roots`), keeping the part of the path that says which artifact
    was meant, then the result goes through `redact_published_value` for
    credentials and any other host path.
    """
    if not text:
        return text
    roots = _configured_roots()
    def token(m: re.Match) -> str:
        name, sep = roots[m.group(1)], m.group(2)
        if name:
            return name + sep
        return "" if sep else "."
    return redact_published_value(_root_pattern(roots).sub(token, text))


def published_path(p) -> str:
    """`p` as it may appear in a tracked document: see `scrub_published_text`."""
    return scrub_published_text(str(p))


def _json_safe(v: Any) -> Any:
    """Coerce a parquet stat value (min/max) to a JSON-encodable, publishable form.
    bytes → utf-8 string when valid; date/datetime → ISO format; bool/int/str pass
    through; a finite float passes through and a non-finite one (a legal parquet
    double min/max) becomes None, since strict JSON has no Infinity/NaN; anything
    else stringified. Credentials and host-absolute paths are redacted -- see
    `redact_published_value`."""
    import datetime as _dt
    if v is None or isinstance(v, (bool, int)):
        return v
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, str):
        return redact_published_value(v)
    if isinstance(v, (bytes, bytearray)):
        try:
            return redact_published_value(v.decode("utf-8"))
        except UnicodeDecodeError:
            return v.hex()
    if isinstance(v, (_dt.date, _dt.datetime, _dt.time)):
        return v.isoformat()
    return redact_published_value(str(v))


def read_column_stats(parquet: Path) -> list[dict] | None:
    """Per-column metadata aggregated across row-groups. Returns one dict per
    top-level column with: name, type, length (compressed bytes), null_count,
    min, max. min/max are JSON-safe via `_json_safe`. None when the file is
    missing or unreadable.

    Aggregation: for multi-leaf columns (struct / list-of-struct), `length`
    is summed across leaves; null_count and min/max are dropped (stats live
    on leaves and aren't meaningfully aggregable for nested types).

    Used by both `browse.py` (live parquet stats) and `docs.py snapshot`
    (capture into the snapshot for TUI fallback when the parquet isn't local).
    """
    if not parquet.exists():
        return None
    try:
        import pyarrow.parquet as pq
        pf = pq.ParquetFile(parquet)
        meta = pf.metadata
        arrow_schema = pf.schema_arrow
        phys = meta.schema
    except Exception:
        return None

    # Map each physical leaf to the top-level Arrow field that owns it. A leaf
    # path is dotted ("addr.city"), but so is the name of a top-level column
    # that simply contains a dot -- and splitting on the first dot silently
    # assigns "my.col" to a nonexistent field "my", leaving the real one with no
    # leaves at all and publishing length=0, null_count=0 for it. Match against
    # the declared field names instead: exact first, then longest prefix.
    names = [f.name for f in arrow_schema]
    by_top: dict[str, list[int]] = {n: [] for n in names}
    for ci in range(len(phys.names)):
        path = phys.column(ci).path
        if path in by_top:
            owner = path
        else:
            owner = None
            for n in names:
                if path.startswith(n + ".") and (owner is None or len(n) > len(owner)):
                    owner = n
        if owner is not None:
            by_top[owner].append(ci)

    out: list[dict] = []
    for field in arrow_schema:
        indices = by_top.get(field.name, [])
        length = 0
        for rg_i in range(meta.num_row_groups):
            for ci in indices:
                length += meta.row_group(rg_i).column(ci).total_compressed_size
        mn: Any = None
        mx: Any = None
        nulls: int | None = 0
        if len(indices) == 1:
            ci = indices[0]
            any_stats = False
            partial_nulls = False
            # Bounds from some groups only are not the column's bounds: a group
            # without statistics, or whose bound could not be compared, makes
            # min/max unknown. An all-null group has none to contribute.
            partial_bounds = False
            for rg_i in range(meta.num_row_groups):
                group = meta.row_group(rg_i)
                s = group.column(ci).statistics
                if s is None or s.null_count is None:
                    # One group without a count makes the total a partial sum;
                    # publish unknown rather than an undercount.
                    partial_nulls = True
                if s is None:
                    partial_bounds = True
                    continue
                any_stats = True
                if s.has_min_max:
                    try:
                        if mn is None or s.min < mn:
                            mn = s.min
                        if mx is None or s.max > mx:
                            mx = s.max
                    except (OverflowError, ValueError, TypeError):
                        partial_bounds = True
                elif s.null_count is None or s.null_count != group.num_rows:
                    partial_bounds = True
                if s.null_count is not None:
                    nulls = (nulls or 0) + s.null_count
            if not any_stats or partial_nulls:
                nulls = None
            if partial_bounds:
                mn = mx = None
        else:
            nulls = None  # nested types: stats live on leaves, not aggregable
        out.append({
            "name": field.name,
            "type": str(field.type),
            "length": length,
            "null_count": nulls,
            "min": _json_safe(mn),
            "max": _json_safe(mx),
        })
    return out


def is_hydrated(spec: dict) -> bool:
    """A dataset derived by fetching the URLs in a parent's columns.

    Built only when asked for by name: its bytes come from the open web.
    """
    return bool((spec.get("derive") or {}).get("hydrate"))
