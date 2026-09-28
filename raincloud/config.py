# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Optional machine settings shared by the loader and build pipeline.

Reading settings never creates directories. A Config is an immutable snapshot;
keep it for the lifetime of an operation to avoid changes in ambient settings.
"""
from __future__ import annotations

import os
import re
import tomllib
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from platformdirs import site_config_dir, user_cache_path, user_config_path, user_data_path

_PATHS = {"data_dir", "scratch_dir", "raw_dir", "cache_dir", "manifest", "snapshot", "catalog_dir"}
# retry_errors: a build re-attempts a format whose writer, with this toolchain,
# already failed to write it at the recipe (`export.run_exporters`).
_BOOLS = {"offline", "retry_errors"}
# Comma-separated writer names, most preferred first (e.g. "rs,java,py").
_LISTS = {"export_priority"}
_KEYS = _PATHS | _BOOLS | _LISTS | {"mirror", "catalog", "catalog_url"}
# Named catalog selectors; anything else is a revision id (or its prefix) or a directory.
_SELECTORS = {"auto", "active", "checkout", "bundled", "local"}
# A revision abbreviated as overviews print it (12 characters), or longer.
REVISION_PREFIX = re.compile(r"[0-9a-f]{4,63}")


def _writers(value: str | list | tuple) -> tuple[str, ...]:
    """Parse "rs,java,py" (or a list of names) into a writer tuple, ignoring blanks."""
    if isinstance(value, str):
        value = value.split(",")
    elif not isinstance(value, (list, tuple)) or not all(isinstance(v, str) for v in value):
        raise ValueError(f"writer priority must be a list of writer names, not {value!r}")
    return tuple(part.strip() for part in value if part.strip())


def redact_url(value: str) -> str:
    """`value` without credentials, query parameters or fragment, for messages."""
    try:
        parts = urlsplit(value)
        return urlunsplit((parts.scheme, parts.netloc.rsplit("@", 1)[-1], parts.path, "", ""))
    except ValueError:
        return "<redacted>"


_ENV = {
    "catalog_dir": "RAINCLOUD_CATALOG_DIR", "catalog": "RAINCLOUD_CATALOG",
    "catalog_url": "RAINCLOUD_CATALOG_URL",
    "data_dir": "RAINCLOUD_OUTPUTS", "scratch_dir": "RAINCLOUD_WORKDIR",
    "raw_dir": "RAINCLOUD_RAW_DOWNLOADS", "cache_dir": "RAINCLOUD_CACHE",
    "manifest": "RAINCLOUD_MANIFEST", "snapshot": "RAINCLOUD_SNAPSHOT",
    "mirror": "RAINCLOUD_MIRROR", "offline": "RAINCLOUD_OFFLINE",
    "export_priority": "RAINCLOUD_EXPORT_PRIORITY", "retry_errors": "RAINCLOUD_RETRY_ERRORS",
}


def config_path() -> Path:
    return user_config_path("raincloud", appauthor=False) / "config.toml"


def system_config_paths() -> list[Path]:
    """Low-to-high priority machine defaults; user settings are applied last."""
    return [Path(p) / "config.toml" for p in reversed(
        site_config_dir("raincloud", appauthor=False, multipath=True).split(os.pathsep)
    )]


def _path(value: str | Path, base: Path) -> Path:
    # `~` and `~/` only. `~user` is refused, here and in the Rust client, so a
    # config value names the same directory in both languages and an unknown
    # user is reported as a bad setting rather than a RuntimeError from
    # `expanduser`.
    text = str(value)
    if text.startswith("~") and text not in ("~",) and not text.startswith("~/"):
        raise ValueError(
            f"{text!r}: ~user paths are not supported; use an absolute path or ~/"
        )
    p = Path(value).expanduser()
    return Path(os.path.abspath(base / p))


def _flag(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Config:
    data_dir: Path
    scratch_dir: Path
    raw_dir: Path
    cache_dir: Path
    home_dir: Path
    catalog_dir: Path
    catalog: str = "auto"
    catalog_url: str | None = None
    mirror: str | None = None
    offline: bool = False
    # What `--retry-errors` sets, carried to a child build (`load(..., retry_errors=True)`).
    retry_errors: bool = False
    # Machine-level writer preference; a catalog or a single spec can override it.
    export_priority: tuple[str, ...] | None = None
    manifest: Path | None = None
    snapshot: Path | None = None
    file: Path | None = None
    origins: tuple[tuple[str, str], ...] = ()

    def subprocess_env(self) -> dict[str, str]:
        """Pin settings for a child build, including deliberate empty values."""
        env = dict(os.environ)
        env.pop("RAINCLOUD_CONFIG", None)
        env["RAINCLOUD_NO_CONFIG"] = "1"
        env["RAINCLOUD_HOME"] = str(self.home_dir)
        for key, name in _ENV.items():
            value = getattr(self, key)
            if isinstance(value, bool):
                env[name] = str(int(value))
            elif key in _LISTS:
                # The spelling resolve_config parses back: "rs,py", or "" for unset.
                env[name] = ",".join(value or ())
            else:
                env[name] = str(value or "")
        return env

    def describe(self) -> dict:
        from .catalogs import resolve_context
        from .exceptions import RaincloudError

        # Inspecting settings must work even when the catalog does not: a broken or
        # unresolvable catalog is precisely when someone runs `raincloud config show`.
        # Only the manifest/snapshot fallbacks need the context; every other key is
        # settings, so report those and mark these two unresolved.
        context, context_error = None, None
        try:
            context = resolve_context(self)
        except (RaincloudError, OSError, ValueError) as exc:
            context_error = f"{type(exc).__name__}: {exc}"

        sources = dict(self.origins)
        result = {}
        for key in sorted(_KEYS):
            value = getattr(self, key)
            if key in {"manifest", "snapshot"} and value is None:
                if context is None:
                    sources[key] = f"unresolved ({context_error})"
                else:
                    value = getattr(context, f"{key}_path")
                    sources[key] = context.source
            if isinstance(value, Path):
                value = str(value)
            if key in {"mirror", "catalog_url"} and value:
                # Never print credentials, signed query parameters or fragments.
                value = redact_url(value)
            elif isinstance(value, tuple):
                value = list(value)
            result[key] = {"value": value, "source": sources.get(key, "default")}
        return result


_active: ContextVar[Config | None] = ContextVar("raincloud_config", default=None)


@contextmanager
def use_config(config: Config):
    token = _active.set(config)
    try:
        yield config
    finally:
        _active.reset(token)


def get_config(*, repo_root: Path | None = None) -> Config:
    return _active.get() or resolve_config(repo_root=repo_root)


def _is_checkout(root: Path) -> bool:
    """Whether `root` is a source checkout (the one place this is decided)."""
    return (root / "sources.json").is_file()


def resolve_config(*, config: str | Path | None = None, no_config: bool = False,
                   repo_root: Path | None = None, **overrides) -> Config:
    """Resolve explicit values > environment > user TOML > system TOML > defaults.

    TOML uses a single [raincloud] table. Relative paths in it are relative to
    the file, whereas environment and API paths are relative to the caller.
    --no-config/RAINCLOUD_NO_CONFIG disables all config-file discovery.

    A source checkout skips the system TOML. Machine defaults describe the
    machine's shared catalog and store for its users; a checkout is a builder,
    and following them would build against the deployed catalog straight into
    the shared store. User TOML and the environment still apply to a checkout.
    """
    from ._bundle import REVISION  # lazy: _bundle imports this module's dependents

    unknown = set(overrides) - _KEYS
    if unknown:
        raise ValueError(f"unknown settings: {', '.join(sorted(unknown))}")
    cwd = Path.cwd()
    # An empty --config/RAINCLOUD_CONFIG means unset, not "require the default file".
    config = config if config != "" else None
    disabled = no_config or (config is None and _flag(os.environ.get("RAINCLOUD_NO_CONFIG", "")))
    selected = config if config is not None else (os.environ.get("RAINCLOUD_CONFIG") or None)
    file = None if disabled else _path(selected or config_path(), cwd)
    values, origins = {}, {}
    root = repo_root if repo_root is not None else Path(__file__).resolve().parent.parent
    checkout = _is_checkout(root)
    machine = [] if checkout else system_config_paths()
    files = [] if disabled else ([file] if selected is not None else [
        p for p in [*machine, file] if p.exists()
    ])
    for settings_file in files:
        try:
            document = tomllib.loads(settings_file.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise ValueError(f"cannot read Raincloud config {settings_file}: {exc}") from exc
        if set(document) - {"raincloud"} or not isinstance(document.get("raincloud", {}), dict):
            raise ValueError(f"{settings_file}: expected a [raincloud] table")
        layer = document.get("raincloud", {})
        unknown = set(layer) - _KEYS
        if unknown:
            raise ValueError(f"{settings_file}: unknown settings: {', '.join(sorted(unknown))}")
        for key, value in list(layer.items()):
            if key in _BOOLS:
                if not isinstance(value, bool):
                    raise ValueError(f"{settings_file}: {key} must be a boolean")
            elif key in _LISTS:
                # A TOML array is the natural spelling here; a comma string is
                # accepted so the file and the env var take the same value.
                if isinstance(value, str):
                    value = layer[key] = _writers(value)
                elif isinstance(value, list) and all(isinstance(v, str) for v in value):
                    value = layer[key] = tuple(value)
                else:
                    raise ValueError(f"{settings_file}: {key} must be a list of writer names")
            elif not isinstance(value, str) or (key in _PATHS and not value):
                raise ValueError(f"{settings_file}: {key} must be a nonempty path/string")
            if key in _PATHS:
                layer[key] = _path(value, settings_file.parent)
            elif key == "catalog" and value not in _SELECTORS and not REVISION.fullmatch(value) \
                    and not (REVISION_PREFIX.fullmatch(value) and not (settings_file.parent / value).exists()):
                # A directory, relative to the file. A revision or its prefix
                # (what overviews print) stays as written, for resolve_context.
                layer[key] = str(_path(value, settings_file.parent))
            elif key == "catalog_url" and not urlsplit(value).scheme:
                layer[key] = str(_path(value, settings_file.parent))
            origins[key] = str(settings_file)
        values.update(layer)
    native_data = user_data_path("raincloud", appauthor=False)
    native_cache = user_cache_path("raincloud", appauthor=False)
    home_env = os.environ.get("RAINCLOUD_HOME")
    home = _path(home_env, cwd) if home_env else (root if checkout else native_data)
    # HOME retains its historical parent-of-outputs and parent-of-scratch meaning.
    if home_env:
        for key, value in (("data_dir", home / "outputs"), ("scratch_dir", home / "_workdir")):
            values[key], origins[key] = value, "RAINCLOUD_HOME"
    for key, name in _ENV.items():
        value = os.environ.get(name)
        if value is not None and (value or key in _BOOLS or key == "mirror"):
            if key in _BOOLS:
                values[key] = _flag(value)
            elif key in _LISTS:
                values[key] = _writers(value)
            else:
                values[key] = _path(value, cwd) if key in _PATHS else value
            origins[key] = name
    for key, value in overrides.items():
        if value is not None:
            if key in _BOOLS and not isinstance(value, bool):
                raise ValueError(f"{key} must be a boolean")
            if key in _LISTS:
                value = _writers(value)
            values[key] = _path(value, cwd) if key in _PATHS else value
            origins[key] = "explicit"
    values.setdefault("catalog_dir", native_data / "catalogs")
    values.setdefault("data_dir", root / "outputs" if checkout else native_data)
    values.setdefault("scratch_dir", root / "_workdir" if checkout else native_cache / "workdir")
    values.setdefault("raw_dir", values["data_dir"] / "raw_downloads")
    values.setdefault("cache_dir", values["data_dir"])
    origins.setdefault("raw_dir", "data_dir/raw_downloads")
    origins.setdefault("cache_dir", "data_dir")
    return Config(**values, home_dir=home, file=file, origins=tuple(sorted(origins.items())))
