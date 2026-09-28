# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Installed command line entry point; build dependencies are loaded only on use."""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
import traceback
import warnings
from pathlib import Path
from urllib.parse import urlsplit

from . import __version__, _open, describe
from ._locking import atomic_write, creation_mode
from .config import config_path, resolve_config, use_config
from .exceptions import BuildToolingMissing, HydratedDatasetWarning, RaincloudError

# `--settings-env` reads the settings JSON from this variable. A process's
# environment is private to its user; its command line is not.
SETTINGS_ENV = "RAINCLOUD_SETTINGS"

# What `raincloud init` writes when no setting is given: every key it can set,
# commented out, so the file says what it is for.
_INIT_TEMPLATE = """\
# Raincloud settings. Uncomment a line to use it; `raincloud config` shows every
# setting in effect and where it came from.
# data_dir = "/data/raincloud"      # prepared datasets (the shared store)
# cache_dir = "~/.cache/raincloud"  # mirror downloads (default: data_dir)
# scratch_dir = "/tmp/raincloud"    # build scratch space
# mirror = "s3://bucket/prefix"     # a store to fetch prepared files from
"""


def _init(args) -> int:
    target = Path(args.config or os.environ.get("RAINCLOUD_CONFIG") or config_path()).expanduser().absolute()
    if args.no_config:
        raise ValueError("init writes a config file; omit --no-config")
    # Serialize only requested settings. JSON strings are valid TOML basic strings.
    settings = {key: getattr(args, key) for key in ("data_dir", "scratch_dir", "cache_dir", "mirror")
                if getattr(args, key) is not None}
    for key in ("data_dir", "scratch_dir", "cache_dir"):
        if key in settings:
            settings[key] = str(Path(settings[key]).expanduser().absolute())
    content = "[raincloud]\n" + ("".join(f"{key} = {json.dumps(value, ensure_ascii=False)}\n"
                                         for key, value in settings.items()) or _INIT_TEMPLATE)
    if target.exists() and not args.force:
        if settings and target.read_text(encoding="utf-8") != content:
            raise ValueError(f"{target} already exists; use --force to replace it")
        print(f"Using existing config: {target}")
        return 0
    target.parent.mkdir(parents=True, exist_ok=True)
    # A mirror URL can carry credentials or a signed query: then the file is
    # this user's alone. Otherwise it is an ordinary file (umask applies), so a
    # machine-wide config written this way stays readable by its users.
    mirror = urlsplit(settings.get("mirror") or "")
    mode = 0o600 if "@" in mirror.netloc or mirror.query or mirror.fragment else creation_mode()
    # Exclusive creation prevents a concurrent init from overwriting settings.
    if not args.force:
        with os.fdopen(os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode), "w",
                       encoding="utf-8") as stream:
            if hasattr(os, "fchmod"):
                os.fchmod(stream.fileno(), mode)  # the umask narrows os.open's mode, never widens it
            stream.write(content)
    else:
        atomic_write(target, content.encode("utf-8"), mode=mode)
    print(f"Config written: {target}")
    return 0


# Typed names people reach for, mapped onto the command that does it.
_ALIASES = {"ls": "list", "search": "list", "info": "describe", "show": "describe", "path": "load"}
# Commands that hand the rest of the line to another parser.
_PASSTHROUGH = {"build", "list", "browse"}


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        # An unknown command or action gets a suggestion, not a usage dump.
        match = re.match(r"argument (?:\w+|\{[^}]*\}): invalid choice: '([^']*)'", message)
        choices = next((action.choices for action in self._actions
                        if isinstance(action, argparse._SubParsersAction)), None)
        if match and choices:
            from ._suggest import hint
            if self.prog == "raincloud":
                reply = hint(match.group(1), [*self._commands, *_ALIASES], noun="command",
                             canonical=_ALIASES, everything="`raincloud help` lists them.")
            else:
                reply = hint(match.group(1), list(choices), noun=f"{self.prog.split()[-1]} action",
                             everything=f"`raincloud help {self.prog.split()[-1]}` lists them.")
            self.exit(2, "raincloud: " + reply + "\n")
        super().error(message)


def _size(nbytes) -> str:
    if nbytes is None:
        return "size unknown"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if nbytes < 1000 or unit == "TB":
            return f"{nbytes:.0f} {unit}" if unit == "B" else f"{nbytes:.1f} {unit}"
        nbytes /= 1000


def _overview(settings) -> str:
    try:
        from .catalogs import resolve_context
        context = resolve_context(settings)
        count = len(context.manifest.get("datasets", []))
        where = (f"Catalog {context.bundle.revision[:12]}: {count} datasets. "
                 f"Data in {settings.data_dir}.")
    except Exception as exc:  # the overview must work even when the catalog does not
        where = f"No catalog could be read ({exc}); `raincloud config` shows the settings in use."
    return textwrap.dedent(f"""\
        raincloud {__version__} -- prepared research datasets on this machine.

          raincloud list [WORD...]     find datasets, e.g. `raincloud list tpch lineitem`
          raincloud describe SLUG      what a dataset is: rows, columns, formats, license
          raincloud load SLUG          print the path to its file (--format vortex|parquet|arrow)
          raincloud build SLUG         prepare a dataset on this machine (needs raincloud[build])
          raincloud browse             browse interactively (needs raincloud[tui])

        In Python: raincloud.load("SLUG").to_arrow(), or .dataset() for DuckDB, Polars or pyarrow.

        {where}
        `raincloud help COMMAND` explains a command; `raincloud --help` lists every one.
        """)


def _describe_text(about: dict, settings) -> str:
    width = min(shutil.get_terminal_size((100, 20)).columns, 100)
    lines = [f"{about['slug']}" + (f" -- {about['name']}" if about.get("name") else "")]
    if about.get("derived_from"):
        fetched = ", ".join(f"{t.get('into')} (from {c})" for c, t in (about.get("hydrated_columns") or {}).items())
        lines += textwrap.wrap(
            f"HYDRATED: {fetched} fetched from the open web. {about.get('advisory') or ''} "
            f"You probably want `raincloud describe {about['derived_from']}`.",
            width - 2, initial_indent="  ", subsequent_indent="  ")
    if about.get("description"):
        lines += textwrap.wrap(about["description"], width - 2, initial_indent="  ", subsequent_indent="  ")
    lines.append("")
    if about.get("license"):
        lines.append(f"license   {about['license']}" + (f"  {about['source_url']}" if about.get("source_url") else ""))
    lines.append(f"rows      {about['rows']:,}" if about.get("rows") is not None else "rows      not recorded")
    # The writer is shown only when it is not the usual one: a Parquet file from
    # arrow-rs is worth knowing about when comparing encodings.
    here = about.get("prepared") or {}
    formats = [f"{fmt} unavailable" if info.get("unavailable") else
               f"{fmt} {_size(info.get('bytes'))}"
               + (f" [{info['writer']}]" if info.get("writer") not in (None, "py", "canonical") else "")
               + (" UNVERIFIED" if info.get("unverified") else "")
               + (f" ({', '.join(n for n, on in (('default', fmt == about['format']), ('here', fmt in here)) if on)})"
                  if fmt == about["format"] or fmt in here else "")
               for fmt, info in sorted(about["formats"].items(), key=lambda kv: (kv[0] != about["format"], kv[0]))]
    lines.append("formats   " + (", ".join(formats) if formats else "none recorded"))
    # What a build measured, quoted: the reason is the toolchain's, not a step to take.
    from ._formats import describe_unavailable
    for fmt, info in sorted(about["formats"].items()):
        if info.get("unavailable"):
            lines += textwrap.wrap(f"{fmt}: {describe_unavailable(info['unavailable'])}", width - 2,
                                   initial_indent="          ", subsequent_indent="            ")
        elif info.get("unverified"):
            # Served, but its writer could not check it reads back; the reason is the writer's.
            lines += textwrap.wrap(f"{fmt}: not verified to read back: {info['unverified']}", width - 2,
                                   initial_indent="          ", subsequent_indent="            ")
    slug = about["slug"]
    if about["format"] in here:
        lines.append(f"load      raincloud load {slug}")
        lines.append(f"          raincloud.load({slug!r}).to_arrow()")
    elif settings.mirror:
        lines.append(f"load      not on this machine yet; `raincloud load {slug}` fetches it from the mirror")
    else:
        lines.append(f"load      not on this machine and no mirror is configured; "
                     f"`raincloud build {slug}` prepares it here (needs raincloud[build])")
    columns = about.get("columns") or []
    if columns:
        lines.append(f"columns   {len(columns)}")
        pad = min(max(len(str(c.get('name'))) for c in columns), 40)
        lines += [f"  {str(c.get('name')):<{pad}}  {c.get('type', '')}" for c in columns]
    return "\n".join(lines)


def _json_flag(parser) -> None:
    # Also accepted after the command (`raincloud load x --json`); SUPPRESS keeps
    # an absent flag here from overriding one given before the command.
    parser.add_argument("--json", action="store_true", dest="as_json", default=argparse.SUPPRESS,
                        help="print results and errors as JSON on stdout")


def main(argv: list[str] | None = None) -> int:
    parser = _Parser(prog="raincloud", description="Prepared research datasets on this machine.")
    parser.add_argument("--version", action="version", version=__version__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--config", help="Use this TOML settings file")
    selection.add_argument("--no-config", action="store_true", help="Ignore all config files (environment still applies)")
    for name in ("data-dir", "scratch-dir", "cache-dir", "mirror"):
        parser.add_argument(f"--{name}", help="Override this setting for the command")
    parser.add_argument("--catalog", help="auto, active, checkout, bundled, local (the `manifest` setting), "
                                          "a revision or its unique prefix, a bundle directory, or a pack directory")
    parser.add_argument("--settings", type=json.loads, default={}, metavar="JSON",
                        help="settings as a JSON object (TOML keys, plus config/no_config)")
    parser.add_argument("--settings-env", action="store_true",
                        help=f"read the --settings JSON object from ${SETTINGS_ENV}; how native readers pass "
                             f"options, since a command line is visible to every account")
    parser.add_argument("--json", action="store_true", dest="as_json",
                        help="print results and errors as JSON on stdout")
    commands = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers = {}

    def command(name, help, aliases=(), **kw):
        sub = commands.add_parser(name, help=help, description=help, aliases=list(aliases), **kw)
        subparsers[name] = sub
        return sub

    def resolution(sub):
        sub.add_argument("slug", nargs="?", help="dataset name; `raincloud list` finds them")
        sub.add_argument("-f", "--format", default="auto",
                         help="vortex, parquet or arrow; auto (the default) picks the first prepared, in that order")
        sub.add_argument("--readers", metavar="FORMATS",
                         help="comma-separated formats the caller decodes itself (native readers); "
                              "auto chooses among these")
        _json_flag(sub)

    command("list", "Find datasets: `raincloud list [WORD...] [--long] [--size xl] ...`",
            aliases=("ls", "search"), add_help=False)
    about = command("describe", "What a dataset is: rows, columns, formats, license; never reads artifacts",
                    aliases=("info", "show"))
    resolution(about)
    read = command("load", "Resolve a dataset's file and print its path", aliases=("path",))
    resolution(read)
    read.add_argument("--build", action="store_true", help="explicitly allow a local build on a prepared-data miss")
    read.add_argument("--retry-errors", action="store_true",
                      help="allow a local build (as --build) that attempts a format even when its writer, "
                           "with this toolchain, already failed to write it at this recipe")
    read.add_argument("--offline", action="store_true", default=None,
                      help="use only files already on this machine; never fetch or build")
    read.add_argument("--mirror", default=argparse.SUPPRESS,
                      help="fetch from this store (s3://, https://, file://) instead of the configured mirror")
    command("browse", "Browse the catalog interactively (needs raincloud[tui])", add_help=False)
    helper = command("help", "Show an overview, or explain one command")
    helper.add_argument("topic", nargs="?")
    command("version", "Print the raincloud version")
    config = command("config", "Show the settings in use and the config file they came from")
    _json_flag(config)
    _json_flag(config.add_subparsers(dest="action").add_parser("show", help="Show the settings in use (the default)"))
    _json_flag(command("capabilities", "Report available prepared-data readers"))
    catalog = command("catalog", "Catalog lifecycle (status by default); never refreshes during normal reads")
    _json_flag(catalog)
    actions = catalog.add_subparsers(dest="action", metavar="ACTION")

    def action(name, help):
        sub = actions.add_parser(name, help=help, description=help)
        _json_flag(sub)
        return sub

    action("status", "Which catalog is selected, and the installed revisions (the default)")
    update = action("update", "Install and activate a catalog revision from a pack directory or its HTTPS copy")
    update.add_argument("--source", help="HTTPS URL or local directory containing latest.json and revision directories")
    update.add_argument("--revision", help="a specific revision instead of the source's latest")
    pin = action("pin", "Activate an installed revision and keep it until unpinned")
    pin.add_argument("revision", nargs="?", help="a revision id or its unique prefix (as the overview prints it)")
    action("rollback", "Return to the previous revision, pinned")
    gc = action("gc", "Remove installed revisions nothing can roll back to")
    gc.add_argument("--keep", type=int, default=10, metavar="N",
                    help="history entries to retain (default 10); 0 keeps only the active one")
    gc.add_argument("--dry-run", action="store_true", help="report what would be removed; remove nothing")
    action("unpin", "Let `catalog update` move the active revision again")
    pack = action("pack", "Write a static upstream directory locally")
    pack.add_argument("--manifest", type=Path, required=True)
    pack.add_argument("--snapshot", type=Path, required=True)
    pack.add_argument("--id", required=True, dest="catalog_id")
    pack.add_argument("--output", type=Path, required=True)
    command("build", "Prepare datasets on this machine: `raincloud build SLUG...` (requires raincloud[build])",
            add_help=False)
    init = command("init", "Write optional machine settings; never moves or fetches data")
    for name in ("data-dir", "scratch-dir", "cache-dir", "mirror"):
        init.add_argument(f"--{name}", default=argparse.SUPPRESS)
    init.add_argument("--force", action="store_true", help="Replace an existing config")
    parser._commands = list(subparsers)

    args, remainder = parser.parse_known_args(argv)
    with warnings.catch_warnings():
        # A warning reads as the tool talking, not as a Python traceback line.
        warnings.showwarning = _show_warning
        try:
            return _dispatch(parser, subparsers, args, remainder)
        except BrokenPipeError:
            return _closed_stdout()
        except Exception as exc:
            if not getattr(args, "as_json", False):
                raise
            # A JSON caller parses stdout whatever happens; the traceback stays on stderr.
            traceback.print_exc()
            print(json.dumps({"error": _error(exc)}))
            return 1


def _closed_stdout() -> int:
    """The reader went away (`raincloud list | head`): that is a finished command, not an error.

    stdout is pointed at /dev/null so the interpreter's final flush does not
    report the same broken pipe again.
    """
    try:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
    except (OSError, ValueError, AttributeError):
        pass
    return 0


def _error(exc: BaseException) -> dict:
    kind = type(exc).__name__ if isinstance(exc, RaincloudError) else next(
        (base.__name__ for base in (ValueError, OSError) if isinstance(exc, base)), type(exc).__name__)
    # The MRO lets a native client classify a class it has no name for by its parent.
    return {"type": kind, "message": str(exc), "mro": [c.__name__ for c in type(exc).__mro__]}


def _show_warning(message, category, filename, lineno, file=None, line=None):
    print(f"raincloud: warning: {message}", file=sys.stderr)


def _config_text(settings) -> str:
    if settings.file is None:
        where = "config file  none (disabled by --no-config / RAINCLOUD_NO_CONFIG)"
    elif settings.file.exists():
        where = f"config file  {settings.file}"
    else:
        where = f"config file  {settings.file} (absent; `raincloud init` writes one)"
    machine = sorted({origin for _, origin in settings.origins if origin.endswith(".toml")} - {str(settings.file)})
    lines = [where] + [f"also read    {path}" for path in machine] + [""]
    described = settings.describe()
    pad = max(map(len, described))
    for key, item in described.items():
        value = item["value"]
        shown = "unset" if value in (None, "") else (",".join(value) if isinstance(value, list) else str(value))
        lines.append(f"{key:<{pad}}  {shown}  [{item['source']}]")
    return "\n".join(lines)


def _capabilities_text(capabilities: dict) -> str:
    return "\n".join(
        f"{fmt:<8} " + (f"available ({info['implementation']})" if info["available"]
                        else f"not installed; install `raincloud[{info.get('extra', fmt)}]`")
        for fmt, info in capabilities.items())


def _catalog_text(action: str, result, settings) -> str:
    from .catalogs import installed_revisions
    if action != "status":
        if not isinstance(result, dict):
            return str(result)
        lines = []
        for key, value in result.items():
            if isinstance(value, dict):
                value = "; ".join(f"{k}: {v}" for k, v in value.items()) or "none"
            elif isinstance(value, list):
                value = ", ".join(map(str, value)) or "none"
            lines.append(f"{key:<16} {value}")
        return "\n".join(lines)
    installed = installed_revisions(settings)
    lines = [f"selected   {result['selected']} ({result['catalog_id']}), revision {result['revision'][:12]}",
             "active     " + (f"{result['active'][:12]}" + (" (pinned)" if result["pinned"] else "")
                               if result["active"] else "none (`raincloud catalog update --source DIR` installs one)"),
             f"history    {len(result['history'])} earlier revision(s)"
             + ("; `raincloud catalog rollback` returns to the last" if result["history"] else ""),
             f"installed  {len(installed)} revision(s) in {result['catalog_dir']}"]
    lines += [f"           {revision[:12]}" + (" (active)" if revision == result["active"] else "")
              for revision in installed]
    return "\n".join(lines)


def _readers(value: str | None) -> set[str] | None:
    if value is None:
        return None
    from ._cache import EXT
    from ._suggest import hint
    names = {part.strip().lower() for part in value.split(",") if part.strip()}
    for name in names - set(EXT):
        raise ValueError("--readers: " + hint(name, list(EXT), noun="format"))
    return names


def _dispatch(parser, subparsers, args, remainder) -> int:
    name = _ALIASES.get(args.command, args.command)
    if remainder and name not in _PASSTHROUGH:
        parser.error(f"unrecognized arguments: {' '.join(remainder)}")
    if name == "help" and args.topic:
        topic = _ALIASES.get(args.topic, args.topic)
        if topic not in subparsers:
            from ._suggest import hint
            print("raincloud: " + hint(args.topic, [*subparsers, *_ALIASES], noun="command", canonical=_ALIASES,
                                        everything="`raincloud help` lists them."), file=sys.stderr)
            return 2
        if topic in _PASSTHROUGH:
            return main([topic, "--help"])
        subparsers[topic].print_help()
        return 0
    if name == "version":
        print(__version__)
        return 0
    as_json = getattr(args, "as_json", False)
    if name in ("describe", "load") and args.slug is None:
        if as_json:
            # A JSON caller gets JSON, and a missing argument is an error.
            print(json.dumps({"error": _error(ValueError(
                f"`raincloud {name}` needs a dataset name; `raincloud list` finds them"))}))
            return 2
        subparsers[name].print_help()
        print("\n`raincloud list` finds dataset names.")
        return 0
    try:
        if name == "init":
            return _init(args)
        if name == "build" and ({"-h", "--help"} & set(remainder) or not remainder):
            return _build_help(remainder)
        overrides = {}
        if args.settings_env:
            if SETTINGS_ENV not in os.environ:
                raise ValueError(f"--settings-env: ${SETTINGS_ENV} is not set")
            # Consumed here, so a `raincloud build` child does not inherit it.
            overrides = json.loads(os.environ.pop(SETTINGS_ENV))
            if not isinstance(overrides, dict):
                raise ValueError(f"${SETTINGS_ENV} must be a JSON object")
        if not isinstance(args.settings, dict):
            raise ValueError("--settings must be a JSON object")
        overrides = {**overrides, **args.settings}
        config_file = overrides.pop("config", args.config)
        no_config = overrides.pop("no_config", False) or args.no_config
        for key in ("data_dir", "scratch_dir", "cache_dir", "mirror", "catalog"):
            if getattr(args, key) is not None:
                overrides[key] = getattr(args, key)
        settings = resolve_config(config=config_file, no_config=no_config, **overrides)
        if name in (None, "help"):
            print(_overview(settings), end="")
        elif name == "list":
            from .pipeline import list_datasets
            with use_config(settings):
                return list_datasets.main([*remainder, *(["--json"] if as_json else [])],
                                          prog="raincloud list")
        elif name == "browse":
            from .pipeline import browse
            with use_config(settings):
                return browse.main(remainder)
        elif name == "capabilities":
            from . import reader_capabilities
            capabilities = reader_capabilities()
            print(json.dumps(capabilities, indent=2) if as_json else _capabilities_text(capabilities))
        elif name == "config":
            print(json.dumps(settings.describe(), indent=2) if as_json else _config_text(settings))
        elif name in ("load", "describe"):
            # Reader-agnostic: the path goes to a reader that may not be this
            # Python's (a Rust or Java client), so pyarrow/vortex availability
            # here says nothing. --readers says what the caller decodes.
            with warnings.catch_warnings():
                if name == "describe":
                    # The summary leads with the advisory itself; loading below only picks a format.
                    warnings.simplefilter("ignore", HydratedDatasetWarning)
                dataset = _open(args.slug, format=args.format, readers=_readers(args.readers),
                                build=getattr(args, "build", False), offline=getattr(args, "offline", None),
                                mirror=getattr(args, "mirror", None), config=settings, readable=False,
                                stacklevel=1, retry_errors=getattr(args, "retry_errors", False))
            if name == "load":
                path = dataset.path()
                print(json.dumps({"slug": args.slug, "format": dataset.format, "path": str(path),
                                  "catalog_revision": dataset.catalog_revision})
                      if as_json else path)
            else:
                # A native client reopens this catalog by selecting catalog_source,
                # and compares catalog_revision to detect that it changed.
                prepared = dataset.local_paths()
                about = {**describe(args.slug, config=settings), "format": dataset.format,
                         "recipe": dataset.recipe_fingerprint, "artifacts": dataset.artifacts,
                         "catalog_source": dataset.catalog_source,
                         "prepared": {fmt: str(path) for fmt, path in prepared.items()}}
                print(json.dumps(about) if as_json else _describe_text(about, settings))
        elif name == "catalog":
            from . import catalogs
            action = args.action or "status"
            if action == "pin" and args.revision is None:
                # Nothing to pin: say what could be, as `catalog status` would.
                status = catalogs.status(settings)
                if as_json:
                    print(json.dumps(status, indent=2))
                    return 0
                print(_catalog_text("status", status, settings))
                print("\nPin one with `raincloud catalog pin REVISION` (a unique prefix is enough).")
                return 0
            if action == "pack":
                result = catalogs.pack(args.manifest, args.snapshot, args.output, args.catalog_id)
            elif action == "update":
                result = catalogs.update(settings, source=args.source, revision=args.revision)
            elif action == "pin":
                result = catalogs.pin(settings, args.revision)
            elif action == "gc":
                result = catalogs.gc(settings, keep=args.keep, dry_run=args.dry_run)
            else:
                result = getattr(catalogs, action)(settings)
            print(json.dumps(result, indent=2) if as_json else _catalog_text(action, result, settings))
            if action == "gc" and result["failed"]:
                print(f"raincloud: could not remove {len(result['failed'])} revision(s); see `failed`",
                      file=sys.stderr)
                return 1
        elif name == "build":
            _require_build()
            from .catalogs import resolve_context
            with resolve_context(settings).pinned(settings) as pinned:
                return subprocess.run([sys.executable, "-m", "raincloud.pipeline.build", *remainder],
                                      env=pinned.subprocess_env()).returncode
    except BrokenPipeError:
        return _closed_stdout()
    except (RaincloudError, ValueError, OSError) as exc:
        if as_json:
            print(json.dumps({"error": _error(exc)}))
        else:
            print(f"raincloud: {exc}", file=sys.stderr)
        return 1
    return 0


def _require_build() -> None:
    """Fail with the loader's diagnosis rather than a child's raw ModuleNotFoundError.

    Absent toolchain -> install the extra; present but broken -> the real cause.
    """
    from ._resolve import _build_import_error
    build_err = _build_import_error()
    if build_err is None:
        return
    if isinstance(build_err, ImportError):
        raise BuildToolingMissing("`raincloud build` needs the build toolchain; install `raincloud[build]`")
    raise BuildToolingMissing("the build pipeline is installed but failed to import: "
                              f"{type(build_err).__name__}: {build_err}")


def _build_help(remainder) -> int:
    """`raincloud build`, bare or with --help: explain it without touching any catalog."""
    print("raincloud build SLUG... -- fetch, transform and export datasets into this machine's data directory.\n"
          "`raincloud list` finds dataset names; `raincloud describe SLUG` shows what a build produces.")
    try:
        _require_build()
    except BuildToolingMissing as exc:
        print(f"\n{exc}")
        return 0
    print()
    sys.stdout.flush()
    return subprocess.run([sys.executable, "-m", "raincloud.pipeline.build", "--help"]).returncode
