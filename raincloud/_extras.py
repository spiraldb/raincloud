# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Name the extra that installs a missing optional dependency.

pyproject.toml is the one declaration of which extra installs what; this reads
it back from the installed package metadata rather than restating it.
"""
from __future__ import annotations

import re
from collections import defaultdict
from importlib import metadata

from .exceptions import BuildToolingMissing

_REQUIREMENT = re.compile(
    r"\s*([A-Za-z0-9_.-]+)\s*(?:\[([^\]]*)\])?[^;]*;.*\bextra\s*==\s*['\"]([^'\"]+)['\"]")


def _canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _closures() -> dict[str, set[str]]:
    """Every raincloud extra -> the distributions it installs, nested extras included.

    Installers differ on whether `raincloud[build]` inside another extra reaches
    the metadata flattened or as a self-reference, so both are read.
    """
    try:
        requirements = metadata.requires("raincloud") or []
    except metadata.PackageNotFoundError:
        return {}
    direct, nested = defaultdict(set), defaultdict(set)
    for line in requirements:
        found = _REQUIREMENT.match(line)
        if not found:
            continue
        name, inner, extra = found.groups()
        if _canonical(name) == "raincloud":
            nested[extra] |= {part.strip() for part in (inner or "").split(",") if part.strip()}
        else:
            direct[extra].add(_canonical(name))

    def closure(extra: str, seen: frozenset = frozenset()) -> set[str]:
        result = set(direct.get(extra, ()))
        for other in nested.get(extra, ()):
            if other not in seen:
                result |= closure(other, seen | {extra})
        return result
    return {extra: closure(extra) for extra in set(direct) | set(nested)}


def _distributions(module: str, known: set[str]) -> set[str]:
    """The distributions that may provide import name `module` (e.g. vortex -> vortex-data)."""
    name = _canonical(module.split(".")[0])
    if name in known:
        return {name}
    installed = {_canonical(d) for d in metadata.packages_distributions().get(module.split(".")[0], ())}
    if installed & known:
        return installed & known
    # A missing module is not installed, so nothing maps it; the usual spelling
    # is the import name plus a suffix (vortex -> vortex-data).
    return {dist for dist in known if dist.startswith(name + "-")}


def extra_for(*modules: str) -> str | None:
    """The smallest raincloud extra that installs every one of `modules`, if any does.

    Smallest by what it installs in total, so `osm` wins over `all` and `build`
    over `generated` (which contains build).
    """
    closures = _closures()
    known = set().union(*closures.values()) if closures else set()
    wanted = [_distributions(module, known) for module in modules]
    hits = [extra for extra, dists in closures.items() if all(options & dists for options in wanted)]
    return min(hits, key=lambda extra: (len(closures[extra]), extra)) if hits else None


def missing(error: ModuleNotFoundError, what: str, *, needs: tuple[str, ...] = ()) -> BuildToolingMissing:
    """A typed error saying which extra `what` needs.

    `needs` lists every module `what` imports, when the caller knows them, so
    the extra named installs all of them rather than just the first one missing.
    """
    module = error.name or str(error)
    extra = extra_for(*dict.fromkeys((module, *needs))) or extra_for(module)
    how = f"install `raincloud[{extra}]`" if extra else f"install `{module}`"
    return BuildToolingMissing(f"{what} needs {module}; {how}")
