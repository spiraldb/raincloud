# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Named generator adapters. Importing the registry does not load toolchains.

Names come from `raincloud._registry.GENERATORS`; the adapter class is imported
and instantiated the first time its name is looked up, so iterating the registry
stays free.
"""
from __future__ import annotations

from collections.abc import MutableMapping
from importlib import import_module
from typing import Iterator

from raincloud._registry import GENERATORS


class _LazyGenerators(MutableMapping):
    """`REGISTRY[name]` resolves on demand; iteration and `len` do not import.

    Mutable because tests inject fakes (`monkeypatch.setitem`), which also has to
    keep working for anyone probing a generator without its toolchain installed.
    An injected entry needs no import path — it is already an object.
    """

    def __init__(self, declared: dict[str, str]) -> None:
        self._declared: dict[str, str | None] = dict(declared)
        self._resolved: dict[str, object] = {}

    def __getitem__(self, name: str):
        if name not in self._resolved:
            target = self._declared[name]  # KeyError for an unknown name, as before
            if target is None:  # injected then deleted from _resolved
                raise KeyError(name)
            module_name, _, attr = target.partition(":")
            self._resolved[name] = getattr(import_module(f".{module_name}", __package__), attr)()
        return self._resolved[name]

    def __setitem__(self, name: str, value) -> None:
        self._declared.setdefault(name, None)
        self._resolved[name] = value

    def __delitem__(self, name: str) -> None:
        known = self._declared.pop(name, _MISSING)
        resolved = self._resolved.pop(name, _MISSING)
        if known is _MISSING and resolved is _MISSING:
            raise KeyError(name)

    def __iter__(self) -> Iterator[str]:
        return iter(self._declared)

    def __len__(self) -> int:
        return len(self._declared)


_MISSING = object()


REGISTRY = _LazyGenerators(GENERATORS)
