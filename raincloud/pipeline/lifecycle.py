# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Shared operation scope for artifact maintenance entry points.

Every writer takes the data store's lock file (`<root>/.raincloud-write.lock`,
a file that stays in place; the lock is on it, not its existence) and replaces
files by rename, so readers see the old file or the new one. Nested stages
reuse the locks the outermost stage took; resource locks are always acquired
in sorted order.
"""
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from functools import wraps
from pathlib import Path

from raincloud._cache import Publication
from raincloud._catalog import Catalog
from raincloud._locking import locked
from raincloud.catalogs import current, operation, resolve_context
from raincloud.config import get_config
from raincloud.exceptions import CatalogConflict

_held = ContextVar("raincloud_maintenance_locks", default=frozenset())


@contextmanager
def operation_lock(*, resources=False):
    """Hold the store locks for one operation and freeze its catalog context.

    Locks the data store; `resources=True` also locks the raw-download and
    scratch roots, for a stage that writes those. Locks are taken at the OUTER
    boundary only: a nested call reuses what is held, and one asking for a root
    the outer call did not take raises RuntimeError, because acquiring it
    mid-operation could deadlock against another process taking the same set
    in sorted order. The selected catalog is frozen for the whole block, so
    every stage inside sees the same one. Yields that context.
    """
    config = get_config()
    context = current() or resolve_context(config)
    roots = {config.data_dir.resolve()}
    if resources:
        roots.update((config.raw_dir.resolve(), config.scratch_dir.resolve()))
    held = _held.get()
    if held and roots - held:
        raise RuntimeError("resource locks must be acquired at the outer operation boundary")
    with ExitStack() as stack:
        for root in sorted(roots - held):
            stack.enter_context(locked(root / ".raincloud-write.lock"))
        stack.enter_context(operation(config, context))
        token = _held.set(held | roots)
        try:
            yield context
        finally:
            _held.reset(token)


def maintenance(*, resources=False):
    """Decorator: run the function inside `operation_lock(resources=...)`."""
    def decorate(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            with operation_lock(resources=resources):
                return fn(*args, **kwargs)
        return wrapped
    return decorate


def entry_for(spec, *, manifest=None, snapshot=None):
    """The catalog entry `spec` would have: the selected catalog with `spec` as
    its slug's recipe, for a recipe that differs from (or is absent in) the
    manifest. The selected catalog itself is not changed. `manifest` and
    `snapshot` default to the selected context's, decoded afresh."""
    context = current()
    manifest = manifest if manifest is not None else context.manifest
    manifest = {**manifest, "datasets": [s for s in manifest["datasets"] if s["slug"] != spec["slug"]] + [spec]}
    return Catalog(snapshot if snapshot is not None else context.snapshot, manifest, context).entry(spec["slug"])


def entry_for_slug(slug):
    """The selected catalog's entry for `slug`."""
    context = current()
    return Catalog(context.snapshot, context.manifest, context).entry(slug)


def require_source(path):
    """Raise FileNotFoundError unless a stage's input file exists. Existence
    only: which artifact it is, the catalog decides."""
    if not Path(path).is_file():
        raise FileNotFoundError(path)


_build_outputs = ContextVar("raincloud_build_outputs", default=None)


class BuildOutputs:
    """Preflight actual producer destinations; never impersonate another recipe."""

    def __init__(self, spec):
        self.spec = spec
        self.canonicals = []
        self.pending = {}
        self._catalog = None  # (manifest, snapshot), decoded once per build

    def preflight(self, slug):
        """Check an output slug before anything is written for it.

        Raises ValueError for a slug that is not a safe path component, and
        CatalogConflict when a multi-output producer would publish a slug the
        manifest declares with a different recipe. Returns the entry the output
        will have under this producer's recipe.
        """
        from raincloud._bundle import SLUG

        if not isinstance(slug, str) or not SLUG.fullmatch(slug):
            raise ValueError(f"unsafe output slug: {slug!r}")
        if self._catalog is None:
            context = current()
            self._catalog = (context.manifest, context.snapshot)
        manifest, snapshot = self._catalog
        entry = entry_for({**self.spec, "slug": slug}, manifest=manifest, snapshot=snapshot)
        if slug != self.spec["slug"]:
            declared = next((s for s in manifest["datasets"] if s["slug"] == slug), None)
            if declared is not None:
                selected = entry_for(declared, manifest=manifest, snapshot=snapshot)
                if selected.recipe != entry.recipe:
                    raise CatalogConflict(
                        f"output {slug!r} has another manifest recipe; "
                        "a producer cannot publish it under that recipe"
                    )
        return entry

    def prepare_canonical(self, path):
        """Hold `path`'s previous file until the build accepts the new one.

        Raises RuntimeError when one build publishes the same canonical twice.
        """
        if path in self.pending:
            raise RuntimeError(f"canonical output published twice in one build: {path}")
        self.pending[path] = Publication(path).__enter__()

    def accept_canonicals(self, paths):
        # Validation has succeeded. Commit each canonical independently, before
        # optional exporters run, so an exporter failure leaves reusable input.
        for path in paths:
            publication = self.pending.pop(path)
            publication.accept()
            publication.__exit__(None, None, None)

    def rollback_pending(self):
        """Restore every canonical the build did not accept."""
        with ExitStack() as stack:
            for publication in self.pending.values():
                stack.callback(publication.__exit__, None, None, None)


@contextmanager
def build_outputs(spec):
    """The scope of one build: canonicals it did not accept are rolled back."""
    state = BuildOutputs(spec)
    token = _build_outputs.set(state)
    try:
        yield state
    finally:
        try:
            state.rollback_pending()
        finally:
            _build_outputs.reset(token)


def canonical_destination(slug, path):
    """Called by the canonical writer before writing `path` (inside a build)."""
    state = _build_outputs.get()
    if state is not None:
        state.preflight(slug)
        state.prepare_canonical(path)


def canonical_completed(path):
    """Called by the canonical writer once `path` is in place (inside a build)."""
    state = _build_outputs.get()
    if state is not None:
        state.canonicals.append(path)
