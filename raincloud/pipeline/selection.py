# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Which datasets a pipeline command runs on: named slugs, or `--all`.

One helper for every stage CLI, so they agree on the two rules people trip
over: a name that is not in the manifest is an error with a did-you-mean,
never silently dropped, and `--all` leaves out hydrated datasets, whose bytes
come from the open web and are built only by name. The raw-input stages
(fetch, extract, generate) refuse a derived dataset outright: it has no
upstream of its own, so running one there would be a silent no-op.
"""
from __future__ import annotations

import sys

from raincloud._suggest import hint

from .spec import is_hydrated


class SelectionError(ValueError):
    """The request names no runnable dataset; the message says why."""


def select_specs(manifest: dict, slugs=(), *, all_: bool = False,
                 include_hydrated: bool = False, quiet: bool = False,
                 verb: str = "run", derived: str = "keep") -> list[dict]:
    """The specs to run, in manifest order for `--all`, argument order otherwise
    (a slug named twice runs once).

    Raises SelectionError when nothing is selected, when slugs and `--all` are
    both given, or when any named slug is unknown (every unknown one is
    reported, with suggestions). `include_hydrated` keeps hydrated datasets in
    an `--all` selection; a hydrated dataset named explicitly is always kept.
    `verb` names the command in the stderr note about skipped datasets.
    `derived="reject"` is for the raw-input stages: `--all` leaves every
    derived dataset out without a note, and naming one raises.
    """
    slugs = list(slugs or ())
    if all_ and slugs:
        raise SelectionError("pass slugs or --all, not both")
    specs = manifest["datasets"]
    if all_:
        if derived == "reject":
            return [s for s in specs if not s.get("derive")]
        chosen = [s for s in specs if include_hydrated or not is_hydrated(s)]
        skipped = len(specs) - len(chosen)
        if skipped and not quiet:
            print(f"[skip] {skipped} hydrated dataset(s); {verb} them by name", file=sys.stderr)
        return chosen
    if not slugs:
        raise SelectionError("nothing selected; pass slugs or --all")
    by_slug = {s["slug"]: s for s in specs}
    unknown = [s for s in slugs if s not in by_slug]
    if unknown:
        names = list(by_slug)
        raise SelectionError("\n".join(
            hint(s, names, everything="`raincloud list` shows every dataset.",
                 narrow="`raincloud list {query}`") for s in unknown))
    chosen = [by_slug[s] for s in dict.fromkeys(slugs)]
    if derived == "reject":
        refused = [s for s in chosen if s.get("derive")]
        if refused:
            raise SelectionError("\n".join(
                f"{s['slug']} is derived from {s['derive'].get('from')!r} and has no upstream to {verb}; "
                f"build it with `raincloud build {s['slug']}`" for s in refused))
    return chosen


def select_or_exit(parser, manifest: dict, slugs=(), **kwargs) -> list[dict]:
    """`select_specs` for an argparse CLI: a SelectionError becomes exit 2."""
    try:
        return select_specs(manifest, slugs, **kwargs)
    except SelectionError as exc:
        parser.exit(2, f"{parser.prog}: {exc}\n")
