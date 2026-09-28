# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Did-you-mean for names people type: dataset slugs and CLI commands."""
from __future__ import annotations

import difflib
import re


def suggest(query: str, names, *, limit: int = 5) -> tuple[list[str], int]:
    """Closest `names` to `query`, best first, and how many more also matched.

    A name matches when it contains every word of the query ("tpch-sf1-lineitem"
    finds "tpcgen-rs-tpch-sf1-lineitem", "lineitem" finds the whole family);
    otherwise close spellings are offered, for typos.
    """
    names = sorted(set(names))
    words = [w for w in re.split(r"[-_\s/]+", query.lower()) if w]
    contained = sorted((n for n in names if words and all(w in n.lower() for w in words)),
                       key=lambda n: (len(n), n))
    if contained:
        return contained[:limit], max(0, len(contained) - limit)
    return difflib.get_close_matches(query, names, n=limit, cutoff=0.6), 0


def hint(query: str, names, *, noun: str = "dataset", everything: str | None = None,
         narrow: str | None = None, canonical: dict[str, str] | None = None) -> str:
    """Message for an unknown name. `everything` says how to see all names;
    `narrow` (formatted with the query) how to see every match when some are cut;
    `canonical` reports an alias match as the name it stands for."""
    found, extra = suggest(query, names)
    if canonical:
        found = list(dict.fromkeys(canonical.get(n, n) for n in found))
    message = f"unknown {noun} {query!r}."
    if not found:
        return f"{message} {everything}" if everything else message
    listed = found[0] if len(found) == 1 else "one of: " + ", ".join(found)
    message += f" Did you mean {listed}?"
    if extra:
        message += f" (+{extra} more" + (f": {narrow.format(query=query)}" if narrow else "") + ")"
    return message
