# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Shared catalog-discovery vocab + filter engine.

Imported by browse.py (TUI), list_datasets.py (CLI), and validate_manifest.py.
Single source of truth for the closed vocabs and the view-preset registry.

Two of the four discovery axes are *editorial* — authored in sources.json:
  * `tags`     — domain vocab; 0–3 per spec; values must come from TAG_VOCAB.
  * `showcase` — editorial tier membership; values must come from SHOWCASE_TIERS.

Two are *derived* — computed by docs.py and stored in snapshot.json per slug:
  * `size_bucket`   — file-size bucket, one of SIZE_BUCKETS.
  * `shape_traits`  — booleans (or null) for each member of TRAIT_FLAGS.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


TAG_VOCAB: tuple[str, ...] = (
    # String content
    "urls",
    "prose",
    "enums",
    "identifiers",
    "code-strings",
    # Numeric content
    "timestamps",
    "embeddings",
    "counts",
    "monetary",
    "measurements",
    # Payload / structure
    "coordinates",
    "binary-payload",
    "nested-json",
)

SHOWCASE_TIERS: tuple[str, ...] = (
    "encoding",
    "stress",
)

SIZE_BUCKETS: tuple[str, ...] = ("xs", "s", "m", "l", "xl")

_KB = 1024
_MB = 1024 * _KB
_GB = 1024 * _MB

SIZE_BUCKET_BOUNDS: dict[str, tuple[float, float]] = {
    "xs": (0, 10 * _MB),
    "s":  (10 * _MB, 100 * _MB),
    "m":  (100 * _MB, 1 * _GB),
    "l":  (1 * _GB, 10 * _GB),
    "xl": (10 * _GB, float("inf")),
}

TRAIT_FLAGS: tuple[str, ...] = (
    "has_nested",
    "has_timestamp",
    "has_variant",
    "string_heavy",
    "wide_row",
    "high_cardinality_present",
)

# Saved facet selections used by the TUI "View" bar and the CLI --view flag.
# Each preset names axes → values; clicking a preset replaces the current
# selection in those axes only (axes not mentioned are cleared).
VIEW_PRESETS: dict[str, dict[str, set[str]]] = {
    "encoding": {"showcase": {"encoding"}},
    "stress":   {"showcase": {"stress"}},
}


def bucket_for_size(num_bytes: int | float) -> str:
    """Return the size-bucket label for an on-disk byte count.

    Negative input is treated as zero (defensive — shouldn't happen).
    """
    if num_bytes < 0:
        num_bytes = 0
    for label in SIZE_BUCKETS:
        lo, hi = SIZE_BUCKET_BOUNDS[label]
        if lo <= num_bytes < hi:
            return label
    return "xl"


@dataclass
class FilterState:
    """Multi-axis catalog filter.

    Selections within an axis OR-combine; across axes they AND-combine.
    `dataclasses.replace(state, tag=new)` produces a fully independent
    copy — unchanged set fields are cloned in `__post_init__` so callers
    can freely mutate either instance.

    `vortex` is tri-valued: None means "don't filter on vortex", True means
    "only available", False means "only skipped".
    """

    showcase: set[str] = field(default_factory=set)
    tag: set[str] = field(default_factory=set)
    size: set[str] = field(default_factory=set)
    trait: set[str] = field(default_factory=set)
    trait_negated: set[str] = field(default_factory=set)
    license: set[str] = field(default_factory=set)
    fetch_type: set[str] = field(default_factory=set)
    vortex: Optional[bool] = None

    def __post_init__(self):
        """Defensively clone mutable set fields so `dataclasses.replace`
        produces fully independent instances (otherwise unchanged set fields
        are shared by reference between old and new)."""
        self.showcase = set(self.showcase)
        self.tag = set(self.tag)
        self.size = set(self.size)
        self.trait = set(self.trait)
        self.trait_negated = set(self.trait_negated)
        self.license = set(self.license)
        self.fetch_type = set(self.fetch_type)

    def is_empty(self) -> bool:
        return (
            not self.showcase and not self.tag and not self.size
            and not self.trait and not self.trait_negated
            and not self.license
            and not self.fetch_type and self.vortex is None
        )

    def matches(self, *, spec: dict, snapshot: dict) -> bool:
        """Return True if the joined (spec, snapshot) record passes all filters."""
        if self.showcase and not (set(spec.get("showcase") or []) & self.showcase):
            return False
        if self.tag and not (set(spec.get("tags") or []) & self.tag):
            return False
        if self.size:
            bucket = snapshot.get("size_bucket")
            if bucket not in self.size:
                return False
        traits = snapshot.get("shape_traits") or {}
        for flag in self.trait:
            if traits.get(flag) is not True:
                return False
        for flag in self.trait_negated:
            if traits.get(flag) is True:
                return False
        if self.license:
            lic = ((spec.get("license") or {}).get("spdx"))
            if lic not in self.license:
                return False
        if self.fetch_type:
            ft = (spec.get("fetch") or {}).get("type")
            if ft not in self.fetch_type:
                return False
        if self.vortex is not None:
            available = bool((spec.get("convert") or {}).get("vortex", False))
            if available != self.vortex:
                return False
        return True


def apply_preset(name: str) -> FilterState:
    """Return a new FilterState carrying only the named preset's selections.

    Axes mentioned by the preset are populated; other axes are empty.
    Raises KeyError if `name` is not a registered preset.
    """
    preset = VIEW_PRESETS[name]
    new = FilterState()
    for axis, values in preset.items():
        if not hasattr(new, axis):
            raise ValueError(
                f"VIEW_PRESETS['{name}'] mentions unknown axis {axis!r}"
            )
        setattr(new, axis, set(values))
    return new


def _is_variant_field(field) -> bool:
    """Detect raincloud's persistent-VARIANT marker in pyarrow field metadata.

    Pyarrow Field is duck-typed via its `.metadata` attribute; this helper
    avoids importing pyarrow into discovery.py (keeping the module
    dependency-light for validate_manifest.py).
    """
    md = getattr(field, "metadata", None) or {}
    return b"__variant_type" in md


SPARK_CHARS = " ▁▂▃▄▅▆▇█"


def sparkline(counts: list[int]) -> str:
    """Render a list of non-negative counts as a Unicode-block sparkline."""
    if not counts:
        return ""
    hi = max(counts)
    if hi == 0:
        return SPARK_CHARS[0] * len(counts)
    return "".join(
        SPARK_CHARS[int(round((c / hi) * (len(SPARK_CHARS) - 1)))]
        for c in counts
    )


def format_column_line(name: str, profile: dict | None) -> str:
    """One-line text rendering of a per-column profile.

    Used by `list_datasets --inspect` (this task) and the TUI detail pane
    (Task 17). For struct/variant or empty profiles, emits a "skipped" note.
    """
    if profile is None:
        return f"  {name:<20} struct/variant       (skipped)"
    dtype = profile.get("dtype", "?")
    if "histogram" in profile:
        spark = sparkline(profile["histogram"]["counts"])
        lo = profile.get("min")
        hi = profile.get("max")
        return f"  {name:<20} {dtype:<14} {lo} … {hi}  {spark}"
    if "ndv_approx" in profile:
        ndv = profile["ndv_approx"]
        top = profile.get("top_values")
        if top:
            preview = ", ".join(f"{t['value']!r}({t['count']})" for t in top[:3])
            return f"  {name:<20} {dtype:<14} NDV≈{ndv}  top: {preview}"
        return f"  {name:<20} {dtype:<14} NDV≈{ndv}, mean_len={profile.get('mean_length')}"
    if "true_count" in profile:
        return f"  {name:<20} {dtype:<14} T:{profile['true_count']} F:{profile['false_count']} N:{profile['null_count']}"
    if "length_min" in profile:
        return f"  {name:<20} {dtype:<14} len {profile['length_min']}…{profile['length_max']} (avg {profile['length_mean']:.1f})"
    return f"  {name:<20} {dtype:<14} (no stats)"
