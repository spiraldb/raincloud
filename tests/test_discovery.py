# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the discovery vocab + filter engine.

These tests are side-effect-free: they don't read sources.json or any parquet.
"""
from __future__ import annotations

import pytest

from scripts.pipeline.discovery import (
    TAG_VOCAB,
    SHOWCASE_TIERS,
    SIZE_BUCKETS,
    SIZE_BUCKET_BOUNDS,
    TRAIT_FLAGS,
    VIEW_PRESETS,
    bucket_for_size,
)


def test_vocab_shapes():
    assert len(TAG_VOCAB) == 12
    assert len(SHOWCASE_TIERS) == 4
    assert SIZE_BUCKETS == ("xs", "s", "m", "l", "xl")
    assert "has_nested" in TRAIT_FLAGS
    assert "high_cardinality_present" in TRAIT_FLAGS
    # No duplicates anywhere.
    for vocab in (TAG_VOCAB, SHOWCASE_TIERS, SIZE_BUCKETS, TRAIT_FLAGS):
        assert len(set(vocab)) == len(vocab)


def test_view_presets_reference_valid_vocab():
    """Every preset only mentions axes and values from the closed vocabs."""
    for name, axes in VIEW_PRESETS.items():
        for axis, values in axes.items():
            if axis == "showcase":
                assert values <= set(SHOWCASE_TIERS), (name, values)
            elif axis == "size":
                assert values <= set(SIZE_BUCKETS), (name, values)
            elif axis == "tag":
                assert values <= set(TAG_VOCAB), (name, values)


@pytest.mark.parametrize("nbytes,expected", [
    (0, "xs"),
    (10 * 1024 * 1024 - 1, "xs"),
    (10 * 1024 * 1024, "s"),
    (100 * 1024 * 1024 - 1, "s"),
    (100 * 1024 * 1024, "m"),
    (1024 ** 3, "l"),
    (10 * (1024 ** 3) - 1, "l"),
    (10 * (1024 ** 3), "xl"),
    (1000 * (1024 ** 3), "xl"),
])
def test_bucket_for_size_boundaries(nbytes, expected):
    assert bucket_for_size(nbytes) == expected


def test_size_bucket_bounds_cover_all_buckets():
    """Every named bucket has a (lo, hi) entry."""
    assert set(SIZE_BUCKET_BOUNDS) == set(SIZE_BUCKETS)


from scripts.pipeline.discovery import FilterState, apply_preset


def test_filter_state_empty_matches_everything():
    state = FilterState()
    spec = {"family": "uci", "license": {"spdx": "MIT"}, "tags": [], "showcase": []}
    assert state.matches(spec=spec, snapshot={})


def test_filter_state_showcase_or_within_axis():
    state = FilterState(showcase={"start-here", "encoding-research"})
    assert state.matches(spec={"showcase": ["start-here"]}, snapshot={})
    assert state.matches(spec={"showcase": ["encoding-research", "other"]}, snapshot={})
    assert not state.matches(spec={"showcase": ["other"]}, snapshot={})
    assert not state.matches(spec={"showcase": []}, snapshot={})


def test_filter_state_tag_and_family_and_combine():
    state = FilterState(tag={"geospatial"}, family={"uci"})
    assert state.matches(
        spec={"family": "uci", "tags": ["geospatial"]}, snapshot={}
    )
    assert not state.matches(
        spec={"family": "kaggle-upstream", "tags": ["geospatial"]}, snapshot={}
    )
    assert not state.matches(
        spec={"family": "uci", "tags": ["finance"]}, snapshot={}
    )


def test_filter_state_size_uses_snapshot_bucket():
    state = FilterState(size={"l", "xl"})
    assert state.matches(spec={}, snapshot={"size_bucket": "l"})
    assert state.matches(spec={}, snapshot={"size_bucket": "xl"})
    assert not state.matches(spec={}, snapshot={"size_bucket": "s"})
    assert not state.matches(spec={}, snapshot={})   # missing => fails size filter


def test_filter_state_trait_positive():
    state = FilterState(trait={"has_nested"})
    assert state.matches(spec={}, snapshot={"shape_traits": {"has_nested": True}})
    assert not state.matches(spec={}, snapshot={"shape_traits": {"has_nested": False}})
    assert not state.matches(spec={}, snapshot={"shape_traits": {"has_nested": None}})
    # Truthy non-True must NOT satisfy a positive trait filter (is True semantics).
    assert not state.matches(spec={}, snapshot={"shape_traits": {"has_nested": 1}})
    assert not state.matches(spec={}, snapshot={"shape_traits": {"has_nested": "yes"}})


def test_filter_state_trait_negated():
    state = FilterState(trait_negated={"has_nested"})
    assert state.matches(spec={}, snapshot={"shape_traits": {"has_nested": False}})
    assert not state.matches(spec={}, snapshot={"shape_traits": {"has_nested": True}})
    # null is "unknown" → does not match a positive negation
    assert state.matches(spec={}, snapshot={"shape_traits": {"has_nested": None}})
    # Truthy non-True is NOT identical to True under is-True semantics — must match a negation.
    assert state.matches(spec={}, snapshot={"shape_traits": {"has_nested": 1}})


def test_filter_state_vortex_two_state():
    available = FilterState(vortex=True)
    skipped = FilterState(vortex=False)
    spec_yes = {"convert": {"vortex": True}}
    spec_no = {"convert": {"vortex": False}}
    spec_unset = {}
    assert available.matches(spec=spec_yes, snapshot={})
    assert not available.matches(spec=spec_no, snapshot={})
    assert skipped.matches(spec=spec_no, snapshot={})
    assert skipped.matches(spec=spec_unset, snapshot={})


def test_apply_preset_stress_test():
    new = apply_preset("stress-test")
    assert new.showcase == {"stress-test"}
    assert new.size == {"l", "xl"}
    # axes not in the preset are cleared
    assert new.tag == set()


def test_apply_preset_start_here_clears_other_axes():
    """Axes that aren't part of the preset come back empty."""
    new = apply_preset("start-here")
    assert new.showcase == {"start-here"}
    assert new.size == set()


def test_apply_preset_unknown_raises():
    with pytest.raises(KeyError):
        apply_preset("no-such-preset")


def test_filter_state_replace_does_not_share_set_fields():
    """dataclasses.replace must produce fully independent copies."""
    import dataclasses
    a = FilterState(showcase={"start-here"}, tag={"geospatial"})
    b = dataclasses.replace(a, family={"uci"})
    # Mutating b's untouched axes must not bleed into a.
    b.tag.add("nlp-text")
    b.showcase.add("encoding-research")
    assert a.tag == {"geospatial"}
    assert a.showcase == {"start-here"}
