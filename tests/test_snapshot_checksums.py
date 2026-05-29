# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import hashlib


def test_sha256_for_path_helper(tmp_path):
    from scripts.pipeline.docs import _sha256_for_path
    f = tmp_path / "x.bin"; f.write_bytes(b"abc")
    assert _sha256_for_path(f) == hashlib.sha256(b"abc").hexdigest()
    assert _sha256_for_path(tmp_path / "missing") is None


def test_sha256_or_reuse_reuses_on_size_match(tmp_path):
    """Default fast path: same size + known prior sha -> reuse without hashing."""
    from scripts.pipeline.docs import _sha256_or_reuse
    f = tmp_path / "x.bin"; f.write_bytes(b"NEWCONTENT")  # 10 bytes
    # prior recorded 10 bytes + a (now stale) sha; reuse keeps the stale sha.
    assert _sha256_or_reuse(f, 10, 10, "STALE") == "STALE"


def test_sha256_or_reuse_force_recomputes_same_size_drift(tmp_path):
    """force=True (the --rehash flag) breaks the publish deadlock: a rebuild
    that changes content without changing byte length must get a fresh sha,
    not the stale prior one the size-match would otherwise reuse."""
    from scripts.pipeline.docs import _sha256_or_reuse
    f = tmp_path / "x.bin"; f.write_bytes(b"NEWCONTENT")  # 10 bytes
    real = hashlib.sha256(b"NEWCONTENT").hexdigest()
    # Without force: stale sha survives (the bug). With force: real sha.
    assert _sha256_or_reuse(f, 10, 10, "STALE") == "STALE"
    assert _sha256_or_reuse(f, 10, 10, "STALE", force=True) == real


def test_sha256_or_reuse_force_preserves_missing(tmp_path):
    """force must still preserve the prior sha for files absent this run, so
    --rehash on a partial checkout doesn't dash out tracked ground truth."""
    from scripts.pipeline.docs import _sha256_or_reuse
    missing = tmp_path / "gone.bin"
    assert _sha256_or_reuse(missing, None, 10, "PRIOR", force=True) == "PRIOR"
