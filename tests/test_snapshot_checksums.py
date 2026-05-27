# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
import hashlib


def test_sha256_for_path_helper(tmp_path):
    from scripts.pipeline.docs import _sha256_for_path
    f = tmp_path / "x.bin"; f.write_bytes(b"abc")
    assert _sha256_for_path(f) == hashlib.sha256(b"abc").hexdigest()
    assert _sha256_for_path(tmp_path / "missing") is None
