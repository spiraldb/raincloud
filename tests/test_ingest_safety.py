# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Untrusted-input and interrupted-run behaviour for the acquisition stages.

Every case here is a defect that produced a plausible wrong answer rather than
an error: an archive whose members overwrite each other, a cache entry that is
a truncated download, a parse option that was silently ignored. They are
grouped in one module because they share a shape — the pipeline believed
something about its inputs that the inputs never promised.
"""
from __future__ import annotations

import bz2
import io
import tarfile
import zipfile
from pathlib import Path

import pytest

from raincloud.pipeline import extract as extract_mod
from raincloud.pipeline import fetch as fetch_mod

# --------------------------------------------------------------------------
# extract: archive members
# --------------------------------------------------------------------------

def _workdir(monkeypatch, tmp_path: Path) -> Path:
    monkeypatch.setenv("RAINCLOUD_WORKDIR", str(tmp_path / "wd"))
    return tmp_path


def test_zip_members_colliding_on_one_path_are_refused(monkeypatch, tmp_path):
    """Two members extracting to one file must fail, not silently overwrite.

    Zip permits duplicate names, and trailing whitespace is stripped for the
    on-disk name, so "t.csv" and "t.csv " both land on "t.csv". The extract list
    still gained one entry per member, so parse read the surviving file once per
    collision and reported that many files' worth of rows drawn from one.
    """
    _workdir(monkeypatch, tmp_path)
    archive = tmp_path / "dup.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("t.csv", "a\n1\n")
        z.writestr("t.csv ", "a\n2\n")

    with pytest.raises(ValueError, match="both extract to"):
        extract_mod.extract_zip({"slug": "dup"}, [archive])


def test_zip_directory_entry_does_not_become_a_file(monkeypatch, tmp_path):
    """A `data/` entry is a directory even though rstrip() leaves the slash on."""
    _workdir(monkeypatch, tmp_path)
    archive = tmp_path / "dir.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("data/", "")
        z.writestr("data/x.csv", "a\n1\n")

    out = extract_mod.extract_zip({"slug": "dirent"}, [archive])
    assert [p.name for p in out] == ["x.csv"]
    assert out[0].parent.is_dir()


def test_tar_symlink_member_cannot_redirect_a_write_outside(monkeypatch, tmp_path):
    """A symlink member plus a member written through it escapes with no `..`.

    This is why a path-traversal check alone is not enough: nothing in either
    member name is suspicious.
    """
    _workdir(monkeypatch, tmp_path)
    outside = tmp_path / "OUTSIDE"
    outside.mkdir()
    archive = tmp_path / "esc.tar"
    with tarfile.open(archive, "w") as t:
        link = tarfile.TarInfo("link")
        link.type = tarfile.SYMTYPE
        link.linkname = str(outside)
        t.addfile(link)
        payload = tarfile.TarInfo("link/pwned")
        payload.size = 5
        t.addfile(payload, io.BytesIO(b"OWNED"))

    try:
        extract_mod.extract_tar({"slug": "esc"}, [archive])
    except ValueError:
        pass  # refusing outright is also acceptable
    assert not (outside / "pwned").exists()


def test_tar_absolute_member_stays_inside_the_workdir(monkeypatch, tmp_path):
    _workdir(monkeypatch, tmp_path)
    target = tmp_path / "ABSOLUTE"
    archive = tmp_path / "abs.tar"
    with tarfile.open(archive, "w") as t:
        info = tarfile.TarInfo(str(target))
        info.size = 3
        t.addfile(info, io.BytesIO(b"bad"))

    try:
        extract_mod.extract_tar({"slug": "abs"}, [archive])
    except ValueError:
        pass
    assert not target.exists()


def test_interrupted_decompression_leaves_nothing_cached(monkeypatch, tmp_path):
    """A killed bunzip2 must not leave a file every later run calls [cached]."""
    _workdir(monkeypatch, tmp_path)
    src = tmp_path / "d.bz2"
    src.write_bytes(bz2.compress(b"a\n1\n2\n3\n"))

    def die(_r, _w, *a, **k):
        raise MemoryError("killed mid-write")

    monkeypatch.setattr(extract_mod.shutil, "copyfileobj", die)
    with pytest.raises(MemoryError):
        extract_mod.extract_bz2({"slug": "bz"}, [src])

    workdir = extract_mod.slug_workdir("bz")
    assert not (workdir / "d").exists()
    assert list(workdir.iterdir()) == []

    monkeypatch.undo()
    _workdir(monkeypatch, tmp_path)
    out = extract_mod.extract_bz2({"slug": "bz"}, [src])
    assert out[0].read_bytes() == b"a\n1\n2\n3\n"


# --------------------------------------------------------------------------
# fetch: what "[cached]" is allowed to mean
# --------------------------------------------------------------------------

class _Response:
    def __init__(self, payload: bytes) -> None:
        self._buf = io.BytesIO(payload)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self._buf.close()

    def read(self, n: int = -1) -> bytes:
        return self._buf.read(n)


@pytest.fixture
def http(monkeypatch, tmp_path):
    """fetch_http wired to a fake upstream, isolated raw dir, no sibling cache."""
    raw = tmp_path / "raw"
    raw.mkdir()
    monkeypatch.setenv("RAINCLOUD_RAW_DOWNLOADS", str(raw))
    monkeypatch.setattr(fetch_mod, "_find_sibling_cache", lambda *a, **kw: None)
    state = {"payload": b"a\n1\n2\n3\n", "calls": 0}

    def fake_urlopen(req, **kwargs):
        state["calls"] += 1
        return _Response(state["payload"])

    monkeypatch.setattr(fetch_mod.urllib.request, "urlopen", fake_urlopen)
    return state


def _spec(url: str) -> dict:
    return {"slug": "s", "fetch": {"type": "http", "urls": [url], "auth": None,
                                   "expected_bytes": None, "expected_sha256": None}}


def test_fetch_records_a_receipt(http):
    out = fetch_mod.fetch_http(_spec("https://example.org/d.csv"))
    receipt = fetch_mod._read_receipts(out[0].parent)["d.csv"]
    assert receipt["bytes"] == len(http["payload"])
    assert receipt["url"] == "https://example.org/d.csv"
    assert len(receipt["sha256"]) == 64


@pytest.mark.parametrize("planted", [b"", b"a\n1\n"], ids=["zero-byte", "truncated"])
def test_fetch_refuses_to_trust_a_file_that_is_not_what_it_fetched(http, planted):
    """With no declared size or sha — 402 of 436 slugs — the receipt is the check.

    Before receipts, `_already_ok` returned True for ANY existing file, so an
    OOM-killed download was served from cache forever.
    """
    spec = _spec("https://example.org/d.csv")
    out = fetch_mod.fetch_http(spec)
    assert http["calls"] == 1

    out[0].write_bytes(planted)
    again = fetch_mod.fetch_http(spec)
    assert http["calls"] == 2, "a file disagreeing with its receipt must be re-fetched"
    assert again[0].read_bytes() == http["payload"]


def test_fetch_reuses_a_file_that_matches_its_receipt(http):
    spec = _spec("https://example.org/d.csv")
    fetch_mod.fetch_http(spec)
    fetch_mod.fetch_http(spec)
    assert http["calls"] == 1


def test_fetch_refetches_when_the_recipe_points_elsewhere(http):
    """Same basename, different url: the cached bytes belong to the old one."""
    fetch_mod.fetch_http(_spec("https://example.org/a/d.csv"))
    assert http["calls"] == 1
    out = fetch_mod.fetch_http(_spec("https://example.org/b/d.csv"))
    assert http["calls"] == 2
    assert fetch_mod._read_receipts(out[0].parent)["d.csv"]["url"].endswith("/b/d.csv")


def test_fetch_accepts_a_file_with_no_receipt(http):
    """A download predating receipts stays valid — refusing would re-pull the catalog."""
    spec = _spec("https://example.org/d.csv")
    out = fetch_mod.fetch_http(spec)
    (out[0].parent / fetch_mod.RECEIPTS).unlink()
    fetch_mod.fetch_http(spec)
    assert http["calls"] == 1


# --------------------------------------------------------------------------
# parse / stats: options that were silently ignored
# --------------------------------------------------------------------------

def test_quote_char_is_honored_without_the_quoting_option(tmp_path):
    """`quote_char` alone used to be discarded by operator precedence.

    `(x or False) if quoting == "none" else '"'` ignores x on the else branch,
    so a spec setting only quote_char got '"' and its rows were skipped as
    malformed — a row count quietly short, with no error.
    """
    pytest.importorskip("pyarrow")
    from raincloud.pipeline.parse import parse_csv

    csv = tmp_path / "t.csv"
    csv.write_text("a,b\n'x,y',2\n")
    table = parse_csv({"parse": {"options": {"quote_char": "'"}}}, csv)
    assert table.num_rows == 1
    assert table.column("a")[0].as_py() == "x,y"


def test_quoting_none_disables_quoting(tmp_path):
    pytest.importorskip("pyarrow")
    from raincloud.pipeline.parse import parse_csv

    csv = tmp_path / "t.csv"
    csv.write_text('a,b\n"x",2\n')
    table = parse_csv({"parse": {"options": {"quoting": "none"}}}, csv)
    assert table.column("a")[0].as_py() == '"x"'


def test_column_stats_key_a_top_level_name_containing_a_dot(tmp_path):
    """Splitting a leaf path on '.' assigned "my.col" to a field that never existed.

    The real column was then left with no leaves and published length=0,
    null_count=0, min/max None — into the snapshot, the wheel and the native
    reader's embedded catalog.
    """
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    from raincloud.pipeline.spec import read_column_stats

    path = tmp_path / "t.parquet"
    pq.write_table(pa.table({"plain": pa.array([1, 2, None]),
                             "my.col": pa.array([10, 20, None])}), path)
    stats = {row["name"]: row for row in read_column_stats(path)}
    assert stats["my.col"]["length"] > 0
    assert stats["my.col"]["null_count"] == 1
    assert (stats["my.col"]["min"], stats["my.col"]["max"]) == (10, 20)


# --------------------------------------------------------------------------
# publish: the gates are default-deny
# --------------------------------------------------------------------------

def test_unknown_slug_is_not_silently_publishable(capsys):
    """Both license gates read the spec with `.get(slug, {})`.

    A slug in the snapshot but absent from sources.json produced None for every
    flag and passed both — default-permit on the checks that exist to stop an
    unclearable corpus reaching a mirror. So publish refuses an unknown name
    (exit 2, with a did-you-mean) before either gate runs.
    """
    from raincloud.pipeline import publish
    from raincloud.pipeline.publish import no_redistribution_slugs, scrape_advisory_slugs

    manifest = {"datasets": [{"slug": "known", "license": {"redistribution_permitted": True}}]}
    slugs = ["known", "ghost"]
    # The license gates say nothing about an unknown slug — which is the point.
    assert scrape_advisory_slugs(manifest, slugs) == []
    assert no_redistribution_slugs(manifest, slugs) == []
    with pytest.raises(SystemExit) as exc:
        publish.main(["uci-irs", "--mirror", "file:///nonexistent-raincloud-mirror", "--dry-run"])
    assert exc.value.code == 2
    assert "uci-iris" in capsys.readouterr().err
