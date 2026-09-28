# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Every writer reads back what it writes: a file that does not read back to the
canonical is never promoted, and a file promoted unverified says so."""
from __future__ import annotations

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import raincloud
from raincloud import _builds, cli
from raincloud._resolve import artifact_key
from raincloud.catalogs import operation
from raincloud.pipeline import build, compliance, docs
from raincloud.pipeline.export import Verdict, exporters, get_exporter, readers, run_bounded
from raincloud.pipeline.export.__main__ import main as export_main
from raincloud.pipeline.export.base import Compliance, ExportResult
from raincloud.pipeline.export.compare import stream_equal
from raincloud.pipeline.spec import prepared_arrow, prepared_parquet, prepared_vortex
from tests.test_pipeline_contracts import TINY, _catalog, _mock_sidecar
from tests.test_pipeline_contracts import stages as stages  # noqa: F401 (fixture)

vortex = pytest.importorskip("vortex")


def _entry(cfg, fmt):
    return _builds.read(cfg.data_dir)[artifact_key("tiny", fmt, 2)]


def _wrong_parquet(monkeypatch):
    """parquet@py writes a valid file holding other values than the canonical's."""
    def write(canonical, dest, row_group, byte_target, *, compression, stats):
        with pa.ipc.open_file(str(canonical)) as reader:
            table = reader.read_all()
        pq.write_table(table.set_column(0, "x", pa.array([7] * table.num_rows)), dest)
        return False
    monkeypatch.setattr(exporters, "_write_parquet", write)


def _junk_vortex(monkeypatch):
    """vortex@py 'writes' bytes no Vortex reader can open."""
    def write(reader, path):
        for _ in reader:
            pass
        open(path, "wb").write(b"not a vortex file")
    monkeypatch.setattr(vortex.io, "write", write)


# ---------- a file that does not read back is never promoted ----------

@pytest.mark.parametrize("fmt, fault, error", [
    ("parquet", _wrong_parquet, "read back: parquet@py: data mismatch vs canonical — rows 0..2: column 'x'"),
    ("vortex", _junk_vortex, "read back: vortex@py: read error — "),
])
def test_a_file_that_does_not_read_back_is_recorded_unavailable(tmp_path, stages, monkeypatch, fmt, fault, error):
    cfg = _catalog(tmp_path, f"rb-{fmt}", [TINY])
    fault(monkeypatch)
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        path = {"parquet": prepared_parquet, "vortex": prepared_vortex}[fmt]("tiny")
        assert not path.exists() and not list(path.parent.glob(".*"))
    measured = _entry(cfg, fmt)["unavailable"]
    assert measured["cell"] == f"{fmt}@py" and error in measured["error"], measured
    other = "vortex" if fmt == "parquet" else "parquet"
    assert _entry(cfg, other)["verified"] is True


@pytest.mark.parametrize("fmt, fault", [("parquet", _wrong_parquet), ("vortex", _junk_vortex)])
def test_the_previous_file_stays_when_a_new_one_does_not_read_back(tmp_path, stages, monkeypatch, capsys, fmt, fault):
    cfg = _catalog(tmp_path, f"rb-keep-{fmt}", [TINY])
    with operation(cfg):
        assert build.run_one(TINY, strict=False)
        good = _entry(cfg, fmt)
        fault(monkeypatch)
        assert export_main(["tiny"]) == 0
        err = capsys.readouterr().err
        assert f"[export failed] tiny/{fmt}: {fmt}@py: a measured write failure — read back: " in err
        assert _entry(cfg, fmt) == good
        assert raincloud.load("tiny", format=fmt, config=cfg).to_arrow()["x"].to_pylist() == [1, 2]


def test_an_in_process_writer_never_reports_its_read_back_unmeasured(tmp_path, monkeypatch):
    real = type(get_exporter("parquet@py")).export

    def unmeasured(self, spec, canonical, dest=None):
        result = real(self, spec, canonical, dest)
        return ExportResult(result.format_id, result.out_path, result.nbytes, result.sha256,
                            Compliance(roundtrip=None, variant_faithful=True))
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    from raincloud.pipeline import canonical
    (path,) = canonical.write_canonical({"slug": "t"}, [("t", pa.table({"x": [1]}))])
    monkeypatch.setattr(type(get_exporter("parquet@py")), "export", unmeasured)
    with pytest.raises(RuntimeError, match="reported its read-back unmeasured"):
        run_bounded(get_exporter("parquet@py"), {"slug": "t"}, path, tmp_path / "out.parquet")
    assert not (tmp_path / "out.parquet").exists()


def test_a_read_back_that_decides_nothing_is_a_bug_not_a_pass(tmp_path, monkeypatch):
    monkeypatch.setattr(readers.PyarrowParquetReader, "read_conformance",
                        lambda self, artifact, canonical: Verdict("skip", note="comparator gap"))
    with pytest.raises(RuntimeError, match="decides pass or fail"):
        exporters.read_back("parquet@py", tmp_path / "x.parquet", tmp_path / "x.arrow.zstd")


# ---------- the streamed comparison ----------

def _batches(values, sizes, log=None):
    start = 0
    for size in sizes:
        if log is not None:
            log.append(size)
        yield pa.record_batch({"x": pa.array(values[start:start + size], type=pa.int64())})
        start += size


SCHEMA = pa.schema({"x": pa.int64()})


def test_streamed_compare_realigns_differing_batch_layouts():
    values = list(range(100))
    assert stream_equal(SCHEMA, _batches(values, [1, 0, 7, 33, 59]), SCHEMA, _batches(values, [50, 50])) == (True, "")
    changed = values[:61] + [-1] + values[62:]
    equal, detail = stream_equal(SCHEMA, _batches(changed, [1, 7, 33, 59]), SCHEMA, _batches(values, [50, 50]))
    assert not equal and detail.startswith("rows 50..100: column 'x'"), detail
    equal, detail = stream_equal(SCHEMA, _batches(values[:90], [45, 45]), SCHEMA, _batches(values, [50, 50]))
    assert (equal, detail) == (False, "row count 90 != canonical 100")


def test_streamed_compare_holds_one_batch_of_each_side():
    """A mismatch in the first window is found before either stream is read
    further: the comparison pulls one batch at a time, never the whole file."""
    values = list(range(1000))
    got_log, expected_log = [], []
    equal, _ = stream_equal(SCHEMA, _batches([-1] + values[1:], [10] * 100, got_log),
                            SCHEMA, _batches(values, [100] * 10, expected_log))
    assert not equal and got_log == [10] and expected_log == [100]


def test_streamed_compare_checks_names_and_types_of_an_empty_file():
    other = pa.schema({"y": pa.int64()})
    assert stream_equal(other, iter(()), SCHEMA, iter(()))[0] is False
    assert stream_equal(pa.schema({"x": pa.string()}), iter(()), SCHEMA, iter(()))[0] is False
    assert stream_equal(SCHEMA, iter(()), SCHEMA, iter(())) == (True, "")


def test_parquet_read_batches_are_sized_by_bytes(tmp_path, monkeypatch):
    """Rows of 1 MiB in groups of 64: a 4 MiB budget reads a few rows a batch,
    while groups of small rows are packed into one batch."""
    monkeypatch.setattr(readers, "_READ_BATCH_BYTES", 4 << 20)
    big = pa.table({"s": pa.array([bytes([i]) * (1 << 20) for i in range(128)], type=pa.binary())})
    small = pa.table({"s": pa.array([b"y"] * 128, type=pa.binary())})
    path = tmp_path / "t.parquet"
    with pq.ParquetWriter(path, big.schema, compression="none", use_dictionary=False) as writer:
        writer.write_table(big, row_group_size=64)
        writer.write_table(small, row_group_size=64)
    plan = readers._read_plan(pq.ParquetFile(path).metadata)
    assert [groups for groups, _ in plan] == [[0], [1], [2, 3]]
    assert all(1 <= rows <= 4 for _, rows in plan[:2]) and plan[2][1] == 128
    _schema, rows, batches = readers.parquet_batches(path)
    sizes = [b.num_rows for b in batches]
    assert rows == 256 and sum(sizes) == 256 and max(sizes[:-1]) <= 4


# ---------- a sidecar that cannot verify: promoted, and says so ----------

_UNVERIFIED_REPORT = """
import argparse, json
import pyarrow as pa, pyarrow.parquet as pq
ap = argparse.ArgumentParser()
for flag in ("--input", "--output", "--report"):
    ap.add_argument(flag)
a = ap.parse_args()
with pa.ipc.open_file(a.input) as r:
    pq.write_table(r.read_all(), a.output)
json.dump({"roundtrip": None, "variant_faithful": True,
           "note": "comparator gap: decimal256 is not compared"}, open(a.report, "w"))
"""
RS_FIRST = {**TINY, "export": {**TINY["export"], "priority": {"parquet": ["rs", "py"]}}}
GAP = "comparator gap: decimal256 is not compared"


def _settings(cfg, name, tmp_path):
    return json.dumps({"no_config": True, "catalog": str(tmp_path / name), "data_dir": str(cfg.data_dir),
                       "cache_dir": str(cfg.cache_dir), "catalog_dir": str(tmp_path / "catalogs"),
                       "offline": True})


def test_an_unverified_sidecar_file_is_promoted_recorded_and_described(tmp_path, stages, monkeypatch, capsys):
    name = "rb-unverified"
    cfg = _catalog(tmp_path, name, [RS_FIRST])
    _mock_sidecar(tmp_path, monkeypatch, _UNVERIFIED_REPORT)
    with operation(cfg):
        assert build._main(["tiny"]) == 0
        out = capsys.readouterr()
        assert f"[unverified] tiny/parquet: parquet@rs published its file without verifying it reads back: {GAP}" \
            in out.err
        assert "unverified=1" in out.out and f"[unverified] tiny/parquet: parquet@rs: {GAP}" in out.out
        entry = _entry(cfg, "parquet")
        assert entry["writer"] == "rs" and entry["verified"] is False and entry["verify_note"] == GAP
        assert _entry(cfg, "vortex")["verified"] is True and "verify_note" not in _entry(cfg, "vortex")
        assert pq.read_table(prepared_parquet("tiny"))["x"].to_pylist() == [1, 2]

        destination = tmp_path / "snapshot.json"
        docs.generate_snapshot(destination=destination)
        assert f"[docs] tiny/parquet: rs did not verify that its file reads back: {GAP}" in capsys.readouterr().err
        snap = json.loads(destination.read_text())["slugs"]["tiny"]
        assert snap["parquet_verified"] is False and snap["parquet_verify_note"] == GAP
        assert snap["vortex_verified"] is True and "vortex_verify_note" not in snap
        assert "arrow_verified" not in snap

    settings = _settings(cfg, name, tmp_path)
    assert cli.main(["--json", "--settings", settings, "describe", "tiny"]) == 0
    about = json.loads(capsys.readouterr().out)
    assert about["formats"]["parquet"]["unverified"] == GAP
    assert "unverified" not in about["formats"]["vortex"]
    assert cli.main(["--settings", settings, "describe", "tiny", "--format", "parquet"]) == 0
    text = " ".join(capsys.readouterr().out.split())
    assert "parquet size unknown UNVERIFIED (default, here)" in text
    assert f"parquet: not verified to read back: {GAP}" in text


def test_describe_shows_the_catalogs_unverified_file(tmp_path, capsys):
    name = "rb-catalog"
    cfg = _catalog(tmp_path, name, [TINY], {"tiny": {
        "parquet_bytes": 10, "parquet_writer": "java", "parquet_verified": False,
        "parquet_verify_note": "OutOfMemoryError while verifying"}})
    assert cli.main(["--json", "--settings", _settings(cfg, name, tmp_path), "describe", "tiny"]) == 0
    about = json.loads(capsys.readouterr().out)
    assert about["formats"]["parquet"]["unverified"] == "OutOfMemoryError while verifying"


# ---------- compliance measures with the same read-back ----------

def test_compliance_write_cells_use_the_writers_read_back(tmp_path, monkeypatch):
    """One reader decides the write verdict whether compliance writes the file
    now or finds it on disk: planting a verdict in it changes both."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    from raincloud.pipeline import canonical
    canonical.write_canonical({"slug": "rb-c"}, [("rb-c", pa.table({"x": [1, 2]}))])

    def measure(**kw):
        (result,) = compliance.run_compliance({"slug": "rb-c"}, cells=["parquet@py"],
                                              reader_ids=["vortex@py"], **kw).write_results
        return result
    fresh = measure(reencode=True)
    assert fresh.compliance.roundtrip is True
    found = measure()
    assert found.compliance.roundtrip is True
    assert found.compliance.note.startswith("pre-existing (in-process, fresh); not re-encoded; read back: ")

    monkeypatch.setattr(readers.PyarrowParquetReader, "read_conformance",
                        lambda self, artifact, canonical: Verdict("fail", note="parquet@py: planted", detail="x"))
    for result in (measure(), measure(reencode=True)):
        assert result.compliance.roundtrip is False and result.sha256 == ""
        assert "read back: parquet@py: planted — x" in result.compliance.note


def test_the_canonical_is_never_read_whole(tmp_path, monkeypatch):
    """The read-back and the read cells stream the canonical batch by batch."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    from raincloud.pipeline import canonical
    (path,) = canonical.write_canonical({"slug": "rb-s"}, [("rb-s", pa.table({"x": list(range(10))}))])

    def refuse(self):
        raise AssertionError("read_all on a canonical")
    monkeypatch.setattr(pa.ipc.RecordBatchFileReader, "read_all", refuse)
    for cell in ("parquet@py", "vortex@py"):
        result = get_exporter(cell).export({"slug": "rb-s"}, path, tmp_path / f"out.{cell[:-3]}")
        assert result.compliance.roundtrip is True
    assert prepared_arrow("rb-s") == path


def test_compliance_does_not_read_an_in_process_file_twice(tmp_path, monkeypatch):
    """The diagonal read cell of an in-process writer is its read-back: the
    reader is not run again over the file in this process."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    from raincloud.pipeline import canonical
    canonical.write_canonical({"slug": "rb-d"}, [("rb-d", pa.table({"x": [1, 2]}))])
    real = readers.PyarrowParquetReader.read_conformance
    calls = []

    def counted(self, artifact, canonical):
        calls.append(artifact)
        return real(self, artifact, canonical)
    monkeypatch.setattr(readers.PyarrowParquetReader, "read_conformance", counted)
    sc = compliance.run_compliance({"slug": "rb-d"}, cells=["parquet@py"], reader_ids=["parquet@py"],
                                   reencode=True)
    (cell,) = sc.read_results
    assert calls == [] and cell.verdict.status == "pass"
    assert cell.verdict.note == "parquet@py: round-trips to canonical"
