# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Tests for the built-in exporters, `run_exporters` and the registered reader set.

Hermetic per test_canonical.py: `RAINCLOUD_HOME` points at a tmp dir so
`prepared_parquet` / `prepared_vortex` resolve under tmp, never the real
outputs/. Covers the default parquet@py + vortex@py fan-out over a canonical
Arrow artifact, the skip-with-note path for an unregistered format, and the
factbook-shaped VARIANT case where the VARIANT column reaches the canonical
Arrow and the Parquet export is honestly marked `variant_faithful=False`.
"""
from __future__ import annotations

import pyarrow as pa
import pytest

from raincloud import duckdb_connect
from raincloud.pipeline import discovery, duckdb_variant
from raincloud.pipeline.export import run_exporters, slug_from_canonical
from raincloud.pipeline.export.exporters import ParquetExporter, VortexExporter
from raincloud.pipeline.spec import (
    prepared_arrow,
    prepared_parquet,
    prepared_vortex,
)
from tests._helpers import write_canonical


def test_run_exporters_default_produces_parquet_and_vortex(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    slug = "exporters-default"
    table = pa.table(
        {"n": pa.array([1, 2, 3], type=pa.int32()), "s": ["a", "b", "c"]}
    )
    canonical_path = write_canonical(slug, table)

    results = run_exporters({"slug": slug}, canonical_path)

    # Both default cells ran, tagged with their qualified ledger cell-ids.
    assert {r.format_id for r in results} == {"parquet@py", "vortex@py"}
    for r in results:
        assert r.nbytes > 0 and len(r.sha256) == 64
        # MEASURED: every export reads its file back and compares it to the
        # canonical before reporting it.
        assert r.compliance.roundtrip is True
        assert r.compliance.roundtrip_measured is True
        # No VARIANT column -> faithful.
        assert r.compliance.variant_faithful is True

    # Bare on-disk dirs; both artifacts round-trip.
    parquet = prepared_parquet(slug)
    vortex_path = prepared_vortex(slug)
    assert parquet.exists() and vortex_path.exists()

    import pyarrow.parquet as pq

    assert pq.read_table(parquet).equals(table)

    import vortex

    vt = vortex.open(str(vortex_path)).to_arrow().read_all()
    assert vt.num_rows == 3 and vt.column_names == ["n", "s"]


def test_run_exporters_raises_on_explicitly_requested_unknown_cell(tmp_path, monkeypatch):
    """An EXPLICIT export.formats entry with no registered exporter FAILS the build.

    It used to be skipped with a stderr note, so a typo'd or not-yet-implemented
    cell produced a successful build that silently lacked the requested output.
    """
    import pytest

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    slug = "exporters-skip"
    table = pa.table({"n": pa.array([1], type=pa.int32())})
    canonical_path = write_canonical(slug, table)

    with pytest.raises(RuntimeError, match="no registered exporter"):
        run_exporters({"slug": slug}, canonical_path, ["parquet", "csv@py"])


def test_run_exporters_honors_parquet_only_formats(tmp_path, monkeypatch):
    """A dataset that opts out of Vortex declares `export.formats: ["parquet"]`
    (the reason in `export.notes`) and exports parquet but NOT vortex -- so a
    vortex-skip slug like code-contests isn't forced through a failing export."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "exporters-novortex"
    table = pa.table({"n": pa.array([1, 2], type=pa.int32())})
    canonical_path = write_canonical(slug, table)

    spec = {"slug": slug, "export": {"formats": ["parquet"], "notes": "vortex cannot encode it"}}
    results = run_exporters(spec, canonical_path)

    assert [r.format_id for r in results] == ["parquet@py"]  # vortex dropped
    assert prepared_parquet(slug).exists()
    assert not prepared_vortex(slug).exists()


@pytest.mark.parametrize("exporter", [ParquetExporter, VortexExporter])
def test_exporter_preserves_variant_storage(tmp_path, monkeypatch, exporter):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    # factbook-shaped: (id, data VARIANT) built in DuckDB, bridged to Arrow.
    con = duckdb_connect()
    try:
        con.execute("CREATE TABLE facts (region VARCHAR, data VARIANT)")
        con.executemany(
            "INSERT INTO facts VALUES (?, CAST(CAST(? AS JSON) AS VARIANT))",
            [("AFRICA", '{"gdp": 1}'), ("EUROPE", '{"pop": [1, 2]}')],
        )
        table = duckdb_variant.to_canonical_arrow(con, "facts")
    finally:
        con.close()

    slug = "exporters-variant"
    canonical_path = write_canonical(slug, table)
    assert canonical_path == prepared_arrow(slug)

    # The VARIANT column survives into the canonical Arrow artifact.
    with pa.ipc.open_file(str(canonical_path)) as reader:
        got = reader.schema
    assert discovery._is_variant_field(got.field("data")) is True
    assert discovery._is_variant_field(got.field("region")) is False

    result = exporter().export({"slug": slug}, canonical_path)
    assert result.format_id == exporter.cell_id
    assert result.compliance.roundtrip is True  # read back and compared
    # pyarrow can't emit a Parquet VARIANT logical type — honest ledger row.
    assert result.compliance.variant_faithful is False
    assert result.compliance.note
    assert result.out_path.exists()
    from raincloud.pipeline.export import get_reader
    verdict = get_reader(exporter.cell_id).read_conformance(result.out_path, canonical_path)
    assert verdict.status == "pass", verdict
    # Export must leave the canonical annotation intact.
    with pa.ipc.open_file(str(canonical_path)) as reader:
        assert discovery._is_variant_field(reader.schema.field("data"))


def test_slug_from_canonical_inverts_output_format_dir(tmp_path, monkeypatch):
    """slug_from_canonical recovers the slug from a canonical path built by the
    real path helper — the inverse of output_format_dir."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    assert slug_from_canonical(prepared_arrow("some-slug")) == "some-slug"


def test_exporters_place_artifacts_by_canonical_slug(tmp_path, monkeypatch):
    """The exporters derive the slug from the canonical PATH, not
    spec["slug"]. A canonical written for `canon-slug` must land its parquet +
    vortex under `canon-slug` even when the spec carries `different-slug`."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))

    table = pa.table({"n": pa.array([1, 2, 3], type=pa.int32()), "s": ["a", "b", "c"]})
    canonical_path = write_canonical("canon-slug", table)

    results = run_exporters({"slug": "different-slug"}, canonical_path)
    assert {r.format_id for r in results} == {"parquet@py", "vortex@py"}

    # Artifacts land under the canonical's slug ...
    assert prepared_parquet("canon-slug").exists()
    assert prepared_vortex("canon-slug").exists()
    # ... NOT under spec["slug"].
    assert not prepared_parquet("different-slug").exists()
    assert not prepared_vortex("different-slug").exists()

    # And the exporter results point at the canonical-slug paths.
    for r in results:
        assert "canon-slug" in str(r.out_path)


def test_vortex_exporter_handles_large_single_stored_batch(tmp_path, monkeypatch):
    """A canonical whose single stored batch is large (3000 rows) round-trips
    through the Vortex exporter. The exporter feeds the canonical's own stored
    batches directly (no re-batch — a zero-copy slice would not bound the buffer
    Vortex sees, and no current slug flushes a batch that nears the i32 ceiling;
    see the VortexExporter docstring). This guards the multi-batch feed path."""
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    import vortex

    slug = "vortex-large-batch"
    n = 3000  # one stored batch, well over any per-slice heuristic
    table = pa.table(
        {
            "id": pa.array(range(n), type=pa.int64()),
            "s": pa.array([f"row-{i}" for i in range(n)]),
        }
    )
    # write_canonical stores this as a single record batch (num_rows=3000).
    canonical_path = write_canonical(slug, table)
    with pa.ipc.open_file(str(canonical_path)) as reader:
        assert reader.num_record_batches == 1
        assert reader.get_batch(0).num_rows == n

    result = VortexExporter().export({"slug": slug}, canonical_path)
    assert result.format_id == "vortex@py"
    assert result.compliance.roundtrip is True  # read back and compared

    vortex_path = prepared_vortex(slug)
    assert vortex_path.exists()
    vt = vortex.open(str(vortex_path)).to_arrow().read_all()
    assert vt.num_rows == n
    assert vt.column_names == ["id", "s"]
    assert vt.column("id").to_pylist() == list(range(n))


def test_overlapping_parquet_exports_keep_their_own_temporary_files(tmp_path, monkeypatch):
    import pyarrow.parquet as pq

    from raincloud.pipeline.canonical import write_canonical
    from raincloud.pipeline.export.exporters import ParquetExporter

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path))
    table = pa.table({"x": [1, 2]})
    canonical = write_canonical({}, [("overlap", table)])[0]
    exporter = ParquetExporter()
    original = pq.ParquetWriter
    nested = False

    class InterleavedWriter:
        def __init__(self, *args, **kwargs):
            self.writer = original(*args, **kwargs)

        def __enter__(self):
            self.writer.__enter__()
            return self

        def __exit__(self, *args):
            return self.writer.__exit__(*args)

        # The exporter accumulates batches and emits a row group with
        # `write_table`; re-enter from there, which is the point in the write
        # where a second export could collide on a shared temp name.
        def write_table(self, *args, **kwargs):
            nonlocal nested
            self.writer.write_table(*args, **kwargs)
            if not nested:
                nested = True
                other = exporter.export({}, canonical)
                assert pq.read_table(other.out_path).equals(table)

    monkeypatch.setattr(pq, "ParquetWriter", InterleavedWriter)
    result = exporter.export({}, canonical)
    assert pq.read_table(result.out_path).equals(table)
    assert not list(result.out_path.parent.glob("*.tmp"))


def _canonical_from_batches(slug, batches, schema):
    """Write a canonical whose STORED batch count we control."""
    from raincloud.pipeline.canonical import open_canonical_writer

    with open_canonical_writer(slug, schema) as writer:
        for b in batches:
            writer.write_batch(b)
    return prepared_arrow(slug)


def test_parquet_row_groups_follow_the_declared_size_not_the_batch_size(tmp_path, monkeypatch):
    """Row-group size must come from `write.row_group_size_rows`, not batching.

    `ParquetWriter.write_batch` opens a NEW row group per call, so writing one
    canonical batch per call made the row-group size whatever the canonical
    happened to carry — `BatchLimits.rows`, which is 4096 and exists to bound
    INGESTION memory. Every published Parquet inherited it: TPC-H SF100 lineitem
    was 146,494 row groups of 4,096 rows while its spec asked for 1,048,576.

    Small numbers here, same shape: 40 stored batches of 100 rows, asking for
    1,000 rows per group, must produce 4 row groups and not 40.
    """
    import pyarrow.parquet as pq

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    slug = "rowgroup-probe"
    schema = pa.schema([("x", pa.int64())])
    batches = [
        pa.record_batch([pa.array(range(i * 100, (i + 1) * 100), type=pa.int64())], schema=schema)
        for i in range(40)
    ]
    canonical_path = _canonical_from_batches(slug, batches, schema)
    with pa.ipc.open_file(str(canonical_path)) as reader:
        assert reader.num_record_batches == 40, "fixture must store 40 separate batches"

    ParquetExporter().export(
        {"slug": slug, "write": {"row_group_size_rows": 1000}}, canonical_path
    )
    meta = pq.ParquetFile(prepared_parquet(slug)).metadata
    assert meta.num_rows == 4000
    assert meta.num_row_groups == 4, (
        f"expected 4 row groups of 1000, got {meta.num_row_groups} — "
        "the writer is following the canonical's batching again"
    )
    assert {meta.row_group(i).num_rows for i in range(4)} == {1000}


def test_parquet_row_group_accumulation_is_bounded_by_bytes(tmp_path, monkeypatch):
    """A wide table must not buffer to the row target before flushing.

    `row_group_size_rows` is stated in rows, which is the right unit for the
    artifact but not a bound on memory: a slug at ~13 KB/row would buffer
    gigabytes on the way to 1M rows. The byte ceiling flushes first, trading
    fewer rows per group for a bounded working set.
    """
    import pyarrow.parquet as pq

    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    monkeypatch.setenv("RAINCLOUD_ROW_GROUP_TARGET_BYTES", str(64 * 1024))
    slug = "rowgroup-wide-probe"
    schema = pa.schema([("blob", pa.binary())])
    # 10 batches x 10 rows x 1 KiB = ~100 KiB total, over the 64 KiB ceiling.
    batches = [
        pa.record_batch([pa.array([b"x" * 1024] * 10, type=pa.binary())], schema=schema)
        for _ in range(10)
    ]
    canonical_path = _canonical_from_batches(slug, batches, schema)

    ParquetExporter().export(
        {"slug": slug, "write": {"row_group_size_rows": 1_000_000}}, canonical_path
    )
    meta = pq.ParquetFile(prepared_parquet(slug)).metadata
    assert meta.num_rows == 100
    assert meta.num_row_groups > 1, (
        "the byte ceiling never fired: everything accumulated into one group"
    )
    assert max(meta.row_group(i).num_rows for i in range(meta.num_row_groups)) < 100


def test_vortex_java_pure_is_not_a_registered_reader():
    from raincloud.pipeline.export.readers import all_readers
    ids = {reader.reader_id for reader in all_readers()}
    assert "vortex@java-pure" not in ids
    assert {"parquet@hardwood", "vortex@jni"} <= ids
