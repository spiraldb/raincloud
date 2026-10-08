# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""parquet@py cuts row groups at exactly the planned row, and the byte ceiling closes
one early only at a batch or slice boundary."""
import pyarrow as pa
import pyarrow.parquet as pq

from raincloud.pipeline.export import exporters as exporters_mod
from raincloud.pipeline.spec import ParquetOptions
from tests._helpers import write_ipc


def _canonical(tmp_path, batches):
    return write_ipc(tmp_path / "c.arrow.zstd", batches, compression=None)


def _ints(start, stop):
    return pa.record_batch([pa.array(range(start, stop), pa.int64())], names=["x"])


def _cuts(canonical, row_group, byte_target):
    with pa.ipc.open_file(str(canonical)) as reader:
        return [(sum(b.num_rows for b in group), by_bytes)
                for group, by_bytes in exporters_mod._groups(reader, row_group, byte_target)]


def test_parquet_py_cuts_groups_at_exactly_the_planned_row_across_batches(tmp_path):
    canonical = _canonical(tmp_path, [_ints(i, min(i + 700, 4500)) for i in range(0, 4500, 700)])
    out = tmp_path / "out.parquet"
    assert exporters_mod._write_parquet(canonical, out, 1000, 1 << 40, options=ParquetOptions()) is False
    meta = pq.ParquetFile(out).metadata
    assert [meta.row_group(i).num_rows for i in range(meta.num_row_groups)] == [1000] * 4 + [500]
    assert pq.read_table(out).column("x").to_pylist() == list(range(4500))


def test_the_byte_ceiling_still_closes_a_group_early_at_a_batch_or_slice_boundary(tmp_path):
    # 700-row int64 batches are 5,600 decoded bytes.
    canonical = _canonical(tmp_path, [_ints(i, i + 700) for i in range(0, 2800, 700)])
    # 6,000 bytes: the ceiling closes each batch before the row cut would slice it.
    assert _cuts(canonical, 1000, 6000) == [(700, True)] * 3 + [(700, None)]
    # 9,000 bytes: rows close first, slicing batches and carrying the remainder.
    assert _cuts(canonical, 1000, 9000) == [(1000, False), (1000, False), (800, None)]
    # A group that starts at a carried remainder slice closes on bytes at the
    # next batch: 300 rows (2,400 bytes) + 700 rows (5,600) exceed 6,000.
    (tmp_path / "c.arrow.zstd").unlink()
    canonical = _canonical(tmp_path, [_ints(0, 1300), _ints(1300, 2000)])
    assert _cuts(canonical, 1000, 6000) == [(1000, False), (300, True), (700, None)]


def test_empty_batches_never_make_an_empty_group_beside_real_ones(tmp_path):
    empty = _ints(0, 0)
    assert _cuts(_canonical(tmp_path, [empty]), 10, 1 << 40) == [(0, None)]
    (tmp_path / "c.arrow.zstd").unlink()
    assert _cuts(_canonical(tmp_path, [_ints(0, 10), empty]), 10, 1 << 40) == [(10, False)]


def test_the_probe_encodes_exactly_the_first_planned_group(tmp_path):
    canonical = _canonical(tmp_path, [_ints(i, i + 700) for i in range(0, 2800, 700)])
    rows, encoded = exporters_mod._probe_encoded(canonical, 1000, tmp_path / "probe.parquet",
                                                 options=ParquetOptions())
    assert rows == 1000 and encoded > 0
    assert not (tmp_path / "probe.parquet").exists()
