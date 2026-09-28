# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""vortex@py on code-contests' shape: wide nested string rows.

code-contests keeps each problem's programs as `struct<language: list<int64>,
solution: list<string>>`, a few hundred KiB of source per row. Vortex regroups
the canonical's stored batches into 8,192-row blocks, so the stored batching
never bounds what it encodes (see the `VortexExporter` docstring). The real
failure needs a >4 GiB block and is measured on the real canonical, not here;
this guards the exporter's feed of that shape across several stored batches
and compares the self-read to the canonical with raincloud's own comparator.

Hermetic: `RAINCLOUD_HOME` points at a tmp dir, as in test_exporters.py.
"""
from __future__ import annotations

import pyarrow as pa

from raincloud.pipeline import canonical
from raincloud.pipeline.export.compare import values_equal
from raincloud.pipeline.export.exporters import VortexExporter

_PROGRAMS = pa.struct([
    ("language", pa.list_(pa.int64())),
    ("solution", pa.list_(pa.string())),
])


def _problems(start: int, rows: int) -> pa.RecordBatch:
    """`rows` problems, each with 8 distinct ~16 KiB programs."""
    programs = []
    for i in range(start, start + rows):
        solutions = [f"// problem {i} program {j}\n" + "x = x + 1;\n" * 1500 for j in range(8)]
        programs.append({"language": [j % 4 for j in range(8)], "solution": solutions})
    return pa.RecordBatch.from_arrays(
        [pa.array(range(start, start + rows), type=pa.int64()), pa.array(programs, type=_PROGRAMS)],
        names=["id", "solutions"],
    )


def test_vortex_exporter_round_trips_wide_nested_string_rows(tmp_path, monkeypatch):
    monkeypatch.setenv("RAINCLOUD_HOME", str(tmp_path / "home"))
    import vortex

    batches = [_problems(0, 20), _problems(20, 20), _problems(40, 7)]
    with canonical.open_canonical_writer("wide-nested-rows", batches[0].schema) as writer:
        for batch in batches:
            writer.write_batch(batch)
    canonical_path = writer.dest
    with pa.ipc.open_file(str(canonical_path)) as reader:
        assert reader.num_record_batches == 3
        expected = reader.read_all()

    result = VortexExporter().export({"slug": "wide-nested-rows"}, canonical_path)
    assert result.format_id == "vortex@py"
    assert result.nbytes > 0

    got = vortex.open(str(result.out_path)).to_arrow().read_all()
    equal, detail = values_equal(got, expected)
    assert equal, detail
