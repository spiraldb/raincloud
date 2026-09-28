# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Nullable containers with Vortex view leaves must remain comparable."""
import pyarrow as pa
import pytest

from raincloud.pipeline.export.compare import values_equal


@pytest.mark.parametrize('binary', [False, True], ids=['oasst-struct-list', 'mmmu-image-struct'])
def test_nullable_view_containers(binary, tmp_path):
    vortex = pytest.importorskip('vortex')
    if binary:
        dtype = pa.struct([('bytes', pa.binary()), ('path', pa.string())])
        values = [{'bytes': b'\xff' * 32, 'path': 'image'}, None,
                  {'bytes': None, 'path': None}, {'bytes': b'other', 'path': 'second'}]
        changed = dict(values[-1], bytes=b'wrong')
    else:
        dtype = pa.struct([('name', pa.list_(pa.string())), ('count', pa.list_(pa.int32()))])
        values = [{'name': ['hello', None], 'count': [1, 2]}, None,
                  {'name': [], 'count': []}, {'name': ['bye'], 'count': [3]}]
        changed = dict(values[-1], name=['wrong'])
    expected = pa.table({'x': pa.array(values, type=dtype)})
    path = tmp_path / 'fixture.vortex'
    vortex.io.write(expected, str(path))
    with vortex.open(str(path)).to_arrow() as reader:
        got = reader.read_all()
    assert 'view' in str(got.schema)
    assert values_equal(got, expected) == (True, '')
    assert values_equal(expected, got) == (True, '')
    assert values_equal(got.slice(1), expected.slice(1)) == (True, '')
    corrupted = pa.table({'x': pa.array(values[:-1] + [changed], type=dtype)})
    assert not values_equal(got, corrupted)[0]
    changed_null = pa.table({'x': pa.array([values[0], values[0], *values[2:]], type=dtype)})
    assert not values_equal(got, changed_null)[0]


@pytest.mark.parametrize('view,plain,values', [
    (pa.string_view(), pa.string(), ['first long dictionary string', 'second']),
    (pa.binary_view(), pa.binary(), [b'\xff' * 32, b'\x00']),
])
def test_dictionary_view_values(view, plain, values):
    dictionary = pa.DictionaryArray.from_arrays(pa.array([0, None, 1, 0], pa.int16()),
                                                pa.array(values, type=view))
    expected = pa.table({'x': pa.array([values[0], None, values[1], values[0]], type=plain)})
    got = pa.table({'x': dictionary})
    assert values_equal(got, expected) == (True, '')
    assert values_equal(expected, got) == (True, '')
    assert not values_equal(got, expected.take(pa.array([2, 1, 0, 3])))[0]
