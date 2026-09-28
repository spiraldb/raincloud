# SPDX-FileCopyrightText: 2026 Raincloud Maintainers
# SPDX-License-Identifier: Apache-2.0
"""Faithful schemas: uci_diabetes_parse and xlsx_parse keep every upstream value."""
from __future__ import annotations

import datetime
import io
import sys
import tarfile
import zipfile

import pyarrow as pa
import pytest

# --------------------------------------------------------------------------
# uci_diabetes_parse: value is float64 when numeric, value_raw is the text
# --------------------------------------------------------------------------

def _diabetes(tmp_path, monkeypatch, text: str):
    tar = io.BytesIO()
    with tarfile.open(fileobj=tar, mode="w") as t:
        data = text.encode()
        info = tarfile.TarInfo("Diabetes-Data/data-02")
        info.size = len(data)
        t.addfile(info, io.BytesIO(data))
    fake = type(sys)("unlzw3")
    fake.unlzw = lambda _: tar.getvalue()
    monkeypatch.setitem(sys.modules, "unlzw3", fake)
    path = tmp_path / "diabetes.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("diabetes-data.tar.Z", b"z")
    return path


def test_diabetes_values_keep_doses_flags_and_text(tmp_path, monkeypatch):
    from raincloud.pipeline.handlers.uci_diabetes_parse import uci_diabetes_parse

    raw = ["4.5", "0Hi", "0Lo", "0''", "009", "", "3A", "150"]
    text = "".join(f"03-09-1989\t08:00\t33\t{v}\n" for v in raw)
    [(_, table)] = uci_diabetes_parse({"slug": "d"}, [(_diabetes(tmp_path, monkeypatch, text), None)])
    assert table.schema.field("value").type == pa.float64()
    assert table.schema.field("value_raw").type == pa.string()
    assert table.column("value").to_pylist() == [4.5, None, None, None, 9.0, None, None, 150.0]
    assert table.column("value_raw").to_pylist() == raw


@pytest.mark.parametrize("code", ["033", "3x", "", "256", " 33"])
def test_diabetes_code_that_is_not_a_plain_uint8_fails(tmp_path, monkeypatch, code):
    from raincloud.pipeline.handlers.uci_diabetes_parse import uci_diabetes_parse

    path = _diabetes(tmp_path, monkeypatch, f"03-09-1989\t08:00\t58\t100\n03-09-1989\t08:00\t{code}\t1\n")
    with pytest.raises(ValueError, match=r"data-02 line 2: code .* is not a uint8 written plainly"):
        uci_diabetes_parse({"slug": "d"}, [(path, None)])


def test_diabetes_value_that_does_not_round_trip_as_float_fails(tmp_path, monkeypatch):
    from raincloud.pipeline.handlers.uci_diabetes_parse import uci_diabetes_parse

    path = _diabetes(tmp_path, monkeypatch, "03-09-1989\t08:00\t58\t12345678901234567890.5\n")
    with pytest.raises(ValueError, match=r"does not round-trip through float64"):
        uci_diabetes_parse({"slug": "d"}, [(path, None)])


# --------------------------------------------------------------------------
# xlsx_parse: per-column types inferred from every cell of every sheet
# --------------------------------------------------------------------------

_HEADER = ["Invoice", "Quantity", "When", "Price", "Customer ID", "Note"]


def _xlsx(tmp_path, sheets: dict[str, list[list]]):
    import openpyxl

    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    for name, rows in sheets.items():
        sheet = workbook.create_sheet(name)
        for row in rows:
            sheet.append(row)
    path = tmp_path / "book.xlsx"
    workbook.save(path)
    return path


def _parse(path, **params):
    from raincloud.pipeline.handlers.xlsx_parse import xlsx_parse

    [(_, table)] = xlsx_parse({"slug": "x"}, [(path, None)], **params)
    return table


def test_xlsx_columns_take_the_narrowest_faithful_type_across_sheets(tmp_path):
    when = datetime.datetime(2009, 12, 1, 7, 45)
    path = _xlsx(tmp_path, {
        "A": [_HEADER, [489449, 12, when, 2, 13085, "x"], ["C489449", -3, when, 6.95, None, 21494]],
        "B": [_HEADER, [536365, 6, when, 2.5499999999999998, 17850, None]],
    })
    table = _parse(path)
    assert table.schema == pa.schema([
        ("sheet_name", pa.string()), ("Invoice", pa.string()), ("Quantity", pa.int64()),
        ("When", pa.timestamp("us")), ("Price", pa.float64()), ("Customer ID", pa.int64()),
        ("Note", pa.string())])
    assert table.to_pydict() == {
        "sheet_name": ["A", "A", "B"],
        "Invoice": ["489449", "C489449", "536365"],
        "Quantity": [12, -3, 6],
        "When": [when] * 3,
        "Price": [2.0, 6.95, 2.55],
        "Customer ID": [13085, None, 17850],
        "Note": ["x", "21494", None],
    }


def test_xlsx_integer_float64_cannot_hold_fails_beside_fractions():
    # openpyxl writes such an integer as float text, so another writer's
    # `<v>1152921504606846977</v>` is exercised at the column step directly.
    from raincloud.pipeline.handlers.xlsx_parse import _column

    with pytest.raises(ValueError, match=r"the integer 1152921504606846977, which float64 cannot"):
        _column("k", [0.5, 2 ** 60 + 1], {"float": "'A'!A2", "int": "'A'!A3"})


def test_xlsx_sheet_param_restricts_and_names_missing_sheets(tmp_path):
    path = _xlsx(tmp_path, {"A": [["k"], [1]], "B": [["k"], ["b"]]})
    assert _parse(path, sheet="A").to_pydict() == {"sheet_name": ["A"], "k": [1]}
    with pytest.raises(ValueError, match=r"has no sheet 'C' \(sheets: \['A', 'B'\]\)"):
        _parse(path, sheet="C")


def test_xlsx_trailing_empty_rows_are_not_records(tmp_path):
    path = _xlsx(tmp_path, {"A": [["k"], [1], [None], [None]]})
    assert _parse(path).column("k").to_pylist() == [1]


@pytest.mark.parametrize("sheets,message", [
    ({"A": [["k"], [datetime.datetime(2020, 1, 1)], [3]]},
     r"column 'k' mixes cell kinds with no single faithful type \(datetime at 'A'!A2, int at 'A'!A3\)"),
    ({"A": [["k"], [True], ["yes"]]}, r"column 'k' mixes cell kinds"),
    ({"A": [["k"], ["=1+1"]]}, r"cell A2 is a formula \('=1\+1'\)"),
    ({"A": [["k"], [1], [None], [2]]}, r"row 3 is empty between data rows"),
    ({"A": [["k"], [1, 2]]}, r"row 2 has cells beyond the 1-column header"),
    ({"A": [["k"], [1]], "B": [["j"], [1]]}, r"\['B'\] header \['j'\] differs"),
    ({"A": [["k", "k"], [1, 2]]}, r"header row must be unique non-empty text"),
    ({"A": [["sheet_name"], [1]]}, r"other than 'sheet_name'"),
])
def test_xlsx_cell_without_a_faithful_place_fails(tmp_path, sheets, message):
    with pytest.raises(ValueError, match=message):
        _parse(_xlsx(tmp_path, sheets))
