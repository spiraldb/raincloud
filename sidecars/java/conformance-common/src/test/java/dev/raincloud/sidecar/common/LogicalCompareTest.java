// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider.MapDictionaryProvider;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

/**
 * Comparator fixtures: binary content equality, float NaN/signed-zero and half
 * decoding, exact unsigned integers, timestamp units and timezones, nested shapes,
 * comparator gaps, and batch-window streaming. A real dataset such as uci-iris
 * alone only exercises FLOAT + STRING.
 */
class LogicalCompareTest {
    private static final String ID = "test";

    private static Field f(String name, ArrowType t) {
        return new Field(name, FieldType.nullable(t), null);
    }

    private static MaterializedTable table(List<Field> fields, List<List<Object>> cols) {
        return new MaterializedTable(fields, cols);
    }

    @SafeVarargs
    private static List<Object> col(Object... vals) {
        return new ArrayList<>(Arrays.asList(vals));
    }

    @Test
    void fixedDimensions_checkedBeforeValuesAndRecursively() {
        for (boolean binary : List.of(false, true)) {
            ArrowType one = binary ? new ArrowType.FixedSizeBinary(1) : new ArrowType.FixedSizeList(1);
            ArrowType two = binary ? new ArrowType.FixedSizeBinary(2) : new ArrowType.FixedSizeList(2);
            List<Field> children = binary ? List.of() : List.of(f("item", new ArrowType.Int(64, true)));
            Field expected = new Field("x", FieldType.nullable(one), children);
            Field got = new Field("x", FieldType.nullable(two), children);
            for (int nesting = 0; nesting < 3; nesting++) {
                for (List<Object> values : List.of(col(), col((Object) null))) {
                    MaterializedTable e = table(List.of(expected), List.of(values));
                    MaterializedTable g = table(List.of(got), List.of(values));
                    assertEquals("fail", LogicalCompare.compare(ID, g, e).status,
                            "must retain dimensions under empty/null parents: " + expected);
                    assertEquals("pass", LogicalCompare.compare(ID, e, e).status);
                }
                ArrowType parent = nesting == 0 ? new ArrowType.Struct() : new ArrowType.List();
                expected = new Field("x", FieldType.nullable(parent), List.of(expected));
                got = new Field("x", FieldType.nullable(parent), List.of(got));
            }
        }
    }

    @Test
    void fixedAndVariableRepresentations_preserveLosslessValues() {
        for (boolean binary : List.of(false, true)) {
            ArrowType fixed = binary ? new ArrowType.FixedSizeBinary(1) : new ArrowType.FixedSizeList(1);
            ArrowType variable = binary ? new ArrowType.Binary() : new ArrowType.List();
            List<Field> children = binary ? List.of() : List.of(f("item", new ArrowType.Int(64, true)));
            Field ef = new Field("x", FieldType.nullable(fixed), children);
            Field gf = new Field("x", FieldType.nullable(variable), children);
            Object value = binary ? new byte[] {1} : List.of(1L);
            Object other = binary ? new byte[] {1, 2} : List.of(1L, 2L);
            for (List<Object> values : List.of(col(), col((Object) null), col(value, null))) {
                MaterializedTable e = table(List.of(ef), List.of(values));
                MaterializedTable g = table(List.of(gf), List.of(values));
                assertEquals("pass", LogicalCompare.compare(ID, g, e).status);
                assertEquals("pass", LogicalCompare.compare(ID, e, g).status);
            }
            assertEquals("fail", LogicalCompare.compare(ID,
                    table(List.of(gf), List.of(col(other))),
                    table(List.of(ef), List.of(col(value)))).status);
        }
    }

    // ---- passes ----------------------------------------------------------

    @Test
    void identicalDoublesAndStrings_pass() {
        List<Field> fs = List.of(
                f("x", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                f("s", new ArrowType.Utf8()));
        MaterializedTable a = table(fs, List.of(col(1.5, 2.5), col(new Text("a"), new Text("b"))));
        MaterializedTable b = table(fs, List.of(col(1.5, 2.5), col(new Text("a"), new Text("b"))));
        Verdict v = LogicalCompare.compare(ID, a, b);
        assertEquals("pass", v.status, v.detail);
    }

    @Test
    void stringVsStringView_pass() {
        // Same logical strings, canonical Utf8 vs got Utf8View — both getObject -> Text.
        MaterializedTable expected = table(
                List.of(f("s", new ArrowType.Utf8())), List.of(col(new Text("hello"), new Text("x"))));
        MaterializedTable got = table(
                List.of(f("s", new ArrowType.Utf8View())), List.of(col(new Text("hello"), new Text("x"))));
        assertEquals("pass", LogicalCompare.compare(ID, got, expected).status);
    }

    @Test
    void int32VsInt64_pass() {
        // Widening across integer widths is lossless — compared as exact BigIntegers.
        MaterializedTable expected = table(
                List.of(f("n", new ArrowType.Int(64, true))), List.of(col(1L, 2L, 3L)));
        MaterializedTable got = table(
                List.of(f("n", new ArrowType.Int(32, true))), List.of(col(1, 2, 3)));
        assertEquals("pass", LogicalCompare.compare(ID, got, expected).status);
    }

    @Test
    void unsignedUint8AsByte_vs_int32_pass() {
        // arrow-java boxes uint8 as a signed Byte holding the raw bits: 162 -> (byte) -94.
        // The unsigned-aware normalization must recover 162 to match a signed-carrier readback.
        MaterializedTable expected = table(
                List.of(f("m", new ArrowType.Int(8, false))), List.of(col((byte) -94)));
        MaterializedTable got = table(
                List.of(f("m", new ArrowType.Int(32, true))), List.of(col(162)));
        assertEquals("pass", LogicalCompare.compare(ID, got, expected).status, "uint8 162 vs int32 162");
    }

    @Test
    void unsignedUint16AsCharacter_vs_int32_pass() {
        // arrow-java UInt2Vector.getObject boxes uint16 as Character — must not ClassCast.
        MaterializedTable expected = table(
                List.of(f("p", new ArrowType.Int(16, false))), List.of(col((Object) (char) 1680)));
        MaterializedTable got = table(
                List.of(f("p", new ArrowType.Int(32, true))), List.of(col(1680)));
        assertEquals("pass", LogicalCompare.compare(ID, got, expected).status, "uint16 1680 vs int32 1680");
    }

    @Test
    void uint64Max_vs_int64MinusOne_fail() {
        // THE ALIASING CASE: arrow-java boxes uint64 as a Long holding raw bits, so
        // uint64 18446744073709551615 arrives as -1L. Normalizing through a signed
        // long made it compare EQUAL to a signed int64 of -1 — a false pass on
        // completely different data. The exact (BigInteger) carrier must reject it.
        MaterializedTable expected = table(
                List.of(f("u", new ArrowType.Int(64, false))), List.of(col(-1L)));
        MaterializedTable got = table(
                List.of(f("u", new ArrowType.Int(64, true))), List.of(col(-1L)));
        assertEquals("fail", LogicalCompare.compare(ID, got, expected).status,
                "uint64 max must NOT equal int64 -1");
    }

    @Test
    void uint64Max_vs_uint64Max_pass() {
        // Same-type uint64 max still compares equal (the fix must not over-reject).
        MaterializedTable expected = table(
                List.of(f("u", new ArrowType.Int(64, false))), List.of(col(-1L)));
        MaterializedTable got = table(
                List.of(f("u", new ArrowType.Int(64, false))), List.of(col(-1L)));
        assertEquals("pass", LogicalCompare.compare(ID, got, expected).status);
    }

    @Test
    void uint64LargeValue_vs_int32_fail() {
        // A uint64 above Long.MAX_VALUE against a small signed value: distinct.
        MaterializedTable expected = table(
                List.of(f("u", new ArrowType.Int(64, false))),
                List.of(col(Long.MIN_VALUE)));  // raw bits = 2^63 unsigned
        MaterializedTable got = table(
                List.of(f("u", new ArrowType.Int(32, true))), List.of(col(1)));
        assertEquals("fail", LogicalCompare.compare(ID, got, expected).status);
    }

    @Test
    void intAsExact_recoversUnsignedValuesAtEveryWidth() {
        // Unit-level: the exact carrier is unsigned-aware at 8/16/32/64.
        assertEquals(java.math.BigInteger.valueOf(162),
                ArrowValues.intAsExact(new ArrowType.Int(8, false), (byte) -94));
        assertEquals(java.math.BigInteger.valueOf(1680),
                ArrowValues.intAsExact(new ArrowType.Int(16, false), (char) 1680));
        assertEquals(java.math.BigInteger.valueOf(4294967295L),
                ArrowValues.intAsExact(new ArrowType.Int(32, false), -1));
        assertEquals(new java.math.BigInteger("18446744073709551615"),
                ArrowValues.intAsExact(new ArrowType.Int(64, false), -1L));
        // Signed stays signed.
        assertEquals(java.math.BigInteger.valueOf(-1L),
                ArrowValues.intAsExact(new ArrowType.Int(64, true), -1L));
    }

    @Test
    void binaryEqual_pass() {
        // byte[] must compare by content (Arrays.equals), not identity.
        MaterializedTable a = table(List.of(f("b", new ArrowType.Binary())),
                List.of(col((Object) new byte[] {1, 2, 3})));
        MaterializedTable b = table(List.of(f("b", new ArrowType.Binary())),
                List.of(col((Object) new byte[] {1, 2, 3})));
        assertEquals("pass", LogicalCompare.compare(ID, a, b).status);
    }

    @Test
    void variantShreddedStruct_pass_withNote() {
        // struct<metadata: binary, value: binary> surfaces as a Map with byte[] children —
        // deep-compare must not fall to container .equals (which would byte[].equals -> fail).
        Map<String, String> variantMd = Map.of("__variant_type", "json");
        Field structField = new Field("v",
                new FieldType(true, new ArrowType.Struct(), null, variantMd),
                List.of(f("metadata", new ArrowType.Binary()), f("value", new ArrowType.Binary())));
        Map<String, Object> m1 = new LinkedHashMap<>();
        m1.put("metadata", new byte[] {0x01});
        m1.put("value", new byte[] {0x2a, 0x2b});
        Map<String, Object> m2 = new LinkedHashMap<>();
        m2.put("metadata", new byte[] {0x01});
        m2.put("value", new byte[] {0x2a, 0x2b});
        MaterializedTable a = table(List.of(structField), List.of(col((Object) m1)));
        MaterializedTable b = table(List.of(structField), List.of(col((Object) m2)));
        Verdict v = LogicalCompare.compare(ID, a, b);
        assertEquals("pass", v.status, v.detail);
        assertTrue(v.note.contains("VARIANT"), v.note);
    }

    @Test
    void structWithDuplicateChildNames_isSkippedNotPassed() {
        // Arrow permits two children with the same name, and a struct cell boxes
        // as a name-keyed Map -- so both children collapse to ONE entry and the
        // last one wins. struct<a,a> holding (1,2) and (9,2) both box to
        // {"a": 2}: the maps below are what the comparator actually receives,
        // and comparing them reported a clean `pass` for columns whose first
        // child differed. The comparator cannot see this shape, so it must say
        // so rather than report a pass it has not earned.
        Field dup = new Field("v", new FieldType(true, new ArrowType.Struct(), null, null),
                List.of(f("a", new ArrowType.Int(32, true)), f("a", new ArrowType.Int(32, true))));
        Map<String, Object> m1 = new LinkedHashMap<>();
        m1.put("a", 2);
        Map<String, Object> m2 = new LinkedHashMap<>();
        m2.put("a", 2);
        MaterializedTable a = table(List.of(dup), List.of(col((Object) m1)));
        MaterializedTable b = table(List.of(dup), List.of(col((Object) m2)));
        Verdict v = LogicalCompare.compare(ID, a, b);
        assertEquals("skip", v.status, v.detail);
    }

    @Test
    void nestedUnsignedComparison_usesChildFields() {
        for (boolean struct : List.of(false, true)) {
            ArrowType outer = struct ? new ArrowType.Struct() : new ArrowType.List();
            Field unsigned = new Field("x", FieldType.nullable(outer),
                    List.of(f("item", new ArrowType.Int(64, false))));
            Field signed = new Field("x", FieldType.nullable(outer),
                    List.of(f("item", new ArrowType.Int(64, true))));
            Object value = struct ? Map.of("item", -1L) : List.of(-1L);
            MaterializedTable expected = table(List.of(unsigned), List.of(col(value)));
            MaterializedTable got = table(List.of(signed), List.of(col(value)));
            assertEquals("fail", LogicalCompare.compare(ID, got, expected).status);
            assertEquals("pass", LogicalCompare.compare(ID, expected, expected).status);
        }
    }

    @Test
    void nestedComparatorGap_isNotAFalsePass() {
        Field days = new Field("x", FieldType.nullable(new ArrowType.List()),
                List.of(f("item", new ArrowType.Date(DateUnit.DAY))));
        Field millis = new Field("x", FieldType.nullable(new ArrowType.List()),
                List.of(f("item", new ArrowType.Date(DateUnit.MILLISECOND))));
        assertEquals("skip", LogicalCompare.compare(ID,
                table(List.of(days), List.of(col(List.of(1)))),
                table(List.of(millis), List.of(col(List.of(86_400_000L))))).status);
    }

    // ---- fails -----------------------------------------------------------

    @Test
    void minusZeroVsZero_fail() {
        // Rust compares float buffers bitwise: -0.0 != +0.0. Match that.
        MaterializedTable a = table(
                List.of(f("x", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))),
                List.of(col(-0.0)));
        MaterializedTable b = table(
                List.of(f("x", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE))),
                List.of(col(0.0)));
        assertEquals("fail", LogicalCompare.compare(ID, a, b).status);
    }

    @Test
    void binaryDiffer_fail() {
        MaterializedTable a = table(List.of(f("b", new ArrowType.Binary())),
                List.of(col((Object) new byte[] {1, 2, 3})));
        MaterializedTable b = table(List.of(f("b", new ArrowType.Binary())),
                List.of(col((Object) new byte[] {1, 2, 4})));
        assertEquals("fail", LogicalCompare.compare(ID, a, b).status);
    }

    @Test
    void nullVsValue_fail() {
        MaterializedTable a = table(List.of(f("s", new ArrowType.Utf8())),
                List.of(col(new Text("a"), null)));
        MaterializedTable b = table(List.of(f("s", new ArrowType.Utf8())),
                List.of(col(new Text("a"), new Text("b"))));
        assertEquals("fail", LogicalCompare.compare(ID, a, b).status);
    }

    @Test
    void rowCountMismatch_fail() {
        MaterializedTable a = table(List.of(f("n", new ArrowType.Int(64, true))), List.of(col(1L, 2L)));
        MaterializedTable b = table(List.of(f("n", new ArrowType.Int(64, true))), List.of(col(1L)));
        assertEquals("fail", LogicalCompare.compare(ID, a, b).status);
    }

    @Test
    void columnNamesDiffer_fail() {
        MaterializedTable a = table(List.of(f("a", new ArrowType.Int(64, true))), List.of(col(1L)));
        MaterializedTable b = table(List.of(f("b", new ArrowType.Int(64, true))), List.of(col(1L)));
        assertEquals("fail", LogicalCompare.compare(ID, a, b).status);
    }

    // ---- timestamps and comparator gaps ----------------------------------

    @Test
    void timestampSecondToMillis_sameInstant_pass() {
        // Parquet has no second-precision timestamp, so SECOND is promoted to MILLIS; the same
        // instant must compare EQUAL (by instant), not skip. 100 s == 100_000 ms.
        MaterializedTable expected = table(
                List.of(f("t", new ArrowType.Timestamp(TimeUnit.SECOND, null))),
                List.of(col(100L)));
        MaterializedTable got = table(
                List.of(f("t", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))),
                List.of(col(100_000L)));
        assertEquals("pass", LogicalCompare.compare(ID, got, expected).status);
    }

    @Test
    void timestampDifferingUnits_differingInstant_fail() {
        // 100 s != 100.001 s — an off-by-1ms real difference must still fail.
        MaterializedTable expected = table(
                List.of(f("t", new ArrowType.Timestamp(TimeUnit.SECOND, null))),
                List.of(col(100L)));
        MaterializedTable got = table(
                List.of(f("t", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))),
                List.of(col(100_001L)));
        assertEquals("fail", LogicalCompare.compare(ID, got, expected).status);
    }

    @Test
    void timestampDifferingTimezone_fail() {
        // A dropped or changed timezone changes what the value means: a mismatch, as in
        // the Python and Rust comparators, even when the stored numbers agree.
        for (List<Object> values : List.of(col(1_000L), col(), col((Object) null))) {
            MaterializedTable expected = table(
                    List.of(f("t", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))), List.of(values));
            MaterializedTable got = table(
                    List.of(f("t", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))), List.of(values));
            Verdict v = LogicalCompare.compare(ID, got, expected);
            assertEquals("fail", v.status, v.detail);
            assertTrue(v.note.contains("schema mismatch"), v.note);
        }
        Field utc = new Field("x", FieldType.nullable(new ArrowType.List()),
                List.of(f("item", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC"))));
        Field local = new Field("x", FieldType.nullable(new ArrowType.List()),
                List.of(f("item", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))));
        assertEquals("fail", LogicalCompare.compare(ID,
                table(List.of(utc), List.of(col(List.of(1000L)))),
                table(List.of(local), List.of(col(List.of(1000L))))).status);
    }

    @Test
    void timestampOverflow_distinctInstants_fail() {
        // Regression: a naive `value * 1_000_000_000L` wraps mod 2^64, so 2^55 seconds
        // (≈ 3.6e25 ns) collides with 0 ns and false-PASSES. Exact (BigInteger) nanos must fail it.
        // 2^55 s * 1e9 ≡ 0 (mod 2^64), yet the two instants are wildly different.
        MaterializedTable got = table(
                List.of(f("t", new ArrowType.Timestamp(TimeUnit.SECOND, null))),
                List.of(col(1L << 55)));
        MaterializedTable expected = table(
                List.of(f("t", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null))),
                List.of(col(0L)));
        assertEquals("fail", LogicalCompare.compare(ID, got, expected).status,
                "2^55 s must not overflow-collide with 0 ns");
    }

    @Test
    void emptyAndNullSchemas_areCheckedWithoutValues() {
        Field integer = f("x", new ArrowType.Int(64, true));
        Field string = f("x", new ArrowType.Utf8());
        Field struct = new Field("x", FieldType.nullable(new ArrowType.Struct()),
                List.of(f("a", new ArrowType.Int(64, true))));
        Field renamed = new Field("x", FieldType.nullable(new ArrowType.Struct()),
                List.of(f("b", new ArrowType.Int(64, true))));
        Field extra = new Field("x", FieldType.nullable(new ArrowType.Struct()),
                List.of(f("a", new ArrowType.Int(64, true)), f("b", new ArrowType.Int(64, true))));
        for (List<Field> pair : List.of(List.of(integer, string), List.of(struct, renamed),
                List.of(struct, extra), List.of(integer, struct))) {
            for (List<Object> values : List.of(col(), col((Object) null))) {
                assertEquals("fail", LogicalCompare.compare(ID,
                        table(List.of(pair.get(0)), List.of(values)),
                        table(List.of(pair.get(1)), List.of(values))).status);
            }
        }
        Field list = new Field("x", FieldType.nullable(new ArrowType.List()), List.of(struct));
        Field renamedList = new Field("x", FieldType.nullable(new ArrowType.List()), List.of(renamed));
        assertEquals("fail", LogicalCompare.compare(ID,
                table(List.of(list), List.of(col(List.of()))),
                table(List.of(renamedList), List.of(col(List.of())))).status);
    }

    @Test
    void emptySchemas_keepNormalizationAndGaps() {
        Field ints = new Field("x", FieldType.nullable(new ArrowType.List()),
                List.of(f("item", new ArrowType.Int(32, true))));
        Field wideInts = new Field("x", FieldType.nullable(new ArrowType.LargeList()),
                List.of(f("element", new ArrowType.Int(64, true))));
        for (List<Object> values : List.of(col(), col((Object) null), col(List.of()))) {
            assertEquals("pass", LogicalCompare.compare(ID,
                    table(List.of(ints), List.of(values)),
                    table(List.of(wideInts), List.of(values))).status);
        }
        Field days = f("x", new ArrowType.Date(DateUnit.DAY));
        Field millis = f("x", new ArrowType.Date(DateUnit.MILLISECOND));
        for (List<Object> values : List.of(col(), col((Object) null))) {
            assertEquals("skip", LogicalCompare.compare(ID,
                    table(List.of(days), List.of(values)),
                    table(List.of(millis), List.of(values))).status);
        }
    }

    @Test
    void schemaMismatch_winsOverSiblingGap() {
        Field expected = new Field("x", FieldType.nullable(new ArrowType.Struct()), List.of(
                f("gap", new ArrowType.Date(DateUnit.DAY)),
                f("a", new ArrowType.Int(64, true))));
        Field got = new Field("x", FieldType.nullable(new ArrowType.Struct()), List.of(
                f("gap", new ArrowType.Date(DateUnit.MILLISECOND)),
                f("renamed", new ArrowType.Int(64, true))));
        assertEquals("fail", LogicalCompare.compare(ID,
                table(List.of(got), List.of(col((Object) null))),
                table(List.of(expected), List.of(col((Object) null)))).status);
    }

    // ---- dictionary decoding (MaterializedTable.Builder) -----------------

    @Test
    void dictionaryColumnDecodesToValues() {
        // A dictionary-encoded canonical column must materialize to its VALUES (keeping the original
        // column name, dropping the dictionary encoding) so it compares equal to a plain-value lane.
        try (RootAllocator allocator = new RootAllocator();
                VarCharVector plain = new VarCharVector("color", allocator)) {
            // dictValues is owned by the dictionary and closed via provider.close() below.
            VarCharVector dictValues = new VarCharVector("dict", allocator);
            dictValues.allocateNew();
            dictValues.setSafe(0, "red".getBytes(StandardCharsets.UTF_8));
            dictValues.setSafe(1, "blue".getBytes(StandardCharsets.UTF_8));
            dictValues.setValueCount(2);
            Dictionary dictionary =
                    new Dictionary(dictValues, new DictionaryEncoding(1L, false, new ArrowType.Int(32, true)));
            plain.allocateNew();
            plain.setSafe(0, "red".getBytes(StandardCharsets.UTF_8));
            plain.setSafe(1, "blue".getBytes(StandardCharsets.UTF_8));
            plain.setValueCount(2);

            try (MapDictionaryProvider provider = new MapDictionaryProvider(dictionary);
                    FieldVector encoded = (FieldVector) DictionaryEncoder.encode(plain, dictionary);
                    VectorSchemaRoot root = new VectorSchemaRoot(List.of(encoded))) {
                root.setRowCount(2);
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.appendBatch(root, provider);
                MaterializedTable t = builder.build();

                assertEquals("color", t.fields.get(0).getName(), "original column name preserved");
                assertEquals(null, t.fields.get(0).getDictionary(), "dictionary encoding dropped");
                assertEquals("red", t.columns.get(0).get(0).toString());
                assertEquals("blue", t.columns.get(0).get(1).toString());
            }
        }
    }

    // ---- half floats -----------------------------------------------------

    private static final ArrowType HALF = new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
    private static final ArrowType SINGLE = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

    @Test
    void halfIsDecodedFromItsRawBits() {
        // Float2Vector.getObject boxes the raw IEEE-754 half bits as a Short: 0x3C00 is 1.0,
        // not 15360.0.
        MaterializedTable half = table(List.of(f("x", HALF)), List.of(col((short) 0x3C00)));
        assertEquals("pass", LogicalCompare.compare(ID, half,
                table(List.of(f("x", SINGLE)), List.of(col(1.0f)))).status);
        assertEquals("fail", LogicalCompare.compare(ID, half,
                table(List.of(f("x", SINGLE)), List.of(col(15360.0f)))).status);
        assertEquals("pass", LogicalCompare.compare(ID, half, half).status);
        assertEquals("fail", LogicalCompare.compare(ID, half,
                table(List.of(f("x", HALF)), List.of(col((short) 0x3C01)))).status);
    }

    @Test
    void halfToFloat_coversEveryClass() {
        assertEquals(0x0000_0000, Float.floatToRawIntBits(ArrowValues.halfToFloat((short) 0x0000)));
        assertEquals(0x8000_0000, Float.floatToRawIntBits(ArrowValues.halfToFloat((short) 0x8000)));
        assertEquals(0x1p-24f, ArrowValues.halfToFloat((short) 0x0001));        // smallest subnormal
        assertEquals(-0x3ffp-24f, ArrowValues.halfToFloat((short) 0x83FF));     // largest subnormal
        assertEquals(0x1p-14f, ArrowValues.halfToFloat((short) 0x0400));        // smallest normal
        assertEquals(65504f, ArrowValues.halfToFloat((short) 0x7BFF));          // largest normal
        assertEquals(-2.5f, ArrowValues.halfToFloat((short) 0xC100));
        assertEquals(Float.POSITIVE_INFINITY, ArrowValues.halfToFloat((short) 0x7C00));
        assertEquals(Float.NEGATIVE_INFINITY, ArrowValues.halfToFloat((short) 0xFC00));
        float nan = ArrowValues.halfToFloat((short) 0x7E01);
        assertTrue(Float.isNaN(nan));
        assertEquals(0x7FC0_2000, Float.floatToRawIntBits(nan), "payload carried into float bits");
    }

    // ---- streaming -------------------------------------------------------

    /** A source that yields the given tables as its batches. */
    private static BatchSource source(List<Field> fields, List<MaterializedTable> batches) {
        Deque<MaterializedTable> queue = new ArrayDeque<>(batches);
        return new BatchSource() {
            @Override
            public MaterializedTable empty() {
                List<List<Object>> cols = new ArrayList<>();
                fields.forEach(x -> cols.add(new ArrayList<>()));
                return table(fields, cols);
            }

            @Override
            public MaterializedTable next() {
                return queue.poll();
            }

            @Override
            public void close() {}
        };
    }

    private static List<MaterializedTable> batches(Field field, int... sizes) {
        List<MaterializedTable> out = new ArrayList<>();
        long next = 0;
        for (int size : sizes) {
            List<Object> values = col();
            for (int i = 0; i < size; i++) {
                values.add(next++);
            }
            out.add(table(List.of(field), List.of(values)));
        }
        return out;
    }

    @Test
    void streamedCompare_alignsMisalignedBatchBoundaries() throws IOException {
        Field n = f("n", new ArrowType.Int(64, true));
        assertEquals("pass", LogicalCompare.compare(ID,
                source(List.of(n), batches(n, 3, 0, 4, 3)),
                source(List.of(n), batches(n, 5, 5))).status);
    }

    @Test
    void streamedCompare_reportsAbsoluteRowAndTotals() throws IOException {
        Field n = f("n", new ArrowType.Int(64, true));
        List<MaterializedTable> got = batches(n, 4, 4);
        got.get(1).columns.get(0).set(2, -1L); // row 6
        Verdict v = LogicalCompare.compare(ID, source(List.of(n), got), source(List.of(n), batches(n, 3, 5)));
        assertEquals("fail", v.status);
        assertTrue(v.detail.contains("row 6"), v.detail);
        Verdict extra = LogicalCompare.compare(ID,
                source(List.of(n), batches(n, 4, 3)), source(List.of(n), batches(n, 5)));
        assertEquals("fail", extra.status);
        assertTrue(extra.note.contains("row count 7 != canonical 5"), extra.note);
        Verdict missing = LogicalCompare.compare(ID,
                source(List.of(n), batches(n)), source(List.of(n), batches(n, 2)));
        assertTrue(missing.note.contains("row count 0 != canonical 2"), missing.note);
    }

    @Test
    void streamedCompare_countsTheRestWithoutMaterializingIt() throws IOException {
        Field n = f("n", new ArrowType.Int(64, true));
        Deque<MaterializedTable> queue = new ArrayDeque<>(batches(n, 3, 3, 4));
        int[] materialized = {0};
        BatchSource expected = new BatchSource() {
            @Override
            public MaterializedTable empty() {
                return table(List.of(n), List.of(col()));
            }

            @Override
            public MaterializedTable next() {
                materialized[0]++;
                return queue.poll();
            }

            @Override
            public long countRemaining() {
                long rows = 0;
                while (!queue.isEmpty()) {
                    rows += queue.poll().rowCount;
                }
                return rows;
            }

            @Override
            public void close() {}
        };
        Verdict short_ = LogicalCompare.compare(ID, source(List.of(n), batches(n, 2)), expected);
        assertTrue(short_.note.contains("row count 2 != canonical 10"), short_.note);
        assertEquals(1, materialized[0], "the unread batches were materialized to count them");
    }

    @Test
    void streamedCompare_checksSchemaWithoutBatches() throws IOException {
        Field n = f("n", new ArrowType.Int(64, true));
        Field s = f("n", new ArrowType.Utf8());
        assertEquals("pass", LogicalCompare.compare(ID,
                source(List.of(n), List.of()), source(List.of(n), List.of())).status);
        assertEquals("fail", LogicalCompare.compare(ID,
                source(List.of(s), List.of()), source(List.of(n), List.of())).status);
    }

    @Test
    void streamedCompare_gapInOneColumnDoesNotHideAFailInAnother() throws IOException {
        Field days = f("d", new ArrowType.Date(DateUnit.DAY));
        Field millis = f("d", new ArrowType.Date(DateUnit.MILLISECOND));
        Field n = f("n", new ArrowType.Int(64, true));
        MaterializedTable expected = table(List.of(days, n), List.of(col(1, 2), col(1L, 2L)));
        MaterializedTable same = table(List.of(millis, n), List.of(col(1L, 2L), col(1L, 2L)));
        MaterializedTable differ = table(List.of(millis, n), List.of(col(1L, 2L), col(1L, 3L)));
        assertEquals("skip", LogicalCompare.compare(ID,
                source(List.of(millis, n), List.of(same)), source(List.of(days, n), List.of(expected))).status);
        assertEquals("fail", LogicalCompare.compare(ID,
                source(List.of(millis, n), List.of(differ)), source(List.of(days, n), List.of(expected))).status);
    }

    // ---- CLI -------------------------------------------------------------

    @Test
    void cliIsStrict() {
        Cli cli = Cli.parse(new String[] {"--input", "a", "--report=r"}, "input", "report");
        assertEquals(java.nio.file.Path.of("a"), cli.path("input"));
        assertEquals(java.nio.file.Path.of("r"), cli.path("report"));
        for (String[] bad : List.of(
                new String[] {"--input", "--report", "r"},     // a value that is an option
                new String[] {"--unknown", "x"},
                new String[] {"positional"},
                new String[] {"--input"},
                new String[] {"--input", "a", "--input", "b"})) {
            assertThrows(IllegalArgumentException.class, () -> Cli.parse(bad, "input", "report"),
                    String.join(" ", bad));
        }
    }
}
