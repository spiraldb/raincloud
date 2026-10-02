// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * LOGICAL comparison of a produced table against the canonical — the JVM lanes'
 * counterpart of the Python {@code _roundtrip_verdict} and the Rust
 * {@code logical_eq}. It is not identical to them: where those two judge a
 * representation change by a reversible cast, this comparator judges only the
 * pairs listed below and reports the rest as a comparator gap. sidecars/README.md
 * tabulates where the three lanes' verdicts differ.
 *
 * <p>Compares at the logical-value level ({@code getObject}), NOT by re-implementing
 * arrow-cast. Bucketed by the (canonical, got) <b>type pair</b>, never by the boxed
 * class {@code getObject} happens to return:
 *
 * <ul>
 *   <li>row count + column names first;
 *   <li>nested values recurse through their child Fields before comparing leaves;
 *       equal leaf types use bit-exact floats and content equality for binary;
 *   <li>differing types, same family: STRING/BINARY/BOOL compare their (identical)
 *       boxed kind; INT compares exact signed/unsigned integers; FLOAT compares the
 *       decoded values' {@code doubleToRawLongBits} (matches Rust's bitwise NaN /
 *       signed-zero semantics; half floats are decoded from their raw bits first);
 *       TIMESTAMP compares the INSTANT across differing units (e.g. the
 *       SECOND→MILLIS promotion Parquet forces) and, when both are zoned, across
 *       zone labels; naive vs zoned, and different families, are a genuine
 *       mismatch (fail);
 *   <li>an INT and a scale-0 DECIMAL compare as exact integers;
 *   <li>a union of exactly Null and T (sparse or dense, at any depth) is a
 *       nullable T, as Arrow Java's Avro adapter spells Avro's ["null", T];
 *       any other union against a non-union is a mismatch;
 *   <li>a type pair the comparator can't confidently judge (differing
 *       DECIMAL/DATE/TIME/DURATION/NESTED encodings, union vs union) → the column is a
 *       <b>comparator gap</b>: the run yields {@code skip} with a distinct note,
 *       never a false {@code pass} and never a {@code fail} for OUR limitation.
 * </ul>
 *
 * <p>Dictionary-encoded columns are decoded to their logical values upstream (in
 * {@link MaterializedTable.Builder}), so they compare as ordinary value columns here.</p>
 *
 * Field metadata (incl. raincloud's {@code __variant_type} marker) never enters the
 * compare — a dropped VARIANT annotation is not a read fail.
 */
public final class LogicalCompare {
    private LogicalCompare() {}

    private static final String VARIANT_MARKER = "__variant_type";

    private enum Family { STRING, INT, FLOAT, BOOL, BINARY, DECIMAL, DATE, TIMESTAMP, NESTED, OTHER }

    /**
     * Compare two batch streams, holding one batch of each side at a time. Batch
     * boundaries need not agree: rows are compared window by window, and the schema
     * is compared even when neither side has a batch.
     */
    public static Verdict compare(String id, BatchSource got, BatchSource expected) throws IOException {
        MaterializedTable expectedSchema = expected.empty();
        Verdict schema = compareRows(id, got.empty(), expectedSchema, 0);
        if ("fail".equals(schema.status)) {
            return schema;
        }
        Window g = new Window(got), e = new Window(expected);
        Verdict gap = null;
        long row = 0;
        while (true) {
            int ga = g.available(), ea = e.available();
            if (ga == 0 || ea == 0) {
                if (ga != 0 || ea != 0) {
                    long gotRows = row + g.countRest(), expectedRows = row + e.countRest();
                    return Verdict.fail(id + ": row count " + gotRows + " != canonical " + expectedRows, "");
                }
                break;
            }
            int n = Math.min(ga, ea);
            Verdict v = compareRows(id, g.take(n), e.take(n), row);
            if ("fail".equals(v.status)) {
                return v;
            }
            if (gap == null && "skip".equals(v.status)) {
                gap = v;
            }
            row += n;
        }
        if (row == 0) {
            return schema; // no rows: the schema verdict is the whole verdict
        }
        return gap != null ? gap : pass(id, expectedSchema);
    }

    /** Compare two materialized tables in full. */
    public static Verdict compare(String id, MaterializedTable got, MaterializedTable expected) {
        if (got.rowCount != expected.rowCount) {
            return Verdict.fail(
                    id + ": row count " + got.rowCount + " != canonical " + expected.rowCount, "");
        }
        return compareRows(id, got, expected, 0);
    }

    // Equal row counts; `offset` is the first row's position in the whole table.
    private static Verdict compareRows(String id, MaterializedTable got, MaterializedTable expected,
            long offset) {
        if (!got.columnNames().equals(expected.columnNames())) {
            return Verdict.fail(id + ": column names differ",
                    "got " + got.columnNames() + " != canonical " + expected.columnNames());
        }

        StringBuilder gaps = new StringBuilder();
        for (int c = 0; c < expected.fields.size(); c++) {
            Field ef = expected.fields.get(c);
            Field gf = got.fields.get(c);
            String name = ef.getName();

            Compatibility shape = compatibility(ef, gf);
            if (shape == Compatibility.MISMATCH) {
                return Verdict.fail(id + ": schema mismatch vs canonical",
                        "column \"" + name + "\": canonical=" + ef + " got=" + gf);
            }
            if (shape == Compatibility.GAP) {
                appendGap(gaps, name + ": " + ef + " vs " + gf);
                continue;
            }

            List<Object> ecol = expected.columns.get(c);
            List<Object> gcol = got.columns.get(c);
            for (int i = 0; i < ecol.size(); i++) {
                if (!cellEquals(ef, ecol.get(i), gf, gcol.get(i))) {
                    return Verdict.fail(id + ": data mismatch vs canonical",
                            "column \"" + name + "\" row " + (offset + i)
                                    + ": canonical=" + show(ecol.get(i))
                                    + " got=" + show(gcol.get(i)));
                }
            }
        }

        if (gaps.length() > 0) {
            return Verdict.skip(id + ": comparator gap (unmeasured columns)", gaps.toString());
        }
        return pass(id, expected);
    }

    private static Verdict pass(String id, MaterializedTable expected) {
        String variantNote = hasVariant(expected.fields)
                ? " (VARIANT column present — compared as its shredded struct)" : "";
        return Verdict.pass(id + ": round-trips to canonical" + variantNote);
    }

    /** One side of a streamed comparison: a batch source read as a row sequence. */
    private static final class Window {
        private final BatchSource source;
        private MaterializedTable current;
        private int offset;
        private boolean done;

        Window(BatchSource source) {
            this.source = source;
        }

        /** Rows left in the current batch, advancing past empty ones; 0 at the end. */
        int available() throws IOException {
            while (!done && (current == null || offset >= current.rowCount)) {
                current = source.next();
                offset = 0;
                done = current == null;
            }
            return done ? 0 : current.rowCount - offset;
        }

        MaterializedTable take(int n) {
            MaterializedTable slice = current.slice(offset, offset + n);
            offset += n;
            return slice;
        }

        /** Rows not yet taken, counted without materializing the batches still unread. */
        long countRest() throws IOException {
            if (done) {
                return 0;
            }
            long rows = current == null ? 0 : current.rowCount - offset;
            current = null;
            done = true;
            return rows + source.countRemaining();
        }
    }

    private static boolean isList(ArrowType type) {
        return type instanceof ArrowType.List || type instanceof ArrowType.LargeList
                || type instanceof ArrowType.FixedSizeList;
    }

    private static boolean sameContainer(ArrowType a, ArrowType b) {
        return (isList(a) && isList(b))
                || (a instanceof ArrowType.Struct && b instanceof ArrowType.Struct)
                || (a instanceof ArrowType.Map && b instanceof ArrowType.Map);
    }

    private enum Compatibility { MATCH, MISMATCH, GAP }

    /** True when two children of one struct share a name (Arrow allows this). */
    private static boolean hasDuplicateNames(List<Field> children) {
        Set<String> seen = new HashSet<>();
        for (Field f : children) {
            if (!seen.add(f.getName())) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@code field} with every union of exactly Null and T replaced by T. Such a
     * union's {@code getObject} is already T's value (null for the Null member), so
     * only the type needs unwrapping. Callers keep the outer name.
     */
    private static Field logical(Field field) {
        while (field.getType() instanceof ArrowType.Union && field.getChildren().size() == 2) {
            Field a = field.getChildren().get(0), b = field.getChildren().get(1);
            boolean aNull = a.getType() instanceof ArrowType.Null, bNull = b.getType() instanceof ArrowType.Null;
            if (aNull == bNull) {
                break;
            }
            field = aNull ? b : a;
        }
        return field;
    }

    private static boolean isScaleZeroDecimal(ArrowType t) {
        return t instanceof ArrowType.Decimal && ((ArrowType.Decimal) t).getScale() == 0;
    }

    private static boolean intAndScaleZeroDecimal(ArrowType a, ArrowType b) {
        return (a instanceof ArrowType.Int && isScaleZeroDecimal(b))
                || (isScaleZeroDecimal(a) && b instanceof ArrowType.Int);
    }

    // Check shape before looking at values: null parents and empty containers still
    // have schemas. List element names are representation details; struct names are not.
    private static Compatibility compatibility(Field expected, Field got) {
        expected = logical(expected);
        got = logical(got);
        if (expected.getDictionary() != null || got.getDictionary() != null) {
            return Compatibility.GAP;
        }
        ArrowType et = expected.getType(), gt = got.getType();
        // Both declared dimensions are logical constraints even when no value can
        // witness their difference. Fixed/variable forms remain comparable below.
        if (et instanceof ArrowType.FixedSizeBinary && gt instanceof ArrowType.FixedSizeBinary
                && ((ArrowType.FixedSizeBinary) et).getByteWidth()
                        != ((ArrowType.FixedSizeBinary) gt).getByteWidth()) {
            return Compatibility.MISMATCH;
        }
        if (et instanceof ArrowType.FixedSizeList && gt instanceof ArrowType.FixedSizeList
                && ((ArrowType.FixedSizeList) et).getListSize()
                        != ((ArrowType.FixedSizeList) gt).getListSize()) {
            return Compatibility.MISMATCH;
        }
        if (family(et) == Family.NESTED || family(gt) == Family.NESTED) {
            if (!sameContainer(et, gt)) {
                // Two unions box by type id, unjudged; a union vs a non-union is another type.
                return et instanceof ArrowType.Union && gt instanceof ArrowType.Union
                        ? Compatibility.GAP : Compatibility.MISMATCH;
            }
            List<Field> ec = expected.getChildren(), gc = got.getChildren();
            if (ec.size() != gc.size()) {
                return Compatibility.MISMATCH;
            }
            if (!(et instanceof ArrowType.Struct) && ec.size() != 1) {
                return Compatibility.GAP; // malformed container schema
            }
            // Struct cells are boxed as name-keyed Maps, so two children sharing
            // a name collapse into one entry and `cellEquals` then compares the
            // same child twice -- struct<a,a> holding (1,2) compares equal to
            // (1,1). Arrow permits duplicate child names, so this is a real
            // shape the comparator cannot see. Report it as a GAP: the column
            // becomes an explicitly unmeasured `skip`, never a silent pass.
            if (et instanceof ArrowType.Struct
                    && (hasDuplicateNames(ec) || hasDuplicateNames(gc))) {
                return Compatibility.GAP;
            }
            Compatibility result = Compatibility.MATCH;
            for (int i = 0; i < ec.size(); i++) {
                if (et instanceof ArrowType.Struct
                        && !ec.get(i).getName().equals(gc.get(i).getName())) {
                    return Compatibility.MISMATCH;
                }
                Compatibility child = compatibility(ec.get(i), gc.get(i));
                if (child == Compatibility.MISMATCH) {
                    return child; // a known mismatch wins over an unsupported sibling
                }
                if (child == Compatibility.GAP) {
                    result = child;
                }
            }
            return result;
        }
        if (intAndScaleZeroDecimal(et, gt)) {
            return Compatibility.MATCH;
        }
        if (family(et) != family(gt)) {
            return Compatibility.MISMATCH;
        }
        // A zone labels UTC instants; it is not data. Naive and zoned differ in kind, as in
        // the Python and Rust comparators.
        if (et instanceof ArrowType.Timestamp && (((ArrowType.Timestamp) et).getTimezone() == null)
                != (((ArrowType.Timestamp) gt).getTimezone() == null)) {
            return Compatibility.MISMATCH;
        }
        return isGap(et, gt) ? Compatibility.GAP : Compatibility.MATCH;
    }

    private static boolean cellEquals(Field expected, Object e, Field got, Object g) {
        if (e == null || g == null) {
            return e == g;
        }
        expected = logical(expected);
        got = logical(got);
        ArrowType et = expected.getType(), gt = got.getType();
        if (family(et) != Family.NESTED && family(gt) != Family.NESTED) {
            return cellEquals(et, e, gt, g);
        }
        if (!sameContainer(et, gt)) {
            return false;
        }
        List<Field> ec = expected.getChildren(), gc = got.getChildren();
        if (ec.size() != gc.size()) {
            return false;
        }
        if (et instanceof ArrowType.Struct) {
            if (!(e instanceof Map) || !(g instanceof Map)) {
                return false;
            }
            Map<?, ?> em = (Map<?, ?>) e, gm = (Map<?, ?>) g;
            for (int i = 0; i < ec.size(); i++) {
                Field ef = ec.get(i), gf = gc.get(i);
                if (!ef.getName().equals(gf.getName())
                        || !cellEquals(ef, em.get(ef.getName()), gf, gm.get(gf.getName()))) {
                    return false;
                }
            }
            // Arrow may omit null children from the boxed map, but undeclared
            // children must not silently escape comparison.
            return em.keySet().stream().allMatch(k -> ec.stream().anyMatch(f -> f.getName().equals(k)))
                    && gm.keySet().stream().allMatch(k -> gc.stream().anyMatch(f -> f.getName().equals(k)));
        }
        // Lists and maps both box as ordered lists; a map child is an entry struct.
        if (!(e instanceof List) || !(g instanceof List) || ec.size() != 1) {
            return false;
        }
        List<?> el = (List<?>) e, gl = (List<?>) g;
        if (el.size() != gl.size()) {
            return false;
        }
        for (int i = 0; i < el.size(); i++) {
            if (!cellEquals(ec.get(0), el.get(i), gc.get(0), gl.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * True when a same-family (et, gt) pair is a comparator gap (can't confidently
     * judge). Only reached from {@link #compatibility}, after cross-family pairs and
     * naive-vs-zoned timestamps have already been ruled a mismatch.
     */
    private static boolean isGap(ArrowType et, ArrowType gt) {
        if (et.equals(gt)) {
            return false; // equal types always comparable via deepEquals
        }
        switch (family(et)) {
            case STRING:
            case INT:
            case FLOAT:
            case BOOL:
            case BINARY:
            case TIMESTAMP: // same instant across units, e.g. the SECOND->MILLIS promotion Parquet forces
                return false;
            default:
                return true; // DECIMAL/DATE/NESTED/OTHER with differing encodings
        }
    }

    private static boolean cellEquals(ArrowType et, Object e, ArrowType gt, Object g) {
        if (e == null && g == null) {
            return true;
        }
        if (e == null || g == null) {
            return false;
        }
        if (et.equals(gt)) {
            return deepEquals(e, g);
        }
        if (intAndScaleZeroDecimal(et, gt)) {
            return exactInteger(et, e).compareTo(exactInteger(gt, g)) == 0;
        }
        Family fe = family(et);
        Family fg = family(gt);
        if (fe != fg) {
            return false; // genuine type mismatch across families
        }
        switch (fe) {
            case INT:
                // EXACT (BigInteger) comparison: a uint64 above Long.MAX_VALUE
                // arrives as a negative long and would otherwise compare equal to
                // the signed int64 of the same bit pattern (uint64 max vs -1).
                return ArrowValues.intAsExact((ArrowType.Int) et, e)
                        .equals(ArrowValues.intAsExact((ArrowType.Int) gt, g));
            case FLOAT:
                return Double.doubleToRawLongBits(ArrowValues.floatAsDouble((ArrowType.FloatingPoint) et, e))
                        == Double.doubleToRawLongBits(ArrowValues.floatAsDouble((ArrowType.FloatingPoint) gt, g));
            case TIMESTAMP:
                return timestampEquals((ArrowType.Timestamp) et, e, (ArrowType.Timestamp) gt, g);
            default:
                // STRING / BINARY / BOOL: getObject yields the same boxed kind on both sides.
                return deepEquals(e, g);
        }
    }

    // An INT or scale-0 DECIMAL cell (boxed as a BigDecimal) as an exact number.
    private static BigDecimal exactInteger(ArrowType t, Object v) {
        return t instanceof ArrowType.Int
                ? new BigDecimal(ArrowValues.intAsExact((ArrowType.Int) t, v)) : (BigDecimal) v;
    }

    // Compares two timestamps, both naive or both zoned (any zones), possibly of differing units.
    // No-tz timestamps box as LocalDateTime already at their wall-clock instant (equal instants
    // compare equal regardless of source unit); tz timestamps box as a raw long UTC instant, so
    // normalize both to nanoseconds.
    private static boolean timestampEquals(ArrowType.Timestamp et, Object e, ArrowType.Timestamp gt, Object g) {
        if (e instanceof java.time.LocalDateTime && g instanceof java.time.LocalDateTime) {
            return e.equals(g);
        }
        if (e instanceof Number && g instanceof Number) {
            return toNanos(et.getUnit(), ((Number) e).longValue())
                    .equals(toNanos(gt.getUnit(), ((Number) g).longValue()));
        }
        return deepEquals(e, g);
    }

    // Exact (BigInteger) so a large SECOND/MILLI instant can't overflow a long and collide with an
    // unrelated instant — a false pass. Correctness over speed: timestamps compare per-cell, but a
    // conformance oracle must never mask a real divergence.
    private static java.math.BigInteger toNanos(TimeUnit unit, long value) {
        final long factor;
        switch (unit) {
            case SECOND:
                factor = 1_000_000_000L;
                break;
            case MILLISECOND:
                factor = 1_000_000L;
                break;
            case MICROSECOND:
                factor = 1_000L;
                break;
            default:
                factor = 1L; // NANOSECOND
        }
        return java.math.BigInteger.valueOf(value).multiply(java.math.BigInteger.valueOf(factor));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static boolean deepEquals(Object a, Object b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a instanceof byte[] && b instanceof byte[]) {
            return Arrays.equals((byte[]) a, (byte[]) b);
        }
        if (a instanceof Double && b instanceof Double) {
            return Double.doubleToRawLongBits((Double) a) == Double.doubleToRawLongBits((Double) b);
        }
        if (a instanceof Float && b instanceof Float) {
            return Float.floatToRawIntBits((Float) a) == Float.floatToRawIntBits((Float) b);
        }
        if (a instanceof Map && b instanceof Map) {
            Map ma = (Map) a;
            Map mb = (Map) b;
            if (!ma.keySet().equals(mb.keySet())) {
                return false;
            }
            for (Object k : ma.keySet()) {
                if (!deepEquals(ma.get(k), mb.get(k))) {
                    return false;
                }
            }
            return true;
        }
        if (a instanceof List && b instanceof List) {
            List la = (List) a;
            List lb = (List) b;
            if (la.size() != lb.size()) {
                return false;
            }
            for (int i = 0; i < la.size(); i++) {
                if (!deepEquals(la.get(i), lb.get(i))) {
                    return false;
                }
            }
            return true;
        }
        return Objects.equals(a, b);
    }

    private static Family family(ArrowType t) {
        switch (t.getTypeID()) {
            case Utf8:
            case LargeUtf8:
            case Utf8View:
                return Family.STRING;
            case Int:
                return Family.INT;
            case FloatingPoint:
                return Family.FLOAT;
            case Bool:
                return Family.BOOL;
            case BinaryView:
            case Binary:
            case LargeBinary:
            case FixedSizeBinary:
                return Family.BINARY;
            case Decimal:
                return Family.DECIMAL;
            case Date:
                return Family.DATE;
            case Timestamp:
                return Family.TIMESTAMP;
            case Struct:
            case List:
            case LargeList:
            case FixedSizeList:
            case Map:
            case Union:
                return Family.NESTED;
            default:
                return Family.OTHER;
        }
    }

    /** True if any top-level field carries raincloud's VARIANT marker. */
    public static boolean hasVariant(List<Field> fields) {
        for (Field f : fields) {
            Map<String, String> md = f.getMetadata();
            if (md != null && md.containsKey(VARIANT_MARKER)) {
                return true;
            }
        }
        return false;
    }

    private static void appendGap(StringBuilder sb, String s) {
        if (sb.length() > 0) {
            sb.append("; ");
        }
        sb.append(s);
    }

    private static String show(Object o) {
        if (o == null) {
            return "null";
        }
        if (o instanceof byte[]) {
            return "bytes[" + ((byte[]) o).length + "]";
        }
        String s = o.toString();
        return s.length() > 60 ? s.substring(0, 60) + "…" : s;
    }
}
