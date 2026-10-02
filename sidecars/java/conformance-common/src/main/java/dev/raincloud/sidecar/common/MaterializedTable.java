// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A materialized table: per-column {@link Field} + the logical values
 * ({@code FieldVector.getObject(i)}), one heap list per column.
 *
 * <p>Materializing via {@code getObject} decouples the comparison from Arrow vector
 * lifetime (values are heap copies — safe after the source reader/allocator is
 * closed). The sidecars materialize one record batch at a time through
 * {@link BatchSource}, so memory is O(batch), not O(table).</p>
 */
public final class MaterializedTable {
    public final List<Field> fields;
    public final List<List<Object>> columns;
    public final int rowCount;

    public MaterializedTable(List<Field> fields, List<List<Object>> columns) {
        this(fields, columns, columns.isEmpty() ? 0 : columns.get(0).size());
    }

    private MaterializedTable(List<Field> fields, List<List<Object>> columns, int rowCount) {
        this.fields = fields;
        this.columns = columns;
        this.rowCount = rowCount;
    }

    public List<String> columnNames() {
        List<String> names = new ArrayList<>(fields.size());
        for (Field f : fields) {
            names.add(f.getName());
        }
        return names;
    }

    /** Rows {@code [from, to)} as a view over the same values. */
    public MaterializedTable slice(int from, int to) {
        List<List<Object>> sliced = new ArrayList<>(columns.size());
        for (List<Object> column : columns) {
            sliced.add(column.subList(from, to));
        }
        return new MaterializedTable(fields, sliced, to - from);
    }

    /** Accumulates a stream of {@link VectorSchemaRoot} batches into one table. */
    public static final class Builder {
        private List<Field> fields;
        private List<List<Object>> columns;
        private int rowCount;

        /** Capture the schema even when the reader supplies no record batches. */
        public void initialize(Schema schema, DictionaryProvider dictionaries) {
            if (fields != null) {
                return;
            }
            fields = new ArrayList<>();
            columns = new ArrayList<>();
            for (Field field : schema.getFields()) {
                DictionaryEncoding encoding = field.getDictionary();
                Dictionary dictionary = encoding == null || dictionaries == null
                        ? null : dictionaries.lookup(encoding.getId());
                fields.add(dictionary == null ? field
                        : recordedField(field, dictionary.getVector().getField()));
                columns.add(new ArrayList<>());
            }
        }

        /** Appends a batch with no dictionary decoding (sources that already carry plain values). */
        public void appendBatch(VectorSchemaRoot root) {
            appendBatch(root, null);
        }

        /**
         * Appends a batch, decoding dictionary-encoded columns to their logical VALUES via
         * {@code dictionaries} so a dictionary lane compares equal to a plain-value lane. The
         * recorded {@link Field} keeps the original name + metadata but adopts the decoded value
         * type with no dictionary encoding. A null provider (or a column with no dictionary) is
         * materialized as-is via {@code getObject}.
         *
         * @throws ComparatorGap for a dictionary-encoded child of a nested column, which
         *     {@code getObject} would box as raw indices
         */
        public void appendBatch(VectorSchemaRoot root, DictionaryProvider dictionaries) {
            initialize(root.getSchema(), dictionaries);
            List<FieldVector> incoming = root.getFieldVectors();
            if (incoming.size() != fields.size()) {
                throw new IllegalStateException(
                        "batch has " + incoming.size() + " columns but this table was initialized "
                                + "with " + fields.size() + "; the recorded schema and the vectors "
                                + "producing values come from different sources");
            }
            int rows = root.getRowCount();
            rowCount = Math.addExact(rowCount, rows);
            for (int c = 0; c < incoming.size(); c++) {
                FieldVector source = incoming.get(c);
                FieldVector values = source;
                boolean decoded = false;
                DictionaryEncoding encoding = source.getField().getDictionary();
                if (encoding != null && dictionaries != null) {
                    Dictionary dictionary = dictionaries.lookup(encoding.getId());
                    if (dictionary != null) {
                        values = (FieldVector) DictionaryEncoder.decode(source, dictionary);
                        decoded = true;
                    }
                }
                // DictionaryEncoder.decode() allocates; every exit below, including the
                // throws, must release it.
                try {
                    appendColumn(c, source.getField().getName(), values, decoded, rows);
                } finally {
                    if (decoded) {
                        values.close();
                    }
                }
            }
        }

        // THE INVARIANT: the recorded Field must describe the values actually appended
        // to this column, all the way down. A recorded type that disagrees with the
        // vector being materialized is the false-pass vector -- LogicalCompare coerces
        // numbers, so a dictionary INDEX of 1 compares equal to a value of 1.0 against a
        // Field claiming FLOAT. Everything below enforces that one property rather than
        // guessing at the ways it can be broken.
        private void appendColumn(int c, String name, FieldVector values, boolean decoded, int rows) {
            String nested = nestedDictionary(values.getField(), name);
            if (nested != null) {
                throw new ComparatorGap("column " + c + " (" + name + "): " + nested
                        + " is dictionary-encoded inside a nested column; only top-level "
                        + "dictionaries are decoded to values");
            }
            if (!sameShape(fields.get(c), values.getField())) {
                if (!columns.get(c).isEmpty()) {
                    throw new IllegalStateException(
                            "column " + c + " (" + name + ") changed type after "
                                    + columns.get(c).size() + " rows were already recorded as "
                                    + fields.get(c) + "; those rows would be compared "
                                    + "under the wrong type");
                }
                if (!decoded) {
                    throw new IllegalStateException(
                            "column " + c + " (" + name + ") was recorded as "
                                    + fields.get(c) + " but is being materialized as "
                                    + values.getField() + "; if it is dictionary-"
                                    + "encoded, pass the dictionary provider to appendBatch");
                }
                // Legitimate: the reader declared the decoded value type up front (a scan
                // schema) while each batch carries indices. Adopt the decoded field.
                fields.set(c, recordedField(fields.get(c), values.getField()));
            }
            List<Object> col = columns.get(c);
            for (int i = 0; i < rows; i++) {
                col.add(values.getObject(i));
            }
        }

        /** Type, dictionary encoding and children agree recursively (names are not compared). */
        private static boolean sameShape(Field recorded, Field actual) {
            if (!sameType(recorded.getType(), actual.getType())
                    || !Objects.equals(recorded.getDictionary(), actual.getDictionary())
                    || recorded.getChildren().size() != actual.getChildren().size()) {
                return false;
            }
            for (int i = 0; i < recorded.getChildren().size(); i++) {
                if (!sameShape(recorded.getChildren().get(i), actual.getChildren().get(i))) {
                    return false;
                }
            }
            return true;
        }

        // UnionVector.getField() re-derives type ids from its members' minor types, so a
        // union read from IPC reports other ids than its schema; mode and members decide.
        private static boolean sameType(ArrowType recorded, ArrowType actual) {
            if (recorded instanceof ArrowType.Union && actual instanceof ArrowType.Union) {
                return ((ArrowType.Union) recorded).getMode() == ((ArrowType.Union) actual).getMode();
            }
            return recorded.equals(actual);
        }

        /** The path of the first dictionary-encoded descendant of {@code field}, or null. */
        private static String nestedDictionary(Field field, String path) {
            for (Field child : field.getChildren()) {
                String childPath = path + "." + child.getName();
                if (child.getDictionary() != null) {
                    return childPath;
                }
                String deeper = nestedDictionary(child, childPath);
                if (deeper != null) {
                    return deeper;
                }
            }
            return null;
        }

        // decode() names the vector after the dictionary ("DICT0"); keep the original column name +
        // metadata but adopt the decoded value type and drop the dictionary encoding.
        private static Field recordedField(Field original, Field valueField) {
            return new Field(original.getName(),
                    new FieldType(original.isNullable(), valueField.getType(), null, original.getMetadata()),
                    valueField.getChildren());
        }

        public MaterializedTable build() {
            if (fields == null) {
                return new MaterializedTable(new ArrayList<>(), new ArrayList<>());
            }
            return new MaterializedTable(fields, columns, rowCount);
        }
    }
}
