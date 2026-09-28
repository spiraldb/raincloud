// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.hardwood;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import dev.hardwood.InputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.reader.ColumnReader;
import dev.hardwood.reader.ColumnReaders;
import dev.hardwood.reader.LayerKind;
import dev.hardwood.reader.ParquetFileReader;
import dev.hardwood.schema.ColumnProjection;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.schema.SchemaNode;
import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.MaterializedTable;
import dev.raincloud.sidecar.common.VariantFidelity;

/**
 * Parquet → Arrow through Hardwood's columnar reader ({@code ColumnReaders}), for the shared
 * comparator. Every leaf column is read in lockstep; each batch's leaf arrays and the
 * per-layer validity and offsets of Hardwood's layer model are assembled into Arrow vectors,
 * which then materialize like every other lane's.
 *
 * <p>The Arrow types are the ones Parquet itself declares, as Arrow C++ reads a file without an
 * {@code ARROW:schema} hint (which this lane ignores): annotated integers at their width and
 * sign, {@code DATE} as date32, {@code TIME}/{@code TIMESTAMP} at their unit (an adjusted-to-UTC
 * timestamp in {@code "UTC"}), decimals as decimal128 (decimal256 above 38 digits),
 * {@code FLOAT16} as a half float, {@code UUID} as fixed_size_binary(16), strings, enums and
 * JSON as utf8, {@code UNKNOWN} as the null type, plain groups as structs, {@code VARIANT} groups
 * as structs carrying the {@code arrow.parquet.variant} extension,
 * and {@code LIST}/{@code MAP} (legacy two-level lists included) as list/map. {@code INT96},
 * {@code INTERVAL}, a key-only map, a repeated field outside a {@code LIST} or {@code MAP}, and
 * a layer model that disagrees with the schema are {@link UnsupportedHardwoodTypeException}: a
 * comparator gap, never a verdict on the file.</p>
 */
public final class HardwoodReader {
    private HardwoodReader() {}

    private static final String VARIANT_EXTENSION_NAME = "ARROW:extension:name";

    /** The top-level columns {@code parquet} declares as VARIANT groups, as Hardwood reads its schema. */
    public static Set<String> variantColumns(Path parquet) throws IOException {
        Set<String> columns = new HashSet<>();
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(parquet))) {
            for (SchemaNode child : file.getFileSchema().getRootNode().children()) {
                if (child instanceof SchemaNode.GroupNode group && group.isVariant()) {
                    columns.add(child.name());
                }
            }
        }
        return columns;
    }

    /** A Parquet file's batches, read through Hardwood. */
    public static BatchSource openParquet(Path parquet, BufferAllocator allocator) throws IOException {
        ParquetFileReader file = ParquetFileReader.open(InputFile.of(parquet));
        try {
            FileSchema fileSchema = file.getFileSchema();
            List<Node> columns = new ArrayList<>();
            List<Field> fields = new ArrayList<>();
            for (SchemaNode child : fileSchema.getRootNode().children()) {
                if (child.repetitionType() == RepetitionType.REPEATED) {
                    throw new UnsupportedHardwoodTypeException(child.name() + ": a repeated field outside a LIST or MAP");
                }
                Node node = node(fileSchema, child, child.name(), child.repetitionType() == RepetitionType.OPTIONAL,
                        new ArrayList<>());
                columns.add(node);
                fields.add(node.field);
            }
            Schema schema = new Schema(fields);
            ColumnReaders readers = fileSchema.getColumnCount() == 0 ? null
                    : file.columnReaders(ColumnProjection.all());
            try {
                if (readers != null) {
                    checkLayers(columns, readers);
                }
                return new Source(file, readers, schema, columns, allocator);
            } catch (RuntimeException e) {
                if (readers != null) {
                    readers.close();
                }
                throw e;
            }
        } catch (RuntimeException e) {
            try {
                file.close();
            } catch (IOException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }

    private static final class Source implements BatchSource {
        private final ParquetFileReader file;
        private final ColumnReaders readers;
        private final Schema schema;
        private final List<Node> columns;
        private final BufferAllocator allocator;

        Source(ParquetFileReader file, ColumnReaders readers, Schema schema, List<Node> columns,
                BufferAllocator allocator) {
            this.file = file;
            this.readers = readers;
            this.schema = schema;
            this.columns = columns;
            this.allocator = allocator;
        }

        @Override
        public MaterializedTable empty() {
            MaterializedTable.Builder builder = new MaterializedTable.Builder();
            builder.initialize(schema, null);
            return builder.build();
        }

        @Override
        public MaterializedTable next() {
            if (readers == null || !readers.nextBatch()) {
                return null;
            }
            int rows = readers.getRecordCount();
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew();
                for (int c = 0; c < columns.size(); c++) {
                    columns.get(c).fill(root.getVector(c), readers, 0, rows);
                }
                root.setRowCount(rows);
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.appendBatch(root);
                return builder.build();
            }
        }

        @Override
        public long countRemaining() {
            long rows = 0;
            while (readers != null && readers.nextBatch()) {
                rows += readers.getRecordCount();
            }
            return rows;
        }

        @Override
        public void close() throws IOException {
            try {
                if (readers != null) {
                    readers.close();
                }
            } finally {
                file.close();
            }
        }
    }

    // ---- the Parquet → Arrow plan ----------------------------------------------------------

    /**
     * @param nullable whether this node's Arrow field is nullable (a 2-level list's repeated
     *     element is not, whatever its repetition)
     * @param layers the layer kinds above this node, which every leaf beneath must report
     */
    private static Node node(FileSchema file, SchemaNode schemaNode, String name, boolean nullable,
            List<LayerKind> layers) {
        if (schemaNode instanceof SchemaNode.PrimitiveNode p) {
            return leaf(p, file.getColumn(p.columnIndex()).typeLength(), name, nullable, List.copyOf(layers));
        }
        SchemaNode.GroupNode group = (SchemaNode.GroupNode) schemaNode;
        boolean optional = group.repetitionType() == RepetitionType.OPTIONAL;
        if (group.isList()) {
            SchemaNode element = group.getListElement();
            if (element == null) {
                throw new UnsupportedHardwoodTypeException(name + ": a LIST group Hardwood finds no element in");
            }
            List<LayerKind> below = with(layers, LayerKind.REPEATED);
            boolean elementNullable = element.repetitionType() == RepetitionType.OPTIONAL;
            Node child = node(file, element, element.name(), elementNullable, below);
            Field field = new Field(name, new FieldType(optional, ArrowType.List.INSTANCE, null), List.of(child.field));
            return new ListNode(field, optional, child);
        }
        if (group.isMap()) {
            SchemaNode key = group.getMapKey(), value = group.getMapValue();
            if (key == null || value == null) {
                throw new UnsupportedHardwoodTypeException(name + ": a MAP group without the standard "
                        + "key_value.key and key_value.value fields");
            }
            List<LayerKind> below = with(layers, LayerKind.REPEATED);
            Node keyNode = node(file, key, key.name(), false, below);
            Node valueNode = node(file, value, value.name(), value.repetitionType() == RepetitionType.OPTIONAL,
                    below);
            String entries = group.children().get(0).name();
            Field entry = new Field(entries, FieldType.notNullable(ArrowType.Struct.INSTANCE),
                    List.of(keyNode.field, valueNode.field));
            Field field = new Field(name, new FieldType(optional, new ArrowType.Map(false), null), List.of(entry));
            return new MapNode(field, optional, keyNode, valueNode);
        }
        if (!group.isStruct() && !group.isVariant()) {
            throw new UnsupportedHardwoodTypeException(name + ": a group annotated " + group.logicalType()
                    + " / " + group.convertedType());
        }
        List<LayerKind> below = optional ? with(layers, LayerKind.STRUCT) : layers;
        List<Node> children = new ArrayList<>();
        List<Field> childFields = new ArrayList<>();
        for (SchemaNode c : group.children()) {
            if (c.repetitionType() == RepetitionType.REPEATED) {
                throw new UnsupportedHardwoodTypeException(name + "." + c.name()
                        + ": a repeated field outside a LIST or MAP");
            }
            Node child = node(file, c, c.name(), c.repetitionType() == RepetitionType.OPTIONAL, below);
            children.add(child);
            childFields.add(child.field);
        }
        // Hardwood names a VARIANT group as one: Arrow names its storage struct with the extension.
        Map<String, String> metadata = group.isVariant()
                ? Map.of(VARIANT_EXTENSION_NAME, VariantFidelity.EXTENSION) : null;
        Field field = new Field(name, new FieldType(nullable, ArrowType.Struct.INSTANCE, null, metadata), childFields);
        return new StructNode(field, optional, children);
    }

    private static List<LayerKind> with(List<LayerKind> layers, LayerKind kind) {
        List<LayerKind> out = new ArrayList<>(layers);
        out.add(kind);
        return out;
    }

    private static Leaf leaf(SchemaNode.PrimitiveNode p, Integer typeLength, String name, boolean nullable,
            List<LayerKind> layers) {
        LogicalType logical = p.logicalType();
        String where = name + " (" + p.type() + (logical == null ? "" : " " + logical) + ")";
        ArrowType type = switch (p.type()) {
            case BOOLEAN -> logical == null ? ArrowType.Bool.INSTANCE : null;
            case INT32 -> switch (logical) {
                case null -> new ArrowType.Int(32, true);
                case LogicalType.IntType t when t.bitWidth() <= 32 -> new ArrowType.Int(t.bitWidth(), t.isSigned());
                case LogicalType.DateType d -> new ArrowType.Date(DateUnit.DAY);
                case LogicalType.TimeType t when t.unit() == LogicalType.TimeUnit.MILLIS ->
                        new ArrowType.Time(TimeUnit.MILLISECOND, 32);
                case LogicalType.DecimalType d -> decimal(d);
                case LogicalType.NullType n -> ArrowType.Null.INSTANCE;
                default -> null;
            };
            case INT64 -> switch (logical) {
                case null -> new ArrowType.Int(64, true);
                case LogicalType.IntType t when t.bitWidth() == 64 -> new ArrowType.Int(64, t.isSigned());
                case LogicalType.TimeType t when t.unit() != LogicalType.TimeUnit.MILLIS ->
                        new ArrowType.Time(unit(t.unit()), 64);
                case LogicalType.TimestampType t -> new ArrowType.Timestamp(unit(t.unit()),
                        t.isAdjustedToUTC() ? "UTC" : null);
                case LogicalType.DecimalType d -> decimal(d);
                case LogicalType.NullType n -> ArrowType.Null.INSTANCE;
                default -> null;
            };
            case FLOAT -> logical == null ? new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE) : null;
            case DOUBLE -> logical == null ? new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE) : null;
            case BYTE_ARRAY -> switch (logical) {
                case null -> ArrowType.Binary.INSTANCE;
                case LogicalType.StringType s -> ArrowType.Utf8.INSTANCE;
                case LogicalType.EnumType e -> ArrowType.Utf8.INSTANCE;
                case LogicalType.JsonType j -> ArrowType.Utf8.INSTANCE;
                case LogicalType.BsonType b -> ArrowType.Binary.INSTANCE;
                case LogicalType.GeometryType g -> ArrowType.Binary.INSTANCE;
                case LogicalType.GeographyType g -> ArrowType.Binary.INSTANCE;
                case LogicalType.DecimalType d -> decimal(d);
                case LogicalType.NullType n -> ArrowType.Null.INSTANCE;
                default -> null;
            };
            case FIXED_LEN_BYTE_ARRAY -> switch (logical) {
                case null -> new ArrowType.FixedSizeBinary(typeLength);
                case LogicalType.Float16Type f when typeLength == 2 ->
                        new ArrowType.FloatingPoint(FloatingPointPrecision.HALF);
                case LogicalType.UuidType u -> new ArrowType.FixedSizeBinary(16);
                case LogicalType.DecimalType d -> decimal(d);
                default -> null;
            };
            default -> null; // INT96, deprecated and not a type Arrow has
        };
        if (type == null) {
            throw new UnsupportedHardwoodTypeException(where + " has no Arrow type this lane reads");
        }
        return new Leaf(new Field(name, new FieldType(nullable, type, null), null), layers, p.columnIndex());
    }

    private static ArrowType decimal(LogicalType.DecimalType d) {
        return new ArrowType.Decimal(d.precision(), d.scale(), d.precision() > 38 ? 256 : 128);
    }

    private static TimeUnit unit(LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> TimeUnit.MILLISECOND;
            case MICROS -> TimeUnit.MICROSECOND;
            case NANOS -> TimeUnit.NANOSECOND;
        };
    }

    /** The layers Hardwood reports for every leaf must be the ones its schema implies. */
    private static void checkLayers(List<Node> columns, ColumnReaders readers) {
        List<Leaf> leaves = new ArrayList<>();
        columns.forEach(c -> c.collect(leaves));
        for (Leaf leaf : leaves) {
            ColumnReader reader = readers.getColumnReader(leaf.index);
            if (reader.getColumnSchema().columnIndex() != leaf.index) {
                throw new IllegalStateException("Hardwood's column " + leaf.index + " is "
                        + reader.getColumnSchema().fieldPath() + " (column " + reader.getColumnSchema().columnIndex()
                        + "): the projection is not in schema order");
            }
            LayerKind[] kinds = new LayerKind[reader.getLayerCount()];
            for (int k = 0; k < kinds.length; k++) {
                kinds[k] = reader.getLayerKind(k);
            }
            if (!Arrays.asList(kinds).equals(leaf.layers)) {
                throw new UnsupportedHardwoodTypeException(reader.getColumnSchema().fieldPath()
                        + ": Hardwood reports layers " + Arrays.toString(kinds) + " where the schema implies "
                        + leaf.layers);
            }
        }
    }

    // ---- planned nodes, and how each fills one batch ----------------------------------------

    private abstract static class Node {
        final Field field;

        Node(Field field) {
            this.field = field;
        }

        /** The first leaf beneath: its layers carry this node's validity and offsets. */
        abstract Leaf first();

        abstract void collect(List<Leaf> leaves);

        /** Fill {@code vector}'s items {@code [0, n)}, the items of layer {@code layer}. */
        abstract void fill(FieldVector vector, ColumnReaders readers, int layer, int n);
    }

    private static final class Leaf extends Node {
        final List<LayerKind> layers;
        final int index;

        Leaf(Field field, List<LayerKind> layers, int index) {
            super(field);
            this.layers = layers;
            this.index = index;
        }

        @Override
        Leaf first() {
            return this;
        }

        @Override
        void collect(List<Leaf> leaves) {
            leaves.add(this);
        }

        @Override
        void fill(FieldVector vector, ColumnReaders readers, int layer, int n) {
            ColumnReader reader = readers.getColumnReader(index);
            if (reader.getValueCount() != n) {
                throw new IllegalStateException(reader.getColumnSchema().fieldPath() + ": Hardwood returned "
                        + reader.getValueCount() + " values for " + n + " items");
            }
            if (vector.getField().getType() instanceof ArrowType.Null) {
                return; // no values; the parent sets the count
            }
            // One pass in item order: a variable-width vector fills the offsets of the nulls
            // it passes as it goes.
            Validity validity = reader.getLeafValidity();
            switch (reader.getColumnSchema().type()) {
                case BOOLEAN -> {
                    boolean[] v = reader.getBooleans();
                    BitVector out = (BitVector) vector;
                    for (int i = 0; i < n; i++) {
                        if (validity.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, v[i] ? 1 : 0);
                        }
                    }
                }
                case INT32 -> {
                    int[] v = reader.getInts();
                    for (int i = 0; i < n; i++) {
                        if (validity.isNull(i)) {
                            vector.setNull(i);
                        } else {
                            setInt(vector, i, v[i]);
                        }
                    }
                }
                case INT64 -> {
                    long[] v = reader.getLongs();
                    for (int i = 0; i < n; i++) {
                        if (validity.isNull(i)) {
                            vector.setNull(i);
                        } else {
                            setLong(vector, i, v[i]);
                        }
                    }
                }
                case FLOAT -> {
                    float[] v = reader.getFloats();
                    Float4Vector out = (Float4Vector) vector;
                    for (int i = 0; i < n; i++) {
                        if (validity.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, v[i]);
                        }
                    }
                }
                case DOUBLE -> {
                    double[] v = reader.getDoubles();
                    Float8Vector out = (Float8Vector) vector;
                    for (int i = 0; i < n; i++) {
                        if (validity.isNull(i)) {
                            out.setNull(i);
                        } else {
                            out.setSafe(i, v[i]);
                        }
                    }
                }
                case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                    byte[] bytes = reader.getBinaryValues();
                    int[] offsets = reader.getBinaryOffsets();
                    for (int i = 0; i < n; i++) {
                        if (validity.isNull(i)) {
                            vector.setNull(i);
                        } else {
                            setBytes(vector, i, bytes, offsets[i], offsets[i + 1] - offsets[i]);
                        }
                    }
                }
                default -> throw new UnsupportedHardwoodTypeException(reader.getColumnSchema().fieldPath()
                        + ": " + reader.getColumnSchema().type());
            }
        }

        private void setInt(FieldVector vector, int i, int v) {
            switch (vector) {
                case IntVector out -> out.setSafe(i, v);
                case TinyIntVector out -> out.setSafe(i, v);
                case SmallIntVector out -> out.setSafe(i, v);
                case UInt1Vector out -> out.setSafe(i, v);
                case UInt2Vector out -> out.setSafe(i, v);
                case UInt4Vector out -> out.setSafe(i, v);
                case DateDayVector out -> out.setSafe(i, v);
                case TimeMilliVector out -> out.setSafe(i, v);
                case DecimalVector out -> out.setSafe(i, decimal(BigInteger.valueOf(v)));
                case Decimal256Vector out -> out.setSafe(i, decimal(BigInteger.valueOf(v)));
                default -> throw new IllegalStateException("no INT32 setter for " + vector.getField());
            }
        }

        private void setLong(FieldVector vector, int i, long v) {
            switch (vector) {
                case BigIntVector out -> out.setSafe(i, v);
                case UInt8Vector out -> out.setSafe(i, v);
                case TimeStampVector out -> out.setSafe(i, v);
                case TimeMicroVector out -> out.setSafe(i, v);
                case TimeNanoVector out -> out.setSafe(i, v);
                case DecimalVector out -> out.setSafe(i, decimal(BigInteger.valueOf(v)));
                case Decimal256Vector out -> out.setSafe(i, decimal(BigInteger.valueOf(v)));
                default -> throw new IllegalStateException("no INT64 setter for " + vector.getField());
            }
        }

        private void setBytes(FieldVector vector, int i, byte[] bytes, int start, int length) {
            switch (vector) {
                case VariableWidthFieldVector out -> out.setSafe(i, bytes, start, length);
                case FixedSizeBinaryVector out -> out.setSafe(i, Arrays.copyOfRange(bytes, start, start + length));
                case Float2Vector out -> out.setSafe(i, (short) ((bytes[start] & 0xFF) | (bytes[start + 1] << 8)));
                case DecimalVector out ->
                        out.setSafe(i, decimal(new BigInteger(Arrays.copyOfRange(bytes, start, start + length))));
                case Decimal256Vector out ->
                        out.setSafe(i, decimal(new BigInteger(Arrays.copyOfRange(bytes, start, start + length))));
                default -> throw new IllegalStateException("no binary setter for " + vector.getField());
            }
        }

        private BigDecimal decimal(BigInteger unscaled) {
            return new BigDecimal(unscaled, ((ArrowType.Decimal) field.getType()).getScale());
        }
    }

    private static final class StructNode extends Node {
        final boolean optional;
        final List<Node> children;

        StructNode(Field field, boolean optional, List<Node> children) {
            super(field);
            this.optional = optional;
            this.children = children;
        }

        @Override
        Leaf first() {
            return children.get(0).first();
        }

        @Override
        void collect(List<Leaf> leaves) {
            children.forEach(c -> c.collect(leaves));
        }

        @Override
        void fill(FieldVector vector, ColumnReaders readers, int layer, int n) {
            StructVector out = (StructVector) vector;
            Validity validity = optional ? readers.getColumnReader(first().index).getLayerValidity(layer) : null;
            for (int i = 0; i < n; i++) {
                if (validity != null && validity.isNull(i)) {
                    out.setNull(i);
                } else {
                    out.setIndexDefined(i);
                }
            }
            int below = optional ? layer + 1 : layer;
            List<FieldVector> fields = out.getChildrenFromFields();
            for (int c = 0; c < children.size(); c++) {
                children.get(c).fill(fields.get(c), readers, below, n);
            }
        }
    }

    /** A list or map: one REPEATED layer, whose offsets delimit the entries of each item. */
    private abstract static class RepeatedNode extends Node {
        final boolean optional;

        RepeatedNode(Field field, boolean optional) {
            super(field);
            this.optional = optional;
        }

        @Override
        void fill(FieldVector vector, ColumnReaders readers, int layer, int n) {
            ColumnReader reader = readers.getColumnReader(first().index);
            int[] offsets = reader.getLayerOffsets(layer);
            Validity validity = reader.getLayerValidity(layer);
            if (offsets[0] != 0) {
                throw new IllegalStateException(reader.getColumnSchema().fieldPath()
                        + ": a batch's layer offsets start at " + offsets[0]);
            }
            ListVector out = (ListVector) vector;
            for (int i = 0; i < n; i++) {
                if (validity.isNull(i)) {
                    out.setNull(i);
                } else {
                    out.startNewValue(i);
                    out.endValue(i, offsets[i + 1] - offsets[i]);
                }
            }
            fillEntries(out.getDataVector(), readers, layer + 1, offsets[n]);
        }

        abstract void fillEntries(FieldVector data, ColumnReaders readers, int layer, int n);
    }

    private static final class ListNode extends RepeatedNode {
        final Node element;

        ListNode(Field field, boolean optional, Node element) {
            super(field, optional);
            this.element = element;
        }

        @Override
        Leaf first() {
            return element.first();
        }

        @Override
        void collect(List<Leaf> leaves) {
            element.collect(leaves);
        }

        @Override
        void fillEntries(FieldVector data, ColumnReaders readers, int layer, int n) {
            element.fill(data, readers, layer, n);
        }
    }

    private static final class MapNode extends RepeatedNode {
        final Node key;
        final Node value;

        MapNode(Field field, boolean optional, Node key, Node value) {
            super(field, optional);
            this.key = key;
            this.value = value;
        }

        @Override
        Leaf first() {
            return key.first();
        }

        @Override
        void collect(List<Leaf> leaves) {
            key.collect(leaves);
            value.collect(leaves);
        }

        @Override
        void fillEntries(FieldVector data, ColumnReaders readers, int layer, int n) {
            StructVector entries = (StructVector) data;
            for (int i = 0; i < n; i++) {
                entries.setIndexDefined(i);
            }
            List<FieldVector> kv = entries.getChildrenFromFields();
            key.fill(kv.get(0), readers, layer, n);
            value.fill(kv.get(1), readers, layer, n);
        }
    }
}
