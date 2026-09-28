// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.hardwood;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
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
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.LargeListVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import dev.hardwood.OutputFile;
import dev.hardwood.Validity;
import dev.hardwood.metadata.CompressionCodec;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.metadata.SchemaElement;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ColumnBatch;
import dev.hardwood.writer.ColumnWriter;
import dev.hardwood.writer.ParquetFileWriter;
import dev.hardwood.writer.WriterConfig;
import dev.raincloud.sidecar.common.Knobs;
import dev.raincloud.sidecar.common.VariantFidelity;

/**
 * Arrow → Parquet through Hardwood's columnar writer ({@code ColumnWriter}): the canonical's
 * batches are handed over one at a time as Hardwood's aligned leaf arrays plus the struct
 * validity and list/map offsets of its layer model. Hardwood has no Arrow API, so this class
 * is the bridge; the bytes, encodings, pages and row groups are Hardwood's.
 *
 * <p>Arrow → Parquet types follow the Arrow C++/Rust writers: signed 8/16-bit and every
 * unsigned width as annotated {@code INT32}/{@code INT64}, half floats as
 * {@code FLOAT16}, decimals as {@code INT32}/{@code INT64} up to 18 digits and a minimal
 * {@code FIXED_LEN_BYTE_ARRAY} above, {@code SECOND}-unit times and timestamps promoted to
 * {@code MILLIS} (Parquet has no seconds unit), date64 as {@code DATE} days, the null type as
 * {@code UNKNOWN}, structs, lists (fixed-size and large included) and maps as their standard
 * groups, a struct carrying the {@code arrow.parquet.variant} extension as a Parquet
 * {@code VARIANT} group (see {@link #annotatedSchema}), and top-level dictionaries decoded to their
 * values. Durations, intervals, unions,
 * run-end encoding, list views, nested dictionaries, duplicate sibling names and names with a
 * '.' beneath which a struct, list or map must be addressed (Hardwood addresses those by
 * dotted path) are {@link UnsupportedHardwoodTypeException}. No {@code ARROW:schema} footer
 * is written: the file is what Parquet itself declares.</p>
 */
public final class HardwoodWriter {
    private HardwoodWriter() {}

    /**
     * The writer configuration for the two raw knob values ({@code null} when unset).
     *
     * <p>{@code rowGroupTargetRows} is the row cap exactly: Hardwood cuts a row group at that
     * many records, splitting a batch at the boundary, so the recipe's
     * {@code write.row_group_size_rows} gives the same row-group plan as parquet@rs and
     * parquet@py. Disabled means no row cap (Hardwood's "cut on bytes alone").</p>
     *
     * <p>{@code rowGroupBufferTargetBytes} is Hardwood's only byte target, and it measures
     * what the writer RETAINS for the open row group — level streams, dictionary indices,
     * value stores and dictionaries — rather than the pre-compression encoded size the Python
     * and Rust lanes size groups by. A dictionary-friendly column retains about four bytes a
     * value however wide its values are, so the same knob can give larger or smaller groups
     * than the other lanes'; there is no Hardwood setting that measures encoded bytes.</p>
     */
    public static WriterConfig writerConfig(String maxRows, String targetEncodedBytes) {
        return WriterConfig.builder()
                .codec(CompressionCodec.ZSTD)
                .rowGroupTargetRows(Knobs.count(Knobs.MAX_ROWS, maxRows, Knobs.DEFAULT_MAX_ROWS, Long.MAX_VALUE))
                .rowGroupBufferTargetBytes(Knobs.count(Knobs.TARGET_ENCODED_BYTES, targetEncodedBytes,
                        Knobs.DEFAULT_TARGET_ENCODED_BYTES, Long.MAX_VALUE))
                .build();
    }

    /** Stream the canonical into a zstd Parquet with the knobs from the environment. */
    public static void writeParquet(Path canonical, Path output, BufferAllocator allocator) throws IOException {
        // Resolved before touching the output, so a bad knob leaves nothing behind.
        writeParquet(canonical, output, allocator,
                writerConfig(System.getenv(Knobs.MAX_ROWS), System.getenv(Knobs.TARGET_ENCODED_BYTES)));
    }

    /**
     * {@link #writeParquet(Path, Path, BufferAllocator)} with a resolved configuration.
     *
     * <p>Atomic w.r.t. failure: Hardwood's local output writes a temporary sibling and renames
     * it on {@code close()}; any failure before then discards it, so a rejected or failed
     * write leaves no {@code output} for the harness to promote.</p>
     */
    public static void writeParquet(Path canonical, Path output, BufferAllocator allocator, WriterConfig config)
            throws IOException {
        Files.deleteIfExists(output);
        try (SeekableByteChannel channel = Files.newByteChannel(canonical, StandardOpenOption.READ);
                ArrowFileReader input = new ArrowFileReader(channel, allocator, CommonsCompressionFactory.INSTANCE)) {
            VectorSchemaRoot root = input.getVectorSchemaRoot();
            List<Node> columns = plan(root.getSchema().getFields(), input);
            FileSchema schema = anyVariant(root.getSchema().getFields()) ? annotatedSchema(columns) : declare(columns);
            OutputFile out = OutputFile.of(output);
            ParquetFileWriter writer = ParquetFileWriter.create(out, schema, config);
            boolean finished = false;
            try {
                ColumnWriter batches = writer.columnWriter();
                while (input.loadNextBatch()) {
                    int rows = root.getRowCount();
                    if (rows > 0) {
                        writeBatch(batches, columns, root, input, rows);
                    }
                }
                finished = true;
                writer.close(); // writes the footer and publishes the file; discards it itself on failure
            } finally {
                if (!finished) {
                    // Never close() a writer that failed mid-stream: that would publish the rows
                    // written so far as a valid file.
                    out.discard();
                }
            }
        }
    }

    private static void writeBatch(ColumnWriter batches, List<Node> columns, VectorSchemaRoot root,
            DictionaryProvider dictionaries, int rows) throws IOException {
        int[] all = new int[rows];
        for (int i = 0; i < rows; i++) {
            all[i] = i;
        }
        List<Consumer<ColumnBatch>> setters = new ArrayList<>();
        List<FieldVector> decoded = new ArrayList<>();
        try {
            for (int c = 0; c < columns.size(); c++) {
                FieldVector vector = root.getVector(c);
                if (vector.getField().getDictionary() != null) {
                    Dictionary dictionary = dictionaries.lookup(vector.getField().getDictionary().getId());
                    vector = (FieldVector) DictionaryEncoder.decode(vector, dictionary);
                    decoded.add(vector);
                }
                columns.get(c).emit(vector, all, null, setters);
            }
            batches.writeBatch(batch -> setters.forEach(set -> set.accept(batch)));
        } finally {
            for (FieldVector vector : decoded) {
                vector.close();
            }
        }
    }

    // ---- the Arrow → Parquet plan ----------------------------------------------------------

    private static List<Node> plan(List<Field> fields, DictionaryProvider dictionaries) {
        requireDistinct("the top level", fields);
        int[] leaves = {0};
        List<Node> columns = new ArrayList<>();
        for (Field field : fields) {
            Field values = field;
            if (field.getDictionary() != null) {
                // Arrow Java types a dictionary column by its indices; the values are the column.
                Field valueField = dictionaries.lookup(field.getDictionary().getId()).getVector().getField();
                values = new Field(field.getName(), new FieldType(
                        field.isNullable(), valueField.getType(), null, field.getMetadata()), valueField.getChildren());
            }
            columns.add(node(values, field.getName(), field.getName(), false, leaves));
        }
        return columns;
    }

    private static void requireDistinct(String where, List<Field> fields) {
        Set<String> seen = new HashSet<>();
        for (Field f : fields) {
            if (!seen.add(f.getName())) {
                throw new UnsupportedHardwoodTypeException("duplicate field name \"" + f.getName() + "\" in "
                        + where + ": Hardwood addresses nested columns by name");
            }
        }
    }

    /**
     * @param name the Parquet field name ({@code null} for a list element or map value, which
     *     Hardwood names itself)
     * @param dotted whether a name on the way here contains a '.', which Hardwood's dotted
     *     paths cannot address
     */
    private static Node node(Field field, String name, String path, boolean dotted, int[] leaves) {
        if (field.getDictionary() != null) {
            throw new UnsupportedHardwoodTypeException(path + ": a dictionary inside a nested column");
        }
        boolean variant = VariantFidelity.isVariant(field);
        if (variant) {
            requireVariantStorage(field, path);
        }
        dotted = dotted || (name != null && name.contains("."));
        RepetitionType repetition = field.isNullable() ? RepetitionType.OPTIONAL : RepetitionType.REQUIRED;
        ArrowType type = field.getType();
        switch (type.getTypeID()) {
            case Struct -> {
                if (field.getChildren().isEmpty()) {
                    throw new UnsupportedHardwoodTypeException(path + ": a struct with no fields");
                }
                if (dotted && repetition == RepetitionType.OPTIONAL) {
                    throw dottedPath(path);
                }
                requireDistinct("struct " + path, field.getChildren());
                List<Node> children = new ArrayList<>();
                for (Field child : field.getChildren()) {
                    children.add(node(child, child.getName(), path + "." + child.getName(), dotted, leaves));
                }
                return new StructNode(name, repetition, path, children, variant);
            }
            case List, LargeList, FixedSizeList -> {
                if (dotted) {
                    throw dottedPath(path);
                }
                Node element = node(field.getChildren().get(0), null, path + ".list.element", false, leaves);
                return new ListNode(name, repetition, path, element);
            }
            case Map -> {
                if (dotted) {
                    throw dottedPath(path);
                }
                List<Field> entry = field.getChildren().get(0).getChildren();
                // A Parquet map key is REQUIRED: a null key is refused as a non-nullable field's null.
                Field k = entry.get(0);
                Field requiredKey = new Field(k.getName(), new FieldType(
                        false, k.getType(), k.getDictionary(), k.getMetadata()), k.getChildren());
                Node key = node(requiredKey, null, path + ".key_value.key", false, leaves);
                if (!(key instanceof Leaf keyLeaf)) {
                    throw new UnsupportedHardwoodTypeException(path + ": a map key that is not a primitive");
                }
                Node value = node(entry.get(1), null, path + ".key_value.value", false, leaves);
                return new MapNode(name, repetition, path, keyLeaf, value);
            }
            default -> {
                return leaf(type, name, repetition, path, leaves[0]++);
            }
        }
    }

    /**
     * Refuses an {@code arrow.parquet.variant} field whose storage is not a VARIANT group's: a
     * struct of a binary {@code metadata}, a binary {@code value} and/or a {@code typed_value},
     * and nothing else.
     */
    private static void requireVariantStorage(Field field, String path) {
        if (field.getType().getTypeID() != ArrowType.ArrowTypeID.Struct) {
            throw new UnsupportedHardwoodTypeException(path + ": " + VariantFidelity.EXTENSION
                    + " storage that is not a struct (" + field.getType() + ")");
        }
        boolean metadata = false, values = false;
        for (Field child : field.getChildren()) {
            boolean binary = child.getType().getTypeID() == ArrowType.ArrowTypeID.Binary
                    || child.getType().getTypeID() == ArrowType.ArrowTypeID.LargeBinary
                    || child.getType().getTypeID() == ArrowType.ArrowTypeID.BinaryView;
            switch (child.getName()) {
                case "metadata", "value" -> {
                    if (!binary) {
                        throw new UnsupportedHardwoodTypeException(path + "." + child.getName() + ": VARIANT "
                                + child.getName() + " that is not binary (" + child.getType() + ")");
                    }
                    metadata |= child.getName().equals("metadata");
                    values |= child.getName().equals("value");
                }
                case "typed_value" -> values = true;
                default -> throw new UnsupportedHardwoodTypeException(path + "." + child.getName()
                        + ": a VARIANT group holds only metadata, value and typed_value");
            }
        }
        if (!metadata || !values) {
            throw new UnsupportedHardwoodTypeException(path + ": a VARIANT group needs metadata and a value "
                    + "or typed_value");
        }
    }

    private static UnsupportedHardwoodTypeException dottedPath(String path) {
        return new UnsupportedHardwoodTypeException(path + ": a field name containing '.' above a struct, list "
                + "or map, which Hardwood addresses by dotted path");
    }

    /** The physical encoding of one Arrow leaf type. */
    private enum Conv {
        BOOL, I8, I16, I32, U8, U16, U32, I64, U64, F16, F32, F64, BYTES, FIXED,
        DEC_INT, DEC_LONG, DEC_FIXED, DATE_DAY, DATE_MILLI, TIME_SEC, TIME_MILLI, TIME_MICRO, TIME_NANO,
        TIMESTAMP_SEC, TIMESTAMP, NULL
    }

    private static Leaf leaf(ArrowType type, String name, RepetitionType repetition, String path, int index) {
        PhysicalType p;
        LogicalType logical = null;
        Integer length = null;
        Conv conv;
        switch (type) {
            case ArrowType.Bool b -> {
                p = PhysicalType.BOOLEAN;
                conv = Conv.BOOL;
            }
            case ArrowType.Int t -> {
                p = t.getBitWidth() == 64 ? PhysicalType.INT64 : PhysicalType.INT32;
                if (!t.getIsSigned() || t.getBitWidth() < 32) {
                    logical = new LogicalType.IntType(t.getBitWidth(), t.getIsSigned());
                }
                conv = switch (t.getBitWidth() * (t.getIsSigned() ? 1 : -1)) {
                    case 8 -> Conv.I8;
                    case 16 -> Conv.I16;
                    case 32 -> Conv.I32;
                    case 64 -> Conv.I64;
                    case -8 -> Conv.U8;
                    case -16 -> Conv.U16;
                    case -32 -> Conv.U32;
                    default -> Conv.U64;
                };
            }
            case ArrowType.FloatingPoint f -> {
                switch (f.getPrecision()) {
                    case HALF -> {
                        p = PhysicalType.FIXED_LEN_BYTE_ARRAY;
                        length = 2;
                        logical = new LogicalType.Float16Type();
                        conv = Conv.F16;
                    }
                    case SINGLE -> {
                        p = PhysicalType.FLOAT;
                        conv = Conv.F32;
                    }
                    default -> {
                        p = PhysicalType.DOUBLE;
                        conv = Conv.F64;
                    }
                }
            }
            case ArrowType.Utf8 s -> {
                p = PhysicalType.BYTE_ARRAY;
                logical = new LogicalType.StringType();
                conv = Conv.BYTES;
            }
            case ArrowType.LargeUtf8 s -> {
                p = PhysicalType.BYTE_ARRAY;
                logical = new LogicalType.StringType();
                conv = Conv.BYTES;
            }
            case ArrowType.Utf8View s -> {
                p = PhysicalType.BYTE_ARRAY;
                logical = new LogicalType.StringType();
                conv = Conv.BYTES;
            }
            case ArrowType.Binary b -> {
                p = PhysicalType.BYTE_ARRAY;
                conv = Conv.BYTES;
            }
            case ArrowType.LargeBinary b -> {
                p = PhysicalType.BYTE_ARRAY;
                conv = Conv.BYTES;
            }
            case ArrowType.BinaryView b -> {
                p = PhysicalType.BYTE_ARRAY;
                conv = Conv.BYTES;
            }
            case ArrowType.FixedSizeBinary b -> {
                p = PhysicalType.FIXED_LEN_BYTE_ARRAY;
                length = b.getByteWidth();
                conv = Conv.FIXED;
            }
            case ArrowType.Decimal d -> {
                logical = new LogicalType.DecimalType(d.getScale(), d.getPrecision());
                if (d.getPrecision() <= 9) {
                    p = PhysicalType.INT32;
                    conv = Conv.DEC_INT;
                } else if (d.getPrecision() <= 18) {
                    p = PhysicalType.INT64;
                    conv = Conv.DEC_LONG;
                } else {
                    p = PhysicalType.FIXED_LEN_BYTE_ARRAY;
                    length = decimalBytes(d.getPrecision());
                    conv = Conv.DEC_FIXED;
                }
            }
            case ArrowType.Date d -> {
                p = PhysicalType.INT32;
                logical = new LogicalType.DateType();
                conv = d.getUnit() == DateUnit.DAY ? Conv.DATE_DAY : Conv.DATE_MILLI;
            }
            case ArrowType.Time t -> {
                // isAdjustedToUTC=true, as the Arrow C++ and Rust writers declare times.
                switch (t.getUnit()) {
                    case SECOND, MILLISECOND -> {
                        p = PhysicalType.INT32;
                        logical = new LogicalType.TimeType(true, LogicalType.TimeUnit.MILLIS);
                        conv = t.getUnit() == org.apache.arrow.vector.types.TimeUnit.SECOND
                                ? Conv.TIME_SEC : Conv.TIME_MILLI;
                    }
                    case MICROSECOND -> {
                        p = PhysicalType.INT64;
                        logical = new LogicalType.TimeType(true, LogicalType.TimeUnit.MICROS);
                        conv = Conv.TIME_MICRO;
                    }
                    default -> {
                        p = PhysicalType.INT64;
                        logical = new LogicalType.TimeType(true, LogicalType.TimeUnit.NANOS);
                        conv = Conv.TIME_NANO;
                    }
                }
            }
            case ArrowType.Timestamp t -> {
                p = PhysicalType.INT64;
                LogicalType.TimeUnit unit = switch (t.getUnit()) {
                    case SECOND, MILLISECOND -> LogicalType.TimeUnit.MILLIS;
                    case MICROSECOND -> LogicalType.TimeUnit.MICROS;
                    case NANOSECOND -> LogicalType.TimeUnit.NANOS;
                };
                logical = new LogicalType.TimestampType(t.getTimezone() != null, unit);
                conv = t.getUnit() == org.apache.arrow.vector.types.TimeUnit.SECOND
                        ? Conv.TIMESTAMP_SEC : Conv.TIMESTAMP;
            }
            case ArrowType.Null n -> {
                p = PhysicalType.INT32;
                logical = new LogicalType.NullType();
                repetition = RepetitionType.OPTIONAL;
                conv = Conv.NULL;
            }
            default -> throw new UnsupportedHardwoodTypeException(path + ": Arrow " + type
                    + " has no Parquet type this lane writes");
        }
        return new Leaf(name, repetition, path, index, p, logical, length, conv);
    }

    /** The fewest bytes whose signed range holds every unscaled value of {@code precision} digits. */
    private static int decimalBytes(int precision) {
        BigInteger max = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
        return (max.bitLength() + 1 + 7) / 8; // + the sign bit
    }

    private static boolean anyVariant(List<Field> fields) {
        for (Field f : fields) {
            if (VariantFidelity.isVariant(f) || anyVariant(f.getChildren())) {
                return true;
            }
        }
        return false;
    }

    /**
     * The file schema the plan declares, with its VARIANT groups annotated. Hardwood's schema
     * builders attach no logical type to a group, so the groups are annotated afterwards
     * through Hardwood's other public route to a schema: its elements
     * ({@code toSchemaElements}), each VARIANT group's element given
     * {@code LogicalType.VariantType}, rebuilt by {@code FileSchema.fromSchemaElements}.
     */
    private static FileSchema annotatedSchema(List<Node> columns) {
        List<SchemaElement> elements = new ArrayList<>(declare(columns).toSchemaElements());
        int next = 1; // the root's element comes first
        for (Node column : columns) {
            next = column.annotate(elements, next);
        }
        if (next != elements.size()) {
            throw new IllegalStateException("Hardwood declared " + elements.size() + " schema elements; the plan "
                    + "accounts for " + next);
        }
        return FileSchema.fromSchemaElements(elements);
    }

    /**
     * Checks that {@code elements[at]} is the group a planned node declared, with {@code children}
     * children (and {@code name}, unless Hardwood names it), and returns it.
     */
    private static SchemaElement group(List<SchemaElement> elements, int at, String name, int children, String path) {
        SchemaElement e = at < elements.size() ? elements.get(at) : null;
        if (e == null || !e.isGroup() || (name != null && !name.equals(e.name()))
                || e.numChildren() == null || e.numChildren() != children) {
            throw new IllegalStateException(path + ": Hardwood's schema element " + at + " is " + e
                    + ", not the planned group of " + children);
        }
        return e;
    }

    private static FileSchema declare(List<Node> columns) {
        FileSchema.Builder builder = FileSchema.builder("schema");
        Declare top = new Declare() {
            @Override
            public void leaf(Leaf l) {
                if (l.length != null && l.logical != null) {
                    builder.addColumn(l.name, l.physical, l.repetition, l.length, l.logical);
                } else if (l.length != null) {
                    builder.addColumn(l.name, l.physical, l.repetition, l.length);
                } else if (l.logical != null) {
                    builder.addColumn(l.name, l.physical, l.repetition, l.logical);
                } else {
                    builder.addColumn(l.name, l.physical, l.repetition);
                }
            }

            @Override
            public void struct(StructNode s) {
                builder.struct(s.name, s.repetition, fields -> s.children.forEach(c -> c.declare(in(fields))));
            }

            @Override
            public void list(ListNode l) {
                builder.list(l.name, l.repetition, element -> l.element.declare(in(element)));
            }

            @Override
            public void map(MapNode m) {
                Leaf k = m.key;
                Consumer<FileSchema.ElementBuilder> value = v -> m.value.declare(in(v));
                if (k.length != null) {
                    builder.map(m.name, m.repetition, k.physical, k.length, k.logical, value);
                } else if (k.logical != null) {
                    builder.map(m.name, m.repetition, k.physical, k.logical, value);
                } else {
                    builder.map(m.name, m.repetition, k.physical, value);
                }
            }
        };
        columns.forEach(c -> c.declare(top));
        return builder.build();
    }

    private static Declare in(FileSchema.StructBuilder builder) {
        return new Declare() {
            @Override
            public void leaf(Leaf l) {
                if (l.length != null && l.logical != null) {
                    builder.addColumn(l.name, l.physical, l.repetition, l.length, l.logical);
                } else if (l.length != null) {
                    builder.addColumn(l.name, l.physical, l.repetition, l.length);
                } else if (l.logical != null) {
                    builder.addColumn(l.name, l.physical, l.repetition, l.logical);
                } else {
                    builder.addColumn(l.name, l.physical, l.repetition);
                }
            }

            @Override
            public void struct(StructNode s) {
                builder.struct(s.name, s.repetition, fields -> s.children.forEach(c -> c.declare(in(fields))));
            }

            @Override
            public void list(ListNode l) {
                builder.list(l.name, l.repetition, element -> l.element.declare(in(element)));
            }

            @Override
            public void map(MapNode m) {
                Leaf k = m.key;
                Consumer<FileSchema.ElementBuilder> value = v -> m.value.declare(in(v));
                if (k.length != null) {
                    builder.map(m.name, m.repetition, k.physical, k.length, k.logical, value);
                } else if (k.logical != null) {
                    builder.map(m.name, m.repetition, k.physical, k.logical, value);
                } else {
                    builder.map(m.name, m.repetition, k.physical, value);
                }
            }
        };
    }

    private static Declare in(FileSchema.ElementBuilder builder) {
        return new Declare() {
            @Override
            public void leaf(Leaf l) {
                if (l.length != null && l.logical != null) {
                    builder.primitive(l.physical, l.repetition, l.length, l.logical);
                } else if (l.length != null) {
                    builder.primitive(l.physical, l.repetition, l.length);
                } else if (l.logical != null) {
                    builder.primitive(l.physical, l.repetition, l.logical);
                } else {
                    builder.primitive(l.physical, l.repetition);
                }
            }

            @Override
            public void struct(StructNode s) {
                builder.struct(s.repetition, fields -> s.children.forEach(c -> c.declare(in(fields))));
            }

            @Override
            public void list(ListNode l) {
                builder.list(l.repetition, element -> l.element.declare(in(element)));
            }

            @Override
            public void map(MapNode m) {
                Leaf k = m.key;
                Consumer<FileSchema.ElementBuilder> value = v -> m.value.declare(in(v));
                if (k.length != null) {
                    builder.map(m.repetition, k.physical, k.length, k.logical, value);
                } else if (k.logical != null) {
                    builder.map(m.repetition, k.physical, k.logical, value);
                } else {
                    builder.map(m.repetition, k.physical, value);
                }
            }
        };
    }

    /** Declares one planned node on whichever of Hardwood's three schema builders holds it. */
    private interface Declare {
        void leaf(Leaf l);

        void struct(StructNode s);

        void list(ListNode l);

        void map(MapNode m);
    }

    // ---- planned nodes, and how each hands one batch to Hardwood ---------------------------

    private abstract static class Node {
        final String name;
        final RepetitionType repetition;
        final String path;

        Node(String name, RepetitionType repetition, String path) {
            this.name = name;
            this.repetition = repetition;
            this.path = path;
        }

        abstract void declare(Declare d);

        /**
         * Walk this node's schema elements, from {@code elements[at]} in Hardwood's depth-first
         * order, annotating any VARIANT group; returns the index after them.
         */
        abstract int annotate(List<SchemaElement> elements, int at);

        /**
         * Add the setters for this node's items: the rows {@code positions} of {@code vector},
         * of which {@code absent} (null when none) marks those beneath a null struct.
         */
        abstract void emit(FieldVector vector, int[] positions, boolean[] absent,
                List<Consumer<ColumnBatch>> setters);

        /** Which items are null — absent above, or null here — refusing a null a REQUIRED field holds. */
        boolean[] nulls(FieldVector vector, int[] positions, boolean[] absent) {
            boolean[] nulls = new boolean[positions.length];
            for (int i = 0; i < positions.length; i++) {
                boolean isNull = vector.isNull(positions[i]);
                boolean above = absent != null && absent[i];
                if (isNull && !above && repetition == RepetitionType.REQUIRED) {
                    throw new IllegalArgumentException(path + ": a null in a field Arrow declares non-nullable "
                            + "(item " + positions[i] + ")");
                }
                nulls[i] = above || isNull;
            }
            return nulls;
        }
    }

    private static final class Leaf extends Node {
        final int index;
        final PhysicalType physical;
        final LogicalType logical;
        final Integer length;
        final Conv conv;

        Leaf(String name, RepetitionType repetition, String path, int index, PhysicalType physical,
                LogicalType logical, Integer length, Conv conv) {
            super(name, repetition, path);
            this.index = index;
            this.physical = physical;
            this.logical = logical;
            this.length = length;
            this.conv = conv;
        }

        @Override
        void declare(Declare d) {
            d.leaf(this);
        }

        @Override
        int annotate(List<SchemaElement> elements, int at) {
            return at + 1;
        }

        @Override
        void emit(FieldVector vector, int[] positions, boolean[] absent, List<Consumer<ColumnBatch>> setters) {
            int n = positions.length;
            boolean[] nulls = conv == Conv.NULL ? all(n) : nulls(vector, positions, absent);
            Validity validity = repetition == RepetitionType.OPTIONAL ? Validity.ofNulls(nulls) : null;
            switch (physical) {
                case BOOLEAN -> {
                    boolean[] values = new boolean[n];
                    BitVector v = (BitVector) vector;
                    for (int i = 0; i < n; i++) {
                        values[i] = !nulls[i] && v.get(positions[i]) != 0;
                    }
                    setters.add(validity == null ? b -> b.booleans(index, values) : b -> b.booleans(index, values, validity));
                }
                case INT32 -> {
                    int[] values = new int[n];
                    for (int i = 0; i < n; i++) {
                        if (!nulls[i]) {
                            values[i] = intValue(vector, positions[i]);
                        }
                    }
                    setters.add(validity == null ? b -> b.ints(index, values) : b -> b.ints(index, values, validity));
                }
                case INT64 -> {
                    long[] values = new long[n];
                    for (int i = 0; i < n; i++) {
                        if (!nulls[i]) {
                            values[i] = longValue(vector, positions[i]);
                        }
                    }
                    setters.add(validity == null ? b -> b.longs(index, values) : b -> b.longs(index, values, validity));
                }
                case FLOAT -> {
                    float[] values = new float[n];
                    Float4Vector v = (Float4Vector) vector;
                    for (int i = 0; i < n; i++) {
                        if (!nulls[i]) {
                            values[i] = v.get(positions[i]);
                        }
                    }
                    setters.add(validity == null ? b -> b.floats(index, values) : b -> b.floats(index, values, validity));
                }
                case DOUBLE -> {
                    double[] values = new double[n];
                    Float8Vector v = (Float8Vector) vector;
                    for (int i = 0; i < n; i++) {
                        if (!nulls[i]) {
                            values[i] = v.get(positions[i]);
                        }
                    }
                    setters.add(validity == null ? b -> b.doubles(index, values) : b -> b.doubles(index, values, validity));
                }
                case BYTE_ARRAY -> {
                    byte[][] values = new byte[n][];
                    for (int i = 0; i < n; i++) {
                        // A present binary value must not be null, even beneath a null parent.
                        values[i] = nulls[i] ? EMPTY : ((VariableWidthFieldVector) vector).get(positions[i]);
                    }
                    setters.add(validity == null ? b -> b.bytes(index, values) : b -> b.bytes(index, values, validity));
                }
                case FIXED_LEN_BYTE_ARRAY -> {
                    byte[][] values = new byte[n][];
                    byte[] zero = new byte[length];
                    for (int i = 0; i < n; i++) {
                        values[i] = nulls[i] ? zero : fixedValue(vector, positions[i]);
                    }
                    setters.add(validity == null ? b -> b.fixed(index, values) : b -> b.fixed(index, values, validity));
                }
                default -> throw new IllegalStateException(path + ": no setter for " + physical);
            }
        }

        private static boolean[] all(int n) {
            boolean[] nulls = new boolean[n];
            java.util.Arrays.fill(nulls, true);
            return nulls;
        }

        private int intValue(FieldVector vector, int pos) {
            return switch (conv) {
                case I8 -> ((TinyIntVector) vector).get(pos);
                case I16 -> ((SmallIntVector) vector).get(pos);
                case I32 -> ((IntVector) vector).get(pos);
                case U8 -> ((UInt1Vector) vector).get(pos) & 0xFF;
                case U16 -> ((UInt2Vector) vector).get(pos);
                case U32 -> ((UInt4Vector) vector).get(pos); // the raw bits: Parquet spells a large uint32 negative
                case DEC_INT -> unscaled(vector, pos).intValueExact();
                case DATE_DAY -> ((DateDayVector) vector).get(pos);
                case DATE_MILLI -> {
                    long millis = ((DateMilliVector) vector).get(pos);
                    if (millis % 86_400_000L != 0) {
                        throw new IllegalArgumentException(path + ": date64 " + millis
                                + " ms is not a whole day, which Parquet's DATE cannot hold");
                    }
                    yield Math.toIntExact(Math.floorDiv(millis, 86_400_000L));
                }
                case TIME_SEC -> Math.multiplyExact(((TimeSecVector) vector).get(pos), 1000);
                case TIME_MILLI -> ((TimeMilliVector) vector).get(pos);
                default -> throw new IllegalStateException(path + ": " + conv + " is not an INT32 conversion");
            };
        }

        private long longValue(FieldVector vector, int pos) {
            return switch (conv) {
                case I64 -> ((BigIntVector) vector).get(pos);
                case U64 -> ((UInt8Vector) vector).get(pos); // raw bits, as Parquet spells a large uint64
                case DEC_LONG -> unscaled(vector, pos).longValueExact();
                case TIME_MICRO -> ((TimeMicroVector) vector).get(pos);
                case TIME_NANO -> ((TimeNanoVector) vector).get(pos);
                case TIMESTAMP_SEC -> Math.multiplyExact(((TimeStampVector) vector).get(pos), 1000L);
                case TIMESTAMP -> ((TimeStampVector) vector).get(pos);
                default -> throw new IllegalStateException(path + ": " + conv + " is not an INT64 conversion");
            };
        }

        private byte[] fixedValue(FieldVector vector, int pos) {
            return switch (conv) {
                case FIXED -> ((FixedSizeBinaryVector) vector).get(pos);
                case F16 -> {
                    short bits = ((Float2Vector) vector).get(pos);
                    yield new byte[] {(byte) bits, (byte) (bits >>> 8)}; // little-endian, per FLOAT16
                }
                case DEC_FIXED -> {
                    // Big-endian two's complement, sign-extended to the column's width.
                    byte[] raw = unscaled(vector, pos).toByteArray();
                    byte[] out = new byte[length];
                    byte fill = raw[0] < 0 ? (byte) 0xFF : 0;
                    int pad = length - raw.length;
                    if (pad < 0) {
                        throw new IllegalArgumentException(path + ": an unscaled decimal wider than "
                                + length + " bytes");
                    }
                    java.util.Arrays.fill(out, 0, pad, fill);
                    System.arraycopy(raw, 0, out, pad, raw.length);
                    yield out;
                }
                default -> throw new IllegalStateException(path + ": " + conv + " is not a fixed-length conversion");
            };
        }

        private static BigInteger unscaled(FieldVector vector, int pos) {
            return ((BigDecimal) vector.getObject(pos)).unscaledValue();
        }
    }

    private static final byte[] EMPTY = new byte[0];

    /** The Variant binary encoding version a VARIANT group declares. */
    private static final int VARIANT_SPEC_VERSION = 1;

    private static final class StructNode extends Node {
        final List<Node> children;
        /** Whether this struct is an {@code arrow.parquet.variant} column's storage. */
        final boolean variant;

        StructNode(String name, RepetitionType repetition, String path, List<Node> children, boolean variant) {
            super(name, repetition, path);
            this.children = children;
            this.variant = variant;
        }

        @Override
        void declare(Declare d) {
            d.struct(this);
        }

        /**
         * A VARIANT group's nulls, which Hardwood may refuse: its {@code ColumnBatch.struct} takes
         * validity only for a group it types as a struct. The refusal is Hardwood's measured
         * limit, reported as such; the write fails and leaves no file.
         */
        private void variantValidity(ColumnBatch batch, Validity validity) {
            try {
                batch.struct(path, validity);
            } catch (IllegalArgumentException e) {
                UnsupportedHardwoodTypeException refused = new UnsupportedHardwoodTypeException(path
                        + ": a null VARIANT row, which " + ParquetFileWriter.DEFAULT_CREATED_BY + " cannot write: "
                        + "ColumnBatch.struct takes no validity for a VARIANT group (\"" + e.getMessage() + "\")");
                refused.initCause(e);
                throw refused;
            }
        }

        @Override
        int annotate(List<SchemaElement> elements, int at) {
            SchemaElement e = group(elements, at, name, children.size(), path);
            if (variant) {
                elements.set(at, new SchemaElement(e.name(), e.type(), e.typeLength(), e.repetitionType(),
                        e.numChildren(), e.convertedType(), e.scale(), e.precision(), e.fieldId(),
                        new LogicalType.VariantType(VARIANT_SPEC_VERSION)));
            }
            int next = at + 1;
            for (Node child : children) {
                next = child.annotate(elements, next);
            }
            return next;
        }

        @Override
        void emit(FieldVector vector, int[] positions, boolean[] absent, List<Consumer<ColumnBatch>> setters) {
            boolean[] nulls = nulls(vector, positions, absent);
            if (repetition == RepetitionType.OPTIONAL && any(nulls)) {
                Validity validity = Validity.ofNulls(nulls);
                setters.add(variant ? b -> variantValidity(b, validity) : b -> b.struct(path, validity));
            }
            List<FieldVector> fields = ((StructVector) vector).getChildrenFromFields();
            for (int c = 0; c < children.size(); c++) {
                children.get(c).emit(fields.get(c), positions, nulls, setters);
            }
        }
    }

    private static boolean any(boolean[] flags) {
        for (boolean f : flags) {
            if (f) {
                return true;
            }
        }
        return false;
    }

    /** A list or map: entry offsets over the concatenated entries of its present items. */
    private abstract static class RepeatedNode extends Node {
        RepeatedNode(String name, RepetitionType repetition, String path) {
            super(name, repetition, path);
        }

        @Override
        void emit(FieldVector vector, int[] positions, boolean[] absent, List<Consumer<ColumnBatch>> setters) {
            boolean[] nulls = nulls(vector, positions, absent);
            int n = positions.length;
            int[] offsets = new int[n + 1];
            long total = 0;
            for (int i = 0; i < n; i++) {
                if (!nulls[i]) {
                    total += end(vector, positions[i]) - start(vector, positions[i]);
                }
                if (total > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(path + ": more than " + Integer.MAX_VALUE
                            + " entries in one batch");
                }
                offsets[i + 1] = (int) total;
            }
            // A null list, or one beneath a null struct, carries no entries (a zero delta),
            // whatever Arrow's offsets say.
            int[] entries = new int[(int) total];
            int e = 0;
            for (int i = 0; i < n; i++) {
                if (!nulls[i]) {
                    for (long p = start(vector, positions[i]); p < end(vector, positions[i]); p++) {
                        entries[e++] = Math.toIntExact(p);
                    }
                }
            }
            Validity validity = repetition == RepetitionType.OPTIONAL ? Validity.ofNulls(nulls) : null;
            setters.add(offsets(offsets, validity));
            emitEntries(vector, entries, setters);
        }

        abstract Consumer<ColumnBatch> offsets(int[] offsets, Validity validity);

        abstract void emitEntries(FieldVector vector, int[] entries, List<Consumer<ColumnBatch>> setters);

        static long start(FieldVector vector, int pos) {
            return switch (vector) {
                case LargeListVector l -> l.getElementStartIndex(pos);
                case FixedSizeListVector f -> (long) pos * f.getListSize();
                case ListVector l -> l.getElementStartIndex(pos);
                default -> throw new IllegalStateException("not a list vector: " + vector.getField());
            };
        }

        static long end(FieldVector vector, int pos) {
            return switch (vector) {
                case LargeListVector l -> l.getElementEndIndex(pos);
                case FixedSizeListVector f -> (long) (pos + 1) * f.getListSize();
                case ListVector l -> l.getElementEndIndex(pos);
                default -> throw new IllegalStateException("not a list vector: " + vector.getField());
            };
        }
    }

    private static final class ListNode extends RepeatedNode {
        final Node element;

        ListNode(String name, RepetitionType repetition, String path, Node element) {
            super(name, repetition, path);
            this.element = element;
        }

        @Override
        void declare(Declare d) {
            d.list(this);
        }

        @Override
        int annotate(List<SchemaElement> elements, int at) {
            group(elements, at, name, 1, path);
            group(elements, at + 1, null, 1, path + " (repeated group)");
            return element.annotate(elements, at + 2);
        }

        @Override
        Consumer<ColumnBatch> offsets(int[] offsets, Validity validity) {
            return validity == null ? b -> b.list(path, offsets) : b -> b.list(path, offsets, validity);
        }

        @Override
        void emitEntries(FieldVector vector, int[] entries, List<Consumer<ColumnBatch>> setters) {
            FieldVector data = switch (vector) {
                case LargeListVector l -> l.getDataVector();
                case FixedSizeListVector f -> f.getDataVector();
                case ListVector l -> l.getDataVector();
                default -> throw new IllegalStateException("not a list vector: " + vector.getField());
            };
            element.emit(data, entries, null, setters);
        }
    }

    private static final class MapNode extends RepeatedNode {
        final Leaf key;
        final Node value;

        MapNode(String name, RepetitionType repetition, String path, Leaf key, Node value) {
            super(name, repetition, path);
            this.key = key;
            this.value = value;
        }

        @Override
        void declare(Declare d) {
            d.map(this);
        }

        @Override
        int annotate(List<SchemaElement> elements, int at) {
            group(elements, at, name, 1, path);
            group(elements, at + 1, null, 2, path + " (key_value)");
            return value.annotate(elements, key.annotate(elements, at + 2));
        }

        @Override
        Consumer<ColumnBatch> offsets(int[] offsets, Validity validity) {
            return validity == null ? b -> b.map(path, offsets) : b -> b.map(path, offsets, validity);
        }

        @Override
        void emitEntries(FieldVector vector, int[] entries, List<Consumer<ColumnBatch>> setters) {
            List<FieldVector> kv = ((StructVector) ((ListVector) vector).getDataVector()).getChildrenFromFields();
            key.emit(kv.get(0), entries, null, setters);
            value.emit(kv.get(1), entries, null, setters);
        }
    }
}
