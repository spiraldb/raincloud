// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.hardwood;

import static dev.raincloud.sidecar.hardwood.Canonicals.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.TimeStampSecVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.InputFile;
import dev.hardwood.reader.ParquetFileReader;
import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.CanonicalReader;
import dev.raincloud.sidecar.common.LogicalCompare;
import dev.raincloud.sidecar.common.ParquetKnobs;
import dev.raincloud.sidecar.common.Verdict;

/** The Arrow ⇆ Hardwood bridge: types, nested shapes, row groups, failure atomicity. */
class HardwoodIoTest {

    @TempDir
    Path tmp;

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close(); // fails the test on a leaked vector
    }

    private static Field field(String name, ArrowType type) {
        return new Field(name, FieldType.nullable(type), null);
    }

    private Verdict roundTrip(Path canonical) throws IOException {
        Path parquet = tmp.resolve(canonical.getFileName() + ".parquet");
        HardwoodWriter.writeParquet(canonical, parquet, allocator, HardwoodWriter.writerConfig(null, null));
        try (BatchSource expected = CanonicalReader.open(canonical, allocator);
                BatchSource got = HardwoodReader.openParquet(parquet, allocator)) {
            return LogicalCompare.compare("parquet@hardwood", got, expected);
        }
    }

    @Test
    void roundTripsEveryFlatTypeTheLaneMaps() throws IOException {
        Schema schema = new Schema(List.of(
                field("i8", new ArrowType.Int(8, true)), field("i16", new ArrowType.Int(16, true)),
                field("i32", new ArrowType.Int(32, true)), field("i64", new ArrowType.Int(64, true)),
                field("u8", new ArrowType.Int(8, false)), field("u16", new ArrowType.Int(16, false)),
                field("u32", new ArrowType.Int(32, false)), field("u64", new ArrowType.Int(64, false)),
                field("f16", new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)),
                field("f32", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                field("f64", new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)),
                field("b", ArrowType.Bool.INSTANCE), field("s", ArrowType.Utf8.INSTANCE),
                field("ls", ArrowType.LargeUtf8.INSTANCE), field("bin", ArrowType.Binary.INSTANCE),
                field("fixed", new ArrowType.FixedSizeBinary(3)),
                field("d9", new ArrowType.Decimal(9, 2, 128)), field("d18", new ArrowType.Decimal(18, 3, 128)),
                field("d38", new ArrowType.Decimal(38, 4, 128)), field("d50", new ArrowType.Decimal(50, 1, 256)),
                field("day", new ArrowType.Date(DateUnit.DAY)),
                field("us", new ArrowType.Time(TimeUnit.MICROSECOND, 64)),
                field("ns", new ArrowType.Time(TimeUnit.NANOSECOND, 64)),
                field("ts_s", new ArrowType.Timestamp(TimeUnit.SECOND, null)),
                field("ts_utc", new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")),
                field("nothing", ArrowType.Null.INSTANCE)));
        Path canonical = Canonicals.write(tmp.resolve("flat.arrow"), allocator, schema, null, root -> {
            int n = 5;
            for (int i = 0; i < n; i++) {
                if (i == 2) {
                    continue; // row 2 is null in every column
                }
                ((TinyIntVector) root.getVector("i8")).setSafe(i, (byte) (i == 0 ? -128 : 127));
                ((SmallIntVector) root.getVector("i16")).setSafe(i, (short) (i == 0 ? -32768 : i));
                ((IntVector) root.getVector("i32")).setSafe(i, i == 0 ? Integer.MIN_VALUE : i);
                ((BigIntVector) root.getVector("i64")).setSafe(i, i == 0 ? Long.MIN_VALUE : i);
                ((UInt1Vector) root.getVector("u8")).setSafe(i, 255 - i);
                ((UInt2Vector) root.getVector("u16")).setSafe(i, 65535 - i);
                ((UInt4Vector) root.getVector("u32")).setSafe(i, -1 - i); // 4294967295 - i
                ((UInt8Vector) root.getVector("u64")).setSafe(i, -1L - i);
                ((Float2Vector) root.getVector("f16")).setSafe(i, (short) (i == 0 ? 0x8000 : 0x3C00)); // -0.0, 1.0
                ((Float4Vector) root.getVector("f32")).setSafe(i, i == 0 ? Float.NaN : 1.5f * i);
                ((Float8Vector) root.getVector("f64")).setSafe(i, i == 0 ? -0.0 : Math.PI * i);
                ((BitVector) root.getVector("b")).setSafe(i, i % 2);
                ((VarCharVector) root.getVector("s")).setSafe(i, utf8(i == 0 ? "" : "sé" + i));
                ((LargeVarCharVector) root.getVector("ls")).setSafe(i, utf8("large " + i));
                ((VarBinaryVector) root.getVector("bin")).setSafe(i, new byte[] {(byte) i, 0, (byte) 0xFF});
                ((FixedSizeBinaryVector) root.getVector("fixed")).setSafe(i, new byte[] {1, 2, (byte) i});
                ((DecimalVector) root.getVector("d9")).setSafe(i, new BigDecimal(i == 0 ? "-9999999.99" : "1.25"));
                ((DecimalVector) root.getVector("d18")).setSafe(i, new BigDecimal("123456789012345.678"));
                ((DecimalVector) root.getVector("d38")).setSafe(i,
                        new BigDecimal(i == 0 ? "-9999999999999999999999999999999999.9999" : "0.0001"));
                ((Decimal256Vector) root.getVector("d50")).setSafe(i,
                        new BigDecimal("1234567890123456789012345678901234567890123456789.5"));
                ((DateDayVector) root.getVector("day")).setSafe(i, i == 0 ? -719162 : 19000 + i);
                ((TimeMicroVector) root.getVector("us")).setSafe(i, 86_399_999_999L - i);
                ((TimeNanoVector) root.getVector("ns")).setSafe(i, i);
                ((TimeStampSecVector) root.getVector("ts_s")).setSafe(i, i == 0 ? -1 : 1_534_377_600L);
                ((TimeStampMilliTZVector) root.getVector("ts_utc")).setSafe(i, 1_534_377_600_123L);
            }
            for (var v : root.getFieldVectors()) {
                v.setNull(2);
            }
            root.setRowCount(n);
        });
        Verdict v = roundTrip(canonical);
        assertEquals("pass", v.status, v.note + " " + v.detail);
    }

    @Test
    void roundTripsNestedShapesAndDictionaries() throws IOException {
        Field element = field("item", new ArrowType.Int(32, true));
        Field list = new Field("l", FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
        Field inner = new Field("tags", FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(field("item", ArrowType.Utf8.INSTANCE)));
        Field struct = new Field("s", FieldType.nullable(ArrowType.Struct.INSTANCE),
                List.of(new Field("req", FieldType.notNullable(new ArrowType.Int(64, true)), null), inner));
        Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                new Field("key", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
                field("value", new ArrowType.Int(32, true))));
        Field map = new Field("m", FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
        DictionaryEncoding encoding = new DictionaryEncoding(7, false, new ArrowType.Int(8, true));
        Field dict = new Field("d", new FieldType(true, new ArrowType.Int(8, true), encoding), null);

        VarCharVector values = new VarCharVector("values", allocator);
        values.allocateNew();
        values.setSafe(0, utf8("alpha"));
        values.setSafe(1, utf8("beta"));
        values.setValueCount(2);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        provider.put(new Dictionary(values, encoding));
        try {
            Schema schema = new Schema(List.of(list, struct, map, dict));
            Path canonical = Canonicals.write(tmp.resolve("nested.arrow"), allocator, schema, provider, root -> {
                ListVector l = (ListVector) root.getVector("l");
                IntVector items = (IntVector) l.getDataVector();
                // [1, 2], null, [], [null, 3]
                l.startNewValue(0);
                items.setSafe(0, 1);
                items.setSafe(1, 2);
                l.endValue(0, 2);
                l.setNull(1);
                l.startNewValue(2);
                l.endValue(2, 0);
                l.startNewValue(3);
                items.setNull(2);
                items.setSafe(3, 3);
                l.endValue(3, 2);
                items.setValueCount(4);

                StructVector s = (StructVector) root.getVector("s");
                BigIntVector req = (BigIntVector) s.getChild("req");
                ListVector tags = (ListVector) s.getChild("tags");
                VarCharVector tagValues = (VarCharVector) tags.getDataVector();
                int t = 0;
                for (int i = 0; i < 4; i++) {
                    s.setIndexDefined(i);
                    req.setSafe(i, 10L * i);
                    tags.startNewValue(i);
                    for (int k = 0; k <= i % 2; k++) {
                        tagValues.setSafe(t++, utf8("t" + i + k));
                    }
                    tags.endValue(i, i % 2 + 1);
                }
                tagValues.setValueCount(t);
                // Row 1's struct is null although its children hold values: Parquet writes it
                // as absent, and the list beneath carries no entries there.
                s.setNull(1);

                // {}, {k0: 0}, null, {k0: 0, k1: null, k2: 2}
                MapVector m = (MapVector) root.getVector("m");
                StructVector kv = (StructVector) m.getDataVector();
                VarCharVector keys = (VarCharVector) kv.getChild("key");
                IntVector vals = (IntVector) kv.getChild("value");
                int e = 0;
                for (int i = 0; i < 4; i++) {
                    if (i == 2) {
                        m.setNull(i);
                        continue;
                    }
                    m.startNewValue(i);
                    for (int k = 0; k < i; k++) {
                        kv.setIndexDefined(e);
                        keys.setSafe(e, utf8("k" + k));
                        if (k == 1) {
                            vals.setNull(e);
                        } else {
                            vals.setSafe(e, k);
                        }
                        e++;
                    }
                    m.endValue(i, i);
                }
                kv.setValueCount(e);

                TinyIntVector d = (TinyIntVector) root.getVector("d");
                d.setSafe(0, 1);
                d.setSafe(1, 0);
                d.setNull(2);
                d.setSafe(3, 1);
                root.setRowCount(4);
            });
            Verdict v = roundTrip(canonical);
            assertEquals("pass", v.status, v.note + " " + v.detail);
        } finally {
            values.close();
        }
    }

    @Test
    void aNullListCarriesNoEntriesWhateverArrowsOffsetsSay() throws IOException {
        Field list = new Field("l", FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(field("item", new ArrowType.Int(64, true))));
        Path canonical = Canonicals.write(tmp.resolve("offsets.arrow"), allocator, new Schema(List.of(list)), null,
                root -> {
                    ListVector l = (ListVector) root.getVector("l");
                    BigIntVector items = (BigIntVector) l.getDataVector();
                    for (int i = 0; i < 3; i++) {
                        l.startNewValue(i);
                        items.setSafe(2 * i, i);
                        items.setSafe(2 * i + 1, -i);
                        l.endValue(i, 2);
                    }
                    items.setValueCount(6);
                    root.setRowCount(3);
                    // Arrow permits a null list whose offsets still span entries.
                    BitVectorHelper.unsetBit(l.getValidityBuffer(), 1);
                });
        Verdict v = roundTrip(canonical);
        assertEquals("pass", v.status, v.note + " " + v.detail);
    }

    private long[] rowGroups(Path parquet) throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(InputFile.of(parquet))) {
            return reader.getFileMetaData().rowGroups().stream().mapToLong(g -> g.numRows()).toArray();
        }
    }

    @Test
    void theRowCapCutsRowGroupsExactlyAndTheByteTargetReachesTheWriter() throws IOException {
        Path canonical = Canonicals.write(tmp.resolve("rows.arrow"), allocator,
                new Schema(List.of(field("n", new ArrowType.Int(64, true)))), null,
                root -> fill(root, 0, 7_000), root -> fill(root, 7_000, 20_000));
        Path capped = tmp.resolve("capped.parquet"), small = tmp.resolve("small.parquet"),
                defaults = tmp.resolve("defaults.parquet");
        HardwoodWriter.writeParquet(canonical, capped, allocator, HardwoodWriter.writerConfig("1e3", "0"));
        HardwoodWriter.writeParquet(canonical, small, allocator, HardwoodWriter.writerConfig("0", "4096"));
        HardwoodWriter.writeParquet(canonical, defaults, allocator, HardwoodWriter.writerConfig(null, null));
        long[] groups = rowGroups(capped);
        assertEquals(20, groups.length);
        for (long g : groups) {
            assertEquals(1_000, g); // cut at the planned row, across the canonical's batch boundary
        }
        assertTrue(rowGroups(small).length > 1, "a 4 KiB target left one row group");
        assertEquals(1, rowGroups(defaults).length);
    }

    private static void fill(VectorSchemaRoot root, int from, int to) {
        BigIntVector n = (BigIntVector) root.getVector("n");
        for (int i = from; i < to; i++) {
            n.setSafe(i - from, i);
        }
        root.setRowCount(to - from);
    }

    /** Options as the environment would give them: alternating setting names (after
     * {@code RAINCLOUD_PARQUET_}) and values. */
    private static ParquetKnobs knobs(String... pairs) {
        java.util.Map<String, String> vars = new java.util.HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            vars.put("RAINCLOUD_PARQUET_" + pairs[i], pairs[i + 1]);
        }
        return ParquetKnobs.from(vars::get);
    }

    @Test
    void parquetKnobsHardwoodCannotHonourAreRefused() {
        for (ParquetKnobs knobs : new ParquetKnobs[] {
                knobs("STATISTICS", "0"), knobs("PAGE_INDEX", "1"), knobs("PAGE_ROWS", "1000"),
                knobs("COMPRESSION_LEVEL", "3"), knobs("STATISTICS_COLUMNS", "10"),
                knobs("PAGE_INDEX_COLUMNS", "10"), knobs("DICTIONARY_PAGE_BYTES", "65536"),
                knobs("PAGE_CHECKSUMS", "0")}) {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                    () -> HardwoodWriter.writerConfig(null, null, knobs));
            assertTrue(e.getMessage().startsWith("parquet@hardwood cannot honour RAINCLOUD_PARQUET_"),
                    e.getMessage());
        }
        // What it can: every codec, no page index, a page size, dictionaries off, checksums on.
        for (String codec : ParquetKnobs.CODECS) {
            HardwoodWriter.writerConfig(null, null, knobs("COMPRESSION", codec, "PAGE_INDEX", "0",
                    "PAGE_BYTES", "4096", "DICTIONARY", "0", "PAGE_CHECKSUMS", "1"));
        }
    }

    @Test
    void aMalformedKnobIsRefusedNamingTheVariable() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> HardwoodWriter.writerConfig("1_000", null));
        assertTrue(e.getMessage().contains("RAINCLOUD_ROW_GROUP_MAX_ROWS="), e.getMessage());
    }

    @Test
    void anUnwritableTypeIsRefusedBeforeAnythingIsWritten() throws IOException {
        for (ArrowType type : List.of(new ArrowType.Duration(TimeUnit.SECOND),
                new ArrowType.Interval(IntervalUnit.DAY_TIME))) {
            Path canonical = Canonicals.write(tmp.resolve(type.getTypeID() + ".arrow"), allocator,
                    new Schema(List.of(field("x", type))), null, root -> root.setRowCount(0));
            Path parquet = tmp.resolve(type.getTypeID() + ".parquet");
            assertThrows(UnsupportedHardwoodTypeException.class, () -> HardwoodWriter.writeParquet(
                    canonical, parquet, allocator, HardwoodWriter.writerConfig(null, null)));
            assertFalse(Files.exists(parquet));
        }
    }

    @Test
    void aWriteThatFailsMidStreamLeavesNoPartialFile() throws IOException {
        // The second batch holds a date64 that is not a whole day: the first batch is already
        // in Hardwood's writer when the failure arrives, and nothing may be published.
        Path canonical = Canonicals.write(tmp.resolve("date64.arrow"), allocator,
                new Schema(List.of(field("d", new ArrowType.Date(DateUnit.MILLISECOND)))), null,
                root -> {
                    ((DateMilliVector) root.getVector("d")).setSafe(0, 86_400_000L);
                    root.setRowCount(1);
                },
                root -> {
                    ((DateMilliVector) root.getVector("d")).setSafe(0, 1L);
                    root.setRowCount(1);
                });
        Path parquet = tmp.resolve("date64.parquet");
        Files.write(parquet, new byte[] {1, 2, 3}); // a stale artifact from an earlier run
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> HardwoodWriter.writeParquet(
                canonical, parquet, allocator, HardwoodWriter.writerConfig("1", null)));
        assertTrue(e.getMessage().contains("not a whole day"), e.getMessage());
        assertFalse(Files.exists(parquet), "a failed write left a file behind");
        List<Path> left = new ArrayList<>();
        try (var files = Files.list(tmp)) {
            files.filter(p -> p.getFileName().toString().contains("date64.parquet")).forEach(left::add);
        }
        assertEquals(List.of(), left, "Hardwood's temporary sibling was not discarded");
    }

    private Path variantCanonical(String name, boolean nullRow) throws IOException {
        Field storage = new Field("v", new FieldType(true, ArrowType.Struct.INSTANCE, null,
                Map.of("ARROW:extension:name", "arrow.parquet.variant", "__variant_type", "1")), List.of(
                new Field("metadata", FieldType.notNullable(ArrowType.Binary.INSTANCE), null),
                field("value", ArrowType.Binary.INSTANCE)));
        return Canonicals.write(tmp.resolve(name), allocator, new Schema(List.of(storage)), null, root -> {
            StructVector v = (StructVector) root.getVector("v");
            for (int i = 0; i < 2; i++) {
                v.setIndexDefined(i);
                ((VarBinaryVector) v.getChild("metadata")).setSafe(i, new byte[] {1, 0, 0});
                ((VarBinaryVector) v.getChild("value")).setSafe(i, new byte[] {0x0c, (byte) i});
            }
            if (nullRow) {
                v.setNull(1);
            }
            root.setRowCount(2);
        });
    }

    @Test
    void aVariantColumnIsWrittenAsAVariantGroupAndReadBackAsTheExtension() throws IOException {
        Path canonical = variantCanonical("variant.arrow", false);
        Verdict v = roundTrip(canonical);
        assertEquals("pass", v.status, v.note + " " + v.detail);
        Path parquet = tmp.resolve("variant.arrow.parquet");
        assertEquals(java.util.Set.of("v"), HardwoodReader.variantColumns(parquet));
        try (BatchSource got = HardwoodReader.openParquet(parquet, allocator)) {
            assertEquals("arrow.parquet.variant", got.empty().fields.get(0).getMetadata().get("ARROW:extension:name"));
        }
    }

    @Test
    void variantGroupsAreAnnotatedWhereverTheySitAmongListsAndMaps() throws IOException {
        // The annotation walks Hardwood's schema elements in step with the plan: a map and a
        // list before the column, and a VARIANT list element, must all line up.
        Map<String, String> extension = Map.of("ARROW:extension:name", "arrow.parquet.variant");
        List<Field> storage = List.of(field("metadata", ArrowType.Binary.INSTANCE), field("value", ArrowType.Binary.INSTANCE));
        Field entries = new Field("entries", FieldType.notNullable(ArrowType.Struct.INSTANCE), List.of(
                new Field("key", FieldType.notNullable(ArrowType.Utf8.INSTANCE), null),
                field("value", new ArrowType.Int(32, true))));
        Field map = new Field("m", FieldType.nullable(new ArrowType.Map(false)), List.of(entries));
        Field element = new Field("item", new FieldType(true, ArrowType.Struct.INSTANCE, null, extension), storage);
        Field list = new Field("l", FieldType.nullable(ArrowType.List.INSTANCE), List.of(element));
        Field v = new Field("v", new FieldType(true, ArrowType.Struct.INSTANCE, null, extension), storage);
        Path canonical = Canonicals.write(tmp.resolve("nested-variant.arrow"), allocator,
                new Schema(List.of(map, list, v)), null, root -> {
                    MapVector m = (MapVector) root.getVector("m");
                    StructVector kv = (StructVector) m.getDataVector();
                    ListVector l = (ListVector) root.getVector("l");
                    StructVector item = (StructVector) l.getDataVector();
                    StructVector top = (StructVector) root.getVector("v");
                    for (int i = 0; i < 2; i++) {
                        m.startNewValue(i);
                        kv.setIndexDefined(i);
                        ((VarCharVector) kv.getChild("key")).setSafe(i, utf8("k" + i));
                        ((IntVector) kv.getChild("value")).setSafe(i, i);
                        m.endValue(i, 1);
                        l.startNewValue(i);
                        for (StructVector variant : List.of(item, top)) {
                            variant.setIndexDefined(i);
                            ((VarBinaryVector) variant.getChild("metadata")).setSafe(i, new byte[] {1, 0, 0});
                            ((VarBinaryVector) variant.getChild("value")).setSafe(i, new byte[] {0x0c, (byte) i});
                        }
                        l.endValue(i, 1);
                    }
                    kv.setValueCount(2);
                    item.setValueCount(2);
                    root.setRowCount(2);
                });
        Verdict verdict = roundTrip(canonical);
        assertEquals("pass", verdict.status, verdict.note + " " + verdict.detail);
        Path parquet = tmp.resolve("nested-variant.arrow.parquet");
        assertEquals(java.util.Set.of("v"), HardwoodReader.variantColumns(parquet));
        try (ParquetFileReader file = ParquetFileReader.open(InputFile.of(parquet))) {
            dev.hardwood.schema.SchemaNode.GroupNode l = (dev.hardwood.schema.SchemaNode.GroupNode)
                    file.getFileSchema().getRootNode().children().get(1);
            assertTrue(((dev.hardwood.schema.SchemaNode.GroupNode) l.getListElement()).isVariant(), l.toString());
            assertFalse(((dev.hardwood.schema.SchemaNode.GroupNode) file.getFileSchema().getRootNode().children().get(0))
                    .isVariant());
        }
    }

    @Test
    void aNullVariantRowIsHardwoodsRefusalAndLeavesNoFile() throws IOException {
        // Hardwood 1.1.0.Beta1's ColumnBatch.struct takes no validity for a VARIANT group.
        Path canonical = variantCanonical("nulls.arrow", true);
        Path parquet = tmp.resolve("nulls.parquet");
        UnsupportedHardwoodTypeException e = assertThrows(UnsupportedHardwoodTypeException.class,
                () -> HardwoodWriter.writeParquet(canonical, parquet, allocator, HardwoodWriter.writerConfig(null, null)));
        assertTrue(e.getMessage().contains("v: a null VARIANT row, which hardwood version "), e.getMessage());
        assertFalse(Files.exists(parquet), "a refused write left an artifact");
    }
}
