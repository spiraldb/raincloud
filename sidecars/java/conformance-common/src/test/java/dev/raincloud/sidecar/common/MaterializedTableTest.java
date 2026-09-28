// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MaterializedTableTest {
    @TempDir Path temp;

    @Test
    void canonicalFileWithNoBatches_retainsSchema() throws Exception {
        Schema schema = new Schema(List.of(new Field("expected",
                FieldType.nullable(new ArrowType.Int(64, true)), null)));
        Path path = temp.resolve("empty.arrow");
        try (RootAllocator allocator = new RootAllocator()) {
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                    FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE);
                    ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
                writer.start();
                writer.end();
            }
            try (BatchSource source = CanonicalReader.open(path, allocator)) {
                MaterializedTable table = source.empty();
                assertEquals(schema.getFields(), table.fields);
                assertEquals(List.of(List.of()), table.columns);
                assertEquals(0, table.rowCount);
                assertNull(source.next());
            }
        }
    }

    @Test
    void zeroColumnBatches_retainIndependentRowCount() {
        try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(), List.of(), 3)) {
            MaterializedTable.Builder builder = new MaterializedTable.Builder();
            builder.initialize(root.getSchema(), null);
            builder.appendBatch(root);
            builder.appendBatch(root);
            assertEquals(6, builder.build().rowCount);
        }
    }

    /** Build a dictionary-encoded column: values are Int indices, dictionary holds the strings. */
    private static VectorSchemaRoot dictionaryRoot(RootAllocator allocator, DictionaryProvider.MapDictionaryProvider provider) {
        VarCharVector dict = new VarCharVector("dict", allocator);
        dict.allocateNew(2);
        dict.setSafe(0, "alpha".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dict.setSafe(1, "beta".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        dict.setValueCount(2);
        DictionaryEncoding encoding = new DictionaryEncoding(7L, false, null);
        provider.put(new Dictionary(dict, encoding));

        IntVector indices = new IntVector(new Field("label",
                new FieldType(true, new ArrowType.Int(32, true), encoding, null), null), allocator);
        indices.allocateNew(2);
        indices.setSafe(0, 1);   // -> "beta"
        indices.setSafe(1, 0);   // -> "alpha"
        indices.setValueCount(2);
        return new VectorSchemaRoot(List.of((FieldVector) indices));
    }

    /**
     * Regression for the false-pass mechanism: with the provider supplied, a dictionary column
     * must materialize its VALUES. Before the fix, ParquetArrowIo recorded the decoded Field but
     * materialized raw indices, and LogicalCompare's numeric coercion could compare index 1 equal
     * to value 1.0 — reporting a pass nothing had verified.
     */
    @Test
    void dictionaryColumn_materializesDecodedValues() {
        try (RootAllocator allocator = new RootAllocator()) {
            DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
            try (VectorSchemaRoot root = dictionaryRoot(allocator, provider)) {
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.initialize(root.getSchema(), provider);
                builder.appendBatch(root, provider);
                MaterializedTable table = builder.build();
                assertEquals(List.of("beta", "alpha"),
                        table.columns.get(0).stream().map(String::valueOf).toList());
                assertTrue(table.fields.get(0).getDictionary() == null,
                        "recorded Field must advertise the decoded value type, not the index type");
            } finally {
                provider.getDictionaryIds().forEach(id -> provider.lookup(id).getVector().close());
            }
        }
    }

    /**
     * The dangerous combination is a provider given to initialize() but withheld from
     * appendBatch(): the Field says "string" while the values are Integer indices. The builder
     * must refuse rather than record a table that compares equal to anything numeric.
     */
    @Test
    void dictionaryRecordedButMaterializedWithoutProvider_isRefused() {
        try (RootAllocator allocator = new RootAllocator()) {
            DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
            try (VectorSchemaRoot root = dictionaryRoot(allocator, provider)) {
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.initialize(root.getSchema(), provider);
                IllegalStateException e = assertThrows(IllegalStateException.class,
                        () -> builder.appendBatch(root));
                assertTrue(e.getMessage().contains("dictionary provider"), e.getMessage());
            } finally {
                provider.getDictionaryIds().forEach(id -> provider.lookup(id).getVector().close());
            }
        }
    }

    /** A batch whose schema differs from the one initialize() saw must be refused, not measured. */
    @Test
    void batchSchemaDrift_isRefused() {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema declared = new Schema(List.of(new Field("v",
                    FieldType.nullable(new ArrowType.Int(64, true)), null)));
            try (VectorSchemaRoot other = VectorSchemaRoot.create(new Schema(List.of(new Field("v",
                    FieldType.nullable(new ArrowType.FloatingPoint(
                            org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null))), allocator)) {
                other.setRowCount(1);
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.initialize(declared, null);
                assertThrows(IllegalStateException.class, () -> builder.appendBatch(other));
            }
        }
    }

    /**
     * The vortex@jni shape: the reader declares DECODED value types up front (a scan-level
     * schema, no provider available yet) while each partition carries dictionary indices.
     * That is legitimate and must be materialized, not refused -- an earlier whole-schema
     * equality guard rejected it, which would have turned a comparator limitation into a
     * catalog-wide conformance failure the moment Vortex exported a dictionary.
     */
    @Test
    void valueTypedSchemaUpFront_thenDictionaryBatches_isAccepted() {
        try (RootAllocator allocator = new RootAllocator()) {
            DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
            try (VectorSchemaRoot root = dictionaryRoot(allocator, provider)) {
                Schema declared = new Schema(List.of(new Field("label",
                        FieldType.nullable(new ArrowType.Utf8()), null)));
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.initialize(declared, null);          // no provider yet, value type
                builder.appendBatch(root, provider);          // indices + provider
                MaterializedTable table = builder.build();
                assertEquals(List.of("beta", "alpha"),
                        table.columns.get(0).stream().map(String::valueOf).toList());
                assertEquals(new ArrowType.Utf8(), table.fields.get(0).getType());
            } finally {
                provider.getDictionaryIds().forEach(id -> provider.lookup(id).getVector().close());
            }
        }
    }

    /** Adopting a decoded type AFTER rows were recorded under another type must be refused. */
    @Test
    void typeChangingAfterRowsRecorded_isRefused() {
        try (RootAllocator allocator = new RootAllocator()) {
            DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
            try (VectorSchemaRoot dict = dictionaryRoot(allocator, provider);
                    VectorSchemaRoot plain = VectorSchemaRoot.create(new Schema(List.of(new Field(
                            "label", FieldType.nullable(new ArrowType.Int(32, true)), null))), allocator)) {
                plain.setRowCount(1);
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.initialize(plain.getSchema(), null);
                builder.appendBatch(plain);                   // rows recorded as Int32
                IllegalStateException e = assertThrows(IllegalStateException.class,
                        () -> builder.appendBatch(dict, provider));
                assertTrue(e.getMessage().contains("changed type after"), e.getMessage());
            } finally {
                provider.getDictionaryIds().forEach(id -> provider.lookup(id).getVector().close());
            }
        }
    }

    /**
     * getObject boxes a dictionary-encoded child of a nested column as raw indices, and
     * only top-level dictionaries are decoded: refuse the column as a comparator gap
     * rather than record indices under a value-typed child Field.
     */
    @Test
    void nestedDictionaryChild_isAComparatorGap() {
        try (RootAllocator allocator = new RootAllocator()) {
            Field child = new Field("c", new FieldType(true, new ArrowType.Int(32, true),
                    new DictionaryEncoding(3L, false, null)), null);
            Field parent = new Field("s", FieldType.nullable(new ArrowType.Struct()), List.of(child));
            Schema declared = new Schema(List.of(new Field("s", FieldType.nullable(new ArrowType.Struct()),
                    List.of(new Field("c", FieldType.nullable(new ArrowType.Utf8()), null)))));
            try (StructVector vector = (StructVector) parent.createVector(allocator);
                    VectorSchemaRoot root = new VectorSchemaRoot(List.of((FieldVector) vector))) {
                root.setRowCount(0);
                for (Schema schema : List.of(declared, root.getSchema())) {
                    MaterializedTable.Builder builder = new MaterializedTable.Builder();
                    builder.initialize(schema, null);
                    ComparatorGap gap = assertThrows(ComparatorGap.class, () -> builder.appendBatch(root));
                    assertTrue(gap.getMessage().contains("s.c"), gap.getMessage());
                }
            }
        }
    }

    /** The invariant covers the whole Field tree, not just the top-level type. */
    @Test
    void nestedChildTypeDrift_isRefused() {
        try (RootAllocator allocator = new RootAllocator()) {
            Schema declared = new Schema(List.of(new Field("s", FieldType.nullable(new ArrowType.Struct()),
                    List.of(new Field("a", FieldType.nullable(new ArrowType.Int(64, true)), null)))));
            Schema actual = new Schema(List.of(new Field("s", FieldType.nullable(new ArrowType.Struct()),
                    List.of(new Field("a", FieldType.nullable(new ArrowType.FloatingPoint(
                            org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE)), null)))));
            try (VectorSchemaRoot root = VectorSchemaRoot.create(actual, allocator)) {
                root.setRowCount(0);
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.initialize(declared, null);
                assertThrows(IllegalStateException.class, () -> builder.appendBatch(root));
            }
        }
    }
}
