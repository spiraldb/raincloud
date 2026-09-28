// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.vortexjni;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.raincloud.sidecar.common.ReaderMain;

/** The {@code vortex@jni} writer end to end: its reports, its exit codes, what stays on disk. */
class ConformanceWriterTest {

    @TempDir
    Path tmp;

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private Path canonical(String name, Schema schema, DictionaryProvider dictionaries,
            Consumer<VectorSchemaRoot> fill) throws IOException {
        Path path = tmp.resolve(name + ".arrow");
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                SeekableByteChannel out = Files.newByteChannel(path,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                ArrowFileWriter writer = new ArrowFileWriter(root, dictionaries, out)) {
            writer.start();
            for (int batch = 0; batch < 2; batch++) {
                root.allocateNew();
                fill.accept(root);
                writer.writeBatch();
            }
            writer.end();
        }
        return path;
    }

    private String write(Path canonical, Path output) throws IOException {
        Path report = tmp.resolve(output.getFileName() + ".json");
        ConformanceWriter.main(new String[] {"--input", canonical.toString(), "--output", output.toString(),
                "--report", report.toString()});
        return Files.readString(report);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void writesVariantDictionaryAndNestedColumnsThatReadBack() throws IOException {
        Field variant = new Field("v", new FieldType(true, ArrowType.Struct.INSTANCE, null,
                Map.of("ARROW:extension:name", "arrow.parquet.variant", "__variant_type", "1")), List.of(
                new Field("metadata", FieldType.notNullable(ArrowType.Binary.INSTANCE), null),
                new Field("value", FieldType.nullable(ArrowType.Binary.INSTANCE), null)));
        Field list = new Field("l", FieldType.nullable(ArrowType.List.INSTANCE),
                List.of(new Field("item", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        DictionaryEncoding encoding = new DictionaryEncoding(3, false, new ArrowType.Int(8, true));
        Field dict = new Field("d", new FieldType(true, new ArrowType.Int(8, true), encoding), null);
        VarCharVector values = new VarCharVector("values", allocator);
        values.allocateNew();
        values.setSafe(0, utf8("alpha"));
        values.setSafe(1, utf8("beta"));
        values.setValueCount(2);
        DictionaryProvider.MapDictionaryProvider provider = new DictionaryProvider.MapDictionaryProvider();
        provider.put(new Dictionary(values, encoding));
        try {
            Path input = canonical("mixed", new Schema(List.of(variant, list, dict)), provider, root -> {
                StructVector v = (StructVector) root.getVector("v");
                v.setIndexDefined(0);
                ((VarBinaryVector) v.getChild("metadata")).setSafe(0, new byte[] {1, 0, 0});
                ((VarBinaryVector) v.getChild("value")).setSafe(0, new byte[] {0x0c, 1});
                v.setNull(1);
                ListVector l = (ListVector) root.getVector("l");
                BigIntVector items = (BigIntVector) l.getDataVector();
                l.startNewValue(0);
                items.setSafe(0, 5);
                l.endValue(0, 1);
                l.setNull(1);
                items.setValueCount(1);
                TinyIntVector d = (TinyIntVector) root.getVector("d");
                d.setSafe(0, 1);
                d.setNull(1);
                root.setRowCount(2);
            });
            Path output = tmp.resolve("mixed.vortex");
            String report = write(input, output);
            assertTrue(report.startsWith("{\"roundtrip\":true,\"variant_faithful\":false,"), report);
            assertTrue(report.contains("VARIANT annotation not preserved"), report);

            Path read = tmp.resolve("mixed.read.json");
            assertEquals(0, ReaderMain.execute(ConformanceWriter.CELL, "artifact.vortex", new String[] {
                "--input", output.toString(), "--canonical", input.toString(), "--report", read.toString()},
                    ConformanceReader::open, t -> false));
            String verdict = Files.readString(read);
            assertTrue(verdict.startsWith("{\"status\":\"pass\""), verdict);
        } finally {
            values.close();
        }
    }

    @Test
    void aTypeVortexCannotTakeIsAMeasuredFailureWithNoOutput() throws IOException {
        // vortex-data 0.86 has no fixed-size binary: the native writer refuses the schema.
        Schema schema = new Schema(List.of(new Field("f",
                FieldType.nullable(new ArrowType.FixedSizeBinary(3)), null)));
        Path input = canonical("fixed", schema, null, root -> {
            ((FixedSizeBinaryVector) root.getVector("f")).setSafe(0, new byte[] {1, 2, 3});
            root.setRowCount(1);
        });
        Path output = tmp.resolve("fixed.vortex");
        String report = write(input, output);
        assertTrue(report.startsWith("{\"roundtrip\":false,\"variant_faithful\":true,"), report);
        assertTrue(report.contains("caused by"), "the native reason is kept: " + report);
        assertFalse(Files.exists(output), "a refused write left an artifact");
    }

    @Test
    void aMissingOrUnknownOptionIsAUsageErrorWithNoReport() throws IOException {
        Path report = tmp.resolve("usage.json");
        String in = tmp.resolve("in.arrow").toString(), out = tmp.resolve("x.vortex").toString();
        String[][] cases = {
            {"--input", in, "--report", report.toString()},
            {"--output", out, "--report", report.toString()},
            {"--input", in, "--output", out},
            {"--input", in, "--output", out, "--report", report.toString(), "--canonical", in},
        };
        for (String[] args : cases) {
            assertEquals(2, ConformanceWriter.run(args, (input, o, a) -> {
                throw new AssertionError("ran on a usage error");
            }), String.join(" ", args));
            assertFalse(Files.exists(report), "a usage error wrote a report");
        }
    }
}
