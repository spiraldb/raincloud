// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.parquetjava;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.raincloud.sidecar.common.WriterMain;

/** {@link ConformanceWriter} end to end: its reports, its exit codes, and what it leaves on disk. */
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

    private Path canonical(String name, Schema schema, Consumer<VectorSchemaRoot> fill) throws IOException {
        Path path = tmp.resolve(name + ".arrow");
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                SeekableByteChannel out = Files.newByteChannel(path,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                ArrowFileWriter writer = new ArrowFileWriter(root, null, out)) {
            fill.accept(root);
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return path;
    }

    private Path numbers(String name) throws IOException {
        Schema schema = new Schema(List.of(new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        return canonical(name, schema, root -> {
            BigIntVector n = (BigIntVector) root.getVector("n");
            for (int i = 0; i < 16; i++) {
                n.setSafe(i, i);
            }
            root.setRowCount(16);
        });
    }

    private String[] args(Path canonical, Path output) {
        return new String[] {"--input", canonical.toString(), "--output", output.toString(),
                "--report", tmp.resolve(output.getFileName() + ".json").toString()};
    }

    private String runWriter(Path canonical, Path output, WriterMain.SelfVerify verify) throws IOException {
        assertEquals(0, ConformanceWriter.run(args(canonical, output), verify));
        return Files.readString(tmp.resolve(output.getFileName() + ".json"));
    }

    private String runWriter(Path canonical, Path output) throws IOException {
        Path report = tmp.resolve(output.getFileName() + ".json");
        ConformanceWriter.main(args(canonical, output));
        return Files.readString(report);
    }

    @Test
    void writerReportsMeasuredRoundTrip() throws IOException {
        Path output = tmp.resolve("plain.parquet");
        String report = runWriter(numbers("plain"), output);
        assertTrue(report.startsWith("{\"roundtrip\":true,\"variant_faithful\":true,"), report);
        assertTrue(report.contains("round-trips to canonical"), report);
        assertTrue(Files.size(output) > 0);
    }

    @Test
    void writerReportsDroppedVariantAnnotation() throws IOException {
        // raincloud's marker on a column that is not arrow.parquet.variant storage: nothing
        // declares it VARIANT, and the measurement says so.
        Schema schema = new Schema(List.of(new Field("v",
                new FieldType(true, new ArrowType.Int(64, true), null, Map.of("__variant_type", "json")),
                null)));
        Path canonical = canonical("variant", schema, root -> {
            ((BigIntVector) root.getVector("v")).setSafe(0, 7L);
            root.setRowCount(1);
        });
        String report = runWriter(canonical, tmp.resolve("variant.parquet"));
        assertTrue(report.startsWith("{\"roundtrip\":true,\"variant_faithful\":false,"), report);
        assertTrue(report.contains("VARIANT not kept: column \\\"v\\\": the file declares no Parquet VARIANT "
                + "logical type; column \\\"v\\\": read back without the arrow.parquet.variant extension"), report);
    }

    @Test
    void writerKeepsAVariantColumnAsParquetVariant() throws IOException {
        Field storage = new Field("v", new FieldType(true, ArrowType.Struct.INSTANCE, null,
                Map.of("ARROW:extension:name", "arrow.parquet.variant", "__variant_type", "1")), List.of(
                        new Field("metadata", FieldType.nullable(ArrowType.Binary.INSTANCE), null),
                        new Field("value", FieldType.nullable(ArrowType.Binary.INSTANCE), null)));
        Path canonical = canonical("kept", new Schema(List.of(storage)), root -> {
            StructVector v = (StructVector) root.getVector("v");
            v.setIndexDefined(0);
            ((VarBinaryVector) v.getChild("metadata")).setSafe(0, new byte[] {1, 0, 0});
            ((VarBinaryVector) v.getChild("value")).setSafe(0, new byte[] {12, 1});
            v.setNull(1);
            root.setRowCount(2);
        });
        Path output = tmp.resolve("kept.parquet");
        String report = runWriter(canonical, output);
        assertTrue(report.startsWith("{\"roundtrip\":true,\"variant_faithful\":true,"), report);
        assertTrue(report.contains("round-trips; VARIANT kept (v)"), report);
        assertEquals(java.util.Set.of("v"), ParquetArrowIo.variantColumns(output));
    }

    @Test
    void writerRejectsUnsupportedTypeWithoutOutput() throws IOException {
        Schema schema = new Schema(List.of(new Field("i",
                FieldType.nullable(new ArrowType.Interval(IntervalUnit.DAY_TIME)), null)));
        Path canonical = canonical("interval", schema, root -> root.setRowCount(0));
        Path output = tmp.resolve("interval.parquet");
        String report = runWriter(canonical, output);
        assertTrue(report.startsWith("{\"roundtrip\":false,\"variant_faithful\":true,"), report);
        assertTrue(report.contains("unsupported type"), report);
        assertFalse(Files.exists(output), "a rejected write left an artifact");
    }

    @Test
    void writerReportsComparatorGapAsUnmeasured() throws IOException {
        // Time32(SECOND) is stored as TIME(MILLIS) and read back as Time32(MILLISECOND).
        // The JVM comparator does not judge time-unit changes (the time32 / time64 row of
        // the table in sidecars/README.md), so the round-trip is unmeasured (null), never
        // a measured failure. If that gap is ever closed, drive this with another row of
        // that table.
        Schema schema = new Schema(List.of(new Field("t",
                FieldType.nullable(new ArrowType.Time(TimeUnit.SECOND, 32)), null)));
        Path canonical = canonical("time", schema, root -> {
            ((TimeSecVector) root.getVector("t")).setSafe(0, 5);
            root.setRowCount(1);
        });
        Path output = tmp.resolve("time.parquet");
        String report = runWriter(canonical, output);
        assertTrue(report.startsWith("{\"roundtrip\":null,\"variant_faithful\":true,"), report);
        assertTrue(report.contains("self-verify unmeasured"), report);
        assertTrue(Files.exists(output));
    }

    @Test
    void anExhaustedComparatorIsUnmeasuredAndKeepsTheFile() throws IOException {
        List<Throwable> limits = List.of(new OutOfMemoryError("Java heap space"),
                new OutOfMemoryException("Unable to allocate buffer"));
        for (Throwable limit : limits) {
            Path output = tmp.resolve(limit.getClass().getSimpleName() + ".parquet");
            String report = runWriter(numbers(limit.getClass().getSimpleName()), output, (input, out, a) -> {
                if (limit instanceof Error error) {
                    throw error;
                }
                throw (RuntimeException) limit;
            });
            assertTrue(report.startsWith("{\"roundtrip\":null,"), report);
            assertTrue(report.contains("comparator resource limit"), report);
            assertTrue(Files.size(output) > 0, "an unmeasured artifact was removed");
        }
    }

    @Test
    void aFailedSelfVerifyIsMeasuredAndKeepsTheFile() throws IOException {
        Path output = tmp.resolve("broken.parquet");
        String report = runWriter(numbers("broken"), output, (input, out, a) -> {
            throw new IOException("re-read failed");
        });
        assertTrue(report.startsWith("{\"roundtrip\":false,"), report);
        assertTrue(report.contains("IOException: re-read failed"), report);
        assertTrue(Files.exists(output), "the file stays; the harness refuses to promote it");
    }

    @Test
    void aMissingOptionIsAUsageErrorWithNoReport() throws IOException {
        Path canonical = numbers("usage");
        Path report = tmp.resolve("usage.json");
        String[][] cases = {
            {"--input", canonical.toString(), "--report", report.toString()},
            {"--output", tmp.resolve("x.parquet").toString(), "--report", report.toString()},
            {"--input", canonical.toString(), "--output", tmp.resolve("x.parquet").toString()},
            {"--input", canonical.toString(), "--output", tmp.resolve("x.parquet").toString(),
                "--report", report.toString(), "--extra", "1"},
        };
        for (String[] args : cases) {
            assertEquals(2, ConformanceWriter.run(args, (input, out, a) -> {
                throw new AssertionError("ran on a usage error");
            }), String.join(" ", args));
            assertFalse(Files.exists(report), "a usage error wrote a report");
        }
    }
}
