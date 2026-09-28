// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The round-trip verdict an out-of-memory error becomes, writing or self-verifying, the measured
 * {@code variant_faithful}, and a leak's note, for every JVM writer lane.
 */
class WriterMainTest {
    private static final String CELL = "x@jvm";
    private static final String ARRAY_LIMIT = "Requested array size exceeds VM limit";

    @TempDir
    Path tmp;

    private Path canonical() throws IOException {
        Path path = tmp.resolve("canonical.arrow");
        Schema schema = new Schema(List.of(new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                SeekableByteChannel out = Files.newByteChannel(path,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                ArrowFileWriter writer = new ArrowFileWriter(root, null, out)) {
            ((BigIntVector) root.getVector("n")).setSafe(0, 1L);
            root.setRowCount(1);
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return path;
    }

    private static Exception wrapped(Error error) {
        return new RuntimeException("decode", new IOException("read", error));
    }

    /** The report of a lane whose writer throws {@code writing}, or else whose self-verify throws {@code verifying}. */
    private String report(Throwable writing, Throwable verifying) throws IOException {
        Path canonical = canonical();
        Path output = tmp.resolve("out");
        Path report = tmp.resolve("report.json");
        int code = WriterMain.execute(CELL, (out, columns, allocator) -> "variant lost", new String[] {"--input", canonical.toString(),
                "--output", output.toString(), "--report", report.toString()},
                (input, out, allocator) -> {
                    if (writing != null) {
                        throw (Exception) (writing instanceof Error e ? new RuntimeException(e) : writing);
                    }
                    Files.writeString(out, "written");
                },
                (input, out, allocator) -> {
                    if (verifying instanceof Error e) {
                        throw e;
                    }
                    throw (Exception) verifying;
                },
                t -> false);
        assertEquals(0, code, "a writer that ran exits 0, whatever it found");
        String json = Files.readString(report);
        Files.delete(report);
        Files.delete(canonical);
        Files.deleteIfExists(output);
        return json;
    }

    @Test
    void anArrayPastTheJvmLimitIsTheImplementationsFailure() throws IOException {
        String write = report(new OutOfMemoryError(ARRAY_LIMIT), null);
        assertTrue(write.startsWith("{\"roundtrip\":false,"), write);
        assertTrue(write.contains("\"note\":\"x@jvm: while writing: requested an array past the JVM's array size limit"),
                write);
        for (Throwable verify : List.of(new OutOfMemoryError(ARRAY_LIMIT), wrapped(new OutOfMemoryError(ARRAY_LIMIT)))) {
            String read = report(null, verify);
            assertTrue(read.startsWith("{\"roundtrip\":false,"), read);
            assertTrue(read.contains("\"note\":\"x@jvm: self-verify fail: read error: requested an array past "
                    + "the JVM's array size limit"), read);
            assertTrue(read.contains(ARRAY_LIMIT), read);
        }
    }

    /** A canonical of one VARIANT-marked struct column {@code v}, as raincloud's canonicals carry it. */
    private Path variantCanonical() throws IOException {
        Path path = tmp.resolve("variant.arrow");
        Field binary = new Field("metadata", FieldType.nullable(ArrowType.Binary.INSTANCE), null);
        Field value = new Field("value", FieldType.nullable(ArrowType.Binary.INSTANCE), null);
        Field v = new Field("v", new FieldType(true, ArrowType.Struct.INSTANCE, null,
                java.util.Map.of("ARROW:extension:name", "arrow.parquet.variant", "__variant_type", "1")),
                List.of(binary, value));
        Schema schema = new Schema(List.of(v));
        try (RootAllocator allocator = new RootAllocator();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                SeekableByteChannel out = Files.newByteChannel(path,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                ArrowFileWriter writer = new ArrowFileWriter(root, null, out)) {
            root.setRowCount(0);
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return path;
    }

    /** The report of a lane that writes {@code canonical} and self-verifies as a pass. */
    private String passing(Path canonical, VariantFidelity.Check variant, WriterMain.Writer writer)
            throws IOException {
        Path output = tmp.resolve("out");
        Path report = tmp.resolve("report.json");
        int code = WriterMain.execute(CELL, variant, new String[] {"--input", canonical.toString(),
                "--output", output.toString(), "--report", report.toString()}, writer,
                (input, out, allocator) -> Verdict.pass("ok"), t -> false);
        assertEquals(0, code);
        String json = Files.readString(report);
        Files.delete(report);
        Files.deleteIfExists(output);
        return json;
    }

    private static final WriterMain.Writer WRITES = (input, out, allocator) -> Files.writeString(out, "written");

    @Test
    void variantFaithfulIsWhatTheLanesCheckMeasures() throws IOException {
        Path canonical = variantCanonical();
        List<List<String>> asked = new java.util.ArrayList<>();
        String kept = passing(canonical, (out, columns, allocator) -> {
            asked.add(columns);
            return null;
        }, WRITES);
        assertEquals(List.of(List.of("v")), asked, "the check is asked about the canonical's VARIANT columns");
        assertTrue(kept.startsWith("{\"roundtrip\":true,\"variant_faithful\":true,"), kept);
        assertTrue(kept.contains("round-trips; VARIANT kept (v)"), kept);

        String lost = passing(canonical, (out, columns, allocator) -> "column \"v\": no logical type", WRITES);
        assertTrue(lost.startsWith("{\"roundtrip\":true,\"variant_faithful\":false,"), lost);
        assertTrue(lost.contains("round-trips; VARIANT not kept: column \\\"v\\\": no logical type"), lost);

        String unmeasured = passing(canonical, (out, columns, allocator) -> {
            throw new IOException("footer unreadable");
        }, WRITES);
        assertTrue(unmeasured.contains("\"variant_faithful\":false,"), unmeasured);
        assertTrue(unmeasured.contains("VARIANT unmeasured: IOException: footer unreadable"), unmeasured);
    }

    @Test
    void withoutVariantColumnsNothingIsMeasured() throws IOException {
        String json = passing(canonical(), (out, columns, allocator) -> {
            throw new AssertionError("asked about " + columns);
        }, WRITES);
        assertTrue(json.startsWith("{\"roundtrip\":true,\"variant_faithful\":true,"), json);
        assertTrue(json.contains("\"note\":\"x@jvm: round-trips to canonical\""), json);
    }

    @Test
    void aLeakIsNotedAndLeavesTheVerdict() throws IOException {
        String json = passing(canonical(), (out, columns, allocator) -> null, (input, out, allocator) -> {
            allocator.buffer(64); // never released
            Files.writeString(out, "written");
        });
        assertTrue(json.startsWith("{\"roundtrip\":true,"), json);
        assertTrue(json.contains("x@jvm: round-trips to canonical; memory leak: allocator ROOT still held 64 B at close"),
                json);
    }

    @Test
    void aHeapShortageInTheSelfVerifyIsUnmeasured() throws IOException {
        for (Throwable verify : List.of(new OutOfMemoryError("Java heap space"),
                wrapped(new OutOfMemoryError("Java heap space")))) {
            String read = report(null, verify);
            assertTrue(read.startsWith("{\"roundtrip\":null,"), read);
            assertTrue(read.contains("comparator resource limit, out of memory; raise -Xmx"), read);
        }
    }
}
