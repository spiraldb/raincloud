// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.Predicate;

import org.apache.arrow.memory.OutOfMemoryException;
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

/** The verdict each kind of reader failure becomes, and the exit codes, for every JVM reader lane. */
class ReaderMainTest {
    private static final String CELL = "x@jvm";

    @TempDir
    Path tmp;

    /** Thrown by a lane's own "cannot represent this type" exception. */
    private static final class LaneGap extends RuntimeException {
        LaneGap(String message) {
            super(message);
        }
    }

    private static final Predicate<Throwable> LANE_GAP = t -> t instanceof LaneGap;

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

    private String[] args(Path canonical, Path report) {
        return new String[] {"--input", tmp.resolve("artifact").toString(), "--canonical", canonical.toString(),
                "--report", report.toString()};
    }

    private String verdict(Throwable failure) throws IOException {
        Path report = tmp.resolve(failure.getClass().getSimpleName() + ".json");
        int code = ReaderMain.execute(CELL, "artifact", args(canonical(), report), (input, allocator) -> {
            if (failure instanceof Error error) {
                throw error;
            }
            throw (Exception) failure;
        }, LANE_GAP);
        assertEquals(0, code, "a reader that ran exits 0, whatever it found");
        String json = Files.readString(report);
        Files.delete(report);
        Files.delete(tmp.resolve("canonical.arrow"));
        return json;
    }

    @Test
    void eachFailureBecomesItsVerdict() throws IOException {
        String gap = verdict(new ComparatorGap("dictionary inside a list"));
        assertTrue(gap.startsWith("{\"status\":\"skip\",\"note\":\"x@jvm: comparator gap (unsupported shape)\""), gap);
        String laneGap = verdict(new LaneGap("INTERVAL"));
        assertTrue(laneGap.startsWith("{\"status\":\"skip\",\"note\":\"x@jvm: comparator gap (unsupported type)\""),
                laneGap);
        for (Throwable limit : List.of(new OutOfMemoryError("Java heap space"),
                new OutOfMemoryException("Unable to allocate buffer"),
                // parquet-arrow-java wraps a decode OOM twice; it is still a resource limit.
                new RuntimeException("corrupt parquet", new IOException("read", new OutOfMemoryError("Java heap space"))))) {
            String oom = verdict(limit);
            assertTrue(oom.startsWith("{\"status\":\"skip\",\"note\":\"x@jvm: comparator resource limit (out of memory)\""),
                    oom);
        }
        // An array past the JVM's limit is the implementation's failure, however it is wrapped.
        for (Throwable limit : List.of(new OutOfMemoryError("Requested array size exceeds VM limit"),
                new RuntimeException("decode", new OutOfMemoryError("Requested array size exceeds VM limit")))) {
            String array = verdict(limit);
            assertTrue(array.startsWith("{\"status\":\"fail\",\"note\":\"x@jvm: read error: requested an array "
                    + "past the JVM's array size limit\""), array);
            assertTrue(array.contains("Requested array size exceeds VM limit"), array);
        }
        String error = verdict(new IOException("truncated footer"));
        assertTrue(error.startsWith("{\"status\":\"fail\",\"note\":\"x@jvm: read error\""), error);
        assertTrue(error.contains("IOException: truncated footer"), error);
    }

    @Test
    void aMissingOrUnknownOptionIsAUsageErrorWithNoReport() throws IOException {
        Path canonical = canonical();
        Path report = tmp.resolve("usage.json");
        String[][] cases = {
            {"--canonical", canonical.toString(), "--report", report.toString()},
            {"--input", "a", "--report", report.toString()},
            {"--input", "a", "--canonical", canonical.toString()},
            {"--input", "a", "--canonical", canonical.toString(), "--report", report.toString(), "--x", "1"},
            {"--input", "a", "--canonical", canonical.toString(), "--report", report.toString(), "stray"},
        };
        for (String[] args : cases) {
            int code = ReaderMain.execute(CELL, "artifact", args, (input, allocator) -> {
                throw new AssertionError("ran on a usage error");
            }, LANE_GAP);
            assertEquals(2, code, String.join(" ", args));
            assertFalse(Files.exists(report), "a usage error wrote a report");
        }
    }

    @Test
    void anUnwritableReportExitsOne() throws IOException {
        Path canonical = canonical();
        String[] args = args(canonical, tmp.resolve("missing-dir").resolve("report.json"));
        assertEquals(1, ReaderMain.execute(CELL, "artifact", args,
                (input, allocator) -> CanonicalReader.open(canonical, allocator), LANE_GAP));
    }

    @Test
    void aLeakIsNotedAndLeavesTheVerdict() throws IOException {
        Path canonical = canonical();
        Path report = tmp.resolve("leak.json");
        int code = ReaderMain.execute(CELL, "artifact", args(canonical, report), (input, allocator) -> {
            allocator.buffer(32); // never released
            return CanonicalReader.open(canonical, allocator);
        }, LANE_GAP);
        assertEquals(0, code);
        String json = Files.readString(report);
        assertTrue(json.startsWith("{\"status\":\"pass\","), json);
        assertTrue(json.contains("memory leak: allocator ROOT still held 32 B at close"), json);
    }
}
