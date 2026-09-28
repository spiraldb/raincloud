// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.parquetjava;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.CanonicalReader;
import dev.raincloud.sidecar.common.LogicalCompare;
import dev.raincloud.sidecar.common.MaterializedTable;
import dev.raincloud.sidecar.common.Verdict;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowReader;

/** Direct coverage for the Arrow⇆Parquet hop the {@code parquet@java} lane depends on. */
class ParquetArrowIoTest {

    @TempDir
    Path tmp;

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        // Fails the test if any vector leaked — the class of bug a round-trip test
        // exists to catch as much as a wrong value.
        allocator.close();
    }

    private Path writeCanonical(int rows) throws IOException {
        Schema schema = new Schema(List.of(
                new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("s", FieldType.nullable(new ArrowType.Utf8()), null)));
        Path path = tmp.resolve("canonical.arrow");
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                SeekableByteChannel out = Files.newByteChannel(path,
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING);
                ArrowFileWriter writer = new ArrowFileWriter(root, null, out)) {
            BigIntVector n = (BigIntVector) root.getVector("n");
            VarCharVector s = (VarCharVector) root.getVector("s");
            n.allocateNew(rows);
            s.allocateNew(rows);
            for (int i = 0; i < rows; i++) {
                if (i % 5 == 0) {
                    n.setNull(i);
                } else {
                    n.setSafe(i, i * 1_000_000_000L);
                }
                s.setSafe(i, ("row-" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
            root.setRowCount(rows);
            writer.start();
            writer.writeBatch();
            writer.end();
        }
        return path;
    }

    private long rows(Path parquet) throws IOException {
        long rows = 0;
        try (BatchSource source = ParquetArrowIo.openParquet(parquet, allocator)) {
            for (MaterializedTable batch = source.next(); batch != null; batch = source.next()) {
                rows += batch.rowCount;
            }
        }
        return rows;
    }

    @Test
    void roundTripsCanonicalArrowThroughParquet() throws IOException {
        Path canonical = writeCanonical(64);
        Path parquet = tmp.resolve("out.parquet");

        ParquetArrowIo.writeParquet(canonical, parquet, allocator);
        assertTrue(Files.size(parquet) > 0, "writer produced no bytes");

        try (BatchSource got = ParquetArrowIo.openParquet(parquet, allocator);
                BatchSource expected = CanonicalReader.open(canonical, allocator)) {
            Verdict verdict = LogicalCompare.compare("parquet@java", got, expected);
            assertEquals("pass", verdict.status, verdict.detail);
        }
        assertEquals(64, rows(parquet));
    }

    @Test
    void writeOverwritesAnExistingOutput() throws IOException {
        Path canonical = writeCanonical(8);
        Path parquet = tmp.resolve("out.parquet");
        // parquet-arrow-java's writer is create-new, so the leftover must be removed
        // for a re-export to succeed rather than failing on the second run.
        Files.write(parquet, new byte[] {1, 2, 3});
        ParquetArrowIo.writeParquet(canonical, parquet, allocator);
        assertEquals(8, rows(parquet));
    }

    @Test
    void failedWriteLeavesNoPartialArtifact() throws IOException {
        // The documented atomicity guarantee: a rejected write must not leave a
        // truncated file for the harness to promote as a successful export.
        Path notArrow = tmp.resolve("garbage.arrow");
        Files.write(notArrow, "this is not an Arrow IPC file".getBytes(
                java.nio.charset.StandardCharsets.UTF_8));
        Path parquet = tmp.resolve("out.parquet");

        assertThrows(Exception.class,
                () -> ParquetArrowIo.writeParquet(notArrow, parquet, allocator));
        assertFalse(Files.exists(parquet), "a failed write left a partial parquet behind");
    }

    // ---- row-group knobs (their grammar is KnobsTest's, in conformance-common) ----

    private long rowGroups(Path parquet) throws IOException {
        try (ParquetArrowReader reader = ParquetArrow.reader(allocator).build(parquet)) {
            while (reader.loadNextBatch()) {
                // drain: the count is known once every group is read
            }
            return reader.rowGroupsCompleted();
        }
    }

    @Test
    void rowGroupKnobs_reachTheWriter() throws IOException {
        Path canonical = writeCanonical(20_000);
        Path defaults = tmp.resolve("defaults.parquet"), small = tmp.resolve("small.parquet"),
                capped = tmp.resolve("capped.parquet");
        ParquetArrowIo.writeParquet(canonical, defaults, allocator, ParquetArrowIo.writeOptions(null, null));
        ParquetArrowIo.writeParquet(canonical, small, allocator, ParquetArrowIo.writeOptions(null, "4096"));
        ParquetArrowIo.writeParquet(canonical, capped, allocator, ParquetArrowIo.writeOptions("1e3", "0"));
        assertEquals(1, rowGroups(defaults));
        assertTrue(rowGroups(small) > 1, "a 4 KiB target left one row group");
        assertEquals(20, rowGroups(capped));
        assertEquals(20_000, rows(small));
    }
}
