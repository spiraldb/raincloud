// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.avrojava;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.MaterializedTable;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
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

class AvroArrowIoTest {
    @TempDir
    Path dir;

    /** A canonical of one non-nullable bigint column, in two batches. */
    private Path canonical(BufferAllocator allocator) throws Exception {
        Path path = dir.resolve("source.arrow");
        Schema schema = new Schema(List.of(new Field("x", FieldType.notNullable(new ArrowType.Int(64, true)), null)));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                ArrowFileWriter writer = new ArrowFileWriter(root, null, channel)) {
            writer.start();
            BigIntVector x = (BigIntVector) root.getVector("x");
            for (int batch = 0; batch < 2; batch++) {
                x.allocateNew(3);
                for (int i = 0; i < 3; i++) {
                    x.set(i, batch * 3 + i);
                }
                root.setRowCount(3);
                writer.writeBatch();
            }
            writer.end();
        }
        return path;
    }

    @Test
    void writesTheSameBytesWithTheFixedMarkerAndReadsThemBack() throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            Path source = canonical(allocator);
            Path first = dir.resolve("first.avro");
            Path second = dir.resolve("second.avro");
            AvroArrowIo.writeAvro(source, first, allocator);
            AvroArrowIo.writeAvro(source, second, allocator);
            byte[] bytes = Files.readAllBytes(first);
            assertArrayEquals(bytes, Files.readAllBytes(second));
            assertEquals("raincloud-avro01", new String(bytes, bytes.length - 16, 16, "US-ASCII"));

            try (BatchSource got = AvroArrowIo.openAvro(first, allocator)) {
                long rows = 0;
                for (MaterializedTable batch = got.next(); batch != null; batch = got.next()) {
                    rows += batch.rowCount;
                }
                assertEquals(6, rows);
                assertNull(got.next());
            }
        }
    }

    @Test
    void aFailedWriteLeavesNoFile() throws Exception {
        try (BufferAllocator allocator = new RootAllocator()) {
            Path missing = dir.resolve("missing.arrow");
            Path output = dir.resolve("out.avro");
            try {
                AvroArrowIo.writeAvro(missing, output, allocator);
            } catch (Exception expected) {
                // the canonical does not exist
            }
            assertFalse(Files.exists(output));
        }
    }
}
