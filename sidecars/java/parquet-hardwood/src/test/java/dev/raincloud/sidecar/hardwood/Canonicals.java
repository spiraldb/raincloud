// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.hardwood;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.Consumer;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

/** Canonical Arrow IPC files (zstd bodies, as a build writes them) for the lane's tests. */
final class Canonicals {
    private Canonicals() {}

    /** One file of the given batches, each filled by one {@code fills} entry. */
    @SafeVarargs
    static Path write(Path path, BufferAllocator allocator, Schema schema, DictionaryProvider dictionaries,
            Consumer<VectorSchemaRoot>... fills) throws IOException {
        try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
                SeekableByteChannel out = Files.newByteChannel(path,
                        StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                ArrowFileWriter writer = new ArrowFileWriter(root, dictionaries, out, null, IpcOption.DEFAULT,
                        CommonsCompressionFactory.INSTANCE, CompressionUtil.CodecType.ZSTD)) {
            writer.start();
            for (Consumer<VectorSchemaRoot> fill : fills) {
                root.allocateNew();
                fill.accept(root);
                writer.writeBatch();
            }
            writer.end();
        }
        return path;
    }

    /** {@code n} int64 rows 0..n-1, one batch. */
    static Path numbers(Path path, BufferAllocator allocator, int n) throws IOException {
        Schema schema = new Schema(List.of(new Field("n", FieldType.nullable(new ArrowType.Int(64, true)), null)));
        return write(path, allocator, schema, null, root -> {
            BigIntVector v = (BigIntVector) root.getVector("n");
            for (int i = 0; i < n; i++) {
                v.setSafe(i, i);
            }
            root.setRowCount(n);
        });
    }

    static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
