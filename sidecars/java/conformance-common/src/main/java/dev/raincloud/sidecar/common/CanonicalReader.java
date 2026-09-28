// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;

/**
 * Reads the canonical {@code <slug>.arrow.zstd} — a standard Apache Arrow IPC
 * *file* whose record-batch bodies are zstd-compressed INSIDE the IPC stream (NOT
 * an outer zstd wrapper). The {@link CommonsCompressionFactory} MUST be passed to
 * the 3-arg {@link ArrowFileReader} constructor: the 2-arg default is
 * {@code NoCompressionCodec.Factory}, which throws on the zstd bodies.
 */
public final class CanonicalReader {
    private CanonicalReader() {}

    /** The canonical's batches; the returned source owns the open file. */
    public static BatchSource open(Path canonical, BufferAllocator allocator) throws IOException {
        SeekableByteChannel channel = Files.newByteChannel(canonical, StandardOpenOption.READ);
        try {
            return BatchSource.of(new ArrowFileReader(channel, allocator, CommonsCompressionFactory.INSTANCE));
        } catch (IOException | RuntimeException e) {
            try {
                channel.close();
            } catch (IOException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
    }
}
