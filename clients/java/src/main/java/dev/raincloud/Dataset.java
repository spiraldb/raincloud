// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import com.fasterxml.jackson.core.type.TypeReference;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Immutable catalog selection with explicit native lifetime. Use try-with-resources.
 * Creating a handle loads metadata only. path/schema/batches may download bytes.
 * Each batch reader owns its stream independently of this handle. Its vectors are
 * valid until the next loadNextBatch() or close(); retain/copy them explicitly if needed.
 *
 * <p>Resolution runs in the raincloud CLI; reads use Raincloud's Rust engine through
 * Arrow's C stream JNI bridge. Creating a handle and each path/schema/batches call
 * run one {@code raincloud} process and block until it exits; it inherits this JVM's
 * environment, working directory (relative settings resolve against it) and stderr,
 * and may wait on the store's download lock with no timeout. Calls on one handle may
 * run concurrently; {@link #close()} waits for the ones in flight.
 */
public final class Dataset implements AutoCloseable {
    /**
     * Lowest native ABI this client can drive: the version that introduced the newest
     * entry point it calls. Native ABI changes are additive (see raincloud.h), so any
     * library at this version or newer satisfies this client.
     */
    private static final int REQUIRED_ABI = 1;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    /** Volatile so a handle published to another thread without a lock is still seen. */
    private volatile Pointer handle;
    Dataset(String slug, String format, String options) {
        int abi = NativeApi.INSTANCE.raincloud_abi_version();
        if (abi < REQUIRED_ABI) {
            throw new IllegalStateException(
                    "Raincloud native ABI " + abi + " is older than the required " + REQUIRED_ABI);
        }
        var out = new PointerByReference(); var error = new NativeApi.Failure();
        NativeApi.check(NativeApi.INSTANCE.raincloud_open(options, slug, format, out, error), error);
        handle = out.getValue();
    }
    /** The open handle, under the read lock the caller holds. */
    private Pointer open() { if (handle == null) throw new IllegalStateException("dataset is closed"); return handle; }
    public Map<String, Object> metadata() {
        lock.readLock().lock();
        try {
            var out = new PointerByReference(); var error = new NativeApi.Failure();
            NativeApi.check(NativeApi.INSTANCE.raincloud_metadata(open(), out, error), error);
            try { return Raincloud.JSON.readValue(out.getValue().getString(0,"UTF-8"), new TypeReference<>() {}); }
            catch (IOException e) { throw new IllegalStateException("invalid native metadata", e); }
            finally { NativeApi.INSTANCE.raincloud_string_free(out.getValue()); }
        } finally { lock.readLock().unlock(); }
    }
    /** The selected representation, e.g. {@code vortex}. */
    public String format() { return (String) metadata().get("format"); }
    public Path path() {
        lock.readLock().lock();
        try {
            var out = new PointerByReference(); var error = new NativeApi.Failure();
            NativeApi.check(NativeApi.INSTANCE.raincloud_path(open(), out, error), error);
            try { return Path.of(out.getValue().getString(0,"UTF-8")); }
            finally { NativeApi.INSTANCE.raincloud_string_free(out.getValue()); }
        } finally { lock.readLock().unlock(); }
    }
    public ArrowReader batches(BufferAllocator allocator) { return batches(allocator, 65536); }
    public ArrowReader batches(BufferAllocator allocator, int batchSize) {
        if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be positive");
        lock.readLock().lock();
        try (ArrowArrayStream stream = ArrowArrayStream.allocateNew(allocator)) {
            var error = new NativeApi.Failure();
            NativeApi.check(NativeApi.INSTANCE.raincloud_batches(open(), new NativeApi.Size(batchSize), new Pointer(stream.memoryAddress()), error), error);
            try { return Data.importArrayStream(allocator, stream, false); }
            finally {
                // A successful import takes the stream and clears its release callback, and a
                // native error leaves the stream empty (no callback). Only a failed import
                // leaves a live stream behind, which must be released here.
                if (stream.snapshot().release != 0) stream.release();
            }
        } finally { lock.readLock().unlock(); }
    }
    public Schema schema(BufferAllocator allocator) throws IOException {
        try (ArrowReader reader = batches(allocator)) { return reader.getVectorSchemaRoot().getSchema(); }
    }
    @Override public void close() {
        lock.writeLock().lock();
        try { if (handle != null) { NativeApi.INSTANCE.raincloud_close(handle); handle = null; } }
        finally { lock.writeLock().unlock(); }
    }
}
