// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.vortexjni;

import java.io.IOException;
import java.lang.ref.Reference;
import java.nio.file.Path;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.MaterializedTable;
import dev.raincloud.sidecar.common.ReaderMain;
import dev.vortex.api.DataSource;
import dev.vortex.api.Scan;
import dev.vortex.api.ScanOptions;
import dev.vortex.api.Session;
import dev.vortex.jni.NativeLoader;

/**
 * {@code vortex@jni} read-conformance sidecar (see {@link ReaderMain} for the CLI
 * contract):
 *
 * <pre>{@code
 *   raincloud-read-vortex-jni --input <artifact.vortex> --canonical <slug.arrow.zstd> --report <report.json>
 * }</pre>
 *
 * Reads the {@code .vortex} artifact via the Vortex JNI bindings (data handed to the
 * JVM as arrow-java vectors over the Arrow C Data Interface) and compares LOGICALLY
 * to the canonical Arrow IPC file.
 */
public final class ConformanceReader {
    public static void main(String[] args) {
        ReaderMain.run("vortex@jni", "artifact.vortex", args, ConformanceReader::open, t -> false);
    }

    /** The {@code .vortex} file's batches, read through vortex-jni in file order. */
    static BatchSource open(Path input, BufferAllocator allocator) {
        NativeLoader.loadJni();
        String uri = input.toAbsolutePath().toUri().toString();
        DataSource ds = DataSource.open(Session.create(), uri);
        // ORDERED scan — `ScanOptions.of()` defaults `ordered()` to FALSE, so a
        // multi-partition file's partitions can arrive in ANY order. Rows are
        // compared positionally against the canonical, so an unordered scan
        // silently shuffles rows and the comparison fails on data that round-tripped
        // perfectly -- nondeterministically (the same cell measured pass, fail, fail
        // on three consecutive runs of `oasst1`), which would seed the oracle with
        // flaky verdicts. Single-partition files (uci-iris, uci-wine) mask it.
        Scan scan = ds.scan(ScanOptions.builder().ordered(true).build());
        Schema schema = scan.arrowSchema(allocator);
        return new BatchSource() {
            private ArrowReader reader;

            @Override
            public MaterializedTable empty() {
                return builder().build();
            }

            /** Load the next batch into {@link #reader}, crossing partitions; false at the end. */
            private boolean advance() throws IOException {
                while (reader == null || !reader.loadNextBatch()) {
                    closePartition();
                    if (!scan.hasNext()) {
                        return false;
                    }
                    reader = scan.next().scanArrow(allocator);
                }
                return true;
            }

            @Override
            public MaterializedTable next() throws IOException {
                if (!advance()) {
                    return null;
                }
                // ArrowReader is a DictionaryProvider. The scan schema declares decoded
                // VALUE types, so a dictionary-encoded partition must be decoded here or
                // the recorded Field and the recorded values disagree.
                MaterializedTable.Builder builder = builder();
                builder.appendBatch(reader.getVectorSchemaRoot(), reader);
                return builder.build();
            }

            private MaterializedTable.Builder builder() {
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.initialize(schema, null);
                return builder;
            }

            @Override
            public long countRemaining() throws IOException {
                long rows = 0;
                while (advance()) {
                    rows += reader.getVectorSchemaRoot().getRowCount();
                }
                return rows;
            }

            private void closePartition() throws IOException {
                if (reader != null) {
                    ArrowReader open = reader;
                    reader = null;
                    open.close();
                }
            }

            @Override
            public void close() throws IOException {
                closePartition();
                // The scan reads through the data source: keep it reachable until the
                // scan is done with, whatever vortex-jni's native handles hold.
                Reference.reachabilityFence(ds);
            }
        };
    }
}
