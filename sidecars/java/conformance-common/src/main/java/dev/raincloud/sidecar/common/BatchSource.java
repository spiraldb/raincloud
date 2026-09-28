// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.io.IOException;

import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * A table read one record batch at a time, each batch materialized on its own.
 *
 * <p>{@link LogicalCompare#compare(String, BatchSource, BatchSource)} holds one
 * batch of each side at a time, so memory is bounded by the largest batch rather
 * than the table.</p>
 */
public interface BatchSource extends AutoCloseable {
    /** The table's recorded fields and no rows, available before any batch is read. */
    MaterializedTable empty();

    /** The next batch, materialized against the source's schema, or null at the end. */
    MaterializedTable next() throws IOException;

    @Override
    void close() throws IOException;

    /**
     * Rows left after the batches already returned, for a row-count mismatch. This
     * default materializes them through {@link #next()}; a source that can count
     * rows without boxing every cell overrides it, as {@link #of} does.
     */
    default long countRemaining() throws IOException {
        long rows = 0;
        for (MaterializedTable batch = next(); batch != null; batch = next()) {
            rows += batch.rowCount;
        }
        return rows;
    }

    /**
     * Batches of {@code reader}, which this source then owns. The reader is also the
     * dictionary provider, so dictionary columns materialize as their values.
     */
    static BatchSource of(ArrowReader reader) throws IOException {
        Schema schema;
        try {
            schema = reader.getVectorSchemaRoot().getSchema();
        } catch (IOException | RuntimeException e) {
            try {
                reader.close();
            } catch (IOException cleanup) {
                e.addSuppressed(cleanup);
            }
            throw e;
        }
        return new BatchSource() {
            @Override
            public MaterializedTable empty() {
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.initialize(schema, reader);
                return builder.build();
            }

            @Override
            public MaterializedTable next() throws IOException {
                if (!reader.loadNextBatch()) {
                    return null;
                }
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.initialize(schema, reader);
                builder.appendBatch(reader.getVectorSchemaRoot(), reader);
                return builder.build();
            }

            @Override
            public long countRemaining() throws IOException {
                long rows = 0;
                while (reader.loadNextBatch()) {
                    rows += reader.getVectorSchemaRoot().getRowCount();
                }
                return rows;
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }
}
