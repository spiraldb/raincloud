// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.parquetjava;

import dev.raincloud.sidecar.common.ReaderMain;
import dev.spiraldb.parquet.arrow.UnsupportedParquetTypeException;

/**
 * {@code parquet@java} READ-conformance sidecar (see {@link ReaderMain} for the CLI
 * contract):
 *
 * <pre>{@code
 *   raincloud-read-parquet-java --input <artifact.parquet> --canonical <slug.arrow.zstd> --report <report.json>
 * }</pre>
 *
 * Reads a produced Parquet through parquet-arrow-java (over Apache parquet-java,
 * Hadoop-free) and compares LOGICALLY to the canonical. A type the bridge can't map is
 * a comparator gap → {@code skip} (a tooling limitation, never a false pass or a
 * {@code fail} for our own gap); any other error → a measured {@code fail}.
 */
public final class ConformanceReader {
    public static void main(String[] args) {
        ReaderMain.run("parquet@java", "artifact.parquet", args, ParquetArrowIo::openParquet,
                t -> t instanceof UnsupportedParquetTypeException);
    }
}
