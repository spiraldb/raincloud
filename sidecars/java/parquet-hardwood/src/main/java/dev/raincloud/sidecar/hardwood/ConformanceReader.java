// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.hardwood;

import dev.raincloud.sidecar.common.ReaderMain;

/**
 * {@code parquet@hardwood} READ-conformance sidecar (see {@link ReaderMain} for the CLI
 * contract):
 *
 * <pre>{@code
 *   raincloud-read-parquet-hardwood --input <artifact.parquet> --canonical <slug.arrow.zstd> --report <report.json>
 * }</pre>
 *
 * Reads a produced Parquet through Hardwood's columnar reader ({@link HardwoodReader}) and
 * compares LOGICALLY to the canonical. A type or shape the lane cannot map is a comparator
 * gap → {@code skip}; any other error → a measured {@code fail}.
 */
public final class ConformanceReader {
    public static void main(String[] args) {
        int code = run(args);
        if (code != 0) {
            System.exit(code);
        }
    }

    /** {@link ReaderMain#execute} for this lane. */
    static int run(String[] args) {
        return ReaderMain.execute(ConformanceWriter.CELL, "artifact.parquet", args, HardwoodReader::openParquet,
                t -> t instanceof UnsupportedHardwoodTypeException);
    }
}
