// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.hardwood;

import java.nio.file.Path;

import org.apache.arrow.memory.BufferAllocator;

import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.CanonicalReader;
import dev.raincloud.sidecar.common.LogicalCompare;
import dev.raincloud.sidecar.common.VariantFidelity;
import dev.raincloud.sidecar.common.Verdict;
import dev.raincloud.sidecar.common.WriterMain;

/**
 * {@code parquet@hardwood} WRITE-conformance sidecar (see {@link WriterMain} for the CLI
 * contract and the report):
 *
 * <pre>{@code
 *   raincloud-export-parquet-hardwood --input <slug.arrow.zstd> --output <dest> --report <report.json>
 * }</pre>
 *
 * Streams the canonical into a zstd Parquet through Hardwood's columnar writer
 * ({@link HardwoodWriter}), then self-verifies by reading it back through Hardwood
 * ({@link HardwoodReader}). A type the lane cannot carry is {@code "unsupported type"}
 * before the write and a comparator gap after it. An {@code arrow.parquet.variant} column is
 * written as a Parquet VARIANT group; whether the file kept it is measured
 * ({@link HardwoodReader#variantColumns} and the Hardwood read-back).
 */
public final class ConformanceWriter {
    static final String CELL = "parquet@hardwood";

    private static Verdict selfVerify(Path input, Path output, BufferAllocator allocator) throws Exception {
        try (BatchSource expected = CanonicalReader.open(input, allocator);
                BatchSource got = HardwoodReader.openParquet(output, allocator)) {
            return LogicalCompare.compare(CELL, got, expected);
        }
    }

    public static void main(String[] args) {
        int code = run(args, ConformanceWriter::selfVerify);
        if (code != 0) {
            System.exit(code);
        }
    }

    /** {@link WriterMain#execute} for this lane, with {@code verify} as its self-verify. */
    static int run(String[] args, WriterMain.SelfVerify verify) {
        return WriterMain.execute(CELL, VariantFidelity.parquet(HardwoodReader::variantColumns,
                HardwoodReader::openParquet), args, HardwoodWriter::writeParquet, verify,
                t -> t instanceof UnsupportedHardwoodTypeException);
    }
}
