// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.parquetjava;

import java.nio.file.Path;

import org.apache.arrow.memory.BufferAllocator;

import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.CanonicalReader;
import dev.raincloud.sidecar.common.LogicalCompare;
import dev.raincloud.sidecar.common.VariantFidelity;
import dev.raincloud.sidecar.common.Verdict;
import dev.raincloud.sidecar.common.WriterMain;
import dev.spiraldb.parquet.arrow.UnsupportedParquetTypeException;

/**
 * {@code parquet@java} WRITE-conformance sidecar (see {@link WriterMain} for the CLI
 * contract and the report):
 *
 * <pre>{@code
 *   raincloud-export-parquet-java --input <slug.arrow.zstd> --output <dest> --report <report.json>
 * }</pre>
 *
 * Streams the canonical into a zstd Parquet through parquet-arrow-java (which drives
 * Apache parquet-java, the reference encoder, Hadoop-free), then self-verifies by
 * re-reading it through the same bridge. A type the bridge cannot map is
 * {@code "unsupported type"} before the write and a comparator gap after it. The bridge
 * writes an {@code arrow.parquet.variant} column as a Parquet VARIANT group; whether the
 * file kept it is measured ({@link ParquetArrowIo#variantColumns} and the bridge's
 * read-back).
 */
public final class ConformanceWriter {
    static final String CELL = "parquet@java";

    private static Verdict selfVerify(Path input, Path output, BufferAllocator allocator) throws Exception {
        try (BatchSource expected = CanonicalReader.open(input, allocator);
                BatchSource got = ParquetArrowIo.openParquet(output, allocator)) {
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
        return WriterMain.execute(CELL, VariantFidelity.parquet(ParquetArrowIo::variantColumns,
                ParquetArrowIo::openParquet), args, ParquetArrowIo::writeParquet, verify,
                t -> t instanceof UnsupportedParquetTypeException);
    }
}
