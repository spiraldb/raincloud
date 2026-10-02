// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.avrojava;

import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.CanonicalReader;
import dev.raincloud.sidecar.common.LogicalCompare;
import dev.raincloud.sidecar.common.Verdict;
import dev.raincloud.sidecar.common.WriterMain;
import java.nio.file.Path;
import org.apache.arrow.memory.BufferAllocator;

/**
 * {@code avro@java} WRITE-conformance sidecar (see {@link WriterMain} for the CLI contract
 * and the report):
 *
 * <pre>{@code
 *   raincloud-export-avro-java --input <slug.arrow.zstd> --output <dest> --report <report.json>
 * }</pre>
 *
 * Writes the canonical as a zstandard Avro object container file through Arrow Java's Avro
 * adapter ({@link AvroArrowIo#writeAvro}), then self-verifies by reading it back through the
 * same adapter. Avro has no VARIANT type, so a VARIANT column is never kept. A type the
 * adapter refuses is the implementation's failure, measured, never a comparator gap: the
 * adapter is what this lane measures.
 */
public final class ConformanceWriter {
    static final String CELL = "avro@java";

    private static Verdict selfVerify(Path input, Path output, BufferAllocator allocator) throws Exception {
        try (BatchSource expected = CanonicalReader.open(input, allocator);
                BatchSource got = AvroArrowIo.openAvro(output, allocator)) {
            return LogicalCompare.compare(CELL, got, expected);
        }
    }

    public static void main(String[] args) {
        WriterMain.run(CELL, (output, variants, allocator) -> "Avro has no VARIANT type", args,
                AvroArrowIo::writeAvro, ConformanceWriter::selfVerify,
                t -> false);
    }
}
