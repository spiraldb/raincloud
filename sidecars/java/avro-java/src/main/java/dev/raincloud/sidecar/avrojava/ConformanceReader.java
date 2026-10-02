// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.avrojava;

import dev.raincloud.sidecar.common.ReaderMain;

/**
 * {@code avro@java} READ-conformance sidecar (see {@link ReaderMain} for the CLI contract):
 *
 * <pre>{@code
 *   raincloud-read-avro-java --input <artifact.avro> --canonical <slug.arrow.zstd> --report <report.json>
 * }</pre>
 *
 * Reads an Avro object container file through Apache Avro's Java implementation and Arrow
 * Java's Avro adapter ({@link AvroArrowIo#openAvro}) and compares LOGICALLY to the canonical.
 */
public final class ConformanceReader {
    public static void main(String[] args) {
        ReaderMain.run(ConformanceWriter.CELL, "artifact.avro", args, AvroArrowIo::openAvro,
                t -> false);
    }
}
