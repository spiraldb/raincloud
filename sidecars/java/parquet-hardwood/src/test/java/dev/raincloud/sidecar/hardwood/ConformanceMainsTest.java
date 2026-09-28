// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.hardwood;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.hardwood.OutputFile;
import dev.hardwood.metadata.LogicalType;
import dev.hardwood.metadata.PhysicalType;
import dev.hardwood.metadata.RepetitionType;
import dev.hardwood.schema.FileSchema;
import dev.hardwood.writer.ParquetFileWriter;
import dev.raincloud.sidecar.common.WriterMain;

/** The two {@code parquet@hardwood} mains end to end: reports, exit codes, what stays on disk. */
class ConformanceMainsTest {

    @TempDir
    Path tmp;

    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    private String write(Path canonical, Path output) throws IOException {
        return write(canonical, output, null);
    }

    private String write(Path canonical, Path output, WriterMain.SelfVerify verify) throws IOException {
        Path report = tmp.resolve(output.getFileName() + ".json");
        String[] args = {"--input", canonical.toString(), "--output", output.toString(), "--report", report.toString()};
        if (verify == null) {
            ConformanceWriter.main(args);
        } else {
            assertEquals(0, ConformanceWriter.run(args, verify));
        }
        return Files.readString(report);
    }

    private String read(Path artifact, Path canonical) throws IOException {
        Path report = tmp.resolve(artifact.getFileName() + ".read.json");
        assertEquals(0, ConformanceReader.run(new String[] {"--input", artifact.toString(),
                "--canonical", canonical.toString(), "--report", report.toString()}));
        return Files.readString(report);
    }

    @Test
    void theWriterReportsAMeasuredRoundTripAndTheReaderPassesItsFile() throws IOException {
        Path canonical = Canonicals.numbers(tmp.resolve("n.arrow"), allocator, 100);
        Path output = tmp.resolve("n.parquet");
        String report = write(canonical, output);
        assertTrue(report.startsWith("{\"roundtrip\":true,\"variant_faithful\":true,"), report);
        assertTrue(Files.size(output) > 0);
        String verdict = read(output, canonical);
        assertTrue(verdict.startsWith("{\"status\":\"pass\""), verdict);
    }

    @Test
    void theWriterReportsTheDroppedVariantAnnotation() throws IOException {
        Schema schema = new Schema(List.of(new Field("v",
                new FieldType(true, new ArrowType.Int(64, true), null, Map.of("__variant_type", "json")), null)));
        Path canonical = Canonicals.write(tmp.resolve("variant.arrow"), allocator, schema, null, root -> {
            ((BigIntVector) root.getVector("v")).setSafe(0, 7L);
            root.setRowCount(1);
        });
        String report = write(canonical, tmp.resolve("variant.parquet"));
        assertTrue(report.startsWith("{\"roundtrip\":true,\"variant_faithful\":false,"), report);
        assertTrue(report.contains("VARIANT not kept: column \\\"v\\\": the file declares no Parquet VARIANT "
                + "logical type; column \\\"v\\\": read back without the arrow.parquet.variant extension"), report);
    }

    @Test
    void anUnwritableTypeIsAMeasuredFailureWithNoOutput() throws IOException {
        Schema schema = new Schema(List.of(new Field("d",
                FieldType.nullable(new ArrowType.Duration(TimeUnit.MILLISECOND)), null)));
        Path canonical = Canonicals.write(tmp.resolve("duration.arrow"), allocator, schema, null,
                root -> root.setRowCount(0));
        Path output = tmp.resolve("duration.parquet");
        String report = write(canonical, output);
        assertTrue(report.startsWith("{\"roundtrip\":false,\"variant_faithful\":true,"), report);
        assertTrue(report.contains("unsupported type") && report.contains("Duration"), report);
        assertFalse(Files.exists(output), "a refused write left an artifact");
    }

    @Test
    void aComparatorGapIsAnUnmeasuredRoundTripThatKeepsTheFile() throws IOException {
        // Time32(SECOND) is stored as TIME(MILLIS) and read back as Time32(MILLISECOND): the JVM
        // comparator does not judge time-unit changes (sidecars/README.md), so unmeasured.
        Schema schema = new Schema(List.of(new Field("t",
                FieldType.nullable(new ArrowType.Time(TimeUnit.SECOND, 32)), null)));
        Path canonical = Canonicals.write(tmp.resolve("time.arrow"), allocator, schema, null, root -> {
            ((TimeSecVector) root.getVector("t")).setSafe(0, 5);
            root.setRowCount(1);
        });
        Path output = tmp.resolve("time.parquet");
        String report = write(canonical, output);
        assertTrue(report.startsWith("{\"roundtrip\":null,\"variant_faithful\":true,"), report);
        assertTrue(report.contains("self-verify unmeasured"), report);
        assertTrue(Files.exists(output));
    }

    @Test
    void aFailedSelfVerifyIsMeasuredAndKeepsTheFile() throws IOException {
        Path output = tmp.resolve("broken.parquet");
        String report = write(Canonicals.numbers(tmp.resolve("broken.arrow"), allocator, 4), output,
                (input, out, a) -> {
                    throw new IOException("re-read failed", new IllegalStateException("the cause"));
                });
        assertTrue(report.startsWith("{\"roundtrip\":false,"), report);
        assertTrue(report.contains("IOException: re-read failed (caused by IllegalStateException: the cause)"),
                report);
        assertTrue(Files.exists(output), "the file stays; the harness refuses to promote it");
    }

    @Test
    void theReaderFailsADataMismatch() throws IOException {
        Path written = Canonicals.numbers(tmp.resolve("four.arrow"), allocator, 4);
        Path artifact = tmp.resolve("four.parquet");
        write(written, artifact);
        Path other = tmp.resolve("other.arrow");
        Canonicals.write(other, allocator, new Schema(List.of(new Field("n",
                FieldType.nullable(new ArrowType.Int(64, true)), null))), null, root -> {
                    BigIntVector n = (BigIntVector) root.getVector("n");
                    for (int i = 0; i < 4; i++) {
                        n.setSafe(i, i == 2 ? 99 : i);
                    }
                    root.setRowCount(4);
                });
        String verdict = read(artifact, other);
        assertTrue(verdict.startsWith("{\"status\":\"fail\",\"note\":\"parquet@hardwood: data mismatch"), verdict);
        assertTrue(verdict.contains("row 2"), verdict);
    }

    @Test
    void aTypeTheReaderCannotMapIsAComparatorGap() throws IOException {
        // INTERVAL has no Arrow type this lane reads: written by Hardwood itself, read as a gap.
        Path artifact = tmp.resolve("interval.parquet");
        FileSchema schema = FileSchema.builder("schema")
                .addColumn("i", PhysicalType.FIXED_LEN_BYTE_ARRAY, RepetitionType.OPTIONAL, 12,
                        new LogicalType.IntervalType())
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(OutputFile.of(artifact), schema)) {
            writer.columnWriter().writeBatch(b -> b.fixed(0, new byte[][] {new byte[12]}));
        }
        Schema arrow = new Schema(List.of(new Field("i",
                FieldType.nullable(new ArrowType.Interval(IntervalUnit.DAY_TIME)), null)));
        Path canonical = Canonicals.write(tmp.resolve("interval.arrow"), allocator, arrow, null,
                root -> root.setRowCount(0));
        String verdict = read(artifact, canonical);
        assertTrue(verdict.startsWith("{\"status\":\"skip\",\"note\":\"parquet@hardwood: comparator gap"), verdict);
        assertTrue(verdict.contains("INTERVAL"), verdict);
    }

    @Test
    void aMissingOrUnknownOptionIsAUsageErrorWithNoReport() throws IOException {
        Path canonical = Canonicals.numbers(tmp.resolve("usage.arrow"), allocator, 1);
        Path report = tmp.resolve("usage.json");
        String out = tmp.resolve("x.parquet").toString();
        String[][] writerCases = {
            {"--input", canonical.toString(), "--report", report.toString()},
            {"--output", out, "--report", report.toString()},
            {"--input", canonical.toString(), "--output", out},
            {"--input", canonical.toString(), "--output", out, "--report", report.toString(), "--extra", "1"},
        };
        for (String[] args : writerCases) {
            assertEquals(2, ConformanceWriter.run(args, (input, o, a) -> {
                throw new AssertionError("ran on a usage error");
            }), String.join(" ", args));
            assertFalse(Files.exists(report), "a usage error wrote a report");
        }
        String[][] readerCases = {
            {"--input", out, "--report", report.toString()},
            {"--input", out, "--canonical", canonical.toString()},
            {"--input", out, "--canonical", canonical.toString(), "--report", report.toString(), "--output", out},
        };
        for (String[] args : readerCases) {
            assertEquals(2, ConformanceReader.run(args), String.join(" ", args));
            assertFalse(Files.exists(report), "a usage error wrote a report");
        }
    }
}
