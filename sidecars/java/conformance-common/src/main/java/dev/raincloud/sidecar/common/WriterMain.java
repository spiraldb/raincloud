// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

/**
 * The WRITE-conformance sidecar main shared by every JVM writer lane — the write twin of
 * {@link ReaderMain}. Implements raincloud's writer CLI contract
 * ({@code raincloud/pipeline/export/sidecar.py}):
 *
 * <pre>{@code
 *   <launcher> --input <slug.arrow.zstd> --output <dest> --report <report.json>
 * }</pre>
 *
 * Has the lane write the canonical to {@code --output}, then SELF-VERIFIES by re-reading the
 * artifact and comparing LOGICALLY one batch at a time, and emits
 * {@code {roundtrip, variant_faithful, note}}:
 *
 * <ul>
 *   <li>{@code roundtrip} is {@code true}/{@code false} for a measured self-verify and
 *       {@code null} when a comparator gap or exhausted memory left it unmeasured (the
 *       artifact was written; the JVM comparator could not judge it), never a measured
 *       failure for our limit. The harness promotes an unmeasured artifact; only
 *       {@code false} refuses it. A type the lane cannot write is {@code false} with no
 *       output ({@code "unsupported type"}). A write or self-verify that asks for an array
 *       past the JVM's array size limit ({@link ReaderMain#arrayLimit}) is {@code false}:
 *       the implementation's limit, not the comparator's;
 *   <li>{@code variant_faithful} is measured on the written file by the lane's
 *       {@link VariantFidelity.Check} when the canonical carries VARIANT columns, and the
 *       note says what was not kept; it is {@code true} when there are none. When the file
 *       was never written, or the check could not run, it is {@code false} meaning unknown.
 * </ul>
 *
 * A written file is kept whatever the self-verify says, so the caller sees the artifact and
 * the honest verdict rather than "no output"; a {@link Writer} that fails must leave none.
 * Memory the lane leaked, found when its allocator closes, is recorded in the note and leaves
 * the verdict as it is ({@link ReaderMain#closeAllocator}). Exit 2 is a usage error,
 * including a missing required option; exit 1 means the report could not be written.
 */
public final class WriterMain {
    private WriterMain() {}

    private static final String USAGE = "usage: --input <slug.arrow.zstd> --output <dest> --report <report.json>";

    /** Writes the canonical {@code input} to {@code output}; on failure it leaves no {@code output}. */
    public interface Writer {
        void write(Path input, Path output, BufferAllocator allocator) throws Exception;
    }

    /** The self-verify: re-read {@code output} and compare it to the canonical {@code input}. */
    public interface SelfVerify {
        Verdict verify(Path input, Path output, BufferAllocator allocator) throws Exception;
    }

    /**
     * Run the writer lane and exit with {@link #execute}'s code when it is not 0.
     *
     * @param cell the conformance cell, e.g. {@code parquet@java}; it prefixes every note
     * @param variant measures whether the written file kept the canonical's VARIANT columns
     * @param gap which lane-specific exceptions mean "this lane cannot represent the type"
     *     (unsupported before the file is written, a comparator gap after) rather than a failure
     */
    public static void run(String cell, VariantFidelity.Check variant, String[] args, Writer writer,
            SelfVerify verify, Predicate<Throwable> gap) {
        int code = execute(cell, variant, args, writer, verify, gap);
        if (code != 0) {
            System.exit(code);
        }
    }

    /**
     * The lane itself, returning the exit code instead of exiting: 0 when a report was
     * written, 2 on a usage error (nothing is written), 1 when the report could not be.
     * Parameters as for {@link #run}.
     */
    public static int execute(String cell, VariantFidelity.Check variant, String[] args, Writer writer,
            SelfVerify verify, Predicate<Throwable> gap) {
        Path input, output, report;
        try {
            Cli cli = Cli.parse(args, "input", "output", "report");
            report = cli.require("report");
            input = cli.require("input");
            output = cli.require("output");
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println(USAGE);
            return 2;
        }
        Boolean variantFaithful = null;
        boolean written = false;
        String leak = null;
        try {
            Boolean roundtrip;
            String note;
            BufferAllocator allocator = new RootAllocator();
            try {
                // From the schema alone: no rows are read to decide which columns are VARIANT.
                List<String> variants;
                try (BatchSource canonical = CanonicalReader.open(input, allocator)) {
                    variants = VariantFidelity.columns(canonical.empty().fields);
                }
                // Vacuously kept with none; unknown until measured on the written file.
                variantFaithful = variants.isEmpty() ? Boolean.TRUE : null;
                writer.write(input, output, allocator);
                written = true;

                // Measured on the file before the self-verify, so a verdict the comparator
                // cannot reach still reports it.
                String variantLoss = null;
                if (!variants.isEmpty()) {
                    try {
                        variantLoss = variant.loss(output, variants, allocator);
                    } catch (Exception e) {
                        variantLoss = "VARIANT unmeasured: " + Report.describe(e);
                    }
                }
                variantFaithful = variantLoss == null;

                Verdict v = verify.verify(input, output, allocator);
                roundtrip = "pass".equals(v.status) ? Boolean.TRUE
                        : "skip".equals(v.status) ? null : Boolean.FALSE;
                if (Boolean.TRUE.equals(roundtrip)) {
                    note = variantLoss != null ? cell + ": round-trips; VARIANT not kept: " + variantLoss
                            : variants.isEmpty() ? cell + ": round-trips to canonical"
                            : cell + ": round-trips; VARIANT kept (" + String.join(", ", variants) + ")";
                } else {
                    note = cell + ": self-verify " + (roundtrip == null ? "unmeasured" : v.status) + ": "
                            + v.note + (v.detail.isEmpty() ? "" : " — " + v.detail)
                            + (variantLoss == null ? "" : "; VARIANT not kept: " + variantLoss);
                }
            } finally {
                leak = ReaderMain.closeAllocator(cell, allocator);
            }
            return writeReport(cell, report, roundtrip, variantFaithful, ReaderMain.withLeak(note, leak));
        } catch (Throwable t) {
            return failed(cell, report, t, written, variantFaithful, leak, gap);
        }
    }

    /** The report for a write or self-verify that threw {@code t}; {@code leak} joins its note. */
    private static int failed(String cell, Path report, Throwable t, boolean written, Boolean variantFaithful,
            String leak, Predicate<Throwable> gap) {
        if (t instanceof ComparatorGap || gap.test(t)) {
            if (written) {
                // The lane wrote the file but cannot read it back (or the comparator
                // cannot judge its shape): the self-verify is unmeasured, not failed.
                return writeReport(cell, report, null, variantFaithful, ReaderMain.withLeak(
                        cell + ": self-verify unmeasured (comparator gap): " + t.getMessage(), leak));
            }
            return writeReport(cell, report, false, variantFaithful,
                    ReaderMain.withLeak(cell + ": unsupported type: " + t.getMessage(), leak));
        }
        if (ReaderMain.arrayLimit(t) != null) {
            // The implementation asked for an array past the JVM's limit, writing or
            // reading back its own file: its failure, which no -Xmx changes.
            t.printStackTrace();
            return writeReport(cell, report, false, variantFaithful, ReaderMain.withLeak(
                    cell + (written ? ": self-verify fail: read error: " : ": while writing: ")
                            + ReaderMain.ARRAY_LIMIT_NOTE + " — " + Report.describe(t), leak));
        }
        if (ReaderMain.outOfMemory(t) != null) {
            if (written) {
                // As in ReaderMain: the comparator ran out of memory (heap or Arrow's
                // off-heap allocator), which says nothing about the file it wrote.
                return writeReport(cell, report, null, variantFaithful, ReaderMain.withLeak(
                        cell + ": self-verify unmeasured (comparator resource limit, out of memory; "
                                + "raise -Xmx or -XX:MaxDirectMemorySize through JAVA_OPTS): "
                                + t.getMessage(), leak));
            }
            // The write itself ran out of memory: the Writer removed the partial file.
            t.printStackTrace();
            return writeReport(cell, report, false, variantFaithful,
                    ReaderMain.withLeak(cell + ": while writing: " + Report.describe(t), leak));
        }
        t.printStackTrace();
        return writeReport(cell, report, false, variantFaithful,
                ReaderMain.withLeak(cell + ": " + Report.describe(t), leak));
    }

    /** Write the report: 0 when written, 1 when it could not be. */
    private static int writeReport(String cell, Path report, Boolean roundtrip, Boolean variantFaithful,
            String note) {
        try {
            // variant_faithful=false stands for "unknown" when the schema was never read.
            Report.writeWrite(report, roundtrip, variantFaithful != null && variantFaithful, note);
            return 0;
        } catch (Exception e) {
            System.err.println("[" + cell + "] failed to write report " + report + ": " + e);
            return 1;
        }
    }
}
