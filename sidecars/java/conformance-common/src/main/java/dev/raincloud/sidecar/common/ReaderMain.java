// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.nio.file.Path;
import java.util.function.Predicate;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;

/**
 * The READ-conformance sidecar main shared by every JVM reader lane. Implements
 * raincloud's reader CLI contract ({@code raincloud/pipeline/export/readers.py}):
 *
 * <pre>{@code
 *   <launcher> --input <artifact> --canonical <slug.arrow.zstd> --report <report.json>
 * }</pre>
 *
 * Reads the artifact and the canonical one batch at a time, compares LOGICALLY and
 * writes a {@code {status, note, detail}} report. <b>Exit 0 means the reader RAN</b>
 * (the verdict — possibly negative — is in the report): any {@link Throwable} is
 * recorded rather than exiting non-zero, so a native panic degrades to a measured
 * fail with detail. A comparator gap or exhausted memory (heap or Arrow's off-heap
 * allocator) is a {@code skip}: a tooling limit, never a verdict on the artifact. A request
 * for an array past the JVM's array size limit is a measured {@code fail}: no memory
 * setting satisfies it (see {@link #arrayLimit}). Memory the lane leaked, found when its
 * allocator closes, is recorded in the note and leaves the verdict as it is
 * ({@link #closeAllocator}).
 * Exit 2 is a usage error, including a missing required option; exit 1 means the
 * report itself could not be written.
 */
public final class ReaderMain {
    private ReaderMain() {}

    /** Opens the artifact under test as a batch source. */
    public interface Opener {
        BatchSource open(Path input, BufferAllocator allocator) throws Exception;
    }

    /**
     * Run the reader lane and exit with {@link #execute}'s code when it is not 0.
     *
     * @param cell the conformance cell, e.g. {@code vortex@jni}; it prefixes every note
     * @param usageInput the artifact placeholder the usage line shows, e.g.
     *     {@code artifact.vortex}
     * @param gap which lane-specific exceptions mean "this reader cannot represent the
     *     artifact's type" (a comparator gap) rather than a read failure
     */
    public static void run(String cell, String usageInput, String[] args, Opener opener,
            Predicate<Throwable> gap) {
        int code = execute(cell, usageInput, args, opener, gap);
        if (code != 0) {
            System.exit(code);
        }
    }

    /**
     * The lane itself, returning the exit code instead of exiting: 0 when a report was
     * written, 2 on a usage error (nothing is read), 1 when the report could not be
     * written. Parameters as for {@link #run}.
     */
    public static int execute(String cell, String usageInput, String[] args, Opener opener,
            Predicate<Throwable> gap) {
        String usage = "usage: --input <" + usageInput + "> --canonical <slug.arrow.zstd> --report <report.json>";
        Path input, canonical, report;
        try {
            Cli cli = Cli.parse(args, "input", "canonical", "report");
            report = cli.require("report");
            input = cli.require("input");
            canonical = cli.require("canonical");
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println(usage);
            return 2;
        }
        Verdict v;
        String leak;
        BufferAllocator allocator = new RootAllocator();
        try {
            v = verdict(cell, input, canonical, opener, gap, allocator);
        } finally {
            leak = closeAllocator(cell, allocator);
        }
        return write(report, v.status, withLeak(v.note, leak), v.detail);
    }

    private static Verdict verdict(String cell, Path input, Path canonical, Opener opener, Predicate<Throwable> gap,
            BufferAllocator allocator) {
        try {
            try (BatchSource expected = CanonicalReader.open(canonical, allocator);
                    BatchSource got = opener.open(input, allocator)) {
                return LogicalCompare.compare(cell, got, expected);
            }
        } catch (ComparatorGap e) {
            return Verdict.skip(cell + ": comparator gap (unsupported shape)", e.getMessage());
        } catch (Throwable t) {
            if (arrayLimit(t) != null) {
                t.printStackTrace();
                return Verdict.fail(cell + ": read error: " + ARRAY_LIMIT_NOTE, Report.describe(t));
            }
            Throwable oom = outOfMemory(t);
            if (oom != null) {
                // The heap (OutOfMemoryError) or Arrow's off-heap allocator
                // (OutOfMemoryException) could not hold one batch of each side.
                return Verdict.skip(cell + ": comparator resource limit (out of memory)",
                        "one batch of each side did not fit in memory; raise -Xmx or "
                                + "-XX:MaxDirectMemorySize (JAVA_OPTS) to measure this artifact: "
                                + oom.getMessage());
            }
            if (gap.test(t)) {
                return Verdict.skip(cell + ": comparator gap (unsupported type)", t.getMessage());
            }
            t.printStackTrace();
            return Verdict.fail(cell + ": read error", Report.describe(t));
        }
    }

    /**
     * Close {@code allocator}, returning what it still held if that is a leak, else
     * {@code null}. A leak is recorded, never a verdict: it says the lane (or the library
     * under it) kept buffers it should have released, not that the data is wrong. Also
     * printed to stderr.
     */
    public static String closeAllocator(String cell, BufferAllocator allocator) {
        long held = allocator.getAllocatedMemory();
        try {
            allocator.close();
            return null;
        } catch (RuntimeException e) {
            System.err.println("[" + cell + "] allocator close: " + e);
            String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage().lines().findFirst().orElse("");
            return "allocator " + allocator.getName() + " still held " + held + " B at close (" + message + ")";
        }
    }

    /** {@code note} with the leak {@link #closeAllocator} found, if any. */
    public static String withLeak(String note, String leak) {
        return leak == null ? note : note + "; memory leak: " + leak;
    }

    /** The HotSpot message of an array request past the JVM's array size limit. */
    static final String ARRAY_LIMIT_MESSAGE = "Requested array size exceeds VM limit";

    /** How a note names {@link #arrayLimit}. */
    public static final String ARRAY_LIMIT_NOTE = "requested an array past the JVM's array size limit";

    /**
     * The out-of-memory error behind {@code t}, if any: a library may wrap one
     * (parquet-arrow-java raises CorruptParquetException caused by an IOException
     * caused by the OOM), and a resource limit must not read as a measured failure.
     * An {@link #arrayLimit} error is not one.
     */
    public static Throwable outOfMemory(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if ((c instanceof OutOfMemoryError || c instanceof OutOfMemoryException) && !isArrayLimit(c)) {
                return c;
            }
        }
        return null;
    }

    /**
     * The array-limit error behind {@code t}, if any: the implementation asked for an array
     * longer than the JVM allows ("Requested array size exceeds VM limit"), which no
     * {@code -Xmx} satisfies. That is the implementation's limit, a measured failure.
     */
    public static Throwable arrayLimit(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause() == c ? null : c.getCause()) {
            if (isArrayLimit(c)) {
                return c;
            }
        }
        return null;
    }

    private static boolean isArrayLimit(Throwable t) {
        return t instanceof OutOfMemoryError && t.getMessage() != null
                && t.getMessage().contains(ARRAY_LIMIT_MESSAGE);
    }

    /** Write the report: 0 when written, 1 when it could not be. */
    private static int write(Path report, String status, String note, String detail) {
        try {
            Report.writeRead(report, status, note, detail);
            return 0;
        } catch (Exception e) {
            System.err.println("failed to write report " + report + ": " + e);
            return 1;
        }
    }
}
