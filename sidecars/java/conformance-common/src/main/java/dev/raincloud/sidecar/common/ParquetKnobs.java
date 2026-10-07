// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * The Parquet write options every Parquet lane is given the same way
 * ({@code raincloud/pipeline/spec.py::ParquetOptions}, and {@code ParquetOptions} in the Rust
 * lane). The sidecar never sees the recipe: {@code SidecarExporter} passes its
 * {@code write.compression} and {@code write.statistics} as {@link #COMPRESSION} and
 * {@link #STATISTICS}, and each page knob only when it is set. An unset page knob ({@code null}
 * here) leaves the writer library's own default; a lane whose library cannot do what a set knob
 * asks refuses it rather than writing something else.
 *
 * @param compression one of {@link #CODECS}
 * @param statistics  whether to write statistics at all
 * @param pageIndex   a ColumnIndex and OffsetIndex for every column chunk, or null
 * @param pageBytes   the data page size target, or null; {@link #PAGE_LIMIT} is no limit
 * @param pageRows    the data page row limit, or null; {@link #PAGE_LIMIT} is no limit
 */
public record ParquetKnobs(String compression, boolean statistics, Boolean pageIndex, Integer pageBytes,
        Integer pageRows) {

    public static final String COMPRESSION = "RAINCLOUD_PARQUET_COMPRESSION";
    public static final String STATISTICS = "RAINCLOUD_PARQUET_STATISTICS";
    public static final String PAGE_INDEX = "RAINCLOUD_PARQUET_PAGE_INDEX";
    public static final String PAGE_BYTES = "RAINCLOUD_PARQUET_PAGE_BYTES";
    public static final String PAGE_ROWS = "RAINCLOUD_PARQUET_PAGE_ROWS";
    public static final Set<String> CODECS = Set.of("zstd", "snappy", "gzip", "lz4", "brotli", "none");
    /** What 0 (no limit) means for a page knob: the libraries take an int. */
    public static final int PAGE_LIMIT = Integer.MAX_VALUE;

    /** Every option unset: zstd, statistics on, each library's page defaults. */
    public static final ParquetKnobs DEFAULT = new ParquetKnobs("zstd", true, null, null, null);

    /** The options from the process environment. */
    public static ParquetKnobs fromEnv() {
        return from(System::getenv);
    }

    /** The options from {@code env}, a variable lookup that returns null when unset. */
    public static ParquetKnobs from(Function<String, String> env) {
        String rawCodec = env.apply(COMPRESSION);
        String compression = rawCodec == null ? "zstd" : rawCodec.strip();
        if (!CODECS.contains(compression)) {
            throw new IllegalArgumentException(COMPRESSION + "='" + rawCodec
                    + "' is not one of zstd, snappy, gzip, lz4, brotli, none");
        }
        Boolean statistics = toggle(STATISTICS, env.apply(STATISTICS));
        Boolean pageIndex = toggle(PAGE_INDEX, env.apply(PAGE_INDEX));
        boolean withStatistics = statistics == null || statistics;
        if (Boolean.TRUE.equals(pageIndex) && !withStatistics) {
            throw new IllegalArgumentException(PAGE_INDEX + "=1 asks for page statistics, but " + STATISTICS
                    + " is 0");
        }
        return new ParquetKnobs(compression, withStatistics, pageIndex, page(PAGE_BYTES, env.apply(PAGE_BYTES)),
                page(PAGE_ROWS, env.apply(PAGE_ROWS)));
    }

    /**
     * An on/off knob as every lane reads it ({@code spec._env_switch}): unset or empty is null;
     * otherwise 1/true/yes/on or 0/false/no/off, in any case.
     */
    static Boolean toggle(String var, String raw) {
        if (raw == null) {
            return null;
        }
        String value = raw.strip().toLowerCase(Locale.ROOT);
        switch (value) {
            case "":
                return null;
            case "1": case "true": case "yes": case "on":
                return true;
            case "0": case "false": case "no": case "off":
                return false;
            default:
                throw new IllegalArgumentException(var + "='" + raw
                        + "' is not a switch; give 1 or 0 (true/false, yes/no, on/off)");
        }
    }

    /** A page knob: unset is null (the library's default); the count grammar otherwise, 0 no limit. */
    private static Integer page(String var, String raw) {
        if (raw == null) {
            return null;
        }
        return (int) Math.min(Knobs.count(var, raw, PAGE_LIMIT, PAGE_LIMIT), PAGE_LIMIT);
    }

    /** Refuse a knob this lane's library cannot honour, naming the lane, the knob and why. */
    public static IllegalArgumentException unsupported(String lane, String var, Object value, String why) {
        return new IllegalArgumentException(lane + " cannot honour " + var + "=" + value + ": " + why);
    }
}
