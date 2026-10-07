// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.util.Locale;
import java.util.Set;
import java.util.function.Function;

/**
 * The Parquet write options every Parquet lane is given the same way
 * ({@code raincloud/pipeline/spec.py::ParquetOptions}, which documents each, and
 * {@code ParquetOptions} in the Rust lane). The sidecar never sees the recipe:
 * {@code SidecarExporter} passes its {@code write.compression} and {@code write.statistics} as
 * {@link #COMPRESSION} and {@link #STATISTICS}, and each install setting only when it is set. An
 * unset setting ({@code null} here) leaves the writer library's own default; a lane whose library
 * cannot do what a set one asks refuses it ({@link #unsupported}) rather than writing something
 * else. A count of {@link #NO_LIMIT} is no limit ({@code 0} in the environment).
 */
public record ParquetKnobs(String compression, Integer compressionLevel, boolean statistics,
        Integer statisticsColumns, Boolean pageIndex, Integer pageIndexColumns, Integer pageBytes,
        Integer pageRows, Boolean dictionary, Integer dictionaryPageBytes, Boolean pageChecksums) {

    public static final String COMPRESSION = "RAINCLOUD_PARQUET_COMPRESSION";
    public static final String COMPRESSION_LEVEL = "RAINCLOUD_PARQUET_COMPRESSION_LEVEL";
    public static final String STATISTICS = "RAINCLOUD_PARQUET_STATISTICS";
    public static final String STATISTICS_COLUMNS = "RAINCLOUD_PARQUET_STATISTICS_COLUMNS";
    public static final String PAGE_INDEX = "RAINCLOUD_PARQUET_PAGE_INDEX";
    public static final String PAGE_INDEX_COLUMNS = "RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS";
    public static final String PAGE_BYTES = "RAINCLOUD_PARQUET_PAGE_BYTES";
    public static final String PAGE_ROWS = "RAINCLOUD_PARQUET_PAGE_ROWS";
    public static final String DICTIONARY = "RAINCLOUD_PARQUET_DICTIONARY";
    public static final String DICTIONARY_PAGE_BYTES = "RAINCLOUD_PARQUET_DICTIONARY_PAGE_BYTES";
    public static final String PAGE_CHECKSUMS = "RAINCLOUD_PARQUET_PAGE_CHECKSUMS";
    public static final Set<String> CODECS = Set.of("zstd", "snappy", "gzip", "lz4", "brotli", "none");
    /** What 0 (no limit) means for a count: the libraries take an int. */
    public static final int NO_LIMIT = Integer.MAX_VALUE;

    /** Every option unset: zstd, statistics on, each library's own defaults. */
    public static final ParquetKnobs DEFAULT = new ParquetKnobs("zstd", null, true, null, null, null, null, null,
            null, null, null);

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
        boolean withStatistics = statistics == null || statistics;
        ParquetKnobs knobs = new ParquetKnobs(compression, level(env.apply(COMPRESSION_LEVEL)), withStatistics,
                count(STATISTICS_COLUMNS, env.apply(STATISTICS_COLUMNS)), toggle(PAGE_INDEX, env.apply(PAGE_INDEX)),
                count(PAGE_INDEX_COLUMNS, env.apply(PAGE_INDEX_COLUMNS)), count(PAGE_BYTES, env.apply(PAGE_BYTES)),
                count(PAGE_ROWS, env.apply(PAGE_ROWS)), toggle(DICTIONARY, env.apply(DICTIONARY)),
                count(DICTIONARY_PAGE_BYTES, env.apply(DICTIONARY_PAGE_BYTES)),
                toggle(PAGE_CHECKSUMS, env.apply(PAGE_CHECKSUMS)));
        if (!withStatistics) {
            if (Boolean.TRUE.equals(knobs.pageIndex) || knobs.statisticsColumns != null
                    || knobs.pageIndexColumns != null) {
                throw new IllegalArgumentException("a page index or per-column statistics ask for statistics, but "
                        + STATISTICS + " is 0");
            }
        }
        if (Boolean.FALSE.equals(knobs.pageIndex) && knobs.pageIndexColumns != null) {
            throw new IllegalArgumentException(PAGE_INDEX_COLUMNS + " asks for a page index, but " + PAGE_INDEX
                    + " is 0");
        }
        return knobs;
    }

    /**
     * An on/off setting as every lane reads it ({@code spec._env_switch}): unset or empty is null;
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

    /** A size or count: unset is null (the library's default); the count grammar otherwise, 0 no limit. */
    private static Integer count(String var, String raw) {
        if (raw == null) {
            return null;
        }
        return (int) Math.min(Knobs.count(var, raw, NO_LIMIT, NO_LIMIT), NO_LIMIT);
    }

    /** A compression level: unset or empty is null, else plain ASCII digits. */
    private static Integer level(String raw) {
        String value = raw == null ? "" : raw.strip();
        if (value.isEmpty()) {
            return null;
        }
        if (!value.chars().allMatch(c -> c >= '0' && c <= '9') || value.length() > 9) {
            throw new IllegalArgumentException(COMPRESSION_LEVEL + "='" + raw
                    + "' is not a compression level; give a whole number such as 3");
        }
        return Integer.parseInt(value);
    }

    /** Refuse an option this lane's library cannot honour, naming the lane, the setting and why. */
    public static IllegalArgumentException unsupported(String lane, String var, Object value, String why) {
        return new IllegalArgumentException(lane + " cannot honour " + var + "=" + value + ": " + why);
    }
}
