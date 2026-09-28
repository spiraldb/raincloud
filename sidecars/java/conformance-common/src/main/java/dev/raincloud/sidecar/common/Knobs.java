// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.util.regex.Pattern;

/**
 * The row-group knobs every JVM writer lane reads, with the grammar every lane shares
 * ({@code raincloud/pipeline/spec.py::_env_count}, the Rust lane, and the cases in
 * {@code sidecars/knob_cases.json}). A recipe's {@code write.row_group_size_rows} reaches a
 * sidecar as {@link #MAX_ROWS}: {@code SidecarExporter} sets it in the child's environment, so
 * the recipe cap wins in every lane as it does in parquet@py.
 */
public final class Knobs {
    private Knobs() {}

    public static final String MAX_ROWS = "RAINCLOUD_ROW_GROUP_MAX_ROWS";
    public static final String TARGET_ENCODED_BYTES = "RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES";

    /** Python's defaults ({@code spec.row_group_max_rows} / {@code spec.row_group_target_encoded_bytes}). */
    public static final long DEFAULT_MAX_ROWS = 10_000_000L;
    public static final long DEFAULT_TARGET_ENCODED_BYTES = 128L << 20;

    /** {@code ^[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$}: ASCII digits, no sign, no separators, no inf/nan. */
    private static final Pattern NUMBER = Pattern.compile("[0-9]+(\\.[0-9]+)?([eE][+-]?[0-9]+)?");

    /** ASCII whitespace as Python's {@code string.whitespace} has it (vertical tab included). */
    private static boolean isKnobSpace(char c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\u000b' || c == '\f';
    }

    private static String trimKnobSpace(String raw) {
        int start = 0, end = raw.length();
        while (start < end && isKnobSpace(raw.charAt(start))) {
            start++;
        }
        while (end > start && isKnobSpace(raw.charAt(end - 1))) {
            end--;
        }
        return raw.substring(start, end);
    }

    /**
     * A whole-number knob: unset → {@code defaultValue}; after trimming ASCII whitespace,
     * empty or 0 → {@code disabled} (no cap); otherwise a plain non-negative number matching
     * {@link #NUMBER}, truncated toward zero ({@code 1e6} is 1,000,000) and saturating at
     * {@code Long.MAX_VALUE}. A value that is not valid UTF-8, is not such a number, is not
     * finite, or truncates to 0 raises rather than quietly meaning the default.
     */
    public static long count(String var, String raw, long defaultValue, long disabled) {
        if (raw == null) {
            return defaultValue;
        }
        // The JVM decodes an environment value it cannot read as UTF-8 with U+FFFD.
        if (raw.indexOf('�') >= 0) {
            throw new IllegalArgumentException(var + "='" + raw
                    + "' is not valid UTF-8; give a plain number (bytes or rows), or 0 to disable");
        }
        String value = trimKnobSpace(raw);
        if (value.isEmpty()) {
            return disabled;
        }
        if (!NUMBER.matcher(value).matches()) {
            throw new IllegalArgumentException(var + "='" + raw
                    + "' is not a number; give a plain value (bytes or rows) such as 1e6, or 0 to disable");
        }
        double number = Double.parseDouble(value);
        if (!Double.isFinite(number)) {
            throw new IllegalArgumentException(var + "='" + raw + "' must be a finite number >= 0 (0 disables it)");
        }
        if (number == 0) {
            return disabled;
        }
        long truncated = (long) number; // saturates at Long.MAX_VALUE
        if (truncated == 0) {
            throw new IllegalArgumentException(var + "='" + raw + "' rounds down to 0; give at least 1, or 0 to disable");
        }
        return truncated;
    }
}
