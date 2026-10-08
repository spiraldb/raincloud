// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.util.Arrays;
import java.util.Locale;

/**
 * How every lane reads a write setting from its environment ({@code raincloud/pipeline/spec.py},
 * which declares them all, and the Rust lane): unset or empty is {@code null}, the library's
 * default; anything malformed is refused naming the variable. The build passes each set setting
 * in one canonical form, so a sidecar run by hand reads the same grammar.
 */
public final class WriteSettings {
    private WriteSettings() {}

    /** What 0 (no limit) means for a size or count: the libraries take an int. */
    public static final int NO_LIMIT = Integer.MAX_VALUE;

    /** An on/off setting: 1/true/yes/on or 0/false/no/off, in any case. */
    public static Boolean toggle(String var, String raw) {
        String value = raw == null ? "" : raw.strip().toLowerCase(Locale.ROOT);
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

    /** A size or count, in the count grammar ({@link Knobs#count}); 0 is {@link #NO_LIMIT}. */
    public static Integer count(String var, String raw) {
        if (raw == null) {
            return null;
        }
        return (int) Math.min(Knobs.count(var, raw, NO_LIMIT, NO_LIMIT), NO_LIMIT);
    }

    /** A compression level: plain ASCII digits. */
    public static Integer level(String var, String raw) {
        String value = raw == null ? "" : raw.strip();
        if (value.isEmpty()) {
            return null;
        }
        if (!value.chars().allMatch(c -> c >= '0' && c <= '9') || value.length() > 9) {
            throw new IllegalArgumentException(var + "='" + raw
                    + "' is not a compression level; give a whole number such as 3");
        }
        return Integer.parseInt(value);
    }

    /** One of {@code choices}, in any case. */
    public static String choice(String var, String raw, String... choices) {
        String value = raw == null ? "" : raw.strip().toLowerCase(Locale.ROOT);
        if (value.isEmpty()) {
            return null;
        }
        if (!Arrays.asList(choices).contains(value)) {
            throw new IllegalArgumentException(var + "='" + raw + "' is not one of " + String.join(", ", choices));
        }
        return value;
    }

    /** Refuse a setting this lane's library cannot honour, naming the lane, the setting and why. */
    public static IllegalArgumentException unsupported(String lane, String var, Object value, String why) {
        return new IllegalArgumentException(lane + " cannot honour " + var + "=" + value + ": " + why);
    }
}
