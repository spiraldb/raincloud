// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud;

/** Stable native error categories; batch-time decoding can also throw IOException. */
public final class RaincloudException extends RuntimeException {
    /** Each kind carries its C ABI code (raincloud.h), which never changes. */
    public enum Kind {
        INVALID_ARGUMENT(1), CATALOG(2), MISSING_REVISION(3), UNKNOWN_SLUG(4), FORMAT_UNAVAILABLE(5),
        OFFLINE_MISS(6), ARTIFACT_NOT_FOUND(7), CHECKSUM_MISMATCH(8), CATALOG_CONFLICT(9),
        TRANSPORT(10), UNSUPPORTED_TYPE(11), IO(12), INTERNAL(13), CORRUPT_ARTIFACT(14);

        public final int code;
        Kind(int code) { this.code = code; }

        /** A code this client does not know (from a newer library) is INTERNAL. */
        public static Kind of(int code) {
            for (Kind kind : values()) if (kind.code == code) return kind;
            return INTERNAL;
        }
    }
    private final int code;
    RaincloudException(int code, String message) { super(message); this.code = code; }
    public int code() { return code; }
    public Kind kind() { return Kind.of(code); }
}
