// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Hand-rolled JSON report writers for the two sidecar contracts (no JSON dep).
 *
 * <ul>
 *   <li>READ  ({@code readers.py}): {@code {"status", "note", "detail"}}.
 *   <li>WRITE ({@code sidecar.py}): {@code {"roundtrip", "variant_faithful", "note"}}.
 *       {@code roundtrip} is JSON {@code null} when the self-verify could not be
 *       measured (a comparator gap), as distinct from a measured {@code false}.
 * </ul>
 */
public final class Report {
    private Report() {}

    public static void writeRead(Path path, String status, String note, String detail) throws IOException {
        String json = "{\"status\":" + quote(status)
                + ",\"note\":" + quote(note)
                + ",\"detail\":" + quote(detail) + "}";
        Files.write(path, json.getBytes(StandardCharsets.UTF_8));
    }

    public static void writeWrite(Path path, Boolean roundtrip, boolean variantFaithful, String note)
            throws IOException {
        String json = "{\"roundtrip\":" + roundtrip
                + ",\"variant_faithful\":" + variantFaithful
                + ",\"note\":" + quote(note) + "}";
        Files.write(path, json.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * {@code Type: message} for a failure, followed by each distinct cause's, so a wrapped
     * native or I/O error keeps the reason it was wrapped around.
     */
    public static String describe(Throwable t) {
        StringBuilder b = new StringBuilder(t.getClass().getSimpleName() + ": " + t.getMessage());
        java.util.Set<Throwable> seen = new java.util.HashSet<>();
        seen.add(t);
        for (Throwable cause = t.getCause(); cause != null && seen.add(cause); cause = cause.getCause()) {
            if (cause.getMessage() != null && t.getMessage() != null && t.getMessage().contains(cause.getMessage())) {
                continue; // already said
            }
            b.append(" (caused by ").append(cause.getClass().getSimpleName()).append(": ")
                    .append(cause.getMessage()).append(')');
        }
        return b.toString();
    }

    static String quote(String s) {
        if (s == null) {
            s = "";
        }
        StringBuilder b = new StringBuilder(s.length() + 2);
        b.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) {
                        b.append(String.format("\\u%04x", (int) c));
                    } else {
                        b.append(c);
                    }
                }
            }
        }
        return b.append('"').toString();
    }
}
