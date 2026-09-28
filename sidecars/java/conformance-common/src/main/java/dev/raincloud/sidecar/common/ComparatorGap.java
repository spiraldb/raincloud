// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

/**
 * A shape this comparator cannot judge, found while reading values rather than
 * from the schema (for example a dictionary-encoded child of a nested column).
 * Reader mains report it as {@code skip}; the writer reports the round-trip as
 * unmeasured. Never a pass, and never a fail for the comparator's own limit.
 */
public final class ComparatorGap extends RuntimeException {
    public ComparatorGap(String message) {
        super(message);
    }
}
