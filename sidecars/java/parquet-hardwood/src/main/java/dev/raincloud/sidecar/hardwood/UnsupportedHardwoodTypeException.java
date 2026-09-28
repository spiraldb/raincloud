// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.hardwood;

/**
 * A type or shape the {@code parquet@hardwood} lane cannot carry between Arrow and Hardwood:
 * one Hardwood cannot write or read, or one this lane's bridge does not map. The writer
 * reports it as {@code "unsupported type"} before the file is written and as an unmeasured
 * self-verify after; the reader reports it as a comparator gap ({@code skip}). Never a pass,
 * and never a fail for the lane's own limit.
 */
public final class UnsupportedHardwoodTypeException extends RuntimeException {
    public UnsupportedHardwoodTypeException(String message) {
        super(message);
    }
}
