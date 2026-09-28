// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

/**
 * A read-conformance verdict — mirrors {@code export/base.py:Verdict}. {@code status}
 * is one of {@code pass|fail|na|skip|spec_ambiguous}.
 */
public final class Verdict {
    public final String status;
    public final String note;
    public final String detail;

    public Verdict(String status, String note, String detail) {
        this.status = status;
        this.note = note == null ? "" : note;
        this.detail = detail == null ? "" : detail;
    }

    public static Verdict pass(String note) {
        return new Verdict("pass", note, "");
    }

    public static Verdict fail(String note, String detail) {
        return new Verdict("fail", note, detail);
    }

    public static Verdict skip(String note, String detail) {
        return new Verdict("skip", note, detail);
    }
}
