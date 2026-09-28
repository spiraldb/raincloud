// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal {@code --key value} / {@code --key=value} parser for the sidecar CLI
 * contracts. As strict as the Rust (clap) sidecars: an unknown key, a positional
 * argument, a repeated key, or a missing value (including a value that is itself
 * an option) is an {@link IllegalArgumentException}, which the mains turn into a
 * usage error (exit 2).
 */
public final class Cli {
    private final Map<String, String> opts;

    private Cli(Map<String, String> opts) {
        this.opts = opts;
    }

    public static Cli parse(String[] args, String... keys) {
        List<String> allowed = List.of(keys);
        Map<String, String> opts = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--") || a.length() == 2) {
                throw new IllegalArgumentException("unexpected argument '" + a + "'");
            }
            String key = a.substring(2);
            String val;
            int eq = key.indexOf('=');
            if (eq >= 0) {
                val = key.substring(eq + 1);
                key = key.substring(0, eq);
            } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                val = args[++i];
            } else {
                throw new IllegalArgumentException("--" + key + " needs a value");
            }
            if (!allowed.contains(key)) {
                throw new IllegalArgumentException("unknown option --" + key);
            }
            if (opts.put(key, val) != null) {
                throw new IllegalArgumentException("--" + key + " given more than once");
            }
        }
        return new Cli(opts);
    }

    public Path path(String key) {
        String v = opts.get(key);
        return (v == null || v.isEmpty()) ? null : Path.of(v);
    }

    public Path require(String key) {
        Path p = path(key);
        if (p == null) {
            throw new IllegalArgumentException("missing required --" + key);
        }
        return p;
    }
}
