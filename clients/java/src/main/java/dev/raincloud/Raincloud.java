// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud;

import java.util.Map;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Datasets-style prepared-data access. Artifact reads never invoke a builder. */
public final class Raincloud {
    static final ObjectMapper JSON = new ObjectMapper();
    private Raincloud() {}
    public static Dataset load(String slug) { return load(slug, "auto", Map.of()); }
    public static Dataset load(String slug, String format, Map<String, ?> settings) {
        try { return new Dataset(slug, format, JSON.writeValueAsString(settings)); }
        catch (JsonProcessingException e) { throw new IllegalArgumentException("invalid settings", e); }
    }
}
