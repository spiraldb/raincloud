// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** The row-group knob grammar every JVM writer lane reads. */
class KnobsTest {

    @Test
    void followsTheSharedCases() throws IOException {
        // The same table pytest and cargo test read: one grammar in every lane.
        JsonNode table = new ObjectMapper().readTree(Path.of(System.getProperty("raincloud.knobCases")).toFile());
        String v = Knobs.MAX_ROWS;
        for (JsonNode c : table.get("cases")) {
            String raw = c.get("raw").asText();
            if (c.has("error")) {
                IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                        () -> Knobs.count(v, raw, 10L, -1L), raw);
                assertTrue(e.getMessage().contains(v + "=") && e.getMessage().contains(c.get("error").asText()),
                        raw + ": " + e.getMessage());
            } else {
                long expected = c.get("value").isNull() ? -1L : c.get("value").asLong();
                assertEquals(expected, Knobs.count(v, raw, 10L, -1L), raw);
            }
        }
        assertEquals(10L, Knobs.count(v, null, 10L, -1L), "unset -> default");
        assertEquals(Long.MAX_VALUE, Knobs.count(v, "1e300", 10L, -1L), "saturates: no cap");
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Knobs.count(v, "1�", 10L, -1L));
        assertTrue(e.getMessage().contains("not valid UTF-8"), e.getMessage());
    }
}
