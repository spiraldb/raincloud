// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

/** The Parquet write options as every JVM Parquet lane reads them. */
class ParquetKnobsTest {

    private static ParquetKnobs from(Map<String, String> vars) {
        return ParquetKnobs.from(name -> vars.get(name.replace("RAINCLOUD_PARQUET_", "")));
    }

    @Test
    void unsetIsEachLibrarysDefault() {
        assertEquals(ParquetKnobs.DEFAULT, from(Map.of()));
        assertEquals(ParquetKnobs.DEFAULT, from(Map.of("PAGE_INDEX", " ")));
    }

    @Test
    void readsWhatThePythonLaneWrites() {
        ParquetKnobs knobs = from(Map.of("COMPRESSION", "lz4", "STATISTICS", "1", "PAGE_INDEX", " On ",
                "PAGE_BYTES", "4096", "PAGE_ROWS", "0"));
        assertEquals(new ParquetKnobs("lz4", true, true, 4096, ParquetKnobs.PAGE_LIMIT), knobs);
    }

    @Test
    void refusesWhatNoLaneCanRead() {
        Map<Map<String, String>, String> cases = Map.of(
                Map.of("PAGE_INDEX", "maybe"), "is not a switch",
                Map.of("COMPRESSION", "lzo"), "is not one of",
                Map.of("PAGE_BYTES", "1MiB"), "is not a number",
                Map.of("PAGE_INDEX", "1", "STATISTICS", "0"), "asks for page statistics");
        cases.forEach((vars, error) -> {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> from(vars));
            assertTrue(e.getMessage().contains(error), vars + ": " + e.getMessage());
        });
    }
}
