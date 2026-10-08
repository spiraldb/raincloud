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
        assertEquals(ParquetKnobs.DEFAULT, from(Map.of("PAGE_INDEX", " ", "COMPRESSION_LEVEL", "")));
    }

    @Test
    void readsWhatThePythonLaneWrites() {
        ParquetKnobs knobs = from(Map.of("COMPRESSION", "gzip", "COMPRESSION_LEVEL", "9", "STATISTICS", "1",
                "STATISTICS_COLUMNS", "100", "PAGE_INDEX", " On ", "PAGE_INDEX_COLUMNS", "10", "PAGE_BYTES", "4096",
                "PAGE_ROWS", "0", "DICTIONARY", "off", "DICTIONARY_PAGE_BYTES", "65536"));
        assertEquals(new ParquetKnobs("gzip", 9, true, 100, true, 10, 4096, ParquetKnobs.NO_LIMIT, false, 65536,
                null), knobs);
        assertEquals(true, from(Map.of("PAGE_CHECKSUMS", "yes")).pageChecksums());
    }

    @Test
    void refusesWhatNoLaneCanRead() {
        Map<Map<String, String>, String> cases = Map.of(
                Map.of("PAGE_INDEX", "maybe"), "is not a switch",
                Map.of("COMPRESSION", "lzo"), "is not one of",
                Map.of("PAGE_BYTES", "1MiB"), "is not a number",
                Map.of("COMPRESSION_LEVEL", "-1"), "is not a compression level",
                Map.of("PAGE_INDEX", "1", "STATISTICS", "0"), "ask for statistics",
                Map.of("STATISTICS_COLUMNS", "5", "STATISTICS", "0"), "ask for statistics",
                Map.of("PAGE_INDEX", "0", "PAGE_INDEX_COLUMNS", "5"), "asks for a page index");
        cases.forEach((vars, error) -> {
            IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> from(vars));
            assertTrue(e.getMessage().contains(error), vars + ": " + e.getMessage());
        });
    }
}
