// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/** The representation rules every lane's comparator shares (sidecars/compare_cases). */
class CompareCasesTest {

    @Test
    void followsTheSharedCases() throws IOException {
        // The same files pytest and cargo test read: one verdict per case in every lane.
        Path dir = Path.of(System.getProperty("raincloud.compareCases"));
        JsonNode table = new ObjectMapper().readTree(dir.resolve("cases.json").toFile());
        List<String> wrong = new ArrayList<>();
        int cases = 0;
        try (RootAllocator allocator = new RootAllocator()) {
            for (JsonNode c : table.get("cases")) {
                String name = c.get("name").asText();
                String want = "equal".equals(c.get("verdict").asText()) ? "pass" : "fail";
                boolean gap = c.path("gap").asBoolean(false);
                Verdict v;
                try (BatchSource got = open(dir.resolve(name + ".got.arrow"), allocator);
                        BatchSource expected = open(dir.resolve(name + ".expected.arrow"), allocator)) {
                    v = LogicalCompare.compare(name, got, expected);
                }
                // A gap case may stay unmeasured, never take the opposite verdict.
                if (!v.status.equals(want) && !(gap && "skip".equals(v.status))) {
                    wrong.add(name + ": want " + want + (gap ? " or skip" : "") + ", got " + v.status
                            + " (" + v.note + "; " + v.detail + ")");
                }
                cases++;
            }
        }
        assertFalse(cases == 0, "no cases in " + dir);
        assertTrue(wrong.isEmpty(), String.join("\n", wrong));
    }

    private static BatchSource open(Path file, RootAllocator allocator) throws IOException {
        return BatchSource.of(new ArrowFileReader(Files.newByteChannel(file), allocator));
    }
}
