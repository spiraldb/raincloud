// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.raincloud.Raincloud;
import org.apache.arrow.memory.RootAllocator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** Standalone consumer of the installed runtime distribution. */
public class Read {
    public static void main(String[] args) throws Exception {
        Map<String, Object> options = new ObjectMapper().readValue(
            Files.readString(Path.of(args[0])), new TypeReference<Map<String, Object>>() {});
        String slug = args.length > 1 ? args[1] : "tiny";
        long expected = args.length > 2 ? Long.parseLong(args[2]) : 8;
        List<String> formats = List.of("arrow", "parquet", "vortex");
        for (String format : formats) {
            long rows = 0;
            try (var allocator = new RootAllocator();
                 var ds = Raincloud.load(slug, format, options);
                 var reader = ds.batches(allocator, 2)) {
                while (reader.loadNextBatch()) rows += reader.getVectorSchemaRoot().getRowCount();
            }
            if (rows != expected) throw new AssertionError(format + ": " + rows);
        }
        System.out.println("Java installed consumer: " + formats.size() + " formats");
    }
}
