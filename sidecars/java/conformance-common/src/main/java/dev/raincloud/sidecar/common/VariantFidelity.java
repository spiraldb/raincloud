// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.common;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Whether a writer lane kept the canonical's VARIANT columns, measured on the artifact it wrote
 * rather than assumed from the lane: the {@code variant_faithful} of the WRITE report.
 */
public final class VariantFidelity {
    private VariantFidelity() {}

    /** raincloud's top-level VARIANT marker (see {@code discovery._is_variant_field}). */
    private static final String MARKER = "__variant_type";
    /** Arrow's field-metadata key for an extension name. */
    static final String EXTENSION_NAME = "ARROW:extension:name";
    /** The Arrow canonical extension of a Parquet VARIANT column. */
    public static final String EXTENSION = "arrow.parquet.variant";

    /** Measures one written artifact. */
    public interface Check {
        /**
         * Why {@code output} does not keep the VARIANT {@code columns}, or {@code null} when it
         * does. {@code columns} is never empty.
         */
        String loss(Path output, List<String> columns, BufferAllocator allocator) throws Exception;
    }

    /** The top-level columns a Parquet file declares with the VARIANT logical type. */
    public interface Declared {
        Set<String> columns(Path parquet) throws Exception;
    }

    /**
     * The canonical's top-level VARIANT columns: raincloud's marker or the
     * {@code arrow.parquet.variant} extension, as {@code discovery._is_variant_field} reads them.
     */
    public static List<String> columns(List<Field> fields) {
        List<String> names = new ArrayList<>();
        for (Field f : fields) {
            Map<String, String> md = f.getMetadata();
            if (md != null && (md.containsKey(MARKER) || EXTENSION.equals(md.get(EXTENSION_NAME)))) {
                names.add(f.getName());
            }
        }
        return names;
    }

    /** Whether {@code field} carries the {@code arrow.parquet.variant} extension. */
    public static boolean isVariant(Field field) {
        Map<String, String> md = field.getMetadata();
        return md != null && EXTENSION.equals(md.get(EXTENSION_NAME));
    }

    /**
     * The Parquet lanes' check: each column must be declared with Parquet's VARIANT logical type
     * ({@code declared}, the lane's own reading of the footer) and read back through the lane
     * ({@code readBack}) with the {@code arrow.parquet.variant} extension. Neither alone is
     * enough: an {@code ARROW:schema} hint carries the extension name through a plain group, and
     * a reader may drop what the file declares.
     */
    public static Check parquet(Declared declared, ReaderMain.Opener readBack) {
        return (output, columns, allocator) -> {
            Set<String> annotated = declared.columns(output);
            List<Field> fields;
            try (BatchSource got = readBack.open(output, allocator)) {
                fields = got.empty().fields;
            }
            List<String> losses = new ArrayList<>();
            for (String column : columns) {
                if (!annotated.contains(column)) {
                    losses.add("column \"" + column + "\": the file declares no Parquet VARIANT logical type");
                }
                Field field = fields.stream().filter(f -> f.getName().equals(column)).findFirst().orElse(null);
                if (field == null || !isVariant(field)) {
                    losses.add("column \"" + column + "\": read back without the " + EXTENSION + " extension");
                }
            }
            return losses.isEmpty() ? null : String.join("; ", losses);
        };
    }
}
