// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.vortexjni;

import java.io.IOException;
import java.lang.ref.Reference;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.dictionary.DictionaryEncoder;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.CanonicalReader;
import dev.raincloud.sidecar.common.LogicalCompare;
import dev.raincloud.sidecar.common.Verdict;
import dev.raincloud.sidecar.common.WriterMain;
import dev.vortex.api.Session;
import dev.vortex.api.VortexWriter;
import dev.vortex.jni.NativeLoader;

/**
 * {@code vortex@jni} WRITE-conformance sidecar (see {@link WriterMain} for the CLI contract
 * and the report):
 *
 * <pre>{@code
 *   raincloud-export-vortex-jni --input <slug.arrow.zstd> --output <dest> --report <report.json>
 * }</pre>
 *
 * Streams the canonical's batches into a {@code .vortex} file through vortex-jni's
 * {@link VortexWriter} (each batch handed over the Arrow C Data Interface, Vortex's default
 * write strategy), then self-verifies by reading it back through vortex-jni
 * ({@link ConformanceReader}). Like vortex@py and vortex@rs it writes a VARIANT column as its
 * storage struct, so Vortex cannot read the extension as native VARIANT, and reports the
 * annotation loss. Top-level dictionary columns are handed over decoded: vortex-jni exports
 * the writer's schema with no dictionary provider, and Vortex chooses its own encodings
 * either way. A dictionary inside a nested column is refused.
 */
public final class ConformanceWriter {
    static final String CELL = "vortex@jni";
    static final String VARIANT_LOSS = "VARIANT annotation not preserved (column kept as its shredded struct)";

    private static final String EXTENSION_NAME = "ARROW:extension:name";
    private static final String VARIANT_EXTENSION = "arrow.parquet.variant";
    private static final List<String> VARIANT_KEYS =
            List.of(EXTENSION_NAME, "ARROW:extension:metadata", "__variant_type");

    private static Verdict selfVerify(Path input, Path output, BufferAllocator allocator) throws Exception {
        try (BatchSource expected = CanonicalReader.open(input, allocator);
                BatchSource got = ConformanceReader.open(output, allocator)) {
            return LogicalCompare.compare(CELL, got, expected);
        }
    }

    public static void main(String[] args) {
        int code = run(args, ConformanceWriter::selfVerify);
        if (code != 0) {
            System.exit(code);
        }
    }

    /** {@link WriterMain#execute} for this lane, with {@code verify} as its self-verify. */
    static int run(String[] args, WriterMain.SelfVerify verify) {
        // Not measured: this lane strips the extension before Vortex sees it (storageSchema),
        // so the loss is known without reading the file back.
        return WriterMain.execute(CELL, (output, columns, allocator) -> VARIANT_LOSS, args,
                ConformanceWriter::writeVortex, verify, t -> false);
    }

    /** Stream the canonical into {@code output}; a failed write removes what it left. */
    static void writeVortex(Path canonical, Path output, BufferAllocator allocator) throws IOException {
        Files.deleteIfExists(output);
        NativeLoader.loadJni();
        boolean written = false;
        try (SeekableByteChannel channel = Files.newByteChannel(canonical, StandardOpenOption.READ);
                ArrowFileReader input = new ArrowFileReader(channel, allocator, CommonsCompressionFactory.INSTANCE)) {
            VectorSchemaRoot root = input.getVectorSchemaRoot();
            Schema storage = storageSchema(root.getSchema(), input);
            Session session = Session.create();
            // vortex-jni 0.86.1's Builder.build() exports the schema, lets the native writer
            // borrow it, and never releases it, so that export (a few hundred bytes a column)
            // outlives the writer. It gets an allocator of its own, left open until the process
            // exits, so the lane's allocator still reports any leak of this lane's own.
            BufferAllocator schemaExport = new RootAllocator();
            try (VortexWriter writer = VortexWriter.builder(
                    session, output.toAbsolutePath().toUri().toString(), storage, schemaExport).build()) {
                while (input.loadNextBatch()) {
                    writeBatch(writer, storage, root, input, allocator);
                }
            } finally {
                // The writer runs on the session's runtime: keep it reachable until it is closed.
                Reference.reachabilityFence(session);
            }
            written = true;
        } finally {
            if (!written) {
                try {
                    Files.deleteIfExists(output);
                } catch (IOException cleanup) {
                    System.err.println("[" + CELL + "] could not remove partial output " + output + ": " + cleanup);
                }
            }
        }
    }

    private static void writeBatch(VortexWriter writer, Schema storage, VectorSchemaRoot root, ArrowFileReader input,
            BufferAllocator allocator) throws IOException {
        List<FieldVector> vectors = new ArrayList<>();
        List<FieldVector> decoded = new ArrayList<>();
        try {
            for (FieldVector vector : root.getFieldVectors()) {
                if (vector.getField().getDictionary() != null) {
                    Dictionary dictionary = input.lookup(vector.getField().getDictionary().getId());
                    vector = (FieldVector) DictionaryEncoder.decode(vector, dictionary);
                    decoded.add(vector);
                }
                vectors.add(vector);
            }
            // The canonical's vectors under the storage schema; not closed here, the reader owns them.
            VectorSchemaRoot batch = new VectorSchemaRoot(storage.getFields(), vectors, root.getRowCount());
            try (ArrowArray array = ArrowArray.allocateNew(allocator);
                    ArrowSchema schema = ArrowSchema.allocateNew(allocator)) {
                Data.exportVectorSchemaRoot(allocator, batch, null, array, schema);
                try {
                    writer.writeBatch(array.memoryAddress(), schema.memoryAddress());
                } finally {
                    // Whatever the native writer did not take ownership of (it moves the array
                    // and borrows the schema) is released here; a moved struct's release is a no-op.
                    array.release();
                    schema.release();
                }
            }
        } finally {
            for (FieldVector vector : decoded) {
                vector.close();
            }
        }
    }

    /**
     * The schema Vortex is handed: VARIANT columns stripped to their storage struct (the
     * {@code vortex_storage_schema} of the Rust lane), and top-level dictionaries as their values.
     */
    static Schema storageSchema(Schema canonical, ArrowFileReader dictionaries) {
        List<Field> fields = new ArrayList<>();
        for (Field field : canonical.getFields()) {
            Map<String, String> metadata = field.getMetadata();
            if (VARIANT_EXTENSION.equals(metadata.get(EXTENSION_NAME))) {
                metadata = new HashMap<>(metadata);
                metadata.keySet().removeAll(VARIANT_KEYS);
            }
            Field values = field;
            if (field.getDictionary() != null) {
                values = dictionaries.lookup(field.getDictionary().getId()).getVector().getField();
            }
            requireNoNestedDictionary(field.getName(), values.getChildren());
            fields.add(new Field(field.getName(),
                    new FieldType(field.isNullable(), values.getType(), null, metadata.isEmpty() ? null : metadata),
                    values.getChildren()));
        }
        return new Schema(fields, canonical.getCustomMetadata());
    }

    private static void requireNoNestedDictionary(String path, List<Field> children) {
        for (Field child : children) {
            if (child.getDictionary() != null) {
                throw new UnsupportedOperationException(path + "." + child.getName()
                        + ": a dictionary inside a nested column, which this lane does not decode");
            }
            requireNoNestedDictionary(path + "." + child.getName(), child.getChildren());
        }
    }
}
