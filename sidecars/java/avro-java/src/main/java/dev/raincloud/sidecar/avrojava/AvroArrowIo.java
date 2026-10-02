// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.avrojava;

import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.MaterializedTable;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Enumeration;
import org.apache.arrow.adapter.avro.ArrowToAvroUtils;
import org.apache.arrow.adapter.avro.AvroToArrow;
import org.apache.arrow.adapter.avro.AvroToArrowConfig;
import org.apache.arrow.adapter.avro.AvroToArrowConfigBuilder;
import org.apache.arrow.adapter.avro.AvroToArrowVectorIterator;
import org.apache.arrow.adapter.avro.producers.CompositeAvroProducer;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.DictionaryProvider;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/**
 * Arrow ⇆ Avro object container files through Arrow Java's Avro adapter and Apache
 * Avro's Java implementation.
 *
 * <p>Writing: the adapter maps the canonical's schema to an Avro schema and encodes each
 * row ({@link ArrowToAvroUtils}); Avro's {@link DataFileWriter} frames the rows into a
 * zstandard container file. Reading: Avro's {@link DataFileStream} yields each block's
 * decoded bytes and the adapter decodes them into Arrow vectors ({@link AvroToArrow}).
 * Nothing here converts a column: a type the adapter does not map is its error.
 */
final class AvroArrowIo {
    private AvroArrowIo() {}

    /**
     * The sync marker of every Avro file raincloud writes. An object container file
     * separates its blocks with a 16-byte marker the writer chooses, and Avro draws it at
     * random by default, so the same canonical would give a file with a different sha256 on
     * every build. The Rust lane writes the same marker.
     */
    static final byte[] SYNC_MARKER = "raincloud-avro01".getBytes(StandardCharsets.US_ASCII);

    /** Write the canonical {@code input} to {@code output}; on failure no {@code output} remains. */
    static void writeAvro(Path input, Path output, BufferAllocator allocator) throws Exception {
        try {
            write(input, output, allocator);
        } catch (Exception | Error e) {
            Files.deleteIfExists(output);
            throw e;
        }
    }

    private static void write(Path input, Path output, BufferAllocator allocator) throws Exception {
        try (SeekableByteChannel channel = Files.newByteChannel(input, StandardOpenOption.READ);
                ArrowFileReader reader =
                        new ArrowFileReader(channel, allocator, CommonsCompressionFactory.INSTANCE)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            // The reader is the dictionary provider: the adapter writes a dictionary column
            // as its values (or an enum), by its own rules.
            DictionaryProvider dictionaries = reader;
            org.apache.avro.Schema schema =
                    ArrowToAvroUtils.createAvroSchema(root.getSchema().getFields(), dictionaries);
            try (OutputStream out = new BufferedOutputStream(Files.newOutputStream(output));
                    DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
                writer.setCodec(CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL));
                writer.create(schema, out, SYNC_MARKER);
                ByteArrayOutputStream row = new ByteArrayOutputStream();
                BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(row, null);
                while (reader.loadNextBatch()) {
                    // A producer per batch, over that batch's vectors. Arrow Java 19.0.0's
                    // CompositeAvroProducer.resetProducerVectors, meant for this, binds every
                    // producer to the root's first vector (its loop never advances its index).
                    CompositeAvroProducer producer =
                            ArrowToAvroUtils.createCompositeProducer(root.getFieldVectors(), dictionaries);
                    for (int i = 0; i < root.getRowCount(); i++) {
                        row.reset();
                        producer.produce(encoder);
                        encoder.flush();
                        writer.appendEncoded(ByteBuffer.wrap(row.toByteArray()));
                    }
                }
            }
        }
    }

    /** The batches of the Avro file at {@code input}, decoded by the adapter. */
    static BatchSource openAvro(Path input, BufferAllocator allocator) throws IOException {
        InputStream in = new BufferedInputStream(Files.newInputStream(input));
        DataFileStream<Object> file;
        try {
            file = new DataFileStream<>(in, new GenericDatumReader<>());
        } catch (IOException | RuntimeException e) {
            in.close();
            throw e;
        }
        org.apache.avro.Schema avroSchema = file.getSchema();
        DictionaryProvider.MapDictionaryProvider dictionaries = new DictionaryProvider.MapDictionaryProvider();
        AvroToArrowConfig config =
                new AvroToArrowConfigBuilder(allocator).setProvider(dictionaries).build();
        // The blocks' decoded bytes, one after another: the rows, as the adapter reads them.
        Enumeration<InputStream> blocks = new Enumeration<>() {
            @Override
            public boolean hasMoreElements() {
                return file.hasNext();
            }

            @Override
            public InputStream nextElement() {
                try {
                    ByteBuffer block = file.nextBlock();
                    return new java.io.ByteArrayInputStream(
                            block.array(), block.arrayOffset() + block.position(), block.remaining());
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }
        };
        AvroToArrowVectorIterator rows;
        VectorSchemaRoot first;
        try {
            rows = AvroToArrow.avroToArrowIterator(avroSchema,
                    DecoderFactory.get().binaryDecoder(new SequenceInputStream(blocks), null), config);
            first = rows.hasNext() ? rows.next() : null;
        } catch (IOException | RuntimeException e) {
            file.close();
            throw e;
        }
        // The schema of the vectors the adapter decodes into, which is what it reads the file
        // as. With no rows there are none, and its schema mapping says.
        Schema schema = first != null ? first.getSchema() : AvroToArrow.avroToAvroSchema(avroSchema, config);
        return new BatchSource() {
            private VectorSchemaRoot pending = first;

            @Override
            public MaterializedTable empty() {
                MaterializedTable.Builder builder = new MaterializedTable.Builder();
                builder.initialize(schema, dictionaries);
                return builder.build();
            }

            @Override
            public MaterializedTable next() {
                VectorSchemaRoot next = pending;
                pending = null;
                if (next == null) {
                    if (!rows.hasNext()) {
                        return null;
                    }
                    next = rows.next();
                }
                try (VectorSchemaRoot batch = next) {
                    MaterializedTable.Builder builder = new MaterializedTable.Builder();
                    builder.initialize(schema, dictionaries);
                    builder.appendBatch(batch, dictionaries);
                    return builder.build();
                }
            }

            @Override
            public void close() throws IOException {
                try {
                    if (pending != null) {
                        pending.close();
                    }
                    rows.close();
                } finally {
                    file.close();
                }
            }
        };
    }
}
