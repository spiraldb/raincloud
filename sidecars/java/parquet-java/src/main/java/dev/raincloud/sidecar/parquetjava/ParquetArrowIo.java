// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
package dev.raincloud.sidecar.parquetjava;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import dev.raincloud.sidecar.common.BatchSource;
import dev.raincloud.sidecar.common.Knobs;
import dev.raincloud.sidecar.common.ParquetKnobs;
import dev.spiraldb.parquet.arrow.Compression;
import dev.spiraldb.parquet.arrow.CoreCompressionCodecFactory;
import dev.spiraldb.parquet.arrow.ParquetArrow;
import dev.spiraldb.parquet.arrow.ParquetArrowWriter;
import dev.spiraldb.parquet.arrow.WriteOptions;

/**
 * Arrow⇆Parquet I/O for the {@code parquet@java} lane, via parquet-arrow-java (Hadoop-free).
 * parquet-java has no native Arrow path, so both directions hop through parquet-arrow-java, which
 * drives parquet-java's {@code ParquetFileWriter}/record reader underneath — the bytes are still
 * parquet-java's, parquet-arrow-java only maps Arrow ⇆ parquet-java's record model.
 */
public final class ParquetArrowIo {
    private ParquetArrowIo() {}

    /**
     * Backstop cap on rows per row group, from {@link Knobs#MAX_ROWS} (the recipe's
     * {@code write.row_group_size_rows} when it declares one). Default 10,000,000; disabled
     * means {@code Integer.MAX_VALUE}, Python's figure.
     */
    static int rowGroupMaxRows(String raw) {
        // A cap above Integer.MAX_VALUE cannot bind: parquet-arrow-java counts rows in an int.
        return (int) Math.min(Integer.MAX_VALUE,
                Knobs.count(Knobs.MAX_ROWS, raw, Knobs.DEFAULT_MAX_ROWS, Integer.MAX_VALUE));
    }

    /**
     * Target size of one row group, from {@link Knobs#TARGET_ENCODED_BYTES}.
     * Default 128 MiB; disabled means {@code 1L << 62}, Python's figure.
     *
     * <p>What parquet-arrow-java measures against it: parquet-java's
     * {@code ColumnWriteStore.getBufferedSize()} after each row, which counts the pages
     * already closed in this group as buffered (compressed) bytes plus the open pages'
     * encoded bytes. It is therefore not the pre-compression size arrow-rs's planner
     * pass and the Python lane measure (see {@code spec.row_group_target_encoded_bytes});
     * the same knob gives larger parquet@java groups.</p>
     */
    static long rowGroupTargetEncodedBytes(String raw) {
        return Knobs.count(Knobs.TARGET_ENCODED_BYTES, raw, Knobs.DEFAULT_TARGET_ENCODED_BYTES, 1L << 62);
    }

    /**
     * The write options for the two raw knob values ({@code null} when unset).
     *
     * <p>parquet-arrow-java's own maxRowGroupRows default (1,048,576) would win over
     * the byte target on a narrow table (TPC-H lineitem encodes to ~63 MiB at 1M
     * rows, half the target), so the row cap is the raincloud backstop instead,
     * exactly as parquet-java ships {@code parquet.block.row.count.limit} effectively
     * off. The backstop remains for the shape bytes cannot catch: one narrow integer
     * column encodes to about a byte a row, so a 128 MiB target alone would put ~134M
     * rows in a group.</p>
     */
    static WriteOptions writeOptions(String maxRows, String targetEncodedBytes) {
        return writeOptions(maxRows, targetEncodedBytes, ParquetKnobs.DEFAULT);
    }

    /**
     * {@link #writeOptions(String, String)} with the Parquet options every lane is given.
     *
     * <p>parquet-arrow-java has no LZ4 or Brotli codec, and no switch for the page index:
     * parquet-java writes a ColumnIndex and OffsetIndex whenever statistics are on, so
     * {@code RAINCLOUD_PARQUET_PAGE_INDEX=0} with statistics on is refused, as are those
     * codecs, rather than written some other way.</p>
     */
    static WriteOptions writeOptions(String maxRows, String targetEncodedBytes, ParquetKnobs knobs) {
        WriteOptions.Builder builder = WriteOptions.builder()
                .compression(compression(knobs.compression()))
                .statisticsEnabled(knobs.statistics())
                .maxRowGroupRows(rowGroupMaxRows(maxRows))
                .targetRowGroupBytes(rowGroupTargetEncodedBytes(targetEncodedBytes));
        if (Boolean.FALSE.equals(knobs.pageIndex()) && knobs.statistics()) {
            throw ParquetKnobs.unsupported("parquet@java", ParquetKnobs.PAGE_INDEX, 0,
                    "parquet-java writes a page index whenever statistics are on");
        }
        if (knobs.pageBytes() != null) {
            builder.pageSizeBytes(knobs.pageBytes());
        }
        if (knobs.pageRows() != null) {
            builder.pageRowLimit(knobs.pageRows());
        }
        return builder.build();
    }

    private static Compression compression(String codec) {
        switch (codec) {
            case "zstd":
                return Compression.ZSTD;
            case "snappy":
                return Compression.SNAPPY;
            case "gzip":
                return Compression.GZIP;
            case "none":
                return Compression.UNCOMPRESSED;
            default:
                throw ParquetKnobs.unsupported("parquet@java", ParquetKnobs.COMPRESSION, codec,
                        "parquet-arrow-java writes only zstd, snappy, gzip or no compression");
        }
    }

    /**
     * Streams the canonical Arrow IPC file straight into a Parquet (no intermediate copy).
     *
     * <p>Atomic w.r.t. failure: any failed write — an exception such as {@link
     * dev.spiraldb.parquet.arrow.UnsupportedParquetTypeException} mid-stream, or an
     * {@link Error} such as {@link OutOfMemoryError} — deletes the partial output rather
     * than leaving a zero-byte / truncated artifact for the harness to promote.</p>
     */
    public static void writeParquet(Path canonicalArrow, Path output, BufferAllocator allocator)
            throws IOException {
        // Resolved before touching the output, so a bad knob leaves nothing behind.
        writeParquet(canonicalArrow, output, allocator,
                writeOptions(System.getenv(Knobs.MAX_ROWS), System.getenv(Knobs.TARGET_ENCODED_BYTES),
                        ParquetKnobs.fromEnv()));
    }

    /** {@link #writeParquet(Path, Path, BufferAllocator)} with resolved options. */
    static void writeParquet(Path canonicalArrow, Path output, BufferAllocator allocator, WriteOptions options)
            throws IOException {
        Files.deleteIfExists(output); // parquet-arrow-java's writer is create-new
        boolean written = false;
        try (SeekableByteChannel channel = Files.newByteChannel(canonicalArrow, StandardOpenOption.READ);
                ArrowFileReader input =
                        new ArrowFileReader(channel, allocator, CommonsCompressionFactory.INSTANCE)) {
            Schema schema = input.getVectorSchemaRoot().getSchema();
            try (ParquetArrowWriter writer = ParquetArrow.writer(schema).options(options).build(output)) {
                writer.writeAll(input);
                writer.finish();
            }
            written = true;
        } finally {
            if (!written) {
                try {
                    Files.deleteIfExists(output);
                } catch (IOException cleanup) {
                    System.err.println("[parquet@java] could not remove partial output " + output + ": " + cleanup);
                }
            }
        }
    }

    /**
     * The top-level columns {@code parquet}'s footer declares with the VARIANT logical type, as
     * parquet-java reads it.
     */
    public static Set<String> variantColumns(Path parquet) throws IOException {
        ParquetReadOptions options = ParquetReadOptions.builder(new PlainParquetConfiguration())
                .withCodecFactory(new CoreCompressionCodecFactory()).build();
        Set<String> columns = new HashSet<>();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(parquet), options)) {
            for (Type field : reader.getFileMetaData().getSchema().getFields()) {
                if (field.getLogicalTypeAnnotation() instanceof LogicalTypeAnnotation.VariantLogicalTypeAnnotation) {
                    columns.add(field.getName());
                }
            }
        }
        return columns;
    }

    /** A Parquet file's batches, read back through parquet-arrow-java for the shared comparator. */
    public static BatchSource openParquet(Path parquet, BufferAllocator allocator) throws IOException {
        // ParquetArrowReader extends ArrowReader, which is a DictionaryProvider; BatchSource
        // passes it to the materializer so dictionary columns record VALUES, not indices.
        return BatchSource.of(ParquetArrow.reader(allocator).build(parquet));
    }
}
