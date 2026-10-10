// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
// The same binary serves the writer and reader contracts; --canonical selects read mode.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/util"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

var countGrammar = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func number(name string, fallback int64) (int64, error) {
	raw, present := os.LookupEnv(name)
	if !present {
		return fallback, nil
	}
	raw = strings.Trim(raw, " \t\n\r\v\f")
	limit := int64(1<<31 - 1)
	if strings.HasPrefix(name, "RAINCLOUD_ROW_GROUP_TARGET_") {
		limit = 1 << 62
	}
	if raw == "" {
		return limit, nil
	}
	if !countGrammar.MatchString(raw) {
		return 0, fmt.Errorf("%s=%q is not a number", name, raw)
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil || math.IsInf(n, 0) || math.IsNaN(n) || (n > 0 && n < 1) {
		return 0, fmt.Errorf("%s=%q must be finite and at least 1, or 0 for no limit", name, raw)
	}
	if n == 0 || n >= float64(limit) {
		return limit, nil
	}
	return int64(n), nil
}
func switchValue(name string, fallback bool) (bool, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	switch raw {
	case "":
		return fallback, nil
	case "0", "false", "off", "no":
		return false, nil
	case "1", "true", "on", "yes":
		return true, nil
	default:
		return false, fmt.Errorf("%s=%q must be a switch", name, raw)
	}
}
func refused(name string) error {
	return fmt.Errorf("parquet@go cannot honour %s=%s", name, os.Getenv(name))
}

func properties(sc *arrow.Schema, rowCap int64) (*parquet.WriterProperties, error) {
	stats, err := switchValue("RAINCLOUD_PARQUET_STATISTICS", true)
	if err != nil {
		return nil, err
	}
	index, err := switchValue("RAINCLOUD_PARQUET_PAGE_INDEX", stats)
	if err != nil {
		return nil, err
	}
	checksums, err := switchValue("RAINCLOUD_PARQUET_PAGE_CHECKSUMS", false)
	if err != nil {
		return nil, err
	}
	if checksums {
		return nil, refused("RAINCLOUD_PARQUET_PAGE_CHECKSUMS")
	}
	if strings.TrimSpace(os.Getenv("RAINCLOUD_PARQUET_PAGE_ROWS")) != "" {
		return nil, refused("RAINCLOUD_PARQUET_PAGE_ROWS")
	}
	dict, err := switchValue("RAINCLOUD_PARQUET_DICTIONARY", true)
	if err != nil {
		return nil, err
	}
	codec := strings.ToLower(strings.TrimSpace(os.Getenv("RAINCLOUD_PARQUET_COMPRESSION")))
	if codec == "" {
		codec = "zstd"
	}
	codecs := map[string]compress.Compression{"zstd": compress.Codecs.Zstd, "gzip": compress.Codecs.Gzip, "snappy": compress.Codecs.Snappy, "brotli": compress.Codecs.Brotli, "lz4": compress.Codecs.Lz4Raw, "none": compress.Codecs.Uncompressed}
	compression, ok := codecs[codec]
	if !ok {
		return nil, refused("RAINCLOUD_PARQUET_COMPRESSION")
	}
	opts := []parquet.WriterProperty{parquet.WithCompression(compression), parquet.WithStats(stats), parquet.WithPageIndexEnabled(index), parquet.WithDictionaryDefault(dict), parquet.WithMaxRowGroupLength(rowCap)}
	if raw := strings.TrimSpace(os.Getenv("RAINCLOUD_PARQUET_COMPRESSION_LEVEL")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, err
		}
		valid := (codec == "zstd" && n >= 1 && n <= 22) || (codec == "gzip" && n >= 0 && n <= 9) || (codec == "brotli" && n >= 0 && n <= 11)
		if !valid {
			return nil, refused("RAINCLOUD_PARQUET_COMPRESSION_LEVEL")
		}
		opts = append(opts, parquet.WithCompressionLevel(n))
	}
	for name, apply := range map[string]func(int64) parquet.WriterProperty{"RAINCLOUD_PARQUET_PAGE_BYTES": parquet.WithDataPageSize, "RAINCLOUD_PARQUET_DICTIONARY_PAGE_BYTES": parquet.WithDictionaryPageSizeLimit} {
		if strings.TrimSpace(os.Getenv(name)) != "" {
			n, err := number(name, 0)
			if err != nil {
				return nil, err
			}
			opts = append(opts, apply(n))
		}
	}
	statCols, err := number("RAINCLOUD_PARQUET_STATISTICS_COLUMNS", 1<<31-1)
	if err != nil {
		return nil, err
	}
	indexCols, err := number("RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS", 1<<31-1)
	if err != nil {
		return nil, err
	}
	if !stats && (index || os.Getenv("RAINCLOUD_PARQUET_STATISTICS_COLUMNS") != "" || os.Getenv("RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS") != "") {
		return nil, fmt.Errorf("page statistics requested with statistics off")
	}
	if !index && os.Getenv("RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS") != "" {
		return nil, fmt.Errorf("page index columns requested with page index off")
	}
	schema, err := pqarrow.ToParquet(sc, parquet.NewWriterProperties(opts...), pqarrow.DefaultWriterProps())
	if err != nil {
		return nil, err
	}
	for i := 0; i < schema.NumColumns(); i++ {
		path := schema.Column(i).Path()
		opts = append(opts, parquet.WithStatsFor(path, stats && int64(i) < statCols), parquet.WithPageIndexEnabledFor(path, index && int64(i) < statCols && int64(i) < indexCols))
	}
	return parquet.NewWriterProperties(opts...), nil
}

func write(input, output string) error {
	source, err := os.Open(input)
	if err != nil {
		return err
	}
	defer source.Close()
	reader, err := ipc.NewFileReader(source)
	if err != nil {
		return err
	}
	defer reader.Close()
	cap, err := number("RAINCLOUD_ROW_GROUP_MAX_ROWS", 10_000_000)
	if err != nil {
		return err
	}
	encoded, err := number("RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES", 128<<20)
	if err != nil {
		return err
	}
	decoded, err := number("RAINCLOUD_ROW_GROUP_TARGET_BYTES", 512<<20)
	if err != nil {
		return err
	}
	props, err := properties(reader.Schema(), cap)
	if err != nil {
		return err
	}
	dest, err := os.Create(output)
	if err != nil {
		return err
	}
	defer dest.Close()
	writer, err := pqarrow.NewFileWriter(reader.Schema(), dest, props, pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
	if err != nil {
		return err
	}
	defer writer.Close()
	var rows, bytes int64
	for i := 0; i < reader.NumRecords(); i++ {
		batch, err := reader.RecordBatchAt(i)
		if err != nil {
			return err
		}
		err = func() error {
			defer batch.Release()
			for offset := int64(0); offset < batch.NumRows(); {
				if rows >= cap || bytes >= decoded || writer.RowGroupTotalBytesWritten() >= encoded {
					if err := writer.NewBufferedRowGroupChecked(); err != nil {
						return err
					}
					rows, bytes = 0, 0
				}
				// Check byte bounds every small slice, rather than retaining a whole canonical batch in the encoder.
				n := min(cap-rows, batch.NumRows()-offset, 1024)
				part := batch.NewSlice(offset, offset+n)
				err := writer.WriteBuffered(part)
				bytes += util.TotalRecordSize(part) * n / max(batch.NumRows(), 1)
				part.Release()
				if err != nil {
					return err
				}
				rows += n
				offset += n
			}
			return nil
		}()
		if err != nil {
			return err
		}
	}
	return writer.Close()
}

func verify(artifact, canonical string) error {
	input, err := os.Open(artifact)
	if err != nil {
		return err
	}
	defer input.Close()
	pq, err := file.NewParquetReader(input)
	if err != nil {
		return err
	}
	defer pq.Close()
	batchRows := int64(1024)
	for i := 0; i < pq.NumRowGroups(); i++ {
		group := pq.MetaData().RowGroup(i)
		if group.NumRows() > 0 {
			batchRows = min(batchRows, max(1, (16<<20)*group.NumRows()/max(1, group.TotalByteSize())))
		}
	}
	reader, err := pqarrow.NewFileReader(pq, pqarrow.ArrowReadProperties{BatchSize: batchRows}, memory.DefaultAllocator)
	if err != nil {
		return err
	}
	records, err := reader.GetRecordReader(context.Background(), nil, nil)
	if err != nil {
		return err
	}
	defer records.Release()
	source, err := os.Open(canonical)
	if err != nil {
		return err
	}
	defer source.Close()
	expected, err := ipc.NewFileReader(source)
	if err != nil {
		return err
	}
	defer expected.Close()
	return compareRecords(records, expected)
}

func run(input, output, canonical string) (err error) {
	// Upstream reports some unsupported types with a panic. Preserve the sidecar report contract.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("Apache Arrow Go: %v", r)
		}
	}()
	if canonical != "" {
		return verify(input, canonical)
	}
	if err := write(input, output); err != nil {
		return err
	}
	return verify(output, input)
}
func main() {
	input := flag.String("input", "", "artifact or canonical IPC file")
	output := flag.String("output", "", "Parquet output")
	canonical := flag.String("canonical", "", "canonical to compare (reader mode)")
	report := flag.String("report", "", "JSON verdict path")
	flag.Parse()
	if *input == "" || *report == "" || (*canonical == "") == (*output == "") {
		flag.Usage()
		os.Exit(2)
	}
	err := run(*input, *output, *canonical)
	note := "Apache Arrow Go pqarrow: round-trips to canonical"
	if err != nil {
		note = err.Error()
	}
	var verdict any
	if *canonical != "" {
		status := "pass"
		if err != nil {
			status = "fail"
		}
		verdict = map[string]any{"status": status, "note": note}
	} else {
		// Measure both the physical logical annotation and the native Arrow extension on read.
		verdict = map[string]any{"roundtrip": err == nil, "variant_faithful": err == nil && variantFaithful(*input, *output), "note": note}
	}
	data, err := json.Marshal(verdict)
	if err == nil {
		err = os.WriteFile(*report, data, 0600)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// A metadata marker alone is not a Parquet VARIANT logical annotation.
func variantFaithful(canonical, artifact string) bool {
	source, err := os.Open(canonical)
	if err != nil {
		return false
	}
	defer source.Close()
	original, err := ipc.NewFileReader(source)
	if err != nil {
		return false
	}
	defer original.Close()
	input, err := os.Open(artifact)
	if err != nil {
		return false
	}
	defer input.Close()
	parquet, err := file.NewParquetReader(input)
	if err != nil {
		return false
	}
	defer parquet.Close()
	reader, err := pqarrow.NewFileReader(parquet, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return false
	}
	restored, err := reader.Schema()
	if err != nil {
		return false
	}
	isExtension := func(t arrow.DataType) bool {
		ext, ok := t.(arrow.ExtensionType)
		return ok && ext.ExtensionName() == "arrow.parquet.variant"
	}
	root := parquet.MetaData().Schema.Root()
	for _, f := range original.Schema().Fields() {
		name, _ := f.Metadata.GetValue("ARROW:extension:name")
		_, marked := f.Metadata.GetValue("__variant_type")
		if !marked && name != "arrow.parquet.variant" && !isExtension(f.Type) {
			continue
		}
		idx := root.FieldIndexByName(f.Name)
		fields := restored.FieldIndices(f.Name)
		if idx < 0 || root.Field(idx).LogicalType().String() != "Variant" || len(fields) != 1 || !isExtension(restored.Field(fields[0]).Type) {
			return false
		}
	}
	return true
}
