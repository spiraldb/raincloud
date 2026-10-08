// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0

//! Shared machinery for raincloud's Rust sidecar binaries.
//!
//! The `[[bin]]` targets implement raincloud's fixed sidecar CLI contracts
//! (see `raincloud/pipeline/export/sidecar.py` for WRITE and
//! `raincloud/pipeline/export/readers.py` for READ):
//!
//! * `parquet-write` / `vortex-write` / `orc-write` / `avro-write` — read the
//!   canonical Arrow IPC file, write the target format, then SELF-VERIFY
//!   (re-read, compare) and emit `{"roundtrip", "variant_faithful", "note"}`.
//! * `parquet-read` / `vortex-read` / `orc-read` / `avro-read` — read an
//!   artifact, compare to the canonical with LOGICAL equality, emit
//!   `{"status", "note", "detail"}`.
//!
//! The `nimble-ffi` member exposes the same contract to the nimble@cpp
//! binaries (`sidecars/nimble`), which are C++ and link it.
//!
//! Every lane streams: both sides are read batch by batch and compared window
//! by window, so memory does not grow with the table. Any failure, a panic
//! included, still ends in a report (see [`run_writer`] and [`run_reader`]).
//! Exit 2 is a usage error (clap); exit 1 means the report itself could not be
//! written.
//!
//! The canonical (`<slug>.arrow.zstd`) is a standard **Apache Arrow IPC file**
//! (`ARROW1` magic); the `zstd` is IPC-internal record-batch-body compression, so
//! it is read with `arrow_ipc::reader::FileReader` and the `arrow-ipc/zstd`
//! feature — never outer-zstd-decompressed.
//!
//! Comparison is LOGICAL, matching the Python in-process readers'
//! `_roundtrip_verdict`: row count + column names, then each column cast losslessly
//! to the canonical column's type (so `string`/`string_view`/`large_string` +
//! dictionary compare equal) and compared at the `ArrayData` level (which ignores
//! field metadata — so a dropped VARIANT annotation does not fail the round-trip;
//! a VARIANT column survives as its shredded `struct<metadata, value>`).

use std::ffi::OsStr;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_avro::compression::CompressionCodec;
use arrow_avro::reader::ReaderBuilder as AvroReaderBuilder;
use arrow_avro::writer::format::AvroOcfFormat;
use arrow_avro::writer::WriterBuilder as AvroWriterBuilder;
use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::properties::WriterProperties;

use vortex::array::stream::ArrayStreamAdapter;
use vortex::arrow::ArrowSessionExt;
use vortex::error::VortexError;
use vortex::file::{OpenOptionsSessionExt, WriteOptionsSessionExt};
use vortex::io::runtime::tokio::TokioRuntime;
use vortex::io::runtime::BlockingRuntime;
use vortex::io::session::RuntimeSessionExt;
use vortex::session::VortexSession;
use vortex::VortexSessionDefault;

/// The write settings of the other formats (`spec.FORMAT_SETTINGS`).
const ORC_COMPRESSION: &str = "RAINCLOUD_ORC_COMPRESSION";
const ORC_COMPRESSION_STRATEGY: &str = "RAINCLOUD_ORC_COMPRESSION_STRATEGY";
const ORC_STRIPE_BYTES: &str = "RAINCLOUD_ORC_STRIPE_BYTES";
const ORC_COMPRESSION_BLOCK_BYTES: &str = "RAINCLOUD_ORC_COMPRESSION_BLOCK_BYTES";
const AVRO_COMPRESSION: &str = "RAINCLOUD_AVRO_COMPRESSION";
const AVRO_COMPRESSION_LEVEL: &str = "RAINCLOUD_AVRO_COMPRESSION_LEVEL";
const AVRO_BLOCK_BYTES: &str = "RAINCLOUD_AVRO_BLOCK_BYTES";
const VORTEX_COMPACT: &str = "RAINCLOUD_VORTEX_COMPACT";
const VORTEX_ROW_BLOCK_ROWS: &str = "RAINCLOUD_VORTEX_ROW_BLOCK_ROWS";
const VORTEX_DATA_BLOCK_BYTES: &str = "RAINCLOUD_VORTEX_DATA_BLOCK_BYTES";

/// raincloud's top-level VARIANT marker (see `discovery._is_variant_field`).
const VARIANT_MARKER: &str = "__variant_type";

/// Arrow's field-metadata key for an extension name.
const EXTENSION_NAME: &str = "ARROW:extension:name";

/// The Arrow canonical extension of a Parquet VARIANT column.
const VARIANT_EXTENSION: &str = "arrow.parquet.variant";

// ---------------------------------------------------------------------------
// Canonical + format IO
// ---------------------------------------------------------------------------

/// Open the canonical Arrow IPC file as a batch stream.
///
/// The `zstd` inside the IPC stream is decoded transparently by `FileReader`
/// (the `arrow-ipc/zstd` feature); the file is NOT outer-zstd-decompressed.
pub fn open_canonical(path: &Path) -> Result<(SchemaRef, FileReader<File>)> {
    let file = File::open(path).with_context(|| format!("open canonical {}", path.display()))?;
    let reader = FileReader::try_new(file, None)
        .with_context(|| format!("read canonical Arrow IPC {}", path.display()))?;
    Ok((reader.schema(), reader))
}

/// The canonical's batches as `anyhow` results, for [`logical_eq_stream`].
pub fn canonical_batches(reader: FileReader<File>) -> impl Iterator<Item = Result<RecordBatch>> {
    reader.map(|b| b.context("read canonical record batch"))
}

// Parquet has no seconds unit for timestamps or times. arrow-rs otherwise
// writes a seconds timestamp as bare INT64 and a seconds time as bare INT32,
// relying on its private Arrow schema to recover the meaning. Promote both to
// milliseconds before encoding so independent readers can identify them from
// the standard Parquet schema, including nested ones.
fn parquet_portable_field(field: &Field) -> Field {
    use arrow_schema::TimeUnit;
    use DataType::*;
    let dtype = match field.data_type() {
        Timestamp(TimeUnit::Second, tz) => Timestamp(TimeUnit::Millisecond, tz.clone()),
        Time32(TimeUnit::Second) => Time32(TimeUnit::Millisecond),
        Struct(fields) => Struct(fields.iter().map(|f| parquet_portable_field(f)).collect()),
        List(child) => List(Arc::new(parquet_portable_field(child))),
        LargeList(child) => LargeList(Arc::new(parquet_portable_field(child))),
        FixedSizeList(child, size) => FixedSizeList(Arc::new(parquet_portable_field(child)), *size),
        Map(child, sorted) => Map(Arc::new(parquet_portable_field(child)), *sorted),
        Dictionary(index, value) => Dictionary(
            index.clone(),
            Box::new(
                parquet_portable_field(&Field::new("value", value.as_ref().clone(), true))
                    .data_type()
                    .clone(),
            ),
        ),
        other => other.clone(),
    };
    field.clone().with_data_type(dtype)
}

/// ASCII whitespace as Python's `string.whitespace` has it: unlike
/// `u8::is_ascii_whitespace`, it includes the vertical tab.
fn is_knob_space(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0b' | '\x0c')
}

/// `^[0-9]+(\.[0-9]+)?([eE][+-]?[0-9]+)?$`: ASCII digits only, no sign, no
/// digit separators, no `inf`/`nan`.
fn is_plain_number(value: &str) -> bool {
    let bytes = value.as_bytes();
    let mut i = 0;
    let digits = |i: &mut usize| {
        let start = *i;
        while bytes.get(*i).is_some_and(u8::is_ascii_digit) {
            *i += 1;
        }
        *i > start
    };
    if !digits(&mut i) {
        return false;
    }
    if bytes.get(i) == Some(&b'.') {
        i += 1;
        if !digits(&mut i) {
            return false;
        }
    }
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(bytes.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        if !digits(&mut i) {
            return false;
        }
    }
    i == bytes.len()
}

/// One whole-number knob, with the grammar every lane shares
/// (`raincloud/pipeline/spec.py::_env_count`, the Java lane, and the cases in
/// `sidecars/knob_cases.json`): unset -> `default`; after trimming ASCII
/// whitespace, empty or 0 -> `disabled` (no cap); otherwise a plain
/// non-negative number as [`is_plain_number`] reads it, truncated toward zero
/// (`1e6` is 1,000,000). A value that is not UTF-8, is not such a number, is
/// not finite, or truncates to 0 is an error rather than quietly the default:
/// these knobs decide row-group boundaries, and so the artifact's sha256.
fn knob(var: &str, raw: Option<&OsStr>, default: usize, disabled: usize) -> Result<usize> {
    let Some(raw) = raw else {
        return Ok(default);
    };
    let Some(raw) = raw.to_str() else {
        bail!("{var}={raw:?} is not valid UTF-8; give a plain number (bytes or rows), or 0 to disable");
    };
    let value = raw.trim_matches(is_knob_space);
    if value.is_empty() {
        return Ok(disabled);
    }
    if !is_plain_number(value) {
        bail!("{var}='{raw}' is not a number; give a plain value (bytes or rows) such as 1e6, or 0 to disable");
    }
    let number: f64 = value
        .parse()
        .with_context(|| format!("{var}='{raw}' is not a number"))?;
    if !number.is_finite() {
        bail!("{var}='{raw}' must be a finite number >= 0 (0 disables it)");
    }
    if number == 0.0 {
        return Ok(disabled);
    }
    // `as` saturates, so a huge value is simply no cap.
    let truncated = number as usize;
    if truncated == 0 {
        bail!("{var}='{raw}' rounds down to 0; give at least 1, or 0 to disable");
    }
    Ok(truncated)
}

/// The row-group limits one Parquet write applies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowGroupLimits {
    /// Target size of one row group. See `spec.row_group_target_encoded_bytes`
    /// for the quantity measured, and [`write_parquet`] for how this lane
    /// measures it.
    pub target_encoded_bytes: usize,
    /// Backstop cap on rows per row group. Bytes decide the group; this catches
    /// the shape bytes cannot — a single narrow integer column would otherwise
    /// reach ~134M rows before a 128 MiB target fired.
    pub max_rows: usize,
}

impl RowGroupLimits {
    /// `RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES` (default 128 MiB) and
    /// `RAINCLOUD_ROW_GROUP_MAX_ROWS` (default 10,000,000). Disabled, they are
    /// the Python lane's figures: 2**62 bytes and 2**31-1 rows.
    ///
    /// The sidecar never sees the recipe: `SidecarExporter` passes a recipe's
    /// `write.row_group_size_rows` as `RAINCLOUD_ROW_GROUP_MAX_ROWS` in this
    /// process's environment, so the recipe cap wins here as it does in
    /// parquet@py.
    pub fn from_env() -> Result<Self> {
        let var = |name| std::env::var_os(name);
        const TARGET: &str = "RAINCLOUD_ROW_GROUP_TARGET_ENCODED_BYTES";
        const MAX_ROWS: &str = "RAINCLOUD_ROW_GROUP_MAX_ROWS";
        Ok(Self {
            target_encoded_bytes: knob(
                TARGET,
                var(TARGET).as_deref(),
                128 << 20,
                // 2**62 does not fit a 32-bit usize, whose MAX is no cap already.
                usize::try_from(1u64 << 62).unwrap_or(usize::MAX),
            )?,
            max_rows: knob(
                MAX_ROWS,
                var(MAX_ROWS).as_deref(),
                10_000_000,
                (1 << 31) - 1,
            )?,
        })
    }
}

/// An on/off knob as every lane reads it (`spec._env_switch`): unset or empty
/// -> `None`; otherwise 1/true/yes/on or 0/false/no/off, in any case.
fn switch(var: &str, raw: Option<&OsStr>) -> Result<Option<bool>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let Some(text) = raw.to_str() else {
        bail!("{var}={raw:?} is not valid UTF-8; give 1 or 0");
    };
    match text
        .trim_matches(is_knob_space)
        .to_ascii_lowercase()
        .as_str()
    {
        "" => Ok(None),
        "1" | "true" | "yes" | "on" => Ok(Some(true)),
        "0" | "false" | "no" | "off" => Ok(Some(false)),
        _ => bail!("{var}='{text}' is not a switch; give 1 or 0 (true/false, yes/no, on/off)"),
    }
}

/// A setting that names one of `choices` (`spec._env_choice`): unset or empty
/// -> `None`; otherwise one of them, in any case.
fn choice(var: &str, choices: &[&'static str]) -> Result<Option<&'static str>> {
    let Some(raw) = std::env::var_os(var) else {
        return Ok(None);
    };
    let text = raw
        .to_str()
        .map(|s| s.trim_matches(is_knob_space).to_ascii_lowercase());
    match text.as_deref() {
        Some("") => Ok(None),
        Some(value) => match choices.iter().find(|c| **c == value) {
            Some(found) => Ok(Some(found)),
            None => bail!("{var}={raw:?} is not one of {}", choices.join(", ")),
        },
        None => bail!("{var}={raw:?} is not valid UTF-8"),
    }
}

/// A size or count setting from the environment, as [`ParquetOptions`] reads
/// its own: unset -> `None` (the library's default); empty or 0 -> no limit.
fn setting_count(var: &str) -> Result<Option<usize>> {
    std::env::var_os(var)
        .map(|raw| knob(var, Some(&raw), 0, (1 << 31) - 1))
        .transpose()
}

/// Refuse a set setting this lane's library has no way to honour.
fn refuse_set<T: std::fmt::Display>(
    lane: &str,
    var: &str,
    value: Option<T>,
    why: &str,
) -> Result<()> {
    match value {
        Some(value) => bail!("{lane} cannot honour {var}={value}: {why}"),
        None => Ok(()),
    }
}

/// The Parquet write options every Parquet lane is given the same way
/// (`raincloud/pipeline/spec.py::ParquetOptions`, which documents each). The
/// sidecar never sees the recipe: `SidecarExporter` passes its
/// `write.compression` and `write.statistics` as `RAINCLOUD_PARQUET_COMPRESSION`
/// and `RAINCLOUD_PARQUET_STATISTICS`, and each install setting only when it is
/// set. Unset leaves arrow-rs's own default: a page index, 1 MiB pages, 20,000
/// rows a page, dictionaries on, no page checksums.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParquetOptions {
    /// The codec, at `RAINCLOUD_PARQUET_COMPRESSION_LEVEL` when that is set.
    pub compression: Compression,
    pub statistics: bool,
    /// Statistics only for the first N leaf columns.
    pub statistics_columns: Option<usize>,
    /// A ColumnIndex and OffsetIndex for every column chunk, or neither.
    pub page_index: Option<bool>,
    /// Page statistics only for the first N leaf columns; every column keeps
    /// its chunk statistics.
    pub page_index_columns: Option<usize>,
    /// Data page size target, in bytes.
    pub page_bytes: Option<usize>,
    /// Data page row limit.
    pub page_rows: Option<usize>,
    pub dictionary: Option<bool>,
    /// Dictionary page size limit, in bytes.
    pub dictionary_page_bytes: Option<usize>,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            compression: Compression::ZSTD(ZstdLevel::default()),
            statistics: true,
            statistics_columns: None,
            page_index: None,
            page_index_columns: None,
            page_bytes: None,
            page_rows: None,
            dictionary: None,
            dictionary_page_bytes: None,
        }
    }
}

impl ParquetOptions {
    pub fn from_env() -> Result<Self> {
        Self::from_vars(|name| std::env::var_os(name))
    }

    /// [`Self::from_env`] over `var`, so a test need not touch the process
    /// environment.
    pub fn from_vars(var: impl Fn(&str) -> Option<std::ffi::OsString>) -> Result<Self> {
        const COMPRESSION: &str = "RAINCLOUD_PARQUET_COMPRESSION";
        const LEVEL: &str = "RAINCLOUD_PARQUET_COMPRESSION_LEVEL";
        const STATISTICS: &str = "RAINCLOUD_PARQUET_STATISTICS";
        const STATISTICS_COLUMNS: &str = "RAINCLOUD_PARQUET_STATISTICS_COLUMNS";
        const PAGE_INDEX: &str = "RAINCLOUD_PARQUET_PAGE_INDEX";
        const PAGE_INDEX_COLUMNS: &str = "RAINCLOUD_PARQUET_PAGE_INDEX_COLUMNS";
        const PAGE_BYTES: &str = "RAINCLOUD_PARQUET_PAGE_BYTES";
        const PAGE_ROWS: &str = "RAINCLOUD_PARQUET_PAGE_ROWS";
        const DICTIONARY: &str = "RAINCLOUD_PARQUET_DICTIONARY";
        const DICTIONARY_PAGE_BYTES: &str = "RAINCLOUD_PARQUET_DICTIONARY_PAGE_BYTES";
        const PAGE_CHECKSUMS: &str = "RAINCLOUD_PARQUET_PAGE_CHECKSUMS";
        let flag = |name: &str| switch(name, var(name).as_deref());
        // Unset is arrow-rs's default; empty or 0 is no limit, as in parquet@py.
        let count = |name: &str| -> Result<Option<usize>> {
            var(name)
                .map(|raw| knob(name, Some(&raw), 0, (1 << 31) - 1))
                .transpose()
        };
        let level = match var(LEVEL) {
            None => None,
            Some(raw) => match raw.to_str().map(|s| s.trim_matches(is_knob_space)) {
                Some("") => None,
                Some(text) if text.bytes().all(|b| b.is_ascii_digit()) => Some(
                    text.parse::<u32>()
                        .with_context(|| format!("{LEVEL}='{text}' is too large"))?,
                ),
                _ => bail!(
                    "{LEVEL}={raw:?} is not a compression level; give a whole number such as 3"
                ),
            },
        };
        let codec = match var(COMPRESSION) {
            None => "zstd".to_string(),
            Some(raw) => raw
                .to_str()
                .map(|s| s.trim_matches(is_knob_space).to_string())
                .unwrap_or_default(),
        };
        let leveled = |name: &str, result: parquet::errors::Result<Compression>| {
            result.map_err(|e| anyhow::anyhow!("{LEVEL}={} for {name}: {e}", level.unwrap()))
        };
        use parquet::basic::{BrotliLevel, GzipLevel};
        let compression = match (codec.as_str(), level) {
            ("zstd", None) => Compression::ZSTD(ZstdLevel::default()),
            ("zstd", Some(l)) => leveled(
                "zstd",
                i32::try_from(l)
                    .map_err(|_| parquet::errors::ParquetError::General("too large".into()))
                    .and_then(ZstdLevel::try_new)
                    .map(Compression::ZSTD),
            )?,
            ("gzip", None) => Compression::GZIP(Default::default()),
            ("gzip", Some(l)) => leveled("gzip", GzipLevel::try_new(l).map(Compression::GZIP))?,
            ("brotli", None) => Compression::BROTLI(Default::default()),
            ("brotli", Some(l)) => {
                leveled("brotli", BrotliLevel::try_new(l).map(Compression::BROTLI))?
            }
            ("snappy" | "lz4" | "none", Some(l)) => {
                bail!("{LEVEL}={l}: {codec} takes no compression level")
            }
            ("snappy", None) => Compression::SNAPPY,
            // pyarrow's "lz4" and Hardwood's LZ4_RAW: the framing the format
            // recommends, not the deprecated Hadoop LZ4.
            ("lz4", None) => Compression::LZ4_RAW,
            ("none", None) => Compression::UNCOMPRESSED,
            _ => {
                bail!("{COMPRESSION}='{codec}' is not one of zstd, snappy, gzip, lz4, brotli, none")
            }
        };
        let statistics = flag(STATISTICS)?.unwrap_or(true);
        let options = Self {
            compression,
            statistics,
            statistics_columns: count(STATISTICS_COLUMNS)?,
            page_index: flag(PAGE_INDEX)?,
            page_index_columns: count(PAGE_INDEX_COLUMNS)?,
            page_bytes: count(PAGE_BYTES)?,
            page_rows: count(PAGE_ROWS)?,
            dictionary: flag(DICTIONARY)?,
            dictionary_page_bytes: count(DICTIONARY_PAGE_BYTES)?,
        };
        if !statistics {
            for (name, asked) in [
                (PAGE_INDEX, options.page_index == Some(true)),
                (STATISTICS_COLUMNS, options.statistics_columns.is_some()),
                (PAGE_INDEX_COLUMNS, options.page_index_columns.is_some()),
            ] {
                if asked {
                    bail!("{name} asks for statistics, but {STATISTICS} is 0");
                }
            }
        }
        if options.page_index == Some(false) && options.page_index_columns.is_some() {
            bail!("{PAGE_INDEX_COLUMNS} asks for a page index, but {PAGE_INDEX} is 0");
        }
        if flag(PAGE_CHECKSUMS)? == Some(true) {
            bail!("parquet@rs cannot honour {PAGE_CHECKSUMS}=1: arrow-rs writes no page checksums");
        }
        Ok(options)
    }

    /// `builder` with these options for a file of `columns`, except
    /// compression, which the caller sets.
    fn apply(
        &self,
        mut builder: parquet::file::properties::WriterPropertiesBuilder,
        columns: &parquet::schema::types::SchemaDescriptor,
    ) -> parquet::file::properties::WriterPropertiesBuilder {
        use parquet::file::properties::EnabledStatistics;
        if !self.statistics {
            builder = builder.set_statistics_enabled(EnabledStatistics::None);
        }
        match self.page_index {
            Some(true) => builder = builder.set_statistics_enabled(EnabledStatistics::Page),
            // Neither index, as pyarrow writes without one: Chunk statistics alone
            // still write an OffsetIndex.
            Some(false) => {
                if self.statistics {
                    builder = builder.set_statistics_enabled(EnabledStatistics::Chunk);
                }
                builder = builder.set_offset_index_disabled(true);
            }
            None => {}
        }
        if self.statistics {
            for (i, column) in columns.columns().iter().enumerate() {
                let path = column.path().clone();
                let level = if self.statistics_columns.is_some_and(|n| i >= n) {
                    EnabledStatistics::None
                } else if let Some(n) = self.page_index_columns {
                    if i < n {
                        EnabledStatistics::Page
                    } else {
                        EnabledStatistics::Chunk
                    }
                } else {
                    continue;
                };
                builder = builder.set_column_statistics_enabled(path, level);
            }
        }
        if let Some(bytes) = self.page_bytes {
            builder = builder.set_data_page_size_limit(bytes);
        }
        if let Some(rows) = self.page_rows {
            builder = builder.set_data_page_row_count_limit(rows);
        }
        if let Some(enabled) = self.dictionary {
            builder = builder.set_dictionary_enabled(enabled);
        }
        if let Some(bytes) = self.dictionary_page_bytes {
            builder = builder.set_dictionary_page_size_limit(bytes);
        }
        builder
    }
}

/// Rows handed to arrow-rs per `write` call.
///
/// arrow-rs applies `max_row_group_bytes` by measuring the rows it has already
/// buffered and slicing the incoming batch at the row that fits. A first write
/// has nothing buffered to measure, so one batch holding the whole table only
/// ever met the row cap: TPC-H SF100 customer came out as groups of 10M and 5M
/// rows, ~1.5 GiB encoded each.
const WRITE_CHUNK_ROWS: usize = 65_536;

/// Decoded bytes one Parquet read batch is planned to hold (see
/// [`read_plan`]). arrow-rs decodes a BYTE_ARRAY column into an i32-offset
/// `Binary`/`Utf8` array, so a batch whose column holds 2 GiB fails ("index
/// overflow decoding byte array"): a fixed 65,536 rows put all 18,622 rows and
/// 2.46 GB of audio of peoples-speech-clean-validation in one batch. The plan
/// sees only each row group's average row, so the budget sits 8x under the
/// limit to absorb rows larger than their group's average. Batch sizes need
/// not match `WRITE_CHUNK_ROWS` or the canonical's, since the comparison
/// realigns batches.
const READ_BATCH_BYTES: u64 = 256 << 20;

/// Cap on rows per Parquet read batch, for what the byte estimate cannot see:
/// a delta- or run-length-encoded column decodes to far more than it encodes.
const READ_BATCH_ROWS: usize = 65_536;

/// Apply the portable-Parquet normalization to each batch and hand it on in
/// slices of at most `WRITE_CHUNK_ROWS`.
fn for_each_chunk<I>(
    schema: &SchemaRef,
    batches: I,
    mut write: impl FnMut(RecordBatch) -> Result<()>,
) -> Result<()>
where
    I: IntoIterator<Item = Result<RecordBatch>>,
{
    // `safe: false` makes Arrow report overflow instead of replacing values
    // with null.
    let options = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    for batch in batches {
        let batch = batch?;
        let columns = batch
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(array, field)| {
                arrow_cast::cast_with_options(array.as_ref(), field.data_type(), &options)
                    .with_context(|| {
                        format!("normalize Parquet timestamp/time field {}", field.name())
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(schema.clone(), columns)?;
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = WRITE_CHUNK_ROWS.min(batch.num_rows() - offset);
            write(batch.slice(offset, len))?;
            offset += len;
        }
    }
    Ok(())
}

/// Stream record batches into a Parquet file (zstd compression, arrow-rs), with
/// the default [`ParquetOptions`]; see [`write_parquet_with`].
pub fn write_parquet<F, I>(
    output: &Path,
    schema: SchemaRef,
    limits: RowGroupLimits,
    open: F,
) -> Result<()>
where
    F: Fn() -> Result<I>,
    I: IntoIterator<Item = Result<RecordBatch>>,
{
    write_parquet_with(output, schema, limits, ParquetOptions::default(), open)
}

/// Stream record batches into a Parquet file with `options` (arrow-rs).
///
/// `open` is called twice, since the batches are read twice rather than held:
/// SF100 lineitem is ~100 GB decoded. Each call must replay the same rows in
/// the same order (a re-opened canonical does), or the second pass would close
/// groups at rows the first pass planned for other data.
///
/// Row groups are sized by ENCODED bytes before compression, the quantity the
/// Python lane measures (see `row_group_target_encoded_bytes` in
/// `raincloud/pipeline/spec.py`): it does not move with the codec, and these
/// artifacts are gated on sha256. arrow-rs's `max_row_group_bytes` measures
/// the group after compression instead (`get_estimated_total_bytes` sums the
/// compressed pages already closed plus the open page's encoded size), which
/// put TPC-H customer at 377 MiB encoded per group for a 128 MiB target.
/// So the first pass encodes uncompressed into a sink, where arrow-rs's measure
/// is the encoded size, and records where each group ends; the second writes
/// the file with `options.compression` and closes groups at those rows. Both
/// passes write the same pages and statistics, so the planned encoded size is
/// the file's.
pub fn write_parquet_with<F, I>(
    output: &Path,
    schema: SchemaRef,
    limits: RowGroupLimits,
    options: ParquetOptions,
    open: F,
) -> Result<()>
where
    F: Fn() -> Result<I>,
    I: IntoIterator<Item = Result<RecordBatch>>,
{
    let schema = Arc::new(Schema::new_with_metadata(
        schema
            .fields()
            .iter()
            .map(|f| parquet_portable_field(f))
            .collect::<Vec<_>>(),
        schema.metadata().clone(),
    ));

    // Pass 1: where the groups end. The row limit must be turned OFF for the
    // byte target to mean anything: arrow-rs produces "the row group with the
    // smaller limit", and its 1Mi-row default would cap TPC-H lineitem at
    // ~63 MiB encoded, exactly as parquet-java ships its own row limit
    // effectively off so that bytes decide. The backstop stays.
    let columns = parquet::arrow::ArrowSchemaConverter::new()
        .convert(&schema)
        .context("derive the Parquet schema")?;
    let plan_props = options
        .apply(WriterProperties::builder(), &columns)
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(limits.max_rows))
        .set_max_row_group_bytes(Some(limits.target_encoded_bytes))
        .build();
    let mut planner = ArrowWriter::try_new(std::io::sink(), schema.clone(), Some(plan_props))
        .context("create arrow-rs row-group planner")?;
    for_each_chunk(&schema, open()?, |chunk| {
        planner.write(&chunk).context("plan parquet row groups")
    })?;
    let plan: Vec<usize> = planner
        .close()
        .context("close row-group planner")?
        .row_groups()
        .iter()
        .map(|g| g.num_rows() as usize)
        .collect();

    // Pass 2: the file, with both automatic limits off so only the plan closes
    // a group.
    let file =
        File::create(output).with_context(|| format!("create parquet {}", output.display()))?;
    let props = options
        .apply(WriterProperties::builder(), &columns)
        .set_compression(options.compression)
        .set_max_row_group_row_count(None)
        .set_max_row_group_bytes(None)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
        .context("create arrow-rs ParquetWriter")?;
    let mut plan = plan.into_iter();
    let mut left = plan.next().unwrap_or(usize::MAX);
    for_each_chunk(&schema, open()?, |chunk| {
        let mut offset = 0;
        while offset < chunk.num_rows() {
            let len = left.min(chunk.num_rows() - offset);
            writer
                .write(&chunk.slice(offset, len))
                .context("write parquet batch")?;
            offset += len;
            left -= len;
            if left == 0 {
                writer.flush().context("flush parquet row group")?;
                left = plan.next().unwrap_or(usize::MAX);
            }
        }
        Ok(())
    })?;
    writer.close().context("close parquet writer")?;
    Ok(())
}

/// A row group's decoded size as its metadata tells it: per column, the
/// larger of the encoded size before compression and, for BYTE_ARRAY columns
/// whose writer recorded it, the value bytes after decoding (a dictionary page
/// encodes a repeated value once, and arrow-rs materializes every copy).
fn decoded_bytes(group: &RowGroupMetaData) -> u64 {
    group
        .columns()
        .iter()
        .map(|column| {
            let encoded = column.uncompressed_size().max(0) as u64;
            let values = column.unencoded_byte_array_data_bytes().unwrap_or(0);
            encoded.max(values.max(0) as u64)
        })
        .sum()
}

/// Consecutive row groups read with one batch size.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ReadSegment {
    groups: Vec<usize>,
    batch_rows: usize,
    /// Planned decoded bytes of one batch, for error messages.
    batch_bytes: u64,
}

/// Batch sizes for reading a Parquet file within `budget` decoded bytes and
/// `max_rows` rows per batch.
///
/// arrow-rs fills a batch across row groups, so no one batch size suits a file
/// whose groups differ; each segment gets its own reader. Consecutive groups
/// that together fit one batch share one (a file of many small groups is not
/// read a sliver at a time); a group that does not is read alone, in batches
/// of as many of its average rows as fit the budget, and never fewer than one
/// row. A batch that stays inside one group is at most that group's size, so
/// only a group over 2 GiB can still overflow, and only if its rows are
/// uneven enough to beat the budget's headroom.
fn read_plan(groups: &[RowGroupMetaData], budget: u64, max_rows: usize) -> Vec<ReadSegment> {
    let mut plan = Vec::new();
    let mut packed: Option<ReadSegment> = None;
    for (index, group) in groups.iter().enumerate() {
        let rows = group.num_rows().max(0) as usize;
        if rows == 0 {
            continue;
        }
        let bytes = decoded_bytes(group);
        if bytes <= budget && rows <= max_rows {
            if let Some(open) = &mut packed {
                if open.batch_bytes + bytes <= budget && open.batch_rows + rows <= max_rows {
                    open.groups.push(index);
                    open.batch_rows += rows;
                    open.batch_bytes += bytes;
                    continue;
                }
            }
            plan.extend(packed.replace(ReadSegment {
                groups: vec![index],
                batch_rows: rows,
                batch_bytes: bytes,
            }));
            continue;
        }
        plan.extend(packed.take());
        // u128: rows * budget can exceed u64 when neither is small.
        let fit = u128::from(budget) * rows as u128 / u128::from(bytes.max(1));
        let batch_rows = (fit.min(rows as u128) as usize).clamp(1, max_rows.min(rows));
        plan.push(ReadSegment {
            groups: vec![index],
            batch_rows,
            batch_bytes: (u128::from(bytes) * batch_rows as u128 / rows as u128) as u64,
        });
    }
    plan.extend(packed);
    plan
}

/// A Parquet file's batches, read segment by segment as [`read_plan`] sizes
/// them.
struct ParquetBatches {
    file: File,
    metadata: ArrowReaderMetadata,
    segments: std::vec::IntoIter<ReadSegment>,
    current: Option<(ReadSegment, ParquetRecordBatchReader)>,
}

impl ParquetBatches {
    fn open_segment(&self, segment: &ReadSegment) -> Result<ParquetRecordBatchReader> {
        let file = self.file.try_clone().context("reopen parquet file")?;
        ParquetRecordBatchReaderBuilder::new_with_metadata(file, self.metadata.clone())
            .with_row_groups(segment.groups.clone())
            .with_batch_size(segment.batch_rows)
            .build()
            .context("build parquet reader")
    }
}

impl Iterator for ParquetBatches {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((segment, reader)) = &mut self.current {
                match reader.next() {
                    Some(Ok(batch)) => return Some(Ok(batch)),
                    Some(Err(e)) => {
                        let (first, last) = (segment.groups[0], segment.groups.last().unwrap());
                        let mut what = format!(
                            "read parquet row groups {first}..={last} in batches of {} rows \
                             (~{} decoded bytes each, planned from the row-group metadata)",
                            segment.batch_rows, segment.batch_bytes
                        );
                        if segment.batch_rows == 1 && e.to_string().contains("overflow") {
                            what.push_str(
                                "; a batch is a single row already, so one row holds more than \
                                 arrow-rs can decode into one array (i32 offsets: 2 GiB)",
                            );
                        }
                        return Some(Err(anyhow::Error::new(e).context(what)));
                    }
                    None => self.current = None,
                }
            }
            let segment = self.segments.next()?;
            match self.open_segment(&segment) {
                Ok(reader) => self.current = Some((segment, reader)),
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Open a Parquet file as a batch stream (arrow-rs), with its Arrow schema.
/// Batches are sized by decoded bytes, not rows: see [`READ_BATCH_BYTES`].
pub fn open_parquet(
    input: &Path,
) -> Result<(SchemaRef, impl Iterator<Item = Result<RecordBatch>>)> {
    open_parquet_with(input, READ_BATCH_BYTES, READ_BATCH_ROWS)
}

fn open_parquet_with(
    input: &Path,
    budget: u64,
    max_rows: usize,
) -> Result<(SchemaRef, ParquetBatches)> {
    let file = File::open(input).with_context(|| format!("open parquet {}", input.display()))?;
    let metadata = ArrowReaderMetadata::load(&file, Default::default())
        .context("open arrow-rs parquet reader")?;
    let schema = metadata.schema().clone();
    let plan = read_plan(metadata.metadata().row_groups(), budget, max_rows);
    Ok((
        schema,
        ParquetBatches {
            file,
            metadata,
            segments: plan.into_iter(),
            current: None,
        },
    ))
}

// ---------------------------------------------------------------------------
// Vortex IO (via the Vortex Rust core)
// ---------------------------------------------------------------------------

fn runtime() -> Result<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new().context("create tokio runtime")
}

/// The schema `vortex@py` hands Vortex: a top-level VARIANT column loses its
/// `arrow.parquet.variant` extension keys and raincloud's marker, so Vortex
/// stores the storage struct rather than reading the column as its native
/// VARIANT. Other metadata is kept.
fn vortex_storage_schema(schema: &Schema) -> SchemaRef {
    const EXTENSION_NAME: &str = "ARROW:extension:name";
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .map(|field| {
            if field.metadata().get(EXTENSION_NAME).map(String::as_str)
                != Some("arrow.parquet.variant")
            {
                return Arc::clone(field);
            }
            let mut metadata = field.metadata().clone();
            for key in [EXTENSION_NAME, "ARROW:extension:metadata", VARIANT_MARKER] {
                metadata.remove(key);
            }
            Arc::new(field.as_ref().clone().with_metadata(metadata))
        })
        .collect();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Stream the canonical into a `.vortex` file with Vortex's default write
/// strategy — chunking, statistics and compression — unless a Vortex write
/// setting asks otherwise (`RAINCLOUD_VORTEX_COMPACT`, `_ROW_BLOCK_ROWS`,
/// `_DATA_BLOCK_BYTES`), and the schema
/// [`vortex_storage_schema`] gives, as `vortex@py` (`vortex.io.write`) does,
/// so the artifact is the one a build would publish, not merely one that
/// round-trips.
pub fn write_vortex(output: &Path, canonical: &Path) -> Result<()> {
    let (schema, batches) = open_canonical(canonical)?;
    let schema = vortex_storage_schema(&schema);
    runtime()?.block_on(async {
        let session = VortexSession::default().with_tokio();
        let dtype = session
            .arrow()
            .from_arrow_schema(&schema)
            .context("canonical schema -> vortex dtype")?;
        let convert = session.clone();
        let arrays = batches.map(move |batch| {
            let batch = batch.map_err(VortexError::from)?;
            let batch = RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())?;
            convert.arrow().from_arrow_record_batch(batch, &schema)
        });
        let stream = ArrayStreamAdapter::new(dtype, futures::stream::iter(arrays));
        let mut file = tokio::fs::File::create(output)
            .await
            .with_context(|| format!("create vortex {}", output.display()))?;
        // The Vortex write settings (`spec.FORMAT_SETTINGS["vortex"]`). Unset is the
        // default strategy; set, the strategy vortex@py builds, BtrBlocks limited to
        // the encodings the session's editions allow, compact on request.
        let compact = switch(VORTEX_COMPACT, std::env::var_os(VORTEX_COMPACT).as_deref())?;
        let row_block_rows = setting_count(VORTEX_ROW_BLOCK_ROWS)?;
        let data_block_bytes = setting_count(VORTEX_DATA_BLOCK_BYTES)?;
        let mut options = session.write_options();
        if compact.is_some() || row_block_rows.is_some() || data_block_bytes.is_some() {
            use vortex::editions::{ComponentKind, EditionSessionExt};
            let allowed = session
                .enabled_component_ids(ComponentKind::Array)
                .into_iter()
                .collect();
            let mut compressor = vortex::compressor::BtrBlocksCompressorBuilder::default();
            if compact == Some(true) {
                compressor = compressor.with_compact();
            }
            let mut strategy = vortex::file::WriteStrategyBuilder::default()
                .with_btrblocks_builder(compressor.retain_allowed_encodings(&allowed));
            if let Some(rows) = row_block_rows {
                strategy = strategy.with_row_block_size(rows);
            }
            if let Some(bytes) = data_block_bytes {
                strategy = strategy.with_data_block_target_bytes(Some(bytes as u64));
            }
            options = options.with_strategy(strategy.build());
        }
        options
            .write(&mut file, stream)
            .await
            .context("write vortex file")?;
        Ok::<(), anyhow::Error>(())
    })
}

/// A `.vortex` file's batches, holding the runtime that drives its scan.
pub struct VortexBatches {
    reader: Box<dyn RecordBatchReader + Send>,
    // Dropped after `reader`, whose scan runs on it.
    _runtime: tokio::runtime::Runtime,
}

impl Iterator for VortexBatches {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next().map(|b| b.context("scan vortex chunk"))
    }
}

/// Open a `.vortex` file as a batch stream, with the Arrow schema of its own
/// dtype and field names. Only logical_eq may normalize physical types, after
/// verifying that conversion is lossless; supplying the canonical schema here
/// would erase renamed fields and cast before logical_eq could detect it.
pub fn open_vortex(input: &Path) -> Result<(SchemaRef, VortexBatches)> {
    let runtime = runtime()?;
    let blocking = TokioRuntime::from(runtime.handle());
    let session = VortexSession::default().with_handle(blocking.handle());
    let file = blocking
        .block_on(session.open_options().open_path(input))
        .with_context(|| format!("open vortex {}", input.display()))?;
    // Derived before scanning so an empty file retains its fields.
    let schema = Arc::new(session.arrow().to_arrow_schema(file.dtype())?);
    let reader = file
        .scan()?
        .into_record_batch_reader(schema.clone(), &blocking)?;
    Ok((
        schema,
        VortexBatches {
            reader: Box::new(reader),
            _runtime: runtime,
        },
    ))
}

// ---------------------------------------------------------------------------
// Logical comparison + variant detection
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// ORC (orc-rust)
// ---------------------------------------------------------------------------

/// `data_type` as the ORC lane stores it: ORC has no unsigned integers or view
/// types, so the lane deliberately expands them, losslessly, before any writer
/// sees them: UInt8/16/32 to the next wider signed integer, UInt64 to
/// Decimal128(20, 0), and views to their plain equivalents, at any depth.
fn orc_storage_type(data_type: &DataType) -> DataType {
    let field = |f: &Arc<Field>| {
        Arc::new(
            f.as_ref()
                .clone()
                .with_data_type(orc_storage_type(f.data_type())),
        )
    };
    match data_type {
        DataType::UInt8 => DataType::Int16,
        DataType::UInt16 => DataType::Int32,
        DataType::UInt32 => DataType::Int64,
        DataType::UInt64 => DataType::Decimal128(20, 0),
        DataType::Utf8View => DataType::Utf8,
        DataType::BinaryView => DataType::Binary,
        DataType::List(f) => DataType::List(field(f)),
        DataType::LargeList(f) => DataType::LargeList(field(f)),
        DataType::FixedSizeList(f, n) => DataType::FixedSizeList(field(f), *n),
        DataType::Map(f, sorted) => DataType::Map(field(f), *sorted),
        DataType::Struct(fields) => DataType::Struct(fields.iter().map(field).collect()),
        other => other.clone(),
    }
}

/// Write `output` from the canonical with orc-rust's `ArrowWriter` and the ORC
/// write settings: unset, zstd, since the API makes the caller pick a codec (its
/// default is none), and its own default stripe and compression block sizes. Columns are first expanded to
/// [`orc_storage_type`]. orc-rust panics on a type it does not write
/// (`unimplemented!("unsupported datatype")`) -- 0.9.0 writes no decimal, so a
/// uint64 column fails here (measured); [`run_writer`] reports that panic as
/// the write's failure.
pub fn write_orc(output: &Path, canonical: &Path) -> Result<()> {
    let (canonical_schema, reader) = open_canonical(canonical)?;
    let schema = Arc::new(Schema::new_with_metadata(
        canonical_schema
            .fields()
            .iter()
            .map(|f| {
                f.as_ref()
                    .clone()
                    .with_data_type(orc_storage_type(f.data_type()))
            })
            .collect::<Vec<_>>(),
        canonical_schema.metadata().clone(),
    ));
    // The ORC write settings (`spec.FORMAT_SETTINGS["orc"]`); unset, zstd.
    use orc_rust::compression::CompressionType;
    refuse_set(
        "orc@rs",
        ORC_COMPRESSION_STRATEGY,
        choice(ORC_COMPRESSION_STRATEGY, &["speed", "compression"])?,
        "orc-rust has no compression strategy",
    )?;
    let mut builder = orc_rust::ArrowWriterBuilder::new(
        File::create(output).with_context(|| format!("create {}", output.display()))?,
        schema.clone(),
    );
    let codec = choice(ORC_COMPRESSION, &["zstd", "snappy", "zlib", "lz4", "none"])?;
    builder = match codec.unwrap_or("zstd") {
        "zstd" => builder.with_compression(CompressionType::Zstd),
        "snappy" => builder.with_compression(CompressionType::Snappy),
        "zlib" => builder.with_compression(CompressionType::Zlib),
        "lz4" => builder.with_compression(CompressionType::Lz4),
        _ => builder,
    };
    if let Some(bytes) = setting_count(ORC_STRIPE_BYTES)? {
        builder = builder.with_stripe_byte_size(bytes);
    }
    if let Some(bytes) = setting_count(ORC_COMPRESSION_BLOCK_BYTES)? {
        builder = builder.with_compression_block_size(bytes);
    }
    let mut writer = builder
        .try_build()
        .context("orc-rust: start the ORC file")?;
    for batch in canonical_batches(reader) {
        let batch = batch?;
        let columns = batch
            .columns()
            .iter()
            .zip(schema.fields())
            .map(|(column, field)| arrow_cast::cast(column, field.data_type()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .context("expand the canonical to ORC's types")?;
        writer
            .write(&RecordBatch::try_new(schema.clone(), columns)?)
            .context("orc-rust: write a record batch")?;
    }
    writer.close().context("orc-rust: finish the ORC file")
}

/// Open an ORC file with orc-rust's `ArrowReader`, in the batches it yields.
pub fn open_orc(input: &Path) -> Result<(SchemaRef, impl Iterator<Item = Result<RecordBatch>>)> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let reader = orc_rust::ArrowReaderBuilder::try_new(file)
        .with_context(|| format!("orc-rust: read the ORC footer of {}", input.display()))?
        .build();
    let schema = reader.schema();
    Ok((
        schema,
        reader.map(|b| b.context("orc-rust: read a record batch")),
    ))
}

// ---------------------------------------------------------------------------
// Avro (arrow-avro)
// ---------------------------------------------------------------------------

/// The sync marker of every Avro file raincloud writes. An object container
/// file separates its blocks with a 16-byte marker the writer chooses, and
/// arrow-avro draws it at random, so the same canonical would give a file with
/// a different sha256 on every build. The Java lane writes the same marker.
pub const AVRO_SYNC_MARKER: &[u8; 16] = b"raincloud-avro01";

/// Replace the random sync marker `drawn` with [`AVRO_SYNC_MARKER`] in the
/// object container file at `path`, in place: after the header and after each
/// block, each checked to be `drawn` first. Nothing else in the file changes.
///
/// arrow-avro offers no way to choose the marker: `AvroOcfFormat` draws it,
/// and the `AvroFormat` trait a caller could implement instead must write the
/// header itself, whose schema JSON arrow-avro builds with a crate-private
/// option. Rewriting the marker keeps every other byte arrow-avro's.
fn fix_sync_marker(path: &Path, drawn: &[u8; 16]) -> Result<()> {
    use std::io::{BufReader, Read, Seek, SeekFrom, Write};

    struct Counted<R> {
        inner: R,
        at: u64,
    }
    impl<R: Read> Counted<R> {
        fn bytes(&mut self, n: u64) -> Result<Vec<u8>> {
            let mut buf = vec![0; n as usize];
            self.inner.read_exact(&mut buf)?;
            self.at += n;
            Ok(buf)
        }
        /// An Avro `long` (zig-zag varint), or None at a clean end of file.
        fn long(&mut self) -> Result<Option<i64>> {
            let (mut n, mut shift) = (0u64, 0);
            loop {
                let mut byte = [0u8];
                if self.inner.read(&mut byte)? == 0 {
                    if shift == 0 {
                        return Ok(None);
                    }
                    bail!("truncated Avro long");
                }
                self.at += 1;
                n |= u64::from(byte[0] & 0x7f) << shift;
                if byte[0] & 0x80 == 0 {
                    return Ok(Some((n >> 1) as i64 ^ -((n & 1) as i64)));
                }
                shift += 7;
            }
        }
        fn need(&mut self) -> Result<i64> {
            self.long()?.context("unexpected end of the Avro file")
        }
        fn marker(&mut self, drawn: &[u8; 16], markers: &mut Vec<u64>) -> Result<()> {
            let at = self.at;
            if self.bytes(16)? != drawn {
                bail!("no sync marker at byte {at}");
            }
            markers.push(at);
            Ok(())
        }
    }

    let mut file = Counted {
        inner: BufReader::new(File::open(path)?),
        at: 0,
    };
    let mut markers = Vec::new();
    if file.bytes(4)? != b"Obj\x01" {
        bail!("not an Avro object container file");
    }
    // The header's metadata map: blocks of key/value pairs, ending with 0.
    loop {
        let mut count = file.need()?;
        if count == 0 {
            break;
        }
        if count < 0 {
            count = -count;
            file.need()?; // the block's byte size
        }
        for _ in 0..2 * count {
            let len = file.need()?;
            file.bytes(len as u64)?;
        }
    }
    file.marker(drawn, &mut markers)?;
    // Data blocks: a row count, a byte size, the bytes, the marker.
    while file.long()?.is_some() {
        let size = file.need()?;
        file.bytes(size as u64)?;
        file.marker(drawn, &mut markers)?;
    }
    let mut out = std::fs::OpenOptions::new().write(true).open(path)?;
    for at in markers {
        out.seek(SeekFrom::Start(at))?;
        out.write_all(AVRO_SYNC_MARKER)?;
    }
    out.sync_all()?;
    Ok(())
}

/// Write `output` from the canonical with arrow-avro's `AvroWriter`: the Avro
/// write settings' codec (unset, zstd), one block per canonical batch, then [`AVRO_SYNC_MARKER`] in place of its random
/// sync marker, for a reproducible file. A type arrow-avro does not write is
/// its error; nothing here converts a column.
pub fn write_avro(output: &Path, canonical: &Path) -> Result<()> {
    let (schema, reader) = open_canonical(canonical)?;
    // The Avro write settings (`spec.FORMAT_SETTINGS["avro"]`); unset, zstd.
    let level = std::env::var_os(AVRO_COMPRESSION_LEVEL)
        .and_then(|raw| {
            raw.to_str()
                .map(|s| s.trim_matches(is_knob_space).to_string())
        })
        .filter(|s| !s.is_empty());
    refuse_set(
        "avro@rs",
        AVRO_COMPRESSION_LEVEL,
        level,
        "arrow-avro has no compression level",
    )?;
    refuse_set(
        "avro@rs",
        AVRO_BLOCK_BYTES,
        setting_count(AVRO_BLOCK_BYTES)?,
        "arrow-avro writes one block per batch, with no block size",
    )?;
    let codec = match choice(
        AVRO_COMPRESSION,
        &["zstd", "deflate", "snappy", "bzip2", "xz", "none"],
    )?
    .unwrap_or("zstd")
    {
        "zstd" => Some(CompressionCodec::ZStandard),
        "deflate" => Some(CompressionCodec::Deflate),
        "snappy" => Some(CompressionCodec::Snappy),
        "bzip2" => Some(CompressionCodec::Bzip2),
        "xz" => Some(CompressionCodec::Xz),
        _ => None,
    };
    let file = File::create(output).with_context(|| format!("create {}", output.display()))?;
    let mut writer = AvroWriterBuilder::new(schema.as_ref().clone())
        .with_compression(codec)
        .build::<_, AvroOcfFormat>(std::io::BufWriter::new(file))
        .context("arrow-avro: start the Avro file")?;
    let drawn = *writer.sync_marker().context("arrow-avro: no sync marker")?;
    for batch in canonical_batches(reader) {
        writer
            .write(&batch?)
            .context("arrow-avro: write a record batch")?;
    }
    writer
        .finish()
        .context("arrow-avro: finish the Avro file")?;
    drop(writer);
    fix_sync_marker(output, &drawn).context("set the Avro sync marker")
}

/// Open an Avro object container file with arrow-avro's `Reader`, in the
/// batches it yields.
pub fn open_avro(input: &Path) -> Result<(SchemaRef, impl Iterator<Item = Result<RecordBatch>>)> {
    let file = File::open(input).with_context(|| format!("open {}", input.display()))?;
    let reader = AvroReaderBuilder::new()
        .build(std::io::BufReader::new(file))
        .with_context(|| format!("arrow-avro: read the Avro header of {}", input.display()))?;
    let schema = reader.schema();
    Ok((
        schema,
        reader.map(|b| b.context("arrow-avro: read a record batch")),
    ))
}

/// True if any top-level field carries raincloud's VARIANT marker.
pub fn has_variant(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .any(|f| f.metadata().contains_key(VARIANT_MARKER))
}

fn is_variant_extension(field: &Field) -> bool {
    field.metadata().get(EXTENSION_NAME).map(String::as_str) == Some(VARIANT_EXTENSION)
}

/// The canonical's top-level VARIANT columns: raincloud's marker or the
/// `arrow.parquet.variant` extension, as `discovery._is_variant_field` reads them.
pub fn variant_columns(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter(|f| f.metadata().contains_key(VARIANT_MARKER) || is_variant_extension(f))
        .map(|f| f.name().clone())
        .collect()
}

/// Why a Parquet file does not keep the canonical's VARIANT columns, or `None`
/// when it does: each must be a group the file declares with Parquet's VARIANT
/// logical type, and must read back (`read_back`, arrow-rs's schema for the
/// file) as the `arrow.parquet.variant` extension. The ARROW:schema hint alone
/// carries the extension name through a plain group, so the read-back is not
/// enough by itself.
pub fn parquet_variant_loss(
    columns: &[String],
    parquet: &Path,
    read_back: &Schema,
) -> Result<Option<String>> {
    use parquet::basic::LogicalType;
    let file =
        File::open(parquet).with_context(|| format!("open parquet {}", parquet.display()))?;
    let metadata = ArrowReaderMetadata::load(&file, Default::default())
        .context("read parquet footer for its VARIANT columns")?;
    let root = metadata
        .metadata()
        .file_metadata()
        .schema_descr()
        .root_schema();
    let mut losses = Vec::new();
    for column in columns {
        let declared = root.get_fields().iter().any(|f| {
            f.name() == column
                && matches!(
                    f.get_basic_info().logical_type_ref(),
                    Some(LogicalType::Variant(_))
                )
        });
        if !declared {
            losses.push(format!(
                "column {column:?}: the file declares no Parquet VARIANT logical type"
            ));
        }
        if !read_back
            .field_with_name(column)
            .is_ok_and(is_variant_extension)
        {
            losses.push(format!(
                "column {column:?}: read back without the {VARIANT_EXTENSION} extension"
            ));
        }
    }
    Ok((!losses.is_empty()).then(|| losses.join("; ")))
}

// Check schema before values: reversible casts of empty/null arrays can erase
// incompatible types and struct children. Representation widths, dictionary
// indices, list element names and metadata are intentionally not identities;
// nor are a null|T union's spelling of a nullable T, a timestamp's zone label,
// or a scale-0 decimal's spelling of an integer (sidecars/compare_cases).
fn compatible_fields(got: &Fields, expected: &Fields) -> bool {
    got.len() == expected.len()
        && got
            .iter()
            .zip(expected)
            .all(|(g, e)| g.name() == e.name() && compatible(g.data_type(), e.data_type()))
}

/// The type id and field of `T` in a union of exactly `null` and `T`, else
/// None: such a union is how some readers spell a nullable `T` (Avro's
/// `["null", T]`).
fn nullable_member(data_type: &DataType) -> Option<(i8, &Arc<Field>)> {
    let DataType::Union(fields, _) = data_type else {
        return None;
    };
    let mut members = fields
        .iter()
        .filter(|(_, f)| f.data_type() != &DataType::Null);
    match (fields.len(), members.next(), members.next()) {
        (2, Some(member), None) => Some(member),
        _ => None,
    }
}

fn has_nullable_union(data_type: &DataType) -> bool {
    use DataType::*;
    nullable_member(data_type).is_some()
        || match data_type {
            Struct(fields) => fields.iter().any(|f| has_nullable_union(f.data_type())),
            List(f)
            | LargeList(f)
            | ListView(f)
            | LargeListView(f)
            | FixedSizeList(f, _)
            | Map(f, _) => has_nullable_union(f.data_type()),
            _ => false,
        }
}

/// `array` with every union of `null` and `T` replaced by the nullable `T` it
/// spells, at any depth of struct, list and map: a row selecting the null
/// member is null, any other is the `T` member's value (a sparse member at the
/// row, a dense one at its offset).
fn without_nullable_unions(
    array: &arrow_array::ArrayRef,
) -> Result<arrow_array::ArrayRef, arrow_schema::ArrowError> {
    use DataType::*;
    if !has_nullable_union(array.data_type()) {
        return Ok(Arc::clone(array));
    }
    if let Some((code, _)) = nullable_member(array.data_type()) {
        let union = array
            .as_any()
            .downcast_ref::<arrow_array::UnionArray>()
            .expect("a union type is a UnionArray");
        let picks: arrow_array::UInt32Array = (0..union.len())
            .map(|i| (union.type_id(i) == code).then(|| union.value_offset(i) as u32))
            .collect();
        let picked = arrow_select::take::take(union.child(code).as_ref(), &picks, None)?;
        return without_nullable_unions(&picked);
    }
    let data = array.to_data();
    let children = data
        .child_data()
        .iter()
        .map(|c| without_nullable_unions(&arrow_array::make_array(c.clone())).map(|a| a.to_data()))
        .collect::<Result<Vec<_>, _>>()?;
    // A field that held the union now holds nulls for its null member.
    let field = |f: &Arc<Field>, child: &DataType| {
        let nullable = f.is_nullable() || nullable_member(f.data_type()).is_some();
        Arc::new(
            f.as_ref()
                .clone()
                .with_data_type(child.clone())
                .with_nullable(nullable),
        )
    };
    let child = |i: usize| children[i].data_type();
    let dtype = match array.data_type() {
        Struct(fields) => Struct(
            fields
                .iter()
                .enumerate()
                .map(|(i, f)| field(f, child(i)))
                .collect(),
        ),
        List(f) => List(field(f, child(0))),
        LargeList(f) => LargeList(field(f, child(0))),
        ListView(f) => ListView(field(f, child(0))),
        LargeListView(f) => LargeListView(field(f, child(0))),
        FixedSizeList(f, n) => FixedSizeList(field(f, child(0)), *n),
        Map(f, sorted) => Map(field(f, child(0)), *sorted),
        _ => unreachable!("has_nullable_union covers these containers only"),
    };
    Ok(arrow_array::make_array(
        data.into_builder()
            .data_type(dtype)
            .child_data(children)
            .build()?,
    ))
}

/// A decimal holding integers: scale 0.
fn is_integral_decimal(data_type: &DataType) -> bool {
    use DataType::*;
    matches!(
        data_type,
        Decimal32(_, 0) | Decimal64(_, 0) | Decimal128(_, 0) | Decimal256(_, 0)
    )
}

fn compatible(got: &DataType, expected: &DataType) -> bool {
    use DataType::*;
    // A null|T union is a nullable T, on either side.
    if let Some((_, member)) = nullable_member(got) {
        return compatible(member.data_type(), expected);
    }
    if let Some((_, member)) = nullable_member(expected) {
        return compatible(got, member.data_type());
    }
    match (got, expected) {
        (Dictionary(_, value), other) | (other, Dictionary(_, value)) => compatible(value, other),
        (Struct(g), Struct(e)) => compatible_fields(g, e),
        (FixedSizeList(_, g), FixedSizeList(_, e)) if g != e => false,
        (
            List(g) | LargeList(g) | ListView(g) | LargeListView(g) | FixedSizeList(g, _),
            List(e) | LargeList(e) | ListView(e) | LargeListView(e) | FixedSizeList(e, _),
        ) => compatible(g.data_type(), e.data_type()),
        (Map(g, _), Map(e, _)) => compatible(g.data_type(), e.data_type()),
        (Union(g, gm), Union(e, em)) => {
            gm == em
                && g.len() == e.len()
                && g.iter().zip(e.iter()).all(|((gc, gf), (ec, ef))| {
                    gc == ec && gf.name() == ef.name() && compatible(gf.data_type(), ef.data_type())
                })
        }
        (RunEndEncoded(_, g), RunEndEncoded(_, e)) => compatible(g.data_type(), e.data_type()),
        // A zone labels UTC instants; it is not data. Naive and zoned differ in kind.
        (Timestamp(_, g), Timestamp(_, e)) => g.is_some() == e.is_some(),
        _ if (got.is_integer() && is_integral_decimal(expected))
            || (is_integral_decimal(got) && expected.is_integer()) =>
        {
            true
        }
        (FixedSizeBinary(g), FixedSizeBinary(e)) if g != e => false,
        (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View)
        | (
            Binary | LargeBinary | BinaryView | FixedSizeBinary(_),
            Binary | LargeBinary | BinaryView | FixedSizeBinary(_),
        )
        | (Date32 | Date64, Date32 | Date64)
        | (Time32(_) | Time64(_), Time32(_) | Time64(_))
        | (Duration(_), Duration(_)) => true,
        _ if got.is_integer() && expected.is_integer() => true,
        _ if got.is_floating() && expected.is_floating() => true,
        _ if got.is_decimal() && expected.is_decimal() => true,
        _ => got == expected,
    }
}

// Arrow has no direct BinaryView <-> FixedSizeBinary kernel. Binary offsets
// bridge the two losslessly; the caller still verifies the reverse conversion.
fn normalize(
    array: &dyn Array,
    target: &DataType,
    opts: &arrow_cast::CastOptions<'_>,
) -> Result<arrow_array::ArrayRef, arrow_schema::ArrowError> {
    // Normalize nested children ourselves: Arrow's container cast kernels do
    // not invoke this bridge for BinaryView/fixed binary, and can retain old
    // nested list element names. Keep the source container layout and buffers;
    // only its children/fields change before the ordinary container cast.
    use DataType::*;
    let child_type = match (array.data_type(), target) {
        (
            source,
            List(field)
            | LargeList(field)
            | ListView(field)
            | LargeListView(field)
            | FixedSizeList(field, _),
        ) => match source {
            List(_) => Some(List(Arc::clone(field))),
            LargeList(_) => Some(LargeList(Arc::clone(field))),
            ListView(_) => Some(ListView(Arc::clone(field))),
            LargeListView(_) => Some(LargeListView(Arc::clone(field))),
            FixedSizeList(_, size) => Some(FixedSizeList(Arc::clone(field), *size)),
            _ => None,
        },
        (Struct(_), Struct(fields)) => Some(Struct(fields.clone())),
        (Map(_, sorted), Map(field, _)) => Some(Map(Arc::clone(field), *sorted)),
        _ => None,
    };
    let nested;
    let array = if let Some(dtype) = child_type {
        let data = array.to_data();
        let fields: Vec<_> = match &dtype {
            Struct(fields) => fields.iter().collect(),
            List(field)
            | LargeList(field)
            | ListView(field)
            | LargeListView(field)
            | FixedSizeList(field, _)
            | Map(field, _) => vec![field],
            _ => unreachable!(),
        };
        let children = data
            .child_data()
            .iter()
            .zip(fields)
            .map(|(child, field)| {
                let child = arrow_array::make_array(child.clone());
                normalize(child.as_ref(), field.data_type(), opts).map(|a| a.to_data())
            })
            .collect::<Result<Vec<_>, _>>()?;
        nested = arrow_array::make_array(
            data.into_builder()
                .data_type(dtype)
                .child_data(children)
                .build()?,
        );
        nested.as_ref()
    } else {
        array
    };
    if matches!(
        (array.data_type(), target),
        (DataType::BinaryView, DataType::FixedSizeBinary(_))
            | (DataType::FixedSizeBinary(_), DataType::BinaryView)
    ) {
        let offsets = arrow_cast::cast_with_options(array, &DataType::Binary, opts)?;
        arrow_cast::cast_with_options(offsets.as_ref(), target, opts)
    } else {
        let cast = arrow_cast::cast_with_options(array, target, opts)?;
        // Arrow's fixed-size-list -> variable-list kernel retains the old
        // element field when the child dtype is unchanged. A same-list cast
        // applies the requested field name/nullability without touching values.
        if matches!(array.data_type(), DataType::FixedSizeList(_, _))
            && cast.data_type() != target
            && matches!(
                target,
                DataType::List(_)
                    | DataType::LargeList(_)
                    | DataType::ListView(_)
                    | DataType::LargeListView(_)
            )
        {
            arrow_cast::cast_with_options(cast.as_ref(), target, opts)
        } else {
            Ok(cast)
        }
    }
}

/// Compare `got` to the canonical `expected` with LOGICAL equality.
///
/// Returns `(matches, detail)` — `detail` is a human-readable mismatch reason
/// when `!matches`, else empty. Row count, names and recursive logical schema
/// compatibility first, then each column, with null|T unions read as nullable T,
/// is cast losslessly to the canonical type (identity when types already match)
/// and compared at the `ArrayData` level. `ArrayData` equality ignores
/// field metadata, so a dropped VARIANT annotation does not fail the round-trip.
fn logical_eq(got: &RecordBatch, expected: &RecordBatch) -> (bool, String) {
    if got.num_rows() != expected.num_rows() {
        return (
            false,
            format!(
                "row count {} != canonical {}",
                got.num_rows(),
                expected.num_rows()
            ),
        );
    }
    let exp_names: Vec<String> = expected
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    let got_names: Vec<String> = got
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    if got_names != exp_names {
        return (
            false,
            format!(
                "column names differ: {:?} != canonical {:?}",
                got_names, exp_names
            ),
        );
    }
    // arrow-rs `cast` defaults to safe=true = TRUNCATE/NULL on a lossy cast —
    // the OPPOSITE of pyarrow's `Table.cast(safe=True)`, which RAISES. To match
    // the Python `_roundtrip_verdict` (a lossless cast or a fail), we (a) cast with
    // safe=false so an out-of-range integer cast errors, and (b) round-trip the
    // cast back to the original type and require it to recover `g` exactly —
    // catching the truncations arrow-rs performs SILENTLY even under safe=false
    // (temporal-unit downcast, decimal rescale, float64->float32). Schema checks
    // reject cross-family coercions even if this particular data casts exactly.
    // Any information-losing coercion is thus a `fail`; benign normalizations
    // (string<->string_view<->large_string, dict<->plain, integer widening)
    // round-trip exactly and stay `pass`.
    let opts = arrow_cast::CastOptions {
        safe: false,
        ..Default::default()
    };
    for (i, (g, e)) in got
        .columns()
        .iter()
        .zip(expected.columns().iter())
        .enumerate()
    {
        if !compatible(g.data_type(), e.data_type()) {
            return (
                false,
                format!(
                    "column {:?}: incompatible logical types {} vs {}",
                    exp_names[i],
                    g.data_type(),
                    e.data_type()
                ),
            );
        }
        let (g, e) = match (without_nullable_unions(g), without_nullable_unions(e)) {
            (Ok(g), Ok(e)) => (g, e),
            (Err(err), _) | (_, Err(err)) => {
                return (
                    false,
                    format!(
                        "column {:?}: cannot read a null|T union as T: {err}",
                        exp_names[i]
                    ),
                );
            }
        };
        let (g, e) = (&g, &e);
        let g_cast = if g.data_type() == e.data_type() {
            Arc::clone(g)
        } else {
            let fwd = match normalize(g.as_ref(), e.data_type(), &opts) {
                Ok(c) => c,
                Err(err) => {
                    return (
                        false,
                        format!(
                            "column {:?}: cast {} -> {} failed: {}",
                            exp_names[i],
                            g.data_type(),
                            e.data_type(),
                            err
                        ),
                    );
                }
            };
            // Losslessness guard: cast back; if it doesn't recover `g`, the
            // forward cast lost information -> a real mismatch (not a round-trip).
            match normalize(fwd.as_ref(), g.data_type(), &opts) {
                Ok(back) if back.to_data() == g.to_data() => fwd,
                Ok(_) => {
                    return (
                        false,
                        format!(
                            "column {:?}: lossy cast {} -> {} (not round-trip-stable)",
                            exp_names[i],
                            g.data_type(),
                            e.data_type()
                        ),
                    );
                }
                Err(err) => {
                    return (
                        false,
                        format!(
                            "column {:?}: cast-back {} -> {} failed: {}",
                            exp_names[i],
                            e.data_type(),
                            g.data_type(),
                            err
                        ),
                    );
                }
            }
        };
        if g_cast.to_data() != e.to_data() {
            return (
                false,
                format!("column {:?}: data mismatch vs canonical", exp_names[i]),
            );
        }
    }
    (true, String::new())
}

/// One side of a streamed comparison: a batch stream read as a row sequence.
struct Rows<I> {
    batches: I,
    current: Option<RecordBatch>,
    offset: usize,
}

impl<I: Iterator<Item = Result<RecordBatch>>> Rows<I> {
    /// Rows left in the current batch, advancing past empty ones; 0 at the end.
    fn available(&mut self) -> Result<usize> {
        loop {
            if let Some(batch) = &self.current {
                if self.offset < batch.num_rows() {
                    return Ok(batch.num_rows() - self.offset);
                }
            }
            match self.batches.next() {
                Some(batch) => {
                    self.current = Some(batch?);
                    self.offset = 0;
                }
                None => {
                    self.current = None;
                    return Ok(0);
                }
            }
        }
    }

    fn take(&mut self, n: usize) -> RecordBatch {
        let batch = self.current.as_ref().expect("take after available");
        let slice = batch.slice(self.offset, n);
        self.offset += n;
        slice
    }

    fn count_rest(&mut self) -> Result<usize> {
        let mut n = 0;
        loop {
            let available = self.available()?;
            if available == 0 {
                return Ok(n);
            }
            self.take(available);
            n += available;
        }
    }
}

/// `logical_eq` over two batch streams, holding one window of each at a time.
///
/// Batch boundaries need not agree: the canonical's ingestion batches
/// (`RAINCLOUD_BATCH_ROWS` rows) are compared against whatever the reader
/// yields, window by window.
pub fn logical_eq_stream<G, E>(
    got_schema: &SchemaRef,
    got: G,
    expected_schema: &SchemaRef,
    expected: E,
) -> Result<(bool, String)>
where
    G: Iterator<Item = Result<RecordBatch>>,
    E: Iterator<Item = Result<RecordBatch>>,
{
    // Names and types, checked even when both sides are empty.
    let (ok, detail) = logical_eq(
        &RecordBatch::new_empty(got_schema.clone()),
        &RecordBatch::new_empty(expected_schema.clone()),
    );
    if !ok {
        return Ok((false, detail));
    }
    let mut got = Rows {
        batches: got,
        current: None,
        offset: 0,
    };
    let mut expected = Rows {
        batches: expected,
        current: None,
        offset: 0,
    };
    let mut row = 0;
    loop {
        let (g, e) = (got.available()?, expected.available()?);
        if g == 0 || e == 0 {
            if g == 0 && e == 0 {
                return Ok((true, String::new()));
            }
            let (g, e) = (row + got.count_rest()?, row + expected.count_rest()?);
            return Ok((false, format!("row count {g} != canonical {e}")));
        }
        let n = g.min(e);
        let (ok, detail) = logical_eq(&got.take(n), &expected.take(n));
        if !ok {
            return Ok((false, format!("rows {row}..{}: {detail}", row + n)));
        }
        row += n;
    }
}

// ---------------------------------------------------------------------------
// Reports
// ---------------------------------------------------------------------------

/// Emit the WRITE-sidecar report: `{"roundtrip", "variant_faithful", "note"}`.
fn write_write_report(
    path: &Path,
    roundtrip: bool,
    variant_faithful: bool,
    note: &str,
) -> Result<()> {
    let v = serde_json::json!({
        "roundtrip": roundtrip,
        "variant_faithful": variant_faithful,
        "note": note,
    });
    std::fs::write(path, serde_json::to_vec(&v).context("serialize report")?)
        .with_context(|| format!("write report {}", path.display()))?;
    Ok(())
}

/// Emit the READ-sidecar report: `{"status", "note", "detail"}`.
fn write_read_report(path: &Path, status: &str, note: &str, detail: &str) -> Result<()> {
    let v = serde_json::json!({
        "status": status,
        "note": note,
        "detail": detail,
    });
    std::fs::write(path, serde_json::to_vec(&v).context("serialize report")?)
        .with_context(|| format!("write report {}", path.display()))?;
    Ok(())
}

/// Run `f`, turning a panic into an error so that a crash in a decoder or an
/// Arrow kernel still ends in a report. The panic hook has already printed
/// the message and its location to stderr.
fn unwound<T>(f: impl FnOnce() -> Result<T>) -> Result<T> {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let what = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "no message".into());
            Err(anyhow::anyhow!("panicked: {what}"))
        }
    }
}

/// Run a WRITE sidecar's `write`, which produces `output` and returns
/// `(roundtrip, variant_faithful, note)`, and report the outcome.
///
/// A failure or a panic is a report too: `roundtrip: false` with the whole
/// error chain as the note, also printed to stderr. `output` is removed on any
/// error, including a self-verify read error after the file was complete, so
/// nothing unverified can be promoted. The exit code is non-zero only when the
/// report itself cannot be written.
pub fn run_writer(
    cell: &str,
    output: &Path,
    report: &Path,
    write: impl FnOnce() -> Result<(bool, bool, String)>,
) -> std::process::ExitCode {
    let (roundtrip, variant_faithful, note) = match unwound(write) {
        Ok(outcome) => outcome,
        Err(e) => {
            eprintln!("{cell}: {e:#}");
            match std::fs::remove_file(output) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => eprintln!("{cell}: cannot remove {}: {err}", output.display()),
            }
            // False = unknown: the failure may precede reading the schema.
            (false, false, format!("{cell}: {e:#}"))
        }
    };
    finish(write_write_report(
        report,
        roundtrip,
        variant_faithful,
        &note,
    ))
}

/// Run a READ sidecar's `read`, which returns `(matches, detail)`, and report
/// the verdict. A failure to read either side, or a panic, is a `fail` whose
/// detail is the whole error chain (also printed to stderr). The exit code is
/// non-zero only when the report itself cannot be written.
pub fn run_reader(
    cell: &str,
    report: &Path,
    read: impl FnOnce() -> Result<(bool, String)>,
) -> std::process::ExitCode {
    let written = match unwound(read) {
        Ok((true, _)) => write_read_report(
            report,
            "pass",
            &format!("{cell}: round-trips to canonical"),
            "",
        ),
        Ok((false, detail)) => write_read_report(
            report,
            "fail",
            &format!("{cell}: data mismatch vs canonical"),
            &detail,
        ),
        Err(e) => {
            eprintln!("{cell}: {e:#}");
            write_read_report(
                report,
                "fail",
                &format!("{cell}: read error"),
                &format!("{e:#}"),
            )
        }
    };
    finish(written)
}

fn finish(written: Result<()>) -> std::process::ExitCode {
    match written {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("cannot write the report: {e:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::FixedSizeListBuilder;
    use arrow_array::builder::Int32Builder;
    use arrow_array::{ArrayRef, BinaryViewArray, FixedSizeBinaryArray, Int64Array, StringArray};
    use arrow_ipc::writer::FileWriter;

    fn batch(values: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values.to_vec()))]).unwrap()
    }

    fn stream(batches: Vec<RecordBatch>) -> impl Iterator<Item = Result<RecordBatch>> {
        batches.into_iter().map(Ok)
    }

    /// A per-test scratch directory, removed when the test ends.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new(test: &str) -> Self {
            let dir = std::env::temp_dir()
                .join(format!("raincloud-sidecars-{}-{test}", std::process::id()));
            match std::fs::remove_dir_all(&dir) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => panic!("{e}"),
                _ => {}
            }
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn path(&self, name: &str) -> std::path::PathBuf {
            self.0.join(name)
        }

        fn canonical(
            &self,
            name: &str,
            schema: &SchemaRef,
            batches: &[RecordBatch],
        ) -> std::path::PathBuf {
            let path = self.path(name);
            let mut writer = FileWriter::try_new(File::create(&path).unwrap(), schema).unwrap();
            for b in batches {
                writer.write(b).unwrap();
            }
            writer.finish().unwrap();
            path
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            if let Err(e) = std::fs::remove_dir_all(&self.0) {
                eprintln!("cannot remove {}: {e}", self.0.display());
            }
        }
    }

    fn report(path: &Path) -> serde_json::Value {
        serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
    }

    #[test]
    fn knobs_follow_the_shared_cases() {
        // The same table pytest and JUnit read: one grammar in every lane.
        let table: serde_json::Value =
            serde_json::from_str(include_str!("../../knob_cases.json")).unwrap();
        let parse = |raw: &str| knob("K", Some(OsStr::new(raw)), 7, 99);
        for case in table["cases"].as_array().unwrap() {
            let raw = case["raw"].as_str().unwrap();
            match case.get("error") {
                Some(error) => {
                    let e = parse(raw).unwrap_err().to_string();
                    assert!(
                        e.contains(error.as_str().unwrap()) && e.contains("K="),
                        "{raw:?}: {e}"
                    );
                }
                None => {
                    let expected = case["value"].as_u64().map_or(99, |v| v as usize);
                    assert_eq!(parse(raw).unwrap(), expected, "{raw:?}");
                }
            }
        }
        assert_eq!(knob("K", None, 7, 99).unwrap(), 7);
        // `as` saturates: a huge value is no cap.
        assert_eq!(parse("1e300").unwrap(), usize::MAX);
    }

    #[cfg(unix)]
    #[test]
    fn a_knob_that_is_not_utf8_is_an_error_not_the_default() {
        use std::os::unix::ffi::OsStrExt;
        let e = knob("K", Some(OsStr::from_bytes(b"1\xff")), 7, 99)
            .unwrap_err()
            .to_string();
        assert!(e.contains("K=") && e.contains("not valid UTF-8"), "{e}");
    }

    #[test]
    fn a_read_error_or_panic_is_a_fail_report() {
        let scratch = Scratch::new("reader-errors");
        let path = scratch.path("read.json");
        let code = run_reader("x@rs", &path, || {
            Err(anyhow::anyhow!("disk on fire")).context("open artifact")
        });
        assert_eq!(code, std::process::ExitCode::SUCCESS);
        assert_eq!(
            report(&path),
            serde_json::json!({"status": "fail", "note": "x@rs: read error",
                               "detail": "open artifact: disk on fire"})
        );
        let code = run_reader("x@rs", &path, || panic!("decoder exploded"));
        assert_eq!(code, std::process::ExitCode::SUCCESS);
        let got = report(&path);
        assert_eq!(got["status"], "fail");
        assert!(
            got["detail"]
                .as_str()
                .unwrap()
                .contains("panicked: decoder exploded"),
            "{got}"
        );
    }

    #[test]
    fn a_failed_self_verify_removes_the_written_output() {
        let scratch = Scratch::new("writer-errors");
        let (output, path) = (scratch.path("out.parquet"), scratch.path("write.json"));
        let code = run_writer("x@rs", &output, &path, || {
            std::fs::write(&output, b"complete but unverified")?;
            Err(anyhow::anyhow!("re-read failed"))
        });
        assert_eq!(code, std::process::ExitCode::SUCCESS);
        assert!(!output.exists());
        assert_eq!(
            report(&path),
            serde_json::json!({"roundtrip": false, "variant_faithful": false,
                               "note": "x@rs: re-read failed"})
        );
        let code = run_writer("x@rs", &output, &path, || {
            std::fs::write(&output, b"partial")?;
            panic!("take after available")
        });
        assert_eq!(code, std::process::ExitCode::SUCCESS);
        assert!(!output.exists());
        let got = report(&path);
        assert_eq!(got["roundtrip"], false);
        assert!(
            got["note"]
                .as_str()
                .unwrap()
                .contains("panicked: take after available"),
            "{got}"
        );
    }

    #[test]
    fn compatibility_is_by_logical_family() {
        use DataType::*;
        assert!(compatible(&Utf8View, &LargeUtf8));
        assert!(compatible(
            &Dictionary(Box::new(Int8), Box::new(Utf8)),
            &Utf8
        ));
        assert!(compatible(&Int8, &UInt64));
        assert!(!compatible(&Int32, &Utf8));
        assert!(!compatible(&Float64, &Int64));
        let ts =
            |tz: Option<&str>| Timestamp(arrow_schema::TimeUnit::Millisecond, tz.map(Into::into));
        assert!(compatible(&ts(Some("UTC")), &ts(Some("UTC"))));
        assert!(!compatible(&ts(Some("UTC")), &ts(None)));
        assert!(!compatible(&FixedSizeBinary(4), &FixedSizeBinary(8)));
        let fields = |name: &str| Struct(vec![Field::new(name, Int32, true)].into());
        assert!(compatible(&fields("a"), &fields("a")));
        assert!(!compatible(&fields("a"), &fields("b")));
    }

    #[test]
    fn comparisons_follow_the_shared_cases() {
        // The same pairs pytest and JUnit read; Rust takes no `gap`.
        let table: serde_json::Value =
            serde_json::from_str(include_str!("../../compare_cases/cases.json")).unwrap();
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../compare_cases");
        for case in table["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let (got_schema, got) = open_canonical(&dir.join(format!("{name}.got.arrow"))).unwrap();
            let (expected_schema, expected) =
                open_canonical(&dir.join(format!("{name}.expected.arrow"))).unwrap();
            let (equal, detail) = logical_eq_stream(
                &got_schema,
                canonical_batches(got),
                &expected_schema,
                canonical_batches(expected),
            )
            .unwrap();
            let verdict = if equal { "equal" } else { "differ" };
            assert_eq!(
                verdict,
                case["verdict"].as_str().unwrap(),
                "{name}: {detail}"
            );
        }
    }

    #[test]
    fn normalize_bridges_fixed_size_forms() {
        let opts = arrow_cast::CastOptions {
            safe: false,
            ..Default::default()
        };
        // Fixed-size list -> list takes the target's element field.
        let mut builder = FixedSizeListBuilder::new(Int32Builder::new(), 2);
        builder.values().append_slice(&[1, 2]);
        builder.append(true);
        let fsl: ArrayRef = Arc::new(builder.finish());
        let target = DataType::List(Arc::new(Field::new("element", DataType::Int32, true)));
        let list = normalize(fsl.as_ref(), &target, &opts).unwrap();
        assert_eq!(list.data_type(), &target);
        // BinaryView <-> FixedSizeBinary through Binary, both ways.
        let view: ArrayRef = Arc::new(BinaryViewArray::from(vec![&b"abcd"[..], &b"wxyz"[..]]));
        let fixed = normalize(view.as_ref(), &DataType::FixedSizeBinary(4), &opts).unwrap();
        let expected = FixedSizeBinaryArray::try_from_iter([b"abcd", b"wxyz"].into_iter()).unwrap();
        assert_eq!(fixed.to_data(), expected.to_data());
        let back = normalize(fixed.as_ref(), &DataType::BinaryView, &opts).unwrap();
        assert_eq!(back.to_data(), view.to_data());
    }

    #[test]
    fn streams_compare_across_misaligned_batches() {
        let schema = batch(&[]).schema();
        let got = stream(vec![batch(&[1, 2, 3]), batch(&[]), batch(&[4, 5])]);
        let expected = stream(vec![batch(&[1]), batch(&[2, 3, 4, 5])]);
        assert_eq!(
            logical_eq_stream(&schema, got, &schema, expected).unwrap(),
            (true, String::new())
        );

        let got = stream(vec![batch(&[1, 2]), batch(&[9])]);
        let expected = stream(vec![batch(&[1, 2, 3])]);
        let (ok, detail) = logical_eq_stream(&schema, got, &schema, expected).unwrap();
        assert!(!ok && detail.starts_with("rows 2..3"), "{detail}");
    }

    #[test]
    fn streams_count_extra_and_missing_rows() {
        let schema = batch(&[]).schema();
        let (ok, detail) = logical_eq_stream(
            &schema,
            stream(vec![batch(&[1, 2, 3])]),
            &schema,
            stream(vec![batch(&[1, 2])]),
        )
        .unwrap();
        assert!(!ok && detail == "row count 3 != canonical 2", "{detail}");
        let (ok, detail) =
            logical_eq_stream(&schema, stream(vec![]), &schema, stream(vec![batch(&[1])])).unwrap();
        assert!(!ok && detail == "row count 0 != canonical 1", "{detail}");
    }

    #[test]
    fn empty_streams_still_compare_schemas() {
        let schema = batch(&[]).schema();
        assert!(
            logical_eq_stream(&schema, stream(vec![]), &schema, stream(vec![]))
                .unwrap()
                .0
        );
        let other = Arc::new(Schema::new(vec![Field::new("y", DataType::Int64, true)]));
        let (ok, detail) =
            logical_eq_stream(&other, stream(vec![]), &schema, stream(vec![])).unwrap();
        assert!(!ok && detail.contains("column names differ"), "{detail}");
    }

    #[test]
    fn the_row_cap_closes_groups_when_bytes_do_not() {
        let rows: Vec<i64> = (0..10).collect();
        let b = batch(&rows);
        let scratch = Scratch::new("capped");
        let output = scratch.path("capped.parquet");
        let limits = RowGroupLimits {
            target_encoded_bytes: 1 << 62,
            max_rows: 3,
        };
        write_parquet(&output, b.schema(), limits, || Ok(stream(vec![b.clone()]))).unwrap();
        let file = ParquetRecordBatchReaderBuilder::try_new(File::open(&output).unwrap()).unwrap();
        let groups: Vec<i64> = file
            .metadata()
            .row_groups()
            .iter()
            .map(|g| g.num_rows())
            .collect();
        assert_eq!(groups, vec![3, 3, 3, 1]);
    }

    /// `groups` row groups of `rows` rows, each row `width` distinct bytes.
    fn binary_parquet(
        scratch: &Scratch,
        groups: usize,
        rows: usize,
        width: usize,
    ) -> (std::path::PathBuf, RecordBatch) {
        use arrow_array::BinaryArray;
        let values: Vec<Vec<u8>> = (0..groups * rows)
            .map(|i| (0..width).map(|j| (i * 31 + j) as u8).collect())
            .collect();
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, false)]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from_iter_values(&values))],
        )
        .unwrap();
        let output = scratch.path("binary.parquet");
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(rows))
            .set_dictionary_enabled(false)
            .build();
        let mut writer =
            ArrowWriter::try_new(File::create(&output).unwrap(), schema, Some(props)).unwrap();
        writer.write(&b).unwrap();
        writer.close().unwrap();
        (output, b)
    }

    /// Rows per batch of reading `path` within `budget` bytes and `max_rows`,
    /// after checking the batches reproduce `expected`.
    fn read_sizes(path: &Path, budget: u64, max_rows: usize, expected: &RecordBatch) -> Vec<usize> {
        let (schema, batches) = open_parquet_with(path, budget, max_rows).unwrap();
        let batches: Vec<_> = batches.map(Result::unwrap).collect();
        let verdict = logical_eq_stream(
            &schema,
            batches.clone().into_iter().map(Ok),
            &expected.schema(),
            stream(vec![expected.clone()]),
        )
        .unwrap();
        assert_eq!(verdict, (true, String::new()));
        batches.iter().map(RecordBatch::num_rows).collect()
    }

    #[test]
    fn parquet_read_batches_are_sized_by_bytes_within_a_group() {
        let scratch = Scratch::new("read-bytes");
        // Three groups of ~100 kB; a 30 kB budget reads each in slices of the
        // 29 rows that fit, and no batch spans two groups.
        let (path, b) = binary_parquet(&scratch, 3, 100, 1000);
        let sizes = read_sizes(&path, 30_000, 65_536, &b);
        assert_eq!(sizes, [29, 29, 29, 13].repeat(3));
        let (_, batches) = open_parquet_with(&path, 30_000, 65_536).unwrap();
        for batch in batches {
            let batch = batch.unwrap();
            assert!(batch.column(0).to_data().buffers()[1].len() <= 30_000);
        }
        // A row larger than the budget is still read, one row at a time.
        assert_eq!(read_sizes(&path, 500, 65_536, &b), vec![1; 300]);
    }

    #[test]
    fn small_parquet_groups_share_a_read_batch() {
        let scratch = Scratch::new("read-packed");
        let (path, b) = binary_parquet(&scratch, 3, 100, 1000);
        // Two ~100 kB groups fit 250 kB together; the third starts a batch.
        assert_eq!(read_sizes(&path, 250_000, 65_536, &b), [200, 100]);
        // The row cap still closes a batch that bytes would not.
        assert_eq!(read_sizes(&path, 250_000, 150, &b), [100, 100, 100]);
        assert_eq!(read_sizes(&path, 250_000, 40, &b), [40, 40, 20].repeat(3));
        // The whole file, and the default plan reads it as one batch.
        assert_eq!(read_sizes(&path, 1 << 30, 65_536, &b), [300]);
        let (_, batches) = open_parquet(&path).unwrap();
        assert_eq!(batches.count(), 1);
    }

    #[test]
    fn a_repeated_value_counts_every_copy_it_decodes_to() {
        use arrow_array::BinaryArray;
        let value = vec![7u8; 1000];
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, false)]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(BinaryArray::from_iter_values(vec![&value; 100]))],
        )
        .unwrap();
        let scratch = Scratch::new("read-dictionary");
        let output = scratch.path("dictionary.parquet");
        let mut writer =
            ArrowWriter::try_new(File::create(&output).unwrap(), schema, None).unwrap();
        writer.write(&b).unwrap();
        writer.close().unwrap();
        let file = ParquetRecordBatchReaderBuilder::try_new(File::open(&output).unwrap()).unwrap();
        let group = &file.metadata().row_groups()[0];
        // The dictionary encodes the value once; decoding makes 100 copies.
        assert!(group.column(0).uncompressed_size() < 10_000);
        assert!(decoded_bytes(group) >= 100_000, "{}", decoded_bytes(group));
    }

    fn options_from(pairs: &[(&str, &str)]) -> Result<ParquetOptions> {
        let vars: std::collections::HashMap<String, std::ffi::OsString> = pairs
            .iter()
            .map(|(k, v)| (format!("RAINCLOUD_PARQUET_{k}"), (*v).into()))
            .collect();
        ParquetOptions::from_vars(|name| vars.get(name).cloned())
    }

    #[test]
    fn parquet_options_read_as_the_python_lane_writes_them() {
        assert_eq!(options_from(&[]).unwrap(), ParquetOptions::default());
        let set = options_from(&[
            ("COMPRESSION", "lz4"),
            ("STATISTICS", "1"),
            ("PAGE_INDEX", " On "),
            ("PAGE_BYTES", "4096"),
            ("PAGE_ROWS", "0"),
        ])
        .unwrap();
        assert_eq!(set.compression, Compression::LZ4_RAW);
        assert_eq!(set.page_index, Some(true));
        assert_eq!(set.page_bytes, Some(4096));
        // 0 is no limit, as in parquet@py.
        assert_eq!(set.page_rows, Some((1 << 31) - 1));
        assert_eq!(
            options_from(&[("PAGE_INDEX", "")]).unwrap().page_index,
            None
        );
        let leveled = options_from(&[
            ("COMPRESSION_LEVEL", "9"),
            ("STATISTICS_COLUMNS", "100"),
            ("PAGE_INDEX_COLUMNS", "10"),
            ("DICTIONARY", "off"),
            ("DICTIONARY_PAGE_BYTES", "65536"),
            ("PAGE_CHECKSUMS", "0"),
        ])
        .unwrap();
        assert_eq!(
            leveled.compression,
            Compression::ZSTD(ZstdLevel::try_new(9).unwrap())
        );
        assert_eq!(
            (leveled.statistics_columns, leveled.page_index_columns),
            (Some(100), Some(10))
        );
        assert_eq!(
            (leveled.dictionary, leveled.dictionary_page_bytes),
            (Some(false), Some(65_536))
        );
        let gzip = options_from(&[("COMPRESSION", "gzip"), ("COMPRESSION_LEVEL", "0")]).unwrap();
        assert_eq!(
            gzip.compression,
            Compression::GZIP(parquet::basic::GzipLevel::try_new(0).unwrap())
        );
        for (pairs, error) in [
            (&[("PAGE_INDEX", "maybe")][..], "is not a switch"),
            (&[("COMPRESSION", "lzo")][..], "is not one of"),
            (&[("PAGE_BYTES", "1MiB")][..], "is not a number"),
            (
                &[("PAGE_INDEX", "1"), ("STATISTICS", "0")][..],
                "asks for statistics",
            ),
            (
                &[("STATISTICS_COLUMNS", "5"), ("STATISTICS", "0")][..],
                "asks for statistics",
            ),
            (
                &[("PAGE_INDEX", "0"), ("PAGE_INDEX_COLUMNS", "5")][..],
                "asks for a page index",
            ),
            (
                &[("COMPRESSION_LEVEL", "-1")][..],
                "is not a compression level",
            ),
            (&[("COMPRESSION_LEVEL", "23")][..], "for zstd"),
            (
                &[("COMPRESSION", "snappy"), ("COMPRESSION_LEVEL", "1")][..],
                "takes no compression level",
            ),
            (
                &[("PAGE_CHECKSUMS", "1")][..],
                "parquet@rs cannot honour RAINCLOUD_PARQUET_PAGE_CHECKSUMS=1",
            ),
        ] {
            let message = options_from(pairs).unwrap_err().to_string();
            assert!(message.contains(error), "{pairs:?}: {message}");
        }
    }

    #[test]
    fn parquet_options_decide_the_page_index_and_the_pages() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from_iter_values(
                0..50_000,
            ))],
        )
        .unwrap();
        let limits = RowGroupLimits {
            target_encoded_bytes: 128 << 20,
            max_rows: 10_000_000,
        };
        let scratch = Scratch::new("parquet-options");
        let written = |name: &str, options: ParquetOptions| {
            let output = scratch.path(name);
            write_parquet_with(&output, schema.clone(), limits, options, || {
                Ok(stream(vec![b.clone()]))
            })
            .unwrap();
            let file = File::open(&output).unwrap();
            let options = parquet::arrow::arrow_reader::ArrowReaderOptions::new()
                .with_page_index_policy(parquet::file::metadata::PageIndexPolicy::Optional);
            let reader =
                ParquetRecordBatchReaderBuilder::try_new_with_options(file, options).unwrap();
            let metadata = reader.metadata().clone();
            let pages = metadata
                .offset_index()
                .map(|index| index[0][0].page_locations().len());
            (metadata.column_index().is_some(), pages)
        };
        // arrow-rs's default: a page index, 20,000 rows a page.
        assert_eq!(
            written("default.parquet", ParquetOptions::default()),
            (true, Some(3))
        );
        let rows = ParquetOptions {
            page_rows: Some(1_000),
            ..Default::default()
        };
        // arrow-rs checks the row limit once per 1,024-value write batch, so a
        // 1,000-row limit gives 1,024-row pages.
        assert_eq!(written("rows.parquet", rows), (true, Some(49)));
        let without = ParquetOptions {
            page_index: Some(false),
            ..Default::default()
        };
        assert_eq!(written("without.parquet", without), (false, None));
    }

    #[test]
    fn seconds_times_and_timestamps_are_written_as_milliseconds() {
        use arrow_array::{Time32SecondArray, TimestampSecondArray};
        let schema = Arc::new(Schema::new(vec![
            Field::new("t", DataType::Time32(arrow_schema::TimeUnit::Second), true),
            Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Second, None),
                true,
            ),
        ]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Time32SecondArray::from(vec![Some(0), None, Some(86_399)])),
                Arc::new(TimestampSecondArray::from(vec![
                    Some(-1),
                    None,
                    Some(1_534_377_600),
                ])),
            ],
        )
        .unwrap();
        let scratch = Scratch::new("seconds");
        let output = scratch.path("seconds.parquet");
        // Explicit, so a developer's RAINCLOUD_ROW_GROUP_* cannot change the test.
        let limits = RowGroupLimits {
            target_encoded_bytes: 128 << 20,
            max_rows: 10_000_000,
        };
        write_parquet(&output, schema.clone(), limits, || {
            Ok(stream(vec![b.clone()]))
        })
        .unwrap();
        let (got_schema, got) = open_parquet(&output).unwrap();
        assert_eq!(
            got_schema.field(0).data_type(),
            &DataType::Time32(arrow_schema::TimeUnit::Millisecond)
        );
        assert!(
            logical_eq_stream(&got_schema, got, &schema, stream(vec![b]))
                .unwrap()
                .0
        );
    }

    fn vortex_round_trip(scratch: &Scratch, schema: &SchemaRef, batches: &[RecordBatch]) -> usize {
        let source = scratch.canonical("source.arrow", schema, batches);
        let output = scratch.path("out.vortex");
        write_vortex(&output, &source).unwrap();
        let (got_schema, got) = open_vortex(&output).unwrap();
        let got: Vec<_> = got.collect();
        let names = |s: &Schema| {
            s.fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&got_schema), names(schema));
        let (_, expected) = open_canonical(&source).unwrap();
        let count = got.len();
        let verdict = logical_eq_stream(
            &got_schema,
            got.into_iter(),
            schema,
            canonical_batches(expected),
        )
        .unwrap();
        assert_eq!(verdict, (true, String::new()));
        count
    }

    fn mixed_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]))
    }

    #[test]
    fn vortex_writes_and_reads_back_in_batches() {
        let schema = mixed_schema();
        let make = |xs: Vec<i64>| {
            let s: Vec<Option<String>> = xs
                .iter()
                .map(|x| (x % 3 != 0).then(|| format!("v{x}")))
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(xs)),
                    Arc::new(StringArray::from(s)),
                ],
            )
            .unwrap()
        };
        let batches = vec![
            make((0..1000).collect()),
            make((1000..1003).collect()),
            make((1003..BATCHES_ROWS).collect()),
        ];
        let scratch = Scratch::new("vortex");
        let count = vortex_round_trip(&scratch, &schema, &batches);
        assert!(count > 1, "the scan came back as {count} batch");
    }

    /// Rows enough for the default strategy to write more than one chunk.
    const BATCHES_ROWS: i64 = 200_000;

    #[test]
    fn an_empty_vortex_file_keeps_its_fields() {
        let schema = mixed_schema();
        let scratch = Scratch::new("vortex-empty");
        vortex_round_trip(&scratch, &schema, &[]);
        let scratch = Scratch::new("vortex-zero-rows");
        vortex_round_trip(&scratch, &schema, &[RecordBatch::new_empty(schema.clone())]);
    }

    #[test]
    fn orc_writes_and_reads_back_across_batches() {
        let schema = mixed_schema();
        let batch = |xs: Vec<i64>| {
            let s: Vec<Option<String>> = xs
                .iter()
                .map(|x| (x % 3 != 0).then(|| format!("v{x}")))
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(xs)),
                    Arc::new(StringArray::from(s)),
                ],
            )
            .unwrap()
        };
        let scratch = Scratch::new("orc");
        let source = scratch.canonical(
            "source.arrow",
            &schema,
            &[batch((0..1000).collect()), batch((1000..5000).collect())],
        );
        let output = scratch.path("out.orc");
        write_orc(&output, &source).unwrap();
        let (got_schema, got) = open_orc(&output).unwrap();
        let (_, expected) = open_canonical(&source).unwrap();
        assert_eq!(
            logical_eq_stream(&got_schema, got, &schema, canonical_batches(expected)).unwrap(),
            (true, String::new())
        );
    }

    #[test]
    fn orc_expands_unsigned_columns_to_wider_signed_ones() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::UInt8, true),
            Field::new("b", DataType::UInt32, true),
        ]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::UInt8Array::from(vec![
                    Some(0),
                    None,
                    Some(255),
                ])),
                Arc::new(arrow_array::UInt32Array::from(vec![
                    Some(0),
                    Some(u32::MAX),
                    None,
                ])),
            ],
        )
        .unwrap();
        let scratch = Scratch::new("orc-unsigned");
        let source = scratch.canonical("source.arrow", &schema, &[b]);
        let output = scratch.path("out.orc");
        write_orc(&output, &source).unwrap();
        let (got_schema, got) = open_orc(&output).unwrap();
        let types: Vec<_> = got_schema.fields().iter().map(|f| f.data_type()).collect();
        assert_eq!(types, [&DataType::Int16, &DataType::Int64]);
        let (_, expected) = open_canonical(&source).unwrap();
        assert_eq!(
            logical_eq_stream(&got_schema, got, &schema, canonical_batches(expected)).unwrap(),
            (true, String::new())
        );
    }

    #[test]
    fn orc_rust_panicking_on_a_uint64_column_is_an_error() {
        // Expanded to Decimal128(20, 0), which orc-rust 0.9.0 does not write.
        let schema = Arc::new(Schema::new(vec![Field::new("u", DataType::UInt64, true)]));
        let b = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::UInt64Array::from(vec![1, u64::MAX]))],
        )
        .unwrap();
        let scratch = Scratch::new("orc-uint64");
        let source = scratch.canonical("source.arrow", &schema, &[b]);
        let output = scratch.path("out.orc");
        let err = unwound(|| write_orc(&output, &source)).unwrap_err();
        assert!(
            format!("{err:#}").contains("unsupported datatype"),
            "{err:#}"
        );
    }

    #[test]
    fn avro_changes_only_the_sync_marker() {
        let schema = mixed_schema();
        let batch = |xs: Vec<i64>| {
            let s: Vec<Option<String>> = xs
                .iter()
                .map(|x| (x % 3 != 0).then(|| format!("v{x}")))
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(xs)),
                    Arc::new(StringArray::from(s)),
                ],
            )
            .unwrap()
        };
        let scratch = Scratch::new("avro");
        let batches = [batch((0..1000).collect()), batch((1000..3000).collect())];
        let source = scratch.canonical("source.arrow", &schema, &batches);
        let output = scratch.path("out.avro");
        write_avro(&output, &source).unwrap();
        let ours = std::fs::read(&output).unwrap();

        // What arrow-avro writes on its own, with its marker swapped for ours.
        let mut w = AvroWriterBuilder::new(schema.as_ref().clone())
            .with_compression(Some(CompressionCodec::ZStandard))
            .build::<_, AvroOcfFormat>(Vec::new())
            .unwrap();
        let drawn = *w.sync_marker().unwrap();
        for b in &batches {
            w.write(b).unwrap();
        }
        w.finish().unwrap();
        let mut theirs = w.into_inner();
        let mut at = 0;
        while let Some(i) = theirs[at..].windows(16).position(|w| w == drawn) {
            theirs[at + i..at + i + 16].copy_from_slice(AVRO_SYNC_MARKER);
            at += i + 16;
        }
        assert_eq!(ours, theirs);
        // After the header and after each of the two blocks.
        assert_eq!(
            ours.windows(16).filter(|w| w == AVRO_SYNC_MARKER).count(),
            3
        );
        let (got_schema, got) = open_avro(&output).unwrap();
        let (_, expected) = open_canonical(&source).unwrap();
        assert!(
            logical_eq_stream(&got_schema, got, &schema, canonical_batches(expected))
                .unwrap()
                .0
        );
    }

    #[test]
    fn vortex_is_handed_a_variant_column_as_its_storage_struct() {
        let storage = DataType::Struct(
            vec![
                Field::new("metadata", DataType::Binary, false),
                Field::new("value", DataType::Binary, true),
            ]
            .into(),
        );
        let marked = Field::new("v", storage, true).with_metadata(
            [
                ("ARROW:extension:name", "arrow.parquet.variant"),
                ("ARROW:extension:metadata", ""),
                (VARIANT_MARKER, "1"),
                ("kept", "yes"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        );
        let schema = Schema::new(vec![marked, Field::new("x", DataType::Int64, true)]);
        let stripped = vortex_storage_schema(&schema);
        let keys: Vec<_> = stripped.field(0).metadata().keys().cloned().collect();
        assert_eq!(keys, vec!["kept".to_string()]);
        assert_eq!(stripped.field(0).data_type(), schema.field(0).data_type());
        assert_eq!(stripped.field(1), schema.field(1));
    }

    /// A one-column canonical whose struct `v` carries `metadata` (as field metadata).
    fn variant_canonical(scratch: &Scratch, metadata: &[(&str, &str)]) -> (SchemaRef, RecordBatch) {
        use arrow_array::{BinaryArray, StructArray};
        let fields: Fields = vec![
            Field::new("metadata", DataType::Binary, true),
            Field::new("value", DataType::Binary, true),
        ]
        .into();
        let v = Field::new("v", DataType::Struct(fields.clone()), true).with_metadata(
            metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        );
        let schema = Arc::new(Schema::new(vec![v]));
        // Variant 1 (int8) twice, and a null row.
        let column = StructArray::new(
            fields,
            vec![
                Arc::new(BinaryArray::from(vec![
                    Some(&[1u8, 0, 0][..]),
                    Some(&[1, 0, 0]),
                    None,
                ])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![
                    Some(&[12u8, 1][..]),
                    Some(&[12, 1]),
                    None,
                ])),
            ],
            Some(vec![true, true, false].into()),
        );
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(column)]).unwrap();
        scratch.canonical("variant.arrow", &schema, std::slice::from_ref(&batch));
        (schema, batch)
    }

    #[test]
    fn a_variant_column_is_written_as_parquet_variant_and_read_back_as_the_extension() {
        let scratch = Scratch::new("parquet-variant");
        let (schema, batch) = variant_canonical(
            &scratch,
            &[(EXTENSION_NAME, VARIANT_EXTENSION), (VARIANT_MARKER, "1")],
        );
        let output = scratch.path("variant.parquet");
        let limits = RowGroupLimits {
            target_encoded_bytes: 1 << 20,
            max_rows: 1 << 20,
        };
        write_parquet(&output, schema.clone(), limits, || {
            Ok(stream(vec![batch.clone()]))
        })
        .unwrap();
        let (got_schema, got) = open_parquet(&output).unwrap();
        let columns = variant_columns(&schema);
        assert_eq!(columns, vec!["v".to_string()]);
        assert_eq!(
            parquet_variant_loss(&columns, &output, &got_schema).unwrap(),
            None
        );
        let (ok, detail) =
            logical_eq_stream(&got_schema, got, &schema, stream(vec![batch])).unwrap();
        assert!(ok, "{detail}");
    }

    #[test]
    fn a_marker_without_the_extension_is_measured_as_not_kept() {
        // arrow-rs annotates only the extension; raincloud's marker alone gives a plain group.
        let scratch = Scratch::new("parquet-variant-marker");
        let (schema, batch) = variant_canonical(&scratch, &[(VARIANT_MARKER, "1")]);
        let output = scratch.path("variant.parquet");
        let limits = RowGroupLimits {
            target_encoded_bytes: 1 << 20,
            max_rows: 1 << 20,
        };
        write_parquet(&output, schema.clone(), limits, || {
            Ok(stream(vec![batch.clone()]))
        })
        .unwrap();
        let (got_schema, _) = open_parquet(&output).unwrap();
        let loss = parquet_variant_loss(&variant_columns(&schema), &output, &got_schema)
            .unwrap()
            .expect("a plain group is not VARIANT");
        assert!(
            loss.contains("declares no Parquet VARIANT logical type"),
            "{loss}"
        );
        assert!(
            loss.contains("read back without the arrow.parquet.variant extension"),
            "{loss}"
        );
    }
}
