// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! `parquet-write` sidecar — write Parquet from the canonical via arrow-rs, then
//! self-verify. Implements raincloud's WRITE CLI contract
//! (`raincloud/pipeline/export/sidecar.py`).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use raincloud_sidecars::{
    canonical_batches, logical_eq_stream, open_canonical, open_parquet, parquet_variant_loss,
    run_writer, variant_columns, write_parquet_with, ParquetOptions, RowGroupLimits,
};

#[derive(Parser)]
#[command(about = "Write a Parquet artifact from the canonical Arrow IPC file (arrow-rs).")]
struct Args {
    /// Canonical Arrow IPC file (`<slug>.arrow.zstd`).
    #[arg(long)]
    input: PathBuf,
    /// Destination Parquet path.
    #[arg(long)]
    output: PathBuf,
    /// JSON report path.
    #[arg(long)]
    report: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    run_writer("parquet@rs", &args.output, &args.report, || {
        // Knobs first, so a malformed one fails before this run writes any
        // output. (Any error removes `--output`, whoever wrote it.)
        let limits = RowGroupLimits::from_env()?;
        let options = ParquetOptions::from_env()?;
        // Streamed end to end: the canonical is re-read rather than held, since a
        // large one (SF100 lineitem, ~100 GB decoded) does not fit in memory.
        let (schema, _) = open_canonical(&args.input)?;
        write_parquet_with(&args.output, schema.clone(), limits, options, || {
            Ok(canonical_batches(open_canonical(&args.input)?.1))
        })?;

        // Self-verify: re-read what we wrote and compare logically to the canonical.
        let (_, canonical) = open_canonical(&args.input)?;
        let (got_schema, got) = open_parquet(&args.output)?;
        let (roundtrip, detail) =
            logical_eq_stream(&got_schema, got, &schema, canonical_batches(canonical))?;
        // Measured, not assumed: the file's logical type and the read-back's extension.
        let variants = variant_columns(&schema);
        let loss = parquet_variant_loss(&variants, &args.output, &got_schema)?;
        let note = match (roundtrip, &loss) {
            (false, None) => format!("parquet@rs: self-verify mismatch: {detail}"),
            (false, Some(loss)) => {
                format!("parquet@rs: self-verify mismatch: {detail}; VARIANT not kept: {loss}")
            }
            (true, Some(loss)) => format!("parquet@rs: round-trips; VARIANT not kept: {loss}"),
            (true, None) if variants.is_empty() => {
                "parquet@rs: round-trips to canonical".to_string()
            }
            (true, None) => format!(
                "parquet@rs: round-trips; VARIANT kept ({})",
                variants.join(", ")
            ),
        };
        Ok((roundtrip, loss.is_none(), note))
    })
}
