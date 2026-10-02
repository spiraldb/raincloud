// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! `avro-write` sidecar — write an Avro object container file from the canonical via arrow-avro,
//! then self-verify. Implements raincloud's WRITE CLI contract
//! (`raincloud/pipeline/export/sidecar.py`).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use raincloud_sidecars::{
    canonical_batches, has_variant, logical_eq_stream, open_avro, open_canonical, run_writer,
    write_avro,
};

#[derive(Parser)]
#[command(about = "Write an Avro artifact from the canonical Arrow IPC file (arrow-avro).")]
struct Args {
    /// Canonical Arrow IPC file (`<slug>.arrow.zstd`).
    #[arg(long)]
    input: PathBuf,
    /// Destination `.avro` path.
    #[arg(long)]
    output: PathBuf,
    /// JSON report path.
    #[arg(long)]
    report: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    run_writer("avro@rs", &args.output, &args.report, || {
        write_avro(&args.output, &args.input)?;

        let (schema, canonical) = open_canonical(&args.input)?;
        let (got_schema, got) = open_avro(&args.output)?;
        let (roundtrip, detail) =
            logical_eq_stream(&got_schema, got, &schema, canonical_batches(canonical))?;
        let variant_faithful = !has_variant(&schema);
        let note = if !roundtrip {
            format!("avro@rs: self-verify mismatch: {detail}")
        } else if variant_faithful {
            "avro@rs: round-trips to canonical".to_string()
        } else {
            "avro@rs: round-trips; VARIANT annotation not preserved (Avro has no VARIANT type)"
                .to_string()
        };
        Ok((roundtrip, variant_faithful, note))
    })
}
