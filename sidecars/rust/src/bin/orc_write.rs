// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! `orc-write` sidecar — write an `.orc` file from the canonical via orc-rust,
//! then self-verify. Implements raincloud's WRITE CLI contract
//! (`raincloud/pipeline/export/sidecar.py`).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use raincloud_sidecars::{
    canonical_batches, has_variant, logical_eq_stream, open_canonical, open_orc, run_writer,
    write_orc,
};

#[derive(Parser)]
#[command(about = "Write an ORC artifact from the canonical Arrow IPC file (orc-rust).")]
struct Args {
    /// Canonical Arrow IPC file (`<slug>.arrow.zstd`).
    #[arg(long)]
    input: PathBuf,
    /// Destination `.orc` path.
    #[arg(long)]
    output: PathBuf,
    /// JSON report path.
    #[arg(long)]
    report: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    run_writer("orc@rs", &args.output, &args.report, || {
        write_orc(&args.output, &args.input)?;

        let (schema, canonical) = open_canonical(&args.input)?;
        let (got_schema, got) = open_orc(&args.output)?;
        let (roundtrip, detail) =
            logical_eq_stream(&got_schema, got, &schema, canonical_batches(canonical))?;
        let variant_faithful = !has_variant(&schema);
        let note = if !roundtrip {
            format!("orc@rs: self-verify mismatch: {detail}")
        } else if variant_faithful {
            "orc@rs: round-trips to canonical".to_string()
        } else {
            "orc@rs: round-trips; VARIANT annotation not preserved (ORC has no VARIANT type)"
                .to_string()
        };
        Ok((roundtrip, variant_faithful, note))
    })
}
