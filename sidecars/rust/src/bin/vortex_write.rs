// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! `vortex-write` sidecar — write a `.vortex` file from the canonical via the
//! Vortex Rust core, then self-verify. Implements raincloud's WRITE CLI contract
//! (`raincloud/pipeline/export/sidecar.py`).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use raincloud_sidecars::{
    canonical_batches, has_variant, logical_eq_stream, open_canonical, open_vortex, run_writer,
    write_vortex,
};

#[derive(Parser)]
#[command(about = "Write a Vortex artifact from the canonical Arrow IPC file (Vortex Rust core).")]
struct Args {
    /// Canonical Arrow IPC file (`<slug>.arrow.zstd`).
    #[arg(long)]
    input: PathBuf,
    /// Destination `.vortex` path.
    #[arg(long)]
    output: PathBuf,
    /// JSON report path.
    #[arg(long)]
    report: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    run_writer("vortex@rs", &args.output, &args.report, || {
        write_vortex(&args.output, &args.input)?;

        // Self-verify using the stored dtype, then compare losslessly to the canonical.
        let (schema, canonical) = open_canonical(&args.input)?;
        let (got_schema, got) = open_vortex(&args.output)?;
        let (roundtrip, detail) =
            logical_eq_stream(&got_schema, got, &schema, canonical_batches(canonical))?;
        let variant_faithful = !has_variant(&schema);
        let note = if !roundtrip {
            format!("vortex@rs: self-verify mismatch: {detail}")
        } else if variant_faithful {
            "vortex@rs: round-trips to canonical".to_string()
        } else {
            "vortex@rs: round-trips; VARIANT annotation not preserved (column kept \
             as its shredded struct)"
                .to_string()
        };
        Ok((roundtrip, variant_faithful, note))
    })
}
