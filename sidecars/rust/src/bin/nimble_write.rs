// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! `nimble-write` sidecar — write a Nimble file from the canonical via upstream
//! Nimble's C++ writer (`raincloud-nimble`), then self-verify through its
//! reader. Implements raincloud's WRITE CLI contract
//! (`raincloud/pipeline/export/sidecar.py`).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use raincloud_sidecars::{
    canonical_batches, has_variant, logical_eq_stream, open_canonical, open_nimble, run_writer,
    write_nimble,
};

#[derive(Parser)]
#[command(
    about = "Write a Nimble artifact from the canonical Arrow IPC file (upstream Nimble, via raincloud-nimble)."
)]
struct Args {
    /// Canonical Arrow IPC file (`<slug>.arrow.zstd`).
    #[arg(long)]
    input: PathBuf,
    /// Destination `.nimble` path.
    #[arg(long)]
    output: PathBuf,
    /// JSON report path.
    #[arg(long)]
    report: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    run_writer("nimble@cpp", &args.output, &args.report, || {
        write_nimble(&args.output, &args.input)?;

        let (schema, canonical) = open_canonical(&args.input)?;
        let (got_schema, got) = open_nimble(&args.output)?;
        let (roundtrip, detail) =
            logical_eq_stream(&got_schema, got, &schema, canonical_batches(canonical))?;
        let variant_faithful = !has_variant(&schema);
        let note = if !roundtrip {
            format!("nimble@cpp: self-verify mismatch: {detail}")
        } else if variant_faithful {
            "nimble@cpp: round-trips to canonical".to_string()
        } else {
            "nimble@cpp: round-trips; VARIANT annotation not preserved (Nimble has no VARIANT type)"
                .to_string()
        };
        Ok((roundtrip, variant_faithful, note))
    })
}
