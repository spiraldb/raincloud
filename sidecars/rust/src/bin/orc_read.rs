// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! `orc-read` sidecar — read an `.orc` artifact via orc-rust and verdict its
//! round-trip to the canonical. Implements raincloud's READ CLI contract
//! (`raincloud/pipeline/export/readers.py`).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use raincloud_sidecars::{
    canonical_batches, logical_eq_stream, open_canonical, open_orc, run_reader,
};

#[derive(Parser)]
#[command(about = "Read an ORC artifact (orc-rust) and verdict vs the canonical Arrow IPC file.")]
struct Args {
    /// `.orc` artifact to read.
    #[arg(long)]
    input: PathBuf,
    /// Canonical Arrow IPC file (`<slug>.arrow.zstd`).
    #[arg(long)]
    canonical: PathBuf,
    /// JSON report path.
    #[arg(long)]
    report: PathBuf,
}

fn main() -> ExitCode {
    let args = Args::parse();
    run_reader("orc@rs", &args.report, || {
        let (schema, canonical) = open_canonical(&args.canonical)?;
        let (got_schema, got) = open_orc(&args.input)?;
        logical_eq_stream(&got_schema, got, &schema, canonical_batches(canonical))
    })
}
