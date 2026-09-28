// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! `vortex-read` sidecar — read a `.vortex` artifact via the Vortex Rust core and
//! verdict its round-trip to the canonical. Implements raincloud's READ CLI
//! contract (`raincloud/pipeline/export/readers.py`).

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use raincloud_sidecars::{
    canonical_batches, logical_eq_stream, open_canonical, open_vortex, run_reader,
};

#[derive(Parser)]
#[command(
    about = "Read a Vortex artifact (Vortex Rust core) and verdict vs the canonical Arrow IPC file."
)]
struct Args {
    /// `.vortex` artifact to read.
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
    run_reader("vortex@rs", &args.report, || {
        let (schema, canonical) = open_canonical(&args.canonical)?;
        // The artifact's own schema, until the lossless logical comparison.
        let (got_schema, got) = open_vortex(&args.input)?;
        logical_eq_stream(&got_schema, got, &schema, canonical_batches(canonical))
    })
}
