// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0

//! The `nimble@cpp` lane's sidecar contract, for its C++ binaries.
//!
//! Nimble is C++ only, so the lane's binaries (`sidecars/nimble`) are C++: they
//! link this library and call [`raincloud_nimble_write_main`] or
//! [`raincloud_nimble_read_main`] with two callbacks over upstream Nimble's
//! `VeloxWriter` and `VeloxReader`. Everything else is the Rust lanes' own:
//! the CLI, reading the canonical with arrow-rs, the logical comparison and the
//! report. Batches cross in memory through the Arrow C stream interface, with
//! nothing converted on the way.
//!
//! A callback returns 0, or non-zero with a NUL-terminated message in `error`
//! (at most `error_len` bytes). It takes ownership of a stream it is handed and
//! fills a stream it is given with one the caller then owns.

use std::ffi::{c_char, c_int, CStr, OsString};
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{bail, Context, Result};
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::RecordBatchReader;
use clap::Parser;
use raincloud_sidecars::{
    canonical_batches, has_variant, logical_eq_stream, open_canonical, run_reader, run_writer,
};

/// Write the batches of `stream` to the Nimble file at `output`.
pub type WriteFn = unsafe extern "C" fn(
    stream: *mut FFI_ArrowArrayStream,
    output: *const c_char,
    error: *mut c_char,
    error_len: usize,
) -> c_int;

/// Fill `out` with a stream of the Nimble file at `input`'s batches.
pub type ReadFn = unsafe extern "C" fn(
    input: *const c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut c_char,
    error_len: usize,
) -> c_int;

const CELL: &str = "nimble@cpp";

#[derive(Parser)]
#[command(about = "Write a Nimble artifact from the canonical Arrow IPC file (upstream Nimble).")]
struct WriteArgs {
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

#[derive(Parser)]
#[command(
    about = "Read a Nimble artifact (upstream Nimble) and verdict vs the canonical Arrow IPC file."
)]
struct ReadArgs {
    /// `.nimble` artifact to read.
    #[arg(long)]
    input: PathBuf,
    /// Canonical Arrow IPC file (`<slug>.arrow.zstd`).
    #[arg(long)]
    canonical: PathBuf,
    /// JSON report path.
    #[arg(long)]
    report: PathBuf,
}

/// `argc`/`argv` as clap reads them.
unsafe fn arguments(argc: c_int, argv: *const *const c_char) -> Vec<OsString> {
    (0..argc.max(0) as usize)
        .map(|i| OsString::from_vec(CStr::from_ptr(*argv.add(i)).to_bytes().to_vec()))
        .collect()
}

fn c_path(path: &Path) -> Result<std::ffi::CString> {
    use std::os::unix::ffi::OsStrExt;
    std::ffi::CString::new(path.as_os_str().as_bytes()).context("a path with a NUL byte")
}

/// Run a callback, turning its non-zero status into an error carrying its message.
fn call(what: &str, f: impl FnOnce(*mut c_char, usize) -> c_int) -> Result<()> {
    let mut error = vec![0 as c_char; 4096];
    if f(error.as_mut_ptr(), error.len()) == 0 {
        return Ok(());
    }
    let last = error.len() - 1;
    error[last] = 0;
    let message = unsafe { CStr::from_ptr(error.as_ptr()) }
        .to_string_lossy()
        .into_owned();
    bail!(
        "{what}: {}",
        if message.is_empty() {
            "failed"
        } else {
            &message
        }
    )
}

/// The Nimble file at `path`, read back by upstream Nimble through `read`.
fn open_nimble(read: ReadFn, path: &Path) -> Result<ArrowArrayStreamReader> {
    let input = c_path(path)?;
    let mut stream = FFI_ArrowArrayStream::empty();
    call("upstream Nimble read", |error, len| unsafe {
        read(input.as_ptr(), &mut stream, error, len)
    })?;
    ArrowArrayStreamReader::try_new(stream).context("the stream Nimble read back")
}

fn compare(read: ReadFn, nimble: &Path, canonical: &Path) -> Result<(bool, String)> {
    let (schema, expected) = open_canonical(canonical)?;
    let got = open_nimble(read, nimble)?;
    let got_schema = got.schema();
    logical_eq_stream(
        &got_schema,
        got.map(|b| b.context("read a batch Nimble read back")),
        &schema,
        canonical_batches(expected),
    )
}

/// The `raincloud-export-nimble-cpp` binary: write the canonical with `write`,
/// then self-verify through `read` (raincloud's WRITE CLI contract).
///
/// # Safety
/// `argv` holds `argc` NUL-terminated strings; the callbacks keep the contract above.
#[no_mangle]
pub unsafe extern "C" fn raincloud_nimble_write_main(
    argc: c_int,
    argv: *const *const c_char,
    write: WriteFn,
    read: ReadFn,
) -> c_int {
    let args = match WriteArgs::try_parse_from(arguments(argc, argv)) {
        Ok(args) => args,
        Err(e) => {
            let _ = e.print();
            return 2;
        }
    };
    exit(run_writer(CELL, &args.output, &args.report, || {
        let (schema, reader) = open_canonical(&args.input)?;
        let variant_faithful = !has_variant(&schema);
        let output = c_path(&args.output)?;
        let batches: Box<dyn RecordBatchReader + Send> = Box::new(reader);
        let mut stream = FFI_ArrowArrayStream::new(batches);
        call("upstream Nimble write", |error, len| unsafe {
            write(&mut stream, output.as_ptr(), error, len)
        })?;
        let (roundtrip, detail) = compare(read, &args.output, &args.input)?;
        let note = if !roundtrip {
            format!("{CELL}: self-verify mismatch: {detail}")
        } else if variant_faithful {
            format!("{CELL}: round-trips to canonical")
        } else {
            format!("{CELL}: round-trips; VARIANT annotation not preserved (Nimble has no VARIANT type)")
        };
        Ok((roundtrip, variant_faithful, note))
    }))
}

/// The `raincloud-read-nimble-cpp` binary: read a Nimble file with `read` and
/// verdict it against the canonical (raincloud's READ CLI contract).
///
/// # Safety
/// As for [`raincloud_nimble_write_main`].
#[no_mangle]
pub unsafe extern "C" fn raincloud_nimble_read_main(
    argc: c_int,
    argv: *const *const c_char,
    read: ReadFn,
) -> c_int {
    let args = match ReadArgs::try_parse_from(arguments(argc, argv)) {
        Ok(args) => args,
        Err(e) => {
            let _ = e.print();
            return 2;
        }
    };
    exit(run_reader(CELL, &args.report, || {
        compare(read, &args.input, &args.canonical)
    }))
}

fn exit(code: ExitCode) -> c_int {
    if code == ExitCode::SUCCESS {
        0
    } else {
        1
    }
}
