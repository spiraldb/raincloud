// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
use crate::{Error, ErrorKind, Result};
use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, SchemaRef};
use std::{
    fs::File,
    panic::{catch_unwind, AssertUnwindSafe},
    path::Path,
};

/// Iterator owns its source and releases it on drop; yielded batches own buffers.
/// `batch_size` bounds returned rows, not the size of an encoded IPC/Vortex chunk.
///
/// A decoder that panics yields an error for that batch and ends the stream;
/// the panic never unwinds out of `next`. It is `Internal`, not
/// `CorruptArtifact`: a panic on damaged bytes cannot be told apart from a
/// decoder bug. This iterator is also the producer behind the C stream, whose
/// callbacks must not unwind into a C, C++ or JVM frame.
pub struct Batches {
    inner: Box<dyn RecordBatchReader + Send>,
    schema: SchemaRef,
    pending: Option<RecordBatch>,
    batch_size: usize,
    panicked: bool,
}
impl Iterator for Batches {
    type Item = std::result::Result<RecordBatch, ArrowError>;
    fn next(&mut self) -> Option<Self::Item> {
        let batch = match self.pending.take() {
            Some(b) => b,
            None if self.panicked => return None,
            None => match catch_unwind(AssertUnwindSafe(|| self.inner.next())) {
                Ok(None) => return None,
                Ok(Some(Ok(b))) => b,
                Ok(Some(Err(e))) => return Some(Err(e)),
                Err(panic) => {
                    // The decoder's state is unknown after a panic: stop here.
                    self.panicked = true;
                    let what = panic
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "no message".into());
                    return Some(Err(ArrowError::ExternalError(Box::new(Error::new(
                        ErrorKind::Internal,
                        format!("the decoder panicked (damaged bytes or a decoder bug): {what}"),
                    )))));
                }
            },
        };
        if batch.num_rows() > self.batch_size {
            self.pending = Some(batch.slice(self.batch_size, batch.num_rows() - self.batch_size));
            Some(Ok(batch.slice(0, self.batch_size)))
        } else {
            Some(Ok(batch))
        }
    }
}
impl RecordBatchReader for Batches {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
impl Batches {
    pub(crate) fn open(path: &Path, format: &str, batch_size: usize) -> Result<Self> {
        if batch_size == 0 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "batch_size must be positive",
            ));
        }
        let inner: Box<dyn RecordBatchReader + Send> = match format {
            "arrow" => Box::new(arrow_ipc::reader::FileReader::try_new(
                File::open(path)?,
                None,
            )?),
            "parquet" => Box::new(
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                    File::open(path)?,
                )?
                .with_batch_size(batch_size)
                .build()?,
            ),
            #[cfg(feature = "vortex")]
            "vortex" => vortex_reader(path)?,
            _ => {
                return Err(Error::new(
                    ErrorKind::FormatUnavailable,
                    format!("reader not compiled for {format}"),
                ))
            }
        };
        Ok(Self::from_reader(inner, batch_size))
    }
    fn from_reader(inner: Box<dyn RecordBatchReader + Send>, batch_size: usize) -> Self {
        let schema = inner.schema();
        Self {
            inner,
            schema,
            pending: None,
            batch_size,
            panicked: false,
        }
    }
}
/// Classify a Vortex failure the way the IPC and Parquet conversions do
/// (`error::io_kind`, `error::arrow_kind`): an I/O failure is `Io`, a type
/// this build cannot express is `UnsupportedType`, and anything else means the
/// bytes are not a readable Vortex file.
#[cfg(feature = "vortex")]
fn vortex_error(e: vortex::error::VortexError) -> Error {
    use crate::error::{arrow_kind, io_kind};
    use vortex::error::VortexError as V;
    let mut cause = &e;
    loop {
        match cause {
            V::Context(_, inner) => cause = inner,
            V::Shared(inner) => cause = inner,
            _ => break,
        }
    }
    let kind = match cause {
        V::Io(io, _) => io_kind(io),
        V::Arrow(arrow, _) => arrow_kind(arrow),
        V::External(source, _) => source
            .downcast_ref::<std::io::Error>()
            .map_or(ErrorKind::CorruptArtifact, io_kind),
        V::NotImplemented(..) => ErrorKind::UnsupportedType,
        _ => ErrorKind::CorruptArtifact,
    };
    Error::new(kind, e)
}
#[cfg(feature = "vortex")]
fn vortex_reader(path: &Path) -> Result<Box<dyn RecordBatchReader + Send>> {
    use vortex::{
        arrow::ArrowSessionExt,
        file::OpenOptionsSessionExt,
        io::{
            runtime::{tokio::TokioRuntime, BlockingRuntime},
            session::RuntimeSessionExt,
        },
        session::VortexSession,
        VortexSessionDefault,
    };
    // A dedicated runtime lets synchronous native clients own their reader;
    // no process-global runtime or caller thread-local runtime is required.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let blocking = TokioRuntime::from(runtime.handle());
    let session = VortexSession::default().with_handle(blocking.handle());
    let file = blocking
        .block_on(session.open_options().open_path(path))
        .map_err(vortex_error)?;
    // The file decoded; only its dtype has no Arrow form in this build.
    let schema = std::sync::Arc::new(
        session
            .arrow()
            .to_arrow_schema(file.dtype())
            .map_err(|e| Error::new(ErrorKind::UnsupportedType, e))?,
    );
    let reader = file
        .scan()
        .and_then(|s| s.into_record_batch_reader(schema, &blocking))
        .map_err(vortex_error)?;
    struct Owned {
        reader: Box<dyn RecordBatchReader + Send>,
        _runtime: tokio::runtime::Runtime,
    }
    impl Iterator for Owned {
        type Item = std::result::Result<RecordBatch, ArrowError>;
        fn next(&mut self) -> Option<Self::Item> {
            self.reader.next()
        }
    }
    impl RecordBatchReader for Owned {
        fn schema(&self) -> SchemaRef {
            self.reader.schema()
        }
    }
    Ok(Box::new(Owned {
        reader: Box::new(reader),
        _runtime: runtime,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    struct Panics(SchemaRef);
    impl Iterator for Panics {
        type Item = std::result::Result<RecordBatch, ArrowError>;
        fn next(&mut self) -> Option<Self::Item> {
            panic!("decoder exploded")
        }
    }
    impl RecordBatchReader for Panics {
        fn schema(&self) -> SchemaRef {
            self.0.clone()
        }
    }

    #[test]
    fn a_decoder_panic_is_a_stream_error_not_an_unwind() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, true)]));
        let batches = Batches::from_reader(Box::new(Panics(schema)), 4);
        // Through the C stream callbacks, as a C, C++ or Java consumer reads it.
        let stream = FFI_ArrowArrayStream::new(Box::new(batches));
        let mut reader = ArrowArrayStreamReader::try_new(stream).unwrap();
        let error = reader.next().unwrap().unwrap_err().to_string();
        assert!(error.contains("decoder exploded"), "{error}");
        assert!(reader.next().is_none());
    }

    #[test]
    fn undecodable_bytes_are_corrupt_not_unsupported() {
        let dir = std::env::temp_dir().join(format!("raincloud-reader-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut formats = vec!["arrow", "parquet"];
        if cfg!(feature = "vortex") {
            formats.push("vortex");
        }
        for format in formats {
            let path = dir.join(format!("zeros.{format}"));
            std::fs::write(&path, [0u8; 4096]).unwrap();
            let kind = Batches::open(&path, format, 8).err().map(|e| e.kind);
            assert_eq!(kind, Some(ErrorKind::CorruptArtifact), "{format}");
            let missing = dir.join(format!("missing.{format}"));
            let kind = Batches::open(&missing, format, 8).err().map(|e| e.kind);
            assert_eq!(kind, Some(ErrorKind::Io), "{format}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[cfg(feature = "vortex")]
    #[test]
    fn vortex_io_failures_are_io_however_wrapped() {
        use std::io;
        use vortex::error::VortexError as V;
        let denied = || io::Error::from(io::ErrorKind::PermissionDenied);
        let eof = || io::Error::from(io::ErrorKind::UnexpectedEof);
        let cases = [
            (V::from(denied()), ErrorKind::Io),
            (V::from(denied()).with_context("open"), ErrorKind::Io),
            (
                V::from(std::sync::Arc::new(V::from(denied()).with_context("scan"))),
                ErrorKind::Io,
            ),
            (
                V::from(ArrowError::IoError("read".into(), denied())),
                ErrorKind::Io,
            ),
            (V::from(eof()), ErrorKind::CorruptArtifact),
            (
                V::from(eof()).with_context("footer"),
                ErrorKind::CorruptArtifact,
            ),
        ];
        for (error, kind) in cases {
            let shown = error.to_string();
            assert_eq!(vortex_error(error).kind, kind, "{shown}");
        }
    }
}
