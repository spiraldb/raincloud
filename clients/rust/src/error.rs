// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
use std::fmt;

/// Stable categories shared with the C and Java APIs. Messages are diagnostic.
///
/// The numbers are ABI: codes are only ever appended, and a C or Java caller
/// treats a code it does not know as `Internal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ErrorKind {
    InvalidArgument = 1,
    Catalog = 2,
    MissingRevision = 3,
    UnknownSlug = 4,
    FormatUnavailable = 5,
    OfflineMiss = 6,
    ArtifactNotFound = 7,
    ChecksumMismatch = 8,
    /// Producer-side (build/publish); a read does not raise it.
    CatalogConflict = 9,
    /// A mirror could not be read: unreachable, refused, or not a store. A
    /// mirror that answers "no such file" is `ArtifactNotFound`.
    Transport = 10,
    /// The bytes are fine and this build cannot represent their type; ask for
    /// another format.
    UnsupportedType = 11,
    Io = 12,
    Internal = 13,
    /// The artifact was found but could not be decoded: truncated, damaged, or
    /// not the format its path claims. Re-fetch or rebuild it; another format
    /// will not help.
    CorruptArtifact = 14,
}
#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}
pub type Result<T> = std::result::Result<T, Error>;
impl ErrorKind {
    /// Every kind, in code order.
    #[cfg(test)]
    const ALL: [Self; 14] = [
        Self::InvalidArgument,
        Self::Catalog,
        Self::MissingRevision,
        Self::UnknownSlug,
        Self::FormatUnavailable,
        Self::OfflineMiss,
        Self::ArtifactNotFound,
        Self::ChecksumMismatch,
        Self::CatalogConflict,
        Self::Transport,
        Self::UnsupportedType,
        Self::Io,
        Self::Internal,
        Self::CorruptArtifact,
    ];
    /// The CLI reports a failure by its Python exception class's MRO. The
    /// first class in it with a code decides, so a subclass keeps its parent's
    /// category unless it has its own (`MissingRevision` is not
    /// `CatalogError`; `FileNotFoundError` is `OSError`).
    pub(crate) fn from_python<'a>(mro: impl IntoIterator<Item = &'a str>) -> Self {
        mro.into_iter()
            .find_map(Self::from_python_class)
            .unwrap_or(Self::Internal)
    }
    fn from_python_class(name: &str) -> Option<Self> {
        Some(match name {
            "ValueError" => Self::InvalidArgument,
            "CatalogError" => Self::Catalog,
            "MissingRevision" => Self::MissingRevision,
            "UnknownSlug" => Self::UnknownSlug,
            "FormatUnavailable" | "MissingDependency" => Self::FormatUnavailable,
            "OfflineMiss" => Self::OfflineMiss,
            "ArtifactNotFound" => Self::ArtifactNotFound,
            "ChecksumMismatch" => Self::ChecksumMismatch,
            "CatalogConflict" => Self::CatalogConflict,
            "MirrorUnavailable" => Self::Transport,
            "UnsupportedType" => Self::UnsupportedType,
            "CorruptArtifact" => Self::CorruptArtifact,
            "OSError" => Self::Io,
            _ => return None,
        })
    }
}
impl Error {
    pub fn new(kind: ErrorKind, message: impl ToString) -> Self {
        Self {
            kind,
            message: message.to_string(),
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}
impl std::error::Error for Error {}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::new(ErrorKind::Io, e)
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::new(ErrorKind::InvalidArgument, e)
    }
}
// Only the "not implemented" variants mean the type is beyond this build, and
// only an I/O failure is `Io`; the rest describe damaged or unreadable bytes. A
// read that ends early is the file being shorter than its own structure says:
// damage, not an I/O failure.
pub(crate) fn io_kind(io: &std::io::Error) -> ErrorKind {
    if io.kind() == std::io::ErrorKind::UnexpectedEof {
        ErrorKind::CorruptArtifact
    } else {
        ErrorKind::Io
    }
}
pub(crate) fn arrow_kind(e: &arrow_schema::ArrowError) -> ErrorKind {
    match e {
        arrow_schema::ArrowError::NotYetImplemented(_) => ErrorKind::UnsupportedType,
        arrow_schema::ArrowError::IoError(_, io) => io_kind(io),
        _ => ErrorKind::CorruptArtifact,
    }
}
impl From<arrow_schema::ArrowError> for Error {
    fn from(e: arrow_schema::ArrowError) -> Self {
        Self::new(arrow_kind(&e), e)
    }
}
impl From<parquet::errors::ParquetError> for Error {
    fn from(e: parquet::errors::ParquetError) -> Self {
        use parquet::errors::ParquetError as P;
        let kind = match &e {
            P::NYI(_) => ErrorKind::UnsupportedType,
            P::External(source) => match source.downcast_ref::<std::io::Error>() {
                Some(io) => io_kind(io),
                None => ErrorKind::CorruptArtifact,
            },
            _ => ErrorKind::CorruptArtifact,
        };
        Self::new(kind, e)
    }
}

#[cfg(test)]
mod tests {
    use super::{Error, ErrorKind};
    use std::io;

    #[test]
    fn only_an_io_failure_is_io() {
        use arrow_schema::ArrowError;
        use parquet::errors::ParquetError;
        for (io, kind) in [
            (io::ErrorKind::PermissionDenied, ErrorKind::Io),
            (io::ErrorKind::Other, ErrorKind::Io),
            (io::ErrorKind::UnexpectedEof, ErrorKind::CorruptArtifact),
        ] {
            let arrow = Error::from(ArrowError::IoError("read".into(), io::Error::from(io)));
            assert_eq!(arrow.kind, kind, "arrow {io:?}");
            let parquet = Error::from(ParquetError::External(Box::new(io::Error::from(io))));
            assert_eq!(parquet.kind, kind, "parquet {io:?}");
        }
        let external = ParquetError::External(Box::new(ArrowError::ComputeError("x".into())));
        assert_eq!(Error::from(external).kind, ErrorKind::CorruptArtifact);
        assert_eq!(
            Error::from(ParquetError::General("bad page".into())).kind,
            ErrorKind::CorruptArtifact
        );
        assert_eq!(
            Error::from(ParquetError::NYI("INTERVAL".into())).kind,
            ErrorKind::UnsupportedType
        );
    }

    #[test]
    fn codes_are_dense_and_ordered() {
        for (i, kind) in ErrorKind::ALL.iter().enumerate() {
            assert_eq!(*kind as i32, i as i32 + 1);
        }
    }

    #[test]
    fn first_known_class_in_the_mro_decides() {
        let cases: &[(&[&str], ErrorKind)] = &[
            (
                &["UnknownSlug", "RaincloudError", "Exception"],
                ErrorKind::UnknownSlug,
            ),
            (
                &["MissingRevision", "CatalogError", "RaincloudError"],
                ErrorKind::MissingRevision,
            ),
            (
                &["FileNotFoundError", "OSError", "Exception"],
                ErrorKind::Io,
            ),
            (
                &["JSONDecodeError", "ValueError", "Exception"],
                ErrorKind::InvalidArgument,
            ),
            (
                &["BuildFailed", "RaincloudError", "Exception"],
                ErrorKind::Internal,
            ),
            (
                &["KeyError", "LookupError", "Exception"],
                ErrorKind::Internal,
            ),
            (&[], ErrorKind::Internal),
        ];
        for (mro, kind) in cases {
            assert_eq!(
                ErrorKind::from_python(mro.iter().copied()),
                *kind,
                "{mro:?}"
            );
        }
    }
}
