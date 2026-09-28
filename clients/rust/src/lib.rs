// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! Datasets-style reads for Rust, C and C++: name a dataset, get Arrow batches.
//! Everything before the bytes — catalog, resolution, download, verification —
//! is dispatched to the `raincloud` command line tool.
//!
//! Every open/path/batches/schema call runs one `raincloud` process and blocks
//! until it exits. It inherits this process's environment, working directory
//! (relative settings resolve against it) and stderr, and may wait on the
//! store's download lock with no timeout.
mod cli;
mod error;
mod ffi;
mod reader;
pub use arrow_array::RecordBatchReader;
pub use error::{Error, ErrorKind, Result};
pub use reader::Batches;
use serde_json::Value;
use std::fmt;

/// A dataset selected from the catalog. Opening reads catalog metadata only;
/// the artifact is resolved when you ask for its path or its batches.
#[derive(Clone)]
pub struct Dataset {
    cli: cli::Cli,
    slug: String,
    format: String,
    revision: String,
    metadata: Value,
}

// The CLI half carries settings, which can carry credentials.
impl fmt::Debug for Dataset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dataset")
            .field("slug", &self.slug)
            .field("format", &self.format)
            .field("catalog_revision", &self.revision)
            .finish_non_exhaustive()
    }
}

/// `load("uci-seeds")` with default settings and automatic format.
pub fn load(slug: &str) -> Result<Dataset> {
    Dataset::load(&Value::Null, slug, "auto")
}

/// A string field of a CLI reply. A missing one means the CLI and this reader
/// disagree about the protocol, which must not be papered over with a default.
fn reply_str<'a>(reply: &'a Value, command: &str, key: &str) -> Result<&'a str> {
    reply[key].as_str().ok_or_else(|| {
        Error::new(
            ErrorKind::Internal,
            format!("`raincloud {command}` replied without a string `{key}`; the CLI and this reader are different versions"),
        )
    })
}

impl Dataset {
    /// `format`: auto, arrow, parquet, or vortex.
    pub fn load(options: &Value, slug: &str, format: &str) -> Result<Self> {
        if slug.is_empty() || slug.contains('\0') {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("{slug:?} is not a dataset name"),
            ));
        }
        // Refused here, not by the CLI: argparse would read `-x` as an option
        // and exit 2, and a NUL cannot reach a command line at all, so both
        // would surface as the wrong kind of error.
        if format.is_empty() || format.contains('\0') || format.starts_with('-') {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("{format:?} is not a format; use auto, arrow, parquet or vortex"),
            ));
        }
        let cli = cli::Cli::new(options)?;
        // `--format=VALUE` keeps the value one argument, and `--` keeps a slug
        // that starts with `-` a name, not an option.
        let metadata = cli.run(
            &[],
            &[
                "describe",
                &format!("--format={format}"),
                "--readers",
                cli::READERS,
                "--",
                slug,
            ],
        )?;
        let selected = reply_str(&metadata, "describe", "format")?;
        if selected.is_empty() || selected.contains('@') {
            return Err(Error::new(
                ErrorKind::Internal,
                format!("`raincloud describe` selected format {selected:?}, which is not a format"),
            ));
        }
        reply_str(&metadata, "describe", "catalog_source")?;
        Ok(Self {
            format: selected.to_owned(),
            revision: reply_str(&metadata, "describe", "catalog_revision")?.to_owned(),
            cli,
            slug: slug.to_owned(),
            metadata,
        })
    }
    /// Catalog record: rows, columns, formats, catalog revision, recipe.
    pub fn metadata(&self) -> &Value {
        &self.metadata
    }
    /// The selected representation, e.g. `vortex`.
    pub fn format(&self) -> &str {
        &self.format
    }
    /// Return a location, not a read lease; publishers may replace it.
    ///
    /// Resolves against the catalog generation this handle opened with. A
    /// revision or pack-directory selection names that generation again; a
    /// checkout, local manifest or bundled catalog that has changed since the
    /// open is refused with `Catalog` rather than read.
    pub fn path(&self) -> Result<std::path::PathBuf> {
        let reply = self.cli.run(
            &[("catalog", &self.metadata["catalog_source"])],
            &[
                "load",
                &format!("--format={}", self.format),
                "--readers",
                cli::READERS,
                "--",
                &self.slug,
            ],
        )?;
        let revision = reply_str(&reply, "load", "catalog_revision")?;
        if revision != self.revision {
            return Err(Error::new(
                ErrorKind::Catalog,
                format!(
                    "the catalog changed since {} was opened (revision {} then, {revision} now); open it again",
                    self.slug, self.revision
                ),
            ));
        }
        Ok(reply_str(&reply, "load", "path")?.into())
    }
    /// Refuses a format this build cannot decode before resolving anything,
    /// so no bytes are downloaded only to be refused.
    pub fn batches(&self, batch_size: usize) -> Result<Batches> {
        if batch_size == 0 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "batch_size must be positive",
            ));
        }
        if !cli::READERS.split(',').any(|f| f == self.format) {
            return Err(Error::new(
                ErrorKind::FormatUnavailable,
                format!("reader not compiled for {}", self.format),
            ));
        }
        Batches::open(&self.path()?, &self.format, batch_size)
    }
    /// May resolve/download bytes; does not materialize rows.
    pub fn schema(&self) -> Result<arrow_schema::SchemaRef> {
        Ok(self.batches(65536)?.schema())
    }
}
