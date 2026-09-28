// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! Catalog selection, resolution, downloads and verification all belong to the
//! `raincloud` command line tool; this crate only runs it and reads the path.
use crate::{Error, ErrorKind, Result};
use serde_json::{Map, Value};
use std::fmt;
use std::process::{Command, Stdio};

/// Settings travel in this environment variable, named by `--settings-env`.
/// A process's environment is readable only by its own user, while its command
/// line (`/proc/*/cmdline`) is readable by every account on the machine, and
/// settings can carry credentials: a mirror URL with a user or a signed query.
const SETTINGS_ENV: &str = "RAINCLOUD_SETTINGS";

/// The formats this build decodes. `auto` chooses among these, not among the
/// readers the CLI's own Python environment happens to have installed.
pub(crate) const READERS: &str = if cfg!(feature = "vortex") {
    "arrow,parquet,vortex"
} else {
    "arrow,parquet"
};

/// How much of a reply that is not JSON to quote in the error.
const EXCERPT_CHARS: usize = 200;

/// How to reach the CLI, and the settings to hand it verbatim.
#[derive(Clone)]
pub(crate) struct Cli {
    exe: String,
    settings: Map<String, Value>,
}

// Settings can carry credentials: name them, never print their values.
impl fmt::Debug for Cli {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Cli")
            .field("exe", &self.exe)
            .field("settings", &self.settings.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Cli {
    /// `options` are Raincloud settings (the TOML keys, plus `config` and
    /// `no_config`), validated by the CLI. The one native-only key, `cli`,
    /// names the executable; otherwise `RAINCLOUD_CLI`, otherwise `PATH`.
    pub(crate) fn new(options: &Value) -> Result<Self> {
        let mut settings = match options {
            Value::Null => Map::new(),
            Value::Object(map) => map.clone(),
            _ => {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "options must be a JSON object",
                ))
            }
        };
        let exe = match settings.remove("cli") {
            Some(Value::String(path)) => path,
            Some(_) => {
                return Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "the `cli` option must be a path",
                ))
            }
            None => std::env::var("RAINCLOUD_CLI").unwrap_or_else(|_| "raincloud".into()),
        };
        Ok(Self { exe, settings })
    }

    /// Run one subcommand with `--json`. Stderr is left attached so warnings
    /// (checksum drift, re-fetches) reach the caller's terminal or log.
    pub(crate) fn run(&self, overrides: &[(&str, &Value)], args: &[&str]) -> Result<Value> {
        let mut settings = self.settings.clone();
        for (key, value) in overrides {
            settings.insert((*key).to_owned(), (*value).clone());
        }
        let output = Command::new(&self.exe)
            .arg("--json")
            .arg("--settings-env")
            .args(args)
            .env(SETTINGS_ENV, Value::Object(settings).to_string())
            .stdin(Stdio::null())
            .stderr(Stdio::inherit())
            .output()
            .map_err(|e| {
                Error::new(
                    ErrorKind::Io,
                    format!(
                        "cannot run `{}` ({e}); install raincloud, or set the `cli` option or RAINCLOUD_CLI",
                        self.exe
                    ),
                )
            })?;
        let command = format!("`{} {}`", self.exe, args.join(" "));
        let reply: Value = match serde_json::from_slice(&output.stdout) {
            Ok(reply) => reply,
            // argparse rejects arguments it does not know with status 2.
            Err(_) if output.status.code() == Some(2) => {
                return Err(Error::new(
                    ErrorKind::Internal,
                    format!(
                        "{command} was rejected; `{}` is probably older than this reader \
                         (needs `raincloud describe`, `--json` and `--settings-env`); see its stderr",
                        self.exe
                    ),
                ))
            }
            Err(_) if !output.status.success() => {
                return Err(Error::new(
                    ErrorKind::Internal,
                    format!("{command} exited with {}; see its stderr", output.status),
                ))
            }
            // Exit 0 with a reply that does not parse: something else wrote to
            // stdout, or the CLI printed a value JSON cannot carry.
            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Internal,
                    format!(
                        "{command} succeeded but its reply is not JSON ({e}): {:?}",
                        excerpt(&output.stdout)
                    ),
                ))
            }
        };
        match reply {
            Value::Object(mut map) if map.contains_key("error") => {
                let error = map.remove("error").unwrap_or_default();
                let class = error["type"].as_str().unwrap_or("unknown error");
                let message = error["message"].as_str().unwrap_or(class);
                let Some(mro) = error["mro"].as_array() else {
                    return Err(Error::new(
                        ErrorKind::Internal,
                        format!(
                            "{command} reported {class} without its class MRO; `{}` is older \
                             than this reader: {message}",
                            self.exe
                        ),
                    ));
                };
                Err(Error::new(
                    ErrorKind::from_python(mro.iter().filter_map(Value::as_str)),
                    message,
                ))
            }
            reply if output.status.success() => Ok(reply),
            _ => Err(Error::new(
                ErrorKind::Internal,
                format!(
                    "{command} exited with {} without reporting an error; see its stderr",
                    output.status
                ),
            )),
        }
    }
}

fn excerpt(stdout: &[u8]) -> String {
    let text = String::from_utf8_lossy(stdout);
    match text.char_indices().nth(EXCERPT_CHARS) {
        Some((end, _)) => format!("{}...", &text[..end]),
        None => text.into_owned(),
    }
}
