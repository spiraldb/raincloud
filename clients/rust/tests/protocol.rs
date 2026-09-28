// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
//! The reader's side of its protocol with the `raincloud` CLI, against a
//! scripted stand-in: no Python, catalog or fixture needed.
#![cfg(unix)]
use raincloud_reader::{Dataset, ErrorKind};
use serde_json::{json, Value};
use std::{
    fs, io,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
};

const SECRET: &str = "s3cr3t-token";

/// One test at a time: a script being written while another test's thread
/// forks is still open in that child until it execs, and running the script
/// then fails with ETXTBSY ("text file busy").
fn serial() -> MutexGuard<'static, ()> {
    static SERIAL: Mutex<()> = Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A scripted CLI in a directory of its own, removed when this is dropped.
struct FakeCli {
    script: PathBuf,
}

impl FakeCli {
    fn path(&self) -> &Path {
        &self.script
    }
    fn dir(&self) -> &Path {
        self.script.parent().unwrap()
    }
    /// Every argument of every run, one per line.
    fn argv(&self) -> String {
        fs::read_to_string(self.dir().join("argv")).unwrap()
    }
}

impl Drop for FakeCli {
    fn drop(&mut self) {
        if let Err(e) = fs::remove_dir_all(self.dir()) {
            eprintln!("cannot remove {}: {e}", self.dir().display());
        }
    }
}

/// A CLI that records its argv and settings, then prints `<command>.out`
/// and exits with `<command>.status` (default 0).
fn fake_cli(name: &str, describe: &str, load: &str) -> FakeCli {
    let dir =
        std::env::temp_dir().join(format!("raincloud-protocol-{}-{name}", std::process::id()));
    // A fresh directory, so no earlier run's argv or settings can be read back.
    match fs::remove_dir_all(&dir) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => panic!("{}: {e}", dir.display()),
        _ => {}
    }
    fs::create_dir_all(&dir).unwrap();
    let script = dir.join("raincloud");
    fs::write(
        &script,
        r#"#!/bin/sh
dir=$(dirname "$0")
printf '%s\n' "$@" >> "$dir/argv"
printf '%s\n' "$RAINCLOUD_SETTINGS" >> "$dir/settings"
for a in "$@"; do case "$a" in describe|load) cmd=$a; break;; esac; done
cat "$dir/$cmd.out"
exit "$(cat "$dir/$cmd.status" 2>/dev/null || echo 0)"
"#,
    )
    .unwrap();
    fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
    fs::write(dir.join("describe.out"), describe).unwrap();
    fs::write(dir.join("load.out"), load).unwrap();
    FakeCli { script }
}

fn describe_reply() -> Value {
    json!({"slug": "tiny", "format": "parquet", "catalog_source": "checkout",
           "catalog_revision": "a".repeat(64), "rows": 8})
}

fn load_reply(revision: &str) -> String {
    json!({"slug": "tiny", "format": "parquet", "path": "/data/tiny.parquet",
           "catalog_revision": revision})
    .to_string()
}

fn options(cli: &FakeCli) -> Value {
    json!({"cli": cli.path(), "mirror": format!("https://reader:{SECRET}@mirror.example/v2?sig={SECRET}")})
}

#[test]
fn settings_travel_in_the_environment_never_on_the_command_line() {
    let _serial = serial();
    let cli = fake_cli(
        "settings",
        &describe_reply().to_string(),
        &load_reply(&"a".repeat(64)),
    );
    let ds = Dataset::load(&options(&cli), "-starts-with-dash", "auto").unwrap();
    assert_eq!(ds.path().unwrap(), PathBuf::from("/data/tiny.parquet"));
    let dir = cli.dir();
    let argv = cli.argv();
    assert!(!argv.contains(SECRET), "settings leaked onto argv:\n{argv}");
    assert!(argv.lines().any(|a| a == "--settings-env"), "{argv}");
    // One argument each: a format can never be read as an option.
    assert!(argv.lines().any(|a| a == "--format=auto"), "{argv}");
    assert!(argv.lines().any(|a| a == "--format=parquet"), "{argv}");
    // `--` keeps a slug that starts with `-` a name, and --readers names this build's decoders.
    assert!(argv.contains("--\n-starts-with-dash\n"), "{argv}");
    let readers = if cfg!(feature = "vortex") {
        "arrow,parquet,vortex"
    } else {
        "arrow,parquet"
    };
    assert!(argv.contains(&format!("--readers\n{readers}\n")), "{argv}");
    let settings: Vec<Value> = fs::read_to_string(dir.join("settings"))
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert!(settings[0]["mirror"].as_str().unwrap().contains(SECRET));
    assert!(settings[0].get("cli").is_none());
    // path() pins the catalog the handle opened with.
    assert_eq!(settings[1]["catalog"], "checkout");
    let shown = format!("{ds:?}");
    assert!(!shown.contains(SECRET), "{shown}");
}

#[test]
fn a_changed_catalog_is_refused_not_read() {
    let _serial = serial();
    let cli = fake_cli(
        "revision",
        &describe_reply().to_string(),
        &load_reply(&"b".repeat(64)),
    );
    let ds = Dataset::load(&options(&cli), "tiny", "auto").unwrap();
    let error = ds.path().unwrap_err();
    assert_eq!(error.kind, ErrorKind::Catalog, "{error}");
    assert!(error.message.contains("open it again"), "{error}");
}

#[test]
fn a_reply_missing_protocol_fields_is_a_version_mismatch() {
    let _serial = serial();
    let mut cases = Vec::new();
    for key in ["format", "catalog_source", "catalog_revision"] {
        let mut reply = describe_reply();
        reply.as_object_mut().unwrap().remove(key);
        cases.push(reply);
    }
    for format in ["", "parquet@rs"] {
        let mut reply = describe_reply();
        reply["format"] = json!(format);
        cases.push(reply);
    }
    for (i, reply) in cases.iter().enumerate() {
        let cli = fake_cli(&format!("fields{i}"), &reply.to_string(), "{}");
        let error = Dataset::load(&options(&cli), "tiny", "auto").unwrap_err();
        assert_eq!(error.kind, ErrorKind::Internal, "{reply}: {error}");
    }
    let cli = fake_cli(
        "nopath",
        &describe_reply().to_string(),
        &json!({"catalog_revision": "a".repeat(64)}).to_string(),
    );
    let error = Dataset::load(&options(&cli), "tiny", "auto")
        .unwrap()
        .path()
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal, "{error}");
}

#[test]
fn stdout_that_is_not_json_is_reported_with_an_excerpt() {
    let _serial = serial();
    let cli = fake_cli("nonjson", "stray print\n{\"format\": NaN}", "{}");
    let error = Dataset::load(&options(&cli), "tiny", "auto").unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(
        error.message.contains("not JSON") && error.message.contains("stray print"),
        "{error}"
    );
}

#[test]
fn errors_map_by_class_mro() {
    let _serial = serial();
    let reply = json!({"error": {"type": "UnknownSlug", "mro": ["UnknownSlug", "RaincloudError", "Exception"],
                                 "message": "no dataset tiny"}});
    let cli = fake_cli("mro", &reply.to_string(), "{}");
    fs::write(cli.dir().join("describe.status"), "1").unwrap();
    let error = Dataset::load(&options(&cli), "tiny", "auto").unwrap_err();
    assert_eq!(error.kind, ErrorKind::UnknownSlug);
    assert_eq!(error.message, "no dataset tiny");

    let reply = json!({"error": {"type": "UnknownSlug", "message": "no dataset tiny"}});
    let cli = fake_cli("nomro", &reply.to_string(), "{}");
    fs::write(cli.dir().join("describe.status"), "1").unwrap();
    let error = Dataset::load(&options(&cli), "tiny", "auto").unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("older"), "{error}");

    let cli = fake_cli("usage", "", "{}");
    fs::write(cli.dir().join("describe.status"), "2").unwrap();
    let error = Dataset::load(&options(&cli), "tiny", "auto").unwrap_err();
    assert_eq!(error.kind, ErrorKind::Internal);
    assert!(error.message.contains("older"), "{error}");
}

#[test]
fn a_name_that_cannot_be_a_slug_is_refused_before_running_anything() {
    let _serial = serial();
    let never = json!({"cli": "/nonexistent/raincloud"});
    for slug in ["", "ti\0ny"] {
        assert_eq!(
            Dataset::load(&never, slug, "auto").unwrap_err().kind,
            ErrorKind::InvalidArgument
        );
    }
}

#[test]
fn a_format_that_cannot_be_one_is_refused_before_running_anything() {
    let _serial = serial();
    let never = json!({"cli": "/nonexistent/raincloud"});
    for format in ["", "-x", "--readers", "par\0quet"] {
        let error = Dataset::load(&never, "tiny", format).unwrap_err();
        assert_eq!(
            error.kind,
            ErrorKind::InvalidArgument,
            "{format:?}: {error}"
        );
    }
}

#[test]
fn batches_refuse_an_undecodable_format_before_resolving_it() {
    let _serial = serial();
    let mut reply = describe_reply();
    reply["format"] = json!("orc");
    let cli = fake_cli(
        "undecodable",
        &reply.to_string(),
        &load_reply(&"a".repeat(64)),
    );
    let ds = Dataset::load(&options(&cli), "tiny", "orc").unwrap();
    let Err(error) = ds.batches(8) else {
        panic!("batches decoded orc")
    };
    assert_eq!(error.kind, ErrorKind::FormatUnavailable, "{error}");
    // Nothing was resolved, so nothing was downloaded.
    assert!(!cli.argv().lines().any(|a| a == "load"), "{}", cli.argv());
    // The path itself still resolves.
    assert_eq!(ds.path().unwrap(), PathBuf::from("/data/tiny.parquet"));
}
