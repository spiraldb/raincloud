// SPDX-FileCopyrightText: 2026 Raincloud Maintainers
// SPDX-License-Identifier: Apache-2.0
use raincloud_reader::{Dataset, ErrorKind, RecordBatchReader};
use serde_json::{json, Value};

// Needs the shared fixture and a `raincloud` CLI (RAINCLOUD_CLI or PATH).
#[test]
fn batches_and_typed_misses() {
    let fixture = std::env::var("RAINCLOUD_TEST_FIXTURE")
        .expect("set RAINCLOUD_TEST_FIXTURE to shared fixture");
    let mut options: Value =
        serde_json::from_slice(&std::fs::read(format!("{fixture}/options.json")).unwrap()).unwrap();
    for format in ["arrow", "parquet", "vortex"] {
        if format == "vortex" && !cfg!(feature = "vortex") {
            continue;
        }
        let ds = Dataset::load(&options, "tiny", format).unwrap();
        assert_eq!(ds.metadata()["catalog_id"], "reader-fixture");
        assert_eq!(ds.format(), format);
        let batches = ds.batches(2).unwrap();
        assert_eq!(batches.schema().fields().len(), 4);
        drop(ds);
        let mut rows = 0;
        for b in batches {
            let b = b.unwrap();
            assert!(b.num_rows() <= 2);
            rows += b.num_rows();
        }
        assert_eq!(rows, 8);
    }
    options["data_dir"] = json!(format!("{fixture}/missing"));
    let ds = Dataset::load(&options, "tiny", "arrow").unwrap();
    assert_eq!(ds.path().unwrap_err().kind, ErrorKind::OfflineMiss);
    assert_eq!(
        Dataset::load(&options, "tiny", "parquet@java")
            .unwrap_err()
            .kind,
        ErrorKind::FormatUnavailable
    );
    assert_eq!(
        Dataset::load(&json!({"cli": "/nonexistent/raincloud"}), "tiny", "auto")
            .unwrap_err()
            .kind,
        ErrorKind::Io
    );
}
