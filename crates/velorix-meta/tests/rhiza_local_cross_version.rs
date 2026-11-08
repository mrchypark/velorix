#![cfg(feature = "rhiza-backend")]

//! Isolated persisted Local `DataDir` KV compatibility check. The legacy
//! writer is built separately against the published rhizadb 0.21.2 crate by
//! the companion script; this test links only the workspace's rhizadb 0.23.3.

use std::process::Command;

use rhizadb::{Config, Db};
use tempfile::tempdir;

#[test]
#[ignore = "run with scripts/check-rhiza-local-cross-version.sh and verified SDK artifacts"]
fn rhiza_0212_local_kv_opens_appends_and_restarts_on_0233() {
    let fixture = std::env::var_os("RHIZA_0212_FIXTURE_BIN")
        .expect("companion script must provide the rhizadb 0.21.2 fixture binary");
    let dir = tempdir().expect("isolated Local DataDir");
    let output = Command::new(fixture)
        .arg("write")
        .arg(dir.path())
        .output()
        .expect("run the separately linked 0.21.2 writer");
    assert!(
        output.status.success(),
        "0.21.2 writer failed; refusing to open or initialize this path with 0.23.3: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        std::fs::read_dir(dir.path())
            .expect("inspect disposable 0.21.2 DataDir")
            .next()
            .is_some(),
        "0.21.2 writer left no DataDir state; refusing to open or initialize it with 0.23.3"
    );

    let config = || Config::new(dir.path()).local().node_id("compat-node");
    let mut db = Db::open(config()).expect("0.23.3 opens the 0.21.2 Local DataDir");
    assert_eq!(
        db.kv_get("compat/key").unwrap().as_deref(),
        Some(b"old-kv".as_slice())
    );

    db.kv_cas("compat-cas", "compat/key", b"old-kv", b"updated-kv")
        .expect("CAS the 0.21.2 value with 0.23.3");
    assert_eq!(
        db.kv_get("compat/key").unwrap().as_deref(),
        Some(b"updated-kv".as_slice())
    );
    db.kv_put("new-kv", "compat/new-key", b"new-kv")
        .expect("append a KV value with 0.23.3");
    db.close().expect("close 0.23.3 before restart");

    let mut reopened = Db::open(config()).expect("restart 0.23.3 on the same Local DataDir");
    assert_eq!(
        reopened.kv_get("compat/key").unwrap().as_deref(),
        Some(b"updated-kv".as_slice())
    );
    assert_eq!(
        reopened.kv_get("compat/new-key").unwrap().as_deref(),
        Some(b"new-kv".as_slice())
    );
    reopened.close().expect("close restarted 0.23.3 database");
}
