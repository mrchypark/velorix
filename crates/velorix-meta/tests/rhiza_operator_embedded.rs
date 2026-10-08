#![cfg(feature = "rhiza-backend")]

//! Focused contract test for the embedded Rhiza Operator path.
//!
//! This is the mode the Rhiza Operator drives: it rewrites the canonical
//! `RHIZA_*` environment between generations, and this process must consume it
//! natively on every start, serve the recovery endpoints on a private port, and
//! close them together with the metadata database.
//!
//! ## Why this test spawns a child process
//!
//! The Rhiza Go engine reads its configuration through `os.Getenv`, which the Go
//! runtime snapshots at process startup. A `std::env::set_var` after startup is
//! therefore invisible to native: an in-process test that mutates the
//! environment silently exercises `RHIZA_*` defaults (`node-1`, `cluster-a`,
//! `async`) instead of the generation the Operator published. That is exactly
//! why upstream requires the environment to be injected before the process
//! starts, and why the supported production path is `Db::open_from_env` with
//! no runtime environment mutation at all.
//!
//! So this file uses one test that plays two roles. Without the marker it is
//! the parent: it creates temporary directories, chooses ports, and re-executes
//! this same test binary with the canonical environment injected through
//! `Command::env`. With the marker it is the child: it opens from that
//! environment and makes every assertion. The parent asserts the child's exit
//! status, so a failed child contract fails this test.
//!
//! It runs a single voter against a filesystem object store, which is enough to
//! exercise `ConfigFromEnv`/`Db::open_from_env` -> `Db::start_operator` ->
//! metadata-operations-on-the-same-Db without the three-voter S3 fixture that
//! `rhiza_recovery.rs` owns.

use std::{
    io::{Read, Write},
    net::TcpStream,
    process::Command,
    time::{Duration, Instant},
};

use tempfile::TempDir;
use velorix_meta::rhiza_meta::RhizaKvMetaStore;
use velorix_meta::MetaStore;

mod common;

/// Set by the parent so the re-executed child knows to run the child role.
const CHILD_MARKER: &str = "VELORIX_TEST_RHIZA_OPERATOR_CHILD";

const ADMIN_TOKEN: &str = "operator-admin-token-fixture";
const PEER_TOKEN: &str = "velorix-operator-peer-token-fixture-0001";

/// Query the native recovery listener with a minimal HTTP/1.1 request. The
/// recovery listener is a native handler, so this only needs enough framing to
/// observe what it answers; it is not production code.
fn request(address: &str, method: &str, target: &str, bearer: Option<&str>) -> (u16, String) {
    let mut stream = TcpStream::connect(address).expect("connect to recovery listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    let mut request =
        format!("{method} {target} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n");
    if let Some(bearer) = bearer {
        request.push_str(&format!("Authorization: Bearer {bearer}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).expect("send request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read response");
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("HTTP status line");
    (status, text)
}

/// True when nothing answers on `address` any more. A closed listener refuses
/// the connection; a lingering one answers with bytes.
fn port_is_closed(address: &str) -> bool {
    let Ok(mut stream) = TcpStream::connect(address) else {
        return true;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    if stream
        .write_all(b"GET /recovery/status HTTP/1.1\r\n\r\n")
        .is_err()
    {
        return true;
    }
    let mut raw = Vec::new();
    match stream.read_to_end(&mut raw) {
        Ok(0) | Err(_) => true,
        Ok(_) => false,
    }
}

#[test]
fn operator_managed_open_serves_recovery_and_shares_the_metadata_db() {
    if std::env::var_os(CHILD_MARKER).is_some() {
        return child_contract();
    }
    parent_contract();
}

/// Re-execute this test binary with the canonical environment present before the
/// process starts, then require the child's assertions to have passed.
fn parent_contract() {
    let root = TempDir::new().expect("temporary root");
    let data_dir = root.path().join("data");
    let object_dir = root.path().join("objects");
    std::fs::create_dir_all(&data_dir).expect("data dir");
    std::fs::create_dir_all(&object_dir).expect("object dir");

    let recovery_port = 39091_u16;
    let peer_port = 39190_u16;
    let member_url_port = 39192_u16;
    // `log_url` is a legal native member field. The Operator publishes it, so this
    // fixture carries it rather than a Velorix-only subset; the credential proof is
    // left to native because a single voter does not require a published key.
    let members = serde_json::json!([{
        "node_id": "node-a",
        "url": format!("http://127.0.0.1:{member_url_port}"),
        "peer_url": format!("quic://127.0.0.1:{peer_port}"),
        "log_url": format!("http://127.0.0.1:{member_url_port}/log"),
    }])
    .to_string();

    // These are the canonical names the Operator maintains, one per contract
    // item, injected before the child process starts.
    let canonical: [(&str, String); 12] = [
        ("RHIZA_NODE_ID", "node-a".into()),
        (
            "RHIZA_CLUSTER_ID",
            "operator-managed-test-generation-1".into(),
        ),
        ("RHIZA_DATA_DIR", data_dir.display().to_string()),
        ("RHIZA_BIND_ADDR", format!("127.0.0.1:{recovery_port}")),
        ("RHIZA_PEER_ADDR", format!("127.0.0.1:{peer_port}")),
        // The Operator's own membership documents carry the native
        // `quepaxa.Member` shape, so the fixture uses that shape too.
        ("RHIZA_CLUSTER_MEMBERS", members),
        ("RHIZA_PEER_TOKEN", PEER_TOKEN.into()),
        ("RHIZA_ADMIN_TOKEN", ADMIN_TOKEN.into()),
        ("RHIZA_OBJSTORE_PROVIDER", "filesystem".into()),
        ("RHIZA_OBJSTORE_DIR", object_dir.display().to_string()),
        ("RHIZA_OBJSTORE_PREFIX", "operator-managed-test".into()),
        ("RHIZA_OBJSTORE_DURABILITY", "before-ack".into()),
    ];

    let executable = std::env::current_exe().expect("current test binary");
    let mut command = Command::new(&executable);
    command
        .arg("--exact")
        .arg("operator_managed_open_serves_recovery_and_shares_the_metadata_db");
    command.arg("--nocapture");
    // An inherited `RHIZA_*` from the developer's shell would silently change
    // what native reads, so clear the whole family before injecting.
    for name in [
        "RHIZA_NODE_ID",
        "RHIZA_CLUSTER_ID",
        "RHIZA_DATA_DIR",
        "RHIZA_BIND_ADDR",
        "RHIZA_PEER_ADDR",
        "RHIZA_CLUSTER_MEMBERS",
        "RHIZA_PEER_TOKENS",
        "RHIZA_PEER_TOKEN",
        "RHIZA_ADMIN_TOKEN",
        "RHIZA_OBJSTORE_PROVIDER",
        "RHIZA_OBJSTORE_BUCKET",
        "RHIZA_OBJSTORE_ENDPOINT",
        "RHIZA_OBJSTORE_DIR",
        "RHIZA_OBJSTORE_PREFIX",
        "RHIZA_OBJSTORE_DURABILITY",
        "RHIZA_OBJSTORE_INSECURE",
        "RHIZA_OBJSTORE_ACCESS_KEY",
        "RHIZA_OBJSTORE_SECRET_KEY",
        "RHIZA_OBJSTORE_SESSION_TOKEN",
        "RHIZA_OBJSTORE_REGION",
        "RHIZA_CHECKPOINT_INTERVAL",
    ] {
        command.env_remove(name);
    }
    for (name, value) in &canonical {
        command.env(name, value);
    }
    // Keep background publication out of the child's short lifetime.
    command.env("RHIZA_CHECKPOINT_INTERVAL", "1h");
    command.env(CHILD_MARKER, "1");
    command.env(
        "VELORIX_TEST_RHIZA_OPERATOR_RECOVERY_BIND",
        format!("127.0.0.1:{recovery_port}"),
    );

    let status = command
        .status()
        .expect("run the operator-managed child process");
    assert!(
        status.success(),
        "the operator-managed child process failed with {status}"
    );
}

/// The child role: open from the canonical environment and verify the contract.
fn child_contract() {
    let recovery_bind = std::env::var("VELORIX_TEST_RHIZA_OPERATOR_RECOVERY_BIND")
        .expect("child recovery bind address");

    // The generation native actually read is observable through the recovery
    // status endpoint, so assert on it rather than assuming the environment
    // reached it.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    runtime.block_on(async {
        let store = RhizaKvMetaStore::open_operator_managed(recovery_bind.clone())
            .await
            .expect("open from the canonical RHIZA_* environment");

        // The listener is started on the same `Db` that serves metadata
        // operations, so the address is reported by native rather than assumed.
        let bound = store
            .recovery_address()
            .expect("operator-managed open binds a recovery listener")
            .to_owned();
        assert_eq!(
            bound, recovery_bind,
            "the recovery listener must bind the requested address"
        );

        // Local readiness, which a learner reports before it has a quorum.
        assert!(
            store.ready().await.expect("native ready"),
            "a voter with a local node must report local readiness"
        );

        let (status, body) = request(&bound, "GET", "/recovery/status", None);
        assert_eq!(status, 200, "recovery status body: {body}");
        // The environment really reached native: these defaults would read
        // `node-1`, `cluster-a`, and `async`.
        assert!(
            body.contains("\"node_id\":\"node-a\""),
            "native must read the injected RHIZA_NODE_ID: {body}"
        );
        assert!(
            body.contains("operator-managed-test-generation-1"),
            "native must read the injected RHIZA_CLUSTER_ID: {body}"
        );
        assert!(
            body.contains("\"durability\":\"before-ack\""),
            "native must read the injected RHIZA_OBJSTORE_DURABILITY: {body}"
        );
        // Local ready and quorum are distinct, and the Operator reads both.
        assert!(
            body.contains("\"quorum\""),
            "quorum must be reported: {body}"
        );

        // Archive publication is authenticated with the admin token native read
        // from RHIZA_ADMIN_TOKEN, and is disabled when that token is empty.
        let (unauthenticated, body) = request(&bound, "POST", "/recovery/archive", None);
        assert!(
            !(200..300).contains(&unauthenticated),
            "recovery archive must reject an unauthenticated request, got \
             {unauthenticated}: {body}"
        );
        let (wrong_token, _) = request(
            &bound,
            "POST",
            "/recovery/archive",
            Some("not-the-admin-token"),
        );
        assert_eq!(
            wrong_token, unauthenticated,
            "a wrong admin token must be refused exactly like no token"
        );
        let (authenticated, body) = request(&bound, "POST", "/recovery/archive", Some(ADMIN_TOKEN));
        assert_eq!(
            authenticated, 200,
            "the admin token native read from the environment must authenticate: {body}"
        );

        // Membership management requires an opt-in this single-voter fixture does
        // not set, so it must stay refused rather than silently answer.
        let (membership, _) = request(&bound, "GET", "/membership/status", Some(ADMIN_TOKEN));
        assert!(
            !(200..300).contains(&membership),
            "membership management must stay disabled without the reconfiguration opt-in"
        );

        // The recovery listener serves only the recovery paths. A Velorix
        // metadata route must not appear here, or the private port would become
        // a second metadata entry point.
        let (not_found, _) = request(&bound, "GET", "/ready", None);
        assert_eq!(
            not_found, 404,
            "the recovery listener must not expose the database HTTP API"
        );

        // Metadata operations run against the same DB the recovery endpoints
        // serve. A circular claim is not evidence, so publish a real relation
        // catalog through the MetaStore and read it back from the same handle.
        let catalog = common::orders_relation_catalog("v1");
        store
            .store_relation_catalog(catalog.clone())
            .await
            .expect("store relation catalog on the operator-managed DB");
        let read = store
            .read_relation_catalog("orders", "v1")
            .await
            .expect("read relation catalog from the same DB");
        assert_eq!(read, catalog);

        store
            .close()
            .await
            .expect("close the operator-managed store");

        // The recovery listener closes with the Db that owns it. Give the OS a
        // moment to release the socket before declaring it still open.
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline && !port_is_closed(&bound) {
            std::thread::sleep(Duration::from_millis(100));
        }
        assert!(
            port_is_closed(&bound),
            "the recovery listener must close with the DB, but {bound} still answers"
        );
    });
}
