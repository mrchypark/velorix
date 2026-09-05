#![cfg(feature = "rhiza-backend")]
use serde_json::json;
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use velorix_meta::rhiza_kv::RhizaKvStore;
use velorix_meta::rhiza_kv_snapshot::{CompareExchange, RhizaKvSnapshot};

#[tokio::test]
async fn kv_binary_absent_cas_reject_and_reopen() {
    let d = tempdir().unwrap();
    let p = d.path().display().to_string();
    let kv = RhizaKvStore::open(p.clone(), "kv-test").await.unwrap();
    assert!(kv.put_if_absent("a", "k", vec![0, 255]).await.unwrap());
    assert!(!kv.put_if_absent("b", "k", vec![1]).await.unwrap());
    assert_eq!(kv.get("k").await.unwrap(), Some(vec![0, 255]));
    assert!(kv
        .put_if_absent("empty", "empty", Vec::new())
        .await
        .unwrap());
    assert_eq!(kv.get("empty").await.unwrap(), Some(Vec::new()));
    assert!(!kv
        .compare_and_set("c", "k", Some(vec![2]), vec![3])
        .await
        .unwrap());
    let changed = kv.put_if_absent("same-request", "changed", vec![1]).await;
    assert!(changed.is_ok());
    let changed = kv.put_if_absent("same-request", "changed", vec![2]).await;
    assert!(matches!(
        changed,
        Err(velorix_meta::rhiza_kv::RhizaKvError::Operation { code, .. })
            if code == "request_conflict"
    ));
    drop(kv);
    let kv = RhizaKvStore::open(p, "kv-test").await.unwrap();
    assert_eq!(kv.get("k").await.unwrap(), Some(vec![0, 255]));
}

#[tokio::test]
async fn snapshot_init_load_stale_conflict_and_replay() {
    let d = tempdir().unwrap();
    let kv = RhizaKvStore::open(d.path().display().to_string(), "snap-test")
        .await
        .unwrap();
    let snap = RhizaKvSnapshot::new(kv.clone());
    assert!(snap.load().await.unwrap().is_none());
    let applied = snap
        .compare_exchange(None, b"hello".to_vec(), "snap-a")
        .await
        .unwrap();
    let token = match applied {
        CompareExchange::Applied(t) => t,
        CompareExchange::Conflict => panic!(),
    };
    assert_eq!(snap.load().await.unwrap().unwrap().1, b"hello");
    assert!(matches!(
        snap.compare_exchange(None, b"other".to_vec(), "snap-b")
            .await
            .unwrap(),
        CompareExchange::Conflict
    ));
    assert!(matches!(
        snap.compare_exchange(Some(token.clone()), b"world".to_vec(), "snap-c")
            .await
            .unwrap(),
        CompareExchange::Applied(_)
    ));
    let replay = snap
        .compare_exchange(Some(token), b"world".to_vec(), "snap-c")
        .await
        .unwrap();
    assert!(matches!(replay, CompareExchange::Applied(_)));
    assert_eq!(snap.load().await.unwrap().unwrap().1, b"world");
}

#[tokio::test]
async fn snapshot_same_expected_root_has_one_winner() {
    let d = tempdir().unwrap();
    let kv = RhizaKvStore::open(d.path().display().to_string(), "snap-race")
        .await
        .unwrap();
    let left = RhizaKvSnapshot::new(kv.clone());
    let right = RhizaKvSnapshot::new(kv);
    let (a, b) = tokio::join!(
        left.compare_exchange(None, b"left".to_vec(), "race-left"),
        right.compare_exchange(None, b"right".to_vec(), "race-right")
    );
    let outcomes = [a.unwrap(), b.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CompareExchange::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, CompareExchange::Conflict))
            .count(),
        1
    );
}

#[tokio::test]
async fn snapshot_load_rejects_missing_page_and_full_digest_mismatch() {
    let d = tempdir().unwrap();
    let kv = RhizaKvStore::open(d.path().display().to_string(), "snap-corrupt")
        .await
        .unwrap();
    let snap = RhizaKvSnapshot::new(kv.clone());
    let page_hash = format!("{:x}", Sha256::digest(b"page"));
    let manifest = json!({
        "schema_version": 1,
        "generation": 1,
        "pages": [page_hash],
        "len": 4,
        "content_digest": format!("{:x}", Sha256::digest(b"wrong"))
    });
    assert!(kv
        .put_if_absent(
            "corrupt-root",
            "velorix/snapshot/root",
            serde_json::to_vec(&manifest).unwrap()
        )
        .await
        .unwrap());
    assert!(matches!(
        snap.load().await,
        Err(velorix_meta::rhiza_kv_snapshot::SnapshotError::Kv(_))
            | Err(velorix_meta::rhiza_kv_snapshot::SnapshotError::Invalid(_))
    ));
    let d = tempdir().unwrap();
    let kv = RhizaKvStore::open(d.path().display().to_string(), "snap-digest")
        .await
        .unwrap();
    let snap = RhizaKvSnapshot::new(kv.clone());
    assert!(kv
        .put_if_absent(
            "page",
            format!("velorix/snapshot/page/{page_hash}"),
            b"page".to_vec(),
        )
        .await
        .unwrap());
    assert!(kv
        .put_if_absent(
            "root",
            "velorix/snapshot/root",
            serde_json::to_vec(&manifest).unwrap()
        )
        .await
        .unwrap());
    assert!(matches!(
        snap.load().await,
        Err(velorix_meta::rhiza_kv_snapshot::SnapshotError::Invalid(_))
    ));
}

#[tokio::test]
async fn snapshot_rejects_oversize() {
    let d = tempdir().unwrap();
    let kv = RhizaKvStore::open(d.path().display().to_string(), "snap-size")
        .await
        .unwrap();
    let snap = RhizaKvSnapshot::new(kv);
    let err = snap
        .compare_exchange(None, vec![0u8; 16 * 1024 * 1024 + 1], "big")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("exceeds"));
}
