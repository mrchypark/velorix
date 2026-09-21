//! Cross-version compatibility driver. Never point this at production data.
//! Run old-write once, then copy the entire fixture before upgrade or rollback probes.
use bytes::Bytes;
use object_store::{local::LocalFileSystem, ObjectStore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{fs, io::Write, path::Path, sync::Arc, time::Duration};
use velorix_storage::{
    manifest::StateObjectRef,
    state::{CheckpointPublishError, StateObjectWrite},
    state_store::{SlateDbStateStore, StateObjectStore},
};

type Error = Box<dyn std::error::Error + Send + Sync>;
const DB_PATH: &str = "compatibility/state";

#[derive(Serialize, Deserialize)]
struct Entry {
    name: String,
    state_ref: StateObjectRef,
    payload_sha256: String,
    payload_bytes: usize,
}

#[derive(Serialize, Deserialize)]
struct Manifest {
    schema_version: u32,
    stage: String,
    entries: Vec<Entry>,
}

fn payload(name: &str) -> Result<Bytes, Error> {
    Ok(match name {
        "binary" => Bytes::from_static(&[0, 255, 128, 1, 0, 17]),
        "empty" => Bytes::new(),
        "large" => Bytes::from((0..262_144).map(|n| (n % 251) as u8).collect::<Vec<_>>()),
        "release" => Bytes::from_static(b"release-after-upgrade"),
        "new" => Bytes::from_static(b"written-by-new-version"),
        _ => return Err(format!("unknown fixture payload: {name}").into()),
    })
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn write_new_json(path: &Path, value: &impl Serialize) -> Result<(), Error> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    Ok(())
}

async fn write_entry(store: &SlateDbStateStore, name: &str) -> Result<Entry, Error> {
    let bytes = payload(name)?;
    let write = StateObjectWrite::new("upgrade-fixture", 0, 1, name, bytes.clone())?;
    let state_ref = store.write_state_object(&write).await?;
    if !matches!(
        store.write_state_object(&write).await,
        Err(CheckpointPublishError::StateObjectAlreadyExists(_))
    ) {
        return Err("duplicate state write did not fail closed".into());
    }
    Ok(Entry {
        name: name.to_owned(),
        state_ref,
        payload_sha256: digest(&bytes),
        payload_bytes: bytes.len(),
    })
}

async fn verify(store: &SlateDbStateStore, manifest: &Manifest) -> Result<(), Error> {
    if manifest.schema_version != 1 || !matches!(manifest.stage.as_str(), "old" | "upgraded") {
        return Err("unsupported fixture manifest".into());
    }
    let expected: &[&str] = if manifest.stage == "old" {
        &["binary", "empty", "large", "release"]
    } else {
        &["binary", "empty", "large", "release", "new"]
    };
    if manifest
        .entries
        .iter()
        .map(|e| e.name.as_str())
        .collect::<Vec<_>>()
        != expected
    {
        return Err("fixture entry list differs from expected exact set".into());
    }
    for entry in &manifest.entries {
        let bytes = payload(&entry.name)?;
        if digest(&bytes) != entry.payload_sha256 || bytes.len() != entry.payload_bytes {
            return Err(format!("fixture payload metadata mismatch: {}", entry.name).into());
        }
        let exists = store.state_object_exists(&entry.state_ref).await?;
        if manifest.stage == "upgraded" && entry.name == "release" {
            if exists
                || !matches!(
                    store.read_state_object(&entry.state_ref).await,
                    Err(CheckpointPublishError::MissingStateObject(_))
                )
            {
                return Err("released state still exists or returned unexpected error".into());
            }
        } else if !exists || store.read_state_object(&entry.state_ref).await? != bytes {
            return Err(format!("readback mismatch: {}", entry.name).into());
        }
    }
    Ok(())
}

async fn run(mode: &str, root: &Path) -> Result<(), Error> {
    let object_path = root.join("objects");
    if mode == "old-write" {
        // Exclusive creation prevents accidentally modifying an existing fixture.
        fs::create_dir(root)?;
        fs::create_dir(&object_path)?;
    }
    let objects: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&object_path)?);
    let mut manifest = if mode == "old-write" {
        Manifest {
            schema_version: 1,
            stage: "old".into(),
            entries: Vec::new(),
        }
    } else {
        let path = if root.join("upgraded-refs.json").exists() {
            root.join("upgraded-refs.json")
        } else {
            root.join("old-refs.json")
        };
        serde_json::from_slice::<Manifest>(&fs::read(path)?)?
    };
    if mode == "upgrade" && manifest.stage != "old" {
        return Err("upgrade requires an untouched copy of the old fixture".into());
    }
    let store = SlateDbStateStore::open(DB_PATH, objects.clone()).await?;
    if mode == "old-write" {
        for name in ["binary", "empty", "large", "release"] {
            manifest.entries.push(write_entry(&store, name).await?);
        }
        verify(&store, &manifest).await?;
        write_new_json(&root.join("old-refs.json"), &manifest)?;
    } else {
        verify(&store, &manifest).await?;
        if mode == "upgrade" {
            manifest.entries.push(write_entry(&store, "new").await?);
            let release = &manifest.entries[3].state_ref;
            if !store.release_state_object(release).await? {
                return Err("release did not report removal".into());
            }
            manifest.stage = "upgraded".into();
            verify(&store, &manifest).await?;
            write_new_json(&root.join("upgraded-refs.json"), &manifest)?;
        }
    }
    store.close().await?;
    let reopened = SlateDbStateStore::open(DB_PATH, objects).await?;
    verify(&reopened, &manifest).await?;
    reopened.close().await?;
    println!(
        "{}",
        serde_json::json!({"schema_version": 1, "mode": mode,
        "stage": manifest.stage, "verified_entries": manifest.entries.len(),
        "reopen_verified": true, "pre_close_durability_proven": false})
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 || !matches!(args[1].as_str(), "old-write" | "verify" | "upgrade") {
        return Err("usage: state_store_upgrade_fixture <old-write|verify|upgrade> <absolute-fixture-dir>; never use production data".into());
    }
    let root = Path::new(&args[2]);
    if !root.is_absolute() || root.parent().is_none() {
        return Err("fixture path must be absolute and not the filesystem root".into());
    }
    tokio::time::timeout(Duration::from_secs(60), run(&args[1], root)).await??;
    Ok(())
}
