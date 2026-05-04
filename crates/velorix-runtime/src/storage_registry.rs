use std::{collections::BTreeMap, sync::Arc};

use datafusion::{
    error::DataFusionError,
    execution::object_store::ObjectStoreUrl,
    object_store::{path::Path, ObjectStore as DataFusionObjectStore},
};
use thiserror::Error;
use url::Url;

#[derive(Clone, Debug, Default)]
pub struct StorageRegistry {
    entries: BTreeMap<String, RegisteredObjectStore>,
}

#[derive(Clone, Debug)]
pub struct RegisteredObjectStore {
    pub store: Arc<dyn DataFusionObjectStore>,
    pub base_url: ObjectStoreUrl,
}

#[derive(Clone, Debug)]
pub struct RegisteredTableLocation {
    pub store: Arc<dyn DataFusionObjectStore>,
    pub base_url: ObjectStoreUrl,
    pub object_path: Path,
    pub table_url: String,
}

#[derive(Debug, Error)]
pub enum StorageRegistryError {
    #[error("invalid store id")]
    InvalidStoreId,
    #[error("invalid object path in {field}: {value}")]
    InvalidObjectPath { field: &'static str, value: String },
    #[error("cross-tenant object key prefix for tenant {tenant_id}: {object_key_prefix}")]
    CrossTenantPrefix {
        tenant_id: String,
        object_key_prefix: String,
    },
    #[error("unregistered object store id {store_id}")]
    UnregisteredStoreId { store_id: String },
    #[error("malformed object store base url {base_url}: {source}")]
    MalformedBaseUrl {
        base_url: String,
        #[source]
        source: DataFusionError,
    },
}

impl StorageRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(
        &mut self,
        store_id: impl Into<String>,
        base_url: &str,
        store: Arc<dyn DataFusionObjectStore>,
    ) -> Result<(), StorageRegistryError> {
        let store_id = store_id.into();
        if store_id.is_empty() {
            return Err(StorageRegistryError::InvalidStoreId);
        }
        let base_url = ObjectStoreUrl::parse(base_url).map_err(|source| {
            StorageRegistryError::MalformedBaseUrl {
                base_url: base_url.to_string(),
                source,
            }
        })?;

        self.entries
            .insert(store_id, RegisteredObjectStore { store, base_url });

        Ok(())
    }

    pub fn resolve_table_location(
        &self,
        store_id: &str,
        tenant_id: &str,
        object_key_prefix: &str,
        snapshot_ref: &str,
    ) -> Result<RegisteredTableLocation, StorageRegistryError> {
        validate_tenant_prefix(tenant_id, object_key_prefix)?;
        validate_object_path("snapshot_ref", snapshot_ref)?;

        let entry = self.entries.get(store_id).ok_or_else(|| {
            StorageRegistryError::UnregisteredStoreId {
                store_id: store_id.to_string(),
            }
        })?;
        let object_path = join_object_paths(object_key_prefix, snapshot_ref);
        let table_url = table_url(&entry.base_url, &object_path);

        Ok(RegisteredTableLocation {
            store: Arc::clone(&entry.store),
            base_url: entry.base_url.clone(),
            object_path: Path::from(object_path),
            table_url,
        })
    }
}

pub fn validate_tenant_prefix(
    tenant_id: &str,
    object_key_prefix: &str,
) -> Result<(), StorageRegistryError> {
    validate_object_path("tenant_id", tenant_id)?;
    validate_object_path("object_key_prefix", object_key_prefix)?;

    let required_prefix = format!("tenants/{tenant_id}/");
    if !object_key_prefix.starts_with(&required_prefix) {
        return Err(StorageRegistryError::CrossTenantPrefix {
            tenant_id: tenant_id.to_string(),
            object_key_prefix: object_key_prefix.to_string(),
        });
    }

    Ok(())
}

fn validate_object_path(field: &'static str, value: &str) -> Result<(), StorageRegistryError> {
    if value.is_empty()
        || value.starts_with('/')
        || value.ends_with('/')
        || value
            .split('/')
            .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return Err(StorageRegistryError::InvalidObjectPath {
            field,
            value: value.to_string(),
        });
    }

    Ok(())
}

fn join_object_paths(object_key_prefix: &str, snapshot_ref: &str) -> String {
    format!(
        "{}/{}",
        object_key_prefix.trim_matches('/'),
        snapshot_ref.trim_matches('/')
    )
}

fn table_url(base_url: &ObjectStoreUrl, object_path: &str) -> String {
    let mut url: Url = <ObjectStoreUrl as AsRef<Url>>::as_ref(base_url).clone();
    url.set_path(&format!("/{}/", object_path.trim_matches('/')));
    url.to_string()
}
