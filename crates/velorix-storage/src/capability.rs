use std::{
    collections::BTreeMap,
    fmt,
    time::{SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode, UpdateVersion};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreCapabilityProfile {
    pub backend_name: String,
    pub conditional_create: bool,
    pub conditional_update: bool,
    pub atomic_visibility: bool,
    pub list_after_write: bool,
    pub read_after_write: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AuthoritativeNamespace {
    Ingest,
    IngestAdmission,
    IngestAdmissionIndex,
    State,
    Output,
    Checkpoint,
    CheckpointIndex,
    CheckpointLifecycle,
    CheckpointRetention,
    CheckpointGcTransition,
    CheckpointRecovery,
    Ownership,
    TableCatalog,
    RelationCatalog,
    ArtifactCatalog,
    BenchmarkEvidence,
    GcRuns,
    Queries,
    QueryPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthoritativeObjectStoreCapabilitiesV1 {
    pub profiles: BTreeMap<AuthoritativeNamespace, ObjectStoreCapabilityProfile>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreCapabilityDiagnostic {
    pub namespace: AuthoritativeNamespace,
    pub backend_name: Option<String>,
    pub missing_capabilities: Vec<RequiredObjectStoreCapability>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreCapabilityProbeReport {
    pub backend_name: String,
    pub probe_key: String,
    pub conditional_create: bool,
    pub conditional_update: bool,
    pub atomic_visibility: bool,
    pub list_after_write: bool,
    pub read_after_write: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequiredObjectStoreCapability {
    ConditionalCreate,
    ConditionalUpdate,
    AtomicVisibility,
    ListAfterWrite,
    ReadAfterWrite,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[error(
    "object store profile `{backend_name}` is missing required Velorix durability capability `{required_capability}`"
)]
pub struct ObjectStoreCapabilityError {
    backend_name: String,
    required_capability: RequiredObjectStoreCapability,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum AuthoritativeObjectStoreCapabilityError {
    #[error("authoritative object-store namespace `{namespace}` is missing a capability profile")]
    MissingNamespace { namespace: AuthoritativeNamespace },
    #[error("authoritative object-store namespace `{namespace}` has invalid capability profile: {source}")]
    NamespaceProfile {
        namespace: AuthoritativeNamespace,
        #[source]
        source: ObjectStoreCapabilityError,
    },
}

#[derive(Debug, Error)]
pub enum ObjectStoreCapabilityProbeError {
    #[error("object-store capability probe write failed for `{probe_key}`: {source}")]
    Write {
        probe_key: String,
        #[source]
        source: object_store::Error,
    },
    #[error("object-store capability probe read failed for `{probe_key}`: {source}")]
    Read {
        probe_key: String,
        #[source]
        source: object_store::Error,
    },
    #[error("object-store capability probe list failed for `{probe_prefix}`: {source}")]
    List {
        probe_prefix: String,
        #[source]
        source: object_store::Error,
    },
    #[error("object-store capability probe `{probe_key}` returned different bytes after create")]
    ReadMismatch { probe_key: String },
    #[error("object-store capability probe `{probe_key}` allowed a stale conditional update")]
    StaleUpdateAccepted { probe_key: String },
    #[error(
        "object-store capability probe `{probe_key}` changed bytes during stale conditional update"
    )]
    StaleUpdateMutatedObject { probe_key: String },
    #[error(transparent)]
    Capability(#[from] ObjectStoreCapabilityError),
}

#[derive(Debug, Error)]
pub enum AuthoritativeObjectStoreCapabilityProbeError {
    #[error(
        "authoritative object-store namespace `{namespace}` failed capability probe: {source}"
    )]
    Namespace {
        namespace: AuthoritativeNamespace,
        #[source]
        source: ObjectStoreCapabilityProbeError,
    },
}

impl ObjectStoreCapabilityProfile {
    pub fn local_development() -> Self {
        Self {
            backend_name: "local-development".to_string(),
            conditional_create: true,
            conditional_update: true,
            atomic_visibility: true,
            list_after_write: true,
            read_after_write: true,
        }
    }

    pub fn production(
        backend_name: impl Into<String>,
        conditional_create: bool,
        conditional_update: bool,
        atomic_visibility: bool,
        list_after_write: bool,
        read_after_write: bool,
    ) -> Result<Self, ObjectStoreCapabilityError> {
        let profile = Self {
            backend_name: backend_name.into(),
            conditional_create,
            conditional_update,
            atomic_visibility,
            list_after_write,
            read_after_write,
        };
        profile.validate_for_velorix_durability()?;

        Ok(profile)
    }

    pub fn validate_for_velorix_durability(&self) -> Result<(), ObjectStoreCapabilityError> {
        for required_capability in RequiredObjectStoreCapability::velorix_durability() {
            if !self.has(required_capability) {
                return Err(ObjectStoreCapabilityError {
                    backend_name: self.backend_name.clone(),
                    required_capability,
                });
            }
        }

        Ok(())
    }

    pub fn validate_for_conditional_update(&self) -> Result<(), ObjectStoreCapabilityError> {
        if !self.conditional_update {
            return Err(ObjectStoreCapabilityError {
                backend_name: self.backend_name.clone(),
                required_capability: RequiredObjectStoreCapability::ConditionalUpdate,
            });
        }

        Ok(())
    }

    fn has(&self, required_capability: RequiredObjectStoreCapability) -> bool {
        match required_capability {
            RequiredObjectStoreCapability::ConditionalCreate => self.conditional_create,
            RequiredObjectStoreCapability::ConditionalUpdate => self.conditional_update,
            RequiredObjectStoreCapability::AtomicVisibility => self.atomic_visibility,
            RequiredObjectStoreCapability::ListAfterWrite => self.list_after_write,
            RequiredObjectStoreCapability::ReadAfterWrite => self.read_after_write,
        }
    }

    fn missing_capabilities(&self) -> Vec<RequiredObjectStoreCapability> {
        RequiredObjectStoreCapability::velorix_durability()
            .into_iter()
            .filter(|capability| !self.has(*capability))
            .collect()
    }
}

impl AuthoritativeObjectStoreCapabilitiesV1 {
    pub fn new(profiles: BTreeMap<AuthoritativeNamespace, ObjectStoreCapabilityProfile>) -> Self {
        Self { profiles }
    }

    pub fn validate_for_startup(&self) -> Result<(), AuthoritativeObjectStoreCapabilityError> {
        for namespace in AuthoritativeNamespace::all() {
            self.validate_namespace(namespace)?;
        }

        Ok(())
    }

    pub fn validate_namespace(
        &self,
        namespace: AuthoritativeNamespace,
    ) -> Result<&ObjectStoreCapabilityProfile, AuthoritativeObjectStoreCapabilityError> {
        let profile = self
            .profiles
            .get(&namespace)
            .ok_or(AuthoritativeObjectStoreCapabilityError::MissingNamespace { namespace })?;
        profile
            .validate_for_velorix_durability()
            .map_err(
                |source| AuthoritativeObjectStoreCapabilityError::NamespaceProfile {
                    namespace,
                    source,
                },
            )?;

        Ok(profile)
    }

    pub fn diagnostics(&self) -> Vec<ObjectStoreCapabilityDiagnostic> {
        AuthoritativeNamespace::all()
            .into_iter()
            .map(|namespace| match self.profiles.get(&namespace) {
                Some(profile) => ObjectStoreCapabilityDiagnostic {
                    namespace,
                    backend_name: Some(profile.backend_name.clone()),
                    missing_capabilities: profile.missing_capabilities(),
                },
                None => ObjectStoreCapabilityDiagnostic {
                    namespace,
                    backend_name: None,
                    missing_capabilities: RequiredObjectStoreCapability::velorix_durability()
                        .to_vec(),
                },
            })
            .collect()
    }
}

impl ObjectStoreCapabilityProbeReport {
    pub fn observed_profile(&self) -> ObjectStoreCapabilityProfile {
        ObjectStoreCapabilityProfile {
            backend_name: self.backend_name.clone(),
            conditional_create: self.conditional_create,
            conditional_update: self.conditional_update,
            atomic_visibility: self.atomic_visibility,
            list_after_write: self.list_after_write,
            read_after_write: self.read_after_write,
        }
    }
}

pub async fn probe_object_store_capabilities(
    store: &dyn ObjectStore,
    backend_name: impl Into<String>,
    probe_prefix: impl AsRef<str>,
) -> Result<ObjectStoreCapabilityProbeReport, ObjectStoreCapabilityProbeError> {
    let backend_name = backend_name.into();
    let probe_prefix = normalize_probe_prefix(probe_prefix.as_ref());
    let probe_key = unique_probe_key(&probe_prefix);
    let path = Path::from(probe_key.as_str());
    let payload = Bytes::from_static(b"velorix-object-store-capability-probe-v1");

    store
        .put_opts(&path, payload.clone().into(), PutMode::Create.into())
        .await
        .map_err(|source| ObjectStoreCapabilityProbeError::Write {
            probe_key: probe_key.clone(),
            source,
        })?;

    let get_result =
        store
            .get(&path)
            .await
            .map_err(|source| ObjectStoreCapabilityProbeError::Read {
                probe_key: probe_key.clone(),
                source,
            })?;
    let read_bytes =
        get_result
            .bytes()
            .await
            .map_err(|source| ObjectStoreCapabilityProbeError::Read {
                probe_key: probe_key.clone(),
                source,
            })?;

    let read_after_write = read_bytes == payload;
    if !read_after_write {
        let _ = store.delete(&path).await;
        return Err(ObjectStoreCapabilityProbeError::ReadMismatch { probe_key });
    }

    let duplicate_create = store
        .put_opts(&path, payload.clone().into(), PutMode::Create.into())
        .await;
    let conditional_create = match duplicate_create {
        Err(object_store::Error::AlreadyExists { .. }) => true,
        Ok(_) => false,
        Err(source) => {
            let _ = store.delete(&path).await;
            return Err(ObjectStoreCapabilityProbeError::Write {
                probe_key: probe_key.clone(),
                source,
            });
        }
    };

    let current_get_result =
        store
            .get(&path)
            .await
            .map_err(|source| ObjectStoreCapabilityProbeError::Read {
                probe_key: probe_key.clone(),
                source,
            })?;
    let update_version = UpdateVersion {
        e_tag: current_get_result.meta.e_tag.clone(),
        version: current_get_result.meta.version.clone(),
    };
    let updated_payload = Bytes::from_static(b"velorix-object-store-capability-probe-v2");
    let update_result = store
        .put_opts(
            &path,
            updated_payload.clone().into(),
            PutMode::Update(update_version.clone()).into(),
        )
        .await;
    let conditional_update = match update_result {
        Ok(_) => match store
            .put_opts(
                &path,
                Bytes::from_static(b"velorix-object-store-stale-update").into(),
                PutMode::Update(update_version).into(),
            )
            .await
        {
            Err(object_store::Error::Precondition { .. }) => true,
            Ok(_) => {
                let _ = store.delete(&path).await;
                return Err(ObjectStoreCapabilityProbeError::StaleUpdateAccepted {
                    probe_key: probe_key.clone(),
                });
            }
            Err(source) => {
                let _ = store.delete(&path).await;
                return Err(ObjectStoreCapabilityProbeError::Write {
                    probe_key: probe_key.clone(),
                    source,
                });
            }
        },
        Err(object_store::Error::NotImplemented) => false,
        Err(source) => {
            let _ = store.delete(&path).await;
            return Err(ObjectStoreCapabilityProbeError::Write {
                probe_key: probe_key.clone(),
                source,
            });
        }
    };
    if conditional_update {
        let post_stale_bytes = store
            .get(&path)
            .await
            .map_err(|source| ObjectStoreCapabilityProbeError::Read {
                probe_key: probe_key.clone(),
                source,
            })?
            .bytes()
            .await
            .map_err(|source| ObjectStoreCapabilityProbeError::Read {
                probe_key: probe_key.clone(),
                source,
            })?;
        if post_stale_bytes != updated_payload {
            let _ = store.delete(&path).await;
            return Err(ObjectStoreCapabilityProbeError::StaleUpdateMutatedObject {
                probe_key: probe_key.clone(),
            });
        }
    }

    let listed = store
        .list(Some(&Path::from(probe_prefix.as_str())))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|source| ObjectStoreCapabilityProbeError::List {
            probe_prefix: probe_prefix.clone(),
            source,
        })?
        .into_iter()
        .any(|meta| meta.location == path);

    let _ = store.delete(&path).await;

    Ok(ObjectStoreCapabilityProbeReport {
        backend_name,
        probe_key,
        conditional_create,
        conditional_update,
        atomic_visibility: read_after_write,
        list_after_write: listed,
        read_after_write,
    })
}

pub async fn probe_production_object_store_profile(
    store: &dyn ObjectStore,
    backend_name: impl Into<String>,
    probe_prefix: impl AsRef<str>,
) -> Result<ObjectStoreCapabilityProfile, ObjectStoreCapabilityProbeError> {
    let profile = probe_object_store_capabilities(store, backend_name, probe_prefix)
        .await?
        .observed_profile();
    profile.validate_for_velorix_durability()?;

    Ok(profile)
}

pub async fn probe_authoritative_object_store_capabilities(
    store: &dyn ObjectStore,
    backend_name: impl AsRef<str>,
    probe_prefix: impl AsRef<str>,
) -> Result<AuthoritativeObjectStoreCapabilitiesV1, AuthoritativeObjectStoreCapabilityProbeError> {
    let mut profiles = BTreeMap::new();
    let probe_prefix = normalize_probe_prefix(probe_prefix.as_ref());

    for namespace in AuthoritativeNamespace::all() {
        let namespace_prefix = format!("{probe_prefix}/{namespace}");
        let profile = probe_production_object_store_profile(
            store,
            backend_name.as_ref().to_string(),
            namespace_prefix,
        )
        .await
        .map_err(
            |source| AuthoritativeObjectStoreCapabilityProbeError::Namespace { namespace, source },
        )?;
        profiles.insert(namespace, profile);
    }

    Ok(AuthoritativeObjectStoreCapabilitiesV1::new(profiles))
}

impl ObjectStoreCapabilityError {
    pub fn backend_name(&self) -> &str {
        &self.backend_name
    }

    pub fn required_capability(&self) -> RequiredObjectStoreCapability {
        self.required_capability
    }
}

fn normalize_probe_prefix(prefix: &str) -> String {
    let prefix = prefix.trim_matches('/');
    if prefix.is_empty() {
        "v1/capability-probes".to_string()
    } else {
        prefix.to_string()
    }
}

fn unique_probe_key(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("{prefix}/pid={}/nanos={nanos}.probe", std::process::id())
}

impl AuthoritativeNamespace {
    pub fn all() -> [Self; 19] {
        [
            Self::Ingest,
            Self::IngestAdmission,
            Self::IngestAdmissionIndex,
            Self::State,
            Self::Output,
            Self::Checkpoint,
            Self::CheckpointIndex,
            Self::CheckpointLifecycle,
            Self::CheckpointRetention,
            Self::CheckpointGcTransition,
            Self::CheckpointRecovery,
            Self::Ownership,
            Self::TableCatalog,
            Self::RelationCatalog,
            Self::ArtifactCatalog,
            Self::BenchmarkEvidence,
            Self::GcRuns,
            Self::Queries,
            Self::QueryPolicy,
        ]
    }
}

impl RequiredObjectStoreCapability {
    fn velorix_durability() -> [Self; 4] {
        [
            Self::ConditionalCreate,
            Self::AtomicVisibility,
            Self::ListAfterWrite,
            Self::ReadAfterWrite,
        ]
    }
}

impl fmt::Display for AuthoritativeNamespace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ingest => write!(f, "ingest"),
            Self::IngestAdmission => write!(f, "ingest-admission"),
            Self::IngestAdmissionIndex => write!(f, "ingest-admission-index"),
            Self::State => write!(f, "state"),
            Self::Output => write!(f, "output"),
            Self::Checkpoint => write!(f, "checkpoint"),
            Self::CheckpointIndex => write!(f, "checkpoint-index"),
            Self::CheckpointLifecycle => write!(f, "checkpoint-lifecycle"),
            Self::CheckpointRetention => write!(f, "checkpoint-retention"),
            Self::CheckpointGcTransition => write!(f, "checkpoint-gc-transitions"),
            Self::CheckpointRecovery => write!(f, "checkpoint-recovery"),
            Self::Ownership => write!(f, "ownership"),
            Self::TableCatalog => write!(f, "table_catalog"),
            Self::RelationCatalog => write!(f, "relation_catalog"),
            Self::ArtifactCatalog => write!(f, "artifact_catalog"),
            Self::BenchmarkEvidence => write!(f, "benchmark_evidence"),
            Self::GcRuns => write!(f, "gc-runs"),
            Self::Queries => write!(f, "queries"),
            Self::QueryPolicy => write!(f, "query-policy"),
        }
    }
}

impl fmt::Display for RequiredObjectStoreCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConditionalCreate => write!(f, "conditional_create"),
            Self::ConditionalUpdate => write!(f, "conditional_update"),
            Self::AtomicVisibility => write!(f, "atomic_visibility"),
            Self::ListAfterWrite => write!(f, "list_after_write"),
            Self::ReadAfterWrite => write!(f, "read_after_write"),
        }
    }
}
