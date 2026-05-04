use std::fmt;

use thiserror::Error;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectStoreCapabilityProfile {
    pub backend_name: String,
    pub conditional_create: bool,
    pub atomic_visibility: bool,
    pub list_after_write: bool,
    pub read_after_write: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequiredObjectStoreCapability {
    ConditionalCreate,
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

impl ObjectStoreCapabilityProfile {
    pub fn local_development() -> Self {
        Self {
            backend_name: "local-development".to_string(),
            conditional_create: true,
            atomic_visibility: true,
            list_after_write: true,
            read_after_write: true,
        }
    }

    pub fn production(
        backend_name: impl Into<String>,
        conditional_create: bool,
        atomic_visibility: bool,
        list_after_write: bool,
        read_after_write: bool,
    ) -> Result<Self, ObjectStoreCapabilityError> {
        let profile = Self {
            backend_name: backend_name.into(),
            conditional_create,
            atomic_visibility,
            list_after_write,
            read_after_write,
        };
        profile.validate_for_velorix_durability()?;

        Ok(profile)
    }

    pub fn validate_for_velorix_durability(&self) -> Result<(), ObjectStoreCapabilityError> {
        for required_capability in RequiredObjectStoreCapability::all() {
            if !self.has(required_capability) {
                return Err(ObjectStoreCapabilityError {
                    backend_name: self.backend_name.clone(),
                    required_capability,
                });
            }
        }

        Ok(())
    }

    fn has(&self, required_capability: RequiredObjectStoreCapability) -> bool {
        match required_capability {
            RequiredObjectStoreCapability::ConditionalCreate => self.conditional_create,
            RequiredObjectStoreCapability::AtomicVisibility => self.atomic_visibility,
            RequiredObjectStoreCapability::ListAfterWrite => self.list_after_write,
            RequiredObjectStoreCapability::ReadAfterWrite => self.read_after_write,
        }
    }
}

impl ObjectStoreCapabilityError {
    pub fn backend_name(&self) -> &str {
        &self.backend_name
    }

    pub fn required_capability(&self) -> RequiredObjectStoreCapability {
        self.required_capability
    }
}

impl RequiredObjectStoreCapability {
    fn all() -> [Self; 4] {
        [
            Self::ConditionalCreate,
            Self::AtomicVisibility,
            Self::ListAfterWrite,
            Self::ReadAfterWrite,
        ]
    }
}

impl fmt::Display for RequiredObjectStoreCapability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConditionalCreate => write!(f, "conditional_create"),
            Self::AtomicVisibility => write!(f, "atomic_visibility"),
            Self::ListAfterWrite => write!(f, "list_after_write"),
            Self::ReadAfterWrite => write!(f, "read_after_write"),
        }
    }
}
