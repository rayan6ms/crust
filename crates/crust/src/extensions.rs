//! Minimal in-tree extension boundary.
//!
//! This is deliberately a Rust composition boundary, not a plugin ABI.  Core
//! modules can register a bounded descriptor and the server can report or
//! look up that module without loading Java/Kotlin artifacts or committing to
//! a future WASM/RPC contract.

use std::fmt;
use std::sync::Arc;

/// The maximum number of first-party extensions admitted by one core.
pub const MAX_IN_TREE_EXTENSIONS: usize = 64;
const MAX_EXTENSION_ID_BYTES: usize = 128;
const MAX_EXTENSION_VERSION_BYTES: usize = 128;
const MAX_EXTENSION_CAPABILITIES: usize = 32;
const MAX_EXTENSION_CAPABILITY_BYTES: usize = 128;

/// A coarse owner category for product diagnostics.  It is intentionally not
/// a protocol/plugin type and can grow with first-party modules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ExtensionKind {
    Media,
    Source,
    Filter,
    Voice,
    Operations,
}

impl ExtensionKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Media => "media",
            Self::Source => "source",
            Self::Filter => "filter",
            Self::Voice => "voice",
            Self::Operations => "operations",
        }
    }
}

/// Stable, human-readable identity and capabilities of an in-tree module.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExtensionDescriptor {
    pub id: String,
    pub version: String,
    pub kind: ExtensionKind,
    pub capabilities: Vec<String>,
}

impl ExtensionDescriptor {
    /// Constructs a descriptor while enforcing the registry's retained-data
    /// bounds.  IDs and versions are opaque strings; interpretation belongs to
    /// the owning first-party module.
    pub fn new(
        id: impl Into<String>,
        version: impl Into<String>,
        kind: ExtensionKind,
        capabilities: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, ExtensionError> {
        let id = id.into();
        if id.is_empty() || id.len() > MAX_EXTENSION_ID_BYTES {
            return Err(ExtensionError::InvalidId);
        }
        let version = version.into();
        if version.is_empty() || version.len() > MAX_EXTENSION_VERSION_BYTES {
            return Err(ExtensionError::InvalidVersion);
        }

        let mut normalized = Vec::new();
        for capability in capabilities {
            let capability = capability.into();
            if capability.is_empty() || capability.len() > MAX_EXTENSION_CAPABILITY_BYTES {
                return Err(ExtensionError::InvalidCapability);
            }
            if normalized.iter().any(|item| item == &capability) {
                return Err(ExtensionError::DuplicateCapability);
            }
            if normalized.len() == MAX_EXTENSION_CAPABILITIES {
                return Err(ExtensionError::TooManyCapabilities);
            }
            normalized.push(capability);
        }

        Ok(Self {
            id,
            version,
            kind,
            capabilities: normalized,
        })
    }
}

/// Errors from descriptor construction or bounded registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtensionError {
    InvalidId,
    InvalidVersion,
    InvalidCapability,
    DuplicateCapability,
    TooManyCapabilities,
    Capacity,
    DuplicateId,
}

impl fmt::Display for ExtensionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::InvalidId => "in-tree extension id is empty or too long",
            Self::InvalidVersion => "in-tree extension version is empty or too long",
            Self::InvalidCapability => "in-tree extension capability is empty or too long",
            Self::DuplicateCapability => "in-tree extension capabilities must be unique",
            Self::TooManyCapabilities => "in-tree extension capability limit reached",
            Self::Capacity => "in-tree extension registry capacity reached",
            Self::DuplicateId => "in-tree extension id is already registered",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for ExtensionError {}

/// A first-party module implemented in the Crust workspace.
pub trait InTreeExtension: Send + Sync {
    /// Returns the stable descriptor used for registration and diagnostics.
    fn descriptor(&self) -> ExtensionDescriptor;
}

/// Adapter for modules that only need to publish a static descriptor.
#[derive(Debug, Clone)]
pub struct StaticExtension {
    descriptor: ExtensionDescriptor,
}

impl StaticExtension {
    pub fn new(descriptor: ExtensionDescriptor) -> Self {
        Self { descriptor }
    }
}

impl InTreeExtension for StaticExtension {
    fn descriptor(&self) -> ExtensionDescriptor {
        self.descriptor.clone()
    }
}

/// Bounded registry owned by the Crust composition root.
#[derive(Clone, Default)]
pub struct ExtensionRegistry {
    extensions: Vec<RegisteredExtension>,
}

#[derive(Clone)]
struct RegisteredExtension {
    descriptor: ExtensionDescriptor,
    extension: Arc<dyn InTreeExtension>,
}

impl fmt::Debug for ExtensionRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExtensionRegistry")
            .field("descriptors", &self.descriptors())
            .finish()
    }
}

impl ExtensionRegistry {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            extensions: Vec::new(),
        }
    }

    /// Registers one in-tree module. IDs are the stable identity and are
    /// unique within one server instance.
    pub fn register<E>(&mut self, extension: E) -> Result<(), ExtensionError>
    where
        E: InTreeExtension + 'static,
    {
        self.register_arc(Arc::new(extension))
    }

    /// Registers an already shared in-tree module.
    pub fn register_arc(
        &mut self,
        extension: Arc<dyn InTreeExtension>,
    ) -> Result<(), ExtensionError> {
        if self.extensions.len() >= MAX_IN_TREE_EXTENSIONS {
            return Err(ExtensionError::Capacity);
        }
        let descriptor = extension.descriptor();
        let id = &descriptor.id;
        if self
            .extensions
            .iter()
            .any(|existing| existing.descriptor.id == *id)
        {
            return Err(ExtensionError::DuplicateId);
        }
        self.extensions.push(RegisteredExtension {
            descriptor,
            extension,
        });
        Ok(())
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<Arc<dyn InTreeExtension>> {
        self.extensions
            .iter()
            .find(|extension| extension.descriptor.id == id)
            .map(|extension| Arc::clone(&extension.extension))
    }

    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.get(id).is_some()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.extensions.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.extensions.is_empty()
    }

    /// Returns descriptors in stable ID order regardless of registration
    /// order, making diagnostics and conformance fixtures deterministic.
    #[must_use]
    pub fn descriptors(&self) -> Vec<ExtensionDescriptor> {
        let mut descriptors: Vec<_> = self
            .extensions
            .iter()
            .map(|extension| extension.descriptor.clone())
            .collect();
        descriptors.sort_by(|left, right| left.id.cmp(&right.id));
        descriptors
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn descriptor(id: &str) -> ExtensionDescriptor {
        ExtensionDescriptor::new(id, "1.0.0", ExtensionKind::Operations, ["test:report"]).unwrap()
    }

    #[test]
    fn deterministic_registry_reports_and_looks_up_in_tree_extensions() {
        let mut registry = ExtensionRegistry::new();
        registry
            .register(StaticExtension::new(descriptor("zeta")))
            .unwrap();
        registry
            .register(StaticExtension::new(descriptor("alpha")))
            .unwrap();

        assert_eq!(registry.len(), 2);
        assert_eq!(
            registry
                .descriptors()
                .into_iter()
                .map(|item| item.id)
                .collect::<Vec<_>>(),
            ["alpha", "zeta"]
        );
        assert!(registry.contains("alpha"));
        assert_eq!(
            registry.get("alpha").unwrap().descriptor(),
            descriptor("alpha")
        );
        assert_eq!(
            registry
                .register(StaticExtension::new(descriptor("alpha")))
                .unwrap_err(),
            ExtensionError::DuplicateId
        );

        let mut full = ExtensionRegistry::new();
        for index in 0..MAX_IN_TREE_EXTENSIONS {
            full.register(StaticExtension::new(descriptor(&format!(
                "extension-{index}"
            ))))
            .unwrap();
        }
        assert_eq!(full.len(), MAX_IN_TREE_EXTENSIONS);
        assert_eq!(
            full.register(StaticExtension::new(descriptor("one-too-many")))
                .unwrap_err(),
            ExtensionError::Capacity
        );
    }
}
