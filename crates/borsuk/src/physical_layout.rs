//! Versioned role-based physical storage layout policy.

use std::{collections::BTreeMap, fmt, str::FromStr};

use crate::{BorsukError, PhysicalObjectRole, Result};

/// Fourth production layout-policy schema. Durable table roles are Parquet;
/// Arrow IPC and packed roles retain their fixed standard encodings.
pub const CURRENT_LAYOUT_POLICY_VERSION: u32 = 4;
/// Physical codec selected for one immutable persisted object.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum PhysicalFormat {
    /// Apache Parquet.
    Parquet,
    /// Apache Arrow IPC file format.
    ArrowIpc,
    /// BORSUK fixed-header packed bytes.
    Packed,
}

impl PhysicalFormat {
    /// Stable persisted name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Parquet => "parquet",
            Self::ArrowIpc => "arrow-ipc",
            Self::Packed => "packed",
        }
    }

    /// Conventional extension for a content-addressed object.
    #[must_use]
    pub const fn extension(self) -> &'static str {
        match self {
            Self::ArrowIpc => "arrow",
            Self::Packed => "bin",
            Self::Parquet => self.as_str(),
        }
    }
}

impl fmt::Display for PhysicalFormat {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for PhysicalFormat {
    type Err = BorsukError;

    fn from_str(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "parquet" => Ok(Self::Parquet),
            "arrow" | "arrow-ipc" => Ok(Self::ArrowIpc),
            "bin" | "binary" | "packed" => Ok(Self::Packed),
            _ => Err(BorsukError::InvalidStorage(format!(
                "unknown physical format `{value}`"
            ))),
        }
    }
}

/// Versioned role registry persisted in every collection catalog.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PhysicalLayoutPolicy {
    /// Policy schema and qualification version.
    pub version: u32,
    /// Required registry entry for every production object role.
    pub role_defaults: BTreeMap<PhysicalObjectRole, PhysicalFormat>,
}

impl Default for PhysicalLayoutPolicy {
    fn default() -> Self {
        Self::production_default()
    }
}

impl PhysicalLayoutPolicy {
    /// Frozen automatic production default for newly created indexes.
    ///
    /// Callers do not provide a collection cardinality estimate. Writers resolve
    /// every immutable object from its actual row count, vector dimensionality,
    /// and element type at the moment the object is persisted. The frozen
    /// normal-segment and WAL tables use Parquet.
    #[must_use]
    pub fn production_default() -> Self {
        Self::production_baseline()
    }

    /// Current registry baseline. Standard table and sidecar formats are owned
    /// by their fixed writers.
    #[must_use]
    pub fn production_baseline() -> Self {
        use PhysicalFormat::{ArrowIpc, Packed, Parquet};
        use PhysicalObjectRole::{
            Catalog, CommitMarker, ExactVectors, FilterIndex, GraphIndex, IdDirectory, LaneHead,
            LateInteraction, LexicalBlock, NormalSegment, ProductCodes, RoutingPage, Tombstone,
            WalRun,
        };
        Self {
            version: CURRENT_LAYOUT_POLICY_VERSION,
            role_defaults: BTreeMap::from([
                (Catalog, Parquet),
                (WalRun, Parquet),
                (LaneHead, Packed),
                (CommitMarker, Packed),
                (RoutingPage, Parquet),
                (GraphIndex, Parquet),
                (NormalSegment, Parquet),
                (ProductCodes, ArrowIpc),
                (ExactVectors, ArrowIpc),
                (FilterIndex, Packed),
                (LexicalBlock, Parquet),
                (LateInteraction, ArrowIpc),
                (Tombstone, Parquet),
                (IdDirectory, Packed),
            ]),
        }
    }

    /// Validate completeness and deterministic rule structure.
    pub fn validate(&self) -> Result<()> {
        if self.version != CURRENT_LAYOUT_POLICY_VERSION {
            return Err(BorsukError::InvalidStorage(format!(
                "unsupported physical layout policy version {}; expected {}",
                self.version, CURRENT_LAYOUT_POLICY_VERSION
            )));
        }
        for role in production_object_roles() {
            if !self.role_defaults.contains_key(role) {
                return Err(BorsukError::InvalidStorage(format!(
                    "physical layout policy has no default for `{}`",
                    role.as_str()
                )));
            }
        }
        if self
            .role_defaults
            .contains_key(&PhysicalObjectRole::Unknown)
        {
            return Err(BorsukError::InvalidStorage(
                "physical layout policy cannot classify the unknown role".to_string(),
            ));
        }
        for (&role, &format) in &self.role_defaults {
            validate_implemented_role_format(role, format)?;
        }
        Ok(())
    }

    /// Resolve one object's codec at write time.
    pub fn resolve(&self, role: PhysicalObjectRole) -> Result<PhysicalFormat> {
        self.validate()?;
        self.role_defaults.get(&role).copied().ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "physical layout policy cannot resolve `{}`",
                role.as_str()
            ))
        })
    }
}

/// Persisted resolution attached to an immutable object reference.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PhysicalLayoutRef {
    /// Logical object role.
    pub object_role: PhysicalObjectRole,
    /// Codec selected by the writer.
    pub physical_format: PhysicalFormat,
    /// Policy version that selected the codec.
    pub layout_policy_version: u32,
}

impl PhysicalLayoutRef {
    /// Resolve and construct one persisted reference.
    pub fn resolve(policy: &PhysicalLayoutPolicy, role: PhysicalObjectRole) -> Result<Self> {
        Ok(Self {
            object_role: role,
            physical_format: policy.resolve(role)?,
            layout_policy_version: policy.version,
        })
    }

    /// Validate the reference is suitable for the expected reader.
    pub fn validate_for(&self, expected_role: PhysicalObjectRole) -> Result<()> {
        if self.object_role != expected_role {
            return Err(BorsukError::InvalidStorage(format!(
                "object reference declares role `{}` but reader requires `{}`",
                self.object_role.as_str(),
                expected_role.as_str()
            )));
        }
        if self.layout_policy_version != CURRENT_LAYOUT_POLICY_VERSION {
            return Err(BorsukError::InvalidStorage(format!(
                "unsupported object layout-policy version {}; expected {}",
                self.layout_policy_version, CURRENT_LAYOUT_POLICY_VERSION
            )));
        }
        validate_implemented_role_format(expected_role, self.physical_format)?;
        Ok(())
    }
}

fn validate_implemented_role_format(
    role: PhysicalObjectRole,
    format: PhysicalFormat,
) -> Result<()> {
    use PhysicalFormat::{ArrowIpc, Packed, Parquet};
    use PhysicalObjectRole::{
        Catalog, CommitMarker, ExactVectors, FilterIndex, GraphIndex, IdDirectory, LaneHead,
        LateInteraction, LexicalBlock, NormalSegment, ProductCodes, RoutingPage, Tombstone, WalRun,
        WriterDirectory,
    };
    let supported = match role {
        Catalog | RoutingPage | GraphIndex | LexicalBlock | Tombstone => format == Parquet,
        WalRun => format == Parquet,
        LaneHead | WriterDirectory | CommitMarker | FilterIndex | IdDirectory => format == Packed,
        ExactVectors | ProductCodes | LateInteraction => format == ArrowIpc,
        NormalSegment => format == Parquet,
        PhysicalObjectRole::Unknown => false,
    };
    if !supported {
        return Err(BorsukError::InvalidStorage(format!(
            "physical format `{format}` is not implemented for object role `{}`",
            role.as_str()
        )));
    }
    Ok(())
}

/// All roles that must have a qualified physical placement.
#[must_use]
pub fn production_object_roles() -> &'static [PhysicalObjectRole] {
    use PhysicalObjectRole::{
        Catalog, CommitMarker, ExactVectors, FilterIndex, GraphIndex, IdDirectory, LaneHead,
        LateInteraction, LexicalBlock, NormalSegment, ProductCodes, RoutingPage, Tombstone, WalRun,
    };
    &[
        Catalog,
        WalRun,
        LaneHead,
        CommitMarker,
        RoutingPage,
        GraphIndex,
        NormalSegment,
        ProductCodes,
        ExactVectors,
        FilterIndex,
        LexicalBlock,
        LateInteraction,
        Tombstone,
        IdDirectory,
    ]
}
