//! Versioned role-based physical storage layout policy.

use std::{collections::BTreeMap, fmt, str::FromStr};

use crate::{BorsukError, DurableTableFormat, PhysicalObjectRole, Result, VectorElementType};

/// Third production layout-policy schema, including graph-table placement and
/// policy-selected Parquet/Vortex WAL runs.
pub const CURRENT_LAYOUT_POLICY_VERSION: u32 = 3;
/// Integrity chunk used for independently authenticated range reads.
pub const RANGE_INTEGRITY_CHUNK_BYTES: usize = 1024 * 1024;
/// Lower row bound retained for the explicitly selected WAL Vortex experiment.
pub const WAL_VORTEX_CANDIDATE_MIN_ROWS: usize = 500;
/// Lower dimensionality bound retained for the explicitly selected WAL Vortex experiment.
pub const WAL_VORTEX_CANDIDATE_MIN_DIMENSIONS: usize = 64;
/// Primary types retained by the local screen for the rejected v5 AWS
/// qualification. The rule remains available only for explicit experiments.
pub const WAL_VORTEX_CANDIDATE_ELEMENT_TYPES: [VectorElementType; 1] = [VectorElementType::Float32];

/// Physical codec selected for one immutable persisted object.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "kebab-case")]
pub enum PhysicalFormat {
    /// Apache Parquet.
    Parquet,
    /// Vortex file format.
    Vortex,
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
            Self::Vortex => "vortex",
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
            Self::Parquet | Self::Vortex => self.as_str(),
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
            "vortex" => Ok(Self::Vortex),
            "arrow" | "arrow-ipc" => Ok(Self::ArrowIpc),
            "bin" | "binary" | "packed" => Ok(Self::Packed),
            _ => Err(BorsukError::InvalidStorage(format!(
                "unknown physical format `{value}`"
            ))),
        }
    }
}

impl From<DurableTableFormat> for PhysicalFormat {
    fn from(value: DurableTableFormat) -> Self {
        match value {
            DurableTableFormat::Parquet => Self::Parquet,
            DurableTableFormat::Vortex => Self::Vortex,
        }
    }
}

impl TryFrom<PhysicalFormat> for DurableTableFormat {
    type Error = BorsukError;

    fn try_from(value: PhysicalFormat) -> Result<Self> {
        match value {
            PhysicalFormat::Parquet => Ok(Self::Parquet),
            PhysicalFormat::Vortex => Ok(Self::Vortex),
            other => Err(BorsukError::InvalidStorage(format!(
                "normal segment cannot use physical format `{other}`"
            ))),
        }
    }
}

/// Whether a policy always uses its role default or evaluates checked rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum PhysicalLayoutPolicyKind {
    /// One persisted format per role.
    Fixed,
    /// Versioned, deterministic rules may specialize a role by row count.
    Adaptive,
}

/// Write-time facts available without encoding the object twice.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PhysicalLayoutContext {
    /// Logical rows stored in the object.
    pub rows: usize,
    /// Primary vector dimensionality, or zero for non-vector objects.
    pub dimensions: usize,
    /// Declared primary vector type when the object contains primary vectors.
    pub vector_element_type: Option<VectorElementType>,
}

/// Deterministic adaptive rule. The matching rule with the largest
/// `(minimum_rows, minimum_dimensions, type-specificity)` tuple wins.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PhysicalLayoutRule {
    /// Object family governed by this rule.
    pub object_role: PhysicalObjectRole,
    /// Inclusive lower logical-row bound.
    pub minimum_rows: usize,
    /// Inclusive lower primary-vector dimensionality.
    pub minimum_dimensions: usize,
    /// Optional primary-vector type allowlist. Empty means every type.
    pub vector_element_types: Vec<VectorElementType>,
    /// Resolved physical codec.
    pub physical_format: PhysicalFormat,
}

/// Versioned role registry persisted in every collection catalog.
///
/// Policy v3 dispatches normal-segment and WAL-run writers. Other entries
/// reserve the production role namespace and reject unsupported proposals;
/// their writers emit the fixed formats listed in the checked storage-object
/// inventory.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PhysicalLayoutPolicy {
    /// Policy schema and qualification version.
    pub version: u32,
    /// Fixed or adaptive resolution.
    pub kind: PhysicalLayoutPolicyKind,
    /// Required registry entry for every production object role.
    pub role_defaults: BTreeMap<PhysicalObjectRole, PhysicalFormat>,
    /// Optional checked adaptive rules.
    pub rules: Vec<PhysicalLayoutRule>,
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
    /// normal-segment and v5 WAL AWS qualifications rejected every Vortex
    /// promotion, so this deliberately returns the all-Parquet baseline for
    /// WAL and normal-segment tables.
    #[must_use]
    pub fn production_default() -> Self {
        Self::production_baseline()
    }

    /// Current registry baseline. Normal segments and WAL runs are
    /// writer-governed in policy v3; other wire formats remain owned by their
    /// fixed writers.
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
            kind: PhysicalLayoutPolicyKind::Fixed,
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
            rules: Vec::new(),
        }
    }

    /// Set one role's baseline codec.
    #[must_use]
    pub fn with_role_format(mut self, role: PhysicalObjectRole, format: PhysicalFormat) -> Self {
        self.role_defaults.insert(role, format);
        self
    }

    /// Add a deterministic row-count rule and make the policy adaptive.
    #[must_use]
    pub fn with_minimum_rows_rule(
        mut self,
        role: PhysicalObjectRole,
        minimum_rows: usize,
        format: PhysicalFormat,
    ) -> Self {
        self.kind = PhysicalLayoutPolicyKind::Adaptive;
        self.rules.push(PhysicalLayoutRule {
            object_role: role,
            minimum_rows,
            minimum_dimensions: 0,
            vector_element_types: Vec::new(),
            physical_format: format,
        });
        self
    }

    /// Add a deterministic rule constrained by vector shape and element type.
    #[must_use]
    pub fn with_vector_characteristics_rule<I>(
        mut self,
        role: PhysicalObjectRole,
        minimum_rows: usize,
        minimum_dimensions: usize,
        vector_element_types: I,
        format: PhysicalFormat,
    ) -> Self
    where
        I: IntoIterator<Item = VectorElementType>,
    {
        self.kind = PhysicalLayoutPolicyKind::Adaptive;
        self.rules.push(PhysicalLayoutRule {
            object_role: role,
            minimum_rows,
            minimum_dimensions,
            vector_element_types: vector_element_types.into_iter().collect(),
            physical_format: format,
        });
        self
    }

    /// Add the measured compact-Vortex WAL experiment without changing any
    /// other production placement.
    ///
    /// The frozen v5 AWS qualification rejected this rule. It remains opt-in
    /// for research and is intentionally absent from [`Self::production_default`].
    #[must_use]
    pub fn with_wal_vortex_candidate(self) -> Self {
        self.with_vector_characteristics_rule(
            PhysicalObjectRole::WalRun,
            WAL_VORTEX_CANDIDATE_MIN_ROWS,
            WAL_VORTEX_CANDIDATE_MIN_DIMENSIONS,
            WAL_VORTEX_CANDIDATE_ELEMENT_TYPES,
            PhysicalFormat::Vortex,
        )
    }

    /// Validate completeness and deterministic rule structure.
    pub fn validate(&self) -> Result<()> {
        if self.version == 0 {
            return Err(BorsukError::InvalidStorage(
                "physical layout policy version must be non-zero".to_string(),
            ));
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
            || self
                .rules
                .iter()
                .any(|rule| rule.object_role == PhysicalObjectRole::Unknown)
        {
            return Err(BorsukError::InvalidStorage(
                "physical layout policy cannot classify the unknown role".to_string(),
            ));
        }
        if self.kind == PhysicalLayoutPolicyKind::Fixed && !self.rules.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "fixed physical layout policy cannot contain adaptive rules".to_string(),
            ));
        }
        for (&role, &format) in &self.role_defaults {
            validate_implemented_role_format(role, format)?;
        }
        for rule in &self.rules {
            validate_implemented_role_format(rule.object_role, rule.physical_format)?;
            if !rule.vector_element_types.is_empty()
                && !matches!(
                    rule.object_role,
                    PhysicalObjectRole::WalRun | PhysicalObjectRole::NormalSegment
                )
            {
                return Err(BorsukError::InvalidStorage(format!(
                    "physical layout type selectors are not implemented for object role `{}`",
                    rule.object_role.as_str()
                )));
            }
            if rule
                .vector_element_types
                .iter()
                .enumerate()
                .any(|(index, element_type)| {
                    rule.vector_element_types[..index].contains(element_type)
                })
            {
                return Err(BorsukError::InvalidStorage(format!(
                    "physical layout rule for `{}` repeats a vector element type",
                    rule.object_role.as_str()
                )));
            }
        }
        for (index, left) in self.rules.iter().enumerate() {
            for right in self.rules.iter().skip(index + 1) {
                if left.object_role == right.object_role
                    && left.minimum_rows == right.minimum_rows
                    && left.minimum_dimensions == right.minimum_dimensions
                    && left.physical_format != right.physical_format
                    && type_selectors_overlap(
                        &left.vector_element_types,
                        &right.vector_element_types,
                    )
                {
                    return Err(BorsukError::InvalidStorage(format!(
                        "physical layout rules for `{}` are ambiguous at rows={} dimensions={}",
                        left.object_role.as_str(),
                        left.minimum_rows,
                        left.minimum_dimensions
                    )));
                }
            }
        }
        Ok(())
    }

    /// Resolve one object's codec at write time.
    pub fn resolve(
        &self,
        role: PhysicalObjectRole,
        context: PhysicalLayoutContext,
    ) -> Result<PhysicalFormat> {
        self.validate()?;
        let baseline = *self.role_defaults.get(&role).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "physical layout policy cannot resolve `{}`",
                role.as_str()
            ))
        })?;
        if self.kind == PhysicalLayoutPolicyKind::Fixed {
            return Ok(baseline);
        }
        Ok(self
            .rules
            .iter()
            .filter(|rule| {
                rule.object_role == role
                    && rule.minimum_rows <= context.rows
                    && rule.minimum_dimensions <= context.dimensions
                    && (rule.vector_element_types.is_empty()
                        || context.vector_element_type.is_some_and(|element_type| {
                            rule.vector_element_types.contains(&element_type)
                        }))
            })
            .max_by_key(|rule| {
                (
                    rule.minimum_rows,
                    rule.minimum_dimensions,
                    usize::from(!rule.vector_element_types.is_empty()),
                )
            })
            .map_or(baseline, |rule| rule.physical_format))
    }
}

fn type_selectors_overlap(left: &[VectorElementType], right: &[VectorElementType]) -> bool {
    left.is_empty()
        || right.is_empty()
        || left.iter().any(|element_type| right.contains(element_type))
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
    /// Byte width of independently checksummed object chunks.
    pub integrity_chunk_bytes: usize,
    /// BLAKE3 hashes, in byte order, covering the complete object.
    pub integrity_checksums: Vec<String>,
}

impl PhysicalLayoutRef {
    /// Resolve and construct one persisted reference.
    pub fn resolve(
        policy: &PhysicalLayoutPolicy,
        role: PhysicalObjectRole,
        context: PhysicalLayoutContext,
    ) -> Result<Self> {
        Ok(Self {
            object_role: role,
            physical_format: policy.resolve(role, context)?,
            layout_policy_version: policy.version,
            integrity_chunk_bytes: 0,
            integrity_checksums: Vec::new(),
        })
    }

    /// Attach complete chunk integrity after the writer has encoded the object.
    #[must_use]
    pub fn with_integrity(mut self, bytes: &[u8]) -> Self {
        self.integrity_chunk_bytes = RANGE_INTEGRITY_CHUNK_BYTES;
        self.integrity_checksums = bytes
            .chunks(RANGE_INTEGRITY_CHUNK_BYTES)
            .map(|chunk| blake3::hash(chunk).to_hex().to_string())
            .collect();
        self
    }

    /// Validate one complete stored integrity chunk.
    pub(crate) fn verify_integrity_chunk(
        &self,
        object_path: &str,
        index: usize,
        bytes: &[u8],
    ) -> Result<()> {
        if self.integrity_chunk_bytes == 0 || self.integrity_checksums.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "range-readable object reference has no chunk integrity".to_string(),
            ));
        }
        if bytes.len() > self.integrity_chunk_bytes {
            return Err(BorsukError::InvalidStorage(
                "range integrity chunk exceeds its declared width".to_string(),
            ));
        }
        let expected = self.integrity_checksums.get(index).ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "range integrity has no checksum for chunk {index}"
            ))
        })?;
        let actual = blake3::hash(bytes).to_hex().to_string();
        if &actual != expected {
            return Err(BorsukError::ChecksumMismatch {
                path: format!("{object_path}#chunk-{index}"),
                expected: expected.clone(),
                actual,
            });
        }
        Ok(())
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
        if self.layout_policy_version == 0 {
            return Err(BorsukError::InvalidStorage(
                "object reference has zero layout-policy version".to_string(),
            ));
        }
        if self.integrity_chunk_bytes == 0 || self.integrity_checksums.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "object reference is missing range integrity".to_string(),
            ));
        }
        Ok(())
    }
}

fn validate_implemented_role_format(
    role: PhysicalObjectRole,
    format: PhysicalFormat,
) -> Result<()> {
    use PhysicalFormat::{ArrowIpc, Packed, Parquet, Vortex};
    use PhysicalObjectRole::{
        Catalog, CommitMarker, ExactVectors, FilterIndex, GraphIndex, IdDirectory, LaneHead,
        LateInteraction, LexicalBlock, NormalSegment, ProductCodes, RoutingPage, Tombstone, WalRun,
        WriterDirectory,
    };
    let supported = match role {
        Catalog | RoutingPage | GraphIndex | LexicalBlock | Tombstone => format == Parquet,
        WalRun => matches!(format, Parquet | Vortex),
        LaneHead | WriterDirectory | CommitMarker | FilterIndex | IdDirectory => format == Packed,
        ExactVectors | ProductCodes | LateInteraction => format == ArrowIpc,
        NormalSegment => matches!(format, Parquet | Vortex),
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
