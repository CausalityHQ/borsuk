//! Pure, unwired assembly for one positioned-source materialization candidate.
//!
//! Task 4 owns publication. These values authenticate immutable artifacts and
//! exact source coverage without mutating a manifest or source checkpoint.

// This module is intentionally production-shaped but unwired: Task 3 builds
// and authenticates candidates, while Task 4 will make the first production
// call at the collection CAS boundary.
#![allow(
    dead_code,
    reason = "Task 3 constructs authenticated candidates; Task 4 will wire their CAS consumer"
)]

use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Range,
    sync::Arc,
    time::Duration,
};

use arrow_array::{
    ArrayRef, BinaryArray, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, ListArray,
    RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
    types::{Float32Type, UInt32Type},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;

use crate::{
    BorsukError, Result,
    format::{
        PositionedRouteAssignment, PositionedRouteAssignmentKind, PositionedRoutePlanRow,
        PositionedRouteProjectionKind,
    },
    global_leaf_run::{
        GlobalLeafArtifactRef, GlobalLeafArtifactRole, PrimaryDenseArtifactCandidate,
    },
    manifest::SegmentLexicalShardRef,
    mutation::MutationState,
    positioned_log::CommitSourceRangeSet,
    row_bundle::{
        ArtifactRef, CanonicalRowBatch, DirectoryPackOptions, DirectoryPartition, DirectoryRow,
        OpenedRowBundleGeneration, RowBundleObjectSink, RowBundlePackOptions,
        pack_canonical_row_bundles_to_sink, pack_directory_partition_run, pack_directory_root,
        stage_existing_row_bundle_generation_to_sink,
    },
    storage::Storage,
};

const ROW_BUNDLE_MODALITY: &str = "@row-bundle";

#[derive(Clone, Debug)]
pub(crate) enum MaterializedRowValue {
    Dense(Vec<f32>),
    Sparse(Vec<(u32, f32)>),
    Text(Vec<(u32, u32)>),
}

#[derive(Clone, Debug)]
pub(crate) struct MaterializedRow {
    pub(crate) modality: String,
    pub(crate) projection_kind: PositionedRouteProjectionKind,
    pub(crate) assignment: PositionedRouteAssignment,
    pub(crate) cell_ordinal: Option<u32>,
    pub(crate) record_id: Vec<u8>,
    pub(crate) projected_ordinal: u32,
    pub(crate) state: MutationState,
    pub(crate) value: MaterializedRowValue,
}

#[derive(Clone, Debug)]
pub(crate) struct MaterializedDirectoryState {
    pub(crate) record_id: Vec<u8>,
    pub(crate) routing_epoch: u64,
    pub(crate) cell_ordinal: u32,
    pub(crate) state: MutationState,
}

#[derive(Debug)]
pub(crate) struct BuiltRowBundleCandidate {
    pub(crate) delta_root: Option<ArtifactRef>,
    pub(crate) roster: Vec<MaterializationArtifactRef>,
}

struct CandidateRowBundleSink<'a> {
    storage: &'a Storage,
    emitted: Vec<ArtifactRef>,
}

impl RowBundleObjectSink for CandidateRowBundleSink<'_> {
    fn emit(&mut self, artifact: &ArtifactRef, bytes: Bytes) -> Result<()> {
        self.storage
            .write_bytes_content_addressed(&artifact.path, &bytes)?;
        self.emitted.push(artifact.clone());
        Ok(())
    }
}

fn projection_kind_code(kind: PositionedRouteProjectionKind) -> u8 {
    match kind {
        PositionedRouteProjectionKind::Primary => 0,
        PositionedRouteProjectionKind::Dense => 1,
        PositionedRouteProjectionKind::Sparse => 2,
        PositionedRouteProjectionKind::Text => 3,
        PositionedRouteProjectionKind::LateInteraction => 4,
    }
}

fn assignment_kind_code(kind: PositionedRouteAssignmentKind) -> u8 {
    match kind {
        PositionedRouteAssignmentKind::Catalog => 0,
        PositionedRouteAssignmentKind::Analyzer => 1,
    }
}

fn canonical_system_fields() -> Vec<Field> {
    vec![
        Field::new("row_bundle_format", DataType::UInt16, false),
        Field::new("modality", DataType::Utf8, false),
        Field::new("projection_kind", DataType::UInt8, false),
        Field::new("assignment_kind", DataType::UInt8, false),
        Field::new("assignment_checksum", DataType::FixedSizeBinary(32), false),
        Field::new("routing_epoch", DataType::UInt64, true),
        Field::new("cell_ordinal", DataType::UInt32, true),
        Field::new("record_id", DataType::Binary, false),
        Field::new("projected_ordinal", DataType::UInt32, false),
        Field::new("mutation_hlc", DataType::UInt64, false),
        Field::new("mutation_writer", DataType::FixedSizeBinary(16), false),
        Field::new("mutation_digest", DataType::FixedSizeBinary(32), false),
    ]
}

fn canonical_system_arrays(rows: &[MaterializedRow]) -> Result<Vec<ArrayRef>> {
    Ok(vec![
        Arc::new(UInt16Array::from_value(1, rows.len())),
        Arc::new(StringArray::from_iter_values(
            rows.iter().map(|row| row.modality.as_str()),
        )),
        Arc::new(UInt8Array::from_iter_values(
            rows.iter()
                .map(|row| projection_kind_code(row.projection_kind)),
        )),
        Arc::new(UInt8Array::from_iter_values(
            rows.iter()
                .map(|row| assignment_kind_code(row.assignment.kind)),
        )),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            rows.iter().map(|row| row.assignment.checksum),
        )?),
        Arc::new(UInt64Array::from_iter(
            rows.iter().map(|row| row.assignment.routing_epoch),
        )),
        Arc::new(UInt32Array::from_iter(
            rows.iter().map(|row| row.cell_ordinal),
        )),
        Arc::new(BinaryArray::from_iter_values(
            rows.iter().map(|row| row.record_id.as_slice()),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.projected_ordinal),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.state.stamp().version().hlc()),
        )),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            rows.iter().map(|row| row.state.stamp().version().writer()),
        )?),
        Arc::new(FixedSizeBinaryArray::try_from_iter(
            rows.iter().map(|row| row.state.stamp().digest()),
        )?),
    ])
}

fn canonical_batch(rows: &[MaterializedRow]) -> Result<CanonicalRowBatch> {
    let mut fields = canonical_system_fields();
    let mut arrays = canonical_system_arrays(rows)?;
    match &rows[0].value {
        MaterializedRowValue::Dense(first) => {
            let dimensions = i32::try_from(first.len()).map_err(|_| {
                BorsukError::InvalidStorage("row-bundle dense dimensions exceed i32".to_owned())
            })?;
            if dimensions == 0
                || rows.iter().any(|row| {
                    !matches!(&row.value, MaterializedRowValue::Dense(vector) if vector.len() == first.len())
                })
            {
                return Err(BorsukError::InvalidStorage(
                    "row-bundle dense values disagree on dimensions".to_owned(),
                ));
            }
            let values = rows
                .iter()
                .flat_map(|row| match &row.value {
                    MaterializedRowValue::Dense(vector) => vector.iter().copied(),
                    _ => unreachable!("dense modality checked"),
                })
                .collect::<Vec<_>>();
            let child = Arc::new(Field::new("item", DataType::Float32, false));
            fields.push(Field::new(
                "dense_vector",
                DataType::FixedSizeList(Arc::clone(&child), dimensions),
                false,
            ));
            arrays.push(Arc::new(FixedSizeListArray::try_new(
                child,
                dimensions,
                Arc::new(Float32Array::from(values)),
                None,
            )?));
        }
        MaterializedRowValue::Sparse(_) => {
            if rows
                .iter()
                .any(|row| !matches!(row.value, MaterializedRowValue::Sparse(_)))
            {
                return Err(BorsukError::InvalidStorage(
                    "row-bundle sparse modality mixes value kinds".to_owned(),
                ));
            }
            fields.push(Field::new(
                "sparse_indices",
                DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
                false,
            ));
            fields.push(Field::new(
                "sparse_values",
                DataType::List(Arc::new(Field::new_list_field(DataType::Float32, true))),
                false,
            ));
            arrays.push(Arc::new(
                ListArray::from_iter_primitive::<UInt32Type, _, _>(rows.iter().map(|row| {
                    match &row.value {
                        MaterializedRowValue::Sparse(terms) => Some(
                            terms
                                .iter()
                                .map(|(term, _)| Some(*term))
                                .collect::<Vec<_>>(),
                        ),
                        _ => unreachable!("sparse modality checked"),
                    }
                })),
            ));
            arrays.push(Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(rows.iter().map(|row| {
                    match &row.value {
                        MaterializedRowValue::Sparse(terms) => Some(
                            terms
                                .iter()
                                .map(|(_, value)| Some(*value))
                                .collect::<Vec<_>>(),
                        ),
                        _ => unreachable!("sparse modality checked"),
                    }
                })),
            ));
        }
        MaterializedRowValue::Text(_) => {
            if rows
                .iter()
                .any(|row| !matches!(row.value, MaterializedRowValue::Text(_)))
            {
                return Err(BorsukError::InvalidStorage(
                    "row-bundle text modality mixes value kinds".to_owned(),
                ));
            }
            for (field_name, values_are_terms) in
                [("text_term_ids", true), ("text_term_frequencies", false)]
            {
                fields.push(Field::new(
                    field_name,
                    DataType::List(Arc::new(Field::new_list_field(DataType::UInt32, true))),
                    false,
                ));
                arrays.push(Arc::new(
                    ListArray::from_iter_primitive::<UInt32Type, _, _>(rows.iter().map(|row| {
                        match &row.value {
                            MaterializedRowValue::Text(terms) => Some(
                                terms
                                    .iter()
                                    .map(|(term, frequency)| {
                                        Some(if values_are_terms { *term } else { *frequency })
                                    })
                                    .collect::<Vec<_>>(),
                            ),
                            _ => unreachable!("text modality checked"),
                        }
                    })),
                ));
            }
        }
    }
    CanonicalRowBatch::try_new(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
}

fn row_bundle_role(path: &str) -> Result<MaterializationArtifactRole> {
    let role = if path.starts_with("row-bundles/") {
        MaterializationArtifactRole::RowBundle
    } else if path.starts_with("row-bundle-summary-shards/") {
        MaterializationArtifactRole::RowBundleSummaryShard
    } else if path.starts_with("row-bundle-rosters/") {
        MaterializationArtifactRole::RowBundleRoster
    } else if path.starts_with("row-bundle-run-roots/") {
        MaterializationArtifactRole::RowBundleRunRoot
    } else if path.starts_with("id-directory/") || path.starts_with("id-directory-roots/") {
        MaterializationArtifactRole::RowBundleDirectory
    } else if path.starts_with("row-bundle-generation-roots/") {
        MaterializationArtifactRole::RowBundleGenerationRoot
    } else {
        return Err(BorsukError::InvalidStorage(format!(
            "unknown row-bundle artifact path `{path}`"
        )));
    };
    Ok(role)
}

pub(crate) fn build_row_bundle_delta(
    storage: &Storage,
    mut rows: Vec<MaterializedRow>,
    directory: Vec<MaterializedDirectoryState>,
    levels: &MaterializationLevelReservation,
) -> Result<BuiltRowBundleCandidate> {
    if rows.is_empty() != directory.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "row-bundle rows and directory authority must both be empty or nonempty".to_owned(),
        ));
    }
    if rows.is_empty() {
        return Ok(BuiltRowBundleCandidate {
            delta_root: None,
            roster: Vec::new(),
        });
    }
    let row_level = levels.row_level.ok_or_else(|| {
        BorsukError::InvalidStorage(
            "row-bundle delta has rows without a reserved row level".to_owned(),
        )
    })?;
    rows.sort_by(|left, right| {
        left.modality
            .cmp(&right.modality)
            .then_with(|| {
                projection_kind_code(left.projection_kind)
                    .cmp(&projection_kind_code(right.projection_kind))
            })
            .then_with(|| {
                assignment_kind_code(left.assignment.kind)
                    .cmp(&assignment_kind_code(right.assignment.kind))
            })
            .then_with(|| left.assignment.checksum.cmp(&right.assignment.checksum))
            .then_with(|| {
                left.assignment
                    .routing_epoch
                    .cmp(&right.assignment.routing_epoch)
            })
            .then_with(|| left.cell_ordinal.cmp(&right.cell_ordinal))
            .then_with(|| left.record_id.cmp(&right.record_id))
            .then_with(|| left.projected_ordinal.cmp(&right.projected_ordinal))
            .then_with(|| {
                left.state
                    .stamp()
                    .version()
                    .cmp(&right.state.stamp().version())
            })
    });
    let mut route_plan = Vec::new();
    let mut batches = Vec::new();
    if let Some(summary_stamp) = rows
        .iter()
        .map(|row| row.state.stamp())
        .min_by_key(|stamp| stamp.version())
    {
        let mut start = 0;
        while start < rows.len() {
            let modality = rows[start].modality.clone();
            let end = rows[start..].partition_point(|row| row.modality == modality) + start;
            let group = &rows[start..end];
            let first = &group[0];
            if group.iter().any(|row| {
                row.projection_kind != first.projection_kind || row.assignment != first.assignment
            }) {
                return Err(BorsukError::InvalidStorage(
                    "one materialized modality changed route authority".to_owned(),
                ));
            }
            route_plan.push(PositionedRoutePlanRow::summary(
                &modality,
                first.projection_kind,
                first.assignment.clone(),
                group.len() as u64,
                summary_stamp,
            )?);
            let mut route_rows = group.iter().collect::<Vec<_>>();
            route_rows.sort_by(|left, right| {
                left.record_id
                    .cmp(&right.record_id)
                    .then_with(|| left.projected_ordinal.cmp(&right.projected_ordinal))
            });
            for row in route_rows {
                route_plan.push(match row.cell_ordinal {
                    Some(cell) => PositionedRoutePlanRow::routed(
                        row.record_id.clone(),
                        &row.modality,
                        row.projection_kind,
                        row.projected_ordinal,
                        row.assignment.clone(),
                        cell,
                        row.state.stamp(),
                    )?,
                    None => PositionedRoutePlanRow::term_partitioned(
                        row.record_id.clone(),
                        &row.modality,
                        row.projection_kind,
                        row.projected_ordinal,
                        row.assignment.clone(),
                        row.state.stamp(),
                    )?,
                });
            }
            batches.push(canonical_batch(group)?);
            start = end;
        }
    }

    let mut sink = CandidateRowBundleSink {
        storage,
        emitted: Vec::new(),
    };
    let packed = pack_canonical_row_bundles_to_sink(
        &batches,
        &route_plan,
        row_level,
        RowBundlePackOptions::production(),
        &mut sink,
    )?;
    let mut partitions = BTreeMap::<DirectoryPartition, Vec<DirectoryRow>>::new();
    for state in directory {
        partitions
            .entry(DirectoryPartition::for_record_id(&state.record_id))
            .or_default()
            .push(DirectoryRow {
                record_id: state.record_id,
                routing_epoch: state.routing_epoch,
                cell_ordinal: state.cell_ordinal,
                state: state.state,
            });
    }
    let mut directory_runs = Vec::new();
    for (partition, mut rows) in partitions {
        rows.sort_by(|left, right| left.record_id.cmp(&right.record_id));
        let directory_level = levels.directory_level(partition).ok_or_else(|| {
            BorsukError::InvalidStorage(
                "row-bundle delta has directory rows without a reserved partition level".to_owned(),
            )
        })?;
        let run = pack_directory_partition_run(
            partition,
            directory_level,
            &rows,
            DirectoryPackOptions::production(),
        )?;
        storage
            .write_bytes_content_addressed(run.reference.artifact().path.as_str(), &run.bytes)?;
        sink.emitted.push(run.reference.artifact().clone());
        directory_runs.push(run.reference);
    }
    let directory_root = pack_directory_root(&directory_runs)?;
    storage.write_bytes_content_addressed(&directory_root.reference.path, &directory_root.bytes)?;
    sink.emitted.push(directory_root.reference.clone());
    let active_runs = [packed.run_ref];
    let generation = stage_existing_row_bundle_generation_to_sink(
        &active_runs,
        &directory_root.reference,
        &mut sink,
    )?;
    let roster = sink
        .emitted
        .iter()
        .map(|artifact| {
            Ok(MaterializationArtifactRef::whole(
                "primary",
                ROW_BUNDLE_MODALITY,
                row_bundle_role(&artifact.path)?,
                artifact,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(BuiltRowBundleCandidate {
        delta_root: Some(generation.root.reference),
        roster,
    })
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum MaterializationArtifactRole {
    CodebookDescriptor,
    DenseDirectoryRoot,
    DenseDirectoryShard,
    DenseBundle,
    DenseCodePlane,
    RowBundle,
    RowBundleSummaryShard,
    RowBundleRoster,
    RowBundleRunRoot,
    RowBundleDirectory,
    RowBundleGenerationRoot,
    LexicalPostings,
    LexicalRows,
    LexicalShard,
    MutationFence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaterializationArtifactRef {
    storage_owner: String,
    modality: String,
    role: MaterializationArtifactRole,
    path: String,
    checksum: String,
    encoded_bytes: u64,
    range: Option<Range<u64>>,
}

impl MaterializationArtifactRef {
    pub(crate) fn whole(
        storage_owner: impl Into<String>,
        modality: impl Into<String>,
        role: MaterializationArtifactRole,
        artifact: &ArtifactRef,
    ) -> Self {
        Self {
            storage_owner: storage_owner.into(),
            modality: modality.into(),
            role,
            path: artifact.path.clone(),
            checksum: artifact.checksum.clone(),
            encoded_bytes: artifact.encoded_bytes,
            range: None,
        }
    }

    pub(crate) fn content(
        storage_owner: impl Into<String>,
        modality: impl Into<String>,
        role: MaterializationArtifactRole,
        path: String,
        checksum: String,
        encoded_bytes: u64,
    ) -> Self {
        Self {
            storage_owner: storage_owner.into(),
            modality: modality.into(),
            role,
            path,
            checksum,
            encoded_bytes,
            range: None,
        }
    }

    pub(crate) fn from_global(modality: &str, artifact: &GlobalLeafArtifactRef) -> Self {
        let role = match artifact.role {
            GlobalLeafArtifactRole::CodebookDescriptor => {
                MaterializationArtifactRole::CodebookDescriptor
            }
            GlobalLeafArtifactRole::LeafDirectoryRoot => {
                MaterializationArtifactRole::DenseDirectoryRoot
            }
            GlobalLeafArtifactRole::LeafDirectoryShard => {
                MaterializationArtifactRole::DenseDirectoryShard
            }
            GlobalLeafArtifactRole::LeafBundle => MaterializationArtifactRole::DenseBundle,
            GlobalLeafArtifactRole::LeafCodePlane => MaterializationArtifactRole::DenseCodePlane,
        };
        Self {
            storage_owner: modality.to_owned(),
            modality: modality.to_owned(),
            role,
            path: artifact.path.clone(),
            checksum: artifact.checksum.clone(),
            encoded_bytes: artifact.encoded_bytes,
            range: artifact.range.clone(),
        }
    }

    pub(crate) fn modality(&self) -> &str {
        &self.modality
    }

    pub(crate) fn storage_owner(&self) -> &str {
        &self.storage_owner
    }

    pub(crate) fn role(&self) -> &MaterializationArtifactRole {
        &self.role
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn checksum(&self) -> &str {
        &self.checksum
    }

    pub(crate) fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    pub(crate) fn range(&self) -> Option<Range<u64>> {
        self.range.clone()
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaterializationSourceTransfer {
    pub(crate) gets: u64,
    pub(crate) bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ModalityConstructionMetrics {
    pub(crate) cpu: Duration,
    pub(crate) encoded_bytes: u64,
    pub(crate) rows: u64,
    pub(crate) objects: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaterializationBaseAuthority {
    coverage: CommitSourceRangeSet,
    head_refs: Vec<MaterializationArtifactRef>,
}

impl MaterializationBaseAuthority {
    pub(crate) fn new(
        coverage: CommitSourceRangeSet,
        mut head_refs: Vec<MaterializationArtifactRef>,
    ) -> Result<Self> {
        coverage.validate_canonical()?;
        if coverage.ranges().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "materialization base coverage is empty".to_owned(),
            ));
        }
        for head in &head_refs {
            if head.storage_owner.is_empty()
                || head.modality.is_empty()
                || head.path.is_empty()
                || head.encoded_bytes == 0
                || blake3::Hash::from_hex(&head.checksum).is_err()
                || head.range.as_ref().is_some_and(|range| {
                    range.start >= range.end
                        || range.end.saturating_sub(range.start) != head.encoded_bytes
                })
            {
                return Err(BorsukError::InvalidStorage(
                    "materialization base contains a malformed artifact head".to_owned(),
                ));
            }
        }
        sort_and_validate_roster(&mut head_refs)?;
        Ok(Self {
            coverage,
            head_refs,
        })
    }

    pub(crate) fn head_refs(&self) -> &[MaterializationArtifactRef] {
        &self.head_refs
    }

    pub(crate) fn chain_anchor(&self) -> MaterializationChainAnchor {
        MaterializationChainAnchor {
            coverage: self.coverage.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaterializationChainAnchor {
    coverage: CommitSourceRangeSet,
}

impl MaterializationChainAnchor {
    pub(crate) fn new(coverage: CommitSourceRangeSet) -> Result<Self> {
        coverage.validate_canonical()?;
        if coverage.ranges().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "materialization chain anchor coverage is empty".to_owned(),
            ));
        }
        Ok(Self { coverage })
    }

    pub(crate) fn coverage(&self) -> &CommitSourceRangeSet {
        &self.coverage
    }

    fn advanced(&self, coverage: CommitSourceRangeSet) -> Result<Self> {
        coverage.validate_canonical()?;
        if coverage.ranges().is_empty() || !coverage.covers(&self.coverage) {
            return Err(BorsukError::InvalidStorage(
                "materialization chain anchor does not extend its prior coverage".to_owned(),
            ));
        }
        Ok(Self { coverage })
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaterializationLevelAuthority {
    row_levels: BTreeSet<u8>,
    directory_levels: BTreeMap<DirectoryPartition, BTreeSet<u8>>,
    dense_levels: BTreeMap<String, BTreeSet<u8>>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaterializationLevelReservation {
    row_level: Option<u8>,
    directory_levels: BTreeMap<DirectoryPartition, u8>,
    dense_levels: BTreeMap<String, u8>,
}

impl MaterializationLevelReservation {
    pub(crate) fn row_level(&self) -> Option<u8> {
        self.row_level
    }

    pub(crate) fn directory_level(&self, partition: DirectoryPartition) -> Option<u8> {
        self.directory_levels.get(&partition).copied()
    }

    pub(crate) fn dense_level(&self, modality: &str) -> Option<u8> {
        self.dense_levels.get(modality).copied()
    }
}

impl MaterializationLevelAuthority {
    pub(crate) fn new(
        row_levels: BTreeSet<u8>,
        directory_levels: BTreeMap<DirectoryPartition, BTreeSet<u8>>,
        dense_levels: BTreeMap<String, BTreeSet<u8>>,
    ) -> Result<Self> {
        if row_levels
            .iter()
            .any(|level| usize::from(*level) >= crate::row_bundle::MAX_ACTIVE_ROW_BUNDLE_LEVELS)
            || directory_levels
                .values()
                .flatten()
                .any(|level| usize::from(*level) >= crate::row_bundle::MAX_ACTIVE_DIRECTORY_LEVELS)
            || dense_levels
                .values()
                .flatten()
                .any(|level| usize::from(*level) >= crate::global_leaf_run::MAX_GLOBAL_LEAF_LEVELS)
            || dense_levels.keys().any(|modality| modality.is_empty())
        {
            return Err(BorsukError::InvalidStorage(
                "materialization level authority is outside an artifact level bound".to_owned(),
            ));
        }
        Ok(Self {
            row_levels,
            directory_levels,
            dense_levels,
        })
    }

    pub(crate) fn from_opened_published_refs(
        opened_row_generation: Option<&OpenedRowBundleGeneration>,
        dense_roots_by_projection: &BTreeMap<String, &crate::global_leaf_run::GlobalAnnRef>,
    ) -> Result<Self> {
        let row_levels = opened_row_generation
            .into_iter()
            .flat_map(|opened| opened.generation().active_runs.iter().map(|run| run.level))
            .collect();
        let mut directory_levels = BTreeMap::<DirectoryPartition, BTreeSet<u8>>::new();
        for run in opened_row_generation
            .into_iter()
            .flat_map(OpenedRowBundleGeneration::directory_runs)
        {
            directory_levels
                .entry(run.partition)
                .or_default()
                .insert(run.level);
        }
        let mut dense_levels = BTreeMap::new();
        for (projection, root) in dense_roots_by_projection {
            root.validate()?;
            let levels = root
                .base()
                .into_iter()
                .chain(root.incremental_runs())
                .map(crate::global_leaf_run::GlobalLeafRunRef::level)
                .collect();
            dense_levels.insert(projection.clone(), levels);
        }
        Self::new(row_levels, directory_levels, dense_levels)
    }

    pub(crate) fn reserve(
        &self,
        touched_partitions: &[DirectoryPartition],
        touched_dense_modalities: &[&str],
    ) -> Result<(MaterializationLevelReservation, Self)> {
        let mut next = self.clone();
        let row_level = if touched_partitions.is_empty() {
            None
        } else {
            let level = (0..crate::row_bundle::MAX_ACTIVE_ROW_BUNDLE_LEVELS)
                .map(|level| u8::try_from(level).expect("row level bound fits u8"))
                .find(|level| !self.row_levels.contains(level))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "materialization row level authority is full".to_owned(),
                    )
                })?;
            next.row_levels.insert(level);
            Some(level)
        };
        let mut reserved_directory = BTreeMap::new();
        for partition in touched_partitions.iter().copied().collect::<BTreeSet<_>>() {
            let occupied = self.directory_levels.get(&partition);
            let level = (0..crate::row_bundle::MAX_ACTIVE_DIRECTORY_LEVELS)
                .map(|level| u8::try_from(level).expect("directory level bound fits u8"))
                .find(|level| occupied.is_none_or(|levels| !levels.contains(level)))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "materialization directory level authority is full".to_owned(),
                    )
                })?;
            next.directory_levels
                .entry(partition)
                .or_default()
                .insert(level);
            reserved_directory.insert(partition, level);
        }
        let mut reserved_dense = BTreeMap::new();
        for modality in touched_dense_modalities
            .iter()
            .copied()
            .collect::<BTreeSet<_>>()
        {
            let occupied = self.dense_levels.get(modality);
            // Level zero is reserved for the immutable offline base. Even a
            // collection without a base root must leave that namespace free.
            let level = (1..crate::global_leaf_run::MAX_GLOBAL_LEAF_LEVELS)
                .map(|level| u8::try_from(level).expect("dense level bound fits u8"))
                .find(|level| occupied.is_none_or(|levels| !levels.contains(level)))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(format!(
                        "materialization dense level authority for `{modality}` is full"
                    ))
                })?;
            next.dense_levels
                .entry(modality.to_owned())
                .or_default()
                .insert(level);
            reserved_dense.insert(modality.to_owned(), level);
        }
        Ok((
            MaterializationLevelReservation {
                row_level,
                directory_levels: reserved_directory,
                dense_levels: reserved_dense,
            },
            next,
        ))
    }

    pub(crate) fn row_levels(&self) -> &BTreeSet<u8> {
        &self.row_levels
    }

    pub(crate) fn directory_levels(&self, partition: DirectoryPartition) -> Option<&BTreeSet<u8>> {
        self.directory_levels.get(&partition)
    }

    pub(crate) fn dense_levels(&self, modality: &str) -> Option<&BTreeSet<u8>> {
        self.dense_levels.get(modality)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct MaterializationBm25Delta {
    source_ranges: CommitSourceRangeSet,
    delta: super::index::Bm25StatsDelta,
}

impl MaterializationBm25Delta {
    pub(crate) fn new(
        source_ranges: CommitSourceRangeSet,
        delta: super::index::Bm25StatsDelta,
    ) -> Result<Self> {
        source_ranges.validate_canonical()?;
        Ok(Self {
            source_ranges,
            delta,
        })
    }

    pub(crate) fn fold<'a>(deltas: impl IntoIterator<Item = &'a Self>) -> Result<Self> {
        let mut coverage = CommitSourceRangeSet::default();
        let mut folded = super::index::Bm25StatsDelta::default();
        for item in deltas {
            coverage = coverage.union_disjoint(&item.source_ranges)?;
            folded.checked_add_assign(&item.delta)?;
        }
        Self::new(coverage, folded)
    }

    pub(crate) fn source_ranges(&self) -> &CommitSourceRangeSet {
        &self.source_ranges
    }

    pub(crate) fn delta(&self) -> &super::index::Bm25StatsDelta {
        &self.delta
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct LexicalDeltaAuthority {
    new_shards: Vec<SegmentLexicalShardRef>,
    new_roster: Vec<MaterializationArtifactRef>,
}

impl LexicalDeltaAuthority {
    pub(crate) fn new(
        new_shards: Vec<SegmentLexicalShardRef>,
        mut new_roster: Vec<MaterializationArtifactRef>,
    ) -> Result<Self> {
        sort_and_validate_roster(&mut new_roster)?;
        Ok(Self {
            new_shards,
            new_roster,
        })
    }

    pub(crate) fn new_shards(&self) -> &[SegmentLexicalShardRef] {
        &self.new_shards
    }

    pub(crate) fn new_roster(&self) -> &[MaterializationArtifactRef] {
        &self.new_roster
    }

    pub(crate) fn global_root_ref(&self) -> Option<&crate::manifest::LexicalRootRef> {
        None
    }
}

#[derive(Clone, Debug)]
// Task 4 consumes the assignment, row, lexical, and timing authority when it
// folds this unwired delta into the atomically published collection state.
#[allow(dead_code)]
pub(crate) struct MaterializedProjectionDelta {
    pub(crate) assignment_checksum: [u8; 32],
    pub(crate) rows: u64,
    pub(crate) dense: Option<PrimaryDenseArtifactCandidate>,
    pub(crate) lexical: Option<LexicalDeltaAuthority>,
    pub(crate) metrics: ModalityConstructionMetrics,
}

#[derive(Clone, Debug)]
// Source-transfer and BM25-ledger fields remain deliberately unwired until the
// Task 4 CAS boundary; retaining them here is part of the candidate contract.
#[allow(dead_code)]
pub(crate) struct MaterializationDeltaCandidate {
    extension: CommitSourceRangeSet,
    chain_anchor: MaterializationChainAnchor,
    level_reservation: MaterializationLevelReservation,
    level_authority: MaterializationLevelAuthority,
    source_transfer: MaterializationSourceTransfer,
    projections: BTreeMap<String, MaterializedProjectionDelta>,
    // This root covers only this extension. Task 4 must merge it with the
    // base generation's active row runs and directory partitions before CAS.
    row_bundle_delta_root: Option<ArtifactRef>,
    directory_updates: Vec<MaterializedDirectoryState>,
    fences: BTreeMap<String, MaterializationArtifactRef>,
    new_roster: Vec<MaterializationArtifactRef>,
    bm25: MaterializationBm25Delta,
}

impl MaterializationDeltaCandidate {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        prior: Option<&MaterializationChainAnchor>,
        extension: CommitSourceRangeSet,
        level_reservation: MaterializationLevelReservation,
        level_authority: MaterializationLevelAuthority,
        source_transfer: MaterializationSourceTransfer,
        projections: BTreeMap<String, MaterializedProjectionDelta>,
        row_bundle_delta_root: Option<ArtifactRef>,
        directory_updates: Vec<MaterializedDirectoryState>,
        fences: BTreeMap<String, MaterializationArtifactRef>,
        mut new_roster: Vec<MaterializationArtifactRef>,
        bm25: MaterializationBm25Delta,
    ) -> Result<Self> {
        extension.validate_canonical()?;
        if extension.ranges().is_empty() || bm25.source_ranges() != &extension {
            return Err(BorsukError::InvalidStorage(
                "materialization delta has empty or inconsistent extension coverage".to_owned(),
            ));
        }
        let full_coverage = match prior {
            Some(prior) => contiguous_extension(prior.coverage(), &extension)?,
            None => extension.clone(),
        };
        if row_bundle_delta_root.is_some() != !directory_updates.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "materialization row delta and directory authority disagree".to_owned(),
            ));
        }
        let chain_anchor = match prior {
            Some(prior) => prior.advanced(full_coverage)?,
            None => MaterializationChainAnchor::new(full_coverage.clone())?,
        };
        sort_and_validate_roster(&mut new_roster)?;
        Ok(Self {
            extension,
            chain_anchor,
            level_reservation,
            level_authority,
            source_transfer,
            projections,
            row_bundle_delta_root,
            directory_updates,
            fences,
            new_roster,
            bm25,
        })
    }

    pub(crate) fn extension_source_ranges(&self) -> &CommitSourceRangeSet {
        &self.extension
    }

    pub(crate) fn directory_owner_update(&self, id: &[u8]) -> Option<&MaterializedDirectoryState> {
        self.directory_updates
            .iter()
            .find(|update| update.record_id == id)
    }

    pub(crate) fn directory_updates(&self) -> &[MaterializedDirectoryState] {
        &self.directory_updates
    }

    pub(crate) fn fence_artifact(&self, modality: &str) -> Option<&MaterializationArtifactRef> {
        self.fences.get(modality)
    }

    pub(crate) fn row_bundle_delta_root(&self) -> Option<&ArtifactRef> {
        self.row_bundle_delta_root.as_ref()
    }

    pub(crate) fn dense_query_artifact(
        &self,
        modality: &str,
    ) -> Option<&crate::global_leaf_run::GlobalLeafRunRef> {
        self.projections
            .get(modality)
            .and_then(|projection| projection.dense.as_ref())
            .and_then(PrimaryDenseArtifactCandidate::new_run)
    }

    pub(crate) fn new_roster(&self) -> &[MaterializationArtifactRef] {
        &self.new_roster
    }

    pub(crate) fn modality_metrics(&self, modality: &str) -> Option<ModalityConstructionMetrics> {
        // Metrics describe every new physical object owned by this modality,
        // including its mandatory mutation fence, not query artifacts alone.
        self.projections
            .get(modality)
            .map(|projection| projection.metrics)
    }

    pub(crate) fn chain_anchor(&self) -> &MaterializationChainAnchor {
        &self.chain_anchor
    }

    pub(crate) fn level_reservation(&self) -> &MaterializationLevelReservation {
        &self.level_reservation
    }

    pub(crate) fn level_authority(&self) -> &MaterializationLevelAuthority {
        &self.level_authority
    }

    pub(crate) fn full_source_coverage(&self) -> &CommitSourceRangeSet {
        self.chain_anchor.coverage()
    }
}

fn contiguous_extension(
    base: &CommitSourceRangeSet,
    extension: &CommitSourceRangeSet,
) -> Result<CommitSourceRangeSet> {
    for range in extension.ranges() {
        let prior = base
            .ranges()
            .iter()
            .filter(|candidate| {
                candidate.source_epoch == range.source_epoch && candidate.shard == range.shard
            })
            .max_by_key(|candidate| candidate.last_sequence);
        let expected = prior.map_or(1, |candidate| candidate.last_sequence.saturating_add(1));
        if range.first_sequence != expected {
            return Err(BorsukError::InvalidStorage(
                "materialization delta extension has a source gap".to_owned(),
            ));
        }
    }
    base.union_disjoint(extension)
}

fn sort_and_validate_roster(roster: &mut [MaterializationArtifactRef]) -> Result<()> {
    roster.sort_by(|left, right| {
        (
            &left.storage_owner,
            &left.role,
            &left.path,
            left.range.as_ref().map(|range| (range.start, range.end)),
        )
            .cmp(&(
                &right.storage_owner,
                &right.role,
                &right.path,
                right.range.as_ref().map(|range| (range.start, range.end)),
            ))
    });
    if roster.windows(2).any(|pair| {
        pair[0].storage_owner == pair[1].storage_owner
            && pair[0].role == pair[1].role
            && pair[0].path == pair[1].path
            && pair[0].range == pair[1].range
    }) {
        return Err(BorsukError::InvalidStorage(
            "materialization delta roster repeats a physical artifact identity".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::{MutationOperation, MutationStamp, MutationVersion};

    fn stamp(hlc: u64) -> MutationState {
        MutationState::new(
            MutationStamp::new(MutationVersion::from_parts(hlc, [7; 16]), [hlc as u8; 32]),
            MutationOperation::Put,
        )
    }

    fn source_coverage(first: u64, last: u64) -> CommitSourceRangeSet {
        CommitSourceRangeSet::new(vec![
            crate::positioned_log::CommitSourceRange::new(1, 0, first, last).unwrap(),
        ])
        .unwrap()
    }

    fn test_head_ref() -> MaterializationArtifactRef {
        MaterializationArtifactRef::content(
            "primary",
            "primary",
            MaterializationArtifactRole::DenseDirectoryRoot,
            "global-leaf/v12/directories/11/directory.parquet".to_owned(),
            "11".repeat(32),
            64,
        )
    }

    #[test]
    fn materialization_base_rejects_malformed_head_refs_and_exposes_valid_heads() {
        let valid = test_head_ref();
        let base =
            MaterializationBaseAuthority::new(source_coverage(1, 1), vec![valid.clone()]).unwrap();
        assert_eq!(base.head_refs(), std::slice::from_ref(&valid));

        let malformed = [
            MaterializationArtifactRef {
                storage_owner: String::new(),
                ..valid.clone()
            },
            MaterializationArtifactRef {
                modality: String::new(),
                ..valid.clone()
            },
            MaterializationArtifactRef {
                path: String::new(),
                ..valid.clone()
            },
            MaterializationArtifactRef {
                checksum: "not-blake3".to_owned(),
                ..valid.clone()
            },
            MaterializationArtifactRef {
                encoded_bytes: 0,
                ..valid.clone()
            },
            MaterializationArtifactRef {
                range: Some(60..65),
                ..valid
            },
        ];
        for head in malformed {
            assert!(MaterializationBaseAuthority::new(source_coverage(1, 1), vec![head]).is_err());
        }
    }

    #[test]
    fn materialization_extension_advances_the_newest_same_shard_range() {
        let base = CommitSourceRangeSet::new(vec![
            crate::positioned_log::CommitSourceRange::new(1, 0, 1, 5).unwrap(),
            crate::positioned_log::CommitSourceRange::new(1, 0, 10, 12).unwrap(),
        ])
        .unwrap();
        let next = source_coverage(13, 13);
        assert!(contiguous_extension(&base, &next).is_ok());

        let stale_gap = source_coverage(6, 6);
        assert!(contiguous_extension(&base, &stale_gap).is_err());
    }

    #[test]
    fn row_bundle_role_rejects_unknown_artifact_paths() {
        let error = row_bundle_role("unexpected/materialization-object.parquet")
            .unwrap_err()
            .to_string();

        assert!(
            error.contains("unknown row-bundle artifact path"),
            "{error}"
        );
    }

    #[test]
    fn row_bundle_delta_rejects_rows_without_directory_authority_at_its_boundary() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(directory.path().to_str().unwrap()).unwrap();
        let rows = vec![MaterializedRow {
            modality: "primary".to_owned(),
            projection_kind: PositionedRouteProjectionKind::Primary,
            assignment: PositionedRouteAssignment::catalog([7; 32], 1).unwrap(),
            cell_ordinal: Some(0),
            record_id: b"orphan-row".to_vec(),
            projected_ordinal: 0,
            state: stamp(1),
            value: MaterializedRowValue::Dense(vec![1.0, 0.0]),
        }];

        let error = build_row_bundle_delta(
            &storage,
            rows,
            Vec::new(),
            &MaterializationLevelReservation::default(),
        )
        .unwrap_err()
        .to_string();

        assert!(
            error.contains("rows and directory authority must both be empty or nonempty"),
            "{error}"
        );
    }

    #[test]
    fn pending_and_published_authority_produce_the_same_ref_free_chain_anchor() {
        let coverage = source_coverage(1, 1);
        let published =
            MaterializationBaseAuthority::new(coverage.clone(), vec![test_head_ref()]).unwrap();
        let candidate = MaterializationDeltaCandidate::new(
            None,
            coverage.clone(),
            MaterializationLevelReservation::default(),
            MaterializationLevelAuthority::default(),
            MaterializationSourceTransfer::default(),
            BTreeMap::new(),
            None,
            Vec::new(),
            BTreeMap::new(),
            Vec::new(),
            MaterializationBm25Delta::new(
                coverage.clone(),
                super::super::index::Bm25StatsDelta::default(),
            )
            .unwrap(),
        )
        .unwrap();

        assert_eq!(candidate.chain_anchor(), &published.chain_anchor());
        let MaterializationChainAnchor { coverage: anchored } = candidate.chain_anchor().clone();
        assert_eq!(anchored, coverage);
    }

    #[test]
    fn materialization_chain_anchor_tracks_only_exact_source_coverage() {
        let coverage = source_coverage(1, 1);
        let anchor = MaterializationChainAnchor::new(coverage.clone()).unwrap();
        let advanced = anchor.advanced(source_coverage(1, 2)).unwrap();

        assert_eq!(anchor.coverage(), &coverage);
        assert_eq!(advanced.coverage(), &source_coverage(1, 2));
    }

    #[test]
    fn published_level_authority_uses_sparse_sets_not_source_range_counts() {
        let partition = DirectoryPartition::for_record_id(b"sparse-level-id");
        let authority = MaterializationLevelAuthority::new(
            std::collections::BTreeSet::from([0, 2]),
            BTreeMap::from([(partition, std::collections::BTreeSet::from([0, 2]))]),
            BTreeMap::from([(
                "primary".to_owned(),
                std::collections::BTreeSet::from([0, 2]),
            )]),
        )
        .unwrap();
        let source_anchor = MaterializationChainAnchor::new(source_coverage(1, 12)).unwrap();

        let (plan, next) = authority.reserve(&[partition], &["primary"]).unwrap();

        assert_eq!(source_anchor.coverage().ranges()[0].last_sequence, 12);
        assert_eq!(plan.row_level(), Some(1));
        assert_eq!(plan.directory_level(partition), Some(1));
        assert_eq!(plan.dense_level("primary"), Some(1));
        assert!(next.row_levels().contains(&1));
        assert!(next.directory_levels(partition).unwrap().contains(&1));
        assert!(next.dense_levels("primary").unwrap().contains(&1));
    }

    #[test]
    fn repeated_row_bearing_reservations_choose_distinct_row_directory_and_dense_levels() {
        let partition = DirectoryPartition::for_record_id(b"repeat-level-id");
        let authority = MaterializationLevelAuthority::new(
            std::collections::BTreeSet::new(),
            BTreeMap::new(),
            BTreeMap::from([("primary".to_owned(), std::collections::BTreeSet::from([0]))]),
        )
        .unwrap();

        let (first, authority) = authority.reserve(&[partition], &["primary"]).unwrap();
        let (second, _) = authority.reserve(&[partition], &["primary"]).unwrap();

        assert_eq!(first.row_level(), Some(0));
        assert_eq!(first.directory_level(partition), Some(0));
        assert_eq!(second.row_level(), Some(1));
        assert_eq!(second.directory_level(partition), Some(1));
        assert_eq!(first.dense_level("primary"), Some(1));
        assert_eq!(second.dense_level("primary"), Some(2));
    }

    #[test]
    fn row_and_directory_reservations_reuse_disjoint_free_levels_independently() {
        let partition = DirectoryPartition::for_record_id(b"disjoint-free-levels");
        let authority = MaterializationLevelAuthority::new(
            std::collections::BTreeSet::from([0]),
            BTreeMap::from([(partition, std::collections::BTreeSet::from([1]))]),
            BTreeMap::new(),
        )
        .unwrap();

        let (plan, _) = authority.reserve(&[partition], &[]).unwrap();

        assert_eq!(plan.row_level(), Some(1));
        assert_eq!(plan.directory_level(partition), Some(0));
    }

    #[test]
    fn duplicate_touched_partitions_have_one_canonical_reservation() {
        let partition = DirectoryPartition::for_record_id(b"duplicate-partition");
        let authority = MaterializationLevelAuthority::default();

        let (duplicate_plan, duplicate_next) = authority
            .reserve(&[partition, partition, partition], &[])
            .unwrap();
        let (canonical_plan, canonical_next) = authority.reserve(&[partition], &[]).unwrap();

        assert_eq!(duplicate_plan, canonical_plan);
        assert_eq!(duplicate_next, canonical_next);
        assert_eq!(duplicate_plan.directory_levels.len(), 1);
    }

    #[test]
    fn opened_published_refs_derive_sparse_projection_level_authority() {
        fn open_generation(
            storage: &Storage,
            root: &ArtifactRef,
        ) -> crate::row_bundle::OpenedRowBundleGeneration {
            let root_bytes = storage
                .read_bytes_with_cache_status_and_checksum(&root.path, &root.checksum)
                .unwrap()
                .bytes;
            crate::row_bundle::open_row_bundle_generation(root, root_bytes.into(), |references| {
                references
                    .iter()
                    .map(|reference| {
                        storage
                            .read_bytes_with_cache_status_and_checksum(
                                &reference.path,
                                &reference.checksum,
                            )
                            .map(|read| read.bytes.into())
                    })
                    .collect()
            })
            .unwrap()
        }

        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(directory.path().to_str().unwrap()).unwrap();
        let assignment = PositionedRouteAssignment::catalog([9; 32], 1).unwrap();
        let build_delta = |record_id: &[u8], version: u64, level: u8| {
            let partition = DirectoryPartition::for_record_id(record_id);
            let levels = MaterializationLevelReservation {
                row_level: Some(level),
                directory_levels: BTreeMap::from([(partition, level)]),
                dense_levels: BTreeMap::new(),
            };
            build_row_bundle_delta(
                &storage,
                vec![MaterializedRow {
                    modality: "primary".to_owned(),
                    projection_kind: PositionedRouteProjectionKind::Primary,
                    assignment: assignment.clone(),
                    cell_ordinal: Some(0),
                    record_id: record_id.to_vec(),
                    projected_ordinal: 0,
                    state: stamp(version),
                    value: MaterializedRowValue::Dense(vec![version as f32, 1.0]),
                }],
                vec![MaterializedDirectoryState {
                    record_id: record_id.to_vec(),
                    routing_epoch: 1,
                    cell_ordinal: 0,
                    state: stamp(version),
                }],
                &levels,
            )
            .unwrap()
            .delta_root
            .unwrap()
        };
        let low_id = b"opened-low";
        let partition = DirectoryPartition::for_record_id(low_id);
        let high_id = (0_u64..)
            .map(|ordinal| format!("opened-high-{ordinal}"))
            .find(|id| DirectoryPartition::for_record_id(id.as_bytes()) == partition)
            .unwrap();
        let low = build_delta(low_id, 1, 0);
        let high = build_delta(high_id.as_bytes(), 2, 2);
        let folded = crate::positioned_materializer::fold_row_bundle_deltas_to_storage(
            &storage,
            None,
            &[low, high],
        )
        .unwrap();
        let opened = open_generation(&storage, &folded.root.reference);

        let codebook_checksum = "ab".repeat(32);
        let codebook = crate::global_leaf_run::GlobalCodebookRef::new(
            "global-leaf/v12/codebooks/ab/codebook.parquet".to_owned(),
            codebook_checksum.clone(),
            crate::VectorMetric::Euclidean,
            2,
            crate::VectorElementType::Float32,
            1,
            1,
            1,
            1,
            0,
            1,
            1,
        );
        let base = crate::global_leaf_run::GlobalLeafRunRef::new_base(
            codebook_checksum,
            crate::global_leaf_run::GlobalLeafDirectoryRef::new(
                "global-leaf/v12/directories/ab/directory.parquet".to_owned(),
                "cd".repeat(32),
                1,
                1,
            ),
            1,
            1,
            1,
            1,
            0,
            1,
            1,
            stamp(1).stamp(),
            stamp(1).stamp(),
        );
        let primary_root =
            crate::global_leaf_run::GlobalAnnRef::new_offline_base(codebook, base, 1, 0).unwrap();
        let dense_roots_by_projection = BTreeMap::from([("primary".to_owned(), &primary_root)]);

        let authority = MaterializationLevelAuthority::from_opened_published_refs(
            Some(&opened),
            &dense_roots_by_projection,
        )
        .unwrap();

        assert_eq!(authority.row_levels(), &BTreeSet::from([0, 2]));
        assert_eq!(
            authority.directory_levels(partition),
            Some(&BTreeSet::from([0, 2]))
        );
        assert_eq!(
            authority.dense_levels("primary"),
            Some(&BTreeSet::from([0]))
        );
        let (reservation, _) = authority.reserve(&[partition], &["primary"]).unwrap();
        assert_eq!(reservation.row_level(), Some(1));
        assert_eq!(reservation.directory_level(partition), Some(1));
        assert_eq!(reservation.dense_level("primary"), Some(1));

        let foreign = build_delta(b"foreign-directory", 3, 3);
        let foreign_opened = open_generation(&storage, &foreign);
        let foreign_authority = MaterializationLevelAuthority::from_opened_published_refs(
            Some(&foreign_opened),
            &dense_roots_by_projection,
        )
        .unwrap();
        assert_eq!(foreign_authority.row_levels(), &BTreeSet::from([3]));
        assert_eq!(
            foreign_authority
                .directory_levels(DirectoryPartition::for_record_id(b"foreign-directory")),
            Some(&BTreeSet::from([3]))
        );
    }

    #[test]
    fn opened_published_refs_reject_a_corrupt_dense_root_before_using_its_levels() {
        let mut value = serde_json::to_value(
            crate::global_leaf_run::GlobalAnnRef::new_offline_base(
                crate::global_leaf_run::GlobalCodebookRef::new(
                    "global-leaf/v12/codebooks/ab/codebook.parquet".to_owned(),
                    "ab".repeat(32),
                    crate::VectorMetric::Euclidean,
                    2,
                    crate::VectorElementType::Float32,
                    1,
                    1,
                    1,
                    1,
                    0,
                    1,
                    1,
                ),
                crate::global_leaf_run::GlobalLeafRunRef::new_base(
                    "ab".repeat(32),
                    crate::global_leaf_run::GlobalLeafDirectoryRef::new(
                        "global-leaf/v12/directories/ab/directory.parquet".to_owned(),
                        "cd".repeat(32),
                        1,
                        1,
                    ),
                    1,
                    1,
                    1,
                    1,
                    0,
                    1,
                    1,
                    stamp(1).stamp(),
                    stamp(1).stamp(),
                ),
                1,
                0,
            )
            .unwrap(),
        )
        .unwrap();
        value["layout_version"] = serde_json::json!(11);
        let corrupt: crate::global_leaf_run::GlobalAnnRef = serde_json::from_value(value).unwrap();

        let error = MaterializationLevelAuthority::from_opened_published_refs(
            None,
            &BTreeMap::from([("primary".to_owned(), &corrupt)]),
        )
        .unwrap_err()
        .to_string();

        assert!(error.contains("V12 global ANN layout version"), "{error}");
    }

    #[test]
    fn row_free_but_touched_directory_full_fails_before_level_reservation() {
        let partition = DirectoryPartition::for_record_id(b"full-directory");
        let authority = MaterializationLevelAuthority::new(
            std::collections::BTreeSet::from([0]),
            BTreeMap::from([(
                partition,
                (0..u8::try_from(crate::row_bundle::MAX_ACTIVE_DIRECTORY_LEVELS).unwrap())
                    .collect(),
            )]),
            BTreeMap::new(),
        )
        .unwrap();

        let error = authority
            .reserve(&[partition], &[])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("directory") && error.contains("level"),
            "{error}"
        );
    }

    #[test]
    fn dense_modalities_allocate_independently_and_one_full_modality_fails_atomically() {
        let partition = DirectoryPartition::for_record_id(b"independent-dense");
        let authority = MaterializationLevelAuthority::new(
            std::collections::BTreeSet::new(),
            BTreeMap::new(),
            BTreeMap::from([
                (
                    "primary".to_owned(),
                    std::collections::BTreeSet::from([0, 1]),
                ),
                ("image".to_owned(), std::collections::BTreeSet::from([0])),
            ]),
        )
        .unwrap();
        let (plan, _) = authority
            .reserve(&[partition], &["primary", "image"])
            .unwrap();
        assert_eq!(plan.dense_level("primary"), Some(2));
        assert_eq!(plan.dense_level("image"), Some(1));

        let full = MaterializationLevelAuthority::new(
            std::collections::BTreeSet::new(),
            BTreeMap::new(),
            BTreeMap::from([
                ("primary".to_owned(), std::collections::BTreeSet::from([0])),
                (
                    "image".to_owned(),
                    (0..u8::try_from(crate::global_leaf_run::MAX_GLOBAL_LEAF_LEVELS).unwrap())
                        .collect(),
                ),
            ]),
        )
        .unwrap();
        let before = full.clone();
        assert!(full.reserve(&[partition], &["primary", "image"]).is_err());
        assert_eq!(full, before);
    }

    #[test]
    fn delete_only_level_reservation_succeeds_when_every_data_plane_is_full() {
        let partition = DirectoryPartition::for_record_id(b"full-but-delete-only");
        let authority = MaterializationLevelAuthority::new(
            (0..u8::try_from(crate::row_bundle::MAX_ACTIVE_ROW_BUNDLE_LEVELS).unwrap()).collect(),
            BTreeMap::from([(
                partition,
                (0..u8::try_from(crate::row_bundle::MAX_ACTIVE_DIRECTORY_LEVELS).unwrap())
                    .collect(),
            )]),
            BTreeMap::from([(
                "primary".to_owned(),
                (0..u8::try_from(crate::global_leaf_run::MAX_GLOBAL_LEAF_LEVELS).unwrap())
                    .collect(),
            )]),
        )
        .unwrap();

        let (plan, next) = authority.reserve(&[], &[]).unwrap();

        assert_eq!(plan.row_level(), None);
        assert_eq!(plan.dense_level("primary"), None);
        assert_eq!(next, authority);
    }

    #[test]
    fn pure_fold_never_substitutes_a_delta_root_for_the_published_generation_root() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(directory.path().to_str().unwrap()).unwrap();
        let assignment = PositionedRouteAssignment::catalog([9; 32], 1).unwrap();
        let build_delta = |record_id: &[u8], version: u64, cell_ordinal: u32, level: u8| {
            let partition = DirectoryPartition::for_record_id(record_id);
            let levels = MaterializationLevelReservation {
                row_level: Some(level),
                directory_levels: BTreeMap::from([(partition, level)]),
                dense_levels: BTreeMap::new(),
            };
            build_row_bundle_delta(
                &storage,
                vec![MaterializedRow {
                    modality: "primary".to_owned(),
                    projection_kind: PositionedRouteProjectionKind::Primary,
                    assignment: assignment.clone(),
                    cell_ordinal: Some(cell_ordinal),
                    record_id: record_id.to_vec(),
                    projected_ordinal: 0,
                    state: stamp(version),
                    value: MaterializedRowValue::Dense(vec![version as f32, 1.0]),
                }],
                vec![MaterializedDirectoryState {
                    record_id: record_id.to_vec(),
                    routing_epoch: 1,
                    cell_ordinal,
                    state: stamp(version),
                }],
                &levels,
            )
            .unwrap()
            .delta_root
            .unwrap()
        };
        let published = build_delta(b"published", 1, 0, 0);
        let delta = build_delta(b"delta", 2, 1, 1);

        let folded = crate::positioned_materializer::fold_row_bundle_deltas_to_storage(
            &storage,
            Some(&published),
            std::slice::from_ref(&delta),
        )
        .unwrap();

        assert_ne!(folded.root.reference, delta);
        assert_ne!(folded.root.reference, published);
        let bytes = storage
            .read_bytes_with_cache_status_and_checksum(
                &folded.root.reference.path,
                &folded.root.reference.checksum,
            )
            .unwrap()
            .bytes;
        let decoded = crate::row_bundle::decode_generation_root(
            &folded.root.reference,
            bytes::Bytes::from(bytes),
        )
        .unwrap();
        assert_eq!(
            decoded
                .active_runs
                .iter()
                .map(|run| run.level)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
    }

    #[test]
    fn row_bundle_uses_physical_assignment_cell_order_independently_of_route_plan_id_order() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(directory.path().to_str().unwrap()).unwrap();
        let assignment = PositionedRouteAssignment::catalog([9; 32], 1).unwrap();
        let route_plan = vec![
            PositionedRoutePlanRow::summary(
                "image",
                PositionedRouteProjectionKind::Dense,
                assignment.clone(),
                2,
                stamp(1).stamp(),
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"a-id".to_vec(),
                "image",
                PositionedRouteProjectionKind::Dense,
                0,
                assignment.clone(),
                2,
                stamp(1).stamp(),
            )
            .unwrap(),
            PositionedRoutePlanRow::routed(
                b"z-id".to_vec(),
                "image",
                PositionedRouteProjectionKind::Dense,
                0,
                assignment.clone(),
                1,
                stamp(2).stamp(),
            )
            .unwrap(),
        ];
        let decoded_route_plan = crate::format::positioned_route_plan_from_parquet(
            &crate::format::positioned_route_plan_to_parquet(&route_plan).unwrap(),
        )
        .unwrap();
        let rows = vec![
            MaterializedRow {
                modality: "image".to_owned(),
                projection_kind: PositionedRouteProjectionKind::Dense,
                assignment: assignment.clone(),
                cell_ordinal: Some(2),
                record_id: b"a-id".to_vec(),
                projected_ordinal: 0,
                state: stamp(1),
                value: MaterializedRowValue::Dense(vec![1.0, 0.0]),
            },
            MaterializedRow {
                modality: "image".to_owned(),
                projection_kind: PositionedRouteProjectionKind::Dense,
                assignment,
                cell_ordinal: Some(1),
                record_id: b"z-id".to_vec(),
                projected_ordinal: 0,
                state: stamp(2),
                value: MaterializedRowValue::Dense(vec![0.0, 1.0]),
            },
        ];
        let directory = vec![
            MaterializedDirectoryState {
                record_id: b"a-id".to_vec(),
                routing_epoch: 1,
                cell_ordinal: 2,
                state: stamp(1),
            },
            MaterializedDirectoryState {
                record_id: b"z-id".to_vec(),
                routing_epoch: 1,
                cell_ordinal: 1,
                state: stamp(2),
            },
        ];

        let levels = MaterializationLevelReservation {
            row_level: Some(0),
            directory_levels: directory
                .iter()
                .map(|row| (DirectoryPartition::for_record_id(&row.record_id), 0))
                .collect(),
            dense_levels: BTreeMap::new(),
        };
        let candidate = build_row_bundle_delta(&storage, rows, directory, &levels).unwrap();

        assert!(candidate.delta_root.is_some());
        let bundle = candidate
            .roster
            .iter()
            .find(|artifact| artifact.role == MaterializationArtifactRole::RowBundle)
            .expect("the materialized rows emit one physical bundle");
        let bytes = storage
            .read_bytes_with_cache_status_and_checksum(&bundle.path, &bundle.checksum)
            .unwrap()
            .bytes;
        let decoded = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            Bytes::from(bytes),
        )
        .unwrap()
        .build()
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap();
        let physical = decoded
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column_by_name("record_id")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap();
                let cells = batch
                    .column_by_name("cell_ordinal")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<UInt32Array>()
                    .unwrap();
                (0..batch.num_rows())
                    .map(|row| (cells.value(row), ids.value(row).to_vec()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            physical,
            vec![(1, b"z-id".to_vec()), (2, b"a-id".to_vec())],
            "physical Parquet rows must remain assignment/cell ordered"
        );
        assert_eq!(
            decoded_route_plan
                .iter()
                .filter_map(|row| Some((row.record_id.clone()?, row.projected_ordinal?)))
                .collect::<Vec<_>>(),
            vec![(b"a-id".to_vec(), 0_u32), (b"z-id".to_vec(), 0_u32)],
            "authenticated route-plan identity remains record-id/projected-ordinal ordered"
        );
    }
}
