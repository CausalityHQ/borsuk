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

use std::{collections::BTreeMap, ops::Range, sync::Arc, time::Duration};

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
        RowBundleObjectSink, RowBundlePackOptions, pack_canonical_row_bundles_to_sink,
        pack_directory_partition_run, pack_directory_root,
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
    target_level: u8,
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
        target_level,
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
        let run = pack_directory_partition_run(
            partition,
            target_level,
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
        // A published Task 4 CAS closes the prior pending chain. Its next
        // immutable delta starts a fresh ref-free pending-level namespace.
        MaterializationChainAnchor {
            coverage: self.coverage.clone(),
            pending_row_deltas: 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaterializationChainAnchor {
    coverage: CommitSourceRangeSet,
    // Counts only unpublished row-bearing deltas since the last Task 4 CAS.
    // Coverage-only delete deltas do not occupy immutable run levels.
    pending_row_deltas: u8,
}

impl MaterializationChainAnchor {
    pub(crate) fn new(coverage: CommitSourceRangeSet) -> Result<Self> {
        coverage.validate_canonical()?;
        if coverage.ranges().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "materialization chain anchor coverage is empty".to_owned(),
            ));
        }
        Ok(Self {
            coverage,
            pending_row_deltas: 0,
        })
    }

    pub(crate) fn coverage(&self) -> &CommitSourceRangeSet {
        &self.coverage
    }

    pub(crate) fn pending_artifact_levels(&self) -> Result<(u8, u8)> {
        if usize::from(self.pending_row_deltas) >= crate::row_bundle::MAX_ACTIVE_ROW_BUNDLE_LEVELS {
            return Err(BorsukError::InvalidStorage(
                "materialization chain exhausted its pending row-run levels".to_owned(),
            ));
        }
        let dense_level = self.pending_row_deltas.checked_add(1).ok_or_else(|| {
            BorsukError::InvalidStorage(
                "materialization chain dense run level overflows u8".to_owned(),
            )
        })?;
        Ok((self.pending_row_deltas, dense_level))
    }

    fn advanced(&self, coverage: CommitSourceRangeSet, row_bearing: bool) -> Result<Self> {
        coverage.validate_canonical()?;
        if coverage.ranges().is_empty() || !coverage.covers(&self.coverage) {
            return Err(BorsukError::InvalidStorage(
                "materialization chain anchor does not extend its prior coverage".to_owned(),
            ));
        }
        let pending_row_deltas = if row_bearing {
            self.pending_artifact_levels()?;
            self.pending_row_deltas.checked_add(1).ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "materialization chain pending row count overflows u8".to_owned(),
                )
            })?
        } else {
            self.pending_row_deltas
        };
        Ok(Self {
            coverage,
            pending_row_deltas,
        })
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
        let row_bearing = row_bundle_delta_root.is_some();
        if row_bearing != !directory_updates.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "materialization row delta and directory authority disagree".to_owned(),
            ));
        }
        let chain_anchor = match prior {
            Some(prior) => prior.advanced(full_coverage, row_bearing)?,
            None => MaterializationChainAnchor::new(full_coverage.clone())?
                .advanced(full_coverage, row_bearing)?,
        };
        sort_and_validate_roster(&mut new_roster)?;
        Ok(Self {
            extension,
            chain_anchor,
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

        let error = build_row_bundle_delta(&storage, rows, Vec::new(), 0)
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
        let MaterializationChainAnchor {
            coverage: anchored,
            pending_row_deltas: _,
        } = candidate.chain_anchor().clone();
        assert_eq!(anchored, coverage);
    }

    #[test]
    fn materialization_chain_anchor_bounds_pending_row_levels_and_ignores_delete_only_deltas() {
        let coverage = source_coverage(1, 1);
        let anchor = MaterializationChainAnchor::new(coverage.clone()).unwrap();
        assert_eq!(anchor.pending_artifact_levels().unwrap(), (0, 1));

        let row_bearing = anchor.advanced(source_coverage(1, 2), true).unwrap();
        assert_eq!(row_bearing.pending_artifact_levels().unwrap(), (1, 2));
        let delete_only = row_bearing.advanced(source_coverage(1, 3), false).unwrap();
        assert_eq!(delete_only.pending_artifact_levels().unwrap(), (1, 2));

        let exhausted = MaterializationChainAnchor {
            coverage,
            pending_row_deltas: u8::try_from(crate::row_bundle::MAX_ACTIVE_ROW_BUNDLE_LEVELS)
                .unwrap(),
        };
        assert!(exhausted.pending_artifact_levels().is_err());
        assert!(exhausted.advanced(source_coverage(1, 2), true).is_err());
        assert_eq!(
            exhausted
                .advanced(source_coverage(1, 2), false)
                .unwrap()
                .pending_row_deltas,
            exhausted.pending_row_deltas
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

        let candidate = build_row_bundle_delta(&storage, rows, directory, 0).unwrap();

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
