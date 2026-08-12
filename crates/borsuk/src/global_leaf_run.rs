use std::sync::{Arc, Condvar, Mutex};

use crate::{
    BorsukError, Result, lane_log::GROUP_COMMIT_STRIPE_COUNT, metric::VectorMetric,
    mutation::MutationStamp, record::VectorElementType, storage::Storage,
};

pub(crate) const GLOBAL_PQ_REF_LAYOUT_VERSION: u8 = 12;
// The durable V12 reference accepts the catalog's full u32 ordinal space. The
// still-wired segment-derived descriptor enforces its own 65,536-cell producer
// bound until Task 4 switches publication to catalog-pinned artifacts.
pub(crate) const MAX_GLOBAL_LEAF_LEVELS: usize = u64::BITS as usize;
pub(crate) const DRIFT_WINDOW_ROWS: usize = 4096;

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(deny_unknown_fields)]
pub(crate) struct LaneSourceRange {
    pub(crate) lane: u16,
    pub(crate) lease_epoch: u64,
    pub(crate) first_sequence: u64,
    pub(crate) last_sequence: u64,
}

impl LaneSourceRange {
    fn validate(&self) -> Result<()> {
        if self.lane >= GROUP_COMMIT_STRIPE_COUNT {
            return invalid("V12 source-range lane is outside the group-commit stripe count");
        }
        if self.lease_epoch == 0 {
            return invalid("V12 source-range lease epoch must be positive");
        }
        if self.first_sequence == 0 || self.last_sequence == 0 {
            return invalid("V12 source-range sequences must be positive");
        }
        if self.first_sequence > self.last_sequence {
            return invalid("V12 source-range first sequence exceeds last sequence");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceRangeSet {
    ranges: Vec<LaneSourceRange>,
}

impl SourceRangeSet {
    pub(crate) fn new(mut ranges: Vec<LaneSourceRange>) -> Result<Self> {
        ranges.sort_unstable_by_key(|range| {
            (
                range.lane,
                range.lease_epoch,
                range.first_sequence,
                range.last_sequence,
            )
        });
        for range in &ranges {
            range.validate()?;
        }
        let mut canonical = Vec::<LaneSourceRange>::with_capacity(ranges.len());
        for range in ranges {
            if let Some(left) = canonical.last_mut()
                && left.lane == range.lane
                && left.lease_epoch == range.lease_epoch
            {
                if left.last_sequence >= range.first_sequence {
                    return Err(BorsukError::InvalidStorage(
                        "V12 source ranges overlap within one lane lease epoch".to_owned(),
                    ));
                }
                if left.last_sequence.checked_add(1) == Some(range.first_sequence) {
                    left.last_sequence = range.last_sequence;
                    continue;
                }
            }
            canonical.push(range);
        }
        Ok(Self { ranges: canonical })
    }

    pub(crate) fn union_disjoint(&self, other: &Self) -> Result<Self> {
        let mut ranges = Vec::with_capacity(
            self.ranges
                .len()
                .checked_add(other.ranges.len())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V12 source-range count overflow".to_owned())
                })?,
        );
        ranges.extend_from_slice(&self.ranges);
        ranges.extend_from_slice(&other.ranges);
        Self::new(ranges)
    }

    fn validate_canonical(&self) -> Result<()> {
        let canonical = Self::new(self.ranges.clone())?;
        if canonical != *self {
            return invalid("V12 source ranges must be sorted canonically");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MutationStampRef {
    hlc: u64,
    writer: [u8; 16],
    digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalLeafDirectoryRef {
    path: String,
    checksum: String,
    encoded_bytes: u64,
    shard_count: u32,
}

impl GlobalLeafDirectoryRef {
    pub(crate) fn new(
        path: String,
        checksum: String,
        encoded_bytes: u64,
        object_count: u32,
    ) -> Self {
        Self {
            path,
            checksum,
            encoded_bytes,
            shard_count: object_count,
        }
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

    pub(crate) fn object_count(&self) -> u32 {
        self.shard_count
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalCodebookRef {
    layout_version: u8,
    descriptor_path: String,
    descriptor_checksum: String,
    metric: VectorMetric,
    dimensions: usize,
    element_type: VectorElementType,
    code_width: usize,
    cell_count: u32,
    candidates: u32,
    probes: u32,
    reconstruction_error_p95_micros: u64,
    resident_bytes: u64,
    storage_bytes: u64,
}

impl GlobalCodebookRef {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        descriptor_path: String,
        descriptor_checksum: String,
        metric: VectorMetric,
        dimensions: usize,
        element_type: VectorElementType,
        code_width: usize,
        cell_count: u32,
        candidates: u32,
        probes: u32,
        reconstruction_error_p95_micros: u64,
        resident_bytes: u64,
        storage_bytes: u64,
    ) -> Self {
        Self {
            layout_version: GLOBAL_PQ_REF_LAYOUT_VERSION,
            descriptor_path,
            descriptor_checksum,
            metric,
            dimensions,
            element_type,
            code_width,
            cell_count,
            candidates,
            probes,
            reconstruction_error_p95_micros,
            resident_bytes,
            storage_bytes,
        }
    }

    pub(crate) fn descriptor_path(&self) -> &str {
        &self.descriptor_path
    }

    pub(crate) fn descriptor_checksum(&self) -> &str {
        &self.descriptor_checksum
    }

    pub(crate) fn metric(&self) -> &VectorMetric {
        &self.metric
    }

    pub(crate) fn dimensions(&self) -> usize {
        self.dimensions
    }

    pub(crate) fn element_type(&self) -> VectorElementType {
        self.element_type
    }

    pub(crate) fn code_width(&self) -> usize {
        self.code_width
    }

    pub(crate) fn cell_count(&self) -> u32 {
        self.cell_count
    }

    pub(crate) fn candidates(&self) -> u32 {
        self.candidates
    }

    pub(crate) fn probes(&self) -> u32 {
        self.probes
    }

    pub(crate) fn reconstruction_error_p95_micros(&self) -> u64 {
        self.reconstruction_error_p95_micros
    }

    pub(crate) fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    pub(crate) fn storage_bytes(&self) -> u64 {
        self.storage_bytes
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalLeafRunRef {
    layout_version: u8,
    level: u8,
    codebook_checksum: String,
    directory: GlobalLeafDirectoryRef,
    rows: u64,
    pages: u64,
    bundles: u64,
    sealed_pages: u64,
    partial_pages: u64,
    encoded_bytes: u64,
    resident_bytes: u64,
    min_stamp: Option<MutationStampRef>,
    max_stamp: Option<MutationStampRef>,
    source_ranges: SourceRangeSet,
}

#[derive(Debug)]
pub(crate) struct ResidentGlobalLeafRun {
    root: crate::global_leaf::GlobalLeafRunDirectoryRoot,
    decoded_shards: Vec<ResidentGlobalLeafShardSlot>,
    level: Option<u8>,
    rows: usize,
    pages: u64,
    bundles: u64,
    sealed_pages: u64,
    partial_pages: u64,
    encoded_bytes: u64,
    persisted_resident_bytes: usize,
}

#[derive(Debug, Default)]
struct ResidentGlobalLeafShardSlot {
    state: Mutex<ResidentGlobalLeafShardState>,
    ready: Condvar,
}

#[derive(Debug, Default)]
enum ResidentGlobalLeafShardState {
    #[default]
    Empty,
    Loading,
    Ready(Arc<Vec<crate::global_leaf::GlobalLeafPageRef>>),
}

fn resident_global_leaf_runtime_overhead(page_count: usize, shard_count: usize) -> usize {
    std::mem::size_of::<ResidentGlobalLeafRun>()
        .saturating_add(
            page_count.saturating_mul(crate::global_leaf::GLOBAL_LEAF_V12_CELL_BOUND_BYTES),
        )
        .saturating_add(
            shard_count.saturating_mul(
                std::mem::size_of::<ResidentGlobalLeafShardSlot>()
                    .saturating_add(std::mem::size_of::<
                        Arc<Vec<crate::global_leaf::GlobalLeafPageRef>>,
                    >())
                    .saturating_add(2 * std::mem::size_of::<usize>())
                    .saturating_add(std::mem::size_of::<
                        Vec<crate::global_leaf::GlobalLeafPageRef>,
                    >()),
            ),
        )
}

impl ResidentGlobalLeafRun {
    pub(crate) fn new(
        root: crate::global_leaf::GlobalLeafRunDirectoryRoot,
        validated_directory: crate::global_leaf::GlobalLeafRunDirectory,
        level: Option<u8>,
    ) -> Result<Self> {
        let rows = validated_directory
            .pages
            .iter()
            .try_fold(0_usize, |total, page| {
                total.checked_add(page.rows as usize).ok_or_else(|| {
                    BorsukError::InvalidStorage("V12 leaf-run row count exceeds usize".to_owned())
                })
            })?;
        let pages = u64::try_from(validated_directory.pages.len()).unwrap_or(u64::MAX);
        let bundles = u64::try_from(validated_directory.bundles.len()).unwrap_or(u64::MAX);
        let sealed_pages = u64::try_from(
            validated_directory
                .pages
                .iter()
                .filter(|page| page.partial_run_count == 0)
                .count(),
        )
        .unwrap_or(u64::MAX);
        let encoded_bytes = validated_directory
            .bundles
            .iter()
            .map(|bundle| bundle.encoded_bytes)
            .chain(
                validated_directory
                    .shards
                    .iter()
                    .map(|shard| shard.encoded_bytes),
            )
            .try_fold(0_u64, |total, bytes| {
                total.checked_add(bytes).ok_or_else(|| {
                    BorsukError::InvalidStorage("V12 leaf-run encoded bytes overflow".to_owned())
                })
            })?;
        let decoded_shards = (0..root.shards().len())
            .map(|_| ResidentGlobalLeafShardSlot::default())
            .collect::<Vec<_>>();
        // V12 persists the exact decoded full-directory size. Runtime-only
        // fixed-slot overhead is accounted separately so the established V12
        // serialized contract does not change under the same format marker.
        let persisted_resident_bytes = validated_directory.resident_bytes();
        Ok(Self {
            root,
            decoded_shards,
            level,
            rows,
            pages,
            bundles,
            sealed_pages,
            partial_pages: pages.saturating_sub(sealed_pages),
            encoded_bytes,
            persisted_resident_bytes,
        })
    }

    pub(crate) fn root(&self) -> &crate::global_leaf::GlobalLeafRunDirectoryRoot {
        &self.root
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.persisted_resident_bytes
    }

    #[cfg(test)]
    pub(crate) fn runtime_resident_bytes(&self) -> usize {
        self.persisted_resident_bytes
            .saturating_add(resident_global_leaf_runtime_overhead(
                usize::try_from(self.pages).unwrap_or(usize::MAX),
                self.decoded_shards.len(),
            ))
    }

    pub(crate) fn pages(&self) -> u64 {
        self.pages
    }

    pub(crate) fn bundles(&self) -> u64 {
        self.bundles
    }

    pub(crate) fn sealed_pages(&self) -> u64 {
        self.sealed_pages
    }

    pub(crate) fn partial_pages(&self) -> u64 {
        self.partial_pages
    }

    pub(crate) fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    pub(crate) fn load_shard<F>(
        &self,
        ordinal: usize,
        loader: F,
    ) -> Result<(Arc<Vec<crate::global_leaf::GlobalLeafPageRef>>, u64, bool)>
    where
        F: FnOnce() -> Result<(Vec<crate::global_leaf::GlobalLeafPageRef>, u64)>,
    {
        let slot = self.decoded_shards.get(ordinal).ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V12 directory shard ordinal exceeds its authenticated root".to_owned(),
            )
        })?;
        let mut loader = Some(loader);
        loop {
            let mut state = slot.state.lock().map_err(|_| {
                BorsukError::InvalidStorage(
                    "V12 decoded directory shard slot lock is poisoned".to_owned(),
                )
            })?;
            match &*state {
                ResidentGlobalLeafShardState::Ready(pages) => {
                    return Ok((Arc::clone(pages), 0, true));
                }
                ResidentGlobalLeafShardState::Loading => {
                    state = slot.ready.wait(state).map_err(|_| {
                        BorsukError::InvalidStorage(
                            "V12 decoded directory shard slot lock is poisoned".to_owned(),
                        )
                    })?;
                    drop(state);
                }
                ResidentGlobalLeafShardState::Empty => {
                    *state = ResidentGlobalLeafShardState::Loading;
                    drop(state);
                    let result = loader
                        .take()
                        .expect("V12 shard-slot leader owns one loader")(
                    );
                    match result {
                        Ok((pages, physical_bytes)) => {
                            let pages = Arc::new(pages);
                            let mut state = slot.state.lock().map_err(|_| {
                                BorsukError::InvalidStorage(
                                    "V12 decoded directory shard slot lock is poisoned".to_owned(),
                                )
                            })?;
                            *state = ResidentGlobalLeafShardState::Ready(Arc::clone(&pages));
                            drop(state);
                            slot.ready.notify_all();
                            return Ok((pages, physical_bytes, false));
                        }
                        Err(error) => {
                            let mut state = slot.state.lock().map_err(|_| {
                                BorsukError::InvalidStorage(
                                    "V12 decoded directory shard slot lock is poisoned".to_owned(),
                                )
                            })?;
                            *state = ResidentGlobalLeafShardState::Empty;
                            drop(state);
                            slot.ready.notify_all();
                            return Err(error);
                        }
                    }
                }
            }
        }
    }

    pub(crate) fn cached_shard(
        &self,
        ordinal: usize,
    ) -> Result<Option<Arc<Vec<crate::global_leaf::GlobalLeafPageRef>>>> {
        let slot = self.decoded_shards.get(ordinal).ok_or_else(|| {
            BorsukError::InvalidStorage(
                "V12 directory shard ordinal exceeds its authenticated root".to_owned(),
            )
        })?;
        slot.state
            .lock()
            .map(|state| match &*state {
                ResidentGlobalLeafShardState::Ready(pages) => Some(Arc::clone(pages)),
                ResidentGlobalLeafShardState::Empty | ResidentGlobalLeafShardState::Loading => None,
            })
            .map_err(|_| {
                BorsukError::InvalidStorage(
                    "V12 decoded directory shard slot lock is poisoned".to_owned(),
                )
            })
    }

    #[cfg(test)]
    pub(crate) fn shard_slot_count(&self) -> usize {
        self.decoded_shards.len()
    }

    #[cfg(test)]
    pub(crate) fn shard_is_loaded(&self, ordinal: usize) -> Result<bool> {
        self.cached_shard(ordinal).map(|pages| pages.is_some())
    }

    pub(crate) fn level(&self) -> Option<u8> {
        self.level
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows
    }
}

impl GlobalLeafRunRef {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_base(
        codebook_checksum: String,
        directory: GlobalLeafDirectoryRef,
        rows: u64,
        pages: u64,
        bundles: u64,
        sealed_pages: u64,
        partial_pages: u64,
        encoded_bytes: u64,
        resident_bytes: u64,
        min_stamp: MutationStamp,
        max_stamp: MutationStamp,
    ) -> Self {
        Self {
            layout_version: GLOBAL_PQ_REF_LAYOUT_VERSION,
            level: 0,
            codebook_checksum,
            directory,
            rows,
            pages,
            bundles,
            sealed_pages,
            partial_pages,
            encoded_bytes,
            resident_bytes,
            min_stamp: Some(MutationStampRef::from_stamp(min_stamp)),
            max_stamp: Some(MutationStampRef::from_stamp(max_stamp)),
            source_ranges: SourceRangeSet::default(),
        }
    }

    pub(crate) fn directory(&self) -> &GlobalLeafDirectoryRef {
        &self.directory
    }

    pub(crate) fn codebook_checksum(&self) -> &str {
        &self.codebook_checksum
    }

    pub(crate) fn encoded_bytes(&self) -> u64 {
        self.encoded_bytes
    }

    pub(crate) fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    fn runtime_resident_overhead(&self) -> u64 {
        let page_count = usize::try_from(self.pages).unwrap_or(usize::MAX);
        let shard_count =
            usize::try_from(self.directory.object_count().saturating_sub(1)).unwrap_or(usize::MAX);
        u64::try_from(resident_global_leaf_runtime_overhead(
            page_count,
            shard_count,
        ))
        .unwrap_or(u64::MAX)
    }

    pub(crate) fn level(&self) -> u8 {
        self.level
    }

    pub(crate) fn rows(&self) -> u64 {
        self.rows
    }

    pub(crate) fn pages(&self) -> u64 {
        self.pages
    }

    pub(crate) fn bundles(&self) -> u64 {
        self.bundles
    }

    pub(crate) fn sealed_pages(&self) -> u64 {
        self.sealed_pages
    }

    pub(crate) fn partial_pages(&self) -> u64 {
        self.partial_pages
    }
}

impl MutationStampRef {
    fn from_stamp(stamp: MutationStamp) -> Self {
        Self {
            hlc: stamp.version().hlc(),
            writer: stamp.version().writer(),
            digest: stamp.digest(),
        }
    }
}

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalDriftState {
    pending_reconstruction_errors_micros: Vec<u64>,
    consecutive_breaches: u8,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GlobalAnnRef {
    layout_version: u8,
    codebook: GlobalCodebookRef,
    base: Option<GlobalLeafRunRef>,
    incremental_runs: Vec<GlobalLeafRunRef>,
    coverage: SourceRangeSet,
    leaf_epoch: u64,
    purge_epoch: u64,
    base_rows: u64,
    appended_live_rows: u64,
    obsolete_rows: u64,
    rows: u64,
    storage_bytes: u64,
    resident_bytes: u64,
    drift: GlobalDriftState,
}

impl GlobalAnnRef {
    pub(crate) fn new_offline_base(
        codebook: GlobalCodebookRef,
        base: GlobalLeafRunRef,
        leaf_epoch: u64,
        purge_epoch: u64,
    ) -> Result<Self> {
        let rows = base.rows;
        let storage_bytes = codebook
            .storage_bytes
            .checked_add(base.encoded_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V12 global ANN storage total overflow".to_owned())
            })?;
        let resident_bytes = codebook
            .resident_bytes
            .checked_add(base.resident_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V12 global ANN resident total overflow".to_owned())
            })?;
        let reference = Self {
            layout_version: GLOBAL_PQ_REF_LAYOUT_VERSION,
            codebook,
            base: Some(base),
            incremental_runs: Vec::new(),
            coverage: SourceRangeSet::default(),
            leaf_epoch,
            purge_epoch,
            base_rows: rows,
            appended_live_rows: 0,
            obsolete_rows: 0,
            rows,
            storage_bytes,
            resident_bytes,
            drift: GlobalDriftState::default(),
        };
        reference.validate()?;
        Ok(reference)
    }

    pub(crate) fn layout_version(&self) -> u8 {
        self.layout_version
    }

    pub(crate) fn codebook(&self) -> &GlobalCodebookRef {
        &self.codebook
    }

    pub(crate) fn base(&self) -> Option<&GlobalLeafRunRef> {
        self.base.as_ref()
    }

    pub(crate) fn incremental_runs(&self) -> &[GlobalLeafRunRef] {
        &self.incremental_runs
    }

    pub(crate) fn leaf_epoch(&self) -> u64 {
        self.leaf_epoch
    }

    pub(crate) fn storage_bytes(&self) -> u64 {
        self.storage_bytes
    }

    pub(crate) fn resident_bytes_estimate(&self) -> u64 {
        let codebook_strings = self
            .codebook
            .descriptor_path
            .capacity()
            .saturating_add(self.codebook.descriptor_checksum.capacity());
        let run_bytes = self
            .base
            .iter()
            .chain(self.incremental_runs.iter())
            .map(|run| {
                run.codebook_checksum
                    .capacity()
                    .saturating_add(run.directory.path.capacity())
                    .saturating_add(run.directory.checksum.capacity())
                    .saturating_add(
                        run.source_ranges
                            .ranges
                            .capacity()
                            .saturating_mul(std::mem::size_of::<LaneSourceRange>()),
                    )
            })
            .fold(0_usize, usize::saturating_add);
        let metadata_bytes = std::mem::size_of::<Self>()
            .saturating_add(codebook_strings)
            .saturating_add(
                self.incremental_runs
                    .capacity()
                    .saturating_mul(std::mem::size_of::<GlobalLeafRunRef>()),
            )
            .saturating_add(run_bytes)
            .saturating_add(
                self.drift
                    .pending_reconstruction_errors_micros
                    .capacity()
                    .saturating_mul(std::mem::size_of::<u64>()),
            );
        u64::try_from(metadata_bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(self.resident_bytes)
            .saturating_add(self.runtime_resident_overhead())
    }

    pub(crate) fn runtime_resident_overhead(&self) -> u64 {
        self.base
            .iter()
            .chain(self.incremental_runs.iter())
            .map(GlobalLeafRunRef::runtime_resident_overhead)
            .fold(0_u64, u64::saturating_add)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.layout_version != GLOBAL_PQ_REF_LAYOUT_VERSION {
            return invalid("V12 global ANN layout version is invalid");
        }
        if self.leaf_epoch == 0 {
            return invalid("V12 global ANN leaf epoch must be nonzero");
        }
        if self.purge_epoch > self.leaf_epoch {
            return invalid("V12 global ANN purge epoch exceeds leaf epoch");
        }
        validate_codebook(&self.codebook)?;
        self.coverage.validate_canonical()?;

        let mut previous_level = None;
        let mut union = SourceRangeSet::default();
        let mut saw_coverage_only_deletion = false;
        let mut storage_bytes = self.codebook.storage_bytes;
        let mut resident_bytes = self.codebook.resident_bytes;

        if let Some(base) = &self.base {
            validate_run(base, &self.codebook, true)?;
            storage_bytes = checked_add(storage_bytes, base.encoded_bytes, "storage total")?;
            resident_bytes = checked_add(resident_bytes, base.resident_bytes, "resident total")?;
        }

        for run in &self.incremental_runs {
            if usize::from(run.level) >= MAX_GLOBAL_LEAF_LEVELS {
                return invalid("V12 leaf-run level exceeds the supported level count");
            }
            if let Some(prior) = previous_level {
                if run.level == prior {
                    return invalid("duplicate V12 leaf-run level");
                }
                if run.level < prior {
                    return invalid("V12 leaf-run levels must be sorted ascending");
                }
            }
            previous_level = Some(run.level);
            validate_run(run, &self.codebook, false)?;
            if run.rows == 0 {
                if saw_coverage_only_deletion {
                    return invalid("V12 global ANN has more than one coverage-only deletion run");
                }
                saw_coverage_only_deletion = true;
            }
            union = union.union_disjoint(&run.source_ranges)?;
            storage_bytes = checked_add(storage_bytes, run.encoded_bytes, "storage total")?;
            resident_bytes = checked_add(resident_bytes, run.resident_bytes, "resident total")?;
        }
        if union != self.coverage {
            return invalid("V12 global ANN source-range coverage does not equal the run union");
        }

        let row_total = checked_add(self.base_rows, self.appended_live_rows, "row total")?;
        if row_total != self.rows {
            return invalid("V12 global ANN row total is inconsistent");
        }
        if self.obsolete_rows > self.rows {
            return invalid("V12 global ANN obsolete rows exceed rows");
        }
        if storage_bytes != self.storage_bytes {
            return invalid("V12 global ANN storage total is inconsistent");
        }
        if resident_bytes != self.resident_bytes {
            return invalid("V12 global ANN resident total is inconsistent");
        }
        if self.drift.pending_reconstruction_errors_micros.len() >= DRIFT_WINDOW_ROWS {
            return invalid("V12 global ANN drift pending errors exceed the drift window");
        }
        if self.drift.consecutive_breaches > 3 {
            return invalid("V12 global ANN drift breach count exceeds three");
        }
        Ok(())
    }

    /// Validate a complete V12 serving shape. Unlike the Task 3 base-only
    /// gate, this accepts authenticated incremental levels while retaining the
    /// invariant that every retired lane range has exactly one searchable run.
    pub(crate) fn validate_serving_shape(&self) -> Result<()> {
        self.validate()?;
        let Some(base) = &self.base else {
            return invalid("V12 serving reference is missing its base run");
        };
        if base.level != 0 || self.base_rows != base.rows {
            return invalid("V12 serving base counters are inconsistent");
        }
        Ok(())
    }
}

fn validate_codebook(codebook: &GlobalCodebookRef) -> Result<()> {
    if codebook.layout_version != GLOBAL_PQ_REF_LAYOUT_VERSION {
        return invalid("V12 global codebook layout version is invalid");
    }
    validate_object_ref(
        "V12 global codebook descriptor",
        &codebook.descriptor_path,
        &codebook.descriptor_checksum,
        codebook.storage_bytes,
    )?;
    if codebook.dimensions == 0 {
        return invalid("V12 global codebook dimensions must be positive");
    }
    codebook
        .element_type
        .fixed_width_bytes(codebook.dimensions)?;
    if codebook.code_width == 0 {
        return invalid("V12 global codebook code width must be positive");
    }
    if codebook.cell_count == 0 {
        return invalid("V12 global codebook cell count must be positive");
    }
    if codebook.candidates == 0 || codebook.candidates > codebook.cell_count {
        return invalid("V12 global codebook candidates must be within the cell count");
    }
    if codebook.probes == 0
        || codebook.probes > codebook.cell_count
        || codebook.probes > codebook.candidates
    {
        return invalid("V12 global codebook probes must be within candidates and the cell count");
    }
    if let VectorMetric::Minkowski { p } = codebook.metric
        && (!p.is_finite() || p < 1.0)
    {
        return invalid("V12 global codebook metric is invalid");
    }
    Ok(())
}

fn validate_run(run: &GlobalLeafRunRef, codebook: &GlobalCodebookRef, is_base: bool) -> Result<()> {
    validate_run_against_checksum(run, &codebook.descriptor_checksum, is_base)
}

fn validate_run_against_checksum(
    run: &GlobalLeafRunRef,
    codebook_checksum: &str,
    is_base: bool,
) -> Result<()> {
    if run.layout_version != GLOBAL_PQ_REF_LAYOUT_VERSION {
        return invalid("V12 leaf-run layout version is invalid");
    }
    if run.codebook_checksum != codebook_checksum {
        return invalid("V12 leaf-run codebook checksum does not match the global codebook");
    }
    validate_object_ref(
        "V12 leaf-run directory",
        &run.directory.path,
        &run.directory.checksum,
        run.directory.encoded_bytes,
    )?;
    if run.directory.shard_count == 0 {
        return invalid("V12 leaf-run directory shard count must be positive");
    }
    run.source_ranges.validate_canonical()?;
    if is_base {
        if !run.source_ranges.ranges.is_empty() {
            return invalid("V12 base leaf run must not have lane source ranges");
        }
        if run.rows == 0 || run.pages == 0 || run.bundles == 0 {
            return invalid("V12 base leaf run must have positive rows, pages, and bundles");
        }
    } else {
        if run.source_ranges.ranges.is_empty() {
            return invalid("V12 incremental leaf run must have lane source ranges");
        }
        let has_leaf_rows = run.rows > 0 || run.pages > 0 || run.bundles > 0;
        let complete_leaf_rows = run.rows > 0 && run.pages > 0 && run.bundles > 0;
        if has_leaf_rows && !complete_leaf_rows {
            return invalid(
                "V12 incremental leaf run rows, pages, and bundles must be all positive or all zero",
            );
        }
    }
    match (
        run.rows > 0,
        run.min_stamp.is_some(),
        run.max_stamp.is_some(),
    ) {
        (true, true, true) | (false, false, false) => {}
        _ => return invalid("V12 leaf-run mutation bounds must match leaf rows"),
    }
    if run
        .sealed_pages
        .checked_add(run.partial_pages)
        .ok_or_else(|| BorsukError::InvalidStorage("V12 leaf-run page total overflow".to_owned()))?
        != run.pages
    {
        return invalid("V12 leaf-run sealed and partial pages do not equal pages");
    }
    Ok(())
}

fn validate_object_ref(label: &str, path: &str, checksum: &str, encoded_bytes: u64) -> Result<()> {
    if path.is_empty()
        || checksum.len() != 64
        || !checksum.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return invalid(&format!(
            "{label} must have a path and a 64-character hexadecimal checksum"
        ));
    }
    if encoded_bytes == 0 {
        return invalid(&format!("{label} encoded bytes must be positive"));
    }
    Ok(())
}

pub(crate) struct PersistedGlobalLeafArtifacts {
    pub(crate) directory: GlobalLeafDirectoryRef,
    pub(crate) page_count: usize,
    pub(crate) bundle_count: usize,
    pub(crate) rows: usize,
    pub(crate) storage_bytes: u64,
    pub(crate) resident_bytes: u64,
}

pub(crate) struct GlobalLeafPersistenceWriter<'a> {
    storage: &'a Storage,
    dimensions: usize,
    element_type: VectorElementType,
    bundle_pages: Vec<crate::global_leaf::GlobalLeafPageInput>,
    bundle_partial_run_counts: Vec<u8>,
    bundle_exact_estimated_bytes: u64,
    bundle_code_rows: usize,
    bundle_code_bytes: u64,
    bundles: Vec<crate::global_leaf::GlobalLeafBundleRef>,
    page_refs: Vec<crate::global_leaf::GlobalLeafPageRef>,
    codebook_checksum: String,
    active_cell: Option<(u32, u32)>,
    last_finalized_cell: Option<u32>,
    page_count: usize,
    rows: usize,
    storage_bytes: u64,
}

impl<'a> GlobalLeafPersistenceWriter<'a> {
    pub(crate) fn new(
        storage: &'a Storage,
        dimensions: usize,
        element_type: VectorElementType,
        codebook_checksum: String,
    ) -> Result<Self> {
        element_type.fixed_width_bytes(dimensions)?;
        Ok(Self {
            storage,
            dimensions,
            element_type,
            bundle_pages: Vec::with_capacity(crate::global_leaf::GLOBAL_LEAF_BUNDLE_MAX_PAGES),
            bundle_partial_run_counts: Vec::with_capacity(
                crate::global_leaf::GLOBAL_LEAF_BUNDLE_MAX_PAGES,
            ),
            bundle_exact_estimated_bytes: 0,
            bundle_code_rows: 0,
            bundle_code_bytes: 0,
            bundles: Vec::new(),
            page_refs: Vec::new(),
            codebook_checksum,
            active_cell: None,
            last_finalized_cell: None,
            page_count: 0,
            rows: 0,
            storage_bytes: 0,
        })
    }

    pub(crate) fn push_cell_chunk(
        &mut self,
        mut pages: Vec<crate::global_leaf::GlobalLeafPageInput>,
    ) -> Result<()> {
        let cell_index = pages
            .first()
            .map(|page| page.cell_index)
            .ok_or_else(|| BorsukError::InvalidStorage("global leaf cell is empty".to_owned()))?;
        if pages.iter().enumerate().any(|(local_ordinal, page)| {
            page.cell_index != cell_index
                || usize::try_from(page.leaf_ordinal).ok() != Some(local_ordinal)
                || page.rows.is_empty()
        }) {
            return Err(BorsukError::InvalidStorage(
                "global leaf cell continuation is not locally canonical".to_owned(),
            ));
        }
        let first_leaf = match self.active_cell {
            Some((active_cell, next_leaf)) if active_cell == cell_index => next_leaf,
            Some(_) => {
                return Err(BorsukError::InvalidStorage(
                    "global leaf cell changed before explicit finalization".to_owned(),
                ));
            }
            None if self
                .last_finalized_cell
                .is_some_and(|prior| prior >= cell_index) =>
            {
                return Err(BorsukError::InvalidStorage(
                    "global leaf cells are not strictly ordered".to_owned(),
                ));
            }
            None => 0,
        };
        for (local_ordinal, page) in pages.iter_mut().enumerate() {
            page.leaf_ordinal = first_leaf
                .checked_add(u32::try_from(local_ordinal).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "global leaf continuation ordinal exceeds u32".to_owned(),
                    )
                })?)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "global leaf continuation ordinal overflows".to_owned(),
                    )
                })?;
        }
        let next_leaf = first_leaf
            .checked_add(u32::try_from(pages.len()).map_err(|_| {
                BorsukError::InvalidStorage(
                    "global leaf continuation page count exceeds u32".to_owned(),
                )
            })?)
            .ok_or_else(|| {
                BorsukError::InvalidStorage(
                    "global leaf continuation page count overflows".to_owned(),
                )
            })?;
        self.active_cell = Some((cell_index, next_leaf));
        self.add_page_totals(&pages)?;
        for page in pages {
            self.push_page(page, 0)?;
        }
        self.flush_bundle()?;
        Ok(())
    }

    pub(crate) fn finalize_cell(&mut self, cell_index: u32) -> Result<()> {
        if self.active_cell.map(|(active, _)| active) != Some(cell_index)
            || !self.bundle_pages.is_empty()
        {
            return Err(BorsukError::InvalidStorage(
                "global leaf finalization does not match its active cell".to_owned(),
            ));
        }
        self.active_cell = None;
        self.last_finalized_cell = Some(cell_index);
        Ok(())
    }

    fn add_page_totals(&mut self, pages: &[crate::global_leaf::GlobalLeafPageInput]) -> Result<()> {
        self.page_count = self.page_count.checked_add(pages.len()).ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf page count overflows".to_owned())
        })?;
        let source_rows = pages.iter().try_fold(0_usize, |rows, page| {
            rows.checked_add(page.rows.len()).ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf row count overflows".to_owned())
            })
        })?;
        self.rows = self.rows.checked_add(source_rows).ok_or_else(|| {
            BorsukError::InvalidStorage("global leaf row count overflows".to_owned())
        })?;
        Ok(())
    }

    fn push_page(
        &mut self,
        page: crate::global_leaf::GlobalLeafPageInput,
        partial_run_count: u8,
    ) -> Result<()> {
        let page_estimate = crate::global_leaf::estimate_global_leaf_bundle_page(
            &page,
            self.dimensions,
            self.element_type,
        )?;
        let candidate_exact_bytes = self
            .bundle_exact_estimated_bytes
            .checked_add(page_estimate.exact_batch_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf bundle estimate overflows".to_owned())
            })?;
        let candidate_code_rows = self
            .bundle_code_rows
            .checked_add(page_estimate.rows)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf bundle row estimate overflows".to_owned())
            })?;
        let candidate_code_bytes = self
            .bundle_code_bytes
            .checked_add(page_estimate.code_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf bundle code estimate overflows".to_owned())
            })?;
        let candidate_bytes = crate::global_leaf::estimate_global_leaf_bundle_bytes(
            candidate_exact_bytes,
            candidate_code_rows,
            candidate_code_bytes,
        )?;
        if !self.bundle_pages.is_empty()
            && (self.bundle_pages.len() == crate::global_leaf::GLOBAL_LEAF_BUNDLE_MAX_PAGES
                || candidate_bytes > crate::global_leaf::GLOBAL_LEAF_BUNDLE_MAX_ENCODED_BYTES)
        {
            self.flush_bundle()?;
        }
        let (candidate_exact_bytes, candidate_code_rows, candidate_code_bytes) =
            if self.bundle_pages.is_empty() {
                (
                    page_estimate.exact_batch_bytes,
                    page_estimate.rows,
                    page_estimate.code_bytes,
                )
            } else {
                (
                    candidate_exact_bytes,
                    candidate_code_rows,
                    candidate_code_bytes,
                )
            };
        if crate::global_leaf::estimate_global_leaf_bundle_bytes(
            candidate_exact_bytes,
            candidate_code_rows,
            candidate_code_bytes,
        )? > crate::global_leaf::GLOBAL_LEAF_BUNDLE_MAX_ENCODED_BYTES
        {
            return Err(BorsukError::InvalidStorage(
                "global leaf page cannot fit the conservative V12 bundle byte cap".to_owned(),
            ));
        }
        self.bundle_pages.push(page);
        self.bundle_partial_run_counts.push(partial_run_count);
        self.bundle_exact_estimated_bytes = candidate_exact_bytes;
        self.bundle_code_rows = candidate_code_rows;
        self.bundle_code_bytes = candidate_code_bytes;
        Ok(())
    }

    fn flush_bundle(&mut self) -> Result<()> {
        if self.bundle_pages.is_empty() {
            return Ok(());
        }
        let pages = std::mem::take(&mut self.bundle_pages);
        let partial_run_counts = std::mem::take(&mut self.bundle_partial_run_counts);
        let estimated_bytes = crate::global_leaf::estimate_global_leaf_bundle_bytes(
            self.bundle_exact_estimated_bytes,
            self.bundle_code_rows,
            self.bundle_code_bytes,
        )?;
        self.bundle_exact_estimated_bytes = 0;
        self.bundle_code_rows = 0;
        self.bundle_code_bytes = 0;
        let encoded = crate::global_leaf::encode_global_leaf_bundle(
            &pages,
            self.dimensions,
            self.element_type,
        )?;
        if u64::try_from(encoded.bytes.len()).map_or(true, |actual| actual > estimated_bytes) {
            return Err(BorsukError::InvalidStorage(
                "global leaf conservative bundle estimate was below encoded bytes".to_owned(),
            ));
        }
        let checksum = *blake3::hash(&encoded.bytes).as_bytes();
        let checksum_hex = blake3::Hash::from_bytes(checksum).to_hex().to_string();
        let path = format!(
            "global-leaf/bundles/{}/bundle-{checksum_hex}.arrow",
            &checksum_hex[..2]
        );
        self.storage
            .write_bytes_content_addressed(&path, &encoded.bytes)?;
        let bundle_index = u32::try_from(self.bundles.len()).map_err(|_| {
            BorsukError::InvalidStorage("global leaf bundle index exceeds u32".to_owned())
        })?;
        let encoded_bytes = u64::try_from(encoded.bytes.len()).map_err(|_| {
            BorsukError::InvalidStorage("global leaf bundle size exceeds u64".to_owned())
        })?;
        self.storage_bytes = self
            .storage_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf storage byte count overflows".to_owned())
            })?;
        self.bundles.push(crate::global_leaf::GlobalLeafBundleRef {
            path,
            checksum,
            encoded_bytes,
            code_plane_offset: encoded.code_plane_offset,
            code_plane_bytes: encoded.code_plane_bytes,
            code_plane_checksum: encoded.code_plane_checksum,
        });
        if encoded.pages.len() != partial_run_counts.len() {
            return Err(BorsukError::InvalidStorage(
                "global leaf encoder changed the page count".to_owned(),
            ));
        }
        let page_refs = encoded
            .pages
            .into_iter()
            .zip(partial_run_counts)
            .map(|(page, partial_run_count)| {
                Ok(crate::global_leaf::GlobalLeafPageRef {
                    cell_index: page.cell_index,
                    leaf_ordinal: page.leaf_ordinal,
                    bundle_index,
                    batch_offset: page.batch_offset,
                    metadata_bytes: page.metadata_bytes,
                    body_bytes: page.body_bytes,
                    batch_bytes: page.batch_bytes,
                    code_offset: page.code_offset,
                    code_bytes: page.code_bytes,
                    code_checksum: page.code_checksum,
                    rows: u32::try_from(page.rows).map_err(|_| {
                        BorsukError::InvalidStorage(
                            "global leaf page row count exceeds u32".to_owned(),
                        )
                    })?,
                    partial_run_count,
                    checksum: page.checksum,
                    centroid_code: page.centroid_code.into_boxed_slice(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        self.page_refs.extend(page_refs);
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<PersistedGlobalLeafArtifacts> {
        if self.active_cell.is_some() || !self.bundle_pages.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "global leaf writer has an unfinalized cell continuation".to_owned(),
            ));
        }
        let persistence = persist_directory_artifacts(
            self.storage,
            &self.codebook_checksum,
            &self.page_refs,
            &self.bundles,
            self.storage_bytes,
        )?;
        Ok(PersistedGlobalLeafArtifacts {
            directory: persistence.directory,
            page_count: self.page_count,
            bundle_count: self.bundles.len(),
            rows: self.rows,
            storage_bytes: persistence.storage_bytes,
            resident_bytes: persistence.resident_bytes,
        })
    }
}

struct PersistedDirectoryArtifacts {
    directory: GlobalLeafDirectoryRef,
    storage_bytes: u64,
    resident_bytes: u64,
}

fn persist_directory_artifacts(
    storage: &Storage,
    codebook_checksum: &str,
    page_refs: &[crate::global_leaf::GlobalLeafPageRef],
    bundles: &[crate::global_leaf::GlobalLeafBundleRef],
    bundle_bytes: u64,
) -> Result<PersistedDirectoryArtifacts> {
    let encoded = crate::global_leaf::encode_global_leaf_run_directory(
        codebook_checksum,
        page_refs,
        bundles,
    )?;
    let mut storage_bytes = bundle_bytes;
    for shard in &encoded.shards {
        storage.write_bytes_content_addressed(&shard.reference.path, &shard.bytes)?;
        storage_bytes = storage_bytes
            .checked_add(shard.reference.encoded_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("global leaf storage byte count overflows".to_owned())
            })?;
    }
    let root_checksum = blake3::hash(&encoded.root).to_hex().to_string();
    let root_path = format!(
        "global-leaf/v12/directories/{}/directory-{root_checksum}.parquet",
        &root_checksum[..2]
    );
    storage.write_bytes_content_addressed(&root_path, &encoded.root)?;
    let root_bytes = u64::try_from(encoded.root.len()).map_err(|_| {
        BorsukError::InvalidStorage("global leaf directory size exceeds u64".to_owned())
    })?;
    storage_bytes = storage_bytes.checked_add(root_bytes).ok_or_else(|| {
        BorsukError::InvalidStorage("global leaf storage byte count overflows".to_owned())
    })?;
    let directory = crate::global_leaf::decode_global_leaf_run_directory(
        codebook_checksum,
        &encoded.root,
        |reference| {
            encoded
                .shards
                .iter()
                .find(|shard| shard.reference == *reference)
                .map(|shard| shard.bytes.clone())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V12 encoded directory lost an external shard".to_owned(),
                    )
                })
        },
    )?;
    let resident_bytes = u64::try_from(directory.resident_bytes()).map_err(|_| {
        BorsukError::InvalidStorage("global leaf directory resident bytes exceed u64".to_owned())
    })?;
    Ok(PersistedDirectoryArtifacts {
        directory: GlobalLeafDirectoryRef::new(
            root_path,
            root_checksum,
            root_bytes,
            encoded.directory_object_count()?,
        ),
        storage_bytes,
        resident_bytes,
    })
}

fn checked_add(left: u64, right: u64, label: &str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| BorsukError::InvalidStorage(format!("V12 global ANN {label} overflow")))
}

fn invalid(message: &str) -> Result<()> {
    Err(BorsukError::InvalidStorage(message.to_owned()))
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::Duration,
    };

    use super::*;
    use crate::{VectorElementType, metric::VectorMetric};

    fn range(
        lane: u16,
        lease_epoch: u64,
        first_sequence: u64,
        last_sequence: u64,
    ) -> LaneSourceRange {
        let range = LaneSourceRange {
            lane,
            lease_epoch,
            first_sequence,
            last_sequence,
        };
        range.validate().unwrap();
        range
    }

    fn valid_ann_ref() -> GlobalAnnRef {
        let coverage = SourceRangeSet::new(vec![range(3, 7, 1, 4)]).unwrap();
        let codebook = GlobalCodebookRef {
            layout_version: GLOBAL_PQ_REF_LAYOUT_VERSION,
            descriptor_path: "global/codebook.json".to_owned(),
            descriptor_checksum: "ab".repeat(32),
            metric: VectorMetric::Euclidean,
            dimensions: 4,
            element_type: VectorElementType::Float32,
            code_width: 4,
            cell_count: 1,
            candidates: 1,
            probes: 1,
            reconstruction_error_p95_micros: 0,
            resident_bytes: 10,
            storage_bytes: 20,
        };
        let incremental_run = GlobalLeafRunRef {
            layout_version: GLOBAL_PQ_REF_LAYOUT_VERSION,
            level: 0,
            codebook_checksum: codebook.descriptor_checksum.clone(),
            directory: GlobalLeafDirectoryRef {
                path: "global/run-0-directory.json".to_owned(),
                checksum: "cd".repeat(32),
                encoded_bytes: 30,
                shard_count: 1,
            },
            rows: 4,
            pages: 1,
            bundles: 1,
            sealed_pages: 1,
            partial_pages: 0,
            encoded_bytes: 30,
            resident_bytes: 40,
            min_stamp: Some(MutationStampRef {
                hlc: 1,
                writer: [1; 16],
                digest: [2; 32],
            }),
            max_stamp: Some(MutationStampRef {
                hlc: 2,
                writer: [1; 16],
                digest: [3; 32],
            }),
            source_ranges: coverage.clone(),
        };
        GlobalAnnRef {
            layout_version: GLOBAL_PQ_REF_LAYOUT_VERSION,
            codebook,
            base: None,
            incremental_runs: vec![incremental_run],
            coverage,
            leaf_epoch: 1,
            purge_epoch: 0,
            base_rows: 0,
            appended_live_rows: 4,
            obsolete_rows: 0,
            rows: 4,
            storage_bytes: 50,
            resident_bytes: 50,
            drift: GlobalDriftState {
                pending_reconstruction_errors_micros: Vec::new(),
                consecutive_breaches: 0,
            },
        }
    }

    fn valid_offline_ann_ref() -> GlobalAnnRef {
        let codebook = GlobalCodebookRef {
            layout_version: GLOBAL_PQ_REF_LAYOUT_VERSION,
            descriptor_path: "global-leaf/v12/codebooks/ab/codebook.parquet".to_owned(),
            descriptor_checksum: "ab".repeat(32),
            metric: VectorMetric::Euclidean,
            dimensions: 4,
            element_type: VectorElementType::Float32,
            code_width: 4,
            cell_count: 4,
            candidates: 4,
            probes: 2,
            reconstruction_error_p95_micros: 7,
            resident_bytes: 10,
            storage_bytes: 20,
        };
        let base = GlobalLeafRunRef {
            layout_version: GLOBAL_PQ_REF_LAYOUT_VERSION,
            level: 0,
            codebook_checksum: codebook.descriptor_checksum.clone(),
            directory: GlobalLeafDirectoryRef {
                path: "global-leaf/v12/directories/cd/directory.parquet".to_owned(),
                checksum: "cd".repeat(32),
                encoded_bytes: 30,
                shard_count: 1,
            },
            rows: 4,
            pages: 1,
            bundles: 1,
            sealed_pages: 1,
            partial_pages: 0,
            encoded_bytes: 30,
            resident_bytes: 40,
            min_stamp: Some(MutationStampRef {
                hlc: 1,
                writer: [1; 16],
                digest: [2; 32],
            }),
            max_stamp: Some(MutationStampRef {
                hlc: 2,
                writer: [1; 16],
                digest: [3; 32],
            }),
            source_ranges: SourceRangeSet::default(),
        };
        GlobalAnnRef::new_offline_base(codebook, base, 1, 0).unwrap()
    }

    #[test]
    fn v12_reference_rejects_old_v11_layout_version() {
        let mut reference = valid_ann_ref();
        reference.layout_version = 11;
        let error = reference.validate().unwrap_err();
        assert!(error.to_string().contains("layout version"), "{error}");
    }

    #[test]
    fn v12_persisted_storage_counts_complete_code_and_exact_union_bundle() {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(directory.path().to_string_lossy().as_ref()).unwrap();
        // This format-layer fixture intentionally exceeds the temporary
        // segment-derived codebook bound to exercise the durable u32 schema.
        let page = crate::global_leaf::GlobalLeafPageInput {
            cell_index: 70_000,
            leaf_ordinal: 0,
            centroid_code: vec![1, 2],
            rows: (0..2)
                .map(|ordinal| crate::global_leaf::GlobalLeafRowInput {
                    id: crate::RecordId::from(format!("storage-row-{ordinal}")),
                    stamp: crate::mutation::MutationStamp::new(
                        crate::mutation::MutationVersion::from_parts(ordinal + 1, [1; 16]),
                        [2; 32],
                    ),
                    code: vec![ordinal as u8, 9].into(),
                    exact: [ordinal as f32, ordinal as f32 + 0.5]
                        .into_iter()
                        .flat_map(f32::to_le_bytes)
                        .collect(),
                })
                .collect(),
        };
        let encoded = crate::global_leaf::encode_global_leaf_bundle(
            std::slice::from_ref(&page),
            2,
            VectorElementType::Float32,
        )
        .unwrap();
        let complete_bundle_bytes = encoded.bytes.len() as u64;
        assert!(
            complete_bundle_bytes
                > encoded.code_plane_bytes
                    + encoded
                        .pages
                        .iter()
                        .map(|page| u64::from(page.batch_bytes))
                        .sum::<u64>(),
            "complete object accounting omitted code-batch metadata/schema/footer overhead"
        );

        let mut writer = GlobalLeafPersistenceWriter::new(
            &storage,
            2,
            VectorElementType::Float32,
            "11aa".to_string(),
        )
        .unwrap();
        writer.push_cell_chunk(vec![page]).unwrap();
        writer.finalize_cell(70_000).unwrap();
        let artifacts = writer.finish().unwrap();
        assert_eq!(
            artifacts.storage_bytes,
            complete_bundle_bytes + artifacts.directory.encoded_bytes(),
            "persisted storage bytes must charge the complete Arrow bundle and directory"
        );
    }

    #[test]
    fn v12_writer_splits_near_cap_int8_pages_before_bundle_overflow() {
        const DIMENSIONS: usize = 128;
        const ROWS_PER_PAGE: usize = 480;
        const CODE_WIDTH: usize = 32;
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::from_uri(directory.path().to_string_lossy().as_ref()).unwrap();
        let pages = (0..crate::global_leaf::GLOBAL_LEAF_BUNDLE_MAX_PAGES)
            .map(|page_ordinal| crate::global_leaf::GlobalLeafPageInput {
                cell_index: 0,
                leaf_ordinal: u32::try_from(page_ordinal).unwrap(),
                centroid_code: vec![page_ordinal as u8; CODE_WIDTH],
                rows: (0..ROWS_PER_PAGE)
                    .map(|row_ordinal| crate::global_leaf::GlobalLeafRowInput {
                        id: crate::RecordId::from(format!("p{page_ordinal}-r{row_ordinal}")),
                        stamp: crate::mutation::MutationStamp::new(
                            crate::mutation::MutationVersion::from_parts(
                                u64::try_from(page_ordinal * ROWS_PER_PAGE + row_ordinal + 1)
                                    .unwrap(),
                                [7; 16],
                            ),
                            [8; 32],
                        ),
                        code: vec![row_ordinal as u8; CODE_WIDTH].into(),
                        exact: vec![row_ordinal as u8; DIMENSIONS],
                    })
                    .collect(),
            })
            .collect();
        let mut writer = GlobalLeafPersistenceWriter::new(
            &storage,
            DIMENSIONS,
            VectorElementType::Int8,
            "11aa".to_string(),
        )
        .unwrap();

        writer
            .push_cell_chunk(pages)
            .expect("writer must split before the 48 MiB encoder backstop");
        assert!(
            writer.bundles.len() > 1,
            "near-cap V12 pages must split by bytes before the 376-page count limit"
        );
        assert!(writer.bundles.iter().all(|bundle| {
            bundle.encoded_bytes <= crate::global_leaf::GLOBAL_LEAF_BUNDLE_MAX_ENCODED_BYTES
        }));
    }

    #[test]
    fn v12_serialized_resident_bytes_stay_stable_while_runtime_slots_are_estimated() {
        let inline = valid_ann_ref();
        let mut sharded = inline.clone();
        sharded.incremental_runs[0].directory.shard_count = 3;
        sharded.validate().unwrap();

        assert_eq!(sharded.resident_bytes, inline.resident_bytes);
        assert!(
            sharded.resident_bytes_estimate() > inline.resident_bytes_estimate(),
            "runtime estimate ignored two authenticated fixed shard slots"
        );
        let run = &sharded.incremental_runs[0];
        assert!(
            sharded.resident_bytes_estimate()
                >= sharded
                    .resident_bytes
                    .saturating_add(run.runtime_resident_overhead()),
            "runtime estimate does not cover the derived fixed-slot reservation"
        );
    }

    #[test]
    fn concurrent_selected_cell_loads_share_one_fixed_accounted_shard_slot() {
        let page_count = crate::global_leaf::GLOBAL_LEAF_DIRECTORY_SHARD_PAGES + 1;
        let pages = (0..crate::global_leaf::GLOBAL_LEAF_DIRECTORY_SHARD_PAGES)
            .map(|ordinal| crate::global_leaf::GlobalLeafPageRef {
                cell_index: 7,
                leaf_ordinal: ordinal as u32,
                bundle_index: 0,
                batch_offset: 16_384 + ordinal as u64 * 1536,
                metadata_bytes: 512,
                body_bytes: 1024,
                batch_bytes: 1536,
                code_offset: 64 + ordinal as u64 * 2,
                code_bytes: 2,
                code_checksum: [(ordinal % 251) as u8; 32],
                rows: 1,
                partial_run_count: 0,
                checksum: [(ordinal % 251) as u8; 32],
                centroid_code: vec![7, (ordinal % 251) as u8].into_boxed_slice(),
            })
            .chain(std::iter::once(crate::global_leaf::GlobalLeafPageRef {
                cell_index: 9,
                leaf_ordinal: 0,
                bundle_index: 0,
                batch_offset: 16_384
                    + crate::global_leaf::GLOBAL_LEAF_DIRECTORY_SHARD_PAGES as u64 * 1536,
                metadata_bytes: 512,
                body_bytes: 1024,
                batch_bytes: 1536,
                code_offset: 64 + crate::global_leaf::GLOBAL_LEAF_DIRECTORY_SHARD_PAGES as u64 * 2,
                code_bytes: 2,
                code_checksum: [9; 32],
                rows: 1,
                partial_run_count: 0,
                checksum: [9; 32],
                centroid_code: vec![9, 0].into_boxed_slice(),
            }))
            .collect::<Vec<_>>();
        let bundles = vec![crate::global_leaf::GlobalLeafBundleRef {
            path: "global-leaf/bundles/fixed-slots.arrow".to_owned(),
            checksum: [9; 32],
            encoded_bytes: 16 * 1024 * 1024,
            code_plane_offset: 64,
            code_plane_bytes: page_count as u64 * 2,
            code_plane_checksum: [10; 32],
        }];
        let encoded =
            crate::global_leaf::encode_global_leaf_run_directory("11aa", &pages, &bundles).unwrap();
        let root = crate::global_leaf::decode_global_leaf_run_directory_root("11aa", &encoded.root)
            .unwrap();
        let selected_shards = root.selected_shards(&[9]).unwrap();
        assert_eq!(selected_shards.len(), 1);
        let selected_shard_ordinal = selected_shards[0].0;
        let directory = crate::global_leaf::decode_global_leaf_run_directory(
            "11aa",
            &encoded.root,
            |reference| {
                Ok(encoded
                    .shards
                    .iter()
                    .find(|shard| shard.reference == *reference)
                    .unwrap()
                    .bytes
                    .clone())
            },
        )
        .unwrap();
        let decoded_directory_bytes = directory.resident_bytes();
        let resident = Arc::new(ResidentGlobalLeafRun::new(root, directory, None).unwrap());
        let calls = Arc::new(AtomicUsize::new(0));
        let start = Arc::new(Barrier::new(2));
        let last_page = Arc::new(pages[page_count - 1].clone());

        thread::scope(|scope| {
            for _ in 0..2 {
                let resident = Arc::clone(&resident);
                let calls = Arc::clone(&calls);
                let start = Arc::clone(&start);
                let last_page = Arc::clone(&last_page);
                scope.spawn(move || {
                    start.wait();
                    let (loaded, _, _) = resident
                        .load_shard(selected_shard_ordinal, || {
                            calls.fetch_add(1, Ordering::SeqCst);
                            thread::sleep(Duration::from_millis(30));
                            Ok((vec![last_page.as_ref().clone()], 17))
                        })
                        .unwrap();
                    assert_eq!(loaded.len(), 1);
                });
            }
        });

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(resident.shard_slot_count(), 2);
        assert_eq!(
            resident.resident_bytes(),
            decoded_directory_bytes,
            "V12 persisted resident bytes changed from the full-directory contract"
        );
        assert!(
            resident.runtime_resident_bytes()
                >= decoded_directory_bytes
                    + 2 * std::mem::size_of::<Arc<Vec<crate::global_leaf::GlobalLeafPageRef>>>(),
            "resident reservation omitted fixed slot/Arc overhead"
        );
        let (_, physical_bytes, shared) = resident
            .load_shard(selected_shard_ordinal, || {
                panic!("warm fixed slot reloaded its shard")
            })
            .unwrap();
        assert_eq!(physical_bytes, 0);
        assert!(shared);
    }

    #[test]
    fn source_ranges_coalesce_adjacent_sequences_per_lane_epoch() {
        let ranges = SourceRangeSet::new(vec![
            range(3, 7, 9, 12),
            range(3, 7, 1, 4),
            range(3, 7, 5, 8),
            range(3, 8, 1, 1),
        ])
        .unwrap();

        assert_eq!(ranges.ranges, [range(3, 7, 1, 12), range(3, 8, 1, 1)]);

        let noncanonical: SourceRangeSet = serde_json::from_str(
            r#"{"ranges":[
                {"lane":3,"lease_epoch":7,"first_sequence":1,"last_sequence":4},
                {"lane":3,"lease_epoch":7,"first_sequence":5,"last_sequence":8}
            ]}"#,
        )
        .unwrap();
        assert!(noncanonical.validate_canonical().is_err());
    }

    #[test]
    fn global_ann_rejects_duplicate_levels_mixed_codebooks_and_bad_totals() {
        let mut ann = valid_ann_ref();
        ann.incremental_runs.push(ann.incremental_runs[0].clone());
        assert!(
            ann.validate()
                .unwrap_err()
                .to_string()
                .contains("duplicate V12 leaf-run level")
        );

        let mut ann = valid_ann_ref();
        ann.incremental_runs[0].codebook_checksum = "ff".repeat(32);
        assert!(
            ann.validate()
                .unwrap_err()
                .to_string()
                .contains("codebook checksum")
        );

        let mut ann = valid_ann_ref();
        ann.rows = ann.rows.checked_add(1).unwrap();
        assert!(
            ann.validate()
                .unwrap_err()
                .to_string()
                .contains("row total")
        );
    }

    #[test]
    fn global_ann_reference_preserves_u32_catalog_format_capacity() {
        let mut ann = valid_ann_ref();
        ann.codebook.cell_count = 70_000;
        ann.codebook.candidates = 70_000;
        ann.codebook.probes = 70_000;
        ann.validate().unwrap();

        ann.codebook.cell_count = 0;
        ann.codebook.candidates = 1;
        ann.codebook.probes = 1;
        let error = ann.validate().unwrap_err();
        assert!(error.to_string().contains("positive"), "{error}");
    }

    #[test]
    fn serving_shape_accepts_a_generic_valid_incremental_reference() {
        let mut ann = valid_offline_ann_ref();
        let mut incremental = valid_ann_ref().incremental_runs.remove(0);
        incremental.level = 1;
        incremental.codebook_checksum = ann.codebook.descriptor_checksum.clone();
        ann.coverage = incremental.source_ranges.clone();
        ann.appended_live_rows = incremental.rows;
        ann.rows += incremental.rows;
        ann.storage_bytes += incremental.encoded_bytes;
        ann.resident_bytes += incremental.resident_bytes;
        ann.incremental_runs.push(incremental);
        ann.validate().unwrap();

        ann.validate_serving_shape().unwrap();
    }

    #[test]
    fn serving_shape_binds_base_level_and_row_counters() {
        let mut ann = valid_offline_ann_ref();
        ann.base.as_mut().unwrap().level = 1;
        assert!(ann.validate().is_ok());
        assert!(ann.validate_serving_shape().is_err());

        let mut ann = valid_offline_ann_ref();
        ann.base_rows += 1;
        ann.rows += 1;
        assert!(ann.validate().is_ok());
        assert!(ann.validate_serving_shape().is_err());
    }

    #[test]
    fn codebook_reference_rejects_probes_above_candidates() {
        let mut ann = valid_offline_ann_ref();
        ann.codebook.candidates = 1;
        ann.codebook.probes = 2;
        let error = ann.validate().unwrap_err().to_string();
        assert!(
            error.contains("probes") && error.contains("candidates"),
            "{error}"
        );
    }
}
