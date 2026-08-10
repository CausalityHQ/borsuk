use crate::{
    BorsukError, Result,
    global_pq_sidecar::ResidentGlobalCodebook,
    lane_log::GROUP_COMMIT_STRIPE_COUNT,
    metric::VectorMetric,
    mutation::MutationStamp,
    record::{VectorElementType, VectorRecord},
    storage::Storage,
};

pub(crate) const GLOBAL_PQ_REF_LAYOUT_VERSION: u8 = 11;
pub(crate) const MAX_GLOBAL_LEAF_LEVELS: usize = u64::BITS as usize;
pub(crate) const DRIFT_WINDOW_ROWS: usize = 4096;

#[derive(Clone, Copy, Debug)]
pub(crate) struct LeafRunBuildConfig {
    pub(crate) dimensions: usize,
    pub(crate) element_type: VectorElementType,
    pub(crate) normalize: bool,
}

#[derive(Debug)]
pub(crate) struct UnpublishedGlobalLeafRun {
    level: u8,
    codebook_checksum: String,
    page_refs: Vec<crate::global_leaf::GlobalLeafPageRef>,
    bundles: Vec<crate::global_leaf::GlobalLeafBundleRef>,
    rows: u64,
    sealed_pages: u64,
    partial_pages: u64,
    bundle_bytes: u64,
    min_stamp: Option<MutationStamp>,
    max_stamp: Option<MutationStamp>,
    source_ranges: SourceRangeSet,
    reconstruction_errors_micros: Vec<u64>,
}

impl UnpublishedGlobalLeafRun {
    pub(crate) fn reconstruction_errors_micros(&self) -> &[u64] {
        &self.reconstruction_errors_micros
    }
}

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
    #[allow(dead_code, reason = "Task 4 direct lane-run publication constructor")]
    pub(crate) fn new(
        lane: u16,
        lease_epoch: u64,
        first_sequence: u64,
        last_sequence: u64,
    ) -> Result<Self> {
        let range = Self {
            lane,
            lease_epoch,
            first_sequence,
            last_sequence,
        };
        range.validate()?;
        Ok(range)
    }

    fn validate(&self) -> Result<()> {
        if self.lane >= GROUP_COMMIT_STRIPE_COUNT {
            return invalid("V11 source-range lane is outside the group-commit stripe count");
        }
        if self.lease_epoch == 0 {
            return invalid("V11 source-range lease epoch must be positive");
        }
        if self.first_sequence == 0 || self.last_sequence == 0 {
            return invalid("V11 source-range sequences must be positive");
        }
        if self.first_sequence > self.last_sequence {
            return invalid("V11 source-range first sequence exceeds last sequence");
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
                        "V11 source ranges overlap within one lane lease epoch".to_owned(),
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

    #[allow(dead_code, reason = "Task 4 direct lane-run publication inspection")]
    pub(crate) fn ranges(&self) -> &[LaneSourceRange] {
        &self.ranges
    }

    #[allow(dead_code, reason = "Task 4 source-range coverage subtraction")]
    pub(crate) fn subtract(&self, covered: &Self) -> Result<CoverageDifference> {
        self.validate_canonical()?;
        covered.validate_canonical()?;
        let mut any_overlap = false;
        let mut remaining = Vec::new();
        for candidate in &self.ranges {
            let mut fragments = vec![*candidate];
            for cover in covered.ranges.iter().filter(|cover| {
                cover.lane == candidate.lane && cover.lease_epoch == candidate.lease_epoch
            }) {
                let mut next_fragments =
                    Vec::with_capacity(fragments.len().checked_add(1).ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "V11 source-range fragment count overflow".to_owned(),
                        )
                    })?);
                for fragment in fragments {
                    if cover.last_sequence < fragment.first_sequence
                        || cover.first_sequence > fragment.last_sequence
                    {
                        next_fragments.push(fragment);
                        continue;
                    }
                    any_overlap = true;
                    if fragment.first_sequence < cover.first_sequence {
                        next_fragments.push(LaneSourceRange::new(
                            fragment.lane,
                            fragment.lease_epoch,
                            fragment.first_sequence,
                            cover.first_sequence.checked_sub(1).ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "V11 source-range subtraction underflow".to_owned(),
                                )
                            })?,
                        )?);
                    }
                    if cover.last_sequence < fragment.last_sequence {
                        next_fragments.push(LaneSourceRange::new(
                            fragment.lane,
                            fragment.lease_epoch,
                            cover.last_sequence.checked_add(1).ok_or_else(|| {
                                BorsukError::InvalidStorage(
                                    "V11 source-range subtraction overflow".to_owned(),
                                )
                            })?,
                            fragment.last_sequence,
                        )?);
                    }
                }
                fragments = next_fragments;
            }
            remaining.extend(fragments);
        }
        if remaining.is_empty() {
            Ok(CoverageDifference::FullyCovered)
        } else {
            let difference = Self::new(remaining)?;
            Ok(if any_overlap {
                CoverageDifference::Partial(difference)
            } else {
                CoverageDifference::Disjoint(difference)
            })
        }
    }

    #[allow(dead_code, reason = "Task 4 source-range coverage validation")]
    pub(crate) fn covers(&self, candidate: &Self) -> bool {
        matches!(
            candidate.subtract(self),
            Ok(CoverageDifference::FullyCovered)
        )
    }

    pub(crate) fn union_disjoint(&self, other: &Self) -> Result<Self> {
        let mut ranges = Vec::with_capacity(
            self.ranges
                .len()
                .checked_add(other.ranges.len())
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V11 source-range count overflow".to_owned())
                })?,
        );
        ranges.extend_from_slice(&self.ranges);
        ranges.extend_from_slice(&other.ranges);
        Self::new(ranges)
    }

    fn validate_canonical(&self) -> Result<()> {
        let canonical = Self::new(self.ranges.clone())?;
        if canonical != *self {
            return invalid("V11 source ranges must be sorted canonically");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(dead_code, reason = "Task 4 direct lane-run publication result")]
pub(crate) enum CoverageDifference {
    FullyCovered,
    Disjoint(SourceRangeSet),
    Partial(SourceRangeSet),
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

#[derive(Debug, Clone)]
pub(crate) struct ResidentGlobalLeafRun {
    directory: crate::global_leaf::GlobalLeafRunDirectory,
    level: Option<u8>,
    rows: usize,
}

impl ResidentGlobalLeafRun {
    pub(crate) fn new(
        directory: crate::global_leaf::GlobalLeafRunDirectory,
        level: Option<u8>,
        rows: usize,
    ) -> Self {
        Self {
            directory,
            level,
            rows,
        }
    }

    pub(crate) fn directory(&self) -> &crate::global_leaf::GlobalLeafRunDirectory {
        &self.directory
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.directory.resident_bytes()
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

impl GlobalDriftState {
    fn observe(
        &mut self,
        errors_micros: impl IntoIterator<Item = u64>,
        baseline_p95_micros: u64,
    ) -> Result<bool> {
        self.pending_reconstruction_errors_micros
            .extend(errors_micros);
        while self.pending_reconstruction_errors_micros.len() >= DRIFT_WINDOW_ROWS {
            let mut window = self
                .pending_reconstruction_errors_micros
                .drain(..DRIFT_WINDOW_ROWS)
                .collect::<Vec<_>>();
            window.sort_unstable();
            let rank = DRIFT_WINDOW_ROWS
                .checked_mul(95)
                .and_then(|scaled| scaled.checked_add(99))
                .map(|scaled| scaled / 100)
                .and_then(|rank| rank.checked_sub(1))
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V11 drift nearest-rank p95 calculation overflowed".to_owned(),
                    )
                })?;
            let window_p95 = window[rank];
            if window_p95.saturating_mul(4) > baseline_p95_micros.saturating_mul(5) {
                self.consecutive_breaches = self.consecutive_breaches.saturating_add(1).min(3);
            } else {
                self.consecutive_breaches = 0;
            }
        }
        Ok(self.consecutive_breaches >= 3)
    }
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
                BorsukError::InvalidStorage("V11 global ANN storage total overflow".to_owned())
            })?;
        let resident_bytes = codebook
            .resident_bytes
            .checked_add(base.resident_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V11 global ANN resident total overflow".to_owned())
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

    #[allow(dead_code, reason = "Task 4 incremental run publication")]
    pub(crate) fn coverage(&self) -> &SourceRangeSet {
        &self.coverage
    }

    #[allow(dead_code, reason = "Task 5 purge-compaction accounting")]
    pub(crate) fn base_rows(&self) -> u64 {
        self.base_rows
    }

    #[allow(dead_code, reason = "Task 5 purge-compaction accounting")]
    pub(crate) fn appended_live_rows(&self) -> u64 {
        self.appended_live_rows
    }

    pub(crate) fn leaf_epoch(&self) -> u64 {
        self.leaf_epoch
    }

    pub(crate) fn storage_bytes(&self) -> u64 {
        self.storage_bytes
    }

    #[allow(dead_code, reason = "Task 4 resident run publication accounting")]
    pub(crate) fn resident_bytes(&self) -> u64 {
        self.resident_bytes
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
    }

    #[cfg(test)]
    #[allow(dead_code, reason = "Task 5 purge-compaction fixture mutation")]
    pub(crate) fn set_appended_live_rows(&mut self, appended_live_rows: u64) {
        self.appended_live_rows = appended_live_rows;
    }

    pub(crate) fn with_level_zero_run(
        &self,
        run: GlobalLeafRunRef,
        reconstruction_errors_micros: &[u64],
    ) -> Result<Self> {
        self.validate()?;
        if run.level != 0 {
            return Err(BorsukError::InvalidStorage(
                "direct V11 lane materialization requires a level-zero run".to_owned(),
            ));
        }
        if self
            .incremental_runs
            .iter()
            .any(|existing| existing.level == 0)
        {
            return Err(BorsukError::InvalidStorage(
                "V11 leaf-run level zero is occupied; binary carry maintenance is required"
                    .to_owned(),
            ));
        }
        let mut candidate = self.clone();
        candidate.coverage = candidate.coverage.union_disjoint(&run.source_ranges)?;
        candidate.appended_live_rows = checked_add(
            candidate.appended_live_rows,
            run.rows,
            "appended live-row total",
        )?;
        candidate.rows = checked_add(candidate.rows, run.rows, "row total")?;
        candidate.storage_bytes =
            checked_add(candidate.storage_bytes, run.encoded_bytes, "storage total")?;
        candidate.resident_bytes = checked_add(
            candidate.resident_bytes,
            run.resident_bytes,
            "resident total",
        )?;
        if usize::try_from(run.rows).ok() != Some(reconstruction_errors_micros.len()) {
            return Err(BorsukError::InvalidStorage(
                "V11 leaf-run reconstruction-error count does not match rows".to_owned(),
            ));
        }
        let _drift_rebuild_required = candidate.drift.observe(
            reconstruction_errors_micros.iter().copied(),
            candidate.codebook.reconstruction_error_p95_micros,
        )?;
        candidate.incremental_runs.push(run);
        candidate
            .incremental_runs
            .sort_unstable_by_key(|run| run.level);
        candidate.validate()?;
        Ok(candidate)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.layout_version != GLOBAL_PQ_REF_LAYOUT_VERSION {
            return invalid("V11 global ANN layout version is invalid");
        }
        if self.leaf_epoch == 0 {
            return invalid("V11 global ANN leaf epoch must be nonzero");
        }
        if self.purge_epoch > self.leaf_epoch {
            return invalid("V11 global ANN purge epoch exceeds leaf epoch");
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
                return invalid("V11 leaf-run level exceeds the supported level count");
            }
            if let Some(prior) = previous_level {
                if run.level == prior {
                    return invalid("duplicate V11 leaf-run level");
                }
                if run.level < prior {
                    return invalid("V11 leaf-run levels must be sorted ascending");
                }
            }
            previous_level = Some(run.level);
            validate_run(run, &self.codebook, false)?;
            if run.rows == 0 {
                if saw_coverage_only_deletion {
                    return invalid("V11 global ANN has more than one coverage-only deletion run");
                }
                saw_coverage_only_deletion = true;
            }
            union = union.union_disjoint(&run.source_ranges)?;
            storage_bytes = checked_add(storage_bytes, run.encoded_bytes, "storage total")?;
            resident_bytes = checked_add(resident_bytes, run.resident_bytes, "resident total")?;
        }
        if union != self.coverage {
            return invalid("V11 global ANN source-range coverage does not equal the run union");
        }

        let row_total = checked_add(self.base_rows, self.appended_live_rows, "row total")?;
        if row_total != self.rows {
            return invalid("V11 global ANN row total is inconsistent");
        }
        if self.obsolete_rows > self.rows {
            return invalid("V11 global ANN obsolete rows exceed rows");
        }
        if storage_bytes != self.storage_bytes {
            return invalid("V11 global ANN storage total is inconsistent");
        }
        if resident_bytes != self.resident_bytes {
            return invalid("V11 global ANN resident total is inconsistent");
        }
        if self.drift.pending_reconstruction_errors_micros.len() >= DRIFT_WINDOW_ROWS {
            return invalid("V11 global ANN drift pending errors exceed the drift window");
        }
        if self.drift.consecutive_breaches > 3 {
            return invalid("V11 global ANN drift breach count exceeds three");
        }
        Ok(())
    }

    /// Validate a complete V11 serving shape. Unlike the Task 3 base-only
    /// gate, this accepts authenticated incremental levels while retaining the
    /// invariant that every retired lane range has exactly one searchable run.
    pub(crate) fn validate_serving_shape(&self) -> Result<()> {
        self.validate()?;
        let Some(base) = &self.base else {
            return invalid("V11 serving reference is missing its base run");
        };
        if base.level != 0 || self.base_rows != base.rows {
            return invalid("V11 serving base counters are inconsistent");
        }
        Ok(())
    }
}

fn validate_codebook(codebook: &GlobalCodebookRef) -> Result<()> {
    if codebook.layout_version != GLOBAL_PQ_REF_LAYOUT_VERSION {
        return invalid("V11 global codebook layout version is invalid");
    }
    validate_object_ref(
        "V11 global codebook descriptor",
        &codebook.descriptor_path,
        &codebook.descriptor_checksum,
        codebook.storage_bytes,
    )?;
    if codebook.dimensions == 0 {
        return invalid("V11 global codebook dimensions must be positive");
    }
    codebook
        .element_type
        .fixed_width_bytes(codebook.dimensions)?;
    if codebook.code_width == 0 {
        return invalid("V11 global codebook code width must be positive");
    }
    if codebook.cell_count == 0 || codebook.cell_count > u32::from(u16::MAX) + 1 {
        return invalid("V11 global codebook cell count must fit the u16 identity space");
    }
    if codebook.candidates == 0 || codebook.candidates > codebook.cell_count {
        return invalid("V11 global codebook candidates must be within the cell count");
    }
    if codebook.probes == 0
        || codebook.probes > codebook.cell_count
        || codebook.probes > codebook.candidates
    {
        return invalid("V11 global codebook probes must be within candidates and the cell count");
    }
    if let VectorMetric::Minkowski { p } = codebook.metric
        && (!p.is_finite() || p < 1.0)
    {
        return invalid("V11 global codebook metric is invalid");
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
        return invalid("V11 leaf-run layout version is invalid");
    }
    if run.codebook_checksum != codebook_checksum {
        return invalid("V11 leaf-run codebook checksum does not match the global codebook");
    }
    validate_object_ref(
        "V11 leaf-run directory",
        &run.directory.path,
        &run.directory.checksum,
        run.directory.encoded_bytes,
    )?;
    if run.directory.shard_count == 0 {
        return invalid("V11 leaf-run directory shard count must be positive");
    }
    run.source_ranges.validate_canonical()?;
    if is_base {
        if !run.source_ranges.ranges.is_empty() {
            return invalid("V11 base leaf run must not have lane source ranges");
        }
        if run.rows == 0 || run.pages == 0 || run.bundles == 0 {
            return invalid("V11 base leaf run must have positive rows, pages, and bundles");
        }
    } else {
        if run.source_ranges.ranges.is_empty() {
            return invalid("V11 incremental leaf run must have lane source ranges");
        }
        let has_leaf_rows = run.rows > 0 || run.pages > 0 || run.bundles > 0;
        let complete_leaf_rows = run.rows > 0 && run.pages > 0 && run.bundles > 0;
        if has_leaf_rows && !complete_leaf_rows {
            return invalid(
                "V11 incremental leaf run rows, pages, and bundles must be all positive or all zero",
            );
        }
    }
    match (
        run.rows > 0,
        run.min_stamp.is_some(),
        run.max_stamp.is_some(),
    ) {
        (true, true, true) | (false, false, false) => {}
        _ => return invalid("V11 leaf-run mutation bounds must match leaf rows"),
    }
    if run
        .sealed_pages
        .checked_add(run.partial_pages)
        .ok_or_else(|| BorsukError::InvalidStorage("V11 leaf-run page total overflow".to_owned()))?
        != run.pages
    {
        return invalid("V11 leaf-run sealed and partial pages do not equal pages");
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
    bundles: Vec<crate::global_leaf::GlobalLeafBundleRef>,
    page_refs: Vec<crate::global_leaf::GlobalLeafPageRef>,
    codebook_checksum: String,
    active_cell: Option<(u16, u32)>,
    last_finalized_cell: Option<u16>,
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

    pub(crate) fn finalize_cell(&mut self, cell_index: u16) -> Result<()> {
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

    fn push_direct_pages(
        &mut self,
        pages: Vec<crate::global_leaf::GlobalLeafPageInput>,
    ) -> Result<()> {
        if self.active_cell.is_some() || self.last_finalized_cell.is_some() {
            return Err(BorsukError::InvalidStorage(
                "direct global leaf pages cannot mix with cell continuations".to_owned(),
            ));
        }
        self.add_page_totals(&pages)?;
        let exact_row_bytes = self.element_type.fixed_width_bytes(self.dimensions)?;
        for page in pages {
            let exact_payload = page
                .rows
                .len()
                .checked_mul(exact_row_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("global leaf exact payload overflows".to_owned())
                })?;
            let partial_run_count =
                u8::from(exact_payload != crate::global_leaf::GLOBAL_LEAF_VECTOR_PAYLOAD_BYTES);
            self.push_page(page, partial_run_count)?;
        }
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
        if self.bundle_pages.len() == crate::global_leaf::GLOBAL_LEAF_BUNDLE_MAX_PAGES {
            self.flush_bundle()?;
        }
        self.bundle_pages.push(page);
        self.bundle_partial_run_counts.push(partial_run_count);
        Ok(())
    }

    fn flush_bundle(&mut self) -> Result<()> {
        if self.bundle_pages.is_empty() {
            return Ok(());
        }
        let pages = std::mem::take(&mut self.bundle_pages);
        let partial_run_counts = std::mem::take(&mut self.bundle_partial_run_counts);
        let encoded = crate::global_leaf::encode_global_leaf_bundle(
            &pages,
            self.dimensions,
            self.element_type,
        )?;
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
        "global-leaf/v11/directories/{}/directory-{root_checksum}.parquet",
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
                        "V11 encoded directory lost an external shard".to_owned(),
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

struct DirectEncodedRecord {
    cell: u16,
    scan_code: Vec<u8>,
    row: crate::global_leaf::GlobalLeafRowInput,
    reconstruction_error_micros: u64,
}

pub(crate) fn build_unpublished_leaf_run_from_records(
    storage: &Storage,
    codebook: &ResidentGlobalCodebook,
    codebook_ref: &GlobalCodebookRef,
    records: &[VectorRecord],
    source_ranges: SourceRangeSet,
    level: u8,
    config: LeafRunBuildConfig,
) -> Result<UnpublishedGlobalLeafRun> {
    if source_ranges.ranges().is_empty() {
        return Err(BorsukError::InvalidStorage(
            "direct V11 leaf run requires nonempty source coverage".to_owned(),
        ));
    }
    source_ranges.validate_canonical()?;
    if usize::from(level) >= MAX_GLOBAL_LEAF_LEVELS {
        return Err(BorsukError::InvalidStorage(
            "direct V11 leaf-run level exceeds the supported level count".to_owned(),
        ));
    }
    if config.dimensions != codebook.dimensions()
        || config.dimensions != codebook_ref.dimensions()
        || config.element_type != codebook.vector_element_type()
        || config.element_type != codebook_ref.element_type()
        || config.normalize != codebook.metric().uses_normalized_euclidean_geometry()
        || codebook.metric() != codebook_ref.metric()
        || codebook.code_bytes_per_vector() != codebook_ref.code_width()
    {
        return Err(BorsukError::InvalidStorage(
            "direct V11 leaf build config does not match the resident codebook".to_owned(),
        ));
    }
    let mut encoded_records = Vec::with_capacity(records.len());
    let mut min_stamp = None;
    let mut max_stamp = None;
    for record in records {
        if record.vector.len() != config.dimensions {
            return Err(BorsukError::InvalidStorage(format!(
                "direct V11 leaf record `{}` has the wrong dimensions",
                record.id
            )));
        }
        let stamp = record.mutation_stamp().ok_or_else(|| {
            BorsukError::InvalidStorage(format!(
                "direct V11 leaf record `{}` has no mutation stamp",
                record.id
            ))
        })?;
        let geometry = if config.normalize {
            crate::metric::unit_l2_normalized(&record.vector)
        } else {
            record.vector.clone()
        };
        let encoded = codebook.encode_record(&geometry)?;
        let mut exact = Vec::new();
        config
            .element_type
            .encode_canonical_fixed_width_into(&record.vector, &mut exact)?;
        encoded_records.push(DirectEncodedRecord {
            cell: encoded.cell,
            scan_code: encoded.scan_code,
            row: crate::global_leaf::GlobalLeafRowInput {
                id: record.id.clone(),
                stamp,
                exact,
            },
            reconstruction_error_micros: encoded.reconstruction_error_micros,
        });
        min_stamp = Some(min_stamp.map_or(stamp, |current: MutationStamp| {
            if stamp.version() < current.version() {
                stamp
            } else {
                current
            }
        }));
        max_stamp = Some(max_stamp.map_or(stamp, |current: MutationStamp| {
            if stamp.version() > current.version() {
                stamp
            } else {
                current
            }
        }));
    }
    encoded_records.sort_unstable_by(|left, right| {
        left.cell
            .cmp(&right.cell)
            .then_with(|| left.scan_code.cmp(&right.scan_code))
            .then_with(|| left.row.id.as_bytes().cmp(right.row.id.as_bytes()))
    });

    let mut pages = Vec::new();
    let mut cell_start = 0_usize;
    while cell_start < encoded_records.len() {
        let cell = encoded_records[cell_start].cell;
        let cell_end = encoded_records[cell_start..]
            .iter()
            .position(|record| record.cell != cell)
            .map_or(encoded_records.len(), |offset| cell_start + offset);
        let rows = encoded_records[cell_start..cell_end]
            .iter()
            .map(|record| record.row.clone())
            .collect::<Vec<_>>();
        for (leaf_ordinal, range) in crate::global_leaf::fit_global_leaf_page_ranges(
            &rows,
            config.dimensions,
            config.element_type,
        )?
        .into_iter()
        .enumerate()
        {
            let middle = cell_start + range.start + range.len() / 2;
            pages.push(crate::global_leaf::GlobalLeafPageInput {
                cell_index: cell,
                leaf_ordinal: u32::try_from(leaf_ordinal).map_err(|_| {
                    BorsukError::InvalidStorage("direct V11 leaf ordinal exceeds u32".to_owned())
                })?,
                centroid_code: encoded_records[middle].scan_code.clone(),
                rows: rows[range].to_vec(),
            });
        }
        cell_start = cell_end;
    }

    let reconstruction_errors_micros = encoded_records
        .iter()
        .map(|record| record.reconstruction_error_micros)
        .collect::<Vec<_>>();
    let mut writer = GlobalLeafPersistenceWriter::new(
        storage,
        config.dimensions,
        config.element_type,
        codebook_ref.descriptor_checksum().to_owned(),
    )?;
    writer.push_direct_pages(pages)?;
    writer.flush_bundle()?;
    let sealed_pages = u64::try_from(
        writer
            .page_refs
            .iter()
            .filter(|page| page.partial_run_count == 0)
            .count(),
    )
    .map_err(|_| BorsukError::InvalidStorage("V11 sealed page count exceeds u64".to_owned()))?;
    let page_count = u64::try_from(writer.page_refs.len())
        .map_err(|_| BorsukError::InvalidStorage("V11 page count exceeds u64".to_owned()))?;
    let partial_pages = page_count.checked_sub(sealed_pages).ok_or_else(|| {
        BorsukError::InvalidStorage("V11 partial page count underflows".to_owned())
    })?;
    let rows = u64::try_from(writer.rows)
        .map_err(|_| BorsukError::InvalidStorage("V11 leaf row count exceeds u64".to_owned()))?;
    let run = UnpublishedGlobalLeafRun {
        level,
        codebook_checksum: writer.codebook_checksum,
        page_refs: writer.page_refs,
        bundles: writer.bundles,
        rows,
        sealed_pages,
        partial_pages,
        bundle_bytes: writer.storage_bytes,
        min_stamp,
        max_stamp,
        source_ranges,
        reconstruction_errors_micros,
    };
    validate_unpublished_run(&run)?;
    Ok(run)
}

fn validate_unpublished_run(run: &UnpublishedGlobalLeafRun) -> Result<()> {
    if run.source_ranges.ranges().is_empty() {
        return invalid("unpublished V11 leaf run has empty source coverage");
    }
    let pages = u64::try_from(run.page_refs.len()).unwrap_or(u64::MAX);
    let bundles = u64::try_from(run.bundles.len()).unwrap_or(u64::MAX);
    let directory_rows = run.page_refs.iter().try_fold(0_u64, |total, page| {
        total.checked_add(u64::from(page.rows)).ok_or_else(|| {
            BorsukError::InvalidStorage("unpublished V11 leaf row count overflows".to_owned())
        })
    })?;
    let bundle_bytes = run.bundles.iter().try_fold(0_u64, |total, bundle| {
        total.checked_add(bundle.encoded_bytes).ok_or_else(|| {
            BorsukError::InvalidStorage("unpublished V11 leaf bytes overflow".to_owned())
        })
    })?;
    if directory_rows != run.rows
        || bundle_bytes != run.bundle_bytes
        || run.sealed_pages.checked_add(run.partial_pages) != Some(pages)
        || run.reconstruction_errors_micros.len() != usize::try_from(run.rows).unwrap_or(usize::MAX)
    {
        return invalid("unpublished V11 leaf totals are inconsistent");
    }
    let has_rows = run.rows > 0;
    if has_rows != (pages > 0)
        || has_rows != (bundles > 0)
        || has_rows != run.min_stamp.is_some()
        || has_rows != run.max_stamp.is_some()
    {
        return invalid("unpublished V11 leaf row artifacts are incomplete");
    }
    Ok(())
}

pub(crate) fn persist_unpublished_leaf_run_directory(
    storage: &Storage,
    run: UnpublishedGlobalLeafRun,
) -> Result<GlobalLeafRunRef> {
    validate_unpublished_run(&run)?;
    let persistence = persist_directory_artifacts(
        storage,
        &run.codebook_checksum,
        &run.page_refs,
        &run.bundles,
        run.bundle_bytes,
    )?;
    let reference = GlobalLeafRunRef {
        layout_version: GLOBAL_PQ_REF_LAYOUT_VERSION,
        level: run.level,
        codebook_checksum: run.codebook_checksum,
        directory: persistence.directory,
        rows: run.rows,
        pages: u64::try_from(run.page_refs.len())
            .map_err(|_| BorsukError::InvalidStorage("V11 page count exceeds u64".to_owned()))?,
        bundles: u64::try_from(run.bundles.len())
            .map_err(|_| BorsukError::InvalidStorage("V11 bundle count exceeds u64".to_owned()))?,
        sealed_pages: run.sealed_pages,
        partial_pages: run.partial_pages,
        encoded_bytes: persistence.storage_bytes,
        resident_bytes: persistence.resident_bytes,
        min_stamp: run.min_stamp.map(MutationStampRef::from_stamp),
        max_stamp: run.max_stamp.map(MutationStampRef::from_stamp),
        source_ranges: run.source_ranges,
    };
    let codebook_checksum = reference.codebook_checksum.clone();
    validate_run_against_checksum(&reference, &codebook_checksum, false)?;
    Ok(reference)
}

fn checked_add(left: u64, right: u64, label: &str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| BorsukError::InvalidStorage(format!("V11 global ANN {label} overflow")))
}

fn invalid(message: &str) -> Result<()> {
    Err(BorsukError::InvalidStorage(message.to_owned()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures_util::TryStreamExt;
    use object_store::{ObjectStore, memory::InMemory};

    use super::*;
    use crate::{
        VectorElementType,
        global_pq_sidecar::GlobalCodebookDescriptor,
        metric::VectorMetric,
        mutation::{CanonicalMutation, MutationVersion},
        rotated_product_quantizer::{
            ProductQuantizerConfig, ProductRotation, RotatedProductQuantizer,
        },
    };

    fn range(
        lane: u16,
        lease_epoch: u64,
        first_sequence: u64,
        last_sequence: u64,
    ) -> LaneSourceRange {
        LaneSourceRange::new(lane, lease_epoch, first_sequence, last_sequence).unwrap()
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
            descriptor_path: "global-leaf/v11/codebooks/ab/codebook.parquet".to_owned(),
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
                path: "global-leaf/v11/directories/cd/directory.parquet".to_owned(),
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

    fn resident_codebook() -> (ResidentGlobalCodebook, GlobalCodebookRef) {
        let training = (0..16)
            .map(|row| vec![row as f32, (row * 3) as f32])
            .collect::<Vec<_>>();
        let config = ProductQuantizerConfig {
            rotation: ProductRotation::Identity,
            seed: 17,
            dimensions: 2,
            subspaces: 1,
            centroids: 4,
            sample_limit: training.len(),
            iterations: 2,
        };
        let scan = RotatedProductQuantizer::fit(config.clone(), &training).unwrap();
        let coarse = RotatedProductQuantizer::fit(config, &training).unwrap();
        let descriptor = GlobalCodebookDescriptor::new(
            scan.state(),
            coarse.state(),
            VectorMetric::Euclidean,
            VectorElementType::Float32,
            4,
            4,
            2,
            0,
        )
        .unwrap();
        let resident = ResidentGlobalCodebook::load(descriptor).unwrap();
        let reference = GlobalCodebookRef::new(
            "global-leaf/v11/codebooks/ab/codebook.parquet".to_owned(),
            "ab".repeat(32),
            VectorMetric::Euclidean,
            2,
            VectorElementType::Float32,
            resident.code_bytes_per_vector(),
            4,
            4,
            2,
            0,
            u64::try_from(resident.resident_bytes()).unwrap(),
            1,
        );
        (resident, reference)
    }

    #[test]
    fn source_ranges_reject_overlap_and_preserve_partial_difference() {
        let covered = SourceRangeSet::new(vec![range(3, 7, 4, 8)]).unwrap();
        let candidate = SourceRangeSet::new(vec![range(3, 7, 1, 12)]).unwrap();
        assert_eq!(
            candidate.subtract(&covered).unwrap(),
            CoverageDifference::Partial(
                SourceRangeSet::new(vec![range(3, 7, 1, 3), range(3, 7, 9, 12),]).unwrap()
            )
        );
        assert!(SourceRangeSet::new(vec![range(3, 7, 1, 4), range(3, 7, 4, 5)]).is_err());
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

        assert_eq!(ranges.ranges(), &[range(3, 7, 1, 12), range(3, 8, 1, 1)]);

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
    fn source_range_subtraction_rejects_malformed_deserialized_operands() {
        let malformed: SourceRangeSet = serde_json::from_str(
            r#"{"ranges":[
                {"lane":3,"lease_epoch":7,"first_sequence":1,"last_sequence":4},
                {"lane":3,"lease_epoch":7,"first_sequence":4,"last_sequence":5}
            ]}"#,
        )
        .unwrap();
        let canonical = SourceRangeSet::new(vec![range(3, 7, 1, 5)]).unwrap();

        assert!(malformed.subtract(&canonical).is_err());
        assert!(canonical.subtract(&malformed).is_err());
    }

    #[test]
    fn global_ann_rejects_duplicate_levels_mixed_codebooks_and_bad_totals() {
        let mut ann = valid_ann_ref();
        ann.incremental_runs.push(ann.incremental_runs[0].clone());
        assert!(
            ann.validate()
                .unwrap_err()
                .to_string()
                .contains("duplicate V11 leaf-run level")
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
    fn global_ann_rejects_cell_count_beyond_u16_identity_space() {
        let mut ann = valid_ann_ref();
        ann.codebook.cell_count = 65_536;
        ann.validate().unwrap();

        ann.codebook.cell_count = 65_537;
        assert!(
            ann.validate()
                .unwrap_err()
                .to_string()
                .contains("cell count")
        );
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
    fn level_zero_run_accepts_and_bounds_a_full_drift_window() {
        let mut ann = valid_offline_ann_ref();
        let mut incremental = valid_ann_ref().incremental_runs.remove(0);
        incremental.codebook_checksum = ann.codebook.descriptor_checksum.clone();
        incremental.rows = u64::try_from(DRIFT_WINDOW_ROWS).unwrap();
        let errors = vec![ann.codebook.reconstruction_error_p95_micros; DRIFT_WINDOW_ROWS];

        ann = ann.with_level_zero_run(incremental, &errors).unwrap();

        assert!(ann.drift.pending_reconstruction_errors_micros.is_empty());
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

    #[test]
    fn deletion_only_drain_publishes_coverage_without_arrow_put() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = Storage::from_object_store(
            "memory:///deletion-only-v11-run".to_owned(),
            Arc::clone(&inner),
        )
        .unwrap();
        let (codebook, codebook_ref) = resident_codebook();
        let coverage = SourceRangeSet::new(vec![range(3, 7, 9, 9)]).unwrap();
        let unpublished = build_unpublished_leaf_run_from_records(
            &storage,
            &codebook,
            &codebook_ref,
            &[],
            coverage.clone(),
            0,
            LeafRunBuildConfig {
                dimensions: 2,
                element_type: VectorElementType::Float32,
                normalize: false,
            },
        )
        .unwrap();
        let run = persist_unpublished_leaf_run_directory(&storage, unpublished).unwrap();

        assert_eq!(run.rows, 0);
        assert_eq!(run.pages, 0);
        assert_eq!(run.bundles, 0);
        assert_eq!(run.source_ranges, coverage);
        assert!(run.min_stamp.is_none());
        assert!(run.max_stamp.is_none());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let paths = runtime
            .block_on(inner.list(None).try_collect::<Vec<_>>())
            .unwrap()
            .into_iter()
            .map(|object| object.location.to_string())
            .collect::<Vec<_>>();
        assert_eq!(paths.len(), 1);
        assert!(paths[0].ends_with(".parquet"));
        assert!(!paths.iter().any(|path| path.ends_with(".arrow")));
    }

    #[test]
    fn direct_builder_encodes_resident_records_into_one_partial_bundle() {
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let storage = Storage::from_object_store(
            "memory:///one-record-v11-run".to_owned(),
            Arc::clone(&inner),
        )
        .unwrap();
        let (codebook, codebook_ref) = resident_codebook();
        let record = CanonicalMutation::put(
            MutationVersion::from_parts(5, [7; 16]),
            VectorRecord::new("resident", vec![2.0, 6.0]),
        )
        .unwrap()
        .into_record()
        .unwrap();
        let unpublished = build_unpublished_leaf_run_from_records(
            &storage,
            &codebook,
            &codebook_ref,
            &[record],
            SourceRangeSet::new(vec![range(4, 8, 11, 11)]).unwrap(),
            0,
            LeafRunBuildConfig {
                dimensions: 2,
                element_type: VectorElementType::Float32,
                normalize: false,
            },
        )
        .unwrap();

        assert_eq!(unpublished.rows, 1);
        assert_eq!(unpublished.page_refs.len(), 1);
        assert_eq!(unpublished.bundles.len(), 1);
        assert_eq!(unpublished.sealed_pages, 0);
        assert_eq!(unpublished.partial_pages, 1);
        assert_eq!(unpublished.reconstruction_errors_micros.len(), 1);
    }
}
