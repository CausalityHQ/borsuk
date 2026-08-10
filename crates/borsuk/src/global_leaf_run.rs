use crate::{
    BorsukError, Result, lane_log::GROUP_COMMIT_STRIPE_COUNT, metric::VectorMetric,
    mutation::MutationStamp, record::VectorElementType,
};

pub(crate) const GLOBAL_PQ_REF_LAYOUT_VERSION: u8 = 11;
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
        for pair in ranges.windows(2) {
            let [left, right] = pair else {
                unreachable!("windows(2) always returns pairs");
            };
            if left.lane == right.lane
                && left.lease_epoch == right.lease_epoch
                && left.last_sequence >= right.first_sequence
            {
                return Err(BorsukError::InvalidStorage(
                    "V11 source ranges overlap within one lane lease epoch".to_owned(),
                ));
            }
        }
        Ok(Self { ranges })
    }

    pub(crate) fn ranges(&self) -> &[LaneSourceRange] {
        &self.ranges
    }

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
}

impl ResidentGlobalLeafRun {
    pub(crate) fn new(directory: crate::global_leaf::GlobalLeafRunDirectory) -> Self {
        Self { directory }
    }

    pub(crate) fn directory(&self) -> &crate::global_leaf::GlobalLeafRunDirectory {
        &self.directory
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.directory.resident_bytes()
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

    pub(crate) fn coverage(&self) -> &SourceRangeSet {
        &self.coverage
    }

    pub(crate) fn base_rows(&self) -> u64 {
        self.base_rows
    }

    pub(crate) fn appended_live_rows(&self) -> u64 {
        self.appended_live_rows
    }

    pub(crate) fn leaf_epoch(&self) -> u64 {
        self.leaf_epoch
    }

    pub(crate) fn storage_bytes(&self) -> u64 {
        self.storage_bytes
    }

    pub(crate) fn resident_bytes(&self) -> u64 {
        self.resident_bytes
    }

    pub(crate) fn resident_bytes_estimate(&self) -> usize {
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
            .sum::<usize>();
        std::mem::size_of::<Self>()
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
            )
            .saturating_add(usize::try_from(self.resident_bytes).unwrap_or(usize::MAX))
    }

    #[cfg(test)]
    pub(crate) fn set_appended_live_rows(&mut self, appended_live_rows: u64) {
        self.appended_live_rows = appended_live_rows;
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
    if codebook.probes == 0 || codebook.probes > codebook.cell_count {
        return invalid("V11 global codebook probes must be within the cell count");
    }
    if let VectorMetric::Minkowski { p } = codebook.metric {
        if !p.is_finite() || p < 1.0 {
            return invalid("V11 global codebook metric is invalid");
        }
    }
    Ok(())
}

fn validate_run(run: &GlobalLeafRunRef, codebook: &GlobalCodebookRef, is_base: bool) -> Result<()> {
    if run.layout_version != GLOBAL_PQ_REF_LAYOUT_VERSION {
        return invalid("V11 leaf-run layout version is invalid");
    }
    if run.codebook_checksum != codebook.descriptor_checksum {
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

fn checked_add(left: u64, right: u64, label: &str) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| BorsukError::InvalidStorage(format!("V11 global ANN {label} overflow")))
}

fn invalid(message: &str) -> Result<()> {
    Err(BorsukError::InvalidStorage(message.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{VectorElementType, metric::VectorMetric};

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
            },
        }
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
}
