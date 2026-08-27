use std::{
    collections::{BTreeMap, BTreeSet},
    mem::size_of,
    ops::Range,
    sync::Arc,
};

use crate::{
    BorsukError, Result, VectorElementType, global_leaf::GLOBAL_LEAF_VECTOR_PAYLOAD_BYTES,
    global_pq_sidecar::GlobalScanQuantizer, record::RecordId,
};

pub(crate) const V21_SELECTOR_MAX_CAPACITY_BYTES: u64 = 40_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// One preregistered V21 selector feasibility arm.
pub struct V21FeasibilityArm {
    /// Maximum logical rows in one projected exact bundle.
    pub bundle_row_limit: u16,
    /// Maximum logical rows represented by one selector region.
    pub selector_span: u16,
    /// Optional one-shot duplicate-read hedge delay.
    pub hedge_delay_ms: Option<u16>,
}

impl V21FeasibilityArm {
    /// Validate that this arm belongs to the frozen feasibility matrix.
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.bundle_row_limit, 128 | 256)
            || !matches!(self.selector_span, 32 | 64)
            || !matches!(self.hedge_delay_ms, None | Some(20 | 35))
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V21 feasibility arm is outside the frozen matrix".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn primary_request_limit(&self) -> usize {
        4_usize.saturating_sub(usize::from(self.hedge_delay_ms.is_some()))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct V21ProjectedRow {
    pub(crate) id: RecordId,
    pub(crate) source_ordinal: u64,
    pub(crate) code: Vec<u8>,
    pub(crate) exact: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct V21ProjectedPage {
    pub(crate) cell_index: u32,
    pub(crate) leaf_ordinal: u32,
    pub(crate) group_ordinal: u32,
    pub(crate) group_path: String,
    pub(crate) group_checksum: [u8; 32],
    pub(crate) offset: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) rows: Vec<Arc<V21ProjectedRow>>,
}

#[derive(Debug, Clone)]
pub(crate) struct V21ProjectedRegion {
    pub(crate) centroid_code: Vec<u8>,
    pub(crate) spread_bits: u16,
    pub(crate) row_start: u16,
    pub(crate) row_count: u16,
}

#[derive(Debug, Clone)]
pub(crate) struct V21ProjectedBundle {
    pub(crate) cell_index: u32,
    pub(crate) bundle_ordinal: u32,
    pub(crate) group_ordinal: u32,
    pub(crate) group_path: String,
    pub(crate) group_checksum: [u8; 32],
    pub(crate) offset: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) rows: Vec<Arc<V21ProjectedRow>>,
    pub(crate) regions: Vec<V21ProjectedRegion>,
}

#[derive(Debug, Clone)]
pub(crate) struct V21ProjectedDirectory {
    pub(crate) bundles: Vec<V21ProjectedBundle>,
    pub(crate) selector_capacity_bytes: u64,
    pub(crate) diagnostic_working_set_bytes: u64,
    pub(crate) rows: u64,
    pub(crate) regions: u64,
    selector_slabs: V21SelectorSlabs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Resource bound that stopped extension of a projected ranked prefix.
pub enum V21LimitingBound {
    /// Every routed bundle fit.
    Exhausted,
    /// Adding the next bundle would exceed the primary request limit.
    Requests,
    /// Adding the next bundle would exceed the physical-byte limit.
    Bytes,
    /// Adding the next bundle would exceed physical amplification.
    Amplification,
    /// The highest-ranked bundle could not fit any permitted plan.
    FirstBundle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One query's claim-ineligible V21 feasibility evidence.
pub struct V21FeasibilityQuerySample {
    /// Zero-based arm position in the caller's frozen arm list.
    pub arm_index: usize,
    /// Zero-based query position in the caller's frozen query list.
    pub query_index: usize,
    /// Number of cells selected by the authenticated V20 router.
    pub routed_cells: usize,
    /// Logical exact rows admitted by the projected plan.
    pub selected_rows: u32,
    /// Projected bundles admitted by the ranked prefix.
    pub selected_bundles: usize,
    /// Primary S3 range requests in the projected plan.
    pub primary_requests: usize,
    /// Maximum actual requests including the optional single hedge.
    pub maximum_actual_requests: usize,
    /// Selected bundle bytes excluding coalesced gaps.
    pub selected_bytes: u64,
    /// Physical bytes including coalesced gaps.
    pub physical_bytes: u64,
    /// Ground-truth IDs covered by the selected bundles.
    pub gt_hits: usize,
    /// Ground-truth IDs present in the stable exact top-k result.
    pub recall_hits: usize,
    /// Bound that stopped further prefix extension.
    pub limiting_bound: V21LimitingBound,
}

#[derive(Debug, Clone)]
/// Claim-ineligible V21 feasibility evidence for one arm.
pub struct V21FeasibilityReport {
    /// Frozen arm authority.
    pub arm: V21FeasibilityArm,
    /// Number of projected exact bundles.
    pub bundle_count: usize,
    /// Number of compact selector regions.
    pub region_count: usize,
    /// Allocated capacity of every production selector slab.
    pub projected_directory_bytes: u64,
    /// Whether the projected selector fits the frozen 40,000,000-byte gate.
    pub selector_within_frozen_cap: bool,
    /// Authenticated rows represented by the directory.
    pub rows: u64,
    /// Arm-major, query-major samples.
    pub samples: Vec<V21FeasibilityQuerySample>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V21FeasibilityRead {
    pub(crate) group_ordinal: u32,
    pub(crate) range: Range<u64>,
    pub(crate) selected_bytes: u64,
    pub(crate) bundle_indexes: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V21FeasibilityPlan {
    pub(crate) selected_bundle_indexes: Vec<u32>,
    pub(crate) reads: Vec<V21FeasibilityRead>,
    pub(crate) selected_rows: u32,
    pub(crate) maximum_actual_requests: usize,
    pub(crate) selected_bytes: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) limiting_bound: V21LimitingBound,
}

#[derive(Debug, Clone)]
struct V21SelectorSlabs {
    group_dictionary: Vec<V21SelectorGroup>,
    bundle_group_ordinals: Vec<u32>,
    bundle_cell_indexes: Vec<u32>,
    bundle_ordinals: Vec<u32>,
    bundle_offsets: Vec<u64>,
    bundle_physical_bytes: Vec<u64>,
    bundle_row_counts: Vec<u16>,
    bundle_region_offsets: Vec<u32>,
    region_codes: Vec<u8>,
    region_spreads: Vec<u16>,
    region_row_starts: Vec<u16>,
    region_row_counts: Vec<u16>,
    cell_ids: Vec<u32>,
    cell_offsets: Vec<u32>,
}

#[derive(Debug, Clone)]
struct V21SelectorGroup {
    ordinal: u32,
    path: Box<str>,
    checksum: [u8; 32],
}

impl V21SelectorSlabs {
    fn from_bundles(
        bundles: &[V21ProjectedBundle],
        authenticated_cell_ids: &[u32],
    ) -> Result<Self> {
        let region_count = bundles.iter().try_fold(0_usize, |total, bundle| {
            total.checked_add(bundle.regions.len()).ok_or_else(|| {
                BorsukError::InvalidStorage("V21 selector region count overflows".to_string())
            })
        })?;
        let code_bytes = bundles.iter().try_fold(0_usize, |total, bundle| {
            bundle.regions.iter().try_fold(total, |total, region| {
                total
                    .checked_add(region.centroid_code.len())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("V21 selector code bytes overflow".to_string())
                    })
            })
        })?;
        let mut groups = BTreeMap::<u32, (&str, [u8; 32])>::new();
        for bundle in bundles {
            if let Some((path, checksum)) = groups.get(&bundle.group_ordinal) {
                if *path != bundle.group_path || *checksum != bundle.group_checksum {
                    return Err(BorsukError::InvalidStorage(
                        "V21 group ordinal has conflicting authority".to_string(),
                    ));
                }
            } else {
                groups.insert(
                    bundle.group_ordinal,
                    (&bundle.group_path, bundle.group_checksum),
                );
            }
        }
        let group_dictionary = groups
            .into_iter()
            .map(|(ordinal, (path, checksum))| V21SelectorGroup {
                ordinal,
                path: Box::<str>::from(path),
                checksum,
            })
            .collect::<Vec<_>>();
        let mut cell_ids = authenticated_cell_ids.to_vec();
        cell_ids.sort_unstable();
        cell_ids.dedup();
        if cell_ids.is_empty() || cell_ids.len() != authenticated_cell_ids.len() {
            return Err(BorsukError::InvalidStorage(
                "V21 selector cell dictionary is empty or duplicated".to_string(),
            ));
        }
        if bundles
            .iter()
            .any(|bundle| cell_ids.binary_search(&bundle.cell_index).is_err())
        {
            return Err(BorsukError::InvalidStorage(
                "V21 selector bundle references an unauthenticated router cell".to_string(),
            ));
        }
        let mut slabs = Self {
            group_dictionary,
            bundle_group_ordinals: Vec::with_capacity(bundles.len()),
            bundle_cell_indexes: Vec::with_capacity(bundles.len()),
            bundle_ordinals: Vec::with_capacity(bundles.len()),
            bundle_offsets: Vec::with_capacity(bundles.len()),
            bundle_physical_bytes: Vec::with_capacity(bundles.len()),
            bundle_row_counts: Vec::with_capacity(bundles.len()),
            bundle_region_offsets: Vec::with_capacity(bundles.len() + 1),
            region_codes: Vec::with_capacity(code_bytes),
            region_spreads: Vec::with_capacity(region_count),
            region_row_starts: Vec::with_capacity(region_count),
            region_row_counts: Vec::with_capacity(region_count),
            cell_offsets: vec![0; cell_ids.len() + 1],
            cell_ids,
        };
        slabs.bundle_region_offsets.push(0);
        for bundle in bundles {
            slabs.bundle_group_ordinals.push(bundle.group_ordinal);
            slabs.bundle_cell_indexes.push(bundle.cell_index);
            slabs.bundle_ordinals.push(bundle.bundle_ordinal);
            slabs.bundle_offsets.push(bundle.offset);
            slabs.bundle_physical_bytes.push(bundle.physical_bytes);
            slabs
                .bundle_row_counts
                .push(u16::try_from(bundle.rows.len()).map_err(|_| {
                    BorsukError::InvalidStorage("V21 selector bundle rows exceed u16".to_string())
                })?);
            for region in &bundle.regions {
                slabs.region_codes.extend_from_slice(&region.centroid_code);
                slabs.region_spreads.push(region.spread_bits);
                slabs.region_row_starts.push(region.row_start);
                slabs.region_row_counts.push(region.row_count);
            }
            slabs
                .bundle_region_offsets
                .push(u32::try_from(slabs.region_spreads.len()).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "V21 selector region offset exceeds u32".to_string(),
                    )
                })?);
            let cell = slabs
                .cell_ids
                .binary_search(&bundle.cell_index)
                .map_err(|_| {
                    BorsukError::InvalidStorage(
                        "V21 selector bundle cell is absent from its dictionary".to_string(),
                    )
                })?;
            slabs.cell_offsets[cell + 1] =
                slabs.cell_offsets[cell + 1].checked_add(1).ok_or_else(|| {
                    BorsukError::InvalidStorage("V21 selector cell rows overflow".to_string())
                })?;
        }
        for cell in 1..slabs.cell_offsets.len() {
            let previous = slabs.cell_offsets[cell - 1];
            slabs.cell_offsets[cell] =
                slabs.cell_offsets[cell]
                    .checked_add(previous)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "V21 selector cell offset overflows".to_string(),
                        )
                    })?;
        }
        Ok(slabs)
    }

    fn capacity_bytes(&self) -> u64 {
        fn bytes<T>(values: &Vec<T>) -> u64 {
            u64::try_from(values.capacity().saturating_mul(size_of::<T>())).unwrap_or(u64::MAX)
        }
        bytes(&self.group_dictionary)
            .saturating_add(
                self.group_dictionary
                    .iter()
                    .map(|group| group.path.len() as u64)
                    .sum::<u64>(),
            )
            .saturating_add(bytes(&self.bundle_group_ordinals))
            .saturating_add(bytes(&self.bundle_cell_indexes))
            .saturating_add(bytes(&self.bundle_ordinals))
            .saturating_add(bytes(&self.bundle_offsets))
            .saturating_add(bytes(&self.bundle_physical_bytes))
            .saturating_add(bytes(&self.bundle_row_counts))
            .saturating_add(bytes(&self.bundle_region_offsets))
            .saturating_add(bytes(&self.region_codes))
            .saturating_add(bytes(&self.region_spreads))
            .saturating_add(bytes(&self.region_row_starts))
            .saturating_add(bytes(&self.region_row_counts))
            .saturating_add(bytes(&self.cell_ids))
            .saturating_add(bytes(&self.cell_offsets))
    }
}

impl V21ProjectedDirectory {
    #[cfg(test)]
    fn bundle_row_counts(&self) -> Vec<usize> {
        self.bundles
            .iter()
            .map(|bundle| bundle.rows.len())
            .collect()
    }

    #[cfg(test)]
    fn region_row_counts(&self) -> Vec<usize> {
        self.bundles
            .iter()
            .flat_map(|bundle| &bundle.regions)
            .map(|region| usize::from(region.row_count))
            .collect()
    }

    #[cfg(test)]
    fn canonical_source_ordinals(&self) -> Vec<u64> {
        self.bundles
            .iter()
            .flat_map(|bundle| &bundle.rows)
            .map(|row| row.source_ordinal)
            .collect()
    }

    #[cfg(test)]
    fn selector_identity(&self) -> Vec<V21SelectorIdentity> {
        self.bundles
            .iter()
            .map(|bundle| {
                (
                    bundle.cell_index,
                    bundle.bundle_ordinal,
                    bundle.group_ordinal,
                    bundle.offset,
                    bundle.physical_bytes,
                    bundle
                        .regions
                        .iter()
                        .map(|region| {
                            (
                                region.centroid_code.clone(),
                                region.spread_bits,
                                region.row_start,
                                region.row_count,
                            )
                        })
                        .collect(),
                )
            })
            .collect()
    }

    #[cfg(test)]
    fn group_identity(&self) -> Vec<(u32, String, [u8; 32])> {
        self.selector_slabs
            .group_dictionary
            .iter()
            .map(|group| (group.ordinal, group.path.to_string(), group.checksum))
            .collect()
    }
}

#[cfg(test)]
type V21RegionIdentity = (Vec<u8>, u16, u16, u16);

#[cfg(test)]
type V21SelectorIdentity = (u32, u32, u32, u64, u64, Vec<V21RegionIdentity>);

fn f16_rounded_up(value: f32) -> Result<u16> {
    if !value.is_finite() {
        return Err(BorsukError::InvalidStorage(
            "V21 selector spread is non-finite".to_string(),
        ));
    }
    let rounded = half::f16::from_f32(value);
    if f32::from(rounded) >= value {
        return Ok(rounded.to_bits());
    }
    let bits = rounded.to_bits();
    let next = if bits & 0x8000 == 0 {
        bits.checked_add(1)
    } else {
        bits.checked_sub(1)
    }
    .ok_or_else(|| {
        BorsukError::InvalidStorage("V21 selector spread cannot round outward".to_string())
    })?;
    let next = half::f16::from_bits(next);
    if !next.is_finite() || f32::from(next) < value {
        return Err(BorsukError::InvalidStorage(
            "V21 selector spread cannot be represented by f16".to_string(),
        ));
    }
    Ok(next.to_bits())
}

fn compact_v21_reads(mut reads: Vec<V21FeasibilityRead>) -> Result<Vec<V21FeasibilityRead>> {
    reads.sort_unstable_by_key(|read| (read.group_ordinal, read.range.start, read.range.end));
    let mut compacted = Vec::<V21FeasibilityRead>::with_capacity(reads.len());
    for mut read in reads {
        if read.range.start >= read.range.end || read.selected_bytes == 0 {
            return Err(BorsukError::InvalidStorage(
                "V21 feasibility read is empty".to_string(),
            ));
        }
        if let Some(previous) = compacted.last_mut()
            && previous.group_ordinal == read.group_ordinal
            && previous.range.end >= read.range.start
        {
            previous.range.end = previous.range.end.max(read.range.end);
            previous.selected_bytes = previous
                .selected_bytes
                .checked_add(read.selected_bytes)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V21 feasibility selected bytes overflow".to_string(),
                    )
                })?;
            previous.bundle_indexes.append(&mut read.bundle_indexes);
        } else {
            compacted.push(read);
        }
    }
    Ok(compacted)
}

fn force_v21_read_limit(
    mut reads: Vec<V21FeasibilityRead>,
    request_limit: usize,
) -> Result<Option<Vec<V21FeasibilityRead>>> {
    while reads.len() > request_limit {
        let cheapest = reads
            .windows(2)
            .enumerate()
            .filter(|(_, pair)| pair[0].group_ordinal == pair[1].group_ordinal)
            .filter_map(|(index, pair)| {
                pair[1]
                    .range
                    .start
                    .checked_sub(pair[0].range.end)
                    .map(|gap| (gap, pair[0].group_ordinal, pair[0].range.start, index))
            })
            .min();
        let Some((_, _, _, index)) = cheapest else {
            return Ok(None);
        };
        let right = reads.remove(index + 1);
        let left = &mut reads[index];
        left.range.end = right.range.end.max(left.range.end);
        left.selected_bytes = left
            .selected_bytes
            .checked_add(right.selected_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V21 feasibility selected bytes overflow".to_string())
            })?;
        left.bundle_indexes.extend(right.bundle_indexes);
    }
    Ok(Some(reads))
}

fn v21_read_totals(reads: &[V21FeasibilityRead]) -> Result<(u64, u64)> {
    reads
        .iter()
        .try_fold((0_u64, 0_u64), |(physical, selected), read| {
            let bytes = read
                .range
                .end
                .checked_sub(read.range.start)
                .ok_or_else(|| {
                    BorsukError::InvalidStorage("V21 feasibility read is reversed".to_string())
                })?;
            Ok((
                physical.checked_add(bytes).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V21 feasibility physical bytes overflow".to_string(),
                    )
                })?,
                selected.checked_add(read.selected_bytes).ok_or_else(|| {
                    BorsukError::InvalidStorage(
                        "V21 feasibility selected bytes overflow".to_string(),
                    )
                })?,
            ))
        })
}

pub(crate) fn plan_v21_feasibility_query(
    directory: &V21ProjectedDirectory,
    routed_cells: &[u32],
    query: &[f32],
    quantizer: &GlobalScanQuantizer,
    arm: V21FeasibilityArm,
) -> Result<V21FeasibilityPlan> {
    arm.validate()?;
    if routed_cells.is_empty() {
        return Err(BorsukError::InvalidSearchOptions(
            "V21 feasibility query has no routed cells".to_string(),
        ));
    }
    let routed = routed_cells.iter().copied().collect::<BTreeSet<_>>();
    let mut candidates = directory
        .bundles
        .iter()
        .enumerate()
        .filter(|(_, bundle)| routed.contains(&bundle.cell_index))
        .map(|(bundle_index, bundle)| (bundle_index, bundle, f32::INFINITY))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "V21 feasibility routing selected no bundles".to_string(),
        ));
    }
    let mut region_codes = Vec::<&[u8]>::new();
    let mut region_owners = Vec::<usize>::new();
    let mut region_spreads = Vec::<f32>::new();
    for (candidate_index, (_, bundle, _)) in candidates.iter().enumerate() {
        if bundle.regions.is_empty() {
            return Err(BorsukError::InvalidStorage(
                "V21 feasibility bundle has no selector regions".to_string(),
            ));
        }
        for region in &bundle.regions {
            region_codes.push(&region.centroid_code);
            region_owners.push(candidate_index);
            region_spreads.push(f32::from(half::f16::from_bits(region.spread_bits)));
        }
    }
    let distances = quantizer.score_codes(query, region_codes)?;
    let mut minimum_scores = vec![f32::INFINITY; candidates.len()];
    for ((distance, owner), spread) in distances.into_iter().zip(region_owners).zip(region_spreads)
    {
        let adjusted = distance - spread;
        if !adjusted.is_finite() {
            return Err(BorsukError::InvalidStorage(
                "V21 feasibility selector score is non-finite".to_string(),
            ));
        }
        minimum_scores[owner] = minimum_scores[owner].min(adjusted);
    }
    for (candidate, score) in candidates.iter_mut().zip(minimum_scores) {
        candidate.2 = score;
    }
    candidates.sort_unstable_by(|left, right| {
        left.2
            .total_cmp(&right.2)
            .then_with(|| left.1.cell_index.cmp(&right.1.cell_index))
            .then_with(|| left.1.bundle_ordinal.cmp(&right.1.bundle_ordinal))
            .then_with(|| left.1.group_ordinal.cmp(&right.1.group_ordinal))
            .then_with(|| left.1.group_path.cmp(&right.1.group_path))
            .then_with(|| left.1.offset.cmp(&right.1.offset))
    });

    let mut selected_bundle_indexes = Vec::<u32>::new();
    let mut selected_rows = 0_u32;
    let mut accepted_reads = Vec::<V21FeasibilityRead>::new();
    let mut limiting_bound = V21LimitingBound::Exhausted;
    for (bundle_index, bundle, _) in candidates {
        let end = bundle
            .offset
            .checked_add(bundle.physical_bytes)
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V21 feasibility bundle range overflows".to_string())
            })?;
        let mut proposed = accepted_reads.clone();
        proposed.push(V21FeasibilityRead {
            group_ordinal: bundle.group_ordinal,
            range: bundle.offset..end,
            selected_bytes: bundle.physical_bytes,
            bundle_indexes: vec![u32::try_from(bundle_index).map_err(|_| {
                BorsukError::InvalidStorage("V21 bundle index exceeds u32".to_string())
            })?],
        });
        let proposed = compact_v21_reads(proposed)?;
        let Some(proposed) = force_v21_read_limit(proposed, arm.primary_request_limit())? else {
            limiting_bound = if selected_bundle_indexes.is_empty() {
                V21LimitingBound::FirstBundle
            } else {
                V21LimitingBound::Requests
            };
            break;
        };
        let (physical_bytes, selected_bytes) = v21_read_totals(&proposed)?;
        let rejected = if physical_bytes > 1_048_576
            || proposed
                .iter()
                .any(|read| read.range.end - read.range.start > 1_048_576)
        {
            Some(V21LimitingBound::Bytes)
        } else if physical_bytes > selected_bytes.saturating_mul(2) {
            Some(V21LimitingBound::Amplification)
        } else {
            None
        };
        if let Some(reason) = rejected {
            limiting_bound = if selected_bundle_indexes.is_empty() {
                V21LimitingBound::FirstBundle
            } else {
                reason
            };
            break;
        }
        accepted_reads = proposed;
        selected_bundle_indexes.push(u32::try_from(bundle_index).map_err(|_| {
            BorsukError::InvalidStorage("V21 bundle index exceeds u32".to_string())
        })?);
        selected_rows = selected_rows
            .checked_add(u32::try_from(bundle.rows.len()).map_err(|_| {
                BorsukError::InvalidStorage("V21 selected rows exceed u32".to_string())
            })?)
            .ok_or_else(|| BorsukError::InvalidStorage("V21 selected rows overflow".to_string()))?;
    }
    let (physical_bytes, selected_bytes) = v21_read_totals(&accepted_reads)?;
    let maximum_actual_requests = accepted_reads.len().saturating_add(usize::from(
        arm.hedge_delay_ms.is_some() && !accepted_reads.is_empty(),
    ));
    Ok(V21FeasibilityPlan {
        selected_bundle_indexes,
        reads: accepted_reads,
        selected_rows,
        maximum_actual_requests,
        selected_bytes,
        physical_bytes,
        limiting_bound,
    })
}

fn build_projected_regions(
    rows: &[Arc<V21ProjectedRow>],
    dimensions: usize,
    element_type: VectorElementType,
    normalize: bool,
    quantizer: &GlobalScanQuantizer,
    selector_span: usize,
) -> Result<Vec<V21ProjectedRegion>> {
    let mut regions = Vec::with_capacity(rows.len().div_ceil(selector_span));
    for (region_index, region_rows) in rows.chunks(selector_span).enumerate() {
        let mut sum = vec![0.0_f64; dimensions];
        for row in region_rows {
            let decoded = element_type.decode_fixed_width(&row.exact, dimensions)?;
            let geometry = if normalize {
                crate::metric::unit_l2_normalized(&decoded)
            } else {
                decoded
            };
            for (total, value) in sum.iter_mut().zip(geometry) {
                *total += f64::from(value);
            }
        }
        let denominator = region_rows.len() as f64;
        let centroid = sum
            .into_iter()
            .map(|value| (value / denominator) as f32)
            .collect::<Vec<_>>();
        let centroid_code = quantizer.encode(&centroid)?;
        let spread = quantizer
            .score_codes(&centroid, region_rows.iter().map(|row| row.code.as_slice()))?
            .into_iter()
            .try_fold(f32::NEG_INFINITY, |maximum, score| {
                if score.is_finite() {
                    Ok::<_, BorsukError>(maximum.max(score))
                } else {
                    Err(BorsukError::InvalidStorage(
                        "V21 selector spread input is non-finite".to_string(),
                    ))
                }
            })?;
        regions.push(V21ProjectedRegion {
            centroid_code,
            spread_bits: f16_rounded_up(spread)?,
            row_start: u16::try_from(region_index.saturating_mul(selector_span)).map_err(|_| {
                BorsukError::InvalidStorage("V21 selector row start exceeds u16".to_string())
            })?,
            row_count: u16::try_from(region_rows.len()).map_err(|_| {
                BorsukError::InvalidStorage("V21 selector row count exceeds u16".to_string())
            })?,
        });
    }
    Ok(regions)
}

struct V21ProjectionContext<'a> {
    dimensions: usize,
    element_type: VectorElementType,
    normalize: bool,
    quantizer: &'a GlobalScanQuantizer,
    selector_span: usize,
}

fn finish_projected_bundle(
    bundles: &mut Vec<V21ProjectedBundle>,
    next_bundle: &mut BTreeMap<u32, u32>,
    mut pages: Vec<V21ProjectedPage>,
    context: &V21ProjectionContext<'_>,
) -> Result<()> {
    let first = pages
        .first()
        .ok_or_else(|| BorsukError::InvalidStorage("V21 projected bundle is empty".to_string()))?;
    let cell_index = first.cell_index;
    let group_ordinal = first.group_ordinal;
    let group_path = first.group_path.clone();
    let group_checksum = first.group_checksum;
    let offset = first.offset;
    let physical_bytes = pages.iter().try_fold(0_u64, |total, page| {
        total.checked_add(page.physical_bytes).ok_or_else(|| {
            BorsukError::InvalidStorage("V21 projected bundle bytes overflow".to_string())
        })
    })?;
    let rows = pages
        .iter_mut()
        .flat_map(|page| std::mem::take(&mut page.rows))
        .collect::<Vec<_>>();
    let regions = build_projected_regions(
        &rows,
        context.dimensions,
        context.element_type,
        context.normalize,
        context.quantizer,
        context.selector_span,
    )?;
    let bundle_ordinal = next_bundle.entry(cell_index).or_default();
    bundles.push(V21ProjectedBundle {
        cell_index,
        bundle_ordinal: *bundle_ordinal,
        group_ordinal,
        group_path,
        group_checksum,
        offset,
        physical_bytes,
        rows,
        regions,
    });
    *bundle_ordinal = bundle_ordinal.checked_add(1).ok_or_else(|| {
        BorsukError::InvalidStorage("V21 projected bundle ordinal overflows".to_string())
    })?;
    Ok(())
}

pub(crate) fn build_v21_projected_directory(
    pages: Vec<V21ProjectedPage>,
    dimensions: usize,
    element_type: VectorElementType,
    normalize: bool,
    quantizer: &GlobalScanQuantizer,
    arm: V21FeasibilityArm,
) -> Result<V21ProjectedDirectory> {
    let cell_ids = pages
        .iter()
        .map(|page| page.cell_index)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    build_v21_projected_directory_with_cell_ids(
        pages,
        dimensions,
        element_type,
        normalize,
        quantizer,
        &cell_ids,
        arm,
    )
}

pub(crate) fn build_v21_projected_directory_with_cell_count(
    pages: Vec<V21ProjectedPage>,
    dimensions: usize,
    element_type: VectorElementType,
    normalize: bool,
    quantizer: &GlobalScanQuantizer,
    cell_count: usize,
    arm: V21FeasibilityArm,
) -> Result<V21ProjectedDirectory> {
    let cell_ids = (0..cell_count)
        .map(|cell| {
            u32::try_from(cell).map_err(|_| {
                BorsukError::InvalidStorage("V21 selector cell count exceeds u32".to_string())
            })
        })
        .collect::<Result<Vec<_>>>()?;
    build_v21_projected_directory_with_cell_ids(
        pages,
        dimensions,
        element_type,
        normalize,
        quantizer,
        &cell_ids,
        arm,
    )
}

pub(crate) fn build_v21_projected_directory_with_cell_ids(
    mut pages: Vec<V21ProjectedPage>,
    dimensions: usize,
    element_type: VectorElementType,
    normalize: bool,
    quantizer: &GlobalScanQuantizer,
    authenticated_cell_ids: &[u32],
    arm: V21FeasibilityArm,
) -> Result<V21ProjectedDirectory> {
    arm.validate()?;
    if pages.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "V21 projected directory has no pages".to_string(),
        ));
    }
    let row_bytes = element_type.fixed_width_bytes(dimensions)?;
    let payload_rows = GLOBAL_LEAF_VECTOR_PAYLOAD_BYTES / row_bytes;
    let maximum_rows = usize::from(arm.bundle_row_limit).min(payload_rows);
    if maximum_rows == 0 {
        return Err(BorsukError::InvalidStorage(
            "V21 exact row exceeds the payload cap".to_string(),
        ));
    }
    let code_width = pages
        .first()
        .and_then(|page| page.rows.first())
        .map(|row| row.code.len())
        .filter(|width| *width > 0)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V21 projected code width is empty".to_string())
        })?;
    for page in &pages {
        if page.rows.is_empty()
            || page.physical_bytes == 0
            || page.rows.len() > maximum_rows
            || page
                .rows
                .iter()
                .any(|row| row.code.len() != code_width || row.exact.len() != row_bytes)
        {
            return Err(BorsukError::InvalidStorage(
                "V21 projected page authority is invalid".to_string(),
            ));
        }
    }
    pages.sort_unstable_by_key(|page| {
        (
            page.cell_index,
            page.group_ordinal,
            page.leaf_ordinal,
            page.offset,
        )
    });
    let mut bundles = Vec::new();
    let mut next_bundle = BTreeMap::new();
    let mut pending = Vec::<V21ProjectedPage>::new();
    let context = V21ProjectionContext {
        dimensions,
        element_type,
        normalize,
        quantizer,
        selector_span: usize::from(arm.selector_span),
    };
    for page in pages {
        let can_merge = pending.last().is_none_or(|previous| {
            let pending_rows = pending.iter().map(|page| page.rows.len()).sum::<usize>();
            let pending_bytes = pending.iter().map(|page| page.physical_bytes).sum::<u64>();
            previous.cell_index == page.cell_index
                && previous.group_ordinal == page.group_ordinal
                && previous.group_path == page.group_path
                && previous.group_checksum == page.group_checksum
                && previous.leaf_ordinal.checked_add(1) == Some(page.leaf_ordinal)
                && previous.offset.checked_add(previous.physical_bytes) == Some(page.offset)
                && pending_rows.saturating_add(page.rows.len()) <= maximum_rows
                && pending_bytes.saturating_add(page.physical_bytes)
                    <= crate::global_leaf::GLOBAL_LEAF_MAX_ENCODED_BYTES
        });
        if !can_merge {
            finish_projected_bundle(
                &mut bundles,
                &mut next_bundle,
                std::mem::take(&mut pending),
                &context,
            )?;
        }
        pending.push(page);
    }
    if !pending.is_empty() {
        finish_projected_bundle(&mut bundles, &mut next_bundle, pending, &context)?;
    }
    let rows = bundles.iter().try_fold(0_u64, |total, bundle| {
        total
            .checked_add(bundle.rows.len() as u64)
            .ok_or_else(|| BorsukError::InvalidStorage("V21 row total overflows".to_string()))
    })?;
    let regions = bundles.iter().try_fold(0_u64, |total, bundle| {
        total
            .checked_add(bundle.regions.len() as u64)
            .ok_or_else(|| BorsukError::InvalidStorage("V21 region total overflows".to_string()))
    })?;
    let diagnostic_working_set_bytes = bundles.iter().fold(0_u64, |total, bundle| {
        bundle.rows.iter().fold(total, |total, row| {
            total
                .saturating_add(row.id.as_bytes().len() as u64)
                .saturating_add(row.code.capacity() as u64)
                .saturating_add(row.exact.capacity() as u64)
        })
    });
    let selector_slabs = V21SelectorSlabs::from_bundles(&bundles, authenticated_cell_ids)?;
    let selector_capacity_bytes = selector_slabs.capacity_bytes();
    Ok(V21ProjectedDirectory {
        selector_capacity_bytes,
        diagnostic_working_set_bytes,
        rows,
        regions,
        bundles,
        selector_slabs,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        V21_SELECTOR_MAX_CAPACITY_BYTES, V21FeasibilityArm, V21LimitingBound, V21ProjectedBundle,
        V21ProjectedDirectory, V21ProjectedPage, V21ProjectedRegion, V21ProjectedRow,
        V21SelectorSlabs, build_v21_projected_directory,
        build_v21_projected_directory_with_cell_count, plan_v21_feasibility_query,
    };
    use crate::{
        VectorElementType,
        global_pq_sidecar::GlobalScanQuantizer,
        record::RecordId,
        rotated_product_quantizer::{
            ProductQuantizerConfig, ProductRotation, RotatedProductQuantizer,
        },
    };

    fn test_quantizer(dimensions: usize) -> GlobalScanQuantizer {
        let training = (0..16)
            .map(|row| {
                (0..dimensions)
                    .map(|dimension| (row * 3 + dimension) as f32 / 17.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        GlobalScanQuantizer::from(
            RotatedProductQuantizer::fit(
                ProductQuantizerConfig {
                    rotation: ProductRotation::Identity,
                    seed: 7,
                    dimensions,
                    subspaces: 1,
                    centroids: 4,
                    sample_limit: training.len(),
                    iterations: 2,
                },
                &training,
            )
            .unwrap(),
        )
    }

    fn projected_row(
        quantizer: &GlobalScanQuantizer,
        source_ordinal: u64,
        dimensions: usize,
    ) -> V21ProjectedRow {
        let vector = (0..dimensions)
            .map(|dimension| (source_ordinal as usize % 17 + dimension) as f32 / 17.0)
            .collect::<Vec<_>>();
        V21ProjectedRow {
            id: RecordId::from(format!("row-{source_ordinal:04}").as_str()),
            source_ordinal,
            code: quantizer.encode(&vector).unwrap(),
            exact: vector.into_iter().flat_map(f32::to_le_bytes).collect(),
        }
    }

    struct ProjectedPageSpec {
        cell_index: u32,
        leaf_ordinal: u32,
        group_ordinal: u32,
        offset: u64,
        first_source_ordinal: u64,
        rows: usize,
        dimensions: usize,
    }

    macro_rules! projected_page {
        ($quantizer:expr, $cell:expr, $leaf:expr, $group:expr, $offset:expr, $source:expr, $rows:expr, $dimensions:expr $(,)?) => {
            projected_page_from_spec(
                $quantizer,
                ProjectedPageSpec {
                    cell_index: $cell,
                    leaf_ordinal: $leaf,
                    group_ordinal: $group,
                    offset: $offset,
                    first_source_ordinal: $source,
                    rows: $rows,
                    dimensions: $dimensions,
                },
            )
        };
    }

    fn projected_page_from_spec(
        quantizer: &GlobalScanQuantizer,
        spec: ProjectedPageSpec,
    ) -> V21ProjectedPage {
        let ProjectedPageSpec {
            cell_index,
            leaf_ordinal,
            group_ordinal,
            offset,
            first_source_ordinal,
            rows,
            dimensions,
        } = spec;
        V21ProjectedPage {
            cell_index,
            leaf_ordinal,
            group_ordinal,
            group_path: format!("groups/{group_ordinal}/bundle.arrow"),
            group_checksum: [u8::try_from(group_ordinal).unwrap(); 32],
            offset,
            physical_bytes: u64::try_from(rows).unwrap() * 100,
            rows: (0..rows)
                .map(|row| {
                    Arc::new(projected_row(
                        quantizer,
                        first_source_ordinal + u64::try_from(row).unwrap(),
                        dimensions,
                    ))
                })
                .collect(),
        }
    }

    fn planner_quantizer() -> GlobalScanQuantizer {
        let training = (0..32).map(|row| vec![row as f32]).collect::<Vec<_>>();
        GlobalScanQuantizer::from(
            RotatedProductQuantizer::fit(
                ProductQuantizerConfig {
                    rotation: ProductRotation::Identity,
                    seed: 11,
                    dimensions: 1,
                    subspaces: 1,
                    centroids: 16,
                    sample_limit: training.len(),
                    iterations: 4,
                },
                &training,
            )
            .unwrap(),
        )
    }

    fn planner_directory(
        quantizer: &GlobalScanQuantizer,
        ranges: &[(u32, u64, u64)],
    ) -> V21ProjectedDirectory {
        let bundles = ranges
            .iter()
            .enumerate()
            .map(|(rank, &(group_ordinal, offset, physical_bytes))| {
                let vector = vec![(rank * 2) as f32];
                let row = V21ProjectedRow {
                    id: RecordId::from(format!("rank-{rank}").as_str()),
                    source_ordinal: rank as u64,
                    code: quantizer.encode(&vector).unwrap(),
                    exact: vector.iter().copied().flat_map(f32::to_le_bytes).collect(),
                };
                V21ProjectedBundle {
                    cell_index: 0,
                    bundle_ordinal: rank as u32,
                    group_ordinal,
                    group_path: format!("groups/{group_ordinal}/bundle.arrow"),
                    group_checksum: [u8::try_from(group_ordinal).unwrap(); 32],
                    offset,
                    physical_bytes,
                    rows: vec![Arc::new(row)],
                    regions: vec![V21ProjectedRegion {
                        centroid_code: quantizer.encode(&vector).unwrap(),
                        spread_bits: half::f16::from_f32(0.0).to_bits(),
                        row_start: 0,
                        row_count: 1,
                    }],
                }
            })
            .collect::<Vec<_>>();
        let selector_slabs = V21SelectorSlabs::from_bundles(&bundles, &[0]).unwrap();
        V21ProjectedDirectory {
            selector_capacity_bytes: selector_slabs.capacity_bytes(),
            diagnostic_working_set_bytes: 0,
            rows: bundles.len() as u64,
            regions: bundles.len() as u64,
            bundles,
            selector_slabs,
        }
    }

    #[test]
    fn v21_feasibility_arm_accepts_only_the_frozen_matrix() {
        for bundle_row_limit in [128, 256] {
            for selector_span in [32, 64] {
                for hedge_delay_ms in [None, Some(20), Some(35)] {
                    V21FeasibilityArm {
                        bundle_row_limit,
                        selector_span,
                        hedge_delay_ms,
                    }
                    .validate()
                    .unwrap();
                }
            }
        }
        assert_eq!(
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            }
            .primary_request_limit(),
            4
        );
        assert_eq!(
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: Some(20),
            }
            .primary_request_limit(),
            3
        );
    }

    #[test]
    fn v21_feasibility_arm_rejects_unregistered_values() {
        for arm in [
            V21FeasibilityArm {
                bundle_row_limit: 0,
                selector_span: 32,
                hedge_delay_ms: None,
            },
            V21FeasibilityArm {
                bundle_row_limit: 192,
                selector_span: 32,
                hedge_delay_ms: None,
            },
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 16,
                hedge_delay_ms: None,
            },
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: Some(25),
            },
        ] {
            assert!(arm.validate().is_err(), "accepted {arm:?}");
        }
    }

    #[test]
    fn v21_projected_directory_merges_only_contiguous_same_authority_pages() {
        let quantizer = test_quantizer(2);
        let mut pages = (0..8)
            .map(|block| {
                projected_page!(
                    &quantizer,
                    7,
                    block,
                    3,
                    1_000 + u64::from(block) * 3_200,
                    u64::from(block) * 32,
                    32,
                    2,
                )
            })
            .collect::<Vec<_>>();
        pages.push(projected_page!(&quantizer, 7, 8, 4, 0, 256, 1, 2));
        let canonical_pages = pages.clone();
        pages.reverse();

        let directory = build_v21_projected_directory(
            pages,
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();
        let canonical = build_v21_projected_directory(
            canonical_pages,
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();

        assert_eq!(directory.bundle_row_counts(), [256, 1]);
        assert_eq!(directory.region_row_counts(), [64, 64, 64, 64, 1]);
        assert_eq!(
            directory.canonical_source_ordinals(),
            (0_u64..257).collect::<Vec<_>>()
        );
        assert_eq!(directory.selector_identity(), canonical.selector_identity());
        assert_eq!(directory.rows, 257);
        assert_eq!(directory.regions, 5);
        assert!(directory.selector_capacity_bytes > 0);
        assert_eq!(
            directory.selector_capacity_bytes,
            directory.selector_slabs.capacity_bytes()
        );
        assert!(directory.diagnostic_working_set_bytes > directory.selector_capacity_bytes);
    }

    #[test]
    fn v21_projected_directory_derives_high_dimension_bundle_rows_from_payload() {
        let quantizer = test_quantizer(768);
        let pages = vec![
            projected_page!(&quantizer, 4, 0, 2, 0, 0, 32, 768),
            projected_page!(&quantizer, 4, 1, 2, 3_200, 32, 32, 768),
        ];

        let directory = build_v21_projected_directory(
            pages,
            768,
            VectorElementType::Float32,
            false,
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();

        assert_eq!(directory.bundle_row_counts(), [32, 32]);
        assert_eq!(directory.region_row_counts(), [32, 32]);
    }

    #[test]
    fn v21_projected_directory_does_not_merge_physical_or_ordinal_gaps() {
        let quantizer = test_quantizer(2);
        let pages = vec![
            projected_page!(&quantizer, 1, 0, 9, 0, 0, 1, 2),
            projected_page!(&quantizer, 1, 1, 9, 200, 1, 1, 2),
            projected_page!(&quantizer, 1, 3, 9, 300, 2, 1, 2),
        ];

        let directory = build_v21_projected_directory(
            pages,
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();

        assert_eq!(directory.bundle_row_counts(), [1, 1, 1]);
    }

    #[test]
    fn v21_projected_directory_charges_group_dictionary_payload() {
        let quantizer = test_quantizer(2);
        let short = vec![projected_page!(&quantizer, 1, 0, 9, 0, 0, 1, 2)];
        let mut long = short.clone();
        long[0].group_path = format!("groups/9/{}/bundle.arrow", "nested".repeat(40));
        let arm = V21FeasibilityArm {
            bundle_row_limit: 256,
            selector_span: 64,
            hedge_delay_ms: None,
        };

        let short = build_v21_projected_directory(
            short,
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            arm,
        )
        .unwrap();
        let long = build_v21_projected_directory(
            long,
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            arm,
        )
        .unwrap();

        assert_eq!(short.bundle_row_counts(), long.bundle_row_counts());
        assert_eq!(
            short.group_identity(),
            vec![(9, "groups/9/bundle.arrow".to_string(), [9; 32])]
        );
        assert!(long.selector_capacity_bytes > short.selector_capacity_bytes);
    }

    #[test]
    fn v21_projected_page_clones_share_authenticated_row_storage() {
        let quantizer = test_quantizer(2);
        let page = projected_page!(&quantizer, 1, 0, 9, 0, 0, 1, 2);
        let cloned = page.clone();

        assert!(std::sync::Arc::ptr_eq(&page.rows[0], &cloned.rows[0]));
    }

    #[test]
    fn v21_projected_directory_charges_every_authenticated_router_cell_offset() {
        let quantizer = test_quantizer(2);
        let arm = V21FeasibilityArm {
            bundle_row_limit: 256,
            selector_span: 64,
            hedge_delay_ms: None,
        };
        let page = projected_page!(&quantizer, 1, 0, 9, 0, 0, 1, 2);

        let compact = build_v21_projected_directory_with_cell_count(
            vec![page.clone()],
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            2,
            arm,
        )
        .unwrap();
        let publication_shape = build_v21_projected_directory_with_cell_count(
            vec![page],
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            16_384,
            arm,
        )
        .unwrap();

        assert_eq!(
            publication_shape.selector_capacity_bytes - compact.selector_capacity_bytes,
            (16_384_u64 - 2) * 2 * size_of::<u32>() as u64
        );
    }

    #[test]
    fn v21_projected_directory_records_selector_capacity_above_frozen_ram_gate() {
        let quantizer = test_quantizer(2);
        let directory = build_v21_projected_directory_with_cell_count(
            vec![projected_page!(&quantizer, 0, 0, 9, 0, 0, 1, 2)],
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            5_000_000,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();

        assert!(directory.selector_capacity_bytes > V21_SELECTOR_MAX_CAPACITY_BYTES);
    }

    #[test]
    fn v21_feasibility_plan_stops_before_a_fifth_request_without_mutating_prefix() {
        let quantizer = planner_quantizer();
        let directory = planner_directory(
            &quantizer,
            &[
                (5, 0, 40),
                (1, 900, 40),
                (4, 50, 40),
                (2, 700, 40),
                (3, 200, 40),
            ],
        );

        let plan = plan_v21_feasibility_query(
            &directory,
            &[0],
            &[0.0],
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();

        assert_eq!(plan.selected_bundle_indexes, [0, 1, 2, 3]);
        assert_eq!(plan.reads.len(), 4);
        assert_eq!(plan.maximum_actual_requests, 4);
        assert_eq!(plan.selected_rows, 4);
        assert_eq!(plan.selected_bytes, 160);
        assert_eq!(plan.physical_bytes, 160);
        assert_eq!(plan.limiting_bound, V21LimitingBound::Requests);
    }

    #[test]
    fn v21_feasibility_plan_rejects_amplification_before_committing_candidate() {
        let quantizer = planner_quantizer();
        let directory = planner_directory(
            &quantizer,
            &[(1, 0, 10), (2, 0, 10), (3, 0, 10), (1, 60, 10)],
        );

        let plan = plan_v21_feasibility_query(
            &directory,
            &[0],
            &[0.0],
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: Some(20),
            },
        )
        .unwrap();

        assert_eq!(plan.selected_bundle_indexes, [0, 1, 2]);
        assert_eq!(plan.reads.len(), 3);
        assert_eq!(plan.selected_bytes, 30);
        assert_eq!(plan.physical_bytes, 30);
        assert_eq!(plan.limiting_bound, V21LimitingBound::Amplification);
    }

    #[test]
    fn v21_feasibility_hedge_reserves_one_of_four_actual_request_slots() {
        let quantizer = planner_quantizer();
        let directory = planner_directory(
            &quantizer,
            &[(1, 0, 10), (2, 0, 10), (3, 0, 10), (4, 0, 10)],
        );

        let plan = plan_v21_feasibility_query(
            &directory,
            &[0],
            &[0.0],
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: Some(20),
            },
        )
        .unwrap();

        assert_eq!(plan.selected_bundle_indexes, [0, 1, 2]);
        assert_eq!(plan.reads.len(), 3);
        assert_eq!(plan.maximum_actual_requests, 4);
        assert_eq!(plan.limiting_bound, V21LimitingBound::Requests);
    }

    #[test]
    fn v21_feasibility_plan_rejects_a_first_bundle_above_one_mib() {
        let quantizer = planner_quantizer();
        let directory = planner_directory(&quantizer, &[(1, 0, 1_048_577)]);

        let plan = plan_v21_feasibility_query(
            &directory,
            &[0],
            &[0.0],
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();

        assert!(plan.selected_bundle_indexes.is_empty());
        assert!(plan.reads.is_empty());
        assert_eq!(plan.selected_rows, 0);
        assert_eq!(plan.physical_bytes, 0);
        assert_eq!(plan.limiting_bound, V21LimitingBound::FirstBundle);
    }
}
