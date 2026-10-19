//! Exact, bounded refinement of an authenticated SQ8 shortlist.
use crate::sq8_s3_range::{RangeFetchError, Sq8ReadStats};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Explicit per-query limits; no dataset or shortlist size is implicit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fp32RerankLimits {
    /// Visible SQ8 candidates refined before the final top-k truncation.
    pub shortlist: usize,
    /// Opt in to deterministic minimum-GET row coalescing within the byte cap.
    #[serde(default)]
    pub coalesce: bool,
    /// Maximum covered canonical bytes, including authenticated unscored bridges.
    pub max_bytes: usize,
    /// Maximum logical canonical ranges, independently of transport retry attempts.
    pub max_gets: usize,
    /// Simultaneously active canonical reads.
    pub max_parallel_gets: usize,
}

/// Exact cosine distance, retained in f64 through pending/base merge.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct Fp32RerankHit {
    /// Logical application ID.
    pub id: i64,
    /// Physical immutable ordinal; pending puts have no base ordinal.
    pub ordinal: Option<usize>,
    /// Sequential f64 cosine over the canonical FP32 coordinates.
    pub score: f64,
}
impl PartialEq for Fp32RerankHit {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Fp32RerankHit {}
impl PartialOrd for Fp32RerankHit {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Fp32RerankHit {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .total_cmp(&other.score)
            .then(self.id.cmp(&other.id))
    }
}

/// Original SQ8 plan and charges plus the additional exact stage.
pub struct Fp32RerankResult {
    /// Unchanged SOURCE nomination and SQ8 physical fetch; candidates are its shortlist.
    pub base: crate::two_bit_generation::TwoBitSearchResult,
    /// Visible final results, including pending normalized puts.
    pub candidates: Vec<Fp32RerankHit>,
    /// Canonical record requests and authenticated bytes.
    pub canonical_stats: Sq8ReadStats,
    /// Ordered physical byte ranges, including authenticated bridge rows.
    pub canonical_ranges: Vec<std::ops::Range<usize>>,
    /// Exact fetch, authentication, scalar scoring and pending merge wall time.
    pub rerank_wall_ns: u128,
}

/// Startup HEAD observations for the canonical conditional-read pin.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct Fp32RerankOpenStats {
    /// One canonical HEAD for a populated generation, zero for an empty base.
    pub head_requests: usize,
    /// Wall time for that HEAD.
    pub head_wall_ns: u128,
    /// Root-authenticated resident row hashes, also present in startup metadata counters.
    pub row_digest_bytes: usize,
}

pub(crate) fn cosine(
    query: &[f32],
    values: impl Iterator<Item = f32>,
) -> Result<f64, RangeFetchError> {
    let (mut qnorm, mut norm, mut dot, mut count) = (0_f64, 0_f64, 0_f64, 0_usize);
    for value in values {
        let Some(&q) = query.get(count) else {
            return Err(RangeFetchError::UnexpectedMetadata);
        };
        if !q.is_finite() || !value.is_finite() {
            return Err(RangeFetchError::UnexpectedMetadata);
        }
        let (q, v) = (f64::from(q), f64::from(value));
        qnorm += q * q;
        norm += v * v;
        dot += q * v;
        count += 1;
    }
    if count != query.len()
        || count == 0
        || !norm.is_finite()
        || (norm - 1.).abs() > 1e-6
        || !qnorm.is_finite()
        || (qnorm - 1.).abs() > 1e-6
        || !dot.is_finite()
    {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    let distance = 1. - (dot / (qnorm.sqrt() * norm.sqrt())).clamp(-1., 1.);
    Ok(if distance == 0. { 0. } else { distance })
}

pub(crate) fn authenticate(
    bytes: &[u8],
    expected: &crate::exact_sq8_nominee::ScoredNominee,
    digest: &[u8],
    query: &[f32],
) -> Result<Fp32RerankHit, RangeFetchError> {
    if query.len().checked_mul(4).and_then(|n| n.checked_add(8)) != Some(bytes.len())
        || digest.len() != 32
        || &Sha256::digest(bytes)[..] != digest
        || i64::from_le_bytes(bytes[..8].try_into().unwrap()) != expected.id
    {
        return Err(RangeFetchError::UnexpectedMetadata);
    }
    Ok(Fp32RerankHit {
        id: expected.id,
        ordinal: Some(expected.ordinal),
        score: cosine(
            query,
            bytes[8..]
                .chunks_exact(4)
                .map(|v| f32::from_le_bytes(v.try_into().unwrap())),
        )?,
    })
}

/// Plan sorted unique physical ordinals. Every bridge record is charged.
/// Joining a gap saves exactly one GET; cheapest gaps minimize bytes at each GET count.
/// Equal gaps join from left to right, giving a deterministic frontier.
pub fn plan_ranges(
    ordinals: &[usize],
    rows: usize,
    dimensions: usize,
    limits: Fp32RerankLimits,
) -> Option<Vec<std::ops::Range<usize>>> {
    query_bytes(dimensions, limits)?;
    let width = dimensions.checked_mul(4)?.checked_add(8)?;
    rows.checked_mul(width)?;
    if ordinals.len() > limits.shortlist
        || ordinals.iter().any(|&o| o >= rows)
        || ordinals.windows(2).any(|w| w[0] >= w[1])
    {
        return None;
    }
    let mut covered = ordinals.len();
    let mut gets = ordinals.len();
    let mut joined = vec![false; ordinals.len().saturating_sub(1)];
    if limits.coalesce {
        let mut gaps = Vec::with_capacity(joined.len());
        for (index, pair) in ordinals.windows(2).enumerate() {
            gaps.push((pair[1] - pair[0] - 1, index));
        }
        gaps.sort_unstable();
        for (gap, index) in gaps {
            let next = covered.checked_add(gap)?;
            if next > limits.max_bytes / width {
                break;
            }
            covered = next;
            gets -= 1;
            joined[index] = true;
        }
    }
    if gets > limits.max_gets || covered.checked_mul(width)? > limits.max_bytes {
        return None;
    }
    let mut ranges: Vec<std::ops::Range<usize>> = Vec::with_capacity(ordinals.len());
    for (index, &ordinal) in ordinals.iter().enumerate() {
        let end = ordinal.checked_add(1)?.checked_mul(width)?;
        if index > 0 && joined[index - 1] {
            ranges.last_mut()?.end = end;
        } else {
            ranges.push(ordinal.checked_mul(width)?..end);
        }
    }
    Some(ranges)
}

pub(crate) fn authenticate_range(
    offset: usize,
    bytes: &[u8],
    nominees: &[crate::exact_sq8_nominee::ScoredNominee],
    digests: &[u8],
    query: &[f32],
) -> Result<Vec<Fp32RerankHit>, RangeFetchError> {
    let bad = || RangeFetchError::UnexpectedMetadata;
    let width = query.len().checked_mul(4).and_then(|n| n.checked_add(8)).ok_or_else(bad)?;
    if query.is_empty() || offset % width != 0 || bytes.is_empty() || bytes.len() % width != 0 {
        return Err(bad());
    }
    let first = offset / width;
    let end = first.checked_add(bytes.len() / width).ok_or_else(bad)?;
    if end.checked_mul(32).is_none_or(|n| n > digests.len()) {
        return Err(bad());
    }
    let selected = &nominees[nominees.partition_point(|h| h.ordinal < first)
        ..nominees.partition_point(|h| h.ordinal < end)];
    let mut hits = Vec::with_capacity(selected.len());
    for (index, row) in bytes.chunks_exact(width).enumerate() {
        let ordinal = first + index;
        let digest = &digests[ordinal * 32..(ordinal + 1) * 32];
        if let Ok(index) = selected.binary_search_by_key(&ordinal, |h| h.ordinal) {
            hits.push(authenticate(row, &selected[index], digest, query)?);
        } else if &Sha256::digest(row)[..] != digest {
            return Err(bad());
        }
    }
    Ok(hits)
}

/// Additional query payload, including all completed outcomes and scheduled futures.
/// Per nominee, 256 bytes covers the sorted nominee copy, ordinals, gap pairs,
/// join flags, range capacity, grouped hit payload and the final flattened hits.
/// Three byte-cap copies cover collection/authentication; descriptor growth is separate.
pub fn query_bytes(dimensions: usize, limits: Fp32RerankLimits) -> Option<usize> {
    if dimensions == 0
        || limits.shortlist == 0
        || limits.max_gets == 0
        || (!limits.coalesce && limits.max_gets < limits.shortlist)
        || limits.max_parallel_gets == 0
        || limits.max_bytes
            < dimensions
                .checked_mul(4)?
                .checked_add(8)?
                .checked_mul(limits.shortlist)?
    {
        return None;
    }
    limits
        .max_bytes
        .checked_mul(3)?
        .checked_add(limits.shortlist.checked_mul(256)?)?
        .checked_add(crate::sq8_s3_range::capsule_fetch_descriptor_bytes::<
            Vec<Fp32RerankHit>,
        >(limits.shortlist)?)?
        .checked_add(
            limits
                .shortlist
                .min(limits.max_parallel_gets)
                .checked_mul(crate::sq8_s3_range::RANGE_FUTURE_ALLOWANCE_BYTES)?,
        )?
        .checked_add(dimensions.checked_mul(8)?)?
        .checked_add(8192)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scalar_score_and_full_id_authentication() {
        let q = [1_f32, 0., 0., 0., 0., 0., 0.];
        let mut record = (-91_i64).to_le_bytes().to_vec();
        record.extend(q.into_iter().flat_map(f32::to_le_bytes));
        let digest = Sha256::digest(&record);
        let expected = crate::exact_sq8_nominee::ScoredNominee {
            id: -91,
            ordinal: 32,
            score: 0.,
        };
        let hit = authenticate(&record, &expected, &digest, &q).unwrap();
        assert_eq!(hit.score.to_bits(), 0_f64.to_bits());
        record[0] ^= 1;
        assert!(authenticate(&record, &expected, &digest, &q).is_err());
        let forged = Sha256::digest(&record);
        assert!(authenticate(&record, &expected, &forged, &q).is_err());
        for d in [1, 7, 768, 1024, 1536] {
            for shortlist in [1, 3, 16, 33] {
                let limits = Fp32RerankLimits {
                    coalesce: false,
                    shortlist,
                    max_bytes: (8 + 4 * d) * shortlist,
                    max_gets: shortlist,
                    max_parallel_gets: 3,
                };
                assert!(query_bytes(d, limits).is_some());
                assert!(
                    query_bytes(
                        d,
                        Fp32RerankLimits {
                            max_bytes: limits.max_bytes - 1,
                            ..limits
                        }
                    )
                    .is_none()
                );
            }
        }
        assert!(
            query_bytes(
                usize::MAX,
                Fp32RerankLimits {
                    coalesce: false,
                    shortlist: 1,
                    max_bytes: usize::MAX,
                    max_gets: 1,
                    max_parallel_gets: 1
                }
            )
            .is_none()
        );
    }
}

pub(crate) fn merge(
    exact: Vec<Fp32RerankHit>,
    query: &[f32],
    top_k: usize,
    mutations: Option<&crate::two_bit_mutations::TwoBitMutationSnapshot>,
) -> Result<Vec<Fp32RerankHit>, RangeFetchError> {
    let mut heap = std::collections::BinaryHeap::with_capacity(top_k);
    let mut offer = |hit: Fp32RerankHit| {
        if heap.len() < top_k {
            heap.push(hit);
        } else if heap.peek().is_some_and(|worst| hit < *worst) {
            *heap.peek_mut().unwrap() = hit;
        }
    };
    for hit in exact {
        offer(hit);
    }
    if let Some(mutations) = mutations {
        for row in mutations.rows() {
            if let Some(vector) = &row.vector {
                offer(Fp32RerankHit {
                    id: row.id,
                    ordinal: None,
                    score: cosine(query, vector.iter().copied())?,
                });
            }
        }
    }
    Ok(heap.into_sorted_vec())
}

/// Recompute the complete remote-generation model from authenticated root geometry.
/// Metadata includes all startup files; caller pins include retained generations.
pub fn modeled_remote_memory(
    root_json: &[u8], metadata_bytes: u64,
    mut limits: crate::two_bit_generation::TwoBitGenerationLimits,
    refinement: Fp32RerankLimits,
    authentication: crate::sq8_s3_range::SourceAuthentication,
    split: Option<crate::sq8_s3_range::Sq8RangeSplit>,
    byte_window_state_bytes: usize,
    semantic_leaf_limit: Option<usize>,
) -> Result<u64, crate::two_bit_generation::TwoBitGenerationError> {
    use crate::two_bit_generation::{Discovery, Manifest, TwoBitGenerationError};
    let bad = TwoBitGenerationError::Invalid;
    let root: Manifest = serde_json::from_slice(root_json).map_err(|_| bad("model root schema"))?;
    if !root.valid_descriptor() || root.low.len() != root.canonical.dimensions {
        return Err(bad("model root identity"));
    }
    let router = match &root.discovery {
        Discovery::Semantic { root_bytes, .. } => *root_bytes as u64,
        _ => 0,
    };
    if matches!(root.discovery, Discovery::Semantic { .. }) {
        limits.max_source_bytes = limits.max_source_bytes.min(64 * 1024 * 1024);
        limits.max_source_gets = limits.max_source_gets.min(128);
    }
    let (mut modeled, _, _) = crate::two_bit_generation::generation_memory(
        &root, limits, true, false, metadata_bytes, router, semantic_leaf_limit,
    )?;
    modeled = modeled.checked_add(authentication.additional_memory_bytes(
        limits.max_parallel_source_gets, limits.max_source_gets, limits.max_active_queries,
    ).ok_or(bad("model authentication"))?)
        .and_then(|n| n.checked_add(byte_window_state_bytes as u64))
        .ok_or(bad("model memory"))?;
    if let Some(split) = split {
        let scratch = split.scratch_bytes(limits.max_query_bytes, limits.max_parallel_gets)
            .and_then(|n| n.checked_sub(crate::sq8_s3_range::Sq8RangeTrace::checked_pending_futures_bytes(
                split.max_physical_gets, limits.max_parallel_gets,
            )?)).ok_or(bad("model split"))?;
        let previous = crate::two_bit_generation::range_scheduler_memory(limits, true, limits.max_query_gets)?;
        let scheduler = crate::two_bit_generation::range_scheduler_memory(limits, true, split.max_physical_gets)?;
        modeled = modeled.checked_sub(previous)
            .and_then(|n| n.checked_add(scheduler))
            .and_then(|n| n.checked_add((scratch as u64).checked_mul(limits.max_active_queries as u64)?))
            .ok_or(bad("model split"))?;
    }
    modeled.checked_add((query_bytes(root.canonical.dimensions, refinement)
        .ok_or(bad("model FP32 limits"))? as u64)
        .checked_mul(limits.max_active_queries as u64).ok_or(bad("model active queries"))?)
        .ok_or(bad("model memory"))
}

#[cfg(test)]
mod coalescing_tests {
    use super::*;

    #[test]
    fn coalescing_frontier_matches_independent_partition_enumeration() {
        for mask in 1_usize..256 {
            let rows = (0..8).filter(|i| mask & (1 << i) != 0).collect::<Vec<_>>();
            for budget in rows.len()..=10 {
                for cap in 1..=rows.len() {
                    let mut best = None;
                    for cuts in 0..(1 << rows.len().saturating_sub(1)) {
                        let mut first = 0;
                        let mut covered = 0;
                        let mut gets = 0;
                        for index in 0..rows.len() {
                            if index + 1 == rows.len() || cuts & (1 << index) != 0 {
                                covered += rows[index] - rows[first] + 1;
                                gets += 1;
                                first = index + 1;
                            }
                        }
                        if covered <= budget && gets <= cap {
                            best = Some(best.map_or((gets, covered), |old| std::cmp::min(old, (gets, covered))));
                        }
                    }
                    let limits = Fp32RerankLimits { coalesce: true, shortlist: rows.len(),
                        max_bytes: budget * 36, max_gets: cap, max_parallel_gets: 3 };
                    let actual = plan_ranges(&rows, 8, 7, limits).map(|p| {
                        assert!(p.capacity() <= rows.len());
                        (p.len(), p.iter().map(|r|r.len()/36).sum::<usize>())
                    });
                    assert_eq!(actual, best, "{rows:?} {budget} {cap}");
                }
            }
        }
    }

    #[test]
    fn coalescing_caps_ties_tail_and_checked_geometry() {
        let limits = Fp32RerankLimits { coalesce: true, shortlist: 3,
            max_bytes: 4 * 12, max_gets: 2, max_parallel_gets: 1 };
        assert_eq!(plan_ranges(&[0,2,4], 5, 1, limits).unwrap(), vec![0..36,48..60]);
        assert!(plan_ranges(&[0,2,4], 5, 1, Fp32RerankLimits {max_gets:1,..limits}).is_none());
        assert_eq!(plan_ranges(&[4], 5, 1, limits).unwrap(), vec![48..60]);
        for rows in [&[0,0][..], &[2,1], &[5]] {
            assert!(plan_ranges(rows, 5, 1, limits).is_none());
        }
        assert!(plan_ranges(&[0], usize::MAX, 1, limits).is_none());
        assert!(plan_ranges(&[0], 1, usize::MAX, limits).is_none());
        assert!(plan_ranges(&[0], 1, 0, limits).is_none());
        assert!(plan_ranges(&[0], 1, 1, Fp32RerankLimits {max_bytes:35,..limits}).is_none());
        assert!(plan_ranges(&[0], 1, 1, Fp32RerankLimits {max_parallel_gets:0,..limits}).is_none());
        assert!(plan_ranges(&[], 0, 1, limits).unwrap().is_empty());
        for dimensions in [1,7,768,1536] {
            let limits = Fp32RerankLimits {coalesce:true,shortlist:3,max_bytes:100*(8+4*dimensions),
                max_gets:1,max_parallel_gets:2};
            let model = query_bytes(dimensions, limits).unwrap();
            let actual_descriptors = 3 * (
                std::mem::size_of::<crate::exact_sq8_nominee::ScoredNominee>()
                + std::mem::size_of::<usize>() + std::mem::size_of::<(usize,usize)>()
                + std::mem::size_of::<bool>() + std::mem::size_of::<std::ops::Range<usize>>()
                + 2*std::mem::size_of::<Fp32RerankHit>());
            assert!(actual_descriptors <= 3*256);
            assert!(model >= 3*limits.max_bytes+actual_descriptors);
        }
    }

    #[test]
    fn coalescing_authenticates_bridges_but_never_scores_them() {
        use crate::exact_sq8_nominee::ScoredNominee;
        let mut bytes = Vec::new();
        for (id, value) in [(-91_i64,1_f32),(i64::MAX,f32::NAN),(17_i64,1_f32)] {
            bytes.extend(id.to_le_bytes());bytes.extend(value.to_le_bytes());
        }
        let digests = bytes.chunks_exact(12).flat_map(|r|Sha256::digest(r)).collect::<Vec<_>>();
        let nominees = [ScoredNominee{id:-91,ordinal:0,score:0.},ScoredNominee{id:17,ordinal:2,score:0.}];
        let hits = authenticate_range(0,&bytes,&nominees,&digests,&[1.]).unwrap();
        assert_eq!(hits.capacity(),2);
        assert_eq!(hits.iter().map(|h|h.id).collect::<Vec<_>>(),vec![-91,17]);
        assert!(hits.iter().all(|h|h.score.to_bits()==0_f64.to_bits()));
        for index in [0,8,12,20,24,32] {
            let mut bad=bytes.clone();bad[index]^=1;
            assert!(authenticate_range(0,&bad,&nominees,&digests,&[1.]).is_err());
        }
        let mut forged=bytes.clone();forged[0]^=1;
        let forged_digests=forged.chunks_exact(12).flat_map(|r|Sha256::digest(r)).collect::<Vec<_>>();
        assert!(authenticate_range(0,&forged,&nominees,&forged_digests,&[1.]).is_err());
        assert!(authenticate_range(1,&bytes,&nominees,&digests,&[1.]).is_err());
        assert!(authenticate_range(0,&bytes[..35],&nominees,&digests,&[1.]).is_err());
        assert!(authenticate_range(0,&bytes,&nominees,&digests[..95],&[1.]).is_err());
    }
}
