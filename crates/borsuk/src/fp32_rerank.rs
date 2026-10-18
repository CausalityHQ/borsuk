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
    /// Sum of individual canonical record bytes.
    pub max_bytes: usize,
    /// Maximum individual record GETs; adjacent records are not combined.
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

/// Additional query payload, including all completed outcomes and scheduled futures.
pub fn query_bytes(dimensions: usize, limits: Fp32RerankLimits) -> Option<usize> {
    if limits.shortlist == 0
        || limits.max_gets < limits.shortlist
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
        .checked_add(limits.shortlist.checked_mul(128)?)?
        .checked_add(crate::sq8_s3_range::capsule_fetch_descriptor_bytes::<
            Fp32RerankHit,
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
        &root, limits, true, false, metadata_bytes, router,
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
