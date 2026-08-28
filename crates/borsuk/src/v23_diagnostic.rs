use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{BorsukError, Result};

pub(crate) const V23_PAGE_MAX_ENCODED_BYTES: u64 = 245_760;
pub(crate) const V23_WAVE_MAX_PAGES: usize = 4;
pub(crate) const V23_WAVE_MAX_BYTES: u64 = 983_040;
pub(crate) const V23_PROCESS_MAX_BYTES: u64 = 3 * 1024 * 1024 * 1024;
pub(crate) const V23_DIAGNOSTIC_QUERIES: usize = 32;
#[allow(dead_code, reason = "consumed by the planned D3 benchmark slice")]
pub(crate) const V23_D3_WAVES: usize = 1_000;
const V23_D1_CPU_MAX_NS: u64 = 15_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Production SIMD quantizer family evaluated by V23.
pub enum V23QuantizerFamily {
    /// Seeded SRHT product quantization with one byte per subspace.
    SrhtPq,
    /// Data-oblivious Fast-TurboQuant MSE scan codec.
    FastTurboQuantMse,
    /// Two-stage production Fast-TurboQuant scan codec.
    FastTurboQuantProd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
/// Canonical identity of one D1 quantizer arm.
pub struct V23D1ArmKey {
    /// Production quantizer family.
    pub family: V23QuantizerFamily,
    /// Fixed encoded bytes carried by every row.
    pub code_width_bytes: u8,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Ordered approximate top-ten result retained as scientific evidence.
pub struct V23RankedResult {
    /// Authenticated raw record IDs in rank order.
    pub ids: Vec<Vec<u8>>,
    /// Approximate distances paired with `ids`.
    pub distances: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// D1 code-fidelity evidence for one frozen query.
pub struct V23D1QuerySample {
    /// Zero-based position in the frozen query authority.
    pub query_index: u32,
    /// Exact ground-truth top-ten record IDs.
    pub ground_truth_ids: Vec<Vec<u8>>,
    /// Code-ranked result over the exact top-2,048 oracle pool.
    pub oracle: V23RankedResult,
    /// Code-ranked result over the complete registered routed pool.
    pub routed: V23RankedResult,
    /// Exact oracle-pool row count.
    pub oracle_candidate_rows: u32,
    /// Complete routed-pool row count.
    pub routed_candidate_rows: u64,
    /// Recomputed ground-truth hits in `oracle`.
    pub oracle_hits: u8,
    /// Recomputed ground-truth hits in `routed`.
    pub routed_hits: u8,
    /// Query preparation plus both production SIMD scans.
    pub cpu_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Aggregate D1 evidence for one quantizer family and width.
pub struct V23D1Arm {
    /// Canonical arm identity.
    pub key: V23D1ArmKey,
    /// SHA-256 of the complete serialized quantizer state.
    pub quantizer_checksum: String,
    /// Query-major scientific evidence.
    pub query_samples: Vec<V23D1QuerySample>,
    /// Oracle-pool recall in parts per million.
    pub oracle_recall_ppm: u64,
    /// Routed-pool recall in parts per million.
    pub routed_recall_ppm: u64,
    /// Nearest-rank p99 CPU time across frozen queries.
    pub cpu_p99_ns: u64,
    /// Conservative encoded-byte projection for four maximum pages.
    pub four_page_projected_bytes: u64,
    /// Exact result of every D1 scientific gate.
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Complete claim-ineligible V23 D1 report.
pub struct V23D1Report {
    /// Exact evidence schema name.
    pub schema: String,
    /// Authenticated source V20 cell-card root checksum.
    pub v20_root_checksum: String,
    /// Authenticated source V20 codebook checksum.
    pub v20_codebook_checksum: String,
    /// SHA-256 of the ordered quantizer-training sample ordinals.
    pub sample_ordinals_checksum: String,
    /// Live rows covered by the immutable source generation.
    pub rows: u64,
    /// Complete source routing-cell count.
    pub routing_cell_count: usize,
    /// Canonically ordered quantizer arms.
    pub arms: Vec<V23D1Arm>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Authenticated immutable V23 diagnostic posting-page reference.
pub struct V23PageRef {
    /// Contiguous zero-based page ordinal.
    pub page_ordinal: u32,
    /// Content-addressed object path.
    pub path: String,
    /// SHA-256 of the complete encoded page.
    pub checksum: String,
    /// Complete encoded object length.
    pub encoded_bytes: u64,
    /// Unique authoritative rows owned by the page.
    pub primary_rows: u32,
    /// Boundary rows replicated into the page.
    pub replicated_rows: u32,
    /// Full-dimensional routing centroid.
    pub centroid: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// D2 page-routing and code-ranking evidence for one frozen query.
pub struct V23D2QuerySample {
    /// Zero-based position in the frozen query authority.
    pub query_index: u32,
    /// Sorted immutable pages fixed before simulated I/O.
    pub page_ordinals: Vec<u32>,
    /// Sum of complete selected page lengths.
    pub encoded_bytes: u64,
    /// Rows scanned before replica deduplication.
    pub candidate_rows: u64,
    /// Exact ground-truth top-ten record IDs.
    pub ground_truth_ids: Vec<Vec<u8>>,
    /// Code-ranked, replica-deduplicated top-ten result.
    pub ranked: V23RankedResult,
    /// Ground-truth rows physically covered by selected pages.
    pub gt_page_hits: u8,
    /// Ground-truth rows returned in `ranked`.
    pub hits: u8,
    /// Per-query recall in parts per million.
    pub recall_ppm: u64,
    /// Router preparation plus production SIMD ranking time.
    pub cpu_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Aggregate D2 evidence for one quantizer/page/replication arm.
pub struct V23D2Arm {
    /// Passing D1 quantizer authority used by this arm.
    pub d1_key: V23D1ArmKey,
    /// Registered primary rows targeted per page.
    pub primary_target_rows: u16,
    /// Maximum primary plus replica assignments permitted per row.
    pub maximum_assignments_per_row: u8,
    /// Complete immutable page directory.
    pub pages: Vec<V23PageRef>,
    /// Unique live corpus rows.
    pub unique_rows: u64,
    /// Primary plus replica row assignments.
    pub total_assignments: u64,
    /// `total_assignments / unique_rows` in parts per million.
    pub storage_amplification_ppm: u64,
    /// Complete encoded resident root bytes.
    pub root_bytes: u64,
    /// Conservative serving-process RAM projection.
    pub projected_ram_bytes: u64,
    /// Measured peak builder resident set.
    pub build_peak_rss_bytes: u64,
    /// Query-major page simulation evidence.
    pub query_samples: Vec<V23D2QuerySample>,
    /// Aggregate recall in parts per million.
    pub aggregate_recall_ppm: u64,
    /// Worst frozen-query recall in parts per million.
    pub minimum_query_recall_ppm: u64,
    /// Nearest-rank p99 CPU time across frozen queries.
    pub cpu_p99_ns: u64,
    /// Exact result of every D2 scientific gate.
    pub passed: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
/// Complete claim-ineligible V23 D2 report.
pub struct V23D2Report {
    /// Exact evidence schema name.
    pub schema: String,
    /// SHA-256 of the prerequisite canonical D1 report.
    pub d1_report_checksum: String,
    /// Unique live corpus rows.
    pub rows: u64,
    /// Canonically ordered D2 arms.
    pub arms: Vec<V23D2Arm>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
/// Query-scoped physical evidence for one real D3 S3 wave.
pub struct V23WaveSample {
    /// Zero-based position in the registered query authority.
    pub query_index: u32,
    /// Sorted page ordinals issued concurrently.
    pub page_ordinals: Vec<u32>,
    /// Sum of complete selected page lengths.
    pub encoded_bytes: u64,
    /// Rows scanned before replica deduplication.
    pub candidate_rows: u64,
    /// Query-scoped S3 Standard GET count.
    pub backing_gets: u32,
    /// Query-scoped S3 Standard response bytes.
    pub backing_bytes: u64,
    /// Query preparation, decode, and SIMD ranking time.
    pub cpu_ns: u64,
    /// Complete measured cold-query wall time.
    pub elapsed_ns: u64,
}

pub(crate) fn validate_wave_sample(sample: &V23WaveSample) -> Result<()> {
    if sample.page_ordinals.is_empty()
        || sample.page_ordinals.len() > V23_WAVE_MAX_PAGES
        || sample
            .page_ordinals
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || sample.encoded_bytes == 0
        || sample.encoded_bytes > V23_WAVE_MAX_BYTES
        || sample.candidate_rows == 0
        || usize::try_from(sample.backing_gets).ok() != Some(sample.page_ordinals.len())
        || sample.backing_bytes != sample.encoded_bytes
        || sample.cpu_ns == 0
        || sample.elapsed_ns == 0
    {
        return Err(BorsukError::InvalidStorage(
            "V23 wave authority differs".to_string(),
        ));
    }
    Ok(())
}

fn valid_checksum(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_ranked_result(result: &V23RankedResult) -> Result<()> {
    if result.ids.len() != 10
        || result.distances.len() != result.ids.len()
        || result.ids.iter().any(Vec::is_empty)
        || result.ids.iter().collect::<BTreeSet<_>>().len() != result.ids.len()
        || result
            .distances
            .iter()
            .any(|distance| !distance.is_finite())
        || (1..result.ids.len()).any(|index| {
            result.distances[index - 1]
                .total_cmp(&result.distances[index])
                .then_with(|| result.ids[index - 1].cmp(&result.ids[index]))
                .is_gt()
        })
    {
        return Err(BorsukError::InvalidStorage(
            "V23 ranked result authority differs".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_d1_report(report: &V23D1Report) -> Result<()> {
    if report.schema != "borsuk-v23-d1-v1"
        || !valid_checksum(&report.v20_root_checksum)
        || !valid_checksum(&report.v20_codebook_checksum)
        || !valid_checksum(&report.sample_ordinals_checksum)
        || report.rows == 0
        || report.routing_cell_count == 0
        || report.arms.is_empty()
        || report
            .arms
            .windows(2)
            .any(|pair| pair[0].key >= pair[1].key)
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D1 report authority differs".to_string(),
        ));
    }
    for arm in &report.arms {
        if ![8, 16, 32, 64].contains(&arm.key.code_width_bytes)
            || !valid_checksum(&arm.quantizer_checksum)
            || arm.query_samples.len() != V23_DIAGNOSTIC_QUERIES
            || arm.four_page_projected_bytes == 0
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D1 arm authority differs".to_string(),
            ));
        }
        let mut oracle_hits = 0_u64;
        let mut routed_hits = 0_u64;
        let mut cpu = Vec::with_capacity(V23_DIAGNOSTIC_QUERIES);
        for (expected_index, sample) in arm.query_samples.iter().enumerate() {
            let truth = sample.ground_truth_ids.iter().collect::<BTreeSet<_>>();
            validate_ranked_result(&sample.oracle)?;
            validate_ranked_result(&sample.routed)?;
            let expected_oracle_hits = sample
                .oracle
                .ids
                .iter()
                .filter(|id| truth.contains(id))
                .count();
            let expected_routed_hits = sample
                .routed
                .ids
                .iter()
                .filter(|id| truth.contains(id))
                .count();
            if usize::try_from(sample.query_index).ok() != Some(expected_index)
                || truth.len() != 10
                || sample.ground_truth_ids.iter().any(Vec::is_empty)
                || sample.oracle_candidate_rows != 2_048
                || sample.routed_candidate_rows == 0
                || sample.routed_candidate_rows > report.rows
                || sample.cpu_ns == 0
                || usize::from(sample.oracle_hits) != expected_oracle_hits
                || usize::from(sample.routed_hits) != expected_routed_hits
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 D1 query authority differs".to_string(),
                ));
            }
            oracle_hits = oracle_hits.saturating_add(u64::from(sample.oracle_hits));
            routed_hits = routed_hits.saturating_add(u64::from(sample.routed_hits));
            cpu.push(sample.cpu_ns);
        }
        cpu.sort_unstable();
        let denominator = (V23_DIAGNOSTIC_QUERIES as u64).saturating_mul(10);
        let expected_oracle_recall = oracle_hits.saturating_mul(1_000_000) / denominator;
        let expected_routed_recall = routed_hits.saturating_mul(1_000_000) / denominator;
        let expected_cpu_p99 = cpu[V23_DIAGNOSTIC_QUERIES - 1];
        let expected_passed = expected_oracle_recall >= 990_000
            && expected_routed_recall >= 975_000
            && expected_cpu_p99 <= V23_D1_CPU_MAX_NS
            && arm.four_page_projected_bytes <= V23_WAVE_MAX_BYTES;
        if arm.oracle_recall_ppm != expected_oracle_recall
            || arm.routed_recall_ppm != expected_routed_recall
            || arm.cpu_p99_ns != expected_cpu_p99
            || arm.passed != expected_passed
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D1 derived authority differs".to_string(),
            ));
        }
    }
    Ok(())
}

fn d2_arm_key(arm: &V23D2Arm) -> (V23D1ArmKey, u16, u8) {
    (
        arm.d1_key,
        arm.primary_target_rows,
        arm.maximum_assignments_per_row,
    )
}

pub(crate) fn validate_d2_report(report: &V23D2Report) -> Result<()> {
    if report.schema != "borsuk-v23-d2-v1"
        || !valid_checksum(&report.d1_report_checksum)
        || report.rows == 0
        || report.arms.is_empty()
        || report
            .arms
            .windows(2)
            .any(|pair| d2_arm_key(&pair[0]) >= d2_arm_key(&pair[1]))
    {
        return Err(BorsukError::InvalidStorage(
            "V23 D2 report authority differs".to_string(),
        ));
    }
    for arm in &report.arms {
        if ![8, 16, 32, 64].contains(&arm.d1_key.code_width_bytes)
            || ![512, 1_024, 2_048].contains(&arm.primary_target_rows)
            || !(1..=3).contains(&arm.maximum_assignments_per_row)
            || arm.pages.is_empty()
            || arm.unique_rows != report.rows
            || arm.root_bytes == 0
            || arm.build_peak_rss_bytes == 0
            || arm.query_samples.len() != V23_DIAGNOSTIC_QUERIES
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D2 arm authority differs".to_string(),
            ));
        }
        let mut primary_rows = 0_u64;
        let mut assignments = 0_u64;
        let centroid_dimensions = arm.pages[0].centroid.len();
        for (page_index, page) in arm.pages.iter().enumerate() {
            let expected_path = format!("v23-pages/{}.bin", page.checksum);
            if usize::try_from(page.page_ordinal).ok() != Some(page_index)
                || !valid_checksum(&page.checksum)
                || page.path != expected_path
                || page.encoded_bytes == 0
                || page.encoded_bytes > V23_PAGE_MAX_ENCODED_BYTES
                || page.primary_rows == 0
                || centroid_dimensions == 0
                || page.centroid.len() != centroid_dimensions
                || page.centroid.iter().any(|value| !value.is_finite())
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 D2 page authority differs".to_string(),
                ));
            }
            primary_rows = primary_rows.saturating_add(u64::from(page.primary_rows));
            assignments = assignments
                .saturating_add(u64::from(page.primary_rows))
                .saturating_add(u64::from(page.replicated_rows));
        }
        let expected_amplification = assignments.saturating_mul(1_000_000) / arm.unique_rows;
        if primary_rows != arm.unique_rows
            || assignments != arm.total_assignments
            || expected_amplification != arm.storage_amplification_ppm
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D2 assignment authority differs".to_string(),
            ));
        }

        let mut total_hits = 0_u64;
        let mut minimum_recall = 1_000_000_u64;
        let mut cpu = Vec::with_capacity(V23_DIAGNOSTIC_QUERIES);
        for (expected_index, sample) in arm.query_samples.iter().enumerate() {
            validate_ranked_result(&sample.ranked)?;
            let truth = sample.ground_truth_ids.iter().collect::<BTreeSet<_>>();
            let expected_hits = sample
                .ranked
                .ids
                .iter()
                .filter(|id| truth.contains(id))
                .count();
            let page_refs = sample
                .page_ordinals
                .iter()
                .map(|ordinal| {
                    usize::try_from(*ordinal)
                        .ok()
                        .and_then(|index| arm.pages.get(index))
                })
                .collect::<Option<Vec<_>>>();
            let expected_bytes = page_refs.as_ref().and_then(|pages| {
                pages
                    .iter()
                    .try_fold(0_u64, |sum, page| sum.checked_add(page.encoded_bytes))
            });
            let expected_rows = page_refs.as_ref().and_then(|pages| {
                pages.iter().try_fold(0_u64, |sum, page| {
                    sum.checked_add(u64::from(page.primary_rows) + u64::from(page.replicated_rows))
                })
            });
            let expected_recall = (expected_hits as u64).saturating_mul(100_000);
            if usize::try_from(sample.query_index).ok() != Some(expected_index)
                || sample.page_ordinals.is_empty()
                || sample.page_ordinals.len() > V23_WAVE_MAX_PAGES
                || sample
                    .page_ordinals
                    .windows(2)
                    .any(|pair| pair[0] >= pair[1])
                || expected_bytes != Some(sample.encoded_bytes)
                || sample.encoded_bytes > V23_WAVE_MAX_BYTES
                || expected_rows != Some(sample.candidate_rows)
                || truth.len() != 10
                || sample.ground_truth_ids.iter().any(Vec::is_empty)
                || sample.gt_page_hits > 10
                || usize::from(sample.hits) != expected_hits
                || sample.recall_ppm != expected_recall
                || sample.cpu_ns == 0
            {
                return Err(BorsukError::InvalidStorage(
                    "V23 D2 query authority differs".to_string(),
                ));
            }
            total_hits = total_hits.saturating_add(u64::from(sample.hits));
            minimum_recall = minimum_recall.min(sample.recall_ppm);
            cpu.push(sample.cpu_ns);
        }
        cpu.sort_unstable();
        let expected_aggregate = total_hits.saturating_mul(1_000_000)
            / ((V23_DIAGNOSTIC_QUERIES as u64).saturating_mul(10));
        let expected_cpu_p99 = cpu[V23_DIAGNOSTIC_QUERIES - 1];
        let expected_passed = expected_aggregate >= 975_000
            && minimum_recall >= 800_000
            && arm.storage_amplification_ppm <= 2_000_000
            && arm.projected_ram_bytes <= V23_PROCESS_MAX_BYTES
            && expected_cpu_p99 <= V23_D1_CPU_MAX_NS;
        if arm.aggregate_recall_ppm != expected_aggregate
            || arm.minimum_query_recall_ppm != minimum_recall
            || arm.cpu_p99_ns != expected_cpu_p99
            || arm.passed != expected_passed
        {
            return Err(BorsukError::InvalidStorage(
                "V23 D2 derived authority differs".to_string(),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        V23_WAVE_MAX_BYTES, V23D1Arm, V23D1ArmKey, V23D1QuerySample, V23D1Report, V23D2Arm,
        V23D2QuerySample, V23D2Report, V23PageRef, V23QuantizerFamily, V23RankedResult,
        V23WaveSample, validate_d1_report, validate_d2_report, validate_wave_sample,
    };

    fn canonical_wave() -> V23WaveSample {
        V23WaveSample {
            query_index: 7,
            page_ordinals: vec![3, 9, 12, 18],
            encoded_bytes: 983_040,
            candidate_rows: 8_192,
            backing_gets: 4,
            backing_bytes: 983_040,
            cpu_ns: 2_000_000,
            elapsed_ns: 40_000_000,
        }
    }

    fn ranked_top_ten() -> V23RankedResult {
        V23RankedResult {
            ids: (0_u8..10).map(|value| vec![b'i', value]).collect(),
            distances: (0_u8..10).map(f32::from).collect(),
        }
    }

    fn canonical_d1_report() -> V23D1Report {
        let query_samples = (0_u32..32)
            .map(|query_index| V23D1QuerySample {
                query_index,
                ground_truth_ids: ranked_top_ten().ids,
                oracle: ranked_top_ten(),
                routed: ranked_top_ten(),
                oracle_candidate_rows: 2_048,
                routed_candidate_rows: 8_192,
                oracle_hits: 10,
                routed_hits: 10,
                cpu_ns: 1_000_000,
            })
            .collect();
        V23D1Report {
            schema: "borsuk-v23-d1-v1".to_string(),
            v20_root_checksum: "a".repeat(64),
            v20_codebook_checksum: "b".repeat(64),
            sample_ordinals_checksum: "c".repeat(64),
            rows: 9_990_000,
            routing_cell_count: 4_096,
            arms: vec![V23D1Arm {
                key: V23D1ArmKey {
                    family: V23QuantizerFamily::SrhtPq,
                    code_width_bytes: 64,
                },
                quantizer_checksum: "d".repeat(64),
                query_samples,
                oracle_recall_ppm: 1_000_000,
                routed_recall_ppm: 1_000_000,
                cpu_p99_ns: 1_000_000,
                four_page_projected_bytes: 900_000,
                passed: true,
            }],
        }
    }

    fn canonical_d2_report() -> V23D2Report {
        let query_samples = (0_u32..32)
            .map(|query_index| V23D2QuerySample {
                query_index,
                page_ordinals: vec![0],
                encoded_bytes: 120_000,
                candidate_rows: 1_000,
                ground_truth_ids: ranked_top_ten().ids,
                ranked: ranked_top_ten(),
                gt_page_hits: 10,
                hits: 10,
                recall_ppm: 1_000_000,
                cpu_ns: 1_000_000,
            })
            .collect();
        V23D2Report {
            schema: "borsuk-v23-d2-v1".to_string(),
            d1_report_checksum: "e".repeat(64),
            rows: 1_000,
            arms: vec![V23D2Arm {
                d1_key: V23D1ArmKey {
                    family: V23QuantizerFamily::SrhtPq,
                    code_width_bytes: 64,
                },
                primary_target_rows: 1_024,
                maximum_assignments_per_row: 1,
                pages: vec![V23PageRef {
                    page_ordinal: 0,
                    path: format!("v23-pages/{}.bin", "f".repeat(64)),
                    checksum: "f".repeat(64),
                    encoded_bytes: 120_000,
                    primary_rows: 1_000,
                    replicated_rows: 0,
                    centroid: vec![0.0, 0.0, 0.0, 0.0],
                }],
                unique_rows: 1_000,
                total_assignments: 1_000,
                storage_amplification_ppm: 1_000_000,
                root_bytes: 4_096,
                projected_ram_bytes: 256 * 1024 * 1024,
                build_peak_rss_bytes: 512 * 1024 * 1024,
                query_samples,
                aggregate_recall_ppm: 1_000_000,
                minimum_query_recall_ppm: 1_000_000,
                cpu_p99_ns: 1_000_000,
                passed: true,
            }],
        }
    }

    #[test]
    fn v23_contract_rejects_a_fifth_page() {
        let sample = canonical_wave();
        validate_wave_sample(&sample).unwrap();

        let mut overflow = sample;
        overflow.page_ordinals.push(21);
        overflow.backing_gets = 5;
        assert!(validate_wave_sample(&overflow).is_err());
    }

    #[test]
    fn v23_contract_rejects_inconsistent_one_wave_accounting() {
        let canonical = canonical_wave();

        let mut unordered = canonical.clone();
        unordered.page_ordinals.swap(1, 2);
        assert!(validate_wave_sample(&unordered).is_err());

        let mut duplicate = canonical.clone();
        duplicate.page_ordinals[2] = duplicate.page_ordinals[1];
        assert!(validate_wave_sample(&duplicate).is_err());

        let mut no_candidates = canonical.clone();
        no_candidates.candidate_rows = 0;
        assert!(validate_wave_sample(&no_candidates).is_err());

        let mut no_bytes = canonical.clone();
        no_bytes.encoded_bytes = 0;
        no_bytes.backing_bytes = 0;
        assert!(validate_wave_sample(&no_bytes).is_err());

        let mut too_many_bytes = canonical.clone();
        too_many_bytes.encoded_bytes = V23_WAVE_MAX_BYTES + 1;
        too_many_bytes.backing_bytes = V23_WAVE_MAX_BYTES + 1;
        assert!(validate_wave_sample(&too_many_bytes).is_err());

        let mut gets_differ = canonical.clone();
        gets_differ.backing_gets -= 1;
        assert!(validate_wave_sample(&gets_differ).is_err());

        let mut backing_bytes_differ = canonical.clone();
        backing_bytes_differ.backing_bytes -= 1;
        assert!(validate_wave_sample(&backing_bytes_differ).is_err());

        let mut no_cpu = canonical.clone();
        no_cpu.cpu_ns = 0;
        assert!(validate_wave_sample(&no_cpu).is_err());

        let mut no_elapsed = canonical;
        no_elapsed.elapsed_ns = 0;
        assert!(validate_wave_sample(&no_elapsed).is_err());
    }

    #[test]
    fn v23_d1_contract_recomputes_gates_and_rejects_wide_codes() {
        let canonical = canonical_d1_report();
        validate_d1_report(&canonical).unwrap();

        let mut wide = canonical.clone();
        wide.arms[0].key.code_width_bytes = 65;
        assert!(validate_d1_report(&wide).is_err());

        let mut aggregate_drift = canonical.clone();
        aggregate_drift.arms[0].routed_recall_ppm -= 1;
        assert!(validate_d1_report(&aggregate_drift).is_err());

        let mut non_finite = canonical.clone();
        non_finite.arms[0].query_samples[0].routed.distances[0] = f32::NAN;
        assert!(validate_d1_report(&non_finite).is_err());

        let mut noncanonical_queries = canonical;
        noncanonical_queries.arms[0].query_samples[31].query_index = 30;
        assert!(validate_d1_report(&noncanonical_queries).is_err());
    }

    #[test]
    fn v23_d2_contract_enforces_page_memory_recall_and_amplification_gates() {
        let canonical = canonical_d2_report();
        validate_d2_report(&canonical).unwrap();

        let mut oversized_page = canonical.clone();
        oversized_page.arms[0].pages[0].encoded_bytes = 245_761;
        oversized_page.arms[0].query_samples[0].encoded_bytes = 245_761;
        assert!(validate_d2_report(&oversized_page).is_err());

        let mut amplification_drift = canonical.clone();
        amplification_drift.arms[0].storage_amplification_ppm = 1_000_001;
        assert!(validate_d2_report(&amplification_drift).is_err());

        let mut ram_overflow = canonical.clone();
        ram_overflow.arms[0].projected_ram_bytes = 3 * 1024 * 1024 * 1024 + 1;
        ram_overflow.arms[0].passed = false;
        validate_d2_report(&ram_overflow).unwrap();
        ram_overflow.arms[0].passed = true;
        assert!(validate_d2_report(&ram_overflow).is_err());

        let mut low_tail_recall = canonical;
        low_tail_recall.arms[0].query_samples[0].ranked.ids[7..].fill(vec![b'x']);
        low_tail_recall.arms[0].query_samples[0].ranked.ids[8] = vec![b'y'];
        low_tail_recall.arms[0].query_samples[0].ranked.ids[9] = vec![b'z'];
        low_tail_recall.arms[0].query_samples[0].hits = 7;
        low_tail_recall.arms[0].query_samples[0].recall_ppm = 700_000;
        low_tail_recall.arms[0].aggregate_recall_ppm = 990_625;
        low_tail_recall.arms[0].minimum_query_recall_ppm = 700_000;
        low_tail_recall.arms[0].passed = false;
        validate_d2_report(&low_tail_recall).unwrap();
        low_tail_recall.arms[0].passed = true;
        assert!(validate_d2_report(&low_tail_recall).is_err());
    }
}
