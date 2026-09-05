use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet},
    time::{Duration, Instant},
};

use bytes::Bytes;
use half::f16;
use rayon::prelude::*;
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result, V27Hierarchy, V27HierarchyArtifacts, V27PageIdentity, V27PageRow,
    decode_v27_hierarchy,
    v27_s3_page::visit_v27_page_rows,
    v30_s3_layout::{
        V30Layout, V30LayoutArtifacts, V30LayoutRecord, V30PageRange, V32PageLocation,
        V32RoutingRange, decode_v30_layout_artifacts, partition_v30_leaf_pages,
    },
    v30_s3_pq::{
        V30CodePlanes, V30PqArtifacts, V30PqCodebook, V30PqReconstructor, V30PqWidth,
        V30QueryTable, decode_v30_pq_artifacts,
    },
};

const MAX_CANDIDATES: usize = 12_288;
const MAX_SELECTED_PAGES: usize = 16;
const MAX_PAGE_BYTES: u64 = 3_145_728;
const CANDIDATE_PRUNE_WINDOW: usize = 32_768;
const V32_CPU_GATE_NS: u64 = 64_000_000;
const V32_COMPUTE_GATE_NS: u64 = 12_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum V32CpuPreflightMode {
    Probe,
    Screen,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32CpuPreflightShape {
    pub source_rows: u64,
    pub roots: usize,
    pub trained_parents: usize,
    pub routing_microleaves: usize,
    pub page_identities: usize,
    pub root_beam: usize,
    pub leaf_beam: usize,
    pub scan_codes: u64,
    pub materialized_code_rows: u64,
    pub high_width_codes: usize,
    pub candidate_depth: usize,
    pub selected_pages: usize,
    pub page_bodies: usize,
    pub page_rows: usize,
    pub candidate_storage: usize,
    pub maximum_materialized_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32CpuPreflightSample {
    pub routing_ns: u64,
    pub page_load_ns: u64,
    pub exact_rerank_ns: u64,
    pub query_elapsed_ns: u64,
    pub process_cpu_ns: u64,
    pub work: V32CpuPreflightWork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32CpuPreflightWork {
    pub roots_scored: usize,
    pub leaves_eligible: usize,
    pub leaves_scanned: usize,
    pub query_table_pairs_built: usize,
    pub peak_query_table_pairs_live: usize,
    pub codes_scanned: u64,
    pub candidates_retained: usize,
    pub pages_considered: usize,
    pub selected_pages: usize,
    pub get_count: usize,
    pub encoded_bytes: u64,
    pub decoded_rows: usize,
    pub unique_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32CpuPreflightSamples {
    pub mode: V32CpuPreflightMode,
    pub warmups: usize,
    pub query_count: usize,
    pub query_seed: u64,
    pub query_sha256: String,
    pub observations: Vec<V32CpuPreflightSample>,
}

#[doc(hidden)]
pub fn v32_cpu_preflight_shape(leaf_beam: usize) -> Result<V32CpuPreflightShape> {
    let scan_codes = match leaf_beam {
        64 => 65_536_u64,
        128 => 131_072,
        256 => 262_144,
        _ => return Err(invalid("V32 CPU preflight arm differs")),
    };
    let high_width_codes = usize::try_from(scan_codes.div_ceil(20))
        .map_err(|_| invalid("V32 CPU preflight shape overflows"))?;
    let base_width_codes = usize::try_from(scan_codes)
        .map_err(|_| invalid("V32 CPU preflight shape overflows"))?
        .checked_sub(high_width_codes)
        .ok_or_else(|| invalid("V32 CPU preflight shape overflows"))?;
    let terms = [
        1_024_u64 * 96 * 2,
        65_536_u64 * 96 * 2,
        163_192_u64 * 224,
        208_334_u64 * 112,
        scan_codes.div_ceil(8),
        u64::try_from(base_width_codes)
            .map_err(|_| invalid("V32 CPU preflight shape overflows"))?
            * 24,
        u64::try_from(high_width_codes)
            .map_err(|_| invalid("V32 CPU preflight shape overflows"))?
            * 48,
        45_056_u64
            * u64::try_from(std::mem::size_of::<Candidate>())
                .map_err(|_| invalid("V32 CPU preflight shape overflows"))?,
        16 * 196_608,
    ];
    let maximum_materialized_bytes = terms.into_iter().try_fold(0_u64, |total, term| {
        total
            .checked_add(term)
            .ok_or_else(|| invalid("V32 CPU preflight shape overflows"))
    })?;
    Ok(V32CpuPreflightShape {
        source_rows: 100_000_000,
        roots: 1_024,
        trained_parents: 65_536,
        routing_microleaves: 163_192,
        page_identities: 208_334,
        root_beam: 64,
        leaf_beam,
        scan_codes,
        materialized_code_rows: scan_codes,
        high_width_codes,
        candidate_depth: 12_288,
        selected_pages: 16,
        page_bodies: 16,
        page_rows: 480,
        candidate_storage: 45_056,
        maximum_materialized_bytes,
    })
}

struct V32CpuPreflightStore {
    bodies: BTreeMap<u32, Bytes>,
}

impl V32PageStore for V32CpuPreflightStore {
    fn read_wave(&self, pages: &[V27PageIdentity]) -> Result<Vec<Bytes>> {
        pages
            .iter()
            .map(|page| {
                self.bodies
                    .get(&page.ordinal)
                    .cloned()
                    .ok_or_else(|| invalid("V32 CPU preflight page differs"))
            })
            .collect()
    }
}

fn v32_cpu_preflight_index(shape: &V32CpuPreflightShape) -> Result<V32Index<V32CpuPreflightStore>> {
    if *shape != v32_cpu_preflight_shape(shape.leaf_beam)? {
        return Err(invalid("V32 CPU preflight shape differs"));
    }
    let unit = f16::from_f32(1.0 / 96.0_f32.sqrt());
    let leaf_roots = (0..shape.trained_parents)
        .map(|parent| {
            u16::try_from(parent % shape.roots)
                .map_err(|_| invalid("V32 CPU preflight root ordinal overflows"))
        })
        .collect::<Result<Vec<_>>>()?;
    let hierarchy = V27Hierarchy {
        roots: vec![[unit; 96]; shape.roots],
        leaves: vec![[unit; 96]; shape.trained_parents],
        leaf_roots,
    };

    let mut bodies = BTreeMap::new();
    let mut pages = Vec::with_capacity(shape.page_identities);
    let mut logical_start = 0_u64;
    for ordinal in 0..shape.page_identities {
        let row_count = (shape.source_rows - logical_start).min(shape.page_rows as u64) as u16;
        let identity = if ordinal < shape.selected_pages {
            let rows = (logical_start..logical_start + u64::from(row_count))
                .map(|source_ordinal| V27PageRow {
                    source_ordinal,
                    vector: [0.25 + source_ordinal as f32 / 100_000.0; 96],
                })
                .collect::<Vec<_>>();
            let (identity, bytes) = crate::encode_v27_page(
                u32::try_from(ordinal)
                    .map_err(|_| invalid("V32 CPU preflight page ordinal overflows"))?,
                row_count,
                0,
                &rows,
            )?;
            bodies.insert(identity.ordinal, Bytes::from(bytes));
            identity
        } else {
            V27PageIdentity {
                ordinal: u32::try_from(ordinal)
                    .map_err(|_| invalid("V32 CPU preflight page ordinal overflows"))?,
                sha256: format!("{:064x}", ordinal + 1),
                encoded_bytes: 1,
                primary_rows: row_count,
                replica_rows: 0,
            }
        };
        pages.push(V30PageRange::from_legacy(
            logical_start,
            row_count,
            &identity,
        )?);
        logical_start += u64::from(row_count);
    }
    if logical_start != shape.source_rows {
        return Err(invalid("V32 CPU preflight page coverage differs"));
    }

    let fixed_rows = 256_u64 * 1_024;
    let tail_leaves = shape
        .routing_microleaves
        .checked_sub(256)
        .ok_or_else(|| invalid("V32 CPU preflight leaf shape differs"))?;
    let tail_rows = shape
        .source_rows
        .checked_sub(fixed_rows)
        .ok_or_else(|| invalid("V32 CPU preflight leaf shape differs"))?;
    let tail_base = tail_rows / tail_leaves as u64;
    let tail_remainder = tail_rows % tail_leaves as u64;
    let eligible_leaves = shape
        .routing_microleaves
        .checked_mul(shape.root_beam)
        .ok_or_else(|| invalid("V32 CPU preflight leaf shape overflows"))?
        .div_ceil(shape.roots);
    let eligible_parents = shape
        .trained_parents
        .checked_mul(shape.root_beam)
        .ok_or_else(|| invalid("V32 CPU preflight parent shape overflows"))?
        / shape.roots;
    let ineligible_parents = shape
        .trained_parents
        .checked_sub(eligible_parents)
        .ok_or_else(|| invalid("V32 CPU preflight parent shape differs"))?;
    let ineligible_roots = shape
        .roots
        .checked_sub(shape.root_beam)
        .ok_or_else(|| invalid("V32 CPU preflight root shape differs"))?;
    let mut leaves = Vec::with_capacity(shape.routing_microleaves);
    let mut logical_start = 0_u64;
    for ordinal in 0..shape.routing_microleaves {
        let row_count = if ordinal < 256 {
            1_024
        } else {
            tail_base + u64::from((ordinal - 256) < tail_remainder as usize)
        };
        let logical_end = logical_start
            .checked_add(row_count)
            .ok_or_else(|| invalid("V32 CPU preflight leaf coverage overflows"))?;
        let page_start = logical_start / shape.page_rows as u64;
        let page_end = logical_end.div_ceil(shape.page_rows as u64);
        let code_parent = if ordinal < eligible_leaves {
            let slot = ordinal.saturating_mul(eligible_parents) / eligible_leaves;
            (slot / shape.root_beam) * shape.roots + slot % shape.root_beam
        } else {
            let slot = (ordinal - eligible_leaves) % ineligible_parents;
            (slot / ineligible_roots) * shape.roots + shape.root_beam + slot % ineligible_roots
        };
        leaves.push(V32RoutingRange {
            leaf_ordinal: u32::try_from(ordinal)
                .map_err(|_| invalid("V32 CPU preflight leaf ordinal overflows"))?,
            code_parent_leaf_ordinal: u32::try_from(code_parent)
                .map_err(|_| invalid("V32 CPU preflight parent ordinal overflows"))?,
            routing_centroid: [unit; 96],
            logical_start,
            row_count,
            page_start: u32::try_from(page_start)
                .map_err(|_| invalid("V32 CPU preflight page ordinal overflows"))?,
            page_count: u32::try_from(page_end - page_start)
                .map_err(|_| invalid("V32 CPU preflight page count overflows"))?,
        });
        logical_start = logical_end;
    }
    let layout = V30Layout::new(shape.source_rows, leaves, pages)?;

    let materialized_rows = usize::try_from(shape.materialized_code_rows)
        .map_err(|_| invalid("V32 CPU preflight code rows overflow"))?;
    let mut high_bits = vec![0_u32; materialized_rows.div_ceil(128) * 4];
    for logical in 0..shape.high_width_codes {
        high_bits[logical / 32] |= 1 << (logical % 32);
    }
    let codes = V30CodePlanes::from_packed_window(
        usize::try_from(shape.source_rows)
            .map_err(|_| invalid("V32 CPU preflight source rows overflow"))?,
        materialized_rows,
        high_bits,
        vec![0; (materialized_rows - shape.high_width_codes) * V30PqWidth::Base24.bytes()],
        vec![0; shape.high_width_codes * V30PqWidth::High48.bytes()],
    )?;
    let base = V30PqCodebook::new(
        V30PqWidth::Base24,
        vec![0.0; V30PqWidth::Base24.subquantizers() * 256 * V30PqWidth::Base24.dimensions()],
    )?;
    let high = V30PqCodebook::new(
        V30PqWidth::High48,
        vec![0.0; V30PqWidth::High48.subquantizers() * 256 * V30PqWidth::High48.dimensions()],
    )?;
    let router = V32Router::new(hierarchy, base, high, layout, codes)?;
    V32Index::new(
        router,
        V32CpuPreflightStore { bodies },
        V32SearchArm {
            root_beam: shape.root_beam,
            leaf_beam: shape.leaf_beam,
            scan_budget: shape.scan_codes,
            candidate_depth: shape.candidate_depth,
            page_count: shape.selected_pages,
        },
    )
}

fn v32_cpu_preflight_query(seed: u64, ordinal: usize) -> [f32; 96] {
    let mut state = seed.wrapping_add(ordinal as u64);
    std::array::from_fn(|_| {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        ((value >> 40) as f32 / (1_u32 << 24) as f32) * 2.0 - 1.0
    })
}

#[doc(hidden)]
pub fn run_v32_cpu_preflight(mode: V32CpuPreflightMode, leaf_beam: usize) -> Result<Vec<u8>> {
    let shape = v32_cpu_preflight_shape(leaf_beam)?;
    let index = v32_cpu_preflight_index(&shape)?;
    let (warmups, query_count) = match mode {
        V32CpuPreflightMode::Probe => (0, 128),
        V32CpuPreflightMode::Screen => (1_024, 10_000),
    };
    let query_seed = 0x243f_6a88_85a3_08d3;
    for ordinal in 0..warmups {
        index.search(&v32_cpu_preflight_query(query_seed, ordinal), 10)?;
    }
    let mut query_digest = Sha256::new();
    let mut observations = Vec::with_capacity(query_count);
    for ordinal in 0..query_count {
        let query = v32_cpu_preflight_query(query_seed, warmups + ordinal);
        for value in query {
            query_digest.update(value.to_le_bytes());
        }
        let (_, sample) = index.cpu_preflight_observation(&query, 10)?;
        observations.push(sample);
    }
    canonical_v32_cpu_preflight_receipt(
        &shape,
        &V32CpuPreflightSamples {
            mode,
            warmups,
            query_count,
            query_seed,
            query_sha256: format!("{:x}", query_digest.finalize()),
            observations,
        },
    )
}

fn v32_cpu_p99(values: impl Iterator<Item = u64>, length: usize) -> Result<u64> {
    let mut values = values.collect::<Vec<_>>();
    if values.len() != length || values.is_empty() {
        return Err(invalid("V32 CPU preflight sample count differs"));
    }
    values.sort_unstable();
    Ok(values[length.saturating_mul(99).div_ceil(100) - 1])
}

fn v32_cpu_preflight_expected_work(shape: &V32CpuPreflightShape) -> Result<V32CpuPreflightWork> {
    let leaves_eligible = shape
        .routing_microleaves
        .checked_mul(shape.root_beam)
        .ok_or_else(|| invalid("V32 CPU preflight leaf shape overflows"))?
        .div_ceil(shape.roots);
    let eligible_parents = shape
        .trained_parents
        .checked_mul(shape.root_beam)
        .ok_or_else(|| invalid("V32 CPU preflight parent shape overflows"))?
        / shape.roots;
    let query_table_pairs_built = (shape.leaf_beam - 1)
        .checked_mul(eligible_parents)
        .ok_or_else(|| invalid("V32 CPU preflight table shape overflows"))?
        / leaves_eligible
        + 1;
    Ok(V32CpuPreflightWork {
        roots_scored: shape.roots,
        leaves_eligible,
        leaves_scanned: shape.leaf_beam,
        query_table_pairs_built,
        peak_query_table_pairs_live: 1,
        codes_scanned: shape.scan_codes,
        candidates_retained: shape.candidate_depth,
        pages_considered: shape.selected_pages,
        selected_pages: shape.selected_pages,
        get_count: shape.page_bodies,
        encoded_bytes: 3_117_216,
        decoded_rows: shape.selected_pages * shape.page_rows,
        unique_rows: shape.selected_pages * shape.page_rows,
    })
}

#[doc(hidden)]
pub fn canonical_v32_cpu_preflight_receipt(
    shape: &V32CpuPreflightShape,
    samples: &V32CpuPreflightSamples,
) -> Result<Vec<u8>> {
    if *shape != v32_cpu_preflight_shape(shape.leaf_beam)? {
        return Err(invalid("V32 CPU preflight shape differs"));
    }
    let expected_samples = match samples.mode {
        V32CpuPreflightMode::Probe => 128,
        V32CpuPreflightMode::Screen => 10_000,
    };
    let expected_warmups = match samples.mode {
        V32CpuPreflightMode::Probe => 0,
        V32CpuPreflightMode::Screen => 1_024,
    };
    if samples.warmups != expected_warmups
        || samples.observations.len() != expected_samples
        || samples.query_count != expected_samples
        || samples.query_seed != 0x243f_6a88_85a3_08d3
        || samples.query_sha256.len() != 64
        || !samples
            .query_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid("V32 CPU preflight sample count differs"));
    }
    for sample in &samples.observations {
        let stage_total = [
            sample.routing_ns,
            sample.page_load_ns,
            sample.exact_rerank_ns,
        ]
        .into_iter()
        .try_fold(0_u64, |total, value| {
            if value == 0 {
                return Err(invalid("V32 CPU preflight sample differs"));
            }
            total
                .checked_add(value)
                .ok_or_else(|| invalid("V32 CPU preflight sample overflows"))
        })?;
        if stage_total > sample.query_elapsed_ns
            || sample.process_cpu_ns == 0
            || sample.work != v32_cpu_preflight_expected_work(shape)?
        {
            return Err(invalid("V32 CPU preflight sample differs"));
        }
    }
    let process_cpu_p99_ns = v32_cpu_p99(
        samples
            .observations
            .iter()
            .map(|sample| sample.process_cpu_ns),
        expected_samples,
    )?;
    let query_elapsed_p99_ns = v32_cpu_p99(
        samples
            .observations
            .iter()
            .map(|sample| sample.query_elapsed_ns),
        expected_samples,
    )?;
    let mut failed_gates = Vec::new();
    match samples.mode {
        V32CpuPreflightMode::Probe => {
            if samples
                .observations
                .iter()
                .all(|sample| sample.process_cpu_ns > V32_CPU_GATE_NS)
            {
                failed_gates.push("total-cpu");
            }
        }
        V32CpuPreflightMode::Screen => {
            if query_elapsed_p99_ns > V32_COMPUTE_GATE_NS {
                failed_gates.push("compute");
            }
            if process_cpu_p99_ns > V32_CPU_GATE_NS {
                failed_gates.push("total-cpu");
            }
        }
    }
    let raw_samples = samples
        .observations
        .iter()
        .map(|sample| {
            let stage_total = sample.routing_ns + sample.page_load_ns + sample.exact_rerank_ns;
            serde_json::json!({
                "exact_rerank_ns": sample.exact_rerank_ns,
                "page_load_ns": sample.page_load_ns,
                "process_cpu_ns": sample.process_cpu_ns,
                "query_elapsed_ns": sample.query_elapsed_ns,
                "routing_ns": sample.routing_ns,
                "unattributed_ns": sample.query_elapsed_ns - stage_total,
                "work": {
                    "candidates_retained": sample.work.candidates_retained,
                    "codes_scanned": sample.work.codes_scanned,
                    "decoded_rows": sample.work.decoded_rows,
                    "encoded_bytes": sample.work.encoded_bytes,
                    "get_count": sample.work.get_count,
                    "leaves_eligible": sample.work.leaves_eligible,
                    "leaves_scanned": sample.work.leaves_scanned,
                    "pages_considered": sample.work.pages_considered,
                    "peak_query_table_pairs_live": sample.work.peak_query_table_pairs_live,
                    "query_table_pairs_built": sample.work.query_table_pairs_built,
                    "roots_scored": sample.work.roots_scored,
                    "selected_pages": sample.work.selected_pages,
                    "unique_rows": sample.work.unique_rows,
                },
            })
        })
        .collect::<Vec<_>>();
    let mode = match samples.mode {
        V32CpuPreflightMode::Probe => "probe",
        V32CpuPreflightMode::Screen => "screen",
    };
    let status = match (samples.mode, failed_gates.is_empty()) {
        (V32CpuPreflightMode::Probe, true) => "probe-continue",
        (V32CpuPreflightMode::Probe, false) => "probe-failed",
        (V32CpuPreflightMode::Screen, true) => "screen-continue",
        (V32CpuPreflightMode::Screen, false) => "screen-failed",
    };
    let gates_enforced = match samples.mode {
        V32CpuPreflightMode::Probe => vec!["total-cpu"],
        V32CpuPreflightMode::Screen => vec!["compute", "total-cpu"],
    };
    let value = serde_json::json!({
        "candidate_storage": shape.candidate_storage,
        "claim_eligible": false,
        "eligible_routing_microleaves": v32_cpu_preflight_expected_work(shape)?.leaves_eligible,
        "failed_gates": failed_gates,
        "gates_enforced": gates_enforced,
        "leaf_beam": shape.leaf_beam,
        "mode": mode,
        "page_identities": shape.page_identities,
        "process_cpu_p99_ns": process_cpu_p99_ns,
        "projected_materialized_bytes": shape.maximum_materialized_bytes,
        "query_count": samples.query_count,
        "query_elapsed_p99_ns": query_elapsed_p99_ns,
        "query_seed": samples.query_seed,
        "query_sha256": samples.query_sha256,
        "raw_samples": raw_samples,
        "root_beam": shape.root_beam,
        "roots": shape.roots,
        "routing_microleaves": shape.routing_microleaves,
        "sample_count": expected_samples,
        "scan_codes": shape.scan_codes,
        "schema": "borsuk-v32-cpu-preflight-v1",
        "selected_pages": shape.selected_pages,
        "status": status,
        "trained_parents": shape.trained_parents,
        "warmups": samples.warmups,
    });
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|_| invalid("V32 CPU preflight receipt serialization failed"))?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32SearchArm {
    pub root_beam: usize,
    pub leaf_beam: usize,
    pub scan_budget: u64,
    pub candidate_depth: usize,
    pub page_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32RoutingWork {
    pub roots_scored: usize,
    pub leaves_eligible: usize,
    pub leaves_scanned: usize,
    pub query_table_pairs_built: usize,
    pub peak_query_table_pairs_live: usize,
    pub codes_scanned: u64,
    pub candidates_retained: usize,
    pub pages_considered: usize,
    pub selected_pages: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32PageSelection {
    pub pages: Vec<V27PageIdentity>,
    pub work: V32RoutingWork,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum V32RoutingTargetStage {
    LeafFrontier,
    CandidateRetention,
    PageReducer,
    SelectedPage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum V32RoutingStopReason {
    RootGated,
    AllLeaves,
    LeafLimit,
    ScanBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum V32SearchPhase {
    RoutingComplete,
    PageReadComplete,
    ExactRerankComplete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32RoutingTargetReport {
    pub logical: u64,
    pub leaf_ordinal: u32,
    pub owner_root_ordinal: u16,
    pub owner_root_rank: usize,
    pub global_routing_leaf_rank: usize,
    pub page_ordinal: u32,
    pub routing_leaf_rank: Option<usize>,
    pub candidate_rank: Option<usize>,
    pub first_unique_page_rank: Option<usize>,
    pub page_in_scanned_pool: bool,
    pub page_in_retained_pool: bool,
    pub page_selected: bool,
    pub stage: V32RoutingTargetStage,
    pub reciprocal_rank_selected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32RoutingDiagnostic {
    pub selection: V32PageSelection,
    pub reciprocal_rank_pages: Vec<V27PageIdentity>,
    pub targets: Vec<V32RoutingTargetReport>,
    pub total_routing_leaves: usize,
    pub scan_budget: u64,
    pub global_leaf_limit: Option<usize>,
    pub stop_reason: V32RoutingStopReason,
    pub next_leaf_rows: Option<u64>,
}

#[derive(Debug, Clone)]
#[doc(hidden)]
pub struct V32Router {
    hierarchy: V27Hierarchy,
    base_codebook: V30PqCodebook,
    high_codebook: V30PqCodebook,
    layout: V30Layout,
    codes: V30CodePlanes,
}

/// An immutable candidate population captured without a truth or virtual layout.
#[derive(Clone)]
#[doc(hidden)]
pub struct V32CandidateReplay<'a> {
    router: &'a V32Router,
    query: [f32; 96],
    arm: V32SearchArm,
    details: RoutingDetails,
}

impl V32CandidateReplay<'_> {
    /// Bind ordered candidates, routing work and stop metadata, excluding truth.
    pub fn sha256(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"borsuk-v32-candidate-replay-v2\0");
        for value in self.query {
            digest.update(value.to_bits().to_le_bytes());
        }
        for value in [
            self.arm.root_beam as u64,
            self.arm.leaf_beam as u64,
            self.arm.scan_budget,
            self.arm.candidate_depth as u64,
            self.arm.page_count as u64,
        ] {
            digest.update(value.to_le_bytes());
        }
        for list in [&self.details.selected_leaves, &self.details.ranked_leaves] {
            digest.update((list.len() as u64).to_le_bytes());
            for leaf in list {
                digest.update(leaf.to_le_bytes());
            }
        }
        digest.update((self.details.ranked_candidates.len() as u64).to_le_bytes());
        for candidate in &self.details.ranked_candidates {
            digest.update(candidate.logical.to_le_bytes());
            digest.update(candidate.score.to_bits().to_le_bytes());
        }
        let work = self.details.selection.work;
        for value in [
            work.roots_scored as u64,
            work.leaves_eligible as u64,
            work.leaves_scanned as u64,
            work.query_table_pairs_built as u64,
            work.peak_query_table_pairs_live as u64,
            work.codes_scanned,
            work.candidates_retained as u64,
            work.pages_considered as u64,
            work.selected_pages as u64,
            self.details.total_routing_leaves as u64,
            self.details.scan_budget,
        ] {
            digest.update(value.to_le_bytes());
        }
        for value in [
            self.details.global_leaf_limit.map(|n| n as u64),
            self.details.next_leaf_rows,
        ] {
            digest.update([u8::from(value.is_some())]);
            digest.update(value.unwrap_or_default().to_le_bytes());
        }
        digest.update([match self.details.stop_reason {
            V32RoutingStopReason::RootGated => 0,
            V32RoutingStopReason::AllLeaves => 1,
            V32RoutingStopReason::LeafLimit => 2,
            V32RoutingStopReason::ScanBudget => 3,
        }]);
        format!("{:x}", digest.finalize())
    }

    /// Join truth onto the captured current-layout selection without rerouting.
    pub fn diagnose(&self, logicals: &[u64]) -> Result<V32RoutingDiagnostic> {
        self.router
            .diagnose_logicals_from_details(&self.query, logicals, self.details.clone())
    }

    /// Apply virtual first-distinct selection to the already captured population.
    pub fn reduce_virtual(
        &self,
        logicals: &[u64],
        layout: &V32VirtualPageLayout,
    ) -> Result<V32VirtualRoutingDiagnostic> {
        self.router.diagnose_virtual_replay(self, logicals, layout)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32VirtualPageLayout {
    page_owners: Vec<u32>,
    page_row_counts: Vec<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32VirtualRoutingDiagnostic {
    pub current: V32RoutingDiagnostic,
    pub candidate_replay_sha256: String,
    pub virtual_pages: Vec<u32>,
    pub virtual_pages_at_eight: Vec<u32>,
    pub virtual_target_pages: Vec<u32>,
    pub virtual_target_selected: Vec<bool>,
    pub virtual_target_selected_at_eight: Vec<bool>,
    pub virtual_layout_sha256: String,
    pub routing_work: V32RoutingWork,
    pub truth_microleaf_count: usize,
    pub truth_virtual_page_count: usize,
    pub recovered_logicals: Vec<u64>,
    pub newly_lost_logicals: Vec<u64>,
}

impl V32VirtualPageLayout {
    fn sha256(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(b"borsuk-v32-virtual-page-layout-v1\0");
        for owner in &self.page_owners {
            digest.update(owner.to_le_bytes());
        }
        digest.update(b"\0row-counts\0");
        for count in &self.page_row_counts {
            digest.update(count.to_le_bytes());
        }
        format!("{:x}", digest.finalize())
    }

    pub fn page_for_logical(&self, logical: u64) -> Result<u32> {
        usize::try_from(logical)
            .ok()
            .and_then(|logical| self.page_owners.get(logical).copied())
            .ok_or_else(|| invalid("V32 virtual page logical ordinal differs"))
    }

    pub fn page_count(&self) -> usize {
        self.page_row_counts.len()
    }

    pub fn page_row_counts(&self) -> &[u16] {
        &self.page_row_counts
    }

    pub fn truth_page_count(&self, logicals: &[u64]) -> Result<usize> {
        let unique = logicals
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if unique.len() != logicals.len() {
            return Err(invalid("V32 virtual truth logicals differ"));
        }
        logicals
            .iter()
            .map(|logical| self.page_for_logical(*logical))
            .collect::<Result<std::collections::BTreeSet<_>>>()
            .map(|pages| pages.len())
    }
}

#[doc(hidden)]
pub trait V32PageStore: Send + Sync {
    fn read_wave(&self, pages: &[V27PageIdentity]) -> Result<Vec<Bytes>>;
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub struct V32Match {
    pub source_ordinal: u64,
    pub squared_distance: f64,
}

#[derive(Debug, Clone, Copy)]
struct ExactCandidate {
    source_ordinal: u64,
    squared_distance: f64,
}

impl PartialEq for ExactCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.squared_distance.to_bits() == other.squared_distance.to_bits()
            && self.source_ordinal == other.source_ordinal
    }
}

impl Eq for ExactCandidate {}

impl Ord for ExactCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.squared_distance
            .total_cmp(&other.squared_distance)
            .then_with(|| self.source_ordinal.cmp(&other.source_ordinal))
    }
}

impl PartialOrd for ExactCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct ExactTopK {
    limit: usize,
    candidates: BinaryHeap<ExactCandidate>,
}

impl ExactTopK {
    fn new(limit: usize) -> Result<Self> {
        if limit == 0 || limit > 10 {
            return Err(invalid("V30 result count differs"));
        }
        Ok(Self {
            limit,
            candidates: BinaryHeap::with_capacity(limit + 1),
        })
    }

    fn insert(&mut self, value: V32Match) {
        let candidate = ExactCandidate {
            source_ordinal: value.source_ordinal,
            squared_distance: value.squared_distance,
        };
        if self.candidates.len() < self.limit {
            self.candidates.push(candidate);
        } else if self
            .candidates
            .peek()
            .is_some_and(|worst| candidate < *worst)
        {
            self.candidates.pop();
            self.candidates.push(candidate);
        }
    }

    fn finish(self) -> Vec<V32Match> {
        let mut values = self.candidates.into_vec();
        values.sort_unstable();
        values
            .into_iter()
            .map(|value| V32Match {
                source_ordinal: value.source_ordinal,
                squared_distance: value.squared_distance,
            })
            .collect()
    }
}

struct ExactPageRerank {
    decoded_rows: usize,
    source_ordinals: Vec<u64>,
    matches: Vec<V32Match>,
}

struct ExactRerankResult {
    decoded_rows: usize,
    unique_rows: usize,
    matches: Vec<V32Match>,
}

fn exact_rerank_pages(
    pages: &[V27PageIdentity],
    bodies: &[Bytes],
    query: &[f32; 96],
    k: usize,
) -> Result<ExactRerankResult> {
    if pages.len() != bodies.len() || pages.is_empty() || pages.len() > MAX_SELECTED_PAGES {
        return Err(invalid("V30 page wave cardinality differs"));
    }
    let page_results = crate::parallel::install(|| {
        pages
            .par_iter()
            .zip(bodies.par_iter())
            .map(|(identity, body)| {
                let expected_rows = usize::from(identity.primary_rows)
                    .checked_add(usize::from(identity.replica_rows))
                    .ok_or_else(|| invalid("V30 selected row count overflows"))?;
                let mut source_ordinals = Vec::with_capacity(expected_rows);
                let mut matches = ExactTopK::new(k)?;
                visit_v27_page_rows(identity, body, |source_ordinal, vector| {
                    source_ordinals.push(source_ordinal);
                    let squared_distance = vector
                        .iter()
                        .zip(query)
                        .map(|(left, right)| {
                            let delta = f64::from(*left) - f64::from(*right);
                            delta * delta
                        })
                        .sum::<f64>();
                    if !squared_distance.is_finite() {
                        return Err(invalid("V30 exact distance differs"));
                    }
                    matches.insert(V32Match {
                        source_ordinal,
                        squared_distance,
                    });
                    Ok(())
                })?;
                if source_ordinals.len() != expected_rows {
                    return Err(invalid("V30 decoded row count differs"));
                }
                Ok(ExactPageRerank {
                    decoded_rows: expected_rows,
                    source_ordinals,
                    matches: matches.finish(),
                })
            })
            .collect::<Vec<Result<ExactPageRerank>>>()
    })
    .into_iter()
    .collect::<Result<Vec<_>>>()?;
    let decoded_rows = page_results.iter().try_fold(0_usize, |total, page| {
        total
            .checked_add(page.decoded_rows)
            .ok_or_else(|| invalid("V30 decoded row count overflows"))
    })?;
    let mut seen = HashSet::with_capacity(decoded_rows);
    let mut matches = ExactTopK::new(k)?;
    for page in page_results {
        for source_ordinal in page.source_ordinals {
            if !seen.insert(source_ordinal) {
                return Err(invalid("V30 exact row ownership differs"));
            }
        }
        for value in page.matches {
            matches.insert(value);
        }
    }
    if seen.len() < k {
        return Err(invalid("V30 exact candidate count differs"));
    }
    Ok(ExactRerankResult {
        decoded_rows,
        unique_rows: seen.len(),
        matches: matches.finish(),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[doc(hidden)]
pub struct V32SearchWork {
    pub routing: V32RoutingWork,
    pub get_count: usize,
    pub encoded_bytes: u64,
    pub decoded_rows: usize,
    pub unique_rows: usize,
}

#[derive(Debug, Clone, PartialEq)]
#[doc(hidden)]
pub struct V32SearchResult {
    pub matches: Vec<V32Match>,
    pub work: V32SearchWork,
    pub candidate_replay_sha256: Option<String>,
    pub requested_pages: Vec<V27PageIdentity>,
}

#[doc(hidden)]
pub struct V32Index<S> {
    router: V32Router,
    store: S,
    arm: V32SearchArm,
    global_leaf_limit: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    score: f32,
    logical: u64,
}

#[derive(Clone)]
struct RoutingDetails {
    selection: V32PageSelection,
    selected_leaves: Vec<u32>,
    ranked_leaves: Vec<u32>,
    ranked_candidates: Vec<Candidate>,
    total_routing_leaves: usize,
    scan_budget: u64,
    global_leaf_limit: Option<usize>,
    stop_reason: V32RoutingStopReason,
    next_leaf_rows: Option<u64>,
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.logical == other.logical
    }
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.logical.cmp(&other.logical))
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct BoundedCandidates {
    limit: usize,
    values: Vec<Candidate>,
}

impl BoundedCandidates {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            values: Vec::with_capacity(limit + CANDIDATE_PRUNE_WINDOW),
        }
    }

    fn insert(&mut self, candidate: Candidate) {
        self.values.push(candidate);
        if self.values.len() == self.limit + CANDIDATE_PRUNE_WINDOW {
            self.prune();
        }
    }

    fn prune(&mut self) {
        if self.values.len() > self.limit {
            self.values
                .select_nth_unstable_by(self.limit, Candidate::cmp);
            self.values.truncate(self.limit);
        }
    }

    #[cfg(test)]
    fn storage_len(&self) -> usize {
        self.values.len()
    }

    fn finish(mut self) -> Vec<Candidate> {
        self.prune();
        self.values.sort_unstable();
        self.values
    }
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn duration_ns(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_nanos()).map_err(|_| invalid("V32 CPU clock overflows"))
}

#[cfg(unix)]
fn process_cpu_time_ns() -> Result<u64> {
    let value = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    let seconds =
        u64::try_from(value.tv_sec).map_err(|_| invalid("V32 process CPU clock differs"))?;
    let nanoseconds =
        u64::try_from(value.tv_nsec).map_err(|_| invalid("V32 process CPU clock differs"))?;
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|total| total.checked_add(nanoseconds))
        .ok_or_else(|| invalid("V32 process CPU clock overflows"))
}

#[cfg(not(unix))]
fn process_cpu_time_ns() -> Result<u64> {
    Err(invalid("V32 process CPU clock is unavailable"))
}

fn normalized(query: &[f32; 96]) -> Result<[f32; 96]> {
    if query.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V30 query is non-finite"));
    }
    let norm = query
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(invalid("V30 query norm differs"));
    }
    Ok(query.map(|value| (f64::from(value) / norm) as f32))
}

fn centroid_distance(query: &[f32; 96], centroid: &[f16; 96]) -> f64 {
    query
        .iter()
        .zip(centroid)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(f32::from(*right));
            delta * delta
        })
        .sum()
}

fn smallest(mut values: Vec<(f64, usize)>, limit: usize) -> Vec<(f64, usize)> {
    let compare = |left: &(f64, usize), right: &(f64, usize)| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    };
    if limit == 0 {
        return Vec::new();
    }
    if limit < values.len() {
        values.select_nth_unstable_by(limit, compare);
        values.truncate(limit);
    }
    values.sort_unstable_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    values
}

fn bounded_global_prefix_rows(
    ranked_leaf_rows: &[u64],
    leaf_limit: usize,
    scan_budget: u64,
) -> Result<(usize, u64, V32RoutingStopReason, Option<u64>)> {
    let mut selected = 0_usize;
    let mut codes = 0_u64;
    while selected < leaf_limit.min(ranked_leaf_rows.len()) {
        let next = codes
            .checked_add(ranked_leaf_rows[selected])
            .ok_or_else(|| invalid("V30 scanned-code count overflows"))?;
        if next > scan_budget {
            break;
        }
        codes = next;
        selected += 1;
    }
    if selected == 0 {
        return Err(invalid("V32 global routing prefix is empty"));
    }
    let (reason, next_leaf_rows) = if selected == ranked_leaf_rows.len() {
        (V32RoutingStopReason::AllLeaves, None)
    } else if selected == leaf_limit {
        (V32RoutingStopReason::LeafLimit, None)
    } else {
        (
            V32RoutingStopReason::ScanBudget,
            Some(ranked_leaf_rows[selected]),
        )
    };
    Ok((selected, codes, reason, next_leaf_rows))
}

fn eligible_v32_routing_leaf_scores(
    query: &[f32; 96],
    root_count: usize,
    selected_roots: &[(f64, usize)],
    trained_parent_roots: &[u16],
    routing_leaves: &[V32RoutingRange],
) -> Result<(Vec<(f64, usize)>, usize)> {
    if root_count == 0 || selected_roots.is_empty() {
        return Err(invalid("V32 selected-root authority differs"));
    }
    let mut selected = vec![false; root_count];
    for &(_, root) in selected_roots {
        let slot = selected
            .get_mut(root)
            .ok_or_else(|| invalid("V32 selected-root authority differs"))?;
        if *slot {
            return Err(invalid("V32 selected-root authority differs"));
        }
        *slot = true;
    }
    let mut membership_lookups = 0_usize;
    let mut scores = Vec::new();
    for (ordinal, leaf) in routing_leaves.iter().enumerate() {
        let parent = usize::try_from(leaf.code_parent_leaf_ordinal)
            .map_err(|_| invalid("V32 routing parent overflows"))?;
        let root = usize::from(
            *trained_parent_roots
                .get(parent)
                .ok_or_else(|| invalid("V32 routing parent differs"))?,
        );
        membership_lookups += 1;
        let is_selected = selected
            .get(root)
            .copied()
            .ok_or_else(|| invalid("V32 routing root differs"))?;
        if is_selected {
            scores.push((centroid_distance(query, &leaf.routing_centroid), ordinal));
        }
    }
    Ok((scores, membership_lookups))
}

impl V32Router {
    pub fn from_artifacts(
        hierarchy: &V27HierarchyArtifacts,
        pq: &V30PqArtifacts,
        layout: &V30LayoutArtifacts,
    ) -> Result<Self> {
        let hierarchy = decode_v27_hierarchy(
            &hierarchy.roots,
            &hierarchy.roots_bytes,
            &hierarchy.leaves,
            &hierarchy.leaves_bytes,
        )?;
        let (base_codebook, high_codebook, codes) = decode_v30_pq_artifacts(pq)?.into_parts();
        let layout = decode_v30_layout_artifacts(layout)?;
        Self::new(hierarchy, base_codebook, high_codebook, layout, codes)
    }

    pub(crate) fn new(
        hierarchy: V27Hierarchy,
        base_codebook: V30PqCodebook,
        high_codebook: V30PqCodebook,
        layout: V30Layout,
        codes: V30CodePlanes,
    ) -> Result<Self> {
        if hierarchy.roots.is_empty()
            || hierarchy.leaves.is_empty()
            || hierarchy.leaf_roots.len() != hierarchy.leaves.len()
            || layout.leaves().iter().any(|leaf| {
                usize::try_from(leaf.code_parent_leaf_ordinal)
                    .ok()
                    .is_none_or(|parent| parent >= hierarchy.leaves.len())
            })
            || layout.source_rows() != codes.logical_rows() as u64
            || base_codebook.width() != V30PqWidth::Base24
            || high_codebook.width() != V30PqWidth::High48
        {
            return Err(invalid("V30 router authority differs"));
        }
        Ok(Self {
            hierarchy,
            base_codebook,
            high_codebook,
            layout,
            codes,
        })
    }

    /// Build the preregistered one-million-row global diagnostic ownership map.
    #[doc(hidden)]
    pub fn global_geometric_page_layout(
        &self,
        logical_sources: &[u64],
    ) -> Result<V32VirtualPageLayout> {
        if self.layout.source_rows() != 1_000_000 {
            return Err(invalid("V32 global scientific shape differs"));
        }
        self.global_geometric_page_layout_with_capacity(logical_sources, 480)
    }

    fn global_geometric_page_layout_with_capacity(
        &self,
        logical_sources: &[u64],
        page_rows: usize,
    ) -> Result<V32VirtualPageLayout> {
        let count = usize::try_from(self.layout.source_rows())
            .map_err(|_| invalid("V32 global row count overflows"))?;
        if count == 0 || count > 1_000_000 || logical_sources.len() != count {
            return Err(invalid("V32 global logical-source cardinality differs"));
        }
        let base = V30PqReconstructor::new(&self.base_codebook)?;
        let high = V30PqReconstructor::new(&self.high_codebook)?;
        let mut vectors = Vec::with_capacity(count);
        for leaf in self.layout.leaves() {
            let centroid = self
                .hierarchy
                .leaves
                .get(leaf.code_parent_leaf_ordinal as usize)
                .ok_or_else(|| invalid("V32 global code parent differs"))?;
            let end = leaf
                .logical_start
                .checked_add(leaf.row_count)
                .ok_or_else(|| invalid("V32 global leaf range overflows"))?;
            for logical in leaf.logical_start..end {
                if logical != vectors.len() as u64 {
                    return Err(invalid("V32 global logical coverage differs"));
                }
                let (width, code) = self.codes.code(vectors.len())?;
                let reconstructor = match width {
                    V30PqWidth::Base24 => &base,
                    V30PqWidth::High48 => &high,
                };
                let residual = reconstructor.reconstruct(code)?;
                let vector = std::array::from_fn(|dimension| {
                    residual[dimension] + f32::from(centroid[dimension])
                });
                vectors.push(normalized(&vector)?);
            }
        }
        if vectors.len() != count {
            return Err(invalid("V32 global logical coverage differs"));
        }
        let global =
            crate::v32_global_pages::global_balanced_pages(&vectors, logical_sources, page_rows)?;
        Ok(V32VirtualPageLayout {
            page_owners: global.owners,
            page_row_counts: global.row_counts,
        })
    }

    pub fn virtual_geometric_page_layout(
        &self,
        logical_sources: &[u64],
        page_rows: usize,
    ) -> Result<V32VirtualPageLayout> {
        let logical_rows = usize::try_from(self.layout.source_rows())
            .map_err(|_| invalid("V32 virtual layout row count overflows"))?;
        let source_to_logical = logical_sources
            .iter()
            .copied()
            .enumerate()
            .map(|(logical, source)| (source, logical))
            .collect::<BTreeMap<_, _>>();
        if logical_sources.len() != logical_rows || source_to_logical.len() != logical_rows {
            return Err(invalid("V32 virtual logical-source authority differs"));
        }
        let base_reconstructor = V30PqReconstructor::new(&self.base_codebook)?;
        let high_reconstructor = V30PqReconstructor::new(&self.high_codebook)?;
        let mut page_owners = vec![u32::MAX; logical_rows];
        let mut page_row_counts = Vec::new();
        for leaf in self.layout.leaves() {
            let parent = usize::try_from(leaf.code_parent_leaf_ordinal)
                .map_err(|_| invalid("V32 virtual code parent overflows"))?;
            let parent_centroid = self
                .hierarchy
                .leaves
                .get(parent)
                .ok_or_else(|| invalid("V32 virtual code parent differs"))?;
            let leaf_end = leaf
                .logical_start
                .checked_add(leaf.row_count)
                .ok_or_else(|| invalid("V32 virtual leaf range overflows"))?;
            let mut rows = Vec::with_capacity(
                usize::try_from(leaf.row_count)
                    .map_err(|_| invalid("V32 virtual leaf row count overflows"))?,
            );
            for logical in leaf.logical_start..leaf_end {
                let logical_index = usize::try_from(logical)
                    .map_err(|_| invalid("V32 virtual logical ordinal overflows"))?;
                let source_ordinal = *logical_sources
                    .get(logical_index)
                    .ok_or_else(|| invalid("V32 virtual logical-source authority differs"))?;
                let (width, code) = self.codes.code(logical_index)?;
                let reconstructor = match width {
                    V30PqWidth::Base24 => &base_reconstructor,
                    V30PqWidth::High48 => &high_reconstructor,
                };
                let residual = reconstructor.reconstruct(code)?;
                let reconstructed = std::array::from_fn(|dimension| {
                    residual[dimension] + f32::from(parent_centroid[dimension])
                });
                let vector = normalized(&reconstructed)?;
                let (base_code, high_code) = match width {
                    V30PqWidth::Base24 => (code.to_vec(), None),
                    // The discarded base code is unavailable in resident state
                    // and irrelevant to geometric partitioning. The exact high
                    // code remains attached to the diagnostic record.
                    V30PqWidth::High48 => {
                        (vec![0; V30PqWidth::Base24.bytes()], Some(code.to_vec()))
                    }
                };
                rows.push(V30LayoutRecord {
                    leaf_ordinal: leaf.leaf_ordinal,
                    source_ordinal,
                    base_code,
                    high_code,
                    vector,
                });
            }
            for page in partition_v30_leaf_pages(rows, page_rows)? {
                let page_ordinal = u32::try_from(page_row_counts.len())
                    .map_err(|_| invalid("V32 virtual page count overflows"))?;
                let row_count = u16::try_from(page.len())
                    .map_err(|_| invalid("V32 virtual page row count overflows"))?;
                for row in page {
                    let logical = *source_to_logical
                        .get(&row.source_ordinal)
                        .ok_or_else(|| invalid("V32 virtual logical-source authority differs"))?;
                    let owner = page_owners
                        .get_mut(logical)
                        .ok_or_else(|| invalid("V32 virtual logical ordinal differs"))?;
                    if *owner != u32::MAX {
                        return Err(invalid("V32 virtual row ownership differs"));
                    }
                    *owner = page_ordinal;
                }
                page_row_counts.push(row_count);
            }
        }
        if page_owners.contains(&u32::MAX) {
            return Err(invalid("V32 virtual row ownership differs"));
        }
        Ok(V32VirtualPageLayout {
            page_owners,
            page_row_counts,
        })
    }

    #[doc(hidden)]
    pub fn validate_page_locations(&self, locations: &[V32PageLocation]) -> Result<()> {
        if locations.len() != self.layout.pages().len()
            || locations
                .iter()
                .zip(self.layout.pages())
                .any(|(location, page)| {
                    location.page_ordinal != page.identity.ordinal
                        || location.sha256 != page.identity.sha256
                        || location.encoded_bytes != page.identity.encoded_bytes
                        || location.row_count != page.row_count
                })
        {
            return Err(invalid("V32 page locations do not match layout"));
        }
        Ok(())
    }

    fn validate_arm(&self, arm: V32SearchArm) -> Result<()> {
        if arm.root_beam == 0
            || arm.root_beam > self.hierarchy.roots.len()
            || arm.leaf_beam == 0
            || !matches!(
                (arm.leaf_beam, arm.scan_budget),
                (1..64, 65_536) | (64, 65_536) | (128, 131_072) | (256, 262_144)
            )
            || arm.candidate_depth == 0
            || arm.candidate_depth > MAX_CANDIDATES
            || arm.page_count == 0
            || arm.page_count > MAX_SELECTED_PAGES
        {
            return Err(invalid("V32 search arm differs"));
        }
        Ok(())
    }

    pub fn select_pages(&self, query: &[f32; 96], arm: V32SearchArm) -> Result<V32PageSelection> {
        self.select_pages_with_leaf_observer(query, arm, &|_| {})
    }

    #[doc(hidden)]
    pub fn diagnose_logicals(
        &self,
        query: &[f32; 96],
        arm: V32SearchArm,
        logicals: &[u64],
    ) -> Result<Vec<V32RoutingTargetReport>> {
        Ok(self
            .diagnose_logicals_with_selection(query, arm, logicals)?
            .targets)
    }

    #[doc(hidden)]
    pub fn diagnose_logicals_with_selection(
        &self,
        query: &[f32; 96],
        arm: V32SearchArm,
        logicals: &[u64],
    ) -> Result<V32RoutingDiagnostic> {
        let details = self.routing_details(query, arm, &|_| {})?;
        self.diagnose_logicals_from_details(query, logicals, details)
    }

    #[doc(hidden)]
    pub fn diagnose_logicals_with_global_prefix(
        &self,
        query: &[f32; 96],
        arm: V32SearchArm,
        leaf_limit: usize,
        logicals: &[u64],
    ) -> Result<V32RoutingDiagnostic> {
        let details = self.routing_details_global_prefix(query, arm, leaf_limit, &|_| {})?;
        self.diagnose_logicals_from_details(query, logicals, details)
    }

    pub fn diagnose_logicals_with_virtual_geometric_global_prefix(
        &self,
        query: &[f32; 96],
        arm: V32SearchArm,
        leaf_limit: usize,
        logicals: &[u64],
        virtual_layout: &V32VirtualPageLayout,
    ) -> Result<V32VirtualRoutingDiagnostic> {
        self.capture_global_replay(query, arm, leaf_limit)?
            .reduce_virtual(logicals, virtual_layout)
    }

    /// Capture query-only routing once for independently authenticated reductions.
    #[doc(hidden)]
    pub fn capture_global_replay(
        &self,
        query: &[f32; 96],
        arm: V32SearchArm,
        leaf_limit: usize,
    ) -> Result<V32CandidateReplay<'_>> {
        let details = self.routing_details_global_prefix(query, arm, leaf_limit, &|_| {})?;
        Ok(V32CandidateReplay {
            router: self,
            query: *query,
            arm,
            details,
        })
    }

    fn diagnose_virtual_replay(
        &self,
        replay: &V32CandidateReplay<'_>,
        logicals: &[u64],
        virtual_layout: &V32VirtualPageLayout,
    ) -> Result<V32VirtualRoutingDiagnostic> {
        if virtual_layout.page_owners.len() != self.codes.logical_rows() {
            return Err(invalid("V32 virtual page layout cardinality differs"));
        }
        let details = &replay.details;
        let arm = replay.arm;
        let candidate_replay_sha256 = replay.sha256();
        let current = replay.diagnose(logicals)?;
        let mut seen = std::collections::BTreeSet::new();
        let mut virtual_pages = Vec::with_capacity(arm.page_count);
        for candidate in &details.ranked_candidates {
            let page = virtual_layout.page_for_logical(candidate.logical)?;
            if seen.insert(page) {
                virtual_pages.push(page);
                if virtual_pages.len() == arm.page_count {
                    break;
                }
            }
        }
        if virtual_pages.len() != arm.page_count {
            return Err(invalid("V32 virtual selected page cardinality differs"));
        }
        let virtual_pages_at_eight = virtual_pages.iter().copied().take(8).collect::<Vec<_>>();
        if virtual_pages_at_eight.len() != 8 {
            return Err(invalid("V32 virtual eight-page cardinality differs"));
        }
        let selected = virtual_pages.iter().copied().collect::<BTreeSet<_>>();
        let selected_at_eight = virtual_pages_at_eight
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let truth_microleaf_count = current
            .targets
            .iter()
            .map(|target| target.leaf_ordinal)
            .collect::<BTreeSet<_>>()
            .len();
        let truth_virtual_page_count = virtual_layout.truth_page_count(logicals)?;
        let virtual_target_pages = logicals
            .iter()
            .map(|logical| virtual_layout.page_for_logical(*logical))
            .collect::<Result<Vec<_>>>()?;
        let virtual_target_selected = virtual_target_pages
            .iter()
            .map(|page| selected.contains(page))
            .collect::<Vec<_>>();
        let virtual_target_selected_at_eight = virtual_target_pages
            .iter()
            .map(|page| selected_at_eight.contains(page))
            .collect::<Vec<_>>();
        let mut recovered_logicals = Vec::new();
        let mut newly_lost_logicals = Vec::new();
        for target in &current.targets {
            let virtual_selected =
                selected_at_eight.contains(&virtual_layout.page_for_logical(target.logical)?);
            match (target.page_selected, virtual_selected) {
                (false, true) => recovered_logicals.push(target.logical),
                (true, false) => newly_lost_logicals.push(target.logical),
                _ => {}
            }
        }
        Ok(V32VirtualRoutingDiagnostic {
            routing_work: details.selection.work,
            current,
            candidate_replay_sha256,
            virtual_pages,
            virtual_pages_at_eight,
            virtual_target_pages,
            virtual_target_selected,
            virtual_target_selected_at_eight,
            virtual_layout_sha256: virtual_layout.sha256(),
            truth_microleaf_count,
            truth_virtual_page_count,
            recovered_logicals,
            newly_lost_logicals,
        })
    }

    fn diagnose_logicals_from_details(
        &self,
        query: &[f32; 96],
        logicals: &[u64],
        details: RoutingDetails,
    ) -> Result<V32RoutingDiagnostic> {
        let unique = logicals
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        if unique.len() != logicals.len()
            || logicals
                .iter()
                .any(|logical| *logical >= self.layout.source_rows())
        {
            return Err(invalid("V30 routing diagnostic target differs"));
        }
        let query = normalized(query)?;
        let root_ranks = smallest(
            self.hierarchy
                .roots
                .iter()
                .enumerate()
                .map(|(ordinal, centroid)| (centroid_distance(&query, centroid), ordinal))
                .collect(),
            self.hierarchy.roots.len(),
        )
        .into_iter()
        .enumerate()
        .map(|(rank, (_, root))| (root, rank + 1))
        .collect::<BTreeMap<_, _>>();
        let global_routing_leaf_ranks = smallest(
            self.layout
                .leaves()
                .iter()
                .enumerate()
                .map(|(ordinal, leaf)| (centroid_distance(&query, &leaf.routing_centroid), ordinal))
                .collect(),
            self.layout.leaves().len(),
        )
        .into_iter()
        .enumerate()
        .map(|(rank, (_, leaf))| (leaf, rank + 1))
        .collect::<BTreeMap<_, _>>();
        let selection = details.selection.clone();
        let routing_leaf_ranks = details
            .ranked_leaves
            .iter()
            .enumerate()
            .map(|(rank, &leaf)| (leaf, rank + 1))
            .collect::<std::collections::BTreeMap<_, _>>();
        let selected_leaves = details
            .selected_leaves
            .iter()
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        let scanned_pages = details
            .selected_leaves
            .iter()
            .flat_map(|leaf| {
                let range = &self.layout.leaves()[*leaf as usize];
                range.page_start..range.page_start + range.page_count
            })
            .collect::<std::collections::BTreeSet<_>>();
        let candidate_ranks = details
            .ranked_candidates
            .iter()
            .enumerate()
            .map(|(rank, candidate)| (candidate.logical, rank))
            .collect::<std::collections::BTreeMap<_, _>>();
        let mut first_unique_page_ranks = std::collections::BTreeMap::<u32, usize>::new();
        let mut reciprocal_rank_scores = std::collections::BTreeMap::<u32, u64>::new();
        for (rank, candidate) in details.ranked_candidates.iter().enumerate() {
            let page = self
                .layout
                .page_for_logical(candidate.logical)
                .ok_or_else(|| invalid("V30 routing diagnostic page differs"))?;
            let next_unique_rank = first_unique_page_ranks.len();
            first_unique_page_ranks
                .entry(page.identity.ordinal)
                .or_insert(next_unique_rank);
            let weight = 1_000_000_000_000_u64 / (rank as u64 + 1);
            let score = reciprocal_rank_scores
                .entry(page.identity.ordinal)
                .or_default();
            *score = score
                .checked_add(weight)
                .ok_or_else(|| invalid("V30 routing diagnostic rank score overflows"))?;
        }
        let mut reciprocal_ranked_pages = reciprocal_rank_scores.into_iter().collect::<Vec<_>>();
        reciprocal_ranked_pages.sort_unstable_by(|left, right| {
            right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0))
        });
        let reciprocal_rank_pages = reciprocal_ranked_pages
            .iter()
            .take(selection.pages.len())
            .map(|(page, _)| {
                self.layout
                    .pages()
                    .get(*page as usize)
                    .filter(|value| value.identity.ordinal == *page)
                    .map(V30PageRange::identity)
                    .ok_or_else(|| invalid("V30 routing diagnostic page differs"))
            })
            .collect::<Result<Vec<_>>>()?;
        let reciprocal_rank_selected = reciprocal_rank_pages
            .iter()
            .map(|page| page.ordinal)
            .collect::<std::collections::BTreeSet<_>>();
        let selected_pages = details
            .selection
            .pages
            .iter()
            .map(|page| page.ordinal)
            .collect::<std::collections::BTreeSet<_>>();
        let targets = logicals
            .iter()
            .map(|logical| {
                let page = self
                    .layout
                    .page_for_logical(*logical)
                    .ok_or_else(|| invalid("V30 routing diagnostic page differs"))?;
                let routing_leaf = self
                    .layout
                    .leaf_for_logical(*logical)
                    .ok_or_else(|| invalid("V32 routing diagnostic leaf differs"))?;
                let code_parent = usize::try_from(routing_leaf.code_parent_leaf_ordinal)
                    .map_err(|_| invalid("V32 routing diagnostic parent overflows"))?;
                let owner_root_ordinal = *self
                    .hierarchy
                    .leaf_roots
                    .get(code_parent)
                    .ok_or_else(|| invalid("V32 routing diagnostic root differs"))?;
                let candidate_rank = candidate_ranks.get(logical).copied();
                let stage = if selected_pages.contains(&page.identity.ordinal) {
                    V32RoutingTargetStage::SelectedPage
                } else if !selected_leaves.contains(&routing_leaf.leaf_ordinal) {
                    V32RoutingTargetStage::LeafFrontier
                } else if candidate_rank.is_none() {
                    V32RoutingTargetStage::CandidateRetention
                } else {
                    V32RoutingTargetStage::PageReducer
                };
                Ok(V32RoutingTargetReport {
                    logical: *logical,
                    leaf_ordinal: routing_leaf.leaf_ordinal,
                    owner_root_ordinal,
                    owner_root_rank: *root_ranks
                        .get(&usize::from(owner_root_ordinal))
                        .ok_or_else(|| invalid("V32 routing diagnostic root rank differs"))?,
                    global_routing_leaf_rank: *global_routing_leaf_ranks
                        .get(&(routing_leaf.leaf_ordinal as usize))
                        .ok_or_else(|| invalid("V32 routing diagnostic global rank differs"))?,
                    page_ordinal: page.identity.ordinal,
                    routing_leaf_rank: routing_leaf_ranks.get(&routing_leaf.leaf_ordinal).copied(),
                    candidate_rank,
                    first_unique_page_rank: first_unique_page_ranks
                        .get(&page.identity.ordinal)
                        .copied(),
                    page_in_scanned_pool: scanned_pages.contains(&page.identity.ordinal),
                    page_in_retained_pool: first_unique_page_ranks
                        .contains_key(&page.identity.ordinal),
                    page_selected: selected_pages.contains(&page.identity.ordinal),
                    stage,
                    reciprocal_rank_selected: reciprocal_rank_selected
                        .contains(&page.identity.ordinal),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(V32RoutingDiagnostic {
            selection,
            reciprocal_rank_pages,
            targets,
            total_routing_leaves: details.total_routing_leaves,
            scan_budget: details.scan_budget,
            global_leaf_limit: details.global_leaf_limit,
            stop_reason: details.stop_reason,
            next_leaf_rows: details.next_leaf_rows,
        })
    }

    fn select_pages_with_leaf_observer<F>(
        &self,
        query: &[f32; 96],
        arm: V32SearchArm,
        observer: &F,
    ) -> Result<V32PageSelection>
    where
        F: Fn(u32),
    {
        Ok(self.routing_details(query, arm, observer)?.selection)
    }

    fn routing_details<F>(
        &self,
        query: &[f32; 96],
        arm: V32SearchArm,
        observer: &F,
    ) -> Result<RoutingDetails>
    where
        F: Fn(u32),
    {
        self.routing_details_with_global_limit(query, arm, None, observer)
    }

    fn routing_details_global_prefix<F>(
        &self,
        query: &[f32; 96],
        arm: V32SearchArm,
        leaf_limit: usize,
        observer: &F,
    ) -> Result<RoutingDetails>
    where
        F: Fn(u32),
    {
        if !(1..=768).contains(&leaf_limit) {
            return Err(invalid("V32 global routing leaf limit differs"));
        }
        self.routing_details_with_global_limit(query, arm, Some(leaf_limit), observer)
    }

    fn routing_details_with_global_limit<F>(
        &self,
        query: &[f32; 96],
        arm: V32SearchArm,
        global_leaf_limit: Option<usize>,
        observer: &F,
    ) -> Result<RoutingDetails>
    where
        F: Fn(u32),
    {
        self.validate_arm(arm)?;
        let query = normalized(query)?;
        let leaves_eligible = if global_leaf_limit.is_some() {
            self.layout
                .leaves()
                .iter()
                .enumerate()
                .map(|(ordinal, leaf)| (centroid_distance(&query, &leaf.routing_centroid), ordinal))
                .collect()
        } else {
            let roots = smallest(
                self.hierarchy
                    .roots
                    .iter()
                    .enumerate()
                    .map(|(ordinal, centroid)| (centroid_distance(&query, centroid), ordinal))
                    .collect(),
                arm.root_beam,
            );
            eligible_v32_routing_leaf_scores(
                &query,
                self.hierarchy.roots.len(),
                &roots,
                &self.hierarchy.leaf_roots,
                self.layout.leaves(),
            )?
            .0
        };
        let leaves_eligible_count = leaves_eligible.len();
        let ranked_leaves = smallest(leaves_eligible, leaves_eligible_count);
        let (mut selected_leaf_count, mut codes_scanned, stop_reason, next_leaf_rows) =
            if let Some(limit) = global_leaf_limit {
                let rows = ranked_leaves
                    .iter()
                    .map(|(_, leaf)| self.layout.leaves()[*leaf].row_count)
                    .collect::<Vec<_>>();
                bounded_global_prefix_rows(&rows, limit, arm.scan_budget)?
            } else {
                let selected = arm.leaf_beam.min(ranked_leaves.len());
                let codes =
                    ranked_leaves[..selected]
                        .iter()
                        .try_fold(0_u64, |total, (_, leaf)| {
                            total
                                .checked_add(self.layout.leaves()[*leaf].row_count)
                                .ok_or_else(|| invalid("V30 scanned-code count overflows"))
                        })?;
                (selected, codes, V32RoutingStopReason::RootGated, None)
            };
        if global_leaf_limit.is_none() {
            while codes_scanned < arm.candidate_depth as u64
                && selected_leaf_count < ranked_leaves.len()
            {
                codes_scanned = codes_scanned
                    .checked_add(
                        self.layout.leaves()[ranked_leaves[selected_leaf_count].1].row_count,
                    )
                    .ok_or_else(|| invalid("V30 scanned-code count overflows"))?;
                selected_leaf_count += 1;
            }
        }
        if codes_scanned > arm.scan_budget {
            return Err(invalid("V30 scanned-code bound differs"));
        }
        let candidate_depth = arm.candidate_depth.min(
            usize::try_from(codes_scanned)
                .map_err(|_| invalid("V30 scanned-code count overflows"))?,
        );
        let leaves = &ranked_leaves[..selected_leaf_count];

        let selected_leaves = leaves
            .iter()
            .map(|(_, leaf)| *leaf as u32)
            .collect::<Vec<_>>();
        let ranked_leaf_ordinals = ranked_leaves
            .iter()
            .map(|(_, leaf)| *leaf as u32)
            .collect::<Vec<_>>();
        let mut candidates = BoundedCandidates::new(candidate_depth);
        let mut base = Vec::with_capacity(32);
        let mut base_slots = Vec::with_capacity(32);
        let mut high = Vec::with_capacity(32);
        let mut high_slots = Vec::with_capacity(32);
        let mut base_scores = [0.0_f32; 32];
        let mut high_scores = [0.0_f32; 32];
        let mut leaves_by_parent = BTreeMap::<usize, Vec<usize>>::new();
        for (_, leaf) in leaves {
            let range = &self.layout.leaves()[*leaf];
            let code_parent = range.code_parent_leaf_ordinal as usize;
            leaves_by_parent.entry(code_parent).or_default().push(*leaf);
        }
        let mut query_table_pairs_live = 0_usize;
        let mut peak_query_table_pairs_live = 0_usize;
        for (code_parent, parent_leaves) in &leaves_by_parent {
            let residual = std::array::from_fn(|dimension| {
                query[dimension] - f32::from(self.hierarchy.leaves[*code_parent][dimension])
            });
            let base_table = V30QueryTable::new(&self.base_codebook, &residual)?;
            let high_table = V30QueryTable::new(&self.high_codebook, &residual)?;
            query_table_pairs_live += 1;
            peak_query_table_pairs_live = peak_query_table_pairs_live.max(query_table_pairs_live);
            for &leaf in parent_leaves {
                let range = &self.layout.leaves()[leaf];
                observer(range.leaf_ordinal);
                let range_end = range.logical_start + range.row_count;
                for block_start in (range.logical_start..range_end).step_by(32) {
                    let block_end = range_end.min(block_start + 32);
                    base.clear();
                    base_slots.clear();
                    high.clear();
                    high_slots.clear();
                    for logical in block_start..block_end {
                        let slot = usize::try_from(logical - block_start)
                            .map_err(|_| invalid("V30 candidate block offset overflows"))?;
                        let (width, code) = self.codes.code(logical as usize)?;
                        match width {
                            V30PqWidth::Base24 => {
                                base.push(code);
                                base_slots.push(slot);
                            }
                            V30PqWidth::High48 => {
                                high.push(code);
                                high_slots.push(slot);
                            }
                        }
                    }
                    let mut scores = [0.0_f32; 32];
                    base_table.score_block_into(&base, &mut base_scores[..base.len()])?;
                    high_table.score_block_into(&high, &mut high_scores[..high.len()])?;
                    for (&slot, &score) in base_slots.iter().zip(&base_scores) {
                        scores[slot] = score;
                    }
                    for (&slot, &score) in high_slots.iter().zip(&high_scores) {
                        scores[slot] = score;
                    }
                    for logical in block_start..block_end {
                        let candidate = Candidate {
                            score: scores[(logical - block_start) as usize],
                            logical,
                        };
                        candidates.insert(candidate);
                    }
                }
            }
            query_table_pairs_live -= 1;
        }
        debug_assert_eq!(query_table_pairs_live, 0);
        let ranked = candidates.finish();
        let mut seen = std::collections::BTreeSet::new();
        let mut pages = Vec::with_capacity(arm.page_count);
        for candidate in &ranked {
            let page = self
                .layout
                .page_for_logical(candidate.logical)
                .ok_or_else(|| invalid("V30 candidate page mapping differs"))?;
            if seen.insert(page.identity.ordinal) {
                pages.push(page.identity());
                if pages.len() == arm.page_count {
                    break;
                }
            }
        }
        if pages.len() != arm.page_count {
            return Err(invalid("V30 selected page cardinality differs"));
        }
        Ok(RoutingDetails {
            selection: V32PageSelection {
                pages,
                work: V32RoutingWork {
                    roots_scored: self.hierarchy.roots.len(),
                    leaves_eligible: leaves_eligible_count,
                    leaves_scanned: selected_leaf_count,
                    query_table_pairs_built: leaves_by_parent.len(),
                    peak_query_table_pairs_live,
                    codes_scanned,
                    candidates_retained: candidate_depth,
                    pages_considered: seen.len(),
                    selected_pages: arm.page_count,
                },
            },
            selected_leaves,
            ranked_leaves: ranked_leaf_ordinals,
            ranked_candidates: ranked,
            total_routing_leaves: self.layout.leaves().len(),
            scan_budget: arm.scan_budget,
            global_leaf_limit,
            stop_reason,
            next_leaf_rows,
        })
    }
}

impl<S: V32PageStore> V32Index<S> {
    pub fn new(router: V32Router, store: S, arm: V32SearchArm) -> Result<Self> {
        router.validate_arm(arm)?;
        Ok(Self {
            router,
            store,
            arm,
            global_leaf_limit: None,
        })
    }

    /// Serve original physical pages using the diagnostic's global-prefix route.
    #[doc(hidden)]
    pub fn new_global_prefix(
        router: V32Router,
        store: S,
        arm: V32SearchArm,
        leaf_limit: usize,
    ) -> Result<Self> {
        if !(1..=768).contains(&leaf_limit) {
            return Err(invalid("V32 global routing leaf limit differs"));
        }
        let mut index = Self::new(router, store, arm)?;
        index.global_leaf_limit = Some(leaf_limit);
        Ok(index)
    }

    pub fn search(&self, query: &[f32; 96], k: usize) -> Result<V32SearchResult> {
        self.search_observed(query, k, |_phase| Ok(()))
    }

    #[doc(hidden)]
    pub fn cpu_preflight_observation(
        &self,
        query: &[f32; 96],
        k: usize,
    ) -> Result<(V32SearchResult, V32CpuPreflightSample)> {
        let query_started = Instant::now();
        let cpu_started = process_cpu_time_ns()?;
        let mut previous = query_started;
        let mut routing_ns = None;
        let mut page_load_ns = None;
        let mut exact_rerank_ns = None;
        let result = self.search_observed(query, k, |phase| {
            let now = Instant::now();
            let elapsed = duration_ns(now.duration_since(previous))?;
            previous = now;
            match phase {
                V32SearchPhase::RoutingComplete => routing_ns = Some(elapsed),
                V32SearchPhase::PageReadComplete => page_load_ns = Some(elapsed),
                V32SearchPhase::ExactRerankComplete => exact_rerank_ns = Some(elapsed),
            }
            Ok(())
        })?;
        let query_elapsed_ns = duration_ns(query_started.elapsed())?;
        let process_cpu_ns = process_cpu_time_ns()?
            .checked_sub(cpu_started)
            .ok_or_else(|| invalid("V32 process CPU clock moved backwards"))?;
        let work = V32CpuPreflightWork {
            roots_scored: result.work.routing.roots_scored,
            leaves_eligible: result.work.routing.leaves_eligible,
            leaves_scanned: result.work.routing.leaves_scanned,
            query_table_pairs_built: result.work.routing.query_table_pairs_built,
            peak_query_table_pairs_live: result.work.routing.peak_query_table_pairs_live,
            codes_scanned: result.work.routing.codes_scanned,
            candidates_retained: result.work.routing.candidates_retained,
            pages_considered: result.work.routing.pages_considered,
            selected_pages: result.work.routing.selected_pages,
            get_count: result.work.get_count,
            encoded_bytes: result.work.encoded_bytes,
            decoded_rows: result.work.decoded_rows,
            unique_rows: result.work.unique_rows,
        };
        Ok((
            result,
            V32CpuPreflightSample {
                routing_ns: routing_ns
                    .ok_or_else(|| invalid("V32 routing timing boundary is missing"))?,
                page_load_ns: page_load_ns
                    .ok_or_else(|| invalid("V32 page timing boundary is missing"))?,
                exact_rerank_ns: exact_rerank_ns
                    .ok_or_else(|| invalid("V32 rerank timing boundary is missing"))?,
                query_elapsed_ns,
                process_cpu_ns,
                work,
            },
        ))
    }

    #[doc(hidden)]
    pub fn search_observed<F>(
        &self,
        query: &[f32; 96],
        k: usize,
        mut observer: F,
    ) -> Result<V32SearchResult>
    where
        F: FnMut(V32SearchPhase) -> Result<()>,
    {
        if k == 0 || k > 10 {
            return Err(invalid("V30 result count differs"));
        }
        let (selection, candidate_replay_sha256) = if let Some(limit) = self.global_leaf_limit {
            // Preserve the original query bytes used by the diagnostic. Rerank
            // normalization is separate and must not be fed back into routing.
            let replay = self.router.capture_global_replay(query, self.arm, limit)?;
            let hash = replay.sha256();
            (replay.details.selection, Some(hash))
        } else {
            // Keep the existing root-route behavior independently qualified.
            (
                self.router.select_pages(&normalized(query)?, self.arm)?,
                None,
            )
        };
        let query = normalized(query)?;
        observer(V32SearchPhase::RoutingComplete)?;
        let authorized_bytes = selection.pages.iter().try_fold(0_u64, |total, page| {
            total
                .checked_add(page.encoded_bytes)
                .ok_or_else(|| invalid("V30 page byte count overflows"))
        })?;
        if authorized_bytes > MAX_PAGE_BYTES {
            return Err(invalid("V30 page byte bound differs"));
        }
        let bodies = self.store.read_wave(&selection.pages)?;
        if bodies.len() != selection.pages.len() {
            return Err(invalid("V30 page wave cardinality differs"));
        }
        observer(V32SearchPhase::PageReadComplete)?;
        let encoded_bytes = bodies.iter().try_fold(0_u64, |total, body| {
            total
                .checked_add(body.len() as u64)
                .ok_or_else(|| invalid("V30 page byte count overflows"))
        })?;
        if encoded_bytes > MAX_PAGE_BYTES {
            return Err(invalid("V30 page byte bound differs"));
        }
        let reranked = exact_rerank_pages(&selection.pages, &bodies, &query, k)?;
        observer(V32SearchPhase::ExactRerankComplete)?;
        Ok(V32SearchResult {
            matches: reranked.matches,
            candidate_replay_sha256,
            work: V32SearchWork {
                routing: selection.work,
                get_count: selection.pages.len(),
                encoded_bytes,
                decoded_rows: reranked.decoded_rows,
                unique_rows: reranked.unique_rows,
            },
            requested_pages: selection.pages,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use bytes::Bytes;
    use half::f16;

    use super::{
        BoundedCandidates, Candidate, ExactTopK, V32CpuPreflightMode, V32CpuPreflightSample,
        V32CpuPreflightSamples, V32Index, V32Match, V32PageStore, V32Router, V32RoutingStopReason,
        V32RoutingTargetStage, V32SearchArm, V32SearchPhase, bounded_global_prefix_rows,
        canonical_v32_cpu_preflight_receipt, eligible_v32_routing_leaf_scores, exact_rerank_pages,
        smallest, v32_cpu_preflight_expected_work, v32_cpu_preflight_index,
        v32_cpu_preflight_shape,
    };
    use crate::{
        V27Hierarchy, V27PageIdentity, V27PageRow, encode_v27_hierarchy, encode_v27_page,
        v30_s3_layout::{V30Layout, V30PageRange, V32RoutingRange, encode_v30_layout_artifacts},
        v30_s3_pq::{V30CodePlanes, V30PqCodebook, V30PqWidth, encode_v30_pq_artifacts},
    };

    type Components = (
        V27Hierarchy,
        V30PqCodebook,
        V30PqCodebook,
        V30Layout,
        V30CodePlanes,
        Vec<(V27PageIdentity, Vec<u8>)>,
    );

    fn components() -> Components {
        let unit = f16::from_f32(1.0 / 96.0_f32.sqrt());
        let hierarchy = V27Hierarchy {
            roots: vec![[unit; 96]],
            leaves: vec![[unit; 96], [-unit; 96]],
            leaf_roots: vec![0, 0],
        };
        let bodies = (0..20_u32)
            .map(|ordinal| {
                let first = u64::from(ordinal) * 2;
                let rows = [first, first + 1].map(|source_ordinal| V27PageRow {
                    source_ordinal,
                    vector: [0.2 + source_ordinal as f32 / 1_000.0; 96],
                });
                encode_v27_page(ordinal, 2, 0, &rows).unwrap()
            })
            .collect::<Vec<_>>();
        let pages = bodies
            .iter()
            .enumerate()
            .map(|(ordinal, (identity, _))| {
                V30PageRange::from_legacy(ordinal as u64 * 2, 2, identity).unwrap()
            })
            .collect::<Vec<_>>();
        let layout = V30Layout::new(
            40,
            vec![
                V32RoutingRange {
                    leaf_ordinal: 0,
                    code_parent_leaf_ordinal: 0,
                    routing_centroid: [unit; 96],
                    logical_start: 0,
                    row_count: 20,
                    page_start: 0,
                    page_count: 10,
                },
                V32RoutingRange {
                    leaf_ordinal: 1,
                    code_parent_leaf_ordinal: 1,
                    routing_centroid: [-unit; 96],
                    logical_start: 20,
                    row_count: 20,
                    page_start: 10,
                    page_count: 10,
                },
            ],
            pages,
        )
        .unwrap();
        let mut high_bits = vec![0_u32; 4];
        high_bits[0] = 0b11;
        let codes =
            V30CodePlanes::from_packed(40, high_bits, vec![0; 38 * 24], vec![0; 2 * 48]).unwrap();
        let base = V30PqCodebook::new(V30PqWidth::Base24, vec![0.0; 24 * 256 * 4]).unwrap();
        let high = V30PqCodebook::new(V30PqWidth::High48, vec![0.0; 48 * 256 * 2]).unwrap();
        (hierarchy, base, high, layout, codes, bodies)
    }

    fn router() -> (V32Router, Vec<(V27PageIdentity, Vec<u8>)>) {
        let (hierarchy, base, high, layout, codes, bodies) = components();
        (
            V32Router::new(hierarchy, base, high, layout, codes).unwrap(),
            bodies,
        )
    }

    #[test]
    fn v32_routing_microleaf_router_ranks_routing_centroids_but_uses_code_parent() {
        // Break caught: routing ordinals are treated as trained PQ-parent
        // ordinals, or sibling routing centroids are ignored by the frontier.
        let unit = f16::from_f32(1.0 / 96.0_f32.sqrt());
        let hierarchy = V27Hierarchy {
            roots: vec![[unit; 96]],
            leaves: vec![[unit; 96]],
            leaf_roots: vec![0],
        };
        let bodies = [[0.25; 96], [-0.25; 96]]
            .into_iter()
            .enumerate()
            .map(|(ordinal, vector)| {
                encode_v27_page(
                    ordinal as u32,
                    1,
                    0,
                    &[V27PageRow {
                        source_ordinal: ordinal as u64,
                        vector,
                    }],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let layout = V30Layout::new(
            2,
            vec![
                V32RoutingRange {
                    leaf_ordinal: 0,
                    code_parent_leaf_ordinal: 0,
                    routing_centroid: [unit; 96],
                    logical_start: 0,
                    row_count: 1,
                    page_start: 0,
                    page_count: 1,
                },
                V32RoutingRange {
                    leaf_ordinal: 1,
                    code_parent_leaf_ordinal: 0,
                    routing_centroid: [-unit; 96],
                    logical_start: 1,
                    row_count: 1,
                    page_start: 1,
                    page_count: 1,
                },
            ],
            bodies
                .iter()
                .enumerate()
                .map(|(ordinal, (identity, _))| {
                    V30PageRange::from_legacy(ordinal as u64, 1, identity).unwrap()
                })
                .collect(),
        )
        .unwrap();
        let codes = V30CodePlanes::from_packed(2, vec![0; 4], vec![0; 48], vec![]).unwrap();
        let base = V30PqCodebook::new(V30PqWidth::Base24, vec![0.0; 24 * 256 * 4]).unwrap();
        let high = V30PqCodebook::new(V30PqWidth::High48, vec![0.0; 48 * 256 * 2]).unwrap();
        let router = V32Router::new(hierarchy, base, high, layout, codes).unwrap();
        let selection = router
            .select_pages(
                &[-1.0; 96],
                V32SearchArm {
                    root_beam: 1,
                    leaf_beam: 1,
                    scan_budget: 65_536,
                    candidate_depth: 1,
                    page_count: 1,
                },
            )
            .unwrap();
        assert_eq!(selection.pages[0].ordinal, 1);
        assert_eq!(selection.work.codes_scanned, 1);
    }

    fn diagnostic_router() -> V32Router {
        diagnostic_router_with_root_escape(false)
    }

    fn diagnostic_router_with_root_escape(root_escape: bool) -> V32Router {
        let unit = f16::from_f32(1.0 / 96.0_f32.sqrt());
        let hierarchy = V27Hierarchy {
            roots: if root_escape {
                vec![[unit; 96], [-unit; 96]]
            } else {
                vec![[unit; 96]]
            },
            leaves: vec![[unit; 96], [-unit; 96]],
            leaf_roots: vec![0, u16::from(root_escape)],
        };
        let pages = (0..10_u32)
            .map(|ordinal| (ordinal, 0, u64::from(ordinal), 1))
            .chain((10..20_u32).map(|ordinal| (ordinal, 0, 10 + u64::from(ordinal - 10) * 2, 2)))
            .chain((20..50_u32).map(|ordinal| (ordinal, 1, 30 + u64::from(ordinal - 20), 1)))
            .map(|(ordinal, _leaf_ordinal, logical_start, row_count)| {
                let rows = (logical_start..logical_start + u64::from(row_count))
                    .map(|source_ordinal| V27PageRow {
                        source_ordinal,
                        vector: [0.2 + source_ordinal as f32 / 1_000.0; 96],
                    })
                    .collect::<Vec<_>>();
                V30PageRange::from_legacy(
                    logical_start,
                    row_count,
                    &encode_v27_page(ordinal, row_count, 0, &rows).unwrap().0,
                )
                .unwrap()
            })
            .collect();
        let layout = V30Layout::new(
            60,
            vec![
                V32RoutingRange {
                    leaf_ordinal: 0,
                    code_parent_leaf_ordinal: 0,
                    routing_centroid: [unit; 96],
                    logical_start: 0,
                    row_count: 30,
                    page_start: 0,
                    page_count: 20,
                },
                V32RoutingRange {
                    leaf_ordinal: 1,
                    code_parent_leaf_ordinal: 1,
                    // Deliberately close to the query despite its distant owner
                    // root, so root-independent enumeration has unique coverage.
                    routing_centroid: if root_escape { [unit; 96] } else { [-unit; 96] },
                    logical_start: 30,
                    row_count: 30,
                    page_start: 20,
                    page_count: 30,
                },
            ],
            pages,
        )
        .unwrap();
        let codes =
            V30CodePlanes::from_packed(60, vec![0_u32; 4], vec![0; 60 * 24], vec![]).unwrap();
        let base = V30PqCodebook::new(V30PqWidth::Base24, vec![0.0; 24 * 256 * 4]).unwrap();
        let high = V30PqCodebook::new(V30PqWidth::High48, vec![0.0; 48 * 256 * 2]).unwrap();
        V32Router::new(hierarchy, base, high, layout, codes).unwrap()
    }

    #[test]
    fn v32_global_replay_is_captured_once_before_layout_and_truth() {
        // Break: candidate capture depends on layout/truth or reductions reroute.
        let mut router = diagnostic_router();
        router.hierarchy.leaves[1] = router.hierarchy.leaves[0];
        let arm = V32SearchArm {
            root_beam: 1,
            leaf_beam: 1,
            scan_budget: 65_536,
            candidate_depth: 60,
            page_count: 10,
        };
        let replay = router.capture_global_replay(&[0.2; 96], arm, 2).unwrap();
        let control = replay.diagnose(&[0, 31]).unwrap();
        let hash = replay.sha256();
        let sources = (0_u64..60).collect::<Vec<_>>();
        let layout = router
            .global_geometric_page_layout_with_capacity(&sources, 4)
            .unwrap();
        let treatment = replay.reduce_virtual(&[0, 31], &layout).unwrap();
        assert_eq!(treatment.current, control);
        assert_eq!(treatment.candidate_replay_sha256, hash);
        assert_eq!(replay.sha256(), hash);
        let changed = replay.reduce_virtual(&[4, 59], &layout).unwrap();
        assert_eq!(changed.virtual_pages, treatment.virtual_pages);
        assert_eq!(changed.candidate_replay_sha256, hash);
    }

    #[test]
    fn v32_global_replay_hash_binds_work_stop_and_order() {
        // Break: the authority digest omits work or stopping metadata.
        let router = diagnostic_router();
        let arm = V32SearchArm {
            root_beam: 1,
            leaf_beam: 1,
            scan_budget: 65_536,
            candidate_depth: 60,
            page_count: 10,
        };
        let replay = router.capture_global_replay(&[0.2; 96], arm, 2).unwrap();
        let original = replay.sha256();
        for mutation in 0..5 {
            let mut changed = replay.clone();
            match mutation {
                0 => changed.details.selection.work.codes_scanned += 1,
                1 => changed.details.next_leaf_rows = Some(123),
                2 => changed.details.stop_reason = super::V32RoutingStopReason::ScanBudget,
                3 => changed.details.ranked_candidates.swap(0, 1),
                4 => changed.details.ranked_candidates[0].score = 123.0,
                _ => unreachable!(),
            }
            assert_ne!(changed.sha256(), original, "mutation={mutation}");
        }
    }

    #[test]
    fn v32_global_layout_crosses_microleaves_without_losing_ownership() {
        // Break: accidentally reusing the microleaf-exclusive partitioner.
        let mut router = diagnostic_router();
        router.hierarchy.leaves[1] = router.hierarchy.leaves[0];
        let sources = (0_u64..60).collect::<Vec<_>>();
        let layout = router
            .global_geometric_page_layout_with_capacity(&sources, 8)
            .unwrap();
        assert_eq!(layout.page_row_counts(), &[8, 8, 8, 8, 7, 7, 7, 7]);
        assert_eq!(layout.page_for_logical(29).unwrap(), 3);
        assert_eq!(layout.page_for_logical(30).unwrap(), 3);
        assert_eq!(layout.page_count(), 8);
        assert!(
            router
                .global_geometric_page_layout_with_capacity(&sources[..59], 8)
                .is_err()
        );
        let mut duplicate = sources.clone();
        duplicate[59] = 58;
        assert!(
            router
                .global_geometric_page_layout_with_capacity(&duplicate, 8)
                .is_err()
        );
        // The public scientific entry point must not silently run a reduced shape.
        assert!(router.global_geometric_page_layout(&sources).is_err());
    }

    #[test]
    fn v32_global_layout_preserves_replay_and_truth_blind_selection() {
        // Break: changing candidate order/work or using truth to choose pages.
        let mut router = diagnostic_router();
        router.hierarchy.leaves[1] = router.hierarchy.leaves[0];
        let sources = (0_u64..60).collect::<Vec<_>>();
        let global = router
            .global_geometric_page_layout_with_capacity(&sources, 4)
            .unwrap();
        let old = router.virtual_geometric_page_layout(&sources, 4).unwrap();
        let arm = V32SearchArm {
            root_beam: 1,
            leaf_beam: 1,
            scan_budget: 65_536,
            candidate_depth: 60,
            page_count: 10,
        };
        let run = |truth: &[u64], layout: &super::V32VirtualPageLayout| {
            router
                .diagnose_logicals_with_virtual_geometric_global_prefix(
                    &[0.2; 96], arm, 2, truth, layout,
                )
                .unwrap()
        };
        let current = run(&[0, 31], &old);
        let treatment = run(&[0, 31], &global);
        let changed_truth = run(&[4, 59], &global);
        assert_eq!(current.current, treatment.current);
        assert_eq!(
            current.candidate_replay_sha256,
            treatment.candidate_replay_sha256
        );
        assert_eq!(current.routing_work, treatment.routing_work);
        assert_eq!(treatment.virtual_pages, changed_truth.virtual_pages);
        assert_eq!(
            treatment.candidate_replay_sha256,
            changed_truth.candidate_replay_sha256
        );
        assert_ne!(
            treatment.virtual_layout_sha256,
            current.virtual_layout_sha256
        );
        assert_eq!(treatment.virtual_pages_at_eight.len(), 8);
    }

    #[test]
    fn v32_virtual_geometric_layout_is_complete_balanced_and_query_blind() {
        // Break caught: virtual pages continue across routing-microleaf
        // boundaries or page ownership depends on diagnostic truth.
        let router = diagnostic_router();
        let logical_sources = (0_u64..60).collect::<Vec<_>>();
        let first = router
            .virtual_geometric_page_layout(&logical_sources, 8)
            .unwrap();
        let second = router
            .virtual_geometric_page_layout(&logical_sources, 8)
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(first.page_count(), 8);
        assert_eq!(first.page_row_counts(), &[8, 8, 7, 7, 8, 8, 7, 7]);
        assert_eq!(
            (0_u64..60)
                .map(|logical| first.page_for_logical(logical).unwrap())
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            8
        );
        assert!(
            (0_u64..30)
                .map(|logical| first.page_for_logical(logical).unwrap())
                .all(|page| page < 4)
        );
        assert!(
            (30_u64..60)
                .map(|logical| first.page_for_logical(logical).unwrap())
                .all(|page| page >= 4)
        );
        assert!(first.page_for_logical(60).is_err());
    }

    #[test]
    fn v32_virtual_geometric_layout_rejects_source_drift_and_reports_eight_page_obstruction() {
        // Break caught: the diagnostic accepts a non-bijective logical-source
        // authority or hides that nine truth pages cannot fit in eight GETs.
        let router = diagnostic_router();
        let logical_sources = (0_u64..60).collect::<Vec<_>>();
        assert!(
            router
                .virtual_geometric_page_layout(&logical_sources[..59], 4)
                .is_err()
        );
        let mut duplicate = logical_sources.clone();
        duplicate[59] = 58;
        assert!(router.virtual_geometric_page_layout(&duplicate, 4).is_err());
        let layout = router
            .virtual_geometric_page_layout(&logical_sources, 4)
            .unwrap();
        assert_eq!(layout.page_count(), 16);
        assert_eq!(
            layout
                .truth_page_count(&[0, 4, 8, 12, 16, 20, 24, 27, 30])
                .unwrap(),
            9
        );
        assert!(layout.truth_page_count(&[0, 0]).is_err());
    }

    #[test]
    fn v32_virtual_geometric_replay_preserves_candidate_order_and_current_control() {
        // Break caught: virtual replay silently rescored candidates or changed
        // the authenticated current-layout control while swapping page owners.
        let router = diagnostic_router();
        let arm = V32SearchArm {
            root_beam: 1,
            leaf_beam: 1,
            scan_budget: 65_536,
            candidate_depth: 60,
            page_count: 10,
        };
        let logical_sources = (0_u64..60).collect::<Vec<_>>();
        let layout = router
            .virtual_geometric_page_layout(&logical_sources, 4)
            .unwrap();
        let expected = router
            .diagnose_logicals_with_global_prefix(&[0.2; 96], arm, 2, &[0, 15])
            .unwrap();
        let replay = router
            .diagnose_logicals_with_virtual_geometric_global_prefix(
                &[0.2; 96],
                arm,
                2,
                &[0, 15],
                &layout,
            )
            .unwrap();
        assert_eq!(replay.current, expected);
        assert_eq!(replay.virtual_layout_sha256.len(), 64);
        assert_eq!(replay.candidate_replay_sha256.len(), 64);
        assert_eq!(replay.virtual_pages, (0_u32..10).collect::<Vec<_>>());
        assert_eq!(
            replay.virtual_pages_at_eight,
            (0_u32..8).collect::<Vec<_>>()
        );
        assert_eq!(replay.virtual_target_selected_at_eight, vec![true, true]);
        assert_eq!(replay.routing_work, expected.selection.work);
        assert_eq!(replay.truth_microleaf_count, 1);
        assert_eq!(replay.truth_virtual_page_count, 2);
        assert_eq!(replay.recovered_logicals, vec![15]);
        assert!(replay.newly_lost_logicals.is_empty());
    }

    #[test]
    fn v32_virtual_geometric_replay_selects_pages_before_truth_and_counts_losses() {
        // Break caught: truth changes selected virtual pages, or treatment
        // reports only recoveries while hiding rows lost by the new layout.
        let (router, _) = router();
        let arm = V32SearchArm {
            root_beam: 1,
            leaf_beam: 2,
            scan_budget: 65_536,
            candidate_depth: 40,
            page_count: 10,
        };
        let logical_sources = (0_u64..40).collect::<Vec<_>>();
        let layout = router
            .virtual_geometric_page_layout(&logical_sources, 1)
            .unwrap();
        let first = router
            .diagnose_logicals_with_virtual_geometric_global_prefix(
                &[0.2; 96],
                arm,
                2,
                &[0, 15],
                &layout,
            )
            .unwrap();
        let changed_truth = router
            .diagnose_logicals_with_virtual_geometric_global_prefix(
                &[0.2; 96],
                arm,
                2,
                &[1, 14],
                &layout,
            )
            .unwrap();
        assert_eq!(first.virtual_pages, changed_truth.virtual_pages);
        assert_eq!(
            first.virtual_layout_sha256,
            changed_truth.virtual_layout_sha256
        );
        assert_eq!(
            first.candidate_replay_sha256,
            changed_truth.candidate_replay_sha256
        );
        assert_eq!(
            first.virtual_pages_at_eight,
            changed_truth.virtual_pages_at_eight
        );
        assert_eq!(first.virtual_pages, (0_u32..10).collect::<Vec<_>>());
        assert_eq!(first.virtual_pages_at_eight, (0_u32..8).collect::<Vec<_>>());
        assert_eq!(first.virtual_target_selected_at_eight, vec![true, false]);
        assert_eq!(
            changed_truth.virtual_target_selected_at_eight,
            vec![true, false]
        );
        assert!(first.recovered_logicals.is_empty());
        assert_eq!(first.newly_lost_logicals, vec![15]);
        assert_eq!(changed_truth.newly_lost_logicals, vec![14]);
    }

    #[test]
    fn v32_s3_search_diagnoses_every_truth_loss_boundary_without_page_reads() {
        // Break caught: a missed truth row is blamed on the hierarchy when it
        // actually survived into PQ candidates or lost only at page reduction.
        let reports = diagnostic_router()
            .diagnose_logicals(
                &[0.2; 96],
                V32SearchArm {
                    root_beam: 1,
                    leaf_beam: 1,
                    scan_budget: 65_536,
                    candidate_depth: 20,
                    page_count: 10,
                },
                &[0, 15, 25, 35],
            )
            .unwrap();
        assert_eq!(reports.len(), 4);
        assert_eq!(reports[0].stage, V32RoutingTargetStage::SelectedPage);
        assert_eq!(reports[0].routing_leaf_rank, Some(1));
        assert_eq!(reports[0].candidate_rank, Some(0));
        assert_eq!(reports[0].first_unique_page_rank, Some(0));
        assert_eq!(reports[1].stage, V32RoutingTargetStage::PageReducer);
        assert_eq!(reports[1].routing_leaf_rank, Some(1));
        assert_eq!(reports[1].candidate_rank, Some(15));
        assert_eq!(reports[1].first_unique_page_rank, Some(12));
        assert!(reports[1].reciprocal_rank_selected);
        assert_eq!(reports[2].stage, V32RoutingTargetStage::CandidateRetention);
        assert_eq!(reports[2].routing_leaf_rank, Some(1));
        assert_eq!(reports[2].candidate_rank, None);
        assert_eq!(reports[2].first_unique_page_rank, None);
        assert!(!reports[2].reciprocal_rank_selected);
        assert_eq!(reports[3].stage, V32RoutingTargetStage::LeafFrontier);
        assert_eq!(reports[3].routing_leaf_rank, Some(2));
        assert_eq!(reports[3].candidate_rank, None);
        assert_eq!(reports[3].first_unique_page_rank, None);
        assert_eq!(
            reports
                .iter()
                .map(|report| report.logical)
                .collect::<Vec<_>>(),
            [0, 15, 25, 35]
        );
    }

    #[test]
    fn v32_s3_search_diagnostic_reports_structural_work_without_page_reads() {
        // Break caught: the fast containment gate reports truth stages but hides
        // a scanned-code or selected-page-byte hard failure until S3 execution.
        let router = diagnostic_router();
        let arm = V32SearchArm {
            root_beam: 1,
            leaf_beam: 1,
            scan_budget: 65_536,
            candidate_depth: 20,
            page_count: 10,
        };
        let diagnostic = router
            .diagnose_logicals_with_selection(&[0.2; 96], arm, &[0, 15])
            .unwrap();
        let changed_targets = router
            .diagnose_logicals_with_selection(&[0.2; 96], arm, &[2, 18])
            .unwrap();
        assert_eq!(diagnostic.selection, changed_targets.selection);
        assert_eq!(
            diagnostic.reciprocal_rank_pages,
            changed_targets.reciprocal_rank_pages
        );
        assert_eq!(diagnostic.targets.len(), 2);
        assert_eq!(diagnostic.selection.work.codes_scanned, 30);
        assert_eq!(diagnostic.selection.pages.len(), 10);
        assert_eq!(
            diagnostic
                .reciprocal_rank_pages
                .iter()
                .map(|page| page.ordinal)
                .collect::<Vec<_>>(),
            [0, 1, 2, 3, 4, 10, 5, 11, 6, 12]
        );
        assert_eq!(
            diagnostic
                .selection
                .pages
                .iter()
                .map(|page| page.encoded_bytes)
                .sum::<u64>(),
            12_020
        );
    }

    #[test]
    fn v32_s3_search_global_prefix_escapes_a_bad_owner_root_without_truth_input() {
        // Break caught: a globally close routing microleaf is permanently hidden
        // behind a distant trained root even when the code budget can admit it.
        let router = diagnostic_router_with_root_escape(true);
        let arm = V32SearchArm {
            root_beam: 1,
            leaf_beam: 1,
            scan_budget: 65_536,
            candidate_depth: 60,
            page_count: 16,
        };
        let rooted = router
            .diagnose_logicals_with_selection(&[0.2; 96], arm, &[35])
            .unwrap();
        let global = router
            .diagnose_logicals_with_global_prefix(&[0.2; 96], arm, 2, &[35])
            .unwrap();
        let changed_truth = router
            .diagnose_logicals_with_global_prefix(&[0.2; 96], arm, 2, &[31])
            .unwrap();

        assert_eq!(rooted.targets[0].stage, V32RoutingTargetStage::LeafFrontier);
        assert_eq!(rooted.targets[0].routing_leaf_rank, None);
        assert!(!rooted.targets[0].page_in_scanned_pool);
        assert!(!rooted.targets[0].page_in_retained_pool);
        assert!(!rooted.targets[0].page_selected);
        assert_eq!(global.targets[0].owner_root_ordinal, 1);
        assert_eq!(global.targets[0].owner_root_rank, 2);
        assert_eq!(global.targets[0].global_routing_leaf_rank, 2);
        assert_eq!(global.targets[0].routing_leaf_rank, Some(2));
        assert_eq!(global.targets[0].candidate_rank, Some(35));
        assert_eq!(global.targets[0].stage, V32RoutingTargetStage::PageReducer);
        assert!(global.targets[0].page_in_scanned_pool);
        assert!(global.targets[0].page_in_retained_pool);
        assert!(!global.targets[0].page_selected);
        assert_eq!(global.total_routing_leaves, 2);
        assert_eq!(global.scan_budget, 65_536);
        assert_eq!(global.global_leaf_limit, Some(2));
        assert_eq!(global.stop_reason, V32RoutingStopReason::AllLeaves);
        assert_eq!(global.next_leaf_rows, None);
        assert_eq!(global.selection.work.leaves_eligible, 2);
        assert_eq!(global.selection.work.leaves_scanned, 2);
        assert_eq!(global.selection.work.codes_scanned, 60);
        assert_eq!(global.selection, changed_truth.selection);
        assert_eq!(
            global.reciprocal_rank_pages,
            changed_truth.reciprocal_rank_pages
        );
    }

    #[test]
    fn v32_s3_search_global_prefix_budget_boundary_stops_without_skipping() {
        // Break caught: global routing overshoots the code budget or skips an
        // oversized next leaf to cherry-pick a later leaf from outside the prefix.
        let mut exact = vec![1_024_u64; 256];
        exact.push(1);
        assert_eq!(
            bounded_global_prefix_rows(&exact, 768, 262_144).unwrap(),
            (256, 262_144, V32RoutingStopReason::ScanBudget, Some(1))
        );
        assert_eq!(
            bounded_global_prefix_rows(&[262_143, 2, 1], 768, 262_144).unwrap(),
            (1, 262_143, V32RoutingStopReason::ScanBudget, Some(2))
        );
        assert_eq!(
            bounded_global_prefix_rows(&vec![1; 769], 768, 262_144).unwrap(),
            (768, 768, V32RoutingStopReason::LeafLimit, None)
        );
    }

    #[test]
    fn v32_s3_search_containment_counts_a_selected_page_after_candidate_pruning() {
        // Break caught: page containment is understated when a truth row is
        // pruned from the PQ candidate heap but another row selects its page,
        // whose exact rerank would still recover the truth row.
        let reports = diagnostic_router()
            .diagnose_logicals(
                &[0.2; 96],
                V32SearchArm {
                    root_beam: 1,
                    leaf_beam: 1,
                    scan_budget: 65_536,
                    candidate_depth: 11,
                    page_count: 11,
                },
                &[11],
            )
            .unwrap();
        assert_eq!(reports[0].candidate_rank, None);
        assert_eq!(reports[0].page_ordinal, 10);
        assert_eq!(reports[0].stage, V32RoutingTargetStage::SelectedPage);
    }

    #[test]
    fn v32_s3_search_production_uses_bounded_pq_candidates_not_page_centroids() {
        // Break caught: production page selection bypasses the authenticated
        // root/leaf/PQ candidate route and silently returns centroid-only work.
        let (hierarchy, base, high, layout, codes, _) = components();
        let router = V32Router::new(hierarchy, base, high, layout, codes).unwrap();
        let selection = router
            .select_pages(
                &[0.2; 96],
                V32SearchArm {
                    root_beam: 1,
                    leaf_beam: 1,
                    scan_budget: 65_536,
                    candidate_depth: 20,
                    page_count: 10,
                },
            )
            .unwrap();

        assert_eq!(selection.pages.len(), 10);
        assert_eq!(selection.work.roots_scored, 1);
        assert_eq!(selection.work.leaves_eligible, 2);
        assert_eq!(selection.work.leaves_scanned, 1);
        assert_eq!(selection.work.codes_scanned, 20);
        assert_eq!(selection.work.candidates_retained, 20);
        assert_eq!(selection.work.selected_pages, 10);
    }

    #[test]
    fn v32_s3_search_reuses_one_live_query_table_pair_across_code_parents() {
        // Break caught: a per-query parent map retained one 72 KiB base/high
        // table pair per selected parent, multiplying transient memory by the
        // beam and concurrent-query count.
        let (hierarchy, base, high, layout, codes, _) = components();
        let router = V32Router::new(hierarchy, base, high, layout, codes).unwrap();
        let selection = router
            .select_pages(
                &[0.2; 96],
                V32SearchArm {
                    root_beam: 1,
                    leaf_beam: 2,
                    scan_budget: 65_536,
                    candidate_depth: 40,
                    page_count: 16,
                },
            )
            .unwrap();

        assert_eq!(selection.work.query_table_pairs_built, 2);
        assert_eq!(selection.work.peak_query_table_pairs_live, 1);
    }

    #[test]
    fn v32_s3_search_binds_full_page_location_table_before_selecting_sixteen() {
        // Break caught: the full corpus page-location table is confused with
        // the per-query 16-page budget, or identity drift is deferred to GET.
        let (hierarchy, base, high, layout, codes, _) = components();
        let locations = layout
            .pages()
            .iter()
            .map(|page| {
                crate::v30_s3_layout::V32PageLocation::from_hex(
                    page.identity.ordinal,
                    &page.identity.sha256_hex(),
                    page.identity.encoded_bytes,
                    page.row_count,
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(locations.len(), 20);
        let router = V32Router::new(hierarchy, base, high, layout, codes).unwrap();
        router.validate_page_locations(&locations).unwrap();

        let mut drifted = locations;
        drifted[17].encoded_bytes += 1;
        assert!(router.validate_page_locations(&drifted).is_err());
        assert!(router.validate_page_locations(&drifted[..16]).is_err());
    }

    #[test]
    fn v32_s3_search_routes_bounded_frontier_to_exactly_ten_unique_pages() {
        // Break caught: high-dimensional routing degenerates to a full scan,
        // allocates corpus-sized scores, or returns vectors before page decode.
        let (router, _) = router();
        let visited = Mutex::new(Vec::new());
        let selection = router
            .select_pages_with_leaf_observer(
                &[0.2; 96],
                V32SearchArm {
                    root_beam: 1,
                    leaf_beam: 1,
                    scan_budget: 65_536,
                    candidate_depth: 20,
                    page_count: 10,
                },
                &|leaf| visited.lock().unwrap().push(leaf),
            )
            .unwrap();
        assert_eq!(
            selection
                .pages
                .iter()
                .map(|page| page.ordinal)
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
        assert_eq!(selection.work.roots_scored, 1);
        assert_eq!(selection.work.leaves_eligible, 2);
        assert_eq!(selection.work.leaves_scanned, 1);
        assert_eq!(selection.work.codes_scanned, 20);
        assert_eq!(selection.work.candidates_retained, 20);
        assert_eq!(selection.work.selected_pages, 10);
        assert_eq!(*visited.lock().unwrap(), vec![0]);
    }

    #[test]
    fn v32_routing_microleaf_caps_rank_only_candidate_depth_at_eligible_population() {
        // Break caught: the frozen 100K rank-evidence cohort has fewer than
        // 12,288 rows below its root frontier and is rejected before emitting
        // truth-microleaf ranks, or extension scans beyond the complete
        // eligible frontier.
        let unit = f16::from_f32(1.0 / 96.0_f32.sqrt());
        let hierarchy = V27Hierarchy {
            roots: vec![[unit; 96]],
            leaves: vec![[unit; 96]],
            leaf_roots: vec![0],
        };
        let leaves = (0..120_u32)
            .map(|ordinal| {
                let logical_start = u64::from(ordinal) * 100;
                let page_start = u32::try_from(logical_start / 480).unwrap();
                let page_end = u32::try_from((logical_start + 99) / 480).unwrap();
                V32RoutingRange {
                    leaf_ordinal: ordinal,
                    code_parent_leaf_ordinal: 0,
                    routing_centroid: [unit; 96],
                    logical_start,
                    row_count: 100,
                    page_start,
                    page_count: page_end - page_start + 1,
                }
            })
            .collect();
        let pages = (0..12_000_u64)
            .step_by(480)
            .enumerate()
            .map(|(ordinal, logical_start)| {
                let row_count = u16::try_from((12_000 - logical_start).min(480)).unwrap();
                V30PageRange::from_legacy(
                    logical_start,
                    row_count,
                    &V27PageIdentity {
                        ordinal: ordinal as u32,
                        sha256: format!("{:064x}", ordinal + 1),
                        encoded_bytes: 1_000,
                        primary_rows: row_count,
                        replica_rows: 0,
                    },
                )
                .unwrap()
            })
            .collect();
        let layout = V30Layout::new(12_000, leaves, pages).unwrap();
        let codes = V30CodePlanes::from_packed(
            12_000,
            vec![0; 12_000_usize.div_ceil(128) * 4],
            vec![0; 12_000 * 24],
            vec![],
        )
        .unwrap();
        let base = V30PqCodebook::new(V30PqWidth::Base24, vec![0.0; 24 * 256 * 4]).unwrap();
        let high = V30PqCodebook::new(V30PqWidth::High48, vec![0.0; 48 * 256 * 2]).unwrap();
        let router = V32Router::new(hierarchy, base, high, layout, codes).unwrap();
        let visited = Mutex::new(Vec::new());
        let selection = router
            .select_pages_with_leaf_observer(
                &[1.0; 96],
                V32SearchArm {
                    root_beam: 1,
                    leaf_beam: 64,
                    scan_budget: 65_536,
                    candidate_depth: 12_288,
                    page_count: 16,
                },
                &|leaf| visited.lock().unwrap().push(leaf),
            )
            .unwrap();
        assert_eq!(selection.work.codes_scanned, 12_000);
        assert_eq!(selection.work.candidates_retained, 12_000);
        // Break caught: the router reported only the scanned prefix as if it
        // were the complete eligible frontier, hiding full-sort work.
        assert_eq!(selection.work.leaves_eligible, 120);
        assert_eq!(selection.work.leaves_scanned, 120);
        // Break caught: sibling routing microleaves rebuilt the same base/high
        // PQ query-table pair once per microleaf instead of once per parent.
        assert_eq!(selection.work.query_table_pairs_built, 1);
        assert_eq!(visited.lock().unwrap().len(), 120);
        assert_eq!(selection.pages.len(), 16);

        assert!(
            router
                .select_pages(
                    &[1.0; 96],
                    V32SearchArm {
                        root_beam: 1,
                        leaf_beam: 64,
                        scan_budget: 131_072,
                        candidate_depth: 12_288,
                        page_count: 16,
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn v32_s3_search_allows_sixteen_pages_but_no_wider_arm() {
        // Break caught: the registered 16-page quality-recovery arm is rejected,
        // or an unbounded page fanout silently expands S3 work.
        let (router, _) = router();
        let selection = router
            .select_pages(
                &[0.2; 96],
                V32SearchArm {
                    root_beam: 1,
                    leaf_beam: 2,
                    scan_budget: 65_536,
                    candidate_depth: 32,
                    page_count: 16,
                },
            )
            .unwrap();
        assert_eq!(selection.pages.len(), 16);

        assert!(
            router
                .select_pages(
                    &[0.2; 96],
                    V32SearchArm {
                        root_beam: 1,
                        leaf_beam: 2,
                        scan_budget: 65_536,
                        candidate_depth: 32,
                        page_count: 17,
                    },
                )
                .is_err()
        );
    }

    #[test]
    fn v32_s3_search_clamps_leaf_beam_to_complete_selected_root_frontier() {
        // Break caught: a maximum serving beam is treated as an exact required
        // leaf count and rejects a selected-root frontier that is smaller.
        let (router, _) = router();
        let selection = router
            .select_pages(
                &[0.2; 96],
                V32SearchArm {
                    root_beam: 1,
                    leaf_beam: 64,
                    scan_budget: 65_536,
                    candidate_depth: 20,
                    page_count: 10,
                },
            )
            .unwrap();

        assert_eq!(selection.work.leaves_eligible, 2);
        assert_eq!(selection.work.leaves_scanned, 2);
        assert_eq!(selection.work.codes_scanned, 40);
    }

    #[test]
    fn v32_s3_search_exact_rerank_retains_only_k_with_distance_identity_ties() {
        // Break caught: exact reranking allocates and sorts every decoded row,
        // or changes the registered (distance, source ordinal) total order.
        let mut top = ExactTopK::new(3).unwrap();
        for (source_ordinal, squared_distance) in
            [(8, 0.5), (3, 0.25), (7, 0.5), (1, 0.75), (2, 0.25)]
        {
            top.insert(V32Match {
                source_ordinal,
                squared_distance,
            });
        }
        assert_eq!(
            top.finish(),
            vec![
                V32Match {
                    source_ordinal: 2,
                    squared_distance: 0.25,
                },
                V32Match {
                    source_ordinal: 3,
                    squared_distance: 0.25,
                },
                V32Match {
                    source_ordinal: 7,
                    squared_distance: 0.5,
                },
            ]
        );
    }

    #[test]
    fn v32_s3_search_exact_rerank_merges_page_local_top_tens_without_order_drift() {
        // Break caught: independent decoded pages are reranked in one serial
        // loop, or page-local truncation changes the registered exact
        // (f64 distance, source ordinal) final order.
        let bodies = [
            (0..24_u64).step_by(2).collect::<Vec<_>>(),
            (1..24_u64).step_by(2).collect::<Vec<_>>(),
        ]
        .into_iter()
        .enumerate()
        .map(|(page_ordinal, source_ordinals)| {
            let rows = source_ordinals
                .into_iter()
                .map(|source_ordinal| V27PageRow {
                    source_ordinal,
                    vector: [0.2 + source_ordinal as f32 / 1_000.0; 96],
                })
                .collect::<Vec<_>>();
            encode_v27_page(page_ordinal as u32, 12, 0, &rows).unwrap()
        })
        .collect::<Vec<_>>();
        let pages = bodies
            .iter()
            .map(|(identity, _)| identity.clone())
            .collect::<Vec<_>>();
        let payloads = bodies
            .iter()
            .map(|(_, body)| Bytes::copy_from_slice(body))
            .collect::<Vec<_>>();
        let query = super::normalized(&[0.2; 96]).unwrap();
        let mut expected = (0..24_u64)
            .map(|source_ordinal| {
                let vector = [0.2 + source_ordinal as f32 / 1_000.0; 96];
                let squared_distance = vector
                    .iter()
                    .zip(query)
                    .map(|(left, right)| {
                        let delta = f64::from(*left) - f64::from(right);
                        delta * delta
                    })
                    .sum::<f64>();
                V32Match {
                    source_ordinal,
                    squared_distance,
                }
            })
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| {
            left.squared_distance
                .total_cmp(&right.squared_distance)
                .then_with(|| left.source_ordinal.cmp(&right.source_ordinal))
        });
        expected.truncate(10);

        let reranked = exact_rerank_pages(&pages, &payloads, &query, 10).unwrap();

        assert_eq!(reranked.decoded_rows, 24);
        assert_eq!(reranked.unique_rows, 24);
        assert_eq!(reranked.matches, expected);
    }

    #[test]
    fn v32_s3_search_candidate_retention_matches_full_sort_and_stays_bounded() {
        // Break caught: routing pays heap-maintenance cost for every scanned row,
        // changes the registered (score, logical) order, or buffers a full scan.
        const LIMIT: usize = 257;
        const PRUNE_WINDOW: usize = 32_768;
        let input = (0..100_000_u64)
            .rev()
            .map(|logical| Candidate {
                score: ((logical * 17) % 4_099) as f32 / 37.0,
                logical,
            })
            .collect::<Vec<_>>();
        let mut expected = input.clone();
        expected.sort_unstable();
        expected.truncate(LIMIT);

        let mut retained = BoundedCandidates::new(LIMIT);
        for candidate in input {
            retained.insert(candidate);
            assert!(retained.storage_len() <= LIMIT + PRUNE_WINDOW);
        }

        assert_eq!(retained.finish(), expected);
    }

    struct MemoryStore {
        calls: Arc<AtomicUsize>,
        bodies: BTreeMap<u32, Bytes>,
    }

    impl V32PageStore for MemoryStore {
        fn read_wave(&self, pages: &[V27PageIdentity]) -> crate::Result<Vec<Bytes>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            pages
                .iter()
                .map(|page| Ok(self.bodies[&page.ordinal].clone()))
                .collect()
        }
    }

    #[test]
    fn v32_s3_search_fetches_one_arrow_wave_and_exactly_reranks_selected_rows() {
        // Break caught: serving downloads the corpus, issues serial GETs, decodes
        // unauthenticated bytes, or returns approximate rather than exact distances.
        let (router, bodies) = router();
        let calls = Arc::new(AtomicUsize::new(0));
        let store = MemoryStore {
            calls: calls.clone(),
            bodies: bodies
                .into_iter()
                .map(|(identity, bytes)| (identity.ordinal, Bytes::from(bytes)))
                .collect(),
        };
        let index = V32Index::new(
            router,
            store,
            V32SearchArm {
                root_beam: 1,
                leaf_beam: 2,
                scan_budget: 65_536,
                candidate_depth: 20,
                page_count: 10,
            },
        )
        .unwrap();
        let mut phases = Vec::new();
        let result = index
            .search_observed(&[0.2; 96], 10, |phase| {
                phases.push(phase);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            phases,
            [
                V32SearchPhase::RoutingComplete,
                V32SearchPhase::PageReadComplete,
                V32SearchPhase::ExactRerankComplete,
            ]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(result.work.get_count, 10);
        assert_eq!(result.work.decoded_rows, 20);
        assert_eq!(result.work.unique_rows, 20);
        assert_eq!(
            result
                .matches
                .iter()
                .map(|entry| entry.source_ordinal)
                .collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
        assert!(
            result
                .matches
                .windows(2)
                .all(|pair| { pair[0].squared_distance < pair[1].squared_distance })
        );
    }

    struct OversizedStore;

    #[test]
    fn v32_global_serving_matches_replay_and_reads_one_exact_wave() {
        // Break: serving substitutes root routing, normalizes routing twice,
        // changes page order, or performs an extra read/routing pass.
        struct RecordingStore {
            requests: Arc<Mutex<Vec<Vec<V27PageIdentity>>>>,
            bodies: BTreeMap<u32, Bytes>,
        }
        impl V32PageStore for RecordingStore {
            fn read_wave(&self, pages: &[V27PageIdentity]) -> crate::Result<Vec<Bytes>> {
                self.requests.lock().unwrap().push(pages.to_vec());
                Ok(pages
                    .iter()
                    .map(|page| self.bodies[&page.ordinal].clone())
                    .collect())
            }
        }
        let (router, bodies) = router();
        let arm = V32SearchArm {
            root_beam: 1,
            leaf_beam: 1,
            scan_budget: 65_536,
            candidate_depth: 40,
            page_count: 16,
        };
        let query = std::array::from_fn(|i| 0.11 + i as f32 / 173.0);
        let replay = router.capture_global_replay(&query, arm, 2).unwrap();
        let expected = replay.details.selection.clone();
        let expected_hash = replay.sha256();
        assert_ne!(
            expected_hash,
            router
                .capture_global_replay(&super::normalized(&query).unwrap(), arm, 2)
                .unwrap()
                .sha256()
        );
        assert_eq!(expected.pages.len(), 16);
        assert_eq!(expected.work.leaves_scanned, 2);
        let requests = Arc::new(Mutex::new(Vec::new()));
        let store = RecordingStore {
            requests: requests.clone(),
            bodies: bodies
                .into_iter()
                .map(|(page, body)| (page.ordinal, Bytes::from(body)))
                .collect(),
        };
        let index = V32Index::new_global_prefix(router, store, arm, 2).unwrap();
        let result = index.search(&query, 10).unwrap();
        assert_eq!(*requests.lock().unwrap(), vec![expected.pages.clone()]);
        assert_eq!(result.requested_pages, expected.pages);
        assert_eq!(result.work.routing, expected.work);
        assert_eq!(
            result.candidate_replay_sha256.as_deref(),
            Some(expected_hash.as_str())
        );
        assert_eq!(result.work.get_count, 16);
        assert_eq!(result.work.decoded_rows, 32);
        assert_eq!(result.matches.len(), 10);
    }

    #[test]
    fn v32_global_serving_rejects_invalid_limit_before_reads() {
        for limit in [0, 769, usize::MAX] {
            let (router, _) = router();
            let calls = Arc::new(AtomicUsize::new(0));
            let store = MemoryStore {
                calls: calls.clone(),
                bodies: BTreeMap::new(),
            };
            let arm = V32SearchArm {
                root_beam: 1,
                leaf_beam: 1,
                scan_budget: 65_536,
                candidate_depth: 40,
                page_count: 16,
            };
            assert!(V32Index::new_global_prefix(router, store, arm, limit).is_err());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
        }
    }

    impl V32PageStore for OversizedStore {
        fn read_wave(&self, pages: &[V27PageIdentity]) -> crate::Result<Vec<Bytes>> {
            Ok(pages
                .iter()
                .map(|_| Bytes::from(vec![0; 314_573]))
                .collect())
        }
    }

    #[test]
    fn v32_s3_search_rejects_more_than_three_mib_before_page_decode() {
        // Break caught: sixteen maximum-size pages exceed the serving byte
        // budget even though each page independently satisfies its row cap.
        let (router, _) = router();
        let index = V32Index::new(
            router,
            OversizedStore,
            V32SearchArm {
                root_beam: 1,
                leaf_beam: 2,
                scan_budget: 65_536,
                candidate_depth: 20,
                page_count: 10,
            },
        )
        .unwrap();
        let error = index.search(&[0.2; 96], 10).unwrap_err().to_string();
        assert!(
            error.contains("page byte bound"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn v32_s3_search_authenticates_all_routing_artifacts_before_use() {
        // Break caught: serving decodes a role before full-byte authentication or
        // accepts hierarchy/PQ/layout objects from different constructions.
        let (hierarchy, base, high, layout, codes, _) = components();
        let hierarchy_artifacts = encode_v27_hierarchy(&hierarchy).unwrap();
        let pq_artifacts = encode_v30_pq_artifacts(&base, &high, &codes).unwrap();
        let layout_artifacts = encode_v30_layout_artifacts(&layout).unwrap();
        let router =
            V32Router::from_artifacts(&hierarchy_artifacts, &pq_artifacts, &layout_artifacts)
                .unwrap();
        assert_eq!(router.layout.source_rows(), 40);

        let mut corrupted = hierarchy_artifacts.clone();
        corrupted.roots_bytes[0] ^= 1;
        assert!(V32Router::from_artifacts(&corrupted, &pq_artifacts, &layout_artifacts).is_err());
    }

    #[test]
    fn v32_cpu_preflight_projects_exact_100m_cardinality_with_only_the_scan_slice() {
        // Break caught: the cheap CPU gate benchmarks 100K metadata or allocates
        // a 100M-row code plane instead of isolating the scale-sensitive routing
        // cardinality and the exact bounded arm work.
        let expected = [
            (64, 65_536_u64, 3_277_usize),
            (128, 131_072, 6_554),
            (256, 262_144, 13_108),
        ];
        for (leaf_beam, scan_codes, high_codes) in expected {
            let shape = v32_cpu_preflight_shape(leaf_beam).unwrap();
            assert_eq!(shape.source_rows, 100_000_000);
            assert_eq!(shape.roots, 1_024);
            assert_eq!(shape.trained_parents, 65_536);
            assert_eq!(shape.routing_microleaves, 163_192);
            assert_eq!(shape.page_identities, 208_334);
            assert_eq!(shape.root_beam, 64);
            assert_eq!(shape.leaf_beam, leaf_beam);
            assert_eq!(shape.scan_codes, scan_codes);
            assert_eq!(shape.materialized_code_rows, scan_codes);
            assert_eq!(shape.high_width_codes, high_codes);
            assert_eq!(shape.candidate_depth, 12_288);
            assert_eq!(shape.selected_pages, 16);
            assert_eq!(shape.page_bodies, 16);
            assert_eq!(shape.page_rows, 480);
            assert_eq!(shape.candidate_storage, 45_056);
            assert!(shape.maximum_materialized_bytes <= 100 * 1_024 * 1_024);
        }
        assert!(v32_cpu_preflight_shape(32).is_err());
        assert!(v32_cpu_preflight_shape(512).is_err());
    }

    #[test]
    fn v32_cpu_preflight_full_shape_runs_one_production_observation() {
        // Break caught: the cardinality receipt is disconnected from the real
        // router/page validator/exact reranker or allocates a 100M-row plane.
        let shape = v32_cpu_preflight_shape(64).unwrap();
        let index = v32_cpu_preflight_index(&shape).unwrap();
        let (result, sample) = index
            .cpu_preflight_observation(&[1.0 / 96.0_f32.sqrt(); 96], 10)
            .unwrap();
        assert_eq!(result.work.routing.roots_scored, 1_024);
        assert_eq!(result.work.routing.leaves_eligible, 10_200);
        assert_eq!(result.work.routing.leaves_scanned, 64);
        assert_eq!(result.work.routing.query_table_pairs_built, 26);
        assert_eq!(result.work.routing.codes_scanned, 65_536);
        assert_eq!(result.work.routing.candidates_retained, 12_288);
        assert_eq!(result.work.get_count, 16);
        assert_eq!(result.work.encoded_bytes, 3_117_216);
        assert_eq!(result.work.decoded_rows, 16 * 480);
        assert_eq!(result.work.unique_rows, 16 * 480);
        assert_eq!(result.matches.len(), 10);
        assert!(sample.routing_ns > 0);
        assert!(sample.page_load_ns > 0);
        assert!(sample.exact_rerank_ns > 0);
        assert_eq!(
            sample.work,
            v32_cpu_preflight_expected_work(&shape).unwrap()
        );
    }

    #[test]
    fn v32_cpu_preflight_root_membership_is_one_bounded_lookup_per_microleaf() {
        // Break caught: filtering every routing microleaf linearly scans the
        // selected-root beam, multiplying 100M-scale routing work by 64.
        let (hierarchy, _base, _high, layout, _codes, _pages) = components();
        let query = [1.0 / 96.0_f32.sqrt(); 96];
        let (scores, membership_lookups) = eligible_v32_routing_leaf_scores(
            &query,
            hierarchy.roots.len(),
            &[(0.0, 0)],
            &hierarchy.leaf_roots,
            layout.leaves(),
        )
        .unwrap();
        assert_eq!(membership_lookups, layout.leaves().len());
        assert_eq!(scores.len(), 2);
        assert_eq!(scores[0].1, 0);
        assert_eq!(scores[1].1, 1);
    }

    #[test]
    fn v32_cpu_preflight_partial_root_selection_preserves_total_tie_order() {
        // Break caught: replacing the full root sort changes deterministic
        // `(distance, ordinal)` selection at the beam boundary.
        let values = vec![(2.0, 8), (1.0, 7), (1.0, 3), (0.5, 9), (1.0, 1)];
        assert_eq!(
            smallest(values.clone(), 3),
            vec![(0.5, 9), (1.0, 1), (1.0, 3)]
        );
        assert_eq!(smallest(values.clone(), values.len()), {
            let mut expected = values;
            expected.sort_unstable_by(|left, right| {
                left.0
                    .partial_cmp(&right.0)
                    .unwrap()
                    .then_with(|| left.1.cmp(&right.1))
            });
            expected
        });
        assert!(smallest(vec![(1.0, 0)], 0).is_empty());
    }

    #[test]
    fn v32_cpu_preflight_observation_times_the_production_query_boundary_once() {
        // Break caught: the fast gate times a benchmark-only kernel, omits page
        // validation/rerank, or executes the production search more than once.
        let (router, bodies) = router();
        let calls = Arc::new(AtomicUsize::new(0));
        let store = MemoryStore {
            calls: calls.clone(),
            bodies: bodies
                .into_iter()
                .map(|(identity, bytes)| (identity.ordinal, Bytes::from(bytes)))
                .collect(),
        };
        let index = V32Index::new(
            router,
            store,
            V32SearchArm {
                root_beam: 1,
                leaf_beam: 2,
                scan_budget: 65_536,
                candidate_depth: 20,
                page_count: 10,
            },
        )
        .unwrap();
        let (result, sample) = index.cpu_preflight_observation(&[0.2; 96], 10).unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(result.work.get_count, 10);
        assert!(sample.routing_ns > 0);
        assert!(sample.page_load_ns > 0);
        assert!(sample.exact_rerank_ns > 0);
        assert!(
            sample.routing_ns + sample.page_load_ns + sample.exact_rerank_ns
                <= sample.query_elapsed_ns
        );
        assert!(sample.process_cpu_ns > 0);
    }

    #[test]
    fn v32_cpu_preflight_receipt_recomputes_probe_samples_and_stops_early() {
        // Break caught: an optimistic summary drops raw samples or labels a
        // synthetic probe as qualifying evidence after every observation has
        // already exceeded the 64 ms process-CPU gate.
        let sample = V32CpuPreflightSample {
            routing_ns: 40_000_000,
            page_load_ns: 5_000_000,
            exact_rerank_ns: 20_000_001,
            query_elapsed_ns: 66_000_000,
            process_cpu_ns: 70_000_001,
            work: v32_cpu_preflight_expected_work(&v32_cpu_preflight_shape(64).unwrap()).unwrap(),
        };
        let samples = V32CpuPreflightSamples {
            mode: V32CpuPreflightMode::Probe,
            warmups: 0,
            query_count: 128,
            query_seed: 0x243f_6a88_85a3_08d3,
            query_sha256: "1111111111111111111111111111111111111111111111111111111111111111"
                .to_owned(),
            observations: vec![sample; 128],
        };
        let bytes =
            canonical_v32_cpu_preflight_receipt(&v32_cpu_preflight_shape(64).unwrap(), &samples)
                .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["claim_eligible"], false);
        assert_eq!(value["mode"], "probe");
        assert_eq!(value["sample_count"], 128);
        assert_eq!(value["status"], "probe-failed");
        assert_eq!(value["failed_gates"], serde_json::json!(["total-cpu"]));
        assert_eq!(value["gates_enforced"], serde_json::json!(["total-cpu"]));
        assert_eq!(value["query_elapsed_p99_ns"], 66_000_000_u64);
        assert_eq!(value["process_cpu_p99_ns"], 70_000_001_u64);
        assert_eq!(value["raw_samples"][0]["unattributed_ns"], 999_999);
        assert_eq!(value["raw_samples"][0]["routing_ns"], 40_000_000);
        assert_eq!(value["raw_samples"][0]["page_load_ns"], 5_000_000);
        assert_eq!(value["raw_samples"][0]["exact_rerank_ns"], 20_000_001);
        assert_eq!(value["raw_samples"][0]["work"]["roots_scored"], 1_024);
        assert_eq!(value["raw_samples"][0]["work"]["leaves_eligible"], 10_200);
        assert_eq!(value["raw_samples"][0]["work"]["leaves_scanned"], 64);
        assert_eq!(
            value["raw_samples"][0]["work"]["query_table_pairs_built"],
            26
        );
        assert_eq!(value["raw_samples"][0]["work"]["codes_scanned"], 65_536);
        assert_eq!(value["raw_samples"][0]["work"]["get_count"], 16);
        assert_eq!(value["query_count"], 128);
        assert_eq!(value["root_beam"], 64);
        assert_eq!(value["roots"], 1_024);
        assert_eq!(value["trained_parents"], 65_536);
        assert_eq!(value["routing_microleaves"], 163_192);
        assert_eq!(value["eligible_routing_microleaves"], 10_200);
        assert_eq!(value["page_identities"], 208_334);
        assert_eq!(value["selected_pages"], 16);
        assert_eq!(value["candidate_storage"], 45_056);
        assert_eq!(
            value["projected_materialized_bytes"],
            v32_cpu_preflight_shape(64)
                .unwrap()
                .maximum_materialized_bytes
        );
        assert_eq!(bytes.last(), Some(&b'\n'));

        let mut drifted = samples;
        drifted.observations[0].query_elapsed_ns = 65_000_000;
        assert!(
            canonical_v32_cpu_preflight_receipt(&v32_cpu_preflight_shape(64).unwrap(), &drifted,)
                .is_err()
        );
    }
}
