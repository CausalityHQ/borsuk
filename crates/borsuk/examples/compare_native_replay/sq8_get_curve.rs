//! Native GET curve at a fixed 256-row SOURCE shortlist.
use super::{ShortlistArm, ShortlistConfig};
use borsuk::sq8_page_authority::{PageAuthority, SelectedSq8Limits, plan_selected_sq8_rows};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
type Result<T> = crate::Result<T>;
pub(super) const CONFIG_SCHEMA: &str = "borsuk-sq8-shortlist-get-curve-config-v2";
pub(super) const SCHEMA: &str = "borsuk-sq8-shortlist-get-curve-v2";
pub(super) const ADMISSION_SCHEMA: &str = "borsuk-sq8-shortlist-get-curve-admission-v2";
pub(super) const SMOKE_SCHEMA: &str = "borsuk-sq8-shortlist-get-curve-smoke-v2";
pub(super) const EXTRA_PAYLOAD: usize = 256 * 1024 * 1024;
const HISTORICAL_CONTROL_SHA: &str =
    "31dfa30cbf176254e7bec7cc8eb5288071f85cfbb9bb98455b61b3c87f5ec786";
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    schema: String,
    pub(super) shortlist: ShortlistConfig,
    pub(super) get_budgets: [usize; 4],
    pub(super) selector_control: crate::CompletedInput,
}
fn validate_budgets(budgets: &[usize; 4]) -> Result<()> {
    crate::require(
        budgets.iter().all(|g| (1..=256).contains(g)) && budgets.windows(2).all(|g| g[0] < g[1]),
        "GET curve four bounded increasing budgets",
    )
}
pub(super) fn read_config(path: &std::path::Path, sha: &str) -> Result<(Config, u64)> {
    let (c, n): (Config, _) = crate::read_config(path, sha)?;
    validate_budgets(&c.get_budgets)?;
    crate::require(
        c.schema == CONFIG_SCHEMA
            && c.shortlist.shortlist_sizes == [256; 4]
            && c.get_budgets == [30, 38, 46, 64]
            && c.selector_control.sha256 == HISTORICAL_CONTROL_SHA,
        "GET curve schema/fixed256/G30 control",
    )?;
    crate::header_pin(&c.selector_control, 16 * 1024 * 1024)?;
    Ok((c, n))
}
pub(super) fn plan_arms(
    a: &PageAuthority,
    scored: &[(usize, f64)],
    budgets: &[usize; 4],
    limits: SelectedSq8Limits,
    cap: usize,
) -> Result<Vec<ShortlistArm>> {
    validate_budgets(budgets)?;
    let nominees = super::shortlist(a.rows(), scored, 256, cap)?;
    budgets
        .iter()
        .map(|&max_gets| {
            let limits = SelectedSq8Limits { max_gets, ..limits };
            let selected = super::budget_admit_shortlist_blocks(a, scored, &nominees, limits, cap)?;
            let plan = plan_selected_sq8_rows(a, &selected, limits)
                .map_err(|e| format!("GET curve plan {e:?}"))?;
            Ok(ShortlistArm {
                maximum: 256,
                selected,
                ranges: plan.ranges,
                bytes: plan.planned_bytes,
                returned: None,
            })
        })
        .collect()
}
pub(super) fn selection_evidence(arms: &[ShortlistArm]) -> Value {
    json!(
        arms.iter()
            .map(|a| json!({"selected":a.selected,
        "ranges":a.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>()}))
            .collect::<Vec<_>>()
    )
}
pub(super) struct Control {
    pub(super) pin: crate::CompletedInput,
    snapshot: crate::HeaderSnapshot,
    rankings: Vec<Value>,
}
impl Control {
    pub(super) fn admit(pin: crate::CompletedInput) -> Result<Self> {
        crate::header_pin(&pin, 16 * 1024 * 1024)?;
        let (body, snapshot) = crate::loss_binary(&pin, pin.bytes)?;
        let old: Value = serde_json::from_slice(&body)?;
        historical_totals(&old)?;
        let rankings = old["queries"]
            .as_array()
            .ok_or("GET historical queries")?
            .iter()
            .map(|q| q["returned_evidence"][0].clone())
            .collect();
        Ok(Self {
            pin,
            snapshot,
            rankings,
        })
    }
    pub(super) fn recheck(&self) -> Result<()> {
        self.snapshot.recheck()
    }
    pub(super) fn verify_ranking(
        &self,
        slot: usize,
        evidence: &Value,
        budgets: &[usize; 4],
    ) -> Result<bool> {
        self.recheck()?;
        let g30 = budgets
            .iter()
            .position(|&g| g == 30)
            .ok_or("GET curve G30 ranking control")?;
        let old = self
            .rankings
            .get(slot)
            .ok_or("GET historical query ordinal")?;
        validate_returned(&evidence[g30])?;
        Ok(old == &evidence[g30])
    }
    pub(super) fn verify(&self, queries: &[Value], budgets: &[usize; 4]) -> Result<Value> {
        self.recheck()?;
        let (body, snapshot) = crate::loss_binary(&self.pin, self.pin.bytes)?;
        let old: Value = serde_json::from_slice(&body)?;
        let comparison = verify_control_queries(&old, queries, budgets)?;
        snapshot.recheck()?;
        self.recheck()?;
        Ok(comparison)
    }
}
fn historical_totals(old: &Value) -> Result<()> {
    // The count curve's false historical-rank flag concerns its older1024 block control.
    super::count_curve::validate_report(old)?;
    crate::require(
        old["shortlist_sizes"] == json!([256, 512, 768, 1024])
            && old["candidate_sq8_get_budget"] == 30
            && old["baseline_sq8_get_budget"] == 32
            && old["baseline_hits10"] == 9780
            && old["nonratcheting_reference_hits10"] == 9780
            && old["ephemeral_sq8_block_rows"] == 32
            && old["baseline_exact_plan_trace_charges_ids_scorebits"] == true
            && old["truth_file_opened_after_all_baseline_controls"] == true,
        "GET retained historical256 protocol and rank-bearing schema",
    )?;
    for q in old["queries"].as_array().ok_or("GET historical queries")? {
        validate_returned(&q["returned_evidence"][0])?;
    }
    Ok(())
}
fn verify_control_queries(old: &Value, queries: &[Value], budgets: &[usize; 4]) -> Result<Value> {
    historical_totals(old)?;
    let g30 = budgets
        .iter()
        .position(|&g| g == 30)
        .ok_or("GET curve missing G30")?;
    let before = old["queries"].as_array().ok_or("GET historical queries")?;
    crate::require(
        queries.len() == 1000 && before.len() == 1000,
        "GET historical query population",
    )?;
    let (mut rank_queries, mut rank_positions, mut ids, mut rows, mut scores) = (0, 0, 0, 0, 0);
    let (mut invariant_queries, mut relative_queries) = (0, 0);
    let mut evidence = Vec::new();
    for (ordinal, (now, old)) in queries.iter().zip(before).enumerate() {
        crate::require(
            now["ordinal"] == ordinal && old["ordinal"] == ordinal,
            "GET historical query order",
        )?;
        validate_returned(&now["returned_evidence"][g30])?;
        let current_rank = now["returned_evidence"][g30]
            .as_array()
            .ok_or("GET current ranks")?;
        let old_rank = old["returned_evidence"][0]
            .as_array()
            .ok_or("GET historical ranks")?;
        let mut rank_differences = Vec::new();
        for (position, (now, old)) in current_rank.iter().zip(old_rank).enumerate() {
            ids += usize::from(now["id"] != old["id"]);
            rows += usize::from(now["target_row"] != old["target_row"]);
            scores += usize::from(now["score_bits"] != old["score_bits"]);
            if now != old {
                rank_positions += 1;
                rank_differences.push(json!({"position":position,"current":now,"historical":old}));
            }
        }
        rank_queries += usize::from(!rank_differences.is_empty());
        let mut current_arm = now["arms"][g30].clone();
        current_arm
            .as_object_mut()
            .ok_or("GET current arm")?
            .remove("get_budget");
        let old_arm = &old["arms"][0];
        let relative = |q: &Value, arm: &Value| {
            json!({
            "baseline_hits10":q["baseline_hits10"],"gained":arm["quality"]["gained"],
            "lost":arm["quality"]["lost"]})
        };
        let relative_same = relative(now, &current_arm) == relative(old, old_arm);
        let invariant = |arm: &Value| -> Result<Value> {
            let mut arm = arm.clone();
            let quality = arm["quality"].as_object_mut().ok_or("GET arm quality")?;
            quality.remove("gained");
            quality.remove("lost");
            Ok(arm)
        };
        let invariant_same = invariant(&current_arm)? == invariant(old_arm)?;
        invariant_queries += usize::from(!invariant_same);
        relative_queries += usize::from(!relative_same);
        if (!rank_differences.is_empty() || !invariant_same || !relative_same) && evidence.len() < 8
        {
            evidence.push(
                json!({"ordinal":ordinal,"rank_differences":rank_differences,
                "current_arm":current_arm,"historical_arm":old_arm,
                "current_baseline_hits10":now["baseline_hits10"],
                "historical_baseline_hits10":old["baseline_hits10"]}),
            );
        }
    }
    Ok(json!({
        "scope":"CURRENT_G30_VS_PINNED_HISTORICAL_COUNT_ARM0_DESCRIPTIVE",
        "query_count":1000,"rank_scope":"ORDERED_ID_TARGET_ROW_SCORE_BITS",
        "ordered_rank_parity":rank_queries==0,"rank_mismatched_queries":rank_queries,
        "rank_mismatched_positions":rank_positions,"id_mismatched_positions":ids,
        "target_row_mismatched_positions":rows,"score_bits_mismatched_positions":scores,
        "arm_invariant_scope":"GEOMETRY_AND_QUALITY_EXCLUDING_GAIN_LOSS",
        "arm_invariant_parity":invariant_queries==0,"arm_invariant_mismatched_queries":invariant_queries,
        "baseline_relative_scope":"BASELINE_HITS_AND_ARM_GAIN_LOSS",
        "baseline_relative_parity":relative_queries==0,"baseline_relative_mismatched_queries":relative_queries,
        "evidence_limit":8,"mismatch_evidence":evidence
    }))
}

fn budgets(report: &Value) -> Result<[usize; 4]> {
    let budgets = serde_json::from_value(report["get_budgets"].clone())?;
    validate_budgets(&budgets)?;
    crate::require(budgets == [30, 38, 46, 64], "GET report frozen budgets")?;
    Ok(budgets)
}
// Geometry validation uses the real planner with the frozen shape; payload is never read.
fn geometry_authority() -> Result<PageAuthority> {
    let digests = vec![0_u8; 1_000_000_usize.div_ceil(32) * 32];
    let manifest = serde_json::to_vec(&json!({"schema":"borsuk-v115-sq8-page-authority-v2",
        "generation":1,"rows":1_000_000,"dimensions":1024,"page_rows":32,
        "object_sha256":"0".repeat(64),"page_digest_sha256":format!("{:x}",Sha256::digest(&digests))}))?;
    PageAuthority::load(
        &manifest,
        &format!("{:x}", Sha256::digest(&manifest)),
        &digests,
    )
    .map_err(|e| format!("GET report geometry authority {e:?}").into())
}
fn aggregate(report: &Value) -> Result<Value> {
    let budgets = budgets(report)?;
    let mut totals = super::count_curve::aggregate_arms(report, &[256; 4], &budgets)?;
    for name in [
        "selected_rows",
        "selected_blocks",
        "pre_cap_runs",
        "shortlist_truth_rows",
        "selected_block_truth_rows",
        "bridge_rescued_hits",
        "block_expansion_rescued_hits",
    ] {
        totals[name] = json!([0, 0, 0, 0]);
    }
    let authority = geometry_authority()?;
    let mut baseline = crate::Charge::default();
    for q in report["queries"].as_array().ok_or("GET queries")? {
        validate_shared_evidence(q)?;
        let blocks: Vec<usize> = serde_json::from_value(q["baseline_sq8_selected_blocks"].clone())?;
        crate::require(
            !blocks.is_empty()
                && blocks.len() <= 256
                && blocks.windows(2).all(|p| p[0] < p[1])
                && blocks.last().is_some_and(|b| *b < 1_000_000 / 32),
            "GET baseline selected block evidence",
        )?;
        let selected = blocks.iter().map(|b| b * 32).collect::<Vec<_>>();
        let plan = plan_selected_sq8_rows(
            &authority,
            &selected,
            SelectedSq8Limits {
                max_gets: 32,
                max_bytes: 32 * 1024 * 1024,
                max_planner_bytes: 4 * 1024 * 1024 + 4096,
            },
        )
        .map_err(|e| format!("GET baseline exact planner {e:?}"))?;
        let charges: crate::Charge = serde_json::from_value(q["baseline_sq8_charges"].clone())?;
        crate::require(
            charges.submitted_gets == plan.ranges.len() as u64
                && charges.verified_bytes == plan.planned_bytes as u64
                && charges.failed_gets == 0,
            "GET baseline charges bound to production selection geometry",
        )?;
        baseline.add(charges)?;
    }
    let mut statistics = Vec::with_capacity(4);
    for (index, &budget) in budgets.iter().enumerate() {
        let mut gets = Vec::with_capacity(1000);
        let mut bytes = Vec::with_capacity(1000);
        for q in report["queries"].as_array().ok_or("GET queries")? {
            let arm = &q["arms"][index];
            crate::require(arm["get_budget"] == budget, "GET arm ceiling binding")?;
            let selected: Vec<usize> =
                serde_json::from_value(q["selection_evidence"][index]["selected"].clone())?;
            crate::require(
                !selected.is_empty()
                    && selected.len() <= 256
                    && selected.windows(2).all(|p| p[0] < p[1])
                    && selected.last().is_some_and(|r| *r < 1_000_000),
                "GET selected row evidence",
            )?;
            let plan = plan_selected_sq8_rows(
                &authority,
                &selected,
                SelectedSq8Limits {
                    max_gets: budget,
                    max_bytes: 32 * 1024 * 1024,
                    max_planner_bytes: 4 * 1024 * 1024 + 4096,
                },
            )
            .map_err(|e| format!("GET report exact planner {e:?}"))?;
            let blocks = selected
                .iter()
                .map(|r| r / 32)
                .collect::<std::collections::BTreeSet<_>>();
            let runs = blocks
                .iter()
                .filter(|&&b| b == 0 || !blocks.contains(&(b - 1)))
                .count();
            crate::require(
                arm["selected_rows"] == selected.len()
                    && arm["selected_blocks"] == blocks.len()
                    && arm["pre_cap_runs"] == runs
                    && arm["gets"] == plan.ranges.len()
                    && arm["physical_bytes"] == plan.planned_bytes
                    && arm["bridge_bytes"]
                        == plan.planned_bytes - blocks.len() * super::GEOMETRY_BLOCK_BYTES as usize
                    && q["selection_evidence"][index]["ranges"]
                        == json!(
                            plan.ranges
                                .iter()
                                .map(|r| [r.start, r.end])
                                .collect::<Vec<_>>()
                        ),
                "GET exact selection/cover/charge evidence",
            )?;
            validate_quality(q, index, &selected, &plan.ranges)?;
            for (name, section) in [
                ("selected_rows", arm),
                ("selected_blocks", arm),
                ("pre_cap_runs", arm),
                ("shortlist_truth_rows", &arm["quality"]),
                ("selected_block_truth_rows", &arm["quality"]),
                ("bridge_rescued_hits", &arm["quality"]),
                ("block_expansion_rescued_hits", &arm["quality"]),
            ] {
                let value = section[name].as_u64().ok_or("GET unsigned arm subtotal")?;
                let total = totals[name][index]
                    .as_u64()
                    .ok_or("GET unsigned accumulated subtotal")?;
                totals[name][index] =
                    json!(total.checked_add(value).ok_or("GET subtotal overflow")?);
            }
            gets.push(plan.ranges.len() as u64);
            bytes.push(plan.planned_bytes as u64);
        }
        let stats = |mut v: Vec<u64>| -> Value {
            v.sort_unstable();
            json!({"count":1000,
            "total":v.iter().sum::<u64>(),"p50":v[499],"p90":v[899],"p95":v[949],"p99":v[989],"max":v[999]})
        };
        statistics.push(json!({"get_budget":budget,"gets":stats(gets),"bytes":stats(bytes)}));
    }
    Ok(json!({"totals":totals,"statistics":statistics,
        "baseline_sq8_gets":baseline.submitted_gets,"baseline_sq8_bytes":baseline.verified_bytes}))
}
fn validate_shared_evidence(q: &Value) -> Result<()> {
    use std::collections::{BTreeMap, BTreeSet};
    let selected = (0..4)
        .map(|a| {
            serde_json::from_value::<Vec<usize>>(q["selection_evidence"][a]["selected"].clone())
        })
        .collect::<std::result::Result<Vec<_>, _>>()?;
    crate::require(
        selected.iter().flatten().collect::<BTreeSet<_>>().len() <= 256,
        "GET shared shortlist union",
    )?;
    let blocks = selected
        .iter()
        .map(|rows| rows.iter().map(|r| r / 32).collect::<BTreeSet<_>>())
        .collect::<Vec<_>>();
    let mut ids = BTreeMap::new();
    let mut rows = BTreeMap::new();
    for a in 0..4 {
        for p in q["returned_evidence"][a]
            .as_array()
            .ok_or("GET returned evidence")?
        {
            let id = p["id"].as_u64().ok_or("GET shared ID")?;
            let row = p["target_row"].as_u64().ok_or("GET shared row")?;
            let bits = p["score_bits"].as_u64().ok_or("GET shared score")?;
            crate::require(
                ids.insert(id, (row, bits))
                    .is_none_or(|old| old == (row, bits))
                    && rows
                        .insert(row, (id, bits))
                        .is_none_or(|old| old == (id, bits)),
                "GET shared returned ID/row/score mapping",
            )?;
        }
        for b in 0..a {
            crate::require(
                q["selection_evidence"][a]["ranges"] != q["selection_evidence"][b]["ranges"]
                    || q["returned_evidence"][a] == q["returned_evidence"][b],
                "GET identical covers require identical rankings",
            )?;
            for block in blocks[a].intersection(&blocks[b]) {
                crate::require(
                    selected[a]
                        .iter()
                        .filter(|r| **r / 32 == *block)
                        .eq(selected[b].iter().filter(|r| **r / 32 == *block)),
                    "GET identical nominees within common admitted blocks",
                )?;
            }
        }
    }
    Ok(())
}
fn validate_returned(evidence: &Value) -> Result<std::collections::BTreeSet<(u64, usize)>> {
    use std::collections::BTreeSet;
    let returned = evidence.as_array().ok_or("GET returned evidence")?;
    crate::require(returned.len() == 10, "GET returned k")?;
    let mut ids = BTreeSet::new();
    let mut rows = BTreeSet::new();
    let mut positions = BTreeSet::new();
    let mut old_score = None;
    for p in returned {
        crate::fields(p, "id target_row score_bits")?;
        let row = usize::try_from(p["target_row"].as_u64().ok_or("GET returned row")?)?;
        let id = p["id"].as_u64().ok_or("GET returned id")?;
        let bits = u32::try_from(p["score_bits"].as_u64().ok_or("GET score bits")?)?;
        let score = f32::from_bits(bits);
        crate::require(
            row < 1_000_000
                && id < 1_000_000
                && score.is_finite()
                && ids.insert(id)
                && rows.insert(row)
                && old_score.is_none_or(|(s, previous_id): (f32, u64)| {
                    s.total_cmp(&score).then(previous_id.cmp(&id)).is_lt()
                }),
            "GET unique in-domain returned rows/IDs and finite ordered scores",
        )?;
        old_score = Some((score, id));
        positions.insert((id, row));
    }
    Ok(positions)
}
fn validate_quality(
    q: &Value,
    index: usize,
    selected: &[usize],
    ranges: &[std::ops::Range<usize>],
) -> Result<()> {
    use std::collections::BTreeSet;
    let baseline: Vec<u64> = serde_json::from_value(q["quality_evidence"]["baseline_ids"].clone())?;
    let truth = q["quality_evidence"]["truth"]
        .as_array()
        .ok_or("GET truth evidence")?;
    crate::require(
        baseline.len() == 10
            && baseline.iter().all(|&id| id < 1_000_000)
            && baseline.iter().collect::<BTreeSet<_>>().len() == 10
            && truth.len() == 10,
        "GET baseline/truth evidence shape",
    )?;
    let positions = validate_returned(&q["returned_evidence"][index])?;
    let ids = positions.iter().map(|&(id, _)| id).collect::<BTreeSet<_>>();
    crate::require(
        positions.iter().all(|&(_, row)| {
            ranges
                .iter()
                .any(|r| r.start / 1036 <= row && row < r.end / 1036)
        }),
        "GET returned cover evidence",
    )?;
    let mut quality = super::ArmQuality::default();
    let mut base = 0;
    let mut truth_ids = BTreeSet::new();
    let mut truth_rows = BTreeSet::new();
    for t in truth {
        let id = t["id"].as_u64().ok_or("GET truth ID")?;
        let row = usize::try_from(t["target_row"].as_u64().ok_or("GET truth row")?)?;
        let source = t["source_scored"]
            .as_bool()
            .ok_or("GET truth SOURCE membership")?;
        crate::require(
            id < 1_000_000
                && row < 1_000_000
                && truth_ids.insert(id)
                && truth_rows.insert(row)
                && (!ids.contains(&id) || positions.contains(&(id, row))),
            "GET unique truth/returned mapping",
        )?;
        let found = ids.contains(&id);
        let original = baseline.contains(&id);
        let nominated = selected.binary_search(&row).is_ok();
        let block = selected.iter().any(|r| r / 32 == row / 32);
        crate::require(
            !nominated || source,
            "GET shortlisted truth must have been SOURCE scored",
        )?;
        let covered = ranges
            .iter()
            .any(|r| r.start / 1036 <= row && row < r.end / 1036);
        base += usize::from(original);
        quality.hits10 += usize::from(found);
        quality.gained += usize::from(found && !original);
        quality.lost += usize::from(original && !found);
        quality.shortlist_truth_rows += usize::from(nominated);
        quality.selected_block_truth_rows += usize::from(block);
        if found {
            quality.bridge_rescued_hits += usize::from(!block);
            quality.block_expansion_rescued_hits += usize::from(block && !nominated);
        } else if !covered {
            quality.miss_buckets[usize::from(source)] += 1;
        } else {
            quality.miss_buckets[if nominated { 2 } else { 3 }] += 1;
        }
    }
    crate::require(
        q["baseline_hits10"] == base
            && q["arms"][index]["quality"] == serde_json::to_value(quality)?,
        "GET recomputed actual returned quality evidence",
    )
}
pub(super) fn finalize_report(report: &mut Value, comparison: Value) -> Result<()> {
    validate_completed_baseline_costs(report)?;
    finalize_report_with_control_sha(report, comparison, HISTORICAL_CONTROL_SHA)
}
fn finalize_report_with_control_sha(
    report: &mut Value,
    comparison: Value,
    expected_control_sha: &str,
) -> Result<()> {
    report["schema"] = json!(SCHEMA);
    report["shortlist_sizes"] = json!([256, 256, 256, 256]);
    report["control_arm_index"] = json!(
        budgets(report)?
            .iter()
            .position(|&g| g == 30)
            .ok_or("GET G30 index")?
    );
    report["closed_selector_control_parity"] = comparison["arm_invariant_parity"].clone();
    report["closed_selector_control_parity_scope"] =
        json!("GEOMETRY_AND_QUALITY_EXCLUDING_GAIN_LOSS");
    report["historical_returned_rank_parity"] = comparison["ordered_rank_parity"].clone();
    report["historical_returned_rank_parity_scope"] = json!("GET_G30_VS_COUNT_CURVE_ARM0");
    report["historical_comparison"] = comparison;
    report["historical_comparison_is_descriptive"] = json!(true);
    report["causal_get_comparison_scope"] = json!("FOUR_CURRENT_ARMS_SHARING_SOURCE");
    report["reference_and_floor_are_descriptive"] = json!(true);
    report["original_physical_layout"] = json!(true);
    report["source_capture_routing_scoring_unchanged"] = json!(true);
    report["source_to_shortlist_cover_loss_includes_budgeted_block_drops"] = json!(true);
    report["input_shortlist_size"] = json!(256);
    report["candidate_sq8_byte_cap"] = json!(32 * 1024 * 1024);
    report["selection_rule"] = json!("MAX_SOURCE_SCORE_BLOCK_GREEDY_EXACT_COVER_ADMISSION");
    report["single_causal_get_change"] = json!(true);
    report["causal_win_claim"] = json!(false);
    report["cost_claim"] = json!(false);
    report["automatic_production_gate"] = json!(false);
    let aggregate = aggregate(report)?;
    report["native_quality_io_totals"] = aggregate["totals"].clone();
    report["io_statistics"] = aggregate["statistics"].clone();
    report["planned_target_gets_totals"] = aggregate["totals"]["gets"].clone();
    report["planned_target_bytes_totals"] = aggregate["totals"]["bytes"].clone();
    report
        .as_object_mut()
        .ok_or("GET report object")?
        .remove("candidate_sq8_get_budget");
    report
        .as_object_mut()
        .ok_or("GET report object")?
        .remove("physical_bytes_totals");
    validate_report_with_control_sha(report, expected_control_sha)
}
// Baseline report geometry is not provenance. Reauthenticate the pinned native
// file through the shared full-cohort reader, then compare every query charge.
fn validate_completed_baseline_costs(report: &Value) -> Result<()> {
    let completed: crate::CompletedConfig =
        serde_json::from_value(report["baseline_completed"].clone())?;
    crate::require(
        completed.schema == crate::SCALE_CONFIG_SCHEMA,
        "GET baseline completed config schema",
    )?;
    let queries = report["queries"].as_array().ok_or("GET baseline queries")?;
    crate::require(queries.len() == 1000, "GET baseline full query roster")?;
    let mut total = crate::Charge::default();
    let run = crate::read_run_observed(
        &completed.input.path,
        &completed.input.sha256,
        Some(&completed),
        |line, query, _| {
            let native: Value = serde_json::from_slice(line)?;
            let recorded = queries.get(query.ordinal).ok_or("GET baseline ordinal")?;
            let charge: crate::Charge =
                serde_json::from_value(recorded["baseline_sq8_charges"].clone())?;
            crate::require(
                recorded["ordinal"].as_u64() == Some(query.ordinal as u64)
                    && charge == query.charges.sq8
                    && recorded["baseline_sq8_selected_blocks"] == native["plan"]["selected_pages"],
                "GET baseline charges disagree with authenticated native query",
            )?;
            total.add(charge)
        },
    )?;
    crate::require(
        run.samples.len() == 1000
            && report["baseline_sq8_gets_total"].as_u64() == Some(total.submitted_gets)
            && report["baseline_sq8_bytes_total"].as_u64() == Some(total.verified_bytes),
        "GET baseline authenticated totals",
    )
}
pub(super) fn validate_report(report: &Value) -> Result<()> {
    validate_completed_baseline_costs(report)?;
    validate_report_with_control_sha(report, HISTORICAL_CONTROL_SHA)
}
fn validate_report_with_control_sha(report: &Value, expected_control_sha: &str) -> Result<()> {
    let budgets = budgets(report)?;
    crate::require(
        report["schema"] == SCHEMA
            && report["status"] == "MEASURED"
            && report["complete"] == true
            && report["query_count"] == 1000
            && report["shortlist_sizes"] == json!([256, 256, 256, 256])
            && report["input_shortlist_size"] == 256
            && report["candidate_sq8_byte_cap"] == 32 * 1024 * 1024
            && report["miss_bucket_order"]
                == json!([
                    "outside_source_and_cover",
                    "source_to_shortlist_cover_loss",
                    "shortlisted_covered_ranking_loss",
                    "unshortlisted_covered_ranking_loss"
                ])
            && report["admissible_queries"] == json!([1000, 1000, 1000, 1000])
            && report["control_arm_index"]
                == budgets
                    .iter()
                    .position(|&g| g == 30)
                    .ok_or("GET G30 index")?
            && report["closed_selector_control_parity_scope"]
                == "GEOMETRY_AND_QUALITY_EXCLUDING_GAIN_LOSS"
            && report["historical_comparison_is_descriptive"] == true
            && report["causal_get_comparison_scope"] == "FOUR_CURRENT_ARMS_SHARING_SOURCE"
            && report["reference_and_floor_are_descriptive"] == true
            && report["historical_returned_rank_parity_scope"] == "GET_G30_VS_COUNT_CURVE_ARM0"
            && report["historical_returned_ranks_checked_before_truth"] == true
            && report["historical_count_curve_older_block_control_rank_parity"] == false
            && report["original_physical_layout"] == true
            && report["source_capture_routing_scoring_unchanged"] == true
            && report["single_causal_get_change"] == true
            && report["nonratcheting_reference_hits10"] == 9780
            && report["recall_floor_hits10"] == 9760
            && report["baseline_sq8_get_budget"] == 32
            && report["ephemeral_sq8_block_rows"] == 32
            && report["baseline_exact_plan_trace_charges_ids_scorebits"] == true
            && report["truth_file_opened_after_all_baseline_controls"] == true
            && report["selection_rule"] == "MAX_SOURCE_SCORE_BLOCK_GREEDY_EXACT_COVER_ADMISSION"
            && report["source_to_shortlist_cover_loss_includes_budgeted_block_drops"] == true
            && report["cold_latency_claim"] == false
            && report["qps_claim"] == false
            && report["vendor_win_claim"] == false
            && report["qualified"] == false
            && report["physical_s3_or_billed_bytes"] == false
            && report["causal_win_claim"] == false
            && report["cost_claim"] == false
            && report["automatic_production_gate"] == false
            && report.get("candidate_sq8_get_budget").is_none(),
        "GET complete fixed protocol report",
    )?;
    let pin: crate::CompletedInput = serde_json::from_value(report["historical_control"].clone())?;
    crate::header_pin(&pin, 16 * 1024 * 1024)?;
    crate::require(
        pin.sha256 == expected_control_sha,
        "GET exact retained control source pin",
    )?;
    let control = Control::admit(pin)?;
    let comparison =
        control.verify(report["queries"].as_array().ok_or("GET queries")?, &budgets)?;
    crate::require(
        report["historical_comparison"] == comparison
            && report["historical_returned_rank_parity"] == comparison["ordered_rank_parity"]
            && report["closed_selector_control_parity"] == comparison["arm_invariant_parity"],
        "GET recomputed descriptive historical comparison",
    )?;
    let aggregate = aggregate(report)?;
    control.recheck()?;
    let totals = &aggregate["totals"];
    let floor = (0..4)
        .map(|a| totals["hits"][a].as_u64().is_some_and(|h| h >= 9760))
        .collect::<Vec<_>>();
    crate::require(
        report["baseline_sq8_gets_total"].as_u64() == aggregate["baseline_sq8_gets"].as_u64()
            && report["baseline_sq8_bytes_total"].as_u64()
                == aggregate["baseline_sq8_bytes"].as_u64()
            && report["measured_hit_subtotals"] == totals["hits"]
            && report["planned_target_gets_totals"] == totals["gets"]
            && report["planned_target_bytes_totals"] == totals["bytes"]
            && report["native_quality_io_totals"] == *totals
            && report["io_statistics"] == aggregate["statistics"]
            && report["quality_floor_met"] == json!(floor),
        "GET recomputed totals/percentiles/fixed floor",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn strict_config_and_budgets_leave_payloads_unopened() {
        let root = super::super::driver_tests::directory();
        let mut shortlist = super::super::driver_tests::config_fixture(&root);
        shortlist["shortlist_sizes"] = json!([256, 256, 256, 256]);
        let base = json!({"schema":CONFIG_SCHEMA,"shortlist":shortlist,"get_budgets":[30,38,46,64],
            "selector_control":{"path":root.join("unopened.json"),"bytes":384810,"sha256":HISTORICAL_CONTROL_SHA}});
        let (path, sha) = super::super::driver_tests::write_config(&root, &base);
        assert_eq!(
            read_config(&path, &sha).unwrap().0.get_budgets,
            [30, 38, 46, 64]
        );
        let mut missing = base.clone();
        missing.as_object_mut().unwrap().remove("get_budgets");
        let (path, sha) = super::super::driver_tests::write_config(&root, &missing);
        assert!(read_config(&path, &sha).is_err());
        for budgets in [
            [0, 30, 38, 46],
            [30, 30, 46, 64],
            [38, 30, 46, 64],
            [30, 38, 46, 257],
            [1, 2, 3, 4],
        ] {
            let mut bad = base.clone();
            bad["get_budgets"] = json!(budgets);
            let (path, sha) = super::super::driver_tests::write_config(&root, &bad);
            assert!(read_config(&path, &sha).is_err());
        }
        for (pointer, value) in [
            ("/schema", json!(super::super::COUNT_CURVE_SCHEMA)),
            ("/schema", json!("borsuk-sq8-shortlist-get-curve-config-v1")),
            ("/shortlist/shortlist_sizes", json!([256, 512, 768, 1024])),
            ("/selector_control/bytes", json!(0)),
            ("/selector_control/sha256", json!("0".repeat(64))),
        ] {
            let mut bad = base.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            let (path, sha) = super::super::driver_tests::write_config(&root, &bad);
            assert!(read_config(&path, &sha).is_err());
        }
        let mut explicit = base.clone();
        explicit["get_budgets"] = json!([1, 2, 30, 256]);
        let (path, sha) = super::super::driver_tests::write_config(&root, &explicit);
        assert!(read_config(&path, &sha).is_err());
        let mut unknown = base.clone();
        unknown["unexpected"] = json!(true);
        let (path, sha) = super::super::driver_tests::write_config(&root, &unknown);
        assert!(read_config(&path, &sha).is_err());
        let (path, _) = super::super::driver_tests::write_config(&root, &base);
        assert!(read_config(&path, &"0".repeat(64)).is_err());
        assert!(!root.join("unopened.json").exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn g30_equivalence_and_more_gets_change_admitted_rows() {
        let authority = super::super::planning_authority(32 * 256 + 1, 2);
        let scored = (0..128)
            .map(|p| (p * 64, (128 - p) as f64))
            .collect::<Vec<_>>();
        let limits = SelectedSq8Limits {
            max_gets: 30,
            max_bytes: 30 * 32 * 14,
            max_planner_bytes: 1 << 20,
        };
        let arms = plan_arms(&authority, &scored, &[30, 38, 46, 64], limits, 1 << 20).unwrap();
        let nominees = super::super::shortlist(authority.rows(), &scored, 256, 1 << 20).unwrap();
        let selected = super::super::budget_admit_shortlist_blocks(
            &authority,
            &scored,
            &nominees,
            limits,
            1 << 20,
        )
        .unwrap();
        let old = plan_selected_sq8_rows(&authority, &selected, limits).unwrap();
        assert_eq!(arms[0].selected, selected);
        assert_eq!(arms[0].ranges, old.ranges);
        assert_eq!(arms[0].bytes, old.planned_bytes);
        // The old GET cap spends the byte ceiling on bridges; more GETs admit more nominees.
        let small = SelectedSq8Limits {
            max_bytes: 60 * 32 * 14,
            ..limits
        };
        let arms = plan_arms(&authority, &scored, &[30, 38, 46, 64], small, 1 << 20).unwrap();
        assert!(arms[3].selected.len() > arms[0].selected.len());
        assert!(
            arms.iter()
                .all(|a| a.maximum == 256 && a.bytes <= small.max_bytes)
        );
        assert!(plan_arms(&authority, &scored, &[30, 38, 46, 64], small, 1).is_err());
    }

    #[test]
    fn partial_tail_exact_byte_cap_and_tied_gaps() {
        let a = super::super::planning_authority(129, 2);
        let scored = [(0, 3.0), (64, 2.0), (128, 1.0)];
        let limits = SelectedSq8Limits {
            max_gets: 1,
            max_bytes: 129 * 14,
            max_planner_bytes: 1 << 20,
        };
        let arms = plan_arms(&a, &scored, &[1, 2, 3, 4], limits, 1 << 20).unwrap();
        assert_eq!(arms[0].ranges, vec![0..129 * 14]);
        assert_eq!(arms[1].bytes, 97 * 14);
        assert_eq!(arms[1].ranges, vec![0..96 * 14, 128 * 14..129 * 14]);
        assert_eq!(arms[2].bytes, 65 * 14);
        let capped = SelectedSq8Limits {
            max_bytes: 65 * 14 - 1,
            ..limits
        };
        let arms = plan_arms(&a, &scored, &[1, 2, 3, 4], capped, 1 << 20).unwrap();
        assert!(arms.iter().all(|a| a.bytes <= capped.max_bytes));
        assert_eq!(arms[2].selected, vec![0, 64]);
    }

    #[test]
    fn bridge_removal_changes_winner_and_can_rescue_or_lose_truth() {
        use sha2::{Digest, Sha256};
        let root = super::super::driver_tests::directory();
        let mut body = Vec::new();
        for row in 0..96_i64 {
            body.extend_from_slice(&row.to_le_bytes());
            body.extend_from_slice(&1_f32.to_le_bytes());
            body.extend_from_slice(if (32..42).contains(&row) {
                &[2, 0]
            } else {
                &[1, 0]
            });
        }
        let path = root.join("sq8.bin");
        std::fs::write(&path, &body).unwrap();
        let digests = body
            .chunks(32 * 14)
            .flat_map(|b| Sha256::digest(b).to_vec())
            .collect::<Vec<_>>();
        let m=serde_json::to_vec(&json!({"schema":"borsuk-v115-sq8-page-authority-v2","generation":1,"rows":96,"dimensions":2,"page_rows":32,"object_sha256":format!("{:x}",Sha256::digest(&body)),"page_digest_sha256":format!("{:x}",Sha256::digest(&digests))})).unwrap();
        let a = PageAuthority::load(&m, &format!("{:x}", Sha256::digest(&m)), &digests).unwrap();
        let pin = crate::CompletedInput {
            path,
            bytes: body.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&body)),
        };
        let (_, mut snapshot) = crate::loss_binary(&pin, pin.bytes).unwrap();
        let mut arms = plan_arms(
            &a,
            &[(0, 2.), (64, 1.)],
            &[1, 2, 3, 4],
            SelectedSq8Limits {
                max_gets: 1,
                max_bytes: body.len(),
                max_planner_bytes: 1 << 20,
            },
            1 << 20,
        )
        .unwrap();
        for arm in &mut arms {
            arm.returned = Some(
                super::super::rank_local_cover(
                    &mut snapshot,
                    &a,
                    &arm.ranges,
                    96,
                    2,
                    &[1., 0.],
                    &[0., 0.],
                    &[1., 1.],
                    body.len(),
                )
                .unwrap(),
            );
        }
        assert_eq!(arms[0].returned.as_ref().unwrap()[0].id, 32);
        assert_eq!(arms[1].returned.as_ref().unwrap()[0].id, 0);
        let positions = (0..96).map(|r| (r as u64, r)).collect();
        for (truth, expected) in [
            ((32..42).collect::<Vec<u64>>(), [10, 0]),
            ((0..10).collect(), [0, 10]),
        ] {
            let baseline = truth
                .iter()
                .map(|&id| crate::Hit {
                    id,
                    score_bits: u64::from(1_f32.to_bits()),
                })
                .collect::<Vec<_>>();
            for index in 0..2 {
                let q = super::super::assess_arm(
                    &arms[index],
                    &truth,
                    &baseline,
                    &positions,
                    &[0, 2],
                    96,
                    2,
                )
                .unwrap()
                .unwrap();
                assert_eq!(q.hits10, expected[index]);
            }
        }
        std::fs::remove_dir_all(root).unwrap();
    }
    fn report_fixture() -> Value {
        let budgets = [30, 38, 46, 64];
        let queries=(0..1000).map(|ordinal| {
            let baseline=if ordinal<220 {(0..9).chain(11..12).collect::<Vec<_>>()}else{(0..10).collect()};
            let quality=json!({"hits10":9,"gained":0,"lost":if ordinal<220{0}else{1},
                "bridge_rescued_hits":0,"block_expansion_rescued_hits":8,"shortlist_truth_rows":1,
                "selected_block_truth_rows":10,"miss_buckets":[0,0,0,1]});
            let arms=budgets.iter().map(|g|json!({"size":256,"get_budget":g,"status":"MEASURED",
                "gets":1,"physical_bytes":33152,"selected_rows":1,"selected_blocks":1,"pre_cap_runs":1,
                "bridge_bytes":0,"quality":quality})).collect::<Vec<_>>();
            let returned=(0..9).chain(10..11).map(|row|json!({"id":row,"target_row":row,"score_bits":1_f32.to_bits()})).collect::<Vec<_>>();
            json!({"ordinal":ordinal,"baseline_hits10":if ordinal<220{9}else{10},"arms":arms,
                "baseline_sq8_selected_blocks":[0],
                "baseline_sq8_charges":{"submitted_gets":1,"verified_bytes":33152,"failed_gets":0},
                "returned_evidence":[returned,returned,returned,returned],
                "selection_evidence":[{"selected":[0],"ranges":[[0,33152]]},{"selected":[0],"ranges":[[0,33152]]},{"selected":[0],"ranges":[[0,33152]]},{"selected":[0],"ranges":[[0,33152]]}],
                "quality_evidence":{"baseline_ids":baseline,"truth":(0..10).map(|row|json!({"id":row,"target_row":row,"source_scored":true})).collect::<Vec<_>>()}})
        }).collect::<Vec<_>>();
        json!({"status":"MEASURED","complete":true,"query_count":1000,"queries":queries,
            "get_budgets":budgets,"shortlist_sizes":[256,256,256,256],"baseline_hits10":9780,
            "baseline_sq8_gets_total":1000,"baseline_sq8_bytes_total":33152000,
            "measured_hit_subtotals":[9000,9000,9000,9000],"admissible_queries":[1000,1000,1000,1000],
            "quality_floor_met":[false,false,false,false],"nonratcheting_reference_hits10":9780,"recall_floor_hits10":9760,
            "historical_control":{"path":"/unopened/control.json","bytes":384810,"sha256":HISTORICAL_CONTROL_SHA},
            "miss_bucket_order":["outside_source_and_cover","source_to_shortlist_cover_loss","shortlisted_covered_ranking_loss","unshortlisted_covered_ranking_loss"],
            "baseline_sq8_get_budget":32,"ephemeral_sq8_block_rows":32,
            "historical_returned_ranks_checked_before_truth":true,"historical_count_curve_older_block_control_rank_parity":false,
            "baseline_exact_plan_trace_charges_ids_scorebits":true,"truth_file_opened_after_all_baseline_controls":true,
            "cold_latency_claim":false,"qps_claim":false,"vendor_win_claim":false,"qualified":false,"physical_s3_or_billed_bytes":false})
    }
    fn historical_fixture() -> Value {
        let mut old = report_fixture();
        old["shortlist_sizes"] = json!([256, 512, 768, 1024]);
        old["candidate_sq8_get_budget"] = json!(30);
        old.as_object_mut()
            .unwrap()
            .remove("baseline_sq8_gets_total");
        old.as_object_mut()
            .unwrap()
            .remove("baseline_sq8_bytes_total");
        for q in old["queries"].as_array_mut().unwrap() {
            q.as_object_mut()
                .unwrap()
                .remove("baseline_sq8_selected_blocks");
            q.as_object_mut().unwrap().remove("baseline_sq8_charges");
            for (a, size) in q["arms"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .zip([256, 512, 768, 1024])
            {
                a["size"] = json!(size);
                a.as_object_mut().unwrap().remove("get_budget");
            }
        }
        super::super::count_curve::finalize_report(&mut old).unwrap();
        old
    }
    fn bound_report_fixture(root: &std::path::Path) -> Value {
        let body = serde_json::to_vec(&historical_fixture()).unwrap();
        let path = root.join("control.json");
        std::fs::write(&path, &body).unwrap();
        let mut report = report_fixture();
        report["historical_control"] =
            json!({"path":path,"bytes":body.len(),"sha256":format!("{:x}",Sha256::digest(&body))});
        report
    }
    fn finalize_fixture(report: &mut Value) -> Result<()> {
        let sha = report["historical_control"]["sha256"]
            .as_str()
            .ok_or("fixture control SHA")?
            .to_owned();
        let pin = serde_json::from_value(report["historical_control"].clone())?;
        let comparison = Control::admit(pin)?.verify(
            report["queries"].as_array().ok_or("fixture queries")?,
            &budgets(report)?,
        )?;
        finalize_report_with_control_sha(report, comparison, &sha)
    }
    fn validate_fixture(report: &Value) -> Result<()> {
        let sha = report["historical_control"]["sha256"]
            .as_str()
            .ok_or("fixture control SHA")?;
        validate_report_with_control_sha(report, sha)
    }
    #[test]
    fn baseline_costs_reauthenticate_full_native_file_and_reject_coherent_tampering() {
        let root = super::super::driver_tests::directory();
        let rows = crate::tests::paired_scale_fixture(16);
        let (config_path, _) = crate::tests::paired_scale_config(&root, [rows.clone(), rows]);
        let config: Value = serde_json::from_slice(&std::fs::read(config_path).unwrap()).unwrap();
        let completed: crate::CompletedConfig =
            serde_json::from_value(config["arms"][0].clone()).unwrap();
        let native = crate::read_run_with(
            &completed.input.path,
            &completed.input.sha256,
            Some(&completed),
        )
        .unwrap();
        let mut total = crate::Charge::default();
        let queries = native
            .samples
            .iter()
            .enumerate()
            .map(|(ordinal, query)| {
                total.add(query.charges.sq8).unwrap();
                json!({"ordinal":ordinal,"baseline_sq8_charges":query.charges.sq8,"baseline_sq8_selected_blocks":[0]})
            })
            .collect::<Vec<_>>();
        let report = json!({"baseline_completed":completed,"queries":queries,
            "baseline_sq8_gets_total":total.submitted_gets,
            "baseline_sq8_bytes_total":total.verified_bytes});
        assert!(validate_completed_baseline_costs(&report).is_ok());
        for (field, sum) in [
            ("submitted_gets", "baseline_sq8_gets_total"),
            ("verified_bytes", "baseline_sq8_bytes_total"),
        ] {
            let mut bad = report.clone();
            bad["queries"][999]["baseline_sq8_charges"][field] = json!(
                bad["queries"][999]["baseline_sq8_charges"][field]
                    .as_u64()
                    .unwrap()
                    + 1
            );
            bad[sum] = json!(bad[sum].as_u64().unwrap() + 1);
            bad["queries"][999]["baseline_sq8_selected_blocks"] = json!([0, 1024]);
            assert!(validate_completed_baseline_costs(&bad).is_err());
        }
        let mut wrong_selection = report.clone();
        wrong_selection["queries"][999]["baseline_sq8_selected_blocks"] = json!([1024]);
        assert!(validate_completed_baseline_costs(&wrong_selection).is_err());
        let mut wrong_header = report.clone();
        wrong_header["baseline_completed"]["expected_identity"]["config_sha256"] =
            json!("0".repeat(64));
        assert!(validate_completed_baseline_costs(&wrong_header).is_err());
        let mut wrong_pin = report.clone();
        wrong_pin["baseline_completed"]["input"]["sha256"] = json!("0".repeat(64));
        assert!(validate_completed_baseline_costs(&wrong_pin).is_err());
        let path = std::path::PathBuf::from(
            report["baseline_completed"]["input"]["path"]
                .as_str()
                .unwrap(),
        );
        let mut body = std::fs::read(&path).unwrap();
        body.push(b'\n');
        std::fs::write(&path, body).unwrap();
        assert!(validate_completed_baseline_costs(&report).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn baseline_cost_totals_are_recomputed_and_bound_to_plan() {
        let root = super::super::driver_tests::directory();
        let mut report = bound_report_fixture(&root);
        finalize_fixture(&mut report).unwrap();
        assert!(validate_fixture(&report).is_ok());
        for (total, charge) in [
            ("baseline_sq8_gets_total", "submitted_gets"),
            ("baseline_sq8_bytes_total", "verified_bytes"),
        ] {
            let mut bad = report.clone();
            bad[total] = json!(report[total].as_u64().unwrap() + 1);
            assert!(validate_fixture(&bad).is_err(), "accepted changed {total}");
            let mut bad = report.clone();
            bad["queries"][999]["baseline_sq8_charges"][charge] = json!(
                report["queries"][999]["baseline_sq8_charges"][charge]
                    .as_u64()
                    .unwrap()
                    + 1
            );
            assert!(
                validate_fixture(&bad).is_err(),
                "accepted query999 {charge}"
            );
            bad[total] = json!(report[total].as_u64().unwrap() + 1);
            assert!(
                validate_fixture(&bad).is_err(),
                "accepted coherent query999 {charge}/{total}"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn baseline_cost_evidence_requires_strict_shapes_and_unsigned_charges() {
        let root = super::super::driver_tests::directory();
        let mut report = bound_report_fixture(&root);
        finalize_fixture(&mut report).unwrap();
        for (pointer, value) in [
            ("/baseline_sq8_gets_total", Value::Null),
            ("/baseline_sq8_gets_total", json!(-1)),
            ("/baseline_sq8_gets_total", json!(1000.0)),
            ("/baseline_sq8_bytes_total", json!("33152000")),
            ("/baseline_sq8_bytes_total", json!(u64::MAX)),
            ("/queries/999/baseline_sq8_charges", Value::Null),
            (
                "/queries/999/baseline_sq8_charges",
                json!({"submitted_gets":1,"verified_bytes":33152}),
            ),
            (
                "/queries/999/baseline_sq8_charges",
                json!({"submitted_gets":1,"verified_bytes":33152,"failed_gets":0,"extra":0}),
            ),
            (
                "/queries/999/baseline_sq8_charges/submitted_gets",
                json!(-1),
            ),
            (
                "/queries/999/baseline_sq8_charges/submitted_gets",
                json!(1.0),
            ),
            (
                "/queries/999/baseline_sq8_charges/verified_bytes",
                json!("33152"),
            ),
            (
                "/queries/999/baseline_sq8_charges/verified_bytes",
                json!(u64::MAX),
            ),
            ("/queries/999/baseline_sq8_charges/failed_gets", json!(1)),
            ("/queries/999/baseline_sq8_selected_blocks", Value::Null),
            ("/queries/999/baseline_sq8_selected_blocks", json!({})),
            ("/queries/999/baseline_sq8_selected_blocks", json!([])),
            ("/queries/999/baseline_sq8_selected_blocks", json!([0, 0])),
            ("/queries/999/baseline_sq8_selected_blocks", json!([2, 0])),
            ("/queries/999/baseline_sq8_selected_blocks", json!([31250])),
            ("/queries/999/baseline_sq8_selected_blocks", json!([-1])),
            (
                "/queries/999/baseline_sq8_selected_blocks",
                json!([u64::MAX]),
            ),
            (
                "/queries/999/baseline_sq8_selected_blocks",
                json!((0..257).collect::<Vec<_>>()),
            ),
        ] {
            let mut bad = report.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(validate_fixture(&bad).is_err(), "accepted {pointer}");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn baseline_cost_totals_accept_independently_derived_query999_cover() {
        let root = super::super::driver_tests::directory();
        let mut report = bound_report_fixture(&root);
        report["queries"][999]["baseline_sq8_selected_blocks"] = json!([0, 2]);
        report["queries"][999]["baseline_sq8_charges"]["submitted_gets"] = json!(2);
        report["queries"][999]["baseline_sq8_charges"]["verified_bytes"] = json!(66304);
        report["baseline_sq8_gets_total"] = json!(1001);
        report["baseline_sq8_bytes_total"] = json!(33185152);
        finalize_fixture(&mut report).unwrap();
        assert!(validate_fixture(&report).is_ok());
        std::fs::remove_dir_all(root).unwrap();
    }

    fn malformed_rankings(returned: &Value) -> Vec<(&'static str, Value)> {
        let mut cases = vec![
            ("null", Value::Null),
            ("object", json!({})),
            ("empty", json!([])),
        ];
        for (pointer, value) in [
            (
                "/9/score_bits",
                json!(u64::from(1_f32.to_bits()) + (1_u64 << 32)),
            ),
            ("/9/score_bits", json!(f32::NAN.to_bits())),
            ("/9/score_bits", json!(f32::INFINITY.to_bits())),
            ("/9/score_bits", json!(-1)),
            ("/9/score_bits", Value::Null),
            ("/9/id", json!(1_000_000)),
            ("/9/id", json!(-1)),
            ("/9/id", Value::Null),
            ("/9/target_row", json!(1_000_000)),
            ("/9/target_row", json!(u64::MAX)),
            ("/9/target_row", Value::Null),
            ("/9/target_row", json!(8)),
            ("/9/score_bits", json!(0)),
        ] {
            let mut bad = returned.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            cases.push((pointer, bad));
        }
        let mut duplicate = returned.clone();
        duplicate[9]["id"] = json!(8);
        duplicate[9]["score_bits"] = json!(2_f32.to_bits());
        cases.push(("duplicate ID with increasing score", duplicate));
        let mut missing = returned.clone();
        missing.as_array_mut().unwrap().pop();
        cases.push(("missing nontruth result", missing));
        let mut extra = returned.clone();
        extra
            .as_array_mut()
            .unwrap()
            .push(json!({"id":11,"target_row":11,"score_bits":1_f32.to_bits()}));
        cases.push(("extra nontruth result", extra));
        let mut absent = returned.clone();
        absent[9].as_object_mut().unwrap().remove("score_bits");
        cases.push(("missing score bits", absent));
        let mut unknown = returned.clone();
        unknown[9]["extra"] = json!(true);
        cases.push(("unknown rank field", unknown));
        cases
    }
    #[test]
    fn quality_rejects_malformed_tail_ranks_and_resealed_reports() {
        let root = super::super::driver_tests::directory();
        let mut report = bound_report_fixture(&root);
        finalize_fixture(&mut report).unwrap();
        assert!(validate_quality(&report["queries"][999], 1, &[0], &[0..33152]).is_ok());
        for (label, returned) in malformed_rankings(&report["queries"][999]["returned_evidence"][1])
        {
            let mut bad = report.clone();
            bad["queries"][999]["returned_evidence"][1] = returned;
            assert!(
                validate_quality(&bad["queries"][999], 1, &[0], &[0..33152]).is_err(),
                "accepted quality {label}"
            );
            assert!(
                finalize_fixture(&mut bad).is_err(),
                "accepted resealed {label}"
            );
        }
        let mut outside_cover = report["queries"][999].clone();
        outside_cover["returned_evidence"][1][9]["target_row"] = json!(32);
        assert!(validate_quality(&outside_cover, 1, &[0], &[0..33152]).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn report_preserves_below_floor_and_rejects_resealed_budget_charge_quality_errors() {
        let root = super::super::driver_tests::directory();
        let mut report = bound_report_fixture(&root);
        finalize_fixture(&mut report).unwrap();
        assert!(validate_fixture(&report).is_ok());
        assert_eq!(report["status"], "MEASURED");
        assert_eq!(report["historical_returned_rank_parity"], true);
        assert_eq!(report["automatic_production_gate"], false);
        assert_eq!(report["io_statistics"][0]["bytes"]["p99"], 33152);
        assert_eq!(
            report["native_quality_io_totals"]["selected_rows"],
            json!([1000, 1000, 1000, 1000])
        );
        assert_eq!(
            report["native_quality_io_totals"]["block_expansion_rescued_hits"],
            json!([8000, 8000, 8000, 8000])
        );
        let mut missing_proof = report_fixture();
        assert!(finalize_fixture(&mut missing_proof).is_err());
        assert_eq!(
            report["native_quality_io_totals"]["lost"],
            json!([780, 780, 780, 780])
        );
        for (pointer, value) in [
            ("/get_budgets", json!([30, 30, 46, 64])),
            ("/historical_control/sha256", json!("0".repeat(64))),
            ("/queries/999/arms/1/get_budget", json!(30)),
            ("/queries/999/arms/1/gets", json!(2)),
            ("/queries/999/arms/1/physical_bytes", json!(66304)),
            ("/queries/999/arms/1/bridge_bytes", json!(33152)),
            ("/queries/999/selection_evidence/1/selected", json!([32])),
            (
                "/queries/999/selection_evidence/1/ranges",
                json!([[0, 66304]]),
            ),
            (
                "/queries/999/arms/1/quality/miss_buckets",
                json!([1, 0, 0, 0]),
            ),
            ("/queries/999/returned_evidence/1/0/id", json!(9)),
            (
                "/queries/999/returned_evidence/1/0/score_bits",
                json!(f32::NAN.to_bits()),
            ),
            ("/queries/999/ordinal", json!(998)),
        ] {
            let mut bad = report.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(
                finalize_fixture(&mut bad).is_err(),
                "accepted resealed {pointer}"
            );
        }
        let mut bad = report.clone();
        bad["io_statistics"][0]["bytes"]["p95"] = json!(1);
        assert!(validate_fixture(&bad).is_err());
        let mut bad = report;
        bad["measured_hit_subtotals"][0] = json!(9001);
        assert!(validate_fixture(&bad).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn report_requires_authenticated_g30_ranks_and_shared_experiment_evidence() {
        let root = super::super::driver_tests::directory();
        let mut report = bound_report_fixture(&root);
        finalize_fixture(&mut report).unwrap();
        assert!(validate_fixture(&report).is_ok());
        assert!(
            validate_report(&report).is_err(),
            "synthetic SHA cannot pass the public boundary"
        );
        let mut descriptor_only = report.clone();
        descriptor_only["historical_control"]["sha256"] = json!(HISTORICAL_CONTROL_SHA);
        descriptor_only["historical_control"]["path"] = json!(root.join("unopened-control.json"));
        assert!(validate_report(&descriptor_only).is_err());
        let mut changed_g30 = report.clone();
        for arm in 0..4 {
            changed_g30["queries"][999]["returned_evidence"][arm][9]["score_bits"] =
                json!(2_f32.to_bits());
        }
        assert!(validate_fixture(&changed_g30).is_err());
        finalize_fixture(&mut changed_g30).unwrap();
        assert!(validate_fixture(&changed_g30).is_ok());
        assert_eq!(changed_g30["historical_returned_rank_parity"], false);
        assert_eq!(
            changed_g30["historical_comparison"]["rank_mismatched_queries"],
            1
        );
        assert_eq!(
            changed_g30["historical_comparison"]["score_bits_mismatched_positions"],
            1
        );
        assert_eq!(
            changed_g30["historical_comparison"]["mismatch_evidence"][0]["ordinal"],
            999
        );
        for arm in 0..4 {
            changed_g30["queries"][999]["returned_evidence"][arm][9]["id"] = json!(11);
            changed_g30["queries"][999]["returned_evidence"][arm][9]["target_row"] = json!(11);
        }
        finalize_fixture(&mut changed_g30).unwrap();
        assert!(validate_fixture(&changed_g30).is_ok());
        assert_eq!(
            changed_g30["historical_comparison"]["id_mismatched_positions"],
            1
        );
        assert_eq!(
            changed_g30["historical_comparison"]["target_row_mismatched_positions"],
            1
        );
        assert_eq!(
            changed_g30["historical_comparison"]["rank_mismatched_positions"],
            1
        );
        for (pointer, value) in [
            ("/queries/999/selection_evidence/1/selected", json!([1])),
            ("/queries/999/returned_evidence/1/9/target_row", json!(11)),
            (
                "/queries/999/returned_evidence/1/9/score_bits",
                json!(2_f32.to_bits()),
            ),
            ("/queries/999/returned_evidence/1/9/id", json!(11)),
        ] {
            let mut bad = report.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(
                finalize_fixture(&mut bad).is_err(),
                "accepted shared contradiction {pointer}"
            );
        }
        let mut bad = report.clone();
        bad["queries"][999]["returned_evidence"][1][9] =
            json!({"id":11,"target_row":11,"score_bits":1_f32.to_bits()});
        assert!(
            finalize_fixture(&mut bad).is_err(),
            "equal covers require equal rankings"
        );
        // Different covers still share the physical mapping and score for a returned ID.
        let mut query = report["queries"][999].clone();
        query["selection_evidence"][1] = json!({"selected":[0,32],"ranges":[[0,66304]]});
        assert!(validate_shared_evidence(&query).is_ok());
        for (key, value) in [
            ("target_row", json!(11)),
            ("score_bits", json!(2_f32.to_bits())),
            ("id", json!(11)),
        ] {
            let mut bad = query.clone();
            bad["returned_evidence"][1][9][key] = value;
            assert!(
                validate_shared_evidence(&bad).is_err(),
                "different-cover contradiction {key}"
            );
        }
        for arm in 0..4 {
            query["selection_evidence"][arm]["selected"] = json!(
                (arm * 65..(arm + 1) * 65)
                    .map(|r| r * 32)
                    .collect::<Vec<_>>()
            );
        }
        assert!(
            validate_shared_evidence(&query).is_err(),
            "nominee union exceeds 256"
        );
        std::fs::write(root.join("control.json"), b"changed").unwrap();
        assert!(
            validate_fixture(&report).is_err(),
            "control bytes must authenticate"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn g30_history_distinguishes_equality_difference_and_corrupt_control() {
        let now = report_fixture();
        let old = historical_fixture();
        let queries = now["queries"].as_array().unwrap();
        let same = verify_control_queries(&old, queries, &[30, 38, 46, 64]).unwrap();
        assert_eq!(same["ordered_rank_parity"], true);
        assert_eq!(same["arm_invariant_parity"], true);
        assert_eq!(same["baseline_relative_parity"], true);
        let mut changed = queries.clone();
        for q in &mut changed[990..] {
            q["returned_evidence"][0][9]["score_bits"] = json!(2_f32.to_bits());
        }
        let different = verify_control_queries(&old, &changed, &[30, 38, 46, 64]).unwrap();
        assert_eq!(different["ordered_rank_parity"], false);
        assert_eq!(different["rank_mismatched_queries"], 10);
        assert_eq!(different["rank_mismatched_positions"], 10);
        assert_eq!(different["mismatch_evidence"].as_array().unwrap().len(), 8);
        assert_eq!(different["arm_invariant_parity"], true);
        let mut corrupt = old.clone();
        corrupt["queries"][999]["arms"][0]["quality"]["hits10"] = json!(10);
        assert!(verify_control_queries(&corrupt, queries, &[30, 38, 46, 64]).is_err());
        let mut corrupt = old;
        corrupt["queries"][999]
            .as_object_mut()
            .unwrap()
            .remove("returned_evidence");
        assert!(verify_control_queries(&corrupt, queries, &[30, 38, 46, 64]).is_err());
    }

    #[test]
    fn changed_geometry_is_descriptive_and_report_comparison_cannot_be_resealed() {
        let root = super::super::driver_tests::directory();
        let mut report = bound_report_fixture(&root);
        for a in 0..4 {
            let q = &mut report["queries"][999];
            q["selection_evidence"][a] = json!({"selected":[0,32],"ranges":[[0,66304]]});
            q["arms"][a]["selected_rows"] = json!(2);
            q["arms"][a]["selected_blocks"] = json!(2);
            q["arms"][a]["physical_bytes"] = json!(66304);
        }
        finalize_fixture(&mut report).unwrap();
        assert!(validate_fixture(&report).is_ok());
        assert_eq!(report["closed_selector_control_parity"], false);
        assert_eq!(
            report["historical_comparison"]["arm_invariant_mismatched_queries"],
            1
        );
        assert_eq!(report["historical_returned_rank_parity"], true);
        for (pointer, value) in [
            ("/closed_selector_control_parity", json!(true)),
            (
                "/historical_comparison/arm_invariant_mismatched_queries",
                json!(0),
            ),
            ("/historical_comparison/mismatch_evidence", json!([])),
            ("/historical_comparison/rank_scope", json!("SUMMARY_ONLY")),
            ("/historical_comparison_is_descriptive", json!(false)),
        ] {
            let mut bad = report.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(validate_fixture(&bad).is_err(), "accepted {pointer}");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn current_baseline_is_recomputed_and_gain_loss_is_conserved() {
        let root = super::super::driver_tests::directory();
        let mut report = bound_report_fixture(&root);
        report["queries"][999]["quality_evidence"]["baseline_ids"] =
            json!((0..9).chain(11..12).collect::<Vec<_>>());
        report["queries"][999]["baseline_hits10"] = json!(9);
        report["baseline_hits10"] = json!(9779);
        for arm in report["queries"][999]["arms"].as_array_mut().unwrap() {
            arm["quality"]["lost"] = json!(0);
        }
        finalize_fixture(&mut report).unwrap();
        assert!(validate_fixture(&report).is_ok());
        assert_eq!(
            report["native_quality_io_totals"]["lost"],
            json!([779, 779, 779, 779])
        );
        assert_eq!(report["historical_comparison"]["ordered_rank_parity"], true);
        assert_eq!(
            report["historical_comparison"]["arm_invariant_parity"],
            true
        );
        assert_eq!(
            report["historical_comparison"]["baseline_relative_parity"],
            false
        );
        assert_eq!(
            report["historical_comparison"]["baseline_relative_mismatched_queries"],
            1
        );
        report["queries"][999]["arms"][0]["quality"]["lost"] = json!(1);
        assert!(finalize_fixture(&mut report).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retained_control_authenticates_complete_ranks_before_truth_and_rechecks_snapshot() {
        let root = super::super::driver_tests::directory();
        let old = historical_fixture();
        assert!(historical_totals(&old).is_ok());
        assert_eq!(
            old["historical_returned_rank_parity"], false,
            "this flag refers to the count curve's older block control"
        );
        let body = serde_json::to_vec(&old).unwrap();
        let path = root.join("control.json");
        std::fs::write(&path, &body).unwrap();
        let control = Control::admit(crate::CompletedInput {
            path: path.clone(),
            bytes: body.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&body)),
        })
        .unwrap();
        let evidence = old["queries"][0]["returned_evidence"].clone();
        control
            .verify_ranking(0, &evidence, &[30, 38, 46, 64])
            .unwrap();
        let mut bad = evidence.clone();
        bad[0][0]["score_bits"] = json!(0);
        assert!(!control.verify_ranking(0, &bad, &[30, 38, 46, 64]).unwrap());
        bad[0][0]["score_bits"] = json!(f32::NAN.to_bits());
        assert!(control.verify_ranking(0, &bad, &[30, 38, 46, 64]).is_err());
        for missing in [Value::Null, json!([]), json!({})] {
            let mut bad = old.clone();
            bad["queries"][999]["returned_evidence"] = missing;
            assert!(historical_totals(&bad).is_err());
        }
        for (label, returned) in malformed_rankings(&old["queries"][999]["returned_evidence"][0]) {
            let mut bad = old.clone();
            bad["queries"][999]["returned_evidence"][0] = returned;
            assert!(
                historical_totals(&bad).is_err(),
                "accepted historical {label}"
            );
        }
        let mut no_ranks = old;
        no_ranks["queries"][999]
            .as_object_mut()
            .unwrap()
            .remove("returned_evidence");
        assert!(historical_totals(&no_ranks).is_err());
        std::fs::write(&path, b"changed").unwrap();
        assert!(control.recheck().is_err());
        assert!(!root.join("unopened-truth.bin").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn admission_smoke_and_measurement_schemas_cannot_be_substituted() {
        let mut admission = json!({"schema":ADMISSION_SCHEMA,"status":"READY_ACTUAL_INPUT","complete":true,
            "query_count":1000,"get_budgets":[30,38,46,64],"shortlist_sizes":[256,256,256,256],
            "candidate_queries_executed":0,"truth_file_opened_for_input_validation_only":true,"truth_ids_validated":10000,
            "all_queries_and_truth_rows_validated":true,"scientific_qualified":false,"qualified":false,
            "whole_source_and_sq8_authenticated":true,"generation_opened_through_runtime_validator":true,
            "all_queries_validated":true,"closed_control_bytes_admitted":true});
        assert!(validate_gate(&admission, ADMISSION_SCHEMA).is_ok());
        assert!(validate_gate(&admission, SMOKE_SCHEMA).is_err());
        admission["truth_file_opened_for_input_validation_only"] = json!(false);
        assert!(validate_gate(&admission, ADMISSION_SCHEMA).is_err());
        let mut smoke = json!({"schema":SMOKE_SCHEMA,"status":"STAGING_SMOKE_CLOSED","complete":true,
            "get_budgets":[30,38,46,64],"shortlist_sizes":[256,256,256,256],"candidate_queries_executed":1,
            "arms_executed":4,"quality_evaluated":false,"truth_file_opened":false,
            "baseline_prefix_exact_plan_trace_ids_scorebits":true,"source_capture_routing_scoring_unchanged":true,
            "closed_control_bytes_admitted":true,"original_physical_layout":true,"g30_historical_returned_rank_prefix_parity":true,
            "historical_prefix_queries_compared":1,"historical_rank_mismatched_prefix_queries":0,
            "historical_prefix_scope":"QUERY0_CURRENT_G30_VS_PINNED_HISTORICAL_COUNT_ARM0",
            "historical_comparison_is_descriptive":true});
        assert!(validate_gate(&smoke, SMOKE_SCHEMA).is_ok());
        smoke["g30_historical_returned_rank_prefix_parity"] = json!(false);
        assert!(validate_gate(&smoke, SMOKE_SCHEMA).is_err());
        smoke["historical_rank_mismatched_prefix_queries"] = json!(1);
        assert!(validate_gate(&smoke, SMOKE_SCHEMA).is_ok());
        assert!(validate_gate(&smoke, ADMISSION_SCHEMA).is_err());
        smoke["quality_evaluated"] = json!(true);
        assert!(validate_gate(&smoke, SMOKE_SCHEMA).is_err());
        let root = super::super::driver_tests::directory();
        let codec = borsuk::rotated_two_bit::RotatedTwoBitCodec::new(&[0., 0.], 1).unwrap();
        let requests = [1_f32.to_le_bytes(), 0_f32.to_le_bytes()].concat();
        let truth = (0..10_u64).flat_map(u64::to_le_bytes).collect::<Vec<_>>();
        super::super::admit_payloads(&requests, &truth, 32, 2, 1, &codec).unwrap();
        assert!(
            super::super::admit_payloads(&requests, &truth[..truth.len() - 1], 32, 2, 1, &codec)
                .is_err()
        );
        let duplicate = vec![0_u8; 80];
        assert!(super::super::admit_payloads(&requests, &duplicate, 32, 2, 1, &codec).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn create_only_and_invalid_errors_preserve_existing_output() {
        let root = super::super::driver_tests::directory();
        let path = root.join("get.json");
        let mut report = bound_report_fixture(&root);
        finalize_fixture(&mut report).unwrap();
        assert_eq!(crate::report_cap(SCHEMA), 32 * 1024 * 1024);
        assert!(
            !crate::execute_report(&path, SCHEMA, || Ok(report)).unwrap(),
            "the public output boundary refuses a synthetic control SHA"
        );
        let before = std::fs::read(&path).unwrap();
        assert!(
            super::super::execute_get_curve(
                &root.join("absent.json"),
                &"0".repeat(64),
                &path,
                false,
                false
            )
            .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
        let invalid = root.join("invalid.json");
        assert!(
            !super::super::execute_get_curve(
                &root.join("absent.json"),
                &"0".repeat(64),
                &invalid,
                false,
                false
            )
            .unwrap()
        );
        let error: Value = serde_json::from_slice(&std::fs::read(&invalid).unwrap()).unwrap();
        assert_eq!(error["schema"], SCHEMA);
        assert_eq!(error["status"], "INVALID");
        let bytes = std::fs::read(&invalid).unwrap();
        assert!(
            super::super::execute_get_curve(
                &root.join("absent.json"),
                &"0".repeat(64),
                &invalid,
                false,
                false
            )
            .is_err()
        );
        assert_eq!(std::fs::read(&invalid).unwrap(), bytes);
        std::fs::remove_dir_all(root).unwrap();
    }
}

pub(super) fn validate_gate(report: &Value, schema: &str) -> Result<()> {
    budgets(report)?;
    crate::require(
        report["schema"] == schema
            && report["complete"] == true
            && report["shortlist_sizes"] == json!([256, 256, 256, 256])
            && report["closed_control_bytes_admitted"] == true,
        "GET gate schema/fixed shortlist/control",
    )?;
    if schema == ADMISSION_SCHEMA {
        crate::require(
            report["status"] == "READY_ACTUAL_INPUT"
                && report["query_count"] == 1000
                && report["candidate_queries_executed"] == 0
                && report["truth_ids_validated"] == 10000
                && report["truth_file_opened_for_input_validation_only"] == true
                && report["all_queries_and_truth_rows_validated"] == true
                && report["scientific_qualified"] == false
                && report["qualified"] == false
                && report["whole_source_and_sq8_authenticated"] == true
                && report["generation_opened_through_runtime_validator"] == true,
            "GET admission actual truth validation without quality measurement",
        )
    } else {
        crate::require(
            schema == SMOKE_SCHEMA
                && report["status"] == "STAGING_SMOKE_CLOSED"
                && report["candidate_queries_executed"] == 1
                && report["arms_executed"] == 4
                && report["quality_evaluated"] == false
                && report["truth_file_opened"] == false
                && report["g30_historical_returned_rank_prefix_parity"].is_boolean()
                && report["historical_prefix_queries_compared"] == 1
                && report["historical_rank_mismatched_prefix_queries"]
                    == usize::from(report["g30_historical_returned_rank_prefix_parity"] == false)
                && report["historical_prefix_scope"]
                    == "QUERY0_CURRENT_G30_VS_PINNED_HISTORICAL_COUNT_ARM0"
                && report["historical_comparison_is_descriptive"] == true
                && report["original_physical_layout"] == true
                && report["baseline_prefix_exact_plan_trace_ids_scorebits"] == true
                && report["source_capture_routing_scoring_unchanged"] == true,
            "GET staging smoke truth separation",
        )
    }
}
