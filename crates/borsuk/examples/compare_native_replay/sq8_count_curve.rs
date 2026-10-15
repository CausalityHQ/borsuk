//! Native SOURCE-count curve over the unchanged physical SQ8 layout.
#![allow(dead_code)]
use super::{ShortlistArm, ShortlistConfig};
use borsuk::sq8_page_authority::{PageAuthority, SelectedSq8Limits, plan_selected_sq8_rows};
type Result<T> = crate::Result<T>;
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    schema: String,
    pub(super) shortlist: ShortlistConfig,
    pub(super) selector_control: crate::CompletedInput,
}
pub(super) fn read_config(path: &std::path::Path, sha: &str) -> Result<(Config, u64)> {
    let (c, n): (Config, _) = crate::read_config(path, sha)?;
    crate::require(
        c.schema == "borsuk-sq8-shortlist-count-curve-config-v1",
        "count curve config schema",
    )?;
    validate_counts(&c.shortlist.shortlist_sizes)?;
    crate::header_pin(&c.selector_control, 1024 * 1024)?;
    Ok((c, n))
}
fn validate_counts(counts: &[usize; 4]) -> Result<()> {
    crate::require(
        counts.iter().all(|n| (32..=1024).contains(n))
            && counts.windows(2).all(|p| p[0] < p[1])
            && counts[3] == 1024,
        "count curve bounded increasing counts and1024 control",
    )
}
pub(super) fn plan_arms(
    a: &PageAuthority,
    scored: &[(usize, f64)],
    counts: &[usize; 4],
    limits: SelectedSq8Limits,
    cap: usize,
) -> Result<Vec<ShortlistArm>> {
    validate_counts(counts)?;
    // The caller funds retained arm outputs; cap bounds each primitive's workspace.
    counts
        .iter()
        .map(|&maximum| {
            let nominees = super::shortlist(a.rows(), scored, maximum, cap)?;
            let selected = super::budget_admit_shortlist_blocks(a, scored, &nominees, limits, cap)?;
            let plan = plan_selected_sq8_rows(a, &selected, limits)
                .map_err(|e| format!("count curve plan {e:?}"))?;
            Ok(ShortlistArm {
                maximum,
                selected,
                ranges: plan.ranges,
                bytes: plan.planned_bytes,
                returned: None,
            })
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    #[test]
    fn curve_config_accepts_lower_counts_without_opening_payloads() {
        let root = super::super::driver_tests::directory();
        let mut inner = super::super::driver_tests::config_fixture(&root);
        inner["shortlist_sizes"] = json!([256, 512, 768, 1024]);
        let base = json!({"schema":"borsuk-sq8-shortlist-count-curve-config-v1","shortlist":inner,
            "selector_control":{"path":root.join("unopened-control.json"),"bytes":384810,"sha256":"e92aa4948cda22386e8e3795324490822319cef18383dcc69be36cb74d68b620"}});
        let (path, sha) = super::super::driver_tests::write_config(&root, &base);
        let parsed = read_config(&path, &sha);
        assert!(
            parsed.is_ok(),
            "lower SOURCE counts must be admitted before payload open: {}",
            parsed.err().map(|e| e.to_string()).unwrap_or_default()
        );
        let mut another = base.clone();
        another["shortlist"]["shortlist_sizes"] = json!([32, 128, 512, 1024]);
        let (path, sha) = super::super::driver_tests::write_config(&root, &another);
        assert!(read_config(&path, &sha).is_ok());
        for counts in [
            [0, 512, 768, 1024],
            [31, 512, 768, 1024],
            [256, 256, 768, 1024],
            [512, 256, 768, 1024],
            [256, 512, 768, 1000],
            [256, 512, 1024, 2048],
        ] {
            let mut bad = base.clone();
            bad["shortlist"]["shortlist_sizes"] = json!(counts);
            let (path, sha) = super::super::driver_tests::write_config(&root, &bad);
            assert!(
                read_config(&path, &sha).is_err(),
                "invalid counts {counts:?}"
            );
        }
        for (pointer, value) in [
            ("/schema", json!("wrong")),
            ("/selector_control/bytes", json!(0)),
            ("/selector_control/bytes", json!(1048577)),
        ] {
            let mut bad = base.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            let (path, sha) = super::super::driver_tests::write_config(&root, &bad);
            assert!(read_config(&path, &sha).is_err(), "{pointer}");
        }
        let mut unknown = base.clone();
        unknown["unexpected"] = json!(true);
        let (path, sha) = super::super::driver_tests::write_config(&root, &unknown);
        assert!(read_config(&path, &sha).is_err());
        let (path, _) = super::super::driver_tests::write_config(&root, &base);
        assert!(read_config(&path, &"0".repeat(64)).is_err());
        assert!(!root.join("unopened-control.json").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn curve_plans_all_source_prefixes_and_preserves_control() {
        let sidecar = vec![0_u8; 4096 / 32 * 32];
        let body=serde_json::to_vec(&json!({"schema":"borsuk-v115-sq8-page-authority-v2","generation":1,"rows":4096,"dimensions":2,"page_rows":32,"object_sha256":"0".repeat(64),"page_digest_sha256":format!("{:x}",Sha256::digest(&sidecar))})).unwrap();
        let a =
            PageAuthority::load(&body, &format!("{:x}", Sha256::digest(&body)), &sidecar).unwrap();
        let scored = (0..1024)
            .map(|row| (row, (1024 - row) as f64))
            .collect::<Vec<_>>();
        let limits = SelectedSq8Limits {
            max_gets: 30,
            max_bytes: 1 << 20,
            max_planner_bytes: 1 << 20,
        };
        let arms = plan_arms(&a, &scored, &[256, 512, 768, 1024], limits, 8 << 20).unwrap();
        assert_eq!(arms.len(), 4, "all four SOURCE prefixes must be planned");
        for (arm, (count, bytes)) in
            arms.iter()
                .zip([(256, 3584), (512, 7168), (768, 10752), (1024, 14336)])
        {
            assert_eq!(arm.maximum, count);
            assert_eq!(arm.selected.len(), count);
            assert_eq!(arm.selected.first(), Some(&0));
            assert_eq!(arm.selected.last(), Some(&(count - 1)));
            assert!(arm.selected.windows(2).all(|p| p[1] == p[0] + 1));
            assert_eq!(arm.bytes, bytes);
            assert_eq!(arm.ranges, vec![0..bytes]);
        }
        let nominees = (0..1024).collect::<Vec<_>>();
        let old =
            super::super::budget_admit_shortlist_blocks(&a, &scored, &nominees, limits, 8 << 20)
                .unwrap();
        assert_eq!(arms[3].selected, old, "unchanged1024 control primitive");
    }
}

pub(super) const SCHEMA: &str = "borsuk-sq8-shortlist-count-curve-v1";
pub(super) const ADMISSION_SCHEMA: &str = "borsuk-sq8-shortlist-count-curve-admission-v1";
pub(super) const SMOKE_SCHEMA: &str = "borsuk-sq8-shortlist-count-curve-smoke-v1";
pub(super) const EXTRA_PAYLOAD: usize = 128 * 1024 * 1024;
pub(super) struct Control {
    pin: crate::CompletedInput,
    snapshot: crate::HeaderSnapshot,
}
impl Control {
    pub(super) fn admit(pin: crate::CompletedInput) -> Result<Self> {
        crate::header_pin(&pin, 1024 * 1024)?;
        let (_, snapshot) = crate::loss_binary(&pin, pin.bytes)?;
        Ok(Self { pin, snapshot })
    }
    pub(super) fn recheck(&self) -> Result<()> {
        self.snapshot.recheck()
    }
    pub(super) fn verify(&self, queries: &[serde_json::Value]) -> Result<()> {
        self.recheck()?;
        let (bytes, snapshot) = crate::loss_binary(&self.pin, self.pin.bytes)?;
        let old: serde_json::Value = serde_json::from_slice(&bytes)?;
        super::validate_block_admission_report(&old)?;
        let before = old["queries"]
            .as_array()
            .ok_or("count control query array")?;
        crate::require(
            queries.len() == 1000 && before.len() == 1000,
            "count control population",
        )?;
        for (now, old) in queries.iter().zip(before) {
            crate::require(
                now["ordinal"] == old["ordinal"]
                    && now["baseline_hits10"] == old["baseline_hits10"]
                    && now["arms"][3] == old["arms"][0],
                "count1024 exact closed-selector parity",
            )?;
        }
        snapshot.recheck()?;
        self.recheck()
    }
}
pub(super) fn returned_evidence(arms: &[ShortlistArm]) -> Result<serde_json::Value> {
    let points=arms.iter().map(|a|{
        Ok(a.returned.as_ref().ok_or("count curve missing ranking")?.iter()
            .map(|p|serde_json::json!({"id":p.id,"target_row":p.ordinal,"score_bits":p.score.to_bits()})).collect::<Vec<_>>())
    }).collect::<Result<Vec<_>>>()?;
    Ok(serde_json::json!(points))
}
fn number(v: &serde_json::Value, key: &str) -> Result<u64> {
    v[key]
        .as_u64()
        .ok_or_else(|| format!("count report {key}").into())
}
fn add(total: &mut u64, value: u64) -> Result<()> {
    *total = total
        .checked_add(value)
        .ok_or("count report total overflow")?;
    Ok(())
}
fn aggregate(report: &serde_json::Value) -> Result<serde_json::Value> {
    use serde_json::json;
    let counts: [usize; 4] = serde_json::from_value(report["shortlist_sizes"].clone())?;
    validate_counts(&counts)?;
    let queries = report["queries"].as_array().ok_or("count curve queries")?;
    crate::require(
        queries.len() == 1000,
        "count curve complete query population",
    )?;
    let (mut hits, mut gets, mut bytes, mut gained, mut lost, mut bridges) = (
        [0_u64; 4], [0_u64; 4], [0_u64; 4], [0_u64; 4], [0_u64; 4], [0_u64; 4],
    );
    let mut misses = [[0_u64; 4]; 4];
    let mut baseline = 0;
    for (ordinal, q) in queries.iter().enumerate() {
        crate::require(
            number(q, "ordinal")? == ordinal as u64,
            "count curve query ordinal",
        )?;
        let base = number(q, "baseline_hits10")?;
        crate::require(base <= 10, "count baseline hits")?;
        add(&mut baseline, base)?;
        let arms = q["arms"].as_array().ok_or("count curve arm array")?;
        let points = q["returned_evidence"]
            .as_array()
            .ok_or("count curve returned evidence")?;
        crate::require(
            arms.len() == 4 && points.len() == 4,
            "count curve four arms",
        )?;
        for a in 0..4 {
            let arm = &arms[a];
            let quality = &arm["quality"];
            let h = number(quality, "hits10")?;
            let g = number(arm, "gets")?;
            let b = number(arm, "physical_bytes")?;
            let gain = number(quality, "gained")?;
            let loss = number(quality, "lost")?;
            let miss: [u64; 4] = serde_json::from_value(quality["miss_buckets"].clone())?;
            crate::require(
                arm["status"] == "MEASURED"
                    && number(arm, "size")? == counts[a] as u64
                    && h <= 10
                    && gain <= 10
                    && loss <= 10
                    && miss.iter().all(|n| *n <= 10)
                    && h + miss.iter().sum::<u64>() == 10
                    && base + gain == h + loss
                    && g > 0
                    && g <= 30
                    && b > 0
                    && b <= 32 * 1024 * 1024,
                "count arm quality/cover conservation",
            )?;
            let rows = points[a].as_array().ok_or("count returned rows")?;
            crate::require(rows.len() == 10, "count returned k")?;
            let mut ids = std::collections::BTreeSet::new();
            let mut ordinals = std::collections::BTreeSet::new();
            for p in rows {
                let id = p["id"].as_i64().ok_or("count returned ID")?;
                let row = number(p, "target_row")?;
                let bits = number(p, "score_bits")?;
                crate::require(
                    (0..1_000_000).contains(&id)
                        && row < 1_000_000
                        && bits <= u32::MAX as u64
                        && f32::from_bits(bits as u32).is_finite()
                        && ids.insert(id)
                        && ordinals.insert(row),
                    "count returned identity/score",
                )?;
            }
            add(&mut hits[a], h)?;
            add(&mut gets[a], g)?;
            add(&mut bytes[a], b)?;
            add(&mut gained[a], gain)?;
            add(&mut lost[a], loss)?;
            add(&mut bridges[a], number(arm, "bridge_bytes")?)?;
            for j in 0..4 {
                add(&mut misses[a][j], miss[j])?;
            }
        }
    }
    crate::require(
        report["baseline_hits10"] == json!(baseline),
        "count baseline total",
    )?;
    Ok(
        json!({"hits":hits,"gets":gets,"bytes":bytes,"gained":gained,"lost":lost,"bridge_bytes":bridges,"miss_buckets":misses}),
    )
}
pub(super) fn finalize_report(report: &mut serde_json::Value) -> Result<()> {
    use serde_json::json;
    let totals = aggregate(report)?;
    report["schema"] = json!(SCHEMA);
    report["control_arm_index"] = json!(3);
    report["planned_target_gets_totals"] = totals["gets"].clone();
    report["planned_target_bytes_totals"] = totals["bytes"].clone();
    report["native_quality_io_totals"] = totals;
    report["closed_selector_control_parity"] = json!(true);
    report["original_physical_layout"] = json!(true);
    report["source_capture_routing_scoring_unchanged"] = json!(true);
    report["single_causal_source_count_change"] = json!(true);
    let eligible = (0..4)
        .map(|a| {
            report["measured_hit_subtotals"][a]
                .as_u64()
                .is_some_and(|h| h >= 9760)
        })
        .collect::<Vec<_>>();
    report["quality_advancement_eligible"] = json!(eligible);
    report
        .as_object_mut()
        .ok_or("count report object")?
        .remove("physical_bytes_totals");
    validate_report(report)
}
pub(super) fn validate_report(report: &serde_json::Value) -> Result<()> {
    use serde_json::json;
    crate::require(
        report["schema"] == SCHEMA
            && report["status"] == "MEASURED"
            && report["complete"] == true
            && report["query_count"] == 1000
            && report["admissible_queries"] == json!([1000, 1000, 1000, 1000])
            && report["control_arm_index"] == 3
            && report["closed_selector_control_parity"] == true
            && report["original_physical_layout"] == true
            && report["source_capture_routing_scoring_unchanged"] == true
            && report["single_causal_source_count_change"] == true
            && report["recall_floor_hits10"] == 9760
            && report["cold_latency_claim"] == false
            && report["vendor_win_claim"] == false,
        "count complete native report",
    )?;
    let totals = aggregate(report)?;
    crate::require(
        report["measured_hit_subtotals"] == totals["hits"]
            && report["planned_target_gets_totals"] == totals["gets"]
            && report["planned_target_bytes_totals"] == totals["bytes"]
            && report["native_quality_io_totals"] == totals,
        "count native totals parity",
    )?;
    let eligible = (0..4)
        .map(|a| totals["hits"][a].as_u64().is_some_and(|h| h >= 9760))
        .collect::<Vec<_>>();
    crate::require(
        report["quality_floor_met"] == json!(eligible)
            && report["quality_advancement_eligible"] == json!(eligible),
        "count fixed quality floor flags",
    )
}

#[cfg(test)]
mod report_tests {
    use super::*;
    use serde_json::{Value, json};

    fn report_fixture() -> Value {
        let counts = [256, 512, 768, 1024];
        let queries = (0..1000)
            .map(|ordinal| {
                let arms = counts
                    .iter()
                    .enumerate()
                    .map(|(a, count)| {
                        let hits = if a < 2 { 9 } else { 10 };
                        json!({"status":"MEASURED","size":count,"gets":1,
                    "physical_bytes":4096,"bridge_bytes":0,
                    "quality":{"hits10":hits,"gained":0,"lost":10-hits,
                        "miss_buckets":[10-hits,0,0,0]}})
                    })
                    .collect::<Vec<_>>();
                let returned = (0..4)
                    .map(|_| {
                        (0..10).map(|row| {
                json!({"id":row,"target_row":row,"score_bits":1.0_f32.to_bits()})
            }).collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                json!({"ordinal":ordinal,"baseline_hits10":10,"arms":arms,
                "returned_evidence":returned})
            })
            .collect::<Vec<_>>();
        json!({"status":"MEASURED","complete":true,"query_count":1000,
            "shortlist_sizes":counts,"queries":queries,"baseline_hits10":10000,
            "admissible_queries":[1000,1000,1000,1000],
            "measured_hit_subtotals":[9000,9000,10000,10000],
            "quality_floor_met":[false,false,true,true],"recall_floor_hits10":9760,
            "cold_latency_claim":false,"vendor_win_claim":false})
    }

    #[test]
    fn curve_report_retains_valid_below_floor_points() {
        let mut report = report_fixture();
        finalize_report(&mut report).unwrap();
        assert_eq!(report["status"], "MEASURED");
        assert_eq!(
            report["quality_advancement_eligible"],
            json!([false, false, true, true])
        );
        assert_eq!(
            report["planned_target_gets_totals"],
            json!([1000, 1000, 1000, 1000])
        );
        assert_eq!(
            report["planned_target_bytes_totals"],
            json!([4096000, 4096000, 4096000, 4096000])
        );
        assert!(validate_report(&report).is_ok());
    }

    #[test]
    fn curve_output_is_create_only_and_uses_its_own_cap() {
        let root = super::super::driver_tests::directory();
        let output = root.join("curve.json");
        let mut report = report_fixture();
        finalize_report(&mut report).unwrap();
        assert_eq!(crate::report_cap(SCHEMA), 16 * 1024 * 1024);
        assert_eq!(
            crate::report_cap(super::super::SHORTLIST_SCHEMA),
            crate::REPORT_CAP
        );
        assert!(crate::execute_report(&output, SCHEMA, || Ok(report)).unwrap());
        let before = std::fs::read(&output).unwrap();
        assert!(
            crate::execute_report(&output, SCHEMA, || panic!(
                "existing output must refuse before computation"
            ))
            .is_err()
        );
        assert_eq!(std::fs::read(&output).unwrap(), before);
        let invalid = root.join("invalid.json");
        assert!(
            !super::super::execute_count_curve(
                &root.join("absent-config.json"),
                &"0".repeat(64),
                &invalid,
                false,
                false
            )
            .unwrap()
        );
        let refused: Value = serde_json::from_slice(&std::fs::read(invalid).unwrap()).unwrap();
        assert_eq!(refused["status"], "INVALID");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn curve_report_uses_fixed_nonratcheting_floor() {
        let mut report = report_fixture();
        for query in report["queries"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .take(760)
        {
            query["arms"][0]["quality"] =
                json!({"hits10":10,"gained":0,"lost":0,"miss_buckets":[0,0,0,0]});
        }
        report["measured_hit_subtotals"][0] = json!(9760);
        report["quality_floor_met"][0] = json!(true);
        finalize_report(&mut report).unwrap();
        assert_eq!(report["quality_advancement_eligible"][0], true);
        report["queries"][0]["arms"][0]["quality"] =
            json!({"hits10":9,"gained":0,"lost":1,"miss_buckets":[1,0,0,0]});
        report["measured_hit_subtotals"][0] = json!(9759);
        report["quality_floor_met"][0] = json!(false);
        finalize_report(&mut report).unwrap();
        assert_eq!(report["status"], "MEASURED");
        assert_eq!(report["quality_advancement_eligible"][0], false);
    }

    #[test]
    fn curve_report_rejects_population_identity_and_accounting_corruption() {
        let mut valid = report_fixture();
        finalize_report(&mut valid).unwrap();
        for (pointer, value) in [
            ("/queries/999/ordinal", json!(998)),
            ("/queries/999/arms/3/quality/hits10", json!(9)),
            ("/queries/999/arms/3/gets", json!(31)),
            ("/queries/999/arms/3/physical_bytes", json!(33554433)),
            ("/queries/999/returned_evidence/3/1/id", json!(0)),
            ("/queries/999/returned_evidence/3/1/target_row", json!(0)),
            (
                "/queries/999/returned_evidence/3/1/score_bits",
                json!(f32::NAN.to_bits()),
            ),
            ("/measured_hit_subtotals/3", json!(9999)),
            ("/planned_target_bytes_totals/3", json!(1)),
            ("/quality_floor_met/0", json!(true)),
            ("/closed_selector_control_parity", json!(false)),
            ("/cold_latency_claim", json!(true)),
            ("/vendor_win_claim", json!(true)),
        ] {
            let mut bad = valid.clone();
            *bad.pointer_mut(pointer).unwrap() = value;
            assert!(
                validate_report(&bad).is_err(),
                "accepted corruption {pointer}"
            );
        }
        let mut missing = valid;
        missing["queries"].as_array_mut().unwrap().pop();
        assert!(validate_report(&missing).is_err());
    }
}
