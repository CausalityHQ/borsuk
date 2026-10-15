//! Native SOURCE-count curve fixtures; intentional RED checkpoint, not adopted.
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
    // RED: retain the old supported shortlist roster until native failure proves
    // that the new bounded lower-count configuration is missing.
    crate::require(
        c.shortlist.shortlist_sizes == [1024, 2048, 4096, 8192],
        "count curve lower counts unsupported",
    )?;
    Ok((c, n))
}
pub(super) fn plan_arms(
    a: &PageAuthority,
    scored: &[(usize, f64)],
    counts: &[usize; 4],
    limits: SelectedSq8Limits,
    cap: usize,
) -> Result<Vec<ShortlistArm>> {
    // RED: the existing budgeted mode evaluates only its single control arm.
    let maximum = counts[3];
    let nominees = super::shortlist(a.rows(), scored, maximum, cap)?;
    let selected = super::budget_admit_shortlist_blocks(a, scored, &nominees, limits, cap)?;
    let plan = plan_selected_sq8_rows(a, &selected, limits)
        .map_err(|e| format!("count curve plan {e:?}"))?;
    Ok(vec![ShortlistArm {
        maximum,
        selected,
        ranges: plan.ranges,
        bytes: plan.planned_bytes,
        returned: None,
    }])
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
