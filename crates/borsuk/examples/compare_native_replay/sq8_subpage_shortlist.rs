//! Native shortlist diagnostic fixtures. No performance or adoption claim.

#[cfg(test)]
mod tests {
    use borsuk::{
        rotated_two_bit::RotatedTwoBitCodec,
        semantic_unit_router::SemanticProfile,
        two_bit_generation::{TwoBitPlanTrace, plan_two_bit_source_walks},
    };
    use std::collections::BTreeSet;

    // Test-only RED stub; no production implementation or runnable CLI yet.
    fn shortlist(
        _rows: usize,
        _scored: &[(usize, f64)],
        _maximum: usize,
        _max_payload_bytes: usize,
    ) -> Result<Vec<usize>, &'static str> {
        Err("shortlist implementation missing")
    }

    #[test]
    fn shortlist_captures_production_scored_rows_excludes_gap_and_breaks_ties() {
        let rows = 513;
        let codec = RotatedTwoBitCodec::new(&[0., 0.], 20260923).unwrap();
        let records = (0..rows)
            .map(|_| codec.encode(&[1., 0.]).unwrap())
            .collect::<Vec<_>>();
        let prepared = codec
            .prepare_query(
                &[1., 0.],
                RotatedTwoBitCodec::required_query_scratch_bytes(2).unwrap(),
            )
            .unwrap();
        let mut trace = TwoBitPlanTrace::default();
        let mut scored = Vec::new();
        let plan = plan_two_bit_source_walks(
            rows,
            2,
            2,
            Some(SemanticProfile::Scale1m),
            &[(0, vec![0, 1, 2, 3, 4, 5, 6, 7, 16])],
            &prepared,
            |row| {
                let record = records.get(row)?;
                scored.push((row, prepared.score(record).unwrap()));
                Some(record.as_slice())
            },
            1,
            1 << 20,
            Some(&mut trace),
        )
        .unwrap();
        assert_eq!(
            scored.iter().map(|&(row, _)| row).collect::<BTreeSet<_>>(),
            (0..256).chain([512]).collect()
        );
        assert!(
            scored
                .iter()
                .all(|&(row, score)| !(256..512).contains(&row) && score.is_finite())
        );
        assert_eq!(
            trace
                .ranked_candidate_pages
                .iter()
                .copied()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([0, 2])
        );
        assert!(!plan.ranges.is_empty());
        assert_eq!(
            shortlist(rows, &scored, 2, 1 << 20)
                .expect("bounded shortlist should retain tied best physical rows"),
            vec![0, 1]
        );
        assert_eq!(
            shortlist(10, &[(9, 1.), (2, 1.), (7, 0.5)], 1, 1 << 20).unwrap(),
            vec![2]
        );
    }
    #[test]
    fn shortlist_authenticated_block_expansion_and_bridges_recover_distinct_rows() {
        use borsuk::{
            exact_sq8_nominee::Sq8Geometry,
            returned_sq8::{ReturnedRange, rank_returned_ranges},
            sq8_page_authority::{PageAuthority, SelectedSq8Limits, plan_selected_sq8_rows},
        };
        use sha2::{Digest, Sha256};
        let rows = 65;
        let width = 14;
        let mut object = Vec::new();
        for row in 0..rows {
            let id: i64 = match row {
                0 => 9,
                32 => 3,
                64 => 7,
                _ => 1_000 + row as i64,
            };
            object.extend_from_slice(&id.to_le_bytes());
            object.extend_from_slice(&1_f32.to_le_bytes());
            object.extend_from_slice(if [0, 32, 64].contains(&row) {
                &[1, 0]
            } else {
                &[0, 1]
            });
        }
        let sidecar = object
            .chunks(32 * width)
            .flat_map(|part| Sha256::digest(part).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2", "generation":1,
            "rows":rows,"dimensions":2,"page_rows":32,
            "object_sha256":format!("{:x}",Sha256::digest(&object)),
            "page_digest_sha256":format!("{:x}",Sha256::digest(&sidecar))
        }))
        .unwrap();
        let authority = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        let selected = shortlist(rows, &[(0, 1.), (64, 0.9)], 2, 1 << 20)
            .expect("shortlist must preserve eligible physical rows");
        assert_eq!(selected, vec![0, 64]);
        let rank = |maximum_gets| {
            let plan = plan_selected_sq8_rows(
                &authority,
                &selected,
                SelectedSq8Limits {
                    max_gets: maximum_gets,
                    max_bytes: object.len(),
                    max_planner_bytes: 5_120,
                },
            )
            .unwrap();
            let ranges = plan
                .ranges
                .iter()
                .map(|r| {
                    let first = r.start / (32 * width);
                    let last = (r.end - 1) / (32 * width);
                    assert_eq!(*r, authority.byte_range(first, last).unwrap());
                    authority
                        .verify_payload(first, last, &object[r.clone()])
                        .unwrap();
                    ReturnedRange {
                        start: r.start,
                        bytes: &object[r.clone()],
                    }
                })
                .collect::<Vec<_>>();
            let scores = rank_returned_ranges(
                Sq8Geometry {
                    rows,
                    dimensions: 2,
                },
                &ranges,
                &[1., 0.],
                &[0., 0.],
                &[1., 1.],
                1,
                plan.planned_bytes,
            )
            .unwrap();
            (plan, scores[0])
        };
        let (separate, selected_best) = rank(2);
        assert_eq!(separate.ranges, vec![0..448, 896..910]);
        assert_eq!(separate.planned_bytes, 462);
        assert_eq!((selected_best.ordinal, selected_best.id), (64, 7));
        let (bridged, bridge_best) = rank(1);
        assert_eq!(bridged.ranges, vec![0..910]);
        assert_eq!(bridged.planned_bytes, 910);
        assert_eq!((bridge_best.ordinal, bridge_best.id), (32, 3));
        assert!(!selected.contains(&bridge_best.ordinal));
        assert_eq!(selected_best.score.to_bits(), bridge_best.score.to_bits());
        // IDs 3/7/9 are not physical ordinals. Gap expansion changed the winner
        // through the real scorer; shortlist-only quality must not hide this.
    }
    #[test]
    fn shortlist_refuses_invalid_scores_ordinals_and_workspace_boundary() {
        // Conservative owned copy/sort/output model: 64*N + 16*min(M,N) + 4096.
        // Borrowed captured scores and authority payloads are caller-funded.
        let scores = [(9, 1.), (2, 1.), (7, 0.5)];
        assert_eq!(shortlist(10, &scores, 2, 4_319), Err("shortlist payload"));
        assert_eq!(shortlist(10, &scores, 2, 4_320).unwrap(), vec![2, 9]);
        for bad in [
            vec![(10, 1.)],
            vec![(1, f64::NAN)],
            vec![(1, f64::INFINITY)],
            vec![(1, f64::NEG_INFINITY)],
            vec![(1, 1.), (1, 0.9)],
            vec![],
        ] {
            assert_eq!(shortlist(10, &bad, 2, 1 << 20), Err("shortlist roster"));
        }
        assert_eq!(shortlist(0, &scores, 2, 1 << 20), Err("shortlist shape"));
        assert_eq!(shortlist(10, &scores, 0, 1 << 20), Err("shortlist shape"));
        assert_eq!(shortlist(10, &scores, 2, 0), Err("shortlist shape"));
        assert_eq!(
            shortlist(10, &scores, usize::MAX, 4_336).unwrap(),
            vec![2, 7, 9]
        );
    }
    #[test]
    fn shortlist_block_only_control_preserves_all_baseline_bytes_ids_and_score_bits() {
        use borsuk::{
            exact_sq8_nominee::Sq8Geometry,
            returned_sq8::{ReturnedRange, rank_returned_ranges},
            sq8_page_authority::{
                PageAuthority, PageError, SelectedSq8Limits, plan_selected_sq8_rows,
            },
        };
        use sha2::{Digest, Sha256};
        let rows = 257;
        let width = 14;
        let mut object = Vec::new();
        for row in 0..rows {
            let id = 1_234_i64 + (rows - row) as i64;
            object.extend_from_slice(&id.to_le_bytes());
            object.extend_from_slice(&1_f32.to_le_bytes());
            object.extend_from_slice(if row % 7 == 0 { &[1, 0] } else { &[0, 1] });
        }
        let authorize = |block| {
            let sidecar = object
                .chunks(block * width)
                .flat_map(|part| Sha256::digest(part).to_vec())
                .collect::<Vec<_>>();
            let manifest = serde_json::to_vec(&serde_json::json!({
                "schema":"borsuk-v115-sq8-page-authority-v2","generation":1,
                "rows":rows,"dimensions":2,"page_rows":block,
                "object_sha256":format!("{:x}",Sha256::digest(&object)),
                "page_digest_sha256":format!("{:x}",Sha256::digest(&sidecar))
            }))
            .unwrap();
            PageAuthority::load(
                &manifest,
                &format!("{:x}", Sha256::digest(&manifest)),
                &sidecar,
            )
            .unwrap()
        };
        let coarse = authorize(256);
        let fine = authorize(32);
        let limits = SelectedSq8Limits {
            max_gets: 2,
            max_bytes: object.len(),
            max_planner_bytes: 8_704,
        };
        let baseline = plan_selected_sq8_rows(&coarse, &[0, 256], limits).unwrap();
        let witnesses = (0..rows).step_by(32).collect::<Vec<_>>();
        let control = plan_selected_sq8_rows(&fine, &witnesses, limits).unwrap();
        assert_eq!(baseline.ranges, vec![0..3_598]);
        assert_eq!(control.ranges, baseline.ranges);
        assert_eq!(control.planned_bytes, baseline.planned_bytes);
        coarse.verify_payload(0, 1, &object).unwrap();
        fine.verify_payload(0, 8, &object).unwrap();
        let rank = |ranges: &[std::ops::Range<usize>]| {
            let returned = ranges
                .iter()
                .map(|r| ReturnedRange {
                    start: r.start,
                    bytes: &object[r.clone()],
                })
                .collect::<Vec<_>>();
            rank_returned_ranges(
                Sq8Geometry {
                    rows,
                    dimensions: 2,
                },
                &returned,
                &[1., 0.],
                &[0., 0.],
                &[1., 1.],
                10,
                object.len(),
            )
            .unwrap()
            .into_iter()
            .map(|s| (s.ordinal, s.id, s.score.to_bits()))
            .collect::<Vec<_>>()
        };
        let actual = rank(&baseline.ranges);
        assert_eq!((actual[0].0, actual[0].1), (252, 1_239));
        assert_eq!(rank(&control.ranges), actual);
        let mut corrupt_tail = object[3_584..].to_vec();
        corrupt_tail[13] ^= 1;
        assert_eq!(
            fine.verify_payload(8, 8, &corrupt_tail),
            Err(PageError::HashMismatch)
        );
    }
}
