//! Native shortlist diagnostic fixtures. No performance or adoption claim.

// Offline diagnostic workspace only. Borrowed capture storage and allocator
// overhead are caller-funded; admission precedes copying/sorting payload.
fn shortlist(
    rows: usize,
    scored: &[(usize, f64)],
    maximum: usize,
    max_payload_bytes: usize,
) -> Result<Vec<usize>, &'static str> {
    if rows == 0 || maximum == 0 || max_payload_bytes == 0 {
        return Err("shortlist shape");
    }
    if scored.is_empty()
        || scored.len() > rows
        || scored
            .iter()
            .any(|&(row, score)| row >= rows || !score.is_finite())
    {
        return Err("shortlist roster");
    }
    let keep = maximum.min(scored.len());
    let modeled = scored
        .len()
        .checked_mul(64)
        .and_then(|bytes| {
            keep.checked_mul(16)
                .and_then(|output| bytes.checked_add(output))
        })
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or("shortlist payload")?;
    if modeled > max_payload_bytes {
        return Err("shortlist payload");
    }
    let mut ranked = Vec::new();
    ranked
        .try_reserve_exact(scored.len())
        .map_err(|_| "shortlist payload")?;
    ranked.extend_from_slice(scored);
    ranked.sort_unstable_by_key(|&(row, _)| row);
    if ranked.windows(2).any(|pair| pair[0].0 == pair[1].0) {
        return Err("shortlist roster");
    }
    ranked.sort_unstable_by(|left, right| right.1.total_cmp(&left.1).then(left.0.cmp(&right.0)));
    let mut selected = Vec::new();
    selected
        .try_reserve_exact(keep)
        .map_err(|_| "shortlist payload")?;
    selected.extend(ranked[..keep].iter().map(|&(row, _)| row));
    selected.sort_unstable();
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::shortlist;
    use borsuk::{
        rotated_two_bit::RotatedTwoBitCodec,
        semantic_unit_router::SemanticProfile,
        two_bit_generation::{TwoBitPlanTrace, plan_two_bit_source_walks},
    };
    use std::collections::BTreeSet;

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
        let flat = records.concat();
        let (captured_plan, captured_trace, captured_scores) = super::capture_scored_rows(
            rows,
            2,
            2,
            &[(0, vec![0, 1, 2, 3, 4, 5, 6, 7, 16])],
            &prepared,
            &flat,
            records[0].len(),
            257,
            257 * 32 + 4096,
            1,
            1 << 20,
        )
        .unwrap();
        assert_eq!(captured_plan.ranges, plan.ranges);
        assert_eq!(captured_plan.selected_pages, plan.selected_pages);
        assert_eq!(
            serde_json::to_value(captured_trace).unwrap(),
            serde_json::to_value(&trace).unwrap()
        );
        assert_eq!(captured_scores, scored);
        assert!(
            super::capture_scored_rows(
                rows,
                2,
                2,
                &[(0, vec![0, 1, 2, 3, 4, 5, 6, 7, 16])],
                &prepared,
                &flat,
                records[0].len(),
                256,
                1 << 20,
                1,
                1 << 20
            )
            .is_err()
        );
        assert!(
            super::capture_scored_rows(
                rows,
                2,
                2,
                &[(0, vec![0, 1, 2, 3, 4, 5, 6, 7, 16])],
                &prepared,
                &flat,
                records[0].len(),
                257,
                257 * 32 + 4095,
                1,
                1 << 20
            )
            .is_err()
        );
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
        let width = 14_usize;
        let mut object = Vec::new();
        for row in 0..rows {
            let id = 1_234_i64 + (rows - row) as i64;
            object.extend_from_slice(&id.to_le_bytes());
            object.extend_from_slice(&1_f32.to_le_bytes());
            object.extend_from_slice(if row % 7 == 0 { &[1, 0] } else { &[0, 1] });
        }
        let authorize = |block: usize| {
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

// The sidecar is ephemeral diagnostic metadata; whole-object authentication must
// finish before any digest is exposed to a candidate planner.
fn authenticated_sq8_digests(
    pin: &super::CompletedInput,
    rows: usize,
    dimensions: usize,
    max_payload_bytes: usize,
) -> super::Result<(Vec<u8>, super::HeaderSnapshot)> {
    use sha2::{Digest, Sha256};
    use std::io::Read;
    super::require(
        rows > 0 && dimensions > 0 && dimensions <= 1024,
        "sidecar geometry",
    )?;
    let width = dimensions.checked_add(12).ok_or("sidecar record width")?;
    let object_bytes = rows.checked_mul(width).ok_or("sidecar object bytes")?;
    let block_bytes = width.checked_mul(32).ok_or("sidecar block bytes")?;
    let sidecar_bytes = rows
        .div_ceil(32)
        .checked_mul(32)
        .ok_or("sidecar digest bytes")?;
    let modeled = sidecar_bytes
        .checked_add(block_bytes)
        .and_then(|n| n.checked_add(4096))
        .ok_or("sidecar workspace overflow")?;
    super::require(modeled <= max_payload_bytes, "sidecar workspace")?;
    super::header_pin(pin, object_bytes as u64)?;
    super::require(pin.bytes == object_bytes as u64, "sidecar exact geometry")?;
    let mut file = super::header_file(&pin.path)?;
    let identity = super::file_identity(&file)?;
    super::require(identity.len == pin.bytes, "sidecar exact file length")?;
    let mut sidecar = Vec::new();
    sidecar.try_reserve_exact(sidecar_bytes)?;
    let mut block = Vec::new();
    block.try_reserve_exact(block_bytes)?;
    block.resize(block_bytes, 0);
    let mut whole = Sha256::new();
    let mut remaining = object_bytes;
    while remaining > 0 {
        let take = remaining.min(block_bytes);
        file.read_exact(&mut block[..take])?;
        whole.update(&block[..take]);
        sidecar.extend_from_slice(&Sha256::digest(&block[..take]));
        remaining -= take;
    }
    super::require(
        file.read(&mut [0])? == 0
            && format!("{:x}", whole.finalize()) == pin.sha256
            && sidecar.len() == sidecar_bytes,
        "sidecar complete SHA/EOF",
    )?;
    let snapshot = super::HeaderSnapshot {
        file,
        path: pin.path.clone(),
        identity,
    };
    snapshot.recheck()?;
    Ok((sidecar, snapshot))
}

#[cfg(test)]
mod digest_tests {
    use super::authenticated_sq8_digests;
    use sha2::{Digest, Sha256};
    use std::io::Write;

    #[test]
    fn shortlist_streamed_sidecar_authenticates_whole_object_and_partial_tail() {
        let rows = 65_usize;
        let dimensions = 2_usize;
        let width = dimensions + 12;
        let path = std::env::temp_dir().join(format!(
            "borsuk-shortlist-digests-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let object = (0..rows * width)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        file.write_all(&object).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let mut pin = super::super::CompletedInput {
            path: path.clone(),
            bytes: object.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&object)),
        };
        let outcome = (|| -> super::super::Result<()> {
            let (digests, snapshot) = authenticated_sq8_digests(&pin, rows, dimensions, 8192)?;
            let expected = object
                .chunks(32 * width)
                .flat_map(|block| Sha256::digest(block).to_vec())
                .collect::<Vec<_>>();
            assert_eq!(digests.len(), 96);
            assert_eq!(digests, expected);
            assert_eq!(
                &digests[64..],
                Sha256::digest(&object[64 * width..]).as_slice()
            );
            snapshot.recheck()?;
            drop(snapshot);
            pin.sha256 = "0".repeat(64);
            assert!(authenticated_sq8_digests(&pin, rows, dimensions, 8192).is_err());
            pin.sha256 = format!("{:x}", Sha256::digest(&object));
            assert!(authenticated_sq8_digests(&pin, rows, dimensions, 1).is_err());
            assert!(authenticated_sq8_digests(&pin, rows + 1, dimensions, 8192).is_err());
            let mut changed = object.clone();
            *changed.last_mut().unwrap() ^= 1;
            std::fs::write(&path, changed)?;
            assert!(authenticated_sq8_digests(&pin, rows, dimensions, 8192).is_err());
            Ok(())
        })();
        std::fs::remove_file(path).unwrap();
        outcome.expect("whole-object authenticated sidecar with exact partial tail");
    }
}

// Replay nomination with the production scorer and capture only rows requested
// by its record callback. Physical SOURCE cover gaps are not eligible rows.
#[allow(clippy::too_many_arguments)]
fn capture_scored_rows(
    rows: usize,
    dimensions: usize,
    nomination_page_limit: usize,
    walks: &[(usize, Vec<usize>)],
    prepared: &borsuk::rotated_two_bit::PreparedTwoBit,
    records: &[u8],
    record_bytes: usize,
    capture_row_cap: usize,
    max_capture_bytes: usize,
    max_gets: usize,
    max_bytes: usize,
) -> super::Result<(
    borsuk::budgeted_page_rank::BudgetedPagePlan,
    borsuk::two_bit_generation::TwoBitPlanTrace,
    Vec<(usize, f64)>,
)> {
    super::require(
        rows > 0 && record_bytes > 0 && capture_row_cap > 0,
        "capture geometry",
    )?;
    super::require(
        rows.checked_mul(record_bytes) == Some(records.len()),
        "capture exact source bytes",
    )?;
    let modeled = capture_row_cap
        .checked_mul(32)
        .and_then(|n| n.checked_add(4096))
        .ok_or("capture workspace overflow")?;
    super::require(modeled <= max_capture_bytes, "capture workspace")?;
    let mut captured = Vec::new();
    captured.try_reserve_exact(capture_row_cap)?;
    let mut trace = borsuk::two_bit_generation::TwoBitPlanTrace::default();
    let plan = borsuk::two_bit_generation::plan_two_bit_source_walks(
        rows,
        dimensions,
        nomination_page_limit,
        Some(borsuk::semantic_unit_router::SemanticProfile::Scale1m),
        walks,
        prepared,
        |row| {
            if captured.len() == capture_row_cap {
                return None;
            }
            let start = row.checked_mul(record_bytes)?;
            let record = records.get(start..start.checked_add(record_bytes)?)?;
            let score = prepared.score(record).ok()?;
            if !score.is_finite() {
                return None;
            }
            captured.push((row, score));
            Some(record)
        },
        max_gets,
        max_bytes,
        Some(&mut trace),
    )?;
    super::require(!captured.is_empty(), "capture empty roster")?;
    Ok((plan, trace, captured))
}
