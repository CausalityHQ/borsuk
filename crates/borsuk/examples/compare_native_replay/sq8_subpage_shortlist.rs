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

const SHORTLIST_SCHEMA: &str = "borsuk-sq8-subpage-shortlist-v1";
pub(super) const ADMISSION_SCHEMA: &str = "borsuk-sq8-subpage-shortlist-input-admission-v1";
const BASELINE_SQ8_GETS: usize = 32;
const CANDIDATE_SQ8_GETS: usize = 30;
const MAX_SCORED_UNITS: usize = 24_576;

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ShortlistConfig {
    schema: String,
    completed: super::CompletedConfig,
    generation_manifest: super::CompletedInput,
    plane_manifest: super::CompletedInput,
    records: super::CompletedInput,
    mean: super::CompletedInput,
    source_digests: super::CompletedInput,
    sq8: super::CompletedInput,
    requests: super::CompletedInput,
    truth: super::CompletedInput,
    store_root: std::path::PathBuf,
    scratch_parent: std::path::PathBuf,
    shortlist_sizes: [usize; 4],
    max_payload_bytes: usize,
}

fn plan_value(
    plan: &borsuk::budgeted_page_rank::BudgetedPagePlan,
    dimensions: usize,
) -> serde_json::Value {
    let page_bytes = (dimensions + 12) * 256;
    let covered: usize = plan
        .ranges
        .iter()
        .map(|r| r.len().div_ceil(page_bytes))
        .sum();
    serde_json::json!({"selected_pages":plan.selected_pages,
        "ranges":plan.ranges.iter().map(|r|[r.start,r.end]).collect::<Vec<_>>(),
        "planned_bytes":plan.planned_bytes,"target_pages":plan.target_pages,
        "target_shortfall":plan.target_shortfall,"primary_pages_retained":plan.primary_pages_retained,
        "covered_pages":covered,"bridge_pages":covered.saturating_sub(plan.selected_pages.len())})
}

fn rank_local_cover(
    snapshot: &mut super::HeaderSnapshot,
    authority: &borsuk::sq8_page_authority::PageAuthority,
    ranges: &[std::ops::Range<usize>],
    rows: usize,
    dimensions: usize,
    query: &[f32],
    low: &[f32],
    step: &[f32],
    max_bytes: usize,
) -> super::Result<Vec<borsuk::exact_sq8_nominee::ScoredNominee>> {
    use std::io::{Read, Seek, SeekFrom};
    snapshot.recheck()?;
    let width = dimensions + 12;
    let bytes = ranges
        .iter()
        .try_fold(0_usize, |n, r| n.checked_add(r.len()))
        .ok_or("cover byte overflow")?;
    super::require(
        !ranges.is_empty()
            && bytes <= max_bytes
            && ranges.windows(2).all(|w| w[0].end <= w[1].start),
        "admitted ordered local cover",
    )?;
    let mut bodies = Vec::with_capacity(ranges.len());
    for range in ranges {
        super::require(
            range.start < range.end
                && range.start % (32 * width) == 0
                && range.end <= rows * width
                && (range.end % (32 * width) == 0 || range.end == rows * width),
            "local cover block geometry",
        )?;
        let mut body = Vec::new();
        body.try_reserve_exact(range.len())?;
        body.resize(range.len(), 0);
        snapshot.file.seek(SeekFrom::Start(range.start as u64))?;
        snapshot.file.read_exact(&mut body)?;
        authority
            .verify_payload(
                range.start / (32 * width),
                (range.end - 1) / (32 * width),
                &body,
            )
            .map_err(|e| format!("SQ8 payload authentication {e:?}"))?;
        bodies.push(body);
    }
    snapshot.recheck()?;
    let returned = ranges
        .iter()
        .zip(&bodies)
        .map(|(r, b)| borsuk::returned_sq8::ReturnedRange {
            start: r.start,
            bytes: b.as_slice(),
        })
        .collect::<Vec<_>>();
    Ok(borsuk::returned_sq8::rank_returned_ranges(
        borsuk::exact_sq8_nominee::Sq8Geometry { rows, dimensions },
        &returned,
        query,
        low,
        step,
        super::K,
        max_bytes,
    )
    .map_err(|e| format!("SQ8 returned ranking {e:?}"))?)
}

struct ShortlistArm {
    maximum: usize,
    selected: Vec<usize>,
    ranges: Vec<std::ops::Range<usize>>,
    bytes: usize,
    returned: Option<Vec<borsuk::exact_sq8_nominee::ScoredNominee>>,
}

fn reduce_shortlist(
    config_path: &std::path::Path,
    config_sha: &str,
) -> super::Result<serde_json::Value> {
    reduce_shortlist_mode(config_path, config_sha, false)
}

fn reduce_shortlist_mode(
    config_path: &std::path::Path,
    config_sha: &str,
    admission_only: bool,
) -> super::Result<serde_json::Value> {
    use super::{
        header_json, header_pin, loss_binary, loss_coverage, read_run_observed, require,
        scan_loss_sq8,
    };
    use borsuk::{
        rotated_two_bit::RotatedTwoBitCodec,
        sq8_page_authority::{PageAuthority, SelectedSq8Limits, plan_selected_sq8_rows},
        two_bit_generation::{TwoBitGeneration, TwoBitGenerationLimits, TwoBitPlanTrace},
    };
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};
    use std::collections::BTreeSet;
    let (c, config_bytes): (ShortlistConfig, _) = super::read_config(config_path, config_sha)?;
    require(
        c.schema == "borsuk-sq8-subpage-shortlist-config-v1"
            && c.completed.schema == super::SCALE_CONFIG_SCHEMA,
        "shortlist schema",
    )?;
    let i: super::Inputs = serde_json::from_value(c.completed.expected_bound_inputs.clone())?;
    super::validate_scale_inputs(&i, &c.completed.expected_bound_inputs)?;
    require(
        i.rows == 1_000_000
            && i.dimensions == 1024
            && i.count == 1000
            && i.k == 10
            && i.metric == "cosine"
            && i.profile == "scale1m"
            && i.source_get_budget.unwrap_or(128) == 128,
        "shortlist frozen population/protocol",
    )?;
    require(
        c.shortlist_sizes == [1024, 2048, 4096, 8192]
            && c.max_payload_bytes == 2 * 1024 * 1024 * 1024,
        "shortlist preregistered curve/envelope",
    )?;
    for header in [
        &c.completed.expected_identity,
        &c.completed.expected_bound_inputs,
    ] {
        require(
            header["fetch_parallelism"].as_u64() == Some(64)
                && header.get("shared_byte_window").is_none(),
            "shortlist fixed fetch64/windowoff headers",
        )?;
    }
    require(
        c.completed.expected_bound_inputs["source_cache"] == "off",
        "shortlist source cache off",
    )?;
    super::header_path(&c.store_root)?;
    super::header_path(&c.scratch_parent)?;
    let generation_dir = c.store_root.join(&i.generation_prefix);
    require(
        c.generation_manifest.path == generation_dir.join("manifest.json")
            && c.plane_manifest.path == generation_dir.join("plane/manifest.json")
            && c.records.path == generation_dir.join("plane/records.bin")
            && c.mean.path == generation_dir.join("plane/mean.bin")
            && c.source_digests.path == generation_dir.join("plane/page_digests.bin"),
        "shortlist actual store descriptor paths",
    )?;
    require(
        c.generation_manifest.sha256 == i.generation_root_sha256
            && c.sq8.sha256 == i.native_sq8_sha256
            && c.requests.sha256 == i.requests_sha256
            && c.truth.sha256 == i.truth_sha256,
        "shortlist original artifact bindings",
    )?;
    header_pin(
        &c.records,
        (i.rows * super::source_record_bytes(i.dimensions)?) as u64,
    )?;
    header_pin(&c.mean, (i.dimensions * 4) as u64)?;
    header_pin(&c.source_digests, (i.rows.div_ceil(32) * 32) as u64)?;
    header_pin(&c.completed.input, super::FILE_CAP)?;
    header_pin(&c.generation_manifest, 64 * 1024)?;
    header_pin(&c.plane_manifest, 64 * 1024)?;
    header_pin(&c.requests, i.requests_bytes)?;
    header_pin(&c.truth, i.truth_bytes)?;
    header_pin(&c.sq8, (i.rows * (i.dimensions + 12)) as u64)?;
    require(
        c.records.bytes == (i.rows * super::source_record_bytes(i.dimensions)?) as u64
            && c.mean.bytes == (i.dimensions * 4) as u64
            && c.source_digests.bytes == (i.rows.div_ceil(32) * 32) as u64
            && c.sq8.bytes == (i.rows * (i.dimensions + 12)) as u64
            && c.requests.bytes == i.requests_bytes
            && c.truth.bytes == i.truth_bytes,
        "shortlist exact descriptor geometry",
    )?;
    // Admit retained caller payload before opening the baseline or allocating
    // source/candidate bodies. Allocator/runtime RSS is a separate native gate.
    let caller_bytes = c.records.bytes as usize
        + c.requests.bytes as usize
        + c.source_digests.bytes as usize
        + i.rows.div_ceil(32) * 32
        + i.count
            * (MAX_SCORED_UNITS * std::mem::size_of::<usize>() + std::mem::size_of::<Vec<usize>>())
        + 32 * 1024 * 1024
        + 1000 * 15360 * 8
        + 128 * 1024 * 1024;
    require(
        caller_bytes < c.max_payload_bytes,
        "shortlist retained caller payload",
    )?;
    let mut originals = Vec::<Vec<usize>>::with_capacity(i.count);
    let run = read_run_observed(
        &c.completed.input.path,
        &c.completed.input.sha256,
        Some(&c.completed),
        |line, q, inputs| {
            require(
                originals.len() < 1000 && q.ordinal == originals.len(),
                "shortlist baseline full query order",
            )?;
            let raw: Value = serde_json::from_slice(line)?;
            require(
                raw["trace"]["semantic_leaves"]
                    .as_array()
                    .is_some_and(|leaves| !leaves.is_empty() && leaves.len() <= 48),
                "shortlist bounded semantic leaves",
            )?;
            let coverage = loss_coverage(line, q, inputs)?;
            require(
                coverage.scored_units.len() <= MAX_SCORED_UNITS,
                "shortlist retained scored-unit cap",
            )?;
            originals.push(coverage.scored_units);
            Ok(())
        },
    )?;
    require(
        originals.len() == 1000 && run.samples.len() == 1000,
        "shortlist full baseline seal",
    )?;
    let (g, gs) = header_json(&c.generation_manifest)?;
    let (plane_body, ps) = loss_binary(&c.plane_manifest, c.plane_manifest.bytes)?;
    let plane: borsuk::two_bit_source::SourcePlaneReceipt = serde_json::from_slice(&plane_body)?;
    require(
        g["plane_manifest_sha256"] == c.plane_manifest.sha256
            && g["sq8_object_sha256"] == c.sq8.sha256
            && plane.rows == i.rows
            && plane.dimensions == i.dimensions
            && plane.record_bytes == super::source_record_bytes(i.dimensions)?
            && plane.records_sha256 == c.records.sha256
            && plane.mean_sha256 == c.mean.sha256
            && plane.page_digest_sha256 == c.source_digests.sha256
            && plane.source_order_sha256 == i.native_order_sha256
            && plane.source_sha256 == i.native_source_sha256
            && plane.sq8_sha256 == i.native_sq8_sha256
            && !plane.query_or_truth_used,
        "shortlist authenticated plane/generation binding",
    )?;
    let (records, rs) = loss_binary(&c.records, (i.rows * plane.record_bytes) as u64)?;
    let (mean_body, ms) = loss_binary(&c.mean, (i.dimensions * 4) as u64)?;
    let (source_sidecar, ds) = loss_binary(&c.source_digests, (i.rows.div_ceil(32) * 32) as u64)?;
    let source_authority = PageAuthority::load_two_bit(
        &plane_body,
        &c.plane_manifest.sha256,
        g["generation"].as_u64().ok_or("shortlist generation")?,
        &source_sidecar,
    )
    .map_err(|e| format!("SOURCE authority {e:?}"))?;
    source_authority
        .verify_payload(0, i.rows.div_ceil(32) - 1, &records)
        .map_err(|e| format!("SOURCE payload authentication {e:?}"))?;
    let mean = mean_body
        .chunks_exact(4)
        .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
        .collect::<Vec<_>>();
    require(mean.iter().all(|x| x.is_finite()), "shortlist finite mean")?;
    let codec = RotatedTwoBitCodec::new(&mean, plane.seed)?;
    let (request_body, qs) = loss_binary(&c.requests, (i.count * i.dimensions * 4) as u64)?;
    // Validate ID bijection and frozen physical order separately from digest derivation.
    let (_, order_snapshot) = scan_loss_sq8(
        &c.sq8,
        i.rows,
        i.dimensions,
        &i.native_order_sha256,
        &BTreeSet::new(),
        |_, _| Ok(()),
    )?;
    let (sidecar, mut sq8_snapshot) =
        authenticated_sq8_digests(&c.sq8, i.rows, i.dimensions, 4 * 1024 * 1024)?;
    let authority_body = serde_json::to_vec(
        &json!({"schema":"borsuk-v115-sq8-page-authority-v2","generation":g["generation"],"rows":i.rows,"dimensions":i.dimensions,"page_rows":32,"object_sha256":c.sq8.sha256,"page_digest_sha256":format!("{:x}",Sha256::digest(&sidecar))}),
    )?;
    let authority = PageAuthority::load(
        &authority_body,
        &format!("{:x}", Sha256::digest(&authority_body)),
        &sidecar,
    )
    .map_err(|e| format!("SQ8 authority {e:?}"))?;
    let low: Vec<f32> = serde_json::from_value(g["low"].clone())?;
    let step: Vec<f32> = serde_json::from_value(g["step"].clone())?;
    require(
        low.len() == i.dimensions
            && step.len() == i.dimensions
            && low.iter().all(|x| x.is_finite())
            && step.iter().all(|x| x.is_finite() && *x > 0.),
        "shortlist SQ8 calibration",
    )?;
    let scratch = RotatedTwoBitCodec::required_query_scratch_bytes(i.dimensions)?;
    let limits = TwoBitGenerationLimits {
        max_memory_bytes: c.max_payload_bytes as u64,
        max_active_queries: 1,
        max_query_bytes: 16_773_120,
        max_query_gets: BASELINE_SQ8_GETS,
        max_parallel_gets: 64,
        max_source_bytes: 64 * 1024 * 1024,
        max_source_gets: 128,
        max_parallel_source_gets: 64,
        max_query_scratch_bytes: scratch + TwoBitPlanTrace::scratch_bytes(i.rows),
        already_pinned_bytes: caller_bytes as u64,
    };
    let store = diagnostic_local_store(&c.store_root)?;
    let prefix = object_store::path::Path::parse(&i.generation_prefix)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let generation = runtime.block_on(TwoBitGeneration::open_remote(
        &store,
        &prefix,
        &i.generation_root_sha256,
        limits,
        &c.scratch_parent,
    ))?;
    if admission_only {
        let (truth, ts) = loss_binary(&c.truth, (i.count * super::K * 8) as u64)?;
        admit_payloads(&request_body, &truth, i.rows, i.dimensions, i.count, &codec)?;
        for snapshot in [
            &gs,
            &ps,
            &rs,
            &ms,
            &ds,
            &qs,
            &order_snapshot,
            &sq8_snapshot,
            &ts,
        ] {
            snapshot.recheck()?;
        }
        require(
            super::file_identity(&super::open_input(&c.completed.input.path)?)?
                == run.file_identity,
            "shortlist admission baseline lifetime identity",
        )?;
        super::read_config::<ShortlistConfig>(config_path, config_sha)?;
        return Ok(json!({
            "schema":ADMISSION_SCHEMA,"status":"READY_ACTUAL_INPUT","complete":true,
            "config_sha256":config_sha,"config_bytes":config_bytes,
            "rows":i.rows,"dimensions":i.dimensions,"query_count":i.count,"truth_ids_validated":i.count*super::K,
            "generation_root_sha256":i.generation_root_sha256,
            "source_records_sha256":c.records.sha256,"source_order_sha256":i.native_order_sha256,
            "sq8_sha256":c.sq8.sha256,"requests_sha256":c.requests.sha256,"truth_sha256":c.truth.sha256,
            "generation_opened_through_runtime_validator":true,"all_queries_and_truth_rows_validated":true,
            "whole_source_and_sq8_authenticated":true,"candidate_queries_executed":0,
            "truth_file_opened_for_input_validation_only":true,"baseline_replay_parity_checked":false,
            "payload_model_bytes":caller_bytes,"local_file_only":true,
            "scientific_qualified":false,"cold_latency_claim":false,"qps_claim":false,"vendor_win_claim":false,
            "qualified":false
        }));
    }
    let mut candidate_queries = Vec::with_capacity(1000);
    for slot in 0..originals.len() {
        let query = request_body[slot * i.dimensions * 4..(slot + 1) * i.dimensions * 4]
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect::<Vec<_>>();
        require(
            query.iter().all(|x| x.is_finite()),
            "shortlist finite query",
        )?;
        let (plan, trace, source, router) =
            runtime.block_on(generation.diagnostic_plan_with_store(&store, &query))?;
        let sample = &run.samples[slot];
        require(
            <[u8; 32]>::from(Sha256::digest(serde_json::to_vec(&plan_value(
                &plan,
                i.dimensions,
            ))?))
                == sample.physical_plan_sha256
                && <[u8; 32]>::from(Sha256::digest(serde_json::to_vec(&serde_json::to_value(
                    &trace,
                )?)?))
                    == sample.trace_sha256,
            "shortlist full production plan/trace parity",
        )?;
        require(
            source.submitted_gets as u64 == sample.charges.source.submitted_gets
                && source.verified_bytes as u64 == sample.charges.source.verified_bytes
                && source.failed_gets == 0
                && router.submitted_gets as u64 == sample.charges.router.submitted_gets
                && router.verified_bytes as u64 == sample.charges.router.verified_bytes
                && router.failed_gets == 0
                && plan.ranges.len() as u64 == sample.charges.sq8.submitted_gets
                && plan.planned_bytes as u64 == sample.charges.sq8.verified_bytes,
            "shortlist full charge parity",
        )?;
        let normalized = borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&query)?;
        let baseline = rank_local_cover(
            &mut sq8_snapshot,
            &authority,
            &plan.ranges,
            i.rows,
            i.dimensions,
            &normalized,
            &low,
            &step,
            16_773_120,
        )?;
        require(
            baseline.len() == sample.returned.len()
                && baseline.iter().zip(&sample.returned).all(|(a, b)| {
                    a.id >= 0 && a.id as u64 == b.id && a.score.to_bits() == b.score_bits
                }),
            "shortlist original ordered IDs/score bits",
        )?;
        let mut walked = trace.semantic_units.clone();
        walked.extend_from_slice(&trace.semantic_seed_additions);
        walked.sort_unstable();
        walked.dedup();
        let seed = trace
            .semantic_units
            .first()
            .ok_or("shortlist semantic seed")?
            / 8;
        let prepared = codec.prepare_query(&query, scratch)?;
        let cap = trace
            .nomination_evaluated_units
            .iter()
            .try_fold(0_usize, |n, u| {
                n.checked_add(((u + 1) * 32).min(i.rows) - u * 32)
            })
            .ok_or("capture count overflow")?;
        let (capture_plan, capture_trace, scored) = capture_scored_rows(
            i.rows,
            i.dimensions,
            trace.ranked_candidate_pages.len(),
            &[(seed, walked)],
            &prepared,
            &records,
            plane.record_bytes,
            cap,
            32 * cap + 4096,
            BASELINE_SQ8_GETS,
            16_773_120,
        )?;
        require(
            capture_plan == plan
                && capture_trace.nomination_evaluated_units == trace.nomination_evaluated_units
                && capture_trace.ranked_candidate_pages == trace.ranked_candidate_pages
                && capture_trace.primary_page == trace.primary_page,
            "shortlist capture exact production nomination",
        )?;
        let witnesses = plan
            .ranges
            .iter()
            .flat_map(|r| (r.start / (i.dimensions + 12)..r.end / (i.dimensions + 12)).step_by(32))
            .collect::<Vec<_>>();
        let control = plan_selected_sq8_rows(
            &authority,
            &witnesses,
            SelectedSq8Limits {
                max_gets: BASELINE_SQ8_GETS,
                max_bytes: 16_773_120,
                max_planner_bytes: 4 * 1024 * 1024 + 4096,
            },
        )
        .map_err(|e| format!("block control {e:?}"))?;
        require(
            control.ranges == plan.ranges && control.planned_bytes == plan.planned_bytes,
            "shortlist 32-block control exact physical bytes",
        )?;
        let control_ranked = rank_local_cover(
            &mut sq8_snapshot,
            &authority,
            &control.ranges,
            i.rows,
            i.dimensions,
            &normalized,
            &low,
            &step,
            16_773_120,
        )?;
        require(
            control_ranked.len() == baseline.len()
                && control_ranked.iter().zip(&baseline).all(|(a, b)| {
                    a.ordinal == b.ordinal && a.id == b.id && a.score.to_bits() == b.score.to_bits()
                }),
            "shortlist block-only control ordered ID/score-bit parity",
        )?;
        let mut arms = Vec::with_capacity(4);
        for maximum in c.shortlist_sizes {
            let selected = shortlist(
                i.rows,
                &scored,
                maximum,
                64 * scored.len() + 16 * maximum.min(scored.len()) + 4096,
            )
            .map_err(|e| e.to_string())?;
            // Compute geometry once. Workspace/shape errors are INVALID;
            // an excessive physical byte count is geometry-only, never fetched.
            let cover = plan_selected_sq8_rows(
                &authority,
                &selected,
                SelectedSq8Limits {
                    max_gets: CANDIDATE_SQ8_GETS,
                    max_bytes: usize::MAX,
                    max_planner_bytes: 4 * 1024 * 1024 + 4096,
                },
            )
            .map_err(|e| format!("candidate geometry {e:?}"))?;
            let returned = if cover.planned_bytes <= 32 * 1024 * 1024 {
                Some(rank_local_cover(
                    &mut sq8_snapshot,
                    &authority,
                    &cover.ranges,
                    i.rows,
                    i.dimensions,
                    &normalized,
                    &low,
                    &step,
                    32 * 1024 * 1024,
                )?)
            } else {
                None
            };
            arms.push(ShortlistArm {
                maximum,
                selected,
                ranges: cover.ranges,
                bytes: cover.planned_bytes,
                returned,
            });
        }
        candidate_queries.push(arms);
    }
    // Every complete original query/control has passed before truth opens.
    let (truth, ts) = loss_binary(&c.truth, (i.count * super::K * 8) as u64)?;
    require(
        truth.len() == i.count * super::K * 8,
        "shortlist full truth geometry",
    )?;
    let mut wanted = BTreeSet::new();
    let truth_rows = truth
        .chunks_exact(super::K * 8)
        .map(|row| {
            row.chunks_exact(8)
                .map(|id| u64::from_le_bytes(id.try_into().unwrap()))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    for ids in &truth_rows {
        require(
            ids.len() == 10
                && ids.iter().all(|id| *id < i.rows as u64)
                && ids.iter().copied().collect::<BTreeSet<_>>().len() == 10,
            "shortlist truth domain/duplicates",
        )?;
        wanted.extend(ids.iter().copied());
    }
    for arms in &candidate_queries {
        for arm in arms {
            if let Some(returned) = &arm.returned {
                for point in returned {
                    require(
                        point.id >= 0 && (point.id as usize) < i.rows,
                        "shortlist returned ID domain",
                    )?;
                    wanted.insert(point.id as u64);
                }
            }
        }
    }
    let (positions, positions_snapshot) = scan_loss_sq8(
        &c.sq8,
        i.rows,
        i.dimensions,
        &i.native_order_sha256,
        &wanted,
        |_, _| Ok(()),
    )?;
    let mut queries = Vec::with_capacity(1000);
    let mut totals = [0_usize; 4];
    let mut admissible = [0_usize; 4];
    let mut physical_bytes = [0_u64; 4];
    let mut baseline_hits = 0_u64;
    for (slot, arms) in candidate_queries.iter().enumerate() {
        let ids = &truth_rows[slot];
        let base = &run.samples[slot];
        let hits = ids
            .iter()
            .filter(|id| base.returned.iter().any(|h| h.id == **id))
            .count();
        require(
            hits as u64 == base.hits10,
            "shortlist original truth reconciliation",
        )?;
        baseline_hits += hits as u64;
        let mut reports = Vec::with_capacity(4);
        for (arm_index, arm) in arms.iter().enumerate() {
            let quality = assess_arm(
                arm,
                ids,
                &base.returned,
                &positions,
                &originals[slot],
                i.rows,
                i.dimensions,
            )?;
            if let Some(q) = &quality {
                admissible[arm_index] += 1;
                physical_bytes[arm_index] += arm.bytes as u64;
                totals[arm_index] += q.hits10;
            }
            reports.push(candidate_report(arm, i.rows, i.dimensions, quality)?);
        }
        queries.push(json!({"ordinal":slot,"baseline_hits10":hits,"arms":reports}));
    }
    require(
        baseline_hits == run.terminal.total_hits10,
        "shortlist baseline total recall",
    )?;
    for snapshot in [
        &gs,
        &ps,
        &rs,
        &ms,
        &ds,
        &qs,
        &order_snapshot,
        &sq8_snapshot,
        &ts,
        &positions_snapshot,
    ] {
        snapshot.recheck()?;
    }
    require(
        super::file_identity(&super::open_input(&c.completed.input.path)?)? == run.file_identity,
        "shortlist baseline lifetime identity",
    )?;
    super::read_config::<ShortlistConfig>(config_path, config_sha)?;
    Ok(
        json!({"schema":SHORTLIST_SCHEMA,"status":"MEASURED","complete":true,"config_sha256":config_sha,"config_bytes":config_bytes,"query_count":1000,"baseline_hits10":baseline_hits,"baseline_sq8_gets_total":run.samples.iter().map(|s|s.charges.sq8.submitted_gets).sum::<u64>(),"baseline_sq8_bytes_total":run.samples.iter().map(|s|s.charges.sq8.verified_bytes).sum::<u64>(),"baseline_sq8_get_budget":BASELINE_SQ8_GETS,"candidate_sq8_get_budget":CANDIDATE_SQ8_GETS,"block_control_rerank_is_determinism_check":true,"baseline_exact_plan_trace_charges_ids_scorebits":true,"truth_file_opened_after_all_baseline_controls":true,"shortlist_sizes":c.shortlist_sizes,"admissible_queries":admissible,"measured_hit_subtotals":totals,"physical_bytes_totals":physical_bytes,"recall_floor_hits10":9760,"nonratcheting_reference_hits10":9780,"quality_floor_met":(0..4).map(|a|if admissible[a]==1000{Some(totals[a]>=9760)}else{None}).collect::<Vec<_>>(),"covered_ranking_loss_combines_sq8_quantization_and_ties":true,"quantization_isolated":false,"physical_s3_or_billed_bytes":false,"miss_bucket_order":["outside_source_and_cover","source_to_shortlist_cover_loss","shortlisted_covered_ranking_loss","unshortlisted_covered_ranking_loss"],"payload_model_bytes":caller_bytes,"ephemeral_sq8_block_rows":32,"queries":queries,"local_file_only":true,"cold_latency_claim":false,"qps_claim":false,"vendor_win_claim":false,"qualified":false}),
    )
}

pub(super) fn execute(
    config: &std::path::Path,
    sha: &str,
    output: &std::path::Path,
) -> super::Result<bool> {
    super::execute_report(output, SHORTLIST_SCHEMA, || reduce_shortlist(config, sha))
}

pub(super) fn execute_admission(
    config: &std::path::Path,
    sha: &str,
    output: &std::path::Path,
) -> super::Result<bool> {
    super::execute_report(output, ADMISSION_SCHEMA, || {
        reduce_shortlist_mode(config, sha, true)
    })
}

#[cfg(test)]
mod driver_tests {
    use super::*;
    use serde_json::{Value, json};
    use sha2::{Digest, Sha256};

    fn directory() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "borsuk-shortlist-driver-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&p).unwrap();
        p
    }
    fn config_fixture(root: &std::path::Path) -> Value {
        const COMPLETED: &str = r#"
{"expected_bound_inputs":{"backend":{"bucket":"borsuk-bench-453182569524-euc1","kind":"s3","physical_prefix":"research/semantic-router/20261010/full1000-local-admission-a0004/retained-source","region":"eu-central-1","sq8_etag":"\"4875c438eb819e00fc58ac993227fb2b\"","sq8_object_key":"semantic/objects/cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1"},"cohort_receipt_sha256":"e0503d41493157a6a0a48627515e62ec452386d0496827028bb9571d406e3a17","corpus_intervals":[{"end":100000,"start":0},{"end":1001000,"start":101000}],"corpus_source_first":0,"count":1000,"credential_source":"imds_instance_role_only","dataset":"CohereLabs/wikipedia-2023-11-embed-multilingual-v3","derivation_receipt_sha256":"00e59ee26745c092f1097ba8892b4f205f7c96b48a757311aedc9072fcc3c96e","dimensions":1024,"execution":{"mode":"full"},"fetch_parallelism":64,"generation_prefix":"semantic/rebound-spatial-returned-a0001/generations/80fb1e8fdea78d72363d28f1406924084227203dd6708d596be3519155b88638","generation_root_sha256":"80fb1e8fdea78d72363d28f1406924084227203dd6708d596be3519155b88638","k":10,"max_memory_bytes":536870912,"metric":"cosine","native_order_sha256":"1060c919e34e93cc9a3d88b93eda226a8635bba41bfc60e5ba47caafb711d413","native_source_sha256":"31f28fa211f9237f0f7e41f569773cd207cefcdce22d4932ec96aee01ff2c40d","native_sq8_sha256":"cc4395af5fdbdeffcb596460aceaad0e55bdcc4af074f405e78ab2dd2be002e1","phase":"bound_inputs","producer_authority":{"executable_sha256":"4f502be7d15a064dba525816c63ad471c3ad2fe616ede14712b67cdb85e3cf66","producer_source_sha256":"a7ba542c3ada061002b09e638a68423236d5499d11aae0e0083cd6e52d38adfd","source_commit":"bc3082a8210c4370ebadae4ba093a7c211072b62","source_order_source_sha256":"1393f99e71be1e68916b598256d1200d56879b439c05d8c66c098345856c9eb4","sq8_source_sha256":"16f03aa7187b4f18676aa511339b07b6e7b8776323710a60071e6ea0d62eb795"},"profile":"scale1m","query_source_first":100000,"requests_bytes":4096000,"requests_sha256":"8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e","reserved_queries_sha256":"8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e","reserved_query_interval":{"end":101000,"start":100000},"revision":"ade45fb52bd549f5e8c065636fe4160a43c2af36","rows":1000000,"selected_count":1000,"serving":{"mode":"baseline"},"source_cache":"off","tie_rule":"corpus_ordinal_ascending","truth_bytes":80000,"truth_opened":false,"truth_sha256":"0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f","source_get_budget":128},"expected_identity":{"billed_bytes":null,"billed_requests":null,"binary_sha256":"59a22eed2e7d3cb8142e35108171cdcd8c27ec83c8968ba307cd9dc092c78083","codec_source_sha256":"eddf88c6c8ee23a732751f49291293f1cc081545ebb9a21c149eb757aa5c38ec","config_sha256":"dde73b3893fe5c71b1870f99cccb2bb667e88b57c0065998fa85dba90400ca18","execution":{"mode":"full"},"external_gate_required":true,"fetch_parallelism":64,"generation_source_sha256":"0eec4171d42d42e601ffc473027870f226342e1aae70e28efc9ced4c79f35f9d","io_measurement":"logical_GET_charges_separate_from_cumulative_process_native_transport","native_transport_includes":"S3_and_IMDS_credential_requests_including_PUT","phase":"identity","physical_s3_measured":false,"returned_source_sha256":"a4dcc7f9bc06cdea835f72860f3d2e796fcb6f29276858c349d624884cc9f74b","router_source_sha256":"a8b6dbc74ea15b08d15499bcf83cab680c17fcc040840e422818b505a8758cd8","runner_source_sha256":"27e893f926f1ce6c56738505795f5afff1384ed595300a3f5a2c08105407ecd3","s3_credential_source":"imds_instance_role_only","schema":"borsuk-cohere-native-baseline-result-v8","scope":"AUTHENTICATED_NATIVE_QUALITY_CORRECTNESS","serving":{"mode":"baseline"},"source_plane_source_sha256":"dbcc4cdbc4bb5c244354b375afd657f8b5cb1892df3f0ff0b8f41ef580de42e0","sq8_range_source_sha256":"bf31d4e0b6e53c9e7da88ab8a5d6d27a748d2492d7a915e7df7eb131c41c45e2","truth_opened":false,"unread_bytes":null,"wire_bytes":null,"source_get_budget":128},"input":{"path":"/mnt/borsuk-source-width64/results/admission-width64.jsonl","bytes":0,"sha256":"PENDING_CLOSED_NATIVE_WHOLE_FILE_PIN"},"schema":"borsuk-completed-native-reduction-config-v4"}
"#;
        let mut completed: Value = serde_json::from_str(COMPLETED).unwrap();
        completed["input"] = json!({"path":root.join("baseline.jsonl"),"bytes":35179156,"sha256":"2b79e015f05de4dc077de099ed423e541efbeeb3d42c93e0a8c6c25a6939d836"});
        let inputs = completed["expected_bound_inputs"].clone();
        let prefix = inputs["generation_prefix"].as_str().unwrap();
        let generation = root.join(prefix);
        let pin = |path: std::path::PathBuf, bytes: u64, sha: Value| json!({"path":path,"bytes":bytes,"sha256":sha});
        json!({"schema":"borsuk-sq8-subpage-shortlist-config-v1","completed":completed,
            "generation_manifest":pin(generation.join("manifest.json"),27501,inputs["generation_root_sha256"].clone()),
            "plane_manifest":pin(generation.join("plane/manifest.json"),653,json!("e0e3af73fd40db57df54a453ede016b20e4fb5d63e3a075685c82132474467c6")),
            "records":pin(generation.join("plane/records.bin"),264000000,json!("e61ffa621fedc44d3cea19ed6c47a73222a9b33c2e567567d21c048cdc826e90")),
            "mean":pin(generation.join("plane/mean.bin"),4096,json!("7875df6ca5726ad2baf2d5565deac6f6db2fadfdadc22d1bb648aea369b30985")),
            "source_digests":pin(generation.join("plane/page_digests.bin"),1000000,json!("63c07feff1b370753008a4ed6e1bc05cd9844ef7a411b22ccc1e97a76ce9aa41")),
            "sq8":pin(root.join("sq8.bin"),1036000000,inputs["native_sq8_sha256"].clone()),
            "requests":pin(root.join("requests.f32"),4096000,inputs["requests_sha256"].clone()),
            "truth":pin(root.join("truth.u64"),80000,inputs["truth_sha256"].clone()),
            "store_root":root,"scratch_parent":root,"shortlist_sizes":[1024,2048,4096,8192],"max_payload_bytes":2147483648_u64})
    }
    fn write_config(root: &std::path::Path, value: &Value) -> (std::path::PathBuf, String) {
        let path = root.join("config.json");
        let body = serde_json::to_vec(value).unwrap();
        std::fs::write(&path, &body).unwrap();
        (path, format!("{:x}", Sha256::digest(&body)))
    }
    #[test]
    fn shortlist_driver_fixed_input_and_unknown_field_refusals_precede_baseline_open() {
        let root = directory();
        std::fs::write(root.join("baseline.jsonl"), b"BASELINE_OPEN_TRIPWIRE").unwrap();
        let base = config_fixture(&root);
        let (path, sha) = write_config(&root, &base);
        assert!(
            reduce_shortlist(&path, &sha)
                .unwrap_err()
                .to_string()
                .contains("expected result bytes"),
            "literal production metadata should reach the baseline opener"
        );
        assert!(
            reduce_shortlist_mode(&path, &sha, true)
                .unwrap_err()
                .to_string()
                .contains("expected result bytes")
        );
        let mut negatives = Vec::new();
        for (field, value) in [
            ("rows", json!(100000)),
            ("dimensions", json!(768)),
            ("count", json!(32)),
            ("k", json!(100)),
            ("metric", json!("l2")),
            ("profile", json!("native100k")),
            ("source_get_budget", json!(96)),
        ] {
            let mut c = base.clone();
            c["completed"]["expected_bound_inputs"][field] = value;
            negatives.push(c);
        }
        for (field, value) in [
            ("schema", json!("wrong")),
            ("max_payload_bytes", json!(1)),
            ("shortlist_sizes", json!([1024, 2048, 4096, 16384])),
            ("unexpected", json!(true)),
        ] {
            let mut c = base.clone();
            c[field] = value;
            negatives.push(c);
        }
        let mut c = base.clone();
        c["generation_manifest"]["sha256"] = json!("0".repeat(64));
        negatives.push(c);
        let mut c = base.clone();
        c["records"]["path"] = json!(root.join("foreign.bin"));
        negatives.push(c);
        for header in ["expected_identity", "expected_bound_inputs"] {
            for width in [16, 32, 128] {
                let mut c = base.clone();
                c["completed"][header]["fetch_parallelism"] = json!(width);
                negatives.push(c);
            }
            let mut c = base.clone();
            c["completed"][header]["shared_byte_window"] = json!({"enabled":true});
            negatives.push(c);
        }
        for role in [
            "records",
            "mean",
            "source_digests",
            "sq8",
            "requests",
            "truth",
        ] {
            let mut c = base.clone();
            c[role]["bytes"] = json!(1);
            negatives.push(c);
        }
        for c in negatives {
            let (path, sha) = write_config(&root, &c);
            let error = reduce_shortlist(&path, &sha).unwrap_err().to_string();
            let admission_error = reduce_shortlist_mode(&path, &sha, true)
                .unwrap_err()
                .to_string();
            assert_eq!(
                admission_error, error,
                "admission and curve must share pre-open refusals"
            );
            assert!(
                !error.contains("expected result bytes"),
                "invalid metadata reached original baseline: {error}"
            );
            assert!(
                !error.contains("No such file"),
                "invalid metadata reached an artifact opener: {error}"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn shortlist_driver_invalid_report_is_durable_and_existing_output_is_preserved() {
        let root = directory();
        let (path, sha) = write_config(&root, &json!({"schema":"wrong"}));
        let output = root.join("report.json");
        assert!(!execute(&path, &sha, &output).unwrap());
        let body = std::fs::read(&output).unwrap();
        let report: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(report["status"], "INVALID");
        assert_eq!(report["complete"], false);
        assert_eq!(report["qualified"], false);
        assert_eq!(report["vendor_or_scientific_win_claim"], false);
        assert!(execute(&path, &sha, &output).is_err());
        assert_eq!(std::fs::read(&output).unwrap(), body);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[derive(Debug, Default, serde::Serialize)]
struct ArmQuality {
    hits10: usize,
    gained: usize,
    lost: usize,
    bridge_rescued_hits: usize,
    block_expansion_rescued_hits: usize,
    shortlist_truth_rows: usize,
    selected_block_truth_rows: usize,
    miss_buckets: [usize; 4],
}

#[allow(clippy::too_many_arguments)]
fn assess_arm(
    arm: &ShortlistArm,
    truth_ids: &[u64],
    baseline: &[super::Hit],
    positions: &std::collections::BTreeMap<u64, usize>,
    scored_units: &[usize],
    rows: usize,
    dimensions: usize,
) -> super::Result<Option<ArmQuality>> {
    let Some(returned) = &arm.returned else {
        return Ok(None);
    };
    super::require(
        truth_ids.len() == super::K && returned.len() == super::K,
        "shortlist full-k accounting",
    )?;
    let width = dimensions + 12;
    let covered = |physical: usize| {
        arm.ranges
            .iter()
            .any(|r| r.start / width <= physical && physical < r.end / width)
    };
    let mut seen = std::collections::BTreeSet::new();
    for point in returned {
        super::require(
            point.id >= 0
                && (point.id as usize) < rows
                && point.score.is_finite()
                && seen.insert(point.id),
            "shortlist returned ID/score/uniqueness",
        )?;
        let physical = *positions
            .get(&(point.id as u64))
            .ok_or("shortlist returned mapping")?;
        super::require(
            point.ordinal == physical && covered(physical),
            "shortlist returned physical ordinal/cover",
        )?;
    }
    let blocks = arm
        .selected
        .iter()
        .map(|r| r / 32)
        .collect::<std::collections::BTreeSet<_>>();
    let mut q = ArmQuality::default();
    let mut original_hits = 0;
    for id in truth_ids {
        let physical = *positions.get(id).ok_or("shortlist truth mapping")?;
        let original = baseline.iter().any(|h| h.id == *id);
        let found = returned.iter().any(|h| h.id as u64 == *id);
        let selected_row = arm.selected.binary_search(&physical).is_ok();
        let selected_block = blocks.contains(&(physical / 32));
        q.shortlist_truth_rows += usize::from(selected_row);
        q.selected_block_truth_rows += usize::from(selected_block);
        original_hits += usize::from(original);
        q.hits10 += usize::from(found);
        q.gained += usize::from(found && !original);
        q.lost += usize::from(original && !found);
        if found {
            q.bridge_rescued_hits += usize::from(!selected_block);
            q.block_expansion_rescued_hits += usize::from(selected_block && !selected_row);
        } else if !covered(physical) {
            q.miss_buckets[usize::from(scored_units.binary_search(&(physical / 32)).is_ok())] += 1;
        } else {
            q.miss_buckets[if selected_row { 2 } else { 3 }] += 1;
        }
    }
    super::require(
        q.hits10 + q.miss_buckets.iter().sum::<usize>() == super::K
            && original_hits + q.gained == q.hits10 + q.lost,
        "shortlist disjoint loss and gain conservation",
    )?;
    Ok(Some(q))
}

fn candidate_report(
    arm: &ShortlistArm,
    rows: usize,
    dimensions: usize,
    quality: Option<ArmQuality>,
) -> super::Result<serde_json::Value> {
    super::require(
        arm.returned.is_some() == quality.is_some(),
        "shortlist measured quality state",
    )?;
    let blocks = arm
        .selected
        .iter()
        .map(|r| r / 32)
        .collect::<std::collections::BTreeSet<_>>();
    let nominated_bytes = blocks
        .iter()
        .try_fold(0_usize, |sum, p| {
            sum.checked_add((((p + 1) * 32).min(rows) - p * 32) * (dimensions + 12))
        })
        .ok_or("nominated byte overflow")?;
    let bridge_bytes = arm
        .bytes
        .checked_sub(nominated_bytes)
        .ok_or("shortlist bridge byte invariant")?;
    let mut previous = None;
    let mut runs = 0;
    for &block in &blocks {
        if previous.is_none_or(|p| p + 1 != block) {
            runs += 1;
        }
        previous = Some(block);
    }
    Ok(
        serde_json::json!({"size":arm.maximum,"status":if quality.is_some(){"MEASURED"}else{"BYTE_CAP_EXCEEDED_GEOMETRY_ONLY"},
        "gets":arm.ranges.len(),"physical_bytes":arm.bytes,"selected_rows":arm.selected.len(),
        "selected_blocks":blocks.len(),"pre_cap_runs":runs,"bridge_bytes":bridge_bytes,"quality":quality}),
    )
}

#[cfg(test)]
mod report_tests {
    use super::*;
    use borsuk::sq8_page_authority::{PageAuthority, SelectedSq8Limits, plan_selected_sq8_rows};
    use serde_json::json;
    use sha2::{Digest, Sha256};
    #[test]
    fn shortlist_overcap_geometry_has_unknown_quality_and_bad_mapping_is_refused() {
        let rows = 1_000_000_usize;
        let dimensions = 1024;
        let sidecar = vec![0_u8; rows.div_ceil(32) * 32];
        let manifest=serde_json::to_vec(&json!({"schema":"borsuk-v115-sq8-page-authority-v2","generation":1,"rows":rows,"dimensions":dimensions,"page_rows":32,"object_sha256":"0".repeat(64),"page_digest_sha256":format!("{:x}",Sha256::digest(&sidecar))})).unwrap();
        let authority = PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &sidecar,
        )
        .unwrap();
        let selected = (0..1024).map(|n| n * 960).collect::<Vec<_>>();
        let plan = plan_selected_sq8_rows(
            &authority,
            &selected,
            SelectedSq8Limits {
                max_gets: CANDIDATE_SQ8_GETS,
                max_bytes: usize::MAX,
                max_planner_bytes: 4 * 1024 * 1024 + 4096,
            },
        )
        .unwrap();
        assert!(plan.planned_bytes > 32 * 1024 * 1024);
        let arm = ShortlistArm {
            maximum: 1024,
            selected,
            ranges: plan.ranges,
            bytes: plan.planned_bytes,
            returned: None,
        };
        let quality = assess_arm(
            &arm,
            &[],
            &[],
            &std::collections::BTreeMap::new(),
            &[],
            rows,
            dimensions,
        )
        .unwrap();
        assert!(quality.is_none());
        let report = candidate_report(&arm, rows, dimensions, quality).unwrap();
        assert_eq!(report["status"], "BYTE_CAP_EXCEEDED_GEOMETRY_ONLY");
        assert!(report["quality"].is_null());
        assert_eq!(report["gets"], CANDIDATE_SQ8_GETS);
        assert_eq!(report["pre_cap_runs"], 1024);
        assert!(report["bridge_bytes"].as_u64().unwrap() <= arm.bytes as u64);
        let ids = (0..10_u64).collect::<Vec<_>>();
        let baseline = ids
            .iter()
            .map(|id| super::super::Hit {
                id: *id,
                score_bits: 0,
            })
            .collect::<Vec<_>>();
        let mut positions = std::collections::BTreeMap::new();
        for id in &ids {
            positions.insert(*id, *id as usize);
        }
        positions.insert(50, 33);
        let returned = (0..9)
            .map(|id| borsuk::exact_sq8_nominee::ScoredNominee {
                ordinal: id as usize,
                id,
                score: id as f32,
            })
            .chain([borsuk::exact_sq8_nominee::ScoredNominee {
                ordinal: 33,
                id: 50,
                score: 10.,
            }])
            .collect::<Vec<_>>();
        let mut arm = ShortlistArm {
            maximum: 2,
            selected: vec![0, 64],
            ranges: vec![0..65 * 14],
            bytes: 65 * 14,
            returned: Some(returned),
        };
        let quality = assess_arm(&arm, &ids, &baseline, &positions, &[0, 1, 2], 65, 2)
            .unwrap()
            .unwrap();
        assert_eq!(quality.shortlist_truth_rows, 1);
        assert_eq!(quality.selected_block_truth_rows, 10);
        assert_eq!(quality.block_expansion_rescued_hits, 8);
        assert_eq!(quality.bridge_rescued_hits, 0);
        assert_eq!(quality.hits10, 9);
        assert_eq!(quality.lost, 1);
        assert_eq!(quality.miss_buckets, [0, 0, 0, 1]);
        arm.returned.as_mut().unwrap()[0].ordinal = 1;
        assert!(
            assess_arm(&arm, &ids, &baseline, &positions, &[0, 1, 2], 65, 2)
                .unwrap_err()
                .to_string()
                .contains("physical ordinal/cover")
        );
    }
}

fn admit_payloads(
    requests: &[u8],
    truth: &[u8],
    rows: usize,
    dimensions: usize,
    count: usize,
    codec: &borsuk::rotated_two_bit::RotatedTwoBitCodec,
) -> super::Result<()> {
    use super::require;
    require(
        rows >= super::K && (1..=1024).contains(&dimensions) && (1..=1000).contains(&count),
        "shortlist admission payload geometry",
    )?;
    let request_bytes = count
        .checked_mul(dimensions)
        .and_then(|n| n.checked_mul(4))
        .ok_or("shortlist admission request byte overflow")?;
    let truth_bytes = count
        .checked_mul(super::K)
        .and_then(|n| n.checked_mul(8))
        .ok_or("shortlist admission truth byte overflow")?;
    require(
        requests.len() == request_bytes && truth.len() == truth_bytes,
        "shortlist admission exact request/truth bytes",
    )?;
    let scratch =
        borsuk::rotated_two_bit::RotatedTwoBitCodec::required_query_scratch_bytes(dimensions)?;
    let mut query = Vec::new();
    query.try_reserve_exact(dimensions)?;
    for row in requests.chunks_exact(dimensions * 4) {
        query.clear();
        query.extend(
            row.chunks_exact(4)
                .map(|b| f32::from_le_bytes(b.try_into().unwrap())),
        );
        borsuk::two_bit_generation::normalize_two_bit_diagnostic_query(&query)?;
        codec.prepare_query(&query, scratch)?;
    }
    for row in truth.chunks_exact(super::K * 8) {
        let mut ids = [0_u64; super::K];
        for (id, bytes) in ids.iter_mut().zip(row.chunks_exact(8)) {
            *id = u64::from_le_bytes(bytes.try_into().unwrap());
        }
        require(
            ids.iter().all(|id| *id < rows as u64),
            "shortlist admission truth ID domain",
        )?;
        ids.sort_unstable();
        require(
            ids.windows(2).all(|w| w[0] != w[1]),
            "shortlist admission duplicate truth ID",
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod admission_tests {
    use super::admit_payloads;
    use borsuk::rotated_two_bit::RotatedTwoBitCodec;

    #[test]
    fn shortlist_admission_validates_all_query_and_truth_rows() {
        let codec = RotatedTwoBitCodec::new(&[0., 0.], 7).unwrap();
        let requests: Vec<u8> = [1_f32, 0., 0., 1.]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect();
        let truth: Vec<u8> = (0_u64..20).flat_map(u64::to_le_bytes).collect();
        admit_payloads(&requests, &truth, 64, 2, 2, &codec)
            .expect("valid native fixture requests and truth must be admitted");

        for value in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            for index in [0, 3] {
                let mut bad = requests.clone();
                bad[index * 4..(index + 1) * 4].copy_from_slice(&value.to_le_bytes());
                assert!(admit_payloads(&bad, &truth, 64, 2, 2, &codec).is_err());
            }
        }
        let mut zero_last = requests.clone();
        zero_last[8..16].fill(0);
        assert!(admit_payloads(&zero_last, &truth, 64, 2, 2, &codec).is_err());
        for len in [0, 1, 15] {
            assert!(admit_payloads(&requests[..len], &truth, 64, 2, 2, &codec).is_err());
        }
        let mut extra_request = requests.clone();
        extra_request.push(0);
        assert!(admit_payloads(&extra_request, &truth, 64, 2, 2, &codec).is_err());
        for len in [0, 1, 159] {
            assert!(admit_payloads(&requests, &truth[..len], 64, 2, 2, &codec).is_err());
        }
        let mut extra_truth = truth.clone();
        extra_truth.push(0);
        assert!(admit_payloads(&requests, &extra_truth, 64, 2, 2, &codec).is_err());
        let mut out_of_domain = truth.clone();
        out_of_domain[19 * 8..20 * 8].copy_from_slice(&64_u64.to_le_bytes());
        assert!(admit_payloads(&requests, &out_of_domain, 64, 2, 2, &codec).is_err());
        let mut duplicate_last = truth.clone();
        duplicate_last[19 * 8..20 * 8].copy_from_slice(&18_u64.to_le_bytes());
        assert!(admit_payloads(&requests, &duplicate_last, 64, 2, 2, &codec).is_err());
        for (rows, dimensions, count) in [(0, 2, 2), (64, 0, 2), (64, 2, 0), (64, 1025, 2)] {
            assert!(admit_payloads(&requests, &truth, rows, dimensions, count, &codec).is_err());
        }
        assert!(admit_payloads(&requests, &truth, 64, 2, usize::MAX, &codec).is_err());
    }

    #[test]
    fn shortlist_admission_publisher_requires_ready_complete_and_no_science() {
        use serde_json::json;
        let root = std::env::temp_dir().join(format!(
            "borsuk-admission-publisher-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        // Synthetic publisher-only fixture, never actual-input admission evidence.
        let ready = json!({"schema":super::ADMISSION_SCHEMA,"status":"READY_ACTUAL_INPUT",
            "complete":true,"candidate_queries_executed":0,"scientific_qualified":false,"mock_fixture":true});
        let out = root.join("ready.json");
        assert!(
            super::super::execute_report(&out, super::ADMISSION_SCHEMA, || Ok(ready.clone()))
                .unwrap()
        );
        let bytes = std::fs::read(&out).unwrap();
        assert!(
            super::super::execute_report(&out, super::ADMISSION_SCHEMA, || Ok(ready.clone()))
                .is_err()
        );
        assert_eq!(std::fs::read(&out).unwrap(), bytes);
        for (index, field, value) in [
            (0, "status", json!("MEASURED")),
            (1, "complete", json!(false)),
            (2, "candidate_queries_executed", json!(1)),
            (3, "scientific_qualified", json!(true)),
            (4, "schema", json!("wrong")),
        ] {
            let mut bad = ready.clone();
            bad[field] = value;
            assert!(
                !super::super::execute_report(
                    &root.join(format!("bad-{index}.json")),
                    super::ADMISSION_SCHEMA,
                    || Ok(bad)
                )
                .unwrap()
            );
        }
        let invalid = root.join("invalid.json");
        assert!(
            !super::execute_admission(&root.join("missing-config"), &"0".repeat(64), &invalid)
                .unwrap()
        );
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&invalid).unwrap()).unwrap();
        assert_eq!(report["status"], "INVALID");
        assert_eq!(report["complete"], false);
        assert_eq!(report["vendor_or_scientific_win_claim"], false);
        std::fs::remove_dir_all(root).unwrap();
    }
}

// Diagnostic-only representation adapter: preserve store metadata/preconditions,
// and stream the requested file range through object_store's bounded 8KiB reader.
// The production range verifier still rejects unadapted File payloads.
use object_store::ObjectStore as _;

#[derive(Debug)]
struct DiagnosticLocalStreamStore {
    inner: object_store::local::LocalFileSystem,
}

impl std::fmt::Display for DiagnosticLocalStreamStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("diagnostic-local-stream")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for DiagnosticLocalStreamStore {
    async fn get_opts(
        &self,
        path: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let result = self.inner.get_opts(path, options).await?;
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let extensions = result.extensions.clone();
        use futures_util::StreamExt;
        let stream = result
            .into_stream()
            .map(|chunk| match chunk {
                Ok(bytes) if bytes.is_empty() => Err(object_store::Error::Generic {
                    store: "diagnostic-local-stream",
                    source: std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "empty local range chunk before EOF",
                    )
                    .into(),
                }),
                other => other,
            })
            .boxed();

        Ok(object_store::GetResult {
            payload: object_store::GetResultPayload::Stream(stream),
            meta,
            range,
            attributes,
            extensions,
        })
    }

    async fn put_opts(
        &self,
        path: &object_store::path::Path,
        payload: object_store::PutPayload,
        options: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &object_store::path::Path,
        options: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(path, options).await
    }

    fn delete_stream(
        &self,
        paths: futures_util::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
    {
        self.inner.delete_stream(paths)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
    {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn diagnostic_local_store(root: &str) -> object_store::Result<DiagnosticLocalStreamStore> {
    Ok(DiagnosticLocalStreamStore {
        inner: object_store::local::LocalFileSystem::new_with_prefix(root)?,
    })
}

#[cfg(test)]
mod local_stream_tests {
    use super::*;
    use futures_util::TryStreamExt;
    use object_store::{ObjectStore, ObjectStoreExt};
    use sha2::{Digest, Sha256};

    #[test]
    fn shortlist_local_range_stream_preserves_bounds_metadata_and_authentication() {
        let root = std::env::temp_dir().join(format!(
            "shortlist-local-stream-{}",
            format!(
                "{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )
        ));
        std::fs::create_dir(&root).unwrap();
        let rows = 2048_usize;
        let dimensions = 2_usize;
        let width = dimensions + 12;
        let mut object = Vec::new();
        for row in 0..rows {
            object.extend_from_slice(&(row as i64).to_le_bytes());
            object.extend_from_slice(&1_f32.to_le_bytes());
            object.extend_from_slice(&[1, 0]);
        }
        std::fs::write(root.join("object.bin"), &object).unwrap();
        let digests = object
            .chunks(32 * width)
            .flat_map(|body| Sha256::digest(body).to_vec())
            .collect::<Vec<_>>();
        let manifest = serde_json::to_vec(&serde_json::json!({"schema":"borsuk-v115-sq8-page-authority-v2","generation":1,"rows":rows,"dimensions":dimensions,"page_rows":32,"object_sha256":format!("{:x}",Sha256::digest(&object)),"page_digest_sha256":format!("{:x}",Sha256::digest(&digests))})).unwrap();
        let authority = borsuk::sq8_page_authority::PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &digests,
        )
        .unwrap();
        let location = object_store::path::Path::parse("object.bin").unwrap();
        let bare = object_store::local::LocalFileSystem::new_with_prefix(&root).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let meta = runtime.block_on(bare.head(&location)).unwrap();
        let etag = meta.e_tag.as_deref().unwrap();
        let budget =
            borsuk::sq8_s3_range::InFlightBytes::with_waiter_limit(32 * width, 1, 4096).unwrap();
        let bare_error = runtime
            .block_on(
                borsuk::sq8_s3_range::fetch_verified_ranges_with_byte_window(
                    &bare,
                    &location,
                    &authority,
                    &[(0, 0)],
                    etag,
                    1,
                    32 * width,
                    1,
                    &budget,
                ),
            )
            .err()
            .expect("expected strict reader refusal");
        assert!(matches!(
            bare_error.error,
            borsuk::sq8_s3_range::RangeFetchError::UnexpectedMetadata
        ));
        assert_eq!(bare_error.stats.submitted_gets, 1);
        assert_eq!(bare_error.stats.verified_bytes, 0);
        let store = diagnostic_local_store(root.to_str().unwrap()).unwrap();
        let result = runtime
            .block_on(
                borsuk::sq8_s3_range::fetch_verified_ranges_with_byte_window(
                    &store,
                    &location,
                    &authority,
                    &[(0, 0)],
                    etag,
                    1,
                    32 * width,
                    1,
                    &budget,
                ),
            )
            .expect("diagnostic local body must satisfy unchanged production stream verification");
        assert_eq!(result.0.len(), 1);
        assert_eq!(result.1.submitted_gets, 1);
        assert_eq!(result.1.verified_bytes, 32 * width);
        let headers = runtime
            .block_on(
                store.get_opts(
                    &location,
                    object_store::GetOptions::new()
                        .with_range(Some(0..(32 * width) as u64))
                        .with_if_match(Some(etag.to_owned())),
                ),
            )
            .unwrap();
        assert_eq!(headers.range, 0..(32 * width) as u64);
        assert_eq!(headers.meta.size, object.len() as u64);
        assert_eq!(headers.meta.e_tag, meta.e_tag);
        assert!(matches!(
            headers.payload,
            object_store::GetResultPayload::Stream(_)
        ));
        let bad_etag = runtime.block_on(
            store.get_opts(
                &location,
                object_store::GetOptions::new()
                    .with_range(Some(0..(32 * width) as u64))
                    .with_if_match(Some("wrong-etag".to_owned())),
            ),
        );
        assert!(matches!(
            bad_etag,
            Err(object_store::Error::Precondition { .. })
        ));
        let mut corrupted = object.clone();
        corrupted[0] ^= 1;
        std::fs::write(root.join("corrupt.bin"), corrupted).unwrap();
        let corrupt_location = object_store::path::Path::parse("corrupt.bin").unwrap();
        let corrupt_meta = runtime.block_on(store.head(&corrupt_location)).unwrap();
        let failure = runtime
            .block_on(
                borsuk::sq8_s3_range::fetch_verified_ranges_with_byte_window(
                    &store,
                    &corrupt_location,
                    &authority,
                    &[(0, 0)],
                    corrupt_meta.e_tag.as_deref().unwrap(),
                    1,
                    32 * width,
                    1,
                    &budget,
                ),
            )
            .err()
            .expect("expected strict reader refusal");
        assert!(matches!(
            failure.error,
            borsuk::sq8_s3_range::RangeFetchError::Page(_)
        ));
        // A nonzero, page-aligned range spans the 8KiB streaming boundary.
        let start = 32 * width;
        let end = 32 * width * 32;
        let multi = runtime
            .block_on(
                store.get_opts(
                    &location,
                    object_store::GetOptions::new()
                        .with_range(Some(start as u64..end as u64))
                        .with_if_match(Some(etag.to_owned())),
                ),
            )
            .unwrap();
        assert_eq!(multi.range, start as u64..end as u64);
        let chunks = runtime
            .block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    let mut body = multi.into_stream();
                    let mut chunks = Vec::new();
                    while let Some(chunk) = body.try_next().await? {
                        assert!(!chunk.is_empty() && chunk.len() <= 8192);
                        chunks.push(chunk);
                    }
                    Ok::<_, object_store::Error>(chunks)
                })
                .await
            })
            .expect("nonzero multi-chunk range must terminate")
            .unwrap();
        assert!(chunks.len() >= 2);
        let streamed = chunks
            .iter()
            .flat_map(|chunk| chunk.iter().copied())
            .collect::<Vec<_>>();
        assert_eq!(streamed, object[start..end]);

        // Headers were accepted while the file was complete; then its open
        // inode is truncated. The first body poll must promptly report EOF.
        std::fs::write(root.join("truncated.bin"), &object).unwrap();
        let truncated_location = object_store::path::Path::parse("truncated.bin").unwrap();
        let truncated_meta = runtime.block_on(store.head(&truncated_location)).unwrap();
        let admitted = runtime
            .block_on(
                store.get_opts(
                    &truncated_location,
                    object_store::GetOptions::new()
                        .with_range(Some(0..(32 * width) as u64))
                        .with_if_match(truncated_meta.e_tag),
                ),
            )
            .unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(root.join("truncated.bin"))
            .unwrap()
            .set_len(0)
            .unwrap();
        let refused = runtime
            .block_on(async {
                tokio::time::timeout(std::time::Duration::from_secs(2), async {
                    admitted.into_stream().try_next().await
                })
                .await
            })
            .expect("truncated admitted stream must promptly refuse rather than loop");
        assert!(refused.is_err());
        std::fs::remove_file(root.join("truncated.bin")).unwrap();
        std::fs::remove_file(root.join("object.bin")).unwrap();
        std::fs::remove_file(root.join("corrupt.bin")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }
}
