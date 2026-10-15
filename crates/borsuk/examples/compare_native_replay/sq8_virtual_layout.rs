//! SOURCE-preserving SQ8 placement and accounting for the offline falsifier.
#![allow(dead_code)]
use crate::unit_layout::{SourceRow, Sq8Row, UnitLayout};
use borsuk::sq8_page_authority::{
    PageAuthority, SelectedSq8Limits, SelectedSq8Plan, plan_selected_sq8_rows,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Seek, SeekFrom},
};

type Result<T> = super::super::Result<T>;
use super::super::require;
pub(super) struct MappedAdmission {
    pub(super) priority_sha256: String,
    pub(super) source_rows: Vec<SourceRow>,
    pub(super) sq8_rows: Vec<Sq8Row>,
    pub(super) plan: SelectedSq8Plan,
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .ok_or_else(|| "mapped payload overflow".into())
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b)
        .ok_or_else(|| "mapped payload overflow".into())
}

/// SOURCE scores and tie keys stay in original coordinates; only the trial
/// canonical cover uses destination rows. Borrowed inputs/layout are caller funded.
pub(super) fn admit_source_blocks(
    authority: &PageAuthority,
    layout: &UnitLayout,
    scored: &[(usize, f64)],
    nominees: &[usize],
    limits: SelectedSq8Limits,
    cap: usize,
) -> Result<MappedAdmission> {
    let (rows, unit_rows) = layout.geometry();
    require(
        rows == authority.rows() && unit_rows == authority.page_rows(),
        "mapped authority/layout geometry",
    )?;
    require(
        !nominees.is_empty()
            && nominees.len() <= 1024
            && nominees.last().is_some_and(|&r| r < rows)
            && nominees.windows(2).all(|p| p[0] < p[1]),
        "mapped SOURCE nominees",
    )?;
    let planner = add(mul(nominees.len(), 512)?, 4096)?;
    require(
        add(mul(nominees.len(), 640)?, 4096)? <= cap && planner <= limits.max_planner_bytes,
        "mapped simultaneous planner payload",
    )?;
    let mut ranked = Vec::with_capacity(nominees.len());
    for &(source, score) in scored {
        require(source < rows && score.is_finite(), "mapped SOURCE score")?;
        if nominees.binary_search(&source).is_ok() {
            require(ranked.len() < nominees.len(), "mapped bounded score count")?;
            ranked.push((source, score));
        }
    }
    require(
        ranked.len() == nominees.len(),
        "mapped complete SOURCE scores",
    )?;
    ranked.sort_unstable_by_key(|&(r, _)| r);
    require(
        ranked.windows(2).all(|p| p[0].0 < p[1].0),
        "mapped unique SOURCE scores",
    )?;
    ranked.sort_unstable_by(|&(a, sa), &(b, sb)| {
        (a / unit_rows)
            .cmp(&(b / unit_rows))
            .then(sb.total_cmp(&sa))
            .then(a.cmp(&b))
    });
    ranked.dedup_by(|a, b| a.0 / unit_rows == b.0 / unit_rows);
    ranked.sort_unstable_by(|&(a, sa), &(b, sb)| {
        sb.total_cmp(&sa)
            .then((a / unit_rows).cmp(&(b / unit_rows)))
    });
    let mut priority = Sha256::new();
    priority.update(b"BORSUK_SOURCE_PRIORITY_V1");
    for &(source, score) in &ranked {
        priority.update(u64::try_from(source / unit_rows)?.to_le_bytes());
        priority.update(score.to_bits().to_le_bytes());
    }
    let priority_sha256 = format!("{:x}", priority.finalize());
    let mut accepted = Vec::<usize>::with_capacity(ranked.len());
    for &(source, _) in &ranked {
        let destination = layout
            .sq8_row(SourceRow(source))
            .ok_or("mapped forward row")?
            .0;
        let representative = mul(destination / unit_rows, unit_rows)?;
        let index = accepted
            .binary_search(&representative)
            .err()
            .ok_or("mapped duplicate destination unit")?;
        accepted.insert(index, representative);
        match plan_selected_sq8_rows(authority, &accepted, limits) {
            Ok(_) => {}
            Err(borsuk::budgeted_page_rank::BudgetedPageError::InsufficientBudget) => {
                accepted.remove(index);
            }
            Err(e) => return Err(format!("mapped canonical planner {e:?}").into()),
        }
    }
    require(!accepted.is_empty(), "mapped no affordable unit")?;
    let mut source_rows = Vec::with_capacity(nominees.len());
    let mut sq8_rows = Vec::with_capacity(nominees.len());
    for &source in nominees {
        let destination = layout
            .sq8_row(SourceRow(source))
            .ok_or("mapped nominee row")?;
        if accepted
            .binary_search(&mul(destination.0 / unit_rows, unit_rows)?)
            .is_ok()
        {
            source_rows.push(SourceRow(source));
            sq8_rows.push(destination);
        }
    }
    sq8_rows.sort_unstable_by_key(|r| r.0);
    let target_rows = sq8_rows.iter().map(|r| r.0).collect::<Vec<_>>();
    let owned = add(
        add(
            mul(ranked.capacity(), std::mem::size_of::<(usize, f64)>())?,
            mul(accepted.capacity(), 8)?,
        )?,
        add(
            add(
                mul(source_rows.capacity(), std::mem::size_of::<SourceRow>())?,
                mul(sq8_rows.capacity(), std::mem::size_of::<Sq8Row>())?,
            )?,
            mul(target_rows.capacity(), 8)?,
        )?,
    )?;
    require(
        add(owned, planner)? <= cap,
        "mapped actual capacity payload",
    )?;
    let plan = plan_selected_sq8_rows(authority, &target_rows, limits)
        .map_err(|e| format!("mapped final cover {e:?}"))?;
    require(
        plan.ranges.len() <= limits.max_gets && plan.planned_bytes <= limits.max_bytes,
        "mapped final cap parity",
    )?;
    Ok(MappedAdmission {
        priority_sha256,
        source_rows,
        sq8_rows,
        plan,
    })
}

/// Membership discovery is always SOURCE-unit based; coverage/returned ordinals
/// belong to the destination SQ8 object. Do not materialize a second ID map.
#[allow(clippy::too_many_arguments)]
pub(super) fn assess_arm_mapped(
    arm: &super::ShortlistArm,
    truth: &[u64],
    baseline: &[crate::Hit],
    source_positions: &BTreeMap<u64, usize>,
    scored_units: &[usize],
    layout: &UnitLayout,
    dimensions: usize,
) -> Result<Option<super::ArmQuality>> {
    let Some(returned) = &arm.returned else {
        return Ok(None);
    };
    let (rows, unit_rows) = layout.geometry();
    require(
        unit_rows == 32 && dimensions > 0 && truth.len() == crate::K && returned.len() == crate::K,
        "mapped quality geometry/full-k",
    )?;
    require(
        scored_units.windows(2).all(|p| p[0] < p[1])
            && scored_units.last().is_none_or(|&u| u < rows.div_ceil(32)),
        "mapped SOURCE discovery roster",
    )?;
    require(
        arm.selected.windows(2).all(|p| p[0] < p[1])
            && arm.selected.last().is_none_or(|&r| r < rows),
        "mapped selected target roster",
    )?;
    let width = add(dimensions, 12)?;
    let covered = |physical: usize| {
        arm.ranges
            .iter()
            .any(|r| r.start / width <= physical && physical < r.end / width)
    };
    let mapped = |id: u64| -> Result<(usize, usize)> {
        let source = *source_positions
            .get(&id)
            .ok_or("mapped ID/SOURCE position")?;
        let target = layout
            .sq8_row(SourceRow(source))
            .ok_or("mapped ID/target position")?
            .0;
        Ok((source, target))
    };
    let mut seen = BTreeSet::new();
    for point in returned {
        require(
            point.id >= 0
                && (point.id as usize) < rows
                && point.score.is_finite()
                && seen.insert(point.id),
            "mapped returned ID/score/uniqueness",
        )?;
        let (_, target) = mapped(point.id as u64)?;
        require(
            point.ordinal == target && covered(target),
            "mapped returned target ordinal/cover",
        )?;
    }
    let blocks = arm
        .selected
        .iter()
        .map(|r| r / unit_rows)
        .collect::<BTreeSet<_>>();
    let mut quality = super::ArmQuality::default();
    let mut original_hits = 0;
    for &id in truth {
        let (source, target) = mapped(id)?;
        let original = baseline.iter().any(|h| h.id == id);
        let found = returned.iter().any(|h| h.id as u64 == id);
        let selected_row = arm.selected.binary_search(&target).is_ok();
        let selected_block = blocks.contains(&(target / unit_rows));
        original_hits += usize::from(original);
        quality.hits10 += usize::from(found);
        quality.gained += usize::from(found && !original);
        quality.lost += usize::from(original && !found);
        quality.shortlist_truth_rows += usize::from(selected_row);
        quality.selected_block_truth_rows += usize::from(selected_block);
        if found {
            quality.bridge_rescued_hits += usize::from(!selected_block);
            quality.block_expansion_rescued_hits += usize::from(selected_block && !selected_row);
        } else if !covered(target) {
            quality.miss_buckets
                [usize::from(scored_units.binary_search(&(source / 32)).is_ok())] += 1;
        } else {
            quality.miss_buckets[if selected_row { 2 } else { 3 }] += 1;
        }
    }
    require(
        quality.hits10 + quality.miss_buckets.iter().sum::<usize>() == crate::K
            && original_hits + quality.gained == quality.hits10 + quality.lost,
        "mapped quality conservation",
    )?;
    Ok(Some(quality))
}

/// Read/authenticate one bounded native leaf at a time. No request/truth inputs
/// exist in this interface, and no centroid plane is decoded or trained here.
pub(super) fn layout_from_router_leaves<R: Read + Seek>(
    router: &borsuk::semantic_unit_router::SemanticUnitRouter,
    input: &mut R,
    cap: usize,
) -> Result<UnitLayout> {
    let m = router.manifest();
    require(
        m.unit_rows == 32 && m.rows > 0 && m.unit_count == m.rows.div_ceil(32),
        "router layout geometry",
    )?;
    let maximum = m
        .leaves
        .iter()
        .map(|l| l.bytes)
        .max()
        .ok_or("router layout no leaves")?;
    let leaf_units = m
        .leaves
        .iter()
        .map(|l| l.unit_count)
        .max()
        .ok_or("router layout no units")?;
    let maps = UnitLayout::required_bytes(m.rows, 32)?;
    let scratch = add(mul(leaf_units, 128)?, 65536)?;
    let modeled = add(add(mul(m.unit_count, 4)?, maps)?, add(maximum, scratch)?)?;
    require(modeled <= cap, "router layout simultaneous payload")?;
    require(
        input.seek(SeekFrom::End(0))? == u64::try_from(m.leaf_payload.bytes)?,
        "router layout payload length",
    )?;
    let mut order = Vec::<u32>::new();
    order.try_reserve_exact(m.unit_count)?;
    let mut body = Vec::new();
    body.try_reserve_exact(maximum)?;
    body.resize(maximum, 0_u8);
    require(
        add(
            add(mul(order.capacity(), 4)?, maps)?,
            add(body.capacity(), scratch)?,
        )? <= cap,
        "router layout actual capacity",
    )?;
    let partial = (m.rows % 32 != 0).then_some(m.unit_count - 1);
    let mut tail_seen = false;
    let mut offset = 0_usize;
    let mut whole = Sha256::new();
    for (id, leaf) in m.leaves.iter().enumerate() {
        require(
            leaf.leaf_id == id && leaf.offset == offset && leaf.bytes <= maximum,
            "router layout contiguous leaves",
        )?;
        let stop = add(offset, leaf.bytes)?;
        require(stop <= m.leaf_payload.bytes, "router layout leaf bound")?;
        input.seek(SeekFrom::Start(u64::try_from(offset)?))?;
        input.read_exact(&mut body[..leaf.bytes])?;
        let units = router.validate_leaf(id, &body[..leaf.bytes])?;
        require(
            add(order.len(), units.len())? <= m.unit_count,
            "router layout bounded roster",
        )?;
        whole.update(&body[..leaf.bytes]);
        for unit in units {
            if Some(unit) == partial {
                require(!tail_seen, "router layout duplicate partial tail")?;
                tail_seen = true;
            } else {
                order.push(u32::try_from(unit)?);
            }
        }
        offset = stop;
    }
    require(
        offset == m.leaf_payload.bytes
            && format!("{:x}", whole.finalize()) == m.leaf_payload.sha256,
        "router layout whole payload hash",
    )?;
    if let Some(tail) = partial {
        require(tail_seen, "router layout missing partial tail")?;
        order.push(u32::try_from(tail)?);
    }
    require(
        order.len() == m.unit_count
            && input.seek(SeekFrom::End(0))? == u64::try_from(m.leaf_payload.bytes)?,
        "router layout complete stable payload",
    )?;
    Ok(UnitLayout::from_destination_order(
        m.rows, 32, &order, maps,
    )?)
}
// Prospective next driver fixture. Not in frozen07/current qualification.
// Missing-feature oracle: retaining physical-coordinate tie keys after mapping
// chooses SOURCE unit2 instead of SOURCE unit0 in this non-self-inverse cycle.
#[cfg(test)]
mod mapped_admission_tests {
    use super::*;
    use crate::unit_layout::{SourceRow, Sq8Row, UnitLayout};
    use borsuk::sq8_page_authority::{PageAuthority, SelectedSq8Limits};
    use sha2::{Digest, Sha256};

    fn target() -> PageAuthority {
        let mut bytes = Vec::new();
        for row in 0..97_i64 {
            bytes.extend_from_slice(&(1000 - row * 3).to_le_bytes());
            bytes.extend_from_slice(&1_f32.to_le_bytes());
            bytes.extend_from_slice(&[0, 0]);
        }
        let mut digests = Vec::new();
        for block in bytes.chunks(32 * 14) {
            digests.extend_from_slice(&Sha256::digest(block));
        }
        let manifest = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2","generation":1,
            "rows":97,"dimensions":2,"page_rows":32,
            "object_sha256":format!("{:x}",Sha256::digest(&bytes)),
            "page_digest_sha256":format!("{:x}",Sha256::digest(&digests))
        }))
        .unwrap();
        PageAuthority::load(
            &manifest,
            &format!("{:x}", Sha256::digest(&manifest)),
            &digests,
        )
        .unwrap()
    }
    #[test]
    fn mapped_admission_preserves_source_priority_ties_and_all_nominees_in_admitted_unit() {
        let authority = target();
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], 1 << 20).unwrap();
        let selected = admit_source_blocks(
            &authority,
            &layout,
            &[(0, 5.0), (1, 5.0), (32, 5.0), (64, 5.0), (96, 4.0)],
            &[0, 1, 32, 64, 96],
            SelectedSq8Limits {
                max_gets: 1,
                max_bytes: 448,
                max_planner_bytes: 1 << 20,
            },
            1 << 20,
        )
        .unwrap();
        assert_eq!(selected.source_rows, vec![SourceRow(0), SourceRow(1)]);
        assert_eq!(selected.sq8_rows, vec![Sq8Row(32), Sq8Row(33)]);
        assert_eq!(selected.plan.ranges, vec![448..896]);
        assert_eq!(selected.plan.planned_bytes, 448);
    }
    #[test]
    fn mapped_admission_skips_unaffordable_full_units_and_accepts_the_tail() {
        let authority = target();
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], 1 << 20).unwrap();
        let selected = admit_source_blocks(
            &authority,
            &layout,
            &[(0, 5.0), (32, 4.0), (64, 3.0), (96, 2.0)],
            &[0, 32, 64, 96],
            SelectedSq8Limits {
                max_gets: 1,
                max_bytes: 14,
                max_planner_bytes: 1 << 20,
            },
            1 << 20,
        )
        .unwrap();
        assert_eq!(selected.source_rows, vec![SourceRow(96)]);
        assert_eq!(selected.sq8_rows, vec![Sq8Row(96)]);
        assert_eq!(selected.plan.ranges, vec![1344..1358]);
    }
}
// Prospective child module of sq8_subpage_shortlist; no live-source change.
// Missing-feature oracle: using SQ8 unit2 to classify SOURCE discovery loses
// the fact that SOURCE unit1 was already discovered.
#[cfg(test)]
mod mapped_quality_tests {
    use super::super::ShortlistArm;
    use super::*;
    use crate::unit_layout::UnitLayout;
    use borsuk::exact_sq8_nominee::ScoredNominee;
    use std::collections::BTreeMap;

    #[test]
    fn mapped_quality_uses_source_discovery_and_target_physical_coverage_separately() {
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], 1 << 20).unwrap();
        let positions = (0..97)
            .map(|row| (row as u64, row))
            .collect::<BTreeMap<_, _>>();
        let truth = (32..42_u64).collect::<Vec<_>>();
        let baseline = truth
            .iter()
            .map(|&id| crate::Hit {
                id,
                score_bits: 1_f32.to_bits(),
            })
            .collect::<Vec<_>>();
        let arm = ShortlistArm {
            maximum: 1024,
            selected: (32..42).collect(),
            ranges: vec![32 * 14..64 * 14],
            bytes: 32 * 14,
            returned: Some(
                (0..10)
                    .map(|id| ScoredNominee {
                        ordinal: 32 + id,
                        id: id as i64,
                        score: 1.0,
                    })
                    .collect(),
            ),
        };
        let quality = assess_arm_mapped(&arm, &truth, &baseline, &positions, &[1], &layout, 2)
            .unwrap()
            .unwrap();
        assert_eq!(quality.hits10, 0);
        assert_eq!((quality.gained, quality.lost), (0, 10));
        assert_eq!(quality.miss_buckets, [0, 10, 0, 0]);
        assert_eq!(
            (
                quality.shortlist_truth_rows,
                quality.selected_block_truth_rows
            ),
            (0, 0)
        );
    }

    #[test]
    fn mapped_quality_refuses_source_ordinals_disguised_as_returned_target_rows() {
        let layout = UnitLayout::from_destination_order(97, 32, &[2, 0, 1, 3], 1 << 20).unwrap();
        let positions = (0..97)
            .map(|row| (row as u64, row))
            .collect::<BTreeMap<_, _>>();
        let truth = (0..10_u64).collect::<Vec<_>>();
        let baseline = truth
            .iter()
            .map(|&id| crate::Hit {
                id,
                score_bits: 1_f32.to_bits(),
            })
            .collect::<Vec<_>>();
        let arm = ShortlistArm {
            maximum: 1024,
            selected: (0..10).collect(),
            ranges: vec![0..32 * 14],
            bytes: 32 * 14,
            returned: Some(
                (0..10)
                    .map(|id| ScoredNominee {
                        ordinal: id,
                        id: id as i64,
                        score: 1.0,
                    })
                    .collect(),
            ),
        };
        assert!(assess_arm_mapped(&arm, &truth, &baseline, &positions, &[0], &layout, 2).is_err());
    }
}
// Prospective query-free preparation fixture. Not in current a655 run.
#[cfg(test)]
mod router_payload_order_tests {
    use super::*;
    use crate::unit_layout::{SourceRow, Sq8Row};
    use borsuk::{
        semantic_unit_router::{self, SemanticProfile, SemanticUnitRouter, SourceIdentity},
        unit_centroid_pages::UnitCentroidPages,
    };
    use sha2::{Digest, Sha256};
    use std::io::Cursor;

    fn actual_router() -> (SemanticUnitRouter, Vec<u8>) {
        let mut sq8 = Vec::new();
        // Four unit means [0,1],[3,1],[2,1],[1,1] around [1.5,1].
        // Actual native radial payload order is 2,3,0,1; membership is 0,0,0,0.
        for row in 0..97_usize {
            sq8.extend_from_slice(&((row as i64 * 13) - 1000).to_le_bytes());
            sq8.extend_from_slice(&1_f32.to_le_bytes());
            sq8.extend_from_slice(&[[0, 1], [3, 1], [2, 1], [1, 1]][row / 32]);
        }
        let centroids = UnitCentroidPages::build_from_sq8_reader(
            &mut Cursor::new(sq8),
            97,
            2,
            32,
            256,
            &[0.0, 0.0],
            &[1.0, 1.0],
        )
        .unwrap();
        let centroid_sha = format!("{:x}", Sha256::digest(&centroids));
        let root_sha = "1".repeat(64);
        let identity = SourceIdentity {
            profile: SemanticProfile::Native100k,
            schema: "unit-locality-fixture",
            root_sha256: &root_sha,
            centroids_sha256: &centroid_sha,
            rows: 97,
            dimensions: 2,
        };
        let artifacts = semantic_unit_router::build(
            &centroids,
            &identity,
            SemanticProfile::Native100k.allocation_cap(),
        )
        .unwrap();
        let hash = format!("{:x}", Sha256::digest(&artifacts.manifest));
        let router = SemanticUnitRouter::open(
            &artifacts.manifest,
            &artifacts.membership,
            &hash,
            &identity,
            1 << 20,
        )
        .unwrap();
        assert_eq!(router.manifest().leaves.len(), 1);
        assert_eq!(router.unit_membership(), &[0, 0, 0, 0]);
        assert_eq!(
            router.validate_leaf(0, &artifacts.leaves).unwrap(),
            vec![2, 3, 0, 1]
        );
        (router, artifacts.leaves)
    }

    #[test]
    fn router_layout_uses_authenticated_payload_order_and_moves_only_partial_tail_last() {
        let (router, leaves) = actual_router();
        let layout = layout_from_router_leaves(&router, &mut Cursor::new(leaves), 1 << 20).unwrap();
        // Literal three-cycle after removing partial unit3 and appending it.
        for (target, source) in [2, 0, 1, 3].into_iter().enumerate() {
            assert_eq!(
                layout.source_row(Sq8Row(target * 32)),
                Some(SourceRow(source * 32))
            );
        }
    }
    #[test]
    fn router_layout_refuses_corrupt_or_trailing_leaf_payloads() {
        let (router, leaves) = actual_router();
        let mut corrupt = leaves.clone();
        corrupt[4] ^= 1;
        assert!(layout_from_router_leaves(&router, &mut Cursor::new(corrupt), 1 << 20).is_err());
        let mut extra = leaves;
        extra.push(0);
        assert!(layout_from_router_leaves(&router, &mut Cursor::new(extra), 1 << 20).is_err());
    }
}

pub(super) const EXTRA_PAYLOAD: usize = 128 * 1024 * 1024;
const QUERY_PAYLOAD: usize = 64 * 1024 * 1024;
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    schema: String,
    pub(super) shortlist: super::ShortlistConfig,
    pub(super) selector_control: crate::CompletedInput,
}
pub(super) fn read_config(path: &std::path::Path, sha: &str) -> Result<(Config, u64)> {
    let (config, n): (Config, _) = crate::read_config(path, sha)?;
    require(
        config.schema == "borsuk-sq8-virtual-layout-config-v1",
        "virtual config schema",
    )?;
    require(
        config.selector_control.bytes > 0 && config.selector_control.bytes <= 1024 * 1024,
        "virtual closed-control report bound",
    )?;
    Ok((config, n))
}
fn text<'a>(v: &'a serde_json::Value, key: &str) -> Result<&'a str> {
    v[key]
        .as_str()
        .ok_or_else(|| format!("virtual descriptor string {key}").into())
}
fn row_digest(rows: impl Iterator<Item = usize>, tag: &[u8]) -> Result<String> {
    let mut h = Sha256::new();
    h.update(tag);
    for row in rows {
        h.update(u64::try_from(row)?.to_le_bytes());
    }
    Ok(format!("{:x}", h.finalize()))
}
pub(super) struct Context {
    pub(super) identity: UnitLayout,
    pub(super) relocated: crate::virtual_sq8::VirtualSq8,
    sq8: crate::HeaderSnapshot,
    snapshots: Vec<crate::HeaderSnapshot>,
    control: crate::CompletedInput,
    control_snapshot: Option<crate::HeaderSnapshot>,
    pub(super) descriptor: serde_json::Value,
}
impl Context {
    /// This phase has no request, truth or baseline-file open. Metadata-only
    /// config fields supply the pinned original generation/source/layout binding.
    pub(super) fn prepare(
        c: &super::ShortlistConfig,
        control: crate::CompletedInput,
    ) -> Result<Self> {
        use borsuk::semantic_unit_router::{SemanticProfile, SemanticUnitRouter, SourceIdentity};
        let i: crate::Inputs = serde_json::from_value(c.completed.expected_bound_inputs.clone())?;
        crate::validate_scale_inputs(&i, &c.completed.expected_bound_inputs)?;
        require(
            c.schema == "borsuk-sq8-subpage-shortlist-config-v1"
                && i.rows == 1_000_000
                && i.dimensions == 1024
                && i.count == 1000
                && i.k == 10,
            "virtual benchmark geometry/config",
        )?;
        require(
            c.max_payload_bytes > EXTRA_PAYLOAD && control.bytes <= 1024 * 1024,
            "virtual caller/control payload",
        )?;
        let directory = c.store_root.join(&i.generation_prefix);
        require(
            c.generation_manifest.path == directory.join("manifest.json")
                && c.plane_manifest.path == directory.join("plane/manifest.json")
                && c.generation_manifest.sha256 == i.generation_root_sha256
                && c.sq8.sha256 == i.native_sq8_sha256,
            "virtual original generation paths/pins",
        )?;
        require(
            c.plane_manifest.bytes > 0
                && c.plane_manifest.bytes <= 65536
                && c.generation_manifest.bytes <= 65536
                && c.sq8.bytes == u64::try_from(mul(i.rows, add(i.dimensions, 12)?)?)?,
            "virtual bounded original metadata/SQ8",
        )?;
        let (g, gs) = crate::header_json(&c.generation_manifest)?;
        let (plane_bytes, ps) = crate::loss_binary(&c.plane_manifest, c.plane_manifest.bytes)?;
        let plane: borsuk::two_bit_source::SourcePlaneReceipt =
            serde_json::from_slice(&plane_bytes)?;
        let d = &g["discovery"];
        require(
            g["schema"] == "borsuk-two-bit-generation-v8"
                && d["mode"] == "semantic"
                && g["plane_manifest_sha256"] == c.plane_manifest.sha256
                && g["sq8_object_sha256"] == c.sq8.sha256
                && plane.rows == i.rows
                && plane.dimensions == i.dimensions
                && d["source_sha256"] == plane.source_sha256
                && d["source_order_sha256"] == plane.source_order_sha256
                && d["mean_sha256"] == plane.mean_sha256
                && d["records_sha256"] == plane.records_sha256
                && d["sq8_sha256"] == plane.sq8_sha256
                && plane.source_order_sha256 == i.native_order_sha256
                && plane.source_sha256 == i.native_source_sha256
                && plane.sq8_sha256 == i.native_sq8_sha256,
            "virtual authenticated construction binding",
        )?;
        let profile: SemanticProfile = serde_json::from_value(d["profile"].clone())?;
        let root_bytes = usize::try_from(d["root_bytes"].as_u64().ok_or("virtual router bytes")?)?;
        require(root_bytes <= profile.root_cap(), "virtual router cap")?;
        let member_bytes = mul(i.rows.div_ceil(32), 4)?;
        let leaves_bytes = mul(i.rows.div_ceil(32), add(4, mul(i.dimensions, 2)?)?)?;
        require(
            d["membership_bytes"] == member_bytes && d["leaves_bytes"] == leaves_bytes,
            "virtual router descriptor lengths",
        )?;
        let pin = |name: &str, bytes: usize, hash: &str| -> Result<crate::CompletedInput> {
            require(crate::valid_sha(hash), "virtual router artifact SHA")?;
            Ok(crate::CompletedInput {
                path: directory.join(name),
                bytes: u64::try_from(bytes)?,
                sha256: hash.to_owned(),
            })
        };
        let root_pin = pin("router/root.bin", root_bytes, text(d, "root_sha256")?)?;
        let member_pin = pin(
            "router/membership.bin",
            member_bytes,
            text(d, "membership_sha256")?,
        )?;
        let leaves_pin = pin("router/leaves.bin", leaves_bytes, text(d, "leaves_sha256")?)?;
        let (root, root_snapshot) = crate::loss_binary(&root_pin, root_pin.bytes)?;
        let (membership, member_snapshot) = crate::loss_binary(&member_pin, member_pin.bytes)?;
        let input = SourceIdentity {
            profile,
            schema: text(d, "input_schema")?,
            root_sha256: text(d, "input_root_sha256")?,
            centroids_sha256: text(d, "centroids_sha256")?,
            rows: i.rows,
            dimensions: i.dimensions,
        };
        let router = SemanticUnitRouter::open(
            &root,
            &membership,
            &root_pin.sha256,
            &input,
            profile.root_cap(),
        )?;
        require(
            router.manifest().leaf_payload.bytes == leaves_bytes
                && router.manifest().leaf_payload.sha256 == leaves_pin.sha256,
            "virtual leaf object binding",
        )?;
        crate::header_pin(&leaves_pin, leaves_pin.bytes)?;
        let file = crate::header_file(&leaves_pin.path)?;
        let id = crate::file_identity(&file)?;
        require(id.len == leaves_pin.bytes, "virtual leaf file length")?;
        let mut leaf_snapshot = crate::HeaderSnapshot {
            file,
            path: leaves_pin.path,
            identity: id,
        };
        leaf_snapshot.recheck()?;
        let layout = layout_from_router_leaves(&router, &mut leaf_snapshot.file, 8 * 1024 * 1024)?;
        leaf_snapshot.recheck()?;
        let mut locator = Sha256::new();
        locator.update(b"BORSUK_UNIT_LOCATOR_LE_U64_V1");
        for unit in 0..i.rows.div_ceil(32) {
            let source = layout
                .source_row(Sq8Row(unit * 32))
                .ok_or("virtual locator inverse")?
                .0
                / 32;
            locator.update(u64::try_from(source)?.to_le_bytes());
        }
        let locator_sha = format!("{:x}", locator.finalize());
        let (sidecar, mut sq8) =
            super::authenticated_sq8_digests(&c.sq8, i.rows, i.dimensions, 8 * 1024 * 1024)?;
        let generation = g["generation"].as_u64().ok_or("virtual generation")?;
        let authority_body = serde_json::to_vec(&serde_json::json!({
            "schema":"borsuk-v115-sq8-page-authority-v2","generation":generation,
            "rows":i.rows,"dimensions":i.dimensions,"page_rows":32,"object_sha256":c.sq8.sha256,
            "page_digest_sha256":format!("{:x}",Sha256::digest(&sidecar))}))?;
        let old = PageAuthority::load(
            &authority_body,
            &format!("{:x}", Sha256::digest(&authority_body)),
            &sidecar,
        )
        .map_err(|e| format!("virtual original authority {e:?}"))?;
        let relocated =
            crate::virtual_sq8::VirtualSq8::prepare(&mut sq8.file, &old, layout, 8 * 1024 * 1024)?;
        let order = (0..i.rows.div_ceil(32))
            .map(|u| u as u32)
            .collect::<Vec<_>>();
        let identity = UnitLayout::from_destination_order(i.rows, 32, &order, 1 << 20)?;
        let descriptor = serde_json::json!({"schema":"borsuk-virtual-sq8-placement-v1",
            "original_generation_sha256":i.generation_root_sha256,"original_sq8_sha256":c.sq8.sha256,
            "router_sha256":root_pin.sha256,"membership_sha256":member_pin.sha256,
            "leaves_sha256":router.manifest().leaf_payload.sha256,"locator_sha256":locator_sha,
            "target_sq8_sha256":relocated.object_sha256(),"rows":i.rows,"dimensions":i.dimensions,
            "unit_rows":32,"unit_count":i.rows.div_ceil(32),"query_free":true,
            "physical_object_written":false,"centroid_plane_opened":false});
        let context = Self {
            identity,
            relocated,
            sq8,
            snapshots: vec![gs, ps, root_snapshot, member_snapshot, leaf_snapshot],
            control,
            control_snapshot: None,
            descriptor,
        };
        context.recheck()?;
        Ok(context)
    }
    /// Authenticate the control as opaque bytes before a paid scored run. Its
    /// JSON/quality fields are parsed only after all candidate decisions finish.
    pub(super) fn admit_control_bytes(&mut self) -> Result<()> {
        let (_, snapshot) = crate::loss_binary(&self.control, self.control.bytes)?;
        self.control_snapshot = Some(snapshot);
        self.recheck()
    }
    pub(super) fn recheck(&self) -> Result<()> {
        self.sq8.recheck()?;
        for s in &self.snapshots {
            s.recheck()?;
        }
        if let Some(s) = &self.control_snapshot {
            s.recheck()?;
        }
        Ok(())
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) fn rank(
        &mut self,
        old_snapshot: &mut crate::HeaderSnapshot,
        old: &PageAuthority,
        scored: &[(usize, f64)],
        nominees: &[usize],
        query: &[f32],
        low: &[f32],
        step: &[f32],
    ) -> Result<(Vec<super::ShortlistArm>, serde_json::Value)> {
        let limits = SelectedSq8Limits {
            max_gets: 30,
            max_bytes: 32 * 1024 * 1024,
            max_planner_bytes: 4 * 1024 * 1024 + 4096,
        };
        let identity = admit_source_blocks(old, &self.identity, scored, nominees, limits, 1 << 20)?;
        let relocated = admit_source_blocks(
            self.relocated.authority(),
            self.relocated.layout(),
            scored,
            nominees,
            limits,
            1 << 20,
        )?;
        require(
            identity.priority_sha256 == relocated.priority_sha256,
            "virtual SOURCE priority parity",
        )?;
        let original = super::rank_local_cover(
            old_snapshot,
            old,
            &identity.plan.ranges,
            old.rows(),
            old.dimensions(),
            query,
            low,
            step,
            limits.max_bytes,
        )?;
        let moved = self.rank_relocated(&relocated.plan, query, low, step)?;
        let returned = [&original, &moved].map(|points| points.iter().map(|p| serde_json::json!({"target_row":p.ordinal,"id":p.id,"score_bits":p.score.to_bits()})).collect::<Vec<_>>());
        let evidence = serde_json::json!({"returned":returned,"nominee_source_sha256":row_digest(nominees.iter().copied(),b"SOURCE_NOMINEES_V1")?,
            "source_priority_sha256":identity.priority_sha256,
            "accepted_source_sha256":[row_digest(identity.source_rows.iter().map(|r|r.0),b"SOURCE_SELECTED_V1")?,
                row_digest(relocated.source_rows.iter().map(|r|r.0),b"SOURCE_SELECTED_V1")?]});
        let arm = |a: MappedAdmission, returned| super::ShortlistArm {
            maximum: 1024,
            selected: a.sq8_rows.into_iter().map(|r| r.0).collect(),
            ranges: a.plan.ranges,
            bytes: a.plan.planned_bytes,
            returned: Some(returned),
        };
        Ok((
            vec![arm(identity, original), arm(relocated, moved)],
            evidence,
        ))
    }
    fn rank_relocated(
        &mut self,
        plan: &SelectedSq8Plan,
        query: &[f32],
        low: &[f32],
        step: &[f32],
    ) -> Result<Vec<borsuk::exact_sq8_nominee::ScoredNominee>> {
        let a = self.relocated.authority();
        let rows = a.rows();
        let dimensions = a.dimensions();
        let width = add(dimensions, 12)?;
        let scoring = add(
            mul(rows.min(plan.planned_bytes / width), 256)?,
            add(mul(dimensions, 4)?, 65536)?,
        )?;
        let identity = self.identity.owned_payload_bytes()?;
        require(
            add(
                add(self.relocated.retained_payload_bytes(), identity)?,
                add(plan.planned_bytes, scoring)?,
            )? <= QUERY_PAYLOAD,
            "virtual aggregate bodies/scorer payload",
        )?;
        self.recheck()?;
        let mut bodies = Vec::with_capacity(plan.ranges.len());
        let mut held = 0_usize;
        for range in &plan.ranges {
            let cap = QUERY_PAYLOAD
                .checked_sub(add(add(held, identity)?, scoring)?)
                .ok_or("virtual remaining body payload")?;
            let body = self
                .relocated
                .read_range(&mut self.sq8.file, range.clone(), cap)?;
            held = add(held, body.capacity())?;
            bodies.push(body);
        }
        self.recheck()?;
        let ranges = plan
            .ranges
            .iter()
            .zip(&bodies)
            .map(|(r, b)| borsuk::returned_sq8::ReturnedRange {
                start: r.start,
                bytes: b,
            })
            .collect::<Vec<_>>();
        Ok(borsuk::returned_sq8::rank_returned_ranges(
            borsuk::exact_sq8_nominee::Sq8Geometry { rows, dimensions },
            &ranges,
            query,
            low,
            step,
            crate::K,
            32 * 1024 * 1024,
        )
        .map_err(|e| format!("virtual returned ranking {e:?}"))?)
    }
    pub(super) fn verify_control(&mut self, queries: &[serde_json::Value]) -> Result<()> {
        // Invoked only after every candidate plan and score is fixed.
        let (bytes, snapshot) = crate::loss_binary(&self.control, self.control.bytes)?;
        let old: serde_json::Value = serde_json::from_slice(&bytes)?;
        super::validate_block_admission_report(&old)?;
        let old_queries = old["queries"]
            .as_array()
            .ok_or("virtual control query array")?;
        require(
            queries.len() == 1000 && old_queries.len() == 1000,
            "virtual closed control population",
        )?;
        for (now, before) in queries.iter().zip(old_queries) {
            require(
                now["ordinal"] == before["ordinal"]
                    && now["baseline_hits10"] == before["baseline_hits10"]
                    && now["arms"][0] == before["arms"][0],
                "virtual identity vs closed selector parity",
            )?;
        }
        self.control_snapshot = Some(snapshot);
        self.recheck()
    }
}
