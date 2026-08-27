use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::{
    BorsukError, Result, VectorElementType,
    global_cell_card::{
        CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION, CellCardExactBlockRef, CellCardGroupRef,
        EncodedCellCardGroup, RankedCellCardExactBlock, encode_cell_card_group,
        plan_cell_card_exact_wave_with_amplification,
    },
    global_leaf::GlobalLeafPageInput,
};

const V22_EXACT_PREFIX_ROWS: [u16; 6] = [10, 256, 512, 1024, 1536, 2048];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V22LayoutKind {
    V20Physical,
    V20TwoPivotRepacked,
    SemanticWithinCell,
    SemanticCrossCell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V22LayoutCensusArm {
    pub(crate) layout: V22LayoutKind,
    pub(crate) microcluster_rows: Option<u8>,
    pub(crate) exact_prefix_rows: u16,
}

impl V22LayoutCensusArm {
    pub(crate) fn validate(self) -> Result<()> {
        let layout_is_valid = matches!(
            (self.layout, self.microcluster_rows),
            (V22LayoutKind::V20Physical, None)
                | (
                    V22LayoutKind::V20TwoPivotRepacked
                        | V22LayoutKind::SemanticWithinCell
                        | V22LayoutKind::SemanticCrossCell,
                    Some(32 | 64)
                )
        );
        if !layout_is_valid || !V22_EXACT_PREFIX_ROWS.contains(&self.exact_prefix_rows) {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 layout census arm is outside the frozen matrix".to_string(),
            ));
        }
        Ok(())
    }
}

pub(crate) fn v22_layout_census_arms() -> Result<Vec<V22LayoutCensusArm>> {
    let mut arms = Vec::with_capacity(V22_EXACT_PREFIX_ROWS.len() * 7);
    for (layout, microcluster_rows) in [
        (V22LayoutKind::V20Physical, None),
        (V22LayoutKind::V20TwoPivotRepacked, Some(32)),
        (V22LayoutKind::V20TwoPivotRepacked, Some(64)),
        (V22LayoutKind::SemanticWithinCell, Some(32)),
        (V22LayoutKind::SemanticWithinCell, Some(64)),
        (V22LayoutKind::SemanticCrossCell, Some(32)),
        (V22LayoutKind::SemanticCrossCell, Some(64)),
    ] {
        for exact_prefix_rows in V22_EXACT_PREFIX_ROWS {
            let arm = V22LayoutCensusArm {
                layout,
                microcluster_rows,
                exact_prefix_rows,
            };
            arm.validate()?;
            arms.push(arm);
        }
    }
    Ok(arms)
}

pub(crate) fn routing_rank(ordered_cells: &[u32], primary_cell: u32) -> Result<usize> {
    if ordered_cells.is_empty() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 ordered routing authority is empty".to_string(),
        ));
    }
    let unique = ordered_cells.iter().copied().collect::<BTreeSet<_>>();
    if unique.len() != ordered_cells.len() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 ordered routing authority contains duplicate cells".to_string(),
        ));
    }
    ordered_cells
        .iter()
        .position(|cell| *cell == primary_cell)
        .map(|rank| rank + 1)
        .ok_or_else(|| {
            BorsukError::InvalidSearchOptions(
                "V22 primary cell is absent from ordered routing authority".to_string(),
            )
        })
}

pub(crate) fn routing_coverage_at_probe(
    ranks: &[usize],
    probes: usize,
    routing_cell_count: usize,
) -> Result<usize> {
    if ranks.is_empty()
        || routing_cell_count == 0
        || probes == 0
        || probes > routing_cell_count
        || ranks
            .iter()
            .any(|rank| *rank == 0 || *rank > routing_cell_count)
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 routing-rank evidence is empty or invalid".to_string(),
        ));
    }
    Ok(ranks.iter().filter(|rank| **rank <= probes).count())
}

#[derive(Debug, Clone)]
pub(crate) struct V22SemanticRow {
    pub(crate) record_id: u64,
    pub(crate) canonical_record_id: Box<[u8]>,
    pub(crate) primary_cell: u32,
    /// Authenticated metric-prepared geometry (including normalization when
    /// required), matching the production V20 locality builder's input.
    pub(crate) geometry: Box<[f32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V22SemanticUnit {
    pub(crate) primary_cell: u32,
    pub(crate) record_ids: Box<[u64]>,
}

#[derive(Debug)]
struct V22SemanticCell {
    primary_cell: u32,
    centroid: Box<[f64]>,
    units: Vec<V22SemanticUnit>,
}

fn semantic_squared_distance(left: &[f32], right: &[f32]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(*right);
            delta * delta
        })
        .sum()
}

fn semantic_centroid(rows: &[V22SemanticRow], indexes: &[usize]) -> Box<[f64]> {
    let mut centroid = vec![0.0_f64; rows[indexes[0]].geometry.len()];
    for index in indexes {
        for (sum, value) in centroid.iter_mut().zip(rows[*index].geometry.iter()) {
            *sum += f64::from(*value);
        }
    }
    for value in &mut centroid {
        *value /= indexes.len() as f64;
    }
    centroid.into()
}

fn semantic_centroid_distance(left: &[f64], right: &[f64]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = left - right;
            delta * delta
        })
        .sum()
}

fn semantic_farthest(rows: &[V22SemanticRow], indexes: &[usize], from: usize) -> usize {
    let mut farthest = indexes[0];
    let mut farthest_distance = semantic_squared_distance(
        rows[farthest].geometry.as_ref(),
        rows[from].geometry.as_ref(),
    );
    for &index in &indexes[1..] {
        let distance =
            semantic_squared_distance(rows[index].geometry.as_ref(), rows[from].geometry.as_ref());
        if distance.total_cmp(&farthest_distance).is_gt()
            || (distance.total_cmp(&farthest_distance).is_eq()
                && rows[index].canonical_record_id < rows[farthest].canonical_record_id)
        {
            farthest = index;
            farthest_distance = distance;
        }
    }
    farthest
}

fn two_pivot_farthest(rows: &[V22SemanticRow], indexes: &[usize], from: usize) -> usize {
    let mut farthest = indexes[0];
    let mut farthest_distance = crate::metric::squared_euclidean_simd(
        rows[farthest].geometry.as_ref(),
        rows[from].geometry.as_ref(),
    );
    for &index in &indexes[1..] {
        let distance = crate::metric::squared_euclidean_simd(
            rows[index].geometry.as_ref(),
            rows[from].geometry.as_ref(),
        );
        if distance.total_cmp(&farthest_distance).is_gt()
            || (distance.total_cmp(&farthest_distance).is_eq()
                && rows[index].canonical_record_id < rows[farthest].canonical_record_id)
        {
            farthest = index;
            farthest_distance = distance;
        }
    }
    farthest
}

fn split_semantic_rows(
    rows: &[V22SemanticRow],
    indexes: &mut [usize],
    leaf_count: usize,
    leaves: &mut Vec<Vec<usize>>,
) {
    if leaf_count == 1 {
        leaves.push(indexes.to_vec());
        return;
    }
    let anchor = *indexes
        .iter()
        .min_by(|left, right| {
            rows[**left]
                .canonical_record_id
                .cmp(&rows[**right].canonical_record_id)
        })
        .expect("nonempty semantic split");
    let first_pivot = semantic_farthest(rows, indexes, anchor);
    let second_pivot = semantic_farthest(rows, indexes, first_pivot);
    let mut scored = indexes
        .iter()
        .map(|index| {
            let score = semantic_squared_distance(
                rows[*index].geometry.as_ref(),
                rows[first_pivot].geometry.as_ref(),
            ) - semantic_squared_distance(
                rows[*index].geometry.as_ref(),
                rows[second_pivot].geometry.as_ref(),
            );
            (*index, score)
        })
        .collect::<Vec<_>>();
    scored.sort_by(|(left, left_score), (right, right_score)| {
        left_score.total_cmp(right_score).then_with(|| {
            rows[*left]
                .canonical_record_id
                .cmp(&rows[*right].canonical_record_id)
        })
    });
    for (target, (index, _)) in indexes.iter_mut().zip(scored) {
        *target = index;
    }
    let left_leaf_count = leaf_count / 2;
    let right_leaf_count = leaf_count - left_leaf_count;
    let middle = indexes.len() * left_leaf_count / leaf_count;
    let (left, right) = indexes.split_at_mut(middle);
    split_semantic_rows(rows, left, left_leaf_count, leaves);
    split_semantic_rows(rows, right, right_leaf_count, leaves);
}

fn nearest_neighbor_order<K: Ord>(centroids: &[Box<[f64]>], keys: &[K]) -> Vec<usize> {
    let mut remaining = (0..centroids.len()).collect::<BTreeSet<_>>();
    let first = *remaining
        .iter()
        .min_by(|left, right| keys[**left].cmp(&keys[**right]))
        .expect("nonempty nearest-neighbor authority");
    remaining.remove(&first);
    let mut order = vec![first];
    while !remaining.is_empty() {
        let prior = *order.last().expect("nearest-neighbor order is nonempty");
        let next = remaining
            .iter()
            .map(|index| {
                (
                    *index,
                    semantic_centroid_distance(&centroids[prior], &centroids[*index]),
                )
            })
            .min_by(|(left, left_distance), (right, right_distance)| {
                left_distance
                    .total_cmp(right_distance)
                    .then_with(|| keys[*left].cmp(&keys[*right]))
            })
            .map(|(index, _)| index)
            .expect("nearest-neighbor remainder is nonempty");
        remaining.remove(&next);
        order.push(next);
    }
    order
}

pub(crate) fn project_v22_semantic_layout(
    rows: &[V22SemanticRow],
    authenticated_cell_order: &[u32],
    microcluster_rows: u8,
    reorder_cells: bool,
) -> Result<Vec<V22SemanticUnit>> {
    if rows.is_empty()
        || !matches!(microcluster_rows, 32 | 64)
        || authenticated_cell_order.is_empty()
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 semantic layout authority is empty or invalid".to_string(),
        ));
    }
    let dimensions = rows[0].geometry.len();
    let unique_ids = rows
        .iter()
        .map(|row| row.record_id)
        .collect::<BTreeSet<_>>();
    let unique_canonical_ids = rows
        .iter()
        .map(|row| row.canonical_record_id.as_ref())
        .collect::<BTreeSet<_>>();
    if dimensions == 0
        || unique_ids.len() != rows.len()
        || unique_canonical_ids.len() != rows.len()
        || rows.iter().any(|row| row.canonical_record_id.is_empty())
        || rows.iter().any(|row| {
            row.geometry.len() != dimensions || row.geometry.iter().any(|value| !value.is_finite())
        })
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 semantic rows are duplicate, nonfinite, or dimensionally inconsistent".to_string(),
        ));
    }

    let mut rows_by_cell = BTreeMap::<u32, Vec<usize>>::new();
    for (index, row) in rows.iter().enumerate() {
        rows_by_cell
            .entry(row.primary_cell)
            .or_default()
            .push(index);
    }
    let ordered_cells = authenticated_cell_order
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if ordered_cells.len() != authenticated_cell_order.len()
        || ordered_cells != rows_by_cell.keys().copied().collect()
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 authenticated cell order does not cover semantic rows exactly".to_string(),
        ));
    }

    let mut cells = BTreeMap::<u32, V22SemanticCell>::new();
    for (&primary_cell, cell_indexes) in &mut rows_by_cell {
        cell_indexes.sort_unstable_by(|left, right| {
            rows[*left]
                .canonical_record_id
                .cmp(&rows[*right].canonical_record_id)
        });
        let cell_centroid = semantic_centroid(rows, cell_indexes);
        let mut leaves = Vec::new();
        let leaf_count = cell_indexes.len().div_ceil(usize::from(microcluster_rows));
        split_semantic_rows(rows, cell_indexes, leaf_count, &mut leaves);
        let leaf_centroids = leaves
            .iter()
            .map(|leaf| semantic_centroid(rows, leaf))
            .collect::<Vec<_>>();
        let leaf_keys = leaves
            .iter()
            .map(|leaf| {
                leaf.iter()
                    .map(|index| rows[*index].canonical_record_id.as_ref())
                    .min()
                    .expect("semantic leaf is nonempty")
                    .to_vec()
            })
            .collect::<Vec<_>>();
        let units = nearest_neighbor_order(&leaf_centroids, &leaf_keys)
            .into_iter()
            .map(|leaf_index| V22SemanticUnit {
                primary_cell,
                record_ids: leaves[leaf_index]
                    .iter()
                    .map(|index| rows[*index].record_id)
                    .collect(),
            })
            .collect();
        cells.insert(
            primary_cell,
            V22SemanticCell {
                primary_cell,
                centroid: cell_centroid,
                units,
            },
        );
    }

    let cell_order = if reorder_cells {
        let sorted_cells = cells.values().collect::<Vec<_>>();
        let centroids = sorted_cells
            .iter()
            .map(|cell| cell.centroid.clone())
            .collect::<Vec<_>>();
        let keys = sorted_cells
            .iter()
            .map(|cell| u64::from(cell.primary_cell))
            .collect::<Vec<_>>();
        nearest_neighbor_order(&centroids, &keys)
            .into_iter()
            .map(|index| sorted_cells[index].primary_cell)
            .collect::<Vec<_>>()
    } else {
        authenticated_cell_order.to_vec()
    };
    let mut projected = Vec::new();
    for primary_cell in cell_order {
        projected.extend(
            cells
                .remove(&primary_cell)
                .expect("validated semantic cell remains present")
                .units,
        );
    }
    Ok(projected)
}

pub(crate) fn project_v22_two_pivot_layout(
    rows: &[V22SemanticRow],
    authenticated_cell_order: &[u32],
    unit_rows: u8,
) -> Result<Vec<V22SemanticUnit>> {
    if rows.is_empty() || !matches!(unit_rows, 32 | 64) || authenticated_cell_order.is_empty() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 two-pivot layout authority is empty or invalid".to_string(),
        ));
    }
    let dimensions = rows[0].geometry.len();
    let unique_ids = rows
        .iter()
        .map(|row| row.record_id)
        .collect::<BTreeSet<_>>();
    let unique_canonical_ids = rows
        .iter()
        .map(|row| row.canonical_record_id.as_ref())
        .collect::<BTreeSet<_>>();
    if dimensions == 0
        || unique_ids.len() != rows.len()
        || unique_canonical_ids.len() != rows.len()
        || rows.iter().any(|row| row.canonical_record_id.is_empty())
        || rows.iter().any(|row| {
            row.geometry.len() != dimensions || row.geometry.iter().any(|value| !value.is_finite())
        })
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 two-pivot rows are duplicate, nonfinite, or dimensionally inconsistent"
                .to_string(),
        ));
    }
    let mut rows_by_cell = BTreeMap::<u32, Vec<usize>>::new();
    for (index, row) in rows.iter().enumerate() {
        rows_by_cell
            .entry(row.primary_cell)
            .or_default()
            .push(index);
    }
    let ordered_cells = authenticated_cell_order
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if ordered_cells.len() != authenticated_cell_order.len()
        || ordered_cells != rows_by_cell.keys().copied().collect()
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 authenticated cell order does not cover two-pivot rows exactly".to_string(),
        ));
    }

    let mut projected = Vec::new();
    for primary_cell in authenticated_cell_order {
        let mut indexes = rows_by_cell
            .remove(primary_cell)
            .expect("validated two-pivot cell remains present");
        indexes.sort_unstable_by(|left, right| {
            rows[*left]
                .canonical_record_id
                .cmp(&rows[*right].canonical_record_id)
        });
        let first = indexes[0];
        let second = two_pivot_farthest(rows, &indexes, first);
        let first_geometry = &rows[first].geometry;
        let projection_axis = rows[second]
            .geometry
            .iter()
            .zip(first_geometry)
            .map(|(second, first)| second - first)
            .collect::<Vec<_>>();
        let mut scored = indexes
            .into_iter()
            .map(|index| {
                let mut offset_geometry = rows[index].geometry.to_vec();
                for (value, first) in offset_geometry.iter_mut().zip(first_geometry) {
                    *value -= first;
                }
                let score = crate::metric::dot_product(&offset_geometry, &projection_axis);
                (index, score)
            })
            .collect::<Vec<_>>();
        scored.sort_by(|(left, left_score), (right, right_score)| {
            left_score.total_cmp(right_score).then_with(|| {
                rows[*left]
                    .canonical_record_id
                    .cmp(&rows[*right].canonical_record_id)
            })
        });
        projected.extend(scored.chunks(usize::from(unit_rows)).map(|chunk| {
            V22SemanticUnit {
                primary_cell: *primary_cell,
                record_ids: chunk
                    .iter()
                    .map(|(index, _)| rows[*index].record_id)
                    .collect(),
            }
        }));
    }
    Ok(projected)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V22ProjectedUnit {
    pub(crate) path: String,
    pub(crate) object_checksum: [u8; 32],
    pub(crate) object_encoded_bytes: u64,
    pub(crate) offset: u64,
    pub(crate) encoded_bytes: u32,
    pub(crate) decoded_bytes: u64,
    pub(crate) record_ids: Box<[u64]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V22ProjectedObjectAuthority {
    pub(crate) path: String,
    pub(crate) checksum: [u8; 32],
    pub(crate) encoded_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V22EncodedRecordAuthority {
    pub(crate) canonical_record_id: Box<[u8]>,
    pub(crate) record_id: u64,
}

#[derive(Debug)]
pub(crate) struct V22EncodedProjection {
    pub(crate) encoded: EncodedCellCardGroup,
    pub(crate) units: Vec<V22ProjectedUnit>,
}

pub(crate) fn project_v22_encoded_cell_card_group(
    pages: &[GlobalLeafPageInput],
    records_by_card: &[Box<[V22EncodedRecordAuthority]>],
    dimensions: usize,
    element_type: VectorElementType,
    content_prefix: &str,
) -> Result<V22EncodedProjection> {
    let exact_row_bytes_usize = element_type.fixed_width_bytes(dimensions)?;
    let exact_row_bytes = exact_row_bytes_usize as u64;
    if exact_row_bytes == 0 || pages.len() != records_by_card.len() {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 encoded group projection authority is empty or mismatched".to_string(),
        ));
    }
    let encoded = encode_cell_card_group(pages, dimensions, element_type)?;
    let path = encoded.content_addressed_path(content_prefix)?;
    let (group, cards) = encoded.references(&path)?;
    let mut canonical_record_ids = BTreeSet::new();
    let mut numeric_record_ids = BTreeSet::new();
    let mut projected = Vec::new();
    for ((card, page), records) in cards.iter().zip(pages).zip(records_by_card) {
        if records.is_empty()
            || records.len() != card.head.rows as usize
            || records.len() != page.rows.len()
            || card.head.cell_index != page.cell_index
            || card.head.card_ordinal != page.leaf_ordinal
            || card.head.leaf_ordinal != page.leaf_ordinal
            || page.rows.iter().zip(records).any(|(row, record)| {
                row.id.as_bytes() != record.canonical_record_id.as_ref()
                    || row.exact.len() != exact_row_bytes_usize
                    || !canonical_record_ids.insert(record.canonical_record_id.as_ref())
                    || !numeric_record_ids.insert(record.record_id)
            })
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 encoded card record authority is empty or mismatched".to_string(),
            ));
        }
        let mut row_offset = 0_usize;
        let mut previous_block_end = 0_u64;
        for (block_index, block) in card.head.exact_blocks.iter().enumerate() {
            if block.block_ordinal != block_index as u32 || block.offset < previous_block_end {
                return Err(BorsukError::InvalidSearchOptions(
                    "V22 encoded blocks are not in canonical row order".to_string(),
                ));
            }
            let block_rows = block.rows as usize;
            let row_end = row_offset.checked_add(block_rows).ok_or_else(|| {
                BorsukError::InvalidSearchOptions(
                    "V22 encoded block row range overflows".to_string(),
                )
            })?;
            let block_record_ids = records.get(row_offset..row_end).ok_or_else(|| {
                BorsukError::InvalidSearchOptions(
                    "V22 encoded blocks exceed record authority".to_string(),
                )
            })?;
            let decoded_bytes = u64::try_from(block_rows)
                .ok()
                .and_then(|rows| rows.checked_mul(exact_row_bytes))
                .ok_or_else(|| {
                    BorsukError::InvalidSearchOptions(
                        "V22 encoded block decoded bytes overflow".to_string(),
                    )
                })?;
            projected.push(V22ProjectedUnit {
                path: group.path.clone(),
                object_checksum: group.checksum,
                object_encoded_bytes: group.encoded_bytes,
                offset: block.offset,
                encoded_bytes: block.bytes,
                decoded_bytes,
                record_ids: block_record_ids
                    .iter()
                    .map(|record| record.record_id)
                    .collect(),
            });
            previous_block_end = block.offset + u64::from(block.bytes);
            row_offset = row_end;
        }
        if row_offset != records.len() {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 encoded blocks do not cover record authority".to_string(),
            ));
        }
    }
    Ok(V22EncodedProjection {
        encoded,
        units: projected,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V22LayoutLimitingBound {
    Eligible,
    Requests,
    Bytes,
    Amplification,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V22LayoutCensus {
    pub(crate) projected_objects: Box<[V22ProjectedObjectAuthority]>,
    pub(crate) useful_bytes: u64,
    pub(crate) selected_bytes: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) speculative_bytes: u64,
    pub(crate) requests: usize,
    pub(crate) selected_rows: u64,
    pub(crate) rows_per_range: Box<[u64]>,
    pub(crate) blocks_per_range: Box<[usize]>,
    pub(crate) packing_purity_ppm: u64,
    pub(crate) physical_amplification_ppm: u64,
    pub(crate) limiting_bound: V22LayoutLimitingBound,
    pub(crate) eligible: bool,
}

pub(crate) fn v22_census_layout_prefix(
    units: &[V22ProjectedUnit],
    ranked_record_ids: &[u64],
    exact_row_bytes: u64,
    max_physical_bytes: u64,
    max_requests: usize,
    max_physical_amplification: u64,
) -> Result<V22LayoutCensus> {
    if units.is_empty()
        || ranked_record_ids.is_empty()
        || exact_row_bytes == 0
        || max_physical_bytes == 0
        || max_requests == 0
        || !(1..=CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION).contains(&max_physical_amplification)
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 projected layout census authority is empty".to_string(),
        ));
    }
    let mut record_to_unit = BTreeMap::<u64, usize>::new();
    let mut ranges_by_path = BTreeMap::<&str, Vec<(u64, u64)>>::new();
    let mut path_authority = BTreeMap::<&str, (u64, [u8; 32])>::new();
    for (unit_index, unit) in units.iter().enumerate() {
        let expected_decoded_bytes = u64::try_from(unit.record_ids.len())
            .ok()
            .and_then(|rows| rows.checked_mul(exact_row_bytes))
            .ok_or_else(|| {
                BorsukError::InvalidSearchOptions(
                    "V22 projected unit decoded byte count overflows".to_string(),
                )
            })?;
        let end = unit
            .offset
            .checked_add(u64::from(unit.encoded_bytes))
            .ok_or_else(|| {
                BorsukError::InvalidSearchOptions("V22 projected unit range overflows".to_string())
            })?;
        if unit.path.is_empty()
            || unit.encoded_bytes == 0
            || unit.record_ids.is_empty()
            || u32::try_from(unit.record_ids.len()).is_err()
            || unit.decoded_bytes != expected_decoded_bytes
            || end > unit.object_encoded_bytes
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 projected unit is empty or oversized".to_string(),
            ));
        }
        for record_id in &unit.record_ids {
            if record_to_unit.insert(*record_id, unit_index).is_some() {
                return Err(BorsukError::InvalidSearchOptions(
                    "V22 projected units contain a duplicate record".to_string(),
                ));
            }
        }
        ranges_by_path
            .entry(unit.path.as_str())
            .or_default()
            .push((unit.offset, end));
        match path_authority.entry(unit.path.as_str()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert((unit.object_encoded_bytes, unit.object_checksum));
            }
            std::collections::btree_map::Entry::Occupied(entry) => {
                if *entry.get() != (unit.object_encoded_bytes, unit.object_checksum) {
                    return Err(BorsukError::InvalidSearchOptions(
                        "V22 projected object checksum authority conflicts".to_string(),
                    ));
                }
            }
        }
    }
    for ranges in ranges_by_path.values_mut() {
        ranges.sort_unstable();
        if ranges.windows(2).any(|pair| pair[1].0 < pair[0].1) {
            return Err(BorsukError::InvalidSearchOptions(
                "V22 projected unit ranges overlap".to_string(),
            ));
        }
    }
    let ranked_unique = ranked_record_ids.iter().copied().collect::<BTreeSet<_>>();
    if ranked_unique.len() != ranked_record_ids.len()
        || ranked_record_ids
            .iter()
            .any(|record_id| !record_to_unit.contains_key(record_id))
    {
        return Err(BorsukError::InvalidSearchOptions(
            "V22 ranked prefix is duplicate or absent from the projected layout".to_string(),
        ));
    }

    let groups = path_authority
        .into_iter()
        .map(|(path, (encoded_bytes, checksum))| {
            (
                path,
                Arc::new(CellCardGroupRef {
                    path: path.to_string(),
                    checksum,
                    encoded_bytes,
                    code_plane_offset: 0,
                    code_plane_bytes: 0,
                    code_plane_checksum: [0; 32],
                }),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut selected_units = BTreeSet::<usize>::new();
    let mut ranked = Vec::new();
    for (rank, record_id) in ranked_record_ids.iter().enumerate() {
        let unit_index = record_to_unit[record_id];
        if !selected_units.insert(unit_index) {
            continue;
        }
        let unit = &units[unit_index];
        ranked.push(RankedCellCardExactBlock {
            head_index: unit_index,
            group: Arc::clone(&groups[unit.path.as_str()]),
            cell_index: 0,
            card_ordinal: u32::try_from(unit_index).map_err(|_| {
                BorsukError::InvalidSearchOptions(
                    "V22 projected unit ordinal overflows".to_string(),
                )
            })?,
            reference: CellCardExactBlockRef {
                block_ordinal: 0,
                offset: unit.offset,
                metadata_bytes: 0,
                body_bytes: unit.encoded_bytes,
                bytes: unit.encoded_bytes,
                rows: unit.record_ids.len() as u32,
                checksum: [0; 32],
            },
            distance: rank as f32,
            row_distances: Box::default(),
        });
    }
    let selected_bytes = ranked.iter().try_fold(0_u64, |total, block| {
        total
            .checked_add(u64::from(block.reference.bytes))
            .ok_or_else(|| {
                BorsukError::InvalidSearchOptions(
                    "V22 selected encoded byte count overflows".to_string(),
                )
            })
    })?;
    let measurement_ceiling = if selected_bytes <= max_physical_bytes {
        max_physical_bytes
    } else {
        selected_bytes
            .checked_mul(max_physical_amplification)
            .ok_or_else(|| {
                BorsukError::InvalidSearchOptions(
                    "V22 physical measurement ceiling overflows".to_string(),
                )
            })?
    };
    let plan = plan_cell_card_exact_wave_with_amplification(
        &ranked,
        measurement_ceiling,
        ranked.len(),
        max_physical_amplification,
    )?;
    let useful_bytes = u64::try_from(ranked_record_ids.len())
        .ok()
        .and_then(|rows| rows.checked_mul(exact_row_bytes))
        .ok_or_else(|| {
            BorsukError::InvalidSearchOptions("V22 useful byte count overflows".to_string())
        })?;
    let purity_numerator = useful_bytes.checked_mul(1_000_000).ok_or_else(|| {
        BorsukError::InvalidSearchOptions("V22 packing purity overflows".to_string())
    })?;
    let amplification_numerator =
        plan.physical_bytes()
            .checked_mul(1_000_000)
            .ok_or_else(|| {
                BorsukError::InvalidSearchOptions(
                    "V22 physical amplification overflows".to_string(),
                )
            })?;
    let limiting_bound = if plan.physical_bytes() > max_physical_bytes {
        V22LayoutLimitingBound::Bytes
    } else if plan.requests() > max_requests {
        let maximum_amplification_plan = plan_cell_card_exact_wave_with_amplification(
            &ranked,
            max_physical_bytes,
            ranked.len(),
            CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION,
        )?;
        if max_physical_amplification < CELL_CARD_EXACT_MAX_PHYSICAL_AMPLIFICATION
            && maximum_amplification_plan.requests() <= max_requests
            && maximum_amplification_plan.physical_bytes() <= max_physical_bytes
        {
            V22LayoutLimitingBound::Amplification
        } else {
            V22LayoutLimitingBound::Requests
        }
    } else {
        V22LayoutLimitingBound::Eligible
    };
    let projected_objects = groups
        .values()
        .map(|group| V22ProjectedObjectAuthority {
            path: group.path.clone(),
            checksum: group.checksum,
            encoded_bytes: group.encoded_bytes,
        })
        .collect();
    Ok(V22LayoutCensus {
        projected_objects,
        useful_bytes,
        selected_bytes: plan.selected_bytes(),
        physical_bytes: plan.physical_bytes(),
        speculative_bytes: plan.speculative_bytes(),
        requests: plan.requests(),
        selected_rows: plan.rows(),
        rows_per_range: plan
            .reads()
            .iter()
            .map(|read| {
                read.blocks
                    .iter()
                    .map(|block| u64::from(block.reference.rows))
                    .sum()
            })
            .collect(),
        blocks_per_range: plan.reads().iter().map(|read| read.blocks.len()).collect(),
        packing_purity_ppm: purity_numerator / plan.physical_bytes(),
        physical_amplification_ppm: amplification_numerator / plan.selected_bytes(),
        limiting_bound,
        eligible: limiting_bound == V22LayoutLimitingBound::Eligible,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        V22EncodedRecordAuthority, V22LayoutCensusArm, V22LayoutKind, V22LayoutLimitingBound,
        V22ProjectedUnit, V22SemanticRow, nearest_neighbor_order,
        project_v22_encoded_cell_card_group, project_v22_semantic_layout,
        project_v22_two_pivot_layout, routing_coverage_at_probe, routing_rank,
        v22_census_layout_prefix, v22_layout_census_arms,
    };
    use crate::{
        VectorElementType,
        global_leaf::{GlobalLeafCodeInput, GlobalLeafPageInput, GlobalLeafRowInput},
        mutation::{MutationStamp, MutationVersion},
        record::RecordId,
    };

    #[test]
    fn v22_layout_census_authority_is_exact_and_canonical() {
        let arms = v22_layout_census_arms().unwrap();
        assert_eq!(arms.len(), 42);
        assert_eq!(
            arms[0],
            V22LayoutCensusArm {
                layout: V22LayoutKind::V20Physical,
                microcluster_rows: None,
                exact_prefix_rows: 10,
            }
        );
        assert_eq!(arms[5].exact_prefix_rows, 2048);
        assert_eq!(arms[6].layout, V22LayoutKind::V20TwoPivotRepacked);
        assert_eq!(arms[6].microcluster_rows, Some(32));
        assert_eq!(arms[12].microcluster_rows, Some(64));
        assert_eq!(arms[18].layout, V22LayoutKind::SemanticWithinCell);
        assert_eq!(arms[18].microcluster_rows, Some(32));
        assert_eq!(arms[24].microcluster_rows, Some(64));
        assert_eq!(arms[30].layout, V22LayoutKind::SemanticCrossCell);
        assert_eq!(arms[30].microcluster_rows, Some(32));
        assert_eq!(arms[36].microcluster_rows, Some(64));
        assert_eq!(arms[41].exact_prefix_rows, 2048);
        for arm in arms {
            arm.validate().unwrap();
        }
    }

    #[test]
    fn v22_layout_census_authority_rejects_factor_drift() {
        for arm in [
            V22LayoutCensusArm {
                layout: V22LayoutKind::V20Physical,
                microcluster_rows: Some(32),
                exact_prefix_rows: 256,
            },
            V22LayoutCensusArm {
                layout: V22LayoutKind::V20TwoPivotRepacked,
                microcluster_rows: None,
                exact_prefix_rows: 256,
            },
            V22LayoutCensusArm {
                layout: V22LayoutKind::SemanticWithinCell,
                microcluster_rows: Some(48),
                exact_prefix_rows: 256,
            },
            V22LayoutCensusArm {
                layout: V22LayoutKind::SemanticWithinCell,
                microcluster_rows: Some(32),
                exact_prefix_rows: 768,
            },
        ] {
            assert!(arm.validate().is_err());
        }
    }

    #[test]
    fn v22_layout_census_routing_rank_subsumes_the_probe_sweep() {
        let ordered_cells = [9, 3, 7, 2, 5];
        assert_eq!(routing_rank(&ordered_cells, 9).unwrap(), 1);
        assert_eq!(routing_rank(&ordered_cells, 2).unwrap(), 4);
        assert_eq!(
            routing_coverage_at_probe(&[1, 4, 4, 5], 3, ordered_cells.len()).unwrap(),
            1
        );
        assert_eq!(
            routing_coverage_at_probe(&[1, 4, 4, 5], 4, ordered_cells.len()).unwrap(),
            3
        );
        assert!(routing_rank(&[], 3).is_err());
        assert!(routing_rank(&[3, 3], 3).is_err());
        assert!(routing_rank(&[3], 7).is_err());
        assert!(routing_coverage_at_probe(&[], 4, 5).is_err());
        assert!(routing_coverage_at_probe(&[1, 0], 4, 5).is_err());
        assert!(routing_coverage_at_probe(&[1], 0, 5).is_err());
        assert!(routing_coverage_at_probe(&[6], 5, 5).is_err());
        assert!(routing_coverage_at_probe(&[1], 6, 5).is_err());
        assert!(routing_coverage_at_probe(&[1], 1, 0).is_err());
        assert_eq!(routing_coverage_at_probe(&[4096], 4096, 4096).unwrap(), 1);
        assert_eq!(
            routing_coverage_at_probe(&[16384], 16384, 16384).unwrap(),
            1
        );
    }

    #[test]
    fn v22_layout_oracle_is_deterministic_and_separates_cell_placement() {
        let mut rows = Vec::new();
        for (primary_cell, cell_position) in [(0_u32, 0.0_f32), (1, 10.0), (2, -10.0)] {
            for ordinal in 0_u64..321 {
                let quadrant = (ordinal % 4) as f32;
                let record_id = u64::from(primary_cell) * 1000 + ordinal;
                rows.push(V22SemanticRow {
                    record_id,
                    canonical_record_id: record_id.to_be_bytes().into(),
                    primary_cell,
                    geometry: vec![
                        cell_position + quadrant * 100.0,
                        quadrant * 10.0 + ordinal as f32 / 1000.0,
                    ]
                    .into(),
                });
            }
        }
        let within = project_v22_semantic_layout(&rows, &[2, 1, 0], 32, false).unwrap();
        assert_eq!(within.len(), 33);
        assert!(within[..11].iter().all(|unit| unit.primary_cell == 2));
        assert!(within[11..22].iter().all(|unit| unit.primary_cell == 1));
        assert!(within[22..].iter().all(|unit| unit.primary_cell == 0));
        for unit in &within {
            assert!((29..=30).contains(&unit.record_ids.len()));
        }

        let cross = project_v22_semantic_layout(&rows, &[2, 1, 0], 32, true).unwrap();
        assert_eq!(cross.len(), 33);
        assert!(cross[..11].iter().all(|unit| unit.primary_cell == 0));
        assert!(cross[11..22].iter().all(|unit| unit.primary_cell == 1));
        assert!(cross[22..].iter().all(|unit| unit.primary_cell == 2));
        assert_ne!(within, cross);

        let two_pivot = project_v22_two_pivot_layout(&rows, &[2, 1, 0], 32).unwrap();
        assert_eq!(two_pivot.len(), 33);
        assert!(two_pivot[..11].iter().all(|unit| unit.primary_cell == 2));
        assert!(two_pivot[11..22].iter().all(|unit| unit.primary_cell == 1));
        assert!(two_pivot[22..].iter().all(|unit| unit.primary_cell == 0));
        assert_eq!(
            two_pivot
                .iter()
                .map(|unit| unit.record_ids.len())
                .collect::<Vec<_>>(),
            [32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 1].repeat(3)
        );
        assert_ne!(two_pivot, within);

        rows.reverse();
        assert_eq!(
            project_v22_semantic_layout(&rows, &[2, 1, 0], 32, false).unwrap(),
            within
        );
        assert_eq!(
            project_v22_semantic_layout(&rows, &[2, 1, 0], 32, true).unwrap(),
            cross
        );
        assert_eq!(
            project_v22_two_pivot_layout(&rows, &[2, 1, 0], 32).unwrap(),
            two_pivot
        );

        let centroids = vec![
            vec![0.0].into_boxed_slice(),
            vec![4.0].into_boxed_slice(),
            vec![1.0].into_boxed_slice(),
            vec![10.0].into_boxed_slice(),
        ];
        assert_eq!(
            nearest_neighbor_order(&centroids, &[0, 1, 2, 3]),
            [0, 2, 1, 3]
        );

        let metric_rows = (0_u64..128)
            .map(|record_id| {
                let cluster = (record_id % 4) as f32;
                V22SemanticRow {
                    record_id,
                    canonical_record_id: record_id.to_be_bytes().into(),
                    primary_cell: 0,
                    geometry: vec![cluster * 1000.0, record_id as f32 / 1000.0].into(),
                }
            })
            .collect::<Vec<_>>();
        let metric_units = project_v22_semantic_layout(&metric_rows, &[0], 32, false).unwrap();
        assert_eq!(metric_units.len(), 4);
        for unit in metric_units {
            let cluster = unit.record_ids[0] % 4;
            assert!(
                unit.record_ids
                    .iter()
                    .all(|record_id| record_id % 4 == cluster)
            );
        }

        let two_pivot_bytes = [
            V22SemanticRow {
                record_id: 10,
                canonical_record_id: vec![2].into(),
                primary_cell: 0,
                geometry: vec![0.0].into(),
            },
            V22SemanticRow {
                record_id: 20,
                canonical_record_id: vec![1].into(),
                primary_cell: 0,
                geometry: vec![10.0].into(),
            },
            V22SemanticRow {
                record_id: 30,
                canonical_record_id: vec![3].into(),
                primary_cell: 0,
                geometry: vec![20.0].into(),
            },
        ];
        assert_eq!(
            project_v22_two_pivot_layout(&two_pivot_bytes, &[0], 32).unwrap()[0]
                .record_ids
                .as_ref(),
            &[30, 20, 10]
        );
        assert_eq!(
            project_v22_semantic_layout(&two_pivot_bytes, &[0], 32, false).unwrap()[0]
                .record_ids
                .as_ref(),
            &[20, 10, 30]
        );
    }

    #[test]
    fn v22_layout_oracle_rejects_unauthenticated_geometry_and_order() {
        let valid = [V22SemanticRow {
            record_id: 7,
            canonical_record_id: vec![7].into(),
            primary_cell: 3,
            geometry: vec![1.0, 2.0].into(),
        }];
        assert!(project_v22_semantic_layout(&valid, &[3], 32, false).is_ok());
        assert!(project_v22_two_pivot_layout(&valid, &[3], 32).is_ok());
        assert!(project_v22_two_pivot_layout(&valid, &[3], 48).is_err());
        assert!(project_v22_two_pivot_layout(&valid, &[4], 32).is_err());
        assert!(project_v22_semantic_layout(&valid, &[3], 48, false).is_err());
        assert!(project_v22_semantic_layout(&valid, &[], 32, false).is_err());
        assert!(project_v22_semantic_layout(&valid, &[3, 3], 32, false).is_err());
        assert!(project_v22_semantic_layout(&valid, &[4], 32, false).is_err());

        let duplicate = [valid[0].clone(), valid[0].clone()];
        assert!(project_v22_semantic_layout(&duplicate, &[3], 32, false).is_err());
        assert!(project_v22_two_pivot_layout(&duplicate, &[3], 32).is_err());
        let duplicate_canonical = [
            valid[0].clone(),
            V22SemanticRow {
                record_id: 8,
                canonical_record_id: vec![7].into(),
                primary_cell: 3,
                geometry: vec![3.0, 4.0].into(),
            },
        ];
        assert!(project_v22_semantic_layout(&duplicate_canonical, &[3], 32, false).is_err());
        assert!(project_v22_two_pivot_layout(&duplicate_canonical, &[3], 32).is_err());
        let mismatched = [
            valid[0].clone(),
            V22SemanticRow {
                record_id: 8,
                canonical_record_id: vec![8].into(),
                primary_cell: 3,
                geometry: vec![1.0].into(),
            },
        ];
        assert!(project_v22_semantic_layout(&mismatched, &[3], 32, false).is_err());
        let nonfinite = [V22SemanticRow {
            record_id: 8,
            canonical_record_id: vec![8].into(),
            primary_cell: 3,
            geometry: vec![f32::NAN, 2.0].into(),
        }];
        assert!(project_v22_semantic_layout(&nonfinite, &[3], 32, false).is_err());
    }

    #[test]
    fn v22_layout_oracle_derives_ranges_from_the_real_encoder() {
        const DIMENSIONS: usize = 96;
        const EXACT_ROW_BYTES: usize = DIMENSIONS * std::mem::size_of::<f32>();

        assert!(
            project_v22_encoded_cell_card_group(
                &[],
                &[],
                DIMENSIONS,
                VectorElementType::Float32,
                "v22-stage-l/empty",
            )
            .is_err()
        );

        let mut next_record_id = 0_u64;
        let mut make_page = |cell_index: u32, rows: usize, compressible: bool| {
            let mut authority = Vec::with_capacity(rows);
            let rows = (0..rows)
                .map(|ordinal| {
                    let record_id = next_record_id;
                    next_record_id += 1;
                    let canonical = format!("v22-{cell_index:02}-{ordinal:04}").into_bytes();
                    authority.push(V22EncodedRecordAuthority {
                        canonical_record_id: canonical.clone().into(),
                        record_id,
                    });
                    let exact = if compressible {
                        vec![0; EXACT_ROW_BYTES]
                    } else {
                        (0..EXACT_ROW_BYTES)
                            .map(|byte| {
                                let mixed = record_id
                                    .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                                    .rotate_left((byte % 63) as u32)
                                    ^ byte as u64;
                                mixed as u8
                            })
                            .collect()
                    };
                    GlobalLeafRowInput {
                        id: RecordId::from_bytes(canonical),
                        stamp: MutationStamp::new(
                            MutationVersion::from_parts(record_id + 1, [cell_index as u8; 16]),
                            [record_id as u8; 32],
                        ),
                        code: GlobalLeafCodeInput::from(vec![cell_index as u8, ordinal as u8]),
                        exact,
                    }
                })
                .collect();
            (
                GlobalLeafPageInput {
                    cell_index,
                    leaf_ordinal: cell_index,
                    centroid_code: vec![cell_index as u8, 0],
                    rows,
                },
                authority.into_boxed_slice(),
            )
        };
        let fixtures = [
            make_page(0, 1, false),
            make_page(1, 32, true),
            make_page(2, 65, false),
        ];
        let pages = fixtures
            .iter()
            .map(|(page, _)| page.clone())
            .collect::<Vec<_>>();
        let authority = fixtures
            .into_iter()
            .map(|(_, authority)| authority)
            .collect::<Vec<_>>();
        let projection = project_v22_encoded_cell_card_group(
            &pages,
            &authority,
            DIMENSIONS,
            VectorElementType::Float32,
            "v22-stage-l/layout",
        )
        .unwrap();
        let encoded = &projection.encoded;
        let expected_path = encoded
            .content_addressed_path("v22-stage-l/layout")
            .unwrap();
        let (group, _) = encoded.references(&expected_path).unwrap();
        let projected = &projection.units;

        assert_eq!(projected.len(), 5);
        assert_eq!(
            projected
                .iter()
                .map(|unit| unit.record_ids.len())
                .collect::<Vec<_>>(),
            [1, 32, 32, 32, 1]
        );
        let expected_blocks = encoded
            .cards
            .iter()
            .flat_map(|card| card.head.exact_blocks.iter())
            .collect::<Vec<_>>();
        for (unit, block) in projected.iter().zip(expected_blocks) {
            assert_eq!(unit.object_checksum, group.checksum);
            assert_eq!(unit.object_encoded_bytes, encoded.bytes.len() as u64);
            assert_eq!(unit.offset, block.offset);
            assert_eq!(unit.encoded_bytes, block.bytes);
            assert_eq!(
                unit.decoded_bytes,
                u64::from(block.rows) * EXACT_ROW_BYTES as u64
            );
            assert!(unit.offset + u64::from(unit.encoded_bytes) <= unit.object_encoded_bytes);
        }
        assert_eq!(projected[0].path, expected_path);
        assert_eq!(
            projected[1].encoded_bytes, projected[2].encoded_bytes,
            "the current uncompressed encoder must not claim a compressibility-dependent size"
        );
        let ranked = authority
            .iter()
            .flat_map(|card| card.iter().map(|record| record.record_id))
            .collect::<Vec<_>>();
        let census = v22_census_layout_prefix(
            &projected,
            &ranked,
            EXACT_ROW_BYTES as u64,
            encoded.bytes.len() as u64,
            projected.len(),
            5,
        )
        .unwrap();
        assert!(census.eligible);
        assert_eq!(census.selected_rows, 98);
        assert_eq!(census.projected_objects.len(), 1);
        assert_eq!(census.projected_objects[0].checksum, group.checksum);
        assert_eq!(
            census.projected_objects[0].encoded_bytes,
            encoded.bytes.len() as u64
        );

        let mut mismatched_authority = authority.clone();
        mismatched_authority[0][0].canonical_record_id = b"wrong-record".to_vec().into();
        assert!(
            project_v22_encoded_cell_card_group(
                &pages,
                &mismatched_authority,
                DIMENSIONS,
                VectorElementType::Float32,
                "v22-stage-l/layout",
            )
            .is_err()
        );
        let mut duplicate_numeric_authority = authority.clone();
        duplicate_numeric_authority[1][0].record_id = duplicate_numeric_authority[0][0].record_id;
        assert!(
            project_v22_encoded_cell_card_group(
                &pages,
                &duplicate_numeric_authority,
                DIMENSIONS,
                VectorElementType::Float32,
                "v22-stage-l/layout",
            )
            .is_err()
        );
        let mut wrong_width_pages = pages.clone();
        wrong_width_pages[0].rows[0].exact.pop();
        assert!(
            project_v22_encoded_cell_card_group(
                &wrong_width_pages,
                &authority,
                DIMENSIONS,
                VectorElementType::Float32,
                "v22-stage-l/layout",
            )
            .is_err()
        );

        const WIDE_DIMENSIONS: usize = 1536;
        const WIDE_ROW_BYTES: usize = WIDE_DIMENSIONS * std::mem::size_of::<f32>();
        let wide_records = (0_u64..17)
            .map(|record_id| V22EncodedRecordAuthority {
                canonical_record_id: format!("wide-{record_id:04}").into_bytes().into(),
                record_id,
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let wide_page = GlobalLeafPageInput {
            cell_index: 9,
            leaf_ordinal: 4,
            centroid_code: vec![9, 4],
            rows: wide_records
                .iter()
                .map(|record| GlobalLeafRowInput {
                    id: RecordId::from_bytes(record.canonical_record_id.to_vec()),
                    stamp: MutationStamp::new(
                        MutationVersion::from_parts(record.record_id + 1, [9; 16]),
                        [record.record_id as u8; 32],
                    ),
                    code: GlobalLeafCodeInput::from(vec![9, record.record_id as u8]),
                    exact: vec![record.record_id as u8; WIDE_ROW_BYTES],
                })
                .collect(),
        };
        let wide_projection = project_v22_encoded_cell_card_group(
            &[wide_page],
            &[wide_records],
            WIDE_DIMENSIONS,
            VectorElementType::Float32,
            "v22-stage-l/wide",
        )
        .unwrap();
        assert_eq!(
            wide_projection
                .units
                .iter()
                .map(|unit| unit.record_ids.len())
                .collect::<Vec<_>>(),
            [16, 1],
            "the real encoder must apply its 96-KiB decoded payload cap below the 32-row clamp"
        );
    }

    #[test]
    fn v22_layout_oracle_reuses_exact_wave_bounds_and_reports_purity() {
        let semantic = [V22ProjectedUnit {
            path: "semantic/group.arrow".to_string(),
            object_checksum: [1; 32],
            object_encoded_bytes: 5632,
            offset: 4096,
            encoded_bytes: 1536,
            decoded_bytes: 1536,
            record_ids: vec![10, 20, 30, 40].into(),
        }];
        let census =
            v22_census_layout_prefix(&semantic, &[10, 20, 30], 384, 1_048_576, 4, 2).unwrap();
        assert_eq!(census.useful_bytes, 1152);
        assert_eq!(census.selected_bytes, 1536);
        assert_eq!(census.physical_bytes, 1536);
        assert_eq!(census.requests, 1);
        assert_eq!(census.selected_rows, 4);
        assert_eq!(census.rows_per_range.as_ref(), &[4]);
        assert_eq!(census.blocks_per_range.as_ref(), &[1]);
        assert_eq!(census.packing_purity_ppm, 750_000);
        assert_eq!(census.speculative_bytes, 0);
        assert_eq!(census.projected_objects.len(), 1);
        assert_eq!(census.projected_objects[0].checksum, [1; 32]);
        assert_eq!(census.projected_objects[0].encoded_bytes, 5632);
        assert_eq!(census.physical_amplification_ppm, 1_000_000);
        assert_eq!(census.limiting_bound, V22LayoutLimitingBound::Eligible);
        assert!(census.eligible);

        let physical = [
            V22ProjectedUnit {
                path: "physical/a.arrow".to_string(),
                object_checksum: [2; 32],
                object_encoded_bytes: 512,
                offset: 0,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![10].into(),
            },
            V22ProjectedUnit {
                path: "physical/b.arrow".to_string(),
                object_checksum: [3; 32],
                object_encoded_bytes: 512,
                offset: 0,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![20].into(),
            },
            V22ProjectedUnit {
                path: "physical/c.arrow".to_string(),
                object_checksum: [4; 32],
                object_encoded_bytes: 512,
                offset: 0,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![30].into(),
            },
        ];
        let census =
            v22_census_layout_prefix(&physical, &[10, 20, 30], 384, 1_048_576, 4, 2).unwrap();
        assert_eq!(census.requests, 3);
        assert_eq!(census.rows_per_range.as_ref(), &[1, 1, 1]);
        let request_limited =
            v22_census_layout_prefix(&physical, &[10, 20, 30], 384, 1_048_576, 2, 5).unwrap();
        assert_eq!(request_limited.requests, 3);
        assert_eq!(
            request_limited.limiting_bound,
            V22LayoutLimitingBound::Requests
        );
        assert!(!request_limited.eligible);

        let coalesced = [
            V22ProjectedUnit {
                path: "coalesced.arrow".to_string(),
                object_checksum: [5; 32],
                object_encoded_bytes: 250,
                offset: 0,
                encoded_bytes: 100,
                decoded_bytes: 384,
                record_ids: vec![10].into(),
            },
            V22ProjectedUnit {
                path: "coalesced.arrow".to_string(),
                object_checksum: [5; 32],
                object_encoded_bytes: 250,
                offset: 100,
                encoded_bytes: 50,
                decoded_bytes: 384,
                record_ids: vec![99].into(),
            },
            V22ProjectedUnit {
                path: "coalesced.arrow".to_string(),
                object_checksum: [5; 32],
                object_encoded_bytes: 250,
                offset: 150,
                encoded_bytes: 100,
                decoded_bytes: 384,
                record_ids: vec![20].into(),
            },
        ];
        let census = v22_census_layout_prefix(&coalesced, &[10, 20], 384, 250, 1, 2).unwrap();
        assert_eq!(census.selected_bytes, 200);
        assert_eq!(census.physical_bytes, 250);
        assert_eq!(census.requests, 1);
        assert_eq!(census.rows_per_range.as_ref(), &[2]);
        assert_eq!(census.blocks_per_range.as_ref(), &[2]);
        assert_eq!(census.physical_amplification_ppm, 1_250_000);
        assert_eq!(census.packing_purity_ppm, 3_072_000);
        assert_eq!(census.speculative_bytes, 50);
        assert!(census.eligible);

        let tighter_bytes =
            v22_census_layout_prefix(&coalesced, &[10, 20], 384, 200, 2, 2).unwrap();
        assert_eq!(tighter_bytes.selected_bytes, 200);
        assert_eq!(tighter_bytes.physical_bytes, 200);
        assert_eq!(tighter_bytes.requests, 2);
        assert_eq!(
            tighter_bytes.limiting_bound,
            V22LayoutLimitingBound::Eligible
        );
        assert!(tighter_bytes.eligible);

        let request_limited =
            v22_census_layout_prefix(&coalesced, &[10, 20], 384, 250, 1, 1).unwrap();
        assert_eq!(request_limited.physical_bytes, 200);
        assert_eq!(request_limited.requests, 2);
        assert_eq!(
            request_limited.limiting_bound,
            V22LayoutLimitingBound::Amplification
        );
        assert!(!request_limited.eligible);
        let byte_limited = v22_census_layout_prefix(&coalesced, &[10, 20], 384, 199, 4, 2).unwrap();
        assert_eq!(byte_limited.selected_bytes, 200);
        assert_eq!(byte_limited.limiting_bound, V22LayoutLimitingBound::Bytes);
        assert!(!byte_limited.eligible);
    }

    #[test]
    fn v22_layout_oracle_rejects_malformed_projected_ranges() {
        let valid = V22ProjectedUnit {
            path: "group.arrow".to_string(),
            object_checksum: [6; 32],
            object_encoded_bytes: 1024,
            offset: 0,
            encoded_bytes: 512,
            decoded_bytes: 384,
            record_ids: vec![10].into(),
        };
        assert!(v22_census_layout_prefix(&[valid.clone()], &[10], 384, 1_048_576, 4, 2).is_ok());
        assert!(v22_census_layout_prefix(&[], &[10], 384, 1_048_576, 4, 2).is_err());
        assert!(v22_census_layout_prefix(&[valid.clone()], &[], 384, 1_048_576, 4, 2).is_err());
        assert!(v22_census_layout_prefix(&[valid.clone()], &[11], 384, 1_048_576, 4, 2).is_err());
        assert!(
            v22_census_layout_prefix(&[valid.clone()], &[10, 10], 384, 1_048_576, 4, 2).is_err()
        );
        let short_object = V22ProjectedUnit {
            object_encoded_bytes: 511,
            ..valid.clone()
        };
        assert!(v22_census_layout_prefix(&[short_object], &[10], 384, 1_048_576, 4, 2).is_err());
        let overlapping = [
            valid.clone(),
            V22ProjectedUnit {
                path: valid.path.clone(),
                object_checksum: valid.object_checksum,
                object_encoded_bytes: 1024,
                offset: 100,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![11].into(),
            },
        ];
        assert!(v22_census_layout_prefix(&overlapping, &[10, 11], 384, 1_048_576, 4, 2).is_err());
        let conflicting_checksum = [
            valid.clone(),
            V22ProjectedUnit {
                path: valid.path.clone(),
                object_checksum: [8; 32],
                object_encoded_bytes: 1024,
                offset: 512,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![11].into(),
            },
        ];
        assert!(
            v22_census_layout_prefix(&conflicting_checksum, &[10, 11], 384, 1_048_576, 4, 2)
                .is_err()
        );
        let conflicting_object_length = [
            valid.clone(),
            V22ProjectedUnit {
                path: valid.path.clone(),
                object_checksum: valid.object_checksum,
                object_encoded_bytes: 2048,
                offset: 512,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![11].into(),
            },
        ];
        assert!(
            v22_census_layout_prefix(&conflicting_object_length, &[10, 11], 384, 1_048_576, 4, 2)
                .is_err()
        );
        let duplicate_row = [
            valid.clone(),
            V22ProjectedUnit {
                path: "other.arrow".to_string(),
                object_checksum: [7; 32],
                object_encoded_bytes: 512,
                offset: 0,
                encoded_bytes: 512,
                decoded_bytes: 384,
                record_ids: vec![10].into(),
            },
        ];
        assert!(v22_census_layout_prefix(&duplicate_row, &[10], 384, 1_048_576, 4, 2).is_err());
        let empty_path = V22ProjectedUnit {
            path: String::new(),
            ..valid.clone()
        };
        assert!(v22_census_layout_prefix(&[empty_path], &[10], 384, 1_048_576, 4, 2).is_err());
        let wrong_decoded_size = V22ProjectedUnit {
            path: "group.arrow".to_string(),
            object_checksum: [6; 32],
            object_encoded_bytes: 64,
            offset: 0,
            encoded_bytes: 64,
            decoded_bytes: 383,
            record_ids: vec![10].into(),
        };
        assert!(
            v22_census_layout_prefix(&[wrong_decoded_size], &[10], 384, 1_048_576, 4, 2).is_err()
        );
        assert!(v22_census_layout_prefix(&[valid], &[10], 384, 1_048_576, 4, 0).is_err());
    }
}
