use std::{collections::BTreeMap, mem::size_of};

use crate::{
    BorsukError, Result, VectorElementType, global_leaf::GLOBAL_LEAF_VECTOR_PAYLOAD_BYTES,
    global_pq_sidecar::GlobalScanQuantizer, record::RecordId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V21FeasibilityArm {
    pub bundle_row_limit: u16,
    pub selector_span: u16,
    pub hedge_delay_ms: Option<u16>,
}

impl V21FeasibilityArm {
    pub fn validate(&self) -> Result<()> {
        if !matches!(self.bundle_row_limit, 128 | 256)
            || !matches!(self.selector_span, 32 | 64)
            || !matches!(self.hedge_delay_ms, None | Some(20 | 35))
        {
            return Err(BorsukError::InvalidSearchOptions(
                "V21 feasibility arm is outside the frozen matrix".to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn primary_request_limit(&self) -> usize {
        4_usize.saturating_sub(usize::from(self.hedge_delay_ms.is_some()))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct V21ProjectedRow {
    pub(crate) id: RecordId,
    pub(crate) source_ordinal: u64,
    pub(crate) code: Vec<u8>,
    pub(crate) exact: Vec<u8>,
}

#[derive(Debug, Clone)]
pub(crate) struct V21ProjectedPage {
    pub(crate) cell_index: u32,
    pub(crate) leaf_ordinal: u32,
    pub(crate) group_ordinal: u32,
    pub(crate) group_path: String,
    pub(crate) group_checksum: [u8; 32],
    pub(crate) offset: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) rows: Vec<V21ProjectedRow>,
}

#[derive(Debug, Clone)]
pub(crate) struct V21ProjectedRegion {
    pub(crate) centroid_code: Vec<u8>,
    pub(crate) spread_bits: u16,
    pub(crate) row_start: u16,
    pub(crate) row_count: u16,
}

#[derive(Debug, Clone)]
pub(crate) struct V21ProjectedBundle {
    pub(crate) cell_index: u32,
    pub(crate) bundle_ordinal: u32,
    pub(crate) group_ordinal: u32,
    pub(crate) group_path: String,
    pub(crate) group_checksum: [u8; 32],
    pub(crate) offset: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) rows: Vec<V21ProjectedRow>,
    pub(crate) regions: Vec<V21ProjectedRegion>,
}

#[derive(Debug, Clone)]
pub(crate) struct V21ProjectedDirectory {
    pub(crate) bundles: Vec<V21ProjectedBundle>,
    pub(crate) selector_capacity_bytes: u64,
    pub(crate) diagnostic_working_set_bytes: u64,
    pub(crate) rows: u64,
    pub(crate) regions: u64,
    selector_slabs: V21SelectorSlabs,
}

#[derive(Debug, Clone)]
struct V21SelectorSlabs {
    group_dictionary: Vec<V21SelectorGroup>,
    bundle_group_ordinals: Vec<u32>,
    bundle_cell_indexes: Vec<u32>,
    bundle_ordinals: Vec<u32>,
    bundle_offsets: Vec<u64>,
    bundle_physical_bytes: Vec<u64>,
    bundle_row_counts: Vec<u16>,
    bundle_region_offsets: Vec<u32>,
    region_codes: Vec<u8>,
    region_spreads: Vec<u16>,
    region_row_starts: Vec<u16>,
    region_row_counts: Vec<u16>,
    cell_offsets: Vec<u32>,
}

#[derive(Debug, Clone)]
struct V21SelectorGroup {
    ordinal: u32,
    path: Box<str>,
    checksum: [u8; 32],
}

impl V21SelectorSlabs {
    fn from_bundles(bundles: &[V21ProjectedBundle]) -> Result<Self> {
        let region_count = bundles.iter().try_fold(0_usize, |total, bundle| {
            total.checked_add(bundle.regions.len()).ok_or_else(|| {
                BorsukError::InvalidStorage("V21 selector region count overflows".to_string())
            })
        })?;
        let code_bytes = bundles.iter().try_fold(0_usize, |total, bundle| {
            bundle.regions.iter().try_fold(total, |total, region| {
                total
                    .checked_add(region.centroid_code.len())
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage("V21 selector code bytes overflow".to_string())
                    })
            })
        })?;
        let mut groups = BTreeMap::<u32, (&str, [u8; 32])>::new();
        for bundle in bundles {
            if let Some((path, checksum)) = groups.get(&bundle.group_ordinal) {
                if *path != bundle.group_path || *checksum != bundle.group_checksum {
                    return Err(BorsukError::InvalidStorage(
                        "V21 group ordinal has conflicting authority".to_string(),
                    ));
                }
            } else {
                groups.insert(
                    bundle.group_ordinal,
                    (&bundle.group_path, bundle.group_checksum),
                );
            }
        }
        let group_dictionary = groups
            .into_iter()
            .map(|(ordinal, (path, checksum))| V21SelectorGroup {
                ordinal,
                path: Box::<str>::from(path),
                checksum,
            })
            .collect::<Vec<_>>();
        let maximum_cell = bundles
            .iter()
            .map(|bundle| bundle.cell_index)
            .max()
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V21 selector has no bundles".to_string())
            })?;
        let cell_count = usize::try_from(maximum_cell)
            .ok()
            .and_then(|cell| cell.checked_add(1))
            .ok_or_else(|| {
                BorsukError::InvalidStorage("V21 selector cell count overflows".to_string())
            })?;
        let mut slabs = Self {
            group_dictionary,
            bundle_group_ordinals: Vec::with_capacity(bundles.len()),
            bundle_cell_indexes: Vec::with_capacity(bundles.len()),
            bundle_ordinals: Vec::with_capacity(bundles.len()),
            bundle_offsets: Vec::with_capacity(bundles.len()),
            bundle_physical_bytes: Vec::with_capacity(bundles.len()),
            bundle_row_counts: Vec::with_capacity(bundles.len()),
            bundle_region_offsets: Vec::with_capacity(bundles.len() + 1),
            region_codes: Vec::with_capacity(code_bytes),
            region_spreads: Vec::with_capacity(region_count),
            region_row_starts: Vec::with_capacity(region_count),
            region_row_counts: Vec::with_capacity(region_count),
            cell_offsets: vec![0; cell_count + 1],
        };
        slabs.bundle_region_offsets.push(0);
        for bundle in bundles {
            slabs.bundle_group_ordinals.push(bundle.group_ordinal);
            slabs.bundle_cell_indexes.push(bundle.cell_index);
            slabs.bundle_ordinals.push(bundle.bundle_ordinal);
            slabs.bundle_offsets.push(bundle.offset);
            slabs.bundle_physical_bytes.push(bundle.physical_bytes);
            slabs
                .bundle_row_counts
                .push(u16::try_from(bundle.rows.len()).map_err(|_| {
                    BorsukError::InvalidStorage("V21 selector bundle rows exceed u16".to_string())
                })?);
            for region in &bundle.regions {
                slabs.region_codes.extend_from_slice(&region.centroid_code);
                slabs.region_spreads.push(region.spread_bits);
                slabs.region_row_starts.push(region.row_start);
                slabs.region_row_counts.push(region.row_count);
            }
            slabs
                .bundle_region_offsets
                .push(u32::try_from(slabs.region_spreads.len()).map_err(|_| {
                    BorsukError::InvalidStorage(
                        "V21 selector region offset exceeds u32".to_string(),
                    )
                })?);
            let cell = usize::try_from(bundle.cell_index).map_err(|_| {
                BorsukError::InvalidStorage("V21 selector cell exceeds usize".to_string())
            })?;
            slabs.cell_offsets[cell + 1] =
                slabs.cell_offsets[cell + 1].checked_add(1).ok_or_else(|| {
                    BorsukError::InvalidStorage("V21 selector cell rows overflow".to_string())
                })?;
        }
        for cell in 1..slabs.cell_offsets.len() {
            let previous = slabs.cell_offsets[cell - 1];
            slabs.cell_offsets[cell] =
                slabs.cell_offsets[cell]
                    .checked_add(previous)
                    .ok_or_else(|| {
                        BorsukError::InvalidStorage(
                            "V21 selector cell offset overflows".to_string(),
                        )
                    })?;
        }
        Ok(slabs)
    }

    fn capacity_bytes(&self) -> u64 {
        fn bytes<T>(values: &Vec<T>) -> u64 {
            u64::try_from(values.capacity().saturating_mul(size_of::<T>())).unwrap_or(u64::MAX)
        }
        bytes(&self.group_dictionary)
            .saturating_add(
                self.group_dictionary
                    .iter()
                    .map(|group| group.path.len() as u64)
                    .sum::<u64>(),
            )
            .saturating_add(bytes(&self.bundle_group_ordinals))
            .saturating_add(bytes(&self.bundle_cell_indexes))
            .saturating_add(bytes(&self.bundle_ordinals))
            .saturating_add(bytes(&self.bundle_offsets))
            .saturating_add(bytes(&self.bundle_physical_bytes))
            .saturating_add(bytes(&self.bundle_row_counts))
            .saturating_add(bytes(&self.bundle_region_offsets))
            .saturating_add(bytes(&self.region_codes))
            .saturating_add(bytes(&self.region_spreads))
            .saturating_add(bytes(&self.region_row_starts))
            .saturating_add(bytes(&self.region_row_counts))
            .saturating_add(bytes(&self.cell_offsets))
    }
}

impl V21ProjectedDirectory {
    #[cfg(test)]
    fn bundle_row_counts(&self) -> Vec<usize> {
        self.bundles
            .iter()
            .map(|bundle| bundle.rows.len())
            .collect()
    }

    #[cfg(test)]
    fn region_row_counts(&self) -> Vec<usize> {
        self.bundles
            .iter()
            .flat_map(|bundle| &bundle.regions)
            .map(|region| usize::from(region.row_count))
            .collect()
    }

    #[cfg(test)]
    fn canonical_source_ordinals(&self) -> Vec<u64> {
        self.bundles
            .iter()
            .flat_map(|bundle| &bundle.rows)
            .map(|row| row.source_ordinal)
            .collect()
    }

    #[cfg(test)]
    fn selector_identity(&self) -> Vec<(u32, u32, u32, u64, u64, Vec<(Vec<u8>, u16, u16, u16)>)> {
        self.bundles
            .iter()
            .map(|bundle| {
                (
                    bundle.cell_index,
                    bundle.bundle_ordinal,
                    bundle.group_ordinal,
                    bundle.offset,
                    bundle.physical_bytes,
                    bundle
                        .regions
                        .iter()
                        .map(|region| {
                            (
                                region.centroid_code.clone(),
                                region.spread_bits,
                                region.row_start,
                                region.row_count,
                            )
                        })
                        .collect(),
                )
            })
            .collect()
    }

    #[cfg(test)]
    fn group_identity(&self) -> Vec<(u32, String, [u8; 32])> {
        self.selector_slabs
            .group_dictionary
            .iter()
            .map(|group| (group.ordinal, group.path.to_string(), group.checksum))
            .collect()
    }
}

fn f16_rounded_up(value: f32) -> Result<u16> {
    if !value.is_finite() {
        return Err(BorsukError::InvalidStorage(
            "V21 selector spread is non-finite".to_string(),
        ));
    }
    let rounded = half::f16::from_f32(value);
    if f32::from(rounded) >= value {
        return Ok(rounded.to_bits());
    }
    let bits = rounded.to_bits();
    let next = if bits & 0x8000 == 0 {
        bits.checked_add(1)
    } else {
        bits.checked_sub(1)
    }
    .ok_or_else(|| {
        BorsukError::InvalidStorage("V21 selector spread cannot round outward".to_string())
    })?;
    let next = half::f16::from_bits(next);
    if !next.is_finite() || f32::from(next) < value {
        return Err(BorsukError::InvalidStorage(
            "V21 selector spread cannot be represented by f16".to_string(),
        ));
    }
    Ok(next.to_bits())
}

fn build_projected_regions(
    rows: &[V21ProjectedRow],
    dimensions: usize,
    element_type: VectorElementType,
    normalize: bool,
    quantizer: &GlobalScanQuantizer,
    selector_span: usize,
) -> Result<Vec<V21ProjectedRegion>> {
    let mut regions = Vec::with_capacity(rows.len().div_ceil(selector_span));
    for (region_index, region_rows) in rows.chunks(selector_span).enumerate() {
        let mut sum = vec![0.0_f64; dimensions];
        for row in region_rows {
            let decoded = element_type.decode_fixed_width(&row.exact, dimensions)?;
            let geometry = if normalize {
                crate::metric::unit_l2_normalized(&decoded)
            } else {
                decoded
            };
            for (total, value) in sum.iter_mut().zip(geometry) {
                *total += f64::from(value);
            }
        }
        let denominator = region_rows.len() as f64;
        let centroid = sum
            .into_iter()
            .map(|value| (value / denominator) as f32)
            .collect::<Vec<_>>();
        let centroid_code = quantizer.encode(&centroid)?;
        let spread = quantizer
            .score_codes(&centroid, region_rows.iter().map(|row| row.code.as_slice()))?
            .into_iter()
            .try_fold(f32::NEG_INFINITY, |maximum, score| {
                if score.is_finite() {
                    Ok::<_, BorsukError>(maximum.max(score))
                } else {
                    Err(BorsukError::InvalidStorage(
                        "V21 selector spread input is non-finite".to_string(),
                    ))
                }
            })?;
        regions.push(V21ProjectedRegion {
            centroid_code,
            spread_bits: f16_rounded_up(spread)?,
            row_start: u16::try_from(region_index.saturating_mul(selector_span)).map_err(|_| {
                BorsukError::InvalidStorage("V21 selector row start exceeds u16".to_string())
            })?,
            row_count: u16::try_from(region_rows.len()).map_err(|_| {
                BorsukError::InvalidStorage("V21 selector row count exceeds u16".to_string())
            })?,
        });
    }
    Ok(regions)
}

fn finish_projected_bundle(
    bundles: &mut Vec<V21ProjectedBundle>,
    next_bundle: &mut BTreeMap<u32, u32>,
    mut pages: Vec<V21ProjectedPage>,
    dimensions: usize,
    element_type: VectorElementType,
    normalize: bool,
    quantizer: &GlobalScanQuantizer,
    selector_span: usize,
) -> Result<()> {
    let first = pages
        .first()
        .ok_or_else(|| BorsukError::InvalidStorage("V21 projected bundle is empty".to_string()))?;
    let cell_index = first.cell_index;
    let group_ordinal = first.group_ordinal;
    let group_path = first.group_path.clone();
    let group_checksum = first.group_checksum;
    let offset = first.offset;
    let physical_bytes = pages.iter().try_fold(0_u64, |total, page| {
        total.checked_add(page.physical_bytes).ok_or_else(|| {
            BorsukError::InvalidStorage("V21 projected bundle bytes overflow".to_string())
        })
    })?;
    let rows = pages
        .iter_mut()
        .flat_map(|page| std::mem::take(&mut page.rows))
        .collect::<Vec<_>>();
    let regions = build_projected_regions(
        &rows,
        dimensions,
        element_type,
        normalize,
        quantizer,
        selector_span,
    )?;
    let bundle_ordinal = next_bundle.entry(cell_index).or_default();
    bundles.push(V21ProjectedBundle {
        cell_index,
        bundle_ordinal: *bundle_ordinal,
        group_ordinal,
        group_path,
        group_checksum,
        offset,
        physical_bytes,
        rows,
        regions,
    });
    *bundle_ordinal = bundle_ordinal.checked_add(1).ok_or_else(|| {
        BorsukError::InvalidStorage("V21 projected bundle ordinal overflows".to_string())
    })?;
    Ok(())
}

pub(crate) fn build_v21_projected_directory(
    mut pages: Vec<V21ProjectedPage>,
    dimensions: usize,
    element_type: VectorElementType,
    normalize: bool,
    quantizer: &GlobalScanQuantizer,
    arm: V21FeasibilityArm,
) -> Result<V21ProjectedDirectory> {
    arm.validate()?;
    if pages.is_empty() {
        return Err(BorsukError::InvalidStorage(
            "V21 projected directory has no pages".to_string(),
        ));
    }
    let row_bytes = element_type.fixed_width_bytes(dimensions)?;
    let payload_rows = GLOBAL_LEAF_VECTOR_PAYLOAD_BYTES / row_bytes;
    let maximum_rows = usize::from(arm.bundle_row_limit).min(payload_rows);
    if maximum_rows == 0 {
        return Err(BorsukError::InvalidStorage(
            "V21 exact row exceeds the payload cap".to_string(),
        ));
    }
    let code_width = pages
        .first()
        .and_then(|page| page.rows.first())
        .map(|row| row.code.len())
        .filter(|width| *width > 0)
        .ok_or_else(|| {
            BorsukError::InvalidStorage("V21 projected code width is empty".to_string())
        })?;
    for page in &pages {
        if page.rows.is_empty()
            || page.physical_bytes == 0
            || page.rows.len() > maximum_rows
            || page
                .rows
                .iter()
                .any(|row| row.code.len() != code_width || row.exact.len() != row_bytes)
        {
            return Err(BorsukError::InvalidStorage(
                "V21 projected page authority is invalid".to_string(),
            ));
        }
    }
    pages.sort_unstable_by_key(|page| {
        (
            page.cell_index,
            page.group_ordinal,
            page.leaf_ordinal,
            page.offset,
        )
    });
    let mut bundles = Vec::new();
    let mut next_bundle = BTreeMap::new();
    let mut pending = Vec::<V21ProjectedPage>::new();
    for page in pages {
        let can_merge = pending.last().is_none_or(|previous| {
            let pending_rows = pending.iter().map(|page| page.rows.len()).sum::<usize>();
            let pending_bytes = pending.iter().map(|page| page.physical_bytes).sum::<u64>();
            previous.cell_index == page.cell_index
                && previous.group_ordinal == page.group_ordinal
                && previous.group_path == page.group_path
                && previous.group_checksum == page.group_checksum
                && previous.leaf_ordinal.checked_add(1) == Some(page.leaf_ordinal)
                && previous.offset.checked_add(previous.physical_bytes) == Some(page.offset)
                && pending_rows.saturating_add(page.rows.len()) <= maximum_rows
                && pending_bytes.saturating_add(page.physical_bytes)
                    <= crate::global_leaf::GLOBAL_LEAF_MAX_ENCODED_BYTES
        });
        if !can_merge {
            finish_projected_bundle(
                &mut bundles,
                &mut next_bundle,
                std::mem::take(&mut pending),
                dimensions,
                element_type,
                normalize,
                quantizer,
                usize::from(arm.selector_span),
            )?;
        }
        pending.push(page);
    }
    if !pending.is_empty() {
        finish_projected_bundle(
            &mut bundles,
            &mut next_bundle,
            pending,
            dimensions,
            element_type,
            normalize,
            quantizer,
            usize::from(arm.selector_span),
        )?;
    }
    let rows = bundles.iter().try_fold(0_u64, |total, bundle| {
        total
            .checked_add(bundle.rows.len() as u64)
            .ok_or_else(|| BorsukError::InvalidStorage("V21 row total overflows".to_string()))
    })?;
    let regions = bundles.iter().try_fold(0_u64, |total, bundle| {
        total
            .checked_add(bundle.regions.len() as u64)
            .ok_or_else(|| BorsukError::InvalidStorage("V21 region total overflows".to_string()))
    })?;
    let diagnostic_working_set_bytes = bundles.iter().fold(0_u64, |total, bundle| {
        bundle.rows.iter().fold(total, |total, row| {
            total
                .saturating_add(row.id.as_bytes().len() as u64)
                .saturating_add(row.code.capacity() as u64)
                .saturating_add(row.exact.capacity() as u64)
        })
    });
    let selector_slabs = V21SelectorSlabs::from_bundles(&bundles)?;
    let selector_capacity_bytes = selector_slabs.capacity_bytes();
    Ok(V21ProjectedDirectory {
        selector_capacity_bytes,
        diagnostic_working_set_bytes,
        rows,
        regions,
        bundles,
        selector_slabs,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        V21FeasibilityArm, V21ProjectedPage, V21ProjectedRow, build_v21_projected_directory,
    };
    use crate::{
        VectorElementType,
        global_pq_sidecar::GlobalScanQuantizer,
        record::RecordId,
        rotated_product_quantizer::{
            ProductQuantizerConfig, ProductRotation, RotatedProductQuantizer,
        },
    };

    fn test_quantizer(dimensions: usize) -> GlobalScanQuantizer {
        let training = (0..16)
            .map(|row| {
                (0..dimensions)
                    .map(|dimension| (row * 3 + dimension) as f32 / 17.0)
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        GlobalScanQuantizer::from(
            RotatedProductQuantizer::fit(
                ProductQuantizerConfig {
                    rotation: ProductRotation::Identity,
                    seed: 7,
                    dimensions,
                    subspaces: 1,
                    centroids: 4,
                    sample_limit: training.len(),
                    iterations: 2,
                },
                &training,
            )
            .unwrap(),
        )
    }

    fn projected_row(
        quantizer: &GlobalScanQuantizer,
        source_ordinal: u64,
        dimensions: usize,
    ) -> V21ProjectedRow {
        let vector = (0..dimensions)
            .map(|dimension| (source_ordinal as usize % 17 + dimension) as f32 / 17.0)
            .collect::<Vec<_>>();
        V21ProjectedRow {
            id: RecordId::from(format!("row-{source_ordinal:04}").as_str()),
            source_ordinal,
            code: quantizer.encode(&vector).unwrap(),
            exact: vector.into_iter().flat_map(f32::to_le_bytes).collect(),
        }
    }

    fn projected_page(
        quantizer: &GlobalScanQuantizer,
        cell_index: u32,
        leaf_ordinal: u32,
        group_ordinal: u32,
        offset: u64,
        first_source_ordinal: u64,
        rows: usize,
        dimensions: usize,
    ) -> V21ProjectedPage {
        V21ProjectedPage {
            cell_index,
            leaf_ordinal,
            group_ordinal,
            group_path: format!("groups/{group_ordinal}/bundle.arrow"),
            group_checksum: [u8::try_from(group_ordinal).unwrap(); 32],
            offset,
            physical_bytes: u64::try_from(rows).unwrap() * 100,
            rows: (0..rows)
                .map(|row| {
                    projected_row(
                        quantizer,
                        first_source_ordinal + u64::try_from(row).unwrap(),
                        dimensions,
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn v21_feasibility_arm_accepts_only_the_frozen_matrix() {
        for bundle_row_limit in [128, 256] {
            for selector_span in [32, 64] {
                for hedge_delay_ms in [None, Some(20), Some(35)] {
                    V21FeasibilityArm {
                        bundle_row_limit,
                        selector_span,
                        hedge_delay_ms,
                    }
                    .validate()
                    .unwrap();
                }
            }
        }
        assert_eq!(
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            }
            .primary_request_limit(),
            4
        );
        assert_eq!(
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: Some(20),
            }
            .primary_request_limit(),
            3
        );
    }

    #[test]
    fn v21_feasibility_arm_rejects_unregistered_values() {
        for arm in [
            V21FeasibilityArm {
                bundle_row_limit: 0,
                selector_span: 32,
                hedge_delay_ms: None,
            },
            V21FeasibilityArm {
                bundle_row_limit: 192,
                selector_span: 32,
                hedge_delay_ms: None,
            },
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 16,
                hedge_delay_ms: None,
            },
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: Some(25),
            },
        ] {
            assert!(arm.validate().is_err(), "accepted {arm:?}");
        }
    }

    #[test]
    fn v21_projected_directory_merges_only_contiguous_same_authority_pages() {
        let quantizer = test_quantizer(2);
        let mut pages = (0..8)
            .map(|block| {
                projected_page(
                    &quantizer,
                    7,
                    block,
                    3,
                    1_000 + u64::from(block) * 3_200,
                    u64::from(block) * 32,
                    32,
                    2,
                )
            })
            .collect::<Vec<_>>();
        pages.push(projected_page(&quantizer, 7, 8, 4, 0, 256, 1, 2));
        let canonical_pages = pages.clone();
        pages.reverse();

        let directory = build_v21_projected_directory(
            pages,
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();
        let canonical = build_v21_projected_directory(
            canonical_pages,
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();

        assert_eq!(directory.bundle_row_counts(), [256, 1]);
        assert_eq!(directory.region_row_counts(), [64, 64, 64, 64, 1]);
        assert_eq!(
            directory.canonical_source_ordinals(),
            (0_u64..257).collect::<Vec<_>>()
        );
        assert_eq!(directory.selector_identity(), canonical.selector_identity());
        assert_eq!(directory.rows, 257);
        assert_eq!(directory.regions, 5);
        assert!(directory.selector_capacity_bytes > 0);
        assert_eq!(
            directory.selector_capacity_bytes,
            directory.selector_slabs.capacity_bytes()
        );
        assert!(directory.diagnostic_working_set_bytes > directory.selector_capacity_bytes);
    }

    #[test]
    fn v21_projected_directory_derives_high_dimension_bundle_rows_from_payload() {
        let quantizer = test_quantizer(768);
        let pages = vec![
            projected_page(&quantizer, 4, 0, 2, 0, 0, 32, 768),
            projected_page(&quantizer, 4, 1, 2, 3_200, 32, 32, 768),
        ];

        let directory = build_v21_projected_directory(
            pages,
            768,
            VectorElementType::Float32,
            false,
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();

        assert_eq!(directory.bundle_row_counts(), [32, 32]);
        assert_eq!(directory.region_row_counts(), [32, 32]);
    }

    #[test]
    fn v21_projected_directory_does_not_merge_physical_or_ordinal_gaps() {
        let quantizer = test_quantizer(2);
        let pages = vec![
            projected_page(&quantizer, 1, 0, 9, 0, 0, 1, 2),
            projected_page(&quantizer, 1, 1, 9, 200, 1, 1, 2),
            projected_page(&quantizer, 1, 3, 9, 300, 2, 1, 2),
        ];

        let directory = build_v21_projected_directory(
            pages,
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            V21FeasibilityArm {
                bundle_row_limit: 256,
                selector_span: 64,
                hedge_delay_ms: None,
            },
        )
        .unwrap();

        assert_eq!(directory.bundle_row_counts(), [1, 1, 1]);
    }

    #[test]
    fn v21_projected_directory_charges_group_dictionary_payload() {
        let quantizer = test_quantizer(2);
        let short = vec![projected_page(&quantizer, 1, 0, 9, 0, 0, 1, 2)];
        let mut long = short.clone();
        long[0].group_path = format!("groups/9/{}/bundle.arrow", "nested".repeat(40));
        let arm = V21FeasibilityArm {
            bundle_row_limit: 256,
            selector_span: 64,
            hedge_delay_ms: None,
        };

        let short = build_v21_projected_directory(
            short,
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            arm,
        )
        .unwrap();
        let long = build_v21_projected_directory(
            long,
            2,
            VectorElementType::Float32,
            false,
            &quantizer,
            arm,
        )
        .unwrap();

        assert_eq!(short.bundle_row_counts(), long.bundle_row_counts());
        assert_eq!(
            short.group_identity(),
            vec![(9, "groups/9/bundle.arrow".to_string(), [9; 32])]
        );
        assert!(long.selector_capacity_bytes > short.selector_capacity_bytes);
    }
}
