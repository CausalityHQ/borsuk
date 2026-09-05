use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use arrow_array::{
    ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, RecordBatch, UInt8Array, UInt32Array,
    UInt64Array,
};
use arrow_ipc::{MetadataVersion, writer::FileWriter, writer::IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use sha2::{Digest, Sha256};

use crate::{
    BorsukError, Result,
    v30_s3_pq::{V30CodePlanes, V30PqCodebook, V30PqReconstructor, V30PqWidth},
};

const DIMENSIONS: usize = 96;

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

#[derive(Debug, Clone, PartialEq)]
struct V33LeafPopulation {
    routing_leaf_ordinal: u32,
    group_ordinal: u32,
    rows: Vec<(u64, [f32; DIMENSIONS])>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V33RoutingRange {
    routing_leaf_ordinal: u32,
    code_parent_leaf_ordinal: u32,
    logical_start: u64,
    row_count: u64,
}

fn reconstruct_v33_leaf_populations(
    base_codebook: &V30PqCodebook,
    high_codebook: &V30PqCodebook,
    codes: &V30CodePlanes,
    code_parent_centers: &[[f32; DIMENSIONS]],
    ranges: &[V33RoutingRange],
    group_of_code_parent: &[u32],
) -> Result<Vec<V33LeafPopulation>> {
    if base_codebook.width() != V30PqWidth::Base24
        || high_codebook.width() != V30PqWidth::High48
        || ranges.is_empty()
        || code_parent_centers.is_empty()
        || code_parent_centers.len() != group_of_code_parent.len()
        || code_parent_centers
            .iter()
            .flatten()
            .any(|value| !value.is_finite())
    {
        return Err(invalid("V33 PQ reconstruction authority differs"));
    }
    let base = V30PqReconstructor::new(base_codebook)?;
    let high = V30PqReconstructor::new(high_codebook)?;
    let mut logical_start = 0_u64;
    let mut populations = Vec::with_capacity(ranges.len());
    for (expected_ordinal, range) in ranges.iter().enumerate() {
        if range.routing_leaf_ordinal != expected_ordinal as u32
            || range.logical_start != logical_start
            || range.row_count == 0
        {
            return Err(invalid("V33 routing range authority differs"));
        }
        let parent = usize::try_from(range.code_parent_leaf_ordinal)
            .map_err(|_| invalid("V33 code parent ordinal overflows"))?;
        let center = code_parent_centers
            .get(parent)
            .ok_or_else(|| invalid("V33 code parent ordinal differs"))?;
        let group_ordinal = *group_of_code_parent
            .get(parent)
            .ok_or_else(|| invalid("V33 code parent group differs"))?;
        let end = range
            .logical_start
            .checked_add(range.row_count)
            .ok_or_else(|| invalid("V33 routing range overflows"))?;
        let mut rows = Vec::with_capacity(
            usize::try_from(range.row_count)
                .map_err(|_| invalid("V33 routing population overflows"))?,
        );
        for logical in range.logical_start..end {
            let logical_index =
                usize::try_from(logical).map_err(|_| invalid("V33 logical ordinal overflows"))?;
            let (width, code) = codes.code(logical_index)?;
            let residual = match width {
                V30PqWidth::Base24 => base.reconstruct(code)?,
                V30PqWidth::High48 => high.reconstruct(code)?,
            };
            let mut reconstructed = [0.0_f32; DIMENSIONS];
            for dimension in 0..DIMENSIONS {
                reconstructed[dimension] = center[dimension] + residual[dimension];
            }
            if reconstructed.iter().any(|value| !value.is_finite()) {
                return Err(invalid("V33 reconstructed row is nonfinite"));
            }
            rows.push((logical, reconstructed));
        }
        populations.push(V33LeafPopulation {
            routing_leaf_ordinal: range.routing_leaf_ordinal,
            group_ordinal,
            rows,
        });
        logical_start = end;
    }
    if logical_start != codes.logical_rows() as u64
        || codes.materialized_rows() != codes.logical_rows()
    {
        return Err(invalid("V33 reconstructed logical coverage differs"));
    }
    Ok(populations)
}

#[derive(Debug, Clone, PartialEq)]
struct V33LeafShape {
    routing_leaf_ordinal: u32,
    group_ordinal: u32,
    logical_start: u64,
    population: u64,
    mean: [f32; DIMENSIONS],
    diagonal_variance: [f32; DIMENSIONS],
    scalar_moment: f32,
    maximum_radius: f32,
    split_dimension: usize,
    split_centers: [[f32; DIMENSIONS]; 2],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum V33ShapeArm {
    Centroid,
    ScalarMoment,
    DiagonalMoment,
    SplitCentroid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V33GroupPopulation {
    ordinal: u32,
    rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct V33ShapeControlBytes {
    scalar_summary_bytes: usize,
    scalar_extra_centers: usize,
    scalar_padding_bytes: usize,
    diagonal_summary_bytes: usize,
    diagonal_control_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct V33LeafShapeArtifact {
    role: &'static str,
    sha256: String,
    encoded_bytes: u64,
    row_count: u64,
    arrow: Vec<u8>,
}

fn v33_shape_control_bytes(leaf_count: usize) -> Result<V33ShapeControlBytes> {
    if leaf_count == 0 {
        return Err(invalid("V33 shape leaf count differs"));
    }
    let center_bytes = DIMENSIONS * size_of::<f32>();
    let scalar_extra_bytes = leaf_count
        .checked_mul(size_of::<f32>())
        .ok_or_else(|| invalid("V33 scalar control bytes overflow"))?;
    let scalar_extra_centers = scalar_extra_bytes / center_bytes;
    let scalar_padding_bytes = scalar_extra_bytes % center_bytes;
    let scalar_summary_bytes = leaf_count
        .checked_mul(center_bytes + size_of::<f32>())
        .ok_or_else(|| invalid("V33 scalar summary bytes overflow"))?;
    let diagonal_summary_bytes = leaf_count
        .checked_mul(center_bytes * 2)
        .ok_or_else(|| invalid("V33 diagonal summary bytes overflow"))?;
    Ok(V33ShapeControlBytes {
        scalar_summary_bytes,
        scalar_extra_centers,
        scalar_padding_bytes,
        diagonal_summary_bytes,
        diagonal_control_bytes: diagonal_summary_bytes,
    })
}

fn summarize_v33_leaf(population: &V33LeafPopulation) -> Result<V33LeafShape> {
    if population.rows.is_empty()
        || population
            .rows
            .iter()
            .any(|(_, row)| row.iter().any(|value| !value.is_finite()))
        || population
            .rows
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .collect::<BTreeSet<_>>()
            .len()
            != population.rows.len()
    {
        return Err(invalid("V33 leaf population differs"));
    }
    let count = population.rows.len() as f64;
    let mut mean64 = [0.0_f64; DIMENSIONS];
    for (_, row) in &population.rows {
        for dimension in 0..DIMENSIONS {
            mean64[dimension] += f64::from(row[dimension]);
        }
    }
    for value in &mut mean64 {
        *value /= count;
    }
    let mut variance64 = [0.0_f64; DIMENSIONS];
    for (_, row) in &population.rows {
        for dimension in 0..DIMENSIONS {
            let delta = f64::from(row[dimension]) - mean64[dimension];
            variance64[dimension] += delta * delta;
        }
    }
    for value in &mut variance64 {
        *value /= count;
    }
    let scalar64 = variance64.iter().sum::<f64>();
    let mut maximum_radius64 = 0.0_f64;
    for (_, row) in &population.rows {
        let mut squared = 0.0_f64;
        for dimension in 0..DIMENSIONS {
            let delta = f64::from(row[dimension]) - mean64[dimension];
            squared += delta * delta;
        }
        maximum_radius64 = maximum_radius64.max(squared.sqrt());
    }
    if mean64
        .iter()
        .chain(variance64.iter())
        .chain(std::iter::once(&scalar64))
        .chain(std::iter::once(&maximum_radius64))
        .any(|value| !value.is_finite())
    {
        return Err(invalid("V33 leaf moment is nonfinite"));
    }
    let split_dimension = variance64
        .iter()
        .enumerate()
        .max_by(|left, right| left.1.total_cmp(right.1).then_with(|| right.0.cmp(&left.0)))
        .unwrap()
        .0;
    let mut ordered = population.rows.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        left.1[split_dimension]
            .total_cmp(&right.1[split_dimension])
            .then_with(|| left.0.cmp(&right.0))
    });
    let mean = mean64.map(|value| value as f32);
    let diagonal_variance = variance64.map(|value| value as f32);
    let mut split_centers = [mean; 2];
    if ordered.len() > 1 {
        let cut = ordered.len() / 2;
        for (slot, rows) in [&ordered[..cut], &ordered[cut..]].into_iter().enumerate() {
            let mut center = [0.0_f64; DIMENSIONS];
            for (_, row) in rows {
                for dimension in 0..DIMENSIONS {
                    center[dimension] += f64::from(row[dimension]);
                }
            }
            for dimension in 0..DIMENSIONS {
                split_centers[slot][dimension] = (center[dimension] / rows.len() as f64) as f32;
            }
        }
    }
    Ok(V33LeafShape {
        routing_leaf_ordinal: population.routing_leaf_ordinal,
        group_ordinal: population.group_ordinal,
        logical_start: population
            .rows
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .min()
            .unwrap(),
        population: population.rows.len() as u64,
        mean,
        diagonal_variance,
        scalar_moment: scalar64 as f32,
        maximum_radius: maximum_radius64 as f32,
        split_dimension,
        split_centers,
    })
}

fn v33_vector_array<'a>(vectors: impl Iterator<Item = &'a [f32; DIMENSIONS]>) -> Result<ArrayRef> {
    let values = Arc::new(Float32Array::from_iter_values(
        vectors.flat_map(|vector| vector.iter().copied()),
    ));
    Ok(Arc::new(FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        DIMENSIONS as i32,
        values,
        None,
    )?))
}

fn encode_v33_leaf_shape_artifact(
    leaves: &[V33LeafShape],
    scalar_split_leaves: &[u32],
) -> Result<V33LeafShapeArtifact> {
    if leaves.is_empty()
        || leaves
            .iter()
            .enumerate()
            .any(|(ordinal, leaf)| leaf.routing_leaf_ordinal != ordinal as u32)
        || scalar_split_leaves.is_empty()
        || scalar_split_leaves
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        || scalar_split_leaves
            .iter()
            .any(|ordinal| usize::try_from(*ordinal).map_or(true, |index| index >= leaves.len()))
    {
        return Err(invalid("V33 leaf shape artifact authority differs"));
    }
    let selected = scalar_split_leaves.iter().copied().collect::<BTreeSet<_>>();
    let vector_field = || {
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float32, false)),
            DIMENSIONS as i32,
        )
    };
    let schema = Schema::new(vec![
        Field::new("routing_leaf_ordinal", DataType::UInt32, false),
        Field::new("group_ordinal", DataType::UInt32, false),
        Field::new("logical_start", DataType::UInt64, false),
        Field::new("population", DataType::UInt64, false),
        Field::new("mean", vector_field(), false),
        Field::new("diagonal_variance", vector_field(), false),
        Field::new("scalar_moment", DataType::Float32, false),
        Field::new("maximum_radius", DataType::Float32, false),
        Field::new("split_dimension", DataType::UInt8, false),
        Field::new("split_center_left", vector_field(), false),
        Field::new("split_center_right", vector_field(), false),
        Field::new("scalar_split_selected", DataType::Boolean, false),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(UInt32Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.routing_leaf_ordinal),
            )),
            Arc::new(UInt32Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.group_ordinal),
            )),
            Arc::new(UInt64Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.logical_start),
            )),
            Arc::new(UInt64Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.population),
            )),
            v33_vector_array(leaves.iter().map(|leaf| &leaf.mean))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.diagonal_variance))?,
            Arc::new(Float32Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.scalar_moment),
            )),
            Arc::new(Float32Array::from_iter_values(
                leaves.iter().map(|leaf| leaf.maximum_radius),
            )),
            Arc::new(UInt8Array::from_iter_values(leaves.iter().map(|leaf| {
                u8::try_from(leaf.split_dimension).expect("V33 split dimension fits u8")
            }))),
            v33_vector_array(leaves.iter().map(|leaf| &leaf.split_centers[0]))?,
            v33_vector_array(leaves.iter().map(|leaf| &leaf.split_centers[1]))?,
            Arc::new(BooleanArray::from(
                leaves
                    .iter()
                    .map(|leaf| selected.contains(&leaf.routing_leaf_ordinal))
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    let mut arrow = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut writer = FileWriter::try_new_with_options(&mut arrow, &schema, options)?;
    writer.write(&batch)?;
    writer.finish()?;
    drop(writer);
    Ok(V33LeafShapeArtifact {
        role: "v33-leaf-shapes-arrow",
        sha256: format!("{:x}", Sha256::digest(&arrow)),
        encoded_bytes: arrow.len() as u64,
        row_count: leaves.len() as u64,
        arrow,
    })
}

fn select_v33_scalar_split_leaves(
    populations: &[V33LeafPopulation],
    additional_centers: usize,
) -> Result<Vec<u32>> {
    if additional_centers == 0
        || populations.is_empty()
        || populations
            .iter()
            .map(|leaf| leaf.routing_leaf_ordinal)
            .collect::<BTreeSet<_>>()
            .len()
            != populations.len()
    {
        return Err(invalid("V33 scalar split authority differs"));
    }
    let mut splittable = populations
        .iter()
        .filter(|leaf| leaf.rows.len() > 1)
        .map(|leaf| (leaf.rows.len(), leaf.routing_leaf_ordinal))
        .collect::<Vec<_>>();
    if splittable.len() < additional_centers {
        return Err(invalid("V33 scalar split population differs"));
    }
    splittable.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
    Ok(splittable
        .into_iter()
        .take(additional_centers)
        .map(|(_, ordinal)| ordinal)
        .collect())
}

fn squared_distance(left: &[f32; DIMENSIONS], right: &[f32; DIMENSIONS]) -> Result<f64> {
    let mut distance = 0.0_f64;
    for dimension in 0..DIMENSIONS {
        let delta = f64::from(left[dimension]) - f64::from(right[dimension]);
        distance += delta * delta;
    }
    if !distance.is_finite() {
        return Err(invalid("V33 shape distance is nonfinite"));
    }
    Ok(if distance == 0.0 { 0.0 } else { distance })
}

fn score_v33_leaf(leaf: &V33LeafShape, query: &[f32; DIMENSIONS], arm: V33ShapeArm) -> Result<f64> {
    if leaf.population == 0
        || query.iter().any(|value| !value.is_finite())
        || leaf.mean.iter().any(|value| !value.is_finite())
        || leaf
            .diagonal_variance
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        || !leaf.scalar_moment.is_finite()
        || leaf.scalar_moment < 0.0
    {
        return Err(invalid("V33 shape score authority differs"));
    }
    let distance = squared_distance(&leaf.mean, query)?;
    let score = match arm {
        V33ShapeArm::Centroid => distance,
        V33ShapeArm::SplitCentroid => squared_distance(&leaf.split_centers[0], query)?
            .min(squared_distance(&leaf.split_centers[1], query)?),
        V33ShapeArm::ScalarMoment => {
            let moment = f64::from(leaf.scalar_moment);
            let variance = 2.0 * moment * moment / DIMENSIONS as f64
                + 4.0 * moment * distance / DIMENSIONS as f64;
            distance + moment - extreme_factor(leaf.population) * variance.sqrt()
        }
        V33ShapeArm::DiagonalMoment => {
            let mut moment = 0.0_f64;
            let mut variance_square = 0.0_f64;
            let mut directional = 0.0_f64;
            for ((query_value, mean), variance) in
                query.iter().zip(&leaf.mean).zip(&leaf.diagonal_variance)
            {
                let variance = f64::from(*variance);
                let delta = f64::from(*query_value) - f64::from(*mean);
                moment += variance;
                variance_square += variance * variance;
                directional += delta * delta * variance;
            }
            distance + moment
                - extreme_factor(leaf.population)
                    * (2.0 * variance_square + 4.0 * directional).sqrt()
        }
    };
    if !score.is_finite() {
        return Err(invalid("V33 shape score is nonfinite"));
    }
    Ok(if score == 0.0 { 0.0 } else { score })
}

fn extreme_factor(population: u64) -> f64 {
    if population <= 1 {
        0.0
    } else {
        (2.0 * (population as f64).ln()).sqrt()
    }
}

fn rank_v33_groups(
    leaves: &[V33LeafShape],
    query: &[f32; DIMENSIONS],
    arm: V33ShapeArm,
) -> Result<Vec<u32>> {
    if leaves.is_empty() {
        return Err(invalid("V33 shape leaf summaries differ"));
    }
    let mut scores = BTreeMap::<u32, f64>::new();
    for leaf in leaves {
        let score = score_v33_leaf(leaf, query, arm)?;
        scores
            .entry(leaf.group_ordinal)
            .and_modify(|current| *current = current.min(score))
            .or_insert(score);
    }
    let mut ranked = scores.into_iter().collect::<Vec<_>>();
    ranked.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok(ranked.into_iter().map(|(ordinal, _)| ordinal).collect())
}

fn select_v33_group_prefix(
    groups: &[V33GroupPopulation],
    ranked: &[u32],
    row_limit: u64,
    group_limit: usize,
) -> Result<Vec<u32>> {
    if groups.is_empty() || row_limit == 0 || group_limit == 0 {
        return Err(invalid("V33 group prefix bounds differ"));
    }
    let by_ordinal = groups
        .iter()
        .map(|group| (group.ordinal, group.rows))
        .collect::<BTreeMap<_, _>>();
    if by_ordinal.len() != groups.len() || groups.iter().any(|group| group.rows == 0) {
        return Err(invalid("V33 group population authority differs"));
    }
    let mut seen = BTreeSet::new();
    let mut selected = Vec::new();
    let mut rows = 0_u64;
    for ordinal in ranked.iter().copied() {
        if !seen.insert(ordinal) {
            return Err(invalid("V33 ranked group authority differs"));
        }
        let population = *by_ordinal
            .get(&ordinal)
            .ok_or_else(|| invalid("V33 ranked group authority differs"))?;
        let next = rows
            .checked_add(population)
            .ok_or_else(|| invalid("V33 selected rows overflow"))?;
        if selected.len() == group_limit || next > row_limit {
            break;
        }
        selected.push(ordinal);
        rows = next;
    }
    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::{
        V33GroupPopulation, V33LeafPopulation, V33RoutingRange, V33ShapeArm,
        encode_v33_leaf_shape_artifact, rank_v33_groups, reconstruct_v33_leaf_populations,
        score_v33_leaf, select_v33_group_prefix, select_v33_scalar_split_leaves,
        summarize_v33_leaf, v33_shape_control_bytes,
    };
    use crate::v30_s3_pq::{V30CodePlanes, V30PqCodebook, V30PqWidth};
    use arrow_array::{Array, FixedSizeListArray, Float32Array, UInt32Array};
    use arrow_ipc::reader::FileReader;
    use arrow_schema::{DataType, Field};
    use sha2::{Digest, Sha256};
    use std::io::Cursor;

    fn row(logical_ordinal: u64, first: f32, second: f32) -> (u64, [f32; 96]) {
        let mut values = [0.0; 96];
        values[0] = first;
        values[1] = second;
        (logical_ordinal, values)
    }

    fn codebook(width: V30PqWidth, label: u8, value: f32) -> V30PqCodebook {
        let dimensions = width.dimensions();
        let mut values = vec![0.0; width.subquantizers() * width.centroids() * dimensions];
        for subquantizer in 0..width.subquantizers() {
            let start = (subquantizer * width.centroids() + usize::from(label)) * dimensions;
            values[start..start + dimensions].fill(value);
        }
        V30PqCodebook::new(width, values).unwrap()
    }

    #[test]
    fn v33_group_shape_reconstruction_uses_fidelity_width_and_code_parent() {
        let base = codebook(V30PqWidth::Base24, 1, 0.5);
        let high = codebook(V30PqWidth::High48, 2, 1.5);
        let codes =
            V30CodePlanes::from_packed(2, vec![0b10, 0, 0, 0], vec![1; 24], vec![2; 48]).unwrap();
        let mut parent_centers = vec![[0.0; 96]; 2];
        parent_centers[0].fill(2.0);
        parent_centers[1].fill(4.0);
        let ranges = [
            V33RoutingRange {
                routing_leaf_ordinal: 0,
                code_parent_leaf_ordinal: 0,
                logical_start: 0,
                row_count: 1,
            },
            V33RoutingRange {
                routing_leaf_ordinal: 1,
                code_parent_leaf_ordinal: 1,
                logical_start: 1,
                row_count: 1,
            },
        ];
        let reconstructed = reconstruct_v33_leaf_populations(
            &base,
            &high,
            &codes,
            &parent_centers,
            &ranges,
            &[7, 9],
        )
        .unwrap();
        assert_eq!(reconstructed.len(), 2);
        assert_eq!(reconstructed[0].group_ordinal, 7);
        assert_eq!(reconstructed[0].rows[0].1, [2.5; 96]);
        assert_eq!(reconstructed[1].group_ordinal, 9);
        assert_eq!(reconstructed[1].rows[0].1, [5.5; 96]);
        assert!(reconstructed[1].rows[0].1.iter().sum::<f32>() > 1.0);

        let mut overlapping = ranges;
        overlapping[1].logical_start = 0;
        assert!(
            reconstruct_v33_leaf_populations(
                &base,
                &high,
                &codes,
                &parent_centers,
                &overlapping,
                &[7, 9],
            )
            .is_err()
        );
    }

    #[test]
    fn v33_group_shape_moments_use_complete_gaussian_variance_without_clamp() {
        let leaf = V33LeafPopulation {
            routing_leaf_ordinal: 7,
            group_ordinal: 3,
            rows: vec![row(10, 1.0, 0.0), row(11, 3.0, 0.0)],
        };
        let summary = summarize_v33_leaf(&leaf).unwrap();
        assert_eq!(summary.population, 2);
        assert_eq!(summary.mean[0], 2.0);
        assert_eq!(summary.diagonal_variance[0], 1.0);
        assert_eq!(summary.scalar_moment, 1.0);
        assert_eq!(summary.split_centers[0][0], 1.0);
        assert_eq!(summary.split_centers[1][0], 3.0);

        let query = row(0, 4.0, 0.0).1;
        let a = (2.0_f64 * 2.0_f64.ln()).sqrt();
        let scalar_expected = 5.0 - a * (18.0_f64 / 96.0).sqrt();
        let diagonal_expected = 5.0 - a * 18.0_f64.sqrt();
        assert_eq!(
            score_v33_leaf(&summary, &query, V33ShapeArm::ScalarMoment).unwrap(),
            scalar_expected
        );
        assert_eq!(
            score_v33_leaf(&summary, &query, V33ShapeArm::DiagonalMoment).unwrap(),
            diagonal_expected
        );
        assert_eq!(
            score_v33_leaf(&summary, &query, V33ShapeArm::SplitCentroid).unwrap(),
            1.0
        );

        let far_spread = V33LeafPopulation {
            routing_leaf_ordinal: 8,
            group_ordinal: 4,
            rows: vec![row(12, -100.0, 0.0), row(13, 100.0, 0.0)],
        };
        let signed = score_v33_leaf(
            &summarize_v33_leaf(&far_spread).unwrap(),
            &[0.0; 96],
            V33ShapeArm::DiagonalMoment,
        )
        .unwrap();
        assert!(
            signed < 0.0,
            "negative ranking evidence must not be clamped"
        );
    }

    #[test]
    fn v33_group_shape_equal_byte_controls_are_exact_and_deterministic() {
        let bytes = v33_shape_control_bytes(4_141).unwrap();
        assert_eq!(bytes.scalar_summary_bytes, 4_141 * 388);
        assert_eq!(bytes.scalar_extra_centers, 43);
        assert_eq!(bytes.scalar_padding_bytes, 52);
        assert_eq!(bytes.diagonal_summary_bytes, 4_141 * 768);
        assert_eq!(bytes.diagonal_control_bytes, bytes.diagonal_summary_bytes);

        let leaf = V33LeafPopulation {
            routing_leaf_ordinal: 2,
            group_ordinal: 1,
            rows: vec![
                row(9, 2.0, 0.0),
                row(4, -2.0, 0.0),
                row(7, 1.0, 0.0),
                row(5, -1.0, 0.0),
            ],
        };
        let summary = summarize_v33_leaf(&leaf).unwrap();
        assert_eq!(summary.split_dimension, 0);
        assert_eq!(summary.split_centers[0][0], -1.5);
        assert_eq!(summary.split_centers[1][0], 1.5);
        assert_eq!(summary.maximum_radius, 2.0);

        let populations = (0..50)
            .map(|ordinal| V33LeafPopulation {
                routing_leaf_ordinal: ordinal,
                group_ordinal: 0,
                rows: (0..(ordinal + 2))
                    .map(|row_ordinal| row(u64::from(row_ordinal), row_ordinal as f32, 0.0))
                    .collect(),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            select_v33_scalar_split_leaves(&populations, 43).unwrap(),
            (7_u32..50).rev().collect::<Vec<_>>()
        );
    }

    #[test]
    fn v33_group_shape_group_min_ties_overflow_and_duplicate_truth_are_preserved() {
        let leaves = vec![
            summarize_v33_leaf(&V33LeafPopulation {
                routing_leaf_ordinal: 2,
                group_ordinal: 1,
                rows: vec![row(0, 1.0, 0.0)],
            })
            .unwrap(),
            summarize_v33_leaf(&V33LeafPopulation {
                routing_leaf_ordinal: 0,
                group_ordinal: 0,
                rows: vec![row(1, 1.0, 0.0)],
            })
            .unwrap(),
            summarize_v33_leaf(&V33LeafPopulation {
                routing_leaf_ordinal: 1,
                group_ordinal: 0,
                rows: vec![row(2, 4.0, 0.0)],
            })
            .unwrap(),
        ];
        let ranked = rank_v33_groups(&leaves, &[0.0; 96], V33ShapeArm::Centroid).unwrap();
        assert_eq!(ranked, vec![0, 1]);

        let groups = vec![
            V33GroupPopulation {
                ordinal: 0,
                rows: 7,
            },
            V33GroupPopulation {
                ordinal: 1,
                rows: 6,
            },
            V33GroupPopulation {
                ordinal: 2,
                rows: 1,
            },
        ];
        assert_eq!(
            select_v33_group_prefix(&groups, &[0, 1, 2], 12, 3).unwrap(),
            vec![0]
        );

        let truth_groups = [0_u32, 0, 1, 0, 1, 1, 0, 1, 0, 1];
        let selected = [0_u32];
        assert_eq!(
            truth_groups
                .iter()
                .filter(|group| selected.contains(group))
                .count(),
            5
        );
    }

    #[test]
    fn v33_group_shape_arrow_artifact_binds_exact_f32_shape_and_split_set() {
        let populations = [
            V33LeafPopulation {
                routing_leaf_ordinal: 0,
                group_ordinal: 4,
                rows: vec![row(0, -1.0, 0.0), row(1, 1.0, 0.0)],
            },
            V33LeafPopulation {
                routing_leaf_ordinal: 1,
                group_ordinal: 5,
                rows: vec![row(2, 2.0, 3.0)],
            },
        ];
        let summaries = populations
            .iter()
            .map(summarize_v33_leaf)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let artifact = encode_v33_leaf_shape_artifact(&summaries, &[0]).unwrap();
        assert_eq!(artifact.role, "v33-leaf-shapes-arrow");
        assert_eq!(artifact.row_count, 2);
        assert_eq!(artifact.encoded_bytes, artifact.arrow.len() as u64);
        assert_eq!(
            artifact.sha256,
            format!("{:x}", Sha256::digest(&artifact.arrow))
        );
        let mut reader = FileReader::try_new(Cursor::new(&artifact.arrow), None).unwrap();
        let schema = reader.schema();
        assert_eq!(
            schema.field(0),
            &Field::new("routing_leaf_ordinal", DataType::UInt32, false)
        );
        assert_eq!(
            schema.field(4),
            &Field::new(
                "mean",
                DataType::FixedSizeList(
                    std::sync::Arc::new(Field::new("element", DataType::Float32, false)),
                    96,
                ),
                false,
            )
        );
        assert_eq!(schema.field(5).name(), "diagonal_variance");
        assert_eq!(schema.field(6).name(), "scalar_moment");
        assert_eq!(schema.field(7).name(), "maximum_radius");
        assert_eq!(schema.field(8).name(), "split_dimension");
        assert_eq!(schema.field(9).name(), "split_center_left");
        assert_eq!(schema.field(10).name(), "split_center_right");
        assert_eq!(schema.field(11).name(), "scalar_split_selected");
        let batch = reader.next().unwrap().unwrap();
        assert!(reader.next().is_none());
        assert!(
            batch
                .columns()
                .iter()
                .all(|column| column.null_count() == 0)
        );
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .values(),
            &[0, 1]
        );
        let means = batch
            .column(4)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(means.len(), 2);
        assert_eq!(
            means
                .values()
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(96),
            2.0
        );
    }
}
