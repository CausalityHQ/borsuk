use std::{io::Cursor, sync::Arc};

use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float16Array, RecordBatch, UInt16Array};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use half::f16;
use rayon::{ThreadPoolBuilder, prelude::*};
use sha2::{Digest, Sha256};

use crate::{BorsukError, Result, V27PageRow};

const DIMENSIONS: i32 = 96;
/// Frozen production root count.
pub const V27_ROOT_CENTROIDS: usize = 1_024;
/// Frozen production leaf count.
pub const V27_LEAF_CENTROIDS: usize = 65_536;
/// Frozen production children per root.
pub const V27_LEAVES_PER_ROOT: usize = 64;

/// Deterministic bounded training shape for the resident V27 hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct V27HierarchyConfig {
    /// Number of first-level centroids.
    pub roots: usize,
    /// Total number of second-level centroids.
    pub leaves: usize,
    /// Fixed Lloyd iteration count at each level.
    pub iterations: usize,
    /// Query-independent initialization seed.
    pub seed: u64,
    /// Parallel assignment workers.
    pub worker_count: usize,
    /// Fixed assignment batch size, independent of worker scheduling.
    pub batch_rows: usize,
}

impl V27HierarchyConfig {
    /// Construct the frozen production hierarchy shape.
    pub fn production(worker_count: usize, batch_rows: usize) -> Self {
        Self {
            roots: V27_ROOT_CENTROIDS,
            leaves: V27_LEAF_CENTROIDS,
            iterations: 4,
            seed: 0x6a09_e667_f3bc_c909,
            worker_count,
            batch_rows,
        }
    }
}

/// Resident two-level V27 routing hierarchy.
#[derive(Debug, Clone, PartialEq)]
pub struct V27Hierarchy {
    /// First-level normalized centroids in ordinal order.
    pub roots: Vec<[f16; 96]>,
    /// Second-level normalized centroids in root-major order.
    pub leaves: Vec<[f16; 96]>,
    /// Parent root ordinal for every leaf.
    pub leaf_roots: Vec<u16>,
}

/// Exact identity for one resident Arrow hierarchy object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V27HierarchyArtifactIdentity {
    /// Frozen semantic role.
    pub role: String,
    /// SHA-256 of the complete Arrow IPC bytes.
    pub sha256: String,
    /// Complete Arrow IPC byte length.
    pub encoded_bytes: u64,
}

/// Authenticated Arrow encodings of both hierarchy levels.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V27HierarchyArtifacts {
    /// Root object identity.
    pub roots: V27HierarchyArtifactIdentity,
    /// Leaf object identity.
    pub leaves: V27HierarchyArtifactIdentity,
    /// Root Arrow IPC bytes.
    pub roots_bytes: Vec<u8>,
    /// Leaf Arrow IPC bytes.
    pub leaves_bytes: Vec<u8>,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn validate_vector(vector: &[f32; 96]) -> Result<[f32; 96]> {
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(invalid("V27 hierarchy vector is non-finite"));
    }
    let norm = vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(invalid("V27 hierarchy vector norm differs"));
    }
    Ok(*vector)
}

fn distance(left: &[f32; 96], right: &[f32; 96]) -> f64 {
    left.iter()
        .zip(right)
        .map(|(left, right)| {
            let delta = f64::from(*left) - f64::from(*right);
            delta * delta
        })
        .sum()
}

fn choose_initial(rows: &[(u64, [f32; 96])], count: usize, seed: u64) -> Vec<[f32; 96]> {
    let mut ordered = rows.iter().collect::<Vec<_>>();
    ordered.sort_unstable_by_key(|row| (splitmix64(row.0 ^ seed), row.0));
    ordered[..count].iter().map(|row| row.1).collect()
}

fn train_level(
    pool: &rayon::ThreadPool,
    rows: &[(u64, [f32; 96])],
    count: usize,
    iterations: usize,
    batch_rows: usize,
    seed: u64,
) -> Result<(Vec<[f32; 96]>, Vec<usize>)> {
    if rows.len() < count.saturating_mul(2) || count == 0 {
        return Err(invalid("V27 hierarchy training population differs"));
    }
    let mut centroids = choose_initial(rows, count, seed);
    let mut assignments = vec![0; rows.len()];
    for _ in 0..iterations {
        assignments = pool
            .install(|| {
                rows.par_chunks(batch_rows)
                    .map(|batch| {
                        batch
                            .iter()
                            .map(|row| {
                                centroids
                                    .iter()
                                    .enumerate()
                                    .map(|(ordinal, centroid)| {
                                        (distance(&row.1, centroid), ordinal)
                                    })
                                    .min_by(|left, right| {
                                        left.0.total_cmp(&right.0).then(left.1.cmp(&right.1))
                                    })
                                    .unwrap()
                                    .1
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .flatten()
            .collect();
        let mut counts = vec![0_usize; count];
        for assignment in &assignments {
            counts[*assignment] += 1;
        }
        for empty in 0..count {
            if counts[empty] != 0 {
                continue;
            }
            let donor = rows
                .iter()
                .enumerate()
                .filter(|(index, _)| counts[assignments[*index]] > 1)
                .map(|(index, row)| {
                    (
                        index,
                        distance(&row.1, &centroids[assignments[index]]),
                        row.0,
                    )
                })
                .max_by(|left, right| {
                    left.1
                        .total_cmp(&right.1)
                        .then_with(|| right.2.cmp(&left.2))
                })
                .ok_or_else(|| invalid("V27 hierarchy empty-cluster repair differs"))?
                .0;
            counts[assignments[donor]] -= 1;
            assignments[donor] = empty;
            counts[empty] = 1;
        }
        let mut sums = vec![[0.0_f64; 96]; count];
        for (row, assignment) in rows.iter().zip(&assignments) {
            for (sum, value) in sums[*assignment].iter_mut().zip(row.1) {
                *sum += f64::from(value);
            }
        }
        centroids = sums
            .into_iter()
            .zip(counts)
            .map(|(sum, count)| validate_vector(&sum.map(|value| (value / count as f64) as f32)))
            .collect::<Result<Vec<_>>>()?;
    }
    Ok((centroids, assignments))
}

/// Fit a deterministic two-level hierarchy from a bounded query-independent sample.
pub fn fit_v27_hierarchy(rows: &[V27PageRow], config: &V27HierarchyConfig) -> Result<V27Hierarchy> {
    if config.roots == 0
        || !config.roots.is_power_of_two()
        || config.leaves < config.roots
        || !config.leaves.is_power_of_two()
        || !config.leaves.is_multiple_of(config.roots)
        || config.iterations == 0
        || config.worker_count == 0
        || config.batch_rows == 0
        || rows.len() < config.leaves.saturating_mul(2)
    {
        return Err(invalid("V27 hierarchy training shape differs"));
    }
    let mut previous = None;
    let normalized = rows
        .iter()
        .map(|row| {
            if previous.is_some_and(|ordinal| row.source_ordinal <= ordinal) {
                return Err(invalid("V27 hierarchy source order differs"));
            }
            previous = Some(row.source_ordinal);
            Ok((row.source_ordinal, validate_vector(&row.vector)?))
        })
        .collect::<Result<Vec<_>>>()?;
    let pool = ThreadPoolBuilder::new()
        .num_threads(config.worker_count)
        .build()
        .map_err(|_| invalid("V27 hierarchy worker pool differs"))?;
    let (roots, root_assignments) = train_level(
        &pool,
        &normalized,
        config.roots,
        config.iterations,
        config.batch_rows,
        config.seed,
    )?;
    let leaves_per_root = config.leaves / config.roots;
    let mut leaves = Vec::with_capacity(config.leaves);
    let mut leaf_roots = Vec::with_capacity(config.leaves);
    for root in 0..config.roots {
        let mut members = normalized
            .iter()
            .zip(&root_assignments)
            .filter(|(_, assignment)| **assignment == root)
            .map(|(row, _)| *row)
            .collect::<Vec<_>>();
        let minimum = leaves_per_root.saturating_mul(2);
        if members.len() < minimum {
            let population = members.len();
            if population == 0 {
                return Err(invalid("V27 hierarchy root population differs"));
            }
            let own_members = members.clone();
            members.extend((population..minimum).map(|index| own_members[index % population]));
        }
        let (children, _) = train_level(
            &pool,
            &members,
            leaves_per_root,
            config.iterations,
            config.batch_rows,
            config.seed ^ u64::try_from(root).unwrap(),
        )?;
        leaves.extend(children);
        leaf_roots.extend(std::iter::repeat_n(
            u16::try_from(root).map_err(|_| invalid("V27 root ordinal overflows"))?,
            leaves_per_root,
        ));
    }
    Ok(V27Hierarchy {
        roots: roots
            .into_iter()
            .map(|row| row.map(f16::from_f32))
            .collect(),
        leaves: leaves
            .into_iter()
            .map(|row| row.map(f16::from_f32))
            .collect(),
        leaf_roots,
    })
}

fn centroid_schema() -> Schema {
    Schema::new(vec![Field::new(
        "centroid",
        DataType::FixedSizeList(
            Arc::new(Field::new("element", DataType::Float16, false)),
            DIMENSIONS,
        ),
        false,
    )])
}

fn leaf_schema() -> Schema {
    Schema::new(vec![
        Field::new("root_ordinal", DataType::UInt16, false),
        Field::new(
            "centroid",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float16, false)),
                DIMENSIONS,
            ),
            false,
        ),
    ])
}

fn centroid_array(rows: &[[f16; 96]]) -> Result<FixedSizeListArray> {
    Ok(FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float16, false)),
        DIMENSIONS,
        Arc::new(Float16Array::from_iter_values(
            rows.iter().flatten().copied(),
        )),
        None,
    )?)
}

fn encode_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)?;
    let mut bytes = Vec::new();
    {
        let mut writer =
            FileWriter::try_new_with_options(&mut bytes, batch.schema().as_ref(), options)?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(bytes)
}

fn identity(role: &str, bytes: &[u8]) -> Result<V27HierarchyArtifactIdentity> {
    Ok(V27HierarchyArtifactIdentity {
        role: role.to_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        encoded_bytes: u64::try_from(bytes.len())
            .map_err(|_| invalid("V27 hierarchy artifact length overflows"))?,
    })
}

fn valid_hierarchy(hierarchy: &V27Hierarchy) -> bool {
    !hierarchy.roots.is_empty()
        && hierarchy.roots.len().is_power_of_two()
        && hierarchy.leaves.len() >= hierarchy.roots.len()
        && hierarchy.leaves.len().is_power_of_two()
        && hierarchy.leaves.len().is_multiple_of(hierarchy.roots.len())
        && hierarchy.leaf_roots.len() == hierarchy.leaves.len()
        && hierarchy.leaf_roots.iter().enumerate().all(|(leaf, root)| {
            usize::from(*root) == leaf / (hierarchy.leaves.len() / hierarchy.roots.len())
        })
        && hierarchy.roots.iter().chain(&hierarchy.leaves).all(|row| {
            row.iter().all(|value| value.is_finite())
                && row
                    .iter()
                    .map(|value| f32::from(*value).powi(2))
                    .sum::<f32>()
                    > 0.0
        })
}

/// Encode the resident hierarchy as two strict Arrow IPC artifacts.
pub fn encode_v27_hierarchy(hierarchy: &V27Hierarchy) -> Result<V27HierarchyArtifacts> {
    if !valid_hierarchy(hierarchy) {
        return Err(invalid("V27 hierarchy authority differs"));
    }
    let roots_bytes = encode_batch(&RecordBatch::try_new(
        Arc::new(centroid_schema()),
        vec![Arc::new(centroid_array(&hierarchy.roots)?) as ArrayRef],
    )?)?;
    let leaves_bytes = encode_batch(&RecordBatch::try_new(
        Arc::new(leaf_schema()),
        vec![
            Arc::new(UInt16Array::from(hierarchy.leaf_roots.clone())),
            Arc::new(centroid_array(&hierarchy.leaves)?),
        ],
    )?)?;
    Ok(V27HierarchyArtifacts {
        roots: identity("v27-roots-arrow", &roots_bytes)?,
        leaves: identity("v27-leaves-arrow", &leaves_bytes)?,
        roots_bytes,
        leaves_bytes,
    })
}

fn authenticate(identity: &V27HierarchyArtifactIdentity, bytes: &[u8], role: &str) -> Result<()> {
    if identity.role != role
        || identity.encoded_bytes != bytes.len() as u64
        || identity.sha256.len() != 64
        || identity.sha256 != format!("{:x}", Sha256::digest(bytes))
    {
        return Err(invalid("V27 hierarchy byte authority differs"));
    }
    Ok(())
}

fn read_batch(bytes: &[u8], expected: &Schema) -> Result<RecordBatch> {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None)?;
    if reader.schema().as_ref() != expected {
        return Err(invalid("V27 hierarchy Arrow schema differs"));
    }
    let batch = reader
        .next()
        .ok_or_else(|| invalid("V27 hierarchy Arrow batch is missing"))??;
    if reader.next().is_some()
        || batch.num_columns() != expected.fields().len()
        || batch
            .columns()
            .iter()
            .any(|column| column.null_count() != 0)
    {
        return Err(invalid("V27 hierarchy Arrow batch differs"));
    }
    Ok(batch)
}

fn read_centroids(array: &ArrayRef) -> Result<Vec<[f16; 96]>> {
    let values = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| invalid("V27 hierarchy centroid column differs"))?
        .values();
    let values = values
        .as_any()
        .downcast_ref::<Float16Array>()
        .ok_or_else(|| invalid("V27 hierarchy centroid values differ"))?;
    let (rows, remainder) = values.values().as_chunks::<96>();
    if !remainder.is_empty() {
        return Err(invalid("V27 hierarchy centroid cardinality differs"));
    }
    Ok(rows.to_vec())
}

/// Authenticate and strictly decode both resident hierarchy artifacts.
pub fn decode_v27_hierarchy(
    roots_identity: &V27HierarchyArtifactIdentity,
    roots_bytes: &[u8],
    leaves_identity: &V27HierarchyArtifactIdentity,
    leaves_bytes: &[u8],
) -> Result<V27Hierarchy> {
    authenticate(roots_identity, roots_bytes, "v27-roots-arrow")?;
    authenticate(leaves_identity, leaves_bytes, "v27-leaves-arrow")?;
    let roots_batch = read_batch(roots_bytes, &centroid_schema())?;
    let leaves_batch = read_batch(leaves_bytes, &leaf_schema())?;
    let roots = read_centroids(roots_batch.column(0))?;
    let leaf_roots = leaves_batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .ok_or_else(|| invalid("V27 hierarchy leaf root column differs"))?
        .values()
        .to_vec();
    let leaves = read_centroids(leaves_batch.column(1))?;
    let hierarchy = V27Hierarchy {
        roots,
        leaves,
        leaf_roots,
    };
    if !valid_hierarchy(&hierarchy) {
        return Err(invalid("V27 hierarchy authority differs"));
    }
    Ok(hierarchy)
}

#[cfg(test)]
mod tests {
    use super::{choose_initial, splitmix64};
    use crate::{
        V27_LEAF_CENTROIDS, V27_LEAVES_PER_ROOT, V27_ROOT_CENTROIDS, V27HierarchyConfig,
        V27PageRow, decode_v27_hierarchy, encode_v27_hierarchy, fit_v27_hierarchy,
    };

    fn rows() -> Vec<V27PageRow> {
        (0..32_u64)
            .map(|source_ordinal| {
                let group = usize::try_from(source_ordinal / 8).unwrap();
                let mut vector = [0.0_f32; 96];
                vector[group] = 1.0;
                vector[4 + usize::try_from(source_ordinal % 8).unwrap()] = 0.01;
                V27PageRow {
                    source_ordinal,
                    vector,
                }
            })
            .collect()
    }

    fn config(worker_count: usize) -> V27HierarchyConfig {
        V27HierarchyConfig {
            roots: 2,
            leaves: 4,
            iterations: 3,
            seed: 0x6a09_e667_f3bc_c909,
            worker_count,
            batch_rows: 8,
        }
    }

    #[test]
    fn v27_s3_router_initializes_centroids_from_the_bounded_hash_order() {
        // Break caught: farthest-first initialization repeatedly scans the growing selected set
        // and becomes effectively quadratic before the 32,768-leaf untouched qualification.
        let rows = rows()
            .into_iter()
            .map(|row| (row.source_ordinal, row.vector))
            .collect::<Vec<_>>();
        let seed = 0x6a09_e667_f3bc_c909;
        let selected = choose_initial(&rows, 8, seed);
        let mut expected = rows.iter().collect::<Vec<_>>();
        expected.sort_unstable_by_key(|row| (splitmix64(row.0 ^ seed), row.0));
        assert_eq!(
            selected,
            expected[..8].iter().map(|row| row.1).collect::<Vec<_>>()
        );
    }

    #[test]
    fn v27_s3_router_training_and_arrow_authority_are_worker_invariant() {
        // Break caught: hierarchy training depends on thread scheduling, leaf ordinals lose their
        // root-major authority, or the resident Arrow artifacts are accepted after byte drift.
        let single = fit_v27_hierarchy(&rows(), &config(1)).unwrap();
        let parallel = fit_v27_hierarchy(&rows(), &config(4)).unwrap();
        assert_eq!(single, parallel);
        assert_eq!(single.roots.len(), 2);
        assert_eq!(single.leaves.len(), 4);
        assert_eq!(single.leaf_roots, vec![0, 0, 1, 1]);

        let artifacts = encode_v27_hierarchy(&single).unwrap();
        assert_eq!(artifacts.roots.role, "v27-roots-arrow");
        assert_eq!(artifacts.leaves.role, "v27-leaves-arrow");
        assert_eq!(
            decode_v27_hierarchy(
                &artifacts.roots,
                &artifacts.roots_bytes,
                &artifacts.leaves,
                &artifacts.leaves_bytes,
            )
            .unwrap(),
            single
        );

        let mut drift = artifacts.leaves.clone();
        drift.sha256 = "0".repeat(64);
        assert!(
            decode_v27_hierarchy(
                &artifacts.roots,
                &artifacts.roots_bytes,
                &drift,
                &artifacts.leaves_bytes,
            )
            .is_err()
        );
    }

    #[test]
    fn v27_s3_router_rejects_ambiguous_training_shapes_and_rows() {
        // Break caught: a malformed configuration or non-authoritative source stream creates a
        // hierarchy whose page routing cannot be reproduced across builders.
        for invalid in [
            V27HierarchyConfig {
                roots: 0,
                ..config(1)
            },
            V27HierarchyConfig {
                leaves: 3,
                ..config(1)
            },
            V27HierarchyConfig {
                iterations: 0,
                ..config(1)
            },
            V27HierarchyConfig {
                worker_count: 0,
                ..config(1)
            },
            V27HierarchyConfig {
                batch_rows: 0,
                ..config(1)
            },
        ] {
            assert!(fit_v27_hierarchy(&rows(), &invalid).is_err());
        }

        let mut duplicate = rows();
        duplicate[1].source_ordinal = duplicate[0].source_ordinal;
        assert!(fit_v27_hierarchy(&duplicate, &config(1)).is_err());

        let mut nonfinite = rows();
        nonfinite[0].vector[0] = f32::NAN;
        assert!(fit_v27_hierarchy(&nonfinite, &config(1)).is_err());
    }

    #[test]
    fn v27_s3_router_repairs_empty_clusters_and_freezes_production_fanout() {
        // Break caught: degenerate samples make construction schedule-dependent or production
        // silently changes away from exactly 1,024 roots and 64 leaves per root.
        let production = V27HierarchyConfig::production(8, 4_096);
        assert_eq!(production.roots, V27_ROOT_CENTROIDS);
        assert_eq!(production.leaves, V27_LEAF_CENTROIDS);
        assert_eq!(production.leaves / production.roots, V27_LEAVES_PER_ROOT);

        let identical = (0..16)
            .map(|source_ordinal| V27PageRow {
                source_ordinal,
                vector: [0.25; 96],
            })
            .collect::<Vec<_>>();
        let degenerate = V27HierarchyConfig {
            roots: 1,
            leaves: 4,
            iterations: 3,
            seed: production.seed,
            worker_count: 4,
            batch_rows: 3,
        };
        let hierarchy = fit_v27_hierarchy(&identical, &degenerate).unwrap();
        assert_eq!(hierarchy.roots.len(), 1);
        assert_eq!(hierarchy.leaves.len(), 4);
        assert_eq!(hierarchy.leaf_roots, vec![0; 4]);
        assert_eq!(f32::from(hierarchy.roots[0][0]), 0.25);
    }

    #[test]
    fn v27_s3_router_expands_underfull_roots_from_only_their_own_members() {
        // Break caught: every underfull root cloned and sorted the complete
        // training sample, then silently trained its leaves on other roots.
        let identical = (0..32)
            .map(|source_ordinal| V27PageRow {
                source_ordinal,
                vector: [0.25; 96],
            })
            .collect::<Vec<_>>();
        let underfull = V27HierarchyConfig {
            roots: 8,
            leaves: 16,
            iterations: 1,
            seed: 7,
            worker_count: 2,
            batch_rows: 8,
        };
        let hierarchy = fit_v27_hierarchy(&identical, &underfull).unwrap();
        assert_eq!(hierarchy.roots.len(), 8);
        assert_eq!(hierarchy.leaves.len(), 16);
        assert!(
            hierarchy
                .leaves
                .iter()
                .all(|leaf| leaf[0] == half::f16::from_f32(0.25))
        );
    }
}
