//! V35 arbitrary-dimensional orthonormal projection and query-kernel contracts.

use std::{io::Cursor, sync::Arc};

use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array, RecordBatch};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    V35ArtifactIdentity, V35Dimensions, V35ProjectionArm, V35ProjectionBlockVisitor,
    V35ProjectionLimits, V35ProjectionSampleSource, V35ProjectionTrainingSpec, build_v35_srht,
    decode_v35_projection_arrow, encode_v35_projection_arrow, project_v35_query_scalar,
    project_v35_query_simd, train_v35_pca,
};
use sha2::{Digest, Sha256};

fn gram(projection: &borsuk::V35Projection, left: usize, right: usize) -> f64 {
    let routing = usize::from(projection.dimensions().routing);
    projection
        .basis_source_major()
        .chunks_exact(routing)
        .fold(0.0_f64, |sum, coefficients| {
            f64::from(coefficients[left]).mul_add(f64::from(coefficients[right]), sum)
        })
}

#[test]
fn v35_projection_srht_control_is_orthonormal_after_non_power_of_two_truncation() {
    // Break caught: zero-padded Hadamard rows are merely normalized after
    // truncation, leaving cross terms as large as 1/3 at D=384.
    for dimensions in [
        V35Dimensions {
            source: 384,
            routing: 64,
        },
        V35Dimensions {
            source: 960,
            routing: 128,
        },
        V35Dimensions {
            source: 3_072,
            routing: 64,
        },
        V35Dimensions {
            source: 384,
            routing: 192,
        },
    ] {
        let projection = build_v35_srht(dimensions, 0x5eed_1234_89ab_cdef).unwrap();
        assert_eq!(projection.arm(), V35ProjectionArm::Srht);
        assert_eq!(projection.dimensions(), dimensions);
        assert_eq!(
            projection.basis_source_major().len(),
            usize::try_from(dimensions.source).unwrap() * usize::from(dimensions.routing)
        );
        for left in 0..usize::from(dimensions.routing) {
            for right in 0..usize::from(dimensions.routing) {
                let expected = if left == right { 1.0 } else { 0.0 };
                assert!(
                    (gram(&projection, left, right) - expected).abs() <= 5e-4,
                    "D={} M={} gram[{left},{right}]={}",
                    dimensions.source,
                    dimensions.routing,
                    gram(&projection, left, right)
                );
            }
        }
        assert!(
            projection
                .basis_source_major()
                .iter()
                .all(|value| { value.is_finite() && (*value != 0.0 || !value.is_sign_negative()) })
        );
    }
}

#[test]
fn v35_projection_srht_control_is_seeded_and_byte_deterministic() {
    // Break caught: dependence handling silently reseeds, iteration order leaks
    // into bytes, or the control carries a PCA sample identity.
    let dimensions = V35Dimensions {
        source: 385,
        routing: 64,
    };
    let first = build_v35_srht(dimensions, 7).unwrap();
    let repeated = build_v35_srht(dimensions, 7).unwrap();
    let other = build_v35_srht(dimensions, 8).unwrap();
    assert_eq!(first.basis_source_major(), repeated.basis_source_major());
    assert_eq!(first.checksum(), repeated.checksum());
    assert_ne!(first.checksum(), other.checksum());
    assert!(first.sample_sha256().is_none());
    assert_eq!(first.algorithm(), "srht-prefix-orthonormalized-v1");

    assert!(
        build_v35_srht(
            V35Dimensions {
                source: 63,
                routing: 64
            },
            7
        )
        .is_err()
    );
    assert!(
        build_v35_srht(
            V35Dimensions {
                source: 384,
                routing: 96
            },
            7
        )
        .is_err()
    );
}

#[test]
fn v35_projection_simd_matches_ordered_f64_authority_exactly() {
    // Break caught: SIMD reduces source dimensions in multiple lanes, keeps f32
    // accumulators, changes FMA order, or silently normalizes the input.
    let projection = build_v35_srht(
        V35Dimensions {
            source: 384,
            routing: 64,
        },
        19,
    )
    .unwrap();
    let queries = [
        vec![0.0_f32; 384],
        (0..384)
            .map(|index| if index % 2 == 0 { 0.125 } else { -0.125 })
            .collect(),
        (0..384)
            .map(|index| f32::from_bits(1 + u32::try_from(index % 31).unwrap()))
            .collect(),
        (0..384)
            .map(|index| ((index * 37 % 251) as f32 - 125.0) / 31.0)
            .collect(),
    ];
    for query in &queries {
        let scalar = project_v35_query_scalar(&projection, query).unwrap();
        let simd = project_v35_query_simd(&projection, query).unwrap();
        assert_eq!(scalar.coordinates().len(), 64);
        assert!(
            scalar
                .coordinates()
                .iter()
                .zip(simd.coordinates())
                .all(|(left, right)| left.to_bits() == right.to_bits())
        );
        assert_eq!(
            scalar.complement_energy().to_bits(),
            simd.complement_energy().to_bits()
        );
        assert!(simd.backend().is_fused());
    }

    let mut nonfinite = vec![0.0; 384];
    nonfinite[383] = f32::INFINITY;
    assert!(project_v35_query_scalar(&projection, &nonfinite).is_err());
    assert!(project_v35_query_simd(&projection, &nonfinite).is_err());
    assert!(project_v35_query_scalar(&projection, &nonfinite[..383]).is_err());
    assert!(project_v35_query_simd(&projection, &nonfinite[..383]).is_err());
}

fn reauthenticate(bytes: &[u8], identity: &V35ArtifactIdentity) -> V35ArtifactIdentity {
    let mut changed = identity.clone();
    changed.length = u64::try_from(bytes.len()).unwrap();
    changed.digest = format!("{:x}", Sha256::digest(bytes));
    changed
}

fn write_batches(schema: Arc<Schema>, batches: &[RecordBatch]) -> Vec<u8> {
    let mut bytes = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
    let mut writer =
        FileWriter::try_new_with_options(&mut bytes, schema.as_ref(), options).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    bytes
}

#[test]
fn v35_projection_arrow_round_trip_binds_source_major_f32_authority() {
    // Break caught: projection metadata is inferred from serving context, the
    // basis is transposed, or an encoded f64 copy becomes serving authority.
    let projection = build_v35_srht(
        V35Dimensions {
            source: 385,
            routing: 64,
        },
        29,
    )
    .unwrap();
    let (bytes, identity) = encode_v35_projection_arrow(
        &projection,
        "active-projection-basis",
        "s3://borsuk-v35-test/generations/active/projection.arrow",
    )
    .unwrap();
    assert_eq!(identity.digest_algorithm, "sha256");
    assert_eq!(identity.length, u64::try_from(bytes.len()).unwrap());
    assert_eq!(identity.role, "active-projection-basis");

    let mut reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
    let schema = reader.schema();
    assert_eq!(schema.fields().len(), 1);
    assert_eq!(schema.fields()[0].name(), "coefficients");
    assert!(!schema.fields()[0].is_nullable());
    assert_eq!(
        schema.fields()[0].data_type(),
        &DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, false)), 64,)
    );
    assert_eq!(schema.metadata().len(), 1);
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    assert_eq!(batch.num_rows(), 385);

    let decoded = decode_v35_projection_arrow(
        &bytes,
        &identity,
        V35ProjectionLimits::for_dimensions(projection.dimensions()).unwrap(),
    )
    .unwrap();
    assert_eq!(decoded, projection);
    assert_eq!(decoded.seed(), 29);
}

#[test]
fn v35_projection_arrow_rejects_reauthenticated_physical_and_identity_drift() {
    // Break caught: valid Arrow framing bypasses the exact schema, allocation,
    // role, complete-file digest, or decoded logical-checksum authority.
    let projection = build_v35_srht(
        V35Dimensions {
            source: 384,
            routing: 64,
        },
        31,
    )
    .unwrap();
    let (bytes, identity) = encode_v35_projection_arrow(
        &projection,
        "retiring-projection-basis",
        "s3://borsuk-v35-test/generations/retiring/projection.arrow",
    )
    .unwrap();

    for mutate in [
        |value: &mut V35ArtifactIdentity| value.uri.push_str(".other"),
        |value: &mut V35ArtifactIdentity| value.length += 1,
        |value: &mut V35ArtifactIdentity| {
            let replacement = if value.digest.starts_with('0') {
                "1"
            } else {
                "0"
            };
            value.digest.replace_range(0..1, replacement);
        },
        |value: &mut V35ArtifactIdentity| value.digest_algorithm = "blake3".to_owned(),
        |value: &mut V35ArtifactIdentity| value.role = "active-routing".to_owned(),
    ] {
        let mut changed = identity.clone();
        mutate(&mut changed);
        assert!(
            decode_v35_projection_arrow(
                &bytes,
                &changed,
                V35ProjectionLimits::for_dimensions(projection.dimensions()).unwrap()
            )
            .is_err()
        );
    }

    let mut reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .unwrap()
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap()
        .values()
        .to_vec();
    let changed_values: ArrayRef = Arc::new(Float32Array::new(values.into(), None));
    let changed_list: ArrayRef = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            64,
            changed_values,
            None,
        )
        .unwrap(),
    );
    let changed_schema = Arc::new(Schema::new_with_metadata(
        vec![Field::new(
            "coefficients",
            changed_list.data_type().clone(),
            false,
        )],
        batch.schema().metadata().clone(),
    ));
    let changed_batch = RecordBatch::try_new(changed_schema.clone(), vec![changed_list]).unwrap();
    let changed_bytes = write_batches(changed_schema, &[changed_batch]);
    assert!(
        decode_v35_projection_arrow(
            &changed_bytes,
            &reauthenticate(&changed_bytes, &identity),
            V35ProjectionLimits::for_dimensions(projection.dimensions()).unwrap()
        )
        .is_err()
    );

    let mut bytes_changed = bytes.clone();
    bytes_changed[0] ^= 1;
    assert!(
        decode_v35_projection_arrow(
            &bytes_changed,
            &identity,
            V35ProjectionLimits::for_dimensions(projection.dimensions()).unwrap()
        )
        .is_err()
    );

    let mut original_reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
    let original_batch = original_reader.next().unwrap().unwrap();
    let duplicate = write_batches(
        original_batch.schema(),
        &[original_batch.clone(), original_batch],
    );
    assert!(
        decode_v35_projection_arrow(
            &duplicate,
            &reauthenticate(&duplicate, &identity),
            V35ProjectionLimits::for_dimensions(projection.dimensions()).unwrap()
        )
        .is_err()
    );
}

struct StreamingSample {
    rows: Vec<Vec<f32>>,
    passes: usize,
    block_rows: usize,
    reverse_on_pass: Option<usize>,
}

impl V35ProjectionSampleSource for StreamingSample {
    fn scan(&mut self, visitor: &mut V35ProjectionBlockVisitor<'_>) -> borsuk::Result<()> {
        self.passes += 1;
        let mut order = (0..self.rows.len()).collect::<Vec<_>>();
        if self.reverse_on_pass == Some(self.passes) {
            order.reverse();
        }
        for indexes in order.chunks(self.block_rows) {
            let ordinals = indexes
                .iter()
                .map(|index| u64::try_from(*index).unwrap())
                .collect::<Vec<_>>();
            let values = indexes
                .iter()
                .flat_map(|index| self.rows[*index].iter().copied())
                .collect::<Vec<_>>();
            visitor(&ordinals, &values)?;
        }
        Ok(())
    }
}

fn pca_rows() -> Vec<Vec<f32>> {
    (0..160)
        .map(|row| {
            (0..80)
                .map(|dimension| {
                    let diagonal = if dimension < 64 {
                        (80 - dimension) as f32
                    } else {
                        0.03125
                    };
                    let sign = if (row * 131 + dimension * 17) % 2 == 0 {
                        1.0
                    } else {
                        -1.0
                    };
                    sign * diagonal + ((row * 19 + dimension * 7) % 13) as f32 / 4096.0
                })
                .collect()
        })
        .collect()
}

fn sample_sha256(rows: &[Vec<f32>]) -> String {
    let mut digest = Sha256::new();
    digest.update(b"borsuk-v35-projection-sample-v1\n");
    for (ordinal, row) in rows.iter().enumerate() {
        digest.update(u64::try_from(ordinal).unwrap().to_le_bytes());
        for value in row {
            digest.update(value.to_bits().to_le_bytes());
        }
    }
    format!("{:x}", digest.finalize())
}

fn pca_spec(rows: &[Vec<f32>]) -> V35ProjectionTrainingSpec {
    V35ProjectionTrainingSpec {
        dimensions: V35Dimensions {
            source: 80,
            routing: 64,
        },
        sample_rows: u64::try_from(rows.len()).unwrap(),
        sample_sha256: sample_sha256(rows),
        sample_role: "construction-projection-sample".to_owned(),
        seed: 0x3141_5926_5358_9793,
        selection_policy: "source-ordinal-hash-bottom-k-v1".to_owned(),
        source_archive_sha256: "ab".repeat(32),
        trainer: "uncentered-subspace-iteration-rr-v1".to_owned(),
        maximum_block_rows: 2_048,
    }
}

#[test]
fn v35_projection_pca_uses_exactly_four_bounded_ordered_sample_passes() {
    // Break caught: training retains the whole sample, forms D-by-D covariance,
    // silently reorders ordinals, or performs an undocumented extra pass.
    let rows = pca_rows();
    let mut source = StreamingSample {
        rows: rows.clone(),
        passes: 0,
        block_rows: 23,
        reverse_on_pass: None,
    };
    let projection = train_v35_pca(&pca_spec(&rows), &mut source).unwrap();
    assert_eq!(source.passes, 4);
    assert_eq!(projection.arm(), V35ProjectionArm::Pca);
    assert_eq!(projection.dimensions().source, 80);
    assert_eq!(projection.dimensions().routing, 64);
    assert_eq!(
        projection.sample_sha256(),
        Some(sample_sha256(&rows).as_str())
    );
    assert_eq!(
        projection.algorithm(),
        "uncentered-subspace-iteration-rr-v1"
    );
    assert_eq!(projection.basis_source_major().len(), 80 * 64);
    let descriptor: serde_json::Value =
        serde_json::from_str(projection.training_descriptor()).unwrap();
    assert_eq!(descriptor["sample_passes"], 4);
    assert_eq!(descriptor["subspace_iterations"], 2);
    assert_eq!(descriptor["oversampling"], 16);
    assert_eq!(descriptor["eigen_cluster_relative_tolerance"], "1e-10");
    assert_eq!(descriptor["sample_role"], "construction-projection-sample");
    for left in 0..64 {
        for right in 0..64 {
            let expected = if left == right { 1.0 } else { 0.0 };
            assert!((gram(&projection, left, right) - expected).abs() <= 5e-4);
        }
    }

    let mut repeated = StreamingSample {
        rows: rows.clone(),
        passes: 0,
        block_rows: 31,
        reverse_on_pass: None,
    };
    let again = train_v35_pca(&pca_spec(&rows), &mut repeated).unwrap();
    assert_eq!(projection.basis_source_major(), again.basis_source_major());
    assert_eq!(projection.checksum(), again.checksum());

    let (bytes, identity) = encode_v35_projection_arrow(
        &projection,
        "active-projection-basis",
        "s3://borsuk-v35-test/generations/pca.arrow",
    )
    .unwrap();
    assert_eq!(
        decode_v35_projection_arrow(
            &bytes,
            &identity,
            V35ProjectionLimits::for_dimensions(projection.dimensions()).unwrap(),
        )
        .unwrap(),
        projection
    );

    let mut reordered = StreamingSample {
        rows,
        passes: 0,
        block_rows: 29,
        reverse_on_pass: Some(3),
    };
    let reordered_spec = pca_spec(&reordered.rows);
    assert!(train_v35_pca(&reordered_spec, &mut reordered).is_err());
}

#[test]
fn v35_projection_pca_rejects_oversized_blocks_and_sample_authority_drift() {
    let rows = pca_rows();
    let mut oversized = StreamingSample {
        rows: rows.clone(),
        passes: 0,
        block_rows: rows.len(),
        reverse_on_pass: None,
    };
    let mut spec = pca_spec(&rows);
    spec.maximum_block_rows = 64;
    assert!(train_v35_pca(&spec, &mut oversized).is_err());

    let mut source = StreamingSample {
        rows: rows.clone(),
        passes: 0,
        block_rows: 32,
        reverse_on_pass: None,
    };
    let mut changed = pca_spec(&rows);
    changed.sample_sha256.replace_range(0..1, "0");
    if changed.sample_sha256 == sample_sha256(&rows) {
        changed.sample_sha256.replace_range(0..1, "1");
    }
    assert!(train_v35_pca(&changed, &mut source).is_err());

    let mut wrong_role_source = StreamingSample {
        rows: rows.clone(),
        passes: 0,
        block_rows: 32,
        reverse_on_pass: None,
    };
    let mut wrong_role = pca_spec(&rows);
    wrong_role.sample_role = "query".to_owned();
    assert!(train_v35_pca(&wrong_role, &mut wrong_role_source).is_err());

    let mut nonfinite_rows = rows;
    nonfinite_rows[159][79] = f32::NAN;
    let mut nonfinite = StreamingSample {
        rows: nonfinite_rows.clone(),
        passes: 0,
        block_rows: 41,
        reverse_on_pass: None,
    };
    assert!(train_v35_pca(&pca_spec(&nonfinite_rows), &mut nonfinite).is_err());
}

#[test]
fn v35_projection_pca_preserves_scaled_rank_and_canonicalizes_null_completion() {
    // Break caught: an absolute covariance threshold discards a small but real
    // signal beyond the first ell source axes and completes the wrong subspace.
    let mut rows = vec![vec![0.0_f32; 96]; 2];
    rows[0][95] = 1e-7;
    rows[1][95] = -1e-7;
    let mut source = StreamingSample {
        rows: rows.clone(),
        passes: 0,
        block_rows: 1,
        reverse_on_pass: None,
    };
    let mut spec = pca_spec(&rows);
    spec.dimensions.source = 96;
    let projection = train_v35_pca(&spec, &mut source).unwrap();
    let mut query = vec![0.0_f32; 96];
    query[95] = 1.0;
    let projected = project_v35_query_scalar(&projection, &query).unwrap();
    let captured = projected
        .coordinates()
        .iter()
        .fold(0.0, |sum, value| value.mul_add(*value, sum));
    assert!(
        captured > 0.999,
        "scaled rank was replaced by axis completion"
    );
}

#[test]
fn v35_projection_pca_canonicalizes_repeated_eigenspace_before_truncation() {
    // Break caught: an eigensolver-specific rotation chooses an arbitrary 64D
    // slice from a repeated 65D eigenspace at the persisted-rank boundary.
    let mut rows = Vec::new();
    for dimension in 0..65 {
        let mut positive = vec![0.0_f32; 80];
        positive[dimension] = 1.0;
        let mut negative = vec![0.0_f32; 80];
        negative[dimension] = -1.0;
        rows.push(positive);
        rows.push(negative);
    }
    let mut source = StreamingSample {
        rows: rows.clone(),
        passes: 0,
        block_rows: 17,
        reverse_on_pass: None,
    };
    let projection = train_v35_pca(&pca_spec(&rows), &mut source).unwrap();
    for dimension in 0..65 {
        let mut query = vec![0.0_f32; 80];
        query[dimension] = 1.0;
        let projected = project_v35_query_scalar(&projection, &query).unwrap();
        let captured = projected
            .coordinates()
            .iter()
            .fold(0.0, |sum, value| value.mul_add(*value, sum));
        if dimension < 64 {
            assert!(captured > 0.999, "source axis {dimension} was not retained");
        } else {
            assert!(
                captured < 1e-6,
                "tie did not exclude the highest source axis"
            );
        }
    }
}

#[test]
fn v35_projection_arrow_rejects_before_exceeding_registered_memory_limits() {
    let projection = build_v35_srht(
        V35Dimensions {
            source: 384,
            routing: 64,
        },
        41,
    )
    .unwrap();
    let (bytes, identity) = encode_v35_projection_arrow(
        &projection,
        "active-projection-basis",
        "s3://borsuk-v35-test/generations/limited.arrow",
    )
    .unwrap();
    let admitted = V35ProjectionLimits::for_dimensions(projection.dimensions()).unwrap();
    let high_dimensional = V35ProjectionLimits::for_dimensions(V35Dimensions {
        source: 3_072,
        routing: 192,
    })
    .unwrap();
    assert_eq!(
        high_dimensional.maximum_training_workspace_bytes,
        64 * 1_048_576
    );
    assert!(
        high_dimensional.projected_training_workspace_bytes
            < high_dimensional.maximum_training_workspace_bytes
    );
    let very_high_dimensional = V35ProjectionLimits::for_dimensions(V35Dimensions {
        source: 10_000,
        routing: 64,
    })
    .unwrap();
    assert!(
        very_high_dimensional.projected_training_workspace_bytes
            > very_high_dimensional.maximum_training_workspace_bytes
    );
    assert!(decode_v35_projection_arrow(&bytes, &identity, admitted).is_ok());
    let mut too_small = admitted;
    too_small.maximum_encoded_bytes = u64::try_from(bytes.len() - 1).unwrap();
    assert!(decode_v35_projection_arrow(&bytes, &identity, too_small).is_err());
    let mut too_small = admitted;
    too_small.maximum_decoded_numeric_bytes -= 4;
    assert!(decode_v35_projection_arrow(&bytes, &identity, too_small).is_err());
    let mut too_small = admitted;
    too_small.maximum_peak_codec_bytes = u64::try_from(bytes.len()).unwrap()
        + u64::from(384_u32 * 64_u32 * 4_u32).checked_mul(2).unwrap()
        - 1;
    assert!(decode_v35_projection_arrow(&bytes, &identity, too_small).is_err());
}

#[test]
fn v35_projection_simd_covers_registered_high_dimension_matrix() {
    // Break caught: one dimension/routing-width combination falls back to a
    // different reduction order or overflows indexing in the four-lane kernel.
    for source in [384, 768, 1_536, 3_072] {
        for routing in [64, 128, 192] {
            let dimensions = V35Dimensions { source, routing };
            let projection =
                build_v35_srht(dimensions, u64::from(source) << 16 | u64::from(routing)).unwrap();
            let query = (0..usize::try_from(source).unwrap())
                .map(|index| match index % 4 {
                    0 => f32::MAX,
                    1 => -f32::MAX,
                    2 => f32::from_bits(1),
                    _ => -f32::from_bits(1),
                })
                .collect::<Vec<_>>();
            let scalar = project_v35_query_scalar(&projection, &query).unwrap();
            let simd = project_v35_query_simd(&projection, &query).unwrap();
            assert!(simd.backend().is_fused());
            assert!(
                scalar
                    .coordinates()
                    .iter()
                    .zip(simd.coordinates())
                    .all(|(left, right)| left.to_bits() == right.to_bits())
            );
            assert_eq!(
                scalar.complement_energy().to_bits(),
                simd.complement_energy().to_bits()
            );
        }
    }
}
