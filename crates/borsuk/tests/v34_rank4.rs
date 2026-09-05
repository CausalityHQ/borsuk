//! Focused authority, algebra, and projection tests for V34 rank-four routing.

use std::{collections::HashMap, io::Cursor, sync::Arc};

use arrow_array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, RecordBatch};
use arrow_ipc::{
    CompressionType, MetadataVersion,
    reader::FileReader,
    writer::{FileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    V34Rank4ArtifactIdentity, V34Rank4LeafInput, build_v34_rank4_generation,
    decode_v34_rank4_arrow, encode_v34_rank4_arrow, project_v34_serving_memory,
    score_v34_rank4_leaf,
};
use sha2::{Digest, Sha256};

const DIMENSIONS: usize = 96;

fn coherent_leaf() -> V34Rank4LeafInput {
    let mut residual_diagonal = [0.0_f32; DIMENSIONS];
    residual_diagonal[0] = 1.0;
    residual_diagonal[1] = 3.0;
    let mut directions = [[0.0_f32; DIMENSIONS]; 4];
    directions[0][0] = 1.0;
    directions[1][0] = 0.5;
    directions[1][1] = 0.5;
    V34Rank4LeafInput {
        leaf_ordinal: 0,
        group_ordinal: 0,
        logical_start: 0,
        population: 2,
        mean: [0.0; DIMENSIONS],
        residual_diagonal,
        eigenvalues: [2.0, 1.0, 0.0, 0.0],
        directions,
    }
}

#[test]
fn v34_rank4_score_matches_hand_reduced_nonorthogonal_covariance() {
    // Break caught: the rank-four score treats persisted directions as
    // orthogonal, omits the residual diagonal, or changes reduction order.
    let generation = build_v34_rank4_generation(vec![coherent_leaf()]).unwrap();
    let leaf = &generation.leaves()[0];
    let mut query = [0.0_f32; DIMENSIONS];
    query[0] = 2.0;
    query[1] = -1.0;
    let expected = 11.5 - (2.0_f64 * 2.0_f64.ln()).sqrt() * 103.5_f64.sqrt();
    assert_eq!(score_v34_rank4_leaf(leaf, &query).unwrap(), expected);

    query[0] = f32::NAN;
    assert!(score_v34_rank4_leaf(leaf, &query).is_err());
}

#[test]
fn v34_rank4_generation_recomputes_authority_and_logical_coverage() {
    // Break caught: persisted cached moments, component signs, or logical
    // intervals are trusted instead of independently authenticated.
    let first = coherent_leaf();
    let mut second = coherent_leaf();
    second.leaf_ordinal = 1;
    second.group_ordinal = 1;
    second.logical_start = 2;
    let generation = build_v34_rank4_generation(vec![first.clone(), second]).unwrap();
    assert_eq!(generation.leaves().len(), 2);
    assert_eq!(generation.logical_rows(), 4);
    assert_eq!(generation.group_count(), 2);

    assert_eq!(generation.leaves()[0].trace(), 6.5);
    assert_eq!(generation.leaves()[0].trace_square(), 21.25);
    assert!(generation.leaves()[0].spectral_bound() >= 5.5);

    let mutations: [fn(&mut V34Rank4LeafInput); 9] = [
        |leaf: &mut V34Rank4LeafInput| leaf.leaf_ordinal = 1,
        |leaf: &mut V34Rank4LeafInput| leaf.logical_start = 1,
        |leaf: &mut V34Rank4LeafInput| leaf.population = 0,
        |leaf: &mut V34Rank4LeafInput| leaf.group_ordinal = 1,
        |leaf: &mut V34Rank4LeafInput| leaf.residual_diagonal[0] = -1.0,
        |leaf: &mut V34Rank4LeafInput| leaf.eigenvalues.swap(0, 1),
        |leaf: &mut V34Rank4LeafInput| leaf.directions[0][0] = -1.0,
        |leaf: &mut V34Rank4LeafInput| leaf.mean[0] = f32::INFINITY,
        |leaf: &mut V34Rank4LeafInput| leaf.directions[0][0] = f32::NAN,
    ];
    for mutate in mutations {
        let mut invalid = first.clone();
        mutate(&mut invalid);
        assert!(build_v34_rank4_generation(vec![invalid]).is_err());
    }
}

#[test]
fn v34_rank4_projection_locks_100m_memory_and_work_bounds() {
    // Break caught: the resident projection drops the retiring generation,
    // confuses decimal and binary units, or hides directional score work.
    let projection = project_v34_serving_memory(414_100, 69_905).unwrap();
    assert_eq!(projection.rank_four_numeric_bytes, 960_712_000);
    assert_eq!(projection.leaf_identity_bytes, 9_938_400);
    assert_eq!(projection.cached_scalar_bytes, 13_251_200);
    assert_eq!(projection.tree_bytes, 35_791_360);
    assert_eq!(projection.active_generation_cap_bytes, 1_040 * 1_048_576);
    assert_eq!(projection.retiring_generation_cap_bytes, 1_040 * 1_048_576);
    assert_eq!(projection.shared_cache_cap_bytes, 128 * 1_048_576);
    assert_eq!(projection.runtime_cap_bytes, 160 * 1_048_576);
    assert_eq!(projection.query_workspace_cap_bytes, 512 * 1_048_576);
    assert_eq!(projection.unallocated_headroom_bytes, 96 * 1_048_576);
    assert_eq!(projection.admission_budget_bytes, 2_976 * 1_048_576);
    assert_eq!(projection.hard_limit_bytes, 3_072 * 1_048_576);
    assert!(projection.admission_budget_bytes < projection.hard_limit_bytes);
    assert_eq!(projection.exhaustive_directional_macs, 414_100 * 4 * 96);

    assert!(project_v34_serving_memory(u64::MAX, 1).is_err());
    assert!(project_v34_serving_memory(1, u64::MAX).is_err());
}

#[test]
fn v34_rank4_generation_canonicalizes_signed_zero_before_scoring() {
    // Break caught: semantically equal leaves retain different f32/f64 bytes,
    // producing different Arrow identities across languages.
    let mut leaf = coherent_leaf();
    leaf.mean[2] = -0.0;
    leaf.residual_diagonal[2] = -0.0;
    leaf.eigenvalues[2] = -0.0;
    leaf.directions[2][2] = -0.0;
    let generation = build_v34_rank4_generation(vec![leaf]).unwrap();
    let canonical = &generation.leaves()[0];
    assert!(!canonical.mean()[2].is_sign_negative());
    assert!(!canonical.residual_diagonal()[2].is_sign_negative());
    assert!(!canonical.eigenvalues()[2].is_sign_negative());
    assert!(!canonical.directions()[2][2].is_sign_negative());
}

#[test]
fn v34_rank4_zero_covariance_and_singleton_have_euclidean_score() {
    // Break caught: a zero radicand or ln(1) is treated as an invalid score.
    let input = V34Rank4LeafInput {
        leaf_ordinal: 0,
        group_ordinal: 0,
        logical_start: 0,
        population: 1,
        mean: [0.0; DIMENSIONS],
        residual_diagonal: [0.0; DIMENSIONS],
        eigenvalues: [0.0; 4],
        directions: [[0.0; DIMENSIONS]; 4],
    };
    let generation = build_v34_rank4_generation(vec![input]).unwrap();
    let mut query = [0.0; DIMENSIONS];
    query[0] = 3.0;
    assert_eq!(generation.leaves()[0].population_factor(), 0.0);
    assert_eq!(
        score_v34_rank4_leaf(&generation.leaves()[0], &query).unwrap(),
        9.0
    );
}

#[test]
fn v34_rank4_spectral_bound_rounds_outward() {
    // Break caught: a round-to-nearest sum falls below the exact covariance
    // norm, allowing a later hierarchical lower bound to prune a valid leaf.
    let mut input = coherent_leaf();
    input.residual_diagonal = [0.0; DIMENSIONS];
    input.residual_diagonal[0] = 1.0;
    input.eigenvalues = [f32::EPSILON, 0.0, 0.0, 0.0];
    input.directions = [[0.0; DIMENSIONS]; 4];
    input.directions[0][0] = 1.0;
    let generation = build_v34_rank4_generation(vec![input]).unwrap();
    let exact = 1.0 + f64::from(f32::EPSILON);
    assert!(generation.leaves()[0].spectral_bound() >= exact);
}

#[test]
fn v34_rank4_arrow_round_trip_preserves_only_validated_rank_four_state() {
    // Break caught: the serving artifact persists the V33 diagnostic ladder,
    // trusts cached moments, or changes compact leaf order on decode.
    let first = coherent_leaf();
    let mut second = coherent_leaf();
    second.leaf_ordinal = 1;
    second.group_ordinal = 1;
    second.logical_start = 2;
    let generation = build_v34_rank4_generation(vec![first, second]).unwrap();
    let (bytes, identity) = encode_v34_rank4_arrow(
        &generation,
        "s3://borsuk-v34-test/generations/rank4.arrow",
        &"11".repeat(32),
        &"22".repeat(32),
        &"33".repeat(32),
    )
    .unwrap();
    assert_eq!(identity.uri, "s3://borsuk-v34-test/generations/rank4.arrow");
    assert_eq!(identity.length, bytes.len() as u64);
    assert_eq!(identity.sha256.len(), 64);
    assert_eq!(identity.source_archive_sha256, "11".repeat(32));
    assert_eq!(identity.reconstruction_sha256, "22".repeat(32));
    assert_eq!(identity.codebooks_sha256, "33".repeat(32));
    assert_eq!(identity.metric, "squared-l2");
    assert_eq!(identity.dimensions, 96);
    assert_eq!(identity.normalization, "none");
    assert_eq!(identity.scorer_version, "v34-rank4-gaussian-lower-tail-v1");

    let decoded = decode_v34_rank4_arrow(&bytes, &identity).unwrap();
    assert_eq!(decoded, generation);
}

#[test]
fn v34_rank4_arrow_authenticates_bytes_before_semantic_use() {
    // Break caught: an object with a valid Arrow envelope is decoded before
    // its registered URI, digest, length, or upstream authority is checked.
    let generation = build_v34_rank4_generation(vec![coherent_leaf()]).unwrap();
    let (bytes, identity) = encode_v34_rank4_arrow(
        &generation,
        "s3://borsuk-v34-test/generations/rank4.arrow",
        &"11".repeat(32),
        &"22".repeat(32),
        &"33".repeat(32),
    )
    .unwrap();
    let mutations: [fn(&mut V34Rank4ArtifactIdentity); 7] = [
        |value| value.uri.push_str(".other"),
        |value| {
            let replacement = if value.sha256.starts_with('0') {
                "1"
            } else {
                "0"
            };
            value.sha256.replace_range(0..1, replacement);
        },
        |value| value.length += 1,
        |value| value.source_archive_sha256.replace_range(0..1, "0"),
        |value| value.reconstruction_sha256.replace_range(0..1, "0"),
        |value| value.codebooks_sha256.replace_range(0..1, "0"),
        |value| value.scorer_version.push_str("-other"),
    ];
    for mutate in mutations {
        let mut changed = identity.clone();
        mutate(&mut changed);
        assert!(decode_v34_rank4_arrow(&bytes, &changed).is_err());
    }
    let mut changed_bytes = bytes.clone();
    changed_bytes[0] ^= 1;
    assert!(decode_v34_rank4_arrow(&changed_bytes, &identity).is_err());
}

fn rewrite_v34_arrow_column(
    bytes: &[u8],
    identity: &V34Rank4ArtifactIdentity,
    column: usize,
    replacement: ArrayRef,
) -> (Vec<u8>, V34Rank4ArtifactIdentity) {
    let mut reader = FileReader::try_new(Cursor::new(bytes), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    let mut columns = batch.columns().to_vec();
    columns[column] = replacement;
    let changed = RecordBatch::try_new(batch.schema(), columns).unwrap();
    let mut output = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
    let mut writer =
        FileWriter::try_new_with_options(&mut output, changed.schema().as_ref(), options).unwrap();
    writer.write(&changed).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let mut changed_identity = identity.clone();
    changed_identity.length = output.len() as u64;
    changed_identity.sha256 = format!("{:x}", Sha256::digest(&output));
    (output, changed_identity)
}

fn reauthenticate_v34_bytes(
    bytes: Vec<u8>,
    identity: &V34Rank4ArtifactIdentity,
) -> (Vec<u8>, V34Rank4ArtifactIdentity) {
    let mut changed_identity = identity.clone();
    changed_identity.length = bytes.len() as u64;
    changed_identity.sha256 = format!("{:x}", Sha256::digest(&bytes));
    (bytes, changed_identity)
}

fn write_v34_arrow_batches(
    schema: Arc<Schema>,
    batches: &[RecordBatch],
    options: IpcWriteOptions,
) -> Vec<u8> {
    let mut output = Vec::new();
    let mut writer =
        FileWriter::try_new_with_options(&mut output, schema.as_ref(), options).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.finish().unwrap();
    drop(writer);
    output
}

#[test]
fn v34_rank4_arrow_rejects_authenticated_physical_and_cached_state_drift() {
    // Break caught: a caller coherently re-hashes malformed Arrow or persisted
    // cached moments are trusted rather than recomputed from compact fields.
    let generation = build_v34_rank4_generation(vec![coherent_leaf()]).unwrap();
    let (bytes, identity) = encode_v34_rank4_arrow(
        &generation,
        "s3://borsuk-v34-test/generations/rank4.arrow",
        &"11".repeat(32),
        &"22".repeat(32),
        &"33".repeat(32),
    )
    .unwrap();
    let mut reader = FileReader::try_new(Cursor::new(&bytes), None).unwrap();
    let schema = reader.schema();
    assert_eq!(
        schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>(),
        [
            "leaf_ordinal",
            "group_ordinal",
            "logical_start",
            "population",
            "mean",
            "residual_diagonal",
            "eigenvalues",
            "directions",
            "population_factor",
            "trace",
            "trace_square",
            "spectral_bound",
        ]
    );
    assert_eq!(schema.metadata().len(), 1);
    let manifest = schema.metadata().get("borsuk.v34.rank4.manifest").unwrap();
    let parsed: serde_json::Value = serde_json::from_str(manifest).unwrap();
    assert_eq!(parsed["dimensions"], 96);
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());

    let traces = batch
        .column(9)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let changed_trace = Arc::new(Float64Array::from_iter_values(
        (0..traces.len()).map(|row| traces.value(row) + 1.0),
    ));
    let (changed_bytes, changed_identity) =
        rewrite_v34_arrow_column(&bytes, &identity, 9, changed_trace);
    assert!(decode_v34_rank4_arrow(&changed_bytes, &changed_identity).is_err());

    let mut direction_values = generation.leaves()[0]
        .directions()
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    direction_values[2 * DIMENSIONS] = f32::NAN;
    let direction_vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        DIMENSIONS as i32,
        Arc::new(Float32Array::from(direction_values)),
        None,
    )
    .unwrap();
    let changed_directions = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new(
                "direction",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    DIMENSIONS as i32,
                ),
                false,
            )),
            4,
            Arc::new(direction_vectors),
            None,
        )
        .unwrap(),
    );
    let (changed_bytes, changed_identity) =
        rewrite_v34_arrow_column(&bytes, &identity, 7, changed_directions);
    assert!(decode_v34_rank4_arrow(&changed_bytes, &changed_identity).is_err());

    let mut inactive_direction_values = generation.leaves()[0]
        .directions()
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    inactive_direction_values[2 * DIMENSIONS] = 1.0;
    let inactive_direction_vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", DataType::Float32, false)),
        DIMENSIONS as i32,
        Arc::new(Float32Array::from(inactive_direction_values)),
        None,
    )
    .unwrap();
    let inactive_directions = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new(
                "direction",
                DataType::FixedSizeList(
                    Arc::new(Field::new("element", DataType::Float32, false)),
                    DIMENSIONS as i32,
                ),
                false,
            )),
            4,
            Arc::new(inactive_direction_vectors),
            None,
        )
        .unwrap(),
    );
    let (changed_bytes, changed_identity) =
        rewrite_v34_arrow_column(&bytes, &identity, 7, inactive_directions);
    assert!(decode_v34_rank4_arrow(&changed_bytes, &changed_identity).is_err());

    let mut negative_zero_mean = generation.leaves()[0].mean().to_vec();
    negative_zero_mean[0] = -0.0;
    let changed_mean = Arc::new(
        FixedSizeListArray::try_new(
            Arc::new(Field::new("element", DataType::Float32, false)),
            DIMENSIONS as i32,
            Arc::new(Float32Array::from(negative_zero_mean)),
            None,
        )
        .unwrap(),
    );
    let (changed_bytes, changed_identity) =
        rewrite_v34_arrow_column(&bytes, &identity, 4, changed_mean);
    assert!(decode_v34_rank4_arrow(&changed_bytes, &changed_identity).is_err());

    let zero_generation = build_v34_rank4_generation(vec![V34Rank4LeafInput {
        leaf_ordinal: 0,
        group_ordinal: 0,
        logical_start: 0,
        population: 1,
        mean: [0.0; DIMENSIONS],
        residual_diagonal: [0.0; DIMENSIONS],
        eigenvalues: [0.0; 4],
        directions: [[0.0; DIMENSIONS]; 4],
    }])
    .unwrap();
    let (zero_bytes, zero_identity) = encode_v34_rank4_arrow(
        &zero_generation,
        "s3://borsuk-v34-test/generations/zero.arrow",
        &"11".repeat(32),
        &"22".repeat(32),
        &"33".repeat(32),
    )
    .unwrap();
    let negative_zero_cache = Arc::new(Float64Array::from(vec![-0.0]));
    let (changed_bytes, changed_identity) =
        rewrite_v34_arrow_column(&zero_bytes, &zero_identity, 8, negative_zero_cache);
    assert!(decode_v34_rank4_arrow(&changed_bytes, &changed_identity).is_err());

    let compressed_options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5)
        .unwrap()
        .try_with_compression(Some(CompressionType::ZSTD))
        .unwrap();
    let mut compressed = Vec::new();
    let mut compressed_writer = FileWriter::try_new_with_options(
        &mut compressed,
        batch.schema().as_ref(),
        compressed_options,
    )
    .unwrap();
    compressed_writer.write(&batch).unwrap();
    compressed_writer.finish().unwrap();
    drop(compressed_writer);
    let (compressed, compressed_identity) = reauthenticate_v34_bytes(compressed, &identity);
    assert!(decode_v34_rank4_arrow(&compressed, &compressed_identity).is_err());

    let options = || IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
    let zero_batch = RecordBatch::new_empty(batch.schema());
    let zero_bytes = write_v34_arrow_batches(batch.schema(), &[zero_batch], options());
    let (zero_bytes, zero_identity) = reauthenticate_v34_bytes(zero_bytes, &identity);
    assert!(decode_v34_rank4_arrow(&zero_bytes, &zero_identity).is_err());

    let multiple_bytes =
        write_v34_arrow_batches(batch.schema(), &[batch.clone(), batch.clone()], options());
    let (multiple_bytes, multiple_identity) = reauthenticate_v34_bytes(multiple_bytes, &identity);
    assert!(decode_v34_rank4_arrow(&multiple_bytes, &multiple_identity).is_err());

    let mut extra_metadata = batch.schema().metadata().clone();
    extra_metadata.insert("unexpected".to_owned(), "value".to_owned());
    let extra_schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().clone(),
        extra_metadata,
    ));
    let extra_batch = RecordBatch::try_new(extra_schema.clone(), batch.columns().to_vec()).unwrap();
    let extra_bytes = write_v34_arrow_batches(extra_schema, &[extra_batch], options());
    let (extra_bytes, extra_identity) = reauthenticate_v34_bytes(extra_bytes, &identity);
    assert!(decode_v34_rank4_arrow(&extra_bytes, &extra_identity).is_err());

    let missing_schema = Arc::new(Schema::new_with_metadata(
        batch.schema().fields().clone(),
        HashMap::new(),
    ));
    let missing_batch =
        RecordBatch::try_new(missing_schema.clone(), batch.columns().to_vec()).unwrap();
    let missing_bytes = write_v34_arrow_batches(missing_schema, &[missing_batch], options());
    let (missing_bytes, missing_identity) = reauthenticate_v34_bytes(missing_bytes, &identity);
    assert!(decode_v34_rank4_arrow(&missing_bytes, &missing_identity).is_err());

    let mut nullable_fields = batch.schema().fields().to_vec();
    nullable_fields[8] = Arc::new(Field::new("population_factor", DataType::Float64, true));
    let nullable_schema = Arc::new(Schema::new_with_metadata(
        nullable_fields,
        batch.schema().metadata().clone(),
    ));
    let mut nullable_columns = batch.columns().to_vec();
    nullable_columns[8] = Arc::new(Float64Array::from(vec![None; batch.num_rows()]));
    let nullable_batch = RecordBatch::try_new(nullable_schema.clone(), nullable_columns).unwrap();
    let nullable_bytes = write_v34_arrow_batches(nullable_schema, &[nullable_batch], options());
    let (nullable_bytes, nullable_identity) = reauthenticate_v34_bytes(nullable_bytes, &identity);
    assert!(decode_v34_rank4_arrow(&nullable_bytes, &nullable_identity).is_err());

    let mut conflicting_leading_schema = bytes.clone();
    let uri = identity.uri.as_bytes();
    let occurrences = conflicting_leading_schema
        .windows(uri.len())
        .enumerate()
        .filter_map(|(index, value)| (value == uri).then_some(index))
        .collect::<Vec<_>>();
    assert_eq!(occurrences.len(), 2);
    conflicting_leading_schema[occurrences[0] + uri.len() - 1] ^= 1;
    let (changed_bytes, changed_identity) =
        reauthenticate_v34_bytes(conflicting_leading_schema, &identity);
    assert!(decode_v34_rank4_arrow(&changed_bytes, &changed_identity).is_err());

    let mut impossible_footer = bytes.clone();
    let footer_length_offset = impossible_footer.len() - 10;
    impossible_footer[footer_length_offset..footer_length_offset + 4]
        .copy_from_slice(&u32::MAX.to_le_bytes());
    let (changed_bytes, changed_identity) = reauthenticate_v34_bytes(impossible_footer, &identity);
    assert!(decode_v34_rank4_arrow(&changed_bytes, &changed_identity).is_err());

    let null_population_factor = Arc::new(Float64Array::from(vec![None; batch.num_rows()]));
    let mut null_columns = batch.columns().to_vec();
    null_columns[8] = null_population_factor;
    assert!(RecordBatch::try_new(batch.schema(), null_columns).is_err());

    let quoted_uri = "s3://borsuk-v34-test/generations/rank4-\"quoted\".arrow";
    let (quoted_bytes, quoted_identity) = encode_v34_rank4_arrow(
        &generation,
        quoted_uri,
        &"11".repeat(32),
        &"22".repeat(32),
        &"33".repeat(32),
    )
    .unwrap();
    let quoted_reader = FileReader::try_new(Cursor::new(&quoted_bytes), None).unwrap();
    let quoted_manifest: serde_json::Value = serde_json::from_str(
        quoted_reader
            .schema()
            .metadata()
            .get("borsuk.v34.rank4.manifest")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(quoted_manifest["uri"], quoted_uri);
    decode_v34_rank4_arrow(&quoted_bytes, &quoted_identity).unwrap();
}
