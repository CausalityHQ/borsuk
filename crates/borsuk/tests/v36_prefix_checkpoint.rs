//! Durable V36 prefix-freeze checkpoint and resume-state contracts.

use std::io::Cursor;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    V36ArtifactIdentity, V36PrefixCheckpointContext, V36PrefixCheckpointManifest,
    V36PrefixCheckpointPhase, V36PrefixIdentityRun, V36PrefixMaterializedArtifacts,
    V36PrefixPopulationCheckpoint, V36PrefixRegisteredSourceObject, V36PrefixRowIdentity,
    V36PrefixSourceObject, canonical_v36_prefix_checkpoint_manifest_bytes,
    decode_v36_prefix_identity_run, encode_v36_prefix_identity_run, restore_v36_prefix_population,
    validate_v36_prefix_checkpoint_manifest_with_context,
    validate_v36_prefix_checkpoint_transition,
};
use sha2::{Digest, Sha256};

fn artifact(role: &str, filename: &str, byte: char) -> V36ArtifactIdentity {
    let digest = byte.to_string().repeat(64);
    V36ArtifactIdentity {
        blake3: digest.clone(),
        encoded_bytes: 1_024,
        role: role.to_owned(),
        sha256: digest.clone(),
        uri: format!("s3://fixture/v36/runs/v36-prefix-screen-fixture/objects/{digest}-{filename}"),
    }
}

fn identity_run_artifact(bytes: &[u8], ordinal: u16) -> V36ArtifactIdentity {
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    V36ArtifactIdentity {
        blake3: blake3::hash(bytes).to_hex().to_string(),
        encoded_bytes: bytes.len().try_into().unwrap(),
        role: format!("population-identity-run-{ordinal:04}"),
        sha256: sha256.clone(),
        uri: format!(
            "s3://fixture/v36/runs/v36-prefix-screen-fixture/objects/{sha256}-population-identity-run-{ordinal:04}.arrow"
        ),
    }
}

fn identity(feature_row_id: u64, row_offset: u64, ordinal: u16) -> V36PrefixRowIdentity {
    V36PrefixRowIdentity {
        feature_row_id,
        row_offset,
        selected_object_ordinal: ordinal,
        source_ordinal: None,
    }
}

fn source_object() -> V36PrefixSourceObject {
    let path = "data/part-0000.parquet";
    let encoded_bytes = 2_048_u64;
    let mut sample = Sha256::new();
    sample.update(b"borsuk-v36-screen-object-v1");
    sample.update(path.as_bytes());
    sample.update(encoded_bytes.to_le_bytes());
    V36PrefixSourceObject {
        blake3: "6".repeat(64),
        encoded_bytes,
        path: path.into(),
        sample_sha256: format!("{:x}", sample.finalize()),
        sha256: "8".repeat(64),
        uri: "https://example.invalid/data/part-0000.parquet".into(),
    }
}

fn registered_source() -> V36PrefixRegisteredSourceObject {
    let source = source_object();
    V36PrefixRegisteredSourceObject {
        encoded_bytes: source.encoded_bytes,
        path: source.path,
        sha256: source.sha256,
        uri: source.uri,
    }
}

fn population_manifest() -> V36PrefixCheckpointManifest {
    V36PrefixCheckpointManifest {
        claim_eligible: false,
        execution_authority_sha256: "1".repeat(64),
        freeze_authority_sha256: "2".repeat(64),
        generation: 0,
        phase: V36PrefixCheckpointPhase::Population,
        population: V36PrefixPopulationCheckpoint {
            consumed_objects: vec![source_object()],
            cutoff_object_ordinal: None,
            cutoff_row_offset: None,
            distinct_rows: 80,
            duplicate_rows: 20,
            identity_runs: vec![artifact(
                "population-identity-run-0000",
                "population-identity-run-0000.arrow",
                '9',
            )],
            next_object_ordinal: 1,
            physical_rows: 100,
        },
        previous_checkpoint: None,
        producer_attempt_id: "v36-prefix-screen-fixture-attempt-0000".into(),
        producer_attempt_ordinal: 0,
        producer_instance_id: "i-fixture".into(),
        run_id: "v36-prefix-screen-fixture".into(),
        schema: "borsuk-v36-prefix-freeze-checkpoint-v1".into(),
        source_archive_sha256: "3".repeat(64),
        source_commit: "4".repeat(40),
        source_registry_sha256: "5".repeat(64),
    }
}

fn checkpoint_context(distinct_candidates: u64) -> V36PrefixCheckpointContext {
    V36PrefixCheckpointContext {
        corpus_rows: 50,
        distinct_candidates,
        freeze_authority_sha256: "2".repeat(64),
        gt_block_rows: 16,
        object_cap: 16,
        object_prefix: "s3://fixture/v36/runs/v36-prefix-screen-fixture/objects/".into(),
        ranked_objects: vec![registered_source()],
        run_id: "v36-prefix-screen-fixture".into(),
        source_archive_sha256: "3".repeat(64),
        source_byte_cap: 8_192,
        source_commit: "4".repeat(40),
        source_registry_sha256: "5".repeat(64),
    }
}

fn materialized_artifacts() -> V36PrefixMaterializedArtifacts {
    V36PrefixMaterializedArtifacts {
        population_authority: artifact("population-authority", "population-authority.json", '1'),
        source: artifact("source", "source.parquet", '2'),
        development_query: artifact("development-query", "development-query.parquet", '3'),
        validation_query: artifact("validation-query", "validation-query.parquet", '4'),
        sealed_holdout_query: artifact("sealed-holdout-query", "sealed-holdout-query.parquet", '5'),
        performance_query: artifact("performance-query", "performance-query.parquet", '6'),
    }
}

#[test]
fn v36_prefix_checkpoint_population_authority_is_canonical_and_closed() {
    let manifest = population_manifest();
    let bytes = canonical_v36_prefix_checkpoint_manifest_bytes(&manifest).unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));
    assert!(!bytes[..bytes.len() - 1].contains(&b'\n'));

    let mut bad_counter = manifest.clone();
    bad_counter.population.duplicate_rows = 21;
    assert!(canonical_v36_prefix_checkpoint_manifest_bytes(&bad_counter).is_err());

    let mut bad_chain = manifest;
    bad_chain.generation = 1;
    assert!(canonical_v36_prefix_checkpoint_manifest_bytes(&bad_chain).is_err());
}

#[test]
fn v36_prefix_checkpoint_gt_binds_complete_all_query_source_prefix() {
    let mut manifest = population_manifest();
    manifest.population.cutoff_object_ordinal = Some(0);
    manifest.population.cutoff_row_offset = Some(79);
    manifest.generation = 1;
    manifest.previous_checkpoint = Some(artifact(
        "checkpoint-manifest",
        "checkpoint-00000000.json",
        'a',
    ));
    manifest.phase = V36PrefixCheckpointPhase::GroundTruth {
        materialized: materialized_artifacts(),
        heaps: artifact("gt-heaps", "gt-heaps-00001024.arrow", 'b'),
        next_source_ordinal: 48,
    };
    let context = checkpoint_context(80);
    assert!(validate_v36_prefix_checkpoint_manifest_with_context(&context, &manifest).is_ok());

    let V36PrefixCheckpointPhase::GroundTruth {
        next_source_ordinal,
        ..
    } = &mut manifest.phase
    else {
        unreachable!()
    };
    *next_source_ordinal = 0;
    assert!(validate_v36_prefix_checkpoint_manifest_with_context(&context, &manifest).is_err());
}

#[test]
fn v36_prefix_checkpoint_context_rejects_foreign_prefix_and_registry() {
    let manifest = population_manifest();
    let context = checkpoint_context(100);
    validate_v36_prefix_checkpoint_manifest_with_context(&context, &manifest).unwrap();

    let mut foreign = context.clone();
    foreign.object_prefix = "s3://fixture/v36/runs/foreign/objects/".into();
    assert!(validate_v36_prefix_checkpoint_manifest_with_context(&foreign, &manifest).is_err());

    let mut wrong_registry = context;
    wrong_registry.ranked_objects[0].path = "data/other.parquet".into();
    assert!(
        validate_v36_prefix_checkpoint_manifest_with_context(&wrong_registry, &manifest).is_err()
    );
}

#[test]
fn v36_prefix_checkpoint_transition_is_monotonic_across_attempts() {
    let context = checkpoint_context(80);
    let mut previous = population_manifest();
    previous.population.cutoff_object_ordinal = Some(0);
    previous.population.cutoff_row_offset = Some(79);
    let previous_bytes = canonical_v36_prefix_checkpoint_manifest_bytes(&previous).unwrap();
    let digest = format!("{:x}", Sha256::digest(&previous_bytes));
    let mut next = previous.clone();
    next.generation = 1;
    next.previous_checkpoint = Some(V36ArtifactIdentity {
        blake3: blake3::hash(&previous_bytes).to_hex().to_string(),
        encoded_bytes: previous_bytes.len().try_into().unwrap(),
        role: "checkpoint-manifest".into(),
        sha256: digest.clone(),
        uri: format!(
            "s3://fixture/v36/runs/v36-prefix-screen-fixture/objects/{digest}-checkpoint-00000000.json"
        ),
    });
    next.producer_attempt_id = "v36-prefix-screen-fixture-attempt-0001".into();
    next.producer_attempt_ordinal = 1;
    next.phase = V36PrefixCheckpointPhase::Materialized {
        artifacts: materialized_artifacts(),
    };
    validate_v36_prefix_checkpoint_transition(&context, &previous, &next).unwrap();

    let mut skipped_materialization = next.clone();
    skipped_materialization.phase = V36PrefixCheckpointPhase::GroundTruth {
        heaps: artifact("gt-heaps", "gt-heaps-00000032.arrow", 'd'),
        materialized: materialized_artifacts(),
        next_source_ordinal: 32,
    };
    assert!(
        validate_v36_prefix_checkpoint_transition(&context, &previous, &skipped_materialization)
            .is_err()
    );

    let mut generation_gap = next.clone();
    generation_gap.generation = 3;
    assert!(validate_v36_prefix_checkpoint_transition(&context, &next, &generation_gap).is_err());
    let mut foreign_run = next;
    foreign_run.run_id = "v36-prefix-screen-foreign".into();
    assert!(validate_v36_prefix_checkpoint_transition(&context, &previous, &foreign_run).is_err());
}

#[test]
fn v36_prefix_checkpoint_identity_run_is_strict_arrow_ipc() {
    let rows = vec![identity(41, 2, 3), identity(7, 9, 3)];
    let run = V36PrefixIdentityRun {
        physical_rows: 12,
        rows: rows.clone(),
        selected_object_ordinal: 3,
        source: source_object(),
    };
    let bytes = encode_v36_prefix_identity_run(&run).unwrap();
    assert_eq!(bytes, encode_v36_prefix_identity_run(&run).unwrap());
    let registered = identity_run_artifact(&bytes, 3);
    assert_eq!(
        decode_v36_prefix_identity_run(&bytes, &registered, &source_object(), 3).unwrap(),
        run
    );

    let mut wrong_role = registered.clone();
    wrong_role.role = "population-identity-run-0004".into();
    assert!(decode_v36_prefix_identity_run(&bytes, &wrong_role, &source_object(), 3).is_err());
    assert!(decode_v36_prefix_identity_run(&bytes, &registered, &source_object(), 4).is_err());

    let mut bad = run.clone();
    bad.rows[0].source_ordinal = Some(0);
    assert!(encode_v36_prefix_identity_run(&bad).is_err());
    bad = run.clone();
    bad.rows.reverse();
    assert!(encode_v36_prefix_identity_run(&bad).is_err());
    bad = run;
    bad.rows[1].feature_row_id = bad.rows[0].feature_row_id;
    assert!(encode_v36_prefix_identity_run(&bad).is_err());
    let mut bad_source = empty_identity_run(3, 5);
    bad_source.source.sample_sha256 = "f".repeat(64);
    assert!(encode_v36_prefix_identity_run(&bad_source).is_err());

    let empty = empty_identity_run(3, 5);
    let empty_bytes = encode_v36_prefix_identity_run(&empty).unwrap();
    assert_eq!(
        decode_v36_prefix_identity_run(
            &empty_bytes,
            &identity_run_artifact(&empty_bytes, 3),
            &source_object(),
            3,
        )
        .unwrap(),
        empty
    );
}

fn empty_identity_run(selected_object_ordinal: u16, physical_rows: u64) -> V36PrefixIdentityRun {
    V36PrefixIdentityRun {
        physical_rows,
        rows: Vec::new(),
        selected_object_ordinal,
        source: source_object(),
    }
}

#[test]
fn v36_prefix_checkpoint_identity_run_rejects_noncanonical_schema() {
    let schema = Schema::new(vec![
        Field::new("feature_row_id", DataType::UInt64, false),
        Field::new("row_offset", DataType::UInt64, true),
    ]);
    let batch = RecordBatch::try_new(
        std::sync::Arc::new(schema.clone()),
        vec![
            std::sync::Arc::new(UInt64Array::from(vec![41])),
            std::sync::Arc::new(UInt64Array::from(vec![Some(2)])),
        ],
    )
    .unwrap();
    let options = IpcWriteOptions::try_new(8, false, arrow_ipc::MetadataVersion::V5).unwrap();
    let mut bytes = Vec::new();
    let mut writer = FileWriter::try_new_with_options(&mut bytes, &schema, options).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);

    let registered = identity_run_artifact(&bytes, 3);
    assert!(decode_v36_prefix_identity_run(&bytes, &registered, &source_object(), 3).is_err());
    assert!(arrow_ipc::reader::FileReader::try_new(Cursor::new(bytes), None).is_ok());
}

#[test]
fn v36_prefix_checkpoint_identity_runs_restore_complete_cutoff_object() {
    let mut second_source = source_object();
    second_source.path = "data/part-0001.parquet".into();
    second_source.uri = "https://example.invalid/data/part-0001.parquet".into();
    second_source.sha256 = "a".repeat(64);
    second_source.blake3 = "b".repeat(64);
    let mut second_sample = Sha256::new();
    second_sample.update(b"borsuk-v36-screen-object-v1");
    second_sample.update(second_source.path.as_bytes());
    second_sample.update(second_source.encoded_bytes.to_le_bytes());
    second_source.sample_sha256 = format!("{:x}", second_sample.finalize());
    let runs = vec![
        V36PrefixIdentityRun {
            physical_rows: 3,
            rows: vec![identity(1, 0, 0), identity(2, 2, 0)],
            selected_object_ordinal: 0,
            source: source_object(),
        },
        V36PrefixIdentityRun {
            physical_rows: 4,
            rows: vec![identity(3, 0, 1), identity(4, 1, 1), identity(5, 3, 1)],
            selected_object_ordinal: 1,
            source: second_source,
        },
    ];
    let restored = restore_v36_prefix_population(&runs, 4).unwrap();
    assert_eq!(restored.cutoff_object_ordinal, 1);
    assert_eq!(restored.cutoff_row_offset, 1);
    assert_eq!(restored.distinct_rows_observed, 5);
    assert_eq!(restored.duplicate_rows, 2);
    assert_eq!(restored.physical_rows, 7);
    assert_eq!(
        restored
            .unique_rows
            .iter()
            .map(|row| row.feature_row_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4]
    );

    let mut duplicate = runs.clone();
    duplicate[1].rows[0].feature_row_id = 1;
    assert!(restore_v36_prefix_population(&duplicate, 4).is_err());
    let mut extra_after_cutoff = runs;
    extra_after_cutoff.push(V36PrefixIdentityRun {
        physical_rows: 1,
        rows: Vec::new(),
        selected_object_ordinal: 2,
        source: source_object(),
    });
    assert!(restore_v36_prefix_population(&extra_after_cutoff, 4).is_err());
}
