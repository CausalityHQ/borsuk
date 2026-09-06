//! Contract tests for the bounded V36 diagnostic population and exact truth.

use std::{fs, path::Path, sync::Arc};

use arrow_array::{
    ArrayRef, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch, StringArray,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    V36ArtifactIdentity, V36PrefixFreezeAuthority, V36PrefixFreezeExecutionAuthority,
    V36PrefixFreezeReceipt, V36PrefixFreezeRequest, V36PrefixGtAccumulator, V36PrefixGtParquetJob,
    V36PrefixInputRow, V36PrefixPopulationAuthority, V36PrefixPopulationCommit,
    V36PrefixQualityRole, V36PrefixRankedSourceObject, V36PrefixRegisteredSourceObject,
    V36PrefixResumeBinding, V36PrefixRoleAuthority, V36PrefixSourceObject,
    bind_v36_prefix_population_authority, canonical_v36_prefix_freeze_authority_bytes,
    canonical_v36_prefix_freeze_execution_authority_bytes,
    canonical_v36_prefix_freeze_receipt_bytes, canonical_v36_prefix_population_authority_bytes,
    canonical_v36_prefix_source_registry_bytes, deduplicate_v36_prefix_row_identities,
    exact_v36_prefix_gt100, load_v36_prefix_freeze_preflight, materialize_v36_prefix_role_parquets,
    rank_v36_prefix_source_objects, restore_v36_prefix_population, scan_v36_prefix_gt100_parquet,
    scan_v36_prefix_object_prefix, scan_v36_prefix_object_prefix_checkpointed,
    scan_v36_prefix_object_prefix_resumed, scan_v36_prefix_query_parquet,
    scan_v36_prefix_registered_input_parquet, scan_v36_prefix_source_parquet,
    select_v36_prefix_roles, v36_prefix_gt100_schema, v36_prefix_query_schema,
    v36_prefix_query_score_sha256, v36_prefix_source_schema, v36_prefix_source_score_sha256,
    validate_v36_prefix_cutoff_membership, validate_v36_prefix_freeze_authority,
    validate_v36_prefix_freeze_execution_authority, validate_v36_prefix_freeze_receipt,
    validate_v36_prefix_input_row, validate_v36_prefix_role_authority,
    write_v36_prefix_gt100_parquet, write_v36_prefix_gt100_roles_from_parquets,
    write_v36_prefix_query_parquet, write_v36_prefix_source_parquet,
};
use sha2::{Digest, Sha256};

const DIMENSIONS: usize = 768;

fn vector(axis: usize, value: f32) -> Vec<f32> {
    let mut vector = vec![0.0; DIMENSIONS];
    vector[0] = 1.0;
    vector[axis] = value;
    vector
}

fn input(feature_row_id: i64, object: u16, row: u64) -> V36PrefixInputRow {
    V36PrefixInputRow {
        feature_row_id,
        selected_object_ordinal: object,
        row_offset: row,
        embedding: vector(1, (row + 1) as f32 / 100.0),
    }
}

fn sample_digest(path: &str, encoded_bytes: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"borsuk-v36-screen-object-v1");
    hasher.update(path.as_bytes());
    hasher.update(encoded_bytes.to_le_bytes());
    format!("{:x}", hasher.finalize())
}

fn role_authorities() -> [V36PrefixRoleAuthority; 4] {
    let definitions = [
        (
            "development",
            1_000,
            "borsuk-v36-prefix-screen-development-query-v1",
        ),
        (
            "validation",
            1_000,
            "borsuk-v36-prefix-screen-validation-query-v1",
        ),
        (
            "sealed-holdout",
            1_000,
            "borsuk-v36-prefix-screen-sealed-holdout-query-v1",
        ),
        (
            "performance",
            10_000,
            "borsuk-v36-prefix-screen-performance-query-v1",
        ),
    ];
    definitions.map(|(role, rows, seed_label)| V36PrefixRoleAuthority {
        role: role.into(),
        rows,
        seed_label: seed_label.into(),
        seed_sha256: format!("{:x}", Sha256::digest(seed_label.as_bytes())),
    })
}

fn population(registry: &[V36PrefixRegisteredSourceObject]) -> V36PrefixPopulationAuthority {
    let mut manifest_order = registry.iter().collect::<Vec<_>>();
    manifest_order.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    let mut manifest = Sha256::new();
    for object in manifest_order {
        manifest.update(object.path.as_bytes());
        manifest.update(b"\t");
        manifest.update(object.sha256.as_bytes());
        manifest.update(b"\t");
        manifest.update(object.encoded_bytes.to_string().as_bytes());
        manifest.update(b"\n");
    }
    let mut consumed = registry
        .iter()
        .map(|object| V36PrefixSourceObject {
            blake3: "9".repeat(64),
            encoded_bytes: object.encoded_bytes,
            path: object.path.clone(),
            sample_sha256: sample_digest(&object.path, object.encoded_bytes),
            sha256: object.sha256.clone(),
            uri: object.uri.clone(),
        })
        .collect::<Vec<_>>();
    consumed.sort_by(|left, right| {
        (&left.sample_sha256, &left.path).cmp(&(&right.sample_sha256, &right.path))
    });
    V36PrefixPopulationAuthority {
        claim_eligible: false,
        construction_capability: "named-query-excluded-corpus-only-no-query-truth".into(),
        consumed_objects: consumed,
        corpus_rows: 1_000_000,
        distinct_candidates: 1_100_000,
        duplicate_rule: "first-selected-object-ordinal-then-row-offset".into(),
        evaluation_capability: "named-artifacts-only-no-source-list-discovery".into(),
        format: "borsuk-v36-prefix-population-authority-v1".into(),
        object_cap: 16,
        object_sampling_algorithm:
            "sha256-borsuk-v36-screen-object-v1-path-utf8-length-le-u64-then-path".into(),
        ordered_source_manifest_sha256: format!("{:x}", manifest.finalize()),
        population_id: "borsuk-v36-prefix-screen-population-v1".into(),
        roles: role_authorities().into(),
        source_byte_cap: 6 * 1024 * 1024 * 1024,
        source_revision: "bfc7465dcf1245bd605d35dcaf5d2177bbc2025a".into(),
        workspace_bytes: 32 * 1024 * 1024,
        workspace_count: 16,
    }
}

fn freeze_authority(registry: &[V36PrefixRegisteredSourceObject]) -> V36PrefixFreezeAuthority {
    let population = population(registry);
    V36PrefixFreezeAuthority {
        claim_eligible: false,
        construction_capability: population.construction_capability,
        corpus_rows: population.corpus_rows,
        distinct_candidates: population.distinct_candidates,
        duplicate_rule: population.duplicate_rule,
        evaluation_capability: population.evaluation_capability,
        object_cap: population.object_cap,
        object_sampling_algorithm: population.object_sampling_algorithm,
        ordered_source_manifest_sha256: population.ordered_source_manifest_sha256,
        registry_encoded_bytes: registry.iter().map(|object| object.encoded_bytes).sum(),
        registry_objects: registry.len().try_into().unwrap(),
        roles: population.roles,
        schema: "borsuk-v36-prefix-freeze-authority-v1".into(),
        source_byte_cap: population.source_byte_cap,
        source_revision: population.source_revision,
        workspace_bytes: population.workspace_bytes,
        workspace_count: population.workspace_count,
        invalid_row_policy: "reject-complete-source-revision".into(),
    }
}

fn source_registry() -> Vec<V36PrefixRegisteredSourceObject> {
    vec![
        V36PrefixRegisteredSourceObject {
            encoded_bytes: 20,
            path: "data/z.parquet".into(),
            sha256: "1".repeat(64),
            uri: "https://huggingface.co/datasets/andropar/relaion2b-natural-embeddings/resolve/bfc7465dcf1245bd605d35dcaf5d2177bbc2025a/data/z.parquet".into(),
        },
        V36PrefixRegisteredSourceObject {
            encoded_bytes: 10,
            path: "data/a.parquet".into(),
            sha256: "2".repeat(64),
            uri: "https://huggingface.co/datasets/andropar/relaion2b-natural-embeddings/resolve/bfc7465dcf1245bd605d35dcaf5d2177bbc2025a/data/a.parquet".into(),
        },
    ]
}

#[test]
fn v36_prefix_dataset_ranks_complete_objects_and_keeps_first_valid_occurrence() {
    let registry = source_registry();
    let authority = freeze_authority(&registry);
    let ranked = rank_v36_prefix_source_objects(&authority, &registry).unwrap();
    let mut expected = registry
        .iter()
        .map(|object| {
            (
                sample_digest(&object.path, object.encoded_bytes),
                object.path.clone(),
            )
        })
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(
        ranked
            .iter()
            .map(|object| (object.sample_sha256.clone(), object.path.clone()))
            .collect::<Vec<_>>(),
        expected
    );

    let rows = [input(7, 0, 0), input(9, 0, 1), input(7, 1, 0)];
    let identities = rows
        .iter()
        .map(validate_v36_prefix_input_row)
        .collect::<borsuk::Result<Vec<_>>>()
        .unwrap();
    let deduplicated = deduplicate_v36_prefix_row_identities(identities).unwrap();
    assert_eq!(
        deduplicated
            .iter()
            .map(|row| row.feature_row_id)
            .collect::<Vec<_>>(),
        [7, 9]
    );
    let mut invalid = input(-1, 0, 0);
    assert!(validate_v36_prefix_input_row(&invalid).is_err());
    invalid.feature_row_id = 1;
    invalid.embedding[3] = f32::NAN;
    assert!(validate_v36_prefix_input_row(&invalid).is_err());
    invalid.embedding[3] = 0.0;
    invalid.embedding.fill(0.0);
    assert!(validate_v36_prefix_input_row(&invalid).is_err());
    invalid.embedding = vec![1.0; DIMENSIONS - 1];
    assert!(validate_v36_prefix_input_row(&invalid).is_err());
}

#[test]
fn v36_prefix_dataset_freeze_input_and_population_output_are_not_circular() {
    let registry = source_registry();
    let authority = freeze_authority(&registry);
    validate_v36_prefix_freeze_authority(&authority, &registry).unwrap();
    let bytes = canonical_v36_prefix_freeze_authority_bytes(&authority, &registry).unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));
    assert!(!bytes.windows(2).any(|pair| pair == b" \n"));
    let ranked = rank_v36_prefix_source_objects(&authority, &registry).unwrap();
    let consumed = ranked
        .into_iter()
        .map(|object| V36PrefixSourceObject {
            blake3: "9".repeat(64),
            encoded_bytes: object.encoded_bytes,
            path: object.path,
            sample_sha256: object.sample_sha256,
            sha256: object.sha256,
            uri: object.uri,
        })
        .collect();
    let population = bind_v36_prefix_population_authority(&authority, consumed, &registry).unwrap();
    assert_eq!(
        population.population_id,
        "borsuk-v36-prefix-screen-population-v1"
    );
    assert_eq!(population.consumed_objects.len(), 2);
    let mut drifted = authority;
    drifted.invalid_row_policy = "skip-invalid".into();
    assert!(validate_v36_prefix_freeze_authority(&drifted, &registry).is_err());

    let mut subset = registry.clone();
    subset.pop();
    let mut subset_authority = freeze_authority(&subset);
    subset_authority.registry_objects = registry.len().try_into().unwrap();
    subset_authority.registry_encoded_bytes =
        registry.iter().map(|object| object.encoded_bytes).sum();
    assert!(validate_v36_prefix_freeze_authority(&subset_authority, &subset).is_err());
}

#[test]
fn v36_prefix_dataset_rejects_unfrozen_population_and_role_authority() {
    let rows = (0..32)
        .map(|ordinal| {
            validate_v36_prefix_input_row(&input(10_000 + ordinal, 0, ordinal as u64)).unwrap()
        })
        .collect::<Vec<_>>();
    let roles = role_authorities();
    validate_v36_prefix_role_authority(&roles).unwrap();
    assert!(validate_v36_prefix_role_authority(&roles[..3]).is_err());
    assert_eq!(
        v36_prefix_query_score_sha256(&roles[0].seed_label, &"a".repeat(64), 1).unwrap(),
        "ab16a833337789b1fbde70e9b6ffc119857bfe07efdb18b1fc4587d7e81d60e2"
    );
    assert_eq!(
        v36_prefix_source_score_sha256(&"a".repeat(64), 2).unwrap(),
        "0356937eddd2eed2e10152577044a5b68c0153f4c0522b130c210753b7c5e94c"
    );
    let registry = source_registry();
    let authority = population(&registry);
    assert!(select_v36_prefix_roles(rows.clone(), &authority, &registry).is_err());
    let mut outside = rows.clone();
    outside[0].selected_object_ordinal = u16::MAX;
    assert!(select_v36_prefix_roles(outside, &authority, &registry).is_err());

    let mut overlapping = roles.clone();
    overlapping[1].seed_sha256 = overlapping[0].seed_sha256.clone();
    assert!(validate_v36_prefix_role_authority(&overlapping).is_err());
    let mut relabeled = roles.clone();
    relabeled[0].seed_label.push_str("-drift");
    relabeled[0].seed_sha256 = format!("{:x}", Sha256::digest(relabeled[0].seed_label.as_bytes()));
    assert!(validate_v36_prefix_role_authority(&relabeled).is_err());
    let mut drifted = authority;
    drifted.ordered_source_manifest_sha256 = "A".repeat(64);
    assert!(select_v36_prefix_roles(rows, &drifted, &registry).is_err());
}

#[test]
fn v36_prefix_dataset_allows_complete_duplicate_only_objects_before_the_cutoff() {
    let rows = vec![
        validate_v36_prefix_input_row(&input(7, 0, 0)).unwrap(),
        validate_v36_prefix_input_row(&input(9, 2, 0)).unwrap(),
    ];
    validate_v36_prefix_cutoff_membership(&rows, 3, 2).unwrap();

    let mut outside = rows.clone();
    outside[0].selected_object_ordinal = 3;
    assert!(validate_v36_prefix_cutoff_membership(&outside, 3, 2).is_err());
    assert!(validate_v36_prefix_cutoff_membership(&rows, 3, 3).is_err());
    assert!(validate_v36_prefix_cutoff_membership(&rows, 2, 2).is_err());
}

fn execution_authority() -> V36PrefixFreezeExecutionAuthority {
    V36PrefixFreezeExecutionAuthority {
        active_wall_seconds: 43_200,
        attempt_id: "v36-prefix-screen-r01-attempt-0001".into(),
        checkpoint_seconds: 300,
        claim_eligible: false,
        inputs: [
            "binary",
            "freeze-authority",
            "source-archive",
            "source-registry",
        ]
        .into_iter()
        .enumerate()
        .map(|(ordinal, role)| V36ArtifactIdentity {
            blake3: format!("{:064x}", ordinal + 1),
            encoded_bytes: 1_024 + ordinal as u64,
            role: role.into(),
            sha256: format!("{:064x}", ordinal + 11),
            uri: format!("s3://fixture/v36/{role}"),
        })
        .collect(),
        output_prefix: "s3://fixture/v36/output/attempt-0001/".into(),
        resume: None,
        schema: "borsuk-v36-prefix-freeze-execution-authority-v2".into(),
        source_commit: "1".repeat(40),
    }
}

fn file_identity(role: &str, uri: &str, path: &Path) -> V36ArtifactIdentity {
    let bytes = fs::read(path).unwrap();
    V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes: bytes.len().try_into().unwrap(),
        role: role.into(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri: uri.into(),
    }
}

#[test]
fn v36_prefix_dataset_execution_authority_binds_provenance_and_lifecycle() {
    let authority = execution_authority();
    validate_v36_prefix_freeze_execution_authority(&authority).unwrap();
    let bytes = canonical_v36_prefix_freeze_execution_authority_bytes(&authority).unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));

    let mut drifted = authority.clone();
    drifted.inputs[0].role = "source-registry".into();
    assert!(validate_v36_prefix_freeze_execution_authority(&drifted).is_err());
    let mut drifted = authority.clone();
    drifted.inputs[1].uri = drifted.inputs[0].uri.clone();
    assert!(validate_v36_prefix_freeze_execution_authority(&drifted).is_err());
    let mut drifted = authority.clone();
    drifted.active_wall_seconds += 1;
    assert!(validate_v36_prefix_freeze_execution_authority(&drifted).is_err());
    let mut shortened = authority.clone();
    shortened.active_wall_seconds = 21_600;
    validate_v36_prefix_freeze_execution_authority(&shortened).unwrap();
    shortened.active_wall_seconds = 0;
    assert!(validate_v36_prefix_freeze_execution_authority(&shortened).is_err());
    let mut drifted = authority;
    drifted.output_prefix.pop();
    assert!(validate_v36_prefix_freeze_execution_authority(&drifted).is_err());

    let mut resumed = execution_authority();
    resumed.resume = Some(V36PrefixResumeBinding {
        generation: 7,
        manifest: V36ArtifactIdentity {
            blake3: "b".repeat(64),
            encoded_bytes: 2_048,
            role: "checkpoint-manifest".into(),
            sha256: "a".repeat(64),
            uri: format!(
                "s3://fixture/v36/checkpoints/objects/{}-checkpoint-00000007.json",
                "a".repeat(64)
            ),
        },
        pointer_encoded_bytes: 1_024,
        pointer_sha256: "c".repeat(64),
        pointer_uri: "s3://fixture/v36/checkpoints/runs/v36-prefix-screen-r01/latest.json".into(),
    });
    validate_v36_prefix_freeze_execution_authority(&resumed).unwrap();
    resumed.resume.as_mut().unwrap().manifest.role = "source".into();
    assert!(validate_v36_prefix_freeze_execution_authority(&resumed).is_err());
}

#[test]
fn v36_prefix_dataset_receipt_binds_population_counters_and_all_outputs() {
    let registry = source_registry();
    let authority = freeze_authority(&registry);
    let execution = execution_authority();
    let population = population(&registry);
    let outputs = [
        ("population-authority", "population-authority.json"),
        ("source", "source.parquet"),
        ("development-query", "development-query.parquet"),
        ("development-gt100", "development-gt100.parquet"),
        ("validation-query", "validation-query.parquet"),
        ("validation-gt100", "validation-gt100.parquet"),
        ("sealed-holdout-query", "sealed-holdout-query.parquet"),
        ("sealed-holdout-gt100", "sealed-holdout-gt100.parquet"),
        ("performance-query", "performance-query.parquet"),
    ]
    .into_iter()
    .enumerate()
    .map(|(ordinal, (role, filename))| V36ArtifactIdentity {
        blake3: format!("{:064x}", ordinal + 31),
        encoded_bytes: 4_096 + ordinal as u64,
        role: role.into(),
        sha256: format!("{:064x}", ordinal + 41),
        uri: format!("{}{filename}", execution.output_prefix),
    })
    .collect();
    let receipt = V36PrefixFreezeReceipt {
        claim_eligible: false,
        cutoff_object_ordinal: 1,
        cutoff_row_offset: 99,
        distinct_rows_observed: 1_100_000,
        duplicate_rows: 100_000,
        execution_authority_sha256: format!(
            "{:x}",
            Sha256::digest(
                canonical_v36_prefix_freeze_execution_authority_bytes(&execution).unwrap()
            )
        ),
        freeze_authority_sha256: format!(
            "{:x}",
            Sha256::digest(
                canonical_v36_prefix_freeze_authority_bytes(&authority, &registry).unwrap()
            )
        ),
        outputs,
        physical_rows: 1_200_000,
        population,
        schema: "borsuk-v36-prefix-freeze-receipt-v1".into(),
        source_archive_sha256: execution.inputs[2].sha256.clone(),
        source_registry_sha256: format!(
            "{:x}",
            Sha256::digest(
                canonical_v36_prefix_source_registry_bytes(&authority, &registry).unwrap()
            )
        ),
    };
    validate_v36_prefix_freeze_receipt(&receipt, &authority, &execution, &registry).unwrap();
    let bytes =
        canonical_v36_prefix_freeze_receipt_bytes(&receipt, &authority, &execution, &registry)
            .unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));

    let mut mutations = Vec::new();
    let mut changed = receipt.clone();
    changed.duplicate_rows -= 1;
    mutations.push(changed);
    let mut changed = receipt.clone();
    changed.outputs.swap(0, 1);
    mutations.push(changed);
    let mut changed = receipt.clone();
    let first_uri = changed.outputs[0].uri.clone();
    changed.outputs[0].uri = changed.outputs[1].uri.clone();
    changed.outputs[1].uri = first_uri;
    mutations.push(changed);
    let mut changed = receipt.clone();
    changed.outputs[0].uri = execution.inputs[0].uri.clone();
    mutations.push(changed);
    let mut changed = receipt.clone();
    changed.execution_authority_sha256 = "f".repeat(64);
    mutations.push(changed);
    for mutation in mutations {
        assert!(
            validate_v36_prefix_freeze_receipt(&mutation, &authority, &execution, &registry)
                .is_err()
        );
    }
}

#[test]
fn v36_prefix_dataset_population_authority_has_canonical_bytes() {
    let registry = source_registry();
    let population = population(&registry);
    let bytes = canonical_v36_prefix_population_authority_bytes(&population, &registry).unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));
    assert_eq!(
        serde_json::from_slice::<V36PrefixPopulationAuthority>(&bytes).unwrap(),
        population
    );
}

#[test]
fn v36_prefix_dataset_preflight_authenticates_every_local_input_before_network() {
    let directory = tempfile::tempdir().unwrap();
    let authority_path = directory.path().join("authority.json");
    let execution_path = directory.path().join("execution.json");
    let registry_path = directory.path().join("registry.json");
    let archive_path = directory.path().join("source.tar.zst");
    let binary_path = directory.path().join("v36_prefix_freeze");
    let output = directory.path().join("output");
    let scratch = directory.path().join("scratch");
    fs::create_dir(&output).unwrap();
    fs::create_dir(&scratch).unwrap();
    fs::write(&archive_path, b"source archive evidence").unwrap();
    fs::write(&binary_path, b"freezer executable evidence").unwrap();

    let registry = source_registry();
    let authority = freeze_authority(&registry);
    fs::write(
        &authority_path,
        canonical_v36_prefix_freeze_authority_bytes(&authority, &registry).unwrap(),
    )
    .unwrap();
    fs::write(
        &registry_path,
        canonical_v36_prefix_source_registry_bytes(&authority, &registry).unwrap(),
    )
    .unwrap();
    let mut execution = execution_authority();
    execution.inputs = [
        ("binary", "s3://fixture/v36/binary", binary_path.as_path()),
        (
            "freeze-authority",
            "s3://fixture/v36/authority",
            authority_path.as_path(),
        ),
        (
            "source-archive",
            "s3://fixture/v36/archive",
            archive_path.as_path(),
        ),
        (
            "source-registry",
            "s3://fixture/v36/registry",
            registry_path.as_path(),
        ),
    ]
    .into_iter()
    .map(|(role, uri, path)| file_identity(role, uri, path))
    .collect();
    fs::write(
        &execution_path,
        canonical_v36_prefix_freeze_execution_authority_bytes(&execution).unwrap(),
    )
    .unwrap();
    let request = V36PrefixFreezeRequest {
        authority: authority_path,
        checkpoint_outbox: directory.path().join("outbox"),
        executable: binary_path,
        execution_authority: execution_path,
        output,
        producer_instance_id: "i-fixture".into(),
        scratch,
        source_archive: archive_path.clone(),
        source_registry: registry_path,
    };
    fs::create_dir(&request.checkpoint_outbox).unwrap();
    let preflight = load_v36_prefix_freeze_preflight(&request).unwrap();
    assert_eq!(preflight.ranked_objects.len(), registry.len());
    assert_eq!(preflight.authority.object_cap, 16);
    assert_eq!(preflight.producer_attempt_ordinal, 1);
    assert_eq!(preflight.checkpoint_context.run_id, "v36-prefix-screen-r01");
    assert_eq!(
        preflight.checkpoint_context.object_prefix,
        "s3://fixture/v36/output/checkpoints/objects/"
    );
    assert_eq!(
        preflight.checkpoint_context.pointer_uri,
        "s3://fixture/v36/output/checkpoints/runs/v36-prefix-screen-r01/latest.json"
    );
    assert_eq!(
        preflight.checkpoint_context.ranked_objects,
        preflight
            .ranked_objects
            .iter()
            .map(|object| V36PrefixRegisteredSourceObject {
                encoded_bytes: object.encoded_bytes,
                path: object.path.clone(),
                sha256: object.sha256.clone(),
                uri: object.uri.clone(),
            })
            .collect::<Vec<_>>()
    );

    fs::write(&archive_path, b"mutated archive evidence").unwrap();
    assert!(load_v36_prefix_freeze_preflight(&request).is_err());
}

#[test]
fn v36_prefix_dataset_parquet_and_exact_truth_are_closed() {
    let child = || Field::new("item", DataType::Float32, false).into();
    assert_eq!(
        v36_prefix_source_schema(),
        Schema::new(vec![
            Field::new("feature_row_id", DataType::UInt64, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(child(), DIMENSIONS as i32),
                false,
            ),
        ])
    );
    assert_eq!(
        v36_prefix_query_schema(),
        Schema::new(vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new("feature_row_id", DataType::UInt64, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(child(), DIMENSIONS as i32),
                false,
            ),
        ])
    );
    assert_eq!(
        v36_prefix_gt100_schema(),
        Schema::new(vec![
            Field::new("query_ordinal", DataType::UInt32, false),
            Field::new("rank", DataType::UInt16, false),
            Field::new("feature_row_id", DataType::UInt64, false),
            Field::new("squared_distance", DataType::Float64, false),
        ])
    );

    let corpus = (0_u64..101)
        .map(|ordinal| borsuk::V36PrefixMaterializedRow {
            feature_row_id: if ordinal == 99 {
                999_999
            } else if ordinal == 100 {
                999_998
            } else {
                1_000 + ordinal
            },
            source_ordinal: Some(ordinal),
            embedding: vector(1, ordinal.min(99) as f32 / 100.0),
        })
        .collect::<Vec<_>>();
    let query = borsuk::V36PrefixQueryRow {
        query_ordinal: 0,
        feature_row_id: 77,
        embedding: vector(1, 0.0),
    };
    let truth =
        exact_v36_prefix_gt100(V36PrefixQualityRole::Development, &corpus, &[query]).unwrap();
    let mut streamed = V36PrefixGtAccumulator::new(
        V36PrefixQualityRole::Development,
        vec![borsuk::V36PrefixQueryRow {
            query_ordinal: 0,
            feature_row_id: 77,
            embedding: vector(1, 0.0),
        }],
    )
    .unwrap();
    streamed.absorb(&corpus[..37]).unwrap();
    streamed.absorb(&corpus[37..]).unwrap();
    assert_eq!(streamed.finish().unwrap(), truth);
    assert_eq!(truth.len(), 100);
    assert!(truth.iter().enumerate().all(|(rank, neighbor)| {
        neighbor.query_ordinal == 0
            && neighbor.rank == rank as u16
            && neighbor.squared_distance.is_finite()
    }));
    assert_eq!(truth[0].squared_distance, 0.0);
    for (rank, neighbor) in truth.iter().take(99).enumerate() {
        let delta = f64::from(rank as f32 / 100.0);
        assert_eq!(neighbor.feature_row_id, 1_000 + rank as u64);
        assert_eq!(
            neighbor.squared_distance.to_bits(),
            (delta * delta).to_bits()
        );
    }
    assert_eq!(truth[99].feature_row_id, 999_998);
    let tied_delta = f64::from(0.99_f32);
    assert_eq!(
        truth[99].squared_distance.to_bits(),
        (tied_delta * tied_delta).to_bits()
    );
    assert!(truth.windows(2).all(|pair| {
        pair[0]
            .squared_distance
            .total_cmp(&pair[1].squared_distance)
            .then(pair[0].feature_row_id.cmp(&pair[1].feature_row_id))
            .is_le()
    }));
}

#[test]
fn v36_prefix_dataset_writes_all_quality_truth_with_one_corpus_scan() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.parquet");
    let child = Arc::new(Field::new("item", DataType::Float32, false));
    let feature_ids = (1_000_u64..1_101).collect::<Vec<_>>();
    let source_embeddings = FixedSizeListArray::try_new(
        child.clone(),
        DIMENSIONS as i32,
        Arc::new(Float32Array::from(
            (0_u64..101)
                .flat_map(|ordinal| vector(1, ordinal as f32 / 100.0))
                .collect::<Vec<_>>(),
        )),
        None,
    )
    .unwrap();
    let source_batch = RecordBatch::try_new(
        Arc::new(v36_prefix_source_schema()),
        vec![
            Arc::new(UInt64Array::from(feature_ids.clone())) as ArrayRef,
            Arc::new(source_embeddings),
        ],
    )
    .unwrap();
    write_v36_prefix_source_parquet(&source_path, &feature_ids, [source_batch]).unwrap();

    let definitions = [
        (
            V36PrefixQualityRole::Development,
            "development",
            70_u64,
            0.0_f32,
        ),
        (V36PrefixQualityRole::Validation, "validation", 71, 0.25),
        (
            V36PrefixQualityRole::SealedHoldout,
            "sealed-holdout",
            72,
            0.5,
        ),
    ];
    let query_paths = definitions
        .iter()
        .map(|(_, name, feature_row_id, value)| {
            let path = directory.path().join(format!("{name}-query.parquet"));
            let embeddings = FixedSizeListArray::try_new(
                child.clone(),
                DIMENSIONS as i32,
                Arc::new(Float32Array::from(vector(1, *value))),
                None,
            )
            .unwrap();
            let batch = RecordBatch::try_new(
                Arc::new(v36_prefix_query_schema()),
                vec![
                    Arc::new(UInt32Array::from(vec![0])) as ArrayRef,
                    Arc::new(UInt64Array::from(vec![*feature_row_id])),
                    Arc::new(embeddings),
                ],
            )
            .unwrap();
            write_v36_prefix_query_parquet(&path, [batch]).unwrap();
            path
        })
        .collect::<Vec<_>>();

    let run = |suffix: &str, workers| {
        let jobs = definitions
            .iter()
            .zip(&query_paths)
            .map(|((role, name, _, _), query)| V36PrefixGtParquetJob {
                expected_queries: 1,
                output: directory.path().join(format!("{name}-gt-{suffix}.parquet")),
                query: query.clone(),
                role: *role,
            })
            .collect::<Vec<_>>();
        let stats =
            write_v36_prefix_gt100_roles_from_parquets(&source_path, &feature_ids, &jobs, workers)
                .unwrap();
        (jobs, stats)
    };
    let (single_jobs, single) = run("single", 1);
    let (parallel_jobs, parallel) = run("parallel", 2);
    assert_eq!(single.source_scans, 1);
    assert_eq!(single.source_rows, 101);
    assert_eq!(single.quality_queries, 3);
    assert_eq!(single, parallel);
    for (single, parallel) in single_jobs.iter().zip(parallel_jobs) {
        assert_eq!(
            fs::read(&single.output).unwrap(),
            fs::read(parallel.output).unwrap()
        );
        scan_v36_prefix_gt100_parquet(&single.output, 1, |_| Ok(())).unwrap();
    }
    let mut overlapping = single_jobs;
    overlapping[0].output = source_path.clone();
    assert!(
        write_v36_prefix_gt100_roles_from_parquets(&source_path, &feature_ids, &overlapping, 1,)
            .is_err()
    );
}

fn source_batch(child_nullable: bool, zero_second_row: bool) -> RecordBatch {
    let child = Arc::new(Field::new("item", DataType::Float32, child_nullable));
    let mut values = vec![0.0_f32; 2 * DIMENSIONS];
    values[0] = 1.0;
    if !zero_second_row {
        values[DIMENSIONS + 1] = 1.0;
    }
    let embeddings = FixedSizeListArray::try_new(
        child.clone(),
        DIMENSIONS as i32,
        Arc::new(Float32Array::from(values)),
        None,
    )
    .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("feature_row_id", DataType::UInt64, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(child, DIMENSIONS as i32),
            false,
        ),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt64Array::from(vec![7, 9])) as ArrayRef,
            Arc::new(embeddings),
        ],
    )
    .unwrap()
}

#[test]
fn v36_prefix_dataset_source_parquet_round_trip_is_streamed_and_strict() {
    let directory = tempfile::tempdir().unwrap();
    let valid = directory.path().join("source.parquet");
    write_v36_prefix_source_parquet(&valid, &[7, 9], [source_batch(false, false)]).unwrap();
    let mut rows = 0_usize;
    scan_v36_prefix_source_parquet(&valid, &[7, 9], |batch| {
        rows += batch.num_rows();
        Ok(())
    })
    .unwrap();
    assert_eq!(rows, 2);
    let original = std::fs::read(&valid).unwrap();
    assert!(scan_v36_prefix_source_parquet(&valid, &[7, 9, 11], |_| Ok(())).is_err());
    assert!(scan_v36_prefix_source_parquet(&valid, &[9, 7], |_| Ok(())).is_err());
    assert!(write_v36_prefix_source_parquet(&valid, &[7, 9], [source_batch(false, true)]).is_err());
    assert_eq!(std::fs::read(&valid).unwrap(), original);

    let wrong_child = directory.path().join("wrong-child.parquet");
    let file = std::fs::File::create(&wrong_child).unwrap();
    let batch = source_batch(true, false);
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    assert!(scan_v36_prefix_source_parquet(&wrong_child, &[7, 9], |_| Ok(())).is_err());

    let zero = directory.path().join("zero.parquet");
    let file = std::fs::File::create(&zero).unwrap();
    let batch = source_batch(false, true);
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    assert!(scan_v36_prefix_source_parquet(&zero, &[7, 9], |_| Ok(())).is_err());
}

#[test]
fn v36_prefix_dataset_query_and_gt_parquet_round_trip_is_strict() {
    let directory = tempfile::tempdir().unwrap();
    let child = Arc::new(Field::new("item", DataType::Float32, false));
    let mut values = vec![0.0_f32; 2 * DIMENSIONS];
    values[0] = 1.0;
    values[DIMENSIONS + 1] = 1.0;
    let embeddings = FixedSizeListArray::try_new(
        child.clone(),
        DIMENSIONS as i32,
        Arc::new(Float32Array::from(values)),
        None,
    )
    .unwrap();
    let query_batch = RecordBatch::try_new(
        Arc::new(v36_prefix_query_schema()),
        vec![
            Arc::new(UInt32Array::from(vec![0, 1])) as ArrayRef,
            Arc::new(UInt64Array::from(vec![70, 90])),
            Arc::new(embeddings),
        ],
    )
    .unwrap();
    let query_path = directory.path().join("query.parquet");
    write_v36_prefix_query_parquet(&query_path, [query_batch.clone()]).unwrap();
    let mut query_rows = 0;
    scan_v36_prefix_query_parquet(&query_path, 2, |batch| {
        query_rows += batch.num_rows();
        Ok(())
    })
    .unwrap();
    assert_eq!(query_rows, 2);
    let mut bad_query = query_batch;
    bad_query = RecordBatch::try_new(
        bad_query.schema(),
        vec![
            Arc::new(UInt32Array::from(vec![0, 2])) as ArrayRef,
            bad_query.column(1).clone(),
            bad_query.column(2).clone(),
        ],
    )
    .unwrap();
    assert!(write_v36_prefix_query_parquet(&query_path, [bad_query]).is_err());

    let query_ordinals = (0_u32..2)
        .flat_map(|ordinal| std::iter::repeat_n(ordinal, 100))
        .collect::<Vec<_>>();
    let ranks = (0_u16..100).chain(0_u16..100).collect::<Vec<_>>();
    let feature_ids = (1_000_u64..1_200).collect::<Vec<_>>();
    let distances = (0_u64..200).map(|value| value as f64).collect::<Vec<_>>();
    let gt_batch = RecordBatch::try_new(
        Arc::new(v36_prefix_gt100_schema()),
        vec![
            Arc::new(UInt32Array::from(query_ordinals)) as ArrayRef,
            Arc::new(UInt16Array::from(ranks)),
            Arc::new(UInt64Array::from(feature_ids)),
            Arc::new(Float64Array::from(distances)),
        ],
    )
    .unwrap();
    let gt_path = directory.path().join("gt.parquet");
    write_v36_prefix_gt100_parquet(&gt_path, [gt_batch.clone()]).unwrap();
    let mut gt_rows = 0;
    scan_v36_prefix_gt100_parquet(&gt_path, 2, |batch| {
        gt_rows += batch.num_rows();
        Ok(())
    })
    .unwrap();
    assert_eq!(gt_rows, 200);
    let mut bad_distances = (0_u64..200).map(|value| value as f64).collect::<Vec<_>>();
    bad_distances[99] = f64::NAN;
    let bad_gt = RecordBatch::try_new(
        gt_batch.schema(),
        vec![
            gt_batch.column(0).clone(),
            gt_batch.column(1).clone(),
            gt_batch.column(2).clone(),
            Arc::new(Float64Array::from(bad_distances)),
        ],
    )
    .unwrap();
    assert!(write_v36_prefix_gt100_parquet(&gt_path, [bad_gt]).is_err());
}

#[test]
fn v36_prefix_dataset_registered_input_is_authenticated_and_strict() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("registered.parquet");
    let child = Arc::new(Field::new("item", DataType::Float32, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8, true),
        Field::new("natural_score", DataType::Float32, true),
        Field::new("feature_row_id", DataType::Int64, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(child.clone(), DIMENSIONS as i32),
            true,
        ),
    ]));
    let mut values = vec![0.0_f32; 2 * DIMENSIONS];
    values[0] = 1.0;
    values[DIMENSIONS + 1] = 1.0;
    let embeddings = FixedSizeListArray::try_new(
        child,
        DIMENSIONS as i32,
        Arc::new(Float32Array::from(values)),
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                None,
                Some("https://example.invalid"),
            ])) as ArrayRef,
            Arc::new(Float32Array::from(vec![None, Some(0.5)])),
            Arc::new(Int64Array::from(vec![Some(7), Some(9)])),
            Arc::new(embeddings),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let object = V36PrefixRankedSourceObject {
        encoded_bytes: bytes.len() as u64,
        path: "data/registered.parquet".into(),
        sample_sha256: sample_digest("data/registered.parquet", bytes.len() as u64),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri: "https://example.invalid/registered.parquet".into(),
    };
    let mut observed = Vec::new();
    assert_eq!(
        scan_v36_prefix_registered_input_parquet(&path, &object, 3, |row| {
            observed.push((
                row.feature_row_id,
                row.selected_object_ordinal,
                row.row_offset,
            ));
            Ok(())
        })
        .unwrap(),
        2
    );
    assert_eq!(observed, [(7, 3, 0), (9, 3, 1)]);
    let mut drifted = object.clone();
    drifted.sha256 = "4".repeat(64);
    assert!(scan_v36_prefix_registered_input_parquet(&path, &drifted, 3, |_| Ok(())).is_err());

    let null_path = directory.path().join("null-id.parquet");
    let null_batch = RecordBatch::try_new(
        batch.schema(),
        vec![
            batch.column(0).clone(),
            batch.column(1).clone(),
            Arc::new(Int64Array::from(vec![Some(7), None])),
            batch.column(3).clone(),
        ],
    )
    .unwrap();
    let file = std::fs::File::create(&null_path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, null_batch.schema(), None).unwrap();
    writer.write(&null_batch).unwrap();
    writer.close().unwrap();
    let null_bytes = std::fs::read(&null_path).unwrap();
    let mut null_object = object;
    null_object.encoded_bytes = null_bytes.len() as u64;
    null_object.sha256 = format!("{:x}", Sha256::digest(&null_bytes));
    assert!(
        scan_v36_prefix_registered_input_parquet(&null_path, &null_object, 3, |_| Ok(())).is_err()
    );
}

fn write_registered_rows(path: &Path, feature_ids: &[i64]) -> V36PrefixRankedSourceObject {
    let child = Arc::new(Field::new("item", DataType::Float32, true));
    let schema = Arc::new(Schema::new(vec![
        Field::new("url", DataType::Utf8, true),
        Field::new("natural_score", DataType::Float32, true),
        Field::new("feature_row_id", DataType::Int64, true),
        Field::new(
            "embedding",
            DataType::FixedSizeList(child.clone(), DIMENSIONS as i32),
            true,
        ),
    ]));
    let mut values = vec![0.0_f32; feature_ids.len() * DIMENSIONS];
    for row in 0..feature_ids.len() {
        values[row * DIMENSIONS + row % DIMENSIONS] = 1.0;
    }
    let embeddings = FixedSizeListArray::try_new(
        child,
        DIMENSIONS as i32,
        Arc::new(Float32Array::from(values)),
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![None::<&str>; feature_ids.len()])) as ArrayRef,
            Arc::new(Float32Array::from(vec![None; feature_ids.len()])),
            Arc::new(Int64Array::from(
                feature_ids.iter().copied().map(Some).collect::<Vec<_>>(),
            )),
            Arc::new(embeddings),
        ],
    )
    .unwrap();
    let file = fs::File::create(path).unwrap();
    let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = fs::read(path).unwrap();
    let name = path.file_name().unwrap().to_str().unwrap();
    V36PrefixRankedSourceObject {
        encoded_bytes: bytes.len().try_into().unwrap(),
        path: format!("data/{name}"),
        sample_sha256: sample_digest(&format!("data/{name}"), bytes.len() as u64),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri: format!("https://example.invalid/{name}"),
    }
}

#[test]
fn v36_prefix_dataset_scans_complete_cutoff_object_and_records_duplicate_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.parquet");
    let second = directory.path().join("second.parquet");
    let ranked = vec![
        write_registered_rows(&first, &[7, 9]),
        write_registered_rows(&second, &[7, 11, 13]),
    ];
    let paths = [first, second];
    let scan = scan_v36_prefix_object_prefix(
        &ranked,
        2,
        ranked.iter().map(|object| object.encoded_bytes).sum(),
        3,
        |ordinal, _| Ok(paths[ordinal].clone()),
    )
    .unwrap();
    assert_eq!(scan.unique_rows.len(), 3);
    assert_eq!(scan.consumed_objects.len(), 2);
    assert_eq!(scan.cutoff_object_ordinal, 1);
    assert_eq!(scan.cutoff_row_offset, 1);
    assert_eq!(scan.physical_rows, 5);
    assert_eq!(scan.distinct_rows_observed, 4);
    assert_eq!(scan.duplicate_rows, 1);

    assert!(
        scan_v36_prefix_object_prefix(&ranked, 1, u64::MAX, 3, |ordinal, _| {
            Ok(paths[ordinal].clone())
        })
        .is_err()
    );
}

#[test]
fn v36_prefix_dataset_checkpoints_only_complete_authenticated_objects() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.parquet");
    let second = directory.path().join("second.parquet");
    let ranked = vec![
        write_registered_rows(&first, &[7, 9]),
        write_registered_rows(&second, &[7, 11, 13]),
    ];
    let paths = [first, second];
    let mut commits = Vec::<V36PrefixPopulationCommit>::new();
    let scan = scan_v36_prefix_object_prefix_checkpointed(
        &ranked,
        2,
        ranked.iter().map(|object| object.encoded_bytes).sum(),
        3,
        |ordinal, _| Ok(paths[ordinal].clone()),
        |commit| {
            commits.push(commit.clone());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(commits.len(), 2);
    assert_eq!(commits[0].run.rows.len(), 2);
    assert_eq!(commits[1].run.rows.len(), 2);
    assert_eq!(commits[1].distinct_rows, 4);
    assert_eq!(commits[1].cutoff, Some((1, 1)));
    assert_eq!(
        restore_v36_prefix_population(
            &commits
                .iter()
                .map(|commit| commit.run.clone())
                .collect::<Vec<_>>(),
            3,
        )
        .unwrap(),
        scan
    );

    let invalid = directory.path().join("invalid.parquet");
    let invalid_ranked = vec![ranked[0].clone(), write_registered_rows(&invalid, &[7, -1])];
    let invalid_paths = [paths[0].clone(), invalid];
    let mut committed_ordinals = Vec::new();
    assert!(
        scan_v36_prefix_object_prefix_checkpointed(
            &invalid_ranked,
            2,
            u64::MAX,
            3,
            |ordinal, _| Ok(invalid_paths[ordinal].clone()),
            |commit| {
                committed_ordinals.push(commit.run.selected_object_ordinal);
                Ok(())
            },
        )
        .is_err()
    );
    assert_eq!(committed_ordinals, vec![0]);
}

#[test]
fn v36_prefix_dataset_resume_matches_uninterrupted_complete_object_scan() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.parquet");
    let second = directory.path().join("second.parquet");
    let third = directory.path().join("third.parquet");
    let ranked = vec![
        write_registered_rows(&first, &[7, 9]),
        write_registered_rows(&second, &[7, 11, 13]),
        write_registered_rows(&third, &[15]),
    ];
    let paths = [first, second, third];
    let mut uninterrupted_commits = Vec::new();
    let uninterrupted = scan_v36_prefix_object_prefix_checkpointed(
        &ranked,
        3,
        u64::MAX,
        3,
        |ordinal, _| Ok(paths[ordinal].clone()),
        |commit| {
            uninterrupted_commits.push(commit.clone());
            Ok(())
        },
    )
    .unwrap();

    let mut first_commits = Vec::new();
    assert!(
        scan_v36_prefix_object_prefix_checkpointed(
            &ranked,
            1,
            u64::MAX,
            3,
            |ordinal, _| Ok(paths[ordinal].clone()),
            |commit| {
                first_commits.push(commit.clone());
                Ok(())
            },
        )
        .is_err()
    );
    let prior_runs = first_commits
        .iter()
        .map(|commit| commit.run.clone())
        .collect::<Vec<_>>();
    let mut acquired = Vec::new();
    let resumed = scan_v36_prefix_object_prefix_resumed(
        &ranked,
        3,
        u64::MAX,
        3,
        &prior_runs,
        |ordinal, _| {
            acquired.push(ordinal);
            Ok(paths[ordinal].clone())
        },
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(acquired, vec![1]);
    assert_eq!(resumed, uninterrupted);

    let complete_runs = uninterrupted_commits
        .iter()
        .map(|commit| commit.run.clone())
        .collect::<Vec<_>>();
    let mut acquired_after_cutoff = Vec::new();
    let resumed_complete = scan_v36_prefix_object_prefix_resumed(
        &ranked,
        3,
        u64::MAX,
        3,
        &complete_runs,
        |ordinal, _| {
            acquired_after_cutoff.push(ordinal);
            Ok(paths[ordinal].clone())
        },
        |_| Ok(()),
    )
    .unwrap();
    assert!(acquired_after_cutoff.is_empty());
    assert_eq!(resumed_complete, uninterrupted);
}

fn identity(
    feature_row_id: u64,
    object: u16,
    row: u64,
    source: Option<u64>,
) -> borsuk::V36PrefixRowIdentity {
    borsuk::V36PrefixRowIdentity {
        feature_row_id,
        row_offset: row,
        selected_object_ordinal: object,
        source_ordinal: source,
    }
}

#[test]
fn v36_prefix_dataset_materializes_canonical_roles_with_bounded_spools() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.parquet");
    let second = directory.path().join("second.parquet");
    let ranked = vec![
        write_registered_rows(&first, &[7, 9, 11]),
        write_registered_rows(&second, &[13, 15, 17]),
    ];
    let split = borsuk::V36PrefixRoleSplit {
        corpus: vec![identity(17, 1, 2, Some(0)), identity(7, 0, 0, Some(1))],
        development: vec![identity(9, 0, 1, None)],
        validation: vec![identity(11, 0, 2, None)],
        sealed_holdout: vec![identity(13, 1, 0, None)],
        performance: vec![identity(15, 1, 1, None)],
    };
    let output = directory.path().join("output");
    let scratch = directory.path().join("scratch");
    fs::create_dir(&output).unwrap();
    fs::create_dir(&scratch).unwrap();
    let paths =
        materialize_v36_prefix_role_parquets(&[first, second], &ranked, &split, &scratch, &output)
            .unwrap();

    let mut source_rows = 0;
    scan_v36_prefix_source_parquet(&paths.source, &[17, 7], |batch| {
        source_rows += batch.num_rows();
        Ok(())
    })
    .unwrap();
    assert_eq!(source_rows, 2);
    for query in [
        paths.development,
        paths.validation,
        paths.sealed_holdout,
        paths.performance,
    ] {
        let mut rows = 0;
        scan_v36_prefix_query_parquet(&query, 1, |batch| {
            rows += batch.num_rows();
            Ok(())
        })
        .unwrap();
        assert_eq!(rows, 1);
    }
    assert!(scratch.read_dir().unwrap().next().is_none());
}
