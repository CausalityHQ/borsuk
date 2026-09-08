//! Contract tests for the bounded V36 diagnostic population and exact truth.

use std::{fs, path::Path, sync::Arc};

use arrow_array::{
    Array, ArrayRef, FixedSizeBinaryArray, FixedSizeListArray, Float32Array, Float64Array,
    Int64Array, RecordBatch, StringArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_ipc::{
    MetadataVersion,
    reader::FileReader as ArrowFileReader,
    writer::{FileWriter as ArrowFileWriter, IpcWriteOptions},
};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    BorsukError, V36ArtifactIdentity, V36PrefixAllQueryGtAccumulator, V36PrefixCheckpointContext,
    V36PrefixCheckpointDependencyFile, V36PrefixCheckpointPointer, V36PrefixCheckpointResumeState,
    V36PrefixExternalIdentityRunRequest, V36PrefixExternalMaterializationRequest,
    V36PrefixExternalSelectionAuthority, V36PrefixExternalSelectionLimits,
    V36PrefixExternalSelectionRequest, V36PrefixFileBackedResumeRequest,
    V36PrefixFileBackedScanRequest, V36PrefixFreezeAuthority, V36PrefixFreezeExecutionAuthority,
    V36PrefixFreezeReceipt, V36PrefixFreezeRequest, V36PrefixGtAccumulator,
    V36PrefixGtHeapCheckpoint, V36PrefixGtHeapEntry, V36PrefixGtParquetJob, V36PrefixIdentityRun,
    V36PrefixIdentityRunFile, V36PrefixInputRow, V36PrefixMaterializedArtifacts,
    V36PrefixPhaseResumeRequest, V36PrefixPopulationAuthority, V36PrefixPopulationCheckpointWriter,
    V36PrefixPopulationCommit, V36PrefixPopulationSelection, V36PrefixQualityRole,
    V36PrefixRankedSourceObject, V36PrefixRegisteredSourceObject, V36PrefixResumeBinding,
    V36PrefixRoleAssignmentContract, V36PrefixRoleAssignmentFile, V36PrefixRoleAssignmentRequest,
    V36PrefixRoleAuthority, V36PrefixSelectedIdsContract, V36PrefixSelectedIdsFile,
    V36PrefixSourceObject, assign_v36_prefix_roles_from_selected_file,
    bind_v36_prefix_population_authority, canonical_v36_prefix_checkpoint_manifest_bytes,
    canonical_v36_prefix_checkpoint_pointer_bytes, canonical_v36_prefix_freeze_authority_bytes,
    canonical_v36_prefix_freeze_execution_authority_bytes,
    canonical_v36_prefix_freeze_receipt_bytes, canonical_v36_prefix_population_authority_bytes,
    canonical_v36_prefix_source_registry_bytes, decode_v36_prefix_gt_heap_checkpoint,
    decode_v36_prefix_identity_run, decode_v36_prefix_selected_ids,
    deduplicate_v36_prefix_row_identities, encode_v36_prefix_gt_heap_checkpoint,
    encode_v36_prefix_identity_run, encode_v36_prefix_selected_ids, exact_v36_prefix_gt100,
    externally_build_v36_prefix_identity_run, externally_select_v36_prefix_population_rows,
    load_v36_prefix_checkpoint_head, load_v36_prefix_freeze_preflight,
    materialize_v36_prefix_assigned_roles, materialize_v36_prefix_role_parquets,
    rank_v36_prefix_source_objects, restore_v36_prefix_checkpoint_phase,
    restore_v36_prefix_file_backed_population_scan, restore_v36_prefix_population,
    run_v36_prefix_gt100_checkpointed, scan_v36_prefix_gt100_parquet,
    scan_v36_prefix_object_prefix, scan_v36_prefix_object_prefix_checkpointed,
    scan_v36_prefix_object_prefix_file_backed, scan_v36_prefix_object_prefix_resumed,
    scan_v36_prefix_query_parquet, scan_v36_prefix_registered_input_parquet,
    scan_v36_prefix_source_parquet, select_v36_prefix_population_rows, select_v36_prefix_roles,
    v36_prefix_gt100_schema, v36_prefix_query_schema, v36_prefix_query_score_sha256,
    v36_prefix_source_schema, v36_prefix_source_score_sha256,
    validate_v36_prefix_cutoff_membership, validate_v36_prefix_freeze_authority,
    validate_v36_prefix_freeze_execution_authority, validate_v36_prefix_freeze_receipt,
    validate_v36_prefix_input_row, validate_v36_prefix_registered_screen_authority,
    validate_v36_prefix_role_authority, write_v36_prefix_gt100_parquet,
    write_v36_prefix_gt100_roles_from_parquets, write_v36_prefix_query_parquet,
    write_v36_prefix_source_parquet,
};
use sha2::{Digest, Sha256};

const DIMENSIONS: usize = 768;

#[test]
fn v36_prefix_dataset_accepts_python_derived_frozen_inputs() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/research");
    let authority_bytes = fs::read(root.join("v36-prefix-screen-authority.json")).unwrap();
    let registry_bytes = fs::read(root.join("v36-prefix-source-registry.json")).unwrap();
    let authority: V36PrefixFreezeAuthority = serde_json::from_slice(&authority_bytes).unwrap();
    let registry: Vec<V36PrefixRegisteredSourceObject> =
        serde_json::from_slice(&registry_bytes).unwrap();

    validate_v36_prefix_freeze_authority(&authority, &registry).unwrap();
    validate_v36_prefix_registered_screen_authority(&authority, &registry).unwrap();
    assert_eq!(
        canonical_v36_prefix_freeze_authority_bytes(&authority, &registry).unwrap(),
        authority_bytes
    );
    assert_eq!(
        canonical_v36_prefix_source_registry_bytes(&authority, &registry).unwrap(),
        registry_bytes
    );
}

#[test]
fn v36_prefix_dataset_registered_screen_rejects_smaller_or_future_windows() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/research");
    let authority_bytes = fs::read(root.join("v36-prefix-screen-authority.json")).unwrap();
    let authority: V36PrefixFreezeAuthority = serde_json::from_slice(&authority_bytes).unwrap();
    let registry: Vec<V36PrefixRegisteredSourceObject> =
        serde_json::from_slice(&fs::read(root.join("v36-prefix-source-registry.json")).unwrap())
            .unwrap();

    let mut smaller = authority.clone();
    smaller.selected_object_count = 1;
    smaller.selected_object_encoded_bytes = 343_259_046;
    assert!(validate_v36_prefix_freeze_authority(&smaller, &registry).is_ok());
    assert!(validate_v36_prefix_registered_screen_authority(&smaller, &registry).is_err());

    let mut future = authority;
    future.cohort_ordinal = 1;
    future.selected_object_start = 16;
    future.selected_object_encoded_bytes = 5_483_342_562;
    future.excluded_population_identity = Some(V36ArtifactIdentity {
        blake3: "1".repeat(64),
        encoded_bytes: 1,
        role: "population-selected-identities".into(),
        sha256: "2".repeat(64),
        uri: format!(
            "s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/{}-population-selected-identities.arrow",
            "2".repeat(64)
        ),
    });
    assert!(validate_v36_prefix_freeze_authority(&future, &registry).is_ok());
    assert!(validate_v36_prefix_registered_screen_authority(&future, &registry).is_err());
}

#[test]
fn v36_prefix_dataset_accepts_v2_representativeness_authority() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/research");
    let mut value: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("v36-prefix-screen-authority.json")).unwrap())
            .unwrap();
    let authority = value.as_object_mut().unwrap();
    authority.insert(
        "schema".into(),
        "borsuk-v36-prefix-freeze-authority-v2".into(),
    );
    authority.insert("cohort_ordinal".into(), 0.into());
    authority.insert("selected_object_start".into(), 0.into());
    authority.insert("selected_object_count".into(), 16.into());
    authority.insert(
        "selected_object_encoded_bytes".into(),
        5_485_265_954_u64.into(),
    );
    authority.insert(
        "dataset_authority_sha256".into(),
        "0d2e8cef3cf27860131a6a8c33d08b858f8837263212cb03515ae53c76acd5c1".into(),
    );
    authority.insert(
        "excluded_population_identity".into(),
        serde_json::Value::Null,
    );
    authority.insert(
        "population_sampling_algorithm".into(),
        "sha256-seed-sha256-manifest-sha256-feature-row-id-le-u64-v2".into(),
    );
    authority.insert(
        "population_seed_label".into(),
        "borsuk-v36-prefix-screen-population-row-v2".into(),
    );
    authority.insert(
        "population_seed_sha256".into(),
        "bcb490ff7944bfa3a0a6d5abe6d35ba34ecaba60b615e214edb057a1a5b63b8e".into(),
    );
    authority.insert(
        "corpus_seed_label".into(),
        "borsuk-v36-prefix-screen-corpus-v2".into(),
    );
    authority.insert(
        "corpus_seed_sha256".into(),
        "56b288d41e87d3b4ba97ac02b9944837e6bde8b402b8fab088861a27ef099f8c".into(),
    );
    authority.insert(
        "future_full_source_exclusion_roles".into(),
        serde_json::json!(["development", "validation", "sealed-holdout", "performance"]),
    );
    for role in authority["roles"].as_array_mut().unwrap() {
        let name = role["role"].as_str().unwrap();
        let label = format!("borsuk-v36-prefix-screen-{name}-query-v2");
        role["seed_sha256"] =
            serde_json::Value::String(format!("{:x}", Sha256::digest(label.as_bytes())));
        role["seed_label"] = serde_json::Value::String(label);
    }
    let authority: V36PrefixFreezeAuthority = serde_json::from_value(value).unwrap();
    let registry: Vec<V36PrefixRegisteredSourceObject> =
        serde_json::from_slice(&fs::read(root.join("v36-prefix-source-registry.json")).unwrap())
            .unwrap();
    validate_v36_prefix_freeze_authority(&authority, &registry).unwrap();

    let mut mutations = Vec::new();
    let mut changed = authority.clone();
    changed.dataset_authority_sha256 = "f".repeat(64);
    mutations.push(changed);
    let mut changed = authority.clone();
    changed.selected_object_encoded_bytes -= 1;
    mutations.push(changed);
    let mut changed = authority.clone();
    changed.population_seed_label.push_str("-drift");
    mutations.push(changed);
    let mut changed = authority.clone();
    changed.corpus_seed_sha256 = "f".repeat(64);
    mutations.push(changed);
    let mut changed = authority.clone();
    changed.future_full_source_exclusion_roles.pop();
    mutations.push(changed);
    let mut changed = authority;
    changed.cohort_ordinal = 1;
    changed.selected_object_start = 16;
    changed.selected_object_encoded_bytes = 5_483_342_562;
    mutations.push(changed);
    for mutation in mutations {
        assert!(validate_v36_prefix_freeze_authority(&mutation, &registry).is_err());
    }
}

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
            "borsuk-v36-prefix-screen-development-query-v2",
        ),
        (
            "validation",
            1_000,
            "borsuk-v36-prefix-screen-validation-query-v2",
        ),
        (
            "sealed-holdout",
            1_000,
            "borsuk-v36-prefix-screen-sealed-holdout-query-v2",
        ),
        (
            "performance",
            10_000,
            "borsuk-v36-prefix-screen-performance-query-v2",
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
        cohort_ordinal: 0,
        construction_capability: "named-query-excluded-corpus-only-no-query-truth".into(),
        consumed_objects: consumed,
        corpus_rows: 1_000_000,
        corpus_seed_label: "borsuk-v36-prefix-screen-corpus-v2".into(),
        corpus_seed_sha256: "56b288d41e87d3b4ba97ac02b9944837e6bde8b402b8fab088861a27ef099f8c"
            .into(),
        dataset_authority_sha256:
            "0d2e8cef3cf27860131a6a8c33d08b858f8837263212cb03515ae53c76acd5c1".into(),
        distinct_candidates: 1_100_000,
        duplicate_rule: "first-selected-object-ordinal-then-row-offset".into(),
        evaluation_capability: "named-artifacts-only-no-source-list-discovery".into(),
        excluded_population_identity: None,
        format: "borsuk-v36-prefix-population-authority-v2".into(),
        future_full_source_exclusion_roles: vec![
            "development".into(),
            "validation".into(),
            "sealed-holdout".into(),
            "performance".into(),
        ],
        object_cap: 16,
        object_sampling_algorithm:
            "sha256-borsuk-v36-screen-object-v1-path-utf8-length-le-u64-then-path".into(),
        ordered_source_manifest_sha256: format!("{:x}", manifest.finalize()),
        population_id: "borsuk-v36-prefix-screen-population-v2".into(),
        population_sampling_algorithm:
            "sha256-seed-sha256-manifest-sha256-feature-row-id-le-u64-v2".into(),
        population_seed_label: "borsuk-v36-prefix-screen-population-row-v2".into(),
        population_seed_sha256: "bcb490ff7944bfa3a0a6d5abe6d35ba34ecaba60b615e214edb057a1a5b63b8e"
            .into(),
        roles: role_authorities().into(),
        selected_object_count: registry.len().try_into().unwrap(),
        selected_object_encoded_bytes: registry.iter().map(|object| object.encoded_bytes).sum(),
        selected_object_start: 0,
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
        cohort_ordinal: 0,
        construction_capability: population.construction_capability,
        corpus_seed_label: "borsuk-v36-prefix-screen-corpus-v2".into(),
        corpus_seed_sha256: "56b288d41e87d3b4ba97ac02b9944837e6bde8b402b8fab088861a27ef099f8c"
            .into(),
        corpus_rows: population.corpus_rows,
        dataset_authority_sha256:
            "0d2e8cef3cf27860131a6a8c33d08b858f8837263212cb03515ae53c76acd5c1".into(),
        distinct_candidates: population.distinct_candidates,
        duplicate_rule: population.duplicate_rule,
        evaluation_capability: population.evaluation_capability,
        excluded_population_identity: None,
        future_full_source_exclusion_roles: vec![
            "development".into(),
            "validation".into(),
            "sealed-holdout".into(),
            "performance".into(),
        ],
        object_cap: population.object_cap,
        object_sampling_algorithm: population.object_sampling_algorithm,
        ordered_source_manifest_sha256: population.ordered_source_manifest_sha256,
        population_sampling_algorithm:
            "sha256-seed-sha256-manifest-sha256-feature-row-id-le-u64-v2".into(),
        population_seed_label: "borsuk-v36-prefix-screen-population-row-v2".into(),
        population_seed_sha256: "bcb490ff7944bfa3a0a6d5abe6d35ba34ecaba60b615e214edb057a1a5b63b8e"
            .into(),
        registry_encoded_bytes: registry.iter().map(|object| object.encoded_bytes).sum(),
        registry_objects: registry.len().try_into().unwrap(),
        roles: population.roles,
        schema: "borsuk-v36-prefix-freeze-authority-v2".into(),
        selected_object_count: registry.len().try_into().unwrap(),
        selected_object_encoded_bytes: registry.iter().map(|object| object.encoded_bytes).sum(),
        selected_object_start: 0,
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
fn v36_prefix_dataset_v2_population_hashes_all_selected_objects_before_cutoff() {
    let rows = vec![
        (1, 0, 0),
        (2, 0, 1),
        (3, 1, 0),
        (4, 2, 0),
        (5, 3, 0),
        (6, 4, 0),
        (7, 5, 0),
        (8, 15, 0),
        (1, 15, 1),
    ]
    .into_iter()
    .map(
        |(feature_row_id, selected_object_ordinal, row_offset)| borsuk::V36PrefixRowIdentity {
            feature_row_id,
            source_ordinal: None,
            selected_object_ordinal,
            row_offset,
        },
    )
    .collect();
    let selected = select_v36_prefix_population_rows(rows, &"1".repeat(64), 4).unwrap();
    assert_eq!(
        selected
            .iter()
            .map(|row| (
                row.feature_row_id,
                row.selected_object_ordinal,
                row.row_offset
            ))
            .collect::<Vec<_>>(),
        vec![(1, 0, 0), (4, 2, 0), (8, 15, 0), (6, 4, 0)]
    );
}

fn selected_ids_contract(selected_rows: u64) -> V36PrefixSelectedIdsContract {
    V36PrefixSelectedIdsContract {
        cohort_ordinal: 0,
        eligible_rows: 8,
        excluded_population_identity: None,
        excluded_rows: 0,
        ordered_source_manifest_sha256: "1".repeat(64),
        population_seed_sha256: "bcb490ff7944bfa3a0a6d5abe6d35ba34ecaba60b615e214edb057a1a5b63b8e"
            .into(),
        selected_object_count: 16,
        selected_object_start: 0,
        selected_rows,
    }
}

fn external_selection_authority(
    contract: &V36PrefixSelectedIdsContract,
) -> V36PrefixExternalSelectionAuthority {
    V36PrefixExternalSelectionAuthority {
        cohort_ordinal: contract.cohort_ordinal,
        distinct_rows: contract.eligible_rows + contract.excluded_rows,
        excluded_population_identity: contract.excluded_population_identity.clone(),
        ordered_source_manifest_sha256: contract.ordered_source_manifest_sha256.clone(),
        population_seed_sha256: contract.population_seed_sha256.clone(),
        selected_object_count: contract.selected_object_count,
        selected_object_start: contract.selected_object_start,
        selected_rows: contract.selected_rows,
    }
}

fn selected_ids_identity(bytes: &[u8]) -> V36ArtifactIdentity {
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    V36ArtifactIdentity {
        blake3: blake3::hash(bytes).to_hex().to_string(),
        encoded_bytes: bytes.len().try_into().unwrap(),
        role: "population-selected-identities".into(),
        sha256: sha256.clone(),
        uri: format!("s3://fixture/v36/{sha256}-population-selected-identities.arrow"),
    }
}

fn external_identity_source(ordinal: u16) -> V36PrefixSourceObject {
    let path = format!("data/part-{ordinal:04}.parquet");
    let encoded_bytes = 1_024_u64;
    let mut sample = Sha256::new();
    sample.update(b"borsuk-v36-screen-object-v1");
    sample.update(path.as_bytes());
    sample.update(encoded_bytes.to_le_bytes());
    V36PrefixSourceObject {
        blake3: format!("{:064x}", u64::from(ordinal) + 1),
        encoded_bytes,
        path,
        sample_sha256: format!("{:x}", sample.finalize()),
        sha256: format!("{:064x}", u64::from(ordinal) + 17),
        uri: format!("https://example.invalid/data/part-{ordinal:04}.parquet"),
    }
}

fn external_identity_file(
    root: &Path,
    ordinal: u16,
    feature_ids: &[u64],
) -> (V36PrefixIdentityRunFile, Vec<borsuk::V36PrefixRowIdentity>) {
    external_identity_file_with_source(
        root,
        ordinal,
        feature_ids,
        external_identity_source(ordinal),
    )
}

fn external_identity_file_with_source(
    root: &Path,
    ordinal: u16,
    feature_ids: &[u64],
    source: V36PrefixSourceObject,
) -> (V36PrefixIdentityRunFile, Vec<borsuk::V36PrefixRowIdentity>) {
    let rows = feature_ids
        .iter()
        .enumerate()
        .map(
            |(row_offset, &feature_row_id)| borsuk::V36PrefixRowIdentity {
                feature_row_id,
                row_offset: row_offset.try_into().unwrap(),
                selected_object_ordinal: ordinal,
                source_ordinal: None,
            },
        )
        .collect::<Vec<_>>();
    let run = V36PrefixIdentityRun {
        physical_rows: rows.len().try_into().unwrap(),
        rows: rows.clone(),
        selected_object_ordinal: ordinal,
        source: source.clone(),
    };
    let bytes = encode_v36_prefix_identity_run(&run).unwrap();
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let identity = V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes: bytes.len().try_into().unwrap(),
        role: format!("population-identity-run-{ordinal:04}"),
        sha256: sha256.clone(),
        uri: format!("s3://fixture/v36/{sha256}-population-identity-run.arrow"),
    };
    let path = root.join(format!("run-{ordinal:04}.arrow"));
    fs::write(&path, bytes).unwrap();
    (
        V36PrefixIdentityRunFile {
            identity,
            path,
            selected_object_ordinal: ordinal,
            source,
        },
        rows,
    )
}

fn external_selection_limits() -> V36PrefixExternalSelectionLimits {
    V36PrefixExternalSelectionLimits {
        io_buffer_bytes: 64,
        max_input_bytes: 1 << 20,
        max_scratch_bytes: 1 << 20,
        max_spills: 16,
        merge_fan_in: 2,
        sort_buffer_records: 2,
    }
}

fn rewrite_external_identity_file(file: &mut V36PrefixIdentityRunFile, bytes: &[u8]) {
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    file.identity.blake3 = blake3::hash(bytes).to_hex().to_string();
    file.identity.encoded_bytes = bytes.len().try_into().unwrap();
    file.identity.sha256 = sha256.clone();
    file.identity.uri = format!("s3://fixture/v36/{sha256}-population-identity-run.arrow");
    fs::write(&file.path, bytes).unwrap();
}

fn external_selected_file(
    root: &Path,
    feature_ids: &[u64],
) -> (V36PrefixSelectedIdsFile, Vec<borsuk::V36PrefixRowIdentity>) {
    external_selected_file_with_manifest(root, feature_ids, &"1".repeat(64))
}

fn external_selected_file_with_manifest(
    root: &Path,
    feature_ids: &[u64],
    ordered_source_manifest_sha256: &str,
) -> (V36PrefixSelectedIdsFile, Vec<borsuk::V36PrefixRowIdentity>) {
    let mut rows = feature_ids
        .iter()
        .enumerate()
        .map(
            |(row_offset, &feature_row_id)| borsuk::V36PrefixRowIdentity {
                feature_row_id,
                row_offset: row_offset.try_into().unwrap(),
                selected_object_ordinal: 0,
                source_ordinal: None,
            },
        )
        .collect::<Vec<_>>();
    rows =
        select_v36_prefix_population_rows(rows, ordered_source_manifest_sha256, feature_ids.len())
            .unwrap();
    let contract = V36PrefixSelectedIdsContract {
        eligible_rows: feature_ids.len().try_into().unwrap(),
        ordered_source_manifest_sha256: ordered_source_manifest_sha256.into(),
        selected_object_count: 1,
        selected_rows: feature_ids.len().try_into().unwrap(),
        ..selected_ids_contract(feature_ids.len().try_into().unwrap())
    };
    let bytes = encode_v36_prefix_selected_ids(&contract, &rows).unwrap();
    let identity = selected_ids_identity(&bytes);
    let path = root.join("cohort-a-selected.arrow");
    fs::write(&path, bytes).unwrap();
    (
        V36PrefixSelectedIdsFile {
            contract,
            identity,
            path,
        },
        rows,
    )
}

fn external_population_rank(feature_row_id: u64) -> [u8; 32] {
    let seed: [u8; 32] = Sha256::digest(b"borsuk-v36-prefix-screen-population-row-v2").into();
    let manifest = [0x11_u8; 32];
    let mut hasher = Sha256::new();
    hasher.update(seed);
    hasher.update(manifest);
    hasher.update(feature_row_id.to_le_bytes());
    hasher.finalize().into()
}

fn rewrite_external_selected_file(
    selected: &mut V36PrefixSelectedIdsFile,
    duplicate_physical_row: bool,
    corrupt_last_score: bool,
) {
    let bytes = fs::read(&selected.path).unwrap();
    let mut reader = ArrowFileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let schema = reader.schema();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    let feature_ids = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .values()
        .to_vec();
    let scores = batch
        .column(1)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();
    let mut score_values = (0..scores.len())
        .map(|index| scores.value(index).to_vec())
        .collect::<Vec<_>>();
    let object_ordinals = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap()
        .values()
        .to_vec();
    let mut row_offsets = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .values()
        .to_vec();
    if duplicate_physical_row {
        row_offsets[1] = row_offsets[0];
    }
    if corrupt_last_score {
        score_values.last_mut().unwrap()[0] ^= 1;
    }
    let rewritten = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt64Array::from(feature_ids)),
            Arc::new(
                FixedSizeBinaryArray::try_from_iter(
                    score_values.iter().map(std::vec::Vec::as_slice),
                )
                .unwrap(),
            ),
            Arc::new(UInt16Array::from(object_ordinals)),
            Arc::new(UInt64Array::from(row_offsets)),
        ],
    )
    .unwrap();
    let mut output = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
    let mut writer = ArrowFileWriter::try_new_with_options(&mut output, &schema, options).unwrap();
    writer.write(&rewritten).unwrap();
    writer.finish().unwrap();
    drop(writer);
    selected.identity = selected_ids_identity(&output);
    fs::write(&selected.path, output).unwrap();
}

fn reduced_role_assignment_contract(
    selected: &V36PrefixSelectedIdsFile,
) -> V36PrefixRoleAssignmentContract {
    let mut roles = role_authorities();
    for (role, rows) in roles.iter_mut().zip([2, 2, 2, 4]) {
        role.rows = rows;
    }
    V36PrefixRoleAssignmentContract {
        corpus_rows: 8,
        corpus_seed_label: "borsuk-v36-prefix-screen-corpus-v2".into(),
        corpus_seed_sha256: "56b288d41e87d3b4ba97ac02b9944837e6bde8b402b8fab088861a27ef099f8c"
            .into(),
        ordered_source_manifest_sha256: selected.contract.ordered_source_manifest_sha256.clone(),
        roles: roles.into(),
        selected_object_count: 1,
        selected_object_start: 0,
        selected_population_identity: selected.identity.clone(),
        selected_rows: selected.contract.selected_rows,
    }
}

fn scalar_role_assignments(
    selected: &[borsuk::V36PrefixRowIdentity],
    contract: &V36PrefixRoleAssignmentContract,
) -> Vec<(u16, u64, u64, u8, u64)> {
    let mut remaining = selected.to_vec();
    let mut assignments = Vec::new();
    for (role_index, role) in contract.roles.iter().enumerate() {
        remaining.sort_by_key(|row| {
            (
                v36_prefix_query_score_sha256(
                    &role.seed_label,
                    &contract.ordered_source_manifest_sha256,
                    row.feature_row_id,
                )
                .unwrap(),
                row.feature_row_id,
            )
        });
        let rest = remaining.split_off(role.rows.try_into().unwrap());
        assignments.extend(remaining.into_iter().enumerate().map(|(ordinal, row)| {
            (
                row.selected_object_ordinal,
                row.row_offset,
                row.feature_row_id,
                u8::try_from(role_index + 1).unwrap(),
                u64::try_from(ordinal).unwrap(),
            )
        }));
        remaining = rest;
    }
    remaining.sort_by_key(|row| {
        (
            v36_prefix_source_score_sha256(
                &contract.ordered_source_manifest_sha256,
                row.feature_row_id,
            )
            .unwrap(),
            row.feature_row_id,
        )
    });
    assignments.extend(
        remaining
            .into_iter()
            .take(contract.corpus_rows.try_into().unwrap())
            .enumerate()
            .map(|(ordinal, row)| {
                (
                    row.selected_object_ordinal,
                    row.row_offset,
                    row.feature_row_id,
                    0,
                    u64::try_from(ordinal).unwrap(),
                )
            }),
    );
    assignments.sort_by_key(|assignment| (assignment.0, assignment.1));
    assignments
}

fn read_role_assignments(path: &Path) -> Vec<(u16, u64, u64, u8, u64)> {
    let mut reader = ArrowFileReader::try_new(fs::File::open(path).unwrap(), None).unwrap();
    let mut rows = Vec::new();
    for batch in &mut reader {
        let batch = batch.unwrap();
        let objects = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        let offsets = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let roles = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        let ordinals = batch
            .column(4)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        rows.extend((0..batch.num_rows()).map(|row| {
            (
                objects.value(row),
                offsets.value(row),
                ids.value(row),
                roles.value(row),
                ordinals.value(row),
            )
        }));
    }
    rows
}

enum RoleAssignmentMutation {
    FeatureId,
    RoleOrdinal,
}

fn rewrite_role_assignment_file(
    assignment: &mut V36PrefixRoleAssignmentFile,
    mutation: RoleAssignmentMutation,
) {
    let bytes = fs::read(&assignment.path).unwrap();
    let mut reader = ArrowFileReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let schema = reader.schema();
    let batch = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    let objects = batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap()
        .values()
        .to_vec();
    let offsets = batch
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .values()
        .to_vec();
    let mut ids = batch
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .values()
        .to_vec();
    let roles = batch
        .column(3)
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap()
        .values()
        .to_vec();
    let mut ordinals = batch
        .column(4)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .values()
        .to_vec();
    match mutation {
        RoleAssignmentMutation::FeatureId => ids[0] ^= 1,
        RoleAssignmentMutation::RoleOrdinal => {
            let role = roles[0];
            let same_role = roles
                .iter()
                .position(|candidate| *candidate == role)
                .unwrap();
            let second = roles
                .iter()
                .enumerate()
                .skip(same_role + 1)
                .find(|(_, candidate)| **candidate == role)
                .map(|(index, _)| index)
                .unwrap();
            ordinals[second] = ordinals[same_role];
        }
    }
    let rewritten = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt16Array::from(objects)),
            Arc::new(UInt64Array::from(offsets)),
            Arc::new(UInt64Array::from(ids)),
            Arc::new(UInt8Array::from(roles)),
            Arc::new(UInt64Array::from(ordinals)),
        ],
    )
    .unwrap();
    let mut output = Vec::new();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
    let mut writer = ArrowFileWriter::try_new_with_options(&mut output, &schema, options).unwrap();
    writer.write(&rewritten).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let sha256 = format!("{:x}", Sha256::digest(&output));
    assignment.identity = V36ArtifactIdentity {
        blake3: blake3::hash(&output).to_hex().to_string(),
        encoded_bytes: output.len().try_into().unwrap(),
        role: "population-role-assignments".into(),
        sha256: sha256.clone(),
        uri: format!("s3://fixture/v36/{sha256}-population-role-assignments.arrow"),
    };
    fs::write(&assignment.path, output).unwrap();
}

#[test]
fn v36_prefix_dataset_external_role_assignment_is_scalar_exact_and_limit_invariant() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let feature_ids = (1..=24).collect::<Vec<_>>();
    let (selected, selected_rows) = external_selected_file(directory.path(), &feature_ids);
    let contract = reduced_role_assignment_contract(&selected);
    let expected = scalar_role_assignments(&selected_rows, &contract);
    let output = directory.path().join("role-assignments.arrow");
    let limits = external_selection_limits();

    let receipt = assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
        contract: &contract,
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        scratch_root: &scratch,
        selected: &selected,
    })
    .unwrap();
    assert_eq!(read_role_assignments(&output), expected);
    assert!(
        expected
            .iter()
            .any(|assignment| assignment.2 == 19 && assignment.3 == 4 && assignment.4 == 3),
        "performance ID 19 is globally rank 5 for a four-row role and proves cumulative heaps"
    );
    assert_eq!(
        receipt.identity.encoded_bytes,
        fs::metadata(&output).unwrap().len()
    );
    assert_eq!(receipt.identity.role, "population-role-assignments");
    assert!(scratch.read_dir().unwrap().next().is_none());

    let varied = directory.path().join("role-assignments-varied.arrow");
    let varied_limits = V36PrefixExternalSelectionLimits {
        io_buffer_bytes: 128,
        merge_fan_in: 3,
        sort_buffer_records: 3,
        ..external_selection_limits()
    };
    assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
        contract: &contract,
        limits: &varied_limits,
        output: &varied,
        output_uri_prefix: "s3://fixture/v36",
        scratch_root: &scratch,
        selected: &selected,
    })
    .unwrap();
    assert_eq!(fs::read(varied).unwrap(), fs::read(output).unwrap());
}

#[test]
fn v36_prefix_dataset_external_role_assignment_rejects_selected_and_role_drift() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let feature_ids = (1..=24).collect::<Vec<_>>();
    let (mut selected, _) = external_selected_file(directory.path(), &feature_ids);
    let mut contract = reduced_role_assignment_contract(&selected);
    rewrite_external_selected_file(&mut selected, false, true);
    contract.selected_population_identity = selected.identity.clone();
    let output = directory.path().join("role-assignments.arrow");
    let limits = external_selection_limits();
    assert!(
        assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
            contract: &contract,
            limits: &limits,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            scratch_root: &scratch,
            selected: &selected,
        })
        .is_err()
    );
    assert!(!output.exists());
    assert!(scratch.read_dir().unwrap().next().is_none());

    let (mut selected, _) = external_selected_file(directory.path(), &feature_ids);
    let mut contract = reduced_role_assignment_contract(&selected);
    rewrite_external_selected_file(&mut selected, true, false);
    contract.selected_population_identity = selected.identity.clone();
    assert!(
        assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
            contract: &contract,
            limits: &limits,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            scratch_root: &scratch,
            selected: &selected,
        })
        .is_err()
    );
    assert!(!output.exists());
    assert!(scratch.read_dir().unwrap().next().is_none());

    let (selected, _) = external_selected_file(directory.path(), &feature_ids);
    let mut contract = reduced_role_assignment_contract(&selected);
    contract.roles[1].seed_label.push_str("-drift");
    contract.roles[1].seed_sha256 = format!(
        "{:x}",
        Sha256::digest(contract.roles[1].seed_label.as_bytes())
    );
    assert!(
        assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
            contract: &contract,
            limits: &limits,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            scratch_root: &scratch,
            selected: &selected,
        })
        .is_err()
    );
    assert!(!output.exists());
    assert!(scratch.read_dir().unwrap().next().is_none());
}

#[test]
fn v36_prefix_dataset_external_role_assignment_rejects_unbounded_query_heaps() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (mut selected, _) = external_selected_file(directory.path(), &[1]);
    selected.contract.eligible_rows = 400_001;
    selected.contract.selected_rows = 400_001;
    let mut contract = reduced_role_assignment_contract(&selected);
    for role in &mut contract.roles {
        role.rows = 100_000;
    }
    contract.corpus_rows = 1;
    contract.selected_rows = 400_001;
    let output = directory.path().join("role-assignments.arrow");
    let limits = external_selection_limits();

    let error = assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
        contract: &contract,
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        scratch_root: &scratch,
        selected: &selected,
    })
    .unwrap_err();
    assert_eq!(error.code(), "v36_prefix_resource_limit");
    assert!(!output.exists());
    assert!(scratch.read_dir().unwrap().next().is_none());
}

#[test]
fn v36_prefix_dataset_external_role_assignment_never_clobbers_output() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (selected, _) = external_selected_file(directory.path(), &(1..=24).collect::<Vec<_>>());
    let contract = reduced_role_assignment_contract(&selected);
    let output = directory.path().join("role-assignments.arrow");
    fs::write(&output, b"preserve").unwrap();
    let limits = external_selection_limits();

    assert!(
        assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
            contract: &contract,
            limits: &limits,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            scratch_root: &scratch,
            selected: &selected,
        })
        .is_err()
    );
    assert_eq!(fs::read(output).unwrap(), b"preserve");
    assert!(scratch.read_dir().unwrap().next().is_none());
}

#[test]
fn v36_prefix_dataset_external_identity_run_is_bounded_deduplicated_and_deterministic() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let input = directory.path().join("source.parquet");
    let source = write_registered_rows(&input, &[41, 7, 41, 9]);
    let (prior, _) = external_identity_file(directory.path(), 0, &[7]);
    let first_output = directory.path().join("identity-first.arrow");
    let second_output = directory.path().join("identity-second.arrow");
    let mut limits = external_selection_limits();
    limits.sort_buffer_records = 1;
    limits.max_spills = 4;
    let first = externally_build_v36_prefix_identity_run(V36PrefixExternalIdentityRunRequest {
        cutoff_local_rank: Some(2),
        input: &input,
        limits: &limits,
        output: &first_output,
        output_uri_prefix: "s3://fixture/v36",
        prior_runs: std::slice::from_ref(&prior),
        scratch_root: &scratch,
        selected_object_ordinal: 1,
        source: &source,
    })
    .unwrap();
    limits.sort_buffer_records = 3;
    let second = externally_build_v36_prefix_identity_run(V36PrefixExternalIdentityRunRequest {
        cutoff_local_rank: Some(2),
        input: &input,
        limits: &limits,
        output: &second_output,
        output_uri_prefix: "s3://fixture/v36",
        prior_runs: &[prior],
        scratch_root: &scratch,
        selected_object_ordinal: 1,
        source: &source,
    })
    .unwrap();

    assert_eq!(first.physical_rows, 4);
    assert_eq!(first.distinct_rows, 2);
    assert_eq!(first.duplicate_rows, 2);
    assert_eq!(first.cutoff_row_offset, Some(3));
    assert_eq!(first.run.identity, second.run.identity);
    assert_eq!(
        fs::read(&first_output).unwrap(),
        fs::read(&second_output).unwrap()
    );
    let decoded = decode_v36_prefix_identity_run(
        &fs::read(&first_output).unwrap(),
        &first.run.identity,
        &first.run.source,
        1,
    )
    .unwrap();
    assert_eq!(
        decoded
            .rows
            .iter()
            .map(|row| (row.feature_row_id, row.row_offset))
            .collect::<Vec<_>>(),
        vec![(9, 3), (41, 0)]
    );
    assert!(scratch.read_dir().unwrap().next().is_none());
}

#[test]
fn v36_prefix_dataset_external_identity_run_rejects_resource_and_predecessor_drift() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let input = directory.path().join("source.parquet");
    let source = write_registered_rows(&input, &[41, 7, 9]);
    let output = directory.path().join("identity.arrow");
    let (first, _) = external_identity_file(directory.path(), 0, &[7]);
    let (overlapping, _) = external_identity_file(directory.path(), 1, &[7]);
    let limits = external_selection_limits();

    assert!(
        externally_build_v36_prefix_identity_run(V36PrefixExternalIdentityRunRequest {
            cutoff_local_rank: None,
            input: &input,
            limits: &limits,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            prior_runs: &[first.clone(), overlapping],
            scratch_root: &scratch,
            selected_object_ordinal: 2,
            source: &source,
        })
        .is_err()
    );
    assert!(!output.exists());
    assert!(scratch.read_dir().unwrap().next().is_none());

    let mut duplicate_source = source.clone();
    duplicate_source.path = first.source.path.clone();
    duplicate_source.uri = first.source.uri.clone();
    duplicate_source.sha256 = first.source.sha256.clone();
    assert!(
        externally_build_v36_prefix_identity_run(V36PrefixExternalIdentityRunRequest {
            cutoff_local_rank: None,
            input: &input,
            limits: &limits,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            prior_runs: &[first],
            scratch_root: &scratch,
            selected_object_ordinal: 1,
            source: &duplicate_source,
        })
        .is_err()
    );
    assert!(!output.exists());
    assert!(scratch.read_dir().unwrap().next().is_none());

    let tiny_input_limit = V36PrefixExternalSelectionLimits {
        max_input_bytes: source.encoded_bytes - 1,
        ..limits
    };
    assert!(
        externally_build_v36_prefix_identity_run(V36PrefixExternalIdentityRunRequest {
            cutoff_local_rank: None,
            input: &input,
            limits: &tiny_input_limit,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            prior_runs: &[],
            scratch_root: &scratch,
            selected_object_ordinal: 0,
            source: &source,
        })
        .is_err()
    );
    assert!(!output.exists());
    assert!(scratch.read_dir().unwrap().next().is_none());

    fs::write(&output, b"preserve").unwrap();
    assert!(
        externally_build_v36_prefix_identity_run(V36PrefixExternalIdentityRunRequest {
            cutoff_local_rank: None,
            input: &input,
            limits: &limits,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            prior_runs: &[],
            scratch_root: &scratch,
            selected_object_ordinal: 0,
            source: &source,
        })
        .is_err()
    );
    assert_eq!(fs::read(output).unwrap(), b"preserve");
    assert!(scratch.read_dir().unwrap().next().is_none());
}

#[test]
fn v36_prefix_dataset_file_backed_population_scan_commits_without_resident_identity_state() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    let run_output = directory.path().join("run-output");
    let outbox = directory.path().join("outbox");
    fs::create_dir(&scratch).unwrap();
    fs::create_dir(&run_output).unwrap();
    fs::create_dir(&outbox).unwrap();
    let source_paths = [
        directory.path().join("source-0000.parquet"),
        directory.path().join("source-0001.parquet"),
        directory.path().join("source-0002.parquet"),
    ];
    let ranked = vec![
        write_registered_rows(&source_paths[0], &[41, 7, 41, 9]),
        write_registered_rows(&source_paths[1], &[41, 9, 99]),
        write_registered_rows(&source_paths[2], &[41, 99]),
    ];
    let source_byte_cap = ranked.iter().map(|object| object.encoded_bytes).sum();
    let object_prefix = "s3://fixture/v36/file-backed/checkpoints/objects/";
    let context = V36PrefixCheckpointContext {
        cohort_ordinal: 0,
        corpus_rows: 1,
        distinct_candidates: 3,
        excluded_population_identity: None,
        freeze_authority_sha256: "2".repeat(64),
        gt_block_rows: 1,
        object_prefix: object_prefix.into(),
        pointer_uri:
            "s3://fixture/v36/file-backed/checkpoints/runs/v36-prefix-screen-file-backed/latest.json"
                .into(),
        ranked_objects: ranked
            .iter()
            .map(|object| V36PrefixRegisteredSourceObject {
                encoded_bytes: object.encoded_bytes,
                path: object.path.clone(),
                sha256: object.sha256.clone(),
                uri: object.uri.clone(),
            })
            .collect(),
        run_id: "v36-prefix-screen-file-backed".into(),
        selected_object_count: 3,
        selected_object_start: 0,
        source_archive_sha256: "3".repeat(64),
        source_byte_cap,
        source_commit: "4".repeat(40),
        source_registry_sha256: "5".repeat(64),
    };
    let mut writer = V36PrefixPopulationCheckpointWriter::create(
        &outbox,
        context,
        "1".repeat(64),
        "v36-prefix-screen-file-backed-attempt-0000".into(),
        0,
        "i-file-backed".into(),
    )
    .unwrap();
    let limits = external_selection_limits();
    let scan = scan_v36_prefix_object_prefix_file_backed(
        V36PrefixFileBackedScanRequest {
            byte_cap: source_byte_cap,
            distinct_candidates: 3,
            limits: &limits,
            output_uri_prefix: object_prefix,
            prior: None,
            ranked_objects: &ranked,
            run_output_root: &run_output,
            scratch_root: &scratch,
            selected_object_start: 0,
        },
        |ordinal, _| Ok(source_paths[usize::from(ordinal)].clone()),
        |boundary| {
            writer.commit_file(boundary)?;
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(scan.distinct_rows, 4);
    assert_eq!(scan.duplicate_rows, 5);
    assert_eq!(scan.physical_rows, 9);
    assert_eq!(scan.consumed_objects.len(), 3);
    assert_eq!(scan.runs.len(), 3);
    assert_eq!(writer.identity_run_files().unwrap().len(), 3);
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);

    let resumed_output = directory.path().join("resumed-run-output");
    fs::create_dir(&resumed_output).unwrap();
    let resume_request = |prior| V36PrefixFileBackedScanRequest {
        byte_cap: source_byte_cap,
        distinct_candidates: 3,
        limits: &limits,
        output_uri_prefix: object_prefix,
        prior: Some(prior),
        ranked_objects: &ranked,
        run_output_root: &resumed_output,
        scratch_root: &scratch,
        selected_object_start: 0,
    };
    assert_eq!(
        scan_v36_prefix_object_prefix_file_backed(
            resume_request(&scan),
            |_, _| panic!("complete resume must not acquire a source"),
            |_| panic!("complete resume must not publish another checkpoint"),
        )
        .unwrap(),
        scan
    );
    let mut forged = scan.clone();
    forged.distinct_rows += 1;
    forged.physical_rows += 1;
    assert!(
        scan_v36_prefix_object_prefix_file_backed(
            resume_request(&forged),
            |_, _| panic!("forged resume must not acquire a source"),
            |_| panic!("forged resume must not publish a checkpoint"),
        )
        .is_err()
    );
}

#[test]
fn v36_prefix_dataset_external_selection_is_file_backed_bounded_and_scalar_exact() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (first, mut rows) = external_identity_file(directory.path(), 0, &[1, 3, 5, 7]);
    let (second, second_rows) = external_identity_file(directory.path(), 1, &[2, 4, 6, 8]);
    rows.extend(second_rows);
    let expected = select_v36_prefix_population_rows(rows, &"1".repeat(64), 4).unwrap();
    let contract = V36PrefixSelectedIdsContract {
        selected_object_count: 2,
        ..selected_ids_contract(4)
    };
    let output = directory.path().join("selected.arrow");

    let runs = [first, second];
    let limits = external_selection_limits();
    let receipt = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: None,
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap();

    let bytes = fs::read(&output).unwrap();
    assert_eq!(receipt.contract, contract);
    assert_eq!(
        bytes,
        encode_v36_prefix_selected_ids(&contract, &expected).unwrap()
    );
    assert_eq!(receipt.identity, selected_ids_identity(&bytes));
    assert_eq!(
        decode_v36_prefix_selected_ids(&bytes, &receipt.identity, &contract)
            .unwrap()
            .rows,
        expected
    );
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);

    let varied_output = directory.path().join("selected-varied.arrow");
    let varied_limits = V36PrefixExternalSelectionLimits {
        merge_fan_in: 3,
        sort_buffer_records: 3,
        ..external_selection_limits()
    };
    externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: None,
        limits: &varied_limits,
        output: &varied_output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap();
    assert_eq!(fs::read(varied_output).unwrap(), bytes);
}

#[test]
fn v36_prefix_dataset_external_selection_derives_complete_exclusion_counts() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (exclusion, _) = external_selected_file(directory.path(), &[2, 5]);
    let (first, _) = external_identity_file(directory.path(), 1, &[2, 3, 4]);
    let (second, _) = external_identity_file(directory.path(), 2, &[5, 6, 7]);
    let authority = V36PrefixExternalSelectionAuthority {
        cohort_ordinal: 1,
        distinct_rows: 6,
        excluded_population_identity: Some(exclusion.identity.clone()),
        ordered_source_manifest_sha256: "1".repeat(64),
        population_seed_sha256: "bcb490ff7944bfa3a0a6d5abe6d35ba34ecaba60b615e214edb057a1a5b63b8e"
            .into(),
        selected_object_count: 2,
        selected_object_start: 1,
        selected_rows: 3,
    };
    let output = directory.path().join("selected-derived.arrow");
    let runs = [first, second];
    let limits = external_selection_limits();

    let receipt = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &authority,
        exclusion: Some(&exclusion),
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap();

    assert_eq!(receipt.contract.eligible_rows, 4);
    assert_eq!(receipt.contract.excluded_rows, 2);
    assert_eq!(receipt.contract.selected_rows, 3);
    decode_v36_prefix_selected_ids(
        &fs::read(output).unwrap(),
        &receipt.identity,
        &receipt.contract,
    )
    .unwrap();

    let mut drifted = authority;
    drifted.distinct_rows = 7;
    let drifted_output = directory.path().join("selected-drifted.arrow");
    let error = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &drifted,
        exclusion: Some(&exclusion),
        limits: &limits,
        output: &drifted_output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap_err();
    assert!(error.to_string().contains("population count differs"));
    assert!(!drifted_output.exists());
}

#[test]
fn v36_prefix_dataset_external_selection_streams_authenticated_cohort_exclusion() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (exclusion, excluded) = external_selected_file(directory.path(), &[2, 5]);
    let (first, mut rows) = external_identity_file(directory.path(), 1, &[2, 3, 4]);
    let (second, second_rows) = external_identity_file(directory.path(), 2, &[5, 6, 7]);
    rows.extend(second_rows);
    let excluded_ids = excluded
        .iter()
        .map(|row| row.feature_row_id)
        .collect::<std::collections::HashSet<_>>();
    rows.retain(|row| !excluded_ids.contains(&row.feature_row_id));
    let expected = select_v36_prefix_population_rows(rows, &"1".repeat(64), 3).unwrap();
    let contract = V36PrefixSelectedIdsContract {
        cohort_ordinal: 1,
        eligible_rows: 4,
        excluded_population_identity: Some(exclusion.identity.clone()),
        excluded_rows: 2,
        selected_object_count: 2,
        selected_object_start: 1,
        selected_rows: 3,
        ..selected_ids_contract(3)
    };
    let runs = [first, second];
    let limits = external_selection_limits();
    let output = directory.path().join("cohort-b-selected.arrow");

    let receipt = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: Some(&exclusion),
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap();

    let bytes = fs::read(&output).unwrap();
    assert_eq!(receipt.contract, contract);
    assert_eq!(
        bytes,
        encode_v36_prefix_selected_ids(&contract, &expected).unwrap()
    );
    assert_eq!(receipt.identity, selected_ids_identity(&bytes));
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_rejects_tampered_cohort_exclusion() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (exclusion, _) = external_selected_file(directory.path(), &[2, 5]);
    let mut bytes = fs::read(&exclusion.path).unwrap();
    bytes[16] ^= 1;
    fs::write(&exclusion.path, bytes).unwrap();
    let (run, _) = external_identity_file(directory.path(), 1, &[2, 3, 4]);
    let contract = V36PrefixSelectedIdsContract {
        cohort_ordinal: 1,
        eligible_rows: 2,
        excluded_population_identity: Some(exclusion.identity.clone()),
        excluded_rows: 1,
        selected_object_count: 1,
        selected_object_start: 1,
        selected_rows: 2,
        ..selected_ids_contract(2)
    };
    let runs = [run];
    let limits = external_selection_limits();
    let output = directory.path().join("cohort-b-selected.arrow");

    assert!(
        externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
            authority: &external_selection_authority(&contract),
            exclusion: Some(&exclusion),
            limits: &limits,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            runs: &runs,
            scratch_root: &scratch,
        })
        .is_err()
    );
    assert!(!output.exists());
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_rejects_duplicate_excluded_physical_row() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (mut exclusion, _) = external_selected_file(directory.path(), &[2, 5]);
    rewrite_external_selected_file(&mut exclusion, true, false);
    let (run, _) = external_identity_file(directory.path(), 1, &[2, 3, 4, 5]);
    let contract = V36PrefixSelectedIdsContract {
        cohort_ordinal: 1,
        eligible_rows: 2,
        excluded_population_identity: Some(exclusion.identity.clone()),
        excluded_rows: 2,
        selected_object_count: 1,
        selected_object_start: 1,
        selected_rows: 2,
        ..selected_ids_contract(2)
    };
    let runs = [run];
    let limits = external_selection_limits();
    let output = directory.path().join("cohort-b-selected.arrow");

    let error = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: Some(&exclusion),
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap_err();

    assert!(error.to_string().contains("selected-ID rows differ"));
    assert!(!output.exists());
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_drains_multibatch_absent_exclusion() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let excluded_ids = (1_u64..=65_537).collect::<Vec<_>>();
    let current_ids = [100_001_u64, 100_002, 100_003, 100_004];
    let current_max = current_ids
        .iter()
        .map(|&feature_row_id| external_population_rank(feature_row_id))
        .max()
        .unwrap();
    assert!(
        excluded_ids
            .iter()
            .any(|&feature_row_id| external_population_rank(feature_row_id) < current_max)
    );
    assert!(
        excluded_ids
            .iter()
            .any(|&feature_row_id| external_population_rank(feature_row_id) > current_max)
    );
    let (exclusion, _) = external_selected_file(directory.path(), &excluded_ids);
    let (run, rows) = external_identity_file(directory.path(), 1, &current_ids);
    let expected = select_v36_prefix_population_rows(rows, &"1".repeat(64), 4).unwrap();
    let contract = V36PrefixSelectedIdsContract {
        cohort_ordinal: 1,
        eligible_rows: 4,
        excluded_population_identity: Some(exclusion.identity.clone()),
        excluded_rows: 0,
        selected_object_count: 1,
        selected_object_start: 1,
        selected_rows: 4,
        ..selected_ids_contract(4)
    };
    let runs = [run];
    let limits = V36PrefixExternalSelectionLimits {
        max_input_bytes: 8 << 20,
        max_scratch_bytes: 32 << 20,
        sort_buffer_records: 65_536,
        ..external_selection_limits()
    };
    let output = directory.path().join("cohort-b-selected.arrow");

    externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: Some(&exclusion),
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap();

    assert_eq!(
        fs::read(&output).unwrap(),
        encode_v36_prefix_selected_ids(&contract, &expected).unwrap()
    );
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_rejects_exclusion_authority_drift() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (exclusion, _) = external_selected_file(directory.path(), &[2, 5]);
    let (run, _) = external_identity_file(directory.path(), 1, &[2, 3, 4, 5]);
    let mut wrong_manifest = exclusion.clone();
    wrong_manifest.contract.ordered_source_manifest_sha256 = "2".repeat(64);
    let mut nonadjacent = exclusion.clone();
    nonadjacent.contract.selected_object_count = 2;
    let mut wrong_role = exclusion.clone();
    wrong_role.identity.role = "population-selected-identities-drift".into();
    let variants = [wrong_manifest, nonadjacent, wrong_role];

    for (ordinal, drifted) in variants.iter().enumerate() {
        let contract = V36PrefixSelectedIdsContract {
            cohort_ordinal: 1,
            eligible_rows: 2,
            excluded_population_identity: Some(drifted.identity.clone()),
            excluded_rows: 2,
            selected_object_count: 1,
            selected_object_start: 1,
            selected_rows: 2,
            ..selected_ids_contract(2)
        };
        let runs = [run.clone()];
        let limits = external_selection_limits();
        let output = directory.path().join(format!("drifted-{ordinal}.arrow"));
        assert!(
            externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
                authority: &external_selection_authority(&contract),
                exclusion: Some(drifted),
                limits: &limits,
                output: &output,
                output_uri_prefix: "s3://fixture/v36",
                runs: &runs,
                scratch_root: &scratch,
            })
            .is_err()
        );
        assert!(!output.exists());
        assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
    }
}

#[test]
fn v36_prefix_dataset_external_selection_rejects_authenticated_invalid_exclusion_suffix() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (mut exclusion, _) = external_selected_file(directory.path(), &[2, 5, 9]);
    rewrite_external_selected_file(&mut exclusion, false, true);
    let (run, _) = external_identity_file(directory.path(), 1, &[100_001, 100_002, 100_003]);
    let contract = V36PrefixSelectedIdsContract {
        cohort_ordinal: 1,
        eligible_rows: 3,
        excluded_population_identity: Some(exclusion.identity.clone()),
        excluded_rows: 0,
        selected_object_count: 1,
        selected_object_start: 1,
        selected_rows: 2,
        ..selected_ids_contract(2)
    };
    let runs = [run];
    let limits = external_selection_limits();
    let output = directory.path().join("cohort-b-selected.arrow");

    let error = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: Some(&exclusion),
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap_err();

    assert!(error.to_string().contains("selected-ID rows differ"));
    assert!(!output.exists());
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_rejects_committed_duplicates_and_cleans_scratch() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (first, _) = external_identity_file(directory.path(), 0, &[1, 3, 5, 7]);
    let (second, _) = external_identity_file(directory.path(), 1, &[2, 3, 6, 8]);
    let contract = V36PrefixSelectedIdsContract {
        selected_object_count: 2,
        ..selected_ids_contract(4)
    };

    let runs = [first, second];
    let limits = external_selection_limits();
    let output = directory.path().join("selected.arrow");
    let error = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: None,
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap_err();

    assert!(error.to_string().contains("global ID repeats"));
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_enforces_peak_scratch_bytes_and_cleans() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (first, _) = external_identity_file(directory.path(), 0, &[1, 3, 5, 7]);
    let (second, _) = external_identity_file(directory.path(), 1, &[2, 4, 6, 8]);
    let contract = V36PrefixSelectedIdsContract {
        selected_object_count: 2,
        ..selected_ids_contract(4)
    };
    let limits = V36PrefixExternalSelectionLimits {
        max_scratch_bytes: 500,
        ..external_selection_limits()
    };

    let runs = [first, second];
    let output = directory.path().join("selected.arrow");
    let error = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: None,
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap_err();

    assert_eq!(error.code(), "v36_prefix_resource_limit");
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_classifies_complete_window_insufficiency() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (first, _) = external_identity_file(directory.path(), 0, &[1, 3, 5, 7]);
    let (second, _) = external_identity_file(directory.path(), 1, &[2, 4, 6, 8]);
    let mut contract = selected_ids_contract(9);
    contract.eligible_rows = 9;
    contract.selected_object_count = 2;
    let runs = [first, second];
    let limits = external_selection_limits();
    let output = directory.path().join("selected.arrow");

    let error = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: None,
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap_err();

    assert_eq!(error.code(), "v36_prefix_source_insufficient");
    assert!(!output.exists());
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_never_clobbers_existing_output() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (first, _) = external_identity_file(directory.path(), 0, &[1, 3, 5, 7]);
    let (second, _) = external_identity_file(directory.path(), 1, &[2, 4, 6, 8]);
    let contract = V36PrefixSelectedIdsContract {
        selected_object_count: 2,
        ..selected_ids_contract(4)
    };
    let runs = [first, second];
    let limits = external_selection_limits();
    let output = directory.path().join("selected.arrow");
    fs::write(&output, b"existing-authority\n").unwrap();

    assert!(
        externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
            authority: &external_selection_authority(&contract),
            exclusion: None,
            limits: &limits,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            runs: &runs,
            scratch_root: &scratch,
        })
        .is_err()
    );
    assert_eq!(fs::read(output).unwrap(), b"existing-authority\n");
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_preflights_arrow_before_allocation() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (mut run, _) = external_identity_file(directory.path(), 0, &[1, 2, 3, 4]);
    let mut bytes = fs::read(&run.path).unwrap();
    let footer_offset = bytes.len() - 10;
    bytes[footer_offset..footer_offset + 4].copy_from_slice(&(1_048_577_u32).to_le_bytes());
    rewrite_external_identity_file(&mut run, &bytes);
    let mut contract = selected_ids_contract(4);
    contract.eligible_rows = 4;
    contract.selected_object_count = 1;
    let runs = [run];
    let limits = external_selection_limits();
    let output = directory.path().join("selected.arrow");

    let error = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: None,
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap_err();

    assert_eq!(error.code(), "v36_prefix_resource_limit");
    assert!(!output.exists());
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_authenticates_input_before_decoding() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (run, _) = external_identity_file(directory.path(), 0, &[1, 2, 3, 4]);
    let mut bytes = fs::read(&run.path).unwrap();
    bytes[16] ^= 1;
    fs::write(&run.path, bytes).unwrap();
    let mut contract = selected_ids_contract(4);
    contract.eligible_rows = 4;
    contract.selected_object_count = 1;
    let runs = [run];
    let limits = external_selection_limits();
    let output = directory.path().join("selected.arrow");

    let error = externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: None,
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap_err();

    assert!(error.to_string().contains("artifact differs"));
    assert!(!output.exists());
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[cfg(unix)]
#[test]
fn v36_prefix_dataset_external_selection_rejects_symlinked_input() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let (mut run, _) = external_identity_file(directory.path(), 0, &[1, 2, 3, 4]);
    let link = directory.path().join("linked.arrow");
    symlink(&run.path, &link).unwrap();
    run.path = link;
    let mut contract = selected_ids_contract(4);
    contract.eligible_rows = 4;
    contract.selected_object_count = 1;
    let runs = [run];
    let limits = external_selection_limits();
    let output = directory.path().join("selected.arrow");

    assert!(
        externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
            authority: &external_selection_authority(&contract),
            exclusion: None,
            limits: &limits,
            output: &output,
            output_uri_prefix: "s3://fixture/v36",
            runs: &runs,
            scratch_root: &scratch,
        })
        .is_err()
    );
    assert!(!output.exists());
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_external_selection_writes_fixed_selected_arrow_batches() {
    let directory = tempfile::tempdir().unwrap();
    let scratch = directory.path().join("scratch");
    fs::create_dir(&scratch).unwrap();
    let feature_ids = (1_u64..=65_537).collect::<Vec<_>>();
    let (run, rows) = external_identity_file(directory.path(), 0, &feature_ids);
    let expected = select_v36_prefix_population_rows(rows, &"1".repeat(64), 65_537).unwrap();
    let mut contract = selected_ids_contract(65_537);
    contract.eligible_rows = 65_537;
    contract.selected_object_count = 1;
    let runs = [run];
    let limits = V36PrefixExternalSelectionLimits {
        max_input_bytes: 8 << 20,
        max_scratch_bytes: 16 << 20,
        sort_buffer_records: 65_536,
        ..external_selection_limits()
    };
    let output = directory.path().join("selected.arrow");

    externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
        authority: &external_selection_authority(&contract),
        exclusion: None,
        limits: &limits,
        output: &output,
        output_uri_prefix: "s3://fixture/v36",
        runs: &runs,
        scratch_root: &scratch,
    })
    .unwrap();

    let bytes = fs::read(output).unwrap();
    let reader = ArrowFileReader::try_new(std::io::Cursor::new(&bytes), None).unwrap();
    assert_eq!(reader.num_batches(), 2);
    assert_eq!(
        bytes,
        encode_v36_prefix_selected_ids(&contract, &expected).unwrap()
    );
    assert_eq!(fs::read_dir(&scratch).unwrap().count(), 0);
}

#[test]
fn v36_prefix_dataset_selected_ids_are_strict_self_certifying_arrow() {
    let rows = vec![
        (1, 0, 0),
        (2, 0, 1),
        (3, 1, 0),
        (4, 2, 0),
        (5, 3, 0),
        (6, 4, 0),
        (7, 5, 0),
        (8, 15, 0),
    ]
    .into_iter()
    .map(
        |(feature_row_id, selected_object_ordinal, row_offset)| borsuk::V36PrefixRowIdentity {
            feature_row_id,
            source_ordinal: None,
            selected_object_ordinal,
            row_offset,
        },
    )
    .collect();
    let selected = select_v36_prefix_population_rows(rows, &"1".repeat(64), 4).unwrap();
    let contract = selected_ids_contract(4);
    let bytes = encode_v36_prefix_selected_ids(&contract, &selected).unwrap();
    assert_eq!(
        bytes,
        encode_v36_prefix_selected_ids(&contract, &selected).unwrap()
    );
    let identity = selected_ids_identity(&bytes);
    let decoded = decode_v36_prefix_selected_ids(&bytes, &identity, &contract).unwrap();
    assert_eq!(decoded.rows, selected);
    assert_eq!(
        decoded.cutoff_feature_row_id,
        selected.last().unwrap().feature_row_id
    );
    assert_eq!(decoded.cutoff_score_sha256.len(), 64);

    let mut wrong_contract = contract.clone();
    wrong_contract.population_seed_sha256 = "2".repeat(64);
    assert!(decode_v36_prefix_selected_ids(&bytes, &identity, &wrong_contract).is_err());
    let mut wrong_identity = identity.clone();
    wrong_identity.sha256 = "3".repeat(64);
    assert!(decode_v36_prefix_selected_ids(&bytes, &wrong_identity, &contract).is_err());

    let mut unordered = selected;
    unordered.swap(0, 1);
    assert!(encode_v36_prefix_selected_ids(&contract, &unordered).is_err());

    let reader = ArrowFileReader::try_new(std::io::Cursor::new(&bytes), None).unwrap();
    let malformed_schema = Schema::new_with_metadata(
        vec![Field::new("feature_row_id", DataType::UInt64, false)],
        reader.schema().metadata().clone(),
    );
    let malformed_batch = RecordBatch::try_new(
        Arc::new(malformed_schema.clone()),
        vec![Arc::new(UInt64Array::from(vec![1, 4, 8, 6]))],
    )
    .unwrap();
    let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
    let mut malformed_bytes = Vec::new();
    let mut writer =
        ArrowFileWriter::try_new_with_options(&mut malformed_bytes, &malformed_schema, options)
            .unwrap();
    writer.write(&malformed_batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    assert!(
        decode_v36_prefix_selected_ids(
            &malformed_bytes,
            &selected_ids_identity(&malformed_bytes),
            &contract,
        )
        .is_err()
    );
}

#[test]
fn v36_prefix_dataset_selected_ids_bind_cohort_exclusion_metadata() {
    let rows = (1_u64..=9)
        .map(|feature_row_id| borsuk::V36PrefixRowIdentity {
            feature_row_id,
            source_ordinal: None,
            selected_object_ordinal: u16::try_from(feature_row_id % 16).unwrap(),
            row_offset: feature_row_id,
        })
        .collect::<Vec<_>>();
    let initially_selected =
        select_v36_prefix_population_rows(rows.clone(), &"1".repeat(64), 4).unwrap();
    let excluded_id = initially_selected[0].feature_row_id;
    let mut expected = select_v36_prefix_population_rows(
        rows.iter()
            .filter(|row| row.feature_row_id != excluded_id)
            .cloned()
            .collect(),
        &"1".repeat(64),
        4,
    )
    .unwrap();
    for row in &mut expected {
        row.selected_object_ordinal += 16;
    }
    let exclusion_bytes = b"cohort-a-selected-identities\n";
    let exclusion_identity = selected_ids_identity(exclusion_bytes);
    let mut contract = selected_ids_contract(4);
    contract.cohort_ordinal = 1;
    contract.eligible_rows = 8;
    contract.excluded_rows = 1;
    contract.excluded_population_identity = Some(exclusion_identity);
    contract.selected_object_start = 16;
    let bytes = encode_v36_prefix_selected_ids(&contract, &expected).unwrap();
    let decoded =
        decode_v36_prefix_selected_ids(&bytes, &selected_ids_identity(&bytes), &contract).unwrap();
    assert_eq!(decoded.rows, expected);
    assert!(
        decoded
            .rows
            .iter()
            .all(|row| row.feature_row_id != excluded_id)
    );
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
        "borsuk-v36-prefix-screen-population-v2"
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
        "a03897cb913c8b5db7b56ef058828fb1077a7df502e92ea6777a5ca451588799"
    );
    assert_eq!(
        v36_prefix_source_score_sha256(&"a".repeat(64), 2).unwrap(),
        "2ffef3c5304e22603265ae503bc65b6833f9e80cef8635e70e4be373a217ee03"
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
        cutoff_object_ordinal: 0,
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
    let mut changed = receipt.clone();
    changed.cutoff_object_ordinal = 2;
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
        resume_checkpoint: None,
        scratch,
        source_archive: archive_path.clone(),
        source_registry: registry_path,
    };
    fs::create_dir(&request.checkpoint_outbox).unwrap();
    let preflight = load_v36_prefix_freeze_preflight(&request).unwrap();
    assert_eq!(preflight.ranked_objects.len(), registry.len());
    assert_eq!(preflight.authority.object_cap, 16);
    assert_eq!(preflight.producer_attempt_ordinal, 1);
    assert_eq!(preflight.checkpoint_context.gt_block_rows, 65_536);
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
fn v36_prefix_dataset_gt_heap_checkpoint_arrow_is_exact_and_resumable() {
    let roles = [
        V36PrefixQualityRole::Development,
        V36PrefixQualityRole::Validation,
        V36PrefixQualityRole::SealedHoldout,
    ];
    let entries = roles
        .into_iter()
        .enumerate()
        .flat_map(|(role_ordinal, role)| {
            (0_u16..101).map(move |rank| V36PrefixGtHeapEntry {
                feature_row_id: 10_000 + role_ordinal as u64 * 1_000 + u64::from(rank),
                query_ordinal: 0,
                rank,
                role,
                squared_distance: f64::from(rank) / 10.0,
            })
        })
        .collect::<Vec<_>>();
    let checkpoint = V36PrefixGtHeapCheckpoint {
        entries,
        next_source_ordinal: 128,
        query_counts: [1, 1, 1],
    };
    let bytes = encode_v36_prefix_gt_heap_checkpoint(&checkpoint).unwrap();
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    let identity = V36ArtifactIdentity {
        role: "gt-heaps".into(),
        uri: format!("s3://fixture/v36/checkpoints/objects/{sha256}-gt-heaps.arrow"),
        encoded_bytes: bytes.len() as u64,
        sha256,
        blake3: blake3::hash(&bytes).to_hex().to_string(),
    };
    assert_eq!(
        decode_v36_prefix_gt_heap_checkpoint(&bytes, &identity, 128, [1, 1, 1]).unwrap(),
        checkpoint
    );

    let rewrite = |version, batch_rows: usize| {
        let schema = ArrowFileReader::try_new(std::io::Cursor::new(&bytes), None)
            .unwrap()
            .schema();
        let options = IpcWriteOptions::try_new(8, false, version).unwrap();
        let mut rewritten = Vec::new();
        let mut writer =
            ArrowFileWriter::try_new_with_options(&mut rewritten, schema.as_ref(), options)
                .unwrap();
        for entries in checkpoint.entries.chunks(batch_rows) {
            writer
                .write(
                    &RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(UInt8Array::from(
                                entries
                                    .iter()
                                    .map(|entry| match entry.role {
                                        V36PrefixQualityRole::Development => 0,
                                        V36PrefixQualityRole::Validation => 1,
                                        V36PrefixQualityRole::SealedHoldout => 2,
                                    })
                                    .collect::<Vec<_>>(),
                            )),
                            Arc::new(UInt32Array::from(
                                entries
                                    .iter()
                                    .map(|entry| entry.query_ordinal)
                                    .collect::<Vec<_>>(),
                            )),
                            Arc::new(UInt16Array::from(
                                entries.iter().map(|entry| entry.rank).collect::<Vec<_>>(),
                            )),
                            Arc::new(UInt64Array::from(
                                entries
                                    .iter()
                                    .map(|entry| entry.feature_row_id)
                                    .collect::<Vec<_>>(),
                            )),
                            Arc::new(Float64Array::from(
                                entries
                                    .iter()
                                    .map(|entry| entry.squared_distance)
                                    .collect::<Vec<_>>(),
                            )),
                        ],
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        writer.finish().unwrap();
        drop(writer);
        rewritten
    };
    let identity_for = |rewritten: &[u8]| {
        let sha256 = format!("{:x}", Sha256::digest(rewritten));
        V36ArtifactIdentity {
            role: "gt-heaps".into(),
            uri: format!("s3://fixture/v36/checkpoints/objects/{sha256}-gt-heaps.arrow"),
            encoded_bytes: rewritten.len() as u64,
            sha256,
            blake3: blake3::hash(rewritten).to_hex().to_string(),
        }
    };
    let v4 = rewrite(MetadataVersion::V4, usize::MAX);
    assert!(decode_v36_prefix_gt_heap_checkpoint(&v4, &identity_for(&v4), 128, [1, 1, 1]).is_err());
    let repartitioned = rewrite(MetadataVersion::V5, 100);
    assert!(
        decode_v36_prefix_gt_heap_checkpoint(
            &repartitioned,
            &identity_for(&repartitioned),
            128,
            [1, 1, 1],
        )
        .is_err()
    );

    let mut reordered = checkpoint.clone();
    reordered.entries.swap(0, 1);
    assert!(encode_v36_prefix_gt_heap_checkpoint(&reordered).is_err());
    let mut nonfinite = checkpoint.clone();
    nonfinite.entries[0].squared_distance = f64::NAN;
    assert!(encode_v36_prefix_gt_heap_checkpoint(&nonfinite).is_err());
    let mut drifted_identity = identity.clone();
    drifted_identity.encoded_bytes += 1;
    assert!(
        decode_v36_prefix_gt_heap_checkpoint(&bytes, &drifted_identity, 128, [1, 1, 1],).is_err()
    );
    assert!(decode_v36_prefix_gt_heap_checkpoint(&bytes, &identity, 256, [1, 1, 1]).is_err());
}

#[test]
fn v36_prefix_dataset_gt_accumulator_resumes_rank_101_without_output_drift() {
    let query = borsuk::V36PrefixQueryRow {
        query_ordinal: 0,
        feature_row_id: 9_999,
        embedding: vector(1, 0.0),
    };
    let corpus = (0_u64..202)
        .map(|ordinal| borsuk::V36PrefixMaterializedRow {
            feature_row_id: 20_000 + ordinal,
            source_ordinal: Some(ordinal),
            embedding: vector(1, ordinal as f32 / 202.0),
        })
        .collect::<Vec<_>>();
    let mut uninterrupted =
        V36PrefixGtAccumulator::new(V36PrefixQualityRole::Development, vec![query.clone()])
            .unwrap();
    uninterrupted.absorb(&corpus).unwrap();

    let mut interrupted =
        V36PrefixGtAccumulator::new(V36PrefixQualityRole::Development, vec![query.clone()])
            .unwrap();
    interrupted.absorb(&corpus[..111]).unwrap();
    let checkpoint_entries = interrupted.checkpoint_entries().unwrap();
    let prefix_feature_ids = corpus[..111]
        .iter()
        .map(|row| row.feature_row_id)
        .collect::<Vec<_>>();
    assert_eq!(checkpoint_entries.len(), 101);
    assert_eq!(checkpoint_entries[100].rank, 100);
    let mut resumed = V36PrefixGtAccumulator::restore(
        V36PrefixQualityRole::Development,
        vec![query.clone()],
        111,
        &prefix_feature_ids,
        checkpoint_entries.clone(),
    )
    .unwrap();
    resumed.absorb(&corpus[111..]).unwrap();
    assert_eq!(resumed.finish().unwrap(), uninterrupted.finish().unwrap());
    let mut duplicate_resume = V36PrefixGtAccumulator::restore(
        V36PrefixQualityRole::Development,
        vec![query.clone()],
        111,
        &prefix_feature_ids,
        checkpoint_entries.clone(),
    )
    .unwrap();
    let mut duplicated = corpus[111].clone();
    duplicated.feature_row_id = prefix_feature_ids[0];
    assert!(duplicate_resume.absorb(&[duplicated]).is_err());
    let mut injected = checkpoint_entries.clone();
    injected[100].feature_row_id = 50_000;
    assert!(
        V36PrefixGtAccumulator::restore(
            V36PrefixQualityRole::Development,
            vec![query.clone()],
            111,
            &prefix_feature_ids,
            injected,
        )
        .is_err()
    );
    assert!(
        V36PrefixGtAccumulator::restore(
            V36PrefixQualityRole::Development,
            vec![query],
            100,
            &prefix_feature_ids,
            checkpoint_entries,
        )
        .is_err()
    );
}

#[test]
fn v36_prefix_dataset_all_query_gt_checkpoint_resumes_one_source_scan_boundary() {
    let queries = [
        V36PrefixQualityRole::Development,
        V36PrefixQualityRole::Validation,
        V36PrefixQualityRole::SealedHoldout,
    ]
    .map(|role| {
        vec![borsuk::V36PrefixQueryRow {
            query_ordinal: 0,
            feature_row_id: 90_000 + role as u64,
            embedding: vector(1, role as u8 as f32 / 10.0),
        }]
    });
    let corpus = (0_u64..202)
        .map(|ordinal| borsuk::V36PrefixMaterializedRow {
            feature_row_id: 20_000 + ordinal,
            source_ordinal: Some(ordinal),
            embedding: vector(1, ordinal as f32 / 202.0),
        })
        .collect::<Vec<_>>();
    let mut uninterrupted = V36PrefixAllQueryGtAccumulator::new(queries.clone()).unwrap();
    uninterrupted.absorb(&corpus).unwrap();
    let expected = uninterrupted.finish().unwrap();

    let mut interrupted = V36PrefixAllQueryGtAccumulator::new(queries.clone()).unwrap();
    interrupted.absorb(&corpus[..111]).unwrap();
    let checkpoint = interrupted.checkpoint().unwrap();
    assert_eq!(checkpoint.next_source_ordinal, 111);
    assert_eq!(checkpoint.query_counts, [1, 1, 1]);
    assert_eq!(checkpoint.entries.len(), 303);
    let prefix_ids = corpus[..111]
        .iter()
        .map(|row| row.feature_row_id)
        .collect::<Vec<_>>();
    let mut resumed =
        V36PrefixAllQueryGtAccumulator::restore(queries, &prefix_ids, checkpoint).unwrap();
    resumed.absorb(&corpus[111..]).unwrap();
    assert_eq!(resumed.finish().unwrap(), expected);
}

#[test]
fn v36_prefix_dataset_file_gt_runner_resumes_only_after_complete_all_query_block() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.parquet");
    let source_ids = (20_000_u64..20_202).collect::<Vec<_>>();
    let embeddings = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::Float32, false)),
        DIMENSIONS as i32,
        Arc::new(Float32Array::from(
            (0_u64..202)
                .flat_map(|ordinal| vector(1, ordinal as f32 / 202.0))
                .collect::<Vec<_>>(),
        )),
        None,
    )
    .unwrap();
    let source_batch = RecordBatch::try_new(
        Arc::new(v36_prefix_source_schema()),
        vec![
            Arc::new(UInt64Array::from(source_ids.clone())) as ArrayRef,
            Arc::new(embeddings),
        ],
    )
    .unwrap();
    write_v36_prefix_source_parquet(&source_path, &source_ids, [source_batch]).unwrap();
    let queries = [0_u64, 1, 2].map(|ordinal| {
        vec![borsuk::V36PrefixQueryRow {
            query_ordinal: 0,
            feature_row_id: 90_000 + ordinal,
            embedding: vector(1, ordinal as f32 / 10.0),
        }]
    });

    let mut full_checkpoints = Vec::new();
    let (full_truth, full_stats) = run_v36_prefix_gt100_checkpointed(
        &source_path,
        &source_ids,
        queries.clone(),
        None,
        128,
        2,
        |checkpoint| {
            full_checkpoints.push(checkpoint.clone());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        full_checkpoints
            .iter()
            .map(|checkpoint| checkpoint.next_source_ordinal)
            .collect::<Vec<_>>(),
        [128, 202]
    );
    assert_eq!(full_stats.source_scans, 1);
    assert_eq!(full_stats.source_rows, 202);
    assert_eq!(full_stats.quality_queries, 3);

    let mut interrupted_checkpoint = None;
    let error = run_v36_prefix_gt100_checkpointed(
        &source_path,
        &source_ids,
        queries.clone(),
        None,
        128,
        2,
        |checkpoint| {
            interrupted_checkpoint = Some(checkpoint.clone());
            Err(BorsukError::InvalidStorage("fixture interruption".into()))
        },
    )
    .unwrap_err();
    assert_eq!(error.code(), "invalid_storage");
    let checkpoint = interrupted_checkpoint.unwrap();
    assert_eq!(checkpoint.next_source_ordinal, 128);

    let mut resumed_boundaries = Vec::new();
    let (resumed_truth, resumed_stats) = run_v36_prefix_gt100_checkpointed(
        &source_path,
        &source_ids,
        queries,
        Some(checkpoint),
        128,
        2,
        |checkpoint| {
            resumed_boundaries.push(checkpoint.next_source_ordinal);
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(resumed_boundaries, [202]);
    assert_eq!(resumed_stats, full_stats);
    assert_eq!(resumed_truth, full_truth);

    let mut eof_boundaries = Vec::new();
    let (eof_truth, eof_stats) = run_v36_prefix_gt100_checkpointed(
        &source_path,
        &source_ids,
        [0_u64, 1, 2].map(|ordinal| {
            vec![borsuk::V36PrefixQueryRow {
                query_ordinal: 0,
                feature_row_id: 90_000 + ordinal,
                embedding: vector(1, ordinal as f32 / 10.0),
            }]
        }),
        full_checkpoints.last().cloned(),
        128,
        2,
        |checkpoint| {
            eof_boundaries.push(checkpoint.next_source_ordinal);
            Ok(())
        },
    )
    .unwrap();
    assert!(eof_boundaries.is_empty());
    assert_eq!(eof_stats, full_stats);
    assert_eq!(eof_truth, full_truth);
}

#[test]
fn v36_prefix_dataset_file_gt_runner_rejects_query_state_above_checkpoint_cap() {
    let mut next_id = 90_000_u64;
    let queries = [1_001_usize; 3].map(|count| {
        (0..count)
            .map(|query_ordinal| {
                let feature_row_id = next_id;
                next_id += 1;
                borsuk::V36PrefixQueryRow {
                    query_ordinal: u32::try_from(query_ordinal).unwrap(),
                    feature_row_id,
                    embedding: vector(1, query_ordinal as f32 / count as f32),
                }
            })
            .collect::<Vec<_>>()
    });
    let error = run_v36_prefix_gt100_checkpointed(
        Path::new("missing-source.parquet"),
        &[1],
        queries,
        None,
        101,
        1,
        |_| Ok(()),
    )
    .unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid storage: V36 prefix GT heap query count differs"
    );
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
    let mut cohort_b = Vec::new();
    scan_v36_prefix_registered_input_parquet(&path, &object, 16, |row| {
        cohort_b.push(row.selected_object_ordinal);
        Ok(())
    })
    .unwrap();
    assert_eq!(cohort_b, [16, 16]);
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

#[test]
fn v36_prefix_dataset_accepts_independent_pyarrow_registered_input() {
    let bytes = include_bytes!("fixtures/v36_pyarrow_registered_input.parquet");
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("registered.parquet");
    fs::write(&path, bytes).unwrap();
    let object = V36PrefixRankedSourceObject {
        encoded_bytes: bytes.len() as u64,
        path: "data/registered.parquet".into(),
        sample_sha256: sample_digest("data/registered.parquet", bytes.len() as u64),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        uri: "https://example.invalid/registered.parquet".into(),
    };
    let mut rows = Vec::new();
    assert_eq!(
        scan_v36_prefix_registered_input_parquet(&path, &object, 0, |row| {
            rows.push(row);
            Ok(())
        })
        .unwrap(),
        1
    );
    assert_eq!(rows[0].feature_row_id, 7);
    assert_eq!(rows[0].embedding.len(), DIMENSIONS);
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

fn ranked_source_manifest_sha256(objects: &[V36PrefixRankedSourceObject]) -> String {
    let mut ordered = objects.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| left.path.as_bytes().cmp(right.path.as_bytes()));
    let mut manifest = Sha256::new();
    for object in ordered {
        manifest.update(object.path.as_bytes());
        manifest.update(b"\t");
        manifest.update(object.sha256.as_bytes());
        manifest.update(b"\t");
        manifest.update(object.encoded_bytes.to_string().as_bytes());
        manifest.update(b"\n");
    }
    format!("{:x}", manifest.finalize())
}

fn checkpoint_artifact_for_file(
    role: &str,
    filename: &str,
    path: &Path,
    object_prefix: &str,
) -> V36ArtifactIdentity {
    let bytes = fs::read(path).unwrap();
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes: bytes.len().try_into().unwrap(),
        role: role.to_owned(),
        sha256: sha256.clone(),
        uri: format!("{object_prefix}{sha256}-{filename}"),
    }
}

fn stage_checkpoint_head(outbox: &Path, ready: &Path, staged: &Path) {
    fs::create_dir(staged).unwrap();
    fs::create_dir(staged.join("objects")).unwrap();
    let ready: serde_json::Value = serde_json::from_slice(&fs::read(ready).unwrap()).unwrap();
    let manifest: V36ArtifactIdentity = serde_json::from_value(ready["manifest"].clone()).unwrap();
    fs::copy(
        outbox
            .join("manifests")
            .join(format!("{}.json", manifest.sha256)),
        staged.join("manifest.json"),
    )
    .unwrap();
    fs::copy(
        outbox.join("pointers").join(format!(
            "{}.json",
            ready["pointer_sha256"].as_str().unwrap()
        )),
        staged.join("pointer.json"),
    )
    .unwrap();
    for identity in
        serde_json::from_value::<Vec<V36ArtifactIdentity>>(ready["dependencies"].clone()).unwrap()
    {
        fs::copy(
            outbox
                .join("objects")
                .join(format!("{}.blob", identity.sha256)),
            staged
                .join("objects")
                .join(format!("{}.blob", identity.sha256)),
        )
        .unwrap();
    }
}

#[test]
fn v36_prefix_dataset_external_materialization_merge_joins_physical_assignments() {
    let directory = tempfile::tempdir().unwrap();
    let first_source = directory.path().join("source-input-a.parquet");
    let second_source = directory.path().join("source-input-b.parquet");
    let feature_ids = (1_i64..=24).collect::<Vec<_>>();
    let mut sources = vec![
        (
            write_registered_rows(&first_source, &feature_ids),
            first_source,
        ),
        (
            write_registered_rows(&second_source, &feature_ids),
            second_source,
        ),
    ];
    sources.sort_by(|left, right| {
        (&left.0.sample_sha256, &left.0.path).cmp(&(&right.0.sample_sha256, &right.0.path))
    });
    let source = sources[0].1.clone();
    let ranked = sources
        .into_iter()
        .map(|(object, _)| object)
        .collect::<Vec<_>>();
    let manifest = ranked_source_manifest_sha256(&ranked);
    let (selected, _) = external_selected_file_with_manifest(
        directory.path(),
        &(1_u64..=24).collect::<Vec<_>>(),
        &manifest,
    );
    let contract = reduced_role_assignment_contract(&selected);
    let assignment_scratch = directory.path().join("assignment-scratch");
    fs::create_dir(&assignment_scratch).unwrap();
    let assignment_path = directory.path().join("role-assignments.arrow");
    let limits = external_selection_limits();
    let receipt = assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
        contract: &contract,
        limits: &limits,
        output: &assignment_path,
        output_uri_prefix: "s3://fixture/v36",
        scratch_root: &assignment_scratch,
        selected: &selected,
    })
    .unwrap();
    let mut expected = read_role_assignments(&assignment_path);
    expected.sort_by_key(|assignment| (assignment.3, assignment.4));
    let assignment = V36PrefixRoleAssignmentFile {
        contract,
        identity: receipt.identity,
        path: assignment_path,
    };
    let scratch = directory.path().join("materialization-scratch");
    let output = directory.path().join("output");
    fs::create_dir(&scratch).unwrap();

    let mut reversed_registry = ranked.clone();
    reversed_registry.reverse();
    assert!(
        materialize_v36_prefix_assigned_roles(V36PrefixExternalMaterializationRequest {
            assignment: &assignment,
            limits: &limits,
            output: &output,
            ranked_objects: &reversed_registry,
            scratch_root: &scratch,
            source_paths: std::slice::from_ref(&source),
        })
        .is_err()
    );
    assert!(!output.exists());
    assert!(scratch.read_dir().unwrap().next().is_none());
    fs::create_dir(&output).unwrap();

    let paths: borsuk::V36PrefixRoleParquetPaths =
        materialize_v36_prefix_assigned_roles(V36PrefixExternalMaterializationRequest {
            assignment: &assignment,
            limits: &limits,
            output: &output,
            ranked_objects: &ranked,
            scratch_root: &scratch,
            source_paths: std::slice::from_ref(&source),
        })
        .unwrap();

    let source_ids = expected
        .iter()
        .filter(|assignment| assignment.3 == 0)
        .map(|assignment| assignment.2)
        .collect::<Vec<_>>();
    scan_v36_prefix_source_parquet(&paths.source, &source_ids, |_| Ok(())).unwrap();
    let query_paths = [
        paths.development,
        paths.validation,
        paths.sealed_holdout,
        paths.performance,
    ];
    for (role, path) in query_paths.into_iter().enumerate() {
        let expected_ids = expected
            .iter()
            .filter(|assignment| assignment.3 == u8::try_from(role + 1).unwrap())
            .map(|assignment| assignment.2)
            .collect::<Vec<_>>();
        let mut actual_ids = Vec::new();
        scan_v36_prefix_query_parquet(&path, expected_ids.len() as u64, |batch| {
            actual_ids.extend_from_slice(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values(),
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(actual_ids, expected_ids);
    }
    assert!(scratch.read_dir().unwrap().next().is_none());
}

#[test]
fn v36_prefix_dataset_reduced_file_backed_checkpoint_restore_select_materialize() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source-input.parquet");
    let mut ranked = vec![write_registered_rows(
        &source_path,
        &(1_i64..=144).collect::<Vec<_>>(),
    )];
    ranked[0].uri = format!(
        "https://huggingface.co/datasets/andropar/relaion2b-natural-embeddings/resolve/{}/{}",
        "bfc7465dcf1245bd605d35dcaf5d2177bbc2025a", ranked[0].path,
    );
    let object_prefix = "s3://fixture/v36/reduced/checkpoints/objects/";
    let context = V36PrefixCheckpointContext {
        cohort_ordinal: 0,
        corpus_rows: 128,
        distinct_candidates: 144,
        excluded_population_identity: None,
        freeze_authority_sha256: "2".repeat(64),
        gt_block_rows: 128,
        object_prefix: object_prefix.into(),
        pointer_uri:
            "s3://fixture/v36/reduced/checkpoints/runs/v36-prefix-screen-reduced/latest.json".into(),
        ranked_objects: ranked
            .iter()
            .map(|object| V36PrefixRegisteredSourceObject {
                encoded_bytes: object.encoded_bytes,
                path: object.path.clone(),
                sha256: object.sha256.clone(),
                uri: object.uri.clone(),
            })
            .collect(),
        run_id: "v36-prefix-screen-reduced".into(),
        selected_object_count: 1,
        selected_object_start: 0,
        source_archive_sha256: "3".repeat(64),
        source_byte_cap: ranked[0].encoded_bytes,
        source_commit: "4".repeat(40),
        source_registry_sha256: "5".repeat(64),
    };
    let first_outbox = directory.path().join("population-outbox");
    fs::create_dir(&first_outbox).unwrap();
    let mut writer = V36PrefixPopulationCheckpointWriter::create(
        &first_outbox,
        context.clone(),
        "1".repeat(64),
        "v36-prefix-screen-reduced-attempt-0000".into(),
        0,
        "i-reduced-a".into(),
    )
    .unwrap();
    let population_scratch = directory.path().join("population-scratch");
    let population_runs = directory.path().join("population-runs");
    fs::create_dir(&population_scratch).unwrap();
    fs::create_dir(&population_runs).unwrap();
    let mut limits = external_selection_limits();
    limits.sort_buffer_records = 64;
    let mut population_ready = None;
    scan_v36_prefix_object_prefix_file_backed(
        V36PrefixFileBackedScanRequest {
            byte_cap: ranked[0].encoded_bytes,
            distinct_candidates: 144,
            limits: &limits,
            output_uri_prefix: object_prefix,
            prior: None,
            ranked_objects: &ranked,
            run_output_root: &population_runs,
            scratch_root: &population_scratch,
            selected_object_start: 0,
        },
        |_, _| Ok(source_path.clone()),
        |boundary| {
            population_ready = Some(writer.commit_file(boundary)?);
            Ok(())
        },
    )
    .unwrap();

    let staged = directory.path().join("staged-population");
    stage_checkpoint_head(&first_outbox, &population_ready.unwrap(), &staged);
    let head = load_v36_prefix_checkpoint_head(&staged, &context).unwrap();
    let restore_scratch = directory.path().join("restore-scratch");
    fs::create_dir(&restore_scratch).unwrap();
    let restored =
        restore_v36_prefix_file_backed_population_scan(&head, &limits, &restore_scratch).unwrap();
    assert_eq!(restored.distinct_rows, 144);
    assert_eq!(restored.duplicate_rows, 0);
    assert_eq!(restored.physical_rows, 144);
    assert_eq!(restored.runs.len(), 1);
    assert!(restore_scratch.read_dir().unwrap().next().is_none());
    let V36PrefixCheckpointResumeState::Population {
        population: resumed_population,
    } = restore_v36_prefix_checkpoint_phase(&head, None, &limits, &restore_scratch).unwrap()
    else {
        panic!("population checkpoint resumed at the wrong phase")
    };
    assert_eq!(resumed_population.distinct_rows, 144);
    let mut forged = head.clone();
    forged.manifest.population.distinct_rows -= 1;
    forged.manifest.population.duplicate_rows += 1;
    assert!(
        restore_v36_prefix_file_backed_population_scan(&forged, &limits, &restore_scratch,)
            .is_err()
    );
    let resumed_outbox = directory.path().join("resumed-outbox");
    fs::create_dir(&resumed_outbox).unwrap();
    let (mut writer, restored) =
        V36PrefixPopulationCheckpointWriter::resume_file_backed(V36PrefixFileBackedResumeRequest {
            execution_authority_sha256: "6".repeat(64),
            head,
            context: context.clone(),
            limits: &limits,
            producer_attempt_id: "v36-prefix-screen-reduced-attempt-0001".into(),
            producer_attempt_ordinal: 1,
            producer_instance_id: "i-reduced-b".into(),
            root: &resumed_outbox,
            scratch_root: &restore_scratch,
        })
        .unwrap();
    assert_eq!(restored.distinct_rows, 144);
    assert_eq!(restored.physical_rows, 144);
    assert_eq!(restored.runs, writer.identity_run_files().unwrap());

    let selection_scratch = directory.path().join("selection-scratch");
    fs::create_dir(&selection_scratch).unwrap();
    let selected_path = directory.path().join("selected.arrow");
    let selected_contract = V36PrefixSelectedIdsContract {
        cohort_ordinal: 0,
        eligible_rows: 144,
        excluded_population_identity: None,
        excluded_rows: 0,
        ordered_source_manifest_sha256: ranked_source_manifest_sha256(&ranked),
        population_seed_sha256: "bcb490ff7944bfa3a0a6d5abe6d35ba34ecaba60b615e214edb057a1a5b63b8e"
            .into(),
        selected_object_count: 1,
        selected_object_start: 0,
        selected_rows: 144,
    };
    let selected_receipt =
        externally_select_v36_prefix_population_rows(V36PrefixExternalSelectionRequest {
            authority: &external_selection_authority(&selected_contract),
            exclusion: None,
            limits: &limits,
            output: &selected_path,
            output_uri_prefix: object_prefix,
            runs: &writer.identity_run_files().unwrap(),
            scratch_root: &selection_scratch,
        })
        .unwrap();
    let selected = decode_v36_prefix_selected_ids(
        &fs::read(&selected_path).unwrap(),
        &selected_receipt.identity,
        &selected_contract,
    )
    .unwrap();
    assert_eq!(
        selected_receipt.cutoff_feature_row_id,
        selected.cutoff_feature_row_id
    );
    assert_eq!(
        selected_receipt.cutoff_score_sha256,
        selected.cutoff_score_sha256
    );
    assert_eq!(
        selected_receipt.contract.eligible_rows,
        selected_contract.eligible_rows
    );
    assert_eq!(
        selected_receipt.contract.excluded_rows,
        selected_contract.excluded_rows
    );
    let selection = V36PrefixPopulationSelection {
        cutoff_feature_row_id: selected_receipt.cutoff_feature_row_id,
        cutoff_score_sha256: selected_receipt.cutoff_score_sha256.clone(),
        eligible_rows: selected_receipt.contract.eligible_rows,
        excluded_rows: selected_receipt.contract.excluded_rows,
        excluded_population_identity: None,
        selected_ids: selected_receipt.identity.clone(),
        selected_rows: selected_contract.selected_rows,
    };
    let selected_ready = writer.commit_selected(&selection, &selected_path).unwrap();
    let staged_selected = directory.path().join("staged-selected");
    stage_checkpoint_head(&resumed_outbox, &selected_ready, &staged_selected);
    let selected_head = load_v36_prefix_checkpoint_head(&staged_selected, &context).unwrap();
    assert!(matches!(
        selected_head.manifest.phase,
        borsuk::V36PrefixCheckpointPhase::Selected { .. }
    ));
    assert_eq!(selected_head.dependencies.len(), 2);
    let V36PrefixCheckpointResumeState::Selected {
        population: resumed_population,
        selected: resumed_selected,
    } = restore_v36_prefix_checkpoint_phase(
        &selected_head,
        Some(&selected_contract),
        &limits,
        &restore_scratch,
    )
    .unwrap()
    else {
        panic!("selected checkpoint resumed at the wrong phase")
    };
    assert_eq!(resumed_population.distinct_rows, 144);
    assert_eq!(resumed_selected.contract, selected_contract);
    assert_eq!(resumed_selected.identity, selection.selected_ids);
    assert_eq!(
        resumed_selected.path,
        staged_selected
            .join("objects")
            .join(format!("{}.blob", selection.selected_ids.sha256))
    );
    let mut drifted_contract = selected_contract.clone();
    drifted_contract.ordered_source_manifest_sha256 = "f".repeat(64);
    assert!(
        restore_v36_prefix_checkpoint_phase(
            &selected_head,
            Some(&drifted_contract),
            &limits,
            &restore_scratch,
        )
        .is_err()
    );
    let substituted_selected_path = directory.path().join("substituted-selected.arrow");
    fs::copy(&resumed_selected.path, &substituted_selected_path).unwrap();
    let mut substituted_head = selected_head.clone();
    substituted_head.dependencies[1].path = substituted_selected_path;
    assert!(
        restore_v36_prefix_checkpoint_phase(
            &substituted_head,
            Some(&selected_contract),
            &limits,
            &restore_scratch,
        )
        .is_err()
    );

    let selected_file = V36PrefixSelectedIdsFile {
        contract: selected_contract,
        identity: selected_receipt.identity,
        path: selected_path,
    };
    let mut assignment_contract = reduced_role_assignment_contract(&selected_file);
    assignment_contract.corpus_rows = 128;
    let mut expected_assignments = scalar_role_assignments(&selected.rows, &assignment_contract);
    expected_assignments.sort_by_key(|assignment| (assignment.3, assignment.4));
    let assignment_path = directory.path().join("role-assignments.arrow");
    let assignment_receipt =
        assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
            contract: &assignment_contract,
            limits: &limits,
            output: &assignment_path,
            output_uri_prefix: object_prefix,
            scratch_root: &selection_scratch,
            selected: &selected_file,
        })
        .unwrap();
    let assignment = V36PrefixRoleAssignmentFile {
        contract: assignment_contract,
        identity: assignment_receipt.identity,
        path: assignment_path,
    };
    let materialization_scratch = directory.path().join("materialization-scratch");
    fs::create_dir(&materialization_scratch).unwrap();
    let materialized_root = directory.path().join("materialized");
    fs::create_dir(&materialized_root).unwrap();
    let paths = materialize_v36_prefix_assigned_roles(V36PrefixExternalMaterializationRequest {
        assignment: &assignment,
        limits: &limits,
        output: &materialized_root,
        ranked_objects: &ranked,
        scratch_root: &materialization_scratch,
        source_paths: std::slice::from_ref(&source_path),
    })
    .unwrap();

    let assert_embedding = |feature_row_id: u64, embedding: &Float32Array| {
        let mut expected = vec![0.0_f32; DIMENSIONS];
        expected[usize::try_from(feature_row_id - 1).unwrap() % DIMENSIONS] = 1.0;
        assert_eq!(embedding.values().as_ref(), expected.as_slice());
    };
    let expected_source_ids = expected_assignments
        .iter()
        .filter(|assignment| assignment.3 == 0)
        .map(|assignment| assignment.2)
        .collect::<Vec<_>>();
    let mut actual_source_ids = Vec::new();
    scan_v36_prefix_source_parquet(&paths.source, &expected_source_ids, |batch| {
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let embeddings = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let feature_row_id = ids.value(row);
            actual_source_ids.push(feature_row_id);
            let embedding = embeddings.value(row);
            assert_embedding(
                feature_row_id,
                embedding.as_any().downcast_ref::<Float32Array>().unwrap(),
            );
        }
        Ok(())
    })
    .unwrap();
    assert_eq!(actual_source_ids, expected_source_ids);
    for (role, path) in [
        paths.development.clone(),
        paths.validation.clone(),
        paths.sealed_holdout.clone(),
        paths.performance.clone(),
    ]
    .into_iter()
    .enumerate()
    {
        let expected_ids = expected_assignments
            .iter()
            .filter(|assignment| assignment.3 == u8::try_from(role + 1).unwrap())
            .map(|assignment| assignment.2)
            .collect::<Vec<_>>();
        let mut actual_ids = Vec::new();
        scan_v36_prefix_query_parquet(&path, expected_ids.len() as u64, |batch| {
            let ordinals = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let embeddings = batch
                .column(2)
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let feature_row_id = ids.value(row);
                assert_eq!(
                    u64::from(ordinals.value(row)),
                    u64::try_from(actual_ids.len()).unwrap()
                );
                actual_ids.push(feature_row_id);
                let embedding = embeddings.value(row);
                assert_embedding(
                    feature_row_id,
                    embedding.as_any().downcast_ref::<Float32Array>().unwrap(),
                );
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(actual_ids, expected_ids);
    }

    let population_authority = materialized_root.join("population-authority.json");
    let registry = ranked
        .iter()
        .map(|object| V36PrefixRegisteredSourceObject {
            encoded_bytes: object.encoded_bytes,
            path: object.path.clone(),
            sha256: object.sha256.clone(),
            uri: object.uri.clone(),
        })
        .collect::<Vec<_>>();
    let mut population = population(&registry);
    population.consumed_objects[0].blake3 = blake3::hash(&fs::read(&source_path).unwrap())
        .to_hex()
        .to_string();
    fs::write(
        &population_authority,
        canonical_v36_prefix_population_authority_bytes(&population, &registry).unwrap(),
    )
    .unwrap();
    let artifact_specs = [
        (
            "population-authority",
            "population-authority.json",
            population_authority,
        ),
        ("source", "source.parquet", paths.source.clone()),
        (
            "development-query",
            "development-query.parquet",
            paths.development.clone(),
        ),
        (
            "validation-query",
            "validation-query.parquet",
            paths.validation.clone(),
        ),
        (
            "sealed-holdout-query",
            "sealed-holdout-query.parquet",
            paths.sealed_holdout.clone(),
        ),
        (
            "performance-query",
            "performance-query.parquet",
            paths.performance.clone(),
        ),
    ];
    let dependencies = artifact_specs
        .iter()
        .map(|(role, filename, path)| V36PrefixCheckpointDependencyFile {
            identity: checkpoint_artifact_for_file(role, filename, path, object_prefix),
            path: path.clone(),
        })
        .collect::<Vec<_>>();
    let artifacts = V36PrefixMaterializedArtifacts {
        population_authority: dependencies[0].identity.clone(),
        source: dependencies[1].identity.clone(),
        development_query: dependencies[2].identity.clone(),
        validation_query: dependencies[3].identity.clone(),
        sealed_holdout_query: dependencies[4].identity.clone(),
        performance_query: dependencies[5].identity.clone(),
    };
    let ready = writer
        .commit_materialized(&artifacts, &dependencies)
        .unwrap();
    assert_eq!(ready.file_name().unwrap(), "generation-00000002.json");
    assert_eq!(
        resumed_outbox.join("commits").read_dir().unwrap().count(),
        2
    );
    let staged_materialized = directory.path().join("staged-materialized");
    stage_checkpoint_head(&resumed_outbox, &ready, &staged_materialized);
    let materialized_head =
        load_v36_prefix_checkpoint_head(&staged_materialized, &context).unwrap();
    assert!(matches!(
        materialized_head.manifest.phase,
        borsuk::V36PrefixCheckpointPhase::Materialized { .. }
    ));
    assert_eq!(materialized_head.dependencies.len(), 8);
    let V36PrefixCheckpointResumeState::Materialized {
        artifacts: restored_artifacts,
        population: resumed_population,
        selected: resumed_selected,
    } = restore_v36_prefix_checkpoint_phase(
        &materialized_head,
        Some(&selected_file.contract),
        &limits,
        &restore_scratch,
    )
    .unwrap()
    else {
        panic!("materialized checkpoint resumed at the wrong phase")
    };
    assert_eq!(resumed_population.distinct_rows, 144);
    assert_eq!(resumed_selected.identity, selection.selected_ids);
    assert_eq!(
        restored_artifacts.population_authority.identity,
        artifacts.population_authority
    );
    assert_eq!(restored_artifacts.source.identity, artifacts.source);
    assert_eq!(
        restored_artifacts.development_query.identity,
        artifacts.development_query
    );
    assert_eq!(
        restored_artifacts.validation_query.identity,
        artifacts.validation_query
    );
    assert_eq!(
        restored_artifacts.sealed_holdout_query.identity,
        artifacts.sealed_holdout_query
    );
    assert_eq!(
        restored_artifacts.performance_query.identity,
        artifacts.performance_query
    );

    let heaps_path = directory.path().join("gt-heaps-00000128.arrow");
    let heap_checkpoint = V36PrefixGtHeapCheckpoint {
        entries: [
            V36PrefixQualityRole::Development,
            V36PrefixQualityRole::Validation,
            V36PrefixQualityRole::SealedHoldout,
        ]
        .into_iter()
        .flat_map(|role| {
            let source_ids = &expected_source_ids;
            (0_u32..2).flat_map(move |query_ordinal| {
                source_ids
                    .iter()
                    .take(101)
                    .enumerate()
                    .map(move |(rank, feature_row_id)| V36PrefixGtHeapEntry {
                        feature_row_id: *feature_row_id,
                        query_ordinal,
                        rank: u16::try_from(rank).unwrap(),
                        role,
                        squared_distance: rank as f64 + f64::from(query_ordinal) / 10.0,
                    })
            })
        })
        .collect(),
        next_source_ordinal: 128,
        query_counts: [2, 2, 2],
    };
    fs::write(
        &heaps_path,
        encode_v36_prefix_gt_heap_checkpoint(&heap_checkpoint).unwrap(),
    )
    .unwrap();
    let heaps = V36PrefixCheckpointDependencyFile {
        identity: checkpoint_artifact_for_file(
            "gt-heaps",
            "gt-heaps-00000128.arrow",
            &heaps_path,
            object_prefix,
        ),
        path: heaps_path,
    };
    let wrong_query_heap_path = directory.path().join("gt-heaps-wrong-query-count.arrow");
    let wrong_query_heap_checkpoint = V36PrefixGtHeapCheckpoint {
        entries: heap_checkpoint
            .entries
            .iter()
            .filter(|entry| entry.query_ordinal == 0)
            .cloned()
            .collect(),
        next_source_ordinal: 128,
        query_counts: [1, 1, 1],
    };
    fs::write(
        &wrong_query_heap_path,
        encode_v36_prefix_gt_heap_checkpoint(&wrong_query_heap_checkpoint).unwrap(),
    )
    .unwrap();
    let wrong_query_heaps = V36PrefixCheckpointDependencyFile {
        identity: checkpoint_artifact_for_file(
            "gt-heaps",
            "gt-heaps-wrong-query-count.arrow",
            &wrong_query_heap_path,
            object_prefix,
        ),
        path: wrong_query_heap_path,
    };
    let wrong_query_outbox = directory.path().join("wrong-query-outbox");
    fs::create_dir(&wrong_query_outbox).unwrap();
    let (mut wrong_query_writer, _) =
        V36PrefixPopulationCheckpointWriter::resume_phase(V36PrefixPhaseResumeRequest {
            execution_authority_sha256: "7".repeat(64),
            head: materialized_head.clone(),
            context: context.clone(),
            limits: &limits,
            producer_attempt_id: "v36-prefix-screen-reduced-attempt-0002".into(),
            producer_attempt_ordinal: 2,
            producer_instance_id: "i-reduced-wrong-query".into(),
            root: &wrong_query_outbox,
            scratch_root: &restore_scratch,
            selected_contract: Some(&selected_file.contract),
        })
        .unwrap();
    let wrong_query_ready = wrong_query_writer
        .commit_ground_truth(&wrong_query_heaps, 128)
        .unwrap();
    let wrong_query_staged = directory.path().join("wrong-query-staged");
    stage_checkpoint_head(&wrong_query_outbox, &wrong_query_ready, &wrong_query_staged);
    let wrong_query_head = load_v36_prefix_checkpoint_head(&wrong_query_staged, &context).unwrap();
    let wrong_query_resume_outbox = directory.path().join("wrong-query-resume-outbox");
    fs::create_dir(&wrong_query_resume_outbox).unwrap();
    assert!(
        V36PrefixPopulationCheckpointWriter::resume_phase(V36PrefixPhaseResumeRequest {
            execution_authority_sha256: "7".repeat(64),
            head: wrong_query_head,
            context: context.clone(),
            limits: &limits,
            producer_attempt_id: "v36-prefix-screen-reduced-attempt-0002".into(),
            producer_attempt_ordinal: 2,
            producer_instance_id: "i-reduced-wrong-query".into(),
            root: &wrong_query_resume_outbox,
            scratch_root: &restore_scratch,
            selected_contract: Some(&selected_file.contract),
        })
        .is_err()
    );
    let same_attempt_outbox = directory.path().join("same-attempt-resume-outbox");
    fs::create_dir(&same_attempt_outbox).unwrap();
    assert!(
        V36PrefixPopulationCheckpointWriter::resume_phase(V36PrefixPhaseResumeRequest {
            execution_authority_sha256: "7".repeat(64),
            head: materialized_head.clone(),
            context: context.clone(),
            limits: &limits,
            producer_attempt_id: "v36-prefix-screen-reduced-attempt-0001".into(),
            producer_attempt_ordinal: 1,
            producer_instance_id: "i-reduced-c".into(),
            root: &same_attempt_outbox,
            scratch_root: &restore_scratch,
            selected_contract: Some(&selected_file.contract),
        })
        .is_err()
    );
    let phase_resumed_outbox = directory.path().join("phase-resumed-outbox");
    fs::create_dir(&phase_resumed_outbox).unwrap();
    let (mut phase_resumed_writer, phase_resumed_state) =
        V36PrefixPopulationCheckpointWriter::resume_phase(V36PrefixPhaseResumeRequest {
            execution_authority_sha256: "7".repeat(64),
            head: materialized_head.clone(),
            context: context.clone(),
            limits: &limits,
            producer_attempt_id: "v36-prefix-screen-reduced-attempt-0002".into(),
            producer_attempt_ordinal: 2,
            producer_instance_id: "i-reduced-c".into(),
            root: &phase_resumed_outbox,
            scratch_root: &restore_scratch,
            selected_contract: Some(&selected_file.contract),
        })
        .unwrap();
    assert!(matches!(
        phase_resumed_state,
        V36PrefixCheckpointResumeState::Materialized { .. }
    ));
    let phase_resumed_ready = phase_resumed_writer
        .commit_ground_truth(&heaps, context.corpus_rows)
        .unwrap();
    let phase_resumed_staged = directory.path().join("phase-resumed-staged");
    stage_checkpoint_head(
        &phase_resumed_outbox,
        &phase_resumed_ready,
        &phase_resumed_staged,
    );
    let phase_resumed_head =
        load_v36_prefix_checkpoint_head(&phase_resumed_staged, &context).unwrap();
    assert_eq!(phase_resumed_head.manifest.generation, 3);
    assert_eq!(
        phase_resumed_head.manifest.producer_attempt_id,
        "v36-prefix-screen-reduced-attempt-0002"
    );
    assert_eq!(phase_resumed_head.dependencies.len(), 9);

    let ground_truth_ready = writer.commit_ground_truth(&heaps, 128).unwrap();
    let staged_ground_truth = directory.path().join("staged-ground-truth");
    stage_checkpoint_head(&resumed_outbox, &ground_truth_ready, &staged_ground_truth);
    let ground_truth_head =
        load_v36_prefix_checkpoint_head(&staged_ground_truth, &context).unwrap();
    let V36PrefixCheckpointResumeState::GroundTruth {
        artifacts: restored_artifacts,
        heaps: restored_heaps,
        next_source_ordinal,
        population: resumed_population,
        selected: resumed_selected,
    } = restore_v36_prefix_checkpoint_phase(
        &ground_truth_head,
        Some(&selected_file.contract),
        &limits,
        &restore_scratch,
    )
    .unwrap()
    else {
        panic!("ground-truth checkpoint resumed at the wrong phase")
    };
    assert_eq!(next_source_ordinal, 128);
    assert_eq!(resumed_population.distinct_rows, 144);
    assert_eq!(resumed_selected.identity, selection.selected_ids);
    assert_eq!(restored_artifacts.source.identity, artifacts.source);
    assert_eq!(restored_heaps.identity, heaps.identity);
    assert_eq!(
        restored_heaps.path,
        staged_ground_truth
            .join("objects")
            .join(format!("{}.blob", heaps.identity.sha256))
    );
    let ground_truth_resume_outbox = directory.path().join("ground-truth-resume-outbox");
    fs::create_dir(&ground_truth_resume_outbox).unwrap();
    let (_, ground_truth_resume_state) =
        V36PrefixPopulationCheckpointWriter::resume_phase(V36PrefixPhaseResumeRequest {
            execution_authority_sha256: "8".repeat(64),
            head: ground_truth_head.clone(),
            context: context.clone(),
            limits: &limits,
            producer_attempt_id: "v36-prefix-screen-reduced-attempt-0002".into(),
            producer_attempt_ordinal: 2,
            producer_instance_id: "i-reduced-d".into(),
            root: &ground_truth_resume_outbox,
            scratch_root: &restore_scratch,
            selected_contract: Some(&selected_file.contract),
        })
        .unwrap();
    assert!(matches!(
        ground_truth_resume_state,
        V36PrefixCheckpointResumeState::GroundTruth { .. }
    ));

    let mut rewritten_ground_truth = ground_truth_head.clone();
    let borsuk::V36PrefixCheckpointPhase::GroundTruth {
        next_source_ordinal,
        ..
    } = &mut rewritten_ground_truth.manifest.phase
    else {
        unreachable!()
    };
    *next_source_ordinal = 256;
    let rewritten_manifest_bytes =
        canonical_v36_prefix_checkpoint_manifest_bytes(&rewritten_ground_truth.manifest).unwrap();
    let rewritten_manifest_sha256 = format!("{:x}", Sha256::digest(&rewritten_manifest_bytes));
    let rewritten_manifest_blake3 = blake3::hash(&rewritten_manifest_bytes).to_hex().to_string();
    let mut rewritten_pointer: V36PrefixCheckpointPointer =
        serde_json::from_slice(&rewritten_ground_truth.pointer_bytes).unwrap();
    rewritten_pointer.manifest = V36ArtifactIdentity {
        role: "checkpoint-manifest".into(),
        uri: format!("{object_prefix}{rewritten_manifest_sha256}-checkpoint-manifest.json"),
        encoded_bytes: rewritten_manifest_bytes.len() as u64,
        sha256: rewritten_manifest_sha256,
        blake3: rewritten_manifest_blake3,
    };
    rewritten_ground_truth.pointer_bytes =
        canonical_v36_prefix_checkpoint_pointer_bytes(&context, &rewritten_pointer).unwrap();
    assert!(
        restore_v36_prefix_checkpoint_phase(
            &rewritten_ground_truth,
            Some(&selected_file.contract),
            &limits,
            &restore_scratch,
        )
        .is_err()
    );

    let mut reordered_ground_truth = ground_truth_head.clone();
    let last = reordered_ground_truth.dependencies.len() - 1;
    reordered_ground_truth.dependencies.swap(last - 1, last);
    assert!(
        restore_v36_prefix_checkpoint_phase(
            &reordered_ground_truth,
            Some(&selected_file.contract),
            &limits,
            &restore_scratch,
        )
        .is_err()
    );
    let substituted_heaps_path = directory.path().join("substituted-heaps.arrow");
    fs::copy(&restored_heaps.path, &substituted_heaps_path).unwrap();
    let mut substituted_ground_truth = ground_truth_head.clone();
    substituted_ground_truth.dependencies[last].path = substituted_heaps_path;
    assert!(
        restore_v36_prefix_checkpoint_phase(
            &substituted_ground_truth,
            Some(&selected_file.contract),
            &limits,
            &restore_scratch,
        )
        .is_err()
    );
    fs::remove_file(&restored_heaps.path).unwrap();
    assert!(
        restore_v36_prefix_checkpoint_phase(
            &ground_truth_head,
            Some(&selected_file.contract),
            &limits,
            &restore_scratch,
        )
        .is_err()
    );
    assert!(selection_scratch.read_dir().unwrap().next().is_none());
    assert!(materialization_scratch.read_dir().unwrap().next().is_none());
}

#[test]
fn v36_prefix_dataset_external_materialization_rejects_rehashed_semantic_drift() {
    for mutation in [
        RoleAssignmentMutation::FeatureId,
        RoleAssignmentMutation::RoleOrdinal,
    ] {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source-input.parquet");
        let feature_ids = (1_i64..=24).collect::<Vec<_>>();
        let ranked = vec![write_registered_rows(&source, &feature_ids)];
        let manifest = ranked_source_manifest_sha256(&ranked);
        let (selected, _) = external_selected_file_with_manifest(
            directory.path(),
            &(1_u64..=24).collect::<Vec<_>>(),
            &manifest,
        );
        let contract = reduced_role_assignment_contract(&selected);
        let assignment_scratch = directory.path().join("assignment-scratch");
        fs::create_dir(&assignment_scratch).unwrap();
        let assignment_path = directory.path().join("role-assignments.arrow");
        let limits = external_selection_limits();
        let receipt = assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
            contract: &contract,
            limits: &limits,
            output: &assignment_path,
            output_uri_prefix: "s3://fixture/v36",
            scratch_root: &assignment_scratch,
            selected: &selected,
        })
        .unwrap();
        let mut assignment = V36PrefixRoleAssignmentFile {
            contract,
            identity: receipt.identity,
            path: assignment_path,
        };
        rewrite_role_assignment_file(&mut assignment, mutation);
        let scratch = directory.path().join("materialization-scratch");
        let output = directory.path().join("output");
        fs::create_dir(&scratch).unwrap();
        fs::create_dir(&output).unwrap();

        assert!(
            materialize_v36_prefix_assigned_roles(V36PrefixExternalMaterializationRequest {
                assignment: &assignment,
                limits: &limits,
                output: &output,
                ranked_objects: &ranked,
                scratch_root: &scratch,
                source_paths: std::slice::from_ref(&source),
            })
            .is_err()
        );
        assert!(output.read_dir().unwrap().next().is_none());
        assert!(scratch.read_dir().unwrap().next().is_none());
    }
}

#[test]
fn v36_prefix_dataset_external_materialization_rejects_source_drift_and_clobber() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source-input.parquet");
    let ranked = vec![write_registered_rows(
        &source,
        &(1_i64..=24).collect::<Vec<_>>(),
    )];
    let manifest = ranked_source_manifest_sha256(&ranked);
    let (selected, _) = external_selected_file_with_manifest(
        directory.path(),
        &(1_u64..=24).collect::<Vec<_>>(),
        &manifest,
    );
    let contract = reduced_role_assignment_contract(&selected);
    let assignment_scratch = directory.path().join("assignment-scratch");
    fs::create_dir(&assignment_scratch).unwrap();
    let assignment_path = directory.path().join("role-assignments.arrow");
    let limits = external_selection_limits();
    let receipt = assign_v36_prefix_roles_from_selected_file(V36PrefixRoleAssignmentRequest {
        contract: &contract,
        limits: &limits,
        output: &assignment_path,
        output_uri_prefix: "s3://fixture/v36",
        scratch_root: &assignment_scratch,
        selected: &selected,
    })
    .unwrap();
    let assignment = V36PrefixRoleAssignmentFile {
        contract,
        identity: receipt.identity,
        path: assignment_path,
    };
    let scratch = directory.path().join("materialization-scratch");
    let output = directory.path().join("output");
    fs::create_dir(&scratch).unwrap();
    let mut drifted = ranked.clone();
    drifted[0].path.push_str("-drift");
    assert!(
        materialize_v36_prefix_assigned_roles(V36PrefixExternalMaterializationRequest {
            assignment: &assignment,
            limits: &limits,
            output: &output,
            ranked_objects: &drifted,
            scratch_root: &scratch,
            source_paths: std::slice::from_ref(&source),
        })
        .is_err()
    );
    assert!(!output.exists());
    let tiny_scratch_limits = V36PrefixExternalSelectionLimits {
        max_scratch_bytes: assignment.identity.encoded_bytes + 1,
        ..external_selection_limits()
    };
    let error = materialize_v36_prefix_assigned_roles(V36PrefixExternalMaterializationRequest {
        assignment: &assignment,
        limits: &tiny_scratch_limits,
        output: &output,
        ranked_objects: &ranked,
        scratch_root: &scratch,
        source_paths: std::slice::from_ref(&source),
    })
    .unwrap_err();
    assert_eq!(error.code(), "v36_prefix_resource_limit");
    assert!(!output.exists());
    assert!(scratch.read_dir().unwrap().next().is_none());
    let oversized_io_limits = V36PrefixExternalSelectionLimits {
        io_buffer_bytes: usize::MAX,
        ..external_selection_limits()
    };
    let error = materialize_v36_prefix_assigned_roles(V36PrefixExternalMaterializationRequest {
        assignment: &assignment,
        limits: &oversized_io_limits,
        output: &output,
        ranked_objects: &ranked,
        scratch_root: &scratch,
        source_paths: std::slice::from_ref(&source),
    })
    .unwrap_err();
    assert_eq!(error.code(), "v36_prefix_resource_limit");
    assert!(!output.exists());
    assert!(scratch.read_dir().unwrap().next().is_none());
    fs::create_dir(&output).unwrap();
    fs::write(output.join("source.parquet"), b"preserve").unwrap();
    assert!(
        materialize_v36_prefix_assigned_roles(V36PrefixExternalMaterializationRequest {
            assignment: &assignment,
            limits: &limits,
            output: &output,
            ranked_objects: &ranked,
            scratch_root: &scratch,
            source_paths: std::slice::from_ref(&source),
        })
        .is_err()
    );
    assert_eq!(
        fs::read(output.join("source.parquet")).unwrap(),
        b"preserve"
    );
    assert!(scratch.read_dir().unwrap().next().is_none());
}

#[test]
fn v36_prefix_dataset_scans_complete_window_after_cutoff_and_records_duplicate_evidence() {
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
        0,
        ranked.iter().map(|object| object.encoded_bytes).sum(),
        3,
        |ordinal, _| Ok(paths[usize::from(ordinal)].clone()),
    )
    .unwrap();
    assert_eq!(scan.unique_rows.len(), 4);
    assert_eq!(scan.consumed_objects.len(), 2);
    assert_eq!(scan.cutoff_object_ordinal, 1);
    assert_eq!(scan.cutoff_row_offset, 1);
    assert_eq!(scan.physical_rows, 5);
    assert_eq!(scan.distinct_rows_observed, 4);
    assert_eq!(scan.duplicate_rows, 1);

    assert!(
        scan_v36_prefix_object_prefix(&ranked[..1], 0, u64::MAX, 3, |ordinal, _| Ok(paths
            [usize::from(ordinal)]
        .clone()))
        .is_err()
    );
}

#[test]
fn v36_prefix_dataset_scans_exact_cohort_b_window_with_global_ordinals() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("sixteen.parquet");
    let second = directory.path().join("seventeen.parquet");
    let ranked = vec![
        write_registered_rows(&first, &[7, 9]),
        write_registered_rows(&second, &[11, 13]),
    ];
    let paths = [first, second];
    let mut acquired = Vec::new();
    let scan = scan_v36_prefix_object_prefix(
        &ranked,
        16,
        ranked.iter().map(|object| object.encoded_bytes).sum(),
        2,
        |ordinal, _| {
            acquired.push(ordinal);
            Ok(paths[usize::from(ordinal - 16)].clone())
        },
    )
    .unwrap();
    assert_eq!(acquired, vec![16, 17]);
    assert_eq!(scan.consumed_objects.len(), 2);
    assert_eq!(scan.distinct_rows_observed, 4);
    assert_eq!(
        scan.unique_rows
            .iter()
            .map(|row| row.selected_object_ordinal)
            .collect::<Vec<_>>(),
        vec![16, 16, 17, 17]
    );
}

#[test]
fn v36_prefix_dataset_rejects_window_that_exceeds_byte_cap() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.parquet");
    let second = directory.path().join("second.parquet");
    let ranked = vec![
        write_registered_rows(&first, &[7, 9]),
        write_registered_rows(&second, &[11, 13]),
    ];
    let paths = [first, second];
    let mut acquired = Vec::new();
    let result =
        scan_v36_prefix_object_prefix(&ranked, 0, ranked[0].encoded_bytes, 2, |ordinal, _| {
            acquired.push(ordinal);
            Ok(paths[usize::from(ordinal)].clone())
        });
    assert!(result.is_err());
    assert_eq!(acquired, vec![0]);
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
        0,
        ranked.iter().map(|object| object.encoded_bytes).sum(),
        3,
        |ordinal, _| Ok(paths[usize::from(ordinal)].clone()),
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
            0,
            u64::MAX,
            3,
            |ordinal, _| Ok(invalid_paths[usize::from(ordinal)].clone()),
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
        0,
        u64::MAX,
        3,
        |ordinal, _| Ok(paths[usize::from(ordinal)].clone()),
        |commit| {
            uninterrupted_commits.push(commit.clone());
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(uninterrupted_commits.len(), 3);
    assert_eq!(
        uninterrupted_commits
            .iter()
            .map(|commit| commit.cutoff)
            .collect::<Vec<_>>(),
        vec![None, Some((1, 1)), None]
    );

    let mut first_commits = Vec::new();
    assert!(
        scan_v36_prefix_object_prefix_checkpointed(
            &ranked,
            0,
            ranked[0].encoded_bytes,
            3,
            |ordinal, _| Ok(paths[usize::from(ordinal)].clone()),
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
        0,
        u64::MAX,
        3,
        &prior_runs,
        |ordinal, _| {
            acquired.push(ordinal);
            Ok(paths[usize::from(ordinal)].clone())
        },
        |_| Ok(()),
    )
    .unwrap();
    assert_eq!(acquired, vec![1, 2]);
    assert_eq!(resumed, uninterrupted);

    let complete_runs = uninterrupted_commits
        .iter()
        .map(|commit| commit.run.clone())
        .collect::<Vec<_>>();
    let mut acquired_after_cutoff = Vec::new();
    let resumed_complete = scan_v36_prefix_object_prefix_resumed(
        &ranked,
        0,
        u64::MAX,
        3,
        &complete_runs,
        |ordinal, _| {
            acquired_after_cutoff.push(ordinal);
            Ok(paths[usize::from(ordinal)].clone())
        },
        |_| Ok(()),
    )
    .unwrap();
    assert!(acquired_after_cutoff.is_empty());
    assert_eq!(resumed_complete, uninterrupted);

    let completed_window_bytes = ranked
        .iter()
        .map(|object| object.encoded_bytes)
        .sum::<u64>();
    assert!(
        scan_v36_prefix_object_prefix_resumed(
            &ranked,
            0,
            completed_window_bytes - 1,
            3,
            &complete_runs,
            |_, _| panic!("a complete restored window must not reacquire source objects"),
            |_| Ok(()),
        )
        .is_err()
    );
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
fn v36_prefix_dataset_materializes_cohort_b_roles_with_global_ordinals() {
    let directory = tempfile::tempdir().unwrap();
    let first = directory.path().join("first.parquet");
    let second = directory.path().join("second.parquet");
    let ranked = vec![
        write_registered_rows(&first, &[7, 9, 11]),
        write_registered_rows(&second, &[13, 15, 17]),
    ];
    let split = borsuk::V36PrefixRoleSplit {
        corpus: vec![identity(17, 17, 2, Some(0)), identity(7, 16, 0, Some(1))],
        development: vec![identity(9, 16, 1, None)],
        validation: vec![identity(11, 16, 2, None)],
        sealed_holdout: vec![identity(13, 17, 0, None)],
        performance: vec![identity(15, 17, 1, None)],
    };
    let output = directory.path().join("output");
    let scratch = directory.path().join("scratch");
    fs::create_dir(&output).unwrap();
    fs::create_dir(&scratch).unwrap();
    let paths = materialize_v36_prefix_role_parquets(
        &[first, second],
        &ranked,
        16,
        &split,
        &scratch,
        &output,
    )
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
