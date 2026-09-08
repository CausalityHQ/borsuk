//! Durable V36 prefix-freeze checkpoint and resume-state contracts.

use std::io::Cursor;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema};
use borsuk::{
    V36ArtifactIdentity, V36PrefixCheckpointContext, V36PrefixCheckpointDependencyFile,
    V36PrefixCheckpointManifest, V36PrefixCheckpointOutbox, V36PrefixCheckpointPhase,
    V36PrefixCheckpointPointer, V36PrefixCheckpointPointerCondition, V36PrefixIdentityRun,
    V36PrefixIdentityRunFile, V36PrefixMaterializedArtifacts, V36PrefixPopulationCheckpoint,
    V36PrefixPopulationCheckpointWriter, V36PrefixPopulationCommit, V36PrefixPopulationFileCommit,
    V36PrefixPopulationSelection, V36PrefixRegisteredSourceObject, V36PrefixRowIdentity,
    V36PrefixSourceObject, canonical_v36_prefix_checkpoint_manifest_bytes,
    canonical_v36_prefix_checkpoint_pointer_bytes, decode_v36_prefix_identity_run,
    encode_v36_prefix_identity_run, load_v36_prefix_checkpoint_head,
    plan_v36_prefix_checkpoint_dependency_closure, plan_v36_prefix_checkpoint_publication,
    restore_v36_prefix_population, restore_v36_prefix_population_state,
    validate_v36_prefix_checkpoint_manifest_with_context,
    validate_v36_prefix_checkpoint_pointer_observation, validate_v36_prefix_checkpoint_transition,
};
use sha2::{Digest, Sha256};

fn artifact(role: &str, filename: &str, byte: char) -> V36ArtifactIdentity {
    let digest = byte.to_string().repeat(64);
    V36ArtifactIdentity {
        blake3: digest.clone(),
        encoded_bytes: 1_024,
        role: role.to_owned(),
        sha256: digest.clone(),
        uri: format!("s3://fixture/v36/checkpoints/objects/{digest}-{filename}"),
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
            "s3://fixture/v36/checkpoints/objects/{sha256}-population-identity-run-{ordinal:04}.arrow"
        ),
    }
}

fn checkpoint_artifact_bytes(role: &str, filename: &str, bytes: &[u8]) -> V36ArtifactIdentity {
    let sha256 = format!("{:x}", Sha256::digest(bytes));
    V36ArtifactIdentity {
        blake3: blake3::hash(bytes).to_hex().to_string(),
        encoded_bytes: bytes.len().try_into().unwrap(),
        role: role.to_owned(),
        sha256: sha256.clone(),
        uri: format!("s3://fixture/v36/checkpoints/objects/{sha256}-{filename}"),
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
    source_object_at(0)
}

fn source_object_at(ordinal: u16) -> V36PrefixSourceObject {
    let path = format!("data/part-{ordinal:04}.parquet");
    let encoded_bytes = 2_048_u64 + u64::from(ordinal);
    let mut sample = Sha256::new();
    sample.update(b"borsuk-v36-screen-object-v1");
    sample.update(path.as_bytes());
    sample.update(encoded_bytes.to_le_bytes());
    V36PrefixSourceObject {
        blake3: "6".repeat(64),
        encoded_bytes,
        path,
        sample_sha256: format!("{:x}", sample.finalize()),
        sha256: if ordinal == 0 {
            "8".repeat(64)
        } else {
            "7".repeat(64)
        },
        uri: format!("https://example.invalid/data/part-{ordinal:04}.parquet"),
    }
}

fn registered_source() -> V36PrefixRegisteredSourceObject {
    registered_source_at(0)
}

fn registered_source_at(ordinal: u16) -> V36PrefixRegisteredSourceObject {
    let source = source_object_at(ordinal);
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
            completed_objects: 1,
            consumed_objects: vec![source_object()],
            distinct_rows: 80,
            duplicate_rows: 20,
            identity_runs: vec![artifact(
                "population-identity-run-0000",
                "population-identity-run-0000.arrow",
                '9',
            )],
            physical_rows: 100,
            selected_object_count: 1,
            selected_object_start: 0,
        },
        previous_checkpoint: None,
        producer_attempt_id: "v36-prefix-screen-fixture-attempt-0000".into(),
        producer_attempt_ordinal: 0,
        producer_instance_id: "i-fixture".into(),
        run_id: "v36-prefix-screen-fixture".into(),
        schema: "borsuk-v36-prefix-freeze-checkpoint-v2".into(),
        source_archive_sha256: "3".repeat(64),
        source_commit: "4".repeat(40),
        source_registry_sha256: "5".repeat(64),
    }
}

fn checkpoint_context(distinct_candidates: u64) -> V36PrefixCheckpointContext {
    V36PrefixCheckpointContext {
        cohort_ordinal: 0,
        corpus_rows: 50,
        distinct_candidates,
        excluded_population_identity: None,
        freeze_authority_sha256: "2".repeat(64),
        gt_block_rows: 16,
        object_prefix: "s3://fixture/v36/checkpoints/objects/".into(),
        pointer_uri: "s3://fixture/v36/checkpoints/runs/v36-prefix-screen-fixture/latest.json"
            .into(),
        ranked_objects: vec![registered_source()],
        run_id: "v36-prefix-screen-fixture".into(),
        selected_object_count: 1,
        selected_object_start: 0,
        source_archive_sha256: "3".repeat(64),
        source_byte_cap: 8_192,
        source_commit: "4".repeat(40),
        source_registry_sha256: "5".repeat(64),
    }
}

fn two_object_checkpoint_context(distinct_candidates: u64) -> V36PrefixCheckpointContext {
    let mut context = checkpoint_context(distinct_candidates);
    context.selected_object_count = 2;
    context.ranked_objects.push(registered_source_at(1));
    context
}

fn three_object_checkpoint_context(distinct_candidates: u64) -> V36PrefixCheckpointContext {
    let mut context = two_object_checkpoint_context(distinct_candidates);
    context.selected_object_count = 3;
    context.ranked_objects.push(registered_source_at(2));
    context
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

fn checkpoint_identity(manifest: &V36PrefixCheckpointManifest) -> V36ArtifactIdentity {
    let bytes = canonical_v36_prefix_checkpoint_manifest_bytes(manifest).unwrap();
    let sha256 = format!("{:x}", Sha256::digest(&bytes));
    V36ArtifactIdentity {
        blake3: blake3::hash(&bytes).to_hex().to_string(),
        encoded_bytes: bytes.len().try_into().unwrap(),
        role: "checkpoint-manifest".into(),
        sha256: sha256.clone(),
        uri: format!(
            "s3://fixture/v36/checkpoints/objects/{sha256}-checkpoint-{:08}.json",
            manifest.generation
        ),
    }
}

fn population_selection() -> V36PrefixPopulationSelection {
    V36PrefixPopulationSelection {
        cutoff_feature_row_id: 41,
        cutoff_score_sha256: "a".repeat(64),
        eligible_rows: 80,
        excluded_rows: 0,
        excluded_population_identity: None,
        selected_ids: artifact(
            "population-selected-identities",
            "population-selected-identities.arrow",
            'b',
        ),
        selected_rows: 80,
    }
}

#[test]
fn v36_prefix_checkpoint_selected_phase_binds_complete_window_and_selection() {
    let selection = population_selection();
    let phase = V36PrefixCheckpointPhase::Selected {
        selection: selection.clone(),
    };
    let value = serde_json::to_value(&phase).unwrap();
    assert_eq!(value["kind"], "selected");
    assert_eq!(value["selection"], serde_json::to_value(selection).unwrap());
}

#[test]
fn v36_prefix_checkpoint_selected_phase_requires_complete_reconciled_selection() {
    let mut manifest = population_manifest();
    manifest.phase = V36PrefixCheckpointPhase::Selected {
        selection: population_selection(),
    };
    validate_v36_prefix_checkpoint_manifest_with_context(&checkpoint_context(80), &manifest)
        .unwrap();

    let V36PrefixCheckpointPhase::Selected { selection } = &mut manifest.phase else {
        unreachable!()
    };
    selection.eligible_rows = 79;
    assert!(
        validate_v36_prefix_checkpoint_manifest_with_context(&checkpoint_context(80), &manifest)
            .is_err()
    );

    let mut incomplete = population_manifest();
    incomplete.population.selected_object_count = 2;
    incomplete.phase = V36PrefixCheckpointPhase::Selected {
        selection: population_selection(),
    };
    let mut context = checkpoint_context(80);
    context.selected_object_count = 2;
    context.ranked_objects.push(registered_source_at(1));
    assert!(validate_v36_prefix_checkpoint_manifest_with_context(&context, &incomplete).is_err());
}

#[test]
fn v36_prefix_checkpoint_selection_binds_cohort_exclusion_evidence() {
    let exclusion = artifact(
        "population-selected-identities",
        "cohort-a-population-selected-identities.arrow",
        'd',
    );
    let mut cohort_a = population_manifest();
    let mut invalid_a = population_selection();
    invalid_a.eligible_rows = 79;
    invalid_a.excluded_rows = 1;
    invalid_a.excluded_population_identity = Some(exclusion.clone());
    cohort_a.phase = V36PrefixCheckpointPhase::Selected {
        selection: invalid_a,
    };
    assert!(
        validate_v36_prefix_checkpoint_manifest_with_context(&checkpoint_context(80), &cohort_a)
            .is_err()
    );

    let mut cohort_b = population_manifest();
    cohort_b.population.distinct_rows = 100;
    cohort_b.population.duplicate_rows = 0;
    cohort_b.population.selected_object_start = 1;
    cohort_b.population.identity_runs[0].role = "population-identity-run-0001".into();
    let mut valid_b = population_selection();
    valid_b.excluded_rows = 20;
    valid_b.excluded_population_identity = Some(exclusion.clone());
    cohort_b.phase = V36PrefixCheckpointPhase::Selected { selection: valid_b };
    let mut context = checkpoint_context(80);
    context.cohort_ordinal = 1;
    context.excluded_population_identity = Some(exclusion);
    context.selected_object_start = 1;
    validate_v36_prefix_checkpoint_manifest_with_context(&context, &cohort_b).unwrap();

    context
        .excluded_population_identity
        .as_mut()
        .unwrap()
        .sha256 = "e".repeat(64);
    assert!(validate_v36_prefix_checkpoint_manifest_with_context(&context, &cohort_b).is_err());
}

#[test]
fn v36_prefix_checkpoint_population_v2_rejects_physical_cutoff_state() {
    let population = serde_json::json!({
        "completed_objects": 1,
        "consumed_objects": [source_object()],
        "distinct_rows": 80,
        "duplicate_rows": 20,
        "identity_runs": [artifact(
            "population-identity-run-0000",
            "population-identity-run-0000.arrow",
            '9',
        )],
        "physical_rows": 100,
        "selected_object_count": 1,
        "selected_object_start": 0,
    });
    let decoded: V36PrefixPopulationCheckpoint =
        serde_json::from_value(population.clone()).unwrap();
    assert_eq!(decoded.completed_objects, 1);
    assert_eq!(decoded.selected_object_start, 0);
    assert_eq!(decoded.selected_object_count, 1);

    let mut legacy = population;
    legacy["cutoff_object_ordinal"] = serde_json::json!(0);
    legacy["cutoff_row_offset"] = serde_json::json!(79);
    assert!(serde_json::from_value::<V36PrefixPopulationCheckpoint>(legacy).is_err());
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
    manifest.generation = 1;
    manifest.previous_checkpoint = Some(artifact(
        "checkpoint-manifest",
        "checkpoint-00000000.json",
        'a',
    ));
    manifest.phase = V36PrefixCheckpointPhase::GroundTruth {
        selection: population_selection(),
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
fn v36_prefix_checkpoint_generic_dependency_closure_is_exact_and_ordered() {
    let population = population_manifest();
    let population_run = population.population.identity_runs[0].clone();
    assert_eq!(
        plan_v36_prefix_checkpoint_dependency_closure(&population).unwrap(),
        vec![population_run.clone()]
    );

    let selection = population_selection();
    let mut selected = population.clone();
    selected.phase = V36PrefixCheckpointPhase::Selected {
        selection: selection.clone(),
    };
    assert_eq!(
        plan_v36_prefix_checkpoint_dependency_closure(&selected).unwrap(),
        vec![population_run.clone(), selection.selected_ids.clone()]
    );

    let materialized = materialized_artifacts();
    let materialized_dependencies = [
        materialized.population_authority.clone(),
        materialized.source.clone(),
        materialized.development_query.clone(),
        materialized.validation_query.clone(),
        materialized.sealed_holdout_query.clone(),
        materialized.performance_query.clone(),
    ];
    let mut materialized_manifest = population.clone();
    materialized_manifest.phase = V36PrefixCheckpointPhase::Materialized {
        artifacts: materialized.clone(),
        selection: selection.clone(),
    };
    let mut expected = vec![population_run.clone(), selection.selected_ids.clone()];
    expected.extend(materialized_dependencies.clone());
    assert_eq!(
        plan_v36_prefix_checkpoint_dependency_closure(&materialized_manifest).unwrap(),
        expected
    );

    let heaps = artifact("gt-heaps", "gt-heaps-00000048.arrow", 'b');
    let mut ground_truth = population;
    ground_truth.phase = V36PrefixCheckpointPhase::GroundTruth {
        heaps: heaps.clone(),
        materialized,
        next_source_ordinal: 48,
        selection: selection.clone(),
    };
    let mut expected = vec![population_run, selection.selected_ids];
    expected.extend(materialized_dependencies);
    expected.push(heaps);
    assert_eq!(
        plan_v36_prefix_checkpoint_dependency_closure(&ground_truth).unwrap(),
        expected
    );

    let V36PrefixCheckpointPhase::GroundTruth { heaps, .. } = &mut ground_truth.phase else {
        unreachable!()
    };
    heaps.uri = ground_truth.population.identity_runs[0].uri.clone();
    assert!(plan_v36_prefix_checkpoint_dependency_closure(&ground_truth).is_err());
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
fn v36_prefix_checkpoint_context_binds_exact_registered_object_window() {
    let manifest = population_manifest();
    let context = checkpoint_context(100);
    assert_eq!(context.selected_object_start, 0);
    assert_eq!(context.selected_object_count, 1);
    validate_v36_prefix_checkpoint_manifest_with_context(&context, &manifest).unwrap();

    let mut wrong_window = context;
    wrong_window.selected_object_start = 1;
    assert!(
        validate_v36_prefix_checkpoint_manifest_with_context(&wrong_window, &manifest).is_err()
    );
}

#[test]
fn v36_prefix_checkpoint_transition_is_monotonic_across_attempts() {
    let context = checkpoint_context(80);
    let previous = population_manifest();
    let mut next = previous.clone();
    next.generation = 1;
    next.previous_checkpoint = Some(checkpoint_identity(&previous));
    next.producer_attempt_id = "v36-prefix-screen-fixture-attempt-0001".into();
    next.producer_attempt_ordinal = 1;
    next.phase = V36PrefixCheckpointPhase::Selected {
        selection: population_selection(),
    };
    validate_v36_prefix_checkpoint_transition(&context, &previous, &next).unwrap();

    let mut skipped_materialization = next.clone();
    skipped_materialization.phase = V36PrefixCheckpointPhase::GroundTruth {
        selection: population_selection(),
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
fn v36_prefix_checkpoint_transition_requires_selected_phase_and_selection_lineage() {
    let context = checkpoint_context(80);
    let population = population_manifest();
    let mut selected = population.clone();
    selected.generation = 1;
    selected.previous_checkpoint = Some(checkpoint_identity(&population));
    selected.phase = V36PrefixCheckpointPhase::Selected {
        selection: population_selection(),
    };
    validate_v36_prefix_checkpoint_transition(&context, &population, &selected).unwrap();

    let mut skipped = selected.clone();
    skipped.phase = V36PrefixCheckpointPhase::Materialized {
        selection: population_selection(),
        artifacts: materialized_artifacts(),
    };
    assert!(validate_v36_prefix_checkpoint_transition(&context, &population, &skipped).is_err());

    let mut materialized = selected.clone();
    materialized.generation = 2;
    materialized.previous_checkpoint = Some(checkpoint_identity(&selected));
    materialized.phase = V36PrefixCheckpointPhase::Materialized {
        selection: population_selection(),
        artifacts: materialized_artifacts(),
    };
    validate_v36_prefix_checkpoint_transition(&context, &selected, &materialized).unwrap();

    let mut changed_selection = materialized.clone();
    let V36PrefixCheckpointPhase::Materialized { selection, .. } = &mut changed_selection.phase
    else {
        unreachable!()
    };
    selection.cutoff_feature_row_id += 1;
    assert!(
        validate_v36_prefix_checkpoint_transition(&context, &selected, &changed_selection).is_err()
    );

    let mut ground_truth = materialized.clone();
    ground_truth.generation = 3;
    ground_truth.previous_checkpoint = Some(checkpoint_identity(&materialized));
    ground_truth.phase = V36PrefixCheckpointPhase::GroundTruth {
        selection: population_selection(),
        materialized: materialized_artifacts(),
        heaps: artifact("gt-heaps", "gt-heaps-00000016.arrow", 'c'),
        next_source_ordinal: 16,
    };
    validate_v36_prefix_checkpoint_transition(&context, &materialized, &ground_truth).unwrap();
}

#[test]
fn v36_prefix_checkpoint_publication_is_dependency_first_and_cas_fenced() {
    let context = checkpoint_context(80);
    let previous = population_manifest();
    let previous_identity = checkpoint_identity(&previous);
    let genesis = plan_v36_prefix_checkpoint_publication(&context, &previous, None, None).unwrap();
    assert_eq!(
        genesis.condition,
        V36PrefixCheckpointPointerCondition::Create
    );
    assert_eq!(genesis.dependencies.len(), 1);
    let current_pointer = V36PrefixCheckpointPointer {
        claim_eligible: false,
        generation: 0,
        manifest: previous_identity.clone(),
        producer_attempt_id: previous.producer_attempt_id.clone(),
        producer_attempt_ordinal: 0,
        run_id: previous.run_id.clone(),
        schema: "borsuk-v36-prefix-checkpoint-pointer-v2".into(),
    };
    let current_bytes =
        canonical_v36_prefix_checkpoint_pointer_bytes(&context, &current_pointer).unwrap();

    let mut next = previous.clone();
    next.generation = 1;
    next.previous_checkpoint = Some(previous_identity);
    let mut skipped_selection = next.clone();
    skipped_selection.phase = V36PrefixCheckpointPhase::Materialized {
        selection: population_selection(),
        artifacts: materialized_artifacts(),
    };
    assert!(
        plan_v36_prefix_checkpoint_publication(
            &context,
            &skipped_selection,
            Some(&previous),
            Some((&current_bytes, "etag-generation-zero")),
        )
        .is_err()
    );
    next.phase = V36PrefixCheckpointPhase::Selected {
        selection: population_selection(),
    };
    let plan = plan_v36_prefix_checkpoint_publication(
        &context,
        &next,
        Some(&previous),
        Some((&current_bytes, "etag-generation-zero")),
    )
    .unwrap();
    assert_eq!(
        plan.condition,
        V36PrefixCheckpointPointerCondition::Replace {
            etag: "etag-generation-zero".into()
        }
    );
    assert_eq!(plan.dependencies.len(), 2);
    assert_eq!(plan.dependencies[0].role, "population-identity-run-0000");
    assert_eq!(plan.dependencies[1].role, "population-selected-identities");
    assert_eq!(plan.manifest.role, "checkpoint-manifest");
    validate_v36_prefix_checkpoint_pointer_observation(&plan.pointer_bytes, &plan.pointer_bytes)
        .unwrap();
    let mut changed = plan.pointer_bytes;
    changed[0] ^= 1;
    assert!(validate_v36_prefix_checkpoint_pointer_observation(&current_bytes, &changed).is_err());

    assert!(
        plan_v36_prefix_checkpoint_publication(&context, &next, Some(&previous), None).is_err()
    );
    let mut wrong_pointer = current_pointer;
    wrong_pointer.manifest.sha256 = "f".repeat(64);
    let wrong_bytes = serde_json::to_vec(&wrong_pointer).unwrap();
    assert!(
        plan_v36_prefix_checkpoint_publication(
            &context,
            &next,
            Some(&previous),
            Some((&wrong_bytes, "etag-wrong")),
        )
        .is_err()
    );
}

#[test]
fn v36_prefix_checkpoint_pointer_v2_rejects_legacy_schema() {
    let context = checkpoint_context(100);
    let manifest = population_manifest();
    let mut pointer = V36PrefixCheckpointPointer {
        claim_eligible: false,
        generation: 0,
        manifest: checkpoint_identity(&manifest),
        producer_attempt_id: manifest.producer_attempt_id.clone(),
        producer_attempt_ordinal: 0,
        run_id: manifest.run_id.clone(),
        schema: "borsuk-v36-prefix-checkpoint-pointer-v2".into(),
    };
    canonical_v36_prefix_checkpoint_pointer_bytes(&context, &pointer).unwrap();
    pointer.schema = "borsuk-v36-prefix-checkpoint-pointer-v1".into();
    assert!(canonical_v36_prefix_checkpoint_pointer_bytes(&context, &pointer).is_err());
}

#[test]
fn v36_prefix_checkpoint_outbox_exposes_only_complete_generations() {
    let run = V36PrefixIdentityRun {
        physical_rows: 100,
        rows: vec![identity(7, 9, 0), identity(41, 2, 0)],
        selected_object_ordinal: 0,
        source: source_object(),
    };
    let run_bytes = encode_v36_prefix_identity_run(&run).unwrap();
    let run_identity = identity_run_artifact(&run_bytes, 0);
    let mut manifest = population_manifest();
    manifest.population.identity_runs = vec![run_identity.clone()];
    let plan =
        plan_v36_prefix_checkpoint_publication(&checkpoint_context(100), &manifest, None, None)
            .unwrap();
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("outbox");
    std::fs::create_dir(&root).unwrap();
    let outbox = V36PrefixCheckpointOutbox::create(&root).unwrap();
    let ready = outbox
        .commit(&plan, &[(run_identity.clone(), run_bytes.clone())])
        .unwrap();
    assert_eq!(ready.file_name().unwrap(), "generation-00000000.json");
    assert!(
        root.join("objects")
            .join(format!("{}.blob", run_identity.sha256))
            .is_file()
    );
    assert!(
        root.join("manifests")
            .join(format!("{}.json", plan.manifest.sha256))
            .is_file()
    );
    assert_eq!(std::fs::read(&ready).unwrap().last(), Some(&b'\n'),);
    let ready_value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&ready).unwrap()).unwrap();
    assert_eq!(
        ready_value["pointer_uri"],
        "s3://fixture/v36/checkpoints/runs/v36-prefix-screen-fixture/latest.json"
    );
    assert!(ready_value["previous_pointer_sha256"].is_null());

    let other = directory.path().join("other");
    std::fs::create_dir(&other).unwrap();
    let outbox = V36PrefixCheckpointOutbox::create(&other).unwrap();
    let mut corrupt = run_bytes.clone();
    corrupt[0] ^= 1;
    assert!(
        outbox
            .commit(&plan, &[(run_identity.clone(), corrupt)])
            .is_err()
    );
    assert!(other.join("commits").read_dir().unwrap().next().is_none());

    let file_root = directory.path().join("file-outbox");
    std::fs::create_dir(&file_root).unwrap();
    let dependency_path = directory.path().join("identity-run.arrow");
    std::fs::write(&dependency_path, &run_bytes).unwrap();
    let file_outbox = V36PrefixCheckpointOutbox::create(&file_root).unwrap();
    let ready = file_outbox
        .commit_files(
            &plan,
            &[V36PrefixCheckpointDependencyFile {
                identity: run_identity.clone(),
                path: dependency_path,
            }],
        )
        .unwrap();
    assert!(ready.is_file());
    assert_eq!(
        std::fs::read(
            file_root
                .join("objects")
                .join(format!("{}.blob", run_identity.sha256))
        )
        .unwrap(),
        run_bytes
    );

    let corrupt_file_root = directory.path().join("corrupt-file-outbox");
    std::fs::create_dir(&corrupt_file_root).unwrap();
    let corrupt_dependency_path = directory.path().join("corrupt-identity-run.arrow");
    std::fs::write(&corrupt_dependency_path, b"wrong").unwrap();
    let corrupt_file_outbox = V36PrefixCheckpointOutbox::create(&corrupt_file_root).unwrap();
    assert!(
        corrupt_file_outbox
            .commit_files(
                &plan,
                &[V36PrefixCheckpointDependencyFile {
                    identity: run_identity,
                    path: corrupt_dependency_path,
                }],
            )
            .is_err()
    );
    assert!(
        corrupt_file_root
            .join("commits")
            .read_dir()
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn v36_prefix_checkpoint_population_writer_commits_one_complete_object_generation() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("outbox");
    std::fs::create_dir(&root).unwrap();
    let context = two_object_checkpoint_context(3);
    let mut writer = V36PrefixPopulationCheckpointWriter::create(
        &root,
        context,
        "1".repeat(64),
        "v36-prefix-screen-fixture-attempt-0000".into(),
        0,
        "i-fixture".into(),
    )
    .unwrap();
    let commit = V36PrefixPopulationCommit {
        cutoff: None,
        distinct_rows: 2,
        duplicate_rows: 98,
        physical_rows: 100,
        run: V36PrefixIdentityRun {
            physical_rows: 100,
            rows: vec![identity(7, 9, 0), identity(41, 2, 0)],
            selected_object_ordinal: 0,
            source: source_object(),
        },
    };
    let ready = writer.commit(&commit).unwrap();
    let run_files = writer.identity_run_files().unwrap();
    assert_eq!(run_files.len(), 1);
    assert_eq!(run_files[0].selected_object_ordinal, 0);
    assert_eq!(run_files[0].source, source_object());
    assert_eq!(run_files[0].identity.role, "population-identity-run-0000");
    assert_eq!(
        run_files[0].path,
        root.join("objects")
            .join(format!("{}.blob", run_files[0].identity.sha256))
    );
    assert_eq!(ready.file_name().unwrap(), "generation-00000000.json");
    assert_eq!(root.join("commits").read_dir().unwrap().count(), 1);
    assert_eq!(root.join("objects").read_dir().unwrap().count(), 1);
    assert_eq!(root.join("manifests").read_dir().unwrap().count(), 1);
    assert_eq!(root.join("pointers").read_dir().unwrap().count(), 1);
    let ready_value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&ready).unwrap()).unwrap();
    let previous_identity: V36ArtifactIdentity =
        serde_json::from_value(ready_value["manifest"].clone()).unwrap();
    let previous_manifest: V36PrefixCheckpointManifest = serde_json::from_slice(
        &std::fs::read(
            root.join("manifests")
                .join(format!("{}.json", previous_identity.sha256)),
        )
        .unwrap(),
    )
    .unwrap();
    let previous_pointer_bytes = std::fs::read(root.join("pointers").join(format!(
        "{}.json",
        ready_value["pointer_sha256"].as_str().unwrap()
    )))
    .unwrap();
    let first_identity = previous_manifest.population.identity_runs[0].clone();
    let first_bytes = std::fs::read(
        root.join("objects")
            .join(format!("{}.blob", first_identity.sha256)),
    )
    .unwrap();
    let staged = directory.path().join("staged-resume");
    std::fs::create_dir(&staged).unwrap();
    std::fs::create_dir(staged.join("objects")).unwrap();
    std::fs::write(staged.join("pointer.json"), &previous_pointer_bytes).unwrap();
    std::fs::write(
        staged.join("manifest.json"),
        canonical_v36_prefix_checkpoint_manifest_bytes(&previous_manifest).unwrap(),
    )
    .unwrap();
    std::fs::write(
        staged
            .join("objects")
            .join(format!("{}.blob", first_identity.sha256)),
        &first_bytes,
    )
    .unwrap();
    let loaded =
        load_v36_prefix_checkpoint_head(&staged, &two_object_checkpoint_context(3)).unwrap();
    assert_eq!(loaded.manifest, previous_manifest);
    assert_eq!(loaded.pointer_bytes, previous_pointer_bytes);
    let staged_dependency = staged
        .join("objects")
        .join(format!("{}.blob", first_identity.sha256));
    assert_eq!(
        loaded.dependencies,
        vec![V36PrefixCheckpointDependencyFile {
            identity: first_identity.clone(),
            path: staged_dependency.clone(),
        }]
    );
    let inconsistent_root = directory.path().join("inconsistent-resume-outbox");
    std::fs::create_dir(&inconsistent_root).unwrap();
    let mut inconsistent_manifest = previous_manifest.clone();
    inconsistent_manifest.population.distinct_rows -= 1;
    inconsistent_manifest.population.duplicate_rows += 1;
    let inconsistent_identity = checkpoint_identity(&inconsistent_manifest);
    let mut inconsistent_pointer: V36PrefixCheckpointPointer =
        serde_json::from_slice(&previous_pointer_bytes).unwrap();
    inconsistent_pointer.manifest = inconsistent_identity;
    let inconsistent_pointer_bytes = canonical_v36_prefix_checkpoint_pointer_bytes(
        &two_object_checkpoint_context(3),
        &inconsistent_pointer,
    )
    .unwrap();
    let mut inconsistent_head = loaded.clone();
    inconsistent_head.manifest = inconsistent_manifest;
    inconsistent_head.pointer_bytes = inconsistent_pointer_bytes;
    assert!(
        V36PrefixPopulationCheckpointWriter::resume(
            &inconsistent_root,
            two_object_checkpoint_context(3),
            "a".repeat(64),
            "v36-prefix-screen-fixture-attempt-0001".into(),
            1,
            "i-replacement".into(),
            inconsistent_head,
        )
        .is_err()
    );
    let corrupt_root = directory.path().join("corrupt-resume-outbox");
    std::fs::create_dir(&corrupt_root).unwrap();
    std::fs::write(&staged_dependency, b"corrupt after load").unwrap();
    assert!(
        V36PrefixPopulationCheckpointWriter::resume(
            &corrupt_root,
            two_object_checkpoint_context(3),
            "a".repeat(64),
            "v36-prefix-screen-fixture-attempt-0001".into(),
            1,
            "i-replacement".into(),
            loaded.clone(),
        )
        .is_err()
    );
    std::fs::write(&staged_dependency, &first_bytes).unwrap();
    let resumed_root = directory.path().join("resumed-outbox");
    std::fs::create_dir(&resumed_root).unwrap();
    let mut resumed_writer = V36PrefixPopulationCheckpointWriter::resume(
        &resumed_root,
        two_object_checkpoint_context(3),
        "a".repeat(64),
        "v36-prefix-screen-fixture-attempt-0001".into(),
        1,
        "i-replacement".into(),
        loaded,
    )
    .unwrap();
    assert_eq!(resumed_writer.identity_run_files().unwrap().len(), 1);
    let second_ready = resumed_writer
        .commit(&V36PrefixPopulationCommit {
            cutoff: Some((1, 4)),
            distinct_rows: 3,
            duplicate_rows: 102,
            physical_rows: 105,
            run: V36PrefixIdentityRun {
                physical_rows: 5,
                rows: vec![identity(99, 4, 1)],
                selected_object_ordinal: 1,
                source: source_object_at(1),
            },
        })
        .unwrap();
    assert_eq!(
        second_ready.file_name().unwrap(),
        "generation-00000001.json"
    );
    assert_eq!(resumed_root.join("commits").read_dir().unwrap().count(), 1);
    assert_eq!(resumed_root.join("objects").read_dir().unwrap().count(), 2);
    let second_ready_value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&second_ready).unwrap()).unwrap();
    assert!(second_ready_value["previous_pointer_sha256"].is_string());

    let invalid_root = directory.path().join("invalid-outbox");
    std::fs::create_dir(&invalid_root).unwrap();
    let mut invalid_writer = V36PrefixPopulationCheckpointWriter::create(
        &invalid_root,
        checkpoint_context(100),
        "1".repeat(64),
        "v36-prefix-screen-fixture-attempt-0000".into(),
        0,
        "i-fixture".into(),
    )
    .unwrap();
    let mut inconsistent = commit;
    inconsistent.distinct_rows = 80;
    inconsistent.duplicate_rows = 20;
    assert!(invalid_writer.commit(&inconsistent).is_err());
    assert!(
        invalid_root
            .join("commits")
            .read_dir()
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn v36_prefix_checkpoint_population_writer_commits_authenticated_run_file() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("outbox");
    std::fs::create_dir(&root).unwrap();
    let run = V36PrefixIdentityRun {
        physical_rows: 100,
        rows: vec![identity(7, 9, 0), identity(41, 2, 0)],
        selected_object_ordinal: 0,
        source: source_object(),
    };
    let run_bytes = encode_v36_prefix_identity_run(&run).unwrap();
    let run_path = directory.path().join("identity-run.arrow");
    std::fs::write(&run_path, &run_bytes).unwrap();
    let boundary = V36PrefixPopulationFileCommit {
        distinct_rows: 2,
        duplicate_rows: 98,
        physical_rows: 100,
        run: V36PrefixIdentityRunFile {
            identity: identity_run_artifact(&run_bytes, 0),
            path: run_path.clone(),
            selected_object_ordinal: 0,
            source: source_object(),
        },
    };
    let mut writer = V36PrefixPopulationCheckpointWriter::create(
        &root,
        two_object_checkpoint_context(3),
        "1".repeat(64),
        "v36-prefix-screen-fixture-attempt-0000".into(),
        0,
        "i-fixture".into(),
    )
    .unwrap();
    let ready = writer.commit_file(&boundary).unwrap();
    assert!(ready.is_file());
    let installed = writer.identity_run_files().unwrap();
    assert_eq!(installed.len(), 1);
    assert_eq!(
        installed[0].path.parent(),
        Some(root.join("objects").as_path())
    );
    std::fs::remove_file(&run_path).unwrap();

    let duplicate_run = V36PrefixIdentityRun {
        physical_rows: 5,
        rows: vec![identity(41, 4, 1)],
        selected_object_ordinal: 1,
        source: source_object_at(1),
    };
    let second_path = directory.path().join("identity-run-0001.arrow");
    let duplicate_bytes = encode_v36_prefix_identity_run(&duplicate_run).unwrap();
    std::fs::write(&second_path, &duplicate_bytes).unwrap();
    let mut duplicate_boundary = V36PrefixPopulationFileCommit {
        distinct_rows: 3,
        duplicate_rows: 102,
        physical_rows: 105,
        run: V36PrefixIdentityRunFile {
            identity: identity_run_artifact(&duplicate_bytes, 1),
            path: second_path.clone(),
            selected_object_ordinal: 1,
            source: source_object_at(1),
        },
    };
    assert!(writer.commit_file(&duplicate_boundary).is_err());

    let second_run = V36PrefixIdentityRun {
        physical_rows: 5,
        rows: vec![identity(99, 4, 1)],
        selected_object_ordinal: 1,
        source: source_object_at(1),
    };
    let second_bytes = encode_v36_prefix_identity_run(&second_run).unwrap();
    std::fs::write(&second_path, &second_bytes).unwrap();
    duplicate_boundary.run.identity = identity_run_artifact(&second_bytes, 1);
    writer.commit_file(&duplicate_boundary).unwrap();

    let corrupt_root = directory.path().join("corrupt-outbox");
    std::fs::create_dir(&corrupt_root).unwrap();
    let mut corrupt_writer = V36PrefixPopulationCheckpointWriter::create(
        &corrupt_root,
        two_object_checkpoint_context(3),
        "1".repeat(64),
        "v36-prefix-screen-fixture-attempt-0000".into(),
        0,
        "i-fixture".into(),
    )
    .unwrap();
    std::fs::write(&run_path, b"corrupt after descriptor creation").unwrap();
    assert!(corrupt_writer.commit_file(&boundary).is_err());
    assert!(
        corrupt_root
            .join("commits")
            .read_dir()
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn v36_prefix_checkpoint_writer_file_backed_selected_phase_authenticates_before_ready() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("outbox");
    std::fs::create_dir(&root).unwrap();
    let mut writer = V36PrefixPopulationCheckpointWriter::create(
        &root,
        checkpoint_context(2),
        "1".repeat(64),
        "v36-prefix-screen-fixture-attempt-0000".into(),
        0,
        "i-fixture".into(),
    )
    .unwrap();
    writer
        .commit(&V36PrefixPopulationCommit {
            cutoff: Some((0, 9)),
            distinct_rows: 2,
            duplicate_rows: 8,
            physical_rows: 10,
            run: V36PrefixIdentityRun {
                physical_rows: 10,
                rows: vec![identity(7, 9, 0), identity(41, 2, 0)],
                selected_object_ordinal: 0,
                source: source_object(),
            },
        })
        .unwrap();

    let selected_bytes = b"selected-arrow-fixture";
    let selected_path = directory.path().join("selected.arrow");
    std::fs::write(&selected_path, selected_bytes).unwrap();
    let selection = V36PrefixPopulationSelection {
        cutoff_feature_row_id: 7,
        cutoff_score_sha256: "a".repeat(64),
        eligible_rows: 2,
        excluded_rows: 0,
        excluded_population_identity: None,
        selected_ids: checkpoint_artifact_bytes(
            "population-selected-identities",
            "population-selected-identities.arrow",
            selected_bytes,
        ),
        selected_rows: 2,
    };

    let corrupt_path = directory.path().join("corrupt-selected.arrow");
    std::fs::write(&corrupt_path, b"wrong").unwrap();
    assert!(writer.commit_selected(&selection, &corrupt_path).is_err());
    assert!(!root.join("commits/generation-00000001.json").exists());

    let ready = writer.commit_selected(&selection, &selected_path).unwrap();
    assert_eq!(ready.file_name().unwrap(), "generation-00000001.json");
    assert!(
        root.join("objects")
            .join(format!("{}.blob", selection.selected_ids.sha256))
            .is_file()
    );
}

#[test]
fn v36_prefix_checkpoint_writer_file_backed_materialized_phase_preserves_selection() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("outbox");
    std::fs::create_dir(&root).unwrap();
    let mut writer = V36PrefixPopulationCheckpointWriter::create(
        &root,
        checkpoint_context(2),
        "1".repeat(64),
        "v36-prefix-screen-fixture-attempt-0000".into(),
        0,
        "i-fixture".into(),
    )
    .unwrap();
    writer
        .commit(&V36PrefixPopulationCommit {
            cutoff: Some((0, 9)),
            distinct_rows: 2,
            duplicate_rows: 8,
            physical_rows: 10,
            run: V36PrefixIdentityRun {
                physical_rows: 10,
                rows: vec![identity(7, 9, 0), identity(41, 2, 0)],
                selected_object_ordinal: 0,
                source: source_object(),
            },
        })
        .unwrap();
    let selected_bytes = b"selected-arrow-fixture";
    let selected_path = directory.path().join("selected.arrow");
    std::fs::write(&selected_path, selected_bytes).unwrap();
    let selection = V36PrefixPopulationSelection {
        cutoff_feature_row_id: 7,
        cutoff_score_sha256: "a".repeat(64),
        eligible_rows: 2,
        excluded_rows: 0,
        excluded_population_identity: None,
        selected_ids: checkpoint_artifact_bytes(
            "population-selected-identities",
            "population-selected-identities.arrow",
            selected_bytes,
        ),
        selected_rows: 2,
    };
    writer.commit_selected(&selection, &selected_path).unwrap();

    let roles = [
        ("population-authority", "population-authority.json"),
        ("source", "source.parquet"),
        ("development-query", "development-query.parquet"),
        ("validation-query", "validation-query.parquet"),
        ("sealed-holdout-query", "sealed-holdout-query.parquet"),
        ("performance-query", "performance-query.parquet"),
    ];
    let mut files = Vec::new();
    for (ordinal, (role, filename)) in roles.into_iter().enumerate() {
        let bytes = format!("materialized-role-{ordinal}").into_bytes();
        let path = directory.path().join(filename);
        std::fs::write(&path, &bytes).unwrap();
        files.push(V36PrefixCheckpointDependencyFile {
            identity: checkpoint_artifact_bytes(role, filename, &bytes),
            path,
        });
    }
    let artifacts = V36PrefixMaterializedArtifacts {
        population_authority: files[0].identity.clone(),
        source: files[1].identity.clone(),
        development_query: files[2].identity.clone(),
        validation_query: files[3].identity.clone(),
        sealed_holdout_query: files[4].identity.clone(),
        performance_query: files[5].identity.clone(),
    };
    let ready = writer.commit_materialized(&artifacts, &files).unwrap();
    assert_eq!(ready.file_name().unwrap(), "generation-00000002.json");
    let ready_value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(ready).unwrap()).unwrap();
    assert_eq!(ready_value["dependencies"].as_array().unwrap().len(), 8);
    assert!(files.iter().all(|dependency| {
        root.join("objects")
            .join(format!("{}.blob", dependency.identity.sha256))
            .is_file()
    }));
}

#[test]
fn v36_prefix_checkpoint_writer_accepts_complete_objects_after_target_crossing() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("outbox");
    std::fs::create_dir(&root).unwrap();
    let mut writer = V36PrefixPopulationCheckpointWriter::create(
        &root,
        three_object_checkpoint_context(1),
        "1".repeat(64),
        "v36-prefix-screen-fixture-attempt-0000".into(),
        0,
        "i-fixture".into(),
    )
    .unwrap();
    for ordinal in 0_u16..3 {
        writer
            .commit(&V36PrefixPopulationCommit {
                cutoff: (ordinal == 0).then_some((0, 0)),
                distinct_rows: u64::from(ordinal) + 1,
                duplicate_rows: 0,
                physical_rows: u64::from(ordinal) + 1,
                run: V36PrefixIdentityRun {
                    physical_rows: 1,
                    rows: vec![identity(u64::from(ordinal) + 41, 0, ordinal)],
                    selected_object_ordinal: ordinal,
                    source: source_object_at(ordinal),
                },
            })
            .unwrap();
    }
    assert_eq!(root.join("commits").read_dir().unwrap().count(), 3);
}

#[test]
fn v36_prefix_checkpoint_writer_retry_after_publication_failure_is_not_poisoned() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("outbox");
    std::fs::create_dir(&root).unwrap();
    let mut writer = V36PrefixPopulationCheckpointWriter::create(
        &root,
        checkpoint_context(100),
        "1".repeat(64),
        "v36-prefix-screen-fixture-attempt-0000".into(),
        0,
        "i-fixture".into(),
    )
    .unwrap();
    let boundary = V36PrefixPopulationCommit {
        cutoff: None,
        distinct_rows: 2,
        duplicate_rows: 98,
        physical_rows: 100,
        run: V36PrefixIdentityRun {
            physical_rows: 100,
            rows: vec![identity(7, 9, 0), identity(41, 2, 0)],
            selected_object_ordinal: 0,
            source: source_object(),
        },
    };
    let blocked_ready = root.join("commits/generation-00000000.json");
    std::fs::create_dir(&blocked_ready).unwrap();
    assert!(writer.commit(&boundary).is_err());
    std::fs::remove_dir(&blocked_ready).unwrap();

    let ready = writer.commit(&boundary).unwrap();
    assert_eq!(ready, blocked_ready);
    assert_eq!(root.join("commits").read_dir().unwrap().count(), 1);
}

#[test]
fn v36_prefix_checkpoint_writer_round_trips_cohort_b_global_ordinal() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("outbox");
    std::fs::create_dir(&root).unwrap();
    let mut context = checkpoint_context(100);
    context.cohort_ordinal = 1;
    context.excluded_population_identity = Some(artifact(
        "population-selected-identities",
        "cohort-a-population-selected-identities.arrow",
        'd',
    ));
    context.selected_object_count = 16;
    context.selected_object_start = 16;
    context.ranked_objects = (0..16).map(registered_source_at).collect();
    context.source_byte_cap = 1_000_000;
    let mut writer = V36PrefixPopulationCheckpointWriter::create(
        &root,
        context.clone(),
        "1".repeat(64),
        "v36-prefix-screen-fixture-attempt-0000".into(),
        0,
        "i-fixture".into(),
    )
    .unwrap();
    let ready = writer
        .commit(&V36PrefixPopulationCommit {
            cutoff: None,
            distinct_rows: 1,
            duplicate_rows: 0,
            physical_rows: 1,
            run: V36PrefixIdentityRun {
                physical_rows: 1,
                rows: vec![identity(41, 0, 16)],
                selected_object_ordinal: 16,
                source: source_object_at(0),
            },
        })
        .unwrap();
    let ready_value: serde_json::Value =
        serde_json::from_slice(&std::fs::read(ready).unwrap()).unwrap();
    let manifest_identity: V36ArtifactIdentity =
        serde_json::from_value(ready_value["manifest"].clone()).unwrap();
    let manifest_bytes = std::fs::read(
        root.join("manifests")
            .join(format!("{}.json", manifest_identity.sha256)),
    )
    .unwrap();
    let manifest: V36PrefixCheckpointManifest = serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(manifest.population.selected_object_start, 16);
    assert_eq!(
        manifest.population.identity_runs[0].role,
        "population-identity-run-0016"
    );
    let run_identity = manifest.population.identity_runs[0].clone();
    let run_bytes = std::fs::read(
        root.join("objects")
            .join(format!("{}.blob", run_identity.sha256)),
    )
    .unwrap();
    let pointer_bytes = std::fs::read(root.join("pointers").join(format!(
        "{}.json",
        ready_value["pointer_sha256"].as_str().unwrap()
    )))
    .unwrap();
    let staged = directory.path().join("staged");
    std::fs::create_dir(&staged).unwrap();
    std::fs::create_dir(staged.join("objects")).unwrap();
    std::fs::write(staged.join("pointer.json"), pointer_bytes).unwrap();
    std::fs::write(staged.join("manifest.json"), manifest_bytes).unwrap();
    std::fs::write(
        staged
            .join("objects")
            .join(format!("{}.blob", run_identity.sha256)),
        run_bytes,
    )
    .unwrap();
    let loaded = load_v36_prefix_checkpoint_head(&staged, &context).unwrap();
    assert_eq!(loaded.manifest, manifest);
}

#[test]
fn v36_prefix_checkpoint_identity_run_is_strict_arrow_ipc() {
    let rows = vec![identity(7, 9, 3), identity(41, 2, 3)];
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

#[test]
fn v36_prefix_checkpoint_identity_run_uses_fixed_bounded_batches() {
    let row_count = 65_537_u64;
    let run = V36PrefixIdentityRun {
        physical_rows: row_count,
        rows: (0..row_count)
            .map(|row_offset| identity(row_offset + 1, row_offset, 3))
            .collect(),
        selected_object_ordinal: 3,
        source: source_object(),
    };

    let bytes = encode_v36_prefix_identity_run(&run).unwrap();
    let mut reader = arrow_ipc::reader::FileReader::try_new(Cursor::new(&bytes), None).unwrap();
    assert_eq!(reader.num_batches(), 2);
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 65_536);
    assert_eq!(reader.next().unwrap().unwrap().num_rows(), 1);
    assert!(reader.next().is_none());
    assert_eq!(
        decode_v36_prefix_identity_run(
            &bytes,
            &identity_run_artifact(&bytes, 3),
            &source_object(),
            3,
        )
        .unwrap(),
        run
    );
}

#[test]
fn v36_prefix_checkpoint_identity_run_v3_is_feature_ordered_with_physical_winners() {
    let run = V36PrefixIdentityRun {
        physical_rows: 3,
        rows: vec![identity(7, 2, 3), identity(41, 0, 3)],
        selected_object_ordinal: 3,
        source: source_object(),
    };
    let bytes = encode_v36_prefix_identity_run(&run).unwrap();
    let reader = arrow_ipc::reader::FileReader::try_new(Cursor::new(&bytes), None).unwrap();
    assert_eq!(
        reader.schema().metadata().get("format").map(String::as_str),
        Some("borsuk-v36-prefix-identity-run-v3")
    );
    assert_eq!(
        decode_v36_prefix_identity_run(
            &bytes,
            &identity_run_artifact(&bytes, 3),
            &source_object(),
            3,
        )
        .unwrap(),
        run
    );

    let mut row_offset_ordered = run;
    row_offset_ordered.rows.swap(0, 1);
    assert!(encode_v36_prefix_identity_run(&row_offset_ordered).is_err());

    let mut duplicate_physical_winner = row_offset_ordered;
    duplicate_physical_winner.rows.swap(0, 1);
    duplicate_physical_winner.rows[1].row_offset = duplicate_physical_winner.rows[0].row_offset;
    assert!(encode_v36_prefix_identity_run(&duplicate_physical_winner).is_err());
}

#[test]
fn v36_prefix_checkpoint_identity_run_accepts_global_cohort_b_ordinal() {
    let run = V36PrefixIdentityRun {
        physical_rows: 3,
        rows: vec![identity(7, 2, 16), identity(41, 0, 16)],
        selected_object_ordinal: 16,
        source: source_object(),
    };
    let bytes = encode_v36_prefix_identity_run(&run).unwrap();
    let registered = identity_run_artifact(&bytes, 16);
    assert_eq!(
        decode_v36_prefix_identity_run(&bytes, &registered, &source_object(), 16).unwrap(),
        run
    );
    let restored = restore_v36_prefix_population_state(&[run], 1).unwrap();
    assert_eq!(restored.next_object_ordinal, 17);
}

#[test]
fn v36_prefix_checkpoint_population_before_cutoff_is_resumable() {
    let run = V36PrefixIdentityRun {
        physical_rows: 3,
        rows: vec![identity(7, 2, 0), identity(41, 0, 0)],
        selected_object_ordinal: 0,
        source: source_object(),
    };
    let restored = restore_v36_prefix_population_state(&[run], 4).unwrap();
    assert_eq!(restored.next_object_ordinal, 1);
    assert_eq!(restored.physical_rows, 3);
    assert_eq!(restored.distinct_rows_observed, 2);
    assert_eq!(restored.duplicate_rows, 1);
    assert_eq!(restored.cutoff, None);
    assert_eq!(
        restored
            .unique_rows
            .iter()
            .map(|row| row.feature_row_id)
            .collect::<Vec<_>>(),
        vec![7, 41],
    );
}

#[test]
fn v36_prefix_checkpoint_restores_complete_window_after_selection_target_is_reached() {
    let runs = vec![
        V36PrefixIdentityRun {
            physical_rows: 1,
            rows: vec![identity(41, 0, 0)],
            selected_object_ordinal: 0,
            source: source_object_at(0),
        },
        V36PrefixIdentityRun {
            physical_rows: 1,
            rows: vec![identity(7, 0, 1)],
            selected_object_ordinal: 1,
            source: source_object_at(1),
        },
    ];
    let restored = restore_v36_prefix_population_state(&runs, 1).unwrap();
    assert_eq!(restored.consumed_objects.len(), 2);
    assert_eq!(restored.distinct_rows_observed, 2);
    assert_eq!(restored.next_object_ordinal, 2);
    assert_eq!(
        restored.unique_rows,
        vec![identity(41, 0, 0), identity(7, 0, 1)]
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
fn v36_prefix_checkpoint_identity_runs_restore_complete_window() {
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
        vec![1, 2, 3, 4, 5]
    );

    let mut duplicate = runs.clone();
    duplicate[1].rows[0].feature_row_id = 1;
    assert!(restore_v36_prefix_population(&duplicate, 4).is_err());
    let mut complete_window = runs;
    complete_window.push(V36PrefixIdentityRun {
        physical_rows: 1,
        rows: Vec::new(),
        selected_object_ordinal: 2,
        source: source_object_at(2),
    });
    let restored = restore_v36_prefix_population(&complete_window, 4).unwrap();
    assert_eq!(restored.cutoff_object_ordinal, 1);
    assert_eq!(restored.cutoff_row_offset, 1);
    assert_eq!(restored.consumed_objects.len(), 3);
}
