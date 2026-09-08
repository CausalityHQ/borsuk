//! Closed authority contracts for the bounded V36 object-sample screen.

use blake3::Hasher as Blake3;
use borsuk::{
    V36ArtifactIdentity, V36ChunkCeiling, V36CoarseCode, V36FineCodec, V36FunnelManifest,
    V36GeometryArm, V36PrefixPopulationAuthority, V36PrefixRegisteredSourceObject,
    V36PrefixRoleAuthority, V36PrefixScreenManifest, V36PrefixSourceObject, V36PrimaryRows,
    V36ProjectionArm, V36RegisteredManifest, V36Replication, V36ResourceRequest, V36ShapeScore,
    canonical_v36_prefix_screen_manifest_bytes, project_v36_resources, validate_v36_manifest,
    validate_v36_prefix_screen_manifest,
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const MIB: u64 = 1_048_576;

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn sample_digest(path: &str, encoded_bytes: u64) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"borsuk-v36-screen-object-v1");
    hasher.update(path.as_bytes());
    hasher.update(encoded_bytes.to_le_bytes());
    format!("{:x}", hasher.finalize())
}

fn artifact(role: &str, byte: u8) -> V36ArtifactIdentity {
    V36ArtifactIdentity {
        blake3: digest(byte.saturating_add(64)),
        encoded_bytes: 1_000 + u64::from(byte),
        role: role.to_owned(),
        sha256: digest(byte),
        uri: format!("s3://borsuk-v36-screen/{role}"),
    }
}

fn canonical(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, canonical(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        scalar => scalar,
    }
}

fn canonical_bytes(value: &impl Serialize) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(&canonical(serde_json::to_value(value).unwrap())).unwrap();
    bytes.push(b'\n');
    bytes
}

fn bound_artifact(role: &str, value: &impl Serialize) -> V36ArtifactIdentity {
    let bytes = canonical_bytes(value);
    V36ArtifactIdentity {
        blake3: Blake3::new().update(&bytes).finalize().to_hex().to_string(),
        encoded_bytes: u64::try_from(bytes.len()).unwrap(),
        role: role.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        uri: format!("s3://borsuk-v36-screen/{role}"),
    }
}

fn source_object(ordinal: u8) -> V36PrefixSourceObject {
    let sample_sha256 = match ordinal {
        0 => "059fb28c1ee68a9d2a15ce5fd0425a0d9aec17e16f8c363dbba0eae49373729a",
        1 => "9bb9461e27e4bbc1e42a76d8fae26d6ad59bb9fb8c98ac13a81c3a28b85b2211",
        2 => "d61045ff634d2d45fb962c3f98691871d83973e752d6617bdea47bf543398475",
        3 => "9dd234a3716fc4915686e5febc420a7cfd98efa71569db4c0bf3b9c7e052ba7c",
        _ => unreachable!(),
    };
    V36PrefixSourceObject {
        blake3: digest(ordinal.saturating_add(96)),
        encoded_bytes: 342_000_000 + u64::from(ordinal),
        path: format!("data/relaion2b_features_{ordinal:05}.parquet"),
        sample_sha256: sample_sha256.to_owned(),
        sha256: digest(ordinal.saturating_add(1)),
        uri: format!(
            "https://huggingface.co/datasets/andropar/relaion2b-natural-embeddings/resolve/bfc7465dcf1245bd605d35dcaf5d2177bbc2025a/data/relaion2b_features_{ordinal:05}.parquet"
        ),
    }
}

fn source_registry() -> Vec<V36PrefixRegisteredSourceObject> {
    (0..4)
        .map(|ordinal| {
            let object = source_object(ordinal);
            V36PrefixRegisteredSourceObject {
                encoded_bytes: object.encoded_bytes,
                path: object.path,
                sha256: object.sha256,
                uri: object.uri,
            }
        })
        .collect()
}

fn population() -> V36PrefixPopulationAuthority {
    V36PrefixPopulationAuthority {
        claim_eligible: false,
        cohort_ordinal: 0,
        construction_capability: "named-query-excluded-corpus-only-no-query-truth".to_owned(),
        consumed_objects: vec![
            source_object(0),
            source_object(1),
            source_object(3),
            source_object(2),
        ],
        corpus_rows: 1_000_000,
        corpus_seed_label: "borsuk-v36-prefix-screen-corpus-v2".to_owned(),
        corpus_seed_sha256: "56b288d41e87d3b4ba97ac02b9944837e6bde8b402b8fab088861a27ef099f8c"
            .to_owned(),
        dataset_authority_sha256:
            "0d2e8cef3cf27860131a6a8c33d08b858f8837263212cb03515ae53c76acd5c1".to_owned(),
        distinct_candidates: 1_100_000,
        duplicate_rule: "first-selected-object-ordinal-then-row-offset".to_owned(),
        evaluation_capability: "named-artifacts-only-no-source-list-discovery".to_owned(),
        excluded_population_identity: None,
        format: "borsuk-v36-prefix-population-authority-v2".to_owned(),
        future_full_source_exclusion_roles: vec![
            "development".to_owned(),
            "validation".to_owned(),
            "sealed-holdout".to_owned(),
            "performance".to_owned(),
        ],
        object_cap: 16,
        object_sampling_algorithm:
            "sha256-borsuk-v36-screen-object-v1-path-utf8-length-le-u64-then-path".to_owned(),
        ordered_source_manifest_sha256:
            "e8127c6fffd6f2c6f2174ddb0344d3d1e73a470c693ba5355b0b9201dac9c514".to_owned(),
        population_id: "borsuk-v36-prefix-screen-population-v2".to_owned(),
        population_sampling_algorithm:
            "sha256-seed-sha256-manifest-sha256-feature-row-id-le-u64-v2".to_owned(),
        population_seed_label: "borsuk-v36-prefix-screen-population-row-v2".to_owned(),
        population_seed_sha256: "bcb490ff7944bfa3a0a6d5abe6d35ba34ecaba60b615e214edb057a1a5b63b8e"
            .to_owned(),
        roles: vec![
            V36PrefixRoleAuthority {
                role: "development".to_owned(),
                rows: 1_000,
                seed_label: "borsuk-v36-prefix-screen-development-query-v2".to_owned(),
                seed_sha256: "da46dc39758d8dd6b71942fb9eadd666b3fce0b2e0114a62359335f645525981"
                    .to_owned(),
            },
            V36PrefixRoleAuthority {
                role: "validation".to_owned(),
                rows: 1_000,
                seed_label: "borsuk-v36-prefix-screen-validation-query-v2".to_owned(),
                seed_sha256: "bcd253d65fc3786900a9e17aa9e4e65ef592d7ac11ca37abdcdfcf6b74aa9a59"
                    .to_owned(),
            },
            V36PrefixRoleAuthority {
                role: "sealed-holdout".to_owned(),
                rows: 1_000,
                seed_label: "borsuk-v36-prefix-screen-sealed-holdout-query-v2".to_owned(),
                seed_sha256: "8e9f673c7451c72bd19e255b248f4212298d1c958d35a831ecaefac8a60b4d84"
                    .to_owned(),
            },
            V36PrefixRoleAuthority {
                role: "performance".to_owned(),
                rows: 10_000,
                seed_label: "borsuk-v36-prefix-screen-performance-query-v2".to_owned(),
                seed_sha256: "a84a6410a7bcad8ca1cc6520dbfd69c1f88ebff08d48610f5a9098d035e52901"
                    .to_owned(),
            },
        ],
        selected_object_count: 4,
        selected_object_encoded_bytes: 1_368_000_006,
        selected_object_start: 0,
        source_byte_cap: 6 * 1_024 * MIB,
        source_revision: "bfc7465dcf1245bd605d35dcaf5d2177bbc2025a".to_owned(),
        workspace_bytes: 32 * MIB,
        workspace_count: 16,
    }
}

fn centered_projection() -> V36ProjectionArm {
    V36ProjectionArm::CenteredSubspace192 {
        artifact: Box::new(artifact("centered-projection-basis", 30)),
    }
}

fn screen_manifest() -> V36PrefixScreenManifest {
    let population = population();
    let projection = centered_projection();
    let projection_artifact = match &projection {
        V36ProjectionArm::CenteredSubspace192 { artifact, .. } => artifact.as_ref().clone(),
        V36ProjectionArm::Srht192 { .. } => bound_artifact("projection", &projection),
    };
    V36PrefixScreenManifest {
        artifacts: vec![
            bound_artifact("population-authority", &population),
            projection_artifact,
            artifact("query-excluded-corpus-parquet", 2),
            artifact("posting-summaries", 3),
        ],
        claim_eligible: false,
        chunk_ceiling: V36ChunkCeiling::Kib512,
        coarse_code: V36CoarseCode::ResidualPq4Code32,
        fine_codec: V36FineCodec::Sq8,
        format: "borsuk-v36-prefix-screen-manifest-v2".to_owned(),
        geometry: V36GeometryArm {
            primary_rows: V36PrimaryRows::Posting4096,
            replication: V36Replication::ClosureEpsilon15,
        },
        population,
        posting_summary_metadata_bytes: 128,
        posting_summary_slot_bytes: 4_736,
        posting_summary_vector_bytes: 4_608,
        projection,
        shape_score: V36ShapeScore::Prototype6,
        unique_k: 1_536,
    }
}

fn rebind_population(manifest: &mut V36PrefixScreenManifest) {
    let identity = bound_artifact("population-authority", &manifest.population);
    *manifest
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.role == "population-authority")
        .unwrap() = identity;
}

fn rebind_projection(manifest: &mut V36PrefixScreenManifest) {
    manifest.artifacts.retain(|artifact| {
        artifact.role != "projection" && artifact.role != "centered-projection-basis"
    });
    manifest.artifacts.push(match &manifest.projection {
        V36ProjectionArm::CenteredSubspace192 { artifact, .. } => artifact.as_ref().clone(),
        V36ProjectionArm::Srht192 { .. } => bound_artifact("projection", &manifest.projection),
    });
}

fn registered(bytes: &[u8]) -> V36RegisteredManifest {
    V36RegisteredManifest {
        blake3: Blake3::new().update(bytes).finalize().to_hex().to_string(),
        encoded_bytes: u64::try_from(bytes.len()).unwrap(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        uri: "s3://borsuk-v36-screen/screen-manifest.json".to_owned(),
    }
}

fn validate(manifest: &V36PrefixScreenManifest) -> bool {
    validate_with_registry(manifest, &source_registry())
}

fn validate_with_registry(
    manifest: &V36PrefixScreenManifest,
    source_registry: &[V36PrefixRegisteredSourceObject],
) -> bool {
    let Ok(bytes) = canonical_v36_prefix_screen_manifest_bytes(manifest) else {
        return false;
    };
    validate_v36_prefix_screen_manifest(&bytes, &registered(&bytes), source_registry).is_ok()
}

#[test]
fn v36_prefix_projection_identity_is_closed() {
    // Break caught: projection evidence is swapped after an outcome or a weak,
    // under-specified centered decomposition is admitted as the frozen arm.
    let baseline = screen_manifest();
    assert!(validate(&baseline));

    let mut srht = baseline.clone();
    srht.projection = V36ProjectionArm::Srht192 { seed: 36 };
    rebind_projection(&mut srht);
    assert!(validate(&srht));

    let mut wrong_seed = srht.clone();
    wrong_seed.projection = V36ProjectionArm::Srht192 { seed: 35 };
    rebind_projection(&mut wrong_seed);
    assert!(!validate(&wrong_seed));

    let baseline_bytes = canonical_v36_prefix_screen_manifest_bytes(&baseline).unwrap();

    for mutate in [
        |identity: &mut V36ArtifactIdentity| identity.encoded_bytes = 0,
        |identity: &mut V36ArtifactIdentity| identity.sha256 = "invalid".to_owned(),
        |identity: &mut V36ArtifactIdentity| identity.blake3 = "invalid".to_owned(),
        |identity: &mut V36ArtifactIdentity| identity.role = "projection".to_owned(),
        |identity: &mut V36ArtifactIdentity| identity.uri.clear(),
    ] {
        let mut changed = baseline.clone();
        if let V36ProjectionArm::CenteredSubspace192 { artifact, .. } = &mut changed.projection {
            mutate(artifact);
        }
        rebind_projection(&mut changed);
        assert!(!validate(&changed));
    }

    let mut outer_substitution = baseline.clone();
    let outer = outer_substitution
        .artifacts
        .iter_mut()
        .find(|artifact| artifact.role == "centered-projection-basis")
        .unwrap();
    outer.sha256 = digest(33);
    assert!(!validate(&outer_substitution));

    let mut old_format = baseline.clone();
    old_format.format = "borsuk-v36-prefix-screen-manifest-v1".to_owned();
    assert!(!validate(&old_format));
    for mutate_registration in [
        |value: &mut V36RegisteredManifest| value.encoded_bytes += 1,
        |value: &mut V36RegisteredManifest| value.sha256 = digest(27),
        |value: &mut V36RegisteredManifest| value.blake3 = digest(26),
    ] {
        let mut changed = registered(&baseline_bytes);
        mutate_registration(&mut changed);
        assert!(
            validate_v36_prefix_screen_manifest(&baseline_bytes, &changed, &source_registry(),)
                .is_err()
        );
    }
    let without_lf = &baseline_bytes[..baseline_bytes.len() - 1];
    assert!(
        validate_v36_prefix_screen_manifest(
            without_lf,
            &registered(without_lf),
            &source_registry(),
        )
        .is_err()
    );
    for (field, invalid_value) in [
        ("shape_score", serde_json::json!("unknown-score")),
        (
            "projection",
            serde_json::json!({"arm": "unknown-projection"}),
        ),
    ] {
        let mut value = serde_json::to_value(&baseline).unwrap();
        value[field] = invalid_value;
        let bytes = canonical_bytes(&value);
        assert!(
            validate_v36_prefix_screen_manifest(&bytes, &registered(&bytes), &source_registry(),)
                .is_err()
        );
    }
    for legacy_field in ["legacy_seed", "solver", "covariance_sha256"] {
        let mut extra_projection_field = serde_json::to_value(&baseline).unwrap();
        extra_projection_field["projection"][legacy_field] = serde_json::json!("legacy");
        let extra_projection_bytes = canonical_bytes(&extra_projection_field);
        assert!(
            validate_v36_prefix_screen_manifest(
                &extra_projection_bytes,
                &registered(&extra_projection_bytes),
                &source_registry(),
            )
            .is_err()
        );
    }
    let mut noncanonical = baseline_bytes.clone();
    noncanonical.insert(noncanonical.len() - 1, b' ');
    assert!(
        validate_v36_prefix_screen_manifest(
            &noncanonical,
            &registered(&noncanonical),
            &source_registry(),
        )
        .is_err()
    );
}

#[test]
fn v36_full_manifest_binds_centered_projection_object_identity() {
    let projection = centered_projection();
    let projection_artifact = match &projection {
        V36ProjectionArm::CenteredSubspace192 { artifact, .. } => artifact.as_ref().clone(),
        V36ProjectionArm::Srht192 { .. } => unreachable!(),
    };
    let manifest = V36FunnelManifest {
        artifacts: vec![
            artifact("dataset-authority", 8),
            artifact("source-parquet", 9),
            projection_artifact,
            artifact("super-centroids", 11),
            artifact("posting-summaries", 12),
            artifact("posting-directory", 13),
            artifact("coarse-directory", 14),
            artifact("fine-directory", 15),
            artifact("visibility-directory", 16),
            artifact("coarse-codebook", 17),
        ],
        chunk_ceiling: V36ChunkCeiling::Kib256,
        claim_eligible: false,
        coarse_code: V36CoarseCode::ResidualPq4Code32,
        fine_codec: V36FineCodec::Sq8,
        format: "borsuk-v36-funnel-manifest-v3".to_owned(),
        geometry: V36GeometryArm {
            primary_rows: V36PrimaryRows::Posting4096,
            replication: V36Replication::ClosureEpsilon15,
        },
        metric: "squared-l2".to_owned(),
        projection_dimensions: 192,
        projection,
        shape_score: V36ShapeScore::Rank4,
        source_dimensions: 768,
        unique_k: 1_536,
    };
    let bytes = canonical_bytes(&manifest);
    assert!(validate_v36_manifest(&bytes, &registered(&bytes)).is_ok());

    let mut substituted = manifest;
    substituted
        .artifacts
        .iter_mut()
        .find(|identity| identity.role == "centered-projection-basis")
        .unwrap()
        .sha256 = digest(34);
    let bytes = canonical_bytes(&substituted);
    assert!(validate_v36_manifest(&bytes, &registered(&bytes)).is_err());
}

#[test]
fn v36_prototype_six_fits_the_equal_summary_slot() {
    // Break caught: prototype-six silently becomes centroid plus six vectors or
    // receives a larger resident budget than the other posting scorers.
    let baseline = screen_manifest();
    assert!(validate(&baseline));

    let bytes = canonical_v36_prefix_screen_manifest_bytes(&baseline).unwrap();
    assert!(
        bytes
            .windows(b"\"prototype-six\"".len())
            .any(|window| window == b"\"prototype-six\"")
    );
    assert_eq!(baseline.posting_summary_vector_bytes, 6 * 192 * 4);
    assert_eq!(
        baseline.posting_summary_vector_bytes + baseline.posting_summary_metadata_bytes,
        4_736
    );

    for mutate in [
        |value: &mut V36PrefixScreenManifest| value.posting_summary_vector_bytes = 7 * 192 * 4,
        |value: &mut V36PrefixScreenManifest| value.posting_summary_metadata_bytes = 129,
        |value: &mut V36PrefixScreenManifest| value.posting_summary_slot_bytes = 4_737,
    ] {
        let mut changed = baseline.clone();
        mutate(&mut changed);
        assert!(!validate(&changed));
    }
}

#[test]
fn v36_prefix_population_is_distinct_from_full_source_authority() {
    // Break caught: bounded sample evidence is relabelled as full-source
    // qualification or gains a source-discovery/truth capability.
    let baseline = screen_manifest();
    assert!(validate(&baseline));

    let mut drifted_registry = source_registry();
    drifted_registry[0].encoded_bytes += 1;
    assert!(!validate_with_registry(&baseline, &drifted_registry));

    let mut forged_uri_manifest = baseline.clone();
    forged_uri_manifest.population.consumed_objects[0].uri =
        "https://attacker.invalid/data/relaion2b_features_00000.parquet".to_owned();
    rebind_population(&mut forged_uri_manifest);
    let mut forged_uri_registry = source_registry();
    forged_uri_registry[0].uri = forged_uri_manifest.population.consumed_objects[0]
        .uri
        .clone();
    assert!(!validate_with_registry(
        &forged_uri_manifest,
        &forged_uri_registry,
    ));

    let mut reordered = baseline.clone();
    reordered.population.consumed_objects.swap(1, 2);
    rebind_population(&mut reordered);
    assert!(!validate(&reordered));

    let mut changed_length = baseline.clone();
    changed_length.population.consumed_objects[0].encoded_bytes += 1;
    changed_length.population.consumed_objects[0].sample_sha256 = sample_digest(
        &changed_length.population.consumed_objects[0].path,
        changed_length.population.consumed_objects[0].encoded_bytes,
    );
    rebind_population(&mut changed_length);
    assert!(!validate(&changed_length));

    let mut overflow = baseline.clone();
    for object in &mut overflow.population.consumed_objects[..2] {
        object.encoded_bytes = u64::MAX;
        object.sample_sha256 = sample_digest(&object.path, object.encoded_bytes);
    }
    rebind_population(&mut overflow);
    assert!(!validate(&overflow));

    let mut too_many = baseline.clone();
    for ordinal in 4_u8..17 {
        let path = format!("data/extra_{ordinal:05}.parquet");
        too_many
            .population
            .consumed_objects
            .push(V36PrefixSourceObject {
                blake3: digest(ordinal.saturating_add(96)),
                encoded_bytes: 1_024,
                path: path.clone(),
                sample_sha256: sample_digest(&path, 1_024),
                sha256: digest(ordinal.saturating_add(1)),
                uri: format!("https://frozen.example/extra/{ordinal:05}.parquet"),
            });
    }
    rebind_population(&mut too_many);
    assert!(!validate(&too_many));

    let mut duplicate_path = baseline.clone();
    duplicate_path.population.consumed_objects[1].path =
        duplicate_path.population.consumed_objects[0].path.clone();
    duplicate_path.population.consumed_objects[1].sample_sha256 = sample_digest(
        &duplicate_path.population.consumed_objects[1].path,
        duplicate_path.population.consumed_objects[1].encoded_bytes,
    );
    rebind_population(&mut duplicate_path);
    assert!(!validate(&duplicate_path));

    let mut missing_role = baseline.clone();
    missing_role.artifacts.pop();
    assert!(!validate(&missing_role));
    let mut extra_role = baseline.clone();
    extra_role.artifacts.push(artifact("unexpected", 31));
    assert!(!validate(&extra_role));

    for mutate in [
        |value: &mut V36PrefixScreenManifest| value.claim_eligible = true,
        |value: &mut V36PrefixScreenManifest| value.population.claim_eligible = true,
        |value: &mut V36PrefixScreenManifest| {
            value.population.population_id = "borsuk-v36-full-source".to_owned()
        },
        |value: &mut V36PrefixScreenManifest| value.population.distinct_candidates = 1_099_999,
        |value: &mut V36PrefixScreenManifest| value.population.corpus_rows = 999_999,
        |value: &mut V36PrefixScreenManifest| value.population.roles[3].rows = 9_999,
        |value: &mut V36PrefixScreenManifest| value.population.roles[0].seed_sha256 = digest(31),
        |value: &mut V36PrefixScreenManifest| value.population.object_cap = 17,
        |value: &mut V36PrefixScreenManifest| value.population.source_byte_cap += 1,
        |value: &mut V36PrefixScreenManifest| value.population.workspace_count = 15,
        |value: &mut V36PrefixScreenManifest| value.population.workspace_bytes = 16 * MIB,
        |value: &mut V36PrefixScreenManifest| {
            value.population.construction_capability = "source-plus-query-truth".to_owned()
        },
        |value: &mut V36PrefixScreenManifest| {
            value.population.evaluation_capability = "list-source-prefix".to_owned()
        },
        |value: &mut V36PrefixScreenManifest| {
            value.population.consumed_objects[1].uri =
                value.population.consumed_objects[0].uri.clone()
        },
        |value: &mut V36PrefixScreenManifest| {
            value.population.consumed_objects[0].sample_sha256 = digest(30)
        },
    ] {
        let mut changed = baseline.clone();
        mutate(&mut changed);
        rebind_population(&mut changed);
        assert!(!validate(&changed));
    }

    let mut contradictory_artifact = baseline.clone();
    contradictory_artifact.population.consumed_objects[0].blake3 = digest(28);
    assert!(!validate(&contradictory_artifact));

    let base_manifest = V36FunnelManifest {
        artifacts: vec![
            artifact("dataset-authority", 8),
            artifact("source-parquet", 9),
            artifact("projection", 10),
            artifact("super-centroids", 11),
            artifact("posting-summaries", 12),
            artifact("posting-directory", 13),
            artifact("coarse-directory", 14),
            artifact("fine-directory", 15),
            artifact("visibility-directory", 16),
            artifact("coarse-codebook", 17),
        ],
        chunk_ceiling: V36ChunkCeiling::Kib256,
        claim_eligible: false,
        coarse_code: V36CoarseCode::ResidualPq4Code32,
        fine_codec: V36FineCodec::Sq8,
        format: "borsuk-v36-funnel-manifest-v3".to_owned(),
        geometry: V36GeometryArm {
            primary_rows: V36PrimaryRows::Posting4096,
            replication: V36Replication::ClosureEpsilon15,
        },
        metric: "squared-l2".to_owned(),
        projection_dimensions: 192,
        projection: V36ProjectionArm::Srht192 { seed: 36 },
        shape_score: V36ShapeScore::Rank4,
        source_dimensions: 768,
        unique_k: 1_536,
    };
    let ledger = project_v36_resources(
        &base_manifest,
        &V36ResourceRequest {
            active_generation_bytes: 32 * MIB,
            compaction_new_arena_bytes: 0,
            compaction_old_arena_bytes: 0,
            decoded_cache_bytes: 64 * MIB,
            delta_coarse_bytes: 0,
            delta_csr_bytes: 0,
            encoded_decoded_overlap_bytes: 32 * MIB,
            fine_interval_directory_bytes: 64 * MIB,
            hnsw_bytes: 0,
            liveness_bytes: 8 * MIB,
            mean_replication_ppm: 1_000_000,
            posting_object_directory_bytes: 64 * MIB,
            recent_fine_bytes: 0,
            retiring_generation_bytes: 0,
            retry_buffer_bytes: 32 * MIB,
            rows: 1_000_000,
        },
    )
    .unwrap();
    assert_eq!(ledger.query_workspace_bytes, 16 * 32 * MIB);
}
