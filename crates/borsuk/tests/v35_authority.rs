//! V35 format authority and resident-memory admission contracts.

use std::collections::BTreeMap;

use borsuk::{
    V35ArtifactIdentity, V35Dimensions, V35GenerationManifest, V35ProjectionArm, V35RemoteCodeRate,
    project_v35_serving_memory, validate_v35_manifest,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn identity(role: &str, ordinal: u8) -> V35ArtifactIdentity {
    V35ArtifactIdentity {
        digest: digest(ordinal),
        digest_algorithm: "sha256".to_owned(),
        length: u64::from(ordinal) + 1,
        role: role.to_owned(),
        uri: format!("s3://frozen-v35/{role}/{ordinal}"),
    }
}

fn manifest() -> V35GenerationManifest {
    V35GenerationManifest {
        artifacts: vec![
            identity("active-routing", 1),
            identity("retiring-routing", 2),
            identity("active-projection-basis", 3),
            identity("retiring-projection-basis", 4),
            identity("code-directory", 5),
            identity("page-directory", 6),
            identity("active-liveness", 7),
            identity("retiring-liveness", 8),
        ],
        dimensions: V35Dimensions {
            routing: 192,
            source: 3_072,
        },
        format: "borsuk-v35-generation-v1".to_owned(),
        leaf_count: 414_100,
        metric: "squared-l2".to_owned(),
        normalization: "none".to_owned(),
        patches_per_leaf: 1,
        projection: V35ProjectionArm::Pca,
        remote_code: V35RemoteCodeRate {
            bits_per_dimension: 4,
            bytes_per_row: 1_536,
        },
        source_archive_sha256: digest(42),
        source_id: "frozen-high-dimensional-source".to_owned(),
        tree_node_count: 69_905,
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

fn manifest_bytes(value: &V35GenerationManifest) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(&canonical(serde_json::to_value(value).unwrap())).unwrap();
    bytes.push(b'\n');
    bytes
}

fn registered_manifest(bytes: &[u8]) -> V35ArtifactIdentity {
    V35ArtifactIdentity {
        digest: format!("{:x}", Sha256::digest(bytes)),
        digest_algorithm: "sha256".to_owned(),
        length: u64::try_from(bytes.len()).unwrap(),
        role: "generation-manifest".to_owned(),
        uri: "s3://frozen-v35/generation-manifest.json".to_owned(),
    }
}

#[test]
fn v35_authority_accepts_dimension_independent_remote_storage() {
    // Break caught: source dimension, routing dimension, or remote byte rate is
    // hard-coded to the old 96D format, or remote S3 bytes are charged as RSS.
    for source in [384_u32, 768, 960, 1_536, 3_072] {
        for routing in [64_u16, 128, 192] {
            let mut value = manifest();
            value.dimensions = V35Dimensions { routing, source };
            value.remote_code.bytes_per_row = source.div_ceil(2);
            let bytes = manifest_bytes(&value);
            let decoded = validate_v35_manifest(&bytes, &registered_manifest(&bytes)).unwrap();
            assert_eq!(decoded, value);
        }
    }

    let projection = project_v35_serving_memory(&manifest()).unwrap();
    assert_eq!(projection.remote_sq4_payload_bytes, 153_600_000_000);
    assert_eq!(projection.object_data_cache_bytes, 159_549_376);
    assert_eq!(projection.admission_budget_bytes, 2_842_461_184);
    assert_eq!(projection.hard_limit_bytes, 3_221_225_472);
}

#[test]
fn v35_authority_rejects_schema_types_and_identity_drift() {
    // Break caught: a malformed/ambiguous manifest or an artifact with the
    // wrong role, digest algorithm, URI, or registered complete-byte identity
    // reaches allocation or remote planning.
    let baseline = manifest();
    let baseline_bytes = manifest_bytes(&baseline);
    let registered = registered_manifest(&baseline_bytes);

    let mut byte_mutations = Vec::new();
    let mut missing = serde_json::to_value(&baseline).unwrap();
    missing.as_object_mut().unwrap().remove("source_id");
    byte_mutations.push(missing);
    let mut extra = serde_json::to_value(&baseline).unwrap();
    extra["legacy_dimensions"] = json!(96);
    byte_mutations.push(extra);
    let mut null = serde_json::to_value(&baseline).unwrap();
    null["leaf_count"] = Value::Null;
    byte_mutations.push(null);
    let mut wrong_type = serde_json::to_value(&baseline).unwrap();
    wrong_type["tree_node_count"] = json!("69905");
    byte_mutations.push(wrong_type);
    for value in byte_mutations {
        let mut bytes = serde_json::to_vec(&canonical(value)).unwrap();
        bytes.push(b'\n');
        assert!(validate_v35_manifest(&bytes, &registered_manifest(&bytes)).is_err());
    }

    let manifest_mutations: [fn(&mut V35GenerationManifest); 15] = [
        |v| v.dimensions.source = 0,
        |v| v.dimensions.routing = 96,
        |v| v.dimensions.routing = 256,
        |v| v.dimensions.routing = 192,
        |v| v.leaf_count = 0,
        |v| v.tree_node_count = 0,
        |v| v.patches_per_leaf = 0,
        |v| v.patches_per_leaf = 3,
        |v| v.remote_code.bits_per_dimension = 6,
        |v| v.remote_code.bytes_per_row = 1,
        |v| v.artifacts[0].digest_algorithm = "blake3".to_owned(),
        |v| v.artifacts[0].digest = "00".repeat(31),
        |v| v.artifacts[0].length = 0,
        |v| v.artifacts[1].uri = v.artifacts[0].uri.clone(),
        |v| v.artifacts[1].role = v.artifacts[0].role.clone(),
    ];
    for (index, mutate) in manifest_mutations.into_iter().enumerate() {
        let mut value = baseline.clone();
        if index == 3 {
            value.dimensions.source = 128;
        }
        mutate(&mut value);
        let bytes = manifest_bytes(&value);
        assert!(validate_v35_manifest(&bytes, &registered_manifest(&bytes)).is_err());
    }

    let mut wrong_registered = registered.clone();
    wrong_registered.digest.replace_range(0..2, "ff");
    assert!(validate_v35_manifest(&baseline_bytes, &wrong_registered).is_err());
    assert!(
        validate_v35_manifest(&baseline_bytes[..baseline_bytes.len() - 1], &registered).is_err()
    );
}

#[test]
fn v35_authority_projects_every_resident_generation_and_overflow() {
    // Break caught: admission omits the retiring generation, bases, liveness,
    // query workspaces, or checked overflow, or silently borrows remote S3.
    let projection = project_v35_serving_memory(&manifest()).unwrap();
    assert_eq!(projection.bytes_per_leaf, 1_664);
    assert_eq!(projection.active_and_retiring_leaf_bytes, 1_378_124_800);
    assert_eq!(projection.active_and_retiring_tree_bytes, 67_108_864);
    assert_eq!(projection.active_and_retiring_basis_bytes, 4_718_592);
    assert_eq!(projection.liveness_plane_bytes, 25_000_000);
    assert_eq!(projection.directory_bytes, 67_108_864);
    assert_eq!(projection.object_data_cache_bytes, 159_549_376);
    assert_eq!(projection.shared_cache_bytes, 251_658_240);
    assert_eq!(projection.delta_bytes, 67_108_864);
    assert_eq!(projection.delta_mutation_directory_bytes, 32_000_000);
    assert_eq!(projection.delta_posting_reference_bytes, 16_000_000);
    assert_eq!(projection.delta_leaf_bytes, 6_890_624);
    assert_eq!(projection.delta_tree_bytes, 223_680);
    assert_eq!(projection.delta_run_overhead_bytes, 4_194_304);
    assert_eq!(projection.delta_reserved_bytes, 7_800_256);
    assert_eq!(projection.runtime_bytes, 268_435_456);
    assert_eq!(projection.query_workspace_bytes, 536_870_912);
    assert_eq!(projection.unallocated_headroom_bytes, 268_435_456);
    assert_eq!(projection.admission_budget_bytes, 2_842_461_184);
    assert_eq!(
        projection.hard_limit_bytes - projection.admission_budget_bytes,
        378_764_288
    );

    for (routing, bytes_per_leaf, leaves) in [
        (64, 640, 530_048_000),
        (128, 1_152, 954_086_400),
        (192, 1_664, 1_378_124_800),
    ] {
        let mut value = manifest();
        value.dimensions.routing = routing;
        let projected = project_v35_serving_memory(&value).unwrap();
        assert_eq!(projected.bytes_per_leaf, bytes_per_leaf);
        assert_eq!(projected.active_and_retiring_leaf_bytes, leaves);
    }

    let mut two_patches = manifest();
    two_patches.patches_per_leaf = 2;
    assert!(project_v35_serving_memory(&two_patches).is_err());
    two_patches.leaf_count = 100_000;
    let two_patch_projection = project_v35_serving_memory(&two_patches).unwrap();
    assert_eq!(two_patch_projection.bytes_per_leaf, 2 * (8 * 192 + 128));

    let mut limit_crossing = manifest();
    limit_crossing.dimensions.routing = 128;
    limit_crossing.leaf_count = 763_221;
    assert!(project_v35_serving_memory(&limit_crossing).is_ok());
    limit_crossing.leaf_count += 1;
    assert!(project_v35_serving_memory(&limit_crossing).is_err());

    let mut overflow = manifest();
    overflow.leaf_count = u64::MAX;
    assert!(project_v35_serving_memory(&overflow).is_err());
    overflow = manifest();
    overflow.tree_node_count = u64::MAX;
    assert!(project_v35_serving_memory(&overflow).is_err());
}
