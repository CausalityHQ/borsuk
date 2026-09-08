//! V36 closed authority and bounded transport contracts.

use std::collections::BTreeMap;

use blake3::Hasher as Blake3;
use borsuk::{
    V36ArtifactIdentity, V36ChunkCeiling, V36CoarseCode, V36FineCodec, V36FunnelManifest,
    V36GeometryArm, V36PrimaryRows, V36ProjectionArm, V36RegisteredManifest, V36Replication,
    V36ResourceRequest, V36ShapeScore, V36TransportDisposition, V36TransportFragment,
    V36TransportLimits, V36TransportPosting, plan_v36_transport, project_v36_resources,
    validate_v36_manifest,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn artifact(role: &str, ordinal: u8) -> V36ArtifactIdentity {
    V36ArtifactIdentity {
        blake3: digest(ordinal.saturating_add(64)),
        encoded_bytes: u64::from(ordinal) + 1,
        role: role.to_owned(),
        sha256: digest(ordinal),
        uri: format!("s3://frozen-v36/{role}/{ordinal}"),
    }
}

fn manifest() -> V36FunnelManifest {
    V36FunnelManifest {
        artifacts: vec![
            artifact("dataset-authority", 1),
            artifact("source-parquet", 2),
            artifact("projection", 3),
            artifact("super-centroids", 4),
            artifact("posting-summaries", 5),
            artifact("coarse-codebook", 6),
            artifact("posting-directory", 7),
            artifact("coarse-directory", 8),
            artifact("fine-directory", 9),
            artifact("visibility-directory", 10),
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

fn manifest_bytes(value: &V36FunnelManifest) -> Vec<u8> {
    let mut bytes = serde_json::to_vec(&canonical(serde_json::to_value(value).unwrap())).unwrap();
    bytes.push(b'\n');
    bytes
}

fn registered(bytes: &[u8]) -> V36RegisteredManifest {
    V36RegisteredManifest {
        blake3: Blake3::new().update(bytes).finalize().to_hex().to_string(),
        encoded_bytes: u64::try_from(bytes.len()).unwrap(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        uri: "s3://frozen-v36/manifest.json".to_owned(),
    }
}

#[test]
fn v36_funnel_authority_accepts_only_the_closed_arm_matrix_and_exact_bytes() {
    // Break caught: a legacy/default arm, malformed identity, or noncanonical
    // manifest reaches resource allocation or scientific evaluation.
    let geometry = [
        (V36PrimaryRows::Control256, V36Replication::Single),
        (V36PrimaryRows::Control256, V36Replication::DoubleControl),
        (V36PrimaryRows::Posting4096, V36Replication::Single),
        (
            V36PrimaryRows::Posting4096,
            V36Replication::ClosureEpsilon05,
        ),
        (
            V36PrimaryRows::Posting4096,
            V36Replication::ClosureEpsilon15,
        ),
        (
            V36PrimaryRows::Posting4096,
            V36Replication::ClosureEpsilon30,
        ),
        (V36PrimaryRows::Posting8192, V36Replication::Single),
        (
            V36PrimaryRows::Posting8192,
            V36Replication::ClosureEpsilon05,
        ),
        (
            V36PrimaryRows::Posting8192,
            V36Replication::ClosureEpsilon15,
        ),
        (
            V36PrimaryRows::Posting8192,
            V36Replication::ClosureEpsilon30,
        ),
    ];
    for (primary_rows, replication) in geometry {
        let mut value = manifest();
        value.shape_score = if primary_rows == V36PrimaryRows::Control256 {
            V36ShapeScore::Centroid
        } else {
            V36ShapeScore::Rank4
        };
        value.geometry = V36GeometryArm {
            primary_rows,
            replication,
        };
        let bytes = manifest_bytes(&value);
        assert_eq!(
            validate_v36_manifest(&bytes, &registered(&bytes)).unwrap(),
            value
        );
    }

    for coarse_code in [V36CoarseCode::ProjectedF32Diagnostic, V36CoarseCode::Sign24] {
        let mut value = manifest();
        value.coarse_code = coarse_code;
        value
            .artifacts
            .retain(|artifact| artifact.role != "coarse-codebook");
        let bytes = manifest_bytes(&value);
        assert_eq!(
            validate_v36_manifest(&bytes, &registered(&bytes)).unwrap(),
            value
        );
    }
    let mut missing_codebook = manifest();
    missing_codebook
        .artifacts
        .retain(|artifact| artifact.role != "coarse-codebook");
    let bytes = manifest_bytes(&missing_codebook);
    assert!(validate_v36_manifest(&bytes, &registered(&bytes)).is_err());

    for mutate in [
        |value: &mut V36FunnelManifest| value.format = "borsuk-v36-funnel-manifest-v2".to_owned(),
        |value: &mut V36FunnelManifest| value.claim_eligible = true,
        |value: &mut V36FunnelManifest| value.projection_dimensions = 96,
        |value: &mut V36FunnelManifest| value.projection = V36ProjectionArm::Srht192 { seed: 35 },
        |value: &mut V36FunnelManifest| value.source_dimensions = 0,
        |value: &mut V36FunnelManifest| value.unique_k = 2_049,
        |value: &mut V36FunnelManifest| {
            value.geometry = V36GeometryArm {
                primary_rows: V36PrimaryRows::Control256,
                replication: V36Replication::ClosureEpsilon15,
            }
        },
        |value: &mut V36FunnelManifest| {
            value.geometry = V36GeometryArm {
                primary_rows: V36PrimaryRows::Posting4096,
                replication: V36Replication::DoubleControl,
            }
        },
        |value: &mut V36FunnelManifest| value.artifacts[0].sha256 = digest(0),
        |value: &mut V36FunnelManifest| value.artifacts[0].blake3.truncate(62),
        |value: &mut V36FunnelManifest| value.artifacts[0].encoded_bytes = 0,
        |value: &mut V36FunnelManifest| value.artifacts[1].role = value.artifacts[0].role.clone(),
        |value: &mut V36FunnelManifest| value.artifacts[1].uri = value.artifacts[0].uri.clone(),
    ] {
        let mut value = manifest();
        mutate(&mut value);
        let bytes = manifest_bytes(&value);
        assert!(validate_v36_manifest(&bytes, &registered(&bytes)).is_err());
    }

    let baseline = manifest();
    let baseline_bytes = manifest_bytes(&baseline);
    let mut wrong_registration = registered(&baseline_bytes);
    wrong_registration.sha256.replace_range(0..2, "ff");
    assert!(validate_v36_manifest(&baseline_bytes, &wrong_registration).is_err());
    let mut wrong_registration = registered(&baseline_bytes);
    wrong_registration.blake3.replace_range(0..2, "ff");
    assert!(validate_v36_manifest(&baseline_bytes, &wrong_registration).is_err());
    assert!(
        validate_v36_manifest(
            &baseline_bytes[..baseline_bytes.len() - 1],
            &registered(&baseline_bytes),
        )
        .is_err()
    );

    let mut extra = serde_json::to_value(&baseline).unwrap();
    extra["legacy_format"] = json!("v35");
    let mut extra_bytes = serde_json::to_vec(&canonical(extra)).unwrap();
    extra_bytes.push(b'\n');
    assert!(validate_v36_manifest(&extra_bytes, &registered(&extra_bytes)).is_err());
}

fn fragment(posting: u32, ordinal: u32, kib: u64, retries: u8) -> V36TransportFragment {
    V36TransportFragment {
        blake3: digest(u8::try_from(ordinal + 33).unwrap()),
        decoded_capacity_bytes: kib * 1_024 + 4_096,
        encoded_bytes: kib * 1_024,
        first_dense_ordinal: u64::from(ordinal) * 1_000,
        fragment_ordinal: ordinal,
        response_metadata_bytes_per_attempt: 256,
        retries,
        row_count: 1_000,
        sha256: digest(u8::try_from(ordinal + 1).unwrap()),
        uri: format!("s3://frozen-v36/coarse/{posting}/{ordinal}"),
    }
}

#[test]
fn v36_funnel_authority_plans_atomic_postings_with_normal_and_retry_caps() {
    // Break caught: the planner partially admits a posting, treats concurrency
    // as fewer GETs, ignores decoder capacity/retries, or widens the envelope.
    let postings = vec![
        V36TransportPosting {
            fragments: vec![fragment(0, 0, 512, 1), fragment(0, 1, 512, 0)],
            posting_ordinal: 0,
            stored_assignment_rows: 2_000,
        },
        V36TransportPosting {
            fragments: (0..13)
                .map(|ordinal| fragment(1, ordinal, 400, 0))
                .collect(),
            posting_ordinal: 1,
            stored_assignment_rows: 13_000,
        },
        V36TransportPosting {
            fragments: vec![fragment(2, 0, 512, 0)],
            posting_ordinal: 2,
            stored_assignment_rows: 1_000,
        },
    ];
    let plan =
        plan_v36_transport(&[0, 1, 2], &postings, V36TransportLimits::qualification()).unwrap();
    assert_eq!(plan.admitted_postings, vec![0]);
    assert_eq!(plan.excluded_postings, vec![1, 2]);
    assert_eq!(plan.normal_gets, 2);
    assert_eq!(plan.normal_encoded_bytes, 1_048_576);
    assert_eq!(plan.hard_gets_with_retries, 3);
    assert_eq!(plan.hard_returned_bytes_with_retries, 1_573_632);
    assert_eq!(plan.decoded_capacity_bytes, 1_056_768);
    assert_eq!(plan.disposition, V36TransportDisposition::Determinate);

    let exact_normal = vec![V36TransportPosting {
        fragments: (0..14)
            .map(|ordinal| fragment(3, ordinal, 512, 0))
            .collect(),
        posting_ordinal: 3,
        stored_assignment_rows: 14_000,
    }];
    let plan =
        plan_v36_transport(&[3], &exact_normal, V36TransportLimits::qualification()).unwrap();
    assert_eq!(plan.normal_gets, 14);
    assert_eq!(plan.normal_encoded_bytes, 7 * 1_048_576);

    let excluded = vec![V36TransportPosting {
        fragments: (0..15)
            .map(|ordinal| fragment(4, ordinal, 400, 0))
            .collect(),
        posting_ordinal: 4,
        stored_assignment_rows: 15_000,
    }];
    let plan = plan_v36_transport(&[4], &excluded, V36TransportLimits::qualification()).unwrap();
    assert!(plan.admitted_postings.is_empty());
    assert_eq!(plan.excluded_postings, vec![4]);

    let mut retry_overflow = exact_normal;
    retry_overflow[0].fragments[0].retries = 3;
    assert_eq!(
        plan_v36_transport(&[3], &retry_overflow, V36TransportLimits::qualification())
            .unwrap()
            .disposition,
        V36TransportDisposition::Indeterminate
    );

    let mut incomplete = postings;
    incomplete[0].fragments.pop();
    assert!(plan_v36_transport(&[0], &incomplete, V36TransportLimits::qualification()).is_err());

    let mut valid_small_tail = fragment(5, 0, 1, 0);
    valid_small_tail.decoded_capacity_bytes = 64;
    let small_tail = [V36TransportPosting {
        fragments: vec![valid_small_tail],
        posting_ordinal: 5,
        stored_assignment_rows: 1_000,
    }];
    assert!(plan_v36_transport(&[5], &small_tail, V36TransportLimits::qualification()).is_ok());
}

fn mib(value: u64) -> u64 {
    value * 1_048_576
}

fn resource_request() -> V36ResourceRequest {
    V36ResourceRequest {
        active_generation_bytes: mib(32),
        compaction_new_arena_bytes: mib(64),
        compaction_old_arena_bytes: mib(64),
        decoded_cache_bytes: mib(256),
        delta_coarse_bytes: mib(256),
        delta_csr_bytes: mib(128),
        encoded_decoded_overlap_bytes: mib(8),
        fine_interval_directory_bytes: mib(32),
        hnsw_bytes: 0,
        liveness_bytes: 25_000_000,
        mean_replication_ppm: 2_000_000,
        posting_object_directory_bytes: mib(32),
        recent_fine_bytes: mib(128),
        retiring_generation_bytes: mib(32),
        retry_buffer_bytes: mib(8),
        rows: 100_000_000,
    }
}

#[test]
fn v36_funnel_authority_projects_checked_resident_and_remote_resources() {
    // Break caught: a projection omits a live arena or overlap, admits a
    // dense per-row resident table, overflows, or charges remote vector bytes
    // as process RSS.
    let value = manifest();
    let request = resource_request();
    let projected = project_v36_resources(&value, &request).unwrap();
    assert_eq!(projected.posting_count, 24_415);
    assert_eq!(projected.super_cell_count, 512);
    assert_eq!(projected.projection_bytes, 589_824);
    assert_eq!(projected.super_centroid_bytes, 393_216);
    assert_eq!(projected.posting_summary_bytes, 115_629_440);
    assert_eq!(projected.minimum_coarse_fragment_count, 18_311);
    assert_eq!(projected.minimum_fine_chunk_count, 299_073);
    assert_eq!(projected.minimum_posting_object_directory_bytes, 25_781_360);
    assert_eq!(projected.minimum_fine_interval_directory_bytes, 4_785_168);
    assert_eq!(projected.directory_bytes, 67_108_864);
    assert_eq!(projected.delta_coarse_and_csr_bytes, 402_653_184);
    assert_eq!(projected.query_workspace_bytes, 536_870_912);
    assert_eq!(projected.runtime_bytes, 536_870_912);
    assert_eq!(projected.resident_admission_bytes, 2_305_873_344);
    assert_eq!(projected.resident_limit_bytes, 3_221_225_472);
    assert_eq!(projected.remote_coarse_payload_bytes, 9_600_000_000);
    assert_eq!(projected.remote_coarse_cap_eight_bytes, 38_400_000_000);
    assert!(projected.construction_replication_pass);
    assert_eq!(projected.remote_fine_payload_bytes, 78_400_000_000);

    for (rows, primary, postings, super_cells, limit) in [
        (
            1_000_000_u64,
            V36PrimaryRows::Posting4096,
            245,
            4,
            mib(2_048),
        ),
        (
            10_000_000,
            V36PrimaryRows::Posting4096,
            2_442,
            64,
            mib(2_048),
        ),
        (
            100_000_000,
            V36PrimaryRows::Posting4096,
            24_415,
            512,
            mib(3_072),
        ),
        (1_000_000, V36PrimaryRows::Posting8192, 123, 4, mib(2_048)),
        (
            10_000_000,
            V36PrimaryRows::Posting8192,
            1_221,
            64,
            mib(2_048),
        ),
        (
            100_000_000,
            V36PrimaryRows::Posting8192,
            12_208,
            512,
            mib(3_072),
        ),
    ] {
        let mut arm = value.clone();
        arm.geometry.primary_rows = primary;
        let mut inputs = request.clone();
        inputs.rows = rows;
        inputs.liveness_bytes = rows.div_ceil(8);
        if rows < 100_000_000 {
            inputs.compaction_new_arena_bytes = 0;
            inputs.compaction_old_arena_bytes = 0;
            inputs.delta_coarse_bytes = 0;
            inputs.delta_csr_bytes = 0;
            inputs.recent_fine_bytes = 0;
            inputs.retiring_generation_bytes = 0;
        }
        let ledger = project_v36_resources(&arm, &inputs).unwrap();
        assert_eq!(ledger.posting_count, postings);
        assert_eq!(ledger.super_cell_count, super_cells);
        assert_eq!(ledger.resident_limit_bytes, limit);
    }

    for rows in [1_000_000, 10_000_000] {
        let mut write_loaded = request.clone();
        write_loaded.rows = rows;
        write_loaded.liveness_bytes = rows.div_ceil(8);
        assert!(project_v36_resources(&value, &write_loaded).is_err());
    }

    for mean_replication_ppm in [1_000_000, 1_500_000, 2_000_000, 3_000_000, 8_000_000] {
        let mut inputs = request.clone();
        inputs.mean_replication_ppm = mean_replication_ppm;
        let ledger = project_v36_resources(&value, &inputs).unwrap();
        assert_eq!(
            ledger.remote_coarse_payload_bytes,
            4_800_000_000 * u64::from(mean_replication_ppm) / 1_000_000
        );
    }

    for (codec, bytes) in [
        (V36FineCodec::Sq8, 78_400_000_000),
        (V36FineCodec::F16, 155_200_000_000),
        (V36FineCodec::SourceF32Control, 308_800_000_000),
    ] {
        let mut arm = value.clone();
        arm.fine_codec = codec;
        let mut inputs = request.clone();
        if codec == V36FineCodec::F16 {
            inputs.posting_object_directory_bytes = mib(64);
            inputs.fine_interval_directory_bytes = mib(16);
        } else if codec == V36FineCodec::SourceF32Control {
            inputs.posting_object_directory_bytes = mib(96);
            inputs.fine_interval_directory_bytes = mib(24);
        }
        assert_eq!(
            project_v36_resources(&arm, &inputs)
                .unwrap()
                .remote_fine_payload_bytes,
            bytes
        );
    }

    for mutate in [
        |v: &mut V36ResourceRequest| v.posting_object_directory_bytes = mib(129),
        |v: &mut V36ResourceRequest| v.decoded_cache_bytes = mib(257),
        |v: &mut V36ResourceRequest| v.delta_csr_bytes = mib(257),
        |v: &mut V36ResourceRequest| v.recent_fine_bytes = mib(129),
        |v: &mut V36ResourceRequest| v.rows = 99_999_999,
        |v: &mut V36ResourceRequest| v.mean_replication_ppm = 8_000_001,
        |v: &mut V36ResourceRequest| v.active_generation_bytes = u64::MAX,
    ] {
        let mut inputs = request.clone();
        mutate(&mut inputs);
        assert!(project_v36_resources(&value, &inputs).is_err());
    }

    let mut failed_construction = request.clone();
    failed_construction.mean_replication_ppm = 3_200_000;
    let failed = project_v36_resources(&value, &failed_construction).unwrap();
    assert!(!failed.construction_replication_pass);
    assert_eq!(failed.remote_coarse_payload_bytes, 15_360_000_000);

    let mut understated_directories = request;
    understated_directories.posting_object_directory_bytes = 0;
    understated_directories.fine_interval_directory_bytes = 0;
    assert!(project_v36_resources(&value, &understated_directories).is_err());
}
