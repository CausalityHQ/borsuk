//! V35 conditional generation-publication contracts.

use std::collections::BTreeMap;

use borsuk::{
    Result, V35ArtifactIdentity, V35ConditionalHeadWrite, V35DeltaManifest, V35DeltaRun,
    V35Dimensions, V35GenerationManifest, V35GenerationPublicationSink, V35ProjectionArm,
    V35PublicationOutcome, V35RemoteCodeRate, V35SnapshotEntry, V35SnapshotVisibility,
    V35StoredArtifactIdentity, decode_v35_head, publish_v35_delta, publish_v35_generation,
    seal_v35_delta, validate_v35_delta_base, validate_v35_delta_manifest,
    validate_v35_delta_visibility,
};
use serde_json::Value;
use sha2::{Digest, Sha256};

fn digest(byte: u8) -> String {
    format!("{byte:02x}").repeat(32)
}

fn object(role: &str, ordinal: u8) -> V35ArtifactIdentity {
    V35ArtifactIdentity {
        digest: digest(ordinal),
        digest_algorithm: "sha256".to_owned(),
        length: u64::from(ordinal) + 1,
        role: role.to_owned(),
        uri: format!("s3://frozen-v35/{role}/{ordinal}"),
    }
}

fn stored(role: &str, ordinal: u8) -> V35StoredArtifactIdentity {
    V35StoredArtifactIdentity {
        object: object(role, ordinal),
        version_id: format!("stored-version-{ordinal:02}"),
    }
}

fn manifest() -> V35GenerationManifest {
    let roles = [
        "active-liveness",
        "active-projection-basis",
        "active-routing",
        "code-directory",
        "page-directory",
        "retiring-liveness",
        "retiring-projection-basis",
        "retiring-routing",
        "snapshot-visibility-directory",
    ];
    V35GenerationManifest {
        artifacts: roles
            .iter()
            .enumerate()
            .map(|(index, role)| V35StoredArtifactIdentity {
                object: object(role, u8::try_from(index + 1).unwrap()),
                version_id: format!("stored-version-{index:02}"),
            })
            .collect(),
        dimensions: V35Dimensions {
            routing: 192,
            source: 768,
        },
        format: "borsuk-v35-generation-v4".to_owned(),
        leaf_count: 400_000,
        metric: "squared-l2".to_owned(),
        normalization: "none".to_owned(),
        patches_per_leaf: 1,
        projection: V35ProjectionArm::Pca,
        remote_code: V35RemoteCodeRate {
            bits_per_dimension: 4,
            bytes_per_row: 384,
        },
        sequence_horizon: 41,
        source_archive_sha256: digest(42),
        source_id: "frozen-source".to_owned(),
        tree_node_count: 60_000,
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

fn manifest_fixture_for(manifest: &V35GenerationManifest) -> (Vec<u8>, V35ArtifactIdentity) {
    let mut bytes =
        serde_json::to_vec(&canonical(serde_json::to_value(manifest).unwrap())).unwrap();
    bytes.push(b'\n');
    let identity = V35ArtifactIdentity {
        digest: format!("{:x}", Sha256::digest(&bytes)),
        digest_algorithm: "sha256".to_owned(),
        length: bytes.len() as u64,
        role: "generation-manifest".to_owned(),
        uri: "s3://frozen-v35/generations/g01/manifest.json".to_owned(),
    };
    (bytes, identity)
}

fn manifest_fixture() -> (Vec<u8>, V35ArtifactIdentity) {
    manifest_fixture_for(&manifest())
}

struct PublicationSink {
    events: Vec<String>,
    head_result: V35ConditionalHeadWrite,
    head_bytes: Vec<u8>,
    reject_manifest: bool,
}

impl V35GenerationPublicationSink for PublicationSink {
    fn write_manifest(&mut self, identity: &V35ArtifactIdentity, bytes: &[u8]) -> Result<String> {
        self.events.push(format!("manifest:{}", identity.digest));
        if self.reject_manifest {
            return Err(borsuk::BorsukError::InvalidStorage(
                "injected manifest failure".to_owned(),
            ));
        }
        assert_eq!(identity.length, bytes.len() as u64);
        Ok("manifest-version-07".to_owned())
    }

    fn compare_and_swap_head(
        &mut self,
        expected_version: Option<&str>,
        bytes: &[u8],
    ) -> Result<V35ConditionalHeadWrite> {
        let parsed: Value = serde_json::from_slice(bytes).unwrap();
        assert_eq!(parsed["format"], "borsuk-v35-generation-head-v2");
        assert_eq!(parsed["manifest"]["version_id"], "manifest-version-07");
        assert!(bytes.ends_with(b"\n"));
        self.events.push(format!(
            "head:{}",
            expected_version.unwrap_or("create-if-absent")
        ));
        self.head_bytes = bytes.to_vec();
        Ok(self.head_result.clone())
    }
}

#[test]
fn v35_publication_writes_manifest_then_conditionally_publishes_head_once() {
    // Break caught: CURRENT is visible before the immutable manifest, or a
    // first-generation publish cannot use create-if-absent semantics.
    let (bytes, identity) = manifest_fixture();
    let mut sink = PublicationSink {
        events: Vec::new(),
        head_result: V35ConditionalHeadWrite::Committed {
            version_id: "head-version-03".to_owned(),
        },
        head_bytes: Vec::new(),
        reject_manifest: false,
    };
    let outcome = publish_v35_generation(&bytes, &identity, None, &mut sink).unwrap();
    assert!(matches!(outcome, V35PublicationOutcome::Committed { .. }));
    assert_eq!(
        sink.events,
        [
            format!("manifest:{}", identity.digest),
            "head:create-if-absent".to_owned(),
        ]
    );
    let decoded = decode_v35_head(&sink.head_bytes).unwrap();
    assert_eq!(decoded.object, identity);
    assert_eq!(decoded.version_id, "manifest-version-07");
}

#[test]
fn v35_publication_preserves_conflict_and_indeterminate_without_retry() {
    // Break caught: an ambiguous conditional write is retried or classified as
    // conflict, making a reachable generation eligible for unsafe cleanup.
    let (bytes, identity) = manifest_fixture();
    for (head_result, expected) in [
        (
            V35ConditionalHeadWrite::Conflict,
            V35PublicationOutcome::Conflict,
        ),
        (
            V35ConditionalHeadWrite::Indeterminate,
            V35PublicationOutcome::Indeterminate,
        ),
    ] {
        let mut sink = PublicationSink {
            events: Vec::new(),
            head_result,
            head_bytes: Vec::new(),
            reject_manifest: false,
        };
        let outcome =
            publish_v35_generation(&bytes, &identity, Some("head-version-02"), &mut sink).unwrap();
        assert_eq!(outcome, expected);
        assert_eq!(sink.events.len(), 2);
    }

    let mut sink = PublicationSink {
        events: Vec::new(),
        head_result: V35ConditionalHeadWrite::Conflict,
        head_bytes: Vec::new(),
        reject_manifest: true,
    };
    assert!(publish_v35_generation(&bytes, &identity, None, &mut sink).is_err());
    assert_eq!(sink.events.len(), 1);

    let mut sink = PublicationSink {
        events: Vec::new(),
        head_result: V35ConditionalHeadWrite::Committed {
            version_id: String::new(),
        },
        head_bytes: Vec::new(),
        reject_manifest: false,
    };
    assert_eq!(
        publish_v35_generation(&bytes, &identity, Some("head-version-02"), &mut sink).unwrap(),
        V35PublicationOutcome::Indeterminate
    );
    assert_eq!(sink.events.len(), 2);
}

#[test]
fn v35_generation_head_reader_rejects_schema_identity_and_canonical_drift() {
    // Break caught: serving accepts a rewritten or malformed head before it
    // has authenticated the exact immutable manifest capability.
    let (bytes, identity) = manifest_fixture();
    let mut sink = PublicationSink {
        events: Vec::new(),
        head_result: V35ConditionalHeadWrite::Committed {
            version_id: "head-version-03".to_owned(),
        },
        head_bytes: Vec::new(),
        reject_manifest: false,
    };
    publish_v35_generation(&bytes, &identity, None, &mut sink).unwrap();
    assert!(decode_v35_head(&sink.head_bytes).is_ok());

    let parsed: Value = serde_json::from_slice(&sink.head_bytes).unwrap();
    let mut mutations = Vec::new();
    let mut changed = parsed.clone();
    changed["format"] = Value::String("borsuk-v35-generation-head-v0".to_owned());
    mutations.push(changed);
    let mut changed = parsed.clone();
    changed["extra"] = Value::Bool(true);
    mutations.push(changed);
    let mut changed = parsed.clone();
    changed["manifest"]["version_id"] = Value::String(String::new());
    mutations.push(changed);
    let mut changed = parsed;
    changed["manifest"]["object"]["role"] = Value::String("page-directory".to_owned());
    mutations.push(changed);
    let mut changed: Value = serde_json::from_slice(&sink.head_bytes).unwrap();
    changed["manifest"]["object"]["uri"] =
        Value::String(format!("s3://frozen-v35/{}", "x".repeat(65_536)));
    mutations.push(changed);

    for mutation in mutations {
        let mut mutated = serde_json::to_vec(&canonical(mutation)).unwrap();
        mutated.push(b'\n');
        assert!(decode_v35_head(&mutated).is_err());
    }
    assert!(decode_v35_head(&sink.head_bytes[..sink.head_bytes.len() - 1]).is_err());
    let mut noncanonical = b" {".to_vec();
    noncanonical.extend_from_slice(&sink.head_bytes[1..]);
    assert!(decode_v35_head(&noncanonical).is_err());
}

fn delta_run(
    ordinal: u8,
    rows: u32,
    sequence_horizon: u64,
    base_generation_sha256: &str,
) -> V35DeltaRun {
    delta_run_range(
        ordinal,
        rows,
        sequence_horizon,
        sequence_horizon,
        base_generation_sha256,
    )
}

fn delta_run_range(
    ordinal: u8,
    rows: u32,
    sequence_floor: u64,
    sequence_horizon: u64,
    base_generation_sha256: &str,
) -> V35DeltaRun {
    V35DeltaRun {
        base_generation_sha256: base_generation_sha256.to_owned(),
        code_directory: stored("code-directory", 20 + ordinal * 3),
        leaf_count: rows.min(u32::from(ordinal) + 100),
        ordinal,
        page_directory: stored("page-directory", 21 + ordinal * 3),
        routing: stored("active-routing", 22 + ordinal * 3),
        rows,
        sequence_floor,
        sequence_horizon,
        tree_node_count: u32::from(ordinal) + 10,
    }
}

fn delta_visibility(sequence_horizon: u64) -> (V35SnapshotVisibility, V35StoredArtifactIdentity) {
    visibility_fixture(
        vec![
            V35SnapshotEntry::new(7, sequence_horizon - 1, true).unwrap(),
            V35SnapshotEntry::new(8, sequence_horizon, false).unwrap(),
        ],
        "visibility",
    )
}

fn visibility_fixture(
    entries: Vec<V35SnapshotEntry>,
    name: &str,
) -> (V35SnapshotVisibility, V35StoredArtifactIdentity) {
    let visibility = V35SnapshotVisibility::new(entries).unwrap();
    let bytes = visibility.canonical_bytes().unwrap();
    let stored = V35StoredArtifactIdentity {
        object: V35ArtifactIdentity {
            digest: format!("{:x}", Sha256::digest(&bytes)),
            digest_algorithm: "sha256".to_owned(),
            length: bytes.len() as u64,
            role: "snapshot-visibility-directory".to_owned(),
            uri: format!("s3://frozen-v35/deltas/d01/{name}.arrow"),
        },
        version_id: format!("{name}-version-01"),
    };
    (visibility, stored)
}

fn delete_visibility(sequence_horizon: u64) -> (V35SnapshotVisibility, V35StoredArtifactIdentity) {
    visibility_fixture(
        vec![
            V35SnapshotEntry::new(7, sequence_horizon - 1, false).unwrap(),
            V35SnapshotEntry::new(8, sequence_horizon, false).unwrap(),
        ],
        "delete-visibility",
    )
}

#[test]
fn v35_delta_manifest_seals_ordered_bounded_versioned_runs() {
    // Break caught: serving cannot pin one exact base-plus-ordered-delta view,
    // or a mutable/unversioned artifact enters the visible snapshot.
    let base_manifest = manifest();
    let (_, base_identity) = manifest_fixture();
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let runs = vec![
        delta_run_range(0, 200_000, 42, 200_009, &base.object.digest),
        delta_run_range(1, 300_000, 200_010, 500_009, &base.object.digest),
    ];
    let (snapshot, visibility) = delta_visibility(500_009);
    let (bytes, identity) = seal_v35_delta(
        base.clone(),
        &base_manifest,
        visibility.clone(),
        &snapshot,
        runs.clone(),
        "s3://frozen-v35/deltas/d01/manifest.json",
    )
    .unwrap();
    let decoded = validate_v35_delta_manifest(&bytes, &identity).unwrap();
    assert_eq!(
        decoded,
        V35DeltaManifest {
            base_generation: base,
            base_sequence_horizon: 41,
            format: "borsuk-v35-delta-manifest-v1".to_owned(),
            patches_per_leaf: 1,
            rows: 500_000,
            routing_dimension: 192,
            runs,
            visibility,
            tombstones: 1,
            visibility_rows: 2,
            visibility_sequence_floor: 500_008,
            visibility_sequence_horizon: 500_009,
        }
    );
    assert!(bytes.ends_with(b"\n"));
}

#[test]
fn v35_delta_manifest_binds_the_authenticated_base_sequence_horizon() {
    // Break caught: a run at or below the horizon already folded into the
    // compacted base is admitted and can resurrect a stale replacement/delete.
    let base_manifest = manifest();
    let (_, base_identity) = manifest_fixture();
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let (snapshot, visibility) = delta_visibility(43);
    let uri = "s3://frozen-v35/deltas/d01/manifest.json";

    assert!(
        seal_v35_delta(
            base.clone(),
            &base_manifest,
            visibility.clone(),
            &snapshot,
            vec![delta_run(0, 1, 41, &base.object.digest)],
            uri,
        )
        .is_err()
    );

    let mut unauthenticated_base = base_manifest;
    unauthenticated_base.sequence_horizon = 40;
    assert!(
        seal_v35_delta(
            base.clone(),
            &unauthenticated_base,
            visibility,
            &snapshot,
            vec![delta_run(0, 1, 42, &base.object.digest)],
            uri,
        )
        .is_err()
    );
}

#[test]
fn v35_delta_manifest_publishes_delete_only_snapshots_without_routing_artifacts() {
    // Break caught: deletes are forced into vector/code/page objects even
    // though latest-sequence visibility suppresses them before routing.
    let base_manifest = manifest();
    let (_, base_identity) = manifest_fixture();
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let (snapshot, visibility) = delete_visibility(43);
    let uri = "s3://frozen-v35/deltas/d01/delete-only-manifest.json";

    let (bytes, identity) = seal_v35_delta(
        base.clone(),
        &base_manifest,
        visibility,
        &snapshot,
        Vec::new(),
        uri,
    )
    .unwrap();
    let decoded = validate_v35_delta_manifest(&bytes, &identity).unwrap();
    assert_eq!(decoded.base_generation, base);
    assert_eq!(decoded.rows, 0);
    assert!(decoded.runs.is_empty());
    assert_eq!(decoded.visibility_rows, 2);
    assert_eq!(decoded.visibility_sequence_horizon, 43);
    let json: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["tombstones"], 2);

    let (live_snapshot, live_visibility) = delta_visibility(43);
    assert!(
        seal_v35_delta(
            decoded.base_generation,
            &base_manifest,
            live_visibility,
            &live_snapshot,
            Vec::new(),
            uri,
        )
        .is_err()
    );
}

#[test]
fn v35_delta_manifest_allows_later_tombstones_without_dummy_physical_runs() {
    // Break caught: a deletion later than the newest retained insertion is
    // forced into a dummy vector/code/page run solely to advance the snapshot.
    let base_manifest = manifest();
    let (_, base_identity) = manifest_fixture();
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let (snapshot, visibility) = visibility_fixture(
        vec![
            V35SnapshotEntry::new(7, 42, true).unwrap(),
            V35SnapshotEntry::new(8, 43, false).unwrap(),
        ],
        "later-delete-visibility",
    );

    let (bytes, identity) = seal_v35_delta(
        base.clone(),
        &base_manifest,
        visibility,
        &snapshot,
        vec![delta_run(0, 1, 42, &base.object.digest)],
        "s3://frozen-v35/deltas/d01/later-delete-manifest.json",
    )
    .unwrap();
    let decoded = validate_v35_delta_manifest(&bytes, &identity).unwrap();
    assert_eq!(decoded.runs[0].sequence_horizon, 42);
    assert_eq!(decoded.visibility_sequence_horizon, 43);
    assert_eq!(decoded.tombstones, 1);
}

#[test]
fn v35_delta_manifest_rejects_every_visibility_sequence_folded_into_the_base() {
    // Break caught: a newer unrelated entry raises the maximum horizon while a
    // stale per-ID tombstone suppresses a newer row already present in base.
    let base_manifest = manifest();
    let (_, base_identity) = manifest_fixture();
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let (snapshot, visibility) = visibility_fixture(
        vec![
            V35SnapshotEntry::new(7, 40, false).unwrap(),
            V35SnapshotEntry::new(8, 42, false).unwrap(),
        ],
        "stale-per-id-visibility",
    );

    assert!(
        seal_v35_delta(
            base,
            &base_manifest,
            visibility,
            &snapshot,
            Vec::new(),
            "s3://frozen-v35/deltas/d01/stale-per-id-manifest.json",
        )
        .is_err()
    );
}

#[test]
fn v35_delta_manifest_binds_run_ranges_visibility_floor_and_leaf_bytes() {
    // Break caught: overlapping compacted sequences reappear, a manifest lies
    // about its snapshot floor, or two-patch leaves exceed the delta reserve.
    let base_manifest = manifest();
    let (_, base_identity) = manifest_fixture_for(&base_manifest);
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let (snapshot, visibility) = delta_visibility(43);
    let uri = "s3://frozen-v35/deltas/d01/range-manifest.json";

    let mut stale_floor = delta_run(0, 1, 42, &base.object.digest);
    stale_floor.sequence_floor = 41;
    assert!(
        seal_v35_delta(
            base.clone(),
            &base_manifest,
            visibility.clone(),
            &snapshot,
            vec![stale_floor],
            uri,
        )
        .is_err()
    );

    let mut overlapping = delta_run(1, 1, 43, &base.object.digest);
    overlapping.sequence_floor = 42;
    assert!(
        seal_v35_delta(
            base.clone(),
            &base_manifest,
            visibility.clone(),
            &snapshot,
            vec![delta_run(0, 1, 42, &base.object.digest), overlapping],
            uri,
        )
        .is_err()
    );

    let mut two_patch_manifest = base_manifest.clone();
    two_patch_manifest.patches_per_leaf = 2;
    let (_, two_patch_identity) = manifest_fixture_for(&two_patch_manifest);
    let over_budget_two_patch_base = V35StoredArtifactIdentity {
        object: two_patch_identity,
        version_id: "two-patch-base-version-01".to_owned(),
    };
    let over_budget_digest = over_budget_two_patch_base.object.digest.clone();
    assert!(
        seal_v35_delta(
            over_budget_two_patch_base,
            &two_patch_manifest,
            visibility.clone(),
            &snapshot,
            vec![delta_run(0, 1, 42, &over_budget_digest)],
            uri,
        )
        .is_err()
    );

    two_patch_manifest.leaf_count = 100_000;
    let (_, two_patch_identity) = manifest_fixture_for(&two_patch_manifest);
    let two_patch_base = V35StoredArtifactIdentity {
        object: two_patch_identity,
        version_id: "two-patch-base-version-02".to_owned(),
    };
    let mut boundary = delta_run(0, 2_070, 42, &two_patch_base.object.digest);
    boundary.leaf_count = 2_070;
    assert!(
        seal_v35_delta(
            two_patch_base.clone(),
            &two_patch_manifest,
            visibility.clone(),
            &snapshot,
            vec![boundary.clone()],
            uri,
        )
        .is_ok()
    );
    boundary.rows = 2_071;
    boundary.leaf_count = 2_071;
    assert!(
        seal_v35_delta(
            two_patch_base,
            &two_patch_manifest,
            visibility,
            &snapshot,
            vec![boundary],
            uri,
        )
        .is_err()
    );
}

#[test]
fn v35_delta_visibility_reader_recomputes_manifest_counts_and_horizons() {
    // Break caught: a canonical delta manifest authenticates one snapshot body
    // while its serving metadata describes a different visibility state.
    let base_manifest = manifest();
    let (_, base_identity) = manifest_fixture_for(&base_manifest);
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let (snapshot, visibility) = delta_visibility(43);
    let (bytes, identity) = seal_v35_delta(
        base.clone(),
        &base_manifest,
        visibility,
        &snapshot,
        vec![delta_run(0, 1, 42, &base.object.digest)],
        "s3://frozen-v35/deltas/d01/visibility-bound-manifest.json",
    )
    .unwrap();
    let manifest = validate_v35_delta_manifest(&bytes, &identity).unwrap();
    assert!(validate_v35_delta_visibility(&manifest, &snapshot).is_ok());

    for mutation in [
        |value: &mut V35DeltaManifest| value.visibility_rows += 1,
        |value: &mut V35DeltaManifest| value.tombstones = 0,
        |value: &mut V35DeltaManifest| value.visibility_sequence_floor += 1,
        |value: &mut V35DeltaManifest| value.visibility_sequence_horizon += 1,
    ] {
        let mut changed = manifest.clone();
        mutation(&mut changed);
        assert!(validate_v35_delta_visibility(&changed, &snapshot).is_err());
    }

    let (uncovered_snapshot, uncovered_visibility) = visibility_fixture(
        vec![
            V35SnapshotEntry::new(7, 43, true).unwrap(),
            V35SnapshotEntry::new(8, 44, false).unwrap(),
        ],
        "uncovered-live-visibility",
    );
    let mut uncovered = manifest;
    uncovered.visibility = uncovered_visibility;
    uncovered.visibility_sequence_floor = 43;
    uncovered.visibility_sequence_horizon = 44;
    assert!(validate_v35_delta_visibility(&uncovered, &uncovered_snapshot).is_err());
}

#[test]
fn v35_delta_base_reader_recomputes_copied_generation_authority() {
    // Break caught: a valid-looking delta changes its base horizon, routing
    // width, or patch count while retaining an unrelated base object identity.
    let base_manifest = manifest();
    let (base_bytes, base_identity) = manifest_fixture_for(&base_manifest);
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let (snapshot, visibility) = delta_visibility(43);
    let (bytes, identity) = seal_v35_delta(
        base.clone(),
        &base_manifest,
        visibility,
        &snapshot,
        vec![delta_run(0, 1, 42, &base.object.digest)],
        "s3://frozen-v35/deltas/d01/base-bound-manifest.json",
    )
    .unwrap();
    let delta = validate_v35_delta_manifest(&bytes, &identity).unwrap();
    assert_eq!(
        validate_v35_delta_base(&delta, &base_bytes).unwrap(),
        base_manifest
    );

    for mutation in [
        |value: &mut V35DeltaManifest| value.base_sequence_horizon -= 1,
        |value: &mut V35DeltaManifest| value.routing_dimension = 128,
        |value: &mut V35DeltaManifest| value.patches_per_leaf = 2,
    ] {
        let mut changed = delta.clone();
        mutation(&mut changed);
        assert!(validate_v35_delta_base(&changed, &base_bytes).is_err());
    }

    let mut over_budget_base = base_manifest.clone();
    over_budget_base.patches_per_leaf = 2;
    let (over_budget_bytes, over_budget_identity) = manifest_fixture_for(&over_budget_base);
    let mut over_budget_delta = delta.clone();
    over_budget_delta.base_generation = V35StoredArtifactIdentity {
        object: over_budget_identity,
        version_id: "over-budget-base-version-01".to_owned(),
    };
    over_budget_delta.patches_per_leaf = 2;
    assert!(validate_v35_delta_base(&over_budget_delta, &over_budget_bytes).is_err());

    let mut changed_base = base_manifest;
    changed_base.sequence_horizon += 1;
    let mut changed_bytes =
        serde_json::to_vec(&canonical(serde_json::to_value(changed_base).unwrap())).unwrap();
    changed_bytes.push(b'\n');
    assert!(validate_v35_delta_base(&delta, &changed_bytes).is_err());
}

#[test]
fn v35_delta_manifest_reader_rejects_registered_schema_and_size_drift() {
    // Break caught: serving trusts a caller-supplied manifest body or allocates
    // an oversized JSON value before authenticating the registered capability.
    let base_manifest = manifest();
    let (_, base_identity) = manifest_fixture();
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let (snapshot, visibility) = delta_visibility(43);
    let (bytes, identity) = seal_v35_delta(
        base.clone(),
        &base_manifest,
        visibility,
        &snapshot,
        vec![delta_run(0, 1, 42, &base.object.digest)],
        "s3://frozen-v35/deltas/d01/manifest.json",
    )
    .unwrap();
    assert!(validate_v35_delta_manifest(&bytes, &identity).is_ok());

    for mutate in [
        |value: &mut V35ArtifactIdentity| value.role = "generation-manifest".to_owned(),
        |value: &mut V35ArtifactIdentity| value.digest.replace_range(0..2, "ff"),
        |value: &mut V35ArtifactIdentity| value.length += 1,
    ] {
        let mut registered = identity.clone();
        mutate(&mut registered);
        assert!(validate_v35_delta_manifest(&bytes, &registered).is_err());
    }

    let mut unknown: Value = serde_json::from_slice(&bytes).unwrap();
    unknown["legacy_runs"] = Value::Array(Vec::new());
    let mut unknown_bytes = serde_json::to_vec(&canonical(unknown)).unwrap();
    unknown_bytes.push(b'\n');
    let unknown_identity = V35ArtifactIdentity {
        digest: format!("{:x}", Sha256::digest(&unknown_bytes)),
        length: unknown_bytes.len() as u64,
        ..identity.clone()
    };
    assert!(validate_v35_delta_manifest(&unknown_bytes, &unknown_identity).is_err());

    let mut noncanonical = b" ".to_vec();
    noncanonical.extend_from_slice(&bytes);
    let noncanonical_identity = V35ArtifactIdentity {
        digest: format!("{:x}", Sha256::digest(&noncanonical)),
        length: noncanonical.len() as u64,
        ..identity
    };
    assert!(validate_v35_delta_manifest(&noncanonical, &noncanonical_identity).is_err());
    assert!(
        validate_v35_delta_manifest(&vec![b' '; 64 * 1_024 + 1], &noncanonical_identity,).is_err()
    );
}

#[test]
fn v35_delta_manifest_rejects_run_admission_order_and_authority_drift() {
    // Break caught: the reader admits more than four runs/one million rows,
    // ambiguous sequence order, or role/version/URI substitution.
    let base_manifest = manifest();
    let (_, base_identity) = manifest_fixture();
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let valid = vec![
        delta_run_range(0, 200_000, 42, 200_009, &base.object.digest),
        delta_run_range(1, 300_000, 200_010, 500_009, &base.object.digest),
    ];
    let (snapshot, visibility) = delta_visibility(500_009);
    let uri = "s3://frozen-v35/deltas/d01/manifest.json";

    let mut cases = Vec::new();
    let mut changed = valid.clone();
    changed[1].ordinal = 2;
    cases.push(changed);
    let mut changed = valid.clone();
    changed[0].routing.object.role = "retiring-routing".to_owned();
    cases.push(changed);
    let mut changed = valid.clone();
    changed[0].code_directory.version_id.clear();
    cases.push(changed);
    let mut changed = valid.clone();
    changed[1].page_directory.object.uri = changed[0].page_directory.object.uri.clone();
    cases.push(changed);
    for runs in cases {
        assert!(
            seal_v35_delta(
                base.clone(),
                &base_manifest,
                visibility.clone(),
                &snapshot,
                runs,
                uri,
            )
            .is_err()
        );
    }

    let five_runs = (0..5)
        .map(|ordinal| delta_run(ordinal, 1, u64::from(ordinal) + 42, &base.object.digest))
        .collect();
    let (five_snapshot, five_visibility) = delta_visibility(46);
    assert!(
        seal_v35_delta(
            base.clone(),
            &base_manifest,
            five_visibility,
            &five_snapshot,
            five_runs,
            uri,
        )
        .is_err()
    );

    let boundary_runs = (0..4)
        .map(|ordinal| {
            delta_run(
                ordinal,
                250_000,
                u64::from(ordinal) + 42,
                &base.object.digest,
            )
        })
        .collect();
    let (boundary_snapshot, boundary_visibility) = delta_visibility(45);
    assert!(
        seal_v35_delta(
            base.clone(),
            &base_manifest,
            boundary_visibility,
            &boundary_snapshot,
            boundary_runs,
            uri,
        )
        .is_ok()
    );

    let (overflow_snapshot, overflow_visibility) = delta_visibility(1_000_001);
    assert!(
        seal_v35_delta(
            base.clone(),
            &base_manifest,
            overflow_visibility,
            &overflow_snapshot,
            vec![
                delta_run(0, 1_000_000, 1_000_000, &base.object.digest),
                delta_run(1, 1, 1_000_001, &base.object.digest),
            ],
            uri,
        )
        .is_err()
    );
    let mut wrong_visibility = visibility.clone();
    wrong_visibility.object.role = "active-liveness".to_owned();
    assert!(
        seal_v35_delta(
            base.clone(),
            &base_manifest,
            wrong_visibility,
            &snapshot,
            valid.clone(),
            uri,
        )
        .is_err()
    );
    let mut wrong_base = base.clone();
    wrong_base.object.role = "active-routing".to_owned();
    assert!(
        seal_v35_delta(
            wrong_base,
            &base_manifest,
            visibility.clone(),
            &snapshot,
            valid,
            uri,
        )
        .is_err()
    );

    let mut oversized = delta_run_range(0, 1, 500_008, 500_009, &base.object.digest);
    oversized.code_directory.object.uri = format!("s3://frozen-v35/{}", "x".repeat(65_536));
    assert!(
        seal_v35_delta(
            base.clone(),
            &base_manifest,
            visibility.clone(),
            &snapshot,
            vec![oversized],
            uri,
        )
        .is_err()
    );

    let (stale_snapshot, stale_visibility) = delta_visibility(500_008);
    assert!(
        seal_v35_delta(
            base.clone(),
            &base_manifest,
            stale_visibility,
            &stale_snapshot,
            vec![delta_run_range(0, 1, 500_007, 500_009, &base.object.digest,)],
            uri,
        )
        .is_err()
    );

    let mut wrong_base_binding = delta_run_range(0, 1, 500_008, 500_009, &base.object.digest);
    wrong_base_binding.base_generation_sha256 = digest(99);
    assert!(
        seal_v35_delta(
            base,
            &base_manifest,
            visibility,
            &snapshot,
            vec![wrong_base_binding],
            uri,
        )
        .is_err()
    );
}

#[test]
fn v35_delta_publication_replaces_the_single_generation_head() {
    // Break caught: base and delta heads can advance independently and expose
    // a torn snapshot, or the shared head refuses an authenticated delta.
    let base_manifest = manifest();
    let (base_bytes, base_identity) = manifest_fixture();
    let base = V35StoredArtifactIdentity {
        object: base_identity,
        version_id: "base-version-01".to_owned(),
    };
    let runs = vec![delta_run_range(0, 2, 500_008, 500_009, &base.object.digest)];
    let (snapshot, visibility) = delta_visibility(500_009);
    let (bytes, identity) = seal_v35_delta(
        base.clone(),
        &base_manifest,
        visibility,
        &snapshot,
        runs,
        "s3://frozen-v35/deltas/d01/manifest.json",
    )
    .unwrap();
    let mut sink = PublicationSink {
        events: Vec::new(),
        head_result: V35ConditionalHeadWrite::Committed {
            version_id: "head-version-04".to_owned(),
        },
        head_bytes: Vec::new(),
        reject_manifest: false,
    };
    assert!(matches!(
        publish_v35_delta(
            &bytes,
            &identity,
            &base_bytes,
            &snapshot,
            Some("head-version-03"),
            &mut sink,
        )
        .unwrap(),
        V35PublicationOutcome::Committed { .. }
    ));
    let target = decode_v35_head(&sink.head_bytes).unwrap();
    assert_eq!(target.object, identity);
    assert_eq!(target.version_id, "manifest-version-07");
    assert_eq!(sink.events.len(), 2);

    let decoded = validate_v35_delta_manifest(&bytes, &identity).unwrap();
    for mutation in [
        |value: &mut V35DeltaManifest| value.base_sequence_horizon -= 1,
        |value: &mut V35DeltaManifest| value.routing_dimension = 128,
        |value: &mut V35DeltaManifest| value.patches_per_leaf = 2,
    ] {
        let mut changed = decoded.clone();
        mutation(&mut changed);
        let mut changed_bytes =
            serde_json::to_vec(&canonical(serde_json::to_value(changed).unwrap())).unwrap();
        changed_bytes.push(b'\n');
        let changed_identity = V35ArtifactIdentity {
            digest: format!("{:x}", Sha256::digest(&changed_bytes)),
            length: changed_bytes.len() as u64,
            ..identity.clone()
        };
        let mut rejecting_sink = PublicationSink {
            events: Vec::new(),
            head_result: V35ConditionalHeadWrite::Conflict,
            head_bytes: Vec::new(),
            reject_manifest: false,
        };
        assert!(
            publish_v35_delta(
                &changed_bytes,
                &changed_identity,
                &base_bytes,
                &snapshot,
                Some("head-version-04"),
                &mut rejecting_sink,
            )
            .is_err()
        );
        assert!(rejecting_sink.events.is_empty());
    }
}
