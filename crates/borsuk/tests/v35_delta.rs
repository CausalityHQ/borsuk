//! V35 conditional generation-publication contracts.

use std::collections::BTreeMap;

use borsuk::{
    Result, V35ArtifactIdentity, V35ConditionalHeadWrite, V35Dimensions, V35GenerationManifest,
    V35GenerationPublicationSink, V35ProjectionArm, V35PublicationOutcome, V35RemoteCodeRate,
    V35StoredArtifactIdentity, publish_v35_generation,
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
        format: "borsuk-v35-generation-v3".to_owned(),
        leaf_count: 400_000,
        metric: "squared-l2".to_owned(),
        normalization: "none".to_owned(),
        patches_per_leaf: 1,
        projection: V35ProjectionArm::Pca,
        remote_code: V35RemoteCodeRate {
            bits_per_dimension: 4,
            bytes_per_row: 384,
        },
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

fn manifest_fixture() -> (Vec<u8>, V35ArtifactIdentity) {
    let mut bytes =
        serde_json::to_vec(&canonical(serde_json::to_value(manifest()).unwrap())).unwrap();
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

struct PublicationSink {
    events: Vec<String>,
    head_result: V35ConditionalHeadWrite,
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
        assert_eq!(parsed["format"], "borsuk-v35-generation-head-v1");
        assert_eq!(parsed["manifest"]["version_id"], "manifest-version-07");
        assert!(bytes.ends_with(b"\n"));
        self.events.push(format!(
            "head:{}",
            expected_version.unwrap_or("create-if-absent")
        ));
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
        reject_manifest: true,
    };
    assert!(publish_v35_generation(&bytes, &identity, None, &mut sink).is_err());
    assert_eq!(sink.events.len(), 1);

    let mut sink = PublicationSink {
        events: Vec::new(),
        head_result: V35ConditionalHeadWrite::Committed {
            version_id: String::new(),
        },
        reject_manifest: false,
    };
    assert_eq!(
        publish_v35_generation(&bytes, &identity, Some("head-version-02"), &mut sink).unwrap(),
        V35PublicationOutcome::Indeterminate
    );
    assert_eq!(sink.events.len(), 2);
}
