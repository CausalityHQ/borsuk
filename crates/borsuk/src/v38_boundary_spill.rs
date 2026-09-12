//! Query-blind hyperplane-boundary spill qualification for V38.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{BorsukError, Result};

const V38_CORPUS_ROWS: u64 = 1_000_000;
const V38_POSTING_COUNT: u32 = 123;
const V38_MAXIMUM_ALTERNATE_ASSIGNMENTS: u64 = 250_000;
const V38_MAXIMUM_OWNERS_PER_ROW: u8 = 2;
const V38_MAXIMUM_ROWS_PER_POSTING: u32 = 10_240;
const V38_PROJECTED_RECORD_BYTES: u32 = 48;
const V38_PROJECTED_FRAMING_ALLOWANCE_BYTES: u32 = 32_768;
const V38_SELECTED_POSTINGS: u32 = 14;
const V38_AGGREGATE_GATE_PPM: u32 = 998_000;
const V38_MINIMUM_GATE_PPM: u32 = 800_000;
const V38_QUERY_COUNT: u32 = 1_000;
const V38_GT_NEIGHBORS: u32 = 100;
const V38_MAXIMUM_QUERY_SOLVER_NODES: u64 = 250_000;
const V38_MAXIMUM_SOLVER_NODES: u64 = 25_000_000;
const V38_MAXIMUM_CERTIFICATE_BYTES: u32 = 256;
const V38_MAXIMUM_WORKERS: u32 = 32;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38SpillSpec {
    corpus_rows: u64,
    posting_count: u32,
    maximum_alternate_assignments: u64,
    maximum_owners_per_row: u8,
    maximum_rows_per_posting: u32,
    projected_record_bytes: u32,
    projected_framing_allowance_bytes: u32,
    selected_postings: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V38SpillCapacity {
    total_posting_capacity: u64,
    maximum_total_assignments: u64,
    maximum_posting_payload_bytes: u64,
    maximum_complete_object_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38ArtifactIdentity {
    blake3: String,
    encoded_bytes: u64,
    role: String,
    sha256: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38OutputTarget {
    role: String,
    uri: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38ConstructionAuthority {
    algorithm: String,
    inputs: Vec<V38ArtifactIdentity>,
    metric: String,
    numeric_backend: String,
    outputs: Vec<V38OutputTarget>,
    schema: String,
    source_archive: V38ArtifactIdentity,
    source_commit: String,
    spec: V38SpillSpec,
    workers: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V38CeilingAuthority {
    aggregate_gate_ppm: u32,
    construction_result: V38ArtifactIdentity,
    development_ground_truth: V38ArtifactIdentity,
    gt_neighbors: u32,
    maximum_certificate_bytes: u32,
    maximum_query_solver_nodes: u64,
    maximum_solver_nodes: u64,
    minimum_gate_ppm: u32,
    posting_summary: V38ArtifactIdentity,
    query_count: u32,
    relation: V38ArtifactIdentity,
    schema: String,
    selected_postings: u32,
}

fn invalid(message: &str) -> BorsukError {
    BorsukError::InvalidStorage(message.to_owned())
}

fn valid_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_s3_object_uri(value: &str) -> bool {
    value
        .strip_prefix("s3://")
        .and_then(|object| object.split_once('/'))
        .is_some_and(|(bucket, key)| !bucket.is_empty() && !key.is_empty())
}

fn validate_artifact(identity: &V38ArtifactIdentity, role: &str) -> Result<()> {
    if identity.role != role
        || identity.encoded_bytes == 0
        || !valid_lower_hex_digest(&identity.sha256)
        || !valid_lower_hex_digest(&identity.blake3)
        || !valid_s3_object_uri(&identity.uri)
    {
        return Err(invalid("V38 artifact authority differs"));
    }
    Ok(())
}

pub(crate) fn project_v38_spill_capacity(spec: &V38SpillSpec) -> Result<V38SpillCapacity> {
    let total_posting_capacity = u64::from(spec.posting_count)
        .checked_mul(u64::from(spec.maximum_rows_per_posting))
        .ok_or_else(|| invalid("V38 posting capacity overflows"))?;
    let maximum_total_assignments = spec
        .corpus_rows
        .checked_add(spec.maximum_alternate_assignments)
        .ok_or_else(|| invalid("V38 assignment capacity overflows"))?;
    let maximum_posting_payload_bytes = u64::from(spec.maximum_rows_per_posting)
        .checked_mul(u64::from(spec.projected_record_bytes))
        .ok_or_else(|| invalid("V38 posting payload overflows"))?;
    let maximum_complete_object_bytes = maximum_posting_payload_bytes
        .checked_add(u64::from(spec.projected_framing_allowance_bytes))
        .ok_or_else(|| invalid("V38 complete object projection overflows"))?;
    Ok(V38SpillCapacity {
        total_posting_capacity,
        maximum_total_assignments,
        maximum_posting_payload_bytes,
        maximum_complete_object_bytes,
    })
}

pub(crate) fn validate_v38_spill_spec(spec: &V38SpillSpec) -> Result<()> {
    if spec.corpus_rows != V38_CORPUS_ROWS
        || spec.posting_count != V38_POSTING_COUNT
        || spec.maximum_alternate_assignments != V38_MAXIMUM_ALTERNATE_ASSIGNMENTS
        || spec.maximum_owners_per_row != V38_MAXIMUM_OWNERS_PER_ROW
        || spec.maximum_rows_per_posting != V38_MAXIMUM_ROWS_PER_POSTING
        || spec.projected_record_bytes != V38_PROJECTED_RECORD_BYTES
        || spec.projected_framing_allowance_bytes != V38_PROJECTED_FRAMING_ALLOWANCE_BYTES
        || spec.selected_postings != V38_SELECTED_POSTINGS
    {
        return Err(invalid("V38 spill specification differs"));
    }
    let projection = project_v38_spill_capacity(spec)?;
    if projection.total_posting_capacity < projection.maximum_total_assignments
        || projection.maximum_posting_payload_bytes != 491_520
        || projection.maximum_complete_object_bytes != 524_288
    {
        return Err(invalid("V38 spill capacity differs"));
    }
    Ok(())
}

fn validate_construction_authority(authority: &V38ConstructionAuthority) -> Result<()> {
    if authority.schema != "borsuk-v38-boundary-spill-authority-v1"
        || authority.algorithm != "query-blind-hyperplane-boundary-spill-v1"
        || authority.metric != "squared-l2"
        || !matches!(
            authority.numeric_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
        || authority.workers == 0
        || authority.workers > V38_MAXIMUM_WORKERS
        || !valid_lower_hex_digest(&authority.source_commit)
    {
        return Err(invalid("V38 construction authority differs"));
    }
    validate_v38_spill_spec(&authority.spec)?;
    validate_artifact(&authority.source_archive, "source-archive")?;
    let roles = [
        "v37-authority",
        "v37-construction-result",
        "v37-tree",
        "v37-ownership",
        "source",
    ];
    if authority.inputs.len() != roles.len() {
        return Err(invalid("V38 construction input authority differs"));
    }
    for (identity, role) in authority.inputs.iter().zip(roles) {
        validate_artifact(identity, role)?;
    }
    let output_roles = ["spill-relation", "spill-postings"];
    if authority.outputs.len() != output_roles.len() {
        return Err(invalid("V38 construction output authority differs"));
    }
    for (output, role) in authority.outputs.iter().zip(output_roles) {
        if output.role != role || !valid_s3_object_uri(&output.uri) {
            return Err(invalid("V38 construction output authority differs"));
        }
    }
    let mut roles = BTreeSet::new();
    let mut uris = BTreeSet::new();
    for (role, uri) in authority
        .inputs
        .iter()
        .map(|item| (item.role.as_str(), item.uri.as_str()))
        .chain(std::iter::once((
            authority.source_archive.role.as_str(),
            authority.source_archive.uri.as_str(),
        )))
        .chain(
            authority
                .outputs
                .iter()
                .map(|item| (item.role.as_str(), item.uri.as_str())),
        )
    {
        if !roles.insert(role) || !uris.insert(uri) {
            return Err(invalid("V38 construction roles overlap"));
        }
    }
    Ok(())
}

fn validate_ceiling_authority(authority: &V38CeilingAuthority) -> Result<()> {
    if authority.schema != "borsuk-v38-boundary-spill-ceiling-authority-v1"
        || authority.aggregate_gate_ppm != V38_AGGREGATE_GATE_PPM
        || authority.minimum_gate_ppm != V38_MINIMUM_GATE_PPM
        || authority.query_count != V38_QUERY_COUNT
        || authority.gt_neighbors != V38_GT_NEIGHBORS
        || authority.selected_postings != V38_SELECTED_POSTINGS
        || authority.maximum_query_solver_nodes != V38_MAXIMUM_QUERY_SOLVER_NODES
        || authority.maximum_solver_nodes != V38_MAXIMUM_SOLVER_NODES
        || authority.maximum_certificate_bytes != V38_MAXIMUM_CERTIFICATE_BYTES
    {
        return Err(invalid("V38 ceiling authority differs"));
    }
    for (identity, role) in [
        (&authority.construction_result, "v38-construction-result"),
        (&authority.development_ground_truth, "gt100"),
        (&authority.posting_summary, "spill-postings"),
        (&authority.relation, "spill-relation"),
    ] {
        validate_artifact(identity, role)?;
    }
    let uris = [
        authority.construction_result.uri.as_str(),
        authority.development_ground_truth.uri.as_str(),
        authority.posting_summary.uri.as_str(),
        authority.relation.uri.as_str(),
    ];
    if uris.into_iter().collect::<BTreeSet<_>>().len() != uris.len() {
        return Err(invalid("V38 ceiling artifact roles overlap"));
    }
    Ok(())
}

fn canonical_json_value(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(object) => {
            let mut entries: Vec<_> = object.into_iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json_value(value)))
                    .collect(),
            )
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonical_json_value).collect())
        }
        value => value,
    }
}

fn canonical_bytes<T: Serialize>(value: &T, context: &str) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value)
        .map_err(|error| invalid(&format!("V38 {context} serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V38 {context} serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn canonical_v38_construction_authority_bytes(
    authority: &V38ConstructionAuthority,
) -> Result<Vec<u8>> {
    validate_construction_authority(authority)?;
    canonical_bytes(authority, "construction authority")
}

pub(crate) fn canonical_v38_ceiling_authority_bytes(
    authority: &V38CeilingAuthority,
) -> Result<Vec<u8>> {
    validate_ceiling_authority(authority)?;
    canonical_bytes(authority, "ceiling authority")
}

/// Validate and return the canonical bytes of a V38 construction authority.
#[doc(hidden)]
pub fn validate_v38_construction_authority_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let authority: V38ConstructionAuthority = serde_json::from_slice(bytes).map_err(|error| {
        invalid(&format!(
            "V38 construction authority parsing failed: {error}"
        ))
    })?;
    let canonical = canonical_v38_construction_authority_bytes(&authority)?;
    if canonical != bytes {
        return Err(invalid(
            "V38 construction authority bytes are not canonical",
        ));
    }
    Ok(canonical)
}

/// Validate and return the canonical bytes of a V38 ceiling authority.
#[doc(hidden)]
pub fn validate_v38_ceiling_authority_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let authority: V38CeilingAuthority = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V38 ceiling authority parsing failed: {error}")))?;
    let canonical = canonical_v38_ceiling_authority_bytes(&authority)?;
    if canonical != bytes {
        return Err(invalid("V38 ceiling authority bytes are not canonical"));
    }
    Ok(canonical)
}

#[cfg(test)]
mod tests {
    use super::{
        V38ArtifactIdentity, V38CeilingAuthority, V38ConstructionAuthority, V38OutputTarget,
        V38SpillSpec, canonical_v38_ceiling_authority_bytes,
        canonical_v38_construction_authority_bytes, project_v38_spill_capacity,
        validate_v38_ceiling_authority_bytes, validate_v38_construction_authority_bytes,
        validate_v38_spill_spec,
    };

    fn digest(byte: u8) -> String {
        format!("{byte:02x}").repeat(32)
    }

    fn artifact(role: &str, byte: u8) -> V38ArtifactIdentity {
        V38ArtifactIdentity {
            blake3: digest(byte.wrapping_add(1)),
            encoded_bytes: 100 + u64::from(byte),
            role: role.to_owned(),
            sha256: digest(byte),
            uri: format!("s3://borsuk-test/v38/{role}-{byte}"),
        }
    }

    fn registered_spec() -> V38SpillSpec {
        V38SpillSpec {
            corpus_rows: 1_000_000,
            posting_count: 123,
            maximum_alternate_assignments: 250_000,
            maximum_owners_per_row: 2,
            maximum_rows_per_posting: 10_240,
            projected_record_bytes: 48,
            projected_framing_allowance_bytes: 32_768,
            selected_postings: 14,
        }
    }

    fn construction_authority() -> V38ConstructionAuthority {
        V38ConstructionAuthority {
            algorithm: "query-blind-hyperplane-boundary-spill-v1".to_owned(),
            inputs: vec![
                artifact("v37-authority", 1),
                artifact("v37-construction-result", 3),
                artifact("v37-tree", 5),
                artifact("v37-ownership", 7),
                artifact("source", 9),
            ],
            metric: "squared-l2".to_owned(),
            numeric_backend: "aarch64-neon-fma".to_owned(),
            outputs: vec![
                V38OutputTarget {
                    role: "spill-relation".to_owned(),
                    uri: "s3://borsuk-test/v38/spill-relation.parquet".to_owned(),
                },
                V38OutputTarget {
                    role: "spill-postings".to_owned(),
                    uri: "s3://borsuk-test/v38/spill-postings.parquet".to_owned(),
                },
            ],
            schema: "borsuk-v38-boundary-spill-authority-v1".to_owned(),
            source_archive: artifact("source-archive", 11),
            source_commit: digest(13),
            spec: registered_spec(),
            workers: 32,
        }
    }

    fn ceiling_authority() -> V38CeilingAuthority {
        V38CeilingAuthority {
            aggregate_gate_ppm: 998_000,
            construction_result: artifact("v38-construction-result", 21),
            development_ground_truth: artifact("gt100", 23),
            gt_neighbors: 100,
            maximum_certificate_bytes: 256,
            maximum_query_solver_nodes: 250_000,
            maximum_solver_nodes: 25_000_000,
            minimum_gate_ppm: 800_000,
            posting_summary: artifact("spill-postings", 25),
            query_count: 1_000,
            relation: artifact("spill-relation", 27),
            schema: "borsuk-v38-boundary-spill-ceiling-authority-v1".to_owned(),
            selected_postings: 14,
        }
    }

    #[test]
    fn v38_boundary_authority_projects_exact_registered_capacity() {
        let projection = project_v38_spill_capacity(&registered_spec()).unwrap();
        assert_eq!(projection.total_posting_capacity, 1_259_520);
        assert_eq!(projection.maximum_total_assignments, 1_250_000);
        assert_eq!(projection.maximum_posting_payload_bytes, 491_520);
        assert_eq!(projection.maximum_complete_object_bytes, 524_288);
        validate_v38_spill_spec(&registered_spec()).unwrap();
    }

    #[test]
    fn v38_boundary_authority_rejects_nonregistered_and_overflowing_specs() {
        for mutate in [
            |spec: &mut V38SpillSpec| spec.maximum_alternate_assignments = 250_001,
            |spec: &mut V38SpillSpec| spec.maximum_owners_per_row = 3,
            |spec: &mut V38SpillSpec| spec.maximum_rows_per_posting = 10_241,
            |spec: &mut V38SpillSpec| spec.projected_record_bytes = 49,
            |spec: &mut V38SpillSpec| spec.selected_postings = 15,
        ] {
            let mut changed = registered_spec();
            mutate(&mut changed);
            assert!(validate_v38_spill_spec(&changed).is_err());
        }
        let mut overflow = registered_spec();
        overflow.corpus_rows = u64::MAX;
        overflow.posting_count = u32::MAX;
        assert!(project_v38_spill_capacity(&overflow).is_err());
    }

    #[test]
    fn v38_boundary_authority_canonicalizes_and_binds_construction_roles() {
        let authority = construction_authority();
        let bytes = canonical_v38_construction_authority_bytes(&authority).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert_eq!(
            validate_v38_construction_authority_bytes(&bytes).unwrap(),
            bytes
        );

        let mut changed = authority.clone();
        changed.inputs[0].role = "construction-manifest".to_owned();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
        let mut changed = authority.clone();
        changed.outputs[0].uri = changed.inputs[0].uri.clone();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
        let mut changed = authority;
        changed.numeric_backend = "scalar-control".to_owned();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
    }

    #[test]
    fn v38_boundary_authority_rejects_schema_type_and_digest_drift() {
        let bytes = canonical_v38_construction_authority_bytes(&construction_authority()).unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("extra".to_owned(), true.into());
        assert!(
            validate_v38_construction_authority_bytes(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["workers"] = "32".into();
        assert!(
            validate_v38_construction_authority_bytes(&serde_json::to_vec(&value).unwrap())
                .is_err()
        );
        let mut changed = construction_authority();
        changed.inputs[2].sha256 = digest(0xff).to_uppercase();
        assert!(canonical_v38_construction_authority_bytes(&changed).is_err());
    }

    #[test]
    fn v38_boundary_authority_ceiling_binds_exact_solver_and_artifact_roles() {
        let authority = ceiling_authority();
        let bytes = canonical_v38_ceiling_authority_bytes(&authority).unwrap();
        assert_eq!(validate_v38_ceiling_authority_bytes(&bytes).unwrap(), bytes);
        for mutate in [
            |value: &mut V38CeilingAuthority| value.selected_postings = 13,
            |value: &mut V38CeilingAuthority| value.maximum_solver_nodes += 1,
            |value: &mut V38CeilingAuthority| value.aggregate_gate_ppm = 997_999,
            |value: &mut V38CeilingAuthority| value.minimum_gate_ppm = 799_999,
        ] {
            let mut changed = authority.clone();
            mutate(&mut changed);
            assert!(canonical_v38_ceiling_authority_bytes(&changed).is_err());
        }
        let mut changed = authority;
        changed.relation.role = "spill-postings".to_owned();
        assert!(canonical_v38_ceiling_authority_bytes(&changed).is_err());
    }
}
