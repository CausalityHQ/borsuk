//! Balanced hyperplane layout and relation-routing qualification for V37.

use crate::{BorsukError, Result};
use serde::{Deserialize, Serialize};

const V37_NODE_METADATA_BYTES: u64 = 32;
const V37_RELATION_RECORD_BYTES: u64 = 8;
const V37_MEMORY_LIMIT_BYTES: u64 = 3 * 1_073_741_824;
const V37_MAXIMUM_RELATION_NODE_VISITS: u64 = 1_024;
const V37_SELECTED_POSTINGS: u64 = 14;

/// Query-independent authority for one balanced hyperplane tree.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37TreeSpec {
    corpus_rows: u64,
    dimensions: u64,
    seed: u64,
    target_primary_rows: u64,
    training_sample_rows: u64,
    two_means_iterations: u64,
}

/// Query-independent authority for one leaf-to-posting relation plane.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37RelationSpec {
    leaf_count: u64,
    maximum_leaf_probes: u64,
    prefix_lengths: Vec<u64>,
    q_bits: u32,
    seed: u64,
}

/// Exact quota projection for a single-owner balanced tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37LayoutProjection {
    pub(crate) leaf_count: u64,
    pub(crate) internal_node_count: u64,
    pub(crate) minimum_leaf_rows: u64,
    pub(crate) maximum_leaf_rows: u64,
    pub(crate) total_rows: u64,
}

/// Resident byte projection for V37's two trees and largest relation prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37ServingProjection {
    pub(crate) ownership_tree_bytes: u64,
    pub(crate) relation_tree_bytes: u64,
    pub(crate) relation_prefix_bytes: u64,
    pub(crate) total_bytes: u64,
}

/// Exact split sizes for one recursive quota-balanced node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37ChildQuota {
    pub(crate) left_rows: u64,
    pub(crate) right_rows: u64,
    pub(crate) left_leaves: u64,
    pub(crate) right_leaves: u64,
}

/// Admission result for the resident one-million-row construction path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V37LayoutDisposition {
    Admissible,
    ResourceRejected,
}

/// Checked resident-construction memory projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37ConstructionProjection {
    pub(crate) source_decode_bytes: u64,
    pub(crate) projected_decode_bytes: u64,
    pub(crate) resident_coordinate_bytes: u64,
    pub(crate) index_bytes: u64,
    pub(crate) score_bytes: u64,
    pub(crate) sample_bytes: u64,
    pub(crate) tree_bytes: u64,
    pub(crate) relation_count_bytes: u64,
    pub(crate) relation_prefix_bytes: u64,
    pub(crate) subtotal_bytes: u64,
    pub(crate) allocator_headroom_bytes: u64,
    pub(crate) total_bytes: u64,
    pub(crate) disposition: V37LayoutDisposition,
}

/// Maximum logical query work for the registered V37 ladder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V37WorkProjection {
    pub(crate) maximum_direct_node_visits: u64,
    pub(crate) maximum_relation_node_visits: u64,
    pub(crate) maximum_relation_records: u64,
    pub(crate) selected_postings: u64,
}

/// Backend-bound numeric authority persisted with every V37 manifest.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37NumericAuthority {
    fma_backend: String,
    lane_width: u32,
    worker_count: u32,
}

/// Exact immutable identity for one V37 authority input.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37ArtifactIdentity {
    blake3: String,
    encoded_bytes: u64,
    role: String,
    sha256: String,
    uri: String,
}

/// Canonical query-independent authority for one V37 construction.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct V37AuthorityManifest {
    algorithm: String,
    metric: String,
    numeric: V37NumericAuthority,
    projection: V37ArtifactIdentity,
    relation: V37RelationSpec,
    schema: String,
    source: V37ArtifactIdentity,
    tree: V37TreeSpec,
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

fn validate_v37_artifact(identity: &V37ArtifactIdentity, role: &str) -> Result<()> {
    if identity.role != role
        || identity.encoded_bytes == 0
        || !valid_lower_hex_digest(&identity.sha256)
        || !valid_lower_hex_digest(&identity.blake3)
        || !valid_s3_object_uri(&identity.uri)
    {
        return Err(invalid("V37 artifact authority differs"));
    }
    Ok(())
}

fn validate_v37_authority(manifest: &V37AuthorityManifest) -> Result<()> {
    if manifest.schema != "borsuk-v37-relation-authority-v1"
        || manifest.algorithm != "balanced-hyperplane-relation-v1"
        || manifest.metric != "squared-l2"
        || !matches!(
            manifest.numeric.fma_backend.as_str(),
            "aarch64-neon-fma" | "x86-avx-fma"
        )
        || manifest.numeric.lane_width != 8
        || manifest.numeric.worker_count == 0
    {
        return Err(invalid("V37 manifest authority differs"));
    }
    validate_v37_specs(&manifest.tree, &manifest.relation)?;
    validate_v37_artifact(&manifest.source, "source-corpus")?;
    validate_v37_artifact(&manifest.projection, "projected-corpus")?;
    if manifest.source.uri == manifest.projection.uri {
        return Err(invalid("V37 artifact roles overlap"));
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

pub(crate) fn canonical_v37_authority_bytes(manifest: &V37AuthorityManifest) -> Result<Vec<u8>> {
    validate_v37_authority(manifest)?;
    let value = serde_json::to_value(manifest)
        .map_err(|error| invalid(&format!("V37 manifest serialization failed: {error}")))?;
    let mut bytes = serde_json::to_vec(&canonical_json_value(value))
        .map_err(|error| invalid(&format!("V37 manifest serialization failed: {error}")))?;
    bytes.push(b'\n');
    Ok(bytes)
}

pub(crate) fn parse_v37_authority_bytes(bytes: &[u8]) -> Result<V37AuthorityManifest> {
    let manifest: V37AuthorityManifest = serde_json::from_slice(bytes)
        .map_err(|error| invalid(&format!("V37 manifest parsing failed: {error}")))?;
    if canonical_v37_authority_bytes(&manifest)? != bytes {
        return Err(invalid("V37 manifest bytes are not canonical"));
    }
    Ok(manifest)
}

pub(crate) fn validate_v37_specs(tree: &V37TreeSpec, relation: &V37RelationSpec) -> Result<()> {
    if tree.corpus_rows == 0
        || tree.dimensions == 0
        || tree.target_primary_rows != 8_192
        || tree.training_sample_rows != 4_096
        || tree.two_means_iterations != 8
        || tree.seed == 0
    {
        return Err(invalid("V37 tree authority differs"));
    }
    if relation.leaf_count != 4_096
        || relation.maximum_leaf_probes != 32
        || relation.prefix_lengths != [16, 32, 64]
        || relation.q_bits != 24
        || relation.seed == 0
        || relation.seed == tree.seed
    {
        return Err(invalid("V37 relation authority differs"));
    }
    Ok(())
}

pub(crate) fn project_v37_child_quota(rows: u64, leaves: u64) -> Result<V37ChildQuota> {
    if leaves <= 1 || rows < leaves {
        return Err(invalid("V37 recursive quota authority differs"));
    }
    let left_leaves = leaves / 2;
    let right_leaves = leaves
        .checked_sub(left_leaves)
        .ok_or_else(|| invalid("V37 recursive quota underflows"))?;
    let left_rows = rows
        .checked_mul(left_leaves)
        .ok_or_else(|| invalid("V37 recursive quota overflows"))?
        / leaves;
    let right_rows = rows
        .checked_sub(left_rows)
        .ok_or_else(|| invalid("V37 recursive quota underflows"))?;
    if left_rows < left_leaves || right_rows < right_leaves {
        return Err(invalid("V37 recursive quota cannot populate leaves"));
    }
    Ok(V37ChildQuota {
        left_rows,
        right_rows,
        left_leaves,
        right_leaves,
    })
}

pub(crate) fn project_v37_layout(tree: &V37TreeSpec) -> Result<V37LayoutProjection> {
    if tree.corpus_rows == 0
        || tree.dimensions == 0
        || tree.seed == 0
        || tree.target_primary_rows != 8_192
        || tree.training_sample_rows != 4_096
        || tree.two_means_iterations != 8
    {
        return Err(invalid("V37 layout authority differs"));
    }
    let leaf_count = tree.corpus_rows.div_ceil(tree.target_primary_rows);
    let internal_node_count = leaf_count
        .checked_sub(1)
        .ok_or_else(|| invalid("V37 layout node count underflows"))?;
    let minimum_leaf_rows = tree.corpus_rows / leaf_count;
    let remainder = tree.corpus_rows % leaf_count;
    let maximum_leaf_rows = minimum_leaf_rows
        .checked_add(u64::from(remainder != 0))
        .ok_or_else(|| invalid("V37 layout row count overflows"))?;
    Ok(V37LayoutProjection {
        leaf_count,
        internal_node_count,
        minimum_leaf_rows,
        maximum_leaf_rows,
        total_rows: tree.corpus_rows,
    })
}

pub(crate) fn project_v37_work(
    tree: &V37TreeSpec,
    relation: &V37RelationSpec,
) -> Result<V37WorkProjection> {
    validate_v37_specs(tree, relation)?;
    let layout = project_v37_layout(tree)?;
    let maximum_direct_node_visits = layout
        .leaf_count
        .checked_mul(2)
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| invalid("V37 direct work projection overflows"))?;
    let maximum_relation_records = relation
        .maximum_leaf_probes
        .checked_mul(
            relation
                .prefix_lengths
                .last()
                .copied()
                .ok_or_else(|| invalid("V37 relation prefix is empty"))?,
        )
        .ok_or_else(|| invalid("V37 relation work projection overflows"))?;
    Ok(V37WorkProjection {
        maximum_direct_node_visits,
        maximum_relation_node_visits: V37_MAXIMUM_RELATION_NODE_VISITS,
        maximum_relation_records,
        selected_postings: V37_SELECTED_POSTINGS,
    })
}

fn v37_tree_bytes(internal_nodes: u64, dimensions: u64) -> Result<u64> {
    let normal_bytes = dimensions
        .checked_mul(4)
        .ok_or_else(|| invalid("V37 tree byte projection overflows"))?;
    internal_nodes
        .checked_mul(
            normal_bytes
                .checked_add(V37_NODE_METADATA_BYTES)
                .ok_or_else(|| invalid("V37 tree byte projection overflows"))?,
        )
        .ok_or_else(|| invalid("V37 tree byte projection overflows"))
}

pub(crate) fn project_v37_serving_bytes(
    tree: &V37TreeSpec,
    relation: &V37RelationSpec,
) -> Result<V37ServingProjection> {
    validate_v37_specs(tree, relation)?;
    let layout = project_v37_layout(tree)?;
    let ownership_tree_bytes = v37_tree_bytes(layout.internal_node_count, tree.dimensions)?;
    let relation_internal_nodes = relation
        .leaf_count
        .checked_sub(1)
        .ok_or_else(|| invalid("V37 relation node count underflows"))?;
    let relation_tree_bytes = v37_tree_bytes(relation_internal_nodes, tree.dimensions)?;
    let maximum_prefix = relation
        .prefix_lengths
        .last()
        .copied()
        .ok_or_else(|| invalid("V37 relation prefix is empty"))?;
    let record_bytes = relation
        .leaf_count
        .checked_mul(maximum_prefix)
        .and_then(|value| value.checked_mul(V37_RELATION_RECORD_BYTES))
        .ok_or_else(|| invalid("V37 relation byte projection overflows"))?;
    let offset_bytes = relation
        .leaf_count
        .checked_add(1)
        .and_then(|value| value.checked_mul(8))
        .ok_or_else(|| invalid("V37 relation byte projection overflows"))?;
    let relation_prefix_bytes = record_bytes
        .checked_add(offset_bytes)
        .ok_or_else(|| invalid("V37 relation byte projection overflows"))?;
    let total_bytes = ownership_tree_bytes
        .checked_add(relation_tree_bytes)
        .and_then(|value| value.checked_add(relation_prefix_bytes))
        .ok_or_else(|| invalid("V37 serving byte projection overflows"))?;
    Ok(V37ServingProjection {
        ownership_tree_bytes,
        relation_tree_bytes,
        relation_prefix_bytes,
        total_bytes,
    })
}

pub(crate) fn project_v37_construction_bytes(
    tree: &V37TreeSpec,
    relation: &V37RelationSpec,
) -> Result<V37ConstructionProjection> {
    validate_v37_specs(tree, relation)?;
    let layout = project_v37_layout(tree)?;
    let serving = project_v37_serving_bytes(tree, relation)?;
    let coordinate_bytes = tree
        .corpus_rows
        .checked_mul(tree.dimensions)
        .and_then(|value| value.checked_mul(4))
        .ok_or_else(|| invalid("V37 coordinate byte projection overflows"))?;
    let source_decode_bytes = coordinate_bytes;
    let projected_decode_bytes = coordinate_bytes;
    let resident_coordinate_bytes = coordinate_bytes;
    let index_bytes = tree
        .corpus_rows
        .checked_mul(16)
        .ok_or_else(|| invalid("V37 index byte projection overflows"))?;
    let score_bytes = tree
        .corpus_rows
        .checked_mul(4)
        .ok_or_else(|| invalid("V37 score byte projection overflows"))?;
    let sample_bytes = tree
        .training_sample_rows
        .min(tree.corpus_rows)
        .checked_mul(tree.dimensions)
        .and_then(|value| value.checked_mul(4))
        .ok_or_else(|| invalid("V37 sample byte projection overflows"))?;
    let tree_bytes = serving
        .ownership_tree_bytes
        .checked_add(serving.relation_tree_bytes)
        .ok_or_else(|| invalid("V37 tree byte projection overflows"))?;
    let relation_count_bytes = relation
        .leaf_count
        .checked_mul(layout.leaf_count)
        .and_then(|value| value.checked_mul(8))
        .ok_or_else(|| invalid("V37 relation count projection overflows"))?;
    let relation_prefix_bytes = serving.relation_prefix_bytes;
    let subtotal_bytes = [
        source_decode_bytes,
        projected_decode_bytes,
        resident_coordinate_bytes,
        index_bytes,
        score_bytes,
        sample_bytes,
        tree_bytes,
        relation_count_bytes,
        relation_prefix_bytes,
    ]
    .into_iter()
    .try_fold(0_u64, |total, value| total.checked_add(value))
    .ok_or_else(|| invalid("V37 construction byte projection overflows"))?;
    let allocator_headroom_bytes = subtotal_bytes / 4;
    let total_bytes = subtotal_bytes
        .checked_add(allocator_headroom_bytes)
        .ok_or_else(|| invalid("V37 construction byte projection overflows"))?;
    let disposition = if total_bytes <= V37_MEMORY_LIMIT_BYTES {
        V37LayoutDisposition::Admissible
    } else {
        V37LayoutDisposition::ResourceRejected
    };
    Ok(V37ConstructionProjection {
        source_decode_bytes,
        projected_decode_bytes,
        resident_coordinate_bytes,
        index_bytes,
        score_bytes,
        sample_bytes,
        tree_bytes,
        relation_count_bytes,
        relation_prefix_bytes,
        subtotal_bytes,
        allocator_headroom_bytes,
        total_bytes,
        disposition,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        V37ArtifactIdentity, V37AuthorityManifest, V37LayoutDisposition, V37NumericAuthority,
        V37RelationSpec, V37TreeSpec, canonical_v37_authority_bytes, parse_v37_authority_bytes,
        project_v37_child_quota, project_v37_construction_bytes, project_v37_layout,
        project_v37_serving_bytes, project_v37_work, validate_v37_specs,
    };

    fn ownership_spec(rows: u64) -> V37TreeSpec {
        V37TreeSpec {
            corpus_rows: rows,
            dimensions: 192,
            seed: 37,
            target_primary_rows: 8_192,
            training_sample_rows: 4_096,
            two_means_iterations: 8,
        }
    }

    fn relation_spec() -> V37RelationSpec {
        V37RelationSpec {
            leaf_count: 4_096,
            maximum_leaf_probes: 32,
            prefix_lengths: vec![16, 32, 64],
            q_bits: 24,
            seed: 38,
        }
    }

    fn artifact(role: &str, digit: char) -> V37ArtifactIdentity {
        V37ArtifactIdentity {
            blake3: digit.to_string().repeat(64),
            encoded_bytes: 1,
            role: role.to_owned(),
            sha256: digit.to_string().repeat(64),
            uri: format!("s3://borsuk-v37-test/{role}"),
        }
    }

    fn authority() -> V37AuthorityManifest {
        V37AuthorityManifest {
            algorithm: "balanced-hyperplane-relation-v1".to_owned(),
            metric: "squared-l2".to_owned(),
            numeric: V37NumericAuthority {
                fma_backend: "aarch64-neon-fma".to_owned(),
                lane_width: 8,
                worker_count: 16,
            },
            projection: artifact("projected-corpus", '2'),
            relation: relation_spec(),
            schema: "borsuk-v37-relation-authority-v1".to_owned(),
            source: artifact("source-corpus", '1'),
            tree: ownership_spec(1_000_000),
        }
    }

    #[test]
    fn v37_relation_authority_projects_recursive_quotas_and_checked_work() {
        let root = project_v37_child_quota(1_000_000, 123).unwrap();
        assert_eq!(root.left_leaves, 61);
        assert_eq!(root.right_leaves, 62);
        assert_eq!(root.left_rows, 495_934);
        assert_eq!(root.right_rows, 504_066);

        let left = project_v37_child_quota(root.left_rows, root.left_leaves).unwrap();
        assert_eq!(left.left_leaves, 30);
        assert_eq!(left.right_leaves, 31);
        assert_eq!(left.left_rows, 243_901);
        assert_eq!(left.right_rows, 252_033);

        let work = project_v37_work(&ownership_spec(1_000_000), &relation_spec()).unwrap();
        assert_eq!(work.maximum_direct_node_visits, 245);
        assert_eq!(work.maximum_relation_node_visits, 1_024);
        assert_eq!(work.maximum_relation_records, 2_048);
        assert_eq!(work.selected_postings, 14);

        assert!(project_v37_child_quota(u64::MAX, 4).is_err());
        assert!(project_v37_child_quota(1, 1).is_err());
    }

    #[test]
    fn v37_relation_authority_projects_exact_construction_memory_and_disposition() {
        let projection =
            project_v37_construction_bytes(&ownership_spec(1_000_000), &relation_spec()).unwrap();
        assert_eq!(projection.source_decode_bytes, 768_000_000);
        assert_eq!(projection.projected_decode_bytes, 768_000_000);
        assert_eq!(projection.resident_coordinate_bytes, 768_000_000);
        assert_eq!(projection.index_bytes, 16_000_000);
        assert_eq!(projection.score_bytes, 4_000_000);
        assert_eq!(projection.sample_bytes, 3_145_728);
        assert_eq!(projection.tree_bytes, 3_373_600);
        assert_eq!(projection.relation_count_bytes, 4_030_464);
        assert_eq!(projection.relation_prefix_bytes, 2_129_928);
        assert_eq!(projection.subtotal_bytes, 2_336_679_720);
        assert_eq!(projection.allocator_headroom_bytes, 584_169_930);
        assert_eq!(projection.total_bytes, 2_920_849_650);
        assert_eq!(projection.disposition, V37LayoutDisposition::Admissible);

        let too_large =
            project_v37_construction_bytes(&ownership_spec(100_000_000), &relation_spec()).unwrap();
        assert_eq!(
            too_large.disposition,
            V37LayoutDisposition::ResourceRejected
        );

        let mut overflow = ownership_spec(1_000_000);
        overflow.dimensions = u64::MAX;
        assert!(project_v37_construction_bytes(&overflow, &relation_spec()).is_err());
    }

    #[test]
    fn v37_relation_authority_projects_exact_one_million_quota_tree() {
        let tree = ownership_spec(1_000_000);
        let relation = relation_spec();
        validate_v37_specs(&tree, &relation).unwrap();

        let projection = project_v37_layout(&tree).unwrap();
        assert_eq!(projection.leaf_count, 123);
        assert_eq!(projection.internal_node_count, 122);
        assert_eq!(projection.minimum_leaf_rows, 8_130);
        assert_eq!(projection.maximum_leaf_rows, 8_131);
        assert_eq!(
            projection.maximum_leaf_rows - projection.minimum_leaf_rows,
            1
        );
        assert_eq!(projection.total_rows, 1_000_000);
    }

    #[test]
    fn v37_relation_authority_projects_checked_hundred_million_ram() {
        let tree = ownership_spec(100_000_000);
        let relation = relation_spec();
        validate_v37_specs(&tree, &relation).unwrap();

        let layout = project_v37_layout(&tree).unwrap();
        assert_eq!(layout.leaf_count, 12_208);
        assert_eq!(layout.internal_node_count, 12_207);
        assert_eq!(layout.minimum_leaf_rows, 8_191);
        assert_eq!(layout.maximum_leaf_rows, 8_192);

        let serving = project_v37_serving_bytes(&tree, &relation).unwrap();
        assert_eq!(serving.ownership_tree_bytes, 9_765_600);
        assert_eq!(serving.relation_tree_bytes, 3_276_000);
        assert_eq!(serving.relation_prefix_bytes, 2_129_928);
        assert_eq!(serving.total_bytes, 15_171_528);
        assert!(serving.total_bytes < 3 * 1_073_741_824);
    }

    #[test]
    fn v37_relation_authority_rejects_numeric_and_policy_drift() {
        let tree = ownership_spec(1_000_000);
        let relation = relation_spec();

        for invalid_tree in [
            V37TreeSpec {
                dimensions: 0,
                ..tree.clone()
            },
            V37TreeSpec {
                corpus_rows: 0,
                ..tree.clone()
            },
            V37TreeSpec {
                target_primary_rows: 0,
                ..tree.clone()
            },
            V37TreeSpec {
                training_sample_rows: 4_095,
                ..tree.clone()
            },
            V37TreeSpec {
                two_means_iterations: 7,
                ..tree.clone()
            },
        ] {
            assert!(validate_v37_specs(&invalid_tree, &relation).is_err());
        }

        for invalid_relation in [
            V37RelationSpec {
                seed: tree.seed,
                ..relation.clone()
            },
            V37RelationSpec {
                leaf_count: 0,
                ..relation.clone()
            },
            V37RelationSpec {
                maximum_leaf_probes: 31,
                ..relation.clone()
            },
            V37RelationSpec {
                prefix_lengths: vec![16, 64],
                ..relation.clone()
            },
            V37RelationSpec {
                q_bits: 23,
                ..relation.clone()
            },
        ] {
            assert!(validate_v37_specs(&tree, &invalid_relation).is_err());
        }
    }

    #[test]
    fn v37_relation_authority_canonical_manifest_rejects_identity_drift() {
        let manifest = authority();
        let bytes = canonical_v37_authority_bytes(&manifest).unwrap();
        assert_eq!(bytes.last(), Some(&b'\n'));
        assert!(!bytes[..bytes.len() - 1].contains(&b'\n'));
        assert_eq!(parse_v37_authority_bytes(&bytes).unwrap(), manifest);

        for invalid in [
            V37AuthorityManifest {
                schema: "borsuk-v37-relation-authority-v2".to_owned(),
                ..manifest.clone()
            },
            V37AuthorityManifest {
                algorithm: "centroid".to_owned(),
                ..manifest.clone()
            },
            V37AuthorityManifest {
                metric: "cosine".to_owned(),
                ..manifest.clone()
            },
            V37AuthorityManifest {
                numeric: V37NumericAuthority {
                    fma_backend: "scalar-control".to_owned(),
                    ..manifest.numeric.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                numeric: V37NumericAuthority {
                    worker_count: 0,
                    ..manifest.numeric.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                source: V37ArtifactIdentity {
                    encoded_bytes: 0,
                    ..manifest.source.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                projection: V37ArtifactIdentity {
                    sha256: "A".repeat(64),
                    ..manifest.projection.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                projection: V37ArtifactIdentity {
                    uri: manifest.source.uri.clone(),
                    ..manifest.projection.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                source: V37ArtifactIdentity {
                    uri: "s3://".to_owned(),
                    ..manifest.source.clone()
                },
                ..manifest.clone()
            },
            V37AuthorityManifest {
                source: V37ArtifactIdentity {
                    uri: "s3://bucket-only".to_owned(),
                    ..manifest.source.clone()
                },
                ..manifest.clone()
            },
        ] {
            assert!(canonical_v37_authority_bytes(&invalid).is_err());
        }

        let mut unknown = bytes.clone();
        unknown.splice(1..1, b"\"extra\":0,".iter().copied());
        assert!(parse_v37_authority_bytes(&unknown).is_err());

        let mut noncanonical = bytes.clone();
        noncanonical.insert(0, b' ');
        assert!(parse_v37_authority_bytes(&noncanonical).is_err());
    }
}
