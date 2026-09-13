use crate::error::{BorsukError, Result};
use crate::v37_relation_router::{
    V37BalancedTree, V37FeatureGroundTruth, select_v37_tree_postings_with_limit,
};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

const V40_MAXIMUM_FRONTIER_POSTINGS: usize = 64;
const V40_MAXIMUM_NODE_POPS: usize = 1_024;

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_s3_uri(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("s3://") else {
        return false;
    };
    let Some((bucket, key)) = rest.split_once('/') else {
        return false;
    };
    !bucket.is_empty()
        && !key.is_empty()
        && !bucket.contains(['?', '#'])
        && !key.contains(['?', '#'])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum V40LocalRunMode {
    SelectDirect,
    EvaluateDirect,
}

impl V40LocalRunMode {
    fn input_roles(self) -> &'static [&'static str] {
        match self {
            Self::SelectDirect => &["v37-authority", "ownership-tree", "development-query"],
            Self::EvaluateDirect => &[
                "v38-ceiling-authority",
                "v38-construction-result",
                "spill-relation",
                "spill-postings",
                "development-ground-truth",
                "direct-selection",
            ],
        }
    }

    fn output_roles(self) -> &'static [&'static str] {
        match self {
            Self::SelectDirect => &["direct-selection"],
            Self::EvaluateDirect => &["direct-result"],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40LocalArtifact {
    role: String,
    path: PathBuf,
    uri: String,
    sha256: String,
    blake3: String,
    encoded_bytes: u64,
}

impl V40LocalArtifact {
    pub(crate) fn try_new(
        role: String,
        path: PathBuf,
        uri: String,
        sha256: String,
        blake3: String,
        encoded_bytes: u64,
    ) -> Result<Self> {
        if role.is_empty()
            || path.as_os_str().is_empty()
            || !valid_s3_uri(&uri)
            || !valid_digest(&sha256)
            || !valid_digest(&blake3)
            || encoded_bytes == 0
        {
            return Err(BorsukError::InvalidStorage(
                "V40 local artifact identity differs".to_owned(),
            ));
        }
        Ok(Self {
            role,
            path,
            uri,
            sha256,
            blake3,
            encoded_bytes,
        })
    }

    pub(crate) fn role(&self) -> &str {
        &self.role
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40LocalOutput {
    role: String,
    path: PathBuf,
}

impl V40LocalOutput {
    pub(crate) fn try_new(role: String, path: PathBuf) -> Result<Self> {
        if role.is_empty() || path.as_os_str().is_empty() {
            return Err(BorsukError::InvalidStorage(
                "V40 local output identity differs".to_owned(),
            ));
        }
        Ok(Self { role, path })
    }

    pub(crate) fn role(&self) -> &str {
        &self.role
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40LocalRunRequest {
    mode: V40LocalRunMode,
    inputs: Vec<V40LocalArtifact>,
    outputs: Vec<V40LocalOutput>,
    workers: u32,
}

impl V40LocalRunRequest {
    pub(crate) fn try_new(
        mode: V40LocalRunMode,
        inputs: Vec<V40LocalArtifact>,
        outputs: Vec<V40LocalOutput>,
        workers: u32,
    ) -> Result<Self> {
        let input_roles = inputs
            .iter()
            .map(V40LocalArtifact::role)
            .collect::<Vec<_>>();
        let output_roles = outputs.iter().map(V40LocalOutput::role).collect::<Vec<_>>();
        let mut paths = BTreeSet::new();
        let mut uris = BTreeSet::new();
        if input_roles != mode.input_roles()
            || output_roles != mode.output_roles()
            || !matches!(workers, 1 | 2 | 4 | 8 | 16 | 32)
            || inputs.iter().any(|input| !paths.insert(input.path()))
            || outputs.iter().any(|output| !paths.insert(output.path()))
            || inputs.iter().any(|input| !uris.insert(input.uri.as_str()))
        {
            return Err(BorsukError::InvalidStorage(
                "V40 local phase capability differs".to_owned(),
            ));
        }
        Ok(Self {
            mode,
            inputs,
            outputs,
            workers,
        })
    }

    pub(crate) fn input_roles(&self) -> Vec<&str> {
        self.inputs.iter().map(V40LocalArtifact::role).collect()
    }

    pub(crate) fn output_roles(&self) -> Vec<&str> {
        self.outputs.iter().map(V40LocalOutput::role).collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40TreeFrontier {
    pub(crate) posting_ordinals: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) fma_backend: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct V40EvaluationSpec {
    pub(crate) selected_postings: u32,
    pub(crate) gt_neighbors: u32,
    pub(crate) aggregate_gate_ppm: u32,
    pub(crate) minimum_gate_ppm: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40DirectSelectionRecord {
    pub(crate) query_ordinal: u32,
    pub(crate) posting_ordinals: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) fma_backend: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40DirectSample {
    pub(crate) query_ordinal: u32,
    pub(crate) selected_postings: Vec<u32>,
    pub(crate) node_pops: u32,
    pub(crate) scored_internal_nodes: u32,
    pub(crate) hits: u32,
    pub(crate) recall_ppm: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct V40DirectEvaluation {
    pub(crate) samples: Vec<V40DirectSample>,
    pub(crate) total_hits: u64,
    pub(crate) aggregate_recall_ppm: u32,
    pub(crate) minimum_recall_ppm: u32,
    pub(crate) passed: bool,
    pub(crate) disposition: String,
}

pub(crate) fn select_v40_tree_frontier(
    tree: &V37BalancedTree,
    expected_backend: &str,
    query: &[f32],
    posting_limit: usize,
    maximum_node_pops: usize,
) -> Result<V40TreeFrontier> {
    if posting_limit == 0
        || posting_limit > V40_MAXIMUM_FRONTIER_POSTINGS
        || maximum_node_pops == 0
        || maximum_node_pops > V40_MAXIMUM_NODE_POPS
    {
        return Err(BorsukError::InvalidStorage(
            "V40 tree frontier authority differs".to_owned(),
        ));
    }
    let selection = select_v37_tree_postings_with_limit(
        tree,
        expected_backend,
        maximum_node_pops,
        posting_limit,
        query,
    )?;
    Ok(V40TreeFrontier {
        posting_ordinals: selection.selected_postings,
        node_pops: selection.node_visits,
        scored_internal_nodes: selection.scored_internal_nodes,
        fma_backend: selection.fma_backend,
    })
}

pub(crate) fn evaluate_v40_direct_recall(
    spec: &V40EvaluationSpec,
    owners: &[(u64, u32, Option<u32>)],
    selections: &[V40DirectSelectionRecord],
    truth: &[V37FeatureGroundTruth],
    expected_backend: &str,
) -> Result<V40DirectEvaluation> {
    let invalid =
        || BorsukError::InvalidStorage("V40 direct evaluation authority differs".to_owned());
    if spec.selected_postings == 0
        || spec.gt_neighbors == 0
        || spec.aggregate_gate_ppm > 1_000_000
        || spec.minimum_gate_ppm > 1_000_000
        || owners.is_empty()
        || selections.is_empty()
        || selections.len() != truth.len()
        || !matches!(expected_backend, "aarch64-neon-fma" | "x86-avx-fma")
    {
        return Err(invalid());
    }

    let mut ownership = BTreeMap::new();
    let mut known_postings = BTreeSet::new();
    for &(feature_id, primary, alternate) in owners {
        if alternate == Some(primary)
            || ownership.insert(feature_id, (primary, alternate)).is_some()
        {
            return Err(invalid());
        }
        known_postings.insert(primary);
        if let Some(posting) = alternate {
            known_postings.insert(posting);
        }
    }
    if ownership
        .keys()
        .copied()
        .ne(owners.iter().map(|owner| owner.0))
    {
        return Err(invalid());
    }

    let selected_count = usize::try_from(spec.selected_postings).map_err(|_| invalid())?;
    let truth_count = usize::try_from(spec.gt_neighbors).map_err(|_| invalid())?;
    let mut samples = Vec::with_capacity(selections.len());
    let mut total_hits = 0_u64;
    let mut minimum_recall_ppm = 1_000_000_u32;

    for (expected_query, (selection, ground_truth)) in selections.iter().zip(truth).enumerate() {
        let expected_query = u32::try_from(expected_query).map_err(|_| invalid())?;
        let selected: BTreeSet<_> = selection.posting_ordinals.iter().copied().collect();
        let truth_ids: BTreeSet<_> = ground_truth.feature_row_ids.iter().copied().collect();
        if selection.query_ordinal != expected_query
            || ground_truth.query_ordinal != expected_query
            || selection.posting_ordinals.len() != selected_count
            || selected.len() != selected_count
            || !selected.is_subset(&known_postings)
            || selection.node_pops == 0
            || selection.scored_internal_nodes == 0
            || selection.scored_internal_nodes > selection.node_pops
            || selection.fma_backend != expected_backend
            || ground_truth.feature_row_ids.len() != truth_count
            || truth_ids.len() != truth_count
        {
            return Err(invalid());
        }

        let mut hits = 0_u32;
        for feature_id in &ground_truth.feature_row_ids {
            let (primary, alternate) = ownership.get(feature_id).ok_or_else(invalid)?;
            if selected.contains(primary)
                || alternate.is_some_and(|posting| selected.contains(&posting))
            {
                hits = hits.checked_add(1).ok_or_else(invalid)?;
            }
        }
        let recall_ppm = u32::try_from(
            u64::from(hits).checked_mul(1_000_000).ok_or_else(invalid)?
                / u64::from(spec.gt_neighbors),
        )
        .map_err(|_| invalid())?;
        total_hits = total_hits
            .checked_add(u64::from(hits))
            .ok_or_else(invalid)?;
        minimum_recall_ppm = minimum_recall_ppm.min(recall_ppm);
        samples.push(V40DirectSample {
            query_ordinal: expected_query,
            selected_postings: selection.posting_ordinals.clone(),
            node_pops: selection.node_pops,
            scored_internal_nodes: selection.scored_internal_nodes,
            hits,
            recall_ppm,
        });
    }

    let possible_hits = u64::try_from(selections.len())
        .map_err(|_| invalid())?
        .checked_mul(u64::from(spec.gt_neighbors))
        .ok_or_else(invalid)?;
    let aggregate_recall_ppm =
        u32::try_from(total_hits.checked_mul(1_000_000).ok_or_else(invalid)? / possible_hits)
            .map_err(|_| invalid())?;
    let passed = aggregate_recall_ppm >= spec.aggregate_gate_ppm
        && minimum_recall_ppm >= spec.minimum_gate_ppm;
    Ok(V40DirectEvaluation {
        samples,
        total_hits,
        aggregate_recall_ppm,
        minimum_recall_ppm,
        passed,
        disposition: if passed {
            "direct-passed"
        } else {
            "direct-failed"
        }
        .to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::super::v37_relation_router::{
        V37BalancedNode, V37BalancedTree, score_v37_hyperplane_fused,
    };
    use super::{
        V40DirectSelectionRecord, V40EvaluationSpec, V40LocalArtifact, V40LocalOutput,
        V40LocalRunMode, V40LocalRunRequest, evaluate_v40_direct_recall, select_v40_tree_frontier,
    };
    use crate::v37_relation_router::V37FeatureGroundTruth;

    fn four_leaf_tree() -> V37BalancedTree {
        let normal = vec![1.0_f32, 0.0, 0.0];
        let leaf = |posting_ordinal| V37BalancedNode {
            normal: Vec::new(),
            boundary_score_bits: 0.0_f32.to_bits(),
            boundary_source_ordinal: 0,
            left_node: None,
            right_node: None,
            posting_ordinal: Some(posting_ordinal),
            population: 1,
        };
        let branch = |population, left_node, right_node| V37BalancedNode {
            normal: normal.clone(),
            boundary_score_bits: 0.0_f32.to_bits(),
            boundary_source_ordinal: 0,
            left_node: Some(left_node),
            right_node: Some(right_node),
            posting_ordinal: None,
            population,
        };
        let backend = score_v37_hyperplane_fused(&[0.0, 0.0, 0.0], &[1.0, 0.0, 0.0])
            .unwrap()
            .1
            .to_owned();
        V37BalancedTree {
            dimensions: 3,
            seed: 40,
            fma_backend: backend,
            nodes: vec![
                branch(4, 1, 4),
                branch(2, 2, 3),
                leaf(0),
                leaf(1),
                branch(2, 5, 6),
                leaf(2),
                leaf(3),
            ],
            leaf_populations: vec![1; 4],
            assignments: Vec::new(),
        }
    }

    #[test]
    fn v40_tree_frontier_matches_exhaustive_order_and_fails_closed() {
        let tree = four_leaf_tree();
        let frontier =
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[1.0, 0.0, 0.0], 4, 7).unwrap();
        assert_eq!(frontier.posting_ordinals, vec![3, 0, 1, 2]);
        assert_eq!(frontier.node_pops, 7);
        assert_eq!(frontier.scored_internal_nodes, 3);
        assert_eq!(frontier.fma_backend, tree.fma_backend);

        assert!(
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[1.0, 0.0, 0.0], 5, 7).is_err()
        );
        assert!(
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[1.0, 0.0, 0.0], 4, 6).is_err()
        );
    }

    #[test]
    fn v40_tree_frontier_handles_equal_margins_signed_zero_and_bad_inputs() {
        let tree = four_leaf_tree();
        let frontier =
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[-0.0, 0.0, 0.0], 4, 7).unwrap();
        assert_eq!(frontier.posting_ordinals, vec![0, 1, 2, 3]);

        assert!(select_v40_tree_frontier(&tree, "scalar-control", &[0.0; 3], 4, 7).is_err());
        assert!(
            select_v40_tree_frontier(&tree, &tree.fma_backend, &[f32::NAN, 0.0, 0.0], 4, 7)
                .is_err()
        );
        assert!(select_v40_tree_frontier(&tree, &tree.fma_backend, &[0.0; 3], 0, 7).is_err());
        assert!(select_v40_tree_frontier(&tree, &tree.fma_backend, &[0.0; 3], 4, 0).is_err());
    }

    fn direct_evaluation_fixture() -> (
        V40EvaluationSpec,
        Vec<(u64, u32, Option<u32>)>,
        Vec<V40DirectSelectionRecord>,
        Vec<V37FeatureGroundTruth>,
    ) {
        let spec = V40EvaluationSpec {
            selected_postings: 2,
            gt_neighbors: 4,
            aggregate_gate_ppm: 750_000,
            minimum_gate_ppm: 750_000,
        };
        let owners = vec![
            (10, 0, Some(1)),
            (11, 2, Some(1)),
            (12, 3, Some(0)),
            (13, 4, None),
            (20, 2, Some(0)),
            (21, 3, None),
            (22, 4, Some(5)),
            (23, 0, None),
        ];
        let selections = vec![
            V40DirectSelectionRecord {
                query_ordinal: 0,
                posting_ordinals: vec![0, 1],
                node_pops: 5,
                scored_internal_nodes: 3,
                fma_backend: "aarch64-neon-fma".to_owned(),
            },
            V40DirectSelectionRecord {
                query_ordinal: 1,
                posting_ordinals: vec![0, 3],
                node_pops: 6,
                scored_internal_nodes: 4,
                fma_backend: "aarch64-neon-fma".to_owned(),
            },
        ];
        let truth = vec![
            V37FeatureGroundTruth {
                query_ordinal: 0,
                feature_row_ids: vec![10, 11, 12, 13],
            },
            V37FeatureGroundTruth {
                query_ordinal: 1,
                feature_row_ids: vec![20, 21, 22, 23],
            },
        ];
        (spec, owners, selections, truth)
    }

    #[test]
    fn v40_direct_evaluation_counts_two_owner_hits_once_and_enforces_gates() {
        let (spec, owners, selections, truth) = direct_evaluation_fixture();
        let result =
            evaluate_v40_direct_recall(&spec, &owners, &selections, &truth, "aarch64-neon-fma")
                .unwrap();
        assert_eq!(result.total_hits, 6);
        assert_eq!(result.aggregate_recall_ppm, 750_000);
        assert_eq!(result.minimum_recall_ppm, 750_000);
        assert_eq!(result.samples[0].hits, 3);
        assert_eq!(result.samples[0].recall_ppm, 750_000);
        assert_eq!(result.samples[1].hits, 3);
        assert!(result.passed);
        assert_eq!(result.disposition, "direct-passed");
    }

    #[test]
    fn v40_direct_evaluation_rejects_selection_truth_and_backend_drift() {
        let (spec, owners, selections, truth) = direct_evaluation_fixture();

        let mut duplicated = selections.clone();
        duplicated[0].posting_ordinals = vec![0, 0];
        assert!(
            evaluate_v40_direct_recall(&spec, &owners, &duplicated, &truth, "aarch64-neon-fma")
                .is_err()
        );

        let mut skipped_query = selections.clone();
        skipped_query[1].query_ordinal = 2;
        assert!(
            evaluate_v40_direct_recall(&spec, &owners, &skipped_query, &truth, "aarch64-neon-fma")
                .is_err()
        );

        let mut unknown_truth = truth.clone();
        unknown_truth[0].feature_row_ids[0] = 99;
        assert!(
            evaluate_v40_direct_recall(
                &spec,
                &owners,
                &selections,
                &unknown_truth,
                "aarch64-neon-fma"
            )
            .is_err()
        );

        assert!(
            evaluate_v40_direct_recall(&spec, &owners, &selections, &truth, "x86-avx-fma").is_err()
        );
    }

    fn local_artifact(role: &str) -> V40LocalArtifact {
        V40LocalArtifact::try_new(
            role.to_owned(),
            PathBuf::from(format!("/tmp/v40-{role}")),
            format!("s3://fixture/v40/{role}"),
            "1".repeat(64),
            "2".repeat(64),
            17,
        )
        .unwrap()
    }

    fn local_output(role: &str) -> V40LocalOutput {
        V40LocalOutput::try_new(role.to_owned(), PathBuf::from(format!("/tmp/v40-{role}"))).unwrap()
    }

    #[test]
    fn v40_authority_direct_modes_separate_query_and_truth_capabilities() {
        let selection_inputs = ["v37-authority", "ownership-tree", "development-query"]
            .map(local_artifact)
            .to_vec();
        let selection = V40LocalRunRequest::try_new(
            V40LocalRunMode::SelectDirect,
            selection_inputs,
            vec![local_output("direct-selection")],
            16,
        )
        .unwrap();
        assert_eq!(
            selection.input_roles(),
            vec!["v37-authority", "ownership-tree", "development-query"]
        );
        assert_eq!(selection.output_roles(), vec!["direct-selection"]);

        let evaluation_inputs = [
            "v38-ceiling-authority",
            "v38-construction-result",
            "spill-relation",
            "spill-postings",
            "development-ground-truth",
            "direct-selection",
        ]
        .map(local_artifact)
        .to_vec();
        let evaluation = V40LocalRunRequest::try_new(
            V40LocalRunMode::EvaluateDirect,
            evaluation_inputs,
            vec![local_output("direct-result")],
            1,
        )
        .unwrap();
        assert_eq!(evaluation.output_roles(), vec!["direct-result"]);
        assert!(!evaluation.input_roles().contains(&"development-query"));
        assert!(
            !selection
                .input_roles()
                .contains(&"development-ground-truth")
        );
    }

    #[test]
    fn v40_authority_direct_modes_reject_identity_role_and_path_drift() {
        assert!(
            V40LocalArtifact::try_new(
                "ownership-tree".to_owned(),
                PathBuf::from("/tmp/tree"),
                "file:///tmp/tree".to_owned(),
                "1".repeat(64),
                "2".repeat(64),
                17,
            )
            .is_err()
        );
        assert!(
            V40LocalArtifact::try_new(
                "ownership-tree".to_owned(),
                PathBuf::from("/tmp/tree"),
                "s3://fixture/tree".to_owned(),
                "1".repeat(63),
                "2".repeat(64),
                17,
            )
            .is_err()
        );

        let mut inputs = ["v37-authority", "ownership-tree", "development-query"]
            .map(local_artifact)
            .to_vec();
        inputs.swap(0, 1);
        assert!(
            V40LocalRunRequest::try_new(
                V40LocalRunMode::SelectDirect,
                inputs,
                vec![local_output("direct-selection")],
                16,
            )
            .is_err()
        );

        let mut overlap = ["v37-authority", "ownership-tree", "development-query"]
            .map(local_artifact)
            .to_vec();
        overlap[1] = overlap[0].clone();
        assert!(
            V40LocalRunRequest::try_new(
                V40LocalRunMode::SelectDirect,
                overlap,
                vec![local_output("direct-selection")],
                16,
            )
            .is_err()
        );

        let inputs = ["v37-authority", "ownership-tree", "development-query"]
            .map(local_artifact)
            .to_vec();
        assert!(
            V40LocalRunRequest::try_new(
                V40LocalRunMode::SelectDirect,
                inputs,
                vec![
                    V40LocalOutput::try_new(
                        "direct-selection".to_owned(),
                        PathBuf::from("/tmp/v40-ownership-tree"),
                    )
                    .unwrap()
                ],
                3,
            )
            .is_err()
        );
    }
}
