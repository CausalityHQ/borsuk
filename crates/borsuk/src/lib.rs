//! BORSUK core library.
#![forbid(unsafe_code)]
//!
//! BORSUK stands for Blob-Oriented Retrieval with Segmental Unified KNN. The
//! core crate stores vectors in immutable external segments and keeps only
//! manifest-level segment summaries in memory while searching.

mod arrow_vector_sidecar;
mod bm25;
pub(crate) mod build_timing;
mod cell_wal;
mod centroid_hnsw;
mod collection_control;
mod error;
mod float8;
mod format;
#[allow(
    dead_code,
    reason = "incremental cell-card construction helpers remain unwired until atomic publication"
)]
mod global_cell_card;
mod global_leaf;
mod global_leaf_run;
mod global_pq_sidecar;
mod group_commit;
mod index;
mod lane_log;
mod late_interaction;
mod late_interaction_sidecar;
mod lexical_build;
mod lexical_root;
mod lexical_simd;
mod logical_cell_catalog;
mod maintenance;
mod manifest;
mod metadata;
mod metric;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "mutation clock foundation is wired into persistence in the next planned slice"
    )
)]
mod mutation;
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "standard mutation extent is connected to the lane writer in the next planned slice"
    )
)]
mod mutation_extent;
mod observability;
mod parallel;
mod physical_layout;
mod positioned_candidate;
mod positioned_log;
mod positioned_materializer;
mod quantizer_sidecar;
mod record;
mod rotated_product_quantizer;
#[allow(
    dead_code,
    reason = "pure row-bundle construction is wired only at the Task 4 atomic format switch"
)]
mod row_bundle;
mod scalar_decode;
mod segment;
mod segment_cache;
mod simd_control;
pub mod sparse;
pub mod sparse_index;
mod storage;
mod storage_trace;
/// Text tokenization helpers for per-record term-frequency storage.
pub mod text;
mod turboquant;
#[allow(
    dead_code,
    reason = "V21 feasibility components are wired incrementally by the committed plan"
)]
mod v21_feasibility;
#[allow(
    dead_code,
    reason = "V22 feasibility components are wired incrementally by the committed plan"
)]
mod v22_feasibility;
#[allow(
    dead_code,
    reason = "V23 balanced-page components are wired incrementally by the committed plan"
)]
mod v23_balanced_pages;
#[allow(
    dead_code,
    reason = "V23 balanced-page Arrow contracts are wired incrementally by the committed plan"
)]
mod v23_balanced_pages_arrow;
#[allow(
    dead_code,
    reason = "V23 balanced-page construction is wired incrementally by the committed plan"
)]
mod v23_balanced_pages_build;
#[allow(
    dead_code,
    reason = "V23 balanced-page evaluation is wired incrementally by the committed plan"
)]
mod v23_balanced_pages_eval;
#[allow(
    dead_code,
    reason = "V23 balanced-page geometry is wired incrementally by the committed plan"
)]
mod v23_balanced_pages_train;
mod v23_diagnostic;
#[allow(
    dead_code,
    reason = "V23 incidence components are wired incrementally by the committed plan"
)]
mod v23_incidence;
#[allow(
    dead_code,
    reason = "V23 incidence evaluation is wired incrementally by the committed plan"
)]
mod v23_incidence_eval;
#[allow(
    dead_code,
    reason = "V23 incidence postings are wired incrementally by the committed plan"
)]
mod v23_incidence_postings;
#[allow(
    dead_code,
    reason = "V23 incidence tree is wired incrementally by the committed plan"
)]
mod v23_incidence_tree;
#[allow(
    dead_code,
    reason = "V23 residual RaBitQ components are wired incrementally by the committed plan"
)]
mod v23_rabitq;
#[allow(
    dead_code,
    reason = "V23 residual RaBitQ Arrow artifacts are wired incrementally by the committed plan"
)]
mod v23_rabitq_arrow;
#[allow(
    dead_code,
    reason = "V23 residual RaBitQ construction is wired incrementally by the committed plan"
)]
mod v23_rabitq_build;
#[allow(
    dead_code,
    reason = "V23 residual RaBitQ evaluation is wired incrementally by the committed plan"
)]
mod v23_rabitq_eval;
mod v23_rabitq_local;
#[allow(
    dead_code,
    reason = "V23 residual RaBitQ quantization is wired incrementally by the committed plan"
)]
mod v23_rabitq_quantizer;
#[allow(
    dead_code,
    reason = "V24 witness components are wired incrementally by the committed plan"
)]
mod v24_witness;
#[allow(
    dead_code,
    reason = "V24 witness evaluation is wired incrementally by the committed plan"
)]
mod v24_witness_eval;
#[allow(
    dead_code,
    reason = "V24 witness graph is wired incrementally by the committed plan"
)]
mod v24_witness_graph;
mod v24_witness_local;
#[allow(
    dead_code,
    reason = "V24 witness postings are wired incrementally by the committed plan"
)]
mod v24_witness_postings;
mod v24_witness_prepare;
mod v27_s3_page;
pub use v27_s3_page::{V27Page, V27PageIdentity, V27PageRow, decode_v27_page, encode_v27_page};
mod v27_s3_router;
pub use v27_s3_router::{
    V27_LEAF_CENTROIDS, V27_LEAVES_PER_ROOT, V27_ROOT_CENTROIDS, V27Hierarchy,
    V27HierarchyArtifactIdentity, V27HierarchyArtifacts, V27HierarchyConfig, decode_v27_hierarchy,
    encode_v27_hierarchy, fit_v27_hierarchy,
};
mod v27_s3_build;
pub use v27_s3_build::{
    V27BuildConfig, V27BuildReceipt, V27LayoutArtifactIdentity, V27LayoutArtifacts, V27PageBuilder,
    V27PageManifest, V27PagePosting, V27PageSink, decode_v27_layout, decode_v27_page_manifest,
    encode_v27_layout, encode_v27_page_manifest,
};
mod v27_s3_search;
mod v30_s3_layout;
mod v30_s3_pq;
mod v30_s3_search;
#[cfg(test)]
mod v32_code_objects;
mod v32_global_pages;
mod v33_group_shape;
mod v34_rank4;
mod v34_route;
mod v35_authority;
mod v35_build;
mod v35_delta;
mod v35_patch;
mod v35_projection;
mod v35_remote;
mod v35_route;
mod v36_funnel;
mod v36_funnel_geometry;
mod v36_prefix_dataset;
pub use v27_s3_search::{V27Match, V27PageStore, V27SearchIndex, V27SearchResult, V27SearchWork};
pub use v27_s3_search::{V27PageSelection, V27Router, V27RoutingWork, V27SearchArm};
#[doc(hidden)]
pub use v30_s3_layout::{
    V30ConstructedIndex, V30ConstructionArtifacts, V30ConstructionBuilder, V30ConstructionConfig,
    V30LayoutArtifactIdentity, V30LayoutArtifacts, V30PageSink, V30Scratch, V32PageLocation,
    V32PageLocationsArtifact, V32PagePrefixes, V32ServingTier, decode_v32_page_locations,
    encode_v32_page_locations,
};
#[doc(hidden)]
pub use v30_s3_pq::{V30PqArtifactIdentity, V30PqArtifacts, V32PqTableWork};
#[doc(hidden)]
pub use v30_s3_search::{
    V32CandidateReplay, V32CpuPreflightMode, V32CpuPreflightSample, V32CpuPreflightSamples,
    V32CpuPreflightShape, V32CpuPreflightWork, V32Index, V32Match, V32PageSelection, V32PageStore,
    V32PqEvaluationWork, V32Router, V32RoutingDiagnostic, V32RoutingStopReason,
    V32RoutingTargetReport, V32RoutingTargetStage, V32RoutingWork, V32SearchArm, V32SearchPhase,
    V32SearchResult, V32SearchWork, V32VirtualPageLayout, V32VirtualRoutingDiagnostic,
    canonical_v32_cpu_preflight_receipt, run_v32_cpu_preflight, v32_cpu_preflight_shape,
};
#[doc(hidden)]
pub use v33_group_shape::{
    V33FullCovarianceCeiling, V33FullCovarianceCeilingRequest, V33GroupShapeBuildRequest,
    V33LeafShapeArtifact, V33LowRankCovarianceLadder, V33LowRankCovarianceLadderRequest,
    V33ReconstructedGroupOracle, V33ReconstructedOracleRequest, build_v33_full_covariance_ceiling,
    build_v33_group_shape_artifact, build_v33_low_rank_covariance_ladder,
    build_v33_reconstructed_group_oracle, canonical_v33_full_covariance_ceiling_result_bytes,
    canonical_v33_low_rank_covariance_ladder_result_bytes,
    canonical_v33_reconstructed_oracle_result_bytes, rank_v33_full_covariance_groups,
    rank_v33_low_rank_covariance_groups, rank_v33_reconstructed_groups,
    v33_reconstructed_group_for_logical,
};
#[doc(hidden)]
pub use v34_rank4::{
    V34Rank4ArtifactIdentity, V34Rank4Generation, V34Rank4Leaf, V34Rank4LeafInput,
    V34ServingMemoryProjection, build_v34_rank4_generation, build_v34_rank4_generation_from_v33,
    decode_v34_rank4_arrow, encode_v34_rank4_arrow, project_v34_serving_memory,
    score_v34_rank4_leaf,
};
#[doc(hidden)]
pub use v34_route::{
    V34GroupStorage, V34RouteBudget, V34RoutePrefix, V34RouteTree, V34SelectedGroup, V34TreeNode,
    bound_v34_node, build_v34_route_tree, exhaustive_v34_route, hierarchical_v34_route,
};
#[doc(hidden)]
pub use v35_authority::{
    V35ArtifactIdentity, V35Dimensions, V35GenerationManifest, V35ProjectionArm, V35RemoteCodeRate,
    V35ServingProjection, V35StoredArtifactIdentity, project_v35_serving_memory,
    validate_v35_manifest,
};
#[doc(hidden)]
pub use v35_build::{
    V35BuildAuthority, V35BuildBlock, V35BuildBlockSource, V35BuildEncodedGroupReceipt,
    V35BuildEncodedObjectSink, V35BuildLeafSink, V35BuildMergeReceipt, V35BuildMergeRow,
    V35BuildMergeSource, V35BuildObjectTarget, V35BuildRow, V35BuildRun, V35BuildRunBatch,
    V35BuildRunCursor, V35BuildRunRow, V35BuildScratchReceipt, V35BuildScratchSink,
    V35BuildStorageGroup, V35BuildStorageGroupAssembler, V35BuildStorageGroupReceipt,
    V35BuildStorageGroupSink, V35MortonModel, build_v35_scratch_runs, decode_v35_build_run_arrow,
    decode_v35_source_block_parquet, encode_v35_build_storage_group, merge_v35_build_runs,
    open_v35_build_run_cursor, train_v35_morton_model,
};
#[doc(hidden)]
pub use v35_delta::{
    V35ConditionalHeadWrite, V35DeltaManifest, V35DeltaRun, V35GenerationPublicationSink,
    V35PublicationOutcome, decode_v35_head, publish_v35_delta, publish_v35_generation,
    seal_v35_delta, validate_v35_delta_base, validate_v35_delta_manifest,
    validate_v35_delta_visibility,
};
#[doc(hidden)]
pub use v35_patch::{
    V35EqualByteCentroidControl, V35LeafPatch, V35LeafPatchArm, V35LeafPatchBuildRequest,
    V35RoutingGeneration, V35RoutingGenerationLimits, build_v35_equal_byte_centroid_control,
    build_v35_leaf_patch, build_v35_leaf_patch_arm, build_v35_leaf_patch_from_merge_rows,
    build_v35_routing_generation, decode_v35_generation_arrow, encode_v35_generation_arrow,
    project_v35_leaf_moment_bytes, project_v35_leaf_seal_workspace_bytes,
    score_v35_equal_byte_centroid_control, score_v35_leaf_patch, score_v35_leaf_patch_arm,
};
#[doc(hidden)]
pub use v35_projection::{
    V35ProjectedQuery, V35Projection, V35ProjectionBackend, V35ProjectionBlockVisitor,
    V35ProjectionLimits, V35ProjectionSampleSource, V35ProjectionTrainingSpec, build_v35_srht,
    decode_v35_projection_arrow, encode_v35_projection_arrow, project_v35_query_scalar,
    project_v35_query_simd, train_v35_pca,
};
#[doc(hidden)]
pub use v35_remote::{
    V35CandidateAccumulator, V35CandidateSet, V35CodeDirectoryBlockReference, V35CodeDirectoryRoot,
    V35EncodedExactPage, V35ExactPageIdentity, V35ExactPageResponse, V35ExactPageRow,
    V35ExactPageSelection, V35ExactPageTransport, V35Match, V35PageDirectoryBlock,
    V35PageDirectoryBlockReference, V35PageDirectoryRoot, V35RemoteChunk, V35RemoteCodeRow,
    V35RemoteDirectoryBlock, V35RemoteDispatch, V35RemoteExecutionFailure, V35RemoteFailureKind,
    V35RemotePlan, V35RemoteRange, V35RemoteRangeResponse, V35RemoteReadReceipt,
    V35ResidualSqDescriptor, V35ResidualSqScorer, V35ScannedCandidate, V35SearchResult,
    V35SnapshotEntry, V35SnapshotVisibility, V35TransportFailure, V35VersionedRangeTransport,
    build_v35_residual_sq_descriptor, decode_v35_code_directory_root_arrow,
    decode_v35_page_directory_arrow, decode_v35_page_directory_root_arrow,
    decode_v35_remote_directory_arrow, decode_v35_snapshot_visibility_arrow,
    encode_v35_code_directory_root_arrow, encode_v35_exact_page_parquet,
    encode_v35_page_directory_arrow, encode_v35_page_directory_root_arrow,
    encode_v35_remote_code_arrow, encode_v35_remote_directory_arrow, execute_v35_remote_plan,
    plan_v35_remote_reads, reduce_v35_scanned_candidates, rerank_v35_exact_pages,
    resolve_v35_exact_pages, scan_v35_code_ranges, select_v35_exact_pages,
    v35_remote_code_schema_digest,
};
#[doc(hidden)]
pub use v35_route::{
    V35GroupStorage, V35RemoteDirectoryBinding, V35RouteBudget, V35RoutePrefix, V35RouteTree,
    V35SelectedGroup, V35TreeNode, bound_v35_node, build_v35_route_tree, exhaustive_v35_route,
    hierarchical_v35_route, project_v35_route_tree_bytes,
};
pub use v36_funnel::{
    V36ArtifactIdentity, V36ChunkCeiling, V36CoarseCode, V36FineCodec, V36FunnelManifest,
    V36GeometryArm, V36PrefixCheckpointContext, V36PrefixCheckpointManifest,
    V36PrefixCheckpointPhase, V36PrefixCheckpointPointer, V36PrefixCheckpointPointerCondition,
    V36PrefixCheckpointPublication, V36PrefixFreezeAuthority, V36PrefixFreezeExecutionAuthority,
    V36PrefixFreezeReceipt, V36PrefixGroundTruthArtifacts, V36PrefixMaterializedArtifacts,
    V36PrefixPopulationAuthority, V36PrefixPopulationCheckpoint, V36PrefixPopulationSelection,
    V36PrefixRegisteredSourceObject, V36PrefixResumeBinding, V36PrefixRoleAuthority,
    V36PrefixScreenManifest, V36PrefixSourceObject, V36PrimaryRows, V36ProjectionArm,
    V36RegisteredManifest, V36Replication, V36ResourceLedger, V36ResourceRequest, V36ShapeScore,
    V36TransportDisposition, V36TransportFragment, V36TransportLimits, V36TransportPlan,
    V36TransportPosting, bind_v36_prefix_population_authority,
    canonical_v36_prefix_checkpoint_manifest_bytes, canonical_v36_prefix_checkpoint_pointer_bytes,
    canonical_v36_prefix_freeze_authority_bytes,
    canonical_v36_prefix_freeze_execution_authority_bytes,
    canonical_v36_prefix_freeze_receipt_bytes, canonical_v36_prefix_population_authority_bytes,
    canonical_v36_prefix_screen_manifest_bytes, canonical_v36_prefix_source_registry_bytes,
    plan_v36_prefix_checkpoint_dependency_closure, plan_v36_prefix_checkpoint_publication,
    plan_v36_transport, project_v36_resources, validate_v36_manifest,
    validate_v36_prefix_checkpoint_manifest, validate_v36_prefix_checkpoint_manifest_with_context,
    validate_v36_prefix_checkpoint_pointer_observation, validate_v36_prefix_checkpoint_transition,
    validate_v36_prefix_freeze_authority, validate_v36_prefix_freeze_execution_authority,
    validate_v36_prefix_freeze_receipt, validate_v36_prefix_population_authority,
    validate_v36_prefix_registered_screen_authority, validate_v36_prefix_screen_manifest,
};
#[doc(hidden)]
pub use v36_funnel_geometry::{
    V36GeometryAdmission, V36GeometryStop, admit_v36_geometry, allocate_v36_hamilton_postings,
    select_v36_closure_owners,
};
#[doc(hidden)]
pub use v36_prefix_dataset::{
    V36PrefixAllQueryGtAccumulator, V36PrefixCheckpointAction, V36PrefixCheckpointCompletion,
    V36PrefixCheckpointCompletionRequest, V36PrefixCheckpointDependencyFile,
    V36PrefixCheckpointGroundTruth, V36PrefixCheckpointGroundTruthRequest, V36PrefixCheckpointHead,
    V36PrefixCheckpointMaterialization, V36PrefixCheckpointMaterializationRequest,
    V36PrefixCheckpointOutbox, V36PrefixCheckpointResumeState, V36PrefixCheckpointSelection,
    V36PrefixCheckpointSelectionRequest, V36PrefixExternalIdentityRunReceipt,
    V36PrefixExternalIdentityRunRequest, V36PrefixExternalMaterializationRequest,
    V36PrefixExternalSelectionAuthority, V36PrefixExternalSelectionLimits,
    V36PrefixExternalSelectionRequest, V36PrefixFileBackedPopulationScan,
    V36PrefixFileBackedResumeRequest, V36PrefixFileBackedScanRequest, V36PrefixFreezePreflight,
    V36PrefixFreezeRequest, V36PrefixGtAccumulator, V36PrefixGtHeapCheckpoint,
    V36PrefixGtHeapEntry, V36PrefixGtNeighbor, V36PrefixGtParquetJob, V36PrefixGtRunStats,
    V36PrefixIdentityRun, V36PrefixIdentityRunFile, V36PrefixInputRow,
    V36PrefixMaterializedCheckpointFiles, V36PrefixMaterializedRow, V36PrefixObjectPrefixScan,
    V36PrefixPhaseResumeRequest, V36PrefixPopulationCheckpointWriter, V36PrefixPopulationCommit,
    V36PrefixPopulationFileCommit, V36PrefixQualityRole, V36PrefixQueryRow,
    V36PrefixRankedSourceObject, V36PrefixRestoredPopulation, V36PrefixRoleAssignmentContract,
    V36PrefixRoleAssignmentFile, V36PrefixRoleAssignmentReceipt, V36PrefixRoleAssignmentRequest,
    V36PrefixRoleParquetPaths, V36PrefixRoleSplit, V36PrefixRowIdentity,
    V36PrefixSelectedFileReceipt, V36PrefixSelectedIds, V36PrefixSelectedIdsContract,
    V36PrefixSelectedIdsFile, assign_v36_prefix_roles_from_selected_file,
    bind_v36_prefix_external_selection_authority, bind_v36_prefix_role_assignment_contract,
    bind_v36_prefix_selected_ids_contract, complete_v36_prefix_checkpoint_ground_truth,
    decode_v36_prefix_gt_heap_checkpoint, decode_v36_prefix_identity_run,
    decode_v36_prefix_selected_ids, deduplicate_v36_prefix_row_identities,
    encode_v36_prefix_gt_heap_checkpoint, encode_v36_prefix_identity_run,
    encode_v36_prefix_selected_ids, exact_v36_prefix_gt100,
    externally_build_v36_prefix_identity_run, externally_select_v36_prefix_population_rows,
    load_v36_prefix_checkpoint_head, load_v36_prefix_freeze_preflight,
    materialize_v36_prefix_assigned_roles, materialize_v36_prefix_checkpoint_selection,
    materialize_v36_prefix_role_parquets, next_v36_prefix_checkpoint_action,
    rank_v36_prefix_source_objects, restore_v36_prefix_checkpoint_phase,
    restore_v36_prefix_file_backed_population_scan, restore_v36_prefix_population,
    restore_v36_prefix_population_state, run_v36_prefix_checkpoint_ground_truth,
    run_v36_prefix_checkpoint_gt100, run_v36_prefix_freeze, run_v36_prefix_gt100_checkpointed,
    scan_v36_prefix_gt100_parquet, scan_v36_prefix_object_prefix,
    scan_v36_prefix_object_prefix_checkpointed, scan_v36_prefix_object_prefix_file_backed,
    scan_v36_prefix_object_prefix_resumed, scan_v36_prefix_query_parquet,
    scan_v36_prefix_registered_input_parquet, scan_v36_prefix_source_parquet,
    select_v36_prefix_checkpoint_population, select_v36_prefix_population_rows,
    select_v36_prefix_roles, v36_prefix_gt100_schema, v36_prefix_query_schema,
    v36_prefix_query_score_sha256, v36_prefix_source_schema, v36_prefix_source_score_sha256,
    validate_v36_prefix_cutoff_membership, validate_v36_prefix_gt_preflight_projection,
    validate_v36_prefix_input_row, validate_v36_prefix_role_authority,
    write_v36_prefix_gt100_parquet, write_v36_prefix_gt100_roles_from_parquets,
    write_v36_prefix_query_parquet, write_v36_prefix_source_parquet,
};
#[allow(
    dead_code,
    reason = "V24 pseudoquery qualification is wired incrementally by the committed plan"
)]
mod v24_witness_pseudoquery;

/// Print and reset the env-gated (`BORSUK_BUILD_TIMING=1`) per-phase build timing
/// breakdown accumulated since the last call. When
/// `BORSUK_BUILD_TIMING_OUTPUT` is set, the same fixed-schema rows are appended
/// to that CSV. A no-op when timing is disabled.
pub fn report_build_timing(label: &str) -> std::io::Result<()> {
    build_timing::report_and_reset(label)
}

/// Public API policy checks.
///
/// Low-level mutation-log storage types are implementation details, not an
/// alternate production durability API.
///
/// ```compile_fail
/// use borsuk::{CellWalRunInput, CellWalStore};
/// ```
pub mod public_api_policy {}

pub use borsuk_pq4::{
    BorsukError as Pq4Error, Pq4BuildConfig, Pq4BuildReport, Pq4Builder, Pq4Index, Pq4Match,
    Pq4OpenOptions, Pq4ShardedIndex, Pq4ShardedOpenOptions, merge_pq4_shard_matches,
};
pub use cell_wal::{
    CellWalConfig, CellWalRunKind, DEFAULT_CELL_WAL_LANES, LogicalCellId, MAX_CELL_WAL_LANES,
};
pub use error::{BorsukError, Result};
pub use format::{vector_records_from_parquet, vector_records_to_parquet};
pub use group_commit::{
    GroupCommitConfig, GroupCommitReceipt, GroupCommitTicket, GroupCommitWriter,
};
pub use index::{
    AdmissionStats, BorsukIndex, ByteAdmissionStats, CanonicalMutationBatch,
    DEFAULT_CELL_CARD_CODE_PLANE_CACHE_BYTES, DEFAULT_EXACT_READ_MAX_PHYSICAL_AMPLIFICATION,
    DEFAULT_LATE_INTERACTION_BATCH_CACHE_BYTES, DEFAULT_LEAF_READ_WIDTH,
    DEFAULT_MAX_ACTIVE_SEARCHES, DEFAULT_MAX_INFLIGHT_LEAF_READS,
    DEFAULT_MAX_PARALLEL_DECODE_RANK_TASKS, DEFAULT_MAX_WAITING_SEARCHES, DEFAULT_RAM_BUDGET_BYTES,
    DEFAULT_ROUTING_PAGE_CACHE_BYTES, DEFAULT_TARGET_SEGMENT_VECTOR_BYTES,
    DEFAULT_WAL_TAIL_CACHE_BYTES, DEFAULT_WAL_TAIL_DECODE_BYTES, FlowControlStats, IndexConfig,
    MAX_RECOMMENDED_SEGMENT_MAX_VECTORS, MIN_RECOMMENDED_SEGMENT_MAX_VECTORS, OpenOptions,
    WarmReport, parse_byte_size, parse_ram_budget, recommended_segment_max_vectors,
};
pub use lane_log::GROUP_COMMIT_STRIPE_COUNT;
#[doc(hidden)]
pub use logical_cell_catalog::train_logical_cell_centroids;
#[doc(hidden)]
pub use manifest::{
    MAX_GLOBAL_DELTA_ROWS, MAX_GLOBAL_DELTA_SEGMENTS, MAX_GLOBAL_DELTA_VECTOR_BYTES,
};
#[doc(hidden)]
pub use v21_feasibility::{
    V21FeasibilityArm, V21FeasibilityQuerySample, V21FeasibilityReport, V21LimitingBound,
};
#[doc(hidden)]
pub use v22_feasibility::{
    V22LayoutKind, V22LayoutLimitingBound, V22StageLExactRow, V22StageLLayoutArmReport,
    V22StageLLayoutQuerySample, V22StageLProjectedObject, V22StageLQueryPrefix, V22StageLRange,
    V22StageLReport,
};
#[doc(hidden)]
pub use v23_balanced_pages::{
    V23BalancedLocalMode, V23BalancedLocalRequest, run_v23_balanced_local_request,
};
#[doc(hidden)]
pub use v23_diagnostic::{
    V23D1Arm, V23D1ArmKey, V23D1QuerySample, V23D1Report, V23D2Arm, V23D2DiagnosticRequest,
    V23D2QuerySample, V23D2Report, V23D3Executor, V23D3WaveResult, V23GlobalAdcEvidenceIdentity,
    V23GlobalAdcLocalArtifactPaths, V23GlobalAdcLocalRunRequest, V23GlobalAdcObjectIdentity,
    V23PagePublisher, V23PageRef, V23QuantizerFamily, V23RankedResult, V23SelectorRef,
    V23WaveSample, run_v23_global_adc_local_request,
};
#[doc(hidden)]
pub use v23_incidence::{
    V23IncidenceLocalDirectoryPhaseRequest, V23IncidenceLocalPhaseRequest,
    V23IncidenceLocalRolePath, V23IncidenceObjectIdentity, V23IncidencePhase, V23IncidenceRunMode,
    V23IncidenceScreenLocalPaths, V23IncidenceScreenLocalRunRequest,
    run_v23_incidence_development_screen_local, run_v23_incidence_local_directory_phase,
    run_v23_incidence_local_phase,
};
#[doc(hidden)]
pub use v23_incidence_eval::{V23IncidenceScreenAuthority, V23IncidenceScreenObjectIdentity};
#[doc(hidden)]
pub use v23_rabitq_local::{
    V23RaBitQConstructionLocalPaths, V23RaBitQConstructionLocalRunRequest,
    V23RaBitQLocalArtifactPaths, V23RaBitQLocalObjectIdentity, V23RaBitQLocalRunRequest,
    run_v23_rabitq_construction_local_request, run_v23_rabitq_local_request,
};

/// Maximum CPU worker threads selected by the automatic small-runtime policy.
pub const DEFAULT_BUILD_THREADS: usize = 4;
/// Independent positioned-log shards available to one joined offline bulk-load
/// prefix before it must be materialized.
pub const BULK_LOAD_SOURCE_SHARDS: usize = positioned_log::SOURCE_SHARD_COUNT as usize;
/// Process-wide build/query CPU worker override. Values must be in `1..=64`;
/// missing or invalid values use the automatic policy: one fewer than the
/// available CPUs, clamped to `1..=DEFAULT_BUILD_THREADS`.
pub const CPU_THREADS_ENV: &str = "BORSUK_CPU_THREADS";
/// Default shared blocking-I/O waiter count. These are parked network waiters,
/// separate from CPU workers, and cover one qualified page-32 object-store
/// wave without adding a hidden second round trip.
pub const DEFAULT_IO_THREADS: usize = 88;
/// Process-wide blocking-I/O waiter override. Values must be in `1..=256` and
/// at least the configured physical GET concurrency.
pub const IO_THREADS_ENV: &str = "BORSUK_IO_THREADS";
/// Default process-wide concurrency limit for physical backing-store GETs.
pub const DEFAULT_BACKING_GET_CONCURRENCY: usize = 64;
/// Process-wide physical backing-store GET concurrency override.
pub const BACKING_GET_CONCURRENCY_ENV: &str = "BORSUK_BACKING_GET_CONCURRENCY";

/// Process-wide physical-resource limits shared by every BORSUK handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessLimits {
    /// Workers shared by verification, decode, SIMD ranking, and builds.
    pub cpu_threads: usize,
    /// Blocking I/O waiters. This must be at least the backing GET cap so the
    /// network semaphore, rather than the waiter pool, is the truthful limit.
    pub io_threads: usize,
    /// Physical backing-store GETs in flight across the process.
    pub s3_get_concurrency: usize,
}

impl Default for ProcessLimits {
    fn default() -> Self {
        let cpus = std::thread::available_parallelism().map_or(1, usize::from);
        Self {
            cpu_threads: automatic_cpu_threads(cpus),
            io_threads: DEFAULT_IO_THREADS,
            s3_get_concurrency: DEFAULT_BACKING_GET_CONCURRENCY,
        }
    }
}

static PROCESS_LIMITS: std::sync::OnceLock<ProcessLimits> = std::sync::OnceLock::new();

/// Configure process-wide pools before their first use. Repeating the same
/// configuration is idempotent; a conflicting late configuration fails.
pub fn configure_process(limits: ProcessLimits) -> Result<()> {
    validate_process_limits(&limits)?;
    if let Some(installed) = PROCESS_LIMITS.get() {
        return if installed == &limits {
            Ok(())
        } else {
            Err(BorsukError::InvalidOpenOptions(
                "process limits are already configured differently".to_string(),
            ))
        };
    }
    PROCESS_LIMITS.set(limits).map_err(|_| {
        BorsukError::InvalidOpenOptions(
            "process limits were configured concurrently with different values".to_string(),
        )
    })
}

fn automatic_cpu_threads(cpus: usize) -> usize {
    cpus.saturating_sub(1).clamp(1, DEFAULT_BUILD_THREADS)
}

fn validate_process_limits(limits: &ProcessLimits) -> Result<()> {
    if !(1..=64).contains(&limits.cpu_threads) {
        return Err(BorsukError::InvalidOpenOptions(
            "process cpu_threads must be in 1..=64".to_string(),
        ));
    }
    if !(1..=128).contains(&limits.s3_get_concurrency) {
        return Err(BorsukError::InvalidOpenOptions(
            "process s3_get_concurrency must be in 1..=128".to_string(),
        ));
    }
    if !(limits.s3_get_concurrency..=256).contains(&limits.io_threads) {
        return Err(BorsukError::InvalidOpenOptions(format!(
            "process io_threads must be in {}..=256",
            limits.s3_get_concurrency
        )));
    }
    Ok(())
}

fn configured_process_limits() -> &'static ProcessLimits {
    PROCESS_LIMITS.get_or_init(|| {
        let defaults = ProcessLimits::default();
        let cpu_threads = std::env::var(CPU_THREADS_ENV)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|threads| (1..=64).contains(threads))
            .unwrap_or(defaults.cpu_threads);
        let s3_get_concurrency = parse_backing_get_concurrency(
            std::env::var(BACKING_GET_CONCURRENCY_ENV).ok().as_deref(),
        );
        let io_threads = std::env::var(IO_THREADS_ENV)
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|threads| (s3_get_concurrency..=256).contains(threads))
            .unwrap_or_else(|| DEFAULT_IO_THREADS.max(s3_get_concurrency));
        ProcessLimits {
            cpu_threads,
            io_threads,
            s3_get_concurrency,
        }
    })
}

/// Return the process-wide CPU worker budget used by build and query pools.
#[must_use]
pub fn configured_cpu_threads() -> usize {
    configured_process_limits().cpu_threads
}

/// Return the process-wide blocking-I/O waiter budget.
#[must_use]
pub fn configured_io_threads() -> usize {
    configured_process_limits().io_threads
}

/// Return the validated process-wide physical backing-store GET concurrency limit.
#[must_use]
pub fn configured_backing_get_concurrency() -> usize {
    configured_process_limits().s3_get_concurrency
}

fn parse_backing_get_concurrency(value: Option<&str>) -> usize {
    value
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| (1..=128).contains(value))
        .unwrap_or(DEFAULT_BACKING_GET_CONCURRENCY)
}

pub use late_interaction::{
    LateInteractionSearchOptions, LateInteractionSearchReport, LateInteractionVector,
    late_interaction_maxsim,
};
pub use maintenance::{
    DEFAULT_MAINTENANCE_LEASE_TTL, MaintenanceConfig, MaintenanceHandle, MaintenanceReport,
};
pub use manifest::{
    DEFAULT_GRAPH_NEIGHBORS, DEFAULT_ROUTING_PAGE_FANOUT,
    DEFAULT_WAL_COLLECTION_FLUSH_THRESHOLD_BYTES, DEFAULT_WAL_FLUSH_THRESHOLD_BYTES,
    DEFAULT_WAL_FLUSH_THRESHOLD_RECORDS, DEFAULT_WAL_FLUSH_THRESHOLD_RUNS, Manifest, PivotSummary,
    SegmentSummary, WalConfig,
};
pub use metadata::{
    Filter, MetaValue, Metadata, MetadataIndex, MetadataStats, Op, metadata_from_json,
    metadata_to_json,
};
/// A dense vector and its stored metadata returned by point-read APIs.
pub type StoredRecord = (Vec<f32>, Metadata);
pub use metric::{VectorMetric, recall_at_k, tie_aware_recall_at_k, vector_metric_names};
#[doc(hidden)]
pub use object_store::ObjectStore;
pub use physical_layout::{
    CURRENT_LAYOUT_POLICY_VERSION, PhysicalFormat, PhysicalLayoutPolicy, PhysicalLayoutRef,
    production_object_roles,
};
pub use positioned_log::{
    CommitSourcePosition, CommittedPositionedMutation, MAX_APPEND_ENCODED_BYTES, MAX_APPEND_ROWS,
    MAX_HEAD_CAS_ATTEMPTS, MAX_PAYLOADS_PER_TRANSACTION, MAX_PENDING_ENVELOPES_PER_SHARD,
    MAX_RECENT_COMMIT_RECEIPTS_PER_SHARD, MAX_SHARD_HEAD_BYTES, MAX_UNMATERIALIZED_BYTES_PER_SHARD,
    MAX_UNMATERIALIZED_ROWS_PER_SHARD, PositionedLogReader, PositionedLogSnapshot,
    PositionedLogWriter, PositionedMaterializationWatermark, PositionedMutationEnvelope,
    PositionedMutationModality, PositionedMutationPayloadInput, PositionedMutationPayloadRef,
    PositionedMutationStamp, PositionedPayloadFormat, SOURCE_SHARD_COUNT,
};
pub use record::{
    AddReport, BuildConfig, CacheExecutionPolicy, CompactionOptions, CompactionReport,
    DEFAULT_COMPACTION_MAX_SEGMENTS, DEFAULT_GARBAGE_COLLECTION_MIN_AGE,
    DEFAULT_SEARCH_PREFETCH_DEPTH, DEFAULT_TURBOQUANT_SEED, DeleteReport, ExplainReport, Fusion,
    GarbageCollectionOptions, GarbageCollectionReport, GlobalPqLayout, GlobalScanCodec,
    HybridOptions, HybridQuery, IncrementalMaintenanceOptions, IncrementalReport, IndexStats,
    LeafCapability, LeafMode, PurgeReport, QuantizerKind, QueryCostModel, RebuildOptions,
    RebuildReport, RecallGuarantee, RecordId, RequestCounts, SearchHit, SearchMode, SearchOptions,
    SearchReport, SearchTerminationReason, SidecarCompression, StorageEncoding, VectorElementType,
    VectorKind, VectorRecord, VectorSpec, leaf_mode_names,
};
pub use sparse::{
    SparseVector, VectorView, cosine_distance, dot, euclidean_distance, inner_product_distance,
    sparse_dense_dot, sparse_dot, squared_euclidean_distance, squared_norm, squared_norm_dense,
    squared_norm_sparse,
};
pub use sparse_index::SparseIndex;
pub use storage_trace::{
    PhysicalObjectRole, StorageAccessEvent, StorageAccessTrace, install_storage_access_trace,
    physical_object_role_for_path,
};
pub use text::{CharNgram, Tokenizer, UnicodeWordLowercase, Whitespace, term_frequencies, term_id};
#[doc(hidden)]
pub use v24_witness_local::{V24LocalPhase, V24LocalRunRequest, run_v24_local_request};
#[doc(hidden)]
pub use v24_witness_prepare::{V24PreparationRunRequest, run_v24_preparation_request};

#[cfg(test)]
mod configuration_tests {
    #[test]
    fn automatic_cpu_budget_reserves_capacity_for_the_embedding_app() {
        assert_eq!(super::automatic_cpu_threads(1), 1);
        assert_eq!(super::automatic_cpu_threads(2), 1);
        assert_eq!(super::automatic_cpu_threads(4), 3);
        assert_eq!(super::automatic_cpu_threads(64), 4);
    }

    #[test]
    fn default_io_waiters_make_the_physical_get_cap_truthful() {
        let limits = super::ProcessLimits::default();
        assert!(limits.io_threads >= limits.s3_get_concurrency);
        super::validate_process_limits(&limits).unwrap();
        let invalid = super::ProcessLimits {
            io_threads: limits.s3_get_concurrency - 1,
            ..limits
        };
        assert!(super::validate_process_limits(&invalid).is_err());
    }

    #[test]
    fn physical_get_admission_configuration_is_fail_closed() {
        assert_eq!(super::parse_backing_get_concurrency(None), 64);
        assert_eq!(super::parse_backing_get_concurrency(Some("1")), 1);
        assert_eq!(super::parse_backing_get_concurrency(Some("128")), 128);
        for invalid in ["", "0", "129", "1024", "many"] {
            assert_eq!(super::parse_backing_get_concurrency(Some(invalid)), 64);
        }
    }
}
