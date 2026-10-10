"""Assurance-only Spot adapter; root freezes authority and owns paid launch.

Config contract: FIXED plus controller_authority_pending=false,
controller_code_sha256={every CODE path:SHA256}, native_source_manifest=
{path,bytes,sha256}. No completed-assurance dependency. Optional leading
--semantic-1m selects the new root and manifest-pinned native identity.
--semantic-1m-test-build selects compile-only execution of the real test-build script.
--semantic-1m-implementation selects the serial implementation gate script.
--startup-wave8-implementation reuses that lifecycle with separate authority.
--root-reuse-implementation qualifies a root-frozen authenticated-root candidate.
--bounded-publication-implementation qualifies the exact bounded publisher candidate.
--fixed48-implementation qualifies the source-only fixed48 routing candidate.
--constrained-split-implementation qualifies the additive source-neighborhood diagnostic.
--cell-overlap-implementation qualifies bounded boundary overlap; no full test execution.
--fine-sq8-implementation qualifies the native fine-group candidate, without a corpus run.
--pq-residual-implementation qualifies the native raw-PQ residual candidate, without a corpus run.
--co-selection-implementation qualifies the source-only virtual-layout falsifier.
--budget-object-selector-implementation qualifies only the four native evaluator gates.
--budget-object-fitter-implementation executes both modules and the same three build gates.
--cohere-sq8-builder builds the existing source-builder example only.
--baseline-pool-implementation qualifies the baseline blocking-pool repair in both owning targets.
--cohere-cohort-implementation qualifies exactly two new preflight bins.
--cohere1024-implementation qualifies the root-supplied D1024 native candidate.
--hierarchical-cells-implementation uses a separate minimal-archive v2 root.
Its config additionally carries hierarchical_archive_authority(...)'s four
ARCHIVE_FIELDS, derived from the committed controller tree before config freeze.
The config itself is excluded from support hashes to avoid a self-hash cycle.
CLI aNNNN | --self-check | --stage REPO OUT | --check-receipt OUT | --replay OUT.
"""
import contextlib
import copy
import fcntl
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import sys
import tempfile
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('qualification requires assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import check_native_workspace_execution as worker
from scripts import launch_native_semantic_router_cold_spot as semantic
from scripts import package_semantic_native_generation as files

shared, peer, startup = semantic.shared, semantic.peer, semantic.startup
SOURCE_IDENTITY = '714794a10c2d886f7a53a9926a4bb63e093cec16858676268e31833aeead2681'
ROOT = semantic.ROOT / 'metadata-waves/implementation-gates/remote-full'
CONFIG = ROOT / 'config.json'
NAME = ''
SCHEMA = 'borsuk-native-workspace-execution-spot-v1'
CONFIG_SCHEMA = 'borsuk-native-workspace-execution-v1'
PREFIX = 'research/semantic-router/20261001/metadata-waves-workspace-'
TOKEN_PREFIX = 'metadata-waves-workspace-'
TAG = 'borsuk-metadata-waves-workspace'
SEMANTIC_1M = False
TEST_BUILD = False
IMPLEMENTATION = False
STARTUP_WAVE8 = False
ROOT_REUSE = False
BOUNDED_PUBLICATION = False
FIXED48 = False
HIERARCHICAL_CELLS = False
CONSTRAINED_SPLIT = False
CELL_OVERLAP = False
FINE_SQ8 = False
PQ_RESIDUAL = False
CO_SELECTION = False
BUDGET_OBJECT_SELECTOR = False
BUDGET_OBJECT_FITTER = False
COHERE1024 = False
COHERE_COHORT = False
COHERE_SQ8_BUILDER = False
EXACT_SQ8_RUNTIME = False
RETAINED_GENERATION_REBIND = False
BASELINE_POOL = False
MINIMAL_ARCHIVE = False
HIERARCHICAL_CELLS_DELTA = ('crates/borsuk/src/bin/hierarchical_semantic_cells.rs',
                          'crates/borsuk/src/hierarchical_semantic_cells.rs')
HIERARCHICAL_CELLS_STAGE_SCHEMA = 'borsuk-hierarchical-cells-implementation-stage-v1'
HIERARCHICAL_CELLS_REQUIRED_TESTS = {
    'hierarchical-cell-tests': tuple('hierarchical_semantic_cells::tests::' + name for name in (
        'semantic_cells_do_not_close_over_old_pages_and_keep_unchanged_ranking',
        'identical_geometry_is_bounded_and_reproducible_without_truth',
        'source_id_binding_budgets_and_corruption_fail_closed_with_charges',
        'loss_receipt_separates_boundary_recovery_nomination_and_final_ranking',
        'cell_local_block_nomination_omits_other_blocks_even_for_tied_codes',
        'resident_directory_preload_is_admitted_charged_and_has_no_query_reads',
        'resident_preload_rejects_corrupt_unused_interior_page',
        'whole_cell_matches_two_stage_with_one_wave_and_full_payload_charges',
        'nonunit_query_nonzero_mean_preserves_native_sq2_scoring_and_nomination',
        'nomination_global_finds_leaf_hidden_by_hierarchy_pruning',
        'nomination_unpruned_parity_caps_source_binding_and_failure_charges',
        'capacity_partition_matches_exhaustive_cost_capacity_and_id_ties',
        'balanced_and_identical_builder_memberships_are_unchanged',
        'identical_sample_fallback_preserves_projected_child_sum_order',
        'skewed_builder_is_lossless_deterministic_and_preserves_sq2_sq8_tail_ranking',
        'partition_format_and_receipt_reject_obsolete_or_incomplete_artifacts',
        'source_probe_greedy_matches_independent_oracle_ties_shortcell_and_numeric_guards',
        'source_probe_raw_identity_native_score_top24_and_no_query_io',
        'source_probe_cap_binding_tamper_missing_id_fifo_and_symlink_fail_closed',
        'source_probe_shortcell_builder_and_original_span_tamper')),
    'hierarchical-cell-bin-tests': ('tests::configurations_reject_unknown_fields_and_truth_in_requests',
        'tests::created_outputs_close_invalid_on_bad_truth_or_output_cap_without_overwrite',
        'tests::nomination_cli_full64_freezes_prefix_and_both_policies_without_truth',
        'tests::nomination_cli_rejects_truth_fields_incomplete_panels_and_tampering',
        'tests::nomination_prefix_rejects_fifo_and_symlink_without_blocking',
        'tests::nomination_freeze_reserve_and_synced_prefix_tamper_close_invalid',
        'tests::source_probe_cli_actual_full_pipeline_freezes_before_truth_and_closes_invalid',
        'tests::source_probe_cli_coverage_fifo_truth_and_output_cap_close_invalid'),}
HIERARCHICAL_CELLS_STAGES = tuple((name, command.split()) for name, command in (
    ('hierarchical-cell-tests', 'cargo test --locked -p borsuk --lib hierarchical_semantic_cells::'),
    ('hierarchical-cell-bin-tests', 'cargo test --locked -p borsuk --bin hierarchical_semantic_cells'),
    ('generation-integration', 'cargo test --locked -p borsuk --test two_bit_generation'),
    ('release', 'cargo build --release --locked -p borsuk --bin hierarchical_semantic_cells --example two_bit_http --bin build_two_bit_generation --bin check_semantic_router_scorer'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND bash scripts/check_rust_test_build.sh')))
CONSTRAINED_SPLIT_DELTA = ('crates/borsuk/src/bin/check_hierarchical_split_balance.rs',
                           'crates/borsuk/src/hierarchical_semantic_cells.rs')
CONSTRAINED_SPLIT_CONTROL = '8ec64936323aca63d6863351623eebd61c085187'
CONSTRAINED_SPLIT_PREFIX = dict(bytes=115165,
    sha256='f30ca0d0ae1e38f28988cf100aec4bdbc68f1ce4acb4f85f488efa83103b2a1f')
CONSTRAINED_SPLIT_STAGE_SCHEMA = 'borsuk-constrained-split-implementation-stage-v1'
# Exact mandatory names from the native worker contract; empty rosters fail closed.
CONSTRAINED_SPLIT_ADDITIVE_TESTS = tuple('hierarchical_semantic_cells::split_balance_diagnostic::tests::' + name for name in (
    'constrained_cost_matches_exhaustive_small_partitions',
    'successful_identical_and_degenerate_splits_are_unchanged',
    'learned_unbalanced_replay_preserves_centers_and_moves_minimum_population',
    'first_eight_candidate_hash_order_is_frozen_without_expansion',
    'deterministic_delta_ties_coordinate_ties_and_partial_panel',
    'original_membership_mismatch_is_invalid',
    'local_cosine_edges_reuse_same_rows_and_exclude_self',
    'canonical_order_nonfinite_and_post_authentication_tamper_fail_closed',
    'artifact_tamper_geometry_and_all_limits_fail_closed',
    'fifo_symlink_ancestors_and_missing_originals_fail_closed',
    'strict_config_and_created_output_terminal_no_overwrite',
    'output_cap_and_fsync_failure_cannot_return_pass',
))
CONSTRAINED_SPLIT_BIN_TESTS = ('tests::strict_source_only_cli',)
CONSTRAINED_SPLIT_STAGES = tuple((name, command.split()) for name, command in (
    ('hierarchical-cell-tests', 'cargo test --locked -p borsuk --lib hierarchical_semantic_cells::'),
    ('split-balance-bin-tests', 'cargo test --locked -p borsuk --bin check_hierarchical_split_balance'),
    ('generation-integration', 'cargo test --locked -p borsuk --test two_bit_generation'),
    ('release', 'cargo build --release --locked -p borsuk --bin check_hierarchical_split_balance --bin hierarchical_semantic_cells --example two_bit_http'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND bash scripts/check_rust_test_build.sh')))

CELL_OVERLAP_DELTA = ('crates/borsuk/src/bin/hierarchical_semantic_cells.rs',
                      'crates/borsuk/src/hierarchical_semantic_cells.rs',
                      'crates/borsuk/src/lib.rs',
                      'crates/borsuk/src/returned_sq8.rs',
                      'crates/borsuk/src/semantic_cell_overlap.rs')
CELL_OVERLAP_STAGE_SCHEMA = 'borsuk-cell-overlap-implementation-stage-v1'
CELL_OVERLAP_REQUIRED_TESTS = {
    'overlap-tests': (
        'hierarchical_semantic_cells::tests::overlap_centroid_capacity_coordinate_tie_outsider_predicates',
        'hierarchical_semantic_cells::tests::overlap_replay_primary_parity_and_mismatch',
        'hierarchical_semantic_cells::tests::overlap_builder_captures_actual_capacity_last_left_key',
        'hierarchical_semantic_cells::tests::overlap_pinned_replace_owner_absent_replica_present',
        'hierarchical_semantic_cells::tests::overlap_delete_all_underfill_and_capacity_refusal',
        'hierarchical_semantic_cells::tests::overlap_full_scanner_matches_independent_sq8',
        'hierarchical_semantic_cells::tests::overlap_authenticated_open_and_selected_fetch_refuse_corruption',
        'semantic_cell_overlap::tests::overlap_degenerate_descent_without_crossing',
        'semantic_cell_overlap::tests::overlap_quota_priority_and_full_destination',
        'semantic_cell_overlap::tests::overlap_equal_path_margin_prefers_depth_then_boundary_identity',
        'semantic_cell_overlap::tests::overlap_corrupt_frame_mapping_and_body',
        'returned_sq8::tests::overlap_nonunit_sq8_and_duplicate_truncation',
        'hierarchical_semantic_cells::tests::overlap_metadata_cap_refuses_before_missing_heavy_files',
        'hierarchical_semantic_cells::tests::overlap_pair_individually_fit_combined_overcap_has_zero_heavy_opens',
        'hierarchical_semantic_cells::tests::overlap_streamed_original_auth_boundaries_and_mutation'),
    'overlap-bin-tests': ('tests::overlap_pair_seal_precedes_truth',)}
CELL_OVERLAP_STAGES = tuple((name, command.split()) for name, command in (
    ('overlap-tests', 'cargo test --locked -p borsuk --lib overlap_ -- --test-threads=1'),
    ('overlap-bin-tests', 'cargo test --locked -p borsuk --bin hierarchical_semantic_cells overlap_pair_seal_precedes_truth -- --test-threads=1'),
    ('hierarchical-cell-regressions', 'cargo test --locked -p borsuk --lib hierarchical_semantic_cells::tests -- --test-threads=1'),
    ('returned-sq8-regressions', 'cargo test --locked -p borsuk --lib returned_sq8::tests -- --test-threads=1'),
    ('release', 'cargo build --release --locked -p borsuk --bin hierarchical_semantic_cells'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh')))

FINE_SQ8_DELTA = ('crates/borsuk/src/bin/hierarchical_semantic_cells.rs',
                  'crates/borsuk/src/corrected_four_bit.rs',
                  'crates/borsuk/src/fine_sq8_groups.rs',
                  'crates/borsuk/src/lib.rs')
FINE_SQ8_STAGE_SCHEMA = 'borsuk-corrected-four-bit-implementation-stage-v1'
# Exact target-qualified names from the final native sibling contract; source SHA is root-owned.
FINE_SQ8_REQUIRED_TESTS = {
    'corrected-four-bit-tests': tuple('corrected_four_bit::tests::' + name for name in (
        'corrected_four_bit_scalar_ratio_and_rounding',
        'corrected_four_bit_exhaustive_tiny_grid',
        'corrected_four_bit_exact_threshold_collision_and_subnormal',
        'corrected_four_bit_rotation_bits_gram_and_corruption',
        'corrected_four_bit_packing_tail_nonunit_and_caps',
    )),
    'fine-sq8-tests': (
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::tests::fine_corrected_four_bit_full128_pipeline_freeze_and_late_invalid',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::tests::fine_corrected_four_bit_prebody_caps_source_eof_and_sync',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::histogram::tests::fine_histogram_sq4_dp_independent_brute_partitions_and_deviations',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::histogram::tests::fine_histogram_sq4_interval_cancellation_against_direct_deviations',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::histogram::tests::fine_histogram_sq4_exhaustive256_mapping_ties_and_uniform_bitwise_parity',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::histogram::tests::fine_histogram_sq4_scalar_scores_odd_tail_zero_nonunit_near_ties',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::histogram::tests::fine_histogram_sq4_book_binding_padding_counts_caps_and_numeric_negatives',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::tests::fine_histogram_sq4_full128_pipeline_books_requests_truth_closure_and_late_invalid',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::tests::fine_histogram_sq4_source_reauthentication_eof_prebody_caps_and_sync',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::tests::fine_sq4_all_codes_numeric_oracle_odd_tail_nonunit_ties',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::tests::fine_sq4_exact_cover_superset_and_binding',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::tests::fine_sq4_auth_fifo_corruption_caps_and_durability',
        'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::tests::fine_sq4_full_pipeline_failure_order_and_sync',
        'fine_sq8_groups::pack_diagnostic::tests::pack_affinity_matches_exhaustive_ties',
        'fine_sq8_groups::pack_diagnostic::tests::pack_tail_bijection_and_cover_match_brute',
        'fine_sq8_groups::pack_diagnostic::tests::pack_graph_binding_corruption_caps',
        'fine_sq8_groups::pack_diagnostic::tests::pack_secure_prefix_and_forbidden_opens',
        'fine_sq8_groups::pack_diagnostic::tests::pack_both_seals_precede_prefix',
        'fine_sq8_groups::pack_diagnostic::tests::pack_incomplete_output_sync_and_supervisor',
        'fine_sq8_groups::tests::fine_remote_failure_preserves_partial_known_stats_and_unknown_bytes',
        'fine_sq8_groups::tests::fine_cover_matches_independent_bruteforce_with_tail',
        'fine_sq8_groups::tests::fine_tail_scalar_ties_nonunit_and_pins',
        'fine_sq8_groups::tests::fine_exhausted_shortlist_is_scored_empty_is_underfilled_and_refusal_reads_nothing',
        'fine_sq8_groups::tests::fine_nonzero_coefficients_scalar_oracle_extremes_and_near_ties',
        'fine_sq8_groups::tests::fine_bridged_corruption_and_best_replacement_outside_shortlist',
        'fine_sq8_groups::tests::fine_auth_length_symlink_and_admission_fail_closed',
    ),
    'pq-codes-graph-tests': (
        'resident_vector_graph::bounded_pq_tests::repaired_producer_above_m0_reopens_and_decoded_allocations_are_bounded',
        'resident_vector_graph::bounded_pq_tests::total_budget_counts_upper_layers_and_reports_partial_ties',
    ),
    'source-pq-tests': (
        'pq64_nominee::source_codes_tests::source_fit_is_ordinal_deterministic_and_codes_only',
    ),
    'fine-sq8-bin-tests': (
        'tests::fine_corrected_four_bit_strict_cli_dispatch',
        'tests::fine_corrected_four_bit_real_native_two_panel_d3_all128_incidental_and_late_truth',
        'tests::fine_histogram_sq4_strict_cli_dispatch_and_schema_identity',
        'tests::fine_histogram_sq4_real_native_builder_paired_pipeline_all128_full_rosters_late_invalid',
        'tests::fine_sq4_real_native_pipeline_128_seal_before_truth',
        'tests::fine_sq4_strict_cli_dispatch',
        'tests::fine_pack_strict_cli_real_tiny_pipeline',
        'tests::fine_real_source_pipeline_seals_both_panels_before_gt_and_is_durable',
        'tests::fine_fresh_process_plane_free_open',
    ),
    'graph-regressions': (
        'resident_vector_graph::tests::graph_ties_keep_smallest_physical_ordinals',
        'resident_vector_graph::tests::graph_returns_stable_ids_and_rejects_generation_mismatch',
    ),
    'pq-regressions': (
        'pq64_nominee::tests::selects_the_nearest_page_and_orders_tied_rows_by_ordinal',
        'pq64_nominee::tests::admits_a_short_final_page_without_padding_rows',
        'pq64_nominee::tests::pads_a_nonmultiple_of_64_dimension_only_inside_pq_distance',
        'pq64_nominee::tests::ninety_six_dimensions_score_the_last_coordinate_in_the_last_subspace',
        'pq64_nominee::tests::huge_page_width_allocates_only_the_one_actual_candidate',
        'pq64_nominee::tests::scores_only_requested_rows_in_caller_order',
        'pq64_nominee::tests::cosine_view_uses_reconstructed_direction_instead_of_squared_l2',
        'pq64_nominee::tests::row_scores_use_the_same_d96_subspace_padding_as_nomination',
        'pq64_nominee::tests::scored_rows_match_nominee_adc_order',
    ),
    'page-cover-regressions': (
        'budgeted_page_rank::tests::source_unit_cover_preserves_all_units_and_clips_tail',
        'budgeted_page_rank::tests::sparse_candidates_never_admit_an_unvisited_page',
        'budgeted_page_rank::tests::complete_sparse_scores_refine_dense_page_admission',
        'budgeted_page_rank::tests::sparse_candidates_reject_duplicates_and_unknown_pages',
        'budgeted_page_rank::tests::incremental_cover_charge_matches_exact_ranges_with_tied_gaps_and_final_page',
        'budgeted_page_rank::tests::incremental_cover_charge_matches_exact_cover_across_page_patterns',
        'budgeted_page_rank::tests::primary_page_precedes_better_scoring_secondary_page',
        'budgeted_page_rank::tests::cheapest_gap_bridge_counts_bytes_and_rejects_over_cap_page',
        'budgeted_page_rank::tests::short_final_page_is_charged_by_its_actual_rows',
        'budgeted_page_rank::tests::signed_zero_scores_tie_by_page_id',
    ),
    'returned-sq8-regressions': (
        'returned_sq8::tests::overlap_nonunit_sq8_and_duplicate_truncation',
        'returned_sq8::tests::query_admission_accounts_for_narrow_rows_concurrent_planners_and_overflow',
        'returned_sq8::tests::ranks_sparse_ranges_and_ties_by_id',
        'returned_sq8::tests::mutation_visibility_precedes_top_k_and_validates_masked_rows',
        'returned_sq8::tests::rejects_overlap_unaligned_budget_and_duplicate_ids',
    ),
    'page-authority-regressions': (
        'sq8_page_authority::tests::two_bit_unit_pages_bind_geometry_and_authenticate_partial_tail',
        'sq8_page_authority::tests::authenticates_short_final_page_and_rejects_changed_bytes_or_etag',
    ),
    's3-range-regressions': (
        'sq8_s3_range::tests::bounded_range_query_future_is_send',
        'sq8_s3_range::tests::source_authority_is_rejected_before_sq8_get',
        'sq8_s3_range::tests::bounded_source_ranges_authenticate_tail_and_charge_failures',
        'sq8_s3_range::tests::redirects_and_unfinished_error_bodies_fail_after_one_request',
        'sq8_s3_range::tests::generation_publish_reload_and_http_search_fail_closed',
        'sq8_s3_range::tests::real_http_range_faults_fail_closed_without_hidden_retries',
        'sq8_s3_range::tests::conditional_short_tail_rejects_mutated_object',
        'sq8_s3_range::tests::bounded_query_reads_only_verified_pages_and_charges_failures',
    ),
    'hierarchical-bin-regressions': (
        'tests::overlap_pair_seal_precedes_truth',
        'tests::source_probe_cli_actual_full_pipeline_freezes_before_truth_and_closes_invalid',
        'tests::source_probe_cli_coverage_fifo_truth_and_output_cap_close_invalid',
        'tests::nomination_cli_full64_freezes_prefix_and_both_policies_without_truth',
        'tests::nomination_cli_rejects_truth_fields_incomplete_panels_and_tampering',
        'tests::nomination_prefix_rejects_fifo_and_symlink_without_blocking',
        'tests::nomination_freeze_reserve_and_synced_prefix_tamper_close_invalid',
        'tests::created_outputs_close_invalid_on_bad_truth_or_output_cap_without_overwrite',
        'tests::configurations_reject_unknown_fields_and_truth_in_requests',
    ),
}
FINE_SQ8_STAGES = tuple((name, command.split()) for name, command in (
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('corrected-four-bit-tests', 'cargo test --locked -p borsuk --lib corrected_four_bit:: -- --test-threads=1'),
    ('fine-sq8-bin-tests', 'cargo test --locked -p borsuk --bin hierarchical_semantic_cells fine_ -- --test-threads=1'),
    ('fine-sq8-tests', 'cargo test --locked -p borsuk --lib fine_sq8_groups:: -- --test-threads=1'),
    ('pq-codes-graph-tests', 'cargo test --locked -p borsuk --lib resident_vector_graph::bounded_pq_tests -- --test-threads=1'),
    ('source-pq-tests', 'cargo test --locked -p borsuk --lib pq64_nominee::source_codes_tests -- --test-threads=1'),
    ('graph-regressions', 'cargo test --locked -p borsuk --lib resident_vector_graph::tests -- --test-threads=1'),
    ('pq-regressions', 'cargo test --locked -p borsuk --lib pq64_nominee::tests -- --test-threads=1'),
    ('page-cover-regressions', 'cargo test --locked -p borsuk --lib budgeted_page_rank::tests -- --test-threads=1'),
    ('returned-sq8-regressions', 'cargo test --locked -p borsuk --lib returned_sq8::tests -- --test-threads=1'),
    ('page-authority-regressions', 'cargo test --locked -p borsuk --lib sq8_page_authority::tests -- --test-threads=1'),
    ('s3-range-regressions', 'cargo test --locked -p borsuk --lib sq8_s3_range::tests -- --test-threads=1'),
    ('hierarchical-bin-regressions', 'cargo test --locked -p borsuk --bin hierarchical_semantic_cells -- --test-threads=1 --skip fine_'),
    ('release', 'cargo build --release --locked -p borsuk --bin hierarchical_semantic_cells'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh')))


PQ_RESIDUAL_DELTA = ('crates/borsuk/src/bin/hierarchical_semantic_cells.rs',
                     'crates/borsuk/src/fine_sq8_groups.rs',
                     'crates/borsuk/src/lib.rs',
                     'crates/borsuk/src/pq64_nominee.rs',
                     'crates/borsuk/src/pq_residual_four_bit.rs')
PQ_RESIDUAL_STAGE_SCHEMA = 'borsuk-pq-residual-implementation-stage-v1'
# Final native sibling contract; the root supplies the native commit and full inventory.
PQ_RESIDUAL_REQUIRED_TESTS = {
    'pq-residual-codec-tests': tuple('pq_residual_four_bit::tests::' + name for name in (
        'pq_residual_raw_pq_floor_padding_and_physical_order',
        'pq_residual_bins_constants_endpoints_subnormal_and_center_ties',
        'pq_residual_original_norm_nonunit_near_ulp_scalar_oracle',
        'pq_residual_book_binding_corruption_padding_and_caps',
        'pq_residual_literal_d1_d3_d65_roundtrip_and_native_constant_ties',
    )),
    'pq-residual-source-tests': tuple('fine_sq8_groups::pack_diagnostic::sq4_diagnostic::pq_residual_source::tests::' + name for name in (
        'pq_residual_dp_independent_brute_cost_and_ties',
        'pq_residual_actual_native_pipeline_selection_no_gt_and_sealed_recount',
        'pq_residual_admission_underflow_and_same_size_bindings',
        'pq_residual_pipeline_pass_mutation_eof_partial_sync_seal_and_late_invalid',
        'pq_residual_frozen_thresholds_cohort_membership_ties_and_supervisor',
    )),
    'pq-residual-bin-tests': ('tests::pq_residual_source_strict_cli_dispatch_no_geometry_or_truth_override',),
    'pq-residual-doc-tests': ('src/pq_residual_four_bit.rs - pq_residual_four_bit::Axis (line 13) - compile fail',),
    **FINE_SQ8_REQUIRED_TESTS,
}
PQ_RESIDUAL_STAGES = (
    ('pq-residual-codec-tests', 'cargo test --locked -p borsuk --lib pq_residual_four_bit::tests -- --test-threads=1'.split()),
    ('pq-residual-source-tests', 'cargo test --locked -p borsuk --lib fine_sq8_groups::pack_diagnostic::sq4_diagnostic::pq_residual_source::tests -- --test-threads=1'.split()),
    ('pq-residual-bin-tests', 'cargo test --locked -p borsuk --bin hierarchical_semantic_cells pq_residual_source_strict_cli_dispatch_no_geometry_or_truth_override -- --test-threads=1'.split()),
    ('pq-residual-doc-tests', 'cargo test --locked -p borsuk --doc pq_residual_four_bit -- --test-threads=1'.split()),
    *((name, command + (['--skip', 'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::pq_residual_source::tests::'] if name == 'fine-sq8-tests'
        else ['--skip', 'tests::pq_residual_source_strict_cli_dispatch_no_geometry_or_truth_override'] if name == 'hierarchical-bin-regressions' else []))
      for name, command in FINE_SQ8_STAGES if name not in ('clippy', 'test-build')),
    *tuple((name, command) for name, command in FINE_SQ8_STAGES if name in ('clippy', 'test-build')),
)


CO_SELECTION_DELTA = ('crates/borsuk/src/bin/hierarchical_semantic_cells.rs',
                      'crates/borsuk/src/co_selection_layout.rs',
                      'crates/borsuk/src/fine_sq8_groups.rs',
                      'crates/borsuk/src/lib.rs')
CO_SELECTION_STAGE_SCHEMA = 'borsuk-co-selection-implementation-stage-v1'
# Final SAME native sibling contract; root supplies native commit and full inventory.
CO_SELECTION_REQUIRED_TESTS = {
    'co-selection-tests': (
        'co_selection_layout::tests::co_selection_cover_matches_independent_interval_dp_and_exhaustive_masks',
        'co_selection_layout::tests::co_selection_anchor_hash_domain_and_caps_are_frozen',
        'co_selection_layout::tests::co_selection_objective_swaps_and_all_ties_match_independent_recomputation',
        'co_selection_layout::tests::co_selection_bijection_short_tail_objects_nominees_and_total_budget',
        'co_selection_layout::tests::co_selection_expansion_uses_one_anchor_and_skips_unaffordable_groups',
        'co_selection_layout::tests::co_selection_source_anchor_purity_actual_nominees_and_identity_tamper',
        'co_selection_layout::tests::co_selection_actual_cli128_seals_before_prefix_and_never_reads_payload_tail',
        'co_selection_layout::tests::co_selection_cli_complete_reject_does_not_mask_late_invalid_or_hash_tamper',
        'co_selection_layout::tests::co_selection_cli_output_fsync_resource_failures_and_strict_config_are_invalid',
        'co_selection_layout::tests::co_selection_phase_ledgers_and_full_batch_work_preflight_are_checked',
        'co_selection_layout::tests::co_selection_terminal_fsync_failure_overrides_all128_success',
    ),
    'co-selection-bin-tests': (
        'tests::co_selection_layout_strict_cli_dispatch_no_geometry_truth_or_selection_override',
    ),
}
CO_SELECTION_STAGES = (
    ('co-selection-tests', 'cargo test --locked -p borsuk --lib co_selection_layout::tests -- --test-threads=1'.split()),
    ('co-selection-bin-tests', 'cargo test --locked -p borsuk --bin hierarchical_semantic_cells co_selection_ -- --test-threads=1'.split()),
    *((name, command + (['--skip', 'tests::co_selection_'] if name == 'hierarchical-bin-regressions' else []))
      for name, command in PQ_RESIDUAL_STAGES),
)


BUDGET_OBJECT_SELECTOR_DELTA = ('crates/borsuk/src/budget_object_selector.rs', 'crates/borsuk/src/lib.rs')
# Historical protocol comparison only; production source authority is root-frozen.
BUDGET_OBJECT_SELECTOR_CHECK_BASE = '2d5ebb79dddb48b7424439b85121f19cf9fd7341'
BUDGET_OBJECT_SELECTOR_STAGE_SCHEMA = 'borsuk-budget-object-selector-implementation-stage-v1'
# Exact names verified against the same worker's final committed source contract.
BUDGET_OBJECT_SELECTOR_REQUIRED_TESTS = {
    'budget-object-selector-tests': tuple('budget_object_selector::tests::'+name for name in (
        'oracle_exhaustive_six_group_four_label_capacity_two',
        'oracle_mixture_33_omitted_global_winner_and_ties',
        'oracle_scale_s335_bounded_candidates_and_charges',
        'oracle_finite_mlp_nonunit_query_matches_independent_scalar',
        'oracle_budget_boundaries_overflow_and_finite_validation',
        'oracle_identity_binding_and_determinism',
        'oracle_move_swap_and_checkpoint_strict_acceptance')),
}
BUDGET_OBJECT_SELECTOR_STAGES = tuple((name, command.split()) for name, command in (
    ('budget-object-selector-tests', 'cargo test --locked -p borsuk --lib budget_object_selector::tests -- --test-threads=1'),
    ('release', 'cargo build --release --locked -p borsuk --lib --bin hierarchical_semantic_cells'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh')))


BUDGET_OBJECT_FITTER_DELTA = ('crates/borsuk/src/budget_object_fitter.rs',
                            'crates/borsuk/src/budget_object_selector.rs', 'crates/borsuk/src/lib.rs')
BUDGET_OBJECT_FITTER_STAGE_SCHEMA = 'borsuk-budget-object-fitter-implementation-stage-v1'
# Exact names from the same native worker final committed source contract.
BUDGET_OBJECT_FITTER_REQUIRED_TESTS = {
    'budget-object-fitter-tests': (
        'budget_object_selector::tests::oracle_exhaustive_six_group_four_label_capacity_two',
        'budget_object_selector::tests::oracle_mixture_33_omitted_global_winner_and_ties',
        'budget_object_selector::tests::oracle_scale_s335_bounded_candidates_and_charges',
        'budget_object_selector::tests::oracle_finite_mlp_nonunit_query_matches_independent_scalar',
        'budget_object_selector::tests::oracle_budget_boundaries_overflow_and_finite_validation',
        'budget_object_selector::tests::oracle_identity_binding_and_determinism',
        'budget_object_selector::tests::oracle_move_swap_and_checkpoint_strict_acceptance',
        'budget_object_selector::tests::oracle_prepared_full_receipts_new_occupancy_tails_and_identity',
        'budget_object_selector::tests::oracle_prepared_checkpoint_full_receipts_and_lower_tail',
        'budget_object_selector::tests::oracle_prepared_scale_s335_exact_serving_receipt',
        'budget_object_selector::tests::oracle_observed_selection_full_receipts_identity_and_no_repeat_validation',
        'budget_object_selector::tests::oracle_observed_unrelated_occupancy_displacement_and_row_underfill',
        'budget_object_selector::tests::oracle_observed_baseline_acceptance_full_receipts_tail_and_stale_state',
        'budget_object_fitter::tests::oracle_fitter_asymmetric_forward_gradient_and_finite_difference',
        'budget_object_fitter::tests::oracle_fitter_sha_initialization_and_fixed_batch_update_order',
        'budget_object_fitter::tests::oracle_fitter_tiny_actual_pipeline_determinism_acceptance_and_rollback',
        'budget_object_fitter::tests::oracle_fitter_actual_lower_tail_checkpoint_rollback_and_current_state',
        'budget_object_fitter::tests::oracle_fitter_capacity_move_smallest_swap_and_production_mass',
        'budget_object_fitter::tests::oracle_fitter_weighted_destinations_anchor_order_and_label_ties',
        'budget_object_fitter::tests::oracle_fitter_diagnostic_tokens_bind_source_and_complete_membership',
        'budget_object_fitter::tests::oracle_fitter_refusal_caps_coexistence_corruption_and_source_unchanged',
    ),
}
BUDGET_OBJECT_FITTER_STAGES = (
    ('budget-object-fitter-tests', 'cargo test --locked -p borsuk --lib budget_object_ -- --test-threads=1'.split()),
    *BUDGET_OBJECT_SELECTOR_STAGES[1:],
)

COHERE1024_DELTA = ('crates/borsuk/src/semantic_unit_router.rs',
                   'crates/borsuk/src/two_bit_generation.rs')
COHERE1024_STAGE_SCHEMA = 'borsuk-cohere1024-implementation-gates-stage-v1'
COHERE1024_REQUIRED_TESTS = {
    'generation-tests': (
        'two_bit_generation::source_walk_tests::native_100k_d1024_generation_serving_scalar_oracle',),
    'generation-release-test': (
        'two_bit_generation::source_walk_tests::native_100k_d1024_generation_serving_scalar_oracle',),
}
COHERE1024_STAGES = tuple((name, command.split()) for name, command in (
    ('generation-tests', 'cargo test --locked -p borsuk --lib two_bit_generation:: -- --test-threads=1'),
    ('generation-release-test', 'cargo test --release --locked -p borsuk --lib two_bit_generation::source_walk_tests::native_100k_d1024_generation_serving_scalar_oracle -- --exact --test-threads=1'),
    ('router-tests', 'cargo test --locked -p borsuk --lib semantic_unit_router:: -- --test-threads=1'),
    ('release', 'cargo build --release --locked -p borsuk --example two_bit_http --bin build_two_bit_generation --bin build_semantic_unit_router --bin repackage_semantic_generation --bin check_semantic_router_scorer'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh')))


# One prospective source-only publisher bin; the control root already holds the baseline
# runner and cohort preparer. Final native identity is root-owned.
COHERE_COHORT_DELTA = ('crates/borsuk/src/bin/publish_two_bit_generation.rs',)
COHERE_COHORT_STAGE_SCHEMA = 'borsuk-cohere-cohort-implementation-gates-stage-v1'
COHERE_COHORT_REQUIRED_TESTS = {
    'cohere-native-publisher-tests': (
        'tests::publishes_real_generation_and_receipt_then_refuses_replay',
        'tests::rejects_wrong_root_cap_existing_head_and_bad_config',
    ),
    'cohere-native-baseline-tests': (
        'tests::production_contract_rejects_unknown_hash_geometry_and_caps',
        'tests::secure_artifacts_reject_hash_length_eof_symlink_fifo_and_bad_rows',
        'tests::scratch_formula_keeps_codec_and_trace_separate',
        'tests::actual_native_queries_seal_and_reduce_against_literal_oracles',
        'tests::missing_or_tampered_truth_is_invalid_only_after_all_queries_are_sealed',
        'tests::prefix_or_requests_tampering_blocks_first_truth_open',
        'tests::source_binding_and_all_request_rows_fail_before_queries',
        'tests::reporting_underfill_keeps_fixed_ten_denominator_and_bounded_output',
    ),
    'cohere-native-cohort-tests': (
        'tests::two_shard_original_bits_scalar_truth_and_locators',
        'tests::fixed_list_and_integer_ids',
        'tests::invalid_embeddings_and_duplicate_ids',
        'tests::strict_config_and_missing_columns',
        'tests::secure_hash_eof_link_fifo_inputs',
        'tests::metadata_and_batch_memory_caps',
        'tests::missing_rows_and_occupied_output',
        'tests::sync_failure_removes_complete_marker',
    ),
}
COHERE_COHORT_STAGES = tuple((name, command.split()) for name, command in (
    ('cohere-native-publisher-tests', 'cargo test --locked -p borsuk --bin publish_two_bit_generation -- --test-threads=1'),
    ('cohere-native-baseline-tests', 'cargo test --locked -p borsuk --bin check_cohere_native_baseline -- --test-threads=1'),
    ('cohere-native-cohort-tests', 'cargo test --locked -p borsuk --bin prepare_cohere_native_cohort -- --test-threads=1'),
    ('release', 'cargo build --release --locked -p borsuk --bin check_cohere_native_baseline --bin prepare_cohere_native_cohort --bin publish_two_bit_generation'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh')))
NON_TEST_STAGES = ('release', 'clippy', 'test-build', 'exact-sq8-probe')
EXACT_SQ8_HISTORICAL_BASE = 'd6b8e1f7b1998292460c2dbc214f45c16905e96c'  # controller before this mode: every earlier mode must stay byte-identical
EXAMPLE_BINARIES = ('/two_bit_http', '/build_sq8_source', '/exact_sq8_runtime_probe')  # copied from target/release/examples

# Exact SQ8 row-blocking runtime probe: a DATA variant of the cohort-family lifecycle. configure()
# rebinds only COHERE_COHORT_{STAGES,STAGE_SCHEMA,REQUIRED_TESTS} below, and restores them for every other mode.
EXACT_SQ8_RUNTIME_DELTA = ('crates/borsuk/Cargo.toml', 'crates/borsuk/examples/exact_sq8_runtime_probe.rs',
                           'crates/borsuk/src/exact_sq8_nominee.rs')
EXACT_SQ8_RUNTIME_STAGE_SCHEMA = 'borsuk-exact-sq8-runtime-gates-stage-v1'
EXACT_SQ8_SCORER_TESTS = tuple('exact_sq8_nominee::tests::' + name for name in (
    'exact_rows_score_and_tie_by_id',
    'rejects_short_final_row_invalid_ordinal_and_nonfinite_query',
    'file_and_ram_placements_score_the_same_bytes',
    'blocked_scores_equal_independent_scalar_oracle_bits',
    'rosters_keep_requested_order_ids_and_ordinals_across_blocks',
    'signed_zero_negative_nonunit_and_near_ties_keep_bits_and_id_order',
    'corrupt_inputs_keep_error_classes_in_every_block_position'))
EXACT_SQ8_RUNTIME_REQUIRED_TESTS = {'exact-sq8-scorer-tests': EXACT_SQ8_SCORER_TESTS,
                                    'exact-sq8-scorer-release-tests': EXACT_SQ8_SCORER_TESTS}
EXACT_SQ8_PROBE_COMMAND = ['bash', '-c', 'exec taskset -c 0 "$CARGO_TARGET_DIR/release/examples/exact_sq8_runtime_probe"']
EXACT_SQ8_RUNTIME_STAGES = tuple((name, command if type(command) is list else command.split()) for name, command in (
    ('exact-sq8-scorer-tests', 'cargo test --locked -p borsuk --lib exact_sq8_nominee:: -- --test-threads=1'),
    ('exact-sq8-scorer-release-tests', 'cargo test --release --locked -p borsuk --lib exact_sq8_nominee:: -- --test-threads=1'),
    ('returned-sq8-tests', 'cargo test --locked -p borsuk --lib returned_sq8:: -- --test-threads=1'),
    ('release', 'cargo build --release --locked -p borsuk --example exact_sq8_runtime_probe --bin check_cohere_native_baseline'),
    ('exact-sq8-probe', EXACT_SQ8_PROBE_COMMAND),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh')))
EXACT_SQ8_RUNTIME_RELEASE = ('binaries/check_cohere_native_baseline', 'binaries/exact_sq8_runtime_probe')
EXACT_SQ8_PROBE_ARTIFACT = 'exact-sq8-probe.jsonl'
EXACT_SQ8_PROBE_SHAPES = {(d, r) for d in (128, 768, 1024) for r in (1, 7, 8, 9, 257, 16192)}
_COHORT_PROTOCOL = (COHERE_COHORT_STAGES, COHERE_COHORT_STAGE_SCHEMA, COHERE_COHORT_REQUIRED_TESTS)
_COHORT_REBOUND = False

# Retained generation rebinding: a second DATA variant of the cohort-family lifecycle (see exact SQ8 above).
# configure() rebinds only COHERE_COHORT_{STAGES,STAGE_SCHEMA,REQUIRED_TESTS}. The roster is frozen to the final names
# the root released for the native worker's final repair; no native commit or SHA is pinned (the root supplies the manifest).
RETAINED_REBIND_HISTORICAL_BASE = '2aa6571414045d5710b23975c66bd0ca8eebc98f'  # controller before this mode: every earlier mode must stay byte-identical
RETAINED_REBIND_DELTA = ('crates/borsuk/src/bin/publish_two_bit_generation.rs', 'crates/borsuk/src/two_bit_store.rs',
                         'crates/borsuk/tests/two_bit_generation.rs')
RETAINED_REBIND_STAGE_SCHEMA = 'borsuk-retained-generation-rebind-implementation-gates-stage-v1'
RETAINED_REBIND_TEST_SOURCES = {'retained-rebind-integration-tests': 'crates/borsuk/tests/two_bit_generation.rs',
                                'retained-rebind-publisher-tests': 'crates/borsuk/src/bin/publish_two_bit_generation.rs'}
RETAINED_REBIND_REQUIRED_TESTS = {
    'retained-rebind-integration-tests': (
        'retained_semantic_republication_preserves_payload_and_score_bits',
        'retained_semantic_republication_refuses_unapproved_changed_or_unbounded_inputs',
    ),
    'retained-rebind-publisher-tests': (
        *_COHORT_PROTOCOL[2]['cohere-native-publisher-tests'],
        'tests::retained_publishes_copied_source_gone_generation_and_bound_receipt',
        'tests::retained_refuses_bad_config_pending_head_and_occupied_output_without_head',
    ),
}
RETAINED_REBIND_STAGES = tuple((name, command.split()) for name, command in (
    ('retained-rebind-integration-tests', 'cargo test --locked -p borsuk --test two_bit_generation retained_semantic_ -- --test-threads=1'),
    ('retained-rebind-publisher-tests', 'cargo test --locked -p borsuk --bin publish_two_bit_generation -- --test-threads=1'),
    ('release', 'cargo build --release --locked -p borsuk --bin publish_two_bit_generation --bin check_cohere_native_baseline'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh')))
RETAINED_REBIND_RELEASE = ('binaries/check_cohere_native_baseline', 'binaries/publish_two_bit_generation')
# Environment only: a0003/a0005 RunInstances lacked Spot capacity in zones 1a/1c. This eu-central-1b subnet is in the
# same VPC as SUBNET; the retained mode alone uses it and configure() restores SUBNET for every other mode.
RETAINED_REBIND_SUBNET = 'subnet-00243d923761c047c'


BASELINE_POOL_DELTA = ('crates/borsuk/src/bin/check_cohere_native_baseline.rs',)
BASELINE_POOL_STAGE_SCHEMA = 'borsuk-baseline-pool-implementation-gates-stage-v1'
BASELINE_POOL_BIN_TESTS = tuple('tests::' + name for name in (
    'query_runtime_bounds_blocking_threads_and_drains',
    'cold_fetch_parallelism_selector_is_explicit_and_bounded',
    'actual_native_queries_seal_and_reduce_against_literal_oracles'))
BASELINE_POOL_REQUIRED_TESTS = {
    'baseline-pool-bin-tests': BASELINE_POOL_BIN_TESTS,
    'baseline-pool-replay-tests': (*('scale_native_runner::' + name for name in BASELINE_POOL_BIN_TESTS),
        'tests::actual_native_scale_runner_reducer_seal_count_mismatches'),
}
# Full targets include Tokio tests and the embedded producer/source-cover test modules.
BASELINE_POOL_TEST_COUNTS = {'baseline-pool-bin-tests': 42, 'baseline-pool-replay-tests': 80}
BASELINE_POOL_STAGES = tuple((name, command.split()) for name, command in (
    ('baseline-pool-bin-tests', 'cargo test --locked -p borsuk --bin check_cohere_native_baseline -- --test-threads=1'),
    ('baseline-pool-replay-tests', 'cargo test --locked -p borsuk --example compare_native_replay -- --test-threads=1'),
    ('release', 'cargo build --release --locked -p borsuk --bin check_cohere_native_baseline'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 CARGO_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh')))
BASELINE_POOL_RELEASE = ('binaries/check_cohere_native_baseline',)


def validate_baseline_pool_sources(base):
    """Bind the three baseline tests to both targets and the reducer to its example."""
    baseline = (Path(base)/BASELINE_POOL_DELTA[0]).read_text()
    replay = (Path(base)/'crates/borsuk/examples/compare_native_replay.rs').read_text()
    assert '#[path = "../src/bin/check_cohere_native_baseline.rs"]\nmod scale_native_runner;' in replay, 'replay baseline module binding'
    # shortcut: these fixed Rust declarations are checked textually; native stage logs remain the test authority.
    for body, names in ((baseline, BASELINE_POOL_BIN_TESTS),
                        (replay, BASELINE_POOL_REQUIRED_TESTS['baseline-pool-replay-tests'][-1:])):
        assert re.search(r'^mod tests \{', body, re.MULTILINE), 'owning tests module'
        for name in names:
            assert len(re.findall(r'^    #\[test\]\n    fn ' + re.escape(name.removeprefix('tests::')) + r'\(', body, re.MULTILINE)) == 1, 'unique owning test declaration: ' + name


def _baseline_pool_self_check():
    """Source/command parsing only; never run a gate or manufacture native evidence."""
    import ast
    import shlex
    base = Path(__file__).resolve().parents[1]
    validate_baseline_pool_sources(base)
    script = base/'scripts/check_baseline_pool_implementation.sh'
    body = script.read_text()
    stages = [shlex.split(line)[1:] for line in body.splitlines() if line.startswith('run_stage ')]
    assert stages == [[name, *command] for name, command in BASELINE_POOL_STAGES], 'exact five serial shell gates'
    assert len(stages) == 5 and all('--test-threads=1' in stage for stage in stages[:2])
    assert stages[2][1:] == ['cargo', 'build', '--release', '--locked', '-p', 'borsuk', '--bin', 'check_cohere_native_baseline']
    names = [name for tests in BASELINE_POOL_REQUIRED_TESTS.values() for name in tests]
    assert len(names) == len(set(names)) == 7, 'distinct owning test names'
    assert BASELINE_POOL_DELTA == ('crates/borsuk/src/bin/check_cohere_native_baseline.rs',)
    assert BASELINE_POOL_RELEASE == ('binaries/check_cohere_native_baseline',)
    for name in ('launch_native_workspace_execution_spot', 'check_native_workspace_execution'):
        ast.parse((base/'scripts'/(name+'.py')).read_text())
    subprocess.run(['bash', '-n', str(script)], check=True)
    print('PASS baseline pool source/command parsing only; native, lifecycle and resource qualification UNVERIFIED')


def retained_rebind_required_tests(required):
    """Fail-closed on any drift (and while empty): both rosters, the two existing publisher names plus exactly two new ones, nothing shared."""
    assert required == RETAINED_REBIND_REQUIRED_TESTS and set(required) == set(RETAINED_REBIND_TEST_SOURCES) and all(required.values()), 'retained rebind mandatory test names pending'
    integration, publisher = (required[name] for name in RETAINED_REBIND_TEST_SOURCES)
    old = _COHORT_PROTOCOL[2]['cohere-native-publisher-tests']
    assert all(len(set(tests)) == len(tests) for tests in required.values()), 'unique retained rebind test names'  # the stage name regexes below are disjoint, so no name can be shared
    assert len(integration) >= 2 and all(type(name) is str and re.fullmatch(r'retained_semantic_[a-zA-Z0-9_]+', name) for name in integration), 'retained integration test names'
    assert len(publisher) == len(old) + 2 and set(old) <= set(publisher) and all(type(name) is str and re.fullmatch(r'tests::[a-zA-Z0-9_]+', name) for name in publisher), 'existing publisher tests plus exactly two new retained tests'
    return required


def validate_retained_rebind_sources(base, mandatory_tests):
    """A misspelt roster name would only fail after the paid run: each must be a real fn in its frozen owning source.

    ponytail: a regex, not a Rust parser, so a commented-out `fn name(` still passes; the remote exact-roster stage check fails closed.
    """
    for stage, source in RETAINED_REBIND_TEST_SOURCES.items():
        text = (Path(base)/source).read_text()
        for name in mandatory_tests[stage]:
            assert re.search(r'\bfn ' + re.escape(name.removeprefix('tests::')) + r'\s*\(', text), 'roster test is not a function in its frozen source: ' + name


def probe_lines(body):
    """Verbatim probe JSON lines from the raw stage log; exact fixed shapes and parity flags, no timing interpretation."""
    lines = [line for line in body.splitlines(keepends=True) if line.startswith(b'{') and b'"candidate_over_baseline_median"' in line]
    parsed = [json.loads(line) for line in lines]
    assert {(row['dimensions'], row['rows']) for row in parsed} == EXACT_SQ8_PROBE_SHAPES and len(parsed) == len(EXACT_SQ8_PROBE_SHAPES), 'exact probe shapes'
    assert all(row['identical_ids_ordinals_score_bits'] is True for row in parsed), 'probe scalar/baseline/candidate parity'
    return b''.join(lines if all(line.endswith(b'\n') for line in lines) else [line.rstrip(b'\n') + b'\n' for line in lines])


COHERE_SQ8_BUILDER_STAGE_SCHEMA = 'borsuk-cohere-sq8-builder-stage-v1'
COHERE_SQ8_BUILDER_STAGES = (('release',
    'cargo build --release --locked -p borsuk --example build_sq8_source'.split()),)


def validate_cohere_sq8_builder_config(config):
    assert config['mandatory_test_names_pending'] is False, 'build-only roster frozen'
    assert type(config['mandatory_tests']) is dict and config['mandatory_tests'] == {}, 'build-only mode has no mandatory tests'
    if 'candidate_delta_paths' in config:
        validate_candidate_delta(config['candidate_delta_paths'])


def completed_native_qualification(base, authority, *, retained=False):
    """Authenticate the root's completed six-gate receipt for this native map."""
    pointer = authority['completed_native_qualification']
    assert type(pointer) is dict and set(pointer) == {'path', 'bytes', 'sha256'}, 'completed native qualification descriptor'
    path = Path(pointer['path'])
    assert not path.is_absolute() and '..' not in path.parts and str(path) == pointer['path'], 'qualification relative path'
    assert type(pointer['bytes']) is int and 0 < pointer['bytes'] <= 65536
    assert type(pointer['sha256']) is str and re.fullmatch('[0-9a-f]{64}', pointer['sha256'])
    assert pointer['path'] not in set(authority['source_sha256']) | set(CODE) | {str(CONFIG), authority['native_source_manifest']['path']}, 'distinct qualification body'
    name = 'completed-native-qualification.json' if retained else pointer['path']
    source = Path(base)
    for part in Path(name).parts:
        source /= part
        assert not source.is_symlink(), 'qualification symlink'
    assert source.is_file(), 'completed native qualification missing'
    body, receipt = files.metadata(source, pointer['sha256'], length=pointer['bytes'])
    assert type(receipt) is dict and {'status', 'source_file_count', 'native_source_identity_sha256',
        'full_source_before_after_root_match', 'terminated', 'stages'} <= set(receipt), 'completed native qualification fields'
    assert receipt['status'] == 'NATIVE_QUALIFIED', 'native qualification incomplete'
    assert receipt['full_source_before_after_root_match'] is receipt['terminated'] is True, 'native source/termination qualification incomplete'
    assert type(receipt['source_file_count']) is int and receipt['source_file_count'] == authority['source_file_count'] == len(authority['source_sha256'])
    assert receipt['native_source_identity_sha256'] == authority['source_identity_sha256'] == worker.source_identity(authority['source_sha256']), 'completed native qualification source drift'
    assert type(receipt['stages']) is list and all(type(stage) is dict and {'name', 'command', 'exit'} <= set(stage) for stage in receipt['stages']), 'completed native qualification stage fields'
    assert [(stage['name'], stage['command']) for stage in receipt['stages']] == list(COHERE1024_STAGES), 'exact completed native qualification gates'
    assert all(type(stage['exit']) is int and stage['exit'] == 0 for stage in receipt['stages']), 'native qualification gate failure'
    return body


def cohere_cohort_required_tests():
    required = COHERE_COHORT_REQUIRED_TESTS
    if BASELINE_POOL:
        assert required == BASELINE_POOL_REQUIRED_TESTS, 'baseline pool mandatory test roster'
        assert set(required) == set(BASELINE_POOL_TEST_COUNTS), 'baseline pool owning stages'
        return required
    if RETAINED_GENERATION_REBIND:
        return retained_rebind_required_tests(required)
    if EXACT_SQ8_RUNTIME:
        assert required == EXACT_SQ8_RUNTIME_REQUIRED_TESTS and set(required) == {'exact-sq8-scorer-tests', 'exact-sq8-scorer-release-tests'}, 'two scorer test rosters required'
        assert all(len(tests) == 7 == len(set(tests)) for tests in required.values()), 'seven unique scorer tests per stage'
        assert all(type(name) is str and re.fullmatch(r'exact_sq8_nominee::tests::[a-zA-Z0-9_]+', name) for tests in required.values() for name in tests), 'exact scorer test names'
        return required
    assert set(required) == {'cohere-native-publisher-tests', 'cohere-native-baseline-tests', 'cohere-native-cohort-tests'}, 'three cohort test rosters required'
    names = [test for tests in required.values() for test in tests]
    assert len(required['cohere-native-publisher-tests']) == 2 and all(len(tests) == 8 for name, tests in required.items() if name != 'cohere-native-publisher-tests') and len(set(names)) == 18, 'two publisher plus eight unique tests per old cohort bin'
    assert all(type(name) is str and re.fullmatch(r'tests::[a-zA-Z0-9_]+', name) for name in names), 'exact cohort bin test names'
    return required


def validate_cohere_cohort_config(config):
    assert config['mandatory_test_names_pending'] is False, 'cohort mandatory tests pending'
    assert config['mandatory_tests'] == {name:list(tests) for name,tests in cohere_cohort_required_tests().items()}, 'exact cohort test roster'


def cohere1024_required_tests():
    assert COHERE1024_REQUIRED_TESTS == {'generation-tests': (
        'two_bit_generation::source_walk_tests::native_100k_d1024_generation_serving_scalar_oracle',),
        'generation-release-test': (
        'two_bit_generation::source_walk_tests::native_100k_d1024_generation_serving_scalar_oracle',)}, 'exact D1024 debug/release mandatory test'
    return COHERE1024_REQUIRED_TESTS


def validate_cohere1024_config(config):
    assert config['mandatory_test_names_pending'] is False, 'D1024 mandatory test pending'
    assert config['mandatory_tests'] == {name:list(tests) for name, tests in cohere1024_required_tests().items()}, 'exact D1024 test roster'


def budget_object_fitter_required_tests():
    required = BUDGET_OBJECT_FITTER_REQUIRED_TESTS
    assert set(required) == {'budget-object-fitter-tests'} and required['budget-object-fitter-tests'], 'native fitter mandatory test names pending'
    names = required['budget-object-fitter-tests']
    assert len(names) == len(set(names)) and all(type(name) is str and re.fullmatch(r'budget_object_(?:selector|fitter)::tests::[a-zA-Z0-9_]+', name) for name in names), 'exact unique native fitter tests'
    original = set(BUDGET_OBJECT_SELECTOR_REQUIRED_TESTS['budget-object-selector-tests'])
    assert original < set(names) and any(name.startswith('budget_object_fitter::tests::') for name in names), 'original selector and additional fitter tests required'
    return required


def validate_budget_object_fitter_config(config):
    assert config['mandatory_test_names_pending'] is False, 'native fitter mandatory test names pending'
    assert config['mandatory_tests'] == {name:list(tests) for name,tests in budget_object_fitter_required_tests().items()}, 'exact native fitter test roster'


def budget_object_selector_required_tests():
    required = BUDGET_OBJECT_SELECTOR_REQUIRED_TESTS
    assert set(required) == {'budget-object-selector-tests'} and required['budget-object-selector-tests'], 'native selector mandatory test names pending'
    names = required['budget-object-selector-tests']
    assert len(names) == len(set(names)) and all(type(name) is str and re.fullmatch(r'budget_object_selector::tests::[a-zA-Z0-9_]+', name) for name in names), 'exact unique native selector tests'
    return required


def validate_budget_object_selector_config(config):
    assert config['mandatory_test_names_pending'] is False, 'native selector mandatory test names pending'
    assert config['mandatory_tests'] == {name:list(tests) for name,tests in budget_object_selector_required_tests().items()}, 'exact native selector test roster'


def co_selection_required_tests():
    assert set(CO_SELECTION_REQUIRED_TESTS) == {'co-selection-tests', 'co-selection-bin-tests'}, 'native co-selection mandatory test names pending'
    required = dict(CO_SELECTION_REQUIRED_TESTS, **pq_residual_required_tests())
    assert set(required) == {name for name, _ in CO_SELECTION_STAGES if name not in ('release', 'clippy', 'test-build')} and all(required.values()), 'complete co-selection test roster'
    names = [test for tests in required.values() for test in tests]
    assert len(names) == len(set(names)), 'exact unique native co-selection tests'
    for stage, tests in CO_SELECTION_REQUIRED_TESTS.items():
        prefix = 'co_selection_layout::tests::' if stage == 'co-selection-tests' else 'tests::co_selection_'
        assert all(type(name) is str and name.startswith(prefix) and re.fullmatch(r'[a-zA-Z0-9_]+(?:::[a-zA-Z0-9_]+)+', name) for name in tests), 'exact native co-selection test names'
    return required


def validate_co_selection_config(config):
    assert {'mandatory_test_names_pending', 'mandatory_tests'} <= set(config), 'complete co-selection test authority'
    assert config['mandatory_test_names_pending'] is False, 'native mandatory test names pending'
    assert config['mandatory_tests'] == {name:list(tests) for name,tests in co_selection_required_tests().items()}, 'exact native co-selection test roster'


def pq_residual_required_tests():
    required = PQ_RESIDUAL_REQUIRED_TESTS
    assert set(required) == {name for name, _ in PQ_RESIDUAL_STAGES if name not in ('release', 'clippy', 'test-build')} and all(required.values()), 'native PQ residual mandatory test names pending'
    names = [test for tests in required.values() for test in tests]
    assert len(names) == len(set(names)), 'exact unique native PQ residual tests'
    for stage, tests in required.items():
        pattern = r'src/pq_residual_four_bit\.rs - pq_residual_four_bit::Axis \(line [1-9][0-9]*\) - compile fail' if stage == 'pq-residual-doc-tests' else r'[a-zA-Z0-9_]+(?:::[a-zA-Z0-9_]+)+'
        assert all(type(name) is str and re.fullmatch(pattern, name) for name in tests), 'exact native PQ residual test names'
    return required


def validate_pq_residual_config(config):
    assert {'mandatory_test_names_pending', 'mandatory_tests'} <= set(config), 'complete PQ residual test authority'
    assert config['mandatory_test_names_pending'] is False, 'native mandatory test names pending'
    assert config['mandatory_tests'] == {name:list(tests) for name,tests in pq_residual_required_tests().items()}, 'exact native PQ residual test roster'


def fine_sq8_required_tests():
    required = FINE_SQ8_REQUIRED_TESTS
    assert set(required) == {name for name, _ in FINE_SQ8_STAGES if name not in ('release', 'clippy', 'test-build')} and all(required.values()), 'native fine SQ8 mandatory test names pending'
    names = [test for tests in required.values() for test in tests]
    assert len(names) == len(set(names)) and all(type(name) is str and re.fullmatch(r'[a-zA-Z0-9_]+(?:::[a-zA-Z0-9_]+)+', name) for name in names), 'exact unique native fine SQ8 tests'
    return required


def validate_fine_sq8_config(config):
    assert {'mandatory_test_names_pending', 'mandatory_tests'} <= set(config), 'complete fine SQ8 test authority'
    assert config['mandatory_test_names_pending'] is False, 'native mandatory test names pending'
    assert config['mandatory_tests'] == {name:list(tests) for name,tests in fine_sq8_required_tests().items()}, 'exact native fine SQ8 test roster'


def cell_overlap_required_tests():
    names = [test for tests in CELL_OVERLAP_REQUIRED_TESTS.values() for test in tests]
    assert names and len(names) == len(set(names)) and all(re.fullmatch(r'[a-zA-Z0-9_]+(?:::[a-zA-Z0-9_]+)+', name) for name in names), 'exact unique native overlap tests'
    return CELL_OVERLAP_REQUIRED_TESTS


def constrained_split_required_tests():
    assert CONSTRAINED_SPLIT_ADDITIVE_TESTS and CONSTRAINED_SPLIT_BIN_TESTS, 'native mandatory test names pending'
    required = {'hierarchical-cell-tests': (*HIERARCHICAL_CELLS_REQUIRED_TESTS['hierarchical-cell-tests'],
                                          *CONSTRAINED_SPLIT_ADDITIVE_TESTS),
                'split-balance-bin-tests': CONSTRAINED_SPLIT_BIN_TESTS}
    names = [name for group in required.values() for name in group]
    assert len(names) == len(set(names)) and all(type(name) is str and re.fullmatch(r'[a-zA-Z0-9_]+(?:::[a-zA-Z0-9_]+)+', name) for name in names), 'exact native test names'
    return required


# Historical source fixture only; production authority is the root-frozen manifest.
FIXED48_CHECK_COMMIT = '5efb136e95956b0cea0aa38214299a42f4264684'
FIXED48_CHECK_CONTROL = 'f86ee6a80ace374d6674c9b65ea9141fc4a7de36'
FIXED48_DELTA = ('crates/borsuk/src/bin/check_semantic_router_scorer.rs',
                 'crates/borsuk/src/semantic_unit_router.rs',
                 'crates/borsuk/src/two_bit_generation.rs')
FIXED48_STAGE_SCHEMA = 'borsuk-fixed48-implementation-stage-v1'
FIXED48_REQUIRED_TESTS = {
    'semantic-unit-router-tests': (
        'semantic_unit_router::tests::fresh48_seed_completion_retains_all_3072_units_and_512_pages',
        'semantic_unit_router::tests::fresh_binary_root_selected_leaves_and_scattered_closure_without_training'),
    'source-walk-tests': (
        'two_bit_generation::source_walk_tests::semantic_object_store_parity',
        'two_bit_generation::source_walk_tests::fresh48_source_walk_completes_512_pages_once_and_rejects_excess_before_io'),
    'semantic-router-scorer-tests': (
        'tests::truth_free_v3_config_rejects_truth_unknown_fields_and_old_schema',
        'tests::frozen_marker_requires_complete64_and_binds_measurement_identity')}
FIXED48_STAGES = tuple((name, command.split()) for name, command in (
    ('semantic-unit-router-tests', 'cargo test --locked -p borsuk --lib semantic_unit_router::'),
    ('source-walk-tests', 'cargo test --locked -p borsuk --lib two_bit_generation::source_walk_tests::'),
    ('semantic-router-scorer-tests', 'cargo test --locked -p borsuk --bin check_semantic_router_scorer'),
    ('generation-integration', 'cargo test --locked -p borsuk --test two_bit_generation'),
    ('release', 'cargo build --locked -p borsuk --release --example two_bit_http --bin check_semantic_router_scorer --bin two_bit_plan_demo'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND bash scripts/check_rust_test_build.sh')))
BOUNDED_PUBLICATION_CHECK_COMMIT = 'da56bcb080f559c81886e4e603226bd5e5415564'
BOUNDED_PUBLICATION_CHECK_CONTROL = '68294a2e67a298006ac79f4dfc96e76e1f7a4ddf'
BOUNDED_PUBLICATION_CHECK_IDENTITY = 'e8c0e6b3d42b5e73ae6faa558c5337b8d25bc8b444d660704256e4c1ec0d1f74'
BOUNDED_PUBLICATION_DELTA = ('crates/borsuk/src/two_bit_generation.rs',
                             'crates/borsuk/src/two_bit_source.rs',
                             'crates/borsuk/src/two_bit_store.rs')
BOUNDED_PUBLICATION_STAGE_SCHEMA = 'borsuk-bounded-publication-implementation-stage-v1'
BOUNDED_PUBLICATION_CAP_TEST = 'two_bit_store::root_seed_tests::publication_reserves_metadata_before_upload_buffers'
BOUNDED_PUBLICATION_STAGES = tuple((name, command.split()) for name, command in (
    ('semantic-object-store-parity', 'cargo test --locked -p borsuk --lib two_bit_generation::source_walk_tests::semantic_object_store_parity -- --exact'),
    ('two-bit-lib-tests', 'cargo test --locked -p borsuk --lib two_bit_ -- --skip two_bit_generation::source_walk_tests::semantic_object_store_parity'),
    ('sq8-s3-range', 'cargo test --locked -p borsuk --lib sq8_s3_range::'),
    ('generation-integration', 'cargo test --locked -p borsuk --test two_bit_generation'),
    ('release', 'cargo build --locked -p borsuk --release --example two_bit_http'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND bash scripts/check_rust_test_build.sh')))
STARTUP_WAVE8_COMMIT = '1e4ed13777a59e7acdd34acc9510b9d156aa17bf'
STARTUP_WAVE8_IDENTITY = '7e4fabf96284e4ccd080cf14b8bfbfd0f3ff41271e920c8e39d5d1fa252cd830'
STARTUP_WAVE8_DELTA = ('crates/borsuk/src/object_native_generation.rs',
                      'crates/borsuk/src/two_bit_generation.rs')
ROOT_REUSE_DELTA = ('crates/borsuk/examples/two_bit_http.rs',
                    'crates/borsuk/src/object_native_generation.rs',
                    'crates/borsuk/src/two_bit_generation.rs',
                    'crates/borsuk/src/two_bit_index.rs',
                    'crates/borsuk/src/two_bit_store.rs')
NATIVE_DELTA = STARTUP_WAVE8_DELTA
RECEIPT_SCHEMA = 'borsuk-native-workspace-execution-receipt-v1'
WALL = 9000
INSTANCE_TYPE, IMAGE_ID = 'c7i.2xlarge', semantic.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = semantic.ROOT_DEVICE_NAME, 'subnet-034528fbd6977848f'
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, 1.25
MODULE = 'scripts.launch_native_workspace_execution_spot'
CODE = tuple('scripts/' + name + '.py' for name in (
    'check_native_metadata_ranges_stats', 'check_native_semantic_router_stats',
    'check_native_startup_build', 'check_native_startup_stats', 'check_native_workspace_execution',
    'launch_native_metadata_ranges_cold_spot', 'launch_native_peer_1m_spot',
    'launch_native_semantic_router_cold_spot', 'launch_native_startup_profile_spot',
    'launch_native_workspace_execution_spot', 'launch_v157_primary_feasibility_spot',
    'launch_v174_relaid_bind_compile_spot', 'package_semantic_native_generation',
    'prepare_native_semantic_publication', 'rest_coexistence_load', 'run_native_cold_first_query',
    'run_native_metadata_ranges_cold', 'run_native_peer_1m_worker', 'run_native_peer_offered_http',
    'run_native_semantic_router_cold', 'run_native_union_http', 'run_native_union_offered_http'))
ARTIFACTS = ('source-qualification.json', 'config.json', 'native-source-manifest.json',
    'source-before.json', 'source-after.json', 'workspace-receipt.json', 'test.log',
    'test-resources.txt', 'workspace-cgroup.json', 'cpu.txt', 'rustc-version.txt',
    'cargo-version.txt', 'run-closed.log')
FULL_ARTIFACTS = ARTIFACTS
RELEASE_ARTIFACTS = tuple('binaries/'+name for name in (
    'two_bit_http', 'build_two_bit_generation', 'build_semantic_unit_router',
    'repackage_semantic_generation', 'check_semantic_router_scorer'))
FULL_RELEASE_ARTIFACTS = RELEASE_ARTIFACTS
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256', 'campaign_schema',
    'source_identity_sha256', 'source_file_count', 'native_source_manifest_sha256',
    'native_source_commit', 'artifact_roster_sha256', 'awscli_version', 'awscli_sha256')
FULL_TERMINAL_IDENTITIES = TERMINAL_IDENTITIES
ARCHIVE_FIELDS = ('source_archive_paths', 'source_archive_paths_sha256',
                  'source_archive_file_count', 'source_archive_support_sha256')
ARCHIVE_IDENTITIES = ('source_archive_paths_sha256', 'source_archive_file_count')
ARCHIVE_GIT = ('git', '-c', 'core.packedGitWindowSize=16m', '-c', 'core.packedGitLimit=32m')
FIXED = dict(schema=CONFIG_SCHEMA, architecture='x86_64', region=peer.REGION, bucket=peer.BUCKET,
    instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, root_device_name=ROOT_DEVICE_NAME,
    subnet_id=SUBNET, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR, compute_cap_usd=COMPUTE_CAP,
    ebs_s3_allowance_usd=.15, memory_bytes=worker.MEMORY, swap_bytes=0, cpu_quota_percent=200,
    tasks_max=512, test_limit_seconds=worker.TEST_SECONDS, service_limit_seconds=worker.SERVICE_SECONDS,
    machine_limit_seconds=WALL, command=list(worker.COMMAND), environment=worker.ENVIRONMENT)
FULL_CODE, FULL_FIXED = CODE, FIXED


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def configure(semantic_1m=False, *, test_build=False, implementation=False, startup_wave8=False, root_reuse=False, bounded_publication=False, fixed48=False, hierarchical_cells=False, constrained_split=False, cell_overlap=False, fine_sq8=False, pq_residual=False, co_selection=False, budget_object_selector=False, budget_object_fitter=False, cohere1024=False, cohere_cohort=False, cohere_sq8_builder=False, exact_sq8_runtime=False, retained_generation_rebind=False, baseline_pool=False):
    """Select the protocol explicitly in every controller/worker process."""
    global SEMANTIC_1M, TEST_BUILD, IMPLEMENTATION, STARTUP_WAVE8, ROOT_REUSE, BOUNDED_PUBLICATION, FIXED48, HIERARCHICAL_CELLS, CONSTRAINED_SPLIT, CELL_OVERLAP, FINE_SQ8, PQ_RESIDUAL, CO_SELECTION, MINIMAL_ARCHIVE, NATIVE_DELTA, ROOT, CONFIG, PREFIX, TOKEN_PREFIX, TAG
    global SCHEMA, CONFIG_SCHEMA, RECEIPT_SCHEMA, CODE, FIXED, ARTIFACTS, RELEASE_ARTIFACTS, TERMINAL_IDENTITIES, SUBNET
    global BUDGET_OBJECT_SELECTOR, BUDGET_OBJECT_FITTER, COHERE1024, COHERE_COHORT, COHERE_SQ8_BUILDER, EXACT_SQ8_RUNTIME, RETAINED_GENERATION_REBIND, BASELINE_POOL
    global COHERE_COHORT_STAGES, COHERE_COHORT_STAGE_SCHEMA, COHERE_COHORT_REQUIRED_TESTS, _COHORT_REBOUND
    assert type(budget_object_selector) is type(budget_object_fitter) is type(cohere1024) is type(cohere_cohort) is type(cohere_sq8_builder) is type(exact_sq8_runtime) is type(retained_generation_rebind) is type(baseline_pool) is bool
    assert type(semantic_1m) is type(test_build) is type(implementation) is type(startup_wave8) is type(root_reuse) is type(bounded_publication) is type(fixed48) is type(hierarchical_cells) is type(constrained_split) is type(cell_overlap) is type(fine_sq8) is type(pq_residual) is type(co_selection) is bool
    scoped = startup_wave8 or root_reuse or bounded_publication or fixed48 or hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder or exact_sq8_runtime or retained_generation_rebind or baseline_pool
    assert sum((startup_wave8, root_reuse, bounded_publication, fixed48, hierarchical_cells, constrained_split, cell_overlap, fine_sq8, pq_residual, co_selection, budget_object_selector, budget_object_fitter, cohere1024, cohere_cohort, cohere_sq8_builder, exact_sq8_runtime, retained_generation_rebind, baseline_pool)) <= 1 and not (scoped and test_build), 'mutually exclusive execution modes'
    if scoped:
        semantic_1m = implementation = True
    assert not (test_build and implementation), 'mutually exclusive execution modes'
    # These modes reuse the cohort-family lifecycle with their own protocol data.
    EXACT_SQ8_RUNTIME = exact_sq8_runtime
    RETAINED_GENERATION_REBIND = retained_generation_rebind
    BASELINE_POOL = baseline_pool
    SUBNET = RETAINED_REBIND_SUBNET if retained_generation_rebind else FULL_FIXED['subnet_id']
    if exact_sq8_runtime or retained_generation_rebind or baseline_pool:
        cohere_cohort = True
        COHERE_COHORT_STAGES, COHERE_COHORT_STAGE_SCHEMA, COHERE_COHORT_REQUIRED_TESTS = (
            (BASELINE_POOL_STAGES, BASELINE_POOL_STAGE_SCHEMA, BASELINE_POOL_REQUIRED_TESTS) if baseline_pool else
            (RETAINED_REBIND_STAGES, RETAINED_REBIND_STAGE_SCHEMA, RETAINED_REBIND_REQUIRED_TESTS) if retained_generation_rebind else
            (EXACT_SQ8_RUNTIME_STAGES, EXACT_SQ8_RUNTIME_STAGE_SCHEMA, EXACT_SQ8_RUNTIME_REQUIRED_TESTS))
        _COHORT_REBOUND = True
    elif _COHORT_REBOUND:
        COHERE_COHORT_STAGES, COHERE_COHORT_STAGE_SCHEMA, COHERE_COHORT_REQUIRED_TESTS = _COHORT_PROTOCOL
        _COHORT_REBOUND = False
    assert not (test_build or implementation) or semantic_1m, 'script requires explicit semantic-1m mode'
    SEMANTIC_1M = semantic_1m
    TEST_BUILD = test_build
    IMPLEMENTATION = implementation
    STARTUP_WAVE8 = startup_wave8
    ROOT_REUSE = root_reuse
    BOUNDED_PUBLICATION = bounded_publication
    FIXED48 = fixed48
    HIERARCHICAL_CELLS = hierarchical_cells
    CONSTRAINED_SPLIT = constrained_split
    CELL_OVERLAP = cell_overlap
    FINE_SQ8 = fine_sq8
    PQ_RESIDUAL = pq_residual
    CO_SELECTION = co_selection
    BUDGET_OBJECT_SELECTOR = budget_object_selector
    BUDGET_OBJECT_FITTER = budget_object_fitter
    COHERE1024 = cohere1024
    COHERE_COHORT = cohere_cohort
    COHERE_SQ8_BUILDER = cohere_sq8_builder
    MINIMAL_ARCHIVE = hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder
    NATIVE_DELTA = BASELINE_POOL_DELTA if baseline_pool else RETAINED_REBIND_DELTA if retained_generation_rebind else EXACT_SQ8_RUNTIME_DELTA if exact_sq8_runtime else () if cohere_sq8_builder else COHERE_COHORT_DELTA if cohere_cohort else COHERE1024_DELTA if cohere1024 else BUDGET_OBJECT_FITTER_DELTA if budget_object_fitter else BUDGET_OBJECT_SELECTOR_DELTA if budget_object_selector else CO_SELECTION_DELTA if co_selection else PQ_RESIDUAL_DELTA if pq_residual else FINE_SQ8_DELTA if fine_sq8 else CELL_OVERLAP_DELTA if cell_overlap else CONSTRAINED_SPLIT_DELTA if constrained_split else HIERARCHICAL_CELLS_DELTA if hierarchical_cells else FIXED48_DELTA if fixed48 else BOUNDED_PUBLICATION_DELTA if bounded_publication else ROOT_REUSE_DELTA if root_reuse else STARTUP_WAVE8_DELTA
    RELEASE_ARTIFACTS = BASELINE_POOL_RELEASE if baseline_pool else RETAINED_REBIND_RELEASE if retained_generation_rebind else EXACT_SQ8_RUNTIME_RELEASE if exact_sq8_runtime else ('binaries/build_sq8_source',) if cohere_sq8_builder else ('binaries/check_cohere_native_baseline', 'binaries/prepare_cohere_native_cohort', 'binaries/publish_two_bit_generation') if cohere_cohort else FULL_RELEASE_ARTIFACTS if cohere1024 else ('binaries/hierarchical_semantic_cells',) if cell_overlap or fine_sq8 or pq_residual or co_selection or budget_object_selector or budget_object_fitter else ('binaries/check_hierarchical_split_balance', 'binaries/hierarchical_semantic_cells', 'binaries/two_bit_http') if constrained_split else ('binaries/hierarchical_semantic_cells', 'binaries/two_bit_http', 'binaries/build_two_bit_generation', 'binaries/check_semantic_router_scorer') if hierarchical_cells else ('binaries/two_bit_http', 'binaries/check_semantic_router_scorer', 'binaries/two_bit_plan_demo') if fixed48 else ('binaries/two_bit_http',) if scoped else FULL_RELEASE_ARTIFACTS
    TERMINAL_IDENTITIES = (*FULL_TERMINAL_IDENTITIES, 'controller_source_commit', 'candidate_delta_paths') if scoped else FULL_TERMINAL_IDENTITIES
    if MINIMAL_ARCHIVE:
        TERMINAL_IDENTITIES += ARCHIVE_IDENTITIES
    ARTIFACTS = (*FULL_ARTIFACTS, *RELEASE_ARTIFACTS, *((EXACT_SQ8_PROBE_ARTIFACT,) if exact_sq8_runtime else ())) if implementation else FULL_ARTIFACTS
    ROOT = (semantic.ROOT.parent/'semantic-1m' if semantic_1m else semantic.ROOT/'metadata-waves') / ('implementation-gates/remote-implementation' if implementation else 'implementation-gates/remote-test-build' if test_build else 'implementation-gates/remote-full')
    CONFIG = ROOT/'config.json'
    TOKEN_PREFIX = ('semantic-1m-implementation-' if implementation else 'semantic-1m-test-build-' if test_build else
                    ('semantic-1m' if semantic_1m else 'metadata-waves') + '-workspace-')
    PREFIX = 'research/semantic-router/20261001/' + TOKEN_PREFIX
    TAG = 'borsuk-' + TOKEN_PREFIX.rstrip('-')
    SCHEMA = 'borsuk-native-workspace-test-build-spot-v1' if test_build else 'borsuk-native-workspace-execution-spot-v1'
    CONFIG_SCHEMA = 'borsuk-native-workspace-test-build-v1' if test_build else FULL_FIXED['schema']
    RECEIPT_SCHEMA = 'borsuk-native-workspace-test-build-receipt-v1' if test_build else 'borsuk-native-workspace-execution-receipt-v1'
    CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh') if test_build else FULL_CODE
    FIXED = dict(FULL_FIXED, schema=CONFIG_SCHEMA)
    if test_build or implementation:
        FIXED.update(execution_kind='implementation-gates' if implementation else 'workspace-test-build',
            command=['bash', 'scripts/check_semantic_1m_implementation.sh' if implementation else 'scripts/check_rust_test_build.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None))
    if implementation:
        SCHEMA = 'borsuk-semantic-1m-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-semantic-1m-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-semantic-1m-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_semantic_1m_implementation.sh')
        FIXED['schema'] = CONFIG_SCHEMA
    if startup_wave8:
        ROOT = semantic.ROOT.parent/'semantic-1m/startup-wave8/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'startup-wave8-implementation-'
        PREFIX = 'research/semantic-router/20261002/' + TOKEN_PREFIX
        TAG = 'borsuk-startup-wave8-implementation'
        SCHEMA = 'borsuk-startup-wave8-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-startup-wave8-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-startup-wave8-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_startup_wave8_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_startup_wave8_implementation.sh'])
    if root_reuse:
        ROOT = semantic.ROOT.parent/'semantic-1m/startup-wave8/root-reuse/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'root-reuse-implementation-'
        PREFIX = 'research/semantic-router/20261002/' + TOKEN_PREFIX
        TAG = 'borsuk-root-reuse-implementation'
        SCHEMA = 'borsuk-root-reuse-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-root-reuse-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-root-reuse-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_root_reuse_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_root_reuse_implementation.sh'])
    if bounded_publication:
        ROOT = semantic.ROOT.parent/'semantic-1m/bounded-publication/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'bounded-publication-implementation-'
        PREFIX = 'research/semantic-router/20261002/' + TOKEN_PREFIX
        TAG = 'borsuk-bounded-publication-implementation'
        SCHEMA = 'borsuk-bounded-publication-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-bounded-publication-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-bounded-publication-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_bounded_publication_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_bounded_publication_implementation.sh'])
    if fixed48:
        ROOT = semantic.ROOT.parent/'semantic-1m/fixed48/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'fixed48-implementation-'
        PREFIX = 'research/semantic-router/20261002/' + TOKEN_PREFIX
        TAG = 'borsuk-fixed48-implementation'
        SCHEMA = 'borsuk-fixed48-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-fixed48-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-fixed48-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_fixed48_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_fixed48_implementation.sh'])
    if hierarchical_cells:
        ROOT = semantic.ROOT.parent/'semantic-1m/hierarchical-cells/source-witness-router/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'source-witness-router-implementation-'
        PREFIX = 'research/semantic-router/20261004/' + TOKEN_PREFIX
        TAG = 'borsuk-source-witness-router-implementation'
        SCHEMA = 'borsuk-hierarchical-cells-implementation-gates-spot-v2'
        CONFIG_SCHEMA = 'borsuk-hierarchical-cells-implementation-gates-v2'
        RECEIPT_SCHEMA = 'borsuk-hierarchical-cells-implementation-gates-receipt-v2'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_hierarchical_cells_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_hierarchical_cells_implementation.sh'])

    if constrained_split:
        ROOT = semantic.ROOT.parent/'semantic-1m/hierarchical-cells/constrained-split/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'constrained-split-implementation-'
        PREFIX = 'research/semantic-router/20261003/' + TOKEN_PREFIX
        TAG = 'borsuk-constrained-split-implementation'
        SCHEMA = 'borsuk-constrained-split-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-constrained-split-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-constrained-split-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_constrained_split_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_constrained_split_implementation.sh'],
            mandatory_test_names_pending=not (CONSTRAINED_SPLIT_ADDITIVE_TESTS and CONSTRAINED_SPLIT_BIN_TESTS),
            mandatory_tests={name:list(tests) for name,tests in constrained_split_required_tests().items()}
                if CONSTRAINED_SPLIT_ADDITIVE_TESTS and CONSTRAINED_SPLIT_BIN_TESTS else {},
            control_native_source_commit=CONSTRAINED_SPLIT_CONTROL,
            control_module_prefix=CONSTRAINED_SPLIT_PREFIX)

    if cell_overlap:
        ROOT = semantic.ROOT.parent/'semantic-1m/hierarchical-cells/boundary-overlap/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'boundary-overlap-implementation-'
        PREFIX = 'research/semantic-router/20261005/' + TOKEN_PREFIX
        TAG = 'borsuk-boundary-overlap-implementation'
        SCHEMA = 'borsuk-cell-overlap-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-cell-overlap-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-cell-overlap-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_cell_overlap_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_cell_overlap_implementation.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None),
            mandatory_test_names_pending=False,
            mandatory_tests={name:list(tests) for name,tests in cell_overlap_required_tests().items()})


    if fine_sq8:
        ROOT = semantic.ROOT.parent/'semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'corrected-four-bit-implementation-'
        PREFIX = 'research/semantic-router/20261005/' + TOKEN_PREFIX
        TAG = 'borsuk-corrected-four-bit-implementation'
        SCHEMA = 'borsuk-corrected-four-bit-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-corrected-four-bit-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-corrected-four-bit-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_fine_sq8_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_fine_sq8_implementation.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None,
                BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1', TOKIO_WORKER_THREADS='1',
                BORSUK_FINE_TEST_ROOT=None, BORSUK_FINE_TEST_REMOTE=None),
            mandatory_test_names_pending=not bool(FINE_SQ8_REQUIRED_TESTS),
            mandatory_tests={name:list(tests) for name,tests in fine_sq8_required_tests().items()}
                if FINE_SQ8_REQUIRED_TESTS else {})


    if pq_residual:
        ROOT = semantic.ROOT.parent/'semantic-1m/fine-sq8-groups/sq4-refinement/pq-residual/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'pq-residual-implementation-'
        PREFIX = 'research/semantic-router/20261006/' + TOKEN_PREFIX
        TAG = 'borsuk-pq-residual-implementation'
        SCHEMA = 'borsuk-pq-residual-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-pq-residual-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-pq-residual-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_pq_residual_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_pq_residual_implementation.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None,
                BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1', TOKIO_WORKER_THREADS='1',
                BORSUK_FINE_TEST_ROOT=None, BORSUK_FINE_TEST_REMOTE=None),
            mandatory_test_names_pending=not bool(PQ_RESIDUAL_REQUIRED_TESTS),
            mandatory_tests={name:list(tests) for name,tests in pq_residual_required_tests().items()}
                if PQ_RESIDUAL_REQUIRED_TESTS else {})


    if co_selection:
        ROOT = semantic.ROOT.parent/'semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'co-selection-implementation-'
        PREFIX = 'research/semantic-router/20261006/' + TOKEN_PREFIX
        TAG = 'borsuk-co-selection-implementation'
        SCHEMA = 'borsuk-co-selection-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-co-selection-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-co-selection-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_co_selection_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_co_selection_implementation.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None,
                BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1', TOKIO_WORKER_THREADS='1',
                BORSUK_FINE_TEST_ROOT=None, BORSUK_FINE_TEST_REMOTE=None),
            mandatory_test_names_pending=not bool(CO_SELECTION_REQUIRED_TESTS),
            mandatory_tests={name:list(tests) for name,tests in co_selection_required_tests().items()}
                if CO_SELECTION_REQUIRED_TESTS else {})


    if budget_object_selector:
        ROOT = semantic.ROOT.parent/'semantic-1m/fine-sq8-groups/budget-object-selector/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'budget-object-selector-implementation-'
        PREFIX = 'research/semantic-router/20261006/' + TOKEN_PREFIX
        TAG = 'borsuk-budget-object-selector-implementation'
        SCHEMA = 'borsuk-budget-object-selector-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-budget-object-selector-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-budget-object-selector-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_budget_object_selector_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_budget_object_selector_implementation.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None,
                BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1', TOKIO_WORKER_THREADS='1',
                BORSUK_FINE_TEST_ROOT=None, BORSUK_FINE_TEST_REMOTE=None, LC_ALL='C'),
            mandatory_test_names_pending=not bool(BUDGET_OBJECT_SELECTOR_REQUIRED_TESTS),
            mandatory_tests={name:list(tests) for name,tests in budget_object_selector_required_tests().items()}
                if BUDGET_OBJECT_SELECTOR_REQUIRED_TESTS else {})


    if budget_object_fitter:
        ROOT = semantic.ROOT.parent/'semantic-1m/fine-sq8-groups/budget-object-selector/fitter-implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'budget-object-fitter-implementation-'
        PREFIX = 'research/semantic-router/20261006/' + TOKEN_PREFIX
        TAG = 'borsuk-budget-object-fitter-implementation'
        SCHEMA = 'borsuk-budget-object-fitter-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-budget-object-fitter-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-budget-object-fitter-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_budget_object_fitter_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_budget_object_fitter_implementation.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None,
                BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1', TOKIO_WORKER_THREADS='1',
                BORSUK_FINE_TEST_ROOT=None, BORSUK_FINE_TEST_REMOTE=None, LC_ALL='C'),
            mandatory_test_names_pending=not bool(BUDGET_OBJECT_FITTER_REQUIRED_TESTS),
            mandatory_tests={name:list(tests) for name,tests in budget_object_fitter_required_tests().items()}
                if BUDGET_OBJECT_FITTER_REQUIRED_TESTS else {})


    if cohere1024:
        ROOT = Path('docs/research/performance-architecture-20260930/cohere1024/implementation-gates')
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'cohere1024-implementation-'
        PREFIX = 'research/semantic-router/20261006/' + TOKEN_PREFIX
        TAG = 'borsuk-cohere1024-implementation'
        SCHEMA = 'borsuk-cohere1024-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-cohere1024-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-cohere1024-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_cohere1024_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_cohere1024_implementation.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None,
                BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1', TOKIO_WORKER_THREADS='1',
                BORSUK_FINE_TEST_ROOT=None, BORSUK_FINE_TEST_REMOTE=None, LC_ALL='C'),
            mandatory_test_names_pending=not bool(COHERE1024_REQUIRED_TESTS),
            mandatory_tests={name:list(tests) for name, tests in cohere1024_required_tests().items()}
                if COHERE1024_REQUIRED_TESTS else {})


    if cohere_cohort:
        ROOT = Path('docs/research/performance-architecture-20260930/cohere1024/cohort-implementation-gates')
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'cohere-cohort-implementation-'
        PREFIX = 'research/semantic-router/20261006/' + TOKEN_PREFIX
        TAG = 'borsuk-cohere-cohort-implementation'
        SCHEMA = 'borsuk-cohere-cohort-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-cohere-cohort-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-cohere-cohort-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_cohere_cohort_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_cohere_cohort_implementation.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None,
                BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1', TOKIO_WORKER_THREADS='1',
                BORSUK_FINE_TEST_ROOT=None, BORSUK_FINE_TEST_REMOTE=None, LC_ALL='C'),
            mandatory_test_names_pending=not bool(COHERE_COHORT_REQUIRED_TESTS),
            mandatory_tests={name:list(tests) for name,tests in cohere_cohort_required_tests().items()}
                if COHERE_COHORT_REQUIRED_TESTS else {})


    if exact_sq8_runtime:
        ROOT = Path('docs/research/performance-architecture-20260930/cohere1024/exact-sq8-runtime-gates')
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'exact-sq8-runtime-gates-'
        PREFIX = 'research/semantic-router/20261006/' + TOKEN_PREFIX
        TAG = 'borsuk-exact-sq8-runtime-gates'
        SCHEMA = 'borsuk-exact-sq8-runtime-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-exact-sq8-runtime-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-exact-sq8-runtime-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_exact_sq8_runtime.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_exact_sq8_runtime.sh'])


    if retained_generation_rebind:
        ROOT = Path('docs/research/performance-architecture-20260930/cohere1024/retained-generation-rebind/implementation-gates')
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'retained-generation-rebind-gates-'
        PREFIX = 'research/semantic-router/20261006/' + TOKEN_PREFIX
        TAG = 'borsuk-retained-generation-rebind-gates'
        SCHEMA = 'borsuk-retained-generation-rebind-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-retained-generation-rebind-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-retained-generation-rebind-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_retained_generation_rebind_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_retained_generation_rebind_implementation.sh'], subnet_id=SUBNET)


    if baseline_pool:
        ROOT = Path('docs/research/performance-architecture-20260930/cohere1024/baseline-pool/implementation-gates')
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'baseline-pool-implementation-gates-'
        PREFIX = 'research/semantic-router/20261010/' + TOKEN_PREFIX
        TAG = 'borsuk-baseline-pool-implementation-gates'
        SCHEMA = 'borsuk-baseline-pool-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-baseline-pool-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-baseline-pool-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_baseline_pool_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_baseline_pool_implementation.sh'])


    if cohere_sq8_builder:
        ROOT = Path('docs/research/performance-architecture-20260930/cohere1024/sq8-builder-qualification')
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'cohere-sq8-builder-'
        PREFIX = 'research/semantic-router/20261006/' + TOKEN_PREFIX
        TAG = 'borsuk-cohere-sq8-builder'
        SCHEMA = 'borsuk-cohere-sq8-builder-spot-v1'
        CONFIG_SCHEMA = 'borsuk-cohere-sq8-builder-v1'
        RECEIPT_SCHEMA = 'borsuk-cohere-sq8-builder-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_cohere_sq8_builder.sh')
        ARTIFACTS = (*FULL_ARTIFACTS, 'completed-native-qualification.json', *RELEASE_ARTIFACTS)
        FIXED.update(schema=CONFIG_SCHEMA, execution_kind='cohere-sq8-builder',
            command=['bash', 'scripts/check_cohere_sq8_builder.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_COMMAND=None,
                BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1', TOKIO_WORKER_THREADS='1',
                BORSUK_FINE_TEST_ROOT=None, BORSUK_FINE_TEST_REMOTE=None, LC_ALL='C'),
            mandatory_test_names_pending=False, mandatory_tests={})


@contextlib.contextmanager
def execution_mode(semantic_1m=False, *, test_build=False, implementation=False, startup_wave8=False, root_reuse=False, bounded_publication=False, fixed48=False, hierarchical_cells=False, constrained_split=False, cell_overlap=False, fine_sq8=False, pq_residual=False, co_selection=False, budget_object_selector=False, budget_object_fitter=False, cohere1024=False, cohere_cohort=False, cohere_sq8_builder=False, exact_sq8_runtime=False, retained_generation_rebind=False, baseline_pool=False):
    """Restore the caller's protocol after a worker or synthetic check."""
    previous = SEMANTIC_1M, TEST_BUILD, IMPLEMENTATION, STARTUP_WAVE8, ROOT_REUSE, BOUNDED_PUBLICATION, FIXED48, HIERARCHICAL_CELLS, CONSTRAINED_SPLIT, CELL_OVERLAP, FINE_SQ8, PQ_RESIDUAL, CO_SELECTION, BUDGET_OBJECT_SELECTOR, BUDGET_OBJECT_FITTER, COHERE1024, COHERE_COHORT, COHERE_SQ8_BUILDER, EXACT_SQ8_RUNTIME, RETAINED_GENERATION_REBIND, BASELINE_POOL
    configure(semantic_1m, test_build=test_build, implementation=implementation, startup_wave8=startup_wave8, root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection, budget_object_selector=budget_object_selector, budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort, cohere_sq8_builder=cohere_sq8_builder, exact_sq8_runtime=exact_sq8_runtime, retained_generation_rebind=retained_generation_rebind, baseline_pool=baseline_pool)
    try:
        yield
    finally:
        configure(previous[0], test_build=previous[1], implementation=previous[2], startup_wave8=previous[3], root_reuse=previous[4], bounded_publication=previous[5], fixed48=previous[6], hierarchical_cells=previous[7], constrained_split=previous[8], cell_overlap=previous[9], fine_sq8=previous[10], pq_residual=previous[11], co_selection=previous[12], budget_object_selector=previous[13], budget_object_fitter=previous[14], cohere1024=previous[15], cohere_cohort=previous[16] and not previous[18] and not previous[19] and not previous[20], cohere_sq8_builder=previous[17], exact_sq8_runtime=previous[18], retained_generation_rebind=previous[19], baseline_pool=previous[20])


def mode_flag():
    if BASELINE_POOL:
        return ' --baseline-pool-implementation'
    if RETAINED_GENERATION_REBIND:
        return ' --retained-generation-rebind-implementation'
    if EXACT_SQ8_RUNTIME:
        return ' --exact-sq8-runtime'
    if COHERE_SQ8_BUILDER:
        return ' --cohere-sq8-builder'
    if COHERE_COHORT:
        return ' --cohere-cohort-implementation'
    if COHERE1024:
        return ' --cohere1024-implementation'
    if BUDGET_OBJECT_FITTER:
        return ' --budget-object-fitter-implementation'
    if BUDGET_OBJECT_SELECTOR:
        return ' --budget-object-selector-implementation'
    return ' --co-selection-implementation' if CO_SELECTION else ' --pq-residual-implementation' if PQ_RESIDUAL else ' --fine-sq8-implementation' if FINE_SQ8 else ' --cell-overlap-implementation' if CELL_OVERLAP else ' --constrained-split-implementation' if CONSTRAINED_SPLIT else ' --hierarchical-cells-implementation' if HIERARCHICAL_CELLS else ' --fixed48-implementation' if FIXED48 else ' --bounded-publication-implementation' if BOUNDED_PUBLICATION else ' --root-reuse-implementation' if ROOT_REUSE else ' --startup-wave8-implementation' if STARTUP_WAVE8 else ' --semantic-1m-implementation' if IMPLEMENTATION else ' --semantic-1m-test-build' if TEST_BUILD else ' --semantic-1m' if SEMANTIC_1M else ''


def archive_binding_prefix():
    if BASELINE_POOL:
        return 'BORSUK_BASELINE_POOL_'
    if RETAINED_GENERATION_REBIND:
        return 'BORSUK_RETAINED_GENERATION_REBIND_'
    if EXACT_SQ8_RUNTIME:
        return 'BORSUK_EXACT_SQ8_RUNTIME_'
    if COHERE_SQ8_BUILDER:
        return 'BORSUK_COHERE_SQ8_BUILDER_'
    if COHERE_COHORT:
        return 'BORSUK_COHERE_COHORT_'
    if COHERE1024:
        return 'BORSUK_COHERE1024_'
    if BUDGET_OBJECT_FITTER:
        return 'BORSUK_BUDGET_OBJECT_FITTER_'
    if BUDGET_OBJECT_SELECTOR:
        return 'BORSUK_BUDGET_OBJECT_SELECTOR_'
    return 'BORSUK_CO_SELECTION_' if CO_SELECTION else 'BORSUK_PQ_RESIDUAL_' if PQ_RESIDUAL else 'BORSUK_FINE_SQ8_' if FINE_SQ8 else 'BORSUK_CELL_OVERLAP_' if CELL_OVERLAP else 'BORSUK_CONSTRAINED_SPLIT_' if CONSTRAINED_SPLIT else 'BORSUK_HIERARCHICAL_'


def validate_constrained_split_config(config):
    required = {name:list(tests) for name,tests in constrained_split_required_tests().items()}
    assert {'mandatory_test_names_pending', 'mandatory_tests', 'control_native_source_commit',
            'control_module_prefix'} <= set(config), 'complete constrained split authority'
    assert config['mandatory_test_names_pending'] is False, 'native mandatory test names pending'
    assert config['mandatory_tests'] == required, 'exact native mandatory test roster'
    assert config['control_native_source_commit'] == CONSTRAINED_SPLIT_CONTROL
    assert config['control_module_prefix'] == CONSTRAINED_SPLIT_PREFIX


def validate_cell_overlap_config(config):
    assert config['mandatory_test_names_pending'] is False, 'native mandatory test names pending'
    assert config['mandatory_tests'] == {name:list(tests) for name,tests in cell_overlap_required_tests().items()}, 'exact native overlap test roster'


def validate_constrained_split_prefix(base):
    path = Path(base)/CONSTRAINED_SPLIT_DELTA[1]
    with path.open('rb') as source:
        prefix = source.read(CONSTRAINED_SPLIT_PREFIX['bytes'])
        assert len(prefix) == CONSTRAINED_SPLIT_PREFIX['bytes'] and source.read(1), 'additive native module'
    assert worker.sha(prefix) == CONSTRAINED_SPLIT_PREFIX['sha256'], 'unchanged native module prefix'


def _hierarchical_archive_roster(base, commit, inventory, manifest_path, *, completed_native_qualification=None):
    """Committed metadata only; CONFIG may be added by the subsequent freeze."""
    assert MINIMAL_ARCHIVE and re.fullmatch('[0-9a-f]{40}', commit)
    tree = subprocess.check_output([*ARCHIVE_GIT, 'ls-tree', '-rz', '--full-tree', commit], cwd=base)
    files = {}
    for entry in tree.split(b'\0'):
        if entry:
            metadata, name = entry.split(b'\t', 1)
            files[os.fsdecode(name)] = metadata.split()
    required = set(inventory) | set(CODE) | {str(CONFIG), str(manifest_path)}
    if COHERE_SQ8_BUILDER:
        required.add(completed_native_qualification['path'])
    paths = sorted({n for n in files if not n.startswith('docs/research/')} | required)
    shared.validate_source_archive_paths(paths)
    for name in paths:
        # The config's own hash is bound by qualification, avoiding a hash cycle.
        if name == str(CONFIG) and name not in files:
            continue
        assert name in files and files[name][:2] in ([b'100644', b'blob'], [b'100755', b'blob']), 'committed regular archive file: '+name
    attribute_paths = sorted(set(paths) | {str(parent) for name in paths for parent in Path(name).parents if parent != Path('.')})
    attrs = subprocess.check_output([*ARCHIVE_GIT, 'check-attr', '--source='+commit, '-z',
        'export-ignore', 'export-subst', '--', *attribute_paths], cwd=base).split(b'\0')
    assert all(v in (b'unspecified', b'unset') for v in attrs[2::3]), 'archive export attributes'
    return paths, files


def hierarchical_archive_authority(base, commit, inventory, manifest_path, *, completed_native_qualification=None):
    """Root-only config input from a full committed tree, before config freeze.

    The returned support hashes cover compiler/runtime fixtures; native and
    controller bytes already have independent manifests. No archive is built.
    """
    paths, files = _hierarchical_archive_roster(base, commit, inventory, manifest_path,
        completed_native_qualification=completed_native_qualification)
    support = sorted(set(paths) - set(inventory) - set(CODE) - {str(CONFIG), str(manifest_path)})
    if COHERE_SQ8_BUILDER:
        support.remove(completed_native_qualification['path'])
    hashes = {}
    producer = subprocess.Popen([*ARCHIVE_GIT, 'cat-file', '--batch'], cwd=base,
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    try:
        for name in support:
            oid = files[name][2]
            producer.stdin.write(oid+b'\n'); producer.stdin.flush()
            header = producer.stdout.readline().split()
            assert len(header) == 3 and header[:2] == [oid, b'blob'], 'committed archive blob: '+name
            remaining = int(header[2]); digest = hashlib.sha256()
            while remaining:
                chunk = producer.stdout.read(min(65536, remaining))
                assert chunk, 'truncated archive blob: '+name
                digest.update(chunk); remaining -= len(chunk)
            assert producer.stdout.read(1) == b'\n', 'archive blob framing'
            hashes[name] = digest.hexdigest()
        producer.stdin.close()
        assert producer.wait() == 0, 'committed archive blob reader'
    finally:
        if not producer.stdin.closed:
            producer.stdin.close()
        producer.stdout.close()
        if producer.poll() is None:
            producer.kill()
        producer.wait()
    return dict(source_archive_paths=paths, source_archive_paths_sha256=worker.sha(encoded(paths)),
                source_archive_file_count=len(paths), source_archive_support_sha256=hashes)


def validate_hierarchical_archive(authority, inventory, manifest_path, base=None):
    """Portable supplied closure; Git authentication is exclusive to preflight."""
    assert set(ARCHIVE_FIELDS) <= set(authority), 'complete archive authority'
    paths = authority['source_archive_paths']
    assert type(paths) is list and shared.validate_source_archive_paths(paths) == sorted(paths), 'sorted exact archive roster'
    assert authority['source_archive_paths_sha256'] == worker.sha(encoded(paths)), 'archive roster SHA'
    assert type(authority['source_archive_file_count']) is int and authority['source_archive_file_count'] == len(paths), 'archive roster count'
    support = authority['source_archive_support_sha256']
    required = set(inventory) | set(CODE) | {str(CONFIG), str(manifest_path)}
    if COHERE_SQ8_BUILDER:
        required.add(authority['completed_native_qualification']['path'])
    assert type(support) is dict and not set(support).intersection(required), 'distinct archive support files'
    assert all(type(n) is str and type(digest) is str and not n.startswith('docs/research/') and re.fullmatch('[0-9a-f]{64}', digest)
               for n, digest in support.items()), 'archive support authority'
    assert set(paths) == set(support) | required, 'exact archive file closure'
    if base is not None:
        for name in paths:
            path = Path(base)
            for part in Path(name).parts:
                path /= part
                assert not path.is_symlink(), 'archive symlink: '+name
            assert path.is_file(), 'archive regular file: '+name
        assert all(worker.artifact(Path(base)/name)['sha256'] == digest for name,digest in support.items()), 'archive support drift'
    return {key: authority[key] for key in ARCHIVE_FIELDS}


def validate_candidate_delta(paths):
    """Bind the exact native subset for a subsequent hierarchical increment."""
    assert type(paths) is list and (paths or COHERE_SQ8_BUILDER) and all(type(path) is str for path in paths), 'candidate native delta list'
    if HIERARCHICAL_CELLS:
        assert paths == sorted(set(paths)) and set(paths) <= set(NATIVE_DELTA), 'exact admitted hierarchical native subset'
    else:
        assert paths == list(NATIVE_DELTA), 'exact candidate native delta'
    return paths


def qualify(base=Path('.')):
    """Portable source/config/code qualification, without Git or completed assurance."""
    base = Path(base).resolve()
    body = (base/CONFIG).read_bytes()
    config = json.loads(body)
    assert config['controller_authority_pending'] is False, 'root authority freeze pending'
    if COHERE_SQ8_BUILDER:
        validate_cohere_sq8_builder_config(config)
        assert set(config) == set(FIXED) | {'controller_authority_pending', 'controller_source_commit',
            'controller_code_sha256', 'native_source_manifest', 'completed_native_qualification', *ARCHIVE_FIELDS}, 'exact SQ8 builder config'
    if COHERE_COHORT:
        validate_cohere_cohort_config(config)
        assert set(config) == set(FIXED) | {'controller_authority_pending', 'controller_source_commit',
            'controller_code_sha256', 'native_source_manifest', *ARCHIVE_FIELDS}, 'exact Cohere cohort config'
    if COHERE1024:
        validate_cohere1024_config(config)
        assert set(config) == set(FIXED) | {'controller_authority_pending', 'controller_source_commit',
            'controller_code_sha256', 'native_source_manifest', *ARCHIVE_FIELDS}, 'exact Cohere config'
    if BUDGET_OBJECT_FITTER:
        validate_budget_object_fitter_config(config)
        assert set(config) == set(FIXED) | {'controller_authority_pending', 'controller_source_commit',
            'controller_code_sha256', 'native_source_manifest', *ARCHIVE_FIELDS}, 'exact fitter config'
    if BUDGET_OBJECT_SELECTOR:
        validate_budget_object_selector_config(config)
        assert set(config) == set(FIXED) | {'controller_authority_pending', 'controller_source_commit',
            'controller_code_sha256', 'native_source_manifest', *ARCHIVE_FIELDS}, 'exact selector config'
    if CO_SELECTION:
        validate_co_selection_config(config)
        assert set(config) == set(FIXED) | {'controller_authority_pending', 'controller_source_commit',
            'controller_code_sha256', 'native_source_manifest', *ARCHIVE_FIELDS}, 'exact co-selection config'
    if PQ_RESIDUAL:
        validate_pq_residual_config(config)
        assert set(config) == set(FIXED) | {'controller_authority_pending', 'controller_source_commit',
            'controller_code_sha256', 'native_source_manifest', *ARCHIVE_FIELDS}, 'exact PQ residual config'
    if FINE_SQ8:
        validate_fine_sq8_config(config)
        assert set(config) == set(FIXED) | {'controller_authority_pending', 'controller_source_commit',
            'controller_code_sha256', 'native_source_manifest', *ARCHIVE_FIELDS}, 'exact fine SQ8 config'
    if CELL_OVERLAP:
        validate_cell_overlap_config(config)
        assert set(config) == set(FIXED) | {'controller_authority_pending', 'controller_source_commit',
            'controller_code_sha256', 'native_source_manifest', *ARCHIVE_FIELDS}, 'exact cell overlap config'
    if CONSTRAINED_SPLIT:
        validate_constrained_split_config(config)
        assert set(config) == set(FIXED) | {'controller_authority_pending', 'controller_source_commit',
            'controller_code_sha256', 'native_source_manifest', *ARCHIVE_FIELDS}, 'exact constrained split config'
    assert all(type(config[k]) is type(v) and config[k] == v for k,v in FIXED.items()), 'fixed execution protocol'
    code = config['controller_code_sha256']
    assert set(code) == set(CODE), 'exact transitive code roster'
    assert all(worker.artifact(base/name)['sha256'] == digest for name,digest in code.items()), 'controller code drift'
    pointer = config['native_source_manifest']
    path = Path(pointer['path'])
    assert not path.is_absolute() and '..' not in path.parts and str(path) == pointer['path']
    assert type(pointer['bytes']) is int and pointer['bytes'] > 0
    assert worker.artifact(base/path) == {key:pointer[key] for key in ('bytes','sha256')}, 'manifest authority'
    manifest = json.loads((base/path).read_bytes())
    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
        assert re.fullmatch('[0-9a-f]{40}', config['controller_source_commit']), 'frozen controller commit'
        assert manifest['schema'] == ('borsuk-baseline-pool-implementation-gates-native-source-manifest-v1' if BASELINE_POOL else 'borsuk-retained-generation-rebind-implementation-gates-native-source-manifest-v1' if RETAINED_GENERATION_REBIND else 'borsuk-exact-sq8-runtime-gates-native-source-manifest-v1' if EXACT_SQ8_RUNTIME else 'borsuk-cohere-sq8-builder-native-source-manifest-v1' if COHERE_SQ8_BUILDER else 'borsuk-cohere-cohort-implementation-gates-native-source-manifest-v1' if COHERE_COHORT else 'borsuk-cohere1024-implementation-gates-native-source-manifest-v1' if COHERE1024 else 'borsuk-budget-object-fitter-native-source-manifest-v1' if BUDGET_OBJECT_FITTER else 'borsuk-budget-object-selector-native-source-manifest-v1' if BUDGET_OBJECT_SELECTOR else 'borsuk-co-selection-native-source-manifest-v1' if CO_SELECTION else 'borsuk-pq-residual-native-source-manifest-v1' if PQ_RESIDUAL else 'borsuk-corrected-four-bit-native-source-manifest-v1' if FINE_SQ8 else 'borsuk-cell-overlap-native-source-manifest-v1' if CELL_OVERLAP else 'borsuk-constrained-split-native-source-manifest-v1' if CONSTRAINED_SPLIT else 'borsuk-hierarchical-cells-native-source-manifest-v1' if HIERARCHICAL_CELLS else 'borsuk-fixed48-native-source-manifest-v1' if FIXED48 else 'borsuk-bounded-publication-native-source-manifest-v1' if BOUNDED_PUBLICATION else 'borsuk-root-reuse-native-source-manifest-v1' if ROOT_REUSE else 'borsuk-startup-wave8-native-source-manifest-v1')
        assert manifest['candidate_qualification_pending'] is True, 'source authority is not completed assurance'
        validate_candidate_delta(manifest['candidate_delta_paths'])
        if STARTUP_WAVE8:
            assert manifest['native_source_commit'] == STARTUP_WAVE8_COMMIT, 'fixed startup candidate commit'
            assert manifest['source_identity_sha256'] == STARTUP_WAVE8_IDENTITY, 'fixed startup candidate identity'
        assert re.fullmatch('[0-9a-f]{40}', manifest['control_native_source_commit'])
        if COHERE_SQ8_BUILDER:
            assert manifest['native_source_commit'] == manifest['control_native_source_commit'], 'unchanged qualified native source'
        if CONSTRAINED_SPLIT:
            assert manifest['control_native_source_commit'] == CONSTRAINED_SPLIT_CONTROL, 'exact original native control'
            validate_constrained_split_prefix(base)
    inventory = worker.source_hashes(base)
    if COHERE_SQ8_BUILDER:
        assert 'crates/borsuk/examples/build_sq8_source.rs' in inventory, 'existing SQ8 builder source required'
    if FINE_SQ8 or PQ_RESIDUAL or CO_SELECTION or BUDGET_OBJECT_SELECTOR or BUDGET_OBJECT_FITTER or COHERE1024 or COHERE_COHORT or COHERE_SQ8_BUILDER:
        assert set(NATIVE_DELTA) <= set(inventory), 'complete corrected four-bit native source roster'
    if BASELINE_POOL:
        validate_baseline_pool_sources(base)
    if RETAINED_GENERATION_REBIND:
        validate_retained_rebind_sources(base, config['mandatory_tests'])
    assert type(manifest['source_file_count']) is int and manifest['source_file_count'] == len(inventory) > 0
    assert manifest['source_sha256'] == inventory, 'full native source drift'
    identity = worker.source_identity(inventory)
    assert identity == manifest['source_identity_sha256']
    assert SEMANTIC_1M or identity == SOURCE_IDENTITY, 'historical native source identity'
    assert re.fullmatch('[0-9a-f]{40}', manifest['native_source_commit'])
    proof = dict(schema='borsuk-baseline-pool-implementation-gates-qualification-v1' if BASELINE_POOL else 'borsuk-retained-generation-rebind-implementation-gates-qualification-v1' if RETAINED_GENERATION_REBIND else 'borsuk-exact-sq8-runtime-gates-qualification-v1' if EXACT_SQ8_RUNTIME else 'borsuk-cohere-sq8-builder-qualification-v1' if COHERE_SQ8_BUILDER else 'borsuk-cohere-cohort-implementation-gates-qualification-v1' if COHERE_COHORT else 'borsuk-cohere1024-implementation-gates-qualification-v1' if COHERE1024 else 'borsuk-budget-object-fitter-implementation-gates-qualification-v1' if BUDGET_OBJECT_FITTER else 'borsuk-budget-object-selector-implementation-gates-qualification-v1' if BUDGET_OBJECT_SELECTOR else 'borsuk-co-selection-implementation-gates-qualification-v1' if CO_SELECTION else 'borsuk-pq-residual-implementation-gates-qualification-v1' if PQ_RESIDUAL else 'borsuk-corrected-four-bit-implementation-gates-qualification-v1' if FINE_SQ8 else 'borsuk-cell-overlap-implementation-gates-qualification-v1' if CELL_OVERLAP else 'borsuk-constrained-split-implementation-gates-qualification-v1' if CONSTRAINED_SPLIT else 'borsuk-hierarchical-cells-implementation-gates-qualification-v2' if HIERARCHICAL_CELLS else 'borsuk-fixed48-implementation-gates-qualification-v1' if FIXED48 else 'borsuk-bounded-publication-implementation-gates-qualification-v1' if BOUNDED_PUBLICATION else 'borsuk-root-reuse-implementation-gates-qualification-v1' if ROOT_REUSE else 'borsuk-startup-wave8-implementation-gates-qualification-v1' if STARTUP_WAVE8 else 'borsuk-semantic-1m-implementation-gates-qualification-v1' if IMPLEMENTATION else 'borsuk-native-workspace-test-build-qualification-v1' if TEST_BUILD else 'borsuk-native-workspace-execution-qualification-v1',
        config_path=str(CONFIG), config_sha256=worker.sha(body), campaign_schema=SCHEMA,
        source_sha256=inventory, source_identity_sha256=identity, source_file_count=len(inventory),
        native_source_commit=manifest['native_source_commit'], native_source_manifest=pointer,
        native_source_manifest_sha256=pointer['sha256'], code_sha256=code,
        code_identity_sha256=worker.sha(encoded(code)), artifact_roster_sha256=worker.sha(encoded(ARTIFACTS)),
        command=list(FIXED['command']), environment=dict(FIXED['environment']),
        actual_full_workspace_execution=False, awscli_version=semantic.AWSCLI_VERSION,
        awscli_sha256=semantic.AWSCLI_SHA256)
    if TEST_BUILD or IMPLEMENTATION:
        proof.update(execution_kind=FIXED['execution_kind'])
        if TEST_BUILD:
            proof.update(actual_workspace_test_build=False)
    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
        proof.update(controller_source_commit=config['controller_source_commit'],
                     candidate_delta_paths=manifest['candidate_delta_paths'])
    if CONSTRAINED_SPLIT:
        proof.update(mandatory_test_names_pending=False, mandatory_tests=config['mandatory_tests'],
                     control_native_source_commit=CONSTRAINED_SPLIT_CONTROL,
                     control_module_prefix=CONSTRAINED_SPLIT_PREFIX)
    if CELL_OVERLAP or FINE_SQ8 or PQ_RESIDUAL or CO_SELECTION or BUDGET_OBJECT_SELECTOR or BUDGET_OBJECT_FITTER or COHERE1024 or COHERE_COHORT or COHERE_SQ8_BUILDER:
        proof.update(mandatory_test_names_pending=False, mandatory_tests=config['mandatory_tests'])
    if COHERE_SQ8_BUILDER:
        proof['completed_native_qualification'] = config['completed_native_qualification']
        completed_native_qualification(base, proof)
    if MINIMAL_ARCHIVE:
        proof.update(validate_hierarchical_archive(config, inventory, pointer['path'], base))
    return proof


def preflight(base=Path('.')):
    assert not subprocess.check_output(['git','status','--porcelain'], cwd=base, text=True).strip(), 'dirty source'
    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
        proof = qualify(base)
        parents = subprocess.check_output(['git','rev-list','--parents','-n','1','HEAD'], cwd=base, text=True).split()
        assert len(parents) == 2, 'one source-bundle parent required'
        config_commit = parents[1]
        controller = proof['controller_source_commit']
        assert subprocess.check_output(['git','rev-list','--parents','-n','1',config_commit], cwd=base, text=True).split() == [config_commit, controller], 'config immediately follows frozen controller'
        assert subprocess.check_output(['git','for-each-ref','--contains='+config_commit,'--format=%(refname)',
            'refs/remotes/origin/'], cwd=base, text=True).strip(), 'controller/config commit not on an origin ref'
        assert subprocess.check_output(['git','diff','--no-renames','--name-only',controller,config_commit],
            cwd=base, text=True).splitlines() == [str(CONFIG)], 'config-only authority freeze'
        assert subprocess.check_output(['git','diff','--no-renames','--name-only',config_commit,'HEAD'],
            cwd=base, text=True).splitlines() == proof['candidate_delta_paths'], 'exact candidate source bundle'
        if COHERE_SQ8_BUILDER:
            assert subprocess.check_output(['git','for-each-ref','--contains='+proof['native_source_commit'],
                '--format=%(refname)', 'refs/remotes/origin/main'], cwd=base, text=True).splitlines() == ['refs/remotes/origin/main'], 'qualified native source not integrated on origin/main'
            native_trees = []
            for commit in (proof['native_source_commit'], controller):
                tree = subprocess.check_output([*ARCHIVE_GIT, 'ls-tree', '-rz', '--full-tree', commit], cwd=base)
                native = {}
                for entry in tree.split(b'\0'):
                    if entry:
                        metadata, name = entry.split(b'\t', 1)
                        name = os.fsdecode(name)
                        if (name.endswith('.rs') or Path(name).name in ('Cargo.toml', 'Cargo.lock')) and not {'.git', 'target'}.intersection(Path(name).parts):
                            assert metadata.split()[:2] in ([b'100644', b'blob'], [b'100755', b'blob']), 'committed regular native source'
                            native[name] = metadata.split()[2]
                assert set(native) == set(proof['source_sha256']), 'complete committed native roster'
                native_trees.append(native)
            assert native_trees[0] == native_trees[1], 'native source changed from qualified commit'
        if BASELINE_POOL:
            assert subprocess.check_output(['git','diff','--no-renames','--diff-filter=M','--name-only',
                config_commit,'HEAD'], cwd=base, text=True).splitlines() == list(NATIVE_DELTA), 'one MODIFIED baseline native path'
        if RETAINED_GENERATION_REBIND:
            assert subprocess.check_output(['git','diff','--no-renames','--diff-filter=M','--name-only',
                config_commit,'HEAD'], cwd=base, text=True).splitlines() == list(NATIVE_DELTA), 'three MODIFIED native paths, no new Rust path'
        if COHERE_COHORT and not (EXACT_SQ8_RUNTIME or RETAINED_GENERATION_REBIND or BASELINE_POOL):
            assert subprocess.check_output(['git','diff','--no-renames','--diff-filter=A','--name-only',
                config_commit,'HEAD'], cwd=base, text=True).splitlines() == list(NATIVE_DELTA), 'one NEW publisher native bin'
        for name in proof['candidate_delta_paths']:
            assert worker.sha(subprocess.check_output(['git','show',proof['native_source_commit']+':'+name], cwd=base)) == proof['source_sha256'][name], 'candidate commit blob: '+name
        if CONSTRAINED_SPLIT:
            assert subprocess.check_output(['git', 'diff', '--no-renames', '--name-only', CONSTRAINED_SPLIT_CONTROL,
                proof['native_source_commit']], cwd=base, text=True).splitlines() == list(NATIVE_DELTA), 'additive candidate-only native commit'
            original = subprocess.check_output(['git', 'show', CONSTRAINED_SPLIT_CONTROL+':'+NATIVE_DELTA[1]], cwd=base)
            assert dict(bytes=len(original), sha256=worker.sha(original)) == CONSTRAINED_SPLIT_PREFIX, 'original native prefix authority'
        if MINIMAL_ARCHIVE:
            committed = hierarchical_archive_authority(base, parents[0], proof['source_sha256'], proof['native_source_manifest']['path'],
                **({'completed_native_qualification':proof['completed_native_qualification']} if COHERE_SQ8_BUILDER else {}))
            assert committed == {key:proof[key] for key in ARCHIVE_FIELDS}, 'committed minimal archive authority'
        return proof
    assert subprocess.check_output(['git','for-each-ref','--contains=HEAD','--format=%(refname)',
        'refs/remotes/origin/'], cwd=base, text=True).strip(), 'source commit not on an origin ref'
    return qualify(base)


def stage(repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    proof = qualify(repo)
    if MINIMAL_ARCHIVE and any(archive_binding_prefix()+key.upper() in os.environ for key in ARCHIVE_IDENTITIES):
        assert all(os.environ.get(archive_binding_prefix()+key.upper()) == str(proof[key]) for key in ARCHIVE_IDENTITIES), 'deployed archive roster binding'
    bodies = [('source-qualification.json', encoded(proof)+b'\n'),
                       ('config.json', (repo/CONFIG).read_bytes()),
                       ('native-source-manifest.json', (repo/proof['native_source_manifest']['path']).read_bytes())]
    if COHERE_SQ8_BUILDER:
        bodies.append(('completed-native-qualification.json', completed_native_qualification(repo, proof)))
    for name, body in bodies:
        with (out/name).open('xb') as output:
            output.write(body); output.flush(); os.fsync(output.fileno())
    return proof


def record_constrained_split_stage(argv, *, cell_overlap=False, fine_sq8=False, pq_residual=False, co_selection=False, budget_object_selector=False, budget_object_fitter=False, cohere1024=False, cohere_cohort=False, cohere_sq8_builder=False):
    """Stage gate shared by the serial shell runner and bounded Python checks."""
    strict_counts = cell_overlap or fine_sq8 or pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder
    required = {} if cohere_sq8_builder else cohere_cohort_required_tests() if cohere_cohort else cohere1024_required_tests() if cohere1024 else budget_object_fitter_required_tests() if budget_object_fitter else budget_object_selector_required_tests() if budget_object_selector else co_selection_required_tests() if co_selection else pq_residual_required_tests() if pq_residual else fine_sq8_required_tests() if fine_sq8 else cell_overlap_required_tests() if cell_overlap else constrained_split_required_tests()
    stages = COHERE_SQ8_BUILDER_STAGES if cohere_sq8_builder else COHERE_COHORT_STAGES if cohere_cohort else COHERE1024_STAGES if cohere1024 else BUDGET_OBJECT_FITTER_STAGES if budget_object_fitter else BUDGET_OBJECT_SELECTOR_STAGES if budget_object_selector else CO_SELECTION_STAGES if co_selection else PQ_RESIDUAL_STAGES if pq_residual else FINE_SQ8_STAGES if fine_sq8 else CELL_OVERLAP_STAGES if cell_overlap else CONSTRAINED_SPLIT_STAGES
    resources = None
    if budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder:
        *argv, resource_log = argv
        if argv[2]:
            resources = Path(resource_log).read_text()
            validate_budget_object_selector_resources(resources, int(argv[3]))
    stage, started, finished, status, log, log_status, *command = argv
    assert dict(stages)[stage] == command, 'exact stage argv'
    tests = passes = None
    failed = ignored = summaries = passed_lines = test_builds = 0
    seen = set()
    duplicate = False
    wrong_stage = False
    if finished:
        passes = dict.fromkeys(required.get(stage, ()), 0)
        if stage not in NON_TEST_STAGES or strict_counts and stage == 'test-build':
            is_test = stage != 'test-build'
            if is_test:
                tests = 0
            with Path(log).open() as source:
                for line in source:
                    if pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder:
                        failed += bool(re.fullmatch(r'test .+ \.\.\. (?:FAILED|ignored)\n?', line))
                        failed += line.startswith('test result: FAILED.')
                    test = re.fullmatch(r'test (?:crates/borsuk/)?(.+) \.\.\. ok\n?' if pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder else r'test (\S+) \.\.\. ok\n?', line)
                    passed_lines += bool(test)
                    if strict_counts and test:
                        duplicate |= test[1] in seen
                        seen.add(test[1])
                        wrong_stage |= (pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort) and test[1] not in passes and any(test[1] in names for name, names in required.items() if name != stage)
                    if test and test[1] in passes:
                        passes[test[1]] += 1
                    summary = re.fullmatch(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; \d+ filtered out;.*\n?', line)
                    if summary and is_test:
                        summaries += 1
                        tests += int(summary[1]) + int(summary[2])
                        failed += int(summary[2]); ignored += int(summary[3])
                    test_builds += bool(re.fullmatch(r'rust-test-build status=0 elapsed_seconds=\d+ jobs=1\n?', line))
    if cohere_sq8_builder and finished:
        with Path(log).open() as source:
            failed += any(re.match(r'(?:test .+ \.\.\. |test result:|rust-test-build )', line) for line in source)
    evidence_invalid = strict_counts and bool(finished) and (
        (stage not in NON_TEST_STAGES and (summaries != 1 or passed_lines != tests)) or
        (stage == 'test-build' and test_builds != 1))
    evidence_invalid |= (pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort) and bool(finished) and (wrong_stage or
        stage.startswith(('cohere-native-', 'exact-sq8-scorer', 'retained-rebind-', 'pq-residual-', 'co-selection-', 'budget-object-selector-', 'budget-object-fitter-')) and (seen != set(passes) or tests != len(passes)))
    evidence_invalid |= cohere1024 and bool(finished) and stage == 'generation-release-test' and (seen != set(passes) or tests != len(passes))
    evidence_invalid |= BASELINE_POOL and bool(finished) and stage in BASELINE_POOL_TEST_COUNTS and tests != BASELINE_POOL_TEST_COUNTS[stage]
    gate = (int(status) or int(log_status) or
        (96 if tests == 0 or failed or ignored or duplicate or evidence_invalid or any(count != 1 for count in passes.values()) else 0)) if finished else None
    print(json.dumps(dict(schema=COHERE_SQ8_BUILDER_STAGE_SCHEMA if cohere_sq8_builder else COHERE_COHORT_STAGE_SCHEMA if cohere_cohort else COHERE1024_STAGE_SCHEMA if cohere1024 else BUDGET_OBJECT_FITTER_STAGE_SCHEMA if budget_object_fitter else BUDGET_OBJECT_SELECTOR_STAGE_SCHEMA if budget_object_selector else CO_SELECTION_STAGE_SCHEMA if co_selection else PQ_RESIDUAL_STAGE_SCHEMA if pq_residual else FINE_SQ8_STAGE_SCHEMA if fine_sq8 else CELL_OVERLAP_STAGE_SCHEMA if cell_overlap else CONSTRAINED_SPLIT_STAGE_SCHEMA, stage=stage,
        started_at=started, finished_at=finished or None, exit_status=int(status) if finished else None,
        log_exit_status=int(log_status) if finished else None, gate_status=gate,
        tests_run=tests, required_test_passes=passes, command=command,
        **({'resources':resources} if budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder else {})), sort_keys=True), flush=True)
    return gate or 0


def validate_budget_object_selector_resources(body, status):
    assert type(body) is str and body, 'actual stage resources required'
    for field, pattern in (
        ('User time (seconds)', r'[0-9]+(?:\.[0-9]+)?'),
        ('System time (seconds)', r'[0-9]+(?:\.[0-9]+)?'),
        ('Elapsed (wall clock) time (h:mm:ss or m:ss)', r'(?:[0-9]+:){1,2}[0-9]+(?:\.[0-9]+)?'),
        ('Maximum resident set size (kbytes)', r'[0-9]+'),
        ('Exit status', r'[0-9]+')):
        matches = re.findall(r'^\s*'+re.escape(field)+r': ('+pattern+r')$', body, re.MULTILINE)
        assert len(matches) == 1, 'stage resource field: '+field
        if field == 'Maximum resident set size (kbytes)':
            assert 0 < int(matches[0])*1024 <= worker.MEMORY, 'stage RSS ceiling'
        if field == 'Exit status':
            assert int(matches[0]) == status, 'stage resource exit'


def validate_bounded_publication_stages(log, *, fixed48=False, hierarchical_cells=False, constrained_split=False, cell_overlap=False, fine_sq8=False, pq_residual=False, co_selection=False, budget_object_selector=False, budget_object_fitter=False, cohere1024=False, cohere_cohort=False, cohere_sq8_builder=False):
    from datetime import datetime
    strict_counts = cell_overlap or fine_sq8 or pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder
    assert sum((fixed48, hierarchical_cells, constrained_split, cell_overlap, fine_sq8, pq_residual, co_selection, budget_object_selector, budget_object_fitter, cohere1024, cohere_cohort, cohere_sq8_builder)) <= 1, 'mutually exclusive stage protocols'
    stages = COHERE_SQ8_BUILDER_STAGES if cohere_sq8_builder else COHERE_COHORT_STAGES if cohere_cohort else COHERE1024_STAGES if cohere1024 else BUDGET_OBJECT_FITTER_STAGES if budget_object_fitter else BUDGET_OBJECT_SELECTOR_STAGES if budget_object_selector else CO_SELECTION_STAGES if co_selection else PQ_RESIDUAL_STAGES if pq_residual else FINE_SQ8_STAGES if fine_sq8 else CELL_OVERLAP_STAGES if cell_overlap else CONSTRAINED_SPLIT_STAGES if constrained_split else HIERARCHICAL_CELLS_STAGES if hierarchical_cells else FIXED48_STAGES if fixed48 else BOUNDED_PUBLICATION_STAGES
    schema = COHERE_SQ8_BUILDER_STAGE_SCHEMA if cohere_sq8_builder else COHERE_COHORT_STAGE_SCHEMA if cohere_cohort else COHERE1024_STAGE_SCHEMA if cohere1024 else BUDGET_OBJECT_FITTER_STAGE_SCHEMA if budget_object_fitter else BUDGET_OBJECT_SELECTOR_STAGE_SCHEMA if budget_object_selector else CO_SELECTION_STAGE_SCHEMA if co_selection else PQ_RESIDUAL_STAGE_SCHEMA if pq_residual else FINE_SQ8_STAGE_SCHEMA if fine_sq8 else CELL_OVERLAP_STAGE_SCHEMA if cell_overlap else CONSTRAINED_SPLIT_STAGE_SCHEMA if constrained_split else HIERARCHICAL_CELLS_STAGE_SCHEMA if hierarchical_cells else FIXED48_STAGE_SCHEMA if fixed48 else BOUNDED_PUBLICATION_STAGE_SCHEMA
    required = {} if cohere_sq8_builder else cohere_cohort_required_tests() if cohere_cohort else cohere1024_required_tests() if cohere1024 else budget_object_fitter_required_tests() if budget_object_fitter else budget_object_selector_required_tests() if budget_object_selector else co_selection_required_tests() if co_selection else pq_residual_required_tests() if pq_residual else fine_sq8_required_tests() if fine_sq8 else cell_overlap_required_tests() if cell_overlap else constrained_split_required_tests() if constrained_split else HIERARCHICAL_CELLS_REQUIRED_TESTS if hierarchical_cells else FIXED48_REQUIRED_TESTS
    named_protocol = fixed48 or hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder
    test_stage_indices = {index for index, (name, _) in enumerate(stages) if name not in NON_TEST_STAGES}
    test_record_positions = {2*index+1 for index in test_stage_indices}
    named_stages = {test: index for index, (name, _) in enumerate(stages)
        for test in required.get(name, ())} if named_protocol else {BOUNDED_PUBLICATION_CAP_TEST: 1}
    passes = dict.fromkeys(named_stages, 0)
    test_counts = [0]*len(stages)
    passed_lines = [0]*len(stages)
    seen = [set() for _ in range(len(stages))]
    summaries = [0]*len(stages)
    test_builds = 0
    records = []
    with Path(log).open() as source:
        for line in source:
            assert not cohere_sq8_builder or not re.match(r'(?:test .+ \.\.\. |test result:|rust-test-build )', line), 'test evidence forbidden in build-only mode'
            assert not (pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort) or not re.fullmatch(r'test .+ \.\.\. (?:FAILED|ignored)\n?', line), 'unsuccessful selector stage test' if budget_object_selector else 'unsuccessful PQ residual stage test'
            assert not (pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort) or not line.startswith('test result: FAILED.'), 'failed selector stage summary' if budget_object_selector else 'failed PQ residual stage summary'
            match = re.fullmatch(r'test (?:crates/borsuk/)?(.+) \.\.\. ok\n?' if pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder else r'test (\S+) \.\.\. ok\n?', line)
            if strict_counts and match:
                assert len(records) in test_record_positions, 'test outside execution stage'
                assert match[1] not in seen[len(records)//2], 'duplicate test in stage: '+match[1]
                seen[len(records)//2].add(match[1])
                passed_lines[len(records)//2] += 1
            if match and match[1] in named_stages:
                test = match[1]
                # Module regressions deliberately rerun overlap tests. The
                # mandatory roster is counted only in its narrow owning stage.
                if cohere1024 or EXACT_SQ8_RUNTIME:  # the same names are owned by two stages (debug and release)
                    assert test in required.get(stages[len(records)//2][0], ()), 'shared-roster test in wrong stage'
                elif cell_overlap and len(records) in (5, 7):
                    pass
                else:
                    assert len(records) == 2*named_stages[test]+1, 'named test in wrong stage: '+test
                    passes[test] += 1
            if named_protocol:
                summary = re.fullmatch(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; \d+ filtered out;.*\n?', line)
                if summary:
                    assert len(records) in test_record_positions, 'tests outside execution stages'
                    assert int(summary[2]) == 0, 'failed tests'
                    assert not (constrained_split or strict_counts) or int(summary[3]) == 0, 'ignored tests forbidden'
                    test_counts[len(records)//2] += int(summary[1])
                    summaries[len(records)//2] += 1
            if (hierarchical_cells or constrained_split or strict_counts) and re.fullmatch(r'rust-test-build status=0 elapsed_seconds=\d+ jobs=1\n?', line):
                assert len(records) == 2*len(stages)-1, 'test-build proof in wrong stage'
                test_builds += 1
            if not line.startswith('{'):
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if cohere_sq8_builder and isinstance(record, dict) and 'stage' in record:
                assert record.get('schema') == schema, 'foreign build stage'
            if isinstance(record, dict) and record.get('schema') == schema:
                records.append(record)
                assert len(records) <= 2*len(stages), 'extra gate stage records'
    assert len(records) == 2*len(stages), 'all completed stages required'
    assert (all(test in seen[index] for index, (name, _) in enumerate(stages) for test in required.get(name, ()))
        if cohere1024 or EXACT_SQ8_RUNTIME else all(count == 1 for count in passes.values())), 'named tests must pass exactly once in each required stage'
    assert not cohere1024 or seen[1] == set(required['generation-release-test']) and test_counts[1] == 1, 'exact release oracle count'
    assert not strict_counts or all(summaries[index] == 1 and passed_lines[index] == test_counts[index] for index in test_stage_indices), 'actual nonzero test counts'
    assert not BASELINE_POOL or all(test_counts[index] == BASELINE_POOL_TEST_COUNTS[stages[index][0]] for index in test_stage_indices), 'exact baseline pool owning target counts'
    assert not (pq_residual or co_selection or budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort) or all(seen[index] == set(required[name]) and test_counts[index] == len(required[name])
        for index, (name, _) in enumerate(stages) if name.startswith(('cohere-native-', 'exact-sq8-scorer', 'retained-rebind-', 'pq-residual-', 'co-selection-', 'budget-object-selector-', 'budget-object-fitter-'))), 'exact selector stage tests' if budget_object_selector else 'exact PQ residual stage tests'
    assert cohere_sq8_builder or not (hierarchical_cells or constrained_split or strict_counts) or test_builds == 1, 'actual unshimmed test-build completion'
    previous_finish = None
    for index, (name, command) in enumerate(stages):
        start, end = records[2*index:2*index+2]
        assert start['stage'] == end['stage'] == name and start['command'] == end['command'] == command
        test_field = 'required_test_passes' if named_protocol else 'publication_cap_test_passed'
        assert start['finished_at'] is start['exit_status'] is start['gate_status'] is start['tests_run'] is start[test_field] is None
        assert type(end['exit_status']) is type(end['gate_status']) is int and end['exit_status'] == end['gate_status'] == 0
        if constrained_split or strict_counts:
            assert start['log_exit_status'] is None
            assert type(end['log_exit_status']) is int and end['log_exit_status'] == 0, 'stage log exit'
        assert start['started_at'] == end['started_at']
        if budget_object_selector or budget_object_fitter or cohere1024 or cohere_cohort or cohere_sq8_builder:
            assert start['resources'] is None
            validate_budget_object_selector_resources(end['resources'], end['exit_status'])
        assert datetime.fromisoformat(end['finished_at']) >= datetime.fromisoformat(start['started_at'])
        assert (type(end['tests_run']) is int and end['tests_run'] > 0) if index in test_stage_indices else end['tests_run'] is None
        if named_protocol:
            assert index not in test_stage_indices or end['tests_run'] == test_counts[index], 'actual test count'
            expected = {test: int(test in seen[index]) if cohere1024 or EXACT_SQ8_RUNTIME else passes[test] for test in required.get(name, ())}
            assert end[test_field] == expected and all(type(count) is int for count in end[test_field].values()), 'actual named test passes'
            started, finished = datetime.fromisoformat(start['started_at']), datetime.fromisoformat(end['finished_at'])
            assert started.utcoffset().total_seconds() == finished.utcoffset().total_seconds() == 0, 'UTC stage timestamps'
            assert previous_finish is None or started >= previous_finish, 'serial stage timestamps'
            previous_finish = finished
        else:
            assert end[test_field] is True if index == 1 else end[test_field] is None
    return records[1::2]


def validate_receipt(out, proof):
    out = Path(out)
    assert proof['config_path'] == str(CONFIG) and proof['campaign_schema'] == SCHEMA, 'receipt mode'
    if COHERE_SQ8_BUILDER:
        validate_cohere_sq8_builder_config(proof)
    if COHERE_COHORT:
        validate_cohere_cohort_config(proof)
    if COHERE1024:
        validate_cohere1024_config(proof)
    if BUDGET_OBJECT_FITTER:
        validate_budget_object_fitter_config(proof)
    if BUDGET_OBJECT_SELECTOR:
        validate_budget_object_selector_config(proof)
    if CO_SELECTION:
        validate_co_selection_config(proof)
    if PQ_RESIDUAL:
        validate_pq_residual_config(proof)
    if FINE_SQ8:
        validate_fine_sq8_config(proof)
    if CELL_OVERLAP:
        validate_cell_overlap_config(proof)
    if CONSTRAINED_SPLIT:
        validate_constrained_split_config(proof)
    if MINIMAL_ARCHIVE:
        validate_hierarchical_archive(proof, proof['source_sha256'], proof['native_source_manifest']['path'])
    receipt = json.loads((out/'workspace-receipt.json').read_bytes())
    assert receipt['schema'] == RECEIPT_SCHEMA
    if COHERE_SQ8_BUILDER:
        validate_cohere_sq8_builder_config(receipt)
        assert receipt['completed_native_qualification'] == proof['completed_native_qualification'], 'receipt completed native qualification binding'
        completed_native_qualification(out, proof, retained=True)
    if COHERE_COHORT:
        validate_cohere_cohort_config(receipt)
    if COHERE1024:
        validate_cohere1024_config(receipt)
    if BUDGET_OBJECT_FITTER:
        validate_budget_object_fitter_config(receipt)
    if BUDGET_OBJECT_SELECTOR:
        validate_budget_object_selector_config(receipt)
    if CO_SELECTION:
        validate_co_selection_config(receipt)
    if PQ_RESIDUAL:
        validate_pq_residual_config(receipt)
    if FINE_SQ8:
        validate_fine_sq8_config(receipt)
    if CELL_OVERLAP:
        validate_cell_overlap_config(receipt)
    if CONSTRAINED_SPLIT:
        validate_constrained_split_config(receipt)
    assert type(receipt['exit_status']) is int and receipt['exit_status'] == 0
    assert type(receipt['gate_status']) is int and receipt['gate_status'] == 0
    assert receipt['qualified'] is receipt['command_started'] is receipt['command_completed'] is True
    assert receipt['source_unchanged'] is True
    assert proof['command'] == FIXED['command'] and proof['environment'] == FIXED['environment'], 'proof protocol'
    if TEST_BUILD or IMPLEMENTATION:
        assert proof['execution_kind'] == receipt['execution_kind'] == FIXED['execution_kind']
        assert receipt['actual_full_workspace_execution'] is False
        assert receipt['command'] == FIXED['command'], 'exact gate script command'
    else:
        assert receipt['command'][1:] == list(worker.COMMAND[1:]), 'exact full command'
    assert isinstance(receipt['command'][0], str) and receipt['command'][0]
    assert receipt['environment'] == FIXED['environment']
    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
        assert all(receipt[key] == proof[key] for key in ('controller_source_commit','candidate_delta_paths')), 'receipt source bundle authority'
    assert receipt['source_sha256'] == proof['source_sha256']
    assert receipt['source_identity_sha256'] == worker.source_identity(receipt['source_sha256']) == proof['source_identity_sha256']
    assert SEMANTIC_1M or proof['source_identity_sha256'] == SOURCE_IDENTITY
    assert type(receipt['source_file_count']) is type(proof['source_file_count']) is int
    assert receipt['source_file_count'] == proof['source_file_count'] == len(proof['source_sha256']) > 0
    for key in ('config_sha256','code_identity_sha256','campaign_schema','artifact_roster_sha256'):
        assert receipt[key] == proof[key], 'receipt ' + key
    assert receipt['qualification_sha256'] == worker.artifact(out/'source-qualification.json')['sha256']
    expected = set(ARTIFACTS) - {'workspace-receipt.json','run-closed.log'}
    assert set(receipt['artifacts']) == expected
    for name, identity in receipt['artifacts'].items():
        assert worker.artifact(out/name) == identity, 'receipt artifact: ' + name
        assert type(identity['bytes']) is int and identity['bytes'] > 0
    if IMPLEMENTATION:
        for name in RELEASE_ARTIFACTS:
            path = out/name
            assert path.is_file() and not path.is_symlink(), 'regular release binary: '+name
            with path.open('rb') as binary:
                assert binary.read(4) == b'\x7fELF', 'ELF release binary: '+name
    if EXACT_SQ8_RUNTIME:
        assert (out/EXACT_SQ8_PROBE_ARTIFACT).read_bytes() == probe_lines((out/'test.log').read_bytes()), 'retained exact probe JSON lines'
    assert worker.artifact(out/'config.json')['sha256'] == proof['config_sha256']
    assert worker.artifact(out/'native-source-manifest.json')['sha256'] == proof['native_source_manifest_sha256']
    for name in ('source-before.json','source-after.json'):
        assert json.loads((out/name).read_bytes()) == proof['source_sha256']
    if BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
        assert receipt['stages'] == validate_bounded_publication_stages(out/'test.log', fixed48=FIXED48, hierarchical_cells=HIERARCHICAL_CELLS, constrained_split=CONSTRAINED_SPLIT, cell_overlap=CELL_OVERLAP, fine_sq8=FINE_SQ8, pq_residual=PQ_RESIDUAL, co_selection=CO_SELECTION, budget_object_selector=BUDGET_OBJECT_SELECTOR, budget_object_fitter=BUDGET_OBJECT_FITTER, cohere1024=COHERE1024, cohere_cohort=COHERE_COHORT, cohere_sq8_builder=COHERE_SQ8_BUILDER), 'actual stage receipt'
    worker.validate_cgroup(json.loads((out/'workspace-cgroup.json').read_bytes()))
    return receipt


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha)
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix)
    assert qualification['campaign_schema'] == SCHEMA and qualification['config_path'] == str(CONFIG)
    if COHERE_SQ8_BUILDER:
        validate_cohere_sq8_builder_config(qualification)
    if COHERE_COHORT:
        validate_cohere_cohort_config(qualification)
    if COHERE1024:
        validate_cohere1024_config(qualification)
    if BUDGET_OBJECT_FITTER:
        validate_budget_object_fitter_config(qualification)
    if BUDGET_OBJECT_SELECTOR:
        validate_budget_object_selector_config(qualification)
    if CO_SELECTION:
        validate_co_selection_config(qualification)
    if PQ_RESIDUAL:
        validate_pq_residual_config(qualification)
    if FINE_SQ8:
        validate_fine_sq8_config(qualification)
    if CELL_OVERLAP:
        validate_cell_overlap_config(qualification)
    if CONSTRAINED_SPLIT:
        validate_constrained_split_config(qualification)
    if MINIMAL_ARCHIVE:
        validate_hierarchical_archive(qualification, qualification['source_sha256'], qualification['native_source_manifest']['path'])
    # Keep only small terminal identities in the existing bootstrap. The full
    # full native map is regenerated from the authenticated archive on the worker.
    adapter = {key:qualification[key] for key in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={'key':'unused'}, native_publisher={'key':'unused'})
    with patch.multiple(semantic, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS,
                        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(semantic, '_offered', return_value=False):
        body = semantic.user_data(commit, archive_sha, archive_key, prefix, adapter)
    flag = mode_flag()
    command = f'''phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
export RUSTUP_HOME="$root/.rustup" CARGO_HOME="$root/.cargo"
export PATH="$CARGO_HOME/bin:$PATH"
curl -fsSL --connect-timeout 10 --max-time 180 https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain 1.98.0
phase=source-qualification
PYTHONPATH="$root/repo" python3.12 -m {MODULE}{flag} --stage "$root/repo" "$root"
phase=execution
systemd-run --unit=native-workspace-execution --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=7260 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$PATH" \\
 python3.12 -m scripts.check_native_workspace_execution{flag} "$CARGO_HOME/bin/cargo" "$root/repo" "$root"
phase=receipt-qualification
PYTHONPATH="$root/repo" python3.12 -m {MODULE}{flag} --check-receipt "$root"
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
'''
    if MINIMAL_ARCHIVE:
        bindings = ' '.join(archive_binding_prefix()+key.upper()+'='+str(qualification[key]) for key in ARCHIVE_IDENTITIES)
        command = command.replace('PYTHONPATH="$root/repo" python3.12 -m '+MODULE+flag+' --stage',
            bindings+' PYTHONPATH="$root/repo" python3.12 -m '+MODULE+flag+' --stage', 1)
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    body = body[:start]+command+body[end:]
    if IMPLEMENTATION and not COHERE_SQ8_BUILDER:
        body = body.replace('phase=source-qualification\n', 'rustup component add clippy --toolchain 1.98.0\nphase=source-qualification\n', 1)
    body = body.replace('/mnt/native-semantic-router-cold', '/mnt/native-workspace-execution')
    body = body.replace('python3.12 time tar gzip util-linux binutils',
                        'python3.12 python3-dev time tar gzip util-linux binutils build-essential pkg-config libssl-dev cmake')
    marker = "'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),"
    assert body.count(marker) == 1
    body = body.replace(marker, marker+"'source_qualification_sha256':artifacts.get('source-qualification.json',{}).get('sha256'),")
    subprocess.run(['bash','-n'], input=body, text=True, check=True)
    terminal = body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0]
    compile(terminal, '<terminal>', 'exec')
    assert len(body.encode()) < 16384, 'EC2 user data limit'
    return body


def poll(ec2, s3, prefix, instance_id, started):
    with patch.object(startup, 'WALL', WALL):
        return startup.poll(ec2, s3, prefix, instance_id, started)


def replay(out):
    out = Path(out)
    reservation = json.loads((out/'aws-reservation.json').read_bytes())
    closed = json.loads((out/'aws-closeout.json').read_bytes())
    terminal = json.loads((out/'aws-terminal.json').read_bytes())
    proof = reservation['qualification']
    assert proof['config_path'] == str(CONFIG) and proof['campaign_schema'] == SCHEMA, 'replay mode'
    if COHERE_SQ8_BUILDER:
        validate_cohere_sq8_builder_config(proof)
    if COHERE_COHORT:
        validate_cohere_cohort_config(proof)
    if COHERE1024:
        validate_cohere1024_config(proof)
    if BUDGET_OBJECT_FITTER:
        validate_budget_object_fitter_config(proof)
    if BUDGET_OBJECT_SELECTOR:
        validate_budget_object_selector_config(proof)
    if FINE_SQ8:
        validate_fine_sq8_config(proof)
    if CELL_OVERLAP:
        validate_cell_overlap_config(proof)
    if CONSTRAINED_SPLIT:
        validate_constrained_split_config(proof)
    assert closed['state'] == 'terminated'
    assert terminal['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert terminal['schema'] == reservation['schema'] == SCHEMA
    for key in ('source_commit','source_archive_sha256'):
        assert terminal[key] == reservation[key], 'campaign source binding'
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], 'terminal ' + key
    if MINIMAL_ARCHIVE:
        assert type(terminal['source_archive_file_count']) is int, 'terminal archive count type'
    assert set(proof['code_sha256']) == set(CODE)
    assert proof['code_identity_sha256'] == worker.sha(encoded(proof['code_sha256']))
    base = Path(__file__).resolve().parents[1]
    assert all(worker.artifact(base/name)['sha256'] == digest for name,digest in proof['code_sha256'].items())
    assert proof['artifact_roster_sha256'] == worker.sha(encoded(ARTIFACTS))
    assert type(proof['source_file_count']) is int and len(proof['source_sha256']) == proof['source_file_count'] > 0
    assert worker.source_identity(proof['source_sha256']) == proof['source_identity_sha256']
    assert SEMANTIC_1M or proof['source_identity_sha256'] == SOURCE_IDENTITY
    if MINIMAL_ARCHIVE:
        validate_hierarchical_archive(proof, proof['source_sha256'], proof['native_source_manifest']['path'])
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    for name, identity in terminal['artifacts'].items():
        assert worker.artifact(out/name) == identity, 'terminal artifact: ' + name
    for name,key in (('config.json','config_sha256'),('native-source-manifest.json','native_source_manifest_sha256')):
        if name in terminal['artifacts']:
            assert worker.artifact(out/name)['sha256'] == proof[key], 'saved authority: ' + name
    if COHERE_SQ8_BUILDER:
        if 'config.json' in terminal['artifacts']:
            assert json.loads((out/'config.json').read_bytes())['completed_native_qualification'] == proof['completed_native_qualification'], 'saved completed native qualification binding'
        if 'completed-native-qualification.json' in terminal['artifacts']:
            completed_native_qualification(out, proof, retained=True)
    if 'source-qualification.json' in terminal['artifacts']:
        assert json.loads((out/'source-qualification.json').read_bytes()) == proof
    assert terminal['source_qualification_sha256'] == terminal['artifacts'].get('source-qualification.json',{}).get('sha256')
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    complete = terminal['exit_code'] == 0 and terminal['phase'] == 'complete'
    assert terminal['status'] == ('complete' if complete else 'failed')
    if complete:
        assert terminal['original_exit_code'] == 0
        assert set(terminal['artifacts']) == set(ARTIFACTS), 'exact completed artifact roster'
        validate_receipt(out, proof)
    result = dict(qualified=complete, actual_full_workspace_execution=complete and not (TEST_BUILD or IMPLEMENTATION),
                  source_identity_sha256=proof['source_identity_sha256'], exit_status=terminal['original_exit_code'])
    if TEST_BUILD:
        assert proof['execution_kind'] == FIXED['execution_kind']
        result.update(execution_kind=FIXED['execution_kind'], actual_workspace_test_build=complete)
    if IMPLEMENTATION:
        assert proof['execution_kind'] == FIXED['execution_kind']
        result.update(execution_kind=FIXED['execution_kind'])
    return result


def collect(s3, prefix, out, instance_id, commit, digest):
    out = Path(out)
    closed = json.loads((out/'aws-closeout.json').read_bytes())
    assert closed['state'] == 'terminated' and instance_id in {n['instance_id'] for n in closed['nodes'].values()}
    raw = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/terminal.json')['Body'].read()
    (out/'aws-terminal.json').write_bytes(raw)
    terminal = json.loads(raw)
    assert terminal['schema'] == SCHEMA and terminal['instance_id'] == instance_id
    assert terminal['source_commit'] == commit and terminal['source_archive_sha256'] == digest
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    for name, identity in terminal['artifacts'].items():
        source = s3.get_object(Bucket=peer.BUCKET, Key=prefix+'/artifacts/'+name)['Body']
        (out/name).parent.mkdir(parents=True, exist_ok=True)
        with (out/name).open('wb') as output, gzip.GzipFile(filename=str(out/(name+'.gz')), mode='wb', mtime=0) as archived:
            for chunk in iter(lambda:source.read(1024*1024), b''):
                output.write(chunk); archived.write(chunk)
        assert worker.artifact(out/name) == identity, 'downloaded artifact: ' + name
        if IMPLEMENTATION and name in RELEASE_ARTIFACTS:
            (out/name).chmod(0o755)
    result = replay(out)
    (out/'collection-replay.json').write_bytes(encoded(dict(terminal_sha256=worker.sha(raw),result=result))+b'\n')
    return terminal


def main(attempt):
    return shared.main(attempt, campaign=sys.modules[__name__])


def _worker_self_check(proof, config_body, manifest_body, completed_native_body=None):
    """Mock only Cargo/cgroup; test the real command, receipt and source gate."""
    from scripts import launch_native_workspace_execution_spot as authority
    inventory = proof['source_sha256']
    counters = {'memory.max': str(worker.MEMORY), 'memory.peak': '10000',
        'memory.swap.max': '0', 'memory.swap.peak': '0', 'memory.swap.events': 'max 0\nfail 0\n',
        'memory.events': 'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n',
        'cpu.max': '200000 100000', 'cpu.stat': 'usage_usec 42\n',
        'pids.max': '512', 'pids.current': '1', 'pids_peak': '4', 'pids.events': 'max 0\n',
        'observer_pid': 42, 'process_ids': [42]}
    saved = None
    failures = ('success', 'exit17', 'timeout', 'source-drift', 'reclaim', 'oom', 'peak', 'orphan', 'log-fsync')
    if IMPLEMENTATION:
        failures += ('missing-binary', 'bad-binary', 'symlink-binary', 'copy-failure')
    if BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
        failures += ('missing-stage', 'zero-stage-record', 'stage-order', 'stage-bool', 'missing-cap-proof')
    if COHERE_SQ8_BUILDER:
        failures += ('swap', 'cpu', 'tasks', 'qualification-body', 'qualification-missing')
    if CONSTRAINED_SPLIT or CELL_OVERLAP or FINE_SQ8 or PQ_RESIDUAL or CO_SELECTION or BUDGET_OBJECT_SELECTOR or BUDGET_OBJECT_FITTER or COHERE1024 or COHERE_COHORT or COHERE_SQ8_BUILDER:
        failures += ('gate-failure', 'tee-failure', 'ignored-tests')
    if EXACT_SQ8_RUNTIME:
        failures += ('probe-parity', 'probe-missing')
    for failure in failures:
        with tempfile.TemporaryDirectory() as tmp:
            repo, out = Path(tmp)/'repo', Path(tmp)/'out'
            repo.mkdir(); out.mkdir()
            (out/'source-qualification.json').write_bytes(encoded(proof))
            (out/'config.json').write_bytes(config_body)
            (out/'native-source-manifest.json').write_bytes(manifest_body)
            (repo/CONFIG).parent.mkdir(parents=True)
            (repo/CONFIG).write_bytes(config_body)
            manifest_path = repo/proof['native_source_manifest']['path']
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            manifest_path.write_bytes(manifest_body)
            if COHERE_SQ8_BUILDER:
                source = repo/proof['completed_native_qualification']['path']
                source.parent.mkdir(parents=True, exist_ok=True)
                source.write_bytes(completed_native_body)
                if failure != 'qualification-missing':
                    (out/'completed-native-qualification.json').write_bytes(b'{}' if failure == 'qualification-body' else completed_native_body)
            changed = dict(inventory, **{'Cargo.toml': '0'*64}) if failure == 'source-drift' else inventory
            after = copy.deepcopy(counters)
            if failure == 'oom':
                after['memory.events'] = counters['memory.events'].replace('oom 0','oom 1').replace('oom_kill 0','oom_kill 1')
            if failure == 'reclaim':
                after['memory.events'] = counters['memory.events'].replace('max 0','max 174')
            if failure == 'peak':
                after['memory.peak'] = str(worker.MEMORY+1)
            if failure == 'orphan':
                after['process_ids'] = [42,43]
            if failure == 'swap':
                after['memory.swap.peak'] = '1'
            if failure == 'cpu':
                after['cpu.max'] = '300000 100000'
            if failure == 'tasks':
                after['pids.max'] = '513'
            calls = []
            persist_failure = [False]
            def run(args, **kw):
                if args[0] != '/usr/bin/time':
                    return subprocess.CompletedProcess(args, 0, b'actual mocked compiler version\n')
                calls.append(args)
                expected_command = FIXED['command'] if TEST_BUILD or IMPLEMENTATION else ['fake-cargo', *worker.COMMAND[1:]]
                assert args[args.index('7200')+1:] == expected_command
                assert args[args.index('--kill-after=30')+1] == '7200'
                assert kw['cwd'] == repo.resolve()
                assert all(kw['env'].get(k) == v for k,v in FIXED['environment'].items())
                if TEST_BUILD or IMPLEMENTATION:
                    assert 'BORSUK_TEST_BUILD_COMMAND' not in kw['env']
                assert not Path(kw['env']['CARGO_TARGET_DIR']).is_relative_to(repo)
                kw['stdout'].write(b'full mocked cargo log\n')
                if BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
                    stages = list(COHERE_SQ8_BUILDER_STAGES if COHERE_SQ8_BUILDER else COHERE_COHORT_STAGES if COHERE_COHORT else COHERE1024_STAGES if COHERE1024 else BUDGET_OBJECT_FITTER_STAGES if BUDGET_OBJECT_FITTER else BUDGET_OBJECT_SELECTOR_STAGES if BUDGET_OBJECT_SELECTOR else CO_SELECTION_STAGES if CO_SELECTION else PQ_RESIDUAL_STAGES if PQ_RESIDUAL else FINE_SQ8_STAGES if FINE_SQ8 else CELL_OVERLAP_STAGES if CELL_OVERLAP else CONSTRAINED_SPLIT_STAGES if CONSTRAINED_SPLIT else HIERARCHICAL_CELLS_STAGES if HIERARCHICAL_CELLS else FIXED48_STAGES if FIXED48 else BOUNDED_PUBLICATION_STAGES)
                    if failure == 'stage-order':
                        stages = [(stages[0][0], ['wrong-command'])] if COHERE_SQ8_BUILDER else list(reversed(stages))
                    for index, (name, command) in enumerate(stages):
                        if failure == 'missing-stage' and index == len(stages)-1:
                            continue
                        record = dict(schema=COHERE_SQ8_BUILDER_STAGE_SCHEMA if COHERE_SQ8_BUILDER else COHERE_COHORT_STAGE_SCHEMA if COHERE_COHORT else COHERE1024_STAGE_SCHEMA if COHERE1024 else BUDGET_OBJECT_FITTER_STAGE_SCHEMA if BUDGET_OBJECT_FITTER else BUDGET_OBJECT_SELECTOR_STAGE_SCHEMA if BUDGET_OBJECT_SELECTOR else CO_SELECTION_STAGE_SCHEMA if CO_SELECTION else PQ_RESIDUAL_STAGE_SCHEMA if PQ_RESIDUAL else FINE_SQ8_STAGE_SCHEMA if FINE_SQ8 else CELL_OVERLAP_STAGE_SCHEMA if CELL_OVERLAP else CONSTRAINED_SPLIT_STAGE_SCHEMA if CONSTRAINED_SPLIT else HIERARCHICAL_CELLS_STAGE_SCHEMA if HIERARCHICAL_CELLS else FIXED48_STAGE_SCHEMA if FIXED48 else BOUNDED_PUBLICATION_STAGE_SCHEMA, stage=name,
                            command=command, started_at='2026-10-02T00:00:00Z',
                            finished_at=None, exit_status=None, gate_status=None, tests_run=None)
                        field = 'required_test_passes' if FIXED48 or MINIMAL_ARCHIVE else 'publication_cap_test_passed'
                        record[field] = None
                        if BUDGET_OBJECT_SELECTOR or BUDGET_OBJECT_FITTER or COHERE1024 or COHERE_COHORT or COHERE_SQ8_BUILDER:
                            record['resources'] = None
                        if CONSTRAINED_SPLIT or CELL_OVERLAP or FINE_SQ8 or PQ_RESIDUAL or CO_SELECTION or BUDGET_OBJECT_SELECTOR or BUDGET_OBJECT_FITTER or COHERE1024 or COHERE_COHORT or COHERE_SQ8_BUILDER:
                            record['log_exit_status'] = None
                        kw['stdout'].write(encoded(record)+b'\n')
                        named = ({} if COHERE_SQ8_BUILDER else cohere_cohort_required_tests() if COHERE_COHORT else cohere1024_required_tests() if COHERE1024 else budget_object_fitter_required_tests() if BUDGET_OBJECT_FITTER else budget_object_selector_required_tests() if BUDGET_OBJECT_SELECTOR else co_selection_required_tests() if CO_SELECTION else pq_residual_required_tests() if PQ_RESIDUAL else fine_sq8_required_tests() if FINE_SQ8 else cell_overlap_required_tests() if CELL_OVERLAP else constrained_split_required_tests() if CONSTRAINED_SPLIT else HIERARCHICAL_CELLS_REQUIRED_TESTS if HIERARCHICAL_CELLS else FIXED48_REQUIRED_TESTS).get(name, ()) if FIXED48 or MINIMAL_ARCHIVE else ()
                        if FIXED48 or MINIMAL_ARCHIVE:
                            if failure != 'missing-cap-proof':
                                for test in named:
                                    kw['stdout'].write(('test '+test+' ... ok\n').encode())
                            if (CELL_OVERLAP or FINE_SQ8 or PQ_RESIDUAL or CO_SELECTION or BUDGET_OBJECT_SELECTOR or BUDGET_OBJECT_FITTER or COHERE1024 or COHERE_COHORT) and not named and name not in NON_TEST_STAGES:
                                kw['stdout'].write(b'test tests::regression ... ok\n')
                            if name not in NON_TEST_STAGES:
                                kw['stdout'].write(f'test result: ok. {max(1, len(named))} passed; 0 failed; {int(failure == "ignored-tests")} ignored; 0 measured; 100 filtered out; finished in 0.00s\n'.encode())
                        elif index == 1 and failure != 'missing-cap-proof':
                            kw['stdout'].write(('test '+BOUNDED_PUBLICATION_CAP_TEST+' ... ok\n').encode())
                        if EXACT_SQ8_RUNTIME and name == 'exact-sq8-probe' and failure != 'probe-missing':
                            for dimensions, rows_ in sorted(EXACT_SQ8_PROBE_SHAPES):
                                kw['stdout'].write(encoded(dict(dimensions=dimensions, rows=rows_, candidate_over_baseline_median=1.0,
                                    identical_ids_ordinals_score_bits=not (failure == 'probe-parity' and (dimensions, rows_) == (128, 1))))+b'\n')
                        if MINIMAL_ARCHIVE and name == 'test-build':
                            kw['stdout'].write(b'rust-test-build status=0 elapsed_seconds=0 jobs=1\n')
                        record.update(finished_at=record['started_at'], exit_status=0, gate_status=0,
                            tests_run=(0 if failure == 'zero-stage-record' else False if failure == 'stage-bool' else max(1, len(named))) if name not in NON_TEST_STAGES else None)
                        if CONSTRAINED_SPLIT or CELL_OVERLAP or FINE_SQ8 or PQ_RESIDUAL or CO_SELECTION or BUDGET_OBJECT_SELECTOR or BUDGET_OBJECT_FITTER or COHERE1024 or COHERE_COHORT or COHERE_SQ8_BUILDER:
                            record['gate_status'] = 96 if failure == 'gate-failure' else 0
                            record['log_exit_status'] = 18 if failure == 'tee-failure' else 0
                        if BUDGET_OBJECT_SELECTOR or BUDGET_OBJECT_FITTER or COHERE1024 or COHERE_COHORT or COHERE_SQ8_BUILDER:
                            record['resources'] = 'User time (seconds): 0.01\nSystem time (seconds): 0.01\nElapsed (wall clock) time (h:mm:ss or m:ss): 0:00.02\nMaximum resident set size (kbytes): 10000\nExit status: 0\n'
                        if COHERE_SQ8_BUILDER and failure in ('zero-stage-record', 'stage-bool'):
                            record['tests_run'] = 0 if failure == 'zero-stage-record' else False
                        if COHERE_SQ8_BUILDER and failure == 'ignored-tests':
                            kw['stdout'].write(b'test tests::unexpected ... ignored\n')
                        record[field] = {test: int(failure != 'missing-cap-proof') for test in named} if FIXED48 or MINIMAL_ARCHIVE else (failure != 'missing-cap-proof') if index == 1 else None
                        if COHERE_SQ8_BUILDER and failure == 'missing-cap-proof':
                            record[field] = {'invented_test': 1}
                        kw['stdout'].write(encoded(record)+b'\n')
                Path(args[args.index('-o')+1]).write_text('GNU time mocked resources\n')
                if IMPLEMENTATION:
                    for name in RELEASE_ARTIFACTS:
                        source = Path(kw['env']['CARGO_TARGET_DIR'])/'release'/('examples' if name.endswith(EXAMPLE_BINARIES) else '')/Path(name).name
                        source.parent.mkdir(parents=True, exist_ok=True)
                        if failure == 'missing-binary' and name == RELEASE_ARTIFACTS[-1]:
                            continue
                        if failure == 'symlink-binary' and name == RELEASE_ARTIFACTS[-1]:
                            source.symlink_to(source.with_name('build_two_bit_generation'))
                        else:
                            source.write_bytes(b'bad binary' if failure == 'bad-binary' else b'\x7fELF mocked release '+name.encode())
                            source.chmod(0o755)
                persist_failure[0] = failure == 'log-fsync'
                return subprocess.CompletedProcess(args, {'exit17':17, 'timeout':124}.get(failure,0))
            def fsync(fd):
                if persist_failure[0]:
                    persist_failure[0] = False
                    raise OSError('test log persistence failed')
            with patch.object(authority, 'qualify', return_value=proof), \
                    patch.object(worker, 'source_hashes', side_effect=[inventory, changed]), \
                    patch.object(worker, 'capture_cgroup', side_effect=[counters, after]), \
                    patch.object(worker.subprocess, 'run', side_effect=run), patch.object(os,'fsync',side_effect=fsync), \
                    contextlib.ExitStack() as patches:
                if failure == 'copy-failure':
                    patches.enter_context(patch.object(worker.shutil, 'copyfileobj', side_effect=OSError('binary copy failed')))
                previous = authority.SEMANTIC_1M, authority.TEST_BUILD, authority.IMPLEMENTATION, authority.STARTUP_WAVE8, authority.ROOT_REUSE, authority.BOUNDED_PUBLICATION, authority.FIXED48, authority.HIERARCHICAL_CELLS, authority.CONSTRAINED_SPLIT, authority.CELL_OVERLAP, authority.FINE_SQ8, authority.PQ_RESIDUAL, authority.CO_SELECTION, authority.CONFIG, authority.CODE, authority.FIXED
                if failure in ('qualification-body', 'qualification-missing'):
                    rejected(lambda:worker.main('cargo', repo, out, cohere_sq8_builder=True))
                    assert not calls and not (out/'target').exists(), 'invalid qualification reached Cargo/target'
                    continue
                result = worker.main('cargo' if TEST_BUILD or IMPLEMENTATION else 'fake-cargo', repo, out,
                                     semantic_1m=SEMANTIC_1M, test_build=TEST_BUILD, implementation=IMPLEMENTATION, startup_wave8=STARTUP_WAVE8, root_reuse=ROOT_REUSE, bounded_publication=BOUNDED_PUBLICATION, fixed48=FIXED48, hierarchical_cells=HIERARCHICAL_CELLS, constrained_split=CONSTRAINED_SPLIT, cell_overlap=CELL_OVERLAP, fine_sq8=FINE_SQ8, pq_residual=PQ_RESIDUAL, co_selection=CO_SELECTION, budget_object_selector=BUDGET_OBJECT_SELECTOR, budget_object_fitter=BUDGET_OBJECT_FITTER, cohere1024=COHERE1024, cohere_cohort=COHERE_COHORT and not (EXACT_SQ8_RUNTIME or RETAINED_GENERATION_REBIND or BASELINE_POOL), cohere_sq8_builder=COHERE_SQ8_BUILDER, exact_sq8_runtime=EXACT_SQ8_RUNTIME, retained_generation_rebind=RETAINED_GENERATION_REBIND, baseline_pool=BASELINE_POOL)
                assert previous == (authority.SEMANTIC_1M, authority.TEST_BUILD, authority.IMPLEMENTATION, authority.STARTUP_WAVE8, authority.ROOT_REUSE, authority.BOUNDED_PUBLICATION, authority.FIXED48, authority.HIERARCHICAL_CELLS, authority.CONSTRAINED_SPLIT, authority.CELL_OVERLAP, authority.FINE_SQ8, authority.PQ_RESIDUAL, authority.CO_SELECTION, authority.CONFIG, authority.CODE, authority.FIXED)
            assert len(calls) == 1, 'full test repeated'
            assert type(result['exit_status']) is int
            assert result['exit_status'] == {'exit17':17, 'timeout':124}.get(failure,0)
            assert result['qualified'] == (failure in ('success','reclaim')), failure
            assert result['source_unchanged'] == (failure != 'source-drift')
            if IMPLEMENTATION and failure in ('exit17','timeout','log-fsync'):
                assert not (out/'binaries').exists(), 'copied binaries after failed pipeline'
            assert json.loads((out/'workspace-receipt.json').read_bytes()) == result
            if failure in ('success','reclaim'):
                validate_receipt(out, proof)
                if failure == 'success':
                    saved = {name:(out/name).read_bytes() for name in ARTIFACTS if (out/name).is_file()}
                    if TEST_BUILD or IMPLEMENTATION:
                        for changes in (dict(schema='borsuk-native-workspace-execution-receipt-v1'),
                                        dict(exit_status=False), dict(gate_status=False),
                                        dict(actual_full_workspace_execution=True),
                                        dict(execution_kind='wrong-mode'),
                                        dict(command=list(worker.COMMAND)),
                                        dict(environment=dict(FIXED['environment'], BORSUK_TEST_BUILD_COMMAND='true'))):
                            (out/'workspace-receipt.json').write_bytes(encoded(dict(result, **changes)))
                            rejected(lambda: validate_receipt(out, proof))
                        (out/'workspace-receipt.json').write_bytes(encoded(result))
                    if IMPLEMENTATION:
                        assert set(result['artifacts']) & set(RELEASE_ARTIFACTS) == set(RELEASE_ARTIFACTS)
                        for name in RELEASE_ARTIFACTS:
                            data = (out/name).read_bytes()
                            source = out/'target/release'/('examples' if name.endswith(EXAMPLE_BINARIES) else '')/Path(name).name
                            assert worker.artifact(out/name) == worker.artifact(source)
                            assert (out/name).stat().st_mode & 0o111
                            (out/name).write_bytes(b'tampered binary')
                            rejected(lambda: validate_receipt(out, proof))
                            (out/name).write_bytes(data)
                            altered = copy.deepcopy(result)
                            del altered['artifacts'][name]
                            (out/'workspace-receipt.json').write_bytes(encoded(altered))
                            rejected(lambda: validate_receipt(out, proof))
                            (out/'workspace-receipt.json').write_bytes(encoded(result))
                    if EXACT_SQ8_RUNTIME:
                        original_probe = (out/EXACT_SQ8_PROBE_ARTIFACT).read_bytes()
                        assert original_probe == probe_lines((out/'test.log').read_bytes()) and EXACT_SQ8_PROBE_ARTIFACT in result['artifacts']
                        for changed_probe in (b'', original_probe.splitlines(True)[0], original_probe+original_probe):
                            (out/EXACT_SQ8_PROBE_ARTIFACT).write_bytes(changed_probe)
                            rejected(lambda: validate_receipt(out, proof))
                        (out/EXACT_SQ8_PROBE_ARTIFACT).write_bytes(original_probe)
                        altered = copy.deepcopy(result)
                        del altered['artifacts'][EXACT_SQ8_PROBE_ARTIFACT]
                        (out/'workspace-receipt.json').write_bytes(encoded(altered))
                        rejected(lambda: validate_receipt(out, proof))
                        (out/'workspace-receipt.json').write_bytes(encoded(result))
                    if COHERE_SQ8_BUILDER:
                        (out/'workspace-receipt.json').write_bytes(encoded(dict(result, completed_native_qualification={})))
                        rejected(lambda:validate_receipt(out, proof))
                        (out/'workspace-receipt.json').write_bytes(encoded(result))
                        (out/'completed-native-qualification.json').write_bytes(b'{}')
                        rejected(lambda:validate_receipt(out, proof))
                        (out/'completed-native-qualification.json').write_bytes(completed_native_body)
                    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
                        for changed in (dict(controller_source_commit='0'*40), dict(candidate_delta_paths=['extra.rs'] if COHERE_SQ8_BUILDER else [])):
                            (out/'workspace-receipt.json').write_bytes(encoded(dict(result, **changed)))
                            rejected(lambda:validate_receipt(out, proof))
                        (out/'workspace-receipt.json').write_bytes(encoded(result))
                    if CONSTRAINED_SPLIT:
                        for key,value in (('mandatory_test_names_pending', True), ('mandatory_tests', {}),
                                          ('control_module_prefix', {}), ('control_native_source_commit', '0'*40)):
                            (out/'workspace-receipt.json').write_bytes(encoded(dict(result, **{key:value})))
                            rejected(lambda:validate_receipt(out, proof))
                        (out/'workspace-receipt.json').write_bytes(encoded(result))
                    if CELL_OVERLAP or FINE_SQ8 or PQ_RESIDUAL or CO_SELECTION or BUDGET_OBJECT_SELECTOR or BUDGET_OBJECT_FITTER or COHERE1024 or COHERE_COHORT or COHERE_SQ8_BUILDER:
                        for key,value in (('mandatory_test_names_pending', True), ('mandatory_tests', {'invented': ['test']} if COHERE_SQ8_BUILDER else {})):
                            (out/'workspace-receipt.json').write_bytes(encoded(dict(result, **{key:value})))
                            rejected(lambda:validate_receipt(out, proof))
                        (out/'workspace-receipt.json').write_bytes(encoded(result))
                    if BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
                        assert len(result['stages']) == (1 if COHERE_SQ8_BUILDER else len(COHERE_COHORT_STAGES) if COHERE_COHORT else len(COHERE1024_STAGES) if COHERE1024 else len(BUDGET_OBJECT_FITTER_STAGES) if BUDGET_OBJECT_FITTER else len(BUDGET_OBJECT_SELECTOR_STAGES) if BUDGET_OBJECT_SELECTOR else len(CO_SELECTION_STAGES) if CO_SELECTION else len(PQ_RESIDUAL_STAGES) if PQ_RESIDUAL else len(FINE_SQ8_STAGES) if FINE_SQ8 else 7 if CELL_OVERLAP else 6 if MINIMAL_ARCHIVE else 7) and all(record['tests_run'] > 0 for record in result['stages'] if record['stage'] not in NON_TEST_STAGES)
                        for stages in ([], result['stages'][:-1], result['stages']*2 if COHERE_SQ8_BUILDER else list(reversed(result['stages']))):
                            (out/'workspace-receipt.json').write_bytes(encoded(dict(result, stages=stages)))
                            rejected(lambda:validate_receipt(out, proof))
                        (out/'workspace-receipt.json').write_bytes(encoded(result))
                (out/'test.log').write_bytes(b'tampered')
                rejected(lambda: validate_receipt(out, proof))
            else:
                assert result['gate_status'] != 0, 'failure gate lost'
                rejected(lambda: validate_receipt(out, proof))
    return dict(saved, **{'run-closed.log': b'closed mocked worker\n'})


def rejected(call):
    try:
        call()
    except (AssertionError, ValueError):
        return
    raise AssertionError('tampered or failed authority accepted')


def _lifecycle_self_check(proof=None):
    from datetime import datetime, timezone
    from unittest.mock import Mock
    module = sys.modules[__name__]
    for failure in ('success','multi-ack','multi-ack-fsync','upload','interrupt', 'wait-failure'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2,s3]
            ec2.describe_instances.return_value = {'Reservations':[]}
            ec2.describe_subnets.return_value = {'Subnets':[{'AvailabilityZone':'mock-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory':[
                {'SpotPrice':'.1','Timestamp':datetime.now(timezone.utc)}]}
            ids = ['i-owned','i-extra'] if failure.startswith('multi-ack') else ['i-owned']
            ec2.run_instances.return_value = {'Instances':[{'InstanceId':n} for n in ids]}
            events = []
            ec2.terminate_instances.side_effect = lambda **kw:events.append('terminate')
            def waited(**kw):
                events.append('wait')
                if failure == 'wait-failure':
                    raise OSError('termination waiter failed')
            ec2.get_waiter.return_value.wait.side_effect = waited
            def collected(*args):
                assert events == ['terminate','wait'], 'collection before termination waiter'
                events.append('collect')
                return dict(status='complete',phase='complete',exit_code=0,artifacts={n:{} for n in ARTIFACTS})
            with patch.object(module,'ROOT',Path(tmp)), patch.object(module,'preflight',return_value=proof or {'config_sha256':'a'*64}), \
                    patch.object(module,'user_data',return_value='mock'), patch.object(module,'collect',side_effect=collected), \
                    patch.object(module,'poll',side_effect=KeyboardInterrupt() if failure == 'interrupt' else None), \
                    patch.object(shared.boto3,'Session',return_value=session), \
                    patch.object(subprocess,'check_output',side_effect=['','0'*40]), \
                    patch.object(shared,'source_archive',return_value=b'mocked archive') as archive, \
                    patch.object(peer,'missing',return_value=True), \
                    patch.object(peer,'put_if_absent',side_effect=[None,None,OSError('upload')] if failure == 'upload' else [None]*3), \
                    patch.object(os,'fsync',side_effect=OSError('persist') if failure.endswith('fsync') else None), \
                    contextlib.redirect_stdout(io.StringIO()):
                try:
                    main('a0001')
                except (OSError,KeyboardInterrupt):
                    assert failure not in ('success','multi-ack')
                else:
                    assert failure in ('success','multi-ack'), 'failure swallowed'
            archive.assert_called_once_with('0'*40, proof['source_archive_paths']) if proof and MINIMAL_ARCHIVE else archive.assert_called_once_with('0'*40)
            if proof and MINIMAL_ARCHIVE:
                reserved = json.loads((Path(tmp)/'a0001/aws-reservation.json').read_bytes())
                assert all(reserved['qualification'][key] == proof[key] for key in ARCHIVE_FIELDS)
            ec2.run_instances.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            assert events == (['terminate','wait'] if failure == 'wait-failure' else ['terminate','wait','collect'])
            assert ec2.run_instances.call_args.kwargs['InstanceType'] == INSTANCE_TYPE
            assert ec2.run_instances.call_args.kwargs['ImageId'] == IMAGE_ID
            assert ec2.run_instances.call_args.kwargs['BlockDeviceMappings'][0]['DeviceName'] == ROOT_DEVICE_NAME
            if failure == 'wait-failure':
                assert not (Path(tmp)/'a0001/aws-closeout.json').exists(), 'closeout before confirmed termination'
            for name in (('aws-launch.json',) if failure == 'wait-failure' else ('aws-launch.json','aws-closeout.json')):
                assert json.loads((Path(tmp)/'a0001'/name).read_bytes())['nodes'] == {
                    str(i):dict(instance_id=n) for i,n in enumerate(ids)}
    with contextlib.redirect_stdout(io.StringIO()):
        shared.self_check(lifecycle_only=True)


def _collection_self_check(proof, files, body):
    from unittest.mock import Mock
    terminal_script = body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0]
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp)
        for name,data in files.items():
            (out/name).parent.mkdir(parents=True, exist_ok=True)
            (out/name).write_bytes(data)
        env = dict(os.environ, INSTANCE_ID='i-owned', EXIT_CODE='0', ORIGINAL_EXIT_CODE='0',
                   PHASE='complete', ARTIFACT_NAMES=' '.join(ARTIFACTS))
        terminal = json.loads(subprocess.check_output([sys.executable,'-c',terminal_script], cwd=out, env=env))
        assert set(terminal['artifacts']) == set(ARTIFACTS)
        reservation = dict(schema=SCHEMA, qualification=proof, source_commit='0'*40,source_archive_sha256='1'*64)
        (out/'aws-reservation.json').write_bytes(encoded(reservation))
        (out/'aws-closeout.json').write_bytes(encoded(dict(state='terminated',nodes={'0':dict(instance_id='i-owned')})))
        store = {PREFIX+'a0001/terminal.json':encoded(terminal),
                 **{PREFIX+'a0001/artifacts/'+name:data for name,data in files.items()}}
        s3 = Mock()
        s3.get_object.side_effect = lambda **kw: {'Body':io.BytesIO(store[kw['Key']])}
        closed_body = (out/'aws-closeout.json').read_bytes()
        for failed_cleanup in (dict(state='running', nodes={'0':dict(instance_id='i-owned')}),
                               dict(state='terminated', nodes={'0':dict(instance_id='i-other')})):
            (out/'aws-closeout.json').write_bytes(encoded(failed_cleanup))
            rejected(lambda:collect(s3, PREFIX+'a0001', out, 'i-owned', '0'*40, '1'*64))
            s3.get_object.assert_not_called()
        (out/'aws-closeout.json').write_bytes(closed_body)
        collect(s3, PREFIX+'a0001', out, 'i-owned', '0'*40, '1'*64)
        replayed = replay(out)
        assert replayed['qualified'] is True
        assert replayed['actual_full_workspace_execution'] is (not (TEST_BUILD or IMPLEMENTATION))
        if TEST_BUILD:
            assert replayed['actual_workspace_test_build'] is True
        if COHERE_SQ8_BUILDER:
            changed_proof = dict(proof, completed_native_qualification=dict(proof['completed_native_qualification'], sha256='0'*64))
            (out/'aws-reservation.json').write_bytes(encoded(dict(reservation, qualification=changed_proof)))
            rejected(lambda:replay(out))
            (out/'aws-reservation.json').write_bytes(encoded(reservation))
            (out/'completed-native-qualification.json').write_bytes(b'{}')
            rejected(lambda:replay(out))
            (out/'completed-native-qualification.json').write_bytes(files['completed-native-qualification.json'])
        for name,data in files.items():
            assert gzip.decompress((out/(name+'.gz')).read_bytes()) == data
        mutations = [dict(terminal,config_sha256='0'*64), dict(terminal,instance_id='i-other'),
                     dict(terminal,source_archive_sha256='2'*64), dict(terminal,exit_code=False),
                     dict(terminal,artifacts={n:v for n,v in terminal['artifacts'].items() if n != 'test.log'})]
        if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
            mutations.extend((dict(terminal,controller_source_commit='0'*40),dict(terminal,candidate_delta_paths=['extra.rs'] if COHERE_SQ8_BUILDER else [])))
        if MINIMAL_ARCHIVE:
            mutations.extend(dict(terminal, **{key: value}) for key,value in
                             (('source_archive_paths_sha256', '0'*64), ('source_archive_file_count', True),
                              ('source_archive_file_count', proof['source_archive_file_count']+1),
                              ('source_archive_file_count', float(proof['source_archive_file_count']))))
        if IMPLEMENTATION:
            downloaded = out/'downloaded'
            downloaded.mkdir()
            for name in ('aws-reservation.json','aws-closeout.json'):
                (downloaded/name).write_bytes((out/name).read_bytes())
            collect(s3,PREFIX+'a0001',downloaded,'i-owned','0'*40,'1'*64)
            assert replay(downloaded)['qualified'] is True
            for name in RELEASE_ARTIFACTS:
                assert (downloaded/name).read_bytes() == gzip.decompress((downloaded/(name+'.gz')).read_bytes()) == files[name]
                assert (downloaded/name).stat().st_mode & 0o111
            name = RELEASE_ARTIFACTS[0]
            mutations.append(dict(terminal,artifacts={n:v for n,v in terminal['artifacts'].items() if n != name}))
            store[PREFIX+'a0001/artifacts/'+name] = b'tampered binary download'
            rejected(lambda:collect(s3,PREFIX+'a0001',downloaded,'i-owned','0'*40,'1'*64))
            store[PREFIX+'a0001/artifacts/'+name] = files[name]
        for changed in mutations:
            (out/'aws-terminal.json').write_bytes(encoded(changed))
            rejected(lambda:replay(out))
        (out/'aws-terminal.json').write_bytes(encoded(terminal))
        (out/'test.log').write_bytes(b'tampered log')
        rejected(lambda:replay(out))
        store[PREFIX+'a0001/artifacts/test.log'] = b'tampered download'
        rejected(lambda:collect(s3,PREFIX+'a0001',out,'i-owned','0'*40,'1'*64))
        store[PREFIX+'a0001/artifacts/test.log'] = files['test.log']
        failed = dict(terminal,exit_code=17,original_exit_code=17,status='failed',phase='execution')
        store[PREFIX+'a0001/terminal.json'] = encoded(failed)
        collect(s3,PREFIX+'a0001',out,'i-owned','0'*40,'1'*64)
        expected = dict(qualified=False,actual_full_workspace_execution=False,
                        source_identity_sha256=proof['source_identity_sha256'],exit_status=17)
        if TEST_BUILD:
            expected.update(execution_kind=FIXED['execution_kind'], actual_workspace_test_build=False)
        if IMPLEMENTATION:
            expected.update(execution_kind=FIXED['execution_kind'])
        assert replay(out) == expected
        assert (out/'test.log').read_bytes() == files['test.log'], 'failed logs lost'
        if IMPLEMENTATION:
            partial = out/'failed-subset'
            partial.mkdir()
            for name in ('aws-reservation.json','aws-closeout.json'):
                (partial/name).write_bytes((out/name).read_bytes())
            failed['artifacts'] = {n:v for n,v in terminal['artifacts'].items() if n not in RELEASE_ARTIFACTS}
            store[PREFIX+'a0001/terminal.json'] = encoded(failed)
            collect(s3,PREFIX+'a0001',partial,'i-owned','0'*40,'1'*64)
            assert replay(partial) == expected and not (partial/'binaries').exists()


def _remote_self_check(native_manifest=None, config_draft=None, manifest_body=None):
    """Run both CLIs away from the repo, faking only Cargo and cgroup files."""
    import shutil
    base = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        repo = root/'repo'
        for name in CODE:
            (repo/name).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(base/name, repo/name)
        if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48:
            for name in native_manifest['source_sha256']:
                path = repo/name
                path.parent.mkdir(parents=True, exist_ok=True)
                body = (base/name).read_bytes()
                if worker.sha(body) != native_manifest['source_sha256'][name]:
                    body = subprocess.check_output(['git','show',native_manifest['native_source_commit']+':'+name], cwd=base)
                path.write_bytes(body)
        else:
            for index in range(399):
                (repo/f'mock-{index}.rs').write_text('// mock native source\n')
        inventory = worker.source_hashes(repo)
        manifest_path = ROOT/'native-source-manifest.json'
        (repo/manifest_path).parent.mkdir(parents=True)
        manifest = native_manifest if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 else dict(source_sha256=inventory, source_file_count=399,
            source_identity_sha256=worker.source_identity(inventory), native_source_commit='1'*40)
        assert manifest['source_sha256'] == inventory
        if manifest_body is None:
            manifest_body = encoded(manifest)
        assert json.loads(manifest_body) == manifest
        (repo/manifest_path).write_bytes(manifest_body)
        config = dict(FIXED if config_draft is None else config_draft, controller_authority_pending=False,
            controller_code_sha256={n:worker.artifact(repo/n)['sha256'] for n in CODE},
            native_source_manifest=dict(path=str(manifest_path), **worker.artifact(repo/manifest_path)))
        if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48:
            config['controller_source_commit'] = '4'*40
        if config_draft is not None:
            assert config_draft['native_source_manifest'] == config['native_source_manifest'], 'actual draft manifest pointer'
            (repo/CONFIG).write_bytes(encoded(config_draft))
            rejected(lambda:qualify(repo))  # The supplied pending authority cannot launch.
            # Exercise the actual freeze/bundle Git chain entirely in this temporary repo.
            candidate = {name:(repo/name).read_bytes() for name in NATIVE_DELTA}
            for name in NATIVE_DELTA:
                (repo/name).write_bytes(subprocess.check_output(['git','show',manifest['control_native_source_commit']+':'+name], cwd=base))
            def git(*args):
                return subprocess.check_output(['git','-c','core.hooksPath=/dev/null','-c','commit.gpgsign=false',*args], cwd=repo).decode().strip()
            git('init','-q')
            common = subprocess.check_output(['git','rev-parse','--git-common-dir'], cwd=base, text=True).strip()
            (repo/'.git/objects/info/alternates').write_text(str((base/Path(common)/'objects').resolve())+'\n')
            for key in ('user.name', 'user.email'):
                git('config', key, subprocess.check_output(['git','config',key], cwd=base, text=True).strip())
            (repo/CONFIG).unlink()
            git('add','.')
            git('commit','-qm','Freeze temporary controller fixture')
            config['controller_source_commit'] = git('rev-parse','HEAD')
        (repo/CONFIG).write_bytes(encoded(config))
        if config_draft is not None:
            git('add',str(CONFIG))
            git('commit','-qm','Freeze temporary config fixture')
            config_commit = git('rev-parse','HEAD')
            for name, body in candidate.items():
                (repo/name).write_bytes(body)
            git('add',*NATIVE_DELTA)
            git('commit','-qm','Bundle temporary candidate fixture')
            git('update-ref','refs/remotes/origin/master',config_commit)
            proof = preflight(repo)
            assert proof['native_source_commit'] == manifest['native_source_commit']
            bootstrap = user_data(git('rev-parse','HEAD'), '1'*64, 'sources/mock', PREFIX+'a0001', proof)
            print(f'PASS actual temporary Git preflight: candidate={proof["native_source_commit"]}; source_files=399; delta={len(NATIVE_DELTA)}; userdata={len(bootstrap.encode())}; origin ref and archive digest synthetic')
        cargo = root/'cargo'
        cargo_commands = [['test', '--locked', '--workspace', '--all-targets', '--no-run'] if TEST_BUILD else list(worker.COMMAND[1:])]
        stage_names = ()
        if IMPLEMENTATION:
            cargo_commands = [command.split() for command in (
                'test --locked -p borsuk --lib two_bit_generation:: -- --test-threads=1',
                'test --locked -p borsuk --bin check_semantic_router_scorer -- --test-threads=1',
                'build --release --locked -p borsuk --example two_bit_http --bin build_two_bit_generation --bin build_semantic_unit_router --bin repackage_semantic_generation --bin check_semantic_router_scorer',
                'clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious',
                'test --locked --workspace --all-targets --no-run')]
            stage_names = ('generation-tests','scorer-tests','release','clippy','test-build')
            stage_schema = 'borsuk-semantic-1m-implementation-stage-v1'
            if STARTUP_WAVE8:
                cargo_commands = [command.split() for command in (
                    'test --locked -p borsuk --lib object_native_generation:: -- --test-threads=1',
                    'test --locked -p borsuk --lib two_bit_generation::source_walk_tests::semantic_object_store_parity -- --exact --test-threads=1',
                    'test --locked -p borsuk --lib two_bit_generation::source_walk_tests::paged_source_matches_reference_and_preserves_failure_charges -- --exact --test-threads=1',
                    'test --locked -p borsuk --lib two_bit_generation::source_walk_tests::fragmented_paged_source_preserves_trace_and_rank_across_get_caps -- --exact --test-threads=1',
                    'build --release --locked -p borsuk --example two_bit_http',
                    'clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious',
                    'test --locked --workspace --all-targets --no-run')]
                stage_names = ('object-native-generation-tests','semantic-object-store-parity','paged-source-parity',
                               'fragmented-paged-source-parity','release','clippy','test-build')
                stage_schema = 'borsuk-startup-wave8-implementation-stage-v1'
            if ROOT_REUSE:
                cargo_commands = [command.split() for command in (
                    'test --locked -p borsuk --lib object_native_generation:: -- --test-threads=1',
                    'test --locked -p borsuk --lib two_bit_store:: -- --test-threads=1',
                    'test --locked -p borsuk --lib two_bit_generation:: -- --test-threads=1',
                    'test --locked -p borsuk --test two_bit_generation -- --test-threads=1',
                    'test --locked -p borsuk --test two_bit_gc_delayed_delete -- --test-threads=1',
                    'test --locked -p borsuk --test two_bit_application_ids -- --test-threads=1',
                    'test --locked -p borsuk --example two_bit_http -- --test-threads=1',
                    'build --release --locked -p borsuk --example two_bit_http',
                    'clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious',
                    'test --locked --workspace --all-targets --no-run')]
                stage_names = ('object-native-generation-tests','two-bit-store-tests','two-bit-generation-tests',
                               'generation-integration','gc-integration','application-ids-integration',
                               'http-example-tests','release','clippy','test-build')
                stage_schema = 'borsuk-root-reuse-implementation-stage-v1'
            if BOUNDED_PUBLICATION or FIXED48:
                protocol = FIXED48_STAGES if FIXED48 else BOUNDED_PUBLICATION_STAGES
                cargo_commands = [command[1:] for _, command in protocol[:-1]] + [
                    ['test','--locked','--workspace','--all-targets','--no-run']]
                stage_names = tuple(name for name, _ in protocol)
                stage_schema = FIXED48_STAGE_SCHEMA if FIXED48 else BOUNDED_PUBLICATION_STAGE_SCHEMA
        cargo.write_text(f'''#!{sys.executable}
import json, os, sys
from pathlib import Path
if sys.argv[1:] == ['-V']:
    print('fake cargo version')
else:
    target = Path(os.environ['CARGO_TARGET_DIR'])
    calls = target.parent/'cargo-called'
    previous = calls.read_text().splitlines() if calls.exists() else []
    assert sys.argv[1:] == {cargo_commands!r}[len(previous)]
    assert os.environ['CARGO_BUILD_JOBS'] == os.environ['RUST_TEST_THREADS'] == '1'
    assert os.environ['RUSTC_WRAPPER'] == os.environ['RUSTC_WORKSPACE_WRAPPER'] == ''
    if {TEST_BUILD or IMPLEMENTATION!r}:
        assert os.environ['BORSUK_TEST_BUILD_JOBS'] == '1'
        assert 'BORSUK_TEST_BUILD_COMMAND' not in os.environ
    assert target.is_dir()
    if not previous: assert not list(target.iterdir())
    with calls.open('a') as out: out.write(json.dumps(sys.argv[1:])+'\\n')
    if {IMPLEMENTATION!r} and len(previous) == {len(cargo_commands)-3}:
        for name in {tuple(Path(name).name for name in RELEASE_ARTIFACTS)!r}:
            if name == os.environ.get('MISSING_BINARY'): continue
            output = target/'release'/('examples' if name == 'two_bit_http' else '')/name
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_bytes(b'\\x7fELF mocked release '+name.encode())
            output.chmod(0o755)
    if os.environ.get('MUTATE'):
        with Path(os.environ['MUTATE']).open('ab') as out: out.write(b' ')
    print('fake workspace cargo')
    if {BOUNDED_PUBLICATION or FIXED48!r} and len(previous) < 4:
        named = {FIXED48_REQUIRED_TESTS!r}.get({stage_names!r}[len(previous)], ()) if {FIXED48!r} else ()
        for test in named:
            case, affected = os.environ.get('CARGO_NAMED_TEST', '').split(':', 1) if ':' in os.environ.get('CARGO_NAMED_TEST', '') else ('', '')
            if test == affected and case == 'missing': continue
            print('test '+test+' ... '+('ignored' if test == affected and case == 'ignored' else 'ok'))
            if test == affected and case == 'duplicate': print('test '+test+' ... ok')
        if {BOUNDED_PUBLICATION!r} and len(previous) == 1 and os.environ.get('CARGO_CAP_TEST') != 'missing':
            print('test {BOUNDED_PUBLICATION_CAP_TEST} ... '+('ignored' if os.environ.get('CARGO_CAP_TEST') == 'ignored' else 'ok'))
        count = 0 if len(previous)+1 == int(os.environ.get('CARGO_ZERO_STAGE', '0')) else max(1, len(named))
        print(f'test result: ok. {{count}} passed; 0 failed; 0 ignored; 0 measured; 100 filtered out; finished in 0.00s')
    sys.exit(int(os.environ.get('CARGO_EXIT', '0')) if len(previous)+1 == int(os.environ.get('CARGO_FAIL_STAGE', '1')) else 0)
''')
        cargo.chmod(0o755)
        if TEST_BUILD or IMPLEMENTATION:
            # Fake Bash records the exact invocation, then executes the real script.
            (root/'bash').write_text(f'''#!{sys.executable}
import os, sys
from pathlib import Path
assert sys.argv[1:] in [['scripts/check_rust_test_build.sh'], {FIXED['command'][1:]!r}]
with (Path(os.environ['CARGO_TARGET_DIR']).parent/'bash-called').open('a') as out:
    out.write(sys.argv[1]+'\\n')
os.execv('/bin/bash', ['/bin/bash', *sys.argv[1:]])
''')
            (root/'bash').chmod(0o755)
        (root/'rustc').write_text('#!/bin/sh\necho fake rustc version\n')
        (root/'rustc').chmod(0o755)
        # Execute the real worker CLI; only its kernel evidence is synthetic.
        runner = '''import os, runpy
from pathlib import Path
from unittest.mock import patch
original = Path.read_text
counters = {'memory.max':'8589934592', 'memory.peak':'10000', 'memory.swap.max':'0',
 'memory.swap.peak':'0', 'memory.swap.events':'max 0\\nfail 0\\n',
 'memory.events':'low 0\\nhigh 0\\nmax 0\\noom 0\\noom_kill 0\\noom_group_kill 0\\n',
 'cpu.max':'200000 100000', 'cpu.stat':'usage_usec 42\\n', 'pids.max':'512',
 'pids.current':'1', 'pids.events':'max 0\\n', 'cgroup.procs':str(os.getpid())}
def read(path, *args, **kwargs):
    if str(path) == '/proc/self/cgroup': return '0::/workspace-mock\\n'
    if str(path.parent) == '/sys/fs/cgroup/workspace-mock': return counters[path.name]
    return original(path, *args, **kwargs)
with patch.object(Path, 'read_text', read):
    runpy.run_module('scripts.check_native_workspace_execution', run_name='__main__')
'''
        env = dict(os.environ, PYTHONPATH=str(repo), MUTATE='', CARGO_EXIT='0', CARGO_FAIL_STAGE='1', MISSING_BINARY='',
                   CARGO_BUILD_JOBS='99', BORSUK_TEST_BUILD_JOBS='99',
                   RUSTC_WRAPPER='forbidden-wrapper', RUSTC_WORKSPACE_WRAPPER='forbidden-wrapper')
        env.pop('BORSUK_TEST_BUILD_COMMAND', None)
        flag = mode_flag().strip()
        cli = [sys.executable, '-m', MODULE, flag]
        failures = [('success',None), ('exit17',None), ('native',NATIVE_DELTA[0] if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 else 'mock-0.rs'),
                    ('config',str(CONFIG)), ('code',CODE[0]), ('manifest',str(manifest_path))]
        if TEST_BUILD or IMPLEMENTATION:
            failures.append(('script','scripts/check_rust_test_build.sh'))
        if IMPLEMENTATION:
            failures.append(('pipeline',FIXED['command'][1]))
            failures.append(('missing-binary',None))
            failures.extend((f'fail-stage-{stage}',None) for stage in range(2,len(cargo_commands)+1))
        if BOUNDED_PUBLICATION or FIXED48:
            failures.extend((f'zero-stage-{stage}',None) for stage in range(1,5))
        if BOUNDED_PUBLICATION:
            failures.extend((name,None) for name in ('missing-cap-test','ignored-cap-test'))
        if FIXED48:
            failures.extend((case+'-named-'+test, None)
                for names in FIXED48_REQUIRED_TESTS.values() for test in names
                for case in ('missing', 'ignored', 'duplicate'))
        for failure, mutation in failures:
            out = root/failure
            out.mkdir()
            subprocess.run([*cli,'--stage',str(repo),str(out)], cwd=root, env=env, check=True)
            before = (repo/mutation).read_bytes() if mutation else None
            zero_tests = failure.startswith('zero-stage-')
            cap_failed = failure in ('missing-cap-test','ignored-cap-test')
            named_case, named_test = failure.split('-named-', 1) if '-named-' in failure else ('', '')
            named_failed = bool(named_case)
            named_stage = next((index+1 for index, (name, _) in enumerate(FIXED48_STAGES)
                if named_test in FIXED48_REQUIRED_TESTS.get(name, ())), 1)
            failing_stage = named_stage if named_failed else 2 if cap_failed else int(failure.rsplit('-',1)[1]) if failure.startswith('fail-stage-') or zero_tests else 1
            cargo_failed = failure == 'exit17' or failure.startswith('fail-stage-')
            pipeline_failed = cargo_failed or zero_tests or cap_failed or named_failed
            result = subprocess.run([sys.executable,'-c',runner,flag,str(cargo),str(repo),str(out)],
                cwd=root, env=dict(env, MUTATE=str(repo/mutation) if mutation else '',
                                  CARGO_EXIT='17' if cargo_failed else '0', CARGO_FAIL_STAGE=str(failing_stage),
                                  CARGO_ZERO_STAGE=str(failing_stage) if zero_tests else '0',
                                  CARGO_CAP_TEST='missing' if failure == 'missing-cap-test' else 'ignored' if failure == 'ignored-cap-test' else 'ok',
                                  CARGO_NAMED_TEST=named_case+':'+named_test if named_failed else '',
                                  MISSING_BINARY=Path(RELEASE_ARTIFACTS[-1]).name if failure == 'missing-binary' else ''), capture_output=True, text=True)
            assert result.returncode == (0 if failure == 'success' else 17 if cargo_failed else 96), result.stderr
            receipt = json.loads((out/'workspace-receipt.json').read_bytes())
            assert receipt['exit_status'] == (17 if cargo_failed else 96 if zero_tests or cap_failed or named_failed else 0)
            assert receipt['qualified'] is (failure == 'success')
            assert receipt['source_file_count'] == 399 and (out/'cargo-called').exists()
            log = (out/'test.log').read_text()
            called = [json.loads(line) for line in (out/'cargo-called').read_text().splitlines()]
            assert called == cargo_commands[:failing_stage] if pipeline_failed else called == cargo_commands
            if IMPLEMENTATION:
                from datetime import datetime
                records = [json.loads(line) for line in log.splitlines() if line.startswith('{')]
                count = failing_stage if pipeline_failed else len(cargo_commands)
                assert len(records) == 2*count
                stage_commands = [['cargo',*args] for args in cargo_commands[:-1]] + [
                    ['env','-u','BORSUK_TEST_BUILD_COMMAND','bash','scripts/check_rust_test_build.sh']]
                for stage in range(count):
                    start, end = records[2*stage:2*stage+2]
                    assert start['schema'] == end['schema'] == stage_schema
                    assert start['command'] == end['command'] == stage_commands[stage]
                    assert start['stage'] == end['stage'] == stage_names[stage]
                    assert start['exit_status'] is start['finished_at'] is None
                    assert type(end['exit_status']) is int and end['exit_status'] == (17 if cargo_failed and stage == count-1 else 0)
                    if BOUNDED_PUBLICATION or FIXED48:
                        field = 'required_test_passes' if FIXED48 else 'publication_cap_test_passed'
                        assert start['tests_run'] is start['gate_status'] is start[field] is None
                        names = FIXED48_REQUIRED_TESTS.get(stage_names[stage], ()) if FIXED48 else ()
                        assert end['tests_run'] == (0 if zero_tests and stage == count-1 else max(1, len(names))) if stage < 4 else end['tests_run'] is None
                        assert end['gate_status'] == (96 if (zero_tests or cap_failed or named_failed) and stage == count-1 else end['exit_status'])
                        if FIXED48:
                            assert end[field] == {test: (2 if named_case == 'duplicate' else 0) if test == named_test else 1 for test in names}
                        else:
                            assert end[field] is (not cap_failed) if stage == 1 else end[field] is None
                    assert start['started_at'] == end['started_at']
                    assert datetime.fromisoformat(end['finished_at']) >= datetime.fromisoformat(start['started_at'])
                bash_called = (out/'bash-called').read_text().splitlines()
                assert bash_called == [FIXED['command'][1]] + (['scripts/check_rust_test_build.sh'] if count == len(cargo_commands) else [])
                assert receipt['execution_kind'] == 'implementation-gates' and receipt['actual_full_workspace_execution'] is False
                if pipeline_failed:
                    assert not (out/'binaries').exists(), 'failed pipeline copied release outputs'
                elif failure == 'success':
                    assert set(receipt['artifacts']) & set(RELEASE_ARTIFACTS) == set(RELEASE_ARTIFACTS)
                    for name in RELEASE_ARTIFACTS:
                        source = out/'target/release'/('examples' if name.endswith(EXAMPLE_BINARIES) else '')/Path(name).name
                        assert worker.artifact(out/name) == worker.artifact(source)
                        assert (out/name).stat().st_mode & 0o111
            else:
                assert log.startswith('fake workspace cargo\n')
            if TEST_BUILD or (IMPLEMENTATION and count == len(cargo_commands)):
                assert (out/'bash-called').exists()
                assert f'rust-test-build status={receipt["exit_status"]}' in log and 'jobs=1' in log
            checked = subprocess.run([*cli,'--check-receipt',str(out)], cwd=root, env=env, capture_output=True)
            assert (checked.returncode == 0) is (failure == 'success'), checked.stderr
            if failure == 'success':
                proof = json.loads((out/'source-qualification.json').read_bytes())
                body = user_data('0'*40, '1'*64, 'sources/mock', PREFIX+'a0001', proof)
                terminal_script = body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0]
                (out/'run-closed.log').write_text('closed synthetic CLI worker\n')
                terminal = subprocess.check_output([sys.executable,'-c',terminal_script], cwd=out,
                    env=dict(env, INSTANCE_ID='i-owned', EXIT_CODE='0', ORIGINAL_EXIT_CODE='0',
                             PHASE='complete', ARTIFACT_NAMES=' '.join(ARTIFACTS)))
                (out/'aws-terminal.json').write_bytes(terminal)
                (out/'aws-reservation.json').write_bytes(encoded(dict(schema=SCHEMA, qualification=proof,
                    source_commit='0'*40, source_archive_sha256='1'*64)))
                (out/'aws-closeout.json').write_bytes(encoded(dict(state='terminated', nodes={'0':dict(instance_id='i-owned')})))
                replayed = json.loads(subprocess.check_output([*cli,'--replay',str(out)], cwd=root, env=env))
                assert replayed['qualified'] is True and replayed['actual_full_workspace_execution'] is (not (TEST_BUILD or IMPLEMENTATION))
                if TEST_BUILD:
                    assert replayed['actual_workspace_test_build'] is True
                if IMPLEMENTATION:
                    assert replayed['execution_kind'] == 'implementation-gates' and 'actual_workspace_test_build' not in replayed
                    name = RELEASE_ARTIFACTS[0]
                    binary = (out/name).read_bytes()
                    (out/name).write_bytes(b'tampered release copy')
                    for action in ('--check-receipt','--replay'):
                        tampered = subprocess.run([*cli,action,str(out)],cwd=root,env=env,capture_output=True)
                        assert tampered.returncode != 0, 'tampered release copy accepted'
                    (out/name).write_bytes(binary)
            if mutation:
                (repo/mutation).write_bytes(before)
        if TEST_BUILD or IMPLEMENTATION:
            # A true/false shim could report success without invoking Cargo.
            for shim in ('true', 'false', 'arbitrary'):
                out = root/('shim-'+shim)
                out.mkdir()
                subprocess.run([*cli,'--stage',str(repo),str(out)], cwd=root, env=env, check=True)
                wrong = subprocess.run([sys.executable,'-c',runner,flag,str(cargo),str(repo),str(out)],
                    cwd=root, env=dict(env, BORSUK_TEST_BUILD_COMMAND=shim), capture_output=True)
                assert wrong.returncode != 0 and not (out/'target').exists()
                assert not (out/'cargo-called').exists()
                if IMPLEMENTATION:
                    direct = subprocess.run(['/bin/bash',FIXED['command'][1]],
                        cwd=repo, env=dict(env, BORSUK_TEST_BUILD_COMMAND=shim), capture_output=True)
                    assert direct.returncode == 2 and b'test-only build shim forbidden' in direct.stderr
        # Missing mode must reject the semantic authority before creating a target.
        out = root/'wrong-mode'
        out.mkdir()
        subprocess.run([*cli,'--stage',str(repo),str(out)], cwd=root, env=env, check=True)
        wrong = subprocess.run([sys.executable,'-c',runner,str(cargo),str(repo),str(out)],
                               cwd=root, env=env, capture_output=True)
        assert wrong.returncode != 0 and not (out/'target').exists()
        if TEST_BUILD or IMPLEMENTATION:
            wrong = subprocess.run([sys.executable,'-c',runner,'--semantic-1m',str(cargo),str(repo),str(out)],
                                   cwd=root, env=env, capture_output=True)
            assert wrong.returncode != 0 and not (out/'target').exists()
            checked = subprocess.run([sys.executable,'-m',MODULE,'--semantic-1m','--check-receipt',str(root/'success')],
                                     cwd=root, env=env, capture_output=True)
            assert checked.returncode != 0, 'script receipt accepted as full execution'
            if IMPLEMENTATION:
                wrong = subprocess.run([sys.executable,'-c',runner,'--semantic-1m-test-build',str(cargo),str(repo),str(out)],
                                       cwd=root, env=env, capture_output=True)
                assert wrong.returncode != 0 and not (out/'target').exists()
                checked = subprocess.run([sys.executable,'-m',MODULE,'--semantic-1m-test-build','--check-receipt',str(root/'success')],
                                         cwd=root, env=env, capture_output=True)
                assert checked.returncode != 0, 'implementation receipt accepted as compile-only'
                if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48:
                    wrong = subprocess.run([sys.executable,'-c',runner,'--semantic-1m-implementation',str(cargo),str(repo),str(out)],
                                           cwd=root, env=env, capture_output=True)
                    assert wrong.returncode != 0 and not (out/'target').exists()
                    checked = subprocess.run([sys.executable,'-m',MODULE,'--semantic-1m-implementation','--check-receipt',str(root/'success')],
                                             cwd=root, env=env, capture_output=True)
                    assert checked.returncode != 0, 'startup-wave8 receipt accepted as semantic-1m implementation'
                    for sibling in ('--startup-wave8-implementation', '--root-reuse-implementation', '--bounded-publication-implementation', '--fixed48-implementation'):
                        if sibling == flag:
                            continue
                        wrong = subprocess.run([sys.executable,'-c',runner,sibling,str(cargo),str(repo),str(out)],
                                               cwd=root, env=env, capture_output=True)
                        assert wrong.returncode != 0 and not (out/'target').exists()
                        checked = subprocess.run([sys.executable,'-m',MODULE,sibling,'--check-receipt',str(root/'success')],
                                                 cwd=root, env=env, capture_output=True)
                        assert checked.returncode != 0, 'sibling native intervention receipt accepted'


def _test_build_protocol_self_check():
    configure(True, test_build=True)
    assert str(ROOT).endswith('semantic-1m/implementation-gates/remote-test-build')
    assert PREFIX == 'research/semantic-router/20261001/semantic-1m-test-build-'
    assert FIXED['command'] == ['bash', 'scripts/check_rust_test_build.sh']
    assert FIXED['environment']['BORSUK_TEST_BUILD_JOBS'] == '1'
    assert FIXED['environment']['BORSUK_TEST_BUILD_COMMAND'] is None
    assert FIXED['execution_kind'] == 'workspace-test-build'
    assert 'scripts/check_rust_test_build.sh' in CODE and len(CODE) == 23
    assert ARTIFACTS == FULL_ARTIFACTS and len(ARTIFACTS) == 13
    configure(True, implementation=True)
    assert len(ARTIFACTS) == 18
    assert ARTIFACTS == (*FULL_ARTIFACTS,*RELEASE_ARTIFACTS)
    assert str(ROOT).endswith('semantic-1m/implementation-gates/remote-implementation')
    assert PREFIX == 'research/semantic-router/20261001/semantic-1m-implementation-'
    assert FIXED['command'] == ['bash', 'scripts/check_semantic_1m_implementation.sh']
    assert FIXED['environment']['BORSUK_TEST_BUILD_JOBS'] == '1'
    assert FIXED['environment']['BORSUK_TEST_BUILD_COMMAND'] is None
    assert FIXED['execution_kind'] == 'implementation-gates'
    assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_semantic_1m_implementation.sh') and len(CODE) == 24
    assert SCHEMA == 'borsuk-semantic-1m-implementation-gates-spot-v1'
    assert FIXED['schema'] == CONFIG_SCHEMA == 'borsuk-semantic-1m-implementation-gates-v1'
    assert RECEIPT_SCHEMA == 'borsuk-semantic-1m-implementation-gates-receipt-v1'
    assert mode_flag() == ' --semantic-1m-implementation'
    for arguments in (dict(implementation=True), dict(test_build=True), dict(semantic_1m=True,test_build=True,implementation=True)):
        rejected(lambda:configure(**arguments))
    configure(True)
    assert len(ARTIFACTS) == 13
    assert FIXED['command'] == list(worker.COMMAND) and len(CODE) == 22
    assert 'execution_kind' not in FIXED
    configure()
    assert TOKEN_PREFIX == 'metadata-waves-workspace-'


def _startup_wave8_protocol_self_check():
    previous = CONFIG, PREFIX, CODE, FIXED, ARTIFACTS, RELEASE_ARTIFACTS, mode_flag()
    with execution_mode(startup_wave8=True):
        assert str(CONFIG).endswith('semantic-1m/startup-wave8/implementation-gates/config.json')
        assert PREFIX == 'research/semantic-router/20261002/startup-wave8-implementation-'
        assert mode_flag() == ' --startup-wave8-implementation'
        assert FIXED['command'] == ['bash', 'scripts/check_startup_wave8_implementation.sh']
        assert FIXED['environment'] == dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None)
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_startup_wave8_implementation.sh')
        assert RELEASE_ARTIFACTS == ('binaries/two_bit_http',)
        assert ARTIFACTS == (*FULL_ARTIFACTS, *RELEASE_ARTIFACTS)
        assert FIXED['schema'] == CONFIG_SCHEMA == 'borsuk-startup-wave8-implementation-gates-v1'
        assert SCHEMA == 'borsuk-startup-wave8-implementation-gates-spot-v1'
        assert RECEIPT_SCHEMA == 'borsuk-startup-wave8-implementation-gates-receipt-v1'
        assert FIXED['execution_kind'] == 'implementation-gates'
        assert WALL == FIXED['machine_limit_seconds'] == 9000
        assert FIXED['compute_cap_usd'] == 1.25 and FIXED['ebs_s3_allowance_usd'] == .15
        rejected(lambda: configure(startup_wave8=True, test_build=True))
    assert previous == (CONFIG, PREFIX, CODE, FIXED, ARTIFACTS, RELEASE_ARTIFACTS, mode_flag())


def _root_reuse_protocol_self_check():
    previous = ROOT_REUSE, NATIVE_DELTA, CONFIG, PREFIX, CODE, FIXED, ARTIFACTS, RELEASE_ARTIFACTS, mode_flag()
    with execution_mode(root_reuse=True):
        assert str(CONFIG).endswith('semantic-1m/startup-wave8/root-reuse/implementation-gates/config.json')
        assert PREFIX == 'research/semantic-router/20261002/root-reuse-implementation-'
        assert mode_flag() == ' --root-reuse-implementation'
        assert FIXED['command'] == ['bash', 'scripts/check_root_reuse_implementation.sh']
        assert FIXED['environment'] == dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None)
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_root_reuse_implementation.sh')
        assert NATIVE_DELTA == ROOT_REUSE_DELTA and len(NATIVE_DELTA) == 5
        assert list(NATIVE_DELTA) == sorted(NATIVE_DELTA)
        assert RELEASE_ARTIFACTS == ('binaries/two_bit_http',)
        assert ARTIFACTS == (*FULL_ARTIFACTS, *RELEASE_ARTIFACTS)
        assert FIXED['schema'] == CONFIG_SCHEMA == 'borsuk-root-reuse-implementation-gates-v1'
        assert SCHEMA == 'borsuk-root-reuse-implementation-gates-spot-v1'
        assert RECEIPT_SCHEMA == 'borsuk-root-reuse-implementation-gates-receipt-v1'
        assert FIXED['execution_kind'] == 'implementation-gates'
        assert WALL == FIXED['machine_limit_seconds'] == 9000
        assert FIXED['compute_cap_usd'] == 1.25 and FIXED['ebs_s3_allowance_usd'] == .15
        for invalid in (dict(root_reuse=True, test_build=True), dict(root_reuse=True, startup_wave8=True),
                        dict(root_reuse=1)):
            rejected(lambda:configure(**invalid))
        with execution_mode(startup_wave8=True):
            assert not ROOT_REUSE and NATIVE_DELTA == STARTUP_WAVE8_DELTA
        assert ROOT_REUSE and NATIVE_DELTA == ROOT_REUSE_DELTA
    assert previous == (ROOT_REUSE, NATIVE_DELTA, CONFIG, PREFIX, CODE, FIXED, ARTIFACTS, RELEASE_ARTIFACTS, mode_flag())


def _bounded_publication_protocol_self_check():
    import inspect
    assert 'bounded_publication' in inspect.signature(configure).parameters, 'bounded publication mode missing'
    previous = CONFIG, PREFIX, CODE, FIXED, ARTIFACTS, RELEASE_ARTIFACTS, mode_flag()
    with execution_mode(bounded_publication=True):
        assert str(CONFIG).endswith('semantic-1m/bounded-publication/implementation-gates/config.json')
        assert PREFIX == 'research/semantic-router/20261002/bounded-publication-implementation-'
        assert mode_flag() == ' --bounded-publication-implementation'
        assert FIXED['command'] == ['bash', 'scripts/check_bounded_publication_implementation.sh']
        assert FIXED['environment'] == dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None)
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_bounded_publication_implementation.sh')
        assert NATIVE_DELTA == BOUNDED_PUBLICATION_DELTA and len(NATIVE_DELTA) == 3
        assert list(NATIVE_DELTA) == sorted(NATIVE_DELTA)
        assert RELEASE_ARTIFACTS == ('binaries/two_bit_http',)
        assert ARTIFACTS == (*FULL_ARTIFACTS, *RELEASE_ARTIFACTS) and len(ARTIFACTS) == 14
        assert FIXED['schema'] == CONFIG_SCHEMA == 'borsuk-bounded-publication-implementation-gates-v1'
        assert SCHEMA == 'borsuk-bounded-publication-implementation-gates-spot-v1'
        assert RECEIPT_SCHEMA == 'borsuk-bounded-publication-implementation-gates-receipt-v1'
        assert FIXED['execution_kind'] == 'implementation-gates'
        assert WALL == FIXED['machine_limit_seconds'] == 9000
        assert FIXED['test_limit_seconds'] == 7200
        assert FIXED['memory_bytes'] == worker.MEMORY and FIXED['swap_bytes'] == 0
        assert FIXED['cpu_quota_percent'] == 200 and FIXED['tasks_max'] == 512
        assert FIXED['compute_cap_usd'] == 1.25 and FIXED['ebs_s3_allowance_usd'] == .15
        for invalid in (dict(bounded_publication=True, test_build=True),
                        dict(bounded_publication=True, startup_wave8=True),
                        dict(bounded_publication=True, root_reuse=True), dict(bounded_publication=1)):
            rejected(lambda:configure(**invalid))
        for mode in ('startup_wave8', 'root_reuse'):
            with execution_mode(**{mode: True}):
                assert not BOUNDED_PUBLICATION
            assert BOUNDED_PUBLICATION and NATIVE_DELTA == BOUNDED_PUBLICATION_DELTA
    assert previous == (CONFIG, PREFIX, CODE, FIXED, ARTIFACTS, RELEASE_ARTIFACTS, mode_flag())


def _fixed48_protocol_self_check():
    import inspect
    assert 'fixed48' in inspect.signature(configure).parameters, 'fixed48 mode missing'
    previous = CONFIG, PREFIX, CODE, FIXED, ARTIFACTS, RELEASE_ARTIFACTS, NATIVE_DELTA, mode_flag()
    with execution_mode(fixed48=True):
        assert str(CONFIG).endswith('semantic-1m/fixed48/implementation-gates/config.json')
        assert PREFIX == 'research/semantic-router/20261002/fixed48-implementation-'
        assert mode_flag() == ' --fixed48-implementation'
        assert FIXED['command'] == ['bash', 'scripts/check_fixed48_implementation.sh']
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_fixed48_implementation.sh')
        assert NATIVE_DELTA == ('crates/borsuk/src/bin/check_semantic_router_scorer.rs',
            'crates/borsuk/src/semantic_unit_router.rs', 'crates/borsuk/src/two_bit_generation.rs')
        assert list(NATIVE_DELTA) == sorted(NATIVE_DELTA)
        assert RELEASE_ARTIFACTS == ('binaries/two_bit_http', 'binaries/check_semantic_router_scorer', 'binaries/two_bit_plan_demo')
        assert ARTIFACTS == (*FULL_ARTIFACTS, *RELEASE_ARTIFACTS) and len(ARTIFACTS) == 16
        assert CONFIG_SCHEMA == FIXED['schema'] == 'borsuk-fixed48-implementation-gates-v1'
        assert SCHEMA == 'borsuk-fixed48-implementation-gates-spot-v1'
        assert RECEIPT_SCHEMA == 'borsuk-fixed48-implementation-gates-receipt-v1'
        assert FIXED['environment'] == dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None)
        assert FIXED['execution_kind'] == 'implementation-gates'
        assert FIXED['memory_bytes'] == 8*1024**3 and FIXED['swap_bytes'] == 0
        assert FIXED['cpu_quota_percent'] == 200 and FIXED['tasks_max'] == 512
        assert FIXED['test_limit_seconds'] == 7200 and FIXED['service_limit_seconds'] == 7260
        assert FIXED['machine_limit_seconds'] == WALL == 9000
        assert FIXED['spot_max_usd_per_hour'] == .50
        assert FIXED['compute_cap_usd'] == 1.25 and FIXED['ebs_s3_allowance_usd'] == .15
        for mode in ('test_build', 'startup_wave8', 'root_reuse', 'bounded_publication'):
            rejected(lambda: configure(fixed48=True, **{mode: True}))
        rejected(lambda: configure(fixed48=1))
        for mode in ('startup_wave8', 'root_reuse', 'bounded_publication'):
            with execution_mode(**{mode: True}):
                assert not FIXED48
            assert FIXED48 and NATIVE_DELTA == FIXED48_DELTA
        try:
            with execution_mode():
                raise RuntimeError('synthetic interruption')
        except RuntimeError:
            assert FIXED48
    assert previous == (CONFIG, PREFIX, CODE, FIXED, ARTIFACTS, RELEASE_ARTIFACTS, NATIVE_DELTA, mode_flag())


def _hierarchical_archive_self_check(*, constrained_split=False, cell_overlap=False, fine_sq8=False, pq_residual=False, co_selection=False):
    """Tiny committed closure; no production archive, native tools or network."""
    module = sys.modules[__name__]
    with execution_mode(hierarchical_cells=not (constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection), constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection), tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp)/'repo'; repo.mkdir()
        code = ('scripts/controller.py',)
        manifest_path = ROOT/'native-source-manifest.json'
        bodies = {name: b'// native fixture\n' for name in NATIVE_DELTA}
        if constrained_split:
            bodies[NATIVE_DELTA[1]] = (Path(__file__).resolve().parents[1]/NATIVE_DELTA[1]).read_bytes()+b'// additive fixture\n'
        bodies.update({'Cargo.toml': b'[workspace]\n', '.cargo/config.toml': b'[build]\n',
            'README.md': b'build instructions\n', 'scripts/controller.py': b'pass\n',
            'crates/other/tests/fixtures/literal[1].json': b'{"fixture":1}\n',
            'docs/research/native-script.rs': b'// required native docs fixture\n',
            'docs/research/old-binary.gz': b'historical body must stay out\n'})
        for name, body in bodies.items():
            path = repo/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
        inventory = worker.source_hashes(repo)
        manifest = dict(schema='borsuk-co-selection-native-source-manifest-v1' if co_selection else 'borsuk-pq-residual-native-source-manifest-v1' if pq_residual else 'borsuk-corrected-four-bit-native-source-manifest-v1' if fine_sq8 else 'borsuk-cell-overlap-native-source-manifest-v1' if cell_overlap else 'borsuk-constrained-split-native-source-manifest-v1' if constrained_split else 'borsuk-hierarchical-cells-native-source-manifest-v1',
            source_sha256=inventory, source_file_count=len(inventory),
            source_identity_sha256=worker.source_identity(inventory), native_source_commit='7'*40,
            controller_source_commit='4'*40, control_native_source_commit=CONSTRAINED_SPLIT_CONTROL if constrained_split else '8'*40,
            candidate_delta_paths=list(NATIVE_DELTA), candidate_qualification_pending=True)
        (repo/manifest_path).parent.mkdir(parents=True, exist_ok=True)
        (repo/manifest_path).write_bytes(encoded(manifest))
        support = {n: worker.sha(b) for n,b in bodies.items()
                   if not n.startswith('docs/research/') and n not in inventory and n not in code}
        paths = sorted(set(support) | set(inventory) | set(code) | {str(CONFIG), str(manifest_path)})
        authority = dict(source_archive_paths=paths, source_archive_paths_sha256=worker.sha(encoded(paths)),
                         source_archive_file_count=len(paths), source_archive_support_sha256=support)
        config = dict(FIXED, controller_authority_pending=False, controller_source_commit='4'*40,
            controller_code_sha256={n: worker.sha(bodies[n]) for n in code},
            native_source_manifest=dict(path=str(manifest_path), **worker.artifact(repo/manifest_path)), **authority)
        (repo/CONFIG).write_bytes(encoded(config))
        with patch.object(module, 'CODE', code):
            proof = qualify(repo)
            assert set(authority) <= set(proof), 'hierarchical qualification must bind the minimal archive roster'
            assert {k: proof[k] for k in authority} == authority
            if fine_sq8 or pq_residual or co_selection:
                # Even a self-consistent shortened inventory must include OWN4.
                for name in NATIVE_DELTA:
                    (repo/name).unlink()
                    shortened = worker.source_hashes(repo)
                    changed = dict(manifest, source_sha256=shortened, source_file_count=len(shortened),
                        source_identity_sha256=worker.source_identity(shortened))
                    (repo/manifest_path).write_bytes(encoded(changed))
                    shortened_paths = [path for path in paths if path != name]
                    changed = dict(config, native_source_manifest=dict(path=str(manifest_path), **worker.artifact(repo/manifest_path)),
                        source_archive_paths=shortened_paths, source_archive_file_count=len(shortened_paths),
                        source_archive_paths_sha256=worker.sha(encoded(shortened_paths)))
                    (repo/CONFIG).write_bytes(encoded(changed))
                    rejected(lambda:qualify(repo))
                    (repo/name).write_bytes(bodies[name])
                (repo/manifest_path).write_bytes(encoded(manifest))
                (repo/CONFIG).write_bytes(encoded(config))
            assert 'docs/research/native-script.rs' in paths and 'docs/research/old-binary.gz' not in paths
            assert 'crates/other/tests/fixtures/literal[1].json' in paths
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=repo)
            git('init', '-q'); git('add', '.')
            git('-c', 'user.name=Roman Bartusiak', '-c', 'user.email=riomus@gmail.com',
                'commit', '-qm', 'Synthetic qualification archive fixture')
            commit = git('rev-parse', 'HEAD').decode().strip()
            assert hierarchical_archive_authority(repo, commit, inventory, str(manifest_path)) == authority
            # Committed bytes, rather than worktree bytes, define the support map.
            fixture = repo/'crates/other/tests/fixtures/literal[1].json'
            fixture.write_bytes(b'uncommitted fixture drift')
            assert hierarchical_archive_authority(repo, commit, inventory, str(manifest_path)) == authority
            fixture.write_bytes(bodies[str(fixture.relative_to(repo))])
            # A self-consistent omitted support file still fails committed preflight.
            omitted = dict(authority, source_archive_support_sha256={n:v for n,v in support.items() if n != 'README.md'})
            omitted['source_archive_paths'] = [n for n in paths if n != 'README.md']
            omitted['source_archive_paths_sha256'] = worker.sha(encoded(omitted['source_archive_paths']))
            omitted['source_archive_file_count'] -= 1
            assert hierarchical_archive_authority(repo, commit, inventory, str(manifest_path)) != omitted
            for invalid in (dict(inventory, **{'missing.rs': '0'*64}),
                            dict(inventory, **{'docs/research': '0'*64})):
                rejected(lambda: hierarchical_archive_authority(repo, commit, invalid, str(manifest_path)))
            for name, attr in (('README.md', 'export-ignore'), ('README.md', 'export-subst'),
                               ('docs', 'export-ignore'), ('crates', 'export-ignore')):
                (repo/'.gitattributes').write_text(name+' '+attr+'\n')
                git('add', '.gitattributes')
                git('-c', 'user.name=Roman Bartusiak', '-c', 'user.email=riomus@gmail.com',
                    'commit', '-qm', 'Synthetic export refusal')
                changed_commit = git('rev-parse', 'HEAD').decode().strip()
                rejected(lambda: hierarchical_archive_authority(repo, changed_commit, inventory, str(manifest_path)))
            (repo/'.gitattributes').unlink()
            (repo/'link').symlink_to('README.md'); git('add', '-A')
            git('-c', 'user.name=Roman Bartusiak', '-c', 'user.email=riomus@gmail.com',
                'commit', '-qm', 'Synthetic nonregular refusal')
            changed_commit = git('rev-parse', 'HEAD').decode().strip()
            rejected(lambda: hierarchical_archive_authority(repo, changed_commit, inventory, str(manifest_path)))
            fixture = repo/'crates/other/tests/fixtures/literal[1].json'
            fixture.write_bytes(b'tampered runtime fixture')
            rejected(lambda: qualify(repo))
            fixture.write_bytes(bodies[str(fixture.relative_to(repo))])
            for key, value in (('source_archive_paths', paths[:-1]),
                    ('source_archive_paths', list(reversed(paths))),
                    ('source_archive_paths', paths+[paths[0]]),
                    ('source_archive_paths', sorted(paths+['docs/research/old-binary.gz'])),
                    ('source_archive_paths_sha256', '0'*64), ('source_archive_file_count', True),
                    ('source_archive_support_sha256', dict(support, **{'docs/research/old-binary.gz': '0'*64})),
                    ('source_archive_support_sha256', dict(support, **{'README.md': None})),
                    ('source_archive_support_sha256', {n:v for n,v in support.items() if n != 'README.md'})):
                changed = dict(config, **{key:value})
                (repo/CONFIG).write_bytes(encoded(changed))
                rejected(lambda: qualify(repo))
            for key in ARCHIVE_FIELDS:
                (repo/CONFIG).write_bytes(encoded({k:v for k,v in config.items() if k != key}))
                rejected(lambda: qualify(repo))
            (repo/CONFIG).write_bytes(encoded(config))
            fixture.unlink(); fixture.symlink_to(repo/'README.md')
            rejected(lambda: qualify(repo))
            fixture.unlink(); fixture.write_bytes(bodies[str(fixture.relative_to(repo))])
            with patch.object(subprocess, 'check_output', side_effect=AssertionError('remote Git forbidden')):
                out = Path(tmp)/'stage'; out.mkdir()
                assert stage(repo, out) == proof
                bindings = {archive_binding_prefix()+key.upper():str(proof[key]) for key in ARCHIVE_IDENTITIES}
                with patch.dict(os.environ, bindings):
                    good = Path(tmp)/'bound-stage'; good.mkdir()
                    assert stage(repo, good) == proof
                    for key in bindings:
                        with patch.dict(os.environ, {key: '0'}):
                            bad = Path(tmp)/('bad-'+key); bad.mkdir()
                            rejected(lambda: stage(repo, bad))
                            assert not list(bad.iterdir()), 'binding refusal before artifact writes'
    shared.source_archive_self_check()
    print('PASS hierarchical minimal archive/native docs/runtime fixture/tamper/no remote Git')


def _hierarchical_archive_metadata_self_check():
    """Actual local Git metadata only, including every crate's test fixtures."""
    base = Path(__file__).resolve().parents[1]
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=base, text=True).strip()
    manifest_path = semantic.ROOT.parent/'semantic-1m/hierarchical-cells/implementation-gates/native-source-manifest.json'
    manifest = json.loads((base/manifest_path).read_bytes())
    with execution_mode(hierarchical_cells=True):
        paths, tree = _hierarchical_archive_roster(base, commit, manifest['source_sha256'], str(manifest_path))
        nonresearch = {n for n in tree if not n.startswith('docs/research/')}
        expected = nonresearch | set(manifest['source_sha256']) | set(CODE) | {str(CONFIG), str(manifest_path)}
        assert set(paths) == expected and set(manifest['source_sha256']) <= set(paths)
        fixtures = {n for n in tree if n.startswith('crates/') and ('/tests/' in n or '/fixtures/' in n)}
        assert fixtures and fixtures <= set(paths), 'all committed Rust test fixtures retained'
        assert {n for n in paths if n.startswith('docs/research/')} == expected - nonresearch, 'no historical research bodies'
        pending = not (base/CONFIG).exists()
        print(f'PASS actual committed archive metadata: commit={commit} nonresearch={len(nonresearch)} native={len(manifest["source_sha256"])} archive={len(paths)} crate_test_files={len(fixtures)} roster_sha256={worker.sha(encoded(paths))}; production_authority_pending={pending}; no archive/data/native run')


def _hierarchical_cells_protocol_self_check():
    previous = CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag()
    with execution_mode(hierarchical_cells=True):
        assert str(ROOT).endswith('semantic-1m/hierarchical-cells/source-witness-router/implementation-gates')
        assert mode_flag() == ' --hierarchical-cells-implementation'
        assert PREFIX == 'research/semantic-router/20261004/source-witness-router-implementation-'
        assert CONFIG_SCHEMA == FIXED['schema'] == 'borsuk-hierarchical-cells-implementation-gates-v2'
        assert SCHEMA == 'borsuk-hierarchical-cells-implementation-gates-spot-v2'
        assert RECEIPT_SCHEMA == 'borsuk-hierarchical-cells-implementation-gates-receipt-v2'
        assert FIXED['command'] == ['bash', 'scripts/check_hierarchical_cells_implementation.sh']
        assert TERMINAL_IDENTITIES == (*FULL_TERMINAL_IDENTITIES, 'controller_source_commit', 'candidate_delta_paths', *ARCHIVE_IDENTITIES)
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_hierarchical_cells_implementation.sh')
        assert NATIVE_DELTA == HIERARCHICAL_CELLS_DELTA and list(NATIVE_DELTA) == sorted(NATIVE_DELTA)
        assert RELEASE_ARTIFACTS == ('binaries/hierarchical_semantic_cells', 'binaries/two_bit_http', 'binaries/build_two_bit_generation', 'binaries/check_semantic_router_scorer')
        assert ARTIFACTS == (*FULL_ARTIFACTS, *RELEASE_ARTIFACTS) and len(ARTIFACTS) == 17
        assert FIXED == dict(FULL_FIXED, schema=CONFIG_SCHEMA, execution_kind='implementation-gates',
            command=FIXED['command'], environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None))
        for mode in ('test_build', 'startup_wave8', 'root_reuse', 'bounded_publication', 'fixed48', 'constrained_split'):
            rejected(lambda: configure(hierarchical_cells=True, **{mode: True}))
        rejected(lambda: configure(hierarchical_cells=1))
        with execution_mode(fixed48=True):
            assert not HIERARCHICAL_CELLS
        assert HIERARCHICAL_CELLS
    for kwargs in ({}, {'semantic_1m':True}, {'semantic_1m':True, 'test_build':True}, {'semantic_1m':True, 'implementation':True},
                   {'startup_wave8':True}, {'root_reuse':True}, {'bounded_publication':True}, {'fixed48':True}):
        with execution_mode(**kwargs):
            assert not set(ARCHIVE_IDENTITIES).intersection(TERMINAL_IDENTITIES), 'old modes retain full archive protocol'
    assert previous == (CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag())


def _constrained_split_self_check():
    """Exact new protocol plus existing hierarchical lifecycle; all native work mocked."""
    import inspect
    module = sys.modules[__name__]
    previous = CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag()
    for function in (configure, execution_mode, worker.main, self_check):
        assert 'constrained_split' in inspect.signature(function).parameters
    with execution_mode(constrained_split=True):
        assert CONSTRAINED_SPLIT and MINIMAL_ARCHIVE and not HIERARCHICAL_CELLS
        assert str(ROOT).endswith('semantic-1m/hierarchical-cells/constrained-split/implementation-gates')
        assert mode_flag() == ' --constrained-split-implementation'
        assert CONFIG_SCHEMA == FIXED['schema'] == 'borsuk-constrained-split-implementation-gates-v1'
        assert SCHEMA == 'borsuk-constrained-split-implementation-gates-spot-v1'
        assert RECEIPT_SCHEMA == 'borsuk-constrained-split-implementation-gates-receipt-v1'
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_constrained_split_implementation.sh')
        assert NATIVE_DELTA == CONSTRAINED_SPLIT_DELTA and list(NATIVE_DELTA) == sorted(NATIVE_DELTA)
        assert RELEASE_ARTIFACTS == ('binaries/check_hierarchical_split_balance', 'binaries/hierarchical_semantic_cells', 'binaries/two_bit_http')
        assert TERMINAL_IDENTITIES == (*FULL_TERMINAL_IDENTITIES, 'controller_source_commit', 'candidate_delta_paths', *ARCHIVE_IDENTITIES)
        assert FIXED == dict(FULL_FIXED, schema=CONFIG_SCHEMA, execution_kind='implementation-gates',
            command=['bash', 'scripts/check_constrained_split_implementation.sh'],
            environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None),
            mandatory_test_names_pending=False,
            mandatory_tests={name:list(tests) for name,tests in constrained_split_required_tests().items()},
            control_native_source_commit=CONSTRAINED_SPLIT_CONTROL, control_module_prefix=CONSTRAINED_SPLIT_PREFIX)
        for mode in ('test_build', 'startup_wave8', 'root_reuse', 'bounded_publication', 'fixed48', 'hierarchical_cells'):
            rejected(lambda:configure(constrained_split=True, **{mode:True}))
        rejected(lambda:configure(constrained_split=1))
        for field in ('CONSTRAINED_SPLIT_ADDITIVE_TESTS', 'CONSTRAINED_SPLIT_BIN_TESTS'):
            with patch.object(module, field, ()):
                rejected(constrained_split_required_tests)
                rejected(lambda:record_constrained_split_stage(['release', 'start', '', '', '', '', *dict(CONSTRAINED_SPLIT_STAGES)['release']]))
        for key,value in (('mandatory_test_names_pending', True), ('mandatory_tests', {}),
                          ('control_module_prefix', {}), ('control_native_source_commit', '0'*40)):
            rejected(lambda:validate_constrained_split_config(dict(FIXED, **{key:value})))
        with execution_mode(hierarchical_cells=True):
            assert not CONSTRAINED_SPLIT and MINIMAL_ARCHIVE
        assert CONSTRAINED_SPLIT
        _hierarchical_cells_script_self_check(constrained_split=True)
        _fixed48_stages_self_check(constrained_split=True)
        _startup_wave8_preflight_self_check(constrained_split=True)
        _hierarchical_archive_self_check(constrained_split=True)
        _self_check()
    _hierarchical_cells_protocol_self_check()
    _hierarchical_cells_script_self_check()
    _fixed48_stages_self_check(hierarchical_cells=True)
    _startup_wave8_preflight_self_check(hierarchical_cells=True)
    self_check(hierarchical_cells=True)
    assert previous == (CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag())
    print('PASS constrained split exact config/native prefix/test names; six serial gates/log exits; existing hierarchical mode preserved')


def _cohere_sq8_builder_self_check():
    """Source/Python/Bash mocks only: never run a compiler or cloud operation."""
    if __name__ == '__main__':
        from scripts import launch_native_workspace_execution_spot as authority
        return authority._cohere_sq8_builder_self_check()
    from shlex import split
    import types
    module = sys.modules[__name__]
    base = Path(__file__).resolve().parents[1]
    old = types.ModuleType('sq8_historical_protocol')
    old.__file__ = __file__
    original = subprocess.check_output([*ARCHIVE_GIT, 'show', '2941dfc9578e02404d77c71aed8f12fc8bdcf0a3:scripts/launch_native_workspace_execution_spot.py'], cwd=base)
    exec(compile(original, '<historical-protocol>', 'exec'), old.__dict__)
    fields = ('ROOT', 'CONFIG', 'PREFIX', 'TOKEN_PREFIX', 'TAG', 'SCHEMA', 'CONFIG_SCHEMA',
              'RECEIPT_SCHEMA', 'CODE', 'FIXED', 'ARTIFACTS', 'RELEASE_ARTIFACTS', 'TERMINAL_IDENTITIES', 'NATIVE_DELTA')
    modes = [{}, dict(semantic_1m=True), dict(semantic_1m=True, test_build=True),
             dict(semantic_1m=True, implementation=True)]
    modes += [{name:True} for name in ('startup_wave8', 'root_reuse', 'bounded_publication',
        'fixed48', 'hierarchical_cells', 'constrained_split', 'cell_overlap', 'fine_sq8',
        'pq_residual', 'co_selection', 'budget_object_selector', 'budget_object_fitter', 'cohere1024')]  # cohere_cohort intentionally changed to the publisher delta
    for mode in modes:
        old.configure(**mode)
        with execution_mode(**mode):
            assert all(globals()[name] == getattr(old, name) for name in fields), 'historical protocol changed'
    with execution_mode(cohere_cohort=True):
        rejected(lambda:validate_candidate_delta([]))
        rejected(lambda:validate_cohere_cohort_config(dict(mandatory_test_names_pending=False, mandatory_tests={})))
    with execution_mode(cohere_sq8_builder=True):
        assert mode_flag() == ' --cohere-sq8-builder' and MINIMAL_ARCHIVE
        assert NATIVE_DELTA == () and validate_candidate_delta([]) == []
        rejected(lambda:validate_candidate_delta(['crates/borsuk/examples/build_sq8_source.rs']))
        rejected(lambda:configure(cohere_sq8_builder=True, cohere_cohort=True))
        assert CODE == (*FULL_CODE, 'scripts/check_cohere_sq8_builder.sh')
        assert RELEASE_ARTIFACTS == ('binaries/build_sq8_source',)
        assert ARTIFACTS == (*FULL_ARTIFACTS, 'completed-native-qualification.json', *RELEASE_ARTIFACTS)
        assert FIXED['memory_bytes'] == 8*1024**3 and FIXED['swap_bytes'] == 0
        assert (FIXED['cpu_quota_percent'], FIXED['tasks_max'], FIXED['environment']['CARGO_BUILD_JOBS']) == (200, 512, '1')
        assert (FIXED['test_limit_seconds'], FIXED['service_limit_seconds'], FIXED['machine_limit_seconds']) == (7200, 7260, 9000)
        validate_cohere_sq8_builder_config(FIXED)
        script = (base/FIXED['command'][1]).read_text()
        assert tuple((args[1], args[2:]) for line in script.splitlines() if line.startswith('run_stage ') for args in (split(line),)) == COHERE_SQ8_BUILDER_STAGES
        subprocess.run(['bash', '-n'], input=script, text=True, check=True)
        functions = script[script.index('stage_record() {'):script.index('run_stage release')]
        for status, tee_status in ((0,0), (17,0), (0,18)):
            with tempfile.TemporaryDirectory() as tmp:
                fake = Path(tmp)/'cargo'
                fake.write_text('#!/usr/bin/env python3\nimport os,sys\nprint("MOCK build only",flush=True)\nsys.exit(int(os.environ["FAKE_CARGO_EXIT"]))\n')
                fake.chmod(0o755)
                tee = 'tee() { /usr/bin/tee "$@"; return 18; }\n' if tee_status else ''
                body = 'set -euo pipefail\nsource_check() { :; }\n'+functions+tee
                body += 'run_stage release cargo build --release --locked -p borsuk --example build_sq8_source\n'
                result = subprocess.run(['bash', '-c', body], cwd=base, text=True, capture_output=True, timeout=30,
                    env=dict(os.environ, PATH=tmp+os.pathsep+os.environ['PATH'], TMPDIR=tmp,
                        PYTHONDONTWRITEBYTECODE='1', FAKE_CARGO_EXIT=str(status)))
                assert result.returncode == (status or tee_status), result.stderr
                records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
                assert len(records) == 2 and records[1]['command'] == COHERE_SQ8_BUILDER_STAGES[0][1]
                assert records[1]['exit_status'] == status and records[1]['log_exit_status'] == tee_status
                assert records[1]['gate_status'] == (status or tee_status)
                assert records[1]['tests_run'] is None and records[1]['required_test_passes'] == {}
                assert list(Path(tmp).iterdir()) == [fake], 'stage temporary files leaked'
        with tempfile.TemporaryDirectory() as tmp:
            repo, out = Path(tmp)/'repo', Path(tmp)/'out'
            repo.mkdir(); out.mkdir()
            bodies = {'Cargo.toml':b'[workspace]\n', 'Cargo.lock':b'version = 4\n',
                'crates/borsuk/examples/build_sq8_source.rs':b'// synthetic source fixture; never compiled\n',
                'docs/research/fixture.rs':b'// dynamic native inventory fixture\n',
                'crates/fixture/tests/fixtures/runtime.json':b'{"fixture":1}\n'}
            bodies.update({name:(base/name).read_bytes() for name in CODE})
            for name, body in bodies.items():
                path = repo/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
            inventory = worker.source_hashes(repo)
            manifest_path = ROOT/'native-source-manifest.json'
            manifest = dict(schema='borsuk-cohere-sq8-builder-native-source-manifest-v1',
                native_source_commit='7'*40, control_native_source_commit='7'*40,
                candidate_qualification_pending=True, candidate_delta_paths=[], source_sha256=inventory,
                source_file_count=len(inventory), source_identity_sha256=worker.source_identity(inventory))
            manifest_body = encoded(manifest)
            (repo/manifest_path).parent.mkdir(parents=True); (repo/manifest_path).write_bytes(manifest_body)
            receipt_path = ROOT/'completed-native-verification.json'
            native_receipt = dict(status='NATIVE_QUALIFIED', source_file_count=len(inventory),
                native_source_identity_sha256=worker.source_identity(inventory),
                full_source_before_after_root_match=True, terminated=True,
                stages=[dict(name=name, command=command, exit=0) for name,command in COHERE1024_STAGES])
            completed_native_body = encoded(native_receipt)
            (repo/receipt_path).write_bytes(completed_native_body)
            support = {name:worker.sha(body) for name,body in bodies.items() if name not in inventory and name not in CODE}
            paths = sorted(set(inventory) | set(CODE) | set(support) | {str(CONFIG), str(manifest_path), str(receipt_path)})
            config = dict(FIXED, controller_authority_pending=False, controller_source_commit='4'*40,
                controller_code_sha256={name:worker.sha(bodies[name]) for name in CODE},
                native_source_manifest=dict(path=str(manifest_path), **worker.artifact(repo/manifest_path)),
                completed_native_qualification=dict(path=str(receipt_path), **worker.artifact(repo/receipt_path)),
                source_archive_paths=paths, source_archive_paths_sha256=worker.sha(encoded(paths)),
                source_archive_file_count=len(paths), source_archive_support_sha256=support)
            config_body = encoded(config); (repo/CONFIG).write_bytes(config_body)
            proof = qualify(repo)
            assert proof['schema'] == 'borsuk-cohere-sq8-builder-qualification-v1'
            assert stage(repo, out) == proof
            assert (out/'completed-native-qualification.json').read_bytes() == completed_native_body
            for key,value in (('mandatory_test_names_pending',True), ('mandatory_tests',{'test':['invented']}),
                ('controller_authority_pending',True), ('controller_source_commit','invalid'),
                ('source_archive_paths_sha256','0'*64), ('source_archive_file_count',True),
                ('command',['true']), ('unexpected_key',True)):
                (repo/CONFIG).write_bytes(encoded(dict(config, **{key:value})))
                rejected(lambda:qualify(repo))
            (repo/CONFIG).write_bytes(config_body)
            for descriptor in ({}, dict(config['completed_native_qualification'], bytes=True),
                dict(config['completed_native_qualification'], bytes=65537),
                dict(config['completed_native_qualification'], bytes=len(completed_native_body)+1),
                dict(config['completed_native_qualification'], sha256='0'*64),
                dict(config['completed_native_qualification'], path='../verification.json'),
                dict(config['completed_native_qualification'], path=str(manifest_path))):
                (repo/CONFIG).write_bytes(encoded(dict(config, completed_native_qualification=descriptor)))
                rejected(lambda:qualify(repo))
            missing = dict(config); del missing['completed_native_qualification']
            (repo/CONFIG).write_bytes(encoded(missing)); rejected(lambda:qualify(repo))
            (repo/CONFIG).write_bytes(config_body)
            (repo/receipt_path).unlink(); rejected(lambda:qualify(repo))
            (repo/receipt_path).symlink_to(repo/manifest_path); rejected(lambda:qualify(repo))
            (repo/receipt_path).unlink()
            (repo/receipt_path).write_bytes(completed_native_body)
            bad_receipts = [dict(native_receipt, **{key:value}) for key,value in (
                ('status','PENDING'), ('source_file_count',True), ('source_file_count',len(inventory)+1),
                ('native_source_identity_sha256','0'*64), ('full_source_before_after_root_match',False),
                ('terminated',False), ('stages',native_receipt['stages'][:-1]),
                ('stages',list(reversed(native_receipt['stages']))), ('stages',native_receipt['stages']*2))]
            missing = dict(native_receipt); del missing['terminated']; bad_receipts.append(missing)
            for index in range(len(COHERE1024_STAGES)):
                for change in (dict(exit=17), dict(exit=False), dict(command=['true'])):
                    changed = copy.deepcopy(native_receipt); changed['stages'][index].update(change)
                    bad_receipts.append(changed)
            for changed in bad_receipts:
                (repo/receipt_path).write_bytes(encoded(changed))
                (repo/CONFIG).write_bytes(encoded(dict(config, completed_native_qualification=dict(path=str(receipt_path), **worker.artifact(repo/receipt_path)))))
                refused = Path(tmp)/'invalid-receipt-output'; refused.mkdir(exist_ok=True)
                rejected(lambda:stage(repo, refused))
                assert not list(refused.iterdir()), 'invalid native receipt staged'
            (repo/receipt_path).write_bytes(completed_native_body); (repo/CONFIG).write_bytes(config_body)
            omitted_paths = [name for name in paths if name != str(receipt_path)]
            (repo/CONFIG).write_bytes(encoded(dict(config, source_archive_paths=omitted_paths,
                source_archive_paths_sha256=worker.sha(encoded(omitted_paths)), source_archive_file_count=len(omitted_paths))))
            rejected(lambda:qualify(repo)); (repo/CONFIG).write_bytes(config_body)
            for name in ('crates/borsuk/examples/build_sq8_source.rs', 'docs/research/fixture.rs', 'Cargo.toml', CODE[0], next(iter(support))):
                path = repo/name; original = path.read_bytes(); path.write_bytes(original+b'drift\n')
                rejected(lambda:qualify(repo)); path.write_bytes(original)
            extra = repo/'extra.rs'; extra.write_bytes(b'// extra native source\n')
            rejected(lambda:qualify(repo)); extra.unlink()
            for key,value in (('candidate_delta_paths',['extra.rs']), ('source_file_count',True),
                ('native_source_commit','8'*40), ('control_native_source_commit','8'*40), ('source_sha256',{})):
                changed_manifest = encoded(dict(manifest, **{key:value}))
                (repo/manifest_path).write_bytes(changed_manifest)
                changed_config = dict(config, native_source_manifest=dict(path=str(manifest_path), **worker.artifact(repo/manifest_path)))
                (repo/CONFIG).write_bytes(encoded(changed_config))
                rejected(lambda:qualify(repo))
            (repo/manifest_path).write_bytes(manifest_body); (repo/CONFIG).write_bytes(config_body)
            bindings = {archive_binding_prefix()+key.upper():str(proof[key]) for key in ARCHIVE_IDENTITIES}
            for key in bindings:
                refused = Path(tmp)/key; refused.mkdir()
                with patch.dict(os.environ, dict(bindings, **{key:'0'})):
                    rejected(lambda:stage(repo, refused))
                assert not list(refused.iterdir())
            native_tree = b''.join(b'100644 blob '+worker.sha(bodies[name]).encode()[:40]+b'\t'+os.fsencode(name)+b'\0' for name in sorted(inventory))
            tree_key = (*ARCHIVE_GIT[1:], 'ls-tree', '-rz', '--full-tree')
            answers = {('status','--porcelain'):'',
                ('rev-list','--parents','-n','1','HEAD'):'6'*40+' '+'5'*40,
                ('rev-list','--parents','-n','1','5'*40):'5'*40+' '+'4'*40,
                ('for-each-ref','--contains='+'5'*40,'--format=%(refname)','refs/remotes/origin/'):'refs/remotes/origin/master',
                ('for-each-ref','--contains='+'7'*40,'--format=%(refname)','refs/remotes/origin/main'):'refs/remotes/origin/main',
                ('diff','--no-renames','--name-only','4'*40,'5'*40):str(CONFIG),
                ('diff','--no-renames','--name-only','5'*40,'HEAD'):'',
                (*tree_key, '7'*40):native_tree,
                (*tree_key, '4'*40):native_tree}
            for key,value in [(None,None), *[(key,value) for key,value in (
                (('status','--porcelain'),'dirty'),
                (('rev-list','--parents','-n','1','HEAD'),'6'*40+' '+'5'*40+' '+'4'*40),
                (('rev-list','--parents','-n','1','5'*40),'5'*40+' '+'8'*40),
                (('for-each-ref','--contains='+'5'*40,'--format=%(refname)','refs/remotes/origin/'),''),
                (('for-each-ref','--contains='+'7'*40,'--format=%(refname)','refs/remotes/origin/main'),''),
                (('for-each-ref','--contains='+'7'*40,'--format=%(refname)','refs/remotes/origin/main'),'refs/remotes/origin/main-preview'),
                (('diff','--no-renames','--name-only','4'*40,'5'*40),str(CONFIG)+'\nextra'),
                (('diff','--no-renames','--name-only','5'*40,'HEAD'),'extra.rs'),
                ((*tree_key, '7'*40),b''),
                ((*tree_key, '7'*40),native_tree.replace(b'100644',b'120000',1)),
                ((*tree_key, '7'*40),native_tree.replace(b'blob ',b'blob '+b'0',1)),
                ((*tree_key, '7'*40),native_tree+b'100644 blob '+b'0'*40+b'\textra.rs\0'))]]:
                changed = dict(answers)
                if key is not None:
                    changed[key] = value
                with patch.object(module, 'qualify', return_value=proof), \
                    patch.object(module, 'hierarchical_archive_authority', return_value={key:proof[key] for key in ARCHIVE_FIELDS}), \
                    patch.object(subprocess, 'check_output', side_effect=lambda args, **kw:changed[tuple(args[1:])]):
                    if key is None:
                        assert preflight(repo) == proof
                    else:
                        rejected(lambda:preflight(repo))
            # Exercise the complete freeze locally, including committed archive support.
            def git(*args):
                return subprocess.check_output(['git', *args], cwd=repo, text=True).strip()
            git('init', '-q')
            git('add', '.')
            git('commit', '-qm', 'Synthetic native source fixture')
            native_commit = git('rev-parse', 'HEAD')
            frozen_manifest = dict(manifest, native_source_commit=native_commit, control_native_source_commit=native_commit)
            (repo/manifest_path).write_bytes(encoded(frozen_manifest))
            (repo/CONFIG).unlink()
            git('add', '-A')
            git('commit', '-qm', 'Synthetic controller freeze')
            controller_commit = git('rev-parse', 'HEAD')
            frozen_config = dict(config, controller_source_commit=controller_commit,
                native_source_manifest=dict(path=str(manifest_path), **worker.artifact(repo/manifest_path)),
                **hierarchical_archive_authority(repo, controller_commit, inventory, str(manifest_path),
                    completed_native_qualification=config['completed_native_qualification']))
            (repo/CONFIG).write_bytes(encoded(frozen_config))
            git('add', str(CONFIG))
            git('commit', '-qm', 'Synthetic config freeze')
            git('update-ref', 'refs/remotes/origin/main', git('rev-parse', 'HEAD'))
            git('commit', '--allow-empty', '-qm', 'Synthetic empty native bundle')
            assert preflight(repo) == qualify(repo)
            body = user_data('0'*40, '1'*64, 'sources/mock', PREFIX+'a0001', proof)
            assert 'rustup component add clippy' not in body and mode_flag() in body
            files = _worker_self_check(proof, config_body, manifest_body, completed_native_body)
            _collection_self_check(proof, files, body)
            _lifecycle_self_check(proof)
    print('PASS cohere-sq8-builder glue: completed six-gate native receipt bound before Cargo; one recorded build; empty native delta/test roster; source/archive/binary/exit/tee/receipt/cgroup/ACK/termination-wait negatives; historical modes preserved; SYNTHETIC MOCKS ONLY, Rust UNRUN')


def _budget_object_selector_self_check(*, budget_object_fitter=False, cohere1024=False, cohere_cohort=False, exact_sq8_runtime=False, retained_generation_rebind=False):
    """Minimal source-bound fixtures only; Rust and cloud execution are mocked."""
    if __name__ == '__main__':
        from scripts import launch_native_workspace_execution_spot as authority
        return authority._budget_object_selector_self_check(budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort, exact_sq8_runtime=exact_sq8_runtime, retained_generation_rebind=retained_generation_rebind)
    import inspect
    from shlex import split
    import types
    module = sys.modules[__name__]
    base = Path(__file__).resolve().parents[1]
    mode_name = 'retained_generation_rebind' if retained_generation_rebind else 'exact_sq8_runtime' if exact_sq8_runtime else 'cohere_cohort' if cohere_cohort else 'cohere1024' if cohere1024 else 'budget_object_fitter' if budget_object_fitter else 'budget_object_selector'
    mode = {mode_name: True}
    tag = 'retained-generation-rebind' if retained_generation_rebind else 'exact-sq8-runtime' if exact_sq8_runtime else 'cohere-cohort' if cohere_cohort else 'cohere1024' if cohere1024 else 'budget-object-fitter' if budget_object_fitter else 'budget-object-selector'
    required_attr = 'RETAINED_REBIND_REQUIRED_TESTS' if retained_generation_rebind else 'EXACT_SQ8_RUNTIME_REQUIRED_TESTS' if exact_sq8_runtime else 'COHERE_COHORT_REQUIRED_TESTS' if cohere_cohort else 'COHERE1024_REQUIRED_TESTS' if cohere1024 else 'BUDGET_OBJECT_FITTER_REQUIRED_TESTS' if budget_object_fitter else 'BUDGET_OBJECT_SELECTOR_REQUIRED_TESTS'
    required_tests = cohere_cohort_required_tests if cohere_cohort else cohere1024_required_tests if cohere1024 else budget_object_fitter_required_tests if budget_object_fitter else budget_object_selector_required_tests
    validate_config = validate_cohere_cohort_config if cohere_cohort else validate_cohere1024_config if cohere1024 else validate_budget_object_fitter_config if budget_object_fitter else validate_budget_object_selector_config
    stages = RETAINED_REBIND_STAGES if retained_generation_rebind else EXACT_SQ8_RUNTIME_STAGES if exact_sq8_runtime else COHERE_COHORT_STAGES if cohere_cohort else COHERE1024_STAGES if cohere1024 else BUDGET_OBJECT_FITTER_STAGES if budget_object_fitter else BUDGET_OBJECT_SELECTOR_STAGES
    stage_name = 'retained-rebind-integration-tests' if retained_generation_rebind else 'exact-sq8-scorer-tests' if exact_sq8_runtime else 'cohere-native-publisher-tests' if cohere_cohort else 'generation-tests' if cohere1024 else tag+'-tests'
    assert mode_name in inspect.signature(configure).parameters, 'execution mode missing'
    assert mode_name in inspect.signature(execution_mode).parameters
    assert mode_name in inspect.signature(worker.main).parameters
    # Compare every historical config/roster byte with the exact opening base.
    old = types.ModuleType('selector_historical_protocol')
    old.__file__ = __file__
    exec(compile(subprocess.check_output([*ARCHIVE_GIT, 'show', (RETAINED_REBIND_HISTORICAL_BASE if retained_generation_rebind else EXACT_SQ8_HISTORICAL_BASE if exact_sq8_runtime else 'a76d2bb4cde8c7daafd93eafb09b5c7264bbcfee' if cohere_cohort else 'e68b9493041daa5562b566b92ef23bddcd1e093d' if cohere1024 else 'bb8fe89f30e8e5dcf9fbb6a5bf4440704dc01f66' if budget_object_fitter else BUDGET_OBJECT_SELECTOR_CHECK_BASE)+':scripts/launch_native_workspace_execution_spot.py'], cwd=base), '<historical-protocol>', 'exec'), old.__dict__)
    fields = ('ROOT', 'CONFIG', 'PREFIX', 'TOKEN_PREFIX', 'TAG', 'SCHEMA', 'CONFIG_SCHEMA',
              'RECEIPT_SCHEMA', 'CODE', 'FIXED', 'ARTIFACTS', 'RELEASE_ARTIFACTS', 'TERMINAL_IDENTITIES', 'NATIVE_DELTA')
    modes = [{}, {'semantic_1m':True}, {'semantic_1m':True, 'test_build':True},
             {'semantic_1m':True, 'implementation':True}]
    modes += [{name:True} for name in ('startup_wave8', 'root_reuse', 'bounded_publication',
        'fixed48', 'hierarchical_cells', 'constrained_split', 'cell_overlap', 'fine_sq8', 'pq_residual', 'co_selection')]
    historical_modes = [*modes, {'budget_object_selector':True}, {'budget_object_fitter':True}, {'cohere1024':True}, {'cohere_cohort':True}, {'cohere_sq8_builder':True}, {'exact_sq8_runtime':True}] if retained_generation_rebind else [*modes, {'budget_object_selector':True}, {'budget_object_fitter':True}, {'cohere1024':True}, {'cohere_cohort':True}, {'cohere_sq8_builder':True}] if exact_sq8_runtime else [*modes, {'budget_object_selector':True}, {'budget_object_fitter':True}, {'cohere1024':True}] if cohere_cohort else [*modes, {"budget_object_selector":True}, {"budget_object_fitter":True}] if cohere1024 else [*modes, {"budget_object_selector":True}] if budget_object_fitter else modes
    for historical_mode in historical_modes:
        old.configure(**historical_mode)
        with execution_mode(**historical_mode):
            assert all(encoded(str(globals()[name]) if isinstance(globals()[name], Path) else globals()[name]) ==
                encoded(str(getattr(old, name)) if isinstance(getattr(old, name), Path) else getattr(old, name)) for name in fields), 'historical protocol bytes changed'
            if retained_generation_rebind:  # switch-back: every other mode keeps the original subnet, FIXED matches SUBNET
                assert SUBNET == FIXED['subnet_id'] == old.SUBNET == old.FIXED['subnet_id'] == FULL_FIXED['subnet_id'], 'historical subnet changed'
    for regression_mode in ('fixed48', 'hierarchical_cells', 'constrained_split', 'cell_overlap', 'fine_sq8', 'pq_residual', 'co_selection'):
        _fixed48_stages_self_check(**{regression_mode:True}) if regression_mode != 'fixed48' else _fixed48_stages_self_check()
    with patch.object(module, required_attr, {}), execution_mode(**mode):
        assert FIXED['mandatory_test_names_pending'] is True
        rejected(required_tests)
        rejected(lambda:validate_config(FIXED))
    retained_synthetic = RETAINED_REBIND_REQUIRED_TESTS or {
        'retained-rebind-integration-tests': ('retained_semantic_synthetic_republication_preserves', 'retained_semantic_synthetic_republication_refuses'),
        'retained-rebind-publisher-tests': (*_COHORT_PROTOCOL[2]['cohere-native-publisher-tests'], 'tests::synthetic_retained_publication_succeeds', 'tests::synthetic_retained_publication_refuses')}
    synthetic_names = retained_synthetic[stage_name] if retained_generation_rebind else EXACT_SQ8_RUNTIME_REQUIRED_TESTS[stage_name] if exact_sq8_runtime else COHERE_COHORT_REQUIRED_TESTS[stage_name] if cohere_cohort else COHERE1024_REQUIRED_TESTS[stage_name] if cohere1024 else (*BUDGET_OBJECT_SELECTOR_REQUIRED_TESTS['budget-object-selector-tests'],
        'budget_object_selector::tests::synthetic_prepared_fixture', 'budget_object_fitter::tests::synthetic_contract_fixture') if budget_object_fitter else ('budget_object_selector::tests::synthetic_contract_fixture',)
    synthetic = retained_synthetic if retained_generation_rebind else EXACT_SQ8_RUNTIME_REQUIRED_TESTS if exact_sq8_runtime else COHERE_COHORT_REQUIRED_TESTS if cohere_cohort else COHERE1024_REQUIRED_TESTS if cohere1024 else {stage_name: synthetic_names}
    with patch.object(module, required_attr, synthetic), execution_mode(**mode):
        assert len(stages) == (5 if retained_generation_rebind else 7 if exact_sq8_runtime else 6 if cohere_cohort else 6 if cohere1024 else 4)
        if retained_generation_rebind:
            assert NATIVE_DELTA == RETAINED_REBIND_DELTA == tuple(sorted(RETAINED_REBIND_DELTA)) and len(NATIVE_DELTA) == 3 and set(RETAINED_REBIND_TEST_SOURCES.values()) <= set(NATIVE_DELTA)
            assert RELEASE_ARTIFACTS == RETAINED_REBIND_RELEASE == ('binaries/check_cohere_native_baseline', 'binaries/publish_two_bit_generation')
            assert str(ROOT) == 'docs/research/performance-architecture-20260930/cohere1024/retained-generation-rebind/implementation-gates'
            assert SUBNET == FIXED['subnet_id'] == RETAINED_REBIND_SUBNET != FULL_FIXED['subnet_id'], 'retained mode alone uses the selected subnet'
            assert PREFIX == 'research/semantic-router/20261006/retained-generation-rebind-gates-' and COHERE_COHORT_STAGES is RETAINED_REBIND_STAGES
            assert [name for name, _ in stages] == ['retained-rebind-integration-tests', 'retained-rebind-publisher-tests', 'release', 'clippy', 'test-build']
            assert all('--locked' in command for _, command in stages if command[0] == 'cargo') and all(command[-2:] == ['--', '--test-threads=1'] for _, command in stages[:2])
            integration_stage, publisher_stage = 'retained-rebind-integration-tests', 'retained-rebind-publisher-tests'
            assert len(synthetic[integration_stage]) >= 2 and len(synthetic[publisher_stage]) == 4
            def structure(roster):
                with patch.object(module, 'RETAINED_REBIND_REQUIRED_TESTS', roster), patch.object(module, 'COHERE_COHORT_REQUIRED_TESTS', roster):
                    return cohere_cohort_required_tests()
            assert structure(synthetic) == synthetic  # positive control: the negatives below fail on structure, not on binding
            old_names = _COHORT_PROTOCOL[2]['cohere-native-publisher-tests']
            for bad_roster in ({}, {integration_stage: synthetic[integration_stage]}, {publisher_stage: synthetic[publisher_stage]},
                    dict(synthetic, **{integration_stage: ()}), dict(synthetic, **{publisher_stage: ()}),
                    dict(synthetic, **{integration_stage: synthetic[integration_stage][:1]}),
                    dict(synthetic, **{integration_stage: (synthetic[integration_stage][0],)*2}),
                    dict(synthetic, **{integration_stage: (*synthetic[integration_stage][:1], 'tests::not_an_integration_name')}),
                    dict(synthetic, **{publisher_stage: synthetic[publisher_stage][1:]}),
                    dict(synthetic, **{publisher_stage: synthetic[publisher_stage][:-1]}),
                    dict(synthetic, **{publisher_stage: (*synthetic[publisher_stage], 'tests::fifth')}),
                    dict(synthetic, **{publisher_stage: (*synthetic[publisher_stage][1:], 'tests::replaces_an_old_test')}),
                    dict(synthetic, **{publisher_stage: (*synthetic[publisher_stage][:3], 'unprefixed_name')}),
                    dict(synthetic, **{publisher_stage: (*synthetic[publisher_stage][:3], synthetic[integration_stage][0])}),
                    dict(synthetic, **{publisher_stage: (*old_names, 'tests::twice', 'tests::twice')}),
                    dict(synthetic, extra_stage=('tests::foreign',))):
                rejected(lambda: structure(bad_roster))
        elif exact_sq8_runtime:
            assert NATIVE_DELTA == EXACT_SQ8_RUNTIME_DELTA == tuple(sorted(EXACT_SQ8_RUNTIME_DELTA)) and len(NATIVE_DELTA) == 3
            assert RELEASE_ARTIFACTS == EXACT_SQ8_RUNTIME_RELEASE == ('binaries/check_cohere_native_baseline', 'binaries/exact_sq8_runtime_probe')
            assert str(ROOT) == 'docs/research/performance-architecture-20260930/cohere1024/exact-sq8-runtime-gates'
            assert COHERE_COHORT_STAGES is EXACT_SQ8_RUNTIME_STAGES and dict(stages)['exact-sq8-probe'] == EXACT_SQ8_PROBE_COMMAND
            assert [name for name, _ in stages] == ['exact-sq8-scorer-tests', 'exact-sq8-scorer-release-tests', 'returned-sq8-tests', 'release', 'exact-sq8-probe', 'clippy', 'test-build']
            assert sum(len(tests) for tests in synthetic.values()) == 14 and all(len(tests) == 7 for tests in synthetic.values())
            good_probe = b''.join(encoded(dict(dimensions=d, rows=r, candidate_over_baseline_median=1.0, identical_ids_ordinals_score_bits=True))+b'\n'
                                  for d, r in sorted(EXACT_SQ8_PROBE_SHAPES))
            assert probe_lines(b'noise\n'+good_probe+b'tail without newline') == good_probe and len(good_probe.splitlines()) == 18
            for bad_probe in (b'', b'no json lines\n', good_probe.replace(b'true', b'false', 1), good_probe.splitlines(True)[0] * 18,
                              b''.join(good_probe.splitlines(True)[1:]), good_probe + good_probe.splitlines(True)[0],
                              good_probe.replace(b'"dimensions":128', b'"dimensions":129', 1), good_probe.replace(b'true', b'1', 1)):
                rejected(lambda: probe_lines(bad_probe))
        elif cohere_cohort:
            assert NATIVE_DELTA == COHERE_COHORT_DELTA == tuple(sorted(COHERE_COHORT_DELTA))
            assert RELEASE_ARTIFACTS == ('binaries/check_cohere_native_baseline', 'binaries/prepare_cohere_native_cohort', 'binaries/publish_two_bit_generation')
            assert len(NATIVE_DELTA) == 1 and sum(len(tests) for tests in synthetic.values()) == 18
            assert str(ROOT) == 'docs/research/performance-architecture-20260930/cohere1024/cohort-implementation-gates'
        elif cohere1024:
            assert NATIVE_DELTA == COHERE1024_DELTA == tuple(sorted(COHERE1024_DELTA))
            assert RELEASE_ARTIFACTS == FULL_RELEASE_ARTIFACTS
            assert str(ROOT) == 'docs/research/performance-architecture-20260930/cohere1024/implementation-gates'
        else:
            assert set(NATIVE_DELTA) == {'crates/borsuk/src/budget_object_selector.rs', 'crates/borsuk/src/lib.rs'} | ({'crates/borsuk/src/budget_object_fitter.rs'} if budget_object_fitter else set())
            assert RELEASE_ARTIFACTS == ('binaries/hierarchical_semantic_cells',)
            assert str(ROOT).endswith('fine-sq8-groups/budget-object-selector/'+('fitter-implementation-gates' if budget_object_fitter else 'implementation-gates'))
        assert mode_flag() == (' --exact-sq8-runtime' if exact_sq8_runtime else ' --'+tag+'-implementation')
        assert archive_binding_prefix() == 'BORSUK_'+tag.replace('-','_').upper()+'_'
        assert CONFIG_SCHEMA == FIXED['schema'] == ('borsuk-exact-sq8-runtime-gates-v1' if exact_sq8_runtime else 'borsuk-'+tag+'-implementation-gates-v1')
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_exact_sq8_runtime.sh' if exact_sq8_runtime else 'scripts/check_'+mode_name+'_implementation.sh')
        assert ARTIFACTS == (*FULL_ARTIFACTS, *RELEASE_ARTIFACTS, *((EXACT_SQ8_PROBE_ARTIFACT,) if exact_sq8_runtime else ()))
        if cohere1024 or cohere_cohort:
            from unittest.mock import Mock
            dispatch = Mock(return_value={'gate_status':0})
            cli = (base/'scripts/check_native_workspace_execution.py').read_text().split("if __name__ == '__main__':", 1)[1]
            with patch.object(sys, 'argv', ['worker', mode_flag().strip(), 'cargo', 'repo', 'out']), contextlib.redirect_stdout(io.StringIO()):
                try:
                    exec(compile('if True:'+cli, '<worker-cli>', 'exec'), dict(sys=sys, json=json, main=dispatch))
                except SystemExit as result:
                    assert result.code == 0
            args, kwargs = dispatch.call_args
            assert dispatch.call_count == 1 and args == ('cargo', 'repo', 'out')
            assert kwargs == {name: name in ('semantic_1m', 'implementation', mode_name)
                for name in inspect.signature(worker.main).parameters if name not in ('cargo', 'repo', 'out')}
        for conflicting_mode in (historical_modes[2:3]+historical_modes[4:] if not (exact_sq8_runtime or retained_generation_rebind) else [{'cohere_cohort':True}, {'cohere_sq8_builder':True}, {'cohere1024':True}, {'budget_object_selector':True}, {'exact_sq8_runtime':True} if retained_generation_rebind else {'retained_generation_rebind':True}]):
            rejected(lambda:configure(**mode, **conflicting_mode))
        rejected(lambda:configure(**{mode_name:1}))
        for invalid in ({}, {stage_name: ()},
                {stage_name: synthetic[stage_name]*2},
                {stage_name: ('tests::foreign',)}):
            with patch.object(module, required_attr, invalid):
                rejected(required_tests)
        if cohere_cohort:
            for roster_stage, names in synthetic.items():
                for invalid_names in (names[:-1], names*2, (*names[:-1], 'tests::foreign')):
                    bad_config = dict(FIXED, mandatory_tests={name:list(tests) for name,tests in synthetic.items()})
                    bad_config['mandatory_tests'][roster_stage] = list(invalid_names)
                    rejected(lambda:validate_config(bad_config))
        if budget_object_fitter:
            for invalid_names in (synthetic_names[1:], synthetic_names[:-1], synthetic_names[:7]):
                with patch.object(module, required_attr, {stage_name:invalid_names}):
                    rejected(required_tests)
        script = (base/FIXED['command'][1]).read_text()
        assert tuple((args[1], args[2:]) for line in script.splitlines() if line.startswith('run_stage ') for args in (split(line),)) == stages
        subprocess.run(['bash', '-n'], input=script, text=True, check=True)
        # Exercise the real Bash/time/tee/end/cleanup with a labelled fake Cargo.
        functions = script[script.index('stage_record() {'):script.index('run_stage '+stage_name)]
        if retained_generation_rebind:  # the recorder runs in a fresh process: hand it the same (possibly synthetic) roster
            injected = functions.replace('c.configure(retained_generation_rebind=True)\nsys.exit', 'c.RETAINED_REBIND_REQUIRED_TESTS = '+encoded(synthetic).decode()+'\nc.configure(retained_generation_rebind=True)\nsys.exit')
            assert injected != functions
            functions = injected
        if not (cohere1024 or cohere_cohort):
            functions = functions.replace('sys.exit(record_constrained_split_stage',
                'from scripts import launch_native_workspace_execution_spot as c\nc.'+required_attr+' = '+encoded(synthetic).decode()+'\nsys.exit(record_constrained_split_stage')
        for name, command in (stages[:2] if exact_sq8_runtime or retained_generation_rebind else stages[:3] if cohere_cohort else stages[:1]):
            fake_output = ''.join('test '+test+' ... ok\n' for test in synthetic[name])+f'test result: ok. {len(synthetic[name])} passed; 0 failed; 0 ignored; 0 measured; 100 filtered out; finished in 0.00s\n'
            with tempfile.TemporaryDirectory() as tmp:
                fake = Path(tmp)/'cargo'
                fake.write_text('#!/usr/bin/env python3\nimport os,sys\nprint('+repr(fake_output)+',end="",flush=True)\nsys.exit(int(os.environ["FAKE_CARGO_EXIT"]))\n')
                fake.chmod(0o755)
                for status, tee_status in ((0,0), (17,0), (0,18)):
                    tee = 'tee() { /usr/bin/tee "$@"; return 18; }\n' if tee_status else ''
                    body = 'set -euo pipefail\nsource_check() { :; }\n'+functions+tee
                    body += 'run_stage '+name+' '+' '.join(command)+'\n'
                    result = subprocess.run(['bash','-c',body], cwd=base, text=True, capture_output=True, timeout=30,
                        env=dict(os.environ, PATH=tmp+os.pathsep+os.environ['PATH'], TMPDIR=tmp,
                            PYTHONDONTWRITEBYTECODE='1', LC_ALL='C', FAKE_CARGO_EXIT=str(status)))
                    assert result.returncode == (status or tee_status), result.stderr
                    records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
                    assert len(records) == 2 and records[1]['command'] == command
                    assert records[1]['exit_status'] == status and records[1]['log_exit_status'] == tee_status
                    assert records[1]['gate_status'] == (status or tee_status)
                    validate_budget_object_selector_resources(records[1]['resources'], status)
                    assert list(Path(tmp).iterdir()) == [fake], 'stage temporary files leaked'
        resources = 'User time (seconds): 0.01\nSystem time (seconds): 0.01\nElapsed (wall clock) time (h:mm:ss or m:ss): 0:00.02\nMaximum resident set size (kbytes): 10000\nExit status: 0\n'
        with tempfile.TemporaryDirectory() as tmp:
            log, usage, combined = (Path(tmp)/name for name in ('stage.log', 'usage.txt', 'test.log'))
            lines = []
            for name, command in stages:
                names = ('semantic_unit_router::tests::synthetic_contract_fixture',) if cohere1024 and name == 'router-tests' else ('returned_sq8::tests::synthetic_nonzero_fixture',) if exact_sq8_runtime and name == 'returned-sq8-tests' else synthetic.get(name, ())
                if cohere1024 and name == 'generation-tests':
                    names += ('two_bit_generation::source_walk_tests::synthetic_regression',)
                good = ''.join('test '+test+' ... ok\n' for test in names)
                if names:
                    good += f'test result: ok. {len(names)} passed; 0 failed; 0 ignored; 0 measured; 100 filtered out; finished in 0.00s\n'
                if name == 'test-build':
                    good += 'rust-test-build status=0 elapsed_seconds=0 jobs=1\n'
                with contextlib.redirect_stdout(io.StringIO()) as output:
                    assert record_constrained_split_stage([name, '2026-10-06T00:00:00Z', '', '', '', '', *command, str(usage)], budget_object_selector=not (budget_object_fitter or cohere1024 or cohere_cohort), budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort) == 0
                lines.append(output.getvalue()); lines.append(good)
                cases = [(good, '0', '0', 0), (good, '17', '0', 17), (good, '0', '18', 18)]
                if names:
                    test = 'test '+names[0]+' ... ok'
                    cases += [(good.replace(test, replacement), '0', '0', 96) for replacement in ('', test+'\n'+test, test.replace('ok', 'FAILED'), test.replace('ok', 'ignored'), *(() if (cohere1024 and name == 'router-tests') or (exact_sq8_runtime and name == 'returned-sq8-tests') else ('test budget_object_selector::tests::foreign ... ok',)))]
                    cases += [(good.replace(str(len(names))+' passed', '0 passed'), '0', '0', 96), (good.replace('0 ignored', '1 ignored'), '0', '0', 96), (good+good, '0', '0', 96)]
                if cohere_cohort and names:
                    if not (exact_sq8_runtime and name == 'returned-sq8-tests'):  # nonzero-count stage admits any extra test
                        cases.append(('test tests::extra ... ok\n'+good.replace(f'{len(names)} passed', f'{len(names)+1} passed'), '0', '0', 96))
                    alternatives = [test for tests in synthetic.values() for test in tests if test not in names]
                    if alternatives:
                        cases.append((good.replace(names[0], alternatives[0]), '0', '0', 96))
                if cohere1024 and name == 'router-tests':
                    cases.append((good.replace(names[0], synthetic_names[0]), '0', '0', 96))
                if cohere1024 and name == 'generation-release-test':
                    cases.append(('test tests::extra ... ok\n'+good.replace('1 passed', '2 passed'), '0', '0', 96))
                if name == 'test-build':
                    cases += [(body, '0', '0', 96) for body in ('', good+good, good.replace('jobs=1', 'jobs=2'))]
                for body, status, tee_status, expected in cases:
                    log.write_text(body); usage.write_text(resources.replace('Exit status: 0', 'Exit status: '+status))
                    with contextlib.redirect_stdout(io.StringIO()) as output:
                        gate = record_constrained_split_stage([name, '2026-10-06T00:00:00Z', '2026-10-06T00:00:00Z', status, str(log), tee_status, *command, str(usage)], budget_object_selector=not (budget_object_fitter or cohere1024 or cohere_cohort), budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort)
                    assert gate == expected, (name, expected, gate)
                    if expected == 0:
                        lines.append(output.getvalue())
                rejected(lambda:record_constrained_split_stage([name, 'start', '', '', '', '', *command, '--extra', str(usage)], budget_object_selector=not (budget_object_fitter or cohere1024 or cohere_cohort), budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort))
            combined.write_text(''.join(lines))
            assert len(validate_bounded_publication_stages(combined, budget_object_selector=not (budget_object_fitter or cohere1024 or cohere_cohort), budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort)) == len(stages)
            good_log = combined.read_text()
            if cohere1024:
                oracle_line = 'test '+synthetic_names[0]+' ... ok\n'
                assert good_log.count(oracle_line) == 2
                for missing in (good_log.replace(oracle_line, '', 1), ''.join(good_log.rsplit(oracle_line, 1))):
                    combined.write_text(missing)
                    rejected(lambda:validate_bounded_publication_stages(combined, cohere1024=True))
            for bad in (good_log.replace(str(len(synthetic_names))+' passed', '0 passed'), good_log.replace('status=0 elapsed', 'status=17 elapsed'),
                    good_log.replace('jobs=1', 'jobs=2'), good_log.replace('"log_exit_status": 0', '"log_exit_status": 18'),
                    good_log.replace('"resources": "User time', '"resources": "Missing time'), good_log+good_log,
                    good_log.replace('test '+synthetic_names[-1]+' ... ok', ''),
                    good_log.replace('"exit_status": 0', '"exit_status": false')):
                combined.write_text(bad)
                rejected(lambda:validate_bounded_publication_stages(combined, budget_object_selector=not (budget_object_fitter or cohere1024 or cohere_cohort), budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort))
            for bad in ('', resources+resources, resources.replace('10000', str(worker.MEMORY)), resources.replace('Exit status: 0', 'Exit status: 17')):
                rejected(lambda:validate_budget_object_selector_resources(bad, 0))
        # Actual portable qualification over a tiny new Rust fixture and runtime support.
        with tempfile.TemporaryDirectory() as tmp:
            repo, out = Path(tmp)/'repo', Path(tmp)/'out'
            repo.mkdir(); out.mkdir()
            bodies = {name:b'// temporary native fixture\n' for name in NATIVE_DELTA}
            if cohere_cohort and not (exact_sq8_runtime or retained_generation_rebind):
                bodies[NATIVE_DELTA[0]] = (base/NATIVE_DELTA[0]).read_bytes()
                # Every mandatory name must be a real test function in its own bin source.
                for roster_stage, source in (('cohere-native-publisher-tests', NATIVE_DELTA[0]),
                        ('cohere-native-baseline-tests', 'crates/borsuk/src/bin/check_cohere_native_baseline.rs'),
                        ('cohere-native-cohort-tests', 'crates/borsuk/src/bin/prepare_cohere_native_cohort.rs')):
                    text = (base/source).read_text()
                    assert all('fn '+name.removeprefix('tests::')+'(' in text for name in synthetic[roster_stage]), roster_stage

            if retained_generation_rebind:
                for roster_stage, source in RETAINED_REBIND_TEST_SOURCES.items():
                    bodies[source] = ''.join('#[test]\nfn '+n.removeprefix('tests::')+'() {}\n' for n in synthetic[roster_stage]).encode()
            bodies.update({'Cargo.toml':b'[workspace]\n', 'Cargo.lock':b'version = 4\n',
                'docs/research/fixture.rs':b'// native fixture in docs\n',
                'crates/fixture/tests/fixtures/runtime.json':b'{"fixture":1}\n'})
            bodies.update({name:(base/name).read_bytes() for name in CODE})
            for name, body in bodies.items():
                path = repo/name; path.parent.mkdir(parents=True, exist_ok=True); path.write_bytes(body)
            inventory = worker.source_hashes(repo)
            manifest_path = ROOT/'native-source-manifest.json'
            manifest = dict(schema='borsuk-'+tag+('-gates' if exact_sq8_runtime else '-implementation-gates' if cohere1024 or cohere_cohort else '')+'-native-source-manifest-v1',
                native_source_commit='7'*40, control_native_source_commit='8'*40,
                candidate_qualification_pending=True, candidate_delta_paths=list(NATIVE_DELTA),
                source_sha256=inventory, source_file_count=len(inventory), source_identity_sha256=worker.source_identity(inventory))
            manifest_body = encoded(manifest)
            (repo/manifest_path).parent.mkdir(parents=True); (repo/manifest_path).write_bytes(manifest_body)
            support = {name:worker.sha(body) for name,body in bodies.items() if name not in inventory and name not in CODE}
            paths = sorted(set(inventory) | set(CODE) | set(support) | {str(CONFIG), str(manifest_path)})
            config = dict(FIXED, controller_authority_pending=False, controller_source_commit='4'*40,
                controller_code_sha256={name:worker.sha(bodies[name]) for name in CODE},
                native_source_manifest=dict(path=str(manifest_path), **worker.artifact(repo/manifest_path)),
                source_archive_paths=paths, source_archive_paths_sha256=worker.sha(encoded(paths)),
                source_archive_file_count=len(paths), source_archive_support_sha256=support)
            config_body = encoded(config); (repo/CONFIG).write_bytes(config_body)
            proof = qualify(repo)
            if retained_generation_rebind:
                mandatory = {stage_key: list(names) for stage_key, names in synthetic.items()}
                validate_retained_rebind_sources(repo, mandatory)
                for roster_stage, source in RETAINED_REBIND_TEST_SOURCES.items():
                    original = (repo/source).read_bytes()
                    for dropped in synthetic[roster_stage]:
                        bare = dropped.removeprefix('tests::')
                        for hidden in (b'fn renamed(', ('// '+bare+'\nfn renamed(').encode()):  # absent, or present only outside a fn
                            (repo/source).write_bytes(original.replace(('fn '+bare+'(').encode(), hidden))
                            rejected(lambda: validate_retained_rebind_sources(repo, mandatory))
                    (repo/source).write_bytes(original)
                with patch.object(module, 'validate_retained_rebind_sources', side_effect=AssertionError('wired into qualify')):
                    rejected(lambda: qualify(repo))
            assert proof['schema'] == ('borsuk-exact-sq8-runtime-gates-qualification-v1' if exact_sq8_runtime else 'borsuk-'+tag+'-implementation-gates-qualification-v1')
            assert stage(repo, out) == proof
            for key, value in (('mandatory_test_names_pending',True), ('mandatory_tests',{}),
                    ('controller_authority_pending',True), ('source_archive_paths_sha256','0'*64),
                    ('source_archive_file_count',True), ('unexpected_key',True)):
                (repo/CONFIG).write_bytes(encoded(dict(config, **{key:value})))
                rejected(lambda:qualify(repo))
            (repo/CONFIG).write_bytes(config_body)
            for name in (NATIVE_DELTA[0], 'Cargo.toml', CODE[0], next(iter(support))):
                path = repo/name; original = path.read_bytes(); path.write_bytes(original+b'drift\n')
                rejected(lambda:qualify(repo)); path.write_bytes(original)
            extra = repo/'extra.rs'; extra.write_bytes(b'// new source drift\n')
            rejected(lambda:qualify(repo)); extra.unlink()
            for delta in ([], list(NATIVE_DELTA[:1]), list(reversed(NATIVE_DELTA)), [*NATIVE_DELTA, 'extra.rs']):
                if delta == list(NATIVE_DELTA):
                    continue  # one-file delta: its prefix/reverse is the accepted value
                rejected(lambda:validate_candidate_delta(delta))
            bindings = {archive_binding_prefix()+key.upper():str(proof[key]) for key in ARCHIVE_IDENTITIES}
            for key in bindings:
                refused = Path(tmp)/key; refused.mkdir()
                with patch.dict(os.environ, dict(bindings, **{key:'0'})):
                    rejected(lambda:stage(repo, refused))
                assert not list(refused.iterdir())
            # Commit only this tiny fixture to prove full archive support closure.
            for args in (['init','-q'], ['add','.'], ['-c','user.name=Roman Bartusiak','-c','user.email=riomus@gmail.com','commit','-qm','Synthetic selector qualification fixture']):
                subprocess.run(['git', *args], cwd=repo, check=True, stdout=subprocess.PIPE)
            commit = subprocess.check_output(['git','rev-parse','HEAD'], cwd=repo, text=True).strip()
            assert hierarchical_archive_authority(repo, commit, inventory, str(manifest_path)) == {key:config[key] for key in ARCHIVE_FIELDS}
            # Authenticate the existing controller -> config -> exact native bundle chain.
            answers = {
                ('status','--porcelain'):'',
                ('rev-list','--parents','-n','1','HEAD'):'6'*40+' '+'5'*40,
                ('rev-list','--parents','-n','1','5'*40):'5'*40+' '+'4'*40,
                ('for-each-ref','--contains='+'5'*40,'--format=%(refname)','refs/remotes/origin/'):'refs/remotes/origin/master',
                ('diff','--no-renames','--name-only','4'*40,'5'*40):str(CONFIG),
                ('diff','--no-renames','--name-only','5'*40,'HEAD'):'\n'.join(NATIVE_DELTA),
                **{('show','7'*40+':'+name):bodies[name] for name in NATIVE_DELTA},
                **({('diff','--no-renames','--diff-filter=A','--name-only','5'*40,'HEAD'):'\n'.join(NATIVE_DELTA)} if cohere_cohort else {}),
                **({('diff','--no-renames','--diff-filter=M','--name-only','5'*40,'HEAD'):'\n'.join(NATIVE_DELTA)} if retained_generation_rebind else {})}
            failures = [(None,None)] + [(key,value) for key,value in (
                (('status','--porcelain'),'dirty'),
                (('rev-list','--parents','-n','1','HEAD'),'6'*40+' '+'5'*40+' '+'4'*40),
                (('rev-list','--parents','-n','1','5'*40),'5'*40+' '+'8'*40),
                (('for-each-ref','--contains='+'5'*40,'--format=%(refname)','refs/remotes/origin/'),''),
                (('diff','--no-renames','--name-only','4'*40,'5'*40),str(CONFIG)+'\nextra'),
                (('diff','--no-renames','--name-only','5'*40,'HEAD'),'' if len(NATIVE_DELTA) == 1 else NATIVE_DELTA[0]),
                (('diff','--no-renames','--name-only','5'*40,'HEAD'),'\n'.join((*NATIVE_DELTA,'extra.rs'))),
                (('show','7'*40+':'+NATIVE_DELTA[0]),b'wrong native blob'))]
            if retained_generation_rebind:
                modified = ('diff','--no-renames','--diff-filter=M','--name-only','5'*40,'HEAD')
                failures += [(modified, ''), (modified, '\n'.join(NATIVE_DELTA[:2])), (modified, '\n'.join(reversed(NATIVE_DELTA))), (modified, '\n'.join((*NATIVE_DELTA, 'crates/borsuk/src/new_rust_path.rs')))]
            if cohere_cohort and not (exact_sq8_runtime or retained_generation_rebind):
                failures.append((('diff','--no-renames','--diff-filter=A','--name-only','5'*40,'HEAD'), ''))
            for key, value in failures:
                changed = dict(answers)
                if key is not None:
                    changed[key] = value
                with patch.object(module, 'qualify', return_value=proof), patch.object(module, 'hierarchical_archive_authority', return_value={key:proof[key] for key in ARCHIVE_FIELDS}), \
                        patch.object(subprocess, 'check_output', side_effect=lambda args, **kw:changed[tuple(args[1:])]):
                    if key is None:
                        assert preflight(repo) == proof
                    else:
                        rejected(lambda:preflight(repo))
            body = user_data('0'*40, '1'*64, 'sources/mock', PREFIX+'a0001', proof)
            assert archive_binding_prefix()+'SOURCE_ARCHIVE' in body and len(body.encode()) < 16384
            files = _worker_self_check(proof, config_body, manifest_body)
            _collection_self_check(proof, files, body)
            _lifecycle_self_check(proof)
    if retained_generation_rebind:  # restoring the default mode puts the original subnet back
        with execution_mode():
            assert SUBNET == FIXED['subnet_id'] == old.SUBNET == FULL_FIXED['subnet_id']
    print('PASS '+tag+(' glue: five serial gates (retained_semantic_ integration tests, publisher bin old+new tests, release publisher+baseline, clippy, test-build); roster='+('FROZEN' if RETAINED_REBIND_REQUIRED_TESTS else 'PENDING (synthetic names)')+'; native roster names must be real fns;' if retained_generation_rebind else ' glue: seven serial gates (7 scorer tests debug+release, returned nonzero, release baseline+probe binaries, one-CPU probe, clippy, test-build);' if exact_sq8_runtime else ' glue: six serial bin gates (publisher 2 + old 16 names, release 3 bins);' if cohere_cohort else ' glue: six serial gates; debug/release oracle once per stage;' if cohere1024 else ' glue: four serial gates;')+' exact names/counts/resources; pending/source/archive/tee/ACK/termination refusals; historical protocol bytes preserved; SYNTHETIC MOCKS ONLY, Rust UNRUN')


def _budget_object_fitter_self_check():
    return _budget_object_selector_self_check(budget_object_fitter=True)

def _co_selection_self_check():
    """Qualification mocks with labelled synthetic names; no native execution."""
    if __name__ == '__main__':
        from scripts import launch_native_workspace_execution_spot as authority
        return authority._co_selection_self_check()
    import inspect
    module = sys.modules[__name__]
    previous = CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag()
    for function in (configure, execution_mode, worker.main, self_check):
        assert 'co_selection' in inspect.signature(function).parameters
    with patch.object(module, 'CO_SELECTION_REQUIRED_TESTS', {}), execution_mode(co_selection=True):
        assert FIXED['mandatory_test_names_pending'] is True
        rejected(co_selection_required_tests)
        rejected(lambda:validate_co_selection_config(FIXED))
        rejected(lambda:record_constrained_split_stage(
            ['release', 'start', '', '', '', '', *dict(CO_SELECTION_STAGES)['release']], co_selection=True))
    synthetic = {'co-selection-tests': ('co_selection_layout::tests::synthetic_contract_fixture',),
                 'co-selection-bin-tests': ('tests::co_selection_synthetic_contract_fixture',)}
    with patch.object(module, 'CO_SELECTION_REQUIRED_TESTS', synthetic), execution_mode(co_selection=True):
        assert CO_SELECTION and MINIMAL_ARCHIVE and not PQ_RESIDUAL
        assert str(ROOT).endswith('semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates')
        assert PREFIX == 'research/semantic-router/20261006/co-selection-implementation-'
        assert mode_flag() == ' --co-selection-implementation'
        assert archive_binding_prefix() == 'BORSUK_CO_SELECTION_'
        assert CONFIG_SCHEMA == FIXED['schema'] == 'borsuk-co-selection-implementation-gates-v1'
        assert SCHEMA == 'borsuk-co-selection-implementation-gates-spot-v1'
        assert RECEIPT_SCHEMA == 'borsuk-co-selection-implementation-gates-receipt-v1'
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_co_selection_implementation.sh')
        assert NATIVE_DELTA == CO_SELECTION_DELTA and len(NATIVE_DELTA) == 4 and list(NATIVE_DELTA) == sorted(NATIVE_DELTA)
        assert validate_candidate_delta(list(NATIVE_DELTA)) == list(NATIVE_DELTA)
        for index in range(len(NATIVE_DELTA)):
            rejected(lambda:validate_candidate_delta(list(NATIVE_DELTA[:index]+NATIVE_DELTA[index+1:])))
        assert len(CO_SELECTION_STAGES) == len(PQ_RESIDUAL_STAGES)+2 == 21
        for (name, command), (old_name, old_command) in zip(CO_SELECTION_STAGES[2:], PQ_RESIDUAL_STAGES):
            assert name == old_name
            assert command == old_command + (['--skip', 'tests::co_selection_'] if name == 'hierarchical-bin-regressions' else [])
        required = co_selection_required_tests()
        assert {name:required[name] for name in PQ_RESIDUAL_REQUIRED_TESTS} == PQ_RESIDUAL_REQUIRED_TESTS
        assert RELEASE_ARTIFACTS == ('binaries/hierarchical_semantic_cells',)
        assert TERMINAL_IDENTITIES == (*FULL_TERMINAL_IDENTITIES, 'controller_source_commit', 'candidate_delta_paths', *ARCHIVE_IDENTITIES)
        for key in ('CARGO_BUILD_JOBS', 'BORSUK_TEST_BUILD_JOBS', 'RUST_TEST_THREADS',
                    'BORSUK_CPU_THREADS', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS'):
            assert FIXED['environment'][key] == '1'
        assert FIXED['environment']['RUSTC_WRAPPER'] == FIXED['environment']['RUSTC_WORKSPACE_WRAPPER'] == ''
        assert all(FIXED['environment'][key] is None for key in ('BORSUK_TEST_BUILD_COMMAND', 'BORSUK_FINE_TEST_ROOT', 'BORSUK_FINE_TEST_REMOTE'))
        for mode in ('test_build', 'startup_wave8', 'root_reuse', 'bounded_publication',
                     'fixed48', 'hierarchical_cells', 'constrained_split', 'cell_overlap', 'fine_sq8', 'pq_residual'):
            rejected(lambda:configure(co_selection=True, **{mode:True}))
        rejected(lambda:configure(co_selection=1))
        for key,value in (('mandatory_test_names_pending', True), ('mandatory_tests', {})):
            rejected(lambda:validate_co_selection_config(dict(FIXED, **{key:value})))
            rejected(lambda:validate_co_selection_config({k:v for k,v in FIXED.items() if k != key}))
        duplicate = dict(synthetic, **{'co-selection-tests': synthetic['co-selection-tests']*2})
        with patch.object(module, 'CO_SELECTION_REQUIRED_TESTS', duplicate):
            rejected(co_selection_required_tests)
        _hierarchical_cells_script_self_check(co_selection=True)
        _pq_residual_shell_self_check(co_selection=True)
        _fixed48_stages_self_check(co_selection=True)
        _startup_wave8_preflight_self_check(co_selection=True)
        _hierarchical_archive_self_check(co_selection=True)
        _self_check()
    assert previous == (CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag())
    print('PASS co-selection qualification SYNTHETIC MOCKS ONLY: 21 serial gates; pending names, exact rosters, archive/source drift, tee/cleanup/terminal refusals; Rust UNRUN')


def _pq_residual_self_check():
    """Bounded Python qualification mocks; native work remains root-owned."""
    import inspect
    module = sys.modules[__name__]
    previous = CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag()
    for function in (configure, execution_mode, worker.main, self_check):
        assert 'pq_residual' in inspect.signature(function).parameters
    with execution_mode(pq_residual=True):
        assert PQ_RESIDUAL and MINIMAL_ARCHIVE and not FINE_SQ8
        assert str(ROOT).endswith('semantic-1m/fine-sq8-groups/sq4-refinement/pq-residual/implementation-gates')
        assert PREFIX == 'research/semantic-router/20261006/pq-residual-implementation-'
        assert mode_flag() == ' --pq-residual-implementation'
        assert CONFIG_SCHEMA == FIXED['schema'] == 'borsuk-pq-residual-implementation-gates-v1'
        assert SCHEMA == 'borsuk-pq-residual-implementation-gates-spot-v1'
        assert RECEIPT_SCHEMA == 'borsuk-pq-residual-implementation-gates-receipt-v1'
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_pq_residual_implementation.sh')
        assert NATIVE_DELTA == PQ_RESIDUAL_DELTA and len(NATIVE_DELTA) == 5 and list(NATIVE_DELTA) == sorted(NATIVE_DELTA)
        assert validate_candidate_delta(list(NATIVE_DELTA)) == list(NATIVE_DELTA)
        for index in range(len(NATIVE_DELTA)):
            rejected(lambda:validate_candidate_delta(list(NATIVE_DELTA[:index]+NATIVE_DELTA[index+1:])))
        rejected(lambda:validate_candidate_delta(sorted((*NATIVE_DELTA, 'crates/borsuk/src/resident_vector_graph.rs'))))
        required = pq_residual_required_tests()
        assert [len(required[name]) for name, _ in PQ_RESIDUAL_STAGES[:4]] == [5, 5, 1, 1]
        assert all('--test-threads=1' in command for _,command in PQ_RESIDUAL_STAGES if command[:2] == ['cargo', 'test'])
        assert RELEASE_ARTIFACTS == ('binaries/hierarchical_semantic_cells',)
        assert TERMINAL_IDENTITIES == (*FULL_TERMINAL_IDENTITIES, 'controller_source_commit', 'candidate_delta_paths', *ARCHIVE_IDENTITIES)
        for key in ('CARGO_BUILD_JOBS', 'BORSUK_TEST_BUILD_JOBS', 'RUST_TEST_THREADS',
                    'BORSUK_CPU_THREADS', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS'):
            assert FIXED['environment'][key] == '1'
        assert FIXED['environment']['RUSTC_WRAPPER'] == FIXED['environment']['RUSTC_WORKSPACE_WRAPPER'] == ''
        assert all(FIXED['environment'][key] is None for key in ('BORSUK_TEST_BUILD_COMMAND', 'BORSUK_FINE_TEST_ROOT', 'BORSUK_FINE_TEST_REMOTE'))
        for mode in ('test_build', 'startup_wave8', 'root_reuse', 'bounded_publication',
                     'fixed48', 'hierarchical_cells', 'constrained_split', 'cell_overlap', 'fine_sq8'):
            rejected(lambda:configure(pq_residual=True, **{mode:True}))
        rejected(lambda:configure(pq_residual=1))
        with patch.object(module, 'PQ_RESIDUAL_REQUIRED_TESTS', {}):
            rejected(pq_residual_required_tests)
            rejected(lambda:record_constrained_split_stage(['release', 'start', '', '', '', '', *dict(PQ_RESIDUAL_STAGES)['release']], pq_residual=True))
        duplicate = dict(required, **{'pq-residual-codec-tests': required['pq-residual-codec-tests']*2})
        with patch.object(module, 'PQ_RESIDUAL_REQUIRED_TESTS', duplicate):
            rejected(pq_residual_required_tests)
        for key,value in (('mandatory_test_names_pending', True), ('mandatory_tests', {})):
            rejected(lambda:validate_pq_residual_config(dict(FIXED, **{key:value})))
            rejected(lambda:validate_pq_residual_config({k:v for k,v in FIXED.items() if k != key}))
        _hierarchical_cells_script_self_check(pq_residual=True)
        _pq_residual_shell_self_check()
        _fixed48_stages_self_check(pq_residual=True)
        _startup_wave8_preflight_self_check(pq_residual=True)
        _hierarchical_archive_self_check(pq_residual=True)
        _self_check()
    assert previous == (CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag())
    print('PASS PQ residual qualification MOCKS ONLY: exact 5+5+1+doc, serial regressions, dynamic native/archive inventory, drift/tee/cleanup/terminal refusals; Rust UNRUN')


def _fine_sq8_self_check():
    """Bounded qualification mocks; never invoke native tools or a corpus run."""
    import inspect
    module = sys.modules[__name__]
    previous = CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag()
    for function in (configure, execution_mode, worker.main, self_check):
        assert 'fine_sq8' in inspect.signature(function).parameters
    with execution_mode(fine_sq8=True):
        assert FINE_SQ8 and MINIMAL_ARCHIVE and not CELL_OVERLAP
        assert str(ROOT).endswith('semantic-1m/fine-sq8-groups/sq4-refinement/corrected-rabitq/implementation-gates')
        assert mode_flag() == ' --fine-sq8-implementation'
        assert CONFIG_SCHEMA == FIXED['schema'] == 'borsuk-corrected-four-bit-implementation-gates-v1'
        assert SCHEMA == 'borsuk-corrected-four-bit-implementation-gates-spot-v1'
        assert RECEIPT_SCHEMA == 'borsuk-corrected-four-bit-implementation-gates-receipt-v1'
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_fine_sq8_implementation.sh')
        assert NATIVE_DELTA == FINE_SQ8_DELTA and len(NATIVE_DELTA) == 4 and list(NATIVE_DELTA) == sorted(NATIVE_DELTA)
        assert validate_candidate_delta(list(FINE_SQ8_DELTA)) == list(FINE_SQ8_DELTA)
        for index in range(len(FINE_SQ8_DELTA)):
            rejected(lambda:validate_candidate_delta(list(FINE_SQ8_DELTA[:index]+FINE_SQ8_DELTA[index+1:])))
        rejected(lambda:validate_candidate_delta(sorted((*FINE_SQ8_DELTA, 'crates/borsuk/src/resident_vector_graph.rs'))))
        assert dict(FINE_SQ8_STAGES)['corrected-four-bit-tests'] == 'cargo test --locked -p borsuk --lib corrected_four_bit:: -- --test-threads=1'.split()
        rejected(lambda:validate_candidate_delta([FINE_SQ8_DELTA[0], FINE_SQ8_DELTA[0]]))
        rejected(lambda:validate_candidate_delta(['crates/borsuk/src/unowned.rs']))
        assert RELEASE_ARTIFACTS == ('binaries/hierarchical_semantic_cells',)
        for key in ('CARGO_BUILD_JOBS', 'BORSUK_TEST_BUILD_JOBS', 'RUST_TEST_THREADS',
                    'BORSUK_CPU_THREADS', 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS'):
            assert FIXED['environment'][key] == '1'
        assert FIXED['environment']['BORSUK_TEST_BUILD_COMMAND'] is None
        assert FIXED['environment']['BORSUK_FINE_TEST_ROOT'] is FIXED['environment']['BORSUK_FINE_TEST_REMOTE'] is None
        assert FIXED['memory_bytes'] == 8*1024**3 and FIXED['swap_bytes'] == 0
        assert FIXED['cpu_quota_percent'] == 200 and FIXED['tasks_max'] == 512
        for mode in ('test_build', 'startup_wave8', 'root_reuse', 'bounded_publication',
                     'fixed48', 'hierarchical_cells', 'constrained_split', 'cell_overlap'):
            rejected(lambda:configure(fine_sq8=True, **{mode:True}))
        rejected(lambda:configure(fine_sq8=1))
        with patch.object(module, 'FINE_SQ8_REQUIRED_TESTS', {}):
            rejected(fine_sq8_required_tests)
            rejected(lambda:record_constrained_split_stage(['release', 'start', '', '', '', '', *dict(FINE_SQ8_STAGES)['release']], fine_sq8=True))
        duplicate = copy.deepcopy(FINE_SQ8_REQUIRED_TESTS)
        first = next(iter(duplicate))
        duplicate[first] = (*duplicate[first], duplicate[first][0])
        with patch.object(module, 'FINE_SQ8_REQUIRED_TESTS', duplicate):
            rejected(fine_sq8_required_tests)
        for key,value in (('mandatory_test_names_pending', True), ('mandatory_tests', {})):
            rejected(lambda:validate_fine_sq8_config(dict(FIXED, **{key:value})))
            rejected(lambda:validate_fine_sq8_config({k:v for k,v in FIXED.items() if k != key}))
        _hierarchical_cells_script_self_check(fine_sq8=True)
        _fixed48_stages_self_check(fine_sq8=True)
        _startup_wave8_preflight_self_check(fine_sq8=True)
        _hierarchical_archive_self_check(fine_sq8=True)
        _self_check()
    assert previous == (CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag())
    print('PASS fine SQ8 qualification MOCKS ONLY: exact roster/commands/counts, source drift, native/tee exit, authenticated terminal and same-ID cleanup; Rust UNRUN')


def _cell_overlap_self_check():
    """Qualification glue only: no native tools, production archive or network."""
    import inspect
    module = sys.modules[__name__]
    previous = CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag()
    for function in (configure, execution_mode, worker.main, self_check):
        assert 'cell_overlap' in inspect.signature(function).parameters
    with execution_mode(cell_overlap=True):
        assert CELL_OVERLAP and MINIMAL_ARCHIVE and not CONSTRAINED_SPLIT
        assert str(ROOT).endswith('semantic-1m/hierarchical-cells/boundary-overlap/implementation-gates')
        assert mode_flag() == ' --cell-overlap-implementation'
        assert CONFIG_SCHEMA == FIXED['schema'] == 'borsuk-cell-overlap-implementation-gates-v1'
        assert SCHEMA == 'borsuk-cell-overlap-implementation-gates-spot-v1'
        assert RECEIPT_SCHEMA == 'borsuk-cell-overlap-implementation-gates-receipt-v1'
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_cell_overlap_implementation.sh')
        assert NATIVE_DELTA == CELL_OVERLAP_DELTA and len(NATIVE_DELTA) == 5 and list(NATIVE_DELTA) == sorted(NATIVE_DELTA)
        assert RELEASE_ARTIFACTS == ('binaries/hierarchical_semantic_cells',)
        assert TERMINAL_IDENTITIES == (*FULL_TERMINAL_IDENTITIES, 'controller_source_commit', 'candidate_delta_paths', *ARCHIVE_IDENTITIES)
        assert FIXED['environment']['CARGO_BUILD_JOBS'] == FIXED['environment']['BORSUK_TEST_BUILD_JOBS'] == '1'
        assert FIXED['environment']['BORSUK_TEST_BUILD_COMMAND'] is None
        for mode in ('test_build', 'startup_wave8', 'root_reuse', 'bounded_publication', 'fixed48', 'hierarchical_cells', 'constrained_split'):
            rejected(lambda:configure(cell_overlap=True, **{mode:True}))
        rejected(lambda:configure(cell_overlap=1))
        with patch.object(module, 'CELL_OVERLAP_REQUIRED_TESTS', {}):
            rejected(cell_overlap_required_tests)
        for key,value in (('mandatory_test_names_pending', True), ('mandatory_tests', {})):
            rejected(lambda:validate_cell_overlap_config(dict(FIXED, **{key:value})))
        _hierarchical_cells_script_self_check(cell_overlap=True)
        _fixed48_stages_self_check(cell_overlap=True)
        _startup_wave8_preflight_self_check(cell_overlap=True)
        _hierarchical_archive_self_check(cell_overlap=True)
        _self_check()
    assert previous == (CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag())
    print('PASS cell overlap qualification MOCKS ONLY: exact names, seven serial stages, dynamic inventory/archive, native exit/closure; no native qualification claimed')


def _hierarchical_cells_script_self_check(*, constrained_split=False, cell_overlap=False, fine_sq8=False, pq_residual=False, co_selection=False):
    """Execute the real stage recorder as Python; never invoke Cargo or Bash."""
    from shlex import split
    script = (Path(__file__).resolve().parent/('check_co_selection_implementation.sh' if co_selection else 'check_pq_residual_implementation.sh' if pq_residual else 'check_fine_sq8_implementation.sh' if fine_sq8 else 'check_cell_overlap_implementation.sh' if cell_overlap else 'check_constrained_split_implementation.sh' if constrained_split else 'check_hierarchical_cells_implementation.sh')).read_text()
    actual = tuple((args[1], args[2:]) for line in script.splitlines()
        if line.startswith('run_stage ') for args in (split(line),))
    stages = CO_SELECTION_STAGES if co_selection else PQ_RESIDUAL_STAGES if pq_residual else FINE_SQ8_STAGES if fine_sq8 else CELL_OVERLAP_STAGES if cell_overlap else CONSTRAINED_SPLIT_STAGES if constrained_split else HIERARCHICAL_CELLS_STAGES
    required = co_selection_required_tests() if co_selection else pq_residual_required_tests() if pq_residual else fine_sq8_required_tests() if fine_sq8 else cell_overlap_required_tests() if cell_overlap else constrained_split_required_tests() if constrained_split else HIERARCHICAL_CELLS_REQUIRED_TESTS
    assert actual == stages, 'exact shell stage commands'
    assert 'set -euo pipefail' in script and 'return "$status"' in script and 'statuses=("${PIPESTATUS[@]}")' in script
    assert 'env -u BORSUK_TEST_BUILD_COMMAND' in script
    recorder = script.split('stage_record() {', 1)[1].split("python3 -c '", 1)[1].split("' \"$@\"", 1)[0]
    compile(recorder, '<stage-recorder>', 'exec')
    with tempfile.TemporaryDirectory() as tmp:
        log = Path(tmp)/'stage.log'
        for index, (name, command) in enumerate(stages):
            names = required.get(name, ())
            if pq_residual or co_selection:
                argv = ['recorder', name, '2026-10-06T00:00:00Z', '', '', '', '', *command]
                with patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()) as output:
                    try:
                        exec(recorder, {})
                    except SystemExit as result:
                        assert result.code == 0
                record = json.loads(output.getvalue())
                assert record['stage'] == name and record['command'] == command
                assert all(record[key] is None for key in ('finished_at', 'exit_status', 'log_exit_status',
                    'gate_status', 'tests_run', 'required_test_passes'))
            lines = ['test '+test+' ... ok' for test in names]
            if (cell_overlap or fine_sq8 or pq_residual or co_selection) and not names and name not in ('release', 'clippy', 'test-build'):
                lines.append('test tests::regression ... ok')
            if name not in ('release', 'clippy', 'test-build'):
                lines.append(f'test result: ok. {max(1, len(names))} passed; 0 failed; 0 ignored; 0 measured; 100 filtered out; finished in 0.00s')
            if (cell_overlap or fine_sq8 or pq_residual or co_selection) and name == 'test-build':
                lines.append('rust-test-build status=0 elapsed_seconds=0 jobs=1')
            good = '\n'.join(lines)+'\n'
            cases = [(good, '0', '0', 0), (good, '17', '0', 17), (good, '0', '18', 18)]
            if (pq_residual or co_selection) and name.startswith(('pq-residual-', 'co-selection-')):
                extra = good.replace(f'{len(names)} passed', f'{len(names)+1} passed')
                cases.append(('test tests::unexpected ... ok\n'+extra, '0', '0', 96))
                cases.extend(('test tests::unexpected ... '+result+'\n'+good, '0', '0', 96) for result in ('FAILED', 'ignored'))
                cases.append((good.replace('test result: ok.', 'test result: FAILED.'), '0', '0', 96))
            if (pq_residual or co_selection) and name == 'pq-residual-doc-tests':
                cases.append((good.replace('test src/', 'test crates/borsuk/src/'), '0', '0', 0))
                cases.append((good.replace('(line 13)', '(line 14)'), '0', '0', 96))
            if (pq_residual or co_selection) and name == 'fine-sq8-tests':
                extra = good.replace(f'{len(names)} passed', f'{len(names)+1} passed')
                cases.append(('test '+required['pq-residual-source-tests'][0]+' ... ok\n'+extra, '0', '0', 96))
            if name not in ('release', 'clippy', 'test-build'):
                cases.append(('', '0', '0', 96))
                if constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection:
                    cases.append((good.replace('0 ignored', '1 ignored'), '0', '0', 96))
            if (cell_overlap or fine_sq8 or pq_residual or co_selection) and name == 'test-build':
                cases.extend((body, '0', '0', 96) for body in ('', good+good, good.replace('jobs=1', 'jobs=2')))
            for test in names:
                passed = 'test '+test+' ... ok'
                for replacement in ('', passed.replace('ok', 'ignored'), passed.replace('ok', 'FAILED'), passed+'\n'+passed):
                    cases.append((good.replace(passed, replacement), '0', '0', 96))
            for body, status, log_status, expected in cases:
                log.write_text(body)
                argv = ['recorder', name, '2026-10-03T00:00:00Z', '2026-10-03T00:00:00Z', status, str(log), log_status, *command]
                with patch.object(sys, 'argv', argv), contextlib.redirect_stdout(io.StringIO()) as output:
                    try:
                        exec(recorder, {})
                    except SystemExit as result:
                        assert result.code == expected
                record = json.loads(output.getvalue())
                assert record['gate_status'] == expected and record['command'] == command
                assert record['schema'] == (CO_SELECTION_STAGE_SCHEMA if co_selection else PQ_RESIDUAL_STAGE_SCHEMA if pq_residual else FINE_SQ8_STAGE_SCHEMA if fine_sq8 else CELL_OVERLAP_STAGE_SCHEMA if cell_overlap else CONSTRAINED_SPLIT_STAGE_SCHEMA if constrained_split else HIERARCHICAL_CELLS_STAGE_SCHEMA)


def _pq_residual_shell_self_check(*, co_selection=False):
    """Real Bash start/command/tee/end/cleanup; only Cargo and source checks are fake."""
    script = (Path(__file__).resolve().parent/('check_co_selection_implementation.sh' if co_selection else 'check_pq_residual_implementation.sh')).read_text()
    functions = script[script.index('stage_record() {'):script.index('# Cheap synthetic native falsifiers')]
    if co_selection:
        # Explicit mock-only authority in the subprocess; never a native roster.
        functions = functions.replace('sys.exit(record_constrained_split_stage',
            'from scripts import launch_native_workspace_execution_spot as c\nc.CO_SELECTION_REQUIRED_TESTS = '+encoded(CO_SELECTION_REQUIRED_TESTS).decode()+'\nsys.exit(record_constrained_split_stage')
    name, command = (CO_SELECTION_STAGES if co_selection else PQ_RESIDUAL_STAGES)[0]
    names = (co_selection_required_tests() if co_selection else pq_residual_required_tests())[name]
    good = ''.join('test '+test+' ... ok\n' for test in names)
    good += f'test result: ok. {len(names)} passed; 0 failed; 0 ignored; 0 measured; 100 filtered out; finished in 0.00s\n'
    with tempfile.TemporaryDirectory() as tmp:
        fake = Path(tmp)/'cargo'
        fake.write_text('#!/usr/bin/env python3\nimport os,sys\nprint('+repr(good)+',end="",flush=True)\nsys.exit(int(os.environ["FAKE_CARGO_EXIT"]))\n')
        fake.chmod(0o755)
        for native_status, tee_status in ((0, 0), (17, 0), (0, 18)):
            env = dict(os.environ, PATH=tmp+os.pathsep+os.environ['PATH'], TMPDIR=tmp,
                PYTHONDONTWRITEBYTECODE='1', FAKE_CARGO_EXIT=str(native_status))
            tee = ('tee() { /usr/bin/tee "$@"; return 18; }\n' if tee_status else '')
            body = 'set -euo pipefail\nsource_check() { :; }\n'+functions+tee
            body += 'run_stage '+name+' '+' '.join(command)+'\n'
            result = subprocess.run(['bash', '-c', body], cwd=Path(__file__).resolve().parents[1],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=30)
            assert result.returncode == (native_status or tee_status), result.stderr
            records = [json.loads(line) for line in result.stdout.splitlines() if line.startswith('{')]
            assert len(records) == 2 and all(record['stage'] == name and record['command'] == command for record in records)
            start, end = records
            assert start['finished_at'] is start['exit_status'] is start['gate_status'] is None
            assert end['exit_status'] == native_status and end['log_exit_status'] == tee_status
            assert end['gate_status'] == (native_status or tee_status) and end['tests_run'] == len(names)
            assert end['required_test_passes'] == dict.fromkeys(names, 1)
            assert list(Path(tmp).iterdir()) == [fake], 'stage log cleaned on success and failure'


def _fixed48_stages_self_check(*, hierarchical_cells=False, constrained_split=False, cell_overlap=False, fine_sq8=False, pq_residual=False, co_selection=False):
    import inspect
    assert 'fixed48' in inspect.signature(validate_bounded_publication_stages).parameters, 'fixed48 stage authentication missing'
    stages = CO_SELECTION_STAGES if co_selection else PQ_RESIDUAL_STAGES if pq_residual else FINE_SQ8_STAGES if fine_sq8 else CELL_OVERLAP_STAGES if cell_overlap else CONSTRAINED_SPLIT_STAGES if constrained_split else HIERARCHICAL_CELLS_STAGES if hierarchical_cells else FIXED48_STAGES
    schema = CO_SELECTION_STAGE_SCHEMA if co_selection else PQ_RESIDUAL_STAGE_SCHEMA if pq_residual else FINE_SQ8_STAGE_SCHEMA if fine_sq8 else CELL_OVERLAP_STAGE_SCHEMA if cell_overlap else CONSTRAINED_SPLIT_STAGE_SCHEMA if constrained_split else HIERARCHICAL_CELLS_STAGE_SCHEMA if hierarchical_cells else FIXED48_STAGE_SCHEMA
    required = co_selection_required_tests() if co_selection else pq_residual_required_tests() if pq_residual else fine_sq8_required_tests() if fine_sq8 else cell_overlap_required_tests() if cell_overlap else constrained_split_required_tests() if constrained_split else HIERARCHICAL_CELLS_REQUIRED_TESTS if hierarchical_cells else FIXED48_REQUIRED_TESTS
    with tempfile.TemporaryDirectory() as tmp:
        log = Path(tmp)/'test.log'
        lines = []
        for index, (name, command) in enumerate(stages):
            record = dict(schema=schema, stage=name, command=command,
                started_at='2026-10-02T00:00:00Z', finished_at=None,
                exit_status=None, gate_status=None, tests_run=None, required_test_passes=None)
            if constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection:
                record['log_exit_status'] = None
            lines.append(encoded(record).decode())
            names = required.get(name, ())
            lines.extend('test '+test+' ... ok' for test in names)
            if (cell_overlap or fine_sq8 or pq_residual or co_selection) and not names and name not in ('release', 'clippy', 'test-build'):
                lines.append('test tests::regression ... ok')
            tests = max(1, len(names)) if name not in ('release', 'clippy', 'test-build') else None
            if name not in ('release', 'clippy', 'test-build'):
                lines.append(f'test result: ok. {tests} passed; 0 failed; 0 ignored; 0 measured; 100 filtered out; finished in 0.00s')
            if (hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection) and name == 'test-build':
                lines.append('rust-test-build status=0 elapsed_seconds=0 jobs=1')
            record.update(finished_at=record['started_at'], exit_status=0, gate_status=0,
                tests_run=tests, required_test_passes={test: 1 for test in names})
            if constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection:
                record['log_exit_status'] = 0
            lines.append(encoded(record).decode())
        log.write_text('\n'.join(lines)+'\n')
        records = validate_bounded_publication_stages(log, fixed48=not (hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection), hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection)
        assert len(records) == len(stages) and [r['tests_run'] for r in records if r['stage'] not in ('release', 'clippy', 'test-build')] == [max(1,len(required.get(name, ()))) for name,_ in stages if name not in ('release', 'clippy', 'test-build')]
        if pq_residual or co_selection:
            doc = next(test for test in required['pq-residual-doc-tests'])
            log.write_text('\n'.join(lines).replace('test '+doc, 'test crates/borsuk/'+doc)+'\n')
            assert len(validate_bounded_publication_stages(log, pq_residual=pq_residual, co_selection=co_selection)) == len(stages)
            # Reject coherent-looking count/receipt inflation and foreign failures.
            for index, (name, _) in ((i, stage) for i, stage in enumerate(stages) if stage[0].startswith(('pq-residual-', 'co-selection-'))):
                end = 2*index+1
                positions = [i for i,line in enumerate(lines) if line.startswith('{')]
                for status in ('ok', 'FAILED', 'ignored'):
                    changed = list(lines)
                    if status == 'ok':
                        summary = positions[end]-1
                        count = len(required[name])
                        changed[summary] = changed[summary].replace(f'{count} passed', f'{count+1} passed')
                        record = json.loads(changed[positions[end]])
                        changed[positions[end]] = encoded(dict(record, tests_run=count+1)).decode()
                    changed.insert(positions[2*index]+1, 'test tests::unexpected ... '+status)
                    log.write_text('\n'.join(changed)+'\n')
                    rejected(lambda:validate_bounded_publication_stages(log, pq_residual=pq_residual, co_selection=co_selection))
        for names in required.values():
            for name in names:
                passed = 'test '+name+' ... ok'
                for replacement in ('', passed.replace('ok', 'ignored'), passed.replace('ok', 'FAILED'), passed+'\n'+passed):
                    log.write_text('\n'.join(lines).replace(passed, replacement)+'\n')
                    rejected(lambda: validate_bounded_publication_stages(log, fixed48=not (hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection), hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection))
                misplaced = [line for line in lines if line != passed]
                misplaced.insert(0, passed)
                log.write_text('\n'.join(misplaced)+'\n')
                rejected(lambda: validate_bounded_publication_stages(log, fixed48=not (hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection), hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection))
        for index, line in enumerate(lines):
            if not line.startswith('{'):
                continue
            record = json.loads(line)
            changes = [('command', ['cargo', 'test', '--workspace']), ('stage', 'wrong-stage')]
            if record['finished_at']:
                changes += [('exit_status', 17), ('exit_status', False), ('gate_status', 96),
                            ('required_test_passes', {}), ('finished_at', '2026-10-01T00:00:00Z')]
                if constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection:
                    changes += [('log_exit_status', 18), ('log_exit_status', False)]
                if record['tests_run'] is not None:
                    changes += [('tests_run', 0), ('tests_run', False), ('tests_run', 99)]
            else:
                changes += [('exit_status', 0), ('required_test_passes', {})]
            for key, value in changes:
                if record[key] == value and type(record[key]) is type(value):
                    continue
                changed = list(lines)
                changed[index] = encoded(dict(record, **{key: value})).decode()
                log.write_text('\n'.join(changed)+'\n')
                rejected(lambda: validate_bounded_publication_stages(log, fixed48=not (hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection), hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection))
        if hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection:
            build_proof = 'rust-test-build status=0 elapsed_seconds=0 jobs=1'
            for replacement in ('', 'rust-test-build status=0 elapsed_seconds=0 jobs=2', build_proof+'\n'+build_proof):
                log.write_text('\n'.join(lines).replace(build_proof, replacement)+'\n')
                rejected(lambda: validate_bounded_publication_stages(log, hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection))
        for changed in (lines[:-1], lines+lines[-2:], list(reversed(lines))):
            log.write_text('\n'.join(changed)+'\n')
            rejected(lambda: validate_bounded_publication_stages(log, fixed48=not (hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection), hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection))
        if cell_overlap:
            # Narrow test passes may recur once in the two module regressions.
            changed = list(lines)
            for name, prefix in (('hierarchical-cell-regressions', 'hierarchical_semantic_cells::'),
                                 ('returned-sq8-regressions', 'returned_sq8::')):
                test = next(test for test in required['overlap-tests'] if test.startswith(prefix))
                start = next(i for i,line in enumerate(changed) if line.startswith('{') and
                    json.loads(line)['stage'] == name and json.loads(line)['finished_at'] is None)
                changed[start+1] = 'test '+test+' ... ok'
            log.write_text('\n'.join(changed)+'\n')
            assert len(validate_bounded_publication_stages(log, cell_overlap=True)) == len(stages)
            for name in ('hierarchical-cell-regressions', 'returned-sq8-regressions'):
                duplicate = list(changed)
                start = next(i for i,line in enumerate(duplicate) if line.startswith('{') and
                    json.loads(line)['stage'] == name and json.loads(line)['finished_at'] is None)
                duplicate.insert(start+1, duplicate[start+1])
                log.write_text('\n'.join(duplicate)+'\n')
                rejected(lambda:validate_bounded_publication_stages(log, cell_overlap=True))


def _startup_wave8_preflight_self_check(*, root_reuse=False, bounded_publication=False, fixed48=False, hierarchical_cells=False, constrained_split=False, cell_overlap=False, fine_sq8=False, pq_residual=False, co_selection=False):
    controller, config_commit, bundle = '4'*40, '5'*40, '6'*40
    blob = b'mocked exact candidate blob'
    with execution_mode(startup_wave8=not (root_reuse or bounded_publication or fixed48 or hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection), root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection):
        proof = dict(controller_source_commit=controller, candidate_delta_paths=list(NATIVE_DELTA),
                     native_source_commit='7'*40 if hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection else FIXED48_CHECK_COMMIT if fixed48 else BOUNDED_PUBLICATION_CHECK_COMMIT if bounded_publication else '7'*40 if root_reuse else STARTUP_WAVE8_COMMIT,
                     source_sha256={name:worker.sha(blob) for name in NATIVE_DELTA})
        if hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection:
            proof.update(source_archive_paths=[], source_archive_paths_sha256='0'*64,
                         source_archive_file_count=0, source_archive_support_sha256={},
                         native_source_manifest=dict(path=str(ROOT/'native-source-manifest.json')))
        answers = {
            ('status','--porcelain'): '',
            ('rev-list','--parents','-n','1','HEAD'): bundle+' '+config_commit,
            ('rev-list','--parents','-n','1',config_commit): config_commit+' '+controller,
            ('for-each-ref','--contains='+config_commit,'--format=%(refname)','refs/remotes/origin/'): 'refs/remotes/origin/master',
            ('diff','--no-renames','--name-only',controller,config_commit): str(CONFIG),
            ('diff','--no-renames','--name-only',config_commit,'HEAD'): '\n'.join(NATIVE_DELTA),
            **{('show',proof['native_source_commit']+':'+name):blob for name in NATIVE_DELTA}}
        if constrained_split:
            original = (Path(__file__).resolve().parents[1]/NATIVE_DELTA[1]).read_bytes()[:CONSTRAINED_SPLIT_PREFIX['bytes']]
            answers[('diff','--no-renames','--name-only',CONSTRAINED_SPLIT_CONTROL,proof['native_source_commit'])] = '\n'.join(NATIVE_DELTA)
            answers[('show',CONSTRAINED_SPLIT_CONTROL+':'+NATIVE_DELTA[1])] = original
        failures = ('success','dirty','merge-bundle','wrong-controller','unpublished-config','extra-config-delta','missing-native-delta','extra-native-delta','wrong-candidate-blob')
        if fixed48:
            failures += ('historical-two-path-bundle',)
        if hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection:
            failures += ('wrong-archive-authority',)
        if constrained_split:
            failures += ('extra-original-delta', 'wrong-original-prefix')
        for failure in failures:
            changed = dict(answers)
            key, value = {
                'success': (('status','--porcelain'), ''),
                'wrong-archive-authority': (('status','--porcelain'), ''),
                'dirty': (('status','--porcelain'), 'dirty'),
                'merge-bundle': (('rev-list','--parents','-n','1','HEAD'), bundle+' '+config_commit+' '+controller),
                'wrong-controller': (('rev-list','--parents','-n','1',config_commit), config_commit+' '+'7'*40),
                'unpublished-config': (('for-each-ref','--contains='+config_commit,'--format=%(refname)','refs/remotes/origin/'), ''),
                'extra-config-delta': (('diff','--no-renames','--name-only',controller,config_commit), str(CONFIG)+'\nother.py'),
                'missing-native-delta': (('diff','--no-renames','--name-only',config_commit,'HEAD'), NATIVE_DELTA[0]),
                'extra-native-delta': (('diff','--no-renames','--name-only',config_commit,'HEAD'), '\n'.join((*NATIVE_DELTA,'other.rs'))),
                'historical-two-path-bundle': (('diff','--no-renames','--name-only',config_commit,'HEAD'), '\n'.join(NATIVE_DELTA[1:])),
                'wrong-candidate-blob': (('show',proof['native_source_commit']+':'+NATIVE_DELTA[0]), b'tampered'),
                'extra-original-delta': (('diff','--no-renames','--name-only',CONSTRAINED_SPLIT_CONTROL,proof['native_source_commit']), '\n'.join((*NATIVE_DELTA,'other.rs'))),
                'wrong-original-prefix': (('show',CONSTRAINED_SPLIT_CONTROL+':'+NATIVE_DELTA[1]), b'tampered original')
            }[failure]
            changed[key] = value
            def git(args, **kwargs):
                assert args[0] == 'git' and tuple(args[1:]) in changed, args
                return changed[tuple(args[1:])]
            with patch.object(sys.modules[__name__], 'qualify', return_value=proof), \
                    patch.object(subprocess,'check_output',side_effect=git), \
                    patch.object(sys.modules[__name__], 'hierarchical_archive_authority',
                                 return_value={key: ('1'*64 if failure == 'wrong-archive-authority' and key == 'source_archive_paths_sha256' else proof[key]) for key in ARCHIVE_FIELDS} if hierarchical_cells or constrained_split or cell_overlap or fine_sq8 or pq_residual or co_selection else {}):
                if failure == 'success':
                    assert preflight() == proof
                else:
                    rejected(lambda:preflight())


def self_check(semantic_1m=False, *, test_build=False, implementation=False, startup_wave8=False, root_reuse=False, bounded_publication=False, fixed48=False, hierarchical_cells=False, constrained_split=False, cell_overlap=False, fine_sq8=False, pq_residual=False, co_selection=False, budget_object_selector=False, budget_object_fitter=False, cohere1024=False, cohere_cohort=False, cohere_sq8_builder=False, exact_sq8_runtime=False, retained_generation_rebind=False, baseline_pool=False):
    with execution_mode(semantic_1m, test_build=test_build, implementation=implementation, startup_wave8=startup_wave8, root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection, budget_object_selector=budget_object_selector, budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort, cohere_sq8_builder=cohere_sq8_builder, exact_sq8_runtime=exact_sq8_runtime, retained_generation_rebind=retained_generation_rebind, baseline_pool=baseline_pool):
        if baseline_pool:
            return _baseline_pool_self_check()
        if cohere_sq8_builder:
            return _cohere_sq8_builder_self_check()
        if retained_generation_rebind:
            _budget_object_selector_self_check(cohere_cohort=True, retained_generation_rebind=True)
            return
        if exact_sq8_runtime:
            _budget_object_selector_self_check(cohere_cohort=True, exact_sq8_runtime=True)
            return
        if cohere_cohort:
            _budget_object_selector_self_check(cohere_cohort=True)
            return
        if cohere1024:
            _budget_object_selector_self_check(cohere1024=True)
            return
        if budget_object_fitter:
            _budget_object_fitter_self_check()
            return
        if budget_object_selector:
            _budget_object_selector_self_check()
        else:
            _self_check()


def _self_check():
    semantic_1m = SEMANTIC_1M
    module = sys.modules[__name__]
    base = Path(__file__).resolve().parents[1]
    manifest = json.loads((base/semantic.ROOT/'metadata-waves/native-source-manifest.json').read_bytes())
    inventory = manifest['source_sha256']
    if MINIMAL_ARCHIVE:
        # Synthetic authority only: the root chooses the production candidate.
        inventory = worker.source_hashes(base)
        for name in NATIVE_DELTA:
            inventory.setdefault(name, worker.sha(b'// synthetic new native source\n'))
        manifest = dict(schema='borsuk-co-selection-native-source-manifest-v1' if CO_SELECTION else 'borsuk-pq-residual-native-source-manifest-v1' if PQ_RESIDUAL else 'borsuk-corrected-four-bit-native-source-manifest-v1' if FINE_SQ8 else 'borsuk-cell-overlap-native-source-manifest-v1' if CELL_OVERLAP else 'borsuk-constrained-split-native-source-manifest-v1' if CONSTRAINED_SPLIT else 'borsuk-hierarchical-cells-native-source-manifest-v1', source_sha256=inventory,
            source_identity_sha256=worker.source_identity(inventory), source_file_count=len(inventory),
            native_source_commit='7'*40, candidate_delta_paths=list(NATIVE_DELTA),
            candidate_qualification_pending=True, control_native_source_commit=CONSTRAINED_SPLIT_CONTROL if CONSTRAINED_SPLIT else '8'*40)
    elif FIXED48:
        inventory = worker.source_hashes(base)
        for name in NATIVE_DELTA:
            inventory[name] = worker.sha(subprocess.check_output(['git','show',FIXED48_CHECK_COMMIT+':'+name], cwd=base))
        assert len(inventory) == 399
        manifest = dict(schema='borsuk-fixed48-native-source-manifest-v1', source_sha256=inventory,
            source_identity_sha256=worker.source_identity(inventory), source_file_count=399,
            native_source_commit=FIXED48_CHECK_COMMIT, candidate_delta_paths=list(NATIVE_DELTA),
            candidate_qualification_pending=True, control_native_source_commit=FIXED48_CHECK_CONTROL)
    elif BOUNDED_PUBLICATION:
        inventory = worker.source_hashes(base)
        for name in NATIVE_DELTA:
            inventory[name] = worker.sha(subprocess.check_output(['git','show',BOUNDED_PUBLICATION_CHECK_COMMIT+':'+name], cwd=base))
        assert len(inventory) == 399 and worker.source_identity(inventory) == BOUNDED_PUBLICATION_CHECK_IDENTITY
        manifest = dict(schema='borsuk-bounded-publication-native-source-manifest-v1', source_sha256=inventory,
            source_identity_sha256=BOUNDED_PUBLICATION_CHECK_IDENTITY, source_file_count=399,
            native_source_commit=BOUNDED_PUBLICATION_CHECK_COMMIT, candidate_delta_paths=list(NATIVE_DELTA),
            candidate_qualification_pending=True, control_native_source_commit=BOUNDED_PUBLICATION_CHECK_CONTROL)
    elif ROOT_REUSE:
        inventory = worker.source_hashes(base)
        manifest = dict(schema='borsuk-root-reuse-native-source-manifest-v1', source_sha256=inventory,
            source_identity_sha256=worker.source_identity(inventory), source_file_count=399,
            native_source_commit='7'*40, candidate_delta_paths=list(NATIVE_DELTA),
            candidate_qualification_pending=True, control_native_source_commit=STARTUP_WAVE8_COMMIT)
    elif STARTUP_WAVE8:
        inventory = json.loads((base/semantic.ROOT.parent/'semantic-1m/startup-wave8/candidate-native-source-manifest.json').read_bytes())['source_sha256']
        assert len(inventory) == 399 and worker.source_identity(inventory) == STARTUP_WAVE8_IDENTITY
        manifest = dict(schema='borsuk-startup-wave8-native-source-manifest-v1', source_sha256=inventory,
            source_identity_sha256=STARTUP_WAVE8_IDENTITY, source_file_count=399,
            native_source_commit=STARTUP_WAVE8_COMMIT, candidate_delta_paths=list(NATIVE_DELTA),
            candidate_qualification_pending=True, control_native_source_commit='f4d76fc040aa89c44b3526e37f148e78d21241fa')
    elif semantic_1m:
        inventory = dict(inventory, **{'Cargo.toml':'1'*64})
        manifest.update(source_sha256=inventory, source_identity_sha256=worker.source_identity(inventory))
        assert manifest['source_identity_sha256'] != SOURCE_IDENTITY
    manifest_path = ROOT/'native-source-manifest.json'
    manifest_body = encoded(manifest)
    config = dict(FIXED, controller_authority_pending=False,
        controller_code_sha256={n:worker.artifact(base/n)['sha256'] for n in CODE},
        native_source_manifest=dict(path=str(manifest_path),bytes=len(manifest_body),sha256=worker.sha(manifest_body)))
    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
        config['controller_source_commit'] = '4'*40
    if MINIMAL_ARCHIVE:
        support = {'crates/fixture/tests/fixtures/runtime.json': worker.sha(b'fixture body\n')}
        paths = sorted(set(inventory) | set(CODE) | set(support) | {str(CONFIG), str(manifest_path)})
        config.update(source_archive_paths=paths, source_archive_paths_sha256=worker.sha(encoded(paths)),
                      source_archive_file_count=len(paths), source_archive_support_sha256=support)
    with tempfile.TemporaryDirectory() as tmp:
        repo, out = Path(tmp)/'repo', Path(tmp)/'out'
        repo.mkdir(); out.mkdir()
        for name in CODE:
            (repo/name).parent.mkdir(parents=True,exist_ok=True)
            if MINIMAL_ARCHIVE:
                (repo/name).write_bytes((base/name).read_bytes())
            else:
                (repo/name).symlink_to(base/name)
        if MINIMAL_ARCHIVE:
            for name in inventory:
                (repo/name).parent.mkdir(parents=True, exist_ok=True)
                (repo/name).write_bytes(b'// fixture native body; source hashes mocked\n')
            if CONSTRAINED_SPLIT:
                (repo/NATIVE_DELTA[1]).write_bytes((base/NATIVE_DELTA[1]).read_bytes() + b'// additive fixture\n')
            for name in support:
                (repo/name).parent.mkdir(parents=True, exist_ok=True)
                (repo/name).write_bytes(b'fixture body\n')
        (repo/manifest_path).parent.mkdir(parents=True,exist_ok=True)
        (repo/manifest_path).write_bytes(manifest_body)
        (repo/CONFIG).parent.mkdir(parents=True,exist_ok=True)
        config_body = encoded(config)
        (repo/CONFIG).write_bytes(config_body)
        with patch.object(worker,'source_hashes',return_value=inventory):
            proof = qualify(repo)
            assert proof['source_identity_sha256'] == manifest['source_identity_sha256']
            if CO_SELECTION:
                assert proof['schema'] == 'borsuk-co-selection-implementation-gates-qualification-v1'
            if PQ_RESIDUAL:
                assert proof['schema'] == 'borsuk-pq-residual-implementation-gates-qualification-v1'
            if FINE_SQ8:
                assert proof['schema'] == 'borsuk-corrected-four-bit-implementation-gates-qualification-v1'
            assert proof['actual_full_workspace_execution'] is False
            stage(repo,out)
            assert json.loads((out/'source-qualification.json').read_bytes()) == proof
            for key,value in (('controller_authority_pending',True), ('memory_bytes',worker.MEMORY+1),
                              ('command',[*FIXED['command'],'--no-run']), ('environment',dict(FIXED['environment'],CARGO_BUILD_JOBS='99')),
                              ('controller_code_sha256',dict(config['controller_code_sha256'],**{CODE[0]:'0'*64})),
                              ('controller_code_sha256',{name:digest for name,digest in config['controller_code_sha256'].items() if name != CODE[0]}),
                              ('controller_code_sha256',dict(config['controller_code_sha256'],**{'scripts/unowned.py':'0'*64})),
                              ('native_source_manifest',dict(config['native_source_manifest'],sha256='0'*64))):
                (repo/CONFIG).write_bytes(encoded(dict(config,**{key:value})))
                rejected(lambda:qualify(repo))
            if CELL_OVERLAP or FINE_SQ8 or PQ_RESIDUAL or CO_SELECTION:
                for key,value in (('mandatory_test_names_pending', True), ('mandatory_tests', {}),
                        ('mandatory_tests', {k:v[:-1] for k,v in config['mandatory_tests'].items()}),
                        ('unexpected_config_key', True)):
                    (repo/CONFIG).write_bytes(encoded(dict(config, **{key:value})))
                    rejected(lambda:qualify(repo))
            if CONSTRAINED_SPLIT:
                for key,value in (('mandatory_test_names_pending', True), ('mandatory_tests', {}),
                        ('mandatory_tests', {k:v[:-1] for k,v in config['mandatory_tests'].items()}),
                        ('control_module_prefix', {}), ('control_native_source_commit', '0'*40),
                        ('unexpected_config_key', True)):
                    (repo/CONFIG).write_bytes(encoded(dict(config, **{key:value})))
                    rejected(lambda:qualify(repo))
                (repo/CONFIG).write_bytes(config_body)
                module_path = repo/NATIVE_DELTA[1]
                original_body = module_path.read_bytes()
                module_path.write_bytes(b'X'+original_body[1:])
                rejected(lambda:qualify(repo))
                module_path.write_bytes(original_body[:CONSTRAINED_SPLIT_PREFIX['bytes']])
                rejected(lambda:qualify(repo))
                module_path.write_bytes(original_body)
            if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
                for value in ('not-a-commit', '4'*39):
                    (repo/CONFIG).write_bytes(encoded(dict(config,controller_source_commit=value)))
                    rejected(lambda:qualify(repo))
            bad_manifest = [('source_identity_sha256','0'*64), ('source_file_count',len(inventory)-1), ('source_file_count',True),
                            ('native_source_commit','not-a-commit')]
            if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or MINIMAL_ARCHIVE:
                bad_manifest.extend((('candidate_delta_paths',[]),
                    ('candidate_delta_paths',dict.fromkeys(NATIVE_DELTA)),
                    ('candidate_delta_paths',list(reversed(NATIVE_DELTA))),
                    ('candidate_qualification_pending',False),('candidate_qualification_pending',1),
                    ('schema','wrong-manifest-mode'),('control_native_source_commit','not-a-commit')))
                if FIXED48:
                    bad_manifest.append(('candidate_delta_paths',list(FIXED48_DELTA[1:])))
                if FINE_SQ8 or PQ_RESIDUAL or CO_SELECTION:
                    bad_manifest.extend(('candidate_delta_paths',list(NATIVE_DELTA[:index]+NATIVE_DELTA[index+1:]))
                        for index in range(len(NATIVE_DELTA)))
                    bad_manifest.append(('candidate_delta_paths', sorted((*NATIVE_DELTA, 'crates/borsuk/src/resident_vector_graph.rs'))))
                if CONSTRAINED_SPLIT:
                    bad_manifest.append(('control_native_source_commit', '0'*40))
                if STARTUP_WAVE8:
                    bad_manifest.append(('native_source_commit','0'*40))
            for key,value in bad_manifest:
                (repo/manifest_path).write_bytes(encoded(dict(manifest,**{key:value})))
                pointer = dict(path=str(manifest_path),**worker.artifact(repo/manifest_path))
                (repo/CONFIG).write_bytes(encoded(dict(config,native_source_manifest=pointer)))
                rejected(lambda:qualify(repo))
            (repo/manifest_path).write_bytes(manifest_body)
            (repo/CONFIG).write_bytes(config_body)
        if not semantic_1m:
            changed = dict(inventory,**{'Cargo.toml':'1'*64})
            (repo/manifest_path).write_bytes(encoded(dict(manifest,source_sha256=changed,
                source_identity_sha256=worker.source_identity(changed))))
            pointer = dict(path=str(manifest_path),**worker.artifact(repo/manifest_path))
            (repo/CONFIG).write_bytes(encoded(dict(config,native_source_manifest=pointer)))
            with patch.object(worker,'source_hashes',return_value=changed):
                rejected(lambda:qualify(repo))  # Default cannot repin historical native code.
            (repo/manifest_path).write_bytes(manifest_body)
            (repo/CONFIG).write_bytes(config_body)
        with patch.object(worker,'source_hashes',return_value=dict(inventory,**{'Cargo.toml':'0'*64})):
            rejected(lambda:qualify(repo))
        with patch.object(subprocess,'check_output',return_value='dirty'):
            rejected(lambda:preflight(repo))
        with patch.object(subprocess,'check_output',side_effect=['','']):
            rejected(lambda:preflight(repo))
        body = user_data('0'*40,'1'*64,'sources/mock',PREFIX+'a0001',proof)
        assert len(CODE) == len(set(CODE)) == (24 if IMPLEMENTATION else 23 if TEST_BUILD else 22)
        assert len(ARTIFACTS) == len(set(ARTIFACTS)) == (14 if CO_SELECTION or PQ_RESIDUAL or FINE_SQ8 or CELL_OVERLAP else 16 if CONSTRAINED_SPLIT else 17 if HIERARCHICAL_CELLS else 16 if FIXED48 else 14 if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION else 18 if IMPLEMENTATION else 13)
        assert '--on-active=9000s' in body and 'RuntimeMaxSec=7260' in body
        assert all(k in body for k in ('MemoryMax=8G','MemorySwapMax=0','CPUQuota=200%','TasksMax=512'))
        assert 'build-essential' in body and 'python3-dev' in body
        clippy_install = 'rustup component add clippy --toolchain 1.98.0'
        if IMPLEMENTATION:
            assert body.index(clippy_install) < body.index('phase=source-qualification'), 'Clippy installed before execution'
        else:
            assert clippy_install not in body, 'unchanged full/test-build bootstrap'
        assert semantic.AWSCLI_URL in body and semantic.AWSCLI_SHA256 in body
        assert 'phase=publication' not in body and 'native-semantic-publication' not in body
        assert 'git ' not in body and 'git\n' not in body
        flag = mode_flag()
        for invocation in (f'{MODULE}{flag} --stage', f'{MODULE}{flag} --check-receipt',
                           f'scripts.check_native_workspace_execution{flag} "$CARGO_HOME/bin/cargo"'):
            assert invocation in body, invocation
        files = _worker_self_check(proof,config_body,manifest_body)
        from scripts.launch_native_semantic_metadata_cold_spot import _full_receipt
        full_check = lambda: _full_receipt(files['workspace-receipt.json'],files['test.log'],inventory,proof['source_identity_sha256'])
        if TEST_BUILD or IMPLEMENTATION:
            rejected(full_check)
        else:
            full_check()
        _collection_self_check(proof,files,body)
    _lifecycle_self_check(proof if MINIMAL_ARCHIVE else None)
    if semantic_1m and not MINIMAL_ARCHIVE:
        _remote_self_check(manifest)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
    print(f'PASS workspace {"implementation-gates" if IMPLEMENTATION else "test-build" if TEST_BUILD else "execution"} ({"co-selection" if CO_SELECTION else "pq-residual" if PQ_RESIDUAL else "fine-sq8" if FINE_SQ8 else "cell-overlap" if CELL_OVERLAP else "constrained-split" if CONSTRAINED_SPLIT else "hierarchical-cells" if HIERARCHICAL_CELLS else "fixed48" if FIXED48 else "bounded-publication" if BOUNDED_PUBLICATION else "root-reuse" if ROOT_REUSE else "startup-wave8" if STARTUP_WAVE8 else "semantic-1m" if semantic_1m else "metadata-waves"}): command once; exit17/timeout/drift/OOM/peak/orphan/persistence/tamper rejected; max reclaim admitted; full/compile/implementation authority checked; all-ACK/fsync/wait-before-collection; remote_cli={semantic_1m and not MINIMAL_ARCHIVE}; code={len(CODE)} artifacts={len(ARTIFACTS)} release_copy={IMPLEMENTATION}; userdata={len(body.encode())} peak_bytes={peak}; AWS/Cargo/cgroup MOCKED')


if __name__ == '__main__':
    args = sys.argv[1:]
    cohere_sq8_builder = args[:1] == ['--cohere-sq8-builder']
    exact_sq8_runtime = args[:1] == ['--exact-sq8-runtime']
    retained_generation_rebind = args[:1] == ['--retained-generation-rebind-implementation']
    baseline_pool = args[:1] == ['--baseline-pool-implementation']
    cohere_cohort = args[:1] == ['--cohere-cohort-implementation']
    cohere1024 = args[:1] == ['--cohere1024-implementation']
    budget_object_fitter = args[:1] == ['--budget-object-fitter-implementation']
    budget_object_selector = args[:1] == ['--budget-object-selector-implementation']
    co_selection = args[:1] == ['--co-selection-implementation']
    pq_residual = args[:1] == ['--pq-residual-implementation']
    fine_sq8 = args[:1] == ['--fine-sq8-implementation']
    cell_overlap = args[:1] == ['--cell-overlap-implementation']
    constrained_split = args[:1] == ['--constrained-split-implementation']
    hierarchical_cells = args[:1] == ['--hierarchical-cells-implementation']
    fixed48 = args[:1] == ['--fixed48-implementation']
    bounded_publication = args[:1] == ['--bounded-publication-implementation']
    root_reuse = args[:1] == ['--root-reuse-implementation']
    startup_wave8 = args[:1] == ['--startup-wave8-implementation']
    implementation = baseline_pool or cohere_sq8_builder or exact_sq8_runtime or retained_generation_rebind or cohere_cohort or cohere1024 or budget_object_fitter or budget_object_selector or co_selection or pq_residual or fine_sq8 or cell_overlap or constrained_split or hierarchical_cells or fixed48 or bounded_publication or root_reuse or startup_wave8 or args[:1] == ['--semantic-1m-implementation']
    test_build = args[:1] == ['--semantic-1m-test-build']
    semantic_1m = implementation or test_build or args[:1] == ['--semantic-1m']
    if semantic_1m:
        args = args[1:]
    configure(semantic_1m, test_build=test_build, implementation=implementation, startup_wave8=startup_wave8, root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection, budget_object_selector=budget_object_selector, budget_object_fitter=budget_object_fitter, cohere1024=cohere1024, cohere_cohort=cohere_cohort, cohere_sq8_builder=cohere_sq8_builder, exact_sq8_runtime=exact_sq8_runtime, retained_generation_rebind=retained_generation_rebind, baseline_pool=baseline_pool)
    if args[:1] and args[0].startswith('--'):
        resource.setrlimit(resource.RLIMIT_AS, (200*1024**2,200*1024**2))
    if args == ['--self-check'] and baseline_pool:
        _baseline_pool_self_check()
    elif args == ['--self-check'] and cohere_sq8_builder:
        _cohere_sq8_builder_self_check()
    elif args == ['--self-check'] and retained_generation_rebind:
        _budget_object_selector_self_check(cohere_cohort=True, retained_generation_rebind=True)
    elif args == ['--self-check'] and exact_sq8_runtime:
        _budget_object_selector_self_check(cohere_cohort=True, exact_sq8_runtime=True)
    elif args == ['--self-check'] and cohere_cohort:
        _budget_object_selector_self_check(cohere_cohort=True)
    elif args == ['--self-check'] and cohere1024:
        _budget_object_selector_self_check(cohere1024=True)
    elif args == ['--self-check'] and budget_object_fitter:
        _budget_object_fitter_self_check()
    elif args == ['--self-check'] and budget_object_selector:
        _budget_object_selector_self_check()
    elif args == ['--self-check'] and co_selection:
        _co_selection_self_check()
    elif args == ['--self-check'] and pq_residual:
        _pq_residual_self_check()
    elif args == ['--self-check'] and fine_sq8:
        _fine_sq8_self_check()
    elif args == ['--self-check'] and cell_overlap:
        _cell_overlap_self_check()
    elif args == ['--self-check'] and constrained_split:
        _constrained_split_self_check()
    elif args == ['--self-check']:
        with execution_mode():
            _test_build_protocol_self_check()
            _startup_wave8_protocol_self_check()
            _startup_wave8_preflight_self_check()
            _root_reuse_protocol_self_check()
            _startup_wave8_preflight_self_check(root_reuse=True)
            _bounded_publication_protocol_self_check()
            _startup_wave8_preflight_self_check(bounded_publication=True)
            _fixed48_protocol_self_check()
            _fixed48_stages_self_check()
            _startup_wave8_preflight_self_check(fixed48=True)
            _hierarchical_archive_self_check()
            _hierarchical_archive_metadata_self_check()
            _hierarchical_cells_protocol_self_check()
            _fixed48_stages_self_check(hierarchical_cells=True)
            _hierarchical_cells_script_self_check()
            _startup_wave8_preflight_self_check(hierarchical_cells=True)
            self_check(hierarchical_cells=True)
        self_check(semantic_1m, test_build=test_build, implementation=implementation, startup_wave8=startup_wave8, root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells, constrained_split=constrained_split, cell_overlap=cell_overlap, fine_sq8=fine_sq8, pq_residual=pq_residual, co_selection=co_selection)
    elif args[:1] == ['--stage']:
        assert len(args) == 3
        stage(*args[1:])
    elif args[:1] == ['--check-receipt']:
        assert len(args) == 2
        out = Path(args[1])
        validate_receipt(out,json.loads((out/'source-qualification.json').read_bytes()))
    elif args[:1] == ['--replay']:
        assert len(args) == 2
        print(json.dumps(replay(args[1]),sort_keys=True))
    else:
        assert len(args) == 1, 'usage: launch_native_workspace_execution_spot.py [--baseline-pool-implementation | --cohere-sq8-builder | --retained-generation-rebind-implementation | --semantic-1m | --semantic-1m-test-build | --semantic-1m-implementation | --startup-wave8-implementation | --root-reuse-implementation | --bounded-publication-implementation | --fixed48-implementation | --hierarchical-cells-implementation | --constrained-split-implementation | --cell-overlap-implementation | --fine-sq8-implementation | --pq-residual-implementation | --co-selection-implementation | --budget-object-selector-implementation | --budget-object-fitter-implementation | --cohere1024-implementation | --cohere-cohort-implementation] aNNNN | --self-check | --stage REPO OUT | --check-receipt OUT | --replay OUT'
        with open('/tmp/borsuk-native-workspace-execution-launch.lock','a+') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            main(args[0])
