#!/usr/bin/env bash
# Exact-source hierarchical-cell research prototype gates; workspace tests are compiled, not all executed.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -n "${BORSUK_TEST_BUILD_COMMAND:-}" ]]; then
  echo 'test-only build shim forbidden' >&2
  exit 2
fi
export CARGO_BUILD_JOBS=1 BORSUK_TEST_BUILD_JOBS=1 RUST_TEST_THREADS=1

stage_record() {
  python3 -c 'import json, re, sys
required = {
    "hierarchical-cell-tests": tuple("hierarchical_semantic_cells::tests::" + name for name in (
        "semantic_cells_do_not_close_over_old_pages_and_keep_unchanged_ranking",
        "identical_geometry_is_bounded_and_reproducible_without_truth",
        "source_id_binding_budgets_and_corruption_fail_closed_with_charges",
        "loss_receipt_separates_boundary_recovery_nomination_and_final_ranking",
        "cell_local_block_nomination_omits_other_blocks_even_for_tied_codes",
        "resident_directory_preload_is_admitted_charged_and_has_no_query_reads",
        "resident_preload_rejects_corrupt_unused_interior_page",
        "whole_cell_matches_two_stage_with_one_wave_and_full_payload_charges",
        "nonunit_query_nonzero_mean_preserves_native_sq2_scoring_and_nomination",
        "nomination_global_finds_leaf_hidden_by_hierarchy_pruning",
        "nomination_unpruned_parity_caps_source_binding_and_failure_charges",
        "capacity_partition_matches_exhaustive_cost_capacity_and_id_ties",
        "balanced_and_identical_builder_memberships_are_unchanged",
        "identical_sample_fallback_preserves_projected_child_sum_order",
        "skewed_builder_is_lossless_deterministic_and_preserves_sq2_sq8_tail_ranking",
        "partition_format_and_receipt_reject_obsolete_or_incomplete_artifacts")),
    "hierarchical-cell-bin-tests": ("tests::configurations_reject_unknown_fields_and_truth_in_requests",
        "tests::created_outputs_close_invalid_on_bad_truth_or_output_cap_without_overwrite",
        "tests::nomination_cli_full64_freezes_prefix_and_both_policies_without_truth",
        "tests::nomination_cli_rejects_truth_fields_incomplete_panels_and_tampering",
        "tests::nomination_prefix_rejects_fifo_and_symlink_without_blocking",
        "tests::nomination_freeze_reserve_and_synced_prefix_tamper_close_invalid"),}
stage, started, finished, status, log, log_status = sys.argv[1:7]
tests = None
passes = None
failed = 0
if finished:
    passes = dict.fromkeys(required.get(stage, ()), 0)
    if stage not in ("release", "clippy", "test-build"):
        tests = 0
        with open(log) as source:
            for line in source:
                test = re.fullmatch(r"test (\S+) \.\.\. ok\n?", line)
                if test and test[1] in passes:
                    passes[test[1]] += 1
                match = re.fullmatch(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; \d+ ignored; \d+ measured; \d+ filtered out;.*\n?", line)
                if match:
                    tests += int(match[1]) + int(match[2])
                    failed += int(match[2])
gate = (int(status) or int(log_status) or (96 if tests == 0 or failed or any(count != 1 for count in passes.values()) else 0)) if finished else None
print(json.dumps(dict(schema="borsuk-hierarchical-cells-implementation-stage-v1", stage=stage,
    started_at=started, finished_at=finished or None,
    exit_status=int(status) if finished else None, gate_status=gate,
    tests_run=tests, required_test_passes=passes, command=sys.argv[7:]), sort_keys=True), flush=True)
sys.exit(gate or 0)' "$@"
}

stage_log="$(mktemp)"
trap 'rm -f "$stage_log"' EXIT
run_stage() {
  local stage="$1" started finished status
  local -a statuses
  shift
  started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stage_record "$stage" "$started" '' '' '' '' "$@"
  set +e
  "$@" 2>&1 | tee "$stage_log"
  statuses=("${PIPESTATUS[@]}")
  finished="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  stage_record "$stage" "$started" "$finished" "${statuses[0]}" "$stage_log" "${statuses[1]}" "$@"
  status=$?
  set -e
  return "$status"
}

run_stage hierarchical-cell-tests cargo test --locked -p borsuk --lib hierarchical_semantic_cells::
run_stage hierarchical-cell-bin-tests cargo test --locked -p borsuk --bin hierarchical_semantic_cells
run_stage generation-integration cargo test --locked -p borsuk --test two_bit_generation
run_stage release cargo build --release --locked -p borsuk --bin hierarchical_semantic_cells --example two_bit_http --bin build_two_bit_generation --bin check_semantic_router_scorer
run_stage clippy cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious
run_stage test-build env -u BORSUK_TEST_BUILD_COMMAND bash scripts/check_rust_test_build.sh
