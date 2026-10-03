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
--hierarchical-cells-implementation qualifies a root-frozen research prototype.
CLI aNNNN | --self-check | --stage REPO OUT | --check-receipt OUT | --replay OUT.
"""
import contextlib
import copy
import fcntl
import gzip
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
HIERARCHICAL_CELLS_DELTA = ('crates/borsuk/src/bin/hierarchical_semantic_cells.rs',
                          'crates/borsuk/src/hierarchical_semantic_cells.rs',
                          'crates/borsuk/src/lib.rs')
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
        'nonunit_query_nonzero_mean_preserves_native_sq2_scoring_and_nomination')),
    'hierarchical-cell-bin-tests': ('tests::configurations_reject_unknown_fields_and_truth_in_requests',)}
HIERARCHICAL_CELLS_STAGES = tuple((name, command.split()) for name, command in (
    ('hierarchical-cell-tests', 'cargo test --locked -p borsuk --lib hierarchical_semantic_cells::'),
    ('hierarchical-cell-bin-tests', 'cargo test --locked -p borsuk --bin hierarchical_semantic_cells'),
    ('generation-integration', 'cargo test --locked -p borsuk --test two_bit_generation'),
    ('release', 'cargo build --release --locked -p borsuk --bin hierarchical_semantic_cells --example two_bit_http --bin build_two_bit_generation --bin check_semantic_router_scorer'),
    ('clippy', 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious'),
    ('test-build', 'env -u BORSUK_TEST_BUILD_COMMAND bash scripts/check_rust_test_build.sh')))
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
FIXED = dict(schema=CONFIG_SCHEMA, architecture='x86_64', region=peer.REGION, bucket=peer.BUCKET,
    instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, root_device_name=ROOT_DEVICE_NAME,
    subnet_id=SUBNET, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR, compute_cap_usd=COMPUTE_CAP,
    ebs_s3_allowance_usd=.15, memory_bytes=worker.MEMORY, swap_bytes=0, cpu_quota_percent=200,
    tasks_max=512, test_limit_seconds=worker.TEST_SECONDS, service_limit_seconds=worker.SERVICE_SECONDS,
    machine_limit_seconds=WALL, command=list(worker.COMMAND), environment=worker.ENVIRONMENT)
FULL_CODE, FULL_FIXED = CODE, FIXED


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def configure(semantic_1m=False, *, test_build=False, implementation=False, startup_wave8=False, root_reuse=False, bounded_publication=False, fixed48=False, hierarchical_cells=False):
    """Select the protocol explicitly in every controller/worker process."""
    global SEMANTIC_1M, TEST_BUILD, IMPLEMENTATION, STARTUP_WAVE8, ROOT_REUSE, BOUNDED_PUBLICATION, FIXED48, HIERARCHICAL_CELLS, NATIVE_DELTA, ROOT, CONFIG, PREFIX, TOKEN_PREFIX, TAG
    global SCHEMA, CONFIG_SCHEMA, RECEIPT_SCHEMA, CODE, FIXED, ARTIFACTS, RELEASE_ARTIFACTS, TERMINAL_IDENTITIES
    assert type(semantic_1m) is type(test_build) is type(implementation) is type(startup_wave8) is type(root_reuse) is type(bounded_publication) is type(fixed48) is type(hierarchical_cells) is bool
    scoped = startup_wave8 or root_reuse or bounded_publication or fixed48 or hierarchical_cells
    assert sum((startup_wave8, root_reuse, bounded_publication, fixed48, hierarchical_cells)) <= 1 and not (scoped and test_build), 'mutually exclusive execution modes'
    if scoped:
        semantic_1m = implementation = True
    assert not (test_build and implementation), 'mutually exclusive execution modes'
    assert not (test_build or implementation) or semantic_1m, 'script requires explicit semantic-1m mode'
    SEMANTIC_1M = semantic_1m
    TEST_BUILD = test_build
    IMPLEMENTATION = implementation
    STARTUP_WAVE8 = startup_wave8
    ROOT_REUSE = root_reuse
    BOUNDED_PUBLICATION = bounded_publication
    FIXED48 = fixed48
    HIERARCHICAL_CELLS = hierarchical_cells
    NATIVE_DELTA = HIERARCHICAL_CELLS_DELTA if hierarchical_cells else FIXED48_DELTA if fixed48 else BOUNDED_PUBLICATION_DELTA if bounded_publication else ROOT_REUSE_DELTA if root_reuse else STARTUP_WAVE8_DELTA
    RELEASE_ARTIFACTS = ('binaries/hierarchical_semantic_cells', 'binaries/two_bit_http', 'binaries/build_two_bit_generation', 'binaries/check_semantic_router_scorer') if hierarchical_cells else ('binaries/two_bit_http', 'binaries/check_semantic_router_scorer', 'binaries/two_bit_plan_demo') if fixed48 else ('binaries/two_bit_http',) if scoped else FULL_RELEASE_ARTIFACTS
    TERMINAL_IDENTITIES = (*FULL_TERMINAL_IDENTITIES, 'controller_source_commit', 'candidate_delta_paths') if scoped else FULL_TERMINAL_IDENTITIES
    ARTIFACTS = (*FULL_ARTIFACTS, *RELEASE_ARTIFACTS) if implementation else FULL_ARTIFACTS
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
        ROOT = semantic.ROOT.parent/'semantic-1m/hierarchical-cells/implementation-gates'
        CONFIG = ROOT/'config.json'
        TOKEN_PREFIX = 'hierarchical-cells-implementation-'
        PREFIX = 'research/semantic-router/20261003/' + TOKEN_PREFIX
        TAG = 'borsuk-hierarchical-cells-implementation'
        SCHEMA = 'borsuk-hierarchical-cells-implementation-gates-spot-v1'
        CONFIG_SCHEMA = 'borsuk-hierarchical-cells-implementation-gates-v1'
        RECEIPT_SCHEMA = 'borsuk-hierarchical-cells-implementation-gates-receipt-v1'
        CODE = (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_hierarchical_cells_implementation.sh')
        FIXED.update(schema=CONFIG_SCHEMA, command=['bash', 'scripts/check_hierarchical_cells_implementation.sh'])


@contextlib.contextmanager
def execution_mode(semantic_1m=False, *, test_build=False, implementation=False, startup_wave8=False, root_reuse=False, bounded_publication=False, fixed48=False, hierarchical_cells=False):
    """Restore the caller's protocol after a worker or synthetic check."""
    previous = SEMANTIC_1M, TEST_BUILD, IMPLEMENTATION, STARTUP_WAVE8, ROOT_REUSE, BOUNDED_PUBLICATION, FIXED48, HIERARCHICAL_CELLS
    configure(semantic_1m, test_build=test_build, implementation=implementation, startup_wave8=startup_wave8, root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells)
    try:
        yield
    finally:
        configure(previous[0], test_build=previous[1], implementation=previous[2], startup_wave8=previous[3], root_reuse=previous[4], bounded_publication=previous[5], fixed48=previous[6], hierarchical_cells=previous[7])


def mode_flag():
    return ' --hierarchical-cells-implementation' if HIERARCHICAL_CELLS else ' --fixed48-implementation' if FIXED48 else ' --bounded-publication-implementation' if BOUNDED_PUBLICATION else ' --root-reuse-implementation' if ROOT_REUSE else ' --startup-wave8-implementation' if STARTUP_WAVE8 else ' --semantic-1m-implementation' if IMPLEMENTATION else ' --semantic-1m-test-build' if TEST_BUILD else ' --semantic-1m' if SEMANTIC_1M else ''


def qualify(base=Path('.')):
    """Portable source/config/code qualification, without Git or completed assurance."""
    base = Path(base).resolve()
    body = (base/CONFIG).read_bytes()
    config = json.loads(body)
    assert config['controller_authority_pending'] is False, 'root authority freeze pending'
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
    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
        assert re.fullmatch('[0-9a-f]{40}', config['controller_source_commit']), 'frozen controller commit'
        assert manifest['schema'] == ('borsuk-hierarchical-cells-native-source-manifest-v1' if HIERARCHICAL_CELLS else 'borsuk-fixed48-native-source-manifest-v1' if FIXED48 else 'borsuk-bounded-publication-native-source-manifest-v1' if BOUNDED_PUBLICATION else 'borsuk-root-reuse-native-source-manifest-v1' if ROOT_REUSE else 'borsuk-startup-wave8-native-source-manifest-v1')
        assert manifest['candidate_qualification_pending'] is True, 'source authority is not completed assurance'
        assert type(manifest['candidate_delta_paths']) is list and manifest['candidate_delta_paths'] == list(NATIVE_DELTA), 'exact candidate native delta'
        if STARTUP_WAVE8:
            assert manifest['native_source_commit'] == STARTUP_WAVE8_COMMIT, 'fixed startup candidate commit'
            assert manifest['source_identity_sha256'] == STARTUP_WAVE8_IDENTITY, 'fixed startup candidate identity'
        assert re.fullmatch('[0-9a-f]{40}', manifest['control_native_source_commit'])
    inventory = worker.source_hashes(base)
    assert type(manifest['source_file_count']) is int and manifest['source_file_count'] == len(inventory) > 0
    assert manifest['source_sha256'] == inventory, 'full native source drift'
    identity = worker.source_identity(inventory)
    assert identity == manifest['source_identity_sha256']
    assert SEMANTIC_1M or identity == SOURCE_IDENTITY, 'historical native source identity'
    assert re.fullmatch('[0-9a-f]{40}', manifest['native_source_commit'])
    proof = dict(schema='borsuk-hierarchical-cells-implementation-gates-qualification-v1' if HIERARCHICAL_CELLS else 'borsuk-fixed48-implementation-gates-qualification-v1' if FIXED48 else 'borsuk-bounded-publication-implementation-gates-qualification-v1' if BOUNDED_PUBLICATION else 'borsuk-root-reuse-implementation-gates-qualification-v1' if ROOT_REUSE else 'borsuk-startup-wave8-implementation-gates-qualification-v1' if STARTUP_WAVE8 else 'borsuk-semantic-1m-implementation-gates-qualification-v1' if IMPLEMENTATION else 'borsuk-native-workspace-test-build-qualification-v1' if TEST_BUILD else 'borsuk-native-workspace-execution-qualification-v1',
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
    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
        proof.update(controller_source_commit=config['controller_source_commit'],
                     candidate_delta_paths=list(NATIVE_DELTA))
    return proof


def preflight(base=Path('.')):
    assert not subprocess.check_output(['git','status','--porcelain'], cwd=base, text=True).strip(), 'dirty source'
    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
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
            cwd=base, text=True).splitlines() == list(NATIVE_DELTA), 'exact candidate source bundle'
        for name in NATIVE_DELTA:
            assert worker.sha(subprocess.check_output(['git','show',proof['native_source_commit']+':'+name], cwd=base)) == proof['source_sha256'][name], 'candidate commit blob: '+name
        return proof
    assert subprocess.check_output(['git','for-each-ref','--contains=HEAD','--format=%(refname)',
        'refs/remotes/origin/'], cwd=base, text=True).strip(), 'source commit not on an origin ref'
    return qualify(base)


def stage(repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    proof = qualify(repo)
    for name, body in (('source-qualification.json', encoded(proof)+b'\n'),
                       ('config.json', (repo/CONFIG).read_bytes()),
                       ('native-source-manifest.json', (repo/proof['native_source_manifest']['path']).read_bytes())):
        with (out/name).open('xb') as output:
            output.write(body); output.flush(); os.fsync(output.fileno())
    return proof


def validate_bounded_publication_stages(log, *, fixed48=False, hierarchical_cells=False):
    from datetime import datetime
    assert not (fixed48 and hierarchical_cells), 'mutually exclusive stage protocols'
    stages = HIERARCHICAL_CELLS_STAGES if hierarchical_cells else FIXED48_STAGES if fixed48 else BOUNDED_PUBLICATION_STAGES
    schema = HIERARCHICAL_CELLS_STAGE_SCHEMA if hierarchical_cells else FIXED48_STAGE_SCHEMA if fixed48 else BOUNDED_PUBLICATION_STAGE_SCHEMA
    required = HIERARCHICAL_CELLS_REQUIRED_TESTS if hierarchical_cells else FIXED48_REQUIRED_TESTS
    named_protocol = fixed48 or hierarchical_cells
    test_stage_count = len(stages) - 3
    named_stages = {test: index for index, (name, _) in enumerate(stages)
        for test in required.get(name, ())} if named_protocol else {BOUNDED_PUBLICATION_CAP_TEST: 1}
    passes = dict.fromkeys(named_stages, 0)
    test_counts = [0]*test_stage_count
    test_builds = 0
    records = []
    with Path(log).open() as source:
        for line in source:
            match = re.fullmatch(r'test (\S+) \.\.\. ok\n?', line)
            if match and match[1] in named_stages:
                test = match[1]
                assert len(records) == 2*named_stages[test]+1, 'named test in wrong stage: '+test
                passes[test] += 1
            if named_protocol:
                summary = re.fullmatch(r'test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; \d+ ignored; \d+ measured; \d+ filtered out;.*\n?', line)
                if summary:
                    assert len(records) in range(1, 2*test_stage_count, 2), 'tests outside execution stages'
                    assert int(summary[2]) == 0, 'failed tests'
                    test_counts[len(records)//2] += int(summary[1])
            if hierarchical_cells and re.fullmatch(r'rust-test-build status=0 elapsed_seconds=\d+ jobs=1\n?', line):
                assert len(records) == 2*len(stages)-1, 'test-build proof in wrong stage'
                test_builds += 1
            if not line.startswith('{'):
                continue
            try:
                record = json.loads(line)
            except ValueError:
                continue
            if isinstance(record, dict) and record.get('schema') == schema:
                records.append(record)
                assert len(records) <= 2*len(stages), 'extra gate stage records'
    assert len(records) == 2*len(stages), 'all completed stages required'
    assert all(count == 1 for count in passes.values()), 'named tests must pass exactly once'
    assert not hierarchical_cells or test_builds == 1, 'actual unshimmed test-build completion'
    previous_finish = None
    for index, (name, command) in enumerate(stages):
        start, end = records[2*index:2*index+2]
        assert start['stage'] == end['stage'] == name and start['command'] == end['command'] == command
        test_field = 'required_test_passes' if named_protocol else 'publication_cap_test_passed'
        assert start['finished_at'] is start['exit_status'] is start['gate_status'] is start['tests_run'] is start[test_field] is None
        assert type(end['exit_status']) is type(end['gate_status']) is int and end['exit_status'] == end['gate_status'] == 0
        assert start['started_at'] == end['started_at']
        assert datetime.fromisoformat(end['finished_at']) >= datetime.fromisoformat(start['started_at'])
        assert (type(end['tests_run']) is int and end['tests_run'] > 0) if index < test_stage_count else end['tests_run'] is None
        if named_protocol:
            assert index >= test_stage_count or end['tests_run'] == test_counts[index], 'actual test count'
            expected = {test: passes[test] for test in required.get(name, ())}
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
    receipt = json.loads((out/'workspace-receipt.json').read_bytes())
    assert receipt['schema'] == RECEIPT_SCHEMA
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
    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
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
    assert worker.artifact(out/'config.json')['sha256'] == proof['config_sha256']
    assert worker.artifact(out/'native-source-manifest.json')['sha256'] == proof['native_source_manifest_sha256']
    for name in ('source-before.json','source-after.json'):
        assert json.loads((out/name).read_bytes()) == proof['source_sha256']
    if BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
        assert receipt['stages'] == validate_bounded_publication_stages(out/'test.log', fixed48=FIXED48, hierarchical_cells=HIERARCHICAL_CELLS), 'actual stage receipt'
    worker.validate_cgroup(json.loads((out/'workspace-cgroup.json').read_bytes()))
    return receipt


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha)
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix)
    assert qualification['campaign_schema'] == SCHEMA and qualification['config_path'] == str(CONFIG)
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
    start, end = body.index('phase=install\n'), body.index('phase=complete\n')
    body = body[:start]+command+body[end:]
    if IMPLEMENTATION:
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
    assert closed['state'] == 'terminated'
    assert terminal['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert terminal['schema'] == reservation['schema'] == SCHEMA
    for key in ('source_commit','source_archive_sha256'):
        assert terminal[key] == reservation[key], 'campaign source binding'
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], 'terminal ' + key
    assert set(proof['code_sha256']) == set(CODE)
    assert proof['code_identity_sha256'] == worker.sha(encoded(proof['code_sha256']))
    base = Path(__file__).resolve().parents[1]
    assert all(worker.artifact(base/name)['sha256'] == digest for name,digest in proof['code_sha256'].items())
    assert proof['artifact_roster_sha256'] == worker.sha(encoded(ARTIFACTS))
    assert type(proof['source_file_count']) is int and len(proof['source_sha256']) == proof['source_file_count'] > 0
    assert worker.source_identity(proof['source_sha256']) == proof['source_identity_sha256']
    assert SEMANTIC_1M or proof['source_identity_sha256'] == SOURCE_IDENTITY
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected artifact'
    for name, identity in terminal['artifacts'].items():
        assert worker.artifact(out/name) == identity, 'terminal artifact: ' + name
    for name,key in (('config.json','config_sha256'),('native-source-manifest.json','native_source_manifest_sha256')):
        if name in terminal['artifacts']:
            assert worker.artifact(out/name)['sha256'] == proof[key], 'saved authority: ' + name
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


def _worker_self_check(proof, config_body, manifest_body):
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
    if BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
        failures += ('missing-stage', 'zero-stage-record', 'stage-order', 'stage-bool', 'missing-cap-proof')
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
                if BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
                    stages = list(HIERARCHICAL_CELLS_STAGES if HIERARCHICAL_CELLS else FIXED48_STAGES if FIXED48 else BOUNDED_PUBLICATION_STAGES)
                    if failure == 'stage-order':
                        stages.reverse()
                    for index, (name, command) in enumerate(stages):
                        if failure == 'missing-stage' and index == len(stages)-1:
                            continue
                        record = dict(schema=HIERARCHICAL_CELLS_STAGE_SCHEMA if HIERARCHICAL_CELLS else FIXED48_STAGE_SCHEMA if FIXED48 else BOUNDED_PUBLICATION_STAGE_SCHEMA, stage=name,
                            command=command, started_at='2026-10-02T00:00:00Z',
                            finished_at=None, exit_status=None, gate_status=None, tests_run=None)
                        field = 'required_test_passes' if FIXED48 or HIERARCHICAL_CELLS else 'publication_cap_test_passed'
                        record[field] = None
                        kw['stdout'].write(encoded(record)+b'\n')
                        named = (HIERARCHICAL_CELLS_REQUIRED_TESTS if HIERARCHICAL_CELLS else FIXED48_REQUIRED_TESTS).get(name, ()) if FIXED48 or HIERARCHICAL_CELLS else ()
                        if FIXED48 or HIERARCHICAL_CELLS:
                            if failure != 'missing-cap-proof':
                                for test in named:
                                    kw['stdout'].write(('test '+test+' ... ok\n').encode())
                            if index < len(stages)-3:
                                kw['stdout'].write(f'test result: ok. {max(1, len(named))} passed; 0 failed; 0 ignored; 0 measured; 100 filtered out; finished in 0.00s\n'.encode())
                        elif index == 1 and failure != 'missing-cap-proof':
                            kw['stdout'].write(('test '+BOUNDED_PUBLICATION_CAP_TEST+' ... ok\n').encode())
                        if HIERARCHICAL_CELLS and name == 'test-build':
                            kw['stdout'].write(b'rust-test-build status=0 elapsed_seconds=0 jobs=1\n')
                        record.update(finished_at=record['started_at'], exit_status=0, gate_status=0,
                            tests_run=(0 if failure == 'zero-stage-record' else False if failure == 'stage-bool' else max(1, len(named))) if index < len(stages)-3 else None)
                        record[field] = {test: int(failure != 'missing-cap-proof') for test in named} if FIXED48 or HIERARCHICAL_CELLS else (failure != 'missing-cap-proof') if index == 1 else None
                        kw['stdout'].write(encoded(record)+b'\n')
                Path(args[args.index('-o')+1]).write_text('GNU time mocked resources\n')
                if IMPLEMENTATION:
                    for name in RELEASE_ARTIFACTS:
                        source = Path(kw['env']['CARGO_TARGET_DIR'])/'release'/('examples' if name.endswith('/two_bit_http') else '')/Path(name).name
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
                previous = authority.SEMANTIC_1M, authority.TEST_BUILD, authority.IMPLEMENTATION, authority.STARTUP_WAVE8, authority.ROOT_REUSE, authority.BOUNDED_PUBLICATION, authority.FIXED48, authority.HIERARCHICAL_CELLS, authority.CONFIG, authority.CODE, authority.FIXED
                result = worker.main('cargo' if TEST_BUILD or IMPLEMENTATION else 'fake-cargo', repo, out,
                                     semantic_1m=SEMANTIC_1M, test_build=TEST_BUILD, implementation=IMPLEMENTATION, startup_wave8=STARTUP_WAVE8, root_reuse=ROOT_REUSE, bounded_publication=BOUNDED_PUBLICATION, fixed48=FIXED48, hierarchical_cells=HIERARCHICAL_CELLS)
                assert previous == (authority.SEMANTIC_1M, authority.TEST_BUILD, authority.IMPLEMENTATION, authority.STARTUP_WAVE8, authority.ROOT_REUSE, authority.BOUNDED_PUBLICATION, authority.FIXED48, authority.HIERARCHICAL_CELLS, authority.CONFIG, authority.CODE, authority.FIXED)
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
                            source = out/'target/release'/('examples' if name.endswith('/two_bit_http') else '')/Path(name).name
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
                    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
                        for changed in (dict(controller_source_commit='0'*40), dict(candidate_delta_paths=[])):
                            (out/'workspace-receipt.json').write_bytes(encoded(dict(result, **changed)))
                            rejected(lambda:validate_receipt(out, proof))
                        (out/'workspace-receipt.json').write_bytes(encoded(result))
                    if BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
                        assert len(result['stages']) == (6 if HIERARCHICAL_CELLS else 7) and all(record['tests_run'] > 0 for record in result['stages'][:-3])
                        for stages in ([], result['stages'][:-1], list(reversed(result['stages']))):
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


def _lifecycle_self_check():
    from datetime import datetime, timezone
    from unittest.mock import Mock
    module = sys.modules[__name__]
    for failure in ('success','multi-ack','multi-ack-fsync','upload','interrupt'):
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
            ec2.get_waiter.return_value.wait.side_effect = lambda **kw:events.append('wait')
            def collected(*args):
                assert events == ['terminate','wait'], 'collection before termination waiter'
                events.append('collect')
                return dict(status='complete',phase='complete',exit_code=0,artifacts={n:{} for n in ARTIFACTS})
            with patch.object(module,'ROOT',Path(tmp)), patch.object(module,'preflight',return_value={'config_sha256':'a'*64}), \
                    patch.object(module,'user_data',return_value='mock'), patch.object(module,'collect',side_effect=collected), \
                    patch.object(module,'poll',side_effect=KeyboardInterrupt() if failure == 'interrupt' else None), \
                    patch.object(shared.boto3,'Session',return_value=session), \
                    patch.object(subprocess,'check_output',side_effect=['','0'*40,b'archive']), \
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
            ec2.run_instances.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            assert events == ['terminate','wait','collect']
            assert ec2.run_instances.call_args.kwargs['InstanceType'] == INSTANCE_TYPE
            assert ec2.run_instances.call_args.kwargs['ImageId'] == IMAGE_ID
            assert ec2.run_instances.call_args.kwargs['BlockDeviceMappings'][0]['DeviceName'] == ROOT_DEVICE_NAME
            for name in ('aws-launch.json','aws-closeout.json'):
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
        collect(s3, PREFIX+'a0001', out, 'i-owned', '0'*40, '1'*64)
        replayed = replay(out)
        assert replayed['qualified'] is True
        assert replayed['actual_full_workspace_execution'] is (not (TEST_BUILD or IMPLEMENTATION))
        if TEST_BUILD:
            assert replayed['actual_workspace_test_build'] is True
        for name,data in files.items():
            assert gzip.decompress((out/(name+'.gz')).read_bytes()) == data
        mutations = [dict(terminal,config_sha256='0'*64), dict(terminal,instance_id='i-other'),
                     dict(terminal,source_archive_sha256='2'*64), dict(terminal,exit_code=False),
                     dict(terminal,artifacts={n:v for n,v in terminal['artifacts'].items() if n != 'test.log'})]
        if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
            mutations.extend((dict(terminal,controller_source_commit='0'*40),dict(terminal,candidate_delta_paths=[])))
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
                        source = out/'target/release'/('examples' if name.endswith('/two_bit_http') else '')/Path(name).name
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


def _hierarchical_cells_protocol_self_check():
    previous = CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag()
    with execution_mode(hierarchical_cells=True):
        assert str(ROOT).endswith('semantic-1m/hierarchical-cells/implementation-gates')
        assert mode_flag() == ' --hierarchical-cells-implementation'
        assert PREFIX == 'research/semantic-router/20261003/hierarchical-cells-implementation-'
        assert CONFIG_SCHEMA == FIXED['schema'] == 'borsuk-hierarchical-cells-implementation-gates-v1'
        assert SCHEMA == 'borsuk-hierarchical-cells-implementation-gates-spot-v1'
        assert RECEIPT_SCHEMA == 'borsuk-hierarchical-cells-implementation-gates-receipt-v1'
        assert FIXED['command'] == ['bash', 'scripts/check_hierarchical_cells_implementation.sh']
        assert CODE == (*FULL_CODE, 'scripts/check_rust_test_build.sh', 'scripts/check_hierarchical_cells_implementation.sh')
        assert NATIVE_DELTA == HIERARCHICAL_CELLS_DELTA and list(NATIVE_DELTA) == sorted(NATIVE_DELTA)
        assert RELEASE_ARTIFACTS == ('binaries/hierarchical_semantic_cells', 'binaries/two_bit_http', 'binaries/build_two_bit_generation', 'binaries/check_semantic_router_scorer')
        assert ARTIFACTS == (*FULL_ARTIFACTS, *RELEASE_ARTIFACTS) and len(ARTIFACTS) == 17
        assert FIXED == dict(FULL_FIXED, schema=CONFIG_SCHEMA, execution_kind='implementation-gates',
            command=FIXED['command'], environment=dict(worker.ENVIRONMENT, BORSUK_TEST_BUILD_JOBS='1', BORSUK_TEST_BUILD_COMMAND=None))
        for mode in ('test_build', 'startup_wave8', 'root_reuse', 'bounded_publication', 'fixed48'):
            rejected(lambda: configure(hierarchical_cells=True, **{mode: True}))
        rejected(lambda: configure(hierarchical_cells=1))
        with execution_mode(fixed48=True):
            assert not HIERARCHICAL_CELLS
        assert HIERARCHICAL_CELLS
    assert previous == (CONFIG, CODE, FIXED, ARTIFACTS, NATIVE_DELTA, mode_flag())


def _hierarchical_cells_script_self_check():
    """Execute the real stage recorder as Python; never invoke Cargo or Bash."""
    from shlex import split
    script = (Path(__file__).resolve().parent/'check_hierarchical_cells_implementation.sh').read_text()
    actual = tuple((args[1], args[2:]) for line in script.splitlines()
        if line.startswith('run_stage ') for args in (split(line),))
    assert actual == HIERARCHICAL_CELLS_STAGES, 'exact shell stage commands'
    recorder = script.split("python3 -c '", 1)[1].split("' \"$@\"", 1)[0]
    compile(recorder, '<stage-recorder>', 'exec')
    with tempfile.TemporaryDirectory() as tmp:
        log = Path(tmp)/'stage.log'
        for index, (name, command) in enumerate(HIERARCHICAL_CELLS_STAGES):
            names = HIERARCHICAL_CELLS_REQUIRED_TESTS.get(name, ())
            lines = ['test '+test+' ... ok' for test in names]
            if index < 3:
                lines.append(f'test result: ok. {max(1, len(names))} passed; 0 failed; 0 ignored; 0 measured; 100 filtered out; finished in 0.00s')
            good = '\n'.join(lines)+'\n'
            cases = [(good, '0', '0', 0), (good, '17', '0', 17), (good, '0', '18', 18)]
            if index < 3:
                cases.append(('', '0', '0', 96))
            for test in names:
                passed = 'test '+test+' ... ok'
                for replacement in ('', passed.replace('ok', 'ignored'), passed+'\n'+passed):
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
                assert record['schema'] == HIERARCHICAL_CELLS_STAGE_SCHEMA


def _fixed48_stages_self_check(*, hierarchical_cells=False):
    import inspect
    assert 'fixed48' in inspect.signature(validate_bounded_publication_stages).parameters, 'fixed48 stage authentication missing'
    stages = HIERARCHICAL_CELLS_STAGES if hierarchical_cells else FIXED48_STAGES
    schema = HIERARCHICAL_CELLS_STAGE_SCHEMA if hierarchical_cells else FIXED48_STAGE_SCHEMA
    required = HIERARCHICAL_CELLS_REQUIRED_TESTS if hierarchical_cells else FIXED48_REQUIRED_TESTS
    with tempfile.TemporaryDirectory() as tmp:
        log = Path(tmp)/'test.log'
        lines = []
        for index, (name, command) in enumerate(stages):
            record = dict(schema=schema, stage=name, command=command,
                started_at='2026-10-02T00:00:00Z', finished_at=None,
                exit_status=None, gate_status=None, tests_run=None, required_test_passes=None)
            lines.append(encoded(record).decode())
            names = required.get(name, ())
            lines.extend('test '+test+' ... ok' for test in names)
            tests = max(1, len(names)) if index < len(stages)-3 else None
            if index < len(stages)-3:
                lines.append(f'test result: ok. {tests} passed; 0 failed; 0 ignored; 0 measured; 100 filtered out; finished in 0.00s')
            if hierarchical_cells and name == 'test-build':
                lines.append('rust-test-build status=0 elapsed_seconds=0 jobs=1')
            record.update(finished_at=record['started_at'], exit_status=0, gate_status=0,
                tests_run=tests, required_test_passes={test: 1 for test in names})
            lines.append(encoded(record).decode())
        log.write_text('\n'.join(lines)+'\n')
        records = validate_bounded_publication_stages(log, fixed48=not hierarchical_cells, hierarchical_cells=hierarchical_cells)
        assert len(records) == len(stages) and [r['tests_run'] for r in records[:-3]] == ([len(HIERARCHICAL_CELLS_REQUIRED_TESTS['hierarchical-cell-tests']), 1, 1] if hierarchical_cells else [2, 2, 2, 1])
        for names in required.values():
            for name in names:
                passed = 'test '+name+' ... ok'
                for replacement in ('', passed.replace('ok', 'ignored'), passed+'\n'+passed):
                    log.write_text('\n'.join(lines).replace(passed, replacement)+'\n')
                    rejected(lambda: validate_bounded_publication_stages(log, fixed48=not hierarchical_cells, hierarchical_cells=hierarchical_cells))
                misplaced = [line for line in lines if line != passed]
                misplaced.insert(0, passed)
                log.write_text('\n'.join(misplaced)+'\n')
                rejected(lambda: validate_bounded_publication_stages(log, fixed48=not hierarchical_cells, hierarchical_cells=hierarchical_cells))
        for index, line in enumerate(lines):
            if not line.startswith('{'):
                continue
            record = json.loads(line)
            changes = [('command', ['cargo', 'test', '--workspace']), ('stage', 'wrong-stage')]
            if record['finished_at']:
                changes += [('exit_status', 17), ('exit_status', False), ('gate_status', 96),
                            ('required_test_passes', {}), ('finished_at', '2026-10-01T00:00:00Z')]
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
                rejected(lambda: validate_bounded_publication_stages(log, fixed48=not hierarchical_cells, hierarchical_cells=hierarchical_cells))
        if hierarchical_cells:
            for replacement in ('', 'rust-test-build status=0 elapsed_seconds=0 jobs=2', 'rust-test-build status=0 elapsed_seconds=0 jobs=1\nrust-test-build status=0 elapsed_seconds=0 jobs=1'):
                log.write_text('\n'.join(lines).replace('rust-test-build status=0 elapsed_seconds=0 jobs=1', replacement)+'\n')
                rejected(lambda: validate_bounded_publication_stages(log, hierarchical_cells=True))
        for changed in (lines[:-1], lines+lines[-2:], list(reversed(lines))):
            log.write_text('\n'.join(changed)+'\n')
            rejected(lambda: validate_bounded_publication_stages(log, fixed48=not hierarchical_cells, hierarchical_cells=hierarchical_cells))


def _startup_wave8_preflight_self_check(*, root_reuse=False, bounded_publication=False, fixed48=False, hierarchical_cells=False):
    controller, config_commit, bundle = '4'*40, '5'*40, '6'*40
    blob = b'mocked exact candidate blob'
    with execution_mode(startup_wave8=not (root_reuse or bounded_publication or fixed48 or hierarchical_cells), root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells):
        proof = dict(controller_source_commit=controller, candidate_delta_paths=list(NATIVE_DELTA),
                     native_source_commit='7'*40 if hierarchical_cells else FIXED48_CHECK_COMMIT if fixed48 else BOUNDED_PUBLICATION_CHECK_COMMIT if bounded_publication else '7'*40 if root_reuse else STARTUP_WAVE8_COMMIT,
                     source_sha256={name:worker.sha(blob) for name in NATIVE_DELTA})
        answers = {
            ('status','--porcelain'): '',
            ('rev-list','--parents','-n','1','HEAD'): bundle+' '+config_commit,
            ('rev-list','--parents','-n','1',config_commit): config_commit+' '+controller,
            ('for-each-ref','--contains='+config_commit,'--format=%(refname)','refs/remotes/origin/'): 'refs/remotes/origin/master',
            ('diff','--no-renames','--name-only',controller,config_commit): str(CONFIG),
            ('diff','--no-renames','--name-only',config_commit,'HEAD'): '\n'.join(NATIVE_DELTA),
            **{('show',proof['native_source_commit']+':'+name):blob for name in NATIVE_DELTA}}
        failures = ('success','dirty','merge-bundle','wrong-controller','unpublished-config','extra-config-delta','missing-native-delta','extra-native-delta','wrong-candidate-blob')
        if fixed48:
            failures += ('historical-two-path-bundle',)
        for failure in failures:
            changed = dict(answers)
            key, value = {
                'success': (('status','--porcelain'), ''),
                'dirty': (('status','--porcelain'), 'dirty'),
                'merge-bundle': (('rev-list','--parents','-n','1','HEAD'), bundle+' '+config_commit+' '+controller),
                'wrong-controller': (('rev-list','--parents','-n','1',config_commit), config_commit+' '+'7'*40),
                'unpublished-config': (('for-each-ref','--contains='+config_commit,'--format=%(refname)','refs/remotes/origin/'), ''),
                'extra-config-delta': (('diff','--no-renames','--name-only',controller,config_commit), str(CONFIG)+'\nother.py'),
                'missing-native-delta': (('diff','--no-renames','--name-only',config_commit,'HEAD'), NATIVE_DELTA[0]),
                'extra-native-delta': (('diff','--no-renames','--name-only',config_commit,'HEAD'), '\n'.join((*NATIVE_DELTA,'other.rs'))),
                'historical-two-path-bundle': (('diff','--no-renames','--name-only',config_commit,'HEAD'), '\n'.join(NATIVE_DELTA[1:])),
                'wrong-candidate-blob': (('show',proof['native_source_commit']+':'+NATIVE_DELTA[0]), b'tampered')
            }[failure]
            changed[key] = value
            def git(args, **kwargs):
                assert args[0] == 'git' and tuple(args[1:]) in changed, args
                return changed[tuple(args[1:])]
            with patch.object(sys.modules[__name__], 'qualify', return_value=proof), \
                    patch.object(subprocess,'check_output',side_effect=git):
                if failure == 'success':
                    assert preflight() == proof
                else:
                    rejected(lambda:preflight())


def self_check(semantic_1m=False, *, test_build=False, implementation=False, startup_wave8=False, root_reuse=False, bounded_publication=False, fixed48=False, hierarchical_cells=False):
    with execution_mode(semantic_1m, test_build=test_build, implementation=implementation, startup_wave8=startup_wave8, root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells):
        _self_check()


def _self_check():
    semantic_1m = SEMANTIC_1M
    module = sys.modules[__name__]
    base = Path(__file__).resolve().parents[1]
    manifest = json.loads((base/semantic.ROOT/'metadata-waves/native-source-manifest.json').read_bytes())
    inventory = manifest['source_sha256']
    if HIERARCHICAL_CELLS:
        # Synthetic authority only: the root chooses the production candidate.
        inventory = worker.source_hashes(base)
        for name in NATIVE_DELTA:
            inventory.setdefault(name, worker.sha(b'// synthetic new native source\n'))
        manifest = dict(schema='borsuk-hierarchical-cells-native-source-manifest-v1', source_sha256=inventory,
            source_identity_sha256=worker.source_identity(inventory), source_file_count=len(inventory),
            native_source_commit='7'*40, candidate_delta_paths=list(NATIVE_DELTA),
            candidate_qualification_pending=True, control_native_source_commit='8'*40)
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
    if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
        config['controller_source_commit'] = '4'*40
    with tempfile.TemporaryDirectory() as tmp:
        repo, out = Path(tmp)/'repo', Path(tmp)/'out'
        repo.mkdir(); out.mkdir()
        for name in CODE:
            (repo/name).parent.mkdir(parents=True,exist_ok=True)
            (repo/name).symlink_to(base/name)
        (repo/manifest_path).parent.mkdir(parents=True,exist_ok=True)
        (repo/manifest_path).write_bytes(manifest_body)
        (repo/CONFIG).parent.mkdir(parents=True,exist_ok=True)
        config_body = encoded(config)
        (repo/CONFIG).write_bytes(config_body)
        with patch.object(worker,'source_hashes',return_value=inventory):
            proof = qualify(repo)
            assert proof['source_identity_sha256'] == manifest['source_identity_sha256']
            assert proof['actual_full_workspace_execution'] is False
            stage(repo,out)
            assert json.loads((out/'source-qualification.json').read_bytes()) == proof
            for key,value in (('controller_authority_pending',True), ('memory_bytes',worker.MEMORY+1),
                              ('command',[*FIXED['command'],'--no-run']), ('environment',dict(FIXED['environment'],CARGO_BUILD_JOBS='2')),
                              ('controller_code_sha256',dict(config['controller_code_sha256'],**{CODE[0]:'0'*64})),
                              ('controller_code_sha256',{name:digest for name,digest in config['controller_code_sha256'].items() if name != CODE[0]}),
                              ('controller_code_sha256',dict(config['controller_code_sha256'],**{'scripts/unowned.py':'0'*64})),
                              ('native_source_manifest',dict(config['native_source_manifest'],sha256='0'*64))):
                (repo/CONFIG).write_bytes(encoded(dict(config,**{key:value})))
                rejected(lambda:qualify(repo))
            if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
                for value in ('not-a-commit', '4'*39):
                    (repo/CONFIG).write_bytes(encoded(dict(config,controller_source_commit=value)))
                    rejected(lambda:qualify(repo))
            bad_manifest = [('source_identity_sha256','0'*64), ('source_file_count',len(inventory)-1), ('source_file_count',True),
                            ('native_source_commit','not-a-commit')]
            if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION or FIXED48 or HIERARCHICAL_CELLS:
                bad_manifest.extend((('candidate_delta_paths',[]),
                    ('candidate_delta_paths',dict.fromkeys(NATIVE_DELTA)),
                    ('candidate_delta_paths',list(reversed(NATIVE_DELTA))),
                    ('candidate_qualification_pending',False),('candidate_qualification_pending',1),
                    ('schema','wrong-manifest-mode'),('control_native_source_commit','not-a-commit')))
                if FIXED48:
                    bad_manifest.append(('candidate_delta_paths',list(FIXED48_DELTA[1:])))
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
        assert len(ARTIFACTS) == len(set(ARTIFACTS)) == (17 if HIERARCHICAL_CELLS else 16 if FIXED48 else 14 if STARTUP_WAVE8 or ROOT_REUSE or BOUNDED_PUBLICATION else 18 if IMPLEMENTATION else 13)
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
    _lifecycle_self_check()
    if semantic_1m and not HIERARCHICAL_CELLS:
        _remote_self_check(manifest)
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
    print(f'PASS workspace {"implementation-gates" if IMPLEMENTATION else "test-build" if TEST_BUILD else "execution"} ({"hierarchical-cells" if HIERARCHICAL_CELLS else "fixed48" if FIXED48 else "bounded-publication" if BOUNDED_PUBLICATION else "root-reuse" if ROOT_REUSE else "startup-wave8" if STARTUP_WAVE8 else "semantic-1m" if semantic_1m else "metadata-waves"}): command once; exit17/timeout/drift/OOM/peak/orphan/persistence/tamper rejected; max reclaim admitted; full/compile/implementation authority checked; all-ACK/fsync/wait-before-collection; remote_cli={semantic_1m and not HIERARCHICAL_CELLS}; code={len(CODE)} artifacts={len(ARTIFACTS)} release_copy={IMPLEMENTATION}; userdata={len(body.encode())} peak_bytes={peak}; AWS/Cargo/cgroup MOCKED')


if __name__ == '__main__':
    args = sys.argv[1:]
    hierarchical_cells = args[:1] == ['--hierarchical-cells-implementation']
    fixed48 = args[:1] == ['--fixed48-implementation']
    bounded_publication = args[:1] == ['--bounded-publication-implementation']
    root_reuse = args[:1] == ['--root-reuse-implementation']
    startup_wave8 = args[:1] == ['--startup-wave8-implementation']
    implementation = hierarchical_cells or fixed48 or bounded_publication or root_reuse or startup_wave8 or args[:1] == ['--semantic-1m-implementation']
    test_build = args[:1] == ['--semantic-1m-test-build']
    semantic_1m = implementation or test_build or args[:1] == ['--semantic-1m']
    if semantic_1m:
        args = args[1:]
    configure(semantic_1m, test_build=test_build, implementation=implementation, startup_wave8=startup_wave8, root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells)
    # ponytail: shared launch archives the repository in memory; stream it in
    # the shared launcher if root's launch resource gate proves insufficient.
    if args[:1] and args[0].startswith('--'):
        resource.setrlimit(resource.RLIMIT_AS, (200*1024**2,200*1024**2))
    if args == ['--self-check']:
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
            _hierarchical_cells_protocol_self_check()
            _fixed48_stages_self_check(hierarchical_cells=True)
            _hierarchical_cells_script_self_check()
            _startup_wave8_preflight_self_check(hierarchical_cells=True)
        self_check(semantic_1m, test_build=test_build, implementation=implementation, startup_wave8=startup_wave8, root_reuse=root_reuse, bounded_publication=bounded_publication, fixed48=fixed48, hierarchical_cells=hierarchical_cells)
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
        assert len(args) == 1, 'usage: launch_native_workspace_execution_spot.py [--semantic-1m | --semantic-1m-test-build | --semantic-1m-implementation | --startup-wave8-implementation | --root-reuse-implementation | --bounded-publication-implementation | --fixed48-implementation | --hierarchical-cells-implementation] aNNNN | --self-check | --stage REPO OUT | --check-receipt OUT | --replay OUT'
        with open('/tmp/borsuk-native-workspace-execution-launch.lock','a+') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            main(args[0])
