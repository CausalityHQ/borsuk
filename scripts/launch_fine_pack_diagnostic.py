"""One frozen native packing diagnostic; root owns config, transport and launch.

CLI: aNNNN | --self-check | --replay OUT. Remote --remote is bootstrap-only.
--sq4 explicitly selects the root-frozen native SQ4 experiment.
--histogram-sq4 selects the separate learned-codebook experiment.
--corrected-four-bit selects the separately qualified direction codec.
--pq-residual-source selects the separately qualified truth-free source probe.
--co-selection-layout selects the qualified source-only virtual layout falsifier.
--corrected-four-bit --canary aNNNN runs only disposable infrastructure admission.
--remote-canary is bootstrap-only; --replay-canary OUT authenticates its closure.
No compiler, query runner, packing algorithm, retries or replacement instances.
"""
import argparse
from datetime import datetime, timedelta
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import signal
import stat
import subprocess
import sys
import time

if not __debug__:
    raise RuntimeError('diagnostic requires assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path('docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/packing-diagnostic')
CONFIG, NAME = ROOT/'config.json', ''
SCHEMA = 'borsuk-fine-pack-diagnostic-spot-v1'
PREFIX = 'research/hierarchical-cells/20261005/fine-pack-diagnostic-'
TOKEN_PREFIX, TAG = 'fine-pack-diagnostic-', 'borsuk-fine-pack-diagnostic'
MODULE = 'scripts.launch_fine_pack_diagnostic'
WALL, COMPUTE_CAP, SPOT_MAX_USD_PER_HOUR = 900, .15, .60
INSTANCE_TYPE, IMAGE_ID = 'c7i.2xlarge', 'ami-0b8a830d6339a9758'
ROOT_DEVICE_NAME, SUBNET = '/dev/sda1', 'subnet-034528fbd6977848f'
REGION, BUCKET = 'eu-central-1', 'borsuk-bench-453182569524-euc1'
REMOTE_ROOT = Path('/mnt/fine-pack-diagnostic')
SUPERVISOR_UNIT = 'fine-pack-supervisor'
CONTROLLERS = {'cpu', 'memory', 'pids'}
BINARY_NAME = 'binaries/hierarchical_semantic_cells'
BINARY_PIN = dict(bytes=7860472, sha256='a4ea1d88223dbcf73b714ca2f75b9556c9d4f1389333391c1ac619a8bb9b4f45')
BINARY_KEY = 'research/semantic-router/20261005/fine-sq8-packing-binary-repair-a0001/artifacts/'+BINARY_NAME
NATIVE_COMMIT = '71f77b3a30ea7f364c7a9d7e82188151a25a6be4'
SOURCE_ID = '13286db011c6f0699f74b0e10e9d340e0ac565346a8c8640bcf5e4b64990b36d'
DIAGNOSTIC_SOURCES = {
    'fine_sq8_groups.rs': 'a66c728c9bc708e542b57a8c8dc4efa681a705c2731717ad3eda2d52546d5027',
    'resident_vector_graph.rs': 'c62459b45caeaaa798b16b24b88180ad6d4dc5bdd554cd804e6d6b354eb3b79d',
    'bin/hierarchical_semantic_cells.rs': 'f636602726e30f8f5c4d67f557546318ffb75f091da30c7a131a1247d39a6bd3'}
CAPS = dict(cpu_threads=1, memory_bytes=256*1024**2, swap_bytes=0,
            deadline_seconds=300, operations=500000000, output_bytes=2*1024**2)
FIXED = dict(region=REGION, bucket=BUCKET, instance_type=INSTANCE_TYPE,
             image_id=IMAGE_ID, subnet_id=SUBNET, root_device_name=ROOT_DEVICE_NAME,
             machine_limit_seconds=WALL, compute_cap_usd=COMPUTE_CAP,
             spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR, ebs_s3_allowance_usd=.15,
             native_cpu_affinity=[0], native_caps=CAPS, tasks_max=32,
             automatic_retry=False, quality_or_performance_claim=False)
# The lifecycle's import closure is frozen too; the remote supervisor imports
# only this file and the SDK. Root supplies the minimal archive roster.
CODE = tuple(sorted('scripts/'+n+'.py' for n in (
    'launch_fine_pack_diagnostic', 'launch_native_metadata_ranges_cold_spot',
    'launch_native_startup_profile_spot', 'launch_native_peer_1m_spot',
    'launch_v174_relaid_bind_compile_spot', 'launch_v157_primary_feasibility_spot',
    'check_native_metadata_ranges_build', 'check_native_metadata_ranges_stats',
    'check_native_startup_build', 'check_native_startup_stats',
    'run_native_metadata_ranges_cold', 'run_native_cold_first_query',
    'run_native_peer_1m_worker', 'run_native_peer_offered_http',
    'run_native_union_offered_http', 'run_native_union_http', 'rest_coexistence_load')))
EVIDENCE = ROOT.parent/'packing-implementation-gates/binary-repair/a0001'
RECEIPTS = tuple(dict(path=str(EVIDENCE/name), bytes=size, sha256=digest) for name, size, digest in (
    ('parent-verification.json', 14424, 'c13efa7743a6d9e9f9d21b0e9e5ec56b3b5058484b2d4eca71ec7c817c6132bd'),
    ('aws-terminal.json', 3128, '3dca512d1b9c89e571944e792efc36d91a66e02c6e5195c78b2810098ea8855e'),
    ('aws-closeout.json', 136, '1d3cce4ac98066fe088ed543e0a34fefa34f8557d935edd63f5f4cf9b5558fd2'),
    ('workspace-receipt.json', 62788, '697db113bf61402d8a6c776fbbd7c94bb12538faec904c2f23ab7bf611a02333'),
    ('source-qualification.json', 410667, 'ffae1c458c116584539b234825a314d2fab71d90f593cd64d19c8849593c56d9')))
INPUT_PINS = (
    ('/mnt/hierarchical-100k/screen/fine/layouts/relaion/manifest.json', 22721, '0a2a2dff6a7abc48143d08eb491a150bbed03957b81b0261c2b8a0434612afec'),
    ('/mnt/hierarchical-100k/screen/fine/layouts/relaion/graph.bin', 26712462, 'f817d7f4fbdd640f66590f58f9e07e9218753fbec4517ba95fc405a78d612325'),
    ('/mnt/hierarchical-100k/screen/fine/layouts/cohere/manifest.json', 22815, 'ea1a792a841014ef08ed805d2872403739ba778ea8b8d3a9e5eab1b7f831c8bb'),
    ('/mnt/hierarchical-100k/screen/fine/layouts/cohere/graph.bin', 26844914, '4afd895f2e684f833437f4e6d4270c4713bbeb54fe67b50919f796e95f896e2e'),
    ('/mnt/hierarchical-100k/packing-original-seal.json', 1109, '374cbd0e8700c85c2b4229468c3a9fa20283deb969b661386fb0078024b9de62'),
    ('/mnt/hierarchical-100k/packing-prefix.jsonl', 1665668, '7abcf7830e10d214999b26fb499cebf925e16b8a88a931ddb9f981ad3d28d025'))
ARTIFACTS = ('config.json', 'native-config.json', 'source-qualification.json',
    'stage-receipt.json', 'native-exit.json', 'resources.json', 'cleanup.json',
    'cpu.txt', 'runtime-abi.json', 'run-closed.log', 'native.log', BINARY_NAME,
    'screen/report.json', 'screen/report.permutations.json', 'screen/report.prefix.jsonl')
ROSTER_SHA = hashlib.sha256(json.dumps(ARTIFACTS, separators=(',', ':')).encode()).hexdigest()
CGROUP_FILES = ('memory.max', 'memory.peak', 'memory.swap.max', 'memory.swap.peak',
                'memory.events', 'memory.swap.events', 'cpu.max', 'cpu.stat',
                'pids.max', 'pids.current', 'pids.events', 'cgroup.procs', 'cgroup.events')
SQ4 = False
HISTOGRAM_SQ4 = False
CORRECTED_FOUR_BIT = False
CANARY = False
PQ_RESIDUAL_SOURCE = False
CO_SELECTION_LAYOUT = False
CO_ROOT = ROOT.parent/'co-selection-layout'
CO_GATES = CO_ROOT/'implementation-gates'
CO_RECEIPTS = ('source-qualification.json.gz', 'workspace-receipt.json.gz',
    'aws-terminal.json', 'aws-closeout.json', 'source-before.json.gz',
    'source-after.json.gz', 'workspace-cgroup.json.gz')
CO_EVIDENCE = (CO_GATES/'config.json', CO_GATES/'native-source-manifest.json',
    Path('scripts/check_co_selection_implementation.sh'),
    *(CO_GATES/'a0001'/n for n in CO_RECEIPTS))
CO_COMMIT = '510bde198af4f28952db2fc85a42018472b23fdc'
CO_SOURCE_ID = '64fe7b41484ece6a6379c716727e9407c9a1d48e0a44e71669c8994c3e32289c'
CO_BINARY_PIN = dict(bytes=13549088, sha256='1db976a4a29de491cf03c14fe7246330124e1fa4d479d4895dd30f84507d7672')
CO_CAPS = dict(memory_bytes=512*1024**2, source_auth_bytes=1024**3,
    construction_operations=512000000000, replay_operations=20000000000,
    output_bytes=64*1024**2, cpu_threads=1, swap_bytes=0)
CO_MANIFEST, CO_PROTOCOL, CO_EVIDENCE_PINS, CO_QUALIFICATION = {}, {}, {}, ()
CO_ORIGINAL_PINS = INPUT_PINS
CO_SOURCE_NAMES = ('co_selection_layout.rs', 'fine_sq8_groups.rs', 'pq64_nominee.rs',
    'resident_vector_graph.rs', 'hierarchical_semantic_cells.rs', 'budgeted_page_rank.rs',
    'sq8_page_authority.rs', 'returned_sq8.rs', 'exact_sq8_nominee.rs', 'centroid_hnsw.rs',
    'sq8_source.rs', 'bin/hierarchical_semantic_cells.rs', 'lib.rs')
PQ_ROOT = ROOT.parent/'sq4-refinement/pq-residual'
PQ_EVIDENCE = (PQ_ROOT/'implementation-gates/source-contract-7bb862b2.json',
    PQ_ROOT/'implementation-gates/native-source-manifest.json',
    PQ_ROOT/'implementation-gates/config.json', PQ_ROOT/'source-probe/native-config-draft.json',
    PQ_ROOT/'source-probe/transport-draft.json', Path('scripts/check_pq_residual_implementation.sh'))
PQ_CONTRACT, PQ_MANIFEST, PQ_DRAFT, PQ_TRANSPORT, PQ_PROTOCOL, PQ_EVIDENCE_PINS = {}, {}, {}, {}, {}, {}
SCIENCE_STATE = {}
CANARY_BINARY_PIN = dict(bytes=10608320, sha256='1cba6503a6a51fad193110b2c2dd6cee324b1cdb8f4235761a9afa6c4b24d00c')
CORRECTED_ROOT = ROOT.parent/'sq4-refinement/corrected-rabitq'
CORRECTED_EVIDENCE = (
    ('source-contract-4ec11b94.json', 9072, 'ad8fcb1fbaae99102497128ea6656fd1ab871fd8995d04df9297ccfa4605607d'),
    ('source-inventory-4ec11b94.json', 47525, '5d6a83873a4239ad9adfa277ba493500c4ad52e2c32795b11bac4cde026fd7cb'),
    ('qualification-source-contract-8c9f76f8.json', 30606, 'b9a2bc45f99e25c71a11fb6674808b5cf262978b49eb27983292c3bb71fb3cc0'),
    ('closed-populations.json', 175929, '764674f0623797d1303158750fa82a568ea5fabd4fd57f7df5b333865e02a29a'))
CORRECTED_PROTOCOL_SHA = 'abd38ba36ba44ee93bd1b0be4a6b3627085b8a1878c3ac45164d51df58aabba8'
CORRECTED_COMMIT = '4ec11b94bdc15b103bc713418e9f2c0705a9d0b6'
# Recomputed from source_identity()'s 12 framed include_bytes and codec body at
# the exact isolated native revision, never from the controller's Rust tree.
CORRECTED_DIAGNOSTIC_SOURCE_ID = '45821354fe7ee55fed119a81c51ef02cd0a82817c7e199d494d75b83366646ab'
CORRECTED_SUPERVISOR_SECONDS = 1800  # Exact native valid() still caps its own deadline at 600s.
CORRECTED_SOURCE, CORRECTED_PROTOCOL, CORRECTED_POPULATIONS = {}, {}, {}
SQ4_NAME, SQ4_SCHEMA, SQ4_CLI = 'sq4', 'borsuk-fixed-sq4', 'check-fine-sq4'
SQ4_CODEC = 'borsuk-sq4-nearest17-original-coefficients-v1'
HISTOGRAM_TRAINER = 'occupied-u8-weighted-contiguous-f64-dp-smallest-predecessor-v1'
# encoded({stages, mandatory_tests}) from root's corrected e91cf344 launcher:
# all14 commands, all nine histogram names and all historical regressions.
HISTOGRAM_QUALIFICATION_PROTOCOL_SHA = 'f5ac8d1e19c074de4acf00c2aac20647c09df8b648a350dafa5453b031c27155'
SQ4_ROOT = ROOT.parent/'sq4-refinement'
# Prospective fixture provenance only; config.native_source owns the qualified pins.
SQ4_COMMIT = 'c2d233d6752d0a058d78c6077e51f31f0255e7c1'
SQ4_SOURCE_ID = 'e808aae7d27e570d1bf7651d70f6144a7a93e09bae46f0222151852fff0a70db'
SQ4_INPUT_ROOT = Path('/mnt/hierarchical-100k')
SQ4_SCRATCH_CAP = 4*1024**3
SQ4_CLOSURE_RESERVE = 12*1024**2  # Receipts + closed log + terminal + live log tail.
SQ4_SOURCES = {
    'crates/borsuk/src/fine_sq8_groups.rs': '250f592b8c2ad654f1b6b7281f08a0b3c420b4d4d157de29f8e2017212788f37',
    'crates/borsuk/src/bin/hierarchical_semantic_cells.rs': '4d46713e88ac6f75230327fc160734bebc991202e7904ee4994ccfd2760f7e6a'}
SQ4_RECEIPTS = ('parent-verification.json', 'source-qualification.json',
                 'workspace-receipt.json', 'aws-terminal.json', 'aws-closeout.json')
SQ4_OUTPUTS = ('screen/report.json', 'screen/report.sq4-0.bin', 'screen/report.sq4-1.bin',
    'screen/report.sq4-payloads.json', 'screen/report.sq4-prefix.jsonl', 'screen/report.sq4-freeze.json',
    *(f'screen/report.sq4-result-{i}.json' for i in range(128)))
SQ4_TESTS = ('tests::fine_sq4_strict_cli_dispatch', 'tests::fine_sq4_real_native_pipeline_128_seal_before_truth',
    *(f'fine_sq8_groups::pack_diagnostic::sq4_diagnostic::tests::{n}' for n in (
        'fine_sq4_all_codes_numeric_oracle_odd_tail_nonunit_ties', 'fine_sq4_exact_cover_superset_and_binding',
        'fine_sq4_auth_fifo_corruption_caps_and_durability', 'fine_sq4_full_pipeline_failure_order_and_sync')))
# Canonical encoded({stages, mandatory_tests}) from bf0667e8's existing
# FINE_SQ8_STAGES/FINE_SQ8_REQUIRED_TESTS. No reduced self-asserted roster.
SQ4_QUALIFICATION_PROTOCOL_SHA = 'd3ac6d4f45391decf7c08f483a6209f99505897c62355a52d40f0a2b42f2cc8a'


def configure_sq4(*, histogram=False, corrected=False):
    """Opt-in campaign only. Resource/transport authority still comes from root."""
    global SQ4, ROOT, CONFIG, SCHEMA, PREFIX, TOKEN_PREFIX, TAG, REMOTE_ROOT
    global CAPS, NATIVE_COMMIT, SOURCE_ID, INPUT_PINS, ARTIFACTS, ROSTER_SHA
    global HISTOGRAM_SQ4, SQ4_NAME, SQ4_SCHEMA, SQ4_CLI, SQ4_CODEC, SQ4_OUTPUTS
    global CORRECTED_FOUR_BIT, SQ4_SOURCES, SQ4_SCRATCH_CAP
    global CORRECTED_SOURCE, CORRECTED_PROTOCOL, CORRECTED_POPULATIONS
    require(not (histogram and corrected), 'one SQ4 campaign mode')
    if SQ4:
        require(HISTOGRAM_SQ4 is histogram and CORRECTED_FOUR_BIT is corrected, 'one SQ4 campaign mode')
        return
    SQ4 = True
    HISTOGRAM_SQ4 = histogram
    CORRECTED_FOUR_BIT = corrected
    ROOT = SQ4_ROOT/'native-diagnostic'; CONFIG = ROOT/'config.json'
    SCHEMA = 'borsuk-fixed-sq4-diagnostic-spot-v1'
    PREFIX = 'research/hierarchical-cells/20261005/fixed-sq4-diagnostic-'
    TOKEN_PREFIX, TAG = 'fixed-sq4-diagnostic-', 'borsuk-fixed-sq4-diagnostic'
    REMOTE_ROOT = Path('/mnt/fixed-sq4-diagnostic')
    CAPS = dict(cpu_threads=1, memory_bytes=1024**3, swap_bytes=0,
                deadline_seconds=600, operations=20000000000, output_bytes=256*1024**2)
    NATIVE_COMMIT, SOURCE_ID = SQ4_COMMIT, SQ4_SOURCE_ID
    if histogram:
        SQ4_NAME, SQ4_SCHEMA, SQ4_CLI = 'histogram-sq4', 'borsuk-histogram-sq4', 'check-fine-histogram-sq4'
        SQ4_CODEC = 'borsuk-sq4-histogram16-original-coefficients-v1'
        ROOT = SQ4_ROOT/'histogram-codebook/native-diagnostic'; CONFIG = ROOT/'config.json'
        SCHEMA = SQ4_SCHEMA+'-diagnostic-spot-v1'
        PREFIX = 'research/hierarchical-cells/20261005/histogram-sq4-diagnostic-'
        TOKEN_PREFIX, TAG = 'histogram-sq4-diagnostic-', 'borsuk-histogram-sq4-diagnostic'
        REMOTE_ROOT = Path('/mnt/histogram-sq4-diagnostic')
        NATIVE_COMMIT, SOURCE_ID = '', ''  # Completed root authority supplies both.
        SQ4_OUTPUTS = tuple(n.replace('.sq4-', '.histogram-sq4-') for n in SQ4_OUTPUTS)+tuple(
            f'screen/report.histogram-sq4-{i}.bin-{suffix}' for i in range(2)
            for suffix in ('book.bin','groups.bin','root.json'))
    if corrected:
        SQ4_NAME, SQ4_SCHEMA, SQ4_CLI = 'corrected-four-bit', 'borsuk-corrected-four-bit', 'check-fine-corrected-four-bit'
        SQ4_CODEC = 'borsuk-corrected-four-bit-direction-v1'
        ROOT = CORRECTED_ROOT/'native-diagnostic'; CONFIG = ROOT/'config.json'
        SCHEMA = SQ4_SCHEMA+'-diagnostic-spot-v1'
        PREFIX = 'research/hierarchical-cells/20261005/corrected-four-bit-diagnostic-'
        TOKEN_PREFIX, TAG = 'corrected-four-bit-diagnostic-', 'borsuk-corrected-four-bit-diagnostic'
        REMOTE_ROOT = Path('/mnt/corrected-four-bit-diagnostic')
        SQ4_SCRATCH_CAP = 8*1024**3
        NATIVE_COMMIT, SOURCE_ID = '', ''  # Completed root proof supplies authority.
        values = []
        for name, size, digest in CORRECTED_EVIDENCE:
            body = read(Path(__file__).resolve().parents[1]/CORRECTED_ROOT/name)
            require(pin(body) == dict(bytes=size, sha256=digest), 'corrected committed evidence: '+name)
            values.append(decode(body))
        contract, CORRECTED_SOURCE, CORRECTED_PROTOCOL, CORRECTED_POPULATIONS = values
        SQ4_SOURCES = contract['file_sha256']
        require(contract['commit'] == CORRECTED_SOURCE['native_source_commit'] == CORRECTED_COMMIT
                and len(CORRECTED_SOURCE['source_sha256']) == 405, 'corrected exact source evidence')
        require(sha(encoded(dict(stages=[(s['stage'],s['command']) for s in CORRECTED_PROTOCOL['stages']],
                mandatory_tests=CORRECTED_PROTOCOL['fixed']['mandatory_tests']))) == CORRECTED_PROTOCOL_SHA,
                'corrected all15 protocol')
        SQ4_OUTPUTS = tuple(n.replace('.sq4-', '.corrected-four-bit-') for n in SQ4_OUTPUTS)+(
            'screen/report.corrected-four-bit-rotation.bin', 'screen/report.corrected-four-bit-startup.json',
            *(f'screen/report.corrected-four-bit-{i}-{suffix}' for i in range(2) for suffix in ('groups.bin','root.json')))
    # Metadata only: never hydrate or parse a retained input during preflight.
    roster = decode(read(Path(__file__).resolve().parents[1]/SQ4_ROOT/'prospective-input-roster.json'))
    require(roster['schema'] == 'borsuk-sq4-prospective-input-roster-v1'
            and pin(encoded(roster['inputs'])) == dict(bytes=5185,
                sha256='657f8a5f34e6865d46f9b526e627debb413ab1f1a6ca9d695f47d38d3dd2ffbb'), 'SQ4 retained roster pin')
    INPUT_PINS = tuple((d['destination'], d['bytes'], d['sha256']) for d in roster['inputs'])
    if corrected:
        INPUT_PINS += ((str(SQ4_INPUT_ROOT/'corrected-four-bit-closed-populations.json'),
                        CORRECTED_EVIDENCE[-1][1], CORRECTED_EVIDENCE[-1][2]),)
    ARTIFACTS = tuple(n for n in ARTIFACTS if not n.startswith('screen/')) + (
        'scratch.json', *(f'qualification/{n}' for n in SQ4_RECEIPTS), *SQ4_OUTPUTS)
    if corrected:
        ARTIFACTS += ('canary-admission.json',)
    ROSTER_SHA = sha(json.dumps(ARTIFACTS, separators=(',', ':')).encode())


def require(condition, message):
    if not condition:
        raise ValueError(message)


def qualification_body(path):
    if str(path).endswith('.gz'):
        with regular(path) as raw, gzip.GzipFile(fileobj=raw) as source:
            body = source.read(4*1024**2+1)
        require(len(body) <= 4*1024**2, 'qualification decompression cap')
        return body
    return read(path)


def configure_co_selection_layout():
    """Only diagnostic glue; root must supply the still-absent run authority."""
    global CO_SELECTION_LAYOUT, SQ4, ROOT, CONFIG, SCHEMA, PREFIX, TOKEN_PREFIX, TAG, REMOTE_ROOT
    global CAPS, FIXED, WALL, COMPUTE_CAP, SQ4_CLI, SQ4_OUTPUTS, ARTIFACTS, ROSTER_SHA
    global NATIVE_COMMIT, SOURCE_ID, INPUT_PINS, SQ4_SCRATCH_CAP
    global CO_MANIFEST, CO_PROTOCOL, CO_EVIDENCE_PINS, CO_QUALIFICATION
    require(not SQ4 or CO_SELECTION_LAYOUT, 'one native experiment')
    if CO_SELECTION_LAYOUT:
        return
    import shlex
    repo = Path(__file__).resolve().parents[1]
    bodies = [read(repo/p) for p in CO_EVIDENCE]
    gates, CO_MANIFEST = map(decode, bodies[:2])
    stages = [shlex.split(line)[1:] for line in bodies[2].decode().splitlines() if line.startswith('run_stage ')]
    CO_PROTOCOL = dict(stages=[(s[0],s[1:]) for s in stages], mandatory_tests=gates['mandatory_tests'])
    require(len(stages) == len({s[0] for s in stages}) == 21
        and len(CO_PROTOCOL['mandatory_tests']['co-selection-tests']) == 11
        and len(CO_PROTOCOL['mandatory_tests']['co-selection-bin-tests']) == 1
        and CO_MANIFEST['native_source_commit'] == CO_COMMIT
        and CO_MANIFEST['source_file_count'] == len(CO_MANIFEST['source_sha256']) == 407
        and CO_MANIFEST['source_identity_sha256'] == CO_SOURCE_ID
        == sha(json.dumps(CO_MANIFEST['source_sha256'],sort_keys=True,separators=(',',':')).encode()),
        'co-selection exact qualified source407/all21 protocol')
    CO_EVIDENCE_PINS = {str(p):pin(b) for p,b in zip(CO_EVIDENCE,bodies)}
    CO_QUALIFICATION = tuple(dict(path=str(p),**CO_EVIDENCE_PINS[str(p)]) for p in CO_EVIDENCE[3:])
    CO_SELECTION_LAYOUT = SQ4 = True  # Reuse opaque staging, scratch and supervisor.
    ROOT = CO_ROOT/'native-diagnostic'; CONFIG = ROOT/'config.json'
    SCHEMA = 'borsuk-co-selection-layout-diagnostic-spot-v1'
    PREFIX = 'research/hierarchical-cells/20261006/co-selection-layout-diagnostic-'
    TOKEN_PREFIX, TAG = 'co-selection-layout-', 'borsuk-co-selection-layout-diagnostic'
    REMOTE_ROOT = Path('/mnt/co-selection-layout-diagnostic')
    CAPS = dict(CO_CAPS, caller_pinned_bytes=0, deadline_seconds=0)
    WALL = COMPUTE_CAP = SQ4_SCRATCH_CAP = 0  # No inferred runtime/cost/scratch admission.
    FIXED = dict(FIXED, machine_limit_seconds=WALL, compute_cap_usd=COMPUTE_CAP, native_caps=CAPS)
    NATIVE_COMMIT, SOURCE_ID, INPUT_PINS = CO_COMMIT, CO_SOURCE_ID, ()
    SQ4_CLI = 'check-co-selection-layout'
    SQ4_OUTPUTS = ('screen/report.json', *(f'screen/report.{dataset}.{suffix}'
        for dataset in ('relaion','cohere') for suffix in ('selections.bin','map.json')),
        'screen/report.held.jsonl', 'screen/report.prefix.jsonl', 'screen/report.plans.jsonl')
    ARTIFACTS = (*ARTIFACTS[:12], 'scratch.json', 'imports.json', *SQ4_OUTPUTS,
        *('qualification/'+n for n in CO_RECEIPTS))
    ROSTER_SHA = sha(json.dumps(ARTIFACTS,separators=(',',':')).encode())


def co_selection_qualification(config, base, collected=False):
    """Replay the existing qualified source/binary receipts, never requalify."""
    require(config['native_qualification'] == list(CO_QUALIFICATION), 'co-selection original qualification roster/pins')
    bodies = {}
    for descriptor in CO_QUALIFICATION:
        name = Path(descriptor['path']).name
        path = Path(base)/('qualification/'+name if collected else descriptor['path'])
        require(file_pin(path) == {k:descriptor[k] for k in ('bytes','sha256')}, 'co-selection qualification drift: '+name)
        bodies[name] = qualification_body(path)
    q,w,t,close,before,after,cgroup = (decode(bodies[n]) for n in CO_RECEIPTS)
    sources = CO_MANIFEST['source_sha256']
    require(q['schema'] == 'borsuk-co-selection-implementation-gates-qualification-v1'
        and w['schema'] == 'borsuk-co-selection-implementation-gates-receipt-v1'
        and t['schema'] == 'borsuk-co-selection-implementation-gates-spot-v1'
        and all(v['source_file_count'] == 407 and v['source_identity_sha256'] == CO_SOURCE_ID for v in (q,w,t))
        and q['native_source_commit'] == t['native_source_commit'] == CO_COMMIT
        and q['source_sha256'] == w['source_sha256'] == before == after == sources,
        'co-selection unchanged exact full407 qualification')
    manifest = dict(path=str(CO_EVIDENCE[1]),**CO_EVIDENCE_PINS[str(CO_EVIDENCE[1])])
    require(q['native_source_manifest'] == manifest
        and q['native_source_manifest_sha256'] == t['native_source_manifest_sha256'] == manifest['sha256']
        and q['code_sha256'][str(CO_EVIDENCE[2])] == CO_EVIDENCE_PINS[str(CO_EVIDENCE[2])]['sha256']
        and q['config_sha256'] == w['config_sha256'] == t['config_sha256'] == CO_EVIDENCE_PINS[str(CO_EVIDENCE[0])]['sha256'],
        'co-selection qualified manifest/protocol/config binding')
    require(q['mandatory_test_names_pending'] is w['mandatory_test_names_pending'] is False
        and w['qualified'] is w['command_started'] is w['command_completed'] is w['source_unchanged'] is True
        and all(type(w[k]) is int and w[k] == 0 for k in ('exit_status','gate_status'))
        and w['qualification_sha256'] == t['source_qualification_sha256'] == sha(bodies[CO_RECEIPTS[0]])
        and q['mandatory_tests'] == w['mandatory_tests'] == CO_PROTOCOL['mandatory_tests']
        and [(s['stage'],s['command']) for s in w['stages']] == CO_PROTOCOL['stages'],
        'co-selection all21 completed original gates')
    previous = None
    for stage in w['stages']:
        started,finished = (datetime.fromisoformat(stage[k]) for k in ('started_at','finished_at'))
        mandatory = CO_PROTOCOL['mandatory_tests'].get(stage['stage'],())
        require(started.utcoffset() == finished.utcoffset() == timedelta(0)
            and finished >= started and (previous is None or started >= previous)
            and all(type(stage[k]) is int and stage[k] == 0 for k in ('exit_status','gate_status','log_exit_status'))
            and (type(stage['tests_run']) is int and stage['tests_run'] >= len(mandatory) > 0 if mandatory else stage['tests_run'] is None)
            and stage['required_test_passes'] == {n:1 for n in mandatory}
            and all(type(n) is int for n in stage['required_test_passes'].values()), 'co-selection serial original gate/pass counts')
        previous = finished
    require(t['status'] == t['phase'] == 'complete'
        and all(type(t[k]) is int and t[k] == 0 for k in ('exit_code','original_exit_code'))
        and close['state'] == 'terminated' and close['nodes'] == {'0':{'instance_id':t['instance_id']}}
        and t['artifacts']['workspace-receipt.json'] == pin(bodies[CO_RECEIPTS[1]])
        and w['artifacts'][BINARY_NAME] == t['artifacts'][BINARY_NAME] == CO_BINARY_PIN,
        'co-selection original exit0/binary/SAME-ID qualification closeout')
    for name in ('source-before.json','source-after.json','workspace-cgroup.json'):
        require(w['artifacts'][name] == t['artifacts'][name] == pin(bodies[name+'.gz']), 'co-selection qualified artifact: '+name)
    require(cgroup['closed'] is True, 'co-selection qualification resource closure')
    for snapshot in (cgroup['before'],cgroup['after']):
        require(int(snapshot['memory.max']) == 8*1024**3 and int(snapshot['memory.peak']) <= 8*1024**3
            and snapshot['cpu.max'].split() == ['200000','100000']
            and int(snapshot['memory.swap.max']) == int(snapshot['memory.swap.peak']) == 0
            and all(events(snapshot['memory.events'])[k] == 0 for k in ('oom','oom_kill','oom_group_kill')),
            'co-selection qualified compiler resources')
    return dict(zip(CO_RECEIPTS,(q,w,t,close,before,after,cgroup)))


def validate_co_selection_config(config, base=None):
    global FIXED, CAPS, WALL, COMPUTE_CAP, SQ4_SCRATCH_CAP, INPUT_PINS
    require(set(config) == {'schema','authority_pending','fixed','native_config','native_config_sha256',
        'source_metadata','binary','inputs','native_source','native_qualification','code_sha256',
        'source_archive_paths','source_archive_paths_sha256'}
        and config['schema'] == SCHEMA and config['authority_pending'] is False, 'co-selection frozen root authority required')
    fixed,native = config['fixed'],config['native_config']
    require(set(native) == {'schema','panels','original_seal','prefix','caps','prior_reads'}
        and native['schema'] == 'borsuk-co-selection-config-v1' and len(native['panels']) == 2
        and sha(encoded(native)) == config['native_config_sha256'] and len(encoded(native)) <= 65536,
        'co-selection strict native Config; no request vectors/GT/SQ8/selection overrides')
    caps = native['caps']
    require(set(caps) == set(CO_CAPS)|{'caller_pinned_bytes','deadline_seconds'}
        and all(type(caps[k]) is int and caps[k] == v for k,v in CO_CAPS.items())
        and type(caps['caller_pinned_bytes']) is int and 0 <= caps['caller_pinned_bytes'] <= caps['memory_bytes']-64*1024**2
        and type(caps['deadline_seconds']) is int and 1 <= caps['deadline_seconds'] <= 86400,
        'co-selection native512MiB/CPU1/noSwap/construction512B/replay20B/auth1GiB/output64MiB')
    variable = {'scratch','native_caps','machine_limit_seconds','compute_cap_usd'}
    require(set(fixed) == set(FIXED)|{'scratch'}
        and all(encoded(fixed[k]) == encoded(FIXED[k]) for k in FIXED if k not in variable)
        and encoded(fixed['native_caps']) == encoded(caps)
        and type(fixed['machine_limit_seconds']) is int and caps['deadline_seconds'] < fixed['machine_limit_seconds'] <= 86400
        and type(fixed['compute_cap_usd']) in (int,float) and 0 < fixed['compute_cap_usd'] < float('inf')
        and fixed['machine_limit_seconds']*fixed['spot_max_usd_per_hour']/3600 <= fixed['compute_cap_usd'],
        'co-selection root frozen wall/cost/host/native caps')
    authority = config['native_source']
    source_pins = {n:CO_MANIFEST['source_sha256']['crates/borsuk/src/'+n] for n in CO_SOURCE_NAMES}
    require(authority == dict(commit=CO_COMMIT,full_source_identity_sha256=CO_SOURCE_ID,
        source_sha256=source_pins,qualification_protocol_sha256=sha(encoded(CO_PROTOCOL))), 'co-selection qualified exact native source')
    prior = native['prior_reads']
    require(set(prior) == {'operations','bytes'} and type(prior['operations']) is type(prior['bytes']) is int
        and 0 <= prior['operations'] <= 32 and 0 <= prior['bytes'] <= 16*1024**2, 'co-selection SAME total32/16MiB prior reads')
    metadata = config['source_metadata']; descriptors = []
    require(type(metadata) is list and len(metadata) == 2, 'co-selection opaque primary/group metadata pair')
    for i,(panel,dataset) in enumerate(zip(native['panels'],('relaion','cohere'))):
        require(set(panel) == {'dataset','root','identity','canonical','source_order','fine_order','pq','graph'}
            and panel['dataset'] == dataset and set(metadata[i]) == {'primary_root','groups'}, 'co-selection strict Panel/support fields')
        identity = panel['identity']
        require(set(identity) == {'generation','rows','dimensions','source','layout','pq'}
            and type(identity['generation']) is int and 0 < identity['generation'] < 2**64
            and type(identity['rows']) is type(identity['dimensions']) is int
            and identity['rows'] == 100000 and identity['dimensions'] == 768
            and all(type(identity[k]) is list and len(identity[k]) == 32
                and all(type(n) is int and 0 <= n <= 255 for n in identity[k]) for k in ('source','layout','pq')),
            'co-selection original PQ/graph identity100k/D768')
        for key,size in (('canonical',308000000),('source_order',800000),('fine_order',800000),('pq',7186456)):
            require(panel[key]['bytes'] == size, 'co-selection source geometry: '+key)
        require(0 < panel['root']['bytes'] <= 65536 and 0 < panel['graph']['bytes'] <= 51200000
            and 0 < metadata[i]['primary_root']['bytes'] <= 65536 and metadata[i]['groups']['bytes'] == 200000,
            'co-selection native metadata/graph bounds')
        for key,j in (('root',2*i),('graph',2*i+1)):
            p,n,h = CO_ORIGINAL_PINS[j]
            require(panel[key] == dict(path=p,bytes=n,sha256=h), 'co-selection original closed root/graph pin')
        descriptors.extend(panel[k] for k in ('root','canonical','source_order','fine_order','pq','graph'))
        descriptors.extend(metadata[i][k] for k in ('primary_root','groups'))
    for key,j in (('original_seal',4),('prefix',5)):
        p,n,h = CO_ORIGINAL_PINS[j]
        require(native[key] == dict(path=p,bytes=n,sha256=h), 'co-selection closed seal/prefix pin')
        descriptors.append(native[key])
    for d in descriptors:
        path = Path(d['path'])
        require(set(d) == {'path','bytes','sha256'} and path.is_absolute() and Path(CO_ORIGINAL_PINS[4][0]).parent in path.parents
            and str(path) == d['path'] and '..' not in path.parts and len(str(path)) <= 4096,
            'co-selection retained absolute source path')
        require(body_pin({k:d[k] for k in ('bytes','sha256')},max_bytes=308000000)['bytes'] > 0, 'co-selection source artifact pin')
    inputs = config['inputs']
    require(len(inputs) == 18 and len({d['destination'] for d in inputs}) == len({d['key'] for d in inputs}) == 18,
        'co-selection exact eighteen distinct opaque sources/metadata')
    for d,a in zip(inputs,descriptors):
        require(set(d) == {'destination','key','bytes','sha256'} and {k:d[k] for k in ('bytes','sha256')} == {k:a[k] for k in ('bytes','sha256')}
            and d['destination'] == a['path'], 'co-selection transport binds native source; no payload/GT/request extras')
        object_key(d['key'])
    binary = config['binary']
    require(set(binary) == {'key','bytes','sha256'} and {k:binary[k] for k in ('bytes','sha256')} == CO_BINARY_PIN,
        'co-selection exact qualified executable')
    object_key(binary['key'])
    require(binary['key'] not in {d['key'] for d in inputs} and config['native_qualification'] == list(CO_QUALIFICATION),
        'co-selection distinct binary/original completed qualification')
    paths = sorted([str(CONFIG),*CODE,*(str(p) for p in CO_EVIDENCE)])
    require(config['source_archive_paths'] == paths and config['source_archive_paths_sha256'] == sha(
        json.dumps(paths,separators=(',',':')).encode()) and set(config['code_sha256']) == set(CODE), 'co-selection minimal existing import/archive closure')
    for digest in config['code_sha256'].values():
        body_pin(dict(bytes=0,sha256=digest))
    scratch = fixed['scratch']
    require(set(scratch) == {'input_bytes','native_output_bytes','binary_bytes','source_archive_bytes',
        'bootstrap_bytes','auxiliary_bytes','cap_bytes'} and all(type(n) is int and n > 0 for n in scratch.values())
        and scratch['input_bytes'] == sum(d['bytes'] for d in inputs)
        and scratch['native_output_bytes'] == caps['output_bytes'] and scratch['binary_bytes'] == binary['bytes']
        and sum(n for k,n in scratch.items() if k != 'cap_bytes') <= scratch['cap_bytes'], 'co-selection root frozen whole-worker scratch')
    if base is not None:
        for name,digest in config['code_sha256'].items():
            require(file_pin(Path(base)/name)['sha256'] == digest, 'co-selection CODE drift: '+name)
        for name,identity in CO_EVIDENCE_PINS.items():
            require(file_pin(Path(base)/name) == identity, 'co-selection evidence drift: '+name)
        co_selection_qualification(config,base)
    FIXED,CAPS = fixed,caps
    WALL,COMPUTE_CAP,SQ4_SCRATCH_CAP = fixed['machine_limit_seconds'],fixed['compute_cap_usd'],scratch['cap_bytes']
    INPUT_PINS = tuple((d['destination'],d['bytes'],d['sha256']) for d in inputs)
    return native


def configure_pq_residual_source():
    """Reuse the packing transport/supervisor; never qualify a pending binary."""
    global PQ_RESIDUAL_SOURCE, SQ4, ROOT, CONFIG, SCHEMA, PREFIX, TOKEN_PREFIX, TAG, REMOTE_ROOT
    global CAPS, FIXED, WALL, COMPUTE_CAP, NATIVE_COMMIT, SOURCE_ID, INPUT_PINS
    global SQ4_CLI, SQ4_OUTPUTS, ARTIFACTS, ROSTER_SHA, SQ4_SCRATCH_CAP
    global PQ_CONTRACT, PQ_MANIFEST, PQ_DRAFT, PQ_TRANSPORT, PQ_PROTOCOL, PQ_EVIDENCE_PINS
    require(not SQ4 or PQ_RESIDUAL_SOURCE, 'one native experiment')
    if PQ_RESIDUAL_SOURCE:
        return
    import shlex
    repo = Path(__file__).resolve().parents[1]
    bodies = [read(repo/p) for p in PQ_EVIDENCE]
    PQ_CONTRACT, PQ_MANIFEST, gates, PQ_DRAFT, PQ_TRANSPORT = map(decode, bodies[:5])
    stages = [shlex.split(line)[1:] for line in bodies[5].decode().splitlines() if line.startswith('run_stage ')]
    PQ_PROTOCOL = dict(stages=[(s[0],s[1:]) for s in stages], mandatory_tests=gates['mandatory_tests'])
    require(len(stages) == 19 and len({s[0] for s in stages}) == 19
        and set(PQ_CONTRACT['test_names']) == {n for k,v in gates['mandatory_tests'].items()
            if k.startswith('pq-residual-') and k != 'pq-residual-doc-tests' for n in v}
        and len(gates['mandatory_tests']['pq-residual-doc-tests']) == 1, 'PQ all19 and eleven+doc native protocol')
    PQ_EVIDENCE_PINS = {str(p):pin(b) for p,b in zip(PQ_EVIDENCE,bodies)}
    PQ_RESIDUAL_SOURCE = SQ4 = True  # Shared scratch/opaque staging and supervisor only.
    ROOT = PQ_ROOT/'source-probe/native-diagnostic'; CONFIG = ROOT/'config.json'
    SCHEMA = 'borsuk-pq-residual-source-diagnostic-spot-v1'
    PREFIX = 'research/hierarchical-cells/20261006/pq-residual-source-diagnostic-'
    TOKEN_PREFIX, TAG = 'pq-residual-source-', 'borsuk-pq-residual-source-diagnostic'
    REMOTE_ROOT = Path('/mnt/pq-residual-source-diagnostic')
    CAPS = dict(cpu_threads=1, memory_bytes=1024**3, swap_bytes=0,
        deadline_seconds=600, operations=20000000000, output_bytes=64*1024**2)
    SQ4_SCRATCH_CAP = 4*1024**3
    WALL, COMPUTE_CAP = 1500, .25
    FIXED = dict(FIXED, machine_limit_seconds=WALL, compute_cap_usd=COMPUTE_CAP, native_caps=CAPS)
    NATIVE_COMMIT, SOURCE_ID = '', ''  # Completed root qualification supplies executable pins.
    INPUT_PINS = tuple((d['destination'],d['bytes'],d['sha256']) for d in PQ_TRANSPORT['inputs'])
    require(len(INPUT_PINS) == 11 and sum(p[1] for p in INPUT_PINS) == 172419557, 'PQ exact eleven opaque inputs')
    SQ4_CLI = 'check-pq-residual-source'
    SQ4_OUTPUTS = ('screen/report.json', *(f'screen/report.pq-residual-{i}-{suffix}' for i in range(2)
        for suffix in ('book.bin','groups.bin','sq8.bin','cohort.bin','root.json')),
        *(f'screen/report.pq-residual-{suffix}.json' for suffix in ('selections','anchors','results','freeze')))
    ARTIFACTS = (*ARTIFACTS[:12], 'scratch.json', *SQ4_OUTPUTS,
        *('qualification/'+n for n in SQ4_RECEIPTS))
    ROSTER_SHA = sha(json.dumps(ARTIFACTS,separators=(',',':')).encode())


def validate_pq_residual_config(config, base=None):
    global FIXED, NATIVE_COMMIT, SOURCE_ID
    require(set(config) == {'schema','authority_pending','fixed','native_config','native_config_sha256',
        'binary','inputs','native_source','native_qualification','code_sha256','source_archive_paths',
        'source_archive_paths_sha256'} and config['schema'] == SCHEMA and config['authority_pending'] is False,
        'PQ frozen root authority; pending qualification cannot launch')
    fixed = config['fixed']
    require(set(fixed) == set(FIXED)|{'scratch'} and all(encoded(fixed[k]) == encoded(FIXED[k])
        for k in FIXED if k != 'scratch'), 'PQ CPU1/1GiB/noSwap/600s; machine1500s/.25+.15/Spot.60')
    authority = config['native_source']
    require(set(authority) == {'commit','full_source_identity_sha256','source_identity_sha256','source_sha256',
        'qualification_protocol_sha256'} and authority['commit'] == PQ_MANIFEST['native_source_commit']
        and authority['full_source_identity_sha256'] == PQ_MANIFEST['source_identity_sha256']
        and authority['source_identity_sha256'] == PQ_CONTRACT['source_identity_sha256']
        and authority['source_identity_sha256'] != authority['full_source_identity_sha256']
        and authority['source_sha256'] == PQ_CONTRACT['owned_source_sha256']
        and authority['qualification_protocol_sha256'] == sha(encoded(PQ_PROTOCOL)), 'PQ exact full406/subset source authority')
    native = config['native_config']
    require(encoded(native) == encoded(PQ_DRAFT) and encoded(native['caps']) == encoded(CAPS)
        and native['source_identity_sha256'] == authority['source_identity_sha256']
        and sha(encoded(native)) == config['native_config_sha256'], 'PQ unchanged strict native config/root paths')
    require(encoded(config['inputs']) == encoded(PQ_TRANSPORT['inputs']), 'PQ exact eleven opaque transports; no graph/requests/GT/prefix')
    binary = config['binary']
    require(set(binary) == {'key','bytes','sha256'} and body_pin({k:binary[k] for k in ('bytes','sha256')})['bytes'] > 0,
        'PQ completed root binary pin required')
    object_key(binary['key'])
    receipts = config['native_qualification']
    require(len(receipts) == 5 and tuple(Path(r['path']).name for r in receipts) == SQ4_RECEIPTS, 'PQ five completed root receipts')
    for r in receipts:
        require(set(r) == {'path','bytes','sha256'} and r['bytes'] > 0, 'PQ qualification descriptor')
        object_key(r['path']); body_pin({k:r[k] for k in ('bytes','sha256')})
    paths = sorted([str(CONFIG),*CODE,*(str(p) for p in PQ_EVIDENCE),*(r['path'] for r in receipts)])
    require(config['source_archive_paths'] == paths and config['source_archive_paths_sha256'] == sha(
        json.dumps(paths,separators=(',',':')).encode()) and set(config['code_sha256']) == set(CODE), 'PQ minimal source archive/CODE')
    for digest in config['code_sha256'].values():
        body_pin(dict(bytes=0,sha256=digest))
    scratch = fixed['scratch']
    require(set(scratch) == {'input_bytes','native_output_bytes','binary_bytes','source_archive_bytes',
        'bootstrap_bytes','auxiliary_bytes','cap_bytes'} and all(type(n) is int and n > 0 for n in scratch.values())
        and scratch['input_bytes'] == sum(p[1] for p in INPUT_PINS)
        and scratch['native_output_bytes'] == CAPS['output_bytes'] and scratch['binary_bytes'] == binary['bytes']
        and sum(n for k,n in scratch.items() if k != 'cap_bytes') <= scratch['cap_bytes'] == SQ4_SCRATCH_CAP,
        'PQ whole-worker scratch including source/archive/venv/input/binary/output')
    if base is not None:
        for name,digest in config['code_sha256'].items():
            require(file_pin(Path(base)/name)['sha256'] == digest, 'PQ CODE drift: '+name)
        for name,identity in PQ_EVIDENCE_PINS.items():
            require(file_pin(Path(base)/name) == identity, 'PQ evidence drift: '+name)
        sq4_qualification(config,base)
    FIXED = fixed
    NATIVE_COMMIT, SOURCE_ID = authority['commit'],authority['source_identity_sha256']
    return native


def configure_canary():
    """Reuse the corrected lifecycle with an independent, metadata-only roster."""
    global CANARY, ROOT, CONFIG, SCHEMA, PREFIX, TOKEN_PREFIX, TAG, REMOTE_ROOT
    global CAPS, FIXED, WALL, COMPUTE_CAP, SQ4_SCRATCH_CAP, SQ4_OUTPUTS, ARTIFACTS, ROSTER_SHA
    global CORRECTED_SUPERVISOR_SECONDS
    require(CORRECTED_FOUR_BIT and not CANARY, 'canary requires corrected mode once')
    SCIENCE_STATE.update({n:globals()[n] for n in ('ROOT','CONFIG','SCHEMA','CAPS','FIXED','WALL',
        'COMPUTE_CAP','SPOT_MAX_USD_PER_HOUR','NATIVE_COMMIT','SOURCE_ID','SQ4_SCRATCH_CAP',
        'ARTIFACTS','ROSTER_SHA','PREFIX','TOKEN_PREFIX','TAG','REMOTE_ROOT','CORRECTED_SUPERVISOR_SECONDS','SQ4_OUTPUTS')})
    CANARY = True
    ROOT = CORRECTED_ROOT/'infrastructure-canary'; CONFIG = ROOT/'config.json'
    SCHEMA = 'borsuk-corrected-four-bit-infrastructure-canary-v1'
    PREFIX = 'research/hierarchical-cells/20261005/corrected-four-bit-canary-'
    TOKEN_PREFIX, TAG = 'corrected-four-bit-canary-', 'borsuk-corrected-four-bit-canary'
    REMOTE_ROOT = Path('/mnt/corrected-four-bit-canary')
    WALL, COMPUTE_CAP, CORRECTED_SUPERVISOR_SECONDS = 480, .12, 120
    SQ4_SCRATCH_CAP = 4*1024**3
    CAPS = dict(cpu_threads=1, memory_bytes=256*1024**2, swap_bytes=0,
        deadline_seconds=120, operations=128, output_bytes=2*1024**2)
    FIXED = dict(FIXED, machine_limit_seconds=WALL, compute_cap_usd=COMPUTE_CAP,
        ebs_s3_allowance_usd=.15, native_caps=CAPS)
    SQ4_OUTPUTS = ()
    ARTIFACTS = ('config.json','science-config.json','native-config.json','source-qualification.json',
        'imports.json','sentinels.json','stage-receipt.json','native-exit.json','resources.json','cleanup.json',
        'scratch.json','cpu.txt','runtime-abi.json','run-closed.log','native.log',BINARY_NAME,
        'metadata/closed-populations.json',*(f'qualification/{n}' for n in SQ4_RECEIPTS))
    ROSTER_SHA = sha(json.dumps(ARTIFACTS,separators=(',',':')).encode())


def science_binding(config):
    """Pending metadata admission may freeze the same eventual science authorities."""
    return {k:config[k] for k in ('fixed','native_config','native_config_sha256','binary','inputs',
        'native_source','native_qualification','code_sha256')}


def validate_canary_config(config, base=None):
    global NATIVE_COMMIT, SOURCE_ID
    require(config['schema'] == SCHEMA and config['authority_pending'] is False
        and set(config) == set(config['canary']['science_config'])|{'canary'}, 'frozen canary authority')
    admission = config['canary']; science = admission['science_config']
    require(set(admission) == {'science_authority','science_config','metadata_only_admission'}
        and type(admission['metadata_only_admission']) is bool, 'explicit metadata-only admission')
    ref = admission['science_authority']
    require(set(ref) == {'path','bytes','sha256'} and ref['bytes'] > 0, 'science authority reference')
    object_key(ref['path']); body_pin({k:ref[k] for k in ('bytes','sha256')})
    require(ref['path'] in (str(SCIENCE_STATE['CONFIG']),str(SCIENCE_STATE['ROOT']/'config-draft.json'))
        and (science['authority_pending'] is False or admission['metadata_only_admission'] is True),
        'pending science needs explicit canary metadata-only admission')
    checked = dict(science, authority_pending=False)
    require('canary_admission' not in checked, 'canary precedes science admission')
    saved = {n:globals()[n] for n in SCIENCE_STATE}
    try:
        globals().update(SCIENCE_STATE)
        validate_sq4_config(checked, base)
    finally:
        globals().update(saved)
    for k,v in science.items():
        if k not in ('schema','authority_pending','fixed','source_archive_paths','source_archive_paths_sha256'):
            require(config[k] == v, 'canary science authority drift: '+k)
    fixed = config['fixed']; scratch = fixed['scratch']
    require(set(fixed) == set(FIXED)|{'scratch'} and all(encoded(fixed[k]) == encoded(v) for k,v in FIXED.items()),
        'canary CPU1/256MiB/swap0/480s/128/.12+.15 caps')
    require(set(scratch) == {'input_bytes','native_output_bytes','binary_bytes','source_archive_bytes',
        'bootstrap_bytes','auxiliary_bytes','cap_bytes'} and all(type(n) is int and n > 0 for n in scratch.values())
        and scratch['input_bytes'] == 175929 and scratch['binary_bytes'] == CANARY_BINARY_PIN['bytes']
        and scratch['native_output_bytes'] == CAPS['output_bytes']
        and sum(n for k,n in scratch.items() if k!='cap_bytes') <= scratch['cap_bytes'] == SQ4_SCRATCH_CAP,
        'canary whole scratch admission')
    require({k:config['binary'][k] for k in ('bytes','sha256')} == CANARY_BINARY_PIN
        and config['binary']['bytes'] <= 16*1024**2, 'canary exact qualified usage executable')
    paths = sorted([str(CONFIG),ref['path'],str(SQ4_ROOT/'prospective-input-roster.json'),*CODE,
        *(r['path'] for r in config['native_qualification']),*(str(CORRECTED_ROOT/n) for n,_,_ in CORRECTED_EVIDENCE)])
    require(config['source_archive_paths'] == paths and config['source_archive_paths_sha256'] == sha(
        json.dumps(paths,separators=(',',':')).encode()), 'canary exact archive roster')
    if base is not None:
        raw = read(Path(base)/ref['path'])
        require(pin(raw) == {k:ref[k] for k in ('bytes','sha256')} and decode(raw) == science,
            'canary original science authority bytes')
    globals()['FIXED'] = fixed
    NATIVE_COMMIT, SOURCE_ID = science['native_source']['commit'],science['native_source']['source_identity_sha256']
    return config['native_config']


def validate_canary_admission(config, base, collected=False):
    """A root-pinned receipt is emitted only after full GO replay and termination."""
    ref = config.get('canary_admission', {})
    require(set(ref) == {'path','bytes','sha256'} and ref['bytes'] > 0, 'science requires terminated GO canary admission')
    object_key(ref['path']); body_pin({k:ref[k] for k in ('bytes','sha256')})
    raw = read(Path(base)/('canary-admission.json' if collected else ref['path']))
    require(pin(raw) == {k:ref[k] for k in ('bytes','sha256')}, 'science canary admission drift')
    receipt = decode(raw)
    require(receipt['schema'] == 'borsuk-corrected-four-bit-canary-admission-v1'
        and receipt['status'] == 'GO' and receipt['binding'] == science_binding(config)
        and receipt['original_exit_code'] == 2 and receipt['closeout']['state'] == 'terminated'
        and receipt['closeout']['nodes'] == {'0':{'instance_id':receipt['instance_id']}}
        and receipt['canary_schema'] == 'borsuk-corrected-four-bit-infrastructure-canary-v1'
        and re.fullmatch(r'research/hierarchical-cells/20261005/corrected-four-bit-canary-a[0-9]{4}',receipt['prefix'])
        and re.fullmatch('[0-9a-f]{40}',receipt['source_commit']), 'unchanged science/native/CODE/refs and terminated GO')
    for k in ('terminal','reservation','launch','config','source_qualification'):
        body_pin(receipt[k]); require(receipt[k]['bytes'] > 0, 'canary original receipt pin')
    body_pin(dict(bytes=0,sha256=receipt['source_archive_sha256']))
    return receipt


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)+'\n').encode()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def decode(body):
    def pairs(rows):
        result = {}
        for key, value in rows:
            require(key not in result, 'duplicate JSON key: '+key)
            result[key] = value
        return result
    return json.loads(body, object_pairs_hook=pairs,
                      parse_constant=lambda n: require(False, 'invalid JSON number: '+n))


def pin(body):
    return dict(bytes=len(body), sha256=sha(body))


def regular(path):
    path = Path(path).absolute()
    require(path.resolve() == path, 'symlink or noncanonical path: '+str(path))
    fd = os.open(path, os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
    try:
        require(stat.S_ISREG(os.fstat(fd).st_mode), 'regular file required: '+str(path))
        return os.fdopen(fd, 'rb')
    except BaseException:
        os.close(fd)
        raise


def file_pin(path):
    digest, size = hashlib.sha256(), 0
    with regular(path) as source:
        for chunk in iter(lambda: source.read(65536), b''):
            digest.update(chunk); size += len(chunk)
    return dict(bytes=size, sha256=digest.hexdigest())


def read(path, limit=4*1024**2):
    with regular(path) as source:
        body = source.read(limit+1)
    require(len(body) <= limit, 'file cap: '+str(path))
    return body


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def write(path, body):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    require(path.absolute().resolve() == path.absolute(), 'write path symlink')
    with path.open('xb') as output:
        output.write(body); output.flush(); os.fsync(output.fileno())
    sync_directory(path.parent)


def body_pin(value, max_bytes=64*1024**2):
    require(type(value) is dict and set(value) == {'bytes', 'sha256'}, 'artifact identity fields')
    require(type(value['bytes']) is int and 0 <= value['bytes'] <= max_bytes, 'artifact byte cap')
    require(type(value['sha256']) is str and re.fullmatch('[0-9a-f]{64}', value['sha256']), 'artifact digest')
    return value


def object_key(key):
    require(type(key) is str and re.fullmatch(r'[A-Za-z0-9_./-]{1,1024}', key)
            and not key.startswith('/') and all(p not in ('', '.', '..') for p in key.split('/')), 'object key')


def native_inputs(native):
    if SQ4:
        return [a for p in native['panels'] for a in (p['root'], p['requests'], p['truth'])]+[native['original_seal'], native['prefix']]+([native['closed_populations']] if CORRECTED_FOUR_BIT else [])
    return [a for p in native['panels'] for a in (p['root'], p['graph'])]+[native['original_seal'], native['prefix']]


def sq4_qualification(config, base, collected=False):
    if CO_SELECTION_LAYOUT:
        return co_selection_qualification(config,base,collected)
    receipts = {}
    identities = {}
    for r in config['native_qualification']:
        name = Path(r['path']).name
        path = Path(base)/('qualification/'+name if collected else r['path'])
        require(file_pin(path) == {k:r[k] for k in ('bytes', 'sha256')}, 'SQ4 qualification drift: '+name)
        receipts[name] = decode(read(path))
        identities[name] = {k:r[k] for k in ('bytes','sha256')}
    v, q, w, t, close = (receipts[n] for n in SQ4_RECEIPTS)
    binary = {k:config['binary'][k] for k in ('bytes', 'sha256')}
    sources = q['source_sha256']
    source_id = sha(json.dumps(sources, sort_keys=True, separators=(',', ':')).encode())
    authority = config['native_source']
    count = 406 if PQ_RESIDUAL_SOURCE else 405 if CORRECTED_FOUR_BIT else 404
    require(source_id == authority['full_source_identity_sha256'] and len(sources) == q['source_file_count'] == w['source_file_count'] == v['source_file_count'] == count
            and all(sources.get(n) == h for n,h in authority['source_sha256'].items())
            and w['source_sha256'] == sources, 'SQ4 full404 source proof')
    if CORRECTED_FOUR_BIT:
        require(sources == CORRECTED_SOURCE['source_sha256'] and source_id == CORRECTED_SOURCE['source_identity_sha256']
                and authority['commit'] == CORRECTED_COMMIT, 'corrected exact full405 source proof')
        require(q['schema'] == 'borsuk-corrected-four-bit-implementation-gates-qualification-v1'
                and w['schema'] == 'borsuk-corrected-four-bit-implementation-gates-receipt-v1'
                and t['schema'] == 'borsuk-corrected-four-bit-implementation-gates-spot-v1'
                and t['source_file_count'] == 405, 'corrected completed qualification schemas')
        before = body_pin(w['artifacts']['source-before.json'])
        require(before['bytes'] > 0 and before == w['artifacts']['source-after.json']
                == t['artifacts']['source-before.json'] == t['artifacts']['source-after.json']
                and w['artifacts'][BINARY_NAME] == binary, 'corrected original unchanged source/binary artifacts')
    if PQ_RESIDUAL_SOURCE:
        require(sources == PQ_MANIFEST['source_sha256'] and source_id == PQ_MANIFEST['source_identity_sha256']
            and q['schema'] == 'borsuk-pq-residual-implementation-gates-qualification-v1'
            and w['schema'] == 'borsuk-pq-residual-implementation-gates-receipt-v1'
            and t['schema'] == 'borsuk-pq-residual-implementation-gates-spot-v1'
            and t['source_file_count'] == 406 and v['qualified'] is True, 'PQ completed full406 root proof')
        before = body_pin(w['artifacts']['source-before.json'])
        require(before['bytes'] > 0 and before == w['artifacts']['source-after.json']
            == t['artifacts']['source-before.json'] == t['artifacts']['source-after.json']
            and w['artifacts'][BINARY_NAME] == binary, 'PQ unchanged source and compiled binary')
        require(v['stages'] == w['stages'] and q['mandatory_tests'] == PQ_PROTOCOL['mandatory_tests']
            and [(s['stage'],s['command']) for s in w['stages']] == PQ_PROTOCOL['stages'], 'PQ exact all19 stages/eleven+doc mandatory names')
        require(q['native_source_manifest'] == dict(path=str(PQ_EVIDENCE[1]),**PQ_EVIDENCE_PINS[str(PQ_EVIDENCE[1])])
            and q['native_source_manifest_sha256'] == PQ_EVIDENCE_PINS[str(PQ_EVIDENCE[1])]['sha256'], 'PQ qualified exact manifest')
    for value in (v, q, t):
        require(value['native_source_commit'] == authority['commit'], 'SQ4 exact native revision')
    require(all(value['source_identity_sha256'] == source_id for value in (v,q,w,t)), 'SQ4 full404 identity')
    require(q['mandatory_test_names_pending'] is w['mandatory_test_names_pending'] is False
            and w['qualified'] is w['command_started'] is w['command_completed'] is w['source_unchanged'] is True
            and type(w['exit_status']) is int and w['exit_status'] == w['gate_status'] == 0
            and w['qualification_sha256'] == identities['source-qualification.json']['sha256'], 'SQ4 completed qualification')
    stages = w['stages']
    required = q['mandatory_tests']
    protocol_sha = authority['qualification_protocol_sha256'] if PQ_RESIDUAL_SOURCE or HISTOGRAM_SQ4 or CORRECTED_FOUR_BIT else SQ4_QUALIFICATION_PROTOCOL_SHA
    require(stages == v['stages'] and w['mandatory_tests'] == required
            and sha(encoded(dict(stages=[(s['stage'],s['command']) for s in stages],
                                 mandatory_tests=required))) == protocol_sha, 'SQ4 exact all14 commands and mandatory roster')
    if HISTOGRAM_SQ4:
        require(protocol_sha == HISTOGRAM_QUALIFICATION_PROTOCOL_SHA and len(stages) == 14
                and v['qualified'] is True, 'histogram completed native protocol')
    if CORRECTED_FOUR_BIT:
        require(protocol_sha == CORRECTED_PROTOCOL_SHA and len(stages) == 15 and v['qualified'] is True,
                'corrected completed all15 native protocol')
    previous_finish = None
    for stage in stages:
        started, finished = (datetime.fromisoformat(stage[k]) for k in ('started_at','finished_at'))
        require(started.utcoffset() == finished.utcoffset() == timedelta(0)
                and finished >= started and (previous_finish is None or started >= previous_finish), 'SQ4 serial completed UTC stages')
        previous_finish = finished
        require(all(
            type(stage[k]) is int and stage[k] == 0 for k in ('exit_status','gate_status','log_exit_status')), 'SQ4 original gate exit')
        mandatory = required.get(stage['stage'], ())
        require((type(stage['tests_run']) is int and stage['tests_run'] >= len(mandatory) > 0)
                if mandatory else stage['tests_run'] is None, 'SQ4 positive counts only on test stages')
        require(stage['required_test_passes'] == {n:1 for n in mandatory}
                and all(type(n) is int for n in stage['required_test_passes'].values()), 'SQ4 exact owning-stage mandatory passes')
    require(v['source_before_equals_after'] is v['all_source_blobs_independently_matched'] is True
            and v['artifact_hashes_independently_verified'] is v['descendants_drained'] is True
            and v['original_controller_exit_status'] == v['swap_peak_bytes'] == v['oom'] == 0
            and v['binary'] == binary and v['instance_closeout'] == close, 'SQ4 parent source/resource proof')
    require(t['status'] == t['phase'] == 'complete' and t['exit_code'] == t['original_exit_code'] == 0
            and close['state'] == 'terminated' and close['nodes'] == {'0':{'instance_id':t['instance_id']}}
            and t['source_qualification_sha256'] == identities['source-qualification.json']['sha256']
            and t['artifacts']['workspace-receipt.json'] == identities['workspace-receipt.json']
            and t['native_source_manifest_sha256'] == q['native_source_manifest_sha256'] == q['native_source_manifest']['sha256']
            and t['artifacts'][BINARY_NAME] == binary, 'SQ4 qualified original terminal/binary/termination')
    return receipts


def validate_sq4_config(config, base=None):
    global FIXED, WALL, COMPUTE_CAP, SPOT_MAX_USD_PER_HOUR, NATIVE_COMMIT, SOURCE_ID
    require(set(config) == {'schema','authority_pending','fixed','native_config','native_config_sha256',
        'binary','inputs','native_source','native_qualification','code_sha256','source_archive_paths','source_archive_paths_sha256'}
        | ({'canary_admission'} if CORRECTED_FOUR_BIT and 'canary_admission' in config else set())
        and config['schema'] == SCHEMA and config['authority_pending'] is False, 'SQ4 frozen root authority')
    fixed = config['fixed']
    require(set(fixed) == set(FIXED)|{'scratch'} and encoded(fixed['native_caps']) == encoded(CAPS)
            and all(encoded(fixed[k]) == encoded(FIXED[k]) for k in FIXED if k not in (
                'machine_limit_seconds','compute_cap_usd','spot_max_usd_per_hour','native_caps','scratch')), 'SQ4 fixed method/host')
    wall, cost, spot = (fixed[k] for k in ('machine_limit_seconds','compute_cap_usd','spot_max_usd_per_hour'))
    require(type(wall) is int and wall == (3600 if CORRECTED_FOUR_BIT else 1800) and type(spot) in (int,float) and spot == .60
            and type(cost) in (int,float) and cost == (.60 if CORRECTED_FOUR_BIT else .30), 'SQ4 root wall/cost/Spot cap')
    native = config['native_config']
    authority = config['native_source']
    require(set(authority) == {'commit','full_source_identity_sha256','source_identity_sha256','source_sha256'}
                | ({'qualification_protocol_sha256'} if HISTOGRAM_SQ4 or CORRECTED_FOUR_BIT else set())
            and re.fullmatch('[0-9a-f]{40}', authority['commit'])
            and set(authority['source_sha256']) == set(SQ4_SOURCES), 'SQ4 exact root source pins')
    for digest in (authority['full_source_identity_sha256'],authority['source_identity_sha256'],*authority['source_sha256'].values()):
        body_pin(dict(bytes=0,sha256=digest))
    if HISTOGRAM_SQ4:
        require(authority['qualification_protocol_sha256'] == HISTOGRAM_QUALIFICATION_PROTOCOL_SHA, 'histogram root native protocol pin')
    if CORRECTED_FOUR_BIT:
        require(authority['qualification_protocol_sha256'] == CORRECTED_PROTOCOL_SHA
                and authority['commit'] == CORRECTED_COMMIT and authority['source_sha256'] == SQ4_SOURCES
                and authority['source_identity_sha256'] == CORRECTED_DIAGNOSTIC_SOURCE_ID
                and authority['full_source_identity_sha256'] == CORRECTED_SOURCE['source_identity_sha256'], 'corrected exact qualified source pins')
    require(set(native) == {'schema','source_identity_sha256','caps','panels','original_seal','prefix'}
                | ({'rotation_seed','construction_operations','query_auth_operations','closed_populations'} if CORRECTED_FOUR_BIT else set())
            and native['schema'] == SQ4_SCHEMA+'-config-v1' and encoded(native['caps']) == encoded(CAPS)
            and native['source_identity_sha256'] == authority['source_identity_sha256'] and len(native['panels']) == 2
            and sha(encoded(native)) == config['native_config_sha256'], 'SQ4 exact native config')
    for p,dataset in zip(native['panels'], ('relaion','cohere')):
        require(set(p) == {'dataset','root','requests','truth','truth_width'} and p['dataset'] == dataset
                and type(p['truth_width']) is int and p['truth_width'] == 100, 'SQ4 native panel')
    expected = [dict(path=p,bytes=n,sha256=h) for p,n,h in INPUT_PINS]
    input_count = 19 if CORRECTED_FOUR_BIT else 18
    require(encoded(native_inputs(native)) == encoded([expected[i] for i in (0,6,7,8,14,15,16,17)]+([expected[18]] if CORRECTED_FOUR_BIT else []))
            and len(config['inputs']) == len(INPUT_PINS) == input_count, 'SQ4 retained original descriptors')
    if CORRECTED_FOUR_BIT:
        require(type(native['rotation_seed']) is list and len(native['rotation_seed']) == 32
                and all(type(v) is int and 0 <= v <= 255 for v in native['rotation_seed']), 'corrected mandatory seed32')
        require(type(native['construction_operations']) is int and 159001952256 <= native['construction_operations'] < 2**64
                and type(native['query_auth_operations']) is int and corrected_query_floor(CAPS['output_bytes']) <= native['query_auth_operations'] < 2**64,
                'corrected explicit separate construction/query-auth admission')
    for d,(path,size,digest) in zip(config['inputs'], INPUT_PINS):
        require(set(d) == {'destination','key','bytes','sha256'} and d['destination'] == path
                and body_pin({k:d[k] for k in ('bytes','sha256')}, max_bytes=size) == dict(bytes=size,sha256=digest), 'SQ4 opaque input transport')
        object_key(d['key'])
    require(len({d['key'] for d in config['inputs']}) == input_count, 'SQ4 distinct input objects')
    require(set(config['binary']) == {'key','bytes','sha256'} and body_pin({k:config['binary'][k] for k in ('bytes','sha256')})['bytes'] > 0, 'SQ4 root binary pin')
    object_key(config['binary']['key'])
    receipts = config['native_qualification']
    require(len(receipts) == 5 and tuple(Path(r['path']).name for r in receipts) == SQ4_RECEIPTS, 'SQ4 five completed qualification receipts')
    for r in receipts:
        require(set(r) == {'path','bytes','sha256'} and r['bytes'] > 0, 'SQ4 qualification descriptor')
        object_key(r['path']); body_pin({k:r[k] for k in ('bytes','sha256')})
    paths = sorted([str(CONFIG), str(SQ4_ROOT/'prospective-input-roster.json'), *CODE, *(r['path'] for r in receipts),
                    *(str(CORRECTED_ROOT/n) for n,_,_ in CORRECTED_EVIDENCE if CORRECTED_FOUR_BIT),
                    *([config['canary_admission']['path']] if CORRECTED_FOUR_BIT and 'canary_admission' in config else [])])
    require(config['source_archive_paths'] == paths and config['source_archive_paths_sha256'] == sha(
        json.dumps(paths,separators=(',',':')).encode()) and set(config['code_sha256']) == set(CODE), 'SQ4 minimal source archive')
    for name,digest in config['code_sha256'].items():
        body_pin(dict(bytes=0,sha256=digest))
        if base is not None:
            require(file_pin(Path(base)/name)['sha256'] == digest, 'SQ4 code drift: '+name)
    if CORRECTED_FOUR_BIT and base is not None:
        for name,size,digest in CORRECTED_EVIDENCE:
            require(file_pin(Path(base)/CORRECTED_ROOT/name) == dict(bytes=size,sha256=digest), 'corrected archive evidence drift')
    scratch = fixed['scratch']
    require(set(scratch) == {'input_bytes','native_output_bytes','binary_bytes','source_archive_bytes',
        'bootstrap_bytes','auxiliary_bytes','cap_bytes'} and all(type(n) is int and n > 0 for n in scratch.values())
        and scratch['input_bytes'] == sum(p[1] for p in INPUT_PINS)
        and scratch['native_output_bytes'] == CAPS['output_bytes'] and scratch['binary_bytes'] == config['binary']['bytes']
        and sum(n for k,n in scratch.items() if k != 'cap_bytes') <= scratch['cap_bytes'] == SQ4_SCRATCH_CAP, 'SQ4 whole scratch coexistence charge')
    if base is not None:
        sq4_qualification(config, base)
    FIXED = fixed
    WALL, COMPUTE_CAP, SPOT_MAX_USD_PER_HOUR = wall, cost, spot
    NATIVE_COMMIT, SOURCE_ID = authority['commit'], authority['source_identity_sha256']
    return native


def corrected_query_floor(output_bytes):
    """Metadata-only prospective allowance; native admits its exact full bound."""
    rows = sum(q['fetched_rows'] for p in CORRECTED_POPULATIONS['panels'] for q in p['queries'])
    source_bytes = sum(INPUT_PINS[i][1] for i in (*range(6), *range(8,14)))
    metadata_bytes = sum(INPUT_PINS[i][1] for i in (6,7,14,15,16,17,18))
    return 43710342688+8*rows+3*source_bytes+3*output_bytes+metadata_bytes


def scratch_observation(root):
    """Charge apparent or allocated bytes, including temporary files and symlinks."""
    sizes = {}
    for tree in (Path(root), SQ4_INPUT_ROOT):
        total = 0
        if tree.exists():
            for directory, names, files in os.walk(tree, followlinks=False):
                for path in (Path(directory), *(Path(directory)/n for n in files),
                             *(Path(directory)/n for n in names if (Path(directory)/n).is_symlink())):
                    try:
                        s = path.lstat()
                    except FileNotFoundError:
                        continue  # A removed temporary file no longer coexists.
                    total += max(s.st_size, s.st_blocks*512)
        sizes[str(tree)] = total
    return dict(roots=sizes, whole_scratch_bytes=sum(sizes.values()))


def scratch_room(root, growth):
    observation = scratch_observation(root)
    require(observation['whole_scratch_bytes']+growth <= SQ4_SCRATCH_CAP, 'SQ4 scratch before closure write')
    return observation


def validate_config(config, base=None):
    if CO_SELECTION_LAYOUT:
        return validate_co_selection_config(config,base)
    if PQ_RESIDUAL_SOURCE:
        return validate_pq_residual_config(config, base)
    if CANARY:
        return validate_canary_config(config, base)
    if SQ4:
        return validate_sq4_config(config, base)
    require(set(config) == {'schema', 'authority_pending', 'fixed', 'native_config',
        'native_config_sha256', 'binary', 'inputs', 'native_qualification', 'code_sha256',
        'source_archive_paths', 'source_archive_paths_sha256'}, 'frozen config fields')
    require(config['schema'] == SCHEMA and config['authority_pending'] is False, 'root must freeze authority')
    require(encoded(config['fixed']) == encoded(FIXED), 'fixed resource/method contract')
    native = config['native_config']
    require(set(native) == {'schema', 'caps', 'panels', 'original_seal', 'prefix'}
            and native['schema'] == 'borsuk-fine-pack-config-v1'
            and encoded(native['caps']) == encoded(CAPS), 'native config contract')
    require(len(native['panels']) == 2, 'two panels')
    for p, dataset in zip(native['panels'], ('relaion', 'cohere')):
        require(set(p) == {'dataset', 'identity', 'root', 'graph'} and p['dataset'] == dataset, 'native panel')
        identity = p['identity']
        require(set(identity) == {'rows', 'dimensions', 'generation', 'source', 'layout', 'pq'}
                and type(identity['rows']) is int and identity['rows'] == 100000
                and type(identity['dimensions']) is int and identity['dimensions'] == 768
                and type(identity['generation']) is int and 0 <= identity['generation'] < 2**64, 'panel geometry')
        for name in ('source', 'layout', 'pq'):
            require(type(identity[name]) is list and len(identity[name]) == 32
                    and all(type(v) is int and 0 <= v <= 255 for v in identity[name]), 'layout identity')
    require(sha(encoded(native)) == config['native_config_sha256'], 'native config digest')
    require(len(config['inputs']) == 6, 'six input bodies only')
    for descriptor, transport, (path, size, digest) in zip(native_inputs(native), config['inputs'], INPUT_PINS):
        require(descriptor == dict(path=path, bytes=size, sha256=digest), 'historical input pin')
        require(set(transport) == {'destination', 'key', 'bytes', 'sha256'}
                and transport['destination'] == path and body_pin({k:transport[k] for k in ('bytes', 'sha256')})
                == dict(bytes=size, sha256=digest), 'exact native input transport')
        object_key(transport['key'])
    require(len({i['key'] for i in config['inputs']}) == 6, 'distinct input objects')
    require(config['binary'] == dict(BINARY_PIN, key=BINARY_KEY), 'qualified executable pin')
    require(config['native_qualification'] == list(RECEIPTS), 'qualified original receipts')
    require(set(config['code_sha256']) == set(CODE), 'code closure')
    for name, digest in config['code_sha256'].items():
        body_pin(dict(bytes=0, sha256=digest))
        if base is not None:
            require(file_pin(Path(base)/name)['sha256'] == digest, 'code drift: '+name)
    # Root fills this roster after integration. Neither config nor archive hash
    # is embedded in itself; qualification and terminal bind their actual bytes.
    paths = sorted([str(CONFIG), *CODE, *(r['path'] for r in RECEIPTS)])
    require(config['source_archive_paths'] == paths
            and config['source_archive_paths_sha256'] == sha(json.dumps(paths, separators=(',', ':')).encode()), 'minimal archive closure')
    if base is not None:
        for receipt in RECEIPTS:
            require(file_pin(Path(base)/receipt['path']) == {k:receipt[k] for k in ('bytes', 'sha256')}, 'qualification drift')
    return native


def preflight(base=Path('.')):
    raw = read(base/CONFIG)
    config = decode(raw)
    validate_config(config, base)
    if CORRECTED_FOUR_BIT and not CANARY:
        validate_canary_admission(config, base)
    proof = dict(config_path=str(CONFIG), config_sha256=sha(raw), campaign_schema=SCHEMA,
                artifact_roster_sha256=ROSTER_SHA, native_config_sha256=config['native_config_sha256'],
                binary=config['binary'], native_source_commit=NATIVE_COMMIT, source_identity_sha256=SOURCE_ID,
                code_sha256=config['code_sha256'], native_qualification=config['native_qualification'],
                source_archive_paths=config['source_archive_paths'],
                source_archive_paths_sha256=config['source_archive_paths_sha256'])
    if SQ4:
        proof['scratch'] = config['fixed']['scratch']
    if CANARY:
        proof['canary'] = config['canary']
        proof['science_binding'] = science_binding(config['canary']['science_config'])
    elif CORRECTED_FOUR_BIT:
        proof['canary_admission'] = config['canary_admission']
    return proof


def lifecycle():
    from scripts import launch_native_metadata_ranges_cold_spot as shared
    return shared


def poll(ec2, s3, prefix, instance_id, started):
    shared = lifecycle()
    original = shared.WALL
    try:
        shared.WALL = WALL
        shared.poll(ec2, s3, prefix, instance_id, started)
    finally:
        shared.WALL = original


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    require(re.fullmatch('[0-9a-f]{40}', commit) and re.fullmatch('[0-9a-f]{64}', archive_sha), 'source archive pins')
    require(re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix), 'attempt prefix')
    object_key(archive_key)
    require(qualification['config_path'] == str(CONFIG), 'bootstrap config path')
    bootstrap_setup, bootstrap_stop, apt_options, supervisor_options, archive_write_guard, archive_extract_guard = '', '', '', '', '', ''
    if SQ4:
        import inspect
        reserve = qualification['scratch']
        require(reserve == FIXED['scratch'], 'SQ4 bootstrap frozen scratch admission')
        # One bounded observer uses the same scan as runtime. All owned package
        # caches, lists, logs, wheels, bytecode and process temporaries coexist here.
        bootstrap_setup = f'''mkdir -p "$root/bootstrap/tmp" "$root/bootstrap/archives/partial" "$root/bootstrap/lists/partial" "$root/bootstrap/log"
export TMPDIR="$root/bootstrap/tmp" TMP="$root/bootstrap/tmp" TEMP="$root/bootstrap/tmp" PIP_CACHE_DIR="$root/bootstrap/cache" PYTHONPYCACHEPREFIX="$root/bootstrap/pycache"
cat >"$root/bootstrap/watch.py" <<'WATCH'
import json, os, signal, sys, time
from pathlib import Path
root=Path({str(REMOTE_ROOT)!r}); SQ4_INPUT_ROOT=Path({str(SQ4_INPUT_ROOT)!r})
{inspect.getsource(scratch_observation)}
reserve={reserve!r}
remaining=sum(reserve[k] for k in ('input_bytes','native_output_bytes','binary_bytes','auxiliary_bytes'))
receipt=dict(interval_seconds=1, sample_count=0, peak_bytes=0, closed=False, cap_exceeded=False, reserve=reserve)
running=True
def stop(*args):
    global running
    running=False
signal.signal(signal.SIGTERM,stop)
try:
    available=os.statvfs(root).f_bavail*os.statvfs(root).f_frsize
    assert available >= sum(v for k,v in reserve.items() if k!='cap_bytes'), 'bootstrap free scratch admission'
    while True:
        observation=scratch_observation(root)
        receipt.update(last=observation, sample_count=receipt['sample_count']+1, peak_bytes=max(receipt['peak_bytes'],observation['whole_scratch_bytes']))
        assert receipt['peak_bytes']+remaining <= reserve['cap_bytes'], 'bootstrap/input/output overlap cap'
        assert receipt['peak_bytes'] <= reserve['bootstrap_bytes']+reserve['source_archive_bytes'], 'bootstrap/source reserve'
        if receipt['sample_count']==1:(root/'bootstrap/ready').touch(exist_ok=False)
        if not running:break
        time.sleep(1)
    receipt['closed']=True
except BaseException:
    receipt['cap_exceeded']=True
    os.kill(int(sys.argv[1]),signal.SIGTERM)
    raise
finally:
    if scratch_observation(root)['whole_scratch_bytes']+65536 <= reserve['cap_bytes']:
        with (root/'bootstrap/scratch.json').open('x') as output:
            json.dump(receipt,output);output.flush();os.fsync(output.fileno())
WATCH
python3 "$root/bootstrap/watch.py" $$ &
watcher=$!
while ! test -f "$root/bootstrap/ready"; do kill -0 "$watcher"; sleep .05; done
'''
        bootstrap_stop = 'kill "$watcher"\nwait "$watcher"\n'
        apt_options = '-o Dir::Cache::archives="$root/bootstrap/archives" -o Dir::State::lists="$root/bootstrap/lists" -o Dir::Log="$root/bootstrap/log" -o APT::Sandbox::User=root '
        supervisor_options = "-p 'ReadOnlyPaths=/tmp /var/tmp' "
        archive_write_guard = f"assert output.tell()+len(chunk) <= {reserve['source_archive_bytes']}, 'source archive scratch reserve'; "
        archive_extract_guard = f"    assert Path('source.tar.gz').stat().st_size+sum(m.size for m in archive.getmembers())+4096*len(archive.getmembers()) <= {reserve['source_archive_bytes']}, 'source extraction overlap reserve'\n"
    # SDK bootstrap, IMDSv2, one request attempt, terminal last; no toolchain.
    body = f'''#!/bin/bash
set -euo pipefail
export AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_DEFAULT_REGION={REGION}
systemd-run --unit=fine-pack-stop --on-active={WALL}s /usr/sbin/shutdown -h now
root={REMOTE_ROOT}
mkdir "$root"
cd "$root"
trap '/usr/sbin/shutdown -h now || true' EXIT
exec >run.log 2>&1
test "$(uname -m)" = x86_64
{bootstrap_setup}apt-get {apt_options}update -qq
DEBIAN_FRONTEND=noninteractive apt-get {apt_options}install -y -qq python3.12 python3.12-venv tar gzip
python3.12 -m venv "$root/venv"
python="$root/venv/bin/python"
"$python" -m pip install --retries 0 --timeout 15 --no-cache-dir --disable-pip-version-check --only-binary=:all: --no-deps boto3==1.40.72 botocore==1.40.72 jmespath==1.0.1 s3transfer==0.14.0 python-dateutil==2.9.0.post0 six==1.17.0 urllib3==2.6.3
"$python" - <<'PY'
import boto3, hashlib, tarfile
import botocore.session
from pathlib import Path
from botocore.config import Config
assert boto3.__version__=='1.40.72' and botocore.__version__=='1.40.72'
assert 'IfNoneMatch' in botocore.session.get_session().get_service_model('s3').operation_model('PutObject').input_shape.members
s3=boto3.client('s3',region_name='{REGION}',config=Config(retries={{'total_max_attempts':1}}))
response=s3.get_object(Bucket='{BUCKET}',Key='{archive_key}')
digest=hashlib.sha256()
with response['Body'] as source, open('source.tar.gz','xb') as output:
    while chunk:=source.read(65536):
        {archive_write_guard}digest.update(chunk); output.write(chunk)
assert digest.hexdigest()=='{archive_sha}'
Path('repo').mkdir()
with tarfile.open('source.tar.gz','r:gz') as archive:
{archive_extract_guard}    archive.extractall('repo',filter='data')
PY
{bootstrap_stop}export PYTHONPATH="$root/repo"
systemd-run --unit={SUPERVISOR_UNIT} --wait --pipe {supervisor_options}-p 'Delegate=cpu memory pids' -p DelegateSubgroup=supervisor -p RuntimeMaxSec={WALL} -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=AWS_RETRY_MODE=standard \\
 {('--setenv=TMPDIR="$TMPDIR" --setenv=TMP="$TMP" --setenv=TEMP="$TEMP" --setenv=PYTHONPYCACHEPREFIX="$PYTHONPYCACHEPREFIX" '+chr(92)) if SQ4 else chr(92)}
 "$python" -m {MODULE} {'--co-selection-layout ' if CO_SELECTION_LAYOUT else '--pq-residual-source ' if PQ_RESIDUAL_SOURCE else '--corrected-four-bit ' if CORRECTED_FOUR_BIT else '--histogram-sq4 ' if HISTOGRAM_SQ4 else '--sq4 ' if SQ4 else ''}{'--remote-canary' if CANARY else '--remote'} "$root/repo" "$root" '{commit}' '{archive_sha}' '{prefix}' '{qualification['config_sha256']}'
'''
    if CANARY:
        # The existing bootstrap and supervisor each get an observed kernel cap.
        body = body.replace('systemd-run --unit=fine-pack-stop', '''if test "${BORSUK_CANARY_BOOTSTRAP_CAPPED:-0}" != 1; then
 exec systemd-run --unit=fine-pack-canary-bootstrap --wait --pipe -p MemoryMax=256M -p MemorySwapMax=0 -p CPUQuota=100% -p TasksMax=128 -p RuntimeMaxSec=480 --setenv=BORSUK_CANARY_BOOTSTRAP_CAPPED=1 /bin/bash "$0"
fi
systemd-run --unit=fine-pack-stop''',1)
        body = body.replace('-p RuntimeMaxSec='+str(WALL)+' -p WorkingDirectory=',
            '-p MemoryMax=256M -p MemorySwapMax=0 -p CPUQuota=100% -p TasksMax=128 -p RuntimeMaxSec='+str(WALL)+' -p WorkingDirectory=',1)
    if CANARY or CO_SELECTION_LAYOUT:
        # Authenticate the extraction roster before any imported code can run.
        paths = qualification['source_archive_paths']
        body = body.replace("    archive.extractall('repo',filter='data')", "    assert sorted(m.name for m in archive.getmembers() if m.isfile()) == "+repr(paths)+", 'canary source archive roster'\n    assert all(m.isfile() or m.isdir() for m in archive.getmembers()), 'canary archive regular files'\n    archive.extractall('repo',filter='data')")
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    require(len(body.encode()) < 16384, 'EC2 userdata cap')
    return body


def download(s3, descriptor, destination):
    if CANARY:
        allowed = (dict(CANARY_BINARY_PIN, key=descriptor.get('key')), dict(
            destination=str(SQ4_INPUT_ROOT/'corrected-four-bit-closed-populations.json'),
            bytes=CORRECTED_EVIDENCE[-1][1],sha256=CORRECTED_EVIDENCE[-1][2],key=descriptor.get('key')))
        require(descriptor in allowed and (str(destination).endswith('/'+BINARY_NAME) if 'destination' not in descriptor
            else Path(destination).name == 'closed-populations.json'), 'canary forbids dataset body GET before dispatch')
        body_pin({k:descriptor[k] for k in ('bytes','sha256')},max_bytes=16*1024**2)
    limit = 64*1024**2
    if SQ4 and 'destination' in descriptor:
        require((CANARY or descriptor['destination'] == str(destination)) and
                (descriptor['destination'],descriptor['bytes'],descriptor['sha256']) in INPUT_PINS, 'SQ4 exact retained download descriptor')
        limit = descriptor['bytes']
    response = s3.get_object(Bucket=BUCKET, Key=descriptor['key'])
    expected = body_pin({k:descriptor[k] for k in ('bytes', 'sha256')}, max_bytes=limit)
    with response['Body'] as source:
        require(response['ContentLength'] == expected['bytes'], 'transport ContentLength')
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        require(destination.absolute().resolve() == destination.absolute(), 'staging symlink')
        digest, total = hashlib.sha256(), 0
        with destination.open('xb') as output:
            while chunk := source.read(min(65536, expected['bytes']-total+1)):
                total += len(chunk)
                require(total <= expected['bytes'], 'transport body exceeds descriptor')
                digest.update(chunk); output.write(chunk)
            require(dict(bytes=total, sha256=digest.hexdigest()) == expected, 'transport body pin')
            output.flush(); os.fsync(output.fileno())
    sync_directory(destination.parent)


def sdk_guard():
    import boto3
    import botocore.session
    require(boto3.__version__ == botocore.__version__ == '1.40.72', 'pinned SDK interpreter')
    members = botocore.session.get_session().get_service_model('s3').operation_model('PutObject').input_shape.members
    require('IfNoneMatch' in members, 'SDK lacks immutable PutObject.IfNoneMatch')
    return dict(boto3=boto3.__version__, botocore=botocore.__version__, put_object_if_none_match=True)


def runtime_abi():
    release = dict(line.split('=',1) for line in Path('/etc/os-release').read_text().splitlines() if '=' in line)
    return dict(machine=platform.machine(), python=list(sys.version_info[:2]),
                os={k:release[k].strip('"') for k in ('ID','VERSION_ID')},
                libc=list(platform.libc_ver()), sdk=sdk_guard())


def canary_imports(repo, config):
    import importlib
    imported = {}
    for name in CODE:
        module = sys.modules[__name__] if name == 'scripts/launch_fine_pack_diagnostic.py' else importlib.import_module(name[:-3].replace('/','.'))
        path = Path(module.__file__).absolute()
        require(path.resolve() == (Path(repo)/name).resolve() and file_pin(path)['sha256'] == config['code_sha256'][name],
            'canary actual import origin: '+name)
        imported[name] = config['code_sha256'][name]
    return imported


def canary_sentinels():
    """Any Python panel/GT open fails immediately; native gets usage-only argv."""
    def forbid(event, args):
        if event == 'open' and isinstance(args[0], (str,bytes,os.PathLike)):
            path = Path(os.fsdecode(args[0])).absolute()
            require(path != SQ4_INPUT_ROOT and SQ4_INPUT_ROOT not in path.parents, 'canary forbids panel/GT opens')
    sys.addaudithook(forbid)
    return dict(panel_gt_root=str(SQ4_INPUT_ROOT),fail_fast_open_sentinel=True,native_usage_only=True)


def canary_transport(s3, config, prefix):
    from types import SimpleNamespace
    attempts = dict(dispatch_attempts=0,limit=128,scope='wrapped-worker-s3-sdk-dispatches')
    selected = {config['binary']['key'],config['inputs'][-1]['key']}
    heads = {d['key'] for d in (*config['inputs'],config['binary'])}
    def call(method, **kwargs):
        key = kwargs['Key']
        own = key == prefix+'/terminal.json' or key.startswith(prefix+'/artifacts/') and key[len(prefix+'/artifacts/'):] in ARTIFACTS
        require(kwargs['Bucket'] == BUCKET and (key in heads if method=='head_object' else key in selected or own if method=='get_object' else own),
            'canary transport refuses dataset GET or foreign object before dispatch')
        require(attempts['dispatch_attempts'] < attempts['limit'], 'canary 128 dispatch cap')
        attempts['dispatch_attempts'] += 1
        return getattr(s3,method)(**kwargs)
    return SimpleNamespace(**{n:(lambda method=n,**kw:call(method,**kw)) for n in ('get_object','head_object','put_object')},canary_attempts=attempts)


def validate_abi(abi):
    require(abi == dict(machine='x86_64', python=[3,12], os=dict(ID='ubuntu',VERSION_ID='24.04'),
        libc=['glibc','2.39'], sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True)), 'selected x86 Ubuntu/Python/native ABI and SDK')


def stage(s3, config, root):
    validate_config(config)
    if CANARY:
        require(all(not os.path.lexists(root/n) for n in (BINARY_NAME,'native-config.json','metadata/closed-populations.json','stage-receipt.json')),
            'canary one original staging attempt; no overwrite')
    scratch = None
    if SQ4:
        reserve = config['fixed']['scratch']
        available = os.statvfs(root).f_bavail*os.statvfs(root).f_frsize
        charged = sum(n for k,n in reserve.items() if k != 'cap_bytes')
        source_bytes = sum(p.stat().st_size for p in (root/'repo').rglob('*') if p.is_file())
        source_bytes += (root/'source.tar.gz').stat().st_size if (root/'source.tar.gz').exists() else 0
        bootstrap_bytes = sum(max(p.lstat().st_size,p.lstat().st_blocks*512) for tree in (root/'venv',root/'bootstrap')
                              for p in tree.rglob('*'))
        bootstrap = decode(read(root/'bootstrap/scratch.json'))
        require(bootstrap['closed'] is True and bootstrap['cap_exceeded'] is False and bootstrap['interval_seconds'] == 1
                and bootstrap['sample_count'] >= 2 and bootstrap['reserve'] == reserve
                and bootstrap['peak_bytes']+sum(reserve[k] for k in ('input_bytes','native_output_bytes','binary_bytes','auxiliary_bytes')) <= SQ4_SCRATCH_CAP,
                'SQ4 observed bootstrap overlap')
        observation = scratch_observation(root)
        remaining = reserve['input_bytes']+reserve['native_output_bytes']+reserve['binary_bytes']+reserve['auxiliary_bytes']
        require(available >= charged and source_bytes <= reserve['source_archive_bytes']
                and bootstrap_bytes <= reserve['bootstrap_bytes']
                and observation['whole_scratch_bytes']+remaining <= reserve['cap_bytes'], 'SQ4 scratch/source/input/output coexistence admission')
        require(all(not os.path.lexists(d['destination']) for d in config['inputs']), 'SQ4 retained input no overwrite')
        scratch = dict(config_sha256=config['native_config_sha256'], observation=observation,
            reserve=reserve, charged_bytes=charged, free_bytes_before=available,
            projected_peak_bytes=observation['whole_scratch_bytes']+remaining,
            source_archive_observed_bytes=source_bytes, bootstrap_observed_bytes=bootstrap_bytes, bootstrap=bootstrap)
    heads = []
    if CANARY:
        for descriptor in (*config['inputs'],config['binary']):
            response = s3.head_object(Bucket=BUCKET,Key=descriptor['key'])
            require(type(response['ContentLength']) is int and response['ContentLength'] == descriptor['bytes'], 'canary HEAD size/key')
            if 'sha256' in response.get('Metadata',{}):
                require(response['Metadata']['sha256'] == descriptor['sha256'], 'canary HEAD digest')
            heads.append(dict(key=descriptor['key'],bytes=response['ContentLength'],sha256=descriptor['sha256'],
                etag=response.get('ETag'),version_id=response.get('VersionId')))
    download(s3, config['binary'], root/BINARY_NAME)
    (root/BINARY_NAME).chmod(0o500)
    write(root/'native-config.json', encoded(config['native_config']))
    selected = config['inputs'][-1:] if CANARY else config['inputs']
    for descriptor in selected:
        if SQ4:
            require(scratch_observation(root)['whole_scratch_bytes']+descriptor['bytes'] <= SQ4_SCRATCH_CAP, 'SQ4 scratch before input write')
        download(s3, descriptor, root/'metadata/closed-populations.json' if CANARY else descriptor['destination'])
    result = dict(binary=file_pin(root/BINARY_NAME), native_config=file_pin(root/'native-config.json'),
                  inputs={d['destination']:file_pin(root/'metadata/closed-populations.json' if CANARY else d['destination']) for d in selected},
                  exact_six_inputs=True, compiler_used=False)
    if SQ4:
        del result['exact_six_inputs']
        result['canary_metadata_only' if CANARY else 'exact_eleven_inputs' if PQ_RESIDUAL_SOURCE else 'exact_nineteen_inputs' if CORRECTED_FOUR_BIT else 'exact_eighteen_inputs'] = True
        result['scratch'] = scratch
    if CANARY:
        result.update(heads=heads,dataset_body_gets=0,selected_get_keys=[config['binary']['key'],selected[0]['key']])
    write(root/'stage-receipt.json', encoded(result))
    return result


def cgroup_snapshot(group):
    return dict(path=str(group), observer_pid=os.getpid(),
                files={name:(group/name).read_text() for name in CGROUP_FILES})


def events(body):
    return {k:int(v) for k, v in (line.split() for line in body.splitlines())}


def validate_snapshot(snapshot):
    f = snapshot['files']
    require(set(f) == set(CGROUP_FILES), 'mandatory kernel resource counters')
    require(int(f['memory.max']) == CAPS['memory_bytes']
            and 0 <= int(f['memory.peak']) <= CAPS['memory_bytes'], 'native kernel memory cap/peak')
    require(int(f['memory.swap.max']) == int(f['memory.swap.peak']) == 0, 'native swap')
    quota, period = map(int, f['cpu.max'].split())
    require(quota == period and period > 0, 'CPU1 quota')
    require(int(f['pids.max']) == FIXED['tasks_max'] and int(f['pids.current']) == 0
            and not f['cgroup.procs'].strip() and events(f['cgroup.events'])['populated'] == 0, 'native descendants drained')
    require(events(f['cpu.stat'])['usage_usec'] >= 0, 'CPU evidence')
    for key in ('memory.events', 'memory.swap.events', 'pids.events'):
        counters = events(f[key])
        required = ('oom', 'oom_kill', 'oom_group_kill') if key == 'memory.events' else tuple(counters)
        require(counters and all(counters.get(k, -1) == 0 for k in required), 'resource failure: '+key)


def validate_resources(resource):
    require(resource['closed'] is True and resource['deadline_exceeded'] is False, 'resource closure/deadline')
    before, after = resource['before'], resource['after']
    require(before['path'] == after['path'], 'same original cgroup')
    for snapshot in (before, after):
        validate_snapshot(snapshot)
    d = resource['delegation']
    require(d['unit'] == SUPERVISOR_UNIT+'.service' and CONTROLLERS <= set(d['available'])
            and CONTROLLERS <= set(d['enabled']) and d['parent_process_ids'] == []
            and d['parent_type'] == 'domain' and d['observer_process_ids'] == [d['observer_pid']]
            and d['observer_pid'] == before['observer_pid'] == after['observer_pid']
            and str(Path(d['parent'])/'native') == before['path']
            and str(Path(d['parent'])/'supervisor') == d['observer'], 'dedicated delegated supervisor/native leaves')
    for key in ('memory.events', 'memory.swap.events', 'pids.events', 'cpu.stat'):
        a, b = events(before['files'][key]), events(after['files'][key])
        require(set(a) == set(b) and all(b[k] >= a[k] for k in a), 'resource counter regression')
    require(resource['cpu_affinity'] == [0] and resource['supervisor_outside_native_cgroup'] is True
            and resource['process_attached_before_exec'] is True
            and resource['pre_exec_limits_observed'] is True
            and resource['descendants_remaining_after_exit'] is False
            and 0 <= resource['elapsed_seconds'] <= (CORRECTED_SUPERVISOR_SECONDS if CORRECTED_FOUR_BIT else CAPS['deadline_seconds']), 'original native resource authority')


def delegated_group(record, proc=Path('/proc/self/cgroup'), root=Path('/sys/fs/cgroup')):
    lines = proc.read_text().splitlines()
    require(len(lines) == 1 and lines[0].startswith('0::/'), 'unified original supervisor cgroup')
    relative = lines[0][4:]
    require(relative and all(p not in ('', '.', '..') for p in relative.split('/')), 'supervisor cgroup path')
    observer = root/relative
    parent = observer.parent
    # Only the dedicated service's delegated subtree is ours to configure.
    require(observer.name == 'supervisor' and parent.name == SUPERVISOR_UNIT+'.service', 'dedicated supervisor service required')
    record.update(unit=parent.name, parent=str(parent), observer=str(observer), observer_pid=os.getpid(),
        available=sorted((parent/'cgroup.controllers').read_text().split()),
        enabled_before=sorted((parent/'cgroup.subtree_control').read_text().split()),
        parent_process_ids=[int(p) for p in (parent/'cgroup.procs').read_text().split()],
        observer_process_ids=[int(p) for p in (observer/'cgroup.procs').read_text().split()],
        parent_type=(parent/'cgroup.type').read_text().strip())
    require(CONTROLLERS <= set(record['available']), 'required delegated cpu/memory/pids controllers unavailable')
    require(record['parent_type'] == 'domain' and record['parent_process_ids'] == []
            and record['observer_process_ids'] == [os.getpid()], 'delegated domain must keep supervisor in its leaf')
    (parent/'cgroup.subtree_control').write_text('+cpu +memory +pids')
    record['enabled'] = sorted((parent/'cgroup.subtree_control').read_text().split())
    require(CONTROLLERS <= set(record['enabled']), 'required native controllers not enabled')
    return parent/'native'


def create_group(group):
    for name, value in (('memory.max', str(CAPS['memory_bytes'])), ('memory.swap.max', '0'),
                        ('cpu.max', '100000 100000'), ('pids.max', str(FIXED['tasks_max'])),
                        ('memory.oom.group', '1')):
        require((group/name).is_file(), 'native controller interface unavailable: '+name)
        (group/name).write_text(value)


def drain_group(group):
    if (group/'cgroup.procs').read_text().strip():
        (group/'cgroup.kill').write_text('1')
    deadline = time.monotonic()+5
    while events((group/'cgroup.events').read_text())['populated']:
        require(time.monotonic() < deadline, 'cgroup drain deadline')
        time.sleep(.05)


def supervise(config, root, *, run_id):
    """Popen owns the original executable; the observer stays outside its cap."""
    command = [str(root/BINARY_NAME), SQ4_CLI if SQ4 else 'check-fine-pack', str(root/'native-config.json'),
               config['native_config_sha256'], str(root/'screen/report.json')]
    if CANARY:
        command = [str(root/BINARY_NAME),SQ4_CLI]  # Strict wrong argc returns usage before any data open.
    (root/'screen').mkdir(exist_ok=False)
    receipt = dict(command=command, process_exit_code=None, process_started=False,
                   config_sha256=config['native_config_sha256'], binary=config['binary'], run_id=run_id)
    resource = dict(closed=False, deadline_exceeded=False, before=None, after=None,
                    cpu_affinity=[0], process_attached_before_exec=False,
                    supervisor_outside_native_cgroup=False, elapsed_seconds=0,
                    descendants_remaining_after_exit=True, delegation={}, pre_exec_limits_observed=False)
    cleanup = dict(drain_complete=False, cleanup_complete=False, output_durable=False)
    process, group, created, error = None, None, False, None
    started = time.monotonic()
    scratch = None
    if SQ4:
        temporary = root/'bootstrap/tmp'
        temporary.mkdir(parents=True, exist_ok=True)
        require(temporary.resolve() == temporary.absolute(), 'SQ4 charged temporary directory')
        scratch = dict(run_id=run_id, config_sha256=config['native_config_sha256'], cap_bytes=SQ4_SCRATCH_CAP,
            admission=decode(read(root/'stage-receipt.json'))['scratch'], sample_count=0, peak_bytes=0,
            interval_seconds=1, closed=False, cap_exceeded=False, closure_reserve_bytes=SQ4_CLOSURE_RESERVE)
    last_scratch = started-1
    def observe_scratch():
        nonlocal last_scratch
        observation = scratch_observation(root)
        scratch.update(last=observation, sample_count=scratch['sample_count']+1,
            peak_bytes=max(scratch['peak_bytes'],observation['whole_scratch_bytes']))
        last_scratch = time.monotonic()
        scratch['projected_closure_peak_bytes'] = observation['whole_scratch_bytes']+SQ4_CLOSURE_RESERVE
        if scratch['projected_closure_peak_bytes'] > SQ4_SCRATCH_CAP:
            scratch['cap_exceeded'] = True
            raise ValueError('SQ4 whole scratch cap')
    old_term = signal.getsignal(signal.SIGTERM)
    def interrupted(signum, frame):
        raise InterruptedError('original supervisor interrupted')
    signal.signal(signal.SIGTERM, interrupted)
    try:
        if SQ4:
            observe_scratch()
            require(file_pin(root/'native-config.json') == pin(encoded(config['native_config']))
                    and file_pin(root/BINARY_NAME) == {k:config['binary'][k] for k in ('bytes','sha256')}, 'SQ4 pre-exec config/binary drift')
        group = delegated_group(resource['delegation'])
        group.mkdir(exist_ok=False); created = True; create_group(group)
        resource['before'] = cgroup_snapshot(group)
        validate_snapshot(resource['before'])
        resource['pre_exec_limits_observed'] = True
        observer = Path('/proc/self/cgroup').read_text()
        require(str(group).removeprefix('/sys/fs/cgroup') not in observer, 'supervisor outside native cap')
        resource['supervisor_outside_native_cgroup'] = True
        fd = os.open(group/'cgroup.procs', os.O_WRONLY)
        def attach():
            os.sched_setaffinity(0, {0})
            os.write(fd, str(os.getpid()).encode())
            os.close(fd)
        environment = dict(os.environ, BORSUK_CPU_THREADS='1', RAYON_NUM_THREADS='1',
                           TOKIO_WORKER_THREADS='1', OMP_NUM_THREADS='1', AWS_MAX_ATTEMPTS='1')
        if SQ4:
            environment.update(TMPDIR=str(temporary), TMP=str(temporary), TEMP=str(temporary))
        try:
            with (root/'native.log').open('xb') as log:
                process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                    env=environment, preexec_fn=attach, pass_fds=(fd,), start_new_session=True)
                receipt.update(process_started=True, process_id=process.pid)
                resource['process_attached_before_exec'] = True
                while process.poll() is None:
                    resource['elapsed_seconds'] = time.monotonic()-started
                    deadline = CORRECTED_SUPERVISOR_SECONDS if CORRECTED_FOUR_BIT else CAPS['deadline_seconds']
                    if resource['elapsed_seconds'] >= deadline:
                        resource['deadline_exceeded'] = True
                        raise TimeoutError('native supervisor '+str(deadline)+'s deadline')
                    require(log.tell() <= 4*1024**2, 'native log cap')
                    if SQ4:
                        require(sum(p.lstat().st_size for p in (root/'screen').iterdir()) <= CAPS['output_bytes'], 'SQ4 whole native output cap')
                        if time.monotonic()-last_scratch >= 1:
                            observe_scratch()
                    time.sleep(.05)
                receipt['process_exit_code'] = process.wait()
                resource['descendants_remaining_after_exit'] = bool((group/'cgroup.procs').read_text().strip())
                log.flush(); os.fsync(log.fileno())
        finally:
            os.close(fd)
    except BaseException as failure:
        error = failure
    finally:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        resource['elapsed_seconds'] = time.monotonic()-started
        try:
            if created:
                drain_group(group)
                cleanup['drain_complete'] = True
                if process is not None:
                    receipt['process_exit_code'] = process.wait(timeout=5)
                resource['after'] = cgroup_snapshot(group); resource['closed'] = True
                group.rmdir(); cleanup['cleanup_complete'] = True
            for name in (('native.log', *SQ4_OUTPUTS) if SQ4 else ('native.log', 'screen/report.json', 'screen/report.permutations.json', 'screen/report.prefix.jsonl')):
                path = root/name
                if path.exists():
                    with regular(path) as output:
                        os.fsync(output.fileno())
            sync_directory(root/'screen'); sync_directory(root)
            cleanup['output_durable'] = True
        except BaseException as failure:
            error = error or failure
            if process is not None and process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                receipt['process_exit_code'] = process.wait(timeout=5)
        if error is not None:
            receipt['error'] = dict(type=type(error).__name__, message=str(error))
        if SQ4 and (root/'screen/report.json').exists():
            try:
                receipt['report_sha256'] = file_pin(root/'screen/report.json')['sha256']
            except BaseException as failure:
                receipt['error'] = dict(type=type(failure).__name__, message=str(failure))
        if SQ4:
            signal.signal(signal.SIGTERM, old_term)
            scratch_room(root, SQ4_CLOSURE_RESERVE)
        write(root/'native-exit.json', encoded(receipt))
        write(root/'resources.json', encoded(resource))
        write(root/'cleanup.json', encoded(cleanup))
        if SQ4:
            try:
                observe_scratch()
            except BaseException as failure:
                scratch['error'] = dict(type=type(failure).__name__, message=str(failure))
            scratch.update(closed=True, process_exit_code=receipt['process_exit_code'],
                           report_sha256=receipt.get('report_sha256'))
            require(len(encoded(scratch)) <= 65536, 'SQ4 scratch receipt closure reserve')
            scratch_room(root, SQ4_CLOSURE_RESERVE)
            write(root/'scratch.json', encoded(scratch))
        signal.signal(signal.SIGTERM, old_term)
    return receipt


def validate_sq4_result(root, config):
    validate_config(config)
    if CORRECTED_FOUR_BIT:
        validate_canary_admission(config,root,collected=True)
    sq4_qualification(config, root, collected=True)
    validate_abi(decode(read(root/'runtime-abi.json')))
    binary = {k:config['binary'][k] for k in ('bytes','sha256')}
    require(file_pin(root/'native-config.json') == pin(encoded(config['native_config']))
            and file_pin(root/BINARY_NAME) == binary, 'SQ4 executed config/binary bytes')
    receipt, resource, cleanup = (decode(read(root/n)) for n in ('native-exit.json','resources.json','cleanup.json'))
    command = receipt['command']; original = Path(command[0]).parent.parent
    require(original.is_absolute() and original.resolve() == original and command == [str(original/BINARY_NAME),
        SQ4_CLI, str(original/'native-config.json'), config['native_config_sha256'], str(original/'screen/report.json')]
        and receipt['config_sha256'] == config['native_config_sha256'] and receipt['binary'] == config['binary']
        and type(receipt['process_exit_code']) is int and receipt['process_exit_code'] == 0
        and receipt['process_started'] is True and 'error' not in receipt, 'SQ4 original exact native command/exit0')
    validate_resources(resource)
    require(all(cleanup.get(k) is True for k in ('drain_complete','cleanup_complete','output_durable')), 'SQ4 native cleanup/durability')
    stage_receipt = decode(read(root/'stage-receipt.json'))
    staged_count = 'exact_eleven_inputs' if PQ_RESIDUAL_SOURCE else 'exact_nineteen_inputs' if CORRECTED_FOUR_BIT else 'exact_eighteen_inputs'
    require(set(stage_receipt) == {'binary','native_config','inputs',staged_count,'compiler_used','scratch'}
        and stage_receipt['binary'] == binary and stage_receipt['native_config'] == pin(encoded(config['native_config']))
        and stage_receipt['inputs'] == {d['destination']:{k:d[k] for k in ('bytes','sha256')} for d in config['inputs']}
        and stage_receipt[staged_count] is True and stage_receipt['compiler_used'] is False, 'SQ4 authenticated opaque staging')
    scratch = decode(read(root/'scratch.json'))
    admission = scratch['admission']; reserve = config['fixed']['scratch']
    require(scratch['run_id'] == receipt['run_id'] and scratch['config_sha256'] == config['native_config_sha256']
        and scratch['cap_bytes'] == SQ4_SCRATCH_CAP and scratch['closed'] is True and scratch['cap_exceeded'] is False
        and scratch['process_exit_code'] == 0 and scratch['interval_seconds'] == 1 and scratch['sample_count'] >= 2
        and 'error' not in scratch and admission == stage_receipt['scratch'] and admission['reserve'] == reserve
        and admission['config_sha256'] == config['native_config_sha256']
        and admission['charged_bytes'] == sum(n for k,n in reserve.items() if k != 'cap_bytes')
        and admission['charged_bytes'] <= admission['free_bytes_before']
        and admission['projected_peak_bytes'] <= SQ4_SCRATCH_CAP
        and admission['source_archive_observed_bytes'] <= reserve['source_archive_bytes']
        and admission['bootstrap_observed_bytes'] <= reserve['bootstrap_bytes']
        and admission['bootstrap']['closed'] is True and admission['bootstrap']['cap_exceeded'] is False
        and admission['bootstrap']['reserve'] == reserve and admission['bootstrap']['sample_count'] >= 2
        and scratch['closure_reserve_bytes'] == SQ4_CLOSURE_RESERVE
        and scratch['projected_closure_peak_bytes'] == scratch['last']['whole_scratch_bytes']+SQ4_CLOSURE_RESERVE <= SQ4_SCRATCH_CAP
        and 0 <= scratch['last']['whole_scratch_bytes'] <= scratch['peak_bytes'] <= SQ4_SCRATCH_CAP
        and set(scratch['last']['roots']) == {str(original),str(SQ4_INPUT_ROOT)}
        and sum(scratch['last']['roots'].values()) == scratch['last']['whole_scratch_bytes'], 'SQ4 whole-worker scratch observations')
    require({p.name for p in (root/'screen').iterdir() if not p.name.endswith('.gz')} == {Path(n).name for n in SQ4_OUTPUTS}, 'SQ4 exact native output closure')
    require(sum(file_pin(root/n)['bytes'] for n in SQ4_OUTPUTS) <= CAPS['output_bytes'], 'SQ4 native output cap')
    if CO_SELECTION_LAYOUT:
        require(decode(read(root/'imports.json')) == config['code_sha256'], 'co-selection actual existing import closure')
        return validate_co_selection_outputs(root,config,original,receipt,scratch)
    if PQ_RESIDUAL_SOURCE:
        return validate_pq_residual_outputs(root,config,original,receipt,scratch)
    body = read(root/'screen/report.json', 16384 if CORRECTED_FOUR_BIT else 8192); report = decode(body)
    require(receipt['report_sha256'] == scratch['report_sha256'] == sha(body)
        and report['schema'] == SQ4_SCHEMA+'-report-v1' and report['codec'] == SQ4_CODEC
        and report['config_sha256'] == config['native_config_sha256'] and report['source_identity_sha256'] == SOURCE_ID
        and report['complete'] is True and type(report['queries']) is int and report['queries'] == 128
        and report['status'] in ('SURVIVED_CONSUMED_PANELS','REJECT') and report['standalone_authority'] is False
        and report['quality_or_performance_claim'] is False and report['requires_matching_supervisor_exit_receipt'] is True, 'SQ4 independently bound terminal report')
    details = report['details']; native = config['native_config']; truth = [p['truth'] for p in native['panels']]
    if CORRECTED_FOUR_BIT:
        return validate_corrected_outputs(root, config, original, report, body)
    require(details['rows'] == 100000 and details['dimensions'] == 768 and details['truth'] == truth
        and details['frozen_original_authority'] is True and details['caps'] == CAPS
        and details['whole_process_supervisor_required'] is True and 0 <= details['operations'] <= CAPS['operations']
        and details['pair_payload_bytes'] == 79200000 and 0 <= details['modeled_peak_bytes'] <= CAPS['memory_bytes']
        and 79200000 <= details['modeled_output_bytes'] <= CAPS['output_bytes'], 'SQ4 frozen native authority/resources')
    def authenticate(descriptor, name):
        require(descriptor == dict(path=str(original/name), **file_pin(root/name)), 'SQ4 native closure pin: '+name)
    freeze_name = f'screen/report.{SQ4_NAME}-freeze.json'
    authenticate(details['freeze'], freeze_name)
    freeze = decode(read(root/freeze_name, 8*1024**2))
    require(freeze['schema'] == SQ4_SCHEMA+'-freeze-v1' and freeze['config_sha256'] == config['native_config_sha256']
        and freeze['source_identity_sha256'] == SOURCE_ID and freeze['truth_opened'] is False
        and freeze['original_seal'] == native['original_seal'] and freeze['truth'] == truth, 'SQ4 freeze before truth')
    authenticate(freeze['payload_seal'], f'screen/report.{SQ4_NAME}-payloads.json')
    authenticate(freeze['nomination_prefix'], f'screen/report.{SQ4_NAME}-prefix.jsonl')
    require(file_pin(root/f'screen/report.{SQ4_NAME}-prefix.jsonl') == {k:native['prefix'][k] for k in ('bytes','sha256')}, 'SQ4 consumed original nomination prefix')
    seal = decode(read(root/f'screen/report.{SQ4_NAME}-payloads.json', 8*1024**2))
    require(seal['schema'] == SQ4_SCHEMA+'-payload-seal-v1' and seal['config_sha256'] == config['native_config_sha256']
        and seal['source_identity_sha256'] == SOURCE_ID and seal['codec'] == report['codec']
        and seal['original_seal'] == native['original_seal'] and seal['queries_opened'] is seal['truth_opened'] is False
        and seal['payloads'] == freeze['payloads'] and len(seal['payloads']) == 2, 'SQ4 paired payload seal')
    for i,payload in enumerate(seal['payloads']):
        authenticate(payload['payload'], f'screen/report.{SQ4_NAME}-{i}.bin')
        require(payload['payload']['bytes'] == 39600000 and payload['original_root'] == native['panels'][i]['root']
            and payload['codec'] == report['codec'] and payload['rows'] == 100000 and payload['dimensions'] == 768
            and payload['row_bytes'] == 396 and payload['group_rows'] == 16, 'SQ4 payload identity/geometry')
        if HISTOGRAM_SQ4:
            generation = payload['histogram_generation']
            stem = f'screen/report.{SQ4_NAME}-{i}.bin-'
            for key,suffix in (('root','root.json'),('book','book.bin'),('groups','groups.bin')):
                authenticate(generation[key], stem+suffix)
            generation_root = decode(read(root/(stem+'root.json')))
            require(set(generation_root) == {'schema','codec','trainer','source_identity_sha256','config_sha256',
                'original_root','original_records','coefficients_sha256','histogram_sha256','rows','dimensions',
                'book','payload','group_hashes','fit'}
                and generation_root['schema'] == SQ4_SCHEMA+'-generation-v1'
                and generation_root['codec'] == SQ4_CODEC and generation_root['trainer'] == generation['trainer'] == HISTOGRAM_TRAINER
                and generation_root['source_identity_sha256'] == SOURCE_ID
                and generation_root['config_sha256'] == config['native_config_sha256']
                and generation_root['original_root'] == payload['original_root']
                and generation_root['original_records'] == payload['original_records']
                and generation_root['rows'] == 100000 and generation_root['dimensions'] == 768
                and generation_root['book'] == generation['book'] and generation_root['group_hashes'] == generation['groups']
                and generation_root['payload'] == payload['payload'] and generation_root['fit'] == generation['fit']
                and generation_root['histogram_sha256'] == generation['fit']['histogram_sha256'], 'histogram native generation bindings')
            require(generation['book']['bytes'] == 144+65*768 and generation['groups']['bytes'] == 6250*32
                and generation['groups']['sha256'] == payload['group_hashes_sha256'], 'histogram native book/groups geometry')
            bits = payload['low_bits']+payload['step_bits']
            require(len(payload['low_bits']) == len(payload['step_bits']) == 768
                and all(type(b) is int and 0 <= b < 2**32 for b in bits), 'histogram native coefficient bits')
            coefficients = sha((768).to_bytes(4,'little')+b''.join(b.to_bytes(4,'little') for b in bits))
            require(generation_root['coefficients_sha256'] == coefficients, 'histogram native coefficient binding')
            book = read(root/(stem+'book.bin'),65536)
            require(book[:144] == b'BORSH401'+(768).to_bytes(4,'little')+(100000).to_bytes(4,'little')
                +bytes.fromhex(payload['original_records']['sha256']+coefficients+generation_root['histogram_sha256'])
                +hashlib.sha256(HISTOGRAM_TRAINER.encode()).digest(), 'histogram native book source binding')
            require(generation['startup_book_reads'] == 1 and generation['startup_book_bytes'] == generation['book']['bytes']
                and generation['startup_generation_reads'] == 3
                and generation['startup_generation_bytes'] == sum(generation[k]['bytes'] for k in ('root','book','groups'))
                and generation['mse_implies_recall'] is generation['updates_gc_integrated'] is False, 'histogram separately counted startup')
    require(len(freeze['results']) == 128, 'SQ4 all128 sealed results')
    envelopes = True
    for i,descriptor in enumerate(freeze['results']):
        name = f'screen/report.{SQ4_NAME}-result-{i}.json'; authenticate(descriptor, name)
        query = decode(read(root/name, 8*1024**2)); plan = query['plan']; sq4 = query['sq4']
        fits = sq4['verified_bytes'] <= 16*1024**2
        require(query['schema'] == SQ4_SCHEMA+'-query-v1' and query['dataset'] == native['panels'][i//64]['dataset']
            and type(query['ordinal']) is int and query['ordinal'] == i%64 and query['nominees_retained'] is True
            and query['original_cover_contained'] is True and query['truth_opened'] is False
            and query['sq8_reference_serving_eligible'] is False and sq4['fetched_ids'] == query['sq8_reference']['fetched_ids']
            and type(sq4['range_reads']) is int and 1 <= sq4['range_reads'] <= 32
            and type(sq4['verified_bytes']) is int and 0 < sq4['verified_bytes'] == plan['candidate_bytes']
            and plan['envelope_fits'] is fits, 'SQ4 authenticated same-population result/envelope')
        envelopes &= fits
        if HISTOGRAM_SQ4:
            metrics = sq4['histogram_metrics']; generation = seal['payloads'][i//64]['histogram_generation']
            require(metrics['direct_packed'] is metrics['payload_ranges_exclude_startup'] is True
                and metrics['total_cold_get_claim'] is False and all(metrics[k] == generation[k] for k in (
                    'startup_book_reads','startup_book_bytes','startup_generation_reads','startup_generation_bytes')),
                'histogram payload metrics exclude startup')
    summaries = details['summaries']
    require(len(summaries) == 2 and details['all128_envelopes_fit'] is envelopes, 'SQ4 native envelope summary')
    quality = True
    for panel,summary in zip(native['panels'], summaries):
        returned = summary['sq4_returned']; mean, p05 = returned['mean_recall'], returned['p05_hits']
        require(summary['dataset'] == panel['dataset'] and type(mean) in (int,float) and 0 <= mean <= 1
                and type(p05) is int and 0 <= p05 <= 100, 'SQ4 native quality summary')
        quality &= mean >= .98 and p05 >= 95
    require(report['status'] == ('SURVIVED_CONSUMED_PANELS' if quality and envelopes else 'REJECT'), 'SQ4 completed disposition')
    result = dict(status=report['status'], valid_diagnostic=True, report_sha256=sha(body),
        native_config_sha256=config['native_config_sha256'], binary=binary, summaries=summaries,
        all128_envelopes_fit=envelopes, quality_or_performance_claim=False)
    if HISTOGRAM_SQ4:
        startup = details['histogram_resources']
        generations = [p['histogram_generation'] for p in seal['payloads']]
        require(startup['startup_book_reads'] == 2 and startup['startup_generation_reads'] == 6
            and all(startup[k] == sum(g[k] for g in generations) for k in ('startup_book_bytes','startup_generation_bytes'))
            and startup['payload_ranges_exclude_startup'] is True and startup['total_cold_get_claim'] is False
            and startup['packed_row_bytes'] == 396, 'histogram startup separate from payload envelope')
        result['histogram_resources'] = startup
    return result


def validate_co_selection_outputs(root, config, original, receipt, scratch):
    """Bind the native closure; Python does not fit, nominate or score vectors."""
    body = read(root/'screen/report.json',8192); report = decode(body)
    sources = {n:CO_MANIFEST['source_sha256']['crates/borsuk/src/'+n] for n in CO_SOURCE_NAMES}
    require(receipt['report_sha256'] == scratch['report_sha256'] == sha(body)
        and report['schema'] == 'borsuk-co-selection-diagnostic-v1'
        and report['config_sha256'] == config['native_config_sha256']
        and report['diagnostic_source_sha256'] == sources and report['complete'] is True
        and type(report['queries']) is int and report['queries'] == 128
        and report['status'] in ('SURVIVED_NECESSARY_LOCALITY','REJECT')
        and all(report[k] is False for k in ('standalone_authority','quality_or_performance_claim',
            'truth_opened','request_vectors_opened','sq8_bodies_opened'))
        and report['source_only_anchor_vectors'] is report['requires_matching_supervisor_exit_receipt'] is True,
        'co-selection complete native report/source/scope binding')
    require(file_pin(root/'native.log')['bytes'] <= 4*1024**2, 'co-selection bounded log; silent exit0 supported')
    details,native = report['details'],config['native_config']
    require(details['caps'] == native['caps'] and details['prior_reads'] == native['prior_reads']
        and details['original_seal'] == native['original_seal']
        and details['delta_bytes'] == details['additional_attempts'] == 0, 'co-selection native authority/no delta/retry')
    for key,cap in (('construction_operations',CAPS['construction_operations']),('replay_operations',CAPS['replay_operations']),
                    ('source_auth_bytes',CAPS['source_auth_bytes']),('modeled_peak_owned_bytes',CAPS['memory_bytes']),
                    ('retained_capacity_bytes',CAPS['memory_bytes']),('wall_ms',CAPS['deadline_seconds']*1000)):
        require(type(details[key]) is int and 0 <= details[key] <= cap, 'co-selection native resource ledger: '+key)
    require(type(details['counted_operations']) is int
        and details['counted_operations'] == details['construction_operations']+details['replay_operations']
        and CAPS['caller_pinned_bytes'] <= details['retained_capacity_bytes'] <= details['modeled_peak_owned_bytes'],
        'co-selection persistent phase/memory ledgers')
    ceiling,output = details['work_preflight'],details['output_preflight']
    require(all(type(ceiling[k]) is int and details[k+'_operations'] <= ceiling[k] <= CAPS[k+'_operations']
        for k in ('construction','replay')) and type(output['total']) is int
        and sum(file_pin(root/n)['bytes'] for n in SQ4_OUTPUTS) <= output['total'] <= CAPS['output_bytes'],
        'co-selection full-batch work/output admission')
    def bound(descriptor, name):
        require(descriptor == dict(path=str(original/name),**file_pin(root/name)), 'co-selection native full-body pin: '+name)
    for key,name in (('nomination_prefix','screen/report.prefix.jsonl'),('plans','screen/report.plans.jsonl'),
                     ('held_diagnostics','screen/report.held.jsonl')):
        bound(details[key],name)
    require(file_pin(root/'screen/report.prefix.jsonl') == {k:native['prefix'][k] for k in ('bytes','sha256')}
        and len(details['maps']) == len(details['source_selections']) == 2, 'co-selection original frozen prefix/paired maps')
    for i,panel in enumerate(native['panels']):
        stem = 'screen/report.'+panel['dataset']
        bound(details['maps'][i],stem+'.map.json'); bound(details['source_selections'][i],stem+'.selections.bin')
        with regular(root/(stem+'.selections.bin')) as selected:
            header = selected.read(208)
        require(header == b'BORSCS01'+bytes.fromhex(''.join(panel[k]['sha256']
            for k in ('root','canonical','source_order','fine_order','pq','graph')))
            +(4096).to_bytes(4,'little')+(512).to_bytes(4,'little')
            and 208+4608*52 <= details['source_selections'][i]['bytes'] <= 19114192,
            'co-selection sealed source selection header/pins/train4096/held512')
        mapping = decode(read(root/(stem+'.map.json'),4*1024**2))
        require(mapping['schema'] == 'borsuk-co-selection-map-v1' and mapping['config_sha256'] == config['native_config_sha256']
            and mapping['diagnostic_source_sha256'] == sources and mapping['source'] == panel
            and mapping['source_selections'] == details['source_selections'][i] and mapping['caps'] == CAPS
            and mapping['work_preflight'] == ceiling and mapping['output_preflight'] == output
            and mapping['held_anchor_count'] == 512 and mapping['held_used_for_fitting'] is False
            and mapping['group_rows'] == 16 and mapping['blocks_per_object'] == 128 and mapping['replication_factor'] == 1
            and mapping['short_tail_last'] is mapping['query_blind'] is True and mapping['within_group_order'] == 'unchanged'
            and all(mapping['original_sources'][k] == config['source_metadata'][i][k] for k in ('primary_root','groups'))
            and all(obj['payload_materialized'] is False and obj['payload_sha256'] is obj['payload_etag'] is None
                for obj in mapping['objects']), 'co-selection query-blind maps/source metadata/virtual objects')
    # Stream the bounded metadata closure; do not decode the selection nominees
    # or reproduce the qualified Rust cover/fit/expansion algorithm here.
    for name,per_panel in (('held',512),('plans',64)):
        counts = [0,0]; fits = [0,0]
        with regular(root/('screen/report.'+name+'.jsonl')) as rows:
            for i in range(2*per_panel):
                line = rows.readline(131073)
                require(0 < len(line) <= 131072 and line.endswith(b'\n'), 'co-selection bounded complete metadata row')
                row = decode(line); panel = i//per_panel; counts[panel] += 1
                require(row['dataset'] == native['panels'][panel]['dataset']
                    and type(row['held_ordinal' if name=='held' else 'ordinal']) is int
                    and row['held_ordinal' if name=='held' else 'ordinal'] == i%per_panel
                    and row['map'] == details['maps'][panel] and row['source_selections'] == details['source_selections'][panel],
                    'co-selection complete ordered paired metadata/pins')
                if name == 'held':
                    require(row['used_for_fitting'] is row['policy_tuning'] is False and type(row['fits']) is bool,
                        'co-selection held diagnostics cannot fit/tune')
                    continue
                plan = row['plan']; p = native['panels'][panel]
                require(row['phase'] == 'co_selection_plan' and row['root'] == p['root']
                    and all(row[k] is False for k in ('truth_opened','request_vectors_opened','sq8_bodies_opened'))
                    and plan['all_nominees_retained'] is True and plan['root_sha256'] == p['root']['sha256']
                    and plan['source_sha256'] == p['canonical']['sha256'] and plan['prior'] == native['prior_reads']
                    and type(plan['fits']) is bool, 'co-selection all128 complete nominee retention/no truth/payload')
                for k,cap in (('total_operations',32),('total_bytes',16*1024**2)):
                    require(plan[k] is None or type(plan[k]) is int and plan[k] >= 0, 'co-selection total cost type')
                    if plan['fits']:
                        require(type(plan[k]) is int and 0 < plan[k] <= cap, 'co-selection SAME total32/16MiB fitted cover')
                fits[panel] += int(plan['fits'])
            require(rows.read(1) == b'', 'co-selection no extra metadata rows')
        if name == 'plans':
            require(details['per_panel_fits'] == fits and all(type(n) is int for n in details['per_panel_fits'])
                and report['status'] == ('SURVIVED_NECESSARY_LOCALITY' if fits == [64,64] else 'REJECT'),
                'co-selection complete scientific REJECT versus INVALID')
    return dict(valid_diagnostic=True,status=report['status'],standalone_authority=False,
        requires_matching_supervisor_exit_receipt=True,quality_or_performance_claim=False,
        native_config_sha256=config['native_config_sha256'],binary=CO_BINARY_PIN,report_sha256=sha(body),
        per_panel_fits=details['per_panel_fits'],construction_operations=details['construction_operations'],
        replay_operations=details['replay_operations'])


def validate_pq_residual_outputs(root, config, original, receipt, scratch):
    """Authenticate Rust's sealed closure, without decoding scores or reducing hits."""
    body = read(root/'screen/report.json',16384)
    report = decode(body)
    require(receipt['report_sha256'] == scratch['report_sha256'] == sha(body)
        and report['schema'] == 'borsuk-pq-residual-source-report-v1'
        and report['codec'] == 'borsuk-pq64-residual4-original-norm-v1'
        and report['source_identity_sha256'] == SOURCE_ID and report['config_sha256'] == config['native_config_sha256']
        and report['complete'] is True and report['status'] in ('SURVIVED_SOURCE_NEIGHBORHOODS','REJECT')
        and report['standalone_authority'] is report['quality_or_performance_claim'] is False
        and report['requests_opened'] is report['truth_opened'] is False
        and report['requires_matching_supervisor_exit_receipt'] is True, 'PQ original native report binding/scope')
    require(file_pin(root/'native.log')['bytes'] <= 4*1024**2, 'PQ bounded native log (silent exit0 supported)')
    details = report['details']
    require(all(type(details[k]) is int and details[k] == n for k,n in
        (('rows',100000),('dimensions',768),('cohort',4096),('anchors',64),('k',100),('scratch_bytes',0)))
        and details['strict_retained_authority'] is True and details['native_query_constant'] is True
        and details['candidate_query_constant'] is False, 'PQ frozen native mechanism geometry/conventions')
    admission = details['admission']
    require(set(admission) == {'cumulative_read_bytes','operations','coexisting_bytes','output_bytes','scratch_bytes'}
        and type(admission['cumulative_read_bytes']) is int and admission['cumulative_read_bytes'] >= 0
        and all(type(admission[k]) is int and 0 <= admission[k] <= n for k,n in
        (('operations',CAPS['operations']),('coexisting_bytes',CAPS['memory_bytes']),
         ('output_bytes',CAPS['output_bytes']),('scratch_bytes',config['native_config']['scratch_bytes'])))
        and type(details['operations']) is int and 0 <= details['operations'] <= admission['operations'], 'PQ native admitted resource bounds')
    def bound(descriptor, name):
        require(descriptor == dict(path=str(original/name),**file_pin(root/name)), 'PQ sealed full-body pin: '+name)
    freeze_name = 'screen/report.pq-residual-freeze.json'
    bound(details['freeze'],freeze_name)
    freeze = decode(read(root/freeze_name))
    require(freeze['schema'] == 'borsuk-pq-residual-freeze-v1'
        and freeze['config_sha256'] == config['native_config_sha256'] and freeze['source_identity_sha256'] == SOURCE_ID
        and freeze['requests_opened'] is freeze['truth_opened'] is False
        and freeze['all_results_sealed_before_reduction'] is True, 'PQ native pre-reduction closure')
    for field in ('selection','anchors','results'):
        bound(freeze[field],f'screen/report.pq-residual-{"selections" if field == "selection" else field}.json')
    generations = [n for n in SQ4_OUTPUTS if re.search(r'pq-residual-[01]-',n)]
    require(freeze['artifacts'] == [dict(path=str(original/n),**file_pin(root/n)) for n in generations]
        and freeze['generations'] == [dict(path=str(original/n),**file_pin(root/n)) for n in generations if n.endswith('-root.json')],
        'PQ exact ten generation artifacts/two roots')
    for i,panel in enumerate(config['native_config']['panels']):
        generation = decode(read(root/f'screen/report.pq-residual-{i}-root.json'))
        require(generation['schema'] == 'borsuk-pq-residual-source-generation-v1'
            and generation['codec'] == report['codec'] and generation['trainer'] == 'extrema-f64-endpoint256-even-dp16-nearest-lowest-v1'
            and generation['config_sha256'] == config['native_config_sha256'] and generation['source_identity_sha256'] == SOURCE_ID
            and generation['source_root'] == panel['root'] and generation['requests_opened'] is generation['truth_opened'] is False
            and generation['rows'] == 100000 and generation['dimensions'] == 768
            and generation['source_passes_authenticated'] == 3 and generation['encoded_rows'] == 4096, 'PQ native generation/source binding')
        for field,offset in (('source_groups',1),('source_order',2),('source_pq',3),('source_records',4)):
            d = config['inputs'][i*5+offset]
            require(generation[field] == dict(path=d['destination'],bytes=d['bytes'],sha256=d['sha256']), 'PQ original opaque source descriptor')
        for field,suffix in (('book','book.bin'),('cohort_residual_groups','groups.bin'),('cohort_native','sq8.bin'),('cohort_residual','cohort.bin')):
            bound(generation[field],f'screen/report.pq-residual-{i}-{suffix}')
    return dict(valid_diagnostic=True,status=report['status'],standalone_authority=False,
        requires_matching_supervisor_exit_receipt=True,quality_or_performance_claim=False,
        native_config_sha256=config['native_config_sha256'],binary={k:config['binary'][k] for k in ('bytes','sha256')},
        report_sha256=sha(body),freeze=details['freeze'])


def validate_corrected_outputs(root, config, original, report, body):
    native, details = config['native_config'], report['details']
    truth = [p['truth'] for p in native['panels']]
    require(details['rows'] == 100000 and details['dimensions'] == 768 and details['truth'] == truth
            and details['caps'] == CAPS and details['frozen_original_authority'] is True
            and details['whole_process_supervisor_required'] is True
            and 0 <= details['modeled_peak_bytes'] <= CAPS['memory_bytes']
            and 79200000 <= details['modeled_output_bytes'] <= CAPS['output_bytes'], 'corrected frozen geometry/resources')
    require(details['construction_limit'] == native['construction_operations']
            and details['construction_work_bound'] == 159001952256
            and 0 <= details['construction_charged_work'] <= details['construction_work_bound'] <= details['construction_limit']
            and details['query_auth_limit'] == native['query_auth_operations']
            and 0 <= details['query_auth_operations'] <= details['query_auth_work_bound'] <= details['query_auth_limit']
            and details['query_auth_work_bound'] >= corrected_query_floor(details['modeled_output_bytes'])
            and [details[k] for k in ('candidate_query_charged_work','decoded_cosine_control_charged_work',
                'unchanged_sq8_control_charged_work')] == [15733844912,16649896880,11326600896]
            and details['charged_work_is_conservative_bound'] is True
            and details['actual_dense_encoding_macs'] == 117964800000, 'corrected separate construction/three-arm/auth work')
    for k in ('total_cold_get_claim','total_cold_byte_claim','lifecycle_qualified','production_snapshot_integration',
              'finite_Haar_unbiasedness_claim','radial_causation_claim'):
        require(details[k] is False, 'corrected scope: '+k)
    require(details['payload_ranges_exclude_startup'] is True, 'corrected startup separate from payload')
    def authenticate(descriptor, suffix):
        name = 'screen/report.corrected-four-bit-'+suffix
        require(descriptor == dict(path=str(original/name), **file_pin(root/name)), 'corrected closure: '+suffix)
        return root/name
    freeze = decode(read(authenticate(details['freeze'], 'freeze.json')))
    require(freeze['schema'] == SQ4_SCHEMA+'-freeze-v1' and freeze['config_sha256'] == config['native_config_sha256']
            and freeze['source_identity_sha256'] == SOURCE_ID and freeze['truth_opened'] is False
            and freeze['original_seal'] == native['original_seal'] and freeze['truth'] == truth
            and freeze['closed_populations'] == native['closed_populations'], 'corrected whole128 pretruth freeze')
    rotation = freeze['rotation']
    path = authenticate(rotation, 'rotation.bin')
    require(rotation['bytes'] == 64+8*768**2, 'corrected persisted full rotation geometry')
    with regular(path) as source:
        require(source.read(64) == b'BORSUK-C4ROT-v1\0'+(768).to_bytes(8,'little')+bytes(40), 'corrected rotation header')
    seal = decode(read(authenticate(freeze['payload_seal'], 'payloads.json')))
    authenticate(freeze['nomination_prefix'], 'prefix.jsonl')
    require(file_pin(root/'screen/report.corrected-four-bit-prefix.jsonl') == {k:native['prefix'][k] for k in ('bytes','sha256')}, 'corrected original nominations')
    require(seal['schema'] == SQ4_SCHEMA+'-payload-seal-v1' and seal['config_sha256'] == config['native_config_sha256']
            and seal['source_identity_sha256'] == SOURCE_ID and seal['rotation'] == rotation
            and seal['queries_opened'] is seal['truth_opened'] is False
            and seal['payloads'] == freeze['payloads'] and len(seal['payloads']) == 2, 'corrected paired persisted seal')
    generations = []
    for i,payload in enumerate(seal['payloads']):
        generation = decode(read(authenticate(payload['root'], str(i)+'-root.json')))
        authenticate(payload['payload'], str(i)+'.bin'); authenticate(payload['groups'], str(i)+'-groups.bin')
        require(generation['schema'] == SQ4_SCHEMA+'-generation-v1' and generation['codec'] == SQ4_CODEC
                and generation['config_sha256'] == config['native_config_sha256'] and generation['source_identity_sha256'] == SOURCE_ID
                and generation['rotation'] == rotation and generation['payload'] == payload['payload'] and generation['groups'] == payload['groups']
                and generation['original_root'] == payload['original_root'] == native['panels'][i]['root']
                and generation['rows'] == 100000 and generation['dimensions'] == 768 and generation['row_bytes'] == 396
                and payload['payload']['bytes'] == 39600000 and payload['groups']['bytes'] == 200000, 'corrected generation bindings')
        for key, offset in (('original_records',5),('original_order',3),('original_groups',2),('original_graph',1),('original_pq',4)):
            p,n,h = INPUT_PINS[i*8+offset]
            require(generation[key] == dict(path=p,bytes=n,sha256=h), 'corrected original descriptor: '+key)
        identity = generation['router_identity']
        require(identity['rows'] == 100000 and identity['dimensions'] == 768, 'corrected original router geometry')
        require(len(generation['low_bits']) == len(generation['step_bits']) == 768
                and all(type(b) is int and 0 <= b < 2**32 for b in generation['low_bits']+generation['step_bits']), 'corrected original coefficient bits')
        generations.append(generation)
    require(len(freeze['results']) == 128, 'corrected all128 sealed results')
    envelopes = True
    for i, descriptor in enumerate(freeze['results']):
        query = decode(read(authenticate(descriptor, 'result-'+str(i)+'.json'),8*1024**2))
        candidate, cosine, reference, plan = (query[k] for k in ('corrected','decoded_cosine','sq8_reference','plan'))
        require(query['schema'] == SQ4_SCHEMA+'-query-v1' and query['dataset'] == native['panels'][i//64]['dataset']
                and type(query['ordinal']) is int and query['ordinal'] == i%64
                and query['generation'] == seal['payloads'][i//64]['root'] and query['rotation'] == rotation
                and query['mutation_revision'] == 0 and query['nominees_retained'] is query['original_cover_contained'] is True
                and query['truth_opened'] is False and candidate['fetched_ids'] == cosine['fetched_ids'] == reference['fetched_ids'], 'corrected same-population three-arm query')
        rows = sum(r['end']-r['start'] for r in plan['row_ranges'])
        old = CORRECTED_POPULATIONS['panels'][i//64]['queries'][i%64]
        require(plan['row_ranges'] == old['row_ranges'] and rows == old['fetched_rows']
                and plan['query_sha256'] == old['query_sha256']
                and sha(b''.join(v.to_bytes(8,'little',signed=True) for v in candidate['fetched_ids'])) == old['fetched_ids_sha256']
                and sha(b''.join(v.to_bytes(8,'little') for v in plan['nominees'])) == old['nominee_ordinals_sha256'],
                'corrected closed ordered population bindings')
        require(type(candidate['range_reads']) is int and 1 <= candidate['range_reads'] == len(plan['row_ranges']) <= 32
                and candidate['verified_bytes'] == plan['candidate_bytes'] == rows*396
                and cosine['verified_bytes'] == reference['verified_bytes'] == rows*780
                and len(candidate['fetched_ids']) == rows, 'corrected exact query payload accounting')
        fits = candidate['verified_bytes'] <= 16*1024**2
        require(plan['envelope_fits'] is fits, 'corrected query envelope'); envelopes &= fits
    require(details['all128_envelopes_fit'] is envelopes and len(details['summaries']) == 2, 'corrected envelope summary')
    quality = True
    for panel, summary in zip(native['panels'],details['summaries']):
        require(summary['dataset'] == panel['dataset'], 'corrected summary panel')
        for arm in ('corrected','decoded_cosine','sq8_reference','fetched_coverage'):
            hits = summary[arm]['hits']
            require(len(hits) == 64 and all(type(v) is int and 0 <= v <= 100 for v in hits)
                    and summary[arm]['total_hits'] == sum(hits) and summary[arm]['mean_recall'] == sum(hits)/6400
                    and summary[arm]['p05_hits'] == sorted(hits)[3], 'corrected summary arithmetic')
        quality &= summary['corrected']['total_hits'] >= 6272 and summary['corrected']['p05_hits'] >= 95
    require(report['status'] == ('SURVIVED_CONSUMED_PANELS' if quality and envelopes else 'REJECT'), 'corrected completed disposition')
    startup = decode(read(authenticate(details['startup'], 'startup.json')))
    objects = startup['distinct_objects']
    expected = [native['original_seal'],native['prefix'],native['closed_populations'],rotation]
    for generation in generations:
        expected += [generation[k] for k in ('original_root','original_graph','original_pq','original_order','original_groups','groups')]
    expected += [p['root'] for p in seal['payloads']]
    require(startup['counted_once'] is True and len(objects) == len({o['artifact']['path'] for o in objects})
            and sorted((o['artifact'] for o in objects),key=lambda a:a['path']) == sorted(expected,key=lambda a:a['path'])
            and all(type(o['read_attempts']) is int and o['read_attempts'] > 0 for o in objects)
            and startup['count'] == details['startup_distinct_objects'] == len(objects)
            and startup['bytes'] == details['startup_distinct_bytes'] == sum(o['artifact']['bytes'] for o in objects)
            and startup['read_attempts'] == sum(o['read_attempts'] for o in objects)
            and startup['read_bytes'] == sum(o['read_attempts']*o['artifact']['bytes'] for o in objects)
            and startup['separate_pretruth_reauthentication_reads'] == 153, 'corrected distinct startup ledger')
    return dict(status=report['status'],valid_diagnostic=True,report_sha256=sha(body),
        native_config_sha256=config['native_config_sha256'],binary={k:config['binary'][k] for k in ('bytes','sha256')},
        summaries=details['summaries'],all128_envelopes_fit=envelopes,quality_or_performance_claim=False,
        startup=startup,construction_charged_work=details['construction_charged_work'],query_auth_operations=details['query_auth_operations'])


def validate_result(root, config):
    if CANARY:
        return validate_canary_result(root, config)
    if SQ4:
        return validate_sq4_result(root, config)
    validate_config(config)
    validate_abi(decode(read(root/'runtime-abi.json')))
    require(file_pin(root/'native-config.json') == pin(encoded(config['native_config'])), 'native config bytes')
    require(file_pin(root/BINARY_NAME) == BINARY_PIN, 'executed binary bytes')
    exit_receipt = decode(read(root/'native-exit.json'))
    resource, cleanup = (decode(read(root/name)) for name in ('resources.json', 'cleanup.json'))
    require(type(exit_receipt['process_exit_code']) is int and exit_receipt['process_exit_code'] == 0
            and exit_receipt['process_started'] is True and 'error' not in exit_receipt, 'original native exit zero')
    command = exit_receipt['command']
    expected_root = Path(command[0]).parent.parent
    require(command == [str(expected_root/BINARY_NAME), 'check-fine-pack', str(expected_root/'native-config.json'),
                config['native_config_sha256'], str(expected_root/'screen/report.json')]
            and exit_receipt['config_sha256'] == config['native_config_sha256']
            and exit_receipt['binary'] == config['binary'], 'original executable/config/command binding')
    validate_resources(resource)
    require(all(cleanup.get(k) is True for k in ('drain_complete', 'cleanup_complete', 'output_durable')), 'native cleanup/durability')
    stage_receipt = decode(read(root/'stage-receipt.json'))
    require(stage_receipt == dict(binary=BINARY_PIN, native_config=pin(encoded(config['native_config'])),
            inputs={d['destination']:{k:d[k] for k in ('bytes', 'sha256')} for d in config['inputs']},
            exact_six_inputs=True, compiler_used=False), 'authenticated original staging')
    body = read(root/'screen/report.json', CAPS['output_bytes'])
    report = decode(body)
    require(report['schema'] == 'borsuk-fine-pack-diagnostic-v1' and report['complete'] is True
            and type(report['queries']) is int and report['queries'] == 128
            and report['config_sha256'] == config['native_config_sha256']
            and report['diagnostic_source_sha256'] == DIAGNOSTIC_SOURCES, 'native complete report binding')
    for key, expected in (('standalone_authority', False), ('requires_matching_supervisor_exit_receipt', True),
            ('quality_or_performance_claim', False), ('truth_opened', False), ('requests_opened', False),
            ('sq8_canonical_pq_bodies_opened', False)):
        require(report[key] is expected, 'native report scope: '+key)
    details = report['details']
    require(details['original_seal'] == config['native_config']['original_seal']
            and details['original_seal_bytes_sha256'] == INPUT_PINS[4][2]
            and details['caps'] == CAPS and 0 <= details['operations'] <= CAPS['operations']
            and 0 <= details['wall_ms'] <= 300000, 'native method/resources binding')
    for key, name in (('permutation_seal', 'screen/report.permutations.json'), ('prefix', 'screen/report.prefix.jsonl')):
        require(details[key] == dict(file_pin(root/name), path=str(expected_root/name)), 'native companion pin: '+key)
    require(file_pin(root/'screen/report.prefix.jsonl') == {k:config['native_config']['prefix'][k] for k in ('bytes', 'sha256')}, 'consumed prefix')
    permutation = decode(read(root/'screen/report.permutations.json', CAPS['output_bytes']))
    native = config['native_config']
    require(permutation['schema'] == 'borsuk-fine-pack-permutations-v1'
            and permutation['config_sha256'] == config['native_config_sha256']
            and permutation['diagnostic_source_sha256'] == DIAGNOSTIC_SOURCES
            and permutation['original_seal'] == native['original_seal']
            and permutation['nomination_prefix'] == native['prefix']
            and permutation['panels'] == native['panels'] and permutation['group_rows'] == 16
            and permutation['groups_per_pack'] == 42 and permutation['query_blind'] is True
            and permutation['original_trace_source_identity_sha256'] == details['original_trace_source_identity_sha256'], 'joint permutation binding')
    require(len(permutation['old_to_new']) == len(permutation['permutation_sha256_le_u32']) == 2, 'both permutations')
    for mapping, digest in zip(permutation['old_to_new'], permutation['permutation_sha256_le_u32']):
        require(len(mapping) == 6250 and all(type(v) is int for v in mapping)
                and sorted(mapping) == list(range(6250)), 'group bijection')
        require(sha(b''.join(v.to_bytes(4, 'little') for v in mapping)) == digest, 'permutation LE32 hash')
    replay = details['replay']
    require(len(replay['queries']) == 128, 'all 128 plans completed')
    passes, maxima = [0, 0], [0, 0]
    for index, q in enumerate(replay['queries']):
        panel = index//64
        require(q['dataset'] == native['panels'][panel]['dataset'] and q['ordinal'] == index%64
                and q['all_nominees_retained'] is True and 1 <= q['nominees'] <= 1024, '128 frozen plan identities')
        ranges = q['ranges']
        require(1 <= len(ranges) <= 32, 'explicit cover cap32')
        end, count = 0, 0
        for r in ranges:
            require(set(r) == {'start', 'end'} and all(type(r[k]) is int for k in r)
                    and end <= r['start'] < r['end'] <= 100000*780
                    and r['start']%780 == r['end']%780 == 0, 'cover range geometry')
            end = r['end']; count += r['end']-r['start']
        fits = count <= 16*1024**2
        require(type(q['planned_bytes']) is int and q['planned_bytes'] == count and q['fits'] is fits, 'cover bytes/disposition')
        passes[panel] += fits; maxima[panel] = max(maxima[panel], count)
    survived = passes == [64, 64]
    require(replay['per_panel_passes'] == passes and replay['per_panel_max_bytes'] == maxima
            and replay['survived'] is survived and report['status'] == ('SURVIVED_NECESSARY_LOCALITY' if survived else 'REJECT'), 'completed locality disposition')
    require(sum(file_pin(root/n)['bytes'] for n in ARTIFACTS if n.startswith('screen/')) <= CAPS['output_bytes'], 'native total output cap')
    return dict(status=report['status'], valid_diagnostic=True, report_sha256=sha(body),
                native_config_sha256=config['native_config_sha256'], binary=BINARY_PIN,
                per_panel_passes=passes, per_panel_max_bytes=maxima, quality_or_performance_claim=False)


def validate_canary_result(root, config):
    validate_config(config)
    sq4_qualification(config,root,collected=True)
    validate_abi(decode(read(root/'runtime-abi.json')))
    ref = config['canary']['science_authority']; raw = read(root/'science-config.json')
    require(pin(raw) == {k:ref[k] for k in ('bytes','sha256')} and decode(raw) == config['canary']['science_config'], 'canary science reference replay')
    require(decode(read(root/'imports.json')) == config['code_sha256']
        and decode(read(root/'sentinels.json')) == dict(panel_gt_root=str(SQ4_INPUT_ROOT),fail_fast_open_sentinel=True,native_usage_only=True),
        'canary real imports and no-panel/GT sentinel')
    require(file_pin(root/BINARY_NAME) == CANARY_BINARY_PIN
        and file_pin(root/'native-config.json') == pin(encoded(config['native_config']))
        and file_pin(root/'metadata/closed-populations.json') == dict(bytes=175929,sha256=CORRECTED_EVIDENCE[-1][2]), 'canary executed/staged exact bytes')
    receipt,resource,cleanup,scratch,stage_receipt = (decode(read(root/n)) for n in
        ('native-exit.json','resources.json','cleanup.json','scratch.json','stage-receipt.json'))
    command = receipt['command']; original = Path(command[0]).parent.parent
    require(original.is_absolute() and command == [str(original/BINARY_NAME),SQ4_CLI]
        and receipt['binary'] == config['binary'] and receipt['config_sha256'] == config['native_config_sha256']
        and receipt['process_started'] is True and type(receipt['process_exit_code']) is int and receipt['process_exit_code'] == 2
        and 'error' not in receipt, 'canary original exact usage CLI/exit2')
    log = read(root/'native.log',2*1024**2)
    require(b'usage: hierarchical_semantic_cells check-fine-corrected-four-bit CONFIG CONFIG_SHA256 NEW_REPORT_JSON' in log,
        'canary actual loader and usage diagnostic')
    require(not (root/'screen').exists() or not any((root/'screen').iterdir()), 'canary forbids native science output')
    validate_resources(resource)
    require(all(cleanup.get(k) is True for k in ('drain_complete','cleanup_complete','output_durable')), 'canary cleanup/durability')
    expected = [dict(key=d['key'],bytes=d['bytes'],sha256=d['sha256']) for d in (*config['inputs'],config['binary'])]
    require([{k:h[k] for k in ('key','bytes','sha256')} for h in stage_receipt['heads']] == expected
        and stage_receipt['canary_metadata_only'] is True and stage_receipt['dataset_body_gets'] == 0
        and stage_receipt['selected_get_keys'] == [config['binary']['key'],config['inputs'][-1]['key']]
        and stage_receipt['binary'] == CANARY_BINARY_PIN and stage_receipt['compiler_used'] is False
        and stage_receipt['native_config'] == pin(encoded(config['native_config']))
        and stage_receipt['inputs'] == {config['inputs'][-1]['destination']:dict(bytes=175929,sha256=CORRECTED_EVIDENCE[-1][2])}, 'canary HEAD20/GET2/noGT transport')
    admission = stage_receipt['scratch']; reserve = config['fixed']['scratch']
    require(scratch['closed'] is True and scratch['cap_exceeded'] is False and 'error' not in scratch
        and scratch['run_id'] == receipt['run_id'] and scratch['process_exit_code'] == 2
        and scratch['config_sha256'] == config['native_config_sha256'] and scratch['cap_bytes'] == SQ4_SCRATCH_CAP
        and scratch['admission'] == admission and admission['reserve'] == reserve
        and admission['config_sha256'] == config['native_config_sha256']
        and admission['charged_bytes'] == sum(v for k,v in reserve.items() if k!='cap_bytes') <= admission['free_bytes_before']
        and admission['projected_peak_bytes'] <= SQ4_SCRATCH_CAP
        and admission['source_archive_observed_bytes'] <= reserve['source_archive_bytes']
        and admission['bootstrap_observed_bytes'] <= reserve['bootstrap_bytes']
        and admission['bootstrap']['closed'] is True and admission['bootstrap']['cap_exceeded'] is False
        and admission['bootstrap']['reserve'] == reserve and admission['bootstrap']['sample_count'] >= 2
        and scratch['sample_count'] >= 2 and scratch['interval_seconds'] == 1
        and scratch['closure_reserve_bytes'] == SQ4_CLOSURE_RESERVE
        and scratch['projected_closure_peak_bytes'] == scratch['last']['whole_scratch_bytes']+SQ4_CLOSURE_RESERVE <= SQ4_SCRATCH_CAP
        and 0 <= scratch['last']['whole_scratch_bytes'] <= scratch['peak_bytes'] <= SQ4_SCRATCH_CAP
        and set(scratch['last']['roots']) == {str(original),str(SQ4_INPUT_ROOT)}
        and sum(scratch['last']['roots'].values()) == scratch['last']['whole_scratch_bytes'], 'canary observed whole scratch closure')
    return dict(status='GO',valid_diagnostic=True,infrastructure_only=True,original_exit_code=2,
        native_config_sha256=config['native_config_sha256'],binary=CANARY_BINARY_PIN,
        binding=science_binding(config['canary']['science_config']),quality_or_performance_claim=False)


def publish(s3, root, prefix, terminal):
    """Retain every present raw body, read back, then publish the terminal."""
    if SQ4:
        scratch_room(root, SQ4_CLOSURE_RESERVE)
    artifacts = {}
    for name in ARTIFACTS:
        path = root/name
        if path.exists():
            body = read(path, 64*1024**2)
            ident = pin(body)
            key = prefix+'/artifacts/'+name
            s3.put_object(Bucket=BUCKET, Key=key, Body=body, IfNoneMatch='*')
            response = s3.get_object(Bucket=BUCKET, Key=key)
            with response['Body'] as source:
                require(pin(source.read(ident['bytes']+1)) == ident, 'upload readback: '+name)
            artifacts[name] = ident
    terminal['artifacts'] = artifacts
    if SQ4:
        observation = scratch_room(root, 4*1024**2)
        terminal['scratch_closure'] = dict(before=observation, cap_bytes=SQ4_SCRATCH_CAP,
            reserve_bytes=4*1024**2, projected_peak_bytes=observation['whole_scratch_bytes']+4*1024**2)
    if CANARY:
        terminal['transport'] = dict(s3.canary_attempts,dispatch_attempts=s3.canary_attempts['dispatch_attempts']+1,
            unobserved_scopes=['bootstrap','root_ec2','root_control','wire_requests','billed_requests'],automatic_retry=False)
        require(terminal['transport']['dispatch_attempts'] <= 128, 'canary worker S3 dispatch/closure reserve')
    raw = encoded(terminal)
    if SQ4:
        require(len(raw)+4096 <= 4*1024**2, 'SQ4 terminal closure reserve')
        scratch_room(root, 4*1024**2)
    write(root/'terminal.json', raw)
    if SQ4:
        scratch_room(root, 0)  # The successful S3 marker follows the actual final scan.
    s3.put_object(Bucket=BUCKET, Key=prefix+'/terminal.json', Body=raw, IfNoneMatch='*')
    return terminal


def remote(repo, root, commit, archive_sha, prefix, config_sha):
    import urllib.request
    import boto3
    from botocore.config import Config
    require(re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix), 'remote prefix')
    sdk_guard()
    s3 = boto3.client('s3', region_name=REGION, config=Config(retries={'total_max_attempts':1}))
    request = urllib.request.Request('http://169.254.169.254/latest/api/token', method='PUT',
                                    headers={'X-aws-ec2-metadata-token-ttl-seconds':'60'})
    with urllib.request.urlopen(request, timeout=5) as response:
        token = response.read(4096).decode()
    request = urllib.request.Request('http://169.254.169.254/latest/meta-data/instance-id',
                                    headers={'X-aws-ec2-metadata-token':token})
    with urllib.request.urlopen(request, timeout=5) as response:
        instance_id = response.read(128).decode()
    terminal = dict(schema=SCHEMA, source_commit=commit, source_archive_sha256=archive_sha,
        instance_id=instance_id, prefix=prefix, config_sha256=config_sha,
        status='failed', phase='execution', exit_code=96, original_exit_code=None,
        disposition='INVALID', artifact_roster_sha256=ROSTER_SHA)
    try:
        abi = runtime_abi(); write(root/'runtime-abi.json', encoded(abi)); validate_abi(abi)
        config_body = read(repo/CONFIG)
        require(sha(config_body) == config_sha, 'root config authentication')
        config = decode(config_body); validate_config(config, repo)
        proof = preflight(repo)
        write(root/'config.json', config_body); write(root/'source-qualification.json', encoded(proof))
        if SQ4:
            for r in config['native_qualification']:
                write(root/'qualification'/Path(r['path']).name, read(repo/r['path']))
        if CANARY:
            write(root/'science-config.json',read(repo/config['canary']['science_authority']['path']))
            write(root/'imports.json',encoded(canary_imports(repo,config)))
            write(root/'sentinels.json',encoded(canary_sentinels()))
            s3 = canary_transport(s3,config,prefix)
        elif CORRECTED_FOUR_BIT:
            write(root/'canary-admission.json',read(repo/config['canary_admission']['path']))
        elif CO_SELECTION_LAYOUT:
            write(root/'imports.json',encoded(canary_imports(repo,config)))
        write(root/'cpu.txt', subprocess.check_output(['lscpu']))
        stage(s3, config, root)
        receipt = supervise(config, root, run_id=prefix+'/'+instance_id)
        terminal['original_exit_code'] = receipt['process_exit_code']
        result = validate_result(root, config)
        terminal.update(status='complete', phase='complete', exit_code=0, disposition=result['status'],
                        result=result, qualification=proof)
    except BaseException as error:
        terminal['error'] = dict(type=type(error).__name__, message=str(error))
    finally:
        if (root/'native-exit.json').exists():
            terminal['original_exit_code'] = decode(read(root/'native-exit.json'))['process_exit_code']
        closed_log = read(root/'run.log', 4*1024**2) if SQ4 else read(root/'run.log')
        if SQ4:
            scratch_room(root, SQ4_CLOSURE_RESERVE)
        write(root/'run-closed.log', closed_log)
        if CANARY and not hasattr(s3,'canary_attempts'):
            # Early ABI/config/import refusal may publish only this attempt's receipts.
            s3 = canary_transport(s3,dict(binary=dict(key='refused/binary'),inputs=[dict(key='refused/metadata')]),prefix)
        publish(s3, root, prefix, terminal)
    return terminal['exit_code']


def replay(out):
    terminal = decode(read(out/'aws-terminal.json'))
    reservation, launch, close = (decode(read(out/name)) for name in ('aws-reservation.json', 'aws-launch.json', 'aws-closeout.json'))
    require(close['state'] == 'terminated' and close['nodes'] == launch['nodes']
            == {'0':{'instance_id':launch['instance_id']}}, 'same acknowledged instance terminated/waited')
    require(terminal['schema'] == reservation['schema'] == SCHEMA
            and terminal['instance_id'] == launch['instance_id'] and terminal['prefix'] == launch['prefix']
            and re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', terminal['prefix']), 'terminal attempt/instance identity')
    for key in ('source_commit', 'source_archive_sha256'):
        require(terminal[key] == reservation[key] == launch[key], 'terminal source binding: '+key)
    require(terminal['config_sha256'] == reservation['config_sha256']
            and terminal['artifact_roster_sha256'] == ROSTER_SHA, 'terminal frozen config/roster')
    require(set(terminal['artifacts']) <= set(ARTIFACTS), 'terminal artifact roster')
    for name, identity in terminal['artifacts'].items():
        body_pin(identity)
        require(file_pin(out/name) == identity, 'collected full artifact hash: '+name)
        if (out/(name+'.gz')).exists():
            with regular(out/(name+'.gz')) as compressed, gzip.GzipFile(fileobj=compressed) as source:
                require(pin(source.read(identity['bytes']+1)) == identity, 'transport gzip hash: '+name)
    if terminal['status'] != 'complete':
        return dict(valid_diagnostic=False, status='INVALID', original_exit_code=terminal['original_exit_code'])
    require(terminal['phase'] == 'complete' and type(terminal['exit_code']) is int
            and type(terminal['original_exit_code']) is int
            and terminal['exit_code'] == 0 and terminal['original_exit_code'] == (2 if CANARY else 0)
            and set(terminal['artifacts']) == set(ARTIFACTS), 'complete original terminal/roster')
    raw = read(out/'config.json')
    require(sha(raw) == terminal['config_sha256'], 'collected config hash')
    config = decode(raw); validate_config(config)
    if SQ4:
        closure = terminal['scratch_closure']
        require(closure['cap_bytes'] == SQ4_SCRATCH_CAP and closure['reserve_bytes'] == 4*1024**2
                and closure['projected_peak_bytes'] == closure['before']['whole_scratch_bytes']+closure['reserve_bytes'] <= SQ4_SCRATCH_CAP
                and sum(closure['before']['roots'].values()) == closure['before']['whole_scratch_bytes'], 'SQ4 final terminal scratch closure')
    if CANARY:
        transport = terminal['transport']
        require(transport['limit'] == 128 and type(transport['dispatch_attempts']) is int
            and 0 < transport['dispatch_attempts'] <= 128 and transport['scope'] == 'wrapped-worker-s3-sdk-dispatches'
            and transport['unobserved_scopes'] == ['bootstrap','root_ec2','root_control','wire_requests','billed_requests']
            and transport['automatic_retry'] is False, 'canary one attempt worker S3 dispatch cap/scope')
    proof = decode(read(out/'source-qualification.json'))
    require(proof == reservation['qualification'] == terminal['qualification']
            and proof['config_sha256'] == sha(raw) and proof['code_sha256'] == config['code_sha256']
            and proof['native_qualification'] == config['native_qualification'], 'original qualification binding')
    result = validate_result(out, config)
    require(decode(read(out/'native-exit.json'))['run_id'] == terminal['prefix']+'/'+terminal['instance_id'], 'original run ID')
    require(result == terminal['result'] and terminal['disposition'] == result['status'], 'original result receipt')
    return result


def collect(s3, prefix, out, instance_id, commit, digest):
    if CANARY or PQ_RESIDUAL_SOURCE or CO_SELECTION_LAYOUT:
        launch,close = (decode(read(out/n)) for n in ('aws-launch.json','aws-closeout.json'))
        require(close['state'] == 'terminated' and close['nodes'] == launch['nodes'] == {'0':{'instance_id':instance_id}}
            and launch['instance_id'] == instance_id, 'canary collection requires SAME acknowledged ID terminated/waited')
    response = s3.get_object(Bucket=BUCKET, Key=prefix+'/terminal.json')
    with response['Body'] as source:
        raw = source.read(4*1024**2+1)
    require(len(raw) <= 4*1024**2, 'terminal cap')
    write(out/'aws-terminal.json', raw)
    terminal = decode(raw)
    require(terminal['instance_id'] == instance_id and terminal['source_commit'] == commit
            and terminal['source_archive_sha256'] == digest and terminal['prefix'] == prefix
            and terminal['schema'] == SCHEMA and set(terminal['artifacts']) <= set(ARTIFACTS), 'original terminal identity')
    if PQ_RESIDUAL_SOURCE or CO_SELECTION_LAYOUT:
        require(terminal['artifact_roster_sha256'] == ROSTER_SHA
            and sum(body_pin(v)['bytes'] for n,v in terminal['artifacts'].items() if n.startswith('screen/')) <= CAPS['output_bytes'],
            'PQ bounded original native collection roster')
    for name, identity in terminal['artifacts'].items():
        body_pin(identity)
        response = s3.get_object(Bucket=BUCKET, Key=prefix+'/artifacts/'+name)
        with response['Body'] as source:
            body = source.read(identity['bytes']+1)
        require(pin(body) == identity, 'remote full artifact hash: '+name)
        write(out/name, body); write(out/(name+'.gz'), gzip.compress(body, mtime=0))
    result = replay(out)
    write(out/'collection-replay.json', encoded(result))
    if CANARY and result['status'] == 'GO':
        write(out/'canary-admission.json',encoded(dict(schema='borsuk-corrected-four-bit-canary-admission-v1',
            status='GO',binding=result['binding'],original_exit_code=2,instance_id=instance_id,prefix=prefix,
            canary_schema=SCHEMA,source_commit=commit,source_archive_sha256=digest,
            closeout=decode(read(out/'aws-closeout.json')),terminal=file_pin(out/'aws-terminal.json'),
            reservation=file_pin(out/'aws-reservation.json'),launch=file_pin(out/'aws-launch.json'),
            config=file_pin(out/'config.json'),source_qualification=file_pin(out/'source-qualification.json'))))
    return terminal


def setup_self_check():
    """Filesystem controller model: refuse before exec, never touch shared roots."""
    import tempfile
    from unittest.mock import patch
    with tempfile.TemporaryDirectory(prefix='fine-pack-delegation-check-') as tmp:
        root=Path(tmp)/'cgroup';parent=root/'system.slice'/(SUPERVISOR_UNIT+'.service')
        observer=parent/'supervisor';observer.mkdir(parents=True)
        proc=Path(tmp)/'proc';proc.write_text('0::/system.slice/'+SUPERVISOR_UNIT+'.service/supervisor\n')
        initial={'cgroup.controllers':'cpu memory pids\n','cgroup.subtree_control':'',
                 'cgroup.procs':'','cgroup.type':'domain\n'}
        for name,body in initial.items():(parent/name).write_text(body)
        (observer/'cgroup.procs').write_text(str(os.getpid()))
        original_write=Path.write_text
        writes=[]
        def enable(path,body,*args,**kwargs):
            writes.append(str(path))
            require(path==parent/'cgroup.subtree_control','only dedicated parent controller write')
            require(body=='+cpu +memory +pids','exact required controllers')
            return original_write(path,'cpu memory pids\n',*args,**kwargs)
        with patch.object(Path,'write_text',enable):
            record={};group=delegated_group(record,proc,root)
        require(group==parent/'native' and writes==[str(parent/'cgroup.subtree_control')]
                and record['enabled']==['cpu','memory','pids'],'delegated controller activation')
        for name,body,label in [('cgroup.controllers','memory pids\n','CPU unavailable'),
                               ('cgroup.procs',str(os.getpid()),'internal observer'),
                               ('cgroup.type','domain threaded\n','threaded domain')]:
            old=(parent/name).read_text();(parent/name).write_text(body)
            with patch.object(Path,'write_text',side_effect=AssertionError('unsafe controller write')):
                try:delegated_group({},proc,root)
                except ValueError:pass
                else:raise AssertionError('accepted '+label)
            (parent/name).write_text(old)
        proc.write_text('0::/system.slice/cloud-final.service\n')
        try:delegated_group({},proc,root)
        except ValueError:pass
        else:raise AssertionError('undelegated shared hierarchy accepted')
        group.mkdir()
        for name in ('memory.max','memory.swap.max'):(group/name).write_text('0')
        try:create_group(group)
        except ValueError as error:require('cpu.max' in str(error),'missing controller diagnostic')
        else:raise AssertionError('missing native CPU interface accepted')
    print('PASS delegation model: explicit controllers; unavailable/internal-PID/threaded/shared-parent/missing-interface refusals before exec')


def self_check(real_cgroup=False):
    """Real fake-native original processes; cgroup, SDK, IMDS and AWS mocked.

    Run under CPU1/256MiB/swap0/120s. No corpus, graph, native ANN or network.
    """
    import copy
    import io
    import tempfile
    from datetime import datetime, timezone
    from unittest.mock import Mock, patch
    from contextlib import ExitStack
    module = sys.modules[__name__]
    shared = lifecycle()
    setup_self_check()
    # Actual installed service model, no client/credentials/network.
    require(sdk_guard() == dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True), 'real installed immutable SDK model')
    import botocore.session
    old_model=Mock()
    old_model.get_service_model.return_value.operation_model.return_value.input_shape.members={}
    with patch.object(botocore.session,'get_session',return_value=old_model):
        try:
            sdk_guard()
        except ValueError as error:
            require('IfNoneMatch' in str(error),'old SDK model rejection')
        else:
            raise AssertionError('old model without immutable PUT accepted')
    old_path=Path('/tmp/borsuk-canary-botocore-1.34.46-s3.json')
    official_old_checked=False
    if old_path.exists():
        from botocore.model import ServiceModel
        old_body=read(old_path)
        require(sha(old_body)=='0e0760c9ae10e9f8af267b0d9708fefabbaec023370f0044bbecc51565f05699','authenticated official old service model')
        service=ServiceModel(decode(old_body),service_name='s3')
        session=Mock();session.get_service_model.return_value=service
        with patch.object(botocore.session,'get_session',return_value=session):
            try:sdk_guard()
            except ValueError as error:require('IfNoneMatch' in str(error),'official old model refused')
            else:raise AssertionError('official old SDK model accepted')
        official_old_checked=True
    fake = '''#!/usr/bin/env python3
import hashlib,json,os,sys,time
from pathlib import Path
_,mode,config_path,config_sha,out=sys.argv
assert mode=='check-fine-pack'
if os.environ.get('BORSUK_FINE_PACK_REAL_CGROUP')=='1':
 group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
 assert group.name=='native' and os.sched_getaffinity(0)=={0}
 assert int((group/'memory.max').read_text())==268435456
 assert int((group/'memory.swap.max').read_text())==0
 assert (group/'cpu.max').read_text().split()==['100000','100000']
 assert int((group/'pids.max').read_text())==32
c=json.loads(Path(config_path).read_bytes());p=Path(out)
def sha(b):return hashlib.sha256(b).hexdigest()
def emit(path,value):
 b=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
 with path.open('xb') as f:f.write(b);f.flush();os.fsync(f.fileno())
 return {'path':str(path),'bytes':len(b),'sha256':sha(b)}
sources=SOURCES
trace='0'*64
maps=[list(range(6250)),list(reversed(range(6250)))]
perms=emit(p.with_suffix('.permutations.json'),{'schema':'borsuk-fine-pack-permutations-v1',
 'config_sha256':config_sha,'diagnostic_source_sha256':sources,'old_to_new':maps,
 'permutation_sha256_le_u32':[sha(b''.join(v.to_bytes(4,'little') for v in m)) for m in maps],
 'original_trace_source_identity_sha256':trace,'original_seal':c['original_seal'],
 'nomination_prefix':c['prefix'],'panels':c['panels'],'group_rows':16,'groups_per_pack':42,'query_blind':True})
prefix=p.with_suffix('.prefix.jsonl');body=Path(c['prefix']['path']).read_bytes()
with prefix.open('xb') as f:f.write(body);f.flush();os.fsync(f.fileno())
prefix_pin={'path':str(prefix),'bytes':len(body),'sha256':sha(body)}
reject=os.environ.get('FINE_PACK_FAKE')=='reject'
queries=[]
for panel in c['panels']:
 for ordinal in range(64):
  size=780*22000 if reject and ordinal==0 else 780
  queries.append({'dataset':panel['dataset'],'ordinal':ordinal,'nominees':1,'all_nominees_retained':True,
   'ranges':[{'start':0,'end':size}],'planned_bytes':size,'fits':size<=16*1024**2})
emit(p,{'schema':'borsuk-fine-pack-diagnostic-v1','complete':True,'queries':128,
 'status':'REJECT' if reject else 'SURVIVED_NECESSARY_LOCALITY','config_sha256':config_sha,
 'diagnostic_source_sha256':sources,'standalone_authority':False,'requires_matching_supervisor_exit_receipt':True,
 'quality_or_performance_claim':False,'truth_opened':False,'requests_opened':False,'sq8_canonical_pq_bodies_opened':False,
 'details':{'original_seal':c['original_seal'],'original_seal_bytes_sha256':c['original_seal']['sha256'],
 'original_trace_source_identity_sha256':trace,'caps':c['caps'],'operations':1,'wall_ms':1,
 'prefix':prefix_pin,'permutation_seal':perms,'replay':{'queries':queries,'survived':not reject,
 'per_panel_passes':[63,63] if reject else [64,64],'per_panel_max_bytes':[780*22000]*2 if reject else [780,780]}}})
fd=os.open(p.parent,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
if os.environ.get('FINE_PACK_FAKE')=='deadline':time.sleep(10)
sys.exit(17 if os.environ.get('FINE_PACK_FAKE')=='nonzero' else 0)
'''.replace('SOURCES', repr(DIAGNOSTIC_SOURCES)).encode()
    fake_pin = pin(fake)
    def failure(call, label):
        try:
            call()
        except (ValueError, FileNotFoundError, FileExistsError, OSError, KeyError, AssertionError):
            return
        raise AssertionError('accepted '+label)
    with tempfile.TemporaryDirectory(prefix='fine-pack-check-') as tmp, ExitStack() as stack:
        base = Path(tmp)
        inputs = base/'inputs'
        inputs.mkdir()
        input_pins = []
        for i in range(6):
            path, body = inputs/str(i), ('synthetic body '+str(i)+'\n').encode()
            write(path, body); input_pins.append((str(path), len(body), sha(body)))
        stack.enter_context(patch.object(module, 'INPUT_PINS', tuple(input_pins)))
        stack.enter_context(patch.object(module, 'BINARY_PIN', fake_pin))
        descriptors = [dict(path=p, bytes=n, sha256=h) for p,n,h in input_pins]
        native = dict(schema='borsuk-fine-pack-config-v1', caps=copy.deepcopy(CAPS),
            panels=[dict(dataset=dataset, root=descriptors[i*2], graph=descriptors[i*2+1],
                identity=dict(rows=100000, dimensions=768, generation=i+1, source=[0]*32, layout=[1]*32, pq=[2]*32))
                for i,dataset in enumerate(('relaion','cohere'))], original_seal=descriptors[4], prefix=descriptors[5])
        paths = sorted([str(CONFIG), *CODE, *(r['path'] for r in RECEIPTS)])
        config = dict(schema=SCHEMA, authority_pending=False, fixed=copy.deepcopy(FIXED), native_config=native,
            native_config_sha256=sha(encoded(native)), binary=dict(fake_pin, key=BINARY_KEY),
            inputs=[dict(destination=d['path'], key='synthetic/'+str(i), bytes=d['bytes'], sha256=d['sha256']) for i,d in enumerate(descriptors)],
            native_qualification=list(RECEIPTS), code_sha256={n:file_pin(n)['sha256'] for n in CODE},
            source_archive_paths=paths, source_archive_paths_sha256=sha(json.dumps(paths,separators=(',',':')).encode()))
        validate_config(config)
        failures = 0
        for key, value in [('authority_pending',True), ('native_config_sha256','0'*64), ('extra',True),
                           ('source_archive_paths_sha256','0'*64), ('native_qualification',[])]:
            bad=copy.deepcopy(config);bad[key]=value
            failure(lambda:validate_config(bad), 'config '+key); failures+=1
        bad=copy.deepcopy(config);bad['fixed']['native_caps']['memory_bytes']*=2
        failure(lambda:validate_config(bad), 'resource tuning'); failures+=1
        bad=copy.deepcopy(config);bad['inputs'][0]['destination']='/tmp/forbidden-requests'
        failure(lambda:validate_config(bad), 'foreign input'); failures+=1
        for body in (b'{"same":1,"same":2}',b'{"value":NaN}'):
            failure(lambda:decode(body), 'JSON duplicate/nonfinite'); failures+=1
        raw = encoded(config)
        proof = dict(config_path=str(CONFIG), config_sha256=sha(raw), campaign_schema=SCHEMA,
            artifact_roster_sha256=ROSTER_SHA, native_config_sha256=config['native_config_sha256'],
            binary=config['binary'], native_source_commit=NATIVE_COMMIT, source_identity_sha256=SOURCE_ID,
            code_sha256=config['code_sha256'], native_qualification=config['native_qualification'],
            source_archive_paths=paths, source_archive_paths_sha256=config['source_archive_paths_sha256'])
        userdata = user_data('a'*40,'b'*64,'synthetic/archive.tar.gz',PREFIX+'a0001',proof)
        require('total_max_attempts' in userdata and 'shutdown' in userdata and 'rustup' not in userdata
                and 'cargo' not in userdata, 'SDK/no-compiler bootstrap')
        compile(userdata.split("<<'PY'\n",1)[1].split('\nPY\n',1)[0], '<bootstrap>', 'exec')
        # Download executes the actual SDK body/auth/fsync/no-overwrite path.
        class Body(io.BytesIO):
            def read(self, n=-1):
                require(0 < n <= 64*1024**2+1, 'bounded SDK read')
                return super().read(n)
        bodies={'synthetic/'+str(i):read(inputs/str(i)) for i in range(6)}
        bodies[BINARY_KEY]=fake
        store={}
        s3=Mock()
        def get_object(**kwargs):
            require(kwargs['Bucket']==BUCKET, 'SDK bucket')
            body=(store if kwargs['Key'] in store else bodies)[kwargs['Key']]
            return dict(Body=Body(body), ContentLength=len(body))
        def put_object(**kwargs):
            require(kwargs['IfNoneMatch']=='*', 'immutable marker/artifact writes')
            require(kwargs['Key'] not in store, 'S3 no overwrite')
            store[kwargs['Key']]=kwargs['Body']
        s3.get_object.side_effect=get_object;s3.put_object.side_effect=put_object
        download(s3, config['binary'], base/'download')
        require(file_pin(base/'download')==fake_pin, 'download identity')
        failure(lambda:download(s3,config['binary'],base/'download'),'download overwrite');failures+=1
        for suffix, body in [('short',fake[:-1]),('long',fake+b'x'),('tamper',b'x'+fake[1:])]:
            bodies[BINARY_KEY]=body
            failure(lambda:download(s3,config['binary'],base/suffix),'transport '+suffix);failures+=1
        bodies[BINARY_KEY]=fake
        # Real original processes; only cgroup pseudo-files are substituted.
        files=dict(zip(CGROUP_FILES, (str(CAPS['memory_bytes']),'4096','0','0',
            'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n',
            'high 0\nmax 0\nfail 0\n','100000 100000','usage_usec 1\nuser_usec 1\nsystem_usec 0\n',
            str(FIXED['tasks_max']),'0','max 0\n','','populated 0\nfrozen 0\n')))
        run_id=PREFIX+'a0001/i-original'
        def fixture(name, mode='success', deadline=False, real=False):
            root=base/name;root.mkdir()
            write(root/'config.json',raw);write(root/'source-qualification.json',encoded(proof))
            write(root/'native-config.json',encoded(native));write(root/BINARY_NAME,fake);(root/BINARY_NAME).chmod(0o700)
            write(root/'stage-receipt.json',encoded(dict(binary=fake_pin,native_config=pin(encoded(native)),
                inputs={d['destination']:{k:d[k] for k in ('bytes','sha256')} for d in config['inputs']},
                exact_six_inputs=True,compiler_used=False)))
            write(root/'cpu.txt',b'synthetic cpu\n');write(root/'run-closed.log',b'')
            write(root/'runtime-abi.json',encoded(dict(machine='x86_64',python=[3,12],
                os=dict(ID='ubuntu',VERSION_ID='24.04'),libc=['glibc','2.39'],
                sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True))))
            parent=base/(name+'-cgroup')/(SUPERVISOR_UNIT+'.service')
            parent.mkdir(parents=True)
            group=parent/'native'
            def delegate(record):
                record.update(unit=SUPERVISOR_UNIT+'.service',parent=str(parent),observer=str(parent/'supervisor'),
                    observer_pid=os.getpid(),available=sorted(CONTROLLERS),enabled=sorted(CONTROLLERS),
                    enabled_before=[],parent_process_ids=[],observer_process_ids=[os.getpid()],parent_type='domain')
                return group
            def create(g):
                (g/'cgroup.procs').write_text('')
            def snapshot(g):
                return dict(path=str(g),observer_pid=os.getpid(),files=copy.deepcopy(files))
            owned=[]
            def drain(g):
                if owned and owned[0].poll() is None:
                    os.killpg(owned[0].pid,signal.SIGKILL)
                (g/'cgroup.procs').unlink()
            real_popen=subprocess.Popen
            def spawn(command, **kwargs):
                p=real_popen(command,**kwargs)
                owned.append(p)
                # Synthetic cgroup cannot remove exited PIDs as a kernel does.
                (group/'cgroup.procs').write_text('')
                return p
            with ExitStack() as process_stack:
                if not real:
                    for method, implementation in (('delegated_group',delegate),('create_group',create),
                            ('cgroup_snapshot',snapshot),('drain_group',drain)):
                        process_stack.enter_context(patch.object(module,method,side_effect=implementation))
                    process_stack.enter_context(patch.object(subprocess,'Popen',side_effect=spawn))
                process_stack.enter_context(patch.dict(os.environ,FINE_PACK_FAKE=mode,
                    BORSUK_FINE_PACK_REAL_CGROUP='1' if real else '0'))
                process_stack.enter_context(patch.dict(CAPS,deadline_seconds=.2 if deadline else 300))
                receipt=supervise(config,root,run_id=run_id)
            require(not group.exists(),'synthetic cgroup cleanup')
            return root,receipt
        root, receipt=fixture('original')
        require(receipt['process_exit_code']==0 and validate_result(root,config)['status']=='SURVIVED_NECESSARY_LOCALITY', 'original fake process closure')
        require(file_pin(root/'native.log')['bytes']==0,'silent exit0 log allowed')
        previous=files['cpu.max'];files['cpu.max']='200000 100000'
        denied, denied_receipt=fixture('pre-exec-cpu-refusal')
        require(denied_receipt['process_started'] is False and denied_receipt['process_exit_code'] is None
                and not (denied/'native.log').exists(),'invalid CPU limit refuses before native exec')
        files['cpu.max']=previous
        rejected, receipt=fixture('rejected','reject')
        require(receipt['process_exit_code']==0 and validate_result(rejected,config)['status']=='REJECT','completed all128 rejection')
        nonzero, receipt=fixture('nonzero','nonzero')
        require(receipt['process_exit_code']==17 and decode(read(nonzero/'screen/report.json'))['complete'] is True,'real original exit17')
        failure(lambda:validate_result(nonzero,config),'PASS body original exit17');failures+=1
        timeout, receipt=fixture('timeout','deadline',True)
        require(receipt['process_exit_code']==-signal.SIGKILL and decode(read(timeout/'resources.json'))['deadline_exceeded'] is True,'real deadline process killed')
        failure(lambda:validate_result(timeout,config),'deadline PASS body');failures+=1
        failure(lambda:supervise(config,root,run_id=run_id),'output overwrite');failures+=1
        if real_cgroup:
            actual, receipt=fixture('actual-cgroup',real=True)
            require(receipt['process_exit_code']==0 and validate_result(actual,config)['valid_diagnostic'],'actual delegated cgroup original process/resources/drain/cleanup')
            actual_resources=decode(read(actual/'resources.json'))
            proof_dir=os.environ.get('BORSUK_FINE_PACK_SETUP_PROOF_OUT')
            if proof_dir:
                destination=Path(proof_dir);destination.mkdir(parents=True,exist_ok=False)
                for name in ('native-exit.json','resources.json','cleanup.json','native.log'):
                    write(destination/name,read(actual/name))
                write(destination/'setup-proof.json',encoded(dict(schema='borsuk-fine-pack-setup-proof-v1',
                    source_file_sha256=file_pin(__file__)['sha256'],invocation_id=os.environ.get('INVOCATION_ID'),
                    original_fake_native_exit=receipt['process_exit_code'],native_ANN_executed=False,
                    cgroups_mocked=False,SDK_transport_and_AWS_mocked=True,
                    artifacts={n:file_pin(destination/n) for n in ('native-exit.json','resources.json','cleanup.json','native.log')})))
            print('PASS actual delegated setup: '+json.dumps(dict(delegation=actual_resources['delegation'],
                memory_peak=actual_resources['after']['files']['memory.peak'],swap_peak=actual_resources['after']['files']['memory.swap.peak'],
                original_exit=receipt['process_exit_code'],cleanup=decode(read(actual/'cleanup.json'))),sort_keys=True))
        def tamper(name, change):
            path=root/name;original=read(path);path.write_bytes(change(original))
            try:failure(lambda:validate_result(root,config),'tamper '+name)
            finally:path.write_bytes(original)
        def changed(body, callback):
            value=decode(body);callback(value);return encoded(value)
        for name, change in (
            ('native-exit.json',lambda b:changed(b,lambda d:d.update(process_exit_code=17))),
            ('cleanup.json',lambda b:changed(b,lambda d:d.update(cleanup_complete=False))),
            ('cleanup.json',lambda b:changed(b,lambda d:d.update(output_durable=False))),
            ('resources.json',lambda b:changed(b,lambda d:d.update(closed=False))),
            ('resources.json',lambda b:changed(b,lambda d:d.update(descendants_remaining_after_exit=True))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'memory.events':'oom 1\noom_kill 1\noom_group_kill 0\n'}))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'memory.peak':str(257*1024**2)}))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'memory.swap.peak':'1'}))),
            ('resources.json',lambda b:changed(b,lambda d:d['after']['files'].update({'cgroup.procs':'999'}))),
            ('stage-receipt.json',lambda b:changed(b,lambda d:d.update(exact_six_inputs=False))),
            ('native-config.json',lambda b:b+b' '),
            (BINARY_NAME,lambda b:b+b' '),
            ('screen/report.json',lambda b:changed(b,lambda d:d.update(config_sha256='0'*64))),
            ('screen/report.json',lambda b:changed(b,lambda d:d.update(queries=127))),
            ('screen/report.permutations.json',lambda b:changed(b,lambda d:d['old_to_new'][0].__setitem__(0,1))),
            ('screen/report.prefix.jsonl',lambda b:b+b'x')):
            tamper(name,change);failures+=1
        for name in ('screen/report.json','screen/report.permutations.json','screen/report.prefix.jsonl','resources.json','cleanup.json','native-exit.json'):
            path=root/name;hidden=path.with_name(path.name+'.hidden');path.rename(hidden)
            try:failure(lambda:validate_result(root,config),'missing '+name);failures+=1
            finally:hidden.rename(path)
        # Joint seal tamper, even if its report descriptor is rehashed.
        original_perms=read(root/'screen/report.permutations.json');original_report=read(root/'screen/report.json')
        perm=decode(original_perms);perm['old_to_new'][0][0]=1
        body=encoded(perm);(root/'screen/report.permutations.json').write_bytes(body)
        report=decode(original_report);report['details']['permutation_seal'].update(pin(body))
        (root/'screen/report.json').write_bytes(encoded(report))
        failure(lambda:validate_result(root,config),'rehashed non-bijection');failures+=1
        (root/'screen/report.permutations.json').write_bytes(original_perms);(root/'screen/report.json').write_bytes(original_report)
        prefix=PREFIX+'a0001'
        result=validate_result(root,config)
        terminal=dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,instance_id='i-original',
            prefix=prefix,config_sha256=sha(raw),status='complete',phase='complete',exit_code=0,original_exit_code=0,
            disposition=result['status'],artifact_roster_sha256=ROSTER_SHA,qualification=proof,result=result)
        publish(s3,root,prefix,terminal)
        require(s3.put_object.call_args.kwargs['Key']==prefix+'/terminal.json','marker published last')
        require(set(terminal['artifacts'])==set(ARTIFACTS),'complete raw artifact roster')
        def controller_receipts(out):
            out.mkdir()
            write(out/'aws-reservation.json',encoded(dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,
                config_sha256=sha(raw),qualification=proof)))
            nodes={'0':{'instance_id':'i-original'}}
            write(out/'aws-launch.json',encoded(dict(instance_id='i-original',nodes=nodes,prefix=prefix,
                source_commit='a'*40,source_archive_sha256='b'*64)))
            write(out/'aws-closeout.json',encoded(dict(state='terminated',nodes=nodes)))
        out=base/'collected';controller_receipts(out)
        collect(s3,prefix,out,'i-original','a'*40,'b'*64)
        require(replay(out)==result,'full hash/raw/gzip original collection replay')
        failure(lambda:collect(s3,prefix,out,'i-original','a'*40,'b'*64),'collection overwrite');failures+=1
        path=out/'screen/report.json';original=read(path);path.write_bytes(original+b' ')
        failure(lambda:replay(out),'collected tamper');failures+=1;path.write_bytes(original)
        key=prefix+'/artifacts/native.log';store[key]=b'tampered'
        damaged=base/'damaged';controller_receipts(damaged)
        failure(lambda:collect(s3,prefix,damaged,'i-original','a'*40,'b'*64),'remote fullhash');failures+=1
        store[key]=b''
        # SDK/upload failure leaves no misleading terminal; raw failures remain.
        failed_prefix=PREFIX+'a0002'
        failed_terminal=dict(terminal,status='failed',phase='execution',exit_code=96,original_exit_code=17,disposition='INVALID',prefix=failed_prefix)
        publish(s3,nonzero,failed_prefix,failed_terminal)
        require(failed_terminal['original_exit_code']==17 and failed_terminal['artifacts']['screen/report.json']['bytes']>0,'failure body preserved')
        reads_before=s3.get_object.call_count
        with patch.object(s3,'get_object',return_value=dict(Body=Body(b'wrong'),ContentLength=5)):
            failure(lambda:publish(s3,rejected,PREFIX+'a0003',copy.deepcopy(terminal)),'upload readback');failures+=1
        require(PREFIX+'a0003/terminal.json' not in store and s3.get_object.call_count>=reads_before,'no marker after failed upload')
        # Same shared ACK ownership, fsync, termination and waiter path used in production.
        import boto3
        campaign=Mock(ROOT=base,NAME='lifecycle',SCHEMA=SCHEMA,WALL=WALL,ARTIFACTS=ARTIFACTS,
            PREFIX=PREFIX,TOKEN_PREFIX=TOKEN_PREFIX,TAG=TAG,SUBNET=SUBNET,INSTANCE_TYPE=INSTANCE_TYPE,
            IMAGE_ID=IMAGE_ID,ROOT_DEVICE_NAME=ROOT_DEVICE_NAME,SPOT_MAX_USD_PER_HOUR=SPOT_MAX_USD_PER_HOUR,COMPUTE_CAP=COMPUTE_CAP)
        campaign.preflight.return_value=dict(config_sha256='c'*64)
        campaign.user_data.return_value='#!/bin/bash\nexit 0\n'
        for mode in ('success','poll','launch-fsync','launch-upload','collect','extra-ack'):
            campaign.ROOT=base/('lifecycle-'+mode);campaign.poll.reset_mock();campaign.collect.reset_mock()
            ec2=Mock();aws_s3=Mock();session=Mock()
            session.client.side_effect=lambda service:ec2 if service=='ec2' else aws_s3
            ec2.describe_instances.return_value={'Reservations':[]}
            ec2.describe_subnets.return_value={'Subnets':[{'AvailabilityZone':'mock'}]}
            ec2.describe_spot_price_history.return_value={'SpotPriceHistory':[{'SpotPrice':'.01','Timestamp':datetime.now(timezone.utc)}]}
            ack=['i-original','i-extra'] if mode=='extra-ack' else ['i-original']
            ec2.run_instances.return_value={'Instances':[{'InstanceId':i} for i in ack]}
            campaign.poll.side_effect=OSError('mock observation') if mode=='poll' else None
            campaign.collect.side_effect=OSError('mock collection') if mode=='collect' else None
            campaign.collect.return_value=dict(status='complete',phase='complete',exit_code=0,artifacts={n:pin(b'') for n in ARTIFACTS})
            chronology=[]
            ec2.terminate_instances.side_effect=lambda **kw:chronology.append(('terminate',kw['InstanceIds']))
            ec2.get_waiter.return_value.wait.side_effect=lambda **kw:chronology.append(('wait',kw['InstanceIds']))
            real_fsync=os.fsync
            def fsync(fd):
                if mode=='launch-fsync':raise OSError('mock launch persistence')
                return real_fsync(fd)
            def upload(key,body):
                if mode=='launch-upload' and key.endswith('/launch.json'):raise OSError('mock launch upload')
            with patch.object(boto3,'Session',return_value=session), \
                 patch.object(shared.subprocess,'check_output',side_effect=['','a'*40]), \
                 patch.object(shared,'source_archive',return_value=b'tiny synthetic archive'), \
                 patch.object(shared.peer,'missing',return_value=True), \
                 patch.object(shared.peer,'put_if_absent',side_effect=upload), \
                 patch.object(shared.os,'fsync',side_effect=fsync):
                if mode in ('success','extra-ack'):shared.main('a0001',campaign=campaign)
                else:failure(lambda:shared.main('a0001',campaign=campaign),'lifecycle '+mode);failures+=1
            require(ec2.run_instances.call_count==1,'one original launch, no retry')
            require(chronology==[('terminate',ack),('wait',ack)],'every ACK same-ID terminate/wait')
            kwargs=ec2.run_instances.call_args.kwargs
            require(kwargs['InstanceType']==INSTANCE_TYPE and kwargs['InstanceMarketOptions']['MarketType']=='spot'
                    and kwargs['InstanceMarketOptions']['SpotOptions']['MaxPrice']=='0.60','bounded c7i Spot launch')
            if mode!='launch-fsync':
                require(decode(read(campaign.ROOT/'lifecycle/a0001/aws-launch.json'))['nodes']=={str(i):{'instance_id':v} for i,v in enumerate(ack)},'durable every-ACK receipt')
        print(f'PASS fine-pack: real fake-native exit0/REJECT/exit17/deadline; {failures} refusals; fullhash/readback/marker-last/no-overwrite; every-ACK same-ID terminate/wait; actual SDK model positive/official_old_negative={official_old_checked}; actual_delegated_cgroup={real_cgroup}; other cgroup/SDK transport/AWS MOCKED; no ANN/graph/corpus/network')


def sq4_self_check(histogram=False, corrected=False):
    """Mock native CLI and qualification metadata; no native/data/AWS execution."""
    import copy
    import tempfile
    from contextlib import ExitStack
    from unittest.mock import patch
    module = sys.modules[__name__]
    configure_sq4(histogram=histogram, corrected=corrected)
    retained_pins = INPUT_PINS
    require(SCHEMA == ('borsuk-corrected-four-bit-diagnostic-spot-v1' if corrected else 'borsuk-histogram-sq4-diagnostic-spot-v1' if histogram else 'borsuk-fixed-sq4-diagnostic-spot-v1'), 'explicit SQ4 mode')
    require(len(INPUT_PINS) == (19 if corrected else 18) and sum(p[1] for p in INPUT_PINS) == 229614200+(175929 if corrected else 0), 'opaque retained roster')
    require(len([n for n in ARTIFACTS if n.startswith('screen/')]) == (140 if histogram or corrected else 134), 'whole native closure')
    # Root-authenticated completed metadata only; the executable/transport below
    # remain mocked. No corpus, compiler or live job is opened by this fixture.
    def committed(path, revision=None):
        repo=Path(__file__).resolve().parents[1]
        revision = revision or 'e0d81804'
        return subprocess.check_output(['git','-c','safe.directory='+str(repo),'show',revision+':'+str(path)],cwd=repo)
    deployment_body=committed(SQ4_ROOT/'qualified-deployment-pins-c2d233d6.json')
    require(pin(deployment_body)==dict(bytes=15955,sha256='97c175233d71e5e6624fa91939baf436a79ec29b3cbbaff2d609ff453464d145'), 'root completed deployment fixture pin')
    deployment=decode(deployment_body)
    actual={}
    for name,descriptor in deployment['qualification_receipts'].items():
        body=committed(descriptor['path'])
        require(pin(body)=={k:descriptor[k] for k in ('bytes','sha256')}, 'root completed receipt fixture pin')
        actual[name]=decode(body)
    old,verification,workspace=(actual[n] for n in ('source-qualification.json','parent-verification.json','workspace-receipt.json'))
    require(deployment['required_stage_protocol']==[dict(stage=s['stage'],command=s['command'],
        required_test_passes=s['required_test_passes'],require_positive_tests_run=s['tests_run'] is not None)
        for s in workspace['stages']], 'actual completed root all14 protocol')
    if histogram:
        import ast
        hist_root = SQ4_ROOT/'histogram-codebook'
        contract = decode(committed(hist_root/'native-source-contract-1057e5a6.json','de0b6e38'))
        prospective = decode(committed(hist_root/'prospective-native-config-1057e5a6.json','de0b6e38'))
        launch = decode(committed(hist_root/'implementation-gates/compiler-repair/config.json','de0b6e38'))
        manifest = decode(committed(hist_root/'implementation-gates/compiler-repair/native-source-manifest.json','de0b6e38'))
        launcher = ast.parse(committed(Path('scripts/launch_native_workspace_execution_spot.py'),'e91cf34439c54a655ccf9edc53e9b66214fda4ae'))
        assignments = {n.targets[0].id:n.value for n in launcher.body
                       if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name)}
        commands = [(n,c.split()) for n,c in ast.literal_eval(assignments['FINE_SQ8_STAGES'].args[0].generators[0].iter)]
        mandatory = ast.literal_eval(assignments['FINE_SQ8_REQUIRED_TESTS'])
        protocol = sha(encoded(dict(stages=commands,mandatory_tests=mandatory)))
        corrected_names = {n:list(v) for n,v in mandatory.items()}
        changed = [(old,new) for stage,names in launch['mandatory_tests'].items()
                   for old,new in zip(names,corrected_names[stage]) if old != new]
        require(protocol == HISTOGRAM_QUALIFICATION_PROTOCOL_SHA and len(commands) == 14
                and len(changed) == 2 and all(old.replace('::histogram::tests::','::tests::') == new
                    for old,new in changed), 'committed corrected native protocol')
        # Rebind synthetic full authority only; original partial gate96 is never admitted.
        launch['mandatory_tests'] = corrected_names
        native_body = committed(Path('crates/borsuk/src/fine_sq8_groups.rs'), '1057e5a6')
        require(pin(native_body) == next({k:d[k] for k in ('bytes','sha256')} for d in contract['owned_files']
                    if d['path'].endswith('/fine_sq8_groups.rs')), 'actual Rust source fixture')
        source_text = native_body.decode()
        for name in mandatory['fine-sq8-tests']:
            if 'fine_histogram_sq4_' not in name:
                continue
            definition = re.search(r'(?m)^ *fn '+re.escape(name.rsplit('::',1)[1])+r'\(',source_text)
            require(definition is not None, 'actual native mandatory test definition')
            tests_indent = re.findall(r'(?m)^( +)mod tests \{',source_text[:definition.start()])[-1]
            require((len(tests_indent) == 12) == ('::histogram::tests::' in name), 'source-bound histogram test namespace')
        for token in ('{extension}-book.bin','{extension}-groups.bin','{extension}-root.json',
                      '{name}-payloads.json','{name}-prefix.jsonl','{name}-freeze.json','{name}-result-{}.json'):
            require(token.encode() in native_body, 'source-bound native roster')
        old.update(source_sha256=manifest['source_sha256'], native_source_manifest=launch['native_source_manifest'],
                   native_source_manifest_sha256=launch['native_source_manifest']['sha256'], mandatory_tests=launch['mandatory_tests'])
        stages = workspace['stages']
        for record,(name,command) in zip(stages,commands):
            tests = mandatory.get(name,())
            record.update(stage=name,command=command,tests_run=max(1,len(tests)) if tests else None,
                         required_test_passes={n:1 for n in tests})
        verification['stages'] = stages
        workspace['mandatory_tests'] = launch['mandatory_tests']
        # These are synthetic completed receipts, never the running native job.
        NATIVE_COMMIT_FIXTURE = manifest['native_source_commit']
        SOURCE_ID_FIXTURE = contract['expected_histogram_source_identity_sha256']
        SOURCES_FIXTURE = {d['path']:d['sha256'] for d in contract['owned_files']}
    elif corrected:
        commands = [(s['stage'],s['command']) for s in CORRECTED_PROTOCOL['stages']]
        mandatory = CORRECTED_PROTOCOL['fixed']['mandatory_tests']
        protocol = CORRECTED_PROTOCOL_SHA
        manifest = decode(committed(CORRECTED_ROOT/'implementation-gates/native-source-manifest.json','6089bd0c'))
        require(manifest['source_sha256'] == CORRECTED_SOURCE['source_sha256'], 'actual isolated full405 map')
        # Read-only source checks reconstruct the native identity and CLI/roster.
        native_body = committed(Path('crates/borsuk/src/fine_sq8_groups.rs'),CORRECTED_COMMIT)
        text = native_body.decode(); start = text.index('pub fn source_identity()',text.index('pub mod sq4_diagnostic'))
        framed = hashlib.sha256()
        for name,path in re.findall(r'"([^"]+)",\s*include_bytes!\("([^"]+)"\)',text[start:text.index('format!("{:x}", h.finalize())',start)]):
            source = committed(Path('crates/borsuk/src')/path,CORRECTED_COMMIT)
            require(sha(source) == CORRECTED_SOURCE['source_sha256']['crates/borsuk/src/'+path], 'actual isolated diagnostic include')
            framed.update(len(name).to_bytes(8,'little'));framed.update(name.encode())
            framed.update(len(source).to_bytes(8,'little'));framed.update(source)
        codec = committed(Path('crates/borsuk/src/corrected_four_bit.rs'),CORRECTED_COMMIT)
        require(sha(b'borsuk-corrected-four-bit-source-v1'+framed.hexdigest().encode()+codec) == CORRECTED_DIAGNOSTIC_SOURCE_ID, 'actual corrected diagnostic identity')
        binary_source = committed(Path('crates/borsuk/src/bin/hierarchical_semantic_cells.rs'),CORRECTED_COMMIT)
        require(b'check-fine-corrected-four-bit CONFIG CONFIG_SHA256 NEW_REPORT_JSON' in binary_source, 'actual exact native CLI')
        for token in ('{NAME}-rotation.bin','{NAME}-startup.json','{NAME}-{index}.bin','{NAME}-{index}-groups.bin','{NAME}-{index}-root.json','{NAME}-payloads.json','{NAME}-prefix.jsonl','{NAME}-freeze.json','{NAME}-result-{}.json'):
            require(token.encode() in native_body,'actual corrected native output roster')
        old.update(source_sha256=manifest['source_sha256'],source_file_count=405,
            native_source_manifest=pin(encoded(manifest)),native_source_manifest_sha256=sha(encoded(manifest)),mandatory_tests=mandatory)
        template = copy.deepcopy(workspace['stages'][0]); stages=[]
        for index,(name,command) in enumerate(commands):
            record = copy.deepcopy(template);tests = mandatory.get(name,())
            instant = datetime.fromisoformat(template['started_at'])+timedelta(seconds=index*2)
            record.update(stage=name,command=command,tests_run=len(tests) if tests else None,
                required_test_passes={n:1 for n in tests},started_at=instant.isoformat(),finished_at=(instant+timedelta(seconds=1)).isoformat())
            stages.append(record)
        workspace.update(stages=stages,mandatory_tests=mandatory,source_file_count=405)
        verification.update(stages=stages,source_file_count=405,qualified=True)
        NATIVE_COMMIT_FIXTURE,SOURCE_ID_FIXTURE,SOURCES_FIXTURE = CORRECTED_COMMIT,CORRECTED_DIAGNOSTIC_SOURCE_ID,SQ4_SOURCES
    else:
        NATIVE_COMMIT_FIXTURE, SOURCE_ID_FIXTURE, SOURCES_FIXTURE = NATIVE_COMMIT, SOURCE_ID, SQ4_SOURCES
    def refused(call):
        try: call()
        except (ValueError, OSError, KeyError): return
        raise AssertionError('unsafe SQ4 acceptance')
    # /tmp is tmpfs on Devbox: raw payload + transport + replay copies need disk.
    with tempfile.TemporaryDirectory(prefix='sq4-glue-check-', dir='/var/tmp') as tmp, ExitStack() as stack:
        base = Path(tmp)
        stack.enter_context(patch.object(module,'NATIVE_COMMIT',NATIVE_COMMIT_FIXTURE))
        stack.enter_context(patch.object(module,'SOURCE_ID',SOURCE_ID_FIXTURE))
        stack.enter_context(patch.object(module,'SQ4_SOURCES',SOURCES_FIXTURE))
        stack.enter_context(patch.object(module, 'SQ4_INPUT_ROOT', base/'retained'))
        pins = tuple((str(base/'retained'/f'input-{i}'), 1, sha(b'x')) for i in range(19 if corrected else 18))
        stack.enter_context(patch.object(module, 'INPUT_PINS', pins))
        descriptors = [dict(path=p, bytes=n, sha256=h) for p,n,h in pins]
        native = dict(schema=SQ4_SCHEMA+'-config-v1', source_identity_sha256=SOURCE_ID,
            caps=copy.deepcopy(CAPS), original_seal=descriptors[16], prefix=descriptors[17],
            panels=[dict(dataset=d, root=descriptors[i*8], requests=descriptors[i*8+6],
                         truth=descriptors[i*8+7], truth_width=100) for i,d in enumerate(('relaion','cohere'))])
        if corrected:
            native.update(rotation_seed=[23]*32,construction_operations=159001952256,query_auth_operations=50000000000,
                closed_populations=descriptors[18])
        fake = b'''#!/usr/bin/env python3
import hashlib,json,os,sys,tempfile,time
from pathlib import Path
_,mode,config_path,config_sha,out=sys.argv
assert mode=='check-fine-sq4' and hashlib.sha256(Path(config_path).read_bytes()).hexdigest()==config_sha
c=json.loads(Path(config_path).read_bytes());p=Path(out);fault=os.environ.get('SQ4_FAKE','')
def emit(path,v):
 b=json.dumps(v,separators=(',',':')).encode()
 with path.open('xb') as f:f.write(b);f.flush();os.fsync(f.fileno())
 return {'path':str(path),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def body(path,b):
 with path.open('xb') as f:f.write(b)
 return {'path':str(path),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
payloads=[]
learned=c['schema']=='borsuk-histogram-sq4-config-v1'
trainer='occupied-u8-weighted-contiguous-f64-dp-smallest-predecessor-v1'
for i,panel in enumerate(c['panels']):
 path=p.with_suffix('.sq4-'+str(i)+'.bin')
 with path.open('xb') as f:f.truncate(39600000)
 h=hashlib.sha256()
 with path.open('rb') as f:
  while b:=f.read(65536):h.update(b)
 payloads.append({'payload':{'path':str(path),'bytes':39600000,'sha256':h.hexdigest()},'original_root':panel['root'],
  'codec':'borsuk-sq4-nearest17-original-coefficients-v1','rows':100000,'dimensions':768,'row_bytes':396,'group_rows':16})
 if learned:
  v=payloads[-1]
  records={'path':panel['root']['path']+'-records','bytes':78000000,'sha256':hashlib.sha256(b'synthetic original records').hexdigest()}
  coeff=hashlib.sha256((768).to_bytes(4,'little')+bytes(768*4)+(1065353216).to_bytes(4,'little')*768).hexdigest()
  fit={'histogram_sha256':hashlib.sha256(b'synthetic histogram').hexdigest()}
  header=b'BORSH401'+(768).to_bytes(4,'little')+(100000).to_bytes(4,'little')+bytes.fromhex(records['sha256']+coeff+fit['histogram_sha256'])+hashlib.sha256(trainer.encode()).digest()
  book=body(Path(str(path)+'-book.bin'),header+(bytes([1])+bytes(64))*768)
  groups=body(Path(str(path)+'-groups.bin'),bytes(200000))
  v.update(original_records=records,low_bits=[0]*768,step_bits=[1065353216]*768,group_hashes_sha256=groups['sha256'])
  generation=emit(Path(str(path)+'-root.json'),{'schema':'borsuk-histogram-sq4-generation-v1','codec':v['codec'],
   'trainer':trainer,'source_identity_sha256':c['source_identity_sha256'],'config_sha256':config_sha,
   'original_root':panel['root'],'original_records':records,'coefficients_sha256':coeff,'histogram_sha256':fit['histogram_sha256'],
   'rows':100000,'dimensions':768,'book':book,'payload':v['payload'],'group_hashes':groups,'fit':fit})
  v['histogram_generation']={'root':generation,'book':book,'groups':groups,'fit':fit,'trainer':trainer,
   'startup_book_reads':1,'startup_book_bytes':book['bytes'],'startup_generation_reads':3,
   'startup_generation_bytes':sum(x['bytes'] for x in (generation,book,groups)),
   'mse_implies_recall':False,'updates_gc_integrated':False}
seal=emit(p.with_suffix('.sq4-payloads.json'),{'schema':'borsuk-fixed-sq4-payload-seal-v1','config_sha256':config_sha,
 'source_identity_sha256':c['source_identity_sha256'],'codec':'borsuk-sq4-nearest17-original-coefficients-v1',
 'original_seal':c['original_seal'],'payloads':payloads,'queries_opened':False,'truth_opened':False})
prefix=body(p.with_suffix('.sq4-prefix.jsonl'),Path(c['prefix']['path']).read_bytes())
results=[]
for i in range(128):
 fits=fault!='envelope' or i!=0
 scored={'fetched_ids':[0],'ranked':[{'id':n,'ordinal':n,'score_bits':0} for n in range(100)],
  'range_reads':1,'verified_bytes':396 if fits else 16777612}
 if learned:
  g=payloads[i//64]['histogram_generation']
  scored['histogram_metrics']={k:g[k] for k in ('startup_book_reads','startup_book_bytes','startup_generation_reads','startup_generation_bytes')}
  scored['histogram_metrics'].update(direct_packed=True,payload_ranges_exclude_startup=True,total_cold_get_claim=False)
 results.append(emit(p.with_suffix('.sq4-result-'+str(i)+'.json'),{'schema':'borsuk-fixed-sq4-query-v1',
  'dataset':c['panels'][i//64]['dataset'],'ordinal':i%64,'nominees_retained':True,'original_cover_contained':True,
  'truth_opened':False,'sq8_reference_serving_eligible':False,'plan':{'envelope_fits':fits,'candidate_bytes':scored['verified_bytes']},
  'sq4':scored,'sq8_reference':scored,'original256_baseline':scored}))
freeze=emit(p.with_suffix('.sq4-freeze.json'),{'schema':'borsuk-fixed-sq4-freeze-v1','config_sha256':config_sha,
 'source_identity_sha256':c['source_identity_sha256'],'payload_seal':seal,'payloads':payloads,'original_seal':c['original_seal'],
 'truth':[x['truth'] for x in c['panels']],'nomination_prefix':prefix,'results':results,'truth_opened':False})
reject=fault in ('reject','envelope')
report={'schema':'borsuk-fixed-sq4-report-v1','codec':'borsuk-sq4-nearest17-original-coefficients-v1',
 'config_sha256':config_sha,'source_identity_sha256':c['source_identity_sha256'],'queries':128,'complete':True,
 'status':'REJECT' if reject else 'SURVIVED_CONSUMED_PANELS','standalone_authority':False,
 'requires_matching_supervisor_exit_receipt':True,'quality_or_performance_claim':False,
 'details':{'freeze':freeze,'rows':100000,'dimensions':768,'truth':[x['truth'] for x in c['panels']],
 'frozen_original_authority':True,'caps':c['caps'],'operations':1,'whole_process_supervisor_required':True,
 'pair_payload_bytes':79200000,'modeled_peak_bytes':100000000,'modeled_output_bytes':90000000,
 'all128_envelopes_fit':fault!='envelope','summaries':[{'dataset':x['dataset'],
  'sq4_returned':{'mean_recall':.97 if fault=='reject' else .99,'p05_hits':94 if fault=='reject' else 99}} for x in c['panels']]}}
if learned:
 assert len(results)==128 and all(Path(x['path']).exists() for x in results) and Path(freeze['path']).exists()
 report['details']['histogram_resources']={'startup_book_reads':2,'startup_book_bytes':100128,
  'startup_generation_reads':6,'startup_generation_bytes':sum(v['histogram_generation']['startup_generation_bytes'] for v in payloads),
  'payload_ranges_exclude_startup':True,'total_cold_get_claim':False,'packed_row_bytes':396}
emit(p,report)
if fault=='drift':Path(config_path).write_bytes(Path(config_path).read_bytes()+b' ')
if fault in ('scratch','external-temp'):
 path=p.parent.parent/'temporary' if fault=='scratch' else Path(tempfile.gettempdir())/'external-temporary'
 if fault=='external-temp':assert path.parent==p.parent.parent/'bootstrap/tmp'
 with path.open('xb') as f:f.truncate(4294967296)
 time.sleep(2)
if fault=='deadline':time.sleep(5)
sys.exit(2 if fault=='latefailure' else 0)
'''
        if histogram:
            fake = fake.replace(b'check-fine-sq4',b'check-fine-histogram-sq4').replace(
                b'borsuk-fixed-sq4',b'borsuk-histogram-sq4').replace(b'.sq4-',b'.histogram-sq4-').replace(
                b'borsuk-sq4-nearest17-original-coefficients-v1',SQ4_CODEC.encode())
        if corrected:
            fake = fake.replace(b'check-fine-sq4',SQ4_CLI.encode()).replace(b'borsuk-fixed-sq4',SQ4_SCHEMA.encode()).replace(
                b'.sq4-',b'.corrected-four-bit-').replace(b'borsuk-sq4-nearest17-original-coefficients-v1',SQ4_CODEC.encode())
            generation_code = '''rotation=body(p.with_suffix('.corrected-four-bit-rotation.bin'),b'BORSUK-C4ROT-v1\\0'+(768).to_bytes(8,'little')+bytes(40+8*768**2))
inputs=INPUTS
for i,v in enumerate(payloads):
 groups=body(p.with_suffix('.corrected-four-bit-'+str(i)+'-groups.bin'),bytes(200000))
 g={'schema':'borsuk-corrected-four-bit-generation-v1','codec':v['codec'],'source_identity_sha256':c['source_identity_sha256'],
  'config_sha256':config_sha,'original_root':v['original_root'],'rotation':rotation,'payload':v['payload'],'groups':groups,
  'rows':100000,'dimensions':768,'row_bytes':396,'router_identity':{'rows':100000,'dimensions':768},'low_bits':[0]*768,'step_bits':[0]*768}
 for key,offset in (('original_records',5),('original_order',3),('original_groups',2),('original_graph',1),('original_pq',4)):g[key]=inputs[i*8+offset]
 root=emit(p.with_suffix('.corrected-four-bit-'+str(i)+'-root.json'),g)
 v.clear();v.update(root=root,payload=g['payload'],groups=groups,original_root=g['original_root'])
'''.replace('INPUTS',repr(descriptors))
            fake = fake.replace(b'seal=emit(',generation_code.encode()+b'seal=emit(',1)
            fake = fake.replace(b"'fetched_ids':[0]",b"'fetched_ids':list(range(16))").replace(b'396 if fits',b'6336 if fits')
            correction_code = ''' if v.get('schema')=='borsuk-corrected-four-bit-query-v1':
  v.pop('sq8_reference_serving_eligible');v.pop('original256_baseline')
  v['corrected']=v.pop('sq4');v['decoded_cosine']=dict(v['corrected'],verified_bytes=12480)
  v['sq8_reference']=dict(v['corrected'],verified_bytes=12480)
  v.update(generation=payloads[i//64]['root'],rotation=rotation,mutation_revision=0)
  v['plan'].update(row_ranges=[{'start':0,'end':16}],nominees=[0],query_sha256='0'*64)
'''
            fake = fake.replace(b' b=json.dumps(v,',correction_code.encode()+b' b=json.dumps(v,',1)
            # Add corrected companions before their parent descriptors are sealed.
            fake = fake.replace(b"'payloads':payloads,'queries_opened'",b"'rotation':rotation,'payloads':payloads,'queries_opened'")
            fake = fake.replace(b"'results':results,'truth_opened'",b"'rotation':rotation,'closed_populations':c['closed_populations'],'results':results,'truth_opened'")
            report_code = '''objects=[c['original_seal'],c['prefix'],c['closed_populations'],rotation]
for i,v in enumerate(payloads):
 g=json.loads(Path(v['root']['path']).read_bytes())
 objects.extend(g[k] for k in ('original_root','original_graph','original_pq','original_order','original_groups','groups'))
 objects.append(v['root'])
startup={'distinct_objects':[{'artifact':a,'read_attempts':1} for a in objects],'bytes':sum(a['bytes'] for a in objects),
 'count':len(objects),'counted_once':True,'read_attempts':len(objects),'read_bytes':sum(a['bytes'] for a in objects),'separate_pretruth_reauthentication_reads':153}
startup_pin=emit(p.with_suffix('.corrected-four-bit-startup.json'),startup)
d=report['details'];d.pop('operations');d.pop('pair_payload_bytes')
d.update(construction_limit=c['construction_operations'],construction_work_bound=159001952256,construction_charged_work=159001952256,
 query_auth_limit=c['query_auth_operations'],query_auth_work_bound=49000000000,query_auth_operations=45000000000,
 candidate_query_charged_work=15733844912,decoded_cosine_control_charged_work=16649896880,unchanged_sq8_control_charged_work=11326600896,
 charged_work_is_conservative_bound=True,actual_dense_encoding_macs=117964800000,startup=startup_pin,
 startup_distinct_objects=startup['count'],startup_distinct_bytes=startup['bytes'],payload_ranges_exclude_startup=True,
 total_cold_get_claim=False,total_cold_byte_claim=False,lifecycle_qualified=False,production_snapshot_integration=False,
 finite_Haar_unbiasedness_claim=False,radial_causation_claim=False)
for summary in d['summaries']:
 summary.pop('sq4_returned');hits=[94 if fault=='reject' else 99]*64
 summary.update({k:{'hits':hits,'total_hits':sum(hits),'mean_recall':sum(hits)/6400,'p05_hits':hits[0]} for k in ('corrected','decoded_cosine','sq8_reference','fetched_coverage')})
'''
            fake = fake.replace(b'emit(p,report)',report_code.encode()+b'emit(p,report)')
            fake = fake.replace(b'f.truncate(4294967296)',b'f.truncate(8589934592)')
            populations = dict(panels=[dict(queries=[dict(row_ranges=[dict(start=0,end=16)],fetched_rows=16,query_sha256='0'*64,
                fetched_ids_sha256=sha(b''.join(n.to_bytes(8,'little',signed=True) for n in range(16))),
                nominee_ordinals_sha256=sha(bytes(8))) for _ in range(64)]) for _ in range(2)])
            stack.enter_context(patch.object(module,'CORRECTED_POPULATIONS',populations))
        source = dict(old['source_sha256'], **SQ4_SOURCES)
        full_id = sha(json.dumps(source, sort_keys=True, separators=(',', ':')).encode())
        require(full_id==(manifest['source_identity_sha256'] if histogram or corrected else '1689c53c7564f989f19da397b32b13f16f10df264d02355928849dcabf1e2849'),'actual prospective full source map')
        old.update(native_source_commit=NATIVE_COMMIT, source_identity_sha256=full_id, source_sha256=source)
        verification.update(native_source_commit=NATIVE_COMMIT, source_identity_sha256=full_id, binary=pin(fake))
        workspace.update(source_identity_sha256=full_id, source_sha256=source)
        terminal = actual['aws-terminal.json']
        terminal.update(native_source_commit=NATIVE_COMMIT, source_identity_sha256=full_id)
        if corrected:
            old['schema'] = 'borsuk-corrected-four-bit-implementation-gates-qualification-v1'
            workspace['schema'] = 'borsuk-corrected-four-bit-implementation-gates-receipt-v1'
            terminal.update(schema='borsuk-corrected-four-bit-implementation-gates-spot-v1',source_file_count=405)
            for value in (workspace,terminal):
                value['artifacts'].update({'source-before.json':pin(encoded(source)),
                    'source-after.json':pin(encoded(source)),BINARY_NAME:pin(fake)})
        terminal['native_source_manifest_sha256'] = old['native_source_manifest_sha256']
        receipt_bodies = dict(zip(SQ4_RECEIPTS, (verification, old, workspace, terminal,
                                               actual['aws-closeout.json'])))
        workspace['qualification_sha256'] = pin(encoded(old))['sha256']
        terminal['artifacts']['workspace-receipt.json'] = pin(encoded(workspace))
        terminal['artifacts'][BINARY_NAME] = pin(fake)
        terminal['source_qualification_sha256'] = pin(encoded(old))['sha256']
        receipts = []
        for name,value in receipt_bodies.items():
            path=base/'proofs'/name; write(path, encoded(value))
            receipts.append(dict(path='proofs/'+name, **file_pin(path)))
        fixed = copy.deepcopy(FIXED); fixed['native_caps']=copy.deepcopy(CAPS)
        fixed.update(machine_limit_seconds=3600 if corrected else 1800,compute_cap_usd=.60 if corrected else .30)
        fixed['scratch'] = dict(input_bytes=len(pins), native_output_bytes=CAPS['output_bytes'], binary_bytes=len(fake),
            source_archive_bytes=8*1024**2, bootstrap_bytes=256*1024**2, auxiliary_bytes=16*1024**2, cap_bytes=SQ4_SCRATCH_CAP)
        paths = sorted([str(CONFIG), str(SQ4_ROOT/'prospective-input-roster.json'), *CODE, *(r['path'] for r in receipts),
                        *(str(CORRECTED_ROOT/n) for n,_,_ in CORRECTED_EVIDENCE if corrected)])
        config = dict(schema=SCHEMA, authority_pending=False, fixed=fixed, native_config=native,
            native_source=dict(commit=NATIVE_COMMIT,full_source_identity_sha256=full_id,
                               source_identity_sha256=SOURCE_ID,source_sha256=SQ4_SOURCES),
            native_config_sha256=sha(encoded(native)), binary=dict(pin(fake), key='mock/native'), inputs=[
                dict(destination=p, bytes=n, sha256=h, key='mock/input-'+str(i)) for i,(p,n,h) in enumerate(pins)],
            native_qualification=receipts, code_sha256={n:file_pin(n)['sha256'] for n in CODE},
            source_archive_paths=paths, source_archive_paths_sha256=sha(json.dumps(paths,separators=(',',':')).encode()))
        if histogram or corrected:
            config['native_source']['qualification_protocol_sha256'] = protocol
        validate_config(config)
        for n in CODE:write(base/n,read(n))
        if corrected:
            for name,_,_ in CORRECTED_EVIDENCE:write(base/CORRECTED_ROOT/name,read(CORRECTED_ROOT/name))
        real=copy.deepcopy(config)
        descriptors=[dict(path=p,bytes=n,sha256=h) for p,n,h in retained_pins]
        real['native_config'].update(original_seal=descriptors[16],prefix=descriptors[17],panels=[
            dict(dataset=d,root=descriptors[i*8],requests=descriptors[i*8+6],truth=descriptors[i*8+7],truth_width=100)
            for i,d in enumerate(('relaion','cohere'))])
        if corrected:
            real['native_config']['closed_populations'] = descriptors[18]
        real['native_config_sha256']=sha(encoded(real['native_config']))
        if not corrected:
            require(pin(encoded(real['native_config']).rstrip(b'\n'))==(pin(encoded(prospective).rstrip(b'\n')) if histogram else dict(bytes=1705,
                sha256='23defe690a3fc9b193ed6e1cc8b91b58ace57b843cf0adeebaf4e124b6a30b4e')), 'exact native config with authenticated sealed request destinations')
        real['inputs']=[dict(destination=p,bytes=n,sha256=h,key='retained/'+str(i)) for i,(p,n,h) in enumerate(retained_pins)]
        real['fixed']['scratch']['input_bytes']=sum(p[1] for p in retained_pins)
        original_regular=regular
        def metadata_only(path):
            require(str(Path(path).absolute()) not in {p for p,_,_ in retained_pins},'metadata preflight opened retained data')
            return original_regular(path)
        with patch.object(module,'INPUT_PINS',retained_pins), patch.object(module,'regular',side_effect=metadata_only):
            write(base/CONFIG,encoded(real));preflight(base)
        print('PASS actual18 retained metadata'+(' + exact closed-populations metadata' if corrected else '')+'; both78000000B records opaque; no data/GT opens',flush=True)
        sq4_qualification(config, base)
        for key,value in (('authority_pending',True), ('native_qualification',[]), ('binary',{}), ('native_config_sha256','0'*64)):
            bad=copy.deepcopy(config);bad[key]=value;refused(lambda:validate_config(bad))
        bad=copy.deepcopy(config);bad['fixed']['scratch']['cap_bytes']=1
        refused(lambda:validate_config(bad))
        bad=copy.deepcopy(config);bad['native_config']['caps']['cpu_threads']=True
        bad['native_config_sha256']=sha(encoded(bad['native_config']))
        refused(lambda:validate_config(bad))
        if histogram:
            for key,value in (('schema','borsuk-fixed-sq4-config-v1'),('source_identity_sha256','0'*64),('extra',True)):
                bad=copy.deepcopy(config);bad['native_config'][key]=value
                bad['native_config_sha256']=sha(encoded(bad['native_config']))
                refused(lambda:validate_config(bad))
            bad=copy.deepcopy(config);bad['native_source']['qualification_protocol_sha256']=SQ4_QUALIFICATION_PROTOCOL_SHA
            refused(lambda:validate_config(bad))
        if corrected:
            for key,value in (('rotation_seed',[True]*32),('construction_operations',20000000000),
                    ('query_auth_operations',43710342688),('closed_populations',{}),('extra',True)):
                bad=copy.deepcopy(config);bad['native_config'][key]=value
                bad['native_config_sha256']=sha(encoded(bad['native_config']));refused(lambda:validate_config(bad))
            for key in ('rotation_seed','construction_operations','query_auth_operations','closed_populations'):
                bad=copy.deepcopy(config);del bad['native_config'][key]
                bad['native_config_sha256']=sha(encoded(bad['native_config']));refused(lambda:validate_config(bad))
            bad=copy.deepcopy(config);bad['fixed']['native_caps']['deadline_seconds']=1800
            bad['native_config']['caps']['deadline_seconds']=1800
            bad['native_config_sha256']=sha(encoded(bad['native_config']));refused(lambda:validate_config(bad))
        # Rehashing all affected receipts cannot admit failed/reordered/missing gates.
        for fault in ('failed','reordered','missing','zero-test','wrongargv','missingmandatory','reducedroster',
                      'misplacedtests','utc','overlap','reversed','non-test-count','pending'):
            bad=copy.deepcopy(config);failed=copy.deepcopy(receipt_bodies)
            stages=failed['workspace-receipt.json']['stages']
            if fault=='failed':stages[0]['exit_status']=2
            elif fault=='reordered':stages[0],stages[1]=stages[1],stages[0]
            elif fault=='missing':stages.pop()
            elif fault=='zero-test':stages[5].update(tests_run=0,required_test_passes={})
            elif fault=='wrongargv':stages[5]['command'][6]='zero_match_filter'
            elif fault=='missingmandatory':stages[5]['required_test_passes'].pop(next(iter(stages[5]['required_test_passes'])))
            elif fault=='reducedroster':
                stages[5]['required_test_passes']={}
                for n in ('source-qualification.json','workspace-receipt.json'):failed[n]['mandatory_tests']['graph-regressions']=[]
            elif fault=='misplacedtests':stages[1]['required_test_passes'].update(stages[2]['required_test_passes']);stages[2]['required_test_passes']={}
            elif fault=='utc':stages[5]['started_at']='2026-10-05T13:05:00+01:00'
            elif fault=='overlap':stages[5]['started_at']=(datetime.fromisoformat(stages[4]['started_at'])-timedelta(seconds=1)).isoformat()
            elif fault=='reversed':stages[5]['finished_at']=(datetime.fromisoformat(stages[5]['started_at'])-timedelta(seconds=1)).isoformat()
            elif fault=='pending':failed['workspace-receipt.json']['mandatory_test_names_pending']=True
            else:stages[0]['tests_run']=1
            failed['parent-verification.json']['stages']=stages
            qualification_sha=pin(encoded(failed['source-qualification.json']))['sha256']
            failed['workspace-receipt.json']['qualification_sha256']=qualification_sha
            failed['aws-terminal.json']['source_qualification_sha256']=qualification_sha
            failed['aws-terminal.json']['artifacts']['workspace-receipt.json']=pin(encoded(failed['workspace-receipt.json']))
            for r in bad['native_qualification']:
                name=Path(r['path']).name;p=base/fault/name;write(p,encoded(failed[name]))
                r.update(path=fault+'/'+name,**file_pin(p))
            try:refused(lambda:sq4_qualification(bad,base))
            except AssertionError as error:raise AssertionError('unsafe rebound qualification: '+fault) from error
        bad=copy.deepcopy(config);bad['native_source']['full_source_identity_sha256']='0'*64
        refused(lambda:sq4_qualification(bad,base))
        original=read(base/'proofs/workspace-receipt.json')
        (base/'proofs/workspace-receipt.json').write_bytes(original+b' ')
        refused(lambda:sq4_qualification(config,base));(base/'proofs/workspace-receipt.json').write_bytes(original)
        if corrected:
            for key in ('source_before_equals_after','all_source_blobs_independently_matched','descendants_drained'):
                original=read(base/'proofs/parent-verification.json');bad=decode(original);bad[key]=False
                (base/'proofs/parent-verification.json').write_bytes(encoded(bad))
                refused(lambda:sq4_qualification(config,base));(base/'proofs/parent-verification.json').write_bytes(original)
        (base/CONFIG).unlink();write(base/CONFIG, encoded(config))
        proof=preflight(base)
        userdata=user_data('a'*40,'b'*64,'mock/archive',PREFIX+'a0001',proof)
        require(('--corrected-four-bit --remote' if corrected else '--histogram-sq4 --remote' if histogram else '--sq4 --remote') in userdata
                and 'DelegateSubgroup=supervisor' in userdata, 'SQ4 SDK delegated bootstrap')
        require(all(s in userdata for s in ('ReadOnlyPaths=/tmp /var/tmp','export TMPDIR=', 'Dir::Cache::archives=',
                'Dir::State::lists=', 'Dir::Log=', '--setenv=TMPDIR=', 'bootstrap/input/output overlap cap',
                'source archive scratch reserve', 'source extraction overlap reserve', 'wait "$watcher"')), 'charged SQ4 generated Bash')
        def bootstrap_fixture(root, fault=False):
            (root/'bootstrap/tmp').mkdir(parents=True)
            with patch.object(module,'REMOTE_ROOT',root):
                generated=user_data('a'*40,'b'*64,'mock/archive',PREFIX+'a0001',proof)
            watcher=generated.split("<<'WATCH'\n",1)[1].split('\nWATCH\n',1)[0]
            compile(watcher,'generated-bootstrap-watch','exec')
            process=subprocess.Popen([sys.executable,'-c',"import os,sys;sys.argv=['watch',str(os.getpid())];"+watcher],
                stdout=subprocess.PIPE,stderr=subprocess.PIPE)
            started=time.monotonic()
            while not (root/'bootstrap/ready').exists() and process.poll() is None:
                require(time.monotonic()-started < 3,'bootstrap admission ACK deadline');time.sleep(.02)
            require(process.poll() is None,'bootstrap admission before writes')
            if fault:
                with (root/'bootstrap/tmp/fault').open('xb') as f:f.truncate(SQ4_SCRATCH_CAP)
            process.terminate()
            stdout,stderr=process.communicate(timeout=3)
            require((process.returncode != 0 and not (root/'bootstrap/scratch.json').exists()) if fault
                    else (process.returncode == 0 and decode(read(root/'bootstrap/scratch.json'))['closed'] is True), 'generated bootstrap actual observer closure')
        bootstrap_denied=base/'bootstrap-denied';bootstrap_denied.mkdir();bootstrap_fixture(bootstrap_denied,True)
        import io
        from types import SimpleNamespace
        class Zeros:
            remaining=78000000
            def __enter__(self):return self
            def __exit__(self,*args):pass
            def read(self,n):
                require(0 < n <= 65536,'large input streaming read cap')
                count=min(n,self.remaining);self.remaining-=count;return bytes(count)
        large=base/'large.bin'
        digest='df4fabbabc07f9e687ba5ae43d2cb72c3c0f8f08e4dfdc6f9d8c91412ef9e7ff'
        descriptor=dict(destination=str(large),bytes=78000000,sha256=digest,key='mock/large')
        refused(lambda:body_pin({k:descriptor[k] for k in ('bytes','sha256')}))
        with patch.object(module,'INPUT_PINS',((str(large),78000000,digest),)):
            download(SimpleNamespace(get_object=lambda **kw:dict(Body=Zeros(),ContentLength=78000000)),descriptor,large)
        require(file_pin(large)==dict(bytes=78000000,sha256=digest),'78000000B synthetic streamed authentication')
        large.unlink()
        store={}
        bodies={'mock/native':fake, **{d['key']:b'x' for d in config['inputs']}}
        calls = [0]
        def get(**kwargs):
            calls[0] += 1
            if kwargs['Key'] in store:
                p=store[kwargs['Key']]
                return dict(Body=regular(p),ContentLength=p.stat().st_size)
            b=bodies[kwargs['Key']]
            return dict(Body=io.BytesIO(b),ContentLength=len(b))
        def put(**kwargs):
            require(kwargs['IfNoneMatch']=='*' and kwargs['Key'] not in store,'immutable put')
            p=base/'store'/sha(kwargs['Key'].encode());write(p,kwargs['Body']);store[kwargs['Key']]=p
        s3=SimpleNamespace(get_object=get,put_object=put)
        files=dict(zip(CGROUP_FILES,(str(CAPS['memory_bytes']),'4096','0','0',
            'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n','high 0\nmax 0\nfail 0\n',
            '100000 100000','usage_usec 1\nuser_usec 1\nsystem_usec 0\n',str(FIXED['tasks_max']),'0','max 0\n','','populated 0\nfrozen 0\n')))
        def fixture(name,fault='',deadline=False):
            root=base/name;root.mkdir()
            write(root/'config.json',encoded(config));write(root/'source-qualification.json',encoded(proof))
            write(root/'run-closed.log',b'');write(root/'cpu.txt',b'mock')
            write(root/'runtime-abi.json',encoded(dict(machine='x86_64',python=[3,12],os=dict(ID='ubuntu',VERSION_ID='24.04'),
                libc=['glibc','2.39'],sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True))))
            for r in receipts:write(root/'qualification'/Path(r['path']).name,read(base/r['path']))
            bootstrap_fixture(root)
            for p,_,_ in pins:
                if Path(p).exists():Path(p).unlink()
            stage(s3,config,root)
            parent=base/(name+'-cgroups')/(SUPERVISOR_UNIT+'.service');parent.mkdir(parents=True)
            group=parent/'native';owned=[]
            def delegate(record):
                record.update(unit=SUPERVISOR_UNIT+'.service',parent=str(parent),observer=str(parent/'supervisor'),observer_pid=os.getpid(),
                    available=sorted(CONTROLLERS),enabled=sorted(CONTROLLERS),parent_process_ids=[],observer_process_ids=[os.getpid()],parent_type='domain')
                return group
            def create(g):(g/'cgroup.procs').write_text('')
            def snapshot(g):return dict(path=str(g),observer_pid=os.getpid(),files=copy.deepcopy(files))
            def drain(g):
                if owned and owned[0].poll() is None:os.killpg(owned[0].pid,signal.SIGKILL)
                (g/'cgroup.procs').unlink()
            original_popen=subprocess.Popen
            def spawn(command,**kwargs):
                p=original_popen(command,**kwargs);owned.append(p);(group/'cgroup.procs').write_text('');return p
            with ExitStack() as execution:
                for method,fn in (('delegated_group',delegate),('create_group',create),('cgroup_snapshot',snapshot),('drain_group',drain)):
                    execution.enter_context(patch.object(module,method,side_effect=fn))
                execution.enter_context(patch.object(subprocess,'Popen',side_effect=spawn))
                execution.enter_context(patch.dict(os.environ,SQ4_FAKE=fault))
                execution.enter_context(patch.dict(CAPS,deadline_seconds=.2 if deadline else 600))
                if corrected:
                    execution.enter_context(patch.object(module,'CORRECTED_SUPERVISOR_SECONDS',.2 if deadline else 1800))
                    if fault=='sync':
                        original_sync=sync_directory
                        def sync_failure(path):
                            if path==root/'screen':raise OSError('mock screen directory sync')
                            return original_sync(path)
                        execution.enter_context(patch.object(module,'sync_directory',side_effect=sync_failure))
                if fault in ('scratch','external-temp'):
                    try:supervise(config,root,run_id=PREFIX+'a0001/i-original')
                    except ValueError:pass
                    else:raise AssertionError('scratch fault accepted '+fault+': '+read(root/'native-exit.json').decode())
                    receipt=dict(process_exit_code=owned[0].returncode)
                else:receipt=supervise(config,root,run_id=PREFIX+'a0001/i-original')
            require(not group.exists(),'drained mock cgroup')
            return root,receipt
        root,receipt=fixture('success')
        result=validate_result(root,config)
        require(receipt['process_exit_code']==0 and result['status']=='SURVIVED_CONSUMED_PANELS' and read(root/'native.log')==b'','silent original closure')
        require(receipt['report_sha256']==file_pin(root/'screen/report.json')['sha256'],'independent terminal report binding')
        refused(lambda:supervise(config,root,run_id='overwrite'))
        if histogram:
            stem=f'screen/report.{SQ4_NAME}-0.bin-'
            seal_name=f'screen/report.{SQ4_NAME}-payloads.json'
            freeze_name=f'screen/report.{SQ4_NAME}-freeze.json'
            def rebound_fault(name, mutate, binary=False):
                names={name,stem+'root.json',seal_name,freeze_name,'screen/report.json','native-exit.json','scratch.json'}
                saved={n:read(root/n) for n in names}
                try:
                    value=read(root/name) if binary else decode(read(root/name))
                    altered=mutate(value)
                    (root/name).write_bytes(altered if binary else encoded(value))
                    g=decode(read(root/(stem+'root.json')))
                    if name==stem+'book.bin':
                        g['book'].update(file_pin(root/name));(root/(stem+'root.json')).write_bytes(encoded(g))
                    seal=decode(read(root/seal_name));generation=seal['payloads'][0]['histogram_generation']
                    generation['root'].update(file_pin(root/(stem+'root.json')))
                    generation['book']=g['book']
                    (root/seal_name).write_bytes(encoded(seal))
                    freeze=decode(read(root/freeze_name));freeze['payloads']=seal['payloads']
                    freeze['payload_seal'].update(file_pin(root/seal_name));(root/freeze_name).write_bytes(encoded(freeze))
                    report=decode(read(root/'screen/report.json'));report['details']['freeze'].update(file_pin(root/freeze_name))
                    (root/'screen/report.json').write_bytes(encoded(report))
                    for n in ('native-exit.json','scratch.json'):
                        v=decode(read(root/n));v['report_sha256']=file_pin(root/'screen/report.json')['sha256']
                        (root/n).write_bytes(encoded(v))
                    refused(lambda:validate_result(root,config))
                finally:
                    for n,b in saved.items():(root/n).write_bytes(b)
            rebound_fault(stem+'root.json',lambda v:v.update(source_identity_sha256='0'*64))
            rebound_fault(stem+'root.json',lambda v:v.update(config_sha256='0'*64))
            rebound_fault(stem+'book.bin',lambda b:b[:16]+bytes(32)+b[48:],True)
            rebound_fault(freeze_name,lambda v:v.update(truth_opened=True))
            rebound_fault(freeze_name,lambda v:v['results'].pop())
            rebound_fault('screen/report.json',lambda v:v.update(schema='borsuk-fixed-sq4-report-v1'))
            rebound_fault('screen/report.json',lambda v:v.update(source_identity_sha256='0'*64))
            for suffix in ('book.bin','groups.bin','root.json'):
                p=root/(stem+suffix);saved=read(p);p.unlink()
                try:refused(lambda:validate_result(root,config))
                finally:write(p,saved)
            p=root/'screen/extra';write(p,b'synthetic');refused(lambda:validate_result(root,config));p.unlink()
        if corrected:
            for suffix in ('rotation.bin','startup.json','0-root.json','1-groups.bin'):
                path=root/('screen/report.corrected-four-bit-'+suffix);saved=read(path,8*1024**2)
                path.write_bytes(saved+b' ');refused(lambda:validate_result(root,config));path.write_bytes(saved)
                path.unlink();refused(lambda:validate_result(root,config));write(path,saved)
            # Preserve all enclosing hashes: semantic binding still refuses.
            names=('screen/report.corrected-four-bit-freeze.json','screen/report.json','native-exit.json','scratch.json')
            saved={n:read(root/n) for n in names}
            for field,value in (('truth_opened',True),('source_identity_sha256','0'*64),('closed_populations',{}),('results',[])):
                freeze=decode(saved[names[0]]);freeze[field]=value;(root/names[0]).write_bytes(encoded(freeze))
                report=decode(saved[names[1]]);report['details']['freeze'].update(file_pin(root/names[0]))
                (root/names[1]).write_bytes(encoded(report))
                for n in names[2:]:
                    receipt=decode(saved[n]);receipt['report_sha256']=file_pin(root/names[1])['sha256'];(root/n).write_bytes(encoded(receipt))
                refused(lambda:validate_result(root,config))
                for n,b in saved.items():(root/n).write_bytes(b)
        for fault in (('reject','latefailure','drift','deadline','scratch','external-temp','sync') if corrected else ('reject','envelope','latefailure','drift','deadline','scratch','external-temp')):
            out,exit_receipt=fixture(fault,fault,deadline=fault=='deadline')
            if fault in ('reject','envelope'):require(validate_result(out,config)['status']=='REJECT','completed REJECT retained')
            else:
                refused(lambda:validate_result(out,config))
                if fault=='latefailure':require(exit_receipt['process_exit_code']==2 and decode(read(out/'screen/report.json'))['complete'] is True,'late exit2 invalidates complete body')
        for name in (f'screen/report.{SQ4_NAME}-1.bin',f'screen/report.{SQ4_NAME}-result-127.json',
                     f'screen/report.{SQ4_NAME}-freeze.json','cleanup.json','resources.json','scratch.json'):
            p=root/name;b=read(p,64*1024**2);p.write_bytes(b+b' ')
            if not name.startswith('screen/'):
                value=decode(b);value[{'cleanup.json':'cleanup_complete','resources.json':'closed','scratch.json':'closed'}[name]]=False
                p.write_bytes(encoded(value))
            refused(lambda:validate_result(root,config));p.write_bytes(b)
        denied=base/'scratch-denied';denied.mkdir()
        bootstrap_fixture(denied)
        with (denied/'temporary').open('xb') as f:f.truncate(SQ4_SCRATCH_CAP)
        previous_calls=calls[0]
        refused(lambda:stage(s3,config,denied))
        require(calls[0]==previous_calls and not (denied/BINARY_NAME).exists(),'scratch refuses before hydration')
        prefix=PREFIX+'a0001'
        terminal=dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,instance_id='i-original',prefix=prefix,
            config_sha256=sha(encoded(config)),status='complete',phase='complete',exit_code=0,original_exit_code=0,
            disposition=result['status'],artifact_roster_sha256=ROSTER_SHA,qualification=proof,result=result)
        near_cap=dict(roots={str(root):SQ4_SCRATCH_CAP-4096,str(SQ4_INPUT_ROOT):0},whole_scratch_bytes=SQ4_SCRATCH_CAP-4096)
        previous_uploads=len(store)
        with patch.object(module,'scratch_observation',return_value=near_cap):
            refused(lambda:publish(s3,root,prefix,copy.deepcopy(terminal)))
        require(len(store)==previous_uploads and not (root/'terminal.json').exists(),'near-cap closure refused before writes/uploads')
        publish(s3,root,prefix,terminal)
        out=base/'collected';out.mkdir()
        write(out/'aws-reservation.json',encoded(dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,
            config_sha256=terminal['config_sha256'],qualification=proof)))
        write(out/'aws-launch.json',encoded(dict(instance_id='i-original',nodes={'0':{'instance_id':'i-original'}},prefix=prefix,
            source_commit='a'*40,source_archive_sha256='b'*64)))
        write(out/'aws-closeout.json',encoded(dict(state='terminated',nodes={'0':{'instance_id':'i-original'}})))
        collect(s3,prefix,out,'i-original','a'*40,'b'*64)
        require(replay(out)==result,'all raw/gzip native closure replay')
        require(read(out/'screen/report.json') == read(root/'screen/report.json')
            and all(read(out/'qualification'/n) == read(root/'qualification'/n) for n in SQ4_RECEIPTS), 'byte-exact native report and qualification receipts')
        refused(lambda:collect(s3,prefix,out,'i-original','a'*40,'b'*64))
        p=out/f'screen/report.{SQ4_NAME}-result-127.json';saved=read(p)
        p.write_bytes(saved+b' ');refused(lambda:replay(out));p.write_bytes(saved)
        if histogram or corrected:
            p=out/'aws-terminal.json';saved=read(p)
            for field,value in (('original_exit_code',2),('artifact_roster_sha256','0'*64),('instance_id','i-other')):
                bad=decode(saved);bad[field]=value;p.write_bytes(encoded(bad));refused(lambda:replay(out))
            bad=decode(saved);bad.update(status='failed',phase='execution',exit_code=96,original_exit_code=2,disposition='INVALID')
            p.write_bytes(encoded(bad));require(replay(out)['status']=='INVALID','execution INVALID distinct from complete REJECT')
            p.write_bytes(saved)
        print('PASS '+SQ4_NAME+' mock-native: exact'+str(15 if corrected else 14)+' argv/mandatory owning-stage passes/positive test counts/serial UTC; generated Bash/bootstrap observer; charged external-temp fault; near-cap closure; exact CLI; silent exit0; REJECT; late exit2; deadline; config/output drift; no overwrite; scratch; cleanup; full raw/gzip collection/replay; byte-exact report/qualification. Cgroup/SDK transport/AWS/qualification metadata SYNTHETIC; no native science.')


def canary_self_check():
    """Source-only checks; transport and usage process are synthetic."""
    assert callable(globals().get('configure_canary')), 'missing corrected infrastructure canary entry'
    import copy
    import io
    import tempfile
    from contextlib import ExitStack
    from types import SimpleNamespace
    from unittest.mock import patch
    module = sys.modules[__name__]
    repo = Path(__file__).resolve().parents[1]
    if not CANARY:
        configure_sq4(corrected=True); configure_canary()
    def refused(call):
        try:call()
        except (ValueError,OSError,KeyError,AssertionError,ImportError):return
        raise AssertionError('unsafe canary accepted')
    sdk_guard()  # Actual installed 1.40.72 service model; never create a client.
    import botocore.session
    old_model = SimpleNamespace(get_service_model=lambda _:SimpleNamespace(operation_model=lambda _:
        SimpleNamespace(input_shape=SimpleNamespace(members={}))))
    with patch.object(botocore.session,'get_session',return_value=old_model):refused(sdk_guard)
    import boto3
    with patch.object(boto3,'__version__','wrong'):refused(sdk_guard)
    setup_self_check()
    science = decode(read(repo/SCIENCE_STATE['ROOT']/'config-draft.json'))
    science['code_sha256'] = {n:file_pin(repo/n)['sha256'] for n in CODE}
    require(canary_imports(repo,science) == science['code_sha256'],'actual local import closure')
    refused(lambda:canary_imports(repo/'wrong-origin',science))
    import importlib
    with patch.object(importlib,'import_module',side_effect=ImportError('missing import')):refused(lambda:canary_imports(repo,science))
    sentinel_hooks = []
    with patch.object(sys,'addaudithook',side_effect=sentinel_hooks.append):canary_sentinels()
    refused(lambda:sentinel_hooks[0]('open',(str(SQ4_INPUT_ROOT/'screen/retained/cohere/truth64'),'r',0)))
    probe = subprocess.run([sys.executable,'-c',
        'from pathlib import Path; from scripts import launch_fine_pack_diagnostic as m; '
        'm.SQ4_INPUT_ROOT=Path("/tmp/canary-forbidden-probe"); m.canary_sentinels(); '
        '(m.SQ4_INPUT_ROOT/"panel-truth").open("rb")'],capture_output=True,text=True)
    require(probe.returncode != 0 and 'canary forbids panel/GT opens' in probe.stderr, 'actual audit hook before panel/GT open')
    usage = b'#!/usr/bin/env python3\nimport sys\nassert sys.argv[1:]==["check-fine-corrected-four-bit"]\nprint("usage: hierarchical_semantic_cells check-fine-corrected-four-bit CONFIG CONFIG_SHA256 NEW_REPORT_JSON")\nsys.exit(2)\n'
    with tempfile.TemporaryDirectory(prefix='corrected-canary-check-') as tmp, ExitStack() as stack:
        base = Path(tmp); input_root = base/'forbidden'; input_root.mkdir()
        stack.enter_context(patch.object(module,'SQ4_INPUT_ROOT',input_root))
        pins = tuple((p.replace('/mnt/hierarchical-100k',str(input_root)),n,h) for p,n,h in INPUT_PINS)
        stack.enter_context(patch.object(module,'INPUT_PINS',pins))
        science = decode(encoded(science).replace(b'/mnt/hierarchical-100k',str(input_root).encode()))
        science['native_config_sha256'] = sha(encoded(science['native_config']))
        for n in (*CODE,str(SQ4_ROOT/'prospective-input-roster.json'),*(str(CORRECTED_ROOT/n) for n,_,_ in CORRECTED_EVIDENCE)):
            write(base/n,read(repo/n))
        for r in science['native_qualification']:write(base/r['path'],read(repo/r['path']))
        # First authenticate the actual completed all405/all15 metadata receipts.
        sq4_qualification(science,base)
        # Only the usage executable and its qualification artifact bindings become synthetic.
        synthetic_pin = pin(usage)
        stack.enter_context(patch.object(module,'CANARY_BINARY_PIN',synthetic_pin))
        science['binary'].update(synthetic_pin)
        science['fixed']['scratch']['binary_bytes'] = synthetic_pin['bytes']
        receipts = {Path(r['path']).name:decode(read(base/r['path'])) for r in science['native_qualification']}
        receipts['workspace-receipt.json']['artifacts'][BINARY_NAME] = synthetic_pin
        receipts['aws-terminal.json']['artifacts'][BINARY_NAME] = synthetic_pin
        receipts['aws-terminal.json']['artifacts']['workspace-receipt.json'] = pin(encoded(receipts['workspace-receipt.json']))
        receipts['parent-verification.json']['binary'] = synthetic_pin
        for r in science['native_qualification']:
            raw = encoded(receipts[Path(r['path']).name]); (base/r['path']).write_bytes(raw); r.update(pin(raw))
        ref_path = SCIENCE_STATE['ROOT']/'config-draft.json'; science_raw = encoded(science); write(base/ref_path,science_raw)
        config = copy.deepcopy(science)
        config.update(schema=SCHEMA,authority_pending=False,fixed=dict(FIXED,scratch=dict(
            input_bytes=175929,native_output_bytes=CAPS['output_bytes'],binary_bytes=synthetic_pin['bytes'],
            source_archive_bytes=32*1024**2,bootstrap_bytes=1024**3,auxiliary_bytes=32*1024**2,cap_bytes=SQ4_SCRATCH_CAP)),
            canary=dict(science_authority=dict(path=str(ref_path),**pin(science_raw)),science_config=copy.deepcopy(science),metadata_only_admission=True))
        paths = sorted([str(CONFIG),str(ref_path),str(SQ4_ROOT/'prospective-input-roster.json'),*CODE,
            *(r['path'] for r in config['native_qualification']),*(str(CORRECTED_ROOT/n) for n,_,_ in CORRECTED_EVIDENCE)])
        config.update(source_archive_paths=paths,source_archive_paths_sha256=sha(json.dumps(paths,separators=(',',':')).encode()))
        write(base/CONFIG,encoded(config)); proof = preflight(base)
        for alter in (lambda c:c.update(authority_pending=True),
            lambda c:c['canary'].update(metadata_only_admission=False),
            lambda c:c['native_source'].update(commit='0'*40),
            lambda c:c['fixed']['native_caps'].update(memory_bytes=1024**3),
            lambda c:c['inputs'][0].update(key='wrong/key'),
            lambda c:c['binary'].update(sha256='0'*64)):
            bad = copy.deepcopy(config); alter(bad); refused(lambda:validate_config(bad,base))
        p = base/config['native_qualification'][2]['path']; saved = read(p)
        bad = decode(saved); bad['mandatory_test_names_pending'] = True; p.write_bytes(encoded(bad))
        bad_config = copy.deepcopy(config);bad_config['native_qualification'][2].update(file_pin(p))
        refused(lambda:sq4_qualification(bad_config,base));p.write_bytes(saved)
        p = base/CODE[0];saved = read(p);p.write_bytes(saved+b' ');refused(lambda:preflight(base));p.write_bytes(saved)
        generated = user_data('a'*40,'b'*64,'mock/archive',PREFIX+'a0001',proof)
        require('--remote-canary' in generated and '--remote "' not in generated
            and 'MemoryMax=256M' in generated and 'canary source archive roster' in generated,'bounded generated bootstrap')
        fake_bin = base/'bin';fake_bin.mkdir()
        write(fake_bin/'systemd-run',('#!'+sys.executable+'\nimport json,sys\nprint(json.dumps(sys.argv[1:]))\n').encode());(fake_bin/'systemd-run').chmod(0o500)
        opening = generated.split('\nroot=',1)[0]
        env = dict(os.environ,PATH=str(fake_bin)+':'+os.environ['PATH'],BORSUK_CANARY_BOOTSTRAP_CAPPED='0')
        invoked = subprocess.run(['bash','-c',opening],env=env,capture_output=True,text=True,check=True)
        commands = [decode(line) for line in invoked.stdout.splitlines()]
        require(len(commands)==1 and '--unit=fine-pack-canary-bootstrap' in commands[0], 'bootstrap cap before exactly one shutdown timer')
        compile(generated.split("<<'WATCH'\n",1)[1].split('\nWATCH\n',1)[0],'canary-bootstrap-watch','exec')
        metadata = read(repo/CORRECTED_ROOT/'closed-populations.json')
        gets,heads,objects = [],[],{}
        def get(**kw):
            key = kw['Key'];gets.append(key)
            if key in objects:body = objects[key]
            elif key == config['binary']['key']:body = usage
            elif key == config['inputs'][-1]['key']:body = metadata
            else:raise AssertionError('dataset body GET')
            return dict(Body=io.BytesIO(body),ContentLength=len(body))
        def head(**kw):
            key = kw['Key'];heads.append(key)
            d = next(d for d in (*config['inputs'],config['binary']) if d['key']==key)
            return dict(ContentLength=d['bytes'],Metadata=dict(sha256=d['sha256']))
        def put(**kw):
            require(kw['IfNoneMatch']=='*' and kw['Key'] not in objects,'immutable canary output')
            objects[kw['Key']] = kw['Body']
        transport = SimpleNamespace(get_object=get,head_object=head,put_object=put)
        def prepare(name):
            root = base/name;root.mkdir();(root/'bootstrap/tmp').mkdir(parents=True)
            write(root/'bootstrap/scratch.json',encoded(dict(closed=True,cap_exceeded=False,interval_seconds=1,
                sample_count=2,peak_bytes=0,reserve=config['fixed']['scratch'])))
            return root
        for fault in ('missing','head-size','body-sha'):
            root = prepare(fault); before = len(gets)
            with ExitStack() as faults:
                if fault=='missing':faults.enter_context(patch.object(transport,'head_object',side_effect=KeyError('missing key')))
                if fault=='head-size':faults.enter_context(patch.object(transport,'head_object',return_value=dict(ContentLength=0)))
                if fault=='body-sha':faults.enter_context(patch.object(transport,'get_object',return_value=dict(Body=io.BytesIO(b'x'),ContentLength=175929)))
                refused(lambda:stage(canary_transport(transport,config,PREFIX+'a0001'),config,root))
            if fault!='body-sha':require(len(gets)==before and not (root/BINARY_NAME).exists(),'HEAD refusal before any body GET')
        root = prepare('original'); s3 = canary_transport(transport,config,PREFIX+'a0001')
        descriptor = config['inputs'][0];before = len(gets)
        refused(lambda:download(s3,descriptor,root/'unsafe'))
        refused(lambda:s3.get_object(Bucket=BUCKET,Key=config['inputs'][7]['key']))
        require(len(gets)==before,'no panel/GT GET reaches transport')
        denied = prepare('scratch-refusal')
        with (denied/'temporary').open('xb') as f:f.truncate(SQ4_SCRATCH_CAP)
        before=len(gets);refused(lambda:stage(s3,config,denied));require(len(gets)==before,'scratch refusal before transport')
        stage(s3,config,root)
        require(heads[-20:] == [d['key'] for d in (*config['inputs'],config['binary'])]
            and gets[-2:] == [config['binary']['key'],config['inputs'][-1]['key']],'HEAD every19+binary; GET metadata+usage only')
        previous_heads=len(heads);refused(lambda:stage(s3,config,root));require(len(heads)==previous_heads,'one original staging attempt before dispatch')
        with patch.object(transport,'get_object',return_value=dict(Body=io.BytesIO(b'x'*175929),ContentLength=175929)):
            refused(lambda:download(s3,config['inputs'][-1],base/'wrong-sha/closed-populations.json'))
        write(root/'config.json',encoded(config));write(root/'science-config.json',science_raw)
        write(root/'source-qualification.json',encoded(proof));write(root/'run-closed.log',b'');write(root/'cpu.txt',b'mock')
        write(root/'imports.json',encoded(config['code_sha256']))
        write(root/'sentinels.json',encoded(dict(panel_gt_root=str(input_root),fail_fast_open_sentinel=True,native_usage_only=True)))
        write(root/'runtime-abi.json',encoded(dict(machine='x86_64',python=[3,12],os=dict(ID='ubuntu',VERSION_ID='24.04'),
            libc=['glibc','2.39'],sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True))))
        for r in config['native_qualification']:write(root/'qualification'/Path(r['path']).name,read(base/r['path']))
        parent = base/(SUPERVISOR_UNIT+'.service');parent.mkdir();group = parent/'native'
        files = dict(zip(CGROUP_FILES,(str(CAPS['memory_bytes']),'4096','0','0',
            'oom 0\noom_kill 0\noom_group_kill 0\n','high 0\nmax 0\nfail 0\n','100000 100000',
            'usage_usec 1\nuser_usec 1\nsystem_usec 0\n',str(FIXED['tasks_max']),'0','max 0\n','','populated 0\nfrozen 0\n')))
        def delegate(record):
            record.update(unit=SUPERVISOR_UNIT+'.service',parent=str(parent),observer=str(parent/'supervisor'),observer_pid=os.getpid(),
                available=sorted(CONTROLLERS),enabled=sorted(CONTROLLERS),parent_process_ids=[],observer_process_ids=[os.getpid()],parent_type='domain')
            return group
        original_popen = subprocess.Popen
        def spawn(command,**kw):
            require(command==[str(root/BINARY_NAME),SQ4_CLI],'usage-only command before spawn')
            kw.pop('preexec_fn');kw.pop('pass_fds');return original_popen(command,**kw)
        with ExitStack() as mocks:
            for method,fn in (('delegated_group',delegate),('create_group',lambda g:(g/'cgroup.procs').write_text('')),
                ('cgroup_snapshot',lambda g:dict(path=str(g),observer_pid=os.getpid(),files=copy.deepcopy(files))),
                ('drain_group',lambda g:(g/'cgroup.procs').unlink())):
                mocks.enter_context(patch.object(module,method,side_effect=fn))
            mocks.enter_context(patch.object(subprocess,'Popen',side_effect=spawn))
            receipt = supervise(config,root,run_id=PREFIX+'a0001/i-original')
        require(receipt['process_exit_code']==2 and not group.exists(),'observed original usage exit2 and cleanup')
        denied = prepare('pre-exec-cpu-refusal');stage(canary_transport(transport,config,PREFIX+'a0001'),config,denied)
        files['cpu.max'] = '200000 100000'
        with ExitStack() as mocks:
            for method,fn in (('delegated_group',delegate),('create_group',lambda g:(g/'cgroup.procs').write_text('')),
                ('cgroup_snapshot',lambda g:dict(path=str(g),observer_pid=os.getpid(),files=copy.deepcopy(files))),
                ('drain_group',lambda g:(g/'cgroup.procs').unlink())):
                mocks.enter_context(patch.object(module,method,side_effect=fn))
            mocks.enter_context(patch.object(subprocess,'Popen',side_effect=AssertionError('exec before resource admission')))
            denied_receipt = supervise(config,denied,run_id='refused')
        require(denied_receipt['process_started'] is False and not (denied/'native.log').exists(),'kernel CPU mismatch before exec')
        files['cpu.max'] = '100000 100000'
        result = validate_result(root,config);require(result['status']=='GO','metadata/usage GO')
        for name,change in (('native-exit.json',lambda d:d.update(process_exit_code=0)),
            ('cleanup.json',lambda d:d.update(cleanup_complete=False)),
            ('resources.json',lambda d:d['after']['files'].update(**{'cpu.max':'200000 100000'})),
            ('resources.json',lambda d:d['after']['files'].update(**{'memory.swap.peak':'1'})),
            ('scratch.json',lambda d:d.update(closed=False)),('imports.json',lambda d:d.update({CODE[0]:'0'*64}))):
            p = root/name;saved = read(p);bad = decode(saved);change(bad);p.write_bytes(encoded(bad))
            refused(lambda:validate_result(root,config));p.write_bytes(saved)
        p = root/'native.log';saved = read(p);p.write_bytes(b'loader failure');refused(lambda:validate_result(root,config));p.write_bytes(saved)
        write(root/'screen/forbidden-science',b'x');refused(lambda:validate_result(root,config));(root/'screen/forbidden-science').unlink()
        prefix = PREFIX+'a0001';terminal = dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,
            instance_id='i-original',prefix=prefix,config_sha256=sha(encoded(config)),status='complete',phase='complete',
            exit_code=0,original_exit_code=2,disposition='GO',artifact_roster_sha256=ROSTER_SHA,qualification=proof,result=result)
        publish(s3,root,prefix,terminal)
        out = base/'collected';out.mkdir()
        write(out/'aws-reservation.json',encoded(dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,
            config_sha256=terminal['config_sha256'],qualification=proof)))
        write(out/'aws-launch.json',encoded(dict(instance_id='i-original',nodes={'0':{'instance_id':'i-original'}},prefix=prefix,
            source_commit='a'*40,source_archive_sha256='b'*64)))
        write(out/'aws-closeout.json',encoded(dict(state='running',nodes={'0':{'instance_id':'i-original'}})))
        before = len(gets);refused(lambda:collect(transport,prefix,out,'i-original','a'*40,'b'*64));require(len(gets)==before,'no collection before terminate/wait')
        (out/'aws-closeout.json').write_bytes(encoded(dict(state='terminated',nodes={'0':{'instance_id':'i-other'}})))
        refused(lambda:collect(transport,prefix,out,'i-original','a'*40,'b'*64))
        (out/'aws-closeout.json').write_bytes(encoded(dict(state='terminated',nodes={'0':{'instance_id':'i-original'}})))
        collect(transport,prefix,out,'i-original','a'*40,'b'*64)
        require(replay(out)==result,'full original artifact/raw/gzip GO replay')
        gate = dict(science,authority_pending=False,canary_admission=dict(path=str((out/'canary-admission.json').relative_to(base)),**file_pin(out/'canary-admission.json')))
        validate_canary_admission(gate,base)
        with patch.dict(globals(),SCIENCE_STATE), patch.object(module,'CANARY',False):
            pending_science = dict(science,authority_pending=False)
            write(base/CONFIG,encoded(pending_science));refused(lambda:preflight(base))
            gate['source_archive_paths'] = sorted([*gate['source_archive_paths'],gate['canary_admission']['path']])
            gate['source_archive_paths_sha256'] = sha(json.dumps(gate['source_archive_paths'],separators=(',',':')).encode())
            (base/CONFIG).write_bytes(encoded(gate));science_proof=preflight(base)
            require(science_proof['canary_admission']==gate['canary_admission'],'science preflight requires authenticated terminated GO')
        for change in (lambda c:c['code_sha256'].update({CODE[0]:'0'*64}),lambda c:c['native_config'].update(rotation_seed=[0]*32),
            lambda c:c['inputs'][0].update(key='drift/key'),lambda c:c['native_source'].update(commit='0'*40)):
            bad=copy.deepcopy(gate);change(bad);refused(lambda:validate_canary_admission(bad,base))
        p=out/'aws-terminal.json';saved=read(p)
        for change in (lambda d:d.update(original_exit_code=0),lambda d:d.update(instance_id='i-other'),
            lambda d:d.update(artifact_roster_sha256='0'*64)):
            bad=decode(saved);change(bad);p.write_bytes(encoded(bad));refused(lambda:replay(out))
        bad=decode(saved);bad.update(status='failed',phase='execution',exit_code=96,disposition='INVALID')
        p.write_bytes(encoded(bad));require(replay(out)['status']=='INVALID','infrastructure INVALID distinct from scientific REJECT');p.write_bytes(saved)
        refused(lambda:collect(transport,prefix,out,'i-original','a'*40,'b'*64))
        s3.canary_attempts['dispatch_attempts']=128;before=len(gets)
        refused(lambda:s3.get_object(Bucket=BUCKET,Key=config['binary']['key']));require(len(gets)==before,'dispatch cap before transport')
    print('PASS corrected canary source-only: actual all405/all15 metadata and real imports/SDK; synthetic usage exit2; HEAD20/GET2; pending/drift/SHA/noGT/resource/cleanup/ACK/collection/replay/GO mismatch refusals. No AWS/network/native science.')


def pq_residual_source_self_check():
    """Synthetic receipts/opaque bytes and real tiny subprocesses; no native/data/AWS."""
    import copy
    import io
    import tempfile
    from contextlib import ExitStack
    from types import SimpleNamespace
    from unittest.mock import Mock, patch
    from datetime import timezone
    module = sys.modules[__name__]
    require(PQ_RESIDUAL_SOURCE and len(INPUT_PINS) == 11 and sum(p[1] for p in INPUT_PINS) == 172419557,
        'actual committed eleven-input roster')
    # Native source is read only through git show. No Rust compiler or binary runs.
    identity = hashlib.sha256(b'borsuk-pq-residual-source-closure-v1')
    for name in ('lib.rs','pq_residual_four_bit.rs','pq64_nominee.rs','fine_sq8_groups.rs','sq8_source.rs',
                 'exact_sq8_nominee.rs','hierarchical_semantic_cells.rs','bin/hierarchical_semantic_cells.rs'):
        body = subprocess.check_output(['git','show',PQ_MANIFEST['native_source_commit']+':crates/borsuk/src/'+name])
        identity.update(len(name).to_bytes(8,'little')); identity.update(name.encode())
        identity.update(len(body).to_bytes(8,'little')); identity.update(body)
        if name == 'fine_sq8_groups.rs':
            for suffix in ('book.bin','groups.bin','sq8.bin','cohort.bin','root.json'):
                require(('pq-residual-{index}-'+suffix).encode() in body, 'actual Rust generation output name')
            for suffix in ('selections','anchors','results','freeze'):
                require(('pq-residual-'+suffix+'.json').encode() in body, 'actual Rust seal output name')
    require(identity.hexdigest() == PQ_CONTRACT['source_identity_sha256']
        and sha(json.dumps(PQ_MANIFEST['source_sha256'],sort_keys=True,separators=(',',':')).encode())
            == PQ_MANIFEST['source_identity_sha256'] != identity.hexdigest(), 'actual distinct full406 and compiled subset identities')
    failures = 0
    def refused(fn):
        nonlocal failures
        try:fn()
        except (ValueError,OSError,AssertionError,KeyError):failures += 1
        else:raise AssertionError('unsafe closure accepted')
    fake = (f'#!{sys.executable}\n'+r'''
import hashlib,json,os,sys,time
from pathlib import Path
assert sys.argv[1]=='check-pq-residual-source' and len(sys.argv)==5
config=Path(sys.argv[2]);digest=sys.argv[3];report=Path(sys.argv[4]);c=json.loads(config.read_bytes())
assert hashlib.sha256(config.read_bytes()).hexdigest()==digest
assert os.environ['BORSUK_CPU_THREADS']==os.environ['RAYON_NUM_THREADS']=='1'
source=c['source_identity_sha256'];codec='borsuk-pq64-residual4-original-norm-v1';fault=os.environ.get('PQ_GLUE_FAKE','')
def emit(suffix,v):
 p=report if suffix is None else report.with_suffix('.pq-residual-'+suffix)
 b=v if isinstance(v,bytes) else (json.dumps(v,sort_keys=True,separators=(',',':'))+'\n').encode()
 p.write_bytes(b)
 return dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
selection=emit('selections.json',b'opaque native selections')
if fault in ('partial','deadline'):
 emit(None,dict(status='INVALID',complete=False))
 if fault=='deadline':time.sleep(10)
 sys.exit(2)
assets=[];roots=[]
for i,panel in enumerate(c['panels']):
 pins=[emit(str(i)+'-'+s,b'opaque native '+s.encode()) for s in ('book.bin','groups.bin','sq8.bin','cohort.bin')]
 root=dict(schema='borsuk-pq-residual-source-generation-v1',codec=codec,trainer='extrema-f64-endpoint256-even-dp16-nearest-lowest-v1',
  config_sha256=digest,source_identity_sha256=source,source_root=panel['root'],requests_opened=False,truth_opened=False,
  rows=100000,dimensions=768,source_passes_authenticated=3,encoded_rows=4096)
 for field,pin in zip(('book','cohort_residual_groups','cohort_native','cohort_residual'),pins):root[field]=pin
 for field,name in (('source_groups','groups.bin'),('source_order','order.bin'),('source_pq','pq.bin'),('source_records','records.bin')):
  p=Path(panel['root']['path']).parent/name;b=p.read_bytes();root[field]=dict(path=str(p),bytes=len(b),sha256=hashlib.sha256(b).hexdigest())
 rp=emit(str(i)+'-root.json',root);assets.extend(pins+[rp]);roots.append(rp)
anchors=emit('anchors.json',b'opaque native anchors');results=emit('results.json',b'opaque native results')
freeze=emit('freeze.json',dict(schema='borsuk-pq-residual-freeze-v1',config_sha256=digest,source_identity_sha256=source,
 selection=selection,anchors=anchors,results=results,artifacts=assets,generations=roots,requests_opened=False,truth_opened=False,
 all_results_sealed_before_reduction=True))
emit(None,dict(schema='borsuk-pq-residual-source-report-v1',codec=codec,source_identity_sha256=source,config_sha256=digest,
 status='REJECT' if fault=='REJECT' else 'SURVIVED_SOURCE_NEIGHBORHOODS',complete=True,standalone_authority=False,
 requires_matching_supervisor_exit_receipt=True,quality_or_performance_claim=False,requests_opened=False,truth_opened=False,
 details=dict(rows=100000,dimensions=768,cohort=4096,anchors=64,k=100,scratch_bytes=0,strict_retained_authority=True,
 native_query_constant=True,candidate_query_constant=False,freeze=freeze,operations=1,
 admission=dict(cumulative_read_bytes=11,operations=1,coexisting_bytes=4096,output_bytes=1048576,scratch_bytes=0))))
sys.exit(2 if fault=='exit2' else 0)
''').encode()
    retained = copy.deepcopy(PQ_TRANSPORT['inputs'])
    with tempfile.TemporaryDirectory(prefix='pq-glue-check-') as tmp, ExitStack() as context:
        base = Path(tmp); input_root = base/'inputs'
        inputs = [dict(d,key='mock/input-'+str(i),bytes=1,sha256=sha(b'x'),
            destination=str(input_root/Path(d['destination']).relative_to(SQ4_INPUT_ROOT))) for i,d in enumerate(retained)]
        native = copy.deepcopy(PQ_DRAFT)
        for i in range(2):native['panels'][i]['root'] = dict(path=inputs[i*5]['destination'],bytes=1,sha256=sha(b'x'))
        native['original_seal'] = dict(path=inputs[-1]['destination'],bytes=1,sha256=sha(b'x'))
        context.enter_context(patch.object(module,'SQ4_INPUT_ROOT',input_root))
        context.enter_context(patch.object(module,'INPUT_PINS',tuple((d['destination'],1,sha(b'x')) for d in inputs)))
        context.enter_context(patch.object(module,'PQ_TRANSPORT',dict(PQ_TRANSPORT,inputs=inputs)))
        context.enter_context(patch.object(module,'PQ_DRAFT',native))
        binary = dict(pin(fake),key='mock/native')
        authority = dict(commit=PQ_MANIFEST['native_source_commit'],full_source_identity_sha256=PQ_MANIFEST['source_identity_sha256'],
            source_identity_sha256=PQ_CONTRACT['source_identity_sha256'],source_sha256=PQ_CONTRACT['owned_source_sha256'],
            qualification_protocol_sha256=sha(encoded(PQ_PROTOCOL)))
        stages = []
        for i,(name,command) in enumerate(PQ_PROTOCOL['stages']):
            required = PQ_PROTOCOL['mandatory_tests'].get(name,[])
            stages.append(dict(stage=name,command=command,started_at=f'2026-10-06T00:00:{i:02}Z',finished_at=f'2026-10-06T00:00:{i:02}Z',
                exit_status=0,gate_status=0,log_exit_status=0,tests_run=len(required) if required else None,
                required_test_passes={n:1 for n in required}))
        common = dict(native_source_commit=authority['commit'],source_identity_sha256=authority['full_source_identity_sha256'],source_file_count=406)
        q = dict(common,schema='borsuk-pq-residual-implementation-gates-qualification-v1',mandatory_test_names_pending=False,
            source_sha256=PQ_MANIFEST['source_sha256'],mandatory_tests=PQ_PROTOCOL['mandatory_tests'],
            native_source_manifest=dict(path=str(PQ_EVIDENCE[1]),**PQ_EVIDENCE_PINS[str(PQ_EVIDENCE[1])]),
            native_source_manifest_sha256=PQ_EVIDENCE_PINS[str(PQ_EVIDENCE[1])]['sha256'])
        old = pin(b'unchanged source inventory')
        w = dict(common,schema='borsuk-pq-residual-implementation-gates-receipt-v1',mandatory_test_names_pending=False,
            qualified=True,command_started=True,command_completed=True,source_unchanged=True,exit_status=0,gate_status=0,
            qualification_sha256=sha(encoded(q)),source_sha256=PQ_MANIFEST['source_sha256'],stages=stages,
            mandatory_tests=PQ_PROTOCOL['mandatory_tests'],artifacts={'source-before.json':old,'source-after.json':old,BINARY_NAME:pin(fake)})
        close = dict(state='terminated',nodes={'0':{'instance_id':'i-qualified-synthetic'}})
        t = dict(common,schema='borsuk-pq-residual-implementation-gates-spot-v1',status='complete',phase='complete',exit_code=0,
            original_exit_code=0,instance_id='i-qualified-synthetic',source_qualification_sha256=sha(encoded(q)),
            native_source_manifest_sha256=q['native_source_manifest_sha256'],
            artifacts={'source-before.json':old,'source-after.json':old,'workspace-receipt.json':pin(encoded(w)),BINARY_NAME:pin(fake)})
        v = dict(common,qualified=True,stages=stages,source_before_equals_after=True,all_source_blobs_independently_matched=True,
            artifact_hashes_independently_verified=True,descendants_drained=True,original_controller_exit_status=0,
            swap_peak_bytes=0,oom=0,binary=pin(fake),instance_closeout=close)
        receipts = []
        for name,value in zip(SQ4_RECEIPTS,(v,q,w,t,close)):
            path=Path('synthetic-qualification')/name;write(base/path,encoded(value));receipts.append(dict(path=str(path),**pin(encoded(value))))
        fixed = copy.deepcopy(FIXED)
        fixed['scratch'] = dict(input_bytes=11,native_output_bytes=CAPS['output_bytes'],binary_bytes=len(fake),
            source_archive_bytes=8*1024**2,bootstrap_bytes=256*1024**2,auxiliary_bytes=16*1024**2,cap_bytes=SQ4_SCRATCH_CAP)
        paths = sorted([str(CONFIG),*CODE,*(str(p) for p in PQ_EVIDENCE),*(r['path'] for r in receipts)])
        config = dict(schema=SCHEMA,authority_pending=False,fixed=fixed,native_config=native,native_config_sha256=sha(encoded(native)),
            binary=binary,inputs=inputs,native_source=authority,native_qualification=receipts,
            code_sha256={n:file_pin(n)['sha256'] for n in CODE},source_archive_paths=paths,
            source_archive_paths_sha256=sha(json.dumps(paths,separators=(',',':')).encode()))
        for name in (*CODE,*(str(p) for p in PQ_EVIDENCE)):write(base/name,read(name))
        write(base/CONFIG,encoded(config));validate_config(config,base)
        proof = preflight(base)
        bootstrap = user_data('a'*40,'b'*64,'mock/source',PREFIX+'a0001',proof)
        require('--pq-residual-source --remote' in bootstrap and 'Delegate=cpu memory pids' in bootstrap
            and 'on-active=1500s' in bootstrap and '--retries 0' in bootstrap and 'rustc' not in bootstrap, 'existing bounded bootstrap/SDK/delegation')
        for field,value in (('authority_pending',True),('binary',{}),('native_qualification',[]),('inputs',inputs[:-1]),
                            ('native_config_sha256','0'*64)):
            bad=copy.deepcopy(config);bad[field]=value;refused(lambda:validate_config(bad,base))
        for field,value in (('full_source_identity_sha256',authority['source_identity_sha256']),('qualification_protocol_sha256','0'*64)):
            bad=copy.deepcopy(config);bad['native_source'][field]=value;refused(lambda:validate_config(bad,base))
        for change in (lambda d:d['inputs'][0].update(key='drift/key'),lambda d:d['native_config'].update(truth={}),
            lambda d:d['code_sha256'].update({CODE[0]:'0'*64}),lambda d:d['fixed']['native_caps'].update(memory_bytes=2*1024**3)):
            bad=copy.deepcopy(config);change(bad);refused(lambda:validate_config(bad,base))
        for change in (lambda d:d.update(qualified=False),lambda d:d['stages'].pop(),
                       lambda d:d['stages'][0].update(tests_run=0),lambda d:d['stages'][0]['required_test_passes'].clear(),
                       lambda d:d['stages'][0].update(exit_status=2),lambda d:d.update(mandatory_test_names_pending=True)):
            bad=copy.deepcopy(w);change(bad)
            path=base/receipts[2]['path'];saved=read(path);path.write_bytes(encoded(bad))
            bad_config=copy.deepcopy(config);bad_config['native_qualification'][2].update(pin(encoded(bad)))
            refused(lambda:validate_config(bad_config,base));path.write_bytes(saved)
        store={};bodies={'mock/native':fake,**{d['key']:b'x' for d in inputs}};gets=[]
        def get(**kw):
            key=kw['Key'];gets.append(key);body=store[key] if key in store else bodies[key]
            return dict(Body=io.BytesIO(body),ContentLength=len(body))
        def put(**kw):
            require(kw['IfNoneMatch']=='*' and kw['Key'] not in store,'immutable marker-last publication');store[kw['Key']]=kw['Body']
        s3=SimpleNamespace(get_object=get,put_object=put)
        files=dict(zip(CGROUP_FILES,(str(CAPS['memory_bytes']),'4096','0','0',
            'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n','high 0\nmax 0\nfail 0\n',
            '100000 100000','usage_usec 1\nuser_usec 1\nsystem_usec 0\n',str(FIXED['tasks_max']),'0','max 0\n','','populated 0\nfrozen 0\n')))
        real_read=read
        def no_input_read(path,*args):
            require(input_root not in Path(path).parents,'Python must not decode retained inputs')
            return real_read(path,*args)
        context.enter_context(patch.object(module,'read',side_effect=no_input_read))
        def fixture(name,fault='',deadline=False,cleanup_failure=False):
            root=base/name;root.mkdir()
            for n,b in (('config.json',encoded(config)),('source-qualification.json',encoded(proof)),('cpu.txt',b'mock'),('run-closed.log',b'')):
                write(root/n,b)
            write(root/'runtime-abi.json',encoded(dict(machine='x86_64',python=[3,12],os=dict(ID='ubuntu',VERSION_ID='24.04'),
                libc=['glibc','2.39'],sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True))))
            for r in receipts:write(root/'qualification'/Path(r['path']).name,read(base/r['path']))
            write(root/'bootstrap/scratch.json',encoded(dict(closed=True,cap_exceeded=False,interval_seconds=1,
                sample_count=2,peak_bytes=65536,reserve=fixed['scratch'])))
            for d in inputs:
                if Path(d['destination']).exists():Path(d['destination']).unlink()
            start=len(gets);stage(s3,config,root)
            require(gets[start:] == ['mock/native',*(d['key'] for d in inputs)],'exact eleven opaque GETs and binary only')
            refused(lambda:stage(s3,config,root))
            parent=base/(name+'-cgroups')/(SUPERVISOR_UNIT+'.service');parent.mkdir(parents=True);group=parent/'native';owned=[]
            def delegate(record):
                record.update(unit=SUPERVISOR_UNIT+'.service',parent=str(parent),observer=str(parent/'supervisor'),observer_pid=os.getpid(),
                    available=sorted(CONTROLLERS),enabled=sorted(CONTROLLERS),parent_process_ids=[],observer_process_ids=[os.getpid()],parent_type='domain')
                return group
            def create(g):(g/'cgroup.procs').write_text('')
            def snapshot(g):return dict(path=str(g),observer_pid=os.getpid(),files=copy.deepcopy(files))
            def drain(g):
                if owned and owned[0].poll() is None:os.killpg(owned[0].pid,signal.SIGKILL)
                (g/'cgroup.procs').unlink()
                if cleanup_failure:raise OSError('synthetic cleanup failure')
            real_popen=subprocess.Popen
            def spawn(command,**kwargs):
                p=real_popen(command,**kwargs);owned.append(p);(group/'cgroup.procs').write_text('');return p
            with ExitStack() as execution:
                for method,fn in (('delegated_group',delegate),('create_group',create),('cgroup_snapshot',snapshot),('drain_group',drain)):
                    execution.enter_context(patch.object(module,method,side_effect=fn))
                execution.enter_context(patch.object(subprocess,'Popen',side_effect=spawn))
                execution.enter_context(patch.object(os,'sched_setaffinity',return_value=None))
                execution.enter_context(patch.dict(os.environ,PQ_GLUE_FAKE=fault))
                execution.enter_context(patch.dict(CAPS,deadline_seconds=.2 if deadline else 600))
                supervise(config,root,run_id=PREFIX+'a0001/i-original')
            return root
        root=fixture('success');result=validate_result(root,config)
        require(result['status']=='SURVIVED_SOURCE_NEIGHBORHOODS' and read(root/'native.log')==b'', 'silent real original exit0/report binding')
        files['cpu.max']='200000 100000';denied=fixture('pre-exec-cpu');files['cpu.max']='100000 100000'
        require(decode(read(denied/'native-exit.json'))['process_started'] is False and not (denied/'native.log').exists(),
            'kernel CPU mismatch refuses before native exec')
        refused(lambda:validate_result(denied,config))
        rejected=fixture('reject','REJECT');require(validate_result(rejected,config)['status']=='REJECT','completed native exit0 REJECT is valid')
        for name,fault,deadline,cleanup_failure in (('exit2','exit2',False,False),('partial','partial',False,False),
            ('deadline','deadline',True,False),('cleanup','',False,True)):
            denied=fixture(name,fault,deadline,cleanup_failure);refused(lambda:validate_result(denied,config))
            if name=='exit2':require(decode(read(denied/'screen/report.json'))['complete'] is True
                and decode(read(denied/'native-exit.json'))['process_exit_code']==2,'PASS body cannot override exit2')
            if name=='partial':partial=denied
        for name,change in (('native-exit.json',lambda d:d.update(process_exit_code=2)),
            ('native-exit.json',lambda d:d.update(report_sha256='0'*64)),('resources.json',lambda d:d.update(deadline_exceeded=True)),
            ('resources.json',lambda d:d['after']['files'].update({'memory.peak':str(2*1024**3)})),
            ('resources.json',lambda d:d['after']['files'].update({'memory.swap.peak':'1'})),
            ('cleanup.json',lambda d:d.update(drain_complete=False)),('scratch.json',lambda d:d.update(cap_exceeded=True)),
            ('screen/report.json',lambda d:d.update(standalone_authority=True))):
            p=root/name;saved=read(p);v=decode(saved);change(v);p.write_bytes(encoded(v));refused(lambda:validate_result(root,config));p.write_bytes(saved)
        for name in SQ4_OUTPUTS:
            p=root/name;saved=read(p);p.write_bytes(saved+b'!');refused(lambda:validate_result(root,config));p.write_bytes(saved)
        for name in ('screen/report.json',BINARY_NAME,'resources.json','cleanup.json','qualification/source-qualification.json'):
            p=root/name;saved=read(p);p.unlink();refused(lambda:validate_result(root,config));write(p,saved)
        def terminal(root,prefix,complete=True):
            return dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,instance_id='i-original',prefix=prefix,
                config_sha256=sha(encoded(config)),artifact_roster_sha256=ROSTER_SHA,status='complete' if complete else 'failed',
                phase='complete' if complete else 'execution',exit_code=0 if complete else 96,
                original_exit_code=decode(read(root/'native-exit.json'))['process_exit_code'],
                disposition=result['status'] if complete else 'INVALID',result=result,qualification=proof)
        def controller(out,prefix):
            write(out/'aws-reservation.json',encoded(dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,
                config_sha256=sha(encoded(config)),qualification=proof)))
            write(out/'aws-launch.json',encoded(dict(instance_id='i-original',nodes={'0':{'instance_id':'i-original'}},prefix=prefix,
                source_commit='a'*40,source_archive_sha256='b'*64)))
            write(out/'aws-closeout.json',encoded(dict(state='terminated',nodes={'0':{'instance_id':'i-original'}})))
        for index,source,complete in ((1,root,True),(2,partial,False)):
            prefix=PREFIX+f'a{index:04}';publish(s3,source,prefix,terminal(source,prefix,complete))
            out=base/('collected-'+str(index));controller(out,prefix)
            collect(s3,prefix,out,'i-original','a'*40,'b'*64)
            require(replay(out)['status'] == ('SURVIVED_SOURCE_NEIGHBORHOODS' if complete else 'INVALID'), 'complete/partial collection replay')
            require(read(out/'screen/report.json')==read(source/'screen/report.json'),'partial raw body preserved')
            p=out/'aws-closeout.json';saved=read(p)
            for close in (dict(state='running',nodes={'0':{'instance_id':'i-original'}}),dict(state='terminated',nodes={'0':{'instance_id':'i-other'}})):
                p.write_bytes(encoded(close));before=len(gets)
                refused(lambda:collect(s3,prefix,out,'i-original','a'*40,'b'*64));require(len(gets)==before,'wait/SAME-ID refusal before GET')
            p.write_bytes(saved)
        shared=lifecycle()
        import boto3
        campaign=Mock(ROOT=base,NAME='lifecycle',SCHEMA=SCHEMA,WALL=WALL,ARTIFACTS=ARTIFACTS,PREFIX=PREFIX,
            TOKEN_PREFIX=TOKEN_PREFIX,TAG=TAG,SUBNET=SUBNET,INSTANCE_TYPE=INSTANCE_TYPE,IMAGE_ID=IMAGE_ID,
            ROOT_DEVICE_NAME=ROOT_DEVICE_NAME,SPOT_MAX_USD_PER_HOUR=.60,COMPUTE_CAP=.25)
        campaign.preflight.return_value=proof;campaign.user_data.return_value=bootstrap
        for mode in ('success','poll','launch-fsync','launch-upload','extra-ack','wait'):
            campaign.ROOT=base/('lifecycle-'+mode);campaign.poll.side_effect=OSError('synthetic poll') if mode=='poll' else None
            ec2=Mock();session=Mock();session.client.side_effect=lambda name:ec2 if name=='ec2' else Mock()
            ec2.describe_instances.return_value={'Reservations':[]};ec2.describe_subnets.return_value={'Subnets':[{'AvailabilityZone':'mock'}]}
            ec2.describe_spot_price_history.return_value={'SpotPriceHistory':[{'SpotPrice':'.01','Timestamp':datetime.now(timezone.utc)}]}
            ack=['i-original','i-extra'] if mode=='extra-ack' else ['i-original']
            ec2.run_instances.return_value={'Instances':[{'InstanceId':i} for i in ack]};chronology=[]
            ec2.terminate_instances.side_effect=lambda **kw:chronology.append(('terminate',kw['InstanceIds']))
            def waited(**kw):
                chronology.append(('wait',kw['InstanceIds']))
                if mode=='wait':raise ValueError('synthetic wait failure')
            ec2.get_waiter.return_value.wait.side_effect=waited
            def collected(*args):
                require(chronology==[('terminate',ack),('wait',ack)] and mode!='wait','collection only after original ACK wait')
                return dict(status='complete',phase='complete',exit_code=0,artifacts={n:pin(b'') for n in ARTIFACTS})
            campaign.collect.side_effect=collected;campaign.collect.reset_mock()
            real_fsync=os.fsync
            def persisted(fd):
                if mode=='launch-fsync':raise OSError('synthetic ACK persistence failure')
                real_fsync(fd)
            def upload(key,body):
                if mode=='launch-upload' and key.endswith('/launch.json'):raise OSError('synthetic launch upload failure')
            with patch.object(boto3,'Session',return_value=session),patch.object(shared.subprocess,'check_output',side_effect=['','a'*40]), \
                patch.object(shared,'source_archive',return_value=b'synthetic archive'),patch.object(shared.peer,'missing',return_value=True), \
                patch.object(shared.peer,'put_if_absent',side_effect=upload),patch.object(shared.os,'fsync',side_effect=persisted):
                if mode in ('success','extra-ack'):shared.main('a0001',campaign=campaign)
                else:refused(lambda:shared.main('a0001',campaign=campaign))
            require(ec2.run_instances.call_count==1 and chronology==[('terminate',ack),('wait',ack)],'every ACK original ID terminate/wait, no replacement')
            if mode=='wait':campaign.collect.assert_not_called()
    print(f'PASS PQ residual source glue: full406/subset, all19/eleven+doc synthetic gates; eleven opaque transfers; real tiny silent exit0/REJECT/exit2/partial/deadline/cleanup; {failures} refusals; full pins/collection/replay/every-ACK wait. No Rust/native/corpus/GT/network/AWS.')


def co_selection_self_check():
    """Small metadata/mock check only. No native, corpus, GT or cloud calls."""
    import copy
    import io
    import tempfile
    from contextlib import ExitStack
    from types import SimpleNamespace
    from unittest.mock import Mock, patch
    module = sys.modules[__name__]; repo = Path(__file__).resolve().parents[1]
    failures = 0
    def refused(fn):
        nonlocal failures
        try: fn()
        except (ValueError,OSError,AssertionError,KeyError): failures += 1
        else: raise AssertionError('accepted invalid co-selection closure')
    native = dict(schema='borsuk-co-selection-config-v1',panels=[],caps=dict(CO_CAPS,caller_pinned_bytes=0,deadline_seconds=600),
        prior_reads=dict(operations=0,bytes=0))
    metadata = []
    for i,dataset in enumerate(('relaion','cohere')):
        def original(j):
            p,n,h = CO_ORIGINAL_PINS[j]; return dict(path=p,bytes=n,sha256=h)
        panel = dict(dataset=dataset,root=original(2*i),graph=original(2*i+1),
            identity=dict(generation=1,rows=100000,dimensions=768,source=[0]*32,layout=[0]*32,pq=[0]*32))
        for k,n in (('canonical',308000000),('source_order',800000),('fine_order',800000),('pq',7186456)):
            panel[k] = dict(path=str(SQ4_INPUT_ROOT/'mock'/dataset/k),bytes=n,sha256='0'*64)
        native['panels'].append(panel)
        metadata.append({k:dict(path=str(SQ4_INPUT_ROOT/'mock'/dataset/k),bytes=n,sha256='0'*64)
            for k,n in (('primary_root',100),('groups',200000))})
    native.update(original_seal=original(4),prefix=original(5))
    descriptors = [a for i,p in enumerate(native['panels']) for a in (
        *(p[k] for k in ('root','canonical','source_order','fine_order','pq','graph')),
        *(metadata[i][k] for k in ('primary_root','groups')))]+[native['original_seal'],native['prefix']]
    inputs = [dict(destination=d['path'],key='mock/source-'+str(i),bytes=d['bytes'],sha256=d['sha256']) for i,d in enumerate(descriptors)]
    fixed = dict(FIXED,machine_limit_seconds=1200,compute_cap_usd=.20,native_caps=native['caps'],scratch=dict(
        input_bytes=sum(d['bytes'] for d in inputs),native_output_bytes=CO_CAPS['output_bytes'],binary_bytes=CO_BINARY_PIN['bytes'],
        source_archive_bytes=8*1024**2,bootstrap_bytes=256*1024**2,auxiliary_bytes=16*1024**2,cap_bytes=2*1024**3))
    paths = sorted([str(CONFIG),*CODE,*(str(p) for p in CO_EVIDENCE)])
    config = dict(schema=SCHEMA,authority_pending=False,fixed=fixed,native_config=native,native_config_sha256=sha(encoded(native)),
        source_metadata=metadata,binary=dict(CO_BINARY_PIN,key='mock/qualified-binary'),inputs=inputs,
        native_source=dict(commit=CO_COMMIT,full_source_identity_sha256=CO_SOURCE_ID,
            source_sha256={n:CO_MANIFEST['source_sha256']['crates/borsuk/src/'+n] for n in CO_SOURCE_NAMES},
            qualification_protocol_sha256=sha(encoded(CO_PROTOCOL))),native_qualification=list(CO_QUALIFICATION),
        code_sha256={n:file_pin(repo/n)['sha256'] for n in CODE},source_archive_paths=paths,
        source_archive_paths_sha256=sha(json.dumps(paths,separators=(',',':')).encode()))
    source_path = 'crates/borsuk/src/co_selection_layout.rs'
    source = read(repo/source_path)
    require(sha(source) == CO_MANIFEST['source_sha256'][source_path], 'qualified native source-path dependency check')
    generation = source.index(b'    fn generate_counts(')
    require(source.index(b'read_pinned(&sources.primary_root',generation) < source.index(b'authenticate(&panel.pq',generation)
        and all(any(d['destination'] == support['primary_root']['path']
            and {k:d[k] for k in ('bytes','sha256')} == {k:support['primary_root'][k] for k in ('bytes','sha256')}
            for d in inputs) for support in metadata), 'native source generation requires both primary-root bodies before PQ auth')
    validate_config(config,repo); co_selection_qualification(config,repo); sdk_guard()
    require(canary_imports(repo,config) == config['code_sha256'], 'actual import/SDK closure')
    for change in (lambda d:d.update(authority_pending=True),lambda d:d['binary'].update(sha256='0'*64),
        lambda d:d['native_config'].update(truth={}),lambda d:d['native_config']['panels'][0].update(requests={}),
        lambda d:d['native_config']['caps'].update(operations=1),lambda d:d['native_config']['caps'].update(memory_bytes=1024**3),
        lambda d:d['native_config']['prior_reads'].update(operations=33),lambda d:d['inputs'].pop(),
        lambda d:d.update(inputs=[a for a in d['inputs'] if a['destination'] not in {p['primary_root']['path'] for p in d['source_metadata']}]),
        lambda d:d['inputs'].append(dict(destination=d['source_metadata'][0]['primary_root']['path'],
            key='mock/extra-primary-root',**{k:d['source_metadata'][0]['primary_root'][k] for k in ('bytes','sha256')})),
        lambda d:d['source_metadata'][0]['primary_root'].update(sha256='invalid'),
        lambda d:d['source_metadata'][0]['groups'].update(sha256='1'*64),
        lambda d:d['native_qualification'].pop(),lambda d:d['native_source'].update(full_source_identity_sha256='0'*64),
        lambda d:d['fixed'].update(compute_cap_usd=.01),lambda d:d['fixed']['scratch'].update(cap_bytes=1),
        lambda d:d['code_sha256'].update({CODE[0]:'0'*64})):
        bad = copy.deepcopy(config); change(bad); bad['native_config_sha256']=sha(encoded(bad['native_config']))
        refused(lambda:validate_config(bad,repo))
    validate_config(config,repo)
    with tempfile.TemporaryDirectory(prefix='co-selection-glue-') as tmp, ExitStack() as context:
        base=Path(tmp); archived=base/'archive'
        for n in (*CODE,*(str(p) for p in CO_EVIDENCE)): write(archived/n,read(repo/n))
        write(archived/CONFIG,encoded(config)); proof=preflight(archived)
        bootstrap=user_data('a'*40,'b'*64,'mock/archive',PREFIX+'a0001',proof)
        require('--co-selection-layout --remote' in bootstrap and 'Delegate=cpu memory pids' in bootstrap
            and 'on-active=1200s' in bootstrap and '--retries 0' in bootstrap and 'source archive roster' in bootstrap
            and 'rustc' not in bootstrap and 'cargo ' not in bootstrap, 'existing frozen bootstrap/delegation/SDK/no compiler')
        (archived/CONFIG).unlink(); refused(lambda:preflight(archived)); write(archived/CONFIG,encoded(config))
        # The real qualification bytes are replayed, including strict counts,
        # while all data transfers and native process behavior are synthetic.
        input_root=base/'inputs'; context.enter_context(patch.object(module,'SQ4_INPUT_ROOT',input_root))
        tiny=[dict(destination=str(input_root/str(i)),key='mock/'+str(i),**pin(b'x')) for i in range(18)]
        staged=base/'stage'; staged.mkdir(); write(staged/'bootstrap/scratch.json',encoded(dict(
            closed=True,cap_exceeded=False,interval_seconds=1,sample_count=2,peak_bytes=4096,reserve=fixed['scratch'])))
        transferred=[]
        s3=SimpleNamespace(get_object=lambda **kw:(transferred.append(kw['Key']) or dict(Body=io.BytesIO(b'x'),ContentLength=1)))
        opaque=dict(config,inputs=tiny,binary=dict(key='mock/binary',**pin(b'x')))
        real_read=read
        def no_input_read(path,*args):
            require(input_root not in Path(path).parents,'Python must never decode retained inputs')
            return real_read(path,*args)
        context.enter_context(patch.object(module,'read',side_effect=no_input_read))
        with patch.object(module,'validate_config'),patch.object(module,'INPUT_PINS',tuple((d['destination'],d['bytes'],d['sha256']) for d in tiny)):
            staged_receipt=stage(s3,opaque,staged); refused(lambda:stage(s3,opaque,staged))
        require(transferred == ['mock/binary',*(d['key'] for d in tiny)] and staged_receipt['exact_eighteen_inputs'] is True,
            'binary/eighteen opaque transfers once; both primary-root bodies required by native source generation')
        for d in tiny: Path(d['destination']).unlink()
        real_pin=file_pin
        def synthetic_pin(path):
            if str(path).endswith('/'+BINARY_NAME) and real_pin(path) == real_body_pin(b'synthetic executable'):
                return dict(CO_BINARY_PIN)
            if str(path).endswith('/screen/report.prefix.jsonl') and real_pin(path) == real_body_pin(b'opaque frozen prefix'):
                return {k:native['prefix'][k] for k in ('bytes','sha256')}
            return real_pin(path)
        real_body_pin=pin
        context.enter_context(patch.object(module,'pin',side_effect=lambda b:
            dict(CO_BINARY_PIN) if b==b'synthetic executable' else
            {k:native['prefix'][k] for k in ('bytes','sha256')} if b==b'opaque frozen prefix' else real_body_pin(b)))
        context.enter_context(patch.object(module,'file_pin',side_effect=synthetic_pin))
        files=dict(zip(CGROUP_FILES,(str(CO_CAPS['memory_bytes']),'4096','0','0',
            'low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n','high 0\nmax 0\nfail 0\n',
            '100000 100000','usage_usec 1\nuser_usec 1\nsystem_usec 0\n',str(FIXED['tasks_max']),'0','max 0\n','','populated 0\nfrozen 0\n')))
        def fixture(name,exit_code=0,reject=False,partial=False,deadline=False,cleanup_failure=False):
            root=base/name; root.mkdir()
            for n,b in (('config.json',encoded(config)),('source-qualification.json',encoded(proof)),('imports.json',encoded(config['code_sha256'])),
                        ('native-config.json',encoded(native)),(BINARY_NAME,b'synthetic executable'),('cpu.txt',b'mock'),('run-closed.log',b'')):
                write(root/n,b)
            write(root/'runtime-abi.json',encoded(dict(machine='x86_64',python=[3,12],os=dict(ID='ubuntu',VERSION_ID='24.04'),
                libc=['glibc','2.39'],sdk=dict(boto3='1.40.72',botocore='1.40.72',put_object_if_none_match=True))))
            for d in CO_QUALIFICATION: write(root/'qualification'/Path(d['path']).name,read(repo/d['path']))
            admission=dict(config_sha256=config['native_config_sha256'],reserve=fixed['scratch'],charged_bytes=sum(
                n for k,n in fixed['scratch'].items() if k!='cap_bytes'),free_bytes_before=4*1024**3,projected_peak_bytes=1024**3,
                source_archive_observed_bytes=4096,bootstrap_observed_bytes=4096,
                bootstrap=dict(closed=True,cap_exceeded=False,reserve=fixed['scratch'],sample_count=2))
            write(root/'stage-receipt.json',encoded(dict(binary=CO_BINARY_PIN,native_config=pin(encoded(native)),
                inputs={d['destination']:{k:d[k] for k in ('bytes','sha256')} for d in inputs},exact_eighteen_inputs=True,
                compiler_used=False,scratch=admission)))
            parent=base/(name+'-groups')/(SUPERVISOR_UNIT+'.service'); parent.mkdir(parents=True); group=parent/'native'
            def delegate(record):
                record.update(unit=SUPERVISOR_UNIT+'.service',parent=str(parent),observer=str(parent/'supervisor'),observer_pid=os.getpid(),
                    available=sorted(CONTROLLERS),enabled=sorted(CONTROLLERS),parent_process_ids=[],observer_process_ids=[os.getpid()],parent_type='domain')
                return group
            def create(g): (g/'cgroup.procs').write_text('')
            def drain(g):
                (g/'cgroup.procs').unlink()
                if cleanup_failure: raise OSError('synthetic drain failure')
            def spawn(command,**kwargs):
                require(command[1:] == [SQ4_CLI,str(root/'native-config.json'),config['native_config_sha256'],str(root/'screen/report.json')],
                    'strict native CLI CONFIG SHA NEW_OUTPUT')
                ceiling=dict(construction=1000,replay=1000); output=dict(total=4*1024**2)
                maps=[]; selections=[]
                for p in native['panels']:
                    stem='screen/report.'+p['dataset']; header=b'BORSCS01'+bytes.fromhex(''.join(p[k]['sha256']
                        for k in ('root','canonical','source_order','fine_order','pq','graph')))+(4096).to_bytes(4,'little')+(512).to_bytes(4,'little')
                    write(root/(stem+'.selections.bin'),header+bytes(4608*52))
                    selected=dict(path=str(root/(stem+'.selections.bin')),**real_pin(root/(stem+'.selections.bin'))); selections.append(selected)
                    mapping=dict(schema='borsuk-co-selection-map-v1',config_sha256=config['native_config_sha256'],
                        diagnostic_source_sha256=config['native_source']['source_sha256'],source=p,source_selections=selected,caps=native['caps'],
                        work_preflight=ceiling,output_preflight=output,held_anchor_count=512,held_used_for_fitting=False,group_rows=16,
                        blocks_per_object=128,replication_factor=1,short_tail_last=True,query_blind=True,within_group_order='unchanged',
                        original_sources=metadata[len(maps)],objects=[dict(payload_materialized=False,payload_sha256=None,payload_etag=None)])
                    write(root/(stem+'.map.json'),encoded(mapping)); maps.append(dict(path=str(root/(stem+'.map.json')),**real_pin(root/(stem+'.map.json'))))
                held=[]; plans=[]
                for i,p in enumerate(native['panels']):
                    for j in range(512): held.append(encoded(dict(dataset=p['dataset'],held_ordinal=j,map=maps[i],source_selections=selections[i],
                        used_for_fitting=False,policy_tuning=False,fits=True)))
                    for j in range(64): plans.append(encoded(dict(phase='co_selection_plan',dataset=p['dataset'],ordinal=j,root=p['root'],
                        map=maps[i],source_selections=selections[i],truth_opened=False,request_vectors_opened=False,sq8_bodies_opened=False,
                        plan=dict(all_nominees_retained=True,root_sha256=p['root']['sha256'],source_sha256=p['canonical']['sha256'],
                            prior=native['prior_reads'],fits=not (reject and i==1 and j==63),total_operations=33 if reject and i==1 and j==63 else 1,
                            total_bytes=1))))
                write(root/'screen/report.held.jsonl',b''.join(held)); write(root/'screen/report.plans.jsonl',b''.join(plans[:-1] if partial else plans))
                write(root/'screen/report.prefix.jsonl',b'opaque frozen prefix')
                details=dict(maps=maps,source_selections=selections,nomination_prefix=dict(path=str(root/'screen/report.prefix.jsonl'),
                    **{k:native['prefix'][k] for k in ('bytes','sha256')}),plans=dict(path=str(root/'screen/report.plans.jsonl'),**real_pin(root/'screen/report.plans.jsonl')),
                    held_diagnostics=dict(path=str(root/'screen/report.held.jsonl'),**real_pin(root/'screen/report.held.jsonl')),per_panel_fits=[64,63 if reject else 64],
                    caps=native['caps'],prior_reads=native['prior_reads'],original_seal=native['original_seal'],delta_bytes=0,additional_attempts=0,
                    construction_operations=1,replay_operations=1,counted_operations=2,source_auth_bytes=1,modeled_peak_owned_bytes=1,
                    retained_capacity_bytes=0,wall_ms=1,work_preflight=ceiling,output_preflight=output)
                write(root/'screen/report.json',encoded(dict(schema='borsuk-co-selection-diagnostic-v1',config_sha256=config['native_config_sha256'],
                    diagnostic_source_sha256=config['native_source']['source_sha256'],complete=True,queries=128,status='REJECT' if reject else 'SURVIVED_NECESSARY_LOCALITY',
                    standalone_authority=False,quality_or_performance_claim=False,truth_opened=False,request_vectors_opened=False,sq8_bodies_opened=False,
                    source_only_anchor_vectors=True,requires_matching_supervisor_exit_receipt=True,details=details)))
                process=Mock(pid=12345); process.poll.return_value=None if deadline else exit_code; process.wait.return_value=exit_code
                return process
            with ExitStack() as execution:
                for method,fn in (('delegated_group',delegate),('create_group',create),('cgroup_snapshot',lambda g:dict(path=str(g),observer_pid=os.getpid(),files=copy.deepcopy(files))),('drain_group',drain)):
                    execution.enter_context(patch.object(module,method,side_effect=fn))
                execution.enter_context(patch.object(subprocess,'Popen',side_effect=spawn))
                execution.enter_context(patch.dict(CAPS,deadline_seconds=.01 if deadline else 600))
                supervise(config,root,run_id=PREFIX+'a0001/i-original')
            return root
        root=fixture('success'); result=validate_result(root,config)
        require(result['status']=='SURVIVED_NECESSARY_LOCALITY' and read(root/'native.log')==b'', 'silent original exit0')
        rejected=fixture('reject',reject=True); require(validate_result(rejected,config)['status']=='REJECT','all128 exit0 REJECT admitted')
        partial=None
        for name,kwargs in (('exit2',dict(exit_code=2)),('partial',dict(partial=True)),('deadline',dict(deadline=True)),('cleanup',dict(cleanup_failure=True))):
            denied=fixture(name,**kwargs); refused(lambda:validate_result(denied,config))
            if name=='partial': partial=denied
        files['cpu.max']='200000 100000'; denied=fixture('pre-exec-cpu'); files['cpu.max']='100000 100000'
        require(decode(read(denied/'native-exit.json'))['process_started'] is False, 'kernel CPU mismatch before exec')
        refused(lambda:validate_result(denied,config))
        for n in SQ4_OUTPUTS:
            p=root/n; saved=read(p); p.write_bytes(saved+b'!'); refused(lambda:validate_result(root,config)); p.write_bytes(saved)
        for n,mutate in (('native-exit.json',lambda d:d.update(process_exit_code=2)),('resources.json',lambda d:d.update(deadline_exceeded=True)),
            ('resources.json',lambda d:d['after']['files'].update({'memory.swap.peak':'1'})),('cleanup.json',lambda d:d.update(drain_complete=False)),
            ('scratch.json',lambda d:d.update(cap_exceeded=True)),('imports.json',lambda d:d.clear())):
            p=root/n; saved=read(p); d=decode(saved); mutate(d); p.write_bytes(encoded(d)); refused(lambda:validate_result(root,config)); p.write_bytes(saved)
        store={}; gets=[]
        def get(**kw): gets.append(kw['Key']); b=store[kw['Key']]; return dict(Body=io.BytesIO(b),ContentLength=len(b))
        def put(**kw): require(kw['IfNoneMatch']=='*' and kw['Key'] not in store,'immutable marker-last publication'); store[kw['Key']]=kw['Body']
        s3=SimpleNamespace(get_object=get,put_object=put)
        for index,source,complete in ((1,root,True),(2,partial,False)):
            prefix=PREFIX+f'a{index:04}'; exit_receipt=decode(read(source/'native-exit.json'))
            terminal=dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,instance_id='i-original',prefix=prefix,
                config_sha256=sha(encoded(config)),artifact_roster_sha256=ROSTER_SHA,status='complete' if complete else 'failed',
                phase='complete' if complete else 'execution',exit_code=0 if complete else 96,original_exit_code=exit_receipt['process_exit_code'],
                disposition=result['status'] if complete else 'INVALID',result=result,qualification=proof)
            if index==2:
                exit_receipt['run_id']=prefix+'/i-original'; (source/'native-exit.json').write_bytes(encoded(exit_receipt))
            publish(s3,source,prefix,terminal); out=base/('collected-'+str(index))
            write(out/'aws-reservation.json',encoded(dict(schema=SCHEMA,source_commit='a'*40,source_archive_sha256='b'*64,config_sha256=sha(encoded(config)),qualification=proof)))
            write(out/'aws-launch.json',encoded(dict(instance_id='i-original',nodes={'0':{'instance_id':'i-original'}},prefix=prefix,source_commit='a'*40,source_archive_sha256='b'*64)))
            write(out/'aws-closeout.json',encoded(dict(state='terminated',nodes={'0':{'instance_id':'i-original'}})))
            collect(s3,prefix,out,'i-original','a'*40,'b'*64)
            if complete:
                # A fresh --replay process has no root runtime admission yet.
                saved_cap=globals()['SQ4_SCRATCH_CAP']; globals()['SQ4_SCRATCH_CAP']=0
                try: require(replay(out)['status']=='SURVIVED_NECESSARY_LOCALITY','fresh replay loads collected root resource authority')
                finally: globals()['SQ4_SCRATCH_CAP']=saved_cap
            require(replay(out)['status']==('SURVIVED_NECESSARY_LOCALITY' if complete else 'INVALID'), 'original complete/partial collect/replay')
            for close in (dict(state='running',nodes={'0':{'instance_id':'i-original'}}),dict(state='terminated',nodes={'0':{'instance_id':'i-other'}})):
                (out/'aws-closeout.json').write_bytes(encoded(close)); before=len(gets)
                refused(lambda:collect(s3,prefix,out,'i-original','a'*40,'b'*64)); require(len(gets)==before,'SAME-ID termination before collection GET')
    print(f'PASS co-selection glue: real source407/all21 qualification; strict root/native fields; opaque18/both primary-root bodies; direct native source-path dependency check; import/SDK/bootstrap; silent exit0/complete REJECT; exit2/partial/deadline/cleanup INVALID; {failures} refusals; SAME-ID collection/replay. No native/corpus/GT/cloud.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('attempt', nargs='?')
    parser.add_argument('--self-check', action='store_true')
    parser.add_argument('--sq4', action='store_true', help='opt in to the frozen native SQ4 experiment')
    parser.add_argument('--histogram-sq4', action='store_true', help='opt in to the frozen native histogram SQ4 experiment')
    parser.add_argument('--corrected-four-bit', action='store_true', help='opt in to the separately qualified corrected direction codec')
    parser.add_argument('--pq-residual-source', action='store_true', help='opt in to the separately qualified truth-free native source probe')
    parser.add_argument('--co-selection-layout', action='store_true', help='opt in to the qualified source-only virtual layout diagnostic')
    parser.add_argument('--canary',action='store_true',help='corrected metadata/usage infrastructure admission only')
    parser.add_argument('--remote-canary',nargs=6)
    parser.add_argument('--replay-canary',type=Path)
    parser.add_argument('--replay', type=Path)
    parser.add_argument('--remote', nargs=6)
    args = parser.parse_args()
    require(sum((args.sq4,args.histogram_sq4,args.corrected_four_bit,args.pq_residual_source,args.co_selection_layout)) <= 1, 'one native experiment')
    if args.co_selection_layout:
        configure_co_selection_layout()
    if args.pq_residual_source:
        configure_pq_residual_source()
    if args.sq4 or args.histogram_sq4 or args.corrected_four_bit:
        configure_sq4(histogram=args.histogram_sq4, corrected=args.corrected_four_bit)
    require(not (args.canary or args.remote_canary or args.replay_canary) or args.corrected_four_bit, 'canary only in corrected mode')
    require(not args.canary or args.attempt is not None or args.self_check, 'canary launch or self-check')
    require(not args.canary or not (args.remote_canary or args.replay_canary), 'one canary entry')
    if args.canary or args.remote_canary or args.replay_canary:
        configure_canary()
    require(sum((args.attempt is not None,args.self_check,args.replay is not None,args.remote is not None,
        args.remote_canary is not None,args.replay_canary is not None)) == 1, 'one CLI mode')
    if args.self_check:
        if CO_SELECTION_LAYOUT:
            co_selection_self_check()
        elif PQ_RESIDUAL_SOURCE:
            pq_residual_source_self_check()
        elif CANARY:
            canary_self_check()
        elif SQ4:
            sq4_self_check(histogram=HISTOGRAM_SQ4, corrected=CORRECTED_FOUR_BIT)
        else:
            self_check(real_cgroup=os.environ.get('BORSUK_FINE_PACK_REAL_CGROUP')=='1')
        return 0
    if args.replay or args.replay_canary:
        print(json.dumps(replay(args.replay or args.replay_canary), sort_keys=True)); return 0
    if args.remote or args.remote_canary:
        repo, root, commit, digest, prefix, config_sha = args.remote or args.remote_canary
        return remote(Path(repo), Path(root), commit, digest, prefix, config_sha)
    require(re.fullmatch(r'a[0-9]{4}', args.attempt), 'attempt must be aNNNN')
    sdk_guard()
    os.environ['AWS_MAX_ATTEMPTS'] = '1'
    os.environ['AWS_RETRY_MODE'] = 'standard'
    with open('/tmp/borsuk-co-selection-layout-diagnostic.lock' if CO_SELECTION_LAYOUT else '/tmp/borsuk-pq-residual-source-diagnostic.lock' if PQ_RESIDUAL_SOURCE else '/tmp/borsuk-corrected-four-bit-diagnostic.lock' if CORRECTED_FOUR_BIT else '/tmp/borsuk-histogram-sq4-diagnostic.lock' if HISTOGRAM_SQ4 else '/tmp/borsuk-fixed-sq4-diagnostic.lock' if SQ4 else '/tmp/borsuk-fine-pack-diagnostic.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        lifecycle().main(args.attempt, campaign=sys.modules[__name__])
    return 0


if __name__ == '__main__':
    sys.exit(main())
