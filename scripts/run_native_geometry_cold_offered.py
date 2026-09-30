"""Strict frozen geometry authority around the unchanged six-slot offered runner."""
import hashlib
import json
import os
from pathlib import Path
import sys

from scripts import run_native_cold_offered as offered

cold = offered.cold
CODE = (*offered.CODE, 'scripts/run_native_geometry_cold_offered.py')
SCHEMA = 'borsuk-native-geometry-cold-offered-v1'
BINARY_SHA = '256dcf6c9d3f893d5c709564a099bf440b482e617800598ddd6538e1ec0cbb21'
SOURCE_IDENTITY = '4bbc6c332e78e5a068001913597cdc82d6c13bd147acd96e019149c190a39a1c'
PROOF_SHA = '2443fe5ab774a3eb908046617d723ca1c70797e5da4254314a8a04aeb87912cb'
STAGING = dict(candidate=dict(range_bytes=4194304, parallel_gets=8))


def validate_config(config, base=Path('.')):
    assert config['schema'] == SCHEMA
    assert (config['count'], config['k']) == (64, 10)
    assert config['offered_qps'] == offered.RATES and config['workers'] == 6 and config['base_port'] == 18080
    assert config['dataset_order'] == ['ReLAION', 'CoHere']
    assert config['client_cpu_affinity'] == [4, 5] and config['native_cpu_affinity'] == [0, 1, 2, 3]
    assert config['worker_limit_seconds'] == 2400 and config['machine_limit_seconds'] == 2700
    assert config['worker_memory_bytes'] == 7*1024**3 and config['native_memory_bytes'] == 1024**3
    assert config['namespace_connect_deadline_seconds'] == 45 and config['native_process_limit_seconds'] == 60
    assert config['query_payload_timeout_seconds'] == 5
    assert config['gates'] == dict(recall_at_10_minimum=.95, all_offers_success=True,
        source_scorer_ordered_id_physical_parity=True, cold_p90_ms_exclusive_maximum=444)
    assert config['namespace_cold_start_included'] is config['previous_observed_development_panel'] is True
    assert config['matched_vendor_measured'] is config['application_sq8_cache'] is config['total_cost_measured'] is False
    assert config['s3_service_cache'] == 'uncontrolled' and config['transport'] == 'loopback plain HTTP'
    assert config['cold_boundary'] == 'preencoded request; process launch through first HTTP response; refused TCP connects included'
    assert config['staging'] == STAGING
    assert (config['bucket'], config['region']) == ('borsuk-bench-453182569524-euc1', 'eu-central-1')
    reference_path = Path('docs/research/native-union-20260928/metadata-geometry-config.json')
    reference_sha = '410ce351d764bccccafcfe6e34697dcc961856288c67f8519c451fa4266143c3'
    assert config['items_source'] == dict(path=str(reference_path), sha256=reference_sha)
    assert cold.sha(Path(base)/reference_path) == reference_sha
    assert config['items'] == json.loads((Path(base)/reference_path).read_text())['items']
    assert config['binary'] == dict(sha256=BINARY_SHA, bytes=12470768)
    assert config['frozen_native_qualification'] == dict(
        path='docs/research/native-union-20260928/metadata-geometry/a0002/verification.json',
        sha256='cbc369cfe714653173525b6557af49c0462b458bf29a9c9ce5a4aacce318452c',
        source_commit='3315455568ea727f277911de96a2231289d6ae5b',
        source_archive_sha256='48bb877cdbe947401084ab56ec237eca1c4d9cff7bbb49a9c35d9380ae235959',
        terminal_sha256='37268e9f0125ef0a3f770271458a0378d27b018571cb4c701965833b9fa1e826')
    assert set(config['code_sha256']) == set(CODE)
    for name, digest in config['code_sha256'].items():
        assert cold.sha(Path(base)/name) == digest, name


def validate_runtime(config, binary, proof_path):
    assert sorted(os.sched_getaffinity(0)) == config['client_cpu_affinity'] == [4, 5]
    assert os.environ['TOKIO_WORKER_THREADS'] == '4'
    assert os.environ['BORSUK_NATIVE_MEMORY_BYTES'] == '1073741824'
    assert os.environ['AWS_MAX_ATTEMPTS'] == '1'
    assert cold.sha(proof_path) == PROOF_SHA
    assert Path(proof_path).stat().st_size == 2111
    proof = json.loads(Path(proof_path).read_text())
    assert proof['qualified'] is True and proof['green_status'] == proof['release_status'] == 0
    assert proof['current_full_suite_pass_claim'] is False
    assert proof['sha_backend']['arm_asm_selected'] is proof['sha_backend']['cpu_sha2_capable'] is True
    assert proof['sha_backend']['x86_asm_selected'] is False
    assert proof['binary_sha256'] == cold.sha(binary) == config['binary']['sha256'] == BINARY_SHA
    assert Path(binary).stat().st_size == proof['binary_bytes'] == config['binary']['bytes'] == 12470768
    assert len(proof['compiled_native_sha256']) == 9
    for name, digest in proof['compiled_native_sha256'].items():
        assert cold.sha(name) == digest, name
    assert proof['compiled_http_sha256'] == cold.sha('crates/borsuk/examples/two_bit_http.rs')
    repo = Path('.')
    paths = set(repo.rglob('*.rs')) | set(repo.rglob('Cargo.toml')) | set(repo.rglob('Cargo.lock'))
    identities = {str(p): cold.sha(p) for p in sorted(paths) if not {'.git', 'target'}.intersection(p.parts)}
    identity = hashlib.sha256(json.dumps(identities, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert len(identities) == proof['source_file_count'] == 395
    assert identity == proof['source_identity_sha256'] == SOURCE_IDENTITY


run = offered.run


def main():
    config_path, digest, binary, proof_path, output = sys.argv[1:]
    assert cold.sha(config_path) == digest
    config = json.loads(Path(config_path).read_text())
    validate_config(config)
    validate_runtime(config, binary, proof_path)
    summary = run(config, binary, Path(output))
    print(json.dumps(dict(closed=True, offered=summary['offered'], successful=summary['successful'],
        capacity_drops=summary['capacity_drops'], errors=summary['errors'])))


if __name__ == '__main__':
    assert len(sys.argv) == 6
    main()
