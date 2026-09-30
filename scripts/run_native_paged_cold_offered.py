"""Reuse the bounded offered scheduler with authenticated paged-source calls."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

from scripts import run_native_cold_offered as offered
from scripts import run_native_paged_cold_first_query as paged

CODE = tuple(dict.fromkeys((*offered.CODE, *paged.CODE, 'scripts/run_native_paged_cold_offered.py')))
SCHEMA = 'borsuk-native-paged-cold-offered-v1'
RATES = [8, .25, .5, 1, 2, 4]
_reduce = offered.reduce_cell


def reduce_cell(records, *args):
    result = _reduce(records, *args)
    successful = [r for r in records if r['outcome'] == 'success']
    for row in successful: paged.validate_response(row['response'])
    for suffix in ('submitted_gets', 'verified_bytes', 'failed_gets'):
        source = sum(r['response']['source_'+suffix] for r in successful)
        sq8 = sum(r['response'][suffix] for r in successful)
        result.update({'source_'+suffix: source, 'sq8_'+suffix: sq8, 'combined_'+suffix: source+sq8})
    result['source_head_requests'] = sum(r['metadata']['source_head_requests'] for r in successful)
    return result


@contextmanager
def scoped_runner():
    with paged.scoped_runner(sys.argv), patch.object(offered, 'RATES', RATES), \
            patch.object(offered, 'reduce_cell', reduce_cell), \
            patch.object(offered.ranges, 'validate_transfer', side_effect=lambda header, files, arm, geometry:
                paged.validate_startup(header['remote_open_stats'], files, header['remote_open_wall_ns'])):
        yield


def main():
    config_path, digest, binary, proof_path, output = sys.argv[1:]
    assert offered.cold.sha(config_path) == digest
    config = json.loads(Path(config_path).read_bytes())
    assert config['schema'] == SCHEMA and config['offered_qps'] == RATES
    assert (config['count'], config['k'], config['workers'], config['base_port']) == (64,10,6,18080)
    assert config['dataset_order'] == ['ReLAION','CoHere']
    assert sorted(os.sched_getaffinity(0)) == config['client_cpu_affinity'] == [4,5]
    assert config['native_cpu_affinity'] == [0,1,2,3]
    assert os.environ['TOKIO_WORKER_THREADS'] == '4'
    assert os.environ['AWS_MAX_ATTEMPTS'] == '1'
    assert os.environ['BORSUK_NATIVE_MEMORY_BYTES'] == '1073741824'
    assert set(config['code_sha256']) == set(CODE)
    for name, value in config['code_sha256'].items(): assert offered.cold.sha(name) == value
    reference = config['items_source']; body = Path(reference['path']).read_bytes()
    assert offered.cold.sha(reference['path']) == reference['sha256']
    assert config['items'] == json.loads(body)['items']
    assert offered.cold.sha(proof_path) == config['boundary_proof']['sha256']
    assert Path(proof_path).stat().st_size == config['boundary_proof']['bytes']
    proof = json.loads(Path(proof_path).read_bytes())
    assert len(proof['compiled_native_sha256']) == 20
    assert proof['sha_backend']['arm_asm_selected'] is proof['sha_backend']['cpu_sha2_capable'] is True
    assert proof['sha_backend']['x86_asm_selected'] is False
    paged.validate_runtime(config, binary, proof_path)
    with scoped_runner(): summary = offered.run(config, binary, Path(output))
    summary.update(source_caps=paged.SOURCE_CAPS, native_source_identity_sha256=config['native_source_identity_sha256'],
        matched_control_latency_measured=False, closed_resident_reference_quality_only=True)
    (Path(output)/'summary.json').write_text(json.dumps(summary, indent=2, allow_nan=False)+'\n')
    print(json.dumps(dict(closed=True, offered=summary['offered'], successful=summary['successful'],
        capacity_drops=summary['capacity_drops'], errors=summary['errors'])))


def self_check():
    old_rates, old_call, old_reduce = offered.RATES, offered.cold.cold_call, offered.reduce_cell
    try:
        with scoped_runner():
            assert offered.RATES == RATES and offered.cold.cold_call is paged.cold_call
            assert offered.reduce_cell is reduce_cell
            raise RuntimeError('synthetic cancellation')
    except RuntimeError: pass
    assert offered.RATES is old_rates and offered.cold.cold_call is old_call and offered.reduce_cell is old_reduce
    from scripts.run_native_paged_cold_first_query_selfcheck import gates_check
    gates_check()
    import gzip
    frozen = Path('docs/research/source-paging-20260930/cold/a0004/screen/relaion-records.jsonl.gz')
    if frozen.exists():
        rows = [json.loads(line) for line in gzip.decompress(frozen.read_bytes()).splitlines()]
        for row in rows:
            row.update(outcome='success', port=18080, scheduled_ns=row['started_ns'],
                dispatched_ns=row['started_ns'], terminal_ns=row['completed_ns'], transfer_accounting=row['metadata'])
        result = reduce_cell(rows, 8, rows[0]['started_ns'], rows[-1]['completed_ns'], False)
        assert result['successful'] == 64 and result['source_head_requests'] == 64
        assert result['combined_submitted_gets'] == result['source_submitted_gets']+result['sq8_submitted_gets']
        assert result['source_verified_bytes'] == 1528486400
    print('PASS offered adapter reuses scheduler, paged guards and restores globals after failure')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']: self_check()
    else:
        assert len(sys.argv) == 6
        main()
