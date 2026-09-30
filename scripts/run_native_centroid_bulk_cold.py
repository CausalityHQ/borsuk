"""Paired centroid bulk conversion intervention using the existing ABBA cold protocol."""
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

from scripts import run_native_metadata_ranges_cold as paired
from scripts import run_native_paged_cold_first_query as paged
from scripts.check_native_startup_build import source_hashes, source_identity

SCHEMA = 'borsuk-native-centroid-bulk-cold-v1'
STAGE = 'crates/borsuk/src/unit_centroid_pages.rs'
CODE = tuple(dict.fromkeys((*paired.CODE, *paged.CODE, 'scripts/run_native_centroid_bulk_cold.py')))


def validate_config(config, base=Path('.')):
    assert config['schema'] == SCHEMA
    assert (config['count'], config['k'], config['ann_queries']) == (64, 10, 256)
    assert config['blocks'] == [dict(arm=a, begin=b, end=e) for a,b,e in paired.BLOCKS]
    assert config['dataset_order'] == ['ReLAION','CoHere']
    assert config['source_caps'] == paged.SOURCE_CAPS
    assert config['staging'] == {arm:dict(range_bytes=4194304,parallel_gets=8) for arm in ('control','candidate')}
    assert set(config['code_sha256']) == set(CODE)
    for name, digest in config['code_sha256'].items(): assert paired.cold.sha(base/name) == digest
    reference = config['items_source']; body = (base/reference['path']).read_bytes()
    assert paired.cold.sha(base/reference['path']) == reference['sha256']
    assert config['items'] == json.loads(body)['items']
    manifest = config['native_manifest']; body = (base/manifest['path']).read_bytes()
    assert paired.cold.sha(base/manifest['path']) == manifest['sha256']
    manifest = json.loads(body)
    assert manifest['schema'] == 'borsuk-centroid-bulk-source-v1'
    identities = source_hashes(base)
    assert identities == manifest['source_sha256'] and len(identities) == manifest['source_file_count'] == 395
    assert source_identity(identities) == manifest['candidate_identity']
    assert manifest['candidate_stage_sha256'] == identities[STAGE]
    assert len(config['compiled_sha256']) == 21
    assert all(identities[name] == digest for name,digest in config['compiled_sha256'].items())
    control = dict(identities, **{STAGE: manifest['control_source']['sha256']})
    assert paired.cold.sha(base/manifest['control_source']['path']) == control[STAGE]
    assert source_identity(control) == manifest['control_identity']
    return manifest


def run(config, binaries, output):
    with paged.scoped_runner(sys.argv), patch.object(paired, 'validate_transfer',
            side_effect=lambda header, files, arm, geometry: paged.validate_startup(
                header['remote_open_stats'], files, header['remote_open_wall_ns'])):
        summary = paired.run(config, binaries, output)
    rows = [json.loads(line) for block in range(4)
            for line in (output/f'block{block}-records.jsonl').read_text().splitlines()]
    decode = {arm: {dataset: {label: paired.cold.quantile([
        row['native_header']['remote_open_stats']['decode_wall_ns']/1e6
        for row in rows if row['arm'] == arm and row['dataset'] == dataset], p)
        for label,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]}
        for dataset in config['dataset_order']} for arm in binaries}
    summary.update(decode_ms=decode, source_caps=paged.SOURCE_CAPS,
        intervention='bounded bulk FP16 centroid conversion; immutable indexes, graph and scorer unchanged',
        decode_gate_passed=all(decode['candidate'][d]['p50'] < decode['control'][d]['p50']
            for d in config['dataset_order']), matched_vendor_measured=False)
    (output/'summary.json').write_text(json.dumps(summary,indent=2,allow_nan=False)+'\n')
    return summary


def main():
    config_path, digest, candidate, candidate_proof, control, control_proof, output = sys.argv[1:]
    assert paired.cold.sha(config_path) == digest
    config = json.loads(Path(config_path).read_bytes()); manifest = validate_config(config)
    assert sorted(os.sched_getaffinity(0)) == [4,5]
    assert os.environ['TOKIO_WORKER_THREADS'] == '4' and os.environ['AWS_MAX_ATTEMPTS'] == '1'
    assert os.environ['BORSUK_NATIVE_MEMORY_BYTES'] == '1073741824'
    for arm,binary,path in [('candidate',candidate,candidate_proof),('control',control,control_proof)]:
        proof = json.loads(Path(path).read_bytes())
        assert proof['qualified'] is True and proof['green_status'] == proof['release_status'] == 0
        assert proof['arm'] == arm and proof['same_worker_toolchain'] is True
        assert proof['source_identity_sha256'] == manifest[arm+'_identity'] and proof['source_file_count'] == 395
        assert paired.cold.sha(binary) == proof['binary_sha256'] and Path(binary).stat().st_size == proof['binary_bytes']
        expected = dict(config['compiled_sha256'])
        if arm == 'control': expected[STAGE] = manifest['control_source']['sha256']
        assert proof['compiled_native_sha256'] == expected and len(expected) == 21
        assert proof['sha_backend']['arm_asm_selected'] is proof['sha_backend']['cpu_sha2_capable'] is True
        assert proof['sha_backend']['x86_asm_selected'] is False
    run(config,dict(candidate=candidate,control=control),Path(output))


def self_check(config=None, base=None):
    import copy
    if config is None:
        import tempfile
        from scripts import launch_native_centroid_bulk_spot as controller
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            return self_check(controller.config_fixture(base, Path(__file__).resolve().parent.parent), base)
    validate_config(config, base)
    for key in ('count', 'source_caps', 'compiled_sha256'):
        changed = copy.deepcopy(config)
        changed[key] = 63 if key == 'count' else {}
        try: validate_config(changed, base)
        except AssertionError: pass
        else: raise AssertionError('changed authority accepted: '+key)
    original = paired.cold.cold_call, paired.validate_transfer
    with patch.object(paired, 'run', side_effect=RuntimeError('synthetic cancellation')):
        try: run(config, {}, Path('unused'))
        except RuntimeError: pass
        else: raise AssertionError('cancellation accepted')
    assert (paired.cold.cold_call, paired.validate_transfer) == original
    print('PASS centroid-bulk config/source guards and paged scope restoration')


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']: self_check()
    else:
        assert len(sys.argv) == 8
        main()
