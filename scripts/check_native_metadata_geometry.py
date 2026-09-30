"""No cloud: check both fresh binary authorities and bounded geometry records."""
import copy
import json
from pathlib import Path
import tempfile
from unittest.mock import patch

from scripts import run_native_metadata_ranges_cold as worker
from scripts import check_native_metadata_ranges_stats as accounting
from scripts import verify_native_metadata_ranges_cold as reducer


def main():
    accounting.main()
    reducer.self_check()
    root = Path('docs/research/native-union-20260928')
    frozen = json.loads((root/'metadata-geometry-config.json').read_text())
    frozen['code_sha256'] = {name: worker.cold.sha(name) for name in worker.CODE}
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        config_path = directory/'config.json'
        binaries, proofs = {}, {}
        for arm in ('candidate', 'control'):
            binary = directory/(arm+'.bin')
            binary.write_bytes(arm.encode())
            binaries[arm] = binary
            compiled = dict(frozen['compiled_sha256'])
            compiled['crates/borsuk/src/object_native_generation.rs'] = frozen[arm+'_native']['stage_sha256']
            proofs[arm] = dict(qualified=True, green_status=0, release_status=0,
                binary_sha256=worker.cold.sha(binary), binary_bytes=binary.stat().st_size,
                compiled_http_sha256=compiled['crates/borsuk/examples/two_bit_http.rs'],
                compiled_native_sha256=compiled,
                source_identity_sha256=frozen[arm+'_native']['source_identity_sha256'],
                source_file_count=395, current_full_suite_pass_claim=False)
        for mutation in (None, 'identity', 'bytes', 'compiled', 'full_suite', 'geometry'):
            config, shaped = copy.deepcopy(frozen), copy.deepcopy(proofs)
            if mutation == 'identity': shaped['control']['source_identity_sha256'] = '0'*64
            elif mutation == 'bytes': shaped['control']['binary_bytes'] += 1
            elif mutation == 'compiled': shaped['control']['compiled_native_sha256']['crates/borsuk/src/unit_centroid_graph.rs'] = '0'*64
            elif mutation == 'full_suite': shaped['candidate']['current_full_suite_pass_claim'] = True
            elif mutation == 'geometry': config['staging']['candidate']['parallel_gets'] = 16
            config_path.write_text(json.dumps(config))
            for arm, proof in shaped.items(): (directory/(arm+'.json')).write_text(json.dumps(proof))
            argv = ['worker', str(config_path), worker.cold.sha(config_path),
                str(binaries['candidate']), str(directory/'candidate.json'),
                str(binaries['control']), str(directory/'control.json'), str(directory/'screen')]
            with patch.object(worker.sys, 'argv', argv), \
                 patch.object(worker.os, 'sched_getaffinity', return_value={4, 5}), \
                 patch.object(worker, 'run') as run:
                try: worker.main()
                except AssertionError:
                    assert mutation is not None
                    run.assert_not_called()
                else:
                    assert mutation is None, mutation
                    run.assert_called_once()
    print('fresh geometry binary authority PASS (identity/size/compiled/full-suite/geometry guards)')


if __name__ == '__main__': main()
