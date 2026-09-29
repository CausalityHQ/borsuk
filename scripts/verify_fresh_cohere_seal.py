"""Verify closed CoHere query construction and independently sealed S3 bodies."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile

import numpy as np

from build_cohere_1m_source import BUCKET, SOURCE_SHA, digest
from scripts.run_native_source_frontier_1m import oracle, self_check


def main():
    work = Path(sys.argv[1])
    directory = work / 'fresh-identity'
    path = directory / 'seal.json'
    seal = json.loads(path.read_text())
    assert seal['status'] == 'complete' and seal['exit_code'] == 0
    resources = (work / 'identity-seal-resources.txt').read_text()
    assert 'Exit status: 0' in resources
    root = Path('docs/research/native-union-20260928')
    source = json.loads((root / 'cohere-source-1m/a0001/verification.json').read_text())
    assert source['valid_source_construction'] and source['state'] == 'local-complete'
    assert seal['root_sha256'] == source['root_sha256']
    assert seal['source_raw_sha256'] == source['source_raw_sha256'] == SOURCE_SHA
    assert seal['query_rows'] == [1005000, 1005999]
    assert seal['development_ordinals'] == [0, 63] and seal['prospective_ordinals'] == [64, 999]
    assert not seal['complete_prior_query_audit'] and not seal['ann_quality_measured']
    assert seal['controller_sha256'] == digest(Path('scripts/audit_fresh_cohere_vectors.py'))
    assert seal['preregister_sha256'] == digest(root / 'fresh-cohere-1m-preregister.md')
    assert seal['oracle_source_sha256'] == digest(Path('scripts/run_native_source_frontier_1m.py'))
    prefix = 'research/native-union/20260929/fresh-cohere-seal-a0001'
    assert set(seal['artifacts']) == {'queries.raw', 'requests.jsonl', 'truth.u32', 'identity.json'}
    with tempfile.TemporaryDirectory() as temp:
        def remote(key, local, expected):
            target = Path(temp) / local.name
            subprocess.run(['aws', '--profile', 'causality', 's3', 'cp',
                            's3://' + BUCKET + '/' + key, str(target), '--only-show-errors'], check=True)
            assert digest(target) == digest(local) == expected
            assert target.stat().st_size == local.stat().st_size
        remote(prefix + '/terminal.json', path, digest(path))
        for name, ident in seal['artifacts'].items():
            local = directory / name
            assert ident['key'] == prefix + '/sealed/' + name
            assert local.stat().st_size == ident['bytes']
            remote(ident['key'], local, ident['sha256'])
    identity = json.loads((directory / 'identity.json').read_text())
    assert identity['decision'] == 'GO scoped fixed-panel construction'
    assert identity['query_count'] == 1000 and identity['candidate_rows'] == seal['query_rows']
    assert identity['source_build_terminal_sha256'] == source['terminal_sha256']
    assert identity['query_raw_sha256'] == seal['artifacts']['queries.raw']['sha256']
    assert identity['source_candidate_sha256'] == digest(root / 'fresh-cohere-source-candidate.json')
    assert not any(identity['duplicate_counts'].values()) and not identity['complete_prior_query_audit']
    queries = np.fromfile(directory / 'queries.raw', dtype='<f4').reshape(1000, 768)
    truth = np.fromfile(directory / 'truth.u32', dtype='<u4').reshape(1000, 100)
    assert np.isfinite(queries).all() and (queries != 0).any(axis=1).all()
    assert all(len(set(map(int, row))) == 100 and max(row) < 1000000 for row in truth)
    with (directory / 'requests.jsonl').open() as stream:
        for ordinal, vector in enumerate(queries):
            row = json.loads(next(stream))
            assert row == dict(query_ordinal=ordinal, query=vector.tolist())
        assert stream.read() == ''
    self_check()
    # One exact development-row recomputation checks serialized GT independently of construction output.
    assert digest(work / 'source.raw') == SOURCE_SHA
    assert np.array_equal(oracle(work / 'source.raw', queries[:1], 1000000)[0], truth[0])
    report = dict(valid_construction=True, state='local-complete', qualification=False,
                  ann_quality_measured=False, complete_prior_query_audit=False,
                  terminal_sha256=digest(path), root_sha256=seal['root_sha256'],
                  source_raw_sha256=SOURCE_SHA, sealed_artifacts=seal['artifacts'],
                  independently_hashed_remote_bodies=5, requests_and_truth_geometry_verified=True,
                  exact_development_gt_row_recomputed=0, oracle_self_check=True,
                  oracle_wall_seconds=seal['oracle_wall_seconds'],
                  controller_sha256=seal['controller_sha256'],
                  verification_controller_sha256=digest(Path(__file__)))
    out = root / 'fresh-cohere-seal/a0001'
    out.mkdir(parents=True, exist_ok=True)
    for name in ['identity.json', 'seal.json']:
        (out / name).write_bytes((directory / name).read_bytes())
    (out / 'process-resources.txt').write_text(resources)
    (out / 'verification.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(dict(valid_construction=True, state='local-complete',
                          terminal_sha256=report['terminal_sha256'], ann_quality_measured=False)))


if __name__ == '__main__':
    assert len(sys.argv) == 2
    main()
