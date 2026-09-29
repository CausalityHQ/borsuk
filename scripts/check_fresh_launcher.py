"""Exercise launcher authority gates without AWS, archives or query bodies."""
import copy
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

ROOT = Path('docs/research/native-union-20260928')
spec = importlib.util.spec_from_file_location('launcher', ROOT / 'aws-fresh-rank16-dev64.py')
launcher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(launcher)


class Accepted(Exception):
    pass


def check():
    old_text, old_bytes = Path.read_text, Path.read_bytes
    base = json.loads((ROOT / 'fresh-rank16-dev64-config.json').read_text())
    base['code_sha256'] = {name: launcher.sha(Path(name).read_bytes())
                           for name in base['code_sha256']}
    source = json.loads((ROOT / 'cohere-source-1m/a0001/verification.json').read_text())

    def accepted(family, altered=None):
        config = copy.deepcopy(base)
        virtual = {}
        if family == 'cohere':
            proof = copy.deepcopy(source)
            if altered:
                proof[altered[0]] = altered[1]
            config.update(schema='borsuk-fresh-cohere-1m-dev64-v1',
                          root_sha256=source['root_sha256'],
                          source_raw_sha256=source['source_raw_sha256'],
                          generation_artifacts={**source['generation_artifacts'],
                                                'generation/canonical.bin': source['canonical_artifact']},
                          root_manifest=source['generation_artifacts']['generation/manifest.json'])
            virtual[ROOT / 'cohere-source-1m/a0001/verification.json'] = json.dumps(proof).encode()
            config['source_construction_verification_sha256'] = launcher.sha(next(iter(virtual.values())))
            seal = dict(valid_construction=True, state='local-complete',
                        sealed_artifacts=config['sealed'], root_sha256=source['root_sha256'])
            seal_path = ROOT / 'fresh-cohere-seal/a0001/verification.json'
            virtual[seal_path] = json.dumps(seal).encode()
            config['sealed_construction_verification_sha256'] = launcher.sha(virtual[seal_path])
        virtual[ROOT / f'fresh-{family}-dev64-config.json'] = json.dumps(config).encode()
        with patch.object(Path, 'read_text', lambda p, *a, **k: virtual[p].decode() if p in virtual else old_text(p, *a, **k)), \
             patch.object(Path, 'read_bytes', lambda p: virtual[p] if p in virtual else old_bytes(p)), \
             patch.object(launcher.subprocess, 'check_output', side_effect=['', 'f' * 40]), \
             patch.object(launcher.subprocess, 'run'), \
             patch.object(launcher.io, 'BytesIO', side_effect=Accepted), \
             patch.object(launcher.boto3, 'Session', side_effect=AssertionError('AWS execution')):
            try:
                launcher.main('a0001', 'subnet-0a12dbed0ca6fac25', family=family)
            except Accepted:
                return True
            except ValueError:
                return False
        raise AssertionError('preflight did not terminate')

    assert accepted('rank16') and accepted('cohere')
    if (ROOT / 'fresh-cohere-dev64-config.json').exists():
        actual = json.loads((ROOT / 'fresh-cohere-dev64-config.json').read_text())
        assert actual['generation_artifacts']['generation/canonical.bin'] == source['canonical_artifact'], 'native publisher requires authenticated local canonical.bin'
        with patch.object(launcher.subprocess, 'check_output', side_effect=['', 'f' * 40]), \
             patch.object(launcher.subprocess, 'run'), \
             patch.object(launcher.io, 'BytesIO', side_effect=Accepted), \
             patch.object(launcher.boto3, 'Session', side_effect=AssertionError('AWS execution')):
            try:
                launcher.main('a0001', 'subnet-0a12dbed0ca6fac25', family='cohere')
            except Accepted:
                pass
            else:
                raise AssertionError('actual CoHere authority did not terminate at archive boundary')
    for changed in [('valid_source_construction', False), ('state', 'running'),
                    ('remote_bodies_independently_hashed', False), ('query_or_truth_used', True),
                    ('quality_measured', True), ('root_sha256', '0' * 64),
                    ('source_raw_sha256', '0' * 64)]:
        assert not accepted('cohere', changed), changed
    print(json.dumps(dict(passed=True, accepted_source_authorities=2,
                          rejected_source_authority_changes=7, aws_or_query_execution=False,
                          launcher_sha256=launcher.sha((ROOT / 'aws-fresh-rank16-dev64.py').read_bytes()))))


if __name__ == '__main__':
    check()
