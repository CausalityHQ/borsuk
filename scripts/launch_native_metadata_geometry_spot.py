"""Matched metadata geometry campaign; --self-check never launches compute."""
import fcntl
import json
from pathlib import Path
import sys

from scripts import launch_native_metadata_ranges_cold_spot as shared
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts import run_native_metadata_ranges_cold as worker
from scripts.check_native_startup_build import FOCUSED_ARM, source_hashes, source_identity

ROOT = peer.ROOT
CONFIG = ROOT / 'metadata-geometry-config.json'
SCHEMA = 'borsuk-native-metadata-geometry-spot-v1'
PREFIX = 'research/native-union/20260930/metadata-geometry-'
NAME = 'metadata-geometry'
TOKEN_PREFIX = 'metadata-geometry-'
TAG = 'borsuk-metadata-geometry'
WALL = 4200
COMPUTE_CAP = .35
NATIVE_COMMIT = '1224634bfb121eef533789a422f1c06fd7adf142'
CONTROL_IDENTITY = '6566a30c7ccfbf5ec8a8d4481b2568fc020b67e831e5d186a94f5025ab156ec2'
CANDIDATE_IDENTITY = '4bbc6c332e78e5a068001913597cdc82d6c13bd147acd96e019149c190a39a1c'
STAGE = shared.STAGE
GRAPH = 'crates/borsuk/src/unit_centroid_graph.rs'
COMPILED = (*FOCUSED_ARM, GRAPH)
CODE = (*worker.CODE, 'scripts/launch_native_metadata_geometry_spot.py',
        'scripts/check_native_metadata_geometry_build.py')
TOOLCHAIN = ('rustc-version.txt', 'cargo-version.txt', 'cpuinfo.txt',
             'arm-feature-tree.txt', 'x86-feature-tree.txt')
ARM_ARTIFACTS = ('binaries/two_bit_http', 'boundary-check.json', 'compiled-source.json',
    *('compiled-source/' + name for name in COMPILED),
    'object-native.log', 'generation.log', 'http.log', 'source.log', 'graph.log',
    'release.log', *TOOLCHAIN)
ARTIFACTS = ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt',
    'source-qualification.json', *TOOLCHAIN, 'boundary-cgroup.json',
    'profile.log', 'profile-resources.txt', 'profile-cgroup.json',
    *ARM_ARTIFACTS, *('control/' + name for name in ARM_ARTIFACTS),
    'screen/summary.json', *('screen/block' + str(block) + '-records.jsonl' for block in range(4)))
# Candidate toolchain evidence is already in the global root.
ARTIFACTS = tuple(dict.fromkeys(ARTIFACTS))


def preflight(base=Path('.')):
    base = Path(base)
    config_body = (base / CONFIG).read_bytes()
    config = json.loads(config_body)
    assert config['schema'] == 'borsuk-native-metadata-geometry-v1'
    reference = config['items_source']
    assert reference == dict(path=str(ROOT / 'cold-first-query-config.json'),
        sha256='78ea8ea182432ad8dc38a5b879b97229796232cb6f21d30a40b204eec38c26a1')
    old_body = (base / reference['path']).read_bytes()
    assert peer.sha(old_body) == reference['sha256']
    old = json.loads(old_body)
    # The immutable closed protocol is reused verbatim, except the campaign
    # schema, runtime budgets and the explicitly preregistered paired gate.
    for key, value in old.items():
        if key not in {'schema', 'binary', 'frozen_native_qualification',
                       'code_sha256', 'gates', 'worker_limit_seconds', 'machine_limit_seconds'}:
            assert config[key] == value, key
    assert config['gates'] == dict(old['gates'], candidate_cold_p90_lower_than_control=True)
    assert config['worker_limit_seconds'] == 1500 and config['machine_limit_seconds'] == WALL
    assert config['ann_queries'] == 256
    assert config['blocks'] == [dict(arm=a, begin=b, end=e) for a, b, e in worker.BLOCKS]
    assert config['staging'] == dict(control=dict(range_bytes=8388608, parallel_gets=4),
        candidate=dict(range_bytes=4194304, parallel_gets=8))
    assert not {'control_binary', 'binary', 'frozen_native_qualification',
                'reviewed_native_delta_sha256'} & config.keys()
    assert set(config['code_sha256']) == set(worker.CODE)
    for name, digest in config['code_sha256'].items():
        assert peer.sha((base / name).read_bytes()) == digest, name
    code = {name: peer.sha((base / name).read_bytes()) for name in CODE}
    identities = source_hashes(base)
    assert len(identities) == 395 and source_identity(identities) == CANDIDATE_IDENTITY
    compiled = {name: identities[name] for name in COMPILED}
    assert config['compiled_sha256'] == compiled
    assert config['candidate_native'] == dict(source_identity_sha256=CANDIDATE_IDENTITY,
        source_file_count=395, stage_sha256=identities[STAGE])
    stage_ref = config['control_stage']
    assert stage_ref['path'] == str(ROOT / 'metadata-geometry-control-stage.txt')
    assert stage_ref['sha256'] == '14b2b51cf97449b4e23e343049c0896ec94f731408a45e5798b79f720631cef6'
    stage_body = (base / stage_ref['path']).read_bytes()
    assert peer.sha(stage_body) == stage_ref['sha256'] != identities[STAGE]
    # Authenticate the whole control epoch by replacing ONLY the staging file.
    control = dict(identities, **{STAGE: stage_ref['sha256']})
    assert source_identity(control) == CONTROL_IDENTITY
    assert config['control_native'] == dict(source_commit=NATIVE_COMMIT,
        source_identity_sha256=CONTROL_IDENTITY, source_file_count=395,
        stage_sha256=stage_ref['sha256'])
    peers_body = (base / ROOT / 'peer-1m-config.json').read_bytes()
    assert peer.sha(peers_body) == 'cd32e35343deff1b5006b83aa1be558a1821873c095856cce85fcde221d691aa'
    peers = json.loads(peers_body)
    for item, prior in zip(config['items'], peers['items']):
        assert {k: v for k, v in item.items() if k != 'metadata_files'} == prior
        receipt = (base / item['closed_dev_verification_path']).read_bytes()
        assert peer.sha(receipt) == item['closed_dev_verification_sha256']
        dev = json.loads(receipt)
        assert dev['valid_measurement'] and dev['state'] == 'terminated'
    return dict(config_sha256=peer.sha(config_body), code_sha256=code,
        source_identity_sha256=CANDIDATE_IDENTITY, source_file_count=395,
        compiled_native_sha256=compiled, control_source_identity_sha256=CONTROL_IDENTITY,
        control_compiled_native_sha256={name: control[name] for name in COMPILED},
        control_source_commit=NATIVE_COMMIT, control_stage=stage_ref,
        native_rebuilt=True, control_native_rebuilt=True, current_full_suite_pass_claim=False,
        artifact_roster_sha256=peer.sha(json.dumps(ARTIFACTS, separators=(',', ':')).encode()),
        config_path=str(CONFIG), campaign_schema=SCHEMA)


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    # Shared generators have module constants; scope their bindings and restore
    # them so generating this campaign cannot contaminate another campaign.
    from unittest.mock import patch
    with patch.multiple(shared, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS, CONFIG=CONFIG), \
         patch.multiple(runner, WALL_SECONDS=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        body = shared.user_data(commit, archive_sha, archive_key, prefix, qualification)
    body = body.replace('native-metadata-ranges-cold', 'native-metadata-geometry')
    body = body.replace('metadata-ranges-build', 'metadata-geometry-build')
    body = body.replace('check_native_metadata_ranges_build.py', 'check_native_metadata_geometry_build.py')
    import subprocess
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384
    return body


def poll(ec2, s3, prefix, instance_id, started):
    from unittest.mock import patch
    with patch.object(shared, 'WALL', WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


def collect(s3, prefix, out, instance_id, commit, digest):
    from unittest.mock import patch
    with patch.multiple(shared, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        return shared.collect(s3, prefix, out, instance_id, commit, digest)


def main(attempt):
    return shared.main(attempt, sys.modules[__name__])


def build_self_check(repo, proof):
    import contextlib
    import io
    import subprocess
    import tempfile
    from unittest.mock import patch
    from scripts import check_native_metadata_geometry_build as build
    candidate_body = (repo / STAGE).read_bytes()
    control_body = (repo / proof['control_stage']['path']).read_bytes()
    original_manifest = (repo / 'Cargo.toml').read_bytes()
    for failure in ('success', 'control-test', 'candidate-clean', 'candidate-test', 'graph-count', 'toolchain', 'mutation'):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / 'source-qualification.json').write_text(json.dumps(proof))
            commands = []
            arm = 'control'
            def features(cargo, base, destination):
                for name in TOOLCHAIN:
                    (destination / name).write_bytes(('same-worker-' + name + ('changed' if failure == 'toolchain' and destination == out else '')).encode())
                return dict(arm_asm_selected=True, x86_asm_selected=False,
                            cpu_sha2_capable=True, toolchain_parity_asserted=False)
            def fake_cargo(args, stdout, **kwargs):
                nonlocal arm
                commands.append(args[1])
                assert args[0] == 'fake-cargo'
                assert args[args.index('--target-dir') + 1] == str(out / 'target')
                assert kwargs['check'] is True
                if args[1] == 'clean':
                    assert commands == ['test']*5 + ['build', 'clean']
                    arm = 'candidate'
                    assert (repo / STAGE).read_bytes() == candidate_body
                    if failure == 'candidate-clean':
                        raise subprocess.CalledProcessError(1, args)
                    (out / 'target/release/examples/two_bit_http').unlink()
                    return
                assert '--locked' in args and '--release' in args
                assert args[args.index('--jobs') + 1] == '4'
                assert (repo / STAGE).read_bytes() == (control_body if arm == 'control' else candidate_body)
                if failure == arm + '-test':
                    raise subprocess.CalledProcessError(1, args)
                if failure == 'mutation':
                    (repo / 'Cargo.toml').write_bytes(original_manifest + b'\n# unreviewed mutation\n')
                if args[1] == 'test':
                    count = (6 if failure == 'graph-count' else 7) if args[-1] == 'unit_centroid_graph::tests' else 1
                    stdout.write('test result: ok. ' + str(count) + ' passed; 0 failed;\n')
                    stdout.write('\n'.join('test ' + name + ' ... ok' for name in build.SOURCE_TESTS))
                else:
                    binary = out / 'target/release/examples/two_bit_http'
                    binary.parent.mkdir(parents=True, exist_ok=True)
                    binary.write_bytes((arm + '-fresh-binary').encode())
            try:
                with patch.object(build, 'feature_checks', side_effect=features), \
                     patch.object(build, 'capture_cgroup'), \
                     patch.object(subprocess, 'run', side_effect=fake_cargo), \
                     contextlib.redirect_stdout(io.StringIO()):
                    build.main('fake-cargo', repo, out)
            except (subprocess.CalledProcessError, AssertionError):
                assert failure != 'success'
            else:
                assert failure == 'success', 'qualification failure swallowed'
                assert commands == ['test']*5 + ['build', 'clean'] + ['test']*5 + ['build']
                for arm, destination in [('control', out / 'control'), ('candidate', out)]:
                    boundary = json.loads((destination / 'boundary-check.json').read_bytes())
                    expected = proof['control_compiled_native_sha256' if arm == 'control' else 'compiled_native_sha256']
                    assert boundary['compiled_native_sha256'] == expected
                    assert boundary['focused_tests'] == ['object-native', 'generation', 'http', 'source', 'graph']
                    binary = (destination / 'binaries/two_bit_http').read_bytes()
                    assert binary == (arm + '-fresh-binary').encode()
                    assert boundary['binary_sha256'] == peer.sha(binary) and boundary['binary_bytes'] == len(binary)
                    assert boundary['same_worker_toolchain'] and boundary['sha_backend']['toolchain_parity_asserted']
                    assert boundary['current_full_suite_pass_claim'] is False
                    for name, digest in expected.items():
                        assert peer.sha((destination / 'compiled-source' / name).read_bytes()) == digest
            finally:
                assert (repo / STAGE).read_bytes() == candidate_body
                (repo / 'Cargo.toml').write_bytes(original_manifest)


def lifecycle_self_check():
    import tempfile
    import os
    import subprocess
    from datetime import datetime, timezone
    from unittest.mock import Mock, patch
    module = sys.modules[__name__]
    for failure in ('fsync', 'launch-upload', 'poll', 'interrupt', 'success'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations': []}
            ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'mock-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [
                {'SpotPrice': '0.1', 'Timestamp': datetime.now(timezone.utc)}]}
            ec2.run_instances.return_value = {'Instances': [{'InstanceId': 'i-geometry'}]}
            events = []
            ec2.terminate_instances.side_effect = lambda **kw: events.append('terminate')
            ec2.get_waiter.return_value.wait.side_effect = lambda **kw: events.append('wait')
            def collected(*args):
                assert events == ['terminate', 'wait']
                events.append('collect')
                return dict(status='complete', phase='complete', exit_code=0,
                            artifacts={name: {} for name in ARTIFACTS})
            writes = [None, None, OSError('upload')] if failure == 'launch-upload' else [None]*3
            error = None if failure == 'success' else (KeyboardInterrupt() if failure == 'interrupt' else RuntimeError('interruption'))
            with patch.object(module, 'ROOT', Path(tmp)), \
                 patch.object(shared.boto3, 'Session', return_value=session), \
                 patch.object(subprocess, 'check_output', side_effect=['', '0'*40, b'archive']), \
                 patch.object(module, 'preflight', return_value={'config_sha256': '1'*64}), \
                 patch.object(module, 'user_data', return_value='mock'), \
                 patch.object(peer, 'missing', return_value=True), \
                 patch.object(peer, 'put_if_absent', side_effect=writes), \
                 patch.object(os, 'fsync', side_effect=OSError('persist') if failure == 'fsync' else None), \
                 patch.object(module, 'poll', side_effect=error), \
                 patch.object(module, 'collect', side_effect=collected):
                try:
                    main('a0001')
                except (OSError, RuntimeError, KeyboardInterrupt):
                    assert failure != 'success'
                else:
                    assert failure == 'success'
            ec2.run_instances.assert_called_once()
            launch = ec2.run_instances.call_args.kwargs
            assert launch['ClientToken'].startswith(TOKEN_PREFIX)
            assert len(launch['ClientToken']) <= 64
            assert launch['TagSpecifications'][0]['Tags'][0]['Value'] == TAG
            assert launch['InstanceType'] == 'c7g.2xlarge'
            ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-geometry'])
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-geometry'])
            out = Path(tmp) / NAME / 'a0001'
            close = json.loads((out / 'aws-closeout.json').read_bytes())
            assert close['nodes'] == {'0': {'instance_id': 'i-geometry'}}
            assert events == ['terminate', 'wait', 'collect']
            reservation = json.loads((out / 'aws-reservation.json').read_bytes())
            assert reservation['schema'] == SCHEMA and reservation['compute_cap_usd'] == COMPUTE_CAP
    print('geometry shared lifecycle PASS')


def self_check():
    import tempfile
    import subprocess
    from unittest.mock import patch
    with tempfile.TemporaryDirectory() as tmp:
        base = Path(tmp)
        def write(name, body):
            target = base / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(body)
        identities = source_hashes(Path('.'))
        for name in identities:
            write(name, Path(name).read_bytes())
        root = peer.ROOT
        for name in ('metadata-ranges-config.json', 'cold-first-query-config.json', 'peer-1m-config.json'):
            write(root / name, (root / name).read_bytes())
        old = json.loads((root / 'metadata-ranges-config.json').read_bytes())
        for item in old['items']:
            name = item['closed_dev_verification_path']
            write(name, Path(name).read_bytes())
        for name in CODE:
            write(name, Path(name).read_bytes())
        stage = subprocess.check_output(['git', 'show', NATIVE_COMMIT + ':' + STAGE])
        stage_path = root / 'metadata-geometry-control-stage.txt'
        write(stage_path, stage)
        control = dict(identities, **{STAGE: peer.sha(stage)})
        config = dict(old, schema='borsuk-native-metadata-geometry-v1',
            control_native=dict(source_commit=NATIVE_COMMIT, source_identity_sha256=source_identity(control),
                source_file_count=395, stage_sha256=peer.sha(stage)),
            candidate_native=dict(source_identity_sha256=source_identity(identities),
                source_file_count=395, stage_sha256=identities[STAGE]),
            compiled_sha256={name: identities[name] for name in COMPILED},
            control_stage=dict(path=str(stage_path), sha256=peer.sha(stage)),
            staging=dict(control=dict(range_bytes=8388608, parallel_gets=4),
                         candidate=dict(range_bytes=4194304, parallel_gets=8)),
            items_source=dict(path=str(root / 'cold-first-query-config.json'),
                sha256=peer.sha((root / 'cold-first-query-config.json').read_bytes())),
            code_sha256={name: peer.sha((base / name).read_bytes()) for name in worker.CODE})
        for key in ('control_binary', 'reviewed_native_delta_sha256', 'frozen_native_qualification'):
            del config[key]
        write(CONFIG, json.dumps(config).encode())
        proof = preflight(base)
        assert proof['source_file_count'] == 395 and len(proof['compiled_native_sha256']) == 9
        assert set(proof['code_sha256']) == set(CODE)
        assert proof['control_native_rebuilt'] and proof['current_full_suite_pass_claim'] is False
        assert (base / STAGE).read_bytes() == Path(STAGE).read_bytes()
        body = user_data('0'*40, '1'*64, 'sources/mock', 'mock', proof)
        assert len(body.encode()) < 16384
        for token in ('--on-active=4200s', 'MemoryMax=10G', 'RuntimeMaxSec=2430',
            'MemoryMax=8G', 'RuntimeMaxSec=1530', 'ulimit -v 4194304',
            'TOKIO_WORKER_THREADS=4', 'BORSUK_NATIVE_MEMORY_BYTES=1073741824',
            'taskset -c 4-5', 'taskset -c 0-3', 'check_native_metadata_geometry_build.py',
            '30 2400', '30 1500'):
            assert token in body, token
        for key, value in [('count', 63), ('ann_queries', 255),
            ('blocks', list(reversed(config['blocks']))),
            ('staging', dict(config['staging'], candidate=dict(range_bytes=8388608, parallel_gets=8))),
            ('items', [dict(config['items'][0], metadata_files={}), config['items'][1]]),
            ('control_native', dict(config['control_native'], source_identity_sha256='0'*64)),
            ('compiled_sha256', dict(config['compiled_sha256'], **{GRAPH: '0'*64}))]:
            write(CONFIG, json.dumps(dict(config, **{key: value})).encode())
            try:
                preflight(base)
            except AssertionError:
                pass
            else:
                raise AssertionError('changed authority accepted: ' + key)
        write(CONFIG, json.dumps(config).encode())
        for name in ('Cargo.toml', STAGE, worker.CODE[0], stage_path):
            before = (base / name).read_bytes()
            write(name, before + b'changed')
            try:
                preflight(base)
            except AssertionError:
                pass
            else:
                raise AssertionError('changed file accepted: ' + str(name))
            finally:
                write(name, before)
        build_self_check(base, proof)
        print('authority/build/restoration/user-data PASS; bytes=' + str(len(body.encode())))
    # Exercise the unchanged shared ownership/poll/cleanup implementation, without
    # its legacy native-epoch fixture (geometry has its own authority check above).
    with patch.object(shared, 'FROZEN', Path('/nonexistent-geometry-self-check')):
        shared.self_check()
    lifecycle_self_check()


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        assert len(sys.argv) == 2, 'usage: python3 -m scripts.launch_native_metadata_geometry_spot aNNNN'
        with open('/tmp/borsuk-native-metadata-geometry-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[1])
