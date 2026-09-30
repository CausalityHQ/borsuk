"""Bounded paired centroid-bulk campaign; --self-check uses mocks only."""
import base64
import fcntl
import gzip
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

from scripts import launch_native_metadata_ranges_cold_spot as shared
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts import run_native_centroid_bulk_cold as worker
from scripts.check_native_startup_build import FOCUSED_ARM, source_hashes, source_identity
from scripts.check_native_centroid_bulk_build import CHECKS

ROOT = Path('docs/research/source-paging-20260930/centroid-bulk')
CONFIG = ROOT / 'config.json'
# The parent freezes the config after integration and completed local assurance.
CONFIG_SHA = None
MANIFEST = ROOT / 'native-source-manifest.json'
MANIFEST_SHA = '0dcb1b0342bc257edc772c6106b813648537ef7930f6d5a2e53b2dbc7b2505a2'
SCHEMA = 'borsuk-native-centroid-bulk-spot-v1'
PREFIX = 'research/source-paging/20260930/centroid-bulk-'
NAME = 'cold'
TOKEN_PREFIX = 'centroid-bulk-cold-'
TAG = 'borsuk-centroid-bulk-cold'
SUBNET = 'subnet-034528fbd6977848f'
WALL = 5400
COMPUTE_CAP = .45
CANDIDATE_IDENTITY = '14fa7148e55d898ab79b1a8b4a208dfc02266f7e2610e1c084080550d78b882c'
CONTROL_IDENTITY = '46e5ca162da947f3596b00291211ecd05e2059b721938047b8c1487189aedb6d'
STAGE = worker.STAGE
COMPILED = (*FOCUSED_ARM, *('crates/borsuk/src/' + name for name in (
    'sq8_s3_range.rs', 'sq8_page_authority.rs', 'two_bit_source.rs', 'two_bit_build.rs',
    'two_bit_index.rs', 'unit_centroid_graph.rs', 'bin/build_two_bit_graph_variant.rs',
    'bin/two_bit_plan_demo.rs', 'bin/two_bit_union_nomination.rs', 'bin/two_bit_walk_nomination.rs')),
    'crates/borsuk/tests/two_bit_application_ids.rs', 'crates/borsuk/tests/two_bit_gc_delayed_delete.rs', STAGE)
# Include late imports in the shared helpers, including their self-check paths.
CODE = (*worker.CODE, 'scripts/launch_native_centroid_bulk_spot.py',
    'scripts/check_native_centroid_bulk_build.py',
    'scripts/launch_native_metadata_ranges_cold_spot.py',
    'scripts/launch_native_startup_profile_spot.py', 'scripts/launch_native_peer_1m_spot.py',
    'scripts/launch_v174_relaid_bind_compile_spot.py', 'scripts/launch_v157_primary_feasibility_spot.py',
    'scripts/check_native_startup_build.py', 'scripts/check_native_paged_source_build.py',
    'scripts/check_native_metadata_ranges_build.py', 'scripts/launch_native_paged_source_cold_spot.py')
TOOLCHAIN = ('rustc-version.txt', 'cargo-version.txt', 'cpuinfo.txt',
             'arm-feature-tree.txt', 'x86-feature-tree.txt')
ARM_ARTIFACTS = ('binaries/two_bit_http', 'boundary-check.json', 'compiled-source.json',
    *('compiled-source/' + name for name in COMPILED),
    *(name + '.log' for name, _ in CHECKS), 'release.log', *TOOLCHAIN)
ARTIFACTS = ('source-qualification.json', 'cpu.txt', 'test.log', 'test-resources.txt',
    'run-closed.log', 'boundary-cgroup.json', 'profile.log', 'profile-resources.txt',
    'profile-cgroup.json', *ARM_ARTIFACTS, *('control/' + name for name in ARM_ARTIFACTS),
    'full-suite.log', 'full-suite-status.json', 'screen/summary.json',
    *('screen/block' + str(block) + '-records.jsonl' for block in range(4)))

ASSURANCE_SCOPE = 'Source-qualified local x86_64 workspace suite reused; ARM focused qualification only'
ASSURANCE_PLATFORM = 'x86_64-unknown-linux-gnu'
ASSURANCE_COMMAND = ['cargo', 'test', '--locked', '--workspace', '--all-targets']


def authenticate_assurance(base, config, identities):
    assert 'native_assurance' in config, 'completed local native_assurance pointer required'
    pointer = config['native_assurance']
    assert set(pointer) == {'path', 'sha256'}
    path = Path(pointer['path'])
    assert path == ROOT / 'implementation-gates/verification.json'
    assert not path.is_absolute() and '..' not in path.parts
    body = (base / path).read_bytes()
    assert peer.sha(body) == pointer['sha256'], 'changed local assurance proof'
    proof = json.loads(body)
    assert proof['schema'] == 'borsuk-centroid-bulk-implementation-gates-v1'
    assert proof['source_sha256'] == identities and len(identities) == proof['source_file_count'] == 395
    assert proof['source_identity_sha256'] == source_identity(identities) == CANDIDATE_IDENTITY
    assert proof['full_workspace_execution_pending'] is False, 'local workspace proof is pending'
    for key in ('full_workspace_execution_status', 'workspace_test_compilation_status',
                'clippy_status', 'affected_target_status'):
        assert type(proof[key]) is int and proof[key] == 0, key
    assert proof['workspace_command'] == ASSURANCE_COMMAND
    assert proof['workspace_platform'] == ASSURANCE_PLATFORM
    assert proof['workspace_env'] == dict(CARGO_BUILD_JOBS='2', CARGO_TARGET_DIR='/data/target', RUSTC_WRAPPER='')
    log_path = path.parent / 'full-workspace.log.gz'
    log = gzip.decompress((base / log_path).read_bytes())
    identity = proof['artifacts']['full-workspace.log']
    assert len(log) == identity['bytes'] > 0 and peer.sha(log) == identity['sha256']
    assert b'test result: ok.' in log and b'0 failed;' in log and b'FAILED' not in log
    return dict(pointer, log_path=str(log_path), log_sha256=identity['sha256'],
        log_bytes=identity['bytes'], platform=ASSURANCE_PLATFORM, command=ASSURANCE_COMMAND,
        source_identity_sha256=CANDIDATE_IDENTITY, source_file_count=395, scope=ASSURANCE_SCOPE), log


def preflight(base=Path('.')):
    base = Path(base)
    body = (base / CONFIG).read_bytes()
    assert peer.sha(body) == CONFIG_SHA, 'unregistered campaign config'
    config = json.loads(body)
    assert len(worker.CODE) == 12
    manifest = worker.validate_config(config, base)
    assert config['native_manifest'] == dict(path=str(MANIFEST), sha256=MANIFEST_SHA)
    assert manifest['schema'] == 'borsuk-centroid-bulk-source-v1'
    assert manifest['candidate_identity'] == CANDIDATE_IDENTITY
    assert manifest['control_identity'] == CONTROL_IDENTITY
    assert manifest['current_full_suite_pass_claim'] is False
    assert manifest['native_qualification_pending'] is True
    assert config['items_source'] == dict(path='docs/research/source-paging-20260930/cold-config.json',
        sha256='92a5e94ab57cced22405d42527bd4f1b54f0f0a839b8444722b7a111145100b9')
    old = json.loads((base / config['items_source']['path']).read_bytes())
    for key, value in old.items():
        if key not in {'schema', 'binary', 'code_sha256', 'machine_limit_seconds',
                       'native_source_identity_sha256', 'native_source_file_count', 'native_source_manifest'}:
            assert config[key] == value, key
    assert (config['region'], config['bucket']) == (peer.REGION, peer.BUCKET)
    assert config['machine_limit_seconds'] == WALL and config['worker_limit_seconds'] == 1500
    assert not {'binary', 'control_binary', 'frozen_native_qualification'} & config.keys()
    identities = manifest['source_sha256']
    assert manifest['candidate_stage_sha256'] == identities[STAGE]
    compiled = {name: identities[name] for name in COMPILED}
    assert len(COMPILED) == len(compiled) == 21 and config['compiled_sha256'] == compiled
    control_source = manifest['control_source']
    assert control_source == dict(path=str(ROOT / 'control-unit-centroid-pages.txt'),
        sha256='8135bbe4b766f66a944c17636a976dec958189adc227dbeb3e40cda58911ecd2')
    control = dict(identities, **{STAGE: control_source['sha256']})
    assert source_identity(control) == CONTROL_IDENTITY
    assurance, _ = authenticate_assurance(base, config, identities)
    return dict(config_path=str(CONFIG), config_sha256=peer.sha(body), campaign_schema=SCHEMA,
        manifest_path=str(MANIFEST), manifest_sha256=MANIFEST_SHA,
        source_identity_sha256=CANDIDATE_IDENTITY, source_file_count=395,
        control_source_identity_sha256=CONTROL_IDENTITY, control_source=control_source,
        compiled_native_sha256=compiled,
        control_compiled_native_sha256={name: control[name] for name in COMPILED},
        code_sha256={name: peer.sha((base / name).read_bytes()) for name in CODE},
        artifact_roster_sha256=peer.sha(json.dumps(ARTIFACTS, separators=(',', ':')).encode()),
        native_assurance=assurance, reused_source_full_suite_pass_claim=True,
        native_rebuilt=True, control_native_rebuilt=True, current_full_suite_pass_claim=False,
        full_suite_runs=0, full_suite_scope=ASSURANCE_SCOPE)


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    # Reuse the reviewed uploader/bootstrap and replace only this campaign's commands.
    minimal = dict(config_sha256=qualification['config_sha256'])
    with patch.multiple(shared, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS, CONFIG=CONFIG), \
         patch.multiple(runner, WALL_SECONDS=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        body = shared.user_data(commit, archive_sha, archive_key, prefix, minimal)
    for filename, value, marker in [('artifact-roster.json', ARTIFACTS, 'ROSTER'),
                                   ('source-qualification.json', qualification, 'QUALIFICATION')]:
        begin = body.index(f"cat >{filename} <<'{marker}'")
        end = body.index('\n' + marker, begin) + len(marker) + 1
        encoded = base64.b64encode(gzip.compress(json.dumps(value, sort_keys=True,
            separators=(',', ':')).encode(), mtime=0)).decode()
        command = "python3 -c 'import base64,gzip; from pathlib import Path; " + \
            f'Path("{filename}").write_bytes(gzip.decompress(base64.b64decode("{encoded}")))\''
        body = body[:begin] + command + body[end:]
    body = body.replace('native-metadata-ranges-cold', 'native-centroid-bulk-cold')
    body = body.replace('metadata-ranges-build', 'centroid-bulk-build')
    body = body.replace('RuntimeMaxSec=2430', 'RuntimeMaxSec=3630').replace('30 2400', '30 3600')
    body = body.replace('python3.12 "$root/repo/scripts/check_native_metadata_ranges_build.py"',
                        'python3.12 -m scripts.check_native_centroid_bulk_build')
    body = body.replace('python3.12 scripts/run_native_metadata_ranges_cold.py',
                        'python3.12 -m scripts.run_native_centroid_bulk_cold')
    body = body.replace('python3.12 scripts/check_native_startup_build.py --cgroup',
                        'python3.12 -m scripts.check_native_startup_build --cgroup')
    body = body.replace("  'artifacts':artifacts}",
        f"  'config_sha256':'{qualification['config_sha256']}',\n"
        f"  'manifest_sha256':'{qualification['manifest_sha256']}',\n"
        f"  'artifact_roster_sha256':'{qualification['artifact_roster_sha256']}',\n"
        "  'artifacts':artifacts}")
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384
    return body


def poll(ec2, s3, prefix, instance_id, started):
    with patch.object(shared, 'WALL', WALL):
        return shared.poll(ec2, s3, prefix, instance_id, started)


def collect(s3, prefix, out, instance_id, commit, digest):
    with patch.multiple(shared, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        terminal = shared.collect(s3, prefix, out, instance_id, commit, digest)
    qualification = json.loads((out / 'aws-reservation.json').read_bytes())['qualification']
    for key in ('config_sha256', 'manifest_sha256', 'artifact_roster_sha256'):
        assert terminal[key] == qualification[key], key
    return terminal


def main(attempt):
    return shared.main(attempt, sys.modules[__name__])


def lifecycle_self_check():
    """Exercise this campaign through shared main; every AWS method is mocked."""
    from datetime import datetime, timezone
    from unittest.mock import Mock
    import contextlib
    import io
    import tempfile
    module = sys.modules[__name__]
    tokens = []
    for failure in (None, 'fsync', 'upload', 'poll', 'interrupt', 'multi-ack'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations': []}
            ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'eu-central-1a'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory': [
                {'SpotPrice': '.1', 'Timestamp': datetime.now(timezone.utc)}]}
            ids = ['i-owned', 'i-extra'] if failure == 'multi-ack' else ['i-owned']
            ec2.run_instances.return_value = {'Instances': [{'InstanceId': value} for value in ids]}
            events = []
            ec2.terminate_instances.side_effect = lambda **kwargs: events.append('terminate')
            ec2.get_waiter.return_value.wait.side_effect = lambda **kwargs: events.append('wait')
            def collected(*args):
                assert events == ['terminate', 'wait']
                assert args[3] == 'i-owned'
                events.append('collect')
                return dict(status='complete', phase='complete', exit_code=0,
                    artifacts={name: {} for name in ARTIFACTS})
            error = KeyboardInterrupt() if failure == 'interrupt' else RuntimeError('mock failure')
            writes = [None, None, OSError('upload')] if failure == 'upload' else [None, None, None]
            with patch.object(module, 'ROOT', Path(tmp)), \
                    patch.object(module, 'preflight', return_value={'config_sha256': 'a'*64}), \
                    patch.object(module, 'user_data', return_value='mock'), \
                    patch.object(module, 'poll', side_effect=error if failure in ('poll', 'interrupt') else None), \
                    patch.object(module, 'collect', side_effect=collected), \
                    patch.object(shared.boto3, 'Session', return_value=session), \
                    patch.object(shared.subprocess, 'check_output', side_effect=['', '0'*40, b'archive']), \
                    patch.object(peer, 'missing', return_value=True), \
                    patch.object(peer, 'put_if_absent', side_effect=writes), \
                    patch.object(shared.os, 'fsync', side_effect=OSError('persist') if failure == 'fsync' else None), \
                    contextlib.redirect_stdout(io.StringIO()):
                attempt = 'a0002' if failure == 'multi-ack' else 'a0001'
                try:
                    main(attempt)
                except (OSError, RuntimeError, KeyboardInterrupt):
                    assert failure is not None
                else:
                    assert failure in (None, 'multi-ack')
            assert events == ['terminate', 'wait', 'collect']
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            ec2.run_instances.assert_called_once()
            launch = ec2.run_instances.call_args.kwargs
            assert launch['InstanceType'] == 'c7g.2xlarge'
            assert launch['MinCount'] == launch['MaxCount'] == 1
            assert launch['InstanceMarketOptions']['SpotOptions']['MaxPrice'] == '0.30'
            assert launch['BlockDeviceMappings'][0]['Ebs'] == dict(
                DeleteOnTermination=True, Encrypted=True, VolumeSize=80, VolumeType='gp3')
            assert launch['NetworkInterfaces'][0]['SubnetId'] == SUBNET
            assert launch['TagSpecifications'][0]['Tags'][0]['Value'] == TAG
            tokens.append(launch['ClientToken'])
            receipt = json.loads((Path(tmp)/NAME/attempt/'aws-launch.json').read_bytes())
            assert receipt['nodes'] == {str(i): dict(instance_id=value) for i, value in enumerate(ids)}
            reservation = json.loads((Path(tmp)/NAME/attempt/'aws-reservation.json').read_bytes())
            assert reservation['schema'] == SCHEMA and reservation['wall_seconds'] == WALL
            assert reservation['compute_cap_usd'] == COMPUTE_CAP and reservation['ebs_s3_allowance_usd'] == .15
            assert reservation['total_cost_measured'] is False
            close = json.loads((Path(tmp)/NAME/attempt/'aws-closeout.json').read_bytes())
            assert close['state'] == 'terminated'
            assert close['nodes'] == {str(i): dict(instance_id=value) for i, value in enumerate(ids)}
    assert tokens[0] != tokens[-1]
    assert all(token.startswith(TOKEN_PREFIX) and len(token) == 64 for token in tokens)
    print('PASS shared ACK/fsync/upload/poll/interrupt, same owned IDs waited before collection')


def build_self_check(repo, proof):
    import contextlib
    import io
    import tempfile
    from scripts import check_native_centroid_bulk_build as build
    candidate_body = (repo / STAGE).read_bytes()
    control_body = (repo / proof['control_source']['path']).read_bytes()
    uncompiled_source = repo / 'crates/borsuk/src/lib.rs'
    original_source = uncompiled_source.read_bytes()
    for failure in ('success', 'control-test', 'candidate-clean', 'candidate-test',
                    'control-graph-count', 'candidate-graph-count',
                    'control-centroid-count', 'candidate-centroid-count', 'toolchain',
                    'features', 'mutation', 'repaired-target', 'repaired-count', 'stale-target', 'qualification'):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            bad = dict(proof, source_file_count=394) if failure == 'qualification' else proof
            (out / 'source-qualification.json').write_text(json.dumps(bad))
            if failure == 'stale-target':
                (out / 'target').mkdir()
            commands = []
            arm = 'control'
            def features(cargo, base, destination):
                for name in TOOLCHAIN:
                    (destination / name).write_bytes(('same-worker-' + name +
                        ('changed' if failure == 'toolchain' and destination == out else '')).encode())
                return dict(arm_asm_selected=failure != 'features', x86_asm_selected=False,
                            cpu_sha2_capable=True, toolchain_parity_asserted=False)
            def fake_cargo(args, stdout, **kwargs):
                nonlocal arm
                commands.append(args[1])
                assert args[0] == 'fake-cargo'
                assert args[args.index('--target-dir') + 1] == str(out / 'target')
                binary = out / 'target/release/examples/two_bit_http'
                if args[1] == 'clean':
                    assert commands == ['test']*10 + ['build', 'clean']
                    arm = 'candidate'
                    assert (repo / STAGE).read_bytes() == candidate_body
                    if failure == 'candidate-clean':
                        raise subprocess.CalledProcessError(1, args)
                    binary.unlink()
                    return
                assert '--locked' in args
                assert ('--release' in args) is ('--workspace' not in args)
                assert args[args.index('--jobs') + 1] == '4'
                assert (repo / STAGE).read_bytes() == (control_body if arm == 'control' else candidate_body)
                if failure == arm + '-test':
                    raise subprocess.CalledProcessError(1, args)
                if failure == 'repaired-target' and args[-2:] == ['--test', 'exact_sq8_mirror_direct']:
                    raise subprocess.CalledProcessError(101, args)
                if failure == 'mutation':
                    uncompiled_source.write_bytes(original_source + b'\n// unreviewed mutation\n')
                assert '--workspace' not in args, 'ARM full workspace suite must never run'
                assert kwargs['check'] is True
                if args[1] == 'test':
                    count = 1
                    if args[-1] == 'unit_centroid_graph::tests':
                        count = 7 if failure == arm + '-graph-count' else 8
                    if args[-1] == 'unit_centroid_pages::tests':
                        count = 3 if arm == 'control' else 4
                        if failure == arm + '-centroid-count': count -= 1
                    if args[-2:] == ['--test', 'exact_sq8_mirror_direct']:
                        count = 3 if failure == 'repaired-count' else 4
                    stdout.write(f'test result: ok. {count} passed; 0 failed;\n')
                    stdout.write('\n'.join('test ' + name + ' ... ok'
                        for name in (*build.SOURCE_TESTS, *build.SOURCE_WALK_TESTS)))
                else:
                    assert not binary.exists()
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
                assert not (out / 'boundary-check.json').exists()
                if failure == 'repaired-target':
                    assert not (out / 'full-suite-status.json').exists()
                    assert commands == ['test']*10 + ['build', 'clean'] + ['test']*11
            else:
                assert failure == 'success', 'qualification failure swallowed'
                assert commands == ['test']*10 + ['build', 'clean'] + ['test']*11 + ['build']
                status = json.loads((out / 'full-suite-status.json').read_bytes())
                assert status['status'] == 0 and status['runs'] == 0 and status['arm'] == 'candidate'
                assert status['scope'] == build.FULL_SUITE_SCOPE
                assert status['native_assurance'] == proof['native_assurance']
                assert status['current_full_suite_pass_claim'] is status['full_workspace_repeated'] is False
                assert status['reused_source_full_suite_pass_claim'] is True
                assert peer.sha((out / 'full-suite.log').read_bytes()) == proof['native_assurance']['log_sha256']
                assert status['repaired_target_status'] == 0
                assert status['repaired_target_command'][-2:] == ['--test', 'exact_sq8_mirror_direct']
                assert not (out / 'control/full-suite.log').exists()
                for arm, destination in [('control', out / 'control'), ('candidate', out)]:
                    boundary = json.loads((destination / 'boundary-check.json').read_bytes())
                    expected = proof['control_compiled_native_sha256' if arm == 'control' else 'compiled_native_sha256']
                    assert boundary['compiled_native_sha256'] == expected
                    assert boundary['focused_tests'] == [name for name, _ in CHECKS]
                    binary = (destination / 'binaries/two_bit_http').read_bytes()
                    assert binary == (arm + '-fresh-binary').encode()
                    assert boundary['binary_sha256'] == peer.sha(binary) and boundary['binary_bytes'] == len(binary)
                    assert boundary['same_worker_toolchain'] and boundary['sha_backend']['toolchain_parity_asserted']
                    assert boundary['current_full_suite_pass_claim'] is False
                    assert boundary['full_workspace_repeated'] is False
                    assert boundary['reused_source_full_suite_pass_claim'] is (arm == 'candidate')
                    assert boundary['full_suite_runs'] == 0
                    for name, digest in expected.items():
                        assert peer.sha((destination / 'compiled-source' / name).read_bytes()) == digest
                runtime_proof_self_check(repo, out)
            finally:
                assert (repo / STAGE).read_bytes() == candidate_body
                uncompiled_source.write_bytes(original_source)


def runtime_proof_self_check(repo, out):
    import copy
    from unittest.mock import Mock
    config = json.loads((repo / CONFIG).read_bytes())
    manifest = worker.validate_config(config, repo)
    proofs = {arm: out / ('' if arm == 'candidate' else 'control') / 'boundary-check.json'
              for arm in ('candidate', 'control')}
    argv = [worker.__file__, str(repo / CONFIG), CONFIG_SHA,
        str(out / 'binaries/two_bit_http'), str(proofs['candidate']),
        str(out / 'control/binaries/two_bit_http'), str(proofs['control']), str(out / 'unused')]
    run = Mock()
    with patch.object(sys, 'argv', argv), patch.object(worker, 'validate_config', return_value=manifest), \
         patch.object(worker.os, 'sched_getaffinity', return_value={4, 5}), \
         patch.dict(os.environ, TOKIO_WORKER_THREADS='4', AWS_MAX_ATTEMPTS='1',
                    BORSUK_NATIVE_MEMORY_BYTES='1073741824'), patch.object(worker, 'run', run):
        worker.main()
        run.assert_called_once()
        for arm, path in proofs.items():
            original = path.read_bytes()
            proof = json.loads(original)
            mutations = dict(qualified=False, green_status=1, release_status=1,
                arm='wrong', same_worker_toolchain=False, source_identity_sha256='0'*64,
                source_file_count=394, binary_sha256='0'*64, binary_bytes=0,
                compiled_native_sha256={}, sha_backend=dict(proof['sha_backend'], x86_asm_selected=True))
            for key, value in mutations.items():
                path.write_text(json.dumps(dict(copy.deepcopy(proof), **{key: value})))
                try:
                    worker.main()
                except AssertionError:
                    pass
                else:
                    raise AssertionError('changed runtime proof accepted: ' + arm + '/' + key)
                finally:
                    path.write_bytes(original)
        run.assert_called_once()


def collection_self_check(qualification):
    import copy
    import io
    import tempfile
    from unittest.mock import Mock
    artifact = b'authenticated closed log'
    terminal = dict(schema=SCHEMA, instance_id='i-owned', source_commit='0'*40,
        source_archive_sha256='1'*64, status='complete', phase='complete', exit_code=0,
        artifacts={'run-closed.log': dict(bytes=len(artifact), sha256=peer.sha(artifact))},
        **{key: qualification[key] for key in ('config_sha256', 'manifest_sha256', 'artifact_roster_sha256')})
    for mutation in (None, 'schema', 'instance_id', 'source_commit', 'source_archive_sha256',
                     'config_sha256', 'manifest_sha256', 'artifact_roster_sha256', 'body', 'bytes', 'roster'):
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            (out / 'aws-reservation.json').write_text(json.dumps(dict(qualification=qualification)))
            changed = copy.deepcopy(terminal)
            if mutation in changed: changed[mutation] = 'wrong'
            if mutation == 'bytes': changed['artifacts']['run-closed.log']['bytes'] += 1
            if mutation == 'roster': changed['artifacts']['../unowned'] = changed['artifacts']['run-closed.log']
            def get_object(**kwargs):
                body = json.dumps(changed).encode() if kwargs['Key'].endswith('/terminal.json') else artifact
                if mutation == 'body' and not kwargs['Key'].endswith('/terminal.json'): body += b'changed'
                return dict(Body=io.BytesIO(body))
            s3 = Mock()
            s3.get_object.side_effect = get_object
            try:
                collected = collect(s3, 'mock', out, 'i-owned', '0'*40, '1'*64)
            except AssertionError:
                assert mutation is not None
            else:
                assert mutation is None and collected == terminal
                assert gzip.decompress((out / 'run-closed.log.gz').read_bytes()) == artifact
    print('PASS collection terminal identity, roster and artifact-body authentication')


def assurance_fixture(repo, config, identities):
    """Synthetic authority in a temporary repo; never completes the real proof."""
    path = ROOT / 'implementation-gates/verification.json'
    log = b'test result: ok. 1 passed; 0 failed; 0 ignored;\n'
    proof = dict(schema='borsuk-centroid-bulk-implementation-gates-v1',
        source_sha256=identities, source_file_count=395,
        source_identity_sha256=CANDIDATE_IDENTITY, full_workspace_execution_pending=False,
        full_workspace_execution_status=0, workspace_test_compilation_status=0,
        clippy_status=0, affected_target_status=0,
        workspace_command=ASSURANCE_COMMAND, workspace_platform=ASSURANCE_PLATFORM,
        workspace_env=dict(CARGO_BUILD_JOBS='2', CARGO_TARGET_DIR='/data/target', RUSTC_WRAPPER=''),
        artifacts={'full-workspace.log': dict(bytes=len(log), sha256=peer.sha(log))})
    (repo / path).parent.mkdir(parents=True, exist_ok=True)
    body = (json.dumps(proof) + '\n').encode()
    (repo / path).write_bytes(body)
    (repo / path.parent / 'full-workspace.log.gz').write_bytes(gzip.compress(log, mtime=0))
    return dict(config, native_assurance=dict(path=str(path), sha256=peer.sha(body)))


def config_fixture(repo, original):
    """Adapt historical protocol inputs only inside a temporary synthetic repository."""
    for name in (*source_hashes(original), *CODE,
                 'docs/research/source-paging-20260930/cold-config.json'):
        path = repo / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes((original / name).read_bytes())
    control_path = ROOT / 'control-unit-centroid-pages.txt'
    control_body = ((original / control_path).read_bytes() if (original / control_path).exists()
        else subprocess.check_output(['git', 'show', '93b11ab4:' + STAGE], cwd=original))
    (repo / control_path).parent.mkdir(parents=True, exist_ok=True)
    (repo / control_path).write_bytes(control_body)
    hashes = source_hashes(repo)
    control = dict(hashes, **{STAGE: peer.sha(control_body)})
    assert source_identity(hashes) == CANDIDATE_IDENTITY
    assert source_identity(control) == CONTROL_IDENTITY
    manifest = dict(schema='borsuk-centroid-bulk-source-v1', source_sha256=hashes,
        source_file_count=395, candidate_identity=CANDIDATE_IDENTITY,
        control_identity=CONTROL_IDENTITY, candidate_stage_sha256=hashes[STAGE],
        control_source=dict(path=str(control_path), sha256=peer.sha(control_body)),
        current_full_suite_pass_claim=False, native_qualification_pending=True)
    (repo / MANIFEST).write_text(json.dumps(manifest, indent=2) + '\n')
    config = json.loads((original / 'docs/research/source-paging-20260930/decode/config.json').read_bytes())
    config.update(schema=worker.SCHEMA,
        code_sha256={name: peer.sha((repo / name).read_bytes()) for name in worker.CODE},
        native_manifest=dict(path=str(MANIFEST), sha256=peer.sha((repo / MANIFEST).read_bytes())),
        compiled_sha256={name: hashes[name] for name in COMPILED})
    config = assurance_fixture(repo, config, hashes)
    (repo / CONFIG).write_text(json.dumps(config, indent=2) + '\n')
    return config


def assurance_self_check(repo, config, identities):
    import copy
    path = repo / config['native_assurance']['path']
    original = path.read_bytes()
    log_path = path.parent / 'full-workspace.log.gz'
    log_body = log_path.read_bytes()
    proof = json.loads(original)
    for key, value in [('schema', 'wrong-schema'), ('full_workspace_execution_pending', True),
            ('full_workspace_execution_status', False),
            ('full_workspace_execution_status', None), ('full_workspace_execution_status', 101),
            ('workspace_test_compilation_status', 1), ('clippy_status', 1),
            ('affected_target_status', 1),
            ('source_identity_sha256', '0'*64), ('source_file_count', 394), ('source_sha256', {}),
            ('workspace_command', ASSURANCE_COMMAND + ['--release']),
            ('workspace_platform', 'aarch64-unknown-linux-gnu'), ('workspace_env', {})]:
        body = json.dumps(dict(proof, **{key: value})).encode()
        path.write_bytes(body)
        changed = dict(config, native_assurance=dict(path=config['native_assurance']['path'], sha256=peer.sha(body)))
        try: authenticate_assurance(repo, changed, identities)
        except AssertionError: pass
        else: raise AssertionError('changed assurance accepted: ' + key)
    path.write_bytes(original)
    for key, value in [('bytes', len(gzip.decompress(log_body)) + 1), ('sha256', '0'*64)]:
        bad = copy.deepcopy(proof); bad['artifacts']['full-workspace.log'][key] = value
        body = json.dumps(bad).encode(); path.write_bytes(body)
        changed = dict(config, native_assurance=dict(path=config['native_assurance']['path'], sha256=peer.sha(body)))
        try: authenticate_assurance(repo, changed, identities)
        except AssertionError: pass
        else: raise AssertionError('changed assurance log identity accepted: ' + key)
    path.write_bytes(original + b' ')
    try: authenticate_assurance(repo, config, identities)
    except AssertionError: pass
    else: raise AssertionError('tampered proof accepted')
    path.write_bytes(original)
    log_path.write_bytes(gzip.compress(b'changed log', mtime=0))
    try: authenticate_assurance(repo, config, identities)
    except AssertionError: pass
    else: raise AssertionError('tampered local log accepted')
    log_path.write_bytes(log_body)
    authenticate_assurance(repo, config, identities)
    print('PASS fixture local assurance pending/status/source/argv/platform/log provenance guards')


def self_check():
    """Real pinned authorities, fake Cargo and mock lifecycle; no cloud/native work."""
    import ast
    import tempfile
    original = Path(__file__).resolve().parent.parent
    # Ensure the explicit small code roster covers every transitive scripts import.
    assert len(CODE) == len(set(CODE)) == 23
    for name in CODE:
        for node in ast.walk(ast.parse((original / name).read_text())):
            imports = []
            if isinstance(node, ast.ImportFrom) and node.module and node.module.startswith('scripts'):
                imports = [node.module] if node.module != 'scripts' else ['scripts.' + a.name for a in node.names]
            elif isinstance(node, ast.Import):
                imports = [a.name for a in node.names if a.name.startswith('scripts.')]
            for module in imports:
                path = module.replace('.', '/') + '.py'
                if (original / path).is_file(): assert path in CODE, path
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp) / 'repo'
        def write(name, body):
            path = repo / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        config = config_fixture(repo, original)
        fixture_config = (repo / CONFIG).read_bytes()
        fixture_pins = dict(CONFIG_SHA=peer.sha(fixture_config),
                            MANIFEST_SHA=peer.sha((repo / MANIFEST).read_bytes()))
        with patch.multiple(sys.modules[__name__], **fixture_pins), \
             patch.multiple('scripts.launch_native_centroid_bulk_spot', **fixture_pins):
            qualification = preflight(repo)
            assurance_self_check(repo, config, source_hashes(repo))
            assert len(ARTIFACTS) == len(set(ARTIFACTS)) == 96
            assert set(qualification['code_sha256']) == set(CODE)
            bindings = shared.WALL, shared.SCHEMA, shared.ARTIFACTS, shared.CONFIG, \
                       runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS
            body = user_data('0'*40, '1'*64, 'sources/mock', 'mock', qualification)
            assert bindings == (shared.WALL, shared.SCHEMA, shared.ARTIFACTS, shared.CONFIG,
                                runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS)
            terminal = body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0]
            compile(terminal, 'terminal-receipt', 'exec')
            for filename in ('artifact-roster.json', 'source-qualification.json'):
                line = next(line for line in body.splitlines() if f'Path("{filename}")' in line)
                encoded = line.split('base64.b64decode("')[1].split('"')[0]
                decoded = json.loads(gzip.decompress(base64.b64decode(encoded)))
                assert decoded == (list(ARTIFACTS) if filename == 'artifact-roster.json' else qualification)
            for token in ('--on-active=5400s', 'MemoryMax=10G', 'RuntimeMaxSec=3630',
                'MemoryMax=8G', 'MemorySwapMax=0', 'RuntimeMaxSec=1530', 'ulimit -v 4194304',
                'TOKIO_WORKER_THREADS=4', 'BORSUK_NATIVE_MEMORY_BYTES=1073741824', 'AWS_MAX_ATTEMPTS=1',
                'taskset -c 4-5', 'taskset -c 0-3 python3.12 -m scripts.check_native_centroid_bulk_build',
                'python3.12 -m scripts.run_native_centroid_bulk_cold', '30 3600', '30 1500',
                '--setenv=PYTHONPATH="$root/repo"', '$1/control/binaries/two_bit_http'):
                assert token in body, token
            for name in (CONFIG, MANIFEST, STAGE, 'Cargo.toml', worker.CODE[0], CODE[-1],
                         ROOT / 'control-unit-centroid-pages.txt',
                         'docs/research/source-paging-20260930/cold-config.json'):
                path = repo / name
                before = path.read_bytes()
                path.write_bytes(before + b'changed')
                try:
                    preflight(repo)
                except (AssertionError, ValueError):
                    pass
                else:
                    # Helper hashes are captured into the qualification and must not
                    # silently continue using its earlier authenticated roster.
                    assert str(name) in CODE and preflight(repo) != qualification
                finally:
                    path.write_bytes(before)
            config = json.loads((repo / CONFIG).read_bytes())
            for key, value in [('count', 63), ('ann_queries', 255), ('blocks', list(reversed(config['blocks']))),
                               ('compiled_sha256', {}), ('items_source', {}), ('source_caps', {}), ('staging', {}),
                               ('native_assurance', {}), ('native_assurance', dict(config['native_assurance'], sha256='0'*64))]:
                changed = json.dumps(dict(config, **{key: value})).encode()
                write(CONFIG, changed)
                with patch.object(sys.modules[__name__], 'CONFIG_SHA', peer.sha(changed)):
                    try: preflight(repo)
                    except (AssertionError, KeyError): pass
                    else: raise AssertionError('changed config accepted: ' + key)
            write(CONFIG, fixture_config)
            build_self_check(repo, qualification)
            collection_self_check(qualification)
            # Exercise the actual -m CLI/import from a non-repository CWD. A bad
            # qualification must fail before the fake Cargo could ever be invoked.
            out = Path(tmp) / 'cli-output'
            out.mkdir()
            (out / 'source-qualification.json').write_text('{}')
            result = subprocess.run([sys.executable, '-m', 'scripts.check_native_centroid_bulk_build',
                '/nonexistent-fake-cargo', str(repo), str(out)], cwd=out,
                env=dict(os.environ, PYTHONPATH=str(repo)), capture_output=True, text=True)
            assert result.returncode != 0 and 'AssertionError' in result.stderr
            assert 'ModuleNotFoundError' not in result.stderr and not (out / 'target').exists()
            print('PASS pinned source/config/control, build order/local assurance reuse/restoration/proof guards; user-data bytes=' + str(len(body.encode())))
    lifecycle_self_check()


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        assert len(sys.argv) == 2, 'usage: python3 -m scripts.launch_native_centroid_bulk_spot aNNNN'
        with open('/tmp/borsuk-native-centroid-bulk-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(sys.argv[1])
