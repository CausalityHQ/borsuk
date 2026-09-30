"""Bounded fresh ARM paged-source build using the shared owned-Spot lifecycle."""
import fcntl
import gzip
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import patch

from scripts import launch_native_metadata_ranges_cold_spot as ranges
from scripts import launch_native_startup_profile_spot as startup
from scripts import launch_native_peer_1m_spot as peer
from scripts import launch_v174_relaid_bind_compile_spot as runner
from scripts import run_native_paged_cold_first_query as worker
from scripts.check_native_startup_build import FOCUSED_ARM, source_hashes, source_identity
from scripts.check_native_paged_source_build import CHECKS

ROOT = Path('docs/research/source-paging-20260930')
NAME = 'cold'
CONFIG = ROOT / 'cold-config.json'
MANIFEST = ROOT / 'native-source-manifest.json'
MANIFEST_SHA = 'e23787927669acf07cbf0d5e687e99ef5a09b24f27e42e76de4c2070991a744c'
NATIVE_COMMIT = '2b0827369965ede5414a23e7be3abfefb92166b9'
SOURCE_IDENTITY = '3f05bdfd2c399fb4f93ab5c73827c7b55d8fd31f73ed967e6ac235976a7d7f64'
SCHEMA = 'borsuk-native-paged-source-cold-spot-v1'
PREFIX = 'research/source-paging/20260930/cold-'
TOKEN_PREFIX = 'paged-source-cold-'
TAG = 'borsuk-paged-source-cold'
SUBNET = 'subnet-00243d923761c047c'
WALL = 4200
COMPUTE_CAP = .35
CODE = (*worker.CODE, 'scripts/launch_native_paged_source_cold_spot.py',
    'scripts/check_native_paged_source_build.py',
    'scripts/launch_native_metadata_ranges_cold_spot.py', 'scripts/check_native_startup_build.py')
COMPILED = (*FOCUSED_ARM, *('crates/borsuk/src/' + name for name in (
    'sq8_s3_range.rs', 'sq8_page_authority.rs', 'two_bit_source.rs', 'two_bit_build.rs',
    'two_bit_index.rs', 'unit_centroid_graph.rs', 'bin/build_two_bit_graph_variant.rs',
    'bin/two_bit_plan_demo.rs', 'bin/two_bit_union_nomination.rs', 'bin/two_bit_walk_nomination.rs')),
    'crates/borsuk/tests/two_bit_application_ids.rs', 'crates/borsuk/tests/two_bit_gc_delayed_delete.rs')
TOOLCHAIN = ('rustc-version.txt', 'cargo-version.txt', 'cpuinfo.txt',
             'arm-feature-tree.txt', 'x86-feature-tree.txt')
ARTIFACTS = ('test.log', 'test-resources.txt', 'run-closed.log', 'cpu.txt',
    'source-qualification.json', 'boundary-check.json', 'boundary-cgroup.json',
    'compiled-source.json', 'binaries/two_bit_http', 'resolved-config.json',
    *(name + '.log' for name, _ in CHECKS), 'release.log', *TOOLCHAIN,
    'profile.log', 'profile-resources.txt', 'profile-cgroup.json', 'screen/summary.json',
    'screen/relaion-records.jsonl', 'screen/cohere-records.jsonl',
    *('compiled-source/' + name for name in COMPILED))


def preflight(base=Path('.')):
    base = Path(base)
    manifest_body = (base / MANIFEST).read_bytes()
    assert peer.sha(manifest_body) == MANIFEST_SHA
    manifest = json.loads(manifest_body)
    assert manifest['schema'] == 'borsuk-native-paged-source-manifest-v1'
    assert manifest['native_source_commit'] == NATIVE_COMMIT
    assert manifest['current_full_suite_pass_claim'] is False
    identities = source_hashes(base)
    assert identities == manifest['source_sha256']
    assert len(identities) == manifest['source_file_count'] == 395
    assert source_identity(identities) == manifest['source_identity_sha256'] == SOURCE_IDENTITY
    body = (base / CONFIG).read_bytes()
    config = json.loads(body)
    assert config['schema'] == worker.SCHEMA
    assert (config['region'], config['bucket'], config['count'], config['k']) == (peer.REGION, peer.BUCKET, 64, 10)
    assert type(config['count']) is type(config['k']) is int
    assert config['dataset_order'] == ['ReLAION', 'CoHere']
    assert config['binary'] is None, 'binary identity must come from this fresh build'
    assert config['native_source_identity_sha256'] == SOURCE_IDENTITY
    assert type(config['native_source_file_count']) is int and config['native_source_file_count'] == 395
    assert config['native_source_manifest'] == dict(path=str(MANIFEST), sha256=MANIFEST_SHA)
    assert set(config['code_sha256']) == set(worker.CODE) and len(worker.CODE) == 10
    for name, digest in config['code_sha256'].items():
        assert peer.sha((base / name).read_bytes()) == digest, name
    assert config['client_cpu_affinity'] == [4, 5] and config['native_cpu_affinity'] == [0, 1, 2, 3]
    assert config['worker_limit_seconds'] == 1500 and config['machine_limit_seconds'] == WALL
    assert config['namespace_connect_deadline_seconds'] == 45 and config['native_process_limit_seconds'] == 60
    assert config['query_payload_timeout_seconds'] == 5
    assert config['previous_observed_development_panel'] is True
    assert config['namespace_cold_start_included'] is True and config['application_sq8_cache'] is False
    assert config['s3_service_cache'] == 'uncontrolled' and config['transport'] == 'loopback plain HTTP'
    assert config['matched_vendor_measured'] is False and config['offered_or_saturation_qps_measured'] is False
    assert config['matched_control_latency_measured'] is False
    assert config['gates']['recall_at_10_minimum'] == .95
    assert config['gates']['all_calls_success'] is True
    assert config['gates']['source_scorer_ordered_id_physical_parity'] is True
    assert config['gates']['cold_start_to_first_http_response_p90_ms_exclusive_maximum'] == 444
    assert config['source_caps'] == worker.SOURCE_CAPS
    assert [item['dataset'] for item in config['items']] == config['dataset_order']
    for item in config['items']:
        assert (item['rows'], item['dimensions']) == (1000000, 768)
        assert len(item['metadata_files']) == 9 and 'plane/records.bin' not in item['metadata_files']
        assert all(type(size) is int and size > 0 for size in item['metadata_files'].values())
        authority = item['authority']
        assert set(authority) == {'root_sha256', 'generation', 'control_epoch'}
        assert len(authority['root_sha256']) == 64 and int(authority['root_sha256'], 16) >= 0
        assert type(authority['generation']) is type(authority['control_epoch']) is int
        assert authority['generation'] > 0 and authority['control_epoch'] > 0
        assert item['indexes'] == {'10': 'research/native-union/20260930/source-paging-v6/indexes/'
                                  + item['dataset'].lower() + '/k10'}
        for name in ('requests', 'reference-k10', 'truth'):
            entry = item['inputs'][name]
            assert type(entry['bytes']) is int and entry['bytes'] > 0
            assert len(entry['sha256']) == 64 and int(entry['sha256'], 16) >= 0
            assert entry['key'] and not entry['key'].startswith('/')
    return dict(config_sha256=peer.sha(body), config_path=str(CONFIG), campaign_schema=SCHEMA,
        manifest_path=str(MANIFEST), manifest_sha256=MANIFEST_SHA, native_source_commit=NATIVE_COMMIT,
        source_identity_sha256=SOURCE_IDENTITY, source_file_count=395,
        compiled_native_sha256={name: identities[name] for name in COMPILED},
        code_sha256={name: peer.sha((base / name).read_bytes()) for name in CODE},
        artifact_roster_sha256=peer.sha(json.dumps(ARTIFACTS, separators=(',', ':')).encode()),
        native_rebuilt=True, current_full_suite_pass_claim=False,
        matched_control_latency_measured=False, matched_vendor_measured=False)


def poll(ec2, s3, prefix, instance_id, started):
    with patch.object(startup, 'WALL', WALL):
        return startup.poll(ec2, s3, prefix, instance_id, started)


def collect(s3, prefix, out, instance_id, commit, digest):
    with patch.multiple(ranges, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        terminal = ranges.collect(s3, prefix, out, instance_id, commit, digest)
    original_sha = json.loads((out / 'aws-reservation.json').read_bytes())['config_sha256']
    assert terminal['config_sha256'] == terminal['original_config_sha256'] == original_sha
    assert terminal['manifest_sha256'] == MANIFEST_SHA
    # Failed builds can have no resolved config. Successful resolution is bound
    # to both original authority and the exact fresh binary artifact.
    if 'resolved-config.json' in terminal['artifacts']:
        resolved_body = gzip.decompress((out / 'resolved-config.json.gz').read_bytes())
        assert terminal['resolved_config_sha256'] == peer.sha(resolved_body)
        resolved = json.loads(resolved_body)
        assert resolved['binary'] == terminal['artifacts']['binaries/two_bit_http']
        original_body = CONFIG.read_bytes()
        assert peer.sha(original_body) == original_sha
        original = json.loads(original_body)
        assert original['binary'] is None
        assert resolved == dict(original, binary=resolved['binary'])
        for name in ('source-qualification.json', 'boundary-check.json'):
            proof = json.loads(gzip.decompress((out / (name + '.gz')).read_bytes()))
            assert proof['original_config_sha256'] == proof['config_sha256'] == original_sha
            assert proof['resolved_config_sha256'] == peer.sha(resolved_body)
            assert proof['binary_sha256'] == resolved['binary']['sha256']
            assert proof['binary_bytes'] == resolved['binary']['bytes']
    else:
        assert terminal['resolved_config_sha256'] is None and terminal['status'] != 'complete'
    return terminal


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    with patch.multiple(runner, WALL_SECONDS=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS):
        body = runner.user_data(commit, archive_sha, archive_key, prefix)
    body = body.replace('v174-relaid-bind-compile', 'native-paged-source-cold')
    roster = json.dumps(ARTIFACTS, separators=(',', ':'))
    body = body.replace(' '.join(ARTIFACTS), "$(python3 -c 'import json; print(\" \".join(json.load(open(\"artifact-roster.json\"))))')")
    body = body.replace(repr(ARTIFACTS), 'json.loads(Path("artifact-roster.json").read_text())')
    body = body.replace('phase=bootstrap', "cat >artifact-roster.json <<'ROSTER'\n" + roster + "\nROSTER\nphase=bootstrap")
    body = body.replace("  'artifacts':artifacts}", f"  'config_sha256':'{qualification['config_sha256']}',\n"
        f"  'original_config_sha256':'{qualification['config_sha256']}',\n"
        f"  'manifest_sha256':'{qualification['manifest_sha256']}',\n"
        "  'resolved_config_sha256':artifacts.get('resolved-config.json',{}).get('sha256'),\n"
        "  'artifacts':artifacts}")
    body = body.replace('gcc gcc-c++ cmake perl tar gzip time',
        'gcc gcc-c++ cmake perl tar gzip time python3-devel python3.12 pkgconf-pkg-config')
    body = body.replace('export RUSTUP_HOME=',
        'python3.12 -m ensurepip\npython3.12 -m pip install -q boto3\nexport RUSTUP_HOME=')
    start, end = body.index('phase=test\n'), body.index('phase=complete')
    proof = json.dumps(qualification, sort_keys=True, separators=(',', ':'))
    command = f'''cat >source-qualification.json <<'QUALIFICATION'
{proof}
QUALIFICATION
phase=binary-qualification
lscpu >cpu.txt
systemd-run --unit=paged-source-build --wait --pipe -p MemoryMax=10G -p MemorySwapMax=0 -p RuntimeMaxSec=2430 \\
 --setenv=PYTHONPATH="$root/repo" --setenv=RUSTUP_HOME="$RUSTUP_HOME" --setenv=CARGO_HOME="$CARGO_HOME" --setenv=PATH="$CARGO_HOME/bin:$PATH" \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 2400 \\
 taskset -c 0-3 python3.12 "$root/repo/scripts/check_native_paged_source_build.py" "$CARGO_HOME/bin/cargo" "$root/repo" "$root" >test.log 2>&1
test -s boundary-check.json && test -s binaries/two_bit_http && test -s resolved-config.json
resolved_sha=$(python3.12 -c 'import json; print(json.load(open("boundary-check.json"))["resolved_config_sha256"])')
phase=profile
systemd-run --unit=native-paged-source-cold --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p RuntimeMaxSec=1530 -p WorkingDirectory="$root/repo" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=TOKIO_WORKER_THREADS=4 --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_NATIVE_MEMORY_BYTES=1073741824 \\
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 1500 \\
 bash -c 'ulimit -v 4194304 || exit 96; taskset -c 4-5 python3.12 -m scripts.run_native_paged_cold_first_query "$1/resolved-config.json" "$2" "$1/binaries/two_bit_http" "$1/boundary-check.json" "$1/screen"; code=$?; taskset -c 0-3 python3.12 scripts/check_native_startup_build.py --cgroup "$1/profile-cgroup.json" || exit 96; exit "$code"' _ "$root" "$resolved_sha" >profile.log 2>&1
'''
    command += '\n'.join('test -s "$root/' + name + '"' for name in ARTIFACTS if name.startswith('screen/')) + '\n'
    body = body[:start] + command + body[end:]
    subprocess.run(['bash', '-n'], input=body, text=True, check=True)
    assert len(body.encode()) < 16384
    assert 'resolved_config_sha256' in body
    return body


def lifecycle_self_check():
    """Exercise this campaign through shared main; every AWS method is mocked."""
    from datetime import datetime, timezone
    import contextlib
    import io
    import tempfile
    from unittest.mock import Mock
    module = sys.modules[__name__]
    tokens = []
    for failure in (None, 'fsync', 'upload', 'poll', 'interrupt', 'multi-ack'):
        with tempfile.TemporaryDirectory() as tmp:
            ec2, s3, session = Mock(), Mock(), Mock()
            session.client.side_effect = [ec2, s3]
            ec2.describe_instances.return_value = {'Reservations': []}
            ec2.describe_subnets.return_value = {'Subnets': [{'AvailabilityZone': 'mock-az'}]}
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
                    patch.object(ranges.boto3, 'Session', return_value=session), \
                    patch.object(ranges.subprocess, 'check_output', side_effect=['', '0'*40, b'archive']), \
                    patch.object(peer, 'missing', return_value=True), \
                    patch.object(peer, 'put_if_absent', side_effect=writes), \
                    patch.object(ranges.os, 'fsync', side_effect=OSError('persist') if failure == 'fsync' else None), \
                    contextlib.redirect_stdout(io.StringIO()):
                attempt = 'a0002' if failure == 'multi-ack' else 'a0001'
                try:
                    ranges.main(attempt, campaign=module)
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
            tokens.append(launch['ClientToken'])
            reservation = json.loads((Path(tmp)/NAME/attempt/'aws-reservation.json').read_bytes())
            assert reservation['schema'] == SCHEMA and reservation['wall_seconds'] == 4200
            assert reservation['compute_cap_usd'] == .35 and reservation['ebs_s3_allowance_usd'] == .15
            assert reservation['total_cost_measured'] is False
            close = json.loads((Path(tmp)/NAME/attempt/'aws-closeout.json').read_bytes())
            assert close['state'] == 'terminated'
            assert close['nodes'] == {str(i): dict(instance_id=value) for i, value in enumerate(ids)}
    assert tokens[0] != tokens[-1]
    assert all(token.startswith(TOKEN_PREFIX) and len(token) <= 64 for token in tokens)
    print('PASS shared ACK/fsync/upload/poll/interrupt, same owned IDs waited before collection')


def self_check():
    """Synthetic configs and fake Cargo only; no cloud or native execution."""
    import contextlib
    import copy
    import io
    import os
    import tempfile
    from unittest.mock import Mock
    from scripts import check_native_paged_source_build as build
    from scripts.check_native_startup_build import SOURCE_TESTS
    module = sys.modules[__name__]
    original = Path.cwd()
    def rejected(call):
        try:
            call()
        except (AssertionError, KeyError, ValueError):
            return
        raise AssertionError('changed authority accepted')
    with tempfile.TemporaryDirectory() as tmp:
        repo = Path(tmp)/'repo'
        def write(name, body):
            path = repo/name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(body)
        for name in (*source_hashes(original), *CODE, MANIFEST):
            write(name, (original/name).read_bytes())
        # Reuse the serial protocol fields; authority and inputs here are synthetic.
        config = json.loads((original/peer.ROOT/'cold-first-query-config.json').read_bytes())
        config.pop('frozen_native_qualification')
        config.update(schema=worker.SCHEMA, binary=None, worker_limit_seconds=1500,
            machine_limit_seconds=WALL, native_source_identity_sha256=SOURCE_IDENTITY,
            native_source_file_count=395, matched_control_latency_measured=False,
            native_source_manifest=dict(path=str(MANIFEST), sha256=MANIFEST_SHA),
            source_caps=worker.SOURCE_CAPS,
            code_sha256={name: peer.sha((repo/name).read_bytes()) for name in worker.CODE})
        for item in config['items']:
            item['indexes'] = {'10': 'research/native-union/20260930/source-paging-v6/indexes/'
                               + item['dataset'].lower() + '/k10'}
            del item['metadata_files']['plane/records.bin']
            item['metadata_files']['plane/page_digests.bin'] = 1000000
        write(CONFIG, json.dumps(config).encode())
        qualification = preflight(repo)
        assert len(COMPILED) == len(set(COMPILED)) == 20
        assert len(ARTIFACTS) == len(set(ARTIFACTS)) == 51
        assert len(CODE) == len(set(CODE)) == 14
        originals = runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS
        body = user_data('0'*40, '1'*64, 'sources/mock', 'mock', qualification)
        assert originals == (runner.WALL_SECONDS, runner.SCHEMA, runner.ARTIFACTS)
        terminal_code = body.split("python3 - <<'PY' >terminal.json\n", 1)[1].split('\nPY\n', 1)[0]
        compile(terminal_code, 'terminal-receipt', 'exec')
        for token in ('--on-active=4200s', 'MemoryMax=10G', 'MemoryMax=8G', 'MemorySwapMax=0',
            'RuntimeMaxSec=2430', 'RuntimeMaxSec=1530', '--kill-after=30 2400',
            '--kill-after=30 1500', 'ulimit -v 4194304', 'taskset -c 4-5',
            'taskset -c 0-3', 'TOKIO_WORKER_THREADS=4', 'AWS_MAX_ATTEMPTS=1',
            'BORSUK_NATIVE_MEMORY_BYTES=1073741824', 'rustup',
            'check_native_paged_source_build.py', 'scripts.run_native_paged_cold_first_query',
            '"$1/resolved-config.json" "$2"', '"$root" "$resolved_sha"'):
            assert token in body, token
        assert not any(token in body for token in ('git fetch', 'git clone', 'control/binaries', '--workspace'))
        for key, value in [('schema', ranges.SCHEMA), ('count', 63), ('k', 100),
            ('binary', dict(sha256='a'*64, bytes=1)), ('worker_limit_seconds', 1501),
            ('machine_limit_seconds', 4201), ('client_cpu_affinity', [0, 1]),
            ('native_source_identity_sha256', '0'*64), ('native_source_file_count', True),
            ('matched_control_latency_measured', True), ('source_caps', {}),
            ('gates', dict(config['gates'], all_calls_success=False)),
            ('items', [dict(config['items'][0], metadata_files={}), config['items'][1]])]:
            write(CONFIG, json.dumps(dict(config, **{key: value})).encode())
            rejected(lambda: preflight(repo))
        write(CONFIG, json.dumps(config).encode())
        for name in ('Cargo.toml', 'crates/borsuk/src/two_bit_source.rs', CODE[0], MANIFEST):
            before = (repo/name).read_bytes()
            write(name, before + b'\nchanged')
            rejected(lambda: preflight(repo))
            write(name, before)
        calls = []
        def fake_features(cargo, base, out):
            assert cargo == 'fake-cargo' and base == repo
            for name in TOOLCHAIN:
                (out/name).write_text('synthetic toolchain\n')
            return dict(arm_asm_selected=True, x86_asm_selected=False,
                cpu_sha2_capable=True, toolchain_parity_asserted=False)
        def fake_cargo(args, stdout, **kwargs):
            assert args[0] == 'fake-cargo' and '--locked' in args and '--release' in args
            assert '-p' in args and args[args.index('-p')+1] == 'borsuk'
            assert args[args.index('--jobs')+1] == '4' and '--workspace' not in args
            calls.append(args)
            if args[1] == 'test':
                stdout.write('test result: ok. 5 passed; 0 failed;\n')
                for name in (*SOURCE_TESTS, *build.SOURCE_WALK_TESTS):
                    stdout.write('test '+name+' ... ok\n')
            else:
                path = Path(args[args.index('--target-dir')+1])/'release/examples/two_bit_http'
                path.parent.mkdir(parents=True)
                path.write_bytes(b'synthetic fresh candidate')
        for mutation in (None, 'native-after-build', 'config-after-build', 'existing-target', 'empty-tests'):
            out = Path(tmp)/str(mutation)
            out.mkdir()
            (out/'source-qualification.json').write_text(json.dumps(qualification))
            if mutation == 'existing-target':
                (out/'target').mkdir()
            before_native = (repo/'Cargo.toml').read_bytes()
            before_config = (repo/CONFIG).read_bytes()
            calls.clear()
            def invoke(args, **kwargs):
                fake_cargo(args, **kwargs)
                if mutation == 'empty-tests' and args[1] == 'test':
                    kwargs['stdout'].write('test result: ok. 0 passed; 0 failed;\n')
                if args[1] == 'build':
                    if mutation == 'native-after-build':
                        write('Cargo.toml', before_native + b'\n# changed\n')
                    if mutation == 'config-after-build':
                        write(CONFIG, json.dumps(dict(config, count=63)).encode())
            with patch.object(build, 'feature_checks', side_effect=fake_features), \
                    patch.object(build, 'capture_cgroup') as cgroup, \
                    patch.object(build.subprocess, 'run', side_effect=invoke), \
                    contextlib.redirect_stdout(io.StringIO()):
                if mutation is None:
                    build.main('fake-cargo', repo, out)
                    cgroup.assert_called_once_with(out/'boundary-cgroup.json')
                else:
                    rejected(lambda: build.main('fake-cargo', repo, out))
                    assert not (out/'boundary-check.json').exists()
            write('Cargo.toml', before_native)
            write(CONFIG, before_config)
            if mutation is not None:
                continue
            assert len(calls) == len(CHECKS)+1 == 10
            assert [args[args.index('--jobs')+2:] for args in calls[:-1]] == [target for _, target in CHECKS]
            assert calls[-1][-2:] == ['--example', 'two_bit_http']
            boundary = json.loads((out/'boundary-check.json').read_bytes())
            qualified = json.loads((out/'source-qualification.json').read_bytes())
            resolved = json.loads((out/'resolved-config.json').read_bytes())
            binary = (out/'binaries/two_bit_http').read_bytes()
            assert boundary['compiled_native_sha256'] == qualification['compiled_native_sha256']
            assert boundary['focused_tests'] == [name for name, _ in CHECKS]
            assert boundary['native_rebuilt'] and boundary['current_full_suite_pass_claim'] is False
            assert boundary['sha_backend']['toolchain_parity_asserted'] is False
            assert resolved == dict(config, binary=dict(sha256=peer.sha(binary), bytes=len(binary)))
            assert qualified['original_config_sha256'] == qualification['config_sha256']
            assert qualified['resolved_config_sha256'] == peer.sha((out/'resolved-config.json').read_bytes())
            assert all((out/'compiled-source'/name).read_bytes() == (repo/name).read_bytes() for name in COMPILED)
            # Exercise the real runtime identity gate with the fake-built binary.
            try:
                os.chdir(repo)
                worker.validate_config(resolved)
                worker.validate_runtime(resolved, out/'binaries/two_bit_http', out/'boundary-check.json')
                rejected(lambda: worker.validate_runtime(dict(resolved,
                    binary=dict(sha256='0'*64, bytes=len(binary))), out/'binaries/two_bit_http', out/'boundary-check.json'))
            finally:
                os.chdir(original)
            # Authenticate collection after shared shutdown, including resolution.
            values = {name: ('synthetic '+name).encode() for name in ARTIFACTS}
            for name in ('source-qualification.json', 'boundary-check.json', 'resolved-config.json', 'binaries/two_bit_http'):
                values[name] = (out/name).read_bytes()
            for change in (None, 'instance', 'archive', 'roster', 'bytes', 'body', 'original', 'resolved'):
                terminal = dict(schema=SCHEMA, instance_id='i-owned', source_commit='0'*40,
                    source_archive_sha256='1'*64, status='complete', phase='complete', exit_code=0,
                    config_sha256=qualification['config_sha256'], original_config_sha256=qualification['config_sha256'],
                    resolved_config_sha256=qualified['resolved_config_sha256'], manifest_sha256=MANIFEST_SHA,
                    artifacts={name: dict(bytes=len(value), sha256=peer.sha(value)) for name, value in values.items()})
                if change == 'instance': terminal['instance_id'] = 'i-other'
                if change == 'archive': terminal['source_archive_sha256'] = '2'*64
                if change == 'roster': terminal['artifacts']['unexpected'] = {}
                if change == 'bytes': terminal['artifacts']['boundary-check.json']['bytes'] += 1
                if change == 'original': terminal['original_config_sha256'] = '0'*64
                if change == 'resolved': terminal['resolved_config_sha256'] = '0'*64
                def get_object(**kwargs):
                    key = kwargs['Key']
                    value = json.dumps(terminal).encode() if key.endswith('/terminal.json') else values[key.split('/artifacts/', 1)[1]]
                    if change == 'body' and key.endswith('/artifacts/boundary-check.json'):
                        value += b'changed'
                    return dict(Body=io.BytesIO(value))
                s3 = Mock()
                s3.get_object.side_effect = get_object
                destination = Path(tmp)/('collected-'+str(change))
                destination.mkdir()
                (destination/'aws-reservation.json').write_text(json.dumps(dict(config_sha256=qualification['config_sha256'])))
                with patch.object(module, 'CONFIG', repo/CONFIG):
                    if change is None:
                        collect(s3, 'mock', destination, 'i-owned', '0'*40, '1'*64)
                        assert all(gzip.decompress((destination/(name+'.gz')).read_bytes()) == value for name, value in values.items())
                    else:
                        rejected(lambda: collect(s3, 'mock', destination, 'i-owned', '0'*40, '1'*64))
        print('PASS synthetic config/whole-source guards, fake focused build/resolution/runtime/collection; userdata bytes='+str(len(body.encode())))
    lifecycle_self_check()


if __name__ == '__main__':
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        assert len(sys.argv) == 2, 'usage: python3 -m scripts.launch_native_paged_source_cold_spot aNNNN'
        with open('/tmp/borsuk-native-paged-source-cold-launch.lock', 'a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            ranges.main(sys.argv[1], campaign=sys.modules[__name__])
