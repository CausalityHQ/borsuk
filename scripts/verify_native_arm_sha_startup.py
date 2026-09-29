"""Verify a closed matched ARM SHA startup panel; never inspect live profiles."""
import json
import gzip
import io
import math
from pathlib import Path
from statistics import median
import tarfile
import tomllib

import boto3
from scripts.check_native_startup_stats import validate
from scripts.launch_native_peer_1m_spot import sha, BUCKET, REGION

ROOT = Path('docs/research/native-union-20260928')

BLOCKS = ('control0', 'candidate1', 'candidate2', 'control3')
DATASETS = ('ReLAION', 'CoHere')


def validate_source(control, candidate):
    """Readers return immutable archived bytes, independent of live Git state."""
    manifest = 'crates/borsuk/Cargo.toml'
    old = tomllib.loads(control(manifest).decode())
    new = tomllib.loads(candidate(manifest).decode())
    arm = new['target'].pop('cfg(target_arch = "aarch64")')
    assert arm == {'dependencies': {'sha2': {'version': '0.10', 'features': ['asm']}}}
    if not new['target']: del new['target']
    assert old == new, 'undeclared package manifest change'
    old_lock = tomllib.loads(control('Cargo.lock').decode())
    new_lock = tomllib.loads(candidate('Cargo.lock').decode())
    packages = new_lock['package']
    added = next(p for p in packages if p['name'] == 'sha2-asm')
    assert added == dict(name='sha2-asm', version='0.6.4',
        source='registry+https://github.com/rust-lang/crates.io-index',
        checksum='b845214d6175804686b2bd482bcffe96651bb2d1200742b712003504a2dac1ab',
        dependencies=['cc'])
    packages.remove(added)
    sha = next(p for p in packages if p['name'] == 'sha2')
    assert sha['version'] == '0.10.9'
    sha['dependencies'].remove('sha2-asm')
    assert old_lock == new_lock, 'undeclared lockfile change'


def summarize(blocks):
    assert set(blocks) == set(BLOCKS)
    for block in BLOCKS:
        rows = blocks[block]
        assert len(rows) == 6
        assert [r['dataset'] for r in rows] == list(DATASETS) * 3
        assert [r['cell'] for r in rows] == list(range(6))
    panels = {}
    for dataset in DATASETS:
        arms = {}
        for arm in ('control', 'candidate'):
            rows = [r for block in BLOCKS if block.startswith(arm)
                    for r in blocks[block] if r['dataset'] == dataset]
            assert len(rows) == 6
            arms[arm] = {key: median(r[key] for r in rows) for key in
                         ('namespace_ready_ms', 'remote_open_ms', 'staging_ms',
                          'get_header_ms', 'stream_and_output_ms', 'awaited_writes_ms', 'decode_ms')}
        delta = {key: arms['candidate'][key] - arms['control'][key] for key in arms['control']}
        panels[dataset] = dict(median_ms=arms, candidate_minus_control_ms=delta,
            diagnostic_gate_passed=delta['namespace_ready_ms'] < 0 and delta['decode_ms'] < 0)
    return dict(panels=panels, diagnostic_gate_passed=all(p['diagnostic_gate_passed'] for p in panels.values()),
                ann_queries=0, first_query_measured=False, matched_vendor_measured=False)


def main(attempt):
    from scripts import launch_native_startup_profile_spot as campaign
    directory = ROOT / 'arm-sha-startup' / attempt
    launch = json.loads((directory/'aws-launch.json').read_text())
    close = json.loads((directory/'aws-closeout.json').read_text())
    session = boto3.Session(profile_name='causality', region_name=REGION)
    s3, ec2 = session.client('s3'), session.client('ec2')
    def remote(key): return s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
    prefix = launch['prefix']
    assert prefix == 'research/native-union/20260929/arm-sha-startup-' + attempt
    assert json.loads(remote(prefix+'/launch.json')) == launch
    terminal_body = remote(prefix+'/terminal.json')
    assert terminal_body == (directory/'aws-terminal.json').read_bytes()
    terminal = json.loads(terminal_body)
    reservation = json.loads(remote(prefix+'/reservation.json'))
    assert reservation == json.loads((directory/'aws-reservation.json').read_text())
    assert terminal['schema'] == reservation['schema'] == campaign.SCHEMA_ARM
    assert terminal['status'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['instance_id'] == launch['instance_id']
    for key in ('source_commit', 'source_archive_sha256'):
        assert terminal[key] == reservation[key] == launch[key]
    archive = remote('research/native-library-check/sources/'+launch['source_archive_sha256']+'.tar.gz')
    assert sha(archive) == launch['source_archive_sha256']
    original = ROOT/'startup-profile/a0001'
    prior_body = (original/'verification.json').read_bytes()
    prior = json.loads(prior_body)
    assert prior['valid_diagnostic'] and prior['state'] == 'terminated'
    control_prefix = 'research/native-union/20260929/startup-profile-a0001'
    control_terminal_body = remote(control_prefix+'/terminal.json')
    assert sha(control_terminal_body) == prior['terminal_sha256']
    assert control_terminal_body == (original/'aws-terminal.json').read_bytes()
    control_terminal = json.loads(control_terminal_body)
    assert control_terminal['status'] == 'complete' and control_terminal['exit_code'] == 0
    control_archive = remote('research/native-library-check/sources/'+prior['source_archive_sha256']+'.tar.gz')
    assert sha(control_archive) == prior['source_archive_sha256']
    artifacts = {}
    assert set(terminal['artifacts']) == set(campaign.ARTIFACTS_ARM)
    for name, identity in terminal['artifacts'].items():
        body = remote(prefix+'/artifacts/'+name)
        assert len(body) == identity['bytes'] and sha(body) == identity['sha256']
        assert body == gzip.decompress((directory/(name+'.gz')).read_bytes())
        artifacts[name] = body
    with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as tar, \
         tarfile.open(fileobj=io.BytesIO(control_archive), mode='r:gz') as old_tar:
        def archived(name): return tar.extractfile(name).read()
        def old(name): return old_tar.extractfile(name).read()
        validate_source(old, archived)
        config_body = archived(str(campaign.CONFIG_ARM))
        assert config_body == campaign.CONFIG_ARM.read_bytes()
        config = json.loads(config_body)
        assert sha(config_body) == reservation['config_sha256']
        panel = config['matched_panel']
        assert panel['order'] == list(BLOCKS)
        assert panel['samples_per_block'] == panel['samples_per_arm_per_dataset'] == 6
        assert panel['ann_queries'] == 0 and panel['prior_control_toolchain_parity_asserted'] is False
        assert panel['control_verification_sha256'] == sha(prior_body)
        assert archived(panel['control_verification_path']) == prior_body
        for key in ('source_commit', 'source_archive_sha256', 'terminal_sha256'):
            assert panel['control_'+key] == prior[key]
        old_config = json.loads(old(str(ROOT/'startup-profile-config.json')))
        assert config['items'] == old_config['items']
        for key in ('schema', 'bucket', 'region', 'dataset_order', 'ann_queries',
                    'first_query_measured', 'matched_vendor_measured'):
            assert config[key] == old_config[key]
        for name, digest in config['code_sha256'].items(): assert sha(archived(name)) == digest
        # Compare every native source/manifest, not just the serving example.
        native = lambda name: name.endswith('.rs') or Path(name).name in ('Cargo.toml', 'Cargo.lock')
        old_names = {m.name for m in old_tar.getmembers() if m.isfile() and native(m.name)}
        new_names = {m.name for m in tar.getmembers() if m.isfile() and native(m.name)}
        assert old_names == new_names
        changed = {name for name in old_names if old(name) != archived(name)}
        assert changed == {'Cargo.lock', 'crates/borsuk/Cargo.toml', 'crates/borsuk/tests/two_bit_source.rs'}, changed
        all_hashes = {name: sha(archived(name)) for name in sorted(new_names)}
        source_identity = sha(json.dumps(all_hashes, sort_keys=True, separators=(',', ':')).encode())
        approved = json.loads(archived(str(ROOT/'arm-sha-preparation/verification.json')))
        for name, digest in approved['source_sha256'].items(): assert sha(archived(name)) == digest
        qualification = reservation['qualification']
        assert json.loads(artifacts['source-qualification.json']) == qualification
        assert qualification['source_identity_sha256'] == source_identity
        assert qualification['source_file_count'] == len(all_hashes)
        assert qualification['panel_order'] == list(BLOCKS)
        assert qualification['campaign_schema'] == campaign.SCHEMA_ARM
        assert qualification['artifact_roster_sha256'] == sha(json.dumps(campaign.ARTIFACTS_ARM, separators=(',', ':')).encode())
        assert qualification['control']['source_archive_sha256'] == prior['source_archive_sha256']
        assert qualification['control']['source_commit'] == prior['source_commit']
        assert qualification['control']['terminal_sha256'] == prior['terminal_sha256']
        assert qualification['control']['toolchain_parity_asserted'] is False
        assert json.loads(artifacts['control-identity.json']) == qualification['control']
        for name, digest in qualification['code_sha256'].items(): assert sha(archived(name)) == digest
        for name in ('scripts/launch_native_startup_profile_spot.py', 'scripts/check_native_startup_build.py'):
            assert sha(Path(name).read_bytes()) == qualification['code_sha256'][name]
        qualified = json.loads(artifacts['boundary-check.json'])
        assert qualified['qualified'] and qualified['green_status'] == qualified['release_status'] == 0
        assert qualified['no_corpus_query'] and not qualified['full_workspace_repeated']
        assert not qualified['current_full_suite_pass_claim']
        source_hashes = qualified['compiled_native_sha256']
        assert set(source_hashes) == set(prior['compiled_native_sha256']) | {
            'Cargo.toml', 'Cargo.lock', 'crates/borsuk/Cargo.toml', 'crates/borsuk/tests/two_bit_source.rs'}
        assert source_hashes == qualification['compiled_native_sha256']
        assert qualified['source_identity_sha256'] == source_identity
        assert qualified['source_file_count'] == len(all_hashes)
        assert qualified['exact_arm_delta_sha256'] == qualification['exact_arm_delta_sha256'] == approved['source_sha256']
        backend = qualified['sha_backend']
        assert backend['arm_asm_selected'] and not backend['x86_asm_selected'] and backend['cpu_sha2_capable']
        assert backend['toolchain_parity_asserted'] is False
        for key, name in [('rustc_sha256','rustc-version.txt'), ('cargo_sha256','cargo-version.txt'), ('cpuinfo_sha256','cpuinfo.txt')]:
            assert backend[key] == sha(artifacts[name])
        assert source_hashes == json.loads(artifacts['compiled-source.json'])
        for name, digest in source_hashes.items():
            assert sha(archived(name)) == sha(artifacts['compiled-source/'+name]) == digest
        for name, digest in prior['compiled_native_sha256'].items(): assert source_hashes[name] == digest
        assert qualified['binary_sha256'] == sha(artifacts['binaries/two_bit_http'])
        assert qualified['compiled_http_sha256'] == prior['compiled_native_sha256']['crates/borsuk/examples/two_bit_http.rs']
    expected_user_data = campaign.user_data(launch['source_commit'], sha(archive),
        'research/native-library-check/sources/'+sha(archive)+'.tar.gz', prefix, qualification, arm_sha=True)
    assert (directory/'aws-user-data.sh').read_text() == expected_user_data
    for name in ('object-native', 'generation', 'http', 'source'):
        assert b'0 failed;' in artifacts[name+'.log'] and b'ok. 0 passed;' not in artifacts[name+'.log']
    assert b'authentication_backend_matches_sha256_known_answers ... ok' in artifacts['source.log']
    assert b'sha2 feature "asm"' in artifacts['arm-feature-tree.txt']
    assert b'sha2 feature "asm"' not in artifacts['x86-feature-tree.txt']
    assert b'force-soft' not in artifacts['arm-feature-tree.txt']
    features = [line.split(b':', 1)[1].split() for line in artifacts['cpuinfo.txt'].splitlines()
                if line.split(b':', 1)[0].strip() == b'Features']
    assert features and all(b'sha2' in flags for flags in features)
    for name in artifacts:
        if name.startswith('control/'):
            original_name = name.removeprefix('control/')
            identity = control_terminal['artifacts'][original_name]
            assert len(artifacts[name]) == identity['bytes'] and sha(artifacts[name]) == identity['sha256']
    control = json.loads(artifacts['control/boundary-check.json'])
    assert control['binary_sha256'] == sha(artifacts['control/binaries/two_bit_http'])
    assert control['compiled_native_sha256'] == prior['compiled_native_sha256']
    installed = [line.split('installed - ', 1)[1].strip()
                 for line in artifacts['control/run-closed.log'].decode().splitlines()
                 if 'installed - rustc ' in line]
    assert len(installed) == 1
    assert artifacts['rustc-version.txt'].decode().splitlines()[0] == installed[0]
    groups = {}
    for name, cap in [('boundary-cgroup.json', 10*1024**3), ('profile-cgroup.json', 8*1024**3)]:
        group = json.loads(artifacts[name])
        assert int(group['memory.max']) == cap and int(group['memory.peak']) < cap
        assert int(group['memory.swap.max']) == int(group['memory.swap.peak']) == 0
        events = dict(line.split() for line in group['memory.events'].splitlines())
        assert events['oom'] == events['oom_kill'] == '0'
        assert group['cpu_affinity'] == [0,1,2,3]
        groups[name] = group
    assert groups['profile-cgroup.json']['rlimit_as_bytes'] == [4*1024**3,4*1024**3]
    blocks = {}
    for block in BLOCKS:
        summary = json.loads(artifacts[f'screen/{block}/summary.json'])
        assert summary['ann_queries'] == 0 and not summary['first_query_measured'] and not summary['matched_vendor_measured']
        records = []
        for cell, dataset in enumerate(config['dataset_order']):
            item = next(item for item in config['items'] if item['dataset']==dataset)
            headers = [json.loads(line) for line in artifacts[f'screen/{block}/cell{cell}-server.log'].splitlines() if line.startswith(b'{')]
            assert len(headers) == 1
            header = headers[0]
            assert header['phase']=='ready' and header['authority']==item['authority'] and header['listen']=='127.0.0.1:8080'
            record = json.loads(artifacts[f'screen/{block}/cell{cell}-profile.json'])
            reduced = validate(header['remote_open_stats'], item['metadata_files'], header['remote_open_wall_ns'])
            assert record == summary['records'][cell] and record['cell']==cell and record['dataset']==dataset
            assert all(record[key]==value for key,value in reduced.items())
            assert record['remote_open_ms']==header['remote_open_wall_ns']/1e6
            assert record['head_read_ms']==header['head_read_wall_ns']/1e6
            assert all(math.isfinite(record[key]) and record[key]>=0 for key in record if key.endswith('_ms'))
            assert record['namespace_ready_ms'] >= record['remote_open_ms']+record['head_read_ms']
            assert json.loads(artifacts[f'screen/{block}/cell{cell}-close.json'])['intentional_stop'] is True
            records.append(record)
        blocks[block] = records
    instance = ec2.describe_instances(InstanceIds=[launch['instance_id']])['Reservations'][0]['Instances'][0]
    assert instance['State']['Name'] == close['state'] == 'terminated' and instance['InstanceLifecycle']=='spot'
    assert instance['InstanceType']=='c7g.2xlarge' and instance['Placement']['AvailabilityZone']==reservation['availability_zone']
    report = dict(valid_diagnostic=True, **summarize(blocks), blocks=blocks, state='terminated',
        instance_id=launch['instance_id'], source_commit=launch['source_commit'],
        source_archive_sha256=sha(archive), config_sha256=sha(config_body), terminal_sha256=sha(terminal_body),
        control_terminal_sha256=sha(control_terminal_body), compiled_native_sha256=source_hashes,
        build_cgroup_peak_bytes=int(groups['boundary-cgroup.json']['memory.peak']),
        profile_cgroup_peak_bytes=int(groups['profile-cgroup.json']['memory.peak']), swap_peak_bytes=0,
        prior_control_compiler_version=installed[0], rustc_release_commit_parity=True,
        full_toolchain_parity_asserted=False,
        candidate_rustc_version=artifacts['rustc-version.txt'].decode().strip())
    (directory/'verification.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps(report))


def self_check():
    blocks = {block: [dict(cell=cell, dataset=dataset, **dict.fromkeys(
        ('namespace_ready_ms', 'remote_open_ms', 'staging_ms', 'get_header_ms',
         'stream_and_output_ms', 'awaited_writes_ms', 'decode_ms'),
        10 if block.startswith('control') else 5))
        for cell, dataset in enumerate(list(DATASETS) * 3)] for block in BLOCKS}
    assert summarize(blocks)['diagnostic_gate_passed']
    for block in ('candidate1', 'candidate2'):
        for row in blocks[block]:
            if row['dataset'] == 'CoHere': row['decode_ms'] = 10
    assert not summarize(blocks)['diagnostic_gate_passed']
    blocks['control0'].pop()
    try:
        summarize(blocks)
    except AssertionError:
        pass
    else:
        raise AssertionError('partial panel accepted')
    print('matched ARM SHA reducer checks PASS')


if __name__ == '__main__':
    import sys
    if sys.argv[1:] == ['--self-check']:
        self_check()
    else:
        main(sys.argv[1] if len(sys.argv)>1 else 'a0001')
