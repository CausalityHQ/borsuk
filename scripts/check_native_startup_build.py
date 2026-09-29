"""Qualify only the native startup paths and retain their exact build identity."""
import gzip
import hashlib
import json
import os
import platform
import resource
from pathlib import Path
import subprocess
import sys

RUNTIME = ('crates/borsuk/examples/two_bit_http.rs',
           'crates/borsuk/src/object_native_generation.rs',
           'crates/borsuk/src/two_bit_generation.rs')
FOCUSED = (*RUNTIME, 'crates/borsuk/tests/two_bit_generation.rs')
ARM_DELTA = {
    'Cargo.lock': '92c0da6805ad13fa78a7d6cbffaccf7738e346d3605ed8c8ac763cf5079f2590',
    'crates/borsuk/Cargo.toml': '474efdef5bd398bef758009882c8bdd859553de8f0271230430469dd96bc4d71',
    'crates/borsuk/tests/two_bit_source.rs': 'd341033f45c614da69b70c3f3c4b1ce86ebaf1f26ba59da03a54e926d00a98d3',
}
FOCUSED_ARM = (*FOCUSED, 'Cargo.toml', *ARM_DELTA)
CONTROL = Path('docs/research/native-union-20260928/startup-profile/a0001')
CONTROL_COMMIT = '217a33dfc6b63d74169a793c3c49ccd9912e4419'
CONTROL_ARCHIVE = '4a8437ff664adbbd4cf8fb6e09382ac7dc923f886615a8a0be6e7990ab6cf6c0'
CONTROL_VERIFICATION = '53eccc0c6f23bca2231b321f198d793bf87f5332066e53caca51f536f2b66e50'
CONTROL_TERMINAL = '404d6c6e69ef98c5ea158612d56db7416efd8723b788f25d3b5e9b35c7fa8093'
SOURCE_TESTS = ('authentication_backend_matches_sha256_known_answers',
    'streams_source_records_and_rejects_wrong_identity_order_budget_and_overwrite',
    'opens_authenticated_plane_and_rejects_wrong_generation_corruption_and_budget')
CONTROL_FILES = ('binaries/two_bit_http', 'boundary-check.json', 'source-qualification.json',
                 'compiled-source.json', 'cpu.txt', 'release.log', 'run-closed.log')
ARM_EVIDENCE = ('source.log', 'arm-feature-tree.txt', 'x86-feature-tree.txt',
                'cpuinfo.txt', 'rustc-version.txt', 'cargo-version.txt', 'control-identity.json',
                *('control/' + name for name in CONTROL_FILES))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def source_hashes(repo):
    paths = set(repo.rglob('*.rs')) | set(repo.rglob('Cargo.toml')) | set(repo.rglob('Cargo.lock'))
    return {str(path.relative_to(repo)): sha(path.read_bytes()) for path in sorted(paths)
            if not {'.git', 'target'}.intersection(path.relative_to(repo).parts)}


def source_identity(hashes):
    return sha(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode())


def control_authority(repo):
    terminal_body = (repo / CONTROL / 'aws-terminal.json').read_bytes()
    assert sha(terminal_body) == CONTROL_TERMINAL
    terminal = json.loads(terminal_body)
    verification_body = (repo / CONTROL / 'verification.json').read_bytes()
    assert sha(verification_body) == CONTROL_VERIFICATION
    verified = json.loads(verification_body)
    assert verified['valid_diagnostic'] and verified['state'] == 'terminated'
    assert verified['ann_queries'] == 0 and len(verified['records']) == 6
    assert verified['source_commit'] == terminal['source_commit'] == CONTROL_COMMIT
    assert verified['source_archive_sha256'] == terminal['source_archive_sha256'] == CONTROL_ARCHIVE
    assert verified['terminal_sha256'] == CONTROL_TERMINAL
    assert terminal['status'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['instance_id'] == verified['instance_id']
    artifacts = {}
    for name in CONTROL_FILES:
        body = gzip.decompress((repo / CONTROL / (name + '.gz')).read_bytes())
        ident = terminal['artifacts'][name]
        assert len(body) == ident['bytes'] and sha(body) == ident['sha256'], name
        artifacts[name] = body
    boundary = json.loads(artifacts['boundary-check.json'])
    qualified = json.loads(artifacts['source-qualification.json'])
    assert boundary['qualified'] and boundary['no_corpus_query']
    assert boundary['green_status'] == boundary['release_status'] == 0
    assert boundary['current_full_suite_pass_claim'] is False
    assert boundary['compiled_native_sha256'] == verified['compiled_native_sha256']
    assert boundary['compiled_native_sha256'] == json.loads(artifacts['compiled-source.json'])
    assert qualified['compiled_native_sha256'] == boundary['compiled_native_sha256']
    assert set(boundary['compiled_native_sha256']) == set(FOCUSED)
    assert boundary['compiled_http_sha256'] == boundary['compiled_native_sha256'][RUNTIME[0]]
    assert boundary['binary_sha256'] == sha(artifacts['binaries/two_bit_http'])
    assert all(sha((repo / name).read_bytes()) == digest
               for name, digest in boundary['compiled_native_sha256'].items())
    return dict(source_commit=CONTROL_COMMIT, source_archive_sha256=CONTROL_ARCHIVE,
                terminal_sha256=CONTROL_TERMINAL,
                artifacts={name: terminal['artifacts'][name] for name in CONTROL_FILES},
                compiled_native_sha256=boundary['compiled_native_sha256'],
                toolchain_parity_asserted=False,
                toolchain_evidence='Archived release/run-closed logs; no archived rustc -vV or cargo -V'), artifacts


def feature_checks(cargo, repo, out):
    assert platform.machine() == 'aarch64', 'candidate must qualify on actual ARM'
    for target, name in [('aarch64-unknown-linux-gnu', 'arm'), ('x86_64-unknown-linux-gnu', 'x86')]:
        text = subprocess.check_output([cargo, 'tree', '--locked', '--manifest-path', str(repo / 'Cargo.toml'),
            '-p', 'borsuk', '--target', target, '-e', 'features', '--prefix', 'none'], text=True)
        (out / (name + '-feature-tree.txt')).write_text(text)
        lines = [line.removesuffix(' (*)') for line in text.splitlines()]
        selected = 'sha2 feature "asm"' in lines
        assert selected == (name == 'arm')
        assert ('sha2-asm v0.6.4' in lines) == (name == 'arm')
        assert ('sha2 feature "sha2-asm"' in lines) == (name == 'arm')
    cpuinfo = Path('/proc/cpuinfo').read_text()
    (out / 'cpuinfo.txt').write_text(cpuinfo)
    features = [set(line.split(':', 1)[1].split()) for line in cpuinfo.splitlines()
                if line.split(':', 1)[0].strip() == 'Features']
    assert features and all('sha2' in flags for flags in features), 'ARM SHA256 CPU capability absent'
    rustc = subprocess.check_output([str(Path(cargo).with_name('rustc')), '-vV'], text=True)
    (out / 'rustc-version.txt').write_text(rustc)
    assert 'host: aarch64-unknown-linux-gnu' in rustc.splitlines()
    assert rustc.splitlines()[0] == 'rustc 1.98.0 (88d9e12ae 2026-08-18)', 'control compiler release/commit differs'
    version = subprocess.check_output([cargo, '-V'], text=True)
    (out / 'cargo-version.txt').write_text(version)
    return dict(arm_asm_selected=True, x86_asm_selected=False, cpu_sha2_capable=True,
                toolchain_parity_asserted=False,
                rustc_sha256=sha(rustc.encode()), cargo_sha256=sha(version.encode()),
                cpuinfo_sha256=sha(cpuinfo.encode()))


def main(cargo, repo, out, arm_sha=False):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    original = {name: (repo / name).read_bytes() for name in (FOCUSED_ARM if arm_sha else FOCUSED)}
    if arm_sha:
        qualification = json.loads((out / 'source-qualification.json').read_text())
        assert {name: sha(original[name]) for name in ARM_DELTA} == ARM_DELTA
        identities = source_hashes(repo)
        assert source_identity(identities) == qualification['source_identity_sha256']
        assert len(identities) == qualification['source_file_count']
        authority, control = control_authority(repo)
        assert authority == qualification['control']
        for name, body in control.items():
            target = out / 'control' / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(body)
        (out / 'control/binaries/two_bit_http').chmod(0o755)
        (out / 'control-identity.json').write_text(json.dumps(authority, indent=2) + '\n')
        features = feature_checks(cargo, repo, out)
    args = ['--release', '--locked', '--manifest-path', str(repo / 'Cargo.toml'),
            '--target-dir', str(out / 'target'), '-p', 'borsuk', '--jobs', '4']
    checks = [('object-native', ['--lib', 'object_native_generation::tests']),
              ('generation', ['--test', 'two_bit_generation']),
              ('http', ['--example', 'two_bit_http'])]
    if arm_sha:
        checks.append(('source', ['--test', 'two_bit_source']))
    for name, target in checks:
        with (out / (name + '.log')).open('x') as log:
            subprocess.run([cargo, 'test', *args, *target], stdout=log,
                           stderr=subprocess.STDOUT, check=True)
        text = (out / (name + '.log')).read_text()
        assert '0 failed;' in text and 'test result: ok. 0 passed;' not in text
        if name == 'source':
            assert all('test ' + test + ' ... ok' in text for test in SOURCE_TESTS)
    with (out / 'release.log').open('x') as log:
        subprocess.run([cargo, 'build', *args, '--example', 'two_bit_http'],
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    assert all((repo / name).read_bytes() == body for name, body in original.items())
    if arm_sha:
        assert source_hashes(repo) == identities
    (out / 'compiled-source.json').write_text(json.dumps(
        {name: sha(body) for name, body in original.items()}, indent=2) + '\n')
    for name, body in original.items():
        target = out / 'compiled-source' / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    binary = (out / 'target/release/examples/two_bit_http').read_bytes()
    (out / 'binaries').mkdir(exist_ok=True)
    target = out / 'binaries/two_bit_http'
    target.write_bytes(binary)
    target.chmod(0o755)
    capture_cgroup(out / 'boundary-cgroup.json')
    report = dict(qualified=True, no_corpus_query=True, full_workspace_repeated=False,
        green_status=0, release_status=0, focused_tests=[name for name, _ in checks],
        compiled_native_sha256={name: sha(body) for name, body in original.items()},
        compiled_http_sha256=sha(original[RUNTIME[0]]), binary_sha256=sha(binary),
        prior_unaffected_assurance_tests=2696, current_full_suite_pass_claim=False)
    if arm_sha:
        report.update(sha_backend=features, source_identity_sha256=source_identity(identities),
            source_file_count=len(identities), exact_arm_delta_sha256=ARM_DELTA,
            historical_assurance_scope='2696 prior tests only; changed dependency not fully requalified',
            control_source_commit=CONTROL_COMMIT, control_source_archive_sha256=CONTROL_ARCHIVE)
    (out / 'boundary-check.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


def capture_cgroup(output):
    group = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    counters = {k: (group/k).read_text() for k in
        ['memory.max', 'memory.peak', 'memory.swap.max', 'memory.swap.peak',
         'memory.swap.events', 'memory.events', 'cpu.stat']}
    counters['rlimit_as_bytes'] = list(resource.getrlimit(resource.RLIMIT_AS))
    counters['cpu_affinity'] = sorted(os.sched_getaffinity(0))
    Path(output).write_text(json.dumps(counters, indent=2) + '\n')
    assert counters['cpu_affinity'] == [0, 1, 2, 3]
    assert int(counters['memory.swap.max']) == int(counters['memory.swap.peak']) == 0
    if Path(output).name == 'profile-cgroup.json':
        assert int(counters['memory.max']) == 8 * 1024**3
        assert counters['rlimit_as_bytes'] == [4 * 1024**3, 4 * 1024**3]
    else:
        assert int(counters['memory.max']) == 10 * 1024**3



if __name__ == '__main__':
    if sys.argv[1] == '--cgroup':
        capture_cgroup(sys.argv[2])
    elif sys.argv[1] == '--arm-sha':
        main(*sys.argv[2:], arm_sha=True)
    else:
        main(*sys.argv[1:])
