"""Qualify only the native startup paths and retain their exact build identity."""
import hashlib
import json
import os
import resource
from pathlib import Path
import subprocess
import sys

RUNTIME = ('crates/borsuk/examples/two_bit_http.rs',
           'crates/borsuk/src/object_native_generation.rs',
           'crates/borsuk/src/two_bit_generation.rs')
FOCUSED = (*RUNTIME, 'crates/borsuk/tests/two_bit_generation.rs')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main(cargo, repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    original = {name: (repo / name).read_bytes() for name in FOCUSED}
    args = ['--release', '--locked', '--manifest-path', str(repo / 'Cargo.toml'),
            '--target-dir', str(out / 'target'), '-p', 'borsuk', '--jobs', '4']
    checks = [('object-native', ['--lib', 'object_native_generation::tests']),
              ('generation', ['--test', 'two_bit_generation']),
              ('http', ['--example', 'two_bit_http'])]
    for name, target in checks:
        with (out / (name + '.log')).open('x') as log:
            subprocess.run([cargo, 'test', *args, *target], stdout=log,
                           stderr=subprocess.STDOUT, check=True)
        text = (out / (name + '.log')).read_text()
        assert '0 failed;' in text and 'test result: ok. 0 passed;' not in text
    with (out / 'release.log').open('x') as log:
        subprocess.run([cargo, 'build', *args, '--example', 'two_bit_http'],
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    assert all((repo / name).read_bytes() == body for name, body in original.items())
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
    else:
        main(*sys.argv[1:])
