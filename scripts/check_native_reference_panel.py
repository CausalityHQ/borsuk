"""AWS-only affected native reference CLI tests and compiled-source authority."""
import atexit
import hashlib
import json
import subprocess
import sys
from pathlib import Path

cargo, repo, out = sys.argv[1:]
repo, out = Path(repo), Path(out)
def resources():
    group = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    (out / 'reference-cgroup.json').write_text(json.dumps({name: (group / name).read_text() for name in
        ['memory.peak', 'memory.swap.peak', 'memory.events', 'cpu.stat'] if (group / name).exists()}, indent=2) + '\n')
atexit.register(resources)
args = ['--release', '--locked', '--manifest-path', str(repo / 'Cargo.toml'),
        '--target-dir', str(out / 'target'), '-p', 'borsuk', '--jobs', '4', '--bin', 'two_bit_plan_demo']
for label, operation in [('tests', 'test'), ('release', 'build')]:
    with (out / (label + '.log')).open('x') as log:
        status = subprocess.run([cargo, operation, *args], stdout=log, stderr=subprocess.STDOUT).returncode
    assert status == 0, (label, status)
    if operation == 'test':
        assert 'test result: ok. 3 passed; 0 failed;' in (out / 'tests.log').read_text()
binary = out / 'target/release/two_bit_plan_demo'
# Invalid options are checked before reading a root or opening S3.
checks = []
for options in [[], ['--top-k', '10'], ['--panel-count', '0', '--top-k', '10'],
                ['--panel-count', '1001', '--top-k', '100'], ['--panel-count', '1000', '--top-k', '11']]:
    run = subprocess.run([str(binary), *options], capture_output=True, text=True)
    assert run.returncode == 1 and 'Error:' in run.stderr
    checks.append(dict(options=options, returncode=run.returncode, stderr=run.stderr))
(out / 'binaries').mkdir(exist_ok=True)
data = binary.read_bytes()
(out / 'binaries/two_bit_plan_demo').write_bytes(data)
source = 'crates/borsuk/src/bin/two_bit_plan_demo.rs'
result = dict(qualified=True, focused_tests_passed=3, cli_preopen_rejection_checks=checks,
              compiled_source_sha256=hashlib.sha256((repo / source).read_bytes()).hexdigest(),
              binary_sha256=hashlib.sha256(data).hexdigest(), binary_bytes=len(data),
              unchanged_library_assurance_reused=2696, no_corpus_query=True, full_workspace_repeated=False)
(out / 'reference-check.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
