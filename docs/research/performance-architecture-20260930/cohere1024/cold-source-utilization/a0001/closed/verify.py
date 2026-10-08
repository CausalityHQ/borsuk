import hashlib
import io
import json
from pathlib import Path
import subprocess

root = Path('/data/target/borsuk-cold-membership-native/source-utilization-a0001')
collected = root / 'collected'
terminal = json.loads((root / 'terminal.json').read_text())
assert terminal['instance_id'] == 'i-031f52784480b4e71' and terminal['source_commit'] == '9b98778dec172972c2e57d695e26341a8cfb18be'
assert terminal['original_exit'] == terminal['exit'] == terminal['qualification_exit'] == 1 and terminal['replay_exit'] is None
assert json.loads((root / 'wait.json').read_text())['exit'] == 0
job = json.loads((root / 'active-job.json').read_text())
assert job['status'] == 'TERMINATED_COLLECTED_NOT_YET_VERIFIED' and job['closure'] == 'TERMINAL'
archive = (root / 'evidence.tar.gz').read_bytes()
assert len(archive) == terminal['evidence']['bytes'] and hashlib.sha256(archive).hexdigest() == terminal['evidence']['sha256']
lines = (root / 'artifacts.sha256').read_text().splitlines()
for line in lines:
    digest, name = line.split('  ', 1)
    assert hashlib.sha256((collected / name.removeprefix('./')).read_bytes()).hexdigest() == digest
native = json.loads((root / 'native-source.json').read_text())
assert native == json.loads((collected / 'native-source.json').read_text()) and len(native) == 425
assert hashlib.sha256(b''.join(p.encode() + b'\0' + h.encode() + b'\n' for p, h in sorted(native.items()))).hexdigest() == terminal['native_source_identity_sha256']
batch = subprocess.run(['/usr/bin/git', 'cat-file', '--batch'], cwd='/home/rb/worktrees/borsuk-prod-ready-v9', input=b''.join((terminal['source_commit'] + ':' + p + '\n').encode() for p in sorted(native)), capture_output=True, check=True)
stream = io.BytesIO(batch.stdout)
for p in sorted(native):
    header = stream.readline().split()
    assert len(header) == 3 and header[1] == b'blob'
    body = stream.read(int(header[2]))
    assert hashlib.sha256(body).hexdigest() == native[p] and stream.read(1) == b'\n'
assert not stream.read()
pins = [line.split('  ', 1)[1] for line in (root / 'source-files.sha256').read_text().splitlines()]
for name in ['source-staging.log', 'source-before.log', 'source-after.log']:
    observed = (collected / name).read_text().splitlines()
    assert len(observed) == len(pins) and set(observed) == {p + ': OK' for p in pins}, name
assert (collected / 'inventory.native-exit').read_text().strip() == '101'
assert (collected / 'inventory.tee-exit').read_text().strip() == '0'
log = (collected / 'inventory.log').read_bytes()
assert hashlib.sha256(log).hexdigest() == '9754b3b4f1952bb909925ff77088d3d19c2618a2c1964b931c397621549d6067'
assert b'recursion limit reached while expanding' in log and b'compare_native_replay.rs:1809:9' in log
assert b'test result:' not in log
for name in ['affected', 'release', 'clippy', 'test-build', 'replay']:
    assert not (collected / (name + '.native-exit')).exists()
assert not (collected / 'source-utilization.json').exists() and not (collected / 'replay-original-exit').exists()
for field, expected in {'cpu.max': '200000 100000', 'memory.max': '8589934592', 'memory.swap.max': '0', 'pids.max': '512'}.items():
    assert (collected / (field + '.before')).read_text().strip() == expected
for name in ['memory.events.after', 'global-memory.events.after']:
    events = {k: int(v) for k, v in (line.split() for line in (collected / name).read_text().splitlines())}
    assert events['oom'] == events['oom_kill'] == 0
assert int((collected / 'global-pids.current.after').read_text()) == 0
assert int((collected / 'global-memory.swap.peak.after').read_text()) == 0
receipt = {'status': 'EXECUTION_INVALID_COMPILER_MACRO_RECURSION', 'instance_id': terminal['instance_id'], 'terminated_and_waited': True, 'source': terminal['source_commit'], 'full_native_file_count': 425, 'full_native_identity': terminal['native_source_identity_sha256'], 'full_source_before_after_git_match': True, 'authenticated_artifact_count': len(lines), 'inventory_compile_exit': 101, 'tests_executed': 0, 'required_stages': 'ALL_FOUR_UNRUN', 'metadata_replay': 'UNRUN', 'raw_log_sha256': hashlib.sha256(log).hexdigest(), 'repair': 'crate recursion_limit256; unchanged algorithm/configuration/resource limits in new recorded attempt', 'algorithm_rejected': False, 'performance_claim': False}
(root / 'independent-failure-verification.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt))
