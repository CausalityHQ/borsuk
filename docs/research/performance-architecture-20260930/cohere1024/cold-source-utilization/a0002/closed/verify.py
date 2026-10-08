"""Independent closure/qualification verification; no vector or ANN work."""
import datetime
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess

root = Path('/data/target/borsuk-cold-membership-native/source-utilization-a0002')
repo = Path('/home/rb/worktrees/borsuk-prod-ready-v9')
collected = root / 'collected'
protocol = json.loads((root / 'protocol.json').read_text())
terminal = json.loads((root / 'terminal.json').read_text())
launch = json.loads((root / 'launch.json').read_text())
instance = launch['Instances'][0]['InstanceId']
assert terminal['instance_id'] == instance
assert json.loads((root / 'wait.json').read_text())['exit'] == 0
assert json.loads((root / 'active-job.json').read_text())['status'] == 'TERMINATED_COLLECTED_NOT_YET_VERIFIED'
assert terminal['source_commit'] == protocol['source']['candidate']
assert terminal['source_archive_sha256'] == protocol['source']['archive_sha256']
assert terminal['native_source_identity_sha256'] == protocol['source']['native_identity']
assert terminal['native_source_file_count'] == protocol['source']['native_file_count']
assert terminal['ann_run'] is False and terminal['performance_claim'] is False and terminal['quality_claim'] is False
for line in (root / 'artifacts.sha256').read_text().splitlines():
    digest, path = line.split('  ', 1)
    assert hashlib.sha256((collected / path.removeprefix('./')).read_bytes()).hexdigest() == digest
native = json.loads((root / 'native-source.json').read_text())
assert native == json.loads((collected / 'native-source.json').read_text())
assert len(native) == protocol['source']['native_file_count']
paths = subprocess.check_output(['/usr/bin/git', 'ls-tree', '-r', '--name-only', terminal['source_commit']], cwd=repo, text=True).splitlines()
assert set(native) == {p for p in paths if p.endswith('.rs') or Path(p).name in ('Cargo.toml', 'Cargo.lock')}
batch = subprocess.run(['/usr/bin/git', 'cat-file', '--batch'], cwd=repo, input=b''.join((terminal['source_commit'] + ':' + p + '\n').encode() for p in sorted(native)), capture_output=True, check=True)
stream = io.BytesIO(batch.stdout)
for path in sorted(native):
    header = stream.readline().split()
    assert len(header) == 3 and header[1] == b'blob', path
    body = stream.read(int(header[2]))
    assert stream.read(1) == b'\n' and hashlib.sha256(body).hexdigest() == native[path], path
assert not stream.read()
del batch, stream, body
assert hashlib.sha256(b''.join(p.encode() + b'\0' + h.encode() + b'\n' for p, h in sorted(native.items()))).hexdigest() == terminal['native_source_identity_sha256']
source_pins = {line.split('  ', 1)[1]: line.split('  ', 1)[0] for line in (root / 'source-files.sha256').read_text().splitlines()}
assert (collected / 'source-files.sha256').read_bytes() == (root / 'source-files.sha256').read_bytes()
def zero(path):
    assert (collected / path).read_text().strip() == '0', path
def source_log(path):
    lines = (collected / path).read_text().splitlines()
    assert len(lines) == len(source_pins) and set(lines) == {p + ': OK' for p in source_pins}, path
for name in ['source-staging.log', 'source-before.log', 'source-after.log']:
    source_log(name)
expected = json.loads((root / 'expected-test-inventory.json').read_text())
assert expected == json.loads((collected / 'expected-test-inventory.json').read_text())
listed = re.findall(r'^(\S+): test$', (collected / 'inventory.log').read_text(), re.M)
assert sorted(listed) == expected and len(listed) == len(set(listed)) == 33
passed = re.findall(r'^test (\S+) \.\.\. ok$', (collected / 'affected.log').read_text(), re.M)
assert sorted(passed) == expected and len(passed) == len(set(passed)) == 33
assert re.search(r'test result: ok\. 33 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;', (collected / 'affected.log').read_text())
commands = {'inventory': 'cargo test --locked -p borsuk --example compare_native_replay -- --list', 'affected': 'cargo test --locked -p borsuk --example compare_native_replay -- --test-threads=1', 'release': 'cargo build --release --locked -p borsuk --example compare_native_replay', 'clippy': 'cargo clippy --locked --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious', 'test-build': 'env -u BORSUK_TEST_BUILD_COMMAND BORSUK_TEST_BUILD_JOBS=1 bash scripts/check_rust_test_build.sh'}
for stage, command in commands.items():
    zero(stage + '.native-exit')
    zero(stage + '.tee-exit')
    assert (collected / (stage + '.command')).read_text().strip() == command, stage
    assert 0 < int((collected / (stage + '.timeout-seconds')).read_text()) <= 7200
    start = datetime.datetime.fromisoformat((collected / (stage + '.started')).read_text().strip().replace('Z', '+00:00'))
    end = datetime.datetime.fromisoformat((collected / (stage + '.finished')).read_text().strip().replace('Z', '+00:00'))
    assert end >= start
    assert 'Exit status: 0' in (collected / (stage + '.time')).read_text()
zero('qualification-exit')
assert terminal['qualification_exit'] == 0
for field, expected_value in {'cpu.max': '200000 100000', 'memory.max': '8589934592', 'memory.swap.max': '0', 'pids.max': '512'}.items():
    assert (collected / (field + '.before')).read_text().strip() == expected_value
def events(name):
    return {k: int(v) for k, v in (line.split() for line in (collected / name).read_text().splitlines())}
for name in ['memory.events.after', 'global-memory.events.after']:
    ev = events(name)
    assert ev['oom'] == ev['oom_kill'] == 0, (name, ev)
assert int((collected / 'memory.swap.peak.after').read_text()) == 0
assert int((collected / 'global-memory.swap.peak.after').read_text()) == 0
assert int((collected / 'global-pids.current.after').read_text()) == 0
binary = (root / 'compare_native_replay').read_bytes()
assert binary[:4] == b'\x7fELF' and len(binary) == int((collected / 'reducer.binary.bytes').read_text())
binary_sha = hashlib.sha256(binary).hexdigest()
assert binary_sha == (collected / 'reducer.binary.sha256').read_text().split()[0]
zero('reducer.binary-upload-exit')
assert (collected / 'replay-cli-smoke.exit').read_text().strip() == '2'
assert not (collected / 'replay-cli-smoke.stdout').read_bytes()
assert 'usage: compare_native_replay --source-utilization CONFIG CONFIG_SHA256 NEW_REPORT_JSON' in (collected / 'replay-cli-smoke.stderr').read_text()
for field, expected_value in {'cpu.max': '100000 100000', 'memory.max': '268435456', 'memory.swap.max': '0', 'pids.max': '128'}.items():
    assert (collected / ('replay-' + field + '.before')).read_text().strip() == expected_value
ev = events('replay-memory.events.after')
assert ev['oom'] == ev['oom_kill'] == 0
assert int((collected / 'replay-memory.swap.peak.after').read_text()) == 0
zero('replay-original-exit')
zero('replay.native-exit')
zero('replay.tee-exit')
zero('final-exit')
assert terminal['original_exit'] == terminal['exit'] == terminal['replay_exit'] == 0
config = json.loads((root / 'replay-config.json').read_text())
assert (collected / 'replay-config.json').read_bytes() == (root / 'replay-config.json').read_bytes()
report = json.loads((collected / 'source-utilization.json').read_text())
assert (collected / 'source-utilization.json').stat().st_size <= 2 * 1024 * 1024
assert report['schema'] == 'borsuk-source-utilization-evidence-v1' and report['status'] == 'MEASURED' and report['complete'] is True and report['count'] == 1000
assert report['input'] == config['input'] and report['identity'] == config['expected_identity'] and report['bound_inputs'] == config['expected_bound_inputs']
assert report['config_sha256'] == hashlib.sha256((root / 'replay-config.json').read_bytes()).hexdigest()
assert report['config_bytes'] == (root / 'replay-config.json').stat().st_size
for field in ['producer_source_commit', 'producer_source_archive_sha256', 'record_bytes', 'source_get_cap', 'source_byte_cap', 'source_unit_cap', 'direct_sq8_get_cap', 'historical_sq8_query_byte_cap']:
    assert report[field] == config[field], field
assert report['reducer_source_sha256'] == native['crates/borsuk/examples/compare_native_replay.rs']
assert report['shared_cover_source_sha256'] == native['crates/borsuk/src/budgeted_page_rank.rs']
for field, expected_value in {'optimistic_hindsight': True, 'production_change': False, 'claims_quality': False, 'claims_latency': False, 'counterfactual_cost_only': True, 'score_or_recall_evaluated': False, 'local_file_only': True, 'producer_provenance_verified': False, 'root_historical_producer_and_limits_admission_required': True, 'corpus_queries_truth_bodies_opened': False, 'native_ann_called': False, 'qualified': False}.items():
    assert report[field] is expected_value, field
queries = report['queries']
assert len(queries) == 1000 and [q['ordinal'] for q in queries] == list(range(1000))
assert report['aggregate']['baseline_bytes']['total'] == 8971573248
assert report['aggregate']['baseline_gets']['total'] == 27343
assert report['aggregate']['original_source_sq8_bytes']['total'] == 25671329664
assert report['aggregate']['original_source_sq8_gets']['total'] == 51943
assert report['direct_closure_sq8_exceeds_historical_byte_cap_queries'] == sum(q['direct_closure_sq8_exceeds_historical_byte_cap'] for q in queries)
assert report['completion_limited_queries'] == sum(q['completion_limited'] for q in queries)
assert report['completion_limit_reached_queries'] == sum(q['completion_limit_reached'] for q in queries)
for q in queries:
    assert 0 < q['baseline_gets'] <= 128 and 0 < q['ideal_gets'] <= 128 and 0 < q['direct_closure_sq8_gets'] <= 32
    assert 0 <= q['potential_saving_bytes'] == q['baseline_bytes'] - q['ideal_bytes']
    assert q['completion_limited'] or q['ideal_bytes'] == q['baseline_bytes']
    assert q['direct_closure_sq8_exceeds_historical_byte_cap'] == (q['direct_closure_sq8_bytes'] > 16773120)
    assert q['direct_minus_original_bytes'] == q['direct_closure_sq8_bytes'] - q['original_source_sq8_bytes']
    assert q['direct_minus_original_gets'] == q['direct_closure_sq8_gets'] - q['original_source_sq8_gets']
    assert q['scored_units'] == q['walked_units'] + q['completion_units']
    assert q['baseline_bytes'] == q['closure_payload_bytes'] + q['baseline_bridge_bytes']
    assert q['scored_payload_bytes'] == q['walked_payload_bytes'] + q['completion_payload_bytes']
    assert q['ideal_bytes'] == q['scored_payload_bytes'] + q['ideal_bridge_bytes']
    assert q['direct_closure_sq8_bytes'] == q['mandatory_closure_sq8_bytes'] + q['direct_closure_sq8_bridge_bytes']
for key, aggregate in report['aggregate'].items():
    values = sorted(q[key] for q in queries)
    assert aggregate == {'total': sum(values), 'median': values[499], 'p95': values[949], 'max': values[-1]}, key
result = {'status': 'NATIVE_QUALIFIED_AND_CLOSED_METADATA_COST_REPORT_VERIFIED', 'instance_id': instance, 'terminated_and_waited': True, 'candidate': terminal['source_commit'], 'full_native_file_count': len(native), 'native_identity': terminal['native_source_identity_sha256'], 'full_source_before_after_match': True, 'test_count': len(passed), 'stages': list(commands), 'binary': {'bytes': len(binary), 'sha256': binary_sha}, 'replay_exit': terminal['replay_exit'], 'producer_provenance': 'root original closed authority and frozen config, not self-claimed by report', 'performance_claim': False, 'quality_claim': False}
(root / 'independent-verification.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
