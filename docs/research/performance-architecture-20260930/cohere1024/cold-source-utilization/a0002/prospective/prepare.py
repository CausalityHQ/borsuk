"""Freeze-ready files for one exact native qualification/metadata replay."""
import hashlib
import json
from pathlib import Path
import re
import subprocess

root = Path('/data/target/borsuk-cold-membership-native/source-utilization-a0002')
repo = Path('/home/rb/worktrees/borsuk-prod-ready-v9')
bundle = json.loads((root / 'bundle.json').read_text())
candidate = bundle['candidate']
contract = json.loads((root / 'candidate-contract.json').read_text())
assert candidate == '44e116b9b8754cef0393fad1f96259bd583f3b7b' == contract['commit']
owned = contract['owned_file']
def source(commit, path):
    return subprocess.check_output(['/usr/bin/git', 'show', commit + ':' + path], cwd=repo)
body = source(candidate, owned)
assert hashlib.sha256(body).hexdigest() == contract['file_sha256']
def tests(body):
    text = body.decode()
    return {m.group(1): text[m.start():text.index('\n    }', m.end()) + len('\n    }')]
            for m in re.finditer(r'    #\[test\]\n    fn (\w+)\([^\n]*\) \{', text)}
old_tests, new_tests = tests(source(contract['exact_base'], owned)), tests(body)
assert old_tests and all(new_tests.get(name) == text for name, text in old_tests.items())
actual_new = set(new_tests) - set(old_tests)
assert actual_new == {n.rsplit('::', 1)[1] for n in contract['new_tests']} and len(actual_new) == 10
assert source(candidate, contract['shared_cover_file']) == source(contract['exact_base'], contract['shared_cover_file'])
assert subprocess.check_output(['/usr/bin/git', 'diff', '--name-only', contract['exact_base'], candidate], cwd=repo, text=True).splitlines() == [owned]
subprocess.run(['/usr/bin/git', 'diff', '--check', contract['exact_base'], candidate], cwd=repo, check=True)
mandatory = ['tests::' + name for name in sorted(actual_new)]
(root / 'mandatory-tests.json').write_text(json.dumps(mandatory, indent=2) + '\n')
cover_tests = tests(source(candidate, contract['shared_cover_file']))
assert len(cover_tests) == 10
full_inventory = sorted(['tests::' + name for name in new_tests] + ['source_cover::tests::' + name for name in cover_tests])
assert len(full_inventory) == 33 and len(set(full_inventory)) == 33
(root / 'expected-test-inventory.json').write_text(json.dumps(full_inventory, indent=2) + '\n')
original = Path('/data/target/borsuk-cold-membership-native/membership-abba-a0001/collected/B1.jsonl')
input_body = original.read_bytes()
assert len(input_body) == 10777291 and hashlib.sha256(input_body).hexdigest() == '29da105428503267e73e9e23e1a61bef9967ccdc9fb4f43ef64b23d306ba0cbb'
(root / 'B1.jsonl').write_bytes(input_body)
config_body = Path('/tmp/borsuk-source-utilization-B1-config-draft-v2.json').read_bytes()
config = json.loads(config_body)
assert set(config) == set(contract['config_example'])
assert config['input'] == {'path': '/mnt/borsuk-http/inputs/B1.jsonl', 'bytes': len(input_body), 'sha256': hashlib.sha256(input_body).hexdigest()}
assert config['historical_sq8_query_byte_cap'] == 16773120
assert config['direct_sq8_get_cap'] == 32 and config['source_unit_cap'] == 2544
(root / 'replay-config.json').write_bytes(config_body)
for name, path in [('gates.sh', '/tmp/borsuk-source-utilization-gates-draft.sh'), ('replay.sh', '/tmp/borsuk-source-utilization-replay-draft.sh')]:
    (root / name).write_bytes(Path(path).read_bytes())
remote = {'source-files.sha256': 'source-files.sha256', 'native-source.json': 'native-source.json', 'mandatory-tests.json': 'mandatory-tests.json', 'expected-test-inventory.json': 'expected-test-inventory.json', 'candidate-contract.json': 'candidate-contract.json', 'gates.sh': 'gates.sh', 'replay.sh': 'replay.sh', 'replay-config.json': 'replay-config.json', 'B1.jsonl': 'inputs/B1.jsonl'}
replay_files = ['B1.jsonl', 'replay-config.json']
(root / 'replay-inputs.sha256').write_text(''.join(hashlib.sha256((root / n).read_bytes()).hexdigest() + '  /mnt/borsuk-http/' + remote[n] + '\n' for n in replay_files))
remote['replay-inputs.sha256'] = 'replay-inputs.sha256'
pins = {name: {'bytes': (root / name).stat().st_size, 'sha256': hashlib.sha256((root / name).read_bytes()).hexdigest(), 'remote_path': dest} for name, dest in remote.items()}
downloads = []
for name, pin in pins.items():
    dest = pin['remote_path']
    downloads.extend([f'aws s3 cp "s3://$bucket/$prefix/inputs/{name}" "{dest}" --only-show-errors', f'printf \'%s  {dest}\\n\' {pin["sha256"]} | sha256sum -c -', f'test "$(stat -c %s "{dest}")" = {pin["bytes"]}'])
downloads.extend(['cp source-files.sha256 native-source.json mandatory-tests.json expected-test-inventory.json candidate-contract.json replay-config.json replay-inputs.sha256 evidence/', '(cd source && sha256sum --check ../source-files.sha256) > evidence/source-staging.log'])
userdata = Path('/tmp/borsuk-source-utilization-user-data-draft-a0002.sh').read_text()
for token, value in {'__SOURCE_COMMIT__': candidate, '__NATIVE_COUNT__': str(bundle['native_file_count']), '__NATIVE_IDENTITY__': bundle['native_identity'], '__ARCHIVE_SHA__': bundle['archive_sha256'], '__PINNED_INPUT_DOWNLOADS__': '\n'.join(downloads)}.items():
    assert token in userdata
    userdata = userdata.replace(token, value)
assert '__' not in userdata
(root / 'user-data.sh').write_text(userdata)
subprocess.run(['bash', '-n', str(root / 'user-data.sh'), str(root / 'gates.sh'), str(root / 'replay.sh')], check=True)
receipt = {'status': 'STATIC_SOURCE_AND_INPUT_PINS_CHECKED_NOT_LAUNCHED', 'candidate': candidate, 'old_test_count': len(old_tests), 'new_test_count': len(actual_new), 'inherited_cover_tests': 'actual remote inventory required', 'source_identity': bundle['native_identity'], 'native_file_count': bundle['native_file_count'], 'inputs': pins, 'userdata': {'bytes': (root / 'user-data.sh').stat().st_size, 'sha256': hashlib.sha256((root / 'user-data.sh').read_bytes()).hexdigest()}, 'native_compilation': 'UNRUN', 'metadata_replay': 'UNRUN', 'performance_claim': False}
(root / 'static-admission.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({k: v for k, v in receipt.items() if k != 'inputs'}))
