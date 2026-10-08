import hashlib
import json
from pathlib import Path
import shlex

prior = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-gate-a0004')
root = Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-qualification-a0003')
bundle = json.loads((root / 'bundle.json').read_text())
contract = json.loads((root / 'candidate-contract.json').read_text())
candidate = contract['commit']
assert candidate == bundle['candidate'] == 'fd2cb50ee5856a5c43131ca23d38c7be8c8bc73b'
prefix = 'research/semantic-router/20261008/cold-two-bit-four-row-qualification-a0003'
stages = [
    ('runner-debug', ['cargo', 'test', '--locked', '-p', 'borsuk', '--bin', 'check_cohere_native_baseline', '--', '--test-threads=1']),
    ('planner-debug', ['cargo', 'test', '--locked', '-p', 'borsuk', '--lib', 'two_bit_generation::source_walk_tests::', '--', '--test-threads=1']),
    ('codec-debug', ['cargo', 'test', '--locked', '-p', 'borsuk', '--lib', 'rotated_two_bit::tests::', '--', '--test-threads=1']),
    ('probe-compile', ['cargo', 'test', '--locked', '--release', '-p', 'borsuk', '--lib', '--no-run', '--message-format=json']),
    ('codec-release', ['cargo', 'test', '--locked', '--release', '-p', 'borsuk', '--lib', 'rotated_two_bit::tests::', '--', '--test-threads=1']),
    ('planner-release', ['cargo', 'test', '--locked', '--release', '-p', 'borsuk', '--lib', 'two_bit_generation::source_walk_tests::', '--', '--test-threads=1']),
    ('runner-release', ['cargo', 'test', '--locked', '--release', '-p', 'borsuk', '--bin', 'check_cohere_native_baseline', '--', '--test-threads=1']),
    ('integration', ['cargo', 'test', '--locked', '-p', 'borsuk', '--test', 'rotated_two_bit', '--test', 'two_bit_source', '--test', 'two_bit_generation', '--', '--test-threads=1']),
    ('release', ['cargo', 'build', '--locked', '--release', '-p', 'borsuk', '--bin', 'check_cohere_native_baseline', '--example', 'two_bit_http']),
    ('control-release', ['env', 'CARGO_TARGET_DIR=/mnt/borsuk-http/source/target', 'cargo', 'build', '--locked', '--release', '--manifest-path', '/mnt/borsuk-http/control-source/Cargo.toml', '-p', 'borsuk', '--bin', 'check_cohere_native_baseline', '--example', 'two_bit_http']),
    ('clippy', ['cargo', 'clippy', '--locked', '--workspace', '--all-targets', '--', '-D', 'clippy::correctness', '-D', 'clippy::suspicious']),
    ('test-build', ['env', '-u', 'BORSUK_TEST_BUILD_COMMAND', 'BORSUK_TEST_BUILD_JOBS=1', 'CARGO_BUILD_JOBS=1', 'bash', 'scripts/check_rust_test_build.sh']),
    ('scalar-control-release', ['env', 'CARGO_TARGET_DIR=/mnt/borsuk-http/target-scalar-control', 'cargo', 'test', '--locked', '--release', '--features', 'scalar-control', '-p', 'borsuk', '--lib', '--message-format=json', 'rotated_two_bit::tests::', '--', '--test-threads=1']),
]
codec = [name for name in contract['mandatory_qualified_tests_debug_and_release'] if name.startswith('rotated_two_bit::')]
planner = [name for name in contract['mandatory_qualified_tests_debug_and_release'] if name.startswith('two_bit_generation::')]
runner = contract['mandatory_runner_qualified_tests_debug_and_release']
assert (len(codec), len(planner), len(runner)) == (4, 8, 3)
required = {name: list(tests) for name, tests in (
    ('runner-debug', runner), ('planner-debug', planner), ('codec-debug', codec),
    ('codec-release', codec), ('planner-release', planner), ('runner-release', runner),
    ('scalar-control-release', codec))}
(root / 'mandatory-tests.json').write_text(json.dumps(required, indent=2) + '\n')
selector = (prior / 'select-probe.py').read_bytes()
(root / 'select-probe.py').write_bytes(selector)
original = (prior / 'gates.sh').read_text()
before, body = original.split('test "$(uname -m)" = x86_64', 1)
header = 'test "$(uname -m)" = x86_64' + body.split('set +e\n(\n', 1)[0]
footer = original.split(')\nstatus=$?\n', 1)[1]
before = before.replace('unset BORSUK_TEST_BUILD_COMMAND', 'unset BORSUK_TEST_BUILD_COMMAND RUSTFLAGS CARGO_ENCODED_RUSTFLAGS')
control_setup = '''
control=/mnt/borsuk-http/control-source
test ! -e "$control"
mkdir "$control"
tar -xzf /mnt/borsuk-http/source.tar.gz -C "$control"
cp /mnt/borsuk-http/control-rotated_two_bit.rs "$control/crates/borsuk/src/rotated_two_bit.rs"
cp /mnt/borsuk-http/control-two_bit_generation.rs "$control/crates/borsuk/src/two_bit_generation.rs"
(cd "$control" && sha256sum --check /mnt/borsuk-http/control-source-files.sha256) > "$evidence/control-source-before.log"
mkdir /mnt/borsuk-http/retained
rustc -vV > "$evidence/rustc-verbose.txt"
for dir in /mnt/borsuk-http/source /mnt/borsuk-http/control-source; do
  sha256sum "$dir/Cargo.toml" "$dir/Cargo.lock"
done > "$evidence/matched-profile-inputs.sha256"
# Both compiler invocations share the explicit empty Rust flags and toolchain.
test ! -e /mnt/.cargo/config && test ! -e /mnt/.cargo/config.toml
test ! -e /mnt/borsuk-http/.cargo/config && test ! -e /mnt/borsuk-http/.cargo/config.toml
test ! -e "$CARGO_HOME/config" && test ! -e "$CARGO_HOME/config.toml"
test ! -e /root/.cargo/config && test ! -e /root/.cargo/config.toml
for dir in /mnt/borsuk-http/source /mnt/borsuk-http/control-source; do
  test ! -e "$dir/.cargo/config" && test ! -e "$dir/.cargo/config.toml"
done
printf '%s\\n' 'no ancestor/source/CARGO_HOME/root Cargo configuration overrides' > "$evidence/cargo-config-admission.txt"
'''
calls = []
for name, argv in stages:
    calls.append(' stage ' + shlex.join([name, *argv]))
    if name == 'probe-compile':
        calls.extend([' python3 /mnt/borsuk-http/select-probe.py',
                      ' sha256sum "$(cat "$evidence/probe-executable.txt")" > "$evidence/probe-elf-before-tests.sha256"'])
    if name == 'release':
        calls.extend([
            ' cp /mnt/borsuk-http/source/target/release/check_cohere_native_baseline /mnt/borsuk-http/retained/candidate-check_cohere_native_baseline',
            ' cp /mnt/borsuk-http/source/target/release/examples/two_bit_http /mnt/borsuk-http/retained/candidate-two_bit_http',
            ' sha256sum /mnt/borsuk-http/retained/candidate-* > "$evidence/candidate-serving-before-control.sha256"'])
    if name == 'control-release':
        calls.extend([
            ' cp /mnt/borsuk-http/source/target/release/check_cohere_native_baseline /mnt/borsuk-http/retained/control-check_cohere_native_baseline',
            ' cp /mnt/borsuk-http/source/target/release/examples/two_bit_http /mnt/borsuk-http/retained/control-two_bit_http',
            ' (cd /mnt/borsuk-http/control-source && sha256sum --check /mnt/borsuk-http/control-source-files.sha256) > "$evidence/control-source-after.log"',
            ' sha256sum --check "$evidence/candidate-serving-before-control.sha256" > "$evidence/candidate-serving-after-control.log"'])
    if name == 'scalar-control-release':
        calls.extend([
            ' python3 /mnt/borsuk-http/select-scalar-control.py',
            ' sha256sum --check "$evidence/probe-elf-before-tests.sha256" > "$evidence/probe-elf-after-tests.log"'])
gates = before + header + control_setup + '\nset +e\n(\n set -e\n' + '\n'.join(calls) + '\n)\nstatus=$?\n' + footer
(root / 'gates.sh').write_text(gates)
scalar_selector = '''import json, pathlib, shutil
root = pathlib.Path('/mnt/borsuk-http/evidence')
items = []
for line in (root / 'scalar-control-release.log').read_text().splitlines():
    try: item = json.loads(line)
    except ValueError: continue
    if (isinstance(item, dict) and item.get('reason') == 'compiler-artifact'
        and item.get('target', {}).get('name') == 'borsuk'
        and item['target'].get('kind') == ['lib'] and item.get('profile', {}).get('test') is True
        and 'scalar-control' in item.get('features', []) and item.get('executable')):
        items.append(item)
assert len(items) == 1, 'scalar-control release libtest missing/ambiguous'
source = pathlib.Path(items[0]['executable'])
assert source.is_file() and source.parent == pathlib.Path('/mnt/borsuk-http/target-scalar-control/release/deps')
shutil.copyfile(source, '/mnt/borsuk-http/retained/scalar-control-libtest')
(root / 'scalar-control-artifact.json').write_text(json.dumps(items[0], indent=2) + '\\n')
'''
(root / 'select-scalar-control.py').write_text(scalar_selector)
def pin(path):
    body = path.read_bytes()
    return {'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
names = ['source-files.sha256', 'native-source.json', 'mandatory-tests.json', 'gates.sh', 'select-probe.py',
         'control-source-files.sha256', 'control-native-source.json', 'control-rotated_two_bit.rs',
         'control-two_bit_generation.rs', 'select-scalar-control.py']
inputs = {name: pin(root / name) for name in names}
userdata = (prior / 'user-data.sh').read_text()
userdata = userdata.replace('cold-two-bit-four-row-compile-a0004', 'cold-two-bit-four-row-qualification-a0003')
userdata = userdata.replace('3138bfb0502c986d187e4098718f8b3945fd39c9', candidate)
old_bundle = json.loads((prior / 'protocol.json').read_text())['source']
userdata = userdata.replace(old_bundle['archive_sha256'], bundle['archive_sha256'])
userdata = userdata.replace(old_bundle['native_identity'], bundle['native_identity'])
for name in ['source-files.sha256', 'native-source.json', 'mandatory-tests.json', 'gates.sh', 'select-probe.py']:
    old = pin(prior / name)
    userdata = userdata.replace(old['sha256'], inputs[name]['sha256'])
userdata = userdata.replace('test "$(jq length mandatory-tests.json)" = 5', 'test "$(jq length mandatory-tests.json)" = 7')
extra = ''
for name in names[5:]:
    extra += f'aws s3 cp "s3://$bucket/$prefix/inputs/{name}" {name} --only-show-errors\n'
    extra += f"printf '%s  {name}\\n' {inputs[name]['sha256']} | sha256sum -c -\n"
    extra += f'cp {name} evidence/\n'
userdata = userdata.replace('touch FROZEN_FOUR_ROW_SOURCE_AND_ROSTER', extra + 'touch FROZEN_FOUR_ROW_SOURCE_AND_ROSTER')
userdata = userdata.replace('borsuk-two-bit-four-row-compile-v1', 'borsuk-two-bit-four-row-native-qualification-v1')
userdata = userdata.replace('native_source_file_count:415,',
    'control_native_identity_sha256:"' + bundle['control']['native_identity'] + '",control_library_source:"' + bundle['control']['library_source'] + '",native_source_file_count:415,')
userdata = userdata.replace('compile and bounded synthetic correctness only; primitive NOT_RUN; full qualification pending',
                            'exact native tests and full compiler gates plus matched-profile serving artifacts; production stack and primitive/cold timing pending')
userdata = userdata.replace('for role in probe-libtest; do',
                            'for role in probe-libtest candidate-check_cohere_native_baseline candidate-two_bit_http control-check_cohere_native_baseline control-two_bit_http scalar-control-libtest; do')
old_case = '''    case "$role" in
      two_bit_http) binary="source/target/release/examples/$role" ;;
      probe-libtest) binary=$(cat evidence/probe-executable.txt 2>/dev/null) ;;
      *) binary="source/target/release/$role" ;;
    esac'''
new_case = '''    if [[ "$role" == probe-libtest ]]; then
      binary=$(cat evidence/probe-executable.txt 2>/dev/null)
    else
      binary="retained/$role"
    fi'''
assert old_case in userdata
userdata = userdata.replace(old_case, new_case)
userdata = userdata.replace('status=$original\n', 'status=$original\n  if [[ ! -f evidence/final-exit ]]; then status=97; fi\n')
(root / 'user-data.sh').write_text(userdata)
protocol = {
    'status': 'PROSPECTIVE_SOURCE_SEALED_NATIVE_UNRUN_NOT_LAUNCHABLE',
    'candidate': candidate, 'prefix': prefix, 'source': bundle, 'inputs': inputs,
    'userdata': pin(root / 'user-data.sh'), 'machine_seconds': 9000, 'build_seconds': 7200,
    'build_memory_bytes': 8 * 1024**3, 'build_cpu': 2, 'build_jobs': 1, 'swap': 0, 'pids': 512,
    'compute_reservation_usd': 1.50, 'ancillary_reservation_usd': .15, 'ebs_gib': 80,
    'spot_bid_max_usd_per_hour': .50, 'serial_stages': [{'name': name, 'argv': argv} for name, argv in stages],
    'retained_roles': ['probe-libtest', 'candidate-check_cohere_native_baseline', 'candidate-two_bit_http',
                       'control-check_cohere_native_baseline', 'control-two_bit_http', 'scalar-control-libtest'],
    'matched_profile': 'candidate and c49-library control share identical repaired runner, all other bytes, default release profile, explicit empty Rust flags, toolchain, dependencies and captured Cargo configuration admission; separate immutable copied serving artifacts',
    'scratch': {'codec': 532480, 'batch': 512, 'trace': 'TwoBitPlanTrace::scratch_bytes(rows)'},
    'previous_review': 'f9b98d02852a459f; unchanged SIMD kernel/dependencies; root additionally reviewed 04944 narrow runner/cfg(test) delta',
    'primitive_run': False, 'cold_run': False, 'native_ANN_corpus_query_truth': False,
    'source_admission': 'PENDING', 'staging_canary': 'PENDING', 'paid_launch_authorized': False,
    'production_stack_qualified': False, 'performance_claim': False,
    'interruption': 'execution INVALID, preserve terminal, terminate/wait, no automatic replacement',
}
(root / 'protocol.json').write_text(json.dumps(protocol, indent=2) + '\n')
print(json.dumps({'status': protocol['status'], 'candidate': candidate, 'stages': len(stages),
                  'inputs': len(inputs), 'native_count': bundle['native_file_count'], 'control_identity': bundle['control']['native_identity']}, indent=2))
