"""Synthetic checks for this exact experiment's shell execution paths only."""
import os
from pathlib import Path
import subprocess
import tempfile

with tempfile.TemporaryDirectory(prefix='borsuk-source-utilization-shell-') as temp:
    root = Path(temp)
    (root / 'evidence').mkdir()
    (root / 'retained').mkdir()
    (root / 'bin').mkdir()
    (root / 'replay-config.json').write_text('{}\n')
    fake = root / 'retained/compare_native_replay'
    fake.write_text('''#!/bin/bash
set -euo pipefail
printf '%s\\n' "$#" >> "$MOCK_ROOT/calls"
[[ "$1" == --source-utilization ]]
if [[ $# == 1 ]]; then
  printf '%s\\n' 'INVALID: usage: compare_native_replay --source-utilization CONFIG CONFIG_SHA256 NEW_REPORT_JSON; external gates required' >&2
  exit "${MOCK_SMOKE_EXIT:-2}"
fi
[[ $# == 4 && "$2" == "$MOCK_ROOT/replay-config.json" && "$4" == "$MOCK_ROOT/evidence/source-utilization.json" ]]
[[ "$3" == "$(sha256sum "$2" | cut -d' ' -f1)" ]]
printf '%s\\n' '{"synthetic":true}' > "$4"
exit "${MOCK_NATIVE_EXIT:-0}"
''')
    fake.chmod(0o755)
    cat = root / 'bin/cat'
    cat.write_text('''#!/bin/bash
set -euo pipefail
case "$1" in
  /sys/fs/cgroup*/cpu.max) echo "${MOCK_CPU_MAX:-100000 100000}";;
  /sys/fs/cgroup*/memory.max) echo 268435456;;
  /sys/fs/cgroup*/memory.swap.max) echo 0;;
  /sys/fs/cgroup*/pids.max) echo 128;;
  *) exec /bin/cat "$@";;
esac
''')
    cat.chmod(0o755)
    script = (root / 'replay.sh')
    original = Path('/tmp/borsuk-source-utilization-replay-draft.sh').read_text()
    script.write_text(original.replace('root=/mnt/borsuk-http', 'root=' + str(root), 1))
    env = dict(os.environ, MOCK_ROOT=str(root), PATH=str(root / 'bin') + ':' + os.environ['PATH'])
    def run(overrides, expected, calls):
        (root / 'calls').unlink(missing_ok=True)
        (root / 'evidence/source-utilization.json').unlink(missing_ok=True)
        q = subprocess.run(['/bin/bash', str(script)], env=dict(env, **overrides), capture_output=True, text=True, timeout=10)
        assert q.returncode == expected, (q.returncode, q.stderr)
        observed = (root / 'calls').read_text().splitlines() if (root / 'calls').exists() else []
        assert observed == calls, observed
    run({}, 0, ['1', '4'])
    assert (root / 'evidence/replay-original-exit').read_text().strip() == '0'
    run({'MOCK_SMOKE_EXIT': '0'}, 1, ['1'])
    run({'MOCK_NATIVE_EXIT': '2'}, 2, ['1', '4'])
    assert (root / 'evidence/replay-original-exit').read_text().strip() == '2'
    run({'MOCK_CPU_MAX': '200000 100000'}, 1, [])
    # Exercise the exact stage function, independent of Cargo and EC2.
    gates = Path('/tmp/borsuk-source-utilization-gates-draft.sh').read_text()
    function = gates[gates.index('stage() {'):gates.index('\nqualification_deadline=', gates.index('stage() {'))]
    harness = root / 'stage.sh'
    harness.write_text('set -euo pipefail\nevidence="$MOCK_ROOT/evidence"\nqualification_deadline=$((SECONDS+10))\n' + function + '\nstage synthetic /bin/bash -c "echo native; exit ${MOCK_STAGE_EXIT:-0}"\necho advanced > "$MOCK_ROOT/advanced"\n')
    q = subprocess.run(['/bin/bash', str(harness)], env=env, capture_output=True, text=True, timeout=15)
    assert q.returncode == 0 and (root / 'advanced').exists()
    (root / 'advanced').unlink()
    q = subprocess.run(['/bin/bash', str(harness)], env=dict(env, MOCK_STAGE_EXIT='17'), capture_output=True, text=True, timeout=15)
    assert q.returncode != 0 and not (root / 'advanced').exists()
    assert (root / 'evidence/synthetic.native-exit').read_text().strip() == '17'
    tee = root / 'bin/tee'
    tee.write_text('#!/bin/bash\n/bin/cat >/dev/null\nexit 18\n')
    tee.chmod(0o755)
    q = subprocess.run(['/bin/bash', str(harness)], env=env, capture_output=True, text=True, timeout=15)
    assert q.returncode != 0 and not (root / 'advanced').exists()
    assert (root / 'evidence/synthetic.native-exit').read_text().strip() == '0'
    assert (root / 'evidence/synthetic.tee-exit').read_text().strip() == '18'
print('PASS: synthetic replay success, CLI refusal, original native exit, resource drift, stage success/native17/tee18; real cgroup/EC2/native compilation/ANN UNRUN')
