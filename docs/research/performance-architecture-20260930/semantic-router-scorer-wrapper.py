import hashlib, json, os, subprocess, sys, time
from pathlib import Path

if sys.flags.optimize:
    raise SystemExit('optimized Python disables admission checks')

def admit(c):
    assert int(c['memory.max']) == 536870912 and int(c['memory.swap.max']) == 0
    assert int(c['pids.max']) == 32 and c['cpu_affinity'] == [0, 1]
    quota, period = map(int, c['cpu.max'].split())
    assert quota == 2 * period

def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1048576), b''):
            h.update(block)
    return h.hexdigest()

if sys.argv[1:] == ['--self-check']:
    good = {'memory.max': '536870912', 'memory.swap.max': '0', 'pids.max': '32',
            'cpu_affinity': [0, 1], 'cpu.max': '200000 100000'}
    admit(good)
    for key, value in [('memory.max', '268435456'), ('memory.swap.max', '1'),
                       ('pids.max', '64'), ('cpu_affinity', [0, 1, 2, 3]),
                       ('cpu.max', '100000 100000')]:
        try:
            admit(good | {key: value})
        except AssertionError:
            continue
        raise AssertionError('incorrect resource contract admitted')
    print('PASS resource admission and five rejection checks; no scorer execution')
    raise SystemExit(0)

binary, binary_sha, config, config_sha, destination = sys.argv[1:]
out = Path(destination)
out.mkdir(exist_ok=False)
group = Path('/sys/fs/cgroup') / Path('/proc/self/cgroup').read_text().split('0::', 1)[1].strip().lstrip('/')
keys = ['memory.max', 'memory.peak', 'memory.swap.max', 'memory.swap.peak',
        'memory.events', 'memory.swap.events', 'cpu.max', 'cpu.stat', 'pids.max']
def counters():
    return {k: (group / k).read_text() for k in keys} | {'cpu_affinity': sorted(os.sched_getaffinity(0))}

before = counters()
admit(before)
assert digest(binary) == binary_sha and digest(config) == config_sha
(out / 'before.json').write_text(json.dumps(before, indent=2) + '\n')
command = ['/usr/bin/time', '-v', '-o', str(out / 'time.txt'), binary, config, config_sha, str(out / 'result.json')]
start = time.monotonic_ns()
with (out / 'stdout.log').open('xb') as stdout, (out / 'stderr.log').open('xb') as stderr:
    result = subprocess.run(command, stdout=stdout, stderr=stderr, check=False)
elapsed = time.monotonic_ns() - start
after = counters()
identities_unchanged = digest(binary) == binary_sha and digest(config) == config_sha
(out / 'terminal.json').write_text(json.dumps(dict(command=command, binary_sha256=binary_sha,
    config_sha256=config_sha, exit=result.returncode, wall_ns=elapsed, before=before, after=after,
    identities_unchanged=identities_unchanged,
    scope='bounded local paired scorer and separate exhaustive SQ8 reference; no cold HTTP or physical S3 measurement'), indent=2) + '\n')
assert identities_unchanged and int(after['memory.swap.peak']) == 0
sys.exit(result.returncode)
