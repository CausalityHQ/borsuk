"""STATIC, UNRUN, no-network fixture for the DescribeInstances mapping observation (settle_instance) of launch-one-canary.py.

Source-bound: `settle_instance` is extracted from the committed launcher with ast and executed in a namespace holding only json and re (the launcher is never
imported or run); the functions the repair must NOT touch (aws, checked, epoch, scratch_proof, settle) are compared with the parent launcher by ast.dump.
Time is a deterministic fake clock.   usage: instance-fixture.py LAUNCHER_PY PARENT_LAUNCHER_PY      exit 0 = every case matched
"""
import ast, json, pathlib, re, sys

path, parent = (pathlib.Path(p) for p in sys.argv[1:3])
text, ptext = path.read_text(), parent.read_text()
def funcs(src): return {n.name: n for n in ast.parse(src).body if isinstance(n, ast.FunctionDef)}
new, old = funcs(text), funcs(ptext)
ns = {'json': json, 're': re}
exec(compile(ast.Module(body=[new['settle_instance']], type_ignores=[]), str(path), 'exec'), ns)
settle_instance = ns['settle_instance']
INST, TOKEN = 'i-03da6cc4fdcadb76c', 'borsuk-next1m-canary-a0006-1791594191'
ROOT, SCR = 'vol-033c44b8f1746d7ad', 'vol-0f221bce1e3b88a31'
fails = []
def check(name, ok):
    print(('PASS ' if ok else 'FAIL ') + name)
    if not ok: fails.append(name)
def mapping(dev, vol): return {'DeviceName': dev, 'Ebs': {'AttachTime': '2026-10-10T01:03:44+00:00', 'DeleteOnTermination': True, 'Status': 'attaching', 'VolumeId': vol, 'EbsCardIndex': 0}}
BOTH = [mapping('/dev/sda1', ROOT), mapping('/dev/sdf', SCR)]
ONLY_SDF = [mapping('/dev/sdf', SCR)]
def body(maps, state='pending', iid=INST, token=TOKEN, drop=None, reservations=1, instances=1):
    i = {'InstanceId': iid, 'ClientToken': token, 'LaunchTime': '2026-10-10T01:03:44+00:00', 'State': {'Name': state}, 'BlockDeviceMappings': maps}
    if drop: i.pop(drop)
    return json.dumps({'Reservations': [{'Instances': [i] * instances}] * reservations}).encode()

class World:
    def __init__(self, script, call_seconds=1.0, extra=None):
        self.t, self.script, self.calls, self.lefts, self.sleeps, self.call_seconds, self.extra = 0.0, list(script), 0, [], [], call_seconds, (extra or {})
    def clock(self): return self.t
    def sleep(self, s): self.sleeps.append(s); self.t += s
    def observe(self, n, left):
        self.calls += 1; self.lefts.append(left)
        assert n == self.calls                        # numbered 1,2,3...: every raw body gets a distinct name
        self.t += min(self.call_seconds, left) + self.extra.get(self.calls, 0.0)
        item = self.script[min(self.calls, len(self.script)) - 1]
        return item if isinstance(item, tuple) else (0, item)
    def run(self): return settle_instance(self.observe, INST, TOKEN, self.clock, self.sleep)

w = World([body(BOTH)]); n, raw, doc = w.run()
check('both mappings at the first observation: settles at once, no sleep', n == 1 and w.calls == 1 and w.sleeps == [] and raw == body(BOTH))
w = World([body(ONLY_SDF), body(BOTH)]); n, raw, doc = w.run()
check('pending with only /dev/sdf, then both: settles on the second observation after one bounded sleep (the a0006 shape)', n == 2 and w.sleeps == [2] and raw == body(BOTH))
w = World([body([]), body(ONLY_SDF), body([mapping('/dev/sda1', ROOT)]), body(BOTH, 'running')]); n, raw, doc = w.run()
check('empty, one, the other one, then both (running): settles on the fourth observation', n == 4 and w.sleeps == [2, 2, 2])
check('every call is clamped: 3 <= limit <= 25, non-increasing', all(3 <= x <= 25 for x in w.lefts) and w.lefts == sorted(w.lefts, reverse=True))
for secs in (1.0, 5.0, 25.0):
    w = World([body(ONLY_SDF)], secs)
    try:
        w.run(); check('permanently missing mapping (call %.0fs) never settles' % secs, False)
    except AssertionError:
        check('permanently missing mapping (call %.0fs) times out fail-closed' % secs, True)
    check('  ... <= 12 observations and the clock never passes 30 s (%.1f)' % w.t, w.calls <= 12 and w.t <= 30.0 + 1e-9 and all(0 <= s <= 2 for s in w.sleeps))
w = World([body(ONLY_SDF), body(BOTH)], 25.0); n, raw, doc = w.run()
check('complete exactly at 30.000 s is accepted (%.3f)' % w.t, n == 2 and w.t == 30.0)
w = World([body(ONLY_SDF), body(BOTH)], 25.0, {2: 0.001})
try:
    w.run(); check('complete mappings arriving at 30.001 s', False)
except AssertionError:
    check('complete mappings that arrive after the window are refused (%.3f)' % w.t, w.calls == 2 and w.t > 30.0)
w = World([body(BOTH)], 31.0, {1: 40.0})
try:
    w.run(); check('first call that overruns the window', False)
except AssertionError:
    check('an overrunning first call is refused even though the body is complete', w.calls == 1 and w.t > 30.0)

def immediate(name, item):
    w = World([item])
    try:
        w.run(); check(name + ' is refused', False)
    except (AssertionError, KeyError, ValueError, TypeError):
        check(name + ' is refused immediately (one observation, no sleep, no retry)', w.calls == 1 and w.sleeps == [])
immediate('AWS error status', (255, b''))
immediate('AWS error with a valid-looking body', (254, body(BOTH)))
immediate('empty body', b'')
immediate('oversize body', b'{' + b' ' * 65536 + b'}')
immediate('non-bytes body', (0, 'text'))
immediate('malformed JSON', b'{"Reservations": [')
immediate('Reservations missing', b'{}')
immediate('no reservation', b'{"Reservations": []}')
immediate('two reservations', body(BOTH, reservations=2))
immediate('two instances', body(BOTH, instances=2))
immediate('foreign instance id', body(BOTH, iid='i-0123456789abcdef0'))
immediate('foreign client token', body(BOTH, token='borsuk-next1m-canary-a0007-1'))
immediate('client token missing', body(BOTH, drop='ClientToken'))
immediate('mappings field missing', body(BOTH, drop='BlockDeviceMappings'))
immediate('foreign device /dev/sdg', body([mapping('/dev/sdg', SCR)]))
immediate('foreign device next to a valid one', body([mapping('/dev/sda1', ROOT), mapping('/dev/sdg', SCR)]))
immediate('duplicate /dev/sdf', body([mapping('/dev/sdf', SCR), mapping('/dev/sdf', ROOT)]))
immediate('duplicate /dev/sda1 plus /dev/sdf', body([mapping('/dev/sda1', ROOT), mapping('/dev/sda1', ROOT), mapping('/dev/sdf', SCR)]))
immediate('three mappings', body(BOTH + [mapping('/dev/sdg', 'vol-0000000000000000a')]))
immediate('non-string device name', body([{'DeviceName': 7, 'Ebs': {'VolumeId': SCR}}]))
immediate('Ebs missing', body([{'DeviceName': '/dev/sdf'}]))
immediate('Ebs not an object', body([{'DeviceName': '/dev/sdf', 'Ebs': 'x'}]))
immediate('malformed volume id', body([mapping('/dev/sdf', 'volume-1')]))
immediate('volume id missing', body([{'DeviceName': '/dev/sdf', 'Ebs': {}}]))
immediate('incomplete mappings on a RUNNING instance (not transient)', body(ONLY_SDF, 'running'))
immediate('terminated instance with both mappings', body(BOTH, 'terminated'))
immediate('shutting-down instance with both mappings', body(BOTH, 'shutting-down'))
immediate('stopped instance with both mappings', body(BOTH, 'stopped'))
immediate('state missing', body(BOTH, drop='State'))
w = World([body(ONLY_SDF), (255, b'')])
try:
    w.run(); check('AWS error after a transient incomplete observation', False)
except AssertionError:
    check('AWS error after a transient incomplete observation: no retry (exactly two observations)', w.calls == 2)

# the repair must not touch the settle/proof/guest-facing code: ast equality with the parent launcher
for name in ('aws', 'checked', 'epoch', 'scratch_proof', 'settle'):
    check('%s() is unchanged against the parent launcher' % name, ast.dump(new[name]) == ast.dump(old[name]))
# source properties of the launcher (text only)
check('observations get distinct names, the old single describe is gone', "'describe-original-%02d'%i" in text and "checked('describe-original'" not in text and "'describe-original.stdout'" not in text)
check('the final basename is read back and equals the settled body', "assert (root/instance_evidence).read_bytes()==raw_instances" in text)
check('the binding hash is of the settled final instance body', "'describe_instances_sha256':hashlib.sha256(raw_instances).hexdigest()" in text)
check('the final instance evidence basename is exposed explicitly', "'instance_final_evidence':instance_evidence" in text)
check('observation calls pass keep=True (timeout evidence)', text.count(",left,True)") == 2)
check('one RunInstances, one claim, one cleanup', text.count("checked('run-instances'") == 1 and text.count("open('x')") == 1 and text.count('emergency-terminate') == 1)
check('order: instance settle < mapping asserts < scratch settle < proof < binding write', text.index('settle_instance(lambda') < text.index("mappings=info['BlockDeviceMappings'];assert len(mappings)==2") < text.index('settle(lambda') < text.index('scratch_proof(a[') < text.index("(root/'scratch-launch-binding.json').write_bytes"))
check('sleep only inside the two settle functions and as their arguments', text.count('sleep(') == 2 and text.count('time.sleep') == 2)
print('cases failed: %d' % len(fails))
sys.exit(1 if fails else 0)
