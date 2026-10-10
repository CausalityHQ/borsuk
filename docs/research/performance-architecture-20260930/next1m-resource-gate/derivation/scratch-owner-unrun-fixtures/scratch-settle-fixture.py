"""STATIC, UNRUN, no-network fixture for the scratch attachment settle step of launch-one-canary.py.

Source-bound: `settle` is extracted from the committed launcher with ast and executed in a namespace holding only `json`; the launcher itself (its PENDING guard,
AWS calls, files) is never imported or run. Time is a deterministic fake clock. usage: settle-fixture.py LAUNCHER_PY      exit 0 = every case matched
"""
import ast, json, pathlib, re, subprocess, sys, tempfile, types

path = pathlib.Path(sys.argv[1])
text = path.read_text()
fn = [n for n in ast.parse(text).body if isinstance(n, ast.FunctionDef) and n.name in ('settle', 'aws')]
assert sorted(n.name for n in fn) == ['aws', 'settle']
settle_src = [n for n in fn if n.name == 'settle']; aws_src = [n for n in fn if n.name == 'aws']
ns = {'json': json}
exec(compile(ast.Module(body=settle_src, type_ignores=[]), str(path), 'exec'), ns)
settle = ns['settle']
VOL, INST = 'vol-0c649e190901dd065', 'i-001370d203c1b4f2e'
fails = []
def check(name, ok):
    print(('PASS ' if ok else 'FAIL ') + name)
    if not ok: fails.append(name)

def body(state='attached', att=None, vol=None, drop=None):
    a = {'DeleteOnTermination': True, 'VolumeId': VOL, 'InstanceId': INST, 'Device': '/dev/sdf', 'State': state, 'AttachTime': '2026-10-10T00:38:05+00:00'}
    a.update(att or {})
    v = {'VolumeId': VOL, 'State': 'in-use', 'Attachments': [a]}
    v.update(vol or {})
    if drop: v.pop(drop)
    return json.dumps({'Volumes': [v]}).encode()

class World:
    def __init__(self, script, call_seconds=1.0, extra=None):
        self.t, self.script, self.calls, self.lefts, self.sleeps, self.call_seconds, self.extra = 0.0, list(script), 0, [], [], call_seconds, (extra or {})
    def clock(self): return self.t
    def sleep(self, s): self.sleeps.append(s); self.t += s
    def observe(self, n, left):
        self.calls += 1; self.lefts.append(left)
        assert n == self.calls                      # observations are numbered 1,2,3... so every raw body gets a distinct name
        self.t += min(self.call_seconds, left) + self.extra.get(self.calls, 0.0)   # a call can never outlive the limit it was given (extra{n} > 0 simulates a clamp that failed on call n)
        item = self.script[min(self.calls, len(self.script)) - 1]
        return item if isinstance(item, tuple) else (0, item)
    def run(self): return settle(self.observe, VOL, INST, self.clock, self.sleep)

w = World([body('attached')]); n, raw, doc = w.run()
check('immediate attached: first observation settles, no sleep', n == 1 and w.calls == 1 and w.sleeps == [] and raw == body('attached') and doc['Volumes'][0]['Attachments'][0]['State'] == 'attached')
w = World([body('attaching'), body('attaching'), body('attached')]); n, raw, doc = w.run()
check('attaching -> attached settles on the third observation with bounded sleeps', n == 3 and w.calls == 3 and w.sleeps == [2, 2] and raw == body('attached'))
check('every call is clamped: 3 <= limit <= 25 and decreasing', all(3 <= x <= 25 for x in w.lefts) and w.lefts == sorted(w.lefts, reverse=True))
for secs in (1.0, 5.0, 25.0):
    w = World([body('attaching')], secs)
    try:
        w.run(); check('permanent attaching (call %.0fs) never settles' % secs, False)
    except AssertionError:
        check('permanent attaching (call %.0fs) times out fail-closed' % secs, True)
    check('  ... within 12 observations, no call past 30 s, no sleep past the window (clock %.1f)' % w.t, w.calls <= 12 and w.t <= 30.0 + 1e-9 and all(0 <= s <= 2 for s in w.sleeps))
w = World([body('attaching'), body('attached')], 25.0); n, raw, doc = w.run()
check('attached exactly at the 30 s boundary is still accepted (elapsed %.3f)' % w.t, n == 2 and w.t == 30.0)
w = World([body('attaching'), body('attached')], 25.0, {2: 0.001})
try:
    w.run(); check('attached that arrives after the window (30.001 s)', False)
except AssertionError:
    check('late attached (call finished after 30 s) is refused, never a success (elapsed %.3f)' % w.t, w.calls == 2 and w.t > 30.0)
w = World([body('attached')], 31.0, {1: 40.0})
try:
    w.run(); check('first call that overruns the window', False)
except AssertionError:
    check('immediate attached that overran the window is refused', w.calls == 1 and w.t > 30.0)
def immediate(name, item):
    w = World([item])
    try:
        w.run(); check(name + ' is refused', False)
    except (AssertionError, KeyError, ValueError, TypeError):
        check(name + ' is refused immediately (one observation, no sleep, no retry)', w.calls == 1 and w.sleeps == [])
immediate('AWS error status', (255, b''))
immediate('AWS error with a valid-looking body', (254, body('attached')))
immediate('empty body', b'')
immediate('oversize body', b'{' + b' ' * 65536 + b'}')
immediate('non-bytes body', (0, 'text'))
immediate('malformed JSON', b'{"Volumes": [')
immediate('Volumes missing', b'{}')
immediate('no volumes', b'{"Volumes": []}')
immediate('two volumes', json.dumps({'Volumes': [json.loads(body())['Volumes'][0]] * 2}).encode())
immediate('foreign volume (top level)', body(vol={'VolumeId': 'vol-0000000000000000a'}))
immediate('foreign volume (attachment)', body(att={'VolumeId': 'vol-0000000000000000a'}))
immediate('foreign instance', body(att={'InstanceId': 'i-0123456789abcdef0'}))
immediate('wrong device', body(att={'Device': '/dev/sdg'}))
immediate('attachment state detaching', body('detaching'))
immediate('attachment state detached', body('detached'))
immediate('attachment state busy', body('busy'))
immediate('attachment state missing', body(att={'State': None}))
immediate('attachments missing', body(drop='Attachments'))
immediate('no attachment', body(vol={'Attachments': []}))
immediate('two attachments', body(vol={'Attachments': [json.loads(body())['Volumes'][0]['Attachments'][0]] * 2}))
immediate('attached with a foreign instance', body('attached', att={'InstanceId': 'i-0123456789abcdef0'}))
w = World([body('attaching'), (255, b'')])
try:
    w.run(); check('AWS error after a transient attaching', False)
except AssertionError:
    check('AWS error after a transient attaching: no retry (exactly two observations)', w.calls == 2)

# aws(): a timed-out OBSERVATION keeps its capped partial output and an explicit TIMEOUT outcome under the same name, then propagates; other calls are unchanged
def timeout_world(keep):
    with tempfile.TemporaryDirectory() as d:
        seen = {}
        def run(cmd, **kw):
            seen['timeout'] = kw['timeout']; raise subprocess.TimeoutExpired(cmd, kw['timeout'], output=b'o' * 70000, stderr=b'e' * 10)
        ans = {'json': json, 'subprocess': types.SimpleNamespace(PIPE=subprocess.PIPE, TimeoutExpired=subprocess.TimeoutExpired, run=run), 'root': pathlib.Path(d), 'env': {}}
        exec(compile(ast.Module(body=aws_src, type_ignores=[]), str(path), 'exec'), ans)
        try:
            ans['aws']('describe-scratch-volume-02', ['ec2', 'describe-volumes'], 7, keep) if keep is not None else ans['aws']('plain', ['ec2'], 7)
            raised = False
        except subprocess.TimeoutExpired:
            raised = True
        return raised, seen, {p.name: p.read_bytes() for p in pathlib.Path(d).iterdir()}
raised, seen, files = timeout_world(True)
check('timed-out observation propagates (no retry) with the clamped timeout', raised and seen == {'timeout': 7})
check('  ... stdout capped at 64 KiB, stderr kept, explicit TIMEOUT outcome, command recorded',
      len(files['describe-scratch-volume-02.stdout']) == 65536 and files['describe-scratch-volume-02.stderr'] == b'e' * 10 and files['describe-scratch-volume-02.exit'] == b'TIMEOUT\n'
      and b'"--cli-read-timeout", "7"' in files['describe-scratch-volume-02.command.json'])
raised, seen, files = timeout_world(None)
check('other calls keep the original behavior: propagate and leave only the command file', raised and list(files) == ['plain.command.json'])

# source properties of the launcher (text only)
check('observations get distinct names', "'describe-scratch-volume-%02d'%i" in text and "checked('describe-scratch-volume'" not in text)
check('the final basename is read back and equals the settled body', "assert (root/final_evidence).read_bytes()==raw_volumes" in text)
check('the binding hash is of the settled final body', "'describe_volumes_sha256':hashlib.sha256(raw_volumes).hexdigest()" in text)
check('the final evidence basename is exposed for the verifier pin', "'scratch_volume_final_evidence':final_evidence" in text)
check('per-call time is clamped', "timeout=min(25,limit)" in text and "str(max(1,min(15,int(limit))))" in text)
check('one RunInstances, one claim, one cleanup', text.count("checked('run-instances'") == 1 and text.count("open('x')") == 1 and text.count("emergency-terminate") == 1)
check('no binding before the settle and the unchanged proof', text.index('settle(lambda') < text.index('scratch_proof(a[') < text.index("(root/'scratch-launch-binding.json').write_bytes"))
check('deadline is checked after the call and again before success', 'rc,body=observe(n,min(25,left));assert clock()-start<=window' in text and "assert clock()-start<=window   # and re-checked" in text)
check('only the observation passes keep=True', text.count(',left,True)') == 1 and 'def aws(name,args,limit=25,keep=False):' in text)
check('sleep appears only inside settle and as its argument', text.count('sleep(') == 1 and text.count('time.sleep') == 1)
print('cases failed: %d' % len(fails))
sys.exit(1 if fails else 0)
