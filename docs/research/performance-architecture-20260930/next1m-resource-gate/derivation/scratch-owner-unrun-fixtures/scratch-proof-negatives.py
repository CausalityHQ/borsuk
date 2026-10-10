"""STATIC, UNRUN fixture: negatives for the scratch ownership proof, the binding-v2 schema check and the verifier's raw-request/launch-time/cmp-record helpers.

Source-bound: `epoch` and `scratch_proof` are extracted from the committed launch-one-canary.py with ast and executed in an empty namespace (the launcher
itself is never imported or run, so its PENDING guard and AWS calls stay untouched); the jq program is cut out of validate-scratch-binding.sh and run by jq on
synthetic bindings. No AWS, no network, no device.   usage: scratch-proof-negatives.py LAUNCHER_PY VALIDATE_SH VERIFY_PY      exit 0 = every case matched
"""
import ast, base64, copy, datetime, hashlib, json, pathlib, re, subprocess, sys

launcher, validate, verifier = (pathlib.Path(p) for p in sys.argv[1:4])
tree = ast.parse(launcher.read_text())
wanted = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name in ('epoch', 'scratch_proof')]
assert [n.name for n in wanted] == ['epoch', 'scratch_proof']
ns = {'datetime': datetime}
exec(compile(ast.Module(body=wanted, type_ignores=[]), str(launcher), 'exec'), ns)
proof = ns['scratch_proof']

INSTANCE, ZONE, STARTED, NOW = 'i-0a42ebce98bb09cd9', 'eu-central-1c', 1791590459, 1791590470
SCRATCH, ROOT = 'vol-0c4f0ad58d2ba29b9', 'vol-09d65657769cead73'
def good():
    info = {'InstanceId': INSTANCE, 'LaunchTime': '2026-10-10T00:01:46+00:00', 'Placement': {'AvailabilityZone': ZONE},
            'BlockDeviceMappings': [{'DeviceName': '/dev/sda1', 'Ebs': {'VolumeId': ROOT, 'DeleteOnTermination': True, 'Status': 'attached'}},
                                    {'DeviceName': '/dev/sdf', 'Ebs': {'VolumeId': SCRATCH, 'DeleteOnTermination': True, 'Status': 'attached'}}]}
    vol = {'Volumes': [{'VolumeId': SCRATCH, 'SnapshotId': '', 'Size': 40, 'VolumeType': 'gp3', 'Encrypted': True, 'MultiAttachEnabled': False, 'AvailabilityZone': ZONE,
                        'State': 'in-use', 'CreateTime': '2026-10-10T00:01:46.123000+00:00',
                        'Attachments': [{'InstanceId': INSTANCE, 'Device': '/dev/sdf', 'State': 'attached', 'VolumeId': SCRATCH, 'AttachTime': '2026-10-10T00:01:46.585000+00:00'}]}]}
    return info, vol
launched = int(datetime.datetime.fromisoformat('2026-10-10T00:01:46+00:00').timestamp())
STARTED = launched - 20; NOW = launched + 10
fails = []
def check(name, ok):
    print(('PASS ' if ok else 'FAIL ') + name)
    if not ok: fails.append(name)
def refused(name, mutate, now=None, started=None):
    info, vol = good(); mutate(info, vol)
    try:
        proof(STARTED if started is None else started, ZONE, INSTANCE, info, vol, NOW if now is None else now)
        check(name + ' is refused', False)
    except (AssertionError, KeyError, ValueError, TypeError):
        check(name + ' is refused', True)

info, vol = good()
got = proof(STARTED, ZONE, INSTANCE, info, vol, NOW)
check('valid proof accepted with the exact facts', got == {'scratch': SCRATCH, 'root': ROOT, 'launched': launched, 'created': launched, 'attached': launched})
v = lambda vol: vol['Volumes'][0]
refused('missing SnapshotId field', lambda i, o: v(o).pop('SnapshotId'))
refused('non-empty SnapshotId', lambda i, o: v(o).__setitem__('SnapshotId', 'snap-0123456789abcdef0'))
refused('SnapshotId null', lambda i, o: v(o).__setitem__('SnapshotId', None))
refused('stale launch (older than the request window)', lambda i, o: None, started=launched + 600)
refused('launch in the future', lambda i, o: None, now=launched - 300)
refused('launch later than the finite launcher window', lambda i, o: None, started=launched - 600)
refused('timezone-naive LaunchTime', lambda i, o: i.__setitem__('LaunchTime', '2026-10-10T00:01:46'))
refused('timezone-naive CreateTime', lambda i, o: v(o).__setitem__('CreateTime', '2026-10-10T00:01:46'))
refused('timezone-naive AttachTime', lambda i, o: v(o)['Attachments'][0].__setitem__('AttachTime', '2026-10-10T00:01:46.5'))
refused('malformed timestamp', lambda i, o: v(o).__setitem__('CreateTime', 'yesterday'))
refused('nonpositive timestamp', lambda i, o: v(o).__setitem__('CreateTime', '1970-01-01T00:00:00+00:00'))
refused('numeric timestamp', lambda i, o: v(o).__setitem__('CreateTime', 1791590506))
refused('volume created long before the launch', lambda i, o: v(o).__setitem__('CreateTime', '2026-10-09T00:01:46+00:00'))
refused('attached before it was created', lambda i, o: v(o)['Attachments'][0].__setitem__('AttachTime', '2026-10-10T00:01:00+00:00'))
refused('string DeleteOnTermination "true" (scratch)', lambda i, o: i['BlockDeviceMappings'][1]['Ebs'].__setitem__('DeleteOnTermination', 'true'))
refused('string DeleteOnTermination "false" (root)', lambda i, o: i['BlockDeviceMappings'][0]['Ebs'].__setitem__('DeleteOnTermination', 'false'))
refused('DeleteOnTermination 1', lambda i, o: i['BlockDeviceMappings'][1]['Ebs'].__setitem__('DeleteOnTermination', 1))
refused('string Encrypted', lambda i, o: v(o).__setitem__('Encrypted', 'true'))
refused('Encrypted false', lambda i, o: v(o).__setitem__('Encrypted', False))
refused('MultiAttachEnabled missing', lambda i, o: v(o).pop('MultiAttachEnabled'))
refused('MultiAttachEnabled true', lambda i, o: v(o).__setitem__('MultiAttachEnabled', True))
refused('Size float 40.0', lambda i, o: v(o).__setitem__('Size', 40.0))
refused('Size 41', lambda i, o: v(o).__setitem__('Size', 41))
refused('wrong volume type', lambda i, o: v(o).__setitem__('VolumeType', 'io2'))
refused('state available', lambda i, o: v(o).__setitem__('State', 'available'))
refused('wrong zone', lambda i, o: v(o).__setitem__('AvailabilityZone', 'eu-central-1a'))
refused('two attachments', lambda i, o: v(o)['Attachments'].append(copy.deepcopy(v(o)['Attachments'][0])))
refused('attached to another instance', lambda i, o: v(o)['Attachments'][0].__setitem__('InstanceId', 'i-0123456789abcdef0'))
refused('wrong device', lambda i, o: v(o)['Attachments'][0].__setitem__('Device', '/dev/sdg'))
refused('two volumes in the response', lambda i, o: o['Volumes'].append(copy.deepcopy(v(o))))
refused('volume id differs from the mapping', lambda i, o: v(o).__setitem__('VolumeId', 'vol-0000000000000000a'))
refused('scratch equals root volume', lambda i, o: i['BlockDeviceMappings'][0]['Ebs'].__setitem__('VolumeId', SCRATCH))
refused('third mapping', lambda i, o: i['BlockDeviceMappings'].append({'DeviceName': '/dev/sdg', 'Ebs': {'VolumeId': 'vol-0000000000000000b', 'DeleteOnTermination': True}}))

# binding-v2 schema program (the jq text is cut out of the committed support asset)
text = validate.read_text()
prog = re.search(r"prog <<'EOF' \|\| true\n(.*?)\nEOF\n", text, re.S).group(1)
binding = {'schema': 'borsuk-scratch-launch-binding-v2', 'instance_id': INSTANCE, 'volume_id': SCRATCH, 'root_volume_id': ROOT, 'device': '/dev/sdf', 'size_bytes': 42949672960,
           'availability_zone': ZONE, 'volume_type': 'gp3', 'encrypted': True, 'multi_attach': False, 'snapshot_empty': True, 'state': 'in-use', 'attached_device': '/dev/sdf',
           'delete_on_termination': True, 'create_time_epoch': launched, 'attach_time_epoch': launched, 'launch_time_epoch': launched,
           'describe_instances_sha256': '0' * 64, 'describe_volumes_sha256': 'f' * 64}
def jq(b, i=INSTANCE, az=ZONE):
    r = subprocess.run(['jq', '-ers', '--arg', 'i', i, '--arg', 'az', az, prog], input=json.dumps(b), capture_output=True, text=True)
    return r.returncode, r.stdout.strip()
check('binding v2 accepted, prints "<scratch> <root>"', jq(binding) == (0, SCRATCH + ' ' + ROOT))
def bad(name, mutate, **kw):
    b = copy.deepcopy(binding); mutate(b)
    check(name + ' is refused', jq(b, **kw)[0] != 0)
bad('binding with a missing key', lambda b: b.pop('snapshot_empty'))
bad('binding with an extra key', lambda b: b.__setitem__('extra', 1))
bad('snapshot_empty "true" string', lambda b: b.__setitem__('snapshot_empty', 'true'))
bad('snapshot_empty false', lambda b: b.__setitem__('snapshot_empty', False))
bad('delete_on_termination string', lambda b: b.__setitem__('delete_on_termination', 'true'))
bad('multi_attach true', lambda b: b.__setitem__('multi_attach', True))
bad('encrypted string', lambda b: b.__setitem__('encrypted', 'true'))
bad('wrong schema version', lambda b: b.__setitem__('schema', 'borsuk-scratch-launch-binding-v1'))
bad('other instance', lambda b: None, i='i-0123456789abcdef0')
bad('other zone', lambda b: None, az='eu-central-1a')
bad('same scratch and root volume', lambda b: b.__setitem__('root_volume_id', SCRATCH))
bad('bad volume id', lambda b: b.__setitem__('volume_id', 'vol-XYZ'))
bad('stale create time', lambda b: b.__setitem__('create_time_epoch', launched - 3600))
bad('string time', lambda b: b.__setitem__('launch_time_epoch', str(launched)))
bad('nonpositive time', lambda b: b.__setitem__('attach_time_epoch', 0))
bad('fractional time', lambda b: b.__setitem__('create_time_epoch', launched + 0.5))
bad('short describe hash', lambda b: b.__setitem__('describe_volumes_sha256', 'abc'))
bad('size 1', lambda b: b.__setitem__('size_bytes', 1))

# verifier helpers, extracted from the committed verify-closed.py (never imported or run as a program)
vtree = ast.parse(verifier.read_text())
names = ('Invalid', 'req', 'sha', 'raw_user_data_ok', 'launch_times_agree', 'scratch_cmp')
vnodes = [n for n in vtree.body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
assert sorted(n.name for n in vnodes) == sorted(names)
vns = {'hashlib': hashlib, 're': re}
exec(compile(ast.Module(body=vnodes, type_ignores=[]), str(verifier), 'exec'), vns)
script = '#!/usr/bin/env bash\necho scratch\n'
want = hashlib.sha256(script.encode()).hexdigest()
check('raw request user data accepted', vns['raw_user_data_ok']({'UserData': script}, want) is True)
check('base64 text presented as the raw request is refused', vns['raw_user_data_ok']({'UserData': base64.b64encode(script.encode()).decode()}, want) is False)
check('non-string user data is refused', vns['raw_user_data_ok']({'UserData': script.encode()}, want) is False)
agree = vns['launch_times_agree']
check('launch epochs equal everywhere', agree(100, 100, 100) is True)
check('RunInstances within the 2 s skew', agree(100, 102, 100) is True)
check('RunInstances 10 s away is refused', agree(100, 110, 100) is False)
check('binding differs from DescribeInstances (matches only RunInstances) is refused', agree(110, 110, 100) is False)
def cmp_refused(text):
    try:
        vns['scratch_cmp'](text); return False
    except vns['Invalid']:
        return True
check('cmp rc 0 with no output accepted', vns['scratch_cmp']('\nrc=0\n') == {'rc': 0, 'output': ''})
check('cmp rc 1 with the exact difference line accepted', vns['scratch_cmp']('/dev/nvme1n1 /dev/zero differ: byte 4097, line 1\nrc=1\n')['rc'] == 1)
check('cmp rc 2 is refused', cmp_refused('cmp: read error\nrc=2\n'))
check('cmp rc 1 with other text is refused', cmp_refused('something else\nrc=1\n'))
check('cmp rc 0 with output is refused', cmp_refused('x\nrc=0\n'))
check('cmp record without a status line is refused', cmp_refused('/dev/nvme1n1 /dev/zero differ: byte 4097, line 1\n'))
print('cases failed: %d' % len(fails))
sys.exit(1 if fails else 0)
