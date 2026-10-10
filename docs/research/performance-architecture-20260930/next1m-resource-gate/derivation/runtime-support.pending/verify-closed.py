#!/usr/bin/env python3
"""Root replay of ONE closed native 1M/Q32 build-gate run: mechanics and source binding only.

CLI: verify-closed.py REMOTE_RESULTS ROOT_PINS   (both capped metadata; every pin is root-supplied, there are no defaults)

Adapted from the root's closed preparation verifier (qualified historical preparation only). It never decodes vectors, truth,
hits or the opaque baseline result: those are only hashed and length-checked. A retained baseline exit 2/3 is carried verbatim as
a closed nonzero disposition, never as a scientific PASS. Exit 0 means the evidence replays mechanically; 1 = INVALID evidence;
2 = verifier defect. Native schemas below are the frozen Rust bc3082a8 (unchanged to a02c233d) blobs: build_sq8_source.rs derive
receipt v2, two_bit_generation.rs Manifest/Discovery (borsuk-two-bit-generation-v8), two_bit_build.rs page manifest,
two_bit_source.rs SourcePlaneReceipt, publish_two_bit_generation.rs receipt v1, check_cohere_native_baseline.rs config v7.
"""
import calendar, datetime, decimal, hashlib, json, re, struct, sys, tarfile
from pathlib import Path

ROOT = '/mnt/borsuk-scale1m'
SCRATCH = ROOT + '/prepared-parent'
CHAIN_EVIDENCE = ROOT + '/evidence-chain'
ASSETS = ROOT + '/assets'
GIB8 = 8589934592
ROWS, DIMS, QUERIES, K = 1000000, 1024, 32, 10
SEAL_NAMES = ('corpus.f32', 'queries.f32', 'corpus.ids.jsonl', 'queries.ids.jsonl', 'truth.u64')
RESERVED_SHA = '8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e'
DATASET = 'CohereLabs/wikipedia-2023-11-embed-multilingual-v3'
REVISION = 'ade45fb52bd549f5e8c065636fe4160a43c2af36'
INTERVALS = [{'start': 0, 'end': 100000}, {'start': 101000, 'end': 1001000}]
RESERVED_INTERVAL = {'start': 100000, 'end': 101000}
SUPPORT_NAMES = ('config-template.json', 'derivation-config.json', 'gate-config-template.json', 'run_actual_cohort_admission.sh',
                 'run_native_scale_build_gate.sh', 'observer-command.sh', 'collect-native-chain-outer.sh', 'run-native-chain-observer.sh', 'service-stop.sh', 'transport-pins.json', 'transport.py', 'validate-scratch-binding.sh')
PHASES = ('derive', 'stage', 'generation', 'publish', 'baseline')
PHASE_MAX = {'derive': 3600, 'stage': 300, 'generation': 2700, 'publish': 1800, 'baseline': 900}
EXIT_FILES = ('native', 'timeout', 'time', 'tee', 'time-log', 'supervisor-stderr-log', 'native-stderr-log')
MAX_ARCHIVE, MAX_RAW, MAX_MEMBER, MAX_HEADERS, KEEP = 268435456, 268435456, 67108864, 4096, 1048576
NAME_RE = re.compile(r'[A-Za-z0-9._@+=,-]+(?:/[A-Za-z0-9._@+=,-]+)*')
TOP = frozenset('''support.sha256 bootstrap.log bootstrap.exit environment.txt cloud-final-unit.txt scratch-before.txt scratch-after.txt
disk-admission.txt disk-root-after.txt disk-scratch-after-prep.txt deadline-admission.txt transport-unit.exit parity-unit.exit chain-unit.exit
transport-exit.json service-exit.json chain-launch.exit chain-launch.stdout chain-launch.stderr chain-launch.identity chain-observer.unit chain-actual.exit systemd-after-cohort-parity.txt systemd-after-native-chain.txt
prepared-parent_cohort_complete.json prepared-parent_cohort_truth.u64 prepared-parent_derived_derivation.json
prepared-parent_generation_manifest.json prepared-parent_generation_page_manifest.json prepared-parent_generation_plane_manifest.json
prepared-parent_publication-receipt.json finalized-config.json gate-config-template.json gate-config.json chain-argv.json
scratch-launch-binding.json scratch-launch-binding.json.sha256 scratch-cmp.txt'''.split())
OPTIONAL_TOP = frozenset(['prepared-parent_query_baseline-result.jsonl'])
BASELINE_RESULT = 'prepared-parent_query_baseline-result.jsonl'


class Invalid(ValueError):
    pass


def req(ok, why):
    if not ok:
        raise Invalid(why)


def sha(b):
    return hashlib.sha256(b).hexdigest()


def ie(v, n):  # bool-as-int safe numeric equality
    return type(v) is int and v == n


def isint(v):
    return type(v) is int


def hexs(v, n=64):
    return type(v) is str and re.fullmatch('[0-9a-f]{%d}' % n, v) is not None


def uniq(pairs):
    d = {}
    for k, v in pairs:
        req(k not in d, 'duplicate JSON field ' + k)
        d[k] = v
    return d


def no_const(x):
    raise Invalid('non-finite JSON constant ' + x)


def decode(b):
    return json.loads(b.decode('utf-8'), object_pairs_hook=uniq, parse_constant=no_const)


def same(a, b):
    return json.dumps(a, sort_keys=True, separators=(',', ':')) == json.dumps(b, sort_keys=True, separators=(',', ':'))


def safe_rel(p):  # a relative path that cannot leave its base: no absolute form, no '.' or '..' component
    return type(p) is str and NAME_RE.fullmatch(p) is not None and all(c not in ('.', '..') for c in p.split('/'))


def keys(d, names, why):
    req(type(d) is dict and set(d) == set(names), why)


def bounded(p, cap):
    req(p.is_file() and not p.is_symlink(), 'metadata type ' + str(p))
    n = p.stat().st_size
    req(0 < n <= cap, 'metadata size ' + str(p))
    with p.open('rb') as f:
        b = f.read(cap + 1)
    req(len(b) == n, 'metadata growth ' + str(p))
    return b


class Ev:
    """Authenticated archive members: small bodies in memory, large ones as (length, sha) only."""
    def __init__(self, bodies, meta):
        self.bodies, self.meta = bodies, meta

    def b(self, name):
        req(name in self.bodies, 'missing evidence member ' + name)
        return self.bodies[name]

    def t(self, name):
        return self.b(name).decode('utf-8')

    def j(self, name):
        return decode(self.b(name))

    def jl(self, name):
        return [decode(line) for line in self.b(name).splitlines()]

    def has(self, name):
        return name in self.meta


# ----------------------------------------------------------------------------- archive (streaming, bounded)
class Hashing:
    """Read-only proxy that hashes and bounds the compressed archive while tarfile streams it."""
    def __init__(self, f, cap):
        self.f, self.cap, self.n, self.h = f, cap, 0, hashlib.sha256()

    def read(self, size=-1):
        b = self.f.read(size)
        self.n += len(b)
        req(self.n <= self.cap, 'archive size cap')
        self.h.update(b)
        return b


class EvidenceHeader(tarfile.TarInfo):
    headers = 0

    @classmethod
    def frombuf(cls, buf, encoding, errors):
        header = super().frombuf(buf, encoding, errors)
        cls.headers += 1
        req(cls.headers <= MAX_HEADERS, 'archive header count')
        req(header.type in (tarfile.REGTYPE, tarfile.AREGTYPE, tarfile.DIRTYPE), 'archive extended/link/sparse header refused')
        req(0 <= header.size <= MAX_MEMBER and len(header.name) <= 255, 'archive header bounds')
        return header


def parse_manifest(b):
    req(b.endswith(b'\n'), 'manifest newline')
    listed, order = {}, []
    for line in b.decode('ascii').split('\n')[:-1]:
        m = re.fullmatch(r'([0-9a-f]{64})  \./(.+)', line)
        req(m is not None, 'manifest syntax')
        rel = m.group(2)
        req(safe_rel(rel) and rel not in listed, 'manifest path/duplicate')
        listed[rel] = m.group(1)
        order.append(rel)
    req(order == sorted(order, key=lambda r: ('./' + r).encode()), 'manifest generation order')
    return listed


def stream_archive(path, pins, manifest):
    seen, bodies, total, kinds = {}, {}, 0, {}
    with path.open('rb') as raw:
        proxy = Hashing(raw, MAX_ARCHIVE)
        EvidenceHeader.headers = 0
        with tarfile.open(fileobj=proxy, mode='r|gz', tarinfo=EvidenceHeader) as t:
            for m in t:
                name = m.name
                req(name == '.' or name.startswith('./'), 'archive path root')
                rel = '' if name == '.' else name[2:]
                req(rel == '' or safe_rel(rel), 'unsafe archive path')
                req(rel not in kinds, 'duplicate archive header (file/dir/root) ' + (rel or '.'))
                kinds[rel] = 'dir' if m.isdir() else 'file'
                if m.isdir():
                    continue
                req(m.isfile() and rel != '' and rel not in seen and rel in manifest, 'archive type/duplicate/inventory')
                total += m.size
                req(total <= MAX_RAW, 'expanded evidence cap')
                f = t.extractfile(m)
                req(f is not None, 'archive stream')
                h, n, chunks = hashlib.sha256(), 0, []
                while True:
                    c = f.read(1 << 20)
                    if not c:
                        break
                    n += len(c)
                    req(n <= m.size, 'body growth')
                    h.update(c)
                    if m.size <= KEEP:
                        chunks.append(c)
                req(n == m.size and h.hexdigest() == manifest[rel], 'inventory SHA ' + rel)
                seen[rel] = (n, h.hexdigest())
                if m.size <= KEEP:
                    bodies[rel] = b''.join(chunks)
        while proxy.read(1 << 20):
            pass
        req(proxy.n == pins['archive']['bytes'] and proxy.h.hexdigest() == pins['archive']['sha256'], 'independent archive size/SHA')
    req(set(seen) == set(manifest), 'closed exact inventory')
    req(kinds.get('', 'dir') == 'dir', 'archive root entry must be a directory')
    for n in kinds:  # every ancestor of every header must be a directory (an absent ancestor is implied); a file may never be one
        parts = n.split('/')
        for i in range(1, len(parts)):
            req(kinds.get('/'.join(parts[:i]), 'dir') == 'dir', 'file used as a directory ancestor: ' + n)
    return Ev(bodies, seen)


# ----------------------------------------------------------------------------- pins and watcher metadata
def load_pins(p):
    d = decode(bounded(p, 262144))
    keys(d, ('schema', 'run_prefix', 'instance_id', 'user_data_sha256', 'terminal_sha256', 'archive', 'artifacts_manifest_sha256',
             'support', 'prep', 'frozen_seals', 'binding', 'ec2'), 'pins keys')
    req(d['schema'] == 'borsuk-native-scale-build-replay-pins-v1', 'pins schema')
    req(type(d['run_prefix']) is str and re.fullmatch(r'research/semantic-router/[0-9]{8}/[a-z0-9-]+', d['run_prefix']), 'pins run prefix')
    req(type(d['instance_id']) is str and re.fullmatch(r'i-[0-9a-f]+', d['instance_id']), 'pins instance')
    for k in ('user_data_sha256', 'terminal_sha256', 'artifacts_manifest_sha256'):
        req(hexs(d[k]), 'pins ' + k)
    keys(d['archive'], ('bytes', 'sha256'), 'pins archive')
    req(isint(d['archive']['bytes']) and 0 < d['archive']['bytes'] <= MAX_ARCHIVE and hexs(d['archive']['sha256']), 'pins archive values')
    keys(d['support'], ('manifest_sha256', 'files'), 'pins support')
    keys(d['support']['files'], SUPPORT_NAMES, 'pins support roster')
    req(hexs(d['support']['manifest_sha256']) and all(hexs(v) for v in d['support']['files'].values()), 'pins support values')
    keys(d['prep'], ('elf_sha256', 'config_template', 'transport_pins'), 'pins prep')
    req(hexs(d['prep']['elf_sha256']) and type(d['prep']['config_template']) is dict, 'pins prep values')
    tp = d['prep']['transport_pins']
    req(type(tp) is list and len(tp) == 17 and len({i['relative_path'] for i in tp}) == 17, 'pins 17 transport items')
    for i in tp:
        keys(i, ('relative_path', 'bytes', 'sha256'), 'transport item')
        req(isint(i['bytes']) and i['bytes'] > 0 and hexs(i['sha256']) and safe_rel(i['relative_path']), 'transport item values')
    keys(d['frozen_seals'], SEAL_NAMES, 'pins seals')
    for v in d['frozen_seals'].values():
        keys(v, ('bytes', 'sha256'), 'pins seal')
        req(isint(v['bytes']) and v['bytes'] > 0 and hexs(v['sha256']), 'pins seal values')
    keys(d['binding'], ('sha256', 'volume_id'), 'pins binding')
    req(hexs(d['binding']['sha256']) and re.fullmatch(r'vol-[0-9a-f]{8,17}', d['binding']['volume_id']), 'pins binding values')
    ec2 = d['ec2']
    keys(ec2, ('max_spot_price', 'launch_request', 'launch_response', 'instance_running', 'volume', 'instance_terminated'), 'pins ec2')
    req(type(ec2['max_spot_price']) is str and decimal.Decimal(ec2['max_spot_price']) <= decimal.Decimal('0.59'), 'pins spot price')
    for k in ('launch_request', 'launch_response', 'instance_running', 'volume', 'instance_terminated'):
        keys(ec2[k], ('path', 'sha256'), 'pins ec2 file')
        req(safe_rel(ec2[k]['path']) and hexs(ec2[k]['sha256']), 'pins ec2 file values')
    return d


def pinned_json(R, spec, cap=262144):
    body = bounded(R / spec['path'], cap)
    req(sha(body) == spec['sha256'], 'root EC2 evidence pin ' + spec['path'])
    return decode(body)


def parse_time(text):
    m = re.fullmatch(r'(\d{4}-\d\d-\d\d)[T ](\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:?\d\d)', text)
    req(m is not None, 'timestamp syntax ' + text)
    dt = datetime.datetime.strptime(m.group(1) + ' ' + m.group(2), '%Y-%m-%d %H:%M:%S')
    zone = m.group(4)
    off = 0 if zone == 'Z' else (1 if zone[0] == '+' else -1) * (int(zone[1:3]) * 3600 + int(zone[-2:]) * 60)
    return calendar.timegm(dt.timetuple()) - off, m.group(3) or ''


def raw_user_data_ok(q, expected_sha):
    # the launch request carries the RAW shell script string (the AWS CLI encodes it once on the wire); a base64-looking string is a different request and does not match
    return type(q['UserData']) is str and sha(q['UserData'].encode('utf-8')) == expected_sha


def launch_times_agree(binding_epoch, run_epoch, described_epoch):
    # the producer derived the binding from DescribeInstances; RunInstances must report the same launch within a finite 2 s skew
    return binding_epoch == described_epoch and abs(run_epoch - described_epoch) <= 2


def verify_ec2(R, pins, instance, binding, launch_text):
    spec = pins['ec2']
    q, r = pinned_json(R, spec['launch_request']), pinned_json(R, spec['launch_response'])
    run, vol, dead = pinned_json(R, spec['instance_running']), pinned_json(R, spec['volume']), pinned_json(R, spec['instance_terminated'])
    req(raw_user_data_ok(q, pins['user_data_sha256']), 'launch request raw user data')
    req(q['InstanceType'] == 'c7i.2xlarge' and ie(q['MinCount'], 1) and ie(q['MaxCount'], 1) and q['InstanceInitiatedShutdownBehavior'] == 'terminate', 'launch request shape')
    req(q['Placement']['AvailabilityZone'] == 'eu-central-1c' and q['MetadataOptions']['HttpTokens'] == 'required', 'launch request placement/IMDSv2')
    mo = q['InstanceMarketOptions']
    req(mo['MarketType'] == 'spot' and mo['SpotOptions']['SpotInstanceType'] == 'one-time' and mo['SpotOptions']['MaxPrice'] == spec['max_spot_price'], 'launch request one-time spot bid')
    maps = {m['DeviceName']: m['Ebs'] for m in q['BlockDeviceMappings']}
    req(set(maps) == {'/dev/sda1', '/dev/sdf'} and len(q['BlockDeviceMappings']) == 2, 'launch request device roster')
    req(ie(maps['/dev/sda1']['VolumeSize'], 80) and maps['/dev/sda1']['VolumeType'] == 'gp3' and maps['/dev/sda1']['DeleteOnTermination'] is True, 'root volume request')
    sdf = maps['/dev/sdf']
    req(ie(sdf['VolumeSize'], 40) and sdf['VolumeType'] == 'gp3' and sdf['Encrypted'] is True and sdf['DeleteOnTermination'] is True and 'SnapshotId' not in sdf, 'scratch volume request (fresh 40GiB gp3, no snapshot)')
    inst = r['Instances'][0]
    req(inst['InstanceId'] == instance and inst['InstanceType'] == 'c7i.2xlarge' and inst['Placement']['AvailabilityZone'] == 'eu-central-1c', 'launch response instance')
    running = run['Reservations'][0]['Instances'][0]
    req(running['InstanceId'] == instance, 'running description instance')
    lt = re.fullmatch(r'launch_time_raw=(\S+) launch_epoch=(\d+) supplied_started=(\d+) effective_started=(\d+) supplied_minus_launch_seconds=(-?\d+)', launch_text.strip())
    req(lt is not None, 'watcher launch-time receipt syntax')
    run_launch, described_launch, started = parse_time(inst['LaunchTime'])[0], parse_time(running['LaunchTime'])[0], int(lt.group(3))
    req(launch_times_agree(binding['launch_time_epoch'], run_launch, described_launch), 'binding launch epoch equals DescribeInstances and agrees with RunInstances')
    launch_epoch = described_launch
    run_maps = {m['DeviceName']: m['Ebs'] for m in running['BlockDeviceMappings']}
    req(set(run_maps) == {'/dev/sda1', '/dev/sdf'} and len(running['BlockDeviceMappings']) == 2, 'running mapping roster')
    # DeleteOnTermination is a DescribeInstances mapping field (not a DescribeVolumes one)
    req(run_maps['/dev/sdf']['VolumeId'] == binding['volume_id'] and run_maps['/dev/sdf']['DeleteOnTermination'] is True
        and run_maps['/dev/sda1']['VolumeId'] == binding['root_volume_id'] and run_maps['/dev/sda1']['DeleteOnTermination'] is True and binding['volume_id'] != binding['root_volume_id'], 'launch mappings equal the binding volume and root volume')
    v = vol['Volumes']
    req(len(v) == 1 and v[0]['VolumeId'] == binding['volume_id'] and ie(v[0]['Size'], 40) and v[0]['VolumeType'] == 'gp3' and v[0]['Encrypted'] is True and v[0].get('SnapshotId') == ''
        and v[0].get('MultiAttachEnabled') is False and v[0]['State'] == 'in-use' and v[0]['AvailabilityZone'] == q['Placement']['AvailabilityZone'], 'scratch volume description (fresh blank 40GiB, no snapshot, no multi-attach)')
    att = v[0]['Attachments']
    req(len(att) == 1 and att[0]['InstanceId'] == instance and att[0]['Device'] == '/dev/sdf' and att[0]['State'] == 'attached' and att[0]['VolumeId'] == binding['volume_id'], 'scratch volume attachment')
    create, attach = parse_time(v[0]['CreateTime'])[0], parse_time(att[0]['AttachTime'])[0]
    req(started - 60 <= launch_epoch <= started + 240 and launch_epoch - 60 <= create <= launch_epoch + 60 and create - 5 <= attach <= launch_epoch + 120 and create > 0, 'launch/create/attach window (same finite skew as the launcher and the guest)')
    req(binding['create_time_epoch'] == create and binding['attach_time_epoch'] == attach
        and binding['availability_zone'] == q['Placement']['AvailabilityZone'], 'binding times and zone equal the pinned API responses')
    req(binding['describe_instances_sha256'] == spec['instance_running']['sha256'] and binding['describe_volumes_sha256'] == spec['volume']['sha256'], 'binding hashes equal the pinned raw API responses')
    gone = dead['Reservations'][0]['Instances'][0]
    req(gone['InstanceId'] == instance and gone['State']['Name'] == 'terminated', 'root EC2 evidence: original instance terminated')
    m = re.fullmatch(r'launch_time_raw=(\S+) launch_epoch=(\d+) supplied_started=(\d+) effective_started=(\d+) supplied_minus_launch_seconds=(-?\d+)', launch_text.strip())
    req(m is not None, 'watcher launch-time receipt syntax')
    epoch = parse_time(inst['LaunchTime'])[0]
    req(int(m.group(2)) == epoch == parse_time(m.group(1))[0], 'EC2 LaunchTime equals watcher-authenticated epoch')
    req(int(m.group(4)) == min(int(m.group(3)), epoch) and int(m.group(5)) == int(m.group(3)) - epoch, 'effective start is the earlier bound')
    return {'launch_time_epoch': epoch, 'effective_started_epoch': int(m.group(4)), 'supplied_started_epoch': int(m.group(3))}


# ----------------------------------------------------------------------------- shared evidence checks
def counters(value):
    if value == 'absent':
        return {}
    pairs = [line.split() for line in value.splitlines()]
    req(all(len(p) == 2 and p[1].isdigit() for p in pairs), 'counter syntax')
    req(len({p[0] for p in pairs}) == len(pairs), 'duplicate counters')
    return {k: int(v) for k, v in pairs}


def check_resources(ev, P, labels, unit, memory=GIB8, cores=4, cpus='0-3'):
    """Exact 4CPU 0-3 / 8GiB / swap0 / pids128, identical ancestor limits, no OOM/pids change, reclaim only monotonic."""
    lim = ('path', 'cpu_max', 'cpuset_cpus_effective', 'memory_max', 'memory_swap_max', 'pids_max')
    base, prev, prev_label = None, None, None
    for lab in labels:
        eff_body = ev.b(P + 'resources.%s.effective.json' % lab)
        eff = decode(eff_body)
        if base is None:
            base = eff_body
            req(ie(eff['memory_max_bytes'], memory) and ie(eff['swap_max_bytes'], 0) and ie(eff['pids_max'], 128) and ie(eff['cpu_quota_cores'], cores) and eff['cpuset'] == cpus, 'exact resource limits')
        else:
            req(eff_body == base, 'effective limit drift ' + lab)
        recs = ev.jl(P + 'resources.%s.jsonl' % lab)
        required_event_counters(recs)
        req(len(recs) > 1 and recs[0]['path'].endswith('/' + unit), 'ancestor chain / owned unit leaf')
        leaf = recs[0]
        req(int(leaf['memory_current']) <= memory and int(leaf['memory_peak']) <= memory and int(leaf['memory_swap_current']) == 0, 'leaf memory evidence')
        if prev is not None:
            req(len(prev) == len(recs), 'ancestor count')
            expected = []
            for a, b in zip(prev, recs):
                req(all(a[k] == b[k] for k in lim), 'ancestor limits')
                ac, bc = counters(a['memory_events']), counters(b['memory_events'])
                req(all(ac.get(k) == bc.get(k) for k in ('oom', 'oom_kill', 'oom_group_kill')), 'memory OOM event closure')
                req(bc.get('max', 0) >= ac.get('max', 0), 'reclaim counter monotonicity (not a refusal)')
                req(counters(a['pids_events']) == counters(b['pids_events']), 'pids event closure')
                expected.append({'path': a['path'], 'before': ac.get('max'), 'after': bc.get('max'), 'delta': bc.get('max', 0) - ac.get('max', 0)})
            req(same(ev.j(P + 'resource-events.%s.json' % lab), {'from': prev_label, 'to': lab, 'memory_max_events': expected, 'valid': True}), 'independent reclaim event delta')
            req(ev.b(P + 'resource-closure.%s.txt' % lab) == b'true\n', 'resource closure marker')
        prev, prev_label = recs, lab
    return decode(base)


def time_fields(text):
    def field(label):
        values = re.findall(r'^\s*' + re.escape(label) + r':\s*(\d+)\s*$', text, re.M)
        req(len(values) == 1, 'GNU time ' + label)
        return int(values[0])
    return field('Maximum resident set size (kbytes)') * 1024, field('Swaps'), field('Exit status')


def closure_inventory(ev, P, exempt):
    seen = set()
    for line in ev.t(P + 'closure.sha256').splitlines():
        m = re.fullmatch(r'([0-9a-f]{64})  \./(.+)', line)
        req(m is not None, 'closure syntax ' + P)
        req(m.group(2) not in seen, 'closure duplicate')
        seen.add(m.group(2))
        req(P + m.group(2) in ev.meta and ev.meta[P + m.group(2)][1] == m.group(1), 'closure ' + m.group(2))
    expected = {n[len(P):] for n in ev.meta if n.startswith(P)} - set(exempt)
    req(seen == expected and seen, 'complete closure inventory ' + P)


def kv_lines(text):
    out = {}
    for line in text.splitlines():
        if '=' in line:
            k, v = line.split('=', 1)
            out[k] = v
    return out


# ----------------------------------------------------------------------------- preparation (reused closure checks, OWN2 names)
def verify_prep(ev, pins):
    L = 'evidence-local/'
    local = ev.j(L + 'terminal.json')
    req(local['schema'] == 'borsuk-actual-cohort-admission-local-v1' and local['status'] == 'ADMISSION_VERIFIED' and ie(local['intended_exit'], 0)
        and local['actual_manager_and_outer_exit_required'] is True and local['performance_claim'] is False, 'prep local intended status')
    for n in ('config.validated.txt', 'receipt.validated.txt', 'resource-closure.after.txt', 'resource-closure.closed.txt'):
        req(ev.b(L + n) == b'true\n', 'prep validation marker ' + n)
    req(local['elf_sha256'] == pins['prep']['elf_sha256'], 'qualified preparer ELF binding')
    for n in ('native', 'timeout', 'time', 'tee', 'wrapper') + EXIT_FILES[4:]:
        req(ev.b(L + n + '.exit') == b'0\n', 'original prep %s status' % n)
    for n in ('native.stderr.txt', 'supervisor.stderr.txt', 'native.time.txt'):
        req(len(ev.b(L + n)) <= 1048576, 'prep log write cap ' + n)
    req(len(ev.b(L + 'native.stdout.json')) <= 1024, 'prep stdout write cap')
    req(ev.b(L + 'inputs.before.jsonl') == ev.b(L + 'inputs.after.jsonl'), 'prep whole-input closure')
    observed = {(v['path'], v['bytes'], v['sha256']) for v in ev.jl(L + 'inputs.before.jsonl')}
    for it in pins['prep']['transport_pins']:
        rel = it['relative_path']
        if rel.startswith('input/') or rel in ('historical/requests', 'bin/prepare_cohere_native_cohort'):
            req((ASSETS + '/' + rel, it['bytes'], it['sha256']) in observed, 'prep input pin ' + rel)
    cfg, cfg_body = ev.j('finalized-config.json'), ev.b('finalized-config.json')
    req(sha(cfg_body) == local['config_sha256'] and same(cfg, ev.j(L + 'config.json')), 'exact final prep config')
    exp = pins['prep']['config_template']
    comparable = dict(cfg)
    comparable['output_parent'] = exp['output_parent']
    comparable['shards'] = [dict(v, path=exp['shards'][i]['path']) for i, v in enumerate(cfg['shards'])]
    req(same(comparable, exp), 'strict prep config template (only shard paths and output_parent device/inode may change)')
    req(cfg['output_parent']['path'] == SCRATCH and isint(cfg['output_parent']['device']) and cfg['output_parent']['device'] > 0
        and isint(cfg['output_parent']['inode']) and cfg['output_parent']['inode'] > 0, 'prep parent identity (scratch mount)')
    req(len(cfg['shards']) == 11 and [v['path'] for v in cfg['shards']] == ['%s/input/en/%04d.parquet' % (ASSETS, i) for i in range(11)], 'installed source paths')
    eff = check_resources(ev, L, ('before', 'after', 'closed'), 'borsuk-cohort-parity.service')
    receipt, seal = ev.j('prepared-parent_cohort_complete.json'), ev.j(L + 'native.stdout.json')
    req(seal['status'] == 'COMPLETE' and seal['receipt_sha256'] == sha(ev.b('prepared-parent_cohort_complete.json')), 'prep receipt seal')
    req(receipt['schema'] == 'borsuk-cohere-native-cohort-receipt-v3' and receipt['status'] == 'COMPLETE' and receipt['config']['sha256'] == local['config_sha256']
        and receipt['resources'] == cfg['resources'] and receipt['output_parent'] == cfg['output_parent'], 'prep receipt configuration')
    req(receipt['config']['bytes'] == len(cfg_body) and receipt['config']['path'] == ROOT + '/finalized-config.json', 'prep config identity')
    runtime = {k: v for k, v in eff.items() if k not in ('path', 'cpuset', 'pids_max')}
    runtime['enforcement'] = 'cgroup-v2'
    req(same(receipt['runtime_limits'], runtime), 'prep runtime enforcement identity')
    req(receipt['dataset'] == cfg['dataset'] == DATASET and receipt['revision'] == cfg['revision'] == REVISION and receipt['columns'] == {'embedding': 'emb', 'document_id': '_id'} and receipt['metric'] == 'cosine', 'publisher identity')
    inputs = ev.jl(L + 'inputs.before.jsonl')
    req(len(receipt['sources']) == 11, 'prep footer roster')
    for s, c in zip(receipt['sources'], cfg['shards']):
        req(all(s[k] == c[k] for k in ('publisher_path', 'path', 'bytes', 'sha256', 'rows')) and isint(s['footer_bytes']) and 0 < s['footer_bytes'] <= 1048576, 'prep source pin')
        match = [v for v in inputs if v['path'] == s['path']]
        req(len(match) == 1 and match[0]['device'] == s['device'] and match[0]['inode'] == s['inode'], 'prep authenticated source descriptor')
    req(receipt['geometry'] == {'corpus_rows': ROWS, 'query_rows': QUERIES, 'dimensions': DIMS, 'k': K, 'corpus_intervals': INTERVALS,
                                'reserved_query_interval': RESERVED_INTERVAL, 'query_source_ordinals': [100000, 100032]}, 'prep geometry')
    req(receipt['reserved_queries_sha256'] == RESERVED_SHA, 'reserved query seal')
    for key in ('observed_peak_rss_at_receipt_bytes', 'modeled_peak_bytes'):
        req(isint(receipt[key]) and 0 < receipt[key] <= cfg['resources']['actual_memory_bytes'], 'prep memory ' + key)
    req(receipt['modeled_peak_bytes'] <= cfg['resources']['modeled_memory_bytes'], 'prep modeled memory admission')
    peak, swaps, status = time_fields(ev.t(L + 'native.time.txt'))
    req(0 < peak <= GIB8 and swaps == 0 and status == 0, 'prep GNU time ceilings (not ordered against native RSS)')
    accounting = {'id_state_bytes': 1281280000, 'retained_query_vector_bytes': 131072, 'truth_vector_block_bytes': 1048576, 'truth_all_query_top_k_bytes': 5120,
                  'truth_query_norm_bytes': 256, 'truth_heap_header_bytes': 768, 'truth_single_sorted_output_reserve_bytes': 160, 'retained_shard_metadata_bytes': 1476395008,
                  'encoded_and_decoded_row_bytes': 8192, 'control_output_and_failure_buffers_bytes': 67108864, 'caller_memory_bytes': 67108864, 'resident_peak_bytes': 2893086880,
                  'admitted_decoder_peak_bytes': receipt['modeled_peak_bytes'] - 2893086880, 'source_bytes': 2382253857, 'output_cap_bytes': 11271367168, 'caller_scratch_bytes': 1610612736,
                  'temporary_and_failure_reserve_bytes': 67108864, 'failure_outputs_retained_within_output_cap': True, 'process_rss_is_separate': True}
    req(same(receipt['resource_accounting'], accounting) and accounting['admitted_decoder_peak_bytes'] > 0, 'exact prep resource accounting')
    req(receipt['truth_method'] == 'independent single corpus block-major exhaustive scan with bounded per-query top-k heaps; no ANN inputs' and receipt['truth_ties'] == 'ascending corpus ordinal'
        and receipt['truth_arithmetic'] == 'sequential f64 dot and squared-norm sums over original f32; 1-dot/(sqrt(cnorm2)*sqrt(qnorm2))', 'fresh prep truth method (original-vector cosine)')
    req(type(receipt['elapsed_seconds_at_receipt']) in (int, float) and 0 < receipt['elapsed_seconds_at_receipt'] < 2400, 'prep elapsed bound')
    outs = {v['name']: v for v in receipt['outputs']}
    req(set(outs) == set(SEAL_NAMES) and len(receipt['outputs']) == 5, 'prep output roster')
    for n in SEAL_NAMES:
        s = pins['frozen_seals'][n]
        req(set(outs[n]) == {'name', 'bytes', 'sha256'} and outs[n]['bytes'] == s['bytes'] and outs[n]['sha256'] == s['sha256'], 'frozen regenerated seal ' + n)
    auth = ev.jl(L + 'outputs.authenticated.jsonl')
    req(ev.b(L + 'outputs.authenticated.jsonl') == ev.b(L + 'outputs.closed.jsonl') and ev.b(L + 'receipt.authenticated.jsonl') == ev.b(L + 'receipt.closed.jsonl'), 'prep output/receipt closure')
    req(sorted((Path(v['path']).name, v['bytes'], v['sha256']) for v in auth) == sorted((v['name'], v['bytes'], v['sha256']) for v in receipt['outputs']), 'prep outputs independently authenticated')
    pre = ev.j(L + 'prefixes.json')
    req(pre['corpus']['prefix_bytes'] == 409600000 and pre['corpus']['prefix_sha256'] == '3c95fa49a7d3f9d4bf6178f5ac2493e700a30fbcfe91da97a5fcf16a1f5fc09c', 'historical corpus prefix')
    req(pre['requests']['prefix_bytes'] == 131072 and outs['queries.f32']['sha256'] == pre['requests']['prefix_sha256'] == '664f5b269756a1de5a77c4ec359e56ccbe85c87603fa01fc5d87cc3f02e52667', 'historical requests prefix')
    truth = ev.b('prepared-parent_cohort_truth.u64')
    req(len(truth) == 2560 and sha(truth) == outs['truth.u64']['sha256'], 'collected opaque truth seal')
    closure_inventory(ev, L, ('closure.sha256', 'wrapper.exit', 'terminal.json'))
    return receipt


# ----------------------------------------------------------------------------- scratch binding and bootstrap control files
def lsblk_identity(text, label, vol):
    lines = text.split('\n')
    m = re.fullmatch(r'== (before|after) (/dev/nvme\d+n1) (vol-[0-9a-f]+)', lines[0])
    req(m is not None and m.group(1) == label and m.group(3) == vol, 'scratch identity header ' + label)
    start = text.index('{', len(lines[0]))
    data, end = json.JSONDecoder(object_pairs_hook=uniq, parse_constant=no_const).raw_decode(text, start)
    rest = text[end:].split('\n')
    req(vol.replace('-', '') in [r.strip() for r in rest], 'NVMe serial line equals volume id')
    name = m.group(2)[len('/dev/'):]
    dev = [d for d in data['blockdevices'] if d['name'] == name]
    req(len(dev) == 1, 'scratch device unique in lsblk')
    d = dev[0]
    req(str(d['serial']).strip() == vol.replace('-', '') and int(str(d['size'])) == 42949672960 and d['type'] == 'disk' and not d.get('children'), 'scratch device serial/size/no partitions')
    mounts = d.get('mountpoints')
    mounts = [x for x in (mounts if mounts is not None else [d.get('mountpoint')]) if x]
    if label == 'before':
        req(d.get('fstype') in (None, '') and not mounts and 'blkid_rc=2' in text, 'scratch device blank/unmounted before format')
    else:
        req(d.get('fstype') == 'ext4' and SCRATCH in mounts and 'TYPE="ext4"' in text, 'scratch device ext4 mounted after format')


def lsblk_json(text):
    data, _ = json.JSONDecoder(object_pairs_hook=uniq, parse_constant=no_const).raw_decode(text, text.index('{', text.index('\n')))
    return data


def root_unchanged(before, after, root_vol):
    # the root backing disk is found by the binding's root volume id; its partitions, filesystems, UUIDs and mounts must be identical across the scratch format
    states = []
    for text in (before, after):
        disks = [d for d in lsblk_json(text)['blockdevices'] if str(d.get('serial')).strip() == root_vol.replace('-', '') and d['type'] == 'disk']
        req(len(disks) == 1, 'root disk unique by the binding root volume id')
        kids = disks[0].get('children') or []
        states.append((disks[0]['name'], int(str(disks[0]['size'])),
                       [(c['name'], c['type'], c.get('fstype'), c.get('uuid'), int(str(c['size']))) for c in kids],
                       sorted(str(m) for c in kids for m in (c.get('mountpoints') or [c.get('mountpoint')]) if m)))
    req(states[0] == states[1] and '/' in states[0][3], 'root disk partitions/filesystems/UUIDs/mounts unchanged by the scratch format')


def scratch_cmp(text):
    # the zero-prefix comparison is a recorded diagnostic: status 0 (equal, no output) or 1 (one cmp difference line); anything else is invalid
    m = re.fullmatch(r'(.*)\nrc=(\d+)\n', text, re.S)
    req(m is not None, 'scratch cmp record shape')
    out, rc = m.group(1), int(m.group(2))
    req((rc == 0 and out == '') or (rc == 1 and re.fullmatch(r'/dev/nvme\d+n1 /dev/zero differ: byte [1-9]\d*, line [1-9]\d*', out) is not None), 'scratch cmp status 0 or 1 with its exact output')
    return {'rc': rc, 'output': out}


def verify_bootstrap(ev, pins, term):
    instance, exit_ = pins['instance_id'], term['exit']
    sup = {}
    for line in ev.t('support.sha256').splitlines():
        m = re.fullmatch(r'([0-9a-f]{64})  (\S+)', line)
        req(m is not None and m.group(2) not in sup, 'support syntax')
        sup[m.group(2)] = m.group(1)
    req(sha(ev.b('support.sha256')) == pins['support']['manifest_sha256'] and sup == pins['support']['files'], 'support manifest and 9 source pins')
    req(sha(ev.b('gate-config-template.json')) == sup['gate-config-template.json'], 'archived gate template equals frozen support')
    bind_body = ev.b('scratch-launch-binding.json')
    req(sha(bind_body) == pins['binding']['sha256'] and ev.b('scratch-launch-binding.json.sha256') == (sha(bind_body) + '  scratch-launch-binding.json\n').encode(), 'binding pin and exact companion bytes')
    binding = decode(bind_body)
    keys(binding, ('schema', 'instance_id', 'volume_id', 'root_volume_id', 'device', 'size_bytes', 'availability_zone', 'volume_type', 'encrypted', 'multi_attach', 'snapshot_empty',
                   'state', 'attached_device', 'delete_on_termination', 'create_time_epoch', 'attach_time_epoch', 'launch_time_epoch', 'describe_instances_sha256', 'describe_volumes_sha256'), 'binding v2 keys')
    req(binding['schema'] == 'borsuk-scratch-launch-binding-v2' and binding['instance_id'] == instance and binding['device'] == '/dev/sdf' and binding['attached_device'] == '/dev/sdf'
        and ie(binding['size_bytes'], 42949672960) and binding['volume_id'] == pins['binding']['volume_id'] and re.fullmatch(r'vol-[0-9a-f]{8,17}', binding['root_volume_id']) is not None
        and binding['root_volume_id'] != binding['volume_id'] and binding['volume_type'] == 'gp3' and binding['encrypted'] is True and binding['multi_attach'] is False
        and binding['snapshot_empty'] is True and binding['delete_on_termination'] is True and binding['state'] == 'in-use'
        and re.fullmatch(r'[a-z]{2}-[a-z]+-[0-9][a-z]', binding['availability_zone']) is not None
        and all(isint(binding[k]) and binding[k] > 0 for k in ('create_time_epoch', 'attach_time_epoch', 'launch_time_epoch'))
        and hexs(binding['describe_instances_sha256']) and hexs(binding['describe_volumes_sha256']), 'binding v2 content')
    lsblk_identity(ev.t('scratch-before.txt'), 'before', binding['volume_id'])
    lsblk_identity(ev.t('scratch-after.txt'), 'after', binding['volume_id'])
    root_unchanged(ev.t('scratch-before.txt'), ev.t('scratch-after.txt'), binding['root_volume_id'])
    binding_cmp = scratch_cmp(ev.t('scratch-cmp.txt'))
    for n, e in (('bootstrap.exit', exit_), ('transport-unit.exit', 0), ('parity-unit.exit', 0), ('chain-unit.exit', exit_)):
        req(ev.b(n) == ('%d\n' % e).encode(), 'original ' + n)
    ok = {'schema': 'borsuk-parity-service-exit-v1', 'exit_code': 'exited', 'exit_status': '0', 'service_result': 'success'}
    req(same(ev.j('transport-exit.json'), ok) and same(ev.j('service-exit.json'), ok), 'transport/prep manager receipts')
    unit = '%s/service-stop.sh bootstrap borsuk-bench-453182569524-euc1 %s/bootstrap-manager.json' % (ROOT, pins['run_prefix'])
    req('ExecStopPost=/bin/bash ' + unit in ev.t('cloud-final-unit.txt'), 'cloud-final manager drop-in')
    for name in ('systemd-after-cohort-parity.txt',):
        p = kv_lines(ev.t(name))  # a finished transient unit may already be collected, so only liveness is asserted here
        req(p.get('ActiveState') in ('inactive', 'failed') and p.get('MainPID', '0') == '0', 'systemd unit drained ' + name)
    env = ev.t('environment.txt')
    bash = re.search(r'GNU bash, version (\d+)\.(\d+)', env)
    req(bash is not None and (int(bash.group(1)), int(bash.group(2))) >= (5, 1) and re.search(r'^jq-1\.\d+', env, re.M) and 'GNU Time' in env, 'target environment: bash>=5.1, jq, GNU time')
    # disk and deadline admissions
    d = kv_lines(ev.t('disk-admission.txt').replace(' ', '\n'))
    need = sum(i['bytes'] for i in pins['prep']['transport_pins'])
    req(all(re.fullmatch(r'\d+', d.get(k, '')) for k in ('scratch_avail', 'scratch_floor', 'root_avail', 'transport_need', 'root_reserve', 'root_floor')), 'disk admission syntax')
    v = {k: int(x) for k, x in d.items()}
    req(v['scratch_floor'] == 30064771072 and v['root_reserve'] == 1610612736 and v['transport_need'] == need and v['root_floor'] == need + 1610612736
        and v['scratch_avail'] >= v['scratch_floor'] and v['root_avail'] >= v['root_floor'], 'root/scratch disk admission floors')
    rows = [l.split() for l in ev.t('deadline-admission.txt').splitlines()]
    req([r[0] for r in rows] == ['prep', 'chain'] and all(len(r) == 5 and all(x.isdigit() for x in r[1:]) for r in rows), 'deadline admission rows')
    for r, need_s in zip(rows, (13210, 10720)):
        boot, now, n, stop = (int(x) for x in r[1:])
        req(n == need_s and stop - boot == 14400 and boot <= now and now + n <= stop, 'remaining-absolute-time admission ' + r[0])
    req(int(rows[1][2]) >= int(rows[0][2]), 'admission order')
    return sup, binding, binding_cmp


# ----------------------------------------------------------------------------- gate finalization and chain
def f32_bits(v):
    req(type(v) in (int, float), 'f32 value type')
    return struct.unpack('<I', struct.pack('<f', v))[0]


def finite_bits(b):
    return isint(b) and 0 <= b < 4294967296 and (b & 0x7fffffff) < 0x7f800000


def stamp_mtime_us(stamp):
    m = re.fullmatch(r'(\d+):(\d+):(\d+):(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)\.(\d{9}) ([+-]\d{4}):(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{9} [+-]\d{4})', stamp)
    req(m is not None, 'authenticate stamp syntax')
    secs, _ = parse_time(m.group(4).replace(' ', 'T') + '.000000000' + m.group(6))
    return int(m.group(2)), int(m.group(3)), secs * 1000000 + int(m.group(5)[:6])


def verify_gate(ev, pins, sup, term, receipt):
    tmpl, final_body = ev.j('gate-config-template.json'), ev.b('gate-config.json')
    final = decode(final_body)
    cmp = final['prepared']['complete']
    req(tmpl['prepared']['complete']['bytes'] is None and tmpl['prepared']['complete']['sha256'] is None, 'template completion fields null')
    stripped = json.loads(json.dumps(final))
    stripped['prepared']['complete']['bytes'] = stripped['prepared']['complete']['sha256'] = None
    req(same(stripped, tmpl), 'gate finalization changes ONLY prepared.complete bytes/sha256')
    complete_body = ev.b('prepared-parent_cohort_complete.json')
    req(ie(cmp['bytes'], len(complete_body)) and cmp['sha256'] == sha(complete_body) and cmp['path'] == SCRATCH + '/cohort/complete.json', 'completion pin equals archived receipt')
    req(final_body == (json.dumps(final, indent=2, sort_keys=True, ensure_ascii=False) + '\n').encode(), 'preregistered serializer (jq -S --indent 2 + newline)')
    for n in SEAL_NAMES:
        req(same(final['prepared']['outputs'][n], dict(pins['frozen_seals'][n], path=SCRATCH + '/cohort/' + n)), 'gate output seal ' + n)
    gate_sha = sha(final_body)
    req(ev.j('chain-argv.json') == ['bash', ROOT + '/run_native_scale_build_gate.sh', ROOT + '/gate-config.json', gate_sha, CHAIN_EVIDENCE], 'exact chain argv')
    req(final['derive_config']['sha256'] == sup['derivation-config.json'] and ie(final['derive_config']['bytes'], 1145), 'derive config pinned by support')
    return final, gate_sha


def required_event_counters(rows):
    for row in rows:
        if row['path'] != '/sys/fs/cgroup':
            req(row['pids_events'] != 'absent' and 'max' in counters(row['pids_events']), 'missing non-root PID event counter')
            req(row['memory_events'] != 'absent' and
                {'max', 'oom', 'oom_kill', 'oom_group_kill'} <= set(counters(row['memory_events'])),
                'missing non-root memory event counter')


def snapshot_pair(ev, before, after, path, memory, cores, cpus):
    b, a = ev.jl(before), ev.jl(after)
    expected_paths = [path, '/sys/fs/cgroup/system.slice', '/sys/fs/cgroup']
    req([r['path'] for r in b] == expected_paths and [r['path'] for r in a] == expected_paths, 'exact owned cgroup ancestry')
    limits = ('path', 'cpu_max', 'cpuset_cpus_effective', 'memory_max', 'memory_swap_max', 'pids_max')
    def effective(rows, key):
        vals = [int(r[key]) for r in rows if r[key] not in ('absent', 'max')]
        req(vals, 'missing finite ' + key)
        return min(vals)
    for rows in (b, a):
        required_event_counters(rows)
        req(effective(rows, 'memory_max') == memory and effective(rows, 'memory_swap_max') == 0 and effective(rows, 'pids_max') == 128, 'snapshot effective memory/swap/PID limits')
        quotas = []
        for r in rows:
            if r['cpu_max'] == 'absent':
                continue
            parts = r['cpu_max'].split()
            req(len(parts) == 2 and parts[1].isdigit() and int(parts[1]) > 0, 'CPU quota syntax')
            if parts[0] != 'max':
                req(parts[0].isdigit(), 'finite CPU quota')
                quotas.append(decimal.Decimal(parts[0]) / decimal.Decimal(parts[1]))
        req(quotas and min(quotas) == cores and rows[0]['cpuset_cpus_effective'] == cpus, 'snapshot effective CPU limits')
        req(0 <= int(rows[0]['memory_current']) <= memory and 0 <= int(rows[0]['memory_peak']) <= memory and
            int(rows[0]['memory_swap_current']) == int(rows[0]['memory_swap_peak']) == 0 and
            0 <= int(rows[0]['pids_current']) <= int(rows[0]['pids_peak']) <= 128, 'snapshot leaf usage')
    for old, new in zip(b, a):
        req(all(old[k] == new[k] for k in limits), 'snapshot limit drift')
        req(counters(old['pids_events']) == counters(new['pids_events']), 'snapshot PID event drift')
        old_mem, new_mem = counters(old['memory_events']), counters(new['memory_events'])
        req(all(old_mem.get(k) == new_mem.get(k) for k in ('oom', 'oom_kill', 'oom_group_kill')) and
            new_mem.get('max', 0) >= old_mem.get('max', 0), 'snapshot OOM or reclaim drift')
        for r in (old, new):
            req(r['memory_swap_current'] == 'absent' or int(r['memory_swap_current']) == 0, 'ancestor swap usage')
            req(r['memory_swap_peak'] == 'absent' or int(r['memory_swap_peak']) == 0, 'ancestor swap peak')
    for name in (before + '.validated', after + '.validated', after + '.events-valid'):
        req(ev.b(name) == b'true\n', 'snapshot marker ' + name)


def verify_outer_observer(ev, rc, config_sha, recipe_sha):
    outer = ev.j('chain-outer/outer-closure.json')
    unit, ident = outer['unit'], outer['invocation_id']
    req(re.fullmatch(r'borsuk-pid128-observer-[0-9a-f-]+\.service', unit) is not None and hexs(ident, 32), 'observer identity format')
    cg = '/sys/fs/cgroup/system.slice/' + unit
    req(outer['schema'] == 'borsuk-native-scale-build-outer-closure-v2' and outer['status'] == 'CLOSED' and
        outer['recipe_sha256'] == recipe_sha and outer['config_sha256'] == config_sha and
        outer['control_group'] == '/system.slice/' + unit and ie(outer['actual_outer_exit'], rc) and
        outer['drained'] is True and outer['performance_claim'] is False, 'outer original exit binding')
    req(ev.b('chain-observer.unit') == (unit + '\n').encode() and ev.b('chain-launch.exit') == b'0\n' and
        ev.b('chain-actual.exit') == ('%d\n' % rc).encode(), 'launch distinct from original exit')
    manager = kv_lines(ev.t('chain-outer/manager.show'))
    launch = kv_lines(ev.t('chain-launch.identity'))
    req(launch.get('Id') == launch.get('Description') == unit and launch.get('InvocationID') == ident and
        launch.get('ControlGroup') == '/system.slice/' + unit, 'original launch identity binds final observer')
    result = 'success' if rc == 0 else 'exit-code'
    req(manager.get('Id') == manager.get('Description') == unit and manager.get('InvocationID') == ident and
        manager.get('MainPID') == '0' and manager.get('ExecMainCode') == '1' and manager.get('ExecMainStatus') == str(rc) and
        manager.get('Result') == result and manager.get('ActiveState') == ('active' if rc == 0 else 'failed') and
        manager.get('SubState') == ('exited' if rc == 0 else 'failed'), 'original observer manager exit')
    second = kv_lines(ev.t('chain-outer/before-stop.show'))
    req(second.get('InvocationID') == ident and second.get('MainPID') == '0' and second.get('Result') == result and
        second.get('ExecMainCode') == '1' and second.get('ExecMainStatus') == str(rc), 'same invocation before stop')
    after = kv_lines(ev.t('chain-outer/after-stop.show'))
    req(after.get('MainPID') == '0' and after.get('ActiveState') in ('inactive', 'failed'), 'observer stopped')
    drain = ev.j('chain-outer/drain.proof.json')
    req(drain['schema'] == 'borsuk-native-pid128-drain-v1' and drain['path'] == cg and
        drain['invocation_id'] == ident and drain['state'] in ('empty', 'removed'), 'original observer drain path')
    if drain['state'] == 'empty':
        req(counters(ev.t('chain-outer/drain.events')).get('populated') == 0 and
            manager.get('ControlGroup') == '/system.slice/' + unit, 'observer empty proof')
    else:
        req(drain['events'] is None and manager.get('ControlGroup') in ('', '/system.slice/' + unit), 'observer removed proof')
    for key, name in (('manager_show', 'chain-outer/manager.show'), ('outer_exit_file', 'chain-actual.exit'),
                      ('drain_proof', 'chain-outer/drain.proof.json')):
        art = outer[key]
        req(art['path'] == ROOT + '/evidence-root/' + name and
            ie(art['bytes'], ev.meta[name][0]) and art['sha256'] == ev.meta[name][1], 'outer artifact binding ' + key)
    for key, name in (('terminal_sha256', 'terminal.json'), ('manifest_sha256', 'closure.sha256'), ('wrapper_exit_sha256', 'wrapper.exit')):
        req(outer[key] == ev.meta['evidence-chain/' + name][1], 'outer closure body binding ' + key)
    identity = ev.j('evidence-chain/observer.identity.json')
    req(identity['invocation_id'] == ident and identity['control_group'] == '/system.slice/' + unit, 'wrapper observer identity')
    req(ev.b('evidence-chain/cleanup.exit') == b'0\n', 'owned payload cleanup completed')
    snapshot_pair(ev, 'evidence-chain/observer.initial', 'evidence-chain/observer.closure', cg, 268435456, 1, '0')
    return unit


def verify_phase_observer(ev, prefix, name, want):
    unit = ev.t(prefix + 'unit').strip()
    req(re.fullmatch(r'borsuk-pid128-[0-9a-f-]+-' + name + r'\.service', unit) is not None, 'payload unit')
    initial, final = (kv_lines(ev.t(prefix + f)) for f in ('manager.initial.txt', 'manager.final.txt'))
    ident = initial.get('InvocationID')
    req(hexs(ident, 32) and final.get('InvocationID') == ident and initial.get('Description') == final.get('Description') == unit, 'payload original invocation')
    cg = '/sys/fs/cgroup/system.slice/' + unit
    req(initial.get('ControlGroup') == '/system.slice/' + unit and final.get('ControlGroup') in ('', '/system.slice/' + unit), 'payload cgroup identity')
    req(final.get('MainPID') == '0' and final.get('ExecMainCode') == '1' and final.get('ExecMainStatus') == str(want) and
        final.get('Result') == ('success' if want == 0 else 'exit-code'), 'payload original manager exit')
    req(ev.b(prefix + 'manager.start.exit') == ev.b(prefix + 'manager.stop.exit') == b'0\n' and
        ev.b(prefix + 'payload.exit') == ('%d\n' % want).encode(), 'payload launch stop and original exit')
    memory, cores, cpus = (536870912, 1, '0') if name == 'baseline' else (GIB8, 4, '0-3')
    snapshot_pair(ev, prefix + 'resources.initial', prefix + 'resources.final', cg, memory, cores, cpus)
    drain = ev.j(prefix + 'drain.json')
    req(drain['schema'] == 'borsuk-native-pid128-payload-drain-v1' and drain['path'] == cg and
        drain['invocation_id'] == ident and drain['state'] in ('empty', 'removed'), 'payload drain binding')
    if drain['state'] == 'empty':
        req(counters(ev.t(prefix + 'drain.events')).get('populated') == 0, 'payload descendants drained')
    released, drained = ev.t(prefix + 'release.uptime_cs').strip(), ev.t(prefix + 'drain.uptime_cs').strip()
    req(released.isdigit() and drained.isdigit() and int(drained) >= int(released), 'monotonic payload interval')
    summary = kv_lines(ev.t(prefix + 'observer.txt').replace(' ', '\n'))
    req(summary.get('cadence_ms') == '50' and summary.get('timestamps') == 'proc_uptime_centiseconds' and
        summary.get('samples', '').isdigit() and 0 <= int(summary['samples']) <= 80000 and
        summary.get('bytes', '').isdigit() and int(summary['bytes']) == ev.meta[prefix + 'samples.txt'][0] <= 16777216, 'observer sampling metadata (not completeness proof)')


def verify_chain(ev, pins, term, final, gate_sha, prep_receipt):
    C, exit_ = 'evidence-chain/', term['exit']
    ct = ev.j(C + 'terminal.json')
    keys(ct, ('schema', 'status', 'stage', 'intended_exit', 'original_exit', 'signal', 'failed_line', 'config_sha256', 'baseline_native_exit', 'phases_completed', 'evidence',
              'wrapper_exit_scope', 'actual_manager_and_outer_exit_required', 'scope', 'baseline_invoked', 'actual_query_completion_requires_external_replay',
              'instance_termination_verified', 'performance_claim'), 'chain terminal keys')
    status = 'NATIVE_CHAIN_CLOSED' if exit_ == 0 else 'BASELINE_NONZERO_EXIT'
    req(ct['schema'] == 'borsuk-native-scale-build-gate-local-v2' and ct['status'] == status and ie(ct['intended_exit'], exit_) and ie(ct['original_exit'], exit_) and ct['signal'] is None
        and ct['stage'] == 'native_chain_closed' and ie(ct['baseline_native_exit'], exit_) and ct['config_sha256'] == gate_sha and ct['phases_completed'] == list(PHASES)
        and ct['evidence'] == CHAIN_EVIDENCE and ct['wrapper_exit_scope'] == 'intended_exit' and ct['actual_manager_and_outer_exit_required'] is True and ct['baseline_invoked'] is True
        and ct['actual_query_completion_requires_external_replay'] is True and ct['instance_termination_verified'] is False and ct['performance_claim'] is False, 'chain wrapper terminal')
    req(re.fullmatch(r'stage=native_chain_closed line=\d+ original_exit=%d exit=%d signal=\n' % (exit_, exit_), ev.t(C + 'closure.txt')) and ev.b(C + 'wrapper.exit') == ('%d\n' % exit_).encode(), 'chain closure/wrapper exit')
    for n in ('config.validated.txt', 'derive-config.validated.txt', 'complete.validated.txt', 'derivation.validated.txt', 'derivation.f32-roundtrip.txt', 'publish.validated.txt', 'generation.plane-binds-order.txt'):
        req(ev.b(C + n) == b'true\n', 'chain validation marker ' + n)
    req(ev.b(C + 'config.json') == ev.b('gate-config.json') and ev.j(C + 'invocation.json') == [ROOT + '/gate-config.json', gate_sha, CHAIN_EVIDENCE], 'chain config and invocation')
    for n in ('config.original.jsonl', 'config.copy.jsonl'):
        rec = ev.jl(C + n)
        req(len(rec) == 1 and rec[0]['sha256'] == gate_sha and ie(rec[0]['bytes'], len(ev.b('gate-config.json'))), 'chain config authentication ' + n)
    expect = {(ROOT + '/gate-config.json', len(ev.b('gate-config.json')), gate_sha)}
    expect |= {(v['path'], v['bytes'], v['sha256']) for v in final['binaries'].values()}
    expect |= {(v['path'], v['bytes'], v['sha256']) for v in final['prepared']['outputs'].values()}
    expect |= {(final[k]['path'], final[k]['bytes'], final[k]['sha256']) for k in ('derive_config',)}
    expect |= {(final['prepared']['complete']['path'], final['prepared']['complete']['bytes'], final['prepared']['complete']['sha256'])}
    obs = {(v['path'], v['bytes'], v['sha256']) for v in ev.jl(C + 'inputs.before.jsonl')}
    req(obs == expect and ev.b(C + 'inputs.before.jsonl') == ev.b(C + 'inputs.after.jsonl'), 'chain whole-input closure and pins')
    for it in pins['prep']['transport_pins']:
        if it['relative_path'].startswith('bin/') and it['relative_path'] != 'bin/prepare_cohere_native_cohort':
            req((ASSETS + '/' + it['relative_path'], it['bytes'], it['sha256']) in obs, 'chain binary pin ' + it['relative_path'])
    observer_unit = verify_outer_observer(ev, exit_, gate_sha, pins['support']['files']['run_native_scale_build_gate.sh'])
    check_resources(ev, C, ('before',) + tuple('after-' + p for p in PHASES) + ('closed',), observer_unit, 268435456, 1, '0')
    # derivation receipt (frozen derive receipt v2)
    deriv_body = ev.b('prepared-parent_derived_derivation.json')
    d = decode(deriv_body)
    keys(d, ('schema', 'producer_config_sha256', 'status', 'recipe', 'query_or_truth_used', 'original_corpus', 'rows', 'dimensions', 'corpus_intervals', 'outputs',
             'low_f32_bits', 'step_f32_bits', 'producer_authority', 'source_identity_qualification', 'admission'), 'derivation keys')
    seals = pins['frozen_seals']
    req(d['schema'] == 'borsuk-native-scale-derivation-receipt-v2' and d['producer_config_sha256'] == final['derive_config']['sha256'] and d['status'] == 'COMPLETE'
        and d['recipe'] == 'normalize_then_flat_fit_then_sq8' and d['query_or_truth_used'] is False and same(d['original_corpus'], {'bytes': seals['corpus.f32']['bytes'], 'sha256': seals['corpus.f32']['sha256']})
        and ie(d['rows'], ROWS) and ie(d['dimensions'], DIMS) and same(d['corpus_intervals'], INTERVALS) and same(d['producer_authority'], final['producer_authority'])
        and d['source_identity_qualification'] == 'external_frozen_prerequisite_not_self_certified', 'derivation receipt v2')
    outs = d['outputs']
    keys(outs, ('normalized', 'source_order', 'sq8'), 'derivation outputs')
    for k, size in (('normalized', ROWS * DIMS * 4), ('source_order', ROWS * 8), ('sq8', ROWS * (DIMS + 12))):
        keys(outs[k], ('bytes', 'sha256'), 'derivation seal')
        req(ie(outs[k]['bytes'], size) and hexs(outs[k]['sha256']), 'derivation seal ' + k)
    low, step = d['low_f32_bits'], d['step_f32_bits']
    req(len(low) == DIMS and all(finite_bits(b) for b in low) and len(step) == DIMS and all(finite_bits(b) and 0 < b < 0x7f800000 for b in step), 'derivation f32 calibration bits')
    keys(d['admission'], ('wrapper_payload_bytes', 'normalization_api_payload_bytes', 'flat_fit_api_payload_upper_bound_bytes', 'sq8_api_payload_bytes', 'peak_payload_upper_bound_bytes', 'aggregate_scratch_upper_bound_bytes'), 'derivation admission')
    nsha, osha, qsha = outs['normalized']['sha256'], outs['source_order']['sha256'], outs['sq8']['sha256']
    dd = final['namespaces']['derive_dir']
    got = {(Path(r['path']).name, r['bytes'], r['sha256']) for r in ev.jl(C + 'derive-outputs.authenticated.jsonl')}
    req(got == {('normalized.f32', outs['normalized']['bytes'], nsha), ('order.u64', outs['source_order']['bytes'], osha), ('sq8.bin', outs['sq8']['bytes'], qsha)}
        and ev.b(C + 'derive-outputs.authenticated.jsonl') == ev.b(C + 'derive-outputs.closed.jsonl'), 'derive outputs authenticated and closed')
    rd = ev.jl(C + 'derivation.authenticated.jsonl')
    req(len(rd) == 1 and rd[0]['path'] == dd + '/derivation.json' and rd[0]['sha256'] == sha(deriv_body) and ie(rd[0]['bytes'], len(deriv_body)) and ev.b(C + 'derivation.authenticated.jsonl') == ev.b(C + 'derivation.closed.jsonl'), 'derivation receipt authenticated and closed')
    # staged SQ8: create-only full copy and the ETag formula, independently recomputed from the authenticated stamp
    staged_key = 'semantic/objects/' + qsha
    staged_path = final['namespaces']['store_dir'] + '/' + staged_key
    sj = ev.j(C + 'sq8.staged.json')
    keys(sj, ('path', 'object_key', 'etag', 'sha256', 'bytes', 'full_copy', 'create_only'), 'staged keys')
    req(sj['path'] == staged_path and sj['object_key'] == staged_key and sj['sha256'] == qsha and ie(sj['bytes'], outs['sq8']['bytes']) and sj['full_copy'] is True and sj['create_only'] is True, 'staged SQ8 identity')
    st = ev.jl(C + 'sq8.staged.jsonl')
    req(len(st) == 1 and st[0]['path'] == staged_path and st[0]['sha256'] == qsha and ie(st[0]['bytes'], outs['sq8']['bytes']), 'staged SQ8 authentication')
    ino, size, mtime_us = stamp_mtime_us(st[0]['stamp'])
    req(ie(st[0]['inode'], ino) and size == outs['sq8']['bytes'] and sj['etag'] == '"%x-%x-%x"' % (ino, mtime_us, size), 'object_store LocalFileSystem ETag formula (inode-mtime_us-size)')
    for a, b in (('sq8.staged.jsonl', 'sq8.staged.after-etag.jsonl'), ('sq8.staged.jsonl', 'sq8.staged.after-publish.jsonl'), ('sq8.staged.jsonl', 'sq8.staged.closed.jsonl'), ('sq8.source.before.jsonl', 'sq8.source.after.jsonl')):
        req(ev.b(C + a) == ev.b(C + b), 'stage drift ' + b)
    # phases
    cfgs = {n: ev.b(C + 'configs/%s.json' % n) for n in ('generation', 'publish', 'baseline')}
    ns, bins = final['namespaces'], final['binaries']
    argv = {
        'derive': [bins['derive']['path'], '--derive', final['derive_config']['path'], final['derive_config']['sha256'], ns['derive_dir']],
        'stage': ['/usr/bin/dd', 'if=' + dd + '/sq8.bin', 'of=' + staged_path, 'bs=1048576', 'conv=excl,fsync', 'status=none'],
        'generation': [bins['generation']['path'], CHAIN_EVIDENCE + '/configs/generation.json', sha(cfgs['generation']), str(final['generation']['max_memory_bytes']), ns['generation_dir']],
        'publish': [bins['publish']['path'], CHAIN_EVIDENCE + '/configs/publish.json', sha(cfgs['publish']), ns['publish_receipt']],
        'baseline': [bins['baseline']['path'], CHAIN_EVIDENCE + '/configs/baseline.json', sha(cfgs['baseline']), ns['query_dir'] + '/baseline-result.jsonl'],
    }
    for p in PHASES:
        P = C + 'phases/%s/' % p
        ex = {n: ev.b(P + n + '.exit') for n in EXIT_FILES}
        req(all(re.fullmatch(rb'\d+\n', b) for b in ex.values()), 'phase exit syntax ' + p)
        v = {n: int(b) for n, b in ex.items()}
        want = exit_ if p == 'baseline' else 0
        req(v['native'] == v['timeout'] == v['time'] == want and all(v[n] == 0 for n in ('tee', 'time-log', 'supervisor-stderr-log', 'native-stderr-log')), 'original %s exits (native/timeout/time/tee/3 writers)' % p)
        req(ev.j(P + 'argv.json') == argv[p], 'exact %s argv' % p)
        cap = final['phases'][p]
        req(ev.t(P + 'timeout.seconds') == '%d\n' % cap['timeout_seconds'] and 0 < cap['timeout_seconds'] <= PHASE_MAX[p], 'configured %s timeout' % p)
        for n in ('native.stderr.txt', 'supervisor.stderr.txt', 'native.time.txt'):
            req(len(ev.b(P + n)) <= 1048576, 'phase log cap ' + n)
        req(len(ev.b(P + 'native.stdout')) <= cap['stdout_cap_bytes'], 'phase stdout cap ' + p)
        peak, swaps, status = time_fields(ev.t(P + 'native.time.txt'))
        req(0 < peak <= (536870912 if p == 'baseline' else GIB8) and swaps == 0 and status == want, 'phase GNU time ceilings ' + p)
        verify_phase_observer(ev, P, p, want)
    req(len(ev.b(C + 'phases/stage/native.stdout')) == 0 and len(ev.b(C + 'phases/publish/native.stdout')) == 0, 'stage/publish silent success')
    # generation: config (exact f32 calibration), root, plane, page manifest
    gc = decode(cfgs['generation'])
    keys(gc, ('discovery', 'semantic_profile', 'order', 'raw', 'raw_sha256', 'sq8', 'sq8_sha256', 'rows', 'dimensions', 'generation', 'base_epoch', 'low', 'step', 'sq8_object_key', 'sq8_etag'), 'generation config keys')
    rest = {k: v for k, v in gc.items() if k not in ('low', 'step')}
    req(same(rest, {'discovery': 'semantic', 'semantic_profile': 'scale1m', 'order': {'path': dd + '/order.u64', 'sha256': osha}, 'raw': dd + '/normalized.f32', 'raw_sha256': nsha,
                    'sq8': dd + '/sq8.bin', 'sq8_sha256': qsha, 'rows': ROWS, 'dimensions': DIMS, 'generation': 1, 'base_epoch': 0, 'sq8_object_key': staged_key, 'sq8_etag': sj['etag']}), 'generation config')
    req(len(gc['low']) == DIMS and len(gc['step']) == DIMS and [f32_bits(x) for x in gc['low']] == low and [f32_bits(x) for x in gc['step']] == step, 'generation low/step are the exact derivation f32 calibration')
    root_body = ev.b('prepared-parent_generation_manifest.json')
    root_sha = sha(root_body)
    so = ev.b(C + 'phases/generation/native.stdout')
    rg = ev.jl(C + 'generation.root.jsonl')
    req(so == (root_sha + '\n').encode() and len(rg) == 1 and rg[0]['sha256'] == root_sha and rg[0]['path'] == ns['generation_dir'] + '/manifest.json' and ev.b(C + 'generation.root.jsonl') == ev.b(C + 'generation.root.closed.jsonl'), 'generation root identity')
    root = decode(root_body)
    keys(root, ('schema', 'generation', 'base_epoch', 'plane_manifest_sha256', 'page_manifest_sha256', 'discovery', 'sq8_object_sha256', 'sq8_object_key', 'sq8_etag', 'low', 'step', 'canonical'), 'generation root keys')
    plane_body, page_body = ev.b('prepared-parent_generation_plane_manifest.json'), ev.b('prepared-parent_generation_page_manifest.json')
    req(root['schema'] == 'borsuk-two-bit-generation-v8' and ie(root['generation'], 1) and ie(root['base_epoch'], 0) and root['plane_manifest_sha256'] == sha(plane_body) and root['page_manifest_sha256'] == sha(page_body)
        and root['sq8_object_sha256'] == qsha and root['sq8_object_key'] == staged_key and root['sq8_etag'] == sj['etag'], 'generation root bindings')
    req([f32_bits(x) for x in root['low']] == low and [f32_bits(x) for x in root['step']] == step, 'generation root calibration equals derivation')
    disc = root['discovery']
    keys(disc, ('mode', 'profile', 'root_sha256', 'root_bytes', 'membership_sha256', 'membership_bytes', 'leaves_sha256', 'leaves_bytes', 'input_schema', 'input_root_sha256', 'centroids_sha256',
                'source_sha256', 'source_order_sha256', 'mean_sha256', 'records_sha256', 'sq8_sha256'), 'discovery keys')
    req(disc['mode'] == 'semantic' and disc['profile'] == 'scale1m' and disc['source_sha256'] == nsha and disc['source_order_sha256'] == osha and disc['sq8_sha256'] == qsha, 'semantic discovery binds the derived source/order/SQ8')
    can = root['canonical']
    keys(can, ('rows', 'dimensions', 'bytes', 'sha256', 'object_key'), 'canonical keys')
    req(ie(can['rows'], ROWS) and ie(can['dimensions'], DIMS) and ie(can['bytes'], ROWS * (DIMS * 4 + 8)) and hexs(can['sha256']) and re.fullmatch(r'(?:[A-Za-z0-9._-]+/)*objects/' + can['sha256'], can['object_key']) is not None, 'canonical source descriptor')
    plane, page = decode(plane_body), decode(page_body)
    keys(plane, ('schema', 'rows', 'dimensions', 'seed', 'record_bytes', 'source_sha256', 'sq8_sha256', 'source_order_sha256', 'mean_sha256', 'records_sha256', 'page_rows', 'page_digest_sha256', 'query_or_truth_used'), 'plane keys')
    req(plane['schema'] == 'borsuk-two-bit-plane-v3' and ie(plane['rows'], ROWS) and ie(plane['dimensions'], DIMS) and plane['source_sha256'] == nsha and plane['sq8_sha256'] == qsha
        and plane['source_order_sha256'] == osha and ie(plane['page_rows'], 32) and ie(plane['seed'], 20260923) and isint(plane['record_bytes']) and plane['record_bytes'] > 0 and plane['query_or_truth_used'] is False, 'plane binds source/order/SQ8')
    keys(page, ('schema', 'generation', 'rows', 'dimensions', 'page_rows', 'object_sha256', 'page_digest_sha256'), 'page manifest keys')
    req(page['schema'] == 'borsuk-v115-sq8-page-authority-v2' and ie(page['generation'], 1) and ie(page['rows'], ROWS) and ie(page['dimensions'], DIMS) and ie(page['page_rows'], 256) and page['object_sha256'] == qsha, 'page manifest')
    # publication
    pub_cfg = cfgs['publish']
    req(same(decode(pub_cfg), {'schema': 'borsuk-two-bit-local-publication-config-v1', 'root': {'path': ns['generation_dir'], 'sha256': root_sha}, 'store_root': ns['store_dir'], 'prefix': 'semantic/index', 'limits': final['publish']['limits']}), 'publish config')
    pr = ev.j('prepared-parent_publication-receipt.json')
    keys(pr, ('schema', 'config_sha256', 'prefix', 'metadata_prefix', 'root_sha256', 'generation', 'control_epoch'), 'publication receipt keys')
    req(pr['schema'] == 'borsuk-two-bit-local-publication-receipt-v1' and pr['config_sha256'] == sha(pub_cfg) and pr['prefix'] == 'semantic/index' and pr['root_sha256'] == root_sha and ie(pr['generation'], 1)
        and isint(pr['control_epoch']) and type(pr['metadata_prefix']) is str and re.fullmatch(r'[A-Za-z0-9_./-]{1,512}', pr['metadata_prefix']), 'publication receipt')
    # baseline config (28 fields) and the opaque result
    expected = {'schema': 'borsuk-cohere-native-baseline-config-v7', 'dataset': DATASET, 'revision': REVISION, 'metric': 'cosine', 'tie_rule': 'corpus_ordinal_ascending', 'corpus_intervals': INTERVALS,
                'reserved_query_interval': RESERVED_INTERVAL, 'cohort_receipt': final['prepared']['complete'], 'derivation_receipt': {'path': dd + '/derivation.json', 'bytes': len(deriv_body), 'sha256': sha(deriv_body)},
                'producer_authority': final['producer_authority'], 'corpus_source_first': 0, 'query_source_first': 100000, 'rows': ROWS, 'dimensions': DIMS, 'count': QUERIES, 'k': K, 'profile': 'scale1m',
                'backend': {'kind': 'local', 'store_root': ns['store_dir']}, 'generation_prefix': pr['metadata_prefix'], 'generation_root_sha256': root_sha, 'scratch_parent': ns['query_dir'] + '/scratch',
                'requests': final['prepared']['outputs']['queries.f32'], 'truth': final['prepared']['outputs']['truth.u64'], 'native_source': {'source_sha256': nsha, 'sq8_sha256': qsha, 'source_order_sha256': osha},
                'max_memory_bytes': 536870912, 'fetch_parallelism': 16, 'serving': {'mode': 'baseline'}, 'execution': {'mode': 'full'}}
    req(same(decode(cfgs['baseline']), expected), 'baseline config (28 fields, baseline serving, fetch 16)')
    result = ev.meta.get(BASELINE_RESULT)
    absent = ev.has(C + 'baseline-result.absent.txt')
    if result is not None and result[0] > 0:
        rec = ev.jl(C + 'baseline-result.authenticated.jsonl')
        req(len(rec) == 1 and rec[0]['path'] == ns['query_dir'] + '/baseline-result.jsonl' and ie(rec[0]['bytes'], result[0]) and rec[0]['sha256'] == result[1] and not absent and result[0] <= MAX_MEMBER, 'opaque baseline result length/SHA (never decoded)')
    else:
        req(exit_ != 0 and absent and not ev.has(C + 'baseline-result.authenticated.jsonl'), 'baseline result absent/empty only after a nonzero native exit')
    # observational disk evidence compared with the declared envelopes
    adm = kv_lines(ev.t(C + 'disk.admission.txt').replace(' ', '\n'))
    req(all(re.fullmatch(r'\d+', adm.get(k, '')) for k in ('total_cap_bytes', 'fresh_free_required_bytes', 'fs_total', 'fs_avail', 'prepared_five_output_bytes', 'total_cap_minus_prepared')), 'chain disk admission syntax')
    a = {k: int(x) for k, x in adm.items()}
    prepared = sum(seals[n]['bytes'] for n in SEAL_NAMES)
    req(a['total_cap_bytes'] == final['disk_proposal_bytes'] and a['fresh_free_required_bytes'] == final['disk_min_available_bytes'] and a['prepared_five_output_bytes'] == prepared and a['fs_avail'] >= a['fresh_free_required_bytes'] and a['fs_total'] >= a['total_cap_bytes'], 'chain disk admission')
    closed = ev.t(C + 'disk.closed.txt')
    used = 0
    for path in (ns['derive_dir'], ns['store_dir'], ns['generation_dir'], ns['publish_receipt'], ns['query_dir']):
        m = re.search(r'^(\d+)\t' + re.escape(path) + r'$', closed, re.M)
        req(m is not None, 'closed disk sample for ' + path)
        used += int(m.group(1))
    req(used + prepared <= final['disk_proposal_bytes'], 'retained bytes under the declared total cap')
    req(ev.t(C + 'inventory.status').strip() == 'find=0 sort=0', 'retained-file inventory completed')
    closure_inventory(ev, C, ('closure.sha256', 'wrapper.exit'))
    return {'baseline_result': None if result is None else {'bytes': result[0], 'sha256': result[1]}, 'derive_sq8_sha256': qsha, 'generation_root_sha256': root_sha, 'sq8_etag': sj['etag']}


# ----------------------------------------------------------------------------- driver
def main():
    req(len(sys.argv) == 3, 'usage: verify-closed.py REMOTE_RESULTS ROOT_PINS')
    R, pins_path = Path(sys.argv[1]), Path(sys.argv[2])
    pins = load_pins(pins_path)
    instance = pins['instance_id']
    term_body = bounded(R / 'terminal.json', 65536)
    req(sha(term_body) == pins['terminal_sha256'], 'independent collected terminal pin')
    term = decode(term_body)
    keys(term, ('schema', 'instance_id', 'phase', 'original_exit', 'exit', 'evidence', 'chain', 'acceptance', 'publication_verified', 'scientific_success_asserted', 'performance_claim'), 'terminal keys')
    exit_ = term['exit']
    disposition = {0: 'NATIVE_CHAIN_CLOSED', 2: 'BASELINE_NONZERO_EXIT', 3: 'BASELINE_NONZERO_EXIT'}
    req(term['schema'] == 'borsuk-native-scale-build-bootstrap-closed-v1' and term['instance_id'] == instance and term['phase'] == 'complete' and isint(exit_) and exit_ in disposition
        and ie(term['original_exit'], exit_) and same(term['evidence'], {'bytes': pins['archive']['bytes'], 'sha256': pins['archive']['sha256']})
        and same(term['chain'], {'unit_exit': exit_, 'disposition': disposition[exit_]}) and term['acceptance'] == 'PROVISIONAL_REQUIRES_EXTERNAL_BOOTSTRAP_EXIT'
        and term['publication_verified'] is False and term['scientific_success_asserted'] is False and term['performance_claim'] is False, 'bootstrap terminal')
    manager_body = bounded(R / 'bootstrap-manager.json', 4096)
    manager = decode(manager_body)
    keys(manager, ('schema', 'instance_id', 'terminal_sha256', 'exit_code', 'exit_status', 'service_result', 'final_exit'), 'manager keys')
    # PROSPECTIVE cloud-final mapping, TARGET_UNVERIFIED: recorded verbatim below, never synthesized.
    mapping = ('0', 'success') if exit_ == 0 else ('1', 'exit-code')
    req(manager['schema'] == 'borsuk-parity-bootstrap-exit-v1' and manager['instance_id'] == instance and manager['terminal_sha256'] == sha(term_body) and manager['exit_code'] == 'exited'
        and manager['final_exit'] == str(exit_) and (manager['exit_status'], manager['service_result']) == mapping, 'cloud-final manager vs nested bootstrap exit (prospective mapping)')
    col = decode(bounded(R / 'collection.json', 65536))
    keys(col, ('instance_id', 'terminated', 'reason', 'user_data_sha256', 'archive_authenticated', 'manifest_verified', 'independent_replay_pending', 'late', 'cost_bound',
               'supplied_started_epoch', 'effective_started_epoch', 'chain_unit_exit', 'chain_disposition', 'bootstrap_exit', 'performance_claim'), 'collection keys')
    req(col['instance_id'] == instance and col['user_data_sha256'] == pins['user_data_sha256'] and col['archive_authenticated'] is True and col['manifest_verified'] is False and col['late'] is False
        and col['cost_bound'] == 'MODELED_15000_PLUS_120' and ie(col['chain_unit_exit'], exit_) and col['chain_disposition'] == disposition[exit_] and ie(col['bootstrap_exit'], exit_) and col['performance_claim'] is False, 'watcher collection (consistency only)')
    manifest_body = bounded(R / 'artifacts.sha256', 2097152)
    req(sha(manifest_body) == pins['artifacts_manifest_sha256'], 'independent manifest pin')
    ev = stream_archive(R / 'evidence.tar.gz', pins, parse_manifest(manifest_body))
    tops = {n for n in ev.meta if '/' not in n}
    req(TOP <= tops and tops <= TOP | OPTIONAL_TOP and all(n.startswith(('evidence-local/', 'evidence-chain/', 'chain-outer/')) for n in ev.meta if '/' in n), 'exact archive roster')
    sup, binding, binding_cmp = verify_bootstrap(ev, pins, term)
    state_last = bounded(R / 'state.txt', 65536).decode().split()[-1]
    req(state_last == 'terminated', 'watcher final state')
    ec2 = verify_ec2(R, pins, instance, binding, bounded(R / 'launch-time.txt', 4096).decode())
    prep = verify_prep(ev, pins)
    final, gate_sha = verify_gate(ev, pins, sup, term, prep)
    chain = verify_chain(ev, pins, term, final, gate_sha, prep)
    print(json.dumps(dict(
        status='REPLAY_MECHANICS_VERIFIED', independent_replay_verified=True, scope='mechanics and source binding only', instance_id=instance, terminated=True,
        bootstrap_exit=exit_, chain_disposition=disposition[exit_], baseline_native_exit=exit_, baseline_closed_nonzero=exit_ != 0,
        cloud_final_manager={k: manager[k] for k in ('exit_code', 'exit_status', 'service_result', 'final_exit')}, chain_unit_manager=kv_lines(ev.t('chain-outer/manager.show')),
        cloud_final_mapping_status='TARGET_UNVERIFIED_PROSPECTIVE_PINNED', baseline_result=chain['baseline_result'], generation_root_sha256=chain['generation_root_sha256'],
        ec2=ec2, scratch_cmp=binding_cmp, binding_schema=binding['schema'], performance_claim=False, scientific_success_asserted=False, quality_not_evaluated=True, baseline_output_opaque=True,
        open_seams=['cloud-final exit mapping unproven on the real target (canary must exercise 0,2,3)', 'volume deletion proof not supported by source',
                    'instance role input-write restriction not supported by source', 'watcher manifest_verified:false (this replay checked the manifest)'])))


if __name__ == '__main__':
    try:
        main()
    except Invalid as e:
        print(json.dumps({'status': 'INVALID', 'reason': str(e)}), file=sys.stderr)
        sys.exit(1)
    except (KeyError, IndexError, TypeError, ValueError, OSError, tarfile.TarError, decimal.InvalidOperation) as e:
        print(json.dumps({'status': 'INVALID', 'reason': '%s: %s' % (type(e).__name__, e)}), file=sys.stderr)
        sys.exit(1)
    except Exception as e:  # a defect in this verifier, never evidence of a good run
        print(json.dumps({'status': 'VERIFIER_DEFECT', 'reason': '%s: %s' % (type(e).__name__, e)}), file=sys.stderr)
        sys.exit(2)
