"""STATIC, UNRUN fixture for the three a0008 canary repairs (W handoff, T4 explicit-deny line, T3 systemd command-line expansion).

Source-bound: the W jq program is cut out of the committed canary-coordinator.sh, the `denied` function and the cg_inner function out of the committed
canary-tail.sh; bash and jq are driven on synthetic files only (no AWS, no systemd, no unit, no device). Nothing here is executed by the author.
usage: canary-fault-fixture.py NEW_TAIL NEW_COORDINATOR BASE_TAIL BASE_COORDINATOR       exit 0 = every case matched

FIFO adjustment (the only difference from the receipt baf53192...): NEW_COORDINATOR may now also carry the four exact cloud-init FIFO path edits of
d4a544dc (typed). Before the single hunk comparison in section D those four edits are reversed one by one, each required to occur exactly once in
NEW_COORDINATOR (new spelling) and exactly once in BASE_COORDINATOR (old spelling); a miss exits 1. Nothing else is normalized and no check was added,
removed or skipped; the tail-pin check still hashes the REAL NEW_COORDINATOR. BASE_* stay the b9e12f58 sources.
"""
import difflib, hashlib, json, pathlib, re, subprocess, sys, tempfile

new_tail, new_coord, base_tail, base_coord = (pathlib.Path(p) for p in sys.argv[1:5])
TT, CT, BT, BC = (p.read_text() for p in (new_tail, new_coord, base_tail, base_coord))
fails = []
# FIFO adjustment: exact (new, old) pairs, newest spelling first; silent when satisfied, FAIL + exit 1 otherwise (the PASS roster is unchanged)
FIFO_BACK = (('fifo=/run/cloud-init/share/hook-hotplug-cmd\n', 'fifo=/run/cloud-init/hook-hotplug-cmd\n'),
             (' ex=(! \\( -path ./share/hook-hotplug-cmd -type p \\))\n', ' ex=(! -path ./hook-hotplug-cmd)\n'),
             ('--exclude=./share/hook-hotplug-cmd -cf', '--exclude=./hook-hotplug-cmd -cf'),
             ('admitted_special:["/run/cloud-init/share/hook-hotplug-cmd"]', 'admitted_special:["/run/cloud-init/hook-hotplug-cmd"]'))
CT_FIFO_BACK = CT
for _new, _old in FIFO_BACK:
    if CT.count(_new) != 1 or BC.count(_old) != 1 or CT_FIFO_BACK.count(_new) != 1:
        print('FAIL FIFO adjustment: %r must occur once in NEW_COORDINATOR and %r once in BASE_COORDINATOR' % (_new, _old)); sys.exit(1)
    CT_FIFO_BACK = CT_FIFO_BACK.replace(_new, _old)
if 'share/hook-hotplug-cmd' in CT_FIFO_BACK:
    print('FAIL FIFO adjustment: an unaccounted share/hook-hotplug-cmd spelling remains'); sys.exit(1)
def check(name, ok):
    print(('PASS ' if ok else 'FAIL ') + name)
    if not ok: fails.append(name)

INST, BUCKET = 'i-02eab9c9baa8a26b2', 'borsuk-bench-453182569524-euc1'
KEY = 'research/semantic-router/20261010/actual1m-scale-chain-canary-a0008/inputs/permission-probe.txt'

# ---- A. W: the real canary handoff (original_exit 99 / exit 99 / phase canary / null chain), nothing broader
prog = re.search(r"jq -e --arg i \"\$instance\" '(\(keys == \[.*?)' \"\$root/terminal\.json\"", CT, re.S).group(1)
terminal = {"acceptance": "PROVISIONAL_REQUIRES_EXTERNAL_BOOTSTRAP_EXIT", "chain": {"disposition": None, "unit_exit": None}, "evidence": {"bytes": 21134, "sha256": "b3fd4dfdafa272ff7e0f981ebe969852994905676fc592c02bc955b28f76639f"},
            "exit": 99, "instance_id": INST, "original_exit": 99, "performance_claim": False, "phase": "canary", "publication_verified": False,
            "schema": "borsuk-native-scale-build-bootstrap-closed-v1", "scientific_success_asserted": False}
def w_ok(doc, inst=INST):
    return subprocess.run(['jq', '-e', '--arg', 'i', inst, prog], input=json.dumps(doc), capture_output=True, text=True).returncode == 0
def mut(**kw):
    d = json.loads(json.dumps(terminal)); d.update(kw); return d
check('W accepts the actual a0008 terminal handoff', w_ok(terminal))
for name, doc in [('original_exit 0 (the old expectation)', mut(original_exit=0)), ('exit 0', mut(exit=0)), ('exit 98', mut(exit=98)), ('original_exit "99" string', mut(original_exit='99')),
                  ('phase bootstrap', mut(phase='bootstrap')), ('phase complete', mut(phase='complete')), ('chain unit_exit 0', mut(chain={'unit_exit': 0, 'disposition': None})),
                  ('chain disposition set', mut(chain={'unit_exit': None, 'disposition': 'NATIVE_CHAIN_CLOSED'})), ('chain extra key', mut(chain={'unit_exit': None, 'disposition': None, 'x': 1})),
                  ('publication_verified true', mut(publication_verified=True)), ('scientific_success_asserted true', mut(scientific_success_asserted=True)),
                  ('performance_claim true', mut(performance_claim=True)), ('other acceptance', mut(acceptance='VERIFIED')), ('other schema', mut(schema='x')),
                  ('evidence bytes 0', mut(evidence={'bytes': 0, 'sha256': 'a' * 64})), ('evidence bytes float', mut(evidence={'bytes': 1.5, 'sha256': 'a' * 64})),
                  ('evidence bytes string', mut(evidence={'bytes': '9', 'sha256': 'a' * 64})), ('evidence short sha', mut(evidence={'bytes': 9, 'sha256': 'a'})),
                  ('evidence extra key', mut(evidence={'bytes': 9, 'sha256': 'a' * 64, 'x': 1})), ('extra top-level key', mut(extra=1))]:
    check('W refuses ' + name, not w_ok(doc))
check('W refuses a foreign instance id', not w_ok(terminal, 'i-0123456789abcdef0'))
missing = json.loads(json.dumps(terminal)); missing.pop('phase')
check('W refuses a missing key', not w_ok(missing))
check('the manager record expectation is unchanged (exited / 1 / exit-code / final 99)', "'.exit_code == \"exited\" and .exit_status == \"1\" and .service_result == \"exit-code\"'" in CT and '.final_exit == "99"' in CT)

# ---- B. T4: the exact AWS CLI v2 explicit-deny line, status 254
def cut(text, head):
    m = re.search(r'^' + re.escape(head) + r'.*?^\}$', text, re.S | re.M)
    assert m is not None, head
    return m.group(0)
denied_src = cut(TT, 'denied() {')
ACCT, ROLE = '453182569524', 'borsuk-bench-role'
def deny_line(op, acct=ACCT, role=ROLE, inst=INST, bucket=BUCKET, key=KEY, tail=' with an explicit deny in an identity-based policy'):
    return ('An error occurred (AccessDenied) when calling the %s operation: User: arn:aws:sts::%s:assumed-role/%s/%s is not authorized to perform: s3:%s on resource: "arn:aws:s3:::%s/%s"%s'
            % (op, acct, role, inst, op, bucket, key, tail))
def run_denied(op, err, rc=254, name='t'):
    with tempfile.TemporaryDirectory() as d:
        pathlib.Path(d, 'iam.%s.rc' % name).write_text('%s\n' % rc); pathlib.Path(d, 'iam.%s.err' % name).write_text(err)
        r = subprocess.run(['bash', '-c', 'set -u; ev=$1; instance=$2; bucket=$3; ' + denied_src + '\ndenied "$4" "$5" "$6"', '_', d, INST, BUCKET, name, op, KEY], capture_output=True, text=True)
        return r.returncode == 0
actual = lambda op: '\naws: [ERROR]: ' + deny_line(op) + '\n'
check('T4 accepts the actual a0008 PutObject deny', run_denied('PutObject', actual('PutObject')))
check('T4 accepts the actual a0008 DeleteObject deny', run_denied('DeleteObject', actual('DeleteObject')))
check('T4 accepts the same line without the optional aws: [ERROR]: prefix and blank line', run_denied('PutObject', deny_line('PutObject') + '\n'))
for rc in (0, 1, 2, 124, 125, 137, 255):
    check('T4 refuses status %d even with the right text' % rc, not run_denied('PutObject', actual('PutObject'), rc))
check('T4 refuses 412 PreconditionFailed', not run_denied('PutObject', '\naws: [ERROR]: An error occurred (PreconditionFailed) when calling the PutObject operation: At least one of the pre-conditions you specified did not hold\n'))
check('T4 refuses the Delete line for Put', not run_denied('PutObject', actual('DeleteObject')))
check('T4 refuses the Put line for Delete', not run_denied('DeleteObject', actual('PutObject')))
check('T4 refuses an implicit deny', not run_denied('PutObject', '\naws: [ERROR]: ' + deny_line('PutObject', tail=' because no identity-based policy allows the s3:PutObject action') + '\n'))
check('T4 refuses another resource key', not run_denied('PutObject', '\naws: [ERROR]: ' + deny_line('PutObject', key='research/other/inputs/permission-probe.txt') + '\n'))
check('T4 refuses another bucket', not run_denied('PutObject', '\naws: [ERROR]: ' + deny_line('PutObject', bucket='other-bucket') + '\n'))
check('T4 refuses another instance', not run_denied('PutObject', '\naws: [ERROR]: ' + deny_line('PutObject', inst='i-0123456789abcdef0') + '\n'))
check('T4 refuses another role', not run_denied('PutObject', '\naws: [ERROR]: ' + deny_line('PutObject', role='other-role') + '\n'))
check('T4 refuses an 11-digit account', not run_denied('PutObject', '\naws: [ERROR]: ' + deny_line('PutObject', acct='45318256952') + '\n'))
check('T4 refuses a non-numeric account', not run_denied('PutObject', '\naws: [ERROR]: ' + deny_line('PutObject', acct='45318256952x') + '\n'))
check('T4 refuses an extra trailing line', not run_denied('PutObject', actual('PutObject') + 'extra\n'))
check('T4 refuses two error lines', not run_denied('PutObject', actual('PutObject') + actual('PutObject')))
check('T4 refuses two leading blank lines', not run_denied('PutObject', '\n\naws: [ERROR]: ' + deny_line('PutObject') + '\n'))
check('T4 refuses a doubled prefix', not run_denied('PutObject', '\naws: [ERROR]: aws: [ERROR]: ' + deny_line('PutObject') + '\n'))
check('T4 refuses trailing garbage on the line', not run_denied('PutObject', '\naws: [ERROR]: ' + deny_line('PutObject') + ' x\n'))
check('T4 refuses empty stderr (a timeout or a silent failure)', not run_denied('PutObject', ''))
check('T4 callers pass the key', 'denied putdeny PutObject "$key"' in TT and 'denied deldeny DeleteObject "$key"' in TT)

# ---- C. T3: systemd expands $ in command-line words (systemd-run(1); services since v254) - model of the documented rule
def systemd_words(argv):
    out = []
    for w in argv:
        if w.startswith('$') and w[1:2] not in ('{', '$'):
            continue                                   # "$FOO" as its own word: replaced by the (unset) variable split at whitespace
        w = w.replace('$$', '\x00')
        w = re.sub(r'\$\{[^}]*\}', '', w)              # ${NAME} (valid or invalid name, always unset in the unit) evaluates to the empty string
        out.append(w.replace('\x00', '$'))
    return out
fn = cut(BT, 'cg_inner() {')
old_text = subprocess.run(['bash', '-c', fn + '\ndeclare -f cg_inner; printf "%s\\n" \'cg_inner "$1"\''], capture_output=True, text=True).stdout   # what the OLD tail put on the command line
old_word = old_text
modeled = systemd_words(['bash', '-c', old_word, '_', '/mnt/borsuk-scale1m/evidence-root/canary-tail/cgroup.stray.pid'])[2]
check('OLD argv: the manager blanks "${args[@]}", leaving jq -cn "" (an empty program) - the exact a0008 cgroup.err', '${args[@]}' in old_word and 'jq -cn "" ' in modeled and '${args[@]}' not in modeled)
check('OLD argv: the other braced expansions are blanked too', '${name//./_}' in old_word and '${d%/*}' in old_word and 'key=;' in modeled and 'd=;' in modeled)
new_t3 = cut(TT, 't3() {')
sd = re.search(r'systemd-run .*?>\s*"\$ev/cgroup\.resources\.jsonl"', new_t3, re.S).group(0)
check('NEW: the unit command line is `bash "$ev/cg_inner.sh" "$pidf"` (no function text, no -c)', 'bash "$ev/cg_inner.sh" "$pidf"' in sd and 'declare -f' not in sd and 'bash -c' not in sd and '$(' not in sd)
check('NEW: the script file is written first with a checked write', '{ declare -f cg_inner; printf \'%s\\n\' \'cg_inner "$1"\'; } > "$ev/cg_inner.sh" || return 1' in new_t3 and new_t3.index('cg_inner.sh" || return 1') < new_t3.index('systemd-run '))
words = systemd_words(['bash', '/mnt/borsuk-scale1m/evidence-root/canary-tail/cg_inner.sh', '/mnt/borsuk-scale1m/evidence-root/canary-tail/cgroup.stray.pid'])
check('NEW: the model leaves the three command-line words untouched and they hold no $ or %', words == ['bash', '/mnt/borsuk-scale1m/evidence-root/canary-tail/cg_inner.sh', '/mnt/borsuk-scale1m/evidence-root/canary-tail/cgroup.stray.pid'] and not any(c in ''.join(words) for c in '$%'))
probe = re.search(r"probe_py <<'EOF' \|\| true\n(.*?)\nEOF\n", CT, re.S).group(1)
check('the coordinator probe word (python -c script) carries no $ or %', '$' not in probe and '%' not in probe)
check('every other systemd-run in the sources passes only shell-expanded paths/properties', all('declare -f' not in l and '-c "$(' not in l for l in (TT + CT).splitlines() if 'systemd-run' in l))

# ---- D. nothing else moved
def hunks(a, b): return [l for l in difflib.unified_diff(a.splitlines(), b.splitlines(), n=0, lineterm='') if l.startswith('@@')]
check('coordinator differs from the base in exactly one hunk (the W terminal check)', len(hunks(BC, CT_FIFO_BACK)) == 1)
check('tail differs from the base only in the pin, the T3 launch and the T4 deny helper/callers', len(hunks(BT, TT)) <= 8)
for token in ('step_begin T3 20', '-p RuntimeMaxSec=8 -p TimeoutStopSec=4', '-p MemoryMax=8G', 't3_clean || dr=$?', 'need 900', 'need 780', 'step=$((full - 8))' if False else 'step_stop=$((full - 8))', 'wc_stop=$((EPOCHSECONDS + 120))', 'T3.drain.exit', "cap(\"memory_max\") == 8589934592"):
    check('unchanged limit/cleanup text present in both: ' + token, token in TT and token in BT)
check('the tail pins the new coordinator hash', ('coord_sha=' + hashlib.sha256(CT.encode()).hexdigest()) in TT and ('coord_sha=' + hashlib.sha256(CT.encode()).hexdigest()) not in BT)
print('cases failed: %d' % len(fails))
sys.exit(1 if fails else 0)
