#!/usr/bin/env python3
"""STATIC, UNRUN source-bound fixture for the a0011 OWN2 repair (coordinator held loaded by its own After=, strict actual ExecStopPost, strict cloud-init 26.1 status-less shape).

usage: canary-a11-repair-fixture.py NEW_TAIL NEW_COORD BASE_TAIL BASE_COORD HIST_FIFO_FIXTURE HIST_WT3T4_FIXTURE D262_TAIL D262_COORD B9_TAIL B9_COORD CCE5_COORD
  NEW_*   = the repaired canary-tail.sh / canary-coordinator.sh under test;  BASE_* = exact root base 6ce3037a (== d4a544dc) sources
  HIST_*  = the PRESERVED historical fixtures (sha256 pinned below, never edited);  D262_/B9_/CCE5_ = the historical sources those fixtures were written against
Sections:  P pins | R exact repair diff + inverse | H preserved historical fixtures run UNMODIFIED on the HISTORICAL-COPY normalization (the inverse of this repair; evidence of
historical-check continuity ONLY, NOT actual-source runtime qualification) | T tail After= | O ordering/no-cycle model | G readback driven on synthetic After lists |
E ExecStopPost clause on the closed a0011 case-N.json records | C cloud-init program on the closed a0011 status/result bytes + negatives.   exit 0 = every case matched.
Nothing here is executed by its author; bash/jq/python are driven on synthetic or embedded closed-a0011 data only.
"""
import copy, difflib, hashlib, json, pathlib, re, subprocess, sys, tempfile

(NEW_TAIL, NEW_COORD, BASE_TAIL, BASE_COORD, HIST_FIFO, HIST_WT, D_TAIL, D_COORD, B_TAIL, B_COORD, C_COORD) = sys.argv[1:12]
rd = lambda p: pathlib.Path(p).read_bytes()
sha = lambda b: hashlib.sha256(b).hexdigest()
NT, NC, BT, BC = (rd(p).decode() for p in (NEW_TAIL, NEW_COORD, BASE_TAIL, BASE_COORD))
fails = []
passed = 0
def check(name, ok):
    global passed
    print(('PASS ' if ok else 'FAIL ') + name)
    if ok: passed += 1
    else: fails.append(name)

# ---- P. pins: the inputs are exactly the intended historical/base objects
PINS = {
    'BASE_TAIL': (BASE_TAIL, 'b069a2e76805683d207988e3e9764cbefb9a92b17195b8be06e4e889b2376659'),
    'BASE_COORD': (BASE_COORD, '983a3f16e5f3567e587729502d82d3a0c33cf90df3f7054e08559b66583f0acc'),
    'HIST_FIFO_FIXTURE': (HIST_FIFO, '78e23213f40b362baed4c0227cb0eaac8e08e26b474928460292e44aa2b7f6ee'),
    'HIST_WT3T4_FIXTURE': (HIST_WT, 'fc970608fd25094941a62ea3404a98ae73b0661c8ac78aa128f978053356e306'),
    'D262_TAIL': (D_TAIL, '046316866e35788682c013337eba8118ec126ecfcfcd8e35aff61274842251c4'),
    'D262_COORD': (D_COORD, '7c2db63dea0d935614066e44b5f67e652d2a8137a7a5d883262106a348d70392'),
    'B9_TAIL': (B_TAIL, 'ba736c3d4b6c05bd6b6c242f795ad2ac138580ab66a15df9856efe23b0fb9aae'),
    'B9_COORD': (B_COORD, 'bff35749d957c1e76635b1170c2ef390fe97dadf5b3dc72712b65451397e6f35'),
    'CCE5_COORD': (C_COORD, 'd4ab3ec877393421140ecd7be9e110e3a4fa1f629cc0d0dc377d9b42e2fef406'),
}
for name, (path, want) in PINS.items():
    check('pin ' + name + ' sha256 ' + want[:12], sha(rd(path)) == want)
if fails:
    print('FAIL: an input is not the pinned object; nothing further can be trusted'); sys.exit(1)

# ---- R. the exact repair: (new, old) literal pairs, each exactly once; inverse of NEW must equal BASE byte for byte
COORD_PAIRS = [('  errs=$(jq -er \'.v1.errors | length\' "$res/case-$n.result.json") || errs=bad\n fi\n # cloud-init 26.1 with the copy\'s status.json deleted: ONE stage runs (modules-final) and the four stage records alias ONE errors list (nullstatus.copy()), so the raw result repeats the\n # owner errors four times. The raw files are kept; the exact frozen shape is required, never uniqued: owner errors [] (case 0) or the single scripts-user failure (cases 2/3), ALL four\n # stage lists equal to the owner, only modules-final carries times, and result.errors is exactly the ordered stage concatenation. Both files must hold exactly ONE JSON document\n # (-s on the result, --slurpfile length on the status), so no earlier false document can be masked and no extra document ignored.\n if [[ -f $res/case-$n.result.json && -f $res/case-$n.status.json ]]; then\n  jq -e -s --argjson want "$ers" --arg err "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))" --slurpfile s "$res/case-$n.status.json" \'["init","init-local","modules-config","modules-final"] as $st | (length == 1 and ($s | length) == 1) and (.[0] as $r | $s[0].v1 as $v | (($r.v1 | keys) == ["datasource","errors"]) and (($v | keys) == ["datasource","init","init-local","modules-config","modules-final","stage"]) and ($v.stage == null) and ($st | all(.[]; . as $k | ($v[$k] | type) == "object" and ($v[$k] | keys) == ["errors","finished","recoverable_errors","start"] and ($v[$k].errors | type) == "array" and ($v[$k].recoverable_errors | type) == "object")) and ([$st[] | select($v[.].start != null or $v[.].finished != null)] == ["modules-final"]) and ($v["modules-final"].start | type == "number" and . > 0) and ($v["modules-final"].finished | type == "number" and . >= $v["modules-final"].start) and ($v["modules-final"].errors == (if $want == 0 then [] else [$err] end)) and ($st | all(.[]; . as $k | $v[$k].errors == $v["modules-final"].errors)) and ($r.v1.datasource == $v.datasource) and ($r.v1.errors == [$st[] as $k | $v[$k].errors[]]))\' "$res/case-$n.result.json" > /dev/null || { ok=0; sev 1 "case $n cloud-init status/result is not the exact frozen shape (owner errors expected $ers, raw result errors $errs)"; }\n fi\n', '  errs=$(jq -er \'.v1.errors | length\' "$res/case-$n.result.json") || errs=bad\n  [[ $errs == "$ers" ]] || { ok=0; sev 1 "case $n cloud-init error count $errs"; }\n fi\n'), ('symlinks:($l|split("\\n")|map(select(length>0))),coordinator_after:($ca|split(" ")|map(select(length>0))|sort),units:($u|split("\\n"))}\'', 'symlinks:($l|split("\\n")|map(select(length>0))),units:($u|split("\\n"))}\''), ('--rawfile l "$res/symlinks.tsv" --arg ca "$ca" \\\n \'{schema:"borsuk-canary-admission-v1"', '--rawfile l "$res/symlinks.tsv" \\\n \'{schema:"borsuk-canary-admission-v1"'), ('done\n# the ACTIVE coordinator keeps the three case units loaded: systemd.unit(5) UNIT GARBAGE COLLECTION - another loaded unit\'s After= references them, so a stopped successful\n# case unit is not unloaded and its actual ExecStopPost record survives to the show below (After= alone never starts them). Bounded readback of the loaded manager state:\nca=$(run 3 systemctl show borsuk-canary-coordinator.service -p After --value) && (( ${#ca} <= 2048 )) || { sev 2 "coordinator After unreadable or oversized"; exit 0; }\nfor w in cloud-final.service borsuk-canary-final-0.service borsuk-canary-final-2.service borsuk-canary-final-3.service; do [[ " $ca " == *" $w "* ]] || { sev 2 "coordinator After lacks $w"; exit 0; }; done\n# case trees are copied AFTER the reload (the generator rewrites /run/cloud-init); edits touch the copies only\n', 'done\n# case trees are copied AFTER the reload (the generator rewrites /run/cloud-init); edits touch the copies only\n')]
TAIL_PAIRS = [('Description=Borsuk canary coordinator\nAfter=cloud-final.service borsuk-canary-final-0.service borsuk-canary-final-2.service borsuk-canary-final-3.service\n', 'Description=Borsuk canary coordinator\nAfter=cloud-final.service\n'), ('coord_sha=b2ec593fe50776462dcb403237f1a971361bfa2249e8c303f9d4815ed968700b\n', 'coord_sha=983a3f16e5f3567e587729502d82d3a0c33cf90df3f7054e08559b66583f0acc\n')]
def inverse(text, pairs):
    for new, old in pairs:
        if text.count(new) != 1:
            return None
        text = text.replace(new, old)
    return text
inv_coord, inv_tail = inverse(NC, COORD_PAIRS), inverse(NT, TAIL_PAIRS)
check('every coordinator repair literal occurs exactly once in NEW_COORD', inv_coord is not None)
check('every tail repair literal occurs exactly once in NEW_TAIL', inv_tail is not None)
check('inverse(NEW_COORD) == BASE_COORD byte for byte (the diff is exactly the authorized coordinator repair)', inv_coord == BC)
check('inverse(NEW_TAIL) == BASE_TAIL byte for byte (the diff is exactly the After line + the coordinator pin)', inv_tail == BT)
check('NEW differs from BASE in both files', NC != BC and NT != BT)
check('the tail pins the sha256 of the REAL new coordinator', ('coord_sha=' + sha(NC.encode())) in NT.splitlines())
def hunks(a, b): return [l for l in difflib.unified_diff(a.splitlines(), b.splitlines(), n=0, lineterm='') if l.startswith('@@')]
check('coordinator delta is exactly 3 hunks (readback, admission arg+key, cloud-init check)', len(hunks(BC, NC)) == 3)
check('tail delta is exactly 2 hunks (pin, After line)', len(hunks(BT, NT)) == 2)
check('the hook-record clause is byte-identical to the base (strict actual code=exited status=0 for ALL cases)', [l for l in NC.splitlines() if 'ExecStopPost did not run as the exact 1-argument hook' in l] == [l for l in BC.splitlines() if 'ExecStopPost did not run as the exact 1-argument hook' in l] != [])
for tok in ('step_begin "case$n" 30', 'case_stop=$((EPOCHSECONDS + 30))', 'run 5 systemctl stop "$u"', 'drain "$n"', 'systemctl start --no-block "$u"', 'run 10 systemctl daemon-reload'):
    check('unchanged case command/budget text: ' + tok, NC.count(tok) == BC.count(tok) >= 1)
check('exactly one daemon-reload remains in the coordinator (no extra reload)', len(re.findall(r'systemctl daemon-reload', NC)) == len(re.findall(r'systemctl daemon-reload', BC)) == 1)

# ---- H. PRESERVED historical fixtures, UNMODIFIED, on the HISTORICAL-COPY normalization (= inverse of this repair = the sources they were written for)
print('HISTORICAL-COPY normalization follows: the preserved fixtures below run on the INVERSE of the repair; this is NOT actual-source runtime qualification of the repaired files')
with tempfile.TemporaryDirectory() as hd:
    hp = pathlib.Path(hd)
    (hp / 'HISTORICAL-COPY-canary-tail.sh').write_text(inv_tail)
    (hp / 'HISTORICAL-COPY-canary-coordinator.sh').write_text(inv_coord)
    ht, hc = str(hp / 'HISTORICAL-COPY-canary-tail.sh'), str(hp / 'HISTORICAL-COPY-canary-coordinator.sh')
    r = subprocess.run([sys.executable, HIST_WT, ht, hc, B_TAIL, B_COORD], capture_output=True, text=True)
    pl = [l for l in r.stdout.splitlines() if l.startswith('PASS ')]
    check('preserved W/T3/T4 fixture exits 0 on the historical copy', r.returncode == 0 and r.stdout.rstrip().endswith('cases failed: 0'))
    check('preserved W/T3/T4 roster is unchanged: exactly 71 PASS lines and no FAIL line', len(pl) == 71 and not any(l.startswith('FAIL ') for l in r.stdout.splitlines()))
    if r.returncode != 0: print(r.stdout[-2000:], r.stderr[-2000:])
    r = subprocess.run([sys.executable, HIST_FIFO, ht, hc, D_TAIL, D_COORD, C_COORD], capture_output=True, text=True)
    last = r.stdout.rstrip().splitlines()[-1] if r.stdout.strip() else ''
    check('preserved FIFO path/type fixture exits 0 on the historical copy (its own roster, last line: ' + last + ')', r.returncode == 0 and re.fullmatch(r'ALL \d+ PASS', last) is not None and 'FAIL' not in r.stdout)
    if r.returncode != 0: print(r.stdout[-2000:], r.stderr[-2000:])

# ---- T. tail: the coordinator unit's After= is the holder
m = re.search(r"cat > /run/systemd/system/borsuk-canary-coordinator\.service <<'EOF'\n(.*?)\nEOF\n", NT, re.S)
unit = m.group(1) if m else ''
CASES = ['borsuk-canary-final-0.service', 'borsuk-canary-final-2.service', 'borsuk-canary-final-3.service']
check('tail coordinator unit has exactly one After= line: cloud-final.service then the three exact case units', [l for l in unit.splitlines() if l.startswith('After=')] == ['After=cloud-final.service ' + ' '.join(CASES)])
check('After= is the only dependency naming a case unit (no Requires/Wants/BindsTo/PartOf/Requisite/Upholds/Conflicts/Before/OnFailure/Triggers)', not any('borsuk-canary-final' in l and not l.startswith('After=') for l in unit.splitlines()))
check('the coordinator unit has no Before= line and no Install section', not any(l.startswith(('Before=', '[Install]')) for l in unit.splitlines()))
check('the coordinator unit body is otherwise unchanged (Type, ExecStart, RuntimeMaxSec, TimeoutStopSec, KillMode)', all(s in unit for s in ('Type=simple', 'ExecStart=/bin/bash /mnt/borsuk-scale1m/canary-coordinator.sh', 'RuntimeMaxSec=480', 'TimeoutStopSec=15', 'KillMode=control-group')))
check('the tail still performs exactly one daemon-reload and a --no-block coordinator start', NT.count('systemctl daemon-reload') == BT.count('systemctl daemon-reload') == 1 and 'systemctl start --no-block borsuk-canary-coordinator.service' in NT)

# ---- O. ordering model: no cycle (documented systemd.unit(5): After= is ordering only, independent of Requires/Wants; the model is the retained fragment + this unit + default deps)
FRAG = '# /usr/lib/systemd/system/cloud-final.service\n[Unit]\n# https://docs.cloud-init.io/en/latest/explanation/boot.html\nDescription=Cloud-init: Final Stage\nAfter=network-online.target time-sync.target cloud-config.service rc-local.service\nAfter=multi-user.target\nBefore=apt-daily.service\nWants=network-online.target cloud-config.service\nConditionPathExists=!/etc/cloud/cloud-init.disabled\nConditionKernelCommandLine=!cloud-init=disabled\nConditionEnvironment=!KERNEL_CMDLINE=cloud-init=disabled\n\n\n[Service]\nType=oneshot\nExecStart=/usr/bin/cloud-init modules --mode=final\nRemainAfterExit=yes\nTimeoutSec=0\nKillMode=process\nTasksMax=infinity\n\n# Output needs to appear in instance console output\nStandardOutput=journal+console\n\n[Install]\nWantedBy=cloud-init.target\n\n# /run/systemd/system/cloud-final.service.d/borsuk-exit.conf\n[Service]\nExecStopPost=/bin/bash /mnt/borsuk-scale1m/service-stop.sh bootstrap borsuk-bench-453182569524-euc1 research/semantic-router/20261010/actual1m-scale-chain-canary-a0011/bootstrap-manager.json\n'
check('retained cloud-final fragment is the pinned a0011 member', sha(FRAG.encode()) == '7c224599a4d24cab63331ee50dc9c905827cd9dabb2baeaafab740e133dcd872')
frag_unit = FRAG.split('\n\n# /run/systemd')[0]
def deps(text, key):
    out = []
    for l in text.splitlines():
        if l.startswith(key + '='): out += l.split('=', 1)[1].split()
    return out
case_after, case_before = deps(frag_unit, 'After'), deps(frag_unit, 'Before')
edges = {}   # edge a -> b means a is ordered BEFORE b
def order(a, b): edges.setdefault(a, set()).add(b)
for c in CASES:
    for a in case_after: order(a, c)
    for b in case_before: order(c, b)
for a in deps(unit, 'After'): order(a, 'borsuk-canary-coordinator.service')
for a in ('sysinit.target', 'basic.target'): order(a, 'borsuk-canary-coordinator.service')      # DefaultDependencies=yes
order('borsuk-canary-coordinator.service', 'shutdown.target')
def has_cycle():
    state = {}
    def dfs(n):
        state[n] = 1
        for k in edges.get(n, ()):
            if state.get(k) == 1 or (k not in state and dfs(k)): return True
        state[n] = 2
        return False
    return any(n not in state and dfs(n) for n in list(edges))
check('model: the real fragment orders the case units After network-online/time-sync/cloud-config/rc-local/multi-user and Before apt-daily only', set(case_after) >= {'network-online.target', 'cloud-config.service', 'multi-user.target'} and case_before == ['apt-daily.service'])
check('model: no ordering cycle with coordinator After the three case units', not has_cycle())
COORD = 'borsuk-canary-coordinator.service'
check('model: only its After= list and the default basics are ordered before the coordinator, and only shutdown.target after it (a cycle needs an edge back into it)', {a for a, ts in edges.items() if COORD in ts} == set(deps(unit, 'After')) | {'sysinit.target', 'basic.target'} and edges.get(COORD) == {'shutdown.target'})
check('no case unit text in the coordinator writes After=/Before= (case units keep exactly the fragment ordering)', not any(re.search(r'\b(After|Before)=', l) for l in NC.splitlines() if not l.lstrip().startswith('#')))

# ---- G. readback code driven on synthetic After lists (stub run/sev; the extracted source lines are the real ones)
ca_line = next(l for l in NC.splitlines() if l.startswith('ca=$(run 3 systemctl show borsuk-canary-coordinator.service -p After --value)'))
w_line = next(l for l in NC.splitlines() if l.startswith('for w in cloud-final.service borsuk-canary-final-0.service'))
def readback(after):
    s = 'sev(){ echo "SEV $*"; }\nrun(){ shift; printf "%s" "$CA"; }\n' + ca_line + '\n' + w_line + '\necho READBACK_OK\n'
    r = subprocess.run(['bash', '-c', s], capture_output=True, text=True, env={'CA': after, 'PATH': '/usr/bin:/bin'})
    return 'READBACK_OK' in r.stdout and r.returncode == 0
FULL = 'sysinit.target basic.target cloud-final.service ' + ' '.join(CASES) + ' system.slice'
check('readback accepts the full loaded After list (any order, extra implicit entries)', readback(FULL) and readback(' '.join(reversed(FULL.split()))))
for label, bad in [('missing case 0', FULL.replace(CASES[0], '')), ('missing case 2', FULL.replace(CASES[1], '')), ('missing case 3', FULL.replace(CASES[2], '')),
                   ('missing cloud-final', FULL.replace('cloud-final.service', '')), ('only cloud-final (the old unit)', 'sysinit.target basic.target cloud-final.service'),
                   ('empty', ''), ('prefixed superstring', FULL.replace(CASES[0], 'x' + CASES[0])), ('suffixed superstring', FULL.replace(CASES[1], CASES[1] + 'x')),
                   ('case 02 instead of 2', FULL.replace(CASES[1], 'borsuk-canary-final-02.service')), ('wrong unit type', FULL.replace(CASES[2], 'borsuk-canary-final-3.socket')),
                   ('oversized (> 2048)', FULL + ' ' + 'x' * 2100)]:
    check('readback refuses: ' + label, not readback(bad))
check('admission.json records the readback (arg + sorted coordinator_after key) in the existing jq', '--arg ca "$ca"' in NC and 'coordinator_after:($ca|split(" ")|map(select(length>0))|sort)' in NC)

# ---- E. ExecStopPost clause on the closed a0011 case-N.json exec_stop_post strings (actual populated record required for ALL cases; the pristine null line refused)
CASEJ = {'0': ('06aca2dd1c94e76be8469d0c5663a4dd815148abba5f6e43b46303cec0a8014b', '{\n  "schema": "borsuk-canary-case-v1",\n  "case": 0,\n  "class": "SUCCEEDED",\n  "systemd": {\n    "active": "active",\n    "sub": "exited",\n    "result": "success",\n    "exec_main_code": "1",\n    "exec_main_status": "0"\n  },\n  "exec_stop_post": "ExecStopPost={ path=/bin/bash ; argv[]=/bin/bash /mnt/borsuk-scale1m/service-stop.sh bootstrap ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; status=0/0 }",\n  "modules_ran": "Running module scripts-user,",\n  "probe": {\n    "modules": [\n      [\n        "scripts-user",\n        "always"\n      ]\n    ],\n    "paths": [\n      "/var/log/cloud-init-output.log",\n      "/var/log/cloud-init.log"\n    ]\n  },\n  "all_pinned_tuples_match": false,\n  "modified_copy": true,\n  "production_closure": false\n}\n'), '2': ('fd24bd3583eb4bf94dc65b3eb5b77cc4b2cc6048dd91fe26e36b9dd709e3c360', '{\n  "schema": "borsuk-canary-case-v1",\n  "case": 2,\n  "class": "EXITED_NONZERO",\n  "systemd": {\n    "active": "failed",\n    "sub": "failed",\n    "result": "exit-code",\n    "exec_main_code": "1",\n    "exec_main_status": "1"\n  },\n  "exec_stop_post": "ExecStopPost={ path=/bin/bash ; argv[]=/bin/bash /mnt/borsuk-scale1m/service-stop.sh bootstrap ; ignore_errors=no ; start_time=[Sat 2026-10-10 02:01:10 UTC] ; stop_time=[Sat 2026-10-10 02:01:10 UTC] ; pid=4929 ; code=exited ; status=0 }",\n  "modules_ran": "Running module scripts-user,",\n  "probe": {\n    "modules": [\n      [\n        "scripts-user",\n        "always"\n      ]\n    ],\n    "paths": [\n      "/var/log/cloud-init-output.log",\n      "/var/log/cloud-init.log"\n    ]\n  },\n  "all_pinned_tuples_match": false,\n  "modified_copy": true,\n  "production_closure": false\n}\n'), '3': ('d2045c032f5aed9c89d69a2221828cfce24548a50a51117d02c10f28f082f387', '{\n  "schema": "borsuk-canary-case-v1",\n  "case": 3,\n  "class": "EXITED_NONZERO",\n  "systemd": {\n    "active": "failed",\n    "sub": "failed",\n    "result": "exit-code",\n    "exec_main_code": "1",\n    "exec_main_status": "1"\n  },\n  "exec_stop_post": "ExecStopPost={ path=/bin/bash ; argv[]=/bin/bash /mnt/borsuk-scale1m/service-stop.sh bootstrap ; ignore_errors=no ; start_time=[Sat 2026-10-10 02:01:11 UTC] ; stop_time=[Sat 2026-10-10 02:01:11 UTC] ; pid=5009 ; code=exited ; status=0 }",\n  "modules_ran": "Running module scripts-user,",\n  "probe": {\n    "modules": [\n      [\n        "scripts-user",\n        "always"\n      ]\n    ],\n    "paths": [\n      "/var/log/cloud-init-output.log",\n      "/var/log/cloud-init.log"\n    ]\n  },\n  "all_pinned_tuples_match": false,\n  "modified_copy": true,\n  "production_closure": false\n}\n')}
hook_line = next(l for l in NC.splitlines() if 'ExecStopPost did not run as the exact 1-argument hook' in l)
def hook_ok(esp):
    s = 'sev(){ echo "SEV $*"; }\nok=1; n=0; root=/mnt/borsuk-scale1m\nesp=$ESP\n' + hook_line + '\necho OK=$ok\n'
    r = subprocess.run(['bash', '-c', s], capture_output=True, text=True, env={'ESP': esp, 'PATH': '/usr/bin:/bin'})
    return 'OK=1' in r.stdout
for n_ in ('2', '3'):
    check('hook clause accepts the actual a0011 populated record of case ' + n_, hook_ok(json.loads(CASEJ[n_][1])['exec_stop_post']))
pristine = json.loads(CASEJ['0'][1])['exec_stop_post']
check('hook clause REFUSES the actual a0011 case 0 null/pristine record (pid=0 code=(null)): no waiver', not hook_ok(pristine))
pop = json.loads(CASEJ['2'][1])['exec_stop_post']
check('hook clause refuses code=exited status=1', not hook_ok(pop.replace('status=0 }', 'status=1 }')))
check('hook clause refuses a different argv', not hook_ok(pop.replace('service-stop.sh bootstrap ;', 'service-stop.sh parity ;')))
check('hook clause refuses ignore_errors=yes', not hook_ok(pop.replace('ignore_errors=no', 'ignore_errors=yes')))
check('hook clause refuses a doubled ExecStopPost line', not hook_ok(pop + '\n' + pop))

# ---- C. cloud-init status/result program on the closed a0011 bytes + negatives
EMB = {'case-0.status.json': ('e3de9fa87c010af91353e35f58f25f2a966ad326d8ac9a6866504d889418cde9', '{\n "v1": {\n  "datasource": null,\n  "init": {\n   "errors": [],\n   "finished": null,\n   "recoverable_errors": {},\n   "start": null\n  },\n  "init-local": {\n   "errors": [],\n   "finished": null,\n   "recoverable_errors": {},\n   "start": null\n  },\n  "modules-config": {\n   "errors": [],\n   "finished": null,\n   "recoverable_errors": {},\n   "start": null\n  },\n  "modules-final": {\n   "errors": [],\n   "finished": 114.44,\n   "recoverable_errors": {},\n   "start": 114.41\n  },\n  "stage": null\n }\n}\n'), 'case-0.result.json': ('583b52a12040edf10a69bcd01cdb05b00d9f7bf1ea9afa305b6c86d7bac610ba', '{\n "v1": {\n  "datasource": null,\n  "errors": []\n }\n}\n'), 'case-2.status.json': ('465c2ad44191f3e9ced0a3e1282e2f252340c62fbf5c79bf2fd8ae714d2621b6', '{\n "v1": {\n  "datasource": null,\n  "init": {\n   "errors": [\n    "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))"\n   ],\n   "finished": null,\n   "recoverable_errors": {\n    "WARNING": [\n     "Failed to run module scripts-user (scripts in /var/lib/cloud/instance/scripts)",\n     "Running module scripts-user (<module \'cloudinit.config.cc_scripts_user\' from \'/usr/lib/python3/dist-packages/cloudinit/config/cc_scripts_user.py\'>) failed"\n    ]\n   },\n   "start": null\n  },\n  "init-local": {\n   "errors": [\n    "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))"\n   ],\n   "finished": null,\n   "recoverable_errors": {\n    "WARNING": [\n     "Failed to run module scripts-user (scripts in /var/lib/cloud/instance/scripts)",\n     "Running module scripts-user (<module \'cloudinit.config.cc_scripts_user\' from \'/usr/lib/python3/dist-packages/cloudinit/config/cc_scripts_user.py\'>) failed"\n    ]\n   },\n   "start": null\n  },\n  "modules-config": {\n   "errors": [\n    "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))"\n   ],\n   "finished": null,\n   "recoverable_errors": {\n    "WARNING": [\n     "Failed to run module scripts-user (scripts in /var/lib/cloud/instance/scripts)",\n     "Running module scripts-user (<module \'cloudinit.config.cc_scripts_user\' from \'/usr/lib/python3/dist-packages/cloudinit/config/cc_scripts_user.py\'>) failed"\n    ]\n   },\n   "start": null\n  },\n  "modules-final": {\n   "errors": [\n    "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))"\n   ],\n   "finished": 115.25,\n   "recoverable_errors": {\n    "WARNING": [\n     "Failed to run module scripts-user (scripts in /var/lib/cloud/instance/scripts)",\n     "Running module scripts-user (<module \'cloudinit.config.cc_scripts_user\' from \'/usr/lib/python3/dist-packages/cloudinit/config/cc_scripts_user.py\'>) failed"\n    ]\n   },\n   "start": 115.23\n  },\n  "stage": null\n }\n}\n'), 'case-2.result.json': ('105f45850a70793f74ef7f51b455e3f6487cb93945bb98e07eaae4ced7da6ff7', '{\n "v1": {\n  "datasource": null,\n  "errors": [\n   "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))",\n   "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))",\n   "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))",\n   "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))"\n  ]\n }\n}\n'), 'case-3.status.json': ('3418e57e919fd9de4ce0f04de777c42a2808050241d0fa40ed0b14d931947d27', '{\n "v1": {\n  "datasource": null,\n  "init": {\n   "errors": [\n    "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))"\n   ],\n   "finished": null,\n   "recoverable_errors": {\n    "WARNING": [\n     "Failed to run module scripts-user (scripts in /var/lib/cloud/instance/scripts)",\n     "Running module scripts-user (<module \'cloudinit.config.cc_scripts_user\' from \'/usr/lib/python3/dist-packages/cloudinit/config/cc_scripts_user.py\'>) failed"\n    ]\n   },\n   "start": null\n  },\n  "init-local": {\n   "errors": [\n    "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))"\n   ],\n   "finished": null,\n   "recoverable_errors": {\n    "WARNING": [\n     "Failed to run module scripts-user (scripts in /var/lib/cloud/instance/scripts)",\n     "Running module scripts-user (<module \'cloudinit.config.cc_scripts_user\' from \'/usr/lib/python3/dist-packages/cloudinit/config/cc_scripts_user.py\'>) failed"\n    ]\n   },\n   "start": null\n  },\n  "modules-config": {\n   "errors": [\n    "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))"\n   ],\n   "finished": null,\n   "recoverable_errors": {\n    "WARNING": [\n     "Failed to run module scripts-user (scripts in /var/lib/cloud/instance/scripts)",\n     "Running module scripts-user (<module \'cloudinit.config.cc_scripts_user\' from \'/usr/lib/python3/dist-packages/cloudinit/config/cc_scripts_user.py\'>) failed"\n    ]\n   },\n   "start": null\n  },\n  "modules-final": {\n   "errors": [\n    "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))"\n   ],\n   "finished": 116.04,\n   "recoverable_errors": {\n    "WARNING": [\n     "Failed to run module scripts-user (scripts in /var/lib/cloud/instance/scripts)",\n     "Running module scripts-user (<module \'cloudinit.config.cc_scripts_user\' from \'/usr/lib/python3/dist-packages/cloudinit/config/cc_scripts_user.py\'>) failed"\n    ]\n   },\n   "start": 116.01\n  },\n  "stage": null\n }\n}\n'), 'case-3.result.json': ('105f45850a70793f74ef7f51b455e3f6487cb93945bb98e07eaae4ced7da6ff7', '{\n "v1": {\n  "datasource": null,\n  "errors": [\n   "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))",\n   "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))",\n   "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))",\n   "(\'scripts-user\', RuntimeError(\'Runparts: 1 failures (part-001) in 1 attempted commands\'))"\n  ]\n }\n}\n')}
for name, (want, text) in list(EMB.items()) + [('case-%s.json' % k, v) for k, v in CASEJ.items()]:
    check('embedded closed a0011 bytes match their recorded sha256: ' + name, sha(text.encode()) == want)
pm = re.search(r"jq (-e -s) --argjson want \"\$ers\" --arg err \"([^\"]*)\" --slurpfile s \"\$res/case-\$n\.status\.json\" '(.*?)' \"\$res/case-\$n\.result\.json\"", NC, re.S)
FLAGS, ERR, PROG = pm.group(1).split(), pm.group(2), pm.group(3)
check('the cloud-init check slurps the result (-e -s) and guards both document counts before indexing', FLAGS == ['-e', '-s'] and PROG.count('(length == 1 and ($s | length) == 1) and (.[0] as $r | $s[0].v1 as $v |') == 1)
check("the pinned owner error is the exact a0011 string", ERR == "('scripts-user', RuntimeError('Runparts: 1 failures (part-001) in 1 attempted commands'))")
def cinit(status_text, result_text, want, prog=PROG, err=ERR):
    with tempfile.TemporaryDirectory() as d:
        pathlib.Path(d, 's.json').write_text(status_text); pathlib.Path(d, 'r.json').write_text(result_text)
        return subprocess.run(['jq'] + FLAGS + ['--argjson', 'want', str(want), '--arg', 'err', err, '--slurpfile', 's', d + '/s.json', prog, d + '/r.json'], capture_output=True, text=True).returncode == 0
T = lambda k: EMB[k][1]
for n_, want in (('0', 0), ('2', 1), ('3', 1)):
    check('cloud-init program accepts the ACTUAL closed a0011 case %s status+result bytes (owner errors %d)' % (n_, want), cinit(T('case-%s.status.json' % n_), T('case-%s.result.json' % n_), want))
check('refuses case 0 files with want=1', not cinit(T('case-0.status.json'), T('case-0.result.json'), 1))
check('refuses case 2 files with want=0', not cinit(T('case-2.status.json'), T('case-2.result.json'), 0))
check('refuses status/result swapped between case 0 and case 2', not cinit(T('case-2.status.json'), T('case-0.result.json'), 1) and not cinit(T('case-0.status.json'), T('case-2.result.json'), 0))

S2, R2 = json.loads(T('case-2.status.json')), json.loads(T('case-2.result.json'))
E1 = ERR
STG = ['init', 'init-local', 'modules-config', 'modules-final']
def concat(s): return [e for k in STG for e in s['v1'][k]['errors']]
def case(mutate_status=None, mutate_result=None, recompute=False, want=1):
    s, r = copy.deepcopy(S2), copy.deepcopy(R2)
    if mutate_status: mutate_status(s)
    if recompute: r['v1']['errors'] = concat(s)
    if mutate_result: mutate_result(r)
    return cinit(json.dumps(s), json.dumps(r), want)
def setall(s, errs):
    for k in STG: s['v1'][k]['errors'] = list(errs)
check('control: the re-serialized actual case 2 is accepted (mutation harness is sound)', case())
neg = [
 ('owner error text differs (all four lists alike)', dict(mutate_status=lambda s: setall(s, ['x']), recompute=True)),
 ('owner has no error but want is 1', dict(mutate_status=lambda s: setall(s, []), recompute=True)),
 ('duplicate: owner (and all stages) carry two entries', dict(mutate_status=lambda s: setall(s, [E1, E1]), recompute=True)),
 ('non-owner stage carries a different error', dict(mutate_status=lambda s: s['v1']['init'].__setitem__('errors', ['other']), recompute=True)),
 ('non-aliased: only the owner has the error (no future-compat alternative)', dict(mutate_status=lambda s: [s['v1'][k].__setitem__('errors', []) for k in STG[:3]], recompute=True)),
 ('non-aliased: owner empty, a non-owner has the error', dict(mutate_status=lambda s: (s['v1']['modules-final'].__setitem__('errors', []), s['v1']['init'].__setitem__('errors', [E1])), recompute=True)),
 ('a second stage started (start+finished set)', dict(mutate_status=lambda s: s['v1']['init'].update(start=1.0, finished=2.0))),
 ('a second stage has only a finished time', dict(mutate_status=lambda s: s['v1']['init-local'].update(finished=2.0))),
 ('a second stage has only a start time', dict(mutate_status=lambda s: s['v1']['modules-config'].update(start=2.0))),
 ('stage not cleared', dict(mutate_status=lambda s: s['v1'].update(stage='modules-final'))),
 ('result trimmed to one entry (unique/dedup)', dict(mutate_result=lambda r: r['v1'].update(errors=r['v1']['errors'][:1]))),
 ('result has an extra entry', dict(mutate_result=lambda r: r['v1']['errors'].append('extra'))),
 ('result datasource differs from status', dict(mutate_result=lambda r: r['v1'].update(datasource='x'))),
 ('result has an extra top-level key', dict(mutate_result=lambda r: r['v1'].update(x=1))),
 ('result lacks datasource', dict(mutate_result=lambda r: r['v1'].pop('datasource'))),
 ('owner finished before start', dict(mutate_status=lambda s: s['v1']['modules-final'].update(start=200.0, finished=100.0))),
 ('owner start is 0', dict(mutate_status=lambda s: s['v1']['modules-final'].update(start=0))),
 ('owner start is negative', dict(mutate_status=lambda s: s['v1']['modules-final'].update(start=-1.0))),
 ('owner start is a string', dict(mutate_status=lambda s: s['v1']['modules-final'].update(start='115.23'))),
 ('owner start is a boolean', dict(mutate_status=lambda s: s['v1']['modules-final'].update(start=True))),
 ('owner start is null', dict(mutate_status=lambda s: s['v1']['modules-final'].update(start=None))),
 ('owner finished is null', dict(mutate_status=lambda s: s['v1']['modules-final'].update(finished=None))),
 ('owner finished is a string', dict(mutate_status=lambda s: s['v1']['modules-final'].update(finished='115.25'))),
 ('an extra stage key', dict(mutate_status=lambda s: s['v1'].update({'modules-extra': copy.deepcopy(s['v1']['init'])}))),
 ('a missing stage key', dict(mutate_status=lambda s: s['v1'].pop('init-local'))),
 ('an extra top-level v1 key', dict(mutate_status=lambda s: s['v1'].update(x=1))),
 ('a stage record with an extra key', dict(mutate_status=lambda s: s['v1']['init'].update(x=1))),
 ('a stage record missing recoverable_errors', dict(mutate_status=lambda s: s['v1']['init'].pop('recoverable_errors'))),
 ('recoverable_errors is a list', dict(mutate_status=lambda s: s['v1']['init'].update(recoverable_errors=[]))),
 ('errors is a string', dict(mutate_status=lambda s: s['v1']['init'].update(errors=E1))),
 ('owner errors is null', dict(mutate_status=lambda s: s['v1']['modules-final'].update(errors=None))),
]
for label, kw in neg:
    check('cloud-init program refuses: ' + label, not case(**kw))
check('cloud-init program refuses a different owner string even if equal in all lists and the result', not cinit(T('case-2.status.json'), T('case-2.result.json'), 1, err="('scripts-user', RuntimeError('Runparts: 2 failures (part-001) in 1 attempted commands'))"))
c0 = json.loads(T('case-0.status.json')); c0r = json.loads(T('case-0.result.json'))
check('case 0: a non-empty owner error list is refused', not cinit(json.dumps({**c0, 'v1': {**c0['v1'], 'modules-final': {**c0['v1']['modules-final'], 'errors': [E1]}}}), json.dumps({'v1': {'datasource': None, 'errors': [E1]}}), 0))
check('case 0: all four non-empty aliased lists are refused when 0 is expected', not cinit(json.dumps(S2), json.dumps(R2), 0))

# multi-document / empty inputs: without -s and the length guards, $s[0] (or an earlier false result document) could be ignored
GS, GR = T('case-2.status.json'), T('case-2.result.json')
BS = json.dumps({**S2, 'v1': {**S2['v1'], 'stage': 'modules-final'}})                  # a status document that alone is refused
BR = json.dumps({'v1': {'datasource': None, 'errors': R2['v1']['errors'][:1]}})          # a result document that alone is refused
check('control: the single bad status / bad result documents are refused on their own', not cinit(BS, GR, 1) and not cinit(GS, BR, 1))
for label, st, rs in [('status: good then an identical second document', GS + '\n' + GS, GR), ('status: good then a bad second document', GS + '\n' + BS, GR),
                      ('status: bad then good (the good one is never the one indexed)', BS + '\n' + GS, GR), ('status: good then null', GS + '\nnull', GR),
                      ('status: good then trailing garbage', GS + '\n{', GR), ('status: empty file', '', GR), ('status: whitespace only', ' \n', GR),
                      ('result: good then an identical second document', GS, GR + '\n' + GR), ('result: bad then good (earlier false, later true)', GS, BR + '\n' + GR),
                      ('result: good then bad', GS, GR + '\n' + BR), ('result: good then null', GS, GR + '\nnull'), ('result: good then trailing garbage', GS, GR + '\n{'),
                      ('result: empty file', GS, ''), ('result: whitespace only', GS, ' \n'), ('both: two good documents each', GS + '\n' + GS, GR + '\n' + GR)]:
    check('cloud-init program refuses multi-document/empty input - ' + label, not cinit(st, rs, 1))
check('control: the exact good pair is still accepted after the multi-document negatives', cinit(GS, GR, 1))

print('cases passed: %d, failed: %d' % (passed, len(fails)))
sys.exit(1 if fails else 0)
