import json, os, sys, time
from pathlib import Path
repo = Path('/home/rb/worktrees/borsuk-fine-sq8-scratch-admission-repair')
sys.path.insert(0, str(repo))
config = json.loads((repo/'docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/paired100k/a0001/screen/config.json').read_text())
forbidden = {config['execution']['binary']['path']}
for d in config['execution']['inputs'].values():
    forbidden.update(p['path'] for c in ('layout','original') for p in d[c].values())
    forbidden.update(d[n]['path'] for n in ('requests64','truth64'))
forbidden.add(str(repo/config['qualification_transport']['binaries/hierarchical_semantic_cells']['path']))
violations = []
def audit(event, args):
    message = None
    if event == 'socket.connect': message = 'network connect'
    if event == 'subprocess.Popen':
        cmd = args[1]
        if any(Path(str(c)).name in ('cargo','rustc','aws','curl','wget') for c in cmd): message = str(cmd)
    if event == 'open' and isinstance(args[0], (str, bytes)):
        p = os.fsdecode(args[0]); mode = args[1] or ''
        if p in forbidden and ('r' in mode or args[2] & os.O_ACCMODE == os.O_RDONLY): message = 'cold input read: '+p
    if message:
        violations.append(message)
        raise AssertionError(message)
sys.addaudithook(audit)
group = Path('/sys/fs/cgroup'+Path('/proc/self/cgroup').read_text().strip().split(':')[-1])
before = {n:(group/n).read_text().strip() for n in ('memory.max','memory.swap.max','cpu.max')}
assert before == {'memory.max':'268435456','memory.swap.max':'0','cpu.max':'100000 100000'}, before
assert list(os.sched_getaffinity(0)) == [0]
from scripts import launch_hierarchical_cells_100k_spot as launcher
from scripts import run_cell_overlap_pair as helper
receipt = dict(kernel_limits=before, cpu_affinity=[0], source_only=True, started=time.time(),checks={})
for name, check in [('scratch',launcher.fine_scratch_self_check), ('fine_helper',helper.fine_self_check), ('fine_launcher',launcher.fine_launcher_self_check), ('legacy_helper',helper.self_check), ('legacy_launcher',launcher.overlap_self_check)]:
    started = time.monotonic()
    value = check()
    receipt['checks'][name] = dict(passed=True,seconds=time.monotonic()-started,result=value)
    print('CHECK CLOSED '+name,flush=True)
    Path('/tmp/borsuk-fine-scratch-parent-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
receipt.update(finished=time.time(),violations=violations,kernel_peak={n:(group/n).read_text().strip() for n in ('memory.peak','memory.swap.peak','memory.events')})
assert not violations,violations
Path('/tmp/borsuk-fine-scratch-parent-receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
print('PASS bounded source-only gate; actual kernel limits, no native/data/GT hydration or network',flush=True)
