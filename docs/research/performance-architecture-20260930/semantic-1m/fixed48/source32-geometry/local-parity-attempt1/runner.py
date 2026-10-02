import hashlib, json, os, pathlib, signal, subprocess, sys, time

root = pathlib.Path('/home/rb/worktrees/borsuk-prod-ready-v9')
out = pathlib.Path(__file__).parent
os.chdir(root)
sys.path.insert(0, str(root))
from scripts.run_native_semantic_router_cold import native_inventory, encoded

def sha(body):
    return hashlib.sha256(body).hexdigest()

def save(name, value):
    with (out/name).open('x') as f:
        json.dump(value, f, sort_keys=True, indent=2)
        f.write('\n')

def identity(path):
    body = path.read_bytes()
    return dict(path=str(path), bytes=len(body), sha256=sha(body))

with (out/'original-attempt').open('x') as f:
    f.write(str(time.time_ns())+'\n')
source = json.loads((out/'source-audit.json').read_text())
before = native_inventory()
assert before == source['native_inventory']
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == source['base_revision']
assert not subprocess.check_output(['git', 'status', '--porcelain'])
save('source-before.json', before)
cg = pathlib.Path('/sys/fs/cgroup') / pathlib.Path(pathlib.Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1]).relative_to('/')

def resources():
    names = ['memory.max', 'memory.current', 'memory.peak', 'memory.swap.max', 'memory.swap.current',
             'memory.swap.peak', 'memory.events', 'memory.swap.events', 'cpu.max', 'cpu.stat',
             'pids.max', 'pids.current', 'pids.peak', 'pids.events', 'cgroup.procs']
    values = {n: (cg/n).read_text().strip() for n in names if (cg/n).exists()}
    values['host_memory_pressure'] = pathlib.Path('/proc/pressure/memory').read_text()
    values['host_meminfo'] = pathlib.Path('/proc/meminfo').read_text()
    values['time_ns'] = time.time_ns()
    return values

initial = resources()
assert cg.name == 'borsuk-source32-parity-20261002.scope'
assert initial['memory.max'] == '8589934592' and initial['memory.swap.max'] == '0'
assert initial['cpu.max'].split() == ['200000', '100000'] and initial['pids.max'] == '512'
save('resources-before.json', initial)
env = {k: os.environ[k] for k in ['PATH', 'HOME', 'USER', 'LOGNAME', 'LANG', 'CARGO_HOME', 'RUSTUP_HOME'] if k in os.environ}
env.update(CARGO_TARGET_DIR='/data/target', CARGO_BUILD_JOBS='1', CARGO_INCREMENTAL='0',
           RUSTC_WRAPPER='', RUSTC_WORKSPACE_WRAPPER='', RAYON_NUM_THREADS='2',
           TOKIO_WORKER_THREADS='2', RUST_TEST_THREADS='1', OMP_NUM_THREADS='2', OPENBLAS_NUM_THREADS='2', MKL_NUM_THREADS='2')
command = ['cargo','test','--locked','-p','borsuk','--lib','two_bit_generation::source_walk_tests::fragmented_paged_source_preserves_trace_and_rank_across_get_caps','--','--exact']
save('invocation.json', dict(command=command, environment=env, cwd=str(root), cgroup=str(cg),
                           rustc=subprocess.check_output(['rustc', '-Vv'], env=env, text=True),
                           runner=identity(pathlib.Path(__file__))))
start = time.time_ns()
reason = None
pressure_started = None
sampled_memory = sampled_swap = 0
print(json.dumps(dict(phase='start', command=command, scope=str(cg), source=source['native_source_identity_sha256'])), flush=True)
with (out/'parity.log').open('xb') as log, (out/'resources.jsonl').open('x') as samples:
    child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, env=env, start_new_session=True)
    save('active.json', dict(cargo_pid=child.pid, runner_pid=os.getpid(), scope=str(cg), start_ns=start))
    while child.poll() is None:
        full = float(pathlib.Path('/proc/pressure/memory').read_text().split('full avg10=', 1)[1].split()[0])
        memory = int((cg/'memory.current').read_text())
        swap = int((cg/'memory.swap.current').read_text())
        sampled_memory = max(sampled_memory, memory)
        sampled_swap = max(sampled_swap, swap)
        now = time.monotonic()
        pressure_started = (pressure_started if pressure_started is not None else now) if full >= 1 else None
        samples.write(json.dumps(dict(time_ns=time.time_ns(), memory_bytes=memory, swap_bytes=swap,
                                      host_full_avg10=full, pressure_seconds=0 if pressure_started is None else now-pressure_started))+'\n')
        samples.flush()
        if (pressure_started is not None and now-pressure_started >= 30) or time.time_ns()-start >= 600_000_000_000:
            reason = 'host memory PSI full avg10 >= 1 sustained for at least 30 seconds' if pressure_started is not None and now-pressure_started >= 30 else '600-second narrow gate wall cap'
            (out/'STOP').write_text(reason+'\n')
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
            break
        time.sleep(2)
    status = child.wait()
after = native_inventory()
save('source-after.json', after)
final = resources()
save('resources-after.json', final)
receipt = dict(command=command, environment=env, cwd=str(root), cgroup=str(cg), start_ns=start,
               end_ns=time.time_ns(), status=status, stop_reason=reason, source_unchanged=before==after,
               native_source_identity_before=sha(encoded(before).encode()), native_source_identity_after=sha(encoded(after).encode()),
               resources_before=initial, resources_after=final, sampled_memory_peak_bytes=sampled_memory,
               sampled_swap_peak_bytes=sampled_swap, log=identity(out/'parity.log'))
save('parity-receipt.json', receipt)
print(json.dumps(dict(phase='terminal', status=status, stop_reason=reason, source_unchanged=before==after,
                     memory_peak=final.get('memory.peak'), swap_peak=final.get('memory.swap.peak'))), flush=True)
assert before == after, 'source changed during build'
if status != 0 or reason:
    sys.exit(status if status > 0 else 1)
log = (out/'parity.log').read_text()
assert '1 passed; 0 failed;' in log, 'exact test not executed'
print(json.dumps(dict(phase='qualified', test_count=1)),flush=True)
