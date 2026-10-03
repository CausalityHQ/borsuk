import hashlib, json, pathlib, sys, time

root = pathlib.Path('/home/rb/worktrees/borsuk-global-leaf-staging-timeout-fix')
sys.path.insert(0, str(root))
from scripts import launch_hierarchical_cells_100k_spot as c

out = pathlib.Path(sys.argv[1])
out.mkdir(exist_ok=False)
started = time.monotonic()
group = pathlib.Path('/sys/fs/cgroup') / pathlib.Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1].lstrip('/')
def resources():
    return {n: (group/n).read_text().strip() for n in ('memory.max', 'memory.swap.max', 'memory.peak', 'memory.swap.peak', 'memory.events', 'cpu.max')}
before = resources()
assert before['memory.max'] == str(256 << 20) and before['memory.swap.max'] == '0'
quota, period = before['cpu.max'].split()
assert quota != 'max' and int(quota) <= int(period)
config = json.loads((root / 'docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/paired100k/global-leaf-probe/config.json').read_text())
config['authority_pending'] = False
config['code_sha256'] = {p: c.local.identity(root/p)['sha256'] for p in c.PROBE_CODE}
evidence = c.probe.original_evidence(root, config)
roles = {r: c.probe.qualify_role(r, config['roles'][r], root) for r in c.probe.ROLE_NAMES}
fixtures = json.loads(pathlib.Path('/data/orchestration/borsuk-global-leaf-real-request-fixture-20261003/receipt.json').read_text())
panels = {}
for item in fixtures['datasets']:
    name = item['dataset']
    old = evidence['items'][name]
    hashes, aggregate = c.probe.request_hashes(item['requests'], old['diagnostic']['requests'])
    assert aggregate == old['query_f32_sha256'] and len(hashes) == 64
    for trace in old['traces']:
        for key in ('primary_ids', 'covered_ids'):
            ids = trace[key]
            assert len(ids) == len(set(ids)) and all(type(v) is int and 0 <= v < 100000 for v in ids)
        assert set(trace['primary_ids']) <= set(trace['covered_ids'])
    panels[name] = dict(requests=item['requests'], query_f32_sha256=aggregate, queries=64)
assert set(panels) == {'relaion', 'cohere'}
proof = dict(code_identity_sha256=c.ids.sha(c.ids.encoded(config['code_sha256'])),
    refs_identity_sha256=c.ids.sha(c.ids.encoded(dict(evidence=config['evidence'],
        roles={r: config['roles'][r]['refs'] for r in c.probe.ROLE_NAMES}))),
    native_identity_sha256=c.ids.sha(c.ids.encoded(roles)))
result = dict(schema='borsuk-global-leaf-cheap-real-input-result-v1', panels=panels,
    historical_control_schema_admitted=True, original_root_authority_pins_authenticated=True,
    truth_opens=0, ann_queries=0, native_processes=0, selection_receipts=0,
    source_sha256=config['code_sha256'], cgroup_before=before, cgroup_after=resources(),
    wall_seconds=time.monotonic()-started)
assert int(result['cgroup_after']['memory.swap.peak']) == 0
events = dict(line.split() for line in result['cgroup_after']['memory.events'].splitlines())
assert events['oom'] == events['oom_kill'] == '0'
body = c.local.canonical(result)
(out/'result.json').write_bytes(body)
config['admission'] = dict(schema='borsuk-global-leaf-real-no-gt-admission-v2', authority_pending=False,
    source_bound_real_no_gt=True, config_sha256=c.local.sha(c.local.canonical({k:v for k,v in config.items() if k != 'admission'})),
    result_sha256=hashlib.sha256(body).hexdigest(), **proof)
(out/'config-draft.json').write_bytes(c.local.canonical(config))
print(json.dumps(dict(output=str(out), result_sha256=config['admission']['result_sha256'],
    queries=128, truth_opens=0, native_processes=0, wall_seconds=result['wall_seconds'])))
