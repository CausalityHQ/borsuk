#!/usr/bin/env python3
"""Pure config/header/source checks. No payloads, native execution, or network."""
import ast, copy, json
from pathlib import Path
import prepare as p
before=(p.E/'qualified-native-authority.json').read_bytes()
a=p.root_authority()
files=p.files_to_create()
assert 'qualified-native-authority.json' not in files
assert json.loads(files['preparation.json'])['native_authority_sha256']==p.sha(before)
assert (p.E/'qualified-native-authority.json').read_bytes()==before
assert a['instance_id']=='i-09f8571d0edf296e9'
assert p.retained()['instance_id']=='i-0abcae7251af31318'
checks=0
for name in p.NAMES:
 raw,expected=p.make_cell(name,a);c=json.loads(raw);e=json.loads(expected)
 assert c['count']==1000 and c['max_active_queries']==p.active(name)
 assert c['execution']==({'mode':'full'} if name in p.MEASURED else {'mode':'diagnostic_panel','ordinals':list(range(p.count(name))),'trace':False})
 assert c['query_runtime']=='multi_thread2' and c['fetch_parallelism']==64
 assert c['source_authentication']=='offload2' and c['source_cache_bytes']==0 and c['semantic_leaf_limit']==64
 assert c['serving']['shortlist']==16 and c['serving']['mode']=='fp32_rerank'
 assert e['identity']['config_sha256']==p.sha(raw) and e['identity']['binary_sha256']==p.RUNNER
 assert e['bound_inputs']['selected_count']==p.count(name)
 assert e['identity']['execution']==e['bound_inputs']['execution']==c['execution']
 assert set(e)=={'identity','bound_inputs'} # Startup/model remains the reader's computation.
 original=json.loads((p.E0/f'Q{p.active(name)}-config.json').read_bytes())
 assert {k for k in c if c[k]!=original[k]}=={'execution','scratch_parent'}
 template=json.loads((p.E0/f'Q{p.active(name)}-expected.UNBOUND.json').read_bytes())
 assert {k for k in e['identity'] if e['identity'][k]!=template['identity'][k]}=={'binary_sha256','runner_source_sha256','config_sha256','execution'}
 assert {k for k in e['bound_inputs'] if e['bound_inputs'][k]!=template['bound_inputs'][k]}=={'execution','selected_count'}
 assert raw==p.make_cell(name,a)[0] and expected==p.make_cell(name,a)[1]
 checks+=1
for names in [('admit-A','admit-B'),('canary-A','canary-B'),('A1','B1'),('A2','B2')]:
 x,y=[json.loads(p.make_cell(n,a)[0]) for n in names]
 assert {k for k in x if x[k]!=y[k]}=={'scratch_parent','max_active_queries'}
 checks+=1
for n in ['prepare.py','run-full1000.py','selfcheck.py']:
 ast.parse((p.P/n).read_text(),filename=n);checks+=1
assert 4*(60+5+30+5)==400 and 4*(600+5+120+5)==2920
assert 4*2*20==160 and 400+2920+160+720==4200
s=(p.P/'run-full1000.py').read_text()
assert "if vp.exists()" not in s and "range(32)" not in s
assert "600 if name in MEASURED else 60" in s and "120 if name in MEASURED else 30" in s
assert "'--if-none-match','*'" in s and "'--checksum-mode','ENABLED'" in s
assert "'AWS_MAX_ATTEMPTS':'1'" in s and "timeout=20" in s and "--profile" not in s
assert "'query_process_cpu_ns'] is None" in s and "'transport'] is None" in s
launch=(p.P/'launch.sh').read_text()
for token in ['1792443900','AllowedCPUs=0-7','CPUQuota=800%','MemoryMax=8G','MemorySwapMax=0','TasksMax=1024','RuntimeMaxSec=4200','KillMode=control-group','OOMPolicy=stop']:
 assert token in launch,token
controller=(p.P/'run-controller.sh').read_text()
assert 'cargo-target.lock' in controller and 'native-benchmark.lock' in controller
assert 'flock -n 9' in controller and 'flock -n 8' in controller
assert 'AWS_SHARED_CREDENTIALS_FILE=/dev/null' in controller
print(json.dumps({'status':'PASS','metadata_cases':checks,'payloads_opened':False,'native_executed':False}))
