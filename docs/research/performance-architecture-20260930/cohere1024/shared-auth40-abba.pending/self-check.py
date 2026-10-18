from pathlib import Path
import ast,json,hashlib,subprocess
p=Path(__file__).parent
def sha(f):return hashlib.sha256(f.read_bytes()).hexdigest()
for f in p.glob('*.py'):ast.parse(f.read_text())
for f in p.rglob('*.json'):json.loads(f.read_bytes())
for row in json.loads((p/'config-pins.unselected.json').read_bytes())['configs']:
 f=p/'configs'/row['name'];assert f.stat().st_size==row['bytes'] and sha(f)==row['sha256']
a=json.loads((p/'header-authority.json').read_bytes())
for stage in ('full','admission','smoke'):
 cs=[json.loads((p/'configs'/f'{stage}.{arm}.json').read_bytes()) for arm in ('A','B')]
 assert cs[0]['source_authentication']=='inline' and cs[1]['source_authentication']=='offload2'
 assert {k:v for k,v in cs[0].items() if k!='source_authentication'}=={k:v for k,v in cs[1].items() if k!='source_authentication'}
 c=cs[0];assert c['count']==1000 and c['semantic_leaf_limit']==40 and c['fetch_parallelism']==64
 assert c['s3_read_retry']=='one_retry_100ms' and c['scratch_parent']==str(p/'scratch0000')
 assert c['execution']==({'mode':'full'} if stage=='full' else {'mode':'diagnostic_panel','ordinals':[0] if stage=='admission' else [32,161],'trace':False})
 for key in ('requests','truth','cohort_receipt','derivation_receipt'):
  f=Path(c[key]['path']);assert f.stat().st_size==c[key]['bytes'] and sha(f)==c[key]['sha256']
for arm in ('A','B'):
 h=json.loads((p/'frozen-headers'/f'{arm}.json').read_bytes())
 assert h['expected_identity']['config_sha256']==sha(p/'configs'/f'full.{arm}.json')
 assert h['expected_identity']['binary_sha256']==a['binary_sha256'][arm]
 assert h['expected_bound_inputs']['selected_count']==1000 and h['expected_bound_inputs']['source_cache']=='off'
 assert h['expected_bound_inputs']['semantic_leaf_limit']==40
 assert sha(p/'artifacts'/f'check_cohere_native_baseline.{arm}')==a['binary_sha256'][arm]
assert sha(p/'artifacts/compare_native_replay')==a['strict_reader_sha256']
canonical=json.loads((p/'canonical/leaf40.json').read_bytes())
assert set(map(int,canonical['queries']))==set(map(int,canonical['hits']))==set(range(1000))
assert sum(canonical['hits'].values())==9737
assert canonical['sum']['submitted_gets']==127811 and canonical['sum']['verified_bytes']==34229466368
# Check the reused reducer's digest responds to score-bit/order changes, excluding unrelated timing/transport.
tree=ast.parse((p/'reduce-closed.py').read_text())
dig=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='dig')
ns={'json':json,'hashlib':hashlib};exec(compile(ast.Module(body=[dig],type_ignores=[]),'<digest>','exec'),ns)
assert ns['dig']({'returned':[{'score_bits':1},2]})!=ns['dig']({'returned':[2,{'score_bits':1}]})
assert ns['dig']({'returned':[{'score_bits':1}]})!=ns['dig']({'returned':[{'score_bits':2}]})
run=(p/'run-abba.sh').read_text();reduce=(p/'reduce-closed.py').read_text()
assert 'A1 B1 B2 A2' in run and 'arms=A' in run and 'arms=B' in run
assert 'canonical_equal_queries=1000' in reduce and "doc['cross_arm_parity']=cross" in reduce
assert "canonical['queries']" in reduce and "t['all_queries_sealed']" in reduce
assert 'leaf48' not in run and 'leaf48' not in reduce
assert run.index('compare_native_replay" --completed-scale')<run.index('checkpoint.sh" cell')
assert 'if-none-match' in (p/'checkpoint.sh').read_text()
print('PASS: Bash/ShellCheck/AST/JSON, fixed config+input+ELF/header pins, canonical1000 and parity digest checks; no runtime invoked')

import importlib.util,copy
spec=importlib.util.spec_from_file_location('resources',p/'check-resources.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
resource=json.loads((p/'resource-authority.json').read_bytes())
assert resource['extra_library_bytes']=={'A':0,'B':2380032}
for arm in 'AB':
 h=json.loads((p/'frozen-headers'/f'{arm}.json').read_bytes())
 sample=[h['expected_identity'],h['expected_bound_inputs'],dict(phase='startup',**resource['startup']['full'],source_authentication_extra_library_bytes=resource['extra_library_bytes'][arm])]
 module.validate(sample,'full',arm)
 for key in ['caller_pinned_bytes','query_scratch_bytes','source_authentication_extra_library_bytes','library_cap_bytes']:
  bad=copy.deepcopy(sample);bad[-1][key]+=1
  try:module.validate(bad,'full',arm)
  except AssertionError:pass
  else:raise AssertionError('resource mutation accepted: '+key)
print('PASS: both policy headers; exact resource values; cap/caller/scratch/worker one-byte mutations rejected')

for stage in ['admission','smoke']:
 for arm in 'AB':
  h=json.loads((p/'gate-headers'/f'{stage}.{arm}.json').read_bytes())
  sample=[h['expected_identity'],h['expected_bound_inputs'],dict(phase='startup',**resource['startup'][stage],source_authentication_extra_library_bytes=resource['extra_library_bytes'][arm]),dict(resource['diagnostic_admission'][stage],source_authentication_extra_library_bytes=resource['extra_library_bytes'][arm])]
  module.validate(sample,stage,arm)
  bad=copy.deepcopy(sample);bad[-1]['caller_pinned_bytes']+=1
  try:module.validate(bad,stage,arm)
  except AssertionError:pass
  else:raise AssertionError('diagnostic caller mutation accepted')
print('PASS: actual admission0 and separate smoke32,161 frozen resource/header checks for both arms')
