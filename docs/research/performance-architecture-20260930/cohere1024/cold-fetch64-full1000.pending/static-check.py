from pathlib import Path
import importlib.util,json,hashlib,subprocess,ast
c=Path(__file__).resolve().parent
ast.parse((c/'run-prepared.py').read_text())
for f in ['aws-bounded.sh','launch-root.sh']:subprocess.run(['bash','-n',str(c/f)],check=True)
spec=importlib.util.spec_from_file_location('packet',c/'run-prepared.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
cases=m.selfcheck()
frozen={}
for name,(cfg,x,h) in cases.items():
 frozen[name]={'config_sha256':h,'expected_sha256':hashlib.sha256((json.dumps(x,sort_keys=True,separators=(',',':'))+'\n').encode()).hexdigest(),'cached':False,'arm':name[-1] if '-' in name else name[0]}
 if name in ['A1','B1','B2','A2']:
  assert cfg['execution']=={'mode':'full'} and x['prospective_startup']['diagnostic_pinned_bytes']==0
  assert x['prospective_startup']['caller_pinned_bytes']==7010842
  assert x['prospective_startup']['fp32_modeled_memory_bytes']==(471434981 if name[0]=='A' else 471043301)
assert len(frozen)==8
try:m.bound_protocol()
except AssertionError as e:assert str(e).startswith('HOLD:')
else:raise AssertionError('HOLD accepted')
out={'status':'PASS','frozen_cases':frozen,'native_executed':False,'payloads_read':False,'cases':8}
(c/'STATIC-CHECKS.json').write_text(json.dumps(out,indent=2,sort_keys=True)+'\n')
print('PASS 8 frozen cases; Full model pins; shell/Python syntax; HOLD rejection')
