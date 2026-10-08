import json,hashlib
from pathlib import Path
r=Path('/data/target/borsuk-cold-membership-native/direct-closure-canary-a0001');p=json.loads((r/'protocol.json').read_text());t=json.loads((r/'terminal.json').read_text());col=r/'collected';i=json.loads((r/'launch.json').read_text())['Instances'][0]['InstanceId']
assert t['instance_id']==i and t['schema']=='borsuk-direct-closure-native-canary-closed-v1';assert t['exit']==t['original_exit']==0 and t['native_exit']==2 and t['performance_claim'] is False;assert t['binary_source_commit']==p['candidate'];assert json.loads((r/'wait.json').read_text())['exit']==0
for line in (r/'artifacts.sha256').read_text().splitlines():
 sha,name=line.split('  ',1);assert '..' not in Path(name).parts;assert hashlib.sha256((col/name.removeprefix('./')).read_bytes()).hexdigest()==sha
for n in ['config.json','stage.sh']:assert (r/n).read_bytes()==(col/n).read_bytes()
for n,v in {'cpu.max':'100000 100000','memory.max':'536870912','memory.swap.max':'0','pids.max':'256'}.items():assert (col/(n+'.before')).read_text().strip()==v
assert int((col/'memory.peak.after').read_text())<=536870912 and (col/'memory.swap.peak.after').read_text().strip()=='0'
events=dict(line.split() for line in (col/'memory.events.after').read_text().splitlines());assert events['oom']==events['oom_kill']=='0';assert not (col/'scratch-files.txt').read_text().strip()
assert (col/'native-exit').read_text().strip()=='2' and (col/'final-exit').read_text().strip()=='0'
for n in ['pins-before.txt','pins-after.txt']:
 lines=(col/n).read_text().splitlines();assert len(lines)==5 and all(x.endswith(': OK') for x in lines)
records=[];raw=(col/'canary-result.jsonl').read_text();decoder=json.JSONDecoder()
while raw.strip():
 value,end=decoder.raw_decode(raw.lstrip());records.append(value);raw=raw.lstrip()[end:]
assert len(records)==2 and records[0]['phase']=='identity' and records[1]['phase']=='terminal';s=records[1]['summary'];assert s['status']=='INVALID' and s['stage']=='config' and s['error']=='artifact identity/SHA' and s['completed_queries']==0 and not s['truth_opened'] and not s['all_queries_sealed'] and s['sum']['submitted_gets']==s['binding_charge']['submitted_gets']==0
assert records[0]['schema']=='borsuk-cohere-native-baseline-result-v3' and records[0]['binary_sha256']=='67c7eb51e3ff6bb9b77c1c9f12cac3b402501b708d91bd6d95ea20b029a8ab0b' and records[0]['config_sha256']=='0'*64
assert t['qualified_source_identity_sha256']=='4d04840e2db4487f9e77ef57e5eadef42684ab3d354c1a51c2adc20a8c3474e9'
state=(col/'systemd-after.txt').read_text();assert 'MainPID=0' in state and 'ActiveState=inactive' in state
v={'status':'DISPOSABLE_CANARY_GO','instance_id':i,'terminated':True,'native_exit':2,'wrapper_exit':0,'zero_queries_GETs_truth':True,'performance_claim':False,'native_query_S3_SDK_exercised_here':False,'prior_real_input_admission_sha256':p['prior_real_admission']};(r/'independent-verification.json').write_text(json.dumps(v,indent=2)+'\n');print(json.dumps(v))
