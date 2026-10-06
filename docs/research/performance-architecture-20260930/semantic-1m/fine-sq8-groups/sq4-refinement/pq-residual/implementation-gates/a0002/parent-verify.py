import hashlib,json,sys
from pathlib import Path
base=Path('/home/rb/worktrees/borsuk-pq-residual-qualification-bundle-a0002')
sys.path.insert(0,str(base))
from scripts import launch_native_workspace_execution_spot as ctl
ctl.configure(pq_residual=True)
out=base/ctl.ROOT/'a0002'
result=ctl.replay(out)
t=json.loads((out/'aws-terminal.json').read_bytes())
before=json.loads((out/'source-before.json').read_bytes());after=json.loads((out/'source-after.json').read_bytes())
assert before==after
actual=ctl.worker.source_hashes(base)
assert actual==before, 'full frozen checkout source mismatch'
assert len(actual)==406
for n,p in t['artifacts'].items():
 b=(out/n).read_bytes();assert len(b)==p['bytes'] and hashlib.sha256(b).hexdigest()==p['sha256'],n
r=json.loads((out/'workspace-receipt.json').read_bytes())
assert len(r['stages'])==19 and all(s['exit_status']==s['gate_status']==s['log_exit_status']==0 for s in r['stages'])
assert result['qualified'] and result['exit_status']==0
proof={'schema':'borsuk-pq-residual-parent-native-verification-v1','verified':True,'result':result,'source_file_count':406,'artifact_count':len(t['artifacts']),'terminal_sha256':hashlib.sha256((out/'aws-terminal.json').read_bytes()).hexdigest(),'instance_id':t['instance_id'],'source_commit':t['source_commit'],'native_source_commit':t['native_source_commit'],'all19_stages_passed':True,'new_tests':11,'compile_fail_doc_tests':1,'same_instance_terminated':True,'actual_full_workspace_execution':False,'science_executed':False,'quality_or_performance_claim':False,'stages':r['stages']}
Path('/tmp/borsuk-pq-native-a0002-parent-verification.json').write_text(json.dumps(proof,sort_keys=True,indent=2)+'\n')
print(json.dumps({k:v for k,v in proof.items() if k!='stages'},sort_keys=True))
