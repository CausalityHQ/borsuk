from pathlib import Path
import hashlib,subprocess,sys,re,json
D=Path('/tmp/borsuk-next1m-pid128-repair-draft/source')
assert len(sys.argv)==2 and re.fullmatch('[0-9a-f]{40}',sys.argv[1])
revision=sys.argv[1]
inputs={}
for name in ['next1m-canary-worker.sh','next1m-canary-watch.sh','next1m-canary-freeze.py','next1m-canary-launch-once.sh','collector-smoke.sh','collect-native-chain-outer.sh','observer-command.sh']:
 path='docs/research/performance-architecture-20260930/next1m-resource-gate/derivation/runtime-support.pending/'+name
 body=subprocess.run(['git','show',revision+':'+path],cwd='/home/rb/worktrees/borsuk-prod-ready-v9',capture_output=True,check=True,timeout=30).stdout
 assert body==(D/name).read_bytes(),name
 inputs[name]=hashlib.sha256(body).hexdigest()
worker=(D/'next1m-canary-worker.sh').read_text()
a=worker.index('for spec in \\\n'); b=worker.index('; do',a)+len('; do')
names=['collector-smoke.sh','collect-native-chain-outer.sh','observer-command.sh']
specs=[]
for n in names:
 body=(D/n).read_bytes();specs.append("  '"+n+':'+str(len(body))+':'+hashlib.sha256(body).hexdigest()+"'")
worker=worker[:a]+'for spec in \\\n'+' \\\n'.join(specs)+'; do'+worker[b:]
a=worker.index('mkdir "$support/bin"\n');b=worker.index('phase=canary\n',a);worker=worker[:a]+worker[b:]
old='/usr/bin/taskset -c 0 /usr/bin/timeout -k 10 1500 /bin/bash "$support/wrapper-canary.sh" "$support/run_native_scale_build_gate.sh" "$support/bin" /mnt/borsuk-pid-evidence/canary'
new='/usr/bin/taskset -c 0 /usr/bin/timeout -k 10 300 /bin/bash "$support/collector-smoke.sh" "$support/collect-native-chain-outer.sh" "$support/observer-command.sh" /mnt/borsuk-pid-evidence/collector-smoke'
assert worker.count(old)==1;worker=worker.replace(old,new)
worker=worker.replace('RuntimeMaxSec=1510','RuntimeMaxSec=310').replace('SECONDS+1520','SECONDS+320').replace('+1690 <=','+490 <=')
worker=worker.replace('/mnt/borsuk-pid-evidence/canary/result.json','/mnt/borsuk-pid-evidence/collector-smoke/result.json').replace('WRAPPER_CHECKS_VERIFIED','COLLECTOR_MECHANICS_VERIFIED').replace('WRAPPER_CANARY_ROOT_REPLAY_REQUIRED','COLLECTOR_SMOKE_ROOT_REPLAY_REQUIRED').replace('CLI_USAGE_ONLY_PENDING_ROOT_REPLAY','SYNTHETIC_COLLECTOR_ONLY_PENDING_ROOT_REPLAY').replace('native_cli_usage_only:true','native_cli_usage_only:false,synthetic_chain_metadata:true')
watch=(D/'next1m-canary-watch.sh').read_text().replace('.native_cli_usage_only==true','.native_cli_usage_only==false and .synthetic_chain_metadata==true').replace('CLI_USAGE_ONLY_PENDING_ROOT_REPLAY','SYNTHETIC_COLLECTOR_ONLY_PENDING_ROOT_REPLAY')
freeze=(D/'next1m-canary-freeze.py').read_text().replace("{'wrapper-canary.sh','run_native_scale_build_gate.sh','verify-closed.py'}","{'collector-smoke.sh','collect-native-chain-outer.sh','observer-command.sh'}").replace('READY_DISPOSABLE_SUPERVISOR_CANARY','READY_DISPOSABLE_COLLECTOR_SMOKE')
launch=(D/'next1m-canary-launch-once.sh').read_text();assert launch.count('canary-a0004-')==1;launch=launch.replace('canary-a0004-','canary-a0005-')
# Pending adapter only; no launch/freeze/native fixture execution.
outputs={}
for name,body in [('next1m-collector-worker.sh',worker),('next1m-collector-watch.sh',watch),('next1m-collector-freeze.py',freeze),('next1m-collector-launch-once.sh',launch)]:
 (D/name).write_text(body);print(name,len(body.encode()),hashlib.sha256(body.encode()).hexdigest())
 outputs[name]={'bytes':len(body.encode()),'sha256':hashlib.sha256(body.encode()).hexdigest()}
assert 'support/bin' not in worker and 'wrapper-canary.sh' not in worker and '1500 /bin/bash' not in worker
(D/'collector-canary-adapter-source.json').write_text(json.dumps({'status':'SOURCE_ONLY_ADAPTER_RUNTIME_UNVERIFIED','input_revision':revision,'inputs':inputs,'outputs':outputs,'new_controller_framework':False,'ann_execution':False,'native_binary_downloads':False,'staging_timeout_seconds':300,'parent_runtime_seconds':310,'parent_timeout_stop_seconds':10,'launch_authority':False,'performance_claim':False},indent=2)+'\n')
