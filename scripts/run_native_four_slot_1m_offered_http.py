"""AWS-only same-panel offered HTTP with two slots and CLOSED native references."""
import atexit,json,os,resource,struct,subprocess,sys
from pathlib import Path
from scripts.run_native_current_1m_offered_http import run,sha,write

config_path,digest,out,binaries,prefix=sys.argv[1:];out,binaries=Path(out),Path(binaries)
config=json.loads(Path(config_path).read_text());assert sha(config_path)==digest
assert config['schema']=='borsuk-four-slot-1m-offered-http-dev-v1' and config['setting_order']==[10,100,100,10] and config['query_slots']==4
assert (config['rows'],config['dimensions'],config['first'],config['count'],config['offered_qps'])==(1000000,768,0,64,8) and not config['fresh_cohort_used']
assert os.environ['BORSUK_NATIVE_MEMORY_BYTES']=='1073741824'
for name,expected in config['dependencies'].items():assert sha(name)==expected,name
out.mkdir();config['measurement_prefix']=prefix
def resources():
    group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
    write(out/'cgroup.json',dict(cgroup={k:(group/k).read_text() for k in ['memory.max','memory.peak','memory.swap.max','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},address_space_limit=list(resource.getrlimit(resource.RLIMIT_AS)),cpu_affinity=sorted(os.sched_getaffinity(0))))
atexit.register(resources)
for role,ident in config['artifacts'].items():
    path=out/role;path.parent.mkdir(parents=True,exist_ok=True);subprocess.run(['aws','s3','cp','s3://'+config['bucket']+'/'+ident['key'],str(path),'--only-show-errors'],check=True)
    assert path.stat().st_size==ident['bytes'] and sha(path)==ident['sha256'],role
original=(out/'original-requests').read_bytes().splitlines(keepends=True);assert len(original)==1000
(out/'requests64.jsonl').write_bytes(b''.join(original[:64]));requests=[json.loads(row) for row in original[:64]];assert [r['query_ordinal'] for r in requests]==list(range(64))
gt=struct.unpack('<6400I',(out/'truth.u32').read_bytes());truth=[gt[q*100:(q+1)*100] for q in range(64)]
quality=json.loads((out/'native-quality.json').read_text());assert quality['native_quality']['10']['hits']==635 and quality['native_quality']['100']['hits']==6318 and quality['original_strict_diagnostic_preserved']=='KILL 1M consumed development quality' and not quality['fresh_cohort_used']
refs={k:[json.loads(row) for row in (out/('reference-k'+str(k)+'.jsonl')).read_text().splitlines()][1:-1] for k in [10,100]}
assert all(len(records)==64 and [r['query_ordinal'] for r in records]==list(range(64)) for records in refs.values())
runs=[]
for rep,k in enumerate(config['setting_order']):
    result=run(out,rep,k,config,binaries/'two_bit_http',config['closed_index_prefix']+'/k'+str(k),requests,refs[k],truth);runs.append(result)
    if k==10 and not result['development_gate_passed']:break
k10=[r for r in runs if r['k']==10];passed=len(k10)==2 and all(r['development_gate_passed'] for r in k10);failures={}
for r in k10:
    for outcome,count in r['outcomes'].items():
        if outcome!='success':failures[outcome]=failures.get(outcome,0)+count
write(out/'decision.json',dict(decision='GO current1M consumed development offered8QPS only' if passed else 'FAIL current1M consumed development offered8QPS',runs=runs,native_quality=quality['native_quality'],query_slots=4,actual_native_references_reused=True,failure_outcomes=failures,published_k10_p90_context_ms=444,original_strict_diagnostic_preserved=quality['original_strict_diagnostic_preserved'],fresh_cohort_used=False,qualification=False,matched_control_http_measured=False,matched_vendor_measured=False,lifecycle_cost_measured=False,all_physical_query_counters_complete=all(r['physical_counters_complete'] for r in runs)))
