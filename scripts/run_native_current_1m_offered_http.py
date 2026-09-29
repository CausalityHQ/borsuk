"""AWS-only current ReLAION1M consumed development HTTP feasibility measurement."""
import atexit,hashlib,json,os,resource,signal,struct,subprocess,sys,time
from pathlib import Path
from scripts.run_native_union_http import connection,quantile,write
from scripts.run_native_union_offered_http import measure


def sha(path):
    digest=hashlib.sha256()
    with Path(path).open('rb') as source:
        for body in iter(lambda:source.read(1024*1024),b''):digest.update(body)
    return digest.hexdigest()


def plan_sha(records):
    return hashlib.sha256(''.join(json.dumps({k:r[k] for k in ['query_ordinal','ranges','planned_bytes']},sort_keys=True,separators=(',',':'))+'\n' for r in records).encode()).hexdigest()


def native(out,phase,args):
    with (out/(phase+'.log')).open('x') as log:
        subprocess.run(['/usr/bin/time','-v','-o',str(out/(phase+'.time')),'timeout','--signal=TERM','--kill-after=10','360',*map(str,args)],check=True,stdout=log,stderr=subprocess.STDOUT)


def run(out,rep,k,config,binary,index,requests,reference,truth):
    directory=out/('run'+str(rep)+'-k'+str(k));directory.mkdir()
    authority=dict(root_sha256=config['root_sha256'],generation=1,control_epoch=1)
    command=[str(binary),config['bucket'],config['region'],index,authority['root_sha256'],'1','1','127.0.0.1:8080']
    with (directory/'server.log').open('x') as log:
        server=subprocess.Popen(['/usr/bin/time','-v','-o',str(directory/'server.time'),'timeout','--signal=TERM','--kill-after=5','180',*command],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:
            deadline=time.monotonic()+45
            while True:
                if server.poll() is not None:raise ValueError('server closed before readiness')
                try:
                    client=connection()
                    try:
                        client.request('GET','/health');response=client.getresponse();ready=json.loads(response.read())
                    finally:client.close()
                    if response.status!=200 or ready['authority']!=authority or ready['dimensions']!=768:raise ValueError('head/readiness authority')
                    break
                except (OSError,ConnectionError):
                    if time.monotonic()>deadline:raise TimeoutError('server readiness')
                    time.sleep(.1)
            samples,result=measure(requests,reference,truth,authority,k=k,offered_qps=8,workers=8,timeout_seconds=5)
            if server.poll() is not None:raise ValueError('server terminated during measurement')
            with (directory/'http.jsonl').open('x') as stream:
                for row in samples:stream.write(json.dumps(row,sort_keys=True,allow_nan=False)+'\n')
            result.update(rep=rep,dataset='ReLAION',rows=1000000,dimensions=768,split='consumed external development0-63',authority=authority,metadata_resident=True,application_sq8_cache=False,fresh_cohort_used=False,matched_control_http_measured=False)
            if not result['identity_parity_valid']:raise ValueError('HTTP source/scorer/native reference integrity')
            result['development_gate_passed']=(result['successful_count']==64 and result['mean_offered_recall']>=.95 and result['successful_incoming_http_ms']['p90']<444 and result['achieved_successful_qps']>=8)
            write(directory/'result.json',result)
        finally:
            intentional_stop=server.poll() is None
            if intentional_stop:
                children=(Path('/proc')/str(server.pid)/'task'/str(server.pid)/'children').read_text().split()
                if len(children)!=1:raise ValueError('native timeout process authority')
                timeout_pid=int(children[0]);os.kill(timeout_pid,signal.SIGTERM)
                try:server.wait(timeout=8)
                except subprocess.TimeoutExpired:os.killpg(timeout_pid,signal.SIGKILL);server.wait(timeout=3)
            write(directory/'server-closeout.json',dict(returncode=server.returncode,intentional_stop=intentional_stop))
            if (directory/'result.json').exists():
                subprocess.run(['aws','s3','cp',str(directory),'s3://'+config['bucket']+'/'+config['measurement_prefix']+'/artifacts/screen/'+directory.name,'--recursive','--only-show-errors'],check=True)
    return result


def main():
    config_path,digest,out,binaries,prefix=sys.argv[1:];out,binaries=Path(out),Path(binaries)
    config=json.loads(Path(config_path).read_text());assert sha(config_path)==digest
    assert config['schema']=='borsuk-current-1m-offered-http-dev-v1' and config['setting_order']==[10,100,100,10]
    assert (config['rows'],config['dimensions'],config['first'],config['count'],config['offered_qps'])==(1000000,768,0,64,8)
    assert os.environ['BORSUK_NATIVE_MEMORY_BYTES']=='1073741824' and not config['fresh_cohort_used']
    config['measurement_prefix']=prefix
    for name,expected in config['dependencies'].items():assert sha(name)==expected,name
    out.mkdir();generation=out/'generation';generation.mkdir()
    def resources():
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        write(out/'cgroup.json',dict(cgroup={k:(group/k).read_text() for k in ['memory.max','memory.peak','memory.swap.max','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},address_space_limit=list(resource.getrlimit(resource.RLIMIT_AS)),cpu_affinity=sorted(os.sched_getaffinity(0))))
    atexit.register(resources)
    for role,ident in config['artifacts'].items():
        path=out/role;path.parent.mkdir(parents=True,exist_ok=True)
        subprocess.run(['aws','s3','cp','s3://'+config['bucket']+'/'+ident['key'],str(path),'--only-show-errors'],check=True)
        assert path.stat().st_size==ident['bytes'] and sha(path)==ident['sha256'],role
    original=(out/'original-requests').read_bytes().splitlines(keepends=True)
    assert len(original)==1000
    panel=out/'requests64.jsonl';panel.write_bytes(b''.join(original[:64]))
    requests=[json.loads(row) for row in panel.read_text().splitlines()]
    assert [r['query_ordinal'] for r in requests]==list(range(64))
    truth=[list(row) for row in zip(*[iter(struct.unpack('<6400I',(out/'truth.u32').read_bytes()))]*100)]
    assert len(truth)==64 and all(len(set(row))==100 and max(row)<1000000 for row in truth)
    closed=json.loads((out/'closed-quality.json').read_text())
    assert closed['decision']=='KILL 1M consumed development quality' and closed['root_sha256']['candidate']==config['root_sha256']
    assert closed['returned_hits']['candidate']==6318 and closed['flat_hits']==6363
    refs={};indexes={};native_quality={}
    for k in [100,10]:
        index=prefix+'/indexes/relaion/k'+str(k);indexes[k]=index;name='reference-k'+str(k)
        native(out,name,[binaries/'two_bit_plan_demo',generation,config['root_sha256'],panel,sha(panel),out/(name+'.jsonl'),0,64,'--live-s3',config['bucket'],config['region'],index,'--panel-count',64,'--top-k',k])
        records=[json.loads(row) for row in (out/(name+'.jsonl')).read_text().splitlines()]
        assert len(records)==66 and records[0]['root_sha256']==config['root_sha256'] and records[0]['generation']==records[0]['control_epoch']==1 and records[0]['top_k']==k and records[0]['declared_panel_count']==64 and records[-1]['count']==64
        refs[k]=records[1:-1]
        assert plan_sha(refs[k])==closed['plans_sha256']['candidate']
        for q,row in enumerate(refs[k]):
            assert row['query_ordinal']==q and row['ids']==closed['expected_ids']['candidate'][q][:k]
            assert len(set(row['ids']))==k and len(row['ranges'])==row['submitted_gets']<=32 and row['planned_bytes']==row['verified_bytes']<=16773120 and row['failed_gets']==0
        hits=sum(len(set(row['ids'])&set(truth[q][:k])) for q,row in enumerate(refs[k]))
        native_quality[str(k)]=dict(hits=hits,denominator=64*k,recall=hits/(64*k),actual_native_serving=True,offline_ordered_id_plan_parity=True)
    write(out/'native-quality.json',dict(dataset='ReLAION',rows=1000000,dimensions=768,split='consumed external development0-63',native_quality=native_quality,original_strict_diagnostic_preserved=closed['decision'],fresh_cohort_used=False,qualification=False,requests64_sha256=sha(panel)))
    runs=[]
    for rep,k in enumerate(config['setting_order']):
        result=run(out,rep,k,config,binaries/'two_bit_http',indexes[k],requests,refs[k],truth);runs.append(result)
        if k==10 and not result['development_gate_passed']:break
    k10=[r for r in runs if r['k']==10]
    passed=len(k10)==2 and all(r['development_gate_passed'] for r in k10)
    failure_outcomes={}
    for r in k10:
        for outcome,count in r['outcomes'].items():
            if outcome!='success':failure_outcomes[outcome]=failure_outcomes.get(outcome,0)+count
    result=dict(decision='GO current1M consumed development offered8QPS only' if passed else 'FAIL current1M consumed development offered8QPS',runs=runs,native_quality=native_quality,failure_outcomes=failure_outcomes,published_k10_p90_context_ms=444,original_strict_diagnostic_preserved=closed['decision'],fresh_cohort_used=False,qualification=False,matched_control_http_measured=False,matched_vendor_measured=False,lifecycle_cost_measured=False,all_physical_query_counters_complete=all(r['physical_counters_complete'] for r in runs))
    write(out/'decision.json',result)


if __name__=='__main__':main()
