"""AWS-only matched incoming-HTTP dev64 gate, immutable head/ID/counter authority."""
import atexit,hashlib,http.client,json,math,socket,struct,subprocess,sys,time
from pathlib import Path

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
def quantile(values,p):
    values=sorted(values);i=(len(values)-1)*p;lo=math.floor(i);hi=math.ceil(i);return values[lo]+(values[hi]-values[lo])*(i-lo)
def connection():return http.client.HTTPConnection('127.0.0.1',8080,timeout=5)
def post(client,body):
    client.request('POST','/search',body,{'Content-Type':'application/json'});response=client.getresponse();raw=response.read();return response.status,raw

def run(item,arm,rep,config,out,binary,requests,reference,gt,prefix):
    directory=out/('run'+str(rep)+'-'+arm);directory.mkdir();authority=dict(root_sha256=item['root_sha256'][arm],generation=1,control_epoch=1)
    command=[str(binary),config['bucket'],config['region'],item['index_prefix'][arm],authority['root_sha256'],'1','1','127.0.0.1:8080']
    with (directory/'server.log').open('x') as log:
        server=subprocess.Popen(['/usr/bin/time','-v','-o',str(directory/'server.time'),'timeout','--signal=TERM','--kill-after=5','120',*command],stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:
            deadline=time.monotonic()+30
            while True:
                if server.poll() is not None:raise ValueError('server closed before readiness')
                try:
                    health=connection();health.request('GET','/health');response=health.getresponse();ready=json.loads(response.read());health.close()
                    if response.status!=200 or ready['authority']!=authority or ready['dimensions']!=768:raise ValueError('head/readiness authority')
                    break
                except (ConnectionError,OSError):
                    if time.monotonic()>deadline:raise TimeoutError('server readiness')
                    time.sleep(.1)
            # Admission/identity probes issue no valid ANN request or data GET.
            client=connection();valid=dict(query=requests[0]['query'],k=100,**authority)
            for bad in [dict(valid,control_epoch=2),dict(valid,generation=2),dict(valid,root_sha256='f'*64)]:
                status,_=post(client,json.dumps(bad));assert status==409
            status,_=post(client,json.dumps(dict(valid,query=[0.0]*768)));assert status==400
            status,_=post(client,json.dumps(dict(valid,unexpected=True)));assert status==422
            status,_=post(client,b' '*65537);assert status==413;client.close()
            held=socket.create_connection(('127.0.0.1',8080),timeout=5)
            held.sendall(b'POST /search HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 60000\r\n\r\n{')
            try:
                deadline=time.monotonic()+2
                while True:
                    probe=connection();status,_=post(probe,json.dumps(dict(valid,query=[0.0]*768)));probe.close()
                    if status==503:break
                    assert status==400
                    if time.monotonic()>deadline:raise TimeoutError('nonqueued admission')
                    time.sleep(.01)
            finally:held.close()
            deadline=time.monotonic()+2
            while True:
                probe=connection();status,_=post(probe,json.dumps(dict(valid,query=[0.0]*768)));probe.close()
                if status==400:break
                assert status==503
                if time.monotonic()>deadline:raise TimeoutError('admission release')
                time.sleep(.01)
            write(directory/'boundary.json',dict(root=409,generation=409,epoch=409,zero=400,unknown=422,body_cap=413,pressure=503,released=400,valid_ann_probes=0))
            client=connection();samples=[];loop_start=time.perf_counter_ns()
            with (directory/'http.jsonl').open('x') as stream:
                for q in range(64):
                    body=json.dumps(dict(query=requests[q]['query'],k=100,**authority),separators=(',',':')).encode();start=time.perf_counter_ns();status,raw=post(client,body);r=json.loads(raw);elapsed=time.perf_counter_ns()-start
                    if status!=200 or r['authority']!=authority:raise ValueError('HTTP response authority/status')
                    expected=reference[q+1]
                    if any(r[k]!=expected[k] for k in ['ids','ranges','planned_bytes','submitted_gets','verified_bytes','failed_gets']):raise ValueError('current native source/scorer/ordered-ID/physical parity')
                    assert len(r['ids'])==len(set(r['ids']))==100 and len(r['ranges'])==r['submitted_gets']<=32 and r['planned_bytes']==r['verified_bytes']<=16773120 and r['failed_gets']==0
                    sample=dict(query_ordinal=q,http_wall_ns=elapsed,returned_hits=len(set(r['ids'])&set(gt[q*100:(q+1)*100])),request_bytes=len(body),response_bytes=len(raw),**r);stream.write(json.dumps(sample,sort_keys=True)+'\n');samples.append(sample)
            measurement_wall_ns=time.perf_counter_ns()-loop_start;client.close()
            hits=sum(r['returned_hits'] for r in samples);p05=sorted(r['returned_hits'] for r in samples)[3]
            if hits!=item['returned_hits'][arm] or p05!=item['p05'][arm]:raise ValueError('HTTP exact consumed quality parity')
            result=dict(rep=rep,arm=arm,authority=authority,count=64,returned_hits=hits,p05=p05,serial_observed_qps=64e9/measurement_wall_ns,measurement_wall_ns=measurement_wall_ns,incoming_http_ms={label:quantile([r['http_wall_ns']/1e6 for r in samples],p) for label,p in [('p50',.5),('p90',.9),('p95',.95),('p99',.99)]},data_get_attempts=sum(r['submitted_gets'] for r in samples),verified_bytes=sum(r['verified_bytes'] for r in samples),failed_gets=0,source_scorer_ordered_id_parity=True)
            if server.poll() is not None:raise ValueError('server terminated during completed measurement')
            write(directory/'result.json',result);return result
        finally:
            intentional_stop=server.poll() is None
            if intentional_stop:
                import os,signal
                children=(Path('/proc')/str(server.pid)/'task'/str(server.pid)/'children').read_text().split()
                if len(children)!=1:raise ValueError('native timeout process authority')
                timeout_pid=int(children[0]);os.kill(timeout_pid,signal.SIGTERM)
                try:server.wait(timeout=8)
                except subprocess.TimeoutExpired:os.killpg(timeout_pid,signal.SIGKILL);server.wait(timeout=3)
            write(directory/'server-closeout.json',dict(returncode=server.returncode,intentional_stop=intentional_stop))
            if (directory/'result.json').exists():
                subprocess.run(['aws','s3','cp',str(directory),'s3://'+config['bucket']+'/'+prefix+'/artifacts/screen/'+item['name']+'/'+directory.name,'--recursive','--only-show-errors'],check=True)

def main():
    config_path,digest,out,binary,prefix=sys.argv[1:];out,binary=Path(out),Path(binary);config=json.loads(Path(config_path).read_text())
    assert sha(config_path)==digest and config['schema']=='borsuk-native-http-dev64-v1' and config['arm_order']==['control','candidate','candidate','control'] and [i['name'] for i in config['items']]==['relaion','cohere']
    out.mkdir();results=[]
    def resources():
        group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('0::')[-1].lstrip('/')
        import os,resource
        write(out/'cgroup.json',dict(cgroup={k:(group/k).read_text() for k in ['memory.max','memory.peak','memory.swap.max','memory.swap.peak','memory.events','cpu.stat'] if (group/k).exists()},address_space_limit=list(resource.getrlimit(resource.RLIMIT_AS)),cpu_affinity=sorted(os.sched_getaffinity(0))))
    atexit.register(resources)
    for item in config['items']:
        directory=out/item['name'];directory.mkdir()
        for name,ident in item['artifacts'].items():
            p=directory/name;subprocess.run(['aws','s3','cp','s3://'+config['bucket']+'/'+ident['key'],str(p),'--only-show-errors'],check=True)
            assert p.stat().st_size==ident['bytes'] and sha(p)==ident['sha256']
        requests=[json.loads(s) for s in (directory/'requests').read_text().splitlines()];assert len(requests)==1000 and all(r['query_ordinal']==q for q,r in enumerate(requests))
        gt=struct.unpack('<100000I',(directory/'truth').read_bytes());references={a:[json.loads(s) for s in (directory/('reference-'+a)).read_text().splitlines()] for a in ['control','candidate']}
        assert all(len(r)==66 and r[-1]['count']==64 and r[0]['root_sha256']==item['root_sha256'][a] for a,r in references.items())
        with (directory/'bad-head.log').open('x') as log:
            status=subprocess.run(['/usr/bin/time','-v','-o',str(directory/'bad-head.time'),'timeout','--signal=TERM','--kill-after=5','30',str(binary),config['bucket'],config['region'],item['index_prefix']['control'],item['root_sha256']['control'],'1','2','127.0.0.1:8080'],stdout=log,stderr=subprocess.STDOUT).returncode
        assert status==1 and 'trusted head authority mismatch' in (directory/'bad-head.log').read_text()
        write(directory/'bad-head.json',dict(returncode=status,wrong_epoch_rejected=True,ann_queries=0))
        runs=[run(item,a,rep,config,directory,binary,requests,references[a],gt,prefix) for rep,a in enumerate(config['arm_order'])]
        medians={a:{p:quantile([r['incoming_http_ms'][p] for r in runs if r['arm']==a],.5) for p in ['p50','p90','p95','p99']} for a in references}
        candidate=item['returned_hits']['candidate'];quality=candidate/64>=98 and item['p05']['candidate']>=95 and item['flat_hits']-candidate<=32 and candidate>=item['returned_hits']['control'];passed=quality and medians['candidate']['p90']<=250 and medians['candidate']['p95']<=400
        result=dict(dataset=item['name'],decision='GO HTTP development envelope only' if passed else 'KILL HTTP development envelope',runs=runs,arm_median_incoming_http_ms=medians,arm_median_serial_qps={a:quantile([r['serial_observed_qps'] for r in runs if r['arm']==a],.5) for a in references},qualification=False,fresh_cohort_used=False,matched_vendor_measured=False,concurrent_qps_measured=False)
        write(directory/'result.json',result);results.append(result)
        if not passed:break
    write(out/'decision.json',dict(decision=results[-1]['decision'],results=results,qualification=False))

if __name__=='__main__':main()
