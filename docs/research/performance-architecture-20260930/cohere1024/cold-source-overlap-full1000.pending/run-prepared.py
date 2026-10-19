from pathlib import Path
import json,hashlib,subprocess,importlib.util,struct,math,time,tarfile,base64,sys,signal,resource
C=Path(__file__).resolve().parent
D=Path('/mnt/borsuk-workspace/evidence')
E=D/'cold-source-overlap-full1000-a0002'
OLD=D/'routing64-fp32-a0001'
P=json.loads((C/'authorities.json').read_bytes())
AWS=str(C/'aws-bounded.sh')
PREFIX='research/semantic-router/20261019/cold-source-overlap-full1000-a0002/'
assert hashlib.sha256((D/'fp32-coalescing-root-screen-a0002/run.py').read_bytes()).hexdigest()=='446761034a43e9dbc1670b862fa8a5504cce3baa50701d2cb6fa8aaf9de15b7f'
spec=importlib.util.spec_from_file_location('helpers',str(D/'fp32-coalescing-root-screen-a0002/run.py'))
u=importlib.util.module_from_spec(spec);spec.loader.exec_module(u);u.E=E;u.PREFIX=PREFIX;u.AWS=AWS
sha=lambda b:hashlib.sha256(b).hexdigest()
write=u.write
def checked_arm(arm):
 a=P[arm];w=Path(a['source_path'])
 assert sha(Path(a['qualification_path']).read_bytes())==a['qualification_sha256']
 assert set(a['qualified_gate_statuses'])=={'affected_compile','focused_tests','workspace_clippy','workspace_testbuild','release_runner_reader'} and all(x==0 for x in a['qualified_gate_statuses'].values())
 assert subprocess.run(['git','-C',str(w),'diff','--quiet','HEAD','--','crates']).returncode==0
 assert sha(Path(a['runner_path']).read_bytes())==a['runner_sha256']
 assert sha(Path(a['reader_path']).read_bytes())==a['reader_sha256']
 assert subprocess.check_output(['git','-C',str(w),'rev-parse','HEAD']).decode().strip()==a['source_commit']
 fm=json.loads((D/'routing64-fp32-control-a0001/source-params.json').read_bytes())['source_files']
 return dict(a,source_files=fm,source_hashes={k:sha((w/v).read_bytes()) for k,v in fm.items()})
def make_case(base,xbase,arm,name,ordinals):
 a=arm;cfg=json.loads(json.dumps(base));x={k:json.loads(json.dumps(xbase[k])) for k in ['identity','bound_inputs']}
 cfg['execution']={'mode':'full'} if name in ['A1','B1','B2','A2'] else {'mode':'diagnostic_panel','ordinals':ordinals,'trace':False}
 cfg['serving']={'mode':'fp32_rerank','coalesce':False,'shortlist':16,'max_fp32_bytes':65664,'max_fp32_gets':16,'max_parallel_fp32_gets':16}
 cfg['source_cache_bytes']=0
 assert cfg['semantic_leaf_limit']==64 and cfg['max_memory_bytes']==2147483648 and cfg['sq8_range_split'] is None
 assert cfg['source_get_budget']==128 and cfg['source_byte_budget']==67108864 and cfg['sq8_get_budget']==32 and cfg['sq8_byte_budget']==33554432 and cfg['fetch_parallelism']==128
 h=sha((json.dumps(cfg,sort_keys=True,separators=(',',':'))+'\n').encode())
 x['identity'].update(a['source_hashes'])
 x['identity'].update(binary_sha256=a['runner_sha256'],config_sha256=h,execution=cfg['execution'],serving=cfg['serving'])
 x['bound_inputs'].update(execution=cfg['execution'],serving=cfg['serving'],selected_count=len(ordinals))
 x['prospective_startup']={'caller_pinned_bytes':7010842+(0 if cfg['execution']['mode']=='full' else 524288+len(ordinals)*8),'diagnostic_pinned_bytes':0 if cfg['execution']['mode']=='full' else 524288}
 return cfg,x,h
def prepare():
 protocol=json.loads((C/'protocol.json').read_bytes())
 assert time.time()<protocol['launch_before_epoch']
 assert sha((D/'fp32-coalescing-root-screen-a0002/run.py').read_bytes())=='446761034a43e9dbc1670b862fa8a5504cce3baa50701d2cb6fa8aaf9de15b7f'
 selfcheck()
 E.mkdir()
 arms={k:checked_arm(k) for k in ['A','B']}
 p={'arms':arms,'input_authorities_sha256':sha((C/'authorities.json').read_bytes()),'controller_sha256':sha(Path(__file__).read_bytes()),'protocol_sha256':sha((C/'protocol.json').read_bytes()),'instance':P['instance'],'aws_wrapper_sha256':sha((C/'aws-bounded.sh').read_bytes())}
 write(E/'authority.json',p);write(E/'measurement-addendum.json',protocol)
 base=json.loads((C/'frozeninputs/base-config.json').read_bytes())
 xbase=json.loads((C/'frozeninputs/base-expected.json').read_bytes())
 frozen={}
 cases=[('admit-A','A',protocol['admission_ordinals']),('admit-B','B',protocol['admission_ordinals']),('canary-A','A',protocol['canary_ordinals']),('canary-B','B',protocol['canary_ordinals'])]+[(n,n[0],protocol['panel']) for n in protocol['order']]
 for name,arm,ordinals in cases:
  a=arms[arm];cfg,x,h=make_case(base,xbase,a,name,ordinals)
  assert write(E/(name+'-config.json'),cfg)==h
  frozen[name]={'config_sha256':h,'expected_sha256':write(E/(name+'-expected.json'),x),'cached':False,'arm':arm}
 write(E/'frozen.json',frozen)
 print('PREPARED',sha((E/'frozen.json').read_bytes()))
def run():
 p=json.loads((E/'authority.json').read_bytes());protocol=json.loads((C/'protocol.json').read_bytes())
 assert sha(Path(__file__).read_bytes())==p['controller_sha256'] and sha((C/'protocol.json').read_bytes())==p['protocol_sha256']
 assert sha((C/'authorities.json').read_bytes())==p['input_authorities_sha256']
 for a in p['arms'].values():
  assert sha(Path(a['runner_path']).read_bytes())==a['runner_sha256'] and sha(Path(a['reader_path']).read_bytes())==a['reader_sha256']
  assert all(sha((Path(a['source_path'])/v).read_bytes())==a['source_hashes'][k] for k,v in a['source_files'].items())
 assert sha((C/'aws-bounded.sh').read_bytes())==p['aws_wrapper_sha256']
 frozen=json.loads((E/'frozen.json').read_bytes())
 old={};h=hashlib.sha256();seen=[]
 with (OLD/'full64-after-positive.jsonl').open('rb') as f:
  for line in f:
   h.update(line);r=json.loads(line)
   if r.get('phase')=='query':
    seen.append(r['ordinal'])
    if r['ordinal'] in set(protocol['panel'])|{0,1}:old[r['ordinal']]=r
 assert h.hexdigest()=='ba9ddc699283fc9b107a06af871a22a6e8bda63b473072b2c2dff44fc3166f1e' and seen==list(range(1000))
 truth=Path('/mnt/borsuk-sq8-shortlist/inputs/truth.u64').read_bytes()
 query=Path('/mnt/borsuk-sq8-shortlist/inputs/queries.f32').read_bytes()
 assert sha(truth)=='0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f'
 assert sha(query)=='8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e'
 G=Path('/mnt/borsuk-workspace/data/fp32-rerank-serving-real1m-a0001/generation')
 rootraw=(G/'manifest.json').read_bytes();assert sha(rootraw)=='14c8cbba54b3299ad681a40ce0455caac78f46c316dd1714da6b475733c5fde5'
 root=json.loads(rootraw);digests=(G/'canonical_row_digests.bin').read_bytes();assert sha(digests)==root['canonical']['row_digests_sha256']
 canonical=(G/'canonical.bin').open('rb');assert canonical.seek(0,2)==4104000000
 def hits(q):return len({r['id'] for r in q['returned']}&set(struct.unpack_from('<10Q',truth,q['ordinal']*80)))
 def oracle(q):
  vals=list(struct.unpack_from('<1024f',query,q['ordinal']*4096));norm=0.
  for v in vals:norm+=v*v
  if abs(norm-1.)>1e-6:vals=[struct.unpack('<f',struct.pack('<f',v/math.sqrt(norm)))[0] for v in vals]
  result=[]
  for nominee in q['canonical_stage']['shortlist']:
   ordinal=nominee['ordinal'];canonical.seek(ordinal*4104);b=canonical.read(4104)
   assert sha(b)==digests[ordinal*32:(ordinal+1)*32].hex() and struct.unpack_from('<q',b)[0]==nominee['id']
   row=struct.unpack_from('<1024f',b,8);qn=vn=dot=0.
   for x,y in zip(vals,row):qn+=x*x;vn+=y*y;dot+=x*y
   score=1.-max(-1.,min(1.,dot/(math.sqrt(qn)*math.sqrt(vn))));score=0. if score==0. else score
   result.append((score,nominee['id'],ordinal,struct.unpack('<Q',struct.pack('<d',score))[0]))
  result.sort();assert q['returned']==[{'id':x[1],'score_bits':x[3]} for x in result[:10]]
  assert q['returned_physical_ordinals']==[x[2] for x in result[:10]]
 arms={};startups={};status=1
 def cell(name):
  m=frozen[name];a=p['arms'][m['arm']];RUN=Path(a['runner_path']);READER=Path(a['reader_path']);cp=E/(name+'-config.json');xp=E/(name+'-expected.json')
  assert sha(cp.read_bytes())==m['config_sha256'] and sha(xp.read_bytes())==m['expected_sha256']
  cfg=json.loads(cp.read_bytes());assert cfg['source_cache_bytes']==0;scratch=Path(cfg['scratch_parent']);scratch.mkdir(exist_ok=True);assert not any(scratch.iterdir())
  out=E/(name+'.jsonl')
  outside_start=time.monotonic_ns();outside_cpu_before=resource.getrusage(resource.RUSAGE_CHILDREN)
  u.call(name+'-native',['/usr/bin/time','-v','-o',str(E/(name+'-resource.txt')),str(RUN),str(cp),m['config_sha256'],str(out)],protocol['native_runtime_max_seconds'] if name in protocol['order'] else protocol['prelude_native_seconds'])
  outside_wall=time.monotonic_ns()-outside_start;outside_cpu_after=resource.getrusage(resource.RUSAGE_CHILDREN)
  outside_cpu=int(((outside_cpu_after.ru_utime+outside_cpu_after.ru_stime)-(outside_cpu_before.ru_utime+outside_cpu_before.ru_stime))*1e9)
  assert not any(scratch.iterdir()) and out.stat().st_size<=67108864
  raw=out.read_bytes();assert raw.endswith(b'\n');rows=list(map(json.loads,raw.splitlines()));ex=json.loads(xp.read_bytes())
  assert rows[:2]==[ex['identity'],ex['bound_inputs']]
  startup=next(r for r in rows if r.get('phase')=='startup')
  assert startup['caller_pinned_bytes']==ex['prospective_startup']['caller_pinned_bytes']
  assert startup['library_cap_bytes']==2147483648 and startup['fp32_rerank']['modeled_memory_bytes']<=2147483648
  rc={'schema':'borsuk-completed-native-reduction-config-v4','input':{'path':str(out),'bytes':len(raw),'sha256':sha(raw)},'expected_identity':ex['identity'],'expected_bound_inputs':ex['bound_inputs']}
  rp=E/(name+'-reader-config.json');rh=write(rp,rc);u.call(name+'-reader',[str(READER),'--fp32-serving',str(rp),rh,str(E/(name+'-report.json'))],protocol['reader_runtime_max_seconds'] if name in protocol['order'] else protocol['prelude_reader_seconds'])
  if m['cached']:assert startup['source_cache']=={'charged_bytes':264089528,'capacity_bytes':264050688,'occupied_bytes':0,'hits':0,'misses':0,'evictions':0,'insertions':0}
  else:assert 'source_cache' not in startup
  qs=[q for q in rows if q.get('phase')=='query'];assert [q['ordinal'] for q in qs]==(list(range(1000)) if cfg['execution']['mode']=='full' else cfg['execution']['ordinals'])
  summary=rows[-1]['summary'];assert summary['status']=='MEASURED' and summary['complete'] and summary['all_queries_sealed']
  assert 0<summary['process_wall_ns']<=outside_wall+5000000
  assert 0<summary['process_cpu_ns']<=outside_cpu+50000000
  delta=[]
  for q in qs:
   ref=old[q['ordinal']];oracle(q)
   assert not q['source_nomination_skipped'] and q['planning_scope']=='source_nomination_and_cover'
   for k in ['returned','returned_physical_ordinals','plan','trace']:
    assert q[k]==ref[k],(name,q['ordinal'],k)
   assert q['canonical_stage']['shortlist']==ref['canonical_stage']['shortlist']
   for k in ['sq8','canonical','router']:assert q['charges'][k]==ref['charges'][k]
   assert not m['cached'] and q['charges']==ref['charges'] and 'source_cache' not in q
   a,b=hits(ref),hits(q)
   if a!=b:
    gt=set(struct.unpack_from('<10Q',truth,q['ordinal']*80))
    aa={v['id'] for v in ref['returned']};bb={v['id'] for v in q['returned']}
    delta.append({'ordinal':q['ordinal'],'before':a,'after':b,'gained_truth_ids':sorted((bb-aa)&gt),'displaced_truth_ids':sorted((aa-bb)&gt)})
  assert summary['recall_numerator']==sum(map(hits,qs))
  result={'raw_sha256':sha(raw),'queries':len(qs),'hits':summary['recall_numerator'],'denominator':10*len(qs),'quality_changes':delta,'summary':summary,'startup_modeled_bytes':startup['fp32_rerank']['modeled_memory_bytes'],'caller_pinned_bytes':startup['caller_pinned_bytes'],'query_wall_ns':sum(q['query_wall_ns'] for q in qs),'query_wall_samples_ns':[q['query_wall_ns'] for q in qs],'p_ns':{str(n):sorted(q['query_wall_ns'] for q in qs)[math.ceil(n*len(qs))-1] for n in [.5,.9,.95,.99]},'independent_f64_oracle':True,'source_cache_on':m['cached'],'first_query_wall_ns':qs[0]['query_wall_ns'],'final_source_cache':qs[-1].get('source_cache'),'process_wall_ns':summary.get('process_wall_ns'),'process_CPU_ns':summary.get('process_cpu_ns'),'initialization_isolated':False,'external_process_wall_ns':outside_wall,'external_children_CPU_ns':outside_cpu,'reported_non_query_wall_ns':summary['process_wall_ns']-sum(q['query_wall_ns'] for q in qs)}
  result['external_resource_usage']=(E/(name+'-resource.txt')).read_text()
  write(E/(name+'-verification.json'),result);u.checkpoint(name);return result
 try:
  prelude={name:cell(name) for name in protocol['preflight']}
  write(E/'preflight.done',{'native_real_inputs_and_S3':True,'mocked_operations':False,'all_native_and_reader_exits':0,'scratch_cleanup':True})
  assert time.time()<protocol['measurement_launch_before_epoch']
  for name in protocol['order']:arms[name]=cell(name)
  assert all(x['hits']==9902 and x['queries']==1000 and x['denominator']==10000 for x in arms.values())
  gets=sum(x['summary']['sum']['submitted_gets'] for x in [*prelude.values(),*arms.values()]);bytes_=sum(x['summary']['sum']['verified_bytes'] for x in [*prelude.values(),*arms.values()])
  assert gets<=protocol['query_get_ceiling'] and bytes_<=protocol['query_verified_byte_ceiling']
  assert sum(x.stat().st_size for x in E.glob('*.jsonl'))<=protocol['campaign_output_bytes']
  aa=[arms['A1'],arms['A2']];bb=[arms['B1'],arms['B2']]
  wall_ratio=sum(x['query_wall_ns'] for x in bb)/sum(x['query_wall_ns'] for x in aa)
  cpu_ratio=sum(x['summary']['query_process_cpu_ns'] for x in bb)/sum(x['summary']['query_process_cpu_ns'] for x in aa)
  checks={'p95_AB1':arms['B1']['p_ns']['0.95']<=.95*arms['A1']['p_ns']['0.95'],'p95_AB2':arms['B2']['p_ns']['0.95']<=.95*arms['A2']['p_ns']['0.95'],'no_net_recall_loss':arms['B1']['hits']==9902,'no_query_wall_regression':wall_ratio<=1.,'no_query_CPU_regression':cpu_ratio<=1.}
  process_wall_ratio=sum(x['external_process_wall_ns'] for x in bb)/sum(x['external_process_wall_ns'] for x in aa)
  process_cpu_ratio=sum(x['external_children_CPU_ns'] for x in bb)/sum(x['external_children_CPU_ns'] for x in aa)
  poola=sorted(v for x in aa for v in x['query_wall_samples_ns']);poolb=sorted(v for x in bb for v in x['query_wall_samples_ns'])
  p90_ratio=poolb[math.ceil(.9*len(poolb))-1]/poola[math.ceil(.9*len(poola))-1]
  checks.update(query_wall_5percent=wall_ratio<=.95,pooled_p90_nonregression=p90_ratio<=1.,no_process_wall_regression=process_wall_ratio<=1.,no_process_CPU_regression=process_cpu_ratio<=1.05)
  decision='MATCHED_QUALITY_LATENCY_SUPPORTED_ON_FULL1000' if all(checks.values()) else ('VALID_QUALITY_GAIN_PARETO' if arms['B1']['hits']>9902 else 'VALID_TRADEOFF_OR_NEGATIVE_ON_FULL1000')
  write(E/'report.json',{'schema':'borsuk-cold-source-overlap-full1000-result-v1','complete':True,'decision':decision,'checks':checks,'arms':arms,'wall_B_over_A':wall_ratio,'CPU_B_over_A':cpu_ratio,'external_process_wall_B_over_A':process_wall_ratio,'external_children_CPU_B_over_A':process_cpu_ratio,'pooled_p90_B_over_A':p90_ratio,'population_claim':False,'provider_cache_eviction_proven':False,'vendor_win_claim':False})
  status=0
 except Exception as ex:
  write(E/'error.json',{'status':'INVALID','error':str(ex)})
 finally:
  (E/'pipeline.exit').write_text(str(status)+'\n')
  archive=E/'evidence.tar.gz'
  with tarfile.open(archive,'w:gz',compresslevel=1) as t:
   for d in [C,E]:
    for f in d.rglob('*'):
     if f.is_file() and f!=archive:t.add(f,arcname=d.name+'/'+str(f.relative_to(d)))
  def put(p):
   h=hashlib.sha256(p.read_bytes()).digest();cs=base64.b64encode(h).decode();key=PREFIX+p.name
   subprocess.run([AWS,'--region','eu-central-1','s3api','put-object','--bucket','borsuk-bench-453182569524-euc1','--key',key,'--body',str(p),'--if-none-match','*','--checksum-algorithm','SHA256','--checksum-sha256',cs],check=True,stdout=subprocess.DEVNULL)
   v=json.loads(subprocess.check_output([AWS,'--region','eu-central-1','s3api','head-object','--bucket','borsuk-bench-453182569524-euc1','--key',key,'--checksum-mode','ENABLED']));assert v['ChecksumSHA256']==cs and v['ContentLength']==p.stat().st_size
   return {'key':key,'bytes':p.stat().st_size,'sha256':h.hex()}
  ar=put(archive);write(E/'terminal.json',{'native_pipeline_exit':status,'archive':ar,'instance':P['instance'],'terminal_epoch':time.time()});put(E/'terminal.json')
 raise SystemExit(status)

def selfcheck():
 pins=json.loads((C/'frozeninputs/pins.json').read_bytes())
 for n,h in pins.items():assert sha((C/'frozeninputs'/n).read_bytes())==h
 base=json.loads((C/'frozeninputs/base-config.json').read_bytes());xb=json.loads((C/'frozeninputs/base-expected.json').read_bytes())
 protocol=json.loads((C/'protocol.json').read_bytes())
 assert sum(protocol['budget_seconds'].values())==4200
 assert protocol['measurement_launch_before_epoch']+4200<=1792411680
 assert protocol['panel']==list(range(1000)) and protocol['order']==['A1','B1','B2','A2']
 assert len(protocol['admission_ordinals'])==8 and len(protocol['canary_ordinals'])==2
 assert protocol['screen_gates']['both_paired_p95_ratio_max']==.95
 assert protocol['query_get_ceiling']==4020*176
 assert protocol['query_verified_byte_ceiling']==4020*(67108864+33554432+65664)
 assert protocol['quality_reference_hits']==9902 and protocol['quality_denominator']==10000
 assert protocol['caller_pinned_full_bytes']==7535194-524288-8*8==7535146-524288-8*2==7535674-524288-8*68
 assert protocol['caller_pin_evidence']['source_panel_minus_full']=='524288 + 8 * count'
 for arm in ['A','B']:
  a=checked_arm(arm)
  assert a['reader_sha256']=={'A':'11f70589d210a102d4808358fc416bfd2c2cb80b2f993b56243f6504e7b85dfb','B':'fa16dccadc44f974a019f8613122fc5aa88f6f2ac2d5b469d4200fe9f922073e'}[arm]
  cases=[('admit-'+arm,protocol['admission_ordinals']),('canary-'+arm,protocol['canary_ordinals'])]+[(n,protocol['panel']) for n in protocol['order'] if n[0]==arm]
  for name,ordinals in cases:
   cfg,x,h=make_case(base,xb,a,name,ordinals)
   assert h==sha((json.dumps(cfg,sort_keys=True,separators=(',',':'))+chr(10)).encode())
   assert cfg['count']==x['bound_inputs']['count']==1000
   assert cfg['dimensions']==1024 and cfg['rows']==1000000 and cfg['k']==10 and cfg['metric']=='cosine'
   assert cfg['source_authentication']=='offload2' and cfg.get('max_active_queries') is None
   assert cfg['source_cache_bytes']==0 and cfg['max_memory_bytes']==2147483648 and cfg['semantic_leaf_limit']==64
   assert cfg['execution']==x['identity']['execution']==x['bound_inputs']['execution']
   assert cfg['serving']==x['identity']['serving']==x['bound_inputs']['serving']
   assert x['bound_inputs']['selected_count']==len(ordinals)
   assert x['identity']['config_sha256']==h and x['identity']['binary_sha256']==a['runner_sha256']
   assert x['bound_inputs']['requests_sha256']==cfg['requests']['sha256']
   assert x['bound_inputs']['truth_sha256']==cfg['truth']['sha256']
   assert cfg['scratch_parent']==base['scratch_parent']
   if name in protocol['order']:
    assert cfg['execution']=={'mode':'full'} and x['bound_inputs']['selected_count']==1000
    assert x['prospective_startup']=={'caller_pinned_bytes':7010842,'diagnostic_pinned_bytes':0}
 print('STATIC_SELF_CHECK_PASS: 8 cases, exact full mode/count/caps/header/source/binary pins; no payload or native execution')

if __name__=='__main__':
 if sys.argv[1]=='selfcheck':selfcheck()
 elif sys.argv[1]=='prepare':prepare()
 elif sys.argv[1]=='run':
  signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(RuntimeError('owned unit termination; preserve INVALID')))
  run()
 else:raise SystemExit(2)
