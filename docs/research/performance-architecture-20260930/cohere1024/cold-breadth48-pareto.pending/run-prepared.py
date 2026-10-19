from pathlib import Path
import json,hashlib,subprocess,importlib.util,struct,math,time,tarfile,base64,sys,signal,resource
C=Path(__file__).resolve().parent
D=Path('/mnt/borsuk-workspace/evidence')
E=D/'cold-breadth48-pareto-a0001'
OLD=D/'routing64-fp32-a0001'
P=json.loads((C/'authorities.json').read_bytes())
AWS=str(C/'aws-bounded.sh')
PREFIX='research/semantic-router/20261019/cold-breadth48-pareto-a0001/'
assert hashlib.sha256((D/'fp32-coalescing-root-screen-a0002/run.py').read_bytes()).hexdigest()=='446761034a43e9dbc1670b862fa8a5504cce3baa50701d2cb6fa8aaf9de15b7f'
spec=importlib.util.spec_from_file_location('helpers',str(D/'fp32-coalescing-root-screen-a0002/run.py'))
u=importlib.util.module_from_spec(spec);spec.loader.exec_module(u);u.E=E;u.PREFIX=PREFIX;u.AWS=AWS
sha=lambda b:hashlib.sha256(b).hexdigest()
write=u.write
def checked_arm(arm):
 a=P[arm]
 q=json.loads(Path(a['qualification_path']).read_bytes())
 assert sha(Path(a['qualification_path']).read_bytes())==a['qualification_sha256']
 assert q['gates']==a['qualified_gate_statuses'] and all(v==0 for v in q['gates'].values())
 assert q['source_commit']==a['source_commit']
 assert subprocess.check_output(['git','-C',a['source_path'],'rev-parse','HEAD:crates']).decode().strip()==a['qualified_tree']
 return a
def make_case(base,xbase,arm,name,ordinals):
 a=arm;cfg=json.loads(json.dumps(base));x={k:json.loads(json.dumps(xbase[k])) for k in ['identity','bound_inputs']}
 cfg['execution']={'mode':'diagnostic_panel','ordinals':ordinals,'trace':False}
 cfg['serving']={'mode':'fp32_rerank','coalesce':False,'shortlist':16,'max_fp32_bytes':65664,'max_fp32_gets':16,'max_parallel_fp32_gets':16}
 cfg['source_cache_bytes']=0
 assert cfg['semantic_leaf_limit'] in [64,48] and cfg['max_memory_bytes']==2147483648 and cfg['sq8_range_split'] is None
 assert cfg['source_get_budget']==128 and cfg['source_byte_budget']==67108864 and cfg['sq8_get_budget']==32 and cfg['sq8_byte_budget']==33554432 and cfg['fetch_parallelism']==128
 h=sha((json.dumps(cfg,sort_keys=True,separators=(',',':'))+'\n').encode())
 x['identity'].update(a['source_hashes'])
 x['identity'].update(binary_sha256=a['runner_sha256'],config_sha256=h,execution=cfg['execution'],serving=cfg['serving'])
 x['bound_inputs'].update(execution=cfg['execution'],serving=cfg['serving'],selected_count=len(ordinals))
 models=json.loads((C/'model-pins.json').read_bytes())
 x['prospective_startup']=dict(models[str(cfg['semantic_leaf_limit'])])
 delta=x['prospective_startup']['diagnostic_pinned_bytes']+len(ordinals)*8
 x['prospective_startup']['caller_pinned_bytes']+=delta
 x['prospective_startup']['fp32_modeled_memory_bytes']+=delta
 return cfg,x,h
def prepare():
 protocol=json.loads((C/'protocol.json').read_bytes())
 assert time.time()<protocol['launch_before_epoch']
 assert sha((D/'fp32-coalescing-root-screen-a0002/run.py').read_bytes())=='446761034a43e9dbc1670b862fa8a5504cce3baa50701d2cb6fa8aaf9de15b7f'
 selfcheck()
 E.mkdir()
 arms={k:checked_arm(k) for k in ['A','B']}
 p={'arms':arms,'input_authorities_sha256':sha((C/'authorities.json').read_bytes()),'controller_sha256':sha(Path(__file__).read_bytes()),'protocol_sha256':sha((C/'protocol.json').read_bytes()),'instance':P['instance'],'aws_wrapper_sha256':sha((C/'aws-bounded.sh').read_bytes()),'model_pins_sha256':sha((C/'model-pins.json').read_bytes())}
 write(E/'authority.json',p);write(E/'measurement-addendum.json',protocol)
 bases={k:json.loads((C/'frozeninputs'/f'{k}-config.json').read_bytes()) for k in ['A','B']}
 xbases={k:json.loads((C/'frozeninputs'/f'{k}-expected.json').read_bytes()) for k in ['A','B']}
 frozen={}
 cases=[('admit-A','A',protocol['admission_ordinals']),('admit-B','B',protocol['admission_ordinals']),('canary-A','A',protocol['canary_ordinals']),('canary-B','B',protocol['canary_ordinals'])]+[(n,n[0],protocol['panel']) for n in protocol['order']]
 for name,arm,ordinals in cases:
  a=arms[arm];cfg,x,h=make_case(bases[arm],xbases[arm],a,name,ordinals)
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
  assert checked_arm('A')['qualified_tree']==protocol['source_tree']
 assert sha((C/'aws-bounded.sh').read_bytes())==p['aws_wrapper_sha256']
 assert sha((C/'model-pins.json').read_bytes())==p['model_pins_sha256']
 frozen=json.loads((E/'frozen.json').read_bytes())
 refs=json.loads((C/'closed-references.json').read_bytes())
 assert sha((C/'closed-references.json').read_bytes())==protocol['reference_authority_sha256']
 old={}
 for arm,ref in refs['references'].items():
  h=hashlib.sha256();seen=[];old[arm]={};size=0;last=None;headers=[]
  with Path(ref['path']).open('rb') as f:
   for line in f:
    assert line.endswith(b'\n')
    h.update(line);size+=len(line);row=json.loads(line);last=row
    if len(headers)<2:headers.append(row)
    if row.get('phase')=='query':
     seen.append(row['ordinal'])
     if row['ordinal'] in set(protocol['panel']+protocol['admission_ordinals']+protocol['canary_ordinals']):old[arm][row['ordinal']]=row
  expected=json.loads((C/'frozeninputs'/f'{arm}-expected.json').read_bytes())
  assert headers==[expected['identity'],expected['bound_inputs']]
  assert headers[1]['generation_root_sha256']==json.loads((C/'frozeninputs'/f'{arm}-config.json').read_bytes())['generation_root_sha256']
  assert h.hexdigest()==ref['sha256'] and size==ref['bytes'] and seen==list(range(1000))
  assert ref['native_exit']==ref['reader_exit']==0 and ref['ordinals']==1000
  assert last['summary']['complete'] and last['summary']['all_queries_sealed'] and last['summary']['status']=='MEASURED'
  assert last['summary']['recall_numerator']==ref['historical_full_hits']
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
  assert startup['fp32_rerank']['modeled_memory_bytes']==ex['prospective_startup']['fp32_modeled_memory_bytes']
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
   ref=old[m['arm']][q['ordinal']];oracle(q)
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
  assert all(x['queries']==68 and x['denominator']==680 and not x['quality_changes'] for x in arms.values())
  gets=sum(x['summary']['sum']['submitted_gets'] for x in [*prelude.values(),*arms.values()]);bytes_=sum(x['summary']['sum']['verified_bytes'] for x in [*prelude.values(),*arms.values()])
  assert gets<=protocol['query_get_ceiling'] and bytes_<=protocol['query_verified_byte_ceiling']
  assert sum(x.stat().st_size for x in E.glob('*.jsonl'))<=protocol['campaign_output_bytes']
  aa=[arms['A1'],arms['A2']];bb=[arms['B1'],arms['B2']]
  wall_ratio=sum(x['query_wall_ns'] for x in bb)/sum(x['query_wall_ns'] for x in aa)
  cpu_ratio=sum(x['summary']['query_process_cpu_ns'] for x in bb)/sum(x['summary']['query_process_cpu_ns'] for x in aa)
  checks={'p95_AB1':arms['B1']['p_ns']['0.95']<=.90*arms['A1']['p_ns']['0.95'],'p95_AB2':arms['B2']['p_ns']['0.95']<=.90*arms['A2']['p_ns']['0.95'],'query_wall_10percent':wall_ratio<=.90,'no_query_CPU_regression':cpu_ratio<=1.}
  process_wall_ratio=sum(x['external_process_wall_ns'] for x in bb)/sum(x['external_process_wall_ns'] for x in aa)
  process_cpu_ratio=sum(x['external_children_CPU_ns'] for x in bb)/sum(x['external_children_CPU_ns'] for x in aa)
  poola=sorted(v for x in aa for v in x['query_wall_samples_ns']);poolb=sorted(v for x in bb for v in x['query_wall_samples_ns'])
  p90_ratio=poolb[math.ceil(.9*len(poolb))-1]/poola[math.ceil(.9*len(poola))-1]
  get_ratio=sum(x['summary']['sum']['submitted_gets'] for x in bb)/sum(x['summary']['sum']['submitted_gets'] for x in aa)
  byte_ratio=sum(x['summary']['sum']['verified_bytes'] for x in bb)/sum(x['summary']['sum']['verified_bytes'] for x in aa)
  checks.update(pooled_p90_nonregression=p90_ratio<=1.,external_wall_5percent=process_wall_ratio<=.95,no_process_CPU_regression=process_cpu_ratio<=1.05,GET_nonregression=get_ratio<=1.,verified_bytes_nonregression=byte_ratio<=1.)
  decision='LATENCY_GATES_MET_WITH_HISTORICAL_QUALITY_TRADEOFF' if all(checks.values()) else 'VALID_TRADEOFF_OR_NEGATIVE_ON_FIXED68'
  write(E/'report.json',{'schema':'borsuk-cold-breadth48-pareto-result-v1','complete':True,'decision':decision,'checks':checks,'arms':arms,'wall_B_over_A':wall_ratio,'CPU_B_over_A':cpu_ratio,'external_process_wall_B_over_A':process_wall_ratio,'external_children_CPU_B_over_A':process_cpu_ratio,'pooled_p90_B_over_A':p90_ratio,'logical_GET_B_over_A':get_ratio,'verified_bytes_B_over_A':byte_ratio,'historical_full_quality':{'A_hits':9902,'B_hits':9852,'denominator':10000,'new_measurement':False},'panel_quality_delta_hits_B_minus_A':arms['B1']['hits']-arms['A1']['hits'],'population_claim':False,'provider_cache_eviction_proven':False,'vendor_win_claim':False,'readiness_claim':False,'generalization_claim':False})
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
 protocol=json.loads((C/'protocol.json').read_bytes())
 refs=json.loads((C/'closed-references.json').read_bytes())
 assert sha((C/'closed-references.json').read_bytes())==protocol['reference_authority_sha256']
 assert sum(protocol['budget_seconds'].values())==protocol['whole_unit_seconds']==1800
 assert protocol['measurement_launch_before_epoch']+1800<1792413600
 assert protocol['panel']==refs['fixed_panel'] and len(set(protocol['panel']))==68
 assert protocol['order']==['A1','B1','B2','A2']
 assert len(protocol['admission_ordinals'])==8 and len(protocol['canary_ordinals'])==2
 assert not set(protocol['admission_ordinals'])&set(protocol['canary_ordinals'])
 assert protocol['query_get_ceiling']==292*176
 assert protocol['query_verified_byte_ceiling']==292*(67108864+33554432+65664)
 assert P['A']==P['B']
 models=json.loads((C/'model-pins.json').read_bytes())
 assert models['64']['caller_pinned_bytes']-models['48']['caller_pinned_bytes']==68392
 assert models['48']['fp32_modeled_memory_bytes']==468017695+160+2231766
 assert models['64']['fp32_modeled_memory_bytes']+524288+8*68==471959813
 assert models['64']['diagnostic_pinned_bytes']==2*256*1024
 assert models['48']['diagnostic_pinned_bytes']==2*128*1024
 assert protocol['screen_gates']=={'both_paired_p95_ratio_max':.90,'pooled_query_wall_ratio_max':.90,'pooled_p90_ratio_max':1.,'query_cpu_ratio_max':1.,'external_process_wall_ratio_max':.95,'external_children_cpu_ratio_max':1.05,'logical_GET_ratio_max':1.,'verified_bytes_ratio_max':1.,'exact_per_arm_reference':True}
 cases={}
 for arm,limit in [('A',64),('B',48)]:
  a=checked_arm(arm)
  base=json.loads((C/'frozeninputs'/f'{arm}-config.json').read_bytes())
  xb=json.loads((C/'frozeninputs'/f'{arm}-expected.json').read_bytes())
  assert sha((C/'frozeninputs'/f'{arm}-config.json').read_bytes())==xb['identity']['config_sha256']
  assert base['semantic_leaf_limit']==xb['identity']['semantic_leaf_limit']==xb['bound_inputs']['semantic_leaf_limit']==limit
  for name,ordinals in [('admit-'+arm,protocol['admission_ordinals']),('canary-'+arm,protocol['canary_ordinals'])]+[(n,protocol['panel']) for n in protocol['order'] if n[0]==arm]:
   cfg,x,h=make_case(base,xb,a,name,ordinals);cases[name]=(cfg,x,h)
   assert cfg['count']==x['bound_inputs']['count']==1000
   assert cfg['dimensions']==1024 and cfg['rows']==1000000 and cfg['k']==10 and cfg['metric']=='cosine'
   assert cfg['source_authentication']=='offload2' and cfg.get('max_active_queries') is None
   assert cfg['source_cache_bytes']==0 and cfg['max_memory_bytes']==2147483648
   assert cfg['execution']==x['identity']['execution']==x['bound_inputs']['execution']=={'mode':'diagnostic_panel','ordinals':ordinals,'trace':False}
   assert x['bound_inputs']['selected_count']==len(ordinals)
   assert x['identity']['config_sha256']==h and x['identity']['binary_sha256']==a['runner_sha256']
   assert x['identity']['semantic_leaf_limit']==x['bound_inputs']['semantic_leaf_limit']==limit
   assert cfg['scratch_parent']==base['scratch_parent']
   delta=models[str(limit)]['diagnostic_pinned_bytes']+8*len(ordinals)
   assert x['prospective_startup']['caller_pinned_bytes']==models[str(limit)]['caller_pinned_bytes']+delta
   assert x['prospective_startup']['fp32_modeled_memory_bytes']==models[str(limit)]['fp32_modeled_memory_bytes']+delta
   for key in ['requests','truth']:
    assert x['bound_inputs'][key+'_sha256']==cfg[key]['sha256']
   for key in ['source_get_budget','source_byte_budget','sq8_get_budget','sq8_byte_budget','generation_root_sha256','serving','source_authentication','fetch_parallelism']:
    assert cfg[key]==x['identity'].get(key,cfg[key])==x['bound_inputs'][key]
 assert {k for k in cases['A1'][0] if cases['A1'][0][k]!=cases['B1'][0][k]}=={'semantic_leaf_limit'}
 assert cases['A1']==cases['A2'] and cases['B1']==cases['B2']
 print('STATIC_SELF_CHECK_PASS: 8 cases, per-arm config/header/model/reference pins; no payload or native execution')

if __name__=='__main__':
 if sys.argv[1]=='selfcheck':selfcheck()
 elif sys.argv[1]=='prepare':prepare()
 elif sys.argv[1]=='run':
  signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(RuntimeError('owned unit termination; preserve INVALID')))
  run()
 else:raise SystemExit(2)
