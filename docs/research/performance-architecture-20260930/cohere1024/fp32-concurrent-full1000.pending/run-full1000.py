#!/usr/bin/env python3
# Full1000 adaptation of retained qualified screen glue. Root executes; never auto-resume.
import os,sys,json,hashlib,subprocess,time,signal,struct,math,tarfile,base64,traceback
from pathlib import Path
from prepare import E, NAMES, MEASURED, retained, root_authority, make_cell, count, RUNNER, READER as READER_PIN
sha=lambda b:hashlib.sha256(b).hexdigest()
def write(p,v):
 with p.open('x') as f:json.dump(v,f,sort_keys=True,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
def call(name,args,seconds):
 assert time.monotonic()+seconds+5<DEADLINE,'cell cannot fit inside 4200s envelope'
 native=name.endswith('-native')
 if native:
  assert args[0]=='/usr/bin/time'
  split=args.index('-o')+2
  args=args[:split]+['bash','-c','set -C; status=$1; shift; "$@"; r=$?; printf "%s\\n" "$r" > "$status"; exit "$r"','native-exit',str(E/(name+'.native-exit'))]+args[split:]
 with (E/(name+'.stdout')).open('xb') as out,(E/(name+'.stderr')).open('xb') as err:
  p=subprocess.Popen(args,stdout=out,stderr=err,start_new_session=True)
  timed_out=False
  try:rc=p.wait(timeout=seconds)
  except subprocess.TimeoutExpired:
   timed_out=True;os.killpg(p.pid,signal.SIGTERM)
   try:rc=p.wait(timeout=5)
   except subprocess.TimeoutExpired:os.killpg(p.pid,signal.SIGKILL);rc=p.wait()
 write(E/(name+'-process.json'),{'process_exit':rc,'timed_out':timed_out,'native_exit_known':native and (E/(name+'.native-exit')).exists()})
 with (E/(name+'.exit')).open('x') as f:f.write(str(124 if timed_out else rc)+'\n')
 if native and not timed_out:assert (E/(name+'.native-exit')).read_text().strip()=='0'
 assert not timed_out and rc==0,(name,rc,timed_out)
def armA(n):return n.endswith('-A') or n.startswith('A')

def cell(name):
 cell_run=CONTROL_RUN if armA(name) else RUN
 cell_reader=CONTROL_READER if armA(name) else READER
 cpath=E/(name+'-config.json');c=json.loads(cpath.read_bytes());ex=json.loads((E/(name+'-expected.json')).read_bytes())
 assert c['source_cache_bytes']==0 and c['fetch_parallelism']==64 and c['semantic_leaf_limit']==64
 scratch=Path(c['scratch_parent']);scratch.mkdir();assert not any(scratch.iterdir())
 out=E/(name+'.jsonl');assert not out.exists()
 call(name+'-native',['/usr/bin/time','-v','-o',str(E/(name+'-resource.txt')),str(cell_run),str(cpath),sha(cpath.read_bytes()),str(out)],600 if name in MEASURED else 60)
 assert not any(scratch.iterdir())
 raw=out.read_bytes();assert len(raw)<=67108864 and raw.endswith(b'\n')
 rows=[json.loads(x) for x in raw.splitlines()];assert rows[:2]==[ex['identity'],ex['bound_inputs']]
 rc={'schema':'borsuk-completed-native-reduction-config-v4','input':{'path':str(out),'bytes':len(raw),'sha256':sha(raw)},'expected_identity':ex['identity'],'expected_bound_inputs':ex['bound_inputs']}
 rp=E/(name+'-reader-config.json');write(rp,rc)
 call(name+'-reader',[str(cell_reader),'--fp32-serving',str(rp),sha(rp.read_bytes()),str(E/(name+'-report.json'))],120 if name in MEASURED else 30)
 report=json.loads((E/(name+'-report.json')).read_bytes());assert report['valid'] is True
 qs=[q for q in rows if q.get('phase')=='query'];assert [q['ordinal'] for q in qs]==list(range(count(name)))
 for q in qs:
  ref=refs[q['ordinal']];oracle(q)
  for key in ['returned','returned_physical_ordinals','plan','trace','charges']:assert q[key]==ref[key],(name,q['ordinal'],key)
  assert q['canonical_stage']['shortlist']==ref['canonical_stage']['shortlist']
  assert not q['source_nomination_skipped'] and 'source_cache' not in q
 summary=rows[-1]['summary'];assert summary['complete'] and summary['all_queries_sealed'] and summary['status']=='MEASURED' and summary['recall_numerator']==sum(map(hits,qs))
 result={'queries':len(qs),'hits':summary['recall_numerator'],'denominator':len(qs)*10,'source_cache_bytes':0,'gold_exact':True,'independent_f64_oracle':True,'strict_reader_exit':0,'scratch_cleanup':True,'raw_sha256':sha(raw),'raw_bytes':len(raw),'full1000':name in MEASURED}
 cohort=report['query_cohort'];assert cohort['max_active_queries']==c['max_active_queries']
 assert report['query_wall_ns']==cohort['elapsed_ns'] and cohort['elapsed_ns']>0
 assert all(q['query_process_cpu_ns'] is None and q['transport'] is None for q in qs)
 values=sorted(q['query_wall_ns'] for q in qs)
 result.update(query_cohort_elapsed_ns=cohort['elapsed_ns'],cohort_cpu_ns=cohort['process_cpu_ns'],
   cohort_qps=len(qs)*1e9/cohort['elapsed_ns'],
   query_wall_ms={str(p):values[math.ceil(len(values)*p)-1]/1e6 for p in [.5,.9,.95,.99]},
   sdk_native_transport_scope='S3_and_IMDS_not_wire_or_billed')
 write(E/(name+'-verification.json'),result);return result

def checkpoint(name):
 if name not in ['A1','B1','B2','A2']:return
 archive=E/(name+'-checkpoint.tar.gz')
 files=[f for f in E.iterdir() if f.is_file() and (f.name.startswith(name+'.') or f.name.startswith(name+'-') or f.name.startswith('admit-') or f.name.startswith('canary-') or f.name in ['README.md','qualified-native-authority.json','gold-authority.json','run-full1000.py','run-controller.sh','launch.sh','prepare.py','preparation.json','oracle-reused.py','launch.json'])]
 if not archive.exists():
  with tarfile.open(archive,'x:gz') as t:
   for f in sorted(files):t.add(f,arcname=f.name)
 raw=archive.read_bytes();b64=base64.b64encode(hashlib.sha256(raw).digest()).decode();key='research/semantic-router/20261019/fp32-concurrent-full1000-a0001/'+archive.name
 aws='/mnt/borsuk-retained-root/usr/local/aws-cli/v2/2.36.11/dist/aws'
 def cli(label,args):
  result=subprocess.run([aws,'--region','eu-central-1','--cli-connect-timeout','5','--cli-read-timeout','10','s3api']+args,capture_output=True,timeout=20,env={**os.environ,'AWS_MAX_ATTEMPTS':'1','AWS_RETRY_MODE':'standard'})
  (E/(name+'-checkpoint-'+label+'.stdout')).write_bytes(result.stdout);(E/(name+'-checkpoint-'+label+'.stderr')).write_bytes(result.stderr);assert result.returncode==0,(name,label,result.returncode);return json.loads(result.stdout)
 put=cli('put',['put-object','--bucket','borsuk-bench-453182569524-euc1','--key',key,'--body',str(archive),'--if-none-match','*','--checksum-algorithm','SHA256','--checksum-sha256',b64]);assert put['ChecksumSHA256']==b64
 head=cli('head',['head-object','--bucket','borsuk-bench-453182569524-euc1','--key',key,'--checksum-mode','ENABLED']);assert head['ContentLength']==len(raw) and head['ChecksumSHA256']==b64
 write(E/(name+'-checkpoint.json'),{'status':'CONDITIONAL_PUT_AND_CHECKSUM_HEAD_PASS','key':key,'bytes':len(raw),'sha256':sha(raw),'outside_native_timing':True})

def hits(q):return len({r['id'] for r in q['returned']}&set(struct.unpack_from('<10Q',truth,q['ordinal']*80)))

def main():
 global RUN,READER,CONTROL_RUN,CONTROL_READER,refs,truth,query,G,canonical,digests,oracle,DEADLINE
 assert time.time()<1792443900,'launch cutoff21:05UTC'
 DEADLINE=time.monotonic()+4200
 assert not any((E/(n+'-verification.json')).exists() for n in NAMES),'no resume or auto-retry'
 prep=json.loads((E/'preparation.json').read_bytes())
 for name,pin in prep['files_sha256'].items():assert sha((E/name).read_bytes())==pin,name
 assert sha((E/'gold-authority.json').read_bytes())==prep['gold_authority_sha256']
 assert Path('/sys/devices/virtual/dmi/id/board_asset_tag').read_text().strip()=='i-09f8571d0edf296e9'
 subprocess.run(['systemctl','is-active','--quiet','borsuk-fp32-concurrent-full1000-a0001-shutdown.timer'],check=True)
 assert sha((E/'qualified-native-authority.json').read_bytes())==prep['native_authority_sha256']
 a=root_authority()
 assert a['qualification_complete'] is True and a['all_owned_compiler_processes_terminal'] is True
 RUN=CONTROL_RUN=Path(a['runner_path']);READER=CONTROL_READER=Path(a['reader_path'])
 assert sha(RUN.read_bytes())==a['runner_sha256'] and sha(READER.read_bytes())==a['reader_sha256']
 assert a['instance_id']=='i-09f8571d0edf296e9'
 assert a['runner_sha256']==RUNNER and a['reader_sha256']==READER_PIN
 write(E/'launch.json',{'instance_id':'i-09f8571d0edf296e9','qualification_instance_id':retained()['instance_id'],'started_epoch':time.time(),'outer_seconds':4200,'budget_seconds':{'prelude':400,'full':2920,'checkpoints':160,'reserve':720}})
 authority=json.loads((E/'gold-authority.json').read_bytes())
 assert authority['gold_path']==str(E/'gold-full1000.json') and authority['records']==1000
 assert authority['source_sha256']=='69b56a0464d9db152625b32051438fdc644785f6dc18564f68e0980a06e6b8b1' and authority['source_completed_measured'] is True
 raw=(E/'gold-full1000.json').read_bytes();assert len(raw)==authority['gold_bytes'] and sha(raw)==authority['gold_sha256']=='ce13a10a30f0cbaafe2a8aaefdeea54a5e1118ed3bf812168d371d690c18f74c'
 gold=json.loads(raw);refs={}
 assert gold['schema']==authority['gold_schema']=='borsuk-historical-gold-full1000-v1'
 for q in gold['records']:
  q['canonical_stage']={'shortlist':q['canonical_shortlist']};refs[q['ordinal']]=q
 assert list(refs)==list(range(1000)) and len(gold['records'])==1000
 assert gold['generation_root_sha256']==authority['generation_root_sha256']
 query=Path('/mnt/borsuk-sq8-shortlist/inputs/queries.f32').read_bytes()
 truth=Path('/mnt/borsuk-sq8-shortlist/inputs/truth.u64').read_bytes()
 assert sha(query)=='8460a81ff2f979deff7d82bede874a1301f47dfd3e4589305c9f53e020920d5e'
 assert sha(truth)=='0e38d943adaf62f197b98fe8b01b2edec2008010df760717ababfa5054693b7f'
 G=Path('/mnt/borsuk-workspace/data/fp32-rerank-serving-real1m-a0001/generation')
 b=(G/'manifest.json').read_bytes();assert sha(b)==gold['generation_root_sha256'];root=json.loads(b)
 digests=(G/'canonical_row_digests.bin').read_bytes();assert sha(digests)==root['canonical']['row_digests_sha256']
 canonical=(G/'canonical.bin').open('rb');assert canonical.seek(0,2)==4104000000
 code=(E/'oracle-reused.py').read_bytes();assert sha(code)=='6a8f9e381ff11c6c9a1e6a6f40987cee155d7d9b8dcce36894194f2f4e807f2a'
 exec(compile(code,str(E/'oracle-reused.py'),'exec'),globals())
 names=NAMES
 for name in names:
  config,expected=make_cell(name,a)
  assert (E/(name+'-config.json')).read_bytes()==config
  assert (E/(name+'-expected.json')).read_bytes()==expected
 results={}
 for name in names:
  if name in ['A1','B1','B2','A2']:
   assert all((E/(g+'-verification.json')).exists() for g in ['admit-A','admit-B','canary-A','canary-B'])
  results[name]=cell(name)
  checkpoint(name)
 measured={n:results[n] for n in ['A1','B1','B2','A2']}
 write(E/'result.json',{'schema':'borsuk-cold-fp32-concurrency-full1000-v1','status':'VALID_MATCHED_FULL1000_PAIR',
   'measured':measured,'admission_and_canary_pass':True,'population_tail_claim':'frozen_1000_query_cohort_only','vendor_win':False,
   'application_cache_bytes':0,'provider_cache_eviction_proven':False,
   'no_overlapping_query_cpu_or_transport_sums':True,'source_commit':a['commit']})
 write(E/'terminal.json',{'status':'VALID_MATCHED_FULL1000_PAIR','cells':8,'measurement_cells':4,'controller_exit':0})
if __name__=='__main__':
 def interrupted(signum,frame):raise RuntimeError('interrupted: incomplete INVALID, no automatic retry')
 signal.signal(signal.SIGTERM,interrupted)
 try:main()
 except BaseException as e:
  with (E/'controller-failure.txt').open('x') as f:traceback.print_exc(file=f)
  raise
