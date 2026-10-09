"""Draft CLOSED-receipt verifier; not launch authority. No incomplete data reads."""
import pathlib,json,hashlib,tarfile,subprocess,importlib.util,sys,re,os
from fractions import Fraction
def release_cache(path):
 with path.open('rb') as fd:os.fsync(fd.fileno());os.posix_fadvise(fd.fileno(),0,0,os.POSIX_FADV_DONTNEED)
r=pathlib.Path(sys.argv[1]);p=json.loads((r/'protocol.json').read_text());t=json.loads((r/'terminal.json').read_text());job=json.loads((r/'active-job.json').read_text());i=job['instance_id']
assert p['status']=='FROZEN_OVERHEAD_CANARY' and t['schema']=='borsuk-sq8-attribution-overhead-canary-closed-v1'
assert t['instance_id']==i and t['binary_source_commit']==p['candidate'] and t['qualified_source_identity_sha256']==p['qualification']['native_source_identity_sha256'];assert t['original_exit']==t['exit']==t['native_exit']==0
assert json.loads((r/'wait.json').read_text())=={'instance_id':i,'terminated':True}
a=r/'evidence.tar.gz';assert a.stat().st_size==t['evidence']['bytes']
h=hashlib.sha256()
with a.open('rb') as f:
 for b in iter(lambda:f.read(1048576),b''):h.update(b)
assert h.hexdigest()==t['evidence']['sha256'];col=r/'verified-collected';col.mkdir(exist_ok=False)
with tarfile.open(a) as archive:
 members=archive.getmembers();assert len(members)<=4096 and sum(x.size for x in members)<=2147483648
 for x in members:assert not x.name.startswith('/') and '..' not in pathlib.PurePosixPath(x.name).parts and (x.isdir() or x.isfile())
 for member in members:
  archive.extract(member,col,filter='data')
  if member.isfile():release_cache(col/member.name)
seen=set()
for line in (r/'artifacts.sha256').read_text().splitlines():
 digest,name=line.split('  ',1);path=pathlib.PurePosixPath(name);assert not path.is_absolute() and '..' not in path.parts and name not in seen;seen.add(name)
 assert hashlib.sha256((col/name.removeprefix('./')).read_bytes()).hexdigest()==digest
 release_cache(col/name.removeprefix('./'))
assert {x.name for x in col.iterdir() if x.is_file()}=={pathlib.PurePosixPath(x).name for x in seen}
for name,pin in p['inputs'].items():
 b=(r/name).read_bytes();assert b==(col/name).read_bytes() and len(b)==pin['bytes'] and hashlib.sha256(b).hexdigest()==pin['sha256']
for name,value in {'cpu.max':'100000 100000','memory.max':'536870912','memory.swap.max':'0','pids.max':'256'}.items():assert (col/(name+'.before')).read_text().strip()==value
assert int((col/'memory.peak.after').read_text())<=536870912 and int((col/'memory.swap.peak.after').read_text())==0
mem=dict(x.split() for x in (col/'memory.events.after').read_text().splitlines());assert mem['oom']==mem['oom_kill']=='0'
assert not (col/'scratch-files.txt').read_text().strip();assert (col/'final-exit').read_text().strip()=='0'
state=(col/'systemd-after.txt').read_text();assert 'MainPID=0' in state and 'ActiveState=inactive' in state
negative=[json.loads(x) for x in (col/'negative-cli.jsonl').read_text().splitlines() if x.strip()];assert (col/'negative-cli.native-exit').read_text().strip()=='2'
assert [x['phase'] for x in negative]==['identity','terminal'];ns=negative[-1]['summary'];assert ns['status']=='INVALID' and ns['stage']=='config' and ns['error']=='artifact identity/SHA' and ns['completed_queries']==0 and ns['truth_opened'] is False and ns['all_queries_sealed'] is False
assert all(v==0 for arm in ns['charges'].values() for v in arm.values())
assert negative[0]['binary_sha256']==p['binary']['sha256'] and negative[0]['config_sha256']=='0'*64
panel=json.loads(pathlib.Path(p['panel']['path']).read_text());assert hashlib.sha256(pathlib.Path(p['panel']['path']).read_bytes()).hexdigest()==p['panel']['sha256']
refs={x['ordinal']:x['reference'] for x in panel['expected_rows']};runs=[];old_hits={}
for arm,pin in panel['source_references'].items():
 path=pathlib.Path(pin['path']);assert path.stat().st_size==pin['bytes'];digest=hashlib.sha256();hits={}
 with path.open('rb') as fd:
  for b in iter(lambda:fd.read(1048576),b''):digest.update(b)
 assert digest.hexdigest()==pin['sha256']
 with path.open() as fd:
  for line in fd:
   row=json.loads(line)
   if row['phase']=='recall':hits[row['ordinal']]=row['hits10']
 assert len(hits)==1000;old_hits[arm]=hits
source_fields={'runner_source_sha256':'bin/check_cohere_native_baseline.rs','generation_source_sha256':'two_bit_generation.rs','router_source_sha256':'semantic_unit_router.rs','codec_source_sha256':'rotated_two_bit.rs','source_plane_source_sha256':'two_bit_source.rs','sq8_range_source_sha256':'sq8_s3_range.rs','returned_source_sha256':'returned_sq8.rs'}
source_hashes={key:hashlib.sha256(subprocess.check_output(['git','show',p['candidate']+':crates/borsuk/src/'+name])).hexdigest() for key,name in source_fields.items()}
assert all(negative[0][key]==value for key,value in source_hashes.items())

previous_end=None
assert len(p['order'])==256
for call in p['order']:
 cfg=json.loads((r/call['config']).read_text());ords=cfg['execution']['ordinals'];label=call['output'].removesuffix('.jsonl');arm=call['block'][0]
 assert cfg['execution']['trace']==call['trace'] and (col/(label+'.native-exit')).read_text().strip()=='0'
 import datetime
 start=datetime.datetime.fromisoformat((col/(label+'.started')).read_text().strip().replace('Z','+00:00'));end=datetime.datetime.fromisoformat((col/(label+'.finished')).read_text().strip().replace('Z','+00:00'));assert end>=start and (previous_end is None or start>=previous_end);previous_end=end
 prefix=hashlib.sha256();offset=0;queries=[];seal=None;terminal=None;recalls=[]
 for raw in (col/call['output']).open('rb'):
  assert len(raw)<=65536 and raw.endswith(b'\n');d=json.loads(raw);phase=d['phase']
  if phase=='identity':assert offset==0 and d['binary_sha256']==p['binary']['sha256'] and d['config_sha256']==p['inputs'][call['config']]['sha256'] and all(d[key]==value for key,value in source_hashes.items())
  if phase=='bound_inputs':
   assert all(d[key]==cfg[key] for key in ['rows','count','dimensions','k','metric','generation_root_sha256','generation_prefix','fetch_parallelism','dataset','revision','corpus_source_first','query_source_first','backend'])
   assert d['requests_sha256']==cfg['requests']['sha256'] and d['truth_sha256']==cfg['truth']['sha256'] and d['execution']==cfg['execution']
  if phase=='query':
   slot=len(queries);assert seal is None and d['ordinal']==ords[slot] and d['selected_slot']==slot and d['truth_opened'] is False
   assert d['returned_count']==10 and d['underfill'] is False
   canonical=json.dumps({k:d[k] for k in ['ordinal','returned','charges','plan','trace']},sort_keys=True,separators=(',',':')).encode()
   assert hashlib.sha256(canonical).hexdigest()==refs[d['ordinal']][arm]['canonical_semantic_record_sha256']
   assert ('diagnostic' in d)==call['trace']
   if call['trace']:assert 'omitted' not in d['diagnostic'] and d['diagnostic']['sq8_range_trace']['outcome']=='ranked' and d['diagnostic']['sq8_range_trace']['dropped_ranges']==0
   if call['trace']:
    tr=d['diagnostic']['sq8_range_trace'];assert len(tr['range_fields'])==22 and len(tr['ranges'])==tr['planned_ranges']==d['charges']['sq8']['submitted_gets']
    assert tr['all_ranges_complete_ns']<=tr['rank_start_ns']<=tr['rank_end_ns']<=tr['release_end_ns']<=d['stages']['sq8']['end_ns']
   queries.append(d)
  if phase=='all_queries_sealed':
   assert seal is None and len(queries)==64 and d['truth_opened'] is False and d['requires_successful_sync'] and d['requires_successful_directory_sync']
   assert d['prefix_bytes']==offset and d['prefix_sha256']==prefix.hexdigest();prefix.update(raw);offset+=len(raw);seal={'prefix_bytes':d['prefix_bytes'],'prefix_sha256':d['prefix_sha256'],'sealed_bytes':offset,'sealed_sha256':prefix.hexdigest()};continue
  if phase=='recall':assert seal is not None and d['ordinal']==ords[len(recalls)] and d['hits10']==old_hits[arm][d['ordinal']];recalls.append(d)
  if phase=='terminal':assert terminal is None;terminal=d['summary']
  if seal is None:prefix.update(raw);offset+=len(raw)
 assert terminal is not None and seal is not None and len(queries)==len(recalls)==64 and terminal['status']=='MEASURED' and terminal['complete']
 assert all(terminal[k]==v for k,v in seal.items()) and terminal['all_queries_sealed']
 assert terminal['recall_numerator']==sum(x['hits10'] for x in recalls) and terminal['recall_denominator']==640
 assert terminal['population_count']==1000 and terminal['selected_count']==terminal['executed_count']==64 and terminal['population_percentiles_valid'] is False
 assert terminal['query_wall_ns']==sum(x['query_wall_ns'] for x in queries) and terminal['query_process_cpu_ns']==sum(x['query_process_cpu_ns'] for x in queries)
 release_cache(col/call['output'])
 timing=(col/(label+'.time')).read_text();assert 'Exit status: 0' in timing
 def seconds(key):
  match=re.search(re.escape(key)+r': ([0-9]+\.[0-9]{2})',timing);assert match;return match.group(1)
 runs.append({'arm':call['arm'],'quad':call['quad'],'trace':call['trace'],'ordinals':ords,'wall_ns':[x['query_wall_ns'] for x in queries],'cpu_ns':[x['query_process_cpu_ns'] for x in queries],'os_user_seconds':seconds('User time (seconds)'),'os_system_seconds':seconds('System time (seconds)')})
assert hashlib.sha256(pathlib.Path('/tmp/borsuk-attribution-canary-decision-v2.py').read_bytes()).hexdigest()==p['decision_sha256']
s=importlib.util.spec_from_file_location('decision','/tmp/borsuk-attribution-canary-decision-v2.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
out=m.decision(runs);out.update({'receipt_validation_performed':True,'candidate':p['candidate'],'instance_id':i,'terminated':True,'semantic_parity':True,'sealed_before_truth':True,'native_calls':256,'searches':16384});(r/'independent-canary-verification.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))
