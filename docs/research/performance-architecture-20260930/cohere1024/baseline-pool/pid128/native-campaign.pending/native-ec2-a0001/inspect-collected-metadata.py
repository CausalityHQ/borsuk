"""Root artifact inspection: JSON receipts/resource counters and opaque hashes only.
No entrypoint, native/input validator, request/GT read, or ANN/reducer execution.
"""
import hashlib,json,pathlib,re,tarfile
d=pathlib.Path('/tmp/borsuk-pid128-native-ec2-a0001.final')
def digest(b):return hashlib.sha256(b).hexdigest()
def obj(b):return json.loads(b)
terminal=obj((d/'collection/terminal.json').read_bytes())
body=(d/'collection/evidence.tar.gz').read_bytes()
assert len(body)==terminal['evidence_bytes'] and digest(body)==terminal['evidence_sha256']
assert terminal['instance_id']=='i-098477877e462a70d' and terminal['bootstrap_exit']==terminal['test_exit']==terminal['cleanup_exit']==0
assert (d/'collection/volume.absent').read_text().strip()=='true'
assert (d/'collection/root.status').read_text().strip()=='COLLECTED_NATIVE_GATE_EVIDENCE_ROOT_REPLAY_REQUIRED'
show=(d/'watch.terminal.show').read_text()
assert all(x in show.splitlines() for x in ['InvocationID=d586090c878f43e084e88a2e0a240fbf','MainPID=0','ExecMainCode=1','ExecMainStatus=0','Result=success'])
archive=tarfile.open(d/'collection/evidence.tar.gz')
members=archive.getmembers(); assert sum(m.size for m in members)<=134217728
assert all((m.isfile() or m.isdir()) and not m.name.startswith('/') and '..' not in pathlib.PurePosixPath(m.name).parts for m in members)
files={m.name:archive.extractfile(m).read() for m in members if m.isfile()}
assert len(files)==sum(m.isfile() for m in members)
def read(n):return files[n]
def map_path(p):
 if p.startswith('/mnt/borsuk-pid-evidence/'):return 'borsuk-pid-evidence/'+p.removeprefix('/mnt/borsuk-pid-evidence/')
 if p.startswith('/mnt/borsuk-pool-pid/'):return 'borsuk-validator/replay/'+p.removeprefix('/mnt/borsuk-pool-pid/')
 if p.startswith('/var/lib/borsuk-validator/'):return 'borsuk-validator/'+p.removeprefix('/var/lib/borsuk-validator/')
 raise AssertionError('unretained path '+p)
def art(a):
 b=read(map_path(a['path']));assert len(b)==a['bytes'] and digest(b)==a['sha256'];return b
stages={};total_manifest_entries=0
for stage in ['admission','staging','negative','widths']:
 base='borsuk-pid-evidence/'+stage+'/'
 t=obj(read(base+'terminal.json')); outer=obj(read('borsuk-pid-evidence/'+stage+'-outer/outer-closure.json'))
 assert outer['status']=='CLOSED' and outer['drained'] is True
 assert outer['recipe_sha256']=='f0273d2353b198030dd9df4718a142a88c009b6e78861f8cf9d5b9f1dd9639d3'
 actual=98 if stage=='negative' else 0
 assert outer['actual_outer_exit']==actual
 assert int(read(base+'wrapper.exit'))==actual
 assert outer['control_group']=='/system.slice/'+outer['unit']
 assert read('borsuk-pid-evidence/'+stage+'.invocation').decode().strip()==outer['invocation_id']
 manager=art(outer['manager_show']).decode().splitlines()
 assert 'InvocationID='+outer['invocation_id'] in manager and 'MainPID=0' in manager
 assert 'ExecMainCode=1' in manager and 'ExecMainStatus='+str(actual) in manager
 drain=obj(art(outer['drain_proof']))
 assert drain['state'] in ['empty','removed'] and drain['invocation_id']==outer['invocation_id']
 assert drain['path']=='/sys/fs/cgroup'+outer['control_group']
 art(outer['outer_exit_file'])
 if stage=='admission':
  assert digest(read(base+'terminal.json'))==outer['terminal_sha256']
  assert digest(read(base+'closure.sha256'))==outer['manifest_sha256']
  assert digest(read(base+'wrapper.exit'))==outer['wrapper_exit_sha256']
  assert digest(read(base+'admission.json'))==outer['admission_sha256']
 else:
  for key in ['terminal','manifest','wrapper_exit']:art(outer[key])
 listed=set()
 for line in read(base+'closure.sha256').decode().splitlines():
  h,rel=line.split('  ',1);assert re.fullmatch('[0-9a-f]{64}',h) and rel.startswith('./')
  assert '..' not in pathlib.PurePosixPath(rel).parts
  name=base+rel[2:];assert name not in listed;listed.add(name);assert digest(read(name))==h
 actual_names={n for n in files if n.startswith(base)}-{base+'closure.sha256',base+'wrapper.exit'}
 assert listed==actual_names, (stage,sorted(actual_names-listed),sorted(listed-actual_names))
 total_manifest_entries+=len(listed)
 stages[stage]={'actual_outer_exit':actual,'manifest_entries':len(listed),'drain_state':drain['state'],'terminal':t}
# Exact replay roster: never parse raw query result bodies.
replay=read('borsuk-validator/replay-roster.tsv').decode().splitlines()
assert len(replay)==18
for line in replay:
 rel,size,h=line.split('\t');b=read('borsuk-validator/replay/'+rel)
 assert len(b)==int(size) and int(size)<=4194304 and digest(b)==h
assert read('borsuk-pid-evidence/campaign.exit')==b'original=0 cleanup=0 phase=closed\n'
negative=obj(art(obj(read('borsuk-pid-evidence/negative-outer/outer-closure.json'))['controlled_negative_proof']))
assert negative['timeout_exit']==124 and 500<=negative['elapsed_centiseconds']<6000
assert all(negative[k] is True for k in ['resource_events_unchanged','descendant_alive_after_timeout','drain_before_natural_exit'])
def counters(s):
 if s=='absent':return {}
 result={}
 for line in s.splitlines():
  k,v=line.split();assert k not in result;result[k]=int(v)
 return result
phases=[]
for name in sorted(files):
 if '/phases/' not in name or not name.endswith('/resources.initial'):continue
 base=name.removesuffix('resources.initial')
 before=[obj(x) for x in read(base+'resources.initial').splitlines()]
 after=[obj(x) for x in read(base+'resources.final').splitlines()]
 assert len(before)==len(after) and [x['path'] for x in before]==[x['path'] for x in after]
 for b,a in zip(before,after):
  for key in ['pids_events','memory_events','memory_swap_events']:
   bc,ac=counters(b[key]),counters(a[key]);assert bc==ac,(base,key,bc,ac)
  assert a['memory_swap_current'] in ['0','absent'] and a['memory_swap_peak'] in ['0','absent']
 scope=after[0]; assert scope['pids_max']=='128' and scope['memory_swap_max']=='0'
 phase_name=base.rstrip('/').split('/')[-1]
 large=phase_name in ['prepare','derive','copy','generation','publish']
 assert scope['memory_max']==str(8589934592 if large else 268435456 if '/negative/' in base else 536870912)
 assert scope['cpu_max']==('400000 100000' if large else '100000 100000')
 assert scope['cpuset_cpus_effective']==('0-3' if large else '0')
 assert int(scope['pids_peak'])<=128
 ph=obj(read(base+'drain.json'));assert ph['state'] in ['empty','removed']
 if '/negative/' in base:
  # The deliberately killed fixture has no native completion receipt.
  assert base+'native.exit' not in files
  assert int(read(base+'timeout.exit'))==int(read(base+'time.exit'))==124
  assert int(read(base+'payload.exit'))==98
  native_exit=None
 else:
  assert int(read(base+'native.exit'))==0
  for name in ['timeout.exit','time.exit','tee.exit','payload.exit','native-log.exit','time-log.exit','supervisor-log.exit','manager.start.exit','manager.stop.exit']:
   assert int(read(base+name))==0,(base,name)
  native_exit=0
 phases.append({'path':base,'native_exit':native_exit,'controlled_timeout':native_exit is None,'pids_peak':int(scope['pids_peak']),'pids_max_events':counters(scope['pids_events'])['max'],'memory_peak_bytes':int(scope['memory_peak']),'drain_state':ph['state']})
summaries={}
for width in [16,32]:
 base='borsuk-pid-evidence/widths/phases/query'+str(width)+'/'
 s=obj(read(base+'native.stdout'));assert s['status']=='MEASURED' and s['complete'] and s['all_queries_sealed']
 assert s['queries']==s['executed_count']==32 and s['physical_s3_measured'] is False and s['population_percentiles_valid'] is False
 raw=read('borsuk-validator/replay/prepared-parent/query'+str(width)+'/result.jsonl')
 # Query seal precedes opening GT; later recall/terminal rows extend the file.
 assert 0<s['prefix_bytes']<s['sealed_bytes']<=len(raw)
 assert digest(raw[:s['sealed_bytes']])==s['sealed_sha256']
 assert digest(raw[:s['prefix_bytes']])==s['prefix_sha256']
 assert s['recall_denominator']==320 and 0<=s['recall_numerator']<=320 and s['underfilled_queries']==0
 assert s['mean_recall10']==s['recall_numerator']/s['recall_denominator']
 summaries[str(width)]={k:s[k] for k in ['status','queries','mean_recall10','recall_numerator','recall_denominator','underfilled_queries','query_wall_ns','observed_process_peak_bytes','physical_s3_measured','population_percentiles_valid','sum','requests_sha256','truth_sha256','generation_root_sha256']}
report={'status':'PID128_REAL_INPUT_MECHANICS_METADATA_VERIFIED','instance_id':terminal['instance_id'],'terminated':True,'root_volume_absent':True,'archive_sha256':digest(body),'archive_bytes':len(body),'archive_regular_files':len(files),'sealed_manifest_entries':total_manifest_entries,'replay_entries':len(replay),'stages':stages,'phases':phases,'controlled_negative':negative,'native_summaries':summaries,'local_native_execution':False,'local_request_truth_opened':False,'performance_claim':False,'cold_claim':False,'competitor_claim':False,'population_recall_claim':False,'remaining':'Prospective cold S3 fixture/protocol, runtime input and disposable staging gates, then actual matched cold execution. No full-cohort or competitor acceptance from this prefix mechanics gate.'}
(d/'metadata-verification.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:report[k] for k in ['status','archive_regular_files','sealed_manifest_entries','replay_entries','phases','native_summaries']},indent=2))
