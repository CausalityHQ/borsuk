import json,pathlib,statistics,math,sys,hashlib
r=pathlib.Path('/mnt/borsuk-http/evidence')
reports=[]
for line in (r/'probe.log').read_text().splitlines():
 try:x=json.loads(line[line.index('{'):])
 except ValueError:continue
 if isinstance(x,dict) and x.get('schema')=='borsuk-page-auth-backend-probe-v1':reports.append(x)
assert len(reports)==1,'missing/ambiguous probe JSON'
x=reports[0]
(r/'probe-report.json').write_text(json.dumps(x,indent=2)+'\n')
assert x['status'] in ['PASS','REJECT','INVALID']
props=dict(s.split('=',1) for s in (r/'probe-systemd.txt').read_text().splitlines() if '=' in s)
assert props.get('MainPID')=='0' and props.get('ActiveState')=='inactive'
assert props.get('ExecMainStatus')=='0' and props.get('Result')=='success'
assert int((r/'probe-memory.peak.after').read_text())<=268435456 and int((r/'probe-memory.swap.peak.after').read_text())==0
events=dict(v.split() for v in (r/'probe-memory.events.after').read_text().splitlines())
assert all(int(events[k])==0 for k in ['max','oom','oom_kill'])
assert props['RuntimeMaxUSec']=='2min' and props['LimitCPU']=='30'
assert props['MemoryMax']=='268435456' and props['MemorySwapMax']=='0' and props['TasksMax']=='128'
# Admission readbacks were checked inside the exact owned leaf before exec.
for field,val in [('cpu.max','100000 100000'),('memory.max','268435456'),('memory.swap.max','0'),('pids.max','128'),('cpuset.cpus.effective','0')]:
 assert (r/('probe-'+field+'.before')).read_text().strip()==val
assert (r/'probe-cpu-limit.txt').read_text().strip()=='30'
assert '1 passed; 0 failed; 0 ignored; 0 measured' in (r/'probe.log').read_text()
assert (r/'probe.log').read_text().count('test sq8_page_authority::tests::page_auth_backend_release_probe ... ')==1
assert (r/'probe-native-exit').read_text().strip()=='0'
if x['status']=='INVALID':
 (r/'probe-independent.json').write_text(json.dumps({'status':'INVALID','report_preserved':True,'performance_claim':False})+'\n');sys.exit(43)
assert x['full_class_weights_bytes']==[8971573248,16699756416]
assert x['screen']=={'minimum_weighted_cpu_saving':.2,'maximum_full_class_median_ratio':1.05}
assert x['external_limits_required']=={'cpu_cores':1,'memory_bytes':268435456,'swap_bytes':0,'cpu_seconds':30,'wall_seconds':120}
assert x['total_cpu_ns']<28000000000 and x['total_wall_ns']<110000000000
classes=['source_full','sq8_full','source_tail','sq8_tail']
spec=[(32,264,32,0,8448),(256,1036,256,0,265216),(49,264,32,1,4488),(273,1036,256,1,17612)]
assert len(x['geometry'])==4 and len(x['blocks'])==40
for g,label,(rows,record,page_rows,page,length) in zip(x['geometry'],classes,spec):
 assert g['label']==label and g['dimensions']==1024 and g['rows']==rows and g['record_bytes']==record and g['page_rows']==page_rows
 assert g['first_page']==page and g['last_page']==page and g['payload_bytes']==length and g['object_bytes']==rows*record
 assert g['range']==[page*page_rows*record,rows*record]
 raw=bytes((i*37+i//251+1024*11)%256 for i in range(rows*record))
 expected=hashlib.sha256(raw[g['range'][0]:g['range'][1]]).hexdigest()
 assert g['payload_sha256']==g['candidate_payload_sha256']==expected
 assert g['object_sha256']==hashlib.sha256(raw).hexdigest()
samples={c:{'sha2_control':[],'aws_lc_candidate':[]} for c in classes}
for idx,b in enumerate(x['blocks']):
 block=idx//8;label=classes[(idx%8)//2];pos=idx%2
 order=['sha2_control','aws_lc_candidate'] if block%2==0 else ['aws_lc_candidate','sha2_control']
 assert (b['block'],b['class'],b['position'],b['backend'])==(block,label,pos,order[pos])
 length=spec[classes.index(label)][4]
 assert b['requested_calls']==(33554432+length-1)//length
 assert 0<=b['calls']<=b['requested_calls'] and b['bytes']==b['calls']*length
 if x['semantic_match']:
  assert b['complete'] is True and b['calls']==b['requested_calls'] and b['error'] is None and b['cpu_ns']>0 and b['wall_ns']>0
  samples[label][b['backend']].append(b['cpu_ns']/b['bytes'])
if not x['semantic_match']:
 assert x['status']=='REJECT'
 (r/'probe-independent.json').write_text(json.dumps({'status':'REJECT','reason':'native semantic mismatch','performance_claim':False})+'\n');sys.exit(42)
assert len(x['summaries'])==4
control=[];candidate=[];ratios=[]
for summary,label in zip(x['summaries'],classes):
 a=samples[label]['sha2_control'];b=samples[label]['aws_lc_candidate'];assert len(a)==len(b)==5
 ca,cb,ratio=statistics.median(a),statistics.median(b),statistics.median(y/z for y,z in zip(b,a))
 assert summary['class']==label
 for key,expected in [('control_median_cpu_ns_per_byte',ca),('candidate_median_cpu_ns_per_byte',cb),('median_paired_cpu_ratio',ratio)]:assert math.isclose(summary[key],expected,rel_tol=1e-12,abs_tol=1e-12)
 control.append(ca);candidate.append(cb);ratios.append(ratio)
w=[8971573248,16699756416]
saving=1-sum(w[i]*candidate[i] for i in range(2))/sum(w[i]*control[i] for i in range(2))
assert math.isclose(x['weighted_cpu_saving'],saving,rel_tol=1e-12,abs_tol=1e-12)
status='PASS' if saving>=.2 and all(v<=1.05 for v in ratios[:2]) else 'REJECT'
assert status==x['status']
(r/'probe-independent.json').write_text(json.dumps({'status':status,'weighted_cpu_saving':saving,'full_class_median_paired_ratios':ratios[:2],'all40_records_validated':True,'geometry_and_payload_digests_independently_matched':True,'external_leaf_caps_readback':True,'performance_claim':False},indent=2)+'\n')
sys.exit(0 if status=='PASS' else 42)
