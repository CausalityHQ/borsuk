import pathlib,json,re,statistics,math,hashlib
r=pathlib.Path('/mnt/borsuk-http/evidence')
def reject_constant(x):raise ValueError('nonfinite JSON constant '+x)
objects=[]
for line in (r/'probe.log').read_text().splitlines():
 start=line.find('{')
 if start<0:continue
 try:x=json.loads(line[start:],parse_constant=reject_constant)
 except ValueError:continue
 if isinstance(x,dict) and x.get('schema')=='borsuk.two-bit-gather.primitive.v1':objects.append(x)
assert len(objects)==1,'primitive receipt missing/ambiguous'
x=objects[0];assert x['status'] in ['ACCEPT','REJECT','INVALID']
if x['status']=='INVALID':raise SystemExit(43)
assert x['identity']['source_commit']=='751b5327571c7fec17af4bbdd7f1b1594f0aaf9d' and x['identity']['avx2'] is True and x['identity']['release'] is True
assert x['identity']['arch']=='x86_64' and x['identity']['os']=='linux'
authority=json.loads((r/'expected-primitive-identity.json').read_text())
assert set(authority)=={'schema','source_commit','executable_sha256','rotated_two_bit_sha256','two_bit_generation_sha256'}
assert authority['schema']=='borsuk-two-bit-gather-primitive-identity-v1'
for field in ['source_commit','executable_sha256','rotated_two_bit_sha256','two_bit_generation_sha256']:
 assert x['identity'][field]==authority[field]
 assert re.fullmatch('[0-9a-f]{40}' if field=='source_commit' else '[0-9a-f]{64}',authority[field])

# A complete timing verdict requires both live native resource snapshots.
for controls in [x['controls_before'], x['controls_after']]:
 assert isinstance(controls,dict) and controls['path'].startswith('/')
 assert controls['cpu_max']=='100000 100000'
 for key,value in [('memory_max',268435456),('memory_swap_max',0),('memory_swap_current',0),('pids_max',128)]:
  assert type(controls[key]) is int and controls[key]==value
 for key in ['memory_current','memory_peak']:
  assert type(controls[key]) is int and 0<=controls[key]<=268435456
 events=dict(line.split() for line in controls['memory_events'].splitlines())
 for key in ['max','oom','oom_kill']:assert key in events and int(events[key])==0
assert x['controls_before']['path']==x['controls_after']['path']
assert type(x['process_cpu_end_ns']) is int and type(x['total_wall_ns']) is int
assert x['expected_ordered_records']==x['completed_ordered_records']==80 and x['blocks']==5
assert len(x['records'])==80 and len(x['geometries'])==len(x['screens'])==8
geometries={};target=4*1024*1024
for g in x['geometries']:
 key=(g['dimensions'],g['panel_rows']);assert key not in geometries and key[0] in [257,768,1024,1025] and key[1] in [32,17]
 tail=key[0]%256;padded=key[0]//256*256+(0 if tail==0 else 1<<(tail-1).bit_length());packed=(padded+3)//4;record=packed+8;reps=(target+key[1]*record-1)//(key[1]*record)
 assert g['padded_dimensions']==padded and g['packed_bytes']==packed and g['record_bytes']==record and g['repetitions']==reps
 assert g['table_bytes']==packed*256*8 and g['requested_calls_per_cell']==reps*key[1] and g['requested_bytes_per_cell']==reps*key[1]*record
 assert g['preparation_timed'] is False and g['scalar_score_bits_sha256']==g['candidate_score_bits_sha256']
 for field in ['scalar_score_bits_sha256','record_sha256','table_sha256']:assert re.fullmatch('[0-9a-f]{64}',g[field])
 geometries[key]=g
assert set(geometries)=={(d,n) for d in [257,768,1024,1025] for n in [32,17]}
pairs={};expected_ord=0
for block in range(5):
 for d in [257,768,1024,1025]:
  for n in [32,17]:
   for position,arm in enumerate(['frozen_scalar','public_dispatch'] if block%2==0 else ['public_dispatch','frozen_scalar']):
    c=x['records'][expected_ord];g=geometries[d,n]
    assert c['ordinal']==expected_ord and c['block']==block and c['dimensions']==d and c['rows']==n and c['position']==position and c['scorer']==arm and c['complete'] is True
    assert c['requested_calls']==c['completed_calls']==g['requested_calls_per_cell'] and c['requested_bytes']==c['completed_bytes']==g['requested_bytes_per_cell'] and c['record_bytes']==g['record_bytes']
    assert type(c['process_cpu_ns']) is int and c['process_cpu_ns']>0 and type(c['wall_ns']) is int and c['wall_ns']>0
    pairs[block,d,n,arm]=c['process_cpu_ns'];expected_ord+=1
pass_all=True;seen_screens=set()
for screen in x['screens']:
 d,n=screen['dimensions'],screen['rows'];assert (d,n) in geometries and (d,n) not in seen_screens;seen_screens.add((d,n))
 ratios=[pairs[b,d,n,'public_dispatch']/pairs[b,d,n,'frozen_scalar'] for b in range(5)]
 median=statistics.median(ratios);first=statistics.median(ratios[::2]);second=statistics.median(ratios[1::2]);limit=.8 if d==1024 else 1.05
 passed=median<=limit and (d!=1024 or first<=limit and second<=limit);pass_all &= passed
 assert screen['candidate_over_scalar_cpu_ratios_in_block_order']==ratios and screen['median_ratio']==median and screen['scalar_first_median_ratio']==first and screen['candidate_first_median_ratio']==second and screen['maximum_ratio']==limit and screen['pass']==passed
assert seen_screens==set(geometries) and (x['status']=='ACCEPT')==pass_all
assert x['process_cpu_end_ns']<30_000_000_000 and x['total_wall_ns']<120_000_000_000
# Original native and tee exits plus live unit/cgroup admission are checked separately.
(r/'independent-primitive-math.json').write_text(json.dumps({'status':'ACCEPT' if pass_all else 'REJECT','all80_geometry_calls_bytes_and_ratios_recomputed':True,'candidate':x['identity']['source_commit'],'performance_claim':'primitive CPU only; not ANN'},indent=2)+'\n')
raise SystemExit(0 if pass_all else 42)
