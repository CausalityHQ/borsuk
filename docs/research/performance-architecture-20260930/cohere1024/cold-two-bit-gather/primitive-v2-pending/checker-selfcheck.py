import tempfile,pathlib,json,hashlib,subprocess,copy
src=pathlib.Path(__file__).with_name('probe-check.py').read_text()
with tempfile.TemporaryDirectory() as tmp:
 r=pathlib.Path(tmp);checker=r/'check.py';checker.write_text(src.replace("'/mnt/borsuk-http/evidence'",repr(tmp)))
 geometry=[];cells=[];screens=[]
 for d in [257,768,1024,1025]:
  for n in [32,17]:
   tail=d%256;padded=d//256*256+(0 if tail==0 else 1<<(tail-1).bit_length());packed=(padded+3)//4;record=packed+8;reps=(268435456+n*record-1)//(n*record)
   geometry.append(dict(dimensions=d,panel_rows=n,padded_dimensions=padded,packed_bytes=packed,record_bytes=record,repetitions=reps,table_bytes=packed*2048,requested_calls_per_cell=reps*n,requested_bytes_per_cell=reps*n*record,preparation_timed=False,scalar_score_bits_sha256='a'*64,candidate_score_bits_sha256='a'*64,record_sha256='b'*64,table_sha256='c'*64))
   screens.append(dict(dimensions=d,rows=n,candidate_over_scalar_cpu_ratios_in_block_order=[.75]*5,median_ratio=.75,scalar_first_median_ratio=.75,candidate_first_median_ratio=.75,maximum_ratio=.8 if d==1024 else 1.05,pass_=True));screens[-1]['pass']=screens[-1].pop('pass_')
 for block in range(5):
  for g in geometry:
   for pos,arm in enumerate(['frozen_scalar','public_dispatch'] if block%2==0 else ['public_dispatch','frozen_scalar']):
    cells.append(dict(ordinal=len(cells),block=block,dimensions=g['dimensions'],rows=g['panel_rows'],position=pos,scorer=arm,complete=True,requested_calls=g['requested_calls_per_cell'],completed_calls=g['requested_calls_per_cell'],requested_bytes=g['requested_bytes_per_cell'],completed_bytes=g['requested_bytes_per_cell'],record_bytes=g['record_bytes'],process_cpu_ns=100000000 if arm=='frozen_scalar' else 75000000,wall_ns=120000000))
 x=dict(schema='borsuk.two-bit-gather.primitive.v2',status='ACCEPT',identity=dict(source_commit='45456b5168043d5405b7b792ed9360be5e657cfc',avx2=True,release=True,arch='x86_64',os='linux'),expected_ordered_records=80,completed_ordered_records=80,blocks=5,records=cells,geometries=geometry,screens=screens,process_cpu_end_ns=7000000000,total_wall_ns=9600000000,target_encoded_bytes_per_cell=268435456,minimum_cell_cpu_ns=50000000)
 controls=dict(path='/fixture-native-unit',cpu_max='100000 100000',memory_max=268435456,memory_swap_max=0,memory_swap_current=0,pids_max=128,memory_current=20000000,memory_peak=21000000,memory_events='max 0\noom 0\noom_kill 0\n')
 x['controls_before']=copy.deepcopy(controls);x['controls_after']=copy.deepcopy(controls)
 authority=dict(schema='borsuk-two-bit-gather-primitive-identity-v1',source_commit=x['identity']['source_commit'],executable_sha256='d'*64,rotated_two_bit_sha256='e'*64,two_bit_generation_sha256='f'*64)
 for field in ['executable_sha256','rotated_two_bit_sha256','two_bit_generation_sha256']:x['identity'][field]=authority[field]
 (r/'expected-primitive-identity.json').write_text(json.dumps(authority))
 def run(v):
  (r/'probe.log').write_text('test primitive ... '+json.dumps(v)+'\n');return subprocess.run(['python3',str(checker)],capture_output=True).returncode
 assert run(x)==0
 mutations=[lambda z:z.update(minimum_cell_cpu_ns=1),lambda z:z.update(target_encoded_bytes_per_cell=4194304),lambda z:z['records'][0].update(process_cpu_ns=49999999),lambda z:z['geometries'][0].update(padded_dimensions=512),lambda z:z['geometries'][6].update(packed_bytes=320),lambda z:z['records'].pop(),lambda z:z['records'][0].update(completed_calls=1),lambda z:z['screens'][0].update(median_ratio=.7),lambda z:z['screens'].__setitem__(7,copy.deepcopy(z['screens'][0])),lambda z:z['identity'].update(avx2=False),lambda z:z['records'][0].update(process_cpu_ns=True),lambda z:z['controls_after'].update(memory_events='max 1\noom 0\noom_kill 0\n'),lambda z:z['controls_after'].update(path='/different-unit'),lambda z:z['controls_before'].update(memory_max=True),lambda z:z['identity'].update(executable_sha256='0'*64),lambda z:z['identity'].update(rotated_two_bit_sha256='0'*64)]
 for mutate in mutations:
  bad=copy.deepcopy(x);mutate(bad);assert run(bad)!=0
 reject=copy.deepcopy(x);reject['status']='REJECT'
 for c in reject['records']:
  if c['scorer']=='public_dispatch':c['process_cpu_ns']=110000000
 for s in reject['screens']:s.update(candidate_over_scalar_cpu_ratios_in_block_order=[1.1]*5,median_ratio=1.1,scalar_first_median_ratio=1.1,candidate_first_median_ratio=1.1,**{'pass':False})
 assert run(reject)==42
 print('PASS synthetic checker positive, valid REJECT42, missing cell, false counts/ratios, duplicate screen, absent ISA and bool CPU negatives; no Rust/ANN execution.')
