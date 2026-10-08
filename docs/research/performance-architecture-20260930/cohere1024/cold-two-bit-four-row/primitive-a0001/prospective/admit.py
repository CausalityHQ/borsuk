import copy,hashlib,json,subprocess,tempfile
from pathlib import Path
r=Path('/data/target/borsuk-cold-membership-native/two-bit-four-row-primitive-a0001');old=r.parent/'two-bit-gather-primitive-a0002'
p=json.loads((r/'protocol.json').read_text());identity=json.loads((r/'expected-primitive-identity.json').read_text())
for n,pin in p['inputs'].items():
 body=(r/n).read_bytes();assert len(body)==pin['bytes'] and hashlib.sha256(body).hexdigest()==pin['sha256'];assert pin['sha256'] in (r/'user-data.sh').read_text()
el=Path(p['same_retained_elf']['path'])
with el.open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==p['same_retained_elf']['sha256']
assert el.stat().st_size==p['same_retained_elf']['bytes']
for n,status in [('compile-receipt.json','FULL_NATIVE_CORRECTNESS_VERIFIED_SERVING_STACK_PENDING'),('codegen-receipt.json','ORDERED_SIMD_AND_MATCHED_PRODUCTION_STACK_PASS')]:assert json.loads((r/n).read_text())['status']==status
assert identity['codegen_receipt_sha256']==p['inputs']['codegen-receipt.json']['sha256']
assert p['cost_reservation']['spot_bid_max_usd_hour']*p['host']['machine_seconds']/3600<=p['cost_reservation']['compute_usd']
for n in ['probe.sh','gates.sh','user-data.sh']:subprocess.run(['bash','-n',str(r/n)],check=True)
check=(r/'probe-check.py').read_text()
x=json.loads((old/'native-report.json').read_text());x['schema']='borsuk.two-bit-four-row.primitive.v1';x['status']='ACCEPT';x['identity'].update({k:v for k,v in identity.items() if k!='schema'})
for g in x['geometries']:
 g['validation_batching_and_unit_maximum_timed']=True;g['unit_score_bits']=0;g['row_order']='ascending 0..panel_rows, repeated'
for cell in x['records']:
 cell['scorer']={'frozen_scalar':'authenticated_scalar','public_dispatch':'authenticated_four_row'}[cell['scorer']]
 cell['process_cpu_ns']=100000000 if cell['scorer']=='authenticated_scalar' else 75000000
 cell['wall_ns']=cell['process_cpu_ns']+1000
for s in x['screens']:
 s.update(candidate_over_scalar_cpu_ratios_in_block_order=[.75]*5,median_ratio=.75,scalar_first_median_ratio=.75,candidate_first_median_ratio=.75);s['pass']=True
def run(obj,auth=identity):
 with tempfile.TemporaryDirectory() as td:
  d=Path(td);(d/'probe.log').write_text(json.dumps(obj)+'\n');(d/'expected-primitive-identity.json').write_text(json.dumps(auth));(d/'check.py').write_text(check.replace("'/mnt/borsuk-http/evidence'",repr(td)))
  q=subprocess.run(['python3',str(d/'check.py')],capture_output=True);return q.returncode,q.stderr.decode()
code,error=run(x);assert code==0,error
negatives=0
for key in ['source_commit','executable_sha256','rotated_two_bit_sha256','two_bit_generation_sha256','codegen_receipt_sha256']:
 z=copy.deepcopy(x);z['identity'][key]='0'*len(z['identity'][key]);assert run(z)[0]!=0;negatives+=1
for mutate in [lambda z:z['records'][0].update(process_cpu_ns=49999999),lambda z:z['records'][0].update(completed_bytes=0),lambda z:z['records'][0].update(scorer='wrong'),lambda z:z['geometries'][0].update(candidate_score_bits_sha256='0'*64),lambda z:z['controls_after'].update(memory_swap_current=1),lambda z:z['controls_after'].update(memory_events='low 0\nhigh 0\nmax 1\noom 0\noom_kill 0\n'),lambda z:z.update(completed_ordered_records=79),lambda z:z.update(status='REJECT')]:
 z=copy.deepcopy(x);mutate(z);assert run(z)[0]!=0;negatives+=1
receipt={'status':'SOURCE_BOUND_PRIMITIVE_ADMISSION_PASS_NATIVE_CANARY_PENDING','source_commit':p['candidate_commit'],'exact_retained_elf_authenticated':True,'input_pins_and_shell_syntax_verified':True,'synthetic_math_accept_verified':True,'synthetic_refusals':negatives,'native_execution':False,'launch_allowed_after_frozen_protocol_and_native_canary':True}
(r/'local-admission.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt))
