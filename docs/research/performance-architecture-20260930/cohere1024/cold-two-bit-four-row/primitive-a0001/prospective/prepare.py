import hashlib,json,subprocess
from pathlib import Path
b=Path('/data/target/borsuk-cold-membership-native');r=b/'two-bit-four-row-primitive-a0001';old=b/'two-bit-gather-primitive-a0002';c=b/'two-bit-four-row-qualification-a0004';v=b/'two-bit-four-row-control-build-a0001'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
x=json.loads((r/'expected-primitive-identity.json').read_text());op=json.loads((old/'protocol.json').read_text());p=json.loads((r/'protocol-draft.json').read_text())
assert json.loads((v/'qualified-codegen.json').read_text())['status']=='ORDERED_SIMD_AND_MATCHED_PRODUCTION_STACK_PASS'
oldtest='rotated_two_bit::tests::two_bit_gather_release_primitive';newtest=p['test']
probe=(old/'probe.sh').read_text().replace(oldtest,newtest)
oldenv='export BORSUK_TWO_BIT_GATHER_SOURCE_COMMIT='+op['candidate_commit']
envs={'SOURCE_COMMIT':x['source_commit'],'ROTATED_SHA256':x['rotated_two_bit_sha256'],'GENERATION_SHA256':x['two_bit_generation_sha256'],'ELF_SHA256':x['executable_sha256'],'CODEGEN_SHA256':x['codegen_receipt_sha256']}
probe=probe.replace(oldenv,'\n'.join('export BORSUK_TWO_BIT_FOUR_ROW_'+k+'='+val for k,val in envs.items()))
(r/'probe.sh').write_text(probe)
gates=(old/'gates.sh').read_text().replace(oldtest,newtest).replace('gather-staging','four-row-staging')
(r/'gates.sh').write_text(gates)
# Retain root assurance bodies as pinned inputs; their recorded old status is immutable.
(r/'compile-receipt.json').write_bytes((c/'independent-verification.json').read_bytes())
(r/'codegen-receipt.json').write_bytes((v/'qualified-codegen.json').read_bytes())
inputs={n:{'bytes':(r/n).stat().st_size,'sha256':sha(r/n)} for n in ['probe.sh','probe-check.py','gates.sh','expected-primitive-identity.json','compile-receipt.json','codegen-receipt.json']}
ud=(old/'user-data.sh').read_text().replace(op['prefix'],'research/semantic-router/20261008/cold-two-bit-four-row-primitive-a0001').replace('gather-staging','four-row-staging').replace('borsuk-two-bit-gather-primitive-v2','borsuk-two-bit-four-row-primitive-v1').replace('ordered two-bit gather primitive','ordered independent four-row SIMD primitive')
pin=json.loads((c/'probe-libtest.binary.json').read_text());print(pin)
key=json.loads((c/'protocol.json').read_text())['prefix']+'/supplemental/probe-libtest';pin['key']=key
ud=ud.replace(op['same_retained_elf']['key'],key).replace(op['same_retained_elf']['sha256'],pin['sha256']).replace(str(op['same_retained_elf']['bytes']),str(pin['bytes'])).replace(op['candidate_commit'],x['source_commit']).replace(op['native_identity'],'a7fe9601a9345da69223d75bf8fb6a765d1607c1f02b144e8e8763c90b0b2e8b')
for n in op['inputs']:ud=ud.replace(op['inputs'][n]['sha256'],inputs[n]['sha256'])
extra=''
for n in ['compile-receipt.json','codegen-receipt.json']:
 extra+='aws s3 cp "s3://$bucket/$prefix/inputs/'+n+'" "evidence/'+n+'" --only-show-errors\nprintf \'%s  evidence/'+n+'\\n\' '+inputs[n]['sha256']+' | sha256sum -c -\n'
ud=ud.replace('phase=probe\n',extra+'\nphase=probe\n')
(r/'user-data.sh').write_text(ud)
proto={**op,'schema':'borsuk-two-bit-four-row-primitive-v1-protocol','prefix':'research/semantic-router/20261008/cold-two-bit-four-row-primitive-a0001','candidate_commit':x['source_commit'],'native_identity':'a7fe9601a9345da69223d75bf8fb6a765d1607c1f02b144e8e8763c90b0b2e8b','same_retained_elf':{**pin,'path':str(c/'probe-libtest')},'inputs':inputs,'userdata':{'bytes':len(ud.encode()),'sha256':sha(r/'user-data.sh')},'compile_receipt':{'path':str(c/'independent-verification.json'),'sha256':sha(c/'independent-verification.json')},'codegen_receipt':{'path':str(v/'qualified-codegen.json'),'sha256':sha(v/'qualified-codegen.json')},'test':newtest}
(r/'protocol.json').write_text(json.dumps(proto,indent=2)+'\n')
for n in ['probe.sh','gates.sh','user-data.sh']:subprocess.run(['bash','-n',str(r/n)],check=True)
assert 'two-bit-gather' not in ud and oldtest not in probe+gates and op['candidate_commit'] not in ud+probe
print('PASS prepared exact retained binary/source/receipt pins; no launch')
