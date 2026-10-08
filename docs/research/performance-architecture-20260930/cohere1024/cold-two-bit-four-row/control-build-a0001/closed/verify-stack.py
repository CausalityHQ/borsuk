import hashlib,json,re,subprocess,datetime
from pathlib import Path
base=Path('/data/target/borsuk-cold-membership-native')
c=base/'two-bit-four-row-qualification-a0004';r=base/'two-bit-four-row-control-build-a0001'
assert json.loads((r/'independent-verification.json').read_text())['status']=='ISOLATED_CONTROL_BUILD_VERIFIED_SERVING_STACK_PENDING'
def asm(p,start,size):
 return subprocess.check_output(['objdump','-d','-C',f'--start-address={start}',f'--stop-address={start+size}',str(p)],text=True)
def frame(s,expected):
 pro=s.splitlines()[7:20]
 # Decode complete function from its symbol boundary, never from an arbitrary byte.
 instructions=[line for line in s.splitlines() if re.match(r'^\s*[0-9a-f]+:',line)]
 prefix='\n'.join(instructions[:10])
 pushes=len(re.findall(r'\bpush\s',prefix))
 subs=re.findall(r'sub\s+\$0x([0-9a-f]+),%rsp',prefix)
 value=8+pushes*8+sum(int(x,16) for x in subs)
 assert value==expected,(value,expected,prefix)
 assert not re.search(r'\band\s+[^\n]*%rsp',s)
 return value
records=[]
for role,cp,rp,croot,rroot,chelper,rhelper,kernel,scalar,slot,rs in [
 ('baseline',c/'candidate-check_cohere_native_baseline',r/'control-check_cohere_native_baseline',(0x499340,0x1514,864),(0x4c2850,0x17d0,832),(0x49f3a0,0xa60,496),(0x4c7760,0x221,272),(0x52a700,695,80),(0x50d3b0,0x1b7,16),0xbc5f00,0xbb4f20),
 ('http',c/'candidate-two_bit_http',r/'control-two_bit_http',(0x45af80,0x1514,864),(0x3c6740,0x1519,832),(0x463d90,0xa4d,480),(0x3d3640,0x221,272),(0x4f1d80,695,80),(0x4e86c0,0x1b7,16),0xb75580,0xb77700)]:
 outputs={};frames={}
 for label,p,spec in [('candidate-root',cp,croot),('control-root',rp,rroot),('candidate-helper',cp,chelper),('control-helper',rp,rhelper),('candidate-kernel',cp,kernel),('control-kernel',rp,scalar)]:
  s=asm(p,*spec[:2]);frames[label]=frame(s,spec[2]);outputs[label]=s
  (r/(role+'-'+label+'-stack-disassembly.txt')).write_text(s)
 assert re.search(r'call\s+'+format(chelper[0],'x')+r'\b',outputs['candidate-root'])
 assert re.search(r'call\s+'+format(rhelper[0],'x')+r'\b',outputs['control-root'])
 for p,target,got,label in [(cp,kernel[0],slot,'candidate-helper'),(rp,scalar[0],rs,'control-helper')]:
  rel=subprocess.check_output(['readelf','-rW',str(p)],text=True)
  assert re.search(r'^0*'+format(got,'x')+r'\s+.*R_X86_64_RELATIVE\s+'+format(target,'x')+r'\s*$',rel,re.M)
  assert re.search(r'call[^\n]*# '+format(got,'x')+r'\b',outputs[label])
 increment=sum(frames[x] for x in ['candidate-root','candidate-helper','candidate-kernel'])-sum(frames[x] for x in ['control-root','control-helper','control-kernel'])
 assert increment<=512
 # Conservative bound ignores the verified 32B/16B reduction in async caller frame.
 records.append({'role':role,'candidate_elf_sha256':hashlib.file_digest(cp.open('rb'),'sha256').hexdigest(),'control_elf_sha256':hashlib.file_digest(rp.open('rb'),'sha256').hexdigest(),'frames_including_saved_registers_and_return_addresses':frames,'conservative_increment_bytes':increment,'disassembly_sha256':{k:hashlib.sha256(v.encode()).hexdigest() for k,v in outputs.items()}})
receipt={'status':'MATCHED_SERVING_SCORING_STACK_PASS','source_commit':'fd2cb50ee5856a5c43131ca23d38c7be8c8bc73b','limit_increment_bytes':512,'records':records,'manual_callsite_audit':'No transient stack pushes at root-to-helper or helper-to-score callsites. Async caller frames candidate baseline3840/control3872 and candidate HTTP3584/control3600; outgoing arguments use preallocated frame slots. Conservative bound ignores these caller reductions. Error/panic/unwind paths are outside valid authenticated scoring path. Frame includes return addresses and saved registers.','ordered_kernel_receipt_sha256':hashlib.sha256((c/'independent-ordered-kernel-audit.json').read_bytes()).hexdigest(),'primitive_allowed_after_campaign_admission':True,'primitive_run':False,'performance_claim':False,'at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
(r/'independent-matched-serving-stack-audit.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps(receipt))
