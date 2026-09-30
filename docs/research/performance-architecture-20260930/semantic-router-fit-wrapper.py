import hashlib,json,os,subprocess,sys,time
from pathlib import Path
binary,binary_sha,root,root_sha,destination=sys.argv[1:]
out=Path(destination)
out.mkdir(exist_ok=False)
group=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().split('0::',1)[1].strip().lstrip('/')
keys=['memory.max','memory.peak','memory.swap.max','memory.swap.peak','memory.events','memory.swap.events','cpu.max','cpu.stat','pids.max']
def counters():
    return {k:(group/k).read_text() for k in keys}|{'cpu_affinity':sorted(os.sched_getaffinity(0))}
before=counters()
assert int(before['memory.max'])==268435456 and int(before['memory.swap.max'])==0
assert int(before['pids.max'])==32 and before['cpu_affinity']==[0,1]
quota,period=map(int,before['cpu.max'].split());assert quota==2*period
h=hashlib.sha256()
with open(binary,'rb') as f:
    for block in iter(lambda:f.read(1048576),b''):h.update(block)
assert h.hexdigest()==binary_sha
(out/'before.json').write_text(json.dumps(before,indent=2)+'\n')
command=['/usr/bin/time','-v','-o',str(out/'time.txt'),binary,root,root_sha,str(out/'router')]
start=time.monotonic_ns()
with (out/'stdout.log').open('xb') as stdout,(out/'stderr.log').open('xb') as stderr:
    result=subprocess.run(command,stdout=stdout,stderr=stderr,check=False)
elapsed=time.monotonic_ns()-start
after=counters()
(out/'terminal.json').write_text(json.dumps(dict(command=command,binary_sha256=binary_sha,exit=result.returncode,wall_ns=elapsed,before=before,after=after,scope='construction only; no query/scorer/serving performance'),indent=2)+'\n')
assert int(after['memory.swap.peak'])==0
sys.exit(result.returncode)
