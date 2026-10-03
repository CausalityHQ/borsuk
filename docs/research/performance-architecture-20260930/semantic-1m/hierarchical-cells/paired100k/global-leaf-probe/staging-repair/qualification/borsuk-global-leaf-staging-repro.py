import cProfile,hashlib,io,json,os,pstats,resource,shutil,sys,tarfile,tempfile,threading,time
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from scripts import run_hierarchical_global_leaf_probe as probe
from scripts import launch_hierarchical_cells_100k_spot as launcher
from scripts import prepare_hierarchical_cells_100k as local
from unittest.mock import patch
N=512
cgroup=Path('/sys/fs/cgroup')/Path('/proc/self/cgroup').read_text().strip().split('::',1)[1].lstrip('/')
def group_snapshot():
 return dict(cpu_affinity=sorted(os.sched_getaffinity(0)),**{n:(cgroup/n).read_text().strip() for n in ('memory.max','memory.swap.max','cpu.max','memory.peak','memory.swap.peak','memory.events')})
before=group_snapshot()
assert before['memory.max']=='268435456' and before['memory.swap.max']=='0' and before['cpu.max']=='100000 100000' and before['cpu_affinity']==[0],before
with tempfile.TemporaryDirectory(prefix="global-leaf-staging-profile-") as tmp:
 root=Path(tmp)/"worker"; original=Path(tmp)/"original"
 root.mkdir();original.mkdir()
 for folder in (root,original):
  for i in range(2048): (folder/str(i)).write_bytes(b"small scratch file")
 archive=root/"sources.tar.gz"; inventory={}
 with tarfile.open(archive,"w:gz",format=tarfile.USTAR_FORMAT) as tar:
  for i in range(N):
   name=f"support/{i:04d}";body=b"source bytes";entry=tarfile.TarInfo(name);entry.size=len(body)
   tar.addfile(entry,io.BytesIO(body));inventory[name]=hashlib.sha256(body).hexdigest()
 pin=local.identity(archive);baseline=shutil.disk_usage(root).used
 counters={"scan_calls":0,"scan_file_entries":0,"scan_ns":0,"checks":0,"heartbeat_calls":0}
 real_walk=os.walk; real_directory=local.directory_bytes
 def counted_walk(*args,**kwargs):
  for folder,dirs,names in real_walk(*args,**kwargs):
   counters["scan_file_entries"]+=len(names);yield folder,dirs,names
 def counted_directory(path):
  started=time.monotonic_ns();counters["scan_calls"]+=1
  try:return real_directory(path)
  finally:counters["scan_ns"]+=time.monotonic_ns()-started
 peaks={"scratch_bytes":0}
 def check(*,scan=True):
  counters["checks"]+=int(scan);counters["heartbeat_calls"]+=int(not scan)
  launcher.probe_resource_check(root,baseline,256<<20,time.monotonic()+120,[],peaks,scan=scan)
 stopped=threading.Event()
 def monitor():
  while not stopped.wait(1):check()
 thread=threading.Thread(target=monitor,daemon=True)
 with patch.object(probe,"ORIGINAL_ROOT",original),patch.object(os,"walk",counted_walk),patch.object(local,"directory_bytes",counted_directory):
  prof=cProfile.Profile();started=time.monotonic();thread.start();prof.enable()
  try:probe.archive_sources(pin,inventory,{}, {},check)
  finally:prof.disable();stopped.set();thread.join()
 elapsed=time.monotonic()-started
 result=dict(counters,archive_members=N,archive_bytes=pin["bytes"],wall_seconds=elapsed,max_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024)
 Path("/tmp/borsuk-global-leaf-staging-after.json").write_text(json.dumps(dict(result,guard_accounting=peaks,cgroup_before=before,cgroup_after=group_snapshot()),sort_keys=True))
 print(json.dumps(result,sort_keys=True),flush=True)
 stats=pstats.Stats(prof).sort_stats("cumulative");stats.print_stats(12)
 assert counters["scan_file_entries"]<100000, "full scratch traversal repeated per read-only archive member"
