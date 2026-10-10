import pathlib,subprocess,json,hashlib,os,resource,sys,time
from urllib.parse import urlsplit,urljoin
HF_HOST='huggingface.co';MAX_REDIRECTS=5
def resolve(url, hosts):
    """Follow at most MAX_REDIRECTS HTTPS hops; every hop needs an exact allowed host, no userinfo, port None/443. Body-free HEAD probes."""
    allowed = {HF_HOST, *hosts}
    for _ in range(MAX_REDIRECTS + 1):
        parts = urlsplit(url)
        assert parts.scheme == 'https' and parts.hostname in allowed and '@' not in parts.netloc \
            and parts.username is None and parts.password is None and parts.port in (None, 443), 'redirect host/scheme/userinfo/port policy'
        headers = subprocess.run(['curl', '-sS', '--proto', '=https', '--connect-timeout', '10', '--max-time', '60',
            '--head', url], check=True, capture_output=True, text=True, timeout=90).stdout
        lines = headers.replace('\r', '').split('\n')
        status = int(lines[0].split()[1])
        if 300 <= status < 400:
            location = [l.split(':', 1)[1].strip() for l in lines if l.lower().startswith('location:')]
            assert len(location) == 1, 'single redirect location'
            url = urljoin(url, location[0])
            continue
        assert status == 200, 'final transport status'
        return url
    raise AssertionError('too many redirects')

def sha(path):
 h=hashlib.sha256()
 with path.open('rb') as f:
  for b in iter(lambda:f.read(1048576),b''):h.update(b)
 return h.hexdigest()
def syncdir(path):
 fd=os.open(path,os.O_DIRECTORY|os.O_NOFOLLOW)
 try:os.fsync(fd)
 finally:os.close(fd)
def fetch(item,root):
 size=item['bytes'];digest=item['sha256'];dest=root/item['relative_path'];part=dest.with_name(dest.name+'.part')
 assert type(size) is int and 0<size<=5368709120 and len(digest)==64 and all(c in '0123456789abcdef' for c in digest)
 assert not pathlib.PurePosixPath(item['relative_path']).is_absolute() and '..' not in pathlib.PurePosixPath(item['relative_path']).parts
 dest.parent.mkdir(parents=True,exist_ok=True);assert not dest.exists() and not part.exists()
 def cap():resource.setrlimit(resource.RLIMIT_FSIZE,(size,size))
 if item['kind']=='https':
  url=resolve(item['url'],['us.aws.cdn.hf.co'])
  subprocess.run(['curl','-fsS','--proto','=https','--max-redirs','0','--connect-timeout','10','--max-time','300','--max-filesize',str(size),'--output',str(part),url],check=True,timeout=310,preexec_fn=cap)
 else:
  assert item['kind']=='s3' and item['bucket']=='borsuk-bench-453182569524-euc1' and item['key'].startswith('research/')
  subprocess.run(['aws','--region','eu-central-1','--cli-connect-timeout','10','--cli-read-timeout','60','s3api','get-object','--bucket',item['bucket'],'--key',item['key'],'--if-match',item['etag'],str(part)],check=True,stdout=subprocess.DEVNULL,timeout=300,preexec_fn=cap)
 stat=part.lstat();assert part.is_file() and not part.is_symlink() and stat.st_nlink==1 and stat.st_size==size
 assert sha(part)==digest
 with part.open('rb') as f:os.fsync(f.fileno())
 os.link(part,dest);part.unlink();syncdir(dest.parent)
 return {'relative_path':item['relative_path'],'bytes':size,'sha256':digest,'whole_body_authenticated':True}
pins=pathlib.Path(sys.argv[1]);expected=sys.argv[2];root=pathlib.Path(sys.argv[3]);assert sha(pins)==expected
items=json.loads(pins.read_text());assert len(items)==10 and len({i['relative_path'] for i in items})==10
root.mkdir();syncdir(root.parent);receipts=[]
for i in items:receipts.append(fetch(i,root))
with (root/'transport-receipt.json').open('x') as f:json.dump({'items':receipts,'body_decode':False},f,indent=2);f.write(chr(10));f.flush();os.fsync(f.fileno())
syncdir(root)
