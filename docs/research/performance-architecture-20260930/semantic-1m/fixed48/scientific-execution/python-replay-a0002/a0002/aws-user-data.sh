#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=4500s /usr/sbin/shutdown -h now
root=/mnt/cohere-fixed48-scientific
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='config.json driver-config.json asset-manifest.json source-qualification.json bootstrap-staging.json staging.json transport-budget.json runtime-abi.json cpu.txt tool-versions.json run-closed.log profile.log profile-resources.txt driver-process.log driver-replay.log driver-replay.json profile-cgroup.json scientific-closure.json failure.json screen/cleanup.json screen/config.json screen/consumed-queries.raw screen/decision.json screen/duplicate-audit.json screen/input-hashes.json screen/local-sq8-head.json screen/measurement-cleanup.json screen/measurement-receipt.json screen/measurement-resources.json screen/measurement-sequence.json screen/offline-config.json screen/offline-result.json screen/oracle.json screen/original-generation-root.json screen/panel.json screen/prior-queries.raw screen/queries.raw screen/records.jsonl screen/rehost.json screen/requests.jsonl screen/resources.json screen/score-resources.json screen/score-resources.txt screen/score.log screen/scorer screen/scorer-config.json screen/source-order.u64 screen/source-qualification.json screen/source-root.json screen/source.raw screen/sq8-ordinal-check.json screen/stage-sequence.json screen/store/research/semantic-router/20261002/fixed48-fresh64/canonical.bin screen/store/research/semantic-router/20261002/fixed48-fresh64/centroids.bin screen/store/research/semantic-router/20261002/fixed48-fresh64/manifest.json screen/store/research/semantic-router/20261002/fixed48-fresh64/page_digests.bin screen/store/research/semantic-router/20261002/fixed48-fresh64/page_manifest.json screen/store/research/semantic-router/20261002/fixed48-fresh64/plane/manifest.json screen/store/research/semantic-router/20261002/fixed48-fresh64/plane/mean.bin screen/store/research/semantic-router/20261002/fixed48-fresh64/plane/page_digests.bin screen/store/research/semantic-router/20261002/fixed48-fresh64/plane/records.bin screen/store/research/semantic-router/20261002/fixed48-fresh64/router/leaves.bin screen/store/research/semantic-router/20261002/fixed48-fresh64/router/membership.bin screen/store/research/semantic-router/20261002/fixed48-fresh64/router/root.bin screen/store/research/semantic-router/20261002/fixed48-retained-artifacts/objects/b2f2f7dbec79c8646495b4a1500c363c5ccbf51690cf1e936d1264e7ee3839e1 screen/store/research/semantic-router/20261002/fixed48-retained-artifacts/objects/be2200926dbf7e241523d37c3b061c775911bd94f589a8fc67cadb632659e8dc screen/test-queries.raw screen/truth.i64 screen/truth.u32 screen/failure.json screen/failure-resources.json screen/failure-cleanup.json screen/COMPLETE.json'
finish() {
  original_code=$?
  code=$original_code
  trap - EXIT TERM
  set +e
  cd "$root"
  { printf 'BORSUK_BOOTSTRAP phase=%s original_exit_code=%s\n' "$phase" "$original_code"; tail -c 4096 run.log; printf '\n'; } >/dev/ttyS0 2>/dev/null || true
  cp run.log run-closed.log || code=96
  token=$(curl -fsS --connect-timeout 2 --max-time 5 -X PUT -H 'X-aws-ec2-metadata-token-ttl-seconds: 60' http://169.254.169.254/latest/api/token) || code=96
  instance_id=$(curl -fsS --connect-timeout 2 --max-time 5 -H "X-aws-ec2-metadata-token: $token" http://169.254.169.254/latest/meta-data/instance-id) || code=96
  aws_ready=0
  if command -v aws >/dev/null; then
    aws_ready=1
    for name in $ARTIFACT_NAMES; do
      if [ -f "$name" ] && { [ "$name" != screen/COMPLETE.json ] || [ "$code" = 0 ]; }; then
        systemd-run --quiet --wait --pipe -p MemoryMax=12884901888 -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=135 --setenv=AWS_MAX_ATTEMPTS=1 timeout --kill-after=5 120 aws s3 cp "$root/$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fixed48-scientific-a0002/artifacts/$name" --only-show-errors || code=96
      fi
    done
  else code=96; fi
  write_terminal() {
    INSTANCE_ID="$instance_id" EXIT_CODE="$code" ORIGINAL_EXIT_CODE="$original_code" PHASE="$phase" python3 - <<'PY' >terminal.json
import hashlib,json,os
from pathlib import Path
artifacts={}
for name in os.environ['ARTIFACT_NAMES'].split():
    path=Path(name)
    if path.is_file() and (name != 'screen/COMPLETE.json' or os.environ['EXIT_CODE'] == '0'):
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-cohere-fixed48-fresh-scientific-spot-v1','source_commit':'57fd5a3036995c75b6fd79b919afe57d682b6bfb',
  **json.loads(Path('source-qualification.json').read_text()),
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  }
  terminal_ready=0
  if command -v python3 >/dev/null; then write_terminal && terminal_ready=1 || code=96; else code=96; fi
  { if [ "$terminal_ready" = 1 ]; then printf 'BORSUK_TERMINAL '; cat terminal.json; else printf 'BORSUK_TERMINAL unavailable\n'; fi; } >>/dev/ttyS0 2>/dev/null || true
  if [ "$aws_ready" = 1 ] && [ "$terminal_ready" = 1 ]; then
    timeout --kill-after=5 60 aws s3 cp "$root/terminal.json" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fixed48-scientific-a0002/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
  fi
  printf 'BORSUK_FINISH phase=%s original_exit_code=%s exit_code=%s\n' "$phase" "$original_code" "$code" >>/dev/ttyS0 2>/dev/null || true
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
python3 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("H4sIAAAAAAAC/22T3a7bRgyE38XXdbBL7pK7eRmDy59YSGwHkk+Soui7l85FT9BGl6LwcTgz+usk+3ML0edlfxxP3y/HVaDT6eNJGkZZPaioikmvFab6JBy94VxtzTBcwECjCUmxgaVYq03LdIV5+uMkx+HPy03uW/jxfEcTFQEe7lwXk7ZanHCtKFXMtMMcUNvCMpSnlRrFGWG6rCHeWu8v9PdDv2zvyF5iLRaI1iTJsppALWWM0oqPPERT8ERW0XyFCtgb0LQ+qDYLekd+8/3YHvdkwgekD7XmSOX2VbZP98uhV79JztZjP94+n/Vx9d3Psf1wa+Mcux/X86Gb39PVTc/H18fz/O0n4mF+2ew1eP75rnuxcSMPtog5SvrpGJa6CtcORq7cSZfUMpz6tKnUq9VR18rjEH+S77F9+gWZFOB8NKGl54KAdLbXbhVr8Kz53TIqBTsUpt48pmYQ0Lxknom0fUsbLv8lj4lDw4XnNDYBHxW4CMfAQBi8bJQZFVaWYdbsw9Sa8XGdGUGmkeTd4/iNDRIlb3UotKILpmTFpFk35YHii1J3R4YsAOSAwcxyTxSiCqBJPvSxp+a13WX/1eBuLz90Dh4yvREpDlpdSusVWi6SLABSzEWsmuYsz95hjoSitxf58barX2TXa9qSrtxu2/PVOQ5LsQVpzp45pUjjuWadEp6+04CV96z/I/5VpwbZdfBKXRSrQ9fAmv9YuDY3ws7OoiaaESo4SnZYJDvD3BXp9Pc/X/uBkcIDAAA=",validate=True)))'
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1
phase=apt-update
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
phase=apt-install
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip python3-boto3 python3.12 python3.12-venv time tar gzip util-linux binutils
phase=awscli-download
curl -fsSL --connect-timeout 10 --max-time 180 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6' | sha256sum -c -
test "$(stat -c %s awscliv2.zip)" = 73022935
phase=awscli-install
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install
cli_version=$(aws --version)
[[ "$cli_version" == aws-cli/2.36.11\ * ]]
phase=source-download
systemd-run --quiet --wait --pipe -p MemoryMax=12884901888 -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=300 -p WorkingDirectory="$root" python3.12 - borsuk-bench-453182569524-euc1 research/native-library-check/sources/cd28212e165ac31e25cf31638fec4ed6357e7acdac201c2e3a937aad74775c36.tar.gz cd28212e165ac31e25cf31638fec4ed6357e7acdac201c2e3a937aad74775c36 17179869184 <<'SOURCE'
import boto3,hashlib,json,os,sys,tarfile
from pathlib import Path
from botocore.config import Config
root=Path.cwd(); limit=int(sys.argv[4])
def size():
 seen=set(); total=0
 for base,ds,fs in os.walk(root):
  for n in fs:
   p=Path(base)/n
   if p.is_symlink(): continue
   x=p.stat(); k=x.st_dev,x.st_ino
   if k not in seen: total+=max(x.st_size,x.st_blocks*512); seen.add(k)
 return total
available=((limit-size())//4096)*4096; assert available>0
s3=boto3.client('s3',region_name='eu-central-1',config=Config(retries={'total_max_attempts':1},connect_timeout=10,read_timeout=30))
h=hashlib.sha256(); count=0
with s3.get_object(Bucket=sys.argv[1],Key=sys.argv[2])['Body'] as body,Path('source.tar.gz').open('xb') as out:
 for chunk in iter(lambda:body.read(1048576),b''):
  count+=len(chunk); assert count<=available,'source staging scratch bound'
  h.update(chunk);out.write(chunk)
 out.flush();os.fsync(out.fileno())
assert h.hexdigest()==sys.argv[3],'source archive authentication'
reserve=0
with tarfile.open('source.tar.gz',mode='r|gz') as archive:
 for m in archive:
  assert not m.name.startswith('/') and '..' not in Path(m.name).parts
  if m.isfile(): reserve+=((m.size+4095)//4096)*4096
assert size()+reserve+(16<<20)<=limit,'source extraction scratch reserve'
Path('bootstrap-staging.json').write_text(json.dumps(dict(source_archive_bytes=count,source_archive_sha256=h.hexdigest(),source_repository_reserve_bytes=reserve,scratch_before_extract_bytes=size(),scratch_limit_bytes=limit,source_authenticated=True,source_gets=1,scope='source archive/repo/AWSCLI bootstrap staging beneath worker root; install and uploads separately observed'))+'\n')
SOURCE
printf '%s  source.tar.gz\n' 'cd28212e165ac31e25cf31638fec4ed6357e7acdac201c2e3a937aad74775c36' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fixed48-scientific-configs/de6b59eed6b1ea111dd598ae5aca17b7bbc27715106403bee8f10fd0eb08eede.json source-qualification.json --only-show-errors
printf '%s  source-qualification.json\n' 'de6b59eed6b1ea111dd598ae5aca17b7bbc27715106403bee8f10fd0eb08eede' | sha256sum -c -
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fixed48-scientific-configs/b0b227777c7df057d7f201a515d131f791256bd60035207654ef9cb7624e0272.json config.json --only-show-errors
printf '%s  config.json\n' 'b0b227777c7df057d7f201a515d131f791256bd60035207654ef9cb7624e0272' | sha256sum -c -
python3.12 -m venv --system-site-packages "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
lscpu >cpu.txt
phase=scientific
systemd-run --unit=cohere-fixed48-scientific --wait --pipe -p MemoryMax=12884901888 -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=3660 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=BLIS_NUM_THREADS=2 --setenv=NUMEXPR_NUM_THREADS=2 --setenv=VECLIB_MAXIMUM_THREADS=2 --setenv=RAYON_NUM_THREADS=2 --setenv=TOKIO_WORKER_THREADS=2 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 3600 \
 "$root/venv/bin/python" -m scripts.launch_cohere_fixed48_fresh_scientific_spot --stage "$root/repo" "$root" research/semantic-router/20261002/fixed48-scientific-a0002 >profile.log 2>&1
phase=complete
