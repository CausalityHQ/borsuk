#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3000s /usr/sbin/shutdown -h now
root=/mnt/cohere-fixed48-cold
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='config.json controller-config.json asset-manifest.json source-qualification.json bootstrap-staging.json runtime-abi.json cpu.txt tool-versions.json run-closed.log profile.log profile-resources.txt profile-cgroup.json staging.json cold-closure.json failure.json controller-sdk-ledger.jsonl native/source-qualification.json native/config.json native/native-source-manifest.json native/source-before.json native/source-after.json native/workspace-receipt.json native/test.log native/test-resources.txt native/workspace-cgroup.json native/cpu.txt native/rustc-version.txt native/cargo-version.txt native/run-closed.log native/binaries/two_bit_http native/binaries/check_semantic_router_scorer native/binaries/two_bit_plan_demo native/aws-reservation.json native/aws-closeout.json native/aws-terminal.json native/collection-replay.json native/root-verification.json screen/config.json screen/source-qualification.json screen/sdk-ledger.jsonl screen/input-hashes.json screen/publisher-requests.jsonl screen/request-derivative.json screen/original-generation-root.json screen/transport-delta.json screen/publication.log screen/publication-resources.txt screen/publication-reference.jsonl screen/publication.json screen/sealed-reference-k10.jsonl screen/records.jsonl screen/resources.json screen/cleanup.json screen/summary.json screen/scientific-reference.json screen/COMPLETE.json'
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
  if test -x "$root/bin/aws"; then
    aws_ready=1
    for name in $ARTIFACT_NAMES; do
      if [ -f "$name" ] && { [ "$name" != screen/COMPLETE.json ] || [ "$code" = 0 ]; }; then
        systemd-run --quiet --wait --pipe -p MemoryMax=12884901888 -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=65 --setenv=AWS_MAX_ATTEMPTS=1 timeout --kill-after=5 60 "$root/bin/aws" s3 cp "$root/$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fixed48-cold-a0002/artifacts/$name" --only-show-errors || code=96
      fi
    done
  else code=96; fi
  write_terminal() {
    INSTANCE_ID="$instance_id" EXIT_CODE="$code" ORIGINAL_EXIT_CODE="$original_code" PHASE="$phase" python3 - <<'PY' >terminal.json
import base64,hashlib,json,os
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
diagnostics={}
if int(os.environ['EXIT_CODE']):
    for name in ('failure.json', 'cold-closure.json', 'screen/summary.json', 'screen/cleanup.json', 'profile.log'):
        if name in artifacts:
            offset=max(0,artifacts[name]['bytes']-4096)
            with Path(name).open('rb') as source:
                source.seek(offset); body=source.read(4096)
            diagnostics[name]={'offset':offset,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest(),'body_base64':base64.b64encode(body).decode()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-cohere-fixed48-cold-http-spot-v1','source_commit':'2c66c934f00d0ae8737be26310d3276d7b6de926',
  **{k:v for k,v in json.loads(Path('source-qualification.json').read_text()).items() if k != 'proof_sha256'},
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts,'failure_diagnostics':diagnostics},sort_keys=True,separators=(',',':')))
PY
  }
  terminal_ready=0
  if command -v python3 >/dev/null; then write_terminal && terminal_ready=1 || code=96; else code=96; fi
  { if [ "$terminal_ready" = 1 ]; then printf 'BORSUK_TERMINAL '; cat terminal.json; else printf 'BORSUK_TERMINAL unavailable\n'; fi; } >>/dev/ttyS0 2>/dev/null || true
  if [ "$aws_ready" = 1 ] && [ "$terminal_ready" = 1 ]; then
    timeout --kill-after=5 60 "$root/bin/aws" s3 cp "$root/terminal.json" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fixed48-cold-a0002/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
  fi
  printf 'BORSUK_FINISH phase=%s original_exit_code=%s exit_code=%s\n' "$phase" "$original_code" "$code" >>/dev/ttyS0 2>/dev/null || true
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
python3 -c 'import base64,gzip; from pathlib import Path; Path("source-qualification.json").write_bytes(gzip.decompress(base64.b64decode("H4sIAAAAAAAC/3WUXY7jRgyE7+LnzILdZP/lMgbJJtdCxtZA0swmCHL3UEkWXiSTN7vV+kgWq/T7hbdjcdbjuq37Ydt1v3Eu9fLzBVpNOrWSdIcmZWAe1AVyL9BsjoKzkjdIpXPccLI5BSB50ZQ058tPF953O653fixu+/FEq5EkLNUN6pi1SqHmzijARdxmQeY8evc0aEyTZmTC1Srk3N38O3q/LtMex3L89mQ3leDWgdbmSEbMjToOyQOlShAtd05Nq2FCoR5DJmwsSa0IO57sb7u+Lk9kARdpnJ2IWzcW4pwAegcCi9FFScbApqxxhJqxUI7BSsBpen0iP2zbl/URzPwF65eU4pEsD95+GABwWJ9wTluxIWTPGiKlSZ4yYYKWm7jXxjb7EBo1jlsqPsdM0E9xlO9vvHx9XHe92Z0DKuu2v//youvNNnvx5Veb1OPv63y5Hcfby/62Hi8fZzu6TvtE1sECmqKCjxDDtYoYORqWPivyaMxhC9TWYolOpZGmmkf1XCa39FdX68OXr09kh8kjrCRqinG7KM3zvbCYJq3tNBqqCLYMBdLQ88y6dKrVsvLfyGNbX1/Dt/+mExEwd8XgF49mC+mU6Cmq5RpKgbeeWgyA3pXcSiUqubZymo4g6A8+lg+77uv7pp9pwjOWm6P9jGpmgOQ5G800XUEw9UDOkSm8Bh43MGyCGLUL5p7QosTbtq7+JIryrDDTGN0iEZA5hVmTDpguofmQiEMbPZ3r02aNoQ/1KMgyvct34mfBKEIpllNGyhxNgvlMKqihcStVzo5UtUeRyZoEWgiEbr1C6kaZTva7vC777cevhJJ59sh943BpbM+V0xwYuscPKylVO9ORucbitBZRR50zRUAa4Rm37T06vdv1f5zn04CslzKbzJlZEUpNLdZLJbI7sbuX+IJ4JQjpskGnTiMPKUgxU1T4Z4O86e1cqK73+3KcIdRadcTa4kVg6w2bWK6RsYm51ShYp41c/4t4duehn8foBaHXmBkcyGdOUZ/j4Qhvhf8saDJqfIukhNvjZk5uw8vljz8BmrWZqoMFAAA=",validate=True)))'
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1
export PATH="$root/bin:$PATH"
mkdir -p "$root/apt"
phase=apt-update
timeout --kill-after=30 180 apt-get -o Dir::Cache::archives="$root/apt" -qq -o DPkg::Lock::Timeout=120 update
phase=apt-install
timeout --kill-after=30 300 apt-get -o Dir::Cache::archives="$root/apt" -qq -y -o DPkg::Lock::Timeout=120 install curl unzip python3-boto3 python3.12 python3.12-venv time tar gzip util-linux binutils
cat >whole-reserve.py <<'RESERVE'
import os,shutil,sys,zipfile
from pathlib import Path
root=Path(sys.argv[1]); limit=int(sys.argv[2]); reserve=int(sys.argv[3]); seen=set(); used=0
for base,ds,fs in os.walk(root,followlinks=False):
 for name in fs:
  p=Path(base)/name
  if p.is_symlink(): continue
  s=p.stat(); key=s.st_dev,s.st_ino
  if key not in seen: used+=max(s.st_size,s.st_blocks*512); seen.add(key)
if len(sys.argv)>4:
 with zipfile.ZipFile(sys.argv[4]) as z:
  reserve+=3*sum(((m.file_size+4095)//4096)*4096 for m in z.infolist())
assert used+reserve<=limit and shutil.disk_usage(root).free>=reserve,'bootstrap whole-worker reserve'
RESERVE
python3 whole-reserve.py "$root" 17179869184 73022935
phase=awscli-download
curl -fsSL --connect-timeout 10 --max-time 180 --max-filesize 73022935 --output awscliv2.zip https://awscli.amazonaws.com/awscli-exe-linux-x86_64-2.36.11.zip
printf '%s  awscliv2.zip\n' '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6' | sha256sum -c -
test "$(stat -c %s awscliv2.zip)" = 73022935
phase=awscli-install
python3 whole-reserve.py "$root" 17179869184 16777216 awscliv2.zip
timeout --kill-after=30 120 unzip -q awscliv2.zip
timeout --kill-after=30 120 ./aws/install --install-dir "$root/aws-cli" --bin-dir "$root/bin"
cli_version=$("$root/bin/aws" --version)
[[ "$cli_version" == aws-cli/2.36.11\ * ]]
phase=source-download
systemd-run --quiet --wait --pipe -p MemoryMax=12884901888 -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=300 -p WorkingDirectory="$root" python3.12 - borsuk-bench-453182569524-euc1 research/native-library-check/sources/ff03cfc9d53086eca0f04fd2129baff098c4c5fe76db96a74b5ec386e21fe9f5.tar.gz ff03cfc9d53086eca0f04fd2129baff098c4c5fe76db96a74b5ec386e21fe9f5 17179869184 <<'SOURCE'
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
printf '%s  source.tar.gz\n' 'ff03cfc9d53086eca0f04fd2129baff098c4c5fe76db96a74b5ec386e21fe9f5' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
python3.12 -m venv --system-site-packages "$root/venv"
lscpu >cpu.txt
phase=cold
systemd-run --unit=cohere-fixed48-cold --wait --pipe -p MemoryMax=12884901888 -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=2400 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=BLIS_NUM_THREADS=2 --setenv=NUMEXPR_NUM_THREADS=2 --setenv=VECLIB_MAXIMUM_THREADS=2 --setenv=RAYON_NUM_THREADS=2 --setenv=TOKIO_WORKER_THREADS=2 \
 --setenv=BORSUK_COLD_SOURCE_COMMIT=2c66c934f00d0ae8737be26310d3276d7b6de926 --setenv=BORSUK_COLD_ARCHIVE_SHA256=ff03cfc9d53086eca0f04fd2129baff098c4c5fe76db96a74b5ec386e21fe9f5 \
 /usr/bin/time -v -o "$root/profile-resources.txt" timeout --signal=TERM --kill-after=30 2340 \
 taskset -c 4-5 "$root/venv/bin/python" -m scripts.launch_cohere_fixed48_cold_http_spot --stage "$root/repo" "$root" research/semantic-router/20261002/fixed48-cold-a0002 >profile.log 2>&1
phase=complete
