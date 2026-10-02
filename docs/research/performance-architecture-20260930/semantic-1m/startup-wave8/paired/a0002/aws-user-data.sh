#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=3600s /usr/sbin/shutdown -h now
root=/mnt/native-startup-wave8-paired
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='test-resources.txt run-closed.log screen/source-qualification.json screen/config.json screen/tool-versions.json screen/input-hashes.json screen/records.jsonl screen/failures.jsonl screen/summary.json screen/resources.json screen/paired-cgroup.json screen/cleanup.json screen/cell0-records.jsonl screen/cell0-summary.json screen/cell0-seal.json screen/cell1-records.jsonl screen/cell1-summary.json screen/cell1-seal.json screen/cell2-records.jsonl screen/cell2-summary.json screen/cell2-seal.json screen/cell3-records.jsonl screen/cell3-summary.json screen/cell3-seal.json'
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
      if [ -f "$name" ]; then
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fresh1m-startup-wave8-paired-a0002/artifacts/$name" --only-show-errors || code=96
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
    if path.is_file():
        digest=hashlib.sha256()
        with path.open('rb') as source:
            for chunk in iter(lambda: source.read(1024*1024),b''):
                digest.update(chunk)
        artifacts[name]={'bytes':path.stat().st_size,'sha256':digest.hexdigest()}
code=int(os.environ['EXIT_CODE'])
print(json.dumps({'schema':'borsuk-startup-wave8-paired-spot-v2','source_commit':'c0c403209ab1129ba7739044b18f3991ac052ee5',
  'source_archive_sha256':'f3e4b583d81302d5053bb89fb869830a64a49d44ae4d2657a3e71afa58e4330e','config_sha256': 'a5a41ebcb70360f699b081a3da64a8e1d7b1aad43d1e87a930ce33720bf08045', 'code_identity_sha256': 'e980a4b1585ed0761bfbb878d1a107042a34fa0bafb34992baa3143e2912f907', 'refs_identity_sha256': '29e5794e179a8fffb1fb433a4f4950f679533f3425be260abb1636521fe18f79', 'roles': {'candidate': {'wave_objects': 8, 'native_source_commit': '1e4ed13777a59e7acdd34acc9510b9d156aa17bf', 'source_identity_sha256': '7e4fabf96284e4ccd080cf14b8bfbfd0f3ff41271e920c8e39d5d1fa252cd830', 'binary': {'bytes': 16153680, 'sha256': '0eec7c65d558fb8477f48dc3f3702b404b649b0888ce2250b2c7433726875fbc'}, 'proof': {'bytes': 45755, 'sha256': 'c1ee39cc98358b965e938d0a3e2d5d92bc04cc505b14b3cfd41b5f2c5f15f6ff'}, 'source_manifest': {'bytes': 46612, 'sha256': '975638f75cff8c18204e6f43b78b09da7d463a96bc7e35bf30490635d5027c60'}, 'current_whole_tree_full_execution': False}, 'control': {'wave_objects': 4, 'native_source_commit': 'f4d76fc040aa89c44b3526e37f148e78d21241fa', 'source_identity_sha256': '295a79de9a499cc388db14b4b78ac9fcd4f1eb8673ceb5c1222dfc7f119e9ae4', 'binary': {'bytes': 16153608, 'sha256': 'f80d3616e868c05eaf012ced1bb22281c57f064f7ca29f3de9c3aa321ea3befe'}, 'proof': {'bytes': 48434, 'sha256': 'ce590768f41d6b51c15410537299d91ed302dee84b9082fda8f13a194468c532'}, 'source_manifest': {'bytes': 43941, 'sha256': '9ffc6d754a935d301c1d45a8e7cdea03105b6a0b1ac21d0ddc4dddab7d7083d8'}, 'current_whole_tree_full_execution': False}}, 'measurement_prefix': 'research/semantic-router/20261002/fresh1m-startup-wave8-paired-a0002', 'prices': {'bytes': 5278, 'sha256': 'bce75ab16f2fcaece67266d98a63b7603abda4c2b918b83894d5ce6f7f272359'}, 'cold_terminal_sha256': '25de5c930a066bdae71d48078e23901d79c6bc150770987cfaeeb5c105d714b6', 'native_rebuilt': False, 'publication_invocations': 0, 'current_whole_tree_full_execution': False, 'namespace_prefix': 'research/semantic-router/20261001/fresh1m-cold-a0005/native', 'controller_code_identity_sha256': '863cc68a6d8cba0c393deae4d77cdc4a5d0228c32cecab1ca0e0047faedf5b54', 'controller_sha256': '17f26c6f291fd1ccc6256cba553c7077fa761784235a3b29a7f8e2ede7e4f320', 'campaign_schema': 'borsuk-startup-wave8-paired-spot-v2', 'artifact_roster_sha256': 'dcefcc596a6216243bc087166fc8cdf9805ca8503b0d6a85fb2ad5668bf53851', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6',
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),'scientific_qualification':('PASS' if json.loads(Path('screen/summary.json').read_bytes())['paired_gate_passed'] is True else 'FAIL') if code==0 and os.environ['PHASE']=='complete' else None,
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  }
  terminal_ready=0
  if command -v python3 >/dev/null; then write_terminal && terminal_ready=1 || code=96; else code=96; fi
  { if [ "$terminal_ready" = 1 ]; then printf 'BORSUK_TERMINAL '; cat terminal.json; else printf 'BORSUK_TERMINAL unavailable\n'; fi; } >>/dev/ttyS0 2>/dev/null || true
  if [ "$aws_ready" = 1 ] && [ "$terminal_ready" = 1 ]; then
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261002/fresh1m-startup-wave8-paired-a0002/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
  fi
  printf 'BORSUK_FINISH phase=%s original_exit_code=%s exit_code=%s\n' "$phase" "$original_code" "$code" >>/dev/ttyS0 2>/dev/null || true
  shutdown -h now || true
  exit "$code"
}
trap finish EXIT
trap 'exit 97' TERM
exec >run.log 2>&1
export DEBIAN_FRONTEND=noninteractive AWS_MAX_ATTEMPTS=1
phase=apt-update
timeout --kill-after=30 180 apt-get -qq -o DPkg::Lock::Timeout=120 update
phase=apt-install
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip python3.12 python3.12-venv time tar gzip util-linux binutils
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/f3e4b583d81302d5053bb89fb869830a64a49d44ae4d2657a3e71afa58e4330e.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'f3e4b583d81302d5053bb89fb869830a64a49d44ae4d2657a3e71afa58e4330e' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0 boto3==1.40.72 botocore==1.40.72 s3transfer==0.14.0 jmespath==1.0.1 python-dateutil==2.9.0 six==1.17.0 urllib3==2.6.3
test "$(getconf GNU_LIBC_VERSION)" = 'glibc 2.39'
PYTHONPATH="$root/repo" "$root/venv/bin/python" -c 'import numpy, pyarrow'
PYTHONPATH="$root/repo" "$root/venv/bin/python" -m scripts.launch_native_startup_wave8_paired_spot --import-smoke
phase=quality
systemd-run --unit=startup-wave8-paired --wait --pipe -p MemoryMax=8G -p IOAccounting=yes -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=3000 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C \
 --setenv=BORSUK_OFFERED_SOURCE_COMMIT=c0c403209ab1129ba7739044b18f3991ac052ee5 --setenv=BORSUK_OFFERED_ARCHIVE_SHA256=f3e4b583d81302d5053bb89fb869830a64a49d44ae4d2657a3e71afa58e4330e \
 --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=BLIS_NUM_THREADS=2 --setenv=NUMEXPR_NUM_THREADS=2 --setenv=RAYON_NUM_THREADS=2 --setenv=TOKIO_WORKER_THREADS=2 \
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=90 3000 \
 taskset -c 4-5 "$root/venv/bin/python" -m scripts.launch_native_startup_wave8_paired_spot --worker "$root/repo/docs/research/performance-architecture-20260930/semantic-1m/startup-wave8/paired8-config.json" a5a41ebcb70360f699b081a3da64a8e1d7b1aad43d1e87a930ce33720bf08045 "$root/repo" "$root/screen" research/semantic-router/20261002/fresh1m-startup-wave8-paired-a0002
for name in $ARTIFACT_NAMES; do
 if [ "$name" = screen/failures.jsonl ]; then test -f "$name"; elif [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
phase=complete
