#!/bin/bash
set -euo pipefail
systemd-run --unit=native-semantic-router-cold-stop --on-active=1800s /usr/sbin/shutdown -h now
root=/mnt/native-workspace-execution
mkdir -p "$root" && cd "$root"
phase=bootstrap
export ARTIFACT_NAMES='source-qualification.json config.json native-source-manifest.json source-before.json source-after.json workspace-receipt.json test.log test-resources.txt workspace-cgroup.json cpu.txt rustc-version.txt cargo-version.txt run-closed.log binaries/check_cohere_native_baseline'
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
        timeout --kill-after=5 60 aws s3 cp "$name" "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261010/baseline-pool-staging-smoke-a0001/artifacts/$name" --only-show-errors || code=96
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
print(json.dumps({'schema':'borsuk-baseline-pool-implementation-gates-spot-v1','source_commit':'3b7cea83180d102bfd182c1920946006aa57ef24',
  'source_archive_sha256':'dee6632f99475a1c7e2db41fab7d4ec284dc9ff07bfd08fd511d693a961da16c','config_sha256': 'a2ff769403b71c79213ad917c53d706f5c2d751601a2822a873b1e94531364ed', 'code_identity_sha256': 'd27b3d20a69dbe136f3d833a3f5b4946896abc75d9b903a03d11edfc83218f1d', 'campaign_schema': 'borsuk-baseline-pool-implementation-gates-spot-v1', 'source_identity_sha256': 'e4419d90fc1b16a3d6f1300fb98c76f81d56c82e11252a5a7bb50b896bca860f', 'source_file_count': 426, 'native_source_manifest_sha256': 'a849242746ae5786039bf734be888f501945ce0c61df30b122c3de723cd282cc', 'native_source_commit': 'c3e52c8bbf0fcc985c0a8a06d2abeec5d7442d38', 'artifact_roster_sha256': '6f7d020885dca4052b9305988c382a3f99d7948d6759f308d9216b3e09be2d1b', 'awscli_version': '2.36.11', 'awscli_sha256': '50fbb7a2f44a78eab4a210088040e8f0bc4b9937cac8043c2354269d58614df6', 'controller_source_commit': 'c913daa43ad98f3ffd94d6c644a3fe7b0354517a', 'candidate_delta_paths': ['crates/borsuk/src/bin/check_cohere_native_baseline.rs'], 'source_archive_paths_sha256': 'd58e6109f91d25b28c7124fd1c99f2493961715173bce04d664c02c9a3894014', 'source_archive_file_count': 2467, 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256'),
  'instance_id':os.environ['INSTANCE_ID'],'exit_code':code,'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),'source_qualification_sha256':artifacts.get('source-qualification.json',{}).get('sha256'),
  'phase':os.environ['PHASE'],
  'status':'complete' if code==0 and os.environ['PHASE']=='complete' else 'failed',
  'artifacts':artifacts},sort_keys=True,separators=(',',':')))
PY
  }
  terminal_ready=0
  if command -v python3 >/dev/null; then write_terminal && terminal_ready=1 || code=96; else code=96; fi
  { if [ "$terminal_ready" = 1 ]; then printf 'BORSUK_TERMINAL '; cat terminal.json; else printf 'BORSUK_TERMINAL unavailable\n'; fi; } >>/dev/ttyS0 2>/dev/null || true
  if [ "$aws_ready" = 1 ] && [ "$terminal_ready" = 1 ]; then
    timeout --kill-after=5 60 aws s3 cp terminal.json "s3://borsuk-bench-453182569524-euc1/research/semantic-router/20261010/baseline-pool-staging-smoke-a0001/terminal.json" --only-show-errors || { code=96; write_terminal || terminal_ready=0; }
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
timeout --kill-after=30 300 apt-get -qq -y -o DPkg::Lock::Timeout=120 install curl unzip python3-boto3 python3.12 python3-dev time tar gzip util-linux binutils build-essential pkg-config libssl-dev cmake
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
aws s3 cp 's3://borsuk-bench-453182569524-euc1/research/native-library-check/sources/dee6632f99475a1c7e2db41fab7d4ec284dc9ff07bfd08fd511d693a961da16c.tar.gz' source.tar.gz --only-show-errors
printf '%s  source.tar.gz\n' 'dee6632f99475a1c7e2db41fab7d4ec284dc9ff07bfd08fd511d693a961da16c' | sha256sum -c -
mkdir repo && tar -xzf source.tar.gz -C repo
phase=canary-source-admission
PYTHONPATH="$root/repo" python3.12 -m scripts.launch_native_workspace_execution_spot --baseline-pool-implementation --stage "$root/repo" "$root"
phase=canary-cli-and-failure-evidence
systemd-run --unit=baseline-pool-staging-smoke --wait --pipe -p MemoryMax=256M -p MemorySwapMax=0 -p CPUQuota=100% -p TasksMax=128 -p RuntimeMaxSec=120 -p WorkingDirectory="$root/repo" --setenv=PYTHONPATH="$root/repo" python3.12 - "$root" <<'CANARY'
import subprocess,sys,tempfile
from pathlib import Path
root=Path(sys.argv[1]);repo=root/'repo'
from scripts import launch_native_workspace_execution_spot as c
c.configure(baseline_pool=True);c._baseline_pool_self_check()
cmd=[sys.executable,'-m','scripts.launch_native_workspace_execution_spot','--baseline-pool-implementation','--check-receipt',str(root)]
r=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
assert r.returncode!=0
(root/'test.log').write_bytes(b'CANARY ONLY: real imports/source-admission/CLI/time/tee; recorder MOCKED; Cargo/native/ANN unrun\n'+r.stdout)
s=(repo/'scripts/check_baseline_pool_implementation.sh').read_text()
a=s.index('run_stage() {');b=s.index('\nrun_stage baseline-pool-bin-tests',a);fn=s[a:b]
with tempfile.TemporaryDirectory(dir=root) as d:
 for name,command,want,failtee in [('command17',['/bin/sh','-c','exit 17'],17,False),('recorder23',['/bin/true'],23,False),('tee-failure',['/bin/sh','-c','printf smoke'],1,True)]:
  script='set -euo pipefail\ncd '+str(repo)+'\nstage_log='+d+'/log\nstage_resources='+d+'/time\nsource_check() { :; }\nstage_record() { if [[ -z $3 ]]; then return 0; else return 23; fi; }\n'
  if failtee:script+='tee() { /usr/bin/tee /dev/full; }\n'
  script+=fn+'\nrun_stage smoke '+subprocess.list2cmdline(command)+'\n'
  # shlex quoting, never interpolate a command string as shell syntax.
  import shlex
  script=script[:script.rindex('\nrun_stage smoke ')]+ '\nrun_stage smoke '+shlex.join(command)+'\n'
  p=subprocess.run(['/bin/bash','-c',script],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=15)
  assert p.returncode==want,(name,p.returncode,want)
  assert b'baseline-pool-stage-observation' in p.stdout and b'baseline-pool-resources-begin' in p.stdout and b'baseline-pool-recorder-observation' in p.stdout
  with (root/'test.log').open('ab') as f:f.write(('CASE '+name+' exit='+str(p.returncode)+'\n').encode()+p.stdout)
print('STAGING_SMOKE_CHECKS_PASSED; no native qualification or performance claim')
CANARY
phase=canary-expected-failure
exit 99
phase=complete
