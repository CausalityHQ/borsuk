#!/bin/bash
set -euo pipefail
export AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_DEFAULT_REGION=eu-central-1
systemd-run --unit=fine-pack-stop --on-active=900s /usr/sbin/shutdown -h now
root=/mnt/fine-pack-diagnostic
mkdir "$root"
cd "$root"
trap '/usr/sbin/shutdown -h now || true' EXIT
exec >run.log 2>&1
test "$(uname -m)" = x86_64
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3.12 python3.12-venv tar gzip
python3.12 -m venv "$root/venv"
python="$root/venv/bin/python"
"$python" -m pip install --retries 0 --timeout 15 --no-cache-dir --disable-pip-version-check --only-binary=:all: --no-deps boto3==1.40.72 botocore==1.40.72 jmespath==1.0.1 s3transfer==0.14.0 python-dateutil==2.9.0.post0 six==1.17.0 urllib3==2.6.3
"$python" - <<'PY'
import boto3, hashlib, tarfile
import botocore.session
from pathlib import Path
from botocore.config import Config
assert boto3.__version__=='1.40.72' and botocore.__version__=='1.40.72'
assert 'IfNoneMatch' in botocore.session.get_session().get_service_model('s3').operation_model('PutObject').input_shape.members
s3=boto3.client('s3',region_name='eu-central-1',config=Config(retries={'total_max_attempts':1}))
response=s3.get_object(Bucket='borsuk-bench-453182569524-euc1',Key='research/native-library-check/sources/148a777b4af8d5fea0166bdde9d114c6a72035977cb210481bef1922645406b8.tar.gz')
digest=hashlib.sha256()
with response['Body'] as source, open('source.tar.gz','xb') as output:
    while chunk:=source.read(65536):
        digest.update(chunk); output.write(chunk)
assert digest.hexdigest()=='148a777b4af8d5fea0166bdde9d114c6a72035977cb210481bef1922645406b8'
Path('repo').mkdir()
with tarfile.open('source.tar.gz','r:gz') as archive:
    archive.extractall('repo',filter='data')
PY
export PYTHONPATH="$root/repo"
systemd-run --unit=fine-pack-supervisor --wait --pipe -p 'Delegate=cpu memory pids' -p DelegateSubgroup=supervisor -p RuntimeMaxSec=900 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=AWS_RETRY_MODE=standard \
 "$python" -m scripts.launch_fine_pack_diagnostic --remote "$root/repo" "$root" 'b2369c8a03cf06c7d5d0149d9d45442c57cc498e' '148a777b4af8d5fea0166bdde9d114c6a72035977cb210481bef1922645406b8' 'research/hierarchical-cells/20261005/fine-pack-diagnostic-a0002' '6e57f42b7a50e22819267c53a83419db33259650b1fdeff6822a1f3d4dd2895c'
