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
response=s3.get_object(Bucket='borsuk-bench-453182569524-euc1',Key='research/native-library-check/sources/aef4c25f0187408a7f8a3dcb46b87a09a6194b05da9fff553db6b10cc0a6e0b1.tar.gz')
digest=hashlib.sha256()
with response['Body'] as source, open('source.tar.gz','xb') as output:
    while chunk:=source.read(65536):
        digest.update(chunk); output.write(chunk)
assert digest.hexdigest()=='aef4c25f0187408a7f8a3dcb46b87a09a6194b05da9fff553db6b10cc0a6e0b1'
Path('repo').mkdir()
with tarfile.open('source.tar.gz','r:gz') as archive:
    archive.extractall('repo',filter='data')
PY
export PYTHONPATH="$root/repo"
"$python" -m scripts.launch_fine_pack_diagnostic --remote "$root/repo" "$root" '3e9fbf989b119967ec2e4443d96ed9a7ebc27960' 'aef4c25f0187408a7f8a3dcb46b87a09a6194b05da9fff553db6b10cc0a6e0b1' 'research/hierarchical-cells/20261005/fine-pack-diagnostic-a0001' '4e3f3687dabeeef35c6b3de361d55f533ae0f13a144e90a8660cf734613e958a'
