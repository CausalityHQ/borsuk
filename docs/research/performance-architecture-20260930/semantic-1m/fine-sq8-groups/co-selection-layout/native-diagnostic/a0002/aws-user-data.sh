#!/bin/bash
set -euo pipefail
export AWS_MAX_ATTEMPTS=1 AWS_RETRY_MODE=standard AWS_DEFAULT_REGION=eu-central-1
systemd-run --unit=fine-pack-stop --on-active=9000s /usr/sbin/shutdown -h now
root=/mnt/co-selection-layout-diagnostic
mkdir "$root"
cd "$root"
trap '/usr/sbin/shutdown -h now || true' EXIT
exec >run.log 2>&1
test "$(uname -m)" = x86_64
mkdir -p "$root/bootstrap/tmp" "$root/bootstrap/archives/partial" "$root/bootstrap/lists/partial" "$root/bootstrap/log"
export TMPDIR="$root/bootstrap/tmp" TMP="$root/bootstrap/tmp" TEMP="$root/bootstrap/tmp" PIP_CACHE_DIR="$root/bootstrap/cache" PYTHONPYCACHEPREFIX="$root/bootstrap/pycache"
cat >"$root/bootstrap/watch.py" <<'WATCH'
import json, os, signal, sys, time
from pathlib import Path
root=Path('/mnt/co-selection-layout-diagnostic'); SQ4_INPUT_ROOT=Path('/mnt/hierarchical-100k')
def scratch_observation(root):
    """Charge apparent or allocated bytes, including temporary files and symlinks."""
    sizes = {}
    for tree in (Path(root), SQ4_INPUT_ROOT):
        total = 0
        if tree.exists():
            for directory, names, files in os.walk(tree, followlinks=False):
                for path in (Path(directory), *(Path(directory)/n for n in files),
                             *(Path(directory)/n for n in names if (Path(directory)/n).is_symlink())):
                    try:
                        s = path.lstat()
                    except FileNotFoundError:
                        continue  # A removed temporary file no longer coexists.
                    total += max(s.st_size, s.st_blocks*512)
        sizes[str(tree)] = total
    return dict(roots=sizes, whole_scratch_bytes=sum(sizes.values()))

reserve={'auxiliary_bytes': 67108864, 'binary_bytes': 13549088, 'bootstrap_bytes': 1073741824, 'cap_bytes': 4294967296, 'input_bytes': 689304557, 'native_output_bytes': 67108864, 'source_archive_bytes': 134217728}
remaining=sum(reserve[k] for k in ('input_bytes','native_output_bytes','binary_bytes','auxiliary_bytes'))
receipt=dict(interval_seconds=1, sample_count=0, peak_bytes=0, closed=False, cap_exceeded=False, reserve=reserve)
running=True
def stop(*args):
    global running
    running=False
signal.signal(signal.SIGTERM,stop)
try:
    available=os.statvfs(root).f_bavail*os.statvfs(root).f_frsize
    assert available >= sum(v for k,v in reserve.items() if k!='cap_bytes'), 'bootstrap free scratch admission'
    while True:
        observation=scratch_observation(root)
        receipt.update(last=observation, sample_count=receipt['sample_count']+1, peak_bytes=max(receipt['peak_bytes'],observation['whole_scratch_bytes']))
        assert receipt['peak_bytes']+remaining <= reserve['cap_bytes'], 'bootstrap/input/output overlap cap'
        assert receipt['peak_bytes'] <= reserve['bootstrap_bytes']+reserve['source_archive_bytes'], 'bootstrap/source reserve'
        if receipt['sample_count']==1:(root/'bootstrap/ready').touch(exist_ok=False)
        if not running:break
        time.sleep(1)
    receipt['closed']=True
except BaseException:
    receipt['cap_exceeded']=True
    os.kill(int(sys.argv[1]),signal.SIGTERM)
    raise
finally:
    if scratch_observation(root)['whole_scratch_bytes']+65536 <= reserve['cap_bytes']:
        with (root/'bootstrap/scratch.json').open('x') as output:
            json.dump(receipt,output);output.flush();os.fsync(output.fileno())
WATCH
python3 "$root/bootstrap/watch.py" $$ &
watcher=$!
while ! test -f "$root/bootstrap/ready"; do kill -0 "$watcher"; sleep .05; done
apt-get -o Dir::Cache::archives="$root/bootstrap/archives" -o Dir::State::lists="$root/bootstrap/lists" -o Dir::Log="$root/bootstrap/log" -o APT::Sandbox::User=root update -qq
DEBIAN_FRONTEND=noninteractive apt-get -o Dir::Cache::archives="$root/bootstrap/archives" -o Dir::State::lists="$root/bootstrap/lists" -o Dir::Log="$root/bootstrap/log" -o APT::Sandbox::User=root install -y -qq python3.12 python3.12-venv tar gzip
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
response=s3.get_object(Bucket='borsuk-bench-453182569524-euc1',Key='research/native-library-check/sources/b17f5ddc8599ae54048cd2d8f389927d6d91583c72f862c14bc7ab94c3cefd70.tar.gz')
digest=hashlib.sha256()
with response['Body'] as source, open('source.tar.gz','xb') as output:
    while chunk:=source.read(65536):
        assert output.tell()+len(chunk) <= 134217728, 'source archive scratch reserve'; digest.update(chunk); output.write(chunk)
assert digest.hexdigest()=='b17f5ddc8599ae54048cd2d8f389927d6d91583c72f862c14bc7ab94c3cefd70'
Path('repo').mkdir()
with tarfile.open('source.tar.gz','r:gz') as archive:
    assert Path('source.tar.gz').stat().st_size+sum(m.size for m in archive.getmembers())+4096*len(archive.getmembers()) <= 134217728, 'source extraction overlap reserve'
    assert sorted(m.name for m in archive.getmembers() if m.isfile()) == ['docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates/a0001/aws-closeout.json', 'docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates/a0001/aws-terminal.json', 'docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates/a0001/source-after.json.gz', 'docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates/a0001/source-before.json.gz', 'docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates/a0001/source-qualification.json.gz', 'docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates/a0001/workspace-cgroup.json.gz', 'docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates/a0001/workspace-receipt.json.gz', 'docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates/config.json', 'docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/implementation-gates/native-source-manifest.json', 'docs/research/performance-architecture-20260930/semantic-1m/fine-sq8-groups/co-selection-layout/native-diagnostic/config.json', 'scripts/check_co_selection_implementation.sh', 'scripts/check_native_metadata_ranges_build.py', 'scripts/check_native_metadata_ranges_stats.py', 'scripts/check_native_startup_build.py', 'scripts/check_native_startup_stats.py', 'scripts/launch_fine_pack_diagnostic.py', 'scripts/launch_native_metadata_ranges_cold_spot.py', 'scripts/launch_native_peer_1m_spot.py', 'scripts/launch_native_startup_profile_spot.py', 'scripts/launch_v157_primary_feasibility_spot.py', 'scripts/launch_v174_relaid_bind_compile_spot.py', 'scripts/rest_coexistence_load.py', 'scripts/run_native_cold_first_query.py', 'scripts/run_native_metadata_ranges_cold.py', 'scripts/run_native_peer_1m_worker.py', 'scripts/run_native_peer_offered_http.py', 'scripts/run_native_union_http.py', 'scripts/run_native_union_offered_http.py'], 'canary source archive roster'
    assert all(m.isfile() or m.isdir() for m in archive.getmembers()), 'canary archive regular files'
    archive.extractall('repo',filter='data')
PY
kill "$watcher"
wait "$watcher"
export PYTHONPATH="$root/repo"
systemd-run --unit=fine-pack-supervisor --wait --pipe -p 'ReadOnlyPaths=/tmp /var/tmp' -p 'Delegate=cpu memory pids' -p DelegateSubgroup=supervisor -p RuntimeMaxSec=9000 -p WorkingDirectory="$root" \
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=AWS_RETRY_MODE=standard \
 --setenv=TMPDIR="$TMPDIR" --setenv=TMP="$TMP" --setenv=TEMP="$TEMP" --setenv=PYTHONPYCACHEPREFIX="$PYTHONPYCACHEPREFIX" \
 "$python" -m scripts.launch_fine_pack_diagnostic --co-selection-layout --remote "$root/repo" "$root" '7cbcdce671da7c4a25b3ff19cd755cacdd3a9aa4' 'b17f5ddc8599ae54048cd2d8f389927d6d91583c72f862c14bc7ab94c3cefd70' 'research/hierarchical-cells/20261006/co-selection-layout-diagnostic-a0002' '0aecf0091a391502bb08df7cf2fff469989b7f906e80fa620231d12527d1810a'
