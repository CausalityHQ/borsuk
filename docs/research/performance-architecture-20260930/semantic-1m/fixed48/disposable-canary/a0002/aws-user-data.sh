#!/bin/bash
set -Eeuo pipefail
root=/mnt/cohere-fixed48-canary
mkdir -p "$root"
cd "$root"
shutdown -h +15
cat >bootstrap.py <<'BOOTSTRAP'
def sdk_guard(model=None):
    """Read the installed service model without credentials or network calls."""
    import boto3, botocore, botocore.session
    model = botocore.session.get_session().get_service_model('s3') if model is None else model
    assert 'IfNoneMatch' in model.operation_model('PutObject').input_shape.members, 'SDK lacks PutObject.IfNoneMatch'
    return dict(boto3=boto3.__version__, botocore=botocore.__version__, conditional_put=True, network_calls=0)
def bootstrap(mode, root, proof, archive_key, prefix, names):
    """Standalone authenticated bootstrap/terminal, embedded before repo imports."""
    import hashlib, json, os, shutil, sys, tarfile, time, urllib.request
    from pathlib import Path
    import boto3
    from botocore.config import Config
    root = Path(root)
    capability = sdk_guard()
    assert capability['boto3'] == capability['botocore'] == '1.40.72', 'pinned canary SDK'
    s3 = boto3.client('s3', region_name='eu-central-1', config=Config(
        retries={'total_max_attempts': 1}, connect_timeout=5, read_timeout=10))
    bucket = 'borsuk-bench-453182569524-euc1'
    def digest(body):
        return hashlib.sha256(body).hexdigest()
    def dump(path, value):
        with (root / path).open('w') as out:
            json.dump(value, out, sort_keys=True); out.write('\n'); out.flush(); os.fsync(out.fileno())
    def size():
        total, seen = 0, set()
        for base, ds, fs in os.walk(root, followlinks=False):
            for n in fs:
                p = Path(base) / n
                if p.is_symlink(): continue  # venv executables; archive links are rejected separately
                st = p.stat(); key = st.st_dev, st.st_ino
                if key not in seen:
                    seen.add(key); total += max(st.st_size, st.st_blocks * 512)
        assert total <= 2147483648, 'bootstrap scratch cap'
        return total
    ledger = root / 'bootstrap-sdk-ledger.jsonl'
    def call(operation, key, **kwargs):
        row = dict(operation=operation, key=key, bucket=bucket, attempts=0, retries=0, error=None)
        token = 'canary-' + str(time.monotonic_ns())
        def sent(**unused):
            row['attempts'] += 1
        s3.meta.events.register('before-send.s3', sent, unique_id=token)
        try:
            response = getattr(s3, operation)(Bucket=bucket, Key=key, **kwargs)
            row.update(status=response['ResponseMetadata']['HTTPStatusCode'],
                retries=response['ResponseMetadata'].get('RetryAttempts', 0))
            assert row['attempts'] == 1 and row['retries'] == 0 and row['status'] == 200, 'bootstrap SDK attempt/status'
            return response
        except BaseException as error:
            row['error'] = type(error).__name__
            raise
        finally:
            s3.meta.events.unregister('before-send.s3', unique_id=token)
            with ledger.open('ab') as out:
                out.write((json.dumps(row, sort_keys=True) + '\n').encode()); out.flush(); os.fsync(out.fileno())
    def get(key, path, length, expected):
        count, h = 0, hashlib.sha256()
        response = call('get_object', key)
        try:
            assert response['ContentLength'] == length
            with path.open('xb') as out:
                while True:
                    chunk = response['Body'].read(min(1048576, length + 1 - count))
                    if not chunk: break
                    count += len(chunk); assert count <= length
                    h.update(chunk); out.write(chunk)
                out.flush(); os.fsync(out.fileno())
            assert count == length and h.hexdigest() == expected, 'bootstrap body authentication'
        finally:
            response['Body'].close()
    try:
        if mode == 'setup':
            used = size()
            head = call('head_object', archive_key)
            length = head['ContentLength']
            assert 0 < length <= 2147483648 - used - 16777216, 'archive admission'
            get(archive_key, root / 'source.tar.gz', length, proof['source_archive_sha256'])
            reserve, seen = 0, set()
            with tarfile.open(root / 'source.tar.gz', mode='r|gz') as archive:
                for m in archive:
                    assert m.name and not m.name.startswith('/') and '..' not in Path(m.name).parts and m.name not in seen
                    assert (m.isfile() or m.isdir()) and m.size >= 0, 'source archive special file/link/size'
                    seen.add(m.name)
                    if m.isfile(): reserve += ((m.size + 4095) // 4096) * 4096
            assert size() + reserve + 16777216 <= 2147483648 and shutil.disk_usage(root).free >= reserve + 16777216, 'source extraction reserve'
            repo = root / 'repo'; repo.mkdir()
            with tarfile.open(root / 'source.tar.gz', mode='r|gz') as archive:
                for m in archive:
                    path = repo / m.name
                    if m.isdir(): path.mkdir(parents=True, exist_ok=True)
                    else:
                        path.parent.mkdir(parents=True, exist_ok=True)
                        with archive.extractfile(m) as stream, path.open('xb') as out:
                            shutil.copyfileobj(stream, out, 1048576)
                        path.chmod(m.mode & 0o777)
            (root / 'source.tar.gz').unlink()
            key = 'research/semantic-router/20261002/fixed48-canary-configs/' + proof['config_sha256'] + '.json'
            get(key, root / 'config.json', proof['config_bytes'], proof['config_sha256'])
            config = json.loads((root / 'config.json').read_bytes())
            assert config['execution_source'] == dict(commit=proof['source_archive_commit'], archive_sha256=proof['source_archive_sha256'])
            assert digest(json.dumps(config['code_sha256'], sort_keys=True, separators=(',', ':')).encode()) == proof['code_identity_sha256']
            for name, pin in config['code_sha256'].items():
                assert not Path(name).is_absolute() and '..' not in Path(name).parts
                assert digest((repo / name).read_bytes()) == pin, 'code authentication before imports'
            destination = repo / proof['config_path']
            assert not destination.exists(), 'self-referencing source/config'
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / 'config.json', destination)
            dump('source-qualification.json', proof)
            dump('bootstrap-staging.json', dict(source_archive_bytes=length, source_archive_sha256=proof['source_archive_sha256'],
                source_authenticated=True, source_extraction_bytes=reserve, source_archive_removed=True,
                config_authenticated=True, code_authenticated_before_import=True, sdk=capability, scratch_bytes=size()))
            return
        assert mode == 'finish'
        original = int(os.environ['CANARY_EXIT_CODE']); code = original
        temporary, cleanup_error = root / 'canary-temp', None
        try:
            (root / 'source.tar.gz').unlink(missing_ok=True)
            if temporary.exists(): shutil.rmtree(temporary)
        except Exception as error:
            code = 96; cleanup_error = type(error).__name__
        if not (root / 'failure.json').exists():
            dump('failure.json', dict(status='failed', phase=os.environ.get('CANARY_PHASE'), original_exit_code=original, replacement_allowed=False))
        if not (root / 'cleanup.json').exists():
            dump('cleanup.json', dict(temporary_files_removed=not temporary.exists() and not (root / 'source.tar.gz').exists(), sdk_closed=True, process_cleanup=False, worker_entered=False))
        instance = ''
        try:
            request = urllib.request.Request('http://169.254.169.254/latest/api/token', method='PUT',
                headers={'X-aws-ec2-metadata-token-ttl-seconds': '60'})
            with urllib.request.urlopen(request, timeout=3) as response: token = response.read(4096).decode()
            request = urllib.request.Request('http://169.254.169.254/latest/meta-data/instance-id',
                headers={'X-aws-ec2-metadata-token': token})
            with urllib.request.urlopen(request, timeout=3) as response: instance = response.read(4096).decode()
        except Exception: code = 96
        files, uploads = {}, []
        # The SDK ledger is uploaded last; its own PUT is counted in terminal.
        ordered = [n for n in names if n != 'bootstrap-sdk-ledger.jsonl'] + ['bootstrap-sdk-ledger.jsonl']
        for name in ordered:
            path = root / name
            if not path.is_file(): continue
            try:
                body = path.read_bytes()
                assert len(body) <= 1048576, 'terminal artifact cap'
                call('put_object', prefix + '/artifacts/' + name, Body=body, IfNoneMatch='*')
                files[name] = dict(bytes=len(body), sha256=digest(body))
                uploads.append(dict(name=name, status=200, attempts=1, retries=0))
            except Exception as error:
                code = 96; uploads.append(dict(name=name, error=type(error).__name__))
        stage_rows = []
        if (root / 'sdk-ledger.jsonl').is_file():
            stage_rows = [json.loads(r) for r in (root / 'sdk-ledger.jsonl').read_bytes().splitlines()]
        boot_rows = [json.loads(r) for r in ledger.read_bytes().splitlines()] if ledger.exists() else []
        requests = len(stage_rows) + len(boot_rows) + 1
        if requests > 64: code = 96
        terminal = dict(schema=proof['campaign_schema'], instance_id=instance,
            source_commit=proof['source_archive_commit'], source_archive_sha256=proof['source_archive_sha256'],
            **{n: proof[n] for n in ('config_sha256', 'code_identity_sha256', 'asset_manifest_sha256', 'artifact_roster_sha256')},
            phase='complete' if code == 0 else os.environ.get('CANARY_PHASE', 'failed'),
            status='complete' if code == 0 else 'failed', original_exit_code=original, exit_code=code,
            infrastructure_status='INFRA_GO' if code == 0 else 'FAIL', performance_measured=False,
            scientific_status='UNMEASURED', artifacts=files, uploads=uploads, cleanup_error=cleanup_error,
            resource_ledger=dict(s3_requests_including_terminal_put=requests, request_cap=64,
                scope='bootstrap + readonly worker SDK + artifact/terminal uploads; excludes root lifecycle/collection',
                limits=proof['resources']), terminal_put_response='confirmed only by parent collection')
        dump('terminal.json', terminal)
        print('BORSUK_TERMINAL ' + json.dumps(terminal, sort_keys=True), flush=True)
        call('put_object', prefix + '/terminal.json', Body=(root / 'terminal.json').read_bytes(), IfNoneMatch='*')
        return code
    finally:
        s3.close()

import base64,gzip,json,sys
proof=json.loads(gzip.decompress(base64.b64decode('H4sIAAAAAAAC/21T21LjMAz9lzy31LJlx+nPZGRZplmaptgp0GH491VZtsws+ypZR+civ3dU16kQr2Nd2ip1bAeyPnT7LnqxzpeEMETJ2WSKjgEF8+CyQ3ECGZzzWXuMPXlynNAm77MlJuw2HbUm6zjTaSrS1m9oFkzgfChiwpBDSB77UsglQz4Vyd4R2SHGAgMOWVIvKImCBGNtLFIUmmk+0/R4GhsfZCYFTUttl6dtmd4kY9zmqZ2XRukoW6YT1ev2BW5zS5ZxynJap/X6zch758VhlBKsSQWMyxGMHawZYrHc50Jx6L0xQYYUii8GvC+hz6TcBcwn8qlMj2O6rtK6/RCxv9fOtB50SV647ao0ocqH3VlqWaqaw7K9FaZVeL1U2Vpjgxmc2TUVpjx5C/PuS9buh6zdnx0Pv9py+mZxF2aELRVvAwZgG4AK9JoohFzEhmApUC8sgoQSqbcMORGk4l0MjJ4UUhkvl8o3Ve+KP58vq4xM5/HScrc3D+B17fkyPl+WlUaVxeput7fGbDpJbWxupONxeb0pvc9suplU80nG4zRPehuizPPNuNtYleeLXkzT23nr9gE3XeNKKx/+2msBe4wuYNSW1JeJfwCFoEDqQ5U8zjIv9frf4Vm5jWnJ9y4YjL4P2npVjV9FhVqpPX0R8mA33etSn/S//LvVmA8d/TRs/Iz1Rc1aZn2kaTjN1SWyaKUwagIDFAmEXo/fcPaY7UDsU/cD4h4oGU6QAyerhxgi6c3mASC7DClFtGA8gPVorACGknos+j01U+QIQtx9/Ab2qP969AMAAA==')))
sys.exit(bootstrap(sys.argv[1], '/mnt/cohere-fixed48-canary', proof, 'research/native-library-check/sources/a0cb1d6cb29b668a48ed911d3d1bb8421051125402e146fb74f0c4ba14c81eac.tar.gz', 'research/semantic-router/20261002/fixed48-canary-a0002', ('config.json', 'source-qualification.json', 'bootstrap-staging.json', 'bootstrap-sdk-ledger.jsonl', 'sdk-ledger.jsonl', 'asset-heads.json', 'small-body.json', 'imports.json', 'cli.json', 'cli.log', 'resources.json', 'cleanup.json', 'summary.json', 'failure.json', 'profile.log', 'profile-resources.txt')) or 0)
BOOTSTRAP
cat >worker.sh <<'WORKER'
#!/bin/bash
set -Eeuo pipefail
cd /mnt/cohere-fixed48-canary
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  set +e
  sdk_python="$PWD/venv/bin/python"
  test -x "$sdk_python" || sdk_python=python3.12
  CANARY_EXIT_CODE="$code" CANARY_PHASE="$phase" "$sdk_python" bootstrap.py finish >>terminal-upload.log 2>&1
  result=$?
  cat terminal-upload.log >/dev/ttyS0 2>/dev/null || true
  shutdown -h now || true
  exit "$result"
}
trap finish EXIT
trap 'exit 143' TERM
export AWS_MAX_ATTEMPTS=1 AWS_METADATA_SERVICE_NUM_ATTEMPTS=1 DEBIAN_FRONTEND=noninteractive LC_ALL=C
mkdir -p apt
exec >profile.log 2>&1
phase=install
if ! python3.12 -m venv "$PWD/venv"; then
  timeout --kill-after=5 60 apt-get -o Dir::Cache::archives="$PWD/apt" -qq -o Acquire::Retries=0 -o DPkg::Lock::Timeout=15 update
  timeout --kill-after=5 90 apt-get -o Dir::Cache::archives="$PWD/apt" -qq -y -o Acquire::Retries=0 -o DPkg::Lock::Timeout=15 install python3.12-venv
  python3.12 -m venv "$PWD/venv"
fi
export TMPDIR="$PWD/pip-temp"
mkdir -p "$TMPDIR"
timeout --kill-after=5 90 "$PWD/venv/bin/python" -m pip install --disable-pip-version-check --no-cache-dir --only-binary=:all: --retries=0 --timeout=10 boto3==1.40.72 botocore==1.40.72
rm -rf "$TMPDIR"
phase=source
"$PWD/venv/bin/python" bootstrap.py setup
phase=canary
export PYTHONPATH="$PWD/repo" PYTHONDONTWRITEBYTECODE=1
/usr/bin/time -v -o profile-resources.txt timeout --signal=TERM --kill-after=5 600 \
  "$PWD/venv/bin/python" -m scripts.launch_cohere_fixed48_canary_spot --stage "$PWD/repo" "$PWD" research/semantic-router/20261002/fixed48-canary-a0002
phase=complete
WORKER
systemd-run --unit=cohere-fixed48-canary --wait --pipe -p MemoryMax=2147483648 -p MemorySwapMax=0 \
 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=660 -p KillMode=control-group \
 -p WorkingDirectory="$root" /bin/bash "$root/worker.sh"
