#!/usr/bin/env python3
"""Retained a5 offered gate; parent freezes source, then three JSON authorities.

aNNNN | --contract | --self-check | --stage REPO WORKER_ROOT PREFIX
Runtime integration is a separate gate; --self-check uses explicit temp fixtures.
Shared cold helpers own SDK staging, every ACK, termination/wait and collection.
"""
import base64
from contextlib import contextmanager
import fcntl
import gzip
import io
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import stat
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
sys.dont_write_bytecode = True
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import launch_cohere_fixed48_cold_http_spot as cold
from scripts import launch_cohere_fixed48_canary_spot as canary
from scripts import run_native_cold_offered as offered
# Capture runtime CODE before preflight substitutes the larger cold.CODE closure.
from scripts import run_cohere_fixed48_offered_http as offered_runtime

science, fixed = cold.science, cold.runtime
encoded, sha, artifact = cold.encoded, cold.sha, cold.artifact
regular_path, read_json, lifecycle = cold.regular_path, cold.read_json, cold.lifecycle
shared_preflight = cold.preflight
OWN = 'scripts/launch_cohere_fixed48_offered_http_spot.py'
RUNTIME = 'scripts/run_cohere_fixed48_offered_http.py'
MODULE = OWN[:-3].replace('/', '.')
ROOT = fixed.BASE / 'offered-http'
CONFIG, CONTROL, MANIFEST = (ROOT / n for n in ('config.json', 'controller-config.json', 'asset-manifest.json'))
NAME = ''
SCHEMA = 'borsuk-cohere-fixed48-offered-http-spot-v1'
ASSET_SCHEMA = 'borsuk-fixed48-offered-assets-v1'
PREFIX, TOKEN_PREFIX, TAG = ('research/semantic-router/20261002/fixed48-offered-',
    'fixed48-offered-', 'borsuk-fixed48-offered-http')
WORK_ROOT = Path('/mnt/cohere-fixed48-offered')
WALL, SERVICE_SECONDS, WORKER_SECONDS, CLEANUP_SECONDS = 1800, 1500, 1440, 90
MEMORY, SCRATCH, OUTPUT_BYTES, STATIC_BYTES = 12 << 30, 16 << 30, 64 << 20, 2 << 30
REGION, BUCKET = cold.REGION, cold.BUCKET
INSTANCE_TYPE, IMAGE_ID, ROOT_DEVICE_NAME, SUBNET = (cold.INSTANCE_TYPE, cold.IMAGE_ID, cold.ROOT_DEVICE_NAME, cold.SUBNET)
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP, ALLOWANCE = .50, .30, .15
AWSCLI_VERSION, AWSCLI_SHA256 = cold.AWSCLI_VERSION, cold.AWSCLI_SHA256
CODE = tuple(sorted(set((*cold.CODE, *canary.CODE, *offered.CODE, RUNTIME, OWN))))
CELL_FILES = tuple(f'rate{i}-{suffix}' for i in range(6) for suffix in ('records.jsonl', 'summary.json'))
RUNTIME_FILES = ('config.json', 'source-qualification.json', 'input-hashes.json', 'records.jsonl',
    'failures.jsonl', 'summary.json', 'resources.json', 'offered-cgroup.json', 'cleanup.json',
    'failure.json', 'terminal.json', *CELL_FILES)
CONTROLLER_FILES = ('config.json', 'controller-config.json', 'asset-manifest.json', 'source-qualification.json',
    'bootstrap-staging.json', 'controller-sdk-ledger.jsonl', 'cpu.txt', 'tool-versions.json',
    'run-closed.log', 'profile.log', 'profile-resources.txt', 'profile-cgroup.json',
    'staging.json', 'offered-closure.json', 'failure.json')
ARTIFACTS = (*CONTROLLER_FILES, *('screen/' + n for n in RUNTIME_FILES))
TERMINAL_IDENTITIES = ('config_sha256', 'controller_config_sha256', 'asset_manifest_sha256',
    'code_identity_sha256', 'runtime_code_identity_sha256', 'refs_identity_sha256',
    'artifact_roster_sha256', 'native_source_identity_sha256', 'binary_sha256',
    'campaign_schema', 'awscli_version', 'awscli_sha256')
FIELDS = {'schema', 'authority_pending', 'execution_source', 'code_sha256', 'runtime_config', 'asset_manifest'}
HTTP = 'native/binaries/two_bit_http'
UNUSED = ('native/binaries/check_semantic_router_scorer', 'native/binaries/two_bit_plan_demo')
MUTABLE = ('run.log', 'run-closed.log', 'profile.log', 'profile-resources.txt', 'profile-cgroup.json',
    'staging.json', 'offered-closure.json', 'failure.json', 'terminal.json')
DIAGNOSTIC_FILES = ('failure.json', 'offered-closure.json', 'screen/summary.json', 'screen/cleanup.json', 'profile.log')


def write(path, value):
    Path(path).parent.mkdir(parents=True,exist_ok=True)
    cold.write(path,value)


def runtime():
    return offered_runtime


def code_closure(repo):
    with patch.object(cold, 'OWN', OWN), patch.object(fixed, 'CODE', runtime().CODE):
        return cold.code_closure(repo)


def contract():
    return dict(schema=SCHEMA+'-contract', freeze_owner='parent', root=str(ROOT),
        config=str(CONFIG), controller_config=str(CONTROL), asset_manifest=str(MANIFEST),
        controller_fields=sorted(FIELDS), controller_schema=SCHEMA, asset_schema=ASSET_SCHEMA,
        pointer='path/bytes/sha256; runtime historical pointers may additionally use archived_path=path+.gz',
        CODE=list(CODE), runtime_CODE=list(runtime().CODE), ARTIFACTS=list(ARTIFACTS),
        runtime_outputs=list(RUNTIME_FILES), runtime_fixed=runtime().FIXED, resources=runtime().LIMITS,
        staging_assets=[HTTP], unused_historical_binaries=list(UNUSED),
        staging='Three small frozen JSONs, proof, archived small historical bodies and qualified HTTP only. No new publication/build/scorer/oracle/full plane.',
        source_seal=dict(scope='entire worker root, excluding fresh external screen output',
            total_reservation_bytes=STATIC_BYTES, controller_growth_reserve_bytes=OUTPUT_BYTES,
            remove_before_run=['source.tar.gz', 'awscliv2.zip', 'aws/', 'apt/', 'pip-temp/'],
            mutable_controller_files=list(MUTABLE), retained='All other existing regular files/directories sealed readonly; every byte charged to SAME 2GiB reservation.',
            dispatch='Runtime observes mutable external output; callback checks only bounded controller files; whole-worker inventory before/after campaign.'),
        measurement_prefix=PREFIX+'aNNNN', retained_namespace='authenticated a5 namespace; no publication',
        cli='aNNNN | --contract | --self-check | --stage REPO WORKER_ROOT PREFIX',
        callback='Valid closed/drained cell only; fsynced records then summary, conditional PUT records then summary; INVALID never uploaded as a cell marker.',
        instance_type=INSTANCE_TYPE, image_id=IMAGE_ID, region=REGION, root_device_name=ROOT_DEVICE_NAME,
        root_volume=dict(encrypted=True, bytes_gib=80, type='gp3', delete_on_termination=True),
        metadata='IMDSv2 required', machine_limit_seconds=WALL, service_limit_seconds=SERVICE_SECONDS,
        worker_limit_seconds=WORKER_SECONDS, cleanup_reserve_seconds=CLEANUP_SECONDS,
        compute_cap_usd=COMPUTE_CAP, spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR,
        ebs_s3_allowance_usd=ALLOWANCE, freeze='CODE in clean pushed source archive; three JSON authorities committed separately afterwards.',
        scientific_status='PASS/FAIL with execution0; INVALID nonzero; all384 declared positions including aborted.',
        runtime_integration='Parent must check final combined qualify/callback/replay before freeze/cloud.', launch_authorized=False)


def qualify(base=Path('.'), config_path=None, config_sha=None):
    repo = regular_path(base).resolve()
    path = regular_path(repo/CONFIG if config_path is None else config_path)
    identity = artifact(path)
    assert 0 < identity['bytes'] <= 1 << 20 and (config_sha is None or identity['sha256'] == config_sha), 'config identity'
    control = read_json(repo/CONTROL)
    assert set(control) == FIELDS and control['schema'] == SCHEMA and control['authority_pending'] is False, 'controller freeze pending'
    assert science.read_repo(repo, control['runtime_config'], CONFIG) == path.read_bytes()
    closure = code_closure(repo)
    assert closure == CODE and control['code_sha256'] == {n:artifact(repo/n)['sha256'] for n in closure}, 'controller source drift'
    worker = runtime()
    assert tuple(worker.OUTPUTS) == RUNTIME_FILES and worker.UNUSED == UNUSED, 'runtime artifact seam drift'
    config, proof, evidence = worker.qualify(path, identity['sha256'], repo)
    assert config['execution_source'] == control['execution_source'], 'source authorities differ'
    assert config['bucket'] == BUCKET and config['region'] == REGION
    assert config['resources']['static_repository_reserve_bytes'] == STATIC_BYTES
    assert config['resources']['output_reserve_bytes'] == OUTPUT_BYTES
    assert config['worker_limit_seconds'] == WORKER_SECONDS and config['service_limit_seconds'] == SERVICE_SECONDS
    assert config['machine_limit_seconds'] == WALL and config['cleanup_reserve_seconds'] == CLEANUP_SECONDS
    manifest = json.loads(science.read_repo(repo, control['asset_manifest'], MANIFEST))
    assert set(manifest) == {'schema','authority_pending','assets'}
    assert manifest['schema'] == ASSET_SCHEMA and manifest['authority_pending'] is False
    archived = json.loads(worker.read(repo, config['cold_run']['files']['asset-manifest.json']))
    entry = archived['assets']['qualification/binaries/two_bit_http']
    assert manifest['assets'] == {HTTP:entry}, 'only qualified HTTP binary may be staged'
    assert worker.identity(entry) == worker.identity(evidence['binary']), 'HTTP identity bridge'
    assert set(entry['source']) <= {'bucket','key','etag','version_id'} and {'bucket','key'} <= set(entry['source'])
    assert entry['source']['bucket'] == BUCKET
    cold.driver.safe_key(entry['source']['key'])
    return dict(config_sha256=identity['sha256'], controller_config_sha256=artifact(repo/CONTROL)['sha256'],
        asset_manifest_sha256=control['asset_manifest']['sha256'],
        code_identity_sha256=sha(encoded(control['code_sha256'])), runtime_code_identity_sha256=proof['code_identity_sha256'],
        refs_identity_sha256=proof['refs_identity_sha256'], binary_sha256=proof['binary_sha256'],
        native_source_identity_sha256=proof['native_source_identity_sha256'], native_source_file_count=399,
        artifact_roster_sha256=sha(encoded(ARTIFACTS)), campaign_schema=SCHEMA,
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256,
        source_archive_commit=config['execution_source']['commit'], source_archive_sha256=config['execution_source']['archive_sha256'],
        namespace_prefix=config['namespace_prefix'], output_reserve_bytes=OUTPUT_BYTES,
        authority_paths=list(map(str,(CONFIG,CONTROL,MANIFEST))), assets=manifest['assets'])


def preflight(base=Path('.'), collection_out=None):
    with patch.multiple(cold, ROOT=ROOT, CONFIG=CONFIG, CODE=CODE, qualify=qualify):
        return shared_preflight(base, collection_out)


def config_key(digest):
    assert re.fullmatch('[0-9a-f]{64}', digest)
    return 'research/semantic-router/20261002/fixed48-offered-configs/'+digest+'.json'


def stage_configs(proof):
    with patch.object(cold, 'config_key', config_key):
        return cold.stage_configs(proof)


def user_data(commit, archive_sha, archive_key, prefix, proof):
    assert (commit,archive_sha) == (proof['source_archive_commit'],proof['source_archive_sha256'])
    assert re.fullmatch(re.escape(PREFIX)+'a[0-9]{4}',prefix)
    # Cold's namespace check belongs to its publisher. Retained offers never publish.
    adapter = dict(proof,namespace_prefix=prefix+'/serving')
    with patch.multiple(cold, CONFIG=CONFIG, ROOT=ROOT, SCHEMA=SCHEMA, PREFIX=PREFIX,
        MODULE=MODULE, WORK_ROOT=WORK_ROOT, WALL=WALL, WORKER_SECONDS=WORKER_SECONDS,
        SERVICE_SECONDS=SERVICE_SECONDS, MEMORY=MEMORY, SCRATCH=SCRATCH,
        ARTIFACTS=ARTIFACTS, TERMINAL_IDENTITIES=TERMINAL_IDENTITIES, DIAGNOSTIC_FILES=DIAGNOSTIC_FILES):
        body = cold.user_data(commit,archive_sha,archive_key,prefix,adapter)
    early = {k:proof[k] for k in (*TERMINAL_IDENTITIES,'source_archive_commit','source_archive_sha256')}
    old = dict(early,proof_sha256=sha(encoded(adapter)))
    new = dict(early,proof_sha256=sha(encoded(proof)))
    encode = lambda v:base64.b64encode(gzip.compress(encoded(v),mtime=0)).decode()
    assert body.count(encode(old)) == 1, 'early proof hook drift'
    body = body.replace(encode(old),encode(new),1)
    body = body.replace('BORSUK_COLD_', 'BORSUK_OFFERED_').replace('cohere-fixed48-cold', 'cohere-fixed48-offered')
    body = body.replace('--setenv=PYTHONPATH=', '--setenv=PYTHONDONTWRITEBYTECODE=1 --setenv=PYTHONPATH=')
    body = body.replace('export DEBIAN_FRONTEND=', 'export PYTHONDONTWRITEBYTECODE=1\nexport DEBIAN_FRONTEND=',1)
    cleanup = 'rm -f "$root/source.tar.gz" "$root/awscliv2.zip"\nrm -rf "$root/aws" "$root/apt" "$root/pip-temp"\n'
    assert body.count('phase=install\n') == 1
    body = body.replace('phase=install\n', 'phase=install\n'+cleanup,1)
    # Both cell checkpoints and terminal bodies are immutable, one-attempt PUTs.
    for target, key in (('$root/$name',f's3://{BUCKET}/{prefix}/artifacts/$name'),
                        ('$root/terminal.json',f's3://{BUCKET}/{prefix}/terminal.json')):
        old = f'"$root/bin/aws" s3 cp "{target}" "{key}" --only-show-errors'
        new = f'"$root/bin/aws" s3api put-object --if-none-match \'*\' --bucket {BUCKET} --key "{key.split(BUCKET+"/",1)[1]}" --body "{target}" --cli-connect-timeout 5 --cli-read-timeout 15 --no-cli-pager'
        assert body.count(old) == 1, 'conditional upload hook drift'
        body = body.replace(old,new,1)
    for label, program in re.findall(r"<<'([A-Z]+)'[^\n]*\n(.*?)\n\1\n",body,re.S):
        compile(program,'<offered-'+label+'>','exec')
    subprocess.run(['bash','-n'],input=body,text=True,check=True,timeout=5)
    assert len(body.encode()) <= 16384, 'EC2 user-data cap'
    assert not any(x in body for x in ('cargo ', 'rustup', '--publish', 's3 cp ', 'two_bit_plan_demo', 'check_semantic_router_scorer'))
    return body


def controller_bound(root):
    total = 0
    for name in MUTABLE:
        path = regular_path(root/name)
        if path.exists():
            assert path.is_file()
            size = path.stat(); total += max(size.st_size,size.st_blocks*512)
    assert total <= OUTPUT_BYTES, 'bounded controller log/closeout reserve'
    return total


def seal_worker(root, repo):
    usage = science.scratch_usage(root)
    assert max(usage.values())+OUTPUT_BYTES <= STATIC_BYTES, 'ALL worker static bodies plus controller growth must fit SAME2GiB'
    assert science.scratch_bytes(root)+OUTPUT_BYTES <= SCRATCH
    assert not (root/'screen').exists()
    for parent, dirs, files in os.walk(root,followlinks=False,topdown=False):
        for name in files:
            path = Path(parent)/name
            if path.is_symlink():
                assert not path.is_relative_to(repo), 'source symlink'
                continue  # venv/platform links; their inode is inventoried, no copied payload.
            assert stat.S_ISREG(path.stat().st_mode), 'nonregular worker body'
            if path.relative_to(root).as_posix() not in MUTABLE:
                path.chmod(0o444 | (path.stat().st_mode & 0o111))
        if Path(parent) != root:
            Path(parent).chmod(0o555)
    assert runtime().scratch_usage(repo,readonly=True)
    return dict(sealed=True,whole_worker_static_usage=usage,total_reservation_bytes=STATIC_BYTES,
        controller_growth_reserve_bytes=OUTPUT_BYTES,mutable_controller_files=list(MUTABLE),
        static_scope='ALL existing worker-root bodies including source, venv, installed CLI and staged authorities; external screen separately observed')


def checkpoint(marker, paths, config, proof, output, uploaded, put):
    assert set(paths) == {'records','summary'}
    index = marker['rate_index']
    assert type(index) is int and 0 <= index < 6 and index == len(uploaded), 'duplicate/out-of-order cell'
    assert marker['closed'] is marker['cleanup_confirmed'] is marker['execution_gate_passed'] is True
    assert marker['status'] in ('PASS','FAIL'), 'INVALID cell marker forbidden'
    paths = {n:regular_path(p) for n,p in paths.items()}
    for n,suffix in (('records','records.jsonl'),('summary','summary.json')):
        assert paths[n].is_file() and paths[n].resolve() == output.resolve()/f'rate{index}-{suffix}'
        assert artifact(paths[n])['bytes'] <= OUTPUT_BYTES
    assert read_json(paths['summary']) == marker and artifact(paths['records']) == marker['records']
    for key in ('config_sha256','code_identity_sha256','refs_identity_sha256','binary_sha256'):
        assert marker[key] == proof[key], 'cell authority drift'
    rows = [json.loads(line) for line in paths['records'].read_bytes().splitlines()]
    assert len(rows) == 64 and [r['query_ordinal'] for r in rows] == list(range(64))
    assert all(r['rate_index'] == index for r in rows)
    for name in ('records','summary'):
        put([str(WORK_ROOT/'bin/aws'),'s3api','put-object','--if-none-match','*',
            '--bucket',config['bucket'],'--key',config['measurement_prefix']+'/cells/'+paths[name].name,
            '--body',str(paths[name]),'--region',config['region'],
            '--cli-connect-timeout','5','--cli-read-timeout','15','--no-cli-pager'])
    uploaded.add(index)


def stage(repo, out, prefix):
    repo, out = regular_path(repo).resolve(), regular_path(out).resolve()
    assert out == WORK_ROOT and repo == out/'repo', 'exact worker root'
    assert re.fullmatch(re.escape(PREFIX)+'a[0-9]{4}',prefix)
    started, before = time.monotonic(), fixed.snapshot()
    closure = dict(closed=False,runtime_invocations=0,build_invocations=0,publication_invocations=0,
        scientific_scorer_invocations=0,oracle_invocations=0,process_cleanup=False,replay_passed=False,
        execution_status='FAIL',scientific_status='INVALID')
    staging = dict(closed=False,peak_scratch_bytes=0,sealed=False)
    failure = dict(status='pending',replacement_allowed=False)
    s3, uploaded = None, set()
    previous = signal.getsignal(signal.SIGTERM)
    def interrupted(signum,frame):
        raise InterruptedError('controller stage interrupted')
    signal.signal(signal.SIGTERM,interrupted)
    try:
        fixed.check_cgroup(before,before,runtime().LIMITS,drained=True)
        import boto3
        from botocore.config import Config
        capability = fixed.sdk_guard()
        s3 = boto3.client('s3',region_name=REGION,config=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=5))
        early = read_json(out/'source-qualification.json')
        deadline = started+WORKER_SECONDS-CLEANUP_SECONDS
        def bound(reserve=0):
            assert time.monotonic() < deadline, 'staging deadline'
            usage = science.scratch_usage(out)
            size = max(usage.values()); staging['peak_scratch_bytes'] = max(staging['peak_scratch_bytes'],size)
            assert size+reserve <= SCRATCH and shutil.disk_usage(out).free >= reserve
        with (out/'controller-sdk-ledger.jsonl').open('xb') as ledger:
            def download(digest,target):
                with fixed.sdk_operation(s3,'head_object',BUCKET,config_key(digest),ledger) as (response,row):
                    assert response['ResponseMetadata']['HTTPStatusCode'] == 200
                    identity = science.pin(dict(bytes=response['ContentLength'],sha256=digest),1 << 20)
                fixed.fetch(s3,dict(bucket=BUCKET,key=config_key(digest)),identity,ledger,bound,deadline,target)
            download(early['proof_sha256'],out/'frozen-qualification.json')
            proof = read_json(out/'frozen-qualification.json')
            assert all(early[k] == proof[k] for k in (*TERMINAL_IDENTITIES,'source_archive_commit','source_archive_sha256'))
            for name,key in (('config.json','config_sha256'),('controller-config.json','controller_config_sha256'),('asset-manifest.json','asset_manifest_sha256')):
                download(proof[key],out/name)
                destination = repo/ROOT/name
                assert not destination.exists(), 'frozen JSON present in execution archive'
                write(destination,(out/name).read_bytes())
            config = read_json(out/'config.json')
            assert config['execution_source'] == dict(commit=proof['source_archive_commit'],archive_sha256=proof['source_archive_sha256'])
            for key,env in (('commit','BORSUK_OFFERED_SOURCE_COMMIT'),('archive_sha256','BORSUK_OFFERED_ARCHIVE_SHA256')):
                assert os.environ.get(env) == config['execution_source'][key], 'execution source environment'
            assert set(proof['assets']) == {HTTP}, 'bounded staging roster'
            entry = proof['assets'][HTTP]
            pointer = config['cold_run']['files'][HTTP]
            assert runtime().identity(pointer) == runtime().identity(entry), 'staging HTTP identity'
            target = repo/pointer.get('archived_path',pointer['path'])
            if not target.exists():
                assert 'archived_path' not in pointer, 'missing compressed historical HTTP'
                source = entry['source']
                assert set(source) <= {'bucket','key','etag','version_id'} and source['bucket'] == BUCKET
                fixed.fetch(s3,source,entry,ledger,bound,deadline,target,etag=source.get('etag'))
            runtime().read(repo,pointer,materialize=False)
        s3.close(); s3 = None
        # Exactly these unused archived binaries can be absent under root's full56 attestation.
        for name in UNUSED:
            pointer = config['cold_run']['files'][name]
            regular_path(repo/pointer.get('archived_path',pointer['path'])).unlink(missing_ok=True)
        assert qualify(repo,out/'config.json',proof['config_sha256']) == proof, 'worker authority drift'
        write(out/'source-qualification.json',proof)
        import platform
        release = platform.freedesktop_os_release()
        assert platform.machine() == 'x86_64' and (release['ID'],release['VERSION_ID']) == ('ubuntu','24.04')
        assert all(os.environ.get(n) == '2' for n in fixed.retained.THREAD_ENV) and os.environ.get('AWS_MAX_ATTEMPTS') == '1'
        write(out/'tool-versions.json',dict(python=sys.version,architecture=platform.machine(),os_release=release,
            thread_environment=dict.fromkeys(fixed.retained.THREAD_ENV,'2'),aws_max_attempts=1,sdk=capability))
        _,bootstrap = lifecycle()
        # Check the installed CLI before any native process, using the shared one-attempt reaper.
        cli = str(out/'bin/aws')
        assert bootstrap._checkpoint_cli_call([cli,'--version']).startswith(('aws-cli/'+AWSCLI_VERSION+' ').encode())
        assert 'IfNoneMatch' in json.loads(bootstrap._checkpoint_cli_call([cli,'s3api','put-object',
            '--generate-cli-skeleton','input','--no-sign-request','--no-cli-pager']))
        staging.update(seal_worker(out,repo))
        worker = runtime()
        _,worker_proof,_ = worker.qualify(out/'config.json',proof['config_sha256'],repo)
        def closed(marker,paths):
            controller_bound(out)
            checkpoint(marker,paths,dict(config,measurement_prefix=prefix),worker_proof,out/'screen',uploaded,bootstrap._checkpoint_cli_call)
        closure['runtime_invocations'] = 1
        worker.main(out/'config.json',proof['config_sha256'],repo,out/'screen',on_cell_closed=closed)
        summary = worker.replay(out/'config.json',proof['config_sha256'],repo,out/'screen')
        assert summary['execution_gate_passed'] is True and summary['status'] in ('PASS','FAIL'), 'execution INVALID'
        assert len(uploaded) == 6, 'all valid closed cells checkpointed, including aborted positions'
        closure.update(closed=True,process_cleanup=True,replay_passed=True,execution_status='SUCCESS',scientific_status=summary['status'])
        failure['status'] = 'complete'; staging['closed'] = True
    except BaseException as error:
        failure.update(status='failed',error_type=type(error).__name__,error=str(error))
    finally:
        signal.signal(signal.SIGTERM,previous)
        try:
            if s3 is not None: s3.close()
            counters = dict(before=before,after=fixed.snapshot(),closed=True)
            fixed.check_cgroup(before,counters['after'],runtime().LIMITS,drained=True)
            assert not (out/'screen/scratch').exists(), 'runtime scratch/owner cleanup incomplete'
            controller_bound(out)
            staging['scratch_usage'] = science.scratch_usage(out)
            final_size = max(staging['scratch_usage'].values())
            staging['peak_scratch_bytes'] = max(staging['peak_scratch_bytes'],final_size)
            assert final_size <= SCRATCH
            write(out/'profile-cgroup.json',counters)
        except BaseException as error:
            closure.update(closed=False,process_cleanup=False,execution_status='FAIL',scientific_status='INVALID')
            failure.update(status='failed',cleanup_error_type=type(error).__name__,cleanup_error=str(error))
        closure['wall_seconds'] = time.monotonic()-started
        if not closure['closed']:
            staging['closed'] = False; closure['scientific_status'] = 'INVALID'
        write(out/'staging.json',staging); write(out/'offered-closure.json',closure); write(out/'failure.json',failure)
        fixed.driver.sync_directory(out)
    return closure


def poll(ec2,s3,prefix,instance_id,started):
    with patch.object(cold,'WALL',WALL):
        return cold.poll(ec2,s3,prefix,instance_id,started)


def validate_closed(out,proof,terminal,files):
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    assert 0 <= terminal['exit_code'] <= 255 and 0 <= terminal['original_exit_code'] <= 255
    complete = terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == 0
    assert terminal['status'] == ('complete' if complete else 'failed')
    if not complete:
        assert terminal['exit_code'] != 0
        if 'screen/records.jsonl' in files and (out/'screen/records.jsonl').exists():
            rows = [json.loads(r) for r in (out/'screen/records.jsonl').read_bytes().splitlines()]
            assert [(r['rate_index'],r['query_ordinal']) for r in rows] == [(i,q) for i in range(6) for q in range(64)]
        return False
    assert terminal['original_exit_code'] == 0 and set(files) == set(ARTIFACTS)
    assert sum(p['bytes'] for p in files.values()) <= OUTPUT_BYTES
    for name,key in (('config.json','config_sha256'),('controller-config.json','controller_config_sha256'),('asset-manifest.json','asset_manifest_sha256')):
        assert files[name]['sha256'] == proof[key], 'frozen JSON body drift'
    assert read_json(out/'source-qualification.json') == proof
    summary = runtime().replay(out/'config.json',proof['config_sha256'],Path.cwd(),out/'screen')
    assert summary['execution_gate_passed'] is True and summary['status'] in ('PASS','FAIL')
    closure = read_json(out/'offered-closure.json')
    assert closure['closed'] is closure['process_cleanup'] is closure['replay_passed'] is True
    assert closure['execution_status'] == 'SUCCESS' and closure['scientific_status'] == summary['status']
    assert closure['runtime_invocations'] == 1 and 0 <= closure['wall_seconds'] <= WORKER_SECONDS
    assert all(closure[k] == 0 for k in ('build_invocations','publication_invocations','scientific_scorer_invocations','oracle_invocations'))
    counters = read_json(out/'profile-cgroup.json'); assert counters['closed'] is True
    fixed.check_cgroup(counters['before'],counters['after'],runtime().LIMITS,drained=True)
    staging = read_json(out/'staging.json')
    assert staging['closed'] is staging['sealed'] is True
    assert staging['total_reservation_bytes'] == STATIC_BYTES and staging['controller_growth_reserve_bytes'] == OUTPUT_BYTES
    assert max(staging['whole_worker_static_usage'].values())+OUTPUT_BYTES <= STATIC_BYTES
    assert staging['mutable_controller_files'] == list(MUTABLE) and staging['peak_scratch_bytes'] <= SCRATCH
    assert max(staging['scratch_usage'].values()) <= SCRATCH
    boot = read_json(out/'bootstrap-staging.json')
    assert boot['source_authenticated'] is True and boot['source_archive_sha256'] == proof['source_archive_sha256']
    assert boot['scratch_limit_bytes'] == SCRATCH and boot['scratch_before_extract_bytes']+boot['source_repository_reserve_bytes'] <= SCRATCH
    versions = read_json(out/'tool-versions.json')
    assert versions['architecture'] == 'x86_64' and versions['python'].startswith('3.12.')
    assert (versions['os_release']['ID'],versions['os_release']['VERSION_ID']) == ('ubuntu','24.04')
    assert versions['thread_environment'] == dict.fromkeys(fixed.retained.THREAD_ENV,'2') and versions['aws_max_attempts'] == 1
    assert versions['sdk'] == boot['sdk']
    assert versions['sdk'] == dict(boto3=fixed.SDK_VERSION,botocore=fixed.SDK_VERSION,conditional_put=True,
        network_calls=0,python_executable=str(WORK_ROOT/'venv/bin/python'))
    timing = (out/'profile-resources.txt').read_text()
    assert 0 <= int(timing.split('Maximum resident set size (kbytes): ',1)[1].splitlines()[0])*1024 <= MEMORY
    assert int(timing.split('Exit status: ',1)[1].splitlines()[0]) == 0
    assert read_json(out/'failure.json')['status'] == 'complete'
    ledger = [json.loads(line) for line in (out/'controller-sdk-ledger.jsonl').read_bytes().splitlines()]
    assert [r['operation'] for r in ledger[:8]] == ['head_object','get_object']*4 and len(ledger) in (8,9)
    assert all(r['sdk_http_dispatch_attempts'] == 1 and r['retry_attempts'] == 0 and r['error'] is None for r in ledger)
    return True


def collect(s3,prefix,out,instance_id,commit,digest):
    # Shared collector enforces exact ACKed IDs terminated/waited before this fetch.
    def get_object(**kwargs):
        response = s3.get_object(**kwargs)
        try:
            metadata = response['ResponseMetadata']
            assert metadata['HTTPStatusCode'] == 200 and metadata.get('RetryAttempts',0) == 0
            if kwargs['Key'] == prefix+'/terminal.json':
                with response['Body'] as stream:
                    body = stream.read((1 << 20)+1)
                assert 0 < len(body) <= 1 << 20 and response['ContentLength'] == len(body)
                terminal = json.loads(body); files = terminal['artifacts']
                assert set(files) <= set(ARTIFACTS)
                for pin in files.values():
                    assert set(pin) == {'bytes','sha256'} and type(pin['bytes']) is int and 0 <= pin['bytes'] <= OUTPUT_BYTES
                    assert re.fullmatch('[0-9a-f]{64}',pin['sha256'])
                assert sum(pin['bytes'] for pin in files.values()) <= OUTPUT_BYTES, 'bounded WHOLE collection'
                response = dict(response,Body=io.BytesIO(body))
            return response
        except BaseException:
            response['Body'].close(); raise
    with patch.multiple(cold,ROOT=ROOT,PREFIX=PREFIX,SCHEMA=SCHEMA,ARTIFACTS=ARTIFACTS,
        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES,MAX_BODY_BYTES=OUTPUT_BYTES,
        DIAGNOSTIC_FILES=DIAGNOSTIC_FILES,preflight=preflight,validate_closed=validate_closed):
        return cold.collect(SimpleNamespace(get_object=get_object),prefix,out,instance_id,commit,digest)


@contextmanager
def lifecycle_adapter():
    with patch.multiple(canary,SCHEMA=SCHEMA,COMPUTE_CAP=COMPUTE_CAP,ALLOWANCE=ALLOWANCE):
        with canary.lifecycle_adapter() as shared:
            yield shared


def main(attempt):
    assert re.fullmatch('a[0-9]{4}',attempt)
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        proof = preflight(); stage_configs(proof)
        assert SPOT_MAX_USD_PER_HOUR*WALL/3600 <= COMPUTE_CAP
        with lifecycle_adapter() as shared:
            return shared.main(attempt,campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def staging_collection_check(work,repo,config,proof,worker,capability,rejects,thaw):
    """Tiny fixtures exercise the actual stage and shared stream collector."""
    from contextlib import ExitStack
    import copy
    import platform
    module = sys.modules[__name__]; shared,bootstrap = lifecycle()
    class SDK:
        def __init__(self,objects):
            self.objects,self.closed,self.calls,self.handlers = objects,False,[],{}
            self.meta = SimpleNamespace(events=SimpleNamespace(register=self.register,unregister=self.unregister))
        def register(self,event,fn,unique_id): self.handlers[unique_id] = (event,fn)
        def unregister(self,event,unique_id): self.handlers.pop(unique_id)
        def response(self,key):
            self.calls.append(key)
            for event,fn in list(self.handlers.values()):
                if event == 'before-send.s3': fn()
            body = self.objects[key]
            return dict(ContentLength=len(body),ResponseMetadata=dict(HTTPStatusCode=200,RetryAttempts=0))
        def head_object(self,Bucket,Key): return self.response(Key)
        def get_object(self,Bucket,Key): return dict(self.response(Key),Body=io.BytesIO(self.objects[Key]))
        def close(self): self.closed=True
    checks,bodies = 0,None
    worker_proof = dict(config_sha256=proof['config_sha256'],code_identity_sha256=proof['runtime_code_identity_sha256'],
        refs_identity_sha256=proof['refs_identity_sha256'],binary_sha256=proof['binary_sha256'])
    for mode in ('closed-FAIL','config-tamper','static-overflow','INVALID','cleanup-failure','callback-failure'):
        dest = work/('stage-'+mode); stage_repo = dest/'repo'; stage_repo.mkdir(parents=True)
        for pointer in config['cold_run']['files'].values():
            target = stage_repo/pointer['path']; write(target,(repo/pointer['path']).read_bytes())
        for name in ('cpu.txt','profile.log','run.log'): write(dest/name,b'explicit stage fixture\n')
        write(dest/'profile-resources.txt',b'Maximum resident set size (kbytes): 42\nExit status: 0\n')
        write(dest/'bootstrap-staging.json',dict(source_authenticated=True,source_archive_sha256=proof['source_archive_sha256'],
            scratch_limit_bytes=SCRATCH,scratch_before_extract_bytes=0,source_repository_reserve_bytes=4096,sdk=capability))
        objects = {config_key(sha(b)):b for b in (encoded(proof),(repo/CONFIG).read_bytes(),(repo/CONTROL).read_bytes(),(repo/MANIFEST).read_bytes())}
        if mode == 'config-tamper': objects[config_key(proof['config_sha256'])] += b'tamper'
        remote = SDK(objects)
        write(dest/'source-qualification.json',dict(**{k:proof[k] for k in (*TERMINAL_IDENTITIES,'source_archive_commit','source_archive_sha256')},proof_sha256=sha(encoded(proof))))
        summary = dict(status='INVALID' if mode == 'INVALID' else 'FAIL',execution_gate_passed=mode != 'INVALID')
        def fake_run(path,digest,base,output,*,on_cell_closed):
            assert base == stage_repo and not output.exists() and output == dest/'screen'
            assert stage_repo.stat().st_mode & 0o222 == 0 and remote.closed and not remote.handlers
            output.mkdir(); all_rows=[]
            for name in RUNTIME_FILES: write(output/name,b'closed explicit runtime fixture\n')
            for index in range(6):
                part = [dict(rate_index=index,query_ordinal=q,outcome='aborted') for q in range(64)]; all_rows.extend(part)
                records = output/f'rate{index}-records.jsonl'; write(records,b''.join(encoded(r)+b'\n' for r in part))
                marker = dict(worker_proof,rate_index=index,status=summary['status'],closed=True,cleanup_confirmed=True,
                    execution_gate_passed=summary['execution_gate_passed'],records=artifact(records))
                target = output/f'rate{index}-summary.json'; write(target,marker)
                if summary['execution_gate_passed']: on_cell_closed(marker,dict(records=records,summary=target))
            write(output/'records.jsonl',b''.join(encoded(r)+b'\n' for r in all_rows)); write(output/'summary.json',summary)
            if mode == 'cleanup-failure': (output/'scratch').mkdir()
            return 0 if summary['execution_gate_passed'] else 1
        def cli_call(command):
            if command[-1] == '--version': return ('aws-cli/'+AWSCLI_VERSION+' fixture').encode()
            if '--generate-cli-skeleton' in command: return b'{"IfNoneMatch":""}'
            if mode == 'callback-failure': raise RuntimeError('checkpoint upload failed')
            return b'{}'
        worker.main,worker.replay = fake_run,lambda *args:summary
        worker.qualify = lambda *args:(config,worker_proof,{})
        with ExitStack() as stack:
            stack.enter_context(patch.object(module,'runtime',return_value=worker)); stack.enter_context(patch.object(module,'WORK_ROOT',dest))
            stack.enter_context(patch.object(module,'qualify',return_value=proof))
            stack.enter_context(patch.object(fixed,'snapshot',return_value={})); stack.enter_context(patch.object(fixed,'check_cgroup'))
            stack.enter_context(patch.object(fixed,'sdk_guard',return_value=capability))
            stack.enter_context(patch.object(shared.boto3,'client',return_value=remote))
            stack.enter_context(patch.object(bootstrap,'_checkpoint_cli_call',side_effect=cli_call))
            stack.enter_context(patch.object(platform,'freedesktop_os_release',return_value=dict(ID='ubuntu',VERSION_ID='24.04')))
            stack.enter_context(patch.object(sys,'version','3.12.fixture (explicit remote interpreter fixture)'))
            stack.enter_context(patch.dict(os.environ,dict.fromkeys(fixed.retained.THREAD_ENV,'2')|dict(AWS_MAX_ATTEMPTS='1',
                BORSUK_OFFERED_SOURCE_COMMIT=proof['source_archive_commit'],BORSUK_OFFERED_ARCHIVE_SHA256=proof['source_archive_sha256'])))
            if mode == 'static-overflow': stack.enter_context(patch.object(module,'STATIC_BYTES',1))
            closed = stage(stage_repo,dest,PREFIX+'a0001')
        assert closed['closed'] is (mode == 'closed-FAIL'),(mode,read_json(dest/'failure.json'))
        assert remote.closed and not remote.handlers
        if mode == 'closed-FAIL':
            assert closed['scientific_status'] == 'FAIL' and closed['execution_status'] == 'SUCCESS'
            write(dest/'run-closed.log',(dest/'run.log').read_bytes()); bodies={name:(dest/name).read_bytes() for name in ARTIFACTS}
        else: assert closed['scientific_status'] == 'INVALID' and read_json(dest/'failure.json')['status'] == 'failed'
        thaw(dest); checks += 1
    dest = work/'collection/a0001'; dest.mkdir(parents=True)
    nodes = {'0':dict(instance_id='i-owned'),'1':dict(instance_id='i-extra')}
    launch = dict(nodes=nodes,instance_id='i-owned',prefix=PREFIX+'a0001',source_commit=proof['source_archive_commit'],source_archive_sha256=proof['source_archive_sha256'])
    reservation = dict(schema=SCHEMA,qualification=proof,config_sha256=proof['config_sha256'],source_commit=proof['source_archive_commit'],source_archive_sha256=proof['source_archive_sha256'])
    write(dest/'aws-launch.json',launch); write(dest/'aws-closeout.json',dict(nodes=nodes,state='terminated')); write(dest/'aws-reservation.json',reservation)
    terminal = dict(proof,schema=SCHEMA,instance_id='i-owned',source_commit=proof['source_archive_commit'],source_archive_sha256=proof['source_archive_sha256'],
        phase='complete',status='complete',exit_code=0,original_exit_code=0,artifacts={n:dict(bytes=len(b),sha256=sha(b)) for n,b in bodies.items()})
    objects = {PREFIX+'a0001/artifacts/'+n:b for n,b in bodies.items()}; terminal_key=PREFIX+'a0001/terminal.json'; objects[terminal_key]=encoded(terminal)
    worker.replay = lambda *args:dict(status='FAIL',execution_gate_passed=True)
    with patch.object(module,'ROOT',dest.parent),patch.object(module,'preflight',return_value=proof),\
        patch.object(module,'runtime',return_value=worker),patch.object(fixed,'check_cgroup'):
        collect(SDK(objects),PREFIX+'a0001',dest,'i-owned',proof['source_archive_commit'],proof['source_archive_sha256'])
        receipt = read_json(dest/'collection-receipt.json'); assert receipt['whole_body_verification'] is receipt['complete'] is True and receipt['scientific_status'] == 'FAIL'; checks += 1
        for field,value in (('instance_id','i-foreign'),('exit_code',True),('source_commit','0'*40),('code_identity_sha256','0'*64)):
            objects[terminal_key]=encoded(dict(terminal,**{field:value}))
            rejects(lambda:collect(SDK(objects),PREFIX+'a0001',dest,'i-owned',proof['source_archive_commit'],proof['source_archive_sha256']))
        objects[terminal_key]=encoded(terminal)
        key=PREFIX+'a0001/artifacts/screen/records.jsonl'; original=objects[key]; objects[key]+=b'tamper'
        rejects(lambda:collect(SDK(objects),PREFIX+'a0001',dest,'i-owned',proof['source_archive_commit'],proof['source_archive_sha256'])); objects[key]=original
        for closeout in (dict(nodes={'0':nodes['0']},state='terminated'),dict(nodes=nodes,state='running')):
            write(dest/'aws-closeout.json',closeout); remote=SDK(objects)
            rejects(lambda:collect(remote,PREFIX+'a0001',dest,'i-owned',proof['source_archive_commit'],proof['source_archive_sha256'])); assert not remote.calls
        write(dest/'aws-closeout.json',dict(nodes=nodes,state='terminated'))
        bad=copy.deepcopy(terminal); bad['artifacts']['screen/records.jsonl']['bytes']=OUTPUT_BYTES; objects[terminal_key]=encoded(bad)
        rejects(lambda:collect(SDK(objects),PREFIX+'a0001',dest,'i-owned',proof['source_archive_commit'],proof['source_archive_sha256']))
    return checks


def self_check():
    """Local controller seams, with explicit temporary runtime and SDK doubles."""
    import copy
    from contextlib import ExitStack, redirect_stdout
    from datetime import datetime, timezone
    import resource
    import shutil
    import tempfile
    from unittest.mock import Mock
    resource.setrlimit(resource.RLIMIT_AS,(256 << 20,256 << 20)); signal.alarm(55)
    started, checks = time.monotonic(), 0
    module, origin = sys.modules[__name__], Path(__file__).resolve().parents[1]
    shared, bootstrap = lifecycle()
    def rejects(call):
        nonlocal checks
        try: call()
        except (AssertionError,ValueError,KeyError,OSError,RuntimeError,subprocess.CalledProcessError): checks += 1; return
        raise AssertionError('negative controller check admitted')
    def thaw(root):
        for parent, dirs, files in os.walk(root,followlinks=False):
            Path(parent).chmod(0o700)
            for name in files:
                path = Path(parent)/name
                if not path.is_symlink(): path.chmod(0o600)
    worker_code = tuple(sorted(set((*cold.CODE,*offered.CODE,RUNTIME))))
    limits = {k:v for k,v in fixed.HOST.items() if k != 'publisher_memory_bytes'}
    limits.update(driver_reserve_bytes=2 << 30,output_reserve_bytes=OUTPUT_BYTES,static_repository_reserve_bytes=STATIC_BYTES)
    worker = SimpleNamespace(CODE=worker_code,OUTPUTS=RUNTIME_FILES,UNUSED=UNUSED,LIMITS=limits,
        FIXED=dict(schema='EXPLICIT-TEMPORARY-RUNTIME-FIXTURE',offered_qps=offered.RATES,workers=5))
    worker.identity = lambda p:{k:p[k] for k in ('bytes','sha256')}
    worker.read = lambda repo,pointer,**kw:(repo/pointer['path']).read_bytes()
    worker.scratch_usage = lambda repo,**kw:science.scratch_usage(repo)
    capability = dict(boto3=fixed.SDK_VERSION,botocore=fixed.SDK_VERSION,conditional_put=True,network_calls=0,
        python_executable=str(WORK_ROOT/'venv/bin/python'))
    with tempfile.TemporaryDirectory(prefix='offered-controller-fixture-') as tmp:
        work = Path(tmp); repo = work/'repo'; repo.mkdir()
        for name in CODE:
            target = repo/name; target.parent.mkdir(parents=True,exist_ok=True)
            if name != RUNTIME: shutil.copyfile(origin/name,target)
        from scripts import prepare_cohere_top32_coverage as imported_authority
        pointer = imported_authority.FIXED['quality_config']
        name = pointer.get('archived_path',pointer['path'])
        target = repo/name; target.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(origin/name,target)
        name = str(fixed.library.quality.panel.AUTHORITY)
        target = repo/name; target.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(origin/name,target)
        stub = '"""Explicit temporary runtime fixture; never science."""\nimport json\n'
        for name,value in (('CODE',worker_code),('FIXED',worker.FIXED),('LIMITS',limits)):
            stub += name+'=json.loads('+repr(json.dumps(value))+')\n'
        (repo/RUNTIME).write_text(stub)
        source = dict(commit='a'*40,archive_sha256='b'*64)
        binary = b'explicit HTTP fixture; never executable\n'
        pointer = dict(path='fixture/http',bytes=len(binary),sha256=sha(binary))
        entry = dict(bytes=len(binary),sha256=sha(binary),source=dict(bucket=BUCKET,key='fixture/http'))
        write(repo/pointer['path'],binary)
        archived = repo/'fixture/manifest.json'; write(archived,dict(assets={'qualification/binaries/two_bit_http':entry}))
        historical = {HTTP:pointer,'asset-manifest.json':dict(path='fixture/manifest.json',**artifact(archived))}
        for name in UNUSED:
            target = repo/'fixture'/Path(name).name; write(target,b'unused fixture')
            historical[name] = dict(path=str(target.relative_to(repo)),**artifact(target))
        config = dict(worker.FIXED,bucket=BUCKET,region=REGION,namespace_prefix='retained/a5',execution_source=source,
            worker_limit_seconds=WORKER_SECONDS,service_limit_seconds=SERVICE_SECONDS,machine_limit_seconds=WALL,
            cleanup_reserve_seconds=CLEANUP_SECONDS,resources=limits,cold_run=dict(files=historical),
            code_sha256={n:artifact(repo/n)['sha256'] for n in worker_code})
        worker_proof = dict(code_identity_sha256=sha(encoded(config['code_sha256'])),refs_identity_sha256='c'*64,
            binary_sha256=sha(binary),native_source_identity_sha256=cold.driver.SOURCE_ID)
        def fixture_qualify(path,digest,base):
            assert artifact(Path(path))['sha256'] == digest
            value = read_json(path)
            assert value['code_sha256'] == {n:artifact(base/n)['sha256'] for n in worker_code}
            return value,dict(worker_proof,config_sha256=digest),dict(binary=pointer)
        worker.qualify = fixture_qualify
        manifest = dict(schema=ASSET_SCHEMA,authority_pending=False,assets={HTTP:entry})
        write(repo/CONFIG,config); write(repo/MANIFEST,manifest)
        control = dict(schema=SCHEMA,authority_pending=False,execution_source=source,
            code_sha256={n:artifact(repo/n)['sha256'] for n in CODE},
            runtime_config=dict(path=str(CONFIG),**artifact(repo/CONFIG)),asset_manifest=dict(path=str(MANIFEST),**artifact(repo/MANIFEST)))
        write(repo/CONTROL,control)
        with patch.object(module,'runtime',return_value=worker):
            proof = qualify(repo); assert code_closure(repo) == CODE; checks += 1
            rejects(lambda:qualify(repo,config_sha='0'*64))
            for key,value in (('authority_pending',True),('execution_source',{}),('code_sha256',{})):
                write(repo/CONTROL,dict(control,**{key:value})); rejects(lambda:qualify(repo))
            write(repo/CONTROL,control)
            for name in (OWN,RUNTIME):
                target = repo/name; original = target.read_bytes(); target.write_bytes(original+b'\n# tamper\n')
                rejects(lambda:qualify(repo)); target.write_bytes(original)
            changed = copy.deepcopy(manifest); changed['assets']['sq8.bin'] = entry; write(repo/MANIFEST,changed)
            write(repo/CONTROL,dict(control,asset_manifest=dict(path=str(MANIFEST),**artifact(repo/MANIFEST))))
            rejects(lambda:qualify(repo)); write(repo/MANIFEST,manifest); write(repo/CONTROL,control)
        # Fresh default CLI must qualify before any runtime import warms its CODE.
        # Only historical evidence and the post-preflight cloud boundary are doubles.
        cli_repo = work/'cli-repo'; shutil.copytree(repo,cli_repo)
        real_worker = runtime()
        (cli_repo/RUNTIME).write_text((origin/RUNTIME).read_text()+
            '\n# Explicit temporary historical fixture; never science.\n'+
            'historical_inputs = lambda repo, config: '+repr(dict(binary=pointer))+'\n')
        controller = (cli_repo/OWN).read_text()
        controller = controller.replace("\nif __name__ == '__main__':",
            "\ndef stage_configs(proof):\n    print(json.dumps(proof,sort_keys=True))\n    raise SystemExit(0)\n\nif __name__ == '__main__':",1)
        (cli_repo/OWN).write_text(controller)
        for name in (CONFIG,CONTROL,MANIFEST): (cli_repo/name).unlink()
        prices = cli_repo/'fixture/prices.json'; write(prices,dict(explicit_temporary_fixture=True))
        def git(*args):
            return subprocess.check_output(['git','-c','core.hooksPath=/dev/null',*args],cwd=cli_repo,stderr=subprocess.PIPE)
        git('init','--initial-branch=master'); git('add','.'); git('commit','-m','Temporary CLI source fixture')
        cli_source = dict(commit=git('rev-parse','HEAD').decode().strip())
        cli_source['archive_sha256'] = cold.archive_digest(cli_source['commit'],cli_repo)
        cli_config = dict(real_worker.FIXED,bucket=BUCKET,namespace_prefix=config['namespace_prefix'],
            code_sha256={n:artifact(cli_repo/n)['sha256'] for n in worker_code},execution_source=cli_source,
            cold_run=config['cold_run'],cold_source_authority={},resources=real_worker.LIMITS,
            prices=dict(path='fixture/prices.json',**artifact(prices)))
        write(cli_repo/CONFIG,cli_config); write(cli_repo/MANIFEST,manifest)
        write(cli_repo/CONTROL,dict(control,execution_source=cli_source,
            code_sha256={n:artifact(cli_repo/n)['sha256'] for n in CODE},
            runtime_config=dict(path=str(CONFIG),**artifact(cli_repo/CONFIG))))
        git('add','.'); git('commit','-m','Temporary CLI authority fixture')
        git('update-ref','refs/remotes/origin/main','HEAD')
        result = subprocess.run([sys.executable,'-B','-m',MODULE,'a0001'],cwd=cli_repo,
            capture_output=True,timeout=10,env=dict(os.environ,PYTHONPATH=str(cli_repo)))
        assert result.returncode == 0,('fresh default CLI preflight',result.stderr)
        assert json.loads(result.stdout)['runtime_code_identity_sha256'] == sha(encoded(cli_config['code_sha256']))
        checks += 1
        for args,expected in (([],2),(['--stage'],2),(['--contract'],0)):
            result = subprocess.run([sys.executable,'-B','-m',MODULE,*args],cwd=repo,capture_output=True,timeout=10,
                env=dict(os.environ,PYTHONPATH=str(repo)))
            assert result.returncode == expected,(args,result.stderr)
            if expected == 0: assert json.loads(result.stdout)['runtime_fixed']['schema'] == worker.FIXED['schema']
            checks += 1
        with patch.object(module,'CONFIG',repo/CONFIG):
            body = user_data(source['commit'],source['archive_sha256'],'fixture/source.tar.gz',PREFIX+'a0001',proof)
        assert all(x in body for x in ('--on-active=1800s','MemoryMax=12884901888','MemorySwapMax=0','CPUQuota=200%',
            'TasksMax=512','RuntimeMaxSec=1500','--kill-after=30 1440','taskset -c 4-5'))
        assert body.index('rm -f "$root/source.tar.gz"') < body.index('phase=cold\n')
        finish = body[body.index('finish() {\n'):body.index('trap finish EXIT\n')]
        for mode in ('PASS','FAIL','INVALID','upload-failure'):
            dest = work/('shell-'+mode); dest.mkdir()
            for name in ARTIFACTS: write(dest/name,b'closed fixture\n')
            write(dest/'source-qualification.json',proof); write(dest/'run.log',b'closed log\n')
            write(dest/'screen/summary.json',dict(status=mode))
            cli = dest/'bin/aws'; cli.parent.mkdir()
            cli.write_text('''#!/bin/bash
root="${0%/bin/aws}"
[[ "$1 $2" == 's3api put-object' ]] || exit 40
while [ "$#" -gt 0 ]; do
 case "$1" in --body) file="$2"; shift;; --key) key="$2"; shift;; --if-none-match) condition="$2"; shift;; esac
 shift
done
test -f "$file" && test "$condition" = '*' || exit 41
printf '%s\\n' "$key" >> "$root/uploads"
if [[ "$MODE" = upload-failure && "$key" = */artifacts/* ]]; then exit 55; fi
'''); cli.chmod(0o700)
            stubs = '''curl() { case "$*" in */api/token*) echo token;; *) echo i-owned;; esac; }
shutdown() { :; }
systemd-run() { while [ "$1" != timeout ]; do shift; done; "$@"; }
'''
            script = stubs+'root='+shlex.quote(str(dest))+'; phase='+('cold' if mode == 'INVALID' else 'complete')+'; export MODE='+mode+'; export ARTIFACT_NAMES='+shlex.quote(' '.join(ARTIFACTS))+'; cd "$root"\n'
            script += finish.replace('/dev/ttyS0',str(dest/'serial'))+'\n(exit '+('7' if mode == 'INVALID' else '0')+'); finish\n'
            result = subprocess.run(['bash','-c',script],capture_output=True,timeout=10,env=dict(os.environ,PATH=str(cli.parent)+':/usr/bin:/bin'))
            terminal = read_json(dest/'terminal.json'); expected = 7 if mode == 'INVALID' else 96 if mode == 'upload-failure' else 0
            assert terminal['exit_code'] == result.returncode == expected,(mode,result.stderr)
            assert terminal['original_exit_code'] == (7 if mode == 'INVALID' else 0)
            assert read_json(dest/'screen/summary.json')['status'] == mode
            assert len((dest/'uploads').read_text().splitlines()) == len(ARTIFACTS)+1; checks += 1
        cell = work/'cell'; cell.mkdir(); rows = [dict(query_ordinal=q,rate_index=0,outcome='aborted') for q in range(64)]
        write(cell/'rate0-records.jsonl',b''.join(encoded(r)+b'\n' for r in rows))
        cell_proof = dict(worker_proof,config_sha256=proof['config_sha256'])
        marker = dict(cell_proof,rate_index=0,status='FAIL',closed=True,cleanup_confirmed=True,execution_gate_passed=True,records=artifact(cell/'rate0-records.jsonl'))
        write(cell/'rate0-summary.json',marker); paths = dict(records=cell/'rate0-records.jsonl',summary=cell/'rate0-summary.json')
        calls, uploaded = [],set()
        checkpoint(marker,paths,dict(config,measurement_prefix=PREFIX+'a0001'),cell_proof,cell,uploaded,calls.append)
        assert uploaded == {0} and [c[c.index('--key')+1] for c in calls] == [PREFIX+'a0001/cells/rate0-records.jsonl',PREFIX+'a0001/cells/rate0-summary.json']; checks += 1
        rejects(lambda:checkpoint(marker,paths,config,cell_proof,cell,uploaded,calls.append))
        for key,value in (('status','INVALID'),('closed',False),('cleanup_confirmed',False),('execution_gate_passed',False),('config_sha256','0'*64),('rate_index',1)):
            bad = dict(marker,**{key:value}); write(paths['summary'],bad)
            rejects(lambda:checkpoint(bad,paths,config,cell_proof,cell,set(),calls.append))
        write(paths['summary'],marker); original = paths['records'].read_bytes(); paths['records'].write_bytes(original+b'tamper')
        rejects(lambda:checkpoint(marker,paths,config,cell_proof,cell,set(),calls.append)); paths['records'].write_bytes(original)
        with patch.object(cold,'config_key',config_key): assert cold.stage_configs_check()['scenarios'] == 14
        checks += 1
        for mode in ('success','multi-ack-fsync','interrupt','interruption','wait'):
            ec2,s3,session = Mock(),Mock(),Mock(); session.client.side_effect = [ec2,s3]
            client_calls = session.client
            ec2.describe_instances.return_value = {'Reservations':[]}; ec2.describe_subnets.return_value = {'Subnets':[{'AvailabilityZone':'fixture-az'}]}
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory':[{'SpotPrice':'.1','Timestamp':datetime.now(timezone.utc)}]}
            acked = ['i-owned','i-extra'] if mode.startswith('multi') else ['i-owned']
            launch_call = ec2.run_instances; launch_call.return_value = {'Instances':[{'InstanceId':n} for n in acked]}
            events = []; ec2.terminate_instances.side_effect = lambda **kw:events.append('terminate')
            def waited(**kw):
                events.append('wait')
                if mode == 'wait': raise RuntimeError('wait failed')
            ec2.get_waiter.return_value.wait.side_effect = waited
            def collected(*args):
                assert events == ['terminate','wait']; events.append('collect')
                return dict(status='complete',phase='complete',exit_code=0,artifacts=dict.fromkeys(ARTIFACTS))
            with ExitStack() as stack:
                for name,value in (('ROOT',work/('launch-'+mode)),('preflight',Mock(return_value=proof)),('stage_configs',Mock()),
                    ('user_data',Mock(return_value='fixture')),('collect',Mock(side_effect=collected)),
                    ('poll',Mock(side_effect={'interrupt':KeyboardInterrupt(),'interruption':RuntimeError('Spot interruption')}.get(mode)))):
                    stack.enter_context(patch.object(module,name,value))
                stack.enter_context(patch.object(shared.boto3,'Session',return_value=session))
                stack.enter_context(patch.object(subprocess,'check_output',side_effect=['','a'*40,b'fixture archive']))
                stack.enter_context(patch.object(subprocess,'run')); stack.enter_context(patch.object(shared.peer,'missing',return_value=True))
                stack.enter_context(patch.object(shared.peer,'put_if_absent'))
                stack.enter_context(patch.object(os,'fsync',side_effect=OSError('fsync failed') if mode.endswith('fsync') else None))
                stack.enter_context(redirect_stdout(io.StringIO()))
                try: main('a0001')
                except (OSError,RuntimeError,KeyboardInterrupt): assert mode != 'success'
                else: assert mode == 'success'
            launch_call.assert_called_once(); ec2.terminate_instances.assert_called_once_with(InstanceIds=acked)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=acked)
            assert events == (['terminate','wait'] if mode == 'wait' else ['terminate','wait','collect'])
            params = launch_call.call_args.kwargs
            assert params['MetadataOptions'] == dict(HttpTokens='required',HttpEndpoint='enabled',HttpPutResponseHopLimit=1)
            assert params['InstanceType'] == INSTANCE_TYPE and params['ImageId'] == IMAGE_ID
            assert params['BlockDeviceMappings'] == [dict(DeviceName='/dev/sda1',Ebs=dict(DeleteOnTermination=True,Encrypted=True,VolumeSize=80,VolumeType='gp3'))]
            reservation = read_json(work/('launch-'+mode)/'a0001/aws-reservation.json')
            assert reservation['wall_seconds'] == WALL and reservation['compute_cap_usd'] == COMPUTE_CAP and reservation['ebs_s3_allowance_usd'] == ALLOWANCE
            assert all(c.kwargs['config'].retries['total_max_attempts'] == 1 for c in client_calls.call_args_list); checks += 1
        # Further stage/collection fixtures share these frozen authorities.
        checks += staging_collection_check(work,repo,config,proof,worker,capability,rejects,thaw)
        thaw(work)
    signal.alarm(0)
    return dict(passed=True,checks=checks,source_sha256=artifact(Path(__file__))['sha256'],
        elapsed_seconds=time.monotonic()-started,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        runtime_fixture='explicit temporary stub; actual combined integration remains parent-owned',
        fresh_cli_preflight=True,real_runtime_integration=False,native_or_cloud_execution=False,
        launch_authorized=False,user_data_bytes=len(body.encode()))


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['--contract']:
            print(json.dumps(contract(),sort_keys=True))
        elif sys.argv[1:] == ['--self-check']:
            print(json.dumps(self_check(),sort_keys=True))
        elif sys.argv[1:2] == ['--stage']:
            assert len(sys.argv) == 5, '--stage REPO WORKER_ROOT PREFIX'
            sys.exit(0 if stage(Path(sys.argv[2]),Path(sys.argv[3]),sys.argv[4])['closed'] else 2)
        else:
            assert len(sys.argv) == 2, 'usage: aNNNN | --contract | --self-check'
            with open('/tmp/borsuk-fixed48-offered-http-spot-launch.lock','a+') as lock:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB); main(sys.argv[1])
    except (Exception,KeyboardInterrupt) as error:
        print(type(error).__name__+': '+str(error),file=sys.stderr); sys.exit(2)
