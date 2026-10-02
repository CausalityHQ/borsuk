#!/usr/bin/env python3
"""One disposable offered infrastructure gate; parent owns freeze and cloud.

--contract | --self-check | --stage REPO WORKER_ROOT PREFIX | aNNNN
All experiment entry points stay unused. Shared helpers own ACKs and teardown.
"""
from contextlib import contextmanager
import fcntl
import importlib
import inspect
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
sys.dont_write_bytecode = True
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import launch_cohere_fixed48_canary_spot as canary
from scripts import run_cohere_fixed48_offered_http as runtime

cold, fixed, science = canary.cold, canary.runtime, canary.science
encoded, sha, artifact, write = cold.encoded, cold.sha, cold.artifact, cold.write
regular_path, read_json, lifecycle = cold.regular_path, cold.read_json, cold.lifecycle
OWN = 'scripts/launch_cohere_fixed48_offered_canary_spot.py'
CONTROLLER = 'scripts/launch_cohere_fixed48_offered_http_spot.py'
MODULE = OWN[:-3].replace('/', '.')
ROOT = fixed.BASE / 'offered-canary'
CONFIG, NAME = ROOT / 'config.json', ''
QUALIFICATION, HEAD_ROSTER = ROOT/'offered-qualification.json',ROOT/'asset-head-roster.json'
VERIFICATION = fixed.BASE / 'offered-runtime-verification/root-verification.json'
CONTROLLER_VERIFICATION = fixed.BASE / 'offered-controller-verification/root-verification.json'
SCHEMA = 'borsuk-fixed48-offered-disposable-canary-v1'
PREFIX, TOKEN_PREFIX, TAG = ('research/semantic-router/20261002/fixed48-offered-canary-',
    'fixed48-offered-canary-', 'borsuk-fixed48-offered-canary')
WORK_ROOT = Path('/mnt/cohere-fixed48-offered-canary')
WALL, SERVICE_SECONDS, WORKER_SECONDS = 480, 180, 150
MEMORY, SCRATCH = 2 << 30, 4 << 30
REGION, BUCKET = cold.REGION, cold.BUCKET
INSTANCE_TYPE, IMAGE_ID, ROOT_DEVICE_NAME, SUBNET = cold.INSTANCE_TYPE, cold.IMAGE_ID, cold.ROOT_DEVICE_NAME, cold.SUBNET
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP, ALLOWANCE = .50, .12, .05
SDK_VERSION = canary.SDK_VERSION
LIMITS = dict(machine_limit_seconds=WALL, service_limit_seconds=SERVICE_SECONDS,
    worker_limit_seconds=WORKER_SECONDS, shared_memory_bytes=MEMORY, swap_bytes=0,
    cpu_quota_percent=200, tasks_max=512, threads=2, scratch_bytes=SCRATCH,
    requests_max=128, small_body_bytes=1 << 20, compute_cap_usd=COMPUTE_CAP,
    ebs_s3_allowance_usd=ALLOWANCE)
CODE = tuple(sorted(set((*runtime.CODE, *canary.CODE, CONTROLLER, OWN))))
FIELDS = {'schema', 'authority_pending', 'execution_source', 'code_sha256', 'resources',
    'runtime_config', 'controller_config', 'asset_manifest', 'runtime_verification', 'offered_qualification', 'head_roster'}
ARTIFACTS = ('config.json', 'source-qualification.json', 'bootstrap-staging.json',
    'bootstrap-sdk-ledger.jsonl', 'sdk-ledger.jsonl', 'asset-heads.json', 'small-body.json',
    'imports.json', 'cli.json', 'cli.log', 'resources.json', 'cleanup.json', 'summary.json',
    'failure.json', 'profile.log', 'profile-resources.txt', 'eligibility.json',
    'offered-config.json', 'offered-controller-config.json', 'offered-asset-manifest.json',
    'offered-qualification.json', 'asset-head-roster.json')
IDENTITIES = ('config_sha256', 'code_identity_sha256', 'asset_manifest_sha256', 'artifact_roster_sha256',
    'offered_config_sha256', 'offered_controller_config_sha256', 'runtime_code_identity_sha256',
    'runtime_verification_sha256', 'head_roster_sha256', 'native_source_identity_sha256','offered_qualification_sha256')
AUTHORITY_FILES = (('runtime_config','offered-config.json'),('controller_config','offered-controller-config.json'),
    ('asset_manifest','offered-asset-manifest.json'),('offered_qualification','offered-qualification.json'),('head_roster','asset-head-roster.json'))


def controller():
    return importlib.import_module(CONTROLLER[:-3].replace('/', '.'))


def contract():
    return dict(schema=SCHEMA+'-contract', freeze_owner='parent', root=str(ROOT), config=str(CONFIG),
        config_schema=SCHEMA, config_fields=sorted(FIELDS), CODE=list(CODE), ARTIFACTS=list(ARTIFACTS),
        resources=LIMITS, sdk=dict(boto3=SDK_VERSION, botocore=SDK_VERSION, required='PutObject.IfNoneMatch'),
        offered_authorities={n:str(runtime.ROOT / p) for n,p in
            (('runtime_config','config.json'), ('controller_config','controller-config.json'), ('asset_manifest','asset-manifest.json'))},
        runtime_verification=str(VERIFICATION),controller_verification=str(CONTROLLER_VERIFICATION), pointer='exact path/bytes/sha256; each body <=1MiB',
        offered_qualification=dict(path=str(QUALIFICATION),bytes='<=1MiB',sha256='SHA of exact encoded(controller.qualify(...)); binds OFFERED config, never own canary config'),
        head_roster=dict(path=str(HEAD_ROSTER),schema=SCHEMA+'-heads',bytes='<=1MiB',sha256='SHA of parent-frozen metadata roster derived by prepare_authorities'),
        execution_source='same NEW archive/commit as offered authorities; separate from authenticated historical a5 bridge',
        source_freeze='All six JSON authorities absent from executor archive and committed afterwards. Detached offered qualification and roster bind offered source/config before own canary freeze.',
        head_roster_description='Derived only: retained remote38 + offered three authorities/qualification + canary config/roster + retained serving head/startup/canonical/SQ8/leaves.',
        small_body='ONE selected asset GET: authenticated retained serving generations/ROOT_SHA/manifest.json, <=1MiB; separate small authority bootstrap GETs.',
        staging='Source archive contains historical bodies/HTTP. Hydrate offered three JSONs + detached root qualification + HEAD roster. Metadata-only qualify verifies hashes/native399/completed root certificates. NO ctl.qualify/runtime.qualify/historical_inputs/reducer/GT remotely.',
        result='terminal INFRA_GO / FAIL only; scientific_status UNMEASURED, no performance claim',
        prefix=PREFIX+'aNNNN', instance_type=INSTANCE_TYPE, region=REGION, image_id=IMAGE_ID,
        root_device_name=ROOT_DEVICE_NAME, root_volume=dict(gib=80,type='gp3',encrypted=True,delete_on_termination=True),
        metadata='IMDSv2 required', spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR, fresh_quote_max_age_seconds=300,
        cli='--contract | --prepare-authorities (LOCAL root only) | --self-check | --stage REPO WORKER_ROOT PREFIX | aNNNN',
        actual_integration='requires final controller in combined source; root freezes/launches after its real-fixture cheap gate',
        native_calls=0, offered_main_calls=0, publication_calls=0, performance_measured=False, launch_authorized=False)


def root_gate(repo, pointer):
    """Authenticate the completed real-fixture receipt against CURRENT executor."""
    receipt = json.loads(science.read_repo(repo,pointer,VERIFICATION))
    assert receipt['schema'] == 'borsuk-fixed48-offered-runtime-root-verification-v1'
    assert receipt['actual_fixture_passed'] is receipt['qualified'] is True and receipt['exit_status'] == 0
    assert all(receipt[k] is False for k in ('ann_measurement','cloud_execution','native_execution','full_rust_suite_reexecuted'))
    assert receipt['source'] == dict(path=runtime.OWN,**artifact(repo/runtime.OWN)), 'current real fixture source drift'
    pins = [receipt['contract'], receipt['logs']['root'], receipt['logs']['resources'], *receipt['logs']['worker'].values()]
    for p in pins: science.read_repo(repo,p)
    authority = json.loads(science.read_repo(repo,receipt['contract']))
    assert tuple(authority['CODE']) == runtime.CODE
    assert authority['code_sha256'] == {n:artifact(repo/n)['sha256'] for n in runtime.CODE}, 'completed executor closure drift'
    cp = dict(path=str(CONTROLLER_VERIFICATION),**artifact(repo/CONTROLLER_VERIFICATION))
    control = json.loads(science.read_repo(repo,cp))
    assert control['schema'] == 'borsuk-fixed48-offered-controller-root-verification-v1'
    assert control['qualified'] is True and control['native_or_cloud_execution'] is control['launch_authorized'] is False
    assert control['authenticated_requests'] == 64 and control['native_source_files'] == 399
    assert control['source'] == dict(path=CONTROLLER,**artifact(repo/CONTROLLER)), 'current controller receipt drift'
    assert len(control['checks']) == 2 and all(c['exit_status'] == 0 for c in control['checks'])
    for p in control['artifacts'].values(): science.read_repo(repo,p)
    return {p['path']:dict(bytes=p['bytes'],sha256=p['sha256']) for p in [pointer,*pins,cp,*control['artifacts'].values()]}


def config_key(digest):
    assert re.fullmatch('[0-9a-f]{64}',digest)
    return 'research/semantic-router/20261002/fixed48-offered-canary-configs/'+digest+'.json'


def head_roster(repo, config, offered_config, offered_proof, evidence, manifest):
    ctl = controller()
    archived = json.loads(runtime.read(repo,offered_config['cold_run']['files']['asset-manifest.json']))
    assets, bridges = {}, {}
    def add(name, pin, source):
        cold.driver.safe_key(name)
        assert type(pin['bytes']) is int and pin['bytes'] > 0 and re.fullmatch('[0-9a-f]{64}',pin['sha256'])
        assert set(source) <= {'bucket','key','etag','version_id'} and {'bucket','key'} <= set(source)
        assert source['bucket'] == BUCKET; cold.driver.safe_key(source['key'])
        key = source['bucket'],source['key']
        assert key not in {(a['source']['bucket'],a['source']['key']) for a in assets.values()}, 'duplicate frozen key'
        assets[name] = dict(bytes=pin['bytes'],sha256=pin['sha256'],source=source)
    for name, entry in archived['assets'].items():
        source = entry['source']
        if 'repo_path' in source:
            p = dict(path=source['repo_path'],**runtime.identity(entry))
            science.read_repo(repo,p); bridges[name] = p
        else: add('retained/'+name,entry,source)
    assert len(bridges) == 5 and len(assets) == 33, 'retained full38 seam'
    assert manifest['assets'] == offered_proof['assets']
    assert manifest['assets'][ctl.HTTP] == archived['assets']['qualification/binaries/two_bit_http']
    for field in ('runtime_config','controller_config','asset_manifest'):
        pin = config[field]; add('offered/'+field,pin,dict(bucket=BUCKET,key=ctl.config_key(pin['sha256'])))
    body = encoded(offered_proof)
    add('offered/qualification',dict(bytes=len(body),sha256=sha(body)),dict(bucket=BUCKET,key=ctl.config_key(sha(body))))
    publication, arm, namespace = evidence['publication'],evidence['arm'],offered_config['namespace_prefix']
    root_sha = arm['authority']['root_sha256']
    add('serving/head.json',arm['head_file'],dict(bucket=BUCKET,key=namespace+'/head.json'))
    for name in cold.library.STARTUP:
        add('serving/'+name,dict(bytes=arm['metadata_files'][name],sha256=arm['metadata_sha256'][name]),
            dict(bucket=BUCKET,key=namespace+'/generations/'+root_sha+'/'+name))
    for name in ('canonical','sq8'):
        pin = publication[name]; add('serving/'+name,pin,dict(bucket=BUCKET,key=pin['key'],etag=pin['etag']))
    pin = arm['leaf_object']
    add('serving/router/leaves.bin',pin,dict(bucket=BUCKET,key=namespace+'/objects/'+pin['sha256']))
    small = 'serving/manifest.json'
    body = encoded(publication['manifest'])+b'\n'
    assert runtime.identity(assets[small]) == dict(bytes=len(body),sha256=sha(body)) and len(body) <= LIMITS['small_body_bytes']
    return assets,bridges,small


def metadata_evidence(repo, config):
    """Only closed authority envelopes and native source hashes; no panel bodies."""
    files = config['cold_run']['files']
    assert config['cold_run']['directory'] == str(runtime.COLD) and set(files) == set(runtime.COLD_ROSTER)
    for name,pin in files.items(): assert pin['path'] == str(runtime.COLD/name)
    get = lambda name:json.loads(runtime.read(repo,files[name]))
    bridge = json.loads(runtime.read(repo,config['cold_source_authority']))
    terminal,audit = get('aws-terminal.json'),get('root-audit.json')
    assert bridge['whole_body_verification'] is True and bridge['original_controller_exit_status'] == 0
    assert bridge['terminal'] == files['aws-terminal.json'] and bridge['root_audit'] == files['root-audit.json']
    assert bridge['authenticated_artifacts'] == terminal['artifacts']
    assert terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == terminal['original_exit_code'] == 0
    assert audit['all_declared_artifacts_authenticated'] is audit['execution_gate_passed'] is audit['quality_gate_passed'] is audit['cleanup_proven'] is True
    assert audit['independently_observed_instance_state'] == 'terminated'
    native = get('native/source-qualification.json')
    source = cold.driver.qualification.worker.source_hashes(repo)
    assert len(source) == 399 and source == native['source_sha256'], 'current native source drift'
    assert cold.driver.qualification.worker.source_identity(source) == bridge['native_source_identity_sha256'] == cold.driver.SOURCE_ID
    binary = files['native/binaries/two_bit_http']
    assert artifact(regular_path(repo/binary['path'])) == runtime.identity(binary), 'HTTP body identity'
    abi = get('runtime-abi.json')
    assert abi['qualified'] is True and abi['architecture'] == 'x86_64' and abi['libc'] == 'glibc'
    assert abi['binaries']['two_bit_http'] == runtime.identity(binary), 'qualified HTTP ABI binding'
    publication = get('screen/publication.json')
    assert publication['retained_for_offered_gate'] is publication['publication_via_production_library'] is True
    assert publication['process']['exit_status'] == 0 and publication['process']['process_cleanup'] is True
    arm = publication['arm']; assert arm['indexes'] == {'10':config['namespace_prefix']}
    return dict(publication=publication,arm=arm,binary=binary,required_glibc=abi['required_glibc']['two_bit_http'])


def prepare_authorities(base=Path('.')):
    """LOCAL root only: bind its completed offered qualification to metadata."""
    repo = regular_path(base).resolve(); ctl = controller()
    proof = ctl.qualify(repo)
    cfg = read_json(repo/ctl.CONFIG)
    manifest = read_json(repo/ctl.MANIFEST)
    pointers = {f:dict(path=str(p),**artifact(repo/p)) for f,p in
        (('runtime_config',ctl.CONFIG),('controller_config',ctl.CONTROL),('asset_manifest',ctl.MANIFEST))}
    assets,bridges,small = head_roster(repo,pointers,cfg,proof,metadata_evidence(repo,cfg),manifest)
    roster = dict(schema=SCHEMA+'-heads',authority_pending=False,assets=assets,repository_bridges=bridges,
        small_asset=small,offered_qualification_sha256=sha(encoded(proof)))
    return dict(offered_qualification=proof,head_roster=roster)


def qualify(base=Path('.'), config_path=None, config_sha=None):
    """Remote-safe qualification: completed root proofs, metadata and hashes only."""
    repo = regular_path(base).resolve()
    path = regular_path(repo/CONFIG if config_path is None else config_path)
    pin = science.pin(artifact(path),1 << 20)
    assert config_sha is None or pin['sha256'] == config_sha, 'canary config identity'
    config = read_json(path)
    assert set(config) == FIELDS and config['schema'] == SCHEMA and config['authority_pending'] is False, 'canary freeze pending'
    assert config['resources'] == LIMITS and all(type(config['resources'][k]) is type(v) for k,v in LIMITS.items())
    assert config['code_sha256'] == {n:artifact(regular_path(repo/n))['sha256'] for n in CODE}, 'new executor closure drift'
    ctl = controller()
    assert Path(ctl.__file__).resolve() == repo/CONTROLLER
    with patch.object(cold,'OWN',OWN), patch.object(fixed,'CODE',CODE):
        assert cold.code_closure(repo) == CODE, 'exact transitive closure'
    for field, expected in (('runtime_config',ctl.CONFIG),('controller_config',ctl.CONTROL),('asset_manifest',ctl.MANIFEST)):
        science.read_repo(repo,config[field],expected)
    gates = root_gate(repo,config['runtime_verification'])
    offered_proof = json.loads(science.read_repo(repo,config['offered_qualification'],QUALIFICATION))
    assert science.read_repo(repo,config['offered_qualification']) == encoded(offered_proof), 'detached proof canonical bytes'
    offered_config = read_json(repo/ctl.CONFIG); control = read_json(repo/ctl.CONTROL)
    assert control['schema'] == ctl.SCHEMA and control['authority_pending'] is False
    assert set(control) == ctl.FIELDS and control['execution_source'] == offered_config['execution_source']
    assert control['runtime_config'] == config['runtime_config'] and control['asset_manifest'] == config['asset_manifest']
    assert control['code_sha256'] == {n:artifact(repo/n)['sha256'] for n in ctl.CODE}
    assert offered_config['code_sha256'] == {n:artifact(repo/n)['sha256'] for n in runtime.CODE}
    assert set(offered_config) == set(runtime.FIXED)|{'bucket','namespace_prefix','code_sha256','execution_source','cold_run','cold_source_authority','prices','resources'}
    assert all(encoded(offered_config[k]) == encoded(v) for k,v in runtime.FIXED.items())
    assert offered_config['resources'] == runtime.LIMITS and offered_config['bucket'] == BUCKET and offered_config['region'] == REGION
    assert config['execution_source'] == offered_config['execution_source'], 'new source authorities differ'
    assert set(config['execution_source']) == {'commit','archive_sha256'}
    assert re.fullmatch('[0-9a-f]{40}',config['execution_source']['commit']) and re.fullmatch('[0-9a-f]{64}',config['execution_source']['archive_sha256'])
    assert config['execution_source']['archive_sha256'] != '0'*64, 'source archive freeze pending'
    assert offered_proof['source_archive_commit'] == config['execution_source']['commit'] and offered_proof['source_archive_sha256'] == config['execution_source']['archive_sha256']
    for field,key in (('runtime_config','config_sha256'),('controller_config','controller_config_sha256'),('asset_manifest','asset_manifest_sha256')):
        assert config[field]['sha256'] == offered_proof[key], 'detached offered authority binding'
    assert offered_proof['code_identity_sha256'] == sha(encoded(control['code_sha256']))
    assert offered_proof['runtime_code_identity_sha256'] == sha(encoded(offered_config['code_sha256']))
    assert offered_proof['native_source_identity_sha256'] == cold.driver.SOURCE_ID and offered_proof['native_source_file_count'] == 399
    assert offered_proof['artifact_roster_sha256'] == sha(encoded(ctl.ARTIFACTS)) and offered_proof['campaign_schema'] == ctl.SCHEMA
    assert offered_proof['namespace_prefix'] == offered_config['namespace_prefix']
    evidence = metadata_evidence(repo,offered_config)
    assert offered_proof['binary_sha256'] == evidence['binary']['sha256']
    manifest = json.loads(science.read_repo(repo,config['asset_manifest'],ctl.MANIFEST))
    assert manifest['schema'] == ctl.ASSET_SCHEMA and manifest['authority_pending'] is False
    assets, bridges, small = head_roster(repo,config,offered_config,offered_proof,evidence,manifest)
    roster = json.loads(science.read_repo(repo,config['head_roster'],HEAD_ROSTER))
    assert roster == dict(schema=SCHEMA+'-heads',authority_pending=False,assets=assets,repository_bridges=bridges,
        small_asset=small,offered_qualification_sha256=config['offered_qualification']['sha256']), 'exact frozen HEAD roster'
    assets['canary/config'] = dict(pin,source=dict(bucket=BUCKET,key=config_key(pin['sha256'])))
    assets['canary/head-roster'] = dict(runtime.identity(config['head_roster']),source=dict(bucket=BUCKET,key=config_key(config['head_roster']['sha256'])))
    assert len(assets) + len(ARTIFACTS) + 11 <= LIMITS['requests_max'], 'whole SDK request admission'
    return dict(config_path=str(CONFIG),config_bytes=pin['bytes'],config_sha256=pin['sha256'],
        source_archive_commit=config['execution_source']['commit'],source_archive_sha256=config['execution_source']['archive_sha256'],
        code_identity_sha256=sha(encoded(config['code_sha256'])),asset_manifest_sha256=config['asset_manifest']['sha256'],
        artifact_roster_sha256=sha(encoded(ARTIFACTS)),campaign_schema=SCHEMA,
        offered_config_sha256=config['runtime_config']['sha256'],offered_controller_config_sha256=config['controller_config']['sha256'],
        runtime_code_identity_sha256=offered_proof['runtime_code_identity_sha256'],runtime_verification_sha256=config['runtime_verification']['sha256'],
        native_source_identity_sha256=offered_proof['native_source_identity_sha256'],head_roster_sha256=sha(encoded(assets)),
        required_glibc=evidence['required_glibc'],
        offered_qualification_sha256=config['offered_qualification']['sha256'],
        resources=LIMITS,code_sha256=config['code_sha256'],admission_gates=gates,assets=assets,repository_bridges=bridges,small_asset=small,
        offered_authorities={f:config[f] for f in ('runtime_config','controller_config','asset_manifest','offered_qualification','head_roster')},offered_qualification=offered_proof)


def preflight(base=Path('.'), collection_out=None):
    repo = regular_path(base).resolve(); owned = None
    if collection_out is not None:
        out = regular_path(collection_out).resolve()
        assert re.fullmatch('a[0-9]{4}',out.name) and out == repo/ROOT/out.name
        owned = str(out.relative_to(repo))+'/'
    status = subprocess.check_output(['git','status','--porcelain','-z','--untracked-files=all'],cwd=repo,text=True)
    assert all(owned and row.startswith('?? ') and row[3:].startswith(owned) for row in status.split('\0') if row), 'dirty frozen source'
    proof = qualify(repo); source = proof['source_archive_commit']
    for a,b in ((source,'HEAD'),(source,'origin/main'),('HEAD','origin/main')):
        subprocess.run(['git','merge-base','--is-ancestor',a,b],cwd=repo,check=True)
    authority_paths = [str(CONFIG),*(p['path'] for p in proof['offered_authorities'].values())]
    for name in authority_paths:
        assert (repo/name).read_bytes() == subprocess.check_output(['git','show','HEAD:'+name],cwd=repo)
        assert subprocess.run(['git','cat-file','-e',source+':'+name],cwd=repo,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode != 0, 'source must precede JSON freeze'
    for name in (*CODE,*proof['admission_gates'],*(p['path'] for p in proof['repository_bridges'].values())):
        assert (repo/name).read_bytes() == subprocess.check_output(['git','show',source+':'+name],cwd=repo), 'frozen source/body drift'
    assert science.archive_digest(source,repo) == proof['source_archive_sha256']
    return proof


def bootstrap_source():
    """Keep the existing bounded bootstrap, with explicit offered policy seams."""
    source = inspect.getsource(canary.bootstrap)
    changes = {
        '2147483648': "proof['resources']['scratch_bytes']",
        "'research/semantic-router/20261002/fixed48-canary-configs/'": "'research/semantic-router/20261002/fixed48-offered-canary-configs/'",
        "('config_sha256', 'code_identity_sha256', 'asset_manifest_sha256', 'artifact_roster_sha256')": repr(IDENTITIES),
        'if requests > 64:': "if requests > proof['resources']['requests_max']:",
        'request_cap=64,': "request_cap=proof['resources']['requests_max'],",
    }
    for old,new in changes.items():
        assert old in source, 'shared bootstrap hook drift: '+old
        source = source.replace(old,new)
    return source


def user_data(commit,archive_sha,archive_key,prefix,qualification):
    real_getsource = inspect.getsource
    adapted = bootstrap_source()
    def source(function):
        return adapted if function is canary.bootstrap else real_getsource(function)
    with patch.multiple(canary,ROOT=ROOT,CONFIG=CONFIG,SCHEMA=SCHEMA,PREFIX=PREFIX,MODULE=MODULE,
            WORK_ROOT=WORK_ROOT,WALL=WALL,WORKER_SECONDS=WORKER_SECONDS,SERVICE_SECONDS=SERVICE_SECONDS,
            MEMORY=MEMORY,SCRATCH=SCRATCH,LIMITS=LIMITS,IDENTITIES=IDENTITIES,ARTIFACTS=ARTIFACTS), \
            patch.object(inspect,'getsource',side_effect=source):
        body = canary.user_data(commit,archive_sha,archive_key,prefix,qualification)
    body = body.replace('shutdown -h +15','shutdown -h +8').replace('cohere-fixed48-canary','cohere-fixed48-offered-canary')
    env = '\n'.join('export '+n+'=2' for n in fixed.retained.THREAD_ENV)
    body = body.replace('phase=source\n',env+'\nphase=source\n',1)
    body = body.replace('phase=canary\n','rm -rf "$PWD/apt"\nphase=canary\n',1)
    subprocess.run(['bash','-n'],input=body,text=True,check=True,timeout=5)
    assert len(body.encode()) < 16384
    return body


def head_assets(s3,assets,ledger,out,check):
    rows = []
    for name,entry in sorted(assets.items()):
        source = entry['source']; check()
        kwargs = {'VersionId':source['version_id']} if 'version_id' in source else {}
        with fixed.sdk_operation(s3,'head_object',source['bucket'],source['key'],ledger,**kwargs) as (head,row):
            assert head['ResponseMetadata']['HTTPStatusCode'] == 200 and head['ContentLength'] == entry['bytes'], 'HEAD size/status: '+name
            for pin,field in (('etag','ETag'),('version_id','VersionId')):
                if pin in source: assert head.get(field) == source[pin], 'HEAD '+pin
            rows.append(dict(name=name,bucket=source['bucket'],key=source['key'],bytes=head['ContentLength'],
                etag=head.get('ETag'),version_id=head.get('VersionId'),declared_sha256=entry['sha256'],sha_authenticated=False))
        write(out/'asset-heads.json',dict(heads=rows,distinct_keys=len(rows),complete=False))
    assert len({(r['bucket'],r['key']) for r in rows}) == len(assets), 'all-key distinct HEAD roster'
    write(out/'asset-heads.json',dict(heads=rows,distinct_keys=len(rows),complete=True))
    return rows


def cli_smoke(repo,out,check):
    rows = []
    with (out/'cli.log').open('xb') as log:
        for module,status in ((CONTROLLER[:-3].replace('/','.'),2),(runtime.OWN[:-3].replace('/','.'),1),(MODULE,2)):
            check()
            with subprocess.Popen([sys.executable,'-m',module],cwd=repo,env=dict(os.environ,
                    PYTHONPATH=str(repo),PYTHONDONTWRITEBYTECODE='1',AWS_MAX_ATTEMPTS='1'),stdout=log,stderr=log) as child:
                try: code = child.wait(timeout=15)
                except BaseException: child.kill(); child.wait(); raise
            assert code == status, 'usage CLI: '+module
            rows.append(dict(module=module,argv=[],returncode=code,real=True,scientific_invocation=False))
        log.flush(); os.fsync(log.fileno())
    body = (out/'cli.log').read_bytes()
    assert 0 < len(body) <= 1 << 20 and body.count(b'usage:') == 2
    assert b'AssertionError: CONFIG SHA REPO OUTPUT or --replay CONFIG SHA REPO CLOSED_OUTPUT' in body
    assert b'ModuleNotFoundError' not in body and b'ImportError' not in body
    write(out/'cli.json',dict(calls=rows,complete=True,logs=artifact(out/'cli.log')))
    return rows


def eligibility(repo,config):
    import platform
    ctl = controller(); _,bootstrap = lifecycle()
    release = platform.freedesktop_os_release(); libc,version = os.confstr('CS_GNU_LIBC_VERSION').split()
    assert platform.machine() == 'x86_64' and (release['ID'],release['VERSION_ID']) == ('ubuntu','24.04')
    assert libc == 'glibc' and bootstrap._version(version) >= bootstrap._version(bootstrap.RUNTIME_GLIBC)
    historical = read_json(repo/ctl.CONFIG)
    binary = regular_path(repo/historical['cold_run']['files'][ctl.HTTP]['path'])
    abi = json.loads(runtime.read(repo,historical['cold_run']['files']['runtime-abi.json']))
    assert abi['qualified'] is True and abi['binaries']['two_bit_http'] == artifact(binary), 'retained ABI/current binary binding'
    required = abi['required_glibc']['two_bit_http']
    assert bootstrap._version(required) <= bootstrap._version(version), 'HTTP libc eligibility'
    assert all(os.environ.get(n) == '2' for n in fixed.retained.THREAD_ENV) and os.environ.get('AWS_MAX_ATTEMPTS') == '1'
    return dict(architecture=platform.machine(),os_release=release,libc=libc,glibc_version=version,
        required_glibc=required,binary=artifact(binary),native_processes=0,python_executable=sys.executable,
        threads=dict.fromkeys(fixed.retained.THREAD_ENV,'2'))


def stage(repo,out,prefix):
    repo,out = regular_path(repo).resolve(),regular_path(out).resolve()
    assert out == WORK_ROOT and repo == out/'repo' and re.fullmatch(re.escape(PREFIX)+'a[0-9]{4}',prefix)
    started = time.monotonic(); resources = {}
    summary = dict(infrastructure_status='FAIL',scientific_status='UNMEASURED',performance_measured=False,
        native_ann_calls=0,scientific_calls=0,offered_main_calls=0,closed=False)
    cleanup = dict(temporary_files_removed=False,sdk_closed=False,process_cleanup=False)
    failure = dict(status='pending',replacement_allowed=False)
    temporary,s3 = out/'canary-temp',None
    previous = [signal.getsignal(n) for n in (signal.SIGTERM,signal.SIGALRM)]
    def interrupted(signum,frame): raise InterruptedError('canary interrupted')
    for n in (signal.SIGTERM,signal.SIGALRM): signal.signal(n,interrupted)
    signal.setitimer(signal.ITIMER_REAL,WORKER_SECONDS)
    try:
        import boto3
        from botocore.config import Config
        capability = canary.sdk_guard()
        assert capability['boto3'] == capability['botocore'] == SDK_VERSION
        assert fixed.sdk_guard()['python_executable'] == sys.executable
        s3 = boto3.client('s3',region_name=REGION,config=Config(retries={'total_max_attempts':1},connect_timeout=5,read_timeout=5))
        with fixed.observe(out,LIMITS,resources,started+WORKER_SECONDS) as check:
            temporary.mkdir()
            config = read_json(out/'config.json')
            ctl = controller()
            with (out/'sdk-ledger.jsonl').open('xb') as ledger:
                for field,name in AUTHORITY_FILES:
                    pin = config[field]; target = repo/pin['path']
                    assert not target.exists(), 'authority present in execution source'
                    key = config_key(pin['sha256']) if field == 'head_roster' else ctl.config_key(pin['sha256'])
                    fixed.fetch(s3,dict(bucket=BUCKET,key=key),pin,ledger,check,started+WORKER_SECONDS,out/name)
                    target.parent.mkdir(parents=True,exist_ok=True); write(target,(out/name).read_bytes())
                proof = qualify(repo,out/'config.json')
                early = read_json(out/'source-qualification.json')
                assert all(proof[k] == early[k] for k in (*IDENTITIES,'source_archive_commit','source_archive_sha256','config_bytes'))
                write(out/'source-qualification.json',proof)
                abi = eligibility(repo,config); write(out/'eligibility.json',abi)
                modules = [MODULE,CONTROLLER[:-3].replace('/','.'),runtime.OWN[:-3].replace('/','.')]
                write(out/'imports.json',dict(complete=True,real=True,python_executable=sys.executable,sdk=capability,
                    modules=modules,files={n:artifact(repo/n) for n in (OWN,CONTROLLER,runtime.OWN)}))
                heads = head_assets(s3,proof['assets'],ledger,out,check)
                small = proof['small_asset']; pin = proof['assets'][small]
                selected = next(r for r in heads if r['name'] == small)
                body = fixed.fetch(s3,pin['source'],pin,ledger,check,started+WORKER_SECONDS,temporary/'manifest.json',etag=selected['etag'])
                write(out/'small-body.json',dict(name=small,**body,authenticated=True,full_body=True,selected_asset_gets=1,large_asset_gets=0))
            cli = cli_smoke(repo,out,check)
            shutil.rmtree(temporary); cleanup['temporary_files_removed'] = True
            s3.close(); s3 = None; cleanup['sdk_closed'] = True
            check(); summary.update(infrastructure_status='INFRA_GO',head_keys=len(heads),cli=cli)
        cleanup['process_cleanup'] = True; failure['status'] = 'complete'
    except BaseException as error:
        summary['infrastructure_status'] = 'FAIL'
        failure.update(status='failed',error_type=type(error).__name__,error=str(error))
    finally:
        try:
            try:
                if temporary.exists(): shutil.rmtree(temporary)
                cleanup['temporary_files_removed'] = not temporary.exists()
            finally:
                if s3 is not None: s3.close(); cleanup['sdk_closed'] = True
            if resources.get('cgroup',{}).get('closed'):
                fixed.check_cgroup(resources['cgroup']['before'],resources['cgroup']['after'],LIMITS,drained=True)
                cleanup['process_cleanup'] = True
        except BaseException as error:
            summary['infrastructure_status'] = 'FAIL'; failure.update(status='failed',cleanup_error=str(error))
        signal.setitimer(signal.ITIMER_REAL,0)
        for n,handler in zip((signal.SIGTERM,signal.SIGALRM),previous): signal.signal(n,handler)
        summary['closed'] = all(cleanup.values())
        if not summary['closed']: summary['infrastructure_status'] = 'FAIL'
        resources.update(wall_seconds=time.monotonic()-started,limits=LIMITS,source_scope='whole worker root')
        for name,value in (('resources.json',resources),('cleanup.json',cleanup),('failure.json',failure),('summary.json',summary)): write(out/name,value)
    return summary


def poll(ec2,s3,prefix,instance_id,started):
    with patch.object(canary,'WALL',WALL): return canary.poll(ec2,s3,prefix,instance_id,started)


def validate_closed(out,proof,terminal):
    assert terminal['infrastructure_status'] in ('INFRA_GO','FAIL')
    assert terminal['performance_measured'] is False and terminal['scientific_status'] == 'UNMEASURED'
    assert terminal['resource_ledger']['limits'] == LIMITS and terminal['resource_ledger']['request_cap'] == LIMITS['requests_max']
    assert 0 < terminal['resource_ledger']['s3_requests_including_terminal_put'] <= LIMITS['requests_max']
    if terminal['infrastructure_status'] == 'FAIL':
        assert terminal['status'] == 'failed' and terminal['exit_code'] != 0
        return False
    assert terminal['status'] == terminal['phase'] == 'complete' and terminal['exit_code'] == terminal['original_exit_code'] == 0
    assert set(terminal['artifacts']) == set(ARTIFACTS)
    for name,pin in terminal['artifacts'].items(): assert artifact(out/name) == pin, 'closed artifact identity'
    summary,cleanup,resources = (read_json(out/n) for n in ('summary.json','cleanup.json','resources.json'))
    assert summary['infrastructure_status'] == 'INFRA_GO' and summary['closed'] is True
    assert summary['scientific_status'] == 'UNMEASURED' and summary['performance_measured'] is False
    assert summary['native_ann_calls'] == summary['scientific_calls'] == summary['offered_main_calls'] == 0
    assert cleanup == dict(temporary_files_removed=True,sdk_closed=True,process_cleanup=True)
    assert resources['limits'] == LIMITS and resources['wall_seconds'] <= WORKER_SECONDS and resources['peak_scratch_bytes'] <= SCRATCH
    assert resources['cgroup']['closed'] is True
    fixed.check_cgroup(resources['cgroup']['before'],resources['cgroup']['after'],LIMITS,drained=True)
    assert read_json(out/'failure.json')['status'] == 'complete'
    assert artifact(out/'config.json') == dict(bytes=proof['config_bytes'],sha256=proof['config_sha256'])
    assert read_json(out/'source-qualification.json') == proof
    boot = read_json(out/'bootstrap-staging.json')
    assert all(boot[k] is True for k in ('source_authenticated','config_authenticated','code_authenticated_before_import','source_archive_removed'))
    assert boot['source_archive_sha256'] == proof['source_archive_sha256'] and boot['scratch_bytes'] <= SCRATCH
    imports = read_json(out/'imports.json')
    assert imports['real'] is imports['complete'] is True and imports['sdk'] == boot['sdk'] == dict(boto3=SDK_VERSION,botocore=SDK_VERSION,conditional_put=True,network_calls=0)
    assert imports['python_executable']
    assert imports['modules'] == [MODULE,CONTROLLER[:-3].replace('/','.'),runtime.OWN[:-3].replace('/','.')]
    for n in (OWN,CONTROLLER,runtime.OWN): assert imports['files'][n]['sha256'] == proof['code_sha256'][n]
    for field,name in AUTHORITY_FILES:
        assert artifact(out/name) == runtime.identity(proof['offered_authorities'][field])
    abi = read_json(out/'eligibility.json'); _,bootstrap = lifecycle()
    assert abi['architecture'] == 'x86_64' and (abi['os_release']['ID'],abi['os_release']['VERSION_ID']) == ('ubuntu','24.04')
    assert abi['libc'] == 'glibc' and bootstrap._version(abi['glibc_version']) >= bootstrap._version(bootstrap.RUNTIME_GLIBC)
    assert bootstrap._version(abi['required_glibc']) <= bootstrap._version(abi['glibc_version']) and abi['native_processes'] == 0
    assert abi['required_glibc'] == proof['required_glibc']
    assert abi['python_executable'] == imports['python_executable'] and abi['threads'] == dict.fromkeys(fixed.retained.THREAD_ENV,'2')
    assert abi['binary'] == runtime.identity(proof['offered_qualification']['assets']['native/binaries/two_bit_http'])
    heads = read_json(out/'asset-heads.json')
    assert heads['complete'] is True and heads['distinct_keys'] == len(heads['heads']) == len(proof['assets'])
    assert [r['name'] for r in heads['heads']] == sorted(proof['assets'])
    for row in heads['heads']:
        entry = proof['assets'][row['name']]; source = entry['source']
        assert (row['bucket'],row['key'],row['bytes'],row['declared_sha256']) == (source['bucket'],source['key'],entry['bytes'],entry['sha256'])
        assert row['sha_authenticated'] is False
        for n in ('etag','version_id'):
            if n in source: assert row[n] == source[n]
    small = read_json(out/'small-body.json'); pin = proof['assets'][proof['small_asset']]
    assert small == dict(name=proof['small_asset'],**runtime.identity(pin),authenticated=True,full_body=True,selected_asset_gets=1,large_asset_gets=0)
    cli = read_json(out/'cli.json')
    assert cli['complete'] is True and cli['logs'] == artifact(out/'cli.log')
    assert [(r['module'],r['argv'],r['returncode'],r['real'],r['scientific_invocation']) for r in cli['calls']] == [
        (CONTROLLER[:-3].replace('/','.'),[],2,True,False),(runtime.OWN[:-3].replace('/','.'),[],1,True,False),(MODULE,[],2,True,False)]
    sdk = [json.loads(r) for r in (out/'sdk-ledger.jsonl').read_bytes().splitlines()]
    expected = [('get_object',(config_key if f == 'head_roster' else controller().config_key)(proof['offered_authorities'][f]['sha256'])) for f,_ in AUTHORITY_FILES]
    expected += [('head_object',proof['assets'][n]['source']['key']) for n in sorted(proof['assets'])]
    expected += [('get_object',pin['source']['key'])]
    assert [(r['operation'],r['key']) for r in sdk] == expected
    assert all(r['bucket'] == BUCKET and r['sdk_http_dispatch_attempts'] == 1 and r['retry_attempts'] == 0 and r['error'] is None and r['http_status'] == 200 for r in sdk)
    assert [r['consumed_response_bytes'] for r in sdk] == [proof['offered_authorities'][f]['bytes'] for f,_ in AUTHORITY_FILES] + [0]*len(proof['assets']) + [pin['bytes']]
    boot_sdk = [json.loads(r) for r in (out/'bootstrap-sdk-ledger.jsonl').read_bytes().splitlines()]
    source_key = 'research/native-library-check/sources/'+proof['source_archive_sha256']+'.tar.gz'
    assert [(r['operation'],r['key']) for r in boot_sdk[:3]] == [('head_object',source_key),('get_object',source_key),('get_object',config_key(proof['config_sha256']))]
    assert all(r['attempts'] == 1 and r['retries'] == 0 and r['error'] is None and r['status'] == 200 for r in boot_sdk)
    assert len(boot_sdk) == 3+len(ARTIFACTS)-1
    assert all(r['operation'] == 'put_object' for r in boot_sdk[3:])
    assert {r['key'].rsplit('/artifacts/',1)[-1] for r in boot_sdk[3:]} == set(ARTIFACTS)-{'bootstrap-sdk-ledger.jsonl'}
    assert terminal['resource_ledger']['s3_requests_including_terminal_put'] == len(sdk)+len(boot_sdk)+2
    assert {r['name'] for r in terminal['uploads']} == set(ARTIFACTS) and len(terminal['uploads']) == len(ARTIFACTS)
    assert all(r.get('status') == 200 and r.get('attempts') == 1 and r.get('retries') == 0 for r in terminal['uploads'])
    return True


def collect(s3,prefix,out,instance_id,commit,digest):
    with patch.multiple(canary,ROOT=ROOT,CONFIG=CONFIG,PREFIX=PREFIX,SCHEMA=SCHEMA,ARTIFACTS=ARTIFACTS,
            IDENTITIES=IDENTITIES,preflight=preflight,validate_closed=validate_closed,COMPUTE_CAP=COMPUTE_CAP,ALLOWANCE=ALLOWANCE):
        return canary.collect(s3,prefix,out,instance_id,commit,digest)


@contextmanager
def lifecycle_adapter():
    from datetime import datetime,timezone
    shared,_ = lifecycle()
    real_session = shared.boto3.Session
    def session(*args,**kwargs):
        value = real_session(*args,**kwargs); real_client = value.client
        def client(service,*args,**kwargs):
            result = real_client(service,*args,**kwargs)
            if service == 'ec2':
                quote = result.describe_spot_price_history
                def fresh(**kwargs):
                    response = quote(**kwargs); row = response['SpotPriceHistory'][0]
                    age = (datetime.now(timezone.utc)-row['Timestamp']).total_seconds()
                    assert -10 <= age <= 300 and 0 < float(row['SpotPrice']) <= SPOT_MAX_USD_PER_HOUR, 'fresh Spot admission'
                    assert SPOT_MAX_USD_PER_HOUR*WALL/3600 <= COMPUTE_CAP
                    return response
                result.describe_spot_price_history = fresh
            return result
        value.client = client; return value
    with patch.object(shared.boto3,'Session',side_effect=session), \
            patch.multiple(canary,SCHEMA=SCHEMA,ALLOWANCE=ALLOWANCE):
        with canary.lifecycle_adapter() as shared: yield shared


def stage_config(proof):
    controller().stage_configs(proof['offered_qualification'])
    for path,pin in ((CONFIG,dict(bytes=proof['config_bytes'],sha256=proof['config_sha256'])),(HEAD_ROSTER,proof['offered_authorities']['head_roster'])):
        with patch.multiple(canary,CONFIG=path,config_key=config_key):
            canary.stage_config(dict(config_bytes=pin['bytes'],config_sha256=pin['sha256']))


def main(attempt):
    assert re.fullmatch('a[0-9]{4}',attempt)
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        proof = preflight(); stage_config(proof)
        with lifecycle_adapter() as shared: return shared.main(attempt,campaign=sys.modules[__name__])
    finally: os.chdir(before)


def self_check():
    """Real source/fixture/model/Bash/CLI; only SDK, lifecycle and cgroup doubles."""
    import copy
    from contextlib import ExitStack,redirect_stdout
    from datetime import datetime,timedelta,timezone
    import gzip
    import resource
    import tarfile
    from types import SimpleNamespace
    from unittest.mock import Mock
    import boto3
    import urllib.request
    from botocore.model import ServiceModel
    from botocore.session import get_session
    resource.setrlimit(resource.RLIMIT_AS,(256 << 20,256 << 20)); signal.alarm(55)
    started,checks = time.monotonic(),[]
    module,repo = sys.modules[__name__],Path(__file__).resolve().parents[1]
    def reject(call):
        try: call()
        except (AssertionError,OSError,ValueError,KeyError,RuntimeError,subprocess.CalledProcessError): return
        raise AssertionError('negative canary check admitted')
    def snap():
        return dict(cgroup='/explicit-fixture',**{'memory.max':str(MEMORY),'memory.peak':'10000',
            'memory.swap.max':'0','memory.swap.peak':'0','cpu.max':'200000 100000','pids.max':'512',
            'pids.current':'1','process_ids':[os.getpid()],'memory.events':'oom 0\noom_kill 0\nmax 0\n',
            'memory.swap.events':'max 0\n','pids.events':'max 0\n'})
    with tempfile.TemporaryDirectory(prefix='offered-canary-check-') as tmp, \
            tempfile.TemporaryDirectory(prefix='offered-canary-source-check-',dir=repo.parent) as source_tmp, \
            patch.object(boto3,'Session',side_effect=AssertionError('cloud forbidden')) as cloud, \
            patch.object(runtime,'main',side_effect=AssertionError('offered experiment forbidden')) as experiment:
        work = Path(tmp)
        gate = dict(path=str(VERIFICATION),**artifact(repo/VERIFICATION))
        assert root_gate(repo,gate)
        reject(lambda: root_gate(repo,dict(gate,sha256='0'*64)))
        files = {}
        for n in runtime.COLD_ROSTER:
            target = repo/runtime.COLD/n
            pointer = dict(path=str(runtime.COLD/n))
            if target.exists(): pointer.update(artifact(target))
            else:
                body = gzip.decompress(Path(str(target)+'.gz').read_bytes())
                pointer.update(bytes=len(body),sha256=sha(body),archived_path=pointer['path']+'.gz')
            files[n] = pointer
        archived = json.loads(runtime.read(repo,files['asset-manifest.json']))
        old = json.loads(runtime.read(repo,files['config.json']))
        bridge_path = cold.ROOT/'offered-cold-source-authority.json'
        prices_path = fixed.BASE/'offered-price-reference.json'
        offered_config = dict(runtime.FIXED,bucket=BUCKET,namespace_prefix=old['namespace_prefix'],resources=runtime.LIMITS,
            code_sha256={n:artifact(repo/n)['sha256'] for n in runtime.CODE},
            execution_source=dict(commit='a'*40,archive_sha256='b'*64),cold_run=dict(directory=str(runtime.COLD),files=files),
            cold_source_authority=dict(path=str(bridge_path),**artifact(repo/bridge_path)),
            prices=dict(path=str(prices_path),**artifact(repo/prices_path)))
        path = work/'real-fixture-config.json'; write(path,offered_config)
        _,runtime_proof,evidence = runtime.qualify(path,artifact(path)['sha256'],repo)
        assert len(evidence['requests']) == 64 and runtime_proof['native_source_file_count'] == 399
        changed = copy.deepcopy(offered_config); changed['code_sha256'][runtime.OWN] = '0'*64
        write(path,changed); reject(lambda: runtime.qualify(path,artifact(path)['sha256'],repo)); write(path,offered_config)
        checks.append('actual-current-root-receipt/full56-native399-sealed64-qualified-bridge/code-tamper')
        # Hardlinks preserve all real bodies without payload copies or source writes.
        combined = Path(source_tmp)/'combined'
        shutil.copytree(repo,combined,copy_function=os.link,ignore=shutil.ignore_patterns('.git','__pycache__','target'))
        ctl_real = controller()
        real_manifest = dict(schema=ctl_real.ASSET_SCHEMA,authority_pending=False,
            assets={ctl_real.HTTP:archived['assets']['qualification/binaries/two_bit_http']})
        for name,value in ((ctl_real.CONFIG,offered_config),(ctl_real.MANIFEST,real_manifest)):
            assert not (combined/name).exists(), 'temporary authorities require clean unfrozen input'
            (combined/name).parent.mkdir(parents=True,exist_ok=True); write(combined/name,value)
        def pointer(name): return dict(path=str(name),**artifact(combined/name))
        real_control = dict(schema=ctl_real.SCHEMA,authority_pending=False,execution_source=offered_config['execution_source'],
            code_sha256={n:artifact(combined/n)['sha256'] for n in ctl_real.CODE},
            runtime_config=pointer(ctl_real.CONFIG),asset_manifest=pointer(ctl_real.MANIFEST))
        assert not (combined/ctl_real.CONTROL).exists(); write(combined/ctl_real.CONTROL,real_control)
        real_config = dict(schema=SCHEMA,authority_pending=False,execution_source=offered_config['execution_source'],
            code_sha256={n:artifact(combined/n)['sha256'] for n in CODE},resources=LIMITS,
            runtime_config=pointer(ctl_real.CONFIG),controller_config=pointer(ctl_real.CONTROL),
            asset_manifest=pointer(ctl_real.MANIFEST),runtime_verification=pointer(VERIFICATION))
        assert not (combined/CONFIG).exists(); (combined/CONFIG).parent.mkdir(parents=True,exist_ok=True); write(combined/CONFIG,real_config)
        program = '''import json
from pathlib import Path
from contextlib import ExitStack
from unittest.mock import patch
import boto3
from scripts import launch_cohere_fixed48_offered_canary_spot as c
from scripts import run_cohere_fixed48_offered_http as r
prepared=c.prepare_authorities()
c.write(c.QUALIFICATION,c.encoded(prepared['offered_qualification']))
c.write(c.HEAD_ROSTER,prepared['head_roster'])
cfg=c.read_json(c.CONFIG)
for field,path in (('offered_qualification',c.QUALIFICATION),('head_roster',c.HEAD_ROSTER)):
    cfg[field]=dict(path=str(path),**c.artifact(path))
c.write(c.CONFIG,cfg)
opened=[]; original_open=Path.open
def metadata_open(path,*args,**kwargs):
    assert path.name not in ('truth.i64','requests.jsonl','records.jsonl','publisher-requests.jsonl','sealed-reference-k10.jsonl'), 'GT/panel body forbidden remotely'
    opened.append(str(path)); return original_open(path,*args,**kwargs)
with ExitStack() as stack:
    spies=[]
    for owner,name in ((boto3,'client'),(boto3,'Session'),(r,'main'),(r,'qualify'),(r,'historical_inputs'),(r,'reduce_cell'),(c.controller(),'qualify'),(c.fixed,'panel_inputs'),(c.fixed,'reduce_records')):
        spies.append(stack.enter_context(patch.object(owner,name,side_effect=AssertionError('scientific/cloud path forbidden remotely: '+name))))
    stack.enter_context(patch.object(Path,'open',metadata_open))
    p=c.qualify(Path.cwd())
    assert len(p['assets']) == 51 and p['offered_qualification']['native_source_file_count'] == 399
    import platform,os
    with patch.object(platform,'freedesktop_os_release',return_value=dict(ID='ubuntu',VERSION_ID='24.04')), patch.object(platform,'machine',return_value='x86_64'), patch.object(os,'confstr',return_value='glibc 2.39'), patch.dict(os.environ,dict.fromkeys(c.fixed.retained.THREAD_ENV,'2')|{'AWS_MAX_ATTEMPTS':'1'}):
        assert c.eligibility(Path.cwd(),cfg)['required_glibc'] == p['required_glibc']
    cfg=c.read_json(c.CONFIG); cfg['authority_pending']=True; c.write(c.CONFIG,cfg)
    try: c.qualify(Path.cwd())
    except AssertionError: pass
    else: raise AssertionError('pending authority admitted')
    for spy in spies: spy.assert_not_called()
    print(json.dumps(dict(real_combined=True,metadata_only=True,GT_reads=0,code_paths=len(c.CODE),head_keys=len(p['assets']),native_source_files=399,source_certificate_gates=len(p['admission_gates']))))
'''
        integration = subprocess.run([sys.executable,'-B','-c',program],cwd=combined,env=dict(os.environ,
            PYTHONPATH=str(combined),PYTHONDONTWRITEBYTECODE='1',AWS_MAX_ATTEMPTS='1'),capture_output=True,text=True,timeout=20)
        assert integration.returncode == 0, integration.stderr
        real_combined = json.loads(integration.stdout)
        checks.append('actual-final-controller-runtime-canary-metadata-qualify/TEMP-detached-proof-roster/noGT-spies/pending-negative')
        capability = canary.sdk_guard()
        assert capability == dict(boto3=SDK_VERSION,botocore=SDK_VERSION,conditional_put=True,network_calls=0)
        model = copy.deepcopy(get_session().get_component('data_loader').load_service_model('s3','service-2'))
        model['shapes'][model['operations']['PutObject']['input']['shape']]['members'].pop('IfNoneMatch')
        reject(lambda: canary.sdk_guard(ServiceModel(model)))
        checks.append('actual-installed-pinned-SDK/conditional-PUT-service-model-negative')

        controller_path = repo/CONTROLLER
        assert controller_path.is_file(), 'final integrated controller source required'
        source_repo = work/'cli-source'; source_repo.mkdir()
        for n in CODE:
            target = source_repo/n; target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(controller_path if n == CONTROLLER else repo/n,target)
        from scripts import prepare_cohere_top32_coverage as panel
        for p in (panel.FIXED['quality_config'],dict(path=str(cold.library.quality.panel.AUTHORITY))):
            n = p.get('archived_path',p['path']); target = source_repo/n
            target.parent.mkdir(parents=True,exist_ok=True); shutil.copyfile(repo/n,target)
        cli_out = work/'cli'; cli_out.mkdir(); cli_smoke(source_repo,cli_out,lambda:None)
        checks.append('real-three-module-imports/noargs-CLI/owned-child-wait')

        ctl = SimpleNamespace(CONFIG=runtime.ROOT/'config.json',CONTROL=runtime.ROOT/'controller-config.json',
            MANIFEST=runtime.ROOT/'asset-manifest.json',HTTP='native/binaries/two_bit_http',
            config_key=lambda d:'research/semantic-router/20261002/fixed48-offered-configs/'+d+'.json')
        manifest = dict(schema='borsuk-fixed48-offered-assets-v1',authority_pending=False,
            assets={ctl.HTTP:archived['assets']['qualification/binaries/two_bit_http']})
        authorities = {'runtime_config':encoded(offered_config),'controller_config':b'{"explicit_local_controller_fixture":true}',
            'asset_manifest':encoded(manifest)}
        config = dict(schema=SCHEMA,authority_pending=False,execution_source=offered_config['execution_source'],
            resources=LIMITS,code_sha256={n:artifact(source_repo/n)['sha256'] for n in CODE},runtime_verification=gate)
        for field,name in (('runtime_config',ctl.CONFIG),('controller_config',ctl.CONTROL),('asset_manifest',ctl.MANIFEST)):
            body = authorities[field]; config[field] = dict(path=str(name),bytes=len(body),sha256=sha(body))
        offered_proof = dict(assets=manifest['assets'])
        with patch.object(module,'controller',return_value=ctl):
            assets,bridges,small = head_roster(repo,config,offered_config,offered_proof,evidence,manifest)
        authorities['offered_qualification'] = encoded(offered_proof)
        authorities['head_roster'] = encoded(dict(schema=SCHEMA+'-heads',authority_pending=False,assets=assets,
            repository_bridges=bridges,small_asset=small,offered_qualification_sha256=sha(authorities['offered_qualification'])))
        for field,name in (('offered_qualification',QUALIFICATION),('head_roster',HEAD_ROSTER)):
            body = authorities[field]; config[field] = dict(path=str(name),bytes=len(body),sha256=sha(body))
        config_body = encoded(config); pin = dict(bytes=len(config_body),sha256=sha(config_body))
        assets['canary/config'] = dict(pin,source=dict(bucket=BUCKET,key=config_key(pin['sha256'])))
        assets['canary/head-roster'] = dict(runtime.identity(config['head_roster']),source=dict(bucket=BUCKET,key=config_key(config['head_roster']['sha256'])))
        proof = dict(config_path=str(CONFIG),config_bytes=pin['bytes'],config_sha256=pin['sha256'],
            source_archive_commit='a'*40,source_archive_sha256='b'*64,campaign_schema=SCHEMA,
            code_identity_sha256=sha(encoded(config['code_sha256'])),code_sha256=config['code_sha256'],
            asset_manifest_sha256=config['asset_manifest']['sha256'],artifact_roster_sha256=sha(encoded(ARTIFACTS)),
            offered_config_sha256=config['runtime_config']['sha256'],offered_controller_config_sha256=config['controller_config']['sha256'],
            runtime_code_identity_sha256=runtime_proof['code_identity_sha256'],runtime_verification_sha256=gate['sha256'],
            native_source_identity_sha256=runtime_proof['native_source_identity_sha256'],head_roster_sha256=sha(encoded(assets)),
            required_glibc='2.38',
            offered_qualification_sha256=config['offered_qualification']['sha256'],
            resources=LIMITS,assets=assets,repository_bridges=bridges,small_asset=small,offered_qualification=offered_proof,
            offered_authorities={f:config[f] for f in authorities},admission_gates={})
        assert len(assets) == 51 and len(bridges) == 5
        assert runtime.identity(assets[small]) == dict(bytes=34661,sha256='aa9f0919843660e965953f67453ee0b0ca6a58255b2cfab52e1880fad4c608dc')
        small_body = encoded(evidence['publication']['manifest'])+b'\n'
        class SDK:
            def __init__(self,mode='success'):
                self.mode,self.calls,self.streams,self.bodies,self.closed,self.handlers = mode,[],[],{},False,{}
                self.meta = SimpleNamespace(events=SimpleNamespace(register=self.register,unregister=self.unregister))
            def register(self,event,handler,unique_id): self.handlers[unique_id] = (event,handler)
            def unregister(self,event,unique_id): self.handlers.pop(unique_id)
            def response(self,operation,key):
                self.calls.append((operation,key))
                for event,handler in list(self.handlers.values()):
                    if event == 'before-send.s3':
                        handler()
                        if self.mode == 'retry': handler()
                return dict(ResponseMetadata=dict(HTTPStatusCode=200,RetryAttempts=0))
            def head_object(self,Bucket,Key,**kwargs):
                response = self.response('head_object',Key)
                if self.mode == 'missing': raise FileNotFoundError(Key)
                if Key in self.bodies: entry = dict(bytes=len(self.bodies[Key]),source={})
                else: entry = next(a for a in assets.values() if a['source']['key'] == Key)
                response.update(ContentLength=entry['bytes']+(self.mode == 'size'),ETag=entry['source'].get('etag','"fixture"'))
                if 'version_id' in entry['source']: response['VersionId'] = entry['source']['version_id']
                if self.mode == 'etag': response['ETag'] = 'changed'
                return response
            def get_object(self,Bucket,Key,**kwargs):
                response = self.response('get_object',Key)
                body = self.bodies.get(Key)
                if body is None:
                    assert Key == assets[small]['source']['key'], 'large/nonselected GET forbidden'
                    body = small_body
                    if self.mode == 'tamper': body = b'!'+body[1:]
                stream = io.BytesIO(body); self.streams.append(stream)
                response.update(Body=stream,ContentLength=len(body),ETag='"fixture"'); return response
            def put_object(self,Bucket,Key,Body,**kwargs):
                assert kwargs['IfNoneMatch'] == '*' and Key not in self.bodies
                response = self.response('put_object',Key)
                if self.mode == 'upload': raise OSError('upload failure')
                self.bodies[Key] = bytes(Body); return response
            def close(self): self.closed = True
        for mode in ('success','missing','size','etag','retry','tamper'):
            out = work/('heads-'+mode); out.mkdir(); sdk = SDK(mode)
            with (out/'ledger').open('xb') as ledger:
                if mode in ('missing','size','etag','retry'): reject(lambda: head_assets(sdk,assets,ledger,out,lambda:None))
                else:
                    assert len(head_assets(sdk,assets,ledger,out,lambda:None)) == 51
                    action = lambda: fixed.fetch(sdk,assets[small]['source'],assets[small],ledger,lambda *args:None,time.monotonic()+5,out/'root',etag='"fixture"')
                    if mode == 'tamper': reject(action)
                    else: assert action() == runtime.identity(assets[small])
                    assert len(sdk.calls) == 52 and all(s.closed for s in sdk.streams)
                    assert not (out/'root.part').exists()
        checks.append('derived-all51-HEAD/no-large-GET/etag-size-missing-retry/body-tamper/part-cleanup')
        source_key = 'research/native-library-check/sources/'+'b'*64+'.tar.gz'
        shell = user_data('a'*40,'b'*64,source_key,PREFIX+'a0001',proof)
        assert all(v in shell for v in ('shutdown -h +8','RuntimeMaxSec=180','MemoryMax=2147483648','MemorySwapMax=0','CPUQuota=200%','TasksMax=512',' 150 ', 'export OMP_NUM_THREADS=2'))
        embedded = re.search(r"<<'BOOTSTRAP'\n(.*?)BOOTSTRAP",shell,re.S)[1]
        namespace = {}; exec(embedded.split('\nsys.exit(bootstrap')[0],namespace)
        boot = namespace['bootstrap']
        checks.append('actual-generated-Bash/embedded-Python/480-180-150/2GiB-noSwap-threads2')
        # Execute the EXACT generated setup, including code authentication before imports.
        for mode in ('success','source','config','code','link','self-reference'):
            out = work/('bootstrap-'+mode); out.mkdir(); raw = io.BytesIO(); content = b'print(1)\n'
            with tarfile.open(fileobj=raw,mode='w') as archive:
                member = tarfile.TarInfo('scripts/source.py'); body = b'print(2)\n' if mode == 'code' else content
                member.size = len(body); archive.addfile(member,io.BytesIO(body))
                if mode == 'link':
                    member = tarfile.TarInfo('escape'); member.type = tarfile.SYMTYPE; member.linkname = '/tmp'; archive.addfile(member)
                if mode == 'self-reference':
                    member = tarfile.TarInfo(str(CONFIG)); member.size = 2; archive.addfile(member,io.BytesIO(b'{}'))
            archive = gzip.compress(raw.getvalue(),mtime=0); binding = dict(commit='a'*40,archive_sha256=sha(archive))
            cfg = dict(execution_source=binding,code_sha256={'scripts/source.py':sha(content)})
            body = encoded(cfg); key = 'research/native-library-check/sources/'+sha(archive)+'.tar.gz'
            early = dict(proof,source_archive_sha256=sha(archive),config_bytes=len(body),config_sha256=sha(body),code_identity_sha256=sha(encoded(cfg['code_sha256'])))
            sdk = SDK(); sdk.bodies = {key:archive,config_key(sha(body)):body}
            if mode == 'source': sdk.bodies[key] = b'!'+archive[1:]
            if mode == 'config': sdk.bodies[config_key(sha(body))] = b'!'+body[1:]
            with patch.object(boto3,'client',return_value=sdk):
                action = lambda: boot('setup',out,early,key,PREFIX+'a0001',ARTIFACTS)
                if mode == 'success': action(); assert read_json(out/'bootstrap-staging.json')['code_authenticated_before_import'] is True
                else: reject(action)
            assert sdk.closed and all(s.closed for s in sdk.streams)
        checks.append('actual-embedded-setup/source-config-code-link-selfreference-negatives')
        sdk_success = None
        for mode in ('success','missing','tamper','cli','cleanup','eligibility'):
            out = work/('stage-'+mode); out.mkdir(); worker_repo = out/'repo'
            shutil.copytree(source_repo,worker_repo)
            write(out/'config.json',config_body); write(out/'source-qualification.json',proof)
            sdk = SDK(mode if mode in ('missing','tamper') else 'success')
            sdk.bodies = {(config_key if f == 'head_roster' else ctl.config_key)(config[f]['sha256']):authorities[f] for f in authorities}
            real_remove = shutil.rmtree
            def remove(path,*args,**kwargs):
                if mode == 'cleanup' and Path(path).name == 'canary-temp': raise OSError('fixture cleanup failure')
                return real_remove(path,*args,**kwargs)
            abi = dict(architecture='x86_64',os_release=dict(ID='ubuntu',VERSION_ID='24.04'),libc='glibc',glibc_version='2.39',
                required_glibc='2.38',binary=runtime.identity(manifest['assets'][ctl.HTTP]),native_processes=0,
                python_executable=sys.executable,threads=dict.fromkeys(fixed.retained.THREAD_ENV,'2'))
            with ExitStack() as stack:
                for context in (patch.object(module,'WORK_ROOT',out),patch.object(module,'qualify',return_value=proof),
                        patch.object(module,'controller',return_value=ctl),patch.object(boto3,'client',return_value=sdk),
                        patch.object(fixed,'snapshot',side_effect=snap),patch.object(shutil,'rmtree',side_effect=remove),
                        patch.object(module,'eligibility',side_effect=AssertionError('ineligible') if mode == 'eligibility' else None,return_value=abi)):
                    stack.enter_context(context)
                if mode == 'cli': stack.enter_context(patch.object(module,'cli_smoke',side_effect=AssertionError('CLI failure')))
                result = stage(worker_repo,out,PREFIX+'a0001')
            assert result['infrastructure_status'] == ('INFRA_GO' if mode == 'success' else 'FAIL'), read_json(out/'failure.json')
            assert sdk.closed and not any(p.name.endswith('.part') for p in out.rglob('*'))
            if mode == 'success': sdk_success = sdk
        checks.append('real-stage/five-authority-downloads/imports/HEAD/selectedGET/CLI/cleanup/eligibility-failures')
        # The same stage artifacts go through actual terminal and collection.
        out = work/'stage-success'
        write(out/'bootstrap-staging.json',dict(source_authenticated=True,config_authenticated=True,code_authenticated_before_import=True,
            source_archive_removed=True,source_archive_sha256='b'*64,sdk=capability,scratch_bytes=10000))
        write(out/'profile.log',b'explicit mocked remote profile\n'); write(out/'profile-resources.txt',b'local fixture only\n')
        with (out/'bootstrap-sdk-ledger.jsonl').open('wb') as ledger:
            for operation,key in (('head_object',source_key),('get_object',source_key),('get_object',config_key(proof['config_sha256']))):
                ledger.write(encoded(dict(operation=operation,key=key,bucket=BUCKET,attempts=1,retries=0,error=None,status=200))+b'\n')
        sdk = SDK()
        with patch.object(boto3,'client',return_value=sdk),patch.object(urllib.request,'urlopen',side_effect=[io.BytesIO(b'token'),io.BytesIO(b'i-owned')]), \
                patch.dict(os.environ,CANARY_EXIT_CODE='0',CANARY_PHASE='complete'),redirect_stdout(io.StringIO()):
            assert boot('finish',out,proof,source_key,PREFIX+'a0001',ARTIFACTS) == 0
        terminal = json.loads(sdk.bodies[PREFIX+'a0001/terminal.json'])
        closed_out = work/'uploaded-snapshot'; closed_out.mkdir()
        for name in ARTIFACTS: write(closed_out/name,sdk.bodies[PREFIX+'a0001/artifacts/'+name])
        with patch.object(module,'controller',return_value=ctl): assert validate_closed(closed_out,proof,terminal)
        for field,value in (('infrastructure_status','GO'),('scientific_status','PASS'),('performance_measured',True),('exit_code',3)):
            changed = copy.deepcopy(terminal); changed[field] = value; reject(lambda: validate_closed(closed_out,proof,changed))
        nodes = {'0':dict(instance_id='i-owned')}
        launch = dict(instance_id='i-owned',nodes=nodes,prefix=PREFIX+'a0001',source_commit='a'*40,source_archive_sha256='b'*64)
        with patch.object(module,'ROOT',work/'collect'),patch.object(module,'preflight',return_value=proof),patch.object(module,'controller',return_value=ctl):
            dest = module.ROOT/'a0001'; dest.mkdir(parents=True)
            write(dest/'aws-launch.json',launch); write(dest/'aws-closeout.json',dict(state='terminated',nodes=nodes))
            write(dest/'aws-reservation.json',dict(source_commit='a'*40,source_archive_sha256='b'*64,qualification=proof,compute_cap_usd=COMPUTE_CAP,ebs_s3_allowance_usd=ALLOWANCE))
            assert collect(sdk,PREFIX+'a0001',dest,'i-owned','a'*40,'b'*64)['infrastructure_status'] == 'INFRA_GO'
            write(dest/'aws-closeout.json',dict(state='running',nodes=nodes)); calls = len(sdk.calls)
            reject(lambda: collect(sdk,PREFIX+'a0001',dest,'i-owned','a'*40,'b'*64)); assert len(sdk.calls) == calls
            write(dest/'aws-closeout.json',dict(state='terminated',nodes=nodes))
            key = PREFIX+'a0001/artifacts/cleanup.json'; sdk.bodies[key] = b'!'+sdk.bodies[key][1:]
            reject(lambda: collect(sdk,PREFIX+'a0001',dest,'i-owned','a'*40,'b'*64))
        checks.append('actual-INFRA_GO-terminal/full22-collector/body-terminal-tamper/closeout-before-any-GET')
        for mode in ('failure','cleanup','upload'):
            out = work/('terminal-'+mode); out.mkdir(); (out/'canary-temp').mkdir(); sdk = SDK('upload' if mode == 'upload' else 'success')
            with ExitStack() as stack:
                for context in (patch.object(boto3,'client',return_value=sdk),patch.object(urllib.request,'urlopen',side_effect=[io.BytesIO(b'token'),io.BytesIO(b'i-owned')]),
                        patch.dict(os.environ,CANARY_EXIT_CODE='7',CANARY_PHASE='source'),redirect_stdout(io.StringIO())): stack.enter_context(context)
                if mode == 'cleanup': stack.enter_context(patch.object(shutil,'rmtree',side_effect=OSError('cleanup refused')))
                action = lambda: boot('finish',out,proof,source_key,PREFIX+'a0001',ARTIFACTS)
                if mode == 'upload':
                    reject(action); status = read_json(out/'terminal.json')['exit_code']
                else: status = action()
            assert status == (7 if mode == 'failure' else 96) and sdk.closed
            if mode != 'upload':
                terminal_fail = json.loads(sdk.bodies[PREFIX+'a0001/terminal.json'])
                assert terminal_fail['infrastructure_status'] == 'FAIL' and validate_closed(out,proof,terminal_fail) is False
        checks.append('failed-terminal-retained/cleanup-failure/upload-failure/no-replacement')

        shared,_ = lifecycle()
        for mode in ('success','multi-ack','fsync','poll','wait','stale'):
            ec2,s3,session = Mock(),Mock(),Mock(); session.client.side_effect = [ec2,s3]
            ec2.describe_instances.return_value = {'Reservations':[]}
            ec2.describe_subnets.return_value = {'Subnets':[{'AvailabilityZone':'fixture-az'}]}
            timestamp = datetime.now(timezone.utc)-timedelta(seconds=301 if mode == 'stale' else 0)
            ec2.describe_spot_price_history.return_value = {'SpotPriceHistory':[dict(SpotPrice='0.1',Timestamp=timestamp)]}
            ids = ['i-owned','i-extra'] if mode == 'multi-ack' else ['i-owned']
            ec2.run_instances.return_value = {'Instances':[{'InstanceId':n} for n in ids]}; launch_call = ec2.run_instances
            events = []; ec2.terminate_instances.side_effect = lambda **kw:events.append('terminate')
            def waited(**kw):
                events.append('wait')
                if mode == 'wait': raise RuntimeError('wait failed')
            ec2.get_waiter.return_value.wait.side_effect = waited
            def collected(*args):
                assert events == ['terminate','wait']; events.append('collect')
                return dict(status='complete',phase='complete',exit_code=0,artifacts=dict.fromkeys(ARTIFACTS))
            launch_proof = dict(proof,source_archive_sha256=sha(gzip.compress(b'fixture archive',mtime=0)))
            with patch.object(module,'ROOT',work/('launch-'+mode)),patch.object(module,'preflight',return_value=launch_proof), \
                    patch.object(module,'stage_config'),patch.object(module,'user_data',return_value='fixture'), \
                    patch.object(shared.boto3,'Session',return_value=session),patch.object(subprocess,'check_output',side_effect=['','a'*40,b'fixture archive']), \
                    patch.object(subprocess,'run'),patch.object(shared.peer,'missing',return_value=True),patch.object(shared.peer,'put_if_absent'), \
                    patch.object(module,'poll',side_effect=InterruptedError('interrupted') if mode == 'poll' else None), \
                    patch.object(module,'collect',side_effect=collected) as collector, \
                    patch.object(os,'fsync',side_effect=OSError('fsync') if mode == 'fsync' else None),redirect_stdout(io.StringIO()):
                if mode in ('success','multi-ack'): main('a0001')
                else: reject(lambda: main('a0001'))
            if mode == 'stale': launch_call.assert_not_called(); collector.assert_not_called(); continue
            launch_call.assert_called_once()
            ec2.terminate_instances.assert_called_once_with(InstanceIds=ids)
            ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=ids)
            if mode == 'wait': collector.assert_not_called()
            else: assert events == ['terminate','wait','collect']
            params = launch_call.call_args.kwargs
            assert params['MetadataOptions'] == dict(HttpTokens='required',HttpEndpoint='enabled',HttpPutResponseHopLimit=1)
            assert params['InstanceType'] == INSTANCE_TYPE and params['ImageId'] == IMAGE_ID
            assert params['BlockDeviceMappings'] == [dict(DeviceName=ROOT_DEVICE_NAME,Ebs=dict(DeleteOnTermination=True,Encrypted=True,VolumeSize=80,VolumeType='gp3'))]
            reservation = read_json(work/('launch-'+mode)/'a0001/aws-reservation.json')
            assert reservation['compute_cap_usd'] == COMPUTE_CAP and reservation['ebs_s3_allowance_usd'] == ALLOWANCE and reservation['wall_seconds'] == WALL
        checks.append('shared-lifecycle/all-ACK-fsync-interrupt/sameIDs-terminate-wait/IMDSv2/caps/freshquote-negative')
        cloud.assert_not_called(); experiment.assert_not_called()
    signal.alarm(0)
    return dict(schema=SCHEMA+'-self-check',passed=True,checks=checks,elapsed_seconds=time.monotonic()-started,
        peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,code_paths=len(CODE),artifacts=len(ARTIFACTS),
        real_fixture=True,head_keys=51,selected_body_bytes=34661,network_calls=0,native_calls=0,offered_main_calls=0,
        mocked=['SDK/lifecycle/cgroup','temporary offered authority hydration/worker proof/ABI'],
        real_combined=real_combined,integration='Actual final combined source qualified with TEMP authorities; production freeze/cheap gate/cloud remain parent-owned',
        cli_controller_source=dict(path=str(controller_path),**artifact(controller_path)))


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['--contract']: print(json.dumps(contract(),indent=2))
        elif sys.argv[1:] == ['--prepare-authorities']: print(json.dumps(prepare_authorities(),sort_keys=True))
        elif sys.argv[1:] == ['--self-check']: print(json.dumps(self_check(),sort_keys=True))
        elif sys.argv[1:2] == ['--stage']:
            assert len(sys.argv) == 5, '--stage REPO WORKER_ROOT PREFIX'
            sys.exit(0 if stage(Path(sys.argv[2]),Path(sys.argv[3]),sys.argv[4])['infrastructure_status'] == 'INFRA_GO' else 2)
        else:
            assert len(sys.argv) == 2, 'usage: aNNNN | --contract | --self-check | --stage REPO WORKER_ROOT PREFIX'
            with open('/tmp/borsuk-fixed48-offered-canary-spot-launch.lock','a+') as lock:
                fcntl.flock(lock,fcntl.LOCK_EX | fcntl.LOCK_NB); main(sys.argv[1])
    except (Exception,KeyboardInterrupt) as error:
        print(type(error).__name__+': '+str(error),file=sys.stderr); sys.exit(2)
