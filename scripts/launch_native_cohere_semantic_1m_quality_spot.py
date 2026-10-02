"""Shared Spot lifecycle for one frozen CoHere FIRST1M quality arm.

CLI: aNNNN | --self-check. Root owns config/proofs/freeze/paid launch.
Config is worker.FIXED + authority_pending/code_sha256/refs/panel/inputs/binaries.
Original ReLAION refs/binaries are reused only for binary assurance.
CoHere inputs are original raw/sq8/order/root_manifest. panel contains directory,
verification (root-pinned construction-only checker report) and nine sealed files.
The original preparation remains FAIL. Quality/admission FAIL closes the arm.
"""
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

from scripts import run_native_cohere_semantic_1m_quality as worker

panel,ids = worker.panel,worker.ids
ROOT,CONFIG = worker.ROOT,worker.CONFIG
NAME = ''
SCHEMA = 'borsuk-cohere-semantic-1m-quality-spot-v1'
PREFIX = 'research/semantic-router/20261002/cohere-fresh1m-quality-'
TOKEN_PREFIX,TAG = 'cohere-semantic-1m-quality-','borsuk-cohere-semantic-1m-quality'
WALL = 9000
INSTANCE_TYPE,IMAGE_ID = panel.INSTANCE_TYPE,panel.IMAGE_ID
ROOT_DEVICE_NAME,SUBNET = panel.ROOT_DEVICE_NAME,panel.SUBNET
SPOT_MAX_USD_PER_HOUR,COMPUTE_CAP = .50,1.25
AWSCLI_VERSION,AWSCLI_SHA256 = panel.AWSCLI_VERSION,panel.AWSCLI_SHA256
CODE = worker.CODE
ARTIFACTS = ('test-resources.txt','run-closed.log',*('screen/'+n for n in worker.OUTPUTS))
TERMINAL_IDENTITIES = ('config_sha256','code_identity_sha256','refs_identity_sha256',
    'panel_identity_sha256','inputs_identity_sha256','artifact_roster_sha256',
    'source_identity_sha256','original_source_identity_sha256','campaign_schema',
    'awscli_version','awscli_sha256','dataset','original_preparation_campaign_status','binary_assurance_only_reused')


def qualify(base=Path('.')):
    base = Path(base).resolve()
    config_path = base/CONFIG
    _,proof = worker.qualify(config_path,worker.artifact(config_path)['sha256'],base)
    return dict(proof,campaign_schema=SCHEMA,artifact_roster_sha256=worker.sha(worker.encoded(ARTIFACTS)),
                awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256)


def preflight(base=Path('.')):
    proof = qualify(base)
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=base,text=True).strip(), 'dirty source'
    return proof


def user_data(commit,archive_sha,archive_key,prefix,qualification):
    assert re.fullmatch('[0-9a-f]{40}',commit) and re.fullmatch('[0-9a-f]{64}',archive_sha)
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}',prefix)
    assert qualification['config_path']==str(CONFIG) and qualification['campaign_schema']==SCHEMA
    _,bootstrap = ids.lifecycle()
    adapter = {k:qualification[k] for k in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG),native_binary={'key':'unused'},native_publisher={'key':'unused'})
    with patch.multiple(bootstrap,WALL=WALL,SCHEMA=SCHEMA,ARTIFACTS=ARTIFACTS,
                        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES),patch.object(bootstrap,'_offered',return_value=False):
        body = bootstrap.user_data(commit,archive_sha,archive_key,prefix,adapter)
    env = ' '.join('--setenv='+n+'=2' for n in worker.THREAD_ENV)
    command = f'''phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
phase=quality
systemd-run --unit=cohere-semantic-1m-quality --wait --pipe -p MemoryMax=4G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=7200 -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=LC_ALL=C \\
 --setenv=BORSUK_QUALITY_SOURCE_COMMIT={commit} --setenv=BORSUK_QUALITY_ARCHIVE_SHA256={archive_sha} \\
 {env} \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=5 7200 \\
 "$root/venv/bin/python" -m scripts.run_native_cohere_semantic_1m_quality "$root/repo/{CONFIG}" {qualification['config_sha256']} "$root/repo" "$root/screen"
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
'''
    start,end = body.index('phase=install\n'),body.index('phase=complete\n')
    body = body[:start]+command+body[end:]
    body = body.replace('/mnt/native-semantic-router-cold','/mnt/native-cohere-semantic-1m-quality')
    body = body.replace('python3-boto3 python3.12','python3.12 python3.12-venv')
    body = body.replace(", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256')",'')
    subprocess.run(['bash','-n'],input=body,text=True,check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0],'<terminal>','exec')
    assert len(body.encode())<16384
    assert all(n not in body for n in ('rustup','cargo','unused','--publish','repackage_semantic'))
    return body


def poll(ec2,s3,prefix,instance_id,started):
    return panel.poll(ec2,s3,prefix,instance_id,started)  # Same 9000s machine envelope.


def replay(out):
    """Authenticate completed receipts without repeating build or scoring."""
    out = Path(out)
    launch,closed,reservation,terminal = (json.loads((out/n).read_bytes()) for n in (
        'aws-launch.json','aws-closeout.json','aws-reservation.json','aws-terminal.json'))
    assert closed['state']=='terminated' and closed['nodes']==launch['nodes']
    assert terminal['instance_id']==launch['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert terminal['schema']==reservation['schema']==SCHEMA
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}',launch['prefix'])
    for key in ('source_commit','source_archive_sha256'):
        assert terminal[key]==reservation[key]==launch[key]
    proof = reservation['qualification']
    assert proof==qualify(Path(__file__).resolve().parents[1]), 'frozen local authority'
    for key in TERMINAL_IDENTITIES:
        assert terminal[key]==proof[key], 'terminal pin: '+key
    assert set(terminal['artifacts'])<=set(ARTIFACTS)
    for n,p in terminal['artifacts'].items():
        assert worker.artifact(out/n)==p, 'terminal body: '+n
    complete = terminal['phase']=='complete' and terminal['exit_code']==0
    assert terminal['status']==('complete' if complete else 'failed')
    if complete:
        assert terminal['original_exit_code']==0 and set(terminal['artifacts'])==set(ARTIFACTS)
        screen = out/'screen'
        assert worker.artifact(screen/'config.json')['sha256']==proof['config_sha256']
        source = {k:terminal[k] for k in ('source_commit','source_archive_sha256')}
        assert json.loads((screen/'source-qualification.json').read_bytes())==dict(
            (k,v) for k,v in dict(proof,**source).items()
            if k not in ('campaign_schema','artifact_roster_sha256','awscli_version','awscli_sha256'))
        counters = json.loads((screen/'quality-cgroup.json').read_bytes())
        worker.validate_cgroup(counters)
        report,clean,summary = (json.loads((screen/n).read_bytes()) for n in ('resources.json','cleanup.json','summary.json'))
        assert report['schema']=='borsuk-cohere-semantic-1m-quality-resources-v1'
        assert report['enforced_memory_max_bytes']==worker.MEMORY
        assert report['aggregate_memory_peak_bytes']==int(counters['after']['memory.peak'])
        assert report['memory_events']==counters['after']['memory.events'] and report['peak_is_enforced_limit'] is False
        assert clean['scratch_removed'] is True and clean['build_invocations']==clean['score_invocations']==1
        assert report['build_invocations']==report['score_invocations']==1 and 0<=report['wall_seconds']<=7200
        for n,limit in (('preparation',1800),('build',3600),('score',900)):
            assert 0<=report['stages'][n]['wall_seconds']<=limit
        assert report['stages']['build']['exit_status']==0
        assert report['stages']['score']['exit_status']==(0 if summary['status']=='PASS' else 2)
        digest = worker.artifact(screen/'scorer-config.json')['sha256']
        assert summary==worker.score_summary(screen/'records.jsonl',proof,digest)
        scorer=json.loads((screen/'scorer-config.json').read_bytes())
        assert scorer['dataset']=='CoHere' and scorer['profile']=='fresh1m'
        assert scorer['first']==0 and scorer['count']==64 and scorer['max_memory_bytes']==worker.PAYLOAD
        assert worker.artifact(screen/'generation-manifest.json')['sha256']==scorer['generation_root_sha256']
        ordinal = json.loads((screen/'sq8-ordinal-check.json').read_bytes())
        assert ordinal['rows_checked']==1_000_000 and ordinal['record_bytes']==780
        assert ordinal['id_matches_order'] is ordinal['complete_bijection'] is True
    return dict(executed=complete,physical_s3_measured=False,current_whole_tree_full_execution=False,
        original_preparation_campaign_status='FAIL',dataset='CoHere',
        ann_quality_measured=complete and summary.get('complete') is True,
        quality_status=summary['status'] if complete else 'UNKNOWN')


def collect(s3,prefix,out,instance_id,commit,digest):
    launch,closed = (json.loads((Path(out)/n).read_bytes()) for n in ('aws-launch.json','aws-closeout.json'))
    assert closed['state']=='terminated' and closed['nodes']==launch['nodes']
    assert launch['prefix']==prefix and launch['instance_id']==instance_id
    with patch.multiple(ids,SCHEMA=SCHEMA,ARTIFACTS=ARTIFACTS,replay=replay):
        return ids.collect(s3,prefix,out,instance_id,commit,digest)


def main(attempt):
    assert re.fullmatch(r'a[0-9]{4}',attempt), 'attempt must be aNNNN'
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        preflight()  # Authenticate all authorities before loading the SDK/lifecycle.
        shared,_ = ids.lifecycle()
        return shared.main(attempt,campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def self_check():
    """Real metadata/binary hashes; synthetic data and mocked SDK/processes only."""
    import copy
    import gzip
    import io
    import tempfile
    import resource
    import signal
    resource.setrlimit(resource.RLIMIT_AS,(200<<20,200<<20))
    signal.alarm(55)
    from types import ModuleType
    from unittest.mock import Mock
    module = sys.modules[__name__]
    here = Path(__file__).resolve().parents[1]
    def rejects(action):
        try:
            action()
        except (AssertionError,ValueError,KeyError,FileNotFoundError,FileExistsError):
            return
        raise AssertionError('invalid authority accepted')
    # Source/binary authentication is real, but no binary is executed and no
    # corpus/query/GT payload is opened by this local qualification check.
    old=json.loads(worker.read(here,worker.NATIVE_CONFIG))
    _,real=worker.native_authority(here,old['refs'],old['binaries'])
    assert real['source_file_count']==399 and real['native_rebuilt'] is False
    assert real['binary_sha256']=={n:p['sha256'] for n,p in old['binaries'].items()}
    rejects(lambda:worker.native_authority(here,old['refs'],dict(old['binaries'],
        builder=dict(old['binaries']['builder'],sha256='0'*64))))
    assert worker.corpus_inputs(here)['raw']['bytes']==3_072_000_000
    if not (here/CONFIG).exists():
        rejects(lambda:qualify(here))  # Actual pending root gate, before SDK.
    with tempfile.TemporaryDirectory() as directory:
        tmp = Path(directory); repo = tmp/'repo'; repo.mkdir()
        for n in CODE:
            target = repo/n; target.parent.mkdir(parents=True,exist_ok=True)
            target.write_bytes((here/n).read_bytes())
        config = dict(worker.FIXED,authority_pending=False,
            code_sha256={n:worker.artifact(repo/n)['sha256'] for n in CODE},
            refs={},panel={},inputs={},binaries={})
        target = repo/CONFIG; target.parent.mkdir(parents=True)
        target.write_bytes(worker.encoded(config))
        # Protocol/resource/source failures precede authority bodies and cloud.
        for n,value in [('authority_pending',True),('first',1000),('memory_bytes',worker.MEMORY+1),
                        ('threads',4),('score_invocations',2),('minimum_hits10',600),('dataset','ReLAION')]:
            changed = dict(config,**{n:value}); target.write_bytes(worker.encoded(changed))
            rejects(lambda:worker.qualify(target,worker.artifact(target)['sha256'],repo))
        target.write_bytes(worker.encoded(config))
        rejects(lambda:worker.qualify(target,'0'*64,repo))
        changed = dict(config,code_sha256=dict(config['code_sha256'],**{CODE[0]:'0'*64}))
        target.write_bytes(worker.encoded(changed))
        rejects(lambda:worker.qualify(target,worker.artifact(target)['sha256'],repo))
        target.unlink(); rejects(lambda:worker.qualify(target,'0'*64,repo))
        # The construction report is root-issued metadata. Its scope remains
        # independent of the failed preparation safety gate. All bodies below
        # are synthetic substitutions; no existing query or truth is decoded.
        original=json.loads(worker.read(here,worker.TERMINAL))
        observed=json.loads(worker.read(here,worker.READBACK))
        original_proof=json.loads((here/worker.ORIGINAL/'aws-reservation.json').read_bytes())['qualification']
        corpus=json.loads((here/worker.BASE/'cohere-panel-tools/metadata-authority.json').read_bytes())['corpus']
        inputs=worker.corpus_inputs(here)
        files={n:dict(path=str(worker.ORIGINAL/'screen'/n),**original['artifacts']['screen/'+n]) for n in worker.SEALED}
        evidence={str(worker.ORIGINAL/n):p for n,p in original['artifacts'].items()}
        evidence.update({str(worker.ORIGINAL/n):worker.artifact(here/worker.ORIGINAL/n)
            for n in ('aws-terminal.json','aws-reservation.json','aws-launch.json','aws-closeout.json')})
        evidence[worker.READBACK['path']]=worker.identity(worker.READBACK)
        structural=dict(schema='borsuk-cohere-closed-panel-construction-check-v1',
            scoped_construction_passed=True,original_cgroup_validator_rejected=True,
            original_campaign_status='FAIL',resource_qualification='failed',
            queries=64,dimensions=768,gt_k=100,truth_id_space='source ordinal',
            fresh_sealed_observations=observed['artifact_bodies_authenticated'],
            source={n:original[n] for n in ('source_commit','source_archive_sha256')},
            sealed_artifacts=files,evidence_bodies=evidence,validator_body=dict(bytes=1,sha256='a'*64),
            original_reference_bodies=original_proof['refs'],
            original_code_bodies={n:worker.artifact(here/n) for n in original_proof['code_sha256']},
            source_corpus=corpus,construction_inputs={n:worker.identity(inputs[k]) for n,k in
                (('source.raw','raw'),('source-order.u64','order'),('source-root.json','root_manifest'))},
            **dict.fromkeys(('prepared_campaign_passed','resource_qualification_passed','ann_quality_measured',
              'complete_historical_coverage','serving_or_build_qualified','root_authority_issued',
              'input_reconstruction_or_gt_reexecution','historical_etags_reconstructed'),False))
        authority=dict(directory=str(worker.ORIGINAL),verification=dict(
            path=str(ROOT/'construction-verification.json'),bytes=1,sha256='b'*64),files=files)
        def construction_read(_,p):
            value={authority['verification']['path']:structural,worker.TERMINAL['path']:original,
                   worker.READBACK['path']:observed,str(worker.ORIGINAL/'aws-reservation.json'):
                   {'qualification':original_proof}}
            return worker.encoded(value[p['path']]) if p['path'] in value else b'synthetic body'
        def construction_artifact(p):
            name=str(Path(p).relative_to(repo))
            if name=='scripts/check_cohere_closed_panel_construction.py':
                return structural['validator_body']
            if name==worker.HISTORICAL_ROOT:
                return worker.identity(inputs['root_manifest'])
            return evidence.get(name,structural['original_code_bodies'].get(name))
        with patch.object(worker,'read',side_effect=construction_read),\
             patch.object(worker,'artifact',side_effect=construction_artifact),\
             patch.object(worker,'corpus_inputs',return_value=inputs):
            worker.construction_authority(repo,authority,inputs)
            for name,value in [('original_campaign_status','PASS'),('prepared_campaign_passed',True),
                               ('scoped_construction_passed',False),('gt_k',10),('truth_id_space','physical ordinal')]:
                with patch.dict(structural,{name:value}):
                    rejects(lambda:worker.construction_authority(repo,authority,inputs))
            rejects(lambda:worker.construction_authority(repo,authority,dict(inputs,
                raw=dict(inputs['raw'],sha256='0'*64))))
            rejects(lambda:worker.construction_authority(repo,dict(authority,files={}),inputs))
        # Complete root qualification also checks CoHere inputs independently
        # of binary assurance, before any construction payload is admitted.
        good=dict(config,inputs=inputs)
        with patch.object(worker,'native_authority',return_value=({},dict(real))),\
             patch.object(worker,'corpus_inputs',return_value=inputs),\
             patch.object(worker,'construction_authority',return_value=structural):
            target.write_bytes(worker.encoded(good))
            assert worker.qualify(target,worker.artifact(target)['sha256'],repo)[1]['dataset']=='CoHere'
            target.write_bytes(worker.encoded(dict(good,inputs=dict(inputs,raw=dict(inputs['raw'],sha256='0'*64)))))
            rejects(lambda:worker.qualify(target,worker.artifact(target)['sha256'],repo))
        regular = repo/'body'; regular.write_bytes(b'synthetic')
        pointer = dict(path='body',**worker.artifact(regular))
        assert worker.read(repo,pointer)==b'synthetic'
        rejects(lambda:worker.read(repo,dict(pointer,sha256='0'*64)))
        rejects(lambda:worker.read(repo,dict(pointer,path='../escape')))
        order,sq8 = tmp/'order',tmp/'sq8'
        order.write_bytes(worker.native.struct.pack('<QQQ',2,0,1))
        sq8.write_bytes(b''.join(worker.native.struct.pack('<qf',i,1)+b'\1\2' for i in (2,0,1)))
        assert worker.ordinal_check(sq8,order,3,2)['complete_bijection']
        sq8.write_bytes(worker.native.struct.pack('<qf',0,1)+b'\1\2'+sq8.read_bytes()[14:])
        rejects(lambda:worker.ordinal_check(sq8,order,3,2))
        order.write_bytes(worker.native.struct.pack('<QQQ',0,0,1)); rejects(lambda:worker.ordinal_check(sq8,order,3,2))
        head = worker.local_head(sq8)
        stat = sq8.stat()
        assert head['etag']==f'"{stat.st_ino:x}-{stat.st_mtime_ns//1000:x}-{stat.st_size:x}"'
        os.utime(sq8,ns=(stat.st_atime_ns,stat.st_mtime_ns+1000))
        assert worker.local_head(sq8)['etag']!=head['etag']
        proof = dict(binary_sha256={'scorer':'1'*64},native_source_sha256={worker.SCORER_SOURCE:'2'*64,
            'crates/borsuk/src/semantic_unit_router.rs':'3'*64})
        events = [dict(phase='identity',schema='borsuk-semantic-router-scorer-result-v2',config_sha256='4'*64,
            binary_sha256='1'*64,scorer_source_sha256='2'*64,router_source_sha256='3'*64,physical_s3_measured=False),
            *[dict(phase='frozen_query',ordinal=i,truth_opened=False,returned_ids=list(range(100)),
                source_gets=10,source_bytes=1024,sq8_gets=3,sq8_bytes=78000) for i in range(64)],
            dict(phase='all_queries_frozen',count=64),
            *[dict(phase='evaluation',ordinal=i,returned_hits10=10,returned_hits100=99,
                **{n:dict(hits10=10,hits100=100) for n in ('nominated_units','page_closure',
                'source_scored_units','source_ranked_pages','sq8_admitted_ranges')}) for i in range(64)],
            dict(phase='terminal',summary=dict(complete=True,queries=64,hits10=640,hits100=6336,
                mean_returned_r10=1.0,mean_returned_r100=.99,status='PASS',physical_s3_measured=False))]
        records = tmp/'records'
        def save(value):
            records.write_text(''.join(json.dumps(e)+'\n' for e in value))
        save(events); assert worker.score_summary(records,proof,'4'*64)['status']=='PASS'
        for missing in (32,33):
            threshold=copy.deepcopy(events)
            for e in [e for e in threshold if e['phase']=='evaluation'][:missing]:
                e['returned_hits10']=9
            terminal=threshold[-1]['summary']
            terminal.update(hits10=640-missing,mean_returned_r10=(640-missing)/640,
                            status='PASS' if missing==32 else 'FAIL')
            save(threshold)
            assert worker.score_summary(records,proof,'4'*64)['status']==terminal['status']
        for changed in (events[:-1],events[:65]+events[66:],
                        [dict(events[0],binary_sha256='0'*64),*events[1:]],
                        [*events[:-1],dict(events[-1],summary=dict(events[-1]['summary'],hits10=608))],
                        [events[0],dict(events[1],truth_opened=True),*events[2:]],
                        [events[0],events[66],*events[1:]],
                        [*events[:-1],dict(events[-1],summary=dict(events[-1]['summary'],mean_returned_r100=.98))]):
            save(changed); rejects(lambda:worker.score_summary(records,proof,'4'*64))
        failed_events=[events[0],events[1],dict(phase='query_failure',ordinal=1,error='source cap',
            source_gets=128,source_bytes=64*1024**2,sq8_gets=0,sq8_bytes=0,stages=[]),
            dict(phase='terminal',summary=dict(complete=False,status='FAIL',error='source cap'))]
        save(failed_events)
        failed_summary=worker.score_summary(records,proof,'4'*64)
        assert failed_summary['raw_failed_arm_closed'] and failed_summary['ann_quality_measured'] is False
        assert failed_summary['queries_frozen']==1 and failed_summary['failure_decomposition']==[failed_events[2]]
        save([*failed_events[:-1],dict(failed_events[-1],summary=dict(complete=False,status='PASS',error='source cap'))])
        rejects(lambda:worker.score_summary(records,proof,'4'*64))
        counters = {'cgroup':'synthetic','observer_pid':42,'process_ids':[42],
            'memory.max':str(worker.MEMORY),'memory.peak':str(worker.MEMORY+4096),
            'memory.swap.max':'0','memory.swap.peak':'0','memory.events':'max 1\noom 0\noom_kill 0\n',
            'memory.swap.events':'max 0\n','cpu.max':'200000 100000','cpu.stat':'usage_usec 1',
            'pids.max':'512','pids.current':'1','pids.events':'max 0\n'}
        worker.validate_cgroup(dict(before=counters,after=counters,closed=True))
        for key,value in [('memory.max','2'),('memory.swap.peak','1'),('cpu.max','100000 100000'),
                          ('pids.max','513'),('memory.events','oom_kill 1\n'),('process_ids',[42,43])]:
            rejects(lambda:worker.validate_cgroup(dict(before=counters,after=dict(counters,**{key:value}),closed=True)))
        # Scoped original full execution plus the sole qualified scorer delta.
        original = {f'synthetic/{i}.rs':'a'*64 for i in range(398)}
        original[worker.SCORER_SOURCE]='b'*64
        current = dict(original,**{worker.SCORER_SOURCE:'c'*64})
        native_refs = {n:dict(path=p,bytes=1,sha256='d'*64) for n,p in worker.REF_PATHS.items()}
        binary_refs = {n:dict(path='synthetic/'+n,key='synthetic/'+n,bytes=1,sha256='e'*64) for n in ('builder','scorer')}
        values = dict(source_manifest=dict(source_sha256=original,source_identity_sha256=worker.native.source_identity(original)),
            scorer_source=dict(native_inventory=current,native_source_identity_sha256=worker.native.source_identity(current)),
            source_id_repair=dict(passed=True,helper_inputs_unchanged=True,fixed_protocol_unchanged=True),input_authorities={})
        for kind in ('implementation','full'):
            artifacts = {'native-source-manifest.json':worker.identity(native_refs['source_manifest'])}
            if kind=='implementation':
                artifacts['binaries/build_two_bit_generation']=worker.identity(binary_refs['builder'])
            values[kind+'_receipt']=dict(qualified=True,command_completed=True,command_started=True,
                source_unchanged=True,exit_status=0,gate_status=0,source_sha256=original,
                source_identity_sha256=worker.native.source_identity(original),artifacts=artifacts,
                command=['mock','test','--release','--locked','--workspace','--all-targets'])
            values[kind+'_terminal']=dict(phase='complete',status='complete',exit_code=0,original_exit_code=0,
                source_identity_sha256=worker.native.source_identity(original),artifacts=dict(artifacts,
                **{'workspace-receipt.json':worker.identity(native_refs[kind+'_receipt'])}))
            values[kind+'_verification']=dict(qualified=True,exit_status=0,oom_kills=0,swap_peak_bytes=0,
                actual_full_workspace_execution=kind=='full',source_identity_sha256=worker.native.source_identity(original))
        values['scorer_verification']=dict(status='VERIFIED',production_library_changed=False,
            committed_blob_matches_verified_source=True,full_workspace_execution_rerun=False,
            red_expected_runtime_failure=True,green_tests_passed=4,scorer_source_sha256='c'*64,
            native_source_identity_sha256=worker.native.source_identity(current),
            gates=[dict(name=n,status=101 if n=='red-a2' else 0,source_unchanged=True,stop_reason=None,
                        observed_swap_peak_bytes=0) for n in ('red-a2','green','release','clippy','workspace-test-build')],
            release_binary=dict(binary_bytes=1,binary_sha256='e'*64,scorer_source_sha256='c'*64))
        values['assets']=dict(schema='borsuk-semantic-1m-quality-assets-v1',bucket=worker.FIXED['bucket'],
            source_sha256=current,source_identity_sha256=worker.native.source_identity(current),source_file_count=399,
            binaries=binary_refs,production_library_unchanged=True,scorer_conditional_s3_readback_passed=True,
            full_execution_current_whole_tree_claim=False)
        inverse = {p:n for n,p in worker.REF_PATHS.items()}
        def native_read(_,p):
            return worker.encoded(values[inverse[p['path']]]) if p['path'] in inverse else b'x'
        with patch.object(worker.native,'read',side_effect=native_read),patch.object(worker.native,'source_hashes',return_value=current):
            _,scoped = worker.native.native_authority(repo,native_refs,binary_refs)
            assert scoped['original_full_workspace_execution_reused'] and not scoped['current_whole_tree_full_execution']
            changed = dict(binary_refs,builder=dict(binary_refs['builder'],sha256='0'*64))
            rejects(lambda:worker.native_authority(repo,native_refs,changed))
            with patch.object(worker.native,'source_hashes',return_value=dict(current,**{'synthetic/0.rs':'0'*64})):
                rejects(lambda:worker.native.native_authority(repo,native_refs,binary_refs))
            values['full_verification']['actual_full_workspace_execution']=False
            rejects(lambda:worker.native.native_authority(repo,native_refs,binary_refs))
            values['full_verification']['actual_full_workspace_execution']=True
        # Exercise the complete adapter flow while every process/body is synthetic.
        def synthetic_run(args,log,seconds,timing=None):
            with Path(log).open('a') as stream:
                stream.write('synthetic process output\n')
            if timing:
                Path(timing).write_text('Maximum resident set size (kbytes): 42\n')
            if args[0]=='aws':
                worker.write(Path(args[4]),blobs[args[3].rsplit('/',1)[-1]])
            elif args[0].endswith('/binaries/builder'):
                built=json.loads(Path(args[1]).read_bytes())
                assert built['semantic_profile']=='fresh1m' and built['discovery']=='semantic'
                assert built['low']==[0]*768 and built['step']==[1]*768
                assert built['sq8_etag']==worker.local_head(Path(built['sq8']))['etag']
                destination=Path(args[-1]); destination.mkdir(parents=True)
                worker.write(destination/'manifest.json',b'synthetic v8 manifest')
                Path(log).write_text(worker.artifact(destination/'manifest.json')['sha256']+'\n')
            else:
                assert args[0].endswith('/binaries/scorer')
                scorer=json.loads(Path(args[1]).read_bytes())
                assert scorer['dataset']=='CoHere'
                assert scorer['first']==0 and scorer['count']==64 and scorer['profile']=='fresh1m'
                assert Path(scorer['requests']['path']).read_bytes()==b'synthetic requests'
                assert Path(scorer['truth']['path']).read_bytes()==b'synthetic source ordinal truth'
                emitted=copy.deepcopy(events); emitted[0]['config_sha256']=args[2]
                Path(args[-1]).write_text(''.join(json.dumps(e)+'\n' for e in emitted))
            return dict(exit_status=0,wall_seconds=.001,process_peak_rss_kib=42,process_cleanup=True,command=args)
        blobs = dict(raw=b'raw',sq8=b'synthetic sq8',order=b'synthetic order',
            root_manifest=worker.encoded(dict(schema='borsuk-two-bit-generation-v4',
                canonical=dict(rows=1_000_000,dimensions=768),sq8_object_sha256=worker.sha(b'synthetic sq8'),
                sq8_object_key='mock/sq8',generation=1,base_epoch=0,low=[0]*768,step=[1]*768)),
            builder=b'synthetic qualified builder',scorer=b'synthetic qualified scorer')
        def body_pointer(n):
            return dict(key='mock/'+n,bytes=len(blobs[n]),sha256=worker.sha(blobs[n]))
        run_config=dict(config,inputs={n:body_pointer(n) for n in ('raw','sq8','order','root_manifest')},binaries={n:body_pointer(n) for n in ('builder','scorer')},
            refs={'input_authorities':{}},panel={'files':{'requests.jsonl':dict(name='requests',bytes=18,sha256=worker.sha(b'synthetic requests')),
            'truth.i64':dict(name='truth',bytes=30,sha256=worker.sha(b'synthetic source ordinal truth'))}})
        def runtime_read(_,p):
            return {'requests':b'synthetic requests','truth':b'synthetic source ordinal truth'}.get(p.get('name'),worker.encoded({'raw':{'sha256':'5'*64}}))
        true_artifact=worker.artifact
        def runtime_artifact(p):
            return worker.identity(run_config['inputs']['root_manifest']) if str(p).endswith(worker.HISTORICAL_ROOT) else true_artifact(p)
        def runtime_ordinals(sq8,order):
            assert Path(sq8).read_bytes()==blobs['sq8'] and Path(order).read_bytes()==blobs['order']
            return dict(rows_checked=1_000_000,record_bytes=780,id_matches_order=True,complete_bijection=True,
                        sq8=true_artifact(sq8),order=true_artifact(order))
        tools=dict(machine='x86_64',os_release=dict(ID='ubuntu',VERSION_ID='24.04'))
        environment=dict.fromkeys(worker.THREAD_ENV,'2')
        environment.update(BORSUK_QUALITY_SOURCE_COMMIT='0'*40,BORSUK_QUALITY_ARCHIVE_SHA256='1'*64)
        for mode in ('complete','failed','recall-fail','admission-fail','download-tamper','header-tamper'):
            fail=mode in ('failed','download-tamper','header-tamper')
            def execute(*args,**kwargs):
                if mode=='failed' and args[0][0].endswith('/binaries/builder'):
                    raise RuntimeError('synthetic build failure')
                result=synthetic_run(*args,**kwargs)
                if mode=='download-tamper' and args[0][0]=='aws':
                    Path(args[0][4]).write_bytes(b'tampered download')
                if mode=='header-tamper' and args[0][0]=='aws' and args[0][3].endswith('/root_manifest'):
                    value=json.loads(Path(args[0][4]).read_bytes()); value['schema']='borsuk-two-bit-generation-v3'
                    Path(args[0][4]).write_bytes(worker.encoded(value))
                if args[0][0].endswith('/binaries/scorer') and mode in ('recall-fail','admission-fail'):
                    emitted=copy.deepcopy(failed_events if mode=='admission-fail' else events)
                    emitted[0]['config_sha256']=args[0][2]
                    if mode=='recall-fail':
                        for e in [e for e in emitted if e['phase']=='evaluation']:
                            e['returned_hits10']=9
                        emitted[-1]['summary'].update(hits10=576,mean_returned_r10=.9,status='FAIL')
                    Path(args[0][-1]).write_text(''.join(json.dumps(e)+'\n' for e in emitted))
                    result['exit_status']=2
                return result
            target.write_bytes(worker.encoded(config))
            out=tmp/mode
            with patch.object(worker,'qualify',return_value=(run_config,proof)),patch.object(worker,'read',side_effect=runtime_read),\
                 patch.object(worker,'artifact',side_effect=runtime_artifact),patch.object(worker,'ordinal_check',side_effect=runtime_ordinals),\
                 patch.object(worker,'run_process',side_effect=execute),patch.object(ids,'capture_cgroup',return_value=counters),\
                 patch.object(ids,'tool_versions',return_value=tools),patch.object(worker.importlib.metadata,'version',side_effect=lambda n:worker.FIXED['versions'][n]),\
                 patch.dict(os.environ,environment):
                try:
                    result=worker.main(target,'4'*64,repo,out)
                except (RuntimeError,AssertionError):
                    assert fail
                else:
                    assert not fail and result['status']==('PASS' if mode=='complete' else 'FAIL')
            assert not (out/'scratch').exists()
            cleanup=json.loads((out/'cleanup.json').read_bytes())
            assert cleanup['scratch_removed']
            assert cleanup['build_invocations']==(0 if mode in ('download-tamper','header-tamper') else 1)
            assert cleanup['score_invocations']==(0 if fail else 1)
            report=json.loads((out/'resources.json').read_bytes())
            assert report['aggregate_memory_peak_bytes']==worker.MEMORY+4096
            assert report['peak_is_enforced_limit'] is False
        process = Mock(pid=1234)
        with patch.object(subprocess,'Popen',return_value=process),patch.object(worker.native.os,'killpg') as killed:
            process.wait.side_effect=[subprocess.TimeoutExpired('mock',1),0,0]
            try:
                worker.run_process(['mock'],tmp/'timeout.log',1)
            except subprocess.TimeoutExpired:
                pass
            else:
                raise AssertionError('timeout accepted')
            assert killed.call_args_list[0].args==(1234,worker.native.signal.SIGTERM)
            assert killed.call_args_list[-1].args==(1234,worker.native.signal.SIGKILL)
        sdk,botocore,exceptions = (ModuleType(n) for n in ('boto3','botocore','botocore.exceptions'))
        class SDKError(Exception):
            def __init__(self,**kwargs):
                super().__init__('mock SDK')
        for n in ('ClientError','EndpointConnectionError','ReadTimeoutError'):
            setattr(exceptions,n,SDKError)
        sdk.Session=Mock(side_effect=AssertionError('cloud forbidden')); botocore.exceptions=exceptions
        with patch.dict(sys.modules,{'boto3':sdk,'botocore':botocore,'botocore.exceptions':exceptions}):
            shared,_ = ids.lifecycle()
            proof = dict.fromkeys(TERMINAL_IDENTITIES,'1'*64)
            proof.update(config_path=str(CONFIG),campaign_schema=SCHEMA,awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256,
                dataset='CoHere',original_preparation_campaign_status='FAIL',binary_assurance_only_reused=True)
            body = user_data('0'*40,'1'*64,'source/key',PREFIX+'a0001',proof)
            assert 'MemoryMax=4G' in body and 'RuntimeMaxSec=7200' in body and '--on-active=9000s' in body
            assert 'MemorySwapMax=0' in body and 'CPUQuota=200%' in body and 'TasksMax=512' in body
            assert all('--setenv='+n+'=2' in body for n in worker.THREAD_ENV)
            command=body.split('systemd-run --unit=cohere-semantic-1m-quality',1)[1].split('\nfor name',1)[0]
            shell='systemd-run() { printf "%s\\n" "$@"; }; root=/synthetic; systemd-run --unit=cohere-semantic-1m-quality'+command
            argv=subprocess.check_output(['bash','-c',shell],text=True).splitlines()
            assert all('--setenv='+n+'=2' in argv for n in worker.THREAD_ENV) and '+' not in argv
            assert '-m' in argv and 'scripts.run_native_cohere_semantic_1m_quality' in argv
            with patch.object(module,'qualify',side_effect=FileNotFoundError('pending')):
                rejects(lambda:main('a0001'))
            sdk.Session.assert_not_called()
            # The real streaming collector/replay sees only synthetic artifacts.
            collected=tmp/'collection'; collected.mkdir()
            worker.shutil.copytree(tmp/'complete',collected/'screen')
            for n in ('test-resources.txt','run-closed.log'):
                worker.write(collected/n,b'synthetic whole service log\n')
            frozen=dict(proof,binary_sha256={'scorer':'1'*64},native_source_sha256={worker.SCORER_SOURCE:'2'*64,
                'crates/borsuk/src/semantic_unit_router.rs':'3'*64},
                config_sha256=worker.artifact(collected/'screen/config.json')['sha256'])
            source=dict(source_commit='0'*40,source_archive_sha256='1'*64)
            worker_source=dict((k,v) for k,v in dict(frozen,**source).items()
                if k not in ('campaign_schema','artifact_roster_sha256','awscli_version','awscli_sha256'))
            (collected/'screen/source-qualification.json').write_bytes(worker.encoded(worker_source))
            launch=dict(source,instance_id='i-synthetic',nodes={'0':{'instance_id':'i-synthetic'}},prefix=PREFIX+'a0001')
            reservation=dict(source,schema=SCHEMA,qualification=frozen)
            closed=dict(nodes=launch['nodes'],state='terminated')
            terminal=dict(source,**{n:frozen[n] for n in TERMINAL_IDENTITIES},schema=SCHEMA,
                instance_id='i-synthetic',status='complete',phase='complete',exit_code=0,original_exit_code=0,
                artifacts={n:worker.artifact(collected/n) for n in ARTIFACTS})
            for n,value in (('aws-launch.json',launch),('aws-reservation.json',reservation),('aws-closeout.json',closed)):
                worker.write(collected/n,value)
            s3=Mock()
            def get(**kwargs):
                key=kwargs['Key']
                data=worker.encoded(terminal) if key.endswith('/terminal.json') else (collected/key.split('/artifacts/',1)[1]).read_bytes()
                return {'Body':io.BytesIO(data)}
            s3.get_object.side_effect=get
            with patch.object(module,'qualify',return_value=frozen):
                assert collect(s3,launch['prefix'],collected,'i-synthetic','0'*40,'1'*64)['exit_code']==0
                assert s3.get_object.call_count==len(ARTIFACTS)+1
                assert replay(collected)['quality_status']=='PASS'
                for mode in ('recall-fail','admission-fail'):
                    for name in worker.OUTPUTS:
                        if name!='source-qualification.json':
                            worker.shutil.copyfile(tmp/mode/name,collected/'screen'/name)
                    terminal['artifacts']={n:worker.artifact(collected/n) for n in ARTIFACTS}
                    s3.reset_mock()
                    assert collect(s3,launch['prefix'],collected,'i-synthetic','0'*40,'1'*64)['exit_code']==0
                    checked=replay(collected)
                    assert checked['executed'] and checked['quality_status']=='FAIL'
                    assert checked['ann_quality_measured'] is (mode=='recall-fail')
                    assert checked['original_preparation_campaign_status']=='FAIL'
                closed['nodes']={'0':{'instance_id':'i-unowned'}}
                (collected/'aws-closeout.json').write_bytes(worker.encoded(closed))
                s3.reset_mock(); rejects(lambda:collect(s3,launch['prefix'],collected,'i-synthetic','0'*40,'1'*64))
                s3.get_object.assert_not_called()
            with panel.contextlib.redirect_stdout(io.StringIO()):
                shared.self_check(lifecycle_only=True)
            # Exercise this campaign through the shared main, including its
            # Spot defaults and ACK ownership when fsync fails after launch.
            from datetime import datetime, timezone
            for failure in (False,True):
                ec2,s3,session=Mock(),Mock(),Mock()
                session.client.side_effect=[ec2,s3]
                ec2.describe_instances.return_value={'Reservations':[]}
                ec2.describe_subnets.return_value={'Subnets':[{'AvailabilityZone':'mock-az'}]}
                ec2.describe_spot_price_history.return_value={'SpotPriceHistory':[
                    {'SpotPrice':'0.1','Timestamp':datetime.now(timezone.utc)}]}
                ec2.run_instances.return_value={'Instances':[{'InstanceId':'i-owned'},{'InstanceId':'i-extra'}]}
                calls=[]
                ec2.terminate_instances.side_effect=lambda **kw:calls.append('terminate')
                ec2.get_waiter.return_value.wait.side_effect=lambda **kw:calls.append('wait')
                def closed_collect(*args):
                    assert calls==['terminate','wait']; calls.append('collect')
                    return dict(status='complete',phase='complete',exit_code=0,artifacts=dict.fromkeys(ARTIFACTS,{}))
                destination=tmp/('ack-fsync-fail' if failure else 'ack-complete')
                with patch.object(module,'ROOT',destination),patch.object(module,'preflight',return_value=proof),\
                     patch.object(shared.boto3,'Session',return_value=session),\
                     patch.object(shared.subprocess,'check_output',side_effect=['','0'*40,b'synthetic source archive']),\
                     patch.object(shared.peer,'missing',return_value=True),patch.object(shared.peer,'put_if_absent'),\
                     patch.object(os,'fsync',side_effect=OSError('synthetic fsync failure') if failure else None),\
                     patch.object(module,'poll'),patch.object(module,'collect',side_effect=closed_collect),\
                     panel.contextlib.redirect_stdout(io.StringIO()):
                    try:
                        main('a0001')
                    except OSError:
                        assert failure
                    else:
                        assert not failure
                args=ec2.run_instances.call_args.kwargs
                assert args['InstanceType']==INSTANCE_TYPE and args['ImageId']==IMAGE_ID
                assert args['BlockDeviceMappings'][0]['DeviceName']==ROOT_DEVICE_NAME
                assert args['InstanceMarketOptions']['SpotOptions']['MaxPrice']=='0.50'
                ec2.run_instances.assert_called_once()
                ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-owned','i-extra'])
                ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-owned','i-extra'])
                reservation=json.loads((destination/'a0001/aws-reservation.json').read_bytes())
                assert reservation['schema']==SCHEMA and reservation['compute_cap_usd']==1.25
                assert reservation['ebs_s3_allowance_usd']==.15 and reservation['wall_seconds']==9000
                assert calls==['terminate','wait','collect']
    signal.alarm(0)
    print('PASS real native metadata/binary hashes; synthetic CoHere source/config/construction/ordinal/score/failure/resource checks and mocked ACK/fsync/termination-before-collection. Native quality/cloud UNRUN.')


if __name__ == '__main__':
    if sys.argv[1:]==['--self-check']:
        self_check()
    else:
        assert len(sys.argv)==2, 'usage: aNNNN | --self-check'
        with open('/tmp/borsuk-cohere-semantic-1m-quality-launch.lock','a+') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            main(sys.argv[1])
