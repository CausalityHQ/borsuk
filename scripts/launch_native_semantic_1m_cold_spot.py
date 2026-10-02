"""Root-frozen cold gate: aNNNN | --self-check | --measurement-self-check | --replay DIRECTORY."""
from pathlib import Path
import fcntl
import json
import os
import re
import subprocess
import sys
from unittest.mock import patch

if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')

from scripts import run_native_semantic_1m_cold as worker

panel,ids,shared_quality = worker.panel,worker.ids,worker.quality_spot
ROOT,CONFIG = worker.ROOT,worker.CONFIG
NAME = ''
SCHEMA = 'borsuk-semantic-1m-cold-spot-v1'
PREFIX = 'research/semantic-router/20261001/fresh1m-cold-'
TOKEN_PREFIX,TAG = 'semantic-1m-cold-','borsuk-semantic-1m-cold'
WALL = 9000
INSTANCE_TYPE,IMAGE_ID = panel.INSTANCE_TYPE,panel.IMAGE_ID
ROOT_DEVICE_NAME,SUBNET = panel.ROOT_DEVICE_NAME,panel.SUBNET
SPOT_MAX_USD_PER_HOUR,COMPUTE_CAP = .50,1.25
AWSCLI_VERSION,AWSCLI_SHA256 = panel.AWSCLI_VERSION,panel.AWSCLI_SHA256
CODE = worker.CODE
ARTIFACTS = ('test-resources.txt','run-closed.log',*('screen/'+n for n in worker.OUTPUTS))
TERMINAL_IDENTITIES = (*shared_quality.TERMINAL_IDENTITIES,'quality_reference_sha256','namespace_prefix')


def qualify(base=Path('.')):
    base=Path(base).resolve()
    _,proof=worker.qualify(base/CONFIG,worker.artifact(base/CONFIG)['sha256'],base)
    return dict(proof,campaign_schema=SCHEMA,artifact_roster_sha256=worker.sha(worker.encoded(ARTIFACTS)),
        awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256)


def preflight(base=Path('.')):
    proof=qualify(base)
    assert not subprocess.check_output(['git','status','--porcelain'],cwd=base,text=True).strip(), 'dirty source'
    return proof


def user_data(commit,archive_sha,archive_key,prefix,qualification):
    assert qualification['namespace_prefix']==prefix+'/native', 'campaign namespace binding'
    # Reuse the checked shared bootstrap/trap; replace only the bounded service command.
    with patch.multiple(shared_quality,CONFIG=CONFIG,SCHEMA=SCHEMA,PREFIX=PREFIX,
        ARTIFACTS=ARTIFACTS,TERMINAL_IDENTITIES=TERMINAL_IDENTITIES):
        body=shared_quality.user_data(commit,archive_sha,archive_key,prefix,qualification)
    body=body.replace('MemoryMax=1G','MemoryMax=8G -p IOAccounting=yes')
    body=body.replace('BORSUK_QUALITY_','BORSUK_COLD_')
    body=body.replace('semantic-1m-quality','semantic-1m-cold')
    body=body.replace('scripts.run_native_semantic_1m_quality','scripts.run_native_semantic_1m_cold')
    body=body.replace('if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi',
        'if [ "$name" = run-closed.log ]; then name=run.log; fi; test ! -L "$name"; test -f "$name"; if [ "$name" = screen/failures.jsonl ] || [ "$name" = screen/publication.log ]; then :; else test -s "$name"; fi')
    body=body.replace('"$root/venv/bin/python" -m scripts.run_native_semantic_1m_cold',
        'taskset -c 4-5 "$root/venv/bin/python" -m scripts.run_native_semantic_1m_cold')
    subprocess.run(['bash','-n'],input=body,text=True,check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0],'<terminal>','exec')
    assert len(body.encode())<16384 and 'MemoryMax=8G' in body
    return body


def poll(ec2,s3,prefix,instance_id,started):
    return panel.poll(ec2,s3,prefix,instance_id,started)


def archived_authority(repo, config_path, source_commit, source_archive_sha256, source_authority=None):
    """Validate original authority; live qualify/preflight still pin current code.

    Deployments authenticate the root's small source authority through NEW config.
    Local replay can independently recover those same pins from the original Git
    commit. Both routes reuse every existing native/quality/config qualifier gate.
    """
    repo, config_path = Path(repo).resolve(), Path(config_path).absolute()
    assert re.fullmatch('[0-9a-f]{40}',source_commit), 'archived source commit'
    assert re.fullmatch('[0-9a-f]{64}',source_archive_sha256), 'archived source archive'
    assert config_path.stat().st_size<=65536, 'small original config'
    config_identity=worker.artifact(config_path); config=json.loads(config_path.read_bytes())
    assert set(config['code_sha256'])==set(CODE), 'archived transitive code closure'
    authority=None
    if source_authority is None:
        def original(name):
            return subprocess.check_output(['git','show',source_commit+':'+name],cwd=repo,timeout=5)
        assert config_identity['sha256']==worker.sha(original(str(CONFIG))), 'archived original config'
        pins={n:worker.sha(original(n)) for n in CODE}
    else:
        authority=json.loads(worker.read(repo,source_authority))
        pins=authority['code_sha256']
    assert pins==config['code_sha256'], 'archived code pins'
    artifact=worker.artifact; code={repo/n:dict(sha256=d) for n,d in pins.items()}
    with patch.object(worker,'CONFIG',config_path), patch.object(worker,'artifact',
        side_effect=lambda p:code[Path(p)] if Path(p) in code else artifact(p)):
        _,proof=worker.qualify(config_path,config_identity['sha256'],repo)
    proof['config_path']=str(CONFIG)
    proof=dict(proof,campaign_schema=SCHEMA,artifact_roster_sha256=worker.sha(worker.encoded(ARTIFACTS)),
        awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256)
    if authority is not None:
        expected=dict(schema='borsuk-semantic-1m-cold-source-authority-v1',authority_pending=False,
            source_commit=source_commit,source_archive_sha256=source_archive_sha256,
            config=config_identity,code_sha256=pins,qualification=proof)
        assert worker.encoded(authority)==worker.encoded(expected), 'exact root archived source authority'
    return config,proof


def closed_artifacts(out, repo, source_authority=None):
    """Authenticate the terminated campaign without changing its historical status."""
    out=Path(out)
    for n in ('aws-launch.json','aws-closeout.json','aws-reservation.json','aws-terminal.json'):
        worker.artifact(out/n)  # Regular receipts, including their historical status.
    launch,closed,reservation,terminal=(json.loads((out/n).read_bytes()) for n in (
        'aws-launch.json','aws-closeout.json','aws-reservation.json','aws-terminal.json'))
    assert closed['state']=='terminated' and closed['nodes']==launch['nodes']
    assert terminal['instance_id']==launch['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    assert terminal['schema']==reservation['schema']==SCHEMA
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}',launch['prefix'])
    for k in ('source_commit','source_archive_sha256'):
        assert terminal[k]==reservation[k]==launch[k]
    config,proof=archived_authority(repo,out/'screen/config.json',terminal['source_commit'],
        terminal['source_archive_sha256'],source_authority)
    assert worker.encoded(proof)==worker.encoded(reservation['qualification']), 'archived frozen authority'
    assert proof['namespace_prefix']==launch['prefix']+'/native'
    for k in TERMINAL_IDENTITIES: assert terminal[k]==proof[k], 'terminal authority: '+k
    assert set(terminal['artifacts'])<=set(ARTIFACTS)
    for n,p in terminal['artifacts'].items():
        assert set(p)=={'bytes','sha256'} and type(p['bytes']) is int and p['bytes']>=0, 'terminal body length: '+n
        assert worker.artifact(out/n)==p, 'terminal body: '+n
    assert all(type(terminal[n]) is int for n in ('exit_code','original_exit_code')), 'integer campaign exits'
    complete=terminal['phase']=='complete' and terminal['exit_code']==0
    assert terminal['status']==('complete' if complete else 'failed')
    return config,proof,terminal,complete


def replay(out):
    """Campaign replay preserves execution FAIL, even with valid measurements."""
    out=Path(out); repo=Path(__file__).resolve().parents[1]
    config,proof,terminal,complete=closed_artifacts(out,repo)
    if not complete:
        if 'screen/records.jsonl' in terminal['artifacts']:
            rows=[json.loads(line) for line in (out/'screen/records.jsonl').read_bytes().splitlines()]
            assert [r['query_ordinal'] for r in rows]==list(range(64))
        return dict(executed=False,execution_gate_passed=False)
    assert terminal['original_exit_code']==0
    return validate_measurement(out,repo,config,proof,terminal)


def validate_measurement(out,repo,config,proof,terminal):
    """The complete replay gates, shared by campaign replay and qualification."""
    assert set(terminal['artifacts'])==set(ARTIFACTS), 'complete measurement roster'
    assert all(p['bytes']>0 or n in ('screen/publication.log','screen/failures.jsonl')
        for n,p in terminal['artifacts'].items()), 'required measurement bodies nonempty'
    screen=out/'screen'
    assert worker.artifact(screen/'config.json')['sha256']==proof['config_sha256']
    source={k:terminal[k] for k in ('source_commit','source_archive_sha256')}
    assert json.loads((screen/'source-qualification.json').read_bytes())==dict(
        (k,v) for k,v in dict(proof,**source).items()
        if k not in ('campaign_schema','artifact_roster_sha256','awscli_version','awscli_sha256'))
    counters=json.loads((screen/'cold-cgroup.json').read_bytes()); worker.validate_cgroup(counters)
    report,clean,summary=(json.loads((screen/n).read_bytes()) for n in ('resources.json','cleanup.json','summary.json'))
    assert clean['valid'] is clean['scratch_removed'] is clean['observer_stopped'] is True
    assert clean['build_invocations']==clean['publication_invocations']==1 and clean['cold_invocations']==64
    assert report['build_invocations']==report['publication_invocations']==1 and report['cold_invocations']==64
    assert 0<=report['wall_seconds']<=7200
    for n,limit in (('preparation',1800),('build',3600),('publication',1800),('cold',900)):
        assert 0<=report['stages'][n]['wall_seconds']<=limit
    assert report['stages']['build']['exit_status']==report['stages']['publication']['exit_status']==0
    ordinal=json.loads((screen/'sq8-ordinal-check.json').read_bytes())
    assert ordinal['rows_checked']==1_000_000 and ordinal['record_bytes']==780
    assert ordinal['id_matches_order'] is ordinal['complete_bijection'] is True
    assert report['phase_admission']==dict(builder_memory_bytes=worker.PAYLOAD,
        publisher_memory_bytes=config['publisher_memory_bytes'],native_memory_bytes=worker.NATIVE,
        server_query_slots=worker.SERVER_QUERY_SLOTS,native_budget_model=worker.native_budget_model(),
        memory_bytes=worker.MEMORY,swap_bytes=0), 'phase memory admission'
    qconfig=json.loads(worker.read(repo,config['quality_config']))
    original=worker.read(repo,qconfig['panel']['files']['screen/requests.jsonl'])
    derivative=worker.publisher_requests(original)
    assert (screen/'publisher-requests.jsonl').read_bytes()==derivative
    expected_derivative=dict(original=worker.identity(qconfig['panel']['files']['screen/requests.jsonl']),
        derivative=worker.artifact(screen/'publisher-requests.jsonl'),only_ordinal_key_changed=True,f32_bits_equal=True)
    assert json.loads((screen/'request-derivative.json').read_bytes())==expected_derivative
    truth_body=worker.read(repo,qconfig['panel']['files']['screen/truth.i64'])
    truth=[list(worker.struct.unpack_from('<100q',truth_body,q*800)) for q in range(64)]
    refs=worker.source_reference(worker.read(repo,config['quality_run']['files']['screen/records.jsonl']))
    publication=json.loads((screen/'publication.json').read_bytes()); arm=publication['arm']
    worker.validate_roster(arm)
    assert report['native_budget_model']==worker.native_budget_model(arm['metadata_files'])
    assert arm['indexes']=={'10':config['namespace_prefix']}
    assert publication['head']['root_sha256']==arm['authority']['root_sha256']
    head=worker.base64.b64decode(publication['head_body_base64'],validate=True)
    assert json.loads(head)==publication['head'] and dict(bytes=len(head),sha256=worker.sha(head))==arm['head_file']
    remote=json.loads((screen/'remote-sq8.json').read_bytes())
    assert publication['remote_sq8']==remote and remote['backend']=='AmazonS3' and remote['remote_readback_authenticated'] is True
    assert remote['key']==config['namespace_prefix']+'/objects/'+qconfig['inputs']['sq8']['sha256']
    assert worker.identity(remote)==worker.identity(qconfig['inputs']['sq8'])
    manifest=publication['manifest']
    assert manifest['sq8_object_key']==remote['key'] and manifest['sq8_etag']==remote['etag']
    builder=json.loads((screen/'builder-config.json').read_bytes())
    assert builder['sq8_object_key']==remote['key'] and builder['sq8_etag']==remote['etag']
    assert publication['reference']==worker.artifact(screen/'publication-reference.jsonl')
    assert publication['validation']==worker.publication_reference(screen/'publication-reference.jsonl',arm,refs)
    records=[json.loads(line) for line in (screen/'records.jsonl').read_bytes().splitlines()]
    assert (screen/'records.jsonl').read_bytes()==b''.join(worker.encoded(r)+b'\n' for r in records), 'canonical closed ledger'
    requests=[worker.http_request(json.loads(line)['query'],arm['authority']) for line in derivative.splitlines()]
    expected=[dict(r,ids=r['ids'][:10]) for r in refs]
    start_ns=summary['serial_start_ns']; end_ns=summary['serial_end_ns']
    assert summary==worker.reduce_records(records,arm,requests,expected,truth,start_ns,end_ns)
    assert summary['execution_gate_passed'] is True
    assert (screen/'failures.jsonl').read_bytes()==b''
    return dict(executed=True,execution_gate_passed=True,quality_gate_passed=summary['quality_gate_passed'],
        context_p90_attained=summary['context_p90_attained'],
        historical_campaign_status={k:terminal[k] for k in ('status','phase','exit_code','original_exit_code')},
        measurement_gate_passed=True,quality_gate=summary['quality_gate_passed'],
        context444miss=not summary['context_p90_attained'],valid_calls=summary['valid_calls'],
        actual_http_attempts=summary['actual_http_attempts'],identity_gate=True,resource_gate=True,cleanup_gate=True)


def qualify_measurement(out, repo=None, source_authority=None):
    """Qualify all closed measurements regardless of historical campaign status.

    This does not authorize use of a failed campaign. Offered qualification must
    separately authenticate the root's narrowly bound fail disposition.
    """
    out=Path(out); repo=Path(repo or Path(__file__).resolve().parents[1]).resolve()
    config,proof,terminal,_=closed_artifacts(out,repo,source_authority)
    result=validate_measurement(out,repo,config,proof,terminal)
    # Measurement execution and campaign execution are distinct facts.
    result.pop('executed'); result.pop('execution_gate_passed')
    return dict(result,qualification=proof,cold_config=config)


def fail_disposition(repo, run, pointer, checked):
    """Only a source-bound, root-frozen bootstrap log failure permits reuse."""
    status=checked['historical_campaign_status']
    if status==dict(status='complete',phase='complete',exit_code=0,original_exit_code=0):
        assert pointer is None, 'complete campaign needs no fail disposition'
        return
    assert status==dict(status='failed',phase='quality',exit_code=1,original_exit_code=1), 'unsupported historical failure'
    assert pointer is not None, 'root fail disposition required'
    terminal=json.loads(worker.read(repo,run['files']['aws-terminal.json']))
    expected=dict(schema='borsuk-semantic-1m-cold-fail-disposition-v1',authority_pending=False,
        decision='reuse-authenticated-closed-measurement-for-offered',reason='bootstrap-empty-publication-log',
        cold_directory=run['directory'],source_commit=terminal['source_commit'],
        source_archive_sha256=terminal['source_archive_sha256'],
        terminal=worker.identity(run['files']['aws-terminal.json']),config=worker.identity(run['files']['screen/config.json']),
        artifact_roster_sha256=terminal['artifact_roster_sha256'],artifacts=terminal['artifacts'],
        historical_campaign_status=status,service_exit_status=0,failed_presence_artifact='screen/publication.log',
        only_bootstrap_presence_failure=True)
    assert worker.encoded(json.loads(worker.read(repo,pointer)))==worker.encoded(expected), 'exact root fail disposition'
    log=worker.read(repo,run['files']['run-closed.log']).decode()
    markers=('Running as unit: semantic-1m-cold.service','Finished with result: success',
        'Main processes terminated with: code=exited/status=0')
    assert all(log.splitlines().count(m)==1 for m in markers), 'unique successful Python service'
    assert log.count('Running as unit:')==log.count('Finished with result:')==log.count('Main processes terminated with:')==1
    assert log.index(markers[0])<log.index(markers[1])<log.index(markers[2])
    assert terminal['artifacts']['screen/publication.log']==dict(bytes=0,sha256=worker.sha(b'')), 'sole empty bootstrap log'
    assert all(p['bytes']>0 for n,p in terminal['artifacts'].items()
        if n not in ('screen/publication.log','screen/failures.jsonl')), 'no other presence failure'
    assert all(checked[n] is True for n in ('measurement_gate_passed','quality_gate','identity_gate','resource_gate','cleanup_gate'))
    assert checked['valid_calls']==checked['actual_http_attempts']==64


def collect(s3,prefix,out,instance_id,commit,digest):
    launch,closed=(json.loads((Path(out)/n).read_bytes()) for n in ('aws-launch.json','aws-closeout.json'))
    assert closed['state']=='terminated' and closed['nodes']==launch['nodes']
    assert launch['prefix']==prefix and launch['instance_id']==instance_id
    with patch.multiple(ids,SCHEMA=SCHEMA,ARTIFACTS=ARTIFACTS,replay=replay):
        return ids.collect(s3,prefix,out,instance_id,commit,digest)


def main(attempt):
    assert re.fullmatch(r'a[0-9]{4}',attempt)
    before=Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        preflight()
        lifecycle,_=ids.lifecycle()
        return lifecycle.main(attempt,campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def measurement_self_check():
    """Executed Bash rule and authenticated actual a0005/tamper regressions only."""
    import copy
    import tempfile
    from unittest.mock import Mock
    started=worker.time.monotonic(); repo=Path(__file__).resolve().parents[1]
    fixture=repo/ROOT/'a0005'
    def rejects(action):
        try: action()
        except (AssertionError,ValueError,KeyError,FileNotFoundError,RuntimeError): return
        raise AssertionError('invalid closed measurement accepted')
    checked=qualify_measurement(fixture,repo)
    assert checked['historical_campaign_status']==dict(status='failed',phase='quality',exit_code=1,original_exit_code=1)
    assert all(checked[n] is True for n in ('measurement_gate_passed','quality_gate','context444miss','identity_gate','resource_gate','cleanup_gate'))
    assert checked['valid_calls']==checked['actual_http_attempts']==64
    assert replay(fixture)==dict(executed=False,execution_gate_passed=False)
    # Historical cold authority is usable, but it cannot authorize NEW code.
    rejects(lambda:worker.qualify(repo/CONFIG,worker.artifact(repo/CONFIG)['sha256'],repo))
    reservation=json.loads((fixture/'aws-reservation.json').read_bytes())
    terminal=json.loads((fixture/'aws-terminal.json').read_bytes())
    body=user_data(terminal['source_commit'],terminal['source_archive_sha256'],'source/key',PREFIX+'a0005',reservation['qualification'])
    rule=body.rsplit('for name in $ARTIFACT_NAMES; do\n',1)[1].split('\ndone',1)[0]
    assert 'aws' not in rule and 'test -f' in rule, 'test only final presence loop'
    old='if [ "$name" = screen/failures.jsonl ]; then test -f "$name"; elif [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi'
    with tempfile.TemporaryDirectory() as directory:
        tmp=Path(directory); mock=tmp/'presence'; mock.mkdir()
        for n in ARTIFACTS:
            p=mock/('run.log' if n=='run-closed.log' else n); p.parent.mkdir(parents=True,exist_ok=True)
            p.write_bytes(b'' if n in ('screen/publication.log','screen/failures.jsonl') else b'required')
        def bash(clause, names=ARTIFACTS):
            script='for name in '+ ' '.join(names)+'; do\n'+clause+'\ndone'
            return subprocess.run(['bash','-ec',script],cwd=mock,timeout=5,check=False).returncode
        assert bash(old)==1, 'RED legacy rejects zero-byte publisher log'
        assert bash(rule)==0, 'GREEN only empty publication/failure logs allowed'
        for n in ARTIFACTS:
            p=mock/('run.log' if n=='run-closed.log' else n); original=p.read_bytes()
            p.unlink(); assert bash(rule)!=0, 'absent accepted: '+n
            p.mkdir(); assert bash(rule)!=0, 'directory accepted: '+n
            p.rmdir(); p.symlink_to(mock/'screen/build.log'); assert bash(rule)!=0, 'symlink accepted: '+n
            p.unlink(); p.write_bytes(b'')
            assert (bash(rule)==0)==(n in ('screen/publication.log','screen/failures.jsonl')), 'required empty accepted: '+n
            p.write_bytes(original)
        copied=tmp/'closed'; worker.shutil.copytree(fixture,copied)
        def change_json(name,change,authenticate=False):
            path=copied/name; original=path.read_bytes(); tpath=copied/'aws-terminal.json'; oldterminal=tpath.read_bytes()
            value=json.loads(original); change(value); path.write_bytes(worker.encoded(value)+b'\n')
            if authenticate:
                receipt=json.loads(oldterminal); receipt['artifacts'][name]=worker.artifact(path)
                tpath.write_bytes(worker.encoded(receipt))
            try: rejects(lambda:qualify_measurement(copied,repo))
            finally: path.write_bytes(original); tpath.write_bytes(oldterminal)
        module=sys.modules[__name__]
        with patch.object(module,'archived_authority',return_value=(checked['cold_config'],checked['qualification'])):
            for n in ARTIFACTS:
                p=copied/n; original=p.read_bytes(); p.write_bytes(original+b'tamper')
                rejects(lambda:qualify_measurement(copied,repo)); p.write_bytes(original)
            for name,change in (
                ('aws-closeout.json',lambda v:v.update(state='running')),
                ('aws-launch.json',lambda v:v.update(instance_id='i-unrelated')),
                ('aws-terminal.json',lambda v:v.update(source_archive_sha256='0'*64)),
                ('aws-terminal.json',lambda v:v['artifacts'].pop('screen/publication.log')),
                ('aws-reservation.json',lambda v:v['qualification'].update(code_identity_sha256='0'*64))):
                change_json(name,change)
            for name,change in (
                ('screen/summary.json',lambda v:v.update(returned_hits=623)),
                ('screen/resources.json',lambda v:v.update(cold_invocations=63)),
                ('screen/resources.json',lambda v:v.update(wall_seconds=7201)),
                ('screen/resources.json',lambda v:v['phase_admission'].update(native_memory_bytes=worker.PAYLOAD)),
                ('screen/cold-cgroup.json',lambda v:v['after'].update({'memory.swap.peak':'1\n'})),
                ('screen/cleanup.json',lambda v:v.update(valid=False)),
                ('screen/publication.json',lambda v:v['head'].update(root_sha256='0'*64))):
                change_json(name,change,True)
            path=copied/'screen/records.jsonl'; original=path.read_bytes(); rows=[json.loads(line) for line in original.splitlines()]
            rows[0]['response']['ids'][0]=999999
            path.write_bytes(b''.join(worker.encoded(r)+b'\n' for r in rows))
            oldterminal=(copied/'aws-terminal.json').read_bytes(); receipt=json.loads(oldterminal)
            receipt['artifacts']['screen/records.jsonl']=worker.artifact(path)
            (copied/'aws-terminal.json').write_bytes(worker.encoded(receipt))
            rejects(lambda:qualify_measurement(copied,repo))
            path.write_bytes(original); (copied/'aws-terminal.json').write_bytes(oldterminal)
        # Local archived Git authority also rejects a tampered config with rehashed receipt.
        changed=tmp/'config.json'; changed.write_bytes((fixture/'screen/config.json').read_bytes())
        config=json.loads(changed.read_bytes()); config['code_sha256'][CODE[0]]='0'*64
        changed.write_bytes(worker.encoded(config))
        rejects(lambda:archived_authority(repo,changed,terminal['source_commit'],terminal['source_archive_sha256']))
        # Exercise the offered qualification with the real closed measurement.
        from scripts import run_native_semantic_1m_offered as offered_worker
        sandbox=tmp/'repo'; sandbox.mkdir()
        for n in offered_worker.CODE:
            target=sandbox/n; target.parent.mkdir(parents=True,exist_ok=True); target.write_bytes((repo/n).read_bytes())
        worker.shutil.copytree(fixture,sandbox/ROOT/'a0005')
        target=sandbox/offered_worker.CONFIG; target.parent.mkdir(parents=True)
        run=dict(directory=str(ROOT/'a0005'),files={n:dict(path=str(ROOT/'a0005'/n),**worker.artifact(fixture/n)) for n in offered_worker.COLD_ROSTER})
        disposition=dict(schema='borsuk-semantic-1m-cold-fail-disposition-v1',authority_pending=False,
            decision='reuse-authenticated-closed-measurement-for-offered',reason='bootstrap-empty-publication-log',
            cold_directory=run['directory'],source_commit=terminal['source_commit'],source_archive_sha256=terminal['source_archive_sha256'],
            terminal=worker.identity(run['files']['aws-terminal.json']),config=worker.identity(run['files']['screen/config.json']),
            artifact_roster_sha256=terminal['artifact_roster_sha256'],artifacts=terminal['artifacts'],
            historical_campaign_status=checked['historical_campaign_status'],service_exit_status=0,
            failed_presence_artifact='screen/publication.log',only_bootstrap_presence_failure=True)
        dpath=tmp/'disposition.json'; dpath.write_bytes(worker.encoded(disposition))
        price=tmp/'prices.json'; price.write_bytes(worker.encoded(dict(provenance='synthetic-only')))
        config=dict(offered_worker.FIXED,authority_pending=False,code_sha256={n:worker.artifact(sandbox/n)['sha256'] for n in offered_worker.CODE},
            cold_config=run['files']['screen/config.json'],cold_run=run,cold_fail_disposition=dict(path='mock-disposition',**worker.artifact(dpath)),
            measurement_prefix=offered_worker.PREFIX+'a0001',prices=dict(path='mock-prices',**worker.artifact(price)))
        source_authority=dict(schema='borsuk-semantic-1m-cold-source-authority-v1',authority_pending=False,
            source_commit=terminal['source_commit'],source_archive_sha256=terminal['source_archive_sha256'],
            config=worker.identity(run['files']['screen/config.json']),code_sha256=checked['cold_config']['code_sha256'],qualification=checked['qualification'])
        source=tmp/'source-authority.json'; source.write_bytes(worker.encoded(source_authority))
        config['cold_source_authority']=dict(path='mock-source-authority',**worker.artifact(source))
        real_read=worker.read
        def read(_,pointer):
            if pointer['path']=='mock-source-authority':
                value=source.read_bytes(); assert dict(bytes=len(value),sha256=worker.sha(value))==worker.identity(pointer); return value
            if pointer['path']=='mock-disposition':
                value=dpath.read_bytes(); assert dict(bytes=len(value),sha256=worker.sha(value))==worker.identity(pointer); return value
            if pointer['path']=='mock-prices': return price.read_bytes()
            return real_read(repo,pointer)
        # A deployed git archive has no history; the frozen bridge needs no Git.
        with patch.object(worker,'read',side_effect=read),patch.object(subprocess,'check_output',side_effect=AssertionError('Git forbidden')):
            assert qualify_measurement(fixture,repo,config['cold_source_authority'])==checked
            for field,value in (('authority_pending',True),('source_commit','0'*40),('source_archive_sha256','0'*64),
                ('config',dict(bytes=1,sha256='0'*64)),('qualification',{}),('code_sha256',{})):
                bad=dict(source_authority,**{field:value}); source.write_bytes(worker.encoded(bad))
                pointer=dict(path='mock-source-authority',**worker.artifact(source))
                rejects(lambda:qualify_measurement(fixture,repo,pointer))
            source.write_bytes(worker.encoded(source_authority))
        def qualify_config():
            target.write_bytes(worker.encoded(config))
            return offered_worker.qualify(target,worker.artifact(target)['sha256'],sandbox)
        with patch.object(offered_worker,'read',side_effect=read),patch.object(worker,'read',side_effect=read), \
            patch.object(offered_worker.cold_spot,'qualify_measurement',side_effect=lambda *args:checked),patch.object(offered_worker.subprocess,'Popen',side_effect=AssertionError('native forbidden')):
            _,proof=qualify_config()
            assert proof['cold_terminal_sha256']==run['files']['aws-terminal.json']['sha256']
            saved=copy.deepcopy(config)
            for pointer in (None,dict(config['cold_fail_disposition'],sha256='0'*64)):
                config['cold_fail_disposition']=pointer; rejects(qualify_config)
            config=copy.deepcopy(saved)
            config['cold_source_authority']=None; rejects(qualify_config); config=copy.deepcopy(saved)
            for field,value in (('authority_pending',True),('source_commit','0'*40),('service_exit_status',1),
                ('only_bootstrap_presence_failure',False),('only_bootstrap_presence_failure',1),('reason','other-failure')):
                bad=dict(disposition,**{field:value}); dpath.write_bytes(worker.encoded(bad))
                config['cold_fail_disposition']=dict(path='mock-disposition',**worker.artifact(dpath)); rejects(qualify_config)
            dpath.write_bytes(worker.encoded(disposition)); config=copy.deepcopy(saved)
            for field,value in (('valid_calls',63),('quality_gate',False),('cleanup_gate',False),('resource_gate',False)):
                bad=dict(checked,**{field:value})
                with patch.object(offered_worker.cold_spot,'qualify_measurement',return_value=bad): rejects(qualify_config)
            config['code_sha256'][offered_worker.CODE[0]]='0'*64; rejects(qualify_config)
        # Authenticated service logs must have one unambiguous normal exit.
        for log in (b'Main processes terminated with: code=exited/status=0\n',
            (fixture/'run-closed.log').read_bytes()+b'Main processes terminated with: code=exited/status=1\n'):
            def bad_log(_,p): return log if p['path'].endswith('/run-closed.log') else read(_,p)
            with patch.object(worker,'read',side_effect=bad_log): rejects(lambda:fail_disposition(repo,run,saved['cold_fail_disposition'],checked))
    assert worker.time.monotonic()-started<55
    assert worker.resource.getrusage(worker.resource.RUSAGE_SELF).ru_maxrss*1024<=200*1024**2
    print('PASS Bash RED/GREEN and all25 absent/directory/symlink/required-empty negatives; actual a0005 measurement/GT/summary/resources/cleanup PASS, campaign FAIL and context444miss preserved; body/identity/source/reducer/resource/cleanup/disposition/code-pin tamper negatives; native/cloud UNRUN')


def self_check():
    started=worker.time.monotonic()
    measurement_self_check()
    import copy
    import io
    import tempfile
    from types import ModuleType
    from unittest.mock import Mock
    module=sys.modules[__name__]
    here=Path(__file__).resolve().parents[1]
    def rejects(action):
        try: action()
        except (AssertionError,ValueError,KeyError,FileNotFoundError,FileExistsError,RuntimeError): return
        raise AssertionError('invalid evidence accepted')
    assert worker.FIXED['native_memory_bytes']==1024**3, 'four-slot HTTP admission'
    worst=worker.native_budget_model()
    assert worst['server_query_slots']==worker.FIXED['server_query_slots']==4
    assert worst['modeled_remote_payload_bytes']==938423992<worker.NATIVE
    assert worst['source_payload_per_slot']==134217728 and worst['query_payload_per_slot']==57276032
    source = b'{"ordinal":0,"query":[1.0,-0.0]}\n'
    derivative = worker.publisher_requests(source, rows=1, dimensions=2)
    assert derivative == b'{"query":[1.0,-0.0],"query_ordinal":0}\n'
    assert worker.scientific_gate(607)['quality_gate_passed'] is False
    assert worker.scientific_gate(608)['quality_gate_passed'] is True
    for body in (b'{"ordinal":1,"query":[1,0]}',b'{"query_ordinal":0,"query":[1,0]}',
                 b'{"ordinal":0,"query":[1]}',b'{"ordinal":false,"query":[1,0]}',
                 b'{"ordinal":0,"query":[1,NaN]}'):
        rejects(lambda:worker.publisher_requests(body,rows=1,dimensions=2))
    assert worker.http_request([1,-0.0],dict(root_sha256='a'*64,generation=1,control_epoch=1))==worker.encoded(
        dict(query=[1,-0.0],k=10,root_sha256='a'*64,generation=1,control_epoch=1))
    with tempfile.TemporaryDirectory() as directory:
        tmp=Path(directory)
        # The closed native publisher is the contract fixture, not the mock below.
        closed=here/ROOT/'a0003'
        terminal=json.loads((closed/'aws-terminal.json').read_bytes())
        launch=json.loads((closed/'aws-launch.json').read_bytes())
        closeout=json.loads((closed/'aws-closeout.json').read_bytes())
        reservation=json.loads((closed/'aws-reservation.json').read_bytes())
        assert closeout['state']=='terminated' and closeout['nodes']==launch['nodes']
        assert terminal['instance_id']==launch['instance_id']==launch['nodes']['0']['instance_id']
        for k in ('source_commit','source_archive_sha256'):
            assert terminal[k]==launch[k]==reservation[k]
        assert terminal['status']=='failed' and terminal['original_exit_code']==terminal['exit_code']==1
        fixture=closed/'screen/publication-reference.jsonl'
        assert worker.artifact(fixture)==terminal['artifacts']['screen/publication-reference.jsonl']==dict(
            bytes=96415,sha256='1d47c84a05e33e40537310fbe5e7c197cedca4c9cb481f31ad38e73991d02e63')
        def closed_read(name):
            pointer=dict(path=str((closed/name).relative_to(here)),**terminal['artifacts'][name])
            return worker.read(here,pointer)
        config_body=closed_read('screen/config.json')
        frozen_config=json.loads(config_body)
        assert worker.sha(config_body)==terminal['config_sha256']==reservation['qualification']['config_sha256']
        builder=json.loads(closed_read('screen/builder-config.json'))
        root_sha=closed_read('screen/build.log').decode().strip()
        assert re.fullmatch('[0-9a-f]{64}',root_sha)
        assert builder['generation']==frozen_config['generation']==1
        assert builder['base_epoch']==frozen_config['base_epoch']==0
        published=dict(authority=dict(root_sha256=root_sha,generation=builder['generation'],control_epoch=1))
        quality_body=worker.read(here,frozen_config['quality_run']['files']['screen/records.jsonl'])
        assert worker.sha(quality_body)==terminal['quality_reference_sha256']
        references=worker.source_reference(quality_body)
        events=[json.loads(line) for line in fixture.read_bytes().splitlines()]
        validated=worker.publication_reference(fixture,published,references)
        assert validated==dict(validated_queries=64,top_k=100,source_scorer_parity=True,
            publish_wall_ns=events[0]['publish_wall_ns'],remote_open_wall_ns=events[0]['remote_open_wall_ns'])
        qconfig=json.loads(worker.read(here,frozen_config['quality_config']))
        truth_body=worker.read(here,qconfig['panel']['files']['screen/truth.i64'])
        truth=[worker.struct.unpack_from('<100q',truth_body,q*800) for q in range(64)]
        assert sum(len(set(r['ids'][:10])&set(t[:10])) for r,t in zip(events[1:-1],truth))==624
        assert sum(len(set(r['ids'])&set(t)) for r,t in zip(events[1:-1],truth))==6077
        verification=json.loads((closed/'verification.json').read_bytes())
        assert verification['status']=='EXECUTION_FAILED' and verification['cold_invocations']==0
        assert json.loads(closed_read('screen/resources.json'))['cold_invocations']==0
        candidate=tmp/'publication-reference.jsonl'
        def reject_events(bad,refs=references,authority=published):
            candidate.write_bytes(b''.join(worker.encoded(r)+b'\n' for r in bad))
            rejects(lambda:worker.publication_reference(candidate,authority,refs))
        for bad in (events[:-1],events+[events[-1]],events[:1]+events[2:],events[1:],
                    [events[-1],*events[1:-1],events[0]]):
            reject_events(bad)
        for index,field,values in (
            (0,'phase',('query',)),(0,'top_k',(10,True,100.0)),
            (0,'declared_panel_count',(63,64.0)),
            (1,'phase',('query-error','startup','summary')),
            (1,'query_ordinal',(1,False,0.0)),(64,'query_ordinal',(62,)),
            (65,'phase',('query',)),(65,'count',(63,True,64.0,'64')),
            (65,'top_k',(10,True,100.0,'100')),
            (65,'measurement_wall_ns',(-1,True,1.0,'1',None))):
            for value in values:
                bad=copy.deepcopy(events); bad[index][field]=value; reject_events(bad)
        for field in ('count','top_k','measurement_wall_ns'):
            bad=copy.deepcopy(events); del bad[-1][field]; reject_events(bad)
        for field in worker.PARITY:
            bad=copy.deepcopy(events); bad[1][field]=None; reject_events(bad)
        for field in published['authority']:
            bad=copy.deepcopy(events); bad[0][field]=None; reject_events(bad)
        reject_events(events,refs=references[:-1])
        reject_events(events,authority=dict(authority=dict(published['authority'],root_sha256='0'*64)))
        for measurement in (0,1):
            good=copy.deepcopy(events); good[-1]['measurement_wall_ns']=measurement
            candidate.write_bytes(b''.join(worker.encoded(r)+b'\n' for r in good))
            assert worker.publication_reference(candidate,published,references)==validated
        # Existing source/binary/config/ordinal/run_process tests remain shared.
        with worker.panel.contextlib.redirect_stdout(io.StringIO()): shared_quality.self_check()
        local=tmp/'sq8'; local.write_bytes(b'packed')
        head=dict(ContentLength=6,ETag='"remote-s3-etag"')
        assert worker.sq8_authority(head,local,worker.artifact(local))==head['ETag']
        rejects(lambda:worker.sq8_authority(dict(head,ETag=worker.quality.local_head(local)['etag']),local,worker.artifact(local)))
        rejects(lambda:worker.sq8_authority(dict(head,ContentLength=5),local,worker.artifact(local)))
        proof=dict(qualified=True,authority_pending=False,production_library_unchanged=True,
            current_whole_tree_full_execution=False,source_file_count=399,native_source_sha256={'a.rs':'a'*64},
            source_identity_sha256='b'*64,original_full_source_identity_sha256='c'*64,
            binary_bytes=6,binary_sha256=worker.sha(b'packed'),release_status=0,clippy_status=0,
            workspace_test_compilation_status=0,oom_kills=0,swap_peak_bytes=0)
        qproof=dict(native_source_sha256=proof['native_source_sha256'],source_identity_sha256='b'*64,
            original_source_identity_sha256='c'*64)
        with patch.object(worker,'read',return_value=worker.encoded(proof)):
            worker.native_proof(tmp,{},worker.artifact(local),qproof)
            rejects(lambda:worker.native_proof(tmp,{},dict(worker.artifact(local),sha256='0'*64),qproof))
            rejects(lambda:worker.native_proof(tmp,{},worker.artifact(local),dict(qproof,native_source_sha256={})))
        for field,value in (('authority_pending',True),('release_status',None),('clippy_status',False),
            ('workspace_test_compilation_status',101),('oom_kills',1),('swap_peak_bytes',1)):
            with patch.object(worker,'read',return_value=worker.encoded(dict(proof,**{field:value}))):
                rejects(lambda:worker.native_proof(tmp,{},worker.artifact(local),qproof))
        # Dynamic v8 startup geometry, independent of the old 100k/v7 checker.
        sizes=[2000,1000,3907*32,500,3072,1_000_000,1024,125000]
        arm=dict(discovery='semantic',authority=dict(root_sha256='a'*64,generation=1,control_epoch=1),
            indexes={'10':PREFIX+'a0001/native'},metadata_files=dict(zip(worker.STARTUP,sizes)),
            metadata_sha256={n:'a'*64 for n in worker.STARTUP},head_file=dict(bytes=200,sha256='a'*64),
            leaf_object=dict(bytes=31_250*1540,sha256='a'*64))
        worker.validate_roster(json.loads(worker.encoded(arm)))
        metadata=[]
        for i,(name,size) in enumerate(zip(worker.STARTUP,sizes)):
            metadata.append(dict(name=name,bytes=size,chunks=1,metadata_wave=0 if i==0 else (i-1)//4+1,
                metadata_wave_wall_ns=10,logical_head_requests=int(name not in worker.EXACT),logical_get_requests=1,
                head_wall_ns=int(name not in worker.EXACT),get_wall_ns=2,stream_wall_ns=3,write_wall_ns=1,
                payload_buffer_bound_bytes=size))
        startup=dict(metadata=metadata,staging_wall_ns=30,decode_wall_ns=2,source_head_requests=1,
            router_head_requests=1,source_head_wall_ns=2,router_head_wall_ns=2)
        opened=worker.validate_startup(startup,arm,40)
        assert opened['metadata_objects']==8 and opened['logical_metadata_head_requests']==3
        for kind in ('v7','payload','length','hash','wave','head','buffer','order'):
            bad_arm=copy.deepcopy(arm); bad=copy.deepcopy(startup)
            if kind=='v7': bad_arm['metadata_files']['router/manifest.json']=bad_arm['metadata_files'].pop('router/root.bin')
            elif kind=='payload': bad_arm['metadata_files']['plane/records.bin']=100
            elif kind=='length': bad_arm['metadata_files']['page_digests.bin']=12512
            elif kind=='hash': bad_arm['metadata_sha256']['manifest.json']='0'*64
            elif kind=='wave': bad['metadata'][1]['metadata_wave']=0
            elif kind=='head': bad['metadata'][6]['logical_head_requests']=1
            elif kind=='buffer': bad['metadata'][6]['payload_buffer_bound_bytes']+=1
            else: bad['metadata'][1],bad['metadata'][2]=bad['metadata'][2],bad['metadata'][1]
            rejects(lambda:worker.validate_startup(bad,bad_arm,40))
        def transport_report(methods,payload):
            return dict(schema='borsuk-native-transport-v1',scope='process_all_native_s3_readers',per_query_delta=False,
                attempt_measurement='submitted HttpService calls, not confirmed wire or S3 requests',method_order=worker.stats.METHODS,
                status_counts_format='[http_status,count] nonzero entries',
                payload_measurement='consumed response data frames, including unauthenticated payload',
                unknown=worker.stats.UNKNOWN,dropped_error_body_consumed_bytes=0,
                totals=dict(attempts=sum(methods),method_counts=methods,status_counts=[[200,sum(methods)]],
                    transport_failures=0,stream_failures=0,dropped_error_bodies=0,consumed_payload_bytes=payload))
        ready=dict(phase='ready',authority=arm['authority'],listen='127.0.0.1:8080',remote_open_stats=startup,
            remote_open_wall_ns=40,head_read_wall_ns=10,
            transport=transport_report([12,5,1]+[0]*7,sum(sizes)+200+2000+300))
        expected=dict(query_ordinal=0,ids=list(range(10)),ranges=[[0,199680]],planned_bytes=199680,
            submitted_gets=1,verified_bytes=199680,failed_gets=0,source_submitted_gets=1,
            source_verified_bytes=100,source_failed_gets=0,router_submitted_gets=8,router_verified_bytes=500,router_failed_gets=0)
        stages={n:dict(start_ns=i*10+1,end_ns=i*10+8) for i,n in enumerate(worker.stats.STAGES)}
        stages['leaf_peak_inflight']=8
        response=dict(expected,authority=arm['authority'],query_stages=stages,native_wall_ns=40,
            transport=transport_report([22,5,1]+[0]*7,ready['transport']['totals']['consumed_payload_bytes']+200280))
        worker.validate_query(response,arm,expected,list(range(100)))
        accounting=worker.transport(ready,response,arm)
        assert accounting['imds_token_PUTs']==1 and accounting['imds_credential_GETs']==2
        assert accounting['inferred_credential_consumed_bytes']==300
        for kind in ('GET','HEAD','PUT','credential','root','queryGET','payload','counter','status'):
            h,r=copy.deepcopy(ready),copy.deepcopy(response)
            if kind in ('GET','HEAD','PUT'):
                index={'GET':0,'HEAD':1,'PUT':2}[kind]; m=h['transport']['totals']['method_counts']; m[index]+=1
                h['transport']['totals']['attempts']+=1; h['transport']['totals']['status_counts'][0][1]+=1
            elif kind=='credential': h['transport']['totals']['consumed_payload_bytes']-=300
            elif kind=='root': h['authority']['root_sha256']='0'*64
            elif kind=='queryGET':
                r['transport']['totals']['method_counts'][0]+=1; r['transport']['totals']['attempts']+=1
                r['transport']['totals']['status_counts'][0][1]+=1
            elif kind=='payload': r['transport']['totals']['consumed_payload_bytes']+=1
            elif kind=='counter': r['submitted_gets']+=1
            else: r['transport']['totals']['status_counts'][0][0]=500
            rejects(lambda:worker.transport(h,r,arm))
        high_rss='User time (seconds): 0.01\nSystem time (seconds): 0.01\nMaximum resident set size (kbytes): 786432\n'
        assert worker.telemetry.resources(high_rss,worker.NATIVE)['rss_peak_bytes']==768*1024**2
        rejects(lambda:worker.telemetry.resources(high_rss,worker.PAYLOAD))
        time_log='User time (seconds): 0.01\nSystem time (seconds): 0.01\nMaximum resident set size (kbytes): 42\n'
        # The real cold_call executes against a mocked socket/native process.
        process=Mock(pid=1234,returncode=143); process.poll.return_value=None
        def spawn(args,**kwargs):
            assert kwargs['env']['BORSUK_NATIVE_MEMORY_BYTES']==str(worker.NATIVE)
            assert args[args.index('prlimit')+1]=='--as=4294967296:4294967296'
            kwargs['stdout'].write(json.dumps(ready)+'\n'); kwargs['stdout'].flush()
            Path(args[3]).write_text(time_log)
            return process
        def killpg(pid,sig):
            assert pid==1234
            if sig==0: raise ProcessLookupError()
        cpu_before=dict(pid=1235,start_ticks=100,user_ticks=1,system_ticks=1,address_space_limit_bytes=4*1024**3,cpu_affinity=[0,1,2,3])
        cpu_after=dict(cpu_before,user_ticks=2)
        request=worker.http_request([1]*768,arm['authority'])
        with patch.object(worker.subprocess,'Popen',side_effect=spawn),patch.object(worker.cold.http.client,'HTTPConnection') as connection,\
             patch.object(worker.cold,'post',return_value=(200,worker.encoded(response))),\
             patch.object(worker,'native_cpu',side_effect=[cpu_before,cpu_after]),\
             patch.object(worker,'cold_stop',return_value=dict(intentional_stop=True,returncode=143)),\
             patch.object(worker.os,'killpg',side_effect=killpg):
            row=worker.measured_call('synthetic',dict(bucket='mock',region='mock'),arm,request,expected,list(range(100)))
        assert row['outcome']=='success',row
        assert row['completed_ns']==row['first_wire_completed_ns'] and row['native_close']['process_group_closed']
        assert row['query_cpu']['ticks']==1 and row['http_attempts']==1
        process.poll.return_value=124
        with patch.object(worker.subprocess,'Popen',side_effect=spawn),\
             patch.object(worker,'cold_stop',return_value=dict(intentional_stop=False,returncode=124)),\
             patch.object(worker.os,'killpg',side_effect=killpg):
            startup_failure=worker.measured_call('synthetic',dict(bucket='mock',region='mock'),arm,request,expected,list(range(100)))
        assert startup_failure['outcome']=='failed' and startup_failure['http_attempts']==0
        assert startup_failure['native_process_started'] and startup_failure['native_server_log']
        assert startup_failure['native_close']['process_group_closed']
        process.poll.return_value=None
        with patch.object(worker,'cold_stop',return_value=dict(intentional_stop=False,returncode=124)),\
             patch.object(worker.os,'killpg',side_effect=killpg) as killed:
            assert worker.close_native(process)['process_group_closed']
        assert [c.args for c in killed.call_args_list]==[(1234,worker.signal.SIGTERM),(1234,worker.signal.SIGKILL),(1234,0)]
        # Independent reduction sees exact 64 rows; incomplete or changed ledgers fail.
        records=[]; cursor=0
        for q in range(64):
            r=copy.deepcopy(row); r.update(query_ordinal=q,started_ns=cursor,completed_ns=cursor+1000000,
                terminal_ns=cursor+2000000,cold_start_to_first_http_response_ns=1000000)
            records.append(r); cursor+=3000000
        requests=[request]*64; refs=[dict(expected,query_ordinal=q) for q in range(64)]; truth=[list(range(100))]*64
        reduced=worker.reduce_records(records,arm,requests,refs,truth,0,cursor)
        assert reduced['execution_gate_passed'] and reduced['returned_hits']==640
        assert reduced['context_p90_attained'] and reduced['sustainable_qps']=='UNMEASURED'
        for kind in ('ledger','cleanup','resource','raw','identity','timing'):
            bad=copy.deepcopy(records)
            if kind=='ledger': bad.pop()
            elif kind=='cleanup': bad[1]['native_close']['process_group_closed']=False
            elif kind=='resource': bad[1]['resources']['rss_peak_bytes']=worker.NATIVE+1
            elif kind=='raw': bad[1]['raw_response_base64']=worker.base64.b64encode(b'{}').decode()
            elif kind=='identity': bad[1]['request_sha256']='0'*64
            else: bad[1]['started_ns']=0
            rejects(lambda:worker.reduce_records(bad,arm,requests,refs,truth,0,cursor))
        failed=dict(records[0],outcome='failed',error_type='RuntimeError',error='synthetic failure')
        broken=[failed,*worker.aborted_rows('fatal call',1)]
        assert not worker.reduce_records(broken,arm,requests,refs,truth,0,cursor)['execution_gate_passed']
        # Config pending and identity rejects precede all source/cloud access.
        repo=tmp/'repo'; repo.mkdir()
        for name in CODE:
            path=repo/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes((here/name).read_bytes())
        config=dict(worker.FIXED,authority_pending=True,code_sha256={n:worker.artifact(repo/n)['sha256'] for n in CODE},
            quality_config={},quality_run={},native_proofs={},binaries={},namespace_prefix=PREFIX+'a0001/native')
        target=repo/CONFIG; target.parent.mkdir(parents=True); target.write_bytes(worker.encoded(config))
        rejects(lambda:worker.qualify(target,worker.artifact(target)['sha256'],repo))
        rejects(lambda:worker.qualify(target,'0'*64,repo))
        config['authority_pending']=False
        for field,value in (('rows',100000),('k',100),('memory_bytes',worker.MEMORY+1),
                            ('publisher_memory_bytes',512 * 1024**2),('native_memory_bytes',worker.PAYLOAD),
                            ('server_query_slots',1),('server_query_slots',True),('native_budget_model',{})):
            target.write_bytes(worker.encoded(dict(config,**{field:value})))
            with patch.object(worker,'read') as reads,patch.object(worker.quality,'run_process') as processes:
                rejects(lambda:worker.qualify(target,worker.artifact(target)['sha256'],repo))
                reads.assert_not_called(); processes.assert_not_called()
        changed=dict(config,code_sha256=dict(config['code_sha256'],**{CODE[0]:'0'*64}))
        target.write_bytes(worker.encoded(changed)); rejects(lambda:worker.qualify(target,worker.artifact(target)['sha256'],repo))
        # Reject undersized qualification before creating outputs or processes.
        target.write_bytes(worker.encoded(dict(config,native_memory_bytes=worker.PAYLOAD)))
        out=tmp/'fatal'
        with patch.object(worker.subprocess,'Popen') as processes,patch.object(worker,'read') as reads:
            rejects(lambda:worker.main(target,worker.artifact(target)['sha256'],repo,out))
            processes.assert_not_called(); reads.assert_not_called()
        assert not out.exists()
        # Full runtime and closed offline replay, with synthetic transfers/processes.
        request_body=b''.join(worker.encoded(dict(ordinal=q,query=[1.0]*768))+b'\n' for q in range(64))
        frozen_queries=[dict(phase='frozen_query',ordinal=q,returned_ids=list(range(100)),
            ranges=[dict(start=0,end=199680)],sq8_gets=1,sq8_bytes=199680,source_gets=1,source_bytes=100,
            leaf_gets=8,leaf_bytes=500) for q in range(64)]
        quality_records=b''.join(worker.encoded(r)+b'\n' for r in frozen_queries)
        coefficient_body=worker.encoded(dict(rows=1_000_000,dimensions=768,raw_sha256='5'*64,
            sq8_sha256=worker.sha(b'packed'),low=[0]*768,step=[1]*768))
        blobs=dict(parquet=b'parquet',sq8=b'packed',order=b'order',coefficients=coefficient_body,
            builder=b'builder',http=b'http',publisher=b'publisher')
        def object_pointer(name):
            return dict(key='synthetic/'+name,bytes=len(blobs[name]),sha256=worker.sha(blobs[name]))
        def authority_pointer(name,body): return dict(path=name,bytes=len(body),sha256=worker.sha(body))
        requests_pointer=authority_pointer('synthetic/requests.jsonl',request_body)
        records_pointer=authority_pointer('synthetic/records.jsonl',quality_records)
        qconfig=dict(inputs={n:object_pointer('coefficients' if n=='builder' else n) for n in ('parquet','sq8','order','builder')},
            binaries={'builder':object_pointer('builder')},refs={'input_authorities':dict(path='synthetic/raw-authority.json')},
            panel={'files':{'screen/requests.jsonl':requests_pointer}})
        runconfig=dict(config,quality_config=dict(path=str(worker.quality.CONFIG),bytes=1,sha256='6'*64),
            quality_run=dict(files={'screen/records.jsonl':records_pointer}),
            binaries={n:object_pointer(n) for n in ('http','publisher')})
        runproof=dict(config_path=str(CONFIG),config_sha256=worker.sha(worker.encoded(runconfig)),
            code_identity_sha256='1'*64,refs_identity_sha256='2'*64,panel_identity_sha256='3'*64,
            inputs_identity_sha256='4'*64,source_identity_sha256='a'*64,original_source_identity_sha256='b'*64,
            quality_reference_sha256=worker.sha(quality_records),namespace_prefix=PREFIX+'a0001/native',binary_sha256={})
        counters={'cgroup':'synthetic','observer_pid':42,'process_ids':[42],
            'memory.max':str(worker.MEMORY),'memory.peak':str(worker.MEMORY+4096),'memory.current':'100',
            'memory.stat':'anon 40\nfile 60\n','io.stat':'synthetic','memory.swap.max':'0','memory.swap.peak':'0',
            'memory.events':'max 1\noom 0\noom_kill 0\n','memory.swap.events':'max 0\n',
            'cpu.max':'200000 100000','cpu.stat':'usage_usec 1','pids.max':'512','pids.current':'1','pids.events':'max 0\n'}
        # IO observation may be absent; required resource safety counters still fail closed.
        group=tmp/'cgroup'; group.mkdir()
        for name in ('memory.current','memory.stat'): (group/name).write_text(counters[name])
        captured={k:v for k,v in counters.items() if k not in ('memory.current','memory.stat','io.stat')}
        captured['cgroup']=str(group)
        with patch.object(worker.ids,'capture_cgroup',side_effect=lambda:dict(captured)):
            missing=worker.capture()
            assert missing['io.stat']=='UNMEASURED'
            assert missing['io.stat_unavailable']==dict(type='FileNotFoundError',errno=2)
            worker.validate_cgroup(dict(before=missing,after=missing,closed=True))
            (group/'io.stat').write_text('259:0 rbytes=123 wbytes=456 rios=1 wios=2\n')
            present=worker.capture()
            assert present['io.stat']=='259:0 rbytes=123 wbytes=456 rios=1 wios=2\n'
            assert 'io.stat_unavailable' not in present
            for name in ('memory.current','memory.stat'):
                (group/name).unlink()
                rejects(worker.capture)
                (group/name).write_text(counters[name])
        for name in ('memory.max','cpu.stat','memory.events'):
            unsafe=dict(missing); del unsafe[name]
            rejects(lambda:worker.validate_cgroup(dict(before=missing,after=unsafe,closed=True)))
        # Fake only the huge raw/leaf lengths; all retained small bodies authenticate normally.
        original_artifact=worker.artifact
        def runtime_artifact(path):
            if Path(path).name=='raw': return dict(bytes=3_072_000_000,sha256='5'*64)
            actual=original_artifact(path)
            if str(path).endswith('/router/leaves.bin'): actual['bytes']=31_250*1540
            return actual
        generated={}; current_truth=[None]; current_mode=[None]
        def synthetic_run(args,log,seconds,timing=None):
            assert seconds>0
            if args[0]=='taskset':
                assert args[:3]==['taskset','-c','0-3']; args=args[3:]
            Path(log).parent.mkdir(parents=True,exist_ok=True)
            if timing: Path(timing).write_text(time_log)
            result=dict(command=args,exit_status=0,process_cleanup=True,wall_seconds=.001,process_peak_rss_kib=42)
            if args[0]=='aws':
                if args[1:3]==['s3','cp']:
                    key=args[3].split('/',3)[-1]
                    if key.startswith('synthetic/'): data=blobs[key.rsplit('/',1)[-1]]
                    elif key.endswith('/head.json'): data=generated['head']
                    else: data=(generated['root']/key.split('/generations/',1)[1].split('/',1)[1]).read_bytes()
                    Path(args[4]).write_bytes(data)
                elif args[2]=='head-object':
                    Path(log).write_bytes(worker.encoded(dict(ContentLength=6,ETag='"s3-current-etag"')))
                    return result
                elif args[2]=='get-object':
                    assert args[args.index('--if-match')+1]=='"s3-current-etag"'
                    Path(args[-1]).write_bytes(b'packed')
                else:
                    assert args[2]=='put-object' and args[-2:]==['--if-none-match','*']
            elif args[0]==sys.executable:
                Path(args[-2]).write_bytes(b'raw')
            elif args[0].endswith('/binaries/builder'):
                assert args[3]==str(512 * 1024**2)
                assert os.environ.get('BORSUK_NATIVE_MEMORY_BYTES')==environment.get('BORSUK_NATIVE_MEMORY_BYTES')
                if current_mode[0]=='build-failure': raise RuntimeError('synthetic build failed')
                builder=json.loads(Path(args[1]).read_bytes())
                assert builder['sq8_etag']=='"s3-current-etag"' and builder['semantic_profile']=='fresh1m'
                root=Path(args[-1]); root.mkdir()
                for name,size in zip(worker.STARTUP,sizes):
                    if name=='manifest.json': continue
                    path=root/name; path.parent.mkdir(parents=True,exist_ok=True); path.write_bytes(bytes(size))
                (root/'router/leaves.bin').write_bytes(b'synthetic leaves')
                discovery=dict(mode='semantic',profile='fresh1m')
                for label,name in (('root','root'),('membership','membership'),('leaves','leaves')):
                    descriptor=runtime_artifact(root/('router/'+name+'.bin'))
                    discovery[label+'_bytes']=descriptor['bytes']; discovery[label+'_sha256']=descriptor['sha256']
                manifest=dict(schema='borsuk-two-bit-generation-v8',generation=1,base_epoch=0,
                    discovery=discovery,sq8_object_key=builder['sq8_object_key'],sq8_etag=builder['sq8_etag'])
                (root/'manifest.json').write_bytes(worker.encoded(manifest))
                root_sha=runtime_artifact(root/'manifest.json')['sha256']
                generated.update(root=root,head=worker.encoded(dict(schema='borsuk-two-bit-head-v2',generation=1,epoch=1,
                    root_sha256=root_sha,mutation=None,fence=None)))
                Path(log).write_text(root_sha+'\n'); return result
            else:
                assert args[0].endswith('/binaries/publisher')
                assert os.environ['BORSUK_NATIVE_MEMORY_BYTES']==str(1024**3), 'publisher admission'
                if current_mode[0]=='publication-failure': raise RuntimeError('synthetic publication failed')
                assert args[-4:]==['--panel-count','64','--top-k','100']
                refs=worker.source_reference(quality_records)
                startup_event=dict(phase='startup',root_sha256=args[2],generation=1,control_epoch=1,
                    top_k=100,declared_panel_count=64,publish_wall_ns=1,head_read_wall_ns=10,remote_open_wall_ns=40)
                events=[startup_event,*[dict(phase='query',query_wall_ns=40,query_process_cpu_ns=10,
                    **{k:r[k] for k in ('query_ordinal',*worker.PARITY)}) for r in refs],
                    dict(phase='summary',count=64,top_k=100,measurement_wall_ns=2560)]
                Path(args[5]).write_bytes(b''.join(worker.encoded(r)+b'\n' for r in events))
            with Path(log).open('ab') as stream: stream.write(b'synthetic transfer/process\n')
            return result
        def runtime_read(_,pointer):
            name=pointer['path']
            if name=='synthetic/requests.jsonl': return request_body
            if name=='synthetic/records.jsonl': return quality_records
            if name=='synthetic/truth.i64': return current_truth[0]
            if name=='synthetic/raw-authority.json': return worker.encoded(dict(raw=dict(sha256='5'*64)))
            if name==str(worker.quality.CONFIG): return worker.encoded(qconfig)
            raise AssertionError(name)
        def synthetic_call(binary,cfg,published,body,ref,t):
            assert cfg['native_memory_bytes']==worker.NATIVE
            assert os.environ.get('BORSUK_NATIVE_MEMORY_BYTES')==environment.get('BORSUK_NATIVE_MEMORY_BYTES')
            q=ref['query_ordinal']; result=copy.deepcopy(row)
            now=worker.time.monotonic_ns()
            result.update(query_ordinal=q,started_ns=now-100000,completed_ns=now-50000,terminal_ns=now,
                cold_start_to_first_http_response_ns=50000,expected_authority=published['authority'],
                request_sha256=worker.sha(body),request_bytes=len(body))
            # Build startup telemetry from the actually generated frozen metadata.
            m=copy.deepcopy(metadata)
            for entry in m:
                entry['bytes']=published['metadata_files'][entry['name']]
                entry['payload_buffer_bound_bytes']=entry['bytes']
            h=dict(ready,authority=published['authority'],remote_open_stats=dict(startup,metadata=m))
            h['transport']=transport_report([12,5,1]+[0]*7,sum(published['metadata_files'].values())+
                published['head_file']['bytes']+published['metadata_files']['manifest.json']+300)
            r=dict(response,**ref,authority=published['authority'])
            r['transport']=transport_report([22,5,1]+[0]*7,h['transport']['totals']['consumed_payload_bytes']+200280)
            result.update(native_header=h,native_server_log=json.dumps(h)+'\n',response=r,
                raw_response_base64=worker.base64.b64encode(worker.encoded(r)).decode(),
                returned_hits=worker.validate_query(r,published,ref,t),accounting=worker.transport(h,r,published))
            if current_mode[0]=='call-failure' and q==1:
                result.update(outcome='failed',error_type='TimeoutError',error='synthetic timeout')
            return result
        environment=dict.fromkeys(worker.THREAD_ENV,'2')
        environment.update(AWS_MAX_ATTEMPTS='1',BORSUK_COLD_SOURCE_COMMIT='0'*40,BORSUK_COLD_ARCHIVE_SHA256='1'*64)
        for mode in ('complete','threshold608','scientific-fail','build-failure','publication-failure','call-failure','cleanup-failure'):
            current_mode[0]=mode
            environment['BORSUK_NATIVE_MEMORY_BYTES']='prior admission'
            if mode=='complete': environment.pop('BORSUK_NATIVE_MEMORY_BYTES')
            expected_truth=[]
            for q in range(64):
                t=list(range(100))
                if mode in ('scientific-fail','threshold608') and q<(33 if mode=='scientific-fail' else 32): t[9]=999
                expected_truth.extend(t)
            current_truth[0]=worker.struct.pack('<'+'q'*6400,*expected_truth)
            qconfig['panel']['files']['screen/truth.i64']=authority_pointer('synthetic/truth.i64',current_truth[0])
            target.write_bytes(worker.encoded(runconfig)); runproof['config_sha256']=worker.artifact(target)['sha256']
            out=tmp/mode
            real_remove=worker.shutil.rmtree
            def remove(path,*args,**kwargs):
                if mode=='cleanup-failure': raise OSError('synthetic cleanup failure')
                return real_remove(path,*args,**kwargs)
            with patch.object(worker,'qualify',return_value=(runconfig,runproof)),\
                 patch.object(worker.quality,'qualify',return_value=(qconfig,{})),\
                 patch.object(worker,'read',side_effect=runtime_read),patch.object(worker,'artifact',side_effect=runtime_artifact),\
                 patch.object(worker,'capture',return_value=counters),patch.object(worker.ids,'tool_versions',return_value={}),\
                 patch.object(worker.importlib.metadata,'version',side_effect=lambda n:worker.FIXED['versions'][n]),\
                 patch.object(worker.os,'sched_getaffinity',return_value={4,5}),\
                 patch.object(worker.quality,'run_process',side_effect=synthetic_run),\
                 patch.object(worker.quality,'ordinal_check',side_effect=lambda sq8,order:dict(rows_checked=1_000_000,record_bytes=780,
                    id_matches_order=True,complete_bijection=True,sq8=runtime_artifact(sq8),order=runtime_artifact(order))),\
                 patch.object(worker,'measured_call',side_effect=synthetic_call),patch.dict(os.environ,environment),\
                 patch.object(worker.shutil,'rmtree',side_effect=remove):
                if mode=='complete': os.environ.pop('BORSUK_NATIVE_MEMORY_BYTES',None)
                try: result=worker.main(target,runproof['config_sha256'],repo,out)
                except (AssertionError,RuntimeError,OSError):
                    assert mode in ('build-failure','publication-failure','call-failure','cleanup-failure'),mode
                else:
                    assert mode in ('complete','threshold608','scientific-fail')
                    assert result['execution_gate_passed'] and result['quality_gate_passed']==(mode!='scientific-fail')
                    assert result['returned_hits']==dict(complete=640,threshold608=608,**{'scientific-fail':607})[mode]
                assert os.environ.get('BORSUK_NATIVE_MEMORY_BYTES')==environment.get('BORSUK_NATIVE_MEMORY_BYTES')
            admission=json.loads((out/'resources.json').read_bytes())['phase_admission']
            assert admission==dict(builder_memory_bytes=512 * 1024**2,publisher_memory_bytes=1024**3,
                native_memory_bytes=worker.NATIVE,server_query_slots=4,native_budget_model=worker.native_budget_model(),
                memory_bytes=8 * 1024**3,swap_bytes=0)
            if mode=='cleanup-failure': real_remove(out/'scratch')
            assert not (out/'scratch').exists()
            saved=[json.loads(line) for line in (out/'records.jsonl').read_bytes().splitlines()]
            assert [r['query_ordinal'] for r in saved]==list(range(64))
            if mode in ('build-failure','publication-failure'): assert all(r['outcome']=='aborted' for r in saved)
            if mode=='call-failure': assert saved[1]['outcome']=='failed' and all(r['outcome']=='aborted' for r in saved[2:])
            if mode in ('complete','threshold608','scientific-fail'):
                collected=tmp/('collected-'+mode); collected.mkdir(); worker.shutil.copytree(out,collected/'screen')
                for name in ('test-resources.txt','run-closed.log'): (collected/name).write_bytes(b'synthetic outer service log')
                receipt=dict(runproof,campaign_schema=SCHEMA,artifact_roster_sha256=worker.sha(worker.encoded(ARTIFACTS)),
                    awscli_version=AWSCLI_VERSION,awscli_sha256=AWSCLI_SHA256)
                launch=dict(instance_id='i-owned',nodes={'0':dict(instance_id='i-owned')},prefix=PREFIX+'a0001',
                    source_commit='0'*40,source_archive_sha256='1'*64)
                reservation=dict(schema=SCHEMA,qualification=receipt,source_commit='0'*40,source_archive_sha256='1'*64)
                terminal=dict(schema=SCHEMA,instance_id='i-owned',status='complete',phase='complete',exit_code=0,original_exit_code=0,
                    source_commit='0'*40,source_archive_sha256='1'*64,**{n:receipt[n] for n in TERMINAL_IDENTITIES},
                    artifacts={n:worker.artifact(collected/n) for n in ARTIFACTS})
                for name,value in (('aws-launch.json',launch),('aws-closeout.json',dict(nodes=launch['nodes'],state='terminated')),
                    ('aws-reservation.json',reservation),('aws-terminal.json',terminal)):
                    (collected/name).write_bytes(worker.encoded(value))
                with patch.object(module,'archived_authority',return_value=(runconfig,receipt)),patch.object(worker,'read',side_effect=runtime_read):
                    replayed=replay(collected)
                    assert replayed['executed'] and replayed['quality_gate_passed']==(mode!='scientific-fail')
                    s3=Mock()
                    def get(**kwargs):
                        key=kwargs['Key']; path=collected/'aws-terminal.json' if key.endswith('/terminal.json') else collected/key.split('/artifacts/',1)[1]
                        return {'Body':io.BytesIO(path.read_bytes())}
                    s3.get_object.side_effect=get
                    collect(s3,launch['prefix'],collected,'i-owned','0'*40,'1'*64)
                    assert s3.get_object.call_count==len(ARTIFACTS)+1
                    (collected/'aws-closeout.json').write_bytes(worker.encoded(dict(state='running',nodes=launch['nodes'])))
                    s3.reset_mock(); rejects(lambda:collect(s3,launch['prefix'],collected,'i-owned','0'*40,'1'*64))
                    s3.get_object.assert_not_called()
        # Keep lifecycle tests synthetic, including this adapter's exact cloud geometry.
        sdk,botocore,exceptions=(ModuleType(n) for n in ('boto3','botocore','botocore.exceptions'))
        class SDKError(Exception):
            def __init__(self,**kwargs): super().__init__('synthetic SDK')
        for n in ('ClientError','EndpointConnectionError','ReadTimeoutError'): setattr(exceptions,n,SDKError)
        sdk.Session=Mock(side_effect=AssertionError('cloud forbidden')); botocore.exceptions=exceptions
        with patch.dict(sys.modules,{'boto3':sdk,'botocore':botocore,'botocore.exceptions':exceptions}):
            lifecycle,_=ids.lifecycle()
            frozen=dict.fromkeys(TERMINAL_IDENTITIES,'1'*64)
            frozen.update(config_path=str(CONFIG),campaign_schema=SCHEMA,awscli_version=AWSCLI_VERSION,
                awscli_sha256=AWSCLI_SHA256,namespace_prefix=PREFIX+'a0001/native')
            body=user_data('0'*40,'1'*64,'source/key',PREFIX+'a0001',frozen)
            assert all(x in body for x in ('MemoryMax=8G','MemorySwapMax=0','CPUQuota=200%','TasksMax=512',
                'RuntimeMaxSec=7200','--on-active=9000s','taskset -c 4-5','PYTHONPATH="$root/repo"'))
            assert 'ulimit -v' not in body, 'AS ceiling applies to native serving only'
            assert 'scripts.run_native_semantic_1m_cold' in body and 'scripts.run_native_semantic_1m_quality' not in body
            command=body.split('systemd-run --unit=semantic-1m-cold',1)[1].split('\nfor name',1)[0]
            shell='systemd-run() { printf "%s\\n" "$@"; }; root=/synthetic; systemd-run --unit=semantic-1m-cold'+command
            argv=subprocess.check_output(['bash','-c',shell],text=True).splitlines()
            assert 'IOAccounting=yes' in argv
            assert '--setenv=PYTHONPATH=/synthetic/repo' in argv
            assert all('--setenv='+n+'=2' in argv for n in worker.THREAD_ENV)
            assert argv[-7:][1:3]==['-m','scripts.run_native_semantic_1m_cold'],argv
            with patch.object(module,'qualify',side_effect=AssertionError('pending')):
                rejects(lambda:main('a0001'))
            sdk.Session.assert_not_called()
            with patch.dict(sys.modules,{lifecycle.__name__:lifecycle}),worker.panel.contextlib.redirect_stdout(io.StringIO()):
                lifecycle.self_check(lifecycle_only=True)
    assert worker.time.monotonic()-started<55
    assert worker.resource.getrusage(worker.resource.RUSAGE_SELF).ru_maxrss*1024<=200*1024**2
    print('PASS FIRST1M v8 contract, closed a0003 authenticated 66-event publisher/scorer parity and R10=624/640 R100=6077/6400 (campaign FAIL preserved), builder512MiB/publisher-HTTP1GiB four-slot admission and env restoration, ordinal/f32 parity, S3 ETag, dynamic startup/IMDS/transport, cold wire boundary, 607/608, ledger/resources/cleanup, original PGID and shared ACK/fsync/termination closure; native UNRUN')


if __name__ == '__main__':
    if sys.argv[1:] == ['--measurement-self-check']:
        measurement_self_check()
    elif sys.argv[1:] == ['--self-check']:
        self_check()
    elif len(sys.argv)==3 and sys.argv[1]=='--replay':
        print(json.dumps(replay(sys.argv[2]),sort_keys=True))
    else:
        assert len(sys.argv)==2, 'usage: aNNNN | --self-check | --measurement-self-check | --replay DIRECTORY'
        with open('/tmp/borsuk-semantic-1m-cold-launch.lock','a+') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            main(sys.argv[1])
