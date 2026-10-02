"""One root-frozen CoHere FIRST1M build and fixed64 production diagnostic score.

CLI: CONFIG CONFIG_SHA REPO NEW_OUTPUT. Root owns authorities/freeze/launch.
No compilation, extraction, oracle, reselection, retry or cold performance claim.
"""
import importlib.metadata
import json
import math
import os
from pathlib import Path
import resource
import shutil
import sys
import threading
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_native_semantic_1m_quality as native

panel, ids = native.panel, native.ids
encoded, sha, artifact, write = native.encoded, native.sha, native.artifact, native.write
read, identity = native.read, native.identity
ordinal_check, local_head, run_process = native.ordinal_check, native.local_head, native.run_process
SCORER_SOURCE, BUILDER_SOURCE = native.SCORER_SOURCE, native.BUILDER_SOURCE
BASE = native.BASE
ROOT = BASE/'cohere-quality'
CONFIG = ROOT/'config.json'
MEMORY, PAYLOAD = 4 << 30, native.PAYLOAD
THREAD_ENV = native.THREAD_ENV
FIXED = dict(native.FIXED, schema='borsuk-cohere-semantic-1m-quality-execution-v1',
             dataset='CoHere', memory_bytes=MEMORY, original_preparation_campaign_status='FAIL')
REF_PATHS = native.REF_PATHS
CODE = tuple(sorted((*native.CODE, 'scripts/run_native_cohere_semantic_1m_quality.py',
                     'scripts/launch_native_cohere_semantic_1m_quality_spot.py')))
OUTPUTS = (*(n for n in native.OUTPUTS if n != 'extraction-resources.txt'), 'generation-manifest.json')
ORIGINAL = BASE/'cohere-preparation/a0001'
SEALED = ('queries.raw','requests.jsonl','truth.u32','truth.i64','panel.json',
          'duplicate-audit.json','oracle.json','resources.json','decision.json')
PROTOCOL = dict(path=str(ROOT/'prospective-protocol.json'), bytes=2319,
    sha256='a0bca96d3c056548a9aed3f16f968e64f40f370d5a6fa0575d5ba8c337139aa3')
NATIVE_CONFIG = dict(path=str(native.CONFIG), bytes=20439,
    sha256='d513a25d24739167ab8d7392b594e4e4373db65e7cc3de3fe5e01668fa1de8ee')
READBACK = dict(path=str(BASE/'cohere-preparation/a0001-scoped-readback/readback.json'),bytes=5497,
    sha256='ceb282074e66500f1213ea230d416c2a49c23cdb32c070d2cdc0a6f00b415aea')
TERMINAL = dict(path=str(ORIGINAL/'aws-terminal.json'),bytes=3421,
    sha256='ab8b1fab71fcda376e3a71143b196ecf55abfce52ad1994cc297ce5f98fa8706')
HISTORICAL_ROOT = str(BASE/'cohere-input-authorities/historical-source-root.json')


def native_authority(repo, refs, binaries):
    old = json.loads(read(repo,NATIVE_CONFIG))
    assert refs == old['refs'] and binaries == old['binaries'], 'immutable binary assurance refs'
    # Only binary/source assurance is reused; old dataset/quality is never admitted.
    return native.native_authority(repo,refs,binaries)


def corpus_inputs(repo):
    protocol = json.loads(read(repo,PROTOCOL))
    return {n:{k:p[k] for k in ('key','bytes','sha256')}
            for n,p in protocol['corpus_inputs'].items()}


def construction_authority(repo, authority, inputs):
    assert set(authority)=={'directory','verification','files'}
    assert authority['directory']==str(ORIGINAL)
    assert authority['verification']['path']==str(ROOT/'construction-verification.json')
    report = json.loads(read(repo,authority['verification']))
    assert report['schema']=='borsuk-cohere-closed-panel-construction-check-v1'
    assert report['scoped_construction_passed'] is report['original_cgroup_validator_rejected'] is True
    assert report['original_campaign_status']=='FAIL' and report['resource_qualification']=='failed'
    for n in ('prepared_campaign_passed','resource_qualification_passed','ann_quality_measured',
              'complete_historical_coverage','serving_or_build_qualified','root_authority_issued',
              'input_reconstruction_or_gt_reexecution','historical_etags_reconstructed'):
        assert report[n] is False, 'construction scope: '+n
    assert (report['queries'],report['dimensions'],report['gt_k'],report['truth_id_space'])==(64,768,100,'source ordinal')
    terminal = json.loads(read(repo,TERMINAL))
    assert terminal['status']=='failed' and terminal['exit_code']==terminal['original_exit_code']==1
    observed = json.loads(read(repo,READBACK))
    assert observed['original_campaign_status']=='FAIL' and observed['prepared_campaign_passed'] is False
    assert report['fresh_sealed_observations']==observed['artifact_bodies_authenticated']
    assert report['source']=={n:terminal[n] for n in ('source_commit','source_archive_sha256')}
    assert set(authority['files'])==set(report['sealed_artifacts'])==set(SEALED)
    for n,p in authority['files'].items():
        assert p['path']==str(ORIGINAL/'screen'/n)
        assert identity(p)==identity(report['sealed_artifacts'][n])==terminal['artifacts']['screen/'+n]
        assert identity(p)==identity(observed['artifact_bodies_authenticated'][n])
        read(repo,p)
    # Authenticate the checker and every byte it attested, without rerunning GT.
    assert artifact(panel.repo_path(repo,'scripts/check_cohere_closed_panel_construction.py'))==report['validator_body']
    expected_evidence = {str(ORIGINAL/n):p for n,p in terminal['artifacts'].items()}
    for n in ('aws-terminal.json','aws-reservation.json','aws-launch.json','aws-closeout.json'):
        expected_evidence[str(ORIGINAL/n)] = artifact(panel.repo_path(repo,str(ORIGINAL/n)))
    expected_evidence[READBACK['path']] = identity(READBACK)
    assert report['evidence_bodies']==expected_evidence
    for name,p in report['evidence_bodies'].items():
        read(repo,dict(path=name,**p))
    original_proof=json.loads(read(repo,dict(path=str(ORIGINAL/'aws-reservation.json'),
        **report['evidence_bodies'][str(ORIGINAL/'aws-reservation.json')])) )['qualification']
    assert report['original_reference_bodies']==original_proof['refs']
    assert {n:p['sha256'] for n,p in report['original_code_bodies'].items()}==original_proof['code_sha256']
    for name,p in report['original_code_bodies'].items():
        assert artifact(panel.repo_path(repo,name))==identity(p)
    for p in report['original_reference_bodies'].values():
        read(repo,p)
    corpus=report['source_corpus']
    assert (corpus['rows'],corpus['dimensions'],corpus['metric'],corpus['source_interval'])==(1_000_000,768,'cosine',[0,1_000_000])
    assert {n:{k:corpus[n][k] for k in ('key','bytes','sha256')} for n in inputs}==inputs==corpus_inputs(repo)
    assert artifact(panel.repo_path(repo,HISTORICAL_ROOT))==identity(inputs['root_manifest'])
    for n,name in (('raw','source.raw'),('order','source-order.u64'),('root_manifest','source-root.json')):
        assert identity(report['construction_inputs'][name])==identity(inputs[n])
    return report


def qualify(config_path, expected_sha, repo):
    repo,config_path=Path(repo).resolve(),Path(config_path).absolute()
    assert config_path==repo/CONFIG and config_path.is_file() and not config_path.is_symlink(), 'root regular config path'
    assert config_path.stat().st_size<=65536 and sha(config_path.read_bytes())==expected_sha, 'config identity'
    config=json.loads(config_path.read_bytes())
    assert config['authority_pending'] is False, 'root freeze pending'
    assert set(config)==set(FIXED)|{'authority_pending','code_sha256','refs','panel','inputs','binaries'}
    assert all(type(config[n]) is type(v) and config[n]==v for n,v in FIXED.items()), 'fixed quality/resource protocol'
    assert set(config['code_sha256'])==set(CODE), 'exact transitive code closure'
    assert all(artifact(panel.repo_path(repo,n))['sha256']==d for n,d in config['code_sha256'].items()), 'source drift'
    _,proof=native_authority(repo,config['refs'],config['binaries'])
    assert set(config['inputs'])=={'raw','sq8','order','root_manifest'}
    assert config['inputs']==corpus_inputs(repo), 'CoHere corpus identity'
    construction_authority(repo,config['panel'],config['inputs'])
    proof.update(config_path=str(CONFIG),config_sha256=expected_sha,
        code_identity_sha256=sha(encoded(config['code_sha256'])),
        refs_identity_sha256=sha(encoded(config['refs'])),panel_identity_sha256=sha(encoded(config['panel'])),
        inputs_identity_sha256=sha(encoded(config['inputs'])),qualification=False,physical_s3_measured=False,
        dataset='CoHere',original_preparation_campaign_status='FAIL',binary_assurance_only_reused=True)
    return config,proof


def validate_cgroup(report):
    with patch.object(panel,'MEMORY',MEMORY):
        panel.validate_cgroup(report)


def score_summary(records, proof, scorer_config_sha):
    events=[json.loads(line) for line in Path(records).read_text().splitlines()]
    assert events and events[-1]['phase']=='terminal'
    summary=events[-1]['summary']
    assert events[0]['phase']=='identity' and sum(e['phase']=='identity' for e in events)==1
    assert sum(e['phase']=='terminal' for e in events)==1
    assert all(e.get('truth_opened',False) is False for e in events
               if e['phase'] in ('startup','frozen_query','all_queries_frozen','query_failure'))
    if summary.get('complete') is True:
        assert all(e['phase'] in ('identity','startup','frozen_query','all_queries_frozen','evaluation','terminal') for e in events)
        result=native.score_summary(records,proof,scorer_config_sha)
        evaluated=[e for e in events if e['phase']=='evaluation']
        result.update(dataset='CoHere',per_query_returned_r10=[e['returned_hits10']/10 for e in evaluated],
            per_query_returned_r100=[e['returned_hits100']/100 for e in evaluated])
        return result
    # A production admission failure closes this arm with its raw trace, never PASS.
    assert summary['complete'] is False and summary['status']=='FAIL'
    assert type(summary.get('error')) is str and summary['error']
    first=events[0]
    assert first['schema']=='borsuk-semantic-router-scorer-result-v2' and first['phase']=='identity'
    assert first['config_sha256']==scorer_config_sha and first['binary_sha256']==proof['binary_sha256']['scorer']
    assert first['scorer_source_sha256']==proof['native_source_sha256'][SCORER_SOURCE]
    assert first['router_source_sha256']==proof['native_source_sha256']['crates/borsuk/src/semantic_unit_router.rs']
    assert first['physical_s3_measured'] is False
    frozen=[e for e in events if e['phase']=='frozen_query']
    assert [e['ordinal'] for e in frozen]==list(range(len(frozen))) and len(frozen)<=64
    assert all(e['truth_opened'] is False for e in frozen)
    for e in frozen:
        assert 10<=len(e['returned_ids'])<=100 and len(e['returned_ids'])==len(set(e['returned_ids']))
        assert all(type(row) is int and 0<=row<1_000_000 for row in e['returned_ids'])
    assert all(e['phase'] in ('identity','startup','frozen_query','query_failure','terminal') for e in events)
    failure=[e for e in events if e['phase']=='query_failure']
    assert len(failure)<=1
    if failure:
        assert failure[0]['ordinal']==len(frozen)<64
        assert events[-2]==failure[0]
    return dict(summary,dataset='CoHere',ann_quality_measured=False,physical_s3_measured=False,
        local_logical_io_only=True,retuned=False,raw_failed_arm_closed=True,
        queries_frozen=len(frozen),failure_decomposition=failure,
        current_whole_tree_full_execution=False)


def main(config_path, expected_sha, repo, output):
    repo,output = Path(repo).resolve(),Path(output).absolute()
    config,proof = qualify(config_path,expected_sha,repo)
    assert output.is_absolute() and not output.is_symlink() and not output.exists()
    assert not output.resolve().is_relative_to(repo), 'scratch outside source required'
    counters = dict(before=ids.capture_cgroup(),closed=False)
    validate_cgroup(dict(counters,after=counters['before'],closed=True))
    output.mkdir(parents=True,exist_ok=False)
    scratch = output/'scratch'; scratch.mkdir()
    report = dict(schema='borsuk-cohere-semantic-1m-quality-resources-v1',stages={},
        build_invocations=0,score_invocations=0,scratch_peak_observed_bytes=0,
        scratch_sample_interval_seconds=1,physical_s3_measured=False)
    started = time.monotonic()
    stop = threading.Event()
    def sample():
        while not stop.is_set():
            try:
                used = sum(p.stat().st_size for p in scratch.rglob('*') if p.is_file() and not p.is_symlink())
            except FileNotFoundError:
                continue
            report['scratch_peak_observed_bytes'] = max(report['scratch_peak_observed_bytes'],used)
            stop.wait(1)
    monitor = threading.Thread(target=sample,daemon=True)
    monitor.start()
    try:
        validate_cgroup(dict(counters,after=counters['before'],closed=True))
        versions = {n:importlib.metadata.version(n) for n in ('numpy','pyarrow')}
        assert versions == {n:FIXED['versions'][n] for n in versions}
        assert all(os.environ.get(n)=='2' for n in THREAD_ENV), 'two-thread environment'
        tools = ids.tool_versions()
        assert tools['machine']=='x86_64' and tools['os_release']['ID']=='ubuntu' and tools['os_release']['VERSION_ID']=='24.04'
        write(output/'tool-versions.json',dict(tools,**versions,object_store='0.14.1',thread_environment={n:os.environ[n] for n in THREAD_ENV}))
        source = {k:os.environ['BORSUK_QUALITY_'+n] for k,n in (('source_commit','SOURCE_COMMIT'),('source_archive_sha256','ARCHIVE_SHA256'))}
        assert panel.re.fullmatch('[0-9a-f]{40}',source['source_commit']) and panel.re.fullmatch('[0-9a-f]{64}',source['source_archive_sha256'])
        write(output/'source-qualification.json',dict(proof,**source))
        write(output/'config.json',Path(config_path).read_bytes())
        write(output/'preparation.log',b'Authenticate original RAW downloads; verify complete SQ8 source ordinal mapping.\n')
        preparation = time.monotonic()
        def remaining():
            left = 1800-(time.monotonic()-preparation)
            assert left>0, 'preparation deadline'
            return left
        for kind,pointers in (('inputs',config['inputs']),('binaries',config['binaries'])):
            (scratch/kind).mkdir()
            for n,p in pointers.items():
                target = scratch/kind/n
                result = run_process(['aws','s3','cp',f"s3://{config['bucket']}/{p['key']}",str(target),'--only-show-errors'],output/'preparation.log',remaining())
                assert result['exit_status']==0 and artifact(target)==identity(p), 'download body: '+n
                if kind=='binaries':
                    target.chmod(0o500)
        inputs, binaries = scratch/'inputs',scratch/'binaries'
        raw_sha = config['inputs']['raw']['sha256']
        assert artifact(inputs/'raw')==identity(config['inputs']['raw'])
        check = ordinal_check(inputs/'sq8',inputs/'order')
        write(output/'sq8-ordinal-check.json',check)
        remaining()
        historical=json.loads((inputs/'root_manifest').read_bytes())
        assert artifact(inputs/'root_manifest')==artifact(panel.repo_path(repo,HISTORICAL_ROOT))
        assert historical['schema']=='borsuk-two-bit-generation-v4'
        assert historical['canonical']['rows']==1_000_000 and historical['canonical']['dimensions']==768
        assert historical['sq8_object_sha256']==check['sq8']['sha256']
        assert historical['sq8_object_key']==config['inputs']['sq8']['key']
        assert historical['generation']==1 and historical['base_epoch']==0
        for n in ('low','step'):
            assert len(historical[n])==768 and all(type(v) in (int,float) and math.isfinite(v) for v in historical[n])
        assert all(v>0 for v in historical['step'])
        store = scratch/'store'; (store/'quality/objects').mkdir(parents=True)
        sq8 = store/'quality/objects'/check['sq8']['sha256']
        (inputs/'sq8').rename(sq8)
        head = local_head(sq8); write(output/'local-sq8-head.json',head)
        write(output/'input-hashes.json',dict(inputs=config['inputs'],raw=artifact(inputs/'raw'),
            panel=config['panel'],binaries={n:artifact(binaries/n) for n in config['binaries']}))
        builder = dict(discovery='semantic',semantic_profile='fresh1m',
            order=dict(path=str(inputs/'order'),sha256=check['order']['sha256']),
            raw=str(inputs/'raw'),raw_sha256=raw_sha,sq8=str(sq8),sq8_sha256=check['sq8']['sha256'],
            rows=1_000_000,dimensions=768,generation=1,base_epoch=0,
            low=historical['low'],step=historical['step'],
            sq8_object_key='quality/objects/'+check['sq8']['sha256'],sq8_etag=head['etag'])
        write(output/'builder-config.json',builder)
        report['stages']['preparation']=dict(wall_seconds=time.monotonic()-preparation,
            process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            cgroup=ids.capture_cgroup())
        assert report['stages']['preparation']['wall_seconds']<=1800
        report['build_invocations']+=1
        result = run_process([str(binaries/'builder'),str(output/'builder-config.json'),
            artifact(output/'builder-config.json')['sha256'],str(PAYLOAD),str(store/'quality/index')],
            output/'build.log',3600,output/'build-resources.txt')
        report['stages']['build']=dict(result,cgroup=ids.capture_cgroup())
        assert result['exit_status']==0 and local_head(sq8)==head, 'build failure/SQ8 metadata drift'
        root_body = store/'quality/index/manifest.json'
        root_sha = artifact(root_body)['sha256']
        assert (output/'build.log').read_text().splitlines()[-1]==root_sha, 'native build root SHA'
        write(output/'generation-manifest.json',root_body.read_bytes())
        authority = config['panel']['files']
        scorer = dict(schema='borsuk-semantic-router-scorer-config-v2',dataset='CoHere',profile='fresh1m',
            rows=1_000_000,dimensions=768,first=0,count=64,store_root=str(store),
            generation_prefix='quality/index',generation_root_sha256=root_sha,
            scratch_parent=str(scratch),max_memory_bytes=PAYLOAD,
            order=dict(path=str(inputs/'order'),**check['order']))
        for n,name in (('requests','requests.jsonl'),('truth','truth.i64')):
            p = authority[name]
            target = scratch/name; write(target,read(repo,p))
            scorer[n]=dict(path=str(target),**identity(p))
        write(output/'scorer-config.json',scorer)
        # Final consumed body pins precede the sole scorer invocation.
        assert artifact(sq8)==identity(config['inputs']['sq8']) and local_head(sq8)==head
        assert all(artifact(binaries/n)==identity(p) for n,p in config['binaries'].items())
        report['score_invocations']+=1
        result = run_process([str(binaries/'scorer'),str(output/'scorer-config.json'),
            artifact(output/'scorer-config.json')['sha256'],str(output/'records.jsonl')],
            output/'score.log',900,output/'score-resources.txt')
        report['stages']['score']=dict(result,cgroup=ids.capture_cgroup())
        assert result['exit_status'] in (0,2), 'native scorer execution failure'
        summary = score_summary(output/'records.jsonl',proof,artifact(output/'scorer-config.json')['sha256'])
        assert result['exit_status']==(0 if summary['status']=='PASS' else 2)
        write(output/'summary.json',summary)
        assert local_head(sq8)==head, 'SQ8 metadata changed during production read'
        return summary
    finally:
        stop.set(); monitor.join(timeout=5)
        assert not monitor.is_alive(), 'scratch observer still running'
        report.update(wall_seconds=time.monotonic()-started,
            process_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
        shutil.rmtree(scratch)
        write(output/'cleanup.json',dict(scratch_removed=not scratch.exists(),owned_output=str(output),
            build_invocations=report['build_invocations'],score_invocations=report['score_invocations']))
        counters.update(after=ids.capture_cgroup(),closed=True)
        report.update(enforced_memory_max_bytes=MEMORY,aggregate_memory_peak_bytes=int(counters['after']['memory.peak']),
            memory_events=counters['after']['memory.events'],peak_is_enforced_limit=False)
        write(output/'resources.json',report); write(output/'quality-cgroup.json',counters)
        validate_cgroup(counters)
        assert report['wall_seconds']<=7200, 'whole service deadline'


if __name__ == '__main__':
    assert len(sys.argv)==5, 'usage: CONFIG CONFIG_SHA REPO NEW_OUTPUT'
    main(*sys.argv[1:])
