"""One frozen ReLAION fresh1m build and production diagnostic score.

CLI: CONFIG CONFIG_SHA REPO NEW_OUTPUT. Root owns config/freeze/launch/reduction.
No compilation, oracle, selection, SQ8 refit, retry or cold-S3 performance claim.
The launcher --self-check exercises this adapter with synthetic/mocked bodies.
"""
import gzip
import importlib.metadata
import json
import math
import os
from pathlib import Path
import resource
import signal
import shutil
import struct
import subprocess
import sys
import threading
import time
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import launch_native_semantic_fresh_panel_spot as panel
from scripts.check_native_startup_build import source_hashes, source_identity

ids = panel.ids
encoded, sha, artifact, write = ids.encoded, ids.sha, ids.artifact, ids.write
ROOT = Path('docs/research/performance-architecture-20260930/semantic-1m/quality-execution')
CONFIG = ROOT / 'config.json'
MEMORY, PAYLOAD = 1024**3, 512 * 1024**2
THREAD_ENV = (*panel.THREAD_ENV, 'RAYON_NUM_THREADS', 'TOKIO_WORKER_THREADS')
FIXED = dict(schema='borsuk-semantic-1m-quality-execution-v1', dataset='ReLAION',
    rows=1_000_000, dimensions=768, metric='cosine', profile='fresh1m',
    generation_format=8, generation=1, base_epoch=0, first=0, count=64, k=100,
    minimum_hits10=608, denominator10=640, retune_allowed=False,
    build_invocations=1, score_invocations=1, physical_s3_measured=False,
    cold_http_measured=False, complete_historical_coverage=False,
    architecture='x86_64', region=panel.REGION, bucket=panel.BUCKET,
    instance_type=panel.INSTANCE_TYPE, image_id=panel.IMAGE_ID,
    root_device_name=panel.ROOT_DEVICE_NAME, subnet_id=panel.SUBNET,
    volume_gib=80, volume_type='gp3', encrypted=True, delete_on_termination=True,
    spot_max_usd_per_hour=.50, compute_cap_usd=1.25, ebs_s3_allowance_usd=.15,
    memory_bytes=MEMORY, payload_bytes=PAYLOAD, swap_bytes=0,
    cpu_quota_percent=200, tasks_max=512, threads=2,
    preparation_limit_seconds=1800, build_limit_seconds=3600,
    score_limit_seconds=900, service_limit_seconds=7200, machine_limit_seconds=9000,
    versions={'numpy':'2.3.3', 'pyarrow':'24.0.0', 'object_store':'0.14.1'})
BASE = ROOT.parent
GATES = BASE / 'implementation-gates'
REF_PATHS = {
    'input_authorities': str(BASE/'input-authorities/verification.json'),
    'source_id_repair': str(BASE/'panel-tools/remote-construction/source-id-repair/authority-verification.json'),
    'implementation_verification': str(GATES/'remote-implementation/a0002/verification.json'),
    'implementation_terminal': str(GATES/'remote-implementation/a0002/aws-terminal.json'),
    'implementation_receipt': str(GATES/'remote-implementation/a0002/workspace-receipt.json'),
    'source_manifest': str(GATES/'remote-implementation/a0002/native-source-manifest.json'),
    'full_verification': str(GATES/'remote-full/a0001/verification.json'),
    'full_terminal': str(GATES/'remote-full/a0001/aws-terminal.json'),
    'full_receipt': str(GATES/'remote-full/a0001/workspace-receipt.json'),
    'scorer_verification': str(GATES/'scorer-file-adapter/verification.json'),
    'scorer_source': str(GATES/'scorer-file-adapter/release-source.json'),
    'assets': str(ROOT/'assets/verification.json')}
SCORER_SOURCE = 'crates/borsuk/src/bin/check_semantic_router_scorer.rs'
BUILDER_SOURCE = 'crates/borsuk/src/bin/build_two_bit_generation.rs'
HELPER_CODE = tuple(json.loads((Path(__file__).resolve().parents[1]/panel.AUTHORITY).read_bytes())['code_sha256'])
CODE = tuple(sorted(set((*panel.CODE, *HELPER_CODE,
    'scripts/run_native_semantic_1m_quality.py', 'scripts/launch_native_semantic_1m_quality_spot.py'))))
OUTPUTS = ('source-qualification.json', 'config.json', 'tool-versions.json',
    'input-hashes.json', 'sq8-ordinal-check.json', 'local-sq8-head.json',
    'builder-config.json', 'scorer-config.json', 'build.log', 'build-resources.txt',
    'score.log', 'score-resources.txt', 'records.jsonl', 'summary.json',
    'resources.json', 'quality-cgroup.json', 'cleanup.json',
    'preparation.log', 'extraction-resources.txt')


def identity(pointer, keys=('bytes','sha256')):
    assert type(pointer['bytes']) is int and pointer['bytes'] > 0
    assert type(pointer['sha256']) is str and panel.re.fullmatch('[0-9a-f]{64}', pointer['sha256'])
    return {k:pointer[k] for k in keys}


def read(repo, pointer):
    """Small locally authenticated authority; gzip is transport, not identity."""
    assert set(pointer) in ({'path','bytes','sha256'}, {'path','bytes','sha256','archived_path'},
                           {'path','key','bytes','sha256'}, {'path','key','bytes','sha256','archived_path'})
    path = panel.repo_path(repo, pointer.get('archived_path',pointer['path']))
    assert path.is_file() and path.stat().st_size <= 16*1024**2, 'small regular authority required'
    body = path.read_bytes()
    if 'archived_path' in pointer:
        assert pointer['archived_path'] == pointer['path']+'.gz'
        body = gzip.decompress(body)
    assert dict(bytes=len(body),sha256=sha(body)) == identity(pointer), 'authority body identity'
    return body


def native_authority(repo, refs, binaries):
    assert set(refs) == set(REF_PATHS), 'exact qualification authority roster'
    assert all(refs[n]['path'] == p for n,p in REF_PATHS.items()), 'qualification authority paths'
    values = {n:json.loads(read(repo,p)) for n,p in refs.items()}
    original = values['source_manifest']['source_sha256']
    original_id = source_identity(original)
    assert len(original) == 399 and original_id == values['source_manifest']['source_identity_sha256']
    for kind in ('implementation','full'):
        verified,terminal,receipt = (values[kind+'_'+n] for n in ('verification','terminal','receipt'))
        assert verified['qualified'] is True and verified['exit_status'] == 0
        assert terminal['phase'] == terminal['status'] == 'complete'
        assert terminal['exit_code'] == terminal['original_exit_code'] == receipt['exit_status'] == receipt['gate_status'] == 0
        assert receipt['qualified'] is receipt['command_completed'] is receipt['command_started'] is receipt['source_unchanged'] is True
        assert receipt['source_sha256'] == original
        assert verified['source_identity_sha256'] == terminal['source_identity_sha256'] == receipt['source_identity_sha256'] == original_id
        assert identity(refs[kind+'_receipt']) == terminal['artifacts']['workspace-receipt.json']
        assert identity(refs['source_manifest']) == terminal['artifacts']['native-source-manifest.json']
        assert receipt['artifacts'] == {n:p for n,p in terminal['artifacts'].items() if n not in ('workspace-receipt.json','run-closed.log')}
        assert verified['oom_kills'] == verified['swap_peak_bytes'] == 0
    assert values['full_verification']['actual_full_workspace_execution'] is True
    assert values['full_receipt']['command'][-5:] == ['test','--release','--locked','--workspace','--all-targets']
    scorer = values['scorer_verification']
    assert scorer['status'] == 'VERIFIED' and scorer['production_library_changed'] is False
    assert scorer['committed_blob_matches_verified_source'] is True and scorer['full_workspace_execution_rerun'] is False
    assert scorer['red_expected_runtime_failure'] is True and scorer['green_tests_passed'] == 4
    gates = {g['name']:g for g in scorer['gates']}
    assert set(gates) == {'red-a2','green','release','clippy','workspace-test-build'}
    assert all(g['status'] == (101 if n == 'red-a2' else 0) and g['source_unchanged'] is True
               and g['stop_reason'] is None and g['observed_swap_peak_bytes'] == 0 for n,g in gates.items())
    current = source_hashes(repo)
    expected = dict(original, **{SCORER_SOURCE:scorer['scorer_source_sha256']})
    assert current == expected == values['scorer_source']['native_inventory'], 'unqualified native source delta'
    assert source_identity(current) == scorer['native_source_identity_sha256'] == values['scorer_source']['native_source_identity_sha256']
    assert set(binaries) == {'builder','scorer'}
    for p in binaries.values():
        assert 'key' in p and type(p['key']) is str and p['key'] and '\n' not in p['key']
        read(repo,p)  # Authenticate exact qualified binary before any cloud.
    assert identity(binaries['builder']) == values['implementation_terminal']['artifacts']['binaries/build_two_bit_generation']
    binary = scorer['release_binary']
    assert identity(binaries['scorer']) == dict(bytes=binary['binary_bytes'],sha256=binary['binary_sha256'])
    assert binary['scorer_source_sha256'] == current[SCORER_SOURCE]
    assets = values['assets']
    assert assets['schema']=='borsuk-semantic-1m-quality-assets-v1' and assets['bucket']==FIXED['bucket']
    assert assets['source_sha256']==current and assets['source_identity_sha256']==source_identity(current)
    assert assets['source_file_count']==399 and assets['binaries']==binaries
    assert assets['production_library_unchanged'] is assets['scorer_conditional_s3_readback_passed'] is True
    assert assets['full_execution_current_whole_tree_claim'] is False
    assert values['source_id_repair']['passed'] is values['source_id_repair']['helper_inputs_unchanged'] is values['source_id_repair']['fixed_protocol_unchanged'] is True
    return values, dict(original_full_workspace_execution_reused=True,
        original_source_identity_sha256=original_id, source_identity_sha256=source_identity(current),
        source_file_count=399, native_source_sha256=current,
        separately_qualified_scorer=True, current_whole_tree_full_execution=False,
        native_rebuilt=False, binary_sha256={n:p['sha256'] for n,p in binaries.items()})


def panel_authority(repo, authority):
    directory = panel.repo_path(repo, authority['directory'])
    roster = {*panel.ARTIFACTS, 'aws-reservation.json','aws-launch.json','aws-closeout.json',
              'aws-terminal.json','verification.json','root-replay.json'}
    assert set(authority) == {'directory','files'} and set(authority['files']) == roster
    for n,p in authority['files'].items():
        assert p['path'] == str(Path(authority['directory'])/n)
        read(repo,p)
    verified = json.loads(read(repo,authority['files']['verification.json']))
    assert verified['valid_construction'] is verified['remote_exhaustive_replay'] is True
    assert verified['instance_state'] == 'terminated' and verified['complete_historical_coverage'] is False
    assert verified['terminal_sha256'] == authority['files']['aws-terminal.json']['sha256']
    checked = panel.replay(directory)  # Existing metadata replay; never the oracle.
    assert checked['constructed'] is checked['remote_exhaustive_replay'] is True and checked['exit_status'] == 0
    construction = json.loads(read(repo,authority['files']['construction-config.json']))
    sealed = json.loads(read(repo,authority['files']['screen/seal-readback.json']))['artifacts']
    for n in ('queries.raw','requests.jsonl','truth.i64','panel.json','oracle.json'):
        assert identity(sealed[n]) == identity(authority['files']['screen/'+n])
    return construction


def qualify(config_path, expected_sha, repo):
    repo,config_path = Path(repo).resolve(),Path(config_path).resolve()
    assert config_path == repo/CONFIG and config_path.is_file() and not config_path.is_symlink(), 'root regular config path'
    assert config_path.stat().st_size <= 65536
    body = config_path.read_bytes()
    assert sha(body) == expected_sha, 'config identity'
    config = json.loads(body)
    assert config['authority_pending'] is False, 'root freeze pending'
    assert set(config) == set(FIXED) | {'authority_pending','code_sha256','refs','panel','inputs','binaries'}
    assert all(type(config[n]) is type(v) and config[n] == v for n,v in FIXED.items()), 'fixed quality/resource protocol'
    assert set(config['code_sha256']) == set(CODE), 'exact transitive code closure'
    assert all(artifact(panel.repo_path(repo,n))['sha256'] == d for n,d in config['code_sha256'].items()), 'adapter/helper source drift'
    values,proof = native_authority(repo,config['refs'],config['binaries'])
    construction = panel_authority(repo,config['panel'])
    inputs,source = config['inputs'],values['input_authorities']
    assert set(inputs) == {'parquet','sq8','order','builder'}
    assert all(set(p) == {'key','bytes','sha256'} and type(p['key']) is str and p['key'] and '\n' not in p['key'] for p in inputs.values())
    assert inputs['parquet'] == construction['source_parquet']
    assert inputs['sq8'] == {k:source['sq8'][k] for k in ('key','bytes','sha256')}
    for n,s in (('order','order.u64'),('builder','builder.json')):
        assert inputs[n] == {k:source['metadata'][s][k] for k in ('key','bytes','sha256')}
    assert construction['source_raw_sha256'] == source['raw']['sha256'] and source['raw']['bytes'] == 3_072_000_000
    assert source['order_complete_bijection_verified'] is source['coefficient_f32_bits_compared'] is True
    proof.update(config_path=str(CONFIG),config_sha256=expected_sha,
        code_identity_sha256=sha(encoded(config['code_sha256'])),
        refs_identity_sha256=sha(encoded(config['refs'])),panel_identity_sha256=sha(encoded(config['panel'])),
        inputs_identity_sha256=sha(encoded(inputs)),qualification=False,physical_s3_measured=False)
    return config,proof


def validate_cgroup(report):
    with patch.object(panel,'MEMORY',MEMORY):
        panel.validate_cgroup(report)


def ordinal_check(sq8, order, rows=1_000_000, dimensions=768):
    """Stream every packed record and authenticate the complete permutation."""
    assert artifact(sq8)['bytes'] == rows*(dimensions+12) and artifact(order)['bytes'] == rows*8
    seen = bytearray((rows+7)//8)
    with Path(sq8).open('rb') as packed, Path(order).open('rb') as permutation:
        for p in range(rows):
            record,word = packed.read(dimensions+12),permutation.read(8)
            row = struct.unpack('<Q',word)[0]
            assert row < rows and not seen[row//8] & (1 << (row%8)), 'order not a bijection'
            seen[row//8] |= 1 << (row%8)
            assert struct.unpack_from('<q',record)[0] == row, 'SQ8 id[p] != order[p] at '+str(p)
            assert math.isfinite(struct.unpack_from('<f',record,8)[0]), 'SQ8 nonfinite norm'
        assert not packed.read(1) and not permutation.read(1)
    return dict(rows_checked=rows,record_bytes=dimensions+12,id_matches_order=True,complete_bijection=True,
                sq8=artifact(sq8),order=artifact(order))


def local_head(path):
    """Pinned object_store 0.14.1 Unix LocalFileSystem get_etag formula."""
    assert not Path(path).is_symlink()
    with Path(path).open('rb') as opened:
        stat = os.fstat(opened.fileno())
    assert stat.st_mtime_ns >= 0
    micros = stat.st_mtime_ns//1000
    return dict(backend='object_store.LocalFileSystem',version='0.14.1',
        inode=stat.st_ino,mtime_ns=stat.st_mtime_ns,mtime_microseconds=micros,bytes=stat.st_size,
        etag=f'"{stat.st_ino:x}-{micros:x}-{stat.st_size:x}"',
        conditional_production_read_required=True,archived_s3_etag_used=False)


def run_process(args, log_path, seconds, timing=None):
    """Own the process group and reap it on failure, timeout and success."""
    started = time.monotonic()
    command = (['/usr/bin/time','-v','-o',str(timing),*args] if timing else args)
    with Path(log_path).open('ab') as log:
        process = subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:
            status = process.wait(timeout=seconds)
        finally:
            try:
                os.killpg(process.pid,signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            try:
                os.killpg(process.pid,signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
            log.flush(); os.fsync(log.fileno())
    result = dict(command=args,exit_status=status,wall_seconds=time.monotonic()-started,process_cleanup=True)
    if timing:
        text = Path(timing).read_text()
        result['process_peak_rss_kib'] = int(text.split('Maximum resident set size (kbytes): ',1)[1].splitlines()[0])
    return result


def score_summary(records, proof, scorer_config_sha):
    events = [json.loads(line) for line in Path(records).read_text().splitlines()]
    assert events[0]['phase'] == 'identity' and events[0]['schema'] == 'borsuk-semantic-router-scorer-result-v2'
    assert events[0]['config_sha256'] == scorer_config_sha and events[0]['binary_sha256'] == proof['binary_sha256']['scorer']
    for key,path in (('scorer_source_sha256',SCORER_SOURCE),('router_source_sha256','crates/borsuk/src/semantic_unit_router.rs')):
        assert events[0][key] == proof['native_source_sha256'][path]
    assert not events[0]['physical_s3_measured']
    frozen = [e for e in events if e['phase'] == 'frozen_query']
    evaluated = [e for e in events if e['phase'] == 'evaluation']
    assert [e['ordinal'] for e in frozen] == [e['ordinal'] for e in evaluated] == list(range(64))
    assert all(e['truth_opened'] is False for e in frozen)
    for e in frozen:
        assert len(e['returned_ids'])==len(set(e['returned_ids'])) and 10<=len(e['returned_ids'])<=100
        assert all(type(row) is int and 0<=row<1_000_000 for row in e['returned_ids'])
        for n,cap in (('source_gets',128),('source_bytes',64*1024**2),('sq8_gets',32),('sq8_bytes',16773120)):
            assert type(e[n]) is int and 0<=e[n]<=cap, 'unchanged production cap: '+n
    for e in evaluated:
        for n in ('nominated_units','page_closure','source_scored_units','source_ranked_pages','sq8_admitted_ranges'):
            assert set(e[n])=={'hits10','hits100'}
            assert type(e[n]['hits10']) is int and 0<=e[n]['hits10']<=10
            assert type(e[n]['hits100']) is int and 0<=e[n]['hits100']<=100
    boundary = [i for i,e in enumerate(events) if e['phase']=='all_queries_frozen']
    assert len(boundary)==1 and events[boundary[0]]['count']==64
    assert all(i<boundary[0] for i,e in enumerate(events) if e['phase']=='frozen_query')
    assert all(i>boundary[0] for i,e in enumerate(events) if e['phase']=='evaluation')
    summary = events[-1]['summary']
    assert events[-1]['phase']=='terminal' and summary['complete'] is True and summary['queries']==64
    hits10 = sum(e['returned_hits10'] for e in evaluated)
    hits100 = sum(e['returned_hits100'] for e in evaluated)
    assert all(type(e['returned_hits10']) is int and 0<=e['returned_hits10']<=10
        and type(e['returned_hits100']) is int and 0<=e['returned_hits100']<=100 for e in evaluated)
    assert (summary['hits10'],summary['hits100']) == (hits10,hits100)
    assert summary['mean_returned_r10']==hits10/640 and summary['mean_returned_r100']==hits100/6400
    assert summary['status']==('PASS' if hits10>=608 else 'FAIL') and summary['physical_s3_measured'] is False
    return dict(summary,local_logical_io_only=True,retuned=False,
        decomposition_recorded=True,current_whole_tree_full_execution=False)


def main(config_path, expected_sha, repo, output):
    repo,output = Path(repo).resolve(),Path(output).absolute()
    config,proof = qualify(config_path,expected_sha,repo)
    assert output.is_absolute() and not output.is_symlink() and not output.exists()
    assert not output.resolve().is_relative_to(repo), 'scratch outside source required'
    counters = dict(before=ids.capture_cgroup(),closed=False)
    validate_cgroup(dict(counters,after=counters['before'],closed=True))
    output.mkdir(parents=True,exist_ok=False)
    scratch = output/'scratch'; scratch.mkdir()
    report = dict(schema='borsuk-semantic-1m-quality-resources-v1',stages={},
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
        write(output/'preparation.log',b'Authenticate downloads; extract original raw; verify SQ8 source ordinal mapping.\n')
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
        # Existing extraction in a timed child, in this same service/cgroup.
        raw_sha = json.loads(read(repo,config['refs']['input_authorities']))['raw']['sha256']
        result = run_process([sys.executable,'-c',
            'from pathlib import Path; import sys; from scripts import seal_v36_rank16_fresh_1m as seal; seal.source_raw(Path(sys.argv[1]),Path(sys.argv[2]),sys.argv[3])',
            str(inputs/'parquet'),str(scratch/'raw'),raw_sha],output/'preparation.log',remaining(),output/'extraction-resources.txt')
        assert result['exit_status']==0, 'original-ordinal extraction failed'
        report['stages']['extraction']=dict(result,cgroup=ids.capture_cgroup())
        assert artifact(scratch/'raw')==dict(bytes=3_072_000_000,sha256=raw_sha)
        check = ordinal_check(inputs/'sq8',inputs/'order')
        write(output/'sq8-ordinal-check.json',check)
        remaining()
        historical = json.loads((inputs/'builder').read_bytes())
        assert historical['raw_sha256']==raw_sha and historical['sq8_sha256']==config['inputs']['sq8']['sha256']
        assert (historical['rows'],historical['dimensions'])==(1_000_000,768)
        for n in ('low','step'):
            assert len(historical[n])==768 and all(math.isfinite(v) for v in historical[n])
        store = scratch/'store'; (store/'quality/objects').mkdir(parents=True)
        sq8 = store/'quality/objects'/check['sq8']['sha256']
        (inputs/'sq8').rename(sq8)
        head = local_head(sq8); write(output/'local-sq8-head.json',head)
        write(output/'input-hashes.json',dict(inputs=config['inputs'],raw=artifact(scratch/'raw'),
            panel=config['panel'],binaries={n:artifact(binaries/n) for n in config['binaries']}))
        builder = dict(discovery='semantic',semantic_profile='fresh1m',
            order=dict(path=str(inputs/'order'),sha256=check['order']['sha256']),
            raw=str(scratch/'raw'),raw_sha256=raw_sha,sq8=str(sq8),sq8_sha256=check['sq8']['sha256'],
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
        authority = config['panel']['files']
        scorer = dict(schema='borsuk-semantic-router-scorer-config-v2',dataset='ReLAION',profile='fresh1m',
            rows=1_000_000,dimensions=768,first=0,count=64,store_root=str(store),
            generation_prefix='quality/index',generation_root_sha256=root_sha,
            scratch_parent=str(scratch),max_memory_bytes=PAYLOAD,
            order=dict(path=str(inputs/'order'),**check['order']))
        for n,name in (('requests','requests.jsonl'),('truth','truth.i64')):
            p = authority['screen/'+name]
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
        write(output/'resources.json',report); write(output/'quality-cgroup.json',counters)
        validate_cgroup(counters)
        assert report['wall_seconds']<=7200, 'whole service deadline'


if __name__ == '__main__':
    assert len(sys.argv)==5, 'usage: CONFIG CONFIG_SHA REPO NEW_OUTPUT'
    main(*sys.argv[1:])
