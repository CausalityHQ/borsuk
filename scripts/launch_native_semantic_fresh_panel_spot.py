"""Root-frozen fresh-panel construction; no local vector/oracle execution.

CLI: aNNNN | --self-check | --stage REPO OUTPUT PREFIX | --replay OUTPUT.
main(attempt) delegates the unchanged metadata Spot lifecycle; collect/replay
authenticate small completion bodies and the remote exhaustive-replay receipt.
--stage runs the unchanged preparation helper and its --replay subprocess in
the SAME service/cgroup. It requires BORSUK_PANEL_{CONFIG_SHA256,SOURCE_COMMIT,
ARCHIVE_SHA256}. The helper's conditional PUT and HEAD/GET readback are intact.

ROOT/config.json: FIXED plus controller_authority_pending=false,
controller_code_sha256={exact CODE path: SHA256}, and
construction_config={repo-relative path,bytes,sha256}. That pointer must bind
the existing helper schema, exact unchanged 20-file helper closure and seven
authenticated refs. The draft's sole null panel must be replaced by authentic
selected-panel bytes. This wrapper never creates/fills/freezes a root config.

ARTIFACTS is the exact small terminal/upload roster. source.parquet, source.raw,
consumed-queries.raw and selected shard downloads stay on the disposable worker;
input-hashes.json authenticates their identities. --replay NEVER calls the 1M
oracle locally. Neither completion nor the mock self-check claims ANN quality,
historical coverage or production qualification.

Bounded check (stdlib and mocked helper/dependencies/SDK, no network):
  timeout 55s python3 -m scripts.launch_native_semantic_fresh_panel_spot --self-check
"""
import contextlib
import fcntl
import importlib.metadata
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
from unittest.mock import patch

if not __debug__:
    raise RuntimeError('authority checks require assertions enabled')
if __package__ in (None, ''):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import launch_native_semantic_panel_ids_spot as ids

ROOT = Path('docs/research/performance-architecture-20260930/semantic-1m/panel-tools/remote-construction')
CONFIG = ROOT / 'config.json'
NAME = ''
MODULE = 'scripts.launch_native_semantic_fresh_panel_spot'
HELPER_MODULE = 'scripts.prepare_semantic_1m_fresh_panel'
SCHEMA = 'borsuk-semantic-fresh-panel-spot-v1'
PREFIX = 'research/semantic-router/20261001/fresh-panel-'
TOKEN_PREFIX, TAG = 'semantic-fresh-panel-', 'borsuk-semantic-fresh-panel'
WALL, WORKER_SECONDS, SERVICE_SECONDS = 9000, 7200, 7260
MEMORY = ids.MEMORY
INSTANCE_TYPE, IMAGE_ID = ids.INSTANCE_TYPE, ids.IMAGE_ID
ROOT_DEVICE_NAME, SUBNET = ids.ROOT_DEVICE_NAME, ids.SUBNET
REGION, BUCKET = ids.REGION, ids.BUCKET
SPOT_MAX_USD_PER_HOUR, COMPUTE_CAP = .50, 1.25
AWSCLI_VERSION, AWSCLI_SHA256 = ids.AWSCLI_VERSION, ids.AWSCLI_SHA256
THREAD_ENV = ('OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS','BLIS_NUM_THREADS','NUMEXPR_NUM_THREADS')
AUTHORITY = ROOT.parent / 'construction-config.source-id-v2.draft.json'
AUTHORITY_SHA = '88f5136c1c33b7a3ddd74a82b0543297b84ee4525fef82fc837cc4f3111cdbd5'
CODE = tuple(sorted((*ids.CODE, 'scripts/launch_native_semantic_fresh_panel_spot.py')))
OUTPUTS = ('queries.raw', 'requests.jsonl', 'truth.u32', 'truth.i64', 'panel.json',
           'duplicate-audit.json', 'oracle.json', 'resources.json')
SCREEN = (*OUTPUTS, 'decision.json', 'seal-readback.json')
ARTIFACTS = ('source-qualification.json', 'config.json', 'construction-config.json',
    'input-hashes.json', 'remote-replay.json', 'construction-cgroup.json',
    'tool-versions.json', 'test.log', 'test-resources.txt', 'run-closed.log',
    *('screen/' + name for name in SCREEN))
FIXED = dict(schema='borsuk-semantic-fresh-panel-construction-v1',
    architecture='x86_64', region=REGION, bucket=BUCKET, instance_type=INSTANCE_TYPE,
    image_id=IMAGE_ID, root_device_name=ROOT_DEVICE_NAME, subnet_id=SUBNET,
    volume_gib=80, volume_type='gp3', encrypted=True, delete_on_termination=True,
    spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR, compute_cap_usd=COMPUTE_CAP,
    ebs_s3_allowance_usd=.15, memory_bytes=MEMORY, swap_bytes=0,
    cpu_quota_percent=200, tasks_max=512, worker_limit_seconds=WORKER_SECONDS,
    service_limit_seconds=SERVICE_SECONDS, machine_limit_seconds=WALL,
    versions={'numpy':'2.3.3', 'pyarrow':'24.0.0'}, blas_openmp_threads=2,
    rows=1_000_000, dimensions=768, queries=64, k=100,
    reservoir_ordinals=[1000,1063], quality_peek_allowed=False,
    complete_historical_coverage=False)
TERMINAL_IDENTITIES = ('config_sha256', 'code_identity_sha256',
    'construction_config_sha256', 'construction_code_identity_sha256',
    'refs_identity_sha256', 'artifact_roster_sha256', 'campaign_schema',
    'awscli_version', 'awscli_sha256')
encoded, sha, artifact, write = ids.encoded, ids.sha, ids.artifact, ids.write


def repo_path(repo, name):
    assert type(name) is str and name and not Path(name).is_absolute(), 'relative repository path required'
    path = (repo/name).resolve()
    assert path.is_relative_to(repo.resolve()), 'reference escapes repository'
    assert not (repo/name).is_symlink(), 'regular repository body required'
    return path


def authenticated(repo, pointer):
    assert set(pointer) == {'path','bytes','sha256'}, 'exact body pointer required'
    assert type(pointer['bytes']) is int and pointer['bytes'] > 0
    assert type(pointer['sha256']) is str and re.fullmatch('[0-9a-f]{64}', pointer['sha256'])
    path = repo_path(repo, pointer['path'])
    assert artifact(path) == {k:pointer[k] for k in ('bytes','sha256')}, 'reference body identity'
    return path.read_bytes()


def construction(repo, pointer, body=None):
    """Authenticate helper authority using stdlib and the existing panel validator."""
    draft_body = (repo/AUTHORITY).read_bytes()
    assert sha(draft_body) == AUTHORITY_SHA, 'helper authority changed'
    draft = json.loads(draft_body)
    body = authenticated(repo, pointer) if body is None else body
    assert dict(bytes=len(body), sha256=sha(body)) == {k:pointer[k] for k in ('bytes','sha256')}
    config = json.loads(body)
    assert set(config['refs']) == set(draft['refs']) and len(config['refs']) == 7
    panel = config['refs']['panel']
    assert all(panel.get(k) is not None for k in ('path','bytes','sha256')), 'selected panel pending'
    expected = dict(draft, refs=dict(draft['refs'], panel=panel))
    assert encoded(config) == encoded(expected), 'frozen helper configuration/source descriptors'
    assert len(config['code_sha256']) == 20
    for name,digest in config['code_sha256'].items():
        assert artifact(repo_path(repo,name))['sha256'] == digest, 'helper code drift: ' + name
    bodies = {name:authenticated(repo,p) for name,p in config['refs'].items()}
    old, verified = (json.loads(bodies[n]) for n in ('old_config','old_verification'))
    assert verified['valid_construction'] is True and verified['ann_quality_measured'] is False
    assert verified['config_sha256'] == config['refs']['old_config']['sha256']
    assert verified['panel_sha256'] == old['panel_sha256'] == config['refs']['old_panel']['sha256']
    assert old['registry_sha256'] == config['refs']['registry']['sha256']
    assert old['bucket'] == config['bucket'] and old['source_parquet'] == config['source_parquet']
    assert old['source_raw_sha256'] == config['source_raw_sha256']
    assert verified['sealed_artifacts']['queries.raw'] == config['consumed_queries']
    # Reuse the metadata-only validator unchanged; this is not a selector.
    with tempfile.TemporaryDirectory(prefix='semantic-panel-authority-') as directory:
        layout = Path(directory)
        write(layout/'panel.json', bodies['panel'])
        for name in ('old_panel','overlap'):
            write(layout/f'inputs/{name}.json', bodies[name])
        checked = ids.validate_panel(layout)
    combined = json.loads(bodies['old_panel'])['selected'] + checked['selected']
    assert len({(r['source_rank'],r['source_row_offset']) for r in combined}) == 1064, 'repeated physical locator'
    ids.ranked_shards(*(repo/config['refs'][n]['path'] for n in ('registry','population','overlap')))
    return config


def qualify(base=Path('.')):
    base = Path(base).resolve()
    body = (base/CONFIG).read_bytes()  # Missing or pending authority stops before cloud.
    config = json.loads(body)
    assert config['controller_authority_pending'] is False, 'root authority freeze pending'
    assert all(type(config[k]) is type(v) and config[k] == v for k,v in FIXED.items()), 'fixed construction resources/protocol'
    assert set(config) == set(FIXED) | {'controller_authority_pending','controller_code_sha256','construction_config'}
    code = config['controller_code_sha256']
    assert set(code) == set(CODE), 'exact transitive controller code roster'
    assert all(artifact(repo_path(base,n))['sha256'] == d for n,d in code.items()), 'controller code drift'
    helper = construction(base, config['construction_config'])
    return dict(config_path=str(CONFIG), config_sha256=sha(body), campaign_schema=SCHEMA,
        code_sha256=code, code_identity_sha256=sha(encoded(code)),
        construction_config=config['construction_config'],
        construction_config_sha256=config['construction_config']['sha256'],
        construction_code_sha256=helper['code_sha256'],
        construction_code_identity_sha256=sha(encoded(helper['code_sha256'])),
        refs=helper['refs'], refs_identity_sha256=sha(encoded(helper['refs'])),
        artifact_roster_sha256=sha(encoded(ARTIFACTS)), ann_quality_measured=False,
        complete_historical_coverage=False, qualification=False,
        awscli_version=AWSCLI_VERSION, awscli_sha256=AWSCLI_SHA256)


def preflight(base=Path('.')):
    proof = qualify(base)
    assert not subprocess.check_output(['git','status','--porcelain'], cwd=base, text=True).strip(), 'dirty source'
    return proof


def tool_versions():
    result = ids.tool_versions()
    result['numpy'] = importlib.metadata.version('numpy')
    assert result['numpy'] == FIXED['versions']['numpy']
    result['thread_environment'] = {n:os.environ[n] for n in THREAD_ENV}
    assert result['thread_environment'] == dict.fromkeys(THREAD_ENV,'2'), 'BLAS/OpenMP thread caps'
    return result


def validate_cgroup(report):
    """Check enforced limits; kernel peak/reclaim observations remain unmodified.

    memory.peak can exceed memory.max during reclaim without an OOM. Record it,
    along with memory.events.max, rather than treating it as an enforcement knob.
    """
    assert report['closed'] is True
    for counters in (report['before'],report['after']):
        assert int(counters['memory.max']) == MEMORY and int(counters['memory.peak']) >= 0
        assert int(counters['memory.swap.max']) == int(counters['memory.swap.peak']) == 0
        quota,period = map(int,counters['cpu.max'].split())
        assert period > 0 and quota == period*2 and int(counters['pids.max']) == 512
        assert 0 < int(counters['pids.current']) <= 512 and counters['cpu.stat'].strip()
        assert counters['observer_pid'] in counters['process_ids']
        for name in ('memory.events','memory.swap.events','pids.events'):
            events = dict(line.split() for line in counters[name].splitlines())
            failures = ('oom','oom_kill','oom_group_kill') if name == 'memory.events' else events
            assert all(int(events.get(key,0)) == 0 for key in failures), 'resource failure: ' + name
    assert report['before']['cgroup'] == report['after']['cgroup'], 'cgroup changed'
    assert set(report['after']['process_ids']) <= set(report['before']['process_ids']), 'construction descendants remain'


def query_sources(repo, config):
    shards = ids.ranked_shards(*(repo/config['refs'][n]['path'] for n in ('registry','population','overlap')))
    panel = json.loads((repo/config['refs']['panel']['path']).read_bytes())
    return [dict(rank=rank, path=shards[rank]['path'], bytes=shards[rank]['encoded_bytes'],
                 sha256=shards[rank]['sha256']) for rank in sorted({r['source_rank'] for r in panel['selected']})]


def commands(repo, out, prefix, proof):
    args = [str(repo/proof['construction_config']['path']), proof['construction_config_sha256'], str(repo), str(out/'screen')]
    return ([sys.executable,'-m',HELPER_MODULE,*args,prefix],
            [sys.executable,'-m',HELPER_MODULE,'--replay',*args])


def stage(repo, out, prefix):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    assert out.is_dir() and not out.is_symlink() and not out.is_relative_to(repo), 'owned output outside source required'
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}', prefix), 'fresh attempt prefix'
    counters = dict(before=ids.capture_cgroup(), closed=False)
    receipt = dict(schema='borsuk-semantic-fresh-panel-remote-replay-v1',
        prepared=False, replayed=False, ann_quality_measured=False, qualification=False,
        complete_historical_coverage=False)
    error = None
    try:
        validate_cgroup(dict(counters, after=counters['before'], closed=True))
        proof = qualify(repo)
        assert proof['config_sha256'] == os.environ['BORSUK_PANEL_CONFIG_SHA256'], 'bootstrap config pin'
        source = dict(source_commit=os.environ['BORSUK_PANEL_SOURCE_COMMIT'],
            source_archive_sha256=os.environ['BORSUK_PANEL_ARCHIVE_SHA256'])
        assert re.fullmatch('[0-9a-f]{40}',source['source_commit']) and re.fullmatch('[0-9a-f]{64}',source['source_archive_sha256'])
        config_body = (repo/proof['construction_config']['path']).read_bytes()
        config = json.loads(config_body)
        write(out/'source-qualification.json', dict(proof, **source))
        write(out/'config.json', (repo/CONFIG).read_bytes())
        write(out/'construction-config.json', config_body)
        write(out/'tool-versions.json', tool_versions())
        prepare_command, replay_command = commands(repo,out,prefix,proof)
        receipt.update(**source, config_sha256=proof['config_sha256'],
            construction_config_sha256=proof['construction_config_sha256'],
            code_identity_sha256=proof['code_identity_sha256'], prefix=prefix,
            prepare_command=prepare_command, replay_command=replay_command)
        assert not (out/'screen').exists() and not (out/'screen').is_symlink(), 'fresh helper output required'
        with (out/'test.log').open('x') as log:
            env = dict(os.environ, PYTHONPATH=str(repo))
            result = subprocess.run(prepare_command, cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
            log.flush(); os.fsync(log.fileno())
            receipt['prepare_exit_status'] = result.returncode
            assert result.returncode == 0, 'preparation failed; no replacement or retry'
            receipt['prepared'] = True
            inputs = {'source.parquet': config['source_parquet'],
                'source.raw':dict(bytes=3_072_000_000, sha256=config['source_raw_sha256']),
                'consumed-queries.raw':config['consumed_queries']}
            for name,pointer in inputs.items():
                assert artifact(out/'screen'/name) == {k:pointer[k] for k in ('bytes','sha256')}, 'remote input body: ' + name
            write(out/'input-hashes.json', dict(schema='borsuk-semantic-fresh-panel-inputs-v1',
                construction_config=proof['construction_config'], refs=config['refs'],
                code_sha256=config['code_sha256'], inputs=inputs,
                authenticated_query_sources=query_sources(repo,config)))
            result = subprocess.run(replay_command, cwd=repo, env=env, stdout=subprocess.PIPE,
                stderr=log, text=True, check=False)
            log.write(result.stdout); log.flush(); os.fsync(log.fileno())
            receipt['replay_exit_status'] = result.returncode
            assert result.returncode == 0, 'remote exhaustive replay failed'
            checked = json.loads(result.stdout)
            assert checked == dict(passed=True, remote_objects_reopened=False, ann_quality_measured=False)
            assert qualify(repo) == proof, 'authority changed during construction/replay'
            receipt.update(replayed=True, helper_replay=checked,
                input_hashes=artifact(out/'input-hashes.json'),
                artifacts={name:artifact(out/'screen'/name) for name in SCREEN})
    except BaseException as failure:
        error = failure
        receipt.update(replayed=False, error_type=type(failure).__name__, error=str(failure))
        raise
    finally:
        counters.update(after=ids.capture_cgroup(), closed=True)
        try:
            validate_cgroup(counters)
        except AssertionError:
            receipt['replayed'] = False
            if error is None:
                raise
        finally:
            write(out/'construction-cgroup.json', counters)
            write(out/'remote-replay.json', receipt)
    return receipt


def user_data(commit, archive_sha, archive_key, prefix, qualification):
    assert re.fullmatch('[0-9a-f]{40}',commit) and re.fullmatch('[0-9a-f]{64}',archive_sha)
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}',prefix)
    assert qualification['config_path'] == str(CONFIG) and qualification['campaign_schema'] == SCHEMA
    _, bootstrap = ids.lifecycle()
    adapter = {k:qualification[k] for k in TERMINAL_IDENTITIES}
    adapter.update(config_path=str(CONFIG), native_binary={'key':'unused'}, native_publisher={'key':'unused'})
    with patch.multiple(bootstrap, WALL=WALL, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS,
                        TERMINAL_IDENTITIES=TERMINAL_IDENTITIES), patch.object(bootstrap,'_offered',return_value=False):
        body = bootstrap.user_data(commit,archive_sha,archive_key,prefix,adapter)
    command = f'''phase=install
test "$(uname -m)" = x86_64
. /etc/os-release
test "$ID" = ubuntu && test "$VERSION_ID" = 24.04
python3.12 -m venv "$root/venv"
"$root/venv/bin/python" -m pip install --disable-pip-version-check --only-binary=:all: --no-deps numpy==2.3.3 pyarrow==24.0.0
phase=construction
systemd-run --unit=semantic-fresh-panel --wait --pipe -p MemoryMax=8G -p MemorySwapMax=0 -p CPUQuota=200% -p TasksMax=512 -p RuntimeMaxSec=7260 -p WorkingDirectory="$root" \\
 --setenv=PYTHONPATH="$root/repo" --setenv=AWS_MAX_ATTEMPTS=1 --setenv=BORSUK_PANEL_CONFIG_SHA256={qualification['config_sha256']} \\
 --setenv=BORSUK_PANEL_SOURCE_COMMIT={commit} --setenv=BORSUK_PANEL_ARCHIVE_SHA256={archive_sha} \\
 --setenv=OPENBLAS_NUM_THREADS=2 --setenv=OMP_NUM_THREADS=2 --setenv=MKL_NUM_THREADS=2 --setenv=BLIS_NUM_THREADS=2 --setenv=NUMEXPR_NUM_THREADS=2 \\
 /usr/bin/time -v -o "$root/test-resources.txt" timeout --signal=TERM --kill-after=30 7200 \\
 "$root/venv/bin/python" -m {MODULE} --stage "$root/repo" "$root" {prefix}
for name in $ARTIFACT_NAMES; do
 if [ "$name" = run-closed.log ]; then test -s run.log; else test -s "$name"; fi
done
'''
    start,end = body.index('phase=install\n'),body.index('phase=complete\n')
    body = body[:start] + command + body[end:]
    body = body.replace('/mnt/native-semantic-router-cold','/mnt/native-semantic-fresh-panel')
    body = body.replace('python3-boto3 python3.12','python3.12 python3.12-venv')
    body = body.replace(", 'runtime_abi_sha256':artifacts.get('runtime-abi.json',{}).get('sha256')", '')
    marker = "'original_exit_code':int(os.environ['ORIGINAL_EXIT_CODE']),"
    assert body.count(marker) == 1
    body = body.replace(marker, marker + "'source_qualification_sha256':artifacts.get('source-qualification.json',{}).get('sha256'),'remote_replay_sha256':artifacts.get('remote-replay.json',{}).get('sha256'),")
    subprocess.run(['bash','-n'],input=body,text=True,check=True)
    compile(body.split("python3 - <<'PY' >terminal.json\n",1)[1].split('\nPY\n',1)[0],'<terminal>','exec')
    assert len(body.encode()) < 16384
    return body


def poll(ec2,s3,prefix,instance_id,started):
    shared,_ = ids.lifecycle()
    with patch.object(shared,'WALL',WALL):
        return shared.poll(ec2,s3,prefix,instance_id,started)


def validate_screen(repo, out, config, proof, prefix):
    screen = out/'screen'
    decision = json.loads((screen/'decision.json').read_bytes())
    fixed = dict(schema='borsuk-semantic-1m-construction-v1',
        config_sha256=proof['construction_config_sha256'], refs=config['refs'],
        code_sha256=config['code_sha256'], source_parquet=config['source_parquet'],
        source_raw_sha256=config['source_raw_sha256'], consumed_queries=config['consumed_queries'],
        qualification=False, ann_quality_measured=False, complete_historical_coverage=False,
        reservoir_ordinals=[1000,1063], query_ordinals=[0,63], queries=64, gt_k=100,
        truth_id_space='source ordinal', prefix=prefix, resource_pointer='resources.json',
        oracle_pointer='oracle.json', duplicate_audit_pointer='duplicate-audit.json',
        seal_completion_pointer='seal-readback.json')
    assert encoded({k:decision[k] for k in fixed}) == encoded(fixed), 'helper decision binding'
    assert set(decision['artifacts']) == set(OUTPUTS)
    for name,ident in decision['artifacts'].items():
        assert ident == dict(path=name, **artifact(screen/name)), 'helper output body: ' + name
    assert artifact(screen/'panel.json') == {k:config['refs']['panel'][k] for k in ('bytes','sha256')}
    for name,size in (('queries.raw',196608),('truth.u32',25600),('truth.i64',51200)):
        assert artifact(screen/name)['bytes'] == size, 'output geometry'
    audit = json.loads((screen/'duplicate-audit.json').read_bytes())
    fixed = dict(schema='borsuk-semantic-1m-vector-audit-v1', passed=True,
        normalization=config['normalization'], new_rows_audited=64,
        indexed_rows_audited=1_000_000, consumed_rows_audited=1000,
        replacement_allowed=False, complete_historical_coverage=False,
        config_sha256=proof['construction_config_sha256'],
        panel_sha256=config['refs']['panel']['sha256'], source_raw_sha256=config['source_raw_sha256'],
        consumed_queries=config['consumed_queries'], authenticated_query_sources=query_sources(repo,config))
    assert encoded({k:audit[k] for k in fixed}) == encoded(fixed), 'duplicate audit binding'
    for name in ('raw_sha256','unit_sha256'):
        assert len(audit[name]) == len(set(audit[name])) == 64
        assert all(re.fullmatch('[0-9a-f]{64}',h) for h in audit[name])
    oracle = json.loads((screen/'oracle.json').read_bytes())
    assert oracle['schema'] == 'borsuk-semantic-1m-oracle-v1'
    assert oracle['passed'] is oracle['oracle_self_check'] is oracle['exhaustive_block_sort_top100_merge'] is True
    assert (oracle['rows'],oracle['queries'],oracle['k']) == (1_000_000,64,100)
    assert oracle['truth_id_space'] == 'source ordinal' and oracle['source_raw_sha256'] == config['source_raw_sha256']
    assert oracle['truth_u32'] == decision['artifacts']['truth.u32'] and oracle['truth_i64'] == decision['artifacts']['truth.i64']
    sealed = json.loads((screen/'seal-readback.json').read_bytes())
    expected = dict(decision['artifacts'], **{'decision.json':dict(path='decision.json', **artifact(screen/'decision.json'))})
    assert sealed['schema'] == 'borsuk-semantic-1m-seal-readback-v1' and sealed['config_identity_in_decision'] is True
    assert set(sealed['artifacts']) == set(expected)
    for name,ident in expected.items():
        entry = sealed['artifacts'][name]
        assert entry['authenticated_readback'] is True and entry['key'] == prefix+'/sealed/'+name
        assert {k:entry[k] for k in ('bytes','sha256')} == {k:ident[k] for k in ('bytes','sha256')}
        assert type(entry['etag']) is str and entry['etag']
    inputs = json.loads((out/'input-hashes.json').read_bytes())
    assert inputs == dict(schema='borsuk-semantic-fresh-panel-inputs-v1',
        construction_config=proof['construction_config'], refs=config['refs'],
        code_sha256=config['code_sha256'], inputs={'source.parquet':config['source_parquet'],
            'source.raw':dict(bytes=3_072_000_000,sha256=config['source_raw_sha256']),
            'consumed-queries.raw':config['consumed_queries']},
        authenticated_query_sources=query_sources(repo,config))


def replay(out):
    """Read small receipts only; never import the helper or rerun its oracle."""
    out = Path(out)
    repo = Path(__file__).resolve().parents[1]
    reservation,launch,closed,terminal = (json.loads((out/n).read_bytes()) for n in (
        'aws-reservation.json','aws-launch.json','aws-closeout.json','aws-terminal.json'))
    proof = reservation['qualification']
    assert proof == qualify(repo), 'frozen campaign authority'
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes'], 'same ACKed IDs closed'
    assert terminal['instance_id'] == launch['instance_id'] in {n['instance_id'] for n in closed['nodes'].values()}
    prefix = launch['prefix']
    assert re.fullmatch(re.escape(PREFIX)+r'a[0-9]{4}',prefix)
    assert terminal['schema'] == reservation['schema'] == SCHEMA
    for key,value in dict(wall_seconds=WALL,instance_type=INSTANCE_TYPE,image_id=IMAGE_ID,
        root_device_name=ROOT_DEVICE_NAME,subnet_id=SUBNET,spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR,
        compute_cap_usd=COMPUTE_CAP,ebs_s3_allowance_usd=.15,total_cost_measured=False).items():
        assert type(reservation[key]) is type(value) and reservation[key] == value, 'reservation resource pin: ' + key
    for key in ('source_commit','source_archive_sha256'):
        assert terminal[key] == reservation[key] == launch[key], 'source binding'
    for key in TERMINAL_IDENTITIES:
        assert terminal[key] == proof[key], 'terminal pin: ' + key
    assert set(terminal['artifacts']) <= set(ARTIFACTS), 'unexpected upload body'
    for name,ident in terminal['artifacts'].items():
        assert artifact(out/name) == ident, 'terminal body: ' + name
    for field,name in (('source_qualification_sha256','source-qualification.json'),('remote_replay_sha256','remote-replay.json')):
        assert terminal[field] == terminal['artifacts'].get(name,{}).get('sha256'), 'terminal receipt pin'
    source = {k:terminal[k] for k in ('source_commit','source_archive_sha256')}
    if 'source-qualification.json' in terminal['artifacts']:
        assert json.loads((out/'source-qualification.json').read_bytes()) == dict(proof,**source)
    if 'config.json' in terminal['artifacts']:
        assert sha((out/'config.json').read_bytes()) == proof['config_sha256']
    config = None
    if 'construction-config.json' in terminal['artifacts']:
        config = construction(repo,proof['construction_config'],(out/'construction-config.json').read_bytes())
    assert type(terminal['exit_code']) is type(terminal['original_exit_code']) is int
    complete = terminal['exit_code'] == 0 and terminal['phase'] == 'complete'
    assert terminal['status'] == ('complete' if complete else 'failed')
    if complete:
        assert terminal['original_exit_code'] == 0 and set(terminal['artifacts']) == set(ARTIFACTS), 'exact completed roster'
        report = json.loads((out/'remote-replay.json').read_bytes())
        assert report['schema'] == 'borsuk-semantic-fresh-panel-remote-replay-v1'
        assert report['prepared'] is report['replayed'] is True
        assert report['ann_quality_measured'] is report['qualification'] is report['complete_historical_coverage'] is False
        assert type(report['prepare_exit_status']) is type(report['replay_exit_status']) is int
        assert report['prepare_exit_status'] == report['replay_exit_status'] == 0
        assert report['helper_replay'] == dict(passed=True,remote_objects_reopened=False,ann_quality_measured=False)
        for key in ('config_sha256','construction_config_sha256','code_identity_sha256'):
            assert report[key] == proof[key]
        assert all(report[k] == v for k,v in source.items()) and report['prefix'] == prefix
        assert report['input_hashes'] == artifact(out/'input-hashes.json')
        assert report['artifacts'] == {n:artifact(out/'screen'/n) for n in SCREEN}
        versions = json.loads((out/'tool-versions.json').read_bytes())
        assert versions['numpy'] == '2.3.3' and versions['pyarrow'] == '24.0.0'
        assert versions['thread_environment'] == dict.fromkeys(THREAD_ENV,'2')
        assert versions['python'].startswith('3.12') and versions['machine'] == 'x86_64'
        assert versions['os_release']['ID'] == 'ubuntu' and versions['os_release']['VERSION_ID'] == '24.04'
        first,second = report['prepare_command'],report['replay_command']
        assert len(first) == len(second) == 8 and first[0] == second[0] == versions['executable']
        assert first[1:3] == second[1:3] == ['-m',HELPER_MODULE] and second[3] == '--replay'
        assert first[4] == second[5] == proof['construction_config_sha256'] and first[7] == prefix
        assert first[3:7] == second[4:8]
        remote_repo,remote_screen = Path(first[5]),Path(first[6])
        assert remote_repo.is_absolute() and remote_screen.is_absolute() and remote_screen.name == 'screen'
        assert Path(first[0]).is_absolute(), 'remote executable origin'
        assert first[3] == str(remote_repo/proof['construction_config']['path'])
        assert not remote_screen.is_relative_to(remote_repo)
        cgroup = json.loads((out/'construction-cgroup.json').read_bytes())
        validate_cgroup(cgroup)
        resources = json.loads((out/'screen/resources.json').read_bytes())
        assert resources['ann_quality_measured'] is False and resources['cgroup_path'] == cgroup['before']['cgroup']
        for name in ('memory.max','memory.swap.max','memory.swap.peak'):
            assert int(resources['cgroup'][name]) == (MEMORY if name == 'memory.max' else 0)
        assert int(resources['cgroup']['memory.peak']) >= 0
        events = dict(line.split() for line in resources['cgroup']['memory.events'].splitlines())
        assert all(int(events.get(n,0)) == 0 for n in ('oom','oom_kill','oom_group_kill'))
        assert 0 <= resources['wall_seconds'] <= WORKER_SECONDS and resources['process_max_rss_kib'] >= 0
        validate_screen(repo,out,config,proof,prefix)
    return dict(constructed=complete, remote_exhaustive_replay=complete,
        ann_quality_measured=False, qualification=False, exit_status=terminal['original_exit_code'])


def collect(s3,prefix,out,instance_id,commit,digest):
    out = Path(out)
    launch,closed = (json.loads((out/n).read_bytes()) for n in ('aws-launch.json','aws-closeout.json'))
    assert closed['state'] == 'terminated' and closed['nodes'] == launch['nodes'], 'close ALL original ACKs before collection'
    assert launch['prefix'] == prefix and launch['instance_id'] == instance_id, 'owned collection prefix/instance'
    # The existing streaming collector enforces termination before its first GET.
    with patch.multiple(ids, SCHEMA=SCHEMA, ARTIFACTS=ARTIFACTS, replay=replay):
        return ids.collect(s3,prefix,out,instance_id,commit,digest)


def main(attempt):
    assert re.fullmatch(r'a[0-9]{4}',attempt), 'attempt must be aNNNN'
    before = Path.cwd()
    try:
        os.chdir(Path(__file__).resolve().parents[1])
        shared,_ = ids.lifecycle()
        return shared.main(attempt,campaign=sys.modules[__name__])
    finally:
        os.chdir(before)


def self_check():
    """Real validators and shell/shared lifecycle, synthetic bodies and SDK only."""
    import ast
    import copy
    from datetime import datetime, timezone
    import resource
    import time
    from types import ModuleType
    from unittest.mock import Mock
    started = time.monotonic()
    module = sys.modules[__name__]
    here = Path(__file__).resolve().parents[1]

    def rejects(action):
        try:
            action()
        except (AssertionError,ValueError,KeyError,FileNotFoundError,FileExistsError):
            return
        raise AssertionError('invalid authority accepted')

    def closure(name, seen):
        if name in seen:
            return
        seen.add(name)
        for node in ast.walk(ast.parse((here/name).read_bytes())):
            targets = []
            if isinstance(node,ast.ImportFrom):
                targets = [a.name for a in node.names] if node.module == 'scripts' else (
                    [node.module[8:]] if node.module and node.module.startswith('scripts.') else [])
            elif isinstance(node,ast.Import):
                targets = [a.name[8:] for a in node.names if a.name.startswith('scripts.')]
            for target in targets:
                path = 'scripts/'+target.replace('.','/')+'.py'
                if (here/path).is_file():
                    closure(path,seen)
    seen = set()
    closure('scripts/launch_native_semantic_fresh_panel_spot.py',seen)
    assert seen == set(CODE), 'controller transitive closure drift'
    seen = set()
    closure('scripts/prepare_semantic_1m_fresh_panel.py',seen)
    draft = json.loads((here/AUTHORITY).read_bytes())
    assert seen == set(draft['code_sha256']) and len(seen) == 20, 'helper transitive closure drift'
    counters = {'cgroup':'/synthetic/construction','observer_pid':42,'process_ids':[42],
        'memory.max':str(MEMORY),'memory.peak':str(MEMORY+700416),
        'memory.swap.max':'0','memory.swap.peak':'0',
        'memory.events':'max 7190\noom 0\noom_kill 0\noom_group_kill 0\n',
        'memory.swap.events':'max 0\nfail 0\n','cpu.max':'200000 100000',
        'cpu.stat':'usage_usec 42\n','pids.max':'512','pids.current':'1','pids.events':'max 0\n'}
    validate_cgroup(dict(before=counters,after=counters,closed=True))
    for key,value in [('memory.max',str(MEMORY+1)),('memory.swap.peak','1'),
        ('memory.events','max 0\noom 1\n'),('cpu.max','300000 100000'),
        ('process_ids',[42,43]),('pids.events','max 1\n')]:
        rejects(lambda:validate_cgroup(dict(before=counters,after=dict(counters,**{key:value}),closed=True)))

    sdk,botocore,exceptions = (ModuleType(n) for n in ('boto3','botocore','botocore.exceptions'))
    class SDKError(Exception):
        def __init__(self, **kwargs):
            super().__init__('synthetic SDK failure')
    for name in ('ClientError','EndpointConnectionError','ReadTimeoutError'):
        setattr(exceptions,name,SDKError)
    sdk.Session = Mock(side_effect=AssertionError('real cloud forbidden'))
    botocore.exceptions = exceptions
    fake_deps = {'boto3':sdk,'botocore':botocore,'botocore.exceptions':exceptions}
    with patch.dict(sys.modules,fake_deps), patch('urllib.request.urlopen',side_effect=AssertionError('download forbidden')):
        shared,_ = ids.lifecycle()
        if not (here/CONFIG).exists():
            with patch.object(subprocess,'check_output',return_value=''):
                rejects(lambda:main('a0001'))
        sdk.Session.assert_not_called()
        with tempfile.TemporaryDirectory() as directory:
            tmp = Path(directory)
            repo = tmp/'repo'
            for name in {*CODE,*draft['code_sha256'],str(AUTHORITY),*(p['path'] for n,p in draft['refs'].items() if n != 'panel')}:
                path = repo/name
                path.parent.mkdir(parents=True,exist_ok=True)
                path.write_bytes((here/name).read_bytes())
            old = json.loads((repo/draft['refs']['old_panel']['path']).read_bytes())['selected']
            # Only synthetic metadata for exercising validation, never a real panel.
            rows = [dict(query_ordinal=i,feature_row_id=2**63+i,source_rank=16,
                source_row_offset=i,selector_sha256=sha(ids.SEED+(2**63+i).to_bytes(8,'little'))) for i in range(64)]
            rows.sort(key=lambda r:(r['selector_sha256'],r['feature_row_id']))
            reservoir = old + [dict(r,query_ordinal=1000+i) for i,r in enumerate(rows)]
            panel = dict(schema='borsuk-semantic-1m-fresh-panel-ids-v1',metadata_only=True,
                query_embeddings_or_gt_opened=False,prior_query_audit_pass=False,qualification=False,
                complete_historical_coverage=False,selector_seed=ids.SEED.decode(),
                source_registry_sha256=ids.REFS['registry']['sha256'],
                population_authority_sha256=ids.REFS['population']['sha256'],
                physical_id_report_sha256=ids.REFS['overlap']['sha256'],
                consumed_panel_sha256=ids.REFS['old_panel']['sha256'],
                consumed_prefix_tuple_sha256=ids.tuple_digest(old),reservoir_tuple_sha256=ids.tuple_digest(reservoir),
                reservoir_ordinals=[1000,1063],development_ordinals=[0,63],sealed_ordinals=[],
                selected=[dict(r,query_ordinal=i,reservoir_ordinal=1000+i) for i,r in enumerate(rows)])
            panel_path = repo/'synthetic-panel.json'
            panel_path.write_bytes(encoded(panel))
            helper = dict(draft,refs=dict(draft['refs'],panel=dict(path='synthetic-panel.json',**artifact(panel_path))))
            helper_path = repo/'synthetic-helper.json'
            helper_path.write_bytes(encoded(helper))
            config = dict(FIXED,controller_authority_pending=False,
                controller_code_sha256={n:artifact(repo/n)['sha256'] for n in CODE},
                construction_config=dict(path='synthetic-helper.json',**artifact(helper_path)))
            config_path = repo/CONFIG
            config_path.parent.mkdir(parents=True,exist_ok=True)
            config_path.write_bytes(encoded(config))
            proof = qualify(repo)
            config_path.write_bytes(encoded(dict(config,controller_authority_pending=True)))
            with patch.object(module,'CONFIG',config_path),patch.object(subprocess,'check_output',return_value=''):
                rejects(lambda:main('a0001'))
            sdk.Session.assert_not_called()
            config_path.write_bytes(encoded(config))
            for key,value in [('controller_authority_pending',True),('memory_bytes',MEMORY+1),
                ('versions',{'numpy':'2.3.2','pyarrow':'24.0.0'}),('quality_peek_allowed',True),
                ('reservoir_ordinals',[0,63]),('controller_code_sha256',{})]:
                config_path.write_bytes(encoded(dict(config,**{key:value})))
                rejects(lambda:qualify(repo))
            config_path.write_bytes(encoded(config))
            for name in (CODE[0],*helper['code_sha256'],*(p['path'] for p in helper['refs'].values())):
                path = repo/name
                original = path.read_bytes()
                path.write_bytes(original+b'\n')
                rejects(lambda:qualify(repo))
                path.write_bytes(original)
            for key,value in [('source_raw_sha256','0'*64),('schema','wrong'),('refs',draft['refs']),
                ('code_sha256',{}),('queries',63)]:
                body = encoded(dict(helper,**{key:value}))
                helper_path.write_bytes(body)
                config_path.write_bytes(encoded(dict(config,construction_config=dict(path='synthetic-helper.json',**artifact(helper_path)))))
                rejects(lambda:qualify(repo))
            helper_path.write_bytes(encoded(helper)); config_path.write_bytes(encoded(config))
            bad_panel = copy.deepcopy(panel)
            bad_panel['selected'][0]['reservoir_ordinal'] = 0
            panel_path.write_bytes(encoded(bad_panel))
            bad_helper = dict(helper,refs=dict(helper['refs'],panel=dict(path='synthetic-panel.json',**artifact(panel_path))))
            helper_path.write_bytes(encoded(bad_helper))
            config_path.write_bytes(encoded(dict(config,construction_config=dict(path='synthetic-helper.json',**artifact(helper_path)))))
            rejects(lambda:qualify(repo))
            panel_path.write_bytes(encoded(panel)); helper_path.write_bytes(encoded(helper)); config_path.write_bytes(encoded(config))
            rejects(lambda:repo_path(repo,'../escape'))
            source = dict(source_commit='0'*40,source_archive_sha256='1'*64)
            environment = dict(BORSUK_PANEL_CONFIG_SHA256=proof['config_sha256'],
                BORSUK_PANEL_SOURCE_COMMIT=source['source_commit'],BORSUK_PANEL_ARCHIVE_SHA256=source['source_archive_sha256'])
            versions = dict(python='3.12 synthetic',executable=sys.executable,numpy='2.3.3',
                pyarrow='24.0.0',machine='x86_64',os_release={'ID':'ubuntu','VERSION_ID':'24.04'},
                thread_environment=dict.fromkeys(THREAD_ENV,'2'))
            inputs = {'source.parquet':helper['source_parquet'],
                'source.raw':dict(bytes=3_072_000_000,sha256=helper['source_raw_sha256']),
                'consumed-queries.raw':helper['consumed_queries']}
            completed = None
            for failure in ('success','duplicate','body','replay','replay-body','interrupt','config-pin','scratch','oom'):
                out = tmp/failure
                out.mkdir()
                if failure == 'scratch':
                    (out/'screen').mkdir()
                calls = []
                def fake_artifact(path):
                    path = Path(path)
                    if path.parent == out/'screen' and path.name in inputs:
                        ident = {k:inputs[path.name][k] for k in ('bytes','sha256')}
                        return dict(ident,sha256='f'*64) if failure == 'body' else ident
                    return ids.artifact(path)
                def helper_run(command,**kw):
                    first,second = commands(repo,out,PREFIX+'a0001',proof)
                    assert command == (second if '--replay' in command else first)
                    assert kw['cwd'] == repo and kw['env']['PYTHONPATH'] == str(repo)
                    calls.append(command)
                    if '--replay' in command:
                        return subprocess.CompletedProcess(command,17 if failure == 'replay' else 0,
                            stdout='{}' if failure == 'replay-body' else json.dumps(dict(passed=True,remote_objects_reopened=False,ann_quality_measured=False)))
                    if failure == 'interrupt':
                        raise KeyboardInterrupt
                    if failure == 'duplicate':
                        kw['stdout'].write('new panel duplicates indexed FIRST1M; no replacement allowed\n')
                        return subprocess.CompletedProcess(command,17)
                    screen = out/'screen'
                    screen.mkdir()
                    for name in inputs:
                        (screen/name).write_bytes(b'synthetic input, not vectors/parquet')
                    for name,size in (('queries.raw',196608),('truth.u32',25600),('truth.i64',51200)):
                        (screen/name).write_bytes(b'\0'*size)
                    (screen/'requests.jsonl').write_text('synthetic requests; no decoded embeddings\n')
                    write(screen/'panel.json',encoded(panel))
                    write(screen/'duplicate-audit.json',dict(schema='borsuk-semantic-1m-vector-audit-v1',passed=True,
                        normalization=helper['normalization'],new_rows_audited=64,indexed_rows_audited=1_000_000,
                        consumed_rows_audited=1000,replacement_allowed=False,complete_historical_coverage=False,
                        config_sha256=proof['construction_config_sha256'],panel_sha256=helper['refs']['panel']['sha256'],
                        source_raw_sha256=helper['source_raw_sha256'],consumed_queries=helper['consumed_queries'],
                        authenticated_query_sources=query_sources(repo,helper),
                        raw_sha256=[f'{i:064x}' for i in range(64)],unit_sha256=[f'{i+64:064x}' for i in range(64)]))
                    write(screen/'oracle.json',dict(schema='borsuk-semantic-1m-oracle-v1',passed=True,
                        oracle_self_check=True,exhaustive_block_sort_top100_merge=True,rows=1_000_000,
                        queries=64,k=100,truth_id_space='source ordinal',source_raw_sha256=helper['source_raw_sha256'],
                        truth_u32=dict(path='truth.u32',**artifact(screen/'truth.u32')),
                        truth_i64=dict(path='truth.i64',**artifact(screen/'truth.i64'))))
                    write(screen/'resources.json',dict(ann_quality_measured=False,wall_seconds=1,process_max_rss_kib=1024,
                        cgroup_path=counters['cgroup'],cgroup={k:counters[k] for k in (
                            'memory.max','memory.peak','memory.swap.max','memory.swap.peak','memory.events')}))
                    decision = dict(schema='borsuk-semantic-1m-construction-v1',config_sha256=proof['construction_config_sha256'],
                        refs=helper['refs'],code_sha256=helper['code_sha256'],source_parquet=helper['source_parquet'],
                        source_raw_sha256=helper['source_raw_sha256'],consumed_queries=helper['consumed_queries'],
                        qualification=False,ann_quality_measured=False,complete_historical_coverage=False,
                        reservoir_ordinals=[1000,1063],query_ordinals=[0,63],queries=64,gt_k=100,truth_id_space='source ordinal',
                        prefix=PREFIX+'a0001',resource_pointer='resources.json',oracle_pointer='oracle.json',
                        duplicate_audit_pointer='duplicate-audit.json',seal_completion_pointer='seal-readback.json',
                        artifacts={n:dict(path=n,**artifact(screen/n)) for n in OUTPUTS})
                    write(screen/'decision.json',decision)
                    write(screen/'seal-readback.json',dict(schema='borsuk-semantic-1m-seal-readback-v1',config_identity_in_decision=True,
                        artifacts={n:dict(key=PREFIX+'a0001/sealed/'+n,**artifact(screen/n),etag='synthetic',authenticated_readback=True)
                                   for n in (*OUTPUTS,'decision.json')}))
                    kw['stdout'].write('synthetic helper completion\n')
                    return subprocess.CompletedProcess(command,0)
                captured = dict(counters,**{'memory.events':'max 0\noom 1\n'}) if failure == 'oom' else counters
                env = dict(environment,BORSUK_PANEL_CONFIG_SHA256='f'*64) if failure == 'config-pin' else environment
                with patch.object(ids,'capture_cgroup',return_value=captured),patch.object(module,'tool_versions',return_value=versions), \
                        patch.object(module,'artifact',side_effect=fake_artifact),patch.object(subprocess,'run',side_effect=helper_run),patch.dict(os.environ,env):
                    try:
                        stage(repo,out,PREFIX+'a0001')
                    except KeyboardInterrupt:
                        assert failure == 'interrupt'
                    except (AssertionError,FileExistsError):
                        assert failure != 'success'
                    else:
                        assert failure == 'success'
                report = json.loads((out/'remote-replay.json').read_bytes())
                assert report['replayed'] is (failure == 'success')
                assert len(calls) == (0 if failure in ('config-pin','scratch','oom') else 2 if failure in ('success','replay','replay-body') else 1)
                assert json.loads((out/'construction-cgroup.json').read_bytes())['after']['memory.peak'] == str(MEMORY+700416)
                if failure == 'duplicate':
                    assert 'no replacement allowed' in (out/'test.log').read_text()
                    assert report['prepare_exit_status'] == 17 and report['prepared'] is False
                if failure == 'success':
                    completed = out

            out = completed
            for name in ('test-resources.txt','run-closed.log'):
                (out/name).write_text('synthetic closed resources/log\n')
            nodes = {'0':{'instance_id':'i-original'},'1':{'instance_id':'i-extra'}}
            reservation = dict(schema=SCHEMA,qualification=proof,**source,wall_seconds=WALL,
                instance_type=INSTANCE_TYPE,image_id=IMAGE_ID,root_device_name=ROOT_DEVICE_NAME,
                subnet_id=SUBNET,spot_max_usd_per_hour=SPOT_MAX_USD_PER_HOUR,
                compute_cap_usd=COMPUTE_CAP,ebs_s3_allowance_usd=.15,total_cost_measured=False)
            launch = dict(instance_id='i-original',nodes=nodes,prefix=PREFIX+'a0001',**source)
            terminal = dict(schema=SCHEMA,instance_id='i-original',**source,
                **{k:proof[k] for k in TERMINAL_IDENTITIES},phase='complete',status='complete',exit_code=0,original_exit_code=0,
                artifacts={n:artifact(out/n) for n in ARTIFACTS},
                source_qualification_sha256=artifact(out/'source-qualification.json')['sha256'],
                remote_replay_sha256=artifact(out/'remote-replay.json')['sha256'])
            for name,value in [('aws-reservation.json',reservation),('aws-launch.json',launch),
                ('aws-closeout.json',dict(state='terminated',nodes=nodes)),('aws-terminal.json',terminal)]:
                (out/name).write_bytes(encoded(value))
            with patch.object(module,'__file__',str(repo/'scripts/launch_native_semantic_fresh_panel_spot.py')):
                assert replay(out)['constructed']
                for name in ARTIFACTS:
                    path = out/name
                    original = path.read_bytes()
                    path.write_bytes(original+b'tampered')
                    rejects(lambda:replay(out))
                    path.write_bytes(original)
                for key,value in [('config_sha256','f'*64),('source_commit','f'*40),('instance_id','i-unowned'),
                                  ('remote_replay_sha256','f'*64),('artifacts',{})]:
                    (out/'aws-terminal.json').write_bytes(encoded(dict(terminal,**{key:value})))
                    rejects(lambda:replay(out))
                (out/'aws-terminal.json').write_bytes(encoded(terminal))
                baseline = {n:(out/n).read_bytes() for n in ARTIFACTS}
                # Rehash the entire receipt chain, exercising semantic validation.
                for name,key,value in [('remote-replay.json','helper_replay',{}),
                    ('remote-replay.json','replayed',False),
                    ('screen/decision.json','qualification',True),
                    ('screen/duplicate-audit.json','passed',False),
                    ('screen/oracle.json','rows',999999),
                    ('screen/seal-readback.json','config_identity_in_decision',False),
                    ('input-hashes.json','inputs',{}),
                    ('construction-cgroup.json','closed',False),
                    ('tool-versions.json','thread_environment',{})]:
                    path = out/name
                    forged = json.loads(path.read_bytes())
                    forged[key] = value
                    path.write_bytes(encoded(forged))
                    decision_path = out/'screen/decision.json'
                    rebound = json.loads(decision_path.read_bytes())
                    rebound['artifacts'] = {n:dict(path=n,**artifact(out/'screen'/n)) for n in OUTPUTS}
                    decision_path.write_bytes(encoded(rebound))
                    seal_path = out/'screen/seal-readback.json'
                    rebound = json.loads(seal_path.read_bytes())
                    for n in (*OUTPUTS,'decision.json'):
                        rebound['artifacts'][n].update(artifact(out/'screen'/n))
                    seal_path.write_bytes(encoded(rebound))
                    report_path = out/'remote-replay.json'
                    rebound = json.loads(report_path.read_bytes())
                    rebound.update(artifacts={n:artifact(out/'screen'/n) for n in SCREEN},input_hashes=artifact(out/'input-hashes.json'))
                    report_path.write_bytes(encoded(rebound))
                    altered = copy.deepcopy(terminal)
                    altered['artifacts'] = {n:artifact(out/n) for n in ARTIFACTS}
                    altered['remote_replay_sha256'] = artifact(report_path)['sha256']
                    (out/'aws-terminal.json').write_bytes(encoded(altered))
                    rejects(lambda:replay(out))
                    for n,body in baseline.items():
                        (out/n).write_bytes(body)
                (out/'aws-terminal.json').write_bytes(encoded(terminal))
                (out/'aws-reservation.json').write_bytes(encoded(dict(reservation,memory_bytes=MEMORY+1,wall_seconds=WALL+1)))
                rejects(lambda:replay(out))
                (out/'aws-reservation.json').write_bytes(encoded(reservation))
                bodies = {n:(out/n).read_bytes() for n in ARTIFACTS}
                def get_object(**kw):
                    key = kw['Key']
                    assert not any(key.endswith('/'+n) for n in inputs), 'large input collected'
                    return {'Body':io.BytesIO(encoded(terminal) if key.endswith('/terminal.json') else bodies[key.split('/artifacts/',1)[1]])}
                s3 = Mock()
                s3.get_object.side_effect = get_object
                collect(s3,PREFIX+'a0001',out,'i-original',source['source_commit'],source['source_archive_sha256'])
                assert s3.get_object.call_count == len(ARTIFACTS)+1
                closed_body = (out/'aws-closeout.json').read_bytes()
                (out/'aws-closeout.json').write_bytes(encoded(dict(state='running',nodes=nodes)))
                s3.reset_mock()
                rejects(lambda:collect(s3,PREFIX+'a0001',out,'i-original',source['source_commit'],source['source_archive_sha256']))
                s3.get_object.assert_not_called()
                (out/'aws-closeout.json').write_bytes(closed_body)
                (out/'aws-closeout.json').write_bytes(encoded(dict(state='terminated',nodes={'0':nodes['0']})))
                rejects(lambda:collect(s3,PREFIX+'a0001',out,'i-original',source['source_commit'],source['source_archive_sha256']))
                s3.get_object.assert_not_called()
                (out/'aws-closeout.json').write_bytes(closed_body)
                s3.get_object.side_effect = lambda **kw:{'Body':io.BytesIO(encoded(terminal) if kw['Key'].endswith('/terminal.json') else b'tampered')}
                rejects(lambda:collect(s3,PREFIX+'a0001',out,'i-original',source['source_commit'],source['source_archive_sha256']))
                for code in (17,97):
                    failed = dict(terminal,phase='construction',status='failed',exit_code=code,original_exit_code=code,
                        artifacts={'run-closed.log':terminal['artifacts']['run-closed.log']},
                        remote_replay_sha256=None,source_qualification_sha256=None)
                    (out/'aws-terminal.json').write_bytes(encoded(failed))
                    assert replay(out)['constructed'] is False and replay(out)['exit_status'] == code

            body = user_data(source['source_commit'],source['source_archive_sha256'],'sources/synthetic.tar.gz',PREFIX+'a0001',proof)
            assert all(token in body for token in ('--on-active=9000s','RuntimeMaxSec=7260','MemoryMax=8G','MemorySwapMax=0',
                'CPUQuota=200%','TasksMax=512','--kill-after=30 7200','numpy==2.3.3','pyarrow==24.0.0','python3.12-venv',
                '--setenv=PYTHONPATH="$root/repo"','--setenv=OPENBLAS_NUM_THREADS=2','--setenv=OMP_NUM_THREADS=2',AWSCLI_SHA256))
            assert all(token not in body for token in ('rustup','cargo','--publish','unused','source.raw','source.parquet'))
            service = body[body.index('systemd-run --unit=semantic-fresh-panel '):body.index('for name in $ARTIFACT_NAMES; do',body.index('phase=construction'))]
            result = subprocess.run(['bash','-c','systemd-run() { printf "%s\\n" "$@"; }; root=/synthetic; '+service],
                cwd=tmp,text=True,capture_output=True,check=True)
            assert 'PYTHONPATH=/synthetic/repo' in result.stdout and '--stage\n/synthetic/repo\n/synthetic\n'+PREFIX+'a0001' in result.stdout
            portable = subprocess.run([sys.executable,'-c',
                'import runpy,sys; runpy.run_path(sys.argv[1]); assert not any(n in sys.modules for n in ("numpy","pyarrow","boto3","scripts.prepare_semantic_1m_fresh_panel"))',
                str(here/'scripts/launch_native_semantic_fresh_panel_spot.py')],cwd=tmp,capture_output=True,text=True,check=True)
            assert portable.stdout == portable.stderr == ''

        with contextlib.redirect_stdout(io.StringIO()):
            shared.self_check(lifecycle_only=True)
        for failure in ('success','fsync','interrupt'):
            with tempfile.TemporaryDirectory() as directory:
                ec2,s3,session = Mock(),Mock(),Mock()
                session.client.side_effect = [ec2,s3]
                ec2.describe_instances.return_value = {'Reservations':[]}
                ec2.describe_subnets.return_value = {'Subnets':[{'AvailabilityZone':'mock-az'}]}
                ec2.describe_spot_price_history.return_value = {'SpotPriceHistory':[
                    {'SpotPrice':'0.1','Timestamp':datetime.now(timezone.utc)}]}
                ec2.run_instances.return_value = {'Instances':[{'InstanceId':'i-original'},{'InstanceId':'i-extra'}]}
                events = []
                ec2.terminate_instances.side_effect = lambda **kw:events.append('terminate')
                ec2.get_waiter.return_value.wait.side_effect = lambda **kw:events.append('wait')
                def collected(*args):
                    assert events == ['terminate','wait']
                    events.append('collect')
                    return dict(status='complete',phase='complete',exit_code=0,artifacts={n:{} for n in ARTIFACTS})
                with patch.object(module,'ROOT',Path(directory)),patch.object(module,'preflight',return_value=proof), \
                    patch.object(module,'user_data',return_value='mock'),patch.object(module,'collect',side_effect=collected), \
                    patch.object(module,'poll',side_effect=KeyboardInterrupt() if failure == 'interrupt' else None), \
                    patch.object(shared.boto3,'Session',return_value=session),patch.object(subprocess,'check_output',side_effect=['','0'*40,b'archive']), \
                    patch.object(shared.peer,'missing',return_value=True),patch.object(shared.peer,'put_if_absent'), \
                    patch.object(shared.os,'fsync',side_effect=OSError('persist') if failure == 'fsync' else None),contextlib.redirect_stdout(io.StringIO()):
                    try:
                        main('a0001')
                    except (OSError,KeyboardInterrupt):
                        assert failure != 'success'
                    else:
                        assert failure == 'success'
                assert events == ['terminate','wait','collect']
                ec2.run_instances.assert_called_once()
                ec2.terminate_instances.assert_called_once_with(InstanceIds=['i-original','i-extra'])
                ec2.get_waiter.return_value.wait.assert_called_once_with(InstanceIds=['i-original','i-extra'])
                kwargs = ec2.run_instances.call_args.kwargs
                assert kwargs['InstanceType'] == INSTANCE_TYPE and kwargs['ImageId'] == IMAGE_ID
                assert kwargs['NetworkInterfaces'][0]['SubnetId'] == SUBNET
                assert kwargs['BlockDeviceMappings'] == [{'DeviceName':'/dev/sda1','Ebs':{
                    'DeleteOnTermination':True,'Encrypted':True,'VolumeSize':80,'VolumeType':'gp3'}}]
                assert kwargs['InstanceMarketOptions']['SpotOptions']['MaxPrice'] == '0.50'
                launch_path = Path(directory)/'a0001/aws-launch.json'
                assert json.loads(launch_path.read_bytes())['nodes'] == nodes
                reserved = json.loads((launch_path.parent/'aws-reservation.json').read_bytes())
                assert reserved['wall_seconds'] == WALL and reserved['compute_cap_usd'] == COMPUTE_CAP and reserved['ebs_s3_allowance_usd'] == .15
        rejects(lambda:main('a０００１'))
    assert not any(n in sys.modules for n in ('numpy','pyarrow',HELPER_MODULE)), 'heavy imports forbidden'
    elapsed = time.monotonic()-started
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    assert elapsed < 55 and peak < 200*1024
    print(f'PASS stdlib/mock authority/body/remote-replay/resources/tamper/failure/shell/ACK cleanup checks; '
          f'{len(CODE)} controller pins, 20 helper pins, 7 refs, {len(ARTIFACTS)} artifacts; '
          f'user-data {len(body.encode())} bytes; {elapsed:.2f}s/{peak}KiB; no scientific claim')


if __name__ == '__main__':
    args = sys.argv[1:]
    if args == ['--self-check']:
        self_check()
    elif len(args) == 4 and args[0] == '--stage':
        stage(Path(args[1]),Path(args[2]),args[3])
    elif len(args) == 2 and args[0] == '--replay':
        print(json.dumps(replay(Path(args[1])),sort_keys=True))
    else:
        assert len(args) == 1, __doc__
        with open('/tmp/borsuk-semantic-fresh-panel-launch.lock','a+') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX | fcntl.LOCK_NB)
            main(args[0])
