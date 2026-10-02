import json
from pathlib import Path
import resource
import shutil
import signal
import sys
import tempfile
import time

repo = Path(sys.argv[1]).absolute()
sys.path.insert(0, str(repo))
from scripts import launch_cohere_fixed48_fresh_scientific_spot as h
resource.setrlimit(resource.RLIMIT_AS, (256 << 20, 256 << 20))
signal.alarm(55)
started = time.monotonic()
original = repo / h.ROOT / 'a0002'
before = {str(p.relative_to(original)): h.artifact(p) for p in original.rglob('*') if p.is_file()}
checks = []

def rejected(label, call, fragment=None):
    try:
        call()
    except (AssertionError, ValueError, OSError) as error:
        if fragment is not None:
            assert fragment in str(error), (label, error)
        checks.append(dict(name=label, rejected=True, error_type=type(error).__name__, error=str(error)))
    else:
        raise AssertionError('admitted: ' + label)

# The normal validator still refuses this genuine seven-variable tool report.
proof = h.read_json(original / 'source-qualification.json')
terminal = h.read_json(original / 'aws-terminal.json')
rejected('default-strict-original-seven', lambda: h.validate_closed(original, proof, terminal, terminal['artifacts']), 'exact collector thread environment')
result = h.validate_historical_a0002(repo)
assert result['scientific_status'] == 'GO' and result['original_collection_execution_status'] == 'FAIL'
assert result['original_controller_exit'] == 2 and result['authenticated_terminal_bodies'] == 70

with tempfile.TemporaryDirectory(prefix='fixed48-historical-check-') as work:
    clone_repo = Path(work)
    out = clone_repo / h.ROOT / 'a0002'
    shutil.copytree(original, out)
    assert h.validate_historical_a0002(clone_repo) == result
    for name in ('screen/resources.json', 'screen/measurement-resources.json', 'tool-versions.json'):
        path = out / name
        raw = path.read_bytes()
        for mode in ('missing-BLIS', 'bad-BLIS', 'extra-env'):
            value = json.loads(raw)
            env = value['thread_environment']
            if mode == 'missing-BLIS': env.pop('BLIS_NUM_THREADS', None)
            if mode == 'bad-BLIS': env['BLIS_NUM_THREADS'] = '1'
            if mode == 'extra-env': env['EXTRA_NUM_THREADS'] = '2'
            if name == 'tool-versions.json' and mode == 'missing-BLIS':
                env.pop('OMP_NUM_THREADS')  # BLIS was already omitted historically.
            h.write(path, value)
            rejected(name + '/' + mode, lambda: h.validate_historical_a0002(clone_repo), 'historical local body identity')
            path.write_bytes(raw)
        path.unlink()
        rejected(name + '/missing-real-report', lambda: h.validate_historical_a0002(clone_repo))
        path.write_bytes(raw)
    for name, field, replacement in (
        ('aws-terminal.json', 'source_commit', '0' * 40),
        ('source-qualification.json', 'source_archive_sha256', '0' * 64),
        ('aws-closeout.json', 'state', 'running'),
        ('config.json', 'execution_source', dict(commit='0' * 40, archive_sha256='0' * 64)),
        ('driver-config.json', 'execution_source', dict(commit='0' * 40, archive_sha256='0' * 64)),
        ('collection-error.json', 'execution_status', 'SUCCESS')):
        path = out / name
        raw = path.read_bytes()
        h.write(path, dict(json.loads(raw), **{field: replacement}))
        rejected(name + '/authority-tamper', lambda: h.validate_historical_a0002(clone_repo))
        path.write_bytes(raw)
    for name in ('collection-error.json', 'collection-progress.json'):
        path = out / name
        raw = path.read_bytes()
        value = json.loads(raw)
        roster = value.get('authenticated_files', value.get('files'))
        roster['screen/resources.json']['sha256'] = '0' * 64
        h.write(path, value)
        rejected(name + '/bad-body-hash', lambda: h.validate_historical_a0002(clone_repo))
        path.write_bytes(raw)
        value = json.loads(raw)
        roster = value.get('authenticated_files', value.get('files'))
        roster['screen/resources.json']['full_body_stream_verified'] = False
        h.write(path, value)
        rejected(name + '/unauthenticated-original-body', lambda: h.validate_historical_a0002(clone_repo))
        path.write_bytes(raw)
    # Caller dictionaries cannot substitute a different authority or body roster.
    changed = dict(terminal, source_archive_sha256='0' * 64)
    rejected('caller-terminal-tamper', lambda: h.validate_closed(out, proof, changed, terminal['artifacts'], historical_a0002=True))
    changed = dict(proof, config_sha256='0' * 64)
    rejected('caller-proof-tamper', lambda: h.validate_closed(out, changed, terminal, terminal['artifacts'], historical_a0002=True))
    changed = dict(terminal['artifacts'])
    changed['screen/resources.json'] = dict(bytes=1, sha256='0' * 64)
    rejected('caller-body-pin-tamper', lambda: h.validate_closed(out, proof, terminal, changed, historical_a0002=True))
    assert h.validate_historical_a0002(clone_repo) == result

assert before == {str(p.relative_to(original)): h.artifact(p) for p in original.rglob('*') if p.is_file()}
signal.alarm(0)
print(json.dumps(dict(passed=True, historical=result, original_receipts_unchanged=True,
    negatives=checks, negative_checks=len(checks), elapsed_seconds=time.monotonic()-started,
    peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
    large_bodies_hydrated=False, science_or_cloud_executed=False), sort_keys=True))
