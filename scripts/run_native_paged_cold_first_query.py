"""Paged-source accounting around the unchanged serial cold-first-query protocol."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import sys
import tempfile

from scripts import run_native_cold_first_query as old
from scripts.check_native_paged_source_stats import validate_response, validate_startup

CODE = (*old.CODE, 'scripts/check_native_paged_source_stats.py',
        'scripts/check_native_metadata_ranges_stats.py',
        'scripts/run_native_paged_cold_first_query.py')
SCHEMA = 'borsuk-native-paged-cold-first-query-v1'
SOURCE_CAPS = dict(source_submitted_gets=128, source_verified_bytes=67108864,
    source_failed_gets=0, sq8_submitted_gets=32, sq8_verified_bytes=16773120,
    sq8_failed_gets=0, combined_submitted_gets=160, combined_verified_bytes=83881984,
    combined_failed_gets=0)
_checked_response = old.checked_response
_cold_call = old.cold_call
_reduce_panel = old.reduce_panel


def checked_response(response, expected, truth, authority):
    hits = _checked_response(response, expected, truth, authority)
    validate_response(response)
    return hits


def cold_call(binary, config, item, body, expected, truth, failure_stream=None, *, port=8080):
    record = _cold_call(binary, config, item, body, expected, truth, failure_stream, port=port)
    # Retain the unchanged reference/GT fields for the final per-record reduction.
    record.update(reference_response={key: expected[key] for key in old.FIELDS},
        truth_at_10=list(truth[:10]), expected_authority=item['authority'],
        metadata_files=item['metadata_files'])
    return record


def reduce_panel(records):
    previous = 0
    for record in records:
        header = record['native_header']
        assert header['phase'] == 'ready' and header['authority'] == record['expected_authority']
        assert record['native_close']['intentional_stop'] is True
        assert record['returned_hits'] == checked_response(record['response'],
            record['reference_response'], record['truth_at_10'], record['expected_authority'])
        metadata = validate_startup(header['remote_open_stats'], record['metadata_files'],
                                    header['remote_open_wall_ns'])
        assert record['metadata'] == metadata
        assert type(header['head_read_wall_ns']) is int and header['head_read_wall_ns'] >= 0
        assert previous <= record['started_ns'] <= record['completed_ns']
        assert record['cold_start_to_first_http_response_ns'] == record['completed_ns'] - record['started_ns']
        assert record['cold_start_to_first_http_response_ns'] >= header['remote_open_wall_ns'] + header['head_read_wall_ns']
        previous = record['completed_ns']
    result = _reduce_panel(records)  # All 64 ordinals, one successful request each; unchanged gates.
    for suffix in ('submitted_gets', 'verified_bytes', 'failed_gets'):
        source = sum(record['response']['source_' + suffix] for record in records)
        sq8 = sum(record['response'][suffix] for record in records)
        result.update({'source_' + suffix: source, 'sq8_' + suffix: sq8,
                       'combined_' + suffix: source + sq8})
    result.update(source_head_requests=sum(record['metadata']['source_head_requests'] for record in records),
        logical_metadata_head_requests=sum(record['metadata']['logical_metadata_head_requests'] for record in records),
        logical_metadata_get_requests=sum(record['metadata']['logical_metadata_get_requests'] for record in records),
        metadata_payload_buffer_bound_bytes=max(record['metadata']['payload_buffer_bound_bytes'] for record in records),
        source_head_wall_ns=sum(record['native_header']['remote_open_stats']['source_head_wall_ns'] for record in records),
        source_head_ms=sum(record['metadata']['source_head_ms'] for record in records),
        head_read_wall_ns=sum(record['native_header']['head_read_wall_ns'] for record in records))
    return result


def validate_config(config):
    assert config['schema'] == SCHEMA
    assert (config['count'], config['k']) == (64, 10)
    assert config['dataset_order'] == ['ReLAION', 'CoHere']
    assert set(config['code_sha256']) == set(CODE) and len(CODE) == 10
    for name, digest in config['code_sha256'].items():
        assert old.sha(name) == digest, name
    assert type(config['native_source_file_count']) is int and config['native_source_file_count'] == 395
    assert [item['dataset'] for item in config['items']] == config['dataset_order']
    for item in config['items']:
        assert len(item['metadata_files']) == 9 and 'plane/records.bin' not in item['metadata_files']


def validate_runtime(config, binary, qualification_path):
    qualified = json.loads(Path(qualification_path).read_text())
    assert qualified['qualified'] is True and qualified['green_status'] == qualified['release_status'] == 0
    assert qualified['current_full_suite_pass_claim'] is False
    assert qualified['binary_sha256'] == old.sha(binary) == config['binary']['sha256']
    assert Path(binary).stat().st_size == config['binary']['bytes']
    for name, digest in qualified['compiled_native_sha256'].items():
        assert old.sha(name) == digest, name
    repo = Path('.')
    paths = set(repo.rglob('*.rs')) | set(repo.rglob('Cargo.toml')) | set(repo.rglob('Cargo.lock'))
    identities = {str(path): old.sha(path) for path in sorted(paths)
                  if not {'.git', 'target'}.intersection(path.parts)}
    identity = hashlib.sha256(json.dumps(identities, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    assert len(identities) == qualified['source_file_count'] == config['native_source_file_count'] == 395
    assert identity == qualified['source_identity_sha256'] == config['native_source_identity_sha256']


@contextmanager
def scoped_runner(argv):
    replacements = dict(CODE=CODE, checked_response=checked_response, validate=validate_startup,
                        cold_call=cold_call, reduce_panel=reduce_panel)
    saved = {name: getattr(old, name) for name in replacements}
    saved_argv = sys.argv
    try:
        for name, value in replacements.items():
            setattr(old, name, value)
        sys.argv = argv
        yield
    finally:
        sys.argv = saved_argv
        for name, value in saved.items():
            setattr(old, name, value)


def main():
    config_path, digest, binary, qualification_path, output = sys.argv[1:]
    assert old.sha(config_path) == digest
    config = json.loads(Path(config_path).read_text())
    validate_config(config)
    validate_runtime(config, binary, qualification_path)
    with tempfile.TemporaryDirectory() as tmp:
        adapter = Path(tmp) / 'internal-config.json'
        adapter.write_text(json.dumps(dict(config, schema='borsuk-native-cold-first-query-v1')))
        with scoped_runner([sys.argv[0], str(adapter), old.sha(adapter), binary, qualification_path, output]):
            old.main()
    summary_path = Path(output) / 'summary.json'
    summary = json.loads(summary_path.read_text())
    summary.update(schema='borsuk-native-paged-cold-first-query-result-v1',
        original_config_sha256=digest, source_caps=SOURCE_CAPS,
        native_source_identity_sha256=config['native_source_identity_sha256'],
        native_source_file_count=config['native_source_file_count'],
        closed_resident_reference_quality_only=True, matched_control_latency_measured=False,
        matched_vendor_measured=False)
    summary_path.write_text(json.dumps(summary, indent=2, allow_nan=False) + '\n')


if __name__ == '__main__':
    assert len(sys.argv) == 6
    main()
