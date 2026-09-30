"""Qualify one fresh ARM paged-source candidate, without a historical control."""
import json
from pathlib import Path
import subprocess
import sys

from scripts.check_native_startup_build import (RUNTIME, SOURCE_TESTS, sha,
    source_hashes, feature_checks, capture_cgroup)

CHECKS = (
    ('source', ['--test', 'two_bit_source']),
    ('source-walk', ['--lib', 'two_bit_generation::source_walk_tests']),
    ('sq8-transport', ['--lib', 'sq8_s3_range::tests']),
    ('object-native', ['--lib', 'object_native_generation::tests']),
    ('graph', ['--lib', 'unit_centroid_graph::tests']),
    ('generation', ['--test', 'two_bit_generation']),
    ('application-ids', ['--test', 'two_bit_application_ids']),
    ('gc', ['--test', 'two_bit_gc_delayed_delete']),
    ('http', ['--example', 'two_bit_http']),
)
SOURCE_WALK_TESTS = (
    'paged_source_matches_reference_and_preserves_failure_charges',
    'fragmented_paged_source_preserves_trace_and_rank_across_get_caps',
)


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def main(cargo, repo, out):
    from scripts import launch_native_paged_source_cold_spot as controller
    repo, out = Path(repo).resolve(), Path(out).resolve()
    qualification = json.loads((out / 'source-qualification.json').read_bytes())
    assert controller.preflight(repo) == qualification
    # A fresh output directory is essential: never qualify a cached binary.
    target_dir = out / 'target'
    assert not target_dir.exists(), 'fresh build target already exists'
    identities = source_hashes(repo)
    original = {name: (repo / name).read_bytes() for name in controller.COMPILED}
    compiled = {name: sha(body) for name, body in original.items()}
    assert compiled == qualification['compiled_native_sha256']
    features = feature_checks(cargo, repo, out)
    args = ['--release', '--locked', '--manifest-path', str(repo / 'Cargo.toml'),
            '--target-dir', str(target_dir), '-p', 'borsuk', '--jobs', '4']
    assert controller.preflight(repo) == qualification
    for name, target in CHECKS:
        with (out / (name + '.log')).open('x') as log:
            subprocess.run([cargo, 'test', *args, *target], stdout=log,
                           stderr=subprocess.STDOUT, check=True)
        text = (out / (name + '.log')).read_text()
        assert '0 failed;' in text and 'test result: ok. 0 passed;' not in text, name
        required = SOURCE_TESTS if name == 'source' else SOURCE_WALK_TESTS if name == 'source-walk' else ()
        assert all(test + ' ... ok' in text for test in required), name
    with (out / 'release.log').open('x') as log:
        subprocess.run([cargo, 'build', *args, '--example', 'two_bit_http'],
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    # Reauthenticate the whole manifest, config and Python roster after Cargo.
    assert controller.preflight(repo) == qualification
    assert source_hashes(repo) == identities
    assert all((repo / name).read_bytes() == body for name, body in original.items())
    write_json(out / 'compiled-source.json', compiled)
    for name, body in original.items():
        snapshot = out / 'compiled-source' / name
        snapshot.parent.mkdir(parents=True, exist_ok=True)
        snapshot.write_bytes(body)
    binary = (target_dir / 'release/examples/two_bit_http').read_bytes()
    assert binary, 'empty release binary'
    binary_path = out / 'binaries/two_bit_http'
    binary_path.parent.mkdir(exist_ok=True)
    binary_path.write_bytes(binary)
    binary_path.chmod(0o755)
    config_body = (repo / controller.CONFIG).read_bytes()
    assert sha(config_body) == qualification['config_sha256']
    config = json.loads(config_body)
    assert config['binary'] is None
    resolved = dict(config, binary=dict(sha256=sha(binary), bytes=len(binary)))
    resolved_path = out / 'resolved-config.json'
    write_json(resolved_path, resolved)
    resolution = dict(original_config_sha256=sha(config_body),
        resolved_config_sha256=sha(resolved_path.read_bytes()),
        resolved_config_path='resolved-config.json', binary_sha256=sha(binary),
        binary_bytes=len(binary))
    capture_cgroup(out / 'boundary-cgroup.json')
    report = dict(qualified=True, no_corpus_query=True, full_workspace_repeated=False,
        green_status=0, release_status=0, focused_tests=[name for name, _ in CHECKS],
        compiled_native_sha256=compiled, compiled_http_sha256=compiled[RUNTIME[0]],
        sha_backend=features, source_identity_sha256=qualification['source_identity_sha256'],
        source_file_count=len(identities), native_source_commit=controller.NATIVE_COMMIT,
        manifest_sha256=qualification['manifest_sha256'], native_rebuilt=True,
        arm='candidate', matched_control_latency_measured=False,
        current_full_suite_pass_claim=False, config_sha256=sha(config_body), **resolution)
    write_json(out / 'boundary-check.json', report)
    write_json(out / 'source-qualification.json', dict(qualification, **resolution))
    print(json.dumps(report))


if __name__ == '__main__':
    assert len(sys.argv) == 4, 'usage: python3 -m scripts.check_native_paged_source_build CARGO REPO OUTPUT'
    main(*sys.argv[1:])
