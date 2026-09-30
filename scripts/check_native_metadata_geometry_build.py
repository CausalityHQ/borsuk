"""Build and qualify both authenticated native geometry arms on one ARM worker."""
import json
from pathlib import Path
import subprocess
import sys

from scripts.check_native_startup_build import (RUNTIME, SOURCE_TESTS, sha,
    source_hashes, source_identity, feature_checks, capture_cgroup)

CHECKS = (('object-native', ['--lib', 'object_native_generation::tests']),
          ('generation', ['--test', 'two_bit_generation']),
          ('http', ['--example', 'two_bit_http']),
          ('source', ['--test', 'two_bit_source']),
          ('graph', ['--lib', 'unit_centroid_graph::tests']))


def main(cargo, repo, out):
    from scripts import launch_native_metadata_geometry_spot as controller
    repo, out = Path(repo).resolve(), Path(out).resolve()
    qualification = json.loads((out / 'source-qualification.json').read_text())
    assert controller.preflight(repo) == qualification
    identities = source_hashes(repo)
    stage = repo / controller.STAGE
    candidate_body = stage.read_bytes()
    control_body = (repo / qualification['control_stage']['path']).read_bytes()
    assert sha(control_body) == qualification['control_stage']['sha256']
    control = dict(identities, **{controller.STAGE: sha(control_body)})
    target_dir = out / 'target'
    args = ['--release', '--locked', '--manifest-path', str(repo / 'Cargo.toml'),
            '--target-dir', str(target_dir), '-p', 'borsuk', '--jobs', '4']
    try:
        for arm, hashes, body, destination in (
            ('control', control, control_body, out / 'control'),
            ('candidate', identities, candidate_body, out)):
            destination.mkdir(parents=True, exist_ok=True)
            stage.write_bytes(body)
            assert source_hashes(repo) == hashes
            expected_identity = qualification['control_source_identity_sha256' if arm == 'control'
                                                else 'source_identity_sha256']
            assert source_identity(hashes) == expected_identity and len(hashes) == 395
            if arm == 'candidate':
                # A restored source mtime cannot qualify an old control output.
                with (destination / 'release.log').open('x') as log:
                    subprocess.run([cargo, 'clean', '-p', 'borsuk', '--manifest-path',
                        str(repo / 'Cargo.toml'), '--target-dir', str(target_dir)],
                        stdout=log, stderr=subprocess.STDOUT, check=True)
            features = feature_checks(cargo, repo, destination)
            if arm == 'control':
                control_features = features
            else:
                assert features == control_features
                for name in controller.TOOLCHAIN:
                    assert (out / name).read_bytes() == (out / 'control' / name).read_bytes(), name
            features = dict(features, toolchain_parity_asserted=True)
            original = {name: (repo / name).read_bytes() for name in controller.COMPILED}
            compiled = {name: sha(body) for name, body in original.items()}
            assert compiled == qualification['control_compiled_native_sha256' if arm == 'control'
                                              else 'compiled_native_sha256']
            # Recheck all native sources immediately before invoking Cargo.
            assert source_hashes(repo) == hashes
            for name, check_args in CHECKS:
                with (destination / (name + '.log')).open('x') as log:
                    subprocess.run([cargo, 'test', *args, *check_args], stdout=log,
                                   stderr=subprocess.STDOUT, check=True)
                text = (destination / (name + '.log')).read_text()
                assert '0 failed;' in text and 'test result: ok. 0 passed;' not in text
                if name == 'source':
                    assert all('test ' + test + ' ... ok' in text for test in SOURCE_TESTS)
                if name == 'graph':
                    assert 'test result: ok. 7 passed; 0 failed;' in text
            with (destination / 'release.log').open('a' if arm == 'candidate' else 'x') as log:
                subprocess.run([cargo, 'build', *args, '--example', 'two_bit_http'],
                               stdout=log, stderr=subprocess.STDOUT, check=True)
            assert source_hashes(repo) == hashes
            assert all((repo / name).read_bytes() == body for name, body in original.items())
            (destination / 'compiled-source.json').write_text(json.dumps(compiled, indent=2) + '\n')
            for name, body in original.items():
                snapshot = destination / 'compiled-source' / name
                snapshot.parent.mkdir(parents=True, exist_ok=True)
                snapshot.write_bytes(body)
            binary = (target_dir / 'release/examples/two_bit_http').read_bytes()
            assert binary, 'empty release binary'
            binary_path = destination / 'binaries/two_bit_http'
            binary_path.parent.mkdir(parents=True, exist_ok=True)
            binary_path.write_bytes(binary)
            binary_path.chmod(0o755)
            report = dict(qualified=True, no_corpus_query=True, full_workspace_repeated=False,
                green_status=0, release_status=0, focused_tests=[name for name, _ in CHECKS],
                compiled_native_sha256=compiled, compiled_http_sha256=compiled[RUNTIME[0]],
                binary_sha256=sha(binary), binary_bytes=len(binary), sha_backend=features,
                source_identity_sha256=expected_identity, source_file_count=len(hashes),
                native_rebuilt=True, same_worker_toolchain=True, arm=arm,
                current_full_suite_pass_claim=False)
            (destination / 'boundary-check.json').write_text(json.dumps(report, indent=2) + '\n')
        capture_cgroup(out / 'boundary-cgroup.json')
    finally:
        stage.write_bytes(candidate_body)
        assert source_hashes(repo) == identities, 'candidate native epoch not restored'
    print(json.dumps(dict(qualified=True, native_rebuilt=True, control_native_rebuilt=True,
                         current_full_suite_pass_claim=False)))


if __name__ == '__main__':
    assert len(sys.argv) == 4, 'usage: python3 -m scripts.check_native_metadata_geometry_build CARGO REPO OUTPUT'
    main(*sys.argv[1:])
