"""Qualify exactly one reviewed staging delta on ARM; preserve the frozen control."""
import gzip
import json
import re
from pathlib import Path
import subprocess
import sys

from scripts.check_native_startup_build import (FOCUSED_ARM, RUNTIME, SOURCE_TESTS,
    sha, source_hashes, source_identity, feature_checks, capture_cgroup)


def main(cargo, repo, out):
    repo, out = Path(repo).resolve(), Path(out).resolve()
    from scripts import launch_native_metadata_ranges_cold_spot as controller
    original = {name: (repo / name).read_bytes() for name in FOCUSED_ARM}
    qualification = json.loads((out / 'source-qualification.json').read_text())
    assert controller.preflight(repo) == qualification
    identities = source_hashes(repo)
    for name in controller.FROZEN_FILES:
        body = gzip.decompress((repo / controller.FROZEN / (name + '.gz')).read_bytes())
        ident = qualification['frozen_binary_artifacts'][name]
        assert len(body) == ident['bytes'] and sha(body) == ident['sha256'], name
        target = out / 'control' / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    (out / 'control/binaries/two_bit_http').chmod(0o755)
    features = feature_checks(cargo, repo, out)
    for name in ('rustc-version.txt', 'cargo-version.txt'):
        assert (out / name).read_bytes() == (out / 'control' / name).read_bytes(), name
    # Same SHA backend selection; cargo-tree root label may include the new checkout path.
    for name in ('arm-feature-tree.txt', 'x86-feature-tree.txt'):
        def lines(body):
            return [re.sub(r'\(/[^)]*/repo/', '(__repo__/', line).removesuffix(' (*)')
                    for line in body.splitlines()]
        assert lines((out / name).read_text()) == lines((out / 'control' / name).read_text()), name
    args = ['--release', '--locked', '--manifest-path', str(repo / 'Cargo.toml'),
            '--target-dir', str(out / 'target'), '-p', 'borsuk', '--jobs', '4']
    checks = [('object-native', ['--lib', 'object_native_generation::tests']),
              ('generation', ['--test', 'two_bit_generation']),
              ('http', ['--example', 'two_bit_http'])]
    checks.append(('source', ['--test', 'two_bit_source']))
    for name, target in checks:
        with (out / (name + '.log')).open('x') as log:
            subprocess.run([cargo, 'test', *args, *target], stdout=log,
                           stderr=subprocess.STDOUT, check=True)
        text = (out / (name + '.log')).read_text()
        assert '0 failed;' in text and 'test result: ok. 0 passed;' not in text
        if name == 'source':
            assert all('test ' + test + ' ... ok' in text for test in SOURCE_TESTS)
    with (out / 'release.log').open('x') as log:
        subprocess.run([cargo, 'build', *args, '--example', 'two_bit_http'],
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    assert all((repo / name).read_bytes() == body for name, body in original.items())
    assert source_hashes(repo) == identities
    (out / 'compiled-source.json').write_text(json.dumps(
        {name: sha(body) for name, body in original.items()}, indent=2) + '\n')
    for name, body in original.items():
        target = out / 'compiled-source' / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    binary = (out / 'target/release/examples/two_bit_http').read_bytes()
    (out / 'binaries').mkdir(exist_ok=True)
    target = out / 'binaries/two_bit_http'
    target.write_bytes(binary)
    target.chmod(0o755)
    capture_cgroup(out / 'boundary-cgroup.json')
    report = dict(qualified=True, no_corpus_query=True, full_workspace_repeated=False,
        green_status=0, release_status=0, focused_tests=[name for name, _ in checks],
        compiled_native_sha256={name: sha(body) for name, body in original.items()},
        compiled_http_sha256=sha(original[RUNTIME[0]]), binary_sha256=sha(binary),
        prior_unaffected_assurance_tests=2696, current_full_suite_pass_claim=False)
    report.update(sha_backend=features, source_identity_sha256=source_identity(identities),
        source_file_count=len(identities), reviewed_native_delta_sha256=qualification['reviewed_native_delta_sha256'],
        historical_assurance_scope=qualification['historical_assurance_scope'],
        control_source_commit=controller.NATIVE_COMMIT,
        control_source_identity_sha256=controller.CONTROL_IDENTITY,
        control_binary_sha256=controller.BINARY_SHA, native_rebuilt=True,
        frozen_native_qualification=qualification['frozen_native_qualification'])
    (out / 'boundary-check.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    assert len(sys.argv) == 4, 'usage: python3 -m scripts.check_native_metadata_ranges_build CARGO REPO OUTPUT'
    main(*sys.argv[1:])
