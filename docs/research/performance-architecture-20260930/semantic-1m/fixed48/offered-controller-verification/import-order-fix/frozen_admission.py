import json
from pathlib import Path
import tempfile
from unittest.mock import patch
from scripts import launch_cohere_fixed48_offered_http_spot as ctl
repo = Path.cwd()
paths = [repo/p for p in (ctl.CONFIG,ctl.CONTROL,ctl.MANIFEST)]
before = {str(p.relative_to(repo)):ctl.artifact(p) for p in paths}
try:
    ctl.qualify(repo)
except AssertionError as error:
    assert str(error) == 'controller source drift', str(error)
    print('Unmodified frozen controller correctly rejected:',error)
else:
    raise AssertionError('old frozen owned-file hash was unexpectedly admitted')
control = ctl.read_json(repo/ctl.CONTROL)
original = dict(control['code_sha256'])
control['code_sha256'][ctl.OWN] = ctl.artifact(repo/ctl.OWN)['sha256']
assert [n for n in original if original[n] != control['code_sha256'][n]] == [ctl.OWN]
with tempfile.TemporaryDirectory(prefix='offered-frozen-owned-hash-fixture-') as tmp:
    refreshed = Path(tmp)/'controller-config.json'
    refreshed.write_bytes(ctl.encoded(control))
    with patch.object(ctl,'CONTROL',refreshed),patch.object(ctl.cold,'CODE',ctl.CODE):
        proof = ctl.qualify(repo)
assert before == {str(p.relative_to(repo)):ctl.artifact(p) for p in paths}
assert proof['source_archive_commit'] == '3cb4b1a58e6b22f72b9de40af7d19c9e374c1d53'
assert proof['native_source_file_count'] == 399 and len(ctl.runtime().CODE) == 81
print(json.dumps(dict(passed=True,fixture_only=True,frozen_authorities_unchanged=before,refreshed_hash_only=ctl.OWN,runtime_code_count=81,controller_code_count=83,source_archive_commit=proof['source_archive_commit'],native_source_file_count=399,binary_sha256=proof['binary_sha256'],fresh_archive_preflight_validated=False,native_or_network_execution=False),sort_keys=True))
