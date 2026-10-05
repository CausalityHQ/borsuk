import hashlib, importlib.util, pathlib, subprocess, sys
repo=pathlib.Path('/home/rb/worktrees/borsuk-source-witness-science-a0001-frozen')
assert pathlib.Path.cwd()==repo
assert subprocess.check_output(['git','status','--porcelain'],text=True)==''
path=repo/'docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/source-witness-router/campaign.py'
assert hashlib.sha256(path.read_bytes()).hexdigest()=='cf157eb2a10ada52ac4648c7c90fea8c19aed85ec27b80c4016abac06e27f58e'
spec=importlib.util.spec_from_file_location('source_witness_science_campaign',path)
module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
campaign=module.campaign(is_canary=False)
# The existing lifecycle accepts an absolute local output ROOT. Remote authority unchanged.
campaign.ROOT=(repo/campaign.ROOT).resolve()
shared,_=module.ids.lifecycle()
shared.main('a0001',campaign=campaign)
