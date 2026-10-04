import hashlib,json,pathlib,time
from scripts import run_source_witness_paired_coverage as runner
base=pathlib.Path('/home/rb/worktrees/borsuk-prod-ready-v9')
path=base/'docs/research/performance-architecture-20260930/semantic-1m/hierarchical-cells/source-witness-router/paired-coverage-config.draft.json'
started=time.monotonic()
config=json.loads(path.read_bytes())
authority,sources,pins=runner.qualify(config,base)
print(json.dumps({'schema':'borsuk-source-witness-production-metadata-admission-v1','exit_status':0,'config':{'path':str(path.relative_to(base)),'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()},'native_source_file_count':len(sources),'native_source_identity_sha256':runner.source_identity(sources),'authority_refs_authenticated':len(pins),'qualified_binary_descriptor_bound':True,'retained_dataset_descriptors':sum(len(x) for x in config['inputs'].values()),'datasets':['ReLAION','CoHere'],'actual_asset_bodies_opened':False,'query_or_ground_truth_read':False,'native_execution':False,'wall_seconds':time.monotonic()-started},sort_keys=True))
