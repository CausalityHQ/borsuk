#!/usr/bin/env bash
set -euo pipefail
cd "${1:?owned job directory required}"
exec 9>prepare.lock
flock -n 9 || exit 75
[ ! -f terminal.json ] || exit 76
phase=bootstrap
finish() {
  code=$?
  trap - EXIT TERM
  EXIT_CODE="$code" PHASE="$phase" python3 - <<'PY' >terminal.json.pending
import hashlib,json,os,time
from pathlib import Path
names=['source.json','vectors.raw','source/source.parquet','source/provenance.json',
       'source/original_ids.u64','layout/layout.npy','layout/sq8.bin','layout/manifest.json',
       'calibration.json','source.time','layout.time','run.log']
artifacts={}
for name in names:
    p=Path(name)
    if p.is_file():
        digest=hashlib.sha256()
        with p.open('rb') as f:
            for block in iter(lambda:f.read(4*1024*1024),b''):digest.update(block)
        artifacts[name]=dict(bytes=p.stat().st_size,sha256=digest.hexdigest())
print(json.dumps(dict(schema='borsuk-native-relaion-preparation-terminal-v1',
    exit_code=int(os.environ['EXIT_CODE']),phase=os.environ['PHASE'],finished_epoch=int(time.time()),
    source_archive_sha256=os.environ['BORSUK_SOURCE_ARCHIVE_SHA'],artifacts=artifacts,
    query_or_truth_used=False),sort_keys=True))
PY
  mv terminal.json.pending terminal.json
  exit "$code"
}
trap finish EXIT
trap 'exit 143' TERM
exec >run.log 2>&1
export PYTHONPATH="$PWD/repo" OPENBLAS_NUM_THREADS=4 OMP_NUM_THREADS=4 MKL_NUM_THREADS=4
phase=dependencies
uv run --no-project --with numpy==2.3.3 --with pyarrow==24.0.0 python -c 'import numpy,pyarrow'
phase=source
/usr/bin/time -v -o source.time uv run --no-project --with numpy==2.3.3 --with pyarrow==24.0.0 python -m scripts.v282_prepare_pair source --input input.parquet --input-sha a199e151b89a496ed20e39fdd951591bbfb4817d682e9111ebe2e1cab7ae550d --kind parquet --output source > source.json
phase=raw
uv run --offline --no-project --with numpy==2.3.3 --with pyarrow==24.0.0 python - <<'PY'
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
remaining=100000
with open('vectors.raw','xb') as target:
    for batch in pq.ParquetFile('input.parquet').iter_batches(batch_size=8192,columns=['embedding']):
        column=batch.column(0)
        assert pa.types.is_fixed_size_list(column.type) and column.type.list_size==768 and column.type.value_type==pa.float32()
        rows=min(remaining,len(column))
        values=column.values.to_numpy(zero_copy_only=False).reshape(len(column),768)[:rows]
        assert np.isfinite(values).all()
        target.write(np.asarray(values,dtype='<f4').tobytes())
        remaining-=rows
        if not remaining:break
assert remaining==0
PY
phase=layout
/usr/bin/time -v -o layout.time uv run --offline --no-project --with numpy==2.3.3 --with pyarrow==24.0.0 python - <<'PY' > layout-command.json
import json
from pathlib import Path
from scripts.v283_semantic_cell_layout import build_semantic_cell_layout
from scripts.v120_source_layout import _load_source
from scripts.v282_prepare_pair import digest
source=Path('source/source.parquet'); provenance=Path('source/provenance.json')
a,b=digest(source),digest(provenance)
result=build_semantic_cell_layout(source,provenance,Path('layout'),expected_source_sha256=a,expected_provenance_sha256=b)
vectors,_=_load_source(source,provenance,expected_source_sha256=a,expected_provenance_sha256=b)
import numpy as np
low=vectors.min(axis=0).astype(np.float32)
span=np.maximum(vectors.max(axis=0).astype(np.float32)-low,np.float32(1e-12)).astype(np.float32)
step=(span/np.float32(255)).astype(np.float32)
Path('calibration.json').write_text(json.dumps(dict(low=low.tolist(),step=step.tolist(),query_or_truth_used=False))+'\n')
print(json.dumps(result,sort_keys=True))
PY
phase=complete
