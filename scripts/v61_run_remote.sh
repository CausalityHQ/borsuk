#!/bin/bash
set -u
root=/mnt/v61-graph-page-ceiling-a613f66b
prefix=s3://borsuk-bench-453182569524-euc1/research/v61-algorithm-first/graph-page-ceiling-a613f66b86eb2952
output_uri="$prefix/a0001"
terminal_file=/tmp/v61-graph-page-ceiling-terminal.json
phase=bootstrap
publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  for name in dependency.log hashes.log run.log time.log; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "$output_uri/$name" --only-show-errors
    fi
  done
  if [ -f result.json ]; then
    aws s3 cp result.json "$output_uri/result.json" --only-show-errors
  fi
  printf '{"exit_code":%d,"phase":"%s","script_sha256":"a613f66b86eb2952fc376e4ce91ae2aa94b37502e94b25a71c23a714fc8feb12"}\n' \
    "$code" "$phase" > "$terminal_file"
  aws s3 cp "$terminal_file" "$output_uri/terminal.json" --only-show-errors
  unlink "$terminal_file"
  exit "$code"
}
trap publish_terminal EXIT
mkdir -p "$root" && cd "$root" || exit 90
phase=script-download
aws s3 cp "$prefix/v61_algorithm_first_graph_page_ceiling.py" probe.py --only-show-errors || exit 91
phase=dependency-install
/opt/conda/envs/pytorch/bin/python3 -m pip install --disable-pip-version-check diskannpy==0.7.0 > dependency.log 2>&1 || {
  exit 92
}
if ! /opt/conda/envs/pytorch/bin/python3 - >> dependency.log 2>&1 <<'PY'
import numpy
import pyarrow
import scipy
print("numpy", numpy.__version__)
print("pyarrow", pyarrow.__version__)
print("scipy", scipy.__version__)
PY
then
  exit 92
fi
phase=input-download
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet source.parquet --only-show-errors || exit 93
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-query.parquet query.parquet --only-show-errors || exit 94
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-gt100.parquet gt.parquet --only-show-errors || exit 95
sha256sum -c > hashes.log 2>&1 <<'HASHES'
a613f66b86eb2952fc376e4ce91ae2aa94b37502e94b25a71c23a714fc8feb12  probe.py
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  query.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  gt.parquet
HASHES
if [ "$?" -ne 0 ]; then
  exit 96
fi
export OMP_NUM_THREADS=64 MKL_NUM_THREADS=64 OPENBLAS_NUM_THREADS=64
phase=scientific-execution
set +e
/usr/bin/time -v /opt/conda/envs/pytorch/bin/python3 probe.py \
  --source source.parquet \
  --development-query query.parquet \
  --development-ground-truth gt.parquet \
  --index-directory index \
  --artifact-directory artifacts \
  --output result.json > run.log 2> time.log
code=$?
set -e
if [ "$code" -ne 0 ]; then
  exit "$code"
fi
if [ ! -f result.json ]; then
  phase=result-missing
  exit 97
fi
phase=artifact-publication
for name in degrees.npy neighbors.npy bfs-order.npy random-order.npy; do
  aws s3 cp "artifacts/$name" "$output_uri/artifacts/$name" --only-show-errors || exit 98
done
aws s3 cp result.json "$output_uri/result.json" --only-show-errors || exit 99
phase=complete
exit 0
