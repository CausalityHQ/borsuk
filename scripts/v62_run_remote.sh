#!/bin/bash
set -u
root=/mnt/v62-page-oracle-a613f66b
prefix=s3://borsuk-bench-453182569524-euc1/research/v62-algorithm-first/page-oracle
output_uri="$prefix/a0001"
terminal_file=/tmp/v62-page-oracle-terminal.json
phase=bootstrap
publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  for name in hashes.log run.log time.log; do
    if [ -f "$name" ]; then
      aws s3 cp "$name" "$output_uri/$name" --only-show-errors
    fi
  done
  if [ -f result.json ]; then
    aws s3 cp result.json "$output_uri/result.json" --only-show-errors
  fi
  printf '{"exit_code":%d,"phase":"%s","script_sha256":"77a388d9475b13035c5c9b1be4ce4fd5b434c32bebee0267cda6cc9bd5c0d3ad"}\n' \
    "$code" "$phase" > "$terminal_file"
  aws s3 cp "$terminal_file" "$output_uri/terminal.json" --only-show-errors
  unlink "$terminal_file"
  exit "$code"
}
trap publish_terminal EXIT
mkdir -p "$root" && cd "$root" || exit 90
phase=script-download
aws s3 cp "$prefix/v62_algorithm_first_page_oracle.py" probe.py --only-show-errors || exit 91
phase=input-download
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet source.parquet --only-show-errors || exit 92
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-gt100.parquet gt.parquet --only-show-errors || exit 93
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v61-algorithm-first/graph-page-ceiling-a613f66b86eb2952/a0001/artifacts/bfs-order.npy bfs-order.npy --only-show-errors || exit 94
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v61-algorithm-first/graph-page-ceiling-a613f66b86eb2952/a0001/artifacts/random-order.npy random-order.npy --only-show-errors || exit 95
sha256sum -c > hashes.log 2>&1 <<'HASHES'
77a388d9475b13035c5c9b1be4ce4fd5b434c32bebee0267cda6cc9bd5c0d3ad  probe.py
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  gt.parquet
189161dc756aa424690b2c28411b88d35b8102fbba5c89ec8daf3bc859ff866f  bfs-order.npy
7ae37c1219d207dd54e252ffbbe30e150711283268687ed01be2208a96aa6608  random-order.npy
HASHES
if [ "$?" -ne 0 ]; then
  exit 96
fi
phase=scientific-execution
set +e
/usr/bin/time -v /opt/conda/envs/pytorch/bin/python3 probe.py \
  --source source.parquet \
  --ground-truth gt.parquet \
  --bfs-order bfs-order.npy \
  --random-order random-order.npy \
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
aws s3 cp result.json "$output_uri/result.json" --only-show-errors || exit 98
phase=complete
exit 0
