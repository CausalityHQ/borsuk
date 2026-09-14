#!/bin/bash
set -u

root=/mnt/v53-page-mlp-e8e8a506
output_uri=s3://borsuk-bench-453182569524-euc1/research/v53-algorithm-first/page-mlp-e8e8a5064f6747bb/a0001
mkdir -p "$root"
cd "$root" || exit 90

aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v53-algorithm-first/page-mlp-e8e8a5064f6747bb/v53_algorithm_first_page_mlp.py probe.py --only-show-errors || exit 91
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet source.parquet --only-show-errors || exit 92
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-query.parquet query.parquet --only-show-errors || exit 93
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-gt100.parquet gt.parquet --only-show-errors || exit 94
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v38-query-blind-boundary-spill/ab96e6863623830f85388fb0a27e530b6f08dff7/runs/v38-build-20260913T062600Z-ab96e686/attempt-0000/spill-relation relation.parquet --only-show-errors || exit 95

sha256sum -c > hashes.log 2>&1 <<'HASHES'
e8e8a5064f6747bb5e3e608e97d2acfedcd75b720c6d4df800505f75bdfc9827  probe.py
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  query.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  gt.parquet
f09bbe55e855329b2fa54f547377e0bc64dba7f64f4f84d0eff9907096396a50  relation.parquet
HASHES
if [ "$?" -ne 0 ]; then
    aws s3 cp hashes.log "$output_uri/hashes.log" --only-show-errors
    exit 96
fi

export OMP_NUM_THREADS=96
export MKL_NUM_THREADS=96
export OPENBLAS_NUM_THREADS=96
set +e
/usr/bin/time -v /opt/conda/envs/pytorch/bin/python3 probe.py \
    --source source.parquet \
    --development-query query.parquet \
    --development-ground-truth gt.parquet \
    --spill-relation relation.parquet \
    --output result.json > run.log 2> time.log
code=$?
set -e

printf '{"exit_code":%d,"instance_id":"i-0b57210f2f98eab8c","script_sha256":"e8e8a5064f6747bb5e3e608e97d2acfedcd75b720c6d4df800505f75bdfc9827"}\n' "$code" > terminal.json
aws s3 cp hashes.log "$output_uri/hashes.log" --only-show-errors
aws s3 cp run.log "$output_uri/run.log" --only-show-errors
aws s3 cp time.log "$output_uri/time.log" --only-show-errors
aws s3 cp terminal.json "$output_uri/terminal.json" --only-show-errors
if [ -f result.json ]; then
    aws s3 cp result.json "$output_uri/result.json" --only-show-errors
fi
exit "$code"
