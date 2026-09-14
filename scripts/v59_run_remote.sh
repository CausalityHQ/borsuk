#!/bin/bash
set -u
root=/mnt/v59-page-sketch-f2d5aa25
output_uri=s3://borsuk-bench-453182569524-euc1/research/v59-algorithm-first/cross-polytope-page-sketch-f2d5aa252cc0b366/a0001
mkdir -p "$root" && cd "$root" || exit 90
prefix=s3://borsuk-bench-453182569524-euc1/research/v59-algorithm-first/cross-polytope-page-sketch-f2d5aa252cc0b366
aws s3 cp "$prefix/v59_algorithm_first_cross_polytope_page_sketch.py" probe.py --only-show-errors || exit 91
aws s3 cp "$prefix/v55_algorithm_first_cross_polytope.py" v55_algorithm_first_cross_polytope.py --only-show-errors || exit 92
aws s3 cp "$prefix/v54_algorithm_first_binary_scan.py" v54_algorithm_first_binary_scan.py --only-show-errors || exit 93
aws s3 cp "$prefix/v53_algorithm_first_page_mlp.py" v53_algorithm_first_page_mlp.py --only-show-errors || exit 94
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet source.parquet --only-show-errors || exit 95
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-query.parquet query.parquet --only-show-errors || exit 96
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-gt100.parquet gt.parquet --only-show-errors || exit 97
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v38-query-blind-boundary-spill/ab96e6863623830f85388fb0a27e530b6f08dff7/runs/v38-build-20260913T062600Z-ab96e686/attempt-0000/spill-relation relation.parquet --only-show-errors || exit 98
sha256sum -c > hashes.log 2>&1 <<'HASHES'
f2d5aa252cc0b366e47b0d62c357df14e63253308e5d1d867c34ec3418cdf1b0  probe.py
f8f497e3dec3f55a9ff9cee4c8fd66cf90fee32f68dbc5745084c1569de176ea  v55_algorithm_first_cross_polytope.py
f6ce623bb58cbba1293ec7de4031c708780200bdc44a8ec17eb550607c92219d  v54_algorithm_first_binary_scan.py
e8e8a5064f6747bb5e3e608e97d2acfedcd75b720c6d4df800505f75bdfc9827  v53_algorithm_first_page_mlp.py
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  query.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  gt.parquet
f09bbe55e855329b2fa54f547377e0bc64dba7f64f4f84d0eff9907096396a50  relation.parquet
HASHES
if [ "$?" -ne 0 ]; then aws s3 cp hashes.log "$output_uri/hashes.log" --only-show-errors; exit 99; fi
export OMP_NUM_THREADS=96 MKL_NUM_THREADS=96 OPENBLAS_NUM_THREADS=96
set +e
/usr/bin/time -v /opt/conda/envs/pytorch/bin/python3 probe.py \
    --source source.parquet \
    --development-query query.parquet \
    --development-ground-truth gt.parquet \
    --spill-relation relation.parquet \
    --output result.json > run.log 2> time.log
code=$?
set -e
printf '{"exit_code":%d,"script_sha256":"f2d5aa252cc0b366e47b0d62c357df14e63253308e5d1d867c34ec3418cdf1b0"}\n' "$code" > terminal.json
for name in hashes.log run.log time.log terminal.json; do aws s3 cp "$name" "$output_uri/$name" --only-show-errors; done
if [ -f result.json ]; then aws s3 cp result.json "$output_uri/result.json" --only-show-errors; fi
exit "$code"
