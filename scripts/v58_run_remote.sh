#!/bin/bash
set -u
root=/mnt/v58-prefix-pages-6a4ab3a5
output_uri=s3://borsuk-bench-453182569524-euc1/research/v58-algorithm-first/prefix-pages-6a4ab3a554edc5ae/a0001
mkdir -p "$root" && cd "$root" || exit 90
prefix=s3://borsuk-bench-453182569524-euc1/research/v58-algorithm-first/prefix-pages-6a4ab3a554edc5ae
aws s3 cp "$prefix/v58_algorithm_first_prefix_pages.py" probe.py --only-show-errors || exit 91
aws s3 cp "$prefix/v57_algorithm_first_leaf_incidence.py" v57_algorithm_first_leaf_incidence.py --only-show-errors || exit 92
aws s3 cp "$prefix/v54_algorithm_first_binary_scan.py" v54_algorithm_first_binary_scan.py --only-show-errors || exit 93
aws s3 cp "$prefix/v53_algorithm_first_page_mlp.py" v53_algorithm_first_page_mlp.py --only-show-errors || exit 94
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet source.parquet --only-show-errors || exit 95
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-query.parquet query.parquet --only-show-errors || exit 96
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-gt100.parquet gt.parquet --only-show-errors || exit 97
sha256sum -c > hashes.log 2>&1 <<'HASHES'
6a4ab3a554edc5aea3fc656219809ed8bc40e9c9f20c25d9340000ee6e8f2495  probe.py
72dd91c9e9b6321b8ef3f064a8a81f7d90135ffb2bbd57525a4f52779e091396  v57_algorithm_first_leaf_incidence.py
f6ce623bb58cbba1293ec7de4031c708780200bdc44a8ec17eb550607c92219d  v54_algorithm_first_binary_scan.py
e8e8a5064f6747bb5e3e608e97d2acfedcd75b720c6d4df800505f75bdfc9827  v53_algorithm_first_page_mlp.py
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  query.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  gt.parquet
HASHES
if [ "$?" -ne 0 ]; then aws s3 cp hashes.log "$output_uri/hashes.log" --only-show-errors; exit 98; fi
export OMP_NUM_THREADS=96 MKL_NUM_THREADS=96 OPENBLAS_NUM_THREADS=96
set +e
/usr/bin/time -v /opt/conda/envs/pytorch/bin/python3 probe.py --source source.parquet --development-query query.parquet --development-ground-truth gt.parquet --output result.json > run.log 2> time.log
code=$?
set -e
printf '{"exit_code":%d,"script_sha256":"6a4ab3a554edc5aea3fc656219809ed8bc40e9c9f20c25d9340000ee6e8f2495"}\n' "$code" > terminal.json
for name in hashes.log run.log time.log terminal.json; do aws s3 cp "$name" "$output_uri/$name" --only-show-errors; done
if [ -f result.json ]; then aws s3 cp result.json "$output_uri/result.json" --only-show-errors; fi
exit "$code"
