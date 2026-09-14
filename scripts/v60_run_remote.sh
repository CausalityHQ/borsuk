#!/bin/bash
set -u
root=/mnt/v60-diskann-6f193347
output_uri=s3://borsuk-bench-453182569524-euc1/research/v60-algorithm-first/diskann-returned-recall-6f193347e7ce5f86/a0001
mkdir -p "$root" && cd "$root" || exit 90
prefix=s3://borsuk-bench-453182569524-euc1/research/v60-algorithm-first/diskann-returned-recall-6f193347e7ce5f86
aws s3 cp "$prefix/v60_algorithm_first_diskann_returned_recall.py" probe.py --only-show-errors || exit 91
/opt/conda/envs/pytorch/bin/python3 -m pip install --disable-pip-version-check diskannpy==0.7.0 > dependency.log 2>&1 || { aws s3 cp dependency.log "$output_uri/dependency.log" --only-show-errors; exit 92; }
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/source.parquet source.parquet --only-show-errors || exit 93
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-query.parquet query.parquet --only-show-errors || exit 94
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000/development-gt100.parquet gt.parquet --only-show-errors || exit 95
sha256sum -c > hashes.log 2>&1 <<'HASHES'
6f193347e7ce5f868da5903fc3d74c993d506adab7ccdf40f445030bca90f2b9  probe.py
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  query.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  gt.parquet
HASHES
if [ "$?" -ne 0 ]; then aws s3 cp hashes.log "$output_uri/hashes.log" --only-show-errors; exit 96; fi
export OMP_NUM_THREADS=64 MKL_NUM_THREADS=64 OPENBLAS_NUM_THREADS=64
set +e
/usr/bin/time -v /opt/conda/envs/pytorch/bin/python3 probe.py \
    --source source.parquet \
    --development-query query.parquet \
    --development-ground-truth gt.parquet \
    --index-directory index \
    --output result.json > run.log 2> time.log
code=$?
set -e
printf '{"exit_code":%d,"script_sha256":"6f193347e7ce5f868da5903fc3d74c993d506adab7ccdf40f445030bca90f2b9"}\n' "$code" > terminal.json
for name in dependency.log hashes.log run.log time.log terminal.json; do aws s3 cp "$name" "$output_uri/$name" --only-show-errors; done
if [ -f result.json ]; then aws s3 cp result.json "$output_uri/result.json" --only-show-errors; fi
exit "$code"
