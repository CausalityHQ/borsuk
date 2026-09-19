#!/bin/bash
set -u
digest=78125af7894dccc4e6793ba65f2303afda4b129e56ef00334ace63a72a6d5ce1
root=/mnt/v68-real-s3-78125af7
prefix=s3://borsuk-bench-453182569524-euc1/research/v68-algorithm-first/real-s3-78125af7894dccc4
output_uri="$prefix/a0001"
terminal_file=/tmp/v68-terminal.json
phase=bootstrap
publish_terminal() {
  code=$?
  trap - EXIT
  set +e
  for name in system.log dependency.log selftest.log hashes.log run.log time.log; do
    [ -f "$name" ] && aws s3 cp "$name" "$output_uri/$name" --only-show-errors
  done
  [ -f result.json ] && aws s3 cp result.json "$output_uri/result.json" --only-show-errors
  printf '{"exit_code":%d,"phase":"%s","script_sha256":"%s"}\n' "$code" "$phase" "$digest" > "$terminal_file"
  aws s3 cp "$terminal_file" "$output_uri/terminal.json" --only-show-errors
  unlink "$terminal_file"
  exit "$code"
}
trap publish_terminal EXIT
mkdir -p "$root" && cd "$root" || exit 90
phase=system-packages
dnf install -y -q time python3-pip > system.log 2>&1 || exit 89
phase=dependency-install
python3 -m venv .venv > dependency.log 2>&1 || exit 91
.venv/bin/python -m pip install --disable-pip-version-check --quiet --upgrade pip >> dependency.log 2>&1 || exit 91
.venv/bin/python -m pip install --disable-pip-version-check --quiet 'numpy==1.26.4' 'pyarrow==17.0.0' 'boto3' >> dependency.log 2>&1 || exit 91
phase=script-download
aws s3 cp "$prefix/v68_algorithm_first_real_s3.py" probe.py --only-show-errors || exit 92
printf '%s  probe.py\n' "$digest" | sha256sum -c > hashes.log 2>&1 || exit 93
phase=self-test
.venv/bin/python probe.py --self-test > selftest.log 2>&1 || exit 94
phase=input-download
base=s3://borsuk-bench-453182569524-euc1/research/v36-prefix-screen/runs/v36-prefix-screen-20260908T174540Z-31445a91/attempt-0000
aws s3 cp "$base/source.parquet" source.parquet --only-show-errors || exit 95
aws s3 cp "$base/development-query.parquet" query.parquet --only-show-errors || exit 96
aws s3 cp "$base/development-gt100.parquet" gt.parquet --only-show-errors || exit 97
aws s3 cp s3://borsuk-bench-453182569524-euc1/research/v63-algorithm-first/layout-oracle-e2f6c2bad99c720b/a0001/artifacts/kmeans_8192-order.npy layout-order.npy --only-show-errors || exit 98
sha256sum -c >> hashes.log 2>&1 <<'HASHES'
2796b579f37afe99ca4aff57e282335a6a79ad30596645957d26326a0560cf86  source.parquet
310bb54f79f2e79d09fe63aa4f6b5c6e9e7ffb31101964f816be978dadb2db54  query.parquet
fed7524fd675087f42b48b2f7fa9192b4661aaa4b665600de8378b8b6c696e11  gt.parquet
32cba9690cd9d0ed3809763e5a0fa3574b09a207da26e93404651acaa1a66a0b  layout-order.npy
HASHES
[ "$?" -ne 0 ] && exit 99
export OMP_NUM_THREADS=32 OPENBLAS_NUM_THREADS=32 AWS_DEFAULT_REGION=eu-central-1
phase=scientific-execution
set +e
/usr/bin/time -v .venv/bin/python probe.py --source source.parquet --development-query query.parquet   --ground-truth gt.parquet --layout-order layout-order.npy   --bucket borsuk-bench-453182569524-euc1 --prefix research/v68-algorithm-first/real-s3-78125af7894dccc4/index   --output result.json > run.log 2> time.log
code=$?
set -e
[ "$code" -ne 0 ] && exit "$code"
[ ! -f result.json ] && { phase=result-missing; exit 100; }
aws s3 cp result.json "$output_uri/result.json" --only-show-errors || exit 101
phase=complete
exit 0
