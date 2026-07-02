# SeaweedFS S3-Compatible Smoke

This example starts a local SeaweedFS S3 endpoint and runs BORSUK's
S3-compatible integration tests against it from Rust, Python, and TypeScript.

```bash
./examples/seaweedfs/run-smoke.sh
```

The script starts SeaweedFS with Docker Compose, creates the test bucket through
the AWS CLI, runs the env-gated Rust S3-compatible integration test and Rust S3
example, builds the Python wheel and Node native addon, runs the Python and
TypeScript API suites with `BORSUK_S3_TEST_URI` set, and tears the compose
stack down afterward. Set `BORSUK_SEAWEEDFS_KEEP_RUNNING=1` to keep the stack
up for manual inspection.

Manual equivalent:

```bash
docker compose -p borsuk-seaweedfs -f examples/seaweedfs/compose.yaml up -d

export AWS_ACCESS_KEY_ID=borsuk
export AWS_SECRET_ACCESS_KEY=borsuk-secret
export AWS_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://borsuk-test

export AWS_ENDPOINT=http://127.0.0.1:8333
export AWS_ALLOW_HTTP=true
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
export BORSUK_S3_TEST_URI=s3://borsuk-test/indexes

cargo test --locked -p borsuk --test s3_compatible -- --nocapture
cargo run --locked -p borsuk --example s3_index

(
  cd python
  uvx maturin build --locked --out dist
  wheel="$(ls -t dist/borsuk-*.whl | head -1)"
  BORSUK_WHEEL_PATH="$wheel" uv run --with "./$wheel" python -m unittest discover tests
)

(cd packages/borsuk && npm run build:native && npm test)
```

Use the same endpoint and credentials from Python or TypeScript. Pass
`cache_dir` / `cacheDir` to keep fetched segment and graph objects on local
NVMe while SeaweedFS remains the durable source of truth.
