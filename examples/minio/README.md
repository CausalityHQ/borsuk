# MinIO S3-Compatible Soak

This example starts a local MinIO S3 endpoint and runs BORSUK's S3-compatible
integration test and the request-rate soak against it.

```bash
./examples/minio/run-smoke.sh
```

The script starts MinIO with Docker Compose, creates the test bucket through the
AWS CLI, runs the env-gated Rust S3-compatible test and the `s3_soak` request-rate
soak with `BORSUK_S3_TEST_URI` set, and tears the stack down afterward. Set
`BORSUK_MINIO_KEEP_RUNNING=1` to keep the stack up for manual inspection.

The soak reports object-store requests per query and per add, query throughput,
tail latency, and how a warm decoded-segment cache trades resident RAM for a
lower request rate. Tune the workload with `BORSUK_SOAK_VECTORS` (default 2000)
and `BORSUK_SOAK_QUERIES` (default 200).

Manual equivalent:

```bash
docker compose -p borsuk-minio -f examples/minio/compose.yaml up -d

export AWS_ACCESS_KEY_ID=borsuk
export AWS_SECRET_ACCESS_KEY=borsuk-secret
export AWS_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://borsuk-test

export AWS_ENDPOINT=http://127.0.0.1:9000
export AWS_ALLOW_HTTP=true
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
export BORSUK_S3_TEST_URI=s3://borsuk-test/indexes

cargo test --locked -p borsuk --test s3_compatible -- --nocapture
cargo test --locked -p borsuk --test s3_soak -- --nocapture
```

The MinIO console is available at http://127.0.0.1:9001 (user `borsuk`,
password `borsuk-secret`) while the stack is running.
