# SeaweedFS S3-Compatible Smoke

This example starts a local SeaweedFS S3 endpoint and runs BORSUK's
S3-compatible integration test against it.

```bash
docker compose -f examples/seaweedfs/compose.yaml up -d

export AWS_ACCESS_KEY_ID=borsuk
export AWS_SECRET_ACCESS_KEY=borsuk-secret
export AWS_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://borsuk-test

export AWS_ENDPOINT=http://127.0.0.1:8333
export AWS_ALLOW_HTTP=true
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST=false
export BORSUK_S3_TEST_URI=s3://borsuk-test/indexes

cargo test -p borsuk --test s3_compatible -- --nocapture
```

Use the same endpoint and credentials from Python or TypeScript. Pass
`cache_dir` / `cacheDir` to keep fetched segment and graph objects on local
NVMe while SeaweedFS remains the durable source of truth.
