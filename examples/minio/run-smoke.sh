#!/usr/bin/env bash
set -euo pipefail

# Bring up a local MinIO S3 endpoint and run BORSUK's S3-compatible integration
# test plus the request-rate soak against it. Mirrors examples/seaweedfs/run-smoke.sh
# but targets MinIO on port 9000. Set BORSUK_MINIO_KEEP_RUNNING=1 to keep the
# stack up for manual inspection.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT/examples/minio/compose.yaml"
PROJECT="${BORSUK_MINIO_PROJECT:-borsuk-minio}"
ENDPOINT="${AWS_ENDPOINT:-http://127.0.0.1:9000}"
BUCKET="${BORSUK_MINIO_BUCKET:-borsuk-test}"
TEST_URI="${BORSUK_S3_TEST_URI:-s3://$BUCKET/indexes}"

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

compose() {
  docker compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  if [[ "${BORSUK_MINIO_KEEP_RUNNING:-0}" != "1" ]]; then
    compose down -v >/dev/null
  fi
}

require_command docker
require_command aws
require_command cargo

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-borsuk}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-borsuk-secret}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-$AWS_REGION}"
export AWS_ALLOW_HTTP="${AWS_ALLOW_HTTP:-true}"
export AWS_VIRTUAL_HOSTED_STYLE_REQUEST="${AWS_VIRTUAL_HOSTED_STYLE_REQUEST:-false}"
export AWS_ENDPOINT="$ENDPOINT"
export BORSUK_S3_TEST_URI="$TEST_URI"

trap cleanup EXIT

compose up -d

for _ in $(seq 1 60); do
  if aws --endpoint-url "$ENDPOINT" s3 ls >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

aws --endpoint-url "$ENDPOINT" s3 mb "s3://$BUCKET" 2>/dev/null || true

cargo test --locked -p borsuk --test s3_compatible -- --nocapture
cargo test --locked -p borsuk --test s3_soak -- --nocapture
