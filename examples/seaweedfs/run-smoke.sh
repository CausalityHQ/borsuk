#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT/examples/seaweedfs/compose.yaml"
PROJECT="${BORSUK_SEAWEEDFS_PROJECT:-borsuk-seaweedfs}"
ENDPOINT="${AWS_ENDPOINT:-http://127.0.0.1:8333}"
BUCKET="${BORSUK_SEAWEEDFS_BUCKET:-borsuk-test}"
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
  if [[ "${BORSUK_SEAWEEDFS_KEEP_RUNNING:-0}" != "1" ]]; then
    compose down -v >/dev/null
  fi
}

require_command docker
require_command aws
require_command cargo
require_command uv
require_command npm

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
cargo run --locked -p borsuk --example s3_index

(
  cd "$ROOT/python"
  smoke_python="$(uv python find --show-version)"
  rm -f dist/borsuk-*.whl
  uv run --no-project --python "$smoke_python" --with 'maturin>=1.9,<2' maturin build --locked --out dist
  wheel="$(find dist -maxdepth 1 -type f -name 'borsuk-*.whl' -print -quit)"
  if [[ -z "$wheel" ]]; then
    echo "maturin build did not produce a borsuk wheel in dist/" >&2
    exit 1
  fi
  BORSUK_WHEEL_PATH="$wheel" uv run --python "$smoke_python" --with "./$wheel" python -m unittest discover tests
)

(
  cd "$ROOT/packages/borsuk"
  if [[ ! -d node_modules ]]; then
    npm ci
  fi
  npm run build:native
  npm test
)
