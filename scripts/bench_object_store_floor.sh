#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT_DIR/docs/research/object-store-floor-campaign.json"
[[ "${BORSUK_RUN_OBJECT_STORE_FLOOR:-0}" == "1" ]] || {
  echo "set BORSUK_RUN_OBJECT_STORE_FLOOR=1 for paid execution" >&2
  exit 2
}
[[ -z "$(git -C "$ROOT_DIR" status --porcelain)" ]] || {
  echo "refusing to benchmark a dirty source tree" >&2
  exit 2
}
OUTPUT="${BORSUK_OBJECT_STORE_FLOOR_OUTPUT_ROOT:?set fresh output root}"
OBJECT_URI="${BORSUK_OBJECT_STORE_FLOOR_URI:?set fresh object prefix}"
RESULT_URI="${BORSUK_OBJECT_STORE_FLOOR_RESULT_URI:-}"
ARCHITECTURE="${BORSUK_ARCHITECTURE:?set architecture}"
INSTANCE_TYPE="${BORSUK_INSTANCE_TYPE:?set instance type}"
REGION="${AWS_REGION:?set AWS_REGION explicitly}"
[[ -z "${AWS_ENDPOINT:-}" && -z "${AWS_ALLOW_HTTP:-}" && -z "${AWS_SKIP_SIGNATURE:-}" ]] || {
  echo "custom or unsigned AWS endpoints are forbidden for the production floor" >&2
  exit 2
}
[[ ! -e "$OUTPUT" ]] || { echo "refusing to replace output $OUTPUT" >&2; exit 3; }
mkdir -p "$OUTPUT"

sync_results() {
  if [[ -n "$RESULT_URI" ]]; then
    if [[ -n "${AWS_PROFILE:-}" ]]; then
      aws --profile "$AWS_PROFILE" s3 sync --only-show-errors "$OUTPUT" "$RESULT_URI"
    else
      aws s3 sync --only-show-errors "$OUTPUT" "$RESULT_URI"
    fi
  fi
}

failed() {
  status=$?
  if (( status != 0 )); then
    rm -f "$OUTPUT/OBJECT_STORE_FLOOR_COMPLETE"
    printf 'failed\n' > "$OUTPUT/OBJECT_STORE_FLOOR_FAILED"
    sync_results || true
  fi
  exit "$status"
}
trap failed EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cp "$MANIFEST" "$OUTPUT/manifest.json"
MANIFEST_SHA256="$(sha256sum "$MANIFEST" | awk '{print $1}')"
SOURCE_SHA256="${BORSUK_SOURCE_SHA256:-$(git -C "$ROOT_DIR" archive --format=tar HEAD | sha256sum | awk '{print $1}')}"
printf '%s\n' \
  "source_sha256=$SOURCE_SHA256" \
  "manifest_sha256=$MANIFEST_SHA256" \
  "architecture=$ARCHITECTURE" \
  "instance_type=$INSTANCE_TYPE" \
  > "$OUTPUT/environment.txt"

samples="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["samples"])' "$MANIFEST")"
warmups="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["warmups"])' "$MANIFEST")"
payload_bytes="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["payload_bytes"])' "$MANIFEST")"
head_bytes="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["head_bytes"])' "$MANIFEST")"

cargo build --locked --release -p borsuk --example object_store_floor
target_dir="$(cargo metadata --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
/usr/bin/time -v -o "$OUTPUT/resources.txt" env \
  BORSUK_OBJECT_STORE_FLOOR_URI="$OBJECT_URI" \
  BORSUK_OBJECT_STORE_FLOOR_OUTPUT="$OUTPUT/measurement" \
  BORSUK_OBJECT_STORE_FLOOR_SAMPLES="$samples" \
  BORSUK_OBJECT_STORE_FLOOR_WARMUPS="$warmups" \
  BORSUK_OBJECT_STORE_FLOOR_PAYLOAD_BYTES="$payload_bytes" \
  BORSUK_OBJECT_STORE_FLOOR_HEAD_BYTES="$head_bytes" \
  BORSUK_OBJECT_STORE_FLOOR_REGION="$REGION" \
  "$target_dir/release/examples/object_store_floor"

cp "$OUTPUT/measurement/OBJECT_STORE_FLOOR_COMPLETE" "$OUTPUT/OBJECT_STORE_FLOOR_COMPLETE"
python3 "$ROOT_DIR/scripts/validate_object_store_floor.py" --manifest "$MANIFEST" "$OUTPUT" \
  > "$OUTPUT/decision.txt"
sync_results
trap - EXIT
printf '%s\n' "$OUTPUT"
