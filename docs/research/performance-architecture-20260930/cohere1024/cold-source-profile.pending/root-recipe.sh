#!/bin/bash
# Root-owned execution only, after independent real-input admission and separate canary.
set -euo pipefail
: "${ROOT_ADMISSION_RECEIPT:?}" "${ROOT_CANARY_RECEIPT:?}"
: "${REFERENCE_CONFIG:?}" "${PROFILE_CONFIG:?}" "${PROFILE_OUTPUT:?}"
: "${RUNNER_ELF:?}" "${RUNNER_SHA256:?}"
# Existence checks only; root independently verifies both gate receipts.
test -s "$ROOT_ADMISSION_RECEIPT"
test -s "$ROOT_CANARY_RECEIPT"
printf '%s  %s\n' "$RUNNER_SHA256" "$RUNNER_ELF" | sha256sum -c -
test ! -e "$PROFILE_CONFIG"
test ! -e "$PROFILE_OUTPUT"
jq -e '
  if .semantic_leaf_limit == 64 and .fetch_parallelism == 128
    and .source_authentication == "offload2" and (.source_cache_bytes // 0) == 0
    and .max_active_queries == null and .serving.mode == "fp32_rerank"
    and .serving.shortlist == 16 and .serving.direct == null
  then .execution = {"mode":"diagnostic_panel","ordinals":[0,14,125,375,454,546,687,859],"trace":true}
  else error("reference must bind the unchanged serial leaf64/width128/cache0/Offload2/FP32-16 pipeline") end
' "$REFERENCE_CONFIG" > "$PROFILE_CONFIG"
config_sha=$(sha256sum "$PROFILE_CONFIG" | cut -d' ' -f1)
"$RUNNER_ELF" "$PROFILE_CONFIG" "$config_sha" "$PROFILE_OUTPUT"
# Root independently pins the produced identity/bound_inputs and seals in its CompletedConfig:
# compare_native_replay --fp32-serving COMPLETED_CONFIG CONFIG_SHA NEW_REPORT
# Retain the original command/terminal receipts, source hashes and both ELF hashes.
