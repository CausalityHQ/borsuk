#!/bin/bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
(cd "$support" && sha256sum -c support.sha256)
printf '%s  %s\n' 2ed5e95abd8005904219ebe8e2058292b46111c1d14ec6a47942a15a6e77049a "$support/admission.root-accepted.json" | sha256sum -c -
test ! -e "$support/smoke.root-accepted.json"
cat > "$support/smoke.root-accepted.json" <<'BORSUK_ROOT_ACCEPTED_END'
{
  "schema": "borsuk-reconstruction-root-phase-acceptance-v1",
  "stage": "smoke",
  "instance_id": "i-0e7ec79ad22db80d1",
  "original_ssm_command_id": "b9be10c6-a52c-4fca-9d82-0133f6caf537",
  "terminal_sha256": "b60aebdacd8467f483d598ec59e6d1ff5b36c6a1c41c223c3e950b6664f861df",
  "native_receipt_sha256": "3de8275b2334c967b5d11f7424fa3eabfcaa25774c16161ef0b5fa241b3e91e3",
  "all_closed_body_hashes_verified": true,
  "original_ssm_success": true,
  "performance_claim": false
}
BORSUK_ROOT_ACCEPTED_END
timeout -k 5 4950 /bin/bash "$support/run-reconstruction-stage.sh" parity
