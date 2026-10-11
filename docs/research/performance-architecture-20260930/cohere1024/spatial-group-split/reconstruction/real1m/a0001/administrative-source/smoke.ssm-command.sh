#!/bin/bash
set -euo pipefail
support=/opt/borsuk-reconstruction-gate
(cd "$support" && sha256sum -c support.sha256)
test ! -e "$support/admission.root-accepted.json"
cat > "$support/admission.root-accepted.json" <<'BORSUK_ROOT_ACCEPTED_END'
{
  "schema": "borsuk-reconstruction-root-phase-acceptance-v1",
  "stage": "admission",
  "instance_id": "i-0e7ec79ad22db80d1",
  "original_ssm_command_id": "0213fc5c-25fd-4e76-8bd4-f10c5ca8af70",
  "terminal_sha256": "3fb0ff53bdfb8b83df57431b4e19e9d25157205cc427c6ae036bd1cd88980fc1",
  "native_receipt_sha256": "eb8e8538822711b20e7a4038828b4fb634cff0cfaedd60d36ed60c88eb7d62cd",
  "all_closed_body_hashes_verified": true,
  "original_ssm_success": true,
  "performance_claim": false
}
BORSUK_ROOT_ACCEPTED_END
timeout -k 5 750 /bin/bash "$support/run-reconstruction-stage.sh" smoke
